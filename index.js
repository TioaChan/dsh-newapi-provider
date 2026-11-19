/**
 * dsh-newapi-provider — Host half.
 *
 * New API gateways inside DeepSeek Harness:
 *   - several gateway "channels" (address + token); every channel is served at
 *     the same time on its own DSH provider route, each with its own model
 *     list, balance and media tools;
 *   - model list from each gateway, with context window / output cap /
 *     modalities from models.dev and per-model, per-channel user overrides;
 *   - token and account balance per channel from /api/usage/token/;
 *   - a session-checked JSON API under /newapi-provider/api/ that the Web UI
 *     half reads and writes (channel edits go through the DSH settings service,
 *     tokens through the DSH credential service).
 * Image and video tools register from ./lib/media.js.
 */
import z from '@deepseek-ai/schemastery';
import { LlmError, RetryPolicySchema, assertUsableApiKey, resolveImageAttachmentAccess, resolveRetryPolicy } from '@deepseek-ai/dsh-llm';
import { credentialRef } from '@deepseek-ai/dsh-credentials';
import { NewApiAdapter, DEFAULT_ENDPOINT_PRIORITY } from './lib/adapter.js';
import { Catalog, chatModels, classify, upstreamModelOf, DEFAULT_CONTEXT_WINDOW, DEFAULT_MAX_TOKENS } from './lib/catalog.js';
import { ModelsDev } from './lib/modelsdev.js';
import { BalanceService } from './lib/balance.js';
import { registerMedia } from './lib/media.js';
import { fetchJson, fingerprint, gatewayRoot, redact } from './lib/util.js';
import { ChannelService, resolveChannels, taskChannel, registerAsyncEffect, validateBaseURL } from './lib/channels.js';
import { ModelTester } from './lib/model-test.js';
export { resolveChannels } from './lib/channels.js';

export const name = 'newapi-provider';
export const inject = ['llm'];

const ROUTE_PREFIX = '/newapi-provider/api/';
const ENDPOINT_TYPES = ['openai', 'anthropic', 'openai-response', 'gemini'];
const KINDS = ['chat', 'image', 'video', 'audio', 'embedding', 'other'];
const LEVELS = ['off', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'];
const MODALITIES = ['text', 'image', 'audio', 'video', 'pdf'];


const ModelOverride = z.object({
  name: z.string().description('Display name in the model picker.'),
  upstreamModel: z.string().default(undefined).description('Upstream wire model id; omitted uses the stable local model id.'),
  kind: z.union(KINDS).description('Force how the model is used: chat models appear in the picker, image/video models are used by the generation tools.'),
  hidden: z.boolean().description('Hide this model everywhere.'),
  contextWindow: z.number().step(1).min(1).description('Context window in tokens (overrides models.dev).'),
  maxTokens: z.number().step(1).min(1).description('Maximum output tokens (overrides models.dev).'),
  input: z.array(z.union(MODALITIES)).default(undefined).description('Input modalities the model accepts.'),
  reasoning: z.boolean().description('Whether the model supports reasoning effort.'),
  reasoningLevels: z.array(z.union(LEVELS)).default(undefined).description('Reasoning effort levels to offer.'),
  thinkingLevelMap: z.dict(z.union([z.string(), z.const(null)])).default(undefined)
    .description('Per-level wire value: a string is sent upstream, null hides the level. Omitted levels fall back to the detected value.'),
  protocol: z.union(ENDPOINT_TYPES).description('Force the wire protocol for this model.'),
  custom: z.boolean().description('Add this model even if the gateway does not list it.'),
});

const Channel = z.object({
  id: z.string().required().description('Stable channel id.'),
  name: z.string().description('Display name.'),
  baseURL: z.string().description('Gateway address, with or without /v1.'),
  apiKeyEnv: z.string().role('credential-ref').description('Credential holding this gateway\'s token (sk-…).'),
  headers: z.dict(z.string()).description('Extra HTTP headers for this gateway.'),
  models: z.dict(ModelOverride).description('Per-model overrides keyed by model id.'),
  enabledModels: z.array(z.string()).default(undefined).description('Models enabled on this channel. Unset shows every model the gateway lists.'),
  providerId: z.string().description('DSH provider route id this channel is served on. Assigned on the first settings write and stable afterwards.'),
});

export const Config = z.object({
  channels: z.array(Channel).default([]).description('New API gateways. Every channel is served at the same time on its own provider route.'),
  activeChannel: z.string().description('Legacy single-route pointer from the one-active version: read to keep that channel\'s provider route id stable, then retired by the next settings write.'),
  baseURL: z.string().description('Single-gateway form (used when `channels` is empty).'),
  apiKeyEnv: z.string().role('credential-ref').description('Single-gateway form: credential holding the token.'),
  models: z.dict(ModelOverride).description('Single-gateway form: per-model overrides.'),
  providerId: z.string().default('newapi-provider').description('Base provider route id used when a channel first gets one. The legacy active channel keeps it, every other channel appends its id, and persisted ids never change afterwards. Changing this setting does not rename existing routes.'),
  displayName: z.string().default('New API').description('Fallback provider name; every route is named after its channel.'),
  endpointPriority: z.array(z.union(ENDPOINT_TYPES)).default([...DEFAULT_ENDPOINT_PRIORITY]).description('Preferred wire protocol when a model supports several.'),
  defaultContextWindow: z.number().step(1).min(1).default(DEFAULT_CONTEXT_WINDOW).description('Context window used when neither models.dev nor an override sizes a model.'),
  defaultMaxTokens: z.number().step(1).min(1).default(DEFAULT_MAX_TOKENS).description('Output cap used when neither models.dev nor an override sizes a model.'),
  useModelsDev: z.boolean().default(true).description('Fill context window, output cap and modalities from models.dev.'),
  headers: z.dict(z.string()).default({}).description('Extra HTTP headers sent to every gateway.'),
  streamIdleTimeoutMs: z.number().min(1000).max(2147483647).default(300_000).description('Abort a model stream that stays silent this long.'),
  balanceRefreshSeconds: z.number().step(1).min(0).default(120).description('Refresh every channel balance this often (0 = only on demand).'),
  retryPolicy: RetryPolicySchema,
}).volatile();

function sendJson(res, status, value) {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.setHeader('cache-control', 'no-store');
  res.end(JSON.stringify(value));
}

async function readBody(req, limit = 64 * 1024) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error('request body too large'), { status: 413 });
    chunks.push(chunk);
  }
  const text = Buffer.concat(chunks).toString('utf8');
  if (!text) return {};
  try { return JSON.parse(text); } catch { throw Object.assign(new Error('invalid JSON body'), { status: 400 }); }
}

export function apply(ctx, config) {
  const settingsNs = ctx.fiber.entry?.options.id ?? 'newapi-provider';
  const current = () => (typeof config?.get === 'function' ? config.get() : config) ?? {};
  const errors = new Map();
  const reportError = (scope, error) => {
    errors.set(scope, { at: Date.now(), code: error?.code ?? 'ERROR', message: String(error?.message ?? error) });
  };

  // This plugin draws its own settings page; hide the generated form.
  ctx.inject(['settings'], (sctx) => {
    sctx.effect(() => sctx.settings.configure({ auto: false }, ctx.fiber));
  });

  let channelsMemo;
  const channels = () => {
    const raw = current();
    if (channelsMemo?.raw !== raw) channelsMemo = { raw, value: resolveChannels(raw) };
    return channelsMemo.value;
  };

  const channelById = (id) => {
    const ch = id ? channels().list.find((c) => c.id === id) : channels().list[0];
    if (!ch) throw Object.assign(new Error(id ? `unknown channel "${id}"; list channel ids with newapi_channels` : 'no channel is configured'), { status: 404 });
    return ch;
  };
  const channelForRoute = (providerId) => channels().list.find((c) => c.providerId === providerId);

  /** Picker name of one route: the channel name, so every gateway is named by its own account. */
  const displayNameFor = (ch) => ch.name || current().displayName || 'New API';

  const connFor = (ch) => {
    const raw = current();
    const overrides = ch.models ?? {};
    return {
      channelId: ch.id,
      channelName: ch.name,
      // Each channel is its own DSH provider route; there is no shared active connection.
      providerId: ch.providerId,
      displayName: displayNameFor(ch),
      baseURL: ch.baseURL,
      apiKeyRef: credentialRef(ch.apiKeyEnv),
      apiKeyName: ch.apiKeyEnv,
      overrides,
      enabledModels: Array.isArray(ch.enabledModels) ? ch.enabledModels : undefined,
      overridesRev: fingerprint(`${ch.id}|${JSON.stringify(overrides)}|${JSON.stringify(ch.enabledModels ?? null)}`),
      endpointPriority: raw.endpointPriority?.length ? raw.endpointPriority : DEFAULT_ENDPOINT_PRIORITY,
      defaultContextWindow: raw.defaultContextWindow ?? DEFAULT_CONTEXT_WINDOW,
      defaultMaxTokens: raw.defaultMaxTokens ?? DEFAULT_MAX_TOKENS,
      headers: { ...(raw.headers ?? {}), ...(ch.headers ?? {}) },
      streamIdleTimeoutMs: raw.streamIdleTimeoutMs ?? 300_000,
      retryPolicy: resolveRetryPolicy(raw.retryPolicy, 'newapi-provider retryPolicy'),
      balanceRefreshSeconds: raw.balanceRefreshSeconds ?? 120,
    };
  };

  const keyFor = async (ch) => {
    const credentials = ctx.get('credentials');
    const ref = credentialRef(ch.apiKeyEnv);
    const hit = credentials ? (await credentials.resolve(ref))?.value : process.env[ch.apiKeyEnv];
    if (!hit) {
      throw new LlmError(`newapi-provider: no token stored for channel "${ch.name}" (credential ${ch.apiKeyEnv}); add it in Settings → New API Provider`, 'MISSING_CREDENTIAL');
    }
    return assertUsableApiKey(hit, 'newapi-provider', ref);
  };

  const modelsDev = new ModelsDev({ enabled: current().useModelsDev !== false, logger: ctx.logger });
  const catalogs = new Catalog({ modelsDev, logger: ctx.logger });
  // Capability data arriving late must not leave the first catalog (built without
  // it) cached for the whole TTL.
  modelsDev.onUpdated = () => catalogs.invalidate();

  const catalogFor = async (ch, { signal, force = false } = {}, captured = connFor(ch)) => {
    const apiKey = await keyFor(ch);
    const conn = { ...captured, keyFingerprint: fingerprint(apiKey) };
    try {
      return await catalogs.get(conn, apiKey, { signal, force });
    } catch (error) {
      const stale = catalogs.peek(conn);
      if (stale) return stale;
      throw new LlmError(redact(`newapi-provider: cannot read the model list from ${gatewayRoot(conn.baseURL)}: ${error.message}`, apiKey), error.code === 'AUTH' ? 'AUTH' : 'TRANSPORT', { cause: error });
    }
  };

  // Capture channel, URL, headers, overrides and credential reference before any await.
  const captureById = (id) => {
    const ch = structuredClone(channelById(id));
    const conn = connFor(ch);
    let key;
    const requireApiKey = () => key ??= keyFor(ch);
    return {
      connection: conn, requireApiKey,
      catalog: async (opts) => catalogs.get({ ...conn, keyFingerprint: fingerprint(await requireApiKey()) }, await requireApiKey(), opts),
    };
  };
  /** Bind an adapter call to the channel behind one provider route. */
  const captureRoute = (providerId) => {
    const ch = channelForRoute(providerId);
    if (!ch) throw new LlmError(`newapi-provider: no channel is registered for route "${providerId}"`, 'NO_ADAPTER');
    return captureById(ch.id);
  };
  const connectionForRoute = (providerId) => {
    const ch = channelForRoute(providerId);
    if (!ch) throw new LlmError(`newapi-provider: no channel is registered for route "${providerId}"`, 'NO_ADAPTER');
    return connFor(ch);
  };
  const adapter = new NewApiAdapter({
    connection: connectionForRoute,
    capture: captureRoute,
    reportError,
    logger: ctx.logger,
    piAuth: undefined,
    resolveAttachments: () => ctx.get('attachments'),
    resolveImageAccess: (attachments, ref) => resolveImageAttachmentAccess(attachments, (hostPath) => ctx.get('fs')?.processPathFromHostPath(hostPath), ref),
  });

  // One adapter registration per channel route. A route still held by another
  // adapter is retried whenever the provider topology changes instead of
  // failing the whole plugin, and a changed retry policy re-registers the route
  // so the captured policy never goes stale.
  let directory;
  let directoryKey;
  let directoryError = null;
  let ensuring = false;
  const routeHandles = new Map(); // providerId -> { handle, key }
  const routeErrors = new Map();
  const ensureRegistration = () => {
    if (ensuring) return;
    ensuring = true;
    try {
      const state = channels();
      const desired = new Map(state.list.map((ch) => [ch.providerId, ch]));
      for (const [provider, held] of [...routeHandles]) {
        if (desired.has(provider)) continue;
        routeHandles.delete(provider);
        routeErrors.delete(provider);
        try { held.handle(); } catch (error) { ctx.logger.warn(error); }
      }
      const entries = state.list.map((ch, index) => ({
        provider: ch.providerId,
        displayName: displayNameFor(ch),
        settingsNs,
        // Address this channel's profile inside the channels array so the DSH
        // Models page can list the route and its credential state.
        settingsPath: ['channels', String(index)],
        declared: true,
        defaultBaseURL: gatewayRoot(ch.baseURL),
      }));
      const dirKey = JSON.stringify(entries);
      directoryError = null;
      try {
        if (directoryKey !== dirKey) {
          if (directory) directory.replace(entries); else directory = ctx.llm.registerConfigurableProviders(entries);
          directoryKey = dirKey;
        }
      } catch (error) {
        directoryError = String(error?.message ?? error);
        directoryKey = undefined;
        ctx.logger.warn(`newapi-provider: provider directory was not updated: ${directoryError}`);
      }
      for (const [provider, ch] of desired) {
        // Registration captures the route's retry policy and display name; both
        // re-register the route when they change, and nothing else does.
        const conn = connFor(ch);
        const key = JSON.stringify({ policy: conn.retryPolicy, name: conn.displayName });
        const held = routeHandles.get(provider);
        if (held && held.key === key) continue;
        if (held) {
          routeHandles.delete(provider);
          try { held.handle(); } catch (error) { ctx.logger.warn(error); }
        }
        try {
          routeHandles.set(provider, { handle: ctx.llm.registerAdapter([provider], adapter), key });
          routeErrors.delete(provider);
        } catch (error) {
          routeErrors.set(provider, String(error?.message ?? error));
          ctx.logger.warn(`newapi-provider: provider route "${provider}" is not available yet (${error.code ?? error.message}); it will be retried when it is released`);
        }
      }
    } finally { ensuring = false; }
  };
  ensureRegistration();
  ctx.on('llm/adapters-updated', () => { ensureRegistration(); });
  ctx.llm.registerModelDiscovery(settingsNs, async (request, signal) => {
    const state = channels();
    // A route the directory named wins; a draft endpoint is a different host.
    const requested = typeof request.provider === 'string' ? channelForRoute(request.provider) : undefined;
    if (typeof request.provider === 'string' && !requested) throw new Error(`unknown provider route "${request.provider}"`);
    const ch = requested ?? state.list[0];
    if (!ch) throw new Error('no channel is configured');
    const saved = connFor(ch);
    const draftRoot = typeof request.baseURL === 'string' && request.baseURL.trim()
      ? validateBaseURL(request.baseURL)
      : undefined;
    const isDraft = draftRoot !== undefined && draftRoot !== gatewayRoot(ch.baseURL);
    if (isDraft && !request.apiKey) throw new Error('a draft gateway requires its own token');
    const apiKey = request.apiKey || await keyFor(ch);
    // A draft address is a different host: never forward this channel's headers,
    // and never cache the draft under the saved channel's identity.
    const conn = isDraft
      ? {
        ...saved,
        channelId: `${ch.id}#draft`,
        providerId: `${ch.providerId}#draft`,
        baseURL: draftRoot,
        headers: { ...(current().headers ?? {}) },
        overrides: {},
        enabledModels: undefined,
        overridesRev: fingerprint(`${ch.id}|draft|${draftRoot}`),
        keyFingerprint: fingerprint(apiKey),
      }
      : { ...saved, keyFingerprint: fingerprint(apiKey) };
    const value = await catalogs.get(conn, apiKey, { signal, force: true });
    return chatModels(value).map((e) => ({
      id: e.id,
      name: e.name,
      ...(e.contextWindow ? { contextWindow: e.contextWindow } : {}),
      ...(e.maxTokens ? { maxTokens: e.maxTokens } : {}),
      inputModalities: e.input.filter((m) => m === 'text' || m === 'image'),
    }));
  });

  // One balance service per channel, created on demand.
  const balances = new Map();
  const balanceOf = (id) => {
    let svc = balances.get(id);
    if (!svc) {
      const ch = structuredClone(channelById(id));
      const conn = connFor(ch);
      svc = new BalanceService({ capture: () => ({ connection: conn, requireApiKey: () => keyFor(ch) }), logger: ctx.logger });
      balances.set(id, svc);
    }
    return svc;
  };
  const refreshBalances = ({ force = false } = {}) => Promise.allSettled(channels().list.map((ch) => balanceOf(ch.id).refresh({ force })));

  ctx.on('loader/volatile-update', () => {
    // The live config object may be replaced or mutated in place; never serve a
    // stale channel list, route id or display name from the memo.
    channelsMemo = undefined;
    catalogs.invalidate();
    modelsDev.enabled = current().useModelsDev !== false;
    // Old services may finish, but are no longer observable after an edit/rotation.
    balances.clear();
    try { ensureRegistration(); } catch (error) { ctx.logger.error(error); }
    void refreshBalances({ force: true });
  });
  void refreshBalances();
  void modelsDev.refresh();
  ctx.effect(() => {
    let timer;
    let stopped = false;
    const schedule = () => {
      if (stopped) return;
      const seconds = Number(current().balanceRefreshSeconds ?? 120);
      if (!(seconds > 0)) return;
      timer = setTimeout(() => {
        refreshBalances().catch(() => {}).finally(schedule);
      }, Math.max(15, seconds) * 1000);
    };
    schedule();
    return () => { stopped = true; clearTimeout(timer); };
  });

  const media = registerMedia(ctx, {
    capture: captureById, reportError,
    captureTask: (task) => {
      const ch = taskChannel(task, channels().list);
      return { connection: connFor(ch), requireApiKey: () => keyFor(ch) };
    },
  });

  // ------------------------------------------------------------ model list & tests

  /** Gateway catalog for one channel, ignoring its saved list (what the gateway offers). */
  const gatewayCatalog = async (ch, opts = {}) => {
    const snapshotCh = structuredClone(ch);
    const conn = { ...connFor(snapshotCh), enabledModels: undefined, channelId: `${snapshotCh.id}#gateway` };
    const apiKey = await keyFor(snapshotCh);
    return catalogs.get({ ...conn, keyFingerprint: fingerprint(apiKey) }, apiKey, opts);
  };

  const tester = new ModelTester({
    channelFacts: async (channelId) => {
      const bound = captureById(channelId);
      const conn = bound.connection;
      return {
        baseURL: conn.baseURL, headers: conn.headers, endpointPriority: conn.endpointPriority,
        apiKey: bound.requireApiKey,
        entryFor: async (modelId) => (await bound.catalog()).entries.find((e) => e.id === modelId),
        submitVideo: (modelId) => media.submitTestVideo(channelId, modelId, bound),
      };
    },
  });

  const discoverModels = async ({ channelId }) => {
    const ch = channelById(channelId);
    const value = await gatewayCatalog(ch, { force: true });
    const enabled = Array.isArray(ch.enabledModels) ? new Set(ch.enabledModels) : undefined;
    const gatewayIds = new Set(value.gatewayIds ?? value.entries.filter((e) => e.source?.gateway).map((e) => e.id));
    return {
      channelId: ch.id,
      fetchedAt: value.fetchedAt,
      listMode: enabled ? 'list' : 'auto',
      models: value.entries.filter((e) => gatewayIds.has(e.upstreamModel ?? e.id)).map((e) => ({
        id: e.id, upstreamModel: e.upstreamModel ?? e.id, kind: e.kind, vendor: e.vendor, input: e.input,
        contextWindow: e.contextWindow ?? null, maxTokens: e.maxTokens ?? null,
        reasoning: e.reasoning, pricing: e.pricing,
        // A gateway model already used through a local id's mapping is not "new".
        status: !enabled || enabled.has(e.id) || e.upstreamModel !== e.id ? 'enabled' : 'new',
      })),
      missing: enabled
        ? [...enabled].filter((id) => !gatewayIds.has(upstreamModelOf({ id, upstreamModel: ch.models?.[id]?.upstreamModel }))).map((id) => ({ id, custom: ch.models?.[id]?.custom === true }))
        : [],
    };
  };

  // ------------------------------------------------------------ config writes

  const storeKey = async (keyName, apiKey) => {
    const credentials = ctx.get('credentials');
    if (!credentials) throw Object.assign(new Error('the DSH credential service is unavailable'), { status: 503 });
    const value = String(apiKey ?? '').trim();
    if (!value || /\s/.test(value)) throw Object.assign(new Error('the token must be a single non-empty string'), { status: 400 });
    await credentials.set(credentialRef(keyName), value);
  };

  const keyConfigured = async (ch) => {
    try { await keyFor(ch); return true; } catch { return false; }
  };

  /** Probe a gateway without saving anything: version, model counts, token quota. */
  const testGateway = async ({ baseURL, apiKey, headers }) => {
    const root = gatewayRoot(baseURL);
    const out = { ok: false, baseURL: root };
    const started = Date.now();
    try {
      const status = await fetchJson(`${root}/api/status`, { headers, timeoutMs: 8_000 }).catch(() => undefined);
      const st = status?.data ?? status;
      out.version = st?.version ?? null;
      out.systemName = st?.system_name ?? null;
      const models = await fetchJson(`${root}/v1/models`, { apiKey, headers, timeoutMs: 10_000 });
      const counts = { total: 0, chat: 0, image: 0, video: 0, other: 0 };
      for (const m of models?.data ?? []) {
        counts.total++;
        const k = classify(m.id, modelsDev.match(m.id));
        if (k === 'chat' || k === 'image' || k === 'video') counts[k]++; else counts.other++;
      }
      out.models = counts;
      out.latencyMs = Date.now() - started;
      out.ok = true;
    } catch (error) {
      out.error = redact(String(error?.message ?? error), apiKey);
      out.code = error?.code ?? 'ERROR';
    }
    return out;
  };

  const channelService = new ChannelService({
    ns: settingsNs, current, settings: () => ctx.get('settings'), keyFor, keyConfigured,
    storeKey, unsetKey: async (name) => ctx.get('credentials')?.unset(credentialRef(name)), testGateway,
    /** Global headers always; channel headers only for the channel's own saved address. */
    headersFor: (ch, baseURL) => {
      const global = current().headers ?? {};
      if (!ch) return { ...global };
      return gatewayRoot(ch.baseURL) === gatewayRoot(baseURL) ? { ...global, ...(ch.headers ?? {}) } : { ...global };
    },
    // First list edit on an automatic-mode channel starts from what the gateway lists now.
    gatewayModelIds: async (ch) => (await gatewayCatalog(ch, { force: true })).entries.map((e) => e.id),
    changed: () => {
      channelsMemo = undefined; catalogs.invalidate(); balances.clear(); ensureRegistration();
      const ids = new Set(channels().list.map((c) => c.id));
      for (const id of new Set([...tester.results.keys()].map((k) => k.split('\u0000')[0]))) if (!ids.has(id)) tester.forget(id);
    },
  });

  // ---------------------------------------------------------------- snapshot

  const withTimeout = async (p, ms) => {
    let timer;
    try { return await Promise.race([p, new Promise((r) => { timer = setTimeout(r, ms); })]); }
    finally { clearTimeout(timer); }
  };

  /**
   * One channel's own model list, with its own overrides and test results.
   * Every channel is maintained independently: what gateway A lists (and what
   * the user enabled there) never leaks into gateway B.
   */
  const modelsOf = async (ch, { force = false } = {}) => {
    const captured = structuredClone(ch);
    const conn = connFor(captured);
    const value = await catalogFor(captured, { force }, conn);
    // A successful read clears that channel's stale diagnostic from "issues".
    errors.delete(`catalog:${captured.id}`);
    return {
      channelId: captured.id,
      listMode: Array.isArray(captured.enabledModels) ? 'list' : 'auto',
      fetchedAt: value.fetchedAt,
      pricingAvailable: value.pricingAvailable,
      pricingError: value.pricingError,
      entries: value.entries.map(({ thinkingLevelMap, ...e }) => ({   // the map is wire detail; the UI gets the same data under its own name
        ...e,
        // Own-property read: built-in names such as `toString` are legal model ids.
        automaticThinkingLevelMap: thinkingLevelMap ?? null,
        override: structuredClone(Object.hasOwn(captured.models ?? {}, e.id) ? captured.models[e.id] : {}),
        automaticContextWindow: e.automaticContextWindow ?? conn.defaultContextWindow,
        automaticMaxTokens: e.automaticMaxTokens ?? conn.defaultMaxTokens,
        lastTest: tester.last(captured.id, e.id) ?? null,
        effectiveContextWindow: e.contextWindow ?? conn.defaultContextWindow,
        effectiveMaxTokens: e.maxTokens ?? conn.defaultMaxTokens,
      })),
    };
  };

  const snapshot = async ({ force = false } = {}) => {
    const state = channels();
    const revision = channelService.revision();
    const services = new Map(state.list.map((c) => [c.id, balanceOf(c.id)]));
    // The first channel's list rides on the snapshot; every other channel is read
    // on demand, so opening this page does not fetch every gateway catalog.
    const first = state.list[0];
    let models = null;
    try {
      models = first ? await modelsOf(first, { force }) : null;
    } catch (error) {
      reportError(`catalog:${first?.id ?? '-'}`, error);
    }
    // Counts for channels whose catalog is not already on the snapshot. Cached
    // catalogs answer immediately; a slow gateway only delays the count.
    const modelCounts = new Map([[first?.id, models ? models.entries.length : null]]);
    const counts = withTimeout(Promise.all(state.list.map(async (c) => {
      if (modelCounts.has(c.id)) return;
      // A saved list already carries its own count; automatic mode needs the gateway catalog.
      if (Array.isArray(c.enabledModels)) { modelCounts.set(c.id, c.enabledModels.length); return; }
      try { modelCounts.set(c.id, (await catalogFor(structuredClone(c))).entries.length); } catch { /* leave the count unknown */ }
    })), 4_000);
    // Load balances for channels not yet queried, without blocking the page for long.
    await withTimeout(Promise.allSettled(state.list.map((c) => {
      const svc = services.get(c.id);
      return svc.state.status === 'idle' || force ? svc.refresh({ force }) : undefined;
    })), 5_000);
    await counts;
    const list = await Promise.all(state.list.map(async (c) => ({
      id: c.id,
      name: c.name,
      baseURL: gatewayRoot(c.baseURL),
      keyName: c.apiKeyEnv,
      keyConfigured: await keyConfigured(c),
      overrideCount: Object.keys(c.models ?? {}).length,
      // Saved list size; null in automatic mode (count comes from the gateway on demand).
      enabledCount: Array.isArray(c.enabledModels) ? c.enabledModels.length : null,
      modelCount: modelCounts.get(c.id) ?? null,
      providerId: c.providerId,
      routeStatus: routeHandles.has(c.providerId) ? 'registered' : 'pending',
      routeError: routeErrors.get(c.providerId) ?? null,
      balance: services.get(c.id).state,
    })));
    return {
      plugin: 'dsh-newapi-provider',
      revision,
      channels: list,
      route: {
        displayName: current().displayName || 'New API',
        routes: state.list.map((c) => ({
          provider: c.providerId,
          channelId: c.id,
          displayName: displayNameFor(c),
          status: routeHandles.has(c.providerId) ? 'registered' : 'pending',
          error: routeErrors.get(c.providerId) ?? null,
        })),
        directoryError,
      },
      defaults: {
        contextWindow: first ? connFor(first).defaultContextWindow : DEFAULT_CONTEXT_WINDOW,
        maxTokens: first ? connFor(first).defaultMaxTokens : DEFAULT_MAX_TOKENS,
      },
      modelsDev: modelsDev.status(),
      models,
      media: media.status(),
      errors: Object.fromEntries(errors),
    };
  };

  // ------------------------------------------------------------- agent tools

  ctx.inject(['tools'], (tctx) => {
    registerAsyncEffect(tctx, () => tctx.tools.register({
      name: 'newapi_channels',
      description: 'Manage New API channels: list, test a free connection, save, delete, edit model overrides, read the gateway model list, edit the channel\'s enabled-model list, or test one model. Every channel is served at the same time on its own provider route. Writes persist shared settings; deleting keeps credentials. List returns bounded metadata and revision, never tokens.',
      parameters: {
        type: 'object', properties: {
          action: { type: 'string', enum: ['list', 'test', 'save', 'delete', 'model_override', 'models_discover', 'models_set', 'model_test'] },
          revision: { type: 'integer', description: 'Required for writes (save, delete, model_override, models_set); use revision from list/state. Stale writes fail.' },
          id: { type: 'string', description: 'Channel for test/save/delete. Omit for a new save.' },
          channelId: { type: 'string', description: 'Required for model_override, models_discover, models_set and model_test; never inferred.' },
          add: { type: 'array', items: { type: 'string' }, description: 'models_set: local model ids to enable on the channel.' },
          remove: { type: 'array', items: { type: 'string' }, description: 'models_set: model ids to remove from the channel list.' },
          clearOverrides: { type: 'boolean', description: 'models_set: also delete overrides of removed models (kept by default).' },
          overrides: { type: 'object', description: 'models_set: per-model override patches keyed by model id, e.g. {"id":{"custom":true}} for a model the gateway does not list.' },
          confirmPaid: { type: 'boolean', description: 'model_test: required for image and video models, whose test is a real billed generation. Chat tests send one tiny billed request.' },
          name: { type: 'string' },
          baseURL: { type: 'string', description: 'HTTP(S) gateway. Changing an address requires an explicit token.' },
          apiKey: { type: 'string', description: 'Token for save/test; omit to keep the token on an unchanged gateway.' },
          modelId: { type: 'string', description: 'Stable local model id, not its upstreamModel mapping.' },
          override: { type: 'object', description: 'Partial override: name (display only), upstreamModel (wire id; local id stays unchanged), kind, hidden, contextWindow, maxTokens, input, reasoning, reasoningLevels, protocol, custom. Null removes a field; omitted fields are preserved.' },
          reset: { type: 'boolean', description: 'Remove all overrides for this model.' },
          offset: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 100 },
        }, required: ['action'], additionalProperties: false,
      },
      isConcurrencySafe: (args) => ['list', 'test', 'models_discover'].includes(args?.action),
      output: { schema: { type: 'object' }, render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 1) }] },
      async execute(args) {
        if (args.action === 'models_discover') return JSON.parse(JSON.stringify(await discoverModels(args)));
        if (args.action === 'model_test') {
          const { preview, ...rest } = await tester.run(args);
          return JSON.parse(JSON.stringify({ ...rest, ...(preview?.type === 'video-task' ? { preview } : {}) }));
        }
        return channelService.run(args.action, args);
      },
    }));
    registerAsyncEffect(tctx, () => tctx.tools.register({
      name: 'newapi_balance',
      description: 'Show the remaining balance and usage of New API channels\' tokens (and of the account, when the token allows user-quota queries). Every channel is reported unless channelId is given. Read-only and free.',
      parameters: {
        type: 'object',
        properties: {
          channelId: { type: 'string', description: 'Channel to report; omit to report every channel. Ids come from newapi_channels list.' },
          refresh: { type: 'boolean', description: 'Query the gateway now instead of using the last cached value.' },
        },
        additionalProperties: false,
      },
      isConcurrencySafe: () => true,
      output: {
        schema: { type: 'object' },
        render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 1) }],
      },
      async execute(args) {
        const requested = typeof args?.channelId === 'string' && args.channelId ? channelById(args.channelId) : undefined;
        const list = requested ? [requested] : channels().list;
        const report = await Promise.all(list.map(async (ch) => {
          const svc = balanceOf(ch.id);
          const state = args?.refresh || svc.state.status === 'idle' ? await svc.refresh({ force: Boolean(args?.refresh) }) : svc.state;
          return { id: ch.id, name: ch.name, baseURL: gatewayRoot(ch.baseURL), providerId: ch.providerId, ...state };
        }));
        return JSON.parse(JSON.stringify({ channels: report }));
      },
    }));
    registerAsyncEffect(tctx, () => tctx.tools.register({
      name: 'newapi_models',
      description: 'List local model ids and upstreamModel mappings on a New API channel (the first channel unless channelId is given) with their kind (chat, image, video, …), context window, output cap, input modalities (text, image, audio, video, pdf), reasoning support and pricing ratios. Each channel keeps its own model list and its own provider route. Read-only and free.',
      parameters: {
        type: 'object',
        properties: {
          channelId: { type: 'string', description: 'Channel to list; defaults to the first channel. Ids come from newapi_channels list.' },
          kind: { type: 'string', enum: KINDS, description: 'Only list models of this kind.' },
          refresh: { type: 'boolean', description: 'Re-read the gateway model list now.' },
        },
        additionalProperties: false,
      },
      isConcurrencySafe: () => true,
      output: {
        schema: { type: 'object' },
        render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 1) }],
      },
      async execute(args) {
        const bound = captureById(args?.channelId);
        const conn = bound.connection;
        const value = await bound.catalog({ force: Boolean(args?.refresh) });
        const entries = value.entries.filter((e) => !args?.kind || e.kind === args.kind);
        return JSON.parse(JSON.stringify({
          channel: { id: conn.channelId, name: conn.channelName },
          gateway: gatewayRoot(conn.baseURL),
          providerId: conn.providerId,
          routeStatus: routeHandles.has(conn.providerId) ? 'registered' : 'pending',
          modelsDev: modelsDev.status(),
          pricingAvailable: value.pricingAvailable,
          models: entries.map((e) => ({
            id: e.id,
            upstreamModel: e.upstreamModel ?? e.id,
            kind: e.kind,
            hidden: e.hidden,
            contextWindow: e.contextWindow ?? null,
            effectiveContextWindow: e.contextWindow ?? conn.defaultContextWindow,
            maxTokens: e.maxTokens ?? null,
            input: e.input,
            output: e.outputModalities,
            reasoning: e.reasoning,
            reasoningLevels: e.reasoningLevels ?? [],
            reasoningLevelsSource: e.reasoningLevelsSource ?? 'default',
            endpointTypes: e.endpointTypes,
            vendor: e.vendor,
            pricing: e.pricing,
            capabilitySource: e.source.modelsDev ? `models.dev ${e.source.modelsDev.provider} (${e.source.modelsDev.match})` : 'none',
            overridden: e.source.overridden,
          })),
        }));
      },
    }));
  });

  // ----------------------------------------------------------------- web API

  const routes = {
    'GET state': async () => snapshot(),
    'POST refresh': async () => {
      catalogs.invalidate();
      await modelsDev.refresh({ force: true });
      return snapshot({ force: true });
    },
    ...Object.fromEntries([
      ['channel/test', 'test'], ['channel/save', 'save'],
      ['channel/delete', 'delete'], ['model/override', 'model_override'], ['models/set', 'models_set'],
    ].map(([route, action]) => [`POST ${route}`, async (body) => {
      const result = await channelService.run(action, body);
      return action === 'test' ? result : snapshot();
    }])),
    'POST models/discover': async (body) => discoverModels(body),
    // Any channel's own list, so every gateway is managed independently.
    'POST models/list': async (body) => {
      if (typeof body?.channelId !== 'string' || !body.channelId) throw Object.assign(new Error('channelId is required'), { status: 400 });
      return modelsOf(channelById(body.channelId), { force: body.force === true });
    },
    'POST model/test': async (body) => tester.run(body),
  };

  // `inject` waits for the service: on a cold start the HTTP server may be created
  // after this plugin applies, and reading it once here would silently skip routing
  // (the settings page then fails with 404 until the plugin is reloaded).
  ctx.inject(['webServer'], (wctx) => {
    wctx.effect(() => wctx.webServer.register({
      kind: 'prefix',
      path: ROUTE_PREFIX.slice(0, -1),
      async handler(req, res) {
        // `/api/` belongs to DSH itself, so this plugin uses its own prefix and
        // applies the same session check the Web UI's own requests pass.
        const conn = ctx.get('connection');
        if (!conn || conn.requestRejection(req) !== undefined) return sendJson(res, 403, { error: 'forbidden' });
        const url = new URL(req.url ?? '/', 'http://local');
        try {
          if (url.pathname.startsWith(ROUTE_PREFIX)) {
            const sub = url.pathname.slice(ROUTE_PREFIX.length);
            const route = routes[`${req.method} ${sub}`];
            if (route) {
              let body = {};
              if (req.method === 'POST') {
                // A JSON content type forces a CORS preflight for cross-site callers.
                if (!/^application\/json\b/i.test(req.headers['content-type'] ?? '')) return sendJson(res, 415, { error: 'expected application/json' });
                body = await readBody(req);
              }
              return sendJson(res, 200, await route(body));
            }
          }
          if (await media.handle(req, res, url)) return;
          return sendJson(res, 404, { error: 'not found' });
        } catch (error) {
          return sendJson(res, error?.status ?? 500, { error: String(error?.message ?? error) });
        }
      },
    }));
  });
}
