import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { Catalog } from '../lib/catalog.js';
import { BalanceService } from '../lib/balance.js';

const deferred = () => { let resolve; const promise = new Promise((r) => { resolve = r; }); return { promise, resolve }; };
const modelsDev = { refresh: async () => {}, match: () => undefined };
const conn = (id, name) => ({ channelId: id, baseURL: 'https://same.example', keyFingerprint: 'same', overridesRev: id, headers: {}, overrides: { m: { name } } });

test('catalog cache partitions channels and override generations even on the same gateway/token', async () => {
  const cat = new Catalog({ modelsDev });
  let calls = 0;
  cat.fetchGateway = async () => { calls++; return { models: { data: [{ id: 'm' }] } }; };
  const a = conn('a', 'Model A'), b = conn('b', 'Model B');
  assert.equal((await cat.get(a, 'fake')).entries[0].name, 'Model A');
  assert.equal((await cat.get(b, 'fake')).entries[0].name, 'Model B');
  assert.equal(calls, 2);
  assert.equal(cat.peek(a).entries[0].name, 'Model A');
  assert.equal(cat.peek({ ...a, overridesRev: 'new' }), undefined);
});

test('invalidated in-flight catalog cannot repopulate a newer cache', async () => {
  const cat = new Catalog({ modelsDev }); const gate = deferred();
  cat.fetchGateway = () => gate.promise;
  const a = conn('a', 'A'); const pending = cat.get(a, 'fake');
  cat.invalidate(); gate.resolve({ models: { data: [{ id: 'm' }] } });
  assert.equal((await pending).entries[0].name, 'A'); assert.equal(cat.peek(a), undefined);
});

test('balance captures base and credential together before awaiting credential resolution', async (t) => {
  const originalFetch = globalThis.fetch; t.after(() => { globalThis.fetch = originalFetch; });
  const gate = deferred(); let target = 'a'; const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url, key: options.headers.authorization });
    const data = url.endsWith('/api/status') ? { quota_per_unit: 1, quota_display_type: 'TOKENS' } : { total_available: 12, total_used: 3, total_granted: 15 };
    return new Response(JSON.stringify({ data }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  const svc = new BalanceService({ capture: () => {
    const captured = target;
    return { connection: { baseURL: `https://${captured}.example` }, requireApiKey: async () => { await gate.promise; return `key-${captured}`; } };
  } });
  const pending = svc.refresh(); target = 'b'; gate.resolve();
  const value = await pending;
  assert.equal(value.token.remaining, 12);
  assert.equal(calls.length, 2);
  assert.equal(calls[1].url, 'https://a.example/api/usage/token/'); assert.equal(calls[1].key, 'Bearer key-a');
});

// Load our adapter source with small SDK spies: no DSH imports, paid requests,
// copied official implementation, or runtime modifications are needed.
async function adapterUnderTest() {
  let source = await fs.readFile(new URL('../lib/adapter.js', import.meta.url), 'utf8');
  source = source.replace(/^import .*;\n/gm, '');
  const catalogURL = new URL('../lib/catalog.js', import.meta.url).href;
  const utilURL = new URL('../lib/util.js', import.meta.url).href;
  const prefix = `import { chatModels, DEFAULT_CONTEXT_WINDOW, DEFAULT_MAX_TOKENS } from '${catalogURL}';
import { gatewayRoot, openaiBase } from '${utilURL}';
class LlmAdapter {}
class LlmError extends Error {}
const ReasoningEffortId = x => x;
const createProvider = x => x;
const openAICompletionsApi = () => ({}), openAIResponsesApi = () => ({}), anthropicMessagesApi = () => ({}), googleGenerativeAIApi = () => ({});
class PiAiAdapter {
 constructor(host) { this.host = host; }
 async resolveModel(provider, model) { return { provider, id: model }; }
 async prepareCall(provider, model) { return { stream: options => this.stream(options) }; }
 async *stream(options) {
  const profile = this.host.profiles().get(options.provider);
  yield { url: profile.piProvider.models[0].baseUrl, key: await this.host.resolveApiKey(), name: profile.piProvider.models[0].name };
 }
}
`;
  return (await import(`data:text/javascript;base64,${Buffer.from(prefix + source).toString('base64')}`)).NewApiAdapter;
}

/**
 * Host stub with one immutable channel per provider route, like the real host:
 * `connection(providerId)` and `capture(providerId)` resolve that route only.
 * A capture snapshots the route facts before any await, so a later settings
 * change (address, token) must not leak into an in-flight call.
 */
async function adapterFixture(routes) {
  const errors = [];
  const snapshotOf = (provider) => {
    const route = routes[provider];
    if (!route) throw new Error(`no channel is registered for route "${provider}"`);
    // Copy now: the live config may change while the call awaits its key/catalog.
    const snapshot = { ...route, entries: route.entries?.map((entry) => ({ ...entry })) };
    return {
      connection: {
        channelId: route.channelId, providerId: provider, displayName: route.displayName,
        baseURL: route.baseURL, headers: {}, endpointPriority: ['openai'], apiKeyRef: provider,
        retryPolicy: route.retryPolicy,
      },
      requireApiKey: async () => {
        if (snapshot.gate) await snapshot.gate.promise;
        if (snapshot.keyError) throw snapshot.keyError;
        return snapshot.key;
      },
      catalog: async () => {
        if (snapshot.catalogError) throw snapshot.catalogError;
        return { entries: snapshot.entries ?? [] };
      },
    };
  };
  const host = {
    connection: (provider) => snapshotOf(provider).connection,
    capture: (provider) => snapshotOf(provider),
    reportError: (scope, error) => errors.push({ scope, error }),
  };
  const Adapter = await adapterUnderTest();
  return { adapter: new Adapter(host), errors };
}

const entry = (id, name) => ({ id, name, kind: 'chat', input: ['text'], endpointTypes: ['openai'], source: { overridden: [] } });

test('prepared adapter streams stay on their own route while live config changes; a concurrent route uses its own channel', async () => {
  const gate = deferred();
  const routes = {
    'newapi-provider': { channelId: 'a', displayName: 'Channel A', baseURL: 'https://a.example', key: 'key-a', gate, entries: [entry('m-a', 'Model A')] },
    'newapi-provider-b': { channelId: 'b', displayName: 'Channel B', baseURL: 'https://b.example', key: 'key-b', entries: [entry('m-b', 'Model B')] },
  };
  const { adapter } = await adapterFixture(routes);
  const preparing = adapter.prepareCall('newapi-provider', 'm-a');
  // The settings write that lands while route A is still preparing:
  // a different address and a rotated token for that same channel.
  routes['newapi-provider'].baseURL = 'https://a-changed.example';
  routes['newapi-provider'].key = 'key-a-rotated';
  const next = await adapter.prepareCall('newapi-provider-b', 'm-b');
  gate.resolve();
  const original = await preparing;
  // Metadata resolves against the route the call was created for, not a shared one.
  assert.deepEqual(original.model, { provider: 'newapi-provider', id: 'm-a' });
  assert.deepEqual(next.model, { provider: 'newapi-provider-b', id: 'm-b' });
  assert.deepEqual(await adapter.resolveModel('newapi-provider-b', 'm-b'), { provider: 'newapi-provider-b', id: 'm-b' });
  const collect = async (call, provider, model) => { const result = []; for await (const item of call.stream({ provider, model })) result.push(item); return result[0]; };
  assert.deepEqual(await collect(original, 'newapi-provider', 'm-a'), { url: 'https://a.example/v1', key: 'key-a', name: 'Model A' });
  assert.deepEqual(await collect(next, 'newapi-provider-b', 'm-b'), { url: 'https://b.example/v1', key: 'key-b', name: 'Model B' });
  // A stream binds the route named in options, never a global active channel.
  const fresh = []; for await (const item of adapter.stream({ provider: 'newapi-provider-b', model: 'm-b' })) fresh.push(item);
  assert.deepEqual(fresh[0], { url: 'https://b.example/v1', key: 'key-b', name: 'Model B' });
});

test('listModels, providerInfo and providerRetryPolicy resolve the route they are asked about', async () => {
  const retryPolicy = { maxAttempts: 3, backoffMs: 250 };
  const routes = {
    'newapi-provider': { channelId: 'a', displayName: 'Channel A', baseURL: 'https://a.example', key: 'key-a', retryPolicy, entries: [entry('a-chat', 'A chat')] },
    'newapi-provider-b': { channelId: 'b', displayName: 'Channel B', baseURL: 'https://b.example', key: 'key-b', retryPolicy: undefined, entries: [entry('b-chat', 'B chat')] },
  };
  const { adapter, errors } = await adapterFixture(routes);
  assert.deepEqual(await adapter.listModels('newapi-provider'), [{ provider: 'newapi-provider', id: 'a-chat', name: 'A chat', inputModalities: ['text'] }]);
  assert.deepEqual(await adapter.listModels('newapi-provider-b'), [{ provider: 'newapi-provider-b', id: 'b-chat', name: 'B chat', inputModalities: ['text'] }]);
  // An unknown route is empty, never another channel's catalog.
  assert.deepEqual(await adapter.listModels('missing-route'), []);
  assert.deepEqual(adapter.providerInfo('newapi-provider'), { id: 'newapi-provider', name: 'Channel A' });
  assert.deepEqual(adapter.providerInfo('newapi-provider-b'), { id: 'newapi-provider-b', name: 'Channel B' });
  assert.deepEqual(adapter.providerInfo('missing-route'), { id: 'missing-route', name: 'missing-route' });
  assert.deepEqual(adapter.providerRetryPolicy('newapi-provider'), retryPolicy);
  assert.equal(adapter.providerRetryPolicy('newapi-provider-b'), undefined);
  assert.equal(adapter.providerRetryPolicy('missing-route'), undefined);
  // A catalog failure is reported against that route's channel and yields no models.
  routes['newapi-provider'].catalogError = new Error('gateway down');
  assert.deepEqual(await adapter.listModels('newapi-provider'), []);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].scope, 'catalog:a');
  assert.match(errors[0].error.message, /gateway down/);
  // Route B is unaffected by route A's outage.
  assert.deepEqual(await adapter.listModels('newapi-provider-b'), [{ provider: 'newapi-provider-b', id: 'b-chat', name: 'B chat', inputModalities: ['text'] }]);
});
