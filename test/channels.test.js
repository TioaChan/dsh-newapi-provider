import test from 'node:test';
import assert from 'node:assert/strict';
import { ChannelService, resolveChannels, channelRouteId, routeIdBase, mergeModelOverride, taskChannel, registerAsyncEffect } from '../lib/channels.js';

const BASE = 'newapi-provider';
const channel = (id, extra = {}) => ({ id, name: id.toUpperCase(), baseURL: `https://${id}.example`, apiKeyEnv: id.toUpperCase(), models: {}, ...extra });

function fixture(raw = { channels: [channel('a'), channel('b')], activeChannel: 'a', providerId: BASE, unrelated: { keep: true } }) {
  let value = structuredClone(raw), revision = 7;
  const keys = new Map([['A', 'secret-a'], ['B', 'secret-b']]);
  const calls = [];
  let fail = false, noop = false;
  const settings = {
    describe: () => [{ ns: 'studio', revision }],
    async mutate(ns, ops, expected) {
      calls.push({ ns, ops, expected });
      assert.equal(expected, revision);
      if (fail) throw new Error('persist failed');
      if (noop) return;
      value = structuredClone(value);
      for (const op of ops) { if (op.op === 'set') value[op.path[0]] = structuredClone(op.value); else delete value[op.path[0]]; }
      revision++;
    },
  };
  const host = {
    ns: 'studio', settings: () => settings, current: () => value,
    keyConfigured: async (c) => keys.has(c.apiKeyEnv), keyFor: async (c) => keys.get(c.apiKeyEnv),
    storeKey: async (name, key) => { keys.set(name, key); }, unsetKey: async (name) => keys.delete(name),
    testGateway: async (args) => { calls.push(args); return { ok: true, baseURL: args.baseURL }; },
  };
  return {
    service: new ChannelService(host), keys, calls, current: () => value,
    /** Route ids as the host would actually serve them, after the write. */
    routes: () => resolveChannels(value).list.map((c) => [c.id, c.providerId]),
    setFail: () => { fail = true; }, setNoop: () => { noop = true; },
  };
}

test('legacy resolver keeps the single-gateway token, headers and overrides and owns the base route', () => {
  const state = resolveChannels({ baseURL: 'http://127.0.0.1:3300/v1', apiKeyEnv: 'MY_LOCAL_TOKEN', headers: { x: 'keep' }, models: { m: { custom: true, protocol: 'anthropic' } } });
  assert.equal(state.legacy, true);
  assert.equal(state.list.length, 1);
  const [only] = state.list;
  assert.equal(only.id, 'default');
  assert.equal(only.providerId, BASE);
  assert.equal(only.apiKeyEnv, 'MY_LOCAL_TOKEN');
  assert.equal(only.models.m.protocol, 'anthropic');
  // The one-active resolver's `active` is gone: every channel is a route now.
  assert.ok(!('active' in state));
  // Old fallback behavior: with no activeChannel the first channel owns the base route.
  assert.deepEqual(resolveChannels({ channels: [{ id: 'a' }, { id: 'b' }] }).list.map((c) => c.providerId), [BASE, `${BASE}-b`]);
  assert.throws(() => resolveChannels({ channels: [{ id: 'a' }, { id: 'a' }] }), /duplicate/);
});

test('route id derivation: legacy owner keeps the base, other channels append their id, stored ids win', () => {
  assert.equal(routeIdBase({}), BASE);
  assert.equal(routeIdBase({ providerId: 'studio' }), 'studio');
  const list = [{ id: 'a' }, { id: 'b' }, { id: 'c' }];
  assert.equal(channelRouteId(list[0], { activeChannel: 'b' }, list), `${BASE}-a`);
  assert.equal(channelRouteId(list[1], { activeChannel: 'b' }, list), BASE);
  assert.equal(channelRouteId(list[2], { activeChannel: 'b' }, list), `${BASE}-c`);
  // A stored route id beats the derivation, including for the legacy owner.
  assert.equal(channelRouteId({ id: 'b', providerId: 'pinned' }, { activeChannel: 'b' }, list), 'pinned');

  // The configured base prefixes the legacy owner and every derived route.
  assert.deepEqual(resolveChannels({ channels: [{ id: 'a' }, { id: 'b' }], activeChannel: 'a', providerId: 'studio' }).list.map((c) => c.providerId), ['studio', 'studio-b']);
  // The old single-gateway form keeps the base id.
  assert.equal(resolveChannels({ baseURL: 'https://a.example' }).list[0].providerId, BASE);
  // No activeChannel: the first channel is the legacy owner.
  assert.deepEqual(resolveChannels({ channels: [{ id: 'a' }, { id: 'b' }] }).list.map((c) => c.providerId), [BASE, `${BASE}-b`]);
  // activeChannel naming the second channel moves the base to it.
  assert.deepEqual(resolveChannels({ channels: [{ id: 'a' }, { id: 'b' }], activeChannel: 'b' }).list.map((c) => c.providerId), [`${BASE}-a`, BASE]);
  // A stale/invalid pointer keeps the base route retired instead of guessing an owner.
  assert.deepEqual(resolveChannels({ channels: [{ id: 'a' }, { id: 'b' }], activeChannel: 'missing' }).list.map((c) => c.providerId), [`${BASE}-a`, `${BASE}-b`]);
  // Stored ids are honored as written.
  assert.deepEqual(resolveChannels({ channels: [{ id: 'a', providerId: 'frozen-a' }, { id: 'b' }], activeChannel: 'a' }).list.map((c) => c.providerId), ['frozen-a', `${BASE}-b`]);
  // Two stored ids colliding fail loudly instead of shadowing a route.
  assert.throws(() => resolveChannels({ channels: [{ id: 'a', providerId: 'dup' }, { id: 'b', providerId: 'dup' }] }), /duplicate channel route id/);
  // A derived id colliding with a stored one is a duplicate too.
  assert.throws(() => resolveChannels({ channels: [{ id: 'c', providerId: `${BASE}-b` }, { id: 'b' }] }), /duplicate channel route id/);
});

test('list is bounded, credential-free, route-aware and supplies CAS revision', async () => {
  const f = fixture(); const result = await f.service.run('list', { limit: 1 });
  assert.equal(result.channels.length, 1); assert.equal(result.nextOffset, 1); assert.equal(result.total, 2);
  assert.equal(result.revision, 7); assert.equal(result.channels[0].keyConfigured, true);
  assert.ok(!JSON.stringify(result).includes('secret-'));
  // No active/channel switch is reported anymore; each channel carries its route.
  assert.ok(!('activeChannel' in result));
  assert.ok(!('active' in result.channels[0]));
  assert.equal(result.channels[0].providerId, BASE);
  assert.equal(result.channels[0].name, 'A');
  assert.equal(result.channels[0].baseURL, 'https://a.example');
  assert.equal(result.channels[0].keyName, 'A');
  assert.equal(result.channels[0].overrideCount, 0);
  assert.equal(result.channels[0].enabledCount, null);
  const second = await f.service.run('list', { offset: 1 });
  assert.equal(second.channels[0].id, 'b');
  assert.equal(second.channels[0].providerId, `${BASE}-b`);
});

test('writes require revision and serialize competing stale updates', async () => {
  const f = fixture();
  // `activate` was removed with the one-active model.
  await assert.rejects(f.service.run('activate', { id: 'b', revision: 7 }), (error) => error.status === 400 && /unknown channel action/.test(error.message));
  await assert.rejects(f.service.run('save', { id: 'b', name: 'B2', baseURL: 'https://b.example' }), /revision/);
  const outcomes = await Promise.allSettled([
    f.service.run('save', { id: 'b', revision: 7, name: 'B2', baseURL: 'https://b.example' }),
    f.service.run('delete', { id: 'a', revision: 7 }),
  ]);
  assert.equal(outcomes[0].status, 'fulfilled'); assert.equal(outcomes[1].reason.status, 409);
  assert.equal(f.current().channels.length, 2);
  assert.equal(f.current().channels[1].name, 'B2');
  // The winning write froze every route id and retired the legacy pointer.
  assert.equal(f.current().activeChannel, undefined);
  assert.deepEqual(f.current().channels.map((c) => c.providerId), [BASE, `${BASE}-b`]);
  assert.deepEqual(f.routes(), [['a', BASE], ['b', `${BASE}-b`]]);
  assert.equal(f.current().providerId, BASE); assert.deepEqual(f.current().unrelated, { keep: true });
  assert.equal(f.calls[0].expected, 7);
});

test('legacy single-gateway migration keeps its token and overrides, freezes the base route and retires legacy fields', async () => {
  const f = fixture({ baseURL: 'https://a.example', apiKeyEnv: 'A', models: { m: { custom: true } }, headers: { 'x-local': 'keep' }, providerId: BASE });
  await f.service.run('save', { id: 'default', revision: 7, name: 'Local', baseURL: 'https://a.example' });
  const [only] = f.current().channels;
  assert.equal(only.id, 'default');
  assert.equal(only.providerId, BASE);
  assert.equal(only.apiKeyEnv, 'A');
  assert.deepEqual(only.models, { m: { custom: true } });
  assert.equal(f.current().headers['x-local'], 'keep');
  for (const key of ['baseURL', 'apiKeyEnv', 'models', 'activeChannel']) assert.equal(f.current()[key], undefined, `${key} must be retired`);
  assert.equal(f.keys.get('A'), 'secret-a');
  assert.deepEqual(f.routes(), [['default', BASE]]);
});

test('save stages a unique credential; failed persistence cannot rotate a live key', async () => {
  const f = fixture(); f.setFail();
  await assert.rejects(f.service.run('save', { id: 'a', revision: 7, name: 'A', baseURL: 'https://a.example', apiKey: 'new-secret' }), /persist failed/);
  assert.equal(f.keys.size, 2); assert.equal(f.keys.get('A'), 'secret-a');
  assert.equal(f.current().channels[0].apiKeyEnv, 'A');
});

test('save creates a channel on its own derived route; later saves keep every route stable', async () => {
  const f = fixture();
  // A stray legacy `activate` argument has no effect: every channel is always routed.
  const saved = await f.service.run('save', { revision: 7, name: 'C', baseURL: 'https://c.example/v1', apiKey: 'secret-c', activate: true });
  assert.equal(saved.providerId, `${BASE}-${saved.channelId}`);
  const created = f.current().channels.find((c) => c.id === saved.channelId);
  assert.equal(created.baseURL, 'https://c.example');
  assert.equal(created.providerId, saved.providerId);
  assert.equal(f.keys.get(created.apiKeyEnv), 'secret-c');
  assert.ok(!JSON.stringify(saved).includes('secret-c'));
  // One write retires the legacy pointer and freezes all three routes.
  assert.equal(f.current().activeChannel, undefined);
  assert.deepEqual(f.routes(), [['a', BASE], ['b', `${BASE}-b`], [saved.channelId, saved.providerId]]);

  // Re-saving the new channel keeps its derived route.
  const again = await f.service.run('save', { id: saved.channelId, revision: 8, name: 'C2', baseURL: 'https://c.example' });
  assert.equal(again.providerId, saved.providerId);
  assert.deepEqual(f.routes(), [['a', BASE], ['b', `${BASE}-b`], [saved.channelId, saved.providerId]]);
  // Re-saving the legacy owner keeps the base route.
  const owner = await f.service.run('save', { id: 'a', revision: 9, name: 'A2', baseURL: 'https://a.example' });
  assert.equal(owner.providerId, BASE);
  assert.deepEqual(f.routes(), [['a', BASE], ['b', `${BASE}-b`], [saved.channelId, saved.providerId]]);
  // No response field talks about an active channel anymore.
  assert.equal(again.activeChannel, undefined);
});

test('delete retires the base route instead of moving it and keeps at least one channel', async () => {
  const f = fixture();
  // The legacy owner had not persisted a route id yet: deleting it must not promote b to the base route.
  await f.service.run('delete', { id: 'a', revision: 7 });
  assert.deepEqual(f.routes(), [['b', `${BASE}-b`]]);
  assert.equal(f.current().activeChannel, undefined);
  await assert.rejects(f.service.run('delete', { id: 'b', revision: 8 }), /at least one/);
  assert.deepEqual(f.routes(), [['b', `${BASE}-b`]]);
  assert.equal(f.keys.get('B'), 'secret-b');
});

test('settings mutation must update live config before reporting success', async () => {
  const f = fixture(); f.setNoop();
  await assert.rejects(f.service.run('save', { id: 'a', revision: 7, name: 'Renamed', baseURL: 'https://a.example' }), /live channel update/);
  // Nothing was persisted, so even the frozen route id is still absent.
  assert.equal(f.current().channels[0].providerId, undefined);
});

test('model edits target channelId and preserve protocol, custom, unknown fields and explicit false', async () => {
  const f = fixture();
  f.current().channels[0].models.m = { protocol: 'anthropic', custom: true, hidden: true, reasoningLevels: ['high'], future: { keep: true }, maxTokens: 10 };
  await assert.rejects(f.service.run('model_override', { modelId: 'm', revision: 7, override: {} }), /channelId/);
  await f.service.run('model_override', { channelId: 'a', modelId: 'm', revision: 7, override: { maxTokens: 20, hidden: false, contextWindow: null, future: { keep: true } } });
  assert.deepEqual(f.current().channels[0].models.m, { protocol: 'anthropic', custom: true, hidden: false, reasoningLevels: ['high'], future: { keep: true }, maxTokens: 20 });
  assert.deepEqual(f.current().channels[1].models, {});
  // Editing one channel persists routes for all of them and retires the legacy pointer.
  assert.deepEqual(f.routes(), [['a', BASE], ['b', `${BASE}-b`]]);
  assert.equal(f.current().activeChannel, undefined);
  await f.service.run('model_override', { channelId: 'a', modelId: 'm', revision: 8, reset: true });
  assert.deepEqual(f.current().channels[0].models, {});
  assert.throws(() => mergeModelOverride({}, { maxTokens: -1 }), /invalid/);
  assert.throws(() => mergeModelOverride({}, { future: true }), /unsupported/);
});

test('test is read-only and never forwards a saved key to changed or invalid gateway', async () => {
  const f = fixture();
  await assert.rejects(f.service.run('test', { id: 'a', baseURL: 'https://other.example' }), /changed gateway/);
  await assert.rejects(f.service.run('test', { apiKey: 'secret', baseURL: 'https://user:password@example.com' }), /without credentials/);
  const result = await f.service.run('test', { id: 'a' }); assert.equal(result.ok, true);
  assert.equal(f.calls.length, 1); assert.equal(f.calls[0].apiKey, 'secret-a');
  // A read never migrates: the legacy pointer and derived route ids stay as configured.
  assert.equal(f.current().activeChannel, 'a'); assert.equal(f.current().channels[0].providerId, undefined);
  assert.equal(f.service.revision(), 7);
});

test('video task lookup binds origin and fails closed for legacy, deleted or moved gateways', () => {
  const state = resolveChannels({ channels: [{ id: 'a', baseURL: 'https://a.example', apiKeyEnv: 'ROTATED' }, { id: 'b', baseURL: 'https://b.example', apiKeyEnv: 'B' }], activeChannel: 'b' });
  const task = { origin: { channelId: 'a', baseURL: 'https://a.example', keyName: 'ORIGINAL_A' } };
  assert.equal(taskChannel(task, state.list).apiKeyEnv, 'ORIGINAL_A');
  assert.equal(taskChannel(task, state.list).baseURL, 'https://a.example');
  assert.throws(() => taskChannel({}, state.list), /legacy/);
  assert.throws(() => taskChannel(task, state.list.slice(1)), /removed/);
  assert.throws(() => taskChannel(task, [{ ...state.list[0], baseURL: 'https://other.example' }]), /changed/);
});

test('late async tool registration is disposed even after plugin unload', async () => {
  let cleanup, complete, removed = 0;
  const pending = new Promise((r) => { complete = r; });
  registerAsyncEffect({ effect: (fn) => { cleanup = fn(); } }, () => pending);
  const disposing = cleanup();
  complete(() => { removed++; });
  await disposing;
  assert.equal(removed, 1);
});
