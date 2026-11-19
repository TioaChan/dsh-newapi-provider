import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { taskChannel } from '../lib/channels.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'newapi-channel-media-'));
process.env.DSH_HOME = home;
const { registerMedia } = await import('../lib/media.js');

function context(asyncRegistration = false) {
  const tools = new Map(), disposers = [], files = [];
  const ctx = {
    logger: { warn() {}, error() {} },
    effect(fn) { const dispose = fn(); if (dispose) disposers.push(dispose); },
    inject(_deps, fn) { fn({ effect: (effect) => ctx.effect(effect), tools: { register(def) { tools.set(def.name, def); const dispose = () => tools.delete(def.name); return asyncRegistration ? Promise.resolve(dispose) : dispose; } } }); },
    get(name) { return name === 'attachments' ? {
      saveFile: async (input) => { const ref = { attachmentId: 'video', bytes: input.data.length, name: input.name }; files.push(ref); return ref; },
      saveImage: async (input) => ({ attachmentId: 'image', bytes: input.data.length, mediaType: input.mediaType }),
      fileHostPath: () => undefined,
    } : undefined; },
  };
  return { ctx, tools, files, dispose: async () => { await Promise.all(disposers.splice(0).map((d) => d())); } };
}

const channelList = () => [
  { id: 'a', name: 'A', baseURL: 'https://a.example', apiKeyEnv: 'A' },
  { id: 'b', name: 'B', baseURL: 'https://b.example', apiKeyEnv: 'B' },
];
const catalog = async () => ({ entries: [{ id: 'sora-test', kind: 'video', hidden: false }, { id: 'img-test', kind: 'image', hidden: false }] });

/** Host stub of the multi-active contract: capture(undefined) is the first channel, not an active one. */
function mediaHost(list) {
  const bound = (ch) => ({
    connection: { channelId: ch.id, baseURL: ch.baseURL, apiKeyName: ch.apiKeyEnv, headers: {} },
    requireApiKey: async () => `key-${ch.id}`,
    catalog,
  });
  return {
    capture: (channelId) => bound(channelId ? list.find((c) => c.id === channelId) : list[0]),
    captureTask: (task) => bound(taskChannel(task, list)),
    reportError() {},
  };
}

test('video submit, restart and polling keep the originating channel even when the first channel changes', async (t) => {
  const original = globalThis.fetch;
  const calls = [];
  t.after(() => { globalThis.fetch = original; });
  globalThis.fetch = async (url, options) => {
    calls.push({ url, method: options.method ?? 'GET', key: options.headers.authorization });
    assert.ok(url.startsWith('https://a.example/'), 'must only contact the captured origin channel A');
    assert.equal(options.headers.authorization, 'Bearer key-a');
    if (options.method === 'POST') return Response.json({ id: 'remote-same-id', status: 'queued' });
    if (url.endsWith('/content')) return new Response(Buffer.alloc(2048, 1), { headers: { 'content-type': 'video/mp4' } });
    return Response.json({ id: 'remote-same-id', status: 'completed', progress: 100 });
  };
  const list = channelList();
  const host = mediaHost(list);
  const first = context(true);
  registerMedia(first.ctx, host);
  // No channelId: the first channel (A) is used.
  const accepted = await first.tools.get('newapi_video_generate').execute({ prompt: 'offline test' }, { callId: 'call', signal: AbortSignal.timeout(1000) });
  await first.dispose();
  assert.equal(first.tools.size, 0, 'async registration disposers are awaited');
  // After the restart the first channel is B, so a "default channel" poller would
  // now reach the wrong gateway; the task must stay bound to its origin instead.
  list.reverse();
  assert.equal(list[0].id, 'b');
  const resumed = context(); t.after(() => resumed.dispose());
  registerMedia(resumed.ctx, host);
  const status = await resumed.tools.get('newapi_video_status').execute({ taskId: accepted.taskId });
  assert.equal(status.tasks[0].status, 'completed');
  assert.equal(status.tasks[0].origin.channelId, 'a');
  assert.equal(status.tasks[0].origin.keyName, 'A');
  assert.equal(resumed.files.length, 1);
  assert.equal(calls.filter((c) => c.method === 'POST').length, 1);
  assert.equal(calls.length, 3);
});

test('media tools default to the first channel and honor an explicit channelId; a video task binds its origin', async (t) => {
  const original = globalThis.fetch;
  const calls = [];
  t.after(() => { globalThis.fetch = original; });
  const png = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]).toString('base64');
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, method: options.method ?? 'GET', key: options.headers?.authorization });
    if (options.method === 'POST' && url.endsWith('/v1/videos')) return Response.json({ id: 'remote-explicit-video', status: 'queued' });
    if (url.endsWith('/content')) return new Response(Buffer.alloc(4096, 1), { headers: { 'content-type': 'video/mp4' } });
    if (url.includes('/v1/images/')) return Response.json({ data: [{ b64_json: png }] });
    return Response.json({ id: 'remote-explicit-video', status: 'completed', progress: 100 });
  };
  const list = channelList();
  const selected = [];
  const base = mediaHost(list);
  const host = { ...base, capture: (channelId) => { selected.push(channelId); return base.capture(channelId); } };
  const f = context(); t.after(() => f.dispose());
  registerMedia(f.ctx, host);
  const image = f.tools.get('newapi_image_generate');
  const video = f.tools.get('newapi_video_generate');
  const exec = { signal: AbortSignal.timeout(5000), callId: 'offline' };

  await image.execute({ prompt: 'default image' }, exec);
  assert.ok(calls.at(-1).url.startsWith('https://a.example/'), 'image generation defaults to the first channel');
  assert.equal(calls.at(-1).key, 'Bearer key-a');
  await image.execute({ prompt: 'explicit image', channelId: 'b' }, exec);
  assert.ok(calls.at(-1).url.startsWith('https://b.example/'), 'an explicit channelId picks that gateway for images');
  assert.equal(calls.at(-1).key, 'Bearer key-b');

  const accepted = await video.execute({ prompt: 'explicit video', channelId: 'b' }, exec);
  const submit = calls.find((c) => c.method === 'POST' && c.url.endsWith('/v1/videos'));
  assert.ok(submit.url.startsWith('https://b.example/'), 'an explicit channelId picks that gateway for video');
  assert.equal(submit.key, 'Bearer key-b');
  const status = await f.tools.get('newapi_video_status').execute({ taskId: accepted.taskId });
  assert.equal(status.tasks[0].status, 'completed');
  assert.equal(status.tasks[0].origin.channelId, 'b');
  // Every request after the image edit went to channel B, never the default channel A.
  const hosts = calls.map((c) => new URL(c.url).host);
  assert.deepEqual(hosts, ['a.example', 'b.example', 'b.example', 'b.example', 'b.example']);
  // Explicit selection reached the host; polling used captureTask(origin), not capture(args.channelId).
  assert.deepEqual(selected, [undefined, 'b', 'b']);
});
