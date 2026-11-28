// Offline verification: node --test tests/test_pi_contract.mjs (Node >= 22.13).
// Built-in TS stripping and a TypeBox registration stub avoid installing Pi dependencies.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { stripTypeScriptTypes } from 'node:module';
import { readFile, writeFile, mkdir, mkdtemp, rm, stat } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';

const sourceUrl = new URL('../.pi/extensions/qwen-asr.ts', import.meta.url);
const original = await readFile(sourceUrl, 'utf8');
const source = stripTypeScriptTypes(original)
  .replace('import { Type } from "@sinclair/typebox";', 'const Type = new Proxy({}, { get: (_, method) => (...args) => ({ method, args }) });')
  .replaceAll('import.meta.url', JSON.stringify(sourceUrl.href));
const extension = await import(`data:text/javascript;base64,${Buffer.from(source + '\nexport { parseAsrJson, segmentsToSrt, runBatch, defaultOptions, parseCommandArgs, buildOutputPaths, completionIdentity, hasValidCompletion };').toString('base64')}`);
const cases = JSON.parse(await readFile(new URL('./fixtures/asr_cases.json', import.meta.url), 'utf8'));

for (const fixture of cases) {
  test(`shared contract: ${fixture.name}`, () => {
    if (fixture.error) assert.throws(() => extension.parseAsrJson(fixture.payload), /protocol error/i);
    else {
      const actual = extension.parseAsrJson(fixture.payload);
      assert.equal(actual.text, fixture.text);
      assert.deepEqual(actual.segments, fixture.segments);
    }
  });
}

test('nested wrappers, strict timestamps and deterministic fallback', () => {
  assert.equal(extension.parseAsrJson({ result: { data: { output: { text: ' hello ' } } } }).text, 'hello');
  for (const payload of [{}, { segments: [] }, { segments: [{ start: 0, text: 'bad' }] }, { words: [{ start: '0', end: 1, word: 'bad' }] }, { status: 'failed', text: '' }]) {
    assert.throws(() => extension.parseAsrJson(payload), /protocol error/i);
  }
  assert.equal(extension.segmentsToSrt([], 'hello'), '1\n00:00:00,000 --> 00:00:05,000\nhello\n');
  assert.deepEqual(extension.parseAsrJson({ words: [{ start: 0, end: 0.5, word: 'Hello ' }, { start: 0.5, end: 1, word: 'world.' }] }).segments, [{ start: 0, end: 1, text: 'Hello world.' }]);
});

const opts = (inputPath, extra = {}) => ({
  inputPath, baseUrl: 'http://asr.invalid', includeWords: false, mode: 'sync', concurrency: 3,
  overwrite: false, recursive: true, saveJson: false, keepRemoteTask: false,
  asyncDurationSec: 150, asyncSizeMb: 3, pollIntervalMs: 1, taskTimeoutMs: 10000,
  ...extra,
});
const transcript = { text: 'hello', segments: [{ start: 0, end: 1, text: 'hello' }] };
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
async function sandbox(fn) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'pi-asr-contract-'));
  const originalFetch = globalThis.fetch;
  try { await fn(dir); } finally { globalThis.fetch = originalFetch; await rm(dir, { recursive: true, force: true }); }
}

// These tests execute sequentially because each replaces the process-local fetch transport.
test('marker commit, digest validation, JSON requirement, input/request identity and legacy files', async () => sandbox(async (dir) => {
  const input = path.join(dir, 'lecture.part1.wav');
  await writeFile(input, 'audio');
  let requests = 0;
  globalThis.fetch = async () => { requests++; return json(transcript); };
  const options = opts(input);
  let result = await extension.runBatch(options, dir);
  assert.equal(result[0].status, 'done');
  const paths = extension.buildOutputPaths(input, dir);
  assert.equal(path.basename(paths.txt), 'lecture.part1.txt');
  const marker = JSON.parse(await readFile(paths.marker, 'utf8'));
  assert.equal(marker.schema_version, 1);
  assert.equal(marker.producer, 'pi');
  assert.match(marker.source_fingerprint, /^pi-source-v1:/);
  assert.equal(marker.files.txt.name, 'lecture.part1.txt');
  assert.equal(marker.files.txt.size, 6);
  assert.equal((await extension.runBatch(options, dir))[0].status, 'skipped');
  assert.equal(requests, 1);
  await writeFile(paths.marker, JSON.stringify({ ...marker, producer: 'python' }));
  assert.equal((await extension.runBatch(options, dir))[0].status, 'failed');
  await writeFile(paths.marker, JSON.stringify(marker));
  assert.equal((await extension.runBatch({ ...options, saveJson: true }, dir))[0].status, 'failed');
  assert.equal((await extension.runBatch({ ...options, language: 'zh' }, dir))[0].status, 'failed');
  await writeFile(paths.txt, 'tampered');
  assert.equal((await extension.runBatch(options, dir))[0].status, 'failed');
  assert.equal(requests, 1);
  result = await extension.runBatch({ ...options, overwrite: true, saveJson: true }, dir);
  assert.equal(result[0].status, 'done');
  assert.equal((await extension.runBatch({ ...options, saveJson: true }, dir))[0].status, 'skipped');
  await writeFile(input, 'changed audio');
  assert.equal((await extension.runBatch(options, dir))[0].status, 'failed');
  assert.equal(requests, 2);
  await rm(paths.marker);
  assert.match((await extension.runBatch(options, dir))[0].error, /--overwrite/);
  assert.equal(requests, 2);
}));

test('text-only sync response uses one inference POST and shared five-second fallback', async () => sandbox(async (dir) => {
  const input = path.join(dir, 'plain.wav');
  await writeFile(input, 'audio');
  let requests = 0;
  globalThis.fetch = async () => { requests++; return json({ text: 'hello' }); };
  assert.equal((await extension.runBatch(opts(input), dir))[0].status, 'done');
  assert.equal(requests, 1);
  assert.equal(await readFile(path.join(dir, 'plain.srt'), 'utf8'), '1\n00:00:00,000 --> 00:00:05,000\nhello\n');
}));

test('explicit silence writes zero-byte artifacts and is skipped only with valid marker', async () => sandbox(async (dir) => {
  const input = path.join(dir, 'silent.wav');
  await writeFile(input, 'audio');
  let requests = 0;
  globalThis.fetch = async () => { requests++; return json({ text: '' }); };
  assert.equal((await extension.runBatch(opts(input), dir))[0].status, 'done');
  assert.equal((await stat(path.join(dir, 'silent.txt'))).size, 0);
  assert.equal((await extension.runBatch(opts(input), dir))[0].status, 'skipped');
  assert.equal(requests, 1);
}));

test('all colliding output targets fail before HTTP requests', async () => sandbox(async (dir) => {
  await writeFile(path.join(dir, 'clip.wav'), 'one');
  await writeFile(path.join(dir, 'clip.mp3'), 'two');
  let requests = 0;
  globalThis.fetch = async () => { requests++; throw new Error('unexpected network'); };
  const result = await extension.runBatch(opts(dir), dir);
  assert.equal(result.length, 2);
  assert.ok(result.every((item) => item.status === 'failed' && /冲突/.test(item.error)));
  assert.equal(requests, 0);
}));

test('async cleanup occurs after verified local commit; keep-remote-task preserves task', async () => sandbox(async (dir) => {
  const input = path.join(dir, 'clip.wav');
  await writeFile(input, 'audio');
  const options = opts(input, { mode: 'async' });
  let deletes = 0;
  globalThis.fetch = async (url, init = {}) => {
    if (init.method === 'POST') return json({ data: { output: { task_id: 'task/1' } } });
    if (init.method === 'DELETE') {
      deletes++;
      assert.match(String(url), /task%2F1$/);
      assert.equal(await extension.hasValidCompletion(extension.buildOutputPaths(input, dir), await extension.completionIdentity(input, options), false), true);
      return new Response(null, { status: 204 });
    }
    if (String(url).includes('/subtitle?')) return new Response(null, { status: 404 });
    return json({ data: { output: { status: 'completed', result: transcript } } });
  };
  assert.equal((await extension.runBatch(options, dir))[0].status, 'done');
  assert.equal(deletes, 1);
  assert.equal((await extension.runBatch({ ...options, overwrite: true, keepRemoteTask: true }, dir))[0].status, 'done');
  assert.equal(deletes, 1);
}));

test('cleanup failure preserves successful local result and reports warning', async () => sandbox(async (dir) => {
  const input = path.join(dir, 'clip.wav');
  await writeFile(input, 'audio');
  globalThis.fetch = async (url, init = {}) => {
    if (init.method === 'POST') return json({ task_id: 'kept' });
    if (init.method === 'DELETE') return new Response(null, { status: 503 });
    if (String(url).includes('/subtitle?')) return new Response(null, { status: 404 });
    return json({ status: 'completed', result: transcript });
  };
  const options = opts(input, { mode: 'async' });
  const outcome = (await extension.runBatch(options, dir))[0];
  assert.equal(outcome.status, 'done');
  assert.match(outcome.warning, /503/);
  assert.equal((await extension.runBatch(options, dir))[0].status, 'skipped');
}));

test('partial local replacement leaves no marker and never deletes remote result', async () => sandbox(async (dir) => {
  const input = path.join(dir, 'clip.wav');
  await writeFile(input, 'audio');
  const paths = extension.buildOutputPaths(input, dir);
  await mkdir(paths.srt); // Staging succeeds; final SRT rename fails after the TXT replacement.
  let deletes = 0;
  globalThis.fetch = async (url, init = {}) => {
    if (init.method === 'DELETE') { deletes++; return new Response(null, { status: 204 }); }
    if (init.method === 'POST') return json({ task_id: 'retained' });
    if (String(url).includes('/subtitle?')) return new Response(null, { status: 404 });
    return json({ status: 'completed', result: transcript });
  };
  const result = await extension.runBatch(opts(input, { mode: 'async', overwrite: true }), dir);
  assert.equal(result[0].status, 'failed');
  assert.equal(deletes, 0);
  await assert.rejects(stat(paths.marker), { code: 'ENOENT' });
  assert.equal(await readFile(paths.txt, 'utf8'), 'hello\n');
}));

test('new option and both Pi execute argument layouts remain supported', async () => sandbox(async (dir) => {
  assert.equal(extension.parseCommandArgs('clip.wav --keep-remote-task --json --overwrite').keepRemoteTask, true);
  let tool;
  extension.default({ registerTool(value) { tool = value; }, registerCommand() {} });
  assert.ok(tool.parameters.args[0].keepRemoteTask);
  const input = path.join(dir, 'clip.wav');
  await writeFile(input, 'audio');
  globalThis.fetch = async () => json(transcript);
  const params = { inputPath: input, mode: 'sync', baseUrl: 'http://asr.invalid', overwrite: true };
  const signal = new AbortController().signal;
  const updates = [];
  const update = (value) => updates.push(value);
  const ctx = { cwd: dir };
  const first = await tool.execute('id', params, signal, update, ctx);
  const second = await tool.execute('id', params, update, ctx, signal);
  assert.equal(first.details.outcomes[0].status, 'done');
  assert.equal(second.details.outcomes[0].status, 'done');
  assert.ok(updates.length > 0);
}));
