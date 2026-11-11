import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { createRequire, isBuiltin } from 'node:module';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { join, resolve, relative, isAbsolute, sep } from 'node:path';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { createModule as createSpawnModule } from '../lib/host/workflow-spawn.factory.mjs';
import { createModule } from '../lib/host/workflow-ptc.factory.mjs';
import dependencies from '../lib/host/dependencies.json' with { type: 'json' };

const require = createRequire(import.meta.url);
const persistence = createRequire(require.resolve('@deepseek-ai/dsh-session-persistence-jsonl/package.json'));
async function importHostModule(name) {
  let url = name;
  if (!isBuiltin(name)) {
    try { url = pathToFileURL(require.resolve(name)).href; }
    catch { url = pathToFileURL(persistence.resolve(name)).href; }
  }
  return import(url);
}
const modules = new Map();
for (const name of dependencies['workflow-ptc']) {
  const value = await importHostModule(name);
  modules.set(name, ['koffi', '@deepseek-ai/schemastery'].includes(name) ? value.default : { ...value, __esModule: true });
}

async function fixture(t, { fault, body = "await agent('one'); await agent('two'); return 'finished';", result, dispose, journalAt = 1, provider = 'spawn' } = {}) {
  const root = await fs.mkdtemp(join(tmpdir(), 'workflow-settlement-'));
  const events = [], starts = [], disposed = [];
  let failWrite = fault === 'journal';
  const fileSystem = { ...fs, async open(path, ...args) {
    const file = await fs.open(path, ...args);
    if (!String(path).endsWith('journal.jsonl')) return file;
    return new Proxy(file, { get(target, key) {
      if (key === 'writeFile') return async data => {
        if (failWrite && JSON.parse(data).type === 'result' && JSON.parse(data).seq === journalAt) {
          failWrite = false;
          throw Object.assign(new Error('journal disk full'), { code: 'ENOSPC' });
        }
        return target.writeFile(data);
      };
      const value = Reflect.get(target, key);
      return typeof value === 'function' ? value.bind(target) : value;
    } });
  } };
  const host = createModule(name => name === 'node:fs/promises' ? fileSystem : modules.get(name), importHostModule);
  const parent = { session: { id: 'parent', header: {}, requestHeader: () => ({ config: {} }) } };
  const runtime = {
    resolve: request => request,
    async run(request) {
      return { value: await host.runWorkflowGuest(request.bindings[0].functions) };
    },
  };
  const start = async request => {
    const id = starts.push(request.prompt[0].text);
    return { childId: `child-${id}`, result: result ? result(id) : Promise.resolve({ stopReason: 'completed', output: [{ type: 'text', text: `result-${id}` }] }),
      async dispose() {
        disposed.push(id);
        if (dispose) await dispose(id);
        if (fault === 'native') throw new Error(`native cleanup failed ${id}`);
      },
    };
  };
  const subagents = { startActivation: request => start(request), getProvider: () => ({ startWorkflow: async request => {
    const activation = await start(request);
    return { ...activation, quiesced: true };
  } }) };
  const ctx = { workingDirectory: { ensure: async () => root }, logger: { warn() {} } };
  const createRun = resumeFromRunId => new host.PtcWorkflowRun(ctx, subagents, runtime, randomUUID(), { name: 'settlement' }, parent,
    { meta: { name: 'settlement' }, body, limits: { maxConcurrentAgents: 2, maxTotalAgents: 8, maxItemsPerCall: 16, syncTimeoutMs: 1000 } },
    provider, { mode: 'danger-full-access' }, { phase() {}, log() {}, agentStart: info => events.push(['start', info.seq]), agentEnd: info => events.push(['end', info.seq, info.outcome]) }, undefined,
    { root, resumeFromRunId, loadWorkflow: async () => { throw new Error('unused'); } });
  const run = createRun();
  t.after(async () => { await run.dispose(); await fs.rm(root, { recursive: true, force: true }); });
  const resume = () => host.WorkflowJournal.create(root, 'parent', randomUUID(), { script: '', meta: {}, resumeFromRunId: run.id });
  return { root, host, run, starts, disposed, events, resume, rerun: () => createRun(run.id) };
}

test('a journal write failure stops subsequent children and releases a quiescent run for resume', async t => {
  const f = await fixture(t, { fault: 'journal' });
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'error');
  assert.match(outcome.error, /journal disk full/);
  assert.deepEqual(f.starts, ['one']);
  assert.deepEqual(f.disposed, [1]);
  const resumed = await f.resume();
  await resumed.close('completed');
});

test('native cleanup failure stops dispatch and retains the run lease', async t => {
  const f = await fixture(t, { fault: 'native' });
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'error');
  assert.match(outcome.error, /native cleanup failed 1/);
  assert.deepEqual(f.starts, ['one']);
  assert.deepEqual(f.disposed, [1]);
  await assert.rejects(f.resume(), /owned/i);
});

test('a script cannot catch settlement failure and dispatch more work', async t => {
  const f = await fixture(t, { fault: 'journal', body: "try { await agent('one'); } catch {} try { await agent('two'); } catch {} return 'ignored';" });
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'error');
  assert.deepEqual(f.starts, ['one']);
  const resumed = await f.resume();
  await resumed.close('completed');
});

test('normal completion still journals ordered results and releases all children', async t => {
  const f = await fixture(t);
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'completed');
  assert.equal(outcome.value, 'finished');
  assert.deepEqual(f.starts, ['one', 'two']);
  assert.deepEqual(f.disposed, [1, 2]);
  const calls = f.host.readJournal(await fs.readFile(join(f.run.transcriptDir, 'journal.jsonl'), 'utf8'));
  assert.equal(calls.get(1).result.output[0].text, 'result-1');
  assert.equal(calls.get(2).result.output[0].text, 'result-2');
  const resumed = await f.resume();
  await resumed.close('completed');
});


test('resume keeps a durable successful prefix after a later journal failure', async t => {
  const f = await fixture(t, { fault: 'journal', journalAt: 2, body: "return [await agent('one'), await agent('two'), await agent('three')];" });
  assert.equal((await f.run.result).stopReason, 'error');
  const resumed = f.rerun();
  const outcome = await resumed.result;
  assert.equal(outcome.stopReason, 'completed');
  assert.deepEqual(outcome.value, ['result-1', 'result-3', 'result-4']);
  assert.deepEqual(f.starts, ['one', 'two', 'two', 'three']);
  await resumed.dispose();
});

test('parallel cleanup joins every child and reports every failure', async t => {
  const entered = Promise.withResolvers(), release = Promise.withResolvers();
  let arrivals = 0;
  t.after(() => release.resolve());
  const f = await fixture(t, { body: "return await parallel([() => agent('one'), () => agent('two')]);", dispose: async id => {
    if (++arrivals === 2) entered.resolve();
    await release.promise;
    throw new Error(`cleanup failure ${id}`);
  } });
  let settled = false;
  f.run.result.then(() => { settled = true; });
  await entered.promise;
  assert.equal(settled, false);
  release.resolve();
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'error');
  for (const id of [1, 2]) assert.match(outcome.error, new RegExp(`cleanup failure ${id}`));
  assert.deepEqual([...f.disposed].sort(), [1, 2]);
  assert.equal(f.run.childFailures.length, 2);
  await assert.rejects(f.resume(), /owned/i);
});

test('cancellation waits for native disposal and releases a safely stopped run', async t => {
  const entered = Promise.withResolvers(), release = Promise.withResolvers();
  t.after(() => release.resolve());
  const f = await fixture(t, { result: () => { entered.resolve(); return new Promise(() => {}); }, dispose: () => release.promise });
  let settled = false;
  f.run.result.then(() => { settled = true; });
  await entered.promise;
  f.run.cancel('user cancelled');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false);
  release.resolve();
  assert.equal((await f.run.result).stopReason, 'cancelled');
  assert.deepEqual(f.starts, ['one']);
  assert.deepEqual(f.disposed, [1]);
  const resumed = await f.resume();
  await resumed.close('completed');
});

test('worktree settlement errors retain diagnostics without retaining a stopped native child lease', async t => {
  const f = await fixture(t, { provider: 'dsh-workflow', dispose: () => { throw new Error('worktree receipt failed'); } });
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'error');
  assert.match(outcome.error, /worktree receipt failed/);
  assert.deepEqual(f.starts, ['one']);
  const resumed = await f.resume();
  await resumed.close('completed');
});

test('ordinary child failures remain per-item outcomes and do not block independent work', async t => {
  const f = await fixture(t, { body: "return [await agent('one'), await agent('two')];", result: id => Promise.resolve({ stopReason: id === 1 ? 'error' : 'completed', output: [{ type: 'text', text: `result-${id}` }], diagnostic: 'model task failed' }) });
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'completed');
  assert.deepEqual(outcome.value, [null, 'result-2']);
  assert.deepEqual(f.starts, ['one', 'two']);
  assert.equal(f.run.childFailures[0].reason, 'model task failed');
});

test('even an undefined disposal rejection is a visible unsafe cleanup failure', async t => {
  const f = await fixture(t, { dispose: () => { throw undefined; } });
  const outcome = await f.run.result;
  assert.equal(outcome.stopReason, 'error');
  assert.match(outcome.error, /cleanup failed: undefined/);
  assert.deepEqual(f.starts, ['one']);
  await assert.rejects(f.resume(), /owned/i);
});


for (const nativeFailure of [false, true]) test(`real worktree receipt failure preserves native quiescence (${nativeFailure ? 'failed' : 'stopped'})`, async t => {
  const directory = await fs.realpath(await fs.mkdtemp(join(tmpdir(), 'workflow-worktree-settlement-')));
  const git = args => execFileSync('git', args, { cwd: directory, stdio: 'pipe' });
  git(['init', '-q']);
  await fs.writeFile(join(directory, 'file.txt'), 'fixture\n');
  git(['add', 'file.txt']);
  git(['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'fixture']);
  let failReceipt = false;
  const spawn = createSpawnModule(name => name === 'node:fs/promises' ? { ...fs, async writeFile(path, ...args) {
    if (failReceipt && String(path).endsWith('.json')) throw new Error('worktree receipt disk full');
    return fs.writeFile(path, ...args);
  } } : modules.get(name), importHostModule);
  const listeners = [], cleanups = [], artifacts = [];
  const native = { prepareContinuable: async () => ({}) };
  const providers = new Map([['spawn', native]]);
  const parent = { id: 'parent', session: { id: 'parent' } };
  let nativeDisposals = 0;
  const ctx = {
    subagents: {
      getProvider: id => providers.get(id),
      registerProvider(provider) { providers.set(provider.name, provider); return () => providers.delete(provider.name); },
      async startActivation(input) {
        await providers.get(input.provider).prepareContinuable({ ...input, sessionId: input.childId, signal: input.signal });
        const agent = { id: input.childId, ctx: { get: () => ({ set: async () => {} }), systemPrompt: { context() {}, getContextOrder: () => 0 } } };
        for (const listener of [...listeners]) await listener({ agent, signal: input.signal });
        return { childId: input.childId, result: Promise.resolve({ stopReason: 'completed', output: [] }), async dispose() {
          nativeDisposals++;
          if (nativeFailure) throw new Error('native descendants still live');
        } };
      },
    },
    sandboxPolicy: { resolve: () => ({ mode: 'danger-full-access', workspaceRoot: directory }) },
    fs: {
      resolve: async (path, options) => resolve(options?.cwd ?? directory, path),
      processPathFromHostPath: path => path, processPath: path => path,
      contains(root, path) { const part = relative(root, path); return part !== '..' && !part.startsWith('..' + sep) && !isAbsolute(part); },
    },
    get: name => name === 'agents' ? { get: () => parent } : undefined,
    on(name, listener) { if (name === 'agent/created') listeners.push(listener); return () => { const i = listeners.indexOf(listener); if (i >= 0) listeners.splice(i, 1); }; },
    effect(setup) { cleanups.push(setup()); },
  };
  spawn.apply(ctx);
  t.after(async () => { for (const cleanup of cleanups) await cleanup(); await fs.rm(directory, { recursive: true, force: true }); });
  const activation = await providers.get('dsh-workflow').startWorkflow({ cwd: directory, parent, prompt: [], signal: new AbortController().signal }, { isolation: 'worktree', onWorktree: value => artifacts.push(value) });
  assert.equal(activation.quiesced, false);
  failReceipt = true;
  await assert.rejects(activation.dispose(), error => {
    assert.match(error.message, /worktree receipt disk full/);
    if (nativeFailure) assert.match(error.message, /native descendants still live/);
    return true;
  });
  assert.equal(nativeDisposals, 1);
  assert.equal(activation.quiesced, !nativeFailure);
  if (nativeFailure) await fs.access(artifacts[0].path);
  else await assert.rejects(fs.access(artifacts[0].path), { code: 'ENOENT' });
});
