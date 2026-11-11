import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Context } from '@deepseek-ai/cordis';
import { WorkflowBudget } from '../src/budget.mjs';
import { WorkflowRuntime } from '../src/runtime.mjs';
import { WorkflowStore } from '../src/store.mjs';

test('captured embedded budgets account for children started after the embedding plugin unloads', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-workflow-embedded-budget-'));
  const previousHome = process.env.DSH_HOME;
  process.env.DSH_HOME = directory;
  const ctx = new Context();
  const fibers = [];
  t.after(async () => {
    try {
      for (const fiber of fibers.reverse()) await fiber.dispose();
    } finally {
      if (previousHome === undefined) delete process.env.DSH_HOME;
      else process.env.DSH_HOME = previousHome;
      await rm(directory, { recursive: true, force: true });
    }
  });
  const mount = async component => {
    const fiber = ctx.plugin(component);
    fibers.push(fiber);
    await fiber;
    return fiber;
  };
  await mount({
    name: 'budget-test-services',
    apply(scope) { scope.provide('agents', {}); scope.provide('sessions', {}); },
  });
  const store = new WorkflowStore(join(directory, 'embedded'));
  const embeddedBudget = new WorkflowBudget(store);
  const embedding = await mount({
    name: 'budget-test-embedding',
    apply(scope) {
      scope.provide('omd', { workflowBudget: embeddedBudget, store });
      scope.on('agent/created', ({ agent }) => embeddedBudget.attach(agent.session), { global: true });
      scope.on('session/event', (session, event) => embeddedBudget.observe(session, event), { global: true });
    },
  });
  await mount(WorkflowRuntime);
  const runtime = ctx.get('dshWorkflow');
  const parent = { id: 'parent', header: {} };
  const input = {
    turn: 1, step: 1,
    messages: [{ id: 'request', source: { kind: 'user' }, content: [{ type: 'text', text: 'Run workflow +10' }] }],
  };
  embeddedBudget.admit(parent, input);
  runtime.workflowBudget.admit(parent, input);
  const support = runtime.support(parent);
  const captured = support.workflowBudget.capture(parent);
  const release = runtime.retain('test-run', support);
  const owner = {...captured.owner, runId:'test-run'};
  const startChild = id => {
    const session = { id, header: { origin: 'subagent', parentSession: parent.id } };
    ctx.emit('agent/created', { agent: { session } });
    // The spawn provider passes the initiating run's captured owner.
    runtime.support(parent, owner).workflowBudget.attach(session, captured.owner);
    return session;
  };
  const settle = (session, outputTokens) => ctx.emit('session/event', session, {
    seq: 1, type: 'assistant/message',
    data: { message: { source: { kind: 'model' } }, usage: { outputTokens } },
  });
  settle(startChild('first'), 3);
  assert.deepEqual(captured.snapshot(), { total: 10, spent: 3, unmetered: 0 });

  await embedding.dispose();
  assert.equal(ctx.get('omd'), undefined);
  assert.equal(runtime.support(parent).store.dir, runtime.store.dir);
  const lateChild = startChild('late');
  assert.deepEqual(embeddedBudget.owner(lateChild), captured.owner);
  settle(lateChild, 9);
  assert.deepEqual(captured.snapshot(), { total: 10, spent: 12, unmetered: 0 });
  // Duplicate delivery and simultaneous independent observation cannot charge twice.
  settle(lateChild, 9);
  assert.equal(captured.snapshot().spent, 12);
  release();
  assert.equal(runtime.support(parent, owner).store.dir, runtime.store.dir);
});
