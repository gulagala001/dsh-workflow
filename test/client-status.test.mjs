import test from 'node:test';
import assert from 'node:assert/strict';
import { workflowJobs, liveWorkflowData } from '../src/client/workflow-status.mjs';

const call = (seq, name = 'workflow', callId = 'call') => ({ seq, type: 'tool/call', data: { name, callId } });
const start = { seq: 2, type: 'tool-workflow/run-start', data: { runId: 'run' } };
const result = (sourceEventSeqs = [1]) => ({ seq: 3, type: 'tool/result', sourceEventSeqs, data: { message: {
  role: 'tool', toolCallId: 'call', source: { kind: 'tool', callId: 'call' },
  content: [{ type: 'text', text: 'workflow "Check" started in the background as job job-1.\nRun ID: run\n' }],
} } });

test('only an exact workflow receipt reads its body and owns a background job', () => {
  for (const calls of [[], [call(1, 'read')], [call(1, 'workflow', 'different')]]) {
    const receipt = result(); Object.defineProperty(receipt.data.message, 'content', { get() { throw Error('unrelated tool body read'); } });
    assert.deepEqual(workflowJobs([...calls, start, receipt]), new Map([['run', { seq: 2 }]]));
  }
  assert.deepEqual(workflowJobs([call(1), start, result()]), new Map([['run', { seq: 2, jobId: 'job-1' }]]));
  for (const receipt of [result([]), { ...result(), seq: 0 }, result([4])])
    assert.equal(workflowJobs([call(1), start, call(4), receipt]).get('run').jobId, undefined);
  const failed = result(); failed.data.message.isError = true;
  assert.equal(workflowJobs([call(1), start, failed]).get('run').jobId, undefined);
  const spoof = result(); spoof.data.message.source.callId = 'other';
  assert.equal(workflowJobs([call(1), start, spoof]).get('run').jobId, undefined);
});

test('live, stopped and failed workflow states retain native ownership and durable endings', () => {
  const entries = [call(1), start, result()], runs = workflowJobs(entries.map(event => ({ event })));
  const node = { id: 'run', data: { status: 'interrupted', phases: [{ members: [{ status: 'interrupted' }, { status: 'completed' }] }] } };
  const job = { id: 'job-1', kind: 'workflow', owner: 'session', status: 'running' };
  assert.equal(liveWorkflowData(node, runs, [job], 'session').status, 'running');
  assert.equal(liveWorkflowData(node, runs, [{...job, status:'stopping'}], 'session').status, 'stopping');
  assert.equal(liveWorkflowData(node, runs, [job], 'other'), node.data);
  assert.equal(liveWorkflowData(node, runs, [{ ...job, status: 'completed' }], 'session'), node.data);
  const ended = workflowJobs([...entries, { seq: 4, type: 'tool-workflow/run-end', data: { runId: 'run', failures: ['failed cleanup'] } }]);
  assert.equal(liveWorkflowData(node, ended, [job], 'session'), node.data);
  assert.equal(liveWorkflowData({ ...node, data: { status: 'completed' } }, ended, [], 'session').status, 'failed');
});
