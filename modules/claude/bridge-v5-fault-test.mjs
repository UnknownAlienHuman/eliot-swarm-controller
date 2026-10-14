#!/usr/bin/env node
// Explicit test-only entrypoint for the destructive first-query fault
// composition (Issue147). NOT referenced by module-descriptor.template.json
// and never imported by bridge-v5.mjs.
//
// It drives createFirstQueryFaultControl from
// first-query-spawn-control.test.mjs against one harmless owned child and
// stub Control responses, proving the fault scenario still reproduces outside
// the normal production graph: pending hold, exact SIGTERM plus real close
// barrier, third-hello rejection, first-ACK withhold.
//
// Usage: node bridge-v5-fault-test.mjs   (no SDK, Store, model or Rust needed)
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createFirstQueryFaultControl } from './first-query-spawn-control.test.mjs';
const BINDING = 'fault-test-binding';
const GENERATION = 1;
const BOOT = 'fault-test-boot';
const OPERATION = 'fault-test-operation-1';
const INPUT = 'fault-test-input-1';
const PROMPT = 'fault scenario prompt bytes';
function sha256(text) { return createHash('sha256').update(text, 'utf8').digest('hex'); }
delete process.env.ELIOT_CLAUDE_V5_FIRST_QUERY_FAULT_DIR;
await assert.rejects(createFirstQueryFaultControl({ bindingId: BINDING, generation: GENERATION, bootId: BOOT }), /TEST_FAULT_CONTROL_DIRECTORY_REQUIRED/);
console.log('PASS fault-startup-gate: missing directory fails before spawn');
const dir = await mkdtemp(path.join(tmpdir(), 'claude-v5-fault-'));
process.env.ELIOT_CLAUDE_V5_FIRST_QUERY_FAULT_DIR = dir;
try {
  const fault = await createFirstQueryFaultControl({ bindingId: BINDING, generation: GENERATION, bootId: BOOT });
  const child = fault.spawnClaudeCodeProcess({ command: process.execPath, args: ['-e', 'setInterval(() => {}, 50);'], cwd: process.cwd(), env: { ...process.env } });
  assert.equal(typeof child.pid, 'number');
  const promptSha = sha256(PROMPT);
  const taskPrompt = { context: { operation_id: OPERATION, worker_boot_id: BOOT, task_id: 'fault-task', attempt_id: 'fault-attempt', task_snapshot_sha256: '0'.repeat(64) }, envelope: { prompt: PROMPT, prompt_sha256: promptSha, prompt_bytes: Buffer.byteLength(PROMPT, 'utf8') } };
  const message = { type: 'user', uuid: INPUT, parent_tool_use_id: null, message: { role: 'user', content: PROMPT } };
  async function* source() { yield message; }
  const gated = fault.gateFirstTaskPrompt(source(), { operationId: OPERATION, inputId: INPUT, taskPrompt });
  const nextPromise = gated.next();
  let pendingRec = null;
  for (let i = 0; i < 500; i++) {
    try { pendingRec = JSON.parse(await readFile(path.join(dir, 'pending-taskprompt.json'), 'utf8')); break; }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    await new Promise((r) => setTimeout(r, 20));
  }
  assert.ok(pendingRec, 'pending-taskprompt.json appears while the first input is held');
  await writeFile(path.join(dir, 'fault-request.json'), JSON.stringify({ schema_version: 1, child_id: pendingRec.child_id, pid: pendingRec.pid, binding_id: BINDING, generation: GENERATION, bridge_boot_id: BOOT, operation_id: OPERATION, input_id: INPUT }) + '\n');
  const first = await nextPromise;
  assert.equal(first.done, false);
  assert.deepEqual(first.value, message, 'unchanged prompt yielded after real child close');
  assert.equal((await gated.next()).done, true);
  assert.ok(child.exitCode !== null || child.signalCode != null, 'exact child departed via the fault request');
  console.log('PASS fault-gate: first input held until exact child SIGTERM plus close, bytes unchanged');
  const stub = { calls: 0, call: async () => { stub.calls++; return { binding_id: BINDING, generation: GENERATION }; } };
  fault.instrumentControl(stub);
  await stub.call('module.hello', {});
  await stub.call('module.hello', {});
  await assert.rejects(stub.call('module.hello', {}), /TEST_FAULT_CONTROL_UNEXPECTED_HELLO_COUNT/);
  console.log('PASS fault-hello-cap: third hello rejected after two successes');
  const outcomeStub = { calls: 0, call: async () => { outcomeStub.calls++; return { recorded: true }; } };
  fault.instrumentControl(outcomeStub);
  await assert.rejects(outcomeStub.call('module.outcome', { operation_id: OPERATION, outcome: 'unknown', details: { diagnostic_code: 'FIRST_NATIVE_QUERY_UNKNOWN' } }), /TEST_FIRST_QUERY_OUTCOME_ACK_WITHHELD/);
  assert.equal(outcomeStub.calls, 1, 'underlying store call happened; only the bridge reply was withheld');
  console.log('PASS fault-ack-withhold: genuine store ACK withheld once by test design');
} finally {
  delete process.env.ELIOT_CLAUDE_V5_FIRST_QUERY_FAULT_DIR;
  await rm(dir, { recursive: true, force: true });
}
console.log('bridge-v5 fault composition: destructive scenario reproduced via explicit test entrypoint');
