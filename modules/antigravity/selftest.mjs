#!/usr/bin/env node
// Fixture self-test for the Antigravity native event codec. No native
// executable, account or model call is involved: fixtures are authored from
// the official Antigravity CLI documentation examples (see fixtures/*.json
// provenance comments). Run:  node selftest.mjs
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import * as codec from './codec.mjs';
import {
  bindPendingLocalResults, localResultMatches, snapshotObservation,
} from './receipt-state.mjs';
import {
  createStreamState, applyNativeEvent, encodeUserMessage, snapshot,
  terminalResultDisposition,
} from './codec.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
async function load(name) {
  const parsed = JSON.parse(await readFile(path.join(here, 'fixtures', name), 'utf8'));
  assert.ok(Array.isArray(parsed.messages), `${name}: messages array required`);
  return parsed.messages;
}
function feed(messages) {
  const state = createStreamState();
  for (const message of messages) applyNativeEvent(state, message);
  return state;
}

// 1. Init identity, step mapping and a SUCCESS terminal result.
{
  const snap = snapshot(feed(await load('init-success.stream.json')));
  assert.equal(snap.adapter.phase, 'ready');
  assert.equal(snap.adapter.entrypoint, 'antigravity_cli_warm_stream');
  assert.equal(snap.adapter.executor_version, null, 'no version is invented');
  assert.equal(snap.adapter.permission_mode, 'request-review');
  assert.equal(snap.adapter.tools_count, 3);
  assert.equal(snap.adapter.model, null, 'model appears in init only when overridden');
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.turns.length, 1);
  assert.equal(snap.turns[0].status, 'SUCCESS');
  assert.equal(snap.turns[0].result_ordinal, 1);
  assert.equal(snap.turns[0].conversation_id, 'c3b66b04-872b-4fbe-a3a4-058a026ef20a');
  assert.match(snap.turns[0].response_sha256, /^[a-f0-9]{64}$/);
  assert.equal(snap.usage.total_tokens, 11007, 'usage comes from the result event');
  assert.equal(snap.usage.basis, 'native_cumulative_session');
  const response = snap.steps.find(s => s.step_index === 3);
  assert.equal(response.step_type, 'agent_response');
  assert.equal(response.state, 'DONE');
  assert.ok(response.text_chars > 100, 'text_delta is accounted, not stored as a message');
  assert.equal(snap.observed_children.length, 0);
  assert.equal(snap.family_completeness, 'partial');
  assert.equal(snap.init_failure, null);
  console.log('PASS init-success: conversation identity, steps, terminal SUCCESS');
}

// 2. Warm session: one init, sequential turns, cumulative counters replaced.
{
  const snap = snapshot(feed(await load('warm-two-turns.stream.json')));
  assert.equal(snap.turns.length, 2);
  assert.deepEqual(snap.turns.map(t => t.num_turns), [1, 2], 'num_turns is cumulative');
  assert.deepEqual(snap.turns.map(t => t.result_ordinal), [1, 2], 'result ordinal is per bridge boot');
  assert.equal(snap.turns[0].response_sha256, snap.turns[1].response_sha256,
    'identical native response bytes have the same SHA-256 fingerprint');
  assert.equal(snap.usage.total_tokens, 30670, 'latest cumulative usage replaces, never sums');
  const step2 = snap.steps.find(s => s.step_index === 2);
  assert.equal(step2.state, 'DONE', 'ACTIVE then DONE updates one step record');
  assert.equal(snap.steps_total, 4, 'docs example carries step indexes 0, 2, 3, 4');
  assert.equal(snap.execution, 'turn_completed');
  console.log('PASS warm-two-turns: sequential turns, cumulative usage replaced');
}

// 3. A soft-denied/failed tool stays as evidence under a SUCCESS result.
{
  const snap = snapshot(feed(await load('tool-error-success.stream.json')));
  assert.equal(snap.turns[0].status, 'SUCCESS');
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.tools_with_errors, 1, 'SUCCESS does not hide the tool failure');
  assert.equal(snap.tool_errors[0].tool_name, 'write_to_file');
  assert.equal(snap.tool_errors[0].error_type, 'permission_denied');
  assert.equal(snap.tool_errors[0].step_index, 5);
  const ok = snap.steps.find(s => s.step_index === 4);
  assert.equal(ok.tool_error, null, 'the successful tool step stays clean');
  console.log('PASS tool-error-success: tool failure survives a SUCCESS result');
}

// 4. Subagents are observed by their own conversation_id, never released.
{
  const snap = snapshot(feed(await load('subagent.stream.json')));
  assert.equal(snap.observed_children.length, 2);
  const research = snap.observed_children.find(c => c.type_name === 'research');
  assert.equal(research.conversation_id, '1b2c3d4e-5f6a-4b7c-8d9e-0f1a2b3c4d5e');
  assert.equal(research.role, 'Explore the repository layout');
  assert.deepEqual(research.workspace_uris, ['file:///home/user/project']);
  assert.ok(snap.observed_children.every(c => c.status === 'observed'),
    'idle is not released: no lifecycle is inferred from the stream');
  assert.equal(snap.family_completeness, 'partial', 'observed children are not a complete family');
  console.log('PASS subagent: children addressed by conversation_id, status stays observed');
}

// 5. A result before init is an init failure, never an empty successful start.
{
  const snap = snapshot(feed(await load('init-failure.stream.json')));
  assert.equal(snap.adapter.phase, 'init_failed');
  assert.equal(snap.execution, 'init_failed');
  assert.equal(snap.init_failure.native_responded, true);
  assert.match(snap.init_failure.error, /invalid model selection/);
  assert.equal(snap.adapter.tools_count, null, 'no identity is invented');
  assert.equal(snap.steps_total, 0);
  console.log('PASS init-failure: distinct init failure, no invented identity');
}

// 6. The only encoder is the documented user event; unknown events tolerated.
{
  assert.deepEqual(encodeUserMessage('Reply with exactly: one'), {
    event: 'user',
    message: { content: 'Reply with exactly: one' },
  });
  assert.throws(() => encodeUserMessage('  '), /PROMPT_TEXT_REQUIRED/);
  const exported = Object.keys(codec).sort();
  assert.deepEqual(exported, [
    'applyNativeEvent', 'createStreamState', 'encodeUserMessage',
    'noteMalformedLine', 'noteProcessExit', 'noteStderr', 'noteStreamEnd',
    'noteStreamFailure', 'snapshot', 'terminalResultDisposition',
  ], 'no control_request/control_response or slash encoder exists in the codec');
  const state = createStreamState();
  applyNativeEvent(state, { event: 'future_thing', payload: {} });
  applyNativeEvent(state, { no_event_field: true });
  const snap = snapshot(state);
  assert.equal(snap.other_events, 1, 'unknown event names are skipped, not fatal');
  assert.equal(snap.gaps, 1, 'a frame without the event discriminator is a gap');
  assert.equal(terminalResultDisposition('SUCCESS'), 'completed');
  assert.equal(terminalResultDisposition('ERROR'), 'failed');
  assert.equal(terminalResultDisposition('CANCELED'), 'cancelled');
  assert.equal(terminalResultDisposition('INTERRUPTED'), 'cancelled');
  assert.equal(terminalResultDisposition('WAITING'), null, 'WAITING is not terminal evidence');
  assert.equal(terminalResultDisposition('RUNNING'), null, 'RUNNING is not terminal evidence');
  assert.equal(terminalResultDisposition('FUTURE_STATUS'), null, 'unknown status is never terminal proof');
  console.log('PASS encoder: user event only; unknown events skipped like the native CLI');
}

// 7. A later terminal result cannot mutate an earlier acknowledged snapshot.
// The fake IPC calls prove module.observe precedes module.outcome and that the
// outcome cites the newly recorded observation, without launching agy.
{
  const liveResults = [];
  const previous = {
    observation_id: 9,
    state: snapshotObservation({ local_execution_results: liveResults }),
  };
  const receipt = {
    input_operation_id: 'op_fixture',
    native_conversation_id: 'conv_fixture',
    bridge_boot_id: 'boot_fixture',
    result_ordinal: 1,
    response_sha256: 'a'.repeat(64),
    status: 'SUCCESS',
  };
  liveResults.push({ ...receipt });
  assert.equal(previous.state.local_execution_results.length, 0,
    'an acknowledged observation is detached from later native results');
  assert.equal(localResultMatches(receipt, previous.state.local_execution_results[0]), false,
    'a result arriving later cannot be backdated onto the old observation');

  const outcome = { details: { local_execution_ref: { ...receipt } } };
  const calls = [];
  const acknowledged = await bindPendingLocalResults(
    [['op_fixture', outcome]],
    previous,
    async () => {
      calls.push('module.observe');
      return {
        observation_id: 10,
        state: snapshotObservation({ local_execution_results: liveResults }),
      };
    },
    async (id, sent) => {
      calls.push({ method: 'module.outcome', id, observation_id: sent.details.local_execution_ref.observation_id });
    },
  );
  assert.equal(acknowledged.observation_id, 10);
  assert.deepEqual(calls, [
    'module.observe',
    { method: 'module.outcome', id: 'op_fixture', observation_id: 10 },
  ]);
  assert.equal(liveResults[0].observation_id, undefined,
    'citing an observation does not mutate the captured native receipt');
  console.log('PASS receipt binding: new result gets a fresh immutable observation before outcome');
}

console.log('Antigravity bridge self-test: all fixture assertions passed');
