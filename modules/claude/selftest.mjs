#!/usr/bin/env node
// Fixture self-test for the Claude stream mapper plus the pinned SDK import
// surface. No native executable, account or model call is involved: fixtures
// are authored from the pinned SDK's own message types (see fixtures/*.json).
// Run after `npm ci --ignore-scripts`:  node selftest.mjs
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startup } from '@anthropic-ai/claude-agent-sdk';
import { explicitQueryModel } from './model-selection.mjs';
import { prepareQuery } from './prepared-query.mjs';
import { createStreamState, applySdkMessage, snapshot } from './stream.mjs';

assert.equal(typeof startup, 'function', 'pinned SDK must export startup()');

// Opening prepares one initialized native process without claiming its
// one-shot WarmQuery. The first exact Task input claims it once; failure never
// makes the same handle replayable. All SDK calls below are test doubles.
{
  const options = { cwd: 'fixture', model: 'sonnet' };
  const input = {
    type: 'user',
    message: { role: 'user', content: 'Task specification: {"title":"fixture"}\n\nmarker' },
    parent_tool_use_id: null,
    uuid: 'd34d0011-1111-4111-8111-111111111111',
  };
  let startupCalls = 0;
  let queryCalls = 0;
  let claimedPrompt = null;
  const prepared = await prepareQuery(async request => {
    startupCalls++;
    assert.deepEqual(request, { options });
    return {
      query(prompt) { queryCalls++; claimedPrompt = prompt; return 'fixture-query'; },
      close() { throw new Error('claimed warm handle must not be closed as unclaimed'); },
    };
  }, options);
  assert.equal(prepared.state, 'prepared');
  assert.equal(startupCalls, 1);
  assert.equal(queryCalls, 0, 'agent.open must not call WarmQuery.query or send a model prompt');
  const oneInput = (async function* () { yield input; })();
  assert.equal(prepared.query(oneInput), 'fixture-query');
  assert.equal(queryCalls, 1);
  assert.equal(claimedPrompt, oneInput, 'the one exact SDK user input is passed unchanged');
  assert.equal(prepared.state, 'claimed');
  assert.throws(() => prepared.query(oneInput), /SDK_WARM_QUERY_ALREADY_CLAIMED/);

  const failed = await prepareQuery(async () => ({
    query() { throw new Error('uncertain query claim'); },
    close() {},
  }), options);
  assert.throws(() => failed.query(oneInput), /uncertain query claim/);
  assert.equal(failed.state, 'claimed', 'an uncertain SDK call cannot be replayed');
  assert.throws(() => failed.query(oneInput), /SDK_WARM_QUERY_ALREADY_CLAIMED/);
  console.log('PASS prepared-query: open prepares without model input; first Task claims WarmQuery once');
}

const here = path.dirname(fileURLToPath(import.meta.url));
async function load(name) {
  const parsed = JSON.parse(await readFile(path.join(here, 'fixtures', name), 'utf8'));
  assert.ok(Array.isArray(parsed.messages), `${name}: messages array required`);
  return parsed.messages;
}
function feed(messages) {
  const state = createStreamState();
  for (const message of messages) applySdkMessage(state, message);
  return state;
}
function messageById(snap, id) {
  return snap.messages.find(m => m.id === id);
}

// 1a. Initial selection is passed as Options.model, then kept distinct from
// the resolved identity reported by the SDK's system/init frame.
{
  const option = explicitQueryModel({ modelId: 'sonnet' });
  assert.deepEqual(option, { model: 'sonnet' }, 'route alias is passed verbatim as SDK Options.model');
  assert.throws(() => explicitQueryModel({}), /MODEL_ID_REQUIRED/,
    'an omitted route model must not fall through to the CLI default');
  assert.throws(() => explicitQueryModel({ modelId: ' sonnet' }), /INVALID_MODEL_ID/,
    'route model identifiers are not silently trimmed');

  const state = feed(await load('model-selection.stream.json'));
  state.requested_model = option.model;
  const snap = snapshot(state);
  assert.equal(snap.adapter.model_requested, 'sonnet');
  assert.equal(snap.adapter.model_effective, 'claude-sonnet-5');
  assert.equal(snap.adapter.model, 'claude-sonnet-5');
  assert.equal(snap.adapter.model_selection_status, 'observed');
  assert.equal(snap.adapter.model_selection_evidence, 'system/init');

  const unknownState = feed(await load('init-failure.stream.json'));
  unknownState.requested_model = option.model;
  const unknown = snapshot(unknownState);
  assert.equal(unknown.adapter.model_requested, 'sonnet');
  assert.equal(unknown.adapter.model_effective, null);
  assert.equal(unknown.adapter.model_selection_status, 'unknown',
    'a missing init model is not promoted from the requested alias');
  console.log('PASS model-selection: explicit Options.model, requested alias vs native init ID, unknown stays unknown');
}

// 1. Init identity, multi-frame message assembly, replay, partials, success.
{
  const snap = snapshot(feed(await load('init-success.stream.json')));
  assert.equal(snap.adapter.phase, 'ready');
  assert.equal(snap.adapter.executor_version, '2.1.0-fixture');
  assert.equal(snap.adapter.model, 'claude-fixture-model');
  assert.equal(snap.adapter.permission_mode, 'default');
  assert.equal(snap.partial_events_seen, 2, 'stream_event frames are counted');
  assert.equal(snap.messages_total, 2, 'partials never become messages');
  const first = messageById(snap, 'msg_fixture_1');
  assert.ok(first, 'first message present');
  assert.deepEqual(first.blocks.map(b => b.type), ['thinking', 'text', 'tool_use'],
    'all blocks of the shared message.id survive in arrival order');
  assert.equal(first.frames, 3, 'replayed frame uuid applied once');
  const read = first.blocks.find(b => b.type === 'tool_use');
  assert.equal(read.id, 'toolu_read_1');
  assert.equal(read.result_seen, true);
  assert.equal(read.result_is_error, false);
  assert.equal(snap.observed_children.length, 0);
  assert.equal(snap.family_completeness, 'partial');
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.turns.length, 1);
  assert.equal(snap.turns[0].subtype, 'success');
  assert.equal(snap.usage.total_cost_usd, 0.0123);
  assert.equal(snap.usage.model_usage['claude-fixture-model'].inputTokens, 230);
  assert.equal(snap.init_failure, null);
  console.log('PASS init-success: identity, block assembly, replay, partials, terminal success');
}

// 2. Child linkage and the repeated-message.id block trap inside a child.
{
  const snap = snapshot(feed(await load('child-and-repeat-id.stream.json')));
  assert.equal(snap.observed_children.length, 1);
  const child = snap.observed_children[0];
  assert.equal(child.tool_use_id, 'toolu_task_1');
  assert.equal(child.subagent_type, 'Explore');
  assert.equal(child.status, 'completed', 'root tool_result completes the child');
  assert.equal(child.result_seen, true);
  const childMessage = messageById(snap, 'msg_fixture_20');
  assert.ok(childMessage, 'child message present');
  assert.equal(childMessage.parent_tool_use_id, 'toolu_task_1');
  assert.deepEqual(childMessage.blocks.map(b => b.type), ['text', 'tool_use'],
    'two frames, one message.id, frame-local index 0 each: no block lost');
  const bash = childMessage.blocks.find(b => b.type === 'tool_use');
  assert.equal(bash.id, 'toolu_bash_1');
  assert.equal(bash.result_seen, true, 'child-internal tool result joined by tool id');
  const root = messageById(snap, 'msg_fixture_10');
  assert.deepEqual(root.blocks.map(b => b.type), ['text', 'tool_use']);
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.family_completeness, 'partial', 'one observed child is not a complete family');
  console.log('PASS child-and-repeat-id: parent_tool_use_id linkage, no block lost, child completion');
}

// 3. Init failure is a distinct outcome, never an empty successful start.
{
  const snap = snapshot(feed(await load('init-failure.stream.json')));
  assert.equal(snap.adapter.phase, 'init_failed');
  assert.equal(snap.execution, 'init_failed');
  assert.equal(snap.init_failure.subtype, 'error_during_execution');
  assert.equal(snap.init_failure.errors.length, 1);
  assert.equal(snap.adapter.executor_version, null, 'no identity is invented');
  assert.equal(snap.messages_total, 0);
  console.log('PASS init-failure: distinct init failure, no invented identity');
}

// 4. Terminal error subtype, denials, and cumulative (never summed) usage.
{
  const messages = await load('terminal-errors.stream.json');
  const state = createStreamState();
  for (const message of messages.slice(0, 3)) applySdkMessage(state, message);
  let snap = snapshot(state);
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.permission_denials.length, 1);
  assert.equal(snap.permission_denials[0].tool_use_id, 'toolu_denied_1');
  assert.equal(snap.usage.total_cost_usd, 0.1);
  applySdkMessage(state, messages[3]);
  snap = snapshot(state);
  assert.equal(snap.execution, 'turn_failed');
  assert.equal(snap.turns.length, 2);
  assert.equal(snap.turns[1].subtype, 'error_max_turns');
  assert.equal(snap.usage.total_cost_usd, 0.25, 'latest cumulative total replaces, never sums');
  console.log('PASS terminal-errors: error subtype retained, cumulative usage replaced');
}

// 5. Each Task input UUID maps only to its SDK-observed terminal result.
{
  const state = feed(await load('task-input-executions.stream.json'));
  const executions = snapshot(state).input_executions;
  assert.equal(executions.length, 2);
  assert.deepEqual(executions.map(item => item.native_input_id), [
    'd34d0011-1111-4111-8111-111111111111',
    'd34d0022-2222-4222-8222-222222222222',
  ]);
  assert.deepEqual(executions.map(item => item.result_frame_uuid), [
    'task-input-result-frame-1',
    'task-input-result-frame-2',
  ]);
  assert.ok(executions.every(item => item.correlation === 'unique'));
  assert.ok(executions.every(item => item.terminal_status === 'completed'));
  assert.ok(executions.every(item => item.effective_model === 'claude-sonnet-5-20261001'));
  assert.deepEqual(executions.map(item => [item.result_sha256, item.result_bytes]), [
    [createHash('sha256').update('first task output', 'utf8').digest('hex'), Buffer.byteLength('first task output', 'utf8')],
    [createHash('sha256').update('second task output', 'utf8').digest('hex'), Buffer.byteLength('second task output', 'utf8')],
  ]);

  const batched = createStreamState();
  applySdkMessage(batched, {
    type: 'system', subtype: 'init', session_id: 'ses_task_input_fixture',
    model: 'claude-sonnet-5-20261001', uuid: 'batch-init-frame',
  });
  applySdkMessage(batched, {
    type: 'result', subtype: 'success', session_id: 'ses_task_input_fixture',
    uuid: 'batch-result-frame', result_index: 2,
    user_message_uuid: 'd34d0022-2222-4222-8222-222222222222',
    user_message_uuids: [
      'd34d0011-1111-4111-8111-111111111111',
      'd34d0022-2222-4222-8222-222222222222',
    ],
    result: 'merged output', is_error: false,
  });
  const merged = snapshot(batched).input_executions;
  assert.equal(merged.length, 2);
  assert.ok(merged.every(item => item.correlation === 'ambiguous_multi_input'),
    'one result consuming several queued inputs cannot complete either producer');
  console.log('PASS task-input-executions: UUID, result frame, model and output digest bind; merged inputs remain ambiguous');
}

// 6. Only the pinned SDK result subtype union can create terminal evidence.
{
  const errorSubtypes = [
    'error_during_execution',
    'error_max_turns',
    'error_max_budget_usd',
    'error_max_structured_output_retries',
  ];
  const inputId = 'd34d0033-3333-4333-8333-333333333333';
  function resultFor(subtypeFields, is_error = true) {
    const state = createStreamState();
    applySdkMessage(state, {
      type: 'system', subtype: 'init', session_id: 'ses_result_union_fixture',
      model: 'claude-sonnet-fixture', uuid: 'result-union-init',
    });
    const result = {
      type: 'result', session_id: 'ses_result_union_fixture',
      uuid: 'result-union-frame', result_index: 0,
      user_message_uuid: inputId, user_message_uuids: [inputId],
      result: 'fixture output',
      ...subtypeFields,
    };
    if (is_error !== null) result.is_error = is_error;
    applySdkMessage(state, result);
    return snapshot(state);
  }

  for (const subtype of errorSubtypes) {
    const event = resultFor({ subtype }).input_executions[0];
    assert.equal(event.result_subtype, subtype);
    assert.equal(event.terminal_status, 'failed');
    assert.equal(event.result_index, 0, 'zero is a valid native result index');
  }
  assert.equal(resultFor({ subtype: 'success' }, false).input_executions[0].terminal_status, 'completed');
  assert.equal(resultFor({ subtype: 'success' }, true).input_executions[0].terminal_status, 'failed',
    'SDK success subtype with is_error true is an error result');

  for (const subtypeFields of [
    {},
    { subtype: 42 },
    { subtype: 'error_future_sdk_reason' },
  ]) {
    const unknown = resultFor(subtypeFields).input_executions[0];
    assert.equal(unknown.terminal_status, null, 'unknown result subtype remains nonterminal');
    assert.equal(resultFor(subtypeFields).execution, 'turn_unknown');
  }
  const missingErrorFlag = resultFor({ subtype: 'success' }, null).input_executions[0];
  assert.equal(missingErrorFlag.terminal_status, null, 'missing SDK error flag is not normalized to success');
  console.log('PASS result-subtype-whitelist: four SDK errors and success/error map; unknown or malformed result remains unresolved');
}

console.log('Claude bridge self-test: all fixture assertions passed');
