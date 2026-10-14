#!/usr/bin/env node
// Fixture self-test for the Claude stream mapper plus the pinned SDK import
// surface. No native executable, account or model call is involved: fixtures
// are authored from the pinned SDK's own message types (see fixtures/*.json).
// Run after `npm ci --ignore-scripts`:  node selftest.mjs
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startup } from '@anthropic-ai/claude-agent-sdk';
import { explicitQueryModel } from './model-selection.mjs';
import { prepareQuery } from './prepared-query.mjs';
import {
  createStreamState, applySdkMessage, noteStreamEnd, noteStreamFailure, snapshot,
} from './stream.mjs';
import { streamEndedSendOutcome } from './pending-outcome.mjs';
import { createFirstQueryControl } from './first-query-control.mjs';

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
async function loadFixture(name) {
  const parsed = JSON.parse(await readFile(path.join(here, 'fixtures', name), 'utf8'));
  assert.ok(Array.isArray(parsed.messages), `${name}: messages array required`);
  return parsed;
}
async function load(name) {
  const parsed = await loadFixture(name);
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

// 1b. Exact replay-once behavior is retained after the bounded UUID set fills.
// The first sequence replays an assistant-content frame; the second replays a
// result frame whose application appends both a turn and an input receipt.
{
  function initializeReadyState(state, sessionId) {
    // This fixture begins after the ordinary init frame, so the first UUID in
    // each saturation sequence is the replay target under test.
    state.phase = 'ready';
    state.init = { session_id: sessionId, model: 'claude-fixture-model' };
  }

  function appendUniqueFramesTo8193(state, prefix) {
    const before = state.frame_uuids.size;
    for (let ordinal = before; ordinal < 8193; ordinal++) {
      applySdkMessage(state, {
        type: 'stream_event',
        event: { type: 'message_start' },
        uuid: `${prefix}-${ordinal}`,
      });
    }
    assert.equal(state.frame_uuids.size, 8192, 'dedup UUID memory remains bounded');
    assert.equal(state.gaps, 1, 'the first unseen frame after capacity is explicit as a gap');
  }

  const contentState = createStreamState();
  initializeReadyState(contentState, 'ses_frame_capacity_content');
  const firstContentFrame = {
    type: 'assistant',
    session_id: 'ses_frame_capacity_content',
    uuid: 'frame-capacity-first-content',
    message: {
      id: 'msg_frame_capacity_content',
      model: 'claude-fixture-model',
      role: 'assistant',
      stop_reason: null,
      content: [{ type: 'text', text: 'content appears once' }],
    },
  };
  applySdkMessage(contentState, firstContentFrame);
  applySdkMessage(contentState, {
    type: 'result',
    session_id: 'ses_frame_capacity_content',
    uuid: 'frame-capacity-content-result',
    subtype: 'success',
    is_error: false,
    user_message_uuid: 'input-frame-capacity-content',
    user_message_uuids: ['input-frame-capacity-content'],
    result: 'one terminal result',
  });
  appendUniqueFramesTo8193(contentState, 'content-padding');
  const contentBeforeReplay = snapshot(contentState);
  applySdkMessage(contentState, firstContentFrame);
  const contentAfterReplay = snapshot(contentState);
  assert.deepEqual(contentAfterReplay.messages, contentBeforeReplay.messages,
    'replayed assistant content does not append a second block or frame');
  assert.deepEqual(contentAfterReplay.turns, contentBeforeReplay.turns);
  assert.deepEqual(contentAfterReplay.input_executions, contentBeforeReplay.input_executions);
  assert.equal(contentAfterReplay.messages[0].blocks.filter(block => block.type === 'text').length, 1);
  assert.equal(contentAfterReplay.turns.length, 1);
  assert.equal(contentAfterReplay.input_executions.length, 1);
  assert.equal(contentAfterReplay.gaps, 1, 'known replay does not add a capacity gap');

  const resultState = createStreamState();
  initializeReadyState(resultState, 'ses_frame_capacity_result');
  const firstResultFrame = {
    type: 'result',
    session_id: 'ses_frame_capacity_result',
    uuid: 'frame-capacity-first-result',
    subtype: 'success',
    is_error: false,
    user_message_uuid: 'input-frame-capacity-result',
    user_message_uuids: ['input-frame-capacity-result'],
    result: 'one terminal result',
  };
  applySdkMessage(resultState, firstResultFrame);
  appendUniqueFramesTo8193(resultState, 'result-padding');
  const resultBeforeReplay = snapshot(resultState);
  applySdkMessage(resultState, firstResultFrame);
  const resultAfterReplay = snapshot(resultState);
  assert.deepEqual(resultAfterReplay.messages, resultBeforeReplay.messages,
    'result replay does not create or duplicate content');
  assert.deepEqual(resultAfterReplay.turns, resultBeforeReplay.turns,
    'result replay does not append a duplicate turn');
  assert.deepEqual(resultAfterReplay.input_executions, resultBeforeReplay.input_executions,
    'result replay does not append a duplicate input execution receipt');
  assert.equal(resultAfterReplay.messages.length, 0);
  assert.equal(resultAfterReplay.turns.length, 1);
  assert.equal(resultAfterReplay.input_executions.length, 1);
  assert.equal(resultAfterReplay.gaps, 1, 'known result replay does not add a capacity gap');
  console.log('PASS frame-uuid capacity: bounded fail-closed keeps replayed content and result once');
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

// 6. EOF or a lost stream response before the matching UUID leaves the queued
// send unknown. The shared helper is the exact projection used by finishSession;
// neither fixture supplies terminal result evidence or authorizes a resend.
{
  for (const name of [
    'eof-after-init-before-input-echo.stream.json',
    'lost-response-after-assistant-no-result.stream.json',
  ]) {
    const fixture = await loadFixture(name);
    const state = feed(fixture.messages);
    if (fixture.termination === 'eof') noteStreamEnd(state);
    else noteStreamFailure(state, new Error(fixture.error));

    const observed = snapshot(state);
    assert.equal(observed.adapter.phase, fixture.expected_phase, name);
    assert.equal(observed.input_executions.some(
      event => event.native_input_id === fixture.unobserved_input_uuid,
    ), false, 'a missing result cannot fabricate terminal input evidence');

    const outcome = streamEndedSendOutcome(
      fixture.pending_send, fixture.session, fixture.bridge_boot_id,
    );
    assert.equal(outcome.outcome, 'unknown', 'stream loss is not input admission');
    assert.equal(outcome.details.diagnostic_code, 'STREAM_ENDED_BEFORE_TURN_EVIDENCE');
    assert.equal(Object.hasOwn(outcome, 'native_input_id'), false);
    assert.equal(Object.hasOwn(outcome.details, 'user_message_uuid'), false);
    if (fixture.pending_send.initialTaskDispatch) {
      assert.equal(Object.hasOwn(outcome, 'native_root_id'), false,
        'init alone cannot adopt the first binding identity');
      assert.equal(outcome.details.completion_condition, 'native_input_admission_unknown');
      assert.equal(outcome.details.bridge_boot_id, fixture.bridge_boot_id);
    } else {
      assert.deepEqual(
        [outcome.native_root_id, outcome.native_scope_key],
        [fixture.session.rootId, fixture.session.scopeKey],
        'a retained session keeps its known identity without claiming the input was admitted',
      );
    }
  }
  console.log('PASS stream-loss-outcomes: EOF and lost response remain unknown without UUID admission or replay');
}

// 7. Only the pinned SDK result subtype union can create terminal evidence.
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

// 8. A failed adapter-bridge startup emits one Unknown and closes this boot.
// The temporary SDK throws before creating any native session or process.
{
  const fixtureRoot = await mkdtemp(path.join(tmpdir(), 'claude-prepare-failure-'));
  try {
    const runtimeRoot = path.join(fixtureRoot, 'runtime');
    const packageRoot = path.join(runtimeRoot, 'node_modules', '@anthropic-ai', 'claude-agent-sdk');
    await mkdir(packageRoot, { recursive: true });
    await writeFile(path.join(packageRoot, 'package.json'), JSON.stringify({
      name: '@anthropic-ai/claude-agent-sdk',
      version: '0.3.287',
      type: 'module',
      exports: { '.': './index.mjs' },
    }));
    await writeFile(path.join(packageRoot, 'index.mjs'), [
      "import { appendFileSync } from 'node:fs';",
      'export async function startup() {',
      "  appendFileSync(process.env.ELIOT_CLAUDE_FIXTURE_STARTUP_MARKER, 'called\\n');",
      "  throw new Error('fixture startup failure');",
      '}',
      '',
    ].join('\n'));

    const repositoryRoot = path.resolve(here, '../..');
    const bridgePath = path.join(repositoryRoot, 'crates', 'swarm-adapter-claude', 'sdk-harness', 'bridge.mjs');
    const startupMarker = path.join(fixtureRoot, 'startup-calls.txt');
    const firstPrepare = {
      kind: 'prepare', operation_id: 'fixture-failed-prepare',
      workspace_root: repositoryRoot, sdk_runtime_root: runtimeRoot,
      native_scope_key: 'claude:fixture', bridge_boot_id: 'fixture-boot',
      binding_id: 'fixture-binding', generation: 1, model_id: 'fixture-model',
      permission_mode: null,
    };
    const secondPrepare = { ...firstPrepare, operation_id: 'fixture-second-prepare' };
    const child = spawn(process.execPath, [bridgePath], {
      cwd: repositoryRoot,
      env: { ...process.env, ELIOT_CLAUDE_FIXTURE_STARTUP_MARKER: startupMarker },
      stdio: ['pipe', 'pipe', 'ignore'],
    });
    let stdout = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', chunk => { stdout += chunk; });
    const closed = new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (code, signal) => resolve({ code, signal }));
    });
    child.stdin.end(`${JSON.stringify(firstPrepare)}\n${JSON.stringify(secondPrepare)}\n`);
    const { code, signal } = await closed;

    assert.equal(signal, null, 'startup failure closes the bridge cleanly');
    assert.equal(code, 0);
    assert.deepEqual(stdout.trim().split(/\r?\n/).map(line => JSON.parse(line)), [
      { kind: 'operation_unknown', operation_id: 'fixture-failed-prepare', diagnostic_code: 'SDK_STARTUP_UNKNOWN' },
    ], 'the failed prepare is reported once and a second prepare is never accepted');
    assert.equal(await readFile(startupMarker, 'utf8'), 'called\n', 'SDK startup was invoked exactly once');
    console.log('PASS adapter-bridge-failed-prepare: one Unknown frame, one startup call, bridge retired');
  } finally {
    await rm(fixtureRoot, { recursive: true, force: true });
  }
}

// 9. Normal bridge.5 first-query control is a pass-through: no fault env,
// no spawn override, unchanged input, unwrapped Control, unlimited hellos.
{
  delete process.env.ELIOT_CLAUDE_V5_FIRST_QUERY_FAULT_DIR;
  const normal = await createFirstQueryControl({ bindingId: 'selftest', generation: 1, bootId: 'selftest-boot' });
  assert.equal(normal.spawnClaudeCodeProcess, undefined, 'SDK default spawn, no test override');
  const input = { type: 'user', uuid: 'selftest-input', parent_tool_use_id: null, message: { role: 'user', content: 'prompt bytes' } };
  async function* source() { yield input; }
  const seen = [];
  for await (const item of normal.gateFirstTaskPrompt(source(), { operationId: 'op', inputId: 'selftest-input', taskPrompt: null })) seen.push(item);
  assert.deepEqual(seen, [input], 'first TaskPrompt reaches the SDK unchanged without waiting');
  assert.equal(seen[0], input, 'same object, no copy or rewrite');
  await normal.noteSdkInitIdentity('ses_selftest');
  const link = { calls: 0, call: async () => { link.calls++; return { recorded: true }; } };
  const before = link.call;
  normal.instrumentControl(link);
  assert.equal(link.call, before, 'Control calls pass through unwrapped');
  for (let i = 0; i < 3; i++) await link.call('module.hello', {});
  assert.equal(link.calls, 3, 'ordinary reconnects are not capped at two hellos');
  await link.call('module.outcome', { operation_id: 'op', outcome: 'unknown', details: { diagnostic_code: 'FIRST_NATIVE_QUERY_UNKNOWN' } });
  assert.equal(link.calls, 4, 'genuine store ACK accepted, never withheld');
  console.log('PASS normal-first-query-control: no env, default spawn, unchanged input, uncapped reconnect, ACK accepted');
}

// 10. Normal bridge.5 graph contains no test fault controller; the fault
// composition stays explicitly selectable and distinguishable.
{
  const bridgeText = await readFile(path.join(here, 'bridge-v5.mjs'), 'utf8');
  assert.equal(bridgeText.includes('first-query-spawn-control.test.mjs'), false, 'production entrypoint imports no test file');
  assert.equal(bridgeText.includes('ELIOT_CLAUDE_V5_FIRST_QUERY_FAULT_DIR'), false, 'normal bootstrap requires no fault directory');
  assert.ok(bridgeText.includes("from './first-query-control.mjs'"), 'normal control seam imported');
  const descriptor = JSON.parse(await readFile(path.join(here, 'module-descriptor.template.json'), 'utf8'));
  assert.equal(descriptor.artifact.artifact_id, 'claude-agent-sdk-0.3.287-bridge.5');
  assert.ok(JSON.stringify(descriptor.launch).includes('bridge-v5.mjs'));
  assert.equal(JSON.stringify(descriptor).includes('FIRST_QUERY_FAULT'), false, 'standard descriptor carries no fault requirement');
  const faultEntry = await readFile(path.join(here, 'bridge-v5-fault-test.mjs'), 'utf8');
  assert.ok(faultEntry.includes('first-query-spawn-control.test.mjs'), 'fault scenario remains reproducible via its explicit entrypoint');
  console.log('PASS bridge-v5-graph: production entry clean, descriptor clean, fault entry explicit');
}

console.log('Claude bridge self-test: all fixture assertions passed');
