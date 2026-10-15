import assert from 'node:assert/strict';
import test from 'node:test';
import {
  applySdkMessage, createStreamState, resultSourceForInput,
} from './stream.mjs';
import { buildClaudeResultPage, publishClaudeResultPage } from './result-pages.mjs';
import { moduleReceipt, taskDispatchAdmission } from './task-prompt.mjs';

const ARTIFACT_ID = 'claude-agent-sdk-0.3.287-bridge.5';
const MODULE_CONTRACT = {
  module_id: 'claude',
  artifact: { artifact_id: ARTIFACT_ID, version: '5', build_id: null },
  protocol: { major: 1, minor: 0 },
};
const IDENTITY = {
  bindingId: 'binding-1',
  generation: 1,
  bridgeBootId: 'boot-1',
  nativeRootId: 'session-1',
  nativeScopeKey: 'claude:/fixture/project',
  inputId: 'input-1',
};

function dispatchCommand() {
  return {
    operation_id: 'dispatch-op',
    method: 'task.dispatch',
    binding_id: IDENTITY.bindingId,
    generation: IDENTITY.generation,
    input_sha256: 'a'.repeat(64),
    route: { runtime: 'module', module_artifact_id: ARTIFACT_ID },
    input: { attempt_id: 'attempt-1' },
  };
}

function dispatchPrompt(command) {
  return {
    envelope: {
      prompt: 'exact TaskPrompt bytes',
      prompt_sha256: 'b'.repeat(64),
      prompt_bytes: Buffer.byteLength('exact TaskPrompt bytes', 'utf8'),
    },
    context: {
      schema_version: 1,
      operation_id: command.operation_id,
      binding_id: command.binding_id,
      binding_generation: command.generation,
      worker_boot_id: IDENTITY.bridgeBootId,
      attempt_id: 'attempt-1',
      task_id: 'task-1',
      task_revision: 3,
      task_snapshot_sha256: 'c'.repeat(64),
      source_text_sha256: 'd'.repeat(64),
      source_text_bytes: 19,
    },
  };
}

function sdkResult(overrides = {}) {
  return {
    type: 'result',
    subtype: 'success',
    uuid: 'result-frame-1',
    session_id: IDENTITY.nativeRootId,
    user_message_uuid: IDENTITY.inputId,
    user_message_uuids: [IDENTITY.inputId],
    result_index: 0,
    is_error: false,
    stop_reason: 'end_turn',
    result: 'hello from the pinned SDK result frame',
    ...overrides,
  };
}

function fixture(message = sdkResult()) {
  const dispatch = dispatchCommand();
  const taskPrompt = dispatchPrompt(dispatch);
  const dispatchAdmission = taskDispatchAdmission(dispatch, taskPrompt, {
    moduleContract: MODULE_CONTRACT,
    bootId: IDENTITY.bridgeBootId,
    inputId: IDENTITY.inputId,
    nativeRootId: IDENTITY.nativeRootId,
    nativeFrame: message,
  });
  const dispatchReceipt = moduleReceipt(dispatch, MODULE_CONTRACT);
  const origin = {
    schema_version: 1,
    kind: 'claude_assistant_result',
    result_operation_id: 'result-op',
    result_input_sha256: 'e'.repeat(64),
    result_selector: {
      kind: 'claude_assistant_result',
      input_operation_id: dispatch.operation_id,
      session_id: IDENTITY.nativeRootId,
    },
    target_operation_id: dispatch.operation_id,
    target_method: 'task.dispatch',
    target_input_sha256: dispatch.input_sha256,
    target_module_receipt: dispatchReceipt,
    target_task_id: 'task-1',
    target_attempt_id: 'attempt-1',
    target_task_revision: 3,
    binding_id: IDENTITY.bindingId,
    binding_generation: IDENTITY.generation,
    bridge_boot_id: IDENTITY.bridgeBootId,
    native_session_id: IDENTITY.nativeRootId,
    native_scope_key: IDENTITY.nativeScopeKey,
    native_input_id: IDENTITY.inputId,
    native_payload_sha256: taskPrompt.envelope.prompt_sha256,
    native_payload_bytes: taskPrompt.envelope.prompt_bytes,
    task_snapshot_sha256: taskPrompt.envelope.task_snapshot_sha256,
    source_text_sha256: taskPrompt.context.source_text_sha256,
    source_text_bytes: taskPrompt.context.source_text_bytes,
    dispatch_admission: dispatchAdmission,
    producer: {
      assignment_id: dispatch.operation_id,
      dispatch_operation_id: dispatch.operation_id,
    },
    descriptor: {
      module_id: MODULE_CONTRACT.module_id,
      artifact: MODULE_CONTRACT.artifact,
      protocol: MODULE_CONTRACT.protocol,
      capabilities: ['agent.result'],
      module_artifact_id: ARTIFACT_ID,
    },
  };
  const command = {
    operation_id: origin.result_operation_id,
    method: 'agent.result',
    binding_id: IDENTITY.bindingId,
    generation: IDENTITY.generation,
    native_root_id: IDENTITY.nativeRootId,
    input_sha256: origin.result_input_sha256,
    target_input_sha256: dispatch.input_sha256,
    route: { runtime: 'module', module_artifact_id: ARTIFACT_ID },
    input: {
      selector: origin.result_selector,
      offset_bytes: 6,
      length_bytes: 5,
      claude_result_origin: origin,
    },
  };
  const stream = createStreamState();
  applySdkMessage(stream, {
    type: 'system',
    subtype: 'init',
    uuid: 'init-frame-1',
    session_id: IDENTITY.nativeRootId,
    model: 'claude-sonnet-fixture',
  });
  applySdkMessage(stream, message);
  return {
    command,
    origin,
    execution: resultSourceForInput(stream, IDENTITY.inputId, IDENTITY.nativeRootId),
  };
}

test('bridge.5 pages the exact unique SDK result frame under the Store-sealed dispatch and boot', () => {
  const { command, origin, execution } = fixture();
  assert.ok(execution);
  const page = buildClaudeResultPage(command, execution.execution, execution.body, MODULE_CONTRACT, {
    bridgeBootId: IDENTITY.bridgeBootId,
    nativeRootId: IDENTITY.nativeRootId,
    nativeScopeKey: IDENTITY.nativeScopeKey,
  });

  assert.equal(page.source.bridge_boot_id, IDENTITY.bridgeBootId);
  assert.equal(page.source.result_frame_uuid, 'result-frame-1');
  assert.equal(page.source.input_operation_id, origin.target_operation_id);
  assert.equal(page.source.native_input_id, IDENTITY.inputId);
  assert.deepEqual(page.source.target_module_receipt, origin.target_module_receipt);
  assert.equal(page.source.result_sha256, execution.body.sha256);
  assert.equal(page.content_base64, Buffer.from('from ', 'utf8').toString('base64'));
  assert.equal(page.offset_bytes, 6);
  assert.equal(page.byte_length, 5);
  assert.equal(page.total_bytes, Buffer.byteLength(sdkResult().result, 'utf8'));
  assert.equal(page.eof, false);
});

test('bridge.5 refuses ambiguous correlation, changed boot and unpageable result frames', () => {
  const ambiguous = fixture(sdkResult({ user_message_uuids: ['input-1', 'input-2'] }));
  assert.equal(ambiguous.execution, null);

  const valid = fixture();
  assert.throws(() => buildClaudeResultPage(
    valid.command,
    valid.execution.execution,
    valid.execution.body,
    MODULE_CONTRACT,
    { ...IDENTITY, bridgeBootId: 'boot-2' },
  ), /CLAUDE_RESULT_SCOPE_INVALID/);

  const tooLarge = fixture(sdkResult({ result: 'x'.repeat(512_001) }));
  assert.equal(tooLarge.execution, null);
});

test('bridge.5 publishes once and retains the exact Store artifact ACK', async () => {
  const { command, execution } = fixture();
  const attempts = new Map();
  let calls = 0;
  const published = await publishClaudeResultPage(
    command,
    execution.execution,
    execution.body,
    MODULE_CONTRACT,
    IDENTITY,
    async (method, input) => {
      calls++;
      assert.equal(method, 'module.result');
      assert.equal(input.operation_id, command.operation_id);
      assert.equal(input.page.source.bridge_boot_id, IDENTITY.bridgeBootId);
      return { recorded: true, artifact_ref: 'claude-result-artifact-1' };
    },
    attempts,
  );

  assert.equal(calls, 1);
  assert.equal(published.acknowledgement.artifact_ref, 'claude-result-artifact-1');
  await assert.rejects(
    publishClaudeResultPage(
      command,
      execution.execution,
      execution.body,
      MODULE_CONTRACT,
      IDENTITY,
      async () => { calls++; return published.acknowledgement; },
      attempts,
    ),
    error => error.diagnosticCode === 'CLAUDE_RESULT_PAGE_REPLAY_PREVENTED',
  );
  assert.equal(calls, 1);
});

async function assertStoreAckUncertaintyDoesNotRetry(reply) {
  const { command, execution } = fixture();
  const attempts = new Map();
  let calls = 0;
  const push = () => publishClaudeResultPage(
    command,
    execution.execution,
    execution.body,
    MODULE_CONTRACT,
    IDENTITY,
    async () => { calls++; return reply(); },
    attempts,
  );
  await assert.rejects(
    push(),
    error => error.diagnosticCode === 'CLAUDE_RESULT_ACK_UNKNOWN'
      && error.nativeAdmissionPossible === true,
  );
  await assert.rejects(
    push(),
    error => error.diagnosticCode === 'CLAUDE_RESULT_PAGE_REPLAY_PREVENTED',
  );
  assert.equal(calls, 1);
}

test('bridge.5 maps a lost Store ACK to Unknown without another push', async () => {
  await assertStoreAckUncertaintyDoesNotRetry(async () => {
    throw new Error('fixture ACK was lost');
  });
});

test('bridge.5 maps a malformed Store ACK to Unknown without another push', async () => {
  await assertStoreAckUncertaintyDoesNotRetry(async () => ({ recorded: false }));
});
