import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { applySdkMessage, createStreamState, snapshot } from './stream.mjs';
import {
  CLAUDE_TASK_PROMPT_ARTIFACT_ID,
  enqueueTaskPrompt,
  taskDispatchAdmission,
  taskPromptFor,
} from './task-prompt.mjs';

const sha256 = value => createHash('sha256').update(value, 'utf8').digest('hex');

function storeShapedDispatch() {
  const bootId = 'bridge-boot-task-prompt-fixture';
  const sourceText = 'Check the exact TaskPrompt bytes: café 🧪';
  const prompt = [
    'Check the exact TaskPrompt bytes: café 🧪',
    '',
    'ELIOT Task identity v1:',
    '{"attempt_id":"attempt-fixture","task_id":"task-fixture","task_revision":7,"task_snapshot_sha256":"' + 'a'.repeat(64) + '"}',
    '',
    'ELIOT Task brief v1:',
    '{"objective":"preserve producer bytes"}',
  ].join('\n');
  const envelope = {
    schema_id: 'swarm.task_prompt',
    schema_version: 1,
    task_id: 'task-fixture',
    task_revision: 7,
    attempt_id: 'attempt-fixture',
    task_snapshot_sha256: 'a'.repeat(64),
    prompt_sha256: sha256(prompt),
    prompt_bytes: Buffer.byteLength(prompt, 'utf8'),
    prompt,
  };
  const context = {
    schema_version: 1,
    operation_id: 'operation-fixture',
    binding_id: 'binding-fixture',
    binding_generation: 4,
    worker_boot_id: bootId,
    attempt_id: envelope.attempt_id,
    task_id: envelope.task_id,
    task_revision: envelope.task_revision,
    task_snapshot_sha256: envelope.task_snapshot_sha256,
    source_text_sha256: sha256(sourceText),
    source_text_bytes: Buffer.byteLength(sourceText, 'utf8'),
  };
  const input = {
    text: sourceText,
    attempt_id: envelope.attempt_id,
    task_prompt: envelope,
    task_dispatch_context: context,
  };
  const command = {
    method: 'task.dispatch',
    operation_id: context.operation_id,
    binding_id: context.binding_id,
    generation: context.binding_generation,
    input_sha256: sha256(JSON.stringify(input)),
    route: { runtime: 'module', module_artifact_id: CLAUDE_TASK_PROMPT_ARTIFACT_ID },
    input,
  };
  return { bootId, sourceText, envelope, context, command };
}

test('bridge.5 carries Store TaskPrompt bytes through a native UUID frame into admission', () => {
  const { bootId, envelope, context, command } = storeShapedDispatch();
  const taskPrompt = taskPromptFor(command, bootId);
  const inputId = 'd34d0055-5555-4555-8555-555555555555';
  const queued = [];

  const nativeInput = enqueueTaskPrompt(taskPrompt, { push: message => queued.push(message) }, inputId);
  assert.deepEqual(queued, [nativeInput]);
  assert.deepEqual(nativeInput, {
    type: 'user',
    message: { role: 'user', content: envelope.prompt },
    parent_tool_use_id: null,
    uuid: inputId,
  });
  assert.equal(Buffer.byteLength(nativeInput.message.content, 'utf8'), envelope.prompt_bytes);

  const nativeRootId = 'ses_task_prompt_fixture';
  const nativeFrame = {
    type: 'result',
    subtype: 'success',
    is_error: false,
    session_id: nativeRootId,
    uuid: 'task-prompt-result-frame',
    result_index: 0,
    user_message_uuid: inputId,
    user_message_uuids: [inputId],
    result: 'offline fixture result',
  };
  const state = createStreamState();
  applySdkMessage(state, {
    type: 'system',
    subtype: 'init',
    session_id: nativeRootId,
    model: 'fixture-model',
    uuid: 'task-prompt-init-frame',
  });
  applySdkMessage(state, nativeFrame);
  const execution = snapshot(state).input_executions[0];
  assert.equal(execution.native_input_id, inputId);
  assert.equal(execution.native_session_id, nativeRootId);
  assert.equal(execution.result_frame_uuid, nativeFrame.uuid);

  const admission = taskDispatchAdmission(command, taskPrompt, {
    moduleContract: {
      module_id: 'claude',
      artifact: { artifact_id: CLAUDE_TASK_PROMPT_ARTIFACT_ID, version: '5' },
      protocol: { major: 1, minor: 0 },
    },
    bootId,
    inputId,
    nativeRootId,
    nativeFrame,
  });
  assert.deepEqual(admission, {
    schema_version: 1,
    module_receipt: {
      schema_version: 1,
      module_id: 'claude',
      artifact: { artifact_id: CLAUDE_TASK_PROMPT_ARTIFACT_ID, version: '5', build_id: null },
      protocol: { major: 1, minor: 0 },
      binding_id: context.binding_id,
      binding_generation: context.binding_generation,
      operation_id: context.operation_id,
      input_sha256: command.input_sha256,
    },
    operation_id: context.operation_id,
    binding_id: context.binding_id,
    binding_generation: context.binding_generation,
    worker_boot_id: bootId,
    attempt_id: envelope.attempt_id,
    task_id: envelope.task_id,
    task_revision: envelope.task_revision,
    task_snapshot_sha256: envelope.task_snapshot_sha256,
    source_text_sha256: context.source_text_sha256,
    source_text_bytes: context.source_text_bytes,
    native_payload_sha256: envelope.prompt_sha256,
    native_payload_bytes: envelope.prompt_bytes,
    native_input_id: inputId,
  });
});

test('bridge.5 rejects altered or legacy snapshot input and admission without the exact native echo', () => {
  const { bootId, command, envelope, context } = storeShapedDispatch();
  const altered = {
    ...command,
    input: { ...command.input, task_prompt: { ...envelope, prompt: `${envelope.prompt}!` } },
  };
  assert.throws(() => taskPromptFor(altered, bootId), { message: 'TASK_PROMPT_INVALID' });

  const legacy = {
    ...command,
    input: { ...command.input, task_snapshot: { brief: { objective: 'legacy' } } },
  };
  assert.throws(() => taskPromptFor(legacy, bootId), { message: 'TASK_PROMPT_INVALID' });

  const taskPrompt = taskPromptFor(command, bootId);
  const frameWithoutEcho = {
    type: 'result',
    session_id: 'ses_task_prompt_fixture',
    uuid: 'unrelated-result-frame',
  };
  assert.throws(() => taskDispatchAdmission(command, taskPrompt, {
    moduleContract: {
      module_id: 'claude',
      artifact: { artifact_id: CLAUDE_TASK_PROMPT_ARTIFACT_ID, version: '5' },
      protocol: { major: 1, minor: 0 },
    },
    bootId,
    inputId: 'd34d0055-5555-4555-8555-555555555555',
    nativeRootId: frameWithoutEcho.session_id,
    nativeFrame: frameWithoutEcho,
  }), { message: 'TASK_DISPATCH_ECHO_REQUIRED' });

  assert.equal(context.operation_id, command.operation_id);
});
const connectedFixtureDir = process.env.ELIOT_TASK_PROMPT_V5_FIXTURE_DIR;
if (connectedFixtureDir) {
  test('Rust Store TaskPrompt v1 crosses into Claude bridge.5 consumer helpers unchanged and correlates echo UUID', async () => {
    const envelopeBytes = await readFile(join(connectedFixtureDir, 'envelope.json'));
    const dispatch = JSON.parse(await readFile(join(connectedFixtureDir, 'dispatch.json'), 'utf8'));
    assert.equal(sha256(envelopeBytes), dispatch.expected_envelope_json_sha256);

    const envelope = JSON.parse(envelopeBytes.toString('utf8'));
    const command = {
      ...dispatch.command,
      input: { ...dispatch.command.input, task_prompt: envelope },
    };
    const taskPrompt = taskPromptFor(command, dispatch.bootId);
    assert.strictEqual(taskPrompt.envelope, envelope);

    const queued = [];
    const nativeInput = enqueueTaskPrompt(
      taskPrompt,
      { push: message => queued.push(message) },
      dispatch.inputId,
      dispatch.nativeRootId,
    );
    assert.deepEqual(queued, [nativeInput]);
    assert.deepEqual(nativeInput, {
      type: 'user',
      message: { role: 'user', content: envelope.prompt },
      parent_tool_use_id: null,
      uuid: dispatch.inputId,
      session_id: dispatch.nativeRootId,
    });
    const nativeBytes = Buffer.from(nativeInput.message.content, 'utf8');
    assert.deepEqual(nativeBytes, Buffer.from(envelope.prompt, 'utf8'));
    assert.equal(nativeBytes.length, envelope.prompt_bytes);
    assert.equal(sha256(nativeBytes), envelope.prompt_sha256);

    const nativeFrame = {
      type: 'result',
      subtype: 'success',
      is_error: false,
      session_id: dispatch.nativeRootId,
      uuid: 'task-prompt-v5-offline-result-frame',
      result_index: 0,
      user_message_uuid: dispatch.inputId,
      user_message_uuids: [dispatch.inputId],
      result: 'offline fixture result',
    };
    const state = createStreamState();
    applySdkMessage(state, {
      type: 'system',
      subtype: 'init',
      session_id: dispatch.nativeRootId,
      model: 'offline-fixture',
      uuid: 'task-prompt-v5-offline-init-frame',
    });
    applySdkMessage(state, nativeFrame);
    const execution = snapshot(state).input_executions[0];
    assert.equal(execution.native_input_id, dispatch.inputId);
    assert.equal(execution.native_session_id, dispatch.nativeRootId);
    assert.equal(execution.result_frame_uuid, nativeFrame.uuid);

    const admission = taskDispatchAdmission(command, taskPrompt, {
      moduleContract: dispatch.moduleContract,
      bootId: dispatch.bootId,
      inputId: dispatch.inputId,
      nativeRootId: dispatch.nativeRootId,
      nativeFrame,
    });
    assert.equal(admission.operation_id, dispatch.command.operation_id);
    assert.equal(admission.binding_id, dispatch.command.binding_id);
    assert.equal(admission.binding_generation, dispatch.command.generation);
    assert.equal(admission.worker_boot_id, dispatch.bootId);
    assert.equal(admission.native_payload_sha256, envelope.prompt_sha256);
    assert.equal(admission.native_payload_bytes, envelope.prompt_bytes);
    assert.equal(admission.native_input_id, dispatch.inputId);

    await writeFile(join(connectedFixtureDir, 'admission.json'), JSON.stringify(admission));
  });
}
