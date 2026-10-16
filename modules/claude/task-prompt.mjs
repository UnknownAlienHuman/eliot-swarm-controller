import { createHash } from 'node:crypto';

export const CLAUDE_TASK_PROMPT_ARTIFACT_ID = 'claude-agent-sdk-0.3.287-bridge.6';
export const CLAUDE_TASK_PROMPT_ARTIFACT_VERSION = '6';

const MAX_PROMPT_BYTES = 1_000_000;
const SHA256 = /^[0-9a-f]{64}$/;
const TASK_PROMPT_FIELDS = [
  'schema_id', 'schema_version', 'task_id', 'task_revision', 'attempt_id',
  'task_snapshot_sha256', 'prompt_sha256', 'prompt_bytes', 'prompt',
];
const TASK_DISPATCH_CONTEXT_FIELDS = [
  'schema_version', 'operation_id', 'binding_id', 'binding_generation', 'worker_boot_id',
  'attempt_id', 'task_id', 'task_revision', 'task_snapshot_sha256', 'source_text_sha256',
  'source_text_bytes',
];

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function exactFields(value, fields) {
  if (!isRecord(value)) return false;
  const actual = Object.keys(value).sort();
  const expected = [...fields].sort();
  return actual.length === expected.length && actual.every((field, index) => field === expected[index]);
}

function validIdentity(value, maxBytes = 512) {
  return typeof value === 'string' && value.trim().length > 0
    && Buffer.byteLength(value, 'utf8') <= maxBytes && !/\p{Cc}/u.test(value);
}

function invalidPrompt() {
  return new Error('TASK_PROMPT_INVALID');
}

export function taskPromptFor(command, bootId) {
  const input = command?.input;
  const envelope = input?.task_prompt;
  const context = input?.task_dispatch_context;
  const sourceText = input?.text;
  const sourceBytes = typeof sourceText === 'string' ? Buffer.byteLength(sourceText, 'utf8') : -1;

  if (!isRecord(input)
      || Object.hasOwn(input, 'task_snapshot')
      || Object.hasOwn(input, 'task_snapshot_canonical')
      || command?.method !== 'task.dispatch'
      || command?.route?.runtime !== 'module'
      || command?.route?.module_artifact_id !== CLAUDE_TASK_PROMPT_ARTIFACT_ID
      || !validIdentity(command?.operation_id)
      || !validIdentity(command?.binding_id)
      || !Number.isSafeInteger(command?.generation) || command.generation <= 0
      || !SHA256.test(command?.input_sha256 ?? '')
      || !exactFields(envelope, TASK_PROMPT_FIELDS)
      || !exactFields(context, TASK_DISPATCH_CONTEXT_FIELDS)
      || envelope.schema_id !== 'swarm.task_prompt' || envelope.schema_version !== 1
      || input.attempt_id !== envelope.attempt_id
      || !validIdentity(envelope.task_id) || !validIdentity(envelope.attempt_id)
      || !Number.isSafeInteger(envelope.task_revision) || envelope.task_revision <= 0
      || !SHA256.test(envelope.task_snapshot_sha256) || !SHA256.test(envelope.prompt_sha256)
      || typeof envelope.prompt !== 'string' || !envelope.prompt.trim()
      || !Number.isSafeInteger(envelope.prompt_bytes) || envelope.prompt_bytes <= 0
      || envelope.prompt_bytes > MAX_PROMPT_BYTES
      || Buffer.byteLength(envelope.prompt, 'utf8') !== envelope.prompt_bytes
      || sha256(Buffer.from(envelope.prompt, 'utf8')) !== envelope.prompt_sha256
      || context.schema_version !== 1
      || !validIdentity(context.operation_id) || !validIdentity(context.binding_id)
      || !validIdentity(context.worker_boot_id)
      || !validIdentity(context.attempt_id) || !validIdentity(context.task_id)
      || !Number.isSafeInteger(context.binding_generation) || context.binding_generation <= 0
      || !Number.isSafeInteger(context.task_revision) || context.task_revision <= 0
      || !SHA256.test(context.task_snapshot_sha256) || !SHA256.test(context.source_text_sha256)
      || !Number.isSafeInteger(context.source_text_bytes) || context.source_text_bytes <= 0
      || !validIdentity(bootId)
      || typeof sourceText !== 'string' || !sourceText.trim()
      || context.operation_id !== command.operation_id
      || context.binding_id !== command.binding_id
      || context.binding_generation !== command.generation
      || context.worker_boot_id !== bootId
      || context.source_text_bytes !== sourceBytes
      || context.source_text_sha256 !== sha256(Buffer.from(sourceText, 'utf8'))
      || context.attempt_id !== envelope.attempt_id
      || context.task_id !== envelope.task_id
      || context.task_revision !== envelope.task_revision
      || context.task_snapshot_sha256 !== envelope.task_snapshot_sha256) {
    throw invalidPrompt();
  }
  return { envelope, context };
}

export function taskPromptUserMessage(taskPrompt, inputId, nativeRootId = null) {
  if (!taskPrompt?.envelope || !validIdentity(inputId)
      || (nativeRootId !== null && !validIdentity(nativeRootId))) {
    throw invalidPrompt();
  }
  const message = {
    type: 'user',
    message: { role: 'user', content: taskPrompt.envelope.prompt },
    parent_tool_use_id: null,
    uuid: inputId,
  };
  if (nativeRootId !== null) message.session_id = nativeRootId;
  return message;
}

// Shared production/test seam for the SDK streaming-input queue. This does
// not render, prefix, suffix, or otherwise rewrite Store-owned prompt bytes.
export function enqueueTaskPrompt(taskPrompt, inputQueue, inputId, nativeRootId = null) {
  if (!inputQueue || typeof inputQueue.push !== 'function') throw invalidPrompt();
  const message = taskPromptUserMessage(taskPrompt, inputId, nativeRootId);
  inputQueue.push(message);
  return message;
}

export function moduleReceipt(command, contract) {
  if (!validIdentity(command?.operation_id) || !validIdentity(command?.binding_id)
      || !Number.isSafeInteger(command?.generation) || command.generation <= 0
      || command?.route?.runtime !== 'module'
      || command?.route?.module_artifact_id !== CLAUDE_TASK_PROMPT_ARTIFACT_ID
      || !SHA256.test(command?.input_sha256 ?? '')
      || contract?.module_id !== 'claude'
      || contract?.artifact?.artifact_id !== CLAUDE_TASK_PROMPT_ARTIFACT_ID
      || contract?.artifact?.version !== CLAUDE_TASK_PROMPT_ARTIFACT_VERSION
      || (contract.artifact.build_id != null && !validIdentity(contract.artifact.build_id))
      || contract?.protocol?.major !== 1 || contract?.protocol?.minor !== 0) {
    throw new Error('HOST_COMMAND_IDENTITY_INVALID');
  }
  return {
    schema_version: 1,
    module_id: contract.module_id,
    artifact: {
      artifact_id: contract.artifact.artifact_id,
      version: contract.artifact.version,
      build_id: contract.artifact.build_id ?? null,
    },
    protocol: { ...contract.protocol },
    binding_id: command.binding_id,
    binding_generation: command.generation,
    operation_id: command.operation_id,
    input_sha256: command.input_sha256,
  };
}

export function taskDispatchAdmission(command, taskPrompt, evidence) {
  const { envelope, context } = taskPrompt ?? {};
  const inputId = evidence?.inputId;
  const nativeRootId = evidence?.nativeRootId;
  const nativeFrame = evidence?.nativeFrame;
  const echoed = new Set();
  if (typeof nativeFrame?.user_message_uuid === 'string') echoed.add(nativeFrame.user_message_uuid);
  if (Array.isArray(nativeFrame?.user_message_uuids)) {
    for (const id of nativeFrame.user_message_uuids) if (typeof id === 'string') echoed.add(id);
  }
  if (!envelope || !context || evidence?.bootId !== context.worker_boot_id
      || !validIdentity(nativeRootId) || nativeFrame?.session_id !== nativeRootId
      || !validIdentity(inputId) || !echoed.has(inputId)) {
    throw new Error('TASK_DISPATCH_ECHO_REQUIRED');
  }
  return {
    schema_version: context.schema_version,
    module_receipt: moduleReceipt(command, evidence.moduleContract),
    operation_id: context.operation_id,
    binding_id: context.binding_id,
    binding_generation: context.binding_generation,
    worker_boot_id: context.worker_boot_id,
    attempt_id: context.attempt_id,
    task_id: context.task_id,
    task_revision: context.task_revision,
    task_snapshot_sha256: context.task_snapshot_sha256,
    source_text_sha256: context.source_text_sha256,
    source_text_bytes: context.source_text_bytes,
    native_payload_sha256: envelope.prompt_sha256,
    native_payload_bytes: envelope.prompt_bytes,
    native_input_id: inputId,
  };
}
