import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { moduleReceipt } from './task-prompt.mjs';

const MAX_RESULT_BYTES = 512_000;
const MAX_PAGE_BYTES = 65_536;
const MAX_UNCERTAIN_RESULT_ATTEMPTS = 128;
const SHA256 = /^[0-9a-f]{64}$/;

function fail(code, uncertain = false) {
  const error = new Error(code);
  error.diagnosticCode = code;
  error.nativeAdmissionPossible = uncertain;
  return error;
}

function identity(value) {
  return typeof value === 'string' && value.length > 0 && value.length <= 512;
}

function digest(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function resultOrigin(command, moduleContract, identityContext) {
  const origin = command?.input?.claude_result_origin;
  const selector = command?.input?.selector;
  const { bridgeBootId, nativeRootId, nativeScopeKey } = identityContext ?? {};
  const targetReceipt = origin?.target_module_receipt;
  const admission = origin?.dispatch_admission;
  if (command?.method !== 'agent.result'
      || command?.route?.runtime !== 'module'
      || !identity(command?.operation_id)
      || !identity(command?.binding_id)
      || !Number.isSafeInteger(command?.generation) || command.generation <= 0
      || !SHA256.test(command?.input_sha256 ?? '')
      || !SHA256.test(command?.target_input_sha256 ?? '')
      || !identity(bridgeBootId) || !identity(nativeRootId) || !identity(nativeScopeKey)
      || selector?.kind !== 'claude_assistant_result'
      || !identity(selector?.input_operation_id) || !identity(selector?.session_id)
      || origin?.schema_version !== 1
      || origin.kind !== 'claude_assistant_result'
      || origin.result_operation_id !== command.operation_id
      || origin.result_input_sha256 !== command.input_sha256
      || origin.result_selector?.kind !== selector.kind
      || origin.result_selector?.input_operation_id !== selector.input_operation_id
      || origin.result_selector?.session_id !== selector.session_id
      || origin.target_operation_id !== selector.input_operation_id
      || origin.target_method !== 'task.dispatch'
      || origin.target_input_sha256 !== command.target_input_sha256
      || origin.binding_id !== command.binding_id
      || origin.binding_generation !== command.generation
      || origin.bridge_boot_id !== bridgeBootId
      || origin.native_session_id !== selector.session_id
      || origin.native_session_id !== nativeRootId
      || origin.native_scope_key !== nativeScopeKey
      || !identity(origin.native_input_id)
      || !SHA256.test(origin.native_payload_sha256 ?? '')
      || !Number.isSafeInteger(origin.native_payload_bytes)
      || origin.native_payload_bytes <= 0
      || !targetReceipt || targetReceipt.operation_id !== origin.target_operation_id
      || targetReceipt.binding_id !== command.binding_id
      || targetReceipt.binding_generation !== command.generation
      || targetReceipt.input_sha256 !== command.target_input_sha256
      || targetReceipt.module_id !== moduleContract?.module_id
      || targetReceipt.artifact?.artifact_id !== moduleContract?.artifact?.artifact_id
      || targetReceipt.artifact?.version !== moduleContract?.artifact?.version
      || !isDeepStrictEqual(targetReceipt.protocol, moduleContract?.protocol)
      || !admission || admission.operation_id !== origin.target_operation_id
      || admission.binding_id !== command.binding_id
      || admission.binding_generation !== command.generation
      || admission.worker_boot_id !== bridgeBootId
      || admission.native_input_id !== origin.native_input_id
      || admission.native_payload_sha256 !== origin.native_payload_sha256
      || admission.native_payload_bytes !== origin.native_payload_bytes
      || !isDeepStrictEqual(admission.module_receipt, targetReceipt)
      || origin.descriptor?.module_id !== moduleContract?.module_id
      || origin.descriptor?.artifact?.artifact_id !== moduleContract?.artifact?.artifact_id
      || origin.descriptor?.artifact?.version !== moduleContract?.artifact?.version
      || !isDeepStrictEqual(origin.descriptor?.protocol, moduleContract?.protocol)
      || origin.descriptor?.module_artifact_id !== moduleContract?.artifact?.artifact_id
      || !Array.isArray(origin.descriptor?.capabilities)
      || !origin.descriptor.capabilities.includes('agent.result')) {
    throw fail('CLAUDE_RESULT_SCOPE_INVALID');
  }
  return origin;
}

export function buildClaudeResultPage(command, execution, bodySource, moduleContract, identityContext) {
  const origin = resultOrigin(command, moduleContract, identityContext);
  if (!execution || execution.correlation !== 'unique'
      || execution.native_session_id !== origin.native_session_id
      || execution.native_input_id !== origin.native_input_id
      || execution.user_message_uuids?.length !== 1
      || execution.user_message_uuids[0] !== origin.native_input_id
      || !identity(execution.result_frame_uuid)
      || !Number.isSafeInteger(execution.result_bytes)
      || execution.result_bytes < 0 || execution.result_bytes > MAX_RESULT_BYTES
      || !SHA256.test(execution.result_sha256 ?? '')
      || !['completed', 'failed'].includes(execution.terminal_status)
      || !identity(execution.result_subtype)
      || typeof bodySource?.body !== 'string'
      || bodySource.bytes !== execution.result_bytes
      || bodySource.sha256 !== execution.result_sha256) {
    throw fail('CLAUDE_RESULT_SOURCE_UNAVAILABLE');
  }

  const resultBytes = Buffer.from(bodySource.body, 'utf8');
  if (resultBytes.length !== execution.result_bytes
      || digest(resultBytes) !== execution.result_sha256) {
    throw fail('CLAUDE_RESULT_SOURCE_DIGEST_MISMATCH');
  }
  const resultReceipt = moduleReceipt(command, moduleContract);
  const offset = command.input.offset_bytes ?? 0;
  const length = Math.min(command.input.length_bytes ?? MAX_PAGE_BYTES, MAX_PAGE_BYTES);
  if (!Number.isSafeInteger(offset) || offset < 0
      || !Number.isSafeInteger(length) || length < 0
      || offset > resultBytes.length || (length === 0 && offset < resultBytes.length)) {
    throw fail('CLAUDE_RESULT_RANGE_INVALID');
  }
  const end = Math.min(offset + length, resultBytes.length);
  const pageBytes = resultBytes.subarray(offset, end);
  const source = {
    kind: 'claude_assistant_result',
    bridge_boot_id: origin.bridge_boot_id,
    result_operation_id: command.operation_id,
    result_input_sha256: command.input_sha256,
    result_module_receipt: resultReceipt,
    input_operation_id: origin.target_operation_id,
    target_method: 'task.dispatch',
    target_input_sha256: origin.target_input_sha256,
    target_module_receipt: origin.target_module_receipt,
    native_session_id: origin.native_session_id,
    native_input_id: origin.native_input_id,
    native_payload_sha256: origin.native_payload_sha256,
    native_payload_bytes: origin.native_payload_bytes,
    result_frame_uuid: execution.result_frame_uuid,
    result_subtype: execution.result_subtype,
    result_status: execution.terminal_status,
    result_sha256: execution.result_sha256,
    result_bytes: execution.result_bytes,
    content_digest: `sha256:${execution.result_sha256}`,
    native_output: 'claude.assistant.result',
    evidence: 'exact_claude_sdk_assistant_result',
    native_response_identity: 'sdk_result_frame',
    execution_complete: true,
    task_completion: 'unknown',
    native_replay: false,
  };
  return {
    source,
    offset_bytes: offset,
    byte_length: pageBytes.length,
    total_bytes: resultBytes.length,
    eof: end === resultBytes.length,
    media_type: 'text/plain; charset=utf-8',
    content_base64: pageBytes.toString('base64'),
    page_sha256: digest(pageBytes),
  };
}

export async function publishClaudeResultPage(
  command,
  execution,
  bodySource,
  moduleContract,
  identityContext,
  call,
  attempts,
  resultJournal,
) {
  const page = buildClaudeResultPage(
    command,
    execution,
    bodySource,
    moduleContract,
    identityContext,
  );
  if (typeof call !== 'function' || !(attempts instanceof Map)
      || !(resultJournal instanceof Map)) {
    throw fail('CLAUDE_RESULT_PUBLISHER_UNAVAILABLE');
  }
  if (attempts.has(command.operation_id)
      || resultJournal.get(command.operation_id)?.method === 'agent.result') {
    throw fail('CLAUDE_RESULT_PAGE_REPLAY_PREVENTED', true);
  }
  if (attempts.size >= MAX_UNCERTAIN_RESULT_ATTEMPTS) {
    throw fail('CLAUDE_RESULT_UNCERTAIN_CAPACITY');
  }
  // This caller-owned map holds only in-flight or ACK-uncertain Operations.
  // Confirmed pages release the slot and converge into the existing journal.
  attempts.set(command.operation_id, true);
  let acknowledgement;
  try {
    acknowledgement = await call('module.result', {
      operation_id: command.operation_id,
      page,
    });
  } catch {
    throw fail('CLAUDE_RESULT_ACK_UNKNOWN', true);
  }
  if (acknowledgement?.recorded !== true
      || typeof acknowledgement.artifact_ref !== 'string'
      || !acknowledgement.artifact_ref.trim()) {
    throw fail('CLAUDE_RESULT_ACK_UNKNOWN', true);
  }
  const origin = command.input.claude_result_origin;
  resultJournal.delete(command.operation_id);
  resultJournal.set(command.operation_id, {
    method: 'agent.result',
    outcome: 'applied',
    completion_condition: 'result_page_recorded',
    artifact_ref: acknowledgement.artifact_ref,
    target_operation_id: origin.target_operation_id,
    result_frame_uuid: page.source.result_frame_uuid,
    bridge_boot_id: identityContext.bridgeBootId,
  });
  while (resultJournal.size > 128) resultJournal.delete(resultJournal.keys().next().value);
  attempts.delete(command.operation_id);
  return { page, acknowledgement };
}

export function acknowledgeClaudeResultOutcome(
  operationId,
  journalEntry,
  acknowledgement,
  attempts,
) {
  if (!(attempts instanceof Map)
      || journalEntry?.method !== 'agent.result'
      || journalEntry.outcome !== 'unknown'
      || acknowledgement?.recorded !== true
      || acknowledgement.state !== 'settled'
      || !(acknowledgement.superseded === true || acknowledgement.replayed === true)) {
    return false;
  }
  return attempts.delete(operationId);
}
