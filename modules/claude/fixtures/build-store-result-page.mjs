// Offline Rust-Store/Bridge.5 page producer fixture. The message shape follows
// @anthropic-ai/claude-agent-sdk 0.3.287 SDKResultMessage; it is not a live run.
import { stdin, stdout } from 'node:process';
import { createInterface } from 'node:readline';
import { applySdkMessage, createStreamState, resultSourceForInput } from '../stream.mjs';
import {
  acknowledgeClaudeResultOutcome, publishClaudeResultPage,
} from '../result-pages.mjs';
import { moduleReceipt } from '../task-prompt.mjs';

const reader = createInterface({ input: stdin, crlfDelay: Infinity });
const lines = reader[Symbol.asyncIterator]();
const firstLine = await lines.next();
if (firstLine.done) throw new Error('FIXTURE_RESULT_INPUT_MISSING');
const input = JSON.parse(firstLine.value);
const origin = input.command?.input?.claude_result_origin;
if (!origin) throw new Error('FIXTURE_RESULT_ORIGIN_MISSING');
const moduleContract = input.module_contract;
if (!moduleContract) throw new Error('FIXTURE_MODULE_CONTRACT_MISSING');

const stream = createStreamState();
applySdkMessage(stream, {
  type: 'system',
  subtype: 'init',
  uuid: input.init_frame_uuid,
  session_id: origin.native_session_id,
  model: input.model,
});
applySdkMessage(stream, input.result_message);
const resultSource = resultSourceForInput(
  stream,
  origin.native_input_id,
  origin.native_session_id,
);
const attempts = new Map();
const journal = new Map();
let resultCallCount = 0;
let outcomeCallCount = 0;
const callStore = async (method, args) => {
  if (method === 'module.result') resultCallCount++;
  else if (method === 'module.outcome') outcomeCallCount++;
  else throw new Error('FIXTURE_UNEXPECTED_STORE_METHOD');
  stdout.write(`${JSON.stringify({ type: method, method, args })}\n`);
  const replyLine = await lines.next();
  if (replyLine.done) throw new Error('FIXTURE_STORE_ACK_LOST');
  const reply = JSON.parse(replyLine.value);
  if (reply.type === 'drop_ack' && method === 'module.result') {
    throw new Error('FIXTURE_STORE_ACK_WITHHELD_AFTER_COMMIT');
  }
  if (reply.type === 'error') throw new Error(`FIXTURE_STORE_REJECTED:${reply.code}`);
  if (reply.type !== 'ack') throw new Error('FIXTURE_STORE_ACK_INVALID');
  return reply.acknowledgement;
};

let published = null;
let resultFailure = null;
let moduleOutcome = null;
let moduleOutcomeAck = null;
try {
  published = await publishClaudeResultPage(
    input.command,
    resultSource?.execution,
    resultSource?.body,
    moduleContract,
    {
      bridgeBootId: origin.bridge_boot_id,
      nativeRootId: origin.native_session_id,
      nativeScopeKey: origin.native_scope_key,
    },
    callStore,
    attempts,
    journal,
  );
} catch (error) {
  if (error?.diagnosticCode !== 'CLAUDE_RESULT_ACK_UNKNOWN'
      || error.nativeAdmissionPossible !== true) throw error;
  resultFailure = error;
  moduleOutcome = {
    operation_id: input.command.operation_id,
    outcome: 'unknown',
    native_root_id: origin.native_session_id,
    native_scope_key: origin.native_scope_key,
    details: {
      module_receipt: moduleReceipt(input.command, moduleContract),
      error_type: error.name,
      diagnostic_code: error.diagnosticCode,
    },
  };
  journal.set(input.command.operation_id, {
    method: 'agent.result',
    outcome: 'unknown',
    completion_condition: null,
    diagnostic_code: error.diagnosticCode,
  });
  moduleOutcomeAck = await callStore('module.outcome', moduleOutcome);
  acknowledgeClaudeResultOutcome(
    input.command.operation_id,
    journal.get(input.command.operation_id),
    moduleOutcomeAck,
    attempts,
  );
}

let replayCallCount = 0;
try {
  await publishClaudeResultPage(
    input.command,
    resultSource?.execution,
    resultSource?.body,
    moduleContract,
    {
      bridgeBootId: origin.bridge_boot_id,
      nativeRootId: origin.native_session_id,
      nativeScopeKey: origin.native_scope_key,
    },
    async () => { replayCallCount++; return published?.acknowledgement; },
    attempts,
    journal,
  );
  throw new Error('FIXTURE_RESULT_REPLAY_WAS_NOT_PREVENTED');
} catch (error) {
  if (error?.diagnosticCode !== 'CLAUDE_RESULT_PAGE_REPLAY_PREVENTED') throw error;
}

stdout.write(`${JSON.stringify({
  type: 'terminal',
  page: published?.page ?? null,
  execution: resultSource?.execution ?? null,
  result_ack: published?.acknowledgement ?? null,
  result_failure: resultFailure
    ? { diagnostic_code: resultFailure.diagnosticCode, native_admission_possible: resultFailure.nativeAdmissionPossible }
    : null,
  module_outcome: moduleOutcome,
  module_outcome_ack: moduleOutcomeAck,
  result_call_count: resultCallCount,
  outcome_call_count: outcomeCallCount,
  attempt_count: attempts.size,
  journal_entry: journal.get(input.command.operation_id) ?? null,
  replay_call_count: replayCallCount,
})}\n`);
reader.close();
stdin.pause();
