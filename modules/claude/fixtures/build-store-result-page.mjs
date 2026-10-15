// Offline Rust-Store/Bridge.5 page producer fixture. The message shape follows
// @anthropic-ai/claude-agent-sdk 0.3.287 SDKResultMessage; it is not a live run.
import { stdin, stdout } from 'node:process';
import { createInterface } from 'node:readline';
import { applySdkMessage, createStreamState, resultSourceForInput } from '../stream.mjs';
import { publishClaudeResultPage } from '../result-pages.mjs';

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
const callStore = async (method, args) => {
  if (method !== 'module.result') throw new Error('FIXTURE_UNEXPECTED_STORE_METHOD');
  stdout.write(`${JSON.stringify({ type: method, method, ...args })}\n`);
  const replyLine = await lines.next();
  if (replyLine.done) throw new Error('FIXTURE_STORE_ACK_LOST');
  const reply = JSON.parse(replyLine.value);
  if (reply.type === 'error') throw new Error(`FIXTURE_STORE_REJECTED:${reply.code}`);
  if (reply.type !== 'ack') throw new Error('FIXTURE_STORE_ACK_INVALID');
  return reply.acknowledgement;
};
const published = await publishClaudeResultPage(
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
);
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
    async () => { replayCallCount++; return published.acknowledgement; },
    attempts,
  );
  throw new Error('FIXTURE_RESULT_REPLAY_WAS_NOT_PREVENTED');
} catch (error) {
  if (error?.diagnosticCode !== 'CLAUDE_RESULT_PAGE_REPLAY_PREVENTED') throw error;
}
stdout.write(`${JSON.stringify({
  type: 'terminal',
  page: published.page,
  execution: resultSource.execution,
  acknowledgement: published.acknowledgement,
  replay_call_count: replayCallCount,
})}\n`);
reader.close();
stdin.pause();
