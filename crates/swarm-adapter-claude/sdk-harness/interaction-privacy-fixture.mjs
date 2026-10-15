import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { projectInteractionInput } from './interaction-projection.mjs';

const bridgePath = fileURLToPath(new URL('./bridge.mjs', import.meta.url));
const root = await mkdtemp(path.join(tmpdir(), 'claude-interaction-privacy-'));
const packageDirectory = path.join(root, 'node_modules', '@anthropic-ai', 'claude-agent-sdk');
const bridgeBootId = process.env.CLAUDE_PRIVACY_FIXTURE_BOOT_ID ?? 'fixture-boot';
const nativeScopeKey = process.env.CLAUDE_PRIVACY_FIXTURE_SCOPE_KEY ?? 'claude:fixture';
const bindingId = process.env.CLAUDE_PRIVACY_FIXTURE_BINDING_ID ?? 'fixture-binding';
const token = `sk-ant-${'abcdefghijklmnop123456'}`;
const questionText = `Use ${token} for this test?`;
const rawInput = {
  questions: [{
    question: questionText,
    header: 'Choice',
    options: [
      { label: 'Keep', description: 'Continue.' },
      { label: 'Stop', description: 'Do not continue.' },
    ],
    multiSelect: false,
  }],
};
const dynamicPermissionKey = `C:\\private\\${token}`;
const permissionProjection = projectInteractionInput('permission', {
  [dynamicPermissionKey]: 'private permission value',
});
assert.equal(JSON.stringify(permissionProjection).includes(dynamicPermissionKey), false);
assert.deepEqual(permissionProjection.fields.map(field => field.name), ['field_1']);
const sdkSource = `
export async function startup({ options }) {
  return {
    query(prompt) {
      return (async function* () {
        const input = await prompt[Symbol.asyncIterator]().next();
        if (input.done) throw new Error('FIXTURE_INPUT_MISSING');
        yield { type: 'system', subtype: 'init', uuid: 'fixture-init', session_id: 'fixture-root' };
        const callbackInput = ${JSON.stringify(rawInput)};
        const toolUseID = 'toolu-privacy-fixture';
        const guard = await options.hooks.PreToolUse[0].hooks[0]({
          hook_event_name: 'PreToolUse',
          tool_name: 'AskUserQuestion',
          tool_use_id: toolUseID,
          tool_input: callbackInput,
        });
        if (guard.hookSpecificOutput.permissionDecision !== 'ask') {
          throw new Error('FIXTURE_PRETOOLUSE_GUARD_FAILED');
        }
        const result = await options.canUseTool('AskUserQuestion', callbackInput, {
          requestId: 'fixture-sdk-request',
          toolUseID,
          signal: new AbortController().signal,
        });
        const expected = {
          behavior: 'allow',
          updatedInput: {
            questions: callbackInput.questions,
            answers: { [callbackInput.questions[0].question]: 'Keep' },
          },
          toolUseID,
        };
        if (JSON.stringify(result) !== JSON.stringify(expected)) {
          throw new Error('FIXTURE_CALLBACK_RESTORATION_FAILED');
        }
        yield { type: 'result', uuid: 'fixture-restored-callback', session_id: 'fixture-root' };
      })();
    },
    close() {},
  };
}
`;

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();
    return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function digest(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

async function nextFrame(iterator) {
  let timer;
  const result = await Promise.race([
    iterator.next(),
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('FIXTURE_TIMEOUT')), 10_000);
    }),
  ]).finally(() => clearTimeout(timer));
  if (result.done) throw new Error('FIXTURE_BRIDGE_CLOSED_EARLY');
  return JSON.parse(result.value);
}

try {
  await mkdir(packageDirectory, { recursive: true });
  await writeFile(path.join(packageDirectory, 'package.json'), JSON.stringify({
    name: '@anthropic-ai/claude-agent-sdk',
    version: '0.3.287',
    type: 'module',
    main: './sdk.mjs',
  }));
  await writeFile(path.join(packageDirectory, 'sdk.mjs'), sdkSource);

  const child = spawn(process.execPath, [bridgePath], { stdio: ['pipe', 'pipe', 'pipe'] });
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  const iterator = lines[Symbol.asyncIterator]();
  let stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-4096); });
  const frames = [];
  const write = value => child.stdin.write(`${JSON.stringify(value)}\n`);
  const closePromise = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', code => resolve(code));
  });

  write({
    kind: 'prepare',
    operation_id: 'fixture-open-operation',
    workspace_root: root,
    sdk_runtime_root: root,
    bridge_boot_id: bridgeBootId,
    native_scope_key: nativeScopeKey,
    binding_id: bindingId,
    generation: 1,
    model_id: 'fixture-model',
    permission_mode: null,
    allow_dangerously_skip_permissions: false,
    native_executable: null,
  });
  const prepared = await nextFrame(iterator);
  frames.push(prepared);
  assert.equal(prepared.kind, 'prepared');

  write({
    kind: 'send',
    operation_id: 'fixture-dispatch-operation',
    initial_dispatch: true,
    native_root_id: null,
    user_message_uuid: '11111111-1111-4111-8111-111111111111',
    text: 'exercise callback projection',
  });
  let requestFrame;
  while (!requestFrame) {
    const frame = await nextFrame(iterator);
    frames.push(frame);
    if (frame.kind === 'interaction_request') requestFrame = frame;
    else if (frame.kind === 'harness_ended') throw new Error('FIXTURE_INTERACTION_NOT_EMITTED');
  }
  const request = requestFrame.request;
  assert.equal(request.schema_version, 2);
  assert.equal(request.request_id, 'fixture-sdk-request');
  assert.equal(request.input_sha256, digest(canonicalJson(request.input)));
  assert.equal(JSON.stringify(request).includes(token), false);
  assert.equal(JSON.stringify(request).includes(questionText), false);
  assert.equal(request.input.questions[0].question.includes('[redacted token]'), true);

  const visibleQuestion = request.input.questions[0].question;
  const reply = {
    type: 'question',
    updated_input: {
      questions: request.input.questions,
      answers: { [visibleQuestion]: 'Keep' },
    },
  };
  write({
    kind: 'control_reply',
    operation_id: 'fixture-reply-operation',
    bridge_boot_id: request.bridge_boot_id,
    native_root_id: request.native_root_id,
    native_scope_key: request.native_scope_key,
    request_id: request.request_id,
    request_sha256: request.request_sha256,
    input_sha256: request.input_sha256,
    interaction_kind: request.kind,
    reply_sha256: digest(canonicalJson(reply)),
    reply,
  });

  let ended = false;
  while (!ended) {
    const frame = await nextFrame(iterator);
    frames.push(frame);
    if (frame.kind === 'harness_ended') ended = true;
  }
  assert.equal(frames.some(frame => frame.kind === 'interaction_reply_ack'
    && frame.input_sha256 === request.input_sha256), true);
  assert.equal(frames.some(frame => frame.kind === 'sdk_frame'
    && frame.frame?.uuid === 'fixture-restored-callback'), true);
  assert.equal(JSON.stringify(frames).includes(token), false);
  assert.equal(JSON.stringify(frames).includes(questionText), false);
  write({ kind: 'stop' });
  child.stdin.end();
  const exitCode = await closePromise;
  assert.equal(exitCode, 0, stderr || 'fixture bridge exited unsuccessfully');
  const report = {
    passed: true,
    request_frame: requestFrame,
    reply_ack: frames.find(frame => frame.kind === 'interaction_reply_ack'),
    restored_callback_observed: frames.some(frame => frame.kind === 'sdk_frame'
      && frame.frame?.uuid === 'fixture-restored-callback'),
  };
  if (process.env.CLAUDE_PRIVACY_FIXTURE_JSON === '1') {
    process.stdout.write(`${JSON.stringify(report)}\n`);
  } else {
    console.log('PASS SDK harness interaction privacy: redacted IPC projection, exact digest fence, Node-only question-key restoration');
  }
} finally {
  await rm(root, { recursive: true, force: true });
}
