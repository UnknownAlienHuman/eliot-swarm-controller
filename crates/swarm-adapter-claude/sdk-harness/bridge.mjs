// Private SDK API driver for the Rust Claude module adapter.
//
// The live SDK WarmQuery, query stream, and input iterator must remain in Node.
// Operation matching, session adoption, receipts, and readback are Rust-owned.
// This process never connects to the Store and only emits a bounded metadata
// projection; prompt, transcript, and tool arguments stay local. A bounded
// final result body is emitted only when it fits the result boundary.
import { createInterface } from 'node:readline';
import { createHash, randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { readFile } from 'node:fs/promises';
import { prepareQuery } from './prepared-query.mjs';

const MAX_INPUT_BYTES = 1_100_000;
const MAX_OUTPUT_BYTES = 700_000;
const MAX_RESULT_BODY_BYTES = 512_000;
const MAX_INPUT_QUEUE = 64;
const MAX_PENDING_INTERACTIONS = 8;
const MAX_INTERACTION_REQUEST_BYTES = 32 * 1024;
const MAX_INTERACTION_REPLY_BYTES = 16 * 1024;
const PERMISSION_MODES = ['default', 'acceptEdits', 'bypassPermissions', 'plan', 'dontAsk', 'auto'];

let session = null;
let stopping = false;

function emit(value) {
  const line = JSON.stringify(value);
  if (Buffer.byteLength(line, 'utf8') > MAX_OUTPUT_BYTES) {
    process.stdout.write(`${JSON.stringify({ kind: 'diagnostic', code: 'HARNESS_OUTPUT_BOUNDARY' })}\n`);
    return;
  }
  process.stdout.write(`${line}\n`);
}

function diagnostic(error, fallback) {
  const code = typeof error?.diagnosticCode === 'string' ? error.diagnosticCode : fallback;
  return /^[A-Z][A-Z0-9_]{0,63}$/.test(code) ? code : fallback;
}

function boundedSdkText(value, maximum) {
  return typeof value === 'string'
    && value.length > 0
    && value.length <= maximum
    && !/[\u0000-\u001f\u007f]/.test(value)
    ? value
    : null;
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();
    return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function allowedKeys(value, expected) {
  return value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).length === expected.length
    && expected.every(key => Object.hasOwn(value, key));
}

function validAnswer(value) {
  return typeof value === 'string' && value.trim().length > 0
    && Buffer.byteLength(value, 'utf8') <= 4_096
    && !value.includes('\u0000');
}

function safeSdkResourceLinks(value) {
  if (!Array.isArray(value)) return null;
  const links = value.slice(0, 50).map(link => {
    const uri = boundedSdkText(link?.uri, 2_048);
    const name = boundedSdkText(link?.name, 256);
    if (uri === null || name === null) return null;
    const safe = { uri, name };
    const title = boundedSdkText(link?.title, 256);
    const mimeType = boundedSdkText(link?.mimeType, 128);
    if (title !== null) safe.title = title;
    if (mimeType !== null) safe.mimeType = mimeType;
    if (Number.isSafeInteger(link?.size) && link.size >= 0) safe.size = link.size;
    return safe;
  }).filter(Boolean);
  return {
    links,
    count: value.length,
    overflow: value.length > 50,
  };
}

function safeSdkFrame(message) {
  const frame = {
    type: boundedSdkText(message?.type, 32) ?? 'unknown',
    subtype: boundedSdkText(message?.subtype, 64),
    session_id: boundedSdkText(message?.session_id, 512),
    uuid: boundedSdkText(message?.uuid, 128),
    user_message_uuid: boundedSdkText(message?.user_message_uuid, 128),
    result_index: Number.isSafeInteger(message?.result_index) ? message.result_index : null,
    is_error: typeof message?.is_error === 'boolean' ? message.is_error : null,
    stop_reason: boundedSdkText(message?.stop_reason, 128),
  };
  if (Array.isArray(message?.user_message_uuids)) {
    const source = message.user_message_uuids;
    frame.user_message_uuids = source.slice(0, 64)
      .filter(value => boundedSdkText(value, 128) !== null);
    frame.user_message_uuids_count = source.length;
    frame.user_message_uuids_overflow = source.length > 64;
  }
  if (message?.type === 'system' && message.subtype === 'init') {
    frame.model = boundedSdkText(message.model, 256);
    frame.claude_code_version = boundedSdkText(message.claude_code_version, 64);
    frame.permission_mode = boundedSdkText(message.permissionMode, 64);
    frame.tools_count = Array.isArray(message.tools) ? message.tools.length : null;
    frame.effort = boundedSdkText(message.effort, 64);
  }
  if (message?.type === 'system' && [
    'task_started', 'task_progress', 'task_notification', 'task_updated',
  ].includes(message.subtype)) {
    frame.task_id = boundedSdkText(message.task_id, 256);
    frame.tool_use_id = boundedSdkText(message.tool_use_id, 256);
    frame.task_type = boundedSdkText(message.task_type, 64);
    frame.subagent_type = boundedSdkText(message.subagent_type, 128);
    frame.task_status = boundedSdkText(message.status, 32);
    frame.task_reason = boundedSdkText(message.reason, 64);
    frame.task_last_tool_name = boundedSdkText(message.last_tool_name, 128);
    frame.is_backgrounded = typeof message.is_backgrounded === 'boolean' ? message.is_backgrounded : null;
    frame.spawn_depth = Number.isSafeInteger(message.spawn_depth)
      && message.spawn_depth >= 0 && message.spawn_depth <= 128
      ? message.spawn_depth
      : null;
    frame.ambient = typeof message.ambient === 'boolean' ? message.ambient : null;
    if (message.subtype === 'task_updated') {
      const patch = message.patch && typeof message.patch === 'object' ? message.patch : {};
      frame.task_patch_status = boundedSdkText(patch.status, 32);
      frame.task_patch_is_backgrounded = typeof patch.is_backgrounded === 'boolean'
        ? patch.is_backgrounded
        : null;
    }
    const resourceLinks = safeSdkResourceLinks(message.resource_links);
    if (resourceLinks !== null) {
      frame.resource_links = resourceLinks.links;
      frame.resource_links_count = resourceLinks.count;
      frame.resource_links_overflow = resourceLinks.overflow;
    }
  }
  if (message?.type === 'assistant' || message?.type === 'user') {
    frame.parent_tool_use_id = boundedSdkText(message.parent_tool_use_id, 256);
    frame.subagent_type = boundedSdkText(message.subagent_type, 128);
  }
  if (message?.type === 'system' && message.subtype === 'permission_denied') {
    frame.agent_id = boundedSdkText(message.agent_id, 256);
    frame.agent_type = boundedSdkText(message.agent_type, 128);
    frame.tool_use_id = boundedSdkText(message.tool_use_id, 256);
  }
  if (message?.type === 'result') {
    const output = typeof message.result === 'string' ? message.result : null;
    frame.result_sha256 = output === null ? null : createHash('sha256').update(output, 'utf8').digest('hex');
    frame.result_bytes = output === null ? null : Buffer.byteLength(output, 'utf8');
    if (output === null) {
      frame.result_body_available = false;
    } else if (frame.result_bytes <= MAX_RESULT_BODY_BYTES) {
      frame.result_content_base64 = Buffer.from(output, 'utf8').toString('base64');
      frame.result_body_available = true;
    } else {
      frame.result_body_available = false;
      frame.result_body_truncated = true;
    }
  }
  return frame;
}

function safeSubagentHookFrame(subtype, input, toolUseID) {
  return {
    type: 'hook',
    subtype,
    session_id: boundedSdkText(input?.session_id, 512),
    agent_id: boundedSdkText(input?.agent_id, 256),
    agent_type: boundedSdkText(input?.agent_type, 128),
    hook_tool_use_id: boundedSdkText(toolUseID, 256),
    prompt_id: boundedSdkText(input?.prompt_id, 128),
  };
}

function captureSubagentHook(subtype) {
  return async (input, toolUseID) => {
    emit({ kind: 'sdk_frame', frame: safeSubagentHookFrame(subtype, input, toolUseID) });
    return {};
  };
}

async function pump(sess) {
  try {
    for await (const message of sess.query) {
      if (message?.type === 'system' && message.subtype === 'init') {
        const root = boundedSdkText(message.session_id, 512);
        if (root !== null) {
          if (sess.nativeRootId !== null && sess.nativeRootId !== root) {
            throw Object.assign(new Error(), { diagnosticCode: 'SDK_ROOT_ID_CHANGED' });
          }
          sess.nativeRootId = root;
        }
      }
      // Raw SDK content remains in the SDK process except for the bounded final
      // result body; Rust owns receipt correlation and verifies its digest.
      if (message?.type !== 'stream_event') emit({ kind: 'sdk_frame', frame: safeSdkFrame(message) });
    }
  } catch (error) {
    emit({ kind: 'diagnostic', code: diagnostic(error, 'SDK_STREAM_FAILURE') });
  }
  sess.ended = true;
  for (const pending of [...sess.pendingInteractions.values()]) {
    retireInteraction(sess, pending, 'SDK_STREAM_ENDED');
  }
  emit({ kind: 'harness_ended', diagnostic_code: 'SDK_STREAM_ENDED' });
}

function sdkImportEntry(packageInfo, packageDirectory) {
  let target = packageInfo.exports;
  if (target && typeof target === 'object' && !Array.isArray(target) && !Object.hasOwn(target, 'import')) {
    target = target['.'];
  }
  const choose = value => {
    if (typeof value === 'string') return value;
    if (Array.isArray(value)) return value.map(choose).find(Boolean) ?? null;
    if (!value || typeof value !== 'object') return null;
    for (const condition of ['import', 'node', 'default']) {
      if (Object.hasOwn(value, condition)) {
        const selected = choose(value[condition]);
        if (selected) return selected;
      }
    }
    return null;
  };
  const relativeEntry = choose(target) ?? packageInfo.module ?? packageInfo.main;
  if (typeof relativeEntry !== 'string' || !relativeEntry.startsWith('./')) return null;
  const resolved = path.resolve(packageDirectory, relativeEntry);
  const relative = path.relative(packageDirectory, resolved);
  if (!relative || relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return null;
  return resolved;
}

async function prepare(command) {
  if (session && !session.ended) throw Object.assign(new Error(), { diagnosticCode: 'SESSION_ALREADY_OPEN' });
  const nodeMajor = Number.parseInt(process.versions.node.split('.')[0], 10);
  if (!Number.isSafeInteger(nodeMajor) || nodeMajor < 22) {
    throw Object.assign(new Error(), { diagnosticCode: 'NODE_VERSION_UNSUPPORTED' });
  }
  if (!path.isAbsolute(command.workspace_root) || !path.isAbsolute(command.sdk_runtime_root)) {
    throw Object.assign(new Error(), { diagnosticCode: 'PATH_MUST_BE_ABSOLUTE' });
  }
  if (typeof command.operation_id !== 'string' || !command.operation_id.trim()
      || typeof command.native_scope_key !== 'string' || !command.native_scope_key.trim()
      || typeof command.bridge_boot_id !== 'string' || !command.bridge_boot_id.trim()
      || typeof command.binding_id !== 'string' || !command.binding_id.trim()
      || !Number.isSafeInteger(command.generation) || command.generation < 1) {
    throw Object.assign(new Error(), { diagnosticCode: 'PREPARE_IDENTITY_REQUIRED' });
  }
  if (typeof command.model_id !== 'string' || !command.model_id.trim() || command.model_id !== command.model_id.trim()) {
    throw Object.assign(new Error(), { diagnosticCode: 'MODEL_ID_REQUIRED' });
  }
  if (command.permission_mode !== null && !PERMISSION_MODES.includes(command.permission_mode)) {
    throw Object.assign(new Error(), { diagnosticCode: 'INVALID_PERMISSION_MODE' });
  }
  if (command.permission_mode === 'bypassPermissions' && command.allow_dangerously_skip_permissions !== true) {
    throw Object.assign(new Error(), { diagnosticCode: 'BYPASS_REQUIRES_EXPLICIT_ROUTE_FLAG' });
  }
  const packageDirectory = path.join(command.sdk_runtime_root, 'node_modules', '@anthropic-ai', 'claude-agent-sdk');
  const packageFile = path.join(packageDirectory, 'package.json');
  const packageInfo = JSON.parse(await readFile(packageFile, 'utf8'));
  const sdkVersion = boundedSdkText(packageInfo.version, 64);
  if (packageInfo.name !== '@anthropic-ai/claude-agent-sdk'
      || packageInfo.version !== '0.3.287' || sdkVersion === null) {
    throw Object.assign(new Error(), { diagnosticCode: 'SDK_VERSION_MISMATCH' });
  }
  const sdkEntry = sdkImportEntry(packageInfo, packageDirectory);
  if (!sdkEntry) throw Object.assign(new Error(), { diagnosticCode: 'SDK_IMPORT_ENTRY_UNAVAILABLE' });
  const sdk = await import(pathToFileURL(sdkEntry).href);
  if (typeof sdk.startup !== 'function') throw Object.assign(new Error(), { diagnosticCode: 'SDK_STARTUP_UNAVAILABLE' });

  const modelOption = { model: command.model_id };
  const sess = {
    input: new InputQueue(),
    query: null,
    preparedQuery: null,
    requestedModel: modelOption.model,
    bridgeBootId: command.bridge_boot_id,
    nativeScopeKey: command.native_scope_key,
    bindingId: command.binding_id,
    generation: command.generation,
    nativeRootId: null,
    pendingInteractions: new Map(),
    ended: false,
  };
  const options = {
    cwd: command.workspace_root,
    ...modelOption,
    includePartialMessages: true,
    forwardSubagentText: true,
    canUseTool: (toolName, toolInput, toolOptions) => requestInteraction(sess, toolName, toolInput, toolOptions),
    hooks: {
      SubagentStart: [{ hooks: [captureSubagentHook('subagent_started')] }],
      SubagentStop: [{ hooks: [captureSubagentHook('subagent_stopped')] }],
    },
  };
  if (command.permission_mode !== null) options.permissionMode = command.permission_mode;
  if (command.permission_mode === 'bypassPermissions') options.allowDangerouslySkipPermissions = true;
  if (typeof command.native_executable === 'string' && command.native_executable.length > 0) {
    if (!path.isAbsolute(command.native_executable) || /\.(cmd|bat)$/i.test(command.native_executable)) {
      throw Object.assign(new Error(), { diagnosticCode: 'NATIVE_EXECUTABLE_INVALID' });
    }
    options.pathToClaudeCodeExecutable = command.native_executable;
  }
  session = sess;
  try {
    // Exact legacy bridge behavior: prepare once, with no native user input.
    sess.preparedQuery = await prepareQuery(sdk.startup, options);
  } catch {
    emit({ kind: 'operation_unknown', operation_id: command.operation_id, diagnostic_code: 'SDK_STARTUP_UNKNOWN' });
    return;
  }
  emit({
    kind: 'prepared',
    operation_id: command.operation_id,
    bridge_boot_id: command.bridge_boot_id,
    requested_model: sess.requestedModel,
    sdk_version: sdkVersion,
    native_session_id: null,
  });
}

class InputQueue {
  constructor() { this.items = []; this.waiters = []; this.closed = false; }
  push(message) {
    if (this.closed) throw Object.assign(new Error(), { diagnosticCode: 'INPUT_CLOSED', nativeAdmissionPossible: true });
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: message, done: false });
    else {
      if (this.items.length >= MAX_INPUT_QUEUE) throw Object.assign(new Error(), { diagnosticCode: 'INPUT_CAPACITY' });
      this.items.push(message);
    }
  }
  close() {
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) waiter({ value: undefined, done: true });
  }
  iterable() {
    const queue = this;
    return { [Symbol.asyncIterator]() { return {
      next() {
        if (queue.items.length) return Promise.resolve({ value: queue.items.shift(), done: false });
        if (queue.closed) return Promise.resolve({ value: undefined, done: true });
        return new Promise(resolve => queue.waiters.push(resolve));
      },
    }; } };
  }
}

function validateInteractionReply(request, reply) {
  if (!reply || typeof reply !== 'object' || Array.isArray(reply)
      || Buffer.byteLength(canonicalJson(reply), 'utf8') > MAX_INTERACTION_REPLY_BYTES) return false;
  const keys = Object.keys(reply);
  if (request.kind === 'permission') {
    if (reply.type !== 'permission') return false;
    if (reply.decision === 'allow') {
      return keys.every(key => ['type', 'decision', 'updated_input'].includes(key))
        && (reply.updated_input === undefined
          || (reply.updated_input && typeof reply.updated_input === 'object'
            && !Array.isArray(reply.updated_input)));
    }
    return reply.decision === 'deny'
      && keys.every(key => ['type', 'decision', 'message'].includes(key))
      && keys.length === 3
      && boundedSdkText(reply.message, 1_024) !== null;
  }
  if (request.kind !== 'question' || request.tool_name !== 'AskUserQuestion'
      || !allowedKeys(reply, ['type', 'updated_input']) || reply.type !== 'question'
      || !allowedKeys(reply.updated_input, ['questions', 'answers'])
      || !Array.isArray(request.input.questions) || request.input.questions.length === 0
      || request.input.questions.length > 16
      || canonicalJson(reply.updated_input.questions) !== canonicalJson(request.input.questions)) {
    return false;
  }
  const answers = reply.updated_input.answers;
  if (!answers || typeof answers !== 'object' || Array.isArray(answers)
      || Object.keys(answers).length !== request.input.questions.length) return false;
  return request.input.questions.every(question => {
    if (!question || typeof question.question !== 'string') return false;
    const answer = answers[question.question];
    if (typeof answer === 'string') return validAnswer(answer);
    return question.multiSelect === true && Array.isArray(answer)
      && answer.length > 0 && answer.length <= 16 && answer.every(validAnswer);
  });
}

function sdkCallbackResult(request, reply) {
  if (request.kind === 'permission' && reply.decision === 'deny') {
    return { behavior: 'deny', message: reply.message };
  }
  const updatedInput = reply.updated_input === undefined ? request.input : reply.updated_input;
  return { behavior: 'allow', updatedInput };
}

function retireInteraction(sess, pending, diagnosticCode) {
  if (sess.pendingInteractions.get(pending.request.request_id) !== pending) return;
  sess.pendingInteractions.delete(pending.request.request_id);
  pending.signal?.removeEventListener('abort', pending.abort);
  emit({
    kind: 'interaction_cancelled',
    bridge_boot_id: sess.bridgeBootId,
    native_root_id: sess.nativeRootId,
    native_scope_key: sess.nativeScopeKey,
    request_id: pending.request.request_id,
    request_sha256: pending.request.request_sha256,
    diagnostic_code: diagnosticCode,
  });
  pending.resolve({
    behavior: 'deny',
    message: 'This Claude SDK callback was cancelled before a matching user reply arrived.',
  });
}

function requestInteraction(sess, toolNameValue, inputValue, callbackOptions) {
  if (!sess || sess.ended) {
    return Promise.resolve({ behavior: 'deny', message: 'The Claude SDK session is no longer active.' });
  }
  const toolName = boundedSdkText(toolNameValue, 256);
  if (toolName === null || !inputValue || typeof inputValue !== 'object' || Array.isArray(inputValue)) {
    return Promise.resolve({ behavior: 'deny', message: 'The Claude SDK callback request is malformed.' });
  }
  if (sess.nativeRootId === null) {
    emit({ kind: 'diagnostic', code: 'INTERACTION_ROOT_UNAVAILABLE' });
    return Promise.resolve({ behavior: 'deny', message: 'The native Claude session identity is not available.' });
  }
  if (sess.pendingInteractions.size >= MAX_PENDING_INTERACTIONS) {
    emit({ kind: 'diagnostic', code: 'INTERACTION_CAPACITY' });
    return Promise.resolve({ behavior: 'deny', message: 'Too many Claude SDK callbacks are awaiting user input.' });
  }
  let input;
  try { input = JSON.parse(JSON.stringify(inputValue)); }
  catch {
    return Promise.resolve({ behavior: 'deny', message: 'The Claude SDK callback input is not JSON-safe.' });
  }
  const requestBytes = Buffer.byteLength(canonicalJson(input), 'utf8');
  if (requestBytes > MAX_INTERACTION_REQUEST_BYTES) {
    emit({ kind: 'diagnostic', code: 'INTERACTION_REQUEST_BOUNDARY' });
    return Promise.resolve({ behavior: 'deny', message: 'The Claude SDK callback request exceeds its size boundary.' });
  }
  const signal = callbackOptions?.signal;
  if (signal?.aborted) {
    return Promise.resolve({ behavior: 'deny', message: 'The Claude SDK callback was cancelled.' });
  }
  const request = {
    schema_version: 1,
    request_id: randomUUID(),
    kind: toolName === 'AskUserQuestion' ? 'question' : 'permission',
    tool_name: toolName,
    input,
  };
  request.request_sha256 = sha256(canonicalJson(request));
  return new Promise(resolve => {
    const pending = { request, resolve, signal, abort: null };
    pending.abort = () => retireInteraction(sess, pending, 'SDK_CALLBACK_ABORTED');
    sess.pendingInteractions.set(request.request_id, pending);
    emit({
      kind: 'interaction_request',
      request: {
        ...request,
        bridge_boot_id: sess.bridgeBootId,
        native_root_id: sess.nativeRootId,
        native_scope_key: sess.nativeScopeKey,
      },
    });
    signal?.addEventListener('abort', pending.abort, { once: true });
  });
}

function handleControlReply(command) {
  const sess = session;
  if (!sess || sess.ended
      || typeof command.operation_id !== 'string' || !command.operation_id.trim()
      || command.bridge_boot_id !== sess.bridgeBootId
      || command.native_scope_key !== sess.nativeScopeKey
      || command.native_root_id !== sess.nativeRootId) {
    throw Object.assign(new Error(), { diagnosticCode: 'INTERACTION_REPLY_IDENTITY' });
  }
  const pending = sess.pendingInteractions.get(command.request_id);
  if (!pending || command.request_sha256 !== pending.request.request_sha256
      || command.interaction_kind !== pending.request.kind
      || typeof command.reply_sha256 !== 'string'
      || !/^[0-9a-f]{64}$/.test(command.reply_sha256)
      || !validateInteractionReply(pending.request, command.reply)
      || sha256(canonicalJson(command.reply)) !== command.reply_sha256) {
    throw Object.assign(new Error(), { diagnosticCode: 'INTERACTION_REPLY_MISMATCH' });
  }
  sess.pendingInteractions.delete(command.request_id);
  pending.signal?.removeEventListener('abort', pending.abort);
  pending.resolve(sdkCallbackResult(pending.request, command.reply));
  emit({
    kind: 'interaction_reply_ack',
    operation_id: command.operation_id,
    bridge_boot_id: sess.bridgeBootId,
    native_root_id: sess.nativeRootId,
    native_scope_key: sess.nativeScopeKey,
    request_id: command.request_id,
    request_sha256: command.request_sha256,
    reply_sha256: command.reply_sha256,
  });
}

function sendInput(command) {
  const sess = session;
  if (!sess || sess.ended) throw Object.assign(new Error(), { diagnosticCode: 'NATIVE_SESSION_NOT_READY' });
  if (typeof command.initial_dispatch !== 'boolean') throw Object.assign(new Error(), { diagnosticCode: 'INPUT_COMMAND_SCHEMA' });
  const initialDispatch = command.initial_dispatch;
  if (initialDispatch && !sess.preparedQuery) throw Object.assign(new Error(), { diagnosticCode: 'PREPARED_EXECUTOR_UNAVAILABLE' });
  if (!initialDispatch && !sess.query) throw Object.assign(new Error(), { diagnosticCode: 'NATIVE_SESSION_NOT_READY' });
  if (initialDispatch ? command.native_root_id !== null : typeof command.native_root_id !== 'string') {
    throw Object.assign(new Error(), { diagnosticCode: 'NATIVE_SESSION_ID_REQUIRED' });
  }
  const inputText = typeof command.text === 'string' ? command.text : '';
  if (!inputText.trim()) throw Object.assign(new Error(), { diagnosticCode: 'INPUT_TEXT_REQUIRED' });
  const inputId = command.user_message_uuid;
  if (typeof inputId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(inputId)) {
    throw Object.assign(new Error(), { diagnosticCode: 'INPUT_UUID_INVALID' });
  }
  const text = inputText;
  const message = {
    type: 'user',
    message: { role: 'user', content: text },
    parent_tool_use_id: null,
    uuid: inputId,
  };
  if (!initialDispatch) {
    message.session_id = command.native_root_id;
  }
  if (initialDispatch) {
    // Burn the one-shot claim before entering the SDK, exactly as the legacy
    // prepared-query helper requires. Any later failure remains Unknown.
    sess.input.push(message);
    const prepared = sess.preparedQuery;
    sess.preparedQuery = null;
    try { sess.query = prepared.query(sess.input.iterable()); }
    catch { throw Object.assign(new Error(), { diagnosticCode: 'FIRST_NATIVE_QUERY_UNKNOWN', nativeAdmissionPossible: true }); }
    void pump(sess);
  } else {
    sess.input.push(message);
  }
  emit({ kind: 'input_queued', operation_id: command.operation_id, user_message_uuid: inputId });
}

async function handle(line) {
  if (Buffer.byteLength(line, 'utf8') > MAX_INPUT_BYTES) {
    emit({ kind: 'diagnostic', code: 'HARNESS_INPUT_BOUNDARY' });
    return;
  }
  let command;
  try { command = JSON.parse(line); }
  catch { emit({ kind: 'diagnostic', code: 'HARNESS_COMMAND_INVALID' }); return; }
  try {
    if (!command || typeof command !== 'object' || typeof command.kind !== 'string') {
      emit({ kind: 'diagnostic', code: 'HARNESS_COMMAND_INVALID' });
    } else if (command.kind === 'prepare') {
      await prepare(command);
    } else if (command.kind === 'send') {
      sendInput(command);
    } else if (command.kind === 'control_reply') {
      handleControlReply(command);
    } else if (command.kind === 'stop') {
      stopping = true;
      if (session && !session.ended) {
        session.input.close();
        try { session.query?.close(); } catch { /* the SDK stream may already be closed */ }
        try { session.preparedQuery?.close(); } catch { /* warm process may already be closed */ }
      }
      emit({ kind: 'stopped' });
    } else {
      emit({ kind: 'diagnostic', operation_id: command.operation_id ?? null, code: 'HARNESS_COMMAND_UNSUPPORTED' });
    }
  } catch (error) {
    emit({
      kind: error?.nativeAdmissionPossible ? 'operation_unknown' : 'operation_rejected',
      operation_id: typeof command?.operation_id === 'string' ? command.operation_id : null,
      diagnostic_code: diagnostic(error, 'HARNESS_OPERATION_FAILED'),
    });
  }
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (stopping) break;
  // Preparation is awaited so a ready receipt cannot race a later input. The
  // SDK pump itself is detached and never blocks the local command reader.
  await handle(line);
}
if (session && !session.ended) {
  session.input.close();
  try { session.query?.close(); } catch { /* stream already closed */ }
  try { session.preparedQuery?.close(); } catch { /* warm process already closed */ }
}
