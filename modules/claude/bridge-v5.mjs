#!/usr/bin/env node
// SDK-owned native execution for Claude Code through the pinned Claude Agent
// SDK. Host IPC reconnect never closes the SDK query or repeats input.
// Artifact scope (bridge.5): Store TaskPrompt v1 bytes are submitted unchanged
// and normalized admission is emitted only after the matching native UUID echo.
// Attach/resume/configure/goal/reply/steer/result pages remain unavailable.
import path from 'node:path';
import os from 'node:os';
import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { isDeepStrictEqual } from 'node:util';
import { startup } from '@anthropic-ai/claude-agent-sdk';
import { Control } from './control.mjs';
import { createFirstQueryFaultControl } from './first-query-spawn-control.test.mjs';
import { prepareQuery } from './prepared-query.mjs';
import { explicitQueryModel, modelSelectionFacts } from './model-selection.mjs';
import {
  firstNativeQueryUnknownOutcome, streamEndedSendOutcome,
} from './pending-outcome.mjs';
import {
  CLAUDE_TASK_PROMPT_ARTIFACT_ID, taskDispatchAdmission, taskPromptFor,
  enqueueTaskPrompt, moduleReceipt,
} from './task-prompt.mjs';
import { loadModuleBootstrap, verifyModuleHello, verifyModuleCommand } from './module-bootstrap-v5.mjs';
import {
  createStreamState, applySdkMessage, notePermissionRequest, noteStderr,
  noteStreamEnd, noteStreamFailure, snapshot,
} from './stream.mjs';

const SDK_VERSION = '0.3.287';
const MODULE_ARTIFACT_ID = CLAUDE_TASK_PROMPT_ARTIFACT_ID;
const ENTRYPOINT = 'claude_agent_sdk_streaming_input';
const PERMISSION_MODES = ['default', 'acceptEdits', 'bypassPermissions', 'plan', 'dontAsk', 'auto'];
const CAPABILITIES = {
  describe: 'implemented',
  open: 'implemented',
  send_next_turn: 'implemented',
  snapshot: 'implemented',
  attach: 'unavailable',
  resume: 'unavailable',
  configure_model: 'unavailable',
  configure_effort: 'unavailable',
  goal: 'unavailable',
  steer: 'unavailable',
  reply: 'unavailable',
  result_pages: 'unavailable',
  recover: 'unavailable',
};
const DENY_MESSAGE = 'Denied by the swarm Claude bridge: tool permission replies (agent.reply) are unavailable in this module artifact, so no tool use is approved implicitly. The request was recorded in the module observation.';
const DEFERRED = Symbol('deferred-outcome');
const RETIRE_AFTER_UNKNOWN = ['SDK_STARTUP_UNKNOWN', 'FIRST_NATIVE_QUERY_UNKNOWN'];

function required(object, key) {
  if (typeof object?.[key] !== 'string' || !object[key].trim()) throw new Error(`MISSING_${key}`);
  return object[key];
}
function codedError(code, nativeAdmissionPossible = false) {
  const error = new Error(code);
  error.diagnosticCode = code;
  error.nativeAdmissionPossible = nativeAdmissionPossible;
  return error;
}
function capabilityError(capability) {
  const error = codedError('CAPABILITY_UNAVAILABLE');
  error.capability = capability;
  return error;
}

const argv = process.argv.slice(2);
if (argv.length !== 2 || argv[0] !== '--config') {
  console.error('Usage: node bridge-v5.mjs --config <module-host-connection.json>'); process.exit(2);
}
const bootstrap = await loadModuleBootstrap(argv[1]);
const { config, credential, managedOwner, bootId, moduleContract } = bootstrap;
if (config.moduleArtifactId !== MODULE_ARTIFACT_ID) throw new Error('MODULE_ARTIFACT_MISMATCH');
const firstQueryFaultControl = await createFirstQueryFaultControl({
  bindingId: bootstrap.bindingId,
  generation: bootstrap.generation,
  bootId,
});
if (config.command !== undefined) {
  if (typeof config.command !== 'string' || !path.isAbsolute(config.command)) throw new Error('NATIVE_EXECUTABLE_MUST_BE_ABSOLUTE');
  if (process.platform === 'win32' && /\.(cmd|bat)$/i.test(config.command)) throw new Error('USE_NATIVE_EXE_NOT_SHELL_WRAPPER');
}

// The existing supervisor/module owner supplies the selected descriptor,
// separate scoped credential and stable worker boot. No checkpoint or
// cross-restart native-session recovery is introduced.

function nativeScopeKey() {
  // The native session store namespace: Claude sessions persist per config
  // directory, so that directory (not a PID, port or route alias) scopes ids.
  const dir = process.env.CLAUDE_CONFIG_DIR
    ? path.resolve(process.env.CLAUDE_CONFIG_DIR)
    : path.join(os.homedir(), '.claude');
  return `claude:${dir}`;
}

class InputQueue {
  constructor() { this.items = []; this.waiters = []; this.closed = false; }
  push(message) {
    if (this.closed) throw codedError('INPUT_CLOSED', true);
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: message, done: false });
    else this.items.push(message);
  }
  close() {
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) waiter({ value: undefined, done: true });
  }
  iterable() {
    const queue = this;
    return {
      [Symbol.asyncIterator]() {
        return {
          next() {
            if (queue.items.length) return Promise.resolve({ value: queue.items.shift(), done: false });
            if (queue.closed) return Promise.resolve({ value: undefined, done: true });
            return new Promise(resolve => queue.waiters.push(resolve));
          },
        };
      },
    };
  }
}

let control = null;
let connected = false;
let stopping = false;
let session = null; // { state, input, query, preparedQuery, rootId, scopeKey, pendingSends, ended }
let configuredModel = null;
let revision = 0;
let lastSentRevision = -1;
const outcomes = new Map(); // operation_id -> RuntimeOutcome awaiting host report
const journal = new Map(); // operation_id -> settled summary for agent.reconcile
const active = new Set(); // operation_ids currently executing
let retirement = null; // exact uncertain outcome; retire only after Store acceptance
let retirementInProgress = false;
let storeRetainedIdentity = null; // binding-scoped identity learned only from an exact Store outcome ACK
let helloRetainedIdentity = null; // this link Store view, never request authority before outcome ACK

function changed() { revision++; }
function storeIdentityPair(rootId, scopeKey) {
  if (rootId == null && scopeKey == null) return null;
  if (typeof rootId !== 'string' || !rootId.trim()
      || typeof scopeKey !== 'string' || !scopeKey.trim()) {
    throw codedError('STORE_RETAINED_IDENTITY_INVALID');
  }
  return { rootId, scopeKey };
}
function identityFromHello(hello) {
  const pair = storeIdentityPair(hello?.native_root_id, hello?.native_scope_key);
  return pair ? { bindingId: hello.binding_id, generation: hello.generation, ...pair } : null;
}
function identityFromAcknowledgedOutcome(outcome) {
  const pair = storeIdentityPair(outcome?.native_root_id, outcome?.native_scope_key);
  if (!pair) return null;
  const receipt = outcome?.details?.module_receipt;
  if (receipt?.binding_id !== bootstrap.bindingId
      || receipt?.binding_generation !== bootstrap.generation
      || receipt?.operation_id !== outcome.operation_id) {
    throw codedError('STORE_OUTCOME_IDENTITY_RECEIPT_MISMATCH');
  }
  return { bindingId: receipt.binding_id, generation: receipt.binding_generation, ...pair };
}
function sameStoreIdentity(left, right) {
  return Boolean(left && right
    && left.bindingId === right.bindingId
    && left.generation === right.generation
    && left.rootId === right.rootId
    && left.scopeKey === right.scopeKey);
}
function saveOutcome(command, result) {
  const operationId = command.operation_id;
  const old = outcomes.get(operationId);
  if (old && ['applied', 'rejected'].includes(old.outcome)) return;
  const receipt = moduleReceipt(command, moduleContract);
  const details = { ...result.details, module_receipt: receipt };
  if (details.dispatch_admission) {
    if (!isDeepStrictEqual(details.dispatch_admission.module_receipt, receipt)) {
      throw codedError('MODULE_RECEIPT_IDENTITY_INVALID');
    }
    details.dispatch_admission = { ...details.dispatch_admission, module_receipt: receipt };
  }
  const outcome = { operation_id: operationId, ...result, details };
  outcomes.set(operationId, outcome);
  if (!retirement && outcome.outcome === 'unknown'
      && RETIRE_AFTER_UNKNOWN.includes(outcome.details?.diagnostic_code)) {
    retirement = { operationId, outcome, acceptedByStore: false };
  }
  journal.set(operationId, {
    method: command.method ?? null,
    outcome: outcome.outcome,
    completion_condition: outcome.details?.completion_condition ?? null,
    diagnostic_code: outcome.details?.diagnostic_code ?? null,
  });
  if (journal.size > 128) journal.delete(journal.keys().next().value);
  changed();
}
async function retireAcceptedUnknown() {
  if (!retirement?.acceptedByStore || outcomes.size !== 0 || active.size !== 0
      || retirementInProgress || stopping) return false;
  retirementInProgress = true;
  // This retires the bridge worker only. The module owner and supervisor still
  // prove process-family departure and retain child custody if cleanup is unknown.
  await stop();
  process.exit(0);
  return true;
}
function currentState() {
  if (session) return session.state;
  const idle = createStreamState();
  idle.phase = 'no_session';
  return idle;
}
function describeFacts(state, requestedModel = null) {
  const modelSelection = modelSelectionFacts(
    state?.requested_model ?? requestedModel ?? configuredModel,
    state?.init?.model,
  );
  return {
    module_artifact_id: MODULE_ARTIFACT_ID,
    entrypoint: ENTRYPOINT,
    sdk_package: '@anthropic-ai/claude-agent-sdk',
    sdk_version: SDK_VERSION,
    executor_version: state?.init?.claude_code_version ?? null,
    executor_source: config.command ? 'explicit_module_config' : 'sdk_bundled_matching_binary',
    session_id: state?.init?.session_id ?? null,
    ...modelSelection,
    model_observed: state?.init?.model ?? null,
    permission_mode_observed: state?.init?.permission_mode ?? null,
    capabilities: CAPABILITIES,
  };
}
function observation() {
  const state = currentState();
  const requestedModel = session?.requestedModel ?? configuredModel;
  const stateSnapshot = snapshot(state, requestedModel);
  // The stream mapper knows the SDK session id and exact result frame. Bind
  // each compact result projection to this bridge boot and configured scope
  // here, where those process-level facts are authoritative.
  stateSnapshot.input_executions = stateSnapshot.input_executions.map(event => ({
    ...event,
    bridge_boot_id: bootId,
    native_scope_key: session?.scopeKey ?? null,
  }));
  const result = {
    ...stateSnapshot,
    describe: describeFacts(state, requestedModel),
    native_root_id: session?.identityAdopted ? session.rootId : null,
    native_scope_key: session?.identityAdopted ? session.scopeKey : null,
    boot_id: bootId,
  };
  if (Buffer.byteLength(JSON.stringify(result)) > 700000) {
    // Keep the newest evidence and say so; never discard outcomes instead.
    result.messages = result.messages.slice(-5);
    result.observed_children = result.observed_children.slice(-50);
    result.truncated = true;
  }
  return result;
}

function settleSends(sess, msg) {
  // A user UUID is useful only on a frame from the actual initialized native
  // session. In particular, never bind an init-only or other-session echo.
  if (!sess.rootId || msg?.session_id !== sess.rootId) return;
  const stamped = new Set();
  if (typeof msg?.user_message_uuid === 'string') stamped.add(msg.user_message_uuid);
  if (Array.isArray(msg?.user_message_uuids)) for (const id of msg.user_message_uuids) if (typeof id === 'string') stamped.add(id);
  if (!stamped.size) return;
  for (const [uuid, entry] of sess.pendingSends) {
    if (!stamped.has(uuid)) continue;
    sess.pendingSends.delete(uuid);
    if (entry.initialTaskDispatch) sess.identityAdopted = true;
    const dispatchAdmission = entry.taskPrompt
      ? taskDispatchAdmission(entry.command, entry.taskPrompt, {
        moduleContract,
        bootId,
        inputId: uuid,
        nativeRootId: sess.rootId,
        nativeFrame: msg,
      })
      : null;
    saveOutcome(entry.command, {
      outcome: 'applied',
      native_root_id: sess.rootId,
      native_scope_key: sess.scopeKey,
      native_input_id: uuid,
      details: {
        completion_condition: 'native_input_admitted',
        evidence: 'native_frame_echo',
        execution_complete: false,
        user_message_uuid: uuid,
        initial_task_dispatch: entry.initialTaskDispatch === true,
        bridge_boot_id: bootId,
        native_frame_session_id: msg.session_id,
        native_scope_key: sess.scopeKey,
        prompt_sha256: entry.promptSha256,
        prompt_bytes: entry.promptBytes,
        task_snapshot_sha256: entry.taskSnapshotSha256,
        ...(entry.initialTaskDispatch ? { system_init_session_id: sess.rootId } : {}),
        ...(dispatchAdmission ? { dispatch_admission: dispatchAdmission } : {}),
      },
    });
  }
}

function finishSession(sess) {
  sess.ended = true;
  if (sess.openWait) {
    const failure = sess.state.init_failure;
    const error = codedError('INIT_FAILURE', failure?.subtype === 'error_during_execution' ? false : true);
    error.initFailure = failure;
    sess.openWait.reject(error);
    sess.openWait = null;
  }
  for (const [inputId, entry] of sess.pendingSends) {
    // settleSends removes an entry only after its queued UUID is echoed by a
    // native frame. A still-pending, validated initial TaskPrompt therefore
    // has no same-session admission evidence and takes the terminal Unknown
    // path that retires this worker only after Store accepts module.outcome.
    const exactInitialTaskPrompt = sess.pendingSends.get(inputId) === entry
      && entry.initialTaskDispatch === true
      && entry.method === 'task.dispatch'
      && entry.taskPrompt?.context?.operation_id === entry.command?.operation_id
      && entry.taskPrompt?.context?.worker_boot_id === bootId;
    const outcome = exactInitialTaskPrompt
      ? firstNativeQueryUnknownOutcome(entry, bootId)
      : streamEndedSendOutcome(entry, sess, bootId);
    saveOutcome(entry.command, outcome);
  }
  sess.pendingSends.clear();
  changed();
}

async function pump(sess) {
  try {
    for await (const msg of sess.query) {
      applySdkMessage(sess.state, msg);
      if (!sess.rootId && sess.state.init?.session_id) {
        sess.rootId = sess.state.init.session_id;
        await firstQueryFaultControl.noteSdkInitIdentity(sess.rootId);
        if (sess.openWait) { sess.openWait.resolve(); sess.openWait = null; }
      }
      if (sess.state.phase === 'init_failed' && sess.openWait) {
        const error = codedError('INIT_FAILURE', sess.state.init_failure?.subtype !== 'error_during_execution');
        error.initFailure = sess.state.init_failure;
        sess.openWait.reject(error);
        sess.openWait = null;
      }
      settleSends(sess, msg);
      changed();
    }
    noteStreamEnd(sess.state);
  } catch (error) {
    noteStreamFailure(sess.state, error);
  }
  finishSession(sess);
}

async function startNative(command) {
  if (session && !session.ended) throw codedError('SESSION_ALREADY_OPEN');
  const nativeOptions = command.route?.native_options ?? {};
  const cwd = required(nativeOptions, 'workspaceRoot');
  if (!path.isAbsolute(cwd)) throw codedError('WORKSPACE_ROOT_MUST_BE_ABSOLUTE');
  let modelOption;
  try { modelOption = explicitQueryModel(nativeOptions); }
  catch (error) { throw codedError(error.message); }
  const requestedModel = modelOption.model;
  let permissionMode;
  if (nativeOptions.permissionMode !== undefined) {
    if (!PERMISSION_MODES.includes(nativeOptions.permissionMode)) throw codedError('INVALID_PERMISSION_MODE');
    permissionMode = nativeOptions.permissionMode;
  }
  if (permissionMode === 'bypassPermissions' && nativeOptions.allowDangerouslySkipPermissions !== true) {
    throw codedError('BYPASS_REQUIRES_EXPLICIT_ROUTE_FLAG');
  }
  const state = createStreamState();
  state.requested_model = requestedModel;
  const input = new InputQueue();
  const sess = {
    state, input, query: null, preparedQuery: null, rootId: null, scopeKey: nativeScopeKey(),
    requestedModel, pendingSends: new Map(), ended: false, firstDispatchStarted: false,
    identityAdopted: false,
  };
  const options = {
    cwd,
    ...modelOption,
    includePartialMessages: true,
    // Complete child messages (not only tool heartbeats) are the C08 mapping.
    forwardSubagentText: true,
    spawnClaudeCodeProcess: firstQueryFaultControl.spawnClaudeCodeProcess,
    canUseTool: (toolName, _toolInput, toolOptions) => {
      notePermissionRequest(state, { tool_name: toolName, tool_use_id: toolOptions.toolUseID });
      changed();
      return Promise.resolve({
        behavior: 'deny',
        message: DENY_MESSAGE,
        ...(typeof toolOptions.toolUseID === 'string' ? { toolUseID: toolOptions.toolUseID } : {}),
      });
    },
    stderr: chunk => { noteStderr(state, chunk); changed(); },
  };
  if (permissionMode) options.permissionMode = permissionMode;
  if (permissionMode === 'bypassPermissions') options.allowDangerouslySkipPermissions = true;
  if (config.command) options.pathToClaudeCodeExecutable = config.command;
  session = sess;
  try {
    // SDK startup completes its initialize handshake and returns a pre-warmed
    // process. It emits no system/init session identity and submits no prompt.
    sess.preparedQuery = await prepareQuery(startup, options);
    state.phase = 'prepared';
  } catch {
    // Startup can fail after process creation; without a native receipt this
    // is Unknown and must not trigger another startup/query attempt.
    throw codedError('SDK_STARTUP_UNKNOWN', true);
  }
  return {
    native_root_id: null,
    native_scope_key: null,
    details: {
      completion_condition: 'native_executor_prepared',
      native_session_state: 'prepared',
      pre_input_executor_ready: true,
      bridge_boot_id: bootId,
      describe: describeFacts(state, requestedModel),
    },
  };
}

function sendNative(command) {
  const sess = session;
  if (!sess || sess.ended) throw codedError('NATIVE_SESSION_NOT_READY');
  const firstTaskDispatch = !sess.rootId && command.method === 'task.dispatch';
  if (!sess.rootId && !firstTaskDispatch) throw codedError('INITIAL_TASK_DISPATCH_REQUIRED');
  if (sess.rootId && command.native_root_id !== sess.rootId) throw codedError('NATIVE_IDENTITY_MISMATCH');
  const p = command.input ?? {};
  let text;
  let taskPrompt = null;
  if (command.method === 'task.dispatch') {
    taskPrompt = taskPromptFor(command, bootId);
    text = taskPrompt.envelope.prompt;
  } else {
    if (p.delivery === 'steer') throw capabilityError('steer');
    if (p.delivery !== undefined && p.delivery !== 'next_turn') throw codedError('UNSUPPORTED_DELIVERY');
    text = required(p, 'text');
  }
  const uuid = randomUUID();
  const taskDispatch = command.method === 'task.dispatch';
  const promptSha256 = taskPrompt?.envelope.prompt_sha256
    ?? createHash('sha256').update(text, 'utf8').digest('hex');
  const entry = {
    operation_id: command.operation_id,
    method: command.method,
    command,
    initialTaskDispatch: taskDispatch ? firstTaskDispatch : false,
    promptSha256,
    promptBytes: taskPrompt?.envelope.prompt_bytes ?? Buffer.byteLength(text, 'utf8'),
    taskSnapshotSha256: taskPrompt?.envelope.task_snapshot_sha256 ?? null,
    taskPrompt,
  };
  sess.pendingSends.set(uuid, entry);
  try {
    const message = taskPrompt ? null : {
        type: 'user',
        message: { role: 'user', content: text },
        parent_tool_use_id: null,
        uuid,
        ...(sess.rootId ? { session_id: sess.rootId } : {}),
      };
    if (firstTaskDispatch) {
      if (!sess.preparedQuery || sess.firstDispatchStarted) throw codedError('PREPARED_EXECUTOR_UNAVAILABLE');
      sess.firstDispatchStarted = true;
      if (taskPrompt) enqueueTaskPrompt(taskPrompt, sess.input, uuid, sess.rootId);
      else sess.input.push(message);
      const prepared = sess.preparedQuery;
      sess.preparedQuery = null;
      try {
          const input = sess.input.iterable();
          const gatedInput = taskPrompt
            ? firstQueryFaultControl.gateFirstTaskPrompt(input, {
                operationId: command.operation_id,
                inputId: uuid,
                taskPrompt,
              })
            : input;
          sess.query = prepared.query(gatedInput);
      } catch {
        throw codedError('FIRST_NATIVE_QUERY_UNKNOWN', true);
      }
      sess.state.phase = 'awaiting_init';
      void pump(sess);
    } else {
      if (!sess.query) throw codedError('NATIVE_SESSION_NOT_READY');
      if (taskPrompt) enqueueTaskPrompt(taskPrompt, sess.input, uuid, sess.rootId);
      else sess.input.push(message);
    }
  } catch (error) {
    if (!error.nativeAdmissionPossible) sess.pendingSends.delete(uuid);
    throw error;
  }
  // Admission into the SDK input queue is local. The outcome settles when a
  // native frame stamped with this send's uuid proves the turn exists, or
  // becomes unknown when the stream ends without that evidence.
  return DEFERRED;
}

async function execute(command) {
  active.add(command.operation_id);
  try {
    let result;
    if (command.method === 'agent.open') {
      result = await startNative(command);
    } else if (command.method === 'task.dispatch' || command.method === 'agent.send') {
      if (sendNative(command) === DEFERRED) return;
    } else if (command.method === 'agent.refresh') {
      const sess = session;
      if (!sess || sess.ended || !sess.rootId || !sess.identityAdopted) throw codedError('NATIVE_SESSION_NOT_READY');
      const target = command.input?.session_id === undefined ? sess.rootId : required(command.input, 'session_id');
      if (target !== sess.rootId && !sess.state.children.has(target)) throw codedError('UNKNOWN_SESSION');
      result = {
        details: {
          completion_condition: 'native_read_completed',
          target,
          snapshot: snapshot(sess.state, sess.requestedModel),
        },
      };
    } else if (command.method === 'agent.reconcile') {
      const targetId = required(command.input ?? {}, 'operation_id');
      const record = journal.get(targetId);
      if (!record) throw codedError('UNKNOWN_OPERATION');
      result = { details: { completion_condition: 'module_journal_readback', recorded: record } };
    } else if (command.method === 'agent.configure') {
      throw capabilityError(command.input?.settings?.model !== undefined ? 'configure_model' : 'configure_effort');
    } else if (command.method === 'agent.goal') throw capabilityError('goal');
    else if (command.method === 'agent.reply') throw capabilityError('reply');
    else if (command.method === 'agent.recover') throw capabilityError('recover');
    else if (command.method === 'agent.result') throw capabilityError('result_pages');
    else throw codedError('UNSUPPORTED_OPERATION');
    if (session?.rootId && !(command.method === 'task.dispatch' && !session.identityAdopted)) {
      result.native_root_id = session.rootId;
      result.native_scope_key = session.scopeKey;
    }
    saveOutcome(command, { outcome: 'applied', ...result });
  } catch (error) {
    const details = {
      error_type: error.name,
      diagnostic_code: error.diagnosticCode ?? String(error.message).slice(0, 120),
    };
    if (error.capability) details.capability = error.capability;
    if (error.initFailure) details.init_failure = error.initFailure;
    const outcome = { outcome: error.nativeAdmissionPossible ? 'unknown' : 'rejected', details };
    if (session?.rootId && !(command.method === 'task.dispatch' && !session.identityAdopted)) {
      outcome.native_root_id = session.rootId;
      outcome.native_scope_key = session.scopeKey;
    }
    saveOutcome(command, outcome);
  } finally {
    active.delete(command.operation_id);
    await retireAcceptedUnknown();
  }
}

async function report(link = control) {
  for (const [id, outcome] of outcomes) {
    const ack = await link.call('module.outcome', outcome);
    if (ack?.recorded !== true) throw codedError('MODULE_OUTCOME_ACK_INVALID');
    // A native frame may have superseded this outcome while IPC awaited.
    if (outcomes.get(id) === outcome) {
      const acknowledgedIdentity = identityFromAcknowledgedOutcome(outcome);
      if (acknowledgedIdentity) {
        if (helloRetainedIdentity && !sameStoreIdentity(acknowledgedIdentity, helloRetainedIdentity)) {
          throw codedError('STORE_RETAINED_IDENTITY_MISMATCH');
        }
        storeRetainedIdentity = acknowledgedIdentity;
      }
      outcomes.delete(id);
      if (retirement?.operationId === id && retirement.outcome === outcome) {
        retirement.acceptedByStore = true;
      }
    }
  }
  if (await retireAcceptedUnknown()) return;
  if (lastSentRevision !== revision) {
    const at = revision;
    await link.call('module.observe', { event_id: `${bootId}:${at}`, sequence: at, state: observation() });
    lastSentRevision = at;
  }
}
let reportBusy = false;
const interval = setInterval(() => {
  if (!connected || reportBusy) return;
  reportBusy = true;
  const link = control;
  void report(link).catch(() => link.close()).finally(() => { reportBusy = false; });
}, 1000);
interval.unref();

async function stop() {
  if (stopping) return;
  stopping = true;
  connected = false;
  control?.close();
  clearInterval(interval);
  // Only explicit termination of this SDK owner closes the native query.
  if (session && !session.ended) {
    session.input.close();
    try { session.query?.close(); } catch { /* stream already over */ }
    try { session.preparedQuery?.close(); } catch { /* warm process already ended */ }
  }
}
process.once('SIGINT', () => { void stop().then(() => process.exit(0)); });
process.once('SIGTERM', () => { void stop().then(() => process.exit(0)); });

let admissionTail = Promise.resolve();
while (!stopping) {
  try {
    control = new Control(config.endpoint, credential, bootstrap.maxFrameBytes);
    firstQueryFaultControl.instrumentControl(control);
    await control.connect();
    helloRetainedIdentity = null;
    const hello = await control.call('module.hello', {
      boot_id: bootId,
      module_artifact_id: config.moduleArtifactId,
      module_contract: moduleContract,
      native_ready: Boolean(session && !session.ended && (session.rootId || session.preparedQuery)),
      ...(managedOwner ? { managed_owner: managedOwner } : {}),
      ...(storeRetainedIdentity?.bindingId === bootstrap.bindingId
        && storeRetainedIdentity.generation === bootstrap.generation
        ? { native_root_id: storeRetainedIdentity.rootId, native_scope_key: storeRetainedIdentity.scopeKey }
        : {}),
    });
    verifyModuleHello(hello, bootstrap);
    helloRetainedIdentity = identityFromHello(hello);
    if (storeRetainedIdentity && !sameStoreIdentity(storeRetainedIdentity, helloRetainedIdentity)) {
      throw codedError('STORE_RETAINED_IDENTITY_MISMATCH');
    }
    configuredModel = typeof hello.route?.native_options?.modelId === 'string'
      ? hello.route.native_options.modelId
      : null;
    if (hello.recovery_required) {
      // This artifact cannot resume a recorded session (see README). The
      // binding stays reconciling for the operator instead of a fresh start.
      console.error(JSON.stringify({ component: 'claude-bridge', code: 'RECOVERY_UNAVAILABLE_IN_ARTIFACT', binding_id: hello.binding_id }));
    }
    lastSentRevision = -1;
    await report();
    connected = true;
    while (!stopping && control.socket) {
      if (retirement) { await delay(50); continue; }
      if (outcomes.size >= 8) { await delay(50); continue; }
      const result = await control.call('module.next', {});
      if (result.command) {
        const command = result.command;
        verifyModuleCommand(command, bootstrap);
        moduleReceipt(command, moduleContract);
        if (active.has(command.operation_id) || outcomes.has(command.operation_id)) continue;
        if (retirement) {
          saveOutcome(command, {
            outcome: 'rejected',
            details: { diagnostic_code: 'BRIDGE_RETIRING_AFTER_UNKNOWN_NATIVE_EFFECT' },
          });
          continue;
        }
        // Serialize native admission, not whole model turns. Read-only
        // refresh/reconcile bypass the queue like replies do for Muse.
        if (['agent.refresh', 'agent.reconcile'].includes(command.method)) void execute(command);
        else admissionTail = admissionTail.then(() => execute(command));
        await admissionTail.catch(() => {});
      }
    }
  } catch (error) {
    console.error(JSON.stringify({ component: 'claude-bridge', code: error.code ?? error.message, native_preserved: Boolean(session && !session.ended) }));
  } finally {
    connected = false;
    control?.close();
  }
  if (!stopping) await delay(1000);
}
