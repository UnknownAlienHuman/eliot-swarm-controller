#!/usr/bin/env node
// Owned native execution for Antigravity through its documented CLI warm
// stream (`agy --input-format stream-json --output-format stream-json`).
// There is no vendor SDK and no shared server for this runtime: the bridge
// owns exactly one CLI process per binding, feeds it sequential user events
// and maps its native event stream through codec.mjs. Host IPC reconnect
// never closes the CLI process or repeats input.
// Artifact scope (bridge.2): describe, agent.open (fresh or exact
// conversation resume), next-turn agent.send/task.dispatch and observation
// snapshots. Durable goal control is not established for this entrypoint,
// mid-session configure/steer/reply and attach are reported unavailable,
// never emulated; see README capability matrix.
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Control } from './control.mjs';
import {
  bindPendingLocalResults, localResultMatches, snapshotObservation,
} from './receipt-state.mjs';
import {
  createStreamState, applyNativeEvent, encodeUserMessage, noteMalformedLine,
  noteStderr, noteStreamEnd, noteStreamFailure, noteProcessExit, snapshot,
  terminalResultDisposition,
} from './codec.mjs';

const ENTRYPOINT = 'antigravity_cli_warm_stream';
const ARTIFACT_ID = 'antigravity-cli-warm-bridge.2';
const EFFORTS = ['low', 'medium', 'high'];
const CAPABILITIES = {
  describe: 'implemented',
  open: 'implemented',
  open_resume_conversation: 'implemented',
  send_next_turn: 'implemented',
  snapshot: 'implemented',
  attach: 'unavailable',
  configure_model: 'unavailable',
  configure_effort: 'unavailable',
  goal: 'unavailable',
  steer: 'unavailable',
  reply: 'unavailable',
  result_pages: 'unavailable',
  recover: 'unavailable',
};
const DEFERRED = Symbol('deferred-outcome');

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
  console.error('Usage: node bridge.mjs --config <local-module.json>'); process.exit(2);
}
const config = JSON.parse(await readFile(argv[1], 'utf8'));
const credential = JSON.parse(await readFile(required(config, 'credentialFile'), 'utf8'));
required(config, 'endpoint');
if (required(config, 'moduleArtifactId') !== ARTIFACT_ID) throw new Error('MODULE_ARTIFACT_MISMATCH');
if (typeof config.command !== 'string' || !path.isAbsolute(config.command)) throw new Error('NATIVE_EXECUTABLE_MUST_BE_ABSOLUTE');
if (process.platform === 'win32' && /\.(cmd|bat)$/i.test(config.command)) throw new Error('USE_NATIVE_EXE_NOT_SHELL_WRAPPER');
if (config.args !== undefined) throw new Error('ARGS_NOT_CONFIGURABLE_BRIDGE_OWNS_ARGV');

// The module-run owner (when used) provides a stable boot identity and the
// managed_owner record for module.hello. This artifact keeps no checkpoint:
// the warm process is owned by this bridge process alone, and cross-restart
// recovery of a recorded conversation is outside its capability matrix
// (exact resume is an explicit new agent.open, never an automatic replay).
let managedOwner = null;
let bootId = randomUUID();
{
  const dir = process.env.ELIOT_SWARM_MODULE_STATE;
  const ownerFile = process.env.ELIOT_SWARM_MODULE_OWNER;
  if (dir && ownerFile) {
    if (!path.isAbsolute(dir) || ownerFile !== path.join(dir, 'owner.json')) throw new Error('INVALID_MODULE_OWNER_PATH');
    const owner = JSON.parse(await readFile(ownerFile, 'utf8'));
    if (owner.version !== 1 || owner.process?.purpose !== 'module' || typeof owner.token !== 'string') throw new Error('INVALID_MODULE_OWNER_RECORD');
    managedOwner = owner;
    bootId = owner.token;
  } else if (dir || ownerFile) {
    throw new Error('INVALID_MODULE_OWNER_PATH');
  }
}

function nativeScopeKey() {
  // The native conversation store namespace: Antigravity CLI keeps its
  // settings and conversations under its own config directory
  // (~/.gemini/antigravity-cli, per the vendor's permissions docs), so that
  // directory — not a PID, port or route alias — scopes conversation ids.
  return `antigravity:${path.join(os.homedir(), '.gemini', 'antigravity-cli')}`;
}

let control = null;
let connected = false;
let stopping = false;
let session = null; // { state, child, rootId, scopeKey, pendingSends, openWait, ended }
let revision = 0;
let lastSentRevision = -1;
const outcomes = new Map(); // operation_id -> RuntimeOutcome awaiting host report
const journal = new Map(); // operation_id -> settled summary for agent.reconcile
const active = new Set(); // operation_ids currently executing
let resultOrdinal = 0; // monotonic for this bridge boot, including warm-process resumes
const localExecutionResults = []; // terminal results awaiting an acknowledged Store outcome
let lastObservedLocalResults = null; // { observation_id, state } from this bridge boot

function changed() { revision++; }
function saveOutcome(operationId, result, method) {
  const old = outcomes.get(operationId);
  if (old && ['applied', 'rejected'].includes(old.outcome)) return;
  const outcome = { operation_id: operationId, ...result };
  outcomes.set(operationId, outcome);
  journal.set(operationId, {
    method: method ?? null,
    outcome: outcome.outcome,
    completion_condition: outcome.details?.completion_condition ?? null,
    diagnostic_code: outcome.details?.diagnostic_code ?? null,
  });
  if (journal.size > 128) journal.delete(journal.keys().next().value);
  changed();
}
function currentState() {
  if (session) return session.state;
  const idle = createStreamState();
  idle.phase = 'no_session';
  return idle;
}
function describeFacts(state) {
  return {
    entrypoint: ENTRYPOINT,
    module_artifact_id: ARTIFACT_ID,
    protocol_basis: 'antigravity.google/docs/cli/headless (AG-HEADLESS, accessed 2026-10-02)',
    // The native stream carries no executor version and no version readback
    // is documented for this entrypoint; the field stays null, not guessed.
    executor_version: null,
    session_id: state?.init?.conversation_id ?? null,
    model_observed: state?.init?.model ?? null,
    permission_mode_observed: state?.init?.permission_mode ?? null,
    capabilities: CAPABILITIES,
  };
}
function observation() {
  const state = currentState();
  return snapshotObservation({
    ...snapshot(state),
    describe: describeFacts(state),
    native_root_id: session?.rootId ?? null,
    native_scope_key: session?.scopeKey ?? null,
    boot_id: bootId,
    local_execution_results: localExecutionResults,
  });
}

function hasLocalResult(outcome) {
  return outcome?.details?.local_execution_ref
    && typeof outcome.details.local_execution_ref === 'object';
}

function removeLocalResult(operationId) {
  for (let i = localExecutionResults.length - 1; i >= 0; i--) {
    if (localExecutionResults[i].input_operation_id === operationId) localExecutionResults.splice(i, 1);
  }
}

async function sendOutcome(link, operationId, outcome) {
  await link.call('module.outcome', outcome);
  if (outcomes.get(operationId) === outcome) {
    outcomes.delete(operationId);
    if (hasLocalResult(outcome)) removeLocalResult(operationId);
  }
}

async function observeForLocalResult(link) {
  const at = revision;
  const state = observation();
  const result = await link.call('module.observe', {
    event_id: `${bootId}:${at}`,
    sequence: at,
    state,
  });
  const id = result?.observation_id;
  if (result?.recorded !== true || result?.stale === true
      || !Number.isSafeInteger(id) || id <= 0) {
    // A replay response has no row ID. Use a fresh monotonic event key before
    // allowing any Operation to cite the evidence.
    if (result?.replayed === true || result?.stale === true) {
      changed();
      return null;
    }
    throw new Error('WARM_RESULT_OBSERVATION_ID_REQUIRED');
  }
  lastSentRevision = at;
  lastObservedLocalResults = { observation_id: id, state };
  return lastObservedLocalResults;
}

function settleSendForResult(sess, turn) {
  // Warm prompts are sequential and result events are consumed in order. The
  // native conversation must match init, and only a terminal result settles.
  const entry = sess.pendingSends[0];
  if (!entry) return;
  if (turn.conversation_id !== sess.rootId) return;
  const disposition = terminalResultDisposition(turn.status);
  if (!disposition) return;
  sess.pendingSends.shift();
  if (!turn.response_sha256) {
    saveOutcome(entry.operation_id, {
      outcome: 'unknown',
      native_root_id: sess.rootId,
      native_scope_key: sess.scopeKey,
      details: { diagnostic_code: 'NATIVE_RESULT_FINGERPRINT_UNAVAILABLE' },
    }, entry.method);
    return;
  }
  const receipt = {
    input_operation_id: entry.operation_id,
    native_conversation_id: sess.rootId,
    bridge_boot_id: bootId,
    result_ordinal: turn.result_ordinal,
    response_sha256: turn.response_sha256,
    status: turn.status,
  };
  // Keep the observed list independent from the reference carried by the
  // outcome; citing an observation must never mutate that captured receipt.
  localExecutionResults.push({ ...receipt });
  saveOutcome(entry.operation_id, {
    outcome: disposition === 'completed' ? 'applied' : 'rejected',
    native_root_id: sess.rootId,
    native_scope_key: sess.scopeKey,
    details: {
      completion_condition: 'native_terminal_result_observed',
      turn_status: turn.status,
      num_turns: turn.num_turns,
      local_execution_ref: receipt,
    },
  }, entry.method);
}

function finishSession(sess) {
  if (sess.ended) return;
  sess.ended = true;
  if (sess.openWait) {
    const failure = sess.state.init_failure;
    const error = codedError('INIT_FAILURE', failure?.native_responded === true ? false : true);
    error.initFailure = failure;
    sess.openWait.reject(error);
    sess.openWait = null;
  }
  for (const entry of sess.pendingSends.splice(0)) {
    saveOutcome(entry.operation_id, {
      outcome: 'unknown',
      native_root_id: sess.rootId,
      native_scope_key: sess.scopeKey,
      details: { diagnostic_code: 'STREAM_ENDED_BEFORE_RESULT_EVIDENCE' },
    }, entry.method);
  }
  changed();
}

async function pump(sess) {
  const lines = createInterface({ input: sess.child.stdout, crlfDelay: Infinity });
  try {
    for await (const line of lines) {
      if (!line.trim()) continue;
      let event;
      try { event = JSON.parse(line); } catch { noteMalformedLine(sess.state); changed(); continue; }
      const ordinalBefore = sess.state.result_ordinal;
      applyNativeEvent(sess.state, event);
      if (!sess.rootId && sess.state.init?.conversation_id) {
        sess.rootId = sess.state.init.conversation_id;
        if (sess.openWait) { sess.openWait.resolve(); sess.openWait = null; }
      }
      if (sess.state.phase === 'init_failed' && sess.openWait) {
        const failure = sess.state.init_failure;
        const error = codedError('INIT_FAILURE', failure?.native_responded === true ? false : true);
        error.initFailure = failure;
        sess.openWait.reject(error);
        sess.openWait = null;
      }
      if (sess.state.result_ordinal > ordinalBefore) {
        resultOrdinal = sess.state.result_ordinal;
        const turn = sess.state.turns.at(-1);
        if (turn?.result_ordinal === resultOrdinal) settleSendForResult(sess, turn);
      }
      changed();
    }
  } catch (error) {
    noteStreamFailure(sess.state, error);
  }
}

async function startNative(command) {
  if (session && !session.ended) throw codedError('SESSION_ALREADY_OPEN');
  const nativeOptions = command.route?.native_options ?? {};
  const cwd = required(nativeOptions, 'workspaceRoot');
  if (!path.isAbsolute(cwd)) throw codedError('WORKSPACE_ROOT_MUST_BE_ABSOLUTE');
  // Launch-time selection only. The stream documents no live model/effort
  // setter (in-stream /model is an ERROR), so configure stays unavailable
  // after open; an unknown model fails loudly at the native boundary.
  const args = ['--input-format', 'stream-json', '--output-format', 'stream-json'];
  if (nativeOptions.modelId !== undefined) args.push('--model', required(nativeOptions, 'modelId'));
  if (nativeOptions.reasoningEffort !== undefined) {
    if (!EFFORTS.includes(nativeOptions.reasoningEffort)) throw codedError('INVALID_EFFORT');
    args.push('--effort', nativeOptions.reasoningEffort);
  }
  if (nativeOptions.agent !== undefined) args.push('--agent', required(nativeOptions, 'agent'));
  const resumeId = command.input?.resume_conversation_id;
  if (resumeId !== undefined) args.push('--conversation', required(command.input ?? {}, 'resume_conversation_id'));
  if (nativeOptions.dangerouslySkipPermissions === true) args.push('--dangerously-skip-permissions');

  const state = createStreamState(resultOrdinal);
  const sess = {
    state, child: null, rootId: null, scopeKey: nativeScopeKey(),
    pendingSends: [], openWait: null, ended: false,
  };
  let child;
  try {
    child = spawn(config.command, args, { cwd, stdio: ['pipe', 'pipe', 'pipe'], detached: false });
  } catch {
    // Spawn rejected the launch locally; nothing native was started.
    throw codedError('NATIVE_SPAWN_FAILED');
  }
  sess.child = child;
  session = sess;
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', chunk => { noteStderr(state, chunk); changed(); });
  child.on('error', error => { noteStreamFailure(state, error); finishSession(sess); });
  child.on('close', code => {
    noteProcessExit(state, code);
    noteStreamEnd(state);
    finishSession(sess);
  });
  void pump(sess);
  await new Promise((resolve, reject) => { sess.openWait = { resolve, reject }; });
  return {
    native_root_id: sess.rootId,
    native_scope_key: sess.scopeKey,
    details: {
      completion_condition: 'native_session_initialized',
      resumed_conversation: resumeId !== undefined,
      describe: describeFacts(state),
    },
  };
}

function sendNative(command) {
  const sess = session;
  if (!sess || sess.ended || !sess.rootId) throw codedError('NATIVE_SESSION_NOT_READY');
  if (command.native_root_id !== sess.rootId) throw codedError('NATIVE_IDENTITY_MISMATCH');
  const p = command.input ?? {};
  let text;
  if (command.method === 'task.dispatch') {
    const spec = p.task_snapshot ? `Task specification: ${JSON.stringify(p.task_snapshot)}` : null;
    const body = typeof p.text === 'string' && p.text.trim() ? p.text : null;
    text = [spec, body].filter(Boolean).join('\n\n');
    if (!text) throw codedError('DISPATCH_TEXT_REQUIRED');
  } else {
    if (p.delivery === 'steer') throw capabilityError('steer');
    if (p.delivery !== undefined && p.delivery !== 'next_turn') throw codedError('UNSUPPORTED_DELIVERY');
    text = required(p, 'text');
  }
  const line = JSON.stringify(encodeUserMessage(text)) + '\n';
  sess.pendingSends.push({ operation_id: command.operation_id, method: command.method });
  try {
    sess.child.stdin.write(line);
  } catch (error) {
    sess.pendingSends.pop();
    throw codedError('NATIVE_WRITE_FAILED', true);
  }
  // Writing the user event is admission into the warm channel only. The
  // outcome settles when this turn's native result event arrives, or becomes
  // unknown when the stream ends without that evidence.
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
      if (!sess || sess.ended || !sess.rootId) throw codedError('NATIVE_SESSION_NOT_READY');
      const target = command.input?.session_id === undefined ? sess.rootId : required(command.input, 'session_id');
      if (target === sess.rootId) {
        result = {
          details: {
            completion_condition: 'native_read_completed',
            target,
            snapshot: snapshot(sess.state),
          },
        };
      } else {
        const child = sess.state.children.get(target);
        if (!child) throw codedError('UNKNOWN_SESSION');
        result = {
          details: {
            completion_condition: 'native_read_completed',
            target,
            child,
          },
        };
      }
    } else if (command.method === 'agent.reconcile') {
      const targetId = required(command.input ?? {}, 'operation_id');
      const record = journal.get(targetId);
      if (!record) throw codedError('UNKNOWN_OPERATION');
      result = { details: { completion_condition: 'module_journal_readback', recorded: record } };
    } else if (command.method === 'agent.attach') throw capabilityError('attach');
    else if (command.method === 'agent.configure') {
      throw capabilityError(command.input?.settings?.model !== undefined ? 'configure_model' : 'configure_effort');
    } else if (command.method === 'agent.goal') throw capabilityError('goal');
    else if (command.method === 'agent.reply') throw capabilityError('reply');
    else if (command.method === 'agent.recover') throw capabilityError('recover');
    else if (command.method === 'agent.result') throw capabilityError('result_pages');
    else throw codedError('UNSUPPORTED_OPERATION');
    if (session?.rootId) {
      result.native_root_id = session.rootId;
      result.native_scope_key = session.scopeKey;
    }
    saveOutcome(command.operation_id, { outcome: 'applied', ...result }, command.method);
  } catch (error) {
    const details = {
      error_type: error.name,
      diagnostic_code: error.diagnosticCode ?? String(error.message).slice(0, 120),
    };
    if (error.capability) details.capability = error.capability;
    if (error.initFailure) details.init_failure = error.initFailure;
    const outcome = { outcome: error.nativeAdmissionPossible ? 'unknown' : 'rejected', details };
    if (session?.rootId) {
      outcome.native_root_id = session.rootId;
      outcome.native_scope_key = session.scopeKey;
    }
    saveOutcome(command.operation_id, outcome, command.method);
  } finally {
    active.delete(command.operation_id);
  }
}

async function report(link = control) {
  // Store the opened identity before any result-backed task dispatch.
  for (const [id, outcome] of outcomes) {
    if (!hasLocalResult(outcome)) await sendOutcome(link, id, outcome);
  }

  // Retry acknowledged receipts before advancing the Store's materialized
  // observation pointer; the cited row must still be the current one.
  for (const [id, outcome] of outcomes) {
    if (!hasLocalResult(outcome)) continue;
    const ref = outcome.details.local_execution_ref;
    if (Number.isSafeInteger(ref.observation_id) && ref.observation_id > 0) {
      await sendOutcome(link, id, outcome);
    }
  }

  let unbound = [...outcomes].filter(([, outcome]) => hasLocalResult(outcome)
    && !(Number.isSafeInteger(outcome.details.local_execution_ref.observation_id)
      && outcome.details.local_execution_ref.observation_id > 0));
  if (unbound.length) {
    const acknowledged = await bindPendingLocalResults(
      unbound,
      lastObservedLocalResults,
      () => observeForLocalResult(link),
      (id, outcome) => sendOutcome(link, id, outcome),
    );
    if (!acknowledged) return;
    lastObservedLocalResults = acknowledged;
  }

  if (lastSentRevision !== revision) {
    const at = revision;
    const state = observation();
    const result = await link.call('module.observe', { event_id: `${bootId}:${at}`, sequence: at, state });
    if (result?.stale === true) {
      changed();
      return;
    }
    lastSentRevision = at;
    if (Number.isSafeInteger(result?.observation_id) && result.observation_id > 0) {
      lastObservedLocalResults = { observation_id: result.observation_id, state };
    }
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
  // Only explicit termination of this bridge ends its own CLI process: EOF
  // on stdin is the documented graceful end (the process exits after the
  // current turn completes); the kill fallback covers a wedged process.
  const sess = session;
  if (sess && !sess.ended && sess.child) {
    try { sess.child.stdin.end(); } catch { /* already closed */ }
    const exited = await Promise.race([
      new Promise(resolve => sess.child.once('close', () => resolve(true))),
      delay(5000).then(() => false),
    ]);
    if (!exited) { try { sess.child.kill(); } catch { /* already gone */ } }
  }
}
process.once('SIGINT', () => { void stop().then(() => process.exit(0)); });
process.once('SIGTERM', () => { void stop().then(() => process.exit(0)); });

let admissionTail = Promise.resolve();
while (!stopping) {
  try {
    control = new Control(config.endpoint, credential);
    await control.connect();
    const hello = await control.call('module.hello', {
      boot_id: bootId,
      module_artifact_id: config.moduleArtifactId,
      native_ready: Boolean(session && !session.ended && session.rootId),
      ...(managedOwner ? { managed_owner: managedOwner } : {}),
      ...(session?.rootId ? { native_root_id: session.rootId, native_scope_key: session.scopeKey } : {}),
    });
    if (hello.recovery_required) {
      // This artifact cannot verify or resume a recorded native owner (see
      // README). The binding stays reconciling for the operator instead of
      // a silent fresh start or a replayed prompt.
      console.error(JSON.stringify({ component: 'antigravity-bridge', code: 'RECOVERY_UNAVAILABLE_IN_ARTIFACT', binding_id: hello.binding_id }));
    }
    lastSentRevision = -1;
    await report();
    connected = true;
    while (!stopping && control.socket) {
      if (outcomes.size >= 8) { await delay(50); continue; }
      const result = await control.call('module.next', {});
      if (result.command) {
        const command = result.command;
        if (active.has(command.operation_id) || outcomes.has(command.operation_id)) continue;
        // Serialize native admission, not whole model turns. Read-only
        // refresh/reconcile bypass the queue like replies do for Muse.
        if (['agent.refresh', 'agent.reconcile'].includes(command.method)) void execute(command);
        else admissionTail = admissionTail.then(() => execute(command));
        await admissionTail.catch(() => {});
      }
    }
  } catch (error) {
    console.error(JSON.stringify({ component: 'antigravity-bridge', code: error.code ?? error.message, native_preserved: Boolean(session && !session.ended) }));
  } finally {
    connected = false;
    control?.close();
  }
  if (!stopping) await delay(1000);
}
