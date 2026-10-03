#!/usr/bin/env node
// Sessionless Command Code adapter. agent.open probes the local executor and
// pinned mod without creating a native session; one task.dispatch starts one
// frozen `cmd -p` batch. Reconcile reads saved files and never resends input.
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { Control } from "../claude/control.mjs";
import {
  MODULE_ARTIFACT_ID,
  batchRunId,
  buildTaskPrompt,
  controlRecordRef,
  describe,
  openRun,
  outcomeFromRun,
  resultRecordRef,
  sha256Hex,
  snapshotRun,
} from "./glue.mjs";

const ENTRYPOINT = "command_headless_one_shot";
const FREE_MODEL_ID = "stealth/space-bunny-alpha";
// Canonical LF digest of the tracked mod source; glue normalizes CRLF checkouts.
const PINNED_MOD_SHA256 = "513eaa7d6034cc22b5abf14d080888cdd3e8782133e39f859b7703db123e1f80";
const CAPABILITIES = {
  describe: "implemented",
  open: "executor_preflight_only_no_native_session",
  task_dispatch: "one_shot_sessionless_batch",
  reconcile: "saved_evidence_readback_only",
  refresh: "module_snapshot_read_only",
  send_next_turn: "unavailable_sessionless_batch",
  configure_model: "unavailable",
  configure_effort: "unavailable",
  goal: "unavailable",
  resume: "unavailable",
  attach: "unavailable",
  steer: "unavailable",
  reply: "unavailable",
  result_pages: "unavailable",
  recover: "unavailable",
};

function required(object, key) {
  if (typeof object?.[key] !== "string" || !object[key].trim()) {
    const error = new Error(`MISSING_${key}`);
    error.code = `MISSING_${key}`;
    throw error;
  }
  return object[key];
}

function codedError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

function capabilityError(capability) {
  const error = codedError("CAPABILITY_UNAVAILABLE");
  error.capability = capability;
  return error;
}

const argv = process.argv.slice(2);
if (argv.length !== 2 || argv[0] !== "--config") {
  console.error("Usage: node bridge.mjs --config <local-module.json>");
  process.exit(2);
}
const config = JSON.parse(await readFile(argv[1], "utf8"));
const credentialFile = required(config, "credentialFile");
const endpoint = required(config, "endpoint");
const credential = JSON.parse(await readFile(credentialFile, "utf8"));
if (config.moduleArtifactId !== MODULE_ARTIFACT_ID) throw codedError("MODULE_ARTIFACT_MISMATCH");
if (typeof config.command !== "string" || !path.isAbsolute(config.command)) {
  throw codedError("NATIVE_EXECUTABLE_MUST_BE_ABSOLUTE");
}
if (process.platform === "win32" && /\.(cmd|bat)$/i.test(config.command)) {
  throw codedError("USE_NATIVE_EXE_NOT_SHELL_WRAPPER");
}
if (!Array.isArray(config.commandArgs ?? []) || (config.commandArgs ?? []).some(arg => typeof arg !== "string" || !path.isAbsolute(arg))) {
  throw codedError("COMMAND_ARGS_MUST_BE_ABSOLUTE_ARGUMENTS");
}
if ((config.commandArgs ?? []).some(arg =>
  ["-p", "--print", "--output-format", "--model", "--mod", "--resume", "-r", "--continue", "-c"].includes(arg)
    || arg.startsWith("--model=")
    || arg.startsWith("--mod=")
    || arg.startsWith("--output-format="))) {
  throw codedError("ARGS_PREFIX_CONFLICTS_WITH_GLUE_OWNED_FLAGS");
}
const controlRoot = required(config, "controlRoot");
if (!path.isAbsolute(controlRoot)) throw codedError("CONTROL_ROOT_MUST_BE_ABSOLUTE");
if (config.modPath !== undefined && (typeof config.modPath !== "string" || !path.isAbsolute(config.modPath))) {
  throw codedError("MOD_PATH_MUST_BE_ABSOLUTE");
}

let managedOwner = null;
let bootId = randomUUID();
{
  const dir = process.env.ELIOT_SWARM_MODULE_STATE;
  const ownerFile = process.env.ELIOT_SWARM_MODULE_OWNER;
  if (dir && ownerFile) {
    if (!path.isAbsolute(dir) || ownerFile !== path.join(dir, "owner.json")) throw codedError("INVALID_MODULE_OWNER_PATH");
    const owner = JSON.parse(await readFile(ownerFile, "utf8"));
    if (owner.version !== 1 || owner.process?.purpose !== "module" || typeof owner.token !== "string") throw codedError("INVALID_MODULE_OWNER_RECORD");
    managedOwner = owner;
    bootId = owner.token;
  } else if (dir || ownerFile) {
    throw codedError("INVALID_MODULE_OWNER_PATH");
  }
}

let control = null;
let connected = false;
let stopping = false;
let revision = 0;
let lastSentRevision = -1;
let currentRoute = null;
let latestPreflight = null;
const outcomes = new Map();
const journal = new Map();
const active = new Set();

function changed() { revision += 1; }
function runDirectory(operationId) {
  return path.join(controlRoot, `op-${sha256Hex(operationId).slice(0, 32)}`);
}
function routeModel(command) {
  const modelId = required(command.route?.native_options ?? {}, "modelId");
  if (modelId !== modelId.trim()) throw codedError("INVALID_REQUESTED_MODEL");
  if (modelId !== FREE_MODEL_ID) throw codedError("UNSUPPORTED_ROUTE_MODEL");
  return modelId;
}
function requestedModelFromRoute() {
  const modelId = currentRoute?.modelId;
  return typeof modelId === "string" ? modelId : null;
}
function moduleDescribe() {
  return {
    module_artifact_id: MODULE_ARTIFACT_ID,
    entrypoint: ENTRYPOINT,
    execution_shape: "sessionless_batch",
    executor_version: latestPreflight?.cli_version ?? null,
    installed_runtime_verified: false,
    requested_model: requestedModelFromRoute(),
    effective_model: null,
    effective_model_status: "unknown",
    capabilities: CAPABILITIES,
  };
}
function observation() {
  return {
    phase: "sessionless",
    native_root_id: null,
    native_scope_key: null,
    native_session_state: "not_started",
    boot_id: bootId,
    describe: moduleDescribe(),
    latest_preflight: latestPreflight,
  };
}
function saveOutcome(operationId, result, method) {
  const previous = outcomes.get(operationId);
  if (previous && ["applied", "rejected"].includes(previous.outcome)) return;
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

async function preflight(command) {
  const modelId = routeModel(command);
  const nativeOptions = command.route.native_options ?? {};
  if (typeof nativeOptions.workspaceRoot !== "string" || !path.isAbsolute(nativeOptions.workspaceRoot)) {
    throw codedError("WORKSPACE_ROOT_MUST_BE_ABSOLUTE");
  }
  const facts = await describe({ ...config, moduleArtifactId: MODULE_ARTIFACT_ID });
  latestPreflight = {
    cli_version: facts.cli_version,
    cli_version_note: facts.cli_version_note,
    mod_sha256: facts.mod.sha256,
    observed_at: new Date().toISOString(),
  };
  changed();
  const modPinMatches = facts.mod.sha256 === PINNED_MOD_SHA256;
  const complete = typeof facts.cli_version === "string" && modPinMatches;
  return {
    outcome: complete ? "applied" : "rejected",
    details: {
      execution_shape: "sessionless_batch",
      completion_condition: complete ? "executor_preflight_completed" : "executor_preflight_rejected",
      requested_model: modelId,
      effective_model: null,
      effective_model_status: "unknown",
      native_session_state: "not_started",
      executor_version: facts.cli_version,
      executor_version_note: facts.cli_version_note,
      mod_sha256: facts.mod.sha256,
      mod_pin_matches: modPinMatches,
      diagnostic_code: complete ? null : "EXECUTOR_PREFLIGHT_INCOMPLETE",
      executor_version_probe_performed: true,
      model_execution_probe_performed: false,
    },
  };
}

function unknownTaskOutcome(admission, diagnosticCode) {
  return {
    outcome: "unknown",
    details: {
      execution_shape: "sessionless_batch",
      batch_run_id: admission?.operation_id ? batchRunId(admission.operation_id) : null,
      requested_model: admission?.requested_model ?? null,
      effective_model: null,
      effective_model_status: "unknown",
      result_subtype: null,
      exit_code: null,
      signal: null,
      anomalies: [diagnosticCode],
      native_session_id: null,
      prompt_sha256: admission?.prompt_sha256 ?? null,
      prompt_bytes: admission?.prompt_bytes ?? null,
      control_record_ref: admission?.operation_id ? controlRecordRef(admission.operation_id) : null,
      result_ref: admission?.operation_id ? resultRecordRef(admission.operation_id) : null,
      artifact_refs: admission?.operation_id ? [
        { kind: "command_control_record", ref: controlRecordRef(admission.operation_id) },
        { kind: "command_result_record", ref: resultRecordRef(admission.operation_id) },
      ] : [],
      result_text_sha256: null,
      result_text_bytes: null,
      diagnostic_code: diagnosticCode,
    },
  };
}

function taskOutcome(run) {
  return outcomeFromRun(run);
}

function runEvidenceMatches(run, admission, operationId) {
  const artifacts = [
    { kind: "command_control_record", ref: controlRecordRef(operationId) },
    { kind: "command_result_record", ref: resultRecordRef(operationId) },
  ];
  return admission?.schema === 1
    && admission.operation_id === operationId
    && admission.batch_run_id === batchRunId(operationId)
    && admission.control_record_ref === controlRecordRef(operationId)
    && admission.result_ref === resultRecordRef(operationId)
    && JSON.stringify(admission.artifact_refs) === JSON.stringify(artifacts)
    && (!run || (
      run.operation_id === operationId
      && run.batch_run_id === batchRunId(operationId)
      && run.requested_model === admission.requested_model
      && run.prompt_sha256 === admission.prompt_sha256
      && run.prompt_bytes === admission.prompt_bytes
      && run.control_record_ref === controlRecordRef(operationId)
      && run.result_ref === resultRecordRef(operationId)
      && JSON.stringify(run.artifact_refs) === JSON.stringify(artifacts)
    ));
}

async function dispatchTask(command) {
  const operationId = required(command, "operation_id");
  const modelId = routeModel(command);
  const nativeOptions = command.route.native_options ?? {};
  const cwd = required(nativeOptions, "workspaceRoot");
  if (!path.isAbsolute(cwd)) throw codedError("WORKSPACE_ROOT_MUST_BE_ABSOLUTE");
  const input = command.input ?? {};
  const prompt = buildTaskPrompt(input.task_snapshot, input.text);
  const controlDir = runDirectory(operationId);
  const record = await openRun(config, {
    operationId,
    requestedModel: modelId,
    prompt,
    controlDir,
    cwd,
  });
  return taskOutcome(record);
}

function reconcileTarget(command) {
  const targetOperationId = required(command.input ?? {}, "operation_id");
  const targetDir = runDirectory(targetOperationId);
  let saved;
  try {
    saved = snapshotRun(targetDir);
  } catch {
    const targetResult = unknownTaskOutcome({ operation_id: targetOperationId }, "saved_record_unreadable");
    saveOutcome(targetOperationId, targetResult, "task.dispatch");
    return {
      target_operation_id: targetOperationId,
      target_record_state: "unreadable",
      target_outcome: targetResult,
    };
  }
  if (saved.admission?.operation_id !== targetOperationId) {
    return {
      target_operation_id: targetOperationId,
      target_record_state: "not_found",
      target_outcome: null,
    };
  }
  const identityMatches = runEvidenceMatches(saved.run, saved.admission, targetOperationId);
  const targetResult = !identityMatches
    ? unknownTaskOutcome(saved.admission, "saved_record_identity_mismatch")
    : saved.run
      ? taskOutcome(saved.run)
      : unknownTaskOutcome(saved.admission, "native_result_missing_after_admission");
  saveOutcome(targetOperationId, targetResult, "task.dispatch");
  return {
    target_operation_id: targetOperationId,
    target_record_state: saved.run ? "terminal_record_observed" : "admission_only",
    target_outcome: { operation_id: targetOperationId, ...targetResult },
  };
}

async function execute(command) {
  const operationId = required(command, "operation_id");
  active.add(operationId);
  try {
    let result;
    if (command.method === "agent.open") {
      result = await preflight(command);
      const { outcome, ...record } = result;
      saveOutcome(operationId, { outcome, details: record.details }, command.method);
    } else if (command.method === "task.dispatch") {
      result = await dispatchTask(command);
      saveOutcome(operationId, result, command.method);
    } else if (command.method === "agent.reconcile") {
      result = reconcileTarget(command);
      saveOutcome(operationId, {
        outcome: "applied",
        details: {
          execution_shape: "sessionless_batch",
          completion_condition: "batch_readback_recorded",
          native_replay: false,
          ...result,
        },
      }, command.method);
    } else if (command.method === "agent.refresh") {
      saveOutcome(operationId, {
        outcome: "applied",
        details: {
          execution_shape: "sessionless_batch",
          completion_condition: "batch_snapshot_readback",
          snapshot: observation(),
        },
      }, command.method);
    } else if (command.method === "agent.send") {
      throw capabilityError("send_next_turn");
    } else if (command.method === "agent.configure") {
      const setting = command.input?.settings?.model !== undefined ? "configure_model" : "configure_effort";
      throw capabilityError(setting);
    } else if (["agent.goal", "agent.reply", "agent.recover", "agent.result"].includes(command.method)) {
      const capability = {
        "agent.goal": "goal",
        "agent.reply": "reply",
        "agent.recover": "recover",
        "agent.result": "result_pages",
      }[command.method];
      throw capabilityError(capability);
    } else {
      throw codedError("UNSUPPORTED_OPERATION");
    }
  } catch (error) {
    let result;
    if (command.method === "task.dispatch") {
      try {
        const dir = runDirectory(operationId);
        const saved = snapshotRun(dir);
        if (saved.admission?.operation_id === operationId) {
          result = !runEvidenceMatches(saved.run, saved.admission, operationId)
            ? unknownTaskOutcome(saved.admission, "saved_record_identity_mismatch")
            : saved.run
              ? taskOutcome(saved.run)
              : unknownTaskOutcome(saved.admission, "native_result_missing_after_admission");
        } else if (existsSync(path.join(dir, "admission.json"))) {
          result = unknownTaskOutcome({ operation_id: operationId }, "admission_record_unreadable");
        }
      } catch { /* corrupt evidence remains an explicit unknown below */ }
    }
    if (!result) {
      result = {
        outcome: "rejected",
        details: {
          diagnostic_code: error.code ?? "COMMAND_BRIDGE_ERROR",
          ...(error.capability ? { capability: error.capability } : {}),
        },
      };
    }
    saveOutcome(operationId, result, command.method);
  } finally {
    active.delete(operationId);
  }
}

async function report(link = control) {
  for (const [id, outcome] of outcomes) {
    await link.call("module.outcome", outcome);
    if (outcomes.get(id) === outcome) outcomes.delete(id);
  }
  if (lastSentRevision !== revision) {
    const sequence = revision;
    await link.call("module.observe", {
      event_id: `${bootId}:${sequence}`,
      sequence,
      state: observation(),
    });
    lastSentRevision = sequence;
  }
}

let reportBusy = false;
const reportTimer = setInterval(() => {
  if (!connected || reportBusy) return;
  reportBusy = true;
  const link = control;
  void report(link).catch(() => link.close()).finally(() => { reportBusy = false; });
}, 1000);
reportTimer.unref();

async function stop() {
  if (stopping) return;
  stopping = true;
  connected = false;
  control?.close();
  clearInterval(reportTimer);
}
process.once("SIGINT", () => { void stop().then(() => process.exit(0)); });
process.once("SIGTERM", () => { void stop().then(() => process.exit(0)); });

while (!stopping) {
  try {
    control = new Control(endpoint, credential);
    await control.connect();
    const hello = await control.call("module.hello", {
      boot_id: bootId,
      module_artifact_id: MODULE_ARTIFACT_ID,
      native_ready: false,
      ...(managedOwner ? { managed_owner: managedOwner } : {}),
    });
    currentRoute = hello.route?.native_options ?? null;
    lastSentRevision = -1;
    await report();
    connected = true;
    while (!stopping && control.socket) {
      if (outcomes.size >= 8) { await delay(50); continue; }
      const result = await control.call("module.next", {});
      if (!result.command) continue;
      const command = result.command;
      if (active.has(command.operation_id) || outcomes.has(command.operation_id)) continue;
      // Task prompts are serialized; reconcile is read-only and can proceed
      // while a native batch is running only in a separate bridge owner.
      await execute(command);
    }
  } catch (error) {
    console.error(JSON.stringify({ component: "command-bridge", code: error.code ?? "MODULE_LINK_FAILED" }));
  } finally {
    connected = false;
    control?.close();
  }
  if (!stopping) await delay(1000);
}
