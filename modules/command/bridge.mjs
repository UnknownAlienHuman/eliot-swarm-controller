#!/usr/bin/env node
// Sessionless Command Code adapter. agent.open probes the local executor and
// pinned mod without creating a native session; one task.dispatch starts one
// frozen `cmd -p` batch. Reconcile reads saved files and never resends input.
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { isDeepStrictEqual } from "node:util";
import { Control } from "../claude/control.mjs";
import {
  HISTORICAL_MODULE_ARTIFACT_ID,
  MODULE_ARTIFACT_ID,
  TASK_PROMPT_CONTRACT_REVISION,
  batchRunId,
  controlRecordRef,
  describe,
  openRun,
  outcomeFromRun,
  resultRecordRef,
  sha256Hex,
  snapshotRun,
  validateTaskPromptDispatch,
} from "./glue.mjs";

const ENTRYPOINT = "command_headless_one_shot";
const FREE_MODEL_ID = "stealth/space-bunny-alpha";
// Canonical LF digest of the tracked mod source; glue normalizes CRLF checkouts.
const PINNED_MOD_SHA256 = "513eaa7d6034cc22b5abf14d080888cdd3e8782133e39f859b7703db123e1f80";
const CONTRACT_SCHEMA = (schemaId) => ({ schema_id: schemaId, version: "1" });
const EXPECTED_MODULE_CONTRACT = {
  schema_version: 1,
  module_id: "runtime.command",
  artifact: {
    artifact_id: MODULE_ARTIFACT_ID,
    version: "5",
  },
  protocol: { major: 1, minor: 0 },
  capabilities: ["agent.open", "agent.reconcile", "agent.refresh", "task.dispatch"],
  config_schema: null,
  command_schemas: [
    CONTRACT_SCHEMA("swarm.runtime_command"),
    CONTRACT_SCHEMA("swarm.task_dispatch_context"),
    CONTRACT_SCHEMA("swarm.task_prompt"),
  ],
  event_schemas: [
    CONTRACT_SCHEMA("swarm.runtime_outcome"),
    CONTRACT_SCHEMA("swarm.task_dispatch_admission"),
  ],
};
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

function ownerModuleContract() {
  if (!managedOwner) throw codedError("MODULE_OWNER_REQUIRED");
  const encoded = process.env.ELIOT_SWARM_MODULE_CONTRACT;
  if (typeof encoded !== "string" || Buffer.byteLength(encoded, "utf8") > 64 * 1024) {
    throw codedError("MODULE_CONTRACT_REQUIRED");
  }
  let claim;
  try {
    claim = JSON.parse(encoded);
  } catch {
    throw codedError("MODULE_CONTRACT_INVALID");
  }
  if (!isDeepStrictEqual(claim, EXPECTED_MODULE_CONTRACT)) {
    throw codedError("MODULE_CONTRACT_MISMATCH");
  }
  return claim;
}

function verifyModuleContractNegotiation(hello, claim) {
  const negotiated = hello?.module_contract_negotiation;
  if (hello?.route?.runtime !== "command"
      || hello.route.module_artifact_id !== MODULE_ARTIFACT_ID
      || negotiated?.status !== "negotiated"
      || negotiated.source !== "store_registered_descriptor"
      || !Number.isSafeInteger(negotiated.descriptor_revision)
      || negotiated.descriptor_revision <= 0
      || negotiated.effects_authorized_by_descriptor !== false
      || negotiated.module_id !== claim.module_id
      || !isDeepStrictEqual(negotiated.artifact, claim.artifact)
      || !isDeepStrictEqual(negotiated.protocol, claim.protocol)
      || !isDeepStrictEqual(negotiated.capabilities, claim.capabilities)
      || !isDeepStrictEqual(negotiated.config_schema, claim.config_schema)
      || !isDeepStrictEqual(negotiated.pre_input_open ?? null, claim.pre_input_open ?? null)
      || !isDeepStrictEqual(negotiated.command_schemas, claim.command_schemas)
      || !isDeepStrictEqual(negotiated.event_schemas, claim.event_schemas)) {
    throw codedError("MODULE_CONTRACT_NEGOTIATION_FAILED");
  }
}

function moduleReceiptFor(command, operationId = command.operation_id, inputSha256 = command.input_sha256) {
  if (typeof command.binding_id !== "string" || command.binding_id.trim() === ""
      || !Number.isSafeInteger(command.generation) || command.generation <= 0
      || typeof operationId !== "string" || operationId.trim() === ""
      || !/^[0-9a-f]{64}$/.test(inputSha256 ?? "")) {
    throw codedError("MODULE_RECEIPT_IDENTITY_INVALID");
  }
  return {
    schema_version: 1,
    module_id: moduleContract.module_id,
    artifact: moduleContract.artifact,
    protocol: moduleContract.protocol,
    binding_id: command.binding_id,
    binding_generation: command.generation,
    operation_id: operationId,
    input_sha256: inputSha256,
  };
}

function attachModuleReceipt(result, moduleReceipt) {
  return {
    ...result,
    details: {
      ...(result?.details && typeof result.details === "object" && !Array.isArray(result.details)
        ? result.details
        : {}),
      module_receipt: moduleReceipt,
    },
  };
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
const MAX_TIMER_DELAY_MS = 2_147_483_647;
const runTimeoutMs = config.runTimeoutMs;
if (runTimeoutMs !== undefined
    && (!Number.isSafeInteger(runTimeoutMs)
      || runTimeoutMs <= 0
      || runTimeoutMs > MAX_TIMER_DELAY_MS)) {
  throw codedError("INVALID_RUN_TIMEOUT_MS");
}
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
const moduleContract = ownerModuleContract();

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
function saveOutcome(operationId, result, method, moduleReceipt) {
  const previous = outcomes.get(operationId);
  if (previous && ["applied", "rejected"].includes(previous.outcome)) return previous;
  const outcome = attachModuleReceipt({ operation_id: operationId, ...result }, moduleReceipt);
  outcomes.set(operationId, outcome);
  journal.set(operationId, {
    method: method ?? null,
    outcome: outcome.outcome,
    completion_condition: outcome.details?.completion_condition ?? null,
    diagnostic_code: outcome.details?.diagnostic_code ?? null,
  });
  if (journal.size > 128) journal.delete(journal.keys().next().value);
  changed();
  return outcome;
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

function unknownTaskOutcome(admission, diagnosticCode, fallback = {}) {
  const operationId = fallback.operation_id ?? admission?.operation_id;
  const facts = fallback.core_binding ?? admission?.core_binding ?? {};
  return {
    outcome: "unknown",
    details: {
      execution_shape: "sessionless_batch",
      batch_run_id: facts.batch_run_id ?? (operationId ? batchRunId(operationId) : null),
      requested_model: fallback.requested_model ?? admission?.requested_model ?? null,
      native_request_model: null,
      native_request_model_status: "unknown",
      native_request_model_evidence: [],
      effective_model: null,
      effective_model_status: "unknown",
      result_subtype: null,
      exit_code: null,
      signal: null,
      spawn_error_observed: false,
      timed_out: false,
      anomalies: [diagnosticCode],
      native_session_id: null,
      prompt_sha256: facts.prompt_sha256 ?? admission?.prompt_sha256 ?? null,
      prompt_bytes: facts.prompt_bytes ?? admission?.prompt_bytes ?? null,
      ...(fallback.task_prompt_contract_revision ? {
        task_prompt_contract_revision: fallback.task_prompt_contract_revision,
        prompt_contract_revision: fallback.task_prompt_contract_revision,
        task_prompt: fallback.task_prompt,
        task_dispatch_context: fallback.task_dispatch_context,
      } : admission?.task_prompt_contract_revision ? {
        task_prompt_contract_revision: admission.task_prompt_contract_revision,
        prompt_contract_revision: admission.prompt_contract_revision,
        task_prompt: admission.task_prompt,
        task_dispatch_context: admission.task_dispatch_context,
      } : {}),
      control_record_ref: operationId ? controlRecordRef(operationId) : null,
      result_ref: operationId ? resultRecordRef(operationId) : null,
      artifact_refs: operationId ? [
        { kind: "command_control_record", ref: controlRecordRef(operationId) },
        { kind: "command_result_record", ref: resultRecordRef(operationId) },
      ] : [],
      result_text_sha256: null,
      result_text_bytes: null,
      diagnostic_code: diagnosticCode,
    },
  };
}

function taskOutcome(run, evidence = run?.evidence_validation) {
  return outcomeFromRun(run, evidence);
}

function runEvidenceMatches(run, admission, operationId, evidence) {
  const artifacts = [
    { kind: "command_control_record", ref: controlRecordRef(operationId) },
    { kind: "command_result_record", ref: resultRecordRef(operationId) },
  ];
  const currentArtifact = admission?.schema === 3
    && admission.module_artifact_id === MODULE_ARTIFACT_ID
    && run?.schema === 3
    && run.module_artifact_id === MODULE_ARTIFACT_ID;
  return evidence?.valid === true
    && currentArtifact
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
      && JSON.stringify(run.core_binding) === JSON.stringify(admission.core_binding)
      && run.control_record_ref === controlRecordRef(operationId)
      && run.result_ref === resultRecordRef(operationId)
      && JSON.stringify(run.artifact_refs) === JSON.stringify(artifacts)
      && (!currentArtifact || (
        admission.task_prompt_contract_revision === TASK_PROMPT_CONTRACT_REVISION
        && run.task_prompt_contract_revision === admission.task_prompt_contract_revision
        && run.prompt_contract_revision === admission.prompt_contract_revision
        && isDeepStrictEqual(run.task_prompt, admission.task_prompt)
        && isDeepStrictEqual(run.task_dispatch_context, admission.task_dispatch_context)
        && isDeepStrictEqual(run.dispatch_admission, admission.dispatch_admission)
      ))
    ));
}

async function dispatchTask(command) {
  const { operationId, modelId, cwd, prompt, coreBinding } = dispatchIdentity(command);
  const controlDir = runDirectory(operationId);
  const record = await openRun(config, {
    operationId,
    requestedModel: modelId,
    prompt,
    coreBinding,
    command,
    bootId,
    moduleContract,
    controlDir,
    cwd,
    ...(runTimeoutMs === undefined ? {} : { timeoutMs: runTimeoutMs }),
  });
  if (record.evidence_validation?.valid !== true) {
    return unknownTaskOutcome(null,
      record.evidence_validation?.diagnostic_code ?? "saved_terminal_evidence_untrusted",
      dispatchFallback({ operationId, modelId, coreBinding }));
  }
  return taskOutcome(record, record.evidence_validation);
}

function dispatchIdentity(command) {
  const operationId = required(command, "operation_id");
  const modelId = routeModel(command);
  const nativeOptions = command.route.native_options ?? {};
  const cwd = required(nativeOptions, "workspaceRoot");
  if (!path.isAbsolute(cwd)) throw codedError("WORKSPACE_ROOT_MUST_BE_ABSOLUTE");
  const validated = validateTaskPromptDispatch(command, bootId, moduleContract);
  if (validated.operationId !== operationId) throw codedError("TASK_DISPATCH_CONTEXT_INVALID");
  return { operationId, modelId, cwd, ...validated };
}

function dispatchFallback(identity) {
  return {
    operation_id: identity.operationId,
    requested_model: identity.modelId,
    core_binding: identity.coreBinding,
    task_prompt_contract_revision: TASK_PROMPT_CONTRACT_REVISION,
    task_prompt: identity.taskPrompt,
    task_dispatch_context: identity.taskDispatchContext,
  };
}

function unknownDispatchOutcome(command, diagnosticCode) {
  try {
    return unknownTaskOutcome(null, diagnosticCode, dispatchFallback(dispatchIdentity(command)));
  } catch {
    // Invalid or incomplete dispatch inputs cannot be made into a core-bound
    // receipt. The Store will reject the missing identity rather than trust
    // values from an unreadable saved record.
    return unknownTaskOutcome(null, diagnosticCode, {
      operation_id: command.operation_id,
    });
  }
}

function matchesCoreBinding(actual, expected) {
  return actual !== null
    && typeof actual === "object"
    && !Array.isArray(actual)
    && Object.keys(actual).sort().join(",") === "batch_run_id,prompt_bytes,prompt_sha256"
    && actual.batch_run_id === expected.batch_run_id
    && actual.prompt_sha256 === expected.prompt_sha256
    && actual.prompt_bytes === expected.prompt_bytes;
}

function targetDispatchFallback(command, targetOperationId) {
  const input = command.input ?? {};
  const requestedModel = input.target_command_requested_model;
  const coreBinding = input.target_command_core_binding;
  if (typeof requestedModel !== "string"
      || requestedModel !== routeModel(command)
      || !coreBinding
      || typeof coreBinding !== "object"
      || Array.isArray(coreBinding)
      || Object.keys(coreBinding).sort().join(",") !== "batch_run_id,prompt_bytes,prompt_sha256"
      || coreBinding.batch_run_id !== batchRunId(targetOperationId)
      || !/^[0-9a-f]{64}$/.test(coreBinding.prompt_sha256 ?? "")
      || !Number.isSafeInteger(coreBinding.prompt_bytes)
      || coreBinding.prompt_bytes < 1) {
    throw codedError("TARGET_COMMAND_CORE_BINDING_INVALID");
  }
  return {
    operation_id: targetOperationId,
    requested_model: requestedModel,
    core_binding: coreBinding,
  };
}

function targetUnknown(targetOperationId, fallback, diagnosticCode, moduleReceipt) {
  const result = unknownTaskOutcome(null, diagnosticCode, fallback);
  return saveOutcome(targetOperationId, result, "task.dispatch", moduleReceipt);
}

function unknownOpenTargetOutcome() {
  return {
    outcome: "unknown",
    details: {
      execution_shape: "sessionless_batch",
      native_session_state: "not_started",
      diagnostic_code: "original_preflight_receipt_unavailable",
    },
  };
}

function reconcileTarget(command) {
  const targetOperationId = required(command.input ?? {}, "operation_id");
  const targetMethod = required(command.input ?? {}, "target_command_method");
  if (!new Set(["agent.open", "task.dispatch"]).has(targetMethod)) {
    throw codedError("TARGET_COMMAND_METHOD_INVALID");
  }
  const targetModuleReceipt = moduleReceiptFor(
    command,
    targetOperationId,
    command.target_input_sha256,
  );
  if (targetMethod === "agent.open") {
    // The original open is a version/mod preflight only. The bridge keeps no
    // durable operation receipt for it, so a lost acknowledgement cannot be
    // reconstructed by probing the CLI again.
    const targetResult = saveOutcome(
      targetOperationId,
      unknownOpenTargetOutcome(),
      "agent.open",
      targetModuleReceipt,
    );
    return {
      target_operation_id: targetOperationId,
      target_record_state: "preflight_receipt_unavailable",
      target_outcome: { operation_id: targetOperationId, ...targetResult },
    };
  }
  const fallback = targetDispatchFallback(command, targetOperationId);
  const targetDir = runDirectory(targetOperationId);
  let saved;
  try {
    saved = snapshotRun(targetDir);
  } catch {
      const targetResult = targetUnknown(
        targetOperationId,
        fallback,
        "saved_record_unreadable",
        targetModuleReceipt,
      );
    return {
      target_operation_id: targetOperationId,
      target_record_state: "unreadable",
      target_outcome: targetResult,
    };
  }
  if (saved.admission?.operation_id !== targetOperationId) {
    const missing = saved.admission === null;
    const targetResult = targetUnknown(
      targetOperationId,
      fallback,
      missing ? "saved_admission_missing" : "saved_admission_identity_mismatch",
      targetModuleReceipt,
    );
    return {
      target_operation_id: targetOperationId,
      target_record_state: missing ? "admission_missing" : "identity_mismatch",
      target_outcome: targetResult,
    };
  }
  const admissionMatchesCore = saved.admission.requested_model === fallback.requested_model
    && matchesCoreBinding(saved.admission.core_binding, fallback.core_binding);
  if (saved.admission.module_artifact_id === HISTORICAL_MODULE_ARTIFACT_ID
      && saved.evidence?.valid === true) {
    const historical = unknownTaskOutcome(null, "historical_artifact_readback_only", fallback);
    historical.details.historical_evidence = {
      source_artifact_id: HISTORICAL_MODULE_ARTIFACT_ID,
      terminal: saved.terminal,
      prompt_sha256: saved.admission.prompt_sha256,
      prompt_bytes: saved.admission.prompt_bytes,
    };
    const targetOutcome = saveOutcome(
      targetOperationId,
      historical,
      "task.dispatch",
      targetModuleReceipt,
    );
    return {
      target_operation_id: targetOperationId,
      target_record_state: "historical_record_readable",
      target_outcome: targetOutcome,
    };
  }
  const identityMatches = admissionMatchesCore
    && runEvidenceMatches(saved.run, saved.admission, targetOperationId, saved.evidence);
  if (!identityMatches) {
    const targetResult = targetUnknown(
      targetOperationId,
      fallback,
      saved.evidence?.diagnostic_code ?? "saved_record_identity_mismatch",
      targetModuleReceipt,
    );
    return {
      target_operation_id: targetOperationId,
      target_record_state: saved.run ? "terminal_record_untrusted" : "admission_untrusted",
      target_outcome: targetResult,
    };
  }
  const targetResult = saved.run
    ? taskOutcome(saved.run, saved.evidence)
    : unknownTaskOutcome(null, "native_result_missing_after_admission", fallback);
  const targetOutcome = saveOutcome(
    targetOperationId,
    targetResult,
    "task.dispatch",
    targetModuleReceipt,
  );
  return {
    target_operation_id: targetOperationId,
    target_record_state: saved.run ? "terminal_record_observed" : "admission_only",
    target_outcome: targetOutcome,
  };
}

async function execute(command) {
  const operationId = required(command, "operation_id");
  const moduleReceipt = moduleReceiptFor(command);
  active.add(operationId);
  try {
    let result;
    if (command.method === "agent.open") {
      result = await preflight(command);
      const { outcome, ...record } = result;
      saveOutcome(operationId, { outcome, details: record.details }, command.method, moduleReceipt);
    } else if (command.method === "task.dispatch") {
      result = await dispatchTask(command);
      saveOutcome(operationId, result, command.method, moduleReceipt);
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
      }, command.method, moduleReceipt);
    } else if (command.method === "agent.refresh") {
      saveOutcome(operationId, {
        outcome: "applied",
        details: {
          execution_shape: "sessionless_batch",
          completion_condition: "batch_snapshot_readback",
          snapshot: observation(),
        },
      }, command.method, moduleReceipt);
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
        const currentIdentity = dispatchIdentity(command);
        const fallback = dispatchFallback(currentIdentity);
        if (saved.admission?.operation_id === operationId) {
          const savedBinding = saved.admission?.core_binding;
          const sameRequestBinding = savedBinding?.batch_run_id === currentIdentity.coreBinding.batch_run_id
            && savedBinding?.prompt_sha256 === currentIdentity.coreBinding.prompt_sha256
            && savedBinding?.prompt_bytes === currentIdentity.coreBinding.prompt_bytes
            && saved.admission?.requested_model === currentIdentity.modelId;
          result = !sameRequestBinding
            ? unknownTaskOutcome(null, "dispatch_identity_conflict", fallback)
            : !runEvidenceMatches(saved.run, saved.admission, operationId, saved.evidence)
              ? unknownTaskOutcome(null, saved.evidence?.diagnostic_code ?? "saved_record_identity_mismatch", fallback)
            : saved.run
              ? taskOutcome(saved.run, saved.evidence)
              : unknownTaskOutcome(null, "native_result_missing_after_admission", fallback);
        } else if (existsSync(path.join(dir, "admission.json"))) {
          result = unknownTaskOutcome(null, "admission_record_unreadable", fallback);
        }
      } catch { /* corrupt evidence remains an explicit unknown below */ }
    }
    if (!result) {
      result = command.method === "task.dispatch"
        ? unknownDispatchOutcome(command, error.code ?? "COMMAND_BRIDGE_ERROR")
        : {
            outcome: "rejected",
            details: {
              diagnostic_code: error.code ?? "COMMAND_BRIDGE_ERROR",
              ...(error.capability ? { capability: error.capability } : {}),
            },
          };
    }
    saveOutcome(operationId, result, command.method, moduleReceipt);
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
      managed_owner: managedOwner,
      module_contract: moduleContract,
    });
    verifyModuleContractNegotiation(hello, moduleContract);
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
