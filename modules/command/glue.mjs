// Eliot Command Code glue — the host-side unit of modules/command.
//
// Drives the installed Command Code CLI (`cmd`) in its documented headless
// print mode (`-p --output-format json`) with the pinned Eliot mod loaded via
// `--mod`, and reduces the run to the module contract's describe / open /
// snapshot facts. bridge.mjs owns module IPC and calls these functions.
//
// Evidence rules, from the architecture and runtime notes:
// - The terminal fact is the final NDJSON result line, corroborated by the
//   process exit code and by the saved run projection. Process exit alone
//   never proves success, and missing/inconsistent saved evidence is unknown.
// - `mod_error` events are observed facts about a mod, not process death:
//   the run continues to its own result line.
// - Mod readiness (mod-journal records written by the mod itself) is tracked
//   independently of process liveness.
// - Usage is reported raw from the result line. Run/round totals are never
//   summed together (runtime notes, Command Code section).

import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import {
  accessSync,
  constants,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";

export const RUNTIME = "command";
export const ENTRYPOINT = "native_mod";
export const TRANSPORT = "headless_ndjson";
export const MODULE_ARTIFACT_ID = "command-mod-0.1.0-glue.5";
export const HISTORICAL_MODULE_ARTIFACT_ID = "command-mod-0.1.0-glue.4";
export const LEGACY_MODULE_ARTIFACT_ID = "command-mod-0.1.0-glue.2";
export const PREVIOUS_MODULE_ARTIFACT_ID = "command-mod-0.1.0-glue.3";
export const TASK_PROMPT_CONTRACT_REVISION = "task-prompt-v1";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
export const DEFAULT_MOD_PATH = join(MODULE_DIR, "mod", "eliot-command.ts");

// Documented headless exit codes (official headless page, accessed
// 2026-10-02). The constant names are the vendor's, kept verbatim.
export const EXIT_MEANINGS = {
  0: "EXIT_SUCCESS",
  1: "EXIT_ERROR",
  3: "EXIT_AUTH_ERROR",
  4: "EXIT_PERMISSION_DENIED",
  5: "EXIT_RATE_LIMITED",
  6: "EXIT_CONNECTION_ERROR",
  7: "EXIT_SERVER_ERROR",
  8: "EXIT_MAX_TURNS_REACHED",
  9: "EXIT_NO_RESPONSE",
  10: "EXIT_INSUFFICIENT_CREDITS",
  130: "EXIT_INTERRUPTED",
};

const RESULT_SUBTYPES = new Set(["success", "error", "max_turns"]);
const STDERR_LIMIT_BYTES = 256 * 1024;

export function sha256Hex(text) {
  return createHash("sha256").update(text, "utf8").digest("hex");
}

const TASK_PROMPT_FIELDS = [
  "schema_id",
  "schema_version",
  "task_id",
  "task_revision",
  "attempt_id",
  "task_snapshot_sha256",
  "prompt_sha256",
  "prompt_bytes",
  "prompt",
];
const TASK_DISPATCH_CONTEXT_FIELDS = [
  "schema_version",
  "operation_id",
  "binding_id",
  "binding_generation",
  "worker_boot_id",
  "attempt_id",
  "task_id",
  "task_revision",
  "task_snapshot_sha256",
  "source_text_sha256",
  "source_text_bytes",
];
const LOWER_SHA256 = /^[0-9a-f]{64}$/;

function exactFields(value, fields) {
  return value !== null
    && typeof value === "object"
    && !Array.isArray(value)
    && Object.keys(value).sort().join(",") === [...fields].sort().join(",");
}

function validBoundedTaskIdentity(value) {
  return typeof value === "string"
    && value.length > 0
    && Buffer.byteLength(value, "utf8") <= 512
    && !/\p{Cc}/u.test(value);
}

function failTaskPrompt(code) {
  const error = new Error(code);
  error.code = code;
  throw error;
}

/** Validate the exact Store dispatch before admission or a native spawn. */
export function validateTaskPromptDispatch(command, bootId, moduleContract) {
  const operationId = command?.operation_id;
  const bindingId = command?.binding_id;
  const generation = command?.generation;
  const inputSha256 = command?.input_sha256;
  const input = command?.input;
  if (command?.method !== "task.dispatch"
      || typeof operationId !== "string" || operationId.trim() === ""
      || typeof bindingId !== "string" || bindingId.trim() === ""
      || !Number.isSafeInteger(generation) || generation <= 0
      || !LOWER_SHA256.test(inputSha256 ?? "")
      || typeof bootId !== "string" || bootId.trim() === ""
      || input === null || typeof input !== "object" || Array.isArray(input)) {
    failTaskPrompt("TASK_DISPATCH_CONTEXT_INVALID");
  }
  if (Object.hasOwn(input, "task_snapshot") || Object.hasOwn(input, "task_snapshot_canonical")) {
    failTaskPrompt("TASK_PROMPT_SNAPSHOT_FALLBACK_FORBIDDEN");
  }
  const envelope = input.task_prompt;
  const context = input.task_dispatch_context;
  if (!exactFields(envelope, TASK_PROMPT_FIELDS)
      || envelope.schema_id !== "swarm.task_prompt"
      || envelope.schema_version !== 1
      || !validBoundedTaskIdentity(envelope.task_id)
      || !Number.isSafeInteger(envelope.task_revision) || envelope.task_revision <= 0
      || !validBoundedTaskIdentity(envelope.attempt_id)
      || !LOWER_SHA256.test(envelope.task_snapshot_sha256 ?? "")
      || !LOWER_SHA256.test(envelope.prompt_sha256 ?? "")
      || typeof envelope.prompt !== "string" || envelope.prompt.trim() === ""
      || !Number.isSafeInteger(envelope.prompt_bytes) || envelope.prompt_bytes <= 0
      || Buffer.byteLength(envelope.prompt, "utf8") !== envelope.prompt_bytes) {
    failTaskPrompt("TASK_PROMPT_INVALID");
  }
  if (input.attempt_id !== envelope.attempt_id) {
    failTaskPrompt("TASK_PROMPT_IDENTITY_MISMATCH");
  }
  if (sha256Hex(envelope.prompt) !== envelope.prompt_sha256) {
    failTaskPrompt("TASK_PROMPT_DIGEST_MISMATCH");
  }
  if (!exactFields(context, TASK_DISPATCH_CONTEXT_FIELDS)
      || context.schema_version !== 1
      || typeof context.operation_id !== "string" || context.operation_id.trim() === ""
      || typeof context.binding_id !== "string" || context.binding_id.trim() === ""
      || !Number.isSafeInteger(context.binding_generation) || context.binding_generation <= 0
      || typeof context.worker_boot_id !== "string" || context.worker_boot_id.trim() === ""
      || typeof context.attempt_id !== "string" || context.attempt_id.trim() === ""
      || typeof context.task_id !== "string" || context.task_id.trim() === ""
      || !Number.isSafeInteger(context.task_revision) || context.task_revision <= 0
      || !LOWER_SHA256.test(context.task_snapshot_sha256 ?? "")
      || !LOWER_SHA256.test(context.source_text_sha256 ?? "")
      || !Number.isSafeInteger(context.source_text_bytes) || context.source_text_bytes < 0
      || context.operation_id !== operationId
      || context.binding_id !== bindingId
      || context.binding_generation !== generation
      || context.worker_boot_id !== bootId
      || context.attempt_id !== envelope.attempt_id
      || context.task_id !== envelope.task_id
      || context.task_revision !== envelope.task_revision
      || context.task_snapshot_sha256 !== envelope.task_snapshot_sha256) {
    failTaskPrompt("TASK_DISPATCH_CONTEXT_INVALID");
  }
  const sourceText = input.text;
  if (typeof sourceText !== "string" || sourceText.trim() === ""
      || Buffer.byteLength(sourceText, "utf8") !== context.source_text_bytes
      || sha256Hex(sourceText) !== context.source_text_sha256) {
    failTaskPrompt("TASK_DISPATCH_CONTEXT_INVALID");
  }
  const coreBinding = commandReceiptFacts(operationId, envelope.prompt);
  if (!coreBindingMatches(input.command_core_binding, coreBinding)) {
    failTaskPrompt("CORE_PROMPT_BINDING_MISMATCH");
  }
  if (!moduleContract || moduleContract.module_id !== "runtime.command"
      || moduleContract.artifact?.artifact_id !== MODULE_ARTIFACT_ID
      || moduleContract.artifact?.version !== "5"
      || moduleContract.protocol?.major !== 1 || moduleContract.protocol?.minor !== 0) {
    failTaskPrompt("MODULE_CONTRACT_MISMATCH");
  }
  const moduleReceipt = {
    schema_version: 1,
    module_id: moduleContract.module_id,
    artifact: moduleContract.artifact,
    protocol: moduleContract.protocol,
    binding_id: bindingId,
    binding_generation: generation,
    operation_id: operationId,
    input_sha256: inputSha256,
  };
  const dispatchAdmission = {
    schema_version: 1,
    module_receipt: moduleReceipt,
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
    native_input_id: null,
  };
  return {
    operationId,
    bindingId,
    generation,
    prompt: envelope.prompt,
    coreBinding,
    taskPrompt: {
      schema_id: envelope.schema_id,
      schema_version: envelope.schema_version,
      task_id: envelope.task_id,
      task_revision: envelope.task_revision,
      attempt_id: envelope.attempt_id,
      task_snapshot_sha256: envelope.task_snapshot_sha256,
      prompt_sha256: envelope.prompt_sha256,
      prompt_bytes: envelope.prompt_bytes,
    },
    taskDispatchContext: { ...context },
    moduleReceipt,
    dispatchAdmission,
  };
}

function operationDigest(operationId) {
  if (typeof operationId !== "string" || operationId.trim() === "") {
    throw new Error("OPERATION_ID_REQUIRED");
  }
  return sha256Hex(operationId).slice(0, 32);
}

export function batchRunId(operationId) {
  return `command-batch:${operationDigest(operationId)}`;
}

export function commandReceiptFacts(operationId, prompt) {
  if (typeof prompt !== "string") throw new Error("PROMPT_REQUIRED");
  return {
    batch_run_id: batchRunId(operationId),
    prompt_sha256: sha256Hex(prompt),
    prompt_bytes: Buffer.byteLength(prompt, "utf8"),
  };
}

function coreBindingMatches(actual, expected) {
  return actual !== null
    && typeof actual === "object"
    && !Array.isArray(actual)
    && Object.keys(actual).sort().join(",") === "batch_run_id,prompt_bytes,prompt_sha256"
    && actual.batch_run_id === expected.batch_run_id
    && actual.prompt_sha256 === expected.prompt_sha256
    && actual.prompt_bytes === expected.prompt_bytes;
}

function nativeEnvironment(controlDir = null) {
  const env = {};
  for (const [name, value] of Object.entries(process.env)) {
    const upper = name.toUpperCase();
    if (upper.startsWith("ELIOT_")
        || upper.startsWith("SWARM_")
        || upper.includes("CAPTURE")) {
      continue;
    }
    env[name] = value;
  }
  if (controlDir) env.ELIOT_COMMAND_CONTROL_DIR = controlDir;
  return env;
}

export function controlRecordRef(operationId) {
  return `command-control:${operationDigest(operationId)}`;
}

export function resultRecordRef(operationId) {
  return `command-result:${operationDigest(operationId)}`;
}

function artifactRefs(operationId) {
  return [
    { kind: "command_control_record", ref: controlRecordRef(operationId) },
    { kind: "command_result_record", ref: resultRecordRef(operationId) },
  ];
}

function resultExitAgrees(result, exit) {
  if (!result || exit?.signal != null || typeof exit?.code !== "number") return false;
  if (result.subtype === "success") return exit.code === 0;
  if (result.subtype === "max_turns") return exit.code === 8;
  return result.subtype === "error"
    && exit.code !== 0
    && exit.code !== 8
    && Object.hasOwn(EXIT_MEANINGS, exit.code);
}

export function outcomeFromRun(run, evidence = run?.evidence_validation) {
  const subtype = run?.result?.subtype ?? null;
  const exitCode = run?.exit?.code ?? null;
  const terminalExitAgrees = resultExitAgrees(run?.result, run?.exit);
  const cleanEvidence = evidence?.valid === true
    && terminalExitAgrees
    && (run?.events?.gaps?.length ?? 0) === 0
    && (run?.anomalies?.length ?? 0) === 0
    && run?.timed_out !== true;
  const applied = cleanEvidence && subtype === "success";
  const rejected = cleanEvidence && (subtype === "error" || subtype === "max_turns");
  // A validated native error already carries a bounded, allowlisted vendor
  // category in the persisted exit projection. Preserve that category in the
  // manager-readable Operation receipt; raw result.error remains local evidence.
  const nativeFailureCode = cleanEvidence && subtype === "error"
    ? (Object.values(EXIT_MEANINGS).includes(run?.exit?.meaning)
      ? run.exit.meaning
      : "NATIVE_RESULT_ERROR")
    : null;
  const resultText = run?.result?.final_text;
  return {
    outcome: applied ? "applied" : rejected ? "rejected" : "unknown",
    details: {
      execution_shape: "sessionless_batch",
      batch_run_id: run?.batch_run_id ?? null,
      ...(cleanEvidence ? { completion_condition: "native_result_observed" } : {}),
      requested_model: run?.requested_model ?? null,
      native_request_model: run?.native_request_model ?? null,
      native_request_model_status: run?.native_request_model_status ?? "unknown",
      native_request_model_evidence: Array.isArray(run?.native_request_model_evidence)
        ? run.native_request_model_evidence
        : [],
      effective_model: null,
      effective_model_status: "unknown",
      result_subtype: subtype,
      exit_code: exitCode,
      signal: run?.exit?.signal ?? null,
      anomalies: Array.isArray(run?.anomalies) ? run.anomalies : ["run_record_missing"],
      native_session_id: run?.session_id ?? null,
      prompt_sha256: run?.prompt_sha256 ?? null,
      prompt_bytes: run?.prompt_bytes ?? null,
      spawn_error_observed: run?.spawn_error_observed === true,
      timed_out: run?.timed_out === true,
      control_record_ref: run?.control_record_ref ?? null,
      result_ref: run?.result_ref ?? null,
      artifact_refs: Array.isArray(run?.artifact_refs) ? run.artifact_refs : [],
      result_text_sha256: typeof resultText === "string" ? sha256Hex(resultText) : null,
      result_text_bytes: typeof resultText === "string" ? Buffer.byteLength(resultText, "utf8") : null,
      ...(typeof run?.task_prompt_contract_revision === "string" ? {
        task_prompt_contract_revision: run.task_prompt_contract_revision,
        prompt_contract_revision: run.task_prompt_contract_revision,
        task_prompt: run.task_prompt,
        task_dispatch_context: run.task_dispatch_context,
      } : {}),
      ...(applied && run?.dispatch_admission ? { dispatch_admission: run.dispatch_admission } : {}),
      ...(nativeFailureCode ? { diagnostic_code: nativeFailureCode } : {}),
      ...(!cleanEvidence ? {
        diagnostic_code: evidence?.diagnostic_code
          ?? run?.anomalies?.[0]
          ?? "NATIVE_RESULT_NOT_VALIDATED",
      } : {}),
    },
  };
}

function createAdmission(options) {
  const coreBinding = options.coreBinding ?? commandReceiptFacts(options.operationId, options.prompt);
  const expectedCoreBinding = commandReceiptFacts(options.operationId, options.prompt);
  if (!coreBindingMatches(coreBinding, expectedCoreBinding)) {
    throw new Error("CORE_PROMPT_BINDING_MISMATCH");
  }
  const validated = validateTaskPromptDispatch(options.command, options.bootId, options.moduleContract);
  if (validated.operationId !== options.operationId
      || validated.prompt !== options.prompt
      || !isDeepStrictEqual(validated.coreBinding, coreBinding)) {
    throw new Error("TASK_PROMPT_DISPATCH_CHANGED");
  }
  return {
    schema: 3,
    module_artifact_id: MODULE_ARTIFACT_ID,
    operation_id: options.operationId,
    execution_shape: "sessionless_batch",
    batch_run_id: coreBinding.batch_run_id,
    requested_model: options.requestedModel,
    prompt_sha256: coreBinding.prompt_sha256,
    prompt_bytes: coreBinding.prompt_bytes,
    core_binding: coreBinding,
    task_prompt_contract_revision: TASK_PROMPT_CONTRACT_REVISION,
    prompt_contract_revision: TASK_PROMPT_CONTRACT_REVISION,
    task_prompt: validated.taskPrompt,
    task_dispatch_context: validated.taskDispatchContext,
    dispatch_admission: validated.dispatchAdmission,
    control_record_ref: controlRecordRef(options.operationId),
    result_ref: resultRecordRef(options.operationId),
    artifact_refs: artifactRefs(options.operationId),
    admitted_at: new Date().toISOString(),
  };
}

function readAdmission(controlDir) {
  const path = join(controlDir, "admission.json");
  return existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : null;
}

function persistAdmission(controlDir, admission) {
  const path = join(controlDir, "admission.json");
  writeFileSync(path, JSON.stringify(admission, null, 2) + "\n", {
    flag: "wx",
    encoding: "utf8",
  });
}

// Classify one NDJSON line from the headless stream. Current event frames wrap
// one AgentEvent in `{type:"event", event}`; direct AgentEvent lines remain
// readable for older fixture/runtime evidence. Keep the exact original line
// beside the semantic projection so persisted events retain native evidence.
export function classifyLine(line) {
  let value;
  try {
    value = JSON.parse(line);
  } catch {
    return { kind: "gap", reason: "unparseable_line", raw: line };
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return { kind: "gap", reason: "non_object_line", raw: line };
  }
  if (typeof value.subtype === "string" && RESULT_SUBTYPES.has(value.subtype)) {
    return { kind: "result", result: value, raw_line: line, frame_format: "result_line" };
  }
  if (value.type === "result") {
    return { kind: "gap", reason: "unknown_result_subtype", raw: line };
  }
  if (value.type === "event") {
    if (value.event === null || typeof value.event !== "object" || Array.isArray(value.event)
        || typeof value.event.type !== "string") {
      return { kind: "gap", reason: "invalid_event_frame", raw: line };
    }
    return {
      kind: "event",
      event: value.event,
      raw_line: line,
      frame_format: "event_envelope",
    };
  }
  if (typeof value.type === "string") {
    return {
      kind: "event",
      event: value,
      raw_line: line,
      frame_format: "direct_event_legacy",
    };
  }
  return { kind: "gap", reason: "unknown_shape", raw: line };
}

function readJsonLines(path) {
  if (!existsSync(path)) {
    return [];
  }
  const records = [];
  for (const line of readFileSync(path, "utf8").split("\n")) {
    if (line.trim() === "") continue;
    try {
      records.push(JSON.parse(line));
    } catch {
      records.push({ kind: "gap", reason: "unparseable_line", raw: line });
    }
  }
  return records;
}

export function readModJournal(controlDir, { legacy = false } = {}) {
  const journalPath = legacy
    ? join(controlDir, "mod-journal.ndjson")
    : join(controlDir, "mod", "mod-journal.ndjson");
  const records = readJsonLines(journalPath);
  return {
    records,
    loaded: records.some((r) => r.kind === "mod_loaded"),
    ready: records.some((r) => r.kind === "mod_ready"),
    sessionEnded: records.some((r) => r.kind === "mod_session_end"),
    localErrors: records.filter((r) => r.kind === "mod_local_error"),
    queueAdmissions: records.filter((r) => r.kind === "queue_admitted"),
    toolsReadbacks: records.filter((r) => r.kind === "tools_readback"),
  };
}

function projectNativeResult(result) {
  if (!result || !RESULT_SUBTYPES.has(result.subtype)) return null;
  const finalText = result.finalText ?? null;
  return {
    subtype: result.subtype,
    stop_reason: result.stopReason ?? null,
    duration_ms: result.durationMs ?? null,
    usage: result.usage ?? null,
    final_text: finalText,
    final_text_sha256: typeof finalText === "string" ? sha256Hex(finalText) : null,
    final_text_bytes: typeof finalText === "string" ? Buffer.byteLength(finalText, "utf8") : null,
    error: result.error ?? null,
  };
}

function eventSummary(records) {
  const eventRecords = records.filter((record) => record?.kind === "event");
  const events = eventRecords.map((record) => record.event);
  const gaps = records.filter((record) => record?.kind === "gap");
  const byType = {};
  for (const event of events) {
    if (typeof event?.type !== "string") continue;
    byType[event.type] = (byType[event.type] ?? 0) + 1;
  }
  return { events, gaps, byType, nativeRequestModel: nativeRequestModelProjection(eventRecords) };
}

function nativeRequestModelProjection(eventRecords) {
  const evidence = eventRecords
    .filter((record) => record.event?.type === "model_request_start"
      || record.event?.type === "model_request_end")
    .filter((record) => typeof record.event.model === "string" && record.event.model.trim() !== "")
    .map((record) => ({
      seq: record.seq,
      event_type: record.event.type,
      model: record.event.model,
    }));
  const models = [...new Set(evidence.map((item) => item.model))];
  return {
    model: models.length === 1 ? models[0] : null,
    status: models.length === 1 ? "observed" : models.length > 1 ? "conflicting" : "unknown",
    evidence,
  };
}

function legacyEventProjection(records) {
  const projectedRecords = records.map((record) => {
    if (record?.kind !== "event") return record;
    const storedEvent = record.event;
    const wrapped = storedEvent?.type === "event"
      && storedEvent.event !== null
      && typeof storedEvent.event === "object"
      && !Array.isArray(storedEvent.event)
      && typeof storedEvent.event.type === "string";
    return {
      ...record,
      event: wrapped ? storedEvent.event : storedEvent,
      stored_frame_format: wrapped ? "event_envelope_parsed" : "direct_event_legacy",
      stored_raw_frame: storedEvent,
    };
  });
  const eventRecords = projectedRecords.filter((record) => record?.kind === "event");
  const summary = eventSummary(projectedRecords);
  return {
    source_artifact_id: PREVIOUS_MODULE_ARTIFACT_ID,
    read_only: true,
    raw_line_available: false,
    total: summary.events.length,
    by_type: summary.byType,
    events: eventRecords.map(({ seq, event, stored_frame_format, stored_raw_frame }) => ({
      seq,
      event,
      frame_format: stored_frame_format,
      stored_frame: stored_raw_frame,
    })),
    gaps: summary.gaps,
    native_request_model: summary.nativeRequestModel.model,
    native_request_model_status: summary.nativeRequestModel.status,
    native_request_model_evidence: summary.nativeRequestModel.evidence,
  };
}

function rawNativeFrameMatches(record) {
  if (typeof record?.raw_line !== "string") return false;
  const classified = classifyLine(record.raw_line);
  return classified.kind === record.kind
    && classified.frame_format === record.frame_format
    && (record.kind === "event"
      ? isDeepStrictEqual(classified.event, record.event)
      : record.kind === "result" && isDeepStrictEqual(classified.result, record.result));
}

function expectedTerminalFacts(result, exit, timedOut, records) {
  const resultIndex = records.findIndex((record) => record?.kind === "result");
  const framesAfterResult = resultIndex < 0
    ? 0
    : records.slice(resultIndex + 1).filter((record) => record?.kind === "event").length;
  const gaps = records.filter((record) => record?.kind === "gap");
  const anomalies = [];
  if (framesAfterResult > 0) anomalies.push("frames_after_result_line");
  if (!result) anomalies.push("native_result_missing");
  if (result?.subtype === "success" && exit?.code !== 0) anomalies.push("exit_result_mismatch");
  if (timedOut) anomalies.push("glue_timeout_killed_owned_child");
  if (exit?.spawn_error != null) anomalies.push("native_spawn_failed");
  if (gaps.length > 0) anomalies.push("native_stream_protocol_gaps");

  let disposition = "unknown";
  let dispositionBasis = "missing_result_line";
  if (result) {
    dispositionBasis = "result_line";
    const terminalValidated = resultExitAgrees(result, exit)
      && !timedOut
      && exit?.spawn_error == null
      && framesAfterResult === 0
      && gaps.length === 0;
    if (terminalValidated) {
      if (result.subtype === "success") disposition = "completed";
      else if (result.subtype === "error") disposition = "failed";
      else if (result.subtype === "max_turns") disposition = "max_turns";
    } else {
      dispositionBasis = "result_exit_or_stream_mismatch";
    }
  }
  return { anomalies, disposition, dispositionBasis, framesAfterResult };
}

function validSavedDispatchAdmission(receipt, context, taskPrompt, operationId) {
  if (!exactFields(receipt, [
    "schema_version", "module_receipt", "operation_id", "binding_id",
    "binding_generation", "worker_boot_id", "attempt_id", "task_id",
    "task_revision", "task_snapshot_sha256", "source_text_sha256",
    "source_text_bytes", "native_payload_sha256", "native_payload_bytes",
    "native_input_id",
  ])) return false;
  const moduleReceipt = receipt.module_receipt;
  if (!exactFields(moduleReceipt, [
    "schema_version", "module_id", "artifact", "protocol", "binding_id",
    "binding_generation", "operation_id", "input_sha256",
  ]) || !exactFields(moduleReceipt.artifact, ["artifact_id", "version"])
      || !exactFields(moduleReceipt.protocol, ["major", "minor"])) return false;
  return receipt.schema_version === 1
    && receipt.operation_id === operationId
    && receipt.binding_id === context.binding_id
    && receipt.binding_generation === context.binding_generation
    && receipt.worker_boot_id === context.worker_boot_id
    && receipt.attempt_id === context.attempt_id
    && receipt.task_id === context.task_id
    && receipt.task_revision === context.task_revision
    && receipt.task_snapshot_sha256 === context.task_snapshot_sha256
    && receipt.source_text_sha256 === context.source_text_sha256
    && receipt.source_text_bytes === context.source_text_bytes
    && receipt.native_payload_sha256 === taskPrompt.prompt_sha256
    && receipt.native_payload_bytes === taskPrompt.prompt_bytes
    && receipt.native_input_id === null
    && moduleReceipt.schema_version === 1
    && moduleReceipt.module_id === "runtime.command"
    && moduleReceipt.artifact.artifact_id === MODULE_ARTIFACT_ID
    && moduleReceipt.artifact.version === "5"
    && moduleReceipt.protocol.major === 1
    && moduleReceipt.protocol.minor === 0
    && moduleReceipt.binding_id === context.binding_id
    && moduleReceipt.binding_generation === context.binding_generation
    && moduleReceipt.operation_id === operationId
    && LOWER_SHA256.test(moduleReceipt.input_sha256 ?? "");
}

function validateSavedEvidence(controlDir, admission, run, records, contract) {
  const invalid = (diagnostic_code) => ({ valid: false, diagnostic_code });
  if (!admission) return invalid("saved_admission_missing");
  if (admission.schema === 1 && run?.module_artifact_id === LEGACY_MODULE_ARTIFACT_ID) {
    return { valid: false, diagnostic_code: "legacy_artifact_read_only" };
  }
  if (!run) return invalid("saved_terminal_evidence_missing");
  if (admission.schema !== contract.schema || admission.module_artifact_id !== contract.artifactId
      || run.schema !== contract.schema || run.module_artifact_id !== contract.artifactId) {
    return invalid("saved_artifact_identity_mismatch");
  }
  const operationId = admission.operation_id;
  const expectedArtifacts = artifactRefs(operationId);
  if (typeof operationId !== "string" || operationId.trim() === ""
      || admission.execution_shape !== "sessionless_batch"
      || admission.batch_run_id !== batchRunId(operationId)
      || admission.control_record_ref !== controlRecordRef(operationId)
      || admission.result_ref !== resultRecordRef(operationId)
      || JSON.stringify(admission.artifact_refs) !== JSON.stringify(expectedArtifacts)
      || admission.core_binding?.batch_run_id !== admission.batch_run_id
      || admission.core_binding?.prompt_sha256 !== admission.prompt_sha256
      || admission.core_binding?.prompt_bytes !== admission.prompt_bytes
      || !/^[0-9a-f]{64}$/.test(admission.prompt_sha256 ?? "")
      || !Number.isSafeInteger(admission.prompt_bytes)
      || admission.prompt_bytes < 1) {
    return invalid("saved_admission_identity_mismatch");
  }
  if (contract.taskPrompt) {
    const taskPrompt = admission.task_prompt;
    const context = admission.task_dispatch_context;
    if (admission.task_prompt_contract_revision !== TASK_PROMPT_CONTRACT_REVISION
        || !exactFields(taskPrompt, [
          "schema_id", "schema_version", "task_id", "task_revision", "attempt_id",
          "task_snapshot_sha256", "prompt_sha256", "prompt_bytes",
        ])
        || taskPrompt.schema_id !== "swarm.task_prompt"
        || taskPrompt.schema_version !== 1
        || !validBoundedTaskIdentity(taskPrompt.task_id)
        || !Number.isSafeInteger(taskPrompt.task_revision) || taskPrompt.task_revision <= 0
        || !validBoundedTaskIdentity(taskPrompt.attempt_id)
        || !LOWER_SHA256.test(taskPrompt.task_snapshot_sha256 ?? "")
        || taskPrompt.prompt_sha256 !== admission.prompt_sha256
        || taskPrompt.prompt_bytes !== admission.prompt_bytes
        || !exactFields(context, TASK_DISPATCH_CONTEXT_FIELDS)
        || context.schema_version !== 1
        || context.operation_id !== operationId
        || typeof context.binding_id !== "string" || context.binding_id.trim() === ""
        || !Number.isSafeInteger(context.binding_generation) || context.binding_generation <= 0
        || typeof context.worker_boot_id !== "string" || context.worker_boot_id.trim() === ""
        || context.task_id !== taskPrompt.task_id
        || context.task_revision !== taskPrompt.task_revision
        || context.attempt_id !== taskPrompt.attempt_id
        || context.task_snapshot_sha256 !== taskPrompt.task_snapshot_sha256
        || !LOWER_SHA256.test(context.source_text_sha256 ?? "")
        || !Number.isSafeInteger(context.source_text_bytes) || context.source_text_bytes < 0
        || admission.prompt_contract_revision !== TASK_PROMPT_CONTRACT_REVISION
        || !validSavedDispatchAdmission(admission.dispatch_admission, context, taskPrompt, operationId)) {
      return invalid("saved_task_prompt_identity_mismatch");
    }
  }
  if (run.runtime !== RUNTIME || run.entrypoint !== ENTRYPOINT || run.transport !== TRANSPORT
      || run.execution_shape !== "sessionless_batch"
      || run.operation_id !== operationId
      || run.batch_run_id !== admission.batch_run_id
      || run.requested_model !== admission.requested_model
      || run.prompt_sha256 !== admission.prompt_sha256
      || run.prompt_bytes !== admission.prompt_bytes
      || !isDeepStrictEqual(run.core_binding, admission.core_binding)
      || run.control_record_ref !== admission.control_record_ref
      || run.result_ref !== admission.result_ref
      || JSON.stringify(run.artifact_refs) !== JSON.stringify(expectedArtifacts)
      || (contract.taskPrompt && (
        run.task_prompt_contract_revision !== admission.task_prompt_contract_revision
        || run.prompt_contract_revision !== admission.prompt_contract_revision
        || !isDeepStrictEqual(run.task_prompt, admission.task_prompt)
        || !isDeepStrictEqual(run.task_dispatch_context, admission.task_dispatch_context)
        || !isDeepStrictEqual(run.dispatch_admission, admission.dispatch_admission)
      ))
      || resolve(run.control_dir ?? "") !== resolve(controlDir)) {
    return invalid("saved_run_identity_mismatch");
  }
  if (!Array.isArray(records) || records.some((record, index) =>
    !record || typeof record !== "object" || record.seq !== index + 1
      || !["event", "result", "gap"].includes(record.kind))) {
    return invalid("saved_event_sequence_invalid");
  }
  const summary = eventSummary(records);
  const resultRecords = records.filter((record) => record.kind === "result");
  if (resultRecords.length > 1
      || (resultRecords.length === 1 && records.at(-1) !== resultRecords[0])) {
    return invalid("saved_result_frame_order_invalid");
  }
  if (summary.events.some((event) => !event || typeof event !== "object"
      || Array.isArray(event) || typeof event.type !== "string")) {
    return invalid("saved_event_frame_invalid");
  }
  if (records.some((record) => (record.kind === "event" || record.kind === "result")
      && !rawNativeFrameMatches(record))) {
    return invalid("saved_native_frame_projection_mismatch");
  }
  const rawResult = resultRecords[0]?.result ?? null;
  if (rawResult && (!projectNativeResult(rawResult)
      || !isDeepStrictEqual(run.result, projectNativeResult(rawResult)))) {
    return invalid("saved_result_projection_mismatch");
  }
  if (!rawResult && run.result !== null) return invalid("saved_result_frame_missing");
  if (!Number.isSafeInteger(run.events?.total) || run.events.total !== summary.events.length
      || !isDeepStrictEqual(run.events?.by_type, summary.byType)
      || !isDeepStrictEqual(run.events?.gaps, summary.gaps)) {
    return invalid("saved_event_summary_mismatch");
  }
  if (run.native_request_model !== summary.nativeRequestModel.model
      || run.native_request_model_status !== summary.nativeRequestModel.status
      || !isDeepStrictEqual(run.native_request_model_evidence, summary.nativeRequestModel.evidence)) {
    return invalid("saved_native_request_model_mismatch");
  }
  const exit = run.exit;
  if (!exit || !["number", "object"].includes(typeof exit.code)
      || (exit.code !== null && (!Number.isInteger(exit.code) || exit.code < 0))
      || !(exit.signal === null || typeof exit.signal === "string")
      || !(exit.spawn_error === null || typeof exit.spawn_error === "string")
      || (typeof run.timed_out !== "boolean")
      || run.spawn_error_observed !== (exit.spawn_error !== null)
      || exit.meaning !== (exit.code !== null ? (EXIT_MEANINGS[exit.code] ?? "UNLISTED_EXIT_CODE") : null)) {
    return invalid("saved_process_exit_facts_invalid");
  }
  const terminal = expectedTerminalFacts(rawResult, exit, run.timed_out, records);
  if (!isDeepStrictEqual(run.anomalies, terminal.anomalies)
      || run.disposition !== terminal.disposition
      || run.disposition_basis !== terminal.dispositionBasis) {
    return invalid("saved_terminal_projection_mismatch");
  }
  const sessionFromEvent = summary.events.find(
    (event) => event.type === "run_start" && typeof event.sessionId === "string",
  )?.sessionId ?? null;
  const expectedSessionId = typeof rawResult?.sessionId === "string"
    ? rawResult.sessionId
    : sessionFromEvent;
  const expectedSessionSource = typeof rawResult?.sessionId === "string"
    ? "result_line"
    : sessionFromEvent ? "run_start_event" : "none";
  if (run.session_id !== expectedSessionId || run.session_id_source !== expectedSessionSource) {
    return invalid("saved_session_evidence_mismatch");
  }
  return { valid: true, diagnostic_code: null };
}

function modErrorEvents(events) {
  return events.filter((e) => e.type === "mod_error");
}

export async function describe(config) {
  const executable = config.command;
  let executablePresent = false;
  let executableNote = "resolved via PATH at spawn time";
  if (executable.includes("/") || executable.includes("\\")) {
    try {
      accessSync(executable, constants.X_OK);
      executablePresent = true;
      executableNote = "absolute path exists and is executable";
    } catch {
      executableNote = "absolute path missing or not executable";
    }
  }
  let version = null;
  let versionNote = "not probed";
  if (executablePresent || !executable.includes("/")) {
    const probe = await runProbe(executable, config.commandArgs ?? []);
    version = probe.version;
    versionNote = probe.note;
  }
  let modSha256 = null;
  const modPath = config.modPath ?? DEFAULT_MOD_PATH;
  if (existsSync(modPath)) {
    // The vendor pin is the canonical LF text digest from Git; Windows
    // checkouts may materialize the same tracked source with CRLF endings.
    const modText = readFileSync(modPath, "utf8").replace(/\r\n/g, "\n");
    modSha256 = sha256Hex(modText);
  }
  return {
    runtime: RUNTIME,
    entrypoint: ENTRYPOINT,
    transport: TRANSPORT,
    module_artifact_id: config.moduleArtifactId ?? MODULE_ARTIFACT_ID,
    executable: { path: executable, present: executablePresent, note: executableNote },
    cli_version: version,
    cli_version_note: versionNote,
    mod: { path: modPath, sha256: modSha256 },
    installed_runtime_verified: false,
    capabilities: {
      describe: "implemented",
      open: "executor_preflight_only_no_native_session",
      task_dispatch: "one_shot_sessionless_batch",
      reconcile: "saved_evidence_readback_only",
      snapshot: "saved_run_readback",
      send_next_turn: "unavailable_sessionless_batch",
      configure_model: "unavailable",
      configure_effort: "unavailable",
      goal: "unavailable",
      resume: "unavailable",
      attach: "unavailable",
      steer: "unavailable",
      reply: "unavailable",
      result_pages: "unavailable",
    },
  };
}

function runProbe(executable, argsPrefix) {
  return new Promise((resolveProbe) => {
    let settled = false;
    const done = (version, note) => {
      if (!settled) {
        settled = true;
        resolveProbe({ version, note });
      }
    };
    let child;
    try {
      child = spawn(executable, [...argsPrefix, "--version"], {
        env: nativeEnvironment(),
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch (error) {
      done(null, `spawn failed: ${error.message}`);
      return;
    }
    let out = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      done(null, "version probe timed out");
    }, 5000);
    child.stdout.on("data", (chunk) => {
      out += chunk.toString("utf8");
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      done(null, `version probe failed: ${error.message}`);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      const first = out.split("\n").map((s) => s.trim()).find((s) => s !== "");
      if (code === 0 && first) {
        done(first, "from --version");
      } else {
        done(null, `version probe exited ${code} without a version line`);
      }
    });
  });
}

// Open one headless run: spawn the native executable with the pinned mod,
// stream its NDJSON, and persist a run record in the control directory.
// `open` returns the same record `snapshot` later reads back from disk.
export async function openRun(config, options) {
  const controlDir = resolve(options.controlDir);
  const runPath = join(controlDir, "run.json");
  const eventsPath = join(controlDir, "events.ndjson");
  const operationId = options.operationId;
  const requestedModel = options.requestedModel;
  if (typeof operationId !== "string" || operationId.trim() === "") {
    throw new Error("OPERATION_ID_REQUIRED");
  }
  if (typeof requestedModel !== "string" || requestedModel.trim() === "" || requestedModel !== requestedModel.trim()) {
    throw new Error("REQUESTED_MODEL_REQUIRED");
  }
  if (typeof options.prompt !== "string" || options.prompt.length === 0) {
    throw new Error("PROMPT_REQUIRED");
  }
  const prefix = config.commandArgs ?? [];
  if (!Array.isArray(prefix) || prefix.some((arg) => typeof arg !== "string")) {
    throw new Error("INVALID_ARGS_PREFIX");
  }
  if (prefix.some((arg) =>
    ["-p", "--print", "--output-format", "--mod", "--model", "--resume", "-r", "--continue", "-c"].includes(arg)
      || arg.startsWith("--model=")
      || arg.startsWith("--mod=")
      || arg.startsWith("--output-format="))) {
    throw new Error("ARGS_PREFIX_CONFLICTS_WITH_GLUE_OWNED_FLAGS");
  }
  const expectedAdmission = createAdmission(options);
  mkdirSync(controlDir, { recursive: true });
  const priorAdmission = readAdmission(controlDir);
  if (priorAdmission) {
    if (
      priorAdmission.schema !== 3
      || priorAdmission.module_artifact_id !== MODULE_ARTIFACT_ID
      || priorAdmission.operation_id !== operationId
      || priorAdmission.batch_run_id !== expectedAdmission.batch_run_id
      || priorAdmission.requested_model !== requestedModel
      || priorAdmission.prompt_sha256 !== expectedAdmission.prompt_sha256
      || priorAdmission.prompt_bytes !== expectedAdmission.prompt_bytes
      || !isDeepStrictEqual(priorAdmission.core_binding, expectedAdmission.core_binding)
      || priorAdmission.task_prompt_contract_revision !== expectedAdmission.task_prompt_contract_revision
        || priorAdmission.prompt_contract_revision !== expectedAdmission.prompt_contract_revision
      || !isDeepStrictEqual(priorAdmission.task_prompt, expectedAdmission.task_prompt)
      || !isDeepStrictEqual(priorAdmission.task_dispatch_context, expectedAdmission.task_dispatch_context)
        || !isDeepStrictEqual(priorAdmission.dispatch_admission, expectedAdmission.dispatch_admission)
      || priorAdmission.control_record_ref !== expectedAdmission.control_record_ref
      || priorAdmission.result_ref !== expectedAdmission.result_ref
      || JSON.stringify(priorAdmission.artifact_refs) !== JSON.stringify(expectedAdmission.artifact_refs)
    ) {
      throw new Error("OPERATION_ID_CONFLICT");
    }
    const snapshot = snapshotRun(controlDir);
    if (snapshot.run) {
      return {
        ...snapshot.run,
        evidence_validation: snapshot.evidence,
        replayed_from_saved_evidence: true,
      };
    }
    return {
      schema: 3,
      runtime: RUNTIME,
      entrypoint: ENTRYPOINT,
      transport: TRANSPORT,
      module_artifact_id: MODULE_ARTIFACT_ID,
      operation_id: operationId,
      batch_run_id: priorAdmission.batch_run_id,
      requested_model: requestedModel,
      native_request_model: null,
      native_request_model_status: "unknown",
      native_request_model_evidence: [],
      core_binding: priorAdmission.core_binding,
      effective_model: null,
      effective_model_status: "unknown",
      control_dir: controlDir,
      control_record_ref: priorAdmission.control_record_ref,
      result_ref: priorAdmission.result_ref,
      artifact_refs: artifactRefs(operationId),
      prompt_sha256: priorAdmission.prompt_sha256,
      prompt_bytes: priorAdmission.prompt_bytes,
      task_prompt_contract_revision: priorAdmission.task_prompt_contract_revision,
      prompt_contract_revision: priorAdmission.prompt_contract_revision,
      task_prompt: priorAdmission.task_prompt,
      task_dispatch_context: priorAdmission.task_dispatch_context,
      dispatch_admission: priorAdmission.dispatch_admission,
      result: null,
      exit: { code: null, signal: null, spawn_error: null, meaning: null },
      spawn_error_observed: false,
      timed_out: false,
      disposition: "unknown",
      disposition_basis: "admission_without_terminal_record",
      anomalies: ["native_result_missing_after_admission"],
      events: { total: 0, by_type: {}, gaps: [] },
      evidence_validation: { valid: false, diagnostic_code: "saved_terminal_evidence_missing" },
      replayed_from_saved_evidence: true,
    };
  }
  if (existsSync(runPath) || existsSync(eventsPath)) {
    throw new Error("CONTROL_RECORD_WITHOUT_ADMISSION");
  }
  const cwd = options.cwd ?? config.cwd;
  if (typeof cwd !== "string" || !isAbsolute(cwd)) {
    throw new Error("WORKSPACE_ROOT_MUST_BE_ABSOLUTE");
  }
  persistAdmission(controlDir, expectedAdmission);
  const modPath = config.modPath ?? DEFAULT_MOD_PATH;
  const args = [
    ...prefix,
    "-p",
    "--output-format",
    "json",
    "--model",
    requestedModel,
    "--mod",
    modPath,
    options.prompt,
  ];
  const startedAt = new Date().toISOString();
  const modControlDir = join(controlDir, "mod");
  mkdirSync(modControlDir, { recursive: true });
  // Prepare every durable journal path before starting the owned child. A
  // local file error must not leave an untracked native process running.
  writeFileSync(eventsPath, "", "utf8");
  const child = spawn(config.command, args, {
    cwd,
    env: nativeEnvironment(modControlDir),
    stdio: ["ignore", "pipe", "pipe"],
  });

  const events = [];
  const eventRecords = [];
  const gaps = [];
  let result = null;
  let resultLineIndex = -1;
  let lineIndex = 0;
  let framesAfterResult = 0;
  let stderrText = "";
  let stderrTruncated = false;

  child.stderr.on("data", (chunk) => {
    if (stderrText.length < STDERR_LIMIT_BYTES) {
      stderrText += chunk.toString("utf8");
      if (stderrText.length > STDERR_LIMIT_BYTES) {
        stderrText = stderrText.slice(0, STDERR_LIMIT_BYTES);
        stderrTruncated = true;
      }
    } else {
      stderrTruncated = true;
    }
  });

  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  const linesDone = new Promise((r) => lines.on("close", r));
  lines.on("line", (line) => {
    if (line.trim() === "") return;
    let classified = classifyLine(line);
    if (classified.kind === "result" && result !== null) {
      classified = { kind: "gap", reason: "duplicate_result_line", raw: line };
    }
    const record = { seq: lineIndex + 1, ...classified };
    if (classified.kind === "event") {
      events.push(classified.event);
      eventRecords.push(record);
      if (resultLineIndex >= 0) framesAfterResult += 1;
    } else if (classified.kind === "result") {
      result = classified.result;
      resultLineIndex = lineIndex;
    } else {
      gaps.push(record);
    }
    writeFileSync(eventsPath, JSON.stringify(record) + "\n", {
      flag: "a",
      encoding: "utf8",
    });
    lineIndex += 1;
  });

  const timeoutMs = options.timeoutMs ?? 0;
  let timedOut = false;
  const exit = await new Promise((resolveExit) => {
    let timer = null;
    let escalationTimer = null;
    let runSettled = false;
    let childExited = false;
    let childClosed = false;
    const clearTimers = () => {
      if (timer) clearTimeout(timer);
      if (escalationTimer) clearTimeout(escalationTimer);
      timer = null;
      escalationTimer = null;
    };
    const settle = (result) => {
      if (runSettled) return;
      runSettled = true;
      clearTimers();
      resolveExit(result);
    };
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        if (runSettled || childExited || childClosed) return;
        timedOut = true;
        child.kill("SIGTERM");
        if (!runSettled && !childExited && !childClosed) {
          escalationTimer = setTimeout(() => {
            if (!runSettled && !childExited && !childClosed) {
              child.kill("SIGKILL");
            }
          }, 2000);
          escalationTimer.unref();
        }
      }, timeoutMs);
    }
    child.once("error", (error) => {
      settle({ code: null, signal: null, spawnError: error.message });
    });
    child.once("exit", () => {
      childExited = true;
      clearTimers();
    });
    child.once("close", (code, signal) => {
      childClosed = true;
      settle({ code, signal, spawnError: null });
    });
  });
  // The child is gone; wait until the line reader flushed the final line.
  await linesDone;

  const finishedAt = new Date().toISOString();
  const journal = readModJournal(controlDir);
  const streamModErrors = modErrorEvents(events);

  let disposition = "unknown";
  let dispositionBasis = "missing_result_line";
  const anomalies = [];
  if (framesAfterResult > 0) anomalies.push("frames_after_result_line");
  if (!result) anomalies.push("native_result_missing");
  if (result && result.subtype === "success" && exit.code !== 0) {
    anomalies.push("exit_result_mismatch");
  }
  if (timedOut) anomalies.push("glue_timeout_killed_owned_child");
  if (exit.spawnError) anomalies.push("native_spawn_failed");
  if (gaps.length > 0) anomalies.push("native_stream_protocol_gaps");
  if (result) {
    dispositionBasis = "result_line";
    const terminalValidated = resultExitAgrees(result, exit)
      && !timedOut
      && exit.spawnError === null
      && framesAfterResult === 0
      && gaps.length === 0;
    if (terminalValidated) {
      if (result.subtype === "success") disposition = "completed";
      else if (result.subtype === "error") disposition = "failed";
      else if (result.subtype === "max_turns") disposition = "max_turns";
    } else {
      dispositionBasis = "result_exit_or_stream_mismatch";
    }
  }

  const summary = eventSummary(eventRecords);
  const nativeRequestModel = summary.nativeRequestModel;
  const sessionFromEvent = events.find(
    (e) => e.type === "run_start" && typeof e.sessionId === "string",
  )?.sessionId;

  const record = {
    schema: 3,
    runtime: RUNTIME,
    entrypoint: ENTRYPOINT,
    transport: TRANSPORT,
    module_artifact_id: MODULE_ARTIFACT_ID,
    execution_shape: "sessionless_batch",
    operation_id: operationId,
    batch_run_id: expectedAdmission.batch_run_id,
    requested_model: requestedModel,
    core_binding: expectedAdmission.core_binding,
    // Command's headless result/event contract has not yielded a documented
    // effective-model identity field; never infer one from the request.
    effective_model: null,
    effective_model_status: "unknown",
    native_request_model: nativeRequestModel.model,
    native_request_model_status: nativeRequestModel.status,
    native_request_model_evidence: nativeRequestModel.evidence,
    control_dir: controlDir,
    control_record_ref: expectedAdmission.control_record_ref,
    result_ref: expectedAdmission.result_ref,
    artifact_refs: artifactRefs(operationId),
    started_at: startedAt,
    finished_at: finishedAt,
    prompt_sha256: sha256Hex(options.prompt),
    prompt_bytes: Buffer.byteLength(options.prompt, "utf8"),
    prompt_length: options.prompt.length,
    task_prompt_contract_revision: expectedAdmission.task_prompt_contract_revision,
    prompt_contract_revision: expectedAdmission.prompt_contract_revision,
    task_prompt: expectedAdmission.task_prompt,
    task_dispatch_context: expectedAdmission.task_dispatch_context,
    dispatch_admission: expectedAdmission.dispatch_admission,
    session_id:
      typeof result?.sessionId === "string"
        ? result.sessionId
        : (sessionFromEvent ?? null),
    session_id_source:
      typeof result?.sessionId === "string"
        ? "result_line"
        : sessionFromEvent
          ? "run_start_event"
          : "none",
    result: result
      ? {
          subtype: result.subtype,
          stop_reason: result.stopReason ?? null,
          duration_ms: result.durationMs ?? null,
          usage: result.usage ?? null,
          final_text: result.finalText ?? null,
          final_text_sha256:
            typeof result.finalText === "string" ? sha256Hex(result.finalText) : null,
          final_text_bytes:
            typeof result.finalText === "string" ? Buffer.byteLength(result.finalText, "utf8") : null,
          error: result.error ?? null,
        }
      : null,
    exit: {
      code: exit.code,
      signal: exit.signal,
      spawn_error: exit.spawnError,
      meaning:
        exit.code !== null && exit.code !== undefined
          ? (EXIT_MEANINGS[exit.code] ?? "UNLISTED_EXIT_CODE")
          : null,
    },
    spawn_error_observed: exit.spawnError !== null,
    disposition,
    disposition_basis: dispositionBasis,
    timed_out: timedOut,
    anomalies,
    mod: {
      loaded: journal.loaded,
      ready: journal.ready,
      session_ended: journal.sessionEnded,
      stream_mod_errors: streamModErrors,
      local_errors: journal.localErrors,
      queue_admissions: journal.queueAdmissions,
      tools_readbacks: journal.toolsReadbacks,
    },
    events: { total: events.length, by_type: summary.byType, gaps },
    stderr: { text: stderrText, truncated: stderrTruncated },
  };
  const tmpPath = `${runPath}.tmp`;
  writeFileSync(tmpPath, JSON.stringify(record, null, 2) + "\n", "utf8");
  renameSync(tmpPath, runPath);
  const snapshot = snapshotRun(controlDir);
  return {
    ...record,
    evidence_validation: snapshot.evidence,
  };
}

// Snapshot of one control directory. Scope is a single headless run, so
// completeness stays `partial`: this is never a family or Task statement.
export function snapshotRun(controlDir) {
  const resolved = resolve(controlDir);
  const runPath = join(resolved, "run.json");
  const run = existsSync(runPath)
    ? JSON.parse(readFileSync(runPath, "utf8"))
    : null;
  const admission = readAdmission(resolved);
  const legacy = admission?.schema === 1 && run?.module_artifact_id === LEGACY_MODULE_ARTIFACT_ID;
  const previousArtifact = admission?.schema === 2
    && admission.module_artifact_id === PREVIOUS_MODULE_ARTIFACT_ID
    && run?.schema === 2
    && run.module_artifact_id === PREVIOUS_MODULE_ARTIFACT_ID;
  const historicalArtifact = admission?.schema === 2
    && admission.module_artifact_id === HISTORICAL_MODULE_ARTIFACT_ID;
  const journal = readModJournal(resolved, { legacy });
  const eventRecords = readJsonLines(join(resolved, "events.ndjson"));
  const evidence = legacy
    ? { valid: false, diagnostic_code: "legacy_artifact_read_only" }
    : previousArtifact
      ? { valid: false, diagnostic_code: "legacy_artifact_read_only" }
      : validateSavedEvidence(resolved, admission, run, eventRecords, historicalArtifact
        ? { schema: 2, artifactId: HISTORICAL_MODULE_ARTIFACT_ID, taskPrompt: false }
        : { schema: 3, artifactId: MODULE_ARTIFACT_ID, taskPrompt: true });
  return {
    scope: "single_headless_run",
    completeness: "partial",
    control_dir: resolved,
    admission,
    terminal: run ? (evidence.valid ? run.disposition : "unknown") : "not_observed",
    run,
    evidence,
    ...(previousArtifact ? { event_projection: legacyEventProjection(eventRecords) } : {}),
    mod: {
      loaded: journal.loaded,
      ready: journal.ready,
      session_ended: journal.sessionEnded,
      records: journal.records.length,
      local_errors: journal.localErrors,
      queue_admissions: journal.queueAdmissions,
      tools_readbacks: journal.toolsReadbacks,
    },
    events_recorded: eventRecords.length,
  };
}

function loadConfig(path) {
  const parsed = JSON.parse(readFileSync(path, "utf8"));
  if (typeof parsed.command !== "string" || parsed.command === "") {
    throw new Error("module config requires a non-empty `command`");
  }
  return parsed;
}

async function main(argv) {
  const [command, ...rest] = argv;
  const flag = (name) => {
    const idx = rest.indexOf(name);
    return idx >= 0 ? rest[idx + 1] : undefined;
  };
  if (command === "describe") {
    const config = loadConfig(flag("--config"));
    process.stdout.write(JSON.stringify(await describe(config), null, 2) + "\n");
    return;
  }
  if (command === "open") {
    throw new Error("STORE_TASK_PROMPT_REQUIRED: dispatch is admitted only by the authenticated module bridge");
  }
  if (command === "snapshot") {
    const controlDir = flag("--control-dir");
    if (!controlDir) throw new Error("snapshot requires --control-dir");
    process.stdout.write(JSON.stringify(snapshotRun(controlDir), null, 2) + "\n");
    return;
  }
  throw new Error(
    "usage: glue.mjs describe --config FILE | snapshot --control-dir DIR",
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`glue: ${error.message}\n`);
    process.exitCode = 1;
  });
}
