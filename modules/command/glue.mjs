// Eliot Command Code glue — the host-side unit of modules/command.
//
// Drives the installed Command Code CLI (`cmd`) in its documented headless
// print mode (`-p --output-format json`) with the pinned Eliot mod loaded via
// `--mod`, and reduces the run to the module contract's describe / open /
// snapshot facts. bridge.mjs owns module IPC and calls these functions.
//
// Evidence rules, from the architecture and runtime notes:
// - The terminal fact of a run is the final NDJSON result line, corroborated
//   by the process exit code. Process exit alone never proves success, and a
//   missing result line leaves the disposition `unknown`.
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
import { fileURLToPath } from "node:url";

export const RUNTIME = "command";
export const ENTRYPOINT = "native_mod";
export const TRANSPORT = "headless_ndjson";
export const MODULE_ARTIFACT_ID = "command-mod-0.1.0-glue.2";

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

export function buildTaskPrompt(taskSnapshot, text) {
  if (taskSnapshot === null || typeof taskSnapshot !== "object" || Array.isArray(taskSnapshot)) {
    throw new Error("TASK_SNAPSHOT_REQUIRED");
  }
  if (text !== undefined && typeof text !== "string") {
    throw new Error("DISPATCH_TEXT_INVALID");
  }
  const body = typeof text === "string" && text.trim() ? text : null;
  return [`Task specification: ${JSON.stringify(taskSnapshot)}`, body]
    .filter(Boolean)
    .join("\n\n");
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
    && Object.hasOwn(EXIT_MEANINGS, exit.code);
}

export function outcomeFromRun(run) {
  const subtype = run?.result?.subtype ?? null;
  const exitCode = run?.exit?.code ?? null;
  const terminalExitAgrees = resultExitAgrees(run?.result, run?.exit);
  const cleanEvidence = terminalExitAgrees
    && (run?.events?.gaps?.length ?? 0) === 0
    && (run?.anomalies?.length ?? 0) === 0
    && run?.timed_out !== true;
  const applied = cleanEvidence && subtype === "success";
  const rejected = cleanEvidence && (subtype === "error" || subtype === "max_turns");
  const resultText = run?.result?.final_text;
  return {
    outcome: applied ? "applied" : rejected ? "rejected" : "unknown",
    details: {
      execution_shape: "sessionless_batch",
      batch_run_id: run?.batch_run_id ?? null,
      ...(cleanEvidence ? { completion_condition: "native_result_observed" } : {}),
      requested_model: run?.requested_model ?? null,
      effective_model: null,
      effective_model_status: "unknown",
      result_subtype: subtype,
      exit_code: exitCode,
      signal: run?.exit?.signal ?? null,
      anomalies: Array.isArray(run?.anomalies) ? run.anomalies : ["run_record_missing"],
      native_session_id: run?.session_id ?? null,
      prompt_sha256: run?.prompt_sha256 ?? null,
      prompt_bytes: run?.prompt_bytes ?? null,
      control_record_ref: run?.control_record_ref ?? null,
      result_ref: run?.result_ref ?? null,
      artifact_refs: Array.isArray(run?.artifact_refs) ? run.artifact_refs : [],
      result_text_sha256: typeof resultText === "string" ? sha256Hex(resultText) : null,
      result_text_bytes: typeof resultText === "string" ? Buffer.byteLength(resultText, "utf8") : null,
      ...(!cleanEvidence ? { diagnostic_code: run?.anomalies?.[0] ?? "NATIVE_RESULT_NOT_VALIDATED" } : {}),
    },
  };
}

function createAdmission(options) {
  return {
    schema: 1,
    operation_id: options.operationId,
    execution_shape: "sessionless_batch",
    batch_run_id: batchRunId(options.operationId),
    requested_model: options.requestedModel,
    prompt_sha256: sha256Hex(options.prompt),
    prompt_bytes: Buffer.byteLength(options.prompt, "utf8"),
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

// Classify one NDJSON line from the headless stream. The stream has two
// documented shapes: event frames (one per AgentEvent, carrying `type`) and
// one final result line (carrying `subtype`). Anything else is a protocol
// gap: preserved, never silently treated as an empty or successful frame.
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
    return { kind: "result", result: value };
  }
  if (typeof value.type === "string") {
    return { kind: "event", event: value };
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

export function readModJournal(controlDir) {
  const records = readJsonLines(join(controlDir, "mod-journal.ndjson"));
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
  mkdirSync(controlDir, { recursive: true });
  const expectedAdmission = createAdmission(options);
  const priorAdmission = readAdmission(controlDir);
  if (priorAdmission) {
    if (
      priorAdmission.schema !== 1
      || priorAdmission.operation_id !== operationId
      || priorAdmission.batch_run_id !== expectedAdmission.batch_run_id
      || priorAdmission.requested_model !== requestedModel
      || priorAdmission.prompt_sha256 !== expectedAdmission.prompt_sha256
      || priorAdmission.prompt_bytes !== expectedAdmission.prompt_bytes
      || priorAdmission.control_record_ref !== expectedAdmission.control_record_ref
      || priorAdmission.result_ref !== expectedAdmission.result_ref
      || JSON.stringify(priorAdmission.artifact_refs) !== JSON.stringify(expectedAdmission.artifact_refs)
    ) {
      throw new Error("OPERATION_ID_CONFLICT");
    }
    const saved = existsSync(runPath) ? JSON.parse(readFileSync(runPath, "utf8")) : null;
    if (saved) {
      if (
        saved.operation_id !== operationId
        || saved.batch_run_id !== expectedAdmission.batch_run_id
        || saved.requested_model !== requestedModel
        || saved.prompt_sha256 !== expectedAdmission.prompt_sha256
        || saved.prompt_bytes !== expectedAdmission.prompt_bytes
        || saved.control_record_ref !== expectedAdmission.control_record_ref
        || saved.result_ref !== expectedAdmission.result_ref
        || JSON.stringify(saved.artifact_refs) !== JSON.stringify(expectedAdmission.artifact_refs)
      ) {
        throw new Error("CONTROL_RECORD_CONFLICT");
      }
      return { ...saved, replayed_from_saved_evidence: true };
    }
    return {
      schema: 1,
      runtime: RUNTIME,
      entrypoint: ENTRYPOINT,
      transport: TRANSPORT,
      module_artifact_id: config.moduleArtifactId ?? MODULE_ARTIFACT_ID,
      operation_id: operationId,
      batch_run_id: priorAdmission.batch_run_id,
      requested_model: requestedModel,
      effective_model: null,
      effective_model_status: "unknown",
      control_dir: controlDir,
      control_record_ref: priorAdmission.control_record_ref,
      result_ref: priorAdmission.result_ref,
      artifact_refs: artifactRefs(operationId),
      prompt_sha256: priorAdmission.prompt_sha256,
      prompt_bytes: priorAdmission.prompt_bytes,
      result: null,
      exit: { code: null, signal: null, spawn_error: null, meaning: null },
      disposition: "unknown",
      disposition_basis: "admission_without_terminal_record",
      anomalies: ["native_result_missing_after_admission"],
      events: { total: 0, by_type: {}, gaps: [] },
      replayed_from_saved_evidence: true,
    };
  }
  if (existsSync(runPath)) {
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
  const child = spawn(config.command, args, {
    cwd,
    env: { ...process.env, ELIOT_COMMAND_CONTROL_DIR: controlDir },
    stdio: ["ignore", "pipe", "pipe"],
  });

  const eventsPath = join(controlDir, "events.ndjson");
  writeFileSync(eventsPath, "", "utf8");
  const events = [];
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
    const record = { seq: events.length + gaps.length + 1, ...classified };
    if (classified.kind === "event") {
      events.push(classified.event);
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
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        timedOut = true;
        child.kill("SIGTERM");
        setTimeout(() => child.kill("SIGKILL"), 2000).unref();
      }, timeoutMs);
    }
    child.on("error", (error) =>
      resolveExit({ code: null, signal: null, spawnError: error.message }),
    );
    child.on("close", (code, signal) => {
      if (timer) clearTimeout(timer);
      resolveExit({ code, signal, spawnError: null });
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

  const eventTypes = {};
  for (const event of events) {
    eventTypes[event.type] = (eventTypes[event.type] ?? 0) + 1;
  }
  const sessionFromEvent = events.find(
    (e) => e.type === "run_start" && typeof e.sessionId === "string",
  )?.sessionId;

  const record = {
    schema: 1,
    runtime: RUNTIME,
    entrypoint: ENTRYPOINT,
    transport: TRANSPORT,
    module_artifact_id: config.moduleArtifactId ?? MODULE_ARTIFACT_ID,
    operation_id: operationId,
      batch_run_id: expectedAdmission.batch_run_id,
    requested_model: requestedModel,
    // Command's headless result/event contract has not yielded a documented
    // effective-model identity field; never infer one from the request.
    effective_model: null,
    effective_model_status: "unknown",
    control_dir: controlDir,
    control_record_ref: expectedAdmission.control_record_ref,
    result_ref: expectedAdmission.result_ref,
    artifact_refs: artifactRefs(operationId),
    started_at: startedAt,
    finished_at: finishedAt,
    prompt_sha256: sha256Hex(options.prompt),
    prompt_bytes: Buffer.byteLength(options.prompt, "utf8"),
    prompt_length: options.prompt.length,
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
    events: { total: events.length, by_type: eventTypes, gaps },
    stderr: { text: stderrText, truncated: stderrTruncated },
  };
  const tmpPath = `${runPath}.tmp`;
  writeFileSync(tmpPath, JSON.stringify(record, null, 2) + "\n", "utf8");
  renameSync(tmpPath, runPath);
  return record;
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
  const journal = readModJournal(resolved);
  const eventRecords = readJsonLines(join(resolved, "events.ndjson"));
  return {
    scope: "single_headless_run",
    completeness: "partial",
    control_dir: resolved,
    admission,
    terminal: run ? run.disposition : "not_observed",
    run,
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
    const config = loadConfig(flag("--config"));
    const prompt = flag("--prompt");
    if (!prompt) throw new Error("open requires --prompt");
    const operationId = flag("--operation-id");
    const requestedModel = flag("--model");
    const cwd = flag("--cwd");
    if (!operationId) throw new Error("open requires --operation-id");
    if (!requestedModel) throw new Error("open requires --model");
    if (!cwd) throw new Error("open requires --cwd");
    let controlDir = flag("--control-dir");
    if (!controlDir) {
      if (!config.controlRoot) {
        throw new Error("open requires --control-dir or config.controlRoot");
      }
      controlDir = join(
        config.controlRoot,
        `run-${new Date().toISOString().replace(/[:.]/g, "-")}-${process.pid}`,
      );
    }
    const record = await openRun(config, {
      operationId,
      requestedModel,
      cwd,
      prompt,
      controlDir,
      timeoutMs: Number(flag("--timeout-ms") ?? 0),
    });
    process.stdout.write(JSON.stringify(record, null, 2) + "\n");
    return;
  }
  if (command === "snapshot") {
    const controlDir = flag("--control-dir");
    if (!controlDir) throw new Error("snapshot requires --control-dir");
    process.stdout.write(JSON.stringify(snapshotRun(controlDir), null, 2) + "\n");
    return;
  }
  throw new Error(
    "usage: glue.mjs describe --config FILE | open --config FILE --operation-id ID --model MODEL --cwd DIR --prompt TEXT [--control-dir DIR] | snapshot --control-dir DIR",
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`glue: ${error.message}\n`);
    process.exitCode = 1;
  });
}
