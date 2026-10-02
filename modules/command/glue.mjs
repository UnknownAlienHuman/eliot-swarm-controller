// Eliot Command Code glue — the host-side unit of modules/command.
//
// Drives the installed Command Code CLI (`cmd`) in its documented headless
// print mode (`-p --output-format json`) with the pinned Eliot mod loaded via
// `--mod`, and reduces the run to the module contract's describe / open /
// snapshot facts. It speaks no host IPC yet: wiring this facade behind the
// controller's RuntimePort is a later slice (see README "Remaining work").
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
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

export const RUNTIME = "command";
export const ENTRYPOINT = "native_mod";
export const TRANSPORT = "headless_ndjson";
export const MODULE_ARTIFACT_ID = "command-mod-0.1.0-glue.1";

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
    const probe = await runProbe(executable, config.argsPrefix ?? []);
    version = probe.version;
    versionNote = probe.note;
  }
  let modSha256 = null;
  const modPath = config.modPath ?? DEFAULT_MOD_PATH;
  if (existsSync(modPath)) {
    modSha256 = sha256Hex(readFileSync(modPath, "utf8"));
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
      open: "implemented_fixture_verified_live_unknown",
      snapshot: "implemented",
      queue_message: "admission_only_via_mod_inbox",
      set_active_tools: "implemented_with_native_readback",
      set_model: "requested_only_no_readback",
      set_effort: "requested_only_no_readback",
      goal: "unavailable_headless_setter_not_established",
      resume: "unavailable_in_this_slice",
      attach: "unavailable_in_this_slice",
      live_steer: "unavailable_as_host_operation",
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
  if (existsSync(runPath)) {
    throw new Error(`control directory already holds a run: ${controlDir}`);
  }
  mkdirSync(controlDir, { recursive: true });
  const modPath = config.modPath ?? DEFAULT_MOD_PATH;
  const args = [
    ...(config.argsPrefix ?? []),
    "-p",
    "--output-format",
    "json",
    "--mod",
    modPath,
    options.prompt,
  ];
  const startedAt = new Date().toISOString();
  const child = spawn(config.command, args, {
    cwd: options.cwd ?? config.cwd ?? process.cwd(),
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
    const classified = classifyLine(line);
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
  if (result) {
    dispositionBasis = "result_line";
    if (result.subtype === "success") disposition = "completed";
    else if (result.subtype === "error") disposition = "failed";
    else if (result.subtype === "max_turns") disposition = "max_turns";
  }
  const anomalies = [];
  if (framesAfterResult > 0) anomalies.push("frames_after_result_line");
  if (result && result.subtype === "success" && exit.code !== 0) {
    anomalies.push("exit_result_mismatch");
  }
  if (timedOut) anomalies.push("glue_timeout_killed_owned_child");

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
    control_dir: controlDir,
    started_at: startedAt,
    finished_at: finishedAt,
    prompt_sha256: sha256Hex(options.prompt),
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
  const journal = readModJournal(resolved);
  const eventRecords = readJsonLines(join(resolved, "events.ndjson"));
  return {
    scope: "single_headless_run",
    completeness: "partial",
    control_dir: resolved,
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
    "usage: glue.mjs describe --config FILE | open --config FILE --prompt TEXT [--control-dir DIR] | snapshot --control-dir DIR",
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`glue: ${error.message}\n`);
    process.exitCode = 1;
  });
}
