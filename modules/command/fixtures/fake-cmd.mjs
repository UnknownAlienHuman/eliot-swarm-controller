#!/usr/bin/env node
// Fixture executable standing in for the installed Command Code CLI (`cmd`).
//
// It emulates ONLY what the official documentation states (headless page and
// Mods page, accessed 2026-10-02; registry ids CC-HEADLESS/CC-MODS):
// - `-p --output-format json` prints NDJSON: `{type:"event",event}` frames
//   followed by one `{type:"result", ...}` line.
// - Documented exit codes (0 success, 3 auth, 8 max turns, 1 general error).
// - `--mod <path>` loads a mod file: its default-export factory receives a
//   ModApi object; a mod that fails to import or throws is a warning on
//   stderr, never a crashed session. `cmd.on` handlers are isolated: a throw
//   becomes a `mod_error` event. Print mode fires the host lifecycle events
//   `session_start` / `session_shutdown` to mod handlers only; they are not
//   part of the printed AgentEvent stream.
//
// This is a synthetic peer for glue/mod tests. It is not the vendor binary,
// makes no model calls, and its event payloads are fixture data built from
// the documented payload highlights — not recorded live traffic.
//
// Scenario selection: FAKE_CMD_SCENARIO env var.
//   success | auth-error | max-turns | mod-error | crash-no-result |
//   no-session-id | success-exit-mismatch  (default: success)

import { pathToFileURL } from "node:url";
import { appendFileSync } from "node:fs";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function parseArgs(argv) {
  const parsed = { print: false, outputFormat: "text", model: null, mods: [], prompt: null, version: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--version") parsed.version = true;
    else if (arg === "-p" || arg === "--print") parsed.print = true;
    else if (arg === "--output-format") parsed.outputFormat = argv[++i];
    else if (arg === "--model") parsed.model = argv[++i];
    else if (arg === "--mod") parsed.mods.push(argv[++i]);
    else if (arg === "--resume" || arg === "-r") i += 1; // value unused by fixtures
    else if (arg === "--continue" || arg === "-c") { /* flag */ }
    else parsed.prompt = arg;
  }
  return parsed;
}

function printLine(value) {
  process.stdout.write(JSON.stringify(value) + "\n");
}

function printEventFrame(event) {
  printLine({ type: "event", event });
}

function printResultFrame(result) {
  printLine({ type: "result", ...result });
}

function recordFilteredEnvironment() {
  const output = process.env.FAKE_CMD_ENV_PROBE_FILE;
  if (!output) return;
  appendFileSync(output, JSON.stringify({
    owner_present: Object.hasOwn(process.env, "ELIOT_SWARM_MODULE_OWNER"),
    state_present: Object.hasOwn(process.env, "ELIOT_SWARM_MODULE_STATE"),
    command_control_dir: process.env.ELIOT_COMMAND_CONTROL_DIR ?? null,
    qual_capture_present: Object.keys(process.env).some((name) => name.startsWith("SWARM_QUAL_")),
    capture_present: Object.keys(process.env).some((name) => name.toUpperCase().includes("CAPTURE")),
    path_present: typeof process.env.PATH === "string",
    vendor_auth_present: Object.hasOwn(process.env, "ANTHROPIC_API_KEY"),
  }) + "\n");
}

function createModHost() {
  const handlers = new Map();
  const activeTools = new Set(["read", "write", "shell", "search"]);
  const calls = [];
  const api = {
    name: "eliot-command",
    cwd: process.cwd(),
    session: undefined,
    on(event, handler) {
      const list = handlers.get(event) ?? [];
      list.push(handler);
      handlers.set(event, list);
      return { dispose() {} };
    },
    queueMessage(request) {
      calls.push({ method: "queueMessage", request });
    },
    getActiveTools() {
      return [...activeTools];
    },
    setActiveTools(names) {
      activeTools.clear();
      for (const name of names) activeTools.add(name);
      calls.push({ method: "setActiveTools", names: [...names] });
    },
    setModel(value) {
      calls.push({ method: "setModel", value });
    },
    setEffort(value) {
      calls.push({ method: "setEffort", value });
    },
  };
  const dispatch = (event) => {
    for (const handler of handlers.get(event.type) ?? []) {
      try {
        handler(event);
      } catch (error) {
        // Documented host behavior: handler throws become mod_error events.
        printEventFrame({
          type: "mod_error",
          modId: api.name,
          hook: `on:${event.type}`,
          error: String(error?.message ?? error),
        });
      }
    }
  };
  return { api, calls, dispatch };
}

async function loadMods(paths, host) {
  for (const path of paths) {
    try {
      const module = await import(pathToFileURL(path).href);
      if (typeof module.default !== "function") {
        process.stderr.write(`warning: mod ${path} exports no factory\n`);
        continue;
      }
      await module.default(host.api);
    } catch (error) {
      // Documented: a failing mod is a warning, never a crashed session.
      process.stderr.write(`warning: mod ${path} failed to load: ${error.message}\n`);
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  recordFilteredEnvironment();
  if (args.version) {
    process.stdout.write("cmd version 1.66.0 (fixture)\n");
    return;
  }
  if (process.env.FAKE_CMD_INVOCATION_FILE) {
    appendFileSync(process.env.FAKE_CMD_INVOCATION_FILE, "native-run\n");
  }
  if (!args.print || args.outputFormat !== "json" || !args.model) {
    process.stderr.write("fixture supports only: -p --output-format json --model <id>\n");
    process.exitCode = 2;
    return;
  }
  process.stderr.write(`fixture observed --model ${args.model}\n`);
  const scenario = process.env.FAKE_CMD_SCENARIO ?? "success";
  const host = createModHost();
  await loadMods(args.mods, host);
  const emit = (event) => {
    printEventFrame(event);
    host.dispatch(event);
  };
  const lifecycle = (type, payload) => host.dispatch({ type, ...payload });
  const usage = { inputTokens: 120, outputTokens: 40 };

  switch (scenario) {
    case "auth-error": {
      // Session never binds: the mod factory ran (mod_loaded) but no
      // session_start fires, and the result line has no sessionId.
      printResultFrame({
        subtype: "error",
        usage: { inputTokens: 0, outputTokens: 0 },
        durationMs: 9,
        finalText: "",
        error: "not authenticated",
      });
      process.exitCode = 3;
      return;
    }
    case "crash-no-result": {
      lifecycle("session_start", { source: "startup" });
      emit({ type: "run_start", sessionId: "ses_fixture_crash" });
      process.stderr.write("fatal: harness died mid-run\n");
      process.exitCode = 1;
      return;
    }
    case "max-turns": {
      lifecycle("session_start", { source: "startup" });
      emit({ type: "run_start", sessionId: "ses_fixture_max" });
      emit({ type: "turn_start", turnNumber: 1 });
      emit({ type: "turn_end", turnNumber: 1, hadToolCalls: false, usage });
      printResultFrame({
        subtype: "max_turns",
        usage,
        durationMs: 640,
        finalText: "partial answer",
        sessionId: "ses_fixture_max",
        stopReason: "max_turns",
      });
      lifecycle("session_shutdown", { reason: "shutdown" });
      process.exitCode = 8;
      return;
    }
    case "mod-error": {
      lifecycle("session_start", { source: "startup" });
      emit({ type: "run_start", sessionId: "ses_fixture_moderr" });
      emit({ type: "turn_start", turnNumber: 1 });
      // A different mod fails; the CLI and this run stay alive.
      emit({
        type: "mod_error",
        modId: "other-mod",
        hook: "beforeToolCall",
        error: "boom",
      });
      emit({ type: "turn_end", turnNumber: 1, hadToolCalls: false, usage });
      emit({ type: "run_end", sessionId: "ses_fixture_moderr", result: { stopReason: "end_turn" } });
      printResultFrame({
        subtype: "success",
        usage,
        durationMs: 210,
        finalText: "done despite the mod error",
        sessionId: "ses_fixture_moderr",
        stopReason: "end_turn",
      });
      lifecycle("session_shutdown", { reason: "shutdown" });
      process.exitCode = 0;
      return;
    }
    case "no-session-id": {
      lifecycle("session_start", { source: "startup" });
      emit({ type: "run_start" });
      emit({ type: "turn_start", turnNumber: 1 });
      emit({ type: "turn_end", turnNumber: 1, hadToolCalls: false, usage });
      printResultFrame({
        subtype: "success",
        usage,
        durationMs: 130,
        finalText: "done without a session id",
        stopReason: "end_turn",
      });
      lifecycle("session_shutdown", { reason: "shutdown" });
      process.exitCode = 0;
      return;
    }
    default: {
      // success: one tool call; the pause lets the mod's inbox poll run so
      // pre-seeded inbox commands are consumed mid-run.
      lifecycle("session_start", { source: "startup" });
      emit({ type: "run_start", sessionId: "ses_fixture_1" });
      emit({ type: "model_request_start", model: args.model });
      emit({ type: "turn_start", turnNumber: 1 });
      emit({ type: "tool_queued", input: { tool: "read", path: "README.md" } });
      emit({ type: "tool_running", description: "Read README.md" });
      await sleep(900);
      emit({ type: "tool_completed", result: "ok" });
      emit({ type: "turn_end", turnNumber: 1, hadToolCalls: true, usage });
      emit({ type: "model_request_end", model: args.model, usage, stopReason: "end_turn" });
      emit({ type: "run_end", sessionId: "ses_fixture_1", result: { stopReason: "end_turn" } });
      printResultFrame({
        subtype: "success",
        usage,
        durationMs: 990,
        finalText: "done",
        sessionId: "ses_fixture_1",
        stopReason: "end_turn",
      });
      lifecycle("session_shutdown", { reason: "shutdown" });
      process.exitCode = scenario === "success-exit-mismatch" ? 1 : 0;
    }
  }
}

main().catch((error) => {
  process.stderr.write(`fixture failure: ${error.message}\n`);
  process.exitCode = 1;
});
