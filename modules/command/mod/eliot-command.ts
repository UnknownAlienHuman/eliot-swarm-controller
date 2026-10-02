// Eliot Command Code mod — the pinned native-side unit of modules/command.
//
// Written against the documented Command Code ModApi (official docs pages
// "Mods", accessed 2026-10-02; source registry ids CC-MODS/CC-AGENTS in
// docs/agent_swarm.runtime-sources-v16.json). The ModApi is documented as
// experimental, and no installed Command Code binary has been qualified with
// this mod yet (installed_runtime_verified: false in the runtime matrix), so
// every behavior here sticks to the documented surface only:
//
// - `cmd.on(event, handler)` observes AgentEvents; it cannot block or rewrite.
// - `cmd.queueMessage({content, deliverAs})` returns void. A queued message is
//   an admission into the native loop, never evidence that it was applied.
// - `cmd.getActiveTools()` / `cmd.setActiveTools(names)` are the documented
//   tool controls; the getter is the readback for the setter.
// - `cmd.setModel(value)` / `cmd.setEffort(value)` are buffered setters with
//   no documented readback, so this mod records them as requested only.
//
// The mod is inert unless the glue sets ELIOT_COMMAND_CONTROL_DIR to a
// per-run control directory. Its only channel to the glue is that directory:
// it appends to mod-journal.ndjson and polls inbox.ndjson. Journal records
// are observations, not a second authoritative queue: the native loop remains
// the only owner of queued work (implementation plan v6, section 7).

import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

export const MOD_NAME = "eliot-command";
export const MOD_VERSION = "0.1.0";
export const CONTROL_DIR_ENV = "ELIOT_COMMAND_CONTROL_DIR";

interface QueueMessageRequest {
  content: string;
  deliverAs?: "steer" | "follow-up";
}

// The subset of the documented ModApi this mod uses. Structural typing only;
// the real host supplies the full object.
interface ModApiLike {
  name: string;
  cwd: string;
  on(event: string, handler: (event: unknown) => void): unknown;
  queueMessage(request: QueueMessageRequest): void;
  getActiveTools(): readonly string[];
  setActiveTools(names: readonly string[]): void;
  setModel(value: string): void;
  setEffort(value: string): void;
}

// AgentEvent types from the documented catalog that this mod journals.
// Streaming partials (text_delta, thinking_delta, message_update, tool_update)
// are deliberately excluded: they are repeated telemetry, not facts, and the
// module contract allows coalescing those instead of storing every version.
const OBSERVED_EVENTS: readonly string[] = [
  "run_start",
  "run_end",
  "turn_start",
  "turn_end",
  "message_start",
  "message_end",
  "model_request_start",
  "model_request_end",
  "tool_queued",
  "tool_denied",
  "tool_hook_blocked",
  "tool_running",
  "tool_completed",
  "tool_errored",
  "subagent_start",
  "subagent_stop",
  "subagent_progress",
  "compaction_start",
  "compaction_done",
  "notice",
  "session_titled",
  "permission_mode_changed",
  "config_setting_changed",
  "continuation_recovery",
  "mod_error",
  "interrupted",
  "run_error",
  "session_start",
  "session_shutdown",
];

interface InboxCommand {
  id: string;
  kind: string;
  content?: string;
  deliverAs?: string;
  names?: string[];
  model?: string;
  effort?: string;
}

export default function eliotCommandMod(cmd: ModApiLike): void {
  const controlDir = process.env[CONTROL_DIR_ENV];
  if (!controlDir) {
    // Not launched by the Eliot glue: stay completely inert.
    return;
  }
  try {
    if (!existsSync(controlDir)) {
      mkdirSync(controlDir, { recursive: true });
    }
  } catch {
    return;
  }

  const journalPath = join(controlDir, "mod-journal.ndjson");
  const inboxPath = join(controlDir, "inbox.ndjson");
  let seq = 0;

  const journal = (kind: string, fields?: Record<string, unknown>): void => {
    seq += 1;
    const record = { seq, ts_ms: Date.now(), kind, ...fields };
    try {
      appendFileSync(journalPath, JSON.stringify(record) + "\n", "utf8");
    } catch {
      // A mod must never crash the native session over its own journal.
    }
  };

  const journalError = (where: string, error: unknown): void => {
    journal("mod_local_error", {
      where,
      error: error instanceof Error ? error.message : String(error),
    });
  };

  journal("mod_loaded", {
    mod: MOD_NAME,
    version: MOD_VERSION,
    host_name: typeof cmd.name === "string" ? cmd.name : null,
    cwd: typeof cmd.cwd === "string" ? cmd.cwd : null,
    pid: process.pid,
  });

  for (const type of OBSERVED_EVENTS) {
    try {
      cmd.on(type, (event: unknown) => {
        if (type === "session_start") {
          journal("mod_ready", { event });
        } else if (type === "session_shutdown") {
          journal("mod_session_end", { event });
        } else {
          journal("native_event", { event_type: type, event });
        }
      });
    } catch (error) {
      journalError(`on:${type}`, error);
    }
  }

  // Inbox: the glue appends one JSON command per line. Lines are consumed by
  // offset; a trailing partial line waits for its newline. Every outcome is
  // journaled under the caller's id so the glue can correlate without
  // treating this file as an authoritative queue.
  let inboxOffset = 0;
  const pollInbox = (): void => {
    let text: string;
    try {
      if (!existsSync(inboxPath)) {
        return;
      }
      text = readFileSync(inboxPath, "utf8");
    } catch (error) {
      journalError("inbox_read", error);
      return;
    }
    if (text.length < inboxOffset) {
      inboxOffset = 0; // file was replaced; restart consumption
    }
    const pending = text.slice(inboxOffset);
    const lastNewline = pending.lastIndexOf("\n");
    if (lastNewline < 0) {
      return;
    }
    const complete = pending.slice(0, lastNewline);
    inboxOffset += lastNewline + 1;
    for (const line of complete.split("\n")) {
      if (line.trim() === "") {
        continue;
      }
      let command: InboxCommand;
      try {
        command = JSON.parse(line) as InboxCommand;
      } catch (error) {
        journal("inbox_rejected", { id: null, reason: "unparseable_line" });
        journalError("inbox_parse", error);
        continue;
      }
      applyInboxCommand(cmd, command, journal, journalError);
    }
  };

  const timer = setInterval(pollInbox, 250) as unknown as {
    unref?: () => void;
  };
  if (typeof timer.unref === "function") {
    timer.unref();
  }
  try {
    cmd.on("session_shutdown", () => {
      clearInterval(timer as unknown as ReturnType<typeof setInterval>);
    });
  } catch (error) {
    journalError("on:session_shutdown:timer", error);
  }
}

function applyInboxCommand(
  cmd: ModApiLike,
  command: InboxCommand,
  journal: (kind: string, fields?: Record<string, unknown>) => void,
  journalError: (where: string, error: unknown) => void,
): void {
  const id = typeof command.id === "string" ? command.id : null;
  if (id === null) {
    journal("inbox_rejected", { id: null, reason: "missing_id" });
    return;
  }
  try {
    switch (command.kind) {
      case "queue_message": {
        if (typeof command.content !== "string" || command.content === "") {
          journal("inbox_rejected", { id, reason: "queue_message_requires_content" });
          return;
        }
        const deliverAs =
          command.deliverAs === "steer" ? "steer" : "follow-up";
        cmd.queueMessage({ content: command.content, deliverAs });
        // queueMessage returns void: this record is native admission only.
        // It is never promoted to "applied" anywhere in this module.
        journal("queue_admitted", { id, deliver_as: deliverAs });
        return;
      }
      case "set_active_tools": {
        if (!Array.isArray(command.names)) {
          journal("inbox_rejected", { id, reason: "set_active_tools_requires_names" });
          return;
        }
        const activeBefore = [...cmd.getActiveTools()];
        cmd.setActiveTools(command.names);
        const activeAfter = [...cmd.getActiveTools()];
        journal("tools_readback", {
          id,
          requested: command.names,
          active_before: activeBefore,
          active_after: activeAfter,
        });
        return;
      }
      case "set_model": {
        if (typeof command.model !== "string" || command.model === "") {
          journal("inbox_rejected", { id, reason: "set_model_requires_model" });
          return;
        }
        cmd.setModel(command.model);
        // Buffered setter, no documented readback: requested, not verified.
        journal("model_requested", { id, model: command.model, applied: "unknown" });
        return;
      }
      case "set_effort": {
        if (typeof command.effort !== "string" || command.effort === "") {
          journal("inbox_rejected", { id, reason: "set_effort_requires_effort" });
          return;
        }
        cmd.setEffort(command.effort);
        journal("effort_requested", { id, effort: command.effort, applied: "unknown" });
        return;
      }
      default:
        journal("inbox_rejected", { id, reason: `unknown_kind:${command.kind}` });
    }
  } catch (error) {
    journalError(`inbox:${command.kind}`, error);
  }
}
