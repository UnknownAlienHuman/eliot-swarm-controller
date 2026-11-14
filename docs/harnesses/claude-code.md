# Claude Code

L6 adapter, after automation/coordination already work on Codex. Owner decision
2026-11-03: a Rust plugin over `claude -p --input-format stream-json
--output-format stream-json --verbose` in one per-Agent plugin/native process
tree; no TypeScript Agent SDK, no Node.

## Sources and unresolved facts

The stream-json record shapes were verified against the installed CLI
(`claude -p --input-format stream-json --output-format stream-json --verbose`,
Claude Code 2.1.278): [headless](https://code.claude.com/docs/en/headless),
[hooks](https://code.claude.com/docs/en/hooks),
[CLI](https://code.claude.com/docs/en/cli-reference).
The TypeScript Agent SDK is not used.
These notes do not establish runtime support or a current license approval.
Verified live on Claude Code 2.1.278: with
`--permission-prompt-tool stdio` a tool use arrives as a `control_request`
`{"subtype":"can_use_tool","tool_name","display_name","input","description",
"permission_suggestions","tool_use_id"}`; a `control_response` with
`{"behavior":"allow","updatedInput":<input>}` runs the tool and
`{"behavior":"deny","message":…}` refuses it, each on the same `request_id`.
Live on a resumed session: the unconditional `set_model` realign call and
the resumed CLI's re-announcement of the same retained session id.
With Claude Code 2.1.278 and `claude-agent-sdk` 0.2.180, initialize reported
`current_permission_mode=default`, `set_model("haiku")` succeeded, and the
native `set_permission_mode("manual")` response was `default`. Resume maps
the frozen `manual` intent to native `default` for comparison, setter input
and echo validation only. The requested profile stays `manual`; effective
mode remains the native readback. Other modes stay literal. Lost setter
replies and unexpected echoes remain `unknown`.

## Native identity and generation

Subject is `(canonical CLAUDE_CONFIG_DIR + project/store identity, exact session id)`.
Resume uses that session in a new Eliot generation. Late stream-json records belong to
the old generation and cannot affect replacement work. Native subagents normally share parent
environment; no independent Eliot token without actual isolated environment support.

## Transport

Run the installed native `claude` executable with `-p --input-format stream-json
--output-format stream-json --verbose` and an Eliot-minted `--session-id`; pass
`--model` for the frozen profile model, `--permission-mode` when the frozen
profile sets one, and `--permission-prompt-tool stdio` always so every
permission ask reaches Eliot. Pass `--resume <session-id>` only for explicit
resume, instead of `--session-id`. Speak NDJSON on stdin/stdout: `control_request`
envelopes (`initialize`, `set_model`, `set_permission_mode`, `interrupt`) with
`request_id` correlation, `user` input records whose Eliot-minted `uuid` is the
exact turn association, the `command_lifecycle` / `assistant` / `system` /
`result` stream records, and inbound `can_use_tool` permission asks answered
with a `control_response` carrying the documented SDK `PermissionResult`.
Arm the message reader before input. SessionStart hook lines carry the minted
session id before any input, so the adapter declares no `start_requires_input`.
A resumed CLI re-announces the same retained session id before readiness.

Each owned Claude CLI launch also receives a private per-session `--mcp-config`
file in the Eliot-created Agent instance directory. It registers the standard
`swarm` tool through an `eliot-swarm` stdio server using the current Eliot executable and explicit
`--data-dir <ELIOT_HOME> mcp --require-agent` arguments. The config contains no
identity values; the server inherits the Agent id, generation, and token from
the native process environment, and `--require-agent` fails closed. The file remains with
the owned CLI and is removed after confirmed stop and drain.

### Worker MCP policy

The additive per-run config above is the `inherit` policy (the default):
other native MCP sources keep Claude Code's normal behavior. Profiles may
select `mcp = "minimal"`, frozen in the profile snapshot; a non-empty
`mcp_allow` is rejected as `unsupported` at config load because Claude has
no selective-inclusion mechanism. `minimal` adds the installed
`--strict-mcp-config` flag ahead of `--mcp-config`, so only the per-run
file applies. Verified
read-only against installed Claude Code 2.1.278 (`claude --help`):
`--strict-mcp-config` means "Only use MCP servers from `--mcp-config`,
ignoring all other MCP configurations". Eliot never writes Claude's user
configuration in either policy.

Requested vs observed: `requested` carries `mcp_policy` and `mcp_allow`;
`effective` carries `mcp_policy` and the server set as unknown (`null` /
unverified). The installed stream-json protocol exposes no MCP inventory
record, so the effective set is not read back and the requested set is never
echoed as observed; the applied `--strict-mcp-config` argv is the qualified
startup difference for `minimal`.

Resolve Windows executables explicitly; never insert an unrequested shell wrapper.
Avoid modes that disable required hooks/instructions.

Stop drops the CLI's stdin and kills only that direct child by handle (kill
after a 2 s grace if EOF does not end it). Grandchildren — tool processes
the CLI itself spawns — are reaped by the generation custody the daemon put
the plugin in, not by the plugin: on Windows the plugin starts inside the
generation's named kill-on-close Job (`OwnedChild::spawn_agent` in
`crates/eliot-daemon/src/platform/custody.rs`, via
`PluginHost::spawn_plugin`), so closing the Job kills the whole family; on
Unix the plugin leads its own process group and the daemon group-kills it
(degraded: escapable via setsid).

## Native mapping

| Eliot verb | Mechanism / limitation |
| --- | --- |
| launch/send | stdin `control_request initialize` with the pre-assigned session id, then one stdin `user` record per send; the Eliot-minted `uuid` echoed in `command_lifecycle` is the exact turn association, not local pipe acceptance |
| steer | exact captured turn via the installed 2.1.278 PreToolUse `additionalContext` hook response (no named native steer method; mechanism `claude.PreToolUse.additionalContext` on the applied ack): queues behind any terminal frame for the live turn only (one waiter; a second is `rejected`), rides the next live callback for that turn after FIFO hook context; a standalone steer with empty FIFO context writes its prepared response, never the empty pass. Preflight the complete encoded native `control_response` frame, including newline, against 1 MiB. If the combined response is too large, reject the steer and deliver the full context-only response when it fits; if context alone is too large, fail before writing without an empty pass or context receipt. Applied is retained only after the confirmed native response; a turn that ends first drops the text as `rejected` (never leaked into another turn), and an uncertain write settles `unknown` without replay. Pre-queue refusals that never enqueue (turn not live, unresolved session or event-source outcome, occupied slot, turn mismatch) are `rejected` as proven no-effect, after same-id retained outcomes. At the RPC deadline, keep the outcome receiver while exact-command withdrawal is queued asynchronously through the event task; keep serving controls, let a confirmed outcome win, retain `rejected` when withdrawal, stop, stream loss, or task close removes the still-queued steer (proven never written), and retain `unknown` only after withdrawal acknowledgement or event-task closure with no outcome available. A closed outcome is retained under the exact command/turn/text binding before replying, so replay stays stable. Permission callbacks are unaffected; native qualification pending a live run with a tool call after the steer queues |
| inject | `next_input` prepended directly inside same user SEND with exact native turn association; `turn_safe_point` uses authenticated HookEvent `before_tool` → PreToolUse hookSpecificOutput.additionalContext → confirmed response write + hook.before_tool receipt, captured generation/turn, no drift/replay; `active_turn` inject unsupported for this CLI |
| interrupt | stdin `control_request interrupt` for the bound active turn uuid, with the `still_queued` readback; the terminal follows and unknown is not repeated |
| stop | drop stdin (the CLI exits on EOF) and kill only the direct CLI child by handle; does not merely detach observation; grandchildren are reaped by the generation Job on Windows / process group on Unix, not the plugin (see Transport) |
| resume | new plugin/CLI with `--resume` and the exact retained session id; never resend prior prompt; the frozen model is re-applied via `set_model` on the idle preflight, and a drifted permission mode via `set_permission_mode` (drift path unverified live: no live resume has drifted yet) |
| approve/deny | exact `can_use_tool` native request id; the public `request.opened` is a common approval with kind `approval`: summary `"<tool> tool permission request; <key>: <value>; ..."` composed from the ask's semantic string arguments (command, description, cwd, file/path targets, flattened in sorted key order as plain text — no native JSON/input/ids in the public details), redacted before the existing event-size decision, and exactly two choices `approve` (decision `approve`) / `deny` (decision `deny`), `feedback_allowed` false; the native tool/input/transport binding stays in private pending state. Approval replies validate against that binary offer: an explicit canonical `choice_id` (from the shared `{choice_id, feedback}` args shape) is accepted only when it names the offered id matching the verb; no autochoice beyond the verb itself, no default choice is read from an out-of-offer id, and feedback is rejected. Allow echoes the asked `input` as `updatedInput`, deny sends the fixed message (verified live, 2.1.278) |
| answer | unsupported: the AskUserQuestion shape on the installed CLI is unverified |
| configure | applied at start, not a runtime verb: `--model` / `--permission-mode` argv on a fresh start, `set_model` / `set_permission_mode` realign on resume; requested vs effective settings are reported before readiness, with `effective.model` null at readiness (the CLI never reads the active model back in `initialize`; `models[]` membership proves availability/membership only, never which model is actually serving). A non-empty `message.model` reported by an actual native `assistant` record is carried on its `message.completed` fact and merged as the only change into `Agent.effective.model`, preserving pid/permissions/session fields and the requested profile alias; missing/null stays unobserved and a reported model change is not a gap |
| goal | no native goal mechanism; goal selection stays in Eliot |

If a turn ends before queued hook context or a queued steer is consumed, do not
deliver it to a later turn: the steer settles `rejected` and the text is
dropped. A delayed hook carrying only FIFO context is inject; steer is the
separate verb above with its own exact-turn queue and mechanism. Neither is
`active_turn` inject, which stays unsupported for this CLI.

Terminal `result.usage` covers only the current user turn's main loop
(subagents excluded) and maps to `TurnUsage` at `main_loop_turn` scope with
`message_id` null; `result.modelUsage` and `total_cost_usd` are
cumulative/estimated and are never turn expenditure. `total_cost_usd`
maps to the retained `SessionUsage` snapshot committed as `session.usage`
(source `claude:result`) with unit `usd_estimate`, never converted;
`result.modelUsage` stays unmapped. Only genuinely supplied
nonnegative counters project (`input_tokens`, `output_tokens`,
`cache_read_input_tokens`, `cache_creation_input_tokens`); missing or
wholly malformed usage stays null so completion proceeds.

## Hook tool diagnostics

Each committed `hook.before_tool` fact carries the exact `turn_id` and opaque
`consumer_id` plus optional bounded progress diagnostics taken only from the
native `PreToolUse` input: `tool` (native tool name, 64 chars), `native_tool_id`
(matched `tool_use_id`, 128 chars), `operation` (closed vocabulary from the tool
name only: file readers `Read`/`Glob`/`Grep` map to `read`, file writers
`Edit`/`Write`/`NotebookEdit` to `write`, `Bash` to `exec`; every other tool
carries no operation), and `path` (one workspace-relative file path, 512 chars).
Only the top-level `tool_input` leaves `file_path`/`path` are read; commands,
contents and every other argument are dropped at extraction. Paths use the same
lexical workspace-relative normalization as native file hints, and an
out-of-workspace path is dropped. Every kept field is `SecretPolicy`-redacted
before its cap, and absent native data stays absent, so `on.event` matching is
unchanged. The wire `HookEvent` request shape is untouched.

## Source ownership and recovery

The stream-json stdout stream owns supplied session/turn/message/tool/usage facts.
A terminal `result` record's `usage` is mapped, when genuinely supplied as
nonnegative counters, into `turn.completed` `data.usage` at `main_loop_turn`
scope with `message_id` null: with persistent streaming input it covers only
the current user turn's main loop (subagents excluded). Cumulative/estimated
`modelUsage` is never a turn total and is not persisted; `total_cost_usd`
is persisted only as the session-scoped `cumulative_cost` in `session.usage`,
never as usage or billing; unavailable/malformed individual counters stay null
(other valid counters are retained), and no valid native counters means
`data.usage` is omitted — never blocks or gaps the terminal.
Callbacks own missing permission/question decisions; hooks never mirror the same
lifecycle. Assistant messages remain individually bounded. Aggregate terminal text
and retained `last_turn` use the shared `MAX_EVENT_BYTES` budget: overflow drops
the aggregate and latches `final_text_unavailable="event_budget"` without changing
the native terminal status or interrupting bounded message events. A partial prefix
is never reported as the full reply; the latch resets only at exact turn activation.
Terminal event data and snapshots retain the same bounded text or unavailable fact.
Distinguish content blocks/tool ids within a message instead of deduplicating all
blocks by message id.

Transcript JSONL is later observe/recovery data with file identity/offset where valid,
not a parallel live producer. No proven replay cursor means gap + exact current
readback, never full-history republishing. Child completion needs exact tool/task
provenance, not parent idle. Resumed cumulative cost is session scope, not a turn delta.

## Login

The CLI keeps its own login in its config directory; the Claude desktop app's
session is host-managed and is not shared with other processes. Before every
launch or resume the plugin runs `claude auth status` with the frozen
environment. When the CLI is logged out it runs the CLI's own
`claude auth login --claudeai` (the owner completes it in the browser) and waits
up to five minutes; an unfinished login rejects the start with
`native_not_dispatched` before any session exists. The plugin never sees or
stores credentials.

## Proxy and qualification

Use documented base URL/auth/header variables in the frozen profile; exclude unrelated
owner API keys. Report unavailable effective-route facts as unknown. Verify init ordering,
late callbacks, context expiry, request ids, resumed inputs, transcript overlap and actual
proxy/native billing in L7. Common generation/ack/custody rules remain in the plugin spec.

## Ad-hoc launch

`swarm launch --harness claude [--model M] --workspace W "prompt"` is
Owner-only and freezes a profile-equivalent snapshot from safe defaults
(`workspace-write`, `never`, MCP `inherit`) plus flags. An omitted `--model`
omits `--model` so the CLI default applies; the effective model is read back
from initialize/stream. `--effort` has no mapping and is rejected. `cmd` is
discovered deterministically (`claude.exe` on `PATH` or the standard user
install location; `.cmd`/`.bat`/`.ps1` wrappers are refused and never frozen).
`swarm models --harness claude` reports aliases plus the CLI default; no live
catalogue exists.
