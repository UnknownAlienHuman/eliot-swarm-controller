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
The send association was rechecked on 2026-11-15 against installed Claude Code
2.1.325 (`--help` and a real Haiku 5.5 stream-json session), alongside the
official [streaming input documentation](https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode).
A UUID-stamped input written while a tool turn was busy emitted its exact
`command_lifecycle.command_uuid` with `queued`, then `started`; the terminal
`result.user_message_uuids` listed both inputs when the CLI consumed the second
inside the running turn. An idle follow-up emitted its own exact lifecycle
and terminal. This observation does not establish that every CLI version or
every busy input will produce that sequence.
On 2026-11-15, a real 2.1.325 stream-json turn with 129 native `Read` calls
reproduced the tool telemetry bound: the 129th `tool_result` was reported as
unknown, and later sends to the session were rejected. The repaired path keeps
the first 128 tool IDs/names without eviction, suppresses later tool telemetry
for that turn, and records one acknowledged `tool` coverage gap. An identity
conflict clears the retained bindings and fences telemetry. These coverage
gaps do not change the native terminal result or make the session unavailable.
Unresolved gap or batch acknowledgements, malformed streams, lost send
associations, and session mismatches remain unknown and block fresh sends.
This is a bounded live observation of Claude Code 2.1.325, not a guarantee
about other CLI versions.
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

Subject is `(hashed canonical CLAUDE_CONFIG_DIR + project/store identity, exact session id)`; the raw directory never enters committed metadata because the daemon also holds that pass_env value as a secret.
Resume uses that session in a new Eliot generation. Late stream-json records belong to
the old generation and cannot affect replacement work. Native subagents normally share parent
environment; no independent Eliot token without actual isolated environment support.
Sidechain assistant/user frames tagged with `parent_tool_use_id` and `agent_id`
are discarded before active-turn/session validation, so they cannot be rebound
to the foreground turn. A pending task notification followed by `system/init`
marks subsequent assistant/user output as autonomous, even if a foreground
input has started; suppression remains through the next terminal result because
the `started` lifecycle can race with autonomous frames. The ambiguous output
prefix and final text are omitted with a message/tool coverage gap rather than
attributed to the foreground turn. A task-notification result without a current
input UUID never settles that turn; with an exact UUID it may settle the
consumed input, while the autonomous output remains omitted.

## Transport

Run the installed native `claude` executable with `-p --input-format stream-json
--output-format stream-json --verbose` and an Eliot-minted `--session-id`; pass
`--model` for the frozen profile model, `--permission-mode` when the frozen
profile sets one, `--effort` when the frozen profile sets `effort` (the CLI
vocabulary `low`/`medium`/`high`/`xhigh`/`max`; absent keeps the CLI default), and `--permission-prompt-tool stdio` always so every
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

Profiles default to `mcp = "minimal"`, which passes
`--strict-mcp-config` and uses the supplied per-run file. A nonempty
`mcp_allow` is rejected at config load and rechecked at launch. The
explicit `inherit` policy adds the per-run file to native MCP
configuration. Before spawn, Eliot checks project `.mcp.json` and
settings, user settings, and both user and workspace-local `mcpServers`
from `.claude.json`. With `CLAUDE_CONFIG_DIR`, the user files come from
that root. Eliot refuses inherit when it sees a settings redirect, a
server identity field with environment expansion, enabled or installed
plugins, or managed files. It also requires
`ENABLE_CLAUDEAI_MCP_SERVERS=false` in the frozen process environment,
because Claude fetches connector entries itself.

An owner Swarm stdio entry or a URL on the configured `cloud_mcp`
hostname fails preflight with `profile_error` naming the entry and source.
The inherit launch also provides a native URL deny rule for that hostname
on all ports, covering remote servers Claude discovers after the file
scan. Pre-spawn refusal retains a definite failed start. A failure after
spawn has its own classification. Eliot does not write the user config.
[Claude Code MCP scopes](https://code.claude.com/docs/en/mcp#mcp-installation-scopes)
and [managed MCP policy](https://code.claude.com/docs/en/managed-mcp).

Requested vs observed: `requested` carries `mcp_policy` and `mcp_allow`;
`effective` carries `mcp_policy` and the server set. The `initialize`
control reply exposes no MCP inventory record, so the effective set stays
unknown (`null` / unverified) at readiness and the requested set is never
echoed as observed; the applied `--strict-mcp-config` argv is the qualified
startup difference for `minimal`. The first post-input `system`/`init`
stream record carries `mcp_servers` (name plus connection status per
server, verified live on installed 2.1.321); the plugin retains that
observed set (names and statuses only, secret-redacted and bounded) and
attaches it to the turn terminal, where the reducer merges it into the
effective set with `mcp_servers_verified` true. An absent or malformed init
inventory leaves the effective set unknown. The profile effort is retained in
`requested.effort` and passed as `--effort`. Initialize does not report the
applied level, so `effective.effort` is null and `effort_verified` is false.
`CLAUDE_CODE_EFFORT_LEVEL` takes precedence over the flag and native settings
can cap effort through `maxEffortLevel`. A conflicting nonempty
`CLAUDE_CODE_EFFORT_LEVEL` in the frozen process environment refuses the
launch before spawn.
[Claude Code environment variables](https://code.claude.com/docs/en/env-vars).

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

The SDK's structured `assistant.error:"billing_error"` on the main assistant
marks a provider billing/quota refusal. If that remains the last main assistant
error at a normal terminal, the turn settles as `failure`, origin `provider`,
with `native_error.code:"quota_exhausted"` and a typed quota restriction (no
reset is invented). The original terminal reason stays in `native_status`.
A later normal main-assistant record clears an earlier refusal, so recovered
turns can succeed. Sidechain records do not change this state; cancelled and
unknown terminal reasons keep their outcomes. This uses the
[vendor SDKAssistantMessage error field](https://code.claude.com/docs/en/agent-sdk/typescript#sdkassistantmessage),
not error prose.

| Eliot verb | Mechanism / limitation |
| --- | --- |
| launch/send | stdin `control_request initialize` with the pre-assigned session id, then one stdin `user` record per send; the Eliot-minted `uuid` echoed in `command_lifecycle` is the exact turn association, not local pipe acceptance |
| steer | exact captured turn via the PreToolUse `additionalContext` hook response (first verified live against 2.1.278; no named native steer method; mechanism `claude.PreToolUse.additionalContext` on the applied ack): queues behind any terminal frame for the live turn only (one waiter; a second is `rejected`), rides the next live callback for that turn after FIFO hook context; a standalone steer with empty FIFO context writes its prepared response, never the empty pass. Preflight the complete encoded native `control_response` frame, including newline, against 1 MiB. If the combined response is too large, reject the steer and deliver the full context-only response when it fits; if context alone is too large, fail before writing without an empty pass or context receipt. Applied is retained only after the confirmed native response; a turn that ends first drops the text as `rejected` (never leaked into another turn), and an uncertain write settles `unknown` without replay. Pre-queue refusals that never enqueue (turn not live, unresolved session or event-source outcome, occupied slot, turn mismatch) are `rejected` as proven no-effect. At the RPC deadline, keep the outcome receiver while exact-command withdrawal is queued asynchronously through the event task; keep serving controls, let a confirmed outcome win, retain `rejected` when withdrawal, stop, stream loss, or task close removes the still-queued steer (proven never written), and retain `unknown` only after withdrawal acknowledgement or event-task closure with no outcome available. Store retains the exact outcome for command retries. Permission callbacks are unaffected; native qualification pending a live run with a tool call after the steer queues |
| inject | `next_input` prepended directly inside same user SEND with exact native turn association; `turn_safe_point` uses authenticated HookEvent `before_tool` → PreToolUse hookSpecificOutput.additionalContext → confirmed response write + hook.before_tool receipt, captured generation/turn, no drift/replay; `active_turn` inject unsupported for this CLI |
| interrupt | stdin `control_request interrupt` for the bound active turn uuid, with the `still_queued` readback; the terminal follows and unknown is not repeated |
| stop | interrupt the captured busy turn and wait for its native terminal within the core bound, then drop stdin (the CLI exits on EOF) and kill only the direct CLI child by handle; missing terminal evidence retains an unknown turn; grandchildren are reaped by the generation Job on Windows / process group on Unix, not the plugin (see Transport) |
| resume | new plugin/CLI with `--resume` and the exact retained session id; never resend prior prompt; the frozen model (or the inherited `configure` overlay's accepted selector on a resume under an overlay) is re-applied via `set_model` on the idle preflight, and a drifted permission mode via `set_permission_mode` (drift path unverified live: no live resume has drifted yet) |
| approve/deny | exact `can_use_tool` native request id; the public `request.opened` is a common approval with kind `approval`: summary `"<tool> tool permission request; <key>: <value>; ..."` composed from the ask's semantic string arguments (command, description, cwd, file/path targets, flattened in sorted key order as plain text — no native JSON/input/ids in the public details), redacted before the existing event-size decision, and exactly two choices `approve` (decision `approve`) / `deny` (decision `deny`), `feedback_allowed` false; the native tool/input/transport binding stays in private pending state. Approval replies validate against that binary offer: an explicit canonical `choice_id` (from the shared `{choice_id, feedback}` args shape) is accepted only when it names the offered id matching the verb; no autochoice beyond the verb itself, no default choice is read from an out-of-offer id, and feedback is rejected. Allow echoes the asked `input` as `updatedInput`, deny sends the fixed message (verified live, 2.1.278) |
| answer | unsupported: the AskUserQuestion shape on the installed CLI is unverified |
| configure | Live model selector through the streaming `set_model` control request ([Agent SDK configuration](https://code.claude.com/docs/en/agent-sdk/configuration#change-configuration-mid-session)). Active turns, pending input and native background continuation refuse before the setter. The plugin checks live permission mode before and after the setter. In `auto`, the current catalogue must advertise auto support for the requested selector. A successful configure commits the accepted selector for subsequent turns and resume. The serving model and its verification come from the next bound, main-thread assistant record (`message.completed` or `turn.model_observed`). A correlated validation refusal includes its reason. Lost or ambiguous replies stay unknown. Effort is launch-only (`--effort`). |
| goal | no native goal mechanism; goal selection stays in Eliot |

Busy stop first sends the native `interrupt` control once for its captured exact
active turn and waits up to the core stop interrupt bound. The exact
`command_lifecycle` cancellation or native terminal `result` supplies the turn
evidence; the control response alone does not. The terminal commit precedes
native session teardown. Missing evidence keeps the turn unknown; force skips
this cooperative window.
The installed 2.1.325 interrupt capture on 2026-11-17 reports
`terminal_reason: aborted_streaming`; this explicit native abort maps to
`interrupted`, preserving the raw reason as `native_status`.

### Send admission and uncertainty

The daemon owns the SEND FIFO and normally waits for the previous turn. The
plugin separately refuses a fresh SEND before writing when its session/source
is unavailable, an exact turn is still active, or one prior input still awaits
association. These are authoritative `rejected` replies carrying the actual
reason: a generic `busy` RPC error after daemon dispatch would be classified
`unknown` despite no native input having been written. The event task repeats
the active/pending/source checks when registering the single input waiter, so
a race with a native frame is also a no-write rejection.

Register the command/UUID waiter before the native write. A proven no-write
error withdraws only that exact registration and retains a rejected outcome.
An ambiguous write or association deadline retains `unknown`, its actual
reason and bounded cause, plus the exact outcome receiver; it never resends
the text or sets a session-wide unknown latch. Silence does not free the
pending slot. Native write and association share a 25-second deadline inside
the daemon's 30-second command RPC budget, leaving time to return the native
cause instead of racing the outer timeout. A later exact lifecycle, after its source commit acknowledgement,
can settle the receiver and retire that slot. Reconcile it before same-command
replay, fresh input, and snapshot readback, retaining the original command/turn
binding. An unresolved source gap or lost event task still refuses fresh writes;
an acknowledged turn-scoped tool telemetry coverage gap does not. Resolving
a daemon command alone does not manufacture native evidence or clear an
unassociated plugin input. After exact late association and turn completion,
later distinct sends can proceed without restarting the Agent.

If a turn ends before queued hook context or a queued steer is consumed, do not
deliver it to a later turn: the steer settles `rejected` and the text is
dropped. A delayed hook carrying only FIFO context is inject; steer is the
separate verb above with its own exact-turn queue and mechanism. Neither is
`active_turn` inject, which stays unsupported for this CLI.

Terminal `result.usage` covers only the current user turn's main loop
(subagents excluded) and maps to `TurnUsage` at `main_loop_turn` scope with
`message_id` null; `result.modelUsage` and `total_cost_usd` are
cumulative/estimated and are never turn expenditure. `result.modelUsage`
sums per model into the retained `SessionUsage` snapshot's
`cumulative_tokens` (`inputTokens`, `outputTokens`, `cacheReadInputTokens`,
`cacheCreationInputTokens`; per-model `costUSD` is ignored because the
session cost authority is `total_cost_usd` below) and `total_cost_usd` maps
to its `cumulative_cost` with unit `usd_estimate`, never converted; both
commit as `session.usage` (source `claude:result`). Only genuinely supplied
nonnegative counters project; missing or wholly malformed usage stays null
so completion proceeds.

Native foreground tool calls arrive as `assistant` message `tool_use` content blocks
(`id` plus tool `name`) with the matching outcome in `user` message
`tool_result` blocks (`tool_use_id` plus the frame's own `is_error`
verdict). The adapter commits `tool.started` per `tool_use` block and
`tool.completed` per matching `tool_result` block, bound to the exact
turn and generation with the native `tool_use.id` as the item id
(`status` `completed`, or `error` when `is_error` is true). Only
foreground blocks map into this tool table. Ordinary sidechain assistant/user
frames carrying `parent_tool_use_id` and `agent_id` stay out, like sidechain
text. The per-turn hook bound counts all tool IDs observed, not just in-flight
calls. The adapter retains the first 128 tool IDs/names without eviction,
fences later tool telemetry for that turn, and records one acknowledged
`tool`-family coverage gap. An identity conflict clears the retained bindings
and fences telemetry. A foreground `tool_result` with no known
`tool_use` before the bound, or a `tool_use` with no name, is a coverage gap,
never a guessed tool. Acknowledging this telemetry gap leaves the native
terminal outcome and session availability intact, and later turns may send.
Verified against the stream-json
record shapes the installed CLI emits (`--help` stream-json
`--output-format`/`--input-format` surface; shapes per the Agent SDK
`ModelUsage` and message `content[]` `tool_use`/`tool_result` docs,
checked against installed 2.1.325; 129-call reproduction captured 2026-11-16 UTC;
result-frame capture in
`plugins/claude/src/events.rs` `result_terminal_completed_settles_success`).

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
Foreground `assistant` `tool_use` blocks commit `tool.started` and the
matching foreground `user` `tool_result` blocks commit `tool.completed`,
each bound to the exact active turn with the native `tool_use.id` as the
item id (see Native mapping). The 128-ID telemetry fence and its acknowledged
turn-scoped coverage gap do not alter terminal settlement; unresolved source
uncertainty still blocks fresh sends.
A terminal `result` record's `usage` is mapped, when genuinely supplied as
nonnegative counters, into `turn.completed` `data.usage` at `main_loop_turn`
scope with `message_id` null: with persistent streaming input it covers only
the current user turn's main loop (subagents excluded). Cumulative/estimated
`modelUsage` is never a turn total: it sums per model into the
session-scoped `cumulative_tokens` in `session.usage`; `total_cost_usd`
is persisted only as the session-scoped `cumulative_cost` in `session.usage`,
never as usage or billing; there is no native occupancy snapshot, so
`context_used_tokens` stays null (never sampled from turn counters), and the
frame-reported model window
(`context_window` on the init or result frame) fills `context_window_tokens`;
the transcript file size (`<config>/projects/*/<session>.jsonl`, found by
exact-filename scan, never a guessed slug) fills `session_file_bytes`.
Unavailable/malformed individual counters stay null
(other valid counters are retained), and no valid native counters means
the corresponding fact is omitted — never blocks or gaps the terminal.
Callbacks own missing permission/question decisions; hooks never mirror the same
lifecycle. Assistant messages remain individually bounded: one over the
persisted-event bound is an acknowledged `message` coverage gap for that message
alone, latches `final_text_unavailable`, and leaves the turn running on a healthy
source. Aggregate terminal text
and retained `last_turn` use the shared `MAX_EVENT_BYTES` budget: overflow drops
the aggregate and latches `final_text_unavailable="event_budget"` without changing
the native terminal status or interrupting bounded message events. A partial prefix
is never reported as the full reply; the latch resets only at exact turn activation.
Terminal event data and snapshots retain the same bounded text or unavailable fact.
Distinguish content blocks/tool ids within a message instead of deduplicating all
blocks by message id.

For the exact active turn, `agent.snapshot` names its `turn_evidence` from the
owned stream-json reader and process waiter. The turn is running while it has no
`result` on a healthy source and a live process. It is terminal from the exact
`result` (universal status, native `terminal_reason` as `native_status`). It is
lost when the observed exit drained stdout without a `result`. A declared gap or
a lost stream proves none of these, so the daemon monitor settles the turn
`unknown` after its bounded reads; the transcript is not read for this.

Transcript JSONL is later observe/recovery data with file identity/offset where valid,
not a parallel live producer. No proven replay cursor means gap + exact current
readback, never full-history republishing. Child completion needs exact tool/task
provenance, not parent idle. Background task-notification facts without an exact
user-message UUID are omitted rather than attributed to the active or most recent
user turn. Resumed cumulative cost is session scope, not a turn delta.

## Login

The standalone CLI uses its own authentication. Before launch or resume,
Eliot runs `claude auth status --json` with the selected executable, workspace,
and frozen environment. An explicit signed-out result settles the start as
failed with `auth_required` before creating a session or sending input.
The CLI and desktop display the exact executable and terminal sign-in command
for the owner to run. Eliot checks status only.
[Claude Code CLI authentication commands](https://code.claude.com/docs/en/cli-reference#cli-commands).

## Proxy and qualification

Use documented base URL/auth/header variables in the frozen profile; exclude unrelated
owner API keys. Report unavailable effective-route facts as unknown. Verify init ordering,
late callbacks, context expiry, request ids, resumed inputs, transcript overlap and actual
proxy/native billing in L7. Common generation/ack/custody rules remain in the plugin spec.

## Ad-hoc launch

`swarm launch --harness claude [--model M] [--effort E] [--cmd PATH] --workspace W "prompt"`
is Owner-only and freezes a profile-equivalent snapshot from safe defaults
(`workspace-write`, `never`, MCP `minimal`) plus flags. Minimal uses
`--strict-mcp-config` with the per-session Swarm MCP config and works when
the Claude install has plugins or cached remote settings. The ad-hoc
profile accepts the daemon's `CLAUDE_CONFIG_DIR` as a route selector,
checking its identity again before dispatch. For a named profile
whose explicit `inherit` preflight is refused, set `mcp = "minimal"`
in that profile or start an ad-hoc Claude chat.

An omitted `--model` omits the native flag so the CLI default applies; the
effective model is read back from initialize/stream. `--effort` maps to the
CLI `--effort` flag in the closed vocabulary `low|medium|high|xhigh|max`
(validated at the same boundary profiles use); an omitted effort keeps the
CLI default.

Discovery picks the newest standalone CLI by `--version` from `PATH` and
standard user install locations (`claude.exe` on Windows, bare `claude` on
Unix, also checking `/usr/local/bin/claude` and `~/.local/bin/claude`).
`.cmd`/`.bat`/`.ps1` wrappers are refused and never frozen; an extensionless
Unix candidate counts as native only after an ELF or Mach-O header check
(`#!` scripts never qualify), and every candidate must be an absolute path
before any `--version` probe runs. The Windows Claude Desktop payload under
`%APPDATA%/Claude/claude-code/<version>/<hash>` requires an explicit
`--cmd` or profile `cmd`. Anthropic documents standalone terminal
installation separately from the Desktop Code tab.
[Claude Code setup](https://code.claude.com/docs/en/setup).
`swarm harnesses` shows the selected absolute `cmd` and lists eligible
standalone `candidates` newest first. `swarm models --harness claude`
reports the native initialize inventory first when the reader proves one:
selectable values with display names and per-model effort levels, each entry
without a provenance marker. Only when the native read is unavailable does
it fall back to the
frozen documented `--model` aliases (`best`, `fable`, `sonnet`, `opus`,
`haiku`, `sonnet[1m]`, `opus[1m]`, `opusplan`, `opusplan[1m]`) and full model
ids (for example `claude-haiku-5-5`, `claude-opus-5-5`), each fallback entry
marked `source = "documented_alias"` with the CLI effort vocabulary attached
and `unknown` availability. An `unknown` read or cleanup outcome,
`cancelled`, an integrity failure or a
capacity refusal propagates as an error and is never cached as a catalogue.
An omitted `--model` selects the CLI default. Launch and live `configure`
use the selectable id. Effort applies at launch, with choices from native
inventory when available. The
native stream-json `initialize` response includes `models` with selectable
values, display names, resolved model IDs and model-specific effort levels when
the CLI reports them. The plugin private `model.list` method reads that
metadata without a prompt or auto-login, using the installed CLI
`--safe-mode` and strict empty MCP config. The daemon verifies the frozen
store and provider route against the exact private environment it forwards.
Missing or changed route selectors and legacy snapshots without route
capture use the alias fallback and bypass the native catalog cache. The
scope label omits `CLAUDE_CONFIG_DIR` and other private selector values.
Inventory membership leaves account entitlement unknown.
[Claude Agent SDK model information](https://code.claude.com/docs/en/agent-sdk/typescript#modelinfo).
## Agent skill delivery (`eliot-swarm`)

Every owned CLI spawn passes `--plugin-dir <data-dir>/skills`, the
daemon-materialized skill root, which doubles as a Claude Code plugin
directory (`.claude-plugin/plugin.json` beside `skills/eliot-swarm/`).
The flag loads the plugin for that session only (`claude --help`:
"Load a plugin from a directory or .zip ... (repeatable)"); no checkout
or global skill location changes, and resume spawns reuse the same argv.
A missing or unloadable directory fails the spawn with its cause.
Implementation: [`plugins/claude/src/host.rs`](../../plugins/claude/src/host.rs)
(`launch_session`).

## Minimal-context measurement (k61, 2026-11-16)

Private daemon, identical trivial turn, CLI default model (`claude-opus-5`
read back), installed 2.1.278. `inherit` (explicit): 5 connected MCP servers
(`claude.ai` connectors x2, `codebase_memory`, `eliot-search`, `eliot-swarm`)
with turn usage input 4 + cache-read 31,006 + output 85. `minimal` (default):
exactly `eliot-swarm`, verified, with input 4 + cache-read 47,049 +
cache-write 8,287 + output 85 (prompt caching varied between runs; the
cache-write drop from 31,144 reflects the smaller cached surface). A
follow-up turn under `minimal` called `api.identity` through the `swarm` tool
(model used the native `ToolSearch` first, then the call, after one Owner
approval) and reported principal `agent`. The installed binary contains the
`ToolSearch` tool, an `ENABLE_TOOL_SEARCH` env switch and a
`toolSearchEnabled` settings field, so deferred loading is natively
supported; Swarm does not enable it per session because no per-server
scoping exists on 2.1.278 and the strict config already bounds `minimal` to
one server.

## Native subagents (`native_subagents` deny)

Per-session mapping, verified on the installed Claude Code 2.1.278 binary:
`claude --help` exposes `--disallowedTools <tools...>` (comma or
space-separated deny list). The `Task` tool launches subagents (official
subagent docs describe deny via `Agent(name)` / `--disallowedTools`; the
installed help lists the flag). When the frozen profile carries
`native_subagents = "deny"` (the default for profiles with a non-empty
`child_profiles`), the plugin passes `--disallowedTools Task` on every CLI
spawn for the session. `allow` adds nothing and preserves today's behavior.
Implementation: `plugins/claude/src/host.rs` (`native_subagents_argv`,
`StartPlan::native_subagents_deny`, `launch_session`). The brief names
2.1.325; the installed binary in this worktree reports 2.1.278, and the flag
was verified there.

## Access levels (`--access ask|auto|full`)

Ad-hoc mapping to the CLI `--permission-mode` argv, frozen as
`permission_mode` and reported in effective settings: `ask` is `manual`
(`can_use_tool` produces pending requests decided with `swarm approve`/`deny`);
`auto` is `auto`; `full` is `bypassPermissions`, the full-access posture.
Implementation:
[`crates/eliot-daemon/src/adhoc.rs`](../../crates/eliot-daemon/src/adhoc.rs)
(`apply_adhoc_access`).


## Read-only provider quota

`harness.quota` is implemented in `plugins/claude/src/quota.rs`, following
OpenCodex 2.84.0's OAuth usage reader as a pattern only. It refuses external or
ambiguous provider and authentication selectors in the profile environment and
native user settings. The installed CLI's `auth status --json` must confirm a
first-party subscription route in the isolated reader directory. Then it reads
the CLI's own
`CLAUDE_CONFIG_DIR/.credentials.json` (default `~/.claude`) without writes, selects
`claudeAiOauth.accessToken`, and calls Anthropic `/api/oauth/usage` with
`anthropic-beta: oauth-2025-04-20`. It projects `five_hour`, `seven_day`, recognized
model weekly buckets and `limits[kind=weekly_scoped]`, never account identity.
Missing credentials, expiry and provider failures remain supported error states
with explicit origin. No refresh, login, model turn or inference probe is attempted.
