# Antigravity CLI (`agy`)

Optional L6 adapter. One warm `agy acp` process belongs to one Eliot Agent
generation and custody unit. Source: [google-antigravity/antigravity-cli](https://github.com/google-antigravity/antigravity-cli).
The manifest's native version (`1.5.2`) is diagnostic metadata, not a
startup version gate. Startup validates the ACP protocol version and the
advertised session selectors.

## Native identity and transport

Subject: `(canonical auth/config scope, exact ACP sessionId)`.

```text
agy acp --stdio --dangerously-skip-permissions=false|true --add-dir <hook-discovery-root>
```

Installed CLI surface (observed 2026-11-17, read-only `--version`/`acp --help`;
1.5.3 on 2026-11-13):
`agy.exe` reports version 1.5.10. `agy acp` accepts `--add-dir` (repeatable),
`--agent`, `--app-data-dir`, `--cli_dir_name`, `--cli_log_file`/`--log-file`,
`--cloud_code_endpoint`, `--dangerously-skip-permissions`, `--disable-telemetry`,
`--gemini-dir`, `--ide-mcp-port`, `--include-default-customizations`,
`--sandbox`, `--stdio`, `--process-per-session`, `--worker`, and auth/endpoint
flags; it accepts no hook-config location or registration flag, and the
top-level `--settings` flag is a print-mode session option, not an `acp`
option. The install directory ships no bundled docs or settings schema, so
hook discovery truly requires a directory root. Inventory, model and quota reads,
and launch check one resolved CLI. The daemon verifies the vendor CLI module in
the executable before running bounded `agy acp --help` and `agy --version`.
The ACP help must advertise stdio and the permission option used at launch.
The manifest version stays diagnostic metadata.

The adapter speaks Agent Client Protocol v1 (pinned schema
`v1-7ec5185c.json`, AGY 1.5.2; permission and usage shapes re-verified
against installed agy 1.5.9) over bounded NDJSON JSON-RPC stdio through
the shared `native_stdio` transport: one process, correlated pending
calls, a bounded frame queue, and output drained before any loss
conclusion. Turn progress arrives as `session/update` notifications;
`session/request_permission` opens a Swarm approval request decided by
an explicit `approve`/`deny` (never auto-approved, see Model and
permission mapping). Every other agent-to-client request (`fs/*`,
`terminal/*`, `elicitation/create` and anything unknown) is explicitly
rejected as unsupported. ACP has no steer, inject or hook method. The
system-context and steer paths below use the separate native CLI
`PreInvocation` hook; they are not ACP operations. `active_turn`
injection remains unsupported.

The adapter uses `start_requires_input = false`: spawn, `initialize`
(protocol 1), then `session/new` — or `session/load` with the exact
retained session id on resume, which sends no input by itself. The
load reply is fenced in wire order behind the history replay: validated
matching-session updates before the reply are discarded (never
republished, never turned into live output), and only the ordered reply
publishes the startup outcome. `session.ready` is refused when the
stream already went unknown during startup. The
pinned `session/load` response deliberately carries no session id, so
the adapter retains the exact id from its correlated request and
validates it against the resume subject. Both paths validate the
returned modes and `model`/`thought_level` selectors, then verify every
config write by its echoed current value before committing
`session.ready`.

## Model and permission mapping

The ACP `model` selector carries base family ids only; the requested CLI
model (`gemini-3.8-flash-high`) translates to base `gemini-3.8-flash` plus
`thought_level = high` via `session/set_config_option` (documented suffixes
`-low`/`-medium`/`-high`/`-xhigh`, observed against agy 1.5.9), and both
writes are verified against the server's advertised options and echoed
values — an unadvertised id fails startup instead of being guessed. Session mode
is applied only when the profile mode names an actually advertised ACP
mode; any other requested mode (including CLI `auto`, which has no ACP
mode id) fails startup with a clear `unsupported` instead of being
silently ignored. The launch-time permission posture remains
`--dangerously-skip-permissions`; cwd is a launch fact (the server is
spawned in the workspace), since the session response echoes no cwd.

An omitted model preserves the native default: perform no model/thought-level
configuration writes, and verify the session-open `configOptions` current values
against their advertised select options. Requested model stays absent; effective
model/level come from those current values. Missing or unadvertised current selectors
fail startup as unknown. `agy models` supplies no default marker and is never used
to guess one. A retained configure overlay still realigns its exact selectors on resume.

A `session/request_permission` ask (`{sessionId, toolCall, options}`)
opens a Swarm approval request with a distinct bounded local id for each
native JSON-RPC request, bound to the exact generation and turn. The
native JSON-RPC id, tool call id and options remain separately retained
inside the plugin only while the native request is live. The request has kind
`approval` and the shared `ApprovalRequest`
public details: a redacted plain-text summary (title plus tool
name/kind and compact raw input, never the native record) and one
choice per usable offered option, `approve` for `allow_once`/
`allow_always` and `deny` for `reject_once`/`reject_always`
(`feedback_allowed: false`; unknown kinds are never offered).
`swarm approve`/`deny` map to the ACP outcome using those kinds: an
explicit `choice_id` must name an option whose kind matches the verb,
otherwise the verb takes the least-privilege default (`allow_once`
before `allow_always`, `reject_once` before `reject_always`); feedback
is rejected, and an unmatched verb/choice rejects before any native
write. The plugin writes `{outcome: {outcome: "selected", optionId}}`
and commits `request.resolved`; only a proven no-write keeps the ask
answerable, anything uncertain latches unknown without a rewrite.
Nothing is ever auto-approved: an unanswered ask keeps the turn busy
(and the Agent waiting) until the owner, the manager, or a cancel
resolves it. A validated permission ask associates the pending input
before `request.opened` is committed. Interrupt/stop latches cancellation
for the exact turn under the event lock and answers both existing and
subsequently arriving asks `cancelled` once (uncertain writes are never
repeated). The latch never carries into a new turn. Asks the
native ends without consuming are closed (`request.closed`) ahead of
the terminal commit. Verified live 2026-11-15 against installed agy
1.5.9: a `git log` ask approved (command ran, final answer returned),
denied (tool refused, empty turn honestly marked), and
interrupt-cancelled (ask closed, turn cancelled).

## Native mapping

Launch establishes the session before input; send dispatches one
`session/prompt` (`{sessionId, prompt:[{type:"text"}]}`) as a **fenced**
call: its terminal reply re-enters the ordered frame channel behind every
preceding `session/update`, so the exact prepared turn settles only
after those updates have been consumed — never from a direct pending
route, a sleep, or silence. A successful stdin flush proves transport
delivery only. Send returns `applied` only after the event owner commits
`turn.started` from matching-session native progress, a validated
permission ask, or the correlated fenced terminal response. A schema-valid
`usage_update` (both `used` and `size`) is early session progress: when one
input is outstanding in this uninterrupted process it can establish its
association without waiting for model text; idle/startup usage creates no
turn. This uses the [pinned ACP v1 schema](https://raw.githubusercontent.com/agentclientprotocol/agent-client-protocol/7ec5185c18ca928b48e457d2d8a163800be013c8/schema/v1/schema.json),
which defines session progress but guarantees neither an admission update
nor its timing. The supplied ctl6 observation (agy 1.5.9, 2026-11-15,
`usage_update` immediately before `turn.started`) has not been re-verified
on this branch: account quota is exhausted for approximately 150 hours.
The terminal `stopReason` settles the turn through
the event state, never from the send ack alone. The fenced registration
is held until that actual in-band terminal reply, an error, a stream
fault, stop, or drop — stream silence is not staleness and no
model-turn timer expires admitted work. `end_turn`/`max_tokens`/`max_turn_requests` complete the
turn, `cancelled` settles an interrupt, `refusal` fails it, and any other
stop reason is a native protocol gap. Assistant text is accumulated from
`agent_message_chunk` updates carrying text blocks only (other content
blocks contribute no text), bounded at 256 KiB, then
`final_text_unavailable:"event_budget"`. A reply within that budget but over the
persisted-event bound records one `message` coverage gap instead of
`message.completed` and the same marker; the turn still completes with its
`stopReason`. Thought and user chunks are
bookkeeping. A normal completion with no assistant text at all keeps its
native `stopReason` but carries
`final_text_unavailable:"empty_native_response"` (see Facts and recovery),
so silence is never presented as a normal answer. Send uses one 25 s
deadline for preparation, the native write and committed association,
inside the daemon's 30 s plugin RPC. Proven local no-write failures settle
as `rejected` (`native_not_dispatched`). A correlated ACP validation refusal
before any committed association also settles as `rejected` (`native_rejected`).
If the deadline elapses after a possible write, the send returns `unknown` and
the pending input and terminal fence remain live. An ambiguous native response
opens a stream gap. Neither case resends input; command retry and settlement
are owned by the daemon router and Store.

Tool calls map from `tool_call` and `tool_call_update` using the native
`toolCallId`. A delta can change only a tool introduced by a `tool_call` or
validated permission ask in the current prompt. Omitted title and status
inherit the last value for that call. AGY can finish an asynchronous tool
after its prompt has completed and emit unsolicited assistant or tool output.
Once session identity is verified, assistant chunks and tool notifications
received without a pending prompt record a `source.gap` scoped to `message` or
`tool`. This applies before the first turn and after a fenced terminal reply.
Each family reports at most one gap per idle episode. An unmatched tool delta
during a later prompt records at most one `tool` gap for that prompt. These
gaps leave the output unattributed. A confirmed coverage gap preserves stream
health and permits the next send on the same live ACP session. An unresolved
gap acknowledgement fences the stream.

**Interrupt** is the ACP `session/cancel` notification scoped to the exact
active Eliot turn (turn id required, stale turns reject). The command
proves dispatch, then waits (bounded) for the fenced reply's actual
settlement of that exact turn: only `stopReason = cancelled` proves the
interrupt and returns `applied`; natural completion, failure, or a
missing settlement stay `unknown` (retained, never resent). Generation,
session and turn races (e.g. the turn already ended) surface through
that settlement and snapshots, never through the interrupt ack. The
wait polls the shared turn state without holding its lock and performs
no callback work while holding it.

Busy stop first sends `session/cancel` once for its captured exact active turn,
answers that turn's pending permission asks as cancelled, and waits up to the
core stop interrupt bound for the fenced `session/prompt` terminal response.
Only native `stopReason: cancelled` proves interruption; other terminal reasons
keep their actual outcome, and missing evidence leaves the turn unknown.
Force skips this window. Stop then sends best-effort `session/close` for the retained session, then
closes stdin and drains stdout before settling the session; force targets
only owned custody. Resume creates a new Agent generation loading the
exact retained session id, without replaying the old prompt. Connected
verbs are launch/resume/send/configure/steer/interrupt/approve/deny/stop.
`configure` switches the live session's `model` and `thought_level`
selectors through `session/set_config_option` at configure time, proving
each write with its echoed current value (`model_verified`,
`effort_verified`); a requested value the session did not advertise
rejects before any write, never guessing, and a refused or unproven switch
stores nothing and never claims applied. An explicit `--effort` wins over a
model suffix; otherwise the suffix, then the session's tracked level,
selects the thought level, and the effective model id recombines base and
level. There is no direct plugin `inject` verb: `active_turn` inject is
unsupported, while system context flows through the shared `next_input`
(claimed at send) and `turn_safe_point` (claimed at `PreInvocation`)
points below. Approve and deny reply to the exact offered native
permission ask; the public details are the shared `ApprovalRequest`
(`swarm approve`/`deny`, optional `choice_id`, no `feedback`), and a lost
reply is unknown and never rewritten. Answer remains `unsupported`.
`profile.store` is unsupported: launch carries no native store binding.

## System context and steer hooks

The Antigravity CLI hooks documentation (accessed 2026-11-07; adapter
version metadata is 1.5.2) defines named hook groups. `PreInvocation` input
includes `conversationId`, zero-indexed `invocationNum`, and
`initialNumSteps` (trajectory step count). The current recommended output is
`{"injectSteps":[{"systemMessage":{"systemMessage":"..."}}]}`;
`ephemeralMessage` and `hookEphemeralMessage` are deprecated. The plugin
stages one named group (`eliot-system-context`) in a dedicated hook-only
directory (`<instance>-hooks/.agents/hooks.json`, a sibling of the private
instance directory holding nothing else: no tokens, logs, state, or sockets)
and passes that directory as the only `--add-dir` root, for both fresh starts
and resumed sessions; hook delivery itself is unchanged. The root is a
plain absolute path: agy builds `file://` URLs from its workspace
roots and rejects the escaped `?` of a Windows verbatim (`\\?\`) path
(observed against agy 1.5.3; installed CLI is now 1.5.9 and the plugin
still stages the root without that prefix),
dropping the whole root and its hooks without firing, so the staged root
is stored without that prefix (it names the identical directory). The real
workspace remains the ACP cwd, and the private instance directory is never a native
root. The adapter does not write global or user-project hook files, copy
authentication, or change login/native-store paths. The MCP config root described
below is separate from this discovery root. The staged hook config, its
`.agents` directory, and the discovery root are removed when the generation
ends. Requested and effective settings carry each native root with its purpose
(`native_roots.workspace` vs `native_roots.hook_discovery`).

Native tool-access restrictions stay at the existing posture: the ACP
`initialize` advertises no filesystem or terminal capability
(`fs.readTextFile`/`writeTextFile: false`, `terminal: false`,
`auth.terminal: false`; unchanged against installed agy 1.5.9, whose
`session/request_permission` round-trip this page bridges instead), so
`fs/*` and `terminal/*` agent-to-client requests stay explicitly
rejected. The CLI also offers `--sandbox`, which is not enabled: it would change
execution semantics beyond hook discovery. Same-user filesystem access is not
a hostile-agent boundary; the guarantee is that secrets and state are never
under a writable native root, not a sandbox.

Each managed ACP generation receives `ELIOT_AGY_HOOK_HELPER` pointing to the
current Eliot executable plus its own loopback endpoint
(`ELIOT_AGY_HOOK_ENDPOINT`) and random token (`ELIOT_AGY_HOOK_TOKEN`). The
private hook command invokes that same executable as a bare token
(`<exe> plugin antigravity-hook`) when its path contains no whitespace or
shell metacharacters. Other command paths remain unsupported until the
vendor executor's quoting behavior is proven. With no managed
endpoint/token, the helper writes `{}` and exits successfully.

On Windows, the Antigravity native child receives a `PATH` with the host's
`SystemRoot\System32` first, followed by the existing effective entries. A
controlled run showed the original npm-first `cmd` resolution sent hook shell
commands to Command Code; this child-only order resolves Windows `cmd.exe`
first without changing the daemon or other providers' environment.

Supported system inject: `next_input` context is claimed FIFO at send
and prepended to the prompt; `turn_safe_point` context is claimed FIFO at a
live `PreInvocation` and applied as nested `injectSteps` `systemMessage`
steps.
Direct `active_turn` inject is unsupported.

The helper captures its kernel `ProcessBirthIdentity { pid, start_time }` at
entry. The plugin verifies that tuple against the OS and accepts it only when
the process start is after the immutable pending-prompt opening boundary
captured before `session/prompt`. The retained callback binding includes the
conversation, Eliot generation, turn, opening boundary, and helper origin;
it is rechecked before `hook.event` can claim FIFO `turn_safe_point` context.
Late or expired callbacks are discarded before claim. Native `invocationNum`
and repeated prompt text are not turn identity. The kernel birth query is
implemented for Windows and Linux; other targets fail closed as unsupported.

The Core `hook.event` response and each steer HookOutput retain their shared
frame bounds; the private IPC frame reserves both bounds plus the complete
response envelope. Claimed context stays FIFO and is never cleared after the
claim. If the combined response cannot fit, context takes priority and steer
remains pending for a later callback in the same prompt. After the helper
confirms its stdout write, the plugin commits `hook.before_model` with the
exact Eliot turn and consumer on the existing event path. A context outcome
is included only when claimed context was written. The `PreInvocation` payload
carries no tool metadata (only conversation, invocation and step counts), so
`hook.before_model` facts carry no tool diagnostics.

`steer` retains its command id, generation, and captured turn while waiting
for a live `PreInvocation` in that prompt. Its host response is deferred while
the same host continues to serve interrupt, stop, shutdown, and other control
requests; an outer RPC timeout does not cancel the retained waiter. Steer
uses a `systemMessage` step, reports `applied` only after helper
stdout confirmation, and reports the mechanism
`antigravity.PreInvocation.injectSteps.systemMessage`. If the turn ends
first, the retained result is stale or unknown and is never resent. This is
the native PreInvocation point, not an ACP steer method or an `active_turn`
inject. Controlled families remain session/turn/message/tool, with
`hook.before_model` owned by the existing turn family.

## Facts and recovery

The terminal `session/prompt` response (`stopReason`) is authoritative
for turn completion, correlated with the single outstanding prompt
prepared before dispatch in the same uninterrupted process. A soft-denied
tool remains visible as a failed `tool_call_update` even under a
successful final turn. The pinned ACP schema names only `stopReason` on
`session/prompt` results, but installed `agy` 1.5.9 extends the response
with a per-turn `usage` object (`inputTokens`, `outputTokens`,
`cachedReadTokens`, `cachedWriteTokens`, `thoughtTokens`, `totalTokens`;
observed 2026-11-15, `gemini-4-argon-high`). The terminal attaches it as
turn-scoped `TurnUsage` (`scope: "turn"`) when usable; a missing or
malformed extension leaves turn usage null and never blocks completion.
Requested, verified and served model are separate facts and one never
stands in for another. When a normal stop reason (`end_turn`,
`max_tokens`, `max_turn_requests`) arrives with no assistant text at all,
the turn still completes with its native `stopReason`, but the terminal
carries `final_text_unavailable: "empty_native_response"` and no
`message.completed` precedes it, so the missing answer is never presented
as a normal reply. ACP v1 `usage_update` carries context occupancy
(`used` tokens currently in context, `size` context-window capacity) and
optional cumulative session `cost` with its currency/unit: session-scoped
facts, not per-turn input/output totals. The adapter ignores them for turn
attribution, so occupancy changes (compaction can reduce `used`) can never
read as token expenditure. The adapter folds `usage_update` into the
retained `SessionUsage` snapshot and commits it as `session.usage` when
accepted (source `antigravity:usage_update`), with `observed_at` captured at
acceptance: `used`/`size` replace the retained context
counters and a supplied cost replaces the retained cumulative cost with
its verbatim unit. No cumulative input/output/cache counters exist on the `usage_update`
surface (`cumulative_tokens` stays null), and occupancy/cost are never
mapped to `TurnUsage`: the session stream alone cannot supply per-turn
usage. Per-turn usage arrives only on the terminal prompt response above.
Live 2026-11-14 with installed `agy` 1.5.3
(`gemini-3.8-flash-high`, served as base `gemini-3.8-flash` plus
`thought_level high`): one Agent, one short task (read `task.txt`, reply
`42`), `end_turn` success with final text `42`. The checked surfaces
reported no usage: the prompt result was `stopReason` only (checked for a
`usage` object and `_meta` usage: absent), the `session/update` stream
carried `agent_message_chunk`/`tool_call`/`tool_call_update` (plus
`hook.before_model` from `PreInvocation`, whose input is only
`conversationId`/`invocationNum`/`initialNumSteps`) and no `usage_update`
kind, so no `session.usage` snapshot was committed (`session_usage: null`)
and the terminal carried no `usage` (turn `usage: null`, `usage --agent`
`turns_without_usage 1`, `sessions snapshots 0`,
`generations_without_snapshot 1`). Text length is never used to estimate
tokens.
Quota exhaustion has no structured signal in the observed ACP response.
Reproduced 2026-11-17 against installed `agy` 1.5.10 and
`gemini-4-argon-high`: an `agent_message_chunk` contains only
`⚠ Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 107h59m40s.`,
followed by `{"stopReason":"end_turn"}`, without an error, usage or `_meta`.
The adapter therefore uses the explicit text fallback in
[`quota::turn_restriction`](../../plugins/antigravity/src/quota.rs): only the
complete assembled assistant warning matches, allowing surrounding whitespace
and the warning glyph. Quoted examples, substrings, additional prose and
truncated answers do not match. This is vendor text matching, not a structured
ACP refusal. A matching normal/refusal terminal settles `failed` / `failure`,
origin `provider`, `native_error.code:"quota_exhausted"`; `native_status` still
preserves `end_turn`. Cancelled and unknown native stops retain their semantics.
The native h/m/s reset hint becomes `provider_restriction.retry_after` in Unix
milliseconds; an absent, malformed or overflowing hint stays null.
The existing exact-generation provider hold blocks that frozen profile/account;
reset expiry only permits an explicit probe and never clears a hold or replays
the refused input. Absence of token usage is never quota evidence. The warning
glyph is UTF-8 (`E2 9A A0`); a single-byte console decoder can garble it.

There is no replay cursor/history API. Snapshots expose retained stream
facts, not a native history poll. Loss means an explicit gap. Do not
replay conversation history or infer success from silence/process exit;
drain buffered stdout first. Unknown input/result is not retried.
For the exact active turn, `agent.snapshot` names its `turn_evidence`. The
turn is running while its fenced `session/prompt` awaits the reply on a
healthy stream and a live process. It is terminal from that reply (ACP
`cancelled` reads as universal `interrupted`; `native_status` keeps the raw
`stopReason`, and a missing reply keeps its `final_text_unavailable` marker, so a
recovered terminal carries it too), retained with its exact binding before any write for it, even
`turn.started` when the reply is the turn's first fact, so a failed write costs only
that fact. The snapshot reads the same state lock the reply's writes hold, so it
never sees a terminal mid-write. A read waiting on that lock holds the Agent's one
snapshot slot, so the monitor's other reads return `busy` and defer, and the
in-flight terminal write fences settlement (shared rules in the
[plugin protocol](../spec/plugin-protocol.md)). It is lost when the observed
process exit drained stdout without a reply. A gap or a lost stream proves
none of these, so the daemon monitor settles the turn `unknown` after its
bounded reads.

## Session MCP servers (managed Agents)

The bootstrap profile uses `mcp = "inherit"`. In that mode Antigravity discovers
the user and workspace configuration directly, including MCP servers bundled
with enabled plugins. Swarm adds its Agent-scoped server through ACP session
setup. The same discovery applies to retained profiles that predate the `mcp`
field. See the vendor docs for [MCP](https://antigravity.google/docs/mcp),
[plugin MCP servers](https://antigravity.google/docs/build-plugins/#mcp-servers)
and [customization discovery](https://antigravity.google/docs/customization-discovery/).

Before launch and resume the daemon checks the user, workspace and standard
plugin MCP files for Owner Swarm transports and reserved Swarm names.
This check includes disabled entries and the Gemini CLI `httpUrl` spelling.
AGY documents `serverUrl` and `url` for remote connections.
AGY 1.5.14 has no documented per-session exclusion for an individual
discovered server. A match refuses startup and identifies the server and file.
Plugin manifests or personal customization redirects that the daemon cannot
check refuse startup and identify the declaring file.

New custom profiles default to `minimal`. The daemon selects enabled `mcp_allow`
entries from the user `~/.gemini/config/mcp_config.json` and workspace MCP files.
When a user entry is allow-listed, a workspace entry with the same name
refuses startup. The daemon atomically publishes the prepared MCP file in the
private instance directory and removes abandoned MCP temporary files at startup.
The plugin installs that prepared file at
`<private-plugins>/<Agent>-agy/config/mcp_config.json` and selects the root with
`acp --gemini-dir`. Exclusion of the user MCP file and user plugins from the
effective ACP session inventory is unverified. Workspace MCP servers outside the allow list, workspace
plugin directories or manifests, and personal customization directory redirects
refuse startup with the source path when the CLI cannot suppress them.
A missing allowed name or an allowed owner Swarm transport also refuses startup.

The private Gemini root persists as part of the native scope for exact resume.
Fresh starts clear a stale private root before installing the prepared file.
The plugin removes the staged MCP file after native exit and retains native
conversations. `inherit` resumes in the original native store. The private
Gemini root is outside the `--add-dir` workspace roots. `--app-data-dir` stays
unset. AGY 1.5.10 rejected absolute application-data paths on Windows on
2026-11-17. Requested and effective settings report the policy and allow list.

The [vendor authentication docs](https://antigravity.google/docs/install?tab=cli)
document local silent sign-in through the OS keyring (Windows Credential Manager).
Isolation changes neither HOME nor that keyring. No token, auth file or owner settings
is copied, and no login flow is launched by the plugin. The existing OpenCode
credential import is a pattern for preserving native authentication under isolation;
AGY's shared keyring makes an export/import unnecessary.

Every managed ACP session carries the Agent-scoped swarm MCP server on the
documented session-setup surface: `session/new` — and `session/load` with
the exact retained session id on resume — sends
`mcpServers: [{name, command, args, env}]` with one stdio entry,
`eliot-swarm`, invoking the current Eliot executable as
`<exe> --data-dir <Agent-bound root> mcp --require-agent`. The entry shape
follows the official ACP v1 schema (`schema/v1/schema.json`: stdio entries
carry name, command (absolute path), args and env, all required, and every
Agent MUST support stdio). `env` stays empty by design: the stdio child
inherits the ACP server's environment, which already carries the
Agent-bound `ELIOT_AGENT_*` identity the daemon injected, so no token
crosses the session params. Identity is per Agent and per run — a start
without the matching inherited Agent generation identity fails
`not_allowed` before any native I/O, never falling back to Owner auth —
and no owner-global config is written. This is the same server command and
identity injection the Codex route override and the Claude MCP config file
use, delivered through this harness's own native MCP configuration.

Antigravity invokes the server over stdio via `call_mcp_tool`. The server exposes exactly one
generic tool, `swarm`, accepting `{ method, params }`. The stdio child inherits caller identity
from the process environment (`ELIOT_AGENT_ID`, `ELIOT_AGENT_GENERATION`, `ELIOT_AGENT_TOKEN`),
which [`eliot_daemon::client_call`](../../crates/eliot-daemon/src/daemon.rs) validates against the
running daemon. Partial, conflicting, or revoked managed identity fails closed; an unmanaged
owner caller uses the existing owner credential path. Administrative methods (such as `daemon.status`)
are rejected with `not_allowed` for managed Agent callers.

Wake and claim notices use the one neutral text every harness gets
(`N unread: mail.inbox, then mail.ack`); an Antigravity Agent resolves those
methods through this tool.

Readback: ACP exposes no MCP inventory on any checked surface
(`session/new` / `session/load` results carry no server list;
`session/update` carries message and tool-call deltas only), so the
effective readback stays `mcp_servers: null, mcp_servers_verified: false`
at readiness instead of echoing the requested entry. Proof that the Agent
reaches swarm is behavioral. Live 2026-11-15 with installed `agy` 1.5.9
(`gemini-3.8-flash-high`, served as base `gemini-3.8-flash` plus
`thought_level high`): one Agent received owner mail, read it
(`mail.inbox`) and acked it (`mail.ack`) through 15 native
`mcp_eliot-swarm_swarm` tool calls with no shell-CLI fallback, and stored
the reply (`42`) via `var.set` (`set_by agent:a1:1`, read back by the
owner). An agent-to-owner `mail.send` is rejected by design (`to` takes
Agent ids only), which surfaced as one failed tool call, never as an
inventory record.

## Project MCP consumption (manual)

An unmanaged Antigravity workspace can also discover Eliot Swarm via its native project plugin path:
`.agents/plugins/eliot-swarm/plugin.json` and `mcp_config.json`. The configuration declares
a stdio MCP server invoking `swarm --data-dir <PATH> mcp`.

Mutations carry a caller-retained ULID in `params.id`; MCP callers may omit it and the server mints a ULID
and returns it in the reply for exact retry. The server adds no automatic retries, defaults, or speculative rollbacks. Output frames and stdio line writes are
strictly bounded to [`MAX_RPC_FRAME_BYTES`](../../crates/eliot-daemon/src/daemon.rs) (4 MiB) via
[`BoundedLineWriter`](../../crates/eliot/src/mcp/bounded_io.rs).

## Permissions and final checks

Use explicit native permission settings and verified effective session
facts. No documented provider base-URL override is assumed; never
substitute another subscription/API route. L7 checks initialize
ordering, session verification, prompt correlation, interrupt settlement,
gaps, and actual permissions/model. Common wire/ack/custody behavior
stays in the plugin spec.

## Ad-hoc launch

`swarm launch --harness antigravity --model M --workspace W "prompt"` is
Owner-only and freezes a profile-equivalent snapshot from safe defaults
(`dangerously_skip_permissions = false`, explicit MCP `inherit`) plus flags. A model
may be omitted to preserve the verified ACP session default described above.
`cmd` is the installed `agy` CLI on `PATH` (it serves both `acp` and
`models`); there is no fallback. An IDE-only install reports `unsupported`
naming the IDE path with guidance to install `agy`: the IDE executable does
not implement the native ACP/model CLI and is never launched for discovery.
CLI identity is established before any discovery probe: a profile or ad-hoc
`cmd` must name the `agy` file itself (an `Antigravity.exe` cmd is refused
with a typed error and never launched), and a PATH `agy` whose bounded
`--version` output does not parse as a CLI triple is refused before any
`models`, quota or launch use. `swarm models --harness antigravity` returns the live
read-only catalogue from `agy models` (exact argv `models`, bounded time and
output, same environment construction as launch): tab-separated native ids
kept verbatim (for example `gemini-4-argon-medium`, `gemini-4-hydrogen-xhigh`,
`gemini-pro-orchestrator-g1-pro`, `claude-opus-5-5-high`, `gpt-oss-120b-medium`),
cached per scope with freshness and credential identity. The documented
effort suffixes (`-low`/`-medium`/`-high`/`-xhigh`) map to the ACP
`thought_level` selector at launch; any other suffix stays part of the base
id and is verified against the advertised session options instead of guessed.
A failing `agy models` child reports an honest typed error naming `models`.
## Agent skill delivery (`eliot-swarm`)

At staging, the plugin writes the daemon-materialized canonical skill
files into the private hook-discovery root beside `hooks.json`
(`<root>/.agents/skills/eliot-swarm/`), the same `--add-dir` directory
the native CLI already discovers hooks from. Both files are
daemon-authored static content, never instance state; the checkout and
the global skill locations stay untouched. A missing canonical copy
fails the start. Skill discovery from `--add-dir` roots is harness
behavior, verified live per release; where the CLI does not surface the
skill, the readiness transcript shows it and the fallback below applies.
Implementation:
[`plugins/antigravity/src/session.rs`](../../plugins/antigravity/src/session.rs)
(`stage_hook_config`).

## Minimal-context measurement (k61, 2026-11-16)

Private daemon, identical trivial turn, `gemini-3.8-flash-high`, installed
1.5.9, explicit `inherit`. ACP exposes no MCP
inventory, so the effective record stays null/unverified; the session carries
exactly the Agent-scoped server by construction. Turn usage: input 14,934 +
cache-read 16,837 + output 559 (total 32,330). A follow-up turn called
`api.identity` through the `swarm` tool (after one Owner approval) and
reported principal `agent`. `agy acp --help` and `agy mcp --help` expose no
isolation or tool-deferral surface, so both stay unsupported.

## Native subagents (`native_subagents` deny): unsupported

Checked against the installed Antigravity 1.5.9 binary (`agy --help`:
`--agent`, `--mode`, `--settings` for a session only, no subagent/task
disable flag): there is no verified per-session switch to turn off native
subagents, so `native_subagents = "deny"` on an Antigravity profile is
`unsupported` at config load (typed, matching the existing pattern for
unmapped fields). `allow` is accepted and preserves today's behavior.
Implementation: `crates/eliot-daemon/src/config.rs`
(`freeze_native_subagents`).

## Access levels (`--access ask|auto|full`)

Ad-hoc mapping, frozen in the snapshot and reported in effective settings:
`ask` and `auto` are `dangerously_skip_permissions=false`, keeping the ACP
`session/request_permission` round-trip decided with `swarm approve`/`deny`;
`full` is `dangerously_skip_permissions=true`, exactly the current full-access
skip. Implementation:
[`crates/eliot-daemon/src/adhoc.rs`](../../crates/eliot-daemon/src/adhoc.rs)
(`apply_adhoc_access`).


## Read-only provider quota

`plugins/antigravity/src/quota.rs` follows OpenCodex 2.84.0's Cloud Code Assist
quota pattern, without copied code. On Windows it reads the installed AGY
`gemini:antigravity` Credential Manager entry (`token.access_token`) read-only,
uses `loadCodeAssist` to read the existing quota project, then
`retrieveUserQuotaSummary` on `daily-cloudcode-pa.googleapis.com`. It uses the
installed 1.5.10 CLI fingerprint, verified from the vendor binary's
`codeassistclient.userAgent` and `CLIAuthProvider.GetUserAgentName`, rather than
the legacy `antigravity/1.0` header. With identical account/project/request,
that old header returns only Claude/GPT groups; the CLI header also returns
Gemini and the account's separate Gemini 4 Argon group (observed 2026-11-17).
The vendor's embedded `QuotaSummaryGroup` / `QuotaSummaryBucket` descriptors
define `groups`, `displayName`, `buckets`, `window`, `remainingFraction` and
`resetTime`. Recognized buckets become separately named Gemini, Gemini 4 Argon
and Claude/GPT 5-hour/weekly windows. A missing group remains absent; no legacy
model-availability response is substituted for account quota. Tokens, email and project
identifiers never leave the plugin. No credential refresh or onboarding is done.
Window-to-model mapping is documented-group only, never substring: the
`Gemini 4 Argon` group constrains models with an `argon` id token, so an
exhausted Argon weekly bucket skips Argon models but never a Hydrogen model;
an unrecognized provider group (projected as a `Model group` label) maps to
no model.
Other platforms currently report a supported environment error because their
native keyring reader is unimplemented; provider failures also remain supported
error states. Queries never start a turn.
