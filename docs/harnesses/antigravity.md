# Antigravity CLI (`agy`)

Optional L6 adapter. One warm `agy acp` process belongs to one Eliot Agent
generation and custody unit. Source: [google-antigravity/antigravity-cli](https://github.com/google-antigravity/antigravity-cli).
The manifest's native version (`1.5.2`) is diagnostic metadata, not a
startup version gate. Startup validates the ACP protocol version and the
advertised session selectors.

## Native identity and transport

Subject: `(canonical auth/config scope, exact ACP sessionId)`.

```text
agy acp --stdio --dangerously-skip-permissions=false|true
```

The adapter speaks Agent Client Protocol v1 (pinned schema
`v1-7ec5185c.json`, AGY 1.5.2) over bounded NDJSON JSON-RPC stdio through
the shared `native_stdio` transport: one process, correlated pending
calls, a bounded frame queue, and output drained before any loss
conclusion. Turn progress arrives as `session/update` notifications;
agent-to-client requests (`fs/*`, `terminal/*`, `session/request_permission`,
`elicitation/create`) are always explicitly rejected as unsupported — never
auto-approved. ACP has no steer, inject or hook method. The system-context
and steer paths below use the separate native CLI `PreInvocation` hook;
they are not ACP operations. `active_turn` injection remains unsupported.

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
`thought_level = high` via `session/set_config_option`, and both writes
are verified against the server's advertised options and echoed values —
an unadvertised id fails startup instead of being guessed. Session mode
is applied only when the profile mode names an actually advertised ACP
mode; any other requested mode (including CLI `auto`, which has no ACP
mode id) fails startup with a clear `unsupported` instead of being
silently ignored. The launch-time permission posture remains
`--dangerously-skip-permissions`; cwd is a launch fact (the server is
spawned in the workspace), since the session response echoes no cwd.

## Native mapping

Launch establishes the session before input; send dispatches one
`session/prompt` (`{sessionId, prompt:[{type:"text"}]}`) as a **fenced**
call: its terminal reply re-enters the ordered frame channel behind every
preceding `session/update`, so the exact prepared turn settles only
after those updates have been consumed — never from a direct pending
route, a sleep, or silence. Send returns `applied` on the first
associated update; the terminal `stopReason` settles the turn through
the event state, never from the send ack alone. The fenced registration
is held until that actual in-band terminal reply, an error, a stream
fault, stop, or drop — stream silence is not staleness and no
model-turn timer expires admitted work. `end_turn`/`max_tokens`/`max_turn_requests` complete the
turn, `cancelled` settles an interrupt, `refusal` fails it, and any other
stop reason is a native protocol gap. Assistant text is accumulated from
`agent_message_chunk` updates (bounded at 256 KiB, then
`final_text_unavailable:"event_budget"`); thought and user chunks are
bookkeeping. Tool calls map from `tool_call`/`tool_call_update` with the
vendor `toolCallId` as the item id. Tool updates are partial deltas:
title and status merge into the known per-id state before any event is
derived, so an omitted field inherits the last known value and only a
delta with no known status at all is a protocol gap.

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

Stop sends a best-effort `session/close` for the retained session, then
closes stdin and drains stdout before settling the session; force targets
only owned custody. Resume creates a new Agent generation loading the
exact retained session id, without replaying the old prompt. Connected
verbs are launch/resume/send/steer/interrupt/stop. Inject (including
`active_turn`), configure, approve, deny and answer remain `unsupported`.
`profile.store` is unsupported: launch carries no native store binding.

## System context and steer hooks

The Antigravity CLI hooks documentation (accessed 2026-11-07; adapter
version metadata is 1.5.2) defines named hook groups. `PreInvocation` input
includes `conversationId`, zero-indexed `invocationNum`, and
`initialNumSteps` (trajectory step count); output `injectSteps` accepts
`ephemeralMessage`. The plugin writes one named group to the generation's
private `.agents/hooks.json` and passes that private directory with
`--add-dir`; the real workspace remains the ACP cwd. Root's version-pinned
ACP log shows both the private Eliot group and a temporary probe group
loaded, then shows the helper command failing because backslash-escaped
quotes around the executable were not recognized. The log does not establish
which layer introduced those codepoints; Root is inspecting the frozen
binary's generated command. This proves customization loading for the tested
`agy acp` path, but not helper execution or event receipt. Root removed the
temporary global probe configuration; the adapter
does not write global or user-project hook files, copy authentication, or
change login/native-store paths.

Each managed ACP generation receives `ELIOT_AGY_HOOK_HELPER` pointing to the
current Eliot executable plus its own loopback endpoint and random token.
The private hook command invokes that same executable as a bare token when
its path contains no whitespace or shell metacharacters. Other command paths
remain unsupported until the vendor executor's quoting behavior is proven.
With no managed endpoint/token, the helper writes `{}` and exits successfully.
The Root probe completed a native source-review task but emitted zero
`hook.before_model` events; no context or steer was queued, and Root found no
native source gap. The remaining runtime gate is a confirmed private-helper
receipt after correcting the command token; hook load messages alone do not
prove helper execution.

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
is included only when claimed context was written.

`steer` retains its command id, generation, and captured turn while waiting
for a live `PreInvocation` in that prompt. Its host response is deferred while
the same host continues to serve interrupt, stop, shutdown, and other control
requests; an outer RPC timeout does not cancel the retained waiter. Steer
uses a separate `ephemeralMessage` step, reports `applied` only after helper
stdout confirmation, and reports the mechanism
`antigravity.PreInvocation.injectSteps.ephemeralMessage`. If the turn ends
first, the retained result is stale or unknown and is never resent. This is
the native PreInvocation point, not an ACP steer method or an `active_turn`
inject. Controlled families remain session/turn/message/tool, with
`hook.before_model` owned by the existing turn family.

## Facts and recovery

The terminal `session/prompt` response (`stopReason`) is authoritative
for turn completion, correlated with the single outstanding prompt
prepared before dispatch in the same uninterrupted process. A soft-denied
tool remains visible as a failed `tool_call_update` even under a
successful final turn. Cumulative usage is not reported by ACP prompt
responses, so snapshots carry `usage: null` and terminal facts carry no
usage counters; requested, verified and served model are separate facts
and one never stands in for another.

There is no replay cursor/history API. Snapshots expose retained stream
facts, not a native history poll. Loss means an explicit gap. Do not
replay conversation history or infer success from silence/process exit;
drain buffered stdout first. Unknown input/result is not retried.

## Project MCP consumption

A managed Antigravity workspace discovers Eliot Swarm via its native project plugin path:
`.agents/plugins/eliot-swarm/plugin.json` and `mcp_config.json`. The configuration declares
a stdio MCP server invoking `swarm --data-dir <PATH> mcp`.

Antigravity invokes the server over stdio via `call_mcp_tool`. The server exposes exactly one
generic tool, `swarm`, accepting `{ method, params }`. The stdio child inherits caller identity
from the process environment (`ELIOT_AGENT_ID`, `ELIOT_AGENT_GENERATION`, `ELIOT_AGENT_TOKEN`),
which [`eliot_daemon::client_call`](../../crates/eliot-daemon/src/daemon.rs) validates against the
running daemon. Partial, conflicting, or revoked managed identity fails closed; an unmanaged
owner caller uses the existing owner credential path. Administrative methods (such as `daemon.status`)
are rejected with `not_allowed` for managed Agent callers.

Mutations require caller-supplied ULIDs in `params`; the server adds no auto-generated identifiers,
automatic retries, defaults, or speculative rollbacks. Output frames and stdio line writes are
strictly bounded to [`MAX_RPC_FRAME_BYTES`](../../crates/eliot-daemon/src/daemon.rs) (4 MiB) via
[`BoundedLineWriter`](../../crates/eliot/src/mcp/bounded_io.rs).

## Permissions and final checks

Use explicit native permission settings and verified effective session
facts. No documented provider base-URL override is assumed; never
substitute another subscription/API route. L7 checks initialize
ordering, session verification, prompt correlation, interrupt settlement,
gaps, and actual permissions/model. Common wire/ack/custody behavior
stays in the plugin spec.
