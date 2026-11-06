# Antigravity CLI (`agy`)

Optional L6 adapter. One warm `agy` process belongs to one Eliot Agent generation and
custody unit. Source: [google-antigravity/antigravity-cli](https://github.com/google-antigravity/antigravity-cli).
The manifest's native version (`1.5.0`) is diagnostic metadata, not a startup version
gate. Startup validates the native `init` facts.

## Native identity and transport

Subject: `(canonical auth/config scope, exact conversation_id)`.

```text
agy --input-format stream-json --output-format stream-json --model M
    --dangerously-skip-permissions=false|true [--mode accept-edits|auto|plan]
    [--conversation ID]
```

One user line and at most one input in flight; stdout init/step_update/result. Results
without input ids are correlated only within that same uninterrupted process and its
single outstanding prompt. Late old stdout is stale. Native child discovery and control
are unsupported.

The native process emits `init` before input. The adapter uses
`start_requires_input = false`: arm stdout, validate the conversation id, model and cwd,
then commit `session.ready` before accepting a separate send. Resume must retain the
exact conversation id and sends no input by itself.

A `step_update` with `step_type = "system_message"` or a no-content `checkpoint`
is bookkeeping metadata, including when idle, resuming, or mid-turn. Validate its
conversation before handling it, and handle it without input association. It does
not acknowledge input, create a turn, append assistant text or complete a turn.
A checkpoint carrying modeled tool payload ([`StepUpdate` DTO](../../plugins/antigravity/src/native/types.rs))
or unknown step type still produces an explicit gap with bounded, redacted structural
diagnostics.

## Native mapping

Launch establishes the session before input; send writes one user record to that warm
process. Local write acceptance is not turn completion. Stop closes stdin and drains
stdout before settling the session; force targets only owned custody. Resume creates a
new Agent generation with `--conversation ID`, without replaying the old prompt.
The adapter exposes no headless approval/question reply channel. Eliot Goal is separate
from native turn control. Slash/foreign control messages are not sent into the stream.

## Eliot profile slice

`harness = "antigravity"` with explicit `cmd` and exact `model`; optional `mode`
(`plan`, `auto`, `accept-edits`) and `dangerously_skip_permissions` (default
false). `effort` is rejected. Connected verbs are launch/resume/send/stop; steer,
inject, interrupt, configure, approve, deny and answer are `unsupported`. Controlled
families are session/turn/message/tool with no request family. `profile.store` is unsupported:
launch carries no native store binding.

## Facts and recovery

The terminal `result` is authoritative for turn completion and can settle its single
outstanding prompt even if no intermediate update was emitted.
SUCCESS/ERROR/INVALID/CANCELED/INTERRUPTED map from the installed schema;
WAITING/RUNNING are nonterminal. A soft-denied tool remains visible even under a successful
final result. Cumulative usage replaces the previous session counter. Requested,
effective and served model are separate facts; one never stands in for another.

There is no replay cursor/history API. Snapshots expose retained stream facts, not a
native history poll. Loss means an explicit gap. Do not replay conversation history or
infer success from silence/process exit; drain buffered stdout first. Unknown input/result
is not retried.

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

Use explicit native permission settings and effective init facts. No documented provider
base-URL override is assumed; never substitute another subscription/API route. L7 checks
init ordering, single-input correlation, late output, gaps, soft denial, children and
actual permissions/model. Common wire/ack/custody behavior stays in the plugin spec.
