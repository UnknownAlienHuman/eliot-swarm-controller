# Codex app-server

Layer L2, first real harness. Rust plugin, default `per_agent`: one plugin + one
app-server per Eliot generation because tools inherit app-server environment. Shared
service mode remains per-agent endpoint unless upstream proves per-thread environment
isolation.

Facts checked 2026-10-25 against `openai/codex` main/release `rust-v0.164.0` and
official app-server/config/hook docs. App-server is experimental; commit the schema
from the owner's installed binary before implementation.

## Transport

`codex app-server --listen stdio://` uses newline JSON-RPC-shaped messages without the
`jsonrpc` field. Initialize with generated-schema capabilities and send the exact
initialized notification from that schema. Do not use a strict JSON-RPC codec requiring
`jsonrpc:2.0`.

One app-server/thread is bound to one Eliot `(agent,generation)`, profile environment,
workspace and account. `CODEX_HOME` login is owner-managed; Eliot never runs login or
silently changes subscription/API/proxy route.

## Start and initial input

Codex supports session start without input:

1. `agent.start` → `thread/start` only;
2. read/validate effective thread id, cwd, model/provider/permissions;
3. emit `session.ready` and return session success;
4. dispatch the linked initial `send` separately through `turn/start`.

`features.start_requires_input=false`. Never combine launch and first turn in one
outcome. A lost `thread/start` reply is read back or remains unknown; no replacement
thread. A lost `turn/start` reply affects only the linked send.

Resume creates a new Eliot generation/app-server and calls `thread/resume` for the
retained canonical thread subject. Old prompt is never resent; optional resume text is
a separate linked send.

## Verb mapping

| Eliot | Codex |
| --- | --- |
| send | `turn/start`; use Eliot command id as native correlation where accepted, never as vendor idempotency proof |
| steer | `turn/steer` with exact active `expectedTurnId`; page thread turns to the end for readback |
| inject | exact steer while busy or explicit prepend-next-send while idle; report mechanism |
| interrupt | `turn/interrupt` for exact turn; unknown is not retried |
| stop | exact interrupt if active, clear native goal, close/end thread if available, then stop this app-server custody unit |
| approve/deny | exact command/file/permission request reply and offered native choice |
| answer | exact `requestUserInput` id |
| configure | supported turn/session fields; report `now|next_turn|next_session` and effective readback |
| goal | native `thread/goal/set|clear` and status mapping; generation change pauses Eliot projection |
| fork | native `thread/fork`; resulting thread is a new canonical Agent row |

`stop --force` is exact only on an exact custody backend. Degraded Unix fallback rejects
that guarantee. Stop clears native goal because v1 observed stopped sessions waking.

## Events/source ownership

The controlled app-server stream owns live session/turn/message/tool/request/goal
families. Rollouts are for discovery/observe-only or explicit cursor recovery, never a
concurrent duplicate source.

Map exact thread/turn/item/request ids. One native item may yield different normalized
kinds; use unique event/update ids so Store dedupe does not erase valid transitions.
A reconnect suppresses committed ids. If app-server/rollout cannot prove a cursor
boundary, report a gap rather than replaying completion.

Children are registered from exact parent/child thread ids using filtered, paginated
`thread/list` plus notifications. Child scope/ref must be canonical; unfiltered lists
can hide subagents and are not atomic, so coverage may be partial.

Observe-only attach reads thread/rollout and never resumes/sends/goals/forks. Read is
not subscription. Silence is not liveness.

## Permissions and proxy

Confirm installed enum fields, then map normalized permissions to Codex sandbox/
approval policy. Requested and effective values remain separate.

OpenCodex/Responses-compatible route uses a named provider with explicit `/v1`,
`wire_api=responses`, named environment key and required headers. Launch overrides are
per-agent; never edit owner global config. Account and proxy capacity are shared scopes.
A successful main turn verifies only that request path, not titles/compaction/subagents.

## L7 cases

- thread start vs linked first turn at every lost-reply point;
- effective model/provider/cwd mismatch;
- long-thread pagination and stale expected turn;
- exact approvals/questions and goal wake-on-stop prevention;
- duplicate reconnect events/source gap;
- child coverage and canonical subject;
- daemon death/force on Windows exact and Linux exact/degraded backends;
- direct and OpenCodex route, requested vs effective billing;
- upstream facts still unverified: initialized notification/setting fields, `-c`
  override behavior, steer during approval, stdin-close behavior and exact close API.

Useful v1 protocol edge cases are at
`dc6f70aa:crates/swarm-adapter-codex/src/lib.rs`; do not port journals/checkpoints.
