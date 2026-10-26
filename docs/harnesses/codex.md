# Codex app-server

Layer L2, first real harness. Rust plugin, default `per_agent`: one plugin + one
app-server per Eliot generation because tools inherit app-server environment. Shared
service remains one endpoint per agent unless upstream proves per-thread environment,
identity and isolated stop.

Facts checked 2026-10-25 against `openai/codex` main/release `rust-v0.164.0` and official
app-server/config/hook docs. App-server is experimental; commit schema from owner's
installed binary before implementation.

## Transport and custody

`codex app-server --listen stdio://` uses newline JSON-RPC-shaped messages without
`jsonrpc` field. Initialize with generated-schema capabilities and exact initialized
notification; do not use strict JSON-RPC codec requiring `jsonrpc:2.0`.

One app-server/thread is bound to one Eliot `(agent,generation)`, frozen profile,
workspace/account and per-agent custody. `CODEX_HOME` login is owner-managed; Eliot
never logs in or changes subscription/API/proxy route silently.

The plugin opens stable source epochs before publishing persisted facts:

- app-server stream owns session/turn/message/tool/request/goal families;
- optional per-agent descendant registration has a separate process-registration
  source;
- rollouts own only observe-only or explicit recovery after handoff.

Every persisted fact is commit-acknowledged with contiguous source sequence. Lost reply
resends identical envelope; vendor reconnect duplicate cannot fire lifecycle twice.

## Start and initial input

Codex supports session start without input:

1. `agent.start` → `thread/start` only;
2. validate effective thread id, cwd, model/provider/permissions;
3. open source families and commit `session.ready`;
4. return session success;
5. dispatch linked initial `send` separately via `turn/start`.

`start_requires_input=false`. Never combine launch/first turn. Lost thread-start reply
is read back or unknown; no replacement thread. Lost turn-start affects only linked
send. Native/HTTP write acceptance is not applied until exact Codex turn association.

Resume creates new Eliot generation/app-server and calls `thread/resume` for retained
canonical thread subject. Old prompt is never resent; optional text is linked send.

## Verb mapping

| Eliot | Codex |
| --- | --- |
| send | `turn/start`; Eliot command id may correlate where accepted, never vendor idempotency proof |
| steer | `turn/steer` with exact `expectedTurnId`; page history to end for readback |
| inject | exact steer while busy or explicit prepend-next-send while idle |
| interrupt | `turn/interrupt` exact turn; unknown never retried |
| stop | exact interrupt if active, clear native goal, close/end thread if available, then stop per-agent app-server custody |
| approve/deny | exact native request id and offered choice |
| answer | exact `requestUserInput` id |
| configure | supported turn/session fields, application point + effective readback |
| goal | native set/clear/status; generation change pauses projection |
| fork | native `thread/fork`; new thread is new canonical Agent row |

Force is exact only on exact custody backend. Degraded Unix rejects exact force. Stop
clears native goal because v1 observed stopped sessions waking.

## Events and readback

The app-server stream is authoritative while controlled. Map exact thread/turn/item/
request/update ids. One item producing multiple kinds needs distinct per-event IDs.
Final output commits before terminal completion.

Reconnect first reads daemon source watermark and native thread history. If app-server/
rollout cannot bridge exact native cursor, close source gapped rather than replaying old
completion. Empty active-list without terminal history becomes `not_active_unknown`,
not idle/completed.

Observe-only attach reads thread/rollout and never resumes/sends/goals/forks. Read is not
subscription; silence is not liveness.

## Native children and attribution

Subagent threads are registered from exact parent/child thread ids using filtered,
paginated `thread/list` plus notifications. Child scope/ref is canonical; unfiltered
list may hide children and pagination is not atomic, so coverage can be partial.

Current app-server process environment is shared by its threads. Until installed
upstream behavior proves per-thread child environment injection, manifest declares
`child_environment=false`: child row exists for observation/control verbs the native
protocol exposes, but child shell/CLI calls inherit parent Eliot token/Git identity and
are attributed to parent. Eliot never invents a distinct child token. L7 must test any
future claim before changing this.

Shared/app-server PID belongs to this per-agent custody. Under future shared service
mode the server PID would be `service:<name>` custody, never assigned to child/agent
sessions.

## Permissions and proxy

Confirm installed enum fields, then map normalized permissions to Codex sandbox/
approval policy. Requested/effective values remain separate.

OpenCodex/Responses route uses named provider with explicit `/v1`, Responses wire API,
named env key and required headers. Frozen per-agent binding never edits owner global
config. Account/proxy capacity are shared scopes. Main turn verifies only that request
path, not auxiliary/title/compaction/subagent calls.

## L7 cases

- lost `source.open`, ready and linked first-turn acknowledgements;
- effective model/provider/cwd mismatch;
- long-history pagination, stale expected turn and missing terminal → unknown;
- exact approvals/questions and goal clear on stop;
- reconnect vendor duplicate/source gap;
- child canonical subject, partial coverage and parent attribution with no child env;
- descendant process registration vs OS enumeration exactly once;
- Windows exact and Linux exact/degraded daemon death/force;
- direct/OpenCodex requested vs effective route and auxiliary calls;
- installed facts still unverified: initialized notification/settings, `-c` overrides,
  steer during approval, stdin-close and exact close API.

Useful v1 edge cases:
`dc6f70aa:crates/swarm-adapter-codex/src/lib.rs`; do not port journals/checkpoints.
