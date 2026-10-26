# Codex app-server

Layer L2, first real harness. Rust plugin, default `per_agent`: one plugin + one
app-server per Eliot generation because tools inherit the app-server environment. A
future shared service remains one endpoint per Agent unless upstream proves per-thread
environment, identity and isolated stop.

Facts checked 2026-10-25 against `openai/codex` main/release `rust-v0.164.0` and official
app-server/config/hook docs. App-server is experimental; generate and commit the schema
from the owner's installed binary before implementation.

## Transport and custody

`codex app-server --listen stdio://` uses newline JSON-RPC-shaped messages without a
`jsonrpc` field. Initialize with generated-schema capabilities and the exact initialized
notification; do not use a strict JSON-RPC codec requiring `jsonrpc:2.0`.

One app-server/thread is bound to one Eliot `(agent,generation)`, frozen binding,
workspace/account and per-Agent custody. `CODEX_HOME` login is owner-managed; Eliot
never logs in or silently changes subscription/API/proxy route.

Before `thread/start` or `thread/resume`, the plugin opens source epochs:

- app-server stream for session/turn/message/tool/request/Goal families;
- optional verified descendant-registration source;
- rollout source only for observe-only or explicit recovery after atomic handoff.

The live stream uses stable per-event native ids where the installed schema supplies
them. Rollout recovery uses its qualified cursor; if neither identity nor cursor can
bridge an interval, the replacement source is opened with a gap. Every persisted fact
is commit-acknowledged with contiguous Eliot source sequence.

## Start and initial input

Codex supports session start without input:

1. persist/open all initial source epochs;
2. `agent.start` → `thread/start` only;
3. validate effective thread id, cwd, model/provider/permissions;
4. commit-acknowledge `session.ready`;
5. return session success;
6. dispatch linked initial `send` separately through `turn/start`.

`start_requires_input=false`. Never combine launch and first turn. A lost
`thread/start` reply is read back or unknown; no replacement thread. A lost
`turn/start` reply affects only linked send. Native write acceptance is not applied until
exact Codex turn association.

Resume opens new-generation source epochs first, starts a new per-Agent app-server and
calls `thread/resume` for the retained canonical subject. Old prompt is never resent;
optional text is a linked send.

Large immutable system notes arrive by `ArtifactRef`, not inline protocol bytes.

## Verb mapping

| Eliot | Codex |
| --- | --- |
| send | `turn/start`; Eliot command id may correlate where accepted, never vendor idempotency proof |
| steer | `turn/steer` with exact `expectedTurnId`; page history to the end for readback |
| inject | exact steer while busy or explicit prepend-next-send while idle |
| interrupt | `turn/interrupt` exact turn; unknown never retried |
| stop | exact interrupt if active, clear native Goal, close/end thread if available, then stop per-Agent app-server custody |
| approve/deny | exact native request id and offered choice |
| answer | exact `requestUserInput` id |
| configure | supported turn/session fields, application point + effective readback |
| Goal | native set/clear/status; generation change pauses Eliot projection |
| fork | native `thread/fork`; new thread is a new canonical Agent row |

Force is exact only on an exact custody backend. Degraded Unix rejects exact force.
Stop clears native Goal because v1 observed stopped sessions waking.

## Events and readback

The app-server stream is authoritative while controlled. Map exact thread/turn/item/
request/update identities. One item producing several normalized kinds needs distinct
per-event ids; do not reuse a bare item id for repeated updates. Persisted event/final
text obey the 64 KiB bound and point to rollout/native detail when truncated. Final
output commits before terminal completion.

Reconnect reads daemon source watermark and native thread history. A matching
native-event receipt or qualified rollout cursor suppresses old lifecycle. If the
installed app-server/rollout cannot bridge a boundary, replace the source atomically with
`state=gap`; never replay an old completion. Empty active-list without terminal history
is `not_active_unknown`, not idle/completed.

Observe-only attach reads thread/rollout without resume/send/Goal/fork. Discovery and
thread/history reads are fully paged below plugin frame limits. Silence is not liveness.

## Native children and attribution

Subagent threads register from exact parent/child thread ids using filtered, paginated
`thread/list` plus notifications. Child scope/ref is canonical; unfiltered list may hide
children and pagination is not an atomic snapshot, so family coverage can be partial and
reported as such.

Current app-server process environment is shared by its threads. Until installed
upstream behavior proves per-thread child environment injection, manifest declares
`child_environment=false`: a child row may be observable/natively controllable through
the parent plugin, but `routing_enabled=false`; child shell/CLI/Git activity is
attributed to parent and no distinct token is minted. A vendor child beyond controller
depth/tree guards is still recorded with reduced authority/wake.

This per-Agent app-server PID belongs to parent Agent custody. Under future shared mode,
server PID belongs to `service:<name>`, never to child/session Agents.

## Permissions and proxy

Confirm installed enum fields, then map normalized permissions to Codex sandbox/
approval policy. Requested/effective values remain separate.

OpenCodex/Responses route uses a named provider with explicit `/v1`, Responses wire API,
named env key and required headers. Frozen binding never edits owner global config.
Account/proxy capacity are shared scopes. Main turn proves only that request path, not
auxiliary/title/compaction/subagent calls.

## L7 cases

- source open committed before `thread/start`/`thread/resume`;
- lost source-open, thread-start, ready and linked first-turn acknowledgements;
- effective model/provider/cwd mismatch;
- long-history/discovery paging and frame bounds;
- stale expected turn and missing terminal → unknown;
- exact approvals/questions and Goal clear on stop;
- event-id replay, rollout cursor handoff and explicit gap;
- child canonical subject, guard behavior, partial coverage and parent attribution with
  no child environment;
- descendant registration vs OS enumeration exactly once;
- Windows exact and Linux exact/degraded daemon death/force;
- direct/OpenCodex requested vs effective route and auxiliary calls;
- installed facts still unverified: initialized notification/settings, `-c` overrides,
  steer during approval, stdin-close and exact close API.

Useful v1 edge cases:
`dc6f70aa:crates/swarm-adapter-codex/src/lib.rs`; do not port journals/checkpoints.
