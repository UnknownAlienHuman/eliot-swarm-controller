# Codex app-server

Layer L2, first real harness. Plugin `plugins/codex` is Rust/built-in and
`mode=per_agent`: one plugin process and one app-server process per controlled Eliot
Agent generation.

Facts checked 2026-10-25 against `openai/codex` main/release `rust-v0.164.0` and the
app-server/config/hook documentation. The protocol is experimental; generate schema
from the installed binary immediately before implementation.

## Sources

- app-server protocol: upstream `codex-rs/app-server-protocol` (`common.rs`, v2 thread/
  turn types, RPC definitions);
- installed schema: `codex app-server generate-json-schema` / `generate-ts`;
- official app-server, hook and configuration documentation.

## Transport, identity and custody

- launch `codex app-server --listen stdio://`;
- newline-delimited JSON shaped like JSON-RPC but without mandatory `jsonrpc:"2.0"`;
- initialize, then initialized notification from the generated installed schema;
- one app-server may technically host many threads, but every tool inherits one process
  environment. Eliot therefore uses one app-server per controlled Agent generation so
  Agent token, Git identity, account/profile and proxy route remain exact;
- native subagent threads inside it share that environment: they are child Agents with
  `own_identity=false` and act through the parent plugin;
- plugin/app-server/tool tree belongs to the Agent Job/process group.

Canonical subject:

```text
native_scope = codex:<canonical CODEX_HOME or equivalent installed store identity>
native_ref   = exact Codex thread id
```

First start creates generation 1. Resume starts a new plugin/app-server and calls
`thread/resume` for the same subject as the next Eliot generation. A later service-mode
transport reconnect may preserve generation only when exact current controller/source
ownership is proven; uncertain/new takeover increments it. Every app-server request and
normalized event carries Eliot generation in the plugin envelope.

## Source ownership and recovery

For a controlled stdio generation, app-server stream owns `session`, `turn`, `message`,
`tool`, `request`, `child`, `goal` and `usage`. L2 does not install hooks or tail rollout
lifecycle beside it.

Before `thread/start`/`thread/resume`, the plugin arms its app-server reader and receives
the committed source-owner plan. `session.ready` commits before start success and before
the first `turn.started`.

The stdio stream has no qualified replay cursor. If it breaks while a service-backed
app-server remains, the replacement source starts at live edge, records `source.gap` and
uses exact `thread/read`/turn/request readback to reconstruct current projection; it does
not republish old lifecycle history.

Rollout JSONL is used for discovery/observe-only sessions or explicit recovery after a
source handoff. Its opaque exclusive cursor is the committed file identity+byte offset.
It resumes strictly after that offset. A complete unparsable record becomes a gap.
Rollout and live stream never own the same family concurrently.

## Authentication and route

The app-server uses the login/config under its profile's `CODEX_HOME`; owner performs
login. Eliot never starts an interactive auth flow and never deletes credentials on
usage/5xx errors.

Requested model/provider/cwd/permissions are compared with effective start/readback
facts. A mismatch fails the command; Eliot does not silently adopt a substituted route.

## Verb mapping

Every operation below is additionally bound to Eliot Agent generation. Turn controls
carry exact Codex turn id; replies carry exact native request id.

| Eliot verb | Codex mechanism | Settlement |
| --- | --- | --- |
| `launch` | `thread/start` without user prompt | emit/commit `session.ready` with thread id/effective settings, then start command applies |
| linked first input / `send` | `turn/start` | applied only when `turn/started` associates this command with exact turn; client message id is correlation, not idempotency |
| `steer` | `turn/steer` + `expectedTurnId` | wrong/ended turn rejects/fails; never moves into another turn |
| `inject` | exact steer while busy, otherwise prepend to next `turn/start` | report actual mechanism/application point |
| `interrupt` | `turn/interrupt` for captured turn | unknown is read back from that turn and never retried |
| `stop` | interrupt exact active turn if needed, clear native Goal, then end this per-Agent app-server | applied only after bound generation ended; force targets only its custody |
| `resume` | new Eliot generation + new app-server + `thread/resume` | old input never resent |
| `fork` | `thread/fork` | resulting thread is a new native subject/Agent |
| `approve|deny` | exact command/file/permissions request reply | accept / accept-for-session / decline according to offered native choices |
| `answer` | exact `requestUserInput` reply | unknown request type is rejected/typed error, never empty default |
| `goal.*` | native thread Goal methods | status maps to Eliot Goal; stop clears it |
| `configure` | per-turn fields where supported; otherwise next session | report `applies`; verify installed schema/readback |

Permissions map to installed Codex sandbox/approval enums. Confirm enum names from the
generated schema; requested and effective values are both shown.

## Event normalization

| Native fact | Eliot fact |
| --- | --- |
| established thread/start readback | one `session.ready` |
| exact turn start/end | `turn.started` / `turn.completed` with turn id |
| item start/end | tool/message/file/plan facts keyed by item id + kind |
| deltas | ephemeral message/tool delta |
| approval/user-input request | exact request event and pending projection |
| request resolved | exact resolution |
| provider/native error | turn failure, provider limit or fatal session error according to scope |
| subagent thread/provenance | canonical child registration with parent generation |

Final assistant text commits before terminal turn. Intermediate statuses and process
exit are not terminal turn events. Duplicate native ids with another canonical payload
are source-integrity failures.

Child discovery uses exact parent/ancestor thread ids and paged filters; never count,
title/cwd/time heuristics. Parent completion does not imply child completion.

## Observe-only behavior

- `thread/read` and rollouts are read-only; do not call resume/Goal/fork/send merely to
  observe;
- route by exact thread id and store scope;
- pagination is not an atomic snapshot; coverage limitations are shown as source health/
  gaps;
- connection silence is not completion.

## Hooks

Codex documents tool, permission, prompt, subagent, compact, stop/interrupt and session
hooks. Do not enable a hook for lifecycle already owned by app-server stream. Add one only
for a proven missing fact or immediate decision, assign that family explicitly and keep
configuration session-scoped.

## OpenCodex proxy

Use a named Codex provider pointed at the explicit OpenCodex `/v1` endpoint; inject
configuration per Agent launch, never edit owner global config. Client/admin credentials
remain separate environment references. Effective provider/endpoint must be observed;
requested URL alone is not proof.

## Retained failure cases for L7

- lost `thread/start` reply: no replacement thread;
- lost `turn/start` reply: no repeated prompt;
- stale generation/turn/request rejected before native I/O;
- long history read pages to the end;
- live stream vs rollout never duplicate lifecycle;
- stream break with no replay cursor records gap and readbacks current state;
- stopped generation clears native Goal and late old events cannot wake replacement;
- child threads share parent environment and are not given fake independent identity.
