# Core model

`eliot-core` contains pure types/reducers. `eliot-store` owns SQLite and allocation.
Wire/JSON names use `snake_case`. Timestamps are signed Unix milliseconds UTC.

## Scalar and identifier bounds

SQLite integers are signed 64-bit. Every integer stored in SQLite is therefore in
`0..=i64::MAX`; Rust may expose a checked `u64` wrapper but must reject larger values.
Generation `0` is reserved for an agent whose first native incarnation has not started.
All real agent/message/event ids and live generations are positive.

| Id | Form | Source | Rule |
| --- | --- | --- | --- |
| `AgentId` | `a<positive i64>` | Store counter | Stable logical agent row. |
| `generation` | checked non-negative integer | Store, per agent | `0` before first start; first incarnation `1`; every explicit resume increments. Exact re-attach preserves it. |
| external `CommandId` | UTF-8 string, 1–128 bytes | caller/CLI | Same id + canonical payload returns the original admission; different payload is invalid. |
| internal derived id | `d_<base32 digest>` | daemon | Fixed-length deterministic digest of a domain tag plus length-delimited canonical components. Parent/run/selector identity remains in `cause`/columns, not in an unbounded concatenated id. |
| event `seq` | positive checked integer | Store counter | Total order of persisted events. |
| `MessageId` | `m<positive i64>` | Store counter | Mailbox identity. |
| `native_scope` / `native_ref` | bounded opaque strings | plugin | Together identify one canonical native subject. Scope must change when the vendor store/server identity resets or can reuse refs. |

The deterministic id helper is used for selector children, linked initial sends, rule
actions and every other derived write. Never form `<128-byte parent>/<suffix>` and
silently exceed the public bound.

## Canonical native subject

One (`native_scope`,`native_ref`) maps to exactly one Eliot `Agent` row for its whole
history. Multiple API clients observe that row through subscriptions; they do not
create duplicate observe-only agents. `attach` returns the existing row, or upgrades
its control state after exact native attach. `detach` changes that same row to
observe-only.

A plugin registering a native child supplies both child scope and ref. If the vendor's
child ids are only parent-local, the plugin constructs a scope that includes the exact
parent native subject. Parent generation is provenance, not a substitute for native
scope.

## Agent

```text
Agent {
  id, generation, name?, project,
  parent?, root, depth,
  origin: eliot | native | external,
  harness, profile,
  native_scope?, native_ref?,
  workspace?, branch?, base_ref?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending: [Request],                  // exact generation + turn + request id
  goal?,                               // current native/Eliot goal projection
  effective?,                         // bounded effective settings and source owners
  labels: map<string,string>,
  control: self | tree | project,
  observe_only: bool,
  created_at, updated_at
}
```

The Store keeps only a hash of the current generation's routing token. The raw token is
created in memory, injected once, and treated as a known secret for masking. Launch or
resume rotates it. A stale/missing token supplied by an agent request is rejected; it
never becomes owner authority.

Native children are first-class agents, but only plugin-declared `child_verbs` are
routable. When a harness cannot inject a distinct child token, child CLI calls are
attributed to the parent and `show` reports the limitation.

### States

Live states: `starting`, `idle`, `busy`, `waiting`. An open stop command is displayed
separately; it never overwrites observed state with a synthetic `stopping` state.

| From | Exact input | To / effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0 |
| `created` | first start dispatched | generation 1, rotate token, `starting` |
| `created` | stop before start dispatch | cancel launch, `stopped` |
| terminal generation `g` | resume dispatched with expected `g` | generation `g+1`, rotate token, clear active turn/requests, `starting` |
| `starting` | matching `session.ready` | `idle` |
| `starting` | definite pre-admission/native start failure | `failed` |
| `starting` | timeout/lost reply after possible native I/O | state unchanged; start command `unknown` |
| `idle` | exact `turn.started(t)` | `busy`, `active_turn=t` |
| `busy` | first exact pending request | `waiting` |
| `waiting` | last exact request resolved | `busy` |
| `busy`/`waiting` | `turn.completed(active_turn)` | `idle`, clear turn and its pending requests |
| any live | exact normal end or successful stop readback | `stopped`, clear turn/requests, pause generation-bound goals |
| any live | fatal session error | `failed`, clear turn/requests, pause generation-bound goals |
| any live | exact custody/session loss | `lost`, clear turn/requests, pause generation-bound goals |

A resume command has two generations: `expected_generation=g` fences admission;
`generation=g+1` is the target incarnation written to the command. The agent row moves
to `g+1` only when resume dispatch commits. A second resume is rejected while one is
open. A cancelled queued resume leaves the old terminal generation unchanged.

Native and Eliot goals belong to the generation that activated them. A generation
change pauses them with `reason=generation_changed`; native readback or an explicit
`goal.resume/set` is required before the new generation is driven.

`detach` preserves observed state and changes only control/observation. Wrong-generation,
wrong-turn, wrong-request and duplicate native inputs do not enter the normal lifecycle
bus. They may produce one bounded `x.eliot.late_native` diagnostic, but cannot fire
ordinary lifecycle rules.

`stalled` is a flag over `busy`/`waiting`. Only progress-bearing native facts update
`last_progress_at`; CPU/Git/mail/source-health do not. The pure reducer is the sole
agent-state writer and is table-tested for every valid, duplicate and stale input.

## Profile

```text
Profile {
  harness, extends?, cmd?: [string],
  model?, effort?, permissions [workspace-write],
  args [[]], env_mode [minimal], env [{}], pass_env [[]], secret_env [[]], proxy?,
  workspace_mode [shared], control [self], git_identity [agent],
  system_note?: path, labels [{}], stall_after [5m], max_live?,
  billing?, account?, setup [[]], scratch [["target/", "node_modules/"]],
  notify_parent [true], wake_on_mail [true]
}
```

Environment and account semantics are defined in
[configuration](configuration.md). `max_live` is a profile-local narrowing; account and
proxy caps are separate shared limits. Requested/effective model, permissions, effort,
endpoint and billing route remain distinct. Eliot never silently changes them.

## Events

```text
Event { seq?, ts, kind, agent?, generation?, source, native_id?, cause?, data }
```

Persisted data is bounded (default 64 KiB). Known secret values are masked in native
output/log events. Writes whose semantics require their original text (commands, mail,
vars, rule payloads) are rejected if they contain a known secret; Eliot never persists
a masked value and later delivers altered semantics.

A core lifecycle event is admitted only when:

1. its source owns that event family;
2. generation matches;
3. turn/request binding matches current state where required;
4. `(agent,generation,kind,native_id)` has not already been persisted when `native_id`
   is present.

Plugins use a per-event native id when the vendor has one. If one native item produces
different normalized kinds, the kind distinguishes them. If repeated updates of the
same kind are legitimate, use the vendor update/event id or omit `native_id`; do not
mislabel an item id as a unique event id.

Main persisted kinds:

| Family | Main fields |
| --- | --- |
| `session.ready/ended/error/lost` | native subject, pids/effective, reason/exit |
| derived `agent.created/starting/idle/busy/waiting/stopped/failed/lost` | from/reason |
| `agent.stalled/unstalled` | since |
| `turn.started/completed` | exact turn id, input ref, terminal status, final text/usage |
| `message.user/agent`, ephemeral `message.delta` | turn id, bounded text |
| `tool.started/completed`, `file.changed`, `plan.updated` | exact turn/item/path data |
| `approval.*`, `question.*` | exact turn/request id and decision/answer |
| `child.spawned` | child id and exact native subject |
| `goal.updated` | mode/status/reason |
| `usage.updated` | token counts and optional integer `cost_microusd`, scope turn/session |
| `provider.limited` | kind, explicit scope/coverage, provider/model/account/route when known, retry_after? |
| process/Git/fs/source/plugin/daemon events | source-specific bounded data |
| command/rule/action/mail/var/claim events | stable object id and state/reason |

Provider limitation scope is `session|model|account|route|unknown`. Unknown coverage
holds only the exact observed route/profile; it never fabricates an account-wide quota.

## Verbs and linked initial input

| Verb | Semantics | Precondition |
| --- | --- | --- |
| `launch` | create row and start generation 1 | preflight passes |
| `send` | new turn; FIFO while busy, bound to one generation | live |
| `steer` | input to exact active turn | busy + native support |
| `inject` | exact safe-point context or explicit prepend-next-send | live + declared mechanism |
| `interrupt` | abort exact active turn, keep session | busy/waiting |
| `stop` | end exact generation; force only with exact custody/native isolation | live, or created to cancel |
| `resume` | start next generation from retained native subject | stopped/lost/failed + support |
| `approve/deny/answer` | exact pending request | matching request |
| `configure` | change settings with reported application point | live |
| `goal.*` | native or Eliot goal control | matching goal/session |
| `cancel` | withdraw queued command before native dispatch | queued |
| `detach` | release control of same canonical native row | controllable external/service session |
| `compact/fork/background` | native-only declared semantics | native support |

User-facing `launch ... PROMPT` and `resume ... PROMPT` are atomic **admission sugar**,
not one ambiguous native effect. The daemon creates:

1. a launch/resume command for session incarnation; and
2. a linked `send` command with a fixed derived id, bound to the target generation.

The linked send stays queued until matching `session.ready`; definite start failure or
owner resolution as failed fails the linked send without native I/O. The API returns
both ids. A harness that cannot establish a session before its first input declares
`start_requires_input`; the host may pass that linked send inside `agent.start`, but
plugin/start reply and snapshot report session-start and initial-send outcomes
separately. `session.ready` alone never proves that initial input was admitted.

Text in launch-linked send, send, steer or inject is at most 512 KiB UTF-8. Longer
content goes in a file and is referenced by a short message.

## Commands

```text
Command {
  id, target?, selector?,
  expected_generation?, generation?, turn_id?, request_id?,
  verb, args, origin, cause?, state, outcome?, created_at, updated_at
}
state = accepted → [queued →] dispatched → applied | rejected | failed | unknown
        queued → cancelled
```

For ordinary agent commands, expected and target generation are equal. Launch has no
expected generation and target generation 1. Resume captures old generation in
`expected_generation` and next generation in `generation`.

Ready native calls serialize per agent. Queued sends remain FIFO but do not block exact
current-turn control, answers, or configuration that can apply now. Every bypass carries
exact generation plus turn/request where applicable. A queued command never crosses a
generation.

- `rejected`: no native I/O.
- `failed`: exact evidence that the intended native effect did not apply.
- `unknown`: it may have applied. No automatic retry, including start, input,
  interrupt, stop, scripts or external writes.
- daemon restart converts `dispatched` to `unknown`; still-current queued commands stay
  queued. A generation change fails older accepted/queued commands as stale.
- exact snapshot/readback or explicit owner resolution settles unknown. Time does not.
- an interrupt is applied only when the bound turn is known interrupted; another
  terminal status is not inferred. A stop is applied only when the bound generation is
  known ended. A newer generation does not by itself settle the older stop.

Selectors expand transactionally at admission. Internal child ids use the fixed
`derived_id` helper. Automation resolves selectors earlier at rule firing and stores
concrete targets/bindings.

## Storage

Use bundled SQLite, WAL, `synchronous=FULL`, one writer task and bounded group commit.
Set `busy_timeout`, verify `application_id`/supported `user_version`, then enable WAL.
Every read connection uses `foreign_keys=ON` and `query_only=ON`; no API stream holds a
read transaction across network waits. Writes use `BEGIN IMMEDIATE`.

```sql
CREATE TABLE id_counters (
  name TEXT PRIMARY KEY,
  next INTEGER NOT NULL CHECK(next BETWEEN 1 AND 9223372036854775807)
);

CREATE TABLE agents (
  id INTEGER PRIMARY KEY,
  generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
  name TEXT UNIQUE, project TEXT NOT NULL,
  parent INTEGER REFERENCES agents(id), root INTEGER NOT NULL REFERENCES agents(id),
  depth INTEGER NOT NULL CHECK(depth >= 0),
  origin TEXT NOT NULL, harness TEXT NOT NULL, profile TEXT NOT NULL,
  native_scope TEXT, native_ref TEXT,
  workspace TEXT, branch TEXT, base_ref TEXT,
  state TEXT NOT NULL, active_turn TEXT,
  stalled_since INTEGER, last_progress_at INTEGER,
  pending TEXT NOT NULL DEFAULT '[]', goal TEXT, effective TEXT,
  labels TEXT NOT NULL DEFAULT '{}', control TEXT NOT NULL DEFAULT 'self',
  observe_only INTEGER NOT NULL DEFAULT 0,
  token_hash BLOB NOT NULL,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);
CREATE UNIQUE INDEX agents_native_subject ON agents(native_scope, native_ref)
  WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;

CREATE TABLE events (
  seq INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL, kind TEXT NOT NULL,
  agent INTEGER, generation INTEGER,
  source TEXT NOT NULL, native_id TEXT, cause TEXT, data TEXT NOT NULL
);
CREATE INDEX events_agent ON events(agent, seq);
CREATE INDEX events_kind ON events(kind, seq);
CREATE UNIQUE INDEX events_native_dedupe
  ON events(agent, generation, kind, native_id)
  WHERE agent IS NOT NULL AND generation IS NOT NULL AND native_id IS NOT NULL;

CREATE TABLE commands (
  id TEXT PRIMARY KEY,
  target INTEGER, selector TEXT,
  expected_generation INTEGER, generation INTEGER,
  turn_id TEXT, request_id TEXT,
  verb TEXT NOT NULL, args TEXT NOT NULL, origin TEXT NOT NULL,
  cause TEXT, state TEXT NOT NULL, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX commands_target ON commands(target, created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');
```

The Store counter allocates an agent id before insert, allowing `root=id` for a
root agent in the same transaction. No `MAX(id)+1` allocation and no transient root 0.
Unique native-event conflicts are handled as duplicate input, not Store failure.

Nothing is reported accepted/applied before commit. On commit failure, retain the same
pending commit in the writer when possible, stop new writes, expose in-memory/log
health, and never repeat its external cause. On daemon crash, durable dispatched state
reconciles as unknown.

The writer monitors WAL/database size, performs periodic passive checkpoints, and
reports readers that pin WAL. Replays use bounded pages. A configurable hard WAL limit
rejects new long reads before disk exhaustion; it never deletes evidence or runs a
blind truncate checkpoint while readers are active.

Retention deletes in bounded batches. State needed after restart is stored in compact
projection tables (for example current per-agent edited paths), not kept by retaining
an unbounded event history. Native/vendor stores are never cleaned generically.
