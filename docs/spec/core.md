# Core model

`eliot-core` contains pure types/reducers. `eliot-store` owns SQLite/allocation.
Wire names are `snake_case`; persisted integers fit SQLite signed 64-bit.

## Scalar and identifier bounds

| Id | Form/range | Rule |
| --- | --- | --- |
| `AgentId` | `a<1..i64::MAX>` | Store counter; stable logical row. |
| generation | `0..i64::MAX` | 0 before first native start; first incarnation 1; explicit resume increments. |
| external command/write id | 1–128 UTF-8 bytes | Same id + canonical payload returns original admission; different payload invalid. |
| internal derived id | fixed `d_<base32 digest>` | Domain tag + length-delimited canonical components; parent identity stays in columns/cause. |
| event `seq`, message id | positive signed-64 counter | Monotonic Store allocation; never `MAX(id)+1`. |
| native subject | bounded `(native_scope,native_ref)` | Canonical vendor store/server namespace + opaque session/thread id. Scope changes before refs can be reused. |

The fixed derived-id helper is used for selector children, linked initial sends, rule
actions and all internal writes; no unbounded string concatenation.

## Canonical native subject

One native subject maps to one Agent row for its history. Observe clients subscribe to
that row. Attach returns/upgrades it; detach changes control on it. A resume must retain
the same canonical subject. A vendor operation producing a different subject is a fork/
new Agent, not resume.

Native child registration includes child scope + ref. If child ids are parent-local,
scope includes the exact parent native subject. Parent generation remains provenance.

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
  pending: [Request],
  goal?, effective?, labels,
  control: self | tree | project,
  observe_only,
  created_at, updated_at
}
```

Generation 0 has no routing token. On first-start/resume dispatch, one transaction
increments/sets generation, stores only `token_hash`, and returns the raw token once for
injection. The raw token is a known secret. Invalid/stale supplied token is rejected,
never owner authority.

### States

Live states: `starting`, `idle`, `busy`, `waiting`. Open stop is a command displayed
beside observed state, not a synthetic `stopping` state.

| From | Exact input | Effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0 |
| created | first start dispatched | generation 1, token rotation, `starting` |
| created | stop/cancel before dispatch | launch and linked send cancelled, `stopped` |
| terminal g | resume dispatched with expected g | generation g+1, token rotation, clear current turn/requests, `starting` |
| starting | matching ready | `idle` |
| starting | definite no-effect/native start failure | `failed`; linked send failed without I/O |
| starting | lost reply after possible start | state unchanged; start unknown; linked send remains blocked |
| idle | exact turn start t | `busy`, `active_turn=t` |
| busy | first exact pending request | `waiting` |
| waiting | last exact request resolved | `busy` |
| busy/waiting | completion of active turn | `idle`, clear turn/request set |
| any live | exact normal end/stop applied | `stopped`, clear turn/requests, pause generation goals |
| any live | fatal session error | `failed`, clear turn/requests, pause goals |
| any live | exact custody/session loss | `lost`, clear turn/requests, pause goals |

Resume stores old `expected_generation=g` and target `generation=g+1`. The row moves
only when dispatch commits. One open resume per agent. Cancelling queued resume also
cancels its linked send and leaves g unchanged.

Generation-bound native/Eliot goals pause on generation change with
`generation_changed`; explicit readback + resume/set rebinds them.

Wrong generation/turn/request/source and duplicate native inputs never enter the normal
lifecycle bus. At most one bounded `x.eliot.late_native` diagnostic is emitted. Only
model-progress facts update `last_progress_at`.

## Profile

```text
Profile {
  harness, extends?, cmd?, model?, effort?, permissions,
  args, env_mode [minimal], env, pass_env, secret_env, proxy?,
  workspace_mode [shared], control [self], git_identity [agent],
  system_note?, labels, stall_after [5m], max_live?,
  billing?, account?, setup, scratch,
  notify_parent [true], wake_on_mail [true]
}
```

Environment/account semantics are in [configuration](configuration.md). Profile cap is
only a local narrowing; account/proxy caps are shared. Requested/effective route facts
remain separate and Eliot never silently substitutes them.

## Events

```text
Event {
  seq?, ts, kind, agent?, generation?, source,
  native_id?, cause?, subject?, data
}
```

`subject` is the bounded event-time projection used by deterministic rule replay:
project, harness, profile, labels, state, workspace/branch and other explicitly
allowlisted match fields. Rules never join a later mutable Agent row to reinterpret an
old event.

Known secret values are masked in native output/log events. Semantic durable writes
(command/mail/var/rule payloads) containing a known secret are rejected rather than
stored masked and later delivered altered.

A persisted plugin lifecycle event requires:

1. authoritative source ownership;
2. current generation and exact turn/request where applicable;
3. a stable per-event `native_id` (vendor event id or persisted source cursor/offset);
4. no existing `(agent,generation,kind,native_id)` row.

If a source cannot provide stable identity, it may publish live ephemeral diagnostics
but cannot claim replay-safe persisted lifecycle. One item may produce different kinds;
kind separates them. Repeated same-kind updates require distinct event/update ids.

Important families include session/agent/turn/message/tool/file/plan/request/child/goal/
usage/provider/process/Git/source plus command/rule/action/mail/var/claim. Cost is
integer `cost_microusd`. Provider limitation includes kind, scope
`session|model|account|route|unknown`, coverage, route/account and optional retry time;
unknown coverage holds only the observed route/profile.

## Verbs and linked initial input

Launch/resume with optional text is atomic admission sugar for two commands:

1. session command targeting the new generation;
2. linked send with a fixed derived id targeting that generation.

Normally the send waits for ready and dispatches separately. A harness with
`start_requires_input` receives the linked command in `agent.start`, but session and
initial-input outcomes remain separate. Ready never proves input admission.

| Verb | Semantics |
| --- | --- |
| launch | create row and start generation 1 after preflight/capacity |
| send | one new turn; FIFO while busy; never crosses generation |
| steer/inject/interrupt | exact current-turn/native-safe-point semantics only |
| stop | exact generation; force only with qualified custody/native isolation |
| resume | next generation from the same canonical native subject |
| approve/deny/answer | exact pending request |
| configure | report application point/effective readback |
| goal.* | generation-bound native/Eliot goal control |
| cancel | withdraw any queued command; linked dependants cancel/fail transactionally |
| detach | release control of the same canonical Agent row |
| compact/fork/background | native-only declared semantics |

Input text limit is 512 KiB UTF-8; larger content is referenced from a file.

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

Ordinary commands have expected=target generation. Launch has no expected generation
and targets 1. Resume expects g and targets g+1.

Ready native calls serialize per agent. Queued future sends remain FIFO but do not block
exact current-turn controls/answers or configuration that applies now. Bypass commands
carry exact generation + turn/request.

- rejected: no native I/O;
- failed: exact evidence intended effect did not apply;
- unknown: may have applied; never retried automatically;
- restart converts durable dispatched to unknown; current queued stays queued;
- generation change fails older accepted/queued commands stale;
- exact readback or explicit owner resolution settles unknown; time/new generation does
  not infer old outcome.

Owner resolution of a start as applied does not synthesize ready/native identity; linked
send remains blocked until exact ready/attach. Resolving start failed cancels/fails the
linked send. Resolving stop applied does not manufacture missing terminal evidence for
other commands.

Selectors expand transactionally; automation already stores concrete targets/bindings.

## Storage

Bundled SQLite, WAL, `synchronous=FULL`, one writer, bounded group commit. Verify
application/schema identity before enabling WAL. All read connections are foreign-key
on + query-only; reads are paginated/deadlined and never hold a transaction across a
network wait.

```sql
CREATE TABLE id_counters (
  name TEXT PRIMARY KEY,
  next INTEGER NOT NULL CHECK(next BETWEEN 1 AND 9223372036854775807)
);

CREATE TABLE agents (
  id INTEGER PRIMARY KEY,
  generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
  name TEXT UNIQUE, project TEXT NOT NULL,
  parent INTEGER REFERENCES agents(id),
  root INTEGER NOT NULL REFERENCES agents(id),
  depth INTEGER NOT NULL CHECK(depth >= 0),
  origin TEXT NOT NULL, harness TEXT NOT NULL, profile TEXT NOT NULL,
  native_scope TEXT, native_ref TEXT,
  workspace TEXT, branch TEXT, base_ref TEXT,
  state TEXT NOT NULL, active_turn TEXT,
  stalled_since INTEGER, last_progress_at INTEGER,
  pending TEXT NOT NULL DEFAULT '[]', goal TEXT, effective TEXT,
  labels TEXT NOT NULL DEFAULT '{}', control TEXT NOT NULL DEFAULT 'self',
  observe_only INTEGER NOT NULL DEFAULT 0,
  token_hash BLOB,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  CHECK((generation = 0 AND token_hash IS NULL) OR
        (generation > 0 AND token_hash IS NOT NULL))
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
  source TEXT NOT NULL, native_id TEXT,
  cause TEXT, subject TEXT, data TEXT NOT NULL
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

The counter allocates root id before self-referencing insert in the same transaction.
All counters, including event seq, are allocated transactionally. A unique native-event
conflict is duplicate input, not Store failure.

Nothing is reported accepted/applied before commit. Failed result commit blocks new
writes, retains the same pending commit where possible and never repeats its external
cause; a daemon crash leaves the durable dispatched effect unknown.

WAL/database/readers are monitored. Replays release read transactions between pages;
stale Eliot-owned reads have deadlines/cancellation. At hard pressure reject new long
reads and expose the pin—never delete evidence or blind-truncate with active readers.

Retention is bounded and may delete only through the minimum active automation event
cursor. Paused/disabled rules do not pin history (their cursor advances by policy).
Current state such as edit attribution lives in compact projection tables, not event
retention. Native stores are never generically cleaned.
