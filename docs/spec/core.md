# Core model

`eliot-core` contains pure types/reducers. `eliot-store` owns SQLite and allocation.
Wire names are `snake_case`; every persisted integer fits SQLite signed 64-bit.

## Scalar and identifier bounds

| Identity | Form/range | Rule |
| --- | --- | --- |
| `AgentId` | `a<1..i64::MAX>` | Store counter; stable logical row. |
| generation | `0..i64::MAX` | 0 before first native start; first incarnation 1; explicit resume increments. |
| external command/write id | 1–128 UTF-8 bytes | Same id + canonical payload returns original admission; different payload is invalid. |
| internal derived id | fixed `d_<base32 digest>` | Domain tag + length-delimited components; parent identity stays in columns/cause. |
| event `seq`, message id | positive signed-64 Store counter | Transactional allocation; never `MAX(id)+1`. |
| native subject | bounded `(native_scope,native_ref)` | Canonical vendor store/server namespace + opaque session/thread id. Scope changes before refs can be reused. |

Derived ids are used for selector children, linked initial sends, rule actions and all
internal writes; no unbounded string concatenation.

## Canonical native subject

One native subject maps to one Agent row for its history. Observe clients subscribe to
that row. Attach returns/upgrades it; detach changes control on it. Resume must retain
the same subject. A native operation producing a different subject is a fork/new Agent.

Native child registration includes child scope and ref. If child ids are parent-local,
scope includes the exact parent native subject. Parent generation remains provenance.

## Agent

```text
Agent {
  id, generation, name?, project,
  parent?, root, depth,
  origin: eliot | native | external,
  harness, profile,
  native_scope?, native_ref?,
  workspace?, workspace_state?, branch?, base_ref?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending: [Request],
  goal?, effective?, labels,
  control: self | tree | project,
  observe_only,
  created_at, updated_at
}
```

Generation 0 has no routing token. On first-start/resume dispatch, one transaction
sets/increments generation, stores only a constant-time-comparable hash, and returns a
raw high-entropy token once for injection. Tokens use a recognizable
`eliot_at_<base64url>` shape so pattern redaction still works after daemon restart.
Invalid/stale supplied token is rejected, never owner authority.

### States

Live states: `starting`, `idle`, `busy`, `waiting`. Open stop is displayed beside the
observed state, not as a synthetic `stopping` state.

| From | Exact input | Effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0 |
| created | first start dispatched | generation 1, token rotation, `starting` |
| created | cancel/stop before dispatch | launch and linked send cancelled, `stopped` |
| terminal g | resume dispatched with expected g | generation g+1, token rotation, clear turn/requests, `starting` |
| starting | matching ready | `idle` |
| starting | definite no-effect start failure | `failed`; linked send fails without I/O |
| starting | lost reply after possible start | state unchanged; start unknown; linked send remains blocked |
| idle | exact turn start t | `busy`, `active_turn=t` |
| busy | first exact pending request | `waiting` |
| waiting | last exact request resolved | `busy` |
| busy/waiting | completion of active turn | `idle`, clear turn/request set |
| any live | exact normal end/stop applied | `stopped`, clear turn/requests, pause generation goals |
| any live | fatal session error | `failed`, clear turn/requests, pause goals |
| any live | exact custody/session loss | `lost`, clear turn/requests, pause goals |

Resume stores old `expected_generation=g` and target `generation=g+1`; the row moves
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
project, harness, profile, labels, state, workspace/branch and explicitly allowlisted
match fields. Rules never join a later mutable Agent row to reinterpret an old event.

Known secret values and the `eliot_at_...` token pattern are masked in native output/
logs. Semantic durable writes containing a known secret are rejected rather than
stored masked and later delivered altered.

A persisted plugin lifecycle event requires authoritative source ownership, current
exact generation/turn/request, a stable per-event `native_id` (vendor event id or
persisted source cursor/offset), and no existing
`(agent,generation,kind,native_id)`. A source without stable identity may publish
best-effort ephemeral diagnostics but cannot claim replay-safe persisted lifecycle.

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
| launch | create row and start generation 1 after preflight/capacity/workspace readiness |
| send | one new turn; FIFO while busy; never crosses generation |
| steer/inject/interrupt | exact current-turn/native-safe-point semantics only |
| stop | exact generation; force only with qualified custody/native isolation |
| resume | next generation from the same canonical native subject |
| approve/deny/answer | exact pending request |
| configure | report application point/effective readback |
| goal.* | generation-bound native/Eliot goal control |
| cancel | withdraw queued command; linked dependants settle transactionally |
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

Ordinary commands have expected=target current generation. Launch has no expected
generation and targets 1. Resume expects g and targets g+1.

Ready native calls serialize per agent. Queued future sends remain FIFO but do not
block exact current-turn controls/answers or configuration that applies now. Bypass
commands carry exact generation plus turn/request.

- rejected: no native I/O;
- failed: exact evidence the intended effect did not apply;
- unknown: may have applied; never retried automatically;
- restart converts durable dispatched to unknown; current queued stays queued;
- generation change fails older accepted/queued commands stale;
- exact readback or explicit owner resolution settles unknown; time/new generation does
  not infer old outcome.

Resolving a start as applied does not synthesize ready/native identity; linked send
remains blocked until exact ready/attach. Resolving start failed cancels/fails the
linked send. Resolving stop applied does not manufacture terminal evidence for another
command.

Selectors expand transactionally; automation already stores concrete targets/bindings.

## Storage

Bundled SQLite, WAL, `synchronous=FULL`, one writer and bounded group commit. Verify
application/schema identity before enabling WAL. Read connections are foreign-key on,
query-only, paginated/deadlined and never hold a transaction across network waits.

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
  workspace TEXT, workspace_state TEXT, branch TEXT, base_ref TEXT,
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

CREATE TABLE turns (
  agent INTEGER NOT NULL REFERENCES agents(id),
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  input_command TEXT REFERENCES commands(id),
  state TEXT NOT NULL,                  -- running|completed
  status TEXT,                          -- completed|interrupted|failed
  final_text TEXT,
  usage TEXT,
  started_at INTEGER NOT NULL,
  completed_at INTEGER,
  PRIMARY KEY(agent, generation, turn_id)
);
CREATE INDEX turns_recent ON turns(agent, generation, completed_at DESC)
  WHERE completed_at IS NOT NULL;

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

Migration order creates `commands` before `turns` (shown conceptually above). Turn
start/completion updates the bounded `turns` projection in the same transaction as the
event/reducer, so `wait`/`result` survive event retention. Final text and usage obey
normal bounds; old turn rows have their own configured retention independent of live
state and unresolved commands.

The counter allocates root id before self-referencing insert in the same transaction.
All counters, including event seq, are transactional. A native-event uniqueness conflict
is duplicate input, not Store failure.

Failed result commit blocks new writes, retains the same pending commit where possible
and never repeats its external cause; daemon crash leaves durable dispatched effects
unknown.

WAL/database/readers are monitored. Replays release transactions between pages;
stale Eliot-owned reads have deadlines/cancellation. At hard pressure reject new long
reads and expose the pin—never delete evidence or blind-truncate with active readers.

Event retention deletes only through the minimum active automation cursor. Paused/
disabled rules advance by policy and do not pin history. Current edit/turn results live
in compact projections rather than event history. Native stores are never generically
cleaned.
