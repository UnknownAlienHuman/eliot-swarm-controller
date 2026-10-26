# Core model

`eliot-core` contains pure types/reducers. `eliot-store` owns SQLite/allocation.
Wire names are `snake_case`; every persisted integer fits SQLite signed 64-bit.

## Scalar and identifier bounds

| Identity | Form/range | Rule |
| --- | --- | --- |
| `AgentId` | `a<1..i64::MAX>` | Store counter; stable logical row. |
| generation | `0..i64::MAX` | 0 before first native start; first incarnation 1; explicit resume increments. |
| external command/write id | 1–128 UTF-8 bytes | Same id + canonical payload returns original admission; different payload invalid. |
| internal derived id | fixed `d_<base32 digest>` | Domain tag + length-delimited components; parent identity stays in columns/cause. |
| event `seq`, message id | positive signed-64 Store counter | Transactional allocation; never `MAX(id)+1`. |
| native subject | bounded `(native_scope,native_ref)` | Canonical vendor store/server namespace + opaque session/thread id. Scope changes before refs can be reused. |

Derived ids cover selector children, linked initial sends, rule actions and all internal
writes; no unbounded concatenation.

## Canonical native subject

One native subject maps to one Agent row for its history. Observe clients subscribe to
that row. Attach returns/upgrades it; detach changes control. Resume retains the same
subject; a different native subject is a fork/new Agent.

Native child registration includes child scope/ref. Parent-local child ids use a scope
containing the exact parent native subject. Parent generation is provenance.

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

Generation 0 has no routing token. First-start/resume stores only a constant-time hash
and returns one raw high-entropy `eliot_at_<base64url>` token for injection. Prefix is
pattern-redactable after restart. Invalid/stale supplied token is rejected, never owner.

### States

Live states: `starting`, `idle`, `busy`, `waiting`. Open stop is displayed beside
observed state, not as synthetic `stopping`.

| From | Exact input | Effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0 |
| created | first start dispatched | generation 1, token rotation, `starting` |
| created | cancel/stop before dispatch | launch + linked send cancelled, `stopped` |
| terminal g | resume dispatched with expected g | generation g+1, token rotation, clear turn/requests, `starting` |
| starting | matching ready | `idle` |
| starting | definite no-effect start failure | `failed`; linked send fails without I/O |
| starting | lost reply after possible start | state unchanged; start unknown; linked send blocked |
| idle | exact turn start t | `busy`, `active_turn=t` |
| busy | first exact pending request | `waiting` |
| waiting | last exact request resolved | `busy` |
| busy/waiting | exact completion of active turn | `idle`, clear turn/requests |
| any live | exact normal end/stop applied | `stopped`, clear current turn/requests, pause goals |
| any live | fatal session error | `failed`, clear current turn/requests, pause goals |
| any live | exact custody/session loss | `lost`, clear current turn/requests, pause goals |

Resume expects g and targets g+1; row moves only when dispatch commits. One open resume.
Cancelling queued resume also cancels linked send and leaves g unchanged.

An admitted async input that has reached a vendor inbox but is not yet associated with
an exact native turn keeps its `send` command `dispatched`. The per-agent input gate
blocks another send/configuration/goal continuation, while stop/readback remain allowed.
The plugin replies applied only after an exact `turn.started`/execution association.
Deadline after possible inbox admission makes the command unknown, not retried, and the
same gate remains until readback proves its turn or proves no effect.

If reconnect proves session exists but cannot prove active turn terminal outcome, turn
becomes `unknown`. Live Agent remains busy/waiting and blocks new send/goal for that
generation; interrupt/stop/readback remain. Exact history may settle it. Owner
`turn.resolve` is allowed only when native turn is proven not active or generation is
terminal; it records `turn.resolved`, not fabricated plugin completion.

Terminal generation may retain unknown historical turn. Resume can start new
generation; unknown never crosses generation. Generation-bound goals pause on change.

Wrong generation/turn/request/source and duplicate native input never enters normal
lifecycle. At most one bounded diagnostic is emitted. Only model progress updates
`last_progress_at`.

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
local narrowing; account/proxy caps shared. Requested/effective route facts remain
separate; Eliot never silently substitutes.

## Events

```text
Event { seq?, ts, kind, agent?, generation?, source, native_id?, cause?, subject?, data }
```

`subject` is bounded event-time projection for deterministic replay. Rules never join a
later mutable Agent row.

Known secrets and token pattern are masked in native output/logs. Semantic durable
writes containing known secret reject rather than storing altered text.

A persisted plugin lifecycle event requires authoritative source, exact current
binding, stable per-event native id (vendor event id or persisted cursor/offset), and no
existing `(agent,generation,kind,native_id)`. Source without stable identity may publish
ephemeral diagnostics but not replay-safe lifecycle.

`turn.started` carries `input_commands:[...]` when known. Additional already-admitted
inputs may associate through exact `turn.inputs`. Same command belongs to at most one
turn. This handles native coalescing without inventing several terminal turns.

Main families include session/agent/turn/message/tool/file/plan/request/child/goal/
usage/provider/process/Git/source plus command/rule/action/mail/var/claim.
`turn.resolved` is daemon/owner evidence. Cost is integer micro-USD. Provider limitation
includes kind/scope/coverage/route/account/retry time.

## Verbs and linked initial input

Launch/resume with optional text creates session command + linked send. Normally send
waits for ready. `start_requires_input` plugin receives linked command in start, but
session/input outcomes remain separate. Ready never proves input.

| Verb | Semantics |
| --- | --- |
| launch | create row and start generation 1 after preflight/capacity/workspace readiness |
| send | one input; FIFO; command applies only when associated with exact native turn/execution; never crosses generation |
| steer/inject/interrupt | exact current-turn/native-safe-point semantics only |
| stop | exact generation; force only with qualified custody/native isolation |
| resume | next generation from same canonical subject |
| approve/deny/answer | exact pending request |
| configure | report application point/effective readback; blocked by unresolved input |
| goal.* | generation-bound goal control; continuation blocked by unresolved input/turn |
| cancel | withdraw queued command only; vendor-inbox input requires native cancel/readback |
| detach | release control of same canonical row |
| compact/fork/background | native-only declared semantics |

Input text ≤512 KiB UTF-8; larger content is file-referenced.

## Commands

```text
Command {
  id, target?, selector?, expected_generation?, generation?, turn_id?, request_id?,
  verb, args, origin, cause?, state, outcome?, created_at, updated_at
}
state = accepted → [queued →] dispatched → applied | rejected | failed | unknown
        queued → cancelled
```

Ordinary expected=target generation. Launch targets 1. Resume expects g/targets g+1.
Ready calls serialize. Queued inputs remain FIFO and do not block current controls, but
one dispatched/unknown input gate blocks later send/configure/goal in that generation.

Rejected means no native I/O; failed exact no-effect; unknown may have applied and is
never retried. Restart makes dispatched unknown. Generation change stales older queued.
Exact readback or owner resolution settles unknown; time/new generation does not infer.

For send, applied means exact association to a native turn/execution, not terminal
success. Turn outcome and command→turn mapping live in turn projections. Resolving send
never manufactures terminal state. Resolving start applied does not synthesize ready;
linked send remains blocked until exact ready/attach.

Selectors expand transactionally; automation stores concrete targets/bindings.

## Storage

Bundled SQLite, WAL, `synchronous=FULL`, one writer and bounded group commit. Verify
application/schema identity before WAL. Reads are foreign-key on, query-only,
paginated/deadlined and release transactions before network waits.

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
CREATE UNIQUE INDEX agents_native_subject ON agents(native_scope,native_ref)
  WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);

CREATE TABLE events (
  seq INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL, kind TEXT NOT NULL,
  agent INTEGER, generation INTEGER,
  source TEXT NOT NULL, native_id TEXT,
  cause TEXT, subject TEXT, data TEXT NOT NULL
);
CREATE INDEX events_agent ON events(agent,seq);
CREATE INDEX events_kind ON events(kind,seq);
CREATE UNIQUE INDEX events_native_dedupe
  ON events(agent,generation,kind,native_id)
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
CREATE INDEX commands_target ON commands(target,created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');

CREATE TABLE turns (
  agent INTEGER NOT NULL REFERENCES agents(id),
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  state TEXT NOT NULL,                  -- running|completed|unknown
  status TEXT,                          -- completed|interrupted|failed
  resolution_source TEXT,               -- native|readback|owner
  final_text TEXT, usage TEXT,
  started_at INTEGER NOT NULL, completed_at INTEGER,
  PRIMARY KEY(agent,generation,turn_id)
);
CREATE INDEX turns_recent ON turns(agent,generation,completed_at DESC)
  WHERE completed_at IS NOT NULL;

CREATE TABLE turn_inputs (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  command_id TEXT NOT NULL UNIQUE REFERENCES commands(id),
  ordinal INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,turn_id,command_id),
  UNIQUE(agent,generation,turn_id,ordinal),
  FOREIGN KEY(agent,generation,turn_id)
    REFERENCES turns(agent,generation,turn_id)
);
```

Turn association/completion/unknown/resolution updates projections with source event/
reducer. `wait --after-command` follows `turn_inputs`; several commands may wait on one
terminal turn. `wait`/`result` survive event retention. Final text/usage bounded;
unresolved turns never pruned.

Counter allocates root id before self-reference. Event seq transactional. Native-event
unique conflict is duplicate input, not Store failure.

Failed result commit blocks writes, retains same pending commit where possible and never
repeats external cause; crash leaves durable dispatched effects unknown.

WAL/readers monitored. Replays release transactions; hard pressure rejects new long
reads and exposes pin—never deletes evidence or blind-truncates active readers.

Event retention follows minimum active automation cursor. Paused/disabled rules advance
by policy. Current edit/turn results live in compact projections. Native stores never
generically cleaned.
