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

A counter allocation that would require `i64::MAX + 1` fails `id_exhausted` before any
row is created. Derived ids cover selector children, linked initial sends, rule actions
and all internal writes; no unbounded concatenation.

## Canonical native subject

One native subject maps to one Agent row for its history. Observe clients subscribe to
that row. Attach returns/upgrades it; detach changes control. Resume retains the same
subject; a different native subject is a fork/new Agent.

Native child registration includes child scope/ref. Parent-local child ids use a scope
containing the exact parent subject. Parent generation is provenance.

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

Generation 0 has no routing token. First start/resume creates one raw high-entropy
`eliot_at_<base64url>` token, stores only its constant-time hash and injects the raw
value once. The prefix remains pattern-redactable after restart.

Token presence follows the **current routing lifecycle**, not generation alone:

- a controlled live generation (`starting|idle|busy|waiting`, `observe_only=false`) has
  exactly one current token hash;
- terminal state, detach/observe-only and generation 0 have no token hash;
- terminal/detach revocation sets `token_hash=NULL` in the same transaction without
  decreasing or resetting generation;
- resume rotates to a new token only when dispatching the next generation.

An invalid, revoked or stale supplied token is rejected and never treated as owner.

### States

Live states are `starting`, `idle`, `busy`, `waiting`. An open stop is displayed beside
the observed state, not as a synthetic `stopping` state.

| From | Exact input | Effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0 |
| created | first start dispatched | generation 1, token rotation, `starting` |
| created | cancel/stop before dispatch | launch + linked send cancelled, `stopped` |
| terminal g | resume dispatched with expected g | generation g+1, new token, clear turn/requests, `starting` |
| starting | matching ready | `idle` |
| starting | definite no-effect start failure | `failed`; revoke token; linked send fails without I/O |
| starting | lost reply after possible start | state unchanged; start unknown; linked send blocked |
| idle | exact turn start t | `busy`, `active_turn=t` |
| busy | first exact pending request | `waiting` |
| waiting | last exact request resolved | `busy` |
| busy/waiting | exact completion of active turn | `idle`, clear turn/requests |
| any live | exact normal end/stop applied | `stopped`, revoke token, clear turn/requests, pause goals |
| any live | fatal session error | `failed`, revoke token, clear turn/requests, pause goals |
| any live | exact custody/session loss | `lost`, revoke token, clear turn/requests, pause goals |

Resume expects g and targets g+1; the row moves only when dispatch commits. Only one
resume may be open. Cancelling a queued resume also cancels its linked send and leaves g
unchanged. Detach preserves the last observed native state, sets `observe_only=true`
and revokes the token in one transaction.

An async input accepted by a vendor inbox but not yet associated with a native turn
keeps `send` dispatched. `input.admitted` durably records the Eliot command and exact
native input/message id without changing Agent state or freeing the input gate. Another
send, configure or goal continuation is blocked; stop/readback remain allowed. Exact
`input.dropped` before turn association settles send failed. Exact turn association
settles send applied. Deadline after possible admission makes send unknown and
preserves the same readback key and gate.

If reconnect proves the session exists but cannot prove the active turn's terminal
outcome, that turn becomes unknown. A live Agent remains busy/waiting and blocks new
send/goal work; interrupt/stop/readback remain allowed. Exact history may settle it.
Owner `turn.resolve` is allowed only when native readback proves the turn not active or
the generation is terminal; it records `turn.resolved`, never a fabricated plugin
completion.

A terminal generation may retain an unknown historical turn. Resume may start a new
generation; unknown work never crosses generation. Generation-bound goals pause on
change.

Wrong generation/turn/request/source and duplicate native input never enter normal
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

Environment/account semantics are in [configuration](configuration.md). A profile cap
is local narrowing; account/proxy caps are shared. Requested/effective route facts stay
separate; Eliot never silently substitutes them.

## Events

```text
Event {
  seq?, ts, kind, agent?, generation?, source,
  source_key?, source_seq?, native_id?, cause?, subject?, data
}
```

For an agent-scoped event, `subject` is a bounded ≤16 KiB post-transition snapshot:
`{id,generation,project,parent,root,depth,harness,profile,state,workspace?,branch?,
control,observe_only,labels}`. It contains no token, credential, pending request text or
unbounded effective settings. Rules evaluate this stored snapshot and never join a
later mutable Agent row.

Known secrets and token patterns are masked in native output/logs. Semantic durable
writes containing a known secret are rejected rather than stored with altered text.

### Commit-acknowledged source sequence

Every authoritative plugin source ownership epoch has a stable bounded `source_key` and
a strictly contiguous positive `source_seq`. The plugin durably assigns the sequence
before sending and allows at most one unacknowledged persisted `event.put` per source
key. Future native facts may accumulate in its bounded spool but are sent in order.

The Store keeps one compact watermark per source key. For an `event.put` canonical
envelope digest D and sequence n:

- `n = high_water + 1`: commit watermark, event and all reducer/projection changes in
  one transaction, then acknowledge;
- `n = high_water` and D equals `last_digest`: lost-reply duplicate; return the saved
  Store event sequence without inserting or firing again, even if that event row was
  pruned;
- `n = high_water` with another digest: source integrity error;
- `n < high_water`: stale local source state; return current watermark and admit
  nothing;
- `n > high_water + 1`: explicit source gap; admit nothing until the missing sequence
  is replayed or the ownership epoch is closed as gapped.

`native_id` remains the exact vendor event/request/turn/item id or cursor where one
exists. It supports readback and vendor replay suppression, but Store crash idempotency
does not depend on retaining one receipt row per native event. A source without a
replayable vendor cursor must preserve its bounded spool; loss beyond that boundary is
a visible gap, not invented history.

A plugin restart/attach reads the current source watermark before publishing. Normal
restart reuses the same source key. A stream→transcript/hook ownership handoff creates a
new source key only after the old cursor boundary is committed. Old source watermark
rows may be removed after the Agent advances beyond that generation and every old
source/spool is closed; row count is therefore O(source ownership epochs), not O(events).

Input/turn events:

- `input.admitted`: `{command_id,native_input_id}`; inbox/transport acceptance only;
- `input.dropped`: exact pre-turn terminal rejection/drop for that native input;
- `turn.started`: `{turn_id,input_commands:[...]}`;
- `turn.inputs`: exact additional already-admitted commands associated to the same turn;
- `turn.completed`: exact native terminal status/output/usage;
- `turn.resolved`: readback/owner resolution, distinct from plugin completion.

The same command belongs to at most one turn. Native coalescing maps several commands
to one turn without inventing several terminals.

Other families include session/agent/message/tool/file/plan/request/child/goal/usage/
provider/process/Git/source plus command/rule/action/mail/var/claim. Cost is integer
micro-USD. Provider limitation includes kind/scope/coverage/route/account/retry time.

## Verbs and linked initial input

Launch/resume with optional text creates a session command plus linked send. Normally
the send waits for ready. A `start_requires_input` plugin receives the linked command
inside start, but session/input outcomes remain separate. Ready never proves input.

| Verb | Semantics |
| --- | --- |
| launch | create row/start generation 1 after preflight/capacity/workspace readiness |
| send | one input; FIFO; applies only at exact native turn/execution association |
| steer/inject/interrupt | exact current-turn/native-safe-point semantics only |
| stop | exact generation; force only with qualified custody/native isolation |
| resume | next generation from the same canonical subject |
| approve/deny/answer | exact pending request |
| configure | application point/readback; blocked by unresolved input |
| goal.* | generation-bound; continuation blocked by unresolved input/turn |
| cancel | queued command only; inbox input requires native cancel/readback |
| detach | release control of the same canonical row and revoke token |
| compact/fork/background | native-only declared semantics |

Input text is at most 512 KiB UTF-8; larger content is file-referenced.

## Commands

```text
Command {
  id, target?, selector?, expected_generation?, generation?, turn_id?, request_id?,
  verb, args, origin, cause?, state, outcome?, created_at, updated_at
}
state = accepted → [queued →] dispatched → applied | rejected | failed | unknown
        queued → cancelled
```

Ordinary expected=target generation. Launch targets 1. Resume expects g and targets
g+1. Ready calls serialize. Queued inputs remain FIFO; one dispatched/unknown input
gate blocks later send/configure/goal in the same generation but not exact control,
stop or readback.

Rejected means no native I/O; failed means exact no-effect evidence; unknown may have
applied and is never retried. Restart makes dispatched unknown. Generation change
stales older queued work. Exact readback or constrained owner resolution settles
unknown; time or a newer generation does not infer an old result.

For send, applied means exact turn/execution association, not inbox acceptance or
terminal success. Command outcome may retain `native_input_id` while dispatched or
unknown. Turn outcome and mapping live in projections. Resolving send never
manufactures a turn. Resolving start applied does not synthesize ready.

Selectors expand transactionally; automation stores concrete targets/bindings.

## Storage

Use bundled SQLite, WAL, `synchronous=FULL`, one writer and bounded group commit.
Verify application/schema identity before WAL. Read connections use foreign keys,
`query_only`, bounded pages/deadlines and release transactions before network waits.

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
  CHECK(
    (token_hash IS NOT NULL AND generation > 0 AND observe_only = 0
      AND state IN ('starting','idle','busy','waiting'))
    OR
    (token_hash IS NULL AND
      (generation = 0 OR observe_only = 1
       OR state NOT IN ('starting','idle','busy','waiting')))
  )
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
  source TEXT NOT NULL,
  source_key TEXT, source_seq INTEGER,
  native_id TEXT, cause TEXT, subject TEXT, data TEXT NOT NULL,
  CHECK((source_key IS NULL AND source_seq IS NULL) OR
        (source_key IS NOT NULL AND source_seq > 0))
);
CREATE INDEX events_agent ON events(agent,seq);
CREATE INDEX events_kind ON events(kind,seq);
CREATE UNIQUE INDEX events_source_sequence
  ON events(agent,generation,source_key,source_seq)
  WHERE agent IS NOT NULL AND generation IS NOT NULL AND source_key IS NOT NULL;
CREATE UNIQUE INDEX events_native_dedupe
  ON events(agent,generation,kind,native_id)
  WHERE agent IS NOT NULL AND generation IS NOT NULL AND native_id IS NOT NULL;

CREATE TABLE plugin_source_watermarks (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  source_key TEXT NOT NULL,
  source_name TEXT NOT NULL,
  high_water_seq INTEGER NOT NULL CHECK(high_water_seq >= 0),
  last_digest BLOB,
  last_event_seq INTEGER,
  native_cursor TEXT,
  state TEXT NOT NULL,                 -- open|closed|gap
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,source_key),
  CHECK((high_water_seq = 0 AND last_digest IS NULL AND last_event_seq IS NULL) OR
        (high_water_seq > 0 AND last_digest IS NOT NULL AND last_event_seq IS NOT NULL))
);

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

CREATE TABLE native_inputs (
  command_id TEXT PRIMARY KEY REFERENCES commands(id),
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  native_input_id TEXT NOT NULL,
  state TEXT NOT NULL,                   -- admitted|associated|dropped|unknown
  admitted_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(agent,generation,native_input_id)
);

CREATE TABLE turns (
  agent INTEGER NOT NULL REFERENCES agents(id),
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  state TEXT NOT NULL,                   -- running|completed|unknown
  status TEXT,                           -- completed|interrupted|failed
  resolution_source TEXT,                -- native|readback|owner
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

`event.put` locks/reads the source watermark, validates sequence/digest and commits the
new watermark, event and projection changes in one transaction. A duplicate of the one
possible unacknowledged sequence returns `last_event_seq`; older source state is rejected
with the current watermark. Event retention never deletes source watermarks.

A source watermark for the current generation remains while that source can legally
reattach/replay. It may be reclaimed only after the Agent advances to a newer
generation (or is explicitly deleted) and every source/spool for the old generation is
closed. A terminal but still-current generation keeps its source rows. This is bounded
by ownership epochs rather than event count.

`input.admitted` updates `native_inputs` with event/command metadata. Association moves
it to associated and inserts `turn_inputs` in the same transaction as the turn event.
Drop/unknown/readback is similarly transactional. `wait --after-command` follows input
then turn. Several commands may wait on the same turn. Results survive event retention;
unresolved inputs/turns are never pruned.

The counter allocates root id before self-reference. Event seq is transactional. A
source-sequence duplicate is normal lost-reply readback, not Store failure.

Failed result commit blocks writes, retains the same pending commit where possible and
never repeats its external cause; crash leaves dispatched effects unknown.

WAL/readers are monitored. Replays release transactions; hard pressure rejects new
long reads and exposes the pin—never deleting evidence or blind-truncating active
readers.

Event retention follows the minimum active automation cursor. Paused/disabled rules
advance by policy. Current native input/edit/turn state and plugin-source watermarks
live in compact projections independent of event rows. Native stores are never
generically cleaned.
