# Core model

`eliot-core` contains pure types and reducers. `eliot-store` owns SQLite, allocation and
projections. Wire names are `snake_case`; every persisted integer fits SQLite signed
64-bit.

## Scalar and identifier bounds

| Identity | Form/range | Rule |
| --- | --- | --- |
| `AgentId` | `a<1..i64::MAX>` | Transactional Store counter; stable logical row. |
| generation | `0..i64::MAX` | 0 before first native start; first incarnation 1; explicit resume increments. |
| binding revision | positive signed-64, per Agent generation | Immutable profile/route snapshot; current pointer may advance within one generation. |
| external `WriteId` | canonical 26-character ULID | Required for every external daemon mutation. |
| internal `DerivedId` | `d_` + unpadded base32 BLAKE3-256 | Domain tag + length-delimited canonical components. |
| `CommandId` | `WriteId` or `DerivedId` | External concrete/fanout parent uses WriteId; internal child/automation uses DerivedId. |
| event `seq`, message id | positive signed-64 Store counter | Transactional allocation; never `MAX(id)+1`. |
| native subject | bounded `(native_scope,native_ref)` | Canonical vendor store/server namespace + opaque session/thread id. |

Allocation that would overflow fails `id_exhausted` before mutation. A derived-id
collision with another canonical payload is an integrity failure.

## External write idempotency

Every external mutation obtains a `WriteId` before send. Receipt lookup precedes age or
floor validation:

1. existing id + same principal/method/canonical semantic payload returns the retained
   current/original result;
2. existing id with another principal/method/payload fails `id_conflict`;
3. only an absent id is checked against monotonic floor, live window and future skew;
4. valid absent id inserts a compact receipt in the same transaction as domain rows.

Canonical payload excludes transport request id, output format, pager, client timeout
and `wait_ms`. Receipt stores a BLAKE3-256 digest and bounded sanitized result/reference.
Receipt retention is a minimum: a referenced receipt survives until every retained
domain row using it can be pruned. Floor advances only through receipts actually deleted
in the same transaction. A later absent retry below floor is `id_expired`, never a new
effect.

CLI uses a locked, atomically replaced monotonic ULID state file. Internal derived ids
never enter the external receipt namespace.

## Canonical native subject

One native subject maps to one Agent row for its whole Eliot history. Observers subscribe
to that row. Attach upgrades/returns it; detach changes control on it. Resume retains the
subject; another subject is fork/new Agent. Scope changes before vendor ref reuse.

Native child registration supplies exact child scope/ref. Parent-local child ids use a
scope containing the parent subject. Existing subject with another parent/root/project/
harness is an integrity conflict, never silent reparenting.

## Agent, binding and routing

```text
Agent {
  id, generation, token_version, routing_enabled,
  name?, project, parent?, root, depth,
  origin: eliot | native | external,
  harness, profile, native_scope?, native_ref?,
  workspace?, workspace_state?, branch?, base_ref?, base_oid?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending: [Request], goal?, effective?, labels,
  control: self | tree | project, observe_only,
  guard_exceeded?, created_at, updated_at
}
```

### Immutable binding revisions

Every target native incarnation has one or more immutable `AgentBinding` revisions:

```text
AgentBinding {
  agent, generation, revision,
  supersedes_revision?,
  mode: launch | resume | observe_attach | control_attach | detach_observe,
  profile_name, profile_revision, binding,
  created_at
}
```

A separate pointer selects one current revision per Agent generation. Binding rows never
change; only the pointer may advance atomically after the corresponding native effect is
proven applied.

- launch creates generation-1 revision 1 and pointer while Agent is generation 0;
- resume creates target generation `g+1`, revision 1 and pointer before dispatch;
- first observe attach creates generation-1 observe revision/pointer after exact attach;
- observe→control creates candidate `n+1`; attach command expects current n and targets
  candidate n+1; exact success moves pointer and projection atomically;
- failed/unknown attach leaves the old pointer; candidate remains command history;
- detach success creates/moves to `detach_observe` revision;
- control reattach creates another control revision and rotates token only after proof;
- exact recovery of an already-controlled incarnation reuses the current revision.

Native generation identifies the vendor incarnation; binding revision identifies Eliot's
control/profile lease within it. `Agent.profile` is a current projection. Every
Agent-scoped native command is admitted against an expected current binding revision and
carries the target revision it is allowed to use.

A pointer change stales all still-queued/accepted native commands from the old revision.
Dispatched/unknown commands remain historical and are never replayed against the new
lease. Exact command/turn readback always follows the revision originally stored on the
command. Native and Eliot Goals are also bound to `(agent,generation,binding_revision)`;
a pointer change pauses them with `binding_changed` until explicit rebind/resume.

Observe attach uses synthetic `observe:<harness>` binding. Control attach requires an
owner-selected allowed profile and capacity reservation; plugin readback must prove the
existing session is compatible before pointer movement.

### Routing token

Native control and direct Eliot routing are independent. `routing_enabled=true` means
Eliot proved it can inject this Agent's distinct token/environment before its own
CLI/tools run. A native child may be controllable through the parent plugin while
`routing_enabled=false`; it has no token and shell/Git attribution is parent/session.

Private `routing.key` derives raw `eliot_at_<base64url>` from
`(agent,generation,token_version)` through keyed BLAKE3. Store keeps only a
domain-separated constant-time-comparable hash.

- controlled live + direct routing: positive version and one hash;
- generation 0, terminal, detached, observe-only or routing-disabled: no hash;
- terminal/detach clears hash without rewriting generation/version;
- control reattach preserves generation, creates a binding revision, increments version
  and enables routing only after exact environment proof;
- recovery of the same current controlled revision preserves version/hash;
- resume increments generation/version when routing remains available.

Missing/mismatched routing key blocks token issue/reissue but not verification of an
already presented token against its retained hash. Owner identity is separate; absence
of an Agent token never implies owner.

### States

Live states are `starting`, `idle`, `busy`, `waiting`. Open stop is displayed beside
observed state; there is no synthetic `stopping` state.

| From | Exact input | Effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0; target generation/revision exists |
| created | first start dispatched | generation 1, optional routing token, `starting` |
| created | cancel/stop before dispatch | launch/linked input cancelled, `stopped` |
| terminal g | resume dispatched with expected g/binding | generation g+1/revision 1, clear turn/requests, `starting` |
| starting | matching ready | `idle` |
| starting | definite no-effect start failure | `failed`, revoke token, linked input fails without I/O |
| starting | possible start + lost reply | unchanged, start unknown, linked input blocked |
| idle | exact turn start t | `busy`, active turn t |
| busy | first exact pending request | `waiting` |
| waiting | last exact request resolved | `busy` |
| busy/waiting | exact completion of active turn | `idle`, clear turn/requests |
| any live | exact normal end/stop | `stopped`, revoke token, clear turn/requests, pause goals |
| any live | fatal session error | `failed`, revoke token, clear turn/requests, pause goals |
| any live | exact session-control loss | `lost`, revoke token, clear turn/requests, pause goals |

Resume row change occurs only when dispatch commits; only one resume may be open.
Cancelling queued resume cancels linked input and leaves generation g.

Detach is valid only for independently owned/reattachable service lifetime. On exact
success it moves to a detach-observe revision, sets observe-only, disables routing,
revokes token, releases claims, stales old-revision queued commands and pauses Goals.
Daemon-owned per-Agent process must stop instead.

An already-created native child is always recorded. Controller launch/depth/tree limits
block new Eliot-created fanout, not vendor reality. A native child beyond a guard gets
`guard_exceeded`, `control=self`, `routing_enabled=false`, no automatic direct wake and
no project authority; native observation and safe parent-mediated verbs remain.

### Input and unknown-turn gates

Vendor inbox acceptance without turn association keeps `send` dispatched.
`input.admitted` may store a native input id when one exists, but does not free the lane.
Another send/configure/Goal continuation is blocked; exact control/stop/readback remains.
Pre-turn drop settles failed. Exact command→turn association settles command applied.
Deadline after possible admission produces unknown and preserves the readback key/lane.

Some vendors expose exact command→turn mapping without a separate input id. Therefore
`native_input_id` is optional. Direct `turn.started` may create input projection and turn
mapping atomically; a later native id may fill it but cannot change mapped turn.

If reconnect proves the session exists but cannot prove active turn terminal outcome,
turn becomes unknown. A live Agent remains busy/waiting and blocks new turn/Goal work.
Owner resolution is allowed only after native-not-active proof or terminal generation and
records `turn.resolved`, never fabricated completion. Unknown old generation/revision
never crosses resume/reattach.

Only exact-generation model progress updates `last_progress_at`.

## Profile

```text
Profile {
  harness, plugin, model?, effort?, permissions,
  args, env_mode [minimal], env, pass_env, secret_env, proxy?,
  workspace_mode [shared], control [self], git_identity [agent],
  system_note_artifact?, labels, stall_after [5m], max_live?,
  billing?, account?, setup_artifacts, scratch,
  notify_parent [true], wake_on_mail [true]
}
```

Owner global config defines executable routes. Project config applies only a validated
narrowing overlay. Environment/account/artifact/binding semantics are in
[configuration](configuration.md). Requested/effective facts remain separate.

## Events and authoritative sources

```text
Event {
  seq?, ts, kind, agent?, generation?, source,
  source_key?, source_seq?, native_id?, cause?, subject?, data
}
```

Persisted data and projected final assistant text are each at most 64 KiB. Larger detail
is explicitly truncated with a safe pointer. Agent event `subject` is a bounded
post-transition snapshot:
`{id,generation,project,parent,root,depth,harness,profile,binding_revision,state,
workspace?,branch?,control,observe_only,routing_enabled,guard_exceeded?,labels}`.
Rules never join a later Agent row.

Known secrets/token patterns are masked in native output. Semantic durable writes with a
known secret reject rather than storing altered text.

### Source epoch and vendor replay

Plugin persists stable `open_id` before `source.open`. Store canonicalizes source name,
families, mechanism, dedupe mode and initial cursor into `open_digest`, then maps
`(agent,generation,open_id)` to one source key. Initial source opens before first native
start/input effect. Lost open reply returns same key/watermark; mismatch is integrity
failure.

Source dedupe mode:

- `strict_cursor`: resumable total-order cursor, replay strictly after committed cursor;
- `event_id`: every persisted lifecycle fact has stable per-event native id and compact
  receipt survives event pruning;
- `none`: no lossless recovery claim; reconnect/handoff exposes a gap.

Exactly one current owner exists per `(agent,generation,family)`. Planned replacement is
one predecessor-aware transaction verifying old key/high-water/cursor, closing/gapping
it, creating new epoch and transferring owners. Separate close/open is explicitly
gapped.

Each epoch emits contiguous positive `source_seq` with at most one unacknowledged durable
request. Daemon computes a canonical digest.

- n=high+1: commit watermark, native receipt when required, event/process projection and
  reducer changes;
- n=high matching digest: lost-reply duplicate, return saved result;
- same n/different digest: integrity error;
- n<high: stale, reject with watermark/cursor;
- n>high+1: hole, admit nothing until replay/gap.

For `event_id`, matching native receipt returns original Store event seq even after event
pruning, while new source sequence may advance. Digest mismatch is integrity failure.
Receipt reclaims only after generation supersession/deletion and all legal replay
sources/spools close.

A well-formed stale binding/turn/request may advance source sequence while producing one
bounded diagnostic. Malformed envelope/undeclared family does not.

Input/turn event shapes:

- `input.admitted {command_id,native_input_id?}`;
- `input.dropped {command_id,native_input_id?}`;
- `turn.started {turn_id,inputs:[{command_id,native_input_id?}]}`;
- `turn.inputs {turn_id,inputs:[...]}`;
- `turn.completed {turn_id,status,final_text?,usage?}`;
- `turn.resolved` — readback/owner resolution, distinct from plugin completion.

A command maps to at most one turn; multiple commands may map to one turn.

## Verbs and commands

Launch/resume with text creates session command plus linked send. Normally linked send
waits for ready. `start_requires_input` may carry it inside start, but session/input
outcomes remain separate. Ready never proves input.

| Verb | Semantics |
| --- | --- |
| launch | generation 1 after preflight/capacity/workspace readiness; target revision 1 |
| attach | acquire observe/control on canonical row; pointer moves only on exact success |
| send | FIFO input, applied only at exact turn association |
| steer/inject/interrupt | exact current-binding/current-turn semantics only |
| stop | exact generation/binding; force only with qualified custody/native isolation |
| resume | next generation from same subject, expected old binding, target revision 1 |
| approve/deny/answer | exact current-binding pending request |
| configure | explicit application point/readback; blocked by unresolved input |
| goal.* | bound to generation and binding revision |
| cancel | queued command only; admitted input needs native cancel/readback |
| detach | candidate observe revision becomes current only on exact success |
| compact/fork/background | native-only declared semantics |

Input text is at most 512 KiB. Large immutable notes/scripts/config use bounded
`ArtifactRef` rather than inline bytes.

```text
Command {
  id: CommandId, receipt_id?: WriteId,
  target?, selector?,
  expected_generation?, expected_binding_revision?,
  generation?, binding_revision?,
  turn_id?, request_id?,
  verb, args, origin, cause?, state, outcome?, created_at, updated_at
}
state = accepted → [queued →] dispatched → applied | rejected | failed | unknown
        queued → cancelled
```

External concrete/fanout parent has `id=receipt_id=write_id`. Linked input, selector
child, rule/timer/Goal command and daemon presentation use derived id/no receipt.

For ordinary native commands, expected and target generation/revision are the same
current pointer. Launch has no expected generation/revision and targets 1/1. Resume
expects old g/current revision and targets g+1/1. Attach/detach expect current revision
and target their candidate revision in the same generation. Pointer change stales older
queued commands before native I/O.

Interactive selector is a Store-only parent. One transaction resolves/sorts concrete
targets, checks limit, creates derived children/links and applies parent with
`{count,digest}`. Children settle/paginate independently.

One dispatched/unknown input gate blocks later send/configure/Goal in the same binding,
but not exact control/stop/readback.

Rejected means no native I/O; failed exact no-effect; unknown may have applied and is
never retried. Restart makes dispatched unknown. Generation/binding change stales queued
work. Exact readback/constrained owner resolution settles. Send applied means turn
association, not terminal success.

## Storage

Use bundled SQLite, WAL, `synchronous=FULL`, one writer and bounded group commit. Verify
application/schema identity before WAL. Reads use foreign keys, `query_only`, bounded
pages/deadlines and release transactions before network waits.

```sql
CREATE TABLE store_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE id_counters (
  name TEXT PRIMARY KEY,
  next INTEGER NOT NULL CHECK(next BETWEEN 1 AND 9223372036854775807)
);

CREATE TABLE write_receipts (
  id TEXT PRIMARY KEY,
  id_time INTEGER NOT NULL,
  principal TEXT NOT NULL,
  method TEXT NOT NULL,
  payload_digest BLOB NOT NULL CHECK(length(payload_digest) = 32),
  state TEXT NOT NULL,
  result_ref TEXT,
  final_result TEXT,
  admitted_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX write_receipts_expiry ON write_receipts(admitted_at)
  WHERE state = 'final';

CREATE TABLE agents (
  id INTEGER PRIMARY KEY,
  generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
  token_version INTEGER NOT NULL DEFAULT 0 CHECK(token_version >= 0),
  routing_enabled INTEGER NOT NULL DEFAULT 0,
  name TEXT UNIQUE, project TEXT NOT NULL,
  parent INTEGER REFERENCES agents(id),
  root INTEGER NOT NULL REFERENCES agents(id),
  depth INTEGER NOT NULL CHECK(depth >= 0),
  origin TEXT NOT NULL, harness TEXT NOT NULL, profile TEXT NOT NULL,
  native_scope TEXT, native_ref TEXT,
  workspace TEXT, workspace_state TEXT, branch TEXT, base_ref TEXT, base_oid TEXT,
  state TEXT NOT NULL, active_turn TEXT,
  stalled_since INTEGER, last_progress_at INTEGER,
  pending TEXT NOT NULL DEFAULT '[]', goal TEXT, effective TEXT,
  labels TEXT NOT NULL DEFAULT '{}', control TEXT NOT NULL DEFAULT 'self',
  observe_only INTEGER NOT NULL DEFAULT 0,
  guard_exceeded INTEGER NOT NULL DEFAULT 0,
  token_hash BLOB,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  CHECK(routing_enabled IN (0,1)),
  CHECK(observe_only IN (0,1)),
  CHECK(guard_exceeded IN (0,1)),
  CHECK(
    (token_hash IS NOT NULL AND length(token_hash) = 32
      AND routing_enabled = 1 AND generation > 0 AND token_version > 0
      AND observe_only = 0
      AND state IN ('starting','idle','busy','waiting'))
    OR
    (token_hash IS NULL AND
      (routing_enabled = 0 OR generation = 0 OR observe_only = 1
       OR state NOT IN ('starting','idle','busy','waiting')))
  ),
  CHECK(generation > 0 OR token_version = 0)
);
CREATE UNIQUE INDEX agents_native_subject ON agents(native_scope,native_ref)
  WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);

CREATE TABLE agent_bindings (
  agent INTEGER NOT NULL REFERENCES agents(id),
  generation INTEGER NOT NULL CHECK(generation > 0),
  revision INTEGER NOT NULL CHECK(revision > 0),
  supersedes_revision INTEGER,
  mode TEXT NOT NULL,
  profile_name TEXT NOT NULL,
  profile_revision TEXT NOT NULL,
  binding TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,revision),
  FOREIGN KEY(agent,generation,supersedes_revision)
    REFERENCES agent_bindings(agent,generation,revision),
  CHECK((revision = 1 AND supersedes_revision IS NULL) OR
        (revision > 1 AND supersedes_revision IS NOT NULL
          AND supersedes_revision < revision))
);

CREATE TABLE agent_binding_current (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation),
  FOREIGN KEY(agent,generation,revision)
    REFERENCES agent_bindings(agent,generation,revision)
);

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

CREATE TABLE plugin_source_watermarks (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  open_id TEXT NOT NULL,
  source_key TEXT NOT NULL,
  source_name TEXT NOT NULL,
  mechanism TEXT NOT NULL,
  dedupe_mode TEXT NOT NULL,
  open_digest BLOB NOT NULL CHECK(length(open_digest) = 32),
  high_water_seq INTEGER NOT NULL CHECK(high_water_seq >= 0),
  last_digest BLOB,
  last_store_seq INTEGER,
  native_cursor TEXT,
  state TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,source_key),
  UNIQUE(source_key),
  UNIQUE(agent,generation,open_id),
  CHECK(dedupe_mode IN ('strict_cursor','event_id','none')),
  CHECK((high_water_seq = 0 AND last_digest IS NULL AND last_store_seq IS NULL) OR
        (high_water_seq > 0 AND length(last_digest) = 32
          AND last_store_seq IS NOT NULL))
);

CREATE TABLE source_family_owners (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  family TEXT NOT NULL,
  source_key TEXT NOT NULL,
  PRIMARY KEY(agent,generation,family),
  FOREIGN KEY(agent,generation,source_key)
    REFERENCES plugin_source_watermarks(agent,generation,source_key)
);

CREATE TABLE native_event_receipts (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  kind TEXT NOT NULL,
  native_id TEXT NOT NULL,
  payload_digest BLOB NOT NULL CHECK(length(payload_digest) = 32),
  first_store_seq INTEGER NOT NULL,
  first_source_key TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,kind,native_id)
);
CREATE INDEX native_event_receipts_generation
  ON native_event_receipts(agent,generation,created_at);

CREATE TABLE commands (
  id TEXT PRIMARY KEY,
  receipt_id TEXT UNIQUE REFERENCES write_receipts(id),
  target INTEGER, selector TEXT,
  expected_generation INTEGER,
  expected_binding_revision INTEGER,
  generation INTEGER,
  binding_revision INTEGER,
  turn_id TEXT, request_id TEXT,
  verb TEXT NOT NULL, args TEXT NOT NULL, origin TEXT NOT NULL,
  cause TEXT, state TEXT NOT NULL, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  CHECK((receipt_id IS NOT NULL AND id = receipt_id) OR
        (receipt_id IS NULL AND id GLOB 'd_*')),
  CHECK((target IS NULL AND generation IS NULL AND binding_revision IS NULL) OR
        (target IS NOT NULL AND generation IS NOT NULL
          AND binding_revision IS NOT NULL)),
  CHECK((expected_binding_revision IS NULL AND expected_generation IS NULL) OR
        (target IS NOT NULL AND expected_binding_revision IS NOT NULL
          AND expected_generation IS NOT NULL)),
  FOREIGN KEY(target,generation,binding_revision)
    REFERENCES agent_bindings(agent,generation,revision),
  FOREIGN KEY(target,expected_generation,expected_binding_revision)
    REFERENCES agent_bindings(agent,generation,revision)
);
CREATE INDEX commands_target ON commands(target,created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');

CREATE TABLE command_links (
  parent_id TEXT NOT NULL REFERENCES commands(id),
  child_id TEXT NOT NULL UNIQUE REFERENCES commands(id),
  kind TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  PRIMARY KEY(parent_id,child_id),
  UNIQUE(parent_id,ordinal)
);
CREATE INDEX command_links_page ON command_links(parent_id,ordinal);

CREATE TABLE native_inputs (
  command_id TEXT PRIMARY KEY,
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  native_input_id TEXT,
  state TEXT NOT NULL,
  admitted_at INTEGER,
  first_observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX native_inputs_native_id
  ON native_inputs(agent,generation,native_input_id)
  WHERE native_input_id IS NOT NULL;

CREATE TABLE turns (
  agent INTEGER NOT NULL REFERENCES agents(id),
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  state TEXT NOT NULL,
  status TEXT,
  resolution_source TEXT,
  final_text TEXT, usage TEXT,
  started_at INTEGER,
  first_observed_at INTEGER NOT NULL,
  completed_at INTEGER,
  PRIMARY KEY(agent,generation,turn_id)
);
CREATE INDEX turns_recent ON turns(agent,generation,completed_at DESC)
  WHERE completed_at IS NOT NULL;

CREATE TABLE turn_inputs (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  command_id TEXT NOT NULL UNIQUE,
  ordinal INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,turn_id,command_id),
  UNIQUE(agent,generation,turn_id,ordinal),
  FOREIGN KEY(agent,generation,turn_id)
    REFERENCES turns(agent,generation,turn_id)
);
```

External receipt/domain admission commits together. Receipt may outlive domain history;
its bounded result preserves stable object/fanout id, count and digest. Internal commands
have no fake receipts.

`native_inputs.command_id` and `turn_inputs.command_id` intentionally lack command FKs so
input/turn readback survives command pruning. Admission/association still requires a
command/retained link. Unresolved input/turn/link rows never age-prune.

Binding rows are immutable. A failed candidate prunes only after every referencing
command/receipt is prunable; every current pointer and ancestry needed for readback
remains. Pointer movement, Agent projection/routing/capacity change, old-binding command
staling, Goal pause and applied transition outcome commit together.

Source open/handoff, source sequence, native receipt, event/projection/reducer changes
commit atomically. Event retention never deletes current watermarks/native receipts
needed for replay.

Counter/root allocation, input/turn association and reducer changes are transactional.
Failed result commit blocks new writes, retains the same pending commit where possible
and never repeats its external cause; crash leaves dispatched effects unknown.

WAL/readers are monitored. Replays release transactions; hard pressure rejects new long
reads and exposes pins, never deleting evidence or blind-truncating active readers.
Event retention follows active automation cursors. Current binding/input/edit/turn state,
receipts and source watermarks live in compact projections. Native stores are never
generically cleaned.
