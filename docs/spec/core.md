# Core model

`eliot-core` contains pure types and reducers. `eliot-store` owns SQLite,
allocation and projections. Wire names are `snake_case`; every persisted integer fits
SQLite signed 64-bit.

## Scalar and identifier bounds

| Identity | Form/range | Rule |
| --- | --- | --- |
| `AgentId` | `a<1..i64::MAX>` | Transactional Store counter; stable logical row. |
| generation | `0..i64::MAX` | 0 before first native start; first incarnation 1; explicit resume increments. |
| external `WriteId` | canonical 26-character ULID | Required for every external daemon mutation. |
| internal `DerivedId` | `d_` + unpadded base32 BLAKE3-256 | Domain tag + length-delimited canonical components. |
| `CommandId` | `WriteId` or `DerivedId` | External concrete/fanout parent uses WriteId; internal child/automation uses DerivedId. |
| event `seq`, message id | positive signed-64 Store counter | Transactional allocation; never `MAX(id)+1`. |
| native subject | bounded `(native_scope,native_ref)` | Canonical vendor store/server namespace + opaque session/thread id. |

Allocation that would overflow fails `id_exhausted` before mutation. A derived-id
collision with a different canonical payload is integrity failure.

## External write idempotency and retention

Every owner/agent/API mutation obtains `WriteId` before send. Receipt lookup occurs
before age/floor checks:

1. existing id + same principal/method/canonical semantic payload returns current/
   original result;
2. existing id with mismatch fails `id_conflict`;
3. only absent id is checked against monotonic floor, live window and future skew;
4. valid absent id inserts a compact receipt in the same transaction as domain rows.

Canonical payload excludes transport/request id, output format, client timeout and
`wait_ms`. Receipt stores BLAKE3-256 digest and bounded sanitized result/reference.
Domain rows may be pruned earlier. Receipt pruning advances `write_id_floor_ms` through
the largest pruned ULID timestamp in the same transaction; later retry is
`id_expired`, never a new effect.

CLI uses a locked, atomically replaced monotonic ULID state file. Internal derived ids
never use external receipt namespace; their lifetime follows durable owner object.

## Canonical native subject

One native subject maps to one Agent row for its history. Observe clients subscribe to
that row. Attach upgrades/returns it; detach changes control on it. Resume retains the
subject; another subject is fork/new Agent. Scope changes before vendor ref reuse.

Native child registration supplies exact child scope/ref. Parent-local child ids use a
scope containing parent subject. Existing subject with different parent/root/project/
harness is integrity conflict, never silent reparenting.

## Agent and routing token

```text
Agent {
  id, generation, token_version,
  name?, project, parent?, root, depth,
  origin: eliot | native | external,
  harness, profile, native_scope?, native_ref?,
  workspace?, workspace_state?, branch?, base_ref?, base_oid?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending: [Request], goal?, effective?, labels,
  control: self | tree | project, observe_only,
  created_at, updated_at
}
```

Private `routing.key` deterministically derives raw
`eliot_at_<base64url>` from `(agent,generation,token_version)` via keyed BLAKE3. Store
keeps only domain-separated hash; comparison is constant-time. This lets a lost child/
reattach reply return the same current token without storing plaintext.

Token lifecycle:

- controlled live (`starting|idle|busy|waiting`, not observe-only): one hash and
  positive version;
- generation 0, terminal, detached, observe-only: no hash;
- terminal/detach clears hash without changing generation/version;
- explicit control reattach preserves generation, increments version and installs fresh
  token after exact attach;
- daemon/plugin recovery of already-controlled incarnation preserves version/hash;
- resume increments generation and version.

Missing/mismatched routing key blocks token issue/reissue but does not prevent
verification of already-presented tokens against stored hashes.

### States

Live states: `starting`, `idle`, `busy`, `waiting`. Open stop is displayed beside
observed state, never synthetic `stopping`.

| From | Exact input | Effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0, token version 0 |
| created | first start dispatched | generation 1, increment token version, hash token, `starting` |
| created | cancel/stop before dispatch | launch/linked input cancelled, `stopped` |
| terminal g | resume dispatched with expected g | generation g+1, increment token version, clear turn/requests, `starting` |
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

Resume row change occurs only when dispatch commits; one resume may be open. Cancelling
queued resume cancels linked input and leaves g.

Detach is valid only for independently owned/reattachable service lifetime. It preserves
observed state, sets observe-only, revokes token and releases claims transactionally.
Daemon-owned per-agent process must stop.

### Input and unknown turn gates

Vendor inbox acceptance without turn association keeps `send` dispatched.
`input.admitted` records command/native input id but does not free lane. Another
send/configure/goal continuation is blocked; exact control/stop/readback remains.
Pre-turn drop settles failed. Exact association settles command applied. Deadline after
possible admission produces unknown and preserves readback key/lane.

If reconnect proves session exists but not active-turn terminal outcome, turn becomes
unknown. Live Agent remains busy/waiting and blocks new turn/goal. Owner resolution is
allowed only after native-not-active proof or terminal generation and records
`turn.resolved`, never fabricated plugin completion. Unknown old generation never
crosses resume.

Only exact-generation model progress updates `last_progress_at`.

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

Environment/account/immutable launch binding are defined in
[configuration](configuration.md). Requested/effective facts remain separate.

## Events and source ownership

```text
Event {
  seq?, ts, kind, agent?, generation?, source,
  source_key?, source_seq?, native_id?, cause?, subject?, data
}
```

Agent event `subject` is bounded post-transition snapshot:
`{id,generation,project,parent,root,depth,harness,profile,state,workspace?,branch?,
control,observe_only,labels}`. It contains no token/credential/request text. Rules use
stored snapshot, never later Agent row.

Known secrets/token patterns are masked in native output. Semantic durable writes with
known secret reject instead of storing altered text.

### Authoritative source epoch

Plugin durably creates stable `open_id` before `source.open`. Store canonicalizes
`source_name`, declared families, mechanism and initial cursor into `open_digest`, then
maps `(agent,generation,open_id)` to one daemon `source_key`. Lost open reply returns the
same key/watermark; mismatched fields are integrity failure.

`source_family_owners` contains exactly one current owner per
`(agent,generation,family)`. Opening/handoff and closing old owner update watermark,
family rows and gap/cursor boundary in one transaction. A source cannot publish a kind
outside its declared families.

Each epoch emits contiguous positive `source_seq`, assigning exact typed envelope before
send and allowing at most one unacknowledged durable request. Daemon computes canonical
digest.

- n=high+1: commit watermark plus normalized event/process projection/reducer changes;
- n=high with matching digest: lost-reply duplicate, return saved Store seq even after
  history pruning;
- same n/different digest: integrity error;
- n<high: stale source state, reject with current watermark/cursor;
- n>high+1: gap, admit nothing until replay or explicit gapped close.

A well-formed vendor duplicate or stale turn/request may advance source sequence while
returning original event seq or a bounded diagnostic; it cannot fire lifecycle twice.
Malformed envelope/undeclared family does not advance.

Both `event.put` and qualified per-agent `process.put` use this contract. Native id/
cursor suppress vendor replay. Current-generation source rows remain while legal
reattach/replay is possible; reclaim after generation supersession/deletion and all
source/spool closure. Storage is O(source epochs), not events.

Input/turn events:

- `input.admitted {command_id,native_input_id}` — transport/inbox acceptance only;
- `input.dropped` — exact pre-turn drop/rejection;
- `turn.started {turn_id,input_commands[]}`;
- `turn.inputs` — later commands coalesced into same turn;
- `turn.completed` — exact native terminal/output/usage;
- `turn.resolved` — readback/owner resolution, distinct from plugin completion.

A command maps to at most one turn; multiple commands may map to one turn.

## Verbs and commands

Launch/resume with text creates session command plus linked send. Normally linked send
waits for ready. `start_requires_input` may carry it inside start, but session/input
outcomes remain separate. Ready never proves input.

| Verb | Semantics |
| --- | --- |
| launch | generation 1 after preflight/capacity/workspace readiness |
| send | FIFO input, applied only at exact turn/execution association |
| steer/inject/interrupt | exact current-turn/native-safe-point semantics only |
| stop | exact generation; force only with qualified custody/native isolation |
| resume | next generation from same canonical subject |
| approve/deny/answer | exact pending request |
| configure | explicit application point/readback; blocked by unresolved input |
| goal.* | generation-bound; continuation blocked by unresolved input/turn |
| cancel | queued command only; admitted input needs native cancel/readback |
| detach | attached/service-backed lifetime only; release control/revoke token |
| compact/fork/background | native-only declared semantics |

Input text at most 512 KiB UTF-8; larger content is file-referenced.

```text
Command {
  id: CommandId, receipt_id?: WriteId,
  target?, selector?, expected_generation?, generation?, turn_id?, request_id?,
  verb, args, origin, cause?, state, outcome?, created_at, updated_at
}
state = accepted → [queued →] dispatched → applied | rejected | failed | unknown
        queued → cancelled
```

External concrete/fanout parent has `id=receipt_id=write_id`. Linked input, selector
child, rule/timer/goal command and daemon presentation use derived id/no receipt.

Interactive selector is Store-only parent. One transaction resolves concrete targets,
checks `max_selector_targets`, creates derived children/links and marks parent applied
with `{count,digest}`. It does not claim child native effects. Exceeding limit rejects
before any row. Children settle independently. Child list is paginated; links remain
while any child open. `--wait` waits captured set only.

Ordinary expected generation equals target. Launch targets 1; resume expects g/targets
g+1. Ready calls serialize. One dispatched/unknown input gate blocks later send/
configure/goal in same generation but not control/stop/readback.

Rejected means no native I/O; failed exact no-effect; unknown may have applied and is
never retried. Restart makes dispatched unknown. Generation change stales queued work.
Exact readback/constrained owner resolution settles; time/new generation does not infer.
Send applied means turn association, not terminal success.

## Storage

Use bundled SQLite, WAL, `synchronous=FULL`, one writer and bounded group commit.
Verify application/schema identity before WAL. Read connections use foreign keys,
`query_only`, bounded pages/deadlines and release transactions before network waits.

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
  state TEXT NOT NULL,                 -- open|final
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
  token_hash BLOB,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  CHECK(
    (token_hash IS NOT NULL AND length(token_hash) = 32
      AND generation > 0 AND token_version > 0 AND observe_only = 0
      AND state IN ('starting','idle','busy','waiting'))
    OR
    (token_hash IS NULL AND
      (generation = 0 OR observe_only = 1
       OR state NOT IN ('starting','idle','busy','waiting')))
  ),
  CHECK(generation > 0 OR token_version = 0)
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
  open_id TEXT NOT NULL,
  source_key TEXT NOT NULL,
  source_name TEXT NOT NULL,
  mechanism TEXT NOT NULL,
  open_digest BLOB NOT NULL CHECK(length(open_digest) = 32),
  high_water_seq INTEGER NOT NULL CHECK(high_water_seq >= 0),
  last_digest BLOB,
  last_store_seq INTEGER,
  native_cursor TEXT,
  state TEXT NOT NULL,                 -- open|closed|gap
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,source_key),
  UNIQUE(source_key),
  UNIQUE(agent,generation,open_id),
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

CREATE TABLE commands (
  id TEXT PRIMARY KEY,
  receipt_id TEXT UNIQUE REFERENCES write_receipts(id),
  target INTEGER, selector TEXT,
  expected_generation INTEGER, generation INTEGER,
  turn_id TEXT, request_id TEXT,
  verb TEXT NOT NULL, args TEXT NOT NULL, origin TEXT NOT NULL,
  cause TEXT, state TEXT NOT NULL, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  CHECK((receipt_id IS NOT NULL AND id = receipt_id) OR
        (receipt_id IS NULL AND id GLOB 'd_*'))
);
CREATE INDEX commands_target ON commands(target,created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');

CREATE TABLE command_links (
  parent_id TEXT NOT NULL REFERENCES commands(id),
  child_id TEXT NOT NULL UNIQUE REFERENCES commands(id),
  kind TEXT NOT NULL,                   -- linked_input|selector_child
  ordinal INTEGER NOT NULL,
  PRIMARY KEY(parent_id,child_id),
  UNIQUE(parent_id,ordinal)
);
CREATE INDEX command_links_page ON command_links(parent_id,ordinal);

CREATE TABLE native_inputs (
  command_id TEXT PRIMARY KEY,
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  native_input_id TEXT NOT NULL,
  state TEXT NOT NULL,                  -- admitted|associated|dropped|unknown
  admitted_at INTEGER,
  first_observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(agent,generation,native_input_id)
);

CREATE TABLE turns (
  agent INTEGER NOT NULL REFERENCES agents(id),
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  state TEXT NOT NULL,                  -- running|completed|unknown
  status TEXT,                          -- completed|interrupted|failed
  resolution_source TEXT,               -- native|readback|owner
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

`native_inputs.command_id` and `turn_inputs.command_id` intentionally lack command FK so
input/turn readback survives command pruning. Admission/association transaction still
requires command or retained link. A direct `turn.started` observed before
`input.admitted` creates associated native-input projection with `admitted_at=NULL`.
Unresolved input/turn/link rows never age-prune.

Selector/linked rows remain while child work open. After normal terminal retention,
receipt summary preserves fanout count/digest/aggregate result; paginated child history
may return `history_gap` rather than inventing list.

`source.open` upserts by open id/digest and atomically installs family owners. Close/
handoff atomically updates owners and cursor/gap boundary. `event.put`/qualified
`process.put` lock watermark and commit watermark plus projection/normalized event.
Vendor duplicate advances watermark without duplicate lifecycle. History retention never
deletes current source watermark.

Input/turn association, counter/root/event allocation and reducer changes are
transactional. Failed result commit blocks new writes, retains same pending commit where
possible and never repeats external cause; crash leaves dispatched effects unknown.

WAL/readers are monitored. Replays release transactions; hard pressure rejects new long
reads and exposes pins, never deleting evidence or blind-truncating active readers.
Event retention follows active automation cursors. Current input/edit/turn state,
receipts and source watermarks live in compact projections. Native stores are never
generically cleaned.
