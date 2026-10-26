# Core model

`eliot-core` contains pure types and reducers. `eliot-store` owns SQLite,
allocation and projections. Wire names are `snake_case`; every persisted integer fits
SQLite signed 64-bit.

## Scalar and identifier bounds

| Identity | Form/range | Rule |
| --- | --- | --- |
| `AgentId` | `a<1..i64::MAX>` | Transactional Store counter; stable logical row. |
| generation | `0..i64::MAX` | 0 before first native start; first incarnation 1; explicit resume increments. |
| external `WriteId` | canonical 26-character ULID | Required for every external mutation. |
| internal `DerivedId` | `d_` + unpadded base32 BLAKE3-256 | Domain tag plus length-delimited canonical components. |
| `CommandId` | `WriteId` or `DerivedId` | External command uses its write id; linked, selector-child and automation commands use derived ids. |
| event `seq`, message id | positive signed-64 Store counter | Transactional allocation; never `MAX(id)+1`. |
| native subject | bounded `(native_scope,native_ref)` | Canonical vendor store/server namespace plus opaque session/thread id. |

Counter exhaustion fails `id_exhausted` before mutation. A derived-id collision with a
different canonical payload is an integrity failure, never idempotent success.

### External write idempotency and retention

Every owner/agent/API mutation obtains a `WriteId` before the request leaves the
client. The daemon performs receipt lookup **before** timestamp-floor checks:

1. existing id + same principal/method/canonical payload returns the original/current
   result;
2. existing id with any mismatch fails `id_conflict`;
3. only an absent id is checked against the persisted floor, live idempotency window
   and future-skew limit;
4. a valid absent id inserts one compact receipt in the same transaction as the domain
   mutation.

The receipt stores a BLAKE3-256 payload digest and a bounded sanitized result/reference.
Terminal domain rows may be pruned earlier. Receipt pruning advances
`write_id_floor_ms` monotonically through the largest pruned ULID timestamp in the same
transaction, so a later retry is `id_expired`, never a new effect.

The CLI uses a current-user-only, locked monotonic ULID state file. Generation and file
replacement are atomic; concurrent CLI processes cannot intentionally reuse one local
state transition. A missing file is seeded from daemon `write_id_floor_ms` plus fresh
randomness.

Internal derived ids never consume the external receipt namespace. Their dedupe lifetime
is defined by the durable parent command, rule/action, timer, goal or source object.

## Canonical native subject

One native subject maps to one Agent row for its history. Observe clients subscribe to
that row. Attach returns or upgrades it; detach changes control on the same row. Resume
retains the subject; a different subject is a fork/new Agent. Scope must change before
a vendor may reuse refs.

Native child registration supplies child scope/ref. Parent-local child ids use a scope
containing the exact parent native subject. Parent generation is provenance, not
identity.

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
`eliot_at_<base64url>` token, stores only a domain-separated BLAKE3-256 hash and injects
the raw value once. Hash comparison is constant-time; the prefix remains pattern-
redactable after restart.

Token presence follows routing lifecycle:

- controlled live (`starting|idle|busy|waiting`, not observe-only) has one hash;
- generation 0, terminal, detached and observe-only have no hash;
- terminal/detach revocation clears the hash in the same transaction without changing
  generation;
- an explicit control reattach to the same still-live incarnation preserves generation
  but issues a fresh token after exact native attach;
- daemon/plugin recovery of an already controlled incarnation preserves its existing
  hash/token; resume creates the next generation and a new token.

Invalid, revoked or stale supplied identity is rejected, never owner.

### States

Live states are `starting`, `idle`, `busy`, `waiting`. An open stop is displayed beside
the observed state, never as synthetic `stopping`.

| From | Exact input | Effect |
| --- | --- | --- |
| — | launch admitted | `created`, generation 0 |
| created | first start dispatched | generation 1, token rotation, `starting` |
| created | cancel/stop before dispatch | launch and linked send cancelled, `stopped` |
| terminal g | resume dispatched with expected g | generation g+1, token rotation, clear turn/requests, `starting` |
| starting | matching `session.ready` | `idle` |
| starting | definite no-effect start failure | `failed`, revoke token; linked send fails without I/O |
| starting | lost reply after possible start | state unchanged; start unknown; linked send blocked |
| idle | exact turn start t | `busy`, `active_turn=t` |
| busy | first exact pending request | `waiting` |
| waiting | last exact request resolved | `busy` |
| busy/waiting | exact completion of active turn | `idle`, clear turn/requests |
| any live | exact normal end/stop applied | `stopped`, revoke token, clear turn/requests, pause goals |
| any live | fatal session error | `failed`, revoke token, clear turn/requests, pause goals |
| any live | exact custody/session loss | `lost`, revoke token, clear turn/requests, pause goals |

Resume expects g and targets g+1; the row changes only when dispatch commits. Only one
resume may be open. Cancelling a queued resume cancels its linked send and leaves g.

Detach is valid only for an attached/service-backed lifetime that remains independently
observable and reattachable. It preserves observed state, sets observe-only, revokes the
token and releases claims transactionally. A daemon-owned per-agent process must stop.

An asynchronous input accepted by a vendor inbox but not associated with a native turn
keeps `send` dispatched. `input.admitted` records the command and native input id without
changing Agent state or freeing the input lane. Another send, configure or goal
continuation is blocked; exact control/stop/readback remains allowed. Exact pre-turn
drop settles failed. Exact turn association settles command applied. Deadline after
possible admission produces unknown and preserves the same readback key/lane.

If reconnect proves the session exists but cannot prove the active turn terminal
outcome, the turn becomes unknown. A live Agent remains busy/waiting and blocks new
turn/goal work. Exact history may settle it. Owner `turn.resolve` is allowed only when
native readback proves not active or generation terminal; it records `turn.resolved`,
never fabricated plugin completion.

A terminal generation may retain historical unknown work; resume starts a new
generation and unknown never crosses it. Only exact-generation model progress updates
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
a local narrowing; account/proxy caps are shared. Requested/effective route facts stay
separate and are never silently substituted.

## Events

```text
Event {
  seq?, ts, kind, agent?, generation?, source,
  source_key?, source_seq?, native_id?, cause?, subject?, data
}
```

For an agent event, `subject` is a bounded post-transition snapshot:
`{id,generation,project,parent,root,depth,harness,profile,state,workspace?,branch?,
control,observe_only,labels}`. It contains no token, credential, request text or
unbounded effective settings. Rules use this snapshot, never a later Agent row.

Known secrets/token patterns are masked in native output/logs. Semantic durable writes
containing a known secret reject rather than storing altered text.

### Commit-acknowledged source sequence

The daemon allocates one bounded `source_key` per authoritative ownership epoch. The
source emits a strictly contiguous positive `source_seq`, durably assigning the exact
typed envelope before send and permitting at most one unacknowledged persisted
`event.put` per key. Later native facts may enter a bounded spool but are sent in order.

The daemon computes a versioned/domain-separated BLAKE3-256 digest over length-prefixed
scalar fields and recursively key-sorted normalized JSON.

- `n = high_water + 1`: commit watermark, event and reducer/projection changes, then
  acknowledge;
- `n = high_water` with matching digest: lost-reply duplicate; return saved Store seq
  even if the event row was pruned;
- same sequence/different digest: integrity error;
- older sequence: reject with current watermark/cursor;
- future sequence with a hole: gap; admit nothing until replay or explicit gapped close.

`native_id`/native cursor suppress vendor replay. A plugin restart reads the host
watermark before publishing. Normal reconnect reuses source key; a source handoff gets
a new key only after the old boundary is committed. Current-generation source rows
remain while legal reattach/replay is possible; reclaim only after generation
supersession/deletion and source/spool closure. Storage is O(ownership epochs), not
events.

Input/turn events:

- `input.admitted {command_id,native_input_id}` — inbox/transport acceptance only;
- `input.dropped` — exact pre-turn terminal rejection/drop;
- `turn.started {turn_id,input_commands[]}`;
- `turn.inputs` — additional exact commands coalesced into the same turn;
- `turn.completed` — exact native terminal status/output/usage;
- `turn.resolved` — readback/owner resolution, distinct from plugin completion.

A command belongs to at most one turn. Multiple commands may map to one native turn
without inventing multiple terminal results.

## Verbs and linked initial input

Launch/resume with text creates a session command plus linked send. Normally send waits
for ready. A `start_requires_input` plugin receives the linked command inside start, but
session/input outcomes remain separate; ready never proves input.

| Verb | Semantics |
| --- | --- |
| launch | create row/start generation 1 after preflight, capacity and workspace readiness |
| send | one input; FIFO; applied only at exact turn/execution association |
| steer/inject/interrupt | exact current-turn/native-safe-point semantics only |
| stop | exact generation; force only with qualified custody/native isolation |
| resume | next generation from the same canonical subject |
| approve/deny/answer | exact pending request |
| configure | explicit application point/readback; blocked by unresolved input |
| goal.* | generation-bound; continuation blocked by unresolved input/turn |
| cancel | queued command only; admitted native input requires native cancel/readback |
| detach | attached/service-backed lifetime only; release control/revoke token |
| compact/fork/background | native-only declared semantics |

Input text is at most 512 KiB UTF-8; larger content is file-referenced.

## Commands

```text
Command {
  id: CommandId,
  receipt_id?: WriteId,
  target?, selector?, expected_generation?, generation?, turn_id?, request_id?,
  verb, args, origin, cause?, state, outcome?, created_at, updated_at
}
state = accepted → [queued →] dispatched → applied | rejected | failed | unknown
        queued → cancelled
```

An external command has `id = receipt_id = write_id`. A linked initial send, selector
child, rule/timer/goal command or daemon presentation has a derived id and no external
receipt. The parent external receipt returns their ids through its bounded result.

An interactive selector command is a Store-only fan-out parent. Its transaction creates
concrete derived child commands and links, then marks the parent applied with the child
set; it does **not** claim the child native effects applied. Child commands execute and
settle independently. CLI `--wait` on a selector waits the exact child set captured at
admission and returns per-child outcomes.

Ordinary expected generation equals target generation. Launch targets 1. Resume expects
g and targets g+1. Ready calls serialize. Queued inputs remain FIFO; one dispatched/
unknown input gate blocks later send/configure/goal in that generation but not exact
control, stop or readback.

Rejected means no native I/O; failed means exact no-effect; unknown may have applied and
is never retried. Restart makes dispatched unknown. Generation change stales older
queued work. Exact readback or constrained owner resolution settles unknown; time/newer
generation does not infer an old result.

For send, applied means exact turn/execution association, not inbox acceptance or
terminal success. Resolving send never manufactures a turn. Resolving start applied
does not synthesize ready.

## Storage

Use bundled SQLite, WAL, `synchronous=FULL`, one writer and bounded group commit.
Verify application/schema identity before WAL. Read connections use foreign keys,
`query_only`, bounded pages/deadlines and release transactions before network waits.

```sql
CREATE TABLE store_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- store_meta['write_id_floor_ms'] is a monotonically increasing integer string.

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
    (token_hash IS NOT NULL AND length(token_hash) = 32
      AND generation > 0 AND observe_only = 0
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
        (high_water_seq > 0 AND length(last_digest) = 32
          AND last_event_seq IS NOT NULL))
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
  kind TEXT NOT NULL,                   -- linked_input|selector_child|presentation
  ordinal INTEGER NOT NULL,
  PRIMARY KEY(parent_id,child_id),
  UNIQUE(parent_id,ordinal)
);

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

External mutation receipt and domain row commit together. An open receipt follows its
domain object; terminalization stores a bounded sanitized summary. The receipt outlives
pruned domain history until its independent idempotency window expires.

`commands.receipt_id` is nullable by design: internal commands are durable through their
parent/run/cause, not fake external receipts. `native_inputs.command_id` and
`turn_inputs.command_id` intentionally have no command FK so compact command history may
be pruned while exact command→native-input→turn readback survives. Their insert/update
transactions still require the command/link to exist at admission time. Unresolved
input/turn/link rows are never age-pruned.

Selector/linked command rows and links remain while any child is open. After all child
work is terminal and normal command retention permits pruning, the external receipt's
bounded final summary preserves the captured child ids/outcomes.

Receipt pruning advances the ULID floor transactionally. `event.put` locks the source
watermark and commits watermark, event and projections together. A duplicate of the one
possible unacknowledged sequence returns `last_event_seq`; event retention never deletes
source watermarks.

A current-generation source row remains while legal reattach/replay is possible. It is
reclaimed only after generation supersession/deletion and source/spool closure. A
terminal but still-current generation keeps it.

`input.admitted` updates `native_inputs`; unknown native admission time stays NULL and
`first_observed_at` is honest. Association inserts `turn_inputs` with the turn event.
Attach/readback may discover a turn with unknown `started_at`; observation time is never
presented as vendor start time. Wait follows input then turn, and results survive event
and command-history retention.

Counter/root/event allocation is transactional. Failed result commit blocks writes,
retains the same pending commit where possible and never repeats the external cause;
crash leaves dispatched effects unknown.

WAL/readers are monitored. Replays release transactions; hard pressure rejects new long
reads and exposes the pin, never deleting evidence or blind-truncating active readers.
Event retention follows active automation cursors. Current input/edit/turn state,
external receipts and source watermarks live in compact projections. Native stores are
never generically cleaned.
