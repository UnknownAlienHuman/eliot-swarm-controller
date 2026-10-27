# Core model

`eliot-core` owns pure data/reduction; `eliot-store` owns allocation, SQLite and queries.
Wire names are snake_case; timestamps are UTC Unix milliseconds; stored integers fit
signed 64-bit. This is the target contract: add later fields/projections with their
first roadmap caller, not as empty L1 subsystems.

## Identifiers

| Identity | Meaning |
| --- | --- |
| `AgentId = a<positive i64>` | stable Eliot Agent row |
| `generation` | nonnegative incarnation fence; 0 before first native dispatch |
| `CommandId` | external ULID or bounded derived id |
| `seq` | globally ordered persisted event id; never reuse a committed value |
| `MessageId = m<positive i64>` | concrete mail delivery row |
| `(native_scope,native_ref)` | opaque native thread/session in its store/account/server scope |
| `turn_id`, `request_id` | exact native execution/request within an Agent generation |
| `native_id` | stable normalized native fact identity within its source |

One native subject maps to one canonical Agent row. Attach/detach/resume update it;
a native fork has another subject and Agent. Optional names are unique and cannot look
like Agent ids/selectors. Public numeric identities must not be recycled after cleanup.

## Generation fence

First launch/attach targets generation 1. Resume, detach and a new/uncertain control
takeover target current + 1. Exact reconnect to the same live ownership may preserve it.
Every changing command stores expected and target generation.

Before native I/O, one dispatch transaction advances the fence, revokes old plugin/source
identity and token, stales old queued work, releases claims, pauses Goals and commits
target source ownership. When the target supports its own environment, create its one
random token in this transaction, before native tools can run.

Success installs the requested projection. Definite first-start/resume failure is failed;
definite takeover/detach failure is lost from Eliot's perspective because the previous
controller was revoked. Unknown keeps the target fence/token and blocks another
transition and ordinary input until exact readback/resolution. Never roll generation
back or rotate another token merely because a reply disappeared.

Only readback proving no process/session received the environment may clear an uncertain
target token. Exact attachment may carry a live native turn/request into the new
projection; otherwise it remains unknown. Old-generation facts are historical/debug
input, not current reducer input or automation triggers.

## External mutation ids

External mutations use a canonical ULID created before send. Identity binds id to
principal, method and canonical semantic payload; exclude JSON-RPC id, presentation,
paging and wait timeout. Same binding returns its retained result; a reused id with
different binding fails `id_conflict`. Absent ids outside the supported retry window
(default 24 h, shorter than retention) fail `id_expired`.

Commands store this directly; other writes use the API's small `writes` table. One
writer checks both namespaces. Derived ids identify linked inputs, selector children,
rule actions and Goal rounds. There is no universal receipt/floor subsystem.

## Agent

```text
Agent {
  id, generation, name?, project, parent?, root, depth,
  origin: eliot|native|external,
  harness, profile, profile_snapshot,
  native_scope?, native_ref?, workspace?, workspace_phase?, branch?, base_ref?, base_oid?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending, goal?, effective?, labels,
  control: self|tree|project, observe_only, own_identity, token?,
  created_at, updated_at
}
```

Snapshots contain bounded nonsecret values and admitted file references, not resolved
secrets. External Agents are observe-only until exact control attach. Native children
remain visible above controller fanout guards; limited children have no widened control
or independent identity. `own_identity=false` is never owner authority.

### States and reducer

| Input | Result |
| --- | --- |
| first launch/attach admission | created, generation 0 |
| start dispatch | target generation, starting |
| exact ready/attach snapshot | reported idle/busy/waiting |
| lost start reply | starting, start command unknown |
| exact turn start | busy with captured active turn |
| first request / last resolved request | waiting / busy |
| exact active turn terminal result | idle, clear that turn's pending requests |
| normal session end / fatal error / proven loss | stopped / failed / lost |

An open stop/transition is a command beside the observed state, not another state
machine. Generation transition blocks ordinary turn work. Terminal settlement cancels
undispatched work, revokes token/own identity, releases claims and pauses Goals atomically.
If a running turn has no terminal evidence, retain it as `unknown` before clearing the
active-turn pointer; do not erase or fabricate its result. Dispatched/unknown commands
remain historical and are never reissued.

Stall is a flag only for busy work with a healthy authoritative source. Waiting on a
request is waiting, not stalled solely through silence. Native progress clears a stall;
CPU/Git/mail/health noise does not. Stale source means progress unknown.

Compute `apply(state,input) -> (next_state,effects)` without I/O or mutating published
state. Store commits event, next projection and derived facts atomically, then publishes.

## Profile

```text
Profile {
  harness, extends?, cmd?, model?, effort?, permissions [workspace-write],
  args [], env {}, pass_env [], secret_env [], inherit_env [false], proxy?,
  workspace_mode [shared], control [self], git_identity [agent],
  system_note?, labels {}, stall_after [5m], max_live?, account?, billing?,
  setup [], scratch ["target/","node_modules/"],
  notify_parent [true], wake_on_mail [true]
}
```

Model/effort are native values. Display requested/effective values separately. Add
profile/account/proxy capacity with their callers; do not silently change a spending route.

## Events and authoritative sources

```text
Event { seq?, ts, kind, family?, agent?, generation?, source,
        native_id?, cursor?, payload_digest?, cause?, subject?, data }
SourceOwner { agent, generation, family, source, mechanism }
SourceState {
  agent, generation, source, state: open|closed, health: ok|slow|stale|error,
  cursor?, last_store_seq?, last_batch_id?, last_batch_digest?,
  last_batch_first_seq?, last_batch_last_seq?,
  gap_id?, gap_digest?, gap_from?, gap_to?
}
```

`subject` is the bounded event-time post-transition Agent projection for rules:
`{id,generation,project,parent,root,depth,harness,profile,state,workspace?,branch?,
control,observe_only,own_identity,labels}`. `cause` carries command/rule/Goal correlation
and loop depth. Do not later substitute the current Agent row when replaying rules.

Native families are session/turn/message/tool/request/child/goal/usage/provider/edit.
Exactly one source owns each implemented family per Agent generation. Daemon-owned
Agent/command/mail/rule/Git/process facts and `session.lost` cannot be supplied as
arbitrary plugin lifecycle. Persist bounded final facts; deltas/samples stay ephemeral.
Mask known secrets in native output; reject known secrets in caller durable text.

Lifecycle, health and gap metadata are independent. Handoff changes named family
ownership, not predecessor lifecycle. Cursor-backed recovery resumes strictly after
its opaque committed cursor. No cursor means live-edge recovery plus explicit gap,
not historical republication. The protocol owns exact batch/gap/handoff behavior.

### One-row commit acknowledgement

A source has at most one unacknowledged native-record batch. Daemon computes canonical
digests and commits the complete batch, cursor, projections and last-batch acknowledgement
together. Accept wholly new input or an exact duplicate of the last batch; changed id
payload/cursor or mixed new/duplicate events is `source_integrity`.

The last-batch acknowledgement survives event pruning. It is not a native-effect retry
or general event journal. A source with no stable native identity/cursor cannot safely
retry a lost acknowledgement and must report a gap/reconcile current state instead.

## Verbs

| Verb | Meaning |
| --- | --- |
| launch | Agent/session start; optional prompt is a linked send |
| attach / detach | exact subject control/observation transition, generation-fenced |
| send | input-to-turn association; terminal result is separate |
| steer / interrupt | exact captured active turn, no implicit fallback |
| inject | context at the reported safe point or next-send prepend |
| stop | end the controlled generation/session; not merely stop watching it |
| resume | explicit reopen of retained subject as new generation; no old input replay |
| approve / deny / answer | exact pending request |
| configure | report now/next_tool/next_turn/next_session application |
| goal.* | native/Eliot goal, added with automation |
| cancel | withdraw undispatched work |
| compact / fork / background | only supported native mechanisms |

Text is at most 512 KiB and must also fit the encoded plugin frame. Unsupported later
verbs are not advertised. No fake success or compatibility executor.

## Commands and narrow gates

```text
Command {
  id, principal, method, payload_digest, target?, selector?,
  expected_generation?, generation?, parent_command?, link_kind?, ordinal?,
  turn_id?, request_id?, verb, args, origin, cause?,
  state, outcome?, created_at, updated_at
}
state = accepted -> [queued ->] dispatched -> applied|rejected|failed|unknown
        accepted|queued -> cancelled
```

Launch/resume text creates a linked `<id>/send`. Selector expansion creates a Store-only
parent plus deterministically ordered concrete children; parent applied means expansion,
not children finished. Later automation uses the same parent/ordinal representation
with `link_kind=rule_action`; only its ordered actions wait for the preceding action.
Selector children remain independent. No second run/action journal is introduced.

Queued future sends are FIFO but do not block current-turn control/request replies.
An unresolved dispatched/unknown input without exact turn association blocks later
send/configure/Goal continuation. Active/unknown turn blocks the next turn; unknown
transition blocks another takeover. Stop/readback and valid exact request replies remain
available.

`rejected` proves no native I/O. `failed` means an authoritative unsuccessful completed
attempt and may include partial side effects; it is not rollback or permission to retry.
`unknown` means outcome cannot be established. An exact negative readback may explicitly
prove no effect; generic errors/timeouts may not. Restart makes dispatched work unknown;
only still-current undispatched work may continue. No external effect is automatically
re-executed merely because it failed or became unknown.

## Storage

Use bundled SQLite, WAL, synchronous FULL, one writer and bounded read queries. Commit
failure blocks new writes/dispatch and never repeats the external cause. The SQL below
is a target sketch, not a generator or a requirement to create unused L1 subsystems.

Public numeric ids use `INTEGER PRIMARY KEY AUTOINCREMENT`. Ordinary `INTEGER PRIMARY
KEY` can reuse a deleted high rowid and break replay cursors. Never reset sqlite_sequence
or explicitly reuse old ids; overflow fails. This uses SQLite's existing allocator, not
a custom identity service ([SQLite documentation](https://sqlite.org/autoinc.html)).

```sql
CREATE TABLE agents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  generation INTEGER NOT NULL DEFAULT 0,
  name TEXT UNIQUE, project TEXT NOT NULL,
  parent INTEGER REFERENCES agents(id), root INTEGER NOT NULL, depth INTEGER NOT NULL,
  origin TEXT NOT NULL, harness TEXT NOT NULL, profile TEXT NOT NULL,
  profile_snapshot TEXT NOT NULL,
  native_scope TEXT, native_ref TEXT,
  workspace TEXT, workspace_phase TEXT, branch TEXT, base_ref TEXT, base_oid TEXT,
  state TEXT NOT NULL, active_turn TEXT, stalled_since INTEGER, last_progress_at INTEGER,
  pending TEXT NOT NULL DEFAULT '[]', goal TEXT, effective TEXT,
  labels TEXT NOT NULL DEFAULT '{}', control TEXT NOT NULL DEFAULT 'self',
  observe_only INTEGER NOT NULL DEFAULT 0, own_identity INTEGER NOT NULL DEFAULT 0,
  token TEXT UNIQUE, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX agents_native ON agents(native_scope,native_ref)
  WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL, kind TEXT NOT NULL, family TEXT,
  agent INTEGER, generation INTEGER, source TEXT NOT NULL,
  native_id TEXT, cursor TEXT, payload_digest BLOB,
  cause TEXT, subject TEXT, data TEXT NOT NULL,
  CHECK(payload_digest IS NULL OR length(payload_digest)=32)
);
CREATE INDEX events_agent ON events(agent,seq);
CREATE INDEX events_kind ON events(kind,seq);
CREATE UNIQUE INDEX events_native ON events(agent,generation,source,native_id)
  WHERE native_id IS NOT NULL;
CREATE TABLE source_owners (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL,
  family TEXT NOT NULL, source TEXT NOT NULL, mechanism TEXT NOT NULL,
  PRIMARY KEY(agent,generation,family)
);
CREATE TABLE source_state (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL, source TEXT NOT NULL,
  cursor TEXT, last_store_seq INTEGER,
  last_batch_id TEXT, last_batch_digest BLOB,
  last_batch_first_seq INTEGER, last_batch_last_seq INTEGER,
  state TEXT NOT NULL, health TEXT NOT NULL,
  gap_id TEXT, gap_digest BLOB, gap_from TEXT, gap_to TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,source),
  CHECK(state IN ('open','closed')),
  CHECK(health IN ('ok','slow','stale','error')),
  CHECK((last_batch_id IS NULL AND last_batch_digest IS NULL
         AND last_batch_first_seq IS NULL AND last_batch_last_seq IS NULL)
        OR (last_batch_id IS NOT NULL AND last_batch_digest IS NOT NULL
         AND length(last_batch_digest)=32 AND last_batch_first_seq IS NOT NULL
         AND last_batch_last_seq IS NOT NULL
         AND last_batch_last_seq >= last_batch_first_seq)),
  CHECK((gap_id IS NULL AND gap_digest IS NULL AND gap_from IS NULL AND gap_to IS NULL)
        OR (gap_id IS NOT NULL AND gap_digest IS NOT NULL AND length(gap_digest)=32))
);
CREATE TABLE commands (
  id TEXT PRIMARY KEY,
  principal TEXT NOT NULL, method TEXT NOT NULL, payload_digest BLOB NOT NULL,
  target INTEGER, selector TEXT, expected_generation INTEGER, generation INTEGER,
  parent_command TEXT REFERENCES commands(id), link_kind TEXT, ordinal INTEGER,
  turn_id TEXT, request_id TEXT,
  verb TEXT NOT NULL, args TEXT NOT NULL, origin TEXT NOT NULL, cause TEXT,
  state TEXT NOT NULL, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  CHECK((parent_command IS NULL AND link_kind IS NULL AND ordinal IS NULL) OR
        (parent_command IS NOT NULL AND link_kind IS NOT NULL
         AND ordinal IS NOT NULL AND ordinal >= 0))
);
CREATE INDEX commands_target ON commands(target,created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');
CREATE UNIQUE INDEX commands_children ON commands(parent_command,ordinal)
  WHERE parent_command IS NOT NULL;
CREATE TABLE turns (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL, turn_id TEXT NOT NULL,
  state TEXT NOT NULL, status TEXT, resolution_source TEXT,
  final_text TEXT, usage TEXT, started_at INTEGER, completed_at INTEGER,
  PRIMARY KEY(agent,generation,turn_id)
);
```

Current projections and source recovery state outlive ordinary event retention. A new
batch replaces only its last-batch acknowledgement. Retained event/native-id history is
not an unlimited replay permit: cursor/no-cursor recovery obeys the protocol.

Retain parent/child command sets while required by open work or the supported history
window; prune the set transactionally, children before parent, so callers never see a
partly pruned expansion. Once gone, `command.children` returns `history_gap`.
