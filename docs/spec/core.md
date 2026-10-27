# Core model

`eliot-core` contains pure types and reducers. `eliot-store` owns allocation, SQLite and
queries. Wire names are `snake_case`; timestamps are Unix milliseconds UTC; persisted
integers fit SQLite signed `INTEGER`.

The identity model follows Agent Sessions and sandbox-driver: one stable logical
session/resource, one monotonically increasing incarnation fence and exact execution
ids below it. Eliot does **not** have binding revisions or a second lease epoch.

## Identifiers

| Identity | Form | Meaning |
| --- | --- | --- |
| `AgentId` | `a<positive i64>` | Stable Eliot row. |
| `generation` | nonnegative `i64`, per Agent | Current observation/control incarnation. 0 means no native incarnation has been admitted yet. |
| `CommandId` | external ULID or bounded derived string | Stable mutation identity. |
| `seq` | positive `i64` | Total order of persisted Eliot events. |
| `MessageId` | `m<positive i64>` | Mail delivery row. |
| native subject | `(native_scope,native_ref)` | Opaque vendor session/thread identity inside one store/server/account namespace. |
| `turn_id` | opaque string from plugin | One native execution/turn inside an Agent generation. |
| `request_id` | opaque string from plugin | One exact approval/question/request inside an Agent generation. |
| `native_id` | opaque string from plugin | One normalized native fact, stable across the overlap that its source may replay. |

Agent names are optional and unique. A name may not look like an Agent id or begin with
`@`.

**One native subject, one Agent.** A `(native_scope,native_ref)` belongs to at most one
Agent row. Observe attach, control attach, detach, resume and re-attach update that row;
a native fork is a new subject and Agent.

### Generation fence

`generation` is the only incarnation/control fence:

- an admitted first launch/attach targets generation 1;
- resume, detach or a new controller/plugin taking ownership of a still-live native
  subject targets current generation + 1;
- exact transport reconnect to the same still-current plugin/source ownership may keep
  the current generation; uncertain takeover increments it;
- every generation-changing command stores both `expected_generation` and
  `generation` (its target generation);
- its dispatch transaction advances the Agent to the target generation **before native
  I/O**, rotates or revokes direct routing identity as appropriate, stales queued work
  from the old generation, releases old-generation claims and pauses Goals;
- exact success updates the requested projection (for example controlled vs observe-only
  and effective profile). Definite failure does not decrement the fence. An unknown
  transition leaves the new generation current and blocks another transition or ordinary
  turn work until exact readback or explicit owner resolution;
- an exact attach snapshot may move an already-running native turn/request set into the
  target generation in the same transaction. Without exact readback those items become
  unknown; they are never guessed complete.

Every Agent-scoped command, plugin request/event, turn, pending request, claim, Goal and
native process registration carries the generation. Input from another generation is
historical/debug input only: it cannot reduce current state or fire normal automation.

## External write ids

Every external mutation carries a ULID generated before send. Idempotency identity is:

```text
(principal, method, canonical semantic payload, write_id)
```

The canonical payload excludes JSON-RPC id, output format, pager, client wait timeout and
other presentation fields.

- same id + same principal/method/payload returns the retained result;
- same id with any difference fails `invalid` (`id_conflict`);
- an absent id older than the supported retry window (default 24 h, while command/write
  rows are retained at least 30 days) fails `invalid` (`id_expired`);
- internal linked, selector and automation commands use bounded derived ids:
  `<launch>/send`, `<selector>/<agent>`, `rule:<rule>@<key>/<n>`,
  `goal:<agent>@<key>`.

Commands store principal/method/payload digest directly. Non-command mutations use the
small `writes` table defined by the API; there is no universal receipt/floor subsystem.
The single Store writer checks both namespaces before inserting an external id, so one
id cannot be reused for a different method.

## Agent

```text
Agent {
  id, generation, name?, project,
  parent?, root, depth,
  origin: eliot | native | external,
  harness, profile, profile_snapshot,
  native_scope?, native_ref?,
  workspace?, branch?, base_ref?, base_oid?, workspace_phase?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending: [Request], goal?, effective?,
  labels: map<string,string>,
  control: self | tree | project,
  observe_only: bool,
  own_identity: bool,
  token?,
  created_at, updated_at
}
```

`profile_snapshot` is the bounded resolved profile frozen at admission. It contains
literal nonsecret values, variable names and frozen note/setup/script bytes or
path+size+digest references—never resolved secret values.

`origin=native` children are always represented once their exact native subject is
known, even when Eliot's launch/tree guard is exceeded. An over-limit child is visible
and safely controllable through its parent plugin where supported, but gets
`control=self`, no project authority, no direct wake and no independent token unless the
plugin can inject a distinct environment before its tools run.

`origin=external` Agents are observe-only until an exact control attach succeeds.
`own_identity=false` means the session cannot authenticate its own Eliot CLI; it is
controlled through the parent/plugin. Eliot does not inject a CLI principal and absence
of a token never becomes owner.

### States

Live states are `starting`, `idle`, `busy`, `waiting`. An open stop or generation
transition is displayed beside the observed state; it does not create another synthetic
state machine.

| From | Generation-matched input | To/effect |
| --- | --- | --- |
| — | launch/first attach admitted | `created`, generation 0 |
| `created` or terminal | `start_dispatched {target_generation}` | `starting`, generation becomes target, token rotates when `own_identity` |
| live observe/control row | `transition_dispatched {target_generation}` | generation becomes target; retain last observed native state while the transition is open |
| `created` | stop/cancel before dispatch | `stopped`; linked input cancelled; no native I/O |
| `starting` | `session.ready` or exact `attached` snapshot | reported `idle|busy|waiting` |
| `starting` | definite pre-effect start failure | `failed` |
| `starting` | possible start with lost result | state remains `starting`; start command unknown; no replacement start |
| any transition | definite no-effect failure | retain the target generation and previous control projection; transition command failed |
| any transition | possible effect with lost result | retain target generation and previous projection; transition command unknown and blocks another transition/turn |
| `idle` | `turn.started {turn_id}` | `busy`, set `active_turn` |
| `busy` | first exact request | `waiting` |
| `waiting` | last request resolved | `busy` |
| `busy|waiting` | exact terminal result of `active_turn` | `idle`, clear turn requests |
| any live | exact native/session end | `stopped` |
| any live | fatal session error | `failed` |
| any live | exact loss of session control | `lost` |

Entering a terminal state cancels accepted/queued commands of that generation, clears
active turn/pending requests and pauses Goals. Dispatched/unknown commands stay
historical and are never reissued.

`stalled` is a flag. Only model-progress facts update `last_progress_at`: turn,
message, tool, plan, file edit and exact request/answer. CPU, Git polling, mail and
source health do not clear it. A stale authoritative source makes progress unknown,
not automatically stalled.

`apply(agent, input) -> Effects` is the only state reducer. It is pure; the Store
commits the source event, projection changes and derived `agent.<state>` event together.

## Profile

```text
Profile {
  harness, extends?, cmd?: [string],
  model?, effort?, permissions [workspace-write],
  args [[]], env [{}], pass_env [[]], secret_env [[]], inherit_env [false], proxy?,
  workspace_mode [shared], control [self], git_identity [agent],
  system_note?, labels [{}], stall_after [5m], max_live?, account?, billing?,
  setup [[]], scratch [["target/","node_modules/"]],
  notify_parent [true], wake_on_mail [true]
}
```

Profile/account/proxy caps all apply. Excess launches remain `created` FIFO. Model and
effort are native values, never translated across harnesses. Requested and effective
settings are displayed separately.

## Events and source ownership

```text
Event {
  seq?, ts, kind, family?, agent?, generation?,
  source, native_id?, cursor?, payload_digest?,
  cause?, subject?, data
}
```

- `subject` is the bounded post-transition Agent snapshot used by rules:
  `{id,generation,project,parent,root,depth,harness,profile,state,workspace?,branch?,
  control,observe_only,own_identity,labels}`. Rules never join a later Agent row for
  historical match/render.
- `cause` links commands/rules/timers/Goals and carries loop depth.
- persisted `data` and projected final text are bounded (default 64 KiB); full detail
  stays native.
- known credential values are masked from native output. Caller-supplied durable text
  containing a known credential is rejected.

Normalized native lifecycle families are `session`, `turn`, `message`, `tool`,
`request`, `child`, `goal`, and `usage`. Exactly one source owns each family for one
`(agent,generation)`.

```text
SourceOwner { agent, generation, family, source, mechanism }
SourceCursor { agent, generation, source, value, last_store_seq }
```

The owner map is committed before a plugin may perform the first native start/input.
A live stream normally owns lifecycle; transcript owns observe/recovery; a hook owns
only a family absent from the primary stream or one synchronous decision.

Handoff is one Store transaction: verify current owner, record its final cursor or an
explicit gap, install the new owner, then allow publication. There are no source epochs,
open ids or source sequence numbers.

A replayable source must provide an opaque exclusive cursor and resume strictly after
the committed cursor. A source with no such cursor starts at the live edge after
reconnect, emits `source.gap`, and never republishes history. `native_id` handles only
bounded overlap: same `(agent,generation,source,native_id)` and same canonical payload
returns the original `seq`; another payload is `source_integrity` and does not advance a
cursor.

Every event in a durable batch that the plugin may resend after a lost acknowledgement
must have a stable normalized `native_id`. Prefer the vendor id; for a cursor-backed
record derive it from the opaque record cursor plus a bounded record-local ordinal/kind.
A source with neither a stable id nor an exclusive cursor must not retry a lost
acknowledgement: it records a gap and reconciles current state instead.

Plugin records derived from one native record are submitted as one bounded atomic batch;
the cursor advances with the whole batch, so a crash cannot commit half a record.

### Main persisted kinds

- `session.ready|ended|error|lost`
- `agent.created|starting|idle|busy|waiting|stopped|failed|lost|stalled|unstalled`
- `input.admitted|dropped`
- `turn.started|completed|resolved`
- `message.user|agent`; ephemeral `message.delta`
- `tool.started|completed`, `file.changed`, `plan.updated`
- `approval.requested|resolved`, `question.asked|answered`
- `child.spawned`, `goal.updated`, turn-end `usage.updated`
- `provider.limited`
- `process.started|exited`; ephemeral `process.stats`
- `git.status|commit|ref`; ephemeral `fs.changed`
- `command.*`, `rule.*`, `mail.*`, `var.changed`, `claim.*`, `source.health|gap`

Plugins may emit `x.<harness>.*`; these never reduce core state unless promoted to a
documented family.

## Verbs

| Verb | Semantics |
| --- | --- |
| `launch` | Create Agent and generation 1 native session; optional prompt is a linked send. |
| `attach` | Observe or control one exact native subject on the canonical Agent row. A new control/observe lease targets the next generation. |
| `send` | Start/join a new native turn. Applied only at exact command→turn association. |
| `steer` | Add input to the exact active turn captured at admission. No implicit fallback. |
| `inject` | Add context at a reported native safe point or prepend to next send. |
| `interrupt` | Abort the exact captured active turn; session remains live. |
| `stop` | End the exact generation. Force only through exact Agent custody or isolated native session stop. |
| `resume` | Re-open terminal Agent/native subject as a new generation; never resend old input. |
| `approve|deny|answer` | Answer the exact pending request id in the captured generation. |
| `configure` | Change settings and report `now|next_tool|next_turn|next_session`. |
| `goal.*` | Native or Eliot Goal, generation-bound. |
| `cancel` | Withdraw queued command before native I/O. |
| `detach` | Release control as the next generation and remain observe-only; never kill an independent service session. |
| `compact|fork|background` | Native-only declared operations. |

Input text is at most 512 KiB UTF-8. Larger immutable material is referenced by a
frozen allowed file/digest or copied bounded bytes.

## Commands and narrow gates

```text
Command {
  id, principal, method, payload_digest,
  target?, selector?, expected_generation?, generation?,
  parent_command?, link_kind?, ordinal?,
  turn_id?, request_id?, verb, args, origin, cause?,
  state, outcome?, created_at, updated_at
}
state = accepted -> [queued ->] dispatched -> applied | rejected | failed | unknown
        queued -> cancelled
```

- launch/resume with text creates a linked input `<start-id>/send` in the same
  transaction. Session start and input settle independently. The linked input stores
  `parent_command=<start-id>`, `link_kind=linked_input`, `ordinal=0`.
- selector expansion creates one child per captured Agent with
  `parent_command=<selector-id>`, `link_kind=selector_target` and a deterministic
  ordinal sorted by Agent id. `command.children` pages this fixed set through the
  `(parent_command,ordinal)` index; no separate link state machine is needed.
- every concrete Agent command captures current `expected_generation`. Generation
  mismatch rejects before native I/O. A generation-changing command also stores its
  target `generation=current+1` and advances the fence in its dispatch transaction.
- queued sends stay FIFO but leave the ready lane while the Agent is busy; current-turn
  controls and exact request replies may pass them.
- one dispatched or unknown input with no exact turn association blocks later `send`,
  `configure` and Eliot Goal continuation in that generation. Exact interrupt, stop,
  request reply and readback remain available.
- an active/unknown turn blocks the next turn until exact terminal readback or explicit
  owner resolution. Resolution is recorded as owner assertion, never plugin completion.
- an unknown generation transition blocks another transition and ordinary turn work;
  exact readback/owner resolution settles only that transition.
- `rejected` proves no native I/O; `failed` proves no effect; `unknown` may have applied
  and is never retried.
- daemon restart changes `dispatched` to `unknown`; queued commands stay queued only if
  their generation is still current.
- selector expansion is one bounded transaction, sorted by Agent id, with derived child
  commands and paginated results.

## Storage

Use bundled SQLite, WAL, `synchronous=FULL`, one writer task with bounded group commit
and a small read pool. Nothing is reported accepted/applied before its transaction
commits. A commit failure blocks new writes and never repeats the external cause.

Layer 1 projections:

```sql
CREATE TABLE agents (
  id INTEGER PRIMARY KEY,
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
  token TEXT UNIQUE,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX agents_native ON agents(native_scope,native_ref)
  WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);

CREATE TABLE events (
  seq INTEGER PRIMARY KEY, ts INTEGER NOT NULL, kind TEXT NOT NULL, family TEXT,
  agent INTEGER, generation INTEGER, source TEXT NOT NULL,
  native_id TEXT, cursor TEXT, payload_digest BLOB,
  cause TEXT, subject TEXT, data TEXT NOT NULL
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
CREATE TABLE source_cursors (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL,
  source TEXT NOT NULL, value TEXT NOT NULL, last_store_seq INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,source)
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
        (parent_command IS NOT NULL AND link_kind IS NOT NULL AND ordinal >= 0))
);
CREATE INDEX commands_target ON commands(target,created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');
CREATE UNIQUE INDEX commands_children
  ON commands(parent_command,ordinal) WHERE parent_command IS NOT NULL;

CREATE TABLE turns (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL, turn_id TEXT NOT NULL,
  state TEXT NOT NULL, status TEXT, resolution_source TEXT,
  final_text TEXT, usage TEXT,
  started_at INTEGER, completed_at INTEGER,
  PRIMARY KEY(agent,generation,turn_id)
);
```

Current projections (`pending`, Goals, turns, source owners/cursors) survive event
retention. A cursor-backed source can no longer replay a record once its committed
exclusive cursor has advanced past it. A source without a cursor must start at the live
edge after reconnect and therefore cannot replay pruned history. Native-id rows are kept
at least through the source's declared bounded-overlap window; they may be pruned only
when the cursor or explicit closed/gapped boundary makes replay of that fact impossible.