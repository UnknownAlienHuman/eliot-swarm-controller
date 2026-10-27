# Core model

`eliot-core` contains pure types and reducers. `eliot-store` owns allocation, SQLite and
queries. Wire names are `snake_case`; timestamps are Unix milliseconds UTC; persisted
integers fit SQLite signed `INTEGER`.

The identity model copies Agent Sessions and sandbox-driver: one stable logical Agent/
native subject, one monotonically increasing incarnation fence and exact execution ids
below it. Eliot has no binding revisions or second lease epoch.

## Identifiers

| Identity | Form | Meaning |
| --- | --- | --- |
| `AgentId` | `a<positive i64>` | Stable Eliot row. |
| `generation` | nonnegative `i64`, per Agent | Current observation/control incarnation; 0 before first native dispatch. |
| `CommandId` | external ULID or bounded derived string | Stable mutation identity. |
| `seq` | positive `i64` | Total order of persisted Eliot events. |
| `MessageId` | `m<positive i64>` | One concrete mailbox delivery. |
| native subject | `(native_scope,native_ref)` | Opaque vendor session/thread id inside one store/server/account namespace. |
| `turn_id` | opaque plugin value | One native turn/execution inside an Agent generation. |
| `request_id` | opaque plugin value | One exact approval/question inside a generation. |
| `native_id` | opaque plugin value | One normalized fact inside one native-record batch. |

Names are optional and unique; they may not resemble Agent ids or selectors.

**One native subject, one Agent.** A `(native_scope,native_ref)` belongs to at most one
Agent row. Attach, detach, resume and controller replacement update that row; a native
fork is a new subject and Agent.

## Generation fence

`generation` is the only incarnation/control fence:

- first launch/attach targets generation 1;
- resume, detach or a new/uncertain controller takeover targets current + 1;
- exact transport reconnect to the same still-current controller/source ownership may
  preserve generation;
- every generation-changing command stores `expected_generation` and target
  `generation`;
- its dispatch transaction advances the Agent fence **before native I/O**, revokes old
  plugin/source principal and old Agent token, stales old queued work, releases claims,
  pauses Goals and commits target source owners/state;
- when direct Agent identity is possible, the same transaction creates one random
  target-generation token. The plugin/native process receives only that token in its
  frozen environment; there is no routing key, token derivation or binding revision;
- exact success installs the requested control/observe/profile projection;
- definite first-start/resume failure becomes `failed`; definite takeover/detach failure
  becomes `lost` from Eliot's perspective because the old controller was revoked;
- unknown transition keeps target generation/current token and blocks another
  transition plus ordinary turn work until exact readback/owner resolution. Generation
  never rolls back and another token is not rotated merely because the reply was lost;
- readback may clear the target token only when it proves no process/session could have
  received the target environment;
- an exact attach snapshot may move a running native turn/request set into the target
  generation. Without exact readback those items remain unknown, never guessed complete.

Every Agent-scoped command, plugin request/event, turn, pending request, claim, Goal and
process registration carries generation. Another generation is historical/debug input
only: it cannot reduce current state or fire normal automation.

## External mutation ids

Every external mutation carries a ULID generated before send. Its identity is:

```text
(principal, method, canonical semantic payload, id)
```

Canonical payload excludes JSON-RPC id, output/pager and client wait timeout.

- same tuple returns retained result;
- same id with another principal/method/payload is `invalid/id_conflict`;
- absent id older than the supported retry window (default 24 h, while rows live at
  least 30 days) is `invalid/id_expired`;
- linked/selector/automation commands use bounded derived ids such as `<launch>/send`,
  `<selector>/<agent>`, `rule:<rule>@<key>/<n>` and `goal:<agent>@<key>`.

Commands store principal/method/payload digest directly. Non-command mutations use the
small `writes` table in the API. The single Store writer checks both id namespaces; no
universal receipt/floor subsystem exists.

## Agent

```text
Agent {
  id, generation, name?, project,
  parent?, root, depth,
  origin: eliot | native | external,
  harness, profile, profile_snapshot,
  native_scope?, native_ref?,
  workspace?, workspace_phase?, branch?, base_ref?, base_oid?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending: [Request], goal?, effective?, labels,
  control: self | tree | project,
  observe_only, own_identity, token?,
  created_at, updated_at
}
```

`profile_snapshot` is the bounded resolved profile and frozen plugin binding from
[configuration](configuration.md). It stores literal nonsecret values, variable names
and bounded bytes or canonical path+size+digest references—never resolved secret values.

Native children are always recorded once their exact subject is known, even above
controller fanout guards. An over-limit child is visible and safely parent-controlled
where supported, but gets `control=self`, no project authority/wake and no independent
token unless the plugin can inject an isolated environment before tools run.

External Agents are observe-only until exact control attach succeeds.
`own_identity=false` means no independent Eliot CLI principal; absence of token never
means owner.

### States and reducer

Live states: `starting`, `idle`, `busy`, `waiting`. Open stop/transition is shown beside
the last observed state; it is not another state machine. While a generation transition
is open, ordinary turn work is blocked and no controller is assumed current.

| From | Generation-matched input | Effect |
| --- | --- | --- |
| — | launch/first attach admitted | `created`, generation 0 |
| `created` or terminal | `start_dispatched {target_generation}` | generation becomes target; `starting`; old identity revoked |
| live row | `transition_dispatched {target_generation}` | generation becomes target; old controller/source/token revoked; diagnostic prior state retained while command open |
| `created` | stop/cancel before dispatch | `stopped`; no native I/O |
| `starting` | exact `session.ready`/attach snapshot | reported `idle|busy|waiting` |
| `starting` | definite no-effect failure | `failed` |
| `starting` | possible start with lost reply | remains `starting`; command unknown |
| open takeover/detach | exact success | install target projection/state |
| open takeover/detach | definite no current controller | `lost` |
| open takeover/detach | possible effect, lost reply | retain unknown transition gate |
| `idle` | `turn.started` | `busy`, set exact `active_turn` |
| `busy` | first pending request | `waiting` |
| `waiting` | last request resolved | `busy` |
| `busy|waiting` | terminal result of exact active turn | `idle`, clear turn requests |
| any live | exact normal session end | `stopped` |
| any live | fatal session error | `failed` |
| any live | daemon-derived exact loss of control | `lost` |

Entering `stopped|failed|lost` cancels accepted/queued commands of that generation,
clears active turn/requests, releases claims, pauses Goals and atomically clears
`token`/`own_identity`. Dispatched/unknown commands remain historical and are never
reissued. Token revocation is required even on degraded Unix where an escaped process
may survive.

`stalled` is a flag. Only generation-matched model progress updates
`last_progress_at`: turn/message/tool/plan/edit and exact request/answer. CPU, Git poll,
mail and source health do not clear it. A stale authoritative source means progress
unknown, not automatically stalled.

`apply(agent,input) -> Effects` is the only state reducer. It is pure. Store commits the
source event, projections and derived `agent.<state>` event together.

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
effort are native values. Requested and effective settings are displayed separately.

## Events and authoritative sources

```text
Event {
  seq?, ts, kind, family?, agent?, generation?,
  source, native_id?, cursor?, payload_digest?,
  cause?, subject?, data
}
```

- `subject` is bounded post-transition Agent state for rules:
  `{id,generation,project,parent,root,depth,harness,profile,state,workspace?,branch?,
  control,observe_only,own_identity,labels}`;
- `cause` links commands/rules/timers/Goals and carries loop depth;
- persisted data/final text are bounded; full detail stays native;
- known credentials are masked from native output; caller durable text containing one
  is rejected;
- daemon computes `payload_digest` from one versioned canonical semantic encoding.

Plugin-published native families are `session`, `turn`, `message`, `tool`, `request`,
`child`, `goal`, `usage`, `provider`, `edit`. Exactly one source owns each family for
one `(agent,generation)`.

```text
SourceOwner { agent, generation, family, source, mechanism }
SourceState {
  agent, generation, source,
  state: open | closed | gap,
  cursor?, last_store_seq?,
  last_batch_id?, last_batch_digest?,
  last_batch_first_seq?, last_batch_last_seq?,
  gap_id?, gap_digest?, gap_from?, gap_to?
}
```

Initial owner/state commits before native I/O. Live stream normally owns controlled
lifecycle; transcript/history owns observe/repair; hook owns only an assigned missing
fact or immediate decision.

Plugins cannot publish daemon-owned `agent.*`, `session.lost`, `turn.resolved`, command,
rule, mail, claim, process or Git events. They publish native
`session.ready|ended|error` and exact native facts; daemon reconciliation derives loss.

A replayable source supplies an opaque exclusive cursor and resumes strictly after it.
A source without one reconnects at live edge, records one idempotent gap and never
publishes old history. Dynamic ownership change is one predecessor-aware CAS handoff
with final cursor or gap. There are no source epochs, open ids or source sequence
numbers.

### One-row commit acknowledgement

One native record becomes one bounded atomic `events.put` batch. The plugin supplies a
stable `batch_id` and stable per-event `native_id`; daemon computes event and batch
digests. Plugin permits at most one unacknowledged batch per source.

Store accepts only:

- a wholly new batch; or
- an exact duplicate of the **last committed batch** recorded in `source_state`.

Same batch/native id with another digest/cursor, or a mixed new/duplicate set, is
`source_integrity`. New batch commit replaces the one-row last-batch receipt. After a
lost reply, plugin/replacement reads `source.state` and retries only if that exact batch
is still the last receipt. A source with neither stable record id nor cursor cannot
retry; it records a gap and reconciles current state.

This keeps commit acknowledgement durable after ordinary event pruning without a
seen-events table or adapter journal. Cross-source overlap is prevented by owner/handoff;
a no-cursor source is contractually live-edge-only after reconnect.

### Persisted kinds

- native: `session.ready|ended|error`, `input.admitted|dropped`,
  `turn.started|completed`, message/tool/edit/plan/request/child/goal/usage/provider;
- daemon: `session.lost`, `turn.resolved`, Agent state/stall, commands/rules/mail/vars/
  claims/source health/gap;
- process/Git: process start/exit and Git status/commit/ref;
- ephemeral: deltas, in-turn usage, process stats, filesystem invalidation.

Unknown `x.<harness>.*` diagnostics never reduce core state unless promoted to a defined
family.

## Verbs

| Verb | Semantics |
| --- | --- |
| `launch` | Create Agent and generation-1 session; optional prompt is linked send. |
| `attach` | Observe/control exact native subject on canonical row; new lease targets next generation. |
| `send` | Start/join native turn; applied only at exact command→turn association. |
| `steer` | Input to exact active turn captured at admission; no implicit fallback. |
| `inject` | Context at reported safe point or prepend to next send. |
| `interrupt` | Abort exact captured turn; session remains live. |
| `stop` | End exact generation; force only exact custody/isolated native stop. |
| `resume` | Reopen terminal Agent as new generation; never resend old input. |
| `approve|deny|answer` | Answer exact pending request in captured generation. |
| `configure` | Change settings and report `now|next_tool|next_turn|next_session`. |
| `goal.*` | Native/Eliot Goal, generation-bound. |
| `cancel` | Withdraw queued command before native I/O. |
| `detach` | Release control as next generation, remain observe-only, never kill independent service. |
| `compact|fork|background` | Native-only declared operations. |

Input text is at most 512 KiB. Larger immutable material is bounded bytes or a frozen
allowed file reference.

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

- launch/resume with text creates linked `<start-id>/send` with parent/link/ordinal;
- selector parent creates deterministic per-Agent children sorted by Agent id; API pages
  them via parent/ordinal;
- every concrete command captures expected generation; generation-changing dispatch
  advances fence/token/source plan before native I/O;
- queued sends stay FIFO but leave ready lane while Agent busy, so current-turn controls
  and request replies may pass;
- one dispatched/unknown input without turn association blocks later send/configure/
  Eliot Goal; exact control/request reply/readback remains available;
- active/unknown turn blocks next turn; unknown generation transition blocks another
  transition and ordinary work;
- `rejected` proves no native I/O, `failed` proves no effect, `unknown` may have applied
  and is never retried;
- restart changes dispatched to unknown; queued work survives only while captured
  generation remains current;
- selector expansion is one bounded transaction and parent applied means only expansion
  committed.

## Storage

Use bundled SQLite, WAL, `synchronous=FULL`, one writer with bounded group commit and a
small read pool. Nothing is reported accepted/applied before commit. Commit failure
blocks new writes and never repeats its external cause.

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
  state TEXT NOT NULL,                    -- open|closed|gap
  gap_id TEXT, gap_digest BLOB, gap_from TEXT, gap_to TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,source),
  CHECK(state IN ('open','closed','gap')),
  CHECK((last_batch_id IS NULL AND last_batch_digest IS NULL
         AND last_batch_first_seq IS NULL AND last_batch_last_seq IS NULL)
        OR
        (last_batch_id IS NOT NULL AND length(last_batch_digest)=32
         AND last_batch_first_seq IS NOT NULL
         AND last_batch_last_seq >= last_batch_first_seq)),
  CHECK((gap_id IS NULL AND gap_digest IS NULL)
        OR (gap_id IS NOT NULL AND length(gap_digest)=32))
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
  final_text TEXT, usage TEXT, started_at INTEGER, completed_at INTEGER,
  PRIMARY KEY(agent,generation,turn_id)
);
```

Current projections survive event retention. `source_state` last-batch receipt remains
until replaced by the next committed batch or the source is safely closed/reclaimed.
Ordinary old event/native-id rows may then prune because cursor sources cannot replay
behind the committed cursor and no-cursor sources reconnect only at live edge plus gap.
Linked/selector command sets follow [D-35](../decisions.md): open/unknown children retain
the parent, terminal children prune first and the complete set disappears before
`command.children` returns `history_gap`.
