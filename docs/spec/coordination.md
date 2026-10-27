# Coordination

Durable mail, small shared variables, Git-backed `who` and advisory claims let Agents
coordinate without a manager relaying every word. They do not create another scheduler.

The mail model borrows CCCC's stable delivery identity and actor generation plus MCP
Agent Mail's recipient-scoped read/ack state.

## Authority

Caller identity is owner, `agent:<id>#<generation>` or scoped automation principal.
Missing/mismatched identity rejects; it never becomes owner.

| Principal | Command scope | Coordination scope |
| --- | --- | --- |
| owner | all Agents/projects | all |
| Agent `self` | itself | its project |
| Agent `tree` | itself + descendants | its project |
| Agent `project` | all Agents in its project | its project |
| global rule/timer/script | owner-authorized actions | all |
| project rule/timer/script | project Agents only | project only |
| Goal | exact bound Agent/generation | that Agent/project reads for checks |

A child never receives broader control than its parent. Every command still passes
current generation/turn/request checks.

`own_identity=false` means the native child/external session has no independent Eliot
principal. It is observed/controlled through its parent plugin. Eliot does not expose an
owner-fallback CLI to it.

Queue mail does not grant command authority. Full-content `inject|send|steer` requires
ordinary command rights on the recipient.

## Mailbox

One `Message` row is one concrete recipient delivery:

```text
Message {
  id, delivery_id,
  sender, sender_generation?,
  recipient, recipient_generation,
  thread?, reply_to_delivery?,
  mode, text, content_digest,
  admitted_at,
  presented_at?, presentation_command?, read_at?
}
```

- direct send uses the external mutation id as `delivery_id`;
- selector expansion captures concrete recipient ids/generations in one bounded
  transaction, sorts them by Agent id and creates one row per recipient with derived
  delivery id `<parent-write-id>/<recipient-agent>`. Rows share one `thread` id;
- same delivery id + different sender generation, recipient generation, mode or body
  digest is `id_conflict`;
- a later replacement generation is never silently retargeted. An unread delivery for
  an older recipient generation is historical: it cannot be presented or acknowledged
  by the replacement generation. Owner/history queries may inspect it; the sender must
  create a new delivery to reach the replacement;
- unknown recipient is an error; it never creates identity;
- text is at most 32 KiB and known credential values are rejected;
- reply references the original delivery and stays inside the original participant set.

### Delivery modes

- `queue` (default): durable unread message. It never starts or changes a model turn by
  itself.
- `inject`: full text at the recipient's next supported safe point; command rights
  required.
- `send`: full text as a new turn; command rights required.
- `steer`: full text in the exact active turn; command rights and native steer required.

Full delivered text is clearly marked:

```text
[eliot message m12 from a7#3 writer-1 | reply-to: delivery-id]
<text>
```

### Queue wake policy

Profile field `wake_on_mail` is one boolean:

- false: queue only;
- true + recipient has current own identity: daemon may create one coalesced generic
  notice, at most one/minute and at most one open notice command:
  `[eliot] N unread messages; run: eliot inbox`;
- true + recipient has no own identity: route one generic notice to the nearest live
  routed parent/project manager; if none exists, remain queue-only.

A queue wake **never includes the message body**, never acts as a provider probe and
never marks mail read. Unknown notice is not retried blindly. Draining, pending native
request, unknown input/turn/control command or provider hold suppresses new wake.

Parent completion notices obey the same policy.

### Inbox acknowledgement

`mail.inbox` returns at most 100 oldest unread deliveries for the caller's exact
`(agent,generation)` and their exact delivery ids; it marks nothing. CLI writes complete
output and then calls:

```text
mail.ack { id: <write id>, deliveries: [delivery-1,delivery-2,...] }
```

Only rows whose recipient and recipient generation match the authenticated caller are
marked read. A crash or lost reply may display them again but cannot lose them; messages
arriving after the fetch are untouched. Ack is idempotent. `mail.read` reads/acks one
exact current-generation delivery.

Unread deliveries of the **current** generation do not expire. An unread delivery from
an older terminal/replaced generation is historical, never reassigned, and may be pruned
after configured mail-history retention once no reply/thread reference needs it.
Acknowledged history follows the same bounded retention. Requests before retained
history return `history_gap` rather than silently skipping rows.

```sql
CREATE TABLE messages (
  id INTEGER PRIMARY KEY,
  delivery_id TEXT NOT NULL UNIQUE,
  sender TEXT NOT NULL,
  sender_generation INTEGER,
  recipient INTEGER NOT NULL,
  recipient_generation INTEGER NOT NULL,
  thread TEXT,
  reply_to_delivery TEXT,
  mode TEXT NOT NULL,
  text TEXT NOT NULL,
  content_digest BLOB NOT NULL,
  admitted_at INTEGER NOT NULL,
  presented_at INTEGER,
  presentation_command TEXT,
  read_at INTEGER
);
CREATE INDEX messages_inbox
  ON messages(recipient,recipient_generation,read_at,id);
CREATE INDEX messages_thread ON messages(thread,id);
```

## Shared variables

Scopes are `project`, `tree` or `global`. Agents/project automation may access their
project; only owner/global automation writes global.

Values are JSON/string ≤16 KiB. Each write increments `version`; `expect_version` is
compare-and-set. `set_by` includes Agent generation or automation principal. Known
credentials are rejected. Every change emits `var.changed`.

```sql
CREATE TABLE vars (
  scope TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  version INTEGER NOT NULL,
  set_by TEXT NOT NULL,
  note TEXT,
  set_at INTEGER NOT NULL,
  PRIMARY KEY(scope,key)
);
```

## Who works on this

`eliot who <pathspec>...` combines:

| Signal | Source | Meaning |
| --- | --- | --- |
| `edited` | current-generation `agent_edits` whose path remains dirty | this Agent changed the path |
| `dirty` | Git status in a worktree used by one live Agent | uncommitted now |
| `branch` | committed path still different from frozen base OID | committed but unmerged |
| `claim` | current-generation claim | announced intention |
| `history` | optional bounded Git log | recent authorship only |

Git identity is injected through environment, never repository config. Shared checkouts
cannot attribute every dirty byte; unmatched paths are `unattributed`. `who` queries
cached/projection data and at most one bounded log; it does not run status itself.

The durable `agent_edits` projection is owned and defined by the
[Git/filesystem monitor](monitoring.md#git-and-filesystem-monitor). Coordination only
reads it; it does not declare or maintain a second table.

When Git proves a path clean, its edit rows are removed. Old-generation rows may remain
for cleanup provenance but are not reported as current workers.

## Claims

Claims are advisory normalized project-relative globs with note/TTL:

```text
eliot claim 'src/api/**' --note "routes refactor" --ttl 2h
eliot release 'src/api/**'
```

They belong to exact `(agent,generation)`. Generation change, terminal state, detach,
expiry or explicit release removes them. Overlap with another live claim or another
Agent's dirty/branch set emits `claim.conflict` and queues one coalesced notice. Claims
never prevent writes.

```sql
CREATE TABLE claims (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  glob TEXT NOT NULL,
  note TEXT,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,glob)
);
```

## Hierarchy

`launch --parent A` inherits project/root and sets depth +1. Configuration guards
controller-created work (`max_depth`, `max_children_per_agent`, `max_tree_agents`) but
cannot erase a native child the harness already created.

A native child above a guard is recorded with `over_limit=true`, `control=self`, no
project authority, no independent token/wake and only safe parent-mediated native verbs.

Tree selectors capture their membership at command admission. Parent completion/Goal
notifications are queue mail. A manager needing synchronous reaction uses exact
`send --wait`, `wait @children` and `result`; queue delivery remains asynchronous.

## Prompt guidance

Only Agents with current own identity receive Eliot CLI guidance. The default note lists
commands present in the running layer and says:

```text
Use shared vars and `eliot who` first, specific addressed messages second.
Do not broadcast routine status or acknowledgements; the controller already exposes it.
Never put credentials in messages, vars or claims.
```

Manager profiles additionally receive exact launch/send/wait/result/steer/interrupt/stop
commands. Routing-disabled children are not told to call Eliot independently.
