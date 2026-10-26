# Coordination

Agents coordinate through durable mail, small shared variables, Git-backed `who` and
advisory claims. These are not another scheduler or security boundary.

## Authority

Caller identity is exact `(agent,generation)` from the current token. Owner has no
token. Invalid/incomplete supplied agent identity is rejected, never treated as owner.

| Caller | Commands | Coordination scope |
| --- | --- | --- |
| owner | all | all |
| `control=self` | itself | project |
| `control=tree` | itself + descendants | project |
| `control=project` | project agents | project |
| automation/script | owner authority, still routed/validated | all |

`send`, `steer` and full-text `inject` are commands and require control rights. Queue
mail does not: the recipient's profile controls whether/how unread mail is presented.

## Mailbox

```text
Message {
  id, sender, sender_generation?, recipient,
  thread?, reply_to?, mode, text,
  admitted_at,
  presented_at?, presented_generation?, presentation_command?,
  acknowledged_at?
}
```

Text is ≤32 KiB and rejected if it contains a known secret. Selector expansion happens
at mail admission into one concrete message per recipient. Unknown recipients are
errors. Large broadcast requires explicit owner/rule confirmation.

### Modes

- `queue` (default): durable unread mail; no sender control required.
- `inject`: full text at an exact native safe point; requires control and declared
  inject mechanism.
- `send`: full text as a new turn; requires control.
- `steer`: full text to exact active turn; requires control and exact steer support.

Delivered full text names sender generation and reply command.

### Capability-aware wake

For `queue` with `wake_on_mail=true` Eliot may present a short notice
`[eliot] N unread messages; run eliot inbox`:

1. busy + exact inject supported → generation-bound inject at next safe point;
2. busy + no inject → keep unread; do not substitute send/steer. When the same
   generation later becomes idle, one short send notice may be admitted;
3. idle → one short send notice if sending is supported/controlled by daemon policy;
4. observe-only, draining, pending request or unknown control → no presentation;
   unread mail remains recoverable.

At most one open presentation command and one notice/minute per recipient. Unknown
presentation is not retried blindly. Replacement generation makes the old presentation
stale; unread messages may be considered anew for the new generation.

Startup/reconnect performs level reconciliation: every recipient with unread mail,
wake enabled and no open presentation is reconsidered from current state. No volatile
wake edge is required.

### Inbox acknowledgement

`mail.inbox` returns unread messages plus a stable batch id without marking them read.
The CLI writes complete stdout, then calls `mail.ack(batch_id)` with a stable id. Lost
inbox response leaves messages unread; lost ack response is idempotently readable.
`mail.read ID` explicitly reads/acknowledges one message. `--all` includes history.

```sql
CREATE TABLE messages (
  id INTEGER PRIMARY KEY,
  sender TEXT NOT NULL,
  sender_generation INTEGER,
  recipient INTEGER NOT NULL,
  thread INTEGER,
  reply_to INTEGER,
  mode TEXT NOT NULL,
  text TEXT NOT NULL,
  admitted_at INTEGER NOT NULL,
  presented_at INTEGER,
  presented_generation INTEGER,
  presentation_command TEXT,
  acknowledged_at INTEGER
);
CREATE INDEX messages_inbox ON messages(recipient, acknowledged_at, id);

CREATE TABLE mail_batches (
  id TEXT PRIMARY KEY,
  recipient INTEGER NOT NULL,
  message_ids TEXT NOT NULL,
  acknowledged_at INTEGER,
  created_at INTEGER NOT NULL
);
```

## Shared variables

Scopes: project (default for agent), tree root, or global. Values are bounded JSON/string
≤16 KiB and cannot contain known secrets. Every write increments version and stores
exact caller generation. `expect_version` is CAS; mismatch is `bad_state`.

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

Signals:

| Signal | Meaning |
| --- | --- |
| `edited` | current-generation native edit attribution whose path remains dirty |
| `dirty` | Git dirty path in a worktree used by one live agent |
| `branch` | committed path still different from base |
| `claim` | current advisory intent |
| `history` | optional bounded authorship provenance only |

Current edit attribution comes from the durable `agent_edits` projection in
[monitoring](monitoring.md#durable-edit-attribution), not retained event history.
Shared checkout paths without exact attribution are `unattributed`. `who` never runs a
fresh status per request and never grants authority.

Agent Git identity is environment-only. Shared-checkout agents stage explicit paths;
one manager commits, or writers use separate worktrees.

## Claims

Claims are generation-bound advisory globs with note/TTL. Overlap with another live
claim or dirty/branch path emits conflict and queues one coalesced message. Claims do
not prevent writes. They expire/release on generation end. Paths/globs are bounded,
normalized, repository-relative and reject traversal.

## Hierarchy

Parent/root/depth are logical relationships. Config limits prevent accidental fan-out.
Native children upsert by canonical native subject, with exact parent generation as
provenance. Parent completion/goal notifications are queue mail and obey the same
wake/capability rules.

Tree selectors resolve at the admitting transaction. Work spawned afterward is not
silently included. A manager needing synchronous results uses exact `send --wait` or
`wait`; queue mail remains asynchronous.

## Prompt guidance

Default note is short and lists only available commands. It tells agents to use vars and
`who` first, specific mail second, avoid acknowledgements/status chatter, and never put
credentials in coordination channels. Manager-capable profiles additionally receive
launch/send/wait/result/steer/interrupt/stop commands.
