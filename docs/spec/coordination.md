# Coordination

Durable mail, small variables, Git-backed `who` and advisory claims let agents
coordinate without another scheduler.

## Authority

Caller identity is exact `(agent,generation)` or an automation principal. Invalid/
incomplete supplied identity is rejected, never owner.

| Principal | Command scope | Coordination scope |
| --- | --- | --- |
| owner | all | all |
| agent `self` | itself | project |
| agent `tree` | itself + descendants | project |
| agent `project` | project agents | project |
| global rule/timer/script | owner | all |
| project rule/timer/script | project agents | that project; no global vars/cross-project mail |
| goal principal | its exact agent generation | that agent/project reads needed for check |

This is a correctness boundary in a one-user system, not hostile same-user isolation.
Commands still pass router generation/turn/request checks. Queue mail does not grant
sender control; full send/steer/inject does.

## Mailbox

```text
Message {
  id, sender principal, sender_generation?, recipient,
  thread?, reply_to?, mode, text,
  admitted_at,
  presented_at?, presented_generation?, presentation_kind?, presentation_command?,
  acknowledged_at?
}
```

Text ≤32 KiB; known secrets reject. Selector expansion is transactional into concrete
recipients. Agent/project automation selectors cannot escape project. Unknown recipient
fails. Large broadcast requires explicit owner/global-rule confirmation.

### Modes

- queue: durable unread mail, recipient wake policy decides presentation;
- inject: full text at declared exact safe point, requires command rights;
- send: full text as a new turn, requires command rights;
- steer: full text into exact active turn, requires exact steer/control.

`presentation_kind=notice|full` distinguishes a mailbox notice from full message
context. Presented is not acknowledged/read.

### Capability-aware wake

For queue + `wake_on_mail=true`:

1. busy with exact inject → one generation-bound notice inject;
2. busy without inject → leave unread; when same generation becomes idle, one short
   send notice may be admitted;
3. idle → one short send notice;
4. observe-only, draining, pending request, provider hold not designated for wake, or
   unknown control → no presentation.

No send/steer substitute while busy. At most one open presentation command and one
notice/minute/recipient. Unknown presentation is not retried. New generation makes old
presentation stale but unread mail eligible again. Startup/current-state reconciliation
finds unread recipients with no open presentation; no volatile wake edge is required.

### Inbox acknowledgement

One immutable open batch exists per recipient. `mail.inbox` returns that same batch
until acknowledged; messages arriving later remain for the next batch. It does not
mark read. CLI writes complete stdout, then `mail.ack(batch_id)` with a stable write id.
Lost inbox leaves unread; lost ack is idempotently read back. `mail.read ID` explicitly
acknowledges one message. `--all` includes history.

```sql
CREATE TABLE messages (
  id INTEGER PRIMARY KEY,
  sender TEXT NOT NULL, sender_generation INTEGER,
  recipient INTEGER NOT NULL,
  thread INTEGER, reply_to INTEGER,
  mode TEXT NOT NULL, text TEXT NOT NULL,
  admitted_at INTEGER NOT NULL,
  presented_at INTEGER, presented_generation INTEGER,
  presentation_kind TEXT, presentation_command TEXT,
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
CREATE UNIQUE INDEX mail_open_batch ON mail_batches(recipient)
  WHERE acknowledged_at IS NULL;
```

## Shared variables

Scopes: project (agent/project rule default), tree root, global. Only owner/global rule
writes global. Values are bounded ≤16 KiB JSON/string, reject known secrets, increment
version and record principal/generation. `expect_version` is CAS.

```sql
CREATE TABLE vars (
  scope TEXT NOT NULL, key TEXT NOT NULL,
  value TEXT NOT NULL, version INTEGER NOT NULL,
  set_by TEXT NOT NULL, note TEXT, set_at INTEGER NOT NULL,
  PRIMARY KEY(scope,key)
);
```

## Who works on this

| Signal | Meaning |
| --- | --- |
| edited | generation-matched native edit projection, path still dirty |
| dirty | Git dirty path in a single-agent worktree |
| branch | committed path still different from base |
| claim | current advisory intention |
| history | optional bounded authorship provenance only |

Current edit state comes from
[durable attribution](monitoring.md#durable-edit-attribution), not retained event
history. Shared checkout paths without exact attribution are `unattributed`. `who`
queries cached projections and never grants authority.

Git identity is environment-only. Shared-checkout agents stage explicit paths; one
manager commits, or writers use worktrees.

## Claims

Generation-bound, repository-relative, normalized globs with note/TTL. Overlap with
another live claim or dirty/branch path emits conflict and one coalesced queue message.
Claims never prevent writes and expire/release on generation end. Project principals
cannot claim outside project.

## Hierarchy

Parent/root/depth are logical. Config limits accidental fan-out. Native children upsert
by canonical native subject with parent generation provenance. Parent completion/goal
notifications use ordinary queue mail and wake rules.

Tree selectors resolve at admission; later-spawned work is not silently included.
Synchronous manager flow uses exact send/wait/result; queue remains asynchronous.

## Prompt guidance

Default note lists only available commands and says: vars/`who` first, specific mail
second, no acknowledgements/status chatter, no credentials. Manager profiles add exact
launch/send/wait/result/steer/interrupt/stop guidance.
