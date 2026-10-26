# Coordination

Durable mail, small variables, Git-backed `who` and advisory claims let agents
coordinate without another scheduler.

## Authority

Caller identity is exact `(agent,generation)` or an automation principal. Invalid or
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

A child inherits project/root. Its requested `control` cannot exceed both the launching
principal and its parent: self→self, tree parent may grant self/tree, project parent may
grant self/tree/project. Native children default to self unless the harness provides an
explicit owner-approved profile rule. A project principal cannot change project,
parent/root or select a profile outside the project's allowlist.

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

Text is at most 32 KiB and known secrets reject. Selector expansion is transactional
into concrete recipients. Agent/project automation selectors cannot escape project.
Unknown recipient fails. Large broadcast requires explicit owner/global-rule
confirmation.

### Modes

- queue: durable unread mail; recipient wake policy decides presentation;
- inject: full text at a declared exact safe point; requires command rights;
- send: full text as a new turn; requires command rights;
- steer: full text into the exact active turn; requires exact steer/control.

`presentation_kind=notice|full` distinguishes a mailbox notice from full message
context. Presented is not acknowledged/read.

### Capability-aware wake

For queue + `wake_on_mail=true`:

1. busy with exact inject → one generation-bound notice inject;
2. busy without inject → leave unread; when the same generation becomes idle, one short
   send notice may be admitted;
3. idle → one short send notice;
4. observe-only, draining, pending request, provider hold not designated for wake, or
   unknown control/input/turn → no presentation.

There is no send/steer substitute while busy. At most one open presentation command and
one notice/minute/recipient. Unknown presentation is not retried. A new generation makes
an old presentation stale but leaves unread mail eligible again. Startup/current-state
reconciliation finds unread recipients with no open presentation; no volatile wake edge
is required.

### Inbox acknowledgement

One immutable open batch exists per recipient. It contains at most 100 messages and at
most 1 MiB of encoded message data; excess unread messages wait for the next batch.
`mail.inbox` returns the same batch until acknowledged. It opens no read transaction
while waiting: `--wait` sleeps on a bounded notification, then performs a short query.

The CLI writes complete stdout, then calls `mail.ack(batch_id)` with a stable write id.
A lost inbox reply leaves unread; a lost ack is idempotently read back. `mail.read ID`
is allowed only when no open batch contains that message; otherwise acknowledge the
batch. `--all` is paginated acknowledged history.

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
CREATE INDEX messages_inbox ON messages(recipient,acknowledged_at,id);

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

Scopes are project (agent/project rule default), tree root or global. Only owner/global
rule writes global. Values are bounded to 16 KiB JSON/string, reject known secrets,
increment version and record principal/generation. `expect_version` is CAS.

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

Claims are bounded repository-relative normalized globs with note/TTL and exact owner
`(agent,generation)`. They are active only while that generation is both live and
controlled. Overlap with another active claim or dirty/branch path emits conflict and
one coalesced queue message. Claims never prevent writes.

Terminal transition, resume to a new generation, detach/observe-only transition or
explicit release removes the old generation's claims in the same transaction as the
control-state change. TTL is a fallback, not the primary cleanup path. A detached or
lost agent therefore cannot continue advertising current intent. Project principals
cannot claim outside their project.

## Hierarchy

Parent/root/depth are logical. Config limits accidental fan-out. Native children upsert
by canonical native subject with parent-generation provenance. Parent completion/goal
notifications use ordinary queue mail and wake rules.

Tree selectors resolve at admission; later-spawned work is not silently included.
Synchronous manager flow uses exact send/wait/result; queue remains asynchronous.

## Prompt guidance

Default note lists only available commands and says: vars/`who` first, specific mail
second, no acknowledgements/status chatter and no credentials. Manager profiles add
exact launch/send/wait/result/steer/interrupt/stop guidance.
