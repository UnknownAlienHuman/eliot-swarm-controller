# Coordination

Durable mail, small variables, Git-backed `who` and advisory claims let agents
coordinate without another scheduler.

## Authority

Caller identity is exact `(agent,generation)` or automation principal. Invalid or
incomplete supplied identity is rejected, never owner.

| Principal | Command scope | Coordination scope |
| --- | --- | --- |
| owner | all | all |
| agent `self` | itself | project |
| agent `tree` | itself + descendants | project |
| agent `project` | project agents | project |
| global rule/timer dispatcher | owner actions after durable admission | all |
| project rule/timer dispatcher | project-agent actions after durable admission | project only |
| goal dispatcher | exact agent generation | that agent/project reads for check |
| managed script/setup/check token | read-only in principal/project scope | no direct mutation; return stdout actions |

This is correctness in a one-user system, not hostile same-user isolation. Commands pass
router generation/turn/request checks. Queue mail does not grant sender control; full
send/steer/inject does.

A rule principal belongs to daemon, not script process. After script exits 0, daemon
validates complete stdout, freezes targets and appends actions under parent principal.
Process token cannot mutate commands/mail/vars, resolve unknowns, run timers/rules or
spawn another script through Eliot. Workspace setup and Goal checks have same read-only
restriction.

A process still inside registered exact/degraded custody cannot obtain no-token owner by
clearing env: live membership classification requires scoped token or rejects. Deliberate
same-user `setsid` escape from degraded process group is outside declared containment
boundary and is reported as such; exact Windows/qualified Linux custody closes it.

Child inherits project/root. Requested control cannot exceed launcher and parent:
self→self; tree parent grants self/tree; project parent grants self/tree/project. Native
children default self unless owner-approved harness profile rule. Project principal
cannot change project/parent/root or choose profile outside allowlist.

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

Text ≤32 KiB; known secrets reject. Selector expansion is transactional, deterministic
by AgentId and subject to `max_selector_targets`; overflow rejects before any message.
Agent/project selectors cannot escape project. Unknown recipient fails. Broadcast above
a separate confirmation threshold requires owner/global-rule explicit confirmation but
still cannot exceed hard fanout limit.

Modes:

- queue: durable unread; recipient wake policy decides presentation;
- inject: full text at exact safe point; command rights required;
- send: full text as new turn; command rights required;
- steer: full text into exact active turn; exact steer/control required.

`presentation_kind=notice|full`; presented is not acknowledged.

### Capability-aware wake

For queue + `wake_on_mail=true`:

1. busy with exact inject → one generation-bound notice inject;
2. busy without inject → unread; same generation idle may get one short send notice;
3. idle → one short send notice;
4. observe-only, draining, pending request, matching provider hold or unknown control/
   input/turn → no presentation.

Wake can never be provider probe. No send/steer substitution while busy. At most one open
presentation command and one notice/minute/recipient. Unknown presentation is not
retried. New generation stales presentation but unread mail remains eligible. Startup
reconciles unread recipients with no open presentation.

### Inbox acknowledgement

One immutable open batch/recipient, max 100 messages and 1 MiB encoded data. Excess
waits next batch. `mail.inbox` returns same batch until acknowledged and holds no read
transaction while waiting. CLI writes complete stdout then receipted `mail.ack`. Lost
inbox leaves unread; lost ack reuses WriteId. `mail.read` rejects when message belongs to
open batch. History is paginated.

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

Unread/open batches never age-prune. Acknowledged history may prune after configured
retention; old cursor returns `history_gap`. Receipts follow independent minimum window
and FK rules.

## Shared variables

Scopes: project (agent/project rule default), tree root, global. Only owner/global rule
writes global. Values ≤16 KiB JSON/string, reject known secrets, increment version and
record principal/generation. `expect_version` is CAS.

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
| edited | live controlled current-generation edit projection whose path remains dirty |
| dirty | Git dirty path in a single-agent worktree |
| branch | committed path still different from frozen base OID |
| claim | current live controlled advisory intention |
| history | optional bounded authorship provenance only |

Current edit state comes from [durable attribution](monitoring.md#durable-edit-attribution),
not retained events. `agent_edits` rows for terminal/detached generations may remain
until path clean for provenance/cleanup, but `who` never reports them as current worker;
shared dirty paths with no live controlled attribution are `unattributed`. Queries use
cached projections and grant no authority.

Git identity is environment-only. Shared-checkout agents stage explicit paths; one
manager commits, or writers use worktrees.

## Claims

Claims are bounded repository-relative normalized globs with note/TTL and exact owner
`(agent,generation)`. Active only while generation live and controlled. Overlap with
another active claim or dirty/branch path emits conflict + one coalesced queue message.
Claims never prevent writes.

Terminal, resume, detach/observe-only or explicit release removes old generation claims
in same transaction. TTL is fallback. Project principal cannot claim outside project.

```sql
CREATE TABLE claims (
  id INTEGER PRIMARY KEY,
  project TEXT NOT NULL,
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  glob TEXT NOT NULL,
  note TEXT,
  expires_at INTEGER,
  created_at INTEGER NOT NULL,
  UNIQUE(agent,generation,glob)
);
CREATE INDEX claims_project ON claims(project,glob);
CREATE INDEX claims_owner ON claims(agent,generation);
```

## Hierarchy

Parent/root/depth are logical. Config limits fanout/depth. Native children upsert by
canonical subject with parent-generation provenance. Parent completion/goal notification
uses ordinary queue mail/wake.

Tree selectors resolve at admission; later work not included. Synchronous manager uses
exact send/wait/result; queue remains asynchronous.

## Prompt guidance

Default note lists available commands and says vars/`who` first, specific mail second,
no acknowledgements/status chatter/credentials. Manager profiles add exact launch/send/
wait/result/steer/interrupt/stop guidance.
