# Coordination

Durable mail, small variables, Git-backed `who` and advisory claims let Agents
coordinate without another scheduler.

## Authority

Caller identity is an explicit owner, Agent/generation or automation principal
credential. Missing/incomplete/revoked identity rejects; it never becomes owner.

| Principal | Command scope | Coordination scope |
| --- | --- | --- |
| owner | all | all |
| Agent `self` | itself | project |
| Agent `tree` | itself + descendants | project |
| Agent `project` | project Agents | project |
| global rule/timer dispatcher | owner actions after durable admission | all |
| project rule/timer dispatcher | project-Agent actions after durable admission | project only |
| Goal dispatcher | exact Agent generation | that Agent/project reads for check |
| managed script/setup/check | read-only in principal/project scope | no direct mutation; return stdout actions |

This is correctness in a one-user system, not hostile same-user isolation. Commands pass
router generation/turn/request checks. Queue mail does not grant sender control; full
send/steer/inject does.

A rule principal belongs to daemon, not the script process. After exit 0, daemon
validates complete stdout, freezes targets and appends actions under the parent
principal. Process credential cannot mutate commands/mail/vars, resolve unknowns, run
timers/rules or spawn another script through Eliot. Workspace setup and Goal checks use
the same read-only restriction.

Child inherits project/root. Requested control cannot exceed launcher and parent:
self→self; tree parent grants self/tree; project parent grants self/tree/project. Native
children default self. A guard-exceeding native child is still recorded but gets
`control=self`, `routing_enabled=false`, no project authority and no automatic wake.
Project principal cannot change project/parent/root or select a profile outside its
allowlist.

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

Text is at most 32 KiB and known secrets reject. Selector expansion is transactional,
deterministic by AgentId and subject to `max_selector_targets`; overflow rejects before
any message. Agent/project selectors cannot escape project. Unknown recipient fails.
Broadcast above a separate confirmation threshold requires owner/global-rule explicit
confirmation but still cannot exceed the hard fanout limit.

Modes:

- `queue`: durable unread; recipient delivery policy decides presentation;
- `inject`: full text at exact safe point; command rights required;
- `send`: full text as a new turn; command rights required;
- `steer`: full text into exact active turn; exact steer/control required.

`presentation_kind=notice|full|parent`; presented is not acknowledged.

### Delivery policy

Profile field is:

```text
mail_delivery = off | notice | full | parent
```

- `off`: queue only; recipient reads explicitly.
- `notice`: daemon presents `[eliot] N unread messages; run eliot inbox`. This requires
  `routing_enabled=true`, because the recipient needs its own credential to read inbox.
- `full`: daemon may present the full bounded message through a qualified native
  inject/send mechanism. It does not require recipient CLI routing, but it does require
  exact generation binding and declared native delivery semantics.
- `parent`: queue one coalesced notice to the nearest live routed parent/project manager;
  original message remains addressed to the child.

For `notice`/`full`:

1. busy + qualified exact inject → one generation-bound presentation at safe point;
2. busy without inject → leave unread; same generation idle may receive one short send
   or full send according to policy;
3. idle → one bounded send presentation if supported;
4. observe-only, draining, pending request, matching provider hold or unknown control/
   input/turn → no presentation.

A routing-disabled Agent with `notice` is treated as queue-only and reports
`delivery_unavailable`; it never receives an unusable CLI instruction. Wake can never be
a provider probe. No send/steer substitution while busy. At most one open presentation
command and one presentation/minute/recipient. Unknown presentation is not retried. A
new generation stales presentation but unread mail remains eligible. Startup reconciles
unread recipients with no open presentation.

### Inbox acknowledgement

One immutable open batch per recipient, at most 100 messages and 1 MiB encoded data.
Excess waits for the next batch. `mail.inbox` returns the same batch until acknowledged
and holds no read transaction while waiting. CLI writes complete stdout then sends a
receipted `mail.ack`. Lost inbox leaves unread; lost ack reuses WriteId. `mail.read`
rejects when the message belongs to an open batch. History is paginated.

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
retention; an old cursor returns `history_gap`. Receipts follow their independent
minimum window and FK rules.

## Shared variables

Scopes are project (Agent/project-rule default), tree root or global. Only owner/global
rule writes global. Values are at most 16 KiB JSON/string, reject known secrets,
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
| edited | live controlled current-generation edit projection whose path remains dirty |
| dirty | Git dirty path in a single-Agent worktree |
| branch | committed path still different from frozen base OID |
| claim | current live controlled advisory intention |
| history | optional bounded authorship provenance only |

Current edit state comes from
[durable attribution](monitoring.md#durable-edit-attribution), not retained events.
Rows for terminal/detached generations may remain until the path is clean for cleanup
provenance, but `who` never reports them as current workers. Shared dirty paths with no
live controlled attribution are `unattributed`. Queries use cached projections and grant
no authority.

Git identity is environment-only. Shared-checkout Agents stage explicit paths; one
manager commits, or writers use worktrees.

## Claims

Claims are bounded repository-relative normalized globs with note/TTL and exact owner
`(agent,generation)`. They are active only while generation is live and controlled.
Overlap with another active claim or dirty/branch path emits conflict plus one coalesced
queue message. Claims never prevent writes.

Terminal, resume, detach/observe-only or explicit release removes old-generation claims
in the same transaction. TTL is fallback. Project principal cannot claim outside its
project.

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

Parent/root/depth are logical. Config limits controller-created fanout/depth. Native
children upsert by canonical subject with parent-generation provenance even when guards
are exceeded. Parent completion/Goal notification uses ordinary queue mail and the
recipient delivery policy.

Tree selectors resolve at admission; later work is not included. Synchronous manager
uses exact send/wait/result; queue remains asynchronous.

## Prompt guidance

Default note lists available commands and says vars/`who` first, specific mail second,
no acknowledgements/status chatter/credentials. A routing-disabled native child is not
told to use Eliot CLI. Manager profiles add exact launch/send/wait/result/steer/
interrupt/stop guidance.
