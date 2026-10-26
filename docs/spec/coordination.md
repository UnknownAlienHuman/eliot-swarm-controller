# Coordination

Agents coordinate directly through the same local API. Mail is durable communication;
variables and `who` carry shared facts; claims are advisory. None of these creates a
second work scheduler.

## Authority

Identity is `(agent id, generation)` from the current token
([API](api.md#transport)).

| Caller | May command agents | May queue mail / read project coordination |
| --- | --- | --- |
| owner (no token) | everyone | everything |
| `control=self` | itself | its project |
| `control=tree` | itself + descendants | its project |
| `control=project` | every agent in its project | its project |
| rule/timer/script | everyone (owner authority) | everything |

A child never gets more control than its parent. `send`, `steer` and `inject` alter a
model turn/context and therefore require command rights on the recipient. `queue` does
not: the recipient opted into how queued mail is presented through its profile.

A stale-generation token cannot queue mail, write vars or command a replacement
session. Owner authority is used only when no token was supplied; an invalid supplied
token never falls back to owner.

## Mailbox

```text
Message {
  id, sender, sender_generation?, recipient,
  thread?, reply_to?, mode, text (≤ 32 KiB),
  admitted_at, presented_at?, presented_generation?, read_at?,
  presentation_command?
}
sender = owner | agent:<id>#<generation> | rule:<id> | timer:<id>
```

`admitted` means stored. `presented` means the plugin confirmed that the full text or
a mailbox notice entered a particular model generation. `read` means an inbox/read
call returned it. Stored is not presented; presented is not understood or answered.

### Recipients and modes

`to` accepts id/name or selector (`@parent`, `@children`, `@siblings`, `@project`,
`@label:role=reviewer`). Expansion happens transactionally at send time: one stable
message per recipient, one shared thread id.

- `queue` (default): store the full text. It does not grant the sender control.
  If recipient profile `wake_on_mail=true`, the daemon may present one short notice:
  `[eliot] N unread messages; run: eliot inbox`. For a busy agent this is an inject at
  the next safe point; for an idle agent it is a short turn. The action is a
  recipient-configured daemon action, not sender command authority.
- `inject`: present the full text at the next safe point; requires command rights.
- `send`: present as a new turn; requires command rights.
- `steer`: present inside the exact active turn; requires command rights and native
  steer support.

Presentation text for a full message is:

```text
[eliot message m12 from a7#3 writer-1 | reply: eliot msg send a7 --reply-to m12 "..."]
<text>
```

### Wake guardrails

- Coalesce unread mail into at most one notice per recipient per minute and at most one
  open presentation command. A message burst never creates a turn per message.
- A presentation command binds the recipient's current generation. If that generation
  changes first, the command fails stale; messages remain unread and may be presented
  to the new generation later.
- No wake/inject occurs while daemon admission is draining, while the recipient has an
  unknown control command, or while a pending approval/question requires an answer.
- A lost/unknown notice is never retried blindly. Mail remains available through
  `inbox`; later presentation requires readback or a new coalesced action after the
  old one is resolved.
- `wake_on_mail=false` means queue-only. Parent completion messages and peer mail obey
  the same setting.
- Broadcast to more than 50 recipients requires owner/rule authority or explicit
  `--confirm-broadcast`.

Unknown recipient is an error, never a new identity. `eliot inbox --wait 60` blocks
without spending another model turn when an already-running agent is waiting on a
peer.

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
  read_at INTEGER
);
CREATE INDEX messages_inbox ON messages(recipient, read_at, id);
```

## Shared variables

Small agreed facts: ports, API names, schema versions and decisions.

```text
eliot var set project/api.base_path /v2 --expect-version 3 --note "agreed with a7"
eliot var get project/api.base_path
eliot var list project/
eliot var watch project/api.*
```

Scopes:

- `project/` (default for an agent), stored as `project:<name>`;
- `tree/`, stored as `tree:<root id>`;
- `global/`.

The owner must name a project explicitly when it has no caller project. Values are
JSON/string ≤16 KiB. Each successful write increments `version`.
`--expect-version` is compare-and-set; mismatch is `bad_state`. Every change emits
`var.changed`. `set_by` includes caller generation so a stale process cannot be
mistaken for the current incarnation.

```sql
CREATE TABLE vars (
  scope TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  version INTEGER NOT NULL,
  set_by TEXT NOT NULL,
  note TEXT,
  set_at INTEGER NOT NULL,
  PRIMARY KEY(scope, key)
);
```

## Who works on this

`eliot who <pathspec>...` combines facts already maintained by the
[Git monitor](monitoring.md#git-monitor):

| Signal | Source | Meaning |
| --- | --- | --- |
| `edited` | generation-matched agent `file.changed` events whose paths remain dirty | this agent changed the path in its current generation |
| `dirty` | Git status for a worktree used by exactly one live agent | uncommitted now |
| `branch` | branch changes still different from base | committed but unmerged |
| `claim` | claim table | announced intention |
| `history` | optional bounded Git log | recent authorship, not current ownership |

Example:

```text
a7#3 writer-1 codex  dirty src/api/routes.rs src/api/mod.rs branch feat/routes
a9#1 tests-2  claude claim src/api/** "integration tests" expires 14:30
```

Each agent process gets Git identity in environment only:

```text
GIT_AUTHOR_NAME / GIT_COMMITTER_NAME = agent name
GIT_AUTHOR_EMAIL / GIT_COMMITTER_EMAIL = <name>.<id>@agents.eliot
```

`git_identity="inherit"` opts out. Eliot never writes repository Git config.

In a shared checkout, Git cannot attribute uncommitted bytes. Eliot reports
agent-specific native edit events plus `unattributed` remaining dirty paths. Agents in
a shared checkout stage explicit paths, never `git add -A` or `.`; one manager should
commit, or writers should use worktrees. Claims and `who` inform but do not lock.

`who` queries cached sets and at most one bounded Git log. It never runs a fresh status
per request. Paths use the common normalization rules.

## Claims

Claims announce intent; they are not locks:

```text
eliot claim 'src/api/**' --note "routes refactor" --ttl 2h
eliot release 'src/api/**'
```

Overlapping claims or claims over another agent's dirty/branch set emit
`claim.conflict` and queue one coalesced message to both. Default TTL is 4 h. Claims
are released on terminal agent state or expiry; a generation change updates/requires a
new claim rather than letting an old process own it indefinitely.

```sql
CREATE TABLE claims (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  glob TEXT NOT NULL,
  note TEXT,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(agent, generation, glob)
);
```

## Hierarchy

`launch --parent A` inherits root/project and sets depth +1. The model itself has no
arbitrary hierarchy limit; configuration prevents accidental fan-out:
`max_depth=8`, `max_children_per_agent=20`, `max_tree_agents=100`; 0 disables a limit.
A rejected launch names the limit.

Native subagents register as `origin=native` children. Tree selectors work for list,
watch, send and stop. When a child reaches terminal state or its goal becomes
done/blocked, Eliot queues one parent message if `notify_parent=true`; mailbox wake
policy decides whether that notice starts/presents a turn.

A manager that must synchronously react should stay in a turn and use
`eliot send <child> "<task>" --wait` or `eliot wait @children`. Queue delivery is for
asynchronous communication and may wake only according to the recipient profile.

## Prompt guidance

Default system note, trimmed to commands present in the running layer:

```text
You are Eliot agent {id} generation {generation} ({name}) on {harness}, project
{project}, parent {parent}. The `eliot` command talks to the controller:
  eliot inbox
  eliot msg send <agent> "<text>"
  eliot var get|set <key> [value]
  eliot who <path>
  eliot claim <glob>
  eliot ls
Use shared vars and `who` first, specific messages second. Ask one concrete recipient
with the facts needed to answer. Do not send acknowledgements or routine status; the
controller already exposes status.
```

Agents with tree/project control additionally get:

```text
  eliot launch --profile <p> --parent {id} "<task>"
  eliot send <agent> "<text>" --wait
  eliot wait @children [--for turn|idle|stopped]
  eliot result <agent>
  eliot steer|interrupt|stop <agent>
```
