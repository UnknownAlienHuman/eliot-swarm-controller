# Coordination

How agents work together without a manager relaying every word. Everything here is
usable from the CLI inside any agent (`eliot ...`) and later from `eliot mcp`.

## Who may do what

Identity comes from `ELIOT_AGENT_TOKEN` (see [API](api.md#transport)). Rules:

| Caller | May command (`send`, `steer`, `stop`, ...) | May message / read vars / `who` |
| --- | --- | --- |
| owner (no token) | everyone | everything |
| agent with `control = self` (default) | itself | its project |
| agent with `control = tree` | itself and its descendants | its project |
| agent with `control = project` (manager role) | every agent in its project | its project |
| global rule / timer / script | everyone (owner authority) | everything |
| project rule / timer (its `do` list, its scripts' stdout actions and their `eliot` calls) | agents of that project | that project |

`control` comes from the profile (`control = "tree"`) or `launch --control`. A child
never gets more control than its parent. An agent without its own identity
(`own_identity = false`, see [core](core.md#agent)) acts as its parent. This is the
whole permission model. Message
modes that start or change a turn (`send`, `steer`) count as commands: a sender needs
command rights on the recipient, otherwise it gets `not_allowed` and can use `queue`
or `inject`.

## Mailbox

```text
Message { id, sender, recipient, thread?, reply_to?, mode, text (≤ 32 KiB),
          admitted_at, presented_at?, read_at? }
// admitted = stored; presented = the plugin reports the text entered the model
// (the daemon's notice or text command — `inject` or `send`, origin `daemon` — is applied);
// read = acknowledged by `eliot inbox` (`mail.ack`) / `mail.read`
sender = origin string: agent:<id> | owner | rule:<id> | timer:<id>   (same form as command origins)
```

- The `to` argument of `mail.send` / `eliot msg send` is an agent id/name or a selector (`@parent`, `@children`, `@siblings`,
  `@project`, `@label:role=reviewer`). A selector expands at send time into one
  message per recipient sharing a `thread` (at most `limits.max_selector_targets`;
  more fails before anything is stored).
- `mode` decides how the text reaches a running agent:
  - `queue` (default): stored; the recipient reads it with `eliot inbox`. It is also
    told: a busy recipient gets a one-line notice injected at its next safe point
    (`[eliot] 1 new message from a7 (writer): eliot inbox`); an idle recipient is woken
    with that notice as a short turn. Wake-ups are coalesced (at most one per
    recipient per minute), need no command rights, and can be turned off per profile
    (`wake_on_mail = false`). This is what lets peers talk without a manager. An
    agent without its own identity cannot run `eliot inbox` as itself (it would read
    its parent's inbox), so it gets the full message text in the notice instead, and
    the message counts as read once presented. A wake-up is never retried: if it
    ends `unknown`, the message stays unread.
  - `inject`: the full text is injected as context (no new turn).
  - `send`: delivered as a new turn (wakes an idle agent).
  - `steer`: delivered into the running turn (urgent).
- Delivered text format, so models recognise it:
  `[eliot message m12 from a7 writer-1 | reply: eliot msg send a7 --reply-to m12 "..."]\n<text>`
- Unknown recipient → error, never an implicit new identity.
- `eliot inbox --wait 60` blocks until a message arrives (for agents waiting on a
  peer instead of polling a model). It prints the unread messages, then acknowledges
  them (`mail.ack`); if the CLI dies in between, they are shown again next time —
  messages are never lost, at worst repeated.
- Unread messages never expire.

Schema (L5 migration):

```sql
CREATE TABLE messages (
  id INTEGER PRIMARY KEY, sender TEXT NOT NULL,   -- origin string, see `from`
  recipient INTEGER NOT NULL,
  thread INTEGER, reply_to INTEGER, mode TEXT NOT NULL, text TEXT NOT NULL,
  admitted_at INTEGER NOT NULL, presented_at INTEGER, read_at INTEGER
);
CREATE INDEX messages_inbox ON messages(recipient, read_at, id);
```

## Shared variables

Small shared facts agents agree on (ports, API names, schema versions, decisions).

```text
eliot var set project/api.base_path /v2 [--expect-version 3] [--note "agreed with a7"]
eliot var get project/api.base_path          → value, version, set_by, set_at
eliot var list project/ [--json]
eliot var watch project/api.*                → streams var.changed
```

- Scopes: `project/` (caller's project; a key without a scope prefix means this),
  `tree/` (caller's root tree), `global/`. Stored as `scope = "project:<name>"`, `"tree:<root id>"` or `"global"`;
  the owner must name the project (`project:<name>/key`) when it has no project.
- Values are JSON or strings, ≤ 16 KiB. Each write bumps `version`;
  `--expect-version` makes it a compare-and-set (`bad_state` on mismatch), so two
  agents cannot silently overwrite each other.
- Every change is a `var.changed` event, so rules can react (e.g. inject the new value
  into every agent of the project).

```sql
CREATE TABLE vars (
  scope TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, version INTEGER NOT NULL,
  set_by TEXT NOT NULL, note TEXT, set_at INTEGER NOT NULL, PRIMARY KEY (scope, key)
);
```

## Who works on this

`eliot who <pathspec>...` answers "which agents are touching these files", from Git
facts collected by the [Git monitor](monitoring.md#git-monitor) and from the agents'
own file-change events:

| Signal | Source | Meaning |
| --- | --- | --- |
| `edited` | `agent_edits`, filled from the agent's `file.changed` events (native edit/write items and tools) for paths that are still dirty | this agent changed these files and they are not committed yet — the only per-agent signal when several agents share one checkout |
| `dirty` | `git status` of a worktree used by exactly one live agent | uncommitted changes to matching paths right now |
| `branch` | the committed-but-unmerged set of the agent's worktree ([Git monitor](monitoring.md#git-monitor); base = `base_oid`, else `base_ref`, else `origin/HEAD` if it exists, else skipped) | committed on the agent's branch and still different from its base |
| `claim` | claims table | the agent announced it will work there |
| `history` (with `--history N`) | `git log -N -- <pathspec>` in the repo | authored recently — provenance, never reported as the current owner |

Output (one line per agent, strongest signal first):

```text
a7  writer-1  codex   dirty   src/api/routes.rs, src/api/mod.rs   branch feat/routes
a9  tests-2   claude  claim   src/api/**  "adding integration tests" (expires 14:30)
```

Mapping commits to agents: each agent worktree has its branch recorded in the
`agents` row, and Eliot gives each agent process its own Git identity through the
environment (`GIT_AUTHOR_NAME`/`GIT_COMMITTER_NAME` = agent name, or the id when it
has none; `GIT_AUTHOR_EMAIL`/`GIT_COMMITTER_EMAIL` = `<name or id>.<id>@agents.eliot`) unless
the profile sets `git_identity = "inherit"`. No repository config is written, so this
also works in shared checkouts. `who --history` maps author emails of that form back to
agents; other authors are shown as is.

In a checkout shared by several agents, `git status` cannot say who changed a file;
there `who` reports `edited` per agent and lists remaining dirty files as
`unattributed`. In a shared checkout an agent stages only its own paths
(`git add <paths>`, never `-A` or `.`); Eliot's own git actions never stage with `-A`
there. `who` and claims inform; they do not prevent conflicts. Several agents
running `git add`/`commit` in one checkout collide on `index.lock` and mix each
other's changes: in `shared` mode let one agent (usually the manager) commit, or give
writers `worktree` mode.

Pathspecs and recorded paths are normalized the same way before matching
([path rules](monitoring.md#starting-processes)).

`who` is a query over the Git monitor's sets, the `agent_edits` table and claims, plus
at most one `git log`; it never runs `git status` itself. Only live agents are
reported as working on a path; rows of stopped agents stay until the path is clean
but are shown only under `--history`.

## Claims

Advisory announcements, not locks (Eliot cannot sandbox a native shell, and a fake
lock is worse than none):

```text
eliot claim 'src/api/**' --note "routes refactor" --ttl 2h
eliot release ['src/api/**']
```

- Overlapping claims, or a claim over another agent's `dirty`/`branch` files, emit
  `claim.conflict` and send a `queue` message to both agents with the other's note.
- Claims expire (`ttl`, default 4 h) and are dropped when the agent stops, fails,
  is lost or is detached.

```sql
CREATE TABLE claims (
  agent INTEGER NOT NULL, glob TEXT NOT NULL, note TEXT, expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL, PRIMARY KEY (agent, glob)
);
```

## Hierarchy

- `launch --parent A` creates a child; `depth = parent.depth + 1`; `root` is
  inherited. No limit in the model; config guards against runaway spawning:
  `limits.max_depth` (8), `limits.max_children_per_agent` (20) and
  `limits.max_tree_agents` (100); `0` means no limit. These guard against accidents,
  not against scale. A launch past a limit fails `not_allowed` and names it (v1 field
  data: one recursive request produced 48+ agents and ~1.5M tokens).
- Native subagents become `origin = native` children automatically. The limits cannot
  refuse a child the harness already created: it is recorded with
  `child.spawned over_limit=true`, gets `control = self` and no mail wake-ups, and
  stays visible and stoppable. A child's `control` limits calls made with the child's
  own token; a child without its own identity cannot be told apart from its parent
  (its tools run in the parent's process), so it acts with the parent's rights — give
  a manager's profile `control = project` only if its native subagents may use it too.
- Tree operations: `stop --tree A`, `send @tree:A`, `ls --tree A`, `watch @tree:A`.
- Parent notifications: when a child reaches `stopped`, `failed`, `lost` or its goal
  becomes `done`/`blocked`, a `queue` message goes to the parent (profile
  `notify_parent`, default on).
- A manager that must react to its children waits inside its own turn with
  `eliot wait @children` or `eliot send <child> "<task>" --wait` — a queued message
  wakes an idle manager only with a coalesced notice (at most one a minute, and not
  at all with `wake_on_mail = false`), and `inject` reaches a busy agent only at its
  next safe point.

## Communication guidance for agent prompts

Research and v1 field data agree: dense chatter costs tokens and makes agents
converge early ([research](../reference/research.md)). Every launched agent therefore
gets this **system note** (it lists only commands the running build has — the
coordination lines arrive with L5) (passed as `system_note` in `agent.start`; the plugin uses
the harness's system/developer-instructions mechanism, or prepends it to the first
prompt). Profiles may replace it with `system_note = "<file>"`.

```text
You are Eliot agent {id} ({name}) on {harness}, project {project}, parent {parent}.
The `eliot` command talks to the controller:
  eliot inbox                      your messages
  eliot msg send <agent> "<text>"  message one agent (reply: --reply-to <msg>)
  eliot var get|set <key> [value]  shared project facts (ports, API names, decisions)
  eliot who <path>                 who else is changing these files
  eliot claim <glob>               announce the area you will change
  eliot ls                         other agents
Coordinate through shared vars and `who` first, messages second. Message one specific
agent, not everyone. Ask concrete questions with the facts needed to answer. Do not
send acknowledgements or status updates; the controller already shows status.
```

Agents with `control = tree` or `project` also get:

```text
You may direct other agents:
  eliot launch --profile <p> --parent {id} "<task>"   start a child
  eliot send <agent> "<text>" --wait                  give work and wait for the answer
  eliot wait @children [--for turn|idle|stopped]      wait for children (re-run after timeout)
  eliot result <agent>                                last answer of an agent
  eliot steer|interrupt|stop <agent>                  correct or stop work
```
