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
| rule / timer / script | everyone (owner authority) | everything |

`control` comes from the profile (`control = "tree"`) or `launch --control`. A child
never gets more control than its parent. This is the whole permission model.

## Mailbox

```text
Message { id, from, to, thread?, reply_to?, mode, text (≤ 32 KiB), created_at, delivered_at?, read_at? }
from = origin string: agent:<id> | owner | rule:<id> | timer:<id>   (same form as command origins)
```

- `to` is an agent id/name or a selector (`@parent`, `@children`, `@siblings`,
  `@project`, `@label:role=reviewer`). A selector expands at send time into one
  message per recipient sharing a `thread`.
- `mode` decides how the text reaches a running agent:
  - `queue` (default): stored. The recipient sees it with `eliot inbox`; additionally,
    if its plugin supports `inject`, a one-line notice
    (`[eliot] 1 new message from a7 (writer): eliot inbox`) is injected.
  - `inject`: the full text is injected as context (no new turn).
  - `send`: delivered as a new turn (wakes an idle agent).
  - `steer`: delivered into the running turn (urgent).
- Delivered text format, so models recognise it:
  `[eliot message m12 from a7 writer-1 | reply: eliot msg send a7 --reply-to m12 "..."]\n<text>`
- Unknown recipient → error, never an implicit new identity.
- `eliot inbox --wait 60` blocks until a message arrives (for agents waiting on a
  peer instead of polling a model).

Schema (L6 migration):

```sql
CREATE TABLE messages (
  id INTEGER PRIMARY KEY, sender TEXT NOT NULL,   -- origin string, see `from`
  recipient INTEGER NOT NULL,
  thread INTEGER, reply_to INTEGER, mode TEXT NOT NULL, text TEXT NOT NULL,
  created_at INTEGER NOT NULL, delivered_at INTEGER, read_at INTEGER
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

- Scopes: `project/` (default: caller's project), `tree/` (caller's root tree),
  `global/`. Stored as `scope = "project:<name>"`, `"tree:<root id>"` or `"global"`;
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
facts collected by the [Git monitor](monitoring.md#git-monitor):

| Signal | Source | Meaning |
| --- | --- | --- |
| `dirty` | `git status` of each live agent's worktree | uncommitted changes to matching paths right now |
| `branch` | `git diff --name-only <base_ref>...HEAD` per agent worktree (`base_ref` unset → `origin/HEAD` if it exists, else skipped) | committed on the agent's branch, not yet in its base |
| `claim` | claims table | the agent announced it will work there |
| `history` (with `--history N`) | `git log -N -- <pathspec>` in the repo | recent commits, mapped to agents by branch or author email |

Output (one line per agent, strongest signal first):

```text
a7  writer-1  codex   dirty   src/api/routes.rs, src/api/mod.rs   branch feat/routes
a9  tests-2   claude  claim   src/api/**  "adding integration tests" (expires 14:30)
```

Mapping commits to agents: each agent worktree has its branch recorded in the
`agents` row, and Eliot sets the worktree-local Git identity
`user.email = <agent-name>.<id>@agents.eliot` unless the profile sets
`git_identity = "inherit"`. `who --history` maps author emails of that form back to
agents; other authors are shown as is.

`who` is a query over in-memory sets plus at most one `git log`; it never runs
`git status` itself.

## Claims

Advisory announcements, not locks (Eliot cannot sandbox a native shell, and a fake
lock is worse than none):

```text
eliot claim 'src/api/**' --note "routes refactor" --ttl 2h
eliot release ['src/api/**']
```

- Overlapping claims, or a claim over another agent's `dirty`/`branch` files, emit
  `claim.conflict` and send a `queue` message to both agents with the other's note.
- Claims expire (`ttl`, default 4 h) and are dropped when the agent stops.

```sql
CREATE TABLE claims (
  agent INTEGER NOT NULL, glob TEXT NOT NULL, note TEXT, expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL, PRIMARY KEY (agent, glob)
);
```

## Hierarchy

- `launch --parent A` creates a child; `depth = parent.depth + 1`; `root` is
  inherited. No depth limit in the model; `limits.max_depth` in config (default 8)
  guards against runaway recursive spawning.
- Native subagents become `origin = native` children automatically.
- Tree operations: `stop --tree A`, `send @tree:A`, `ls --tree A`, `watch @tree:A`.
- Parent notifications: when a child reaches `stopped`, `failed`, `lost` or its goal
  becomes `done`/`blocked`, a `queue` message goes to the parent (configurable).

## Communication guidance for agent prompts

Research and v1 field data agree: dense chatter costs tokens and makes agents
converge early ([research](../reference/research.md)). Every launched agent therefore
gets this **system note** (passed as `system_note` in `agent.start`; the plugin uses
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
