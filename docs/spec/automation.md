# Automation

Automation submits ordinary [commands](core.md#commands) through the same router as
the owner. It adds no workflow state of its own beyond rule/timer bookkeeping.

## Rules

Rules live in TOML files: `<project>/.eliot/rules/*.toml` and `<data_dir>/rules/*.toml`.
The daemon reloads them on change; a file that fails to parse is reported
(`rule.failed`) and the previous version stays active.

```toml
[[rule]]
id = "push-after-writer-turn"
on = { event = "turn.completed", match = { "agent.labels.role" = "writer", "data.status" = "completed" } }
do = [{ action = "git.push", workspace = "${agent.workspace}" }]
limit = { max = 1, per = "agent", window = "10m" }

[[rule]]
id = "fetch-main"
on = { cron = "*/30 * * * *" }
do = [{ action = "git.fetch", repo = "C:/src/app" }]

[[rule]]
id = "relaunch-on-crash"
on = { event = "agent.lost", match = { "agent.labels.restart" = "yes" } }
do = [{ action = "resume", agent = "${agent.id}" }]
limit = { max = 3, per = "agent", window = "1h" }
```

### Triggers (`on`)

| Trigger | Form | Notes |
| --- | --- | --- |
| Event | `event = "<kind glob>"`, optional `match` | Fires once per matching persisted event. |
| Interval | `every = "30m"` | Next fire = last fire + interval; first fire one interval after the rule loads. |
| Cron | `cron = "<5 fields>"`, optional `tz` (default local) | Standard 5-field cron. |
| Once | `at = "2026-11-01T09:00:00+03:00"` | Disabled after firing. |

A rule with a time trigger is a **timer**: `eliot timer list` shows those rules with
their next fire time, `eliot timer run ID` fires one now.

Missed timer ticks while the daemon was down: `catch_up = "latest"` (default, fire
once at startup) or `"none"`. Never a burst. Last fire time is stored per timer.

### Matching

`match` is a map of dotted paths into `{event, agent, data}` to expected values:

- exact value: `"data.status" = "completed"`
- glob: `"data.command" = "glob:cargo test*"`
- regex: `"data.path" = "re:^src/(api|db)/"`
- one of: `"agent.harness" = ["codex", "claude"]`
- presence: `"data.error" = "exists"`

All entries must match. Anything more complex is a `run` action that decides in code.

### Actions (`do`)

Executed in order; a failing action stops the rest of that firing (`rule.failed`).
`${path}` placeholders are replaced from the triggering event; there is no expression
language.

| Action | Fields | Effect |
| --- | --- | --- |
| `launch` | `profile`, `prompt`, `name?`, `parent?`, `labels?`, `worktree?` | new agent |
| `send` / `steer` / `inject` | `agent`, `text` | command to an agent or selector |
| `interrupt` / `stop` / `resume` | `agent` | command |
| `approve` / `deny` | `agent`, `request_id` (default: from event) | answer an approval |
| `goal.set` / `goal.clear` | `agent`, `text`, `check?`, `budget?` | goal |
| `git.fetch` / `git.pull` / `git.push` | `workspace` or `repo`, `remote?`, `ref?` | `pull` is `--ff-only`; `push` never forces |
| `git.worktree` | `repo`, `branch`, `base` | create a worktree |
| `msg.send` | `to`, `text`, `mode?` | mailbox |
| `var.set` | `scope`, `key`, `value` | shared variable |
| `run` | `cmd: [..]`, `cwd?`, `timeout?` (default 60 s), `env?` | script (below) |
| `notify` | `text`, `via?: log|desktop|webhook`, `url?` | tell the owner |
| `emit` | `kind` (`x.user.*`), `data` | custom event for other rules |

Git actions run `git` with explicit argv (no shell), `GIT_TERMINAL_PROMPT=0`, no
editor, inside the daemon's process custody, and emit `git.ref` / `rule.failed`. A
push whose reply is lost is checked with `git ls-remote` — never pushed again blindly.

### Scripts

`run` starts an executable in any language:

- stdin: one JSON object `{event, agent, rule}`;
- env: `ELIOT_HOME`, `ELIOT_RULE`; no agent token, so its `eliot` calls act as the
  owner;
- stdout: zero or more NDJSON actions in the same shape as `do` entries — the daemon
  executes them with the same limits; stderr goes to the daemon log;
- exit code ≠ 0 → `rule.failed` with the stderr tail.

Scripts may also call the `eliot` CLI directly. Scripts are versioned with the project
(`.eliot/scripts/`); the daemon never edits a script file that may be running.

## Approvals

`approval.requested` events go through rules like any event. A rule with `approve`
or `deny` answers it. For plugins that need an inline answer (`decide` request), the
host evaluates only rules whose actions are `approve`/`deny`, with a 2 s budget; no
match → `ask` (the request stays pending for the owner or a manager agent).

```toml
[[rule]]
id = "allow-tests"
on = { event = "approval.requested", match = { "data.kind" = "command", "data.command" = "glob:cargo test*" } }
do = [{ action = "approve" }]
```

## Goal loop

`goal.set` with `mode = native` when the plugin declares `native_goal`, else `eliot`.

```text
Goal { text, check?: string ("marker:<TEXT>" or a shell command line, see core verbs),
       budget: {max_turns (default 20), max_cost_usd?, no_progress (default 3)},
       status, turns_used, last_check? }
```

**Native mode.** Eliot sets the native goal and mirrors `goal.updated`. It enforces
`max_turns`/`max_cost_usd` by pausing the native goal when exceeded. When the native
goal reports done and a `check` exists, Eliot runs it; on failure it reports
`goal.updated status=blocked reason=check_failed` — it does not silently restart.

**Eliot mode.** On every `agent.idle` of an agent with an active goal:

1. Run `check` (if any) in the agent's workspace with a timeout. Pass → `done`.
   With `marker:X`, done when the last `message.agent` contains `X`.
2. Otherwise, if a budget is exhausted → `blocked` (`budget`).
3. If the workspace tree hash (`git write-tree` on a temp index) and the check output
   are both unchanged for `no_progress` consecutive turns → `blocked` (`no_progress`).
4. Otherwise `send` a continuation built from a template:
   `Goal: <text>\nThe check `<cmd>` still fails:\n<last 40 lines>\nContinue.`

`blocked` and `done` emit `goal.updated` and, by default, a message to the parent
agent (or the owner for top-level agents). Continuations are normal `send` commands
with `origin = goal:<agent>`, so they show up and count like any other command.

## Storage (L5 migration)

Rules themselves live in files. The Store keeps only what must survive a restart:

```sql
CREATE TABLE timers (
  rule TEXT PRIMARY KEY, last_fired_at INTEGER, next_due_at INTEGER
);
CREATE TABLE goals (
  agent INTEGER PRIMARY KEY, text TEXT NOT NULL, mode TEXT NOT NULL, check_cmd TEXT,
  status TEXT NOT NULL, max_turns INTEGER NOT NULL, turns_used INTEGER NOT NULL DEFAULT 0,
  max_cost_usd REAL, cost_used REAL NOT NULL DEFAULT 0, no_progress INTEGER NOT NULL,
  stale_turns INTEGER NOT NULL DEFAULT 0, last_tree_hash TEXT, last_check_hash TEXT,
  updated_at INTEGER NOT NULL
);
```

Rate-limit windows and loop-depth counters are in memory; a restart resets them.

## Guardrails

- Every command and event created by automation carries `cause` with a depth counter;
  depth > 5 → the firing is suppressed (`rule.suppressed reason=loop`).
- `limit` per rule (default `max = 10 per rule window 1m`); excess firings are
  suppressed and reported once per window.
- Global `automation.max_actions_per_minute` (default 120).
- `eliot rules pause` stops all automation instantly; `eliot rule test ID --event F`
  prints what a rule would do without doing it.
- Status/failure events of an agent never trigger a full agent turn of the same agent
  by default (`send`/`steer`/`inject` to the event's own agent from an event of that
  agent require `allow_self = true` on the rule). This prevents comment ping-pong
  loops seen in other systems ([lessons](../reference/lessons-v1.md)).

## Workflow recipes

Workflows are rules + scripts + agents, not core features. Examples kept in
`examples/` once L5 lands:

- **Writer → reviewer**: on `turn.completed` of `role=writer` with dirty files →
  `launch` a reviewer with the diff → on reviewer `turn.completed` → `run` a script
  that parses findings and `send`s them back to the writer.
- **Keep main fresh**: `cron */15` → `git.fetch`; on `git.ref` for `origin/main` →
  `inject` "main moved to <sha>" into agents whose base is main.
- **Nightly**: `cron 0 2 * * *` → `launch` profile `maintenance` with a goal and a
  check.
