# Automation

Automation submits ordinary [commands](core.md#commands) through the same router as
the owner. It adds no workflow state of its own beyond rule/timer bookkeeping.

## Rules

Rules live in TOML files: `<project>/.eliot/rules/*.toml` and `<data_dir>/rules/*.toml`.
Rule keys: `id`, `on`, `do`, and optional `limit`, `catch_up`, `allow_self`,
`enabled` (default true).
The daemon reloads them on change; a file that fails to parse is reported as
`source.health` for `config` naming the file and line, and the previous version stays
active.

```toml
[[rule]]
id = "push-writer-commits"
on = { event = "git.commit", match = { "agent.labels.role" = "writer" } }
do = [{ action = "git.push", workspace = "${agent.workspace}" }]

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
| Interval | `every = "30m"`, optional `anchor` (default: when the rule was first loaded; stored in `timers`) | Slot k fires at `anchor + k·period`; the slot's instant is the firing key, so a restart never re-fires a slot and missed slots coalesce to the latest. |
| Cron | `cron = "<5 fields>"`, optional `tz` (default local) | Standard 5-field cron. Time is kept as UTC instants; local time is used only to match fields. In the repeated hour of a DST fall-back each real instant fires once (v1 hit a scheduler failure there; test both sides of the fold); a local time inside a spring-forward gap fires at the first valid instant after it. |
| Once | `at = "2026-11-01T09:00:00+03:00"` | Disabled after firing. |

A timer firing is skipped while the same rule's previous firing still has commands
`queued` or `dispatched` (`rule.suppressed reason=in_flight`) or `unknown`
(`reason=unknown`); skipped firings
are not queued behind it, so a slow `git fetch` never stacks copies. Event firings each
run (each has its own key); a repeated `git.push` of the same branch is a no-op.

A rule with a time trigger is a **timer**: `eliot timer list` shows those rules with
their next fire time, `eliot timer run ID` fires one now.

Missed timer ticks while the daemon was down: `catch_up = "latest"` (default, fire
once at startup) or `"none"`. Never a burst: Tokio intervals default to
`MissedTickBehavior::Burst`, so use `Skip`. Waits use a monotonic clock; after sleep or
a wall-clock jump the daemon recomputes next due times from UTC before firing.

Every firing has a key: the triggering event's `seq`, or the timer slot's scheduled
UTC instant (ms) for every timer kind. The ids of the
commands a firing submits are derived from `rule id + key`, and they are committed in
one transaction with the rule's bookkeeping: the timer row, or the event rule's
**cursor** (the last `seq` it evaluated, table `rule_cursors`). A crash or restart can
therefore never fire the same key twice, and an event or slot that was never
committed is evaluated on the next pass.

- A new rule starts at the current last `seq`: it never fires on history. Editing a
  rule keeps its cursor; events after it are evaluated with the new definition.
- A timer keeps its `anchor` and last fired key in `timers`. Changing the trigger
  (`every`, `anchor`, `cron`, `tz`, `at`) starts it afresh: the new anchor is the
  reload time and nothing before it fires; editing only `do` keeps the schedule.
- A disabled rule (or `eliot rules pause`) advances its cursor without firing and
  reports the skipped range once (`rule.suppressed reason=disabled`).
- A firing that cannot be built — a placeholder that does not resolve, an action the
  rule's scope does not allow, a selector over the limit — commits `rule.failed` and
  advances the cursor; it does not block later events. Only a Store failure leaves the
  cursor where it was, so the event is evaluated again.

### Matching

`match` is a map of dotted paths into `{event, agent, data}` to expected values:

- exact value: `"data.status" = "completed"`
- glob: `"data.command" = "glob:cargo test*"`
- regex: `"data.path" = "re:^src/(api|db)/"`
- one of: `"agent.harness" = ["codex", "claude"]`
- presence: `"data.error" = "exists"`

All entries must match. Anything more complex is a `run` action that decides in code.
`agent.*` paths read the agent row as it is when the event is evaluated (normally
milliseconds after it was stored; after a restart, possibly later).

### Actions (`do`)

Every action of a firing is stored as a command with id `rule:<rule id>@<key>/<n>`
in the firing's transaction; actions that are not agent verbs (`git.*`, `msg.send`,
`var.set`, `run`, `notify`, `emit`) are commands without a target that the daemon
executes itself. They run in order: action n+1 is dispatched only after action n is
`applied`; if n is rejected, failed or `unknown`, the rest are cancelled and the
firing reports `rule.failed`. A project rule's actions may target only agents of its
project. `${path}` placeholders are replaced from the triggering event; there is no expression
language. Event data comes from agents and is untrusted: a placeholder may only fill
an argument value (one argv element, or a field like `text`), never the program
(`cmd[0]`), `cwd`, an env name or an option name.

| Action | Fields | Effect |
| --- | --- | --- |
| any [verb](core.md#verbs) (`launch`, `send`, `steer`, `inject`, `interrupt`, `stop`, `resume`, `approve`, `deny`, `answer`, `configure`, `goal.*`, `cancel`, `detach`, `compact`, `fork`, `background`) | `agent` (id, name or selector; default: the event's agent), then the verb's args; `request_id` defaults to the event's | the same command a person would submit |
| `git.fetch` / `git.pull` / `git.push` | `workspace` or `repo`, `remote?`, `ref?` | `pull` is `--ff-only`; `push` never forces |
| `git.worktree` | `repo`, `branch`, `base` | create a worktree |
| `msg.send` | `to`, `text`, `mode?` | mailbox |
| `var.set` | `scope`, `key`, `value` | shared variable |
| `run` | `cmd: [..]`, `cwd?`, `timeout?` (default 60 s), `env?` | script (below) |
| `notify` | `text`, `via?: log|desktop|webhook`, `url?` | tell the owner |
| `emit` | `kind` (`x.user.*`), `data` | custom event for other rules |

Git actions run `git` with explicit argv (no shell), `GIT_TERMINAL_PROMPT=0`, no
editor, inside the daemon's process custody, and emit `git.ref` / `rule.failed`.
`push` is `git push --porcelain <remote> <ref>` (never forced). A push whose outcome is
lost stays `unknown` until its git process tree is proven gone; only then is it checked
with `git ls-remote` — never pushed again blindly — and until it is settled further
pushes to that repository are refused. Every git action has such a readback, run once
its process tree is proven gone, so an interrupted action cannot block a timer
forever:

| Action | `unknown` settles as |
| --- | --- |
| `git.push` | `applied` if the remote ref (`ls-remote`) is the pushed OID, else `failed` |
| `git.fetch` | `applied` if the remote-tracking refs equal `ls-remote`, else `failed` (the next firing fetches again) |
| `git.pull` | `applied` if HEAD equals its upstream after the fetch readback, else stays `unknown` |
| `git.worktree` | `applied` if `git worktree list` shows the path on the branch, else `failed` |

A `run` action whose outcome is unknown stays `unknown` until the owner resolves it;
its timer reports `rule.suppressed reason=unknown` instead of firing.

### Scripts

`run` starts an executable in any language:

- stdin: one JSON object `{event, agent, rule}`;
- env: the [minimal environment](configuration.md#environment), the action's literal
  `env`, `ELIOT_HOME`, `ELIOT_RULE` and a run token in `ELIOT_AGENT_TOKEN` (kept only in
  the daemon's memory, valid until the script exits): its `eliot` calls act as the rule — owner authority for a global
  rule, that project only for a project rule;
- stdout: zero or more NDJSON actions in the same shape as `do` entries — the daemon
  stores them as commands `<run action id>/<n>` and executes them with the same limits
  and the same project scope as the rule; stderr goes to the daemon log;
- exit code ≠ 0 → `rule.failed` with the stderr tail;
- on `timeout` the daemon kills the script's whole process tree (Job / process group)
  and verifies it is gone; stdout and stderr are capped at 1 MiB each, and more fails
  the firing (`rule.failed reason=output_limit`).

Scripts may also call the `eliot` CLI directly. Scripts are versioned with the project
(`.eliot/scripts/`); the daemon never edits a script file that may be running.

## Approvals

`approval.requested` events go through rules like any event. A rule with `approve`
or `deny` answers it with an ordinary command; the plugin holds the native request
until that command arrives. A hook that must answer synchronously (the
[hook shim](plugin-protocol.md#hooks)) gets the decision of matching `approve`/`deny`
rules within the hook timeout; no match → `ask`; a decision reached after the timeout
is discarded. A request that no approval rule answered when it was raised is sent
once as a `queue` message to the agent's
parent (or the project's `role=manager` agent if there is no parent, else the owner),
naming the request id. Nothing ever answers it by timeout, by picking the first or
"recommended" option, or by denying everything.

```toml
[[rule]]
id = "allow-tests"
on = { event = "approval.requested", match = { "data.kind" = "command", "data.command" = "glob:cargo test*" } }
do = [{ action = "approve" }]
```

## Goal loop

Two independent kinds of goal (owner decision of 2026-10-18, kept from v1): the
harness's **native goal** (default where supported) and the **Eliot goal**, which
drives the agent with ordinary messages. An agent may have one of each at the same
time, with different texts; pausing or clearing one never touches the other.

```text
Goal { mode: native | eliot, text, check?: argv list | "marker:<TEXT>",
       budget: {max_turns (default 20), max_cost_usd?, no_progress (default 3)},
       cadence: on_idle (default) | every <duration>,
       status, turns_used, generation, last_check? }
```

**Native goal.** Eliot sets it through the plugin and mirrors `goal.updated`. It
enforces `max_turns`/`max_cost_usd` by pausing the native goal when exceeded. When the
native goal reports done and a `check` exists, Eliot runs it; on failure it reports
`goal.updated status=blocked reason=check_failed` — it does not silently restart.

**Eliot goal.** On every `agent.idle` (or every `cadence` tick) of an agent with an
active Eliot goal:

1. Skip this round if the agent has a pending approval/question or an `unknown`
   command — those are resolved by their own path, never by a goal message. With
   `cadence = on_idle`, also skip while the same agent has an active native goal: a
   native goal (Codex, Muse) continues by itself when the agent goes idle, and two
   drivers would stack turns. `cadence = every <dur>` still sends (queued until idle)
   beside a native goal, but never stacks: a tick is skipped while the goal's
   previous continuation is `queued`, `dispatched` or `unknown`, or its turn is still
   running.
2. Run `check` (if any) in the agent's workspace, custody and environment (timeout `automation.check_timeout`,
   default 10 min; at most `automation.max_parallel_checks`, default 2, checks run at
   once on the machine). Pass → `done`. With `marker:X`, done when the last `message.agent`
   contains `X`.
3. If a budget is exhausted → `blocked` (`budget`). `max_cost_usd` is accepted only
   for plugins that report cost (see core verbs); `max_turns` always applies.
4. Progress = the workspace fingerprint or the check output changed since the last
   round. Fingerprint: `HEAD` plus, for every dirty or untracked path from
   `git --no-optional-locks status --porcelain=v2 -z` (ignored files and `scratch`
   excluded), its path, size and content hash — read-only; Eliot writes no index and
   no objects into the agent's repository. Above 5,000 files or 64 MiB the fingerprint
   is `unknown`, which never counts as "no progress". No progress for `no_progress`
   consecutive rounds → `blocked` (`no_progress`).
5. Otherwise `send` a continuation built from a template:
   `Goal: <text>\nThe check `<cmd>` still fails:\n<last 40 lines>\nContinue.`
   Repeating the same text on a later round is allowed.

A round has a key: the `seq` of the `agent.idle` event that triggered it, or the
cadence slot. The continuation's id is `goal:<agent>@<key>` and the goal row stores
the last key in the same transaction, so a restart never sends the same round twice,
while the next real `agent.idle` starts the next round.

`blocked` and `done` emit `goal.updated` and, by default, a message to the parent
agent (or the owner for top-level agents). Continuations are normal `send` commands
with `origin = goal:<agent>`, so they show up and count like any other command. An
Eliot goal's budget never limits a native goal running beside it; that needs the native
goal's own budget.

`pause`, `resume` and `clear` bump the goal's `generation`. A continuation is admitted
only if the generation read at the start of the round is still current, so a goal
paused during a long check never sends another turn.

## Storage (L6 migration)

Rules themselves live in files. The Store keeps only what must survive a restart:

```sql
CREATE TABLE rule_cursors (
  rule TEXT PRIMARY KEY, seq INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE TABLE timers (
  rule TEXT PRIMARY KEY, trigger TEXT NOT NULL, anchor INTEGER NOT NULL,
  last_key INTEGER, last_fired_at INTEGER, next_due_at INTEGER
);
CREATE TABLE goals (
  agent INTEGER NOT NULL, mode TEXT NOT NULL, text TEXT NOT NULL, check_cmd TEXT,
  cadence TEXT NOT NULL DEFAULT 'on_idle',
  status TEXT NOT NULL, max_turns INTEGER NOT NULL, turns_used INTEGER NOT NULL DEFAULT 0,
  max_cost_usd REAL, cost_used REAL, no_progress INTEGER NOT NULL,   -- cost_used NULL = not reported
  stale_rounds INTEGER NOT NULL DEFAULT 0, last_fingerprint TEXT, last_check_hash TEXT,   -- stale_rounds: consecutive rounds without progress
  generation INTEGER NOT NULL DEFAULT 0, last_key INTEGER,
  updated_at INTEGER NOT NULL, PRIMARY KEY (agent, mode)
);
```

A rule id of a project rule is `<project>/<id>`, so two projects cannot share a
cursor. On start the daemon reloads cursors, timers and active goals and continues
from them; nothing is kept only in a callback or a sleeping task.

Rate-limit windows are counted from the stored `rule.fired` events, so a restart does
not reset them; loop-depth counters travel in `cause`.

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
`examples/` once L6 lands:

- **Writer → reviewer**: on `turn.completed` of `role=writer` with dirty files →
  `launch` a reviewer with the diff → on reviewer `turn.completed` → `run` a script
  that parses findings and `send`s them back to the writer.
- **Keep main fresh**: `cron */15` → `git.fetch`; on `git.ref` for `origin/main` →
  `inject` "main moved to <sha>" into agents whose base is main.
- **Nightly**: `cron 0 2 * * *` → `launch` profile `maintenance` with a goal and a
  check.
- **Quota fallback**: on `provider.limited kind=quota` of an agent with label
  `fallback=<profile>` → a script checks the failed turn: if it started no tool (no
  side effects), `stop` the agent and `launch` the same prompt on that profile;
  otherwise `notify` — re-running a half-done turn elsewhere could repeat its effects.
  Eliot never switches profile, model or billing route without such a rule.
- **Session rotation** (two rules; v1 field data: long sessions slowed from 7.8 to 1–3
  finished items per hour): (1) on `turn.completed` when turns or session tokens pass
  a threshold → `send` "write a checkpoint to NOTES.md"; (2) on `turn.completed` of
  that checkpoint turn, when the agent has no pending requests and its children are
  idle → `stop` → `launch` a fresh agent with the checkpoint.
