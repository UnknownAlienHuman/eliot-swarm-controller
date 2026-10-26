# Automation

Automation turns events and time into ordinary Eliot commands or bounded local
actions. It adds no Task/review/workflow product model. It does keep the minimum
run/action journal required to survive a crash without repeating an external effect.

## Rules

Rules live in `<project>/.eliot/rules/*.toml` and `<data_dir>/rules/*.toml`.
Required keys are `id`, `on`, `do`; optional keys are `limit`, `catch_up`,
`allow_self`, `enabled` (default true).

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

The daemon reloads files on change. A parse/validation failure emits `rule.failed` and
keeps the last valid version active. Unknown keys and duplicate rule ids are errors.
Deleting/disable prevents new firings; already admitted runs remain visible and may be
cancelled explicitly.

### Triggers (`on`)

| Trigger | Form | Semantics |
| --- | --- | --- |
| Event | `event = "<kind glob>"`, optional `match` | One trigger key per committed event `seq`. Ephemeral events cannot trigger durable actions. |
| Interval | `every = "30m"`, optional `anchor` | Slot `k` is `anchor + k·period`; missed slots coalesce. |
| Cron | `cron = "<5 fields>"`, optional `tz` | Match local fields, persist scheduled UTC instant as key. Repeated DST hour has two distinct instants; a spring gap fires at the first valid instant after it. |
| Once | `at = "2026-11-01T09:00:00+03:00"` | One UTC-instant key, then consumed in timer state. |

A time-trigger firing is suppressed while the same rule has a run in
`queued|running|unknown`; ticks do not stack behind slow work. Event firings remain
independent because each event has its own key, subject to rate limits.

`eliot timer list` shows next due, last key and any in-flight run. `eliot timer run ID`
creates a new manual key; it does not counterfeit a scheduled slot.

Missed ticks while the daemon was down use `catch_up = "latest"` (default) or `none`.
Never burst missed slots. Tokio intervals use `MissedTickBehavior::Skip`; waits use a
monotonic clock and due times are recomputed from UTC after sleep/wall-clock jumps.

## Durable firing and action journal

A firing key is:

- event trigger: event `seq`;
- interval: slot number;
- cron/at: scheduled UTC instant;
- manual: generated command id.

`run_id = rule id + firing key`. In one Store transaction the daemon:

1. verifies `(rule, key)` has no existing run;
2. renders placeholders from the triggering snapshot;
3. validates every action and stores the immutable rendered list;
4. updates timer bookkeeping when applicable;
5. emits `rule.fired` and `action.queued`.

This is **exactly-once admission**. It is not a claim that a network/process effect can
be completed exactly once.

Actions execute in order. A later action starts only after the previous one is
`applied`; `failed`, `unknown` or cancelled stops the run. States are:

```text
queued → dispatched → applied | failed | unknown
queued → cancelled
unknown → applied | failed      // exact readback or owner resolution only
```

Crash rules:

- `queued` actions resume after restart.
- `dispatched` becomes `unknown` after restart.
- an `unknown` action is never executed again automatically.
- store-local actions and Eliot commands use stable ids, so repeating their **admission
  call** with the same id only reads the existing result; this is not a repeated native
  effect.
- external actions require their specified readback. When no exact readback exists,
  the run remains unknown until
  `eliot action resolve RUN/INDEX --as applied|failed --reason TEXT`.

`eliot rules pause` stops admitting new runs and dispatching queued actions; it does
not cancel commands already accepted or processes already dispatched. `rule cancel`
cancels only queued actions, never an unknown external effect.

## Matching

`match` is a map of dotted paths into `{event, agent, data}`:

- exact: `"data.status" = "completed"`
- glob: `"data.command" = "glob:cargo test*"`
- regex: `"data.path" = "re:^src/(api|db)/"`
- one-of: `"agent.harness" = ["codex", "claude"]`
- presence: `"data.error" = "exists"`

All entries must match. Anything more complex is a script action. Matching sees the
committed event and agent projection from the same snapshot, not mutable later state.

## Actions (`do`)

`${path}` placeholders may fill an argument **value** only—never executable, cwd, env
name, option name, action kind or target control scope. Trigger data is untrusted.
Unknown placeholders fail admission; no partially rendered run exists.

| Action | Fields | Execution/readback |
| --- | --- | --- |
| any [verb](core.md#verbs) | `agent` (id/name/selector; default triggering agent), verb args | Submit a command with stable id `run_id/index`; command generation/turn/request binding is resolved by the router at admission. Action mirrors command result. |
| `git.fetch` | `workspace|repo`, `remote?` | Explicit argv. On unknown, compare fetched refs only after process group is gone; never refetch merely to settle. |
| `git.pull` | `workspace|repo`, `remote?`, `ref?` | `--ff-only`; unknown needs HEAD/ref readback and remains unknown if another writer could explain it. |
| `git.push` | `workspace|repo`, `remote?`, `ref?` | Never force. Lost reply: prove process group gone, then exact `ls-remote`; never push again blindly. |
| `git.worktree` | `repo`, `branch`, `base` | Stable intended path/branch; read back both. Existing mismatched path/branch fails. |
| `msg.send` | `to`, `text`, `mode?` | Store transaction with stable action id; repeated admission returns same message. |
| `var.set` | `scope`, `key`, `value`, `if_version?` | Store CAS with stable action id. |
| `run` | `cmd:[..]`, `cwd?`, `timeout?`, `env?` | External process; no generic retry/readback. Unknown stays unknown. Successful bounded stdout may append more actions durably (below). |
| `notify` | `text`, `via?: log|desktop|webhook`, `url?` | Log/desktop can settle locally. Webhook is at-most-one automatic attempt unless the endpoint accepts an idempotency key configured for the action. |
| `emit` | `kind` (`x.user.*`), `data` | Persisted Store event with stable action id. |

All process actions use explicit argv, resolved executable, bounded output and the
custody rules in [monitoring](monitoring.md#starting-processes). Git sets
`GIT_TERMINAL_PROMPT=0` and no editor.

Further pushes to a repository are refused while an earlier push action there is
`unknown`. This is a narrow external-write hold, not a global workflow lock.

## Scripts

A `run` action receives one JSON object on stdin:

```json
{"event": {}, "agent": {}, "rule": {}, "run": {}}
```

Environment includes `ELIOT_HOME` and `ELIOT_RULE`; the script has no agent token, so
explicit `eliot` CLI calls act as the owner. stderr goes to daemon logs. stdout is
bounded to 1 MiB and may contain NDJSON action objects.

The daemon does **not** execute stdout actions while the script is still running. It:

1. waits for exit 0 and complete bounded stdout;
2. parses and validates every action;
3. appends them to the same run in one Store transaction;
4. then dispatches them in order.

Nonzero exit, invalid output or output cap → `failed`. Timeout kills and verifies the
whole process group. If the daemon loses the script after dispatch, the action is
`unknown` and the script is not rerun. Eliot commands the script already submitted are
separately durable under their own ids.

Scripts are versioned under `.eliot/scripts/`; never edit a path that may be executing.
Install a new file and update the rule for future runs.

## Approvals

`approval.requested` events can trigger ordinary rules. `approve`/`deny` actions bind
the exact agent generation, turn and request id. A stale rule run is rejected rather
than answering whatever request is now pending.

A synchronous hook decision evaluates matching rules within the hook deadline. No
match means `ask`. An unanswered asynchronous request produces one queue message to
the parent, else project manager, else owner. Timeout never picks a first/recommended
choice and never means blanket deny.

```toml
[[rule]]
id = "allow-tests"
on = { event = "approval.requested", match = { "data.kind" = "command", "data.command" = "glob:cargo test*" } }
do = [{ action = "approve" }]
```

## Goal loop

Two independent goal modes exist: the harness's **native goal** and an **Eliot goal**
that sends ordinary turns. An agent may have one of each with different text.

```text
Goal {
  agent_generation,
  mode: native | eliot,
  text,
  check?: argv | "marker:<TEXT>",
  budget: {max_turns [20], max_cost_usd?, no_progress [3]},
  cadence: on_idle | every <duration>,
  status, turns_used, revision, last_check?
}
```

### Native goal

The plugin maps native `goal.*` and emits `goal.updated`. Eliot can pause it when the
configured turn/cost budget is exceeded. If native status says done and a `check`
exists, Eliot runs the check; failure sets Eliot's projection to `blocked
reason=check_failed`. It does not silently restart the native goal.

### Eliot goal round

On a matching `agent.idle` or cadence tick:

1. Read one consistent snapshot of agent generation, goal revision, pending requests,
   open unknown commands and native-goal status.
2. Skip if an approval/question or unknown command is open. With `on_idle`, also skip
   while a native goal is active; `every` may intentionally queue a normal send.
3. Run `check` in the workspace (default timeout 10 min; global
   `max_parallel_checks = 2`). `marker:X` succeeds when the latest bounded
   `message.agent` for this generation contains X.
4. If check passes → `done`. If budget is exhausted → `blocked budget`.
5. Progress is a change in check output or the read-only workspace fingerprint from
   [monitoring](monitoring.md#read-only-workspace-fingerprint). An unavailable/incomplete
   fingerprint never counts as "no change" by itself. `no_progress` consecutive
   complete rounds → `blocked no_progress`.
6. Otherwise submit one `send` with origin `goal:<agent>`, bound to the agent generation
   and goal revision:

   `Goal: <text>\nThe check `<cmd>` still fails:\n<last 40 lines>\nContinue.`

Before admitting the send, re-read generation/revision. Pause, clear, resume, a new
native session or a concurrent successful check invalidates the round. A stale round
creates no command. Repeating the same text in a later valid round is allowed.

`pause`, `resume` and `clear` increment `revision`. `blocked`/`done` notify the parent
or owner through the mailbox. Eliot-goal budgets do not limit a native goal beside it;
that mode has its own budget.

## Storage (L6 migration)

Rules remain files. Store only execution state:

```sql
CREATE TABLE automation_runs (
  id TEXT PRIMARY KEY,
  rule TEXT NOT NULL,
  trigger_key TEXT NOT NULL,
  trigger_seq INTEGER,
  state TEXT NOT NULL,              -- queued|running|applied|failed|unknown|cancelled
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(rule, trigger_key)
);
CREATE TABLE automation_actions (
  run TEXT NOT NULL REFERENCES automation_runs(id),
  idx INTEGER NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,            -- immutable rendered action
  state TEXT NOT NULL,
  ref TEXT,                         -- command/message/process/native readback id
  outcome TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(run, idx)
);
CREATE INDEX automation_actions_open ON automation_actions(state)
  WHERE state IN ('queued','dispatched','unknown');

CREATE TABLE timers (
  rule TEXT PRIMARY KEY,
  last_key TEXT,
  last_fired_at INTEGER,
  next_due_at INTEGER
);
CREATE TABLE goals (
  agent INTEGER NOT NULL,
  mode TEXT NOT NULL,
  agent_generation INTEGER NOT NULL,
  text TEXT NOT NULL,
  check_cmd TEXT,
  cadence TEXT NOT NULL DEFAULT 'on_idle',
  status TEXT NOT NULL,
  max_turns INTEGER NOT NULL,
  turns_used INTEGER NOT NULL DEFAULT 0,
  max_cost_usd REAL,
  cost_used REAL,
  no_progress INTEGER NOT NULL,
  stale_turns INTEGER NOT NULL DEFAULT 0,
  last_workspace_hash TEXT,
  last_check_hash TEXT,
  revision INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent, mode)
);
```

Rate-limit windows and loop-depth counters are in memory; restart resets them. Durable
run uniqueness still prevents repeated trigger keys.

## Guardrails

- Every generated command/event/action carries `cause.depth`; depth > 5 suppresses the
  firing.
- Per-rule default limit: 10 firings per minute; global default 120 actions/minute.
- Excess is `rule.suppressed` once per window.
- `eliot rule test ID --event FILE` renders and validates without admitting a run.
- Events of agent A cannot create `send`/`steer`/`inject` to A unless
  `allow_self = true`; this blocks status/failure ping-pong.
- Automation paused or daemon draining admits no new runs and no goal continuation.
- Every target is revalidated by the router at action admission. A stored action never
  bypasses current generation, control or capability checks.

## Workflow recipes

Workflows are rules + scripts + agents, not core product states:

- **Writer → reviewer:** on writer turn completion with dirty files, launch a reviewer;
  parse one bounded review result and send all findings back once.
- **Keep main fresh:** timer `git.fetch`; on a changed remote ref, inject/queue a notice
  to agents whose base is that ref.
- **Nightly:** cron launches a maintenance profile with a goal and exact check.
- **Quota fallback:** only after a failed turn with no tool/side effect; otherwise
  notify. Never rerun a half-done turn on another route.
- **Session rotation:** request a checkpoint; after its exact turn completes and
  children/requests are settled, stop and launch/resume a new generation from the
  checkpoint. Old queued commands expire rather than crossing the rotation.
