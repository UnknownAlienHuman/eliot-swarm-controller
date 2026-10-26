# Automation

Automation turns committed events and time into ordinary Eliot commands or bounded
local actions. It adds no Task/review/workflow product model. It keeps only the
run/action journal required to survive a crash without repeating an external effect or
retargeting a replacement agent.

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

The daemon reloads files on change. Parse/validation failure emits `rule.failed` and
keeps the last valid version. Unknown keys and duplicate ids are errors. Disable/delete
prevents new runs; already admitted immutable runs remain visible and may be cancelled
only while their actions are queued.

### Triggers

| Trigger | Form | Firing key |
| --- | --- | --- |
| Committed event | `event = "<kind glob>"`, optional `match` | event `seq` |
| Interval | `every = "30m"`, optional `anchor` | anchored slot number |
| Cron | `cron = "<5 fields>"`, optional `tz` | scheduled UTC instant |
| Once | `at = "<RFC3339>"` | UTC instant |
| Manual | `eliot timer run ID` | generated command id |

Ephemeral events cannot trigger durable actions. A time firing is suppressed while the
same rule has a run in `queued|running|unknown`; ticks do not stack. Event keys remain
independent subject to rate limits.

Intervals use `anchor + k·period`. Cron persists the scheduled UTC instant; repeated
DST hour gives two instants, a spring gap fires at the first valid instant after it.
Missed daemon time uses `catch_up=latest` (default) or `none`, never a burst. Tokio
intervals use Skip; due time is recomputed after sleep/wall-clock jump.

## Durable firing and target freeze

`run_id = rule id + firing key`. In one Store transaction the daemon:

1. verifies `(rule,key)` is new;
2. reads one consistent trigger snapshot;
3. renders and validates every action;
4. resolves every agent selector to concrete agent ids and their current generations;
5. for current-turn/request actions, captures the exact trigger/native turn/request id
   or rejects the run if the rule did not provide one;
6. expands multi-target actions into immutable per-target actions;
7. updates timer bookkeeping when applicable;
8. persists run/actions and emits `rule.fired` / `action.queued`.

This is **exactly-once admission**. It is not exactly-once external execution.

A delayed action never resolves `@project`, `@tree`, a name or "current generation"
again. Its payload stores `target_agent`, `expected_generation`, and where relevant
`expected_turn_id` / `request_id`. Dispatch submits those expectations to the router.
A replacement generation produces `stale_generation`; it is not silently targeted.
Rules that intend to affect replacement work fire from a new event or are run again
manually with a new firing key.

Non-agent selectors such as a repository/workspace are normalized to their concrete
path/remote/ref at firing. Credentials remain environment-variable references, not
rendered values.

## Action state and crash semantics

Actions execute in order. A later action starts only after the previous one is
`applied`; `failed`, `unknown` or cancellation stops the run.

```text
queued → dispatched → applied | failed | unknown
queued → cancelled
unknown → applied | failed      // exact readback or owner resolution
```

- `queued` resumes after restart.
- `dispatched` becomes `unknown` after restart.
- `unknown` is never executed again automatically.
- Store-local actions and command admissions use stable ids, so a repeated **admission
  request** reads the same row; it does not replay native I/O.
- External actions use the readback defined below. Without exact readback, they remain
  unknown until `eliot action resolve RUN/INDEX --as applied|failed --reason TEXT`.
- `rule cancel` cancels only queued actions. `rules pause` admits no new runs and
  dispatches no queued actions; it does not undo accepted commands/running processes.

## Matching

`match` maps dotted paths in the committed `{event,agent,data}` snapshot:

- exact: `"data.status" = "completed"`
- glob: `"data.command" = "glob:cargo test*"`
- regex: `"data.path" = "re:^src/(api|db)/"`
- one-of: `"agent.harness" = ["codex", "claude"]`
- presence: `"data.error" = "exists"`

All entries match conjunctively. Complex logic belongs in one script action. Matching
never rereads a later mutable agent row.

## Rendering

`${path}` may fill an argument **value** only. It cannot choose executable, action
kind, cwd root, environment-variable name, option name, control scope or credential.
Unknown placeholders fail the firing transaction; no partial run exists.

For event-triggered actions whose `agent` is omitted, the concrete trigger agent and
its event generation are stored. An explicit id/name/selector is resolved at firing.
A delayed `steer`/`interrupt` must name `${data.turn_id}` (or another exact snapshot
path); approval/answer must name `${data.request_id}`. There is no "whatever is
current when this finally runs" mode.

## Actions

| Action | Fields | Execution/readback |
| --- | --- | --- |
| any core verb | concrete target + frozen expected binding, verb args | `command.submit` with stable id `run_id/index`; mirror its state. Router rejects stale binding. |
| `git.fetch` | concrete repo/workspace, remote? | explicit argv. Unknown: only inspect refs after process group is proven gone; do not refetch to settle. |
| `git.pull` | repo/workspace, remote?, ref? | `--ff-only`. Unknown requires exact HEAD/ref evidence and remains unknown if another writer could explain it. |
| `git.push` | repo/workspace, remote?, ref? | never force. Lost reply: prove process gone, then exact `ls-remote`; never push again blindly. |
| `git.worktree` | repo, branch, base, intended path | read back branch+path; existing mismatch fails. |
| `msg.send` | concrete recipients, text, mode?, reply_to? | one stable Store admission per recipient; presentation remains separately observable. |
| `var.set` | concrete scope/key/value/expected version? | Store CAS with stable action id. |
| `run` | fixed `cmd:[..]`, cwd?, timeout?, env refs? | external process; no generic retry/readback. Unknown remains unknown. Successful bounded stdout may append actions as below. |
| `notify` | text, `via=log|desktop|webhook`, URL reference? | log/desktop settle locally. Webhook gets one automatic attempt unless endpoint has an explicit configured idempotency key. |
| `emit` | `x.user.*`, data | persisted Store event with stable action id. |

All process actions use resolved executable + argv, bounded streams and exact custody.
Git disables prompts/editor. Further pushes to one repository are held while an older
push there is unknown; this narrow writer hold is not a workflow lock.

## Scripts

A `run` action receives one bounded JSON object on stdin:

```json
{"event": {}, "agent": {}, "rule": {}, "run": {}}
```

Environment contains `ELIOT_HOME` and `ELIOT_RULE`, but no agent token. Script CLI
calls therefore act as explicit owner calls; they still use normal command admission.
stderr is bounded log text. stdout is ≤1 MiB NDJSON action objects.

The daemon waits for exit 0 and complete output, parses/validates everything, resolves
selectors/bindings against one post-script Store snapshot, and appends immutable
per-target actions to the same run in one transaction. Only then are they dispatched.
A generation that changes after this append becomes stale at router admission.

Nonzero exit, invalid output or cap breach fails the action. Timeout kills/verifies its
process group. If daemon loses the script after dispatch, it is unknown and is never
rerun. Commands already submitted by the script remain independently durable by id.

Scripts are versioned files; never overwrite a path that may execute.

## Approvals

`approval.requested` can trigger a rule. The rendered approve/deny action stores exact
agent generation, turn and request id from the event. A stale run fails rather than
answering a later request.

A synchronous hook decision evaluates matching rules inside the hook deadline against
that exact request. No match means `ask`. An unanswered asynchronous request produces
one coalesced queue message to parent/project manager/owner. Timeout never chooses the
first/recommended option.

```toml
[[rule]]
id = "allow-tests"
on = { event = "approval.requested", match = { "data.kind" = "command", "data.command" = "glob:cargo test*" } }
do = [{ action = "approve", request_id = "${data.request_id}", turn_id = "${data.turn_id}" }]
```

## Goal loop

A harness native goal and one Eliot goal may coexist with separate text/budgets.

```text
Goal {
  agent_generation,
  mode: native | eliot,
  text,
  check?: argv | marker,
  budget: {max_turns [20], max_cost_usd?, no_progress [3]},
  cadence: on_idle | every <duration>,
  status, turns_used, revision, last_check?
}
```

### Native goal

Plugin maps native goal verbs and emits `goal.updated`. Eliot may pause when configured
turn/cost budget is exceeded. If native says done and a check fails, Eliot marks its
projection blocked; it does not silently restart the native goal.

### Eliot round

On matching idle/cadence:

1. read one snapshot: generation, goal revision, active/pending requests, unknown
   commands and native-goal status;
2. skip on pending request/unknown command; `on_idle` also waits for active native
   goal, while `every` may intentionally queue a send;
3. run bounded check in workspace (default 10 min, global parallel limit 2);
4. pass → done; exhausted budget → blocked;
5. progress = change in complete check output or complete read-only workspace
   fingerprint. Incomplete/unavailable fingerprint alone is not no-progress;
6. otherwise re-read generation/revision and submit one send with both expectations.

Pause/clear/resume increments revision. Any generation/revision change invalidates the
round before command admission. Repeated text is allowed only in a later valid round.
Blocked/done sends one mailbox notification according to recipient wake policy.

## Storage (L6)

```sql
CREATE TABLE automation_runs (
  id TEXT PRIMARY KEY,
  rule TEXT NOT NULL,
  trigger_key TEXT NOT NULL,
  trigger_seq INTEGER,
  state TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(rule, trigger_key)
);
CREATE TABLE automation_actions (
  run TEXT NOT NULL REFERENCES automation_runs(id),
  idx INTEGER NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,          -- immutable rendered/expanded action incl. expected bindings
  state TEXT NOT NULL,
  ref TEXT,
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

Rate/depth windows are in memory and reset after restart; durable run uniqueness still
prevents repeated firing keys.

## Guardrails

- Generated effects carry `cause.depth`; >5 suppresses firing.
- Default per-rule limit 10/min; global 120 actions/min; report suppression once/window.
- `rule test` renders/validates from a supplied snapshot but admits nothing.
- An event of agent A cannot send/steer/inject A unless `allow_self=true`.
- Draining/paused automation admits no new run/goal continuation.
- Every frozen target still passes current authority/capability/state and exact-binding
  validation. Stored actions never bypass the router.

## Workflow recipes

Recipes remain rules/scripts, not core states:

- writer completion with dirty files → concrete reviewer launch; one bounded review
  result returned once;
- timer fetches main; changed ref produces generation-bound notice to concrete agents;
- nightly launches one maintenance profile with goal/check;
- quota error notifies or selects an owner-written new route only when no effect began;
- rotation checkpoints, settles exact children/requests, then stops and creates a new
  generation. Old queued actions fail stale rather than crossing it.
