# Automation

Automation turns committed events/time into ordinary commands or bounded local
external actions. It has no Task/review workflow model. Its durable state exists only
to prevent missed triggers, retargeting and blind replay.

## Rule files and revisions

Rules live in project/data-dir TOML. Required keys: `id`, `on`, `do`; optional:
`enabled`, `limit`, `catch_up`, `allow_self`. IDs and files are bounded and validated.

Every valid canonical rule has a fixed `revision` digest. Existing admitted runs retain
the revision they were rendered from. Editing a rule creates a new revision; it never
mutates an old run.

First load of a new event-rule revision starts at the current persisted event high-water
(no implicit historical backfill). Daemon restart with the same revision resumes from
the durable cursor. A future explicit replay command may name a starting seq; ordinary
hot reload does not.

Timer revision semantics are simple and visible: any `on` edit creates a new timer
schedule/anchor. An action-only edit also creates a new rule revision but does not
re-fire a slot already admitted by the previous revision; future due slots use the new
revision. The timer row stores schedule identity separately from action revision.

## Event trigger cursor

Event rules are level-reconciled from the persisted log, not dependent on a volatile
bus callback:

1. read a bounded page `(event_cursor, high_water]`;
2. evaluate each committed event in seq order against the stored rule revision;
3. in one transaction, create any immutable run/actions and advance cursor past that
   event;
4. on conflict from a previously committed run, advance the cursor without duplicating.

Crash before transaction repeats evaluation safely; crash after transaction cannot
lose the event. Ephemeral events never trigger durable automation.

## Timer triggers

- interval: anchored slot number;
- cron: scheduled UTC instant, with explicit timezone/DST rules;
- once: UTC instant;
- manual: generated command id.

Missed time uses `latest` or `none`, never burst. The same rule/schedule does not stack
while a run is queued/running/unknown. Due processing is reconciled from timer rows on
startup, not from an in-memory timer alone.

## Durable firing and frozen target

A run identity is fixed-length derived from rule id, rule revision and trigger key. In
one transaction the daemon:

1. verifies the trigger key has no run for that schedule/revision;
2. reads one consistent trigger snapshot;
3. renders/validates actions;
4. resolves selectors/names to concrete agent ids and exact generations;
5. captures exact turn/request for delayed controls;
6. expands multi-target work to immutable per-target actions;
7. updates cursor/timer state and emits run/action events.

Delayed automation never resolves “whatever is current later”. Replacement generation
is stale and requires a new trigger/manual run.

## Action states

```text
queued → dispatched → applied | failed | unknown
queued → cancelled
unknown → applied | failed       // exact readback or owner resolution
```

Queued actions resume after restart. Dispatched actions become unknown. Unknown is
never run again automatically. Store-local writes use stable ids. External writers
need exact readback or remain unknown. Actions in a run execute in order; unknown or
failed stops later actions.

## Matching and rendering

Match exact/glob/regex/one-of/presence paths in the committed `{event,agent,data}`
snapshot. Core stale/diagnostic events are not eligible unless a rule explicitly
matches their `x.eliot.*` kind.

Placeholders may fill data values only. They cannot choose executable, cwd root, env
name, action kind, option name, credential, authority or target scope. Missing values
fail the firing transaction.

A delayed steer/interrupt must carry exact turn id; approve/answer exact request id.
An omitted agent on an event rule means the concrete triggering agent+generation—not a
future lookup.

## Actions

| Action | Rule |
| --- | --- |
| core verb | submit stable command id with frozen expected/target binding |
| Git fetch/pull/push/worktree | explicit argv and repository/ref intent; push never force; unknown uses exact readback only |
| mail/var | stable Store admission per concrete recipient/key |
| script `run` | fixed executable/argv/cwd; bounded streams; no generic replay |
| `notify log|desktop` | local bounded result |
| `notify webhook` | named configured endpoint only; no raw URL/secret persisted in rule/action |
| `emit` | bounded `x.user.*` persisted event |

Git unknown settlement waits for exact process custody to be empty. Unknown push then
reads exact remote ref; it never pushes again. A repository has a narrow writer hold
while an earlier push is unknown.

`run` environment uses literal nonsecret values plus named `pass_env` references.
Secret values are resolved at execution and never stored in action payload/output. A
script receives bounded JSON on stdin. The daemon buffers all stdout action objects,
waits for exit 0, validates/resolves them against one snapshot, appends them in one
transaction, then dispatches. Lost script outcome is unknown and not rerun.

## Approval rules

Approval/answer actions store exact generation/turn/request from the triggering event.
Synchronous hooks return a decision only after its durable admission commits; Store
failure/timeout returns native-safe `ask`. No timeout chooses a default option.

## Goal loop

Native and Eliot goals are independent but generation-bound. Internal cost is integer
micro-USD; user CLI may accept decimal USD and parse it exactly. No floating-point
budget storage.

```text
Goal {
  agent, agent_generation, mode, text, check?, cadence,
  max_turns, max_cost_microusd?, no_progress,
  status, turns_used, cost_used_microusd?, revision,
  next_due_at?, last_workspace_hash?, last_check_hash?
}
```

A generation change pauses the goal. Resume/set explicitly rebinds it after native
readback where applicable.

Goal rounds are level-reconciled. On startup and after every relevant state change, the
daemon queries active goals whose agent is idle/due and has no in-flight round. It does
not rely solely on an `agent.idle` edge that a crash could miss.

One round:

1. capture generation + goal revision + pending/unknown/native-goal state;
2. skip on pending request or unknown effect; `on_idle` also waits for active native
   goal;
3. run bounded check;
4. pass → done; exhausted exact budget → blocked;
5. progress = changed complete check output or complete read-only workspace
   fingerprint; unavailable fingerprint alone is not no-progress;
6. re-read generation/revision and admit one exact linked send only if unchanged.

A durable `goal_round` identity prevents duplicate continuation after restart. Pause,
clear or generation change invalidates a running check before send admission.

## Mail/rule/launch recovery is level-based

Automation is not the only component using current-state reconciliation:

- unread mail with no presentation action is reconsidered after restart;
- `created` launches and queued resumes are reconsidered against current capacity;
- active goals reconcile as above;
- provider holds/probes reconcile from persisted route state.

A lost wakeup never requires repeating an uncertain native effect.

## Storage

```sql
CREATE TABLE rule_state (
  rule TEXT NOT NULL,
  revision TEXT NOT NULL,
  event_cursor INTEGER NOT NULL,
  loaded_at INTEGER NOT NULL,
  PRIMARY KEY(rule, revision)
);

CREATE TABLE automation_runs (
  id TEXT PRIMARY KEY,
  rule TEXT NOT NULL,
  rule_revision TEXT NOT NULL,
  trigger_key TEXT NOT NULL,
  trigger_seq INTEGER,
  state TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(rule, rule_revision, trigger_key)
);

CREATE TABLE automation_actions (
  run TEXT NOT NULL REFERENCES automation_runs(id),
  idx INTEGER NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
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
  rule TEXT NOT NULL,
  schedule_revision TEXT NOT NULL,
  action_revision TEXT NOT NULL,
  last_key TEXT,
  next_due_at INTEGER,
  PRIMARY KEY(rule, schedule_revision)
);

CREATE TABLE goals (
  agent INTEGER NOT NULL,
  mode TEXT NOT NULL,
  agent_generation INTEGER NOT NULL,
  text TEXT NOT NULL,
  check_cmd TEXT,
  cadence TEXT NOT NULL,
  status TEXT NOT NULL,
  max_turns INTEGER NOT NULL,
  turns_used INTEGER NOT NULL DEFAULT 0,
  max_cost_microusd INTEGER,
  cost_used_microusd INTEGER,
  no_progress INTEGER NOT NULL,
  stale_turns INTEGER NOT NULL DEFAULT 0,
  last_workspace_hash TEXT,
  last_check_hash TEXT,
  revision INTEGER NOT NULL DEFAULT 0,
  next_due_at INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent, mode)
);
```

## Guardrails

Cause depth, per-rule/global rate limits, self-trigger protection, bounded payloads and
current router validation all apply. Paused/draining automation admits no new run or
goal continuation. No guardrail is implemented as another model call.
