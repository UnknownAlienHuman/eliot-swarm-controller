# Automation

Automation turns committed events/time into ordinary commands or bounded local actions.
It has no Task/review workflow model. Durable state exists only to avoid missed
triggers, retargeting and blind replay.

## Rule scope, files and revisions

- `<data_dir>/rules/*.toml`: global rule, owner authority.
- `<registered project>/.eliot/rules/*.toml`: project rule, limited to that project.

Project rules may command/message project agents, use project/tree vars, run scripts in
the registered project and act on that repository. They cannot target another project,
global vars, arbitrary cwd, plugin/service/account config or an undeclared webhook.
Project event rules implicitly require `subject.project` to equal their registered
project even when the TOML omits that match. This is a correctness boundary inside the
one-user trust model, not hostile-process security.

Scripts receive an ephemeral scoped automation token; omitting it does not turn them
into owner. Global scripts get owner scope, project scripts get project scope.

Required keys are `id`, `on`, `do`; optional keys are `enabled`, `limit`, `catch_up`,
`allow_self`. Canonical rule bytes produce a fixed `revision`. Admitted runs retain the
revision they were rendered from; editing never mutates an old run.

First load of a new event-rule revision starts at current event high-water; there is no
implicit historical replay. Restart with the same revision resumes its durable cursor.
Disabled or globally paused event rules advance to current high-water without firing,
so pause does not create a later burst or pin retention. Re-enable starts from that
cursor.

## Event cursor and retention

Event rules reconcile from the persisted log:

1. read a bounded page `(cursor, high_water]`;
2. evaluate the committed event and its stored post-transition subject snapshot;
3. handle one event transactionally as described below;
4. continue in sequence order.

For each event:

- **no match:** advance the cursor only;
- **valid match:** create immutable run/actions and advance the cursor in one
  transaction;
- **duplicate run key:** read the existing run and advance the cursor without another
  effect;
- **deterministic firing error:** create a failed run with no actions, persist bounded
  structured outcome, emit `rule.failed`, and advance the cursor in one transaction;
- **Store/commit failure:** roll back everything including the cursor and retry that
  same event only after Store recovery.

Deterministic firing errors include missing placeholders, invalid rendered action,
secret-bearing durable payload, selector/target absent from the firing snapshot,
project-authority violation and unsupported action kind. They are not retried on every
reconciliation. Mutable execution conditions such as capacity, provider hold or a
currently busy agent produce a valid queued action and are handled by ordinary action
state; they are not rendering failures.

Crash before the transaction repeats safely; crash after it cannot lose or duplicate
the firing admission. Ephemeral and bounded late-diagnostic events are ineligible
unless a rule explicitly matches their `x.*` kind.

Retention deletes only through the minimum cursor of active event-rule revisions. Rule
lag is visible and bounded by configured count/age. When a rule exceeds that limit,
one transaction:

1. records `rule.paused reason=backlog_limit` and the exact skipped sequence range;
2. advances its normal cursor to current high-water so it no longer pins storage;
3. leaves the rule disabled until explicit owner action.

`resume --from-now` continues from that cursor. `resume --catch-up` creates an explicit
new replay revision over the recorded range only while those events still exist; if
retention removed any part, it fails `history_gap`. Nothing is silently discarded or
allowed to pin disk indefinitely.

## Timers and revisions

An interval key is its anchored slot; cron/once key is scheduled UTC instant; manual
key is caller id. DST is evaluated in the declared timezone but identity is UTC. Missed
time uses `latest` or `none`, never a burst. Due rows reconcile on startup.

Changing `on` creates a schedule revision/new anchor. An action-only edit changes action
revision but never re-fires an admitted slot; future slots use the new actions. One
schedule does not stack while its previous run is queued, running or unknown.

## Durable firing and frozen target

Run id is a fixed derived id of principal, rule revision and trigger key. One
transaction:

1. verifies uniqueness;
2. reads the stored trigger snapshot;
3. renders and validates actions under the rule principal;
4. resolves names/selectors to concrete agent ids/generations;
5. captures exact turn/request for delayed controls;
6. expands multi-target work into immutable per-target actions;
7. advances event cursor/timer and emits run/action state.

A delayed action never resolves “whatever is current later”. A replacement generation
is stale and requires a new trigger or manual run.

## Action states

```text
queued → dispatched → applied | failed | unknown
queued → cancelled
unknown → applied | failed       // exact readback or explicit owner resolution
```

Queued actions resume after restart. Dispatched becomes unknown. Unknown is never
executed again automatically. Actions run in order; failed/unknown/cancelled stops later
actions. Store-local writes use stable ids; external writers require exact readback or
remain unknown.

## Matching and rendering

Match exact/glob/regex/one-of/presence paths in stored `{event,subject,data}`. Never join
a later mutable Agent row. Placeholders fill data values only—not executable, cwd root,
environment name, action kind, option, credential, authority or target scope.

A missing/invalid value produces the deterministic failed firing above and advances
past that event; it cannot wedge the rule cursor. An omitted event target means the
exact triggering agent+generation. Delayed steer/interrupt requires exact turn;
request replies require exact request.

## Actions

| Action | Boundary |
| --- | --- |
| core verb | stable command id + frozen expected/target binding, routed under rule principal |
| Git fetch/pull/push/worktree | explicit repo/ref/path intent; push never force; exact readback only |
| mail/var | concrete recipients/scope under rule principal |
| script | fixed executable/argv/project-safe cwd; principal-scoped env; bounded streams; no unknown rerun |
| log/desktop notify | bounded local result |
| webhook | globally named endpoint; project must be allowlisted; no raw URL/secret in payload |
| emit | bounded `x.user.*` in principal scope |

Unknown Git settlement waits until exact process custody is empty. Push then reads the
exact remote ref and never pushes again. One repository has a narrow writer hold while
an earlier push is unknown.

### Script environment and output

Script environment starts from the same minimal OS baseline as other managed processes.
Literal action values must be nonsecret. Named daemon-environment references are
validated **before resolution and process start**:

- a global script may use only names in the global
  `daemon.automation.script_pass_env_allow` list;
- a project script may use only the intersection of that global list and the
  registered project's `pass_env_allow` list;
- a project rule cannot use a variable merely because another profile/project is
  globally allowed to use it;
- returned stdout actions cannot introduce or expand environment references.

Only after authority and both allowlists pass are values resolved in memory and marked
for masking. No resolved value enters run/action payloads, events or logs.

Script stdout actions are fully buffered, parsed, authority-checked and appended in one
transaction after exit 0; only then are they dispatched. A lost script outcome is
unknown and the script is not rerun.

## Approval rules

Request actions freeze generation/turn/request. A synchronous hook decision is a
deadline-bound proposal returned only after its durable command/action admission
commits. Timeout or Store failure returns native-safe `ask`; it never creates a delayed
automatic approval and never chooses a default option.

## Goal loop

Native/Eliot goals are independent and generation-bound. Cost uses integer micro-USD;
`max_cost` is accepted only when the plugin reports exact machine-readable cost.
Generation change pauses both projections until explicit readback/rebind.

```text
Goal {
  agent, generation, mode, text, check?, cadence,
  max_turns, max_cost_microusd?, no_progress,
  status, turns_used, cost_used_microusd?, revision,
  next_due_at?, last_workspace_hash?, last_check_hash?
}
```

Goal rounds reconcile from current rows on startup/state changes; no idle edge is
required. A round is an `automation_run` under principal `goal:<agent>`:

1. capture generation/revision/pending/unknown/native-goal state;
2. skip on request/unknown; on-idle also waits for active native goal;
3. marker check is read-only Store action; argv check is a journaled process action;
4. unknown argv-check outcome blocks the round and is never rerun automatically;
5. pass → done; exact budget exhausted → blocked;
6. progress = changed complete check output or complete workspace fingerprint;
7. re-read generation/revision and admit one exact send if unchanged.

Stable goal-run identity prevents duplicate continuation. Pause, clear or generation
change invalidates a running check before send admission.

## Level reconciliation

The same current-state pattern covers unread mail presentation, created launches,
queued resumes, provider holds/probes and active goals. A lost wakeup never authorizes
repeating an uncertain native effect.

## Storage

```sql
CREATE TABLE rule_state (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  revision TEXT NOT NULL,
  active INTEGER NOT NULL,
  event_cursor INTEGER NOT NULL,
  paused_reason TEXT,
  skipped_from INTEGER,
  skipped_through INTEGER,
  loaded_at INTEGER NOT NULL,
  PRIMARY KEY(principal, rule, revision)
);

CREATE TABLE automation_runs (
  id TEXT PRIMARY KEY,
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  rule_revision TEXT NOT NULL,
  trigger_key TEXT NOT NULL,
  trigger_seq INTEGER,
  state TEXT NOT NULL,                 -- queued|running|applied|failed|unknown|cancelled
  outcome TEXT,                        -- bounded structured result/firing error
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(principal, rule, rule_revision, trigger_key)
);

CREATE TABLE automation_actions (
  run TEXT NOT NULL REFERENCES automation_runs(id),
  idx INTEGER NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  state TEXT NOT NULL,
  ref TEXT, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY(run, idx)
);
CREATE INDEX automation_actions_open ON automation_actions(state)
  WHERE state IN ('queued','dispatched','unknown');

CREATE TABLE timers (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  schedule_revision TEXT NOT NULL,
  action_revision TEXT NOT NULL,
  last_key TEXT, next_due_at INTEGER,
  PRIMARY KEY(principal, rule, schedule_revision)
);

CREATE TABLE goals (
  agent INTEGER NOT NULL, mode TEXT NOT NULL,
  agent_generation INTEGER NOT NULL,
  text TEXT NOT NULL, check_cmd TEXT, cadence TEXT NOT NULL,
  status TEXT NOT NULL,
  max_turns INTEGER NOT NULL, turns_used INTEGER NOT NULL DEFAULT 0,
  max_cost_microusd INTEGER, cost_used_microusd INTEGER,
  no_progress INTEGER NOT NULL, stale_turns INTEGER NOT NULL DEFAULT 0,
  last_workspace_hash TEXT, last_check_hash TEXT,
  revision INTEGER NOT NULL DEFAULT 0,
  next_due_at INTEGER, updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent, mode)
);
```

## Guardrails

Cause depth, principal scope, per-rule/global rate limits, self-trigger protection,
bounded payloads and router validation all apply. Draining admits no new run/goal
round. No guardrail uses another model call.
