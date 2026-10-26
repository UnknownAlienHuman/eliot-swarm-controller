# Automation

Automation turns committed events/time into ordinary commands or bounded local actions.
It has no Task/review workflow model. Durable state exists only to avoid missed
triggers, retargeting and blind replay.

## Rule scope and files

- `<data_dir>/rules/*.toml`: global rule, owner authority.
- `<registered project>/.eliot/rules/*.toml`: project rule, limited to that project.

Project rules may command/message project agents, use project/tree vars, run project
scripts and act on that repository. They cannot target another project, global vars,
arbitrary cwd, plugin/service/account config or undeclared webhook. Project event rules
implicitly require stored `subject.project` to equal their registered project. This is
a correctness boundary inside one-user trust model, not hostile-process security.

Script processes receive ephemeral scoped **read-only** automation token. Clearing or
omitting it never becomes owner because custody classification fails closed. Mutating
script effects are returned as stdout actions and admitted later by daemon under parent
rule principal.

Required keys: `id`, `on`, `do`; optional `enabled`, `limit`, `catch_up`, `allow_self`.
Canonical bytes produce fixed `revision`; admitted runs retain it.

## Revision handoff

New rule id starts at current event high-water H, no implicit backfill. Restart with
same revision resumes durable cursor.

Editing enabled event rule uses one transaction:

1. capture H;
2. old revision → `retiring`, `retire_at_seq=H`;
3. new revision → `active`, `event_cursor=H`.

Old evaluates `(old_cursor,H]`, new `(H,current]`; no overlap/gap. Crash resumes both.
Old becomes retired after cursor H and runs settle. Backlog policy applies independently.

Disable/delete/global pause instead captures H, records skipped `(cursor,H]`, advances
cursor and sets disabled/paused. Re-enable defaults from-now. Explicit catch-up creates
separate replay revision over still-retained recorded range. Parse failure installs
nothing and leaves last valid revision.

## Event cursor and deterministic failure

Rules reconcile persisted log by bounded pages. For each committed event + stored
post-transition subject:

- no match → cursor advances;
- valid match → immutable run/actions + cursor commit together;
- existing trigger key → read existing run + advance;
- deterministic firing error → one failed run with no actions + bounded outcome +
  `rule.failed` + cursor advance;
- Store failure → rollback including cursor; retry only after recovery.

Deterministic errors: missing placeholder, invalid action, secret-bearing payload,
selector/target absent, selector fanout over configured limit, project-authority
violation, unsupported action kind. Capacity/provider hold/busy target is valid queued
work, not rendering error.

Crash before transaction repeats safely; crash after cannot lose/duplicate admission.
Ephemeral and late diagnostics are ineligible unless explicitly matched by `x.*`.

Retention deletes only through minimum cursor of `active|retiring|replay` revisions.
Lag is visible/bounded. On backlog limit one transaction records pause/skipped range,
advances to interval boundary and releases retention. `resume --catch-up` requires full
range still present or returns `history_gap`.

## Timers and revisions

Interval key is anchored slot; cron/once key is UTC instant; manual key is external
WriteId. DST uses declared timezone but identity UTC. Missed time is latest/none, never
burst. Due rows reconcile on startup.

Reload captures wall-clock T and reconciles old schedule through T before closing.
Changed `on` starts new schedule strictly after T. Action-only edit keeps schedule:
slots through T use old action revision, later slots new. Already admitted runs retain
old revision. One schedule does not stack while prior run queued/running/unknown.

## Generic durable run identity

Every firing—event rule, timer, manual rule run or Goal round—uses one journal shape:

```text
RunIdentity {
  principal,
  kind: event_rule | timer | manual | goal,
  owner_key, owner_revision,
  trigger_key
}
```

Examples:

- event rule: owner key rule id, revision rule digest, trigger event seq;
- timer: owner key rule id, revision action/schedule identity, trigger slot/instant;
- manual: owner key rule id, revision current rule, trigger external WriteId;
- Goal: owner key `<agent>/<mode>`, revision goal revision, trigger round ordinal/due key.

Run id is fixed derived digest of this identity. One transaction verifies uniqueness,
reads trigger snapshot, renders under principal, expands selectors into concrete targets
(up to `max_selector_targets`), captures exact generation/turn/request, writes immutable
actions, advances cursor/timer and emits state. Delayed work never retargets replacement
generation.

A core-verb action receives derived command id from `(run,index,target)` and no external
receipt. Multi-target expansion creates one action/command per target in deterministic
AgentId order; limit overflow is deterministic failed firing with no partial rows.

## Action states

```text
queued → dispatched → applied | failed | unknown
queued → cancelled
unknown → applied | failed       // exact readback or owner assertion
```

Queued resumes after restart. Dispatched becomes unknown. Unknown never executes again.
Actions execute in order; failed/unknown/cancelled stops later. Store-local writes use
stable ids; external writers need exact readback or remain unknown.

## Matching and rendering

Match exact/glob/regex/one-of/presence in stored `{event,subject,data}`; never join later
Agent row. Placeholders fill values only, not executable/cwd root/env name/action kind/
credential/authority/scope. Missing/invalid value creates deterministic failed run and
advances cursor. Omitted event target is exact triggering agent+generation. Delayed
controls require exact turn/request.

## Actions

| Action | Boundary |
| --- | --- |
| core verb | derived command id + frozen binding, routed under principal |
| Git fetch/pull/push/worktree | explicit repo/remote/ref/OID/path; no force; exact readback only |
| mail/var | concrete recipient/scope under principal |
| script | fixed executable/argv/project-safe cwd; scoped env; bounded streams; no rerun |
| log/desktop notify | bounded local result |
| webhook | named endpoint; project allowlist; no raw URL/secret |
| emit | bounded `x.user.*` in principal scope |

### External action readback

Every process action records pre-state, intended state and exact custody. Readback begins
only after custody empty; otherwise unknown.

- fetch: explicit remote/refspec; read requested tracking refs against exact remote OIDs;
  remote movement/ambiguity remains unknown; never refetch to settle;
- pull: fast-forward-only; applied requires exact intended upstream OID and clean/readback
  state with no alternate writer explanation;
- push: applied requires exact local OID at exact remote ref; different OID is unknown,
  never another push;
- worktree: applied requires exact normalized path/branch/frozen base/HEAD in porcelain;
- webhook: action id sent as configured idempotency key; no automatic retry; lost response
  settles only through configured exact status lookup;
- generic script/process: no generic readback; lost outcome unknown.

Repository has narrow writer hold while older push/pull/worktree write unknown.

### Script environment and output

Managed script starts from minimal OS baseline. Named environment refs validate before
value lookup/process start:

- global script: global automation allowlist;
- project script: intersection global allowlist ∩ project allowlist;
- returned actions cannot introduce environment refs.

Resolved values exist in memory only and are masked. stdout actions are fully buffered,
parsed, principal/target checked and appended atomically after exit 0; only then dispatch.
Lost script outcome is unknown and script is not rerun.

## Approval rules and synchronous hooks

Asynchronous request actions are ordinary frozen actions and execute only while exact
request remains pending.

Synchronous hook is not ordinary queued action:

1. stable native hook/request id + exact binding + absolute deadline;
2. evaluate bounded rules only—no model/external process;
3. transactionally validate pending request and insert/reuse non-dispatchable proposal
   expiring before deadline margin;
4. return only after commit ack before deadline while still pending;
5. otherwise native-safe `ask`; proposal never dispatches later.

Same-id retry before expiry may return proposal. Late commit is expired/audit-only.

```sql
CREATE TABLE hook_decisions (
  id TEXT PRIMARY KEY,
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  turn_id TEXT,
  request_id TEXT NOT NULL,
  decision TEXT NOT NULL,
  rule TEXT NOT NULL,
  rule_revision TEXT NOT NULL,
  state TEXT NOT NULL,                 -- proposed|returned|expired|resolved
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  resolved_at INTEGER
);
```

## Goal loop

Native/Eliot goals are independent/generation-bound. Cost is integer micro-USD and
accepted only with exact machine-readable plugin cost. Generation change pauses until
explicit rebind.

```text
Goal {
  agent, generation, mode, text, check?, cadence,
  max_turns, max_cost_microusd?, no_progress,
  status, turns_used, cost_used_microusd?, revision,
  next_due_at?, last_workspace_hash?, last_check_hash?
}
```

Goal rounds reconcile from current rows; no idle edge dependency. One round is generic
`automation_run(kind=goal, owner_key=<agent>/<mode>, owner_revision=<goal revision>)`:

1. capture generation/revision/pending/unknown/native-goal state;
2. skip on request/unknown; on-idle also waits active native goal;
3. marker check is read-only; argv check is journaled process action;
4. unknown check blocks and is never rerun;
5. pass → done; exact budget exhausted → blocked;
6. progress = changed complete check or complete workspace fingerprint;
7. re-read generation/revision and admit one exact derived send if unchanged.

Turn budget increments only when goal-linked input associates to a turn. Unknown does
not silently consume/refund; it blocks. Stable run identity prevents duplicate round.

## Level reconciliation

Current-state reconciliation also covers unread mail, created launches, queued resumes,
provider holds/probes and active goals. Lost wakeup never authorizes repeating uncertain
effect.

## Storage

```sql
CREATE TABLE rule_state (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  revision TEXT NOT NULL,
  state TEXT NOT NULL,                 -- active|retiring|retired|paused|disabled|replay
  event_cursor INTEGER NOT NULL,
  retire_at_seq INTEGER,
  paused_reason TEXT,
  skipped_from INTEGER,
  skipped_through INTEGER,
  loaded_at INTEGER NOT NULL,
  PRIMARY KEY(principal,rule,revision),
  CHECK((state='retiring' AND retire_at_seq IS NOT NULL) OR state<>'retiring')
);

CREATE TABLE automation_runs (
  id TEXT PRIMARY KEY,
  principal TEXT NOT NULL,
  kind TEXT NOT NULL,
  owner_key TEXT NOT NULL,
  owner_revision TEXT NOT NULL,
  trigger_key TEXT NOT NULL,
  trigger_seq INTEGER,
  state TEXT NOT NULL,                 -- queued|running|applied|failed|unknown|cancelled
  outcome TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(principal,kind,owner_key,owner_revision,trigger_key)
);
CREATE INDEX automation_runs_rate
  ON automation_runs(principal,owner_key,created_at);

CREATE TABLE automation_actions (
  run TEXT NOT NULL REFERENCES automation_runs(id),
  idx INTEGER NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  state TEXT NOT NULL,
  ref TEXT, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY(run,idx)
);
CREATE INDEX automation_actions_open ON automation_actions(state)
  WHERE state IN ('queued','dispatched','unknown');
CREATE INDEX automation_actions_rate ON automation_actions(created_at);

CREATE TABLE timers (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  schedule_revision TEXT NOT NULL,
  action_revision TEXT NOT NULL,
  state TEXT NOT NULL,
  boundary_at INTEGER,
  last_key TEXT, next_due_at INTEGER,
  PRIMARY KEY(principal,rule,schedule_revision)
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
  PRIMARY KEY(agent,mode)
);
```

Terminal history prunes only when trigger is irrevocably behind relevant cursor/key and
no open/unknown action references it. Open/unknown runs/actions and unresolved hook
proposals never age-prune.

## Guardrails

Cause depth, principal scope, self-trigger protection, bounded payloads and router
validation apply. Rate limits are computed from durable recent `automation_runs` and
`automation_actions` rows (or equivalent durable buckets), not volatile counters that
reset after crash. Draining admits no new run/goal. No guardrail calls a model.
