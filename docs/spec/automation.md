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
implicitly require stored `subject.project` to equal registered project.

Script process receives scoped read-only automation token. Clearing/omitting it never
becomes owner because custody classification fails closed. Mutations are returned as
stdout actions and admitted later by daemon under parent rule principal.

Required keys: `id`, `on`, `do`; optional `enabled`, `limit`, `catch_up`, `allow_self`.
Canonical rule definition is bounded, contains named secret references only, and has
stable `definition_digest`.

### Definition digest is not activation identity

Each time a valid rule definition becomes current, Store creates a fresh immutable
`activation_id`, even when bytes revert A→B→A. The id is a fixed derived value over
principal, rule id, previous activation id, handoff boundary and definition digest.
Therefore reverting to A does not reuse/reset the old A cursor or collide with it.

Store persists canonical compiled definition (trigger, matchers, rendered action
templates, limits/scope) for every `active|retiring|replay` activation. File reload is
not needed to evaluate old backlog after restart. Definition may be pruned only when its
activation interval is closed, no run/action references it and replay is impossible.

A parse/validation failure creates no activation and leaves current behavior intact.

## Event-rule activation handoff

A new rule id starts at current event high-water H, no implicit backfill. Restart resumes
its activation/cursor and persisted definition.

Editing enabled event rule uses one transaction:

1. capture H;
2. old activation → `retiring`, `retire_at_seq=H`;
3. create fresh activation for new definition → `active`, `event_cursor=H`.

Old evaluates `(old_cursor,H]`, new `(H,current]`; no overlap/gap. They may drain
concurrently. Crash resumes both definitions from Store. Old becomes retired after
cursor H and admitted runs settle. Backlog policy applies independently.

Disable/delete/global pause instead captures H, records skipped `(cursor,H]`, advances
cursor and marks activation disabled/paused. Re-enable creates fresh activation from-now.
Explicit catch-up creates fresh replay activation over still-retained recorded range.

## Event cursor and deterministic failure

Rules reconcile persisted log by bounded pages. For each event + stored post-transition
subject:

- no match → cursor advances;
- valid match → immutable run/actions + cursor commit together;
- existing trigger key → read run + advance;
- deterministic firing error → failed run/no actions + bounded outcome + `rule.failed`
  + cursor advance;
- Store failure → rollback including cursor; retry only after recovery.

Deterministic errors: missing placeholder, invalid action, secret-bearing payload,
missing target, selector over configured limit, authority violation, unsupported action.
Capacity/provider hold/busy target is valid queued work, not render failure.

Crash before transaction repeats safely; after cannot lose/duplicate admission.
Ephemeral/late diagnostics are ineligible unless explicitly matched by `x.*`.

Retention deletes only through minimum cursor of active/retiring/replay activations. On
backlog limit, one transaction records pause/skipped range, advances interval boundary
and releases retention. Catch-up requires full range or returns `history_gap`.

## Timers and activation boundaries

Interval key is anchored slot; cron/once key UTC instant; manual key external WriteId.
DST uses declared timezone, identity UTC. Missed is latest/none, never burst. Due rows
reconcile on startup.

Timer activation references the immutable rule activation/definition. Reload captures
wall-clock T and reconciles old activation through T. Changed trigger creates new unique
activation/anchor strictly after T. Action-only edit still creates new rule activation:
slots through T use old activation; later slots new. Reverting bytes creates another
activation, never collides. One schedule does not stack while prior run queued/running/
unknown.

## Generic durable run identity

Every event rule, timer, manual run and Goal round uses:

```text
RunIdentity {
  principal,
  kind: event_rule | timer | manual | goal,
  owner_key,
  owner_activation,
  trigger_key
}
```

For rules/timers/manual, `owner_activation` is immutable rule activation id. For Goal it
is goal revision. Run id is derived from this identity.

One transaction verifies uniqueness, reads trigger snapshot, renders using persisted
activation definition, resolves selectors (up to `max_selector_targets`), captures exact
generation/turn/request, writes per-target actions, advances cursor/timer and emits
state. Replacement generation is stale, never retargeted.

Core action gets derived command id from `(run,index,target)` and no external receipt.
Multi-target order is AgentId; overflow fails firing with no partial rows.

## Action states

```text
queued → dispatched → applied | failed | unknown
queued → cancelled
unknown → applied | failed       // exact readback or owner assertion
```

Queued resumes after restart. Dispatched becomes unknown. Unknown never reruns. Actions
execute in order; failed/unknown/cancelled stops later. Store-local writes use stable
ids; external writers need exact readback or remain unknown.

## Matching and rendering

Match exact/glob/regex/one-of/presence in stored `{event,subject,data}`; never join later
Agent. Placeholders fill values only, not executable/cwd root/env name/action kind/
credential/authority/scope. Invalid value produces failed firing and cursor advance.
Omitted event target is exact triggering agent+generation. Delayed control requires
exact turn/request.

## Actions

| Action | Boundary |
| --- | --- |
| core verb | derived command id + frozen binding under principal |
| Git fetch/pull/push/worktree | explicit repo/remote/ref/OID/path; no force; exact readback |
| mail/var | concrete recipient/scope under principal |
| script | fixed executable/argv/project-safe cwd; scoped env; bounded streams; no rerun |
| log/desktop | bounded local result |
| webhook | named endpoint; project allowlist; no raw URL/secret |
| emit | bounded `x.user.*` in principal scope |

### External action readback

Every process action records pre-state, intended state and custody. Readback begins only
after custody empty; otherwise unknown.

- fetch: explicit remote/refspec; compare requested tracking refs with exact remote OIDs;
  movement/ambiguity unknown; never refetch to settle;
- pull: fast-forward-only; exact intended upstream OID and no alternate-writer ambiguity;
- push: exact local OID at exact remote ref; different OID unknown, never another push;
- worktree: exact path/branch/frozen base/HEAD readback;
- webhook: action id as configured idempotency key; no automatic retry; exact status API
  required for applied readback;
- generic script/process: no generic readback; lost outcome unknown.

Repository has narrow writer hold while older push/pull/worktree write unknown.

### Script environment and output

Managed script starts minimal. Named environment refs validate before lookup/start:

global script uses global automation allowlist; project script uses global∩project;
returned actions cannot introduce refs. Resolved values exist in memory only/masked.
stdout actions are fully buffered, parsed, principal-checked and appended atomically
after exit 0; only then dispatch. Lost script is unknown and not rerun.

## Approval rules and synchronous hooks

Asynchronous request actions are ordinary frozen actions while exact request pending.
Synchronous hook is not queued action:

1. stable native hook/request id + exact binding + deadline;
2. evaluate bounded rules using persisted activation definition only;
3. validate pending request and insert/reuse non-dispatchable proposal expiring before
   margin;
4. return only after commit ack before deadline and still pending;
5. otherwise safe `ask`; proposal never dispatches later.

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
  rule_activation TEXT NOT NULL,
  state TEXT NOT NULL,                 -- proposed|returned|expired|resolved
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  resolved_at INTEGER
);
```

## Goal loop

Native/Eliot goals are independent/generation-bound. Cost is integer micro-USD and only
with exact machine-readable cost. Generation change pauses until explicit rebind.

```text
Goal {
  agent, generation, mode, text, check?, cadence,
  max_turns, max_cost_microusd?, no_progress,
  status, turns_used, cost_used_microusd?, revision,
  next_due_at?, last_workspace_hash?, last_check_hash?
}
```

Goal rounds reconcile current rows. One round is generic run
`kind=goal, owner_key=<agent>/<mode>, owner_activation=<goal revision>`:

1. capture generation/revision/pending/unknown/native-goal;
2. skip request/unknown; on-idle waits active native goal;
3. marker read-only; argv check journaled process action;
4. unknown check blocks/no rerun;
5. pass done; exact budget blocked;
6. progress = changed complete check/fingerprint;
7. re-read generation/revision, admit exact derived send.

Turn budget increments only when linked input associates with turn. Unknown blocks, no
silent consume/refund. Stable identity prevents duplicate round.

## Level reconciliation

Current-state reconciliation covers unread mail, created launches, queued resumes,
provider holds/probes and goals. Lost wake never authorizes repeating uncertain effect.

## Storage

```sql
CREATE TABLE rule_activations (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  definition_digest BLOB NOT NULL CHECK(length(definition_digest)=32),
  definition TEXT NOT NULL,             -- bounded canonical compiled definition
  state TEXT NOT NULL,                  -- active|retiring|retired|paused|disabled|replay
  activated_at INTEGER NOT NULL,
  retired_at INTEGER,
  PRIMARY KEY(principal,rule,activation_id)
);
CREATE INDEX rule_current ON rule_activations(principal,rule,state);

CREATE TABLE event_rule_state (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  event_cursor INTEGER NOT NULL,
  retire_at_seq INTEGER,
  paused_reason TEXT,
  skipped_from INTEGER,
  skipped_through INTEGER,
  PRIMARY KEY(principal,rule,activation_id),
  FOREIGN KEY(principal,rule,activation_id)
    REFERENCES rule_activations(principal,rule,activation_id),
  CHECK((retire_at_seq IS NULL) OR retire_at_seq >= event_cursor)
);

CREATE TABLE automation_runs (
  id TEXT PRIMARY KEY,
  principal TEXT NOT NULL,
  kind TEXT NOT NULL,
  owner_key TEXT NOT NULL,
  owner_activation TEXT NOT NULL,
  trigger_key TEXT NOT NULL,
  trigger_seq INTEGER,
  state TEXT NOT NULL,
  outcome TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(principal,kind,owner_key,owner_activation,trigger_key)
);
CREATE INDEX automation_runs_rate ON automation_runs(principal,owner_key,created_at);

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
  activation_id TEXT NOT NULL,
  schedule_digest BLOB NOT NULL CHECK(length(schedule_digest)=32),
  state TEXT NOT NULL,
  boundary_at INTEGER,
  last_key TEXT, next_due_at INTEGER,
  PRIMARY KEY(principal,rule,activation_id),
  FOREIGN KEY(principal,rule,activation_id)
    REFERENCES rule_activations(principal,rule,activation_id)
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

A-B-A produces three activation ids while definitions A may share digest. Restart can
drain old activation because definition is persisted. Retired definition prunes only
after interval closed, runs/actions terminal and replay impossible.

Terminal history prunes only behind cursor/key and with no open references. Open/
unknown runs/actions and unresolved hook proposals never age-prune.

## Guardrails

Cause depth, principal scope, self-trigger, bounded payloads and router validation apply.
Rate limits use durable recent runs/actions or durable buckets, not volatile counters
reset by crash. Draining admits no new run/goal. No guardrail calls a model.
