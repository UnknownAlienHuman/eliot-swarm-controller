# Automation

Automation turns committed events/time into ordinary commands or bounded local actions.
It has no Task/review workflow model. Durable state exists only to avoid missed
triggers, retargeting and blind replay.

## Rule scope and files

- `<data_dir>/rules/*.toml`: global rule, owner authority.
- `<registered project>/.eliot/rules/*.toml`: project rule, limited to that project.

Project rules may command/message project Agents, use project/tree vars, run project
scripts and act on that repository. They cannot target another project, global vars,
arbitrary cwd, plugin/service/account config or undeclared webhook. Project event rules
implicitly require stored `subject.project` to equal the registered project.

A managed script receives a scoped read-only automation credential. Clearing/omitting it
never becomes owner because every connected caller must present an explicit credential.
Mutations are returned as stdout actions and admitted later by daemon under the parent
rule principal.

Required keys are `id`, `on`, `do`; optional keys are `enabled`, `limit`, `catch_up`,
`allow_self`. Canonical definition is bounded, contains named secret references only and
has stable `definition_digest`.

### Definition digest, activation identity and immutable artifacts

A watcher event whose canonical definition, enabled state and principal equal the
current activation is a no-op. Mtime or identical atomic replace creates no activation.

A semantic install/edit/re-enable creates a fresh immutable `activation_id`, even A→B→A.
It derives from principal, rule id, previous activation, handoff boundary and definition
digest. Reverting never reuses/reset an old cursor.

Compilation resolves every rule-owned script/system-note/template under its allowed
root, rejects path escape, hashes bytes and copies bounded content into private immutable
artifact storage. Persisted definition references artifact digests and configured
executable/argv, not mutable paths. Secret values are never copied.

Store persists trigger, matchers, action templates, limits/scope and artifact digests for
every `active|retiring|replay` activation. Old backlog after restart uses its old
persisted definition. Parse/validation/snapshot failure creates no activation.

## Event-rule activation handoff

A new rule id starts at current event high-water H. Editing enabled rule uses one
transaction:

1. capture H;
2. old activation → `retiring`, `retire_at_seq=H`;
3. fresh activation → `active`, `event_cursor=H`.

Old evaluates `(old_cursor,H]`, new `(H,current]`; no overlap/gap. Crash resumes both
persisted definitions. Old retires after cursor H and all runs settle.

Disable/delete/global pause captures H, records skipped `(cursor,H]`, advances cursor and
marks disabled/paused. Re-enable creates a fresh from-now activation. Explicit catch-up
creates a replay activation over a still-retained skipped range.

## Event cursor and deterministic failure

Rules reconcile persisted log by bounded pages. For each event + stored post-transition
subject:

- no match → cursor advances;
- valid match → immutable run/actions + cursor commit together;
- existing trigger key → read run + advance;
- deterministic firing error → failed run/no actions + bounded outcome + `rule.failed`
  + cursor advance;
- Store failure → rollback including cursor; retry only after recovery.

Deterministic errors include missing placeholder, invalid action, secret payload, missing
target, selector overflow, authority violation, unsupported action and missing/mismatched
artifact. Capacity/provider hold/busy target is valid queued work, not render failure.

Retention deletes only through the minimum cursor of active/retiring/replay activations.
On backlog limit one transaction records pause/skipped range, advances boundary and
releases retention. Catch-up requires complete retained range or `history_gap`.

## Timers and activation boundaries

Interval key is anchored slot; cron/once key is scheduled UTC instant; manual key is
external WriteId. DST uses declared timezone, identity UTC. Missed behavior is
`latest|none`, never burst. Due rows reconcile on startup.

Timer references immutable rule activation/definition. No-op reload creates no schedule.
Semantic reload captures wall-clock T and reconciles old activation through T. Changed
trigger creates new anchor strictly after T. Action-only edit creates new activation:
slots through T use old actions, later slots new. One schedule does not stack while prior
run is queued/running/unknown.

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

For rules/timers/manual, owner activation is rule activation id. For Goal it is Goal
revision. Run id is a fixed derived id of the full identity.

One transaction verifies uniqueness, reads trigger snapshot, renders persisted
definition/artifacts, resolves selectors within hard limit, captures exact generation,
binding revision, turn/request, writes per-target actions, advances cursor/timer and
emits state. Replacement generation/binding is stale, never retargeted.

A core action gets derived command id from `(run,index,target)` and no external receipt.
Multi-target order is AgentId; overflow fails firing with no partial rows.

## Action states

```text
queued → dispatched → applied | failed | unknown
queued → cancelled
unknown → applied | failed       // exact readback or owner assertion
```

Queued actions resume after restart. Dispatched becomes unknown. Unknown never reruns.
Actions execute in order; failed/unknown/cancelled stops later. Store-local writes use
stable ids; external writers need exact readback or remain unknown.

## Matching and rendering

Match exact/glob/regex/one-of/presence in stored `{event,subject,data}`; never join a
later Agent row. Placeholders fill values only—not executable, cwd root, env name,
action kind, credential, authority or scope. Invalid value produces one failed firing
and cursor advance. Omitted event target is exact triggering Agent/generation/binding.
Delayed control requires exact binding, turn and request.

## Actions

| Action | Boundary |
| --- | --- |
| core verb | derived command id + frozen generation/binding/turn/request under principal |
| Git fetch/pull/push/worktree | explicit repo/remote/ref/OID/path; no force; exact readback |
| mail/var | concrete recipient/scope under principal |
| script | immutable artifact + frozen interpreter/argv/cwd; scoped env; bounded streams; no rerun |
| log/desktop | bounded local result |
| webhook | named endpoint; project allowlist; no raw URL/secret |
| emit | bounded `x.user.*` in principal scope |

### External action readback

Every process action records pre-state, intended state and custody. Readback begins only
after custody is empty.

- fetch: compare explicit requested tracking refs with exact remote OIDs; ambiguity is
  unknown, never refetch to settle;
- pull: fast-forward-only; exact intended upstream OID and no alternate-writer ambiguity;
- push: exact local OID at exact remote ref; different OID unknown, never push again;
- worktree: exact path/branch/frozen base/HEAD readback;
- webhook: action id as configured idempotency key; no automatic retry; exact status API
  required for applied readback;
- generic script/process: no generic readback; lost outcome unknown.

Repository has a narrow writer hold while an older write is unknown.

### Script environment and output

Managed script runs persisted artifact, not mutable path, with minimal environment.
Named refs validate before lookup/start: global uses global automation allowlist;
project uses global∩project. Returned actions cannot introduce refs. Secrets are memory
only and masked.

stdout action objects are fully buffered to configured bound, parsed, principal-checked
and appended atomically after exit 0; only then dispatch. Lost script is unknown/no rerun.

## Approval rules and synchronous hooks

Asynchronous request actions are ordinary frozen actions while exact request remains
pending. A synchronous hook is not a queued action:

1. stable native hook/request id + exact generation/binding/turn/request + deadline;
2. evaluate bounded persisted rules;
3. validate pending request and insert/reuse non-dispatchable proposal expiring before
   margin;
4. return only after commit acknowledgement while same binding/request remains pending;
5. otherwise safe `ask`; proposal never dispatches later.

Same-id retry before expiry may return proposal. Late commit is expired/audit-only.

```sql
CREATE TABLE hook_decisions (
  id TEXT PRIMARY KEY,
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  binding_revision INTEGER NOT NULL,
  turn_id TEXT,
  request_id TEXT NOT NULL,
  decision TEXT NOT NULL,
  rule TEXT NOT NULL,
  rule_activation TEXT NOT NULL,
  state TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  resolved_at INTEGER,
  FOREIGN KEY(agent,generation,binding_revision)
    REFERENCES agent_bindings(agent,generation,revision)
);
```

## Goal loop

Native and Eliot Goals are bound to exact `(agent,generation,binding_revision)`. Cost is
integer micro-USD and enabled only with exact machine-readable native cost. Generation
or current-binding pointer change pauses both Goal projections with
`generation_changed|binding_changed`. Explicit Goal resume/set creates a new Goal
revision and rebinds to the current control binding after native readback/capability
validation.

```text
Goal {
  agent, generation, binding_revision, mode, text, check?, cadence,
  max_turns, max_cost_microusd?, no_progress,
  status, turns_used, cost_used_microusd?, revision,
  eligibility_seq, next_due_at?,
  last_workspace_hash?, last_check_hash?
}
```

`eligibility_seq` is the persisted event seq of the latest transaction that can change
whether an `on_idle` round may start: Goal revision/status, Agent generation/state,
**current binding pointer**, pending request set, unresolved input/turn/check or
native-Goal activity. The same transaction updates Goal projection. Global
pause/draining is a gate and does not rewrite every Goal or create a new eligibility key.

Trigger identity:

- `on_idle`: `eligible:<eligibility_seq>` only while Agent is idle, current pointer
  equals Goal binding revision, target is directly controllable (or qualified
  parent-mediated continuation exists), no pending/unknown effect and native-Goal policy
  permits;
- `every`: exact anchored slot; if binding changed/ineligible, slot records skipped or
  blocked according to policy and is never replayed as another effect;
- manual: external WriteId.

One round is generic run
`kind=goal, owner_key=<agent>/<mode>, owner_activation=<Goal revision>`:

1. capture generation/binding/Goal revision/eligibility/pending/unknown/native Goal;
2. skip or record blocked when ineligible; on-idle waits active native Goal;
3. marker check is read-only; argv check is immutable journaled process action;
4. unknown check blocks Goal and is never rerun;
5. pass → done; exact budget exhausted → blocked;
6. progress = changed complete check/fingerprint;
7. re-read generation/current binding/Goal revision/eligibility and admit one exact
   derived send only if all unchanged.

Turn budget increments only when linked input associates with a turn. Unknown input/turn
blocks without silent consume/refund. Unique run identity prevents duplicate continuation
after restart; later terminal/binding/eligibility change creates next key.

A routing-disabled native child cannot run Eliot Goal unless plugin exposes qualified
parent-mediated continuation; otherwise Goal set rejects `unsupported`.

## Level reconciliation

Current-state reconciliation covers unread mail, created launches, queued resumes,
provider holds/probes and Goals. A lost wake never authorizes repeating uncertainty.

## Storage

```sql
CREATE TABLE rule_activations (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  definition_digest BLOB NOT NULL CHECK(length(definition_digest)=32),
  definition TEXT NOT NULL,
  state TEXT NOT NULL,
  activated_at INTEGER NOT NULL,
  retired_at INTEGER,
  PRIMARY KEY(principal,rule,activation_id)
);
CREATE INDEX rule_current ON rule_activations(principal,rule,state);
CREATE UNIQUE INDEX rule_one_active ON rule_activations(principal,rule)
  WHERE state='active';

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
CREATE UNIQUE INDEX timer_one_active ON timers(principal,rule)
  WHERE state='active';

CREATE TABLE goals (
  agent INTEGER NOT NULL,
  mode TEXT NOT NULL,
  agent_generation INTEGER NOT NULL,
  binding_revision INTEGER NOT NULL,
  text TEXT NOT NULL,
  check_spec TEXT,
  cadence TEXT NOT NULL,
  status TEXT NOT NULL,
  max_turns INTEGER NOT NULL,
  turns_used INTEGER NOT NULL DEFAULT 0,
  max_cost_microusd INTEGER,
  cost_used_microusd INTEGER,
  no_progress INTEGER NOT NULL,
  stale_turns INTEGER NOT NULL DEFAULT 0,
  eligibility_seq INTEGER NOT NULL DEFAULT 0,
  last_workspace_hash TEXT,
  last_check_hash TEXT,
  revision INTEGER NOT NULL DEFAULT 0,
  next_due_at INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,mode),
  FOREIGN KEY(agent,agent_generation,binding_revision)
    REFERENCES agent_bindings(agent,generation,revision)
);
```

A→B→A produces three activation ids while definitions may share digest. Duplicate
watcher reload creates none. Restart drains old activation from persisted definition/
artifacts. Retired activation/artifact prunes only after interval closes, runs/actions
terminal and replay impossible.

Terminal history prunes only behind cursor/key and with no open references. Open/unknown
runs/actions and unresolved hook proposals never age-prune.

## Guardrails

Cause depth, principal scope, self-trigger, bounded payloads and router validation apply.
Rate limits use durable recent runs/actions or durable buckets, not volatile counters.
Draining admits no new run/Goal. No guardrail calls a model.
