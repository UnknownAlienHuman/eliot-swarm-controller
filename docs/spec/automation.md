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

Required rule keys are `id`, `on`, `do`; optional keys are `enabled`, `limit`,
`catch_up`, `allow_self`. Canonical rule definition is bounded, contains named secret
references only and has a stable `definition_digest`.

### Definition digest, activation identity and immutable artifacts

A watcher event whose canonical definition, enabled state and principal scope equal the
current activation is a no-op. File mtime, duplicate watcher notification or atomic
replace with identical semantic bytes creates no activation/timer boundary.

A semantic install/edit/re-enable creates a fresh immutable `activation_id`, even when
bytes revert A→B→A. The id is derived from principal, rule id, previous activation id,
handoff boundary and definition digest. Reverting to A never reuses/reset the old A
cursor.

Compilation resolves every rule-owned script/system-note/template under its allowed
root, rejects symlink/device/alternate-stream escape, hashes bytes and copies bounded
content into the private content-addressed artifact store. Persisted compiled definition
references artifact digests, configured interpreter/executable and argv—not mutable
source paths. Secret values are never copied; named references remain.

Store persists trigger, matchers, action templates, limits/scope and artifact digests for
every `active|retiring|replay` activation. Old backlog after restart therefore runs its
old definition and exact script snapshot. Artifacts prune only when no activation/run/
action/workspace binding references them. Parse/validation/snapshot failure creates no
activation and leaves current behavior.

## Event-rule activation handoff

A new rule id starts at current event high-water H, no implicit backfill. Restart resumes
its activation/cursor and persisted definition.

Editing an enabled event rule uses one transaction:

1. capture H;
2. old activation → `retiring`, `retire_at_seq=H`;
3. fresh activation → `active`, `event_cursor=H`.

Old evaluates `(old_cursor,H]`, new `(H,current]`; no overlap/gap. They may drain
concurrently. Crash resumes both definitions. Old becomes retired after cursor H and all
admitted runs settle. Backlog policy applies independently.

Disable/delete/global pause captures H, records skipped `(cursor,H]`, advances cursor and
marks activation disabled/paused. Re-enable creates a fresh from-now activation.
Explicit catch-up creates a fresh replay activation over the still-retained recorded
range.

## Event cursor and deterministic failure

Rules reconcile persisted log by bounded pages. For each event + stored
post-transition subject:

- no match → cursor advances;
- valid match → immutable run/actions + cursor commit together;
- existing trigger key → read run + advance;
- deterministic firing error → failed run/no actions + bounded outcome + `rule.failed`
  + cursor advance;
- Store failure → rollback including cursor; retry only after recovery.

Deterministic errors include missing placeholder, invalid action, secret-bearing payload,
missing target, selector over limit, authority violation, unsupported action and missing/
mismatched immutable artifact. Capacity/provider hold/busy target is valid queued work,
not a render failure.

Crash before transaction repeats safely; after commit cannot lose/duplicate admission.
Ephemeral/late diagnostics are ineligible unless explicitly matched by `x.*`.

Retention deletes only through the minimum cursor of active/retiring/replay activations.
On backlog limit, one transaction records pause/skipped range, advances interval boundary
and releases retention. Catch-up requires the complete range or returns `history_gap`.

## Timers and activation boundaries

Interval key is anchored slot; cron/once key is scheduled UTC instant; manual key is the
external WriteId. DST uses declared timezone, identity UTC. Missed behavior is
`latest|none`, never burst. Due rows reconcile on startup.

Timer activation references immutable rule activation/definition. A no-op reload creates
no schedule row. A semantic reload captures wall-clock T and reconciles old activation
through T. Changed trigger creates a new anchor strictly after T. Action-only edit also
creates a new rule activation: slots through T use old actions, later slots new. One
schedule does not stack while its prior run is queued/running/unknown.

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

For rules/timers/manual, `owner_activation` is the immutable rule activation id. For a
Goal it is the Goal revision. Run id is a fixed derived id of the full identity.

One transaction verifies uniqueness, reads the trigger snapshot, renders using persisted
definition/artifacts, resolves selectors up to `max_selector_targets`, captures exact
generation/turn/request, writes per-target actions, advances cursor/timer and emits
state. Replacement generation is stale, never retargeted.

A core action gets derived command id from `(run,index,target)` and no external receipt.
Multi-target order is AgentId; overflow fails the firing with no partial rows.

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
and cursor advance. Omitted event target is the exact triggering Agent/generation.
Delayed control requires exact turn/request.

## Actions

| Action | Boundary |
| --- | --- |
| core verb | derived command id + frozen binding under principal |
| Git fetch/pull/push/worktree | explicit repo/remote/ref/OID/path; no force; exact readback |
| mail/var | concrete recipient/scope under principal |
| script | immutable artifact + frozen interpreter/argv/cwd; scoped env; bounded streams; no rerun |
| log/desktop | bounded local result |
| webhook | named endpoint; project allowlist; no raw URL/secret |
| emit | bounded `x.user.*` in principal scope |

### External action readback

Every process action records pre-state, intended state and custody. Readback begins only
after custody is empty; otherwise unknown.

- fetch: explicit remote/refspec; compare requested tracking refs with exact remote OIDs;
  movement/ambiguity unknown; never refetch to settle;
- pull: fast-forward-only; exact intended upstream OID and no alternate-writer ambiguity;
- push: exact local OID at exact remote ref; different OID unknown, never another push;
- worktree: exact path/branch/frozen base/HEAD readback;
- webhook: action id as configured idempotency key; no automatic retry; exact status API
  required for applied readback;
- generic script/process: no generic readback; lost outcome unknown.

Repository has a narrow writer hold while an older push/pull/worktree write is unknown.

### Script environment and output

Managed script runs the persisted artifact, not a mutable checkout path, and starts with
minimal environment. Named environment refs validate before lookup/start: global script
uses global automation allowlist; project script uses global∩project. Returned actions
cannot introduce refs. Resolved values exist in memory only and are masked.

stdout action objects are fully buffered up to the configured bound, parsed,
principal-checked and appended atomically after exit 0; only then dispatch. Lost script
is unknown and never rerun.

## Approval rules and synchronous hooks

Asynchronous request actions are ordinary frozen actions while the exact request remains
pending. A synchronous hook is not a queued action:

1. stable native hook/request id + exact binding + deadline;
2. evaluate bounded rules from persisted activation definitions;
3. validate pending request and insert/reuse a non-dispatchable proposal expiring before
   margin;
4. return only after commit acknowledgement before deadline and while still pending;
5. otherwise return safe `ask`; proposal never dispatches later.

Same-id retry before expiry may return the proposal. Late commit is expired/audit-only.

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
  state TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  resolved_at INTEGER
);
```

## Goal loop

Native and Eliot goals are independent and generation-bound. Cost is integer micro-USD
and only enabled with exact machine-readable native cost. Generation change pauses until
explicit rebind.

```text
Goal {
  agent, generation, mode, text, check?, cadence,
  max_turns, max_cost_microusd?, no_progress,
  status, turns_used, cost_used_microusd?, revision,
  eligibility_seq, next_due_at?,
  last_workspace_hash?, last_check_hash?
}
```

`eligibility_seq` is the persisted event sequence of the latest transaction that can
change whether an `on_idle` round may start: Goal revision/status, Agent generation/state,
pending request set, unresolved input/turn/check, native-goal activity or admission
mode. The same transaction updates the Goal projection. Therefore restart sees the same
key, while a real eligible-state change gets a new key.

Trigger identity:

- `on_idle`: `trigger_key="eligible:<eligibility_seq>"` only while Agent is idle,
  directly controllable, no pending request/unknown effect and native-goal policy permits;
- `every`: `trigger_key="slot:<anchored-slot>"`; a blocked slot is recorded skipped or
  blocked according to Goal policy and is never retried as another effect;
- manual: external WriteId.

One round is generic run
`kind=goal, owner_key=<agent>/<mode>, owner_activation=<goal revision>`:

1. capture generation/revision/eligibility/pending/unknown/native-goal state;
2. skip or record blocked when ineligible; `on_idle` waits active native goal;
3. marker check is read-only; argv check uses an immutable journaled process action;
4. unknown check blocks the Goal and is never rerun automatically;
5. pass → done; exact budget exhausted → blocked;
6. progress = changed complete check/fingerprint;
7. re-read generation/revision/eligibility and admit one exact derived send only if the
   trigger is still valid.

Turn budget increments only when linked input associates with a turn. Unknown input/
turn blocks without silent consume/refund. The unique run identity prevents duplicate
continuation after restart; a later terminal turn/other eligibility change creates the
next eligible sequence.

A routing-disabled native child cannot run an Eliot Goal unless its plugin exposes a
qualified parent-mediated continuation mechanism; otherwise Goal set rejects
`unsupported` rather than queuing an unusable direct send.

## Level reconciliation

Current-state reconciliation covers unread mail, created launches, queued resumes,
provider holds/probes and Goals. A lost wake never authorizes repeating an uncertain
effect.

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
  agent INTEGER NOT NULL, mode TEXT NOT NULL,
  agent_generation INTEGER NOT NULL,
  text TEXT NOT NULL, check_spec TEXT, cadence TEXT NOT NULL,
  status TEXT NOT NULL,
  max_turns INTEGER NOT NULL, turns_used INTEGER NOT NULL DEFAULT 0,
  max_cost_microusd INTEGER, cost_used_microusd INTEGER,
  no_progress INTEGER NOT NULL, stale_turns INTEGER NOT NULL DEFAULT 0,
  eligibility_seq INTEGER NOT NULL DEFAULT 0,
  last_workspace_hash TEXT, last_check_hash TEXT,
  revision INTEGER NOT NULL DEFAULT 0,
  next_due_at INTEGER, updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,mode)
);
```

A→B→A produces three activation ids while definitions may share a digest. Duplicate
watcher reload of current A produces none. Restart drains old activation from persisted
definition/artifacts. Retired activation/artifact prunes only after interval closes,
runs/actions are terminal and replay is impossible.

Terminal history prunes only behind cursor/key and with no open references. Open/unknown
runs/actions and unresolved hook proposals never age-prune.

## Guardrails

Cause depth, principal scope, self-trigger, bounded payloads and router validation apply.
Rate limits use durable recent runs/actions or durable buckets, not volatile counters
reset by crash. Draining admits no new run/Goal. No guardrail calls a model.
