# Configuration — agent policy and one automation loop

This document defines the target manager-facing policy. Governor and Manager are agent
roles. A configured action runs through the same Store command as a direct agent call;
it is not a request for a human to perform the step.

Current source still contains `WorkDispatch`, `ReviewDisposition`, `RepairDispatch`,
publication/GitHub-projection workflows, owner-dependent relocation and overlapping
schedulers. Their removal is owned by
[implementation](implementation.md#automation-and-delivery). This target is not an
implementation claim.

## 1. One entry, one owner

A policy entry has one stable internal identity:

```text
entry_id                 immutable
name                     manager-facing
owner_manager_id
project_id
revision
enabled
trigger                   event | interval | cron
selected actions/settings
```

The owning Manager agent configures it once. Enabled entries act on its behalf after
rechecking current rights and object state. They do not mint a Manager credential,
create a second capability grant or require per-occurrence approval.

Disabling prevents future admissions. It does not cancel an admitted Operation,
native turn, review, check, message or external write. Transfer updates owner and
revision of the same entry; it does not clone entries or relocate journals.

## 2. Editing surface

```text
automation.config.get
automation.config.preview       optional validation/diff
automation.config.apply
automation.config.explain
automation.config.transfer
```

- `get` returns scoped entries, revision and current blockers.
- `preview` is read-only and grants nothing.
- `apply` is one expected-revision transaction.
- `explain` reads current facts without provider/model calls.
- `transfer` preserves `entry_id` and admitted history.

Create uses `expected_revision=0`; updates require the current revision. Omitted fields
stay unchanged, supplied arrays replace the array, unknown fields reject, and an
identical request retry returns its original Operation. Remove caller-supplied preview
hashes.

## 3. Retained actions

The target has a small closed action set:

```text
dispatch
review
return_changes
accept
check
goal_message
script
```

Optional integrations may additionally select:

```text
source_import
publish
```

These names describe current-state actions, not workflow types.

| Action | Positive path |
| --- | --- |
| `dispatch` | Query the current ranked queue and admit at most the configured work budget. |
| `review` | Assign a missing required review slot for the current candidate. |
| `return_changes` | Apply current actionable findings with `task.request_changes` and admit one correction `Send` when the owner is live. |
| `accept` | Call the common candidate-specific acceptance gate when frozen review/check evidence is ready. |
| `check` | Admit an exact candidate/profile CheckRun. |
| `goal_message` | Admit an ordinary scheduled/event `agent.send`; native Harness Goal remains separate. |
| `script` | Run one bounded script revision whose returned typed actions use ordinary Store authorization. |
| `source_import` | Select current external source facts and create/revise a Task in a separate mutation. |
| `publish` | Admit the selected accepted-candidate remote effect through its one effect owner. |

Delete target equivalents of `review_disposition`, `repair_dispatch`, automatic Issue
label projection and publication state machines. Their useful behavior is folded into
`return_changes` or the optional `publish` action; it is not transferred to a human.

## 4. Trigger and loop

A retained entry has one trigger:

```text
event { source_id, event_kind, closed typed filter? }
interval { anchor_ms, period_ms }
cron { expression, timezone, catch_up = latest_only }
```

A trigger or Store change is a wake hint. One in-process host loop waits for the
nearest due time, Store changes and a slow safety wake, then queries current state.
Store atomically claims a due occurrence and admits ordinary Operations. Replaying the
same occurrence returns its existing Operation.

Do not create a process, credential, poller, cursor family or private pending queue per
action/entry. A lost wake causes a bounded rescan. Downtime coalesces missed interval
or cron ticks according to the selected policy rather than replaying a burst of stale
model prompts.

## 5. Route, session and permission policy

A dispatch policy selects an ordered list of route aliases and an optional agent
profile:

```json
{
  "enabled": true,
  "trigger": {"kind":"event","source_id":"controller","event_kind":"task.changed"},
  "actions": ["dispatch", "review", "return_changes", "accept"],
  "dispatch": {
    "routes": ["codex-primary", "muse-fallback"],
    "agent_profile": "implementation",
    "reuse_compatible_session": true
  }
}
```

Each route owns exact harness, account/service, model, effort, native options,
credential reference, required semantic capabilities and root limit. The ordered
aliases are the complete fallback policy. Eliot does not silently change provider,
model, effort, permissions or workspace policy.

Automatic selection checks:

1. current route/configuration and required capability;
2. current Task/Attempt and write lease;
3. root capacity;
4. fresh provider availability/usage observation;
5. unresolved native Goal, children, pending requests and effects on a reusable
   binding.

No provider fact permits one bounded first probe. A current hard negative uses the
next configured alias or holds work. Rate limit/reset permits a bounded probe after its
native retry/reset boundary. Stale/gapped facts hold automatic launch while the
Governor may issue an attributable probe under policy; this is not a human-only path.

Reuse a compatible idle session when selected policy permits. Rotation drains the
specific session after current input, children, Goal and uncertain effects are
disposed. Age or low CPU alone never kills or rotates work.

Use the harness's native permission/sandbox/policy controls. A known authorized tool
request is handled by policy. An unresolved form or judgment request is addressed to
the Governor agent with exact request identity and current options. `Send` never
pretends to be `Reply`. Operator intervention is reserved for missing credentials,
missing authority or owner intent not encoded in policy.

## 6. Current-state action semantics

### Dispatch

```text
wake
-> bounded ranked queue
-> first eligible Task + configured route
-> atomic Attempt/workspace/root reservation
-> one launch/open/dispatch Operation chain
```

Rank corrections, unfinished Attempts and explicit project priority before new work.
Filter eligibility before limiting or continue a bounded keyset scan so blocked first
rows cannot starve later work. Reuse the same Attempt/Operation on retry.

Delete WorkDispatch global/per-entry cursors, copied pending subjects, activation
cuts, catch-up watermarks, semantic launch slots, duplicate operation links and
ordinary-blocker quarantine.

### Review and correction

`review` selects one current candidate/slot and configured reviewer profile.
`review.submit` retains verdict, all findings and requirement-linked evidence.

`return_changes` defaults to the complete current actionable findings set. A Governor
may dismiss or defer a disputed finding only through an attributable candidate/review
decision with reason; no human step is implied. The action performs one
`task.request_changes` transition and one child correction `Send` if the current owner
is ready. Otherwise `needs_correction` stays visible for the next resume/dispatch.

Preserve existing reviewer replacement for Unanswered, Inconclusive and
ReturnedForCorrection. Bound unchanged rounds; after the bound, admit one Governor
decision task rather than looping or stopping the entire queue.

### Acceptance

`accept` reads the frozen per-requirement evidence plan, current candidate-specific
reviews and exact CheckRuns. It calls the same acceptance gate as a direct Governor
request. The entry never fabricates review text or evidence. Candidate B cannot reuse
A's review.

### Goal message

Harness Goal is native adapter state. `goal_message` is an independent ordinary
message action:

```text
selected event/due occurrence
-> current Goal/assignment
-> one agent.send Operation
```

A later occurrence may intentionally repeat text. Disabling this action does not pause
or clear native Goal.

## 7. Script policy

A script revision receives only configured closed grants. Current common helpers
should be preferred for dispatch, review, correction, acceptance, Goal messages and
checks. A retained script may return only the typed actions it genuinely needs, such
as exact `Send`, `Reply`, `Configure` or `Goal`, and every returned item is admitted as
an ordinary child Operation under current Manager/object rights.

Do not add arbitrary Store method passthrough, a second permission registry or a
workflow language. Script execution identity is `(ScriptRun, action index)`, so lost
responses do not re-run already admitted effects.

## 8. Transfer and disable

Transfer changes owner/revision of the same `entry_id`. Due occurrences and child
Operations keep their original admitting identity. Future actions recheck current
owner rights.

Remove disabled former-owner copies, successor chains, ancestry walks and domain
journal relocators. A→B→A transfer must preserve one entry rather than collide with
historical copies.

## 9. Storage and explanation

Use one indexed automation-entry representation for identity, owner, project,
revision, enabled state, trigger/next due and selected settings. Store occurrence →
Operation identity only where recurrence requires it. Bounded settings may remain JSON
inside the row.

Do not persist queue items, provider state, candidate lifecycle or action-specific
workflow copies. Those remain in their owning domains.

`automation.config.explain` returns current blockers and next possible action:

- no eligible Task;
- overlapping lease;
- route capability or provider/capacity hold;
- pending native request/effect;
- missing current review/check evidence;
- disabled or stale policy.

Explanations grant nothing and invoke no provider/model.

## 10. Current source removal map

Current `AutomationStep` still contains:

```text
work_dispatch
review_dispatch
review_disposition
repair_dispatch
acceptance
publication
check_run
github_projection
goal_progression
script_run
```

Current source also retains per-domain cursors, activation state, operation links,
quarantine, transfer relocation, standalone scheduler protocol and legacy worker
demand. Migrate one connected action at a time; once callers use the target action,
remove its old enum value, schemas, policy, Store state, fixtures and worker demand in
the same slice.

## 11. Verification

- enabling a policy can run assignment → result → review → all-findings correction →
  fresh review → acceptance without a routine human step;
- same trigger occurrence admits once; lost wake rescans current state;
- blocked first-page Tasks do not hide eligible work;
- only configured route aliases/fallbacks are used;
- native permission policy answers known requests and exact unresolved requests reach
  Governor without default denial/first-choice behavior;
- correction returns all current actionable findings once;
- candidate-specific requirement evidence is required for acceptance;
- disabling stops future admissions but preserves in-flight work/results;
- transfer preserves entry/occurrence identity;
- no separate scheduler process, WorkDispatch queue, repair workflow or projection
  workflow remains after its migration.
