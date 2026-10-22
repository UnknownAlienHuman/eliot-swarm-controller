# Configuration — agent policy and one automation loop

This document defines the target manager-facing policy. Governor and Manager are agent
roles. A configured action runs through the same Store command as a direct agent call;
it is not a request for a human to perform the step.

Current source still contains `WorkDispatch`, `ReviewDisposition`, `RepairDispatch`,
publication/GitHub-projection workflows, owner-dependent relocation, vendor-branched
Goal progression and overlapping schedulers. Their removal is owned by
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
native turn, Goal, review, check, message or external write. Transfer updates owner
and revision of the same entry; it does not clone entries or relocate journals.

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

Create uses `expected_revision=0`; updates require current revision. Omitted fields
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

| Action | Positive path |
| --- | --- |
| `dispatch` | Query current ranked work and admit within the configured budget. |
| `review` | Assign a missing required review slot for the current candidate. |
| `return_changes` | Apply all current findings and admit one correction Send when the owner is live. |
| `accept` | Call the common candidate-specific acceptance gate when frozen review/check evidence is ready. |
| `check` | Admit an exact candidate/profile CheckRun. |
| `goal_message` | Admit an ordinary bounded scheduled/event `agent.send`; native Harness Goal remains separate. |
| `script` | Run one bounded script revision; returned typed actions use ordinary Store authorization. |
| `source_import` | Select current external facts and create/revise a Task separately from collection. |
| `publish` | Admit the selected accepted-candidate remote effect through its one effect owner. |

Delete target equivalents of `review_disposition`, `repair_dispatch`, automatic Issue
label projection and publication state machines. Their useful behavior is folded into
`return_changes` or optional `publish`; it is not transferred to a human.

## 4. Trigger and loop

A retained entry has one trigger:

```text
event { source_id, event_kind, closed typed filter? }
interval { anchor_ms, period_ms }
cron { expression, timezone, catch_up = latest_only }
```

A trigger or Store change is a wake hint. One in-process host loop waits for nearest
due time, Store changes and a slow safety wake, then queries current state. Store
atomically claims an occurrence and admits ordinary Operations. Replaying the same
occurrence returns its existing Operation.

Do not create a process, credential, poller, cursor family or private pending queue per
action/entry. A lost wake causes a bounded rescan. Downtime coalesces missed ticks
according to selected policy rather than replaying stale model prompts.

## 5. Route, dependency, session and permission policy

A dispatch policy selects ordered route aliases, optional agent profile and dependency
mode:

```json
{
  "enabled": true,
  "trigger": {"kind":"event","source_id":"controller","event_kind":"task.changed"},
  "actions": ["dispatch", "review", "return_changes", "accept"],
  "dispatch": {
    "routes": ["codex-primary", "muse-fallback"],
    "agent_profile": "implementation",
    "dependency_mode": "hold",
    "reuse_compatible_session": true,
    "continuation": {
      "max_no_progress_rounds": 3,
      "max_elapsed_ms": null,
      "max_native_turns": null,
      "rotate_on_context_pressure": true
    }
  }
}
```

Each route owns exact harness, account/service, model, effort, native options,
credential reference, required semantic capabilities and root limit. Ordered aliases
are the complete fallback policy. Eliot never silently changes provider, model, effort,
permissions or workspace policy.

Dependency modes:

- `hold`: unresolved required dependencies keep automatic dispatch pending;
- `allow_preparatory`: claim/execute is allowed and unresolved dependencies are shown
  in TaskPacket, but acceptance/publication still requires exact current dependency
  acceptance receipts.

This removes the current unconditional claim gate without pretending dependencies are
satisfied. Manager/Governor may explicitly select preparatory work; ordinary automatic
policy defaults to `hold`. No second dependency workflow or Task type is created.

Automatic selection checks route/config/capability, Task/Attempt/write lease, root
capacity, provider facts and unresolved Goal/children/requests/effects on a reusable
binding.

No provider fact permits one bounded first probe. A current hard negative uses the
next configured alias or holds work. Rate limit/reset permits a bounded probe after its
native retry/reset boundary. Stale/gapped facts hold automatic launch while Governor
may issue an attributable probe under policy; this is not a human-only path.

Reuse a compatible session when policy permits. Session continuation/rotation uses
selected budgets and observable facts, never age alone:

- accepted result, needs-correction, pending question/permission or uncertain effect
  stops ordinary continuation;
- candidate/result/check/finding changes are meaningful progress;
- fresh IDs, repeated terminal turns or unrelated edits are not progress;
- native context pressure, explicit turn/elapsed/cost budget or repeated no-progress
  may request rotation;
- rotation first stops new input, drains Goal/children/questions/effects, retains
  checkpoint/result and then opens/resumes a replacement under a new admitted action.

Do not kill or rotate because CPU is low, a timestamp is old or a client disconnected.

Use native permission/sandbox/policy controls. Known authorized tool requests are
handled by policy. An unresolved form/judgment request is addressed to Governor with
exact request identity/options. `Send` never pretends to be `Reply`. Operator input is
reserved for missing credentials/authority or owner intent not encoded in policy.

## 6. Current-state action semantics

### Dispatch

```text
wake
-> bounded ranked queue
-> first eligible Task + configured route/dependency policy
-> atomic Attempt/workspace/root reservation
-> one launch/open/dispatch Operation chain
```

Rank corrections, unfinished Attempts and explicit project priority before new work.
Filter eligibility before limiting or use bounded keyset scan so blocked first rows
cannot starve later work. Reuse the same Attempt/Operation on retry.

Delete WorkDispatch global/per-entry cursors, pending copies, activation/catch-up
state, semantic launch slots, duplicate links and ordinary-blocker quarantine.

### Review and correction

`review` selects one current candidate/slot and reviewer profile. `review.submit`
retains verdict, all findings and requirement-linked evidence.

`return_changes` defaults to all current actionable findings. Governor may dismiss or
defer a disputed finding only by an attributable candidate/review decision with
reason. The action performs one `task.request_changes` transition and one correction
Send if owner is ready. Otherwise `needs_correction` remains for resume/dispatch.

Preserve reviewer replacement for Unanswered, Inconclusive and ReturnedForCorrection.
Bound unchanged rounds; after the bound admit one Governor decision task rather than
looping or stopping the queue.

### Acceptance

`accept` reads the frozen per-requirement evidence plan, current candidate-specific
reviews/checks and exact dependency receipts. It calls the same gate as direct
Governor request. It never fabricates review text/evidence. Candidate B cannot reuse
A's review.

### Goal message and native Goal

Harness Goal is adapter-native and default where supported. `goal_message` is an
independent ordinary message action:

```text
selected event/due occurrence
-> current Goal/assignment/budget
-> one agent.send Operation
```

Goal policy owns message/template, trigger/cadence, cooldown, optional max occurrences,
turn/elapsed/cost/no-progress budgets, completion evidence and stop/rotation behavior.
Defaults do not invent an unlimited continuation loop. A later occurrence may repeat
text intentionally; the same occurrence never repeats. Disabling Eliot messages does
not pause native Goal, and pausing native Goal does not disable Eliot messages.

Governor may combine native Goal and Eliot messages with different objectives. It
pauses/clears native Goal only through the adapter's real capability and exact session.
No controller “continue” command is rebranded as a vendor Goal.

## 7. Script policy

Prefer common actions for dispatch, review, correction, acceptance, Goal messages and
checks. A retained script may return only typed actions genuinely needed, such as exact
Send, Reply, Configure or Goal; each becomes an ordinary child Operation under current
Manager/object rights.

Do not add arbitrary Store passthrough, second permission registry or workflow
language. Script identity `(ScriptRun, action index)` prevents lost responses from
re-running admitted effects.

## 8. Transfer and disable

Transfer changes owner/revision of the same `entry_id`. Due occurrences and child
Operations keep original admitting identity; future actions recheck current rights.

Remove disabled former-owner copies, successor chains, ancestry walks and journal
relocators. A→B→A preserves one entry rather than colliding with historical copies.

## 9. Storage and explanation

Use one indexed entry representation for identity, owner, project, revision, enabled,
trigger/next due and settings. Store occurrence→Operation identity only where
recurrence requires it. Bounded settings may remain JSON inside the row.

Do not persist queue items, provider state, candidate lifecycle or action-specific
workflow copies.

`automation.config.explain` returns current blockers/next action: no eligible Task,
unresolved dependency under `hold`, overlapping lease, route/provider/capacity hold,
pending native request/effect, exhausted continuation budget, no-progress, missing
review/check evidence or disabled/stale policy. It grants nothing and invokes no
provider/model.

## 10. Current source removal map

Current `AutomationStep` still contains WorkDispatch, ReviewDispatch,
ReviewDisposition, RepairDispatch, Acceptance, Publication, CheckRun,
GithubProjection, GoalProgression and ScriptRun. `has_consumer()` reports true for all,
while several steps have no ordinary common action and depend on separate consumers.
Current Goal progression has only `goal_id` settings and admits one continuation per
terminal event through its own cursor/slot/proof machinery.

Migrate one connected action at a time. Once callers use the target action, remove old
enum value, schemas, policy, Store state, fixtures and worker demand in the same slice.

## 11. Verification

- enabled policy runs assignment→result→review→all-findings correction→fresh review→
  acceptance without routine human step;
- same occurrence admits once; lost wake rescans current state;
- blocked Tasks do not hide eligible work;
- dependency `hold` blocks automatic start; `allow_preparatory` starts visibly but
  cannot accept/publish before exact dependency receipts;
- only configured route fallbacks are used;
- native policy handles known requests and exact unresolved requests reach Governor;
- native/Eliot Goals work alone/together with selected budgets and no infinite default;
- no-progress leads to bounded Governor decision; safe rotation drains/checkpoints;
- correction returns all findings once; candidate-specific evidence gates acceptance;
- disable/transfer preserve in-flight and occurrence identity;
- no standalone scheduler, WorkDispatch queue, repair workflow, Goal cursor workflow
  or GitHub projection workflow remains after its migration.
