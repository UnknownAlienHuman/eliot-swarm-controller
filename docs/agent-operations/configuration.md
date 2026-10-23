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
| `review` | Assign a missing required review slot for the current candidate; replace a timed-out or explicitly unusable assignment by compare-and-swap. |
| `return_changes` | Apply all current actionable findings in one local feedback transition and admit at most one exact correction Send when the owner is live. |
| `accept` | Call the common candidate-specific acceptance gate when frozen review/check evidence is ready; recheck late evidence against that same candidate. |
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

Occurrence identity uses the stable entry and intended trigger occurrence, not the
current owner, configuration revision or wake time. For a calendar, retain its
expression/timezone/anchor generation and intended UTC due instant. An action-only
edit, re-enable, transfer or restart cannot replay an already-considered occurrence;
`include_existing` may select only an unconsidered occurrence. Changing back to an old
calendar generation retains its consumed occurrences. Preserve the useful current
[CheckRun semantics](../schedules.md), without inheriting its owner-copy machinery.

Terminal-driven Goal continuation also retains the exact source input/terminal-event
identity across entries: a new entry or request ID cannot claim the same continuation
twice. A temporary authority/capacity blocker does not consume an unadmitted action;
a later current-state pass rechecks it after rights, ownership or capacity changes.
Do not rewind the entire event history or replay an already-admitted input to recover
that blocked subject.

Keep missing review results and other unresolved prerequisites attached to their exact
Task/candidate/slot. Wait for that subject's configured finite response deadline or
current-state check; silence alone is not a reviewer failure. On an expired deadline,
replace only the exact unanswered assignment by comparing its current identity,
selecting a distinct eligible reviewer under ordinary policy. Bound replacements
per slot. If the bound is exhausted or no eligible reviewer route exists, coalesce one
addressed Governor decision for that subject. A blocker on one subject must not roll
back unrelated admissions in the pass. Deadlines are checked by the shared loop; do
not add a per-review timer, cursor or backlog.

Do not create a process, credential, poller, cursor family or private pending queue per
action/entry. A lost wake causes a bounded rescan. Downtime coalesces missed ticks
according to selected policy rather than replaying stale model prompts.

## 5. Route, dependency, session and permission policy

A target policy selects ordered route aliases, an optional agent profile, dependency
mode and finite review/correction limits. This example is a target shape, not a
configuration accepted by the current legacy parser:

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
  },
  "review": {
    "response_deadline_ms": 3600000,
    "max_replacements_per_slot": 2,
    "max_correction_rounds": 3,
    "max_no_progress_rounds": 2
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

Hold and retry each blocked subject against its own current dependencies. A missing
dependency must not roll back otherwise eligible admissions or stall unrelated
subjects; unknown effects remain attached to their exact resource for readback.

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
reason. Preserve the full ordered actionable package in one local
`task.request_changes` transition. In the same Store transaction, write attributable
feedback, move the current Attempt to `needs_correction`, and admit at most one child
`agent.send` carrying that exact package when the owner binding is eligible. Native
I/O occurs after commit. Otherwise retain `needs_correction` for current-state resume.
Never split findings into competing Sends or treat mailbox feedback as native delivery.

For review replacement, distinguish a missing result from a terminal result. A
configured finite response deadline, measured from the retained assignment admission,
plus a current-state check may make an unanswered slot replaceable; an `inconclusive`
result is replaceable under policy, and a
`ReturnedForCorrection` candidate may receive a replacement assignment for the same
Attempt/current candidate. Use the expected current assignment as a compare-and-swap
precondition and a distinct eligible reviewer; do not infer failure from silence or
replace an assignment whose result/identity changed concurrently. Bound replacement
count. If that limit is reached or no eligible reviewer route remains, coalesce one
addressed Governor decision with the exact candidate, prior assignments/results and
available choices. No independent timer or replacement backlog is needed.

Reserve one of the finite correction rounds when admitting each new correction input.
Assess no-progress from the round's settled candidate, review, check and delivery
evidence; missing or unknown evidence is pending, not an empty result or progress.
Waiting, receipt replay and readback retries do not reserve additional rounds.
Repeated findings/no-change outcomes cannot reset the bound by changing IDs or making
unrelated edits. Resubmission, policy edits, transfer and restart keep prior consumption;
an explicit budget increase adds allowance without erasing it. On exhaustion, stop
new correction admissions and coalesce one
addressed Governor decision while unrelated subjects continue. Preserve unknown native
effects and exact Operation readback until resolved; a budget never authorizes replay
or discarding uncertain delivery.

### Acceptance

`accept` reads the frozen per-requirement evidence plan, current candidate-specific
reviews/checks and exact dependency receipts. It calls the same gate as direct
Governor request. Both callers must resolve required assigned reviews and actionable
finding dispositions from retained candidate-specific facts. An acceptance reason or
fresh feedback cursor cannot substitute for them. Current automatic acceptance copies
the assigned auditor's V2 `requirement_reviews`; preserve that behavior while closing
the direct caller's provenance gap in the common gate ([AR-064](audit-register.md#ar-064)).
Candidate B cannot reuse A's review. If a CheckRun completes after an
initial hold, re-evaluate its retained receipt against the same current candidate and
rerun the common gate; do not reuse it for a replacement candidate. A missing check
may remain pending while that candidate is current. A dependency receipt invalidated
after claim is different: a replacement receipt does not silently rewrite the
Attempt's frozen dependency pin. Return `DEPENDENCY_REVALIDATION_REQUIRED` to a
per-subject replan/reclaim decision under ordinary rights, retaining the candidate
while that decision is made; do not confuse this with `DEPENDENCY_NOT_READY` at claim
resolution or roll back unrelated subjects.

### Goal message and native Goal

Harness Goal is adapter-native and default where the selected adapter exposes the
real capability. `goal_message` is an independent ordinary message action:

```text
selected event/due occurrence
-> current Goal/assignment/budget
-> one agent.send Operation
```

Use the represented Manager's ordinary Send authority on any selected adapter that
supports the requested delivery timing. Do not branch on vendor names or require a
native Goal record/proof as permission to send an Eliot message. Preserve the original
Task/revision/Attempt, source Operation and binding; the technical requester is not a
registered Manager or an independent capability grant.

Check the work phase at admission and again before unsent native input:

| Current subject | Automatic next action |
| --- | --- |
| Current working assignment, eligible occurrence, no unresolved conflicting input/request | Admit at most one bounded Goal Send. |
| Candidate submitted or awaiting its review/check disposition | Pause generic Goal Sends; continue result capture/readback, required checks, review and acceptance. |
| Current `needs_correction` | `return_changes` or its resume path owns one findings-bearing correction Send for the exact candidate/review. Generic Goal Sends cannot compete with or bypass that correction identity. |
| Accepted, superseded or released assignment | No new task-linked Goal input; retain historical result and effect readback. |
| Uncertain input, pending question/permission or incompatible native activity | Read back or answer through the proper action; do not use Goal Send as retry or Reply. |

Goal policy selects message/template, trigger/cadence, cooldown and stop/rotation
behavior. Enabling recurring `goal_message` requires a positive finite
`max_occurrences` and `max_no_progress_rounds`; optional elapsed/native-turn/cost limits
may stop it earlier. Missing bounds reject configuration rather than mean unlimited.
The admission transaction reserves the occurrence against that budget once; identical
retry does not spend twice. A new request ID, entry edit, transfer or restart does not
reset consumption for the same Goal/Task revision/Attempt. An explicit attributable
policy change may authorize additional remaining budget, retaining prior consumption;
it cannot reset counters or refill them automatically on a wake.

Progress is a relevant candidate change, new requirement-linked result/check evidence,
or disposition of an actionable finding. An unchanged candidate can gain useful
evidence; a fresh ID, another terminal event or unrelated edit cannot reset no-progress.
Evaluate a settled round after its required capture/readback so missing evidence is
not mistaken for an empty result. Use existing Goal/entry settings and linked
Operations/evidence for this accounting, not a new workflow journal. Unknown native
usage is not zero; a requested native-turn/cost bound needs the selected source's
capability and current facts. A finite occurrence cap still bounds Eliot messages.
These limits do not stop a simultaneously active native Goal. Do not present an Eliot
message cap as a bound on autonomous native turns or total shared-session spend; those
controls need the native Goal's own supported budget/stop policy.

On exhaustion or the no-progress bound, stop further Goal admissions and coalesce one
addressed Governor decision with the exact subject, evidence and choices to revise,
renew, rotate or stop. Capture, review, acceptance and unrelated work remain eligible.
A later authorized occurrence may intentionally repeat text; the same occurrence
never repeats. Disabling Eliot messages does not pause native Goal, and pausing native
Goal does not disable Eliot messages or replenish their budget.

Governor may combine native Goal and Eliot messages with different objectives. It
pauses/clears native Goal only through the adapter's real capability and exact session.
Native Goal ACK/status is not a terminal turn or Task acceptance. No controller
“continue” command is rebranded as a vendor Goal. See the pinned
[native capability comparison](donor-map.md#native-goal-and-eliot-goal).

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

| Change | Effect on existing work |
| --- | --- |
| Disable or edit entry settings | Stop/change future admissions. Every queued Operation, including Goal/correction Send, keeps its admitted request and attribution; entry revision mismatch alone does not reject it. Actual rights, Task/candidate/phase and binding still fence unsent effects. |
| Transfer entry | Future admissions use the successor's current rights. Existing Operations retain the original represented actor and exact validated continuation authority; no retroactive reattribution or replay. Recheck unclaimed blocked subjects under the successor's rights. |
| Actual grant revocation, Task revision/release, changed candidate phase or binding scope | Revalidate the exact unsent action and reject/supersede it when no longer authorized or eligible. Policy disable is not an authority revocation. |
| Input already sent, accepted or uncertain | Preserve its ordinary lifecycle and exact readback/result obligations. Changing policy cannot prove cancellation or authorize replay. |

Current repair dispatch has an exact transfer-continuation branch, unlike Goal's
ordinary revision check. Its no-continuation branch still rejects queued repair input
after pure disable/edit. Preserve validated current authority when removing that
policy/intent coupling; do not copy another transfer ledger into the target.

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

The [connected source trace](audit-register.md#trace-2026-10-23-goal-policy) records
specific gaps: OpenCode's generated `agent.goal` is rejected at the runtime actor
guard; Codex rechecks enabled/exact policy revision after admission; submitted and
correction Attempts lack a generic-Goal phase fence; finite total/no-progress policy
is absent; ownership-conflict cursor advancement can hide an unclaimed event after
transfer. Direct `reviews::reserve_assign` supports expected-assignment replacement,
but `automation_dispatch` invokes it as `ReviewActor::OnBehalf`, whose request guard
forbids replacement. Configured `ReviewDispatch` therefore cannot perform that
replacement path. Current review-result retry caps limit backoff only, not correction
rounds; no current-state round budget or no-progress Governor disposition is
implemented. The common acceptance gate already checks frozen plan/candidate/check
evidence, but required assigned-review and unresolved-finding checks are not yet shared
by direct acceptance; a revoked dependency pin can abort the review-disposition pass.
The [review trace](audit-register.md#trace-2026-10-23-review-acceptance) qualifies these
separate gaps. Terminal deduplication and calendar occurrence continuity are useful
existing behavior, not evidence that these gaps are fixed.

Migrate one connected action at a time. Once callers use the target action, remove old
enum value, schemas, policy, Store state, fixtures and worker demand in the same slice.

## 11. Verification

- enabled policy runs assignment→result→review→one all-findings local feedback plus
  at most one exact child correction Send→fresh review→acceptance without routine
  human step;
- unanswered reviews wait on a finite configured deadline and current-state check;
  replacement uses expected-assignment CAS, a distinct eligible reviewer and bounded
  replacements; no eligible route or exhausted replacement budget produces one
  addressed Governor decision;
- same occurrence admits once; lost wake rescans current state;
- blocked Tasks do not hide eligible work;
- dependency `hold` blocks automatic start; `allow_preparatory` starts visibly but
  cannot accept/publish before exact dependency receipts;
- only configured route fallbacks are used;
- native policy handles known requests and exact unresolved requests reach Governor;
- native/Eliot Goals work alone/together with selected budgets and no infinite default;
- a submitted candidate pauses generic Goal input while capture/check/review/accept
  continue; correction admits one findings-bearing Send for its exact subject;
- correction rounds are reserved once at new input admission; no-progress is assessed
  from settled evidence and leads to one
  addressed Governor decision; unknown deliveries remain in exact readback;
- correction returns all actionable findings in one feedback package; candidate-
  specific evidence gates acceptance, including late CheckRun evidence rechecked for
  the same candidate without fabricating V2 requirement review evidence;
- disable/transfer preserve in-flight and occurrence identity;
- direct acceptance cannot substitute caller-authored reviews or a fresh cursor for
  required assigned evidence; revoked dependency pins block only their own subject;
- disable/edit alone do not cancel admitted Goal/correction input; real
  revocation/stale Task, candidate, phase or binding scope still fences unsent input;
  transfer retries an unclaimed blocker without replaying history;
- no standalone scheduler, WorkDispatch queue, repair workflow, Goal cursor workflow
  or GitHub projection workflow remains after its migration.
