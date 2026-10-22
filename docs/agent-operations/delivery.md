# Delivery — autonomous work through ordinary commands

This is the target work cycle, not a claim that all handoffs are implemented.
[Implementation](implementation.md) owns source changes;
[status](../implementation-status.md) records evidence. Governor and Manager are agent
roles. They can act directly or enable helpers; neither means a human must approve
every step. The owner sets scope, budgets, credentials and exceptional decisions.

## 1. Delivery without another Task ledger

An enabled policy connects selection, assignment, native execution, capture,
independent review, correction, acceptance and selected publication. Existing Task,
Attempt, Operation, candidate and review records represent the cycle. Helpers query
these records and call the same handlers as the Governor. No second workflow ledger,
coordination platform or model-driven heartbeat is needed.

`explicit` means an attributable command with current authority. It does not mean a
manual click. A project can disable automation, but the automated path is a retained
capability, not future work delegated to the operator.

## 2. Manual controls and automatic equivalents

| Current fact | Enabled action | Completion or next decision |
| --- | --- | --- |
| Eligible work and available route/workspace | `dispatch_once` claims and launches or resumes | One initial input owner and one admitted Operation |
| Native question or permission request | Native permission rules, then addressed Governor `Reply` | Exact request answered; no queued prompt pretending to be a reply |
| Required output or code candidate available | Capture the required complete output or frozen source candidate and submit | Submission applied, not merely queued |
| Candidate lacks required review | `review_once` assigns an independent auditor | Verdict and evidence retained for this candidate |
| Review requests changes | `task.request_changes` plus one correction message when owner is live | All actionable findings returned together |
| Required evidence is ready | `accept_once` calls ordinary `task.accept` | Candidate-specific acceptance |
| Accepted work with publication selected | Existing Forge/GitHub command | Exact target readback, then selected closure/next work |

Each action records the represented manager and subject in its Operation. Helpers do
not gain extra rights. Configuration changes affect future admissions; they do not
rewrite outcomes already admitted under an earlier policy.

## 3. Queue and ownership

Rank corrections and unfinished work before new work, with explicit project priority
and stable tie-breaking. Filter eligible rows before limiting admissions or continue a
bounded keyset scan: blocked first-page items cannot hide the rest of the queue.
Capacity/permission changes and due time wake the same host loop.

Reuse a compatible idle binding when continuation policy permits. A parent being idle
does not settle its children, native Goal, pending requests or unknown effects. Drain
stops new assignments while allowing replies, results and readback. Rotation is a
selected action after work disposition, not an age-based kill.

Preserve `Attempt.start_owner`: native-manager delegation reserves before native
spawn and later binds the observed producer; controller delivery reserves its own
start. Existing `START_OWNED_BY_MANAGER` rejects a second controller start even before
the native child has been bound. A follow-up message is not a new initial dispatch.

One manager owns the mutable worktree and its Git index/commit operations. Leaf writers
can edit disjoint assigned paths; that does not give each writer independent Git-index
ownership. Leases coordinate Eliot admissions, not unrestricted shell access.

## 4. Exact triggers, not a chain that must have started automatically

Manual and automatic callers can supply the same prerequisites. Derive action identity
from the current subject: review slot/candidate, returned review/candidate, or accepted
candidate/remote target. Scheduled Eliot messages instead use the due occurrence;
a later occurrence may intentionally repeat the text.

Commit domain transition and child command admission together when they belong to one
local action. Lost replies return the existing Operation. Lost wakes cause a bounded
current-state rescan. Do not persist copied pending subjects, per-domain catch-up
cursors or another operation-link registry.

Removing automatic-publication machinery removes its duplicate lifecycle, not the
ability of an enabled policy to call publication. Acceptance alone never silently
pushes, merges or closes an Issue.

## 5. Submission and source freeze

TaskPacket includes required sources, selected source revisions, current findings,
write boundaries, deliverable and verification. It omits controller proof JSON, not
necessary requirements. A missing required comment/source remains a visible gap; an
external document is task input, not a grant to change permissions.

Capture the intended tree, baseline and relevant untracked files before review. A
branch name, model claim or retained output page is not a frozen code candidate. The
auditor reads immutable content or an isolated snapshot. Review of A may finish after
B exists; its verdict stays about A. Correction must not mutate the auditor's input.

### Continuation attribution and candidate consumption

Task-linked `Send` retains its authorized Task/Attempt on the existing Operation at
admission. Current selected Manager, Repair and Goal paths already store this relation;
reuse it rather than adding a continuation ledger. Binding-only commands do not all
have an assignment. Ambiguous task-linked input needs an exact target chosen by the
Governor; deliberately taskless session input stays unassigned. Preserve current role
and object grants; do not enable Participant control merely to attach provenance.

Result origin follows the actual source Operation and native input/turn facts, not the
newest Task on a session. Keep first `start_operation_id` and `start_owner` unchanged.
Extend the common result-origin validators through `validate_page_artifact`,
`validate_candidate_origin` and `submissions::candidate_for_attempt`; accepting another
method string at admission alone is not a connected fix. The selected Antigravity
path currently still rejects normalized results from Send. The shared page/assembly
path in [module API](module-api.md#results-and-artifacts) remains the only one.

Preserve existing same-Attempt resubmission: `submissions::current` already permits
unreleased `needs_correction` with expected Task revision and prior submission.
`reserve/begin/finish` retain original provenance and apply only current output; late
published evidence stays historical. Do not introduce another repair-submit state or
new Attempt just to submit a corrected result. A frozen source snapshot is already a
separate code-candidate path; reconstructing model prose is not an extra prerequisite
for that candidate unless the Task requires that output. Required complete native
results still follow durable capture and exact source attribution.

## 6. Review assignment, verdict and audited state

### 6.1 Assignment and retries

Use the existing review slot bound to Task revision, Attempt and candidate. Retain
sponsor, assigned reviewer and actual author separately. Reassignment does not require
a verdict from a missing reviewer: current `store/reviews.rs` already classifies
Unanswered, Inconclusive and ReturnedForCorrection replacements. Preserve that path,
its expected-assignment check and historical late results during reduction.

Superseding a review slot does not prove a native process stopped. A read-only old
reviewer may finish on A, but cannot satisfy the replacement slot or acquire writes.

### 6.2 Result

The assigned auditor returns verdict, all actionable findings and requirement-linked
evidence in one result. Infrastructure failure or incomplete coverage is inconclusive,
not code failure or pass. Return the whole findings set in one correction round; do
not make the manager redispatch each finding.

Native permission modes/rules handle already authorized tool operations. An unresolved
form or judgment question goes to the Governor agent with its current request ID and
options. Do not answer every question with the first/recommended choice, deny all tool
calls or default to a human. Only missing access/authority or genuinely unresolved
owner intent requires operator intervention.

### 6.3 Audited

Required checks and reviews are read from retained candidate-specific evidence by the
common acceptance gate. A caller does not supply another copy of an auditor's proof.
Code changes require review of the changed candidate.

An unchanged tree can gain missing relevant evidence, such as a previously unavailable
check. Conversely a new submission ID, prose explanation or irrelevant edit is not
progress. Compare unresolved findings and applicable evidence, not only source hash.
Known unrelated baseline failures remain distinct under project policy; absence of new
failures alone does not prove a requirement passed.

## 7. Return and repair

With return-changes enabled, the existing Store transition applies actionable review
findings and admits one child `Send` for the live current owner in the same transaction.
Otherwise `needs_correction` remains current Attempt state and the next dispatch/resume
renders the correction packet. No separate RepairDispatch backlog is necessary.

### Delivery is not a successful correction

Use the selected adapter's normalized facts, not one provider's receipt string:

| Observed fact for the exact correction input | Meaning and next action |
| --- | --- |
| Native admission confirmed | Input delivered; await execution/output, not another Send. |
| Attributable native terminal observed | Delivery happened; retain success, failure or cancellation separately, capture required output and follow the configured repair policy. |
| Confirmed rejection before input or cancellation while still unsent | No native execution; the ordinary policy may choose the next authorized action. |
| Response lost or effect uncertain | Bounded exact readback; do not resend from a generic failed/rejected label. |

Current `automation_repair::delivery_state` and historical direct readback require
`native_input_admitted` plus a native input ID. Antigravity reports an attributable
terminal without that ID, so these readers leave it pending. They also treat every
Rejected/Cancelled row as no effect, although a terminal failure can follow execution.
Remove these duplicate readers with G's RepairDispatch deletion; consume the common
Operation's real admission/terminal facts instead. Do not fabricate IDs or add another
vendor/version allowlist. Delivered input, captured body, successful correction,
submission and acceptance remain different facts.

Bound no-progress rounds and cost. When the bound is reached, send one addressed
Governor decision: narrow work, change an approved route, replace the worker/reviewer,
request missing evidence or defer the task. Do not loop indefinitely or require the
human to perform normal manager work. Preserve refuted findings with their evidence so
later agents do not repeat the same accusation.

## 8. Acceptance, upload and publication

Manual `task.accept` and `accept_once` resolve the same frozen requirements and current
review/check references. Missing required evidence, unresolved findings or a review of
another candidate cannot satisfy acceptance. The manager supplies a decision, not a
fabricated reviewer result.

Optional upload for remote checks is not acceptance. Push, merge and Issue closure are
separate authorized effects which an enabled policy may invoke. None is mandatory for
local-only work. Preserve useful partial output without advertising it as complete.

## 9. GitHub boundary

### Intake and feedback loops

Collection, selected import and remote writes have different owners and may all be
automatic. Imported source changes do not overwrite a working Task's frozen requirements
without an explicit revision transition. Preserve repository/item identity, source gaps
and conflict detection; do not recreate a second external work queue.

### Projection writes

Keep existing transport and native authentication unless a replacement deletes actual
code. Policy-selected labels, Checks or summaries call the ordinary effect owner;
remove activation/quarantine/projection workflow copies. Preserve unrelated content.

### Push, merge and retries

Persist external intent before sending. Read-only recovery can retry under bounded
policy; a timed-out write is not automatically safe to resend. Resolve exact remote
state before another mutation. A fresh request ID cannot bypass the unresolved target.

### Disable and recovery

Unknown is a recoverable state with an owner, not an instruction to abandon the task.
Existing reconciliation performs bounded reads; unchanged uncertainty coalesces into
one Governor decision while independent work proceeds. Humans are not a generic retry
service.

A2 must cover a new adapter boot, not only IPC reconnect: fence the old delivery owner,
read retained complete outcome/result bytes, and reconcile them to the original
Operations without relabelling origin boot or resending input. Late evidence remains
historical if its assignment was superseded. Missing durable bytes remain a stated
limit, not an invented success.

Existing `task.submit.recover` is a narrower downstream path: scoped current GM/Operator
reads an already-published submission while preserving its original submitter and
historical target. Reuse it. It does not itself recover an adapter's lost native body;
do not merge both responsibilities into a new generic recovery service.

## 10. Next task and complete handoff

The enabled cycle continues to the next eligible Task after current workspace and
native work are disposed. Preserve delivered outcomes even after a helper is disabled.
A useful connected proof runs assignment, question/reply, candidate A, all-findings
correction, candidate B, independent review, acceptance and selected publication with
no routine operator step, plus relevant reconnect/crash boundaries.

### Preservation evidence and donor limits

The historical preservation pass compared the pre-refactor
[specification at f1b854b6](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md),
the supplied October 6 audit and master audit v26, current code, and the retained
[Claude-script review](../remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md).
The original private ZIP was not reopened; script-line claims remain attributed to
that review, not newly executed scripts. The 2026-10-22 continuation/submission pass
is recorded as AR-009–011 in the [audit register](audit-register.md); it changes target
instructions, not runtime code or qualification status.

| Historical lesson | Retained mechanism / current owner |
| --- | --- |
| Native child already started before registration | `operations.rs` start-owner guard; `producers.rs` assignment correlation |
| Queued steer did not answer a form | Adapter `Reply`, native permission rules and addressed Governor attention |
| One finding per repair round wasted work | `reviews.rs` result + `submissions.rs::request_changes`, all findings together |
| Missing reviewer blocked completion | Existing `ReviewReplacementClass`, not a newly invented workflow |
| Compact TASK.md worked better than controller JSON | Existing `task_prompt.rs` renderer and required source selections |
| Fixed-age rotation or newest-log ownership caused wrong decisions | Native identity/readback, exact process custody and explicit source gaps |
| Old scripts controlled more than message/create | Existing helpers or narrow typed script actions, not another permissions DSL |

Paseo's [loading code](https://github.com/getpaseo/paseo/blob/b4af508e2a9e5a34a8b0ffb8dfaff6fd679da6c7/packages/server/src/server/agent/agent-loading.ts)
provides a small reusable pattern: coalesce loading by identity and recheck a close
barrier before resume. It does not establish native exactly-once execution.
Official Claude [permissions](https://code.claude.com/docs/en/agent-sdk/permissions)
and [user input](https://code.claude.com/docs/en/agent-sdk/user-input) describe native
policy/callback/defer mechanisms. Check the installed SDK seam before adopting newer
features; do not build a private replacement merely from an old missing capability.
