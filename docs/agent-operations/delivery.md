# Delivery — Manual Commands and Manager-Owned Automations

This is the target delivery contract. [Implementation](implementation.md) names
the source changes; [status](../implementation-status.md) records what actually
works. One set of Store handlers serves manual actions and selected automations.

## 1. Delivery without another Task ledger

For review-required work, a manager assigns work, retains the complete candidate,
obtains independent review, returns actionable findings to the owner, and accepts
the corrected candidate after fresh review. Frozen Task requirements decide which
reviews and checks are required. Task, Attempt, Operation, submission and review
records already represent this cycle. Ready, reviewing and audited are useful
projections; they do not require another workflow database or parallel state machine.

The manager chooses actions directly or enables individual handoffs. Waiting for
the manager, evidence or capacity is a visible pending reason with a useful next
action. It does not justify repeated prompts or a new orchestration service.

## 2. Manual controls and automatic equivalents

| Action | Finite effect |
| --- | --- |
| Plan/read/assign | Inspect or select existing work; reserve its owner and capacity before launch. |
| Launch, steer or reply | Deliver to the named current assignment/native input. |
| Capture and `task.submit` | Retain one candidate and apply its submission. |
| `review.assign` / `review.submit` | Assign an auditor; retain that auditor's exact verdict and evidence. |
| `task.request_changes` / owner continuation | Record manager disposition; separately deliver correction to the owner. |
| `task.accept` | Apply one exact-candidate acceptance after the common evidence gate. |
| Explicit Forge/GitHub command | Perform the authorized remote effect and read it back. |

Manual local work needs no automation object or GitHub installation. An enabled
helper calls the same handler with Store-derived manager authority. Neither tool
visibility nor an internal-service role grants additional product rights.

## 3. Queue and ownership

A source Task refers to the registered forge/repository/item identity, with Issue
and PR kinds distinct. Selected requirement changes create revisions. Preserve the
canonical body, selected comments/documents and uncertain source gaps; a generated
brief is a view, and webhook timestamps are not revision tokens.

Queue selection does not dispatch. At admission, recheck current Task/revision,
pool selection where relevant, dependencies, owner, route and capacity. Reserve
active work and workspace before external launch. Filter readiness before page
limits and preserve the manager's chosen ordering.

One manager owns one mutable worktree and one candidate being integrated. Writers
take non-overlapping assignments and return changes to that owner. Auditors inspect
the retained candidate. Capacity includes active and reserved work, including
unknown outcomes; routes sharing a provider account share its budget. Waiting work
remains indexed under the existing Store owner.

## 4. Exact triggers, not a chain that must have started automatically

| Selected handoff | Required committed subject |
| --- | --- |
| Review dispatch | Applied submission with an unfilled required review slot. |
| Review disposition | Actionable assigned findings for the current candidate. |
| Repair delivery | Applied feedback and the actual current owner/continuation. |
| Acceptance | Required passing reviews and checks for that candidate. |
| Goal continuation | The existing assignment's attributable completed input and selected Goal. |

Manual work can supply any prerequisite. A corrected submission is eligible for
fresh review even when its predecessor used the same automation. Persist the
considered subject with its admitted Operation or pending reason and cursor; a lost
wakeup cannot lose work. Domain identity deduplicates manual and automatic callers.

Automatic publication and Issue-label engines are
[removal targets](implementation.md#implementation-order). Explicit remote commands
remain optional. Their absence cannot block local completion.

## 5. Submission and source freeze

A queued `task.submit` is not an applied submission. Use
[Store submissions](../../crates/swarm-kernel-host/src/store/submissions.rs) to
retain and apply the complete intended candidate, claims and configured checks.
Partial delivery requires an explicit project decision.

[Result retrieval and assembly](module-api.md#results-and-artifacts) must provide
a complete attributable body. Model output alone is not source-tree or independent
check evidence. Capture the intended source/tree, baseline and relevant untracked
files; a mutable branch name is insufficient.

Freeze the reviewed candidate. Before correction, finish or move any reader of the
mutable checkout to retained inputs. A reviewer of immutable A may finish after B
becomes current, but its result remains about A. Capture and apply B before asking
for its review; retaining the same checklist text does not make B the same source.

## 6. Review assignment, verdict and audited state

### 6.1 Assignment and retries

Use the existing candidate-specific review slot. It binds sponsor, Task revision,
Attempt, submission/candidate and declared review slot; record the actual auditor
separately. Configuration revisions and request IDs do not create another slot.

Assignment carries the frozen sources, required coverage and expected result.
Replacement requires disposition of the prior attempt; a timeout alone does not
prove it stopped. Retain earlier output and reject its late reply as the
replacement's result. Separate auditors occupy explicitly declared slots.

### 6.2 Result

`review.submit` is authored by the assigned auditor and names its exact candidate.
A pass covers assigned requirements with evidence. A change request identifies a
demonstrated requirement violation and actionable correction. Inconclusive records
missing evidence or infrastructure uncertainty without demanding speculative edits.

Keep verdict, findings and per-requirement evidence in that one retained result.
Provider failure, incomplete coverage or missing required checks cannot become a
pass because the envelope is valid JSON. Unresolved substantive findings remain
visible even if a summary sounds positive.

Authenticated late result/read access remains limited to the retained assignment;
it grants no new work or generic historical artifact access. Credential revocation
still applies. Supersession preserves authorship and original candidate scope.

### 6.3 Audited

Derive eligibility from the required retained assignments/results and checks for
the exact candidate and phase. Do not store another `audited` ledger or copy review
text into an acceptance proof. Candidate replacement or a replacement assignment
changes review applicability. Acceptance invalidation affects its retained decision;
none of these changes erases history or requires a new review-retraction API.

Only required evidence matters; code review does not claim deferred live tests.
A new submission ID alone does not resolve an unchanged finding. Corrected source
or relevant new evidence permits re-review. Repeated unchanged evidence produces a
focused pending reason, not endless model prompts.

## 7. Return and repair

The auditor reports; the authorized manager applies relevant findings through
`task.request_changes`. Only explicit owner input or selected repair delivery
starts correction. Remove document-edition rights through the
[current-format transition](implementation.md#current-format-and-existing-data).

Target the current Attempt owner with exact findings. Keep a valid unreleased
Attempt for correction; changed Task meaning or ownership follows the ordinary
revision/Attempt rules. Reassignment preserves useful partial output and requires
known native/workspace disposition. Infrastructure failure is not a code defect.

## 8. Acceptance, upload and publication

**Current source gap:** direct `task.accept` checks caller-supplied review texts
against requirement IDs, while only the automatic producer requires an assigned
pass. It can therefore accept a review-required candidate without its retained
independent review. Ordinary acceptance authority and candidate checks still apply.

The target [common acceptance gate](implementation.md#automation-and-delivery)
resolves and retains required review/check references, validates them at admission
and rechecks the same evidence before settlement. Both manual and automatic callers
use it. The manager supplies a decision/reason, not a second purported auditor result.
Missing, inconclusive, changes-requested or old-candidate reviews cannot satisfy a
required pass. Acceptance of B needs B's evidence.

The compact core ends at accepted local work. Optional `forge.upload_candidate`
may upload a candidate for selected remote checks; that is not acceptance or
publication. Explicit publication binds accepted candidate, target and effect.
Audit, acceptance, upload, push, merge and Issue closure remain separate facts.

## 9. GitHub boundary

### Intake and feedback loops

Use the existing `gh` transport and registered credentials. A replacement library
must remove enough owned transport code to justify the change; octocrab is not a
required migration. If webhooks are used, verify their source and signature,
retain facts before acknowledgment and reconcile missed/duplicate events.

Remote labels/comments do not choose credentials or revise frozen requirements.
Correlate controller-generated echoes while preserving newly selected normative
content and source uncertainty. GitHub sees the authenticated App/user; internal
manager attribution does not create another independent remote approval.

### Projection writes

Explicit manager commands or project CI can write selected labels, Checks or
summaries. Preserve unrelated content and bind evidence to the exact head.
Correlation markers are not an exactly-once guarantee. Neutral/skipped conclusions
are not a passed audit, and a stale label grants no new action.

### Push, merge and retries

Retain the existing non-force accepted-candidate publication and remote readback.
A preflight old-ref comparison is not atomic compare-and-swap; handle incompatible
movement through the selected Git/forge policy. PR head, base and actual landed
commit are different facts. Optional merge-queue behavior is not a local dependency.

One owner decides retries for each effect. Bounded safe reads may retry; an
ambiguous write requires exact readback before another action. A queued/enqueued
request is not proof of publication or merge. Changing request ID, head or
configuration cannot free the same uncertain target.

### Disable and recovery

Local disable holds future admissions; it does not undo an accepted native/remote
effect. Continue authorized observation and exact readback. Cancellation is a
separate supported action. Isolate the uncertain resource while other work proceeds.

## 10. Next task and complete handoff

Retain issued outcomes regardless of enabled state. Cleanup, closure and next-task
distribution each need their explicit command or currently enabled choice.
Prepare the next Task in the worktree only after conflicting work is disposed of;
retain or integrate useful writer output.

Verify the changed path through assignment, complete result, candidate A review,
correction B, fresh review and acceptance. Include the relevant restart/unknown
outcome case. GitHub, hooks, scripts and every native runtime are not a compulsory
test campaign for an unrelated local change.
