# Delivery Contract — Manual Work, Optional Handoffs and GitHub

Revision 3 · 2026-10-03 · proposed Rust implementation, not shipped functionality.

[Configuration](configuration.md) owns explicit manager control. [Architecture](architecture.md) owns action admission, pause ordering and recovery. [Donor map](donor-map.md) records source/API evidence.

## 1. The same delivery path in manual and delegated operation

**Manual is the default.** A manager can choose every assignment, auditor, correction, continuation and publication without enabling a preset, scheduler or Goal. The controller still observes work and retains exact results.

```text
ready -> assigned -> working -> submitted -> reviewing
                                             |
                      +----------------------+------------------+
                      |                      |                  |
              changes_requested           audited          inconclusive
                      |                      |                  |
             manager decision       manager decision     manager decision
                      |                      |                  |
                  repairing          acceptance and      review/diagnosis
                      |                publication
                 new submission              |
                                         published
```

Each decision may instead be delegated by an explicit manager-selected stage. Saving `reviewed_delivery` describes the path; it does not execute the arrows. `manager_gate` at the end of an otherwise automated pipeline is not fully manual control.

These states are projections over existing Tasks, Attempts, Operations and immutable submissions, not another mutable Task ledger. `awaiting_manager` and `held_by_control` are waiting reasons, not failed implementation. No inactivity timeout means approval.

## 2. Complete manual path

| Manager/participant action | Effect | What never follows implicitly |
|---|---|---|
| Read `swarm.queue.get` or `task.get` | Current task/source/ownership information | Claiming or launching the first row. |
| `swarm.launch.preview` then `swarm.launch`, or authorized existing Task/runtime calls | One selected assignment and its documented workspace/runtime prerequisites | Consuming the rest of the pool. |
| Inspect dashboard/streams, send exact steer/reply when wanted | Observe or direct the named active work | Restarting, changing models or waking unrelated sessions. |
| Submit/capture the integrated candidate | Applied immutable submission when publication succeeds | Launching an auditor. |
| `review.assign` to a chosen available auditor | One exact review slot and its explicitly requested review work | Auto-repair, acceptance or push. |
| Assigned `review.submit` | Retained findings/verdict; local audit coverage projection | Applying Task feedback, launching repair or writing GitHub labels by itself. |
| Authorized `task.request_changes` | Exact-candidate feedback/disposition | A model continuation. |
| Explicit owner continuation/launch | Work on the named correction | An unbounded repair loop. |
| Authorized exact-candidate acceptance and forge operation | Requested accepted publication, with remote readback | Next-task dispatch, unrelated merge, closure or cleanup. |

Use the same application handlers and authority checks as delegated delivery. A manual caller must not enable automation merely to use `review.assign`, `script.run`, `schedule.run_now` or publication. A manager needs the ordinary scoped rights for those operations; manual mode does not turn that manager into GM.

Small-team local work needs no GitHub App, work-pool definition or automatic workflow grant. Optional unavailable integrations produce relevant gaps, not a block on otherwise valid local manual actions. Existing Task source/policy and submission requirements still apply.

## 3. Work pool and optional distribution

### 3.1 Source identity

Stable Task origin is `(forge instance, immutable repository ID, external item ID, item kind)`. Selected source changes create Task/source revisions, not new Tasks for every timestamp or webhook. GitHub Issues and PRs share a number space; distinguish their kind. `updated_at` is not a unique CAS version.

Source indexes preserve the Issue body, selected source comments and canonical documents. A generated brief is a bounded projection, not a new specification. Unknown potentially normative comments remain visible. Text is never executable policy.

`github.work_pool.preview/apply` changes the manager's selection/order over existing Tasks. **Import does not dispatch.** Queue listing, new dependency readiness and available capacity cannot start work in manual/assisted mode.

### 3.2 Admission when requested

For a direct manager-selected item or an explicitly enabled `work_dispatch` stage:

1. Read current Task revision, membership, dependencies and current owner.
2. Resolve the requested or authorized profile; do not silently override manager order/model choice.
3. Reserve Task/Attempt, manager slot and mutable workspace ownership atomically.
4. Commit normal launch/dispatch identity and bounded assignment context.
5. Start the exact work outside the transaction after current authority/control checks.

Ready filtering precedes page limits so blocked rows do not hide runnable work. A queue read is not a reservation. Manual and automatic requests use the same reservation; concurrent requests cannot create two owners.

One manager owns one mutable worktree and in-flight product submission. Writers work on non-overlapping portions of that Issue, not independently published fragments. Heartbeat expiry does not steal ownership. Prior native input, child-family and write-lease disposition must be known before replacement.

Blocker-first reordering, fallback and original-owner preference are explicit configuration. Unavailable capacity gives a wait reason, not repeated starts. When automatic dispatch is off the manager can still select an allowed explicit route for one action.

## 4. Submission and audit boundary

Current anchors are `src/store/submissions.rs::reserve/begin/finish/document/describe`. A queued `task.submit` receipt is not a submission result. Only an applied `task.submission` with retained exact references makes a candidate available for review.

The manager integrates writers, captures/commits the complete intended Issue candidate and executes only the configured phase checks. Every requirement has a disposition. Partial delivery must follow explicit policy, not be an escape from missing integration.

The candidate records source tree, baseline, relevant untracked-source treatment, claims and evidence. It is not a mutable branch pointer. The worktree is frozen during review/publication; an auditor reads retained source or a controlled read-only view.

In manual mode, applied submission updates the dashboard and makes the exact candidate selectable. In delegated mode, a selected `review_dispatch` stage may admit one audit. Failed submission or commit-text parsing never launches a review. WIP commit observation is informational unless a separately enabled rule has a valid candidate and grant.

## 5. Review assignment, findings and audited state

### 5.1 `review.assign`

A manager may call this directly; the workflow service may call it only under enabled control and a review grant. Both reserve the same logical slot `(submission_ref, review_policy_generation, review_slot)`. Retrying or switching control origin does not create a second audit. A deliberate replacement attempt requires observed prior disposition and recorded reassignment, not merely another actor name.

The assignment includes exact Task/Attempt/submission/candidate, source commit/tree, review phase, canonical source index, required coverage, relevant prior findings and result schema. It includes no global queue/transcript. The reviewer cannot alter its evidence or lower required coverage.

Multiple auditors use separate slots. Independence requires actual producer-lineage/permission separation; changing actor IDs or using one model family does not prove independent statistical evidence. Model/provider diversity can be a preference without being a software-version pin.

### 5.2 `review.submit`

Illustrative payload; real identifiers come from the authenticated assignment:

```json
{
  "client_request_id": "review-result-request",
  "review_assignment_id": "assigned-review",
  "submission_ref": "retained-submission",
  "candidate_ref": "retained-candidate",
  "verdict": "changes_requested",
  "coverage": "complete",
  "findings": [
    {
      "finding_id": "missing-consumer",
      "requirement_ids": ["W2"],
      "reason": "The registered caller does not consume the produced result.",
      "evidence_refs": ["retained-source-location"],
      "requested_change": "Connect the result to the documented consumer."
    }
  ],
  "evidence_refs": ["review-evidence"]
}
```

Only the assigned reviewer submits its slot; supplied identities must match. Verdict is `pass`, `changes_requested` or `inconclusive`. Valid JSON does not establish substantive correctness.

A pass requires complete assigned coverage and no unresolved blocker. Missing source, unexecuted required check, empty evidence, quota failure and unknown candidate cannot pass. A requested change needs an actionable violation of actual requirements, not invented requirements or out-of-policy style preferences. An inconclusive result preserves the gap, not a false green or automatic request to rewrite correct code.

Result recording continues even if the manager pauses automation. Late findings stay attached to their original candidate. Supersession/retraction of findings is explicit historical data. Recording a result is not permission to apply its workflow disposition or start another model.

### 5.3 `audited`

The server may compute a local audited projection in every mode. It means all required slots/coverage/checks for the declared audit phase and exact submission pass, with no unresolved blocking finding. Required reviewer count cannot silently default to zero.

`audited_scope` distinguishes code review from integration/live tests and publication readiness. Later-phase tests are not claimed passed. Review retraction, check invalidation, material source change or a new candidate makes the current eligibility stale while preserving old evidence.

A new corrected source tree can be reviewed even with unchanged checklist text. Whitespace-only checklist edits do not remove a source defect. Local audited evidence triggers neither remote labels nor publication unless separately requested/enabled.

## 6. Return and repair are separate decisions

Current `request_changes` requires GM/operator and creates mail, not native input. Extend its existing exact Task/Attempt/submission/candidate checks with a narrow scoped manager/workflow disposition path. Do not distribute GM credentials or create parallel feedback authority.

In manual mode, the review record and proposed correction are visible. The manager chooses whether/how to apply the applicable findings through the guarded feedback method, then explicitly starts or directs repair. A demonstrated unresolved defect still prevents eligibility; choosing when to repair is not permission to ignore it for acceptance.

In delegated mode, `review_disposition` may apply valid feedback. `repair_dispatch` separately allows delivery/continuation. Selecting the former does not silently enable the latter. An inconclusive review needs explicit or enabled diagnosis/review reassignment; infrastructure failure is not a source defect.

Repair normally returns to the existing owner with exact findings and requirement IDs. Configured fallback may act only after prior execution, children, lease and useful partial work are reconciled. Never wake an old session by branch name or use a new request ID to replay possibly delivered input.

Preserve the same unreleased Attempt when still valid; the new submission uses the expected prior submission reference. Changed Task/ownership uses the normal new-Attempt path. Repeated identical failure yields a focused pending diagnostic or manager decision, not endless prompts. Configurable budgets do not turn unfinished work into success.

When repair completes in manual mode, the next review remains a manager choice. In delegated mode it requires the currently enabled review stage; an old pipeline decision is not perpetual authority.

## 7. Acceptance, publication and manual gates

`publication.mode` retains two effect policies:

- `manager_gate`: one candidate-specific decision with audit, remaining conditions, target and proposed effect;
- `auto_after_audit`: eligible work may proceed without another decision only when acceptance/publication stages and their separate grants are actually enabled.

Neither setting activates the pipeline. Full manual control chooses every preceding transition as well. A manager may automate dispatch/review while keeping acceptance/publication manual. Separate acceptance/publication stages mean accepted work can legitimately wait for an explicit push.

Manual and delegated effects pass the same candidate, policy, repository, capability and current-GM checks. Where current methods are GM/operator-only, add only the explicitly authorized scoped delegation described in Architecture. Manual mode never bypasses that gate.

A manager's approval is bound to one exact candidate, target and effect; it cannot authorize future submissions on the branch. If the head/evidence changes, refresh the decision. A request that would publish while another actor owns an uncertain push returns that existing effect or a conflict, not a second push.

Audit, acceptance, upload, merge, publication and Issue bookkeeping remain separate facts. A failed remote label update cannot rerun push; a publication failure does not make valid audited code a new implementation task.

## 8. Local-first audit and optional remote review

There is no requirement to push before the first local audit. Review retained source locally, then perform the requested/enabled acceptance and publication.

When remote CI is deliberately selected, `forge.upload_candidate` uploads only to a configured review-branch namespace under a distinct grant. It is review transport, not accepted publication. It cannot target protected output refs, close Issues, bypass checks or claim acceptance. The manager sees that this option performs a write before remote review.

A manual upload is one explicit command; an automatic upload requires the publication stage and selected review-transport policy. A later CI result is recorded regardless of mode, but cannot auto-merge unless the corresponding effect remains authorized/enabled. Both routes are Rust implementations; no shell `gh`/Python control daemon is needed.

## 9. GitHub integration

### 9.1 Sources and permissions

Use one Rust API/rate boundary with project-local credential handles. Reads, labels/Issues, PRs, Checks and Contents/publication rights are distinct. App or personal-token setup is supported only for endpoints that credential actually permits. Local manual work need not wait for remote setup.

Verify webhook HMAC on raw bytes and validate event/body/repository/installation before durable intake and acknowledgement. Deduplicate deliveries; reconcile unordered/missed events with conditional paged reads. Header strings alone are not authority; a permission-limited response is not proven deletion.

Intake remains observational in manual mode. No comment, mention, edited label, bot quota warning or completed check becomes a model-dispatch command. Source changes never silently rewrite a running Task's frozen requirements or control settings.

### 9.2 Projection writes are optional

The managed `audited` label, exact-commit `eliot/audit` Check, acceptance Check, PR summary and actionable review annotations are remote projections. They need an explicit manual publication/projection request or the enabled `github_projection` stage; they are not read-only housekeeping.

Only complete positive evidence maps to success. Inconclusive does not become neutral/skipped to get a green gate; GitHub required-check behavior is not ELIOT acceptance. Do not write a server-only stale conclusion as if the API accepted it.

Use exact head SHA, recorded Check Run ID and correlation metadata. `external_id` is not a uniqueness guarantee. Reconcile uncertain creates; annotation updates append, so retry batches only with known delivery/readback. Labels cannot be atomically bound to a commit; stale labels never authorize a new head. Preserve unrelated user labels/comments and show projection lag.

Several auditors behind one App do not become multiple independent human GitHub approvals. Do not dismiss required human reviews automatically. Issue closure is an explicit separately permitted completion action; code audit alone is insufficient.

### 9.3 Push and merge

Native GitHub merge queue is optional and depends on repository capabilities. ELIOT's Rust publication queue is separate. Do expensive integration/audit outside database transactions; serialize only the relevant final target, not all repositories.

Existing `forge.publish_ref` uses the exact accepted candidate and normal non-force Git behavior. Its old-ref preflight is not atomic CAS. On incompatible movement refresh/reintegrate/revalidate rather than force-push. Remote readback establishes the observed result, not global ordering over independent writers.

PR merge uses the actual supported API/strategy with expected head when supplied by that API and without bypassing protections. An expected head does not guard arbitrary base movement. Integrated verification needs a qualified strict-up-to-date or merge-group path covering the actual candidate. A local mutex or manager click does not supply missing base-CAS guarantees.

For supported asynchronous merge/queue APIs, retain request identity/options and distinguish accepted/enqueued/merged. A pending conflict or expired result needs exact PR/ref/commit reconciliation, not blind re-merge. Never treat a pre-merge synthetic `merge_commit_sha` as landed. Reject unapproved stacked/downstream effects.

When using GitHub Actions, account for the selected credential's workflow-triggering behavior; token-created events are not all interchangeable. Missing checks remain pending/gaps. Do not run untrusted PR code with privileged workflow credentials to force a green result.

### 9.4 Local pause versus remote automation

A previously requested GitHub auto-merge/merge queue/workflow can outlive ELIOT's local control change. Track its exact IDs and owner in the control/drain view. Stop admitting new local writes, but continue read-only reconciliation of already issued effects.

Removing a queued merge, cancelling a workflow run or clearing a native Goal is a separate explicitly authorized operation when supported. A local manual switch neither disables repository workflows nor proves remote cancellation. Unconfirmed external continuation blocks competing mutation on its target only. Do not falsely report complete manual takeover while it may still commit.

### 9.5 Recovery

Persist intent and target before I/O. Timeout or lost response does not prove the action failed. Read exact remote ref/PR/check state before retry/adoption. Unknown process-tree disposition or external effect holds its scope.

PR/comment/check creation has no general exactly-once guarantee. Serialize local intent, retain remote IDs and correlate readback without claiming markers prevent all duplicates. Turning control off and on cannot generate a new identity for an already applied or uncertain effect.

## 10. Completing work without implicit next work

Confirmation and local recording of an already issued action continue in all modes. Remote labels/summary/closure/cleanup follow only their explicit command or enabled policy. Never delete unresolved submissions or active workspaces because a review branch disappeared.

After the current candidate and children/continuation are resolved, the manager may prepare the next branch in its same worktree and fresh writer context. In manual mode this is a new explicit decision; in delegated mode it requires currently enabled `work_dispatch`. The manager can plan the next Issue without mutating a frozen candidate.

## 11. Acceptance scenarios

- A newly configured project remains manual even after selecting a preset, granting capabilities, importing 1,000 Issues or raising concurrency.
- One manager manually completes launch, review, correction, re-review and publication with every automatic stage off.
- Applied submission updates the dashboard without launching an auditor in manual mode; queued/failed submission launches none in any mode.
- A valid review submitted after pause remains readable/audited where justified but starts no disabled repair/publication action.
- Hybrid mode automates only selected transitions; `manager_gate` alone is not classified as full manual control.
- Manual and automatic assignment of the same review slot starts one auditor; simultaneous publication commands do not send twice.
- Pause before effect-start holds an old queued action; pause after effect-start lists and reconciles the in-flight action.
- Manual handling of a pending transition is not repeated when delegation resumes.
- Late A feedback cannot return/audit/publish B; corrected source with unchanged checklist is reviewable.
- Inconclusive/provider failure does not become a pass or pointless writer repair.
- Local-first review has no first-push deadlock; remote-review upload remains a separate permitted effect.
- A stale audited label never authorizes a new head; failed projection does not repeat publication.
- A pending external merge/native Goal is visible after local pause until supported cancellation/completion is confirmed.
- No built-in stage requires Python, PowerShell, Node, a native GitHub merge queue or a fixed software release.
