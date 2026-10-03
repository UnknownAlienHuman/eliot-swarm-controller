# Delivery — Manual Commands and Manager-Owned Automations

Revision 4 · 2026-10-03 · proposed Rust implementation, not shipped behavior.

[Configuration](configuration.md) defines each manager-owned enabled automation; [Architecture](architecture.md) defines its on-behalf execution and recovery. [Donor map](donor-map.md) records external evidence.

## 1. One delivery path

The manager can choose every step manually. They can also enable any selected handoffs to run on their behalf. There is no separate operating mode, second Task ledger or service-only shortcut.

```text
ready -> assigned -> working -> applied immutable submission -> reviewing
                                                                |
                          +----------------------+--------------+----------+
                          |                      |                         |
                  changes_requested           audited                inconclusive
                          |                      |                         |
                    owner repair           acceptance                 diagnosis /
                          |                      |                    another review
                   new submission            publication
                                                 |
                                          remote readback
```

Each arrow is requested directly by the authorized manager or by a matching enabled automation owned by that manager. Omitted automatic steps remain manager decisions. Successful result recording does not implicitly turn on the following arrow.

These names are projections over existing Task/Attempt/Operation/submission facts. Waiting for a manager, auditor capacity or remote readback is not a defect in the implementation. Silence never means approval.

## 2. Useful manual controls

| Explicit action | Finite effect |
|---|---|
| Queue/Task/assignment read | Current source, status and ownership; no claim. |
| `swarm.launch` or the corresponding existing Task/runtime command | One selected assignment and declared workspace/runtime prerequisites. |
| Inspect streams, exact steer/reply | Observe or direct that work; no unrelated session restart. |
| Submit/capture integrated candidate | One applied immutable submission when publication succeeds. |
| `review.assign` | One named candidate review slot and its requested review work. |
| Assigned `review.submit` | Retained auditor verdict/evidence; no implicit repair or remote write. |
| Authorized `task.request_changes` | Applicable exact-candidate feedback. |
| Explicit owner continuation | Work on that correction, not an unbounded repair loop. |
| Exact-candidate acceptance and forge operation | The requested accepted effect and readback, not every subsequent Task. |

These methods do not require an enabled automation or a cron/Goal/preset object. A small local team can work without a GitHub App; the selected remote action naturally requires its own configured transport and credentials.

An enabled automation invokes these same handlers on behalf of its manager. It does not inhibit manual actions on other work. If both paths reach the same work/review/publication slot, shared admission returns the existing operation or a precise conflict, not duplicate execution.

## 3. Pool import and work distribution

Stable source identity is `(forge instance, immutable repository ID, external item ID, item kind)`. Issue and PR kinds are distinct even when numbers share a namespace. Selected source changes create revisions, not duplicate Tasks per webhook or timestamp; `updated_at` is not a CAS token.

Keep the Issue body, selected source comments and canonical documents addressable. Generated briefs are bounded projections, not new specifications. Unknown potentially normative comments remain visible; comment prose cannot alter automation configuration.

`github.work_pool.preview/apply` selects/order existing Tasks. Import and `swarm.queue.get` do not dispatch. If the manager enables a distribution automation, it acts within that selected pool; it does not consume arbitrary new Issues from the repository.

For explicit manual dispatch or a matching automatic action:

1. Recheck Task revision, pool membership where relevant, dependencies and current ownership.
2. Resolve the selected manager/executor profile and actual available capacity.
3. Reserve Task/Attempt, manager slot and mutable workspace ownership atomically.
4. Commit the existing launch/dispatch Operation and bounded assignment context.
5. Execute outside the transaction after the same owner/action checks.

Filter readiness before page limits so blocked rows do not hide ready work. Preserve manager order unless configured ranking says otherwise. A stale queue page or heartbeat expiration cannot steal another owner's work. Unknown prior native/child disposition requires reconciliation before replacement.

One manager owns one mutable worktree and one in-flight product candidate. Writers implement non-overlapping portions of that Issue and return results for manager integration; their fragments do not become independently published product submissions. A dispatcher may assign multiple authorized managers without merging their workspace ownership.

## 4. Submission is the audit boundary

Source anchors: `src/store/submissions.rs::reserve/begin/finish/document/describe`. A queued `task.submit` is not an applied submission. Only retained applied `task.submission` with exact candidate references makes the work reviewable.

The manager integrates writers, captures/commits the intended complete Issue candidate and runs only the configured current-phase gate. Every requirement has a disposition. Exceptional partial delivery follows explicit project policy; missing production wiring is not automatically acceptable.

Candidate evidence includes source tree, baseline, relevant untracked-source treatment, claims and checks. It is not a mutable branch pointer. Freeze the worktree while its candidate is reviewed/published; auditors use retained source or a controlled read-only view.

Without a matching enabled automation, submission updates the dashboard and waits for a manager's review assignment. With automatic `review_dispatch`, the server reserves the configured auditor slots on behalf of that automation's manager. Failed submit or free-text commit message launches no audit.

A verified commit can separately trigger a manager-enabled notification automation. WIP notification does not imply a completed candidate or costly review. Local Git hook and GitHub event for the same commit coalesce by verified repository/OID and intended action.

## 5. Review assignment and verdict

### 5.1 `review.assign`

The direct manager and the automatic caller share the same authorization and logical slot `(submission_ref, review_policy_generation, review_slot)`. An enabled automation is sufficient standing instruction if its owner already has review-assignment rights; there is no additional mandatory automation grant.

A review assignment contains exact Task revision, Attempt, submission/candidate, commit/tree, acceptance phase, canonical source index, coverage, relevant prior findings and result schema. No whole-fleet history or mutable source pointer is included.

Record both who assigned it and who performs it. The owning manager gets the assignment/cost attribution; the auditor remains a distinct authenticated participant. A script or service cannot submit a pass merely because it arranged the review.

Multiple auditors have separate slots. Check producer-lineage and permission separation where required. A different actor ID or the same model under a different display name is not proof of independent statistical evidence. Model diversity may be a preference, not a fixed-version requirement.

### 5.2 `review.submit`

The assigned auditor supplies an anchored result, for example:

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

Body identities must match the authenticated assignment. `pass`, `changes_requested` and `inconclusive` are distinct; valid JSON alone proves no substantive correctness.

Pass requires complete assigned coverage and no unresolved blocker. Missing source/evidence, unexecuted required checks, quota failure or unknown candidate cannot pass. Change requests identify actual requirement violations, not invented requirements or unconfigured style demands. Inconclusive preserves the gap without asking a writer to rewrite correct code.

Retain late results for their original candidate. They cannot return, approve or publish a newer submission. Findings are explicitly retracted/superseded; do not erase their original evidence. Disabled audit dispatch does not prevent an already assigned auditor from delivering its result.

### 5.3 Local `audited` fact

The server derives audited state from all required review slots/coverage/checks for the exact submission and phase. Required count must not silently become zero. A code-phase audit does not claim later integration/live tests ran.

Review retraction, changed source, new candidate or invalidated evidence updates current eligibility while preserving history. Corrected source with unchanged checklist bytes is reviewable; whitespace-only checklist edits cannot remove a defect.

Local audited state may update without automation. Remote labels/Checks and publication are separate effects and require a manager command or an enabled matching automation.

## 6. Findings, return and repair

Current `request_changes` requires GM/operator and creates mail, not native input. Where the product intends scoped manager feedback, implement that shared capability for manual managers and their automations using existing exact Task/Attempt/submission/candidate guards. Do not create an internal service bypass or give reviewers GM credentials.

The manager can read the findings, apply relevant feedback and then request correction. Alternatively, an enabled reviewed-delivery automation may include `review_disposition` and/or `repair_dispatch`. These are selections inside that definition, not globally gated stages. Selecting disposition alone does not start repair.

The automation's owner remains responsible for the handoff. Repair goes to the actual owning manager with exact findings/requirements; that manager directs their writers. Do not overwrite work ownership with the automation sponsor's ID. A dispatcher authorized for a managed pool still must respect current Task/Attempt ownership.

Preserve an unreleased valid Attempt; a new submission references the prior submission. Changed Task revision or ownership uses the existing new-Attempt path. Fallback to another route/manager occurs only as configured and after prior native work, children, lease and useful partial output are reconciled.

Infrastructure and provider failures are not code defects. Repeated unchanged findings yield one focused diagnostic/manager decision rather than identical prompts. New evidence or corrected code permits progress. No arbitrary fixed retry-round limit declares unfinished work complete.

Re-review after a correction uses the currently enabled audit handoff, or a manual review assignment. Disabling a handoff does not remove existing findings or permit acceptance of a known unresolved defect.

## 7. Acceptance and publication choices

The manager decides which automatic steps to include. With `publication` absent, publication stays manual. With it present in an enabled automation, the server publishes on the owner's behalf only after all actual candidate, audit, acceptance, permission and repository checks pass. No separate `manager_gate/auto_after_audit` switch or global mode is needed.

Acceptance can remain manual while publication is automated after that exact acceptance. Conversely, valid accepted work can wait for a manual push. Editing the automation does not falsify audited/accepted evidence.

The automation may use only the rights its owning manager has. Existing GM/operator-only paths are not made available by an enabled flag. When scoped publication delegation is part of accepted policy, manual and automatic callers share that capability and current epoch check; otherwise return the specific missing right.

A manual approval addresses exact candidate, target and effect. Changes to head/evidence require reevaluation. An already-started or uncertain equivalent push returns its retained reference; disabling/re-enabling cannot generate a fresh identity for it.

Audit, acceptance, upload, merge, publication and Issue closure remain different facts. A failed label update cannot rerun push. A publication problem does not turn correctly audited code into another implementation task.

## 8. Local-first review and remote CI

Local reviewers can inspect captured source before any push. There is no first-push/audit circular dependency.

If the manager selects remote CI, `forge.upload_candidate` uploads the captured candidate only to a configured review-branch namespace, under its actual upload right. It is transport, not accepted publication. It cannot target protected output refs, bypass checks, close Issues or claim acceptance.

A manual upload is one requested command. Automatic upload must be an explicit action of an enabled automation. Show that remote review needs this pre-review write; do not hide it as read-only setup. Later CI results are recorded, but only a configured valid publication action can merge/publish.

These paths are Rust implementations; no internal Python/PowerShell or shell `gh` control daemon is required.

## 9. GitHub integration

### 9.1 Observation and credential identity

Use one Rust API/rate boundary and trusted local repository/credential handles. Reads, labels, PRs, Checks and Contents permissions are distinct. Inventory what the selected App/token can actually do; local work need not wait for unused integrations.

Verify webhook HMAC over raw bytes, validate source context, durably ingest before ACK and deduplicate. Reconcile unordered/missing events with conditional paged reads and shared account rate budgets. Header strings alone cannot authorize actions. Permission errors are not proof of deletion.

A source event matches a manager's configured trigger; it never chooses its owner or credentials. Source text cannot change frozen Task requirements or automations. A bot quota warning is not a failed code review.

"On behalf of manager M" is ELIOT's execution/authorization attribution. GitHub still records its actual authenticated App/user/token actor. Do not forge a human commit author, disguise the App as M, or claim several internal auditors are several independent GitHub user approvals.

### 9.2 Optional projections

Managed audited labels, exact-commit audit/acceptance Checks, PR summary and actionable annotations are writes. They follow explicit manager commands or an enabled `github_projection` step on that manager's behalf. Preserve unrelated labels/comments.

Only complete positive evidence maps to success. GitHub's handling of neutral/skipped required checks is not an ELIOT pass; do not use those conclusions to hide an inconclusive audit. Use only conclusions allowed by the selected API.

Retain exact head SHA, Check Run IDs and correlation. `external_id` is not guaranteed uniqueness. Reconcile ambiguous creation; annotation updates may append, requiring batch identity/readback. Labels are not atomically commit-bound and never authorize publication. Show projection lag instead of trusting a stale label on a new head.

Issue closure needs the manager's explicit configured completion/closure action. Code review alone is insufficient.

### 9.3 Push, merge and integration base

Native GitHub merge queue is optional and repository/plan-dependent. ELIOT's own Rust admission queue must work without it. Run expensive integration/check/review work outside Store transactions; serialize only the relevant final effect target.

Existing `forge.publish_ref` publishes the exact accepted candidate with ordinary non-force Git behavior. Its old-ref preflight is not atomic CAS. Incompatible movement requires refreshed integration/validation, not force-push. Readback proves observed remote state, not global ordering over all external writers.

PR merge must use the actual supported endpoint/strategy, expected head where supported and no protection bypass. Expected head does not guard arbitrary base movement. Verify the actual integration candidate through the selected qualified strict-up-to-date/merge-group path. A local mutex or manager click cannot create a missing base-CAS guarantee.

Where an asynchronous request or native queue is supported, retain request/options and distinguish accepted/enqueued from merged. Expired or conflicting request state needs PR/ref/commit reconciliation, not blind re-merge. Do not count a synthetic pre-merge commit as landed or include unapproved stacked PRs.

GitHub Actions trigger behavior depends on token and event; missing expected checks stay visible. Never execute untrusted PR code with privileged workflow credentials to force a green gate. Validate current endpoint schemas/capabilities when implementing rather than freeze research-era assumptions.

### 9.4 Disable and recovery

Disabling an ELIOT automation stops its future local starts, not an already accepted external workflow, auto-merge or native Goal. Keep exact IDs and actual owner/disposition visible. Read-only reconciliation continues; supported cancellation is a separate requested operation.

Persist effect intent before I/O. Timeout, lost response or unknown process-tree disposition is not evidence that nothing happened. New manual/automatic requests cannot repeat an unknown effect under another ID.

PR/comment/check creation has no general exactly-once guarantee. Retain remote IDs, serialize equivalent local intent and reconcile ambiguity; do not claim a marker excludes all duplicates from external actors. A partial failure blocks only the affected effect, not the entire project.

## 10. Completion and next work

Recording an already issued result continues regardless of enabled state. Further bookkeeping, closure, cleanup and next-task dispatch occur only when directly requested or included in a currently enabled automation. No branch disappearance authorizes deletion of live/unresolved work.

After its candidate and native children/continuation are settled, the owning manager may prepare the next task in the same worktree. An enabled distribution automation can make that next assignment for its owner; otherwise the manager chooses it. Do not modify a candidate still in review.

## 11. Required scenarios

- With no enabled entries, a manager completes the full manual review/repair/publication path.
- One manager enables only audit assignment in one configuration request; no Root/control-mode/grant-creation round is introduced.
- Automatic handoff records its manager, while auditor results retain their real author.
- Manager commands remain usable alongside enabled automations; equivalent concurrent slots execute once.
- A scope-limited manager cannot publish or modify another manager's work by enabling an automation.
- Applied submission triggers the selected review; queued/failed submit or duplicate Git events do not.
- Disabling stops unstarted/follow-up automatic effects while started work and result recording remain visible.
- A held never-started step can be handled manually without enabling its automation or duplicating it.
- Restart preserves saved choices, reviews current ownership and reconciles uncertainty before continuing.
- Late candidate A feedback cannot mutate candidate B; corrected code remains reviewable with unchanged checklist text.
- Inconclusive or provider failure never becomes pass or pointless code repair.
- Local-first audit has no push dependency cycle; remote-review upload is an explicit distinct action.
- Stale labels cannot authorize publication, and projection failure cannot rerun it.
- Built-in delivery requires neither an external-script interpreter nor a fixed software release or native GitHub merge queue.
