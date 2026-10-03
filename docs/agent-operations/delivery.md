# Delivery — Manual Commands and Manager-Owned Automations

Revision 5 · 2026-10-03 · proposed Rust implementation, not shipped behavior.

[Configuration](configuration.md) owns manager settings; [Architecture](architecture.md) owns admission, durable dispatch and readback. One set of handlers serves manual commands and enabled automations.

## 1. Delivery without another Task ledger

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

These names are projections over existing Task/Attempt/Operation/submission records. The manager chooses each action, or enables the particular handoffs they want performed on their behalf. Missing steps remain manual. No global mode or automatic increase in autonomy with agent count.

Normal waiting for the manager, auditor capacity, a peer fact or remote readback is not a code defect. The system must provide the next useful action without repetitively prompting Root.

## 2. Manual controls and automatic equivalents

| Explicit method/action | Finite effect; no implicit downstream workflow |
|---|---|
| Queue/Task/assignment read | Current source, status and ownership; no claim. |
| `swarm.launch` or existing Task/runtime command | One selected assignment and its declared prerequisites. |
| Inspect stream, exact steer/reply | Observe/direct named work; no unrelated restart. |
| Source capture and `task.submit` | One retained candidate/submission when the operation applies. |
| `review.assign` | One exact review slot and explicitly requested review work. |
| Assigned `review.submit` | Authenticated findings/verdict, not Task feedback, repair or push. |
| Manager `task.request_changes` | Applicable exact-candidate disposition. |
| Owner continuation | Work on the correction, not an unbounded default repair loop. |
| Exact acceptance and forge method | The permitted effect and its readback, not the next Task. |

Manual use needs no automation object or GitHub App when the action is local. Existing role, scope, candidate and host guards still apply. Automatic callers exercise the owning manager's current rights through the same handlers, never a stronger internal-service role.

## 3. Queue and ownership

Task source identity is `(forge instance, immutable repository ID, external item ID, item kind)`. Issue and PR kinds remain distinct. Selected source changes make revisions, not duplicate Tasks per webhook; `updated_at` is not a CAS version.

Keep the Issue body, selected normative comments and named canonical documents addressable. A generated brief is a projection, not a substitute specification. Preserve unknown potentially normative comments as gaps. Source prose, bot output and labels do not choose credentials or configuration.

`github.work_pool.preview/apply` selects/orders existing Tasks. Import and queue reads do not dispatch. The manager may enable distribution for that pool, not arbitrary repository work.

For direct or automatic dispatch, recheck current Task/revision, relevant pool membership, dependencies, owner, route and capacity, then reserve the existing Task/Attempt, manager and workspace slot before external launch. The assignment packet states exact work, sources, relevant peers and constraints. A stale queue snapshot or heartbeat does not steal ownership.

One manager owns one mutable worktree and one in-flight product candidate. Writers work on non-overlapping parts of that Issue and return their changes for integration; they do not publish separate fragments. A pool may contain several authorized managers without merging their workspace ownership.

Filter readiness before page limits and keep manager order unless they chose ranking. Capacity waits retain the subject and wake when capacity changes, not repeatedly start rejected jobs. Routes sharing a provider account share its budget. Ordinary local review/delivery must work without GitHub setup or optional script interpreters.

## 4. Exact triggers, not a chain that must have started automatically

Each selected step observes its committed prerequisite independently:

| Step | Trigger/subject | Result |
|---|---|---|
| `review_dispatch` | Applied submission; unfilled required review slot | `review.assign` for that candidate. |
| `review_disposition` | Assigned actionable findings applicable to the current candidate | Existing guarded feedback transition. |
| `repair_dispatch` | Applied current feedback and reconciled owner/continuation | One correction delivery to the actual owner. |
| `acceptance` | Complete required audit/check evidence | Exact-candidate acceptance under current rights. |
| `publication` | Actual exact acceptance and eligible target | Selected forge effect/readback. |
| `github_projection` | Retained audit/acceptance/publication fact | Only configured remote labels/Checks/summary. |

Thus a manual submission can trigger an enabled audit handoff. A manual acceptance can trigger enabled publication without automatic acceptance. A repaired candidate can trigger re-review even though it descends from the same automation. No step can synthesize an absent prerequisite or enable an omitted step.

Atomically retain each considered subject plus its Operation/reservation or pending reason and cursor. On restart, a missed in-memory notification cannot lose the submission. Including existing eligible work at enablement uses the same slots as new events. Local Git and webhook reports of the same verified commit coalesce for the same intended notification; commit observation is not submission success.

## 5. Submission and source freeze

Source anchors are `src/store/submissions.rs::reserve/begin/finish/document/describe`. Queued `task.submit` is not applied `task.submission`; failed/partial publication of the submission document starts no audit.

The manager integrates the complete intended Issue candidate and executes only its configured phase gate. Every requirement retains a disposition. Missing wiring is not an excuse for a nominally complete fragment. Partial delivery follows explicit project policy, not an automatic escape hatch.

Candidate evidence identifies retained source/tree, baseline, relevant untracked treatment, claims and checks, not a branch name. Freeze the candidate worktree during review/publication. Auditors use retained source or a controlled read-only view, never mutable latest branch state selected by name.

An applied return opens a correction phase for the current owner. Capture the old candidate/evidence first. Ensure processes inspecting a mutable checkout have been safely finished or moved to retained read-only inputs before changing it. An auditor still reading immutable candidate A may finish historically; it cannot approve later B. No forced session kill is implied. After correction, capture and apply a new submission before reviewing the changed code.

## 6. Review assignment, verdict and audited state

### 6.1 Assignment and retries

Direct manager and automation reserve the same `(submission_ref, review_policy_generation, review_slot)`. The policy generation identifies coverage/acceptance semantics, not the preferred model string. A profile rename, extra automation or random request ID cannot start a second auditor for that slot.

Assignment contains exact Task revision, Attempt, submission/candidate, commit/tree, phase, canonical sources, required coverage and output schema. Record manager sponsor and actual assigned auditor separately. Several auditors occupy declared separate slots, not cloned identities used to simulate independent evidence.

A failed/inconclusive attempt is not a permanently occupied slot, but timeout alone is not proof the worker stopped. A manager or configured recovery action may replace it only after actual prior native/process/continuation disposition is known. The replacement is a new recorded review attempt in the same logical slot, preserving earlier output and rejecting late output as the replacement's result.

### 6.2 Result

Example proposed payload; identifiers must match the authenticated review assignment:

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

`pass`, `changes_requested` and `inconclusive` are distinct. Valid JSON is not proof of correctness. A pass needs complete assigned coverage and no unresolved blocker. Missing sources, unexecuted required checks, empty evidence, provider failure or unknown candidate cannot pass.

A change request names a demonstrated violation of real requirements, with actionable evidence, not invented style demands or mechanisms. Inconclusive preserves uncertainty rather than sending correct code back for pointless rewriting. A supported critique inside a nominally positive narrative must not be discarded merely because its summary says pass.

A completed assigned review may report after automation disable. Retain late findings against their original candidate and mark historical applicability. Explicit credential revocation still applies; trusted adapter recovery must not impersonate the auditor. Retraction/supersession is explicit history, never deletion of the original finding.

### 6.3 Audited

The server derives `audited` from all required slots, coverage and checks for the exact candidate and audit phase. Count cannot silently become zero. Code review does not claim deferred integration/live tests were executed. Review retraction, invalidated evidence or changed candidate updates eligibility while preserving history.

A new submission ID containing the same relevant source/evidence does not remove an old defect. Corrected source with unchanged checklist text is eligible for re-review. Link the finding to its affected source/evidence, not checklist bytes or timestamps alone. A manual diagnostic rerun remains possible and is explicitly labelled; it cannot manufacture a green verdict.

Local audit aggregation works without enabled automation. Labels, acceptance and publication remain separate actions. The manager-owned handoff cannot fabricate the auditor's signed/authenticated result.

## 7. Return and repair

Current `task.request_changes` uses GM/operator authority and stores mail, not native input. Add intended scoped manager feedback rights in that existing guarded path for both manual and automatic callers. Do not give auditors GM credentials or treat an MCP tool annotation as permission.

The auditor calls `review.submit`. The manager or selected `review_disposition` applies relevant findings. Only explicit owner input or selected `repair_dispatch` starts correction. Feedback admission alone is not proof the correction reached the model; retain exact delivery/readback and retry semantics.

Repair targets the actual current Attempt owner with exact findings and requirement IDs, not the automation sponsor as a replacement owner. The owner manages internal writers. Keep a valid unreleased Attempt; new submission references the prior submission. Changed Task revision/owner follows normal new-Attempt policy.

A configured fallback/reassignment requires prior native family and workspace disposition plus preserved partial output. Infrastructure failure is not a source defect. Repeated unchanged findings yield one focused pending diagnostic, not identical model prompts. Corrected source, new relevant evidence or an explicit permitted diagnostic decision can proceed; there is no arbitrary number of returns that declares unfinished work complete.

## 8. Acceptance, upload and publication

Selecting/omitting acceptance and publication steps expresses the manager's choices; no extra manager-gate mode. Publication can be automatic after manual acceptance, or accepted work can wait for a manual push. Both use the same exact-candidate, current rights, GM/epoch and repository policy checks.

A local audit requires no first push. When remote CI is selected, `forge.upload_candidate` performs an explicitly permitted pre-review upload into its configured review namespace. It is not accepted publication and cannot write protected output refs, close Issues or bypass checks. Later CI output is evidence, not auto-merge authority by itself.

A manual approval binds one candidate, target and effect. Head/evidence changes require revalidation. An equivalent sending/unknown effect returns its retained reference, never a blind replacement. Disabling/re-enabling or changing model preferences does not reset its slot.

Audit, acceptance, upload, push, PR merge, Issue closure and cleanup remain separate facts. Failure to write an audited label cannot rerun publication or ask the writer to rebuild valid code.

## 9. GitHub boundary

### Intake and feedback loops

Use one Rust client/rate boundary with private local repository/credential handles. Inventory actual read/Issue/PR/Check/Contents permissions. Raw-body HMAC, repository/installation validation, durable intake before ACK, dedupe and conditional paged reconciliation handle missing/duplicate/unordered webhooks. Permission failure is not proven deletion.

The event does not choose its owner, credentials or executable policy. ELIOT-generated labels/Checks/comments are correlated projections; their webhook echoes must not revise Task requirements or trigger another identical action. Do not ignore every bot/user comment by username: preserve genuinely new selected normative content and uncertain-source gaps. Frozen requirements change only through the normal authorized Task revision path.

External GitHub author is the authenticated App/user. Internal on-behalf attribution is not a forged human actor or multiple independent GitHub approvals.

### Projection writes

Only configured manager commands/automation write managed labels, commit-specific Checks, summaries and annotations. Preserve unrelated labels/comments. Use exact head SHA, retained remote IDs and batch/readback identity. `external_id` is correlation, not an exactly-once guarantee; annotation updates can append.

Map only complete positive evidence to success. Neutral/skipped GitHub conclusions are not a passed ELIOT audit. Stale labels cannot authorize a new candidate. Code audit alone cannot close an Issue; closure needs its explicit completion policy/action.

### Push, merge and retries

Reuse accepted non-force `forge.publish_ref` and owned Git process readback. Its preflight old-ref check is not atomic CAS. Refresh integration on incompatible movement rather than force-push or weaken verification. A local mutex cannot exclude independent remote writers.

PR merge uses the actual supported endpoint/strategy, expected head where available and no protection bypass. Expected PR head is not an arbitrary base guard. Strict integrated-candidate evidence needs the selected qualified up-to-date/merge-group mechanism; optional GitHub merge queue availability is not a prerequisite for local work. Never claim a guarantee an endpoint cannot enforce.

Accepted/enqueued remote requests are not merged. Retain exact options/IDs and reconcile actual ref/PR/commit state; a synthetic pre-merge SHA is not landed evidence. Do not include unapproved stacked work or dismiss required human reviews automatically.

There is one retry decision owner per external effect. Review the Rust HTTP client's middleware as well as the action worker: it must not invisibly resend ambiguous non-idempotent requests underneath Store. Safe read retries may be bounded/shared; write timeout or disconnect requires effect-specific reconciliation. Marker text and client request IDs do not create a universal GitHub exactly-once guarantee.

Account-level rate pacing uses actual response/reset information. Do not treat every HTTP 429 as the same quota condition or issue a new model job for a provider/account outage. Actions workflow triggering depends on credential/event semantics; missing expected checks remain visible. Never run untrusted PR code with privileged workflow credentials to force a green check.

### Disable and recovery

Local disable does not cancel a remote workflow, auto-merge or native Goal already accepted elsewhere. Track actual IDs and ownership; read-only reconciliation continues. Supported cancellation is a separate manager decision.

Retain intent before I/O and preserve unknown process-tree/effect state. New automatic/manual requests cannot clear uncertainty by inventing a new ID. Isolate an unresolved target; unrelated local work and observation remain usable.

## 10. Next task and complete handoff

Record already-issued outcomes regardless of enabled state. Further bookkeeping, closure, cleanup and next-task distribution require an explicit command or the corresponding currently enabled choice.

Only after its candidate and conflicting native family/continuation are settled may the manager prepare the next Task in the same worktree. No age-based cleanup or branch disappearance removes pending submissions. Writers' useful output must be integrated into the candidate or explicitly retained/dispositioned, not silently abandoned.

A correct first product path is one local manager -> writer -> applied submission -> assigned auditor -> actionable return -> repaired submission -> pass -> authorized acceptance/publication. Every arrow has an actual producer, handler and result reader. Remote GitHub, scripts, cron and larger fleets are additional adapters/triggers, not prerequisites for this path.
