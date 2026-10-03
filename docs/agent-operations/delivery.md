# Delivery Contract — Queue, Audit, Repair and GitHub

Revision 2 · 2026-10-03 · proposed Rust implementation, not shipped functionality.

[Architecture](architecture.md) owns services and recovery. [Configuration](configuration.md) provides the agent-facing preset. [Donor map](donor-map.md) contains the official GitHub/source references behind the external guarantees used below.

## 1. The ordinary workflow

The manager sets work order, eligible roles/runtimes, review policy and publication mode once. ELIOT performs the following without requesting a fresh manager message at every arrow:

```text
ready -> assigned -> working -> submitted -> reviewing
                                             |
                      +----------------------+------------------+
                      |                      |                  |
              changes_requested           audited          inconclusive
                      |                      |                  |
                  repairing          auto_after_audit      review/diagnosis
                      |                or manager_gate
                 new submission              |
                                         accepted
                                             |
                                  publishing -> published
```

These names are a compact delivery projection. They are not a second mutable Task ledger. Task/Attempt/Operation and immutable submission identities remain authoritative. A separate `waiting_reason` describes dependency, route capacity, audit capacity, native input, manager decision, source freshness, publication rules or unknown outcome.

A completed native turn means neither a submitted candidate nor a successful audit. A GitHub label, comment or PR merge means neither Task acceptance nor verified completeness.

## 2. Work pool and distribution

### 2.1 Stable import identity

The stable Task origin is `(forge instance, immutable repository ID, external item ID, item kind)`. A new Issue body/comment selection produces a source/Task revision, not a new Task for every webhook or timestamp. PRs and Issues are distinguished even though GitHub uses a shared number space. Source `updated_at` is not a unique revision or CAS token.

The selected source index includes the Issue body, relevant source comments and canonical documents. Generated briefs are bounded projections with readable source references. No hidden wholesale deletion of unknown comments; unclassified potentially normative changes remain visible to the assigning manager. A comment is not executable workflow policy.

A pool references existing Tasks with manager-supplied order and dependency facts. It is not another editable database of issue status. `github.work_pool.preview/apply` imports/updates the selection idempotently. `swarm.queue.get` reads it. The Rust distributor reevaluates readiness after relevant Task, capacity or review transitions.

### 2.2 Admission transaction

For each eligible item:

1. Read current Task revision, pool membership, dependencies and existing owner.
2. Select an authorized available manager/executor profile without overriding manager order. Filter unready work before page limits so a page of blocked items cannot hide ready work.
3. Reserve the manager slot, Task/Attempt lineage and mutable workspace scope in one Store admission. A queue read is not a reservation.
4. Create the existing launch/dispatch Operation and compact assignment context; native work starts outside the transaction.
5. Publish current assignment and code-scope ownership to the shared #22 neighborhood/overlap projection.

Already assigned work is not stolen because a heartbeat expired. Unknown prior execution must be reconciled before another writer can own that scope. Native children remain under their family's recorded lifecycle owner.

One manager owns one mutable worktree and one in-flight product submission. Parallel writers get non-overlapping portions of that manager's current Issue; they do not create independent product Tasks/worktrees or push individual fragments. Completion of a writer returns its result to the existing manager assignment for integration. A server-assigned manager may supervise the queue while the General Manager is disconnected.

The manager's intended order is the default. Blocker-first reordering, original-owner repair preference and starvation protection are explicit policy, not a hidden model choice. Missing capacity yields a precise wait reason, not repeated launches. An unavailable route does not retain unrelated Tasks indefinitely.

## 3. Submission is the handoff boundary

Current source anchors: `src/store/submissions.rs::reserve`, `begin`, `finish`, `document`, `describe`.

`task.submit` first returns a queued Operation. The review trigger is its **applied `task.submission` observation** with a valid `submission_ref`, not the request receipt. Failed submission publication starts no audit.

Before submission, the manager integrates its writers, commits/captures the intended complete Issue candidate and executes only the configured current-phase checks. Broad tests remain in their declared acceptance phase. Every requirement has a disposition; omitted requirements are not implicitly satisfied. Exceptional partial delivery must be an explicit project-policy decision, not the writer's escape hatch.

The candidate includes the exact source tree and relevant untracked-source treatment, baseline, submission claims and evidence. It does not contain mutable references to whatever happens to be on the branch later. The worktree remains frozen while that candidate is in review/publication. A reviewer can use immutable captured source or a controlled read-only view; it does not need another writable worktree per comment.

## 4. Reviewer assignment and result

### 4.1 Dispatch

`review.assign` is a typed application operation exposed in the deferred review group and normally called by the workflow service. It reserves one required review slot for one exact submission and eligible reviewer. Logical uniqueness is `(submission_ref, review_policy_generation, review_slot)`; transport retry returns the same assignment.

The assignment includes Task revision, Attempt, submission/candidate references, source commit/tree, acceptance phase, source index, review instructions, required coverage, prior actionable findings and result schema. It does not include the entire fleet or transcript. The reviewer cannot edit that evidence, change review requirements or approve its own implementation.

Multiple auditors receive separate slots. Parallelism is configurable. A new actor ID alone does not establish independence: check role separation, producer lineage and credential/workspace permissions. Different models/providers can be preferred, but using the same model family is not falsely advertised as independent statistical evidence.

### 4.2 `review.submit`

Illustrative logical payload; real identifier values come from the review assignment:

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

Caller identity and Task/Attempt/source identities are resolved from the authenticated assignment; body fields must match them. Verdict is `pass`, `changes_requested` or `inconclusive`. Structured validation is necessary but does not prove the review's substantive correctness.

`pass` requires all assigned coverage and no unresolved blocking finding. An empty result, missing evidence, unexecuted required check, unknown candidate or a provider error cannot become pass. `changes_requested` needs actionable findings tied to actual requirements/evidence; invented requirements and stylistic preferences outside policy are not automatic blockers. `inconclusive` retains the gap and can route to another qualified reviewer or diagnosis without making the writer redo unchanged code.

Only the assigned reviewer can submit its slot. A late result is retained against the old submission, not applied to a newer head. Changes to an existing finding are explicit supersession/retraction records; do not silently mutate its original evidence.

### 4.3 Aggregate verdict and `audited`

The server evaluates the required review policy, not majority agreement in chat. `audited` means all required slots and coverage for this exact submission have passed, required checks for the named phase are satisfied, and there is no unresolved blocking finding. Required slot count cannot default to zero by accident.

Record `audited_scope` such as code review separately from test execution or publication readiness. A code-phase audit may be valid while later integration/live checks remain pending; it does not claim those later checks passed. Acceptance uses the actual project's phase policy.

Re-evaluate eligibility after a new submission, material source change, review retraction, invalidated check evidence or incompatible review-policy change. Old successful audit history remains; it does not grant the new candidate audited state. Whitespace changes in a checklist do not remove a real defect, and unchanged checklist bytes do not permanently block a corrected source tree.

## 5. Automatic return and repair

Current `src/store/submissions.rs::request_changes` is GM/operator-only. The implementation must add an explicit scoped authorization branch for the workflow service applying an assigned review disposition. Keep its existing exact Task revision/Attempt/submission/candidate checks and stale-review behavior. Do not distribute a GM credential to auditors or pretend the existing reviewer surface already permits this call.

In one durable decision, record the review outcome, its applicable disposition and a pending repair handoff. Use the existing Task/Attempt feedback transition. `task.request_changes` creates mail but sends no native input; a separately admitted `continue_assignment`/launch action delivers the correction under the manager's standing execution grant.

Repair is normally offered to the existing owning manager, with exact findings and affected requirement IDs. That manager directs its writers. If its route is unavailable, the dispatcher may choose an explicitly authorized fallback only after prior execution, children, lease and useful partial results are reconciled. It never wakes an old session by branch name.

If the same unreleased Attempt remains valid, preserve it and submit a new candidate using `expected_submission_ref` for the old submission. If Task revision or ownership actually changes, use the existing new-Attempt path; do not rewrite an old immutable Attempt.

Infrastructure failures, quota errors and source-read failures are not code defects. Retry read-only collection/review under its contract, or park with a precise reason. A repeated unresolved finding with unchanged relevant code/inputs triggers a focused diagnostic or manager exception rather than automatic identical prompts. New evidence or corrected code permits progress. Budgets and escalation thresholds are configurable; no fixed two-round or ninety-minute rule turns unfinished work into success.

## 6. Automatic publication versus manager gate

The same preset has two modes:

- `auto_after_audit`: eligible audited work proceeds through required phase checks, delegated exact-candidate acceptance and the configured publication action.
- `manager_gate`: create one decision item containing candidate, audit evidence, remaining conditions and proposed effect. An authorized manager/GM approves or returns it; no continuous polling questions are sent to that model.

Managers can enable these modes only within their delegable permissions. Automatic acceptance/publication needs an explicit project-scoped grant from the appropriate authority. The service rechecks current grant, candidate, policy, target, repository rules and relevant GM epoch. The setting is not authority by itself.

The public existing acceptance/forge methods retain their restricted path. Add a narrow delegated execution path to the same underlying guarded transitions, not `Role::Manager => allow all`. The service must not self-grant, mutate protections or evade an epoch change. Unknown in-flight effects remain readback-only across handover.

Successful audit, Task acceptance, remote upload, merge, final publication and Issue bookkeeping are separately recorded. Publication failure does not force reimplementation or revoke a valid audit. A failed label update does not repeat push/merge. Closing an Issue requires configured completion/closure evidence, not merely an audited label.

## 7. Local audit before push; remote checks when needed

There must be no circular requirement that a commit be pushed to obtain its first audit while push requires that same remote audit.

**Local-first route:** reviewers inspect captured local source; ELIOT records audited state; configured acceptance and authorized publication follow. Remote Checks/labels can be projected after the commit becomes available on GitHub.

**Remote-review route:** an explicit `forge.upload_candidate` operation uploads a captured candidate only to a configured review-branch namespace. This is transport for CI/review, not accepted publication, and has distinct authority from existing `forge.publish_ref`. It cannot target protected output refs, close Issues, bypass checks or claim acceptance. PR/check review then determines readiness for final publication/merge.

The preset selects the route deliberately. Remote review is optional, not a mandatory pre-push tax. Built-in Rust handles both; no PowerShell GitHub daemon or `gh` script is required. Existing native Git credentials are resolved only at the trusted process boundary.

## 8. GitHub integration contract

### 8.1 Authentication, intake and reconciliation

Prefer a GitHub App installation for unattended work, with project-local credential references and only required permissions. Reads, Issue/label writes, PR writes, Checks writes and Contents/publication rights are separately reported. A selected personal-token setup may use endpoints it actually supports; do not promise identical Checks behavior for every token type.

Verify webhook HMAC over raw bytes, then validate event/body, installation/repository and allowed action. Durable intake precedes acknowledgement. Deduplicate delivery identity; re-delivery is not new work. Event headers alone are not signed authority. Track source health and reconcile unordered/missed events with paged conditional reads. Transient 404/permission loss is not a proved deletion.

One Rust GitHub adapter services work pools, reviews and projections. Coordinate rate limits per credential/installation, honor retry/reset evidence, use bounded backoff and do not repeatedly create comments on every status tick. Never parse a bot quota warning as review failure or an `@reviewer` mention as dispatch authority.

### 8.2 Remote projection, not remote authority

The controller may expose:

| Remote surface | Meaning |
|---|---|
| Managed `audited` label | Convenient current display, not an authorization input |
| `eliot/audit` check on exact commit | Projection of configured audit coverage/verdict |
| `eliot/acceptance` check when configured | Separate declared acceptance evidence |
| One managed PR summary | Current candidate, auditor, findings and publication state |
| Review comments | Exact actionable source findings, not ordinary coordination chatter |

Only positive complete evidence maps to success. `inconclusive` uses an explicit non-success/action-required result, never `neutral` or `skipped` as a fake pass: GitHub may treat neutral/skipped required checks as acceptable. Keep ELIOT's internal gate authoritative. Only GitHub may set its special stale conclusion; do not invent that API write.

Use `head_sha`, recorded Check Run ID and `external_id` to correlate the result. `external_id` is correlation data, not a GitHub uniqueness constraint. On an ambiguous creation response reconcile by exact commit/app/name/external ID before creating again. Annotation updates append; retain sent-batch identity or read existing annotations before retrying a possibly applied batch.

A label cannot be atomically bound to a commit. A stale label may remain briefly during network loss; the local dashboard reports projection lag, and publication never trusts the label. Update only ELIOT-managed labels/comments, preserving unrelated user content. Delayed projection of an old candidate cannot mark the new head audited.

Several internal auditors sharing one GitHub App do not become several independent GitHub user approvals. Do not auto-dismiss required human reviews or claim an App can approve its own authored PR on behalf of a separate human.

### 8.3 Publication and merge

Expose capabilities actually available for the repository; native GitHub merge queue is optional and is not available for every ownership/plan combination. This repository is currently user-owned, so do not make organization-only merge queues a prerequisite for the product.

ELIOT maintains its own Rust publication admission queue. Expensive source integration/check/review work occurs outside short database transactions. Only the final relevant target publication is serialized; a slow audit on one repository does not block all others.

For native `forge.publish_ref`, use the exact accepted integrated candidate and ordinary non-force Git behavior. If the remote moves incompatibly, refresh/reintegrate and revalidate rather than force-pushing. Existing preflight is not atomic expected-old CAS; preserve that documented limit. Readback proves what commit was published, not an earlier claim about total ordering of all third-party writers.

For PR publication, use the supported GitHub API with expected head SHA and `bypass_rules=false`. Prefer the documented asynchronous merge API when available; retain its request UUID and distinguish accepted/enqueued/merged. A pending-request conflict is reconciled against the stored request options, not retried as a different merge. Stacked operations may affect downstack PRs; reject an unapproved wider scope.

A head guard does not guard the base. Required integrated verification must address the actual integration candidate using a qualified strict-up-to-date check path, or GitHub merge-group checks where available. A local lock alone does not exclude external GitHub writers. A manager click does not magically supply missing base-CAS guarantees. If an exact guarantee cannot be enforced by the selected path, show that limitation and choose another authorized publication path; do not silently weaken it.

Where a GitHub merge queue is used, handle `merge_group` and its own commit, not just `pull_request`/`push`. Enqueue acknowledgement is not merge completion. Never mark a PR's synthetic pre-merge `merge_commit_sha` as an actual landed commit.

If using GitHub Actions in the integration, verify how the chosen token triggers downstream workflows. A push by `GITHUB_TOKEN` is not equivalent to an App-token push; absence of an expected check must not deadlock invisibly or be counted as success. Never execute untrusted PR code with a privileged `pull_request_target` context merely to obtain write access.

### 8.4 Recovery and no hidden replay

Persist intended effect and target before I/O. Read exact remote ref/PR/check state after a lost response. Unknown push/merge or unconfirmed process-tree cleanup holds the affected effect scope. Timeout is not proof that nothing happened.

Creation of PRs/comments/checks has no general exactly-once guarantee. Serialize ELIOT intent, attach correlation data, retain remote IDs and reconcile ambiguity. Do not claim a local receipt or marker prevents every external duplicate. Repeating read-only reconciliation is distinct from replaying an external write.

## 9. Finishing and moving on

After publication is actually confirmed, apply configured labels/summary/Issue bookkeeping once. Protect unresolved submissions and active workspaces from cleanup. A deleted review branch is not permission to delete local work still in use.

Only after the previous candidate is settled and children/continuation are resolved may the manager prepare its next branch in the same worktree. A fresh writer context receives the next Issue and relevant contracts, not the previous entire conversation. Unrelated Tasks are not mixed into a candidate being audited.

## 10. Essential acceptance scenarios

- Manager supplies a queue once; several authorized managers complete eligible Issues without Root relaying every handoff.
- Simultaneous dispatchers reserve one Task once; blocked rows do not hide ready work.
- Applied submission starts one review; queued/failed submission starts none.
- Actionable audit returns to the owner; mail storage alone is not mistaken for delivered native input.
- Late result for candidate A cannot hold or audit candidate B.
- Corrected code with unchanged checklist text is reviewable; whitespace-only edits do not defeat an unresolved finding.
- Inconclusive/provider failure does not become green or trigger pointless writer repair.
- Auto-publication works inside a standing grant; manager-gate mode presents one exact decision.
- Internal delegated transition works without granting reviewers GM or publish credentials.
- Local audit before first push has no remote-check dependency cycle.
- GitHub redelivery, label edits and bot comments do not create duplicate model runs.
- Neutral/skipped Checks cannot fake ELIOT acceptance; remote label lag cannot authorize a new head.
- A lost merge response or unrelated base movement does not trigger blind re-merge.
- No native GitHub merge queue is required for local queue distribution and authorized exact-ref publication.
- No built-in stage requires Python, PowerShell, Node or a frozen CLI/library version.
