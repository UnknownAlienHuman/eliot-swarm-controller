# Audit register

Durable source-audit resume point: inspected revision, exact paths, findings, preserved
behavior and untraced boundaries. Not a runtime registry or second implementation
queue. [Implementation](implementation.md) owns changes;
[status](../implementation-status.md) records landed behavior.
[Audit index](audit-index.md) provides direct evidence checkpoints and source/target links.

## Resume here

- Checkpoint: **2026-10-23 / submission, reviewer replacement, all-findings correction,
  same-Attempt resubmission and acceptance/recovery**.
- Inspected repository head: **`4af640ea6edc190d58a7320674c85cbc5bd3c6b3`**.
  The [index](audit-index.md#evidence-checkpoints) links earlier detailed evidence
  directly; their current dispositions remain below.
- Last connected pass: `submissions::{reserve,begin,finish}` →
  `reviews::{reserve_assign,replacement_class,reserve_submit}` → review disposition →
  `request_changes_core` / correction Send → resubmission → direct/automatic acceptance
  reservation, off-DB verification and final transition. Follow-up reads distinguished
  shared evidence checks from the direct caller's missing assigned-review gate.
  New evidence is [AR-059–064](#trace-2026-10-23-review-acceptance), with AR-011 preserved
  and AR-052 extended to queued repair input.
- Runtime baseline: **`c92120ed70ca7e625440a8a19517c481e73b13c9`**, 2026-10-21.
  The compared interval through the inspected head contains **19 documentation-only
  commits**. Source is unchanged; this is not exhaustive source coverage.
- Overall audit: **incomplete**. There are **64 entries**, including defects, preserved
  mechanisms and partial boundaries; no measured completeness percentage.
- Direct reviewer replacement, all-findings feedback, immutable submissions/reviews
  and the common candidate/check/byte gate already work in the inspected source.
  Automatic replacement and finite correction budgets are missing. Pending-result
  saturation hides later work within an entry; revoked dependency pins can abort a
  whole review-disposition pass. Direct acceptance does not share automatic
  assigned-review/finding checks. No runtime repair or end-to-end completion is claimed.
- **Next exact chain:** `store/checks::{reserve_source,begin_source,finish_source}` →
  `checks::source` snapshot/bytes → `store/checks::{reserve,next,ready}` → selected
  `swarm-checks` / `checks::worker` process → `completion` / `store/checks::finish` →
  cache selection/revalidation → `acceptance::evidence`. Check exact frozen input,
  tool/profile identity, parser coverage, cached evidence, process/resource release,
  cancellation/restart and changes between off-DB verification and commit. Reading
  acceptance's validators has not qualified the CheckRun producer or its cache.
- Source/target entry points: [index](audit-index.md#source-and-target-map),
  [Task policy](../task-policy.md), [delivery](delivery.md), [configuration](configuration.md)
  and implementation G/K. Runtime order remains A1b–A3, then B–V.
- Still unqualified: actual review/replacement/native correction/acceptance execution,
  disable/transfer/restart interleavings, crash/power-loss, full CheckRun production,
  installed independent native/Eliot Goals and the other indexed source boundaries.
- Documentation only. No runtime change, Clippy, tests, benchmark or installed-harness
  qualification ran. Prior exact official donor reads are retained with their original
  version limits in the [donor map](donor-map.md); this pass did not refresh them.

On resume compare changed source with the verified head, then reopen only affected
callers/consumers. Preserve IDs, evidence and qualifications. A source read or docs
commit is neither executed qualification nor a runtime repair.

## Evidence labels

**Source-confirmed** means a named path supports the consequence, not live reproduction.
**Preserve** identifies useful behavior. **Partial** names an untraced boundary. Earlier
rows remain inherited unless explicitly reread. Open findings close only with connected
runtime evidence. Current dispositions below override older broader claims; the
[index](audit-index.md#evidence-checkpoints) links the detailed evidence directly.

## Findings and coverage

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-001"></a>AR-001 | Antigravity reporting versus execution | **Open.** Report retries block native reducer; failure closes stdin and drained facts can remain memory-only. | Independent delivery and continuous native draining. |
| <a id="ar-002"></a>AR-002 | Complete result retention | **Open.** The 32-response cache can evict the only uncollected body. | Durable source lifetime and admission pressure. |
| <a id="ar-003"></a>AR-003 | Exact result identity | **Preserve.** Operation/conversation/generation and explicit unavailable oversized output. | Keep causality, delete package-proof copies. |
| <a id="ar-004"></a>AR-004 | Adapter restart | **Open.** New Antigravity Controller has empty body/delivery caches. | Original-Operation successor delivery, no prompt replay. |
| <a id="ar-005"></a>AR-005 | Admitted page completion | **Preserve.** Sealed normalized origin permits narrow late completion and replay ACK. | First historical admission and partial storage. |
| <a id="ar-006"></a>AR-006 | Continuation output | **Open, cross-route.** Generic origin/adapters remain Dispatch-centric; Muse item addressing does not fix Store admission. | Same source-Operation/page/candidate path for attributable Send. |
| <a id="ar-007"></a>AR-007 | Assembly and byte reads | **Preserve.** Publication/registration, contiguous coverage, digests and object grants. | Crash/power-loss and grant/cache races. |
| <a id="ar-008"></a>AR-008 | Authentication versus completion | **Preserve boundary.** Disabled credentials fail new handshake; authenticated carrier can finish a narrow prior page. | Revocation and successor delivery. |
| <a id="ar-009"></a>AR-009 | Send attribution | **Mixed.** Selected Manager/Repair/Goal paths retain scope; session input can be taskless. | Explicit task-linked versus taskless authority. |
| <a id="ar-010"></a>AR-010 | Repair delivery classification | **Open.** Requires provider-specific ID/condition and treats every Rejected/Cancelled as no effect. | Common admission/terminal facts; executed failure is not non-delivery. |
| <a id="ar-011"></a>AR-011 | Same-Attempt correction | **Preserve; reread.** All actionable findings form one feedback package; `needs_correction` can resubmit against revision/prior submission; stale publication stays historical. | Feedback is not native delivery. Keep continuation origin, immutable review and actual delivery classification; AR-010/052. |
| <a id="ar-012"></a>AR-012 | Revision fences input | **Open.** Old Attempt survives revision; direct Send can become taskless or dispatch without revision recheck. | Supersede unsent old work; check exact subject before effect. |
| <a id="ar-013"></a>AR-013 | GM cleanup of old work | **Open.** GM can revise beneath another owner but cannot release that superseded Attempt. | Scoped historical cleanup without rewriting owner. |
| <a id="ar-014"></a>AR-014 | Release preserves capture | **Open.** Release cancels queued Result together with non-check Operations. | Cancellation by effect semantics. |
| <a id="ar-015"></a>AR-015 | Restart credential | **Preserve.** Stable binding credential; no implicit rotation. | Live restart/revocation. |
| <a id="ar-016"></a>AR-016 | Old-boot first delivery | **Open.** Unseen old-boot terminal fails current-boot validation; committed identical outcome may replay-ACK. | Execution origin separate from delivery actor. |
| <a id="ar-017"></a>AR-017 | Recovery-fenced capture | **Open.** Recovery filters Result before completion-only exception. | Exact completion/readback without native readiness. |
| <a id="ar-018"></a>AR-018 | Supervisor recovery | **Scope correction.** Intake/process recovery is not native-byte import. | Import stays with adapter/Store. |
| <a id="ar-019"></a>AR-019 | Inconclusive lookup completion | **Open.** Antigravity labels completed inconclusive Reconcile itself Unknown. | Applied lookup `resolved:false`; target stays unknown. |
| <a id="ar-020"></a>AR-020 | Terminal versus release | **Boundary.** Late terminal may update unreleased old Attempt; terminal does not prove capture. | Preserve output obligation and producer fences. |
| <a id="ar-021"></a>AR-021 | First historical Result | **Open.** New normalized admission requires current open unreleased Task/Attempt. | Source-scoped completion admission. |
| <a id="ar-022"></a>AR-022 | Automatic capture/readback | **Open.** Automatic consumers do not create Result/Reconcile; Doctor recommends only. | Bounded helpers using ordinary commands. |
| <a id="ar-023"></a>AR-023 | Supervisor readback | **Scope correction.** Unresolved IDs/status do not settle targets. | Semantic recovery in Store/adapter. |
| <a id="ar-024"></a>AR-024 | Page ACK/source release | **Open; target corrected.** Boolean ACK insufficient; local ACK-range ledger duplicates Store. | Store-authoritative complete/next-missing coverage. |
| <a id="ar-025"></a>AR-025 | Heterogeneous sources | **Boundary.** Local bytes/files and provider locators have different lifetimes. | Share ready-record delivery, not source journal. |
| <a id="ar-026"></a>AR-026 | Claude ready-page loss | **Open.** Intent persists, ready page does not; ambiguous ACK becomes Unknown and replay is refused. | Persist exact page before IPC; never repeat SDK input. |
| <a id="ar-027"></a>AR-027 | Codex delivery | **Preserve plus gaps.** Exact page survives restart; reads depend on Dispatch/current root. | Keep delivery, remove checkpoint/tombstones; see AR-032/033. |
| <a id="ar-028"></a>AR-028 | OpenCode retention | **Preserve plus gaps.** Exact replay/assistant parent; permanent `result_source` and Dispatch-only target. | Coverage-driven release without retention graph. |
| <a id="ar-029"></a>AR-029 | Command release | **Preserve plus excess.** Saved pages work; source cleanup uses release-proof Operations/tombstones. | Store-complete coverage then local cleanup. |
| <a id="ar-030"></a>AR-030 | Muse source read | **Preserve plus gap.** Exact item/revision/ref and native ranges; ready page checkpoint. | AR-034 bounds discovery. |
| <a id="ar-031"></a>AR-031 | Coverage owner | **Target correction.** Store owns pages; per-page offset is not aggregate completeness. | Exact coverage ACK/replay, no local union. |
| <a id="ar-032"></a>AR-032 | Historical source versus active root | **Boundary.** Root/family guards protect scope; deleting them cannot recover missing bytes. | Source-authorized reader and actual transition cases. |
| <a id="ar-033"></a>AR-033 | Codex read amplification | **Open.** Each new page rereads thread/history/turn/full body, then slices 24 KiB. Saved replay bypasses this. | Native ranges or one selected-result materialization. |
| <a id="ar-034"></a>AR-034 | Muse discovery budget | **Open.** Repeated `readItem` walk detects cycles but has no aggregate page/byte/time budget. | Resumable discovery and locator reuse. |
| <a id="ar-035"></a>AR-035 | Capture/disclosure grants | **Preserve.** Receipt, Bytes and Assemble differ; historical reviewer gets no new bytes. | Scoped first capture, no invented Operator or wider grants. |
| <a id="ar-036"></a>AR-036 | History-dependent capacity | **Open.** Global historical census/ledger verification precedes current root count. | Indexed current claims, no full-history admission scan. |
| <a id="ar-037"></a>AR-037 | Native-default Antigravity | **Open.** Root identity requires optional `modelId`; root policy can block an empty route. | Budget identity independent of optional model. |
| <a id="ar-038"></a>AR-038 | WorkDispatch saturation | **Open.** 128 pending subjects stop intake of later eligible Task facts. | Fair current-state scan, delete copied pending list. |
| <a id="ar-039"></a>AR-039 | Stale provider precedence | **Open.** Stale matching row outranks fresh Available. Eliot link loss retains rows; accepted native account/closed snapshot invalidates its own source. | Same-source supersession, independent quota constraints and remaining lifecycle cases. |
| <a id="ar-040"></a>AR-040 | Root dedup/admission/scan | **Preserve.** Parent/Open/Dispatch/service share a claim; transaction handoff and stable scan exist. | Route edits and common transaction traced in AR-049/050; live concurrency remains unqualified. |
| <a id="ar-041"></a>AR-041 | Reads/control under quota hold | **Preserve plus earlier gates.** Reads/strict Reconcile and stop/reply controls bypass starts-only quota. | Recovery/ready/scope barriers; AR-017 and AR-045. |
| <a id="ar-042"></a>AR-042 | Cancel held unbound launch | **Open.** Cancelled parent fails workspace sweep's Settled no-effect contract; verified-held unbound lease can stay fenced. | Stage-aware cancellation and in-flight workspace readback. |
| <a id="ar-043"></a>AR-043 | Pre-send Open rejection | **Open.** Guard rejects unsent Open without retiring rootless opening binding; cancel refuses non-Queued target. | Known-no-effect disposition through existing resource owners. |
| <a id="ar-044"></a>AR-044 | Lowered root limit/reuse | **Open policy mismatch.** `other_claims >= limit` can hold zero-root-growth continuation after limit reduction. | Separate root delta from input/quota/drain policy; live reload unqualified. |
| <a id="ar-045"></a>AR-045 | Shutdown when execution is not Ready | **Source-confirmed open barrier.** Admission and pre-effect validator require Ready; recovery queue independently excludes Shutdown. | Authorize exact resource cleanup by custody/scope, not new-work readiness. |
| <a id="ar-046"></a>AR-046 | Explicit detach versus adapter demand | **Source-confirmed open integration gap.** Applied transport shutdown leaves native identity retained; demand still anchors attachment to settled Open and holds its lease. | Separate desired attachment from native occupancy and output obligations; no respawn-loop claim. |
| <a id="ar-047"></a>AR-047 | Departure and binding/workspace disposal | **Preserve plus open handoff.** Historical service departure reader survives stale Task/owner; its writer changes service row only. AR-048 completes the ordinary finalizer search. | Connect retirement after all relevant facts; neither auto-release on socket close nor delete guards. |
| <a id="ar-048"></a>AR-048 | Attached binding never reaches ordinary retirement | **Source-confirmed open.** Only rootless pre-native Open cancellation writes binding release. Attempt/service completion and periodic sweep do not retire an attached binding. | Existing Store disposition must retire the exact resolved binding, then let the sweep release an eligible lease; preserve results and unresolved native work. |
| <a id="ar-049"></a>AR-049 | Budget continuity across route edits | **Source-confirmed target mismatch; bounded claim.** Model/artifact changes split root identity; whole-route digest changes exclude old pending launch claims. Limit-only edits preserve bound-root identity. | Stable admitted budget scope. Stale-plan/current-capacity revalidation already fences launch effects; no oversubscription demonstrated. |
| <a id="ar-050"></a>AR-050 | Direct/automatic claim and Open transaction | **Preserve.** Both actors converge on the same launcher; after external workspace preparation, an Immediate transaction rechecks facts and commits claim/Open/binding association. | Preserve reservation continuity when removing proof manifests. Actual concurrent/restart/native execution remains unqualified. |
| <a id="ar-051"></a>AR-051 | OpenCode automatic Goal actor | **Source-confirmed open.** Generated `agent.goal` uses the technical requester, but the runtime actor branch handles only `agent.send`; missing client registration causes committed rejection before native command. | Ordinary attributed Send across capable adapters; do not create a synthetic Manager credential. Admission-only fixture does not cover dispatch. |
| <a id="ar-052"></a>AR-052 | Disable/edit after Goal or repair admission | **Source-confirmed target mismatch, extended.** Goal rejects queued input after policy disable/edit/transfer. Repair's no-continuation branch also requires the original enabled revision; its exact transfer-continuation branch is distinct. | Preserve admitted intent and actual rights/subject fences. Queued repair rejection retains its semantic slot; re-enable does not create another Send. |
| <a id="ar-053"></a>AR-053 | Goal input during review/correction | **Source-confirmed eligibility gap.** Open/unaccepted Task and unreleased Attempt suffice even when Attempt is `submitted` or `needs_correction`. | Pause generic Goal Sends at those phases; preserve capture/check/review/accept and the one findings-bearing correction Send. No live race or candidate mutation proved. |
| <a id="ar-054"></a>AR-054 | Finite Goal continuation | **Source-confirmed target gap.** Settings contain only `goal_id`; per-pass caps and one-use EventRef do not bound successive valid terminal events. | Finite occurrence/no-progress budget, honest usage and one Governor decision; no fresh-ID or transfer reset. No observed infinite native run claimed. |
| <a id="ar-055"></a>AR-055 | Unclaimed Goal event after transfer | **Source-confirmed open.** Ownership conflict leaves no global slot but advances the entry cursor; relocation carries it unchanged to the authorized successor. | Re-evaluate current unclaimed subject after rights/owner change without global history rewind or replay of claimed work. |
| <a id="ar-056"></a>AR-056 | Terminal source and continuation deduplication | **Preserve.** Exact native source evidence, original Task/Attempt/binding and single-use EventRef at Store admission; ordinary continuation/link/slot/cursor commit together. | Admission does not guarantee native dispatch success. Keep atomic identity while deleting proof/cursor copies; result capture/acceptance gaps AR-006/022 remain open. |
| <a id="ar-057"></a>AR-057 | Calendar occurrence across edits/restart | **Preserve within CheckRun scope.** Generation excludes action/owner/revision; considered occurrence survives re-enable, action edit, transfer and Store reload; catch-up is latest-only. | Carry these semantics to one loop; existing separate scheduler/automation loops do not implement general Goal messages. Source review, not executed restart. |
| <a id="ar-058"></a>AR-058 | Native Goal capability versus controller wrapper | **Capability boundary.** Current Codex Rust lacks `agent.goal`; Muse Node maps native verbs; built-in OpenCode wraps instructions/prompt. Newer native upstream sources are not installed support. | Capability-based mapping, independent ordinary Eliot messages, native admission distinct from completion; pinned donor contracts and runtime qualification. |
| <a id="ar-059"></a>AR-059 | Automatic reviewer replacement | **Source-confirmed missing caller.** Store supports direct Unanswered/Inconclusive/ReturnedForCorrection replacement; automatic assignment forbids replacement and drops its pending subject once assigned. | Bounded current-state `review_once`, configured response deadline, exact assignment CAS and distinct eligible reviewer; one Governor decision when policy cannot continue. |
| <a id="ar-060"></a>AR-060 | Finite correction/no-progress | **Source-confirmed target gap.** Same-Attempt new submissions/reviews can obtain fresh repair slots; exact-result dedupe and retry backoff do not bound rounds. | Reserve finite correction rounds, assess relevant settled evidence, preserve consumption and unresolved effects; no fresh-ID reset. No infinite run was executed. |
| <a id="ar-061"></a>AR-061 | Review-result pending saturation | **Source-confirmed entry-local barrier.** At 64 pending results that entry stops before the next unqueued event, including potentially ready independent work. Other entries continue on normal capacity status. | Fair bounded current-state selection without copying another pending queue; keep unknown effects and late-check recovery. |
| <a id="ar-062"></a>AR-062 | Revoked dependency pin in review consumer | **Source-confirmed cross-subject barrier.** Post-claim invalidation yields `DEPENDENCY_REVALIDATION_REQUIRED`; it rolls back the review-disposition pass and skips queued acceptance verification on that call. | Per-subject replan/reclaim with immutable old pin/candidate; unrelated work progresses. Reaccepting the prerequisite cannot repair the frozen revoked receipt. |
| <a id="ar-063"></a>AR-063 | Frozen evidence and acceptance verification | **Preserve with scope limit.** Direct/automatic paths share candidate, V2 plan, exact checks/dependencies and off-DB byte verification with final revalidation. Late missing CheckRuns retry the retained pass. | Preserve these validators; producer/cache/installed execution remains unqualified. Shared evidence gate does not imply shared assigned-review provenance (AR-064). |
| <a id="ar-064"></a>AR-064 | Direct acceptance review/finding gate | **Source-confirmed target gap.** Direct caller supplies requirement-review arrays; common gate checks their coverage and feedback freshness, not retained assigned review or resolution of actionable findings. Automatic preflight adds those review restrictions. | Resolve required auditor provenance and finding disposition in the common gate; accepting agent supplies decision/reason, not substitute review evidence. |

## Earlier detailed traces

Detailed older prose is retained in immutable Git snapshots instead of being recopied
into every checkpoint. Stable AR IDs and current dispositions remain above; the
[index](audit-index.md#evidence-checkpoints) links all earlier passes directly.

### Prior connected source trace: AR-045–AR-047

[Detailed retirement/control trace at 3c4c859](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3c4c8599f1614460fe69d54d0b59715a26ada797/docs/agent-operations/audit-register.md),
inspected at `c2b998b`: shutdown readiness, detach/demand and historical departure.

<a id="trace-2026-10-23-retirement-admission"></a>

### Source trace at b2519aa: AR-048–AR-050

[Detailed trace at 73ba700](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/73ba7009bbc5c5515fe4c5f85743066e944106ef/docs/agent-operations/audit-register.md#trace-2026-10-23-retirement-admission):
missing attached-binding finalizer, route-edit accounting and preserved common
claim/Open transaction. No live oversubscription/concurrency claim.

<a id="trace-2026-10-23-goal-policy"></a>

### Source trace at 73ba700: AR-051–AR-058

[Detailed trace at 4af640e](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/4af640ea6edc190d58a7320674c85cbc5bd3c6b3/docs/agent-operations/audit-register.md#trace-2026-10-23-goal-policy):
Goal actor/disable/phase/budget/transfer, exact terminal identity, calendar continuity
and current versus upstream native Goals. The AR-052 repair extension is below; it
does not remove the distinct Goal transfer finding or qualify installed behavior.

<a id="trace-2026-10-23-review-acceptance"></a>

## Source trace at 4af640e: review, correction and acceptance

Inspected 2026-10-23 at the head above, runtime `c92120e`. Paths are relative to
`crates/swarm-kernel-host/src` unless qualified. These are connected source reads,
including counterchecks of the direct/automatic paths, not executed fixtures or fixes.

### AR-059 — direct replacement exists; the automatic caller stops after assignment

`reviews::replacement_class` supports an unanswered slot without a verdict,
`inconclusive` without a correction disposition, and `changes_requested` only after
its exact ReturnForCorrection disposition. `reserve_assign` requires the expected
current assignment and a different reviewer. A passing assignment cannot be replaced.
`reserve_submit` retains one immutable result: identical semantic replay coalesces;
different input conflicts. Late results require exact retained non-revoked reviewer
scope and remain historical when the slot or candidate is no longer current.

`automation_dispatch::attempt_review_assignment` is driven by an applied submission.
It constructs `ReviewAssignRequest::for_automation` without a replacement ID;
`reviews::reserve_assign` expressly forbids replacement for `ReviewActor::OnBehalf`.
`recheck_pending` retries unassigned subjects but removes one after successful
assignment. The host's startup/change/safety wake cannot replace an unanswered or
inconclusive reviewer through this path. Existing direct classes are therefore
preserved capability, not implemented automatic replacement.

Target `review_once` must query current slots, use a configured response deadline or
retained inconclusive result, recheck the expected assignment and select a distinct
eligible reviewer. Bound replacement attempts and address one Governor decision if
policy cannot continue. Do not introduce a reviewer daemon or treat silence as death.

### AR-011 and AR-052 — keep all findings and resubmission; fix admitted-input policy

`automation_disposition::consume_selected_disposition` builds one ordered package of
all committed actionable findings. `submissions::request_changes_package_on_behalf` /
`request_changes_core` commits feedback and `needs_correction`; its receipt explicitly
says durable mailbox only, no native input sent and no repair started. If RepairDispatch
is selected, the consumer verifies exact disposition/findings/feedback provenance and
reserves a separate ordinary Send within the reconciliation transaction. Native input
admission, terminal result, corrected submission and acceptance are separate facts.

`submissions::{reserve,begin,finish}` retains the submitted document and candidate,
verifies published provenance after file I/O, and applies it only if the exact revision,
Attempt and prior submission are still current. Otherwise verified publication remains
historical. `needs_correction` can submit again on the same unreleased Attempt. Even
an unchanged artifact may get a new immutable submission and review slot; this is not
permission to overwrite a review or count a new ID as progress.

`RepairDispatchContext::require_current_for_effect` calls `require_current_action`
when no current transfer continuation exists. It requires the original enabled entry
revision and selected RepairDispatch step. Pure disable/edit after queue admission can
therefore make `runtime::next_internal` commit `queued` → `rejected`, `command:null`.
The retained semantic slot resolves to that Operation on re-enable; the current repair
consumer classifies it as NoEffect/skipped rather than admitting another Send.
There is an explicit exact transfer-continuation branch: do not copy Goal's broader
transfer failure onto it. Preserve actual rights, candidate/phase/binding and unresolved
effect fences while separating future policy from admitted intent. AR-010's broad
Rejected/Cancelled classification remains a separate open issue.

### AR-060 — correction identity does not bound correction rounds

The production submission/review/disposition/repair chain has no visible correction
round budget, repeated-content progress comparison or automatic no-progress Governor
escalation. `semantic_slot_id` includes Task/revision/Attempt/submission/candidate and
findings digest. It deduplicates one correction; a new submission/review can have a new
slot. `submissions::finish` returns the same Attempt to `submitted`, not accepted.
A succession of new changes-requested results has no finite stop in these owners.
This is a source-bounded absence finding, not an observed endless native run.

Reserve finite rounds at new correction admission, then assess progress from relevant
settled candidate/check/review evidence. Preserve consumption across ID changes,
policy edits, transfer and restart. Waiting and retrying the same readback are not
another round. Exhaustion stops new input and coalesces one Governor-agent decision;
it never discards unknown native effects or stops their capture/readback.

### AR-061 — pending capacity blocks an entry's later ready result

`review_disposition::reconcile_entry` rechecks bounded pending work first. At
`MAX_PENDING_RESULTS = 64` it returns, or stops before the next unqueued event, without
advancing that entry past the barrier. A later independent ready result for the same
entry cannot enter until pending space opens. Normal capacity status persists and the
outer loop continues other entries; this is not the domain-wide error in AR-062.

`MAX_PENDING_RETRIES = 32` saturates the retry counter; it is not an abandonment bound.
Unresolved work remains and backoff caps at 60 seconds. Preserve this retention,
particularly exact unknown delivery and late-check recovery. Replace backlog-based
eligibility with fair bounded queries over current domain facts, not dropping old
records or adding a larger second queue.

### AR-062 — a revoked frozen dependency escapes subject isolation

`tasks::claim` resolves dependency acceptance receipts for a new Attempt and freezes
them in its snapshot. `acceptance::invalidate` can later revoke such a prerequisite.
With the dependent candidate's checks ready, `acceptance::validate_dependencies`
rejects that pinned receipt with `DEPENDENCY_REVALIDATION_REQUIRED`. This differs
from `DEPENDENCY_NOT_READY` at claim time and from a newer prerequisite revision:
an otherwise valid explicitly pinned older decision remains valid.

`review_disposition::consume_isolated` does not classify this condition locally;
`classify_domain_error` treats it as fatal. `reconcile_automation_domain` rolls back
the entire review-disposition transaction, including other progress/cursors in that
pass. Canonical review results remain retained, but repeated passes can encounter the
same blocker. `Store::reconcile_review_dispositions_once` calls the automatic acceptance
verifier only after that pass succeeds, so already-queued unrelated acceptances are
also not verified on the failed call. No live stall duration was measured.

Treat revoked-pin recovery as a visible per-subject decision. Reaccepting the
prerequisite under a new Operation cannot un-revoke or rewrite the dependent snapshot.
Current recovery closes the dependent Attempt's effects, releases/reclaims after a
valid prerequisite receipt exists, and creates the new Attempt's submission/review.
Retain the old candidate/history; do not bypass evidence or silently migrate its pin.
The target common loop must continue unrelated review and queued verification work.

If invalidation occurs after `task.accept` reservation, `begin_on_behalf` / `finish_on_behalf`
rerun the gate and settle failure. The review consumer retains that terminal receipt
and removes the resolved pending cause; it does not replay the failed acceptance or
automatically revisit that consumed pass after later evidence changes. Recovery needs
an attributable fresh decision under valid current scope, not another request ID for
an unresolved or still-invalid effect.

### AR-063 — preserve the shared evidence/byte gate and late-check recovery

Direct `acceptance::reserve` and automatic `reserve_on_behalf` converge on
`reserve_with_actor` / `evidence`. These check current Task/revision/Attempt/submission/
candidate, accepting actor independence from owner/submitter, exact feedback freshness,
the frozen V2 plan, exact required review coverage, dependency pins, candidate digest/
length and required CheckRun identities/profile revisions/parser coverage. The direct
and on-behalf begin/finish pairs rerun the evidence gate around off-DB artifact-byte
verification and compare the retained manifest before Task acceptance. Do not replace
this with another acceptance engine or remove the frozen plan.

`automation_acceptance` copies `requirement_reviews` from the exact retained assigned
pass. Missing V2 review-required coverage is a capability gap, not fabricated evidence.
Check-to-requirement mapping follows the frozen plan after exact profile-set validation.
Historical V1 has no invented per-requirement mapping. The earlier blanket deletion
wording for automatic evidence arrays was too broad and is corrected in implementation G.

Before admission, missing required checks produce `required_checks_incomplete`.
`review_disposition::is_unresolved` retains the exact event/assignment/result in pending
state and retries it; a later passing exact CheckRun can unlock the same candidate.
The pass is not lost merely because it arrived before checks. Artifact/source/cache
production is the next chain, not established by reading these acceptance validators.
Store checks structure/scope/bytes, while a reviewer must judge whether cited evidence
actually proves the criterion. Neither an evidence array nor source inspection proves
that semantic judgment or installed execution.

### AR-064 — the common gate does not yet own assigned-review/finding provenance

`AcceptActor::reviewer_id` is the direct caller or represented Manager, not the assigned
auditor. Direct `task.accept` supplies `AcceptRequest.reviews`; parse requires unique
requirement IDs and nonempty rationale/evidence strings, and `validate_coverage` requires
the frozen review subset. The common `evidence` function does not resolve a retained
assigned review, check configured reviewer count, scope those review citations to the
assigned artifacts, or inspect unresolved findings. `expected_feedback_observation_id`
only fences a change since the caller's selected cursor. Independence from the
submission owner/submitter remains checked; this is not unauthenticated acceptance.

Automatic preflight additionally requires a canonical assigned complete pass with no
findings, independent reviewer/sponsor identities, current candidate applicability and
no later feedback. It supports the primary slot with `required_reviewers == 1`; a larger
count is an explicit capability gap. These checks are not automatically inherited by
the direct path merely because both callers eventually use `acceptance::evidence`.

Target direct and automatic decisions must resolve required review provenance and
current finding dispositions through that common owner. Keep accepting decision actor,
auditor and author distinct. An authorized decision may dispose a disputed finding
under policy; a fresh cursor or caller-written replacement review array cannot do so.
Reject unsupported review policies before work and preserve exact historical results.

| Inspected owner | Exact read boundary |
| --- | --- |
| `store/submissions.rs`, `submission.rs`, `store/tasks.rs` | Frozen claims/spec/dependency pins, submission reserve/begin/finish, stale publication, all-findings feedback and same-Attempt resubmission. |
| `store/reviews.rs`, `review.rs`, selected `store/coordination.rs` scopes | Slot/candidate identity, replacement classes/CAS, immutable result, exact requirement coverage and non-revoked late-result scope. |
| `store/automation_dispatch.rs` | Submission-driven assignment, pending retry/removal and automated request construction. |
| `store/automation_disposition.rs`, `store/review_disposition.rs` | Exact committed result, all-findings package, subject savepoints, pending/backoff/capacity, fatal classifier and terminal acceptance disposition. |
| `automation/repair.rs`, `store/automation_repair.rs`, `store/runtime.rs` | Linked repair Send, slot identity, current-policy/transfer pre-effect guards, committed rejection and readback classification. |
| `store/automation_acceptance.rs`, `automation/acceptance.rs` | Assigned-pass preflight, late-check selection, retained review evidence, semantic request identity and failed/in-flight receipt handling. |
| `store/acceptance.rs`, host/kernel `acceptance.rs`, `store/mod.rs` | Direct/on-behalf gate and differences, exact frozen evidence, byte-verification stages, invalidation and domain/verifier ordering. |

## Next work and retained evidence

Continue the CheckRun producer/cache chain named in Resume here. Review/correction/
acceptance now have this selected connected source trace; their open defects belong
to implementation, not another repetition of the same pass. Full corrected-result
capture, installed review/native execution and live interleavings remain unqualified.

[Coverage still open](audit-index.md#coverage-still-open) names the other areas;
[evidence checkpoints](audit-index.md#evidence-checkpoints) links prior detailed traces.
Keep IDs and refuted/narrowed claims stable. A documentation commit repairs no runtime.
