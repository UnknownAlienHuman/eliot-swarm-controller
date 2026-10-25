# Failure-prevention register

This register answers one question: **what prevents a confirmed previous failure from being reintroduced by a new implementation agent?**

It does not replace the detailed [audit register](audit-register.md), immutable [audit index](audit-index.md), [lessons learned](../lessons-learned.md) or current source inspection. Every `AR-*` remains open or mixed until its exact source disposition and acceptance evidence are recorded. A design sentence is not closure.

## Closure rule

A finding may be marked `closed` only when all five fields exist:

1. current invariant;
2. exact source/effect owner;
3. implementation slice and deletion/data disposition;
4. executable acceptance case tied to the original failure mode;
5. exact merged commit and executed evidence.

Missing evidence means `open`; partial source repair means `mixed`. Compiler success alone cannot close runtime, crash, restart, security, migration or load findings.

## Source findings AR-001–170

The ranges below cover every stable AR ID. Follow the immutable snapshot link in the [audit index](audit-index.md#evidence-checkpoints) for the per-ID evidence and counterexample.

| AR range | Previous failure class | Current prevention invariant | Owner / implementation slice | Required closure evidence | State |
| --- | --- | --- | --- | --- | --- |
| 001–005 | Antigravity result depended on volatile process/observation state; response body and identity could be lost or misattributed across restart. | Exact Operation, binding generation, origin boot, terminal status, body digest/bytes and Store ACK; no prompt replay after ambiguity. | Adapter + Store result owners; A1–A2. | Crash before/after capture, ACK loss, old/new boot, wrong Operation/body, complete page coverage. | `mixed`: A1 landed; durable delivery/restart remains open. |
| 006–008 | Continuation/result assembly could claim completion without authenticated complete evidence. | Store-authenticated producer identity, aggregate coverage and candidate/result attachment; incomplete evidence remains unknown. | Store results/artifacts; A2/G. | Missing/duplicate/out-of-order pages, unauthorized assembly, exact completion gate. | `open` |
| 009–011 | Send/correction attribution and repair could start or complete the wrong Attempt. | One initial dispatch; follow-up Send is a separate Operation; same-Attempt correction uses exact candidate/revision. | Store runtime/submissions; A/G. | Duplicate request IDs, late Send, correction resubmission and foreign Attempt rejection. | `open` |
| 012–016 | Revision/release/cleanup and old-boot evidence could cross ownership boundaries. | Current revision and binding-generation fences; resource release is separate from verdict; prior-boot facts retain origin. | Store task/runtime/workspace + process custody; A–D. | Revise/dispatch race, partial cleanup, old-boot delivery and stale release cases. | `open` |
| 017–019 | Recovery queue/supervisor lookup could treat inconclusive state as absent or safe. | Exact process birth/group identity, bounded gaps and readback-only recovery; timeout is unknown, never PASS. | Supervisor/process + Store reconciliation; C/D/R. | PID reuse, incomplete family observation, lookup timeout and unrelated-work continuation. | `open` |
| 020–024 | Late terminal/historical capture/readback could complete newer work or require manual replay. | Evidence stays attached to original Operation/Attempt; automatic exact readback; superseded assignment receives history only. | Adapter/Store results; A2. | Late result after replacement, first historical capture, automatic reconciliation without native replay. | `open` |
| 025–031 | Cross-adapter result sources and delivery semantics diverged; coverage ownership was unclear. | One semantic result contract; Store owns authoritative coverage; adapter owns vendor readback; common shell only after two real consumers. | A/L/M–Q. | Two current adapters prove shared behavior; missing source remains explicit; no universal effect journal. | `open` |
| 032–035 | Historical guards/read amplification/discovery/grants could infer authority from metadata. | Live object rights and current source facts; package/version/hash is compatibility/evidence, not authorization. | Store object scope + module API; H/R. | Unauthorized historical read, damaged grant, bounded read amplification and unmatched native subject. | `open` |
| 036–040 | Capacity/default-model/saturation/quota/dedup were copied into parallel state or stale projections. | Capacity counts actual unresolved roots/effects; provider facts have source/epoch/freshness/coverage; route defaults are explicit. | Store launcher/provider; B. | Stale quota, saturation, default model unknown, duplicate root and unrelated-work progress. | `open` |
| 041–044 | Quota bypass/cancelled launch/rejected Open/root limits could leak or over-admit resources. | Admission and reservation are atomic; cancellation/rejection has exact disposition; root limit uses stable service identity. | Launcher/capacity/workspace; B–D. | Cancel/reject races, route edits, capacity release and concurrent root admission. | `open` |
| 045–050 | Shutdown/detach/finalization and route-budget identity were incomplete; direct and queued launch could diverge. | One common two-stage launch transaction; attached binding remains owned until exact retirement; budget scope survives irrelevant route edits. | Store launcher/lifecycle; B–D. | Direct vs queued equivalence, finalizer, detach demand and route mutation under load. | `open` |
| 051–058 | Goal actor/phase/budget/transfer bugs could loop, lose authority or confuse native and Eliot goals. | Exact actor and cause, finite progression/no-progress budgets, independent native/Eliot goals, one ordinary current-state loop. | Automation/runtime adapters; E/N/P/Q. | Restart, transfer, pause/resume, native+Eliot combination and bounded escalation. | `open` |
| 059–064 | Review replacement/correction/fairness/acceptance could reuse stale or self-claimed evidence. | Exact candidate/subject, independent evidence, all findings returned in one correction, finite replacement and acceptance revalidation. | Store reviews/submissions/acceptance; G. | Missing/inconclusive reviewer, stale candidate, multiple findings, fairness and dependency revocation. | `open` |
| 065–070 | CheckRun source/custody/parser/cache/executor duplication could produce unverifiable or stale acceptance evidence. | Immutable source cut, exact process/capture facts, cache lineage and one executor after cutover. | Check owners; K. | Interrupted capture, parser failure, cache reuse lineage, cancellation and standalone-only execution. | `open` |
| 071–078 | ScriptRun restart/actions/readback/cancellation/transfer had split authority and ambiguous child effects. | Atomic parent/child Operations, typed action identity, exact readback, finite cancellation and one execution path. | Script owners; K. | Mid-run cancellation, partial action, restart, duplicate action and legacy-executor absence. | `open` |
| 079–088 | Forge/GitHub effects had sticky holds, duplicate writers and manual unknown reconciliation. | One domain writer, immutable intent, exact remote readback and automatic post-effect recovery; publication lock covers only the write. | Forge/GitHub effect owners; K/U. | Lost response after remote apply, mutable source, duplicate PATCH/merge, cleanup after applied write. | `open` |
| 089–099 | GitHub collection/import used mutable pagination and mixed remote observation with local work. | Stable collector cut and aggregate coverage; explicit Task import; remote view independent from work pool. | GitHub source/import; U. | Pagination mutation, partial coverage, local divergence, import conflict and work-pool deletion. | `open` |
| 100–114 | Mailbox/coordination duplicated products and rewrote or blurred recipient/GM history; disconnected GitHub code remained. | Immutable original sender/recipient, current GM epoch without history rewrite, one mailbox; deleted surfaces stay absent. | Mailbox/GM + S/T/U. | A→B→A handover, old recipient read, grant migration, unresolved row disposition and removed-symbol scan. | `mixed`: disconnected GitHub cleanup landed; mailbox/coordination remains open. |
| 115–126 | Store version/hash bootstrap and readers could split schema truth or validate after commit. | Numeric schema identity, finite v1 bridge, pre-WAL identity, `BEGIN IMMEDIATE`, rollback, no fallible post-commit validation, query-only readers. | Store evolution; I. | Upgrade/downgrade, corruption, interrupted migration, reader cutover and power loss. | `open` |
| 127–141 | IPC/MCP outer locks, hot reads and subscription/report pumps could serialize, deadlock or lose exactness. | Multiplexed correlation, no replay after unknown, separate tool/pump slots, bounded lag, exact pending-input read and WAL routing. | IPC/MCP; H/J. | Concurrent tools/subscriptions, disconnect after commit, slow consumer, lag recovery and load. | `open` |
| 142–159 | Governor observation mixed authority with incomplete provider/process/Git/remote sources and hid unmatched subjects. | Passive source-owned collectors, explicit coverage/gaps, joined WAL projection, unmatched subjects visible, no observation-derived authority. | Observation; R. | PID reuse, process family, provider sequence, dirty worktree, remote divergence and source outage. | `open` |
| 160–170 | Gateway secret, Origin, session capacity, timeout and compatibility launcher boundaries were incomplete. | Absolute bounded private no-follow secrets; fixed principal/profile; closed Origin policy; separate connection/session limits; timeout remains unknown; standalone cutover. | Gateway; V. | ACL/link/special-file negatives, browser/tunnel Origin, session cleanup/capacity, timeout-after-commit and launcher absence. | `open` |

## Previous development-process failures

These gates come from the retained historical lessons and remediation evidence. They apply to every task packet.

| ID | Previous process failure | Prevention gate |
| --- | --- | --- |
| PF-01 | Agents received huge histories, stale queues and contradictory addenda. | One current task revision and one compact task packet; historical documents are evidence only. |
| PF-02 | A symbol, receipt shape, grep hit or bounded reachability heuristic was accepted as proof. | Acceptance names observable behavior and exact evidence; search timeout/inconclusive is `unknown`. |
| PF-03 | Expected checklists copied prior omissions and silently lost requirements. | Task packet cites the independent retained requirement set and related AR/PF IDs; product-meaning change creates a new revision. |
| PF-04 | One review finding caused repeated whole-audit loops; other findings arrived later. | Independent reviewer returns all actionable findings in one correction; replacement is automatic and finite. |
| PF-05 | “Blocked by scope” hid a missing producer required by the task. | Packet names positive path and authorized prerequisite owners; missing required producer changes the packet/revision, not the evidence claim. |
| PF-06 | Parent/UI/process counters were mistaken for native family completion. | Exact root/child/birth/group facts and explicit source coverage; absence or stale view is not zero. |
| PF-07 | Transport failure or stale permission request triggered blind retry/replay. | Unknown effects remain unresolved until exact readback; stale requests terminate locally and never authorize repeated native input. |
| PF-08 | Queue, dashboard, scheduler, provider and capacity views became parallel state machines. | These remain projections over authoritative Tasks/Operations/bindings/workspaces/source facts; lost wakeups recover by bounded current-state query. |
| PF-09 | Routine automation was replaced by operator/manual handoff. | Packet includes the next automatic action or one addressed Governor decision; human work is allowed only for missing authority/credentials/product intent. |
| PF-10 | Multiple writers, worktrees or shared Git indexes corrupted integration. | One manager owns one worktree and shared contracts; leaf scopes are disjoint; one domain owner performs each external write. |
| PF-11 | Broad frameworks, wrappers and compatibility families grew before a working vertical path. | No new abstraction without two current consumers and deletion of both duplicates in the same slice. |
| PF-12 | Source review, mock fixtures or compilation were reported as runtime qualification. | Status records source, focused connected, installed/native, crash/restart, security/migration and load evidence separately. |
| PF-13 | Temporary provider limits overwrote configured policy; requested model was treated as effective execution. | Desired policy, requested route, native-effective value and observed usage remain separate facts with source/freshness. |
| PF-14 | Cleanup success was claimed without exact process/resource release. | Direct exit, family departure, capture completion and resource release are independent facts; timeout preserves ownership and `unknown`. |
| PF-15 | Remote write success and local finalization were conflated, causing repeated external effects. | Immutable intent plus remote readback; retry only unfinished post-action/cleanup, never the already-applied writer. |
| PF-16 | Active documentation pointed to superseded programs as current authority. | [`documentation-map.md`](documentation-map.md) classification is mandatory; archive/evidence can never be a worker instruction. |

## Task-packet gate

Every implementation packet must cite the applicable AR ranges and PF IDs, state how the positive path remains connected, name what old mechanism/data is deleted, and name the focused acceptance case. An uncited historical lesson is not automatically a requirement; a confirmed finding omitted from the packet is a packet defect.

## Current coverage verdict

- All stable AR IDs **001–170 are indexed here by range**, with detailed evidence retained in immutable snapshots.
- This is **not a claim that every source domain has been audited**. The active source audit explicitly remains incomplete.
- Most AR ranges remain open because implementation and executable closure evidence are absent.
- Historical external source material not committed to the repository cannot be treated as independently revalidated until its exact content/hash and disposition are available.
