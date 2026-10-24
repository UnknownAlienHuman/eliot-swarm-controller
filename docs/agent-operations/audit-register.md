# Audit register

Durable source-audit resume point. This file owns finding dispositions and the exact
continuation point; [implementation](implementation.md) owns source changes,
[status](../implementation-status.md) owns landed behavior, and
[the audit index](audit-index.md) owns navigation to immutable evidence.

## Resume here

- Checkpoint: **2026-10-23 / Forge and GitHub remote effects, recovery and ownership**.
- Source inspected at **`38db378c559dd0d95cd60db26702ffa73a954258`**.
  Runtime source is still **`c92120ed70ca7e625440a8a19517c481e73b13c9`**
  from 2026-10-21; the intervening **22 commits are documentation-only**.
- Last connected chain:
  `store/forge::{reserve,next,ready,finish,reconcile}` → `swarm-forge-worker`
  push/readback → `store/github_effects` managed labels →
  `store/github_pr_effects` PR metadata → automation publication/projection →
  restart, transfer and ambiguous-write settlement.
  New evidence is [AR-079–087](#trace-2026-10-23-forge-github-effects).
- Overall audit: **incomplete, 87 entries**. Entries include defects, preserved
  mechanisms, scope corrections and partial boundaries; they are not 87 bugs or a
  measured completion percentage.
- Preserved in this pass: one admitted Forge Operation before external I/O; exact
  accepted-candidate/ref/commit/tree identity; force-with-lease; exact remote-ref
  readback; bounded per-target serialization; retained native-worker custody; and PR
  reconciliation that records an already-authorized GET without rechecking current GM.
- Open in this pass: GitHub API writes have two execution owners; direct `GhCli`
  writes lack pinned executable/process-family custody; automatic label recovery
  ignores `outcome_unknown`; post-write label completion is incorrectly coupled to
  current automation/source/candidate state; publication/projection keep copied
  workflow state; PR metadata has no automatic policy caller.
- **Next exact chain:** `store/github` source poll/observer/work-pool mutation →
  GitHub Issue/PR pagination and source revisions → Task creation/revision/import →
  automation intake → independent remote-ref observation. Prove collection cannot
  mutate Tasks in the same ownership path and that enabled import uses one ordinary
  Task command without copied work-pool lifecycle.
- Runtime implementation order remains **A1b–A3, then B–V**. The source audit does not
  reorder implementation.
- Still unqualified: live Git/GitHub/Windows/Linux behavior, power loss, installed
  credentials/accounts, load/fairness, the autonomous end-to-end work cycle and every
  boundary listed in the index.
- Documentation only. No runtime code, Clippy, test, benchmark or installed-harness
  qualification changed in this checkpoint.

On resume, compare live `main` with the inspected source head, reopen only changed
owners/consumers, and continue the named chain. A source read or docs commit is not a
runtime repair.

## Evidence labels

**Open** names a demonstrated gap. **Preserve** identifies working behavior that a
refactor must retain. **Boundary** or **partial** records an incomplete trace.
**Scope correction** narrows an earlier claim. Current rows override older wording;
the [index](audit-index.md#evidence-checkpoints) links the detailed snapshots.

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
| <a id="ar-063"></a>AR-063 | Frozen evidence and acceptance verification | **Preserve with scope limit.** Direct/automatic paths share candidate, V2 plan, exact checks/dependencies and off-DB byte verification with final revalidation. Late missing CheckRuns retry the retained pass. | Preserve these validators; assigned-review provenance remains open in AR-064. CheckRun producer/cache are qualified by AR-067–069 at source level only. |
| <a id="ar-064"></a>AR-064 | Direct acceptance review/finding gate | **Source-confirmed target gap.** Direct caller supplies requirement-review arrays; common gate checks their coverage and feedback freshness, not retained assigned review or resolution of actionable findings. Automatic preflight adds those review restrictions. | Resolve required auditor provenance and finding disposition in the common gate; accepting agent supplies decision/reason, not substitute review evidence. |
| <a id="ar-065"></a>AR-065 | Interrupted fixed-source capture | **Preserve plus open recovery gap.** Sealed admission lets an already-started exact capture finish after Task/Attempt change. Host restart converts `sending` to `outcome_unknown`; the same exact call may rebuild the deterministic snapshot, but no current-state owner automatically resumes or settles it. | One bounded reconciliation/retry path for the original Operation and admission; no branch replay, new workflow or silent abandonment. |
| <a id="ar-066"></a>AR-066 | Immutable source snapshot | **Preserve.** Exact commit/tree/blob/mode, complete manifest/bytes and deterministic artifact identity are reverified. Symlinks, gitlinks, LFS pointers, case collisions and unsafe paths reject instead of disappearing. | Installed Git/platform and power-loss qualification; external build inputs remain explicit CheckProfile inputs. |
| <a id="ar-067"></a>AR-067 | Check command custody and recovery | **Preserve.** Exact resolved plan/profile/tool/input identity is retained before launch; Store acknowledges the exact process owner before Go; cancellation/restart retain one process group and never replay the command. | Live Windows/Linux/process-escape/load qualification and damaged-custody cases. |
| <a id="ar-068"></a>AR-068 | Parser coverage and process evidence | **Preserve.** A pass requires exit zero, source revalidation, complete parser-specific target identity/coverage, verified output artifacts and proven resource release. Acceptance reruns these checks for the exact required profile revision. | Only implemented parser families are covered; semantic requirement sufficiency still needs independent review where configured. |
| <a id="ar-069"></a>AR-069 | Check result reuse | **Preserve with strict provenance.** Reuse points directly to one original passed process row, forbids cache chains, matches resolved inputs/scope/coverage/result bytes and revalidates that original source as the same currently accepted project candidate. A cached row's null exit code is never trusted alone. | Live revocation/concurrency/large-cache qualification; retain original process/output/source evidence until consumers finish. |
| <a id="ar-070"></a>AR-070 | Two CheckRun executors | **Source-confirmed migration gap, not an evidence bypass.** `checks.executor=None` still sends new work through the kernel-host worker; a verified pin selects standalone `swarm-checks` with no fallback. Both converge on the same custody and completion validators. | Select standalone for all new work, disposition active legacy runs, then delete the hidden host executor/config/fixtures without weakening recovery. |
| <a id="ar-071"></a>AR-071 | Returned action identity | **Source-confirmed open defect.** Child request identity uses parent Operation + run + effect kind; it omits result position/payload. Two valid same-kind actions collide, so the later action is rejected as `SCRIPT_EFFECT_REQUEST_CONFLICT`. | Stable `(ScriptRun, action_index)` plus request digest; identical replay returns the retained action, changed payload conflicts. |
| <a id="ar-072"></a>AR-072 | Script execution/restart custody | **Preserve.** Immutable revision/invocation and represented Manager are rechecked; exact worker/process identity precedes Store Allow; restart reconciles the same family and never launches another interpreter. | Live platform/power-loss/process-escape/load qualification. |
| <a id="ar-073"></a>AR-073 | Parent terminal and child admission | **Preserve with future-effect boundary.** Result artifacts, parent terminal and current Store child mutations commit atomically; identical terminal replay admits no second child. | Native/external returned actions must create ordinary child Operations; no external I/O inside parent completion. |
| <a id="ar-074"></a>AR-074 | Per-action lifecycle and public readback | **Source-confirmed open.** Rejected actions exist only inside private parent result JSON; there is no indexed action record/retry. `script.describe` points to `operation.get`, but public `script.run` result is `not_projected`, hiding child Operation IDs and Store decisions. | Bounded ordered ActionRecords, ordinary child readback and retry only for the same unadmitted action. |
| <a id="ar-075"></a>AR-075 | Returned action vocabulary | **Source-confirmed target gap.** Current effects are TaskCreate, ManagerNotification and Task-owner mailbox message. The latter is not native Send. | Add only current-consumer typed Send/Reply/Configure/Goal actions through existing handlers; no arbitrary method/JSON passthrough. |
| <a id="ar-076"></a>AR-076 | Script executor selection | **Source-confirmed transition gap.** Missing executor pin is accepted into a durable queued run, then deterministically fails before start as `SCRIPT_EXECUTOR_UNSELECTED`. A compatibility launcher remains but delegates to standalone worker. | Reject before run admission, select standalone for all new work, disposition history and delete compatibility path. |
| <a id="ar-077"></a>AR-077 | Automation ScriptRun occurrence and transfer | **Preserve boundary.** Lifecycle triggers require exact observation/Operation/run/result/timestamp readback. Pending intent carries the original occurrence across transfer/edit and revalidates current Manager, script, source and optional Task scope before admission. | Preserve this identity while replacing copied cursor/journal machinery with the one loop. |
| <a id="ar-078"></a>AR-078 | ScriptRun cancellation | **Partial.** Queued ordinary cancellation atomically prevents start, marks the run terminal-before-start and emits one exact lifecycle event. Generic cancellation refuses in-flight work; no public token-addressed mid-run ScriptRun cancellation was found. | Persist exact cancellation intent, signal only the owned family, retain partial output and settle after proven departure; timeout is not cancellation. |
| <a id="ar-079"></a>AR-079 | Forge publication identity and readback | **Preserve.** One retained `forge.publish_ref` binds the accepted candidate, commit/tree, exact repository/ref, expected old target and create/update policy before worker authorization. Push uses exact force-with-lease semantics and settles only from exact remote-ref readback. | Keep this write-before-effect and readback boundary while deleting manifest/proof duplication; live provider/Git qualification remains separate. |
| <a id="ar-080"></a>AR-080 | Forge worker custody and restart | **Preserve plus boundary.** Per-target serialization, retained executable/plan/owner/result files, process-family custody and fair dispatch/reconcile cursors prevent a second push. Restart converts uncertain execution to readback-only and never launches another writer. | Damaged or uninspectable custody can hold the repository; preserve conservative isolation, but routine recoverable holds must continue through the ordinary reconciler rather than a manual release path. |
| <a id="ar-081"></a>AR-081 | GitHub API effect ownership | **Source-confirmed open split.** Managed labels execute through kernel-host `GhCli`, while PR title/body PATCH executes inside `swarm-forge-worker`; remote Git push is another Forge phase. The two GitHub API writes have different executable, custody and recovery owners. | Keep remote Git in Forge; move label and PR metadata writes to one GitHub API effect owner with one exact readback contract and no fallback writer. |
| <a id="ar-082"></a>AR-082 | Direct `GhCli` executable and process custody | **Source-confirmed open.** `github::client::run_gh_api` resolves bare `gh` at spawn time, uses a direct child with `kill_on_drop`, and retains no executable digest, PID/birth/group/Job or successor custody. A write may have happened before cancellation/restart, while only Operation state survives. | Admit a pinned installed GitHub writer, retain its exact process family through departure, and recover only by the original target readback; never repeat the API mutation. |
| <a id="ar-083"></a>AR-083 | Automatic managed-label unknown recovery | **Source-confirmed open.** Host restart marks `sending` label writes `outcome_unknown`, but `automation_github_projection::reconcile_effects_once` selects only `queued`; `call_automatic` refuses unknown work and requires the Manager-only reconciliation path. | The one current-state loop must admit or resume exact readback for automated unknown effects under retained on-behalf authority, without replaying `set_label` or requiring routine human action. |
| <a id="ar-084"></a>AR-084 | Post-write label completion authority | **Source-confirmed open.** After `sending`, both automatic readback and settlement re-run `GithubProjectionContext::require_current`, current accepted-candidate checks, source revision and current slot. Disable/edit/transfer/source refresh can therefore turn an exact observed desired state into `observed_but_unsettled`/`outcome_unknown`. | Separate pre-write authority from completion authority. After the effect boundary, immutable Operation/target/slot identity plus authorized readback must settle the original effect despite later policy or source changes. |
| <a id="ar-085"></a>AR-085 | PR description reconciliation | **Preserve; use as internal donor.** Production PATCH uses a pinned native worker and retained family custody. A separate readback Operation verifies exact repository/PR/head/base/publication and, after the authorized GET, records the observation without rechecking current GM rights. | Move the PATCH into the selected GitHub API owner while retaining this immutable-target and post-readback settlement pattern. |
| <a id="ar-086"></a>AR-086 | Publication/projection workflow copies | **Source-confirmed structural excess.** Publication and label projection each retain global/per-entry cursors, activation/catch-up state, recent records, pending/retry lists or an effect-drain cursor around already durable acceptance facts and Operations. | Replace with bounded current-state selection in the one host loop; keep occurrence/effect identity, exact slots and Forge/GitHub readback, not copied workflow lifecycle. |
| <a id="ar-087"></a>AR-087 | Automatic PR metadata path | **Source-confirmed missing caller.** `github.pull_request.update_description` is exposed as a manual Manager tool and tested as manual; publication automation reserves Forge push and label projection reserves managed labels, but no enabled policy action admits the PR metadata Operation. | When selected by policy, admit the same ordinary PR metadata Operation after exact publication; do not create another PR workflow or make routine metadata a human-only step. |

## Evidence checkpoints

Older detailed reasoning remains immutable instead of being recopied into the active
register. Use the current disposition above when an older snapshot is broader.

| Entries | Snapshot |
| --- | --- |
| AR-001–005 | [`c3e331a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md) |
| AR-006–008 | [`70b99db`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md) |
| AR-009–011 | [`1c61629`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md) |
| AR-012–016 | [`ec01905`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/docs/agent-operations/audit-register.md) |
| AR-017–019 and AR-016 correction | [`f5ccc41`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f5ccc418ba401733d1f778619abeec5c0554416b/docs/agent-operations/audit-register.md) |
| AR-020–024 | [`ac3389c`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ac3389c013840b02a78dc310bcee481d24086402/docs/agent-operations/audit-register.md) |
| AR-025–031 and AR-024 correction | [`9c47d88`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/9c47d88a09a543e9f0ea74228370833f14a6c619/docs/agent-operations/audit-register.md) |
| AR-032–035 | [`a0bc1dc`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/a0bc1dcb69fa4a2d3dea1e51e3c5b0d148bde1bb/docs/agent-operations/audit-register.md) |
| AR-036–040 | [`c0d1b91`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c0d1b91f15636982f977768474f7246b97308d61/docs/agent-operations/audit-register.md) |
| AR-041–044 | [`c2b998b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c2b998b079fe8517c592192c18d6ecbfb6b26aae/docs/agent-operations/audit-register.md) |
| AR-045–047 | [`3c4c859`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3c4c8599f1614460fe69d54d0b59715a26ada797/docs/agent-operations/audit-register.md) |
| AR-048–050 | [`73ba700`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/73ba7009bbc5c5515fe4c5f85743066e944106ef/docs/agent-operations/audit-register.md#trace-2026-10-23-retirement-admission) |
| AR-051–058 | [`4af640e`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/4af640ea6edc190d58a7320674c85cbc5bd3c6b3/docs/agent-operations/audit-register.md#trace-2026-10-23-goal-policy) |
| AR-059–064 | [`5b6b5be`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/5b6b5bee92123ce7d3252054c939fb18619b790f/docs/agent-operations/audit-register.md#trace-2026-10-23-review-acceptance) |
| AR-065–070 | [`8d8a446`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/8d8a44652d110f29031b2ddfdf6db98a5fc7001f/docs/agent-operations/audit-register.md#trace-2026-10-23-checkrun) |
| AR-071–078 | [`62874c8`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/62874c87ecc12af1544e1a6318bf78b5d687426a/docs/agent-operations/audit-register.md#trace-2026-10-23-scriptrun) |
| AR-079–087 | [Current source trace](#trace-2026-10-23-forge-github-effects) |

<a id="trace-2026-10-23-forge-github-effects"></a>
## Source trace at 38db378: Forge and GitHub remote effects

Inspected 2026-10-23. Paths are relative to the repository root. This is a connected
source read, not a live GitHub/Git execution or platform qualification.

### AR-079–080 — Forge already owns the correct remote-Git effect shape

`store/forge::reserve` admits one exact accepted-candidate publication. The retained
intent includes project, Task/Attempt, candidate/submission, commit/tree, canonical
repository, target ref, expected old target and create/update policy. Direct and
automation callers converge on this owner. Before external I/O, `begin` and
`dispatch_authorized` recheck the exact Operation, current accepted candidate, policy,
slot and prepared executable identity, then move only that Operation through the
durable effect boundary.

`swarm-forge-worker` performs one bounded push with exact force-with-lease semantics,
then reads the exact remote ref. The Store settles Applied only when that readback
equals the retained candidate commit. A transport/CLI failure does not authorize a
second push. `Store::supervise_forge_once` reconciles unknown work before dispatching
new work; separate rotating cursors and one oldest Operation per target keep dispatch
and readback bounded without letting one repository monopolize every pass.

Worker executable/plan/owner/result files and process-family receipts are retained.
Restart never starts another writer for the same Operation. An unresolved or
uninspectable family holds that repository instead of guessing it is free. Preserve
this conservative isolation and exact readback. Simplification should remove proof and
workflow copies, not the write-before-effect, force-with-lease, target lane, custody or
readback guarantees.

### AR-081–082 — GitHub API mutations have two owners and one weak process boundary

Managed Issue labels call `github::client::GhCli` directly from kernel-host.
PR title/body updates prepare `github_pr_description` work and execute the PATCH inside
`swarm-forge-worker`. Remote Git push is a distinct Forge phase, which is appropriate;
the two REST mutations are not. They currently have different executable selection,
process custody, timeout and recovery implementations.

`github::client::run_gh_api` spawns bare `gh`, uses a direct child with
`kill_on_drop(true)`, bounds stdout and timeout, and performs no Rust retry. Those are
useful local bounds, but no admitted executable digest, PID/birth/group/Job, private
owner record or successor process custody survives. Host startup can mark a retained
`sending` label/PR Operation `outcome_unknown`, but it cannot establish what happened
to the dropped direct `gh` family. Move both REST effects to one selected GitHub API
owner; keep Forge for remote Git only.

### AR-083–084 — automated label readback is not autonomous and can reject truth

Host recovery converts a `sending` managed-label Operation to `outcome_unknown`.
`automation_github_projection::reconcile_effects_once` selects only `queued`
Operations. `github_effects::call_automatic` explicitly refuses `outcome_unknown` and
points to the retained Manager reconciliation path. Therefore enabled automation does
not complete its own ambiguous write after restart even though the domain has an exact
read-only reconciliation method.

The problem continues after the write in the same call.
`validate_automatic_readback` and `persist_automatic_effect` invoke
`GithubProjectionContext::require_current`, revalidate the current accepted candidate,
registered source revision and current effect slot. If policy is disabled/edited, GM
epoch changes without the exact transfer continuation, or the source revision changes
after the write, an exact desired-state GET is stored as
`observed_but_unsettled`/`outcome_unknown`. Current authority is required before a new
write; it must not erase completion authority for an already admitted immutable effect.

### AR-085 — PR reconciliation contains the better completion pattern

Production PR PATCH uses the pinned native worker and records its process-family result.
An ambiguous original Operation is reconciled by a separate readback Operation bound to
the exact publication, repository, PR ID/number, published head, target/base refs and
requested title/body digest. After the authorized GET,
`resolve_reconcile_target_after_readback` intentionally rechecks immutable target and
slot facts without rechecking current GM rights; `persist_reconcile_readback` can then
settle the original unknown Operation. This is the internal donor for managed-label
completion after the REST writer is consolidated.

### AR-086–087 — copied workflow state remains, while PR metadata stays manual

`automation_publication` retains a global cursor, activation/catch-up state, a
per-entry pending/retry list and recent records around durable acceptance facts and
Forge Operations. `automation_github_projection` retains another global cursor,
per-entry projection state/recent list and a separate effect-drain cursor around
durable acceptance facts, effect slots and managed-label Operations. These are the
workflow copies implementation E/U already intends to remove; exact occurrence and
effect identity remain necessary.

`github.pull_request.update_description` is exposed and tested as a manual
Manager-scoped tool. Publication automation reserves Forge push; GitHub projection
reserves labels. No enabled policy consumer admits the same ordinary PR metadata
Operation. When policy selects that effect, it must use the existing exact
publication-bound command and the common GitHub API owner, not a third PR workflow.

## Inspected source boundaries for AR-079–087

| Source | Read boundary |
| --- | --- |
| `crates/swarm-kernel-host/src/store/forge.rs` | Admission/slot identity, begin/authorization, target lanes, dispatch/reconcile cursors, finish, process-tree hold and restart readback. |
| `crates/swarm-forge-worker/src/main.rs` | Push force-with-lease/readback and `github_pr_description` PATCH worker. |
| `crates/swarm-kernel-host/src/store/github_effects.rs` | Direct/automatic label admission, one write, readback, unknown recovery and settlement fences. |
| `crates/swarm-kernel-host/src/store/github_pr_effects.rs` | Worker-backed PATCH, retained target, exact reconciliation and post-GET settlement. |
| `crates/swarm-kernel-host/src/github/client.rs` | `GhCli` REST transport, executable spawn, timeout, response bounds and no-retry behavior. |
| `crates/swarm-kernel-host/src/automation/authorization.rs` | Retained GitHub projection context, current/transfer authority and source-revision checks. |
| `crates/swarm-kernel-host/src/store/automation_publication.rs` | Acceptance cursor, pending/retry state and Forge reservation. |
| `crates/swarm-kernel-host/src/store/automation_github_projection.rs` | Acceptance/projection state, queued-only effect drain and transfer relocation. |
| `crates/swarm-kernel-host/src/store/mod.rs` | Host-start conversion of interrupted GitHub writes and automation/Forge supervision order. |

Selected paths and symbols, not whole-file certification. External GitHub semantics,
installed `gh`, real remotes, process trees and power loss were not executed.

## Next work

Continue the GitHub source/import chain named in Resume here. Separate collection,
policy import and writes in the evidence before changing the target. Trace pagination,
high-water/source revision, Issue versus PR identity, Task mutation/work-pool coupling,
restart/transfer and remote-ref observation. Preserve bounded reads and immutable source
identity; do not keep collection as a Task/effect workflow or replace routine import
with operator steps.
