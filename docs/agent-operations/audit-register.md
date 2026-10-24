# Audit register

Durable source-audit resume point. This file owns finding dispositions and the exact
continuation point; [implementation](implementation.md) owns source changes,
[status](../implementation-status.md) owns landed behavior, and
[the audit index](audit-index.md) owns navigation to immutable evidence.

## Resume here

- Checkpoint: **2026-10-23 / fixed source, CheckRun execution, cache and acceptance handoff**.
- Source inspected at **`5b6b5bee92123ce7d3252054c939fb18619b790f`**.
  Runtime source is still **`c92120ed70ca7e625440a8a19517c481e73b13c9`**
  from 2026-10-21; the intervening **20 commits are documentation-only**.
- Last connected chain:
  `store/checks::{reserve_source,begin_source,finish_source}` →
  `checks::source` → `store/checks::{reserve,next,ready}` →
  `swarm-checks` / legacy `checks::worker` → completion/recovery →
  cache selection/revalidation → `acceptance::evidence`.
  New evidence is [AR-065–070](#trace-2026-10-23-checkrun).
- Overall audit: **incomplete, 70 entries**. Entries include defects, preserved
  mechanisms, scope corrections and partial boundaries; they are not 70 bugs or a
  measured completion percentage.
- Preserved in this pass: exact immutable Git snapshots; Store-Go-before-command;
  process-group custody; parser-specific coverage; byte/result/process revalidation;
  direct-original-only cache lineage; two-phase acceptance revalidation.
- Open in this pass: `source.capture` has no automatic successor/reconciliation owner
  after host interruption, and new CheckRuns still have two executor implementations
  selected by local configuration.
- **Next exact chain:** `store/scripts::{reserve,next,ready,finish}` →
  `scripts::runner` / `swarm-script-worker` → typed returned actions →
  child Operation admission/readback → cancellation/restart/transfer →
  automation consumers. Trace one exact ScriptRun through every returned action and
  prove that restart cannot rerun either the script or an already admitted child.
- Runtime implementation order remains **A1b–A3, then B–V**. The source audit does not
  reorder implementation.
- Still unqualified: live Windows/Linux process behavior, power loss, installed
  providers, load/fairness, the autonomous end-to-end work cycle and every boundary
  listed in the index.
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
| AR-065–070 | [Current source trace](#trace-2026-10-23-checkrun) |

<a id="trace-2026-10-23-checkrun"></a>
## Source trace at 5b6b5be: fixed source, CheckRun and cache

Inspected 2026-10-23. Paths are relative to `crates/swarm-kernel-host/src` unless
qualified. This is a connected source read, not an executed CheckRun.

### AR-065–066 — fixed-source identity is strong; automatic recovery is missing

`store/checks::reserve_source` freezes the Operation, caller, Task/revision/Attempt,
absolute repository, exact commit and workspace-lease authority. `begin_source`
rechecks current scope in an Immediate transaction and moves only the exact queued or
unknown Operation to `sending`. `finish_source` validates the sealed admission and
deterministic `source-<operation digest>` artifact, but intentionally no longer
requires the Task/Attempt to remain current. An already-started capture can therefore
finish historically instead of being discarded by a later revision or release.

`checks/source` reads one exact commit/tree through bounded Git object operations. It
records every accepted regular file's path, mode, blob identity, length and digest,
rejects unsupported or ambiguous entries, publishes immutable bytes and verifies the
whole manifest again before Store settlement. It does not fetch, checkout, stage,
execute hooks or include uncommitted edits.

On host startup, every `sending`/`native_accepted` Operation becomes
`outcome_unknown`. `Store::capture_source` is the only production executor found: it
performs reserve → begin → off-DB Git/file work → finish in the requesting call.
`begin_source` permits the same exact unknown Operation to repeat this read-only,
deterministic capture, but no scheduler/recovery owner selects it automatically.
Do not add a source workflow. Reuse the original Operation/admission and either settle
the already published deterministic artifact or rerun the exact commit read under a
bounded current-state reconciler.

### AR-067 — one command, one admitted owner, no replay

Check admission resolves the trusted profile, candidate/baseline, executable and
toolchain bytes, effective argv/environment, dependency scope and parser targets before
the final Immediate reservation. A changed fact invalidates the prepared plan.

Both current executors use `swarm_checks::execute_with_plan`. The worker enters its
owned Job/process group and publishes exact process identity before reading the Store
Go file. Missing or uncertain Go never starts the command. Execution uses direct argv,
`env_clear`, the admitted working directory and bounded output files. Cancellation is
token/CheckRun-addressed. Host restart changes `running` to `reconciling`; recovery
reads retained launch/worker/terminal/completion facts, observes exact family departure,
and never launches a replacement command.

Unknown or live process ownership holds only that target resource. Resource release
requires confirmed group emptiness or exact retained departure proof; elapsed time,
a free lock or a signal request is insufficient.

### AR-068 — a pass is more than exit zero

`checks/worker::validate_passed_coverage` rejects any gap. `ExitCode` requires the exact
`process_exit` coverage. `CargoJson` additionally requires the complete expected target
set, unique captured package/manifest/source/kind identities, matching emitted
artifacts, `build_finished=true` and zero parser errors.

Completion verifies source bytes again, all result/output artifacts, profile identity,
input fingerprint, scope plan, process receipt and resource release. A passed row must
have exit code zero and `source_checkout_verified=true`.

`store/acceptance::evidence` repeats the exact required profile/revision and parser
coverage checks, verifies the settled applied CheckRun Operation and loads every result
and output artifact into the byte manifest. This extends AR-063: the common acceptance
gate is backed by a strongly validated producer. It does not resolve AR-064's missing
common assigned-review/finding provenance.

### AR-069 — reuse follows the original process, not the cached row

A cache candidate must be one original passed row: no `cached_from`, exit zero, released
resource, reusable profile, reproducible/versioned inputs, exact cache key, resolved
inputs, scope plan, coverage, process receipt, result and output bytes. Its source
candidate must still be the same accepted candidate for the project.

After off-DB byte verification, the final admission transaction reruns the cache
identity check before creating the derivative row. The derivative has no process or
exit code of its own by design. Acceptance follows `cached_from_check_id` back to the
original row, rejects chains, revalidates the original Operation/process/coverage/
outputs and compares current accepted-source provenance with the frozen cache receipt.
Therefore `exit_code=null` on the derivative is not an acceptance shortcut.

### AR-070 — the duplicate executor is transitional, not evidence authority

`CheckConfig.executor` is optional. With no pin, new CheckRuns still spawn the
kernel-host worker. With a verified absolute executable/SHA/artifact/version pin,
`standalone_host` launches `swarm-checks` and never falls back. Both routes share the
same resolved plan, Store-Go control, process-group executor and completion validators.

Implementation K already selects standalone `swarm-checks` for new work. Migration must
first select it everywhere and disposition concrete old runs, then remove the in-host
worker, fallback configuration, fixtures and dependency edges. Do not keep both merely
because their evidence is equivalent.

## Inspected source boundaries

| Source | Read boundary |
| --- | --- |
| `store/checks.rs` | Source reserve/begin/finish; CheckRun plan/reservation; pending selection; Go/cancel; completion/recovery; cache selection and final recheck. |
| `checks/source.rs` | Git object resolution, manifest/path rules, artifact publication and full byte verification. |
| `checks/worker.rs` | Resolved work, Store control adapter, coverage parser, completion validation, release/recovery. |
| `checks/standalone_host.rs` | Executor pin verification, one-use plan materialization and standalone execution receipt. |
| `swarm-checks/src/lib.rs` | Process-group entry, Store-Go gate, direct command execution, bounded capture, cancellation and family departure. |
| `checks/model.rs` | Profile and executor configuration, trusted environment/tool constraints and request shape. |
| `store/acceptance.rs`, `swarm-kernel/acceptance.rs` | Exact profile/check/cache lineage, artifact manifest and begin/finish revalidation. |
| `store/mod.rs` | Host-interruption transition and synchronous source-capture caller. |

Selected paths, not whole-file certification. Git/provider/OS behavior was not executed.

## Next work

Continue with the ScriptRun chain named in [Resume here](#resume-here). Preserve
CheckRun's useful custody and evidence model, but do not copy its worker journal or
proof vocabulary into scripts. One script execution and each returned typed child need
separate, attributable, non-replayed effects.

The [audit index](audit-index.md#coverage-still-open) owns remaining coverage.
Documentation updates close no runtime finding.
