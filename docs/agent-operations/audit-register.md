# Audit register

Durable source-audit resume point: inspected revision, exact paths, findings, preserved
behavior and untraced boundaries. Not a runtime registry or second implementation
queue. [Implementation](implementation.md) owns changes;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / cancellation, resource disposal and read/control admission**.
- Inspected head: **`c0d1b91f15636982f977768474f7246b97308d61`**. Its comparison with
  `a0bc1dcb` changes only audit-register/launcher documentation. Earlier runtime
  findings remain open; this comparison is not a reread of every source.
- Overall audit: **incomplete**; no measured completeness percentage.
- Traced next-command selection/guards/quota hold, direct Open reservation, generic
  cancellation, held-workspace commit and later admission, workspace release,
  owned-service pre-start cancellation, current root policy and native/IPC disconnect.
- Added **AR-041–AR-044**; extended AR-039. Read/control quota bypass and the exact
  pre-spawn cancellation fence are preserved. Cancellation/rejection must also dispose
  attributable no-effect reservations, not merely change an Operation.
- Owning target: [launcher/queue](../swarm-launcher-assignment-context.md).
- **Next exact chain:** successful `agent.shutdown` / native close -> exact resource
  departure -> binding retirement -> root-count and workspace release. Trace externally
  attached services separately from owned processes, active children and native Goals.
  Then inspect launch cancellation after binding/Open attachment, route-edit budget
  continuity and the complete direct-call transaction/concurrency path. Never infer
  native death from socket close or a terminal Task.
- AR-044 confirms the predicate after lowering a limit, not an executed live reload.
  AR-042 covers a verified-held unbound lease, not every cancellation.
- Documentation only. No runtime change, Clippy, tests, benchmark, installed harness
  or independent verifier ran. External donors/SDKs were not freshly qualified.

On resume compare changed source and recheck callers/consumers. A search hit, read test
or docs commit is neither executed qualification nor a runtime repair.

## Evidence labels

**Source-confirmed** means a named path supports the consequence, not live reproduction.
**Preserve** identifies useful behavior. **Partial** names an untraced boundary. Earlier
rows remain inherited evidence unless explicitly reread.

## Findings and coverage

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| AR-001 | Antigravity reporting versus execution | **Open.** Report retries block native reducer; failure closes stdin and drained facts can remain memory-only. | Independent delivery and continuous native draining. |
| AR-002 | Complete result retention | **Open.** The 32-response cache can evict the only uncollected body. | Durable source lifetime and admission pressure. |
| AR-003 | Exact result identity | **Preserve.** Operation/conversation/generation and explicit unavailable oversized output. | Keep causality, delete package-proof copies. |
| AR-004 | Adapter restart | **Open.** New Antigravity Controller has empty body/delivery caches. | Original-Operation successor delivery, no prompt replay. |
| AR-005 | Admitted page completion | **Preserve.** Sealed normalized origin permits narrow late completion and replay ACK. | First historical admission and partial storage. |
| AR-006 | Continuation output | **Open, cross-route.** Generic origin/adapters remain Dispatch-centric; Muse item addressing does not fix Store admission. | Same source-Operation/page/candidate path for attributable Send. |
| AR-007 | Assembly and byte reads | **Preserve.** Publication/registration, contiguous coverage, digests and object grants. | Crash/power-loss and grant/cache races. |
| AR-008 | Authentication versus completion | **Preserve boundary.** Disabled credentials fail new handshake; authenticated carrier can finish a narrow prior page. | Revocation and successor delivery. |
| AR-009 | Send attribution | **Mixed.** Selected Manager/Repair/Goal paths retain scope; session input can be taskless. | Explicit task-linked versus taskless authority. |
| AR-010 | Repair delivery classification | **Open.** Requires provider-specific ID/condition and treats every Rejected/Cancelled as no effect. | Common admission/terminal facts; executed failure is not non-delivery. |
| AR-011 | Same-Attempt correction | **Preserve.** `needs_correction` can resubmit against revision/prior submission; stale publication stays historical. | Continuation origin through candidate validation. |
| AR-012 | Revision fences input | **Open.** Old Attempt survives revision; direct Send can become taskless or dispatch without revision recheck. | Supersede unsent old work; check exact subject before effect. |
| AR-013 | GM cleanup of old work | **Open.** GM can revise beneath another owner but cannot release that superseded Attempt. | Scoped historical cleanup without rewriting owner. |
| AR-014 | Release preserves capture | **Open.** Release cancels queued Result together with non-check Operations. | Cancellation by effect semantics. |
| AR-015 | Restart credential | **Preserve.** Stable binding credential; no implicit rotation. | Live restart/revocation. |
| AR-016 | Old-boot first delivery | **Open.** Unseen old-boot terminal fails current-boot validation; committed identical outcome may replay-ACK. | Execution origin separate from delivery actor. |
| AR-017 | Recovery-fenced capture | **Open; predicate reread.** Recovery filters Result before completion-only exception. | Exact completion/readback without native readiness. |
| AR-018 | Supervisor recovery | **Scope correction.** Intake/process recovery is not native-byte import. | Import stays with adapter/Store. |
| AR-019 | Inconclusive lookup completion | **Open.** Antigravity labels completed inconclusive Reconcile itself Unknown. | Applied lookup `resolved:false`; target stays unknown. |
| AR-020 | Terminal versus release | **Boundary.** Late terminal may update unreleased old Attempt; terminal does not prove capture. | Preserve output obligation and producer fences. |
| AR-021 | First historical Result | **Open.** New normalized admission requires current open unreleased Task/Attempt. | Source-scoped completion admission. |
| AR-022 | Automatic capture/readback | **Open.** Automatic consumers do not create Result/Reconcile; Doctor recommends only. | Bounded helpers using ordinary commands. |
| AR-023 | Supervisor readback | **Scope correction.** Unresolved IDs/status do not settle targets. | Semantic recovery in Store/adapter. |
| AR-024 | Page ACK/source release | **Open; target corrected.** Boolean ACK insufficient; local ACK-range ledger duplicates Store. | Store-authoritative complete/next-missing coverage. |
| AR-025 | Heterogeneous sources | **Boundary.** Local bytes/files and provider locators have different lifetimes. | Share ready-record delivery, not source journal. |
| AR-026 | Claude ready-page loss | **Open.** Intent persists, ready page does not; ambiguous ACK becomes Unknown and replay is refused. | Persist exact page before IPC; never repeat SDK input. |
| AR-027 | Codex delivery | **Preserve plus gaps.** Exact page survives restart; reads depend on Dispatch/current root. | Keep delivery, remove checkpoint/tombstones; see AR-032/033. |
| AR-028 | OpenCode retention | **Preserve plus gaps.** Exact replay/assistant parent; permanent `result_source` and Dispatch-only target. | Coverage-driven release without retention graph. |
| AR-029 | Command release | **Preserve plus excess.** Saved pages work; source cleanup uses release-proof Operations/tombstones. | Store-complete coverage then local cleanup. |
| AR-030 | Muse source read | **Preserve plus gap.** Exact item/revision/ref and native ranges; ready page checkpoint. | AR-034 bounds discovery. |
| AR-031 | Coverage owner | **Target correction.** Store owns pages; per-page offset is not aggregate completeness. | Exact coverage ACK/replay, no local union. |
| AR-032 | Historical source versus active root | **Boundary.** Root/family guards protect scope; deleting them cannot recover missing bytes. | Source-authorized reader and actual transition cases. |
| AR-033 | Codex read amplification | **Open.** Each new page rereads thread/history/turn/full body, then slices 24 KiB. Saved replay bypasses this. | Native ranges or one selected-result materialization. |
| AR-034 | Muse discovery budget | **Open.** Repeated `readItem` walk detects cycles but has no aggregate page/byte/time budget. | Resumable discovery and locator reuse. |
| AR-035 | Capture/disclosure grants | **Preserve.** Receipt, Bytes and Assemble differ; historical reviewer gets no new bytes. | Scoped first capture, no invented Operator or wider grants. |
| AR-036 | History-dependent capacity | **Open.** Global historical census/ledger verification precedes current root count. | Indexed current claims, no full-history admission scan. |
| AR-037 | Native-default Antigravity | **Open.** Root identity requires optional `modelId`; root policy can block an empty route. | Budget identity independent of optional model. |
| AR-038 | WorkDispatch saturation | **Open.** 128 pending subjects stop intake of later eligible Task facts. | Fair current-state scan, delete copied pending list. |
| AR-039 | Stale provider precedence | **Open; lifecycle narrowed.** Stale matching row outranks fresh Available. Eliot link loss retains rows; delivered native account/closed snapshot invalidates its own source. | Same-source supersession; retain independent quota constraints. |
| AR-040 | Root dedup/admission/scan | **Preserve.** Parent/Open/Dispatch/service share a claim; transaction handoff and stable scan exist. | Full direct concurrency and route-edit matrix. |
| AR-041 | Reads/control under quota hold | **Preserve plus earlier gate.** Result/Refresh/strict Reconcile and stop/reply controls bypass the starts-only quota predicate. Recovery/ready/scope gates still apply. | AR-017; shutdown in reconciling/draining states. |
| AR-042 | Cancel held unbound launch | **Open defect.** Generic Cancelled parent cannot satisfy workspace sweep's Settled no-effect contract; current verified-held unbound lease can stay fenced. | Stage-aware cancellation; read back in-flight workspace effects. |
| AR-043 | Pre-send Open rejection | **Open defect.** Guards reject unsent Open without retiring rootless opening binding; cancellation then refuses non-Queued target. | Known-no-effect rejection/cancellation must converge with resource disposal. |
| AR-044 | Lowered root limit and reuse | **Source-confirmed policy boundary.** Same-root input uses `other_claims >= limit`; lowering below live total can hold zero-root-growth continuation. | Separate root delta from input/quota/drain policy; live reload unqualified. |

## Current pass evidence

Sources below refer to the inspected head. Only named portions are covered.

### AR-041 — preserve the real read/control bypass

`store/runtime.rs::next_internal` (roughly 1263–1840) prioritizes control and permits
readback around unresolved ordinary mutations. Strict selected-module Reconcile is
read-only. Result, Refresh and strict Reconcile are not `starts_native_work`; neither
are steer, Goal pause/clear and reply controls. Only starts enter the later route/quota
predicate. A held start remains queued with a due time, before the sending CAS.

Tracking a read in the old capacity ledger does not prove it allocates a root. Earlier
fences still matter: recovery filters Result before its completion exception (AR-017);
readiness/authority/shutdown checks are independent. Connect the needed positive paths
rather than removing every guard.

### AR-042 — cancellation and workspace release do not converge

- `Store::reconcile_launches_once` in `store/mod.rs` (about 1260–1490) commits verified
  held workspace before a fresh transaction admits the Attempt/Open.
- `launcher::launch_after_workspace_held` can leave that parent queued on route Hold
  before attaching an Attempt/binding. A later pass reuses the held lease.
- `operations::cancel` (about 3850–4005) accepts a queued parent and writes Cancelled
  with `{reason,cancelled_by}`. It has no unbound-launch lease disposition; only the
  agent.open branch closes an opening binding.
- `workspace_lifecycle::candidates` selects a current unbound held lease for closed
  launch cleanup only for a Settled parent with native effect not attempted.
  `closed_launch_has_no_native_effect` separately requires Settled plus the exact
  domain launch result. Generic cancellation satisfies neither check.
- Root counting selects unbound launches only while Queued/Unknown: cancellation drops
  that root reservation, not the held workspace authority.

Thus cancelling a verified-held, current, unbound launch waiting for admission can leave
its workspace authority retained. This is not every cancellation or a live reproduction.
Later invalidation to stale does not make Cancelled satisfy the release helper.

Use the existing launch/workspace owner in cancellation. When preparation completed and
no child/native/service effect or unresolved resource remains, release that exact
reservation/lease authority in the cancellation transaction. Preserve files and history.
If Git/process work crossed its effect boundary, hold it for exact readback. Queued
parent does not mean no Git work started. No directory deletion, routine manual SQL or
new cleanup daemon.

### AR-043 — no-effect rejection needs resource disposition

`operations::reserve_open_route` (about 2940–3050) creates a rootless opening binding
and ties it to Open in a transaction. Another Open on its unreleased lane is refused.

Before `queued -> sending`, `runtime::next_internal` can reject that Open after caller
revocation, disabling new work or making the route Unavailable. Those traced branches
commit the rejected Operation without retiring its binding or synchronizing the old
capacity entry. Local cancellation then refuses the non-Queued target; its special
rootless-opening cleanup is unreachable. Root counting retains an unreleased claim or
reports unknown when its historical ledger no longer agrees.

This is a missing transition in those paths, not proof that all administrative recovery
is absent. Route known-no-effect rejection and safe cancellation through one existing
resource disposition. Inspect independent owned-service starts/children before closing
anything. Native Rejected may follow spawn: `outcome_with_artifacts` correctly keeps
that ownership/identity. Do not apply a generic Rejected-implies-free rule.

### AR-044 — root allocation and new input are different questions

`runtime::binding_route_admission` preserves native route/workspace identity and overlays
only current enablement/root policy. Keep that protection against silent retargeting.

Same-root counting excludes its own claim, but provider admission still requires other
claims < limit. Three live roots with a new limit of two therefore fail each same-root
check (two other claims). `next_internal` applies this to Dispatch/next-turn Send, not
only new roots. This is the confirmed predicate; live config reload was not executed.

Target root-limit reduction prevents positive root growth. Reuse with zero root growth
still obeys new-input, quota, Task and explicit drain policy. An intentional pause of
new turns must be explicit, not hidden in root arithmetic. Do not kill sessions or
hide unresolved claims to make the count fit.

### AR-039 follow-up — distinguish native invalidation from Eliot link loss

`runtime::disconnected` (about 3745–3820) marks its exact Eliot link disconnected and
sent/accepted Operations unknown; it does not invalidate provider rows. Permission
source freshness requires that connection to be connected, so its retained row can
become stale.

`provider_conditions::observe_usage` (385–535), in contrast, calls `invalidate_all`
when an accepted native snapshot reports account/updated or connection/closed. This
is source-binding scoped. A fresh binding updates its own rows, not the old binding's
copy. That establishes a path to the audited stale-rank-2 versus Available-rank-1
conflict for matching shared resources; an already-delivered native invalidation is
not that case.

Keep exact account/auth attribution and independent buckets. Neither deleting all
negative facts on IPC loss nor selecting the globally newest timestamp repairs it.
Full release/revocation/reboot/account-change combinations remain unqualified.

## Useful disposal mechanisms to retain

`operations::fail_reserved_owned_service_start` CASes the existing start row only before
its process boundary; Unknown/observed starts refuse release, departed/no-effect ones
can proceed. `prepare_owned_service_attempt_release` uses it for linked queued work.
Preserve that race boundary, not all duplicate proof encodings. The complete coordinator
process path was not reread here.

`workspace_lifecycle::workspace_resource_use` retains holds for unresolved effects,
checks, producers, exact unreleased binding and owned service. Attempt release is not
native departure. Connect real disposition to these owners instead of deleting fences
or freeing resources by age.

## Inspected sources

| Source | Boundary read |
| --- | --- |
| `store/runtime.rs` | queue selection, starts/guard/hold/rejection, sending CAS, route overlay, Open outcome branches, disconnect/hello. |
| `store/operations.rs` | Open reservation, owned-service pre-start cancellation, linked release and generic cancel. |
| `store/mod.rs::reconcile_launches_once` | held-workspace commit before final admission; external I/O and lifecycle invocation. |
| `store/workspace_lifecycle.rs` | candidate SQL, closed-launch release test, transitions and resource holds. |
| `store/capacity.rs::root_claims_for_route` | unreleased binding/unbound launch selection and same-root exclusions. |
| `store/provider_conditions.rs` | root gate, source identity, native invalidation and retained condition entry. |

## Remaining coverage

Next: shutdown/close/binding retirement, then route edits and full concurrency.
Workspace/process custody, Goals/scheduling, review/acceptance, permissions, migrations,
IPC fairness, external writes and observation remain open tracks. Historical capture
across revocation, source discovery/indexes and installed restart/Windows/load are not
closed by this pass.

Prior detailed evidence and donor references remain in the
[preceding register](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c0d1b91f15636982f977768474f7246b97308d61/docs/agent-operations/audit-register.md)
and its linked passes. All earlier dispositions remain above. Docs do not close runtime
findings.
