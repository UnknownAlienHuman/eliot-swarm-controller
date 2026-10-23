# Audit register

Durable source-audit resume point: inspected revision, exact paths, findings, preserved
behavior and untraced boundaries. Not a runtime registry or second implementation
queue. [Implementation](implementation.md) owns changes;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / scoped shutdown, adapter demand and resource retirement**.
- Inspected head: **`c2b998b079fe8517c592192c18d6ecbfb6b26aae`**. Its difference from
  `c0d1b91` is documentation only. The preceding interrupted pass already recorded
  AR-041–AR-044; this pass continues rather than repeating it.
- Overall audit: **incomplete**; no measured completeness percentage.
- New rows **AR-045–AR-047** trace Shutdown admission/effect fencing, Codex transport
  release, Store settlement/disconnect, module demand and its supervisor consumer,
  owned-service departure and independent workspace holds.
- Owning target updated: [launcher/queue](../swarm-launcher-assignment-context.md).
  An explicit control detach, native-work completion and resource retirement are
  different facts. Do not clear a root to stop adapter demand or restore Ready merely
  to make cleanup callable.
- **Next exact chain:** cancellation after a launch has attached its Attempt/Open
  binding -> child cancellation/late outcome -> owned-service disposition -> binding
  retirement -> workspace release. Then route-edit budget continuity and the complete
  direct-call transaction/concurrency path. Trace the ordinary finalizer before
  claiming that a guarded historical record can never be released.
- Still unqualified: actual shutdown/restart cycles; native Goal/child quiescence after
  external detach; crash without clean-stop receipts; successor stop authority;
  post-shutdown historical capture. No duplicate model execution or respawn loop is
  claimed from the demand predicate alone.
- Documentation only. No runtime change, Clippy, tests, benchmark, installed harness,
  external donor requalification or independent verifier ran.

On resume compare changed source and recheck callers/consumers. A search hit, read test
or docs commit is neither executed qualification nor a runtime repair.

## Evidence labels

**Source-confirmed** means a named path supports the consequence, not live reproduction.
**Preserve** identifies useful behavior. **Partial** names an untraced boundary. Earlier
rows remain inherited evidence unless explicitly reread. Open findings close only with
connected runtime evidence, not with a proposed replacement.

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
| AR-017 | Recovery-fenced capture | **Open.** Recovery filters Result before completion-only exception. | Exact completion/readback without native readiness. |
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
| AR-039 | Stale provider precedence | **Open.** Stale matching row outranks fresh Available. Eliot link loss retains rows; accepted native account/closed snapshot invalidates its own source. | Same-source supersession, independent quota constraints and remaining lifecycle cases. |
| AR-040 | Root dedup/admission/scan | **Preserve.** Parent/Open/Dispatch/service share a claim; transaction handoff and stable scan exist. | Full direct concurrency and route-edit matrix. |
| AR-041 | Reads/control under quota hold | **Preserve plus earlier gates.** Reads/strict Reconcile and stop/reply controls bypass starts-only quota. | Recovery/ready/scope barriers; AR-017 and AR-045. |
| AR-042 | Cancel held unbound launch | **Open.** Cancelled parent fails workspace sweep's Settled no-effect contract; verified-held unbound lease can stay fenced. | Stage-aware cancellation and in-flight workspace readback. |
| AR-043 | Pre-send Open rejection | **Open.** Guard rejects unsent Open without retiring rootless opening binding; cancel refuses non-Queued target. | Known-no-effect disposition through existing resource owners. |
| AR-044 | Lowered root limit/reuse | **Open policy mismatch.** `other_claims >= limit` can hold zero-root-growth continuation after limit reduction. | Separate root delta from input/quota/drain policy; live reload unqualified. |
| AR-045 | Shutdown when execution is not Ready | **Source-confirmed open barrier.** Admission and pre-effect validator require Ready; recovery queue independently excludes Shutdown. | Authorize exact resource cleanup by custody/scope, not new-work readiness. |
| AR-046 | Explicit detach versus adapter demand | **Source-confirmed open integration gap.** Applied transport shutdown leaves native identity retained; demand still anchors attachment to settled Open and holds its lease. | Separate desired attachment from native occupancy and output obligations; no respawn-loop claim. |
| AR-047 | Departure and binding/workspace disposal | **Preserve plus partial handoff.** Historical service departure reader survives stale Task/owner; its writer changes service row only. Unreleased binding independently holds workspace/root. | Connect ordinary retirement after all relevant facts; neither auto-release on socket close nor delete guards. |

## Current pass evidence

Sources below were read at the inspected head. Named portions, not whole-file audits.

### AR-045 — cleanup is gated by execution readiness

`store/runtime.rs::validate_agent_shutdown_request` checks the exact binding/generation
and closed resource scope, then rejects every state except `ready`. Both
`user_command_with_actor` and the `next_internal` guard call it. Before that guard,
`next_internal` selects only Recover/Reconcile when `recovery_required` is true.
Thus an admitted Shutdown can also become undispatchable after a state transition.
Its control priority and quota bypass do not override these earlier fences.

Use current actor/object rights and exact resource custody for cleanup. Do not mark a
failed session Ready or replay Open merely to release its transport. Conversely, a
non-ready state does not authorize an unknown process kill. Keep the native scope's
real supported stop/detach semantics and preserve uncertain effects for readback.

### AR-046 — a saved root is not a request to reconnect forever

`swarm-contracts::module_command` explicitly limits Shutdown to `external_attach` or
`owned_sdk_transport`. Neither claims native turn, Task or process-family completion.
Codex `shutdown_operation` persists intent, closes its own WebSocket, sets
`native_retired` and returns `owned_resource_released` only after its close condition.
Unknown close also retires its local transport; it is not replayed as a fresh effect.

The traced Store outcome path settles that Operation but has no shutdown-specific
binding disposition. `runtime::disconnected` then retains root/scope and moves a
ready/opening binding to Reconciling. That conservative unexpected-disconnect behavior
must remain; it is not a complete implementation of intentional retirement.

`module_demand::operation_candidates` separately selects settled/unknown original Open
for every unreleased binding with a native root/scope, even without unresolved input.
`scope_readback` reports retained native identity whenever either native coordinate is
present, irrespective of a proven module-family departure. In the consumer,
`StandaloneSupervisor::reconcile_demands` keeps seen demand; it drops a stale held lease
only when both native identity and pending Operations are absent. An explicit transport
Shutdown does not remove this attachment demand in the traced chain.

This does not prove duplicate input, repeated process spawn or that the external root
has stopped. Its native Goal/children may still be working. Separate desired control
attachment from observed native occupancy using the existing binding/Operation owner.
After selected detach, old Open alone cannot request a productive reattachment; saved
output/readback and passive observation retain their own bounded obligations. A new
attachment needs an authorized policy action, not erased history or another supervisor.

### AR-047 — retain historical cleanup; connect the last disposition

`launcher_owned_service::departure_source` reconstructs the exact admitted service,
parent/Open, original Attempt and lease from retained provenance. It deliberately does
not require a current Task, current manager or still-held lease. This is a useful donor
for historical cleanup, not a reason to invent another authority registry.

`Store::reconcile_owned_opencode_departures_once` builds read-only departure evidence,
CASes `persist_departure`, increments a count and returns. That CAS changes only
`owned_service_starts` to `service_departed`; it does not close binding or settle native
inputs. The native reader expects exact process/clean-stop evidence. Process exit,
transport close and successful model work remain different facts.

`workspace_lifecycle::workspace_resource_use` independently holds the exact unreleased
binding, unresolved Operations, CheckRun resources, producers and owned-service rows.
A service-departed row removes the service hold, not the binding hold. The inherited
root-count path likewise counts unreleased bindings independently of service state.
Do not turn the service transition into an unconditional workspace release: a shared
service, another writer, unknown input or uncaptured process-local output may remain.

The target joins these facts in the existing disposition path. When actual resource
obligations are resolved, retire the binding claim and let workspace lifecycle release
only its own authority, preserving files and results. Native outcomes are never invented
from OS exit. The complete finalizer/administrative caller matrix and crash without
clean-stop receipts remain open; this pass does not assert every resource is unreleasable.

## Source coverage for this pass

| Source | Read boundary |
| --- | --- |
| `store/runtime.rs` | Shutdown validator and call sites; recovery queue predicate; Open/Dispatch/Recover outcome branches, generic settlement and exact-link disconnect. |
| `swarm-contracts/src/module_command.rs` | Shutdown scopes and explicit non-guarantees; command/demand vocabulary. |
| `swarm-adapter-codex/src/lib.rs` | `shutdown_operation`, connection-close condition, saved intent and intent-only unknown recovery. |
| `store/module_demand.rs` | pending/native-root candidate queries, scope readback, managed-owner family-departure predicate and pending projection entry. |
| `swarm-supervisor/src/standalone.rs` | demand/readback consumer, held-lease removal, registry obligations and outer loop exit. |
| `store/launcher_owned_service.rs` | start/readback ownership, bounded departure loop, historical source, departure proof validation and CAS. |
| `store/workspace_lifecycle.rs` | release transitions, closed-launch boundary and all `workspace_resource_use` holds. |
| `store/operations.rs` | owned-service pre-start fence, linked Operation cancellation and rootless binding closure. |
| `runtime/opencode_v2/owned_service.rs` | exact helper custody, graceful close and retained read-only handle; recovered handle does not acquire original helper's stop authority. |

The last row is a custody boundary, not a fresh audit of the whole native departure
reader. External SDK behavior, live OS process trees and native Goals were not executed.

## Next work and retained evidence

Continue the post-binding cancellation chain named above, then route edits and direct
admission races. Follow actual callers and triggers before proposing another cleanup
worker. Goals/scheduling, review/acceptance, full permissions, migrations, IPC fairness,
external effects and independent observation still have open boundaries. Historical
capture across revocation, query indexes and installed/Windows/load remain unqualified.

Earlier detailed traces and donor references remain in the
[preceding register](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c2b998b079fe8517c592192c18d6ecbfb6b26aae/docs/agent-operations/audit-register.md)
and its linked passes. All AR-001–AR-044 dispositions are retained above.
