# Audit register

Durable source-audit resume point: inspected revision, exact paths, findings, preserved
behavior and untraced boundaries. Not a runtime registry or second implementation
queue. [Implementation](implementation.md) owns changes;
[status](../implementation-status.md) records landed behavior.
[Audit index](audit-index.md) provides direct evidence checkpoints and source/target links.

## Resume here

- Checkpoint: **2026-10-23 / interruption recovery, evidence indexing and status reconciliation**.
- Verified repository head: **`3c4c8599f1614460fe69d54d0b59715a26ada797`**.
  Both interrupted passes were published: `c2b998b` contains AR-041–AR-044;
  `3c4c859` contains AR-045–AR-047. Neither changed runtime source.
- Last connected source pass: **scoped shutdown, adapter demand and resource retirement**,
  inspected at **`c2b998b079fe8517c592192c18d6ecbfb6b26aae`** and published in `3c4c859`.
  The evidence below is retained from that pass, not presented as a fresh full audit.
- Runtime baseline: **`c92120ed70ca7e625440a8a19517c481e73b13c9`**, 2026-10-21.
  The compared interval through the verified head contains 16 documentation-only
  commits. This establishes unchanged source, not exhaustive source coverage.
- Overall audit: **incomplete**. There are **47 entries**, including defects, preserved
  mechanisms and partial boundaries; no measured completeness percentage.
- This checkpoint indexes AR-001–AR-047 with stable `ar-NNN` anchors, flattens the
  evidence-history links and updates status/navigation. It adds no new defect, marks
  none repaired and does not advance the unfinished cancellation trace.
- **Next exact chain:** cancellation after a launch has attached its Attempt/Open
  binding → child cancellation/late outcome → owned-service disposition → binding
  retirement → workspace release. Then route-edit budget continuity and the complete
  direct-call transaction/concurrency path. Trace the ordinary finalizer before
  claiming that a guarded historical record can never be released.
- Source/target entry points: [index](audit-index.md#source-and-target-map),
  [launcher/queue contract](../swarm-launcher-assignment-context.md), implementation B/C/D.
- Still unqualified: actual shutdown/restart; native Goal/child quiescence after
  external detach; crash without clean-stop receipts; successor stop authority;
  post-shutdown historical capture. No duplicate execution or respawn loop is claimed.
- Documentation only. No runtime change, Clippy, tests, benchmark, installed harness,
  external donor requalification or independent verifier ran. Local network fetch
  failed DNS; repository reads and publication use the GitHub connector.

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
| <a id="ar-011"></a>AR-011 | Same-Attempt correction | **Preserve.** `needs_correction` can resubmit against revision/prior submission; stale publication stays historical. | Continuation origin through candidate validation. |
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
| <a id="ar-040"></a>AR-040 | Root dedup/admission/scan | **Preserve.** Parent/Open/Dispatch/service share a claim; transaction handoff and stable scan exist. | Full direct concurrency and route-edit matrix. |
| <a id="ar-041"></a>AR-041 | Reads/control under quota hold | **Preserve plus earlier gates.** Reads/strict Reconcile and stop/reply controls bypass starts-only quota. | Recovery/ready/scope barriers; AR-017 and AR-045. |
| <a id="ar-042"></a>AR-042 | Cancel held unbound launch | **Open.** Cancelled parent fails workspace sweep's Settled no-effect contract; verified-held unbound lease can stay fenced. | Stage-aware cancellation and in-flight workspace readback. |
| <a id="ar-043"></a>AR-043 | Pre-send Open rejection | **Open.** Guard rejects unsent Open without retiring rootless opening binding; cancel refuses non-Queued target. | Known-no-effect disposition through existing resource owners. |
| <a id="ar-044"></a>AR-044 | Lowered root limit/reuse | **Open policy mismatch.** `other_claims >= limit` can hold zero-root-growth continuation after limit reduction. | Separate root delta from input/quota/drain policy; live reload unqualified. |
| <a id="ar-045"></a>AR-045 | Shutdown when execution is not Ready | **Source-confirmed open barrier.** Admission and pre-effect validator require Ready; recovery queue independently excludes Shutdown. | Authorize exact resource cleanup by custody/scope, not new-work readiness. |
| <a id="ar-046"></a>AR-046 | Explicit detach versus adapter demand | **Source-confirmed open integration gap.** Applied transport shutdown leaves native identity retained; demand still anchors attachment to settled Open and holds its lease. | Separate desired attachment from native occupancy and output obligations; no respawn-loop claim. |
| <a id="ar-047"></a>AR-047 | Departure and binding/workspace disposal | **Preserve plus partial handoff.** Historical service departure reader survives stale Task/owner; its writer changes service row only. Unreleased binding independently holds workspace/root. | Connect ordinary retirement after all relevant facts; neither auto-release on socket close nor delete guards. |

## Last connected source trace: AR-045–AR-047

Retained from the pass inspected at `c2b998b` and published at `3c4c859`. The indexing
checkpoint has not rerun it. Earlier details are directly linked in the
[audit index](audit-index.md#evidence-checkpoints).

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

`swarm-contracts::module_command` limits Shutdown to `external_attach` or
`owned_sdk_transport`. Neither claims native turn, Task or process-family completion.
Codex `shutdown_operation` persists intent, closes its own WebSocket, sets
`native_retired` and returns `owned_resource_released` only after its close condition.
Unknown close also retires its local transport; it is not replayed as a fresh effect.

The Store outcome path settles that Operation but has no shutdown-specific binding
disposition. `runtime::disconnected` retains root/scope and moves a ready/opening
binding to Reconciling. Preserve that unexpected-disconnect behavior; it is not a
complete implementation of intentional retirement.

`module_demand::operation_candidates` selects settled/unknown original Open for every
unreleased binding with native root/scope, even without unresolved input.
`scope_readback` reports retained native identity whenever either native coordinate is
present, irrespective of proven module-family departure. In the consumer,
`StandaloneSupervisor::reconcile_demands` keeps seen demand; it drops a stale held lease
only when native identity and pending Operations are both absent. Explicit transport
Shutdown does not remove attachment demand in this chain.

This does not prove duplicate input, repeated spawn or that the external root stopped.
Native Goal/children may still work. Separate desired attachment from observed native
occupancy using existing binding/Operation ownership. After selected detach, old Open
alone cannot request productive reattachment; saved-output/readback and passive
observation keep their bounded obligations. New attachment requires authorized policy,
not erased history or another supervisor.

### AR-047 — retain historical cleanup; connect the last disposition

`launcher_owned_service::departure_source` reconstructs exact admitted service,
parent/Open, original Attempt and lease from retained provenance. It deliberately does
not require current Task, current manager or still-held lease. Preserve this historical
cleanup mechanism rather than inventing another authority registry.

`Store::reconcile_owned_opencode_departures_once` builds read-only departure evidence,
CASes `persist_departure`, increments a count and returns. That CAS changes only
`owned_service_starts` to `service_departed`; it does not close binding or settle native
inputs. The native reader expects exact process/clean-stop evidence. Process exit,
transport close and successful model work remain different facts.

`workspace_lifecycle::workspace_resource_use` independently holds exact unreleased
binding, unresolved Operations, CheckRun resources, producers and owned-service rows.
Service departure removes the service hold, not the binding hold. Root count likewise
counts unreleased bindings independently. Do not release workspace unconditionally:
a shared service, another writer, unknown input or uncaptured local output may remain.

Join these facts in the existing disposition path. When actual obligations resolve,
retire binding claim and let workspace lifecycle release its authority, preserving
files/results. Never invent native outcomes from OS exit. Finalizer/administrative
callers and crash without clean-stop receipts remain open; not every resource has
been proved unreleasable.

## Inspected source boundaries for that trace

| Source | Read boundary |
| --- | --- |
| `store/runtime.rs` | Shutdown validator/callers; recovery selection; Open/Dispatch/Recover outcome branches, generic settlement and exact-link disconnect. |
| `swarm-contracts/src/module_command.rs` | Shutdown scopes/non-guarantees; command/demand vocabulary. |
| `swarm-adapter-codex/src/lib.rs` | `shutdown_operation`, close condition, saved intent and intent-only unknown recovery. |
| `store/module_demand.rs` | pending/native-root candidate queries, scope readback, family-departure predicate and pending projection. |
| `swarm-supervisor/src/standalone.rs` | demand/readback consumer, held-lease removal, registry obligations and outer-loop exit. |
| `store/launcher_owned_service.rs` | start/readback ownership, departure loop, historical source, proof validation and CAS. |
| `store/workspace_lifecycle.rs` | release transitions, closed-launch boundary and `workspace_resource_use` holds. |
| `store/operations.rs` | owned-service pre-start fence, linked cancellation and rootless binding closure. |
| `runtime/opencode_v2/owned_service.rs` | helper custody, graceful close and read-only retained handle; recovered handle does not gain original stop authority. |

Selected paths, not whole-file audit. External SDK behavior, live OS trees and native
Goals were not executed. This checkpoint additionally sampled public Operation
projection/readback diagnostics in `operations.rs`; those reads did not close the
post-binding cancellation chain and create no new finding.

## Next work and retained evidence

Continue the post-binding cancellation chain in Resume here. Its completion must show
both safe refusal after a possible external effect and eventual release after actual
obligations are resolved. Follow ordinary callers/triggers before adding any cleanup
worker. Then trace route edits and direct admission races.

[Coverage still open](audit-index.md#coverage-still-open) names the remaining areas;
[evidence checkpoints](audit-index.md#evidence-checkpoints) provides all prior detailed
traces directly. Findings keep their IDs and dispositions across interruptions.
Documentation updates do not repair runtime; installed/native/Windows/load evidence
remains separate.
