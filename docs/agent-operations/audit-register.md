# Audit register

Durable source-audit resume point: inspected commit, paths, confirmed/refuted findings,
useful mechanisms and untraced boundaries. Not a runtime registry, approval gate or
second implementation queue. [Implementation](implementation.md) owns the work order;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / queue progress and root-capacity admission**.
- Inspected head: **`a0bc1dcb69fa4a2d3dea1e51e3c5b0d148bde1bb`**. The preceding commit
  changed documentation, not the runtime findings carried below.
- Overall audit: **incomplete**; no measured completeness percentage.
- Traced selected root-count, route-admission, WorkDispatch and launch-reservation
  paths, plus provider-condition selection and Antigravity's native-default model.
- Added **AR-036–AR-040**. Root deduplication and the existing stable launch scan are
  preserved; history-dependent capacity and the separate pending-subject cap are not.
- Owning target updated: [launcher/queue](../swarm-launcher-assignment-context.md).
  Implementation B remains the only implementation order.
- **Next exact chain:** `runtime::next_internal` route-admission hold and control/read
  bypasses -> direct `operations::open` and launcher callers ->
  `launcher::fail_launch`, workspace release, binding release and
  `owned_service_starts` disposition -> surviving root claims. Check cancellation,
  rejected/unknown Open, configured-limit reduction and route edits before asserting
  either a leaked reservation or a safe release.
- Also trace provider-source invalidation on release/disconnect/account change. AR-039
  proves precedence when old and fresh rows coexist; it does not claim every release
  leaves that pair or that independent quota windows may be discarded.
- Documentation only. No runtime change, Clippy, test, installed harness, benchmark or
  independent verifier ran. Selected transaction bodies were read; the complete direct
  caller/concurrency matrix was not executed or exhaustively traced.

On resume compare source changes with this head and revisit changed callers/consumers.
A finding remains open until repaired with connected evidence. A search hit or read
test is not a completed runtime check.

## Evidence labels

**Source-confirmed** means the named path supports the consequence, not live
reproduction. **Preserve** means useful existing behavior. **Partial** names an
untraced boundary; **inherited** means earlier evidence not reread in this pass.
Review, implementation and executed verification remain separate.

## Findings and coverage

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| AR-001 | Antigravity reporting versus native execution | **Open.** Report retries block the terminal reducer; failure reaches stdin shutdown and drained facts can remain memory-only. | Independent Store delivery and continuous native draining. |
| AR-002 | Complete result retention | **Open.** The 32-response cache can evict the only uncollected body. | Durable source lifetime and pressure before new admission. |
| AR-003 | Exact result identity | **Preserve.** Operation/conversation/generation and unavailable oversized-output semantics. | Keep causality while deleting package-proof copies. |
| AR-004 | Adapter restart | **Open.** New Antigravity Controller has empty body/delivery caches. | Original-Operation successor delivery, no prompt replay. |
| AR-005 | Admitted page completion | **Preserve.** Sealed normalized origin permits narrow late completion and exact replay ACK. | First historical admission and partial storage. |
| AR-006 | Continuation output | **Open, cross-route.** Generic origin and selected adapters remain Dispatch-centric; Muse item addressing does not repair Store admission. | Same source-Operation/page/candidate path for attributable Send. |
| AR-007 | Assembly and byte reads | **Preserve.** Publication/registration, contiguous coverage, digests and object grants. | Crash/power-loss and remaining grant/cache races. |
| AR-008 | Authentication versus completion | **Preserve boundary.** Disabled credentials fail new handshake; an authenticated carrier has narrow prior-page completion. | Revocation and successor delivery. |
| AR-009 | Send attribution | **Mixed.** Selected Manager/Repair/Goal paths retain Operation scope; session-only input can be taskless. | Explicit task-linked versus taskless authority. |
| AR-010 | Repair delivery classification | **Open.** Requires provider-specific ID/condition and maps every Rejected/Cancelled to no effect. | Common admission/terminal facts; executed failure is not non-delivery. |
| AR-011 | Same-Attempt correction | **Preserve.** `needs_correction` can resubmit against expected revision/prior submission; stale publication stays historical. | Continuation origin through candidate validation. |
| AR-012 | Revision fences input | **Open.** Old Attempt survives revision; direct Send can become accidentally taskless or dispatch without revision recheck. | Supersede unsent old work and check exact subject before effect. |
| AR-013 | GM cleanup of old work | **Open.** Current GM can revise beneath another owner but then cannot release that superseded Attempt. | Historical cleanup authority without rewriting owner. |
| AR-014 | Release preserves capture | **Open.** Release cancels queued Result together with non-check Operations. | Cancel by effect semantics, not broad method exclusion. |
| AR-015 | Restart credential | **Preserve.** Stable binding-scoped credential; no implicit token rotation. | Live restart/revocation. |
| AR-016 | Old-boot first delivery | **Open.** Unseen old-boot terminal fails current-boot validation; identical committed outcome may replay-ACK. | Execution origin distinct from delivery actor. |
| AR-017 | Recovery-fenced capture | **Open.** Recovery predicate filters Result before completion-only exception. | Exact completion/readback without native readiness. |
| AR-018 | Supervisor recovery | **Scope correction.** Intake/process-demand recovery is not native-byte import. | Keep import in adapter/Store owners. |
| AR-019 | Inconclusive lookup completion | **Open.** Antigravity labels a completed inconclusive Reconcile itself Unknown. | Applied lookup with `resolved:false`; target still unknown. |
| AR-020 | Terminal versus release | **Boundary.** Late terminal can update an immutable unreleased old Attempt; terminal does not prove capture. | Preserve output obligation and producer fences. |
| AR-021 | First historical Result | **Open.** New normalized admission requires current open unreleased Task/Attempt. | Source-scoped completion admission; retain sealed late-page path. |
| AR-022 | Automatic capture/readback | **Open.** Current automatic consumers do not create Result/Reconcile; Doctor recommends only. | Bounded current-state helpers through ordinary commands. |
| AR-023 | Supervisor readback | **Scope correction.** Unresolved IDs/status do not settle targets. | Store/adapter semantic recovery. |
| AR-024 | Page ACK and source release | **Open; target corrected.** Boolean ACK is insufficient. An adapter-local ACK-range ledger would duplicate Store. | Store-authoritative complete/next-missing coverage. |
| AR-025 | Heterogeneous sources | **Boundary.** Local body/file and provider locator have different lifetimes. | Share ready-record delivery, not source journal. |
| AR-026 | Claude ready-page loss | **Open.** Intent is journaled but ready page bytes are not; ambiguous ACK becomes Unknown and replay is refused. | Persist exact ready page; never replay SDK execution. |
| AR-027 | Codex delivery | **Preserve plus gaps.** Exact page params survive restart; new reads are Dispatch/current-root dependent. | Keep delivery, remove full checkpoint/tombstones; AR-032/033 qualify source reading. |
| AR-028 | OpenCode retention | **Preserve plus gaps.** Exact page replay and assistant parent; permanent `result_source` and Dispatch-only target. | Coverage-driven release, no retention graph. |
| AR-029 | Command release | **Preserve plus excess.** Exact saved page works; source cleanup has release-proof Operations/tombstones. | Store-complete coverage then local cleanup. |
| AR-030 | Muse source read | **Preserve plus qualification.** Exact item/revision/output ref and native range validation; ready page checkpoint. | AR-034 identifies unbounded repeated discovery; not all native reads are bounded. |
| AR-031 | Coverage owner | **Target correction.** Store owns pages; per-page next offset is not aggregate source completeness. | Exact coverage ACK/replay; no local range union. |
| AR-032 | Historical source versus active session | **Boundary, not a universal reachable root-swap bug.** Codex/Claude compare active root; OpenCode fixes root per binding generation; Muse requires observed family membership. | Source-authorized reader with original service/account scope; prove actual transitions. |
| AR-033 | Codex per-page read amplification | **Open.** Each new page rereads thread/history/turn/full final body, then slices at most 24 KiB. Saved-page replay bypasses this. | Native ranges or one bounded selected-result materialization; measure later. |
| AR-034 | Muse discovery budget | **Open.** Each range calls `readItem`; backward walk detects cycles but has no aggregate page/byte/time budget. | Bounded resumable discovery and reuse of exact item/ref. |
| AR-035 | Historical capture/disclosure grants | **Preserve plus target clarification.** Receipt, Bytes and Assemble differ; exact result caller is capped at Bytes; historical reviewer gets no new bytes. | Current scoped capture authority, not invented Operator or widened grants. |
| AR-036 | Root capacity depends on history | **Source-confirmed defect, open.** Global binding/ledger census and per-scope historical Operation roster precede root counting. Historical limits/damage can yield unknown capacity despite no corresponding live claim. | Indexed current-claim projection; no full-history ledger verification on admission. |
| AR-037 | Antigravity native-default root limit | **Source-confirmed defect, open.** Service-less root identity requires `modelId`, while the selected adapter explicitly permits its absence. With root policy enabled, the count is unknown even before the first root. | Stable configured budget identity independent of optional model selection. |
| AR-038 | WorkDispatch pending saturation | **Source-confirmed defect, open.** At 128 pending subjects, an entry stops reading later Task facts; repeated checks of persistent blockers do not admit a later eligible Task. | Current-state fair candidate scan; remove copied pending subjects rather than raise cap. |
| AR-039 | Stale provider row outranks fresh availability | **Source-confirmed predicate defect, open.** For matching retained shared-resource rows, stale source ranks 2 and current Available ranks 1; cross-binding same-source refresh need not supersede the old row. | Source/bucket-scoped supersession; trace lifecycle invalidation; retain independent constraints. |
| AR-040 | Existing admission/dedup/scan mechanisms | **Preserve, selected paths.** Root set deduplicates normal binding/open/dispatch/service representations; launch callback rereads/reserves in its transaction; pending launches use stable keyset/ceiling. | Full direct caller/concurrency, failure-disposal and route-edit matrix remain open. |

## Current pass evidence

Sources below refer to `a0bc1dcb`; line spans locate this inspected revision, not future
worker instructions. Only the named portions were read.

### AR-036 — an activity limit has become a history limit

`store/capacity.rs`:

- `collect_scopes` (around 2960–3070) enumerates all bindings, including closed ones,
  with a 10,001-row sentinel, then loads the ledger roster. Overflow creates global
  damage and `unknown:capacity-roster-limit`.
- `roster_check_inner` (around 3130–3305) enumerates every Operation and Attempt for
  the selected bindings, not just unresolved work; it also counts producers toward
  the 10,000 visited-row limit and compares the duplicated evidence ledger both ways.
- `root_claims_for_route` (around 3898 onward) calls those routines before assembling
  live root identities. Global roster damage returns `None`. Its owned-service scan
  also includes terminal rows before applying its bounded-row decision.
- `launcher::route_admission` passes that `None` to
  `provider_conditions::route_admission`, which returns `ROUTE_CAPACITY_UNKNOWN`
  for an enabled root policy.

Thus more than 10,000 retained bindings can hold policy-governed routes even when the
old bindings are closed. A sufficiently large completed Operation history can hold
its target scope as well. No production incident or measured latency is claimed.
Even below the limit, admission repeats history decoding/ledger validation rather than
querying only the resource claims that can still consume capacity.

Replace that dependency with a current-claim projection over existing launch/binding
owners, counted in the admission transaction. Index the actual active predicate and
budget key. Keep genuinely unresolved roots, but exclude closed/disposed history.
Do not solve this with a larger cap, history deletion, periodic ledger repair or a
second capacity database. Diagnostic history remains independently pageable.

### AR-037 — optional model is required by an unrelated gate

`modules/antigravity-rust/src/launch.rs::{selected_model_id,build_launch_spec}` returns
`None` for absent `modelId` and deliberately omits `--model`. In
`capacity.rs::root_route_identity` (around 3450–3525), the selected Antigravity artifact
instead requires `root_route_text(options.get("modelId"))`. The missing field returns
`None`, propagated by `root_claims_for_route` before any existing-root query.

For an otherwise valid service-less Antigravity route with `admission_policy`, this
makes even an empty root roster unknown. The defect does not imply every Antigravity
configuration is blocked; a supplied model or different scope takes another path.

Count the configured root budget by its retained route/resource identity. Model and
provider observations still matter to selection/quota, but omission of a supported
native-default override cannot erase root identity. Do not force a fake explicit model
or infer an account/provider from a model name. Route edits must not detach still-live
claims from the budget they actually reserve.

### AR-038 — bounded storage silently becomes queue eligibility

`store/automation_work_dispatch.rs::reconcile_entry` (around 466–559) rechecks at most
eight pending subjects, then calls `consume_work_fact_page` only while
`state.pending.len() < MAX_PENDING_SUBJECTS` (128). If those subjects remain blocked,
new facts past the entry cursor are never considered. A later Task whose dependencies
or write scope are ready cannot get past that local backlog.

This is not lack of every kind of fairness: `enabled_entry_page` rotates automation
entries and the separate launcher scan already has stable pagination. They cannot
free a saturated entry's pending list.

B4 should query current Tasks/Attempts and eligibility, not replay every creation fact
into another pending queue. Filter cheap eligibility before LIMIT; continue a bounded
keyset scan for checks that need inspection. Retain only the host loop's scan position,
not copied Task state. A blocked source remains in its owning Task/dependency domain.

### AR-039 — availability is not a vote among binding histories

`provider_conditions.rs::route_admission` (around 888–1071) loads matching shared-scope
rows. It assigns stale source rank 2, current Available rank 1, current Hold rank 3 and
Unavailable rank 4. A stale old binding therefore wins over a fresh Available row when
both match the same resource. `source_binding_matches` rejects released bindings;
`invalidate_all`, `clear_absent_bucket_facts` and `delete_exact` operate on the selected
source binding/generation rather than superseding another binding's row.

The decision-rule mismatch is confirmed for that retained input state. Whether each
release/disconnect path leaves those rows is not fully traced. A stale independent
quota window or a genuinely ambiguous account is not equivalent to a superseded copy.

Use the latest valid observation of the actual source/account/bucket/window, with its
source epoch and applicable ordering. Then combine independent current constraints.
Do not let an old duplicate veto its successor, choose only the newest global timestamp,
or convert all unknown facts to Available. Reset/retry time makes a bounded refresh or
policy-selected probe due; it is not proof that quota recovered.

### AR-040 — preserve useful mechanisms, not their proof scaffolding

`capacity::root_claims_for_route` uses a `BTreeSet<RootClaimIdentity>`: the same native
root or pre-root binding is not counted again for its Open, Dispatch and owned-service
records. An admitted unbound launch is a temporary root claim; after binding it must
be the same reservation, not another slot. Same-root exclusions are applied after the
sources are collected. The ADMISSION_METHODS ledger includes reads, but that alone
is **not** evidence that each Result/Refresh consumes another root slot.

`launcher::admit_work_dispatch` and `launch_for_actor` reread the plan and reserve the
ordinary launch in the supplied Store transaction. `launch_after_workspace_held`
rechecks admission and carries it through the Attempt/Open-child transition in that
same transaction, avoiding a count of its own half-constructed parent. Preserve atomic
check/reserve and exact subject authority; remove duplicate manifests/digests around
those facts, not the checks themselves. The complete outer direct-call transaction
and concurrent native qualification remain unverified here.

`launcher::{pending_launch_ceiling,pending_launches}` (around 4030–4125) already scans
outstanding launches using immutable `(created_at_ms, operation_id)` and a fixed cycle
ceiling. Updating `updated_at_ms` while held cannot move the candidate back ahead of
that cursor. `launch_reconciliation_snapshot` excludes timestamp-only admission
refreshes from semantic progress. Reuse this pattern; do not invent another scheduler
or describe this existing scan as the WorkDispatch pending-cap defect.

## Source/target map

| Inspected owner | Target action |
| --- | --- |
| `store/capacity.rs` scope collection, root identity/count and roster checks | B3: retain unique current-root semantics; delete evidence-ledger/history dependency. |
| `store/launcher.rs` route predicate, WorkDispatch admission, launch/Open handoff and stable pending scan | B0/B4: one eligibility projection, transactional admission and fair scan. |
| `store/automation_work_dispatch.rs` activation, pending recheck and source-page gate | B4: delete copied pending subjects/cursors with the working current-state path. |
| `store/provider_conditions.rs` shared-source selection, freshness and binding-local invalidation | B2: source-scoped observations and explicit freshness/supersession. |
| Antigravity `launch.rs` optional model handling | Preserve native-default choice while repairing B3 root attribution. |
| Retained Claude-script review: `issue_settled.py`, `plan_next.py`, `state.py` | Keep actionable dependency ranking and omission of parked/review-waiting work until relevant change; not a separate queue. |

The script donor was read through the retained
[review](../remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md), not a freshly opened
private ZIP. External donor repositories and installed SDKs were not requalified.

## Remaining coverage

Continue the exact failure/disposal and control-read chains at the top. Then workspace
custody, Goals/scheduling, review/acceptance, public-object permissions, migrations,
IPC fairness, external writes and independent observation remain open tracks.
Historical capture revocation, root/account changes, coverage query indexes and live
restart/power-loss are still unqualified; this pass does not close result work merely
because the focus moved to B.

Detailed AR-001–AR-035 evidence and prior pass links remain in the
[preceding register](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/a0bc1dcb69fa4a2d3dea1e51e3c5b0d148bde1bb/docs/agent-operations/audit-register.md).
All dispositions remain in the table above. A documentation commit is not a runtime
repair or complete current-head qualification.
