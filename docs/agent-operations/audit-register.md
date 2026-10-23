# Audit register

Durable source-audit resume point: inspected commit, paths, confirmed/refuted findings,
useful mechanisms and untraced boundaries. Not a runtime registry, approval gate or
second implementation queue. [Implementation](implementation.md) owns the work order;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / historical source reads, capture cost and object authority**.
- Inspected head: **`9c47d88a09a543e9f0ea74228370833f14a6c619`**. The comparison from
  `f5ccc418` contains two documentation-only commits; earlier runtime findings are not
  repaired by that interval.
- Overall audit: **incomplete**. No measured completeness percentage.
- Read selected Codex/Claude/OpenCode/Muse result readers, root/family guards, Store
  normalized-origin admission, retained object grants and the base Operation/artifact
  schema. Rechecked original specification sections 6–8 for bounded I/O, fair progress,
  drain and no native start during readback.
- Added **AR-032–AR-035**. Historical reading is not permission to remove root checks.
  The target now distinguishes source lookup, bounded byte reading and exact page
  delivery; a temporary selected-result file is not a transcript mirror.
- **Next exact chain: queue/root capacity B.** Trace
  `capacity::root_claims_for_route` -> launcher route admission/reservation ->
  `automation_work_dispatch::reconcile` -> `runtime::next_internal`, including two
  concurrent launch admissions, one root represented by several Operations, quota hold,
  release and blocked first-page candidates. Check the actual caller/transaction before
  declaring a double count, race or starvation defect.
- Remaining result boundaries: reachable cross-root/account transitions, first historical
  capture grant across revocation/handover, source discovery for every retained route,
  coverage query indexes and live restart/power-loss behavior. This pass does not close
  those by deleting a guard or drawing a new schema.
- Documentation only. No runtime change, Clippy, test, installed harness, benchmark or
  independent verifier ran. Source evidence is not executed qualification.

On resume compare source changes with this head and revisit changed callers/consumers.
Keep findings until repaired with connected evidence. A search hit or a read test is
not a completed runtime check.

## Evidence labels

**Source-confirmed** means the named path was read and supports the stated consequence,
not live reproduction. **Preserve** means useful existing behavior. **Partial** names
an untraced boundary; **inherited** means earlier evidence not reread in this pass.
Review, implementation and executed verification are separate.

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
| AR-032 | Historical source versus active session | **Source-confirmed boundary, not a universal reachable root-swap bug.** Codex/Claude compare active root; OpenCode fixes root per binding generation; Muse requires observed family membership. | Source-authorized reader with original service/account scope; retain custody checks and prove actual transition cases. |
| AR-033 | Codex per-page read amplification | **Open efficiency defect.** Each new page rereads thread/history/turn/full final body, then slices at most 24 KiB. Exact saved-page replay bypasses this. | Native range read or one bounded selected-result materialization; measure later. |
| AR-034 | Muse discovery budget | **Open boundedness defect.** Each result range calls `readItem`; its backward walk has cycle detection but no total page/byte/time budget. | Bounded resumable discovery; reuse exact item/ref instead of repeating the scan. |
| AR-035 | Historical capture and disclosure grants | **Preserve plus target clarification.** Receipt, Bytes and Assemble grants differ; exact result caller is capped at Bytes, historical reviewer gets no new bytes. | Derive first historical capture under current scoped authority; capture cannot create a new grant or widen assembly rights. |

## Current pass evidence

All paths below were read at the inspected head unless a historical revision is named.
These are selected connected portions, not whole-file coverage claims.

### AR-032 — do not remove identity checks to repair historical reading

- `swarm-adapter-codex/src/lib.rs::build_normalized_result_page` checks saved input/turn,
  current root, service scope, workspace and current requested/effective model facts.
- `swarm-adapter-opencode/src/lib.rs::handle_assistant_result` requires the selected
  session to match the command root and original input. `set_root_hint` / `verify_hello`
  enforce a deterministic binding-generation root. That is a custody boundary, not
  evidence of an arbitrary root replacement bug.
- `modules/muse/bridge.mjs` invokes `readResult` only for root/observed children.
- `swarm-adapter-claude/src/lib.rs::handle_result` requires the current SDK session and
  reads `NativeControl::result_for_operation`; a new process starts with empty control
  state. Removing the root comparison cannot recreate missing bytes.

Separate the immutable source locator from the currently active executor. A historical
reader must still use an authorized provider/service/account/workspace and the original
Operation/binding/native output. Never route an old page through an unrelated new
binding, call Open/Resume to make a read pass, or accept a caller-supplied arbitrary
session. Saved ready pages remain Store deliveries independent of this discovery path.
Reachability of every root/account transition is still unqualified.

### AR-033 — bounded page output can hide repeated full reads

[Codex reader](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/9c47d88a09a543e9f0ea74228370833f14a6c619/crates/swarm-adapter-codex/src/lib.rs#L6341-L6742):
`read_thread`, `read_history`, `read_turn`, then `read_final_assistant_response` precede
byte slicing and full-body hashing on each new page Operation. If the body has B bytes
and requires P pages, the selected body is materialized approximately P times, in
addition to history work. This is a source-level cost observation, not measured latency.
The earlier command handler reuses a saved pending page; transport retry is not the cause.

Use a real native range API when available. Otherwise materialize only the selected
immutable final output once, page that private body with existing file primitives, and
release it after Store coverage. This temporary source is not a conversation replica,
generic cache service or new journal. Keep identity/hash checks on actual output bytes.

### AR-034 — one-page memory is not a bounded history lookup

[Muse reader](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/9c47d88a09a543e9f0ea74228370833f14a6c619/modules/muse/results.mjs):
`readItem` loops backward with `limit:100`, a cycle-detection Set and exact item/revision
matching. It has no aggregate page/byte/elapsed-work bound. `readResult` calls it before
every range, including output-ref reads that subsequently use native `item/readOutput`.
A long missing-item lookup can traverse the whole available history; unique cursors also
keep growing the Set. Other Muse recovery limits do not bound this helper.

Retain the useful native range/digest checks and refusal to fetch arbitrary URI/path.
Separate bounded locator discovery from byte reading. A budget stop retains the exact
continuation cursor in the existing source/read action and reports partial coverage,
not absence or success. Reuse the located item revision/ref on following pages. Do not
mirror events or repeatedly restart at the newest page.

### AR-035 — preserve object grants instead of borrowing execution authority

Read `store/object_scope.rs::identity_for_attempt`, `current_manager_task_basis`,
`resolve_task_read`, `artifact_level_for_relation`, `exact_operation_artifact_grant`,
`native_source_grant` and `resolve_assembly_sources`.

The retained Attempt supplies frozen revision/binding identity. Current Manager/GM
Task/project relations and exact result/input callers are separate positive read bases.
Exact-caller artifact access is capped at Bytes; it does not alone authorize Assemble.
Historical reviewer receipt rights do not reopen candidate bytes. Module credentials
cannot inspect arbitrary artifacts. Assembly intersects the source-page grants.

Reuse these identity and policy owners for first historical Result admission, without
requiring the old Task to be current and without treating Operation receipt visibility
as a byte grant. The missing capture authorization remains a scoped Store action, not
an invented Operator principal or a new permission registry. New native work still
requires current Task/Attempt authority. Capture/submission/acceptance stay separate.

## Capture-query contract clarified, not implemented

The base schema has authoritative Operations, Attempts, artifacts and observations;
it has no complete-capture flag that this pass can honestly query as implemented.
[Module API](module-api.md#current-state-correctness-loop) specifies the next action as
one current-state projection: exact output-ready source, missing coverage, no active
same-source read, current capture authority and due retry. Reserve an ordinary Result
in the existing mutation transaction, then perform provider/file I/O outside Store.

Two important gaps are explicit: the first page may require adapter-local exact-output
locator discovery, and a completed failed read cannot be retried forever under the same
immutable rejected request key. These are read attempts, not native prompt retries.
No capture queue, copied Task state or adapter ACK-range ledger is introduced.

## Inspection and preservation limits

| Source | Read boundary |
| --- | --- |
| `store/normalized_result.rs` | descriptor checks, live first admission and retained result validation entry. |
| `store/object_scope.rs` | retained identity, Manager/GM/participant/reviewer grants, exact callers, Bytes/Assemble distinction and source intersection. |
| `swarm-adapter-codex/src/lib.rs` | saved-page command short circuit; normalized body lookup, current-root/config checks, history reads and byte slicing. |
| `swarm-adapter-opencode/src/lib.rs` | exact assistant selector/input/parent, root hint, route and hello validation. |
| `swarm-adapter-claude/src/lib.rs` | startup empty NativeControl, selected current-session result lookup, body/page publication. |
| `modules/muse/results.mjs` / selected `bridge.mjs` caller | exact revision search, family-membership predicate, range and output-ref validation. |
| `migrations/001_core.sql` | Operation identity/due/state, Task/Attempt and artifact/observation owners; not every schema extension/index. |
| Original `docs/agent_swarm.md` at `f1b854b6`, sections 6–8 | bounded service work, fair ready-target progress, drain preserving results, readback without native start. |

The original specification supports these constraints; its older mutually exclusive
Goal wording is not restored over the owner's newer independent-Goals decision.
External donor repositories/installed SDKs were not freshly requalified in this pass.
Existing Eliot implementations were inspected as positive/negative donors.

## Next work and remaining coverage

Start with queue/root-capacity B as named above. Then continue workspace/process,
Goals/scheduling, review/acceptance, public-object permissions, migrations, IPC fairness,
external writes and independent observation. Historical read/capture revocation and
multi-root reachability remain named open boundaries, not reasons to invent another
framework before reading those owners.

Earlier detailed evidence, including the result-source matrix and anti-donor list,
remains in the [preceding register](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/9c47d88a09a543e9f0ea74228370833f14a6c619/docs/agent-operations/audit-register.md)
and its linked prior passes. All AR-001–AR-031 dispositions remain above. A docs commit
is not a runtime repair; installed/native/Windows/load qualification remains separate.
