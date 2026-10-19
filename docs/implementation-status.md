# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** `main` contains one completed runtime reduction and a
source-backed refactoring specification. Runtime source is unchanged since
`25a8e3b`; commits after it through the revision studied here,
`9c6192a70382bb12a842f5b960cab904f95908a7`, change documentation.

[Architecture](agent-operations/architecture.md) is the compact manifesto.
[Implementation](agent-operations/implementation.md) is the only work order.
[Findings](agent-operations/refactoring.md) explain existing excess.
[Donor map](agent-operations/donor-map.md) records reusable mechanisms and limits.

## Completed runtime change

Commit `25a8e3b` removed the host's duplicate general CLI and MCP launcher.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

No later documentation pass is counted as runtime reduction.

## Current executable order

### A — Antigravity vertical slice

| Slice | Target |
| --- | --- |
| A0 | Make v6 the sole active new-work coordinate across source, descriptor, route, package, installer and qualification. |
| A1 | Make terminal outcome self-contained; remove observation ID, duplicate result observation and dispatch proof packet. |
| A2 | Persist exact terminal Store envelopes/result pages until ACK; observations use replaceable source checkpoints. Never replay native input. |
| A3 | Retain only real semantic actions and remove placeholder handlers/receipt identity repetition. |

### B — queue, route and dispatch

| Slice | Target |
| --- | --- |
| B0 | One deterministic queue projection with `why_ranked`. |
| B1 | Ordered explicit route aliases; lane is a view/configuration, not a stored workflow entity. |
| B2 | Source-scoped provider usage/availability without Codex method/artifact proof matrices. |
| B3 | Transactional root count/reservation; remove the per-operation capacity ledger. |
| B4 | `dispatch_once` over current state; delete WorkDispatch cursors/pending copies/semantic slots. |

### C — write ownership

| Slice | Target |
| --- | --- |
| C0 | Repository-relative Task path prefixes define write scope. |
| C1 | Workspace lease is the sole write authority; delete `code.scope.*`. |

### D — process custody

| Slice | Target |
| --- | --- |
| D0 | Minimal owner record: binding/boot, executable, credential ref, PID/birth and process group. |
| D1 | Conservative restart reconciliation; kill only exact owned leftovers. |

### E — scheduling

| Slice | Target |
| --- | --- |
| E0 | One automation-entry schedule model; remove static ScheduleConfig registry. |
| E1 | One in-process wake/timer admission loop over current Store state. |
| E2 | Delete standalone automation scheduler service/process/credentials/page-admit protocol. |
| E3 | Keep optional workers only for real external effects/custody. |

Subsequent slices type the adapter protocol, consolidate one executor per harness,
preserve the two Goal mechanisms, finish passive observation, unify result/review and
remove optional product engines.

## Confirmed source gaps

| Area | Current source condition |
| --- | --- |
| Antigravity | Terminal result can be lost after native completion; outcome depends on a Store observation and proof packet; v5/v6 active selectors disagree. |
| Adapter bus | String/vendor commands, artifact/schema profile ladders, repeated bootstrap/hello/delivery loops and proof-heavy receipts remain. |
| Execution | Parallel OpenCode, Muse, Codex, Claude, Antigravity and Command generations remain. |
| Queue | WorkDispatch copies Task facts into cursor/pending/semantic-slot state and can stall behind 128 blockers. |
| Routes | Fallback policy is not a compact explicit ordered route list. |
| Provider state | Shared DTOs are Codex-specific; transient facts can hold forever after retry/reset; source binding death creates sticky holds; Muse Rust source is rejected by allowlist. |
| Capacity | A second `meta` ledger mirrors Operation phases and counts readback/control methods as capacity. |
| Workspace | Advisory `code.scope.*` coexists with the actual workspace lease/write conflict gate. |
| Scheduling | In-process scheduler, standalone automation scheduler and another automation loop overlap; eleven optional worker slots add lifecycle machinery. |
| Observation | Unbound inventory/joined Governor view is incomplete; process metrics repeatedly hash executables. |
| Results/review | OpenCode missing-parent association remains incomplete; direct and automatic acceptance do not share one candidate-specific evidence gate. |
| Store | Most pure reads use the writer; base SQL text SHA gates open; automation identity depends on owner/name and transfer relocates journals. |
| Product breadth | Concilium, integration ratification/ACK and automatic publication remain in source. |
| Tooling | Qualification retains compiler/checkout/cache equality, artifact-specific validators and proof-packet checks. |

These are findings and deletion targets, not completed runtime repairs.

## Runtime evidence retained

Existing receipts remain scoped to their named artifacts/revisions:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the planned compact protocol, every recovery path or installed
fleet.

## Verification status

The latest study changes documentation only. Runtime implementation starts with
formatting and minimal affected warnings-denied Clippy/build checks, then the
connected cases in
[implementation verification](agent-operations/implementation.md#verification).

Do not treat older broad CI/fixtures as qualification of planned protocol, provider
state, capacity deletion, scheduling consolidation or executor retirement. Record
each landed source slice and net owned-code change here.
