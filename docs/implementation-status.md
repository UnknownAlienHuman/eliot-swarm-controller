# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** `main` contains one completed runtime reduction and the
current architecture/implementation preparation. Runtime source has not changed since
commit `25a8e3b`; later commits through the source revision studied here,
`4cfbbf84`, change documentation.

[Architecture](agent-operations/architecture.md) is the compact product manifesto.
[Implementation](agent-operations/implementation.md) is the only work order.
[Source findings](agent-operations/refactoring.md) explain the current excess.
[Donor map](agent-operations/donor-map.md) records reusable mechanisms and limits.

## Completed source change

### CLI ownership reduction

Commit `25a8e3b` removed the host's duplicate general application CLI and MCP
launcher. Public `swarm` commands remain in `swarm-cli`; the host retains daemon,
local administration and internal-worker entrypoints.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- existing parser-test section retained.

This is the only runtime-code reduction in the current preparation series.

## Current implementation order

### Adapter route

1. **A0:** synchronize active Antigravity Rust selectors on v6.
2. **A1:** make terminal outcome self-contained; remove observation ID/result copy and
   `AntigravityDispatchSessionProof`.
3. **A2:** add a minimal route-local durable Store-delivery spool; terminal/result
   envelopes are immutable files, observation is one replaceable source checkpoint.
4. **A3:** expose only real Antigravity semantic actions and simplify receipts.

### Work admission and ownership

5. **B0/B1:** keep one explainable current-state queue; a lane is an automation/route
   policy view, not a new table/process.
6. **B2:** replace cursor/pending/quarantine WorkDispatch with bounded
   `dispatch_once` current-state admission.
7. **C0/C1:** make the existing Attempt-bound workspace lease the only write
   authority; delete advisory `code.scope.*` product and symbol/interface overlap.
8. **D0/D1:** simplify module-owner launch identity and implement conservative exact
   process reconciliation.

Shared adapter bootstrap/delivery is extracted only after two current adapters delete
their previous implementation in the same slice.

## Confirmed structural gaps

| Area | Current source condition | Target |
| --- | --- | --- |
| WorkDispatch | Tails Task observations into global/per-entry cursors, pending copies, rechecks, semantic slots, quarantine and duplicate launch settings. | Query ranked current Tasks and perform one atomic admission. Blocked Tasks remain Tasks. |
| Lane | Operational policy is spread across automation entry, route, capacity, scripts and queue. | Named projection over existing facts; no new entity, process, cursor or scheduler. |
| Write ownership | Advisory `code.scope` proposal/accept/override/index product does not enforce launch; workspace lease separately stores allowed paths and rejects conflicts. | One Task/Attempt-bound workspace/write lease using repository path prefixes. Delete `code.scope.*`. |
| Process owner | Strong owner-first Job custody exists, but launch plan/resolver repeat artifact/version/build/protocol/full contract. | Keep exact process identity/security; remove duplicate runtime compatibility matrix. |
| Common commands | String/vendor/internal methods leak into Store/capacity/descriptors. | Typed semantic adapter actions; internal lifecycle stays inside adapter. |
| Module contract | Store/adapters exact-compare artifact versions, capabilities, config and command/event schema inventories. | One current protocol major and capability subset. |
| Adapter bootstrap | Current adapters repeat env/owner/credential/hello/delivery code. | Small common readers in existing owners only after two consumers delete old code. |
| Adapter journals | Command/Claude/OpenCode journals combine native recovery, Store delivery, retention and cleanup. | Native effect state stays adapter-specific; share only exact terminal Store delivery when proven twice. |
| Antigravity | Terminal outcome depends on an observation ID/copy; dispatch needs refresh-generated proof. | Self-contained terminal outcome, ordinary state observation, minimal delivery spool. |
| Quotas | Service/method/artifact allowlists and proof SHA; Muse Rust rejected. | Generic authenticated source/epoch/sequence/scope/condition snapshot. |
| Passive observation | Joined local view is incomplete; binding-only inventory and stale connection/process facts remain. | Unbound inventory plus provider/OS/Git/GitHub facts readable without harness calls. |
| Runtime duplication | Parallel OpenCode, Muse, Codex, Claude, Antigravity and Command profiles remain. | One current executor/format per retained runtime after finite disposition. |
| Results/review | Missing-parent result association remains incomplete in standalone OpenCode; direct acceptance differs from automatic evidence gate. | One admitted-turn result window and one candidate-specific review/check gate. |
| Store/automation | Most reads use writer; base SQL SHA gates open; automation transfer/schedulers duplicate ownership. | Bounded WAL reads, explicit migrations, stable automation ID and one scheduler. |
| Product breadth | Concilium, integration ACK/ratification, automatic publication and large coordination surfaces remain. | Outside compact-core completion. |

These are documented source findings, not completed repairs or measured performance
improvements.

## Runtime overview

| Runtime | Current state relevant to refactoring |
| --- | --- |
| Antigravity | Rust descriptor/source v6 exists, active tooling still includes v5. Terminal result delivery can be lost before Store ACK. Node and Rust routes coexist. |
| OpenCode | Built-in and standalone Rust paths coexist. Standalone adapter has durable inbox/history work but a very large combined journal and unresolved result association edges. |
| Codex | Current Rust app-server executor exists; Python executor/SDK remains in repository/fixtures. Native Goal/live controls/account usage are not fully mapped through compact common actions. |
| Muse | Node bridge remains example route; Rust adapter has exact-result evidence but its usage snapshot is rejected by Store artifact allowlist. One executor is not selected. |
| Claude | Rust/SDK path and retained Node generations coexist. Bootstrap and a large journal repeat common and native responsibilities. |
| Command | Batch and ACP implementations coexist with historical profile/version branches and the largest adapter journal. |

## Old Swarm experience retained

The old Claude scripts remain evidence, not a compatibility target. Current docs keep
only useful behavior:

- compact Task prompt;
- explainable ranking;
- route/provider/effort policy;
- one worktree per manager and non-overlapping writes;
- passive multi-source observation;
- exact candidate evidence;
- explicit unknown outcomes.

They do not justify file markers, daemons per lane, newest-log ownership, fixed-age
restarts, automatic form answers or copied queue state.

## Runtime evidence retained

Existing receipts remain scoped to their named artifacts/revisions:

- Muse normalized result: `docs/remediation/2026-10-17/`;
- connected Store cases: `docs/remediation/2026-10-13/`;
- Antigravity settlement/readback: `docs/remediation/2026-10-10/` and
  `docs/remediation/2026-10-12/`.

They do not qualify the planned compact adapter protocol, WorkDispatch deletion,
write-lease migration, process reconciliation or all current installed routes.

## Verification status

The latest study changes documentation only. Runtime implementation starts with
formatting and minimal affected warnings-denied Clippy/build checks, followed by the
connected behavioral cases in
[implementation verification](agent-operations/implementation.md#verification).

Do not interpret older broad CI/fixtures as qualification of planned source changes.
Every completed slice updates this file with behavior, evidence and net owned Rust +
JS + glue, separating tests.
