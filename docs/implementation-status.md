# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** `main` currently contains the completed CLI ownership
reduction and the architecture/implementation preparation. The runtime source is
unchanged since commit `25a8e3b`; later commits through the source revision studied
here, `e7906bf`, changed documentation.

[Architecture](agent-operations/architecture.md) is the compact product manifesto.
[Implementation](agent-operations/implementation.md) is the only work order.
[Source findings](agent-operations/refactoring.md) explain the existing excess, and
[donor map](agent-operations/donor-map.md) records reusable mechanisms and limits.

## Completed source change

### CLI ownership reduction

Commit `25a8e3b` removed the host's duplicate general application CLI and MCP
launcher. Public `swarm` commands remain in `swarm-cli`; the host retains daemon,
local administration and internal-worker entrypoints.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- existing 87-line parser-test section retained.

This is the only runtime-code reduction in the current preparation series.

## Current source study

The latest study found that the adapter boundary is still burdened by a compatibility
and proof system rather than a small semantic port.

### Confirmed structural gaps

| Area | Current source condition | Target |
| --- | --- | --- |
| Common commands | `RuntimeCommandKind` mixes semantic actions with OpenCode, Command ACP and native MCP internal methods. | One typed `AdapterCommand`; vendor/internal operations map to semantic `Send`, `Cancel`, `Shutdown` or stay inside the adapter. |
| Module contract | Store and adapters exact-compare artifact versions, capability lists, config schemas and command/event schema sets. Codex and Command retain version/profile ladders. | One current protocol major and compact capability subset negotiation. Package identity remains inventory/installation data. |
| Adapter bootstrap | Codex, Claude, OpenCode, Muse, Command and Antigravity repeat owner/env/credential parsing, hello equality checks and delivery loops. | Common bootstrap in `swarm-process` and small IPC/re-delivery shell in `swarm-client`, adopted by at least two current Rust adapters before extraction is considered complete. |
| Receipts | Module/package/protocol/request hashes and Task/source identities are repeated across outcomes, dispatch receipts and result origins. Shared contracts also contain vendor readiness and Goal-continuation proof structs. | Authenticated link + Operation/binding identity; keep hashes only on credentials/executable/package bytes and retained result/artifact content. Vendor state becomes observation. |
| Vendor knowledge in Core | Store contains OpenCode/Command artifact checks, OpenCode session validation, Muse-specific family logic, Goal vendor branches and Antigravity/MCP dispatch gates. | Store branches on semantic action and retained object state only; adapters own vendor interpretation. |
| Runtime duplication | Built-in and standalone OpenCode, Node/Rust Muse, retired Python Codex, multiple Claude/Antigravity bridges and Command profiles remain. | One current executor/format per retained runtime after finite disposition of concrete unsettled work. |
| Goals | Harness Goal and Eliot Goal are now documented correctly as independent, but current progression still branches by vendor and lacks recurring message policy. | Native Goal through adapter capabilities; Eliot Goal as ordinary scheduled/event `Send` Operations. |
| Passive observation | Required local joined view is not complete. Binding-only inventory, lost process references, repeated executable hashing, stale restart connections and publication-dependent remote refs remain. | Unbound inventory plus local provider, OS, Git and GitHub observations readable without harness calls. |
| Results and review | Complete-result validation exists. Missing-parent result association remains incomplete for standalone OpenCode. Direct acceptance does not use the same assigned-review prerequisite as automation. | One admitted-turn result window and one common candidate-specific review/check gate. |
| Store/automation | Most reads still use the writer; base SQL SHA gates open; automation uses owner-dependent identity, multiple relocators and overlapping schedulers. | Bounded WAL readers, explicit migrations, stable automation `entry_id` and one scheduler. |
| Product breadth | Concilium, integration ratification/ACK and automatic publication remain in source. | Outside compact-core completion; retain ordinary messages, independent review and explicit accepted-candidate publication. |

These are source findings and documented deletion targets, not completed runtime
repairs or measured performance improvements.

## Runtime evidence retained

Existing runtime receipts remain scoped to their named artifacts and revisions:

- Muse exact normalized result receipt:
  [2026-10-17 verification](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- connected Store cases:
  [batch verification](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- Antigravity EOF settlement:
  [verification](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the new protocol-2 design, all methods, all recovery paths or the
current installed fleet. Historical accepted work proves that named Task flow only;
it does not prove the common acceptance prerequisite is enforced everywhere.

## Runtime overview

| Runtime | Current state relevant to refactoring |
| --- | --- |
| Codex | Current Rust app-server executor exists; Python executor/SDK is retired for new work but remains in repository/fixtures. Native Goal/live controls and account usage exist upstream but are not fully mapped through the current common contract. |
| OpenCode | Built-in and standalone Rust paths coexist. The standalone adapter has native session/result/readback work but assistant association and complete consolidation remain open. Public native MCP phases and loop-step add contract surface. |
| Muse | Node bridge remains the example route; Rust adapter has one exact-result receipt. One complete executor has not been selected. Rust quota snapshots are rejected by the current Store artifact allowlist. |
| Claude | Rust/SDK path and retained Node generations coexist. Live controls, local session readers and quota sources need one current adapter boundary. |
| Command | Batch and ACP implementations exist with historical profile/version branches. ACP already demonstrates semantic prompt/reply/cancel mapping. |
| Antigravity | Node and Rust warm-stream implementations coexist. Shared contracts contain Antigravity-specific readiness proof types that should become ordinary observations. |

## Verification status

No runtime code was changed by the latest architecture/adapter study. Its checks are
document content, links and consistency. Runtime implementation must use scoped
Clippy/build checks for the affected packages first, then connected behavioral cases
listed in [implementation verification](agent-operations/implementation.md#4-verification).

Do not interpret older broad CI or fixture runs as qualification of the planned
protocol, adapter shell, passive observation or executor deletion. Record each
completed source slice and its net owned-code change here when it lands.
