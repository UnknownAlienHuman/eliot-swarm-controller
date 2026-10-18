# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** `main` contains the completed CLI ownership reduction
and the current architecture/implementation preparation. Runtime source has not
changed since commit `25a8e3b`; later commits through the source revision studied
here changed documentation.

[Architecture](agent-operations/architecture.md) is the compact product manifesto.
[Implementation](agent-operations/implementation.md) is the only work order.
[Source findings](agent-operations/refactoring.md) explain current excess, and the
[donor map](agent-operations/donor-map.md) records approved mechanisms and exclusions.

## Completed source change

### CLI ownership reduction

Commit `25a8e3b` removed the host's duplicate general application CLI and MCP
launcher. Public `swarm` commands remain in `swarm-cli`; host retains daemon, local
administration and internal-worker entrypoints.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- existing 87-line parser-test section retained.

This is the only runtime-code reduction in the current preparation series.

## Latest source and donor study

The current adapter boundary is burdened by compatibility/proof machinery and
repeated transport journals rather than a small semantic port.

### Confirmed structural gaps

| Area | Current source condition | Target |
| --- | --- | --- |
| Agent work packets | Workers have historically received full Issues, audits and addenda; policy corrections became more files/scripts. | Manager reads history and gives one compact slice packet with objective, symbols, failure cases, deletion list and minimal check. |
| Common commands | `RuntimeCommandKind` mixes semantic actions with OpenCode, Command ACP and native MCP internal methods. | One typed `AdapterCommand`; preserve explicit `Recover`; internal methods map to `Send`, `Cancel`, `Shutdown` or stay private. |
| Module contract | Store/adapters exact-compare artifact versions, capability lists, config schemas and command/event schema sets. Codex/Command retain version ladders. | One current protocol major and capability subset negotiation. Package identity remains inventory/installation data. |
| Bootstrap/hello | Codex, Claude, OpenCode, Muse, Command and Antigravity repeat owner/env/credential parsing and hello equality. | Common bootstrap in `swarm-process`; common authenticated link in `swarm-client`, adopted by two smaller adapters before broad migration. |
| Store delivery | Every adapter has its own outcome/observation/result outbox and reconnect drain. | One exact `DeliveryOutbox` for Store-bound envelopes only. It never replays native input. |
| Native effect recovery | Large local journals duplicate native identities/logs or generalize the weakest harness semantics. | Native idempotency/readback first; one small adapter-specific prepared-effect record only where the harness cannot classify the outcome. |
| Receipts | Package/protocol/request hashes and Task/source identities repeat across outcomes, dispatch receipts and result origins. Shared contracts contain vendor readiness/Goal proofs. | Authenticated link + Operation/binding/boot identity; hashes only on real immutable bytes; vendor state becomes observation. |
| Vendor knowledge in Core | Store contains OpenCode/Command artifact checks, OpenCode session validation, Muse family logic, Goal branches and Antigravity/MCP gates. | Store branches on semantic action and retained object state; adapters own vendor interpretation. |
| Runtime duplication | Built-in/standalone OpenCode, Node/Rust Muse, retired Python Codex, multiple Claude/Antigravity bridges and Command profiles coexist. | One current executor/format per retained runtime after finite disposition of concrete unsettled work. |
| Goals | Harness Goal and Eliot Goal are documented as independent, but current progression still branches by vendor and lacks general recurrence. | Native Goal through adapter capability; Eliot Goal as ordinary scheduled/event `Send` Operations. |
| Passive observation | Binding-only inventory, lost process references, repeated executable hashing, stale restart connections and publication-dependent remote refs remain. | Unbound inventory plus local provider, OS, Git and GitHub observations readable without harness calls. |
| Results/review | Complete-result validation exists. OpenCode missing-parent association remains incomplete. Direct acceptance lacks the same assigned-review prerequisite as automation. | One admitted-turn result window and one candidate-specific review/check gate. |
| Store/automation | Most reads use the writer; base SQL SHA gates open; automation has owner-dependent identity, relocators and overlapping schedulers. | Bounded WAL readers, explicit migrations, stable automation `entry_id` and one scheduler. |
| Product breadth | Concilium, integration ratification/ACK and automatic publication remain. | Outside compact-core completion; retain ordinary messages, independent review and explicit accepted-candidate publication. |

### Donor conclusions

- Paseo provides a useful semantic session shape but its daemon/UI/provider product is
  not adopted.
- Waku demonstrates one ordered provider worker and one settle point, but is GPL-3.0
  and its current provider drivers are large; pattern only, no code copy/link.
- ACPX confirms exact shared owner, queue-vs-native-admission separation and no blind
  retry after uncertain submission. Its request IDs are not idempotent replay keys.
- Muse and OpenCode expose real durable native command/inbox identities. Codex exposes
  client message identity, exact turn IDs and steer guards; JSON-RPC request ID is not
  durable dedupe.
- Existing `swarm-process` private durable file primitives are sufficient for the
  proposed Store-delivery outbox and small adapter records; no new database/crate is
  justified.

These are source findings and design constraints, not runtime repairs.

## Historical operating experience retained

The old Claude-script swarm showed:

- compact exact microtasks completed reliably; whole Issues caused meta work and
  oversized diffs;
- one manager/issue owner should integrate its own work; a separate integrator is
  needed only across owners;
- file flags and branch heads lose deliveries without a durable consumer ACK;
- two equivalent full audits wasted throughput;
- policy addenda and copied daemons multiplied faster than the product;
- activity, connection, process liveness, assignment and quota must remain separate;
- build parallelism must follow host resources, not agent count;
- compatibility shims and fallback aliases should not preserve abandoned paths.

These lessons are now reflected in `AGENTS.md`, implementation slice packets,
DeliveryOutbox/native-idempotency rules and the finite protocol transition.

## Runtime evidence retained

Existing receipts remain scoped to their named artifacts/revisions:

- Muse exact normalized result receipt:
  [2026-10-17 verification](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- connected Store cases:
  [batch verification](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- Antigravity EOF settlement:
  [verification](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify protocol 2, the shared outbox/bootstrap, passive observation,
all recovery paths or the installed fleet. Historical accepted work proves only its
named Task flow.

## Runtime overview

| Runtime | Current state relevant to refactoring |
| --- | --- |
| Codex | Current Rust app-server executor exists; retired Python executor/SDK remains in repository/fixtures. Native Goal/live controls/account usage exist upstream but are not fully mapped through the compact contract. |
| OpenCode | Built-in and standalone Rust paths coexist. Standalone adapter has session/result/readback work, but assistant association/consolidation remain open. Public native MCP and loop-step inflate the contract. |
| Muse | Node bridge remains example route; Rust adapter has one exact-result receipt. One complete executor is not selected. Rust quota snapshots are rejected by current artifact allowlist. |
| Claude | Rust/SDK path and retained Node generations coexist. Live controls, local readers and quotas need one current boundary. |
| Command | Batch and ACP implementations retain historical profile/version branches. ACP already demonstrates semantic prompt/reply/cancel mapping. |
| Antigravity | Node and Rust warm-stream implementations coexist. Shared contracts contain Antigravity-specific readiness proof types that should become observations. |

## Verification status

No runtime code changed in the latest architecture/donor/script study. Checks are
document content, links and consistency. Runtime construction uses formatting and
minimal affected warnings-denied Clippy/build checks first. Connected cases are listed
in [implementation verification](agent-operations/implementation.md#5-verification);
full product/native/release tests follow when the product path is ready.

Do not interpret old broad CI or fixture runs as qualification of the planned
protocol, Store-delivery outbox, native-idempotency strategy, passive observation or
executor deletion. Record each completed source slice and net owned-code change here.
