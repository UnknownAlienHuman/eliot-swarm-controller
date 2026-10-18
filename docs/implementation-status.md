# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** `main` contains one completed runtime reduction and the
current architecture/refactoring preparation. Runtime source has not changed since
commit `25a8e3b`; subsequent work through
`513f9698c268753dc6f2a6c0cf4d9a53f7f5e472` changed documentation or created and
removed a no-op edit workflow.

[Architecture](agent-operations/architecture.md) is the product manifesto.
[Implementation](agent-operations/implementation.md) is the only work order.
[Source findings](agent-operations/refactoring.md) explain current excess.
[Donor map](agent-operations/donor-map.md) records approved mechanisms and exclusions.

## Completed source reduction

Commit `25a8e3b` removed the host's duplicate general application CLI and MCP
launcher.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- host daemon, local administration and internal-worker entrypoints remain.

This is the only completed runtime-code reduction in the preparation series.

## Next executable slices

### A0 — active Antigravity v6 coordinate

**Not implemented.** Source/descriptor use v6; route example, package policy, native
qualification and narrow installer validation still name v5. Synchronize active
new-work selectors to v6 without editing historical v5 receipts. Remove active dual
version behavior after concrete v5 bindings are disposed.

### A1 — durable Antigravity Store delivery

**Not implemented.** Terminal outcomes/observations/results are retained only in the
worker process. Existing remediation receipts show Store can remain
`outcome_unknown` after worker exit because reconcile depends on dead in-memory
`pending_receipts`.

Target: route-local same-boot durable outbox using existing `swarm-process` private
file primitives. It retries exact Store-bound envelopes only and never repeats native
input.

### A2 — remove Antigravity dispatch proof

**Not implemented.** Dispatch currently depends on a fresh
`AntigravityDispatchSessionProof`, scope/model/stream checks and multiple digests.
Target dispatch uses current assignment/binding/session/revisions; model and stream
state remain ordinary observations. Delete proof DTO, producer, Store validator,
qualification gate and proof-only hashes.

### A3 — semantic Antigravity route

**Not implemented.** Keep only real actions: `Open`, `Dispatch`,
`Send(next_turn)`, `Refresh`, `Result`, `Reconcile` and explicit owned-resource
shutdown/release. Remove placeholder handlers for unsupported controls and collapse
receipt identity to Operation/binding boot/native result facts.

## Confirmed structural gaps

| Area | Current source | Target |
| --- | --- | --- |
| Store delivery | Antigravity and other adapters retain outcomes in route-specific in-memory/file journals; Antigravity loses them on process exit. | exact bounded durable Store-delivery outbox; route-local first, shared only after a second current consumer deletes its old loop |
| Native effects | generic/local journals repeat native state or generalize the weakest harness | native durable ID/readback first; smallest prepared-effect record only when the harness cannot classify outcome |
| Dispatch readiness | Antigravity refresh proof and OpenCode/Muse MCP/readiness gates block first input | ready current binding/session plus assignment/workspace revisions; readiness becomes observation |
| Common commands | semantic actions mixed with OpenCode, ACP and MCP internal methods | one typed semantic command port; internal vendor methods disappear from public Store routing |
| Module contract | artifact/profile/version and exact schema/capability matrices are compared by Store and adapters | one current protocol major and required capability subset; package identity is install/inventory data |
| Bootstrap/link | adapters repeat env/owner/credential/protocol/claim and Eliot IPC loops | Antigravity + Claude prove one `module_owner` reader; `ModuleLink` owns only authenticated Eliot transport |
| Receipts | package/protocol/request/Task/source hashes repeat across outcomes/results | authenticated Operation/binding/boot plus native IDs; hashes only for credentials/package bytes and retained artifact content |
| Runtime duplication | parallel OpenCode, Muse, Codex, Claude, Antigravity and Command implementations/profiles remain | one current executor/format per retained runtime after finite disposition |
| Goals | current Eliot progression branches by provider and has Goal-specific proof families | native Goal through adapter capability; Eliot Goal is ordinary scheduled/event `Send` |
| Observation | binding-only inventory, lost process refs, repeated executable hashing, stale connection facts and weak remote refs remain | joined local provider/OS/Git/GitHub projection readable without harness calls |
| Results/review | OpenCode missing-parent association incomplete; manual/automatic acceptance differ | one admitted-turn result window and one candidate-specific review/check gate |
| Store/automation | most reads use writer; SQL text SHA; owner-dependent automation identity; overlapping schedulers | WAL readers, explicit migrations, stable `entry_id`, one scheduler |
| Product breadth | Concilium, integration ratification/ACK and automatic publication remain | outside compact core; retain normal messages, independent review and explicit accepted-candidate publication |
| Installation/tooling | per-artifact validators, repeated receipts/hashes and build-environment equality | one generic package path, one install byte check and behavior-based qualification |
| CI | 92 KB workflow, custom package classifier and many incident-specific qualification modes | docs + fmt + affected check/Clippy + changed JS + connected slice tests; full qualification manual/release |

These are source findings and deletion targets, not completed repairs.

## Donor conclusions

- Paseo provides a useful semantic session-port pattern; its daemon/UI/provider
  product is not adopted.
- Waku demonstrates one ordered provider-state owner and settle point; it is GPL-3.0
  and used as a pattern only.
- ACPX confirms one shared queue owner, queue acceptance vs prompt start, targeted
  cancel and explicit uncertainty. It is either a replacing optional backend or a
  pattern, never an additional layer beside Eliot's ACP owner.
- Muse, OpenCode and Codex expose stronger native IDs/readback than a universal Eliot
  mutation journal.
- Claude's contained `OwnedBootstrap` and Antigravity can prove one small shared
  bootstrap reader.
- Existing `swarm-process::permissions` is sufficient for a route-local delivery
  outbox; no adapter database/crate is justified.
- Cargo metadata is sufficient for workspace/dependency ownership; build-cache path,
  compiler and checkout equality are not runtime compatibility.

## Runtime overview

| Runtime | Current state relevant to refactoring |
| --- | --- |
| Antigravity | Rust descriptor/source v6; active tooling partly v5. Rust outcome outbox is process-local. Node and Rust implementations coexist. Shared contracts contain Antigravity-specific proof DTOs. |
| OpenCode | built-in and standalone Rust execution coexist. Standalone result/readback work exists; missing-parent association and consolidation remain open. Public native MCP/loop-step expand the contract. |
| Codex | current Rust app-server path exists; retired Python executor/SDK/fixtures remain. Native Goal/live controls/account usage are not fully mapped to the common semantic port. |
| Muse | Node remains example route; Rust has exact-result evidence. One complete executor is not selected. Rust quota snapshots are rejected by the Store artifact allowlist. |
| Claude | Rust adapter plus JS SDK harness/generations coexist. `OwnedBootstrap` duplicates common owner/claim parsing and is a candidate for extraction with Antigravity. |
| Command | Batch v3/v4/v5 and ACP v1/v2 profiles coexist with exact capability/schema alternatives and native method aliases. It is not a suitable first common-shell proof. |

## Historical evidence retained

Existing receipts remain scoped to their named artifact/revision:

- Muse exact normalized result:
  [2026-10-17 verification](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- connected Store cases:
  [batch verification](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- Antigravity EOF settlement:
  [verification](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json);
- Antigravity S2 retained-readback evidence under `docs/remediation/2026-10-10`.

They do not qualify the target protocol, outbox, proof removal, passive observation or
installed fleet. Historical accepted work proves only its named path.

## Verification status

The latest audit/documentation pass changes no runtime source. Runtime implementation
starts with formatting and minimal affected warnings-denied Clippy/build checks, then
the connected behavioral cases in
[implementation verification](agent-operations/implementation.md#verification).

For every completed source slice, update this file with:

- actual behavior delivered;
- old path deleted;
- exact verification evidence;
- net owned Rust + JS + glue, tests reported separately;
- unresolved facts left honest.
