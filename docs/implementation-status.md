# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** `main` contains the CLI ownership reduction and the
current architecture/implementation preparation. Runtime source has not changed since
commit `25a8e3b`; later commits through the source revision studied here changed
documentation.

[Architecture](agent-operations/architecture.md) is the compact product manifesto.
[Implementation](agent-operations/implementation.md) is the only work order.
[Source findings](agent-operations/refactoring.md) explain current excess, and the
[donor map](agent-operations/donor-map.md) records approved mechanisms and exclusions.

## Completed source change

Commit `25a8e3b` removed the host duplicate general CLI and MCP launcher.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon, local administration and internal worker entrypoints remain.

This is the only runtime-code reduction in the preparation series.

## Current executable order

The first route is Antigravity Rust:

1. **A0:** synchronize active artifact v6 across route, installer, package policy and
   qualification.
2. **A1:** remove observation-ID and
   `AntigravityDispatchSessionProof` dependencies from terminal receipt/dispatch.
3. **A2:** add the route-local minimal durable Store-delivery spool.
4. **A3:** expose only real typed Antigravity actions and remove placeholder
   unsupported handlers and repeated receipt identity.

A1 precedes A2 inside the connected candidate because the current outcome is mutated
after `module.observe` ACK. Persisting it first without removing that coupling would
require a dependency journal rather than a small exact-delivery spool.

The second route proves only what is shared:

- Antigravity + Claude may prove one common bootstrap reader;
- Store-delivery moves to `swarm-client` only after a second migrated route uses the
  same minimal semantics and both old loops are deleted;
- Command, Codex and Muse do not define the initial abstraction.

## Latest confirmed source findings

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity terminal delivery | `pending_receipts` is memory-only; outcome is rewritten with Store observation ID before send. | Self-contained terminal outcome, then durable exact outcome/result delivery. |
| Antigravity dispatch | refresh produces a large session proof; Store revalidates and hashes it. | Current binding/boot/session/workspace facts; readiness remains observation. |
| Existing journals | OpenCode, Claude and Command journals mix native intent, Store delivery, retention, tombstones/references and cleanup. | Reuse no whole journal; common mechanism is exact Store-envelope persistence only. |
| Logical execution | harnesses may perform several physical attempts for one logical input. | One Operation/result per logical input; physical attempts are observation. |
| ACP | Command has historical profile ladder; official ACP v2 draft changes admission/state semantics. | ACP remains adapter-local; select one generation, map actions, drain/delete old profile. |
| Observation | Store monitor cursor and provider source cursors are different domains. | Adapter owns source epoch/cursor/gaps; Store owns normalized observation journal. |
| Quota | common DTO hardcodes Codex/Muse methods and Store uses artifact allowlists and duplicated evidence/digests. | Adapter-normalized source/account snapshot; no vendor method/version proof in Core. |
| Runtime duplication | several providers retain parallel executors/generations. | one current executor/format per runtime after finite disposition. |
| Tooling | qualification checks compiler/checkout/cache/schema/proof equality. | package/image/protocol/behavior checks only. |
| Store/automation | writer-bound reads, SQL text SHA, owner-dependent automation identity and multiple schedulers remain. | WAL readers, explicit migrations, stable entry ID and one scheduler. |
| Product breadth | council, ratification/ACK and automatic publication remain. | outside compact-core completion. |

## New donor conclusions

- Official ACP confirms prompt admission is not terminal completion; state updates are
  ID-addressed. ACP capability/version handling belongs inside Command.
- ACPX confirms one shared owner, targeted cancellation, passive replay+follow,
  opaque cursors and explicit outcome unknown/expired cursor.
- OpenCode confirms durable inbox ID, one logical Step with several physical Attempts,
  and subscribe-before-replay with a sync watermark.
- Codex confirms stable client user-message identity, exact turn/steer guards and
  account-limit snapshots separate from run usage.
- Existing Eliot journals are anti-donors as shared implementations; only their exact
  Store-envelope persistence idea survives.
- Existing `swarm-process` private-file primitives are enough for the Antigravity
  spool; no new DB/crate is justified.

## Runtime overview

| Runtime | Current state relevant to refactoring |
| --- | --- |
| Antigravity | Rust v6 source/descriptor; active tooling still had v5 selectors. Memory-only receipt delivery, observation-coupled terminal validation and proof-gated dispatch remain. |
| OpenCode | Built-in and standalone Rust paths coexist. Durable inbox/log mechanisms exist; local journal is oversized and mixes responsibilities. |
| Codex | Current Rust app-server executor exists; Python path remains in repository. Native logical-input/turn IDs and account snapshots are not yet reflected in the compact common contract. |
| Muse | Node example route and Rust candidate coexist. Rust quota snapshots are rejected by current artifact allowlist. |
| Claude | Rust/SDK path plus retained Node generations. `OwnedBootstrap` is a good second bootstrap consumer; its journal is not a common outbox donor. |
| Command | Batch/ACP historical profiles and a very large journal remain. The current ACP crate generation must not be mixed with ACP v2 draft in Core. |

## Runtime evidence retained

Existing receipts remain scoped to their exact artifacts and revisions:

- Muse normalized-result verification:
  [2026-10-17 receipt](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- connected Store batch:
  [verification](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- Antigravity EOF settlement:
  [verification](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the planned Antigravity receipt/spool, typed port, passive source
cursors, quota simplification or executor deletion.

## Verification status

The latest pass changes documentation, not runtime. Documentation validation covers
links, Markdown and diff hygiene.

Runtime construction starts with formatting and minimal affected warnings-denied
Clippy/build checks. Connected cases are listed in
[implementation verification](agent-operations/implementation.md#verification).
Record each completed source slice, behavioral evidence and net owned-code change
here.
