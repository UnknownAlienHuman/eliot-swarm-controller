# Current-source audit and implementation ledger

Date: 2026-10-09. Source baseline: `953efa153cded25f5649719f2aa194182dc3e57f`.

Integration baseline: `88264832b7cde56b917728830666f27b29fc515e`. The subsequent fast-forward changed only the execution-map HIGH-source clarification; the audit's production baseline did not change. The working candidate is uncommitted and remains under implementation.

Status: finite current-source HIGH audit closed with explicit evidence gaps; implementation in progress. This ledger does not claim code completion or release qualification.

The manager fetched `origin`, verified a clean checkout, and fast-forwarded `main` from `40591a2` to this baseline. The initial GitHub inventory listed 37 open PRs and 8 open Issues; documentation-only PR #106 appeared in a subsequent check. Documentation handoffs are implementation instructions, not delivered production code.

## Source binding

The owner supplied these files in `C:/Users/kleym/Downloads/Swarm V27 audit map`:

| Source | SHA-256 |
| --- | --- |
| `ELIOT-Swarm-Controller-Execution-Map-2026-10-09.md` | `399704762b1190ba9e06db8e2cd1586771b566fdf0dd0518e973c223a7feabdb` |
| `ELIOT-Swarm-Controller-Master-Audit-2026-10-09-v27.md` | `3dee8664daba6ac3551128ecf005bfa4e2e21b423346d9d0ce039a1f4f9d2cff` |
| `ELIOT-Swarm-Controller-HIGH-Appendix-Source-2026-10-09.md` | `4a997d7910046092fa0edee66c775c05a8428fd6a7af5d50d686503ff6fcc858` |
| `ELIOT-Swarm-Controller-Original-What-To-Fix-Registry.md` | `ae8406869c2026d0837eec15eaf0af68fcb16448008850597f25a44f5933a974` |
| `ELIOT-Swarm-Controller-Original-Unverified-Suspicions.md` | `2dadaadd9bc56b7abf66ee564732928bffe29ed0d7e08297f1893e1c8b8badb0` |

The published execution map is [EXECUTION-MAP.md](EXECUTION-MAP.md). The supplied V27 update explicitly distinguishes historical V26 claims from delivered PRs #75–#104. Every implementation decision must trace current source and its actual public caller.

V27 D.5 and the readiness criteria refer to a full HIGH appendix. The original sources were located in `C:/Users/kleym/Downloads/Telegram Desktop` and verified:

| Original source | SHA-256 |
| --- | --- |
| `Реестр подозрений.md` | `423bbf2b732c4245605e4a029f34d6f30de9e02f9e440d29240547afe14059c5` |
| `Что чинить + полный реестр.md` | `517f01fa45d94d8b2f37066e3aa126ea9372eb11981855929dcce1376bb8180a` |

The second source has exactly 61 HIGH rows and one MED/HIGH row. The supplied appendix contains the same 62 severity rows, with zero text differences. The supplied full registry also has zero line-content differences against the original; differing file hashes do not imply differing row content. The audit deliberately covers all 62 rows. Its line-numbered immutable inventory is retained as local audit evidence in `.local/manager-audit/high-registry.json`; each source row has exactly one audit owner. A source row is an allegation, not automatically a confirmed defect. Each receives a current-source fixed, refuted, confirmed-with-owner, or explicit unknown verdict before implementation. Quiet-host throughput and executed test adequacy belong to the final verification phase requested by the owner.

## Owners and sequencing

One manager owns this checkout and serializes changes to shared Store, protocol registry, and MCP dispatch files. Audit agents are read-only until their evidence is accepted. Implementation writers receive disjoint file ownership and do not invoke Cargo, publish, or change another writer's files.

| Area | Audit owner | Production dependency |
| --- | --- | --- |
| GM authority and shared Store integration | Manager | R30 precedes authority consumers |
| Operation/Task/Artifact grants and requirement evidence | Laplace | R30, retained positive object relations |
| Process ownership, durable journals, supervisor, hooks | Sagan | Durable file primitives before adapter adoption |
| Mailbox/subscriptions, contract decisions, review/Concilium | Fermat | R30 before GM decisions; shared Store writes serialized |
| Automation, resource fences, launcher, effect state | Arendt | R30 and exact retained effects |
| Codex/OpenCode/Claude/Command controls | Planck | Process/journal primitives; one Codex writer |
| Muse/Antigravity/Zed and external evidence | Kant | Process/journal primitives |
| Frontend boundary, TaskPrompt, donor and qualification matrix | Godel | Stable authoritative contracts |
| HIGH registry provenance and audit completeness | Tesla | All 62 original rows accounted for |
| Independent fallback review of critical authority/ownership | Ampere, native GPT 6 Luna max | Correct stale or unsupported Step 5 findings |

## Accepted current-source findings

The portable [HIGH verdict ledger](HIGH-VERDICTS.json) binds every original row to the supplied appendix, current baseline, verdict, evidence and one owner. All 62 rows occur exactly once. Manager corrections supersede preliminary worker reports. The two `UNKNOWN` rows concern retained Concilium dispatch consequence (#146 source line, owned by automation dispatch) and unidentified-child survival across the health-write-failure/restart scenario (#362 source line, owned by supervisor qualification). They remain explicit acceptance work rather than inferred bugs. Five confirmed MCP rows are test-evidence gaps with production guards already present; their tests belong to the post-code phase.

### R30: inconsistent GM authority and regressing epoch

`store/gm.rs::require_authority` compares raw `client_id` without validating a positive epoch or exact current Manager registration. `handover` uses `unwrap_or(0)` for an invalid previous epoch and a target-role denylist. Forge, automation, launcher authority evidence, and Operation visibility have separate raw readers. The current immutable `gm.handover` receipts can supply the epoch high-water without a new table.

Required invariant: one Store-owned designation parser, exact enabled registered Manager identity, optional paired non-authoritative binding, and monotonically increasing fencing epoch. A lost or damaged current pointer requires local Operator recovery above the validated retained high-water; it never grants Manager authority or becomes epoch zero. Same-client valid rebind preserves its epoch. Status must report damage while remaining readable.

The implementation handoff is on `origin/task/audit-30-gm-fencing`, `docs/remediation/2026-10-08/30-gm-designation-fencing.md`. Its source claims have been checked against this baseline.

### R13/R15: capacity materialization and quota evidence

`store/capacity.rs::load_ledger` replaces a malformed ledger or malformed `entries` with an empty object. `derive_operation` returns active from an earlier Operation start proof before examining later exact producer terminal evidence. `note_outcome` resolves a quota incident on any Applied/Accepted outcome in the scope, without new evidence for the affected quota window.

Required invariant: malformed retained materialization is an explicit error, exact terminal evidence takes precedence over its own earlier start, and unrelated successful effects cannot resolve a quota incident. These are separate facts; no quota-based killing or automatic model/account switching is authorized.

## Audit correction policy

Proposed helper names are not proof that behavior is absent. PRs #89–#91 already changed candidate provenance, Task receipts, and dispatch reuse. A finding must trace the current producer and consumer before being retained. An internal function lacking a Principal argument does not establish caller exposure until the outer method/role gate is traced. Duplicate code or an unverified projection difference is not labeled a confirmed security defect.

Native input ACK is not proof of durable input. Local capture persistence is not proof of immutable Store artifact publication. Direct process exit is not proof of process-family departure. Each relevant acceptance item needs its own evidence.

## Current verification and tool evidence

The warnings-denied production-only Clippy gate passed for `swarm-process`, `swarm-supervisor`, `swarm-checks`, `swarm-script-worker`, `swarm-adapter-codex` and `swarm-cli`, including their shared contracts. The MCP extraction also passed its scoped gate after integration repairs. Exact commands and source hashes are retained in `.local/manager-audit/production-verification.json`; later edits require a fresh affected-source gate. These passes do not qualify the Store host or native execution. Claude retention, capacity, provider-condition admission and remaining active TaskPrompt consumers must finish before the full test phase.

Codex account-level usage permission now comes from the official `ordinaryUsageAllowed` response field, with its own authoritative-read provenance and SHA-256 of the associated opaque backend `accountId`. Sparse window updates do not refresh that permission; account changes invalidate it. The pending refresh marker is independent of the latest notification kind, so a window event cannot erase an account-change refresh. A failed optional read does not repeatedly poll or imply recovery. Store/provider admission integration and runtime qualification remain pending.

Independent source review found a ScriptRun cleanup-result artifact-prefix mismatch and a generic settlement path that could discard retained cleanup after publication failure. The candidate now publishes a distinct valid `scriptresult-<digest>` and preserves cleanup-pending as nonterminal on that failure. Review confirmed the repair at source level; its late-release/public-read/fault qualification is still pending.

- `cargo metadata --locked --no-deps --format-version 1`: passed on the baseline.
- `cargo check --locked -p swarm-kernel-host --lib --bins` with the external target `C:/Users/kleym/AppData/Local/Eliot/build/rust-env-target`: passed; existing unused-import/dead-field warnings were reported.
- Rust compiler and Cargo: 1.98.1. Broad tests and native qualification have not run in this stage.
- Production candidate: scoped `cargo clippy --locked -p swarm-process --lib --bins --no-deps -- -D warnings` passed using the external target after removing unused existing module-owner imports.
- Production candidate: the same scoped warnings-denied Clippy gate passed for `swarm-supervisor` after correcting an unused terminal assignment and nested guards. These two passes do not qualify the host, adapters, process fault scenarios, or whole project.
- Codebase Memory CLI 0.9.0 indexed the current source in fast mode: 24,157 nodes, 122,432 edges, zero skipped files. External cache only; no repository graph artifact. An exact query located `crates/swarm-kernel-host/src/store/gm.rs::require_authority` with 40 incoming graph edges.
- Eight audit agents were spawned on `kilo/stepfun-step-5-preview-free` with `high`. The live route is enabled, explicitly free, and supports tools/reasoning. Preliminary reports contained stale claims about squash merges and candidate provenance; those claims were rejected and amendments requested. The requested native GPT 6 Luna fallback was then spawned at `max` for independent critical-boundary review. A successful task result, rather than spawn acceptance, is required before accepting each report.
- ELIOT Governor MCP startup failed with `PublicationMissing` for `C:/Users/kleym/AppData/Local/Eliot/instances/default/runtime/publication.json`. No packet or writeback was obtained and no runtime repair was attempted. [The repository integration contract](../../eliot-memory-os-integration.md) permits standalone development with this limitation.
- Antigravity currently advertises `gemini-3.8-flash-high`; no Gemini test or audit run has occurred yet.

## Delivery gates

The current source candidate separates immutable launch-plan evidence from live route admission. Launch, open, dispatch and later turns check the same predicate; cancellation and readback remain available. A proved workspace lease commits before the following admission transaction, so a newly held or unavailable route cannot turn its filesystem result into an unknown effect. Recovery of an unknown workspace still performs readback only. These changes await the connected host Clippy and fault qualification.

Task dispatch retains its exact context before returning a native command. Built-in Zed v2 recovery uses the original worker boot and immutable Operation digest, with an exact `builtin.zed` identity and normalized admission receipt. Current Command JS glue5 and Muse bridge9 consume TaskPrompt; Rust Command ACP1, Rust BatchV4, OpenCode controls and Claude retention are still finishing their source gates. Native/model/full-suite claims remain pending.

Finish the available-source audit and resolve each confirmed finding to fixed, refuted, or one implementation owner. Complete production code and scoped formatting/Clippy first. Only then run the full public-path fixtures, model-backed Step 5 free and Antigravity Gemini 3.8 qualification, crash/fault cases, process ownership on supported platforms, and quiet-host load evidence. Publish checked code to GitHub `main` without force push. Record skipped/unavailable acceptance items explicitly; retain the Goal until the requested outcome is actually achieved.
