# Refactoring — Source Findings

Source rechecked through `7f85a00c69f3b52cc250ad7dbded91692078eb97`
on 2026-10-18. This file records why current mechanisms must be replaced or
removed. [Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the single work order; [status](../implementation-status.md)
records completed work.

## Size and concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments. They describe concentration, not reachable production
SLOC. Commit `25a8e3b` removed 1,206 net Rust lines from the duplicate host CLI.
Later commits through this revision changed documentation only.

## 1. Why agents could not finish the project

The development loop rewarded horizontal preparation and incident patches instead of
complete product slices.

- Whole Issues, audits and addenda replaced a current task. Compact exact `TASK.md`
  work completed more reliably.
- Policy lived in copied scripts, regexes and marker files. Each incident created
  another daemon or compatibility path.
- Contracts, schemas and proof packets were built before one caller-to-result path
  worked.
- File flags and branch heads were treated as delivery despite no consumer ACK.
- Equivalent audits and per-agent build trees duplicated work.
- “Implemented” meant types existed, not that a manager could assign work and receive
  a result.

Target: manager reads history, worker receives one slice, production path changes
first, old path disappears, one independent verifier checks the connected boundary.

## 2. The common command bus is not common

`RuntimeCommandKind` mixes semantic actions with internal/vendor methods:

- `native.opencode.loop_step`;
- `native.command.cancel_turn`;
- `native.command.close_session`;
- `native.command.release_evidence`;
- `native.mcp.install|observe|arm|read`.

Store parser, policy, capacity, projection, descriptors and adapters repeat them.
OpenCode loop-step is send/steer. ACP cancel/close are cancel/shutdown. Native MCP and
release evidence are adapter lifecycle.

Replace them route-by-route with the semantic port in
[implementation](implementation.md#p1--replace-the-module-compatibility-matrix-with-protocol-2).
Delete method, capacity entry, schema, fixture and documentation together.

## 3. Package metadata became a behavioral API

`module_contract.rs` retains Codex and Command version/profile ladders.
`module_handshake.rs` exact-compares module/artifact/build, protocol, capabilities,
configuration schema and command/event schema arrays, then adapters compare the echo
again. Store also hardcodes vendor artifact versions and schemas.

Every optional action therefore creates another artifact coordinate, exact schema
set, Store branch and startup validator. Descriptor metadata is functioning as a
second authorization system despite comments saying otherwise.

Target: one current protocol major, required semantic capability subset and one
selected package identity for installation/rollback. Rights remain Store policy.

## 4. Bootstrap and Eliot delivery are reimplemented per adapter

Codex, Claude, OpenCode, Muse, Command and Antigravity repeat environment parsing,
owner membership, credentials, protocol/claim parsing, hello equality, `module.next`,
reconnect and delivery.

Useful existing owners already exist:

- `swarm-process::module_owner` for process membership/private state;
- `swarm-client::ModuleLink` for authenticated Eliot IPC.

Extract only after two current routes use the same behavior and both old
implementations are deleted.

## 5. The journals became products

Current major journal files alone are approximately:

| File | Bytes before submodules/tests |
| --- | ---: |
| Command `journal.rs` | 199,925 |
| Claude `journal.rs` | 183,134 |
| OpenCode `journal.rs` | 138,914 |

These mix several responsibilities:

- native intent and replay classification;
- Store outcome/result/observation delivery;
- result selectors and references;
- append histories and outcome revisions;
- torn-tail evidence;
- startup inventory/cursors/counters;
- tombstones and cleanup queues;
- native checkpoints;
- retention graphs and compaction.

A common implementation extracted from these files would preserve the tumour.

The only shared Store-delivery mechanism is: retain an already-produced exact
outcome/result, send it, remove it after exact ACK. Observations are latest-value
source checkpoints with coverage. Native intent/readback remains adapter-specific.

Antigravity currently has the opposite defect: `pending_receipts` is only memory.
Its terminal result can exist while Store stays `outcome_unknown` after process exit.

## 6. Antigravity duplicates terminal identity through observation

Current flow:

1. terminal result creates a pending outcome with no `observation_id`;
2. adapter publishes an observation;
3. Store returns an observation row ID;
4. adapter rewrites the outcome;
5. Store validates the outcome against the same result copied into the current
   observation.

`runtime/warm_stream.rs` requires the observation pointer, current Store observation,
boot/root/scope, ordinal, response hash and status all to match. This prevents
persisting an immutable outcome before the first Store call and duplicates the same
adapter fact.

Separately, `AntigravityDispatchSessionProof` copies binding, boot, root, scope hash,
model, stream phase, event/gap counters and result ordinal into a refresh result;
launcher code revalidates and hashes it again.

Target: self-contained terminal outcome authenticated by ModuleLink and retained
Operation/binding/boot. Monitoring state stays an observation, not a prerequisite or
receipt witness. Remove both the observation pointer and dispatch proof packet before
adding the minimal durable spool.

## 7. Receipts repeat identity Store already knows

`RuntimeCommand` and `ModuleReceiptIdentity` repeat method/input hashes,
module/artifact/version/build, protocol, binding/generation and Operation ID.
Dispatch/result types repeat Task/Attempt/revision, source hashes, native payload
hashes and receipts again.

Store created the Operation and delivered it through an authenticated binding-scoped
link. Repeating package/request identity creates comparison code but no new authority.

Keep:

- Operation ID;
- generation/boot needed to reject stale reports;
- native logical input/turn/result IDs;
- disposition and bounded details;
- hashes/ranges for actual retained result/artifact bytes.

## 8. Vendor proof types leaked into shared contracts

Shared runtime contracts contain Antigravity session proofs, Goal continuation proof
families and OpenCode native-MCP envelopes. Store contains OpenCode session rules,
Command profile logic, Muse family handling, Goal vendor branches and artifact
allowlists.

Target Store branches on semantic action and retained object state. Native readiness,
model, MCP, physical attempts and recovery facts are adapter observations.

## 9. Logical input and physical attempt are conflated

OpenCode explicitly models one logical Step with several physical Attempts. Codex
provides logical client-message identity plus exact thread/turn identity. Existing
Eliot structures and result logic can still reason as if each terminal provider event
is the one input execution.

Target: one Eliot Operation per logical input. Adapter observations expose physical
attempts; result and acceptance attach to the logical input. A provider retry is not
permission for Eliot to resend.

## 10. Observation has two cursor domains

`monitor.snapshot`/`monitor.follow` correctly use Store `observation_id` and a
transactional journal cut. Provider readers have different cursors: JSONL offsets,
SQLite sequences, inbox IDs, event cursors and source epochs.

Conflating them would either leak vendor storage into the public API or lose gaps.

Target:

- adapter owns provider cursor/epoch and gap recovery;
- Store commits normalized facts, source cursor and coverage;
- Governor uses Store observation cursor;
- expired/foreign provider cursor becomes source coverage gap, not empty state;
- follow/watch disconnect never changes native work.

ACPX and OpenCode both confirm replay/follow must have an explicit synchronization
boundary.

## 11. Quota is another vendor-proof subsystem

`NativeUsageSnapshot` currently hardcodes `UsageService::{Codex,Muse}` and exact
native method names. `ProviderConditionFact` repeats binding, route, provider,
account, model, source connection, evidence method/revision, auth SHA and sequence.
Store `provider_conditions.rs` stores about 27 columns, reconstructs the DTO, hashes
the retained fact and compares several ordering domains.

`native_usage.rs` then admits producers through artifact allowlists. The current Muse
Rust producer is rejected because only Node bridge versions `.9`–`.13` are accepted.

This is a compatibility/provenance matrix, not a compact quota projection.

Target: adapter folds native patches into one normalized source/account capacity
snapshot. Store validates source auth, bounds and source-local monotonic sequence.
Bindings reference one source snapshot instead of copying it. Remove vendor enums,
native method allowlists, auth-context SHA, artifact-version gates and duplicate
condition provenance. Unknown/missing quota is unavailable, not available or zero.

## 12. Runtime duplication remains the largest deletion surface

| Runtime | Parallel surface | Target |
| --- | --- | --- |
| OpenCode | built-in host executor plus standalone Rust adapter | standalone Rust only |
| Muse | Node bridge plus Rust adapter | select one by complete behavior and total Rust+JS+glue |
| Codex | current Rust plus retired Python executor/SDK | Rust only |
| Claude | Rust/SDK path plus retained Node generations | one Rust adapter |
| Antigravity | Node bridges plus Rust warm stream | Rust only |
| Command | Batch v3/v4/v5 and ACP v1/v2 profiles | one current batch and/or one current ACP product, no history ladder |

Historical records do not require an executable.

## 13. Tooling is a second compatibility product

Large workflow/qualification scripts validate checkout, compiler, lockfile,
dependency graph, cache-path hash, artifact schema arrays, preview hashes and proof
packets. This is mostly unrelated to whether an installed adapter can execute a
turn.

Target:

- one package manifest;
- selected executable/resources;
- one install/launch byte check;
- path containment, private credentials and atomic publication;
- runtime protocol/capability check;
- public behavioral qualification.

Remove artifact-specific PowerShell validators, build-environment equality, retired
frontend inventory, proof qualification and incident-specific modes after their
permanent behavior is covered.

<a id="code-reduction-candidates"></a>

## 14. Additional confirmed deletion candidates

- frontend installation scans retired sibling dependency trees;
- duplicate Forge installer;
- host links worker implementation crates;
- two schedulers and `legacy_optional_workers`;
- owner-dependent automation identity and six relocation paths;
- capacity/automation key codecs in `meta`;
- Concilium and integration ratification/ACK products;
- automatic Issue-label publication;
- direct acceptance bypass of assigned-review evidence;
- launch preview hashes and MCP readiness gate;
- most Store reads through one writer;
- base SQL text SHA gate;
- unbounded Muse Rust shutdown;
- per-sample executable hashing.

[Implementation](implementation.md) names the order and owner. Do not create another
issue series from this list.

## 15. Claims rejected

- Exact MCP exposure already has one owner; it is not duplicate permission policy.
- Schemars is not a ready deletion because inspected inputs lack shared DTOs.
- Octocrab is optional; current `gh` is not a large handwritten HTTP stack.
- Result-page retention before ACK is necessary.
- A transcript, alive PID or recent mtime does not settle an uncertain write or prove
  Task completion.
- File moves, Rust rewrites and more crates are not simplification.
- ACP v2 is not Eliot's universal bus and does not justify permanent v1/v2 support.
- Existing OpenCode/Claude/Command journals are not acceptable common outbox
  implementations.

## Implemented source reduction

Commit `25a8e3b`, now in `main`, removed the host duplicate application CLI and MCP
launcher: `main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines.
Later preparation commits are documentation, not runtime deletion.
