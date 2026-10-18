# Refactoring — Source Findings

Source rechecked through `05b6d05828e4f3a9106c7c726034de714db845e8` on
2026-10-18. This file records why current mechanisms must be replaced or removed.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the only work order and replacement design;
[status](../implementation-status.md) records completed work. Source presence is not
a requirement to preserve a subsystem.

## Size and concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments. They describe concentration, not reachable production
SLOC. Commit `25a8e3b` removed 1,206 net Rust lines from the duplicate host CLI;
subsequent commits through this source revision changed documentation only.

## 1. Why agents could not finish the project

The failure was not simply “too much code.” The development loop rewarded horizontal
preparation and local incident patches instead of complete product slices.

### Oversized context replaced a current task

Old Issue threads, several audits, addenda and manager briefs were repeatedly sent to
workers. A 95-KB Issue thread became working context. Weak models then produced
meta-analysis, broad rewrites and conflicting interpretations. The successful field
pattern was the opposite: compact `TASK.md`, exact symbols, one local responsibility
and one completion condition.

Target: the lead manager reads history and donors; coding workers receive one current
slice packet. Historical documents are evidence, not concurrent specifications.

### Policy lived in prose and scripts

In six days the old swarm created or recreated roughly 76 control scripts; 27 files
and about 4,180 lines appeared on one day. There were 22 addenda, ten brief templates,
four sector backups and six acceptance-daemon versions. Each incident added a script,
regex, marker or exception, and editing a live script often caused another copied
version.

Target: current policy is captured by Task/Attempt revision and typed configuration.
Changing policy replaces current work/config; it does not append another daemon,
addendum or compatibility path.

### Work was split horizontally, not vertically

Agents created contracts, schemas, registries, proof packets and fixtures for future
consumers while the caller-to-result path remained incomplete. Later agents had to
support every intermediate representation. “Implemented” meant types existed, not
that a manager could assign work and receive a result.

Target: one vertical slice from Store admission through one adapter and readback,
with the old path deleted. Shared abstractions appear only with two current consumers.

### Delivery had no durable consumer acknowledgment

Historical queues accumulated hundreds of `PUSHED-*` deliveries that no acceptance
consumer read. File flags and branch heads were treated as delivery. The same pattern
now appears as repeated adapter outbox/recovery machinery.

Target: every admitted action has a Store Operation; every Store-bound adapter
record stays in one exact outbox until Store acknowledgment. Native input is never
replayed because delivery to Store was uncertain.

### Review and builds duplicated work

A full Codex audit before delivery followed by another acceptance review produced no
merges for hours. Every worktree/agent also tended to allocate its own Rust target and
parallelism followed agent count rather than host capacity.

Target: the manager integrates once, one independent verifier checks the changed
boundary, and build concurrency follows host/Cargo capacity. During construction run
minimal format/Clippy checks; broader tests follow the connected product path.

## 2. The common command bus is not actually common

[RuntimeCommandKind](../../crates/swarm-contracts/src/module_command.rs) mixes
semantic actions with vendor/internal operations:

- `native.opencode.loop_step`;
- `native.command.cancel_turn` and `native.command.close_session`;
- `native.command.release_evidence`;
- `native.mcp.install|observe|arm|read`.

Store parser, method policy, capacity ledger, operation projection and adapters all
repeat these names. OpenCode loop-step is a send/steer variant. ACP cancel/close are
ordinary cancel/shutdown. Native MCP and release evidence are adapter lifecycle
implementation, not manager concepts.

Replacement: typed semantic actions in
[implementation P1](implementation.md#p1--replace-the-module-compatibility-matrix-with-protocol-2).
Delete old methods from contracts, Store, capacity, adapters, descriptors, tests and
docs in the same slice.

## 3. Package versions became a behavior API

[module_contract.rs](../../crates/swarm-contracts/src/module_contract.rs) contains a
sequence of Codex `ModuleContractTemplate::codex_rust_controller_v*` constructors and
a current descriptor version constant. Command retains Batch V3/V4/V5 and ACP V1/V2
profiles with exact capability/schema combinations.

[module_handshake.rs](../../crates/swarm-kernel-host/src/store/module_handshake.rs)
exact-compares module ID, artifact ID/version/build, protocol, capabilities, config
schema, pre-input contract and command/event schemas. The adapter then compares the
same echo. Store also hardcodes OpenCode and Command artifacts/versions when deciding
whether a common command is valid.

Consequences:

- each optional action creates a new artifact profile and compatibility branch;
- Store knows vendor package history;
- adapters carry old profiles after the current package changes;
- descriptive metadata behaves like runtime authorization.

Replacement: one current protocol major and capability subset negotiation. Package
identity remains installation/inventory data. New work uses protocol 2; protocol 1
is retained only for enumerated unsettled Operations, then removed.

## 4. Every adapter reimplements bootstrap and hello

Codex, Claude, OpenCode, Muse, Command and Antigravity independently implement:

- owner/environment/path parsing;
- credential loading and scope checks;
- protocol parsing;
- full descriptor claim decoding;
- exact capability/schema matching;
- `module.hello` and full response equality checks.

OpenCode and Claude duplicate helpers such as `required_env`, `optional_env`,
`absolute_env_path`, `parse_protocol` and schema matching. Muse explicitly notes that
no reusable bootstrap existed and writes another one. JS bridges repeat the same
negotiation comparisons.

Existing owners already cover the useful boundary:

- [swarm-process module_owner](../../crates/swarm-process/src/module_owner.rs)
  verifies process membership;
- [swarm-client ModuleLink](../../crates/swarm-client/src/module_link.rs) owns one
  authenticated link and hello barrier.

Replacement: a small common bootstrap in those packages. Vendor config and session
state remain adapter code. Do not create a new framework crate.

## 5. Store delivery is duplicated inside vendor journals

Command, Codex, Claude, Muse, OpenCode and Antigravity each implement variants of:

1. connect and hello;
2. pull `module.next`;
3. decode/store command identity;
4. execute vendor action;
5. retain outcome/observation/result page;
6. retry delivery until Store acknowledges;
7. reconnect and repeat pending deliveries.

Examples include Codex `pending_outcomes/acknowledge_outcome`, Claude's operation
outbox, Muse's `state.outbox`, OpenCode's journal acknowledgments and Command's
`RunStore` delivery loop. Their vendor journals have different legitimate native
state, but Eliot delivery semantics are the same.

Replacement: one `DeliveryOutbox` in `swarm-client` containing exact already-produced
Store envelopes. It never records native intent or decides native replay. First
extract it in two lower-complexity adapters and delete their outer loops; do not let
Codex/Muse force vendor state into the common type.

## 6. Native effect journals ignore native idempotency

The current design frequently proves and hashes local intent before considering what
the harness already guarantees:

- Muse has durable UUIDv7 command identity and a cursor-bearing log;
- OpenCode durably admits inbox items and treats matching item IDs idempotently;
- Codex exposes client message IDs, exact turn IDs and steer guards;
- ACPX explicitly says request IDs are not replay keys and uncertain submissions are
  inspected rather than retried.

A universal local journal either duplicates these native stores or weakens their
semantics to the lowest common denominator.

Replacement: native identity/readback first. Only adapters lacking safe native
classification keep one small closed prepared-effect record. Common code owns only
Store delivery. JSON-RPC request IDs, process PIDs, HTTP status and socket ACKs are
not assumed to be durable idempotency.

## 7. Receipts repeat identity and hashes Store already knows

[RuntimeCommand](../../crates/swarm-contracts/src/runtime.rs) carries a string method,
arbitrary JSON, original input SHA and target input SHA.
`ModuleReceiptIdentity` repeats module ID, artifact ID/version/build, protocol,
binding/generation, Operation ID and request hash on every outcome.

Dispatch/result structures repeat Operation, binding/generation, Task, Attempt,
revision, snapshot/source hashes, native payload hash and a full module receipt.
Store created the Operation, retained its request and authenticated the ModuleLink.
Repeating package/request identity adds comparison code and version profiles, not
trust.

Keep:

- Operation ID and current/historical permitted generation/boot;
- native input/turn/request IDs and disposition;
- hashes/ranges for retained result/artifact bytes;
- an adapter-local conflict digest only where journal replay needs it.

Remove package/protocol/request hashes and duplicate Task/source echoes from every
receipt. Store validates its own Operation against the authenticated link.

## 8. Vendor proofs leaked into shared contracts

`swarm-contracts::runtime` contains `AntigravityDispatchSessionProof`, scope-key SHA,
model state, event/gap counters and stream fields. It also contains Goal continuation
context/admission/terminal proof types. `native_mcp.rs` adds a public OpenCode setup
command envelope.

These are observations or adapter implementation details:

- native readiness/model state belongs to `agent.state`;
- MCP setup belongs inside OpenCode adapter lifecycle;
- Harness Goal uses native `Goal` actions;
- Eliot Goal sends an ordinary `Send` Operation.

They must not make all harnesses implement another proof protocol or make Store know
vendor session internals.

## 9. Core still branches on vendor and artifact details

Confirmed examples:

- `selected_native_command_supported` recognizes exact OpenCode/Command artifacts and
  schemas;
- runtime failure validation contains OpenCode session spelling;
- Goal progression selects Codex versus OpenCode behavior;
- capacity/family code contains Muse-specific interpretation;
- dispatch has Antigravity proof and OpenCode/Muse MCP gates;
- result eligibility has OpenCode loop-step logic.

Target Store branches on semantic action and retained object state only. Vendor
interpretation moves to the selected adapter. Remove branches with their producers;
do not hide them behind another allowlist.

## 10. Recovery is real; internal lifecycle methods are not

`agent.recover` has a current consumer: Muse restores a known session after confirmed
bridge-family departure without replaying the Task. Preserve it as typed `Recover`
with common preconditions/outcome and adapter-specific implementation.

Native MCP phases and release-evidence commands are internal lifecycle details exposed
through Store policy/capacity/operations. They multiply schemas, reservations,
receipts and tests without adding manager semantics.

Target:

- ordinary Eliot-link reconnect never replays native input;
- `Recover` restores one known supported session after required loss/readback checks;
- `Open` selects/attaches;
- `Reconcile` reads one uncertain effect;
- `Cancel` and `Shutdown` name exact subjects;
- setup/cleanup remains adapter-internal and appears only as observations/diagnostics.

## 11. Execution duplication is the largest deletion opportunity

| Runtime | Current parallel surface | Target |
| --- | --- | --- |
| OpenCode | built-in host subtree plus standalone Rust adapter; legacy demand still activates built-in route | standalone Rust only; remove host executor/routing and public native MCP phases |
| Muse | Node bridge and Rust adapter | select one by complete behavior and total Rust + JS + glue; delete the other |
| Codex | Rust adapter plus retired Python executor/SDK/fixtures | Rust only; retain only historical readers actually used |
| Claude | Node bridge generations plus Rust/SDK path | one Rust adapter with thin current SDK driver |
| Antigravity | Node warm-stream generations and Rust adapter | one current warm-stream implementation |
| Command | batch profiles and ACP V1/V2 compatibility profiles | current batch plus current ACP protocol; no historical profile ladder |

Historical completed records do not require an executor. Concrete unsettled work gets
a finite disposition before deletion.

## 12. Passive observation still needs source changes

Current code retains pieces of the required view but not the joined Governor
projection:

- `agent.list` is binding-based and misses unmatched sessions;
- `runtime::observe` requires a binding;
- `agent.family` filters through Attempt/producers;
- configured native sources cannot start readers without an Operation;
- owner/worker process identities are dropped before Store projection;
- process metrics depend on live owner membership and repeatedly hash executables;
- startup clears stale connection only for `ready`;
- timeline/follow lose some binding/generation/source identity;
- GitHub polling reads Issues, not an independent remote branch ref.

The replacement in [P7](implementation.md#p7--passive-observation-and-quotas) uses
existing adapter, process/Git and Store owners. It adds neither a provider poll per
viewer nor a transcript database.

## 13. Existing code-reduction candidates remain valid

- frontend installation inspects retired sibling trees and has duplicate Forge
  installer/legacy fixtures;
- qualification treats checkout/compiler/cache paths as compatibility;
- host links six worker implementations;
- two schedulers and `legacy_optional_workers` overlap lifecycle ownership;
- automation transfer relocates several journals and creates successor copies;
- capacity/automation retain JSON/key codecs in `meta`;
- Concilium, integration ACK/ratification and automatic publication are separate
  product engines outside compact core;
- direct acceptance lacks the same assigned-review prerequisite as automation;
- launch preview hashes and MCP readiness block direct work;
- Store serializes most reads through one writer;
- Muse Rust stop can wait indefinitely;
- base SQL text SHA may reject a semantically compatible database before migration.

[Implementation](implementation.md) names the replacement. Do not create a second
issue series from this list.

## 14. Claims explicitly rejected

- Exact MCP exposure already has one owner in `swarm-contracts`; do not remove its
  policy blindly.
- Schemars is not a ready deletion: inspected MCP inputs lack an existing shared DTO
  replacement.
- Octocrab is optional; current `gh` transport is not a large handwritten client.
- Result-page retention before Store ACK is necessary; it must move into the common
  delivery outbox, not disappear.
- A local transcript, alive PID or mtime does not settle an uncertain native write or
  prove Task completion.
- File moves, Rust rewrites, more crates or more generated schemas do not simplify a
  retained responsibility.
- “One file per agent” is not a universal rule: use it only for leaf microtasks; the
  manager owns atomic shared changes.

## Implemented source reduction

Commit `25a8e3b`, now in `main`, removed the host's duplicate application CLI and MCP
launcher: `main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines.
Public commands remain in `swarm-cli`; host daemon/admin/internal worker entrypoints
remain. Later documentation changes are not additional runtime deletion.
