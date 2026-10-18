# Refactoring — Source Findings

Source rechecked at `e7906bfba989d40526bca8a39ecc5d278adf9448` on
2026-10-18. This file records why current mechanisms must be replaced or removed.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the only work order and replacement design;
[status](../implementation-status.md) records completed work. Source presence is not
a requirement to preserve every subsystem.

## Size and concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments. They describe concentration, not reachable production
SLOC. The implemented CLI slice in commit `25a8e3b` removed 1,206 net Rust lines;
subsequent commits through this source revision changed documentation only.

## 1. The common command bus is not actually common

[RuntimeCommandKind](../../crates/swarm-contracts/src/module_command.rs) mixes
semantic actions with vendor/internal operations:

- `native.opencode.loop_step`;
- `native.command.cancel_turn` and `native.command.close_session`;
- `native.command.release_evidence`;
- `native.mcp.install|observe|arm|read`.

The Store parser, method policy, capacity ledger, operation projection and adapters
all repeat these names. The standalone OpenCode adapter routes `loop_step` through
the same send handler, but its current contract does not advertise it and its native
path rejects new loop-step input. Command ACP maps its native cancel/close methods to
turn/session operations. These are not additional controller concepts.

Replacement: the typed semantic actions in
[implementation P1](implementation.md#p1--replace-the-module-compatibility-matrix-with-protocol-2).
Delete each old method from contracts, Store, capacity, adapters, descriptors, tests
and documentation in the same slice.

## 2. Package versions became a behavior/compatibility API

[module_contract.rs](../../crates/swarm-contracts/src/module_contract.rs) contains a
sequence of Codex `ModuleContractTemplate::codex_rust_controller_v*` constructors and
a current descriptor version constant. Command has Batch V3/V4/V5 and ACP V1/V2
profiles with different exact capability/schema combinations.

[module_handshake.rs](../../crates/swarm-kernel-host/src/store/module_handshake.rs)
retains and exact-compares module ID, artifact ID/version/build, protocol,
capabilities, config schema, pre-input contract, command schemas and event schemas.
It then echoes the complete set to the adapter, whose startup code compares it again.
The same file still hardcodes OpenCode artifact `0.5.0`, Command ACP versions 1/2 and
an OpenCode-specific reply schema when deciding whether a command is supported.

Consequences:

- every optional action creates a new artifact profile and compatibility branch;
- Store must know vendor artifact/version/schema details;
- adapters carry historical profiles after the current package changes;
- descriptor metadata is treated as runtime authorization even though comments state
  that it is descriptive only.

Replacement: one current protocol major, typed capabilities and subset negotiation.
Package/version remains installation/inventory data, not Core behavior. Existing
rights are still checked by Store.

## 3. Every adapter reimplements the same bootstrap and hello

The following current Rust paths independently parse the supervisor environment,
verify owner membership, load credentials, parse protocol, parse a full descriptor
claim, compare exact capability/schema sets and validate Store's echoed negotiation:

- [Codex module contract](../../crates/swarm-adapter-codex/src/module_contract.rs);
- [Claude module runtime](../../crates/swarm-adapter-claude/src/module_runtime.rs);
- [OpenCode module runtime](../../crates/swarm-adapter-opencode/src/module_runtime.rs);
- [Muse module runtime](../../crates/swarm-adapter-muse/src/module_runtime.rs);
- [Command module host](../../crates/swarm-adapter-command/src/module_host.rs);
- Antigravity Rust and several retained JS bridges.

OpenCode and Claude duplicate helpers such as `required_env`, `optional_env`,
`absolute_env_path`, `parse_protocol`, capability matching and schema-set matching.
Muse's source explicitly states that no reusable bootstrap existed, then implements
another one. JS bridges repeat `effects_authorized_by_descriptor`, descriptor
revision and exact array equality checks.

Existing owners already cover the useful common boundaries:

- [swarm-process module_owner](../../crates/swarm-process/src/module_owner.rs)
  verifies supervisor-owned process membership;
- [swarm-client ModuleLink](../../crates/swarm-client/src/module_link.rs) owns one
  authenticated link and hello barrier.

Replacement: extend these owners with the small bootstrap/link loop in
[implementation P3](implementation.md#p3--build-the-small-shared-rust-adapter-shell).
Vendor configuration and native state stay in each adapter. Do not introduce a new
crate or general workflow framework.

## 4. Adapter delivery loops and journals repeat Eliot transport logic

Command, Codex, Claude, Muse, OpenCode and Antigravity each implement variants of:

1. connect and hello;
2. pull `module.next`;
3. decode/store command identity;
4. execute vendor action;
5. retain outcome/observation/result page;
6. retry delivery until Store acknowledges;
7. reconnect and repeat pending deliveries.

Native effect journals are necessary where a vendor action may be uncertain. The
repeated Eliot-side pull/re-delivery shell is not. The shared shell should only own
authenticated Eliot IPC and re-delivery of already-retained records. Vendor session,
native readback and result association remain adapter code.

A shared shell is justified only when at least two current Rust adapters adopt it
and delete their previous loops in the same change.

## 5. Receipts repeat identity and hashes already known to Store

[RuntimeCommand](../../crates/swarm-contracts/src/runtime.rs) carries a string method,
arbitrary JSON, original input SHA and target input SHA.
`ModuleReceiptIdentity` repeats module ID, artifact ID/version/build, protocol,
binding/generation, Operation ID and request hash on every outcome.

Dispatch/result structures then repeat:

- Operation, binding and generation;
- Task, Attempt and revision;
- task snapshot and source-text hashes;
- native payload hash/length;
- the full module receipt again;
- selector/result hashes.

Store already retained the original Operation and authenticated the module link. The
adapter cannot legitimately choose another binding or request. Repeating package and
request identity does not create new trust; it creates comparison code, artifact
profiles, fixtures and compatibility branches.

Keep:

- Operation/binding generation/boot needed to reject stale reports;
- native input/turn IDs and disposition;
- hashes/ranges for actual retained result/artifact bytes;
- an adapter-local conflict digest where needed for journal replay.

Remove package/protocol/request hashes from every receipt and duplicate Task/source
hash echoes. Store validates the authenticated link against its own Operation.

## 6. Vendor proofs leaked into shared contracts

`swarm-contracts::runtime` contains
`AntigravityDispatchSessionProof`, including scope-key SHA, model readback state,
event counts, result ordinal, gaps and stream fields. It also contains Goal
continuation context/admission/terminal proof types. `native_mcp.rs` adds another
public command envelope for OpenCode setup phases.

These are adapter observations or implementation details:

- native readiness/model state belongs to `agent.state`;
- MCP installation/readiness belongs inside the selected OpenCode adapter;
- Harness Goal uses native `Goal` actions;
- Eliot Goal sends an ordinary `Send` Operation.

They must not make every harness implement another proof protocol or make Store know
vendor session details. Keep required byte integrity at the actual artifact boundary.

## 7. Core still branches on vendor and historical artifact details

Confirmed examples:

- `selected_native_command_supported` recognizes exact OpenCode/Command artifacts and
  schemas;
- `runtime::public_native_failure` validates a session ID through OpenCode `ses`
  rules;
- Goal progression selects Codex send versus OpenCode pseudo-Goal;
- capacity/public family code contains Muse-specific interpretation;
- launch dispatch has Antigravity-specific proof gating and OpenCode/Muse MCP gates;
- result eligibility has special OpenCode loop-step logic.

The target Store branches on semantic action and retained object state only. Vendor
interpretation moves to the selected adapter. Remove a branch when its producer and
consumer move; do not hide it behind another artifact allowlist.

## 8. Public recovery/control surface accumulated internal operations

`agent.recover` exists as a common method although native recovery differs by
harness and uncertain external effects already have `agent.reconcile`. Several
adapters reject recovery; Muse implements a particular bridge-restart flow.

Likewise native MCP phase methods and release-evidence commands are internal
lifecycle details exposed through Store policy/capacity/operations. This multiplies
public schemas, reservations, receipts and tests without adding manager semantics.

Target:

- adapter lifecycle reconnects/resumes its native session;
- `agent.open` explicitly attaches/resumes where the manager requests it;
- `agent.reconcile` reads back one uncertain effect;
- `agent.cancel` and `agent.shutdown` use explicit subjects;
- internal setup/cleanup remains internal and appears only as observation/diagnostic
  facts.

## 9. Execution duplication remains the largest reduction opportunity

| Runtime | Current parallel surface | Target |
| --- | --- | --- |
| OpenCode | built-in host subtree plus standalone Rust adapter; legacy demand still activates the built-in route | standalone Rust adapter only; remove host executor/routing and public native MCP phases |
| Muse | Node bridge and Rust adapter | select one by complete behavior and total Rust + JS + glue; delete the other |
| Codex | current Rust adapter plus retired Python executor/SDK/fixtures | Rust only; retain only small historical result readers actually used |
| Claude | retained Node bridge generations plus Rust/SDK path | one Rust adapter with thin current SDK driver |
| Antigravity | Node warm-stream generations and Rust adapter | one current warm-stream implementation |
| Command | batch profiles and ACP V1/V2 compatibility profiles | current batch profile plus current ACP protocol; no historical profile ladder |

Historical completed records do not require keeping an executor. Concrete unsettled
work receives a finite disposition before deletion.

## 10. Passive observation still needs source changes

The current code already retains pieces of the required view, but not the joined
Governor projection:

- `agent.list` is binding-based and misses unmatched sessions;
- `runtime::observe` requires a binding;
- `agent.family` filters through Task Attempt/producers;
- supervisor demand has no configured-source reader without an Operation;
- owner/worker process identities are dropped before Store projection;
- process metrics depend on live owner membership and repeatedly hash executables;
- startup clears stale connection only for `ready`, not all unreleased states;
- follow/timeline events lose some binding/generation/source identity;
- GitHub polling reads Issues, not an independent remote branch ref.

The replacement in [implementation P7](implementation.md#p7--passive-observation-and-quotas)
uses existing adapter, host process/Git and Store observation owners. It does not add
a provider poll per viewer or a second transcript database.

## 11. Existing code-reduction candidates remain valid

The following earlier findings still belong to the single implementation order:

- frontend installation inspects retired sibling trees and has a duplicate Forge
  installer/legacy fixture;
- qualification treats checkout/compiler/cache-path provenance as compatibility;
- the host links six worker implementations;
- two schedulers and `legacy_optional_workers` overlap lifecycle ownership;
- automation transfer relocates multiple journals and creates successor copies;
- capacity and automation families retain JSON/key codecs in `meta`;
- Concilium, integration ACK/ratification and automatic publication are separate
  product engines outside the compact core;
- direct acceptance does not require the same retained assigned review evidence as
  automatic acceptance;
- launch preview hashes and MCP readiness block direct work;
- Store serializes most reads through one writer;
- Muse Rust stop can wait indefinitely;
- base SQL text SHA can reject a semantically compatible database before migration.

[Implementation](implementation.md) names the owner and replacement for each. Do not
create a second issue series from this list.

## 12. Claims explicitly rejected

- Exact MCP exposure already has one owner in `swarm-contracts`; its policy is not a
  duplicate permission engine to remove blindly.
- Schemars is not a ready deletion: inspected MCP inputs do not have an existing
  shared DTO replacement.
- Octocrab is optional; current `gh` transport is not a large handwritten HTTP stack.
- Result-page retention before ACK is not waste: it recovers lost acknowledgment.
- A local transcript, alive PID or recent mtime does not settle an uncertain native
  write or prove Task completion.
- File moves, a Rust rewrite or more crates do not simplify a retained responsibility.

## Implemented source reduction

Commit `25a8e3b`, now in `main`, removed the host's duplicate application CLI and MCP
launcher: `main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines.
Public commands remain in `swarm-cli`; host daemon/admin/internal worker entrypoints
remain. Later documentation changes are not additional runtime deletion.
