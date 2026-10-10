# R43. Codex Goal continuation: one current controller artifact, one receipt path, historical decoder only

**Status:** implementation handoff. Production routes, adapters, Store facts and native sessions are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

New Codex bindings use one current Rust controller artifact for dispatch, Goal continuation admission, terminal evidence and readback. Store validates the controller through one selected descriptor/contract identity instead of a second hard-coded legacy artifact ID.

```text
trusted route selects current Codex Rust descriptor
→ task.dispatch admission receipt
→ terminal turn evidence
→ Goal progression reserves agent.send continuation
→ Store enriches exact GoalContinuationAdmissionContext
→ Rust adapter emits GoalContinuationAdmissionReceipt
→ same Store validator accepts terminal evidence
→ next progression uses the exact retained terminal slot
```

The old Python bridge remains readable only for historical retained Operations that it actually produced. It cannot accept new Goal-continuation work after migration. No fallback from Rust to Python is added.

## 2. Confirmed split-brain

### 2.1 Trusted configuration selects the Rust artifact

`config.rs` defines the supported standalone Codex artifact as:

```text
codex-rust-controller.1
```

and validates its native options (`modelProvider`, `model`, `workspaceRoot`). New route configuration therefore has a first-class Rust controller path.

### 2.2 Store's Codex identity helper recognizes only the legacy Python bridge

`runtime/codex.rs` separately defines:

```text
RUNTIME     = "codex"
ARTIFACT_ID = "codex-sdk-18194bf-bridge.3"
```

`is_controller_route` requires that exact legacy artifact. Store uses this helper in:

- task-dispatch receipt validation;
- Goal continuation admission validation;
- terminal Goal evidence acceptance.

A binding on the configured Rust artifact is therefore rejected as `GOAL_UNSUPPORTED_RUNTIME` or `NATIVE_IDENTITY_MISMATCH`, even when its receipt is otherwise complete.

### 2.3 Rust adapter already implements the missing receipt contract

`swarm-adapter-codex` imports and constructs:

```text
GoalContinuationAdmissionContext
GoalContinuationAdmissionReceipt
GoalTerminalEventRef
```

and seals `goal_continuation_admission` in its outcome details. This is not a proposal for a new wire form. The current Store contract and Rust producer already exist; the selected-controller identity is the broken seam.

### 2.4 Pinning a second artifact ID would repeat the defect

Replacing the Python literal with a Rust literal in only `runtime/codex.rs` would still leave:

- artifact identity duplicated between configuration, descriptor/catalog and Store;
- future compatible artifact revisions blocked by another equality gate;
- historical Python receipts mixed with current production authority;
- no explicit migration condition for old routes.

The fix is one selected contract identity, not another allowlist.

## 3. Existing contracts to reuse

Do not invent new continuation DTOs. Reuse:

- `GoalContinuationAdmissionContext`;
- `GoalContinuationAdmissionReceipt`;
- `GoalTerminalEvidence` / `GoalTerminalEventRef`;
- `ModuleReceiptIdentity`;
- selected module descriptor/contract selector retained on the binding;
- current Operation admission/readback and automation terminal slots.

R31/#57 supplies the closed command registry. R20/#46 supplies Store-owned prompt bytes after its migration. R03/#29 continues to own exact Codex steer/input readback; it does not define Goal authority.

## 4. One Codex controller contract resolver

Place one Store/runtime-owned resolver beside the existing Codex receipt validators. Suggested private shape:

```rust
struct CodexControllerContract {
    artifact: ArtifactIdentity,
    protocol: ProtocolVersion,
    task_dispatch_admission: SchemaDescriptor,
    goal_continuation_admission: SchemaDescriptor,
    goal_terminal_evidence: SchemaDescriptor,
}

fn selected_codex_controller_contract(
    binding: &Value,
) -> Result<Option<CodexControllerContract>>;
```

It must derive from the exact selected descriptor/contract selector retained for that binding and require:

- runtime/module identity is the current Codex Rust controller family;
- protocol version required by the Rust adapter;
- exact schemas for dispatch admission, Goal continuation admission and terminal evidence;
- exact selected artifact in the binding receipt;
- descriptor is enabled/current for new work at admission time.

Descriptor capability proves compatibility only. Task/Attempt/Operation/GM authority remains Store-owned.

Do not expose this as a generic plugin framework. It is a private typed resolver for one existing controller contract.

## 5. Single source for the current artifact identity

Move the current Rust artifact identity to the narrow catalog/runtime owner used by both:

- trusted route validation;
- descriptor selection/installation;
- Codex controller contract resolver.

`config.rs` must not own a private duplicate constant. `runtime/codex.rs` must not own a different legacy active constant.

Historical Python artifact IDs live only in a retained-read decoder/list, never in new route admission.

No external version pin is introduced. Artifact identity records the installed implementation selected by the operator; compatibility is the required contract schemas/protocol.

## 6. New-work migration

### 6.1 Rust controller is the only new Goal owner

For new Codex bindings after this slice:

- trusted route points to the Rust controller descriptor;
- Goal continuation methods are advertised only when the selected descriptor contains the complete contract;
- Store enriches `agent.send` with the exact current context;
- Rust adapter emits the current receipt;
- terminal evidence is accepted only against the same selected artifact/protocol/binding generation.

### 6.2 Legacy Python bridge

Retain only what historical readback needs:

- decode old `task.dispatch` and terminal receipts already stored under the legacy artifact;
- preserve immutable Operation/result history;
- do not start a new legacy Goal continuation;
- do not silently reroute an unresolved Rust continuation to Python;
- no `if rust fails then python` path.

After all configured routes and unresolved new-work Operations have left the legacy bridge, remove its new-start registration/descriptor path. The historical decoder may remain versioned and read-only.

## 7. Store wiring

Change the connected path in this order:

1. replace `runtime::codex::is_controller_route` with the selected contract resolver;
2. update task-dispatch receipt validation to consume that contract;
3. update `validate_goal_continuation_admission` to require the same selected artifact/receipt identity;
4. update `codex_goal_terminal_evidence` to require the same contract and exact Operation method-specific admission;
5. update `automation_goal_progression` runtime selection to use the same resolver/read-only projection, not runtime/artifact string tests;
6. update module demand/descriptor capability projection so unsupported partial descriptors do not receive continuation commands;
7. switch the actual configured current route to the Rust controller;
8. disable legacy Python new starts and delete old active gates.

Every step must land in one vertical PR. A DTO-only or config-only change leaves the path broken.

## 8. Preserve exact evidence boundaries

Do not weaken current requirements:

- `task.dispatch` and `agent.send` admissions are different schemas;
- terminal event must be `turn.completed` with exact event digest and native root/turn/input identities;
- continuation Operation must name the exact Task/Attempt/binding/generation and previous terminal event;
- native ACK is not persisted input or terminal completion;
- unknown outcome is readback-only and never replayed blindly;
- a historical artifact cannot be interpreted as the current Rust schema merely because fields look similar.

## 9. Delete duplicated responsibility

After migration remove:

- legacy active `ARTIFACT_ID` from `runtime/codex.rs`;
- private duplicate Rust artifact constant from configuration;
- direct runtime/artifact string predicates for Goal support;
- Python bridge Goal-continuation producer/advertisement, if any;
- compatibility fallback selecting Python when Rust contract validation fails;
- tests that prove only one hard-coded artifact literal.

Keep explicit historical decoders under their historical schema/artifact identities.

## 10. Donor use

No external runtime is needed.

Useful existing patterns:

- ACP capability negotiation: compatibility is negotiated per selected agent/session, not inferred from a product name;
- Bazel REAPI: exact digest/descriptor identity is retained as evidence, while current policy decides whether a new action may start;
- ELIOT module descriptor/receipt path: already provides the needed selected-contract boundary.

Do not import ACP or another controller to solve this internal identity split.

## 11. Exact fixtures

- `codex_rust_route_advertises_complete_goal_contract`
- `codex_rust_dispatch_receipt_is_accepted_by_store`
- `codex_rust_goal_continuation_receipt_round_trips`
- `codex_rust_terminal_event_advances_one_goal_slot`
- `legacy_python_route_cannot_receive_new_goal_continuation`
- `historical_python_receipt_remains_readable`
- `partial_descriptor_missing_goal_schema_fails_before_command_delivery`
- `changed_artifact_same_runtime_cannot_reuse_old_receipt`
- `rust_continuation_unknown_is_not_replayed_to_python`
- `one_terminal_event_cannot_advance_two_goal_operations`

Use the public Store/module path with the real selected descriptor and adapter receipt. Helper-only artifact-ID tests are insufficient.

## 12. Ownership and order

- R01/#27: generic module lifecycle and selected descriptor ownership.
- R03/#29: Codex input/steer readback.
- R20/#46: TaskPrompt bytes.
- R31/#57: closed RuntimeCommand registry.
- R43: Codex controller contract identity, Goal admission/terminal round trip and legacy new-start removal.

One manager owns shared Codex config/runtime/Store files. Adapter and Goal-progression writers rebase onto the same contract rather than copying it.

## 13. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-contracts -p swarm-adapter-codex -p swarm-kernel-host --lib --bins -- -D warnings
```

Then exact transcript fixtures for dispatch→terminal→continuation→terminal. Paid live Codex qualification remains a final project phase.
