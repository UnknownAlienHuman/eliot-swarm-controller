# Claude module — capability-based updates and activation

This document defines how ELIOT updates its Claude adapter without freezing the owner’s external harness or changing the selected subscription route.

It is a packaging and qualification contract, not an instruction to update Claude automatically. The production source still contains historical dependency declarations and a `0.3.287` equality gate in the Rust adapter’s Node driver. [R16](../../docs/remediation/2026-10-07/16-claude-interactions.md) removes the gate and connects official permission/question handling.

## Authoritative sources

1. [ELIOT module contract](../../docs/agent_swarm.module-contract-v2.md): binding, Operation, replay/readback and evidence.
2. [Claude Agent SDK permissions](https://code.claude.com/docs/en/agent-sdk/permissions).
3. [Claude approvals and user input](https://code.claude.com/docs/en/agent-sdk/user-input).
4. [Claude environment variables](https://code.claude.com/docs/en/env-vars).
5. Actual required exports/options/messages of the currently selected installation.

Historical fixtures and source SHAs identify what was reviewed. They do not become an external runtime allowlist.

## Separation of identities

Keep these facts independent:

| Identity | Meaning |
|---|---|
| ELIOT artifact ID / source digest | Exact controller/adapter bytes. |
| External package/runtime version | Observed native implementation used by a launch. |
| Auth/provider route | Subscription, API key, gateway or other explicitly selected route. |
| Session/root ID | Native conversation identity. |
| Adapter boot/connection generation | Current owner of callbacks and transport. |
| Operation/request ID | ELIOT intent and native decision/input correlation. |

Changing one does not rewrite the others. In particular, a new Claude release does not alter old ELIOT receipts, and rolling back ELIOT code does not undo native effects.

## Subscription preservation

A route declared subscription-backed must not be silently changed by adapter environment:

- `ANTHROPIC_API_KEY` overrides Claude Pro/Max/Team/Enterprise subscription and must not be injected or accidentally inherited;
- `ANTHROPIC_AUTH_TOKEN` and base URL/provider variables likewise require explicit route configuration;
- `CLAUDE_CODE_SIMPLE` does not read OAuth/keychain credentials and is not a drop-in subscription mode;
- an ELIOT module credential authenticates local IPC only.

Before first model input, record the chosen auth/provider mode or a truthful unknown. A mismatch fails the launch before effect; it does not trigger login, key creation or backend substitution.

## Compatibility check for a new launch

Do not compare the package version to a hardcoded release or range. Check only the interfaces this ELIOT artifact uses:

- selected package/entrypoint can be imported;
- query/prepared-query ownership contract;
- session/user-message identity in emitted messages;
- abort/cancellation support;
- permission mode option and observed/effective projection;
- `canUseTool` callback shape;
- `PreToolUse` hook and `defer` support if durable interaction is enabled;
- stream/result/init message forms used by the mapper;
- any explicitly declared resume/session persistence operation.

Unknown optional fields are ignored after bounds/type checks. A missing required field disables the affected capability with a precise diagnostic. It does not disable unrelated read-only functionality or cause a downgrade.

The adapter reports the actual package/runtime version it loaded. It never reports a manifest’s historical version as the current executor.

## Permission interaction profiles

The official evaluation order is hooks → deny → ask → mode → allow/auto-approved → `canUseTool`. Update qualification must preserve that order.

ELIOT supports two implementation profiles:

### Live callback

The Node owner remains alive while the callback waits. The adapter projects a bounded pending request and resolves it exactly once through `agent.reply`. Callback state belongs to the current boot and cannot be restored after Node loss.

### Durable defer

A `PreToolUse` hook returns the official `defer` decision. The native session persists, the process may exit, and later work resumes through the documented session mechanism. Enable this capability only when the installed interface exposes the exact hook/defer/resume contract used by ELIOT.

Do not fake durable defer by serializing a Promise or by sending the answer as a new model prompt.

## Files and ownership

| Unit | Owner / permitted change |
|---|---|
| `crates/swarm-adapter-claude/sdk-harness/{bridge,prepared-query}.mjs` | Current SDK loading, live callback/hook ownership and typed Node control frames. R16 owner. |
| `crates/swarm-adapter-claude/src/{config,module_runtime,sdk_harness,native_state,lib,journal,receipt}.rs` | Capability declaration, transport, durable Operation, decision readback and state. |
| `modules/claude/{bridge,stream,model-selection}.mjs` | Historical JavaScript implementation and mapper. Do not add a parallel new permission flow here. |
| `package*.json`, descriptors and examples | Describe ELIOT build/artifact bytes; never prescribe an old external runtime to the owner. |
| private route/configuration | Operator-owned. Repository updates do not rewrite it. |

No second Node process is launched per request. No new generic workflow service is introduced.

## Activation

1. Finish the connected code slice in one PR: loader → capability report → Node frames → Rust adapter → Store attention/admission → readback.
2. Build a new ELIOT artifact identity for changed bytes. Do not relabel the old artifact.
3. For new bindings, resolve the owner-selected current installation and run the compatibility/auth checks above.
4. Keep existing live bindings on their actual owner/process; do not hot-swap the SDK heap.
5. Stop creating new bindings on the superseded ELIOT executor only after the replacement covers its required capabilities.
6. Retain the minimum reader needed for historical receipts; this does not require retaining a second executor forever.

A capability gap blocks only operations that need it. For example, missing durable defer may leave live callback mode available.

## Verification before Ready

Manager-owned gates after implementation:

```sh
cargo clippy --locked -p swarm-adapter-claude -p swarm-contracts -p swarm-kernel-host -p swarm-mcp --lib --bins -- -D warnings
node --check crates/swarm-adapter-claude/sdk-harness/bridge.mjs
```

Final qualification uses the owner-selected subscription installation and records:

- actual runtime/package versions;
- auth/provider mode with no secret values;
- required capabilities present/absent;
- explicit and inherited permission modes;
- auto-approved calls bypassing `canUseTool` as designed;
- ask/deny/`dontAsk` behavior;
- live reply/abort ordering;
- durable defer/resume if supported;
- duplicate/conflicting decisions;
- host reconnect and Node loss;
- no repeated input or decision;
- no false tool/Task terminal from callback ACK.

A syntax check or older fixture pass does not qualify a new runtime. Native qualification does not authorize automatic future upgrades or changes to private configuration.

## Rollback

Rollback ELIOT by selecting the previous ELIOT artifact for **new** bindings. Existing work stays with its current process owner unless an explicit, safe native recovery contract applies. Do not downgrade the external harness merely to match the old artifact.

Unknown native effects remain readback-only. A lost live callback is recorded as lost; a truly deferred request follows its native persisted-session contract. Neither is replayed as a new prompt.