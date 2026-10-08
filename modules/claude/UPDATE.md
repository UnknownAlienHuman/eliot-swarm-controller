# Claude module — updates without freezing the user's harness

**Owner correction: 2026-10-08.** The integration must keep the existing subscription account and follow the user's normally updated native harness. Do not prescribe an old SDK/CLI release, disable native updates, require a downgrade or substitute separately billed model API access.

This document corrects the update requirements. It does **not** claim the code already meets them: the repository still contains fixed package declarations and the Rust Node driver still has its 0.3.287 equality gate. Their removal and connected capability handling belong to [R16](../../docs/remediation/2026-10-07/16-claude-interactions.md). Changing this text does not change installed packages or running sessions.

## Contract sources

- [Module contract](../../docs/agent_swarm.module-contract-v2.md): command identity, ownership, observations and uncertain outcomes.
- [README](README.md): historical JavaScript bridge status versus the separate Rust+Node implementation.
- The definitions and documentation of the native interfaces actually used by the currently installed harness/SDK. Record what was examined; a historical version string in an audit is not a launch allowlist.
- Existing fixtures are authored protocol examples, not live captures. Keep them as evidence for their actual shapes; do not claim they qualify every later release.

## Required update behavior

1. Resolve the currently selected installation for a **new** launch/connection. Retain the existing subscription authorization, workspace and owner-selected model/permissions. Do not create another auth/billing route or silently launch an old bundled executable.
2. Observe runtime/package versions separately from compatibility. Check the actual required imports, initialization, message forms and control methods. A different release number alone is not a rejection; a missing required guarantee limits the affected capability explicitly.
3. Remove release-equality checks together with dependent callers and false version projections. Do not replace one pin with another hardcoded release range or a fabricated universal fallback. Report the runtime actually used.
4. Update package-loading metadata and documentation consistently when needed. A build manifest or lockfile describing the ELIOT build must not force the user's external harness back to an old release. No package installation or update occurs as a side effect of status/Doctor.
5. Preserve the identity of the actual ELIOT implementation and all admitted Operations. The identity of shipped controller bytes is not an external-runtime version freeze. Do not relabel old receipts/checkpoints or report an old SDK version as the current executor version.

## Files and owners

| Unit | Scope |
|---|---|
| `crates/swarm-adapter-claude/sdk-harness/bridge.mjs` and `prepared-query.mjs` | Current selected SDK/CLI boundary, prepared-query ownership and live callbacks. R16 changes this driver; no second driver per question. |
| Rust adapter `src/{config,module_runtime,sdk_harness,native_state,lib,journal,receipt}.rs` | Actual capability declaration, transport, current request identity, durable decision and readback. |
| This directory's `bridge.mjs`, `stream.mjs`, `model-selection.mjs` | Historical JavaScript executor and mapper; do not develop a parallel new reply implementation here. |
| Package manifests, descriptors, examples and READMEs | Describe the implementation actually shipped; remove requirements to freeze the external native product. Do not change live private configuration through repository examples. |

Prepared input must still be claimed once. A newer interface is not permission to replay an uncertain earlier input, adopt a different session under an old ID or answer a callback lost with its process.

## Verification and activation

After the connected code slice is complete, the manager runs scoped formatting, the minimal warnings-denied Clippy gate for changed Rust packages and `node --check` for changed JavaScript. Broad fixture/native/subscription checks follow in the final qualification phase; writers do not run Cargo.

Qualification includes an ordinary compatible native update, truthful capability loss where a required interface is missing, preserved subscription authorization, current model/permission readback, host reconnect, callback cancellation and no duplicate input/decision. No account key is requested merely to run these checks. Record each actual invocation and outcome separately.

Do not overwrite a running bridge or change a live SDK heap during an update. Existing work remains with its actual process owner; new launches resolve the current installation. Recovery/rollback of ELIOT code does not reverse native effects and does not authorize downgrading the user's harness. Unknown effects remain readback-only.
