# Muse SDK bridge — first native integration slice

This module now contains executable code, not just dependency preparation. It uses the complete published `@muse-code/sdk` **1.3.0**, pinned by package-lock.json, and the reviewed MSP schema at `meta-models/muse-code-sdk@a7c10c5dd3f66be412077d29f9d11111af70317b`. Runtime/Windows/model qualification is still pending; SDK syntax/import checks are not a completed model run.

## Ownership and implementation

One independently started Node process owns one native `muse serve` connection and its descendants. The existing Rust host exposes `module.hello/next/outcome/observe` over its authenticated local IPC. There is no new listener, model proxy or second task database. An operator-issued module credential is scoped to one reserved binding/generation and cannot call task acceptance or general manager methods.

The host commits `queued -> sending` before yielding a command. The bridge reports native identity/admission separately from completed turns. It keeps unacknowledged command outcomes until the host commits them, and never repeats a native command after losing a host response. `task.dispatch` includes the immutable Task snapshot. Explicit next-turn input and exact-turn steer use the native SDK. Replies remain independent of command admission and model execution.

Host disconnect does not call SDK.close. The same live bridge reconnects with its boot/native identities. A different bridge process encountering possibly live prior native work is refused for reconciliation rather than silently spawning another executor. Explicitly terminating this bridge closes its owned native connection; it must not be used to stop an unrelated shared service.

Native token deltas and full transcripts stay in Muse. The bridge reports compact state, observed children, pending requests, exact turn terminals and an unsummed native usage snapshot. Family completeness is **partial**, not inferred from a root's idle event. This first slice does not implement automatic bridge launching, crash-time native resume, complete family reconstruction, goal configuration, artifact retrieval, task acceptance, or autonomous handoff after an unknown effect. Do not label full C03 complete.

## First setup (explicit, local)

1. `npm ci --ignore-scripts` in this directory. This installs only the locked local SDK; it does not install Muse, modify global packages or log in.
2. Select the actual installed Muse executable and its existing authentication. On Windows use the native `.exe`, not a `.cmd`/`.bat` shell wrapper. Keep `command` and `args` separate. Choose the installed runtime's supported launch arguments yourself; this module does not edit UAC, permissions or global Muse settings.
3. Add an enabled route to a private controller TOML (example below). Start `swarm --config <file> host`. Call `agent.open` with `{"lane_id":"MC","route":"muse-manager"}` using the existing `swarm call ... --file ...` interface. Save its `binding_id` and `generation`.
4. As operator, run `swarm client-create muse-MC --role module --binding-id <binding> --generation 1 --out <private-credential.json>` with the same `--data-dir`. Save/reuse the printed request ID if a reply is lost. Do not share this credential with the model.
5. Copy `module.example.json` outside the repository. Set the host's exact pipe/socket endpoint printed at startup, credential file, native executable and args. Then run `node modules/muse/bridge.mjs --config <private-module.json>` independently of the host. It waits for the reserved opening operation before spawning Muse.
6. `agent.state` with binding/generation exposes the native readback. For a controller-start Task, call `task.claim` with `start_owner:"controller"`, `binding_id` and `binding_generation`, then `task.dispatch` with its `attempt_id` and `text`. The generic `call` command accepts these fields; the short task-claim CLI does not yet expose binding flags.

```toml
[[routes]]
alias = 'muse-manager'
runtime = 'muse'
module_artifact_id = 'muse-sdk-1.3.0-bridge.1'
enabled = true
[routes.native_options]
workspaceRoot = 'C:\Projects\YourRepository'
modelId = 'muse-spark-1.3-contributor'
reasoningEffort = 'max'
approvalMode = 'allowAll'
```

These are adapter-owned camelCase fields, not the former placeholder snake_case route example. Model/approval mode must exist in the selected native runtime. Effort is sent as an explicit per-turn option; opening the session alone does not prove Max inference. A model readback mismatch is not silently accepted.

`agent.send`: `binding_id`, `generation`, `text`, and `delivery:"next_turn"` or `delivery:"steer"` with `expected_turn_id`.

`agent.reply`: `binding_id`, `generation`, and `reply`. For a pending native server request, use its exact `request_id` plus a protocol-correct `response`. For native approval/user-input commands, use `reply.method` and `reply.params` matching the installed MSP schema; only `approval/decide` and `userInput/answer|cancel|clarify` are accepted, scoped to the observed family. No generic arbitrary-method passthrough is exposed. Full permissions do not authorize guessing an answer to a substantive question.

## Recovery checkpoint and source provenance

The prior interrupted continuation published preparation only (`0e3ccd6b`, `db45be62`). Its recovered input artifact, Actions run 36696436986 / artifact 11087808310, SHA-256 `b0e06843336290e685cd79376c709be4ac7ab79accf3cb09f9bd2beb1826ad52`, contained the SDK source/schema/fixtures/package, not this bridge. The lockfile is committed; the temporary Actions archive is not the dependency authority. No need to reconstruct or roll back the built local controller at c37e6bbf.

Next native work: qualify this actual SDK path, add explicit recovery/reconciliation and task-specific child bindings, then direct OpenCode V2 against the same control contract. OpenCodex is separately deferred in [Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) until the main controller code is complete.
