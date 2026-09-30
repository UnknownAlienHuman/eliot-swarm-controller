# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; the prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, broker or replacement model loop.

## Current implementation — 0.1.0

The local core implements authenticated clients, tasks/revisions/claims, durable request receipts, directed mailbox, incremental reports and module-scoped command admission. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

**The first Muse SDK bridge is now written and connected to the host API.** Its source is in `modules/muse/`; this is no longer only dependency preparation. It starts an explicitly selected native binary on `agent.open`, sends the Task snapshot with the prompt, supplies native per-turn reasoning effort, supports exact-turn steer and pending-request replies, and reports observed children/turn identities. Host disconnect leaves the independently running bridge and native connection alone. New native boots never silently replace possibly live old work.

**Qualification remains distinct from implementation.** Live Muse/Max inference, Windows launch behavior, native crash/resume, complete family reconstruction, goal configuration and task-specific child handoff are not qualified or complete. Family reports remain partial. OpenCode V2, shared Codex, the other adapters, MCP facade, CheckRunner/acceptance, automatic module launching and SCM installation are still pending. Do not mark all C01–C03 complete or equate a model turn ending with accepted work.

### Published checkpoints

- `c37e6bbf`: original local host/store/CLI, Windows/Linux builds and short local Linux invocation.
- `b2bd0211`: first runnable Muse bridge plus binding-scoped admission/fact persistence. [Run 36708933605](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36708933605) passed formatting, Clippy with warnings denied and release builds on Windows and Linux on 2026-09-30.
- `21502426`: wake the existing admission wait channel after a committed native outcome, without waking it for every telemetry event.

The workflow records the exact source SHA in each binary artifact. It is read-only, uses Rust 1.98.1 and committed Cargo.lock, checks formatting rather than rewriting it, and now also syntax-checks the Node bridge and imports its pinned SDK without spawning Muse. Check the run for the artifact's exact SHA; earlier green runs do not qualify later edits or live vendor behavior. No `cargo test`, model call, login or global installation is part of this workflow.

## Build and local core

```powershell
cargo build --locked --release --bin swarm
.\target\release\swarm.exe --data-dir C:\SwarmState host
```

Choose an initially empty dedicated directory. The host retains its ownership lock/marker, database and operator credential there. An unrelated nonempty directory is refused instead of having its permissions rewritten. Global PATH, UAC and vendor configurations are not changed. Read-only CLI commands do not initialize a database or launch the host.

In another PowerShell:

```powershell
$swarm = '.\target\release\swarm.exe'
& $swarm --data-dir C:\SwarmState status
& $swarm --data-dir C:\SwarmState --request-id create-demo-1 task create --project eliot-swarm-controller --file config\task.example.json
& $swarm --data-dir C:\SwarmState task list
# Replace TASK_ID with the returned task_id:
& $swarm --data-dir C:\SwarmState --request-id claim-demo-1 task claim TASK_ID --revision 1
& $swarm --data-dir C:\SwarmState report --after 0
```

Repeat an identical method/payload with its original `--request-id` after a lost response. A different payload under that ID is rejected. New request IDs do not bypass root-origin, unreleased-owner or initial-start uniqueness. Do not store credentials in task text; mailbox and receipts are persistent project data.

`swarm call METHOD --file params.json` invokes typed application methods without shell interpolation. JSON results go to stdout; the logical mutation ID is printed to stderr. `--config config/controller.example.toml` uses the actual implementation configuration, not the broader reference schemas in `docs/`.

### Native Muse setup

Follow [the module README](modules/muse/README.md): enable the private route, reserve `agent.open`, register a `module` credential for its binding/generation, install the locked module-local SDK with `npm ci --ignore-scripts`, and independently run `node modules/muse/bridge.mjs --config <private-file>` with the actual native executable. The example route is disabled; reading status cannot start Muse.

The bridge uses the existing subscription/authentication of the selected native executable. It is not a Go/API substitute for Muse Code Max. Requested per-turn effort and native model readback are recorded; neither is labelled measured inference before a real run. Output history remains in Muse, not a second transcript in SQLite.

`agent.send` supports explicit `next_turn` or `steer` with the expected native turn ID. `agent.reply` carries an exact pending protocol response or a supported native approval/input operation. The module uses separate SDK reader/replies and host outcome reporting; it does not wait for a model to finish before answering protocol requests. Unknown native admission does not cause a new prompt.

### Clients and mailbox

```powershell
& $swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
& $swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

`message.send` accepts recipient/text and optional in_reply_to. Each reader has its own cursor. This is a durable controller mailbox; automatic forwarding/wake to every harness is not implemented. Operator/manager/observer roles coordinate trusted same-user clients, not OS-isolated tenants. A module credential is separately scoped to one native binding and cannot accept tasks or impersonate GM.

Implemented public methods: `host.status/mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch`, `attempt.get/release`, `agent.open/state/list/send/reply`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. The module channel adds `module.hello/next/outcome/observe`. Release never kills a process or manufactures acceptance, and refuses known unfinished assigned runs. The complete producer/acceptance path remains future work. Unsupported methods return an explicit error.

## Next work and deferred integration

Finish and qualify the Muse native path, then add direct OpenCode V2 on the same host contract. Keep the shared Codex target and native subscription routes. The [SIWC note](docs/runtime-notes.md#31-chatgpt-plan-usage--отдельный-oauth-маршрут) describes an optional OAuth path, not installed authorization.

**[Issue #1 — OpenCodex module](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) is scheduled after the main controller code.** The review covers OpenCodex v2.73.0 / `569e3e7d` and its headless Management API. It will compose with the native Codex backend as a provider-service integration, not become another task scheduler or delay current core work. No OpenCodex installation or user configuration change has been performed here.

## Design and development

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target responsibilities and execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transaction boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Native capability, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not proof of implementation |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Source candidates, not an installed runtime inventory |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand evidence, not extra worker instructions |

This README records current readiness; dated design-stage statements do not override present code. Work in `main`, without worktrees. Implement real paths first, then focused formatting/Clippy. Do not build a new broad test framework instead of the product.

`migrations/001_core.sql` remains unchanged. Initialization stamps schema digest, application ID and version atomically; draft/foreign/newer databases and missing authority credentials are not silently replaced. Preserve a cleanly stopped state directory in full; do not copy only a live `.db` without WAL.

Historical briefs were distilled out of the current tree. Originals at `b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62` retain provenance, not present install defaults. The recovered Muse SDK preparation is documented in its module README; current source and lockfile no longer depend on temporary Actions artifact retention.
