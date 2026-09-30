# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, broker or replacement model loop.

## Current implementation — 0.1.0

The core provides authenticated clients, tasks/revisions/claims, durable request receipts, directed mailbox, incremental reports and binding-scoped module admission. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

**The first Muse SDK bridge is implemented and connected to the host API.** `modules/muse/` starts an explicitly configured native binary on a saved opening operation, sends the Task snapshot and per-turn reasoning effort, supports exact-turn steer and native question-answer commands, and reports observed children and turn identities. Bridge.2 adds native goal controls, one-setter configuration, explicit metadata refresh and same-command reconciliation. Bridge.3 adds exact-run Task/child registration, stable retained family pages, child-targeted metadata refresh and cancelled-before-start evidence. A host disconnect does not close the independently running bridge. A new bridge process cannot silently replace possibly live native work.

**Implementation is not runtime qualification.** Live Muse/Max inference and Windows native launch have not been exercised. Crash/resume, complete family reconstruction, autonomous task handoff, full result/artifact retrieval and acceptance are incomplete. OpenCode V2, Codex and other adapters, MCP, CheckRunner and automatic module/service installation remain pending. Family reports are partial; completed model turns do not accept Tasks. Do not mark the whole C01–C03 target complete.

### Code and build checkpoints

- `c37e6bbf`: original local host/store/CLI and short Linux invocation.
- `b2bd0211`: first Muse bridge and module channel; [run 36708933605](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36708933605) passed formatting, Clippy and release builds on Windows/Linux on 2026-09-30.
- `21502426`: native admission outcomes wake the existing command wait channel, without telemetry-driven polling storms.
- `58055475`: MSP presentation receipts are acknowledged immediately; actual decisions remain separate commands. Child goal events cannot overwrite the root goal.
- `45a5e649`: goal/configuration/refresh/reconciliation, monotonic observations and late-outcome handling. [Exact-commit CI](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36714173055) records compilation separately from native qualification.
- `36b4f702`: bridge.3 task-specific producer mapping, stable family pages and observed child refresh; [CI run](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36754364752) records the checks for this code.

Use CI for the exact artifact SHA; an earlier green run is not qualification of later code or live vendor behavior. The read-only workflow pins Rust 1.98.1/Cargo.lock, checks formatting and Clippy, builds the binary, and syntax-checks/imports the locked Muse SDK without starting Muse. It does not run `cargo test`, model calls, OAuth login or global installation. Binary artifacts contain their source SHA, tracked source archive and module source/configuration, not bundled vendor credentials or native runtimes.

## Build and local core

```powershell
cargo build --locked --release --bin swarm
.\target\release\swarm.exe --data-dir C:\SwarmState host
```

Use an initially empty dedicated local directory. The host keeps its ownership lock/marker, database and operator credential there. It refuses unrelated nonempty directories rather than changing their permissions. Global PATH, UAC and vendor settings are untouched. Read-only CLI calls do not initialize a database or launch a host.

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

After a lost reply, repeat the identical method/payload and original request ID. A different payload under that ID is rejected. New IDs do not bypass origin/ownership/initial-start uniqueness. Request IDs go to stderr; JSON results go to stdout. Keep credentials out of task text and persisted mailbox data.

`swarm call METHOD --file params.json` invokes application methods without shell interpolation. `--config config/controller.example.toml` uses the implemented configuration, not the broader reference examples under docs/.

## Native Muse and client integration

Follow [modules/muse/README.md](modules/muse/README.md): enable a private route, reserve agent.open, register a module credential for the returned binding/generation, install the locked module-local SDK, and independently start bridge.mjs with the actual native executable. The example route is disabled. A route/status query alone cannot launch Muse.

The native executable retains its subscription and auth. Requested effort is an explicit per-turn field; readback and actual inference evidence remain distinct. No Go/API route silently substitutes for Muse Code Max. Full history remains native, not duplicated into SQLite.

`agent.send` uses explicit next_turn or exact-turn steer. `agent.reply` submits supported native approval/user-input commands with their actual IDs. The bridge automatically acknowledges only the MSP **presentation receipt** `{}`; this is not permission or an answer. Protocol handling does not wait for GM to make the substantive decision.

```powershell
& $swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
& $swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

`message.send` accepts recipient/text and optional in_reply_to. Readers have independent cursors. This is a durable local mailbox, not yet automatic forwarding/wake for every harness. Same-user roles are cooperative controls, not OS isolation. Module credentials are separately scoped and cannot accept Tasks or impersonate GM.

Additional native controls: `agent.configure`, `agent.goal`, `agent.refresh`, `agent.reconcile`. See the module README for exact fields and native admission/application boundaries. Goal start requires a configured standing effort; refresh is not native resume. Unknown outcomes may be resolved by later evidence, never by an automatic new prompt.

Public methods: `host.status/mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Module methods: `module.hello/next/outcome/observe`. Release never kills a process or manufactures acceptance and refuses known unfinished assigned runs. Registered producers use exact native run evidence, not a session-wide idle flag. Task submission/acceptance and autonomous handoff remain future code; unsupported methods fail explicitly.

### Task-specific child evidence

`swarm task claim` now accepts `--binding-id` / `--generation`. After native delegation, `swarm family BINDING --generation 1` returns a recorded observation and actual child/run identities. `swarm task bind ATTEMPT --assignment NAME --session CHILD --turn TURN --observation-id ID` links that existing run without sending another prompt. Subsequent family pages use the same observation ID. An old completion cannot close a newer assignment; parent idle does not erase registered children. See the module README for the exact boundaries. No observer consumes native results through `subagent/readResult`.

## Next work

This continuation implements the controls above without changing the nine-table migration, dependency locks, toolchain or introducing new services. Native application and long-run recovery still require the actual installed Muse executable and authorized model route; compilation is not that qualification.

Complete and qualify Muse, then direct OpenCode V2 on the same host contract. Preserve the shared Codex/native-subscription targets. The [SIWC note](docs/runtime-notes.md) describes an optional OAuth route, not installed authorization.

**[Issue #1: OpenCodex module](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) is scheduled after the main controller code.** Source review covers v2.73.0 / 569e3e7d and the headless Management API. It composes with native Codex as a provider-service module, not a new scheduler. No OpenCodex installation or user configuration was changed.

## Design and development

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transaction boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Capabilities, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not implementation evidence |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Source candidates, not installed runtimes |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand reference, not extra worker instructions |

This README is the readiness entry point. Dated design-stage statements do not override present code. Work in main, without worktrees. Implement useful paths, then focused formatting/Clippy; do not build a broad test framework instead of the product.

The nine-table runtime migration remains unchanged. Schema identity/version and initial metadata are committed atomically. Foreign/draft/newer databases and missing credentials are not silently replaced. Preserve a cleanly stopped state directory in full; never copy only a live DB without WAL.

Historical briefs remain at b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62 for provenance, not present installation defaults. Current code and SDK lock no longer depend on temporary Actions artifact retention.
