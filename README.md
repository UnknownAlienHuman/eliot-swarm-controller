# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, broker or replacement model loop.

## Current implementation — 0.1.0

The Rust core provides authenticated clients, tasks/revisions/claims, durable request receipts, directed mailbox, incremental reports and binding-scoped module admission. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

**The Muse SDK bridge is connected to the host API.** It opens an explicitly configured native executable, delivers the Task snapshot and per-turn effort, supports exact-turn steer, native questions, goal/configuration controls and explicit reconciliation. Task producers are bound to observed session/run identities; family pages use a retained observation. Bridge.4 adds on-demand result pages and immutable local artifact reads. Host disconnect does not close the independently running bridge or replay model input.

**Implementation is not live qualification.** Muse/Max inference and Windows native launch have not been exercised. Bridge-process crash/resume, complete family reconstruction, automatic handoff, whole-result assembly, Task submission/acceptance and CheckRunner remain unfinished. OpenCode V2, Codex and other adapters, MCP and automatic module/service installation are pending. Partial family data and completed turns do not accept Tasks. Do not mark all C01–C03 complete.

### Recovery checkpoint — 2026-09-30

The interrupted continuation after `00fdce16` did publish its code:

| Commit | Saved work |
| --- | --- |
| `36b4f702` | Exact-run Task/child mapping and retained family pages; earlier baseline. |
| `46a216a7` | `agent.result`, `module.result`, Muse `results.mjs`, immutable artifact publication/read, CLI commands; pinned base64 0.22.1. |
| `684bc33a` | Checked signed SQLite lengths; fixes the first result-slice compilation failure without changing the migration. |

For **`684bc33a79eed64656e291a8fddb74108f531e5e`**, [CI run 36759367801](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36759367801) passed formatting, warnings-denied Clippy, Muse module syntax/SDK import and release builds on Windows and Linux. These are observed completed jobs, not an assumption based on an earlier green commit.

Recovery downloaded artifact `11117908898`, verified its ZIP and SHA-256 `5197475534e15980157ebbc0d8ebfa18f80891bf2e44e1e574ae422224d7255b`, and reconstructed source tree **`7debef1ee27af1dcb44c86de133b1c1a9d3dfefb`**, exactly matching that commit. No additional unpublished source files were found in the current mounted workspace. The downloaded Linux binary's result/artifact help, empty-host startup/status and orderly shutdown were invoked successfully; no native module or model was started. Recovery found documentation lagging behind the code, not a missing result implementation. These README updates do not change the compiled source.

**Resume from the existing code**, not from the old bridge.3 instructions. Relevant files: `modules/muse/results.mjs`, `src/artifacts.rs`, `src/store/results.rs`, and their wired paths in `src/store/mod.rs`, `src/store/runtime.rs` and `src/main.rs`. First finish the remaining Muse recovery/result-consumer boundaries, then direct OpenCode V2 on the same host contract. Do not rewrite the core or restart the platform selection.

The read-only workflow pins Rust 1.98.1/Cargo.lock, checks formatting without rewriting it and builds with `--locked`. Binary artifacts contain the exact source SHA and source archive. No `cargo test`, model calls, OAuth login or global installation is part of these checks. Windows compilation does not qualify native launch/ACL behavior on the owner's machine.

## Build and local core

```powershell
cargo build --locked --release --bin swarm
.\target\release\swarm.exe --data-dir C:\SwarmState host
```

Use an initially empty dedicated local directory. The host keeps its ownership marker/lock, database, artifacts and operator credential there. It refuses unrelated nonempty directories rather than changing their permissions. Global PATH, UAC and vendor settings are untouched. Read-only CLI calls do not initialize a database or launch a host.

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

Follow [modules/muse/README.md](modules/muse/README.md). Enable a private route, reserve `agent.open`, register its scoped module credential, install the locked module-local SDK and independently start the bridge with the actual native executable. The shipped route remains disabled. Match `moduleArtifactId` to the route; new example bindings use **`muse-sdk-1.3.0-bridge.4`**. Do not overwrite an already running bridge in place.

The executable retains its native subscription/auth. Requested effort, effective setting and actual inference evidence remain distinct. No Go/API route silently substitutes for Muse Code Max. Full conversation history remains native rather than duplicated into SQLite.

`agent.send` supports next_turn or exact-turn steer. `agent.reply` submits native approval/user-input commands with their actual IDs. The bridge immediately acknowledges only the MSP presentation receipt `{}`; that is not approval or a substantive answer. `agent.configure/goal/refresh/reconcile` retain their different application boundaries. Goal start requires a configured standing effort; refresh is not resume. Unknown outcomes are not resolved by an automatic new prompt.

### Task-specific children

`task claim` accepts `--binding-id` and `--generation`. After native delegation, `swarm family BINDING --generation 1` returns retained evidence. `swarm task bind ATTEMPT --assignment NAME --session CHILD --turn TURN --observation-id ID` associates an already observed run without starting it again. Subsequent family pages use the same observation ID. Parent idle does not erase children; an old completion does not close a newer assignment.

### Result pages and local artifacts

`swarm result BINDING --generation 1 --file selector.json` queues one read of a pinned native item revision. The Muse-specific selector chooses a subagent result, complete agent message, stored output or patch. Read the returned Operation to obtain `result.details.artifact_ref`, then use:

```powershell
swarm artifact get ARTIFACT_ID
swarm artifact read ARTIFACT_ID --offset 0 --length 65536
```

Supply the normal host/data-dir/credential options. See the module README for selector fields and paging. Artifact offsets are local to that retained page; native-result offsets refer to the source body. The 64 KiB page size limits transport buffers, not the total result size. A retained page is not proof that every page or the entire Task is accepted. No result observation calls the state-changing `subagent/readResult` or fetches arbitrary reference URLs/paths.

### Clients and mailbox

```powershell
swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

`message.send` accepts recipient/text and optional in_reply_to. Readers have independent cursors. This is a durable mailbox, not automatic wake/forwarding for every harness. Same-user roles are cooperative controls, not OS isolation. Module credentials cannot accept Tasks or impersonate GM.

Public methods: `host.status/mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile/result`, `artifact.get/read`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Module methods: `module.hello/next/outcome/observe/result`. Release never kills a process or manufactures acceptance. Unsupported methods fail explicitly.

## Remaining work and design

Complete Muse recovery/result-consumer integration, then direct OpenCode V2. Preserve the shared Codex/native-subscription targets; the [SIWC note](docs/runtime-notes.md) describes an optional OAuth route, not installed authorization.

**[Issue #1: OpenCodex module](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) remains after the main controller code.** It composes with native Codex as a provider-service module, not a second scheduler. No OpenCodex installation or user configuration was changed.

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transaction boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Capabilities, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not implementation evidence |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Source candidates, not installed runtimes |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand reference, not extra worker instructions |

This README is the implementation-status entry point. Dated design-stage statements do not override present code. Work in main, without worktrees. Implement useful paths, then focused formatting/Clippy; do not build a broad test framework instead of the product.

The nine-table runtime migration remains unchanged. Schema identity/version and initial metadata are committed atomically. Foreign/draft/newer databases and missing credentials are not silently replaced. Preserve a cleanly stopped state directory in full; never copy only a live DB without WAL. Historical briefs remain in Git history for provenance, not installation defaults. Temporary Actions retention is not dependency or source authority.
