# Eliot Swarm Controller

Headless, modular Rust controller for native coding-agent harnesses. This is the prototype for Eliot Memory OS's Agent Execution Fabric, not a replacement model loop.

## Current implementation

`swarm` now contains a local user host, authenticated JSON-RPC IPC, a single SQLite owner thread, task/revision/claim operations, durable request receipts, opening-binding reservations, directed mailbox and incremental reports. Windows uses same-user Named Pipes; Unix uses a private local socket. No network listener is opened.

**Native execution is not connected yet.** No adapter, MCP facade, Cargo executor, acceptance pipeline or Windows-service installer is claimed complete. `agent.open` can reserve a durable opening request, but cannot report native readiness. `task.dispatch` refuses a missing/unready binding. The next implementation work is the Muse SDK bridge and direct OpenCode V2 boundary, with the remaining task/check lifecycle added on their real consumers.

The first Rust changes are undergoing compilation/Clippy through the repository workflow. No vendor SDK, model call, Windows agent session or load qualification has run merely because these files exist.

## Build and run

```powershell
cargo build --release --bin swarm
.\target\release\swarm.exe --data-dir C:\SwarmState host
```

Use a dedicated, local state directory. The host creates only its own database, lock and `operator.json` credential there. It does not change global PATH, UAC, existing harness settings or any vendor service. The state directory is restricted to the current user. Do not point it at an existing shared data directory.

In another PowerShell:

```powershell
$swarm = '.\target\release\swarm.exe'
& $swarm --data-dir C:\SwarmState status
& $swarm --data-dir C:\SwarmState --request-id create-demo-1 task create --project eliot-swarm-controller --file config\task.example.json
& $swarm --data-dir C:\SwarmState task list
# Replace TASK_ID with task_id from create/list:
& $swarm --data-dir C:\SwarmState --request-id claim-demo-1 task claim TASK_ID --revision 1
& $swarm --data-dir C:\SwarmState report --after 0
```

Repeat the exact same method/payload with the same `--request-id` after a lost reply. This returns the saved receipt; a different payload under that ID is rejected. A new ID is a new request, although origin identity and unreleased task ownership still prevent duplicate work. A native-manager claim never generates an extra controller prompt.

`swarm call METHOD --file params.json` exposes the implemented application methods without shell interpolation. `--request-id` is printed to stderr before each mutation; JSON results go to stdout. `--config config/controller.example.toml` loads the implementation configuration. The reference configurations in `docs/` describe the broader target and are not silently accepted as implemented settings.

### Local clients and messages

```powershell
& $swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
& $swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

For `message.send`, pass `{"recipient":"W1","text":"..."}` in a params file. The recipient reads `message.read` with its own credential and an `after` cursor. This is a persistent mailbox, not yet a native wake/steer integration. Operator/manager/observer roles coordinate trusted clients of the same Windows user; they do not sandbox full-access agents from one another.

Implemented methods: `host.status`, `host.mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch`, `attempt.get/release`, `agent.open/state/list`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Release requires explicit caller attestation that the assignment is closed; it never kills a process or fabricates successful acceptance. Other methods fail explicitly, without side effects.

## Design and development

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target responsibilities and execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transactional boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Native capability, delivery and lifecycle boundaries |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples; not proof of implementation |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Candidate source pins; not an installed dependency lock |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand evidence, not additional worker instructions |

Work on `main`, without worktrees. Implement useful paths first, then focused `cargo fmt` and `cargo clippy --lib --bins --no-deps -- -D warnings`. Do not build a test framework or run the broad test phase before the working slices.

The runtime migration is frozen under `migrations/`; initialization stamps application ID, schema digest and version in the same transaction. A draft/reference database, unknown schema or missing existing operator credential is not silently overwritten. Keep the entire state directory for recovery; do not copy only a live `.db` without its WAL.

Historical briefs were distilled and removed from the current tree. Originals remain at `b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62`; their local paths, launch commands and cancelled rules are not installation defaults.
