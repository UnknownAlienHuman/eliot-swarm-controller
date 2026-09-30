# Eliot Swarm Controller

Headless, modular Rust controller for native coding-agent harnesses. This is the prototype for Eliot Memory OS's Agent Execution Fabric, not a replacement model loop.

## Current implementation — 0.1.0

`swarm` contains a local user host, authenticated JSON-RPC IPC, a single SQLite owner thread, task/revision/claim operations, durable request receipts, opening-binding reservations, directed mailbox and incremental reports. Windows uses same-user Named Pipes; Unix uses a private local socket. No TCP listener is opened.

**Native execution is not connected yet.** No adapter, MCP facade, Cargo executor, acceptance pipeline or Windows-service installer is claimed complete. `agent.open` reserves a durable opening request but cannot report native readiness. `task.dispatch` refuses a missing/unready binding. There is no fake executor that marks a task running. These are implemented portions of C01/C02 and the local mailbox, not completion of every target contract in those packages.

Next: connect the Muse SDK bridge and direct OpenCode V2 boundary, adding the remaining producer/submission/check transitions where real consumers need them. Preserve native subscriptions, Max and existing shared-server ownership. Do not replace the working base with another architecture rewrite.

### Native integration checkpoint — 2026-09-30

The interrupted continuation reached committed Muse SDK preparation (`0e3ccd6b`, `db45be62`), not a published native executor. Its SDK input artifact was recovered and verified; the exact local dependency lock is now preserved in `modules/muse/package-lock.json`. [The module checkpoint](modules/muse/README.md) records recoverable inputs, missing local-only implementation bytes and the next code boundary. Do not reset or reimplement the working controller base.

The requested OpenAI Sign in with ChatGPT guide was reviewed separately. [Runtime notes §3.1](docs/runtime-notes.md) distinguish its owned stdio/OAuth route from our existing shared-server target, including token renewal and capability limits. This is a documented optional route, not installed authentication or a change to the Muse/OpenCode implementation priority.

### Build evidence

For controller commit `c37e6bbfb67c9fc97aaa6e78772884d43d73d92f`, [CI run 36693929400](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36693929400) passed formatting, Clippy with warnings denied and release builds on Windows and Linux on 2026-09-30. Recovery re-read those successful jobs; it did not rerun them. Rust 1.98.1 and the resolved Cargo.lock are retained. The permanent workflow is read-only, checks formatting without rewriting it, builds with `--locked`, and includes the exact source SHA in its binary artifacts.

The first Linux binary was also invoked locally: host startup, status, task creation/claim, identical-request replay, delta report and orderly shutdown succeeded. This was a short operational invocation, not a test suite or load qualification. No native SDK, model calls, Windows agent sessions or broad tests have run. Windows compilation does not establish runtime/ACL behavior on the owner's machine.

## Build and run

```powershell
cargo build --locked --release --bin swarm
.\target\release\swarm.exe --data-dir C:\SwarmState host
```

Use a dedicated local state directory: initially empty, then recognized by its locked ownership marker. The host creates only its own database, lock and `operator.json` credential there. It does not modify global PATH, UAC, existing harness settings or vendor services. It refuses an unrelated nonempty directory rather than rewriting its permissions. Keep credentials out of Git and messages.

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

For an unpacked CI binary, use `target\release\swarm.exe` in the artifact directory; no Rust install is needed to invoke that executable. Source builds use the pinned `rust-toolchain.toml`.

Repeat the identical method/payload with the same `--request-id` after a lost reply. This returns the saved receipt; a different payload under that ID is rejected. A new ID is a new request, although origin identity and unreleased task ownership still prevent duplicate work. A native-manager claim never generates an extra controller prompt. Do not put passwords or API keys in task text; the control mailbox is persistent project data.

`swarm call METHOD --file params.json` exposes implemented application methods without shell interpolation. `--request-id` is printed to stderr before a mutation; JSON results go to stdout. `--config config/controller.example.toml` loads the implementation configuration. Reference configurations in `docs/` describe the broader target and are not silently accepted as implemented settings. Read-only client calls do not initialize a new database or launch the host.

### Local clients and messages

```powershell
& $swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
& $swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

For `message.send`, pass `{"recipient":"W1","text":"..."}` in a params file. The recipient reads `message.read` with its credential and an `after` cursor. This is a persistent mailbox, not yet native wake/steer. Operator/manager/observer roles coordinate trusted clients of the same OS user; they do not sandbox full-access agents from one another.

Implemented methods: `host.status`, `host.mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch`, `attempt.get/release`, `agent.open/state/list`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Release requires explicit caller attestation that the assignment is closed; it never kills a process or fabricates successful acceptance. Unsupported methods fail without a native effect. Native-manager assignments can be coordinated locally; actual harness launch still belongs to the manager until its adapter is implemented.

## Design and development

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target responsibilities and execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transactional boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Native capability, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples; not proof of implementation |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Candidate source pins; not installed SDKs |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand evidence, not extra worker instructions |

This README records current implementation status. The dated design documents describe the larger target; their earlier "not implemented" statements record the design-stage snapshot, not deletion of the present code. No full C01–C11 completion is implied by compiling this first slice.

Work on `main`, without worktrees. Implement useful paths first, then `cargo fmt --all -- --check` and `cargo clippy --locked --lib --bins --no-deps -- -D warnings`. No test framework or broad test phase before working slices.

The runtime migration is frozen under `migrations/`; initialization stamps application ID, schema digest and version in one transaction. A draft/reference database, unknown schema or missing existing operator credential is not silently overwritten. A cleanly stopped state directory can be copied in full for preservation; do not copy only a live `.db` without its WAL.

Historical briefs were distilled and removed from the current tree. Originals remain at `b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62`; their local paths, launch commands and cancelled rules are not installation defaults.
