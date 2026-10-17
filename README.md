# Eliot Swarm Controller

Headless Rust controller for native coding agents. It owns durable Tasks, Attempts,
Operations, events and results; runtime adapters translate those contracts into
native APIs. One SQLite database, one writer and authenticated local IPC form the
control plane. Windows uses user-restricted Named Pipes; Unix uses a private socket.

Eliot Memory OS is not yet connected on the owner machine. The standalone controller
remains usable; the conditional `eliot_compile_packet_l3` request is recorded in
the [integration contract](docs/eliot-memory-os-integration.md), not claimed as an
implemented packet capability.

For the Memory OS integration, the current owner scope is contract and boundary
preparation only; runtime integration awaits a separate owner instruction.
Standalone controller development and ordinary verification continue independently.

## Current implementation — 0.1.0

Project status is **`PARTIAL_PROGRESS`**. The core has durable request receipts,
Task ownership, module admission, immutable artifacts, submission/review/acceptance
and fixed-source CheckRunner execution with scoped cancellation and recovery.
Runtime source, fixtures and native observations have different coverage.

### Capability and qualification matrix

Use the [current source and completion map](docs/implementation-status.md#current-source-and-completion-map)
for artifact versions, implemented behavior and remaining qualification. Runtime
guides below own their commands and compatibility details. Historical evidence
applies to its recorded revision; a fixture pass is not native qualification.
V3 is the required refactoring target, not a description of a completed rewrite.

## Build and run

Build into the shared external target and explicitly start the host:

```powershell
$target = Join-Path $env:LOCALAPPDATA 'Eliot\build\rust-env-target'
[void][IO.Directory]::CreateDirectory($target)
cargo build --locked --release --package swarm-cli --package eliot-swarm-controller --package swarm-kernel-host --package swarm-supervisor --package swarm-mcp --bins --target-dir $target
$swarmHost = Join-Path (Join-Path $target 'release') 'swarm-host.exe'
& $swarmHost --data-dir C:\SwarmState host
```

Use an initially empty dedicated local directory. The Kernel owns its marker/lock,
database, artifacts and credentials there and refuses unrelated nonempty directories.
Ordinary CLI requests do not initialize a database or start the host. CLI, launcher,
Kernel and supervisor are separate artifacts; installation verifies their declared
sibling/protocol contracts and individual provenance. See [module installation](docs/agent-operations/module-installation.md).

In another PowerShell:

```powershell
$target = Join-Path $env:LOCALAPPDATA 'Eliot\build\rust-env-target'
$swarm = Join-Path (Join-Path $target 'release') 'swarm.exe'
& $swarm --data-dir C:\SwarmState status
& $swarm --data-dir C:\SwarmState --request-id create-demo-1 task create --project eliot-swarm-controller --file config\task.example.json
& $swarm --data-dir C:\SwarmState task list
# Replace TASK_ID with the returned task_id:
& $swarm --data-dir C:\SwarmState --request-id claim-demo-1 task claim TASK_ID --revision 1
& $swarm --data-dir C:\SwarmState report --after 0
```

After a lost reply, repeat the identical method, payload and request ID. Different
content under that ID is rejected; a new ID does not bypass ownership or start
uniqueness. Request IDs go to stderr and JSON results to stdout. Keep secrets out
of persisted Task text and mailbox messages.

`swarm call METHOD --file params.json` invokes the implemented API.
`--config config/controller.example.toml` uses the [implementation configuration](config/controller.example.toml);
broader target examples in specifications are not interchangeable with it.

Host builds require `libclang` for `rusqlite`'s `preupdate_hook` binding generation:
`libclang-dev` on Debian/Ubuntu, or LLVM with `LIBCLANG_PATH` pointing to `bin` on
Windows. See [scoped builds](docs/ci-scoped-builds.md) for incremental development.

## Complete results without model roundtrips

Obtain native pages with `swarm result BINDING --generation 1 --file selector.json --offset N --length 65536`. Each settled `agent.result` provides `result.details.artifact_ref` and `next_offset_bytes`. Reuse the same selector, item revision and source digest for subsequent pages. Native source offsets and local page offsets are different.

Create `pages.json` with actual retained IDs in source-byte order:

```json
{"page_refs":["result-FIRST_PAGE_ID","result-SECOND_PAGE_ID"]}
```

Optional `expected_sha256` means the expected **whole-body** SHA-256, not a page hash. Do not invent one when the source supplies none.

```powershell
swarm --request-id assemble-result-1 artifact assemble --file pages.json
# operation.json contains the returned operation_id:
swarm call operation.get --file operation.json
# After outcome=applied, use result.details.artifact_ref:
swarm artifact get ASSEMBLED_ID
swarm artifact parts ASSEMBLED_ID --after 0 --limit 50
swarm artifact read ASSEMBLED_ID --offset 65520 --length 64
swarm artifact export ASSEMBLED_ID --out .\worker-result.json
```

Assembly rejects mixed bindings/generations/selectors/source revisions, reordered/duplicate/overlapping pages, missing bytes, incorrect EOF and altered bytes. It verifies any supplied native SHA-256 and explicit whole digest; absent native digests remain unverified. Source identity must match except the per-page whole-digest-verification flag.

The initial response is durable admission; `operation.get` reports completion or a concrete error. File work stays off the SQLite thread. Repeats return the same operation; unfinished **local assembly** can resume through the identical request ID after host restart. Existing files are verified, not overwritten. This does not authorize native prompt replay. A known failed assembly remains failed; repaired inputs use a new request ID.

Original pages remain retained; provenance is paginated. Export streams over one authenticated IPC connection, verifies the whole digest and only then publishes the chosen local destination. Existing paths are never replaced; failed exports remove only their own temporary file. The destination path is not sent to the host or model.

64 KiB limits transfers, not the total result. Assembly holds one content page plus descriptors; request/manifest metadata still scales with page count and must fit the IPC envelope. Automatic missing-page fetching, arbitrary reference-URI downloads and Task acceptance are not performed by this path.

## Runtime and workflow guides

| Work | Guide |
| --- | --- |
| OpenCode HTTP service / standalone Rust adapter | [Service integration](modules/opencode/README.md), [Rust adapter](crates/swarm-adapter-opencode/README.md) |
| Codex app-server / provider service | [Codex](modules/codex/README.md), [OpenCodex](modules/opencodex/README.md) |
| Muse native sessions and recorded recovery | [Muse](modules/muse/README.md) |
| Claude / Antigravity / Command | [Claude](modules/claude/README.md), [Antigravity](modules/antigravity/README.md), [Command](modules/command/README.md) |
| Zed batch execution | [Zed](docs/zed-batch.md) |
| Capture, checks, cancellation and recovery | [CheckRunner](docs/check-runner.md) |
| Result assembly and verified export | [Result assembly](#complete-results-without-model-roundtrips) |
| Submission, review, repair and acceptance | [Task policy](docs/task-policy.md), [delivery](docs/agent-operations/delivery.md) |
| Accepted-candidate publication | [Forge publication](docs/forge-publication.md) |
| Participant/GM communication and MCP | [Communication program](docs/agent-communication-program.md), [MCP surfaces](docs/mcp-canonical-surfaces-and-topologies.md) |

## Evidence and development

Start with [AGENTS.md](AGENTS.md), the [V3 documentation index](docs/agent-operations/README.md)
and the [refactoring map](docs/agent-operations/refactoring.md). The map owns current
source gaps and deletion candidates; [implementation status](docs/implementation-status.md)
owns evidence. Read the relevant contract and source for the assigned slice.

One manager integrates one worktree. Complete the connected source path, then run
scoped formatting and minimal strict Clippy with actual reverse dependencies.
`just verify BASE_SHA HEAD_SHA` uses two full commit SHAs; `just verify-full` retains
the full local gate. Documentation-only changes need no Rust build. Do not blanket
format the preserved Atlas donor or run repeated full builds for writer fragments.

Full tests and native qualification follow completed source. A scoped CI pass does
not establish a passing host suite or live runtime behavior. Recorded commands,
source revisions and accessible artifacts in [implementation status](docs/implementation-status.md)
define the evidence; hashes of unavailable operator logs do not establish a new pass.
Preserve a cleanly stopped state directory in full, including its retained identities;
foreign/newer databases and missing credentials are not silently replaced.
