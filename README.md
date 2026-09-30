# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, external broker or replacement model loop.

## Current implementation — 0.1.0

The Rust core provides authenticated clients, tasks/revisions/claims, durable request receipts, directed mailbox, incremental reports, binding-scoped module admission and immutable result storage. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

The Muse SDK bridge opens an explicitly selected native executable, delivers Task snapshots with per-turn effort, handles exact-turn steer/questions/goal/configuration, and reports observed children and run identities. A live bridge reconnects without closing Muse or replaying model input. Bridge.4 reads pinned native result pages without consuming `subagent/readResult`.

**Local whole-result assembly and export are implemented.** `artifact.assemble` validates a complete ordered set of retained pages, publishes a whole-result file and records provenance. `artifact.parts` pages that provenance; `artifact.read` verifies touched segments. `swarm artifact export` reuses one authenticated IPC connection, streams bytes to an explicitly chosen local file and checks the full SHA-256 before publication. It never sends the destination path to a model or the host.

**Still pending:** bridge-process crash/resume, complete native family reconstruction, automatic handoff, Task acceptance, CheckRunner, direct OpenCode V2 and other native adapters, MCP and automatic module/service installation. Whole-result coverage is not Task acceptance. Live Muse/Max inference and Windows native launch remain unqualified. Do not mark all C01–C03 complete.

## Build and run

```powershell
cargo build --locked --release --bin swarm
.\target\release\swarm.exe --data-dir C:\SwarmState host
```

Use an initially empty dedicated local directory. The host owns only its marker/lock, database, artifacts and credentials there. It refuses unrelated nonempty directories; global PATH, UAC and existing vendor settings are not changed. Read-only CLI calls do not initialize a database or launch the host.

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

Repeat the identical method/payload and request ID after a lost reply. A different payload under that ID is rejected. New IDs do not bypass origin/ownership/initial-start uniqueness. Request IDs go to stderr, JSON results to stdout. Keep secrets out of persisted task text and mailbox messages.

`swarm call METHOD --file params.json` invokes the implemented API. `--config config/controller.example.toml` uses the implementation configuration, not the broader target examples under docs/.

## Native Muse, clients and task-specific children

Follow [modules/muse/README.md](modules/muse/README.md). Enable a private route, reserve `agent.open`, register its scoped module credential, install the locked module-local SDK and independently launch the bridge using the installed native executable. The shipped route is disabled; its artifact version is **`muse-sdk-1.3.0-bridge.4`**. These local artifact changes do not require replacing a running bridge.

Native subscriptions/auth and effort remain in the harness. Requested effort, effective setting and observed inference are different evidence. No Go/API route silently substitutes for Muse Code Max. Full conversation history stays native.

`agent.send` supports next-turn input or exact-turn steer. `agent.reply` submits native decisions with current IDs; the bridge's immediate MSP `{}` response acknowledges presentation only. Configure/goal/refresh/reconcile keep their distinct application boundaries. Refresh is not resume and a timeout does not authorize another prompt.

```powershell
swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

`task claim --binding-id BINDING --generation 1` defaults to `native_manager`: native delegation does not receive a duplicate controller start. `swarm family BINDING --generation 1` returns a retained observation; later pages reuse its observation_id. `swarm task bind ATTEMPT --assignment NAME --session CHILD --turn TURN --observation-id ID` maps an already observed run. An old completion does not close a new assignment; parent idle does not erase children. Family coverage remains partial.

Mailbox readers have independent cursors. Module credentials are binding-scoped and cannot accept Tasks or become GM. Same-user roles coordinate trusted clients; they are not an OS sandbox.

## Complete results without model roundtrips

First obtain native pages with `swarm result BINDING --generation 1 --file selector.json --offset N --length 65536`. Each settled `agent.result` provides a `result.details.artifact_ref` and `next_offset_bytes`. Reuse the same selector, item revision and source digest for subsequent pages. Native source offsets and local page offsets are different. See the module README for native selectors.

Create `pages.json` containing the actual retained artifact IDs in source-byte order:

```json
{"page_refs":["result-FIRST_PAGE_ID","result-SECOND_PAGE_ID"]}
```

Optional `expected_sha256` is the expected **whole-body** SHA-256, not a page hash. Do not invent one when the source does not supply it.

```powershell
swarm --data-dir C:\SwarmState --request-id assemble-result-1 artifact assemble --file pages.json
# operation.json contains the returned operation_id:
swarm --data-dir C:\SwarmState call operation.get --file operation.json
# After outcome=applied, use result.details.artifact_ref:
swarm --data-dir C:\SwarmState artifact get ASSEMBLED_ID
swarm --data-dir C:\SwarmState artifact parts ASSEMBLED_ID --after 0 --limit 50
swarm --data-dir C:\SwarmState artifact read ASSEMBLED_ID --offset 65520 --length 64
swarm --data-dir C:\SwarmState artifact export ASSEMBLED_ID --out .\worker-result.json
```

Assembly accepts registered result pages only. It rejects mixed bindings/generations/selectors/source revisions, reordered/duplicate/overlapping pages, missing bytes, incorrect EOF and altered bytes. Source identity must match exactly, except the per-page whole-digest-verification flag. It computes a whole SHA-256 and checks any reported native SHA-256 and explicit expected digest. Missing native digests remain unverified, not fabricated.

The initial response is the saved admission receipt. `operation.get` reports completion or the concrete assembly error. File work runs outside the SQLite owner thread. Concurrent repeats return the same operation; after a host restart an unfinished **local assembly** can continue through the identical request ID. A previously published file is verified, never overwritten. This local recovery rule does not authorize replay of native prompts or restart Muse. A known failed assembly remains a failed outcome; retry a repaired input with a new request ID.

Original pages and source provenance remain retained. The whole result has its own file/digest; large manifests are paginated rather than copied into every range response. Export reads byte ranges over one IPC connection, verifies the final digest and only then creates the requested file. Existing destination paths are never replaced. Failed exports remove only their own temporary file.

The 64 KiB bound applies to byte transfers, not the total result size. Assembly keeps one content page in memory at a time plus its page descriptors. Request/manifest metadata still scales with the number of pages and must fit the existing IPC envelope. Automatic fetching of missing pages, arbitrary reference-URI downloads and semantic acceptance are not performed by this path.

## Task submission and anchored feedback

`task.submit` now seals a complete retained result together with the Attempt's requirement report.
The candidate must be an assembled result or a single page that covers its complete source body.
For a bound Attempt it must belong to that binding/generation. Its bytes are verified off the SQLite
thread before the separate immutable submission document is published. This is a proposed result,
**not a Git checkout snapshot, CheckRunner pass or semantic acceptance**.

Use [config/submission.example.json](config/submission.example.json), replacing the sample IDs:

```powershell
swarm --data-dir C:\SwarmState --request-id submit-1 task submit --file submission.json
# Read the returned operation_id with operation.get; result contains submission_ref.
swarm --data-dir C:\SwarmState task submission SUBMISSION_REF --limit 50
swarm --data-dir C:\SwarmState artifact export SUBMISSION_REF --out .\submission.json
```

`expected_submission_ref` must be explicit: null for the first submission, the previous reference
for a replacement. Revision/owner/reference are checked again at final commit after file I/O.
Concurrent proposals cannot overwrite each other. Submission does not release Task ownership or
require unrelated children to finish; native turn completion does not erase a submitted/review state.
A host restart permits retrying the identical local publication request, not replaying model input.
Read the operation outcome even if the initial durable receipt says queued.

Claims name existing requirement IDs. Omitted requirements become **unreported**, using the frozen
Task specification, rather than silently disappearing. `met` needs an evidence reference; `not_met`
and `deferred` need an explanation. These strings are submitter assertions, not controller-verified
symbols. A partially reported proposal can be retained and reviewed without manufacturing a PASS.
`task.submission` pages the immutable claims; `artifact.read/export` verifies the actual document.

As the operator/decision owner, submit [config/request-changes.example.json](config/request-changes.example.json):

```powershell
swarm --data-dir C:\SwarmState --request-id review-1 task request-changes --file review.json
# As the assigned manager, read message.read using that manager's credential.
```

Feedback names the exact Attempt, Task revision, submission, candidate and a stable finding_id.
An applicable finding changes only that Attempt to needs_correction and atomically enters its owner's
mailbox. Repeating the same finding does not send it again; different content under that identity
conflicts. Stale feedback is preserved as historical evidence and does not alter or notify newer work.
The manager can reply using message.send/in_reply_to with the feedback's message_id, then resubmit
against the previous submission reference. No second task.dispatch, process or automatic native wake
is involved. Acceptance/invalidation and independent CheckRunner policy remain separate unfinished work.

## API and remaining work

Public methods: `host.status/mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch/submit/submission/request_changes`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile/result`, `artifact.get/read/assemble/parts`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Module methods remain `module.hello/next/outcome/observe/result`. Export is a CLI client operation over get/read, not a remote arbitrary-file-write method.

Next: implement the remaining Muse crash-recovery boundary and Task result/CheckRunner consumers; then direct OpenCode V2 on the same contract. Do not rebuild the core or reimplement result paging. Preserve the native shared-Codex and subscription targets. [SIWC notes](docs/runtime-notes.md) describe an optional OAuth route, not installed authorization. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) remains after the main controller code.

## Evidence and development

The submission/feedback slice implements architecture §5/12 and implementation plan §4/9 using the
existing artifact and Operation paths. No migration, dependency, native module or service was added.
Its exact-commit compilation is recorded in CI; the previous result-assembly run below is not its
validation. No native session or model is started by this slice.

**Previous code checkpoint: `1953ff5a5ebbe72cfdc0e605504c7f65439d0a7e`.** Assembly/export was implemented in `3631a646`; `1953ff5a` additionally checks the backing file of an empty export. On 2026-09-30, [CI run 36771097804](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36771097804) completed successfully on Windows and Linux: formatting, warnings-denied Clippy, Muse syntax/SDK import and release build. The dependency locks, native module and nine-table migration were unchanged.

A short local invocation of that Linux binary sent **synthetic pages through the real authenticated module IPC**, then used actual artifact assembly/export. A three-page 153,602-byte body exported byte-for-byte with the expected SHA-256. Repeating the request returned the saved receipt; an existing output was not overwritten. An empty result exported successfully, and a subsequent export after removing only its synthetic backing file failed without publishing a destination. The isolated host exited cleanly. No Muse process, native model, user project, Windows runtime or broad test suite was involved; this does not qualify native inference or crash recovery.

The read-only workflow pins Rust 1.98.1/Cargo.lock and runs formatting, warnings-denied Clippy, release builds and Muse syntax/SDK import checks. It does not run `cargo test`, vendor sessions, login or global installation. Artifacts include exact source SHA/source archive. Windows compilation is not qualification on the owner's machine.

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transactional boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Capabilities, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not implementation evidence |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Source candidates, not installed runtimes |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand reference, not extra worker instructions |

This README records current readiness. Work only in main, without worktrees. Code useful paths first, then focused formatting/Clippy; broad tests follow working slices. The nine-table migration and dependency locks are unchanged by result assembly. Foreign/draft/newer databases and missing credentials are not silently replaced. Preserve a cleanly stopped state directory in full, not a live `.db` without WAL. Historical briefs remain in Git history for provenance, not present install defaults.
