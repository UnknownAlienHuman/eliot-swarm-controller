# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, external broker or replacement model loop.

## Current implementation — 0.1.0

The Rust core provides authenticated clients, tasks/revisions/claims, durable request receipts, directed mailbox, incremental reports, binding-scoped module admission and immutable result storage. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

The Muse SDK bridge opens an explicitly selected native executable, delivers Task snapshots with per-turn effort, handles exact-turn steer/questions/goal/configuration, and reports observed children and run identities. A live bridge reconnects without closing Muse or replaying model input. Bridge.4 reads pinned native result pages without consuming `subagent/readResult`.

**Local whole-result assembly and export are implemented.** `artifact.assemble` validates a complete ordered set of retained pages, publishes a whole-result file and records provenance. `artifact.parts` pages that provenance; `artifact.read` verifies touched segments. `swarm artifact export` reuses one authenticated IPC connection, streams bytes to an explicitly chosen local file and checks the full SHA-256 before publication. It never sends the destination path to a model or the host.

**Still pending:** bridge-process crash/resume, complete native family reconstruction, automatic handoff, CheckRunner, direct OpenCode V2 and other native adapters, MCP and automatic module/service installation. Whole-result coverage is not Task acceptance. Live Muse/Max inference and Windows native launch remain unqualified. Do not mark all C01–C03 complete.

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
is involved. Decision recording and exact invalidation are described below; CheckRunner execution remains unfinished.

## Reviewed acceptance and exact invalidation

`task.accept` records an operator decision about the **exact sealed proposal**, not a claim that
the controller has compiled the repository. The operator must differ from both the owner and
submitter. Same-user client identities prevent accidental self-approval; they do not prove that
a different model or human performed the review.

The Task author explicitly selects `acceptance.required_check_profiles`. An empty list selects
review-only acceptance ([example Task](config/task-review.example.json)); nonempty entries name
`profile_id` and `profile_revision` that must be covered by actual completed CheckRuns. The worker
cannot supply a boolean `passed` in their place. The present product has no CheckRunner producer,
so checks-required policies cannot pass until that path exists. Missing policy on an older Task
is not silently treated as an empty policy: revise the Task explicitly before assigning its next
Attempt. Writing and submission do not require an acceptance policy.

Review-only accepts the retained result and its reviewed requirements. It is **not** a verified
Git checkout, a machine-verified semantic verdict, publication, or a reason to mark a GitHub Issue
code-complete. Decisions record `evidence_level=operator_review` and `source_checkout_verified=false`.
Do not choose review-only when the campaign requires controller-executed build evidence.

```powershell
# Read the exact submission; retain latest_feedback_observation_id.
swarm task submission SUBMISSION_REF
# As a separate decision owner, fill config/acceptance.example.json with real evidence.
swarm --request-id accept-1 task accept --file acceptance.json
# Inspect operation.get; on applied, read its acceptance_operation_id:
swarm task acceptance ACCEPTANCE_OPERATION_ID
# Revoke that decision, not "whichever decision currently has this candidate":
swarm --request-id revoke-1 task invalidate-acceptance --file invalidate.json
```

Use normal data-dir/config/credential arguments. Review must cover exactly the frozen requirement
IDs with rationale and evidence, independently of the writer's `met/unreported` list. Its expected
feedback cursor comes from `task.submission`; new applicable feedback during byte verification
makes the decision proposal stale, without discarding the worker's candidate. Missing/corrupt
submission, candidate or required-check artifact prevents acceptance. File reads stay off the DB
thread; final state, authority, feedback and pinned dependency decisions are checked again inside
the final transaction.

Acceptance retains producer ownership. `attempt.release` permits `outcome=accepted` only after a
recorded acceptance and still checks assigned native runs and outstanding effects/resources.
Acceptance is not cancellation. Invalidating the current decision reopens the Task and returns
one feedback message to its still-owning manager. Invalidating an older decision retains the
historical revocation without clearing a newer decision, even when both accepted the same bytes.
Replies use the existing mailbox; no new prompt, writer, process or Issue mutation is performed.

Dependency lookup now uses valid historical acceptance decisions, rather than only today's Task
pointer. A newer producer revision does not revoke a pinned earlier decision. Explicit revocation
is rechecked when a consumer is accepted. No cascade deletes already accepted consumer results.

## API and remaining work

Public methods: `host.status/mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch/submit/submission/request_changes/accept/acceptance/invalidate_acceptance`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile/result`, `artifact.get/read/assemble/parts`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Module methods remain `module.hello/next/outcome/observe/result`. Export is a CLI client operation over get/read, not a remote arbitrary-file-write method.

Next code slice: C06 CheckRunner, using the existing `check_runs`, Operation, artifact and acceptance contracts (implementation plan §10). Implement actual command execution and Cargo result handling with exact inputs, explicit process/resource disposition and retained output; do not manufacture a passed row or treat a worker result as a verified checkout. Then finish Muse bridge-process recovery and direct OpenCode V2 on the same contract. Do not rebuild the core or reimplement result paging. Preserve the native shared-Codex and subscription targets. [SIWC notes](docs/runtime-notes.md) describe an optional OAuth route, not installed authorization. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) remains after the main controller code.

## Evidence and development

**Recovery checked 2026-10-01:** the interrupted continuation did not advance `main` beyond
`fe161da1ce5edd3514520e7ff6d6ff9cd5ea8978`; the latest build remains `01212a4e` below.
Re-read both successful CI jobs and checked the mounted source/binary archives. No additional
CheckRunner implementation was found in the current workspace or retained archives. The Linux
artifact passed SHA-256/ZIP verification again; all 90 source files reconstructed the exact
`158a147fa968149eb6c12764ed050e0682151e9b` tree. The old acceptance patch predates the already
published fixes: do not reapply it, roll back, or restart SDK preparation. Resume with CheckRunner
from the present code. This recovery changes documentation only; it does not rerun compilation,
model calls or the previous synthetic invocation, and does not claim access to lost ephemeral files.

**Current code checkpoint: `01212a4e2a646693a4d8dd44443779e670f6c555`.** The retained
acceptance implementation was published on the exact `0c34b5e` main base in `bdc4bc17`.
Two compilation defects in the previously uncompiled patch were corrected: sibling Store access
to the sealed submission reader, and comparing a check receipt ID with `&str` rather than `&String`.
On **2026-10-01**, [CI run 36816310704](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36816310704)
passed formatting, warnings-denied Clippy, Muse syntax/SDK import and release builds on **Windows
and Linux** for the exact corrected commit. The initial failing run is not the validation result.
Migrations, dependency locks and native modules remain unchanged.

The downloaded Linux artifact `11141153858` passed ZIP and SHA-256 validation
(`706c046e0c82ad6f8ea8ecf67af4a58311d37932ced7379bed578900fc2efda3`); its archived source
reconstructed the exact Git tree `158a147fa968149eb6c12764ed050e0682151e9b`.
A bounded invocation used the real compiled host/CLI and **synthetic authenticated module data**.
It exercised acceptance/replay without implicit release; rejection of writer self-acceptance;
one-time revocation feedback and reply; reacceptance of the same bytes with a new decision ID;
stale revocation preserving that new decision; rejection of absent policy or missing required
checks; historical dependency resolution followed by explicit revocation; and refusal to accept
corrupted synthetic backing bytes. The isolated host exited with code 0. No native SDK/model,
user repository, Windows runtime or broad test suite was exercised. Positive CheckRunner execution
and native crash recovery remain unqualified, not inferred from these local decision checks.

Previous slices: submission/feedback `8e19f1df` passed
[CI 36775844956](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36775844956);
assembly/export `1953ff5a` passed
[CI 36771097804](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36771097804).
Their earlier synthetic invocations are historical evidence, not substitutes for the current build.
Resume from the published acceptance code; the former downloadable patch is no longer pending.

The read-only workflow pins Rust 1.98.1/Cargo.lock and runs formatting, warnings-denied Clippy, release builds and Muse syntax/SDK import checks. It does not run `cargo test`, vendor sessions, login or global installation. Artifacts include exact source SHA/source archive. Windows compilation is not qualification on the owner's machine.

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transactional boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Capabilities, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not implementation evidence |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Source candidates, not installed runtimes |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand reference, not extra worker instructions |

This README records current readiness. Work only in main, without worktrees. Code useful paths first, then focused formatting/Clippy; broad tests follow working slices. The nine-table migration and dependency locks are unchanged by this acceptance slice. Foreign/draft/newer databases and missing credentials are not silently replaced. Preserve a cleanly stopped state directory in full, not a live `.db` without WAL. Historical briefs remain in Git history for provenance, not present install defaults.
