# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, external broker or replacement model loop.

## Current implementation — 0.1.0

The Rust core provides authenticated clients, tasks/revisions/claims, durable request receipts, directed mailbox, incremental reports, binding-scoped module admission and immutable result storage. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

The Muse SDK bridge opens an explicitly selected native executable, delivers Task snapshots with per-turn effort, handles exact-turn steer/questions/goal/configuration, and reports observed children and run identities. A live bridge reconnects without closing Muse or replaying model input. Bridge.4 reads pinned native result pages without consuming `subagent/readResult`.

**Local whole-result assembly and export are implemented.** `artifact.assemble` validates a complete ordered set of retained pages, publishes a whole-result file and records provenance. `artifact.parts` pages that provenance; `artifact.read` verifies touched segments. `swarm artifact export` reuses one authenticated IPC connection, streams bytes to an explicitly chosen local file and checks the full SHA-256 before publication. It never sends the destination path to a model or the host.

**Still pending:** bridge-process crash/resume, complete native family reconstruction, automatic handoff, active check cancellation/orphan recovery, reverse-dependency scope/cache reuse, direct OpenCode V2 and other native adapters, MCP and automatic module/service installation. Whole-result coverage is not Task acceptance. Live Muse/Max inference and Windows native launch remain unqualified. Do not mark all C01–C03 complete.

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

## Fixed-source CheckRunner

**C06 now executes configured commands and retains machine evidence.** The full path is
`source.capture → task.submit/check.run → task.accept`. Source capture reads exact Git tree/blob
objects and materializes normal files under the controller state directory. It does not switch main,
stage work, create worktrees, run repository hooks or include uncommitted edits. Symlinks, gitlinks,
LFS pointers and unsafe/case-colliding paths are reported as unsupported, not silently omitted.

Use [the CheckRunner guide](docs/check-runner.md), [trusted check configuration](config/checks.example.toml),
[source selector](config/source-capture.example.json), [check request](config/check-run.example.json)
and [checks-required Task](config/task-checked.example.json). Profiles are explicit local configuration,
not arbitrary command lines supplied by a worker. Normal configuration keeps check execution disabled.

```powershell
swarm --request-id capture-1 source capture --file capture.json
# Read the Operation result.candidate_ref; submit/check this same candidate.
swarm --request-id check-1 check run --file check.json
swarm check get CHECK_ID
swarm artifact get RESULT_REF
swarm artifact export OUTPUT_REF --out .\check-stdout.txt
```

Use normal config/data-dir/credential arguments. Check output reaches its owner's durable mailbox;
large stdout/stderr stay in range-readable artifacts. Cargo profiles require the declared target names,
valid build-finished evidence and no parsing/coverage gaps. Exit 0 alone, or a changed source directory,
does not produce a pass. Semantic review and final Task acceptance remain separate.

One transient process of the same `swarm` binary owns a check Job/process group, not a new permanent
service. The host commits its resource claim and worker identity before allowing command execution.
Disconnect/restart of the host does not close an admitted worker; the next host collects its retained
completion. Repeated active requests coalesce within the same Attempt/candidate/profile. Unknown
worker/launch outcomes retain only their resource; they do not authorize a second writer there.
Queued cancellation is available; active cancellation and automatic orphan disposition are unfinished.

## Task submission and anchored feedback

`task.submit` now seals a complete retained result together with the Attempt's requirement report.
The candidate can be an exact source snapshot of this Attempt/revision, an assembled native result,
or a single native page that covers its complete source body. Native results of a bound Attempt must
belong to that binding/generation. The retained document bytes are verified off the SQLite thread
before a separate immutable submission is published. Submission alone is not a machine pass or semantic
acceptance; only the source-snapshot path can be used by CheckRunner.

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
is involved. Decision recording and exact invalidation are described below; CheckRunner does not bypass them.

## Reviewed acceptance and exact invalidation

`task.accept` records an operator decision about the **exact sealed proposal**; machine-check
evidence is recorded separately from semantic review. The operator must differ from both the owner
and submitter. Same-user client identities prevent accidental self-approval; they do not prove that
a different model or human performed the review.

The Task author explicitly selects `acceptance.required_check_profiles`. An empty list selects
review-only acceptance ([example Task](config/task-review.example.json)); nonempty entries name
`profile_id` and `profile_revision` that must be covered by actual completed CheckRuns. The worker
cannot supply a boolean `passed` in their place. The configured CheckRunner produces these records
for exact source candidates; native prose is not a substitute for a checked checkout. Missing policy
on an older Task is not silently treated as empty: revise the Task explicitly before assigning its next
Attempt. Writing and submission do not require an acceptance policy.

Review-only accepts the retained result and its reviewed requirements. It is **not** a verified
Git checkout, a machine-verified semantic verdict, publication, or a reason to mark a GitHub Issue
code-complete. Review-only decisions record `evidence_level=operator_review` and
`source_checkout_verified=false`. Decisions using actual required passes record
`operator_review_with_checks` and source verification, without claiming semantic correctness beyond
the reviewed requirements. Do not choose review-only when the campaign requires executed build evidence.

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

Public methods: `source.capture`, `check.run/get/profiles/cancel`, `host.status/mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch/submit/submission/request_changes/accept/acceptance/invalidate_acceptance`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile/result`, `artifact.get/read/assemble/parts`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Module methods remain `module.hello/next/outcome/observe/result`. Export is a CLI client operation over get/read, not a remote arbitrary-file-write method.

Next: finish the remaining check-worker recovery/cancellation and native Muse crash-recovery
boundaries, then direct OpenCode V2 on the same contract. Exact-source capture, check execution,
result retention and acceptance are already implemented; do not reimplement them or reopen the
platform selection. Preserve the native shared-Codex/subscription targets. [SIWC notes](docs/runtime-notes.md)
describe an optional OAuth route, not installed authorization. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1)
remains after the main controller code.

## Evidence and development

**Current code checkpoint: `c56f50a408b622bb6b2b9449af4003c560fcd315`.** C06 code was published in
`cfa436f8`; a one-line Clippy finding on a byte separator was corrected without warning suppression.
On **2026-10-01**, [CI run 36828917649](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36828917649)
passed formatting, warnings-denied Clippy, Muse syntax/SDK import and release builds on **Windows and Linux**
for that exact corrected commit. The first failing run is not the validation result. The nine-table
migration and native modules are unchanged. libc was already locked; it is now also a direct Linux
dependency for process-group setup. Existing package versions were not updated.

Linux artifact `11146446370` passed ZIP/SHA-256 verification
(`f43a04eb7709379aa7b5bc44d6ce13c9f965ecd34b0a464158d3d78b599a81e7`); all 101 archived source files
matched the uploaded tree `334f0f3a2ba12b4f3c7734f5d2c408009297cd33`.
A bounded invocation of the compiled host/CLI used a self-owned local Git repository and **real configured
Python commands**, not a mocked CheckRun or a native model. It confirmed exact committed-source capture
while leaving different dirty checkout bytes untouched; command output retention and request replay;
nonzero-exit failure; exit-zero source modification producing incomplete; waiting for a live child after
its parent exited; the same worker surviving host restart; queued cancellation while admission was disabled;
and acceptance consuming an actual completed check. Owner mailbox delivery and clean host exit succeeded.
This is a command-executor invocation, not Cargo runtime or Windows Job qualification. The Cargo parser and
Windows implementation compiled; their full runtime behavior, orphan recovery, cache and load remain pending.
No native SDK/model, user repository or broad test suite was exercised.

Previous acceptance code `01212a4e` passed [CI 36816310704](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36816310704).
That earlier synthetic invocation is historical evidence, not the C06 execution proof. Resume from the
present CheckRunner code, not the older documentation-only recovery checkpoint or acceptance patch.

The read-only workflow pins Rust 1.98.1/Cargo.lock and runs formatting, warnings-denied Clippy, release builds and Muse syntax/SDK import checks. It does not run `cargo test`, vendor sessions, login or global installation. Artifacts include exact source SHA/source archive. Windows compilation is not qualification on the owner's machine.

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transactional boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Capabilities, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not implementation evidence |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Source candidates, not installed runtimes |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand reference, not extra worker instructions |

This README records current readiness. Work only in main, without worktrees. Code useful paths first, then focused formatting/Clippy; broad tests follow working slices. The nine-table migration is unchanged; the exact dependency lock is committed. Foreign/draft/newer databases and missing credentials are not silently replaced. Preserve a cleanly stopped state directory in full, not a live `.db` without WAL. Historical briefs remain in Git history for provenance, not present install defaults.
