# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, external broker or replacement model loop.

## Current implementation — 0.1.0

The Rust core provides authenticated clients, task revisions/ownership, durable request receipts, directed mailbox, incremental reports, binding-scoped module admission, immutable artifacts, submission/review and acceptance. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

The Muse SDK bridge opens an explicitly selected native executable, delivers Task snapshots with per-turn effort, handles exact-turn steer/questions/goal/configuration, and reports observed children and run identities. A live bridge reconnects without closing Muse or replaying model input. Bridge.5 retains a recovery checkpoint and supports explicit recovery of a recorded native session through the non-killing `module-run` owner. Pinned result reads do not consume `subagent/readResult`; local whole-result assembly and verified export are implemented.

**CheckRunner executes configured commands on captured Git sources. Active cancellation and recovery of a recorded departed check worker are now implemented.** Cancellation does not stop the host or native agents. Recovery uses the worker lock and OS group identity; it does not infer success or replay a command.

**Still pending:** live Muse resume qualification, missing-identity/native-outcome recovery gaps, complete native family reconstruction, automatic handoff, unresolved pre-identity check launches, reverse-dependency scope/cache reuse, OpenCode cross-restart execution continuation, configure-to-input prerequisite chaining and goal/model/agent controls, other native adapters, MCP and automatic module/service installation. Live Muse/Max inference and Windows native launch remain unqualified. Do not mark all C01–C03 complete.

## Build and run

```powershell
cargo build --locked --release --bin swarm
.\target\release\swarm.exe --data-dir C:\SwarmState host
```

Use an initially empty dedicated local directory. The host owns only its marker/lock, database, artifacts and credentials there. It refuses unrelated nonempty directories; global PATH, UAC and vendor settings are untouched. Read-only CLI calls do not initialize a database or launch the host.

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

After a lost reply, repeat the identical method/payload and request ID. Different content under that ID is rejected. New IDs do not bypass origin/ownership/initial-start uniqueness. Request IDs go to stderr, JSON results to stdout. Keep secrets out of persisted task text and mailbox messages.

`swarm call METHOD --file params.json` invokes the implemented API. `--config config/controller.example.toml` uses the implementation configuration, not the broader target examples under docs/. Commands below also require the appropriate data-dir/config/credential options.

## Direct OpenCode V2 in Rust

The built-in `eliot-opencode-v2.http.1` adapter attaches to an explicitly configured **existing** local HTTP service. It creates scoped sessions, delivers frozen Tasks/next-turn inputs, reads paginated family and pending requests, sends addressed answers, applies controller-owned durable `eliot.*` instruction entries with exact readback, and reconciles lost create/prompt/configuration responses by GET only. One service namespace has one pooled client and volatile SSE reader. No OpenCode CLI, process restart, model substitution or duplicate mutation is used.

**Inbox admission is not native turn completion.** Read-only `agent.result` exports exact completed assistant messages, input-ID-anchored projected intervals, isolated native patches and exact file items from completed tool results into immutable, digest-checked artifacts. Tool-file selectors identify a native message/call/content index; they cannot inject a path or URL. Inline data URIs are decoded exactly, while location-confined `file:` descriptors are read through OpenCode's native `fs.read` route and reread before publication. A separate durable session-log reader maps exact inbox IDs to native execution-start event IDs and terminal disposition, including coalesced inputs and cancellation before delivery. Its checkpointed GET reads recover without input replay; shutdown/uncertain continuation remains unresolved. This is root-execution evidence, not whole-family completion or Task acceptance. Goal/model/agent controls and live OpenCode qualification remain pending; durable controller-owned instruction entries are implemented with exact readback. Unknown children/stream gaps never become an idle family or a passed Task. Follow [the module guide](modules/opencode/README.md) and the disabled route in [configuration](config/controller.example.toml).

The complete pinned `atlas-redact` donor is now used to scrub retained native question/diagnostic copies. Its upstream files remain unchanged behind a separate Cargo wrapper; licenses, rule notices and snapshot hashes are retained in [third-party notices](THIRD_PARTY_NOTICES.md). This is actual library reuse, not a claim that all listed donors were installed.

**Why JavaScript exists:** only the Muse adapter needs the official Node SDK. Its first runnable bridge is commit `b2bd0211` (2026-09-30); controller authority and the new OpenCode adapter are Rust. See [the exact provenance](docs/javascript-provenance.md). No Muse files were deleted or language statistics hidden.

## Native Muse, clients and task-specific children

Follow [modules/muse/README.md](modules/muse/README.md). Enable a private route, reserve `agent.open`, register its scoped module credential, install the locked module-local SDK and independently launch the bridge using the installed native executable. The shipped route is disabled; its artifact version is **`muse-sdk-1.3.0-bridge.5`**. Local check changes do not require replacing a running bridge.

Native subscription/auth and effort remain in the harness. Requested effort, effective setting and observed inference are different evidence. No Go/API route silently substitutes for Muse Code Max. Full conversation history stays native.

`agent.send` supports next-turn input or exact-turn steer. `agent.reply` submits native decisions with current IDs; the bridge's immediate MSP `{}` response acknowledges presentation only. Configure/goal/refresh/reconcile keep their distinct application boundaries. Refresh is not resume; a timeout does not authorize another prompt.

```powershell
swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

`task claim --binding-id BINDING --generation 1` defaults to `native_manager`: native delegation does not receive a duplicate controller start. `swarm family BINDING --generation 1` returns a retained observation; later pages reuse its observation_id. `swarm task bind ATTEMPT --assignment NAME --session CHILD --turn TURN --observation-id ID` maps an already observed run. An old completion does not close a new assignment; parent idle does not erase children. Family coverage remains partial.

Mailbox readers have independent cursors. Module credentials are binding-scoped and cannot accept Tasks or become GM. Same-user roles coordinate trusted clients; they are not an OS sandbox.

### Recorded Muse recovery

For new managed launches, use `swarm module-run --state-dir MODULE_STATE --command ABSOLUTE_NODE_PATH -- BRIDGE_SCRIPT --config MODULE_CONFIG`; paths/argv stay separate. The module directory is distinct from the host directory. The owner holds its OS lock and waits for both the bridge and remaining native processes; losing the owner does not authorize replacing a still-live group.

After the recorded old group has ended, a replacement bridge restores its checkpoint and the host marks that binding `reconciling`. Explicit operator `agent.recover` targets its current `expected_boot_id`. The bridge resumes the same known native session, not a fresh session/fork or replayed Task prompt. A historical open receipt can restore identity but not new-boot readiness. Only the correlated current-boot resume outcome makes the binding ready. Refer to [the module guide](modules/muse/README.md#recorded-session-recovery--bridge5) for the JSON request and failure boundaries.

Existing unguarded bridges are not retroactively qualified. Unknown/corrupt checkpoint or process identity remains an explicit recovery gap; no force-reset or blanket retry is added. Live vendor resume/children/Max have not been exercised by the local process-owner invocation below.


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

## Fixed-source CheckRunner

`source.capture → task.submit/check.run → task.accept`

Capture reads exact Git tree/blob objects and materializes normal files under the controller state directory. It does not switch main, stage work, create worktrees, run repository hooks or include uncommitted edits. Symlinks, gitlinks, LFS pointers and unsafe/case-colliding paths are explicitly unsupported, not silently omitted.

Use [the CheckRunner guide](docs/check-runner.md), [trusted check configuration](config/checks.example.toml), [source selector](config/source-capture.example.json), [check request](config/check-run.example.json) and [checks-required Task](config/task-checked.example.json). Profiles are local configuration, not arbitrary worker-supplied commands. Normal configuration keeps execution disabled.

```powershell
swarm --request-id capture-1 source capture --file capture.json
# Read Operation result.candidate_ref; submit/check that same candidate.
swarm --request-id check-1 check run --file check.json
swarm check get CHECK_ID
swarm artifact get RESULT_REF
swarm artifact export OUTPUT_REF --out .\check-stdout.txt
```

Output reaches the owner's mailbox; full stdout/stderr stay in range-readable artifacts. Cargo profiles require declared targets, valid build-finished evidence and no parsing/coverage gaps. Exit zero or changed sources cannot produce a pass. Semantic review and final acceptance remain separate.

A transient process of the same `swarm` binary owns each check Job/process group. Resource claim and worker identity are committed before tool execution is allowed. Host disconnect/restart does not close admitted workers; the next host collects retained completion. Active identical requests coalesce within an Attempt/candidate/profile. An uncertain outcome retains its resource, not the whole controller.

### Active cancellation and recovery

```powershell
swarm --request-id cancel-1 check cancel CHECK_ID --reason 'Superseded verification'
swarm check get CHECK_ID
```

The reply confirms a **durable request**, not process termination. `check.get` separates `cancel_request` from terminal `cancellation` evidence. Queued work is cancelled without execution even while new-work admission is disabled. New control-version-2 workers receive token/CheckRun-addressed cancellation before go-ahead when both are pending, before tool spawn, while the parent runs and while descendants remain.

Only this check's tool processes are targeted; the reporting worker, host, other checks and native-agent families are not. This is explicit termination, not graceful application shutdown or an age/CPU-triggered policy. Source materialization and artifact sealing remain finite noninterruptible local operations. The target stays held until the owned group is actually empty. A late request does not rewrite a naturally completed verdict. Older already running workers report unsupported cancellation rather than being replaced in place.

After a worker crash, recovery acquires its released lock, rechecks receipts and verifies exact OS group disposition. Live descendants or denied/unknown inspection keep ownership. A prepared terminal receipt is validated and restored with the same artifacts. Without it, retained output becomes **incomplete**, with unknown exit/coverage, never a guessed pass. Recovery neither replays the command nor kills orphaned processes.

Windows uses a uniquely named Global Job and query-only recovery; cancellation validates membership through pinned process handles. Linux uses boot/birth/group identities and pidfds, including whole-process termination when the main thread exits first. These are trusted execution boundaries, not sandboxes against deliberate process-group escape. Missing pre-identity launch evidence, legacy unnamed Windows Jobs and damaged records remain explicit gaps. Platform details and limits are in the CheckRunner guide.

## Task submission and anchored feedback

`task.submit` seals a candidate and requirement report. The candidate can be an exact source snapshot of this Attempt/revision, an assembled native result, or one native page covering its complete source. Bound native results must belong to that binding/generation. Bytes are checked off the DB thread before immutable submission publication. Only source snapshots can be machine-checked by CheckRunner.

```powershell
# Fill config/submission.example.json with actual IDs and claims:
swarm --request-id submit-1 task submit --file submission.json
# Read the operation result.submission_ref:
swarm task submission SUBMISSION_REF --limit 50
swarm artifact export SUBMISSION_REF --out .\submission.json
# Separate decision owner; config/request-changes.example.json:
swarm --request-id review-1 task request-changes --file review.json
```

`expected_submission_ref` is explicit: null for the first submission, the prior reference for replacement. Owner/revision/reference are checked again after file I/O. Concurrent proposals cannot overwrite one another. Submission does not release the producer or wait for unrelated children; late native completion cannot erase submitted/review state. An identical local publication request can continue after restart, not replay model input.

Claims name frozen requirement IDs. Omitted requirements become **unreported**; `met` needs evidence, `not_met/deferred` need reasons. Evidence strings remain writer assertions, not proof of symbol existence/correctness. A useful incomplete proposal is retainable without acceptance.

Feedback names exact Attempt/revision/submission/candidate and a stable finding_id. Applicable feedback sets needs_correction and enters the owner's mailbox atomically. Repeating the same finding does not send it twice; conflicting content under that identity fails. Stale review remains historical and does not alter or notify newer work. The manager can reply with message.send/in_reply_to and resubmit in the same Attempt. No new dispatch, writer or automatic native wake is created.

## Reviewed acceptance and exact invalidation

`task.accept` records a decision about the **exact sealed proposal**. The operator differs from both owner and submitter; client identities prevent accidental self-approval but do not prove that another model/person reviewed the work.

The Task author selects `acceptance.required_check_profiles`. An explicit empty list selects [review-only](config/task-review.example.json); nonempty profile_id/profile_revision entries require actual completed CheckRuns. A worker-supplied `passed:true` cannot replace them. Missing policy on older Tasks is not silently treated as empty; revise before assigning a new Attempt. Writing/submission do not require an acceptance policy.

Review-only records `evidence_level=operator_review`, `source_checkout_verified=false`; it is not a checked Git tree, publication or Issue closure. Required machine passes produce `operator_review_with_checks`, without independently proving semantic correctness. Do not choose review-only when the campaign requires executed builds.

```powershell
swarm task submission SUBMISSION_REF
# Retain latest_feedback_observation_id; fill config/acceptance.example.json:
swarm --request-id accept-1 task accept --file acceptance.json
# After applied, read result.acceptance_operation_id:
swarm task acceptance ACCEPTANCE_OPERATION_ID
# Fill config/invalidate-acceptance.example.json with the exact decision:
swarm --request-id revoke-1 task invalidate-acceptance --file invalidate.json
```

Review covers all frozen requirement IDs with rationale/evidence, independently of the writer's list. New feedback during byte verification makes a decision proposal stale without discarding the candidate. Missing/corrupt candidate, submission or required-check artifacts prevent acceptance. Final authority, ownership, revision, feedback and dependency decisions are rechecked in the transaction.

Acceptance preserves producer ownership. `attempt.release` with accepted requires recorded acceptance and known disposition of assigned native runs, effects and check resources. Invalidation names an **acceptance_operation_id**, not whichever decision now refers to the same SHA. A current decision's invalidation reopens the Task and sends one feedback to its current owner; an old invalidation cannot erase a newer decision. No writer/process/Issue mutation is automatically started.

Dependency lookup uses valid historical acceptance decisions. A newer producer revision does not revoke an earlier pinned decision; explicit revocation is checked again at consumer acceptance. No cascade deletes accepted consumer results.

## API and next code

Public methods: `source.capture`, `check.run/get/profiles/cancel`, `host.status/mode`, `client.register/list`, `task.create/get/list/revise/claim/dispatch/submit/submission/request_changes/accept/acceptance/invalidate_acceptance`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile/recover/result`, `artifact.get/read/assemble/parts`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`. Module methods: `module.hello/next/outcome/observe/result`. Export is a client operation, not a remote arbitrary-file-write method.

**Next: OpenCode configure-to-input prerequisite chaining and goal/model/agent controls on the implemented HTTP path, then cross-restart execution continuation and complete family reconstruction.** Exact detached tool-file retrieval is implemented through native descriptors and the existing artifact path; do not replace it with arbitrary URL/filesystem reads. Ordinary input/execution disposition now uses the native durable log; continuation without a proven terminal remains unresolved, not inferred from idle. Preserve these implemented paths instead of rebuilding them. Do not repeat session/inbox/readback/SSE implementation solely because an older checkpoint called all of C04 pending. Recorded-session Muse recovery and recorded check-worker recovery are implemented; missing identity, unresolved native outcomes and actual vendor/Windows runtime qualification remain explicit. Do not repeat the recorded-recovery implementation solely because an older checkpoint called it pending. Do not reimplement source capture, results, submission, acceptance or CheckRunner. Preserve the native shared-Codex/subscription targets. [SIWC notes](docs/runtime-notes.md) describe an optional OAuth route, not installed auth. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) remains after the main code.

## Evidence and development

**OpenCode source recovery, 2026-10-01 (New York):** the interrupted implementation's 49 staged source files were recovered from tree `f54b9259dbc052c942e089aea95a130166916a01` and published in `b1ff71ff`. Fresh package formatting and minimal warnings-denied Clippy passed on Rust 1.98.1. The retained 16 protocol/Store fixtures were not rerun during recovery; their earlier reported result is not new qualification evidence. Normal CI checks formatting, Clippy, the unchanged Atlas snapshot, Muse syntax/SDK import and release builds on Windows/Linux. Consult the exact commit's run before claiming a platform pass. No tests, native model calls or subscription qualification are run by this recovery workflow.


**Previous Muse recovery baseline: `d385498b00fe1a049c357613a7777cc4a1c83f72`.** The interrupted continuation saved the module owner, checkpoint and controlled session-recovery implementation through `8a84e6be`; it was not lost. Recovery reconstructed its exact 104-file source tree from the CI artifact. A bounded invocation then found an actual runtime defect: changed-boot admission used nonexistent `operations.generation` instead of `binding_generation`. The single-query correction is in `d385498b`; schema, dependencies and native-module source are unchanged by that correction.

On **2026-10-01**, exact [CI run 36872022736](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36872022736) passed formatting, warnings-denied Clippy, Muse syntax/SDK import and release builds on **Windows and Linux**. Earlier green compilation alone did not catch the SQL execution error.

Corrected Linux artifact `11168070937` passed ZIP/SHA-256 verification (`7b1d8422ffbdb858cc984349f3087c63a97eec3f5e6162411ffa865095f2c1d5`); its 104 source files match the fixed tree `d385c392228da6213eb217739c885ceef62d5d2f`.

The exact corrected Linux binary passed 13 directed assertions through the real host and `module-run`: live-owner exclusion, descendant survival after the fixture bridge and launcher were killed, no replacement until that recorded group ended, changed-boot admission, old-link rejection, delayed-open receipt without readiness, input refusal before resume, stale/disabled recovery rejection and correlated current-boot readiness. The real Node checkpoint writer serialized concurrent writes and read the last committed state. Both the remaining managed process and host exited cleanly.

**Native open/resume outcomes in this invocation were synthetic module messages.** It exercised real OS ownership, Store and IPC, not Muse inference or a vendor SDK session. No new test modules, cargo test, native models, user repositories or system settings were involved. Windows Job runtime, actual Muse resume with children/Max, missing-identity windows, Cargo runtime/cache and load remain unqualified. The failed pre-fix invocation and successful corrected one are different evidence, not a renamed old pass.

Historical baselines: C06 `c56f50a4` passed [CI 36828917649](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36828917649) and a real-command invocation of capture, output, failure/source-change rejection, host restart and checked acceptance. Acceptance `01212a4e` passed [CI 36816310704](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36816310704). Those are earlier evidence, not substitutes for this cancellation/recovery run.

The read-only workflow pins Rust 1.98.1/Cargo.lock and checks formatting, Clippy, release builds and Muse syntax/import. Tests remain deferred during code completion; the workflow does not run `cargo test`, vendor sessions, login or global installation. Artifacts include exact source SHA and a source archive.

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transaction boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Capabilities, delivery and lifecycle |
| [CheckRunner](docs/check-runner.md) | Implemented execution/cancellation/recovery details |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not implementation evidence |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Inventory; selected Atlas snapshot is tracked separately, not all installed runtimes |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand evidence, not extra worker instructions |

This README records current readiness. Work only in main, without worktrees. Code useful paths first, then focused formatting/Clippy; broad tests follow working slices. The nine-table migration is unchanged. Foreign/draft/newer databases and missing credentials are not silently replaced. Preserve a cleanly stopped state directory in full, not only a live DB without WAL. Historical briefs remain in Git history for provenance, not present install defaults.
