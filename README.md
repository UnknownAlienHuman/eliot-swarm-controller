# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for Eliot Memory OS's Agent Execution Fabric. One host, one SQLite database, local IPC. No UI, external broker or replacement model loop.

## Current implementation — 0.1.0

The Rust core provides authenticated clients, task revisions/ownership, durable request receipts, directed mailbox, incremental reports, binding-scoped module admission, immutable artifacts, submission/review and acceptance. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

The Muse SDK bridge opens an explicitly selected native executable, delivers Task snapshots with per-turn effort, handles exact-turn steer/questions/goal/configuration, and reports observed children and run identities. A live bridge reconnects without closing Muse or replaying model input. Bridge.5 retains a recovery checkpoint and supports explicit recovery of a recorded native session through the non-killing `module-run` owner. Pinned result reads do not consume `subagent/readResult`; local whole-result assembly and verified export are implemented.

**CheckRunner executes configured commands on captured Git sources. Active cancellation and recovery of a recorded departed check worker are now implemented.** Cancellation does not stop the host or native agents. Recovery uses the worker lock and OS group identity; it does not infer success or replay a command.

**Still pending:** live Muse resume qualification, native-outcome recovery for unrecorded effects, complete native family reconstruction, automatic handoff, reverse-dependency scope/cache reuse, OpenCode cross-restart execution continuation, the remaining Claude surfaces (attach/resume, configure, goal, recovery) and automatic module/service installation. Live Muse/Max inference and Windows native launch remain unqualified. Do not mark all C01–C03 complete.

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

The built-in `eliot-opencode-v2.http.1` adapter attaches to an explicitly configured **existing** local HTTP service. It creates scoped sessions, delivers frozen Tasks/next-turn inputs, reads paginated family and pending requests, sends addressed answers, applies controller-owned durable `eliot.*` instruction entries, switches to one exact registered session agent, and explicitly restores the route's pinned provider/model/variant with catalog/session readback. Lost create/prompt/configuration responses reconcile by GET only. One service namespace has one pooled client and volatile SSE reader. No OpenCode CLI, process restart, hidden model substitution or duplicate mutation is used.

**Inbox admission is not native turn completion.** Read-only `agent.result` exports exact completed assistant messages, input-ID-anchored projected intervals, isolated native patches and exact file items from completed tool results into immutable, digest-checked artifacts. Tool-file selectors identify a native message/call/content index; they cannot inject a path or URL. Inline data URIs are decoded exactly, while location-confined `file:` descriptors are read through OpenCode's native `fs.read` route and reread before publication. A separate durable session-log reader maps exact inbox IDs to native execution-start event IDs and terminal disposition, including coalesced inputs and cancellation before delivery. Its checkpointed GET reads recover without input replay; shutdown/uncertain continuation remains unresolved. The same anchored log protocol now reads tracked child sessions individually — bound, previously open, or natively active members only — so a proven child terminal discharges exactly that child's producer, and the family snapshot reports per-axis coverage counters. Family completeness still stays `partial`: no native read is an atomic family snapshot. This is per-session execution evidence, not whole-family completion or Task acceptance. Live OpenCode qualification remains pending; goal controls are implemented as a controller-recorded durable instruction entry with one-prompt activation (OpenCode has no native goal API and there is no automatic continuation), and durable controller-owned instruction entries, exact session-agent selection and exact route-model restoration are implemented with readback. Unknown children/stream gaps never become an idle family or a passed Task. Follow [the module guide](modules/opencode/README.md) and the disabled route in [configuration](config/controller.example.toml). Updates and rollback follow [modules/opencode/UPDATE.md](modules/opencode/UPDATE.md).

The complete pinned `atlas-redact` donor is now used to scrub retained native question/diagnostic copies. Its upstream files remain unchanged behind a separate Cargo wrapper; licenses, rule notices and snapshot hashes are retained in [third-party notices](THIRD_PARTY_NOTICES.md). Snapshot updates follow [modules/atlas-redact/UPDATE.md](modules/atlas-redact/UPDATE.md). This is actual library reuse, not a claim that all listed donors were installed.

## Zed eval-cli batch runtime

`src/runtime/zed.rs` implements the C11 Zed boundary as a batch executor unit over the pinned native `eval-cli` contract (ZD-EXEC basis `7604aa3f`): `describe` reports the configured entrypoint, the exact `provider/model`, a bounded timeout and the honestly absent capabilities — no persistent control, resume, goal, steer or session family, and Zed native is not an external ACP editor. One batch run spawns `eval-cli --workdir … --model … --instruction … --timeout … --output-dir …` with only the route-named environment keys passed through; values are never persisted. Exit codes keep their native meanings (0 agent finished, 1 error, 2 timeout, 3 interrupted) and exit 0 is a finished run, not Task acceptance. `result.json` is cross-checked against the exit code and the configured model before anything is believed, and `result.json`/`thread.md`/`thread.json` are published as immutable, digest-checked artifacts, paged without truncation. The installed Zed binary is not live-qualified, and host admission wiring (route validation, supervisor, operation mapping for a sessionless runtime) is the next slice: the runtime is not yet reachable through controller operations.

The complete pinned `atlas-redact` donor is now used to scrub retained native question/diagnostic copies. Its upstream files remain unchanged behind a separate Cargo wrapper; licenses, rule notices and snapshot hashes are retained in [third-party notices](THIRD_PARTY_NOTICES.md). This is actual library reuse, not a claim that all listed donors were installed.
>>>>>>> 1f42fe9 (docs: document Zed eval-cli batch runtime boundary)

**Why JavaScript exists:** the Muse adapter and the Claude adapter each use their vendor's official Node SDK; neither the controller authority nor the OpenCode adapter does. Muse's first runnable bridge is commit `b2bd0211` (2026-09-30). See [the exact provenance](docs/javascript-provenance.md). No Muse files were deleted or language statistics hidden.

## Native Muse, clients and task-specific children

Follow [modules/muse/README.md](modules/muse/README.md). Enable a private route, reserve `agent.open`, register its scoped module credential, install the locked module-local SDK and independently launch the bridge using the installed native executable. The shipped route is disabled; its artifact version is **`muse-sdk-1.3.0-bridge.5`**. Local check changes do not require replacing a running bridge. Version updates and rollback follow [modules/muse/UPDATE.md](modules/muse/UPDATE.md).

Native subscription/auth and effort remain in the harness. Requested effort, effective setting and observed inference are different evidence. No Go/API route silently substitutes for Muse Code Max. Full conversation history stays native.

`agent.send` supports next-turn input or exact-turn steer. `agent.reply` submits native decisions with current IDs; the bridge's immediate MSP `{}` response acknowledges presentation only. Configure/goal/refresh/reconcile keep their distinct application boundaries. Refresh is not resume; a timeout does not authorize another prompt.

```powershell
swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

`task claim --binding-id BINDING --generation 1` defaults to `native_manager`: native delegation does not receive a duplicate controller start. `swarm family BINDING --generation 1` returns a retained observation; later pages reuse its observation_id. `swarm task bind ATTEMPT --assignment NAME --session CHILD --turn TURN --observation-id ID` maps an already observed run. An old completion does not close a new assignment; parent idle does not erase children. Family coverage remains partial.

Mailbox readers have independent cursors. Module credentials are binding-scoped and cannot accept Tasks or become GM. Same-user roles coordinate trusted clients; they are not an OS sandbox.

### GM designation and handover

GM authority is a designation with its own epoch, not another client role. `gm.handover` — callable by the local operator or the current GM — names a registered non-module client, optionally with the binding its GM session runs on, and advances the GM epoch by exactly one. Until the first handover, GM-only authority (acceptance and its invalidation, client registration/list, host mode) rests with the local operator alone; afterwards the current GM holds it alongside the operator, and a former GM loses it at both admission and the later dispatch/begin rechecks. Handover changes only the designation record: manager Attempts keep their owners, admitted native work is neither restarted nor cancelled, and pending decisions and per-reader mailbox cursors survive. `host.status` reports the current designation, its epoch and `gm_wake_mode`. No native push path is qualified for the GM entrypoint, so the wake mode is the explicit `checkpoint_poll`: the GM reads `report.delta`/`message.read` itself and the host runs no hidden model polls. Stale queued-action adopt/cancel across a rotation and mailbox transfer to a successor are not defined by the current contract and are not invented here.

### Recorded Muse recovery

For new managed launches, use `swarm module-run --state-dir MODULE_STATE --command ABSOLUTE_NODE_PATH -- BRIDGE_SCRIPT --config MODULE_CONFIG`; paths/argv stay separate. The module directory is distinct from the host directory. The owner holds its OS lock and waits for both the bridge and remaining native processes; losing the owner does not authorize replacing a still-live group.

After the recorded old group has ended, a replacement bridge restores its checkpoint and the host marks that binding `reconciling`. Explicit operator `agent.recover` targets its current `expected_boot_id`. The bridge resumes the same known native session, not a fresh session/fork or replayed Task prompt. A historical open receipt can restore identity but not new-boot readiness. Only the correlated current-boot resume outcome makes the binding ready. Refer to [the module guide](modules/muse/README.md#recorded-session-recovery--bridge5) for the JSON request and failure boundaries.

Existing unguarded bridges are not retroactively qualified. Unknown/corrupt checkpoint or process identity remains an explicit recovery gap; no force-reset or blanket retry is added. Live vendor resume/children/Max have not been exercised by the local process-owner invocation below.

## Claude Agent SDK bridge — first slice

Follow [modules/claude/README.md](modules/claude/README.md). The Claude adapter is a separate SDK-owned bridge over the pinned `@anthropic-ai/claude-agent-sdk` **0.3.287** (Anthropic Commercial Terms) and its matching bundled native binary; its artifact is **`claude-agent-sdk-0.3.287-bridge.1`** and the shipped route stays disabled. It implements describe/open, next-turn `agent.send`/`task.dispatch` and observation snapshots through the same binding-scoped module protocol as Muse. The stream mapper assembles complete messages from frames that share one `message.id` without losing content blocks, links children only by `parent_tool_use_id`, keeps init failures distinct and treats result usage as the SDK's cumulative estimate, never a sum. Attach/resume, configure (model/effort), goal, steer, permission replies, result pages and cross-restart recovery are reported **unavailable** by this artifact, not emulated; tool permission requests are recorded and denied rather than approved implicitly. Fixture streams authored from the pinned SDK types verify the mapping (`node modules/claude/selftest.mjs` after `npm ci`); no live Claude session is qualified by this slice.


## OpenCodex provider-service module — first slice (attach-only observer)

Follow [modules/opencodex/README.md](modules/opencodex/README.md). OpenCodex (`lidge-jun/opencodex` **v2.73.0**, commit `569e3e7`, MIT) is a provider/protocol proxy, not a session owner: execution stays with the native Codex backend above, and this module only attaches to an explicitly configured, already-running service to perform read-only Management API reads (health, memory, providers, protocols, models, usage). Its artifact is **`opencodex-2.73.0-bridge.1`** (Node, standard library only, nothing vendored); the shipped route example stays disabled. The client issues GET requests only, `shutdown` is detach-only (the externally owned shared proxy is never stopped or restarted by the adapter), and the Management admin credential is referenced by environment-variable name only — it never reaches Codex/model tools, a Task/Operation or bridge output. Upstream failures and conflicts (401, 409 `sibling_instance`, version mismatch, missing fields) are recorded as `unknown`, never as an empty healthy fleet, and routed models are labelled from evidence only: requested ≠ `wireModel` ≠ `servedModel`, billing derives solely from the provider's `authMode`, and a Claude model routed via OpenCodex oauth is the operator's Claude subscription through OpenCodex — not the native Claude route and not a "Max route". `doctor.inspect` projects the newest recorded snapshot per binding into `services.opencodex` without calling the service. Fixture verification runs against pinned-contract reconstructions (`node modules/opencodex/selftest.mjs`, 11 tests); no live opencodex service has been installed or qualified by this slice, and settings mutations, owned launch and protocol qualification are later slices.

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

Windows uses a uniquely named Global Job and query-only recovery; cancellation validates membership through pinned process handles. Linux uses boot/birth/group identities and pidfds, including whole-process termination when the main thread exits first. These are trusted execution boundaries, not sandboxes against deliberate process-group escape. The host now records a launch receipt at spawn: a worker that dies before publishing its identity is fixed **incomplete** (unknown exit/coverage) once the spawned process and its prospective group are proven departed — never a guessed pass and never a replay. Launches by an older host have no receipt and remain held, as do legacy unnamed Windows Jobs and damaged records. Platform details and limits are in the CheckRunner guide.

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

Public methods: `source.capture`, `check.run/get/profiles/cancel`, `host.status/mode`, `doctor.inspect`, `client.register/list`, `task.create/get/list/revise/claim/dispatch/submit/submission/request_changes/accept/acceptance/invalidate_acceptance`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile/recover/result`, `artifact.get/read/assemble/parts`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`, `gm.handover`. Module methods: `module.hello/next/outcome/observe/result`. Export is a client operation, not a remote arbitrary-file-write method.

`doctor.inspect` (CLI: `swarm doctor`) is read-only diagnostics over facts already recorded: schema digest/pragmas readback, admission mode, operation/binding/attempt/check state counts, module aggregates, open incidents with redacted details, client aggregates without credential material, configured routes and check profiles by name, and a bounded cross-check that recorded artifact files exist under the data directory. Each finding names a cause and the next addressed step; known build-level gaps (no forge publication, no live native qualification) are reported as gaps, not hidden. Doctor mutates nothing, starts no process, calls no model and performs no repair, resume or respawn — safe recovery stays with the addressed owning operations. Forge publication remains unimplemented: the documented pipeline (intent → remote effect/readback → durable publication fact → separate bookkeeping/cleanup) has no defined method or intent record yet, so there are no publication facts to audit.
>>>>>>> 355e55c (feat: add read-only doctor.inspect controller diagnostics)

### MCP facade for the General Manager

`swarm mcp` serves the same public methods to an MCP client over stdio (rmcp 3.5.0, pinned in Cargo.lock). The GM's client launches it as a child process with the usual `--data-dir`/`--config`/`--credential` options; it is a client of the running host over the same local IPC as the CLI — it never opens the database or a network listener, and closing it does not stop the host or cancel admitted work. Each public method is one tool named after the method with `.` replaced by `_` (e.g. `task_create`, `operation_get`); there is no universal passthrough or shell tool, and the application layer keeps validating every request. Mutations accept a stable `client_request_id` exactly like the CLI; when omitted, one is generated and echoed in the result object. Operations returned by mutations are durable handles to poll with `operation_get`. Live GM tool discovery against a real MCP client remains a qualification step, not a claim made here.

**Next: OpenCode cross-restart execution continuation and complete family reconstruction.** OpenCode goal controls are implemented on the implemented HTTP path as a controller-recorded durable instruction entry — OpenCode has no native goal API, and no automatic continuation after a terminal turn is implied. Exact session-agent selection and exact route-model restoration are implemented through native catalogs and session projection. A different provider/model/variant remains a route change rather than a hidden mutation of an active binding. Explicit configure-to-input prerequisite chaining is implemented with typed applied evidence and relevant-settings revalidation; preserve it rather than inferring dependencies from queue order. Exact detached tool-file retrieval is implemented through native descriptors and the existing artifact path; do not replace it with arbitrary URL/filesystem reads. Ordinary input/execution disposition now uses the native durable log; continuation without a proven terminal remains unresolved, not inferred from idle. Preserve these implemented paths instead of rebuilding them. Do not repeat session/inbox/readback/SSE implementation solely because an older checkpoint called all of C04 pending. Recorded-session Muse recovery and recorded check-worker recovery are implemented; missing identity, unresolved native outcomes and actual vendor/Windows runtime qualification remain explicit. Do not repeat the recorded-recovery implementation solely because an older checkpoint called it pending. Do not reimplement source capture, results, submission, acceptance or CheckRunner. Preserve the native shared-Codex/subscription targets. [SIWC notes](docs/runtime-notes.md) describe an optional OAuth route, not installed auth. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) remains after the main code.

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
