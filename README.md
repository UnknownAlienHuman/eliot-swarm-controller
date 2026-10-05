# Eliot Swarm Controller

Headless, provider-neutral transactional Rust framework for durable commands, Operations, events, actions and messages. Its current application orchestrates native coding-agent harnesses as a prototype for the Eliot Memory OS Agent Execution Fabric. Provider and harness adapters translate common commands into concrete native instructions/APIs, and native events and results back into shared contracts, through the existing `RuntimePort` boundary. The kernel owns shared scheduling and durable state transitions independently of provider. One host, one SQLite database, local IPC; no UI, external broker or replacement model loop.

Eliot Memory OS is not yet connected on the owner machine. The standalone controller remains usable; the requested conditional `eliot_compile_packet_l3` integration and its verification requirements are recorded in [the integration contract](docs/eliot-memory-os-integration.md). This records a request, not an implemented or qualified packet capability.

## Current implementation — 0.1.0

The Rust core provides authenticated clients, task revisions/ownership, durable request receipts, directed mailbox, incremental reports, binding-scoped module admission, immutable artifacts, submission/review and acceptance. Windows uses user-restricted Named Pipes; Unix uses a private socket. No TCP control listener is opened.

The Muse SDK bridge opens an explicitly selected native executable, delivers Task snapshots with per-turn effort, handles exact-turn steer/questions/goal/configuration, and reports observed children and run identities. A live bridge reconnects without closing Muse or replaying model input. The current Bridge.7 retains a recovery checkpoint and supports explicit recovery of a recorded native session through the non-killing `module-run` owner. Pinned result reads do not consume `subagent/readResult`; local whole-result assembly and verified export are implemented.

**CheckRunner executes configured commands on captured Git sources. Active cancellation and recovery of a recorded departed check worker are now implemented.** Cancellation does not stop the host or native agents. Recovery uses the worker lock and OS group identity; it does not infer success or replay a command.

**Still pending:** live Muse resume qualification, native-outcome recovery for unrecorded effects, live child/family evidence, measured performance of the new Store path and OpenCode running-input continuation across service restart. Family projections retain explicit partial coverage where the native API cannot prove completeness. Module updates remain manual for 0.1 under the [accepted owner policy](docs/owner-decisions.md); an automatic installer is deferred. Live Muse/Max inference and Windows native launch remain unqualified. The owner-machine [qualification record](docs/qualification-2026-10-03.md) documents the bounded native OpenCode, Codex, Command and Antigravity contours. The [restart checkpoint](docs/restart-checkpoint-2026-10-03.md) records the new source and remaining gates. Do not mark all C01–C03 complete.

### Capability and qualification matrix

Capability states use the [Documentation Program](docs/documentation-program.md) vocabulary (`implemented`, `fixture_checked`, `live_observed`, `qualified`, `unavailable`, `unknown`). `fixture_checked` is not `qualified`, and no row below claims live qualification unless its notes say so. Current owner decisions and issue bodies define the remaining work; historical verification applies only to its recorded source revision.

| Component | Artifact ID | Status | Notes | Guide |
| --- | --- | --- | --- | --- |
| Core / Store | — (built-in Rust core) | implemented | Durable Tasks, Attempts, Operations, request receipts, immutable artifacts, submissions, review and acceptance. | [Architecture](docs/agent_swarm.md) |
| CheckRunner | — (built-in Rust core) | implemented | Fixed-source execution, cancellation/recovery, frozen versioned-input identities, conservative reverse-dependency scope and conditional reuse of an accepted original process CheckRun. Unknown inputs widen scope or disable reuse. | [CheckRunner](docs/check-runner.md) |
| OpenCode | `eliot-opencode-v2.http.1` | live_observed | Owned native 2.0.7 service with durable events completed one `opencode-go/space-bunny-free` input, exact producer/result readback and same-session open recovery. In-flight service restart remains unqualified. | [OpenCode guide](modules/opencode/README.md) |
| Muse | `muse-sdk-1.3.0-bridge.7` | fixture_checked | Steer, settings, goal/replies, retained result reads, recorded-session recovery and R18 durability/host-death/gap-fill observations implemented; restored child snapshots carry explicit freshness and only durable command rejection settles rejected input. Live Muse resume, live Max inference and Windows launch remain unqualified. | [Muse guide](modules/muse/README.md) |
| Codex | `codex-sdk-18194bf-bridge.3` | fixture_checked | Native child links, bounded lifecycle events, exact-turn history pages and rejection of conflicting child identities are fixture-tested; vendor provenance verifies 89 files. Historical `.2` completed separate native 0.159.0 subscription GPT Luna and OpenCodex Bunny inputs. Current mixed-provider composition remains unqualified; family coverage is partial and auxiliary affinity is unavailable. | [Codex guide](modules/codex/README.md) |
| Claude | `claude-agent-sdk-0.3.287-bridge.3` | fixture_checked | Rootless prepared open, exact first-input identity adoption, echoed input UUID/result correlation, next-turn send and snapshots. Native quota use is deferred until the other routes are ready; unsupported lifecycle capabilities remain unavailable. | [Claude guide](modules/claude/README.md) |
| Antigravity | `antigravity-cli-warm-bridge.2` | live_observed | Installed 1.2.15 Gemini 3.8 Flash open, dispatch and refresh completed on Windows; terminal evidence binds to an immutable recorded observation, without invented native turn IDs. | [Antigravity guide](modules/antigravity/README.md) |
| Command | `command-mod-0.1.0-glue.4` | fixture_checked | Native event envelopes, raw-frame retention, model-request observations and saved terminal/event validation pass fixtures. Historical `.3` completed one Bunny input on installed 1.74.1; native `.4` qualification remains pending and served-model identity remains unknown. | [Command guide](modules/command/README.md) |
| Zed | `eliot-zed.eval-cli.1` | fixture_checked | Sessionless batch admission, retained output pages and readback pass local fixtures; native installation/qualification is deferred by the operator. | [Zed guide](docs/zed-batch.md) |
| OpenCodex | `opencodex-2.75.0-bridge.3` | live_observed | Read-only observation of owned 2.64.0, with the baseline mismatch and unavailable sections retained; GPT Luna subscription and Bunny key-backed proxy routes were observed separately. Configuration/protocol qualification remains pending. | [OpenCodex guide](modules/opencodex/README.md) |
| MCP | — (built-in facade, rmcp 3.5.0) | live_observed | Real local stdio discovery/dispatch verified observer/local-full/GM allowlists (20/53/48 tools); remote gateway remains unqualified. | [MCP profiles](docs/mcp-profiles.md) |
| Remote Gateway | — (optional Rust process) | implemented | Loopback Streamable HTTP reuses the fixed restricted MCP profile and authenticated IPC. Private bearer mapping is implemented; live HTTP, Cloudflare/OAuth and external-client qualification remain pending. | [Gateway](docs/gateway.md) |
| GM / doctor | — (built-in) | implemented | GM designation/handover and read-only `doctor.inspect` implemented; no native push path is qualified for GM wake, so wake stays `checkpoint_poll`. | This README (GM designation and API sections) |
| Policy/source index | `owner-policy-v1` | fixture_checked | Policy revision and selected canonical source digests freeze into each Attempt; the accepted policy section keeps its original digest. | [Task policy](docs/task-policy.md) |
| Scheduler | — (built-in) | implemented | Explicit CheckRun schedules coalesce missed slots; new-work mode, ordinary admission and unknown-effect readback remain authoritative. | [Schedules](docs/schedules.md) |
| Forge | — (built-in) | live_observed | A real R6 API source capture, required CheckRun, independent acceptance and one non-force GitHub main publication passed exact remote readback on 2026-10-03; qualification is scoped to that recorded candidate and path. | [Forge publication](docs/forge-publication.md) |

## Build and run

```powershell
$target = Join-Path $env:LOCALAPPDATA 'eliot-swarm-shared-target'
[void][IO.Directory]::CreateDirectory($target)
cargo build --locked --release --package swarm-cli --bin swarm --target-dir $target
cargo build --locked --release --package eliot-swarm-controller --bin swarm-host --target-dir $target
$swarmHost = Join-Path (Join-Path $target 'release') 'swarm-host.exe'
& $swarmHost --data-dir C:\SwarmState host
```

Use an initially empty dedicated local directory. The host owns only its marker/lock, database, artifacts and credentials there. It refuses unrelated nonempty directories; global PATH, UAC and vendor settings are untouched. Read-only CLI calls do not initialize a database or launch the host. The public swarm client invokes the adjacent swarm-host only for explicit local commands; ordinary application requests never start the host.

The CLI and host are separately built artifacts, each with its own source commit, tree, and image digest in its provenance manifest. The installer checks each manifest against its own receipt and verifies the declared IPC protocol, target, and host-launch contract; the two artifacts need not come from the same source revision.

In another PowerShell:

```powershell
$target = Join-Path $env:LOCALAPPDATA 'eliot-swarm-shared-target'
$swarm = Join-Path (Join-Path $target 'release') 'swarm.exe'
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

**Inbox admission is not native turn completion.** Read-only `agent.result` exports exact completed assistant messages, input-ID-anchored projected intervals, isolated native patches and exact file items from completed tool results into immutable, digest-checked artifacts. Tool-file selectors identify a native message/call/content index; they cannot inject a path or URL. Inline data URIs are decoded exactly, while location-confined `file:` descriptors are read through OpenCode's native `fs.read` route and reread before publication. A separate durable session-log reader maps exact inbox IDs to native execution-start event IDs and terminal disposition, including coalesced inputs and cancellation before delivery. Its checkpointed GET reads recover without input replay; shutdown/uncertain continuation remains unresolved. The same anchored log protocol now reads tracked child sessions individually — bound, previously open, or natively active members only — so a proven child terminal discharges exactly that child's producer, and the family snapshot reports per-axis coverage counters. Family completeness still stays `partial`: no native read is an atomic family snapshot. This is per-session execution evidence, not whole-family completion or Task acceptance. The R6 owner-machine record observes ordinary Bunny execution and same-root host recovery; in-flight service restart remains unqualified; goal controls are implemented as a controller-recorded durable instruction entry with one-prompt activation (OpenCode has no native goal API and there is no automatic continuation), and durable controller-owned instruction entries, exact session-agent selection and exact route-model restoration are implemented with readback. Unknown children/stream gaps never become an idle family or a passed Task. Follow [the module guide](modules/opencode/README.md) and the disabled route in [configuration](config/controller.example.toml). Updates and rollback follow [modules/opencode/UPDATE.md](modules/opencode/UPDATE.md).

The complete pinned `atlas-redact` donor is now used to scrub retained native question/diagnostic copies. Its upstream files remain unchanged behind a separate Cargo wrapper; licenses, rule notices and snapshot hashes are retained in [third-party notices](THIRD_PARTY_NOTICES.md). Snapshot updates follow [modules/atlas-redact/UPDATE.md](modules/atlas-redact/UPDATE.md). This is actual library reuse, not a claim that all listed donors were installed.

## Zed eval-cli batch runtime

`src/runtime/zed.rs` implements the C11 Zed boundary over the pinned native `eval-cli` contract (ZD-EXEC basis `7604aa3f`). The host now owns route admission, the batch supervisor and saved-run readback. `agent.open` is an executor preflight; it creates no native session. `task.dispatch` starts one operation-derived batch run over the frozen Task snapshot. Persistent control, resume, goal, steer and session family remain unavailable. Exit codes keep their native meanings (0 agent finished, 1 error, 2 timeout, 3 interrupted), and exit 0 is a finished run, not Task acceptance. `result.json` is cross-checked against the exit code and configured model, and retained `result.json`/`thread.md`/`thread.json` pages bind to the exact operation, run, binding and generation. Receipt loss invokes readback rather than execution replay. See [Zed batch wiring](docs/zed-batch.md) for configuration and output selectors. Installed-binary qualification remains pending.


**Why JavaScript exists:** Muse and Claude use their vendors' official Node SDKs; Antigravity and Command glue their native executable contracts. The separate OpenCode service owner runs the pinned official native packages under Bun with durable event persistence. Controller authority and the OpenCode HTTP adapter remain Rust. Muse's first runnable bridge is commit `b2bd0211` (2026-09-30). See [the exact provenance](docs/javascript-provenance.md).

## Native Muse, clients and task-specific children

Follow [modules/muse/README.md](modules/muse/README.md). Enable a private route, reserve `agent.open`, register its scoped module credential, install the locked module-local SDK and independently launch the bridge using the installed native executable. The shipped route is disabled; its artifact version is **`muse-sdk-1.3.0-bridge.7`**. Local check changes do not require replacing a running bridge. Version updates and rollback follow [modules/muse/UPDATE.md](modules/muse/UPDATE.md).

Native subscription/auth and effort remain in the harness. Requested effort, effective setting and observed inference are different evidence. No Go/API route silently substitutes for Muse Code Max. Full conversation history stays native.

`agent.send` supports next-turn input or exact-turn steer. `agent.reply` submits native decisions with current IDs; the bridge's immediate MSP `{}` response acknowledges presentation only. Configure/goal/refresh/reconcile keep their distinct application boundaries. Refresh is not resume; a timeout does not authorize another prompt.

```powershell
swarm --data-dir C:\SwarmState --request-id register-w1 client-create W1 --role manager --out C:\SwarmState\W1.credential.json
swarm --data-dir C:\SwarmState --credential C:\SwarmState\W1.credential.json task list
```

`task claim --binding-id BINDING --generation 1` defaults to `native_manager`: native delegation does not receive a duplicate controller start. `swarm family BINDING --generation 1` returns a retained observation; later pages reuse its observation_id. `swarm task bind ATTEMPT --assignment NAME --session CHILD --turn TURN --observation-id ID` maps an already observed run. An old completion does not close a new assignment; parent idle does not erase children. Family coverage remains partial.

Mailbox readers have independent cursors. Module credentials are binding-scoped and cannot accept Tasks or become GM. Same-user roles coordinate trusted clients; they are not an OS sandbox.

### GM designation and handover

The project is persistent controller state and survives loss of the GM chat.
An authenticated connection has a fresh `link_id`; the saved credential retains
its durable `client_id`. A new chat using that credential needs no handover.
`gm.handover`, called by the local operator or current GM, appoints a registered
successor without requiring participation from the old chat. The epoch advances
when the designated client changes; rebinding the same client to another native
session preserves it.

The successor can read retained project Operations and explicitly continue exact
current Attempts. Attempts keep their original owners and audit history records
the actual acting successor. Initial dispatch identity, native binding generation,
unresolved-effect readback and independent acceptance remain checked. Former-owner
automation settings and explanations are available to the current GM through the
explicit read-only `owner_manager_id` selector. See the
[reconnect and recovery commands](docs/gm-session-continuity.md).

`host.status` reports the current designation, epoch and `gm_wake_mode`. Wake is
`checkpoint_poll` until a native push entrypoint is qualified. Queued Forge
publication freezes its authority epoch; a client handover prevents the old
never-sent publication from pushing. Sent or unknown effects require readback,
and historical client-addressed mailbox entries retain their recipients.

### Recorded Muse recovery

For new managed launches, use `swarm module-run --state-dir MODULE_STATE --command ABSOLUTE_NODE_PATH -- BRIDGE_SCRIPT --config MODULE_CONFIG`; paths/argv stay separate. The module directory is distinct from the host directory. The owner holds its OS lock and waits for both the bridge and remaining native processes; losing the owner does not authorize replacing a still-live group.

After the recorded old group has ended, a replacement bridge restores its checkpoint and the host marks that binding `reconciling`. Explicit operator `agent.recover` targets its current `expected_boot_id`. The bridge resumes the same known native session, not a fresh session/fork or replayed Task prompt. A historical open receipt can restore identity but not new-boot readiness. Only the correlated current-boot resume outcome makes the binding ready. Refer to [the module guide](modules/muse/README.md#recorded-session-recovery--bridge5) for the JSON request and failure boundaries.

Existing unguarded bridges are not retroactively qualified. Unknown/corrupt checkpoint or process identity remains an explicit recovery gap; no force-reset or blanket retry is added. Live vendor resume/children/Max have not been exercised by the local process-owner invocation below.

## Claude Agent SDK bridge — first slice

Follow [modules/claude/README.md](modules/claude/README.md). The Claude adapter is a separate SDK-owned bridge over the pinned `@anthropic-ai/claude-agent-sdk` **0.3.287** (Anthropic Commercial Terms) and its matching bundled native binary; its artifact is **`claude-agent-sdk-0.3.287-bridge.3`** and the shipped route stays disabled. Open prepares the native executor without sending a model input or inventing a session ID. Its first frozen Task input adopts the actual SDK session identity after an exact user-UUID echo; subsequent next-turn input uses the same boot and root. Exact SDK result correlation completes only its producer, while Task acceptance remains separate. An explicit route `modelId` reaches the SDK's model option; requested model and native effective model remain separate observations. The stream mapper assembles complete messages from frames that share one `message.id` without losing content blocks, links children only by `parent_tool_use_id`, keeps init failures distinct and treats result usage as the SDK's cumulative estimate, never a sum. Attach/resume, configure (model/effort), goal, steer, permission replies, result pages and cross-restart recovery are reported **unavailable** by this artifact; tool permission requests are recorded and denied rather than approved implicitly. Fixture streams authored from the pinned SDK types verify the mapping (`node modules/claude/selftest.mjs` after `npm ci`); native quota use is deferred until the other routes are ready.


## OpenCodex provider-service module — observer + configuration Operations (bridge.3)

Follow [modules/opencodex/README.md](modules/opencodex/README.md). OpenCodex (`lidge-jun/opencodex` **v2.75.0**, commit `ef0297f`, MIT; the service is operator-managed and not pinned — v2.75.0 is the release this adapter's contracts were last verified against, and the baseline follows upstream current) is a provider/protocol proxy, not a session owner: execution stays with the native Codex backend above, and this module only attaches to an explicitly configured, already-running service to perform Management API reads (health, memory, providers, protocols, models, usage, configuration state) and — as saved Operations — single operator-requested configuration changes (protocol/model settings, the sub-agent surface, client-integration apply/rollback through the documented preview + `planFingerprint` confirm flow, with readback after every write and no blind retries). Its artifact is **`opencodex-2.75.0-bridge.3`** (Node, standard library only, nothing vendored); the shipped route example stays disabled. Observation issues GET requests only and mutations happen only inside an explicit `configure` Operation, `shutdown` is detach-only (the externally owned shared proxy is never stopped or restarted by the adapter), and the Management admin credential is referenced by environment-variable name only — it never reaches Codex/model tools, a Task/Operation or bridge output. Upstream failures and conflicts (401, 409 `sibling_instance`, missing fields) are recorded as `unknown`, never as an empty healthy fleet; an observed service version differing from the adapter's contract baseline is recorded as an observation only — it neither degrades readiness nor blocks configuration Operations, and routed models are labelled from evidence only: requested ≠ `wireModel` ≠ `servedModel`, billing provenance is labelled from the provider's `authMode` rather than inferred from a price estimate, and a Claude model routed via OpenCodex oauth is the operator's Claude subscription through OpenCodex — not the native Claude route and not a "Max route". `doctor.inspect` projects the newest recorded snapshot per binding into `services.opencodex` without calling the service. Fixture verification runs against fixture reconstructions of the upstream contracts (baseline last verified at v2.75.0; `node modules/opencodex/selftest.mjs`, 28 tests); the dated owner-machine record observes an isolated installed 2.64.0 service and the separate Codex execution routes. The baseline/version difference and unavailable Management sections remain recorded; protocol/configuration qualification is still pending.

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
# Read the scoped Operation result.candidate_ref (or candidate_refs for a native result); submit/check that same candidate.
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

`task.submit` seals a candidate and requirement report. An ordinary Participant may submit only its exact current Task revision and Attempt; this does not accept the Task or grant create/claim/accept/runtime/recovery authority. The candidate can be an exact source snapshot of this Attempt/revision, an assembled native result, or one native page covering its complete source. Bound native results must belong to that binding/generation and an applied `agent.result` origin. Bytes are checked off the DB thread before immutable submission publication. Only source snapshots can be machine-checked by CheckRunner.

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

Public methods: `source.capture`, `check.run/get/profiles/cancel`, `host.status/mode`, `doctor.inspect`, `client.register/list`, `task.create/get/list/revise/claim/dispatch/submit/submission/request_changes/accept/acceptance/invalidate_acceptance`, `attempt.get/release/bind_producer`, `agent.open/state/list/family/send/reply/configure/goal/refresh/reconcile/recover/result`, `forge.publish_ref`, `artifact.get/read/assemble/parts`, `route.list`, `operation.get/list/cancel`, `message.send/read`, `report.delta`, `gm.handover`. Module methods: `module.hello/next/outcome/observe/result`. Export is a client operation, not a remote arbitrary-file-write method.

`doctor.inspect` (CLI: `swarm doctor`) is read-only diagnostics over facts already recorded: schema digest/pragmas readback, admission mode, operation/binding/attempt/check state counts, module aggregates, open incidents with redacted details, client aggregates without credential material, configured routes and check profiles by name, and a bounded cross-check that recorded artifact files exist under the data directory. Forge diagnostics report configured publication facts and durable queued/readback Operation aggregates; native publication qualification remains `unknown` without a qualification record. Each finding names a cause and the next addressed step. Doctor mutates nothing, starts no process, calls no model and performs no repair, resume or respawn — safe recovery stays with the addressed owning operations.

`forge.publish_ref` is implemented as a durable queued Operation for one currently accepted exact source-snapshot candidate and one configured non-force ref. It records the admitted GM epoch and rechecks current authority, acceptance and epoch before the push; stale work settles without sending. Once a push may have started, recovery only reads back the exact remote ref and never replays the push. An unconfirmed Git process tree remains a sticky hold that blocks later pushes to the same canonical repository; a matching ref cannot clear it. [Native qualification on 2026-10-03](docs/qualification/2026-10-03-forge-native.md) passed a real source capture, required CheckRun, independent acceptance and one non-force GitHub publication with exact remote readback. New combined gates for the current working changes remain pending.

### MCP facade for the General Manager

`swarm mcp` serves the same public methods to an MCP client over stdio (rmcp 3.5.0, pinned in Cargo.lock). The GM's client launches it as a child process with the usual `--data-dir`/`--config`/`--credential` options; it is a client of the running host over the same local IPC as the CLI — it never opens the database or a network listener, and closing it does not stop the host or cancel admitted work. Each public method is one tool named after the method with `.` replaced by `_` (e.g. `task_create`, `operation_get`); there is no universal passthrough or shell tool, and the application layer keeps validating every request. Mutations accept a stable `client_request_id` exactly like the CLI. Only a caller-known ID chosen before dispatch can be safely retried after a lost reply: when the ID is omitted, the server generates one and echoes it in the result object, but if that response itself is lost the caller never learns the generated ID, so such a call cannot be safely retried — a generated ID helps correlation only when the response is received. Operations returned by mutations are durable handles to poll with `operation_get`. When the client declares the `io.modelcontextprotocol/tasks` extension, a mutation whose operation is still in flight instead returns an MCP task whose `taskId` is that operation's ID: `tasks/get` projects the operation (the single authority — no task state is stored in the facade, and RMCP's `TaskManager` is not used), and `tasks/cancel` submits the existing `operation_cancel` for a still-queued operation. Pending native inputs surfaced by `tasks/get` are answered with `agent_reply`, not `tasks/update`. The facade also advertises an `eliot/subscriptions` extension: the `eliot/subscribe` / `eliot/unsubscribe` protocol methods open and close a bounded subscription over committed facts only — the categories `reports`, `mailbox` and `operations` are exact filters over the one committed observation stream `report_delta` reads, never over a live native stream — and `notifications/eliot/committed` carries each committed transition with the projection frame of the page it was read from. Each subscription's queue is bounded; on overflow the subscriber receives exactly one `notifications/eliot/lagged` marker naming the skipped range and resyncs through `report_delta` / `message_read` / `operation_get` from the last delivered cursor. Subscriptions die with the session: a reconnect is not replay continuity, and state is re-established by cursor plus resync. Live GM tool discovery against a real MCP client remains a qualification step, not a claim made here.

**Next: OpenCode cross-restart execution continuation and complete family reconstruction.** OpenCode goal controls are implemented on the implemented HTTP path as a controller-recorded durable instruction entry — OpenCode has no native goal API, and no automatic continuation after a terminal turn is implied. Exact session-agent selection and exact route-model restoration are implemented through native catalogs and session projection. A different provider/model/variant remains a route change rather than a hidden mutation of an active binding. Explicit configure-to-input prerequisite chaining is implemented with typed applied evidence and relevant-settings revalidation; preserve it rather than inferring dependencies from queue order. Exact detached tool-file retrieval is implemented through native descriptors and the existing artifact path; do not replace it with arbitrary URL/filesystem reads. Ordinary input/execution disposition now uses the native durable log; continuation without a proven terminal remains unresolved, not inferred from idle. Preserve these implemented paths instead of rebuilding them. Do not repeat session/inbox/readback/SSE implementation solely because an older checkpoint called all of C04 pending. Recorded-session Muse recovery and recorded check-worker recovery are implemented; missing identity, unresolved native outcomes and actual vendor/Windows runtime qualification remain explicit. Do not repeat the recorded-recovery implementation solely because an older checkpoint called it pending. Do not reimplement source capture, results, submission, acceptance or CheckRunner. Preserve the native shared-Codex/subscription targets. [SIWC notes](docs/runtime-notes.md) describe an optional OAuth route, not installed auth. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) no longer sits after the main code: the observer/configuration bridge.3 is implemented; the remaining work composes it with the native Codex session owner from issue #9, with explicit provider/model/billing, session affinity, protocol/tool/history checks and live qualification. The externally owned provider service remains distinct from the native session owner.

## Evidence and development

**OpenCode source recovery, 2026-10-01 (New York):** the interrupted implementation's 49 staged source files were recovered from tree `f54b9259dbc052c942e089aea95a130166916a01` and published in `b1ff71ff`. Fresh package formatting and minimal warnings-denied Clippy passed on Rust 1.98.1. The retained 16 protocol/Store fixtures were not rerun during recovery; their earlier reported result is not new qualification evidence. Normal CI checks formatting, Clippy, the unchanged Atlas snapshot, Muse syntax/SDK import and release builds on Windows/Linux. Consult the exact commit's run before claiming a platform pass. No tests, native model calls or subscription qualification are run by this recovery workflow.


**Previous Muse recovery baseline: `d385498b00fe1a049c357613a7777cc4a1c83f72`.** The interrupted continuation saved the module owner, checkpoint and controlled session-recovery implementation through `8a84e6be`; it was not lost. Recovery reconstructed its exact 104-file source tree from the CI artifact. A bounded invocation then found an actual runtime defect: changed-boot admission used nonexistent `operations.generation` instead of `binding_generation`. The single-query correction is in `d385498b`; schema, dependencies and native-module source are unchanged by that correction.

On **2026-10-01**, exact [CI run 36872022736](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36872022736) passed formatting, warnings-denied Clippy, Muse syntax/SDK import and release builds on **Windows and Linux**. Earlier green compilation alone did not catch the SQL execution error.

Corrected Linux artifact `11168070937` passed ZIP/SHA-256 verification (`7b1d8422ffbdb858cc984349f3087c63a97eec3f5e6162411ffa865095f2c1d5`); its 104 source files match the fixed tree `d385c392228da6213eb217739c885ceef62d5d2f`.

The exact corrected Linux binary passed 13 directed assertions through the real host and `module-run`: live-owner exclusion, descendant survival after the fixture bridge and launcher were killed, no replacement until that recorded group ended, changed-boot admission, old-link rejection, delayed-open receipt without readiness, input refusal before resume, stale/disabled recovery rejection and correlated current-boot readiness. The real Node checkpoint writer serialized concurrent writes and read the last committed state. Both the remaining managed process and host exited cleanly.

**Native open/resume outcomes in this invocation were synthetic module messages.** It exercised real OS ownership, Store and IPC, not Muse inference or a vendor SDK session. No new test modules, cargo test, native models, user repositories or system settings were involved. Windows Job runtime, actual Muse resume with children/Max, missing-identity windows, Cargo runtime/cache and load remain unqualified. The failed pre-fix invocation and successful corrected one are different evidence, not a renamed old pass.

Historical baselines: C06 `c56f50a4` passed [CI 36828917649](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36828917649) and a real-command invocation of capture, output, failure/source-change rejection, host restart and checked acceptance. Acceptance `01212a4e` passed [CI 36816310704](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36816310704). Those are earlier evidence, not substitutes for this cancellation/recovery run.

The workflow pins Rust 1.98.1/Cargo.lock and runs scoped formatting/strict Clippy for changed packages and reverse dependents on PRs and main pushes, with changed integration targets and donor integrity checked by path. The full Rust, native fixture, and release matrix remains available through manual `workflow_dispatch`, not every code push; its Rust checks enumerate all owned metadata workspace packages and exclude targets sourced from the Atlas vendor tree. `just verify BASE_SHA HEAD_SHA` runs scoped formatting/Clippy for two full commit SHAs; `just verify-full` retains the full local gate. CI performs no vendor model sessions, login or global installation. Artifacts include exact source SHA and a source archive; consult the exact commit's run before claiming a platform pass.

| Document | Purpose |
| --- | --- |
| [Architecture](docs/agent_swarm.md) | Target execution model |
| [Implementation plan](docs/agent_swarm.implementation-v6.md) | C01–C11 and transaction boundaries |
| [Module contract](docs/agent_swarm.module-contract-v2.md) | Capabilities, delivery and lifecycle |
| [CheckRunner](docs/check-runner.md) | Implemented execution/cancellation/recovery details |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Design examples, not implementation evidence |
| [Agent communication, launcher and MCP program](docs/agent-communication-program.md) | PR #22 canonical entrypoint and links to participant, launcher, surface and catalog requirements |
| [Canonical MCP surfaces](docs/mcp-canonical-surfaces-and-topologies.md) / [tool catalog](docs/mcp-tool-catalog-and-loading.md) | Public names, permission profiles, topology and deferred loading |
| [Agent Operations](docs/agent-operations/README.md) | PR #23 manager-owned configuration, dispatch, review and delivery requirements |
| [Donors](docs/agent_swarm.donors-20260929.toml) | Inventory; selected Atlas snapshot is tracked separately, not all installed runtimes |
| [Lessons](docs/lessons-learned.md) / [runtime notes](docs/runtime-notes.md) / [candidates](docs/candidate-notes.md) | On-demand evidence, not extra worker instructions |

This README records current readiness. Work only in main, without worktrees. Code useful paths first, then focused formatting/Clippy; broad tests follow working slices. The nine-table migration is unchanged. Foreign/draft/newer databases and missing credentials are not silently replaced. Preserve a cleanly stopped state directory in full, not only a live DB without WAL. Historical briefs remain in Git history for provenance, not present install defaults.
