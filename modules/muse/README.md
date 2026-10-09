# Muse SDK bridge — native integration

Uses the complete locked `@muse-code/sdk` **1.3.0** and pinned [MSP schema](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/msp.d.ts). The TaskPrompt migration targets **`muse-sdk-1.3.0-bridge.9`**. The host must retain and accept the exact bridge.9 descriptor before activating new bindings. Live Muse/Max and Windows native launch remain unqualified; syntax checks do not attest model execution.

**Activation blocker:** the host currently accepts bridge.8. Register and retain
the bridge.9 descriptor before selecting it for a new binding. Existing bridge.8
bindings keep their original artifact identity and code.

## Ownership and setup

The bridge owns one `muse serve` connection. Host IPC exposes `module.hello/next/outcome/observe/result`; its credential is scoped to the reserved binding/generation and cannot accept Tasks or become GM. Native subscription/auth, tools and model loop remain in Muse.

1. Run `npm ci --ignore-scripts` here for the locked local SDK, not global packages or a model login.
2. Enable a private controller route with the actual workspace, native model and explicit effort. The shipped route stays disabled. Run the host and reserve `agent.open` with lane_id/route; retain binding_id/generation.
3. Register the scoped module credential: `swarm client-create muse-MC --role module --binding-id BINDING --generation 1 --out PRIVATE_FILE`. Use normal host/data-dir arguments and preserve request IDs.
4. Copy `module.example.json` outside Git, set the actual host endpoint, credential file and native executable/argv. Select a real Windows `.exe`, not `.cmd`/`.bat`. Match moduleArtifactId to the route.
5. Start the module independently with the guarded launcher below. Only its admitted open/recovery operation starts the native executable. No PATH, UAC or vendor service configuration is changed.

```toml
[[routes]]
alias = 'muse-manager'
runtime = 'muse'
module_artifact_id = 'muse-sdk-1.3.0-bridge.9'
enabled = true
[routes.native_options]
workspaceRoot = 'C:\Projects\YourRepository'
modelId = 'muse-spark-1.3-contributor'
reasoningEffort = 'max'
approvalMode = 'allowAll'
```

```powershell
# Replace these example paths; the interpreter and script are separate argv.
swarm module-run --state-dir C:\SwarmModules\MC --command 'C:\Program Files\nodejs\node.exe' -- C:\SwarmCode\modules\muse\bridge.mjs --config C:\SwarmConfig\muse.json
```

Use a dedicated initially empty module-state directory, separate from host data. The launcher holds an OS lock and a non-killing Job/process group. It waits for the bridge and its remaining native descendants; it does not restart or terminate them. A second launcher cannot adopt live work. Even after loss of the launcher, retained native members prevent replacement through recorded OS group identity. Inspection denied or ambiguous is not an empty group. These are trusted-process boundaries, not sandboxes against deliberate external-daemon/process-group escape.

`node bridge.mjs --config FILE` remains an unguarded legacy entrypoint: it has host-reconnect behavior, but no recorded process ownership/checkpoint for automatic bridge-loss recovery. Do not retrofit proof into a running bridge.4 or overwrite its executable/scripts in place. Use a new artifact/binding for changed module code. The managed path uses the SDK's public Connection with a non-detached child transport so ownership follows the launcher; MSP framing and routing remain in the SDK.

Inspect `agent.state`, then claim a controller-start Task with binding-id/generation and send `task.dispatch`. Native-manager claims can delegate children without receiving a second controller prompt. Requested effort, applied defaults and actual inference evidence remain distinct. Max is explicit on turns; no silent alternate provider or reduced effort.

## Pending-request refresh — bridge.8

Server requests and notifications now invalidate the same per-session read
window. Root and child refresh share one inventory replacement path: every
returned request must have the requested session ID and a nonempty unique native
ID before any current entry is removed. A native event during the read keeps the
newer local inventory; a malformed inventory leaves it unchanged. Refresh records
`pending_inventory_applied` separately from `metadata_applied`.

An `approval/updated` frame is a stage update, not a replacement for the original
tool/request identity. The retained request supplies omitted identity fields;
the update supplies the current stage fields, including optional-origin removal.
This follows the pinned SDK's [approval update field ownership](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/approval.ts).
No SDK approval handler or automatic decision is installed. Presentation still
returns `{}`; an answer remains an explicit addressed `agent.reply` Operation.

This code requires artifact `muse-sdk-1.3.0-bridge.9` and refuses to announce a
previous artifact ID. Stage it for new bindings only. Existing bridge.8 bindings
and checkpoints retain their original code and identity; they are not migrated.
The SDK pin is unchanged. Syntax checks are not behavioral qualification; refresh
race fixtures, SDK import and native qualification remain pending for bridge.9.

## TaskPrompt v1 — bridge.9

For `task.dispatch`, the bridge consumes the Store-enriched
`input.task_prompt` and `input.task_dispatch_context`. It checks the exact
envelope fields, Task/binding/operation identity, source-text digest, prompt
SHA-256 and UTF-8 byte count (maximum 1,000,000 bytes) before native submission.
The envelope's `prompt` is the complete native input; the bridge does not render
or append a raw Task snapshot. An acknowledged native command returns the
existing `swarm.task_dispatch_admission@1` receipt bound to the exact prompt
digest, byte count and native command ID.

Proposed descriptor template path: `modules/muse/module-descriptor.template.json`.
It must declare `swarm.task_prompt@1` and `swarm.task_dispatch_context@1` as
command schemas and `swarm.task_dispatch_admission@1` as an event schema, with
the existing runtime command/outcome schemas and `task.dispatch` capability.

## Recorded-session recovery — bridge.5

The local checkpoint persists known session/namespace, relevant settings, compact family observations, pending native command identities/payloads/ACKs and unacknowledged outcomes/result pages. It does not copy credentials or the full native transcript. Writes are serialized and atomically published; a failed checkpoint blocks subsequent native mutations rather than silently losing the recovery boundary. The OS owner serializes this directory, not a second task database.

Host disconnect does not close the SDK. The same live bridge reconnects and reports retained outcomes. After **the bridge process** is lost, restarting the same module-run command first checks that the recorded prior owner and native group are gone. It never kills them to obtain permission. The host independently checks that evidence off the SQLite thread and CAS-checks it again at admission. A changed boot preserves the native conversation and Attempt identities, marks recovery_required and leaves incompatible work pending.

Read `agent.state` for the current bridge_boot_id, then as operator submit:

```json
{
  "binding_id": "EXISTING_BINDING",
  "generation": 1,
  "expected_boot_id": "CURRENT_RECOVERY_BOOT",
  "reason": "Resume the recorded session after verified native-owner departure"
}
```

```powershell
swarm --request-id recover-1 call agent.recover --file recovery.json
swarm call operation.get --file operation.json
```

This is explicit native work, **not a healthcheck**. New-work-disabled prevents admission. It reads the exact recorded session/model in the original native namespace and uses `session/resume` with excludeItems; no `session/start`, fork or original-prompt replay is a fallback. The current connection becomes ready only upon its correlated resume result. Retained outcomes are reported without native resend. Unresolved commands require targeted `agent.reconcile` with their original IDs; nothing repeatedly replays them merely because the bridge restarted.

For native command outcomes, only the pinned SDK's durable `commandRejected` signal (code `-32030` or kind `commandRejected`) settles an admitted command as rejected. `invalidParams`, `overloaded` and `backpressured` admit nothing but remain unknown/held for explicit same-ID reconciliation; the bridge does not retry them automatically.

Binding/generation denotes the same retained conversation here; bridge boot and managed process-group identity separately denote its replacement process owner. Historical run identities and Task-specific ownership are not erased. Root/known-child subscriptions and pending questions are refreshed, but family completeness stays partial and stale terminal evidence cannot close a newer run. Each known child records `snapshot_freshness`: an exact retained snapshot is `stale` after checkpoint restore, a failed read or a later native event; it is `fresh` only after an exact `session/read` remains stable through subscription and pending-request reads; and it is `unknown` when no exact snapshot exists or a read races without a retained snapshot. Failed/missing reads retain the prior snapshot, exact child/item identity and last successful `last_refresh` as historical evidence; `last_refresh_attempt` records the current failure separately. An idle parent does not imply child completion.

## SDK delegation and recorded observations — bridge.7

"Use the whole official SDK" is a supply-chain decision, not a delegation of every protocol state machine. The bridge imports only the SDK's low-level public surface (`spawnMspConnection`, `Connection`, `checkServedFingerprint`, `MspError`); the facade's `Session`, `PendingCommandSet` and `GapFiller` are not in this module. The exact split:

| Concern | Owner |
| --- | --- |
| framing, generated types, request router, server-request plumbing | official SDK |
| command ID and Operation mapping | ELIOT bridge |
| durable checkpoint across bridge loss | ELIOT |
| same-ID replay semantics | native protocol + SDK contract, driven by ELIOT explicit reconcile |
| view gap fill | current ELIOT compact observation; SDK GapFiller is not automatically inherited |
| host-death durability classification | SDK handshake facts + ELIOT module owner/recovery |
| Task/Attempt/acceptance | ELIOT host |

The fixture selftest (`node selftest.mjs`) pins these derivations, runs the real checkpoint implementation, and drives the actual bridge with local fixture host/native processes for B5 protocol behavior. It does not start installed Muse or make model calls.

Bridge.6 records three observation facts that bridge.5 only implied:

- **`host_durability`** — the initialize handshake's `sessionDurability` declaration, classified per SS2.13.1: absent or `durable` reads `durable`; `ephemeral` reads `ephemeral`; any other value is **`unrecognized`** and never collapses into `durable` — an unrecognized profile guarantees nothing. The raw declared value is kept beside the classification.
- **`host_death`** — recorded when the native process exits or fails to spawn: the SS2.11 exit row (`cleanShutdown` is the only non-abnormal row; every other row, however tidy its name, left no `SessionEnd`), the exit evidence, and `session_survives`, which is true only for a `durable` profile. Recording a death invents no terminal turn or session event; a durable session's disposition stays native until explicit recovery, and an ephemeral session is gone with its host.
- **`gap_fill`** — each `view/gap` is recorded as an `unfilled` hole with its opaque `after`/`next` bracket and session ID verbatim (cursors are relayed, never parsed, ordered or merged), distinct from the `gaps` counter and `view_health`. This bridge does not splice-fill: it holds no live fold, so the inability is the recorded fact, and re-anchoring happens only through the existing read paths (`session/read`, `view/page` result reads).

One boundary stays explicit: transport EOF without process exit is not separately recorded. The SDK facade composes EOF into a host-death notification inside `MuseClient`, which this bridge does not use; on the bridge's transports, EOF surfaces as failed pending requests and `unknown` outcomes, and only the process exit produces a `host_death` record.

**Remaining recovery gaps:** crash after the native session is created but before its first checkpoint write completes (the root identity is now persisted immediately after `session/start`, before readback verdicts — the remaining window is the write itself); loss of an admitted host delivery before the bridge journal received it; unknown old native effects; native resume behavior with all vendor background children. Missing or corrupt owner/checkpoint identity is classified explicitly (`MODULE_OWNER_IDENTITY_MISSING`/`MODULE_OWNER_IDENTITY_INVALID`, a persisted `recovery-gap.json` in the module state directory, and `MODULE_OWNER_RECORD_MISSING`/`MODULE_OWNER_RECORD_CORRUPT`/`CHECKPOINT_CORRUPT` on the bridge side) and remains a recovery gap, never an adopted checkpoint, a force-reset or a fresh start. These remain explicit unknown/reconciliation cases, not reasons to fabricate no-effect or start fresh. This is controlled recovery of recorded work, not an autonomous repair service or live qualification of every crash window.

## Goal, configuration and replies

| Host method | Parameters beyond binding_id/generation | Application boundary |
| --- | --- | --- |
| agent.send | text, delivery:next_turn | Native admission, possibly queued. |
| agent.send | text, delivery:steer, expected_turn_id | Correction of the exact active turn, not another Task. |
| agent.reply | reply:{method,params} | approval/decide or userInput/answer, cancel, clarify with current native IDs. |
| agent.configure | settings:{reasoningEffort:max} | Durable native default for subsequent turns; explicit per-turn input remains. |
| agent.configure | settings:{model:{modelId,providerId}} | Waits for matching readback/event, not just ACK. |
| agent.configure | settings:{approvalMode:allowAll} | Effective mode in ACK; does not answer an existing question. |
| agent.goal | action:set/edit, objective | May begin work immediately; context and standing effort must already be set. |
| agent.goal | action:pause/resume/clear | Native continuation, not Task acceptance. |
| agent.refresh | Optional session_id of an observed child | Metadata, questions and subscription; no writer lease/resume. |
| agent.reconcile | operation_id of unresolved command | Targeted same-command reconciliation, not blanket retry. |

Use quoted JSON values through `swarm call METHOD --file FILE`. Configure one setter per Operation because the native setters are separate. Await standing effort before starting/resuming a goal. Pause/clear remain usable while new work is disabled and cancel only locally queued old goal starts. Already admitted effects still need native disposition.

MSP `approval/request` and `userInput/request` receive `{}` as presentation acknowledgement immediately; this is not approval or an answer. Actual decisions retain current choice/requirement IDs. Root goal state excludes child goal events. Ordered compact observations do not turn usage snapshots into summed token charges. Parent idle is not family completion.

## Task-specific children

Managers use native spawn. `attempt.bind_producer` records the observed exact session/run without starting another writer, consuming a result or accepting a Task.

```powershell
swarm task claim TASK --revision 1 --binding-id BINDING --generation 1
swarm family BINDING --generation 1
swarm --request-id bind-worker-1 task bind ATTEMPT --assignment worker-1 --session CHILD --turn TURN --observation-id OBSERVATION
```

Normal host/data-dir/credential options apply. Claim defaults to native_manager. A repeated child session has different assignments for different turns. Registration checks owner, binding, namespace and retained run evidence; an old terminal cannot finish a new run. Family pages reuse observation_id; end-of-page is not proof of complete native discovery. Unresolved registered runs hold their Task, not unrelated Tasks.

The bridge retains child snapshot/last-turn state across parent item updates, tags result summaries with source item/revision and observes turn/unqueued distinctly. Equal-revision replay of the same parent item is ignored; a changed item ID remains a distinct assignment. `resultReady` does not mean accepted. `subagent/readResult` changes native state and is never an observation helper.

## Result retrieval and complete local export

`results.mjs` reads a pinned native item revision. One agent.result obtains one byte page. Selector example:

```json
{"kind":"result","session_id":"PARENT_CONTAINING_ITEM","item_id":"OBSERVED_ITEM","item_revision":1}
```

Kinds: result = serialized structured subagent result; message = complete untruncated agentMessage; output/patch = the available stored reference with explicit output_ref. Optional before_cursor is the native backward-page cursor; expected_digest refers to the source, not a page hash. A missing revision is not replaced with a newer one.

```powershell
swarm --request-id page-1 result BINDING --generation 1 --file selector.json --offset 0 --length 65536
# Read operation.get; then result.details.artifact_ref:
swarm artifact get ARTIFACT_ID
swarm artifact read ARTIFACT_ID --offset 0 --length 65536
```

The native path uses view/page and item/readOutput. It does not call a model, resume a session, consume subagent/readResult or fetch arbitrary paths/URIs from result references. The host validates decoded length/ranges/EOF/SHA-256, publishes immutable bytes and records the result. File work is off the SQLite thread. Replayed matching pages cannot overwrite different retained content.

Source-result offsets and local-page offsets differ. Use next_offset_bytes with the same selector/source revision and a new request ID for the next native page. 64 KiB limits transport buffers, not the full result. Respect utf8/base64 encoding at split byte boundaries.

Host `artifact.assemble` takes ordered page_refs and optional whole-body expected_sha256. It verifies source/binding/revision identity, continuity, EOF and page digests, streams the whole artifact, and checks supplied native/expected whole digests. `artifact.parts` pages provenance. `artifact.read` verifies touched segments. `swarm artifact export ID --out FILE` streams one authenticated IPC session, verifies full SHA-256 and publishes without overwrite. Even an empty export verifies its actual backing artifact. Native items may still occupy SDK memory; transport paging does not promise otherwise.

See the [root workflow](../../README.md#complete-results-without-model-roundtrips). Deterministic local assembly may resume after host loss with its same request ID, not replay model input. Task submission/review/acceptance and CheckRunner already exist in host; file completeness is not semantic acceptance.

## Implementation and remaining work

Recorded-session recovery was saved before the chat interruption, through `8a84e6be`. Source restoration confirmed the exact 104-file tree. A subsequent real Linux process-owner/host invocation found `no such column: generation` in the changed-boot Operation update; `d385498b` corrects it to `binding_generation`, without a migration, dependency change or relaxed guard.

For **`d385498b00fe1a049c357613a7777cc4a1c83f72`**, [CI 36872022736](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36872022736) passed Windows/Linux formatting, warnings-denied Clippy, release builds and Muse syntax/SDK import. The downloaded corrected Linux artifact/source identity was verified separately from the pre-fix binary.

The bounded invocation passed 13 process-owner/host assertions: explicit fixture-process loss, replacement refusal until group departure, changed-boot admission, stale-link exclusion, historical open receipt retaining reconciling, normal-input refusal until resume, stale/disabled recovery rejection, current-boot resume readiness, serialized Node checkpoint publication/read and clean shutdown. The module selftest also drives the actual bridge over local fixture host/native processes for restored-child freshness, unknown server request handling and exact-turn stale rejection. These fixtures do not start Muse or call a model and do not qualify live resume, Max inference, complete native-child recovery or Windows-runtime behavior. No Cargo test was added.

Next code: direct OpenCode V2 through the same host contract. Qualify actual Muse resume/children on the installed runtime separately; do not reimplement the already saved local recovery path. Complete native family discovery, automatic service/module activation and all missing-identity recovery cases remain separate work. Do not reimplement already completed host results, submission, acceptance or CheckRunner. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) stays after the main code.

## Updates

Pins, verification, activation and rollback for this module are recorded in [UPDATE.md](UPDATE.md).
