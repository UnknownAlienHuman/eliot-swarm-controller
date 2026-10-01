# Muse SDK bridge — native integration

Executable module using the complete published `@muse-code/sdk` **1.3.0**, locked locally, and the reviewed [MSP schema](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/msp.d.ts). Current artifact: **`muse-sdk-1.3.0-bridge.4`**. Native/Windows/model qualification is pending; syntax/import checks are not model runs. Host-only result assembly does not require replacing this bridge.

## Ownership and implemented path

One independently started Node process owns a `muse serve` connection. The Rust host exposes `module.hello/next/outcome/observe/result` over authenticated local IPC. Credentials are scoped to one reserved binding/generation; the module cannot accept Tasks or impersonate GM. No extra service, inference proxy or task database is added.

The host commits admission before yielding native commands. Input admissions are ordered; protocol replies do not wait for model completion. Unknown admission is not blindly repeated on reconnect/timeout. Explicit reconciliation uses the original native command ID and retained payload. Outcomes remain until host acknowledgement; Task snapshots, requested effort, native readback and exact turn identities are distinct from Task acceptance.

Host disconnect does not call SDK.close. The same live bridge reconnects with its boot/native identities. A new process confronting possibly live old work requires recovery rather than implicitly creating another executor. Explicitly terminating the bridge closes its native connection, not an unrelated service.

**MSP presentation acknowledgement is not a decision.** `approval/request` and `userInput/request` receive the native `RequestReceipt` `{}` immediately, while the question is retained. Real answers use `approval/decide` or `userInput/*` with current IDs. Root goal observations exclude child goal events.

Token deltas/full transcripts stay native. Compact observations include children, pending questions, recent exact turn terminals and an unsummed usage snapshot. Family completeness remains partial. Automatic launching, bridge-process crash/resume, complete family reconstruction and autonomous handoff are unfinished. Task submission/acceptance and the fixed-source CheckRunner are implemented in the host; see the root README for current status. Do not label full C03 complete.

## Explicit setup

1. Run `npm ci --ignore-scripts` here. It installs the locked local SDK, not Muse/global packages; it does not log in or call a model.
2. Select the actual installed native executable and its existing auth. Windows uses `.exe`, not `.cmd`/`.bat`. Supply executable/argv separately, using supported arguments. No UAC/global settings are edited.
3. Enable a private controller route below, run `swarm --config <file> host`, then `agent.open` with `{"lane_id":"MC","route":"muse-manager"}`. Preserve binding_id/generation.
4. As operator, run `swarm client-create muse-MC --role module --binding-id BINDING --generation 1 --out <private-credential.json>` with the same data-dir. Preserve the request ID; keep the credential out of model context.
5. Copy `module.example.json` outside Git. Set the printed host endpoint, credential path, actual executable/argv and matching moduleArtifactId. Independently run `node modules/muse/bridge.mjs --config <private-module.json>`. Only the reserved opening operation starts Muse.
6. Inspect `agent.state`. Claim a controller-start Task with `task claim TASK --revision N --start-owner controller --binding-id BINDING --generation 1`; dispatch through `task.dispatch` with attempt_id/text. Do not hand-edit DB readiness.

```toml
[[routes]]
alias = 'muse-manager'
runtime = 'muse'
module_artifact_id = 'muse-sdk-1.3.0-bridge.4'
enabled = true
[routes.native_options]
workspaceRoot = 'C:\Projects\YourRepository'
modelId = 'muse-spark-1.3-contributor'
reasoningEffort = 'max'
approvalMode = 'allowAll'
```

These camelCase settings belong to this adapter. Model/approval options must exist in the native runtime. Max is explicit per turn; opening is not evidence of actual inference. Provider/model mismatch is not silently accepted. Use a new artifact/binding for a changed module; do not overwrite a running bridge.

## Goal, configuration and reconciliation

Use `swarm call METHOD --file params.json` with the normal durable request ID. Queued means local admission, not native application.

| Method | Additional parameters beyond binding_id/generation | Boundary |
| --- | --- | --- |
| `agent.send` | text, delivery:next_turn | Native input admission; may queue if busy. |
| `agent.send` | text, delivery:steer, expected_turn_id | Exact current-turn correction, not another Task. |
| `agent.reply` | reply:{method,params} | approval/decide or userInput/answer\|cancel\|clarify; native IDs required. |
| `agent.configure` | settings:{reasoningEffort:max} | One durable setter for subsequent turns, plus explicit per-turn input effort. |
| `agent.configure` | settings:{model:{modelId,providerId}} | Pending until matching readback/event. |
| `agent.configure` | settings:{approvalMode:allowAll} | Effective mode in native ACK; does not answer existing questions. |
| `agent.goal` | action:set\|edit, objective | Can start work immediately; complete objective/context must already be supplied. |
| `agent.goal` | action:pause\|resume\|clear | Native continuation, not acceptance or process termination. |
| `agent.refresh` | Optional session_id of an observed child | Metadata/pending questions and child subscription, without writer lease/resume. |
| `agent.reconcile` | operation_id of unresolved command | Explicit same-command reconciliation on the same live bridge. |

Actual requests are JSON with quoted strings. Configure one setter per Operation because native setters are separate. Before goal set/edit/resume, configure standing effort and wait for its applied outcome. A prior per-turn override does not configure future goal continuation. Current effective settings and immutable initial route are distinct.

New work disabled prevents goal starts/new inputs at dispatch. Goal pause/clear stay available and cancel old locally queued goal starts. Already admitted effects still require native disposition.

Lost native admission may be reconciled with the exact retained ID/payload. Once model-setting admission is known, reconciliation reads state instead of resending. Correlated turn evidence can settle input admission. Lost bridge memory/session-start recovery is not reconstructed by this path. Missing context produces explicit recovery/unavailable status, not a replacement model call.

Late outcomes refine unknown/accepted without overwriting terminal results. Exact turn terminal observed before dispatch ACK is applied to the producer later. Ordered observations reject stale projection updates; legacy unsequenced observations have weaker guarantees. Live events received during snapshot reads take precedence. Metadata refresh does not reconstruct the complete family.

## Task-specific children and stable family pages

Managers launch native children themselves. `attempt.bind_producer` associates an already observed session/run with one Attempt, without dispatching another prompt, consuming a result or accepting the Task. Root dispatch binds its own turn; delegated children are explicitly linked to that Task or their own claimed Tasks.

```powershell
swarm task claim TASK --revision 1 --binding-id BINDING --generation 1
swarm family BINDING --generation 1
swarm --request-id bind-worker-1 task bind ATTEMPT --assignment worker-1 --session CHILD --turn TURN --observation-id OBSERVATION
swarm call attempt.get --file attempt.json
```

Supply normal data-dir/config/credential options. `task claim` defaults to native_manager, so there is no second initial controller prompt. Assignment identity stays stable for that activation; another turn is another assignment. Do not infer run IDs from filenames, timestamps, model names or session IDs alone.

Family reads use SQLite only. Preserve observation_id for subsequent pages; the end of this retained list is not proof of full native discovery. Missing observations are unknown, not zero children. Older observations can support an exact run omitted from the latest buffer.

Producer binding checks owner, unreleased Attempt, binding/generation, namespace, family membership and run evidence. Repetition can add terminal evidence but cannot replace identity. Late mapping during reconciliation does not rewrite the Task snapshot. Unresolved registered runs prevent release; unrelated Tasks keep working.

The bridge preserves child snapshots/last turns across parent item updates, records parent/item/revision provenance of result summaries, and recognizes `turn/unqueued` as cancelled-before-start. `resultReady` is not acceptance. `subagent/readResult` is state-changing in the pinned schema and is never used for observation.

## Result retrieval — bridge.4

Implemented in `results.mjs`, `src/artifacts.rs`, `src/store/results.rs` and their host/CLI call sites. One `agent.result` requests one source page. It reads a pinned item revision, not the latest answer with a similar name.

Create a selector file, replacing all identifiers and revision with observed values:

```json
{
  "kind": "result",
  "session_id": "PARENT_SESSION_CONTAINING_THE_SUBAGENT_ITEM",
  "item_id": "OBSERVED_SUBAGENT_ITEM",
  "item_revision": 1
}
```

| Selector kind | Selected native data |
| --- | --- |
| result | Subagent result object, including structuredData/artifactRefs/evidenceRefs, serialized as canonical JSON. |
| message | Complete agentMessage text; a truncated native message is rejected. |
| output | Available stored outputRef; include exact output_ref ID. |
| patch | Available stored patchRef; include exact output_ref ID. |

Optional before_cursor is an opaque backward-page cursor. Optional expected_digest must match the reported source digest, not the individual page hash. The session must be the root or an observed family member. Exact revision absence does not authorize substitution of a newer revision.

```powershell
swarm --request-id result-page-1 result BINDING --generation 1 --file selector.json --offset 0 --length 65536
# operation.json contains the returned operation_id:
swarm call operation.get --file operation.json
# Once settled with result.details.artifact_ref:
swarm artifact get ARTIFACT_ID
swarm artifact read ARTIFACT_ID --offset 0 --length 65536
```

Use normal host/data-dir/credential options. Muse reads `view/page` to identify the exact item; stored output uses `item/readOutput`. Neither path calls a model, resumes a session, consumes `subagent/readResult`, or follows arbitrary URIs/files from result references.

The live bridge retains unacknowledged pages. The host validates decoded byte length/range/EOF and SHA-256, publishes immutable bytes without overwrite, then registers the artifact and settles the Operation. File I/O stays off the DB thread. Lost acknowledgements can replay the retained page; different bytes/provenance cannot replace it.

Result offset addresses the native source body; artifact-read offset addresses one local page. Use `result.details.next_offset_bytes` for the following native page with a new request ID and the same selector/source identity. The 64 KiB transfer bound is not a full-result size limit. UTF-8 split across ranges returns base64 when necessary; honor the returned encoding.

## Assemble and export retained results

Host-side `artifact.assemble`, implemented in `src/artifacts/assembly.rs` and `src/store/assembly.rs`, combines registered pages without contacting Muse. Pass an ordered `page_refs` list and optional expected whole-body SHA-256 through `swarm artifact assemble --file pages.json`.

Assembly verifies exact source/binding/selector identity, continuous coverage, page digests and EOF; it rejects missing, overlapping, reordered or mixed-source pages. Bytes stream into a new immutable artifact with a computed whole digest, verified against available native SHA-256 and explicit expectations. Missing native digests remain unverified. Original page evidence is retained.

`artifact.parts` pages the provenance list. `artifact.read` addresses the assembled file and verifies touched segments. `swarm artifact export ID --out FILE` uses one authenticated IPC connection, verifies the complete SHA-256 and publishes the destination without overwrite. Even an empty export reads and checks its backing artifact; the destination path is local to the CLI, never a host/model command.

See the [root README](../../README.md#complete-results-without-model-roundtrips) for the full workflow. Known failed assembly returns a saved failure via operation.get. After host interruption, the same request ID can recover deterministic local assembly without repeating a native prompt. Automatic fetching of missing pages and arbitrary reference-target retrieval remain unfinished. Semantic Task acceptance is a separate implemented host decision, not an effect of assembly. Native item materialization can still consume SDK memory even when our content pages are bounded.

## Implementation checkpoint — recovery verified 2026-10-01

The interrupted continuation after `d947cc4c` did not publish another commit or workflow run. The latest compiled code remains `611f7c11c70d6d9be3c24e8f49d27c04e8816589`; [CI 36832982495](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36832982495) was re-read and confirms Windows/Linux formatting, warnings-denied Clippy, Muse syntax/SDK import and release builds. Recovery did not rerun those checks or invoke a native model.

The retained Linux artifact `11147996922` passed SHA-256/ZIP verification; its 101 source files reconstructed exact tree `f3ff96f50db5287e62362f1f82870a47b4f08c2d`. Available development archives contained older source baselines, not new Muse recovery code. No unpublished continuation was found in the accessible workspace; this is not a claim about files outside it. The previous real-command invocation receipt matches the restored binary and remains historical evidence, not a new run.

Host-side paging, assembly/export, Task submission/review/acceptance, fixed-source checks and recorded check-worker cancellation/recovery are already implemented. Resume from the current `main`; do not reapply old acceptance patches, repeat SDK preparation or reimplement those consumers. Check-worker recovery does not provide Muse bridge-process recovery.

**Next code boundary:** `bridge.mjs` and `control.mjs`, coordinated with `src/store/runtime.rs`: recover the known native session and unresolved operations after loss of the bridge process without creating a duplicate executor or replaying a possibly admitted prompt. Preserve native ownership, explicit unknown outcomes and subscription/effort routing. Then connect direct OpenCode V2 through the same host contract. Live Muse/Max and Windows native lifecycle remain unqualified. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) stays after the main code, not a prerequisite.
