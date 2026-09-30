# Muse SDK bridge — first native integration slice

This module contains executable code, not only dependency preparation. It uses the whole published `@muse-code/sdk` **1.3.0**, locked locally, and the reviewed [MSP schema](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/msp.d.ts). Native/Windows/model qualification is pending; syntax/import checks are not a model run.

## Ownership and implemented path

One independently started Node process owns one `muse serve` connection and its native descendants. The existing Rust host exposes `module.hello/next/outcome/observe` over authenticated local IPC. A module credential is scoped to a reserved binding/generation; it cannot call task acceptance or impersonate GM. No new listener, inference proxy or task database is added.

The host commits command admission before yielding native work. Input admissions are ordered; protocol replies do not wait for a model turn. Unknown admission is not blindly repeated on timeout or reconnect. Explicit reconciliation uses the original native command ID and exact retained payload. The bridge retains outcomes until the host commits them, sends the immutable Task snapshot, passes explicit per-turn reasoning effort, and reports model readback and exact native turn IDs separately from Task acceptance.

Host disconnect does not call SDK.close. The same live bridge reconnects with its boot/native identities. A different process confronting possibly live old work requires reconciliation rather than a new implicit executor. Explicitly terminating this bridge closes its own native connection, not an unrelated service.

**MSP presentation acknowledgement is not a decision.** For `approval/request` and `userInput/request`, the published `RequestReceipt` is `{}`: acknowledgement that a surface has or will present the question. The bridge retains the question and returns that receipt immediately. An actual decision is a separate `approval/decide` or `userInput/*` command carrying the current native identifiers. Generic JSON-RPC response bodies do not grant approval. The root's goal observation also excludes child goal events.

Native token deltas and full transcripts stay in Muse. The bridge keeps compact state, observed children, pending questions, recent exact turn terminals and an unsummed native usage snapshot. Family completeness is **partial**. No complete-family claim follows from root idle. Automatic bridge launching, crash-time native resume, complete family reconstruction, artifact retrieval, task acceptance and autonomous handoff after unknown effects remain incomplete. Do not label full C03 complete.

## Explicit first setup

1. Run `npm ci --ignore-scripts` here. This installs only the locked local SDK, not Muse or global packages; no login or model call occurs.
2. Select the installed native Muse executable and its existing auth. Windows uses `.exe`, not `.cmd`/`.bat`. Keep command and argv separate; use supported arguments for that installed binary. The module does not edit UAC or global vendor settings.
3. Add an enabled route to a private controller TOML (below), start `swarm --config <file> host`, and call `agent.open` with `{"lane_id":"MC","route":"muse-manager"}`. Save returned binding_id/generation.
4. As operator, run `swarm client-create muse-MC --role module --binding-id <binding> --generation 1 --out <private-credential.json>` with the same data-dir. Preserve the printed request ID for retries. Keep the credential out of model context.
5. Copy `module.example.json` outside Git. Set the host's pipe/socket printed at startup, private credential path, actual native executable/argv and matching moduleArtifactId. Independently run `node modules/muse/bridge.mjs --config <private-module.json>`. It spawns Muse only when it receives the reserved opening operation.
6. Read `agent.state`. To dispatch a Task, use `swarm call task.claim --file <params>` with `start_owner:"controller"`, binding_id and binding_generation, then `task.dispatch` with attempt_id/text. The short form also supports `task claim TASK --revision N --start-owner controller --binding-id BINDING --generation 1`. Readiness must come from native opening evidence, not a hand-edited DB.

```toml
[[routes]]
alias = 'muse-manager'
runtime = 'muse'
module_artifact_id = 'muse-sdk-1.3.0-bridge.3'
enabled = true
[routes.native_options]
workspaceRoot = 'C:\Projects\YourRepository'
modelId = 'muse-spark-1.3-contributor'
reasoningEffort = 'max'
approvalMode = 'allowAll'
```

These are adapter-owned camelCase fields. Model and approval mode must exist in the native runtime. Effort is explicit per turn; opening a session does not prove Max inference. Model readback mismatch is not silently accepted.

`agent.send` takes binding_id/generation/text and `delivery:"next_turn"` or `delivery:"steer"` with expected_turn_id. `agent.reply` takes binding_id/generation and `reply:{"method":"approval/decide","params":{...}}` or `userInput/answer|cancel|clarify`. Supply the exact installed-schema choice, requirement, question and session IDs from the pending question. The adapter checks the observed family and delegates schema validation to the native SDK/server. It exposes no arbitrary-method passthrough and does not guess substantive answers.

## Goal, configuration and reconciliation — bridge.2

All commands use `swarm call METHOD --file params.json` and the normal durable request ID. A response with `state:"queued"` is local admission; inspect its Operation and `agent.state` for native progress.

| Method | Params in addition to binding_id/generation | Native boundary |
| --- | --- | --- |
| `agent.configure` | `settings:{"reasoningEffort":"max"}` | One native setter; durable default applies to subsequent turns. Explicit input turns also carry effort. |
| `agent.configure` | `settings:{"model":{"modelId":"...","providerId":"..."}}` | Admission remains pending until matching model readback/event. No silent provider fallback. |
| `agent.configure` | `settings:{"approvalMode":"allowAll"}` | Effective mode from the native acknowledgement; applies to the next action, does not answer existing questions. |
| `agent.goal` | `action:"set"` or `"edit"`, `objective:"..."` | May start native work immediately. Complete objective and necessary context must already be supplied. |
| `agent.goal` | `action:"pause"`, `"resume"` or `"clear"` | Native continuation control, not Task acceptance or process termination. |
| `agent.refresh` | Optional `session_id` for an observed child | Read root or selected child metadata/pending questions; child observation is resubscribed from the read cursor, without writer lease or model prompt. |
| `agent.reconcile` | `operation_id:"unresolved-original-operation"` | Explicitly reconcile retained native work on the same live bridge; not a new Task or automatic retry loop. |

Configure one native setter per Operation: this runtime exposes separate setters, not a compound atomic configuration transaction. Before `goal set/edit/resume`, configure the standing reasoning default and wait for its applied outcome; a per-turn override from an earlier prompt does not configure future goal continuations. The immutable initial route remains history; later effective settings appear in native state/outcome evidence. Do not edit a running module in place; new modules/routes use their own artifact version.

`host.mode` with new_work disabled prevents new goal set/edit/resume and new input at dispatch. Pause/clear remain available and cancel older *locally queued* goal starts so priority pause is not followed by a stale queued resume. Already admitted effects require native disposition; no cancellation of unseen remote work is invented.

For a lost native admission reply, reconcile retains the original command ID, method and payload under the SDK's idempotency contract. Once admission is known, model configuration reconciliation only reads state. A correlated native turn event may resolve input admission without resubmission. Unresolved context lost with a bridge crash cannot be reconstructed by this first implementation: it returns an explicit unavailable/recovery result rather than starting a replacement agent. Session/start recovery is not covered by the retained command path yet.

The host now accepts unknown/accepted outcome refinement without replacing a known final result. A turn terminal already stored before the dispatch ACK is applied to its exact producer when the ACK arrives. Observation sequence prevents older snapshots overwriting newer state; legacy modules without sequence retain their weaker partial path. Within refresh, live events received during a read take precedence. Refresh covers the selected observed session, not a complete-family reconstruction or replay of lost history.

## Task-specific children and stable family pages — bridge.3

The manager still launches its children through native tools. `attempt.bind_producer` associates an already observed **session + turn** with one of its Attempts; it sends no prompt, does not consume a child result and does not accept the Task. Controller dispatch already binds its own root turn automatically. Bind its delegated children explicitly when they are part of that same Task, or to their separately claimed Tasks.

```powershell
swarm task claim TASK --revision 1 --binding-id BINDING --generation 1
swarm family BINDING --generation 1
# Use observation_id and the actual child session/turn from the returned evidence:
swarm --request-id bind-worker-1 task bind ATTEMPT --assignment worker-1 --session CHILD --turn TURN --observation-id OBSERVATION
swarm call attempt.get --file attempt.json
```

These examples require the normal data-dir/config/credential options for your host. `task claim` defaults to native_manager; this reserves work without sending a duplicate controller start. `assignment_id` remains stable for that Task-specific activation. Another turn is another assignment; an old completion cannot release it. Do not derive turn IDs from a filename, timestamp, parent model or the child session alone.

`agent.family` / `swarm family` read only SQLite. The first page returns `observation_id`; every later `--after` page requires that same `--observation-id`. The end of a retained list is not proof of complete native-family coverage. Missing observation is unknown, not zero children. Explicit old observations remain usable evidence even after a newer snapshot omits that turn.

Binding verifies owner, unreleased Attempt, exact binding/generation, native namespace, observed family membership and exact run evidence. Repeating a mapping can add later terminal evidence but cannot change its identity. Mapping late evidence is allowed while reconciling, and does not rewrite the Task snapshot after a revision change. Unresolved registered runs prevent release; unrelated Tasks on the same root keep working. Process termination and assignment acceptance remain separate operations.

Bridge.3 preserves a child's snapshot/last turn when a newer parent item arrives, includes native result summary and its **parent/item/revision** source, and handles `turn/unqueued` as cancelled-before-start. `resultReady` and result summary are not Task acceptance. Per the pinned [MSP schema](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/stable/msp.schema.json), `subagent/readResult` is state-changing; observing a summary never invokes it. Full result bodies/artifact retrieval remain pending.

Native observation buffers are still bounded/partial. Lost history and unregistered task descendants are not magically reconstructed by this mapping. A new artifact version is for a new module binding; do not overwrite a running bridge in place. No native run or Windows qualification is implied by compiling this slice.

## Checkpoint and next work

The earlier interrupted preparation (`0e3ccd6b`, `db45be62`) did not contain this bridge. Its recovered source/package input was Actions run 36696436986, artifact 11087808310, SHA-256 `b0e06843336290e685cd79376c709be4ac7ab79accf3cb09f9bd2beb1826ad52`. The source and lockfile now live in Git; temporary artifact retention is not a dependency authority.

The first integration was published in b2bd0211, with admission wakeup in 21502426 and corrected presentation-receipt semantics in 58055475. Bridge.3 task-specific run mapping and family pages are in 36b4f702. See the exact-commit CI run for compilation evidence; no live native session was invoked in these edits.

Next: complete crash recovery and full result retrieval, then direct OpenCode V2 on the same host contract. Exact-run producer mapping and retained family pages are implemented; native qualification is still pending. [OpenCodex Issue #1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) remains after the main controller implementation, not a prerequisite. Do not rewrite the existing core or restore historical briefs.
