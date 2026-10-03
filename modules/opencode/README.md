# Direct OpenCode V2 — `eliot-opencode-v2.http.1`

Built-in Rust adapter for an **already running, externally owned** OpenCode V2 HTTP service. No Node bridge, CLI invocation, process launch, service restart, hidden inference fallback or extra task store. Disabling the route does not terminate native work.

## Contract and exact scope

Implements the HTTP/inbox/readback part of C04: native session creation, immutable Task delivery, next-turn input, family snapshots, addressed form/permission replies, durable instruction entries, exact session-agent and route-model controls, controller-recorded goal, refresh, read-only reconciliation, scoped projected-result export, native per-turn patch export, exact detached tool-file export and durable input/execution disposition. One connection pool and volatile SSE reader serve bindings with the same configured `service_id`. Aliases must share that namespace, connection file and exact version. Do not assign different service IDs/files to the same service: physical endpoint aliases are not automatically discovered.

Canonical requirements: [architecture §8](../../docs/agent_swarm.md), [implementation C04](../../docs/agent_swarm.implementation-v6.md), [module contract §§4–7](../../docs/agent_swarm.module-contract-v2.md). Wire contract reviewed against the [official V2 API](https://opencode.ai/v2/docs/api) and [OpenAPI document](https://opencode.ai/v2/openapi.json), captured 2026-10-01. These documents move; they are not proof that a particular installed server has been qualified.

## Explicit configuration

Keep the shipped route disabled until these values are filled for the intended installation. Use absolute native paths for `connection_file` and `directory`, an exact `/api/info` version, and the exact provider/model/**variant** from the installed model catalog. The adapter refuses missing/disabled variants and verifies resolved location and native settings; it never silently downgrades reasoning or changes provider. A fallback to another workspace is rejected. Native path normalization is accepted only for the same local directory; a controller-created root may not become a child or fork.

```toml
[[routes]]
alias = 'opencode-manager'
runtime = 'opencode_v2'
module_artifact_id = 'eliot-opencode-v2.http.1'
enabled = false
[routes.native_options]
service_id = 'my-local-opencode-store'
connection_file = 'C:\SwarmPrivate\opencode-connection.json'
expected_version = 'REPLACE_WITH_EXACT_INSTALLED_VERSION'
directory = 'C:\Projects\YourRepository'
[routes.native_options.model]
id = 'REPLACE_WITH_NATIVE_MODEL_ID'
providerID = 'REPLACE_WITH_NATIVE_PROVIDER_ID'
variant = 'REPLACE_WITH_EXACT_VARIANT'
```

The connection file is **ELIOT's own explicit record**, not an invented interpretation of OpenCode's `service.json`. Populate it from the service owner's known configuration. It is a bounded regular JSON file; links and pipes are rejected. Store it outside source control and restrict its OS permissions to the intended user.

```json
{
  "schema_version": 1,
  "endpoint": "http://127.0.0.1:12345",
  "pid": 1234,
  "username": "REPLACE_WITH_CONFIGURED_BASIC_USER",
  "password": "REPLACE_WITH_CONFIGURED_BASIC_PASSWORD"
}
```

Port, PID and credentials above are placeholders, not defaults. Credentials are read into the HTTP client, not copied into bindings, operations, Task text or diagnostic errors. Only loopback HTTP origins are accepted; redirects, proxies, URL credentials and automatic HTTP retries are disabled. `/api/info` must match the recorded PID and selected version. This is a same-user trusted-service boundary, **not** an OS sandbox or cryptographic proof that a process with a recycled PID has the same identity. The stable `service_id` must name the same native store across an operator-managed restart.

No native connection occurs merely to list routes or read host status. After explicit `agent.open` admission, the host attaches its built-in worker. No `client.register` module token or `module-run` process is needed for this adapter; those are Muse bridge setup steps. Existing controller credentials and request IDs still apply. Read an operation's result before using its returned binding/generation.

## Optional pinned service owner

The adapter above still attaches to an already-running service. For an explicitly operator-managed local installation, `serve.mjs` composes the official `@opencode/server` **2.0.7** library with its pinned Effect/Node platform packages and runs it under **Bun 1.4.0**. It binds authenticated HTTP only to `127.0.0.1`, enables native `events.persist`, and puts the database, XDG roots, config, temporary files, owner record and connection record below one explicit private state directory. Give that directory and its password file restrictive OS permissions before launch; the launcher refuses reparse paths and does not copy global configuration, provider credentials, history or databases. It writes the generated connection record for the controller at `connection.json`; treat that file as a secret.

Install only the locked module dependencies and run the exact bundled Bun executable selected by the operator:

```powershell
npm ci --ignore-scripts
& '<absolute-path-to-bun-1.4.0.exe>' .\serve.mjs `
  --state-root '<absolute-private-state-directory>' `
  --password-file '<absolute-private-state-directory>\server.password' `
  --port 12345 `
  --model-catalog refresh
```

`refresh` is the live default: OpenCode loads its bundled model snapshot and enables its public Models.dev metadata refresh. That refresh is metadata traffic only; this option does not request inference or qualify provider credentials. The explicit `--model-catalog offline` mode disables both snapshot and fetch for deterministic, credential-free persistence checks. The smoke command below uses only that offline mode, creates a model-free session, verifies the exact durable `session.created` event before and after restarting the owned service, checks the SQLite row read-only, and requires clean observed process exits:

```powershell
node .\selftest.mjs --bun '<absolute-path-to-bun-1.4.0.exe>'
```

Provider connection APIs are location-scoped. Use the same explicit
`location[directory]` query pointing at the private workspace for
`GET /api/integration`, the exact integration GET, credential connection and
catalog reads. The list GET waits for native plugin activation; verify the
exact integration and its key method before connecting an authorized key with
`POST /api/integration/<exact-id>/connect/key`. Omitting the location selects
the native process working directory and can return 404 even when the
integration exists in the intended workspace. Keep keys and connection headers
out of saved/public diagnostics.

A successful smoke verifies service startup and event persistence only. Before any model request, read back the exact provider, model ID and variant from the running native catalog; a bundled snapshot's contents do not qualify a route. Keep the OpenCode version check, catalog identity and actual inference qualification as separate evidence. Stop the owner with Ctrl+C or its explicitly supervised stdin EOF. Before restart, the launcher requires matching owner/receipt schema, nonce, exact PID and state paths; both records must report `runtime_disposed=true` and listener closure, the connection record must be absent, and a non-destructive liveness probe must find the recorded PID gone. The smoke supervisor observes the exact child exit and checks its code against the receipt. A recorded exit-code field alone is not proof that the owner process exited. An active or unconfirmed owner is never killed or overwritten. Preserve the database directory across normal service restarts.

## Durable session configuration

`agent.configure` supports one bounded native configuration unit: OpenCode's complete durable instruction-entry backend. ELIOT keeps the selected [`existing-control-bundle` donor boundary](../../docs/agent_swarm.donors-20260929.toml) for direct HTTP and delegates storage, replacement, removal and next-step rendering to the native backend rather than copying that state machine into Rust. The complete Atlas donor remains unchanged and continues to scrub retained native question/diagnostic copies. Native semantics were reviewed at OpenCode `4c0d0ff4`: [instruction-entry backend](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session/instruction-entry.ts), [key/value boundary](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/schema/src/instruction-entry.ts), and [HTTP routes](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/protocol/src/groups/session.ts).

Only controller-owned keys under `eliot.*` are accepted. One Operation changes one key:

```json
{
  "client_request_id": "oc-policy-1",
  "binding_id": "BINDING_ID",
  "generation": 1,
  "settings": {
    "instruction_entry": {
      "action": "put",
      "key": "eliot.policy",
      "value": {"review_before_submit": true}
    }
  }
}
```

```json
{
  "client_request_id": "oc-policy-remove-1",
  "binding_id": "BINDING_ID",
  "generation": 1,
  "settings": {
    "instruction_entry": {
      "action": "remove",
      "key": "eliot.policy"
    }
  }
}
```

Invoke these through `swarm call agent.configure --file REQUEST.json`. Keys are limited to 128 ASCII bytes and native lowercase alphanumeric/dot/underscore/hyphen syntax. Values use the native 256 KiB JSON boundary. Put permits every JSON value, including `null`; remove does not accept `value`. Credentials and access tokens do not belong in instruction entries because the requested value is retained in the Operation input.

A matching pre-existing value is a valid no-op. Otherwise the adapter sends exactly one native PUT or DELETE and requires an exact subsequent list readback before returning `native_configuration_applied`. The result records the key, desired digest, full entry-list revision, controller-owned settings revision, scope, evidence and actual application boundary. OpenCode stores the value immediately and announces a change/removal to the model at the **next step boundary**; this Operation does not itself start model work.

A lost mutation response becomes `outcome_unknown`. Reconciliation repeats only verified GET/readback and never repeats PUT or DELETE. Current snapshots retain at most 128 controller-owned keys and value digests, not duplicate values; overflow is marked incomplete. An unsupported experimental endpoint, changed binding/model/location, duplicate key, read gap or mismatching value remains explicit. Provider/model/variant, agent, effort and arbitrary native settings are not silently mapped onto this control.

### Exact session-agent selection

The same `agent.configure` method supports one separate native configuration unit: the agent used by subsequent provider turns. One Operation must select either `instruction_entry` or `agent`; combining them is rejected.

```json
{
  "client_request_id": "oc-switch-agent-1",
  "binding_id": "BINDING_ID",
  "generation": 1,
  "settings": {
    "agent": {
      "id": "REPLACE_WITH_EXACT_NATIVE_AGENT_ID"
    }
  }
}
```

Use `swarm call agent.configure --file modules/opencode/switch-agent.example.json`. The adapter reads the complete native agent catalog for the verified route location, rejects duplicate or absent IDs, and never falls back to another agent. An agent-level model override is accepted only when it exactly equals the route's pinned provider/model/variant; an omitted override keeps the route model. This prevents agent selection from silently becoming a model or billing-route change.

A matching pre-existing session agent is an exact no-op. Otherwise the adapter sends one `POST /api/session/{sessionID}/agent`, then requires repeated equal catalog and `session.get` projections proving the selected ID and unchanged agent definition. The typed result records the agent ID, definition/catalog digests, mode, hidden flag, application boundary `subsequent_provider_turn`, and whether the definition carried the same exact model override. Raw system prompts, request settings and permissions are not copied into controller state.

A lost response becomes `outcome_unknown`; reconciliation performs only catalog/session GETs and never repeats the switch. Family snapshots retain the selected agent plus definition/catalog revisions. A later external switch or definition reload invalidates a configure prerequisite for that agent rather than silently starting under changed instructions. This control does not implement effort changes or arbitrary agent-definition mutation; goal set/edit/pause/resume/clear are the separate controller-recorded control below. Native semantics were reviewed at OpenCode `4c0d0ff4`: [session switch routes](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/protocol/src/groups/session.ts), [session projection](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/schema/src/session.ts), [agent catalog routes](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/protocol/src/groups/agent.ts), and [agent schema](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/schema/src/agent.ts).

### Exact route-model control

A third `agent.configure` unit explicitly selects the session model used by subsequent provider turns. The request must contain all three fields and must exactly equal the binding route's pinned provider/model/variant:

```json
{
  "client_request_id": "oc-switch-model-1",
  "binding_id": "BINDING_ID",
  "generation": 1,
  "settings": {
    "model": {
      "id": "REPLACE_WITH_ROUTE_MODEL_ID",
      "providerID": "REPLACE_WITH_ROUTE_PROVIDER_ID",
      "variant": "REPLACE_WITH_ROUTE_VARIANT"
    }
  }
}
```

Use `swarm call agent.configure --file modules/opencode/switch-model.example.json`. This is a repairable, explicit model setup step, not an arbitrary model picker. A different provider, model or variant is rejected before native admission and requires a new route configuration/binding. That preserves the route's model and billing identity instead of silently moving an existing conversation.

The adapter reads the location-scoped native model catalog, requires the exact model and variant to exist and the model to be enabled, and validates the complete projected definition. A matching session model is an exact no-op. Otherwise it sends one `POST /api/session/{sessionID}/model`, then requires two equal catalog/session projections proving the exact model and unchanged definition/variant. The typed result records model/catalog/variant digests, enabled/status evidence, the settings revision and application boundary `subsequent_provider_turn`; raw provider options, headers, body overlays, costs and limits are not duplicated into controller state.

A lost mutation response becomes `outcome_unknown`. Reconciliation performs only `session.get` plus location-scoped `model.list` reads and never repeats the switch. Family snapshots retain compact selected-model evidence. A later external model switch, catalog replacement, variant change or disabling invalidates a prerequisite instead of starting input under a changed model. Ordinary send/reply/result paths also verify the exact route model, while this model-control path deliberately uses identity/location readback so it can repair detected model drift. An active agent whose own model override differs from the route remains invalid; model repair does not legitimize that agent. Native semantics were reviewed at OpenCode `4c0d0ff4`: [session model route](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/protocol/src/groups/session.ts), [model catalog route](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/protocol/src/groups/model.ts), [session projection](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/schema/src/session.ts), [model/reference schema](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/schema/src/model.ts), and [native switch/no-op semantics](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session/session.ts).

## Explicit configure → input prerequisite

`prerequisite_operation_id` implements the short persisted setup sequence required by the module contract. It is accepted on another `agent.configure`, on `agent.send` with `delivery=next_turn`, on `task.dispatch`, and on `agent.goal` (see the goal section below). The value must name one earlier `agent.configure` Operation on the exact same binding generation. Each Operation has at most one direct predecessor; a setup sequence can name the preceding step, but this is not a general DAG or workflow language. Submission order alone never creates a dependency.

A dependent request may be durably queued while its prerequisite is still `queued`, `sending`, `native_accepted` or `outcome_unknown`. Immediately before `queued → sending`, Store rechecks the prerequisite inside the same SQLite transaction that admits the native send. The check requires the saved OpenCode configuration OperationContract, the original typed target/digest, an `applied` RuntimeOutcome for the same native root and service scope, the configuration kind's exact native readback evidence, and valid settings/catalog/definition revisions. A plain ACK, `state=settled` without the typed result, rejection, cancellation, failure or unknown outcome never satisfies the dependency.

The relevant setting must still be effective. A later configure in the same scope—same instruction key, session-agent selection or session-model selection—either remains a wait or supersedes the old setup; a later exact restoration can satisfy the requirement again. Unrelated configuration scopes do not invalidate the dependency. A complete newer native snapshot can independently reconfirm or invalidate the referenced setting. This is the documented per-binding prepare/admission barrier: conflicting configure/input mutations cannot interleave before native input admission, while replies, readback/reconciliation/result operations, stop controls and other bindings continue independently. OpenCode exact-turn steer remains unsupported for its separate atomic-guard reason and cannot carry a setup prerequisite.

Example dependent send, after reading the applied configure Operation ID:

```json
{
  "client_request_id": "oc-send-after-policy-1",
  "binding_id": "BINDING_ID",
  "generation": 1,
  "text": "Continue under the applied controller policy.",
  "delivery": "next_turn",
  "prerequisite_operation_id": "CONFIGURE_OPERATION_ID"
}
```

Use `swarm call agent.send --file modules/opencode/send-after-configuration.example.json`. Initial controller-owned delivery uses the same field in `task.dispatch`; invoke the generic `swarm call task.dispatch --file REQUEST.json` when that explicit setup chain is required. `operation.get` exposes both `prerequisite_operation_id` and the effective OperationContract. A stale or failed prerequisite rejects the dependent Operation before any native input is sent; it is not silently rebound to another configure. The barrier proves configuration at the admission boundary, not frozen context for every later model step, Task acceptance or family completion.

## Controller-recorded goal — not a native goal API

OpenCode V2 has **no native goal API**: at the reviewed pin `4c0d0ff4` the session routes and `Session.Info` schema contain no goal endpoint, field or event, and the local runtime matrix records no durable goal API for this runtime. `agent.goal` therefore implements the architecture's explicitly permitted controller fallback: the goal is a controller-owned durable instruction entry with key `eliot.goal`, stored through the same native backend as the instruction-entry configuration above:

```json
{"objective": "<text>", "status": "active|paused", "revision": 3, "updated_by_operation_id": "<operation_id>"}
```

```json
{
  "client_request_id": "oc-goal-1",
  "binding_id": "BINDING_ID",
  "generation": 1,
  "action": "set",
  "objective": "REPLACE_WITH_GOAL_OBJECTIVE"
}
```

Use `swarm call agent.goal --file modules/opencode/goal.example.json`. One Operation changes only this entry. `set`/`edit` write the objective (bounded to 32 KiB, well below the native entry limit) and, when the recorded content actually changes and the goal is active, admit exactly one activation prompt with the deterministic input ID and a prompt text that names itself a controller-recorded goal. `resume` reactivates a paused record and admits one prompt. `pause` changes only the recorded status: it does not interrupt the current native turn, does not touch the inbox and is not Task acceptance. `clear` deletes the entry. Matching pre-existing state is an exact no-op with no PUT, DELETE or prompt. `edit`/`pause`/`resume` without a record are rejected with `NATIVE_GOAL_ABSENT` before any mutation.

The typed result reports `completion_condition: "native_goal_recorded"` — never `native_goal_admitted`, which would claim a native goal that does not exist — plus the action, status, revision, objective digest, entry-list and settings revisions, `continuation_owner: "controller_record"` and `native_goal_api: false`. The objective itself is not copied into snapshot state: family snapshots expose a separate `goal_configuration` axis with presence, status, revision and digest only, so goal remains an independent observation axis rather than part of the configuration or execution state. `agent.goal` accepts `prerequisite_operation_id` naming an applied `agent.configure` on the same binding generation, so an `open → configure(applied) → goal` sequence is gated by typed applied evidence; a goal Operation can never itself be a prerequisite.

A lost PUT/DELETE or activation-prompt response becomes `outcome_unknown`. Reconciliation repeats only GET/readback and never repeats the entry write or the prompt, including after host restart. Because the completion condition is the record, reconciliation can settle a `set` whose entry landed but whose activation prompt was never sent; its details then honestly report `activation_input_id: null`, `record_applied: true`, `activation_admitted: false` and `activation_execution_started: false`, and an operator `resume` admits the activation. A record written by another operation is not this Operation's evidence and stays unknown.

The goal result keeps the activation facts split, because they are different native facts. `record_applied` states that the exact goal record state is proven by readback. `activation_input_id` and `activation_admitted` state that this Operation's activation input provably exists — by its admission receipt at execution time, or at reconciliation by exact inbox/message readback or by the durable log's exact `session.inbox.enqueued` event. `activation_execution_started` and `activation_execution_ref` state whether model execution provably started for that input: they are derived **only** from the durable execution log, where the input's `inbox.delivered` event correlates to the `session.execution.started` event active at delivery (the started event of that serialized busy period, whose ref is reported; one busy period may serve several inputs delivered while it is active). An admitted input alone never proves a start — the former single `model_work_started` field, which inferred a start from admission, is removed. Delivered-but-no-start-event is uncertainty, reported as `activation_execution_started: false`, never as a start. Execution evidence gathered at reconciliation is additive observation on the settled Operation; it never moves the `native_goal_recorded` completion boundary, and none of these fields is Task acceptance or family completion.

There is **no automatic continuation**: when a native turn terminates, the controller does not re-prompt toward the goal, and native idle is never read as goal completion. Continuation has exactly one owner — this record — and no controller timer duplicates it.

## Addressed native background — `session.background`

`agent.background` maps to the native operation the implementation review names `native.opencode.background_foreground_tools`: `POST /api/session/{sessionID}/background` (identifier `session.background`), reviewed at OpenCode `4c0d0ff4` ([protocol group](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/protocol/src/groups/session.ts), [core `Session.background`](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session.ts), [Job service](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/job.ts)). The endpoint declares no payload, answers `204 NoContent`, and is described natively as moving active foreground backgroundable tools into background observation; idle requests are a no-op. Eligibility is entirely native: the Job service moves the jobs currently blocking the session, and only a call that moved at least one job admits a durable synthetic notice naming the moved work. The adapter therefore offers no tool selector and invents no expected-turn guard. Input is `{session_id?}` — the binding root, or one owned family member proven through the native parent chain after binding ownership/generation verification.

Evidence ladder: the POST alone is never success. Execute reads the current foreground tool inventory first (running/streaming tool parts on the newest projected message page; tool inputs are never copied into the result), sends exactly one background POST, then re-reads. A synthetic notice that was absent before the POST proves the native boundary and names the backgrounded work (`completion_condition:native_foreground_tools_backgrounded`). No notice over an empty pre-read inventory is the documented native idle no-op (`native_background_noop`). No notice while foreground tools were observed running stays `outcome_unknown`: native job state is not directly readable, so the adapter does not guess the tools were backgroundable jobs. A lost POST response is `outcome_unknown` and reconciles by GET/readback only — an observed notice settles the operation, its absence proves nothing, and the POST is never replayed. The native notice carries no operation marker, so reconciliation attributes an observed notice to the unresolved operation by session presence and the result records that attribution basis. If the session verifies but the route itself is rejected, the operation reports `UNSUPPORTED_CAPABILITY` rather than guessing another endpoint shape. Contract revision `opencode-background-v1`.

This operation is distinct from steer (it claims nothing about queued input), from a form/permission reply, from interrupt (nothing is stopped — backgrounded work continues), from declaring the session idle, and from Task cancellation. It is never issued based only on age.

Qualification: **implemented, fixture-checked only.** In-process fixture tests cover the program §13 scenario (a foreground backgroundable tool holds the manager; after backgrounding the child continues, with no interrupt and no cancellation), the idle no-op, the unsupported-route path, lost-response reconciliation without replay, and owned-member versus foreign-session targeting. Live qualification against an installed OpenCode service remains outstanding: until an operator runs it against the installed build, the endpoint's presence on that build is established per operation by the gating above, not assumed.

## Delivery, observation and recovery

A native session has a deterministic controller-owned ID plus binding/generation/operation metadata. Initial creation verifies the model and location. Task dispatch includes the frozen Task snapshot. Each admitted input has a deterministic native message ID. **A prompt response confirms inbox admission, not an executed turn or finished Task.** The retained producer has `native_input_id`, `admission_kind=native_inbox`, `disposition=admitted`; it does not acquire a fabricated turn/run ID.

Lost creation and prompt replies become `outcome_unknown`. Readback checks the exact owned session, queued inbox or delivered user-message ID and original content/metadata, rejecting additional native attachments. It never repeats the POST, including after host restart or while new-work admission is disabled. Unproven outcomes remain unknown. A changed service, model, binding owner or foreign child cannot silently receive the command.

`agent.send` currently supports `delivery=next_turn` only. Exact-turn steer is rejected: a preflight read followed by a steer without an atomic expected-turn guard would still race. Forms and permissions require the exact native request ID, the current pending-body fingerprint and verified ancestry under the binding's owned root. A native answer ACK is not Task acceptance.

The shared SSE reader uses the complete `eventsource-stream` crate. Events invalidate a compact view; they do not become replayable history or terminal evidence. Disconnects, malformed events, native stream-failure envelopes and bounded stream recycling retain a gap. Reconnect issues GET only. There is **no Last-Event-ID replay guarantee**. Notifications are coalesced; periodic bounded GET readback supplements the volatile stream.

Family enumeration uses parent-filtered pagination and bounded ancestry checks. Read failures retain previously observed children as stale rather than deleting them. Root inactivity is not family idleness; unknown active-map or pagination schemas are explicit gaps. Even a completed enumeration is non-atomic and retains `family_completeness=partial`. Native question bodies are scrubbed with the complete pinned Atlas redaction donor before persistence; identities and content fingerprints remain outside the scrubbed payload. Pattern detection is not a guarantee that every possible secret format is recognized.

Per-response body cap: 4 MiB; connection file: 64 KiB; family: 256 retained sessions; pending requests: 128; one family readback: 20 seconds. SSE connections recycle after at most 16 MiB of input, including an unterminated frame. These limits bound observation, never terminate a native agent or imply success. Reaching a bound records incomplete coverage.

## Durable execution evidence

Before `agent.open` is reported as ready, and before a Task/input, goal activation, form/permission reply, or background request can issue a native write that starts or resumes provider work, the adapter reads the exact root's non-following durable log and requires a clean `log.synced` watermark containing that root's `session.created` event with the expected binding, generation, parent and pinned model. A synced empty log is not a successful capability check: open remains `outcome_unknown` with the retained native root and scope plus `DURABLE_EVENT_PERSISTENCE_UNAVAILABLE`; later inference-producing writes are blocked before POST. The probe is evidence-based per root and does not infer support from a version string.

OpenCode `v2.0.7`'s Bus defaults `events.persist` to false, advances its sequence without inserting events when persistence is disabled, and the standard `serve` path does not expose an option to enable it. As a result, its ordinary synced log may have a high-water mark and no retained `session.created` event. The installed service needs an operator-supported way to retain event history before this adapter can become ready. See the pinned upstream [Bus configuration and publish path](https://github.com/anomalyco/opencode/blob/v2.0.7/packages/core/src/bus.ts#L174-L203) and [standard server process options](https://github.com/anomalyco/opencode/blob/v2.0.7/packages/cli/src/server-process.ts#L83-L122).

The separate `GET /api/experimental/session/{sessionID}/log?follow=false` reader correlates the exact inbox ID and original payload with `session.inbox.enqueued`, `session.inbox.delivered` and the serialized `session.execution.started` / terminal lifecycle. It does not reuse the volatile `/api/event` feed or projected idle messages. The native **started event ID** is retained with `native_run_id_kind=execution_started_event`: this identifies a busy period, not an exclusive model turn. Multiple queued or steered inputs can legitimately share it.

Inspect `swarm operation INPUT_OPERATION_ID`: `native_refs.input_execution` contains the native namespace/version, event IDs, sequences, body hashes, captured log watermark and disposition. A dispatch's exact producer gains the same identity and terminal observation. `completed`, `failed` and deliberate native cancellation settle that producer; cancellation before delivery uses the exact inbox cancellation event without inventing a run. A lost prompt ACK can first be resolved from durable admission, even when completion arrived before readback. The immutable admission receipt still describes admission only.

The reader requires verified root origin/settings and a `log.synced` watermark followed by clean end-of-stream before publishing new execution evidence. Sequence positions must increase but need not be contiguous: the native aggregate also contains other event types. A bounded checkpoint retains only correlation state, not prompts, model output or native error bodies. Continuation uses an actual preceding native sequence and replays the last checked event, verifying its ID and hash. Missing/changed anchors, newly appearing events below an already observed watermark, unexpected lifecycle versions and unknown event types remain explicit read gaps. This relies on the native append-only log and stable configured store; it is not protection against arbitrary administrator rewrites of both stores.

At most one execution-log GET task per service runs independently of command delivery. Bindings and input Operations rotate fairly, with at most one start per binding per five seconds. Each read is bounded to 8 MiB, 8,192 events, a 10-second HTTP timeout and a 20-second total deadline. Checked partial progress can be saved, but a broken stream never publishes a newly encountered terminal. Read failures attach to the particular input and do not disable other commands. Host shutdown or reader loss cancels only its GET future; no native input, interrupt or restart is sent.

**Execution disposition is not Task acceptance or family completion.** Results still require their own selectors/digests, review/checks and an explicit assignment-closure decision. Unassigned native children and background work remain outside this root-execution proof. Native shutdown/superseded interruptions retain `recovery_pending`; a new busy period without provable continuation cannot close the old input. Reused input IDs, a missing execution start and changed history remain unresolved. These cases do not authorize prompt replay.

Lifecycle and replay semantics were reviewed against OpenCode source `4c0d0ff478ca9150c163fb8b04a76395e4dccafe`: [HTTP log contract](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/protocol/src/groups/session.ts), [serialized coordinator](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session/run-coordinator.ts), [execution/shutdown claims](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session/execution.ts), [inbox promotion](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session/inbox.ts). This experimental route must exist in the explicitly selected installed version; absence is a read gap, not fallback to an inferred terminal.

## Read-only results

`agent.result` uses the existing `swarm result` → `operation` → `artifact assemble/export` path. The selector file supports an exact completed assistant message (root or a child with a verified parent chain):

```json
{"kind":"message","session_id":"ses_EXACT_NATIVE_ID","message_id":"msg_EXACT_NATIVE_ID"}
```

or a projected interval anchored to an already-sent input Operation on this exact root/binding/generation:

```json
{"kind":"input_interval","session_id":"ses_EXACT_NATIVE_ROOT","input_operation_id":"EXACT_DISPATCH_OR_SEND_OPERATION_ID"}
```

or the native per-file patches for the same input's isolated, closed projected turn:

```json
{"kind":"turn_diff","session_id":"ses_EXACT_NATIVE_ROOT","input_operation_id":"EXACT_DISPATCH_OR_SEND_OPERATION_ID"}
```

or one exact file item emitted by a completed/error tool call inside an exact completed assistant message:

```json
{"kind":"tool_file","session_id":"ses_EXACT_NATIVE_ID","message_id":"msg_EXACT_ASSISTANT_ID","tool_call_id":"EXACT_TOOL_CALL_ID","content_index":0}
```

```powershell
swarm --data-dir C:\SwarmState --request-id result-page-0 result BINDING_ID --generation 1 --file selector.json --offset 0 --length 65536
swarm --data-dir C:\SwarmState operation RESULT_OPERATION_ID
```

The `message`, `input_interval` and `turn_diff` bodies preserve native text, reasoning and tool/result references as **data**; those selectors never follow a URI. Completed tool states are required. `tool_file` is a separate explicit selector and can only dereference the exact file item at the selected tool call/content index. Intervals use unfiltered ordered pagination to find the exact user ID and its first subsequent idle message, not the latest assistant or matching prompt text. Original text/metadata must match; an intervening additional input, compaction or settings switch is rejected. The root's selected model must match every included assistant. Forks, staged reverts, duplicate IDs/cursors, changed bodies and incomplete reads are not silently accepted. The body is read twice and scope checked before/after; this detects observed changes, not an atomic native snapshot.

**None of these selectors proves native run completion.** V2's projected idle message has an outcome but no input/run reference. Source metadata retains `correlation=projected_order_only` for intervals/diffs and `execution_complete=false`, `family_complete=false` for every kind, including a file from a completed tool call. No producer disposition, native turn ID, Task acceptance or release is synthesized.

`turn_diff` calls `GET /api/session/{sessionID}/diff` with **both** `from` and `to` set to the original input's exact native ID. The native range starts at the first user after the preceding idle marker, so the reader also scans backward to that boundary (or the beginning of history) and refuses another user in the same turn. Missing first/last assistant snapshot endpoints report `RESULT_SNAPSHOT_UNAVAILABLE`, not an empty successful diff. Completed assistants and the closed interval exclude the documented active-step working-copy fallback. The diff is read twice, then the input interval and binding scope are rechecked. A changed source, duplicate file path, truncated response or unknown diff schema remains unresolved.

The exported JSON contains `session_id`, `native_input_id`, both snapshot IDs, the interval digest and native `files` entries (`file`, `patch`, `additions`, `deletions`, `status`). It preserves the full-file patch context returned by the native default. Source metadata records both snapshot IDs, the interval digest and file count. Patches and paths remain **data**: no patch application, arbitrary URI/file download or live-checkout verification occurs. This is not a captured CheckRunner source; binary contents are not included. The native turn-range and working-copy behavior were reviewed against [OpenCode source `4c0d0ff4`, `session/diff.ts`](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session/diff.ts) and the 2026-10-01 OpenAPI capture, not a live installed service.

`tool_file` never accepts a URI or path from the caller. It first reads the exact assistant message, requires a completed assistant plus a terminal `completed`/`error` tool state, selects one native `{type:"file",uri,mime,name?}` item, and rejects duplicate call IDs or an out-of-range index. Only two native descriptor forms are supported: a canonical base64 `data:` URI whose media type matches the descriptor, or a `file:` URI lexically under the verified route directory. A file URI is fetched only through `GET /api/fs/read/*` with that exact location; ELIOT does not open the native path itself, follow HTTP(S), redirect, or accept an arbitrary selector path. OpenCode's complete location-confined filesystem backend performs the realpath/symlink boundary. The assistant message/descriptor and raw bytes are then read again and must be identical before publication.

The tool-file artifact contains the raw bytes with the native MIME type. Source metadata stores hashes of the descriptor, message and URI but not the raw URI/path. The source is bounded to 64 MiB and uses the same 64 KiB result pages, whole-body SHA-256 pinning, assembly and export path as other results. Unsupported schemes, oversized content, MIME drift, missing native files or changed bytes remain unresolved. This reuses the selected `existing-control-bundle` direct-HTTP donor boundary and OpenCode's own result/filesystem state machines rather than adding another file store. Native semantics were reviewed against OpenCode `4c0d0ff4`: [tool content schema](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/schema/src/tool.ts), [projected tool state](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/schema/src/session-message.ts), [read-tool data URI production](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/tool/plugin/read.ts) and [location-confined filesystem read](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/filesystem.ts).

Successful Operations return `result.details.artifact_ref`, `source.content_digest` and `next_offset_bytes`. Optional `expected_digest: "sha256:<64 hex digits>"` pins the complete canonical JSON body for structured selectors or the complete raw byte body for `tool_file`. Use the identical selector for all pages; after an unpinned discovery read, re-read page zero with the pinned selector before assembly. Every page and the assembled body are hashed. Native result bytes stay in private artifact files, not in SQLite telemetry.

Unfinished reads recover with GET only, at most one background retry per binding per five seconds, rotating past unresolved reads; queued commands have priority. Explicit `agent.reconcile` also retries the exact saved read. Publication uses the existing no-overwrite file/SQLite boundary. An orphan file can be registered after the same source is read again; changed or missing native data stays unresolved rather than overwriting the orphan or replaying a prompt. A settled request ID returns its saved receipt.

Each timeline pass is bounded to 32 pages of at most 50 messages and 8 MiB of scanned JSON; one result read has a 20-second deadline and publishes at most 64 KiB. Large/old/unavailable results report their limit rather than partial success. Native turn-diff responses obey the 4 MiB per-response cap. Exact native tool-file sources are bounded to 64 MiB and are reread before a page is published; external URI schemes and selector-supplied paths remain unsupported.

## Remaining C04 work — do not declare end-to-end completion

Cross-restart native continuation after shutdown/missing terminal, effort controls and complete family reconstruction remain unfinished. Goal controls are implemented as a controller record, not a native goal API (see the goal section above); automatic continuation after a terminal turn is not implemented. Explicit configure-to-input prerequisite chaining is implemented for durable instruction entries, exact session-agent selection and exact route-model restoration. Exact inline/location-confined tool-file retrieval is implemented; external URI schemes, files outside the verified location and sources above 64 MiB remain deliberately unsupported. The model control preserves the route's existing provider/model/variant; it is not an arbitrary model/billing-route switch and does not claim goal or effort control. The durable-log path now resolves ordinary admitted/delivered inputs and exact pending cancellation; incomplete or unsupported native histories keep the producer unresolved. Parent idle or the latest assistant outcome cannot discharge it. Unsupported methods report an explicit capability error.

Attention/`session.background` is implemented as the addressed `agent.background` operation (see the background section above); its qualification is fixture-level only, not live. Terminology here follows the module contract's evidence ladder ([module contract §§4–7](../../docs/agent_swarm.module-contract-v2.md)): record, ACK, start and terminal are distinct facts, and no capability is documented from research notes alone.

The same durable-log protocol now also reads individual child sessions: a child with a bound non-terminal producer, an unfinished recorded execution period, or a native active mark is read through its own anchored, watermark-gated log scan, and a proven child terminal (by exact execution-start event ID) discharges only that child's producer. Untracked children are never log-read. The snapshot reports per-axis `family_coverage` counters (members total / observed now / active-verified / with execution evidence / with terminal evidence); coverage is evidence accounting, not completeness — `family_completeness` remains `partial` because the native API offers no atomic family snapshot, and a failed or unsynced child read keeps prior evidence instead of inventing or erasing a terminal. Whether the installed OpenCode build serves child logs with the same contract remains a live-qualification item.

The owner-machine [R6 qualification record](../../docs/qualification-2026-10-03.md) observes installed OpenCode `2.0.7` under Bun `1.4.0` on Windows: one exact `opencode-go/space-bunny-free#low` input, retained native execution/result evidence and same-root controller recovery without replay. That observation applies to its recorded dependency revision and executable; in-flight service restart, complete family reconstruction and automatic Task completion remain unqualified. Task acceptance stayed separate. The adapter does not require or install the optional OpenCodex provider proxy (Issue #1).

## Focused implementation evidence

The exact session-agent baseline `1703b293` passed [Windows/Linux CI 36984502928](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36984502928). The route-model slice is published only after Rust 1.98.1 owned-crate formatting, locked minimal lib/bin Clippy and unchanged Atlas verification; permanent Windows/Linux CI on its exact commit remains separate evidence. No tests or live native calls are run for this code-completion slice; dependencies, migrations and the Atlas donor remain unchanged.

The interrupted 2026-10-01 implementation retained 16 protocol/Store fixtures. They were preserved, not rerun during source recovery. Fresh Rust 1.98.1 package formatting and minimal warnings-denied Clippy passed; the Windows/Linux workflow checks the exact commit's formatting, Clippy, donor hashes, release build and existing Muse SDK import. Tests remain deferred while the product code is being completed. Fixture HTTP servers are not OpenCode, and compilation is not live service, model, billing or subscription qualification.

## Updates

Pins, verification, activation and rollback for this adapter are recorded in [UPDATE.md](UPDATE.md).
