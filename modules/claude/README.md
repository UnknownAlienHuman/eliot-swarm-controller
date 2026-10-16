# Claude Agent SDK bridge — native integration (first slice)

Uses the complete locked `@anthropic-ai/claude-agent-sdk` **0.3.287** and its matching bundled native binary packages. The current source candidate is **`claude-agent-sdk-0.3.287-bridge.6`**, selected through the retained descriptor and `bridge-v6.mjs`. An omitted model leaves SDK `Options.model` unset; explicit aliases pass verbatim and effective identity comes only from native readback. The SDK remains under Anthropic's Commercial Terms. A direct CLI canary does not qualify this bridge or its host route.
Existing bridge.3 through bridge.5 bindings keep their original installed bytes and identity. Bridge.6 retains the Store `swarm.task_prompt@1` envelope and dispatch context; installed/native qualification remains pending. Its optional-model helper is separate from the retained required-model helper.

## Ownership and setup

The bridge owns one Claude session through the SDK's streaming-input `startup()`/one-shot `WarmQuery.query()`. Host IPC exposes `module.hello/next/outcome/observe`; its credential is scoped to the reserved binding/generation and cannot accept Tasks or become GM. Native subscription/auth, tools and the model loop remain in Claude Code. The bridge is a separate process from the host; losing the host connection never closes the query or repeats input.

1. Run `npm ci --ignore-scripts` here for the locked local SDK, not global packages or a model login.
2. Materialize and retain the exact [module descriptor](module-descriptor.template.json) through the documented module catalog procedure, then enable a private `runtime = 'module'` route with the actual workspace. The shipped route stays disabled. `workspaceRoot` is required; `modelId` is an optional explicit override. When omitted or null, SDK `Options.model` is omitted and the installed configured harness uses its native CLI default model. An explicit `modelId = 'sonnet'` is passed verbatim as the SDK's `Options.model` at open. `permissionMode` is optional, validated against the SDK enum, and frozen at open. A route `permissionMode = 'bypassPermissions'` additionally requires `allowDangerouslySkipPermissions = true` in the same route options, or open is rejected. The descriptor template is a qualification candidate: replace its placeholders deliberately and select fault controls only for the addressed qualification run.
3. Run the host and reserve `agent.open` with lane_id/route; retain binding_id/generation.
4. Register the scoped module credential: `swarm client-create claude-MC --role module --binding-id BINDING --generation 1 --out PRIVATE_FILE`. Use normal host/data-dir arguments and preserve request IDs.
5. Copy `module.example.json` outside Git; set the actual host endpoint and credential file. An optional `command` field names an explicitly selected installed native executable (absolute path, real `.exe` on Windows, not `.cmd`/`.bat`) for `pathToClaudeCodeExecutable`; when omitted, the SDK launches its own pinned bundled binary from the locked optional package.
6. Start the module independently with the guarded launcher below, in a dedicated initially empty module-state directory separate from host data:

```powershell
swarm module-run --state-dir C:\SwarmModules\CC --command 'C:\Program Files\nodejs\node.exe' -- C:\SwarmCode\modules\claude\bridge-v6.mjs --config C:\SwarmConfig\claude.json
```

```toml
[[routes]]
alias = 'claude-manager'
runtime = 'module'
module_artifact_id = 'claude-agent-sdk-0.3.287-bridge.5'
enabled = true
[routes.native_options]
workspaceRoot = 'C:\Projects\YourRepository'
modelId = 'sonnet'
```

`node bridge.mjs --config FILE` remains an unguarded entrypoint with host-reconnect behavior but no recorded process ownership. Only its admitted open starts the native executable. No PATH, UAC or vendor service configuration is changed.

## Capability matrix — this artifact

Readiness words follow the module contract: `implemented`, `documented`, `observed`, `unavailable`, `unknown`. The SDK type surface exposes more (a `resume` option, `setModel`, `applyFlagSettings`, an effort option); presence in the SDK is not evidence in this controller and grants nothing — the matrix below is the artifact's contract, and the bridge reports it verbatim in every observation under `describe.capabilities`.

| Operation | State | Boundary |
| --- | --- | --- |
| describe | implemented | Entrypoint, SDK version, executor version/permission mode, route-requested model, and separately the model observed in native `system/init` |
| open | implemented | `startup()` prepares a rootless native executor without submitting a model prompt; the observed SDK session id is adopted only with the first Task input echo |
| send (`next_turn`) / task.dispatch | implemented | Exact Store TaskPrompt text and echoed user UUID bind admission to the actual session; the UUID is not a native turn ID or execution completion |
| snapshot (`agent.state`, `agent.refresh`) | implemented | Compact stream and uniquely correlated terminal-input projection (below); refresh is a read, never a native call |
| reconcile | implemented | Bridge-local journal readback only; never resends native input |
| attach | unavailable | No second control owner is created for an existing session |
| resume | unavailable | The SDK option exists but is not exposed or qualified here |
| configure (model/effort) | unavailable | No setter is wired; a route cannot claim an applied model/effort |
| goal | unavailable | No native goal verb is exposed by this artifact |
| steer | unavailable | Streaming input has no expected-turn correction in this mapping |
| live permission/question reply | implemented | PreToolUse, canUseTool and agent.reply form a boot/session/request/digest-fenced one-shot callback path; its resolver remains in the live bridge |
| durable defer | unavailable | SDK 0.3.287 declares defer and resume, but its TypeScript no-new-prompt resume input is unspecified here |
| result pages (`agent.result`) | implemented | The SDK 0.3.287 `SDKResultMessage` UUID, session ID and unique echoed user UUID bind its exact result body to one admitted dispatch; bounded boot-local bodies are paged through Store `module.result` and checked against the retained descriptor, dispatch receipt, current bridge boot and native root. Confirmed ACKs move into the existing result journal and release their one-shot attempt slot; only ACK-uncertain attempts occupy the bounded uncertainty map |
| recover | unavailable | See recovery boundary below |
| shutdown (`agent.shutdown`) | unavailable | This bridge.5 artifact does not declare it. The separate [Rust controller](../claude-rust/README.md) implements only `owned_sdk_transport`; live Task/family completion is not qualified. |

At `agent.open`, SDK 0.3.287 `startup()` prepares an initialized subprocess and returns a one-shot `WarmQuery`; the bridge records a rootless prepared receipt without sending a model prompt. The first exact Task dispatch claims that handle once and submits the Store `swarm.task_prompt@1` envelope bytes unchanged (`taskPrompt.envelope.prompt` via `sendNative`/`enqueueTaskPrompt` with pass-through `first-query-control`). `Task specification: <canonical Task snapshot>` rendering is retained bridge.3/.4 behavior, not new bridge.5 bindings. Only the later native `system/init.session_id` and a native frame echoing the stamped user UUID bind the session and input to the controller. No requested UUID is promoted into native identity. If startup, first dispatch, or the bridge is lost before that receipt, the outcome stays unresolved and the SDK input is never replayed. The implementation and fixture contract are present; installed-runtime/model qualification remains unknown until observed separately.

## Stream mapping

The mapper (`stream.mjs`) is pure and shared by the live path and the fixture self-test, so they cannot drift:

- One API assistant turn arrives as several assistant frames sharing one `message.id`, each carrying the block it delivers at frame-local index 0. Blocks are appended in arrival order under that id; tool blocks dedupe by native tool id. A frame is never deduplicated as a whole message, so a repeated `message.id` cannot lose a tool block. A replayed frame (same frame `uuid`) is applied once.
- The exact frame UUID set is capped at 8,192: known UUIDs remain ignored after saturation, while unseen UUID-bearing frames are refused and counted in gaps. This bounded mapper policy does not claim the pinned SDK replays frames after the cap.
- `stream_event` partials are token deltas: counted as `partial_events_seen`, never inventoried as messages or children.
- Child linkage comes only from complete frames: a root `Task`/`Agent` tool_use block opens a child record keyed by that tool id; subagent frames carry `parent_tool_use_id` (the bridge opens the query with `forwardSubagentText: true`, so complete child messages are forwarded, not only tool heartbeats). A child is completed/failed only by the root `tool_result` for its tool id (or a native `task_notification`); child activity alone never completes it, and family completeness stays `partial`.
- A `result` frame closes a turn: `success` → `turn_completed`; `error_during_execution` / `error_max_turns` / `error_max_budget_usd` / `error_max_structured_output_retries` → `turn_failed` with the native subtype retained. An `error_during_execution` result before any `system/init` is an **init failure** — a distinct recorded outcome with the native `errors[]`, never an empty successful start and never an invented session identity.
- A result contributes a compact `input_executions` receipt only when the SDK reports its actual result-frame UUID, initialized session id, terminal subtype, effective model, and client user-message UUID(s). A single-input result binds that UUID to an output digest and UTF-8 byte count; the transcript itself is not copied into controller state. If the SDK merged several queued inputs into one result, the projection marks them ambiguous and does not complete any one producer. Task/input admission and terminal execution evidence remain separate, and no SDK turn ID is invented.
- Usage is the SDK's cumulative estimate for the query: each result's `total_cost_usd`/`modelUsage` **replaces** the previous snapshot (basis `sdk_cumulative_estimate`). Results are never summed, and a cumulative conversation cost is not a new charge. No quota is inferred from it.
- `system/init` also fixes the observed model, permission mode and `claude_code_version` (the actual executor version, reported separately from the SDK package version). A later settings change is not claimed: this artifact exposes no configure verb, and a system-prompt snapshot is never declared updated by anything here.
- Initial model selection is an optional explicit override from the binding route's `native_options.modelId`, omitted as SDK `Options.model` when absent. Pinned SDK `Options.model` is optional and defaults to the CLI default model. Snapshots report `model_requested` (null when omitted) separately from `model_effective`; the latter is `unknown` until native `system/init.model` is observed. An explicit alias such as `sonnet` is passed verbatim and is not rewritten to the resolved model ID.
- Observations are compact: block kinds/tool ids/result flags, not transcript copies. Host state keeps at most the latest 20 message summaries, 100 children and 32 turn records; overflow is counted in `gaps`, not silently dropped.

## Permission behavior

When the route omits `permissionMode`, `bridge-v6.mjs` leaves SDK `Options.permissionMode` unset; this is not an explicit `permissionMode = 'default'`. `system/init.permissionMode` remains the observed native mode. The bridge registers a `PreToolUse` hook that asks for a controller decision and requires its tool ID/input digest to match before `canUseTool` can publish a pending request. The unresolved callback projects a bounded redacted request, then `agent.reply` validates the exact bridge boot, session, request ID, digest, kind and typed answer before resolving the original Promise once. Allow, deny and `AskUserQuestion` answers preserve the SDK result shape. A callback ACK means only that the callback Promise was settled; it does not claim tool or task completion. Abort, stream end and bridge retirement settle the callback as denied/lost, and a late reply is rejected. Universal `PreToolUse` coverage on the installed 0.3.287 runtime remains unknown until an exact-runtime run; older upstream bypass reports do not establish 0.3.287 behavior.

SDK 0.3.287 declares `PreToolUse` defer, session persistence and resume, but the TypeScript query surface does not document how to resume the deferred tool without supplying a new user prompt. This artifact therefore keeps durable defer and resume unavailable. It never serializes a JavaScript resolver or substitutes a fresh prompt.

## Recovery boundary

This artifact keeps no checkpoint and implements no resume. A host IPC reconnect to the same live bridge boot can keep a rootless prepared binding ready only before any possibly-sent first Task input; an unknown first dispatch remains reconciling. If the bridge process is lost, the host admits only `agent.recover`/`agent.reconcile`; the bridge answers `agent.recover` with `CAPABILITY_UNAVAILABLE`. The binding is not silently restarted, forked or given a replayed prompt: release the lane and open a new binding. The old native session is neither adopted nor killed by this module. Cross-restart continuation is a later slice and must be qualified on the installed runtime before its capability is advertised.

## Fixtures and self-test

`fixtures/` holds stream fixtures authored from the pinned SDK 0.3.287 message types (`sdk.d.ts`) — recorded-shape protocol examples, **not** live captures. After `npm ci --ignore-scripts`:

```powershell
node selftest.mjs
```

It asserts the SDK import surface plus the mapping boundaries above: init identity, multi-frame block assembly without loss, replay applied once, partials excluded from inventory, child linkage/completion, distinct init failure, terminal error subtypes and cumulative-usage replacement. CI runs the same check next to the Muse bridge checks.
EOF or a lost response before matching input-UUID admission remains unknown and is not replayed; these protocol-shape fixtures are not live captures.

## Implementation and remaining work

The bridge reads results only from the current live SDK stream. Ambiguous or missing UUID correlation, unavailable body bytes, changed boot/root, and lost page ACK never trigger a native query or a second `module.result` call. Confirmed pages converge in the existing bounded result journal, so ordinary ACKed reads do not consume uncertainty capacity. The SDK's historical transcript reader is not used to recreate a result after bridge replacement; recovery and durable defer remain unavailable.

Remaining, as separate slices: attach/resume qualification on the installed runtime, durable defer after an exact no-new-prompt resume contract, configure (model/effort) with native readback, goal surface, recorded-session recovery, then the Command and Antigravity adapters. Offline page fixtures are not installed-runtime/model proof.
