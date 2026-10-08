# Claude Agent SDK bridge — native integration (first slice)

Uses the complete locked `@anthropic-ai/claude-agent-sdk` **0.3.287** and its matching bundled native binary packages (`@anthropic-ai/claude-agent-sdk-<platform>` **0.3.287**). New bindings use **`claude-agent-sdk-0.3.287-bridge.3`**. The SDK is under Anthropic's Commercial Terms (see [third-party notices](../../THIRD_PARTY_NOTICES.md)); it is not a permissive open-source donor. A direct CLI canary does not qualify this SDK bridge or its host route.

## Ownership and setup

The bridge owns one Claude session through the SDK's streaming-input `startup()`/one-shot `WarmQuery.query()`. Host IPC exposes `module.hello/next/outcome/observe`; its credential is scoped to the reserved binding/generation and cannot accept Tasks or become GM. Native subscription/auth, tools and the model loop remain in Claude Code. The bridge is a separate process from the host; losing the host connection never closes the query or repeats input.

1. Run `npm ci --ignore-scripts` here for the locked local SDK, not global packages or a model login.
2. Enable a private controller route with the actual workspace and explicit model. The shipped route stays disabled. `workspaceRoot` and `modelId` are required; `modelId = 'sonnet'` is passed verbatim as the SDK's `Options.model` at open. The bridge never accepts the SDK/CLI default as a selected route model. `permissionMode` is optional, validated against the SDK enum, and frozen at open. A route `permissionMode = 'bypassPermissions'` additionally requires `allowDangerouslySkipPermissions = true` in the same route options, or open is rejected.
3. Run the host and reserve `agent.open` with lane_id/route; retain binding_id/generation.
4. Register the scoped module credential: `swarm client-create claude-MC --role module --binding-id BINDING --generation 1 --out PRIVATE_FILE`. Use normal host/data-dir arguments and preserve request IDs.
5. Copy `module.example.json` outside Git; set the actual host endpoint and credential file. An optional `command` field names an explicitly selected installed native executable (absolute path, real `.exe` on Windows, not `.cmd`/`.bat`) for `pathToClaudeCodeExecutable`; when omitted, the SDK launches its own pinned bundled binary from the locked optional package.
6. Start the module independently with the guarded launcher below, in a dedicated initially empty module-state directory separate from host data:

```powershell
swarm module-run --state-dir C:\SwarmModules\CC --command 'C:\Program Files\nodejs\node.exe' -- C:\SwarmCode\modules\claude\bridge.mjs --config C:\SwarmConfig\claude.json
```

```toml
[[routes]]
alias = 'claude-manager'
runtime = 'claude'
module_artifact_id = 'claude-agent-sdk-0.3.287-bridge.3'
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
| send (`next_turn`) / task.dispatch | implemented | Exact Task text/snapshot and echoed user UUID bind admission to the actual session; the UUID is not a native turn ID or execution completion |
| snapshot (`agent.state`, `agent.refresh`) | implemented | Compact stream and uniquely correlated terminal-input projection (below); refresh is a read, never a native call |
| reconcile | implemented | Bridge-local journal readback only; never resends native input |
| attach | unavailable | No second control owner is created for an existing session |
| resume | unavailable | The SDK option exists but is not exposed or qualified here |
| configure (model/effort) | unavailable | No setter is wired; a route cannot claim an applied model/effort |
| goal | unavailable | No native goal verb is exposed by this artifact |
| steer | unavailable | Streaming input has no expected-turn correction in this mapping |
| reply (tool permission answers) | unavailable | See permission behavior below |
| result pages (`agent.result`) | unavailable | No pinned native item paging in this slice |
| recover | unavailable | See recovery boundary below |

At `agent.open`, SDK 0.3.287 `startup()` prepares an initialized subprocess and returns a one-shot `WarmQuery`; the bridge records a rootless prepared receipt without sending a model prompt. The first exact Task dispatch claims that handle once and submits `Task specification: <canonical Task snapshot>` plus the frozen Task text. Only the later native `system/init.session_id` and a native frame echoing the stamped user UUID bind the session and input to the controller. No requested UUID is promoted into native identity. If startup, first dispatch, or the bridge is lost before that receipt, the outcome stays unresolved and the SDK input is never replayed. The implementation and fixture contract are present; installed-runtime/model qualification remains unknown until observed separately.

## Stream mapping

The mapper (`stream.mjs`) is pure and shared by the live path and the fixture self-test, so they cannot drift:

- One API assistant turn arrives as several assistant frames sharing one `message.id`, each carrying the block it delivers at frame-local index 0. Blocks are appended in arrival order under that id; tool blocks dedupe by native tool id. A frame is never deduplicated as a whole message, so a repeated `message.id` cannot lose a tool block. A replayed frame (same frame `uuid`) is applied once.
- `stream_event` partials are token deltas: counted as `partial_events_seen`, never inventoried as messages or children.
- Child linkage comes only from complete frames: a root `Task`/`Agent` tool_use block opens a child record keyed by that tool id; subagent frames carry `parent_tool_use_id` (the bridge opens the query with `forwardSubagentText: true`, so complete child messages are forwarded, not only tool heartbeats). A child is completed/failed only by the root `tool_result` for its tool id (or a native `task_notification`); child activity alone never completes it, and family completeness stays `partial`.
- A `result` frame closes a turn: `success` → `turn_completed`; `error_during_execution` / `error_max_turns` / `error_max_budget_usd` / `error_max_structured_output_retries` → `turn_failed` with the native subtype retained. An `error_during_execution` result before any `system/init` is an **init failure** — a distinct recorded outcome with the native `errors[]`, never an empty successful start and never an invented session identity.
- A result contributes a compact `input_executions` receipt only when the SDK reports its actual result-frame UUID, initialized session id, terminal subtype, effective model, and client user-message UUID(s). A single-input result binds that UUID to an output digest and UTF-8 byte count; the transcript itself is not copied into controller state. If the SDK merged several queued inputs into one result, the projection marks them ambiguous and does not complete any one producer. Task/input admission and terminal execution evidence remain separate, and no SDK turn ID is invented.
- Usage is the SDK's cumulative estimate for the query: each result's `total_cost_usd`/`modelUsage` **replaces** the previous snapshot (basis `sdk_cumulative_estimate`). Results are never summed, and a cumulative conversation cost is not a new charge. No quota is inferred from it.
- `system/init` also fixes the observed model, permission mode and `claude_code_version` (the actual executor version, reported separately from the SDK package version). A later settings change is not claimed: this artifact exposes no configure verb, and a system-prompt snapshot is never declared updated by anything here.
- Initial model selection comes only from the binding route's required `native_options.modelId`, passed as SDK `Options.model`. Snapshots report `model_requested` separately from `model_effective`; the latter is `unknown` until native `system/init.model` is observed. An alias such as `sonnet` is not rewritten to the resolved model ID, and an absent route model rejects open before the SDK query starts.
- Observations are compact: block kinds/tool ids/result flags, not transcript copies. Host state keeps at most the latest 20 message summaries, 100 children and 32 turn records; overflow is counted in `gaps`, not silently dropped.

## Permission behavior

If the route omits `permissionMode`, this bridge leaves the SDK option unset. That is **not a promise of `default` mode**: the SDK's [permission documentation](https://code.claude.com/docs/en/agent-sdk/permissions#permission-modes) records changed omission behavior starting with TypeScript SDK 0.3.286; this module pins 0.3.287. The native executor selects its starting mode from applicable settings/defaults. Set an explicit route mode when the workflow requires one, and distinguish that request from the mode actually observed in `system/init`. This correction changes documentation, not the running configuration or artifact.

`canUseTool` handles only calls that reach that stage of native permission evaluation. Earlier native rules/modes can approve a call without consulting it; `dontAsk` denies calls that would prompt instead of invoking the callback. Consequently the callback is neither a complete tool inventory nor a universal tool firewall. In this artifact, calls that do reach it are recorded in `permission_requests` and immediately denied because `agent.reply` is unavailable. Those records are **historical denials, not live pending requests that a later reply can resolve**. Native denial advisories and result evidence remain distinct from a current callback.

Native [permission flow](https://code.claude.com/docs/en/agent-sdk/permissions) and [approval/user-input callbacks](https://code.claude.com/docs/en/agent-sdk/user-input) were reviewed on 2026-10-07. The proposed Rust-adapter interaction slice is [R16](../../docs/remediation/2026-10-07/16-claude-interactions.md); its existence does not change this legacy artifact's capability matrix. Do not enable bypass or persistent allow rules as an implicit workaround for an unimplemented reply path.

## Recovery boundary

This artifact keeps no checkpoint and implements no resume. A host IPC reconnect to the same live bridge boot can keep a rootless prepared binding ready only before any possibly-sent first Task input; an unknown first dispatch remains reconciling. If the bridge process is lost, the host admits only `agent.recover`/`agent.reconcile`; the bridge answers `agent.recover` with `CAPABILITY_UNAVAILABLE`. The binding is not silently restarted, forked or given a replayed prompt: release the lane and open a new binding. The old native session is neither adopted nor killed by this module. Cross-restart continuation is a later slice and must be qualified on the installed runtime before its capability is advertised.

## Fixtures and self-test

`fixtures/` holds stream fixtures authored from the pinned SDK 0.3.287 message types (`sdk.d.ts`) — recorded-shape protocol examples, **not** live captures. After `npm ci --ignore-scripts`:

```powershell
node selftest.mjs
```

It asserts the SDK import surface plus the mapping boundaries above: init identity, multi-frame block assembly without loss, replay applied once, partials excluded from inventory, child linkage/completion, distinct init failure, terminal error subtypes and cumulative-usage replacement. CI runs the same check next to the Muse bridge checks.

## Implementation and remaining work

First C08 slice for Claude: describe/open/next-turn send/snapshot over the pinned Agent SDK with fixture-verified stream mapping and explicit initial model selection. Remaining, as separate slices: attach/resume qualification on the installed runtime, configure (model/effort) with native readback, goal surface, permission replies, result pages, recorded-session recovery, then the Command and Antigravity adapters. Do not mark C08 complete from this slice.
