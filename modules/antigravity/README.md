# Antigravity CLI bridge — native integration

Drives the installed Antigravity CLI (`agy`) through its documented warm
stream entrypoint: one owned process per binding, launched as
`agy --input-format stream-json --output-format stream-json`, sequential
`{"event":"user"}` prompts on stdin, and the native NDJSON event stream
(`init` / `step_update` / `result`, discriminated by `event`) mapped by
`codec.mjs`. There is no vendor SDK for this runtime and no shared server to
attach to; the bridge owns the CLI process it starts and nothing else. New
bindings use **`antigravity-cli-warm-bridge.2`**. Live Antigravity and
Windows native launch remain unqualified; syntax, fixtures and compilation
do not attest model execution.

Protocol basis: the vendor's official headless documentation
(<https://antigravity.google/docs/cli/headless/>, source AG-HEADLESS in
`docs/agent_swarm.runtime-sources-v16.json`, re-read 2026-10-02) and the
subagents documentation (AG-CHILDREN). The runtime matrix records
`installed_runtime_verified: false` for this runtime; the matrix's admission
rule applies — this document does not enable the module or grant
capabilities, the runtime describes the actual entrypoint.

## Ownership and setup

The bridge owns one `agy` warm-stream connection. Host IPC exposes
`module.hello/next/outcome/observe/result`; its credential is scoped to the
reserved binding/generation and cannot accept Tasks or become GM. Native
subscription/auth, tools and the model loop remain in Antigravity; the
bridge never reads the CLI's config directory, conversation store or logs —
children transcripts (`log_uri`) are native references, not files this
module opens.

1. No package installation: the bridge has zero npm dependencies (Node
   builtins only). The native `agy` executable is installed by the owner
   through the vendor's own installer and authenticated interactively once;
   headless runs use the cached credentials.
2. Enable a private controller route with the actual workspace and, when
   wanted, the native model/effort/agent selection. The shipped route stays
   disabled. Run the host and reserve `agent.open` with lane_id/route;
   retain binding_id/generation.
3. Register the scoped module credential: `swarm client-create antigravity-MC --role module --binding-id BINDING --generation 1 --out PRIVATE_FILE`.
4. Copy `module.example.json` outside Git; set the actual host endpoint,
   credential file and the installed native executable (a real `.exe`, not
   `.cmd`/`.bat`). Match moduleArtifactId to the route. The bridge builds
   the warm-stream argv itself; configured `args` are rejected.
5. Start the module independently with the guarded launcher below. Only its
   admitted open starts the native executable.

```toml
[[routes]]
alias = 'antigravity-manager'
runtime = 'antigravity'
module_artifact_id = 'antigravity-cli-warm-bridge.2'
enabled = true
[routes.native_options]
workspaceRoot = 'C:\Projects\YourRepository'
modelId = 'gemini-3.5-flash-medium'
reasoningEffort = 'high'
```

```powershell
# Replace these example paths; the interpreter and script are separate argv.
swarm module-run --state-dir C:\SwarmModules\AG --command 'C:\Program Files\nodejs\node.exe' -- C:\SwarmCode\modules\antigravity\bridge.mjs --config C:\SwarmConfig\antigravity.json
```

`node bridge.mjs --config FILE` remains an unguarded legacy entrypoint: it
has host-reconnect behavior, but no recorded process ownership/checkpoint.
Do not overwrite its executable/scripts in place; use a new artifact/binding
for changed module code.

## Capability matrix (bridge.2)

Readiness terms follow the module contract: `implemented` is code in this
artifact, `documented` is the vendor contract it maps, `observed` requires
a live run, `unavailable` is an honest absence — never an emulation.

| Operation | State | Boundary |
|---|---|---|
| `describe` | implemented | Actual entrypoint, launch selection and known limits. Executor version is `null`: the native stream reports none and no version readback is documented for this entrypoint. |
| `agent.open` | implemented | Spawns the warm stream; identity is the `conversation_id` of the native `init` event, proven before any prompt is admitted. |
| `agent.open` with `resume_conversation_id` | implemented | Exact native resume via `--conversation <id>` in a new process (the documented resume path). It is an explicit open, never an automatic replay after bridge loss. |
| `agent.send` / `task.dispatch` | implemented, next-turn only | One `user` event per prompt. Terminal `SUCCESS` is applied; terminal `ERROR`, `CANCELED`, or `INTERRUPTED` is rejected. The Store receipt binds the exact operation, conversation, bridge boot, result ordinal, response SHA-256 and acknowledged observation. `WAITING`, `RUNNING`, unknown statuses, missing response bytes, or stream end do not become Applied. |
| `agent.refresh` (snapshot) | implemented | Codec observation of the root conversation or one observed child by its conversation_id. Family completeness stays `partial`. |
| `agent.attach` | unavailable | No shared server or external attach surface exists for this runtime; a live owned session is checked by identity on send/refresh instead. |
| `agent.configure` (model/effort) | unavailable mid-session | Model/effort/agent are launch flags (route `native_options`, applied at open). The stream documents no live setter — in-stream `/model` ends the session with an ERROR result — so nothing is emulated. |
| `agent.goal` | unavailable | Durable goal control is not established for the selected warm CLI entrypoint (runtime matrix). No goal field is invented and no controller fallback is applied silently. |
| steer (`delivery: 'steer'`) | unavailable | Correction is next-turn only, until an earlier delivery is natively confirmed. |
| `agent.reply` | unavailable | The warm stream has no native pending-request path: permission asks in headless mode are soft-denied by the CLI itself and surface as tool evidence (below). |
| `agent.result` (pages) | unavailable | The snapshot carries bounded recent steps/turns instead of invented paging over a store the bridge does not own. |
| `agent.recover` | unavailable | This artifact keeps no checkpoint. After bridge loss the binding stays reconciling; a new explicit open (optionally resuming the recorded conversation_id) is the operator's decision. |

## Observation boundaries

- **A warm result is local execution evidence, not a native turn ID.** Each
  terminal result is associated with the oldest sequentially admitted
  operation and carries the native conversation ID, bridge boot ID, a
  per-boot monotonic result ordinal, and SHA-256 of the exact UTF-8
  `result.response`. The bridge first records an observation containing that
  same operation ID and fingerprint, then sends the outcome with the returned
  Store `observation_id`. No `turn_id` or inbox ID is fabricated. The ordinal
  is adapter-local and never derived from cumulative `num_turns`; warm-process
  resumes under one bridge boot keep it increasing.
- **Only terminal statuses settle.** `SUCCESS` maps to Applied/completed;
  `ERROR` maps to Rejected/failed; `CANCELED` and `INTERRUPTED` map to
  Rejected/cancelled. `WAITING`, `RUNNING`, future statuses, or a terminal
  result without response bytes remain unresolved; stream end reports Unknown.

- **Soft-denied tools survive SUCCESS.** In headless mode a tool that
  cannot obtain approval is soft-denied: the run continues and can end
  `SUCCESS` with exit 0. A step whose `tool_info` carries an `error` is
  kept in `tool_errors` (step index, tool name, error type) and counted in
  `tools_with_errors`; the snapshot never upgrades that to tool success.
  The stderr tail is retained because the vendor also names soft-denied
  tools in a stderr notice.
- **Children are observed, never released.** `step_update.subagent_info`
  lists each subagent by its own `conversation_id` (with `type_name`,
  `role`, `log_uri`, `workspace_uris`). An idle child re-awakens on
  message (AG-CHILDREN), and the stream has no release event, so observed
  children keep status `observed`; family completeness stays `partial`.
- **Cumulative counters replace, never sum.** Per the vendor docs,
  `num_turns`, `duration_seconds` and `usage` in a `result` event are
  cumulative over the session; the snapshot keeps the latest result's
  values with `basis: 'native_cumulative_session'`.
- **Init failure is distinct.** A `result` before any `init` (for example
  the documented unknown-model envelope, with an empty conversation_id) is
  an init failure with the native error retained; no identity is invented.
- **No Claude-format leakage.** Input is encoded only by
  `encodeUserMessage` (`{"event":"user","message":{"content": …}}`).
  `control_request`/`control_response` events and slash input are
  documented to end the native session with an ERROR, and this module has
  no encoder for them. Unknown event names from a newer CLI are skipped
  and counted, mirroring the native tolerance.
- **EOF ends the channel.** Closing stdin is the documented graceful end;
  the bridge closes stdin only on explicit module termination, then waits
  for the current turn before any kill fallback on its own child. Host IPC
  loss never touches the native process.

## Fixtures and self-test

`fixtures/` are authored from the official documentation's own examples
(each file carries its provenance comment; they are not live captures):
init + SUCCESS, the two-prompt warm session, a tool error under a SUCCESS
result, a subagent invocation, and the pre-init error envelope.
`node selftest.mjs` runs the codec assertions with no dependencies, no
native executable and no model call.
