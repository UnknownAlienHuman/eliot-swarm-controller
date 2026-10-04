# Command Code module — sessionless batch bridge

This module launches Command Code’s documented headless `cmd -p --output-format json` entrypoint with the pinned Eliot mod. The current adapter artifact is **`command-mod-0.1.0-glue.4`**. Its fixtures check core-bound prompt identity, semantic event projection, exact raw frame retention, and saved terminal evidence. `.4` has not been natively qualified; any earlier `.3` qualification remains historical and does not qualify `.4`. `.2` and `.3` records remain read-only and are never migrated or replayed by `.4`.

`modules/command/bridge.mjs` uses the existing host JSON-RPC transport in `modules/claude/control.mjs` and the local driver in `glue.mjs`. Configure the installed Node executable in `command` and an absolute `commandArgs` array containing the trusted native CLI entrypoint (no shell wrapper), plus host endpoint/credential and an isolated `controlRoot`, in a private copy of `module.example.json`. The CLI arguments follow that fixed prefix. The controller route must carry `native_options.modelId = 'stealth/space-bunny-alpha'`; this is the selected FREE Command catalog entry and is not asserted equivalent to OpenCode’s `opencode/space-bunny-free`. `workspaceRoot` must be an absolute route option.

The private module config may set `runTimeoutMs` to a positive safe integer from 1 through 2,147,483,647. It sets the deadline for the existing `task.dispatch` child termination path; an absent field preserves the existing no-timeout behavior. On expiry, glue requests child termination, uses its existing two-second escalation, and records the result as `Unknown`; native input is never resent. A timeout is not proof that every descendant has exited, so the owning process supervisor must verify the exact process family before declaring cleanup complete.

## Operation boundary

| Host operation | Behavior |
|---|---|
| `agent.open` | Read-only executor preflight: probe `--version`, hash the pinned mod and validate the workspace path. It creates no Command session and returns `native_session_state=not_started`. It does not prove account access or that the requested model can run. |
| `task.dispatch` | One prompt, one `cmd -p` child, one terminal result. The controller and glue use the shared canonical instruction: the exact dispatch text, then `ELIOT immutable task snapshot`, then the frozen canonical snapshot bytes. Store independently binds the Operation digest and prompt SHA-256/UTF-8 byte count to the original request and immutable Attempt snapshot. The bridge writes admission before spawn, keeps parent evidence in the hashed operation directory, and confines the mod journal/inbox to its `mod/` child directory. |
| `agent.reconcile` | Reads only the saved target operation (`input.operation_id`). For a `task.dispatch` target, the controller supplies the expected identity from that Operation and its frozen Attempt; missing, foreign, malformed, or inconsistent saved evidence becomes a core-bound `Unknown` receipt under the target Operation ID. It never resends a prompt. |
| `agent.refresh` | Reads the sessionless module snapshot. |
| `agent.send`, configure, goal, attach, resume, steer, reply, recover and result pages | Unavailable. The CLI invocation is a one-shot process with no established cross-turn session binding or result-page interface. |

The module keeps `native_root_id`, `native_scope_key`, `turn_id`, and `native_input_id` unset. A session ID emitted by Command is retained only as per-run evidence. The adapter unwraps documented `{type:"event",event:{...}}` frames into semantic events and retains each exact NDJSON line with its frame format in `events.ndjson`; direct event records remain supported for historical fixture compatibility. The `model_request_start` and `model_request_end` events can establish `native_request_model` only. Requested model remains the exact route `modelId`; `effective_model` stays `null`/`unknown`, and no provider identity is inferred from the requested route or native request event.

For a matching `.3` admission and run, `snapshot` may expose a transient semantic `event_projection` from the saved parsed event records. It marks the projection read-only and reports that original line bytes are unavailable. The saved `.3` files are not rewritten, and their terminal remains `Unknown` under `.4`.

## Outcome evidence

`Applied` requires a final native `success` result, exit code 0, no signal, and no recorded protocol gaps or anomalies. Before settling, `.4` checks that each saved raw event/result frame agrees with its semantic projection, then checks the `run.json` result, event sequence/summary, native request-model evidence, and process exit facts. A validated native `error` or `max_turns` result is `Rejected`. Missing, altered, or inconsistent evidence; interrupted/failed spawn; a result/exit disagreement; stream gaps; or any other ambiguous terminal is `Unknown`; exit 0 by itself never completes the Task. Raw `result_subtype`, exit facts and anomalies are retained in the outcome.

The controller receives the core-derived `batch_run_id`, prompt digest/UTF-8 byte count, opaque `control_record_ref` and `result_ref`, result digest/byte count, requested/effective model facts, and allowlisted artifact references. The complete native text and NDJSON remain in the local `controlRoot` record; host result-page retrieval is unavailable in this artifact. The glue removes `ELIOT_*`, `SWARM_*`, and capture-named variables from the version probe and native child, while preserving the ordinary OS home, `PATH`, and vendor authentication environment; only the mod control path is re-added, pointing at the nested `mod/` directory. Admission without a valid terminal record is reconciled as `Unknown` and is never replayed automatically.

Saved-file checks establish internal consistency between local records. They are not tamper-proof against arbitrary filesystem writes by the same user who owns `controlRoot`.

## Local module setup

Copy `module.example.json` to a private path and replace its host endpoint, credential file, executable and control root. The route in `config/controller.example.toml` must use artifact `.glue.4`, an explicit model ID, and an absolute `workspaceRoot`.

```powershell
node bridge.mjs --config C:\SwarmConfig\command.json
node glue.mjs describe --config C:\SwarmConfig\command.json
node glue.mjs open --config C:\SwarmConfig\command.json --operation-id local-probe-1 --model stealth/space-bunny-alpha --cwd C:\Projects\YourRepository --prompt "return a marker" --control-dir C:\SwarmState\command-runs\local-probe-1
node glue.mjs snapshot --control-dir C:\SwarmState\command-runs\local-probe-1
```

The direct `glue.mjs open` form is a development path; it requires an explicit operation ID and model and uses the same durable admission/no-replay rule. Do not point a qualification run at a shared control root.

## Fixture verification

```powershell
npm run check
npm test
```

The `.4` fixtures validate canonical prompt binding, environment separation, model-argument pass-through, wrapped and legacy direct event parsing, raw-frame consistency, native request-model provenance, event/result/exit consistency, mod readbacks, idempotent saved-result readback, admission-only no-replay, historical `.2` handling, read-only `.3` event projection, and `module.hello/next/outcome/observe` wiring against a local JSON-RPC host. They make no vendor model calls. This is fixture verification only; native `.4` qualification remains pending. `vendor.lock` pins the exact bridge/glue digests, shared transport dependency and documented vendor sources; changing a pinned unit requires a new artifact and updated pin.
