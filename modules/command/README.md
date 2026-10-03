# Command Code module — sessionless batch bridge

This module launches Command Code’s documented headless `cmd -p --output-format json` entrypoint with the pinned Eliot mod. The current adapter artifact is **`command-mod-0.1.0-glue.2`**. The module bridge is fixture-checked; it has not yet been qualified through a controller host run. A direct native CLI canary is separate evidence and does not qualify this bridge.

`modules/command/bridge.mjs` uses the existing host JSON-RPC transport in `modules/claude/control.mjs` and the local driver in `glue.mjs`. Configure the installed Node executable in `command` and an absolute `commandArgs` array containing the trusted native CLI entrypoint (no shell wrapper), plus host endpoint/credential and an isolated `controlRoot`, in a private copy of `module.example.json`. The CLI arguments follow that fixed prefix. The controller route must carry `native_options.modelId = 'stealth/space-bunny-alpha'`; this is the selected FREE Command catalog entry and is not asserted equivalent to OpenCode’s `opencode/space-bunny-free`. `workspaceRoot` must be an absolute route option.

## Operation boundary

| Host operation | Behavior |
|---|---|
| `agent.open` | Read-only executor preflight: probe `--version`, hash the pinned mod and validate the workspace path. It creates no Command session and returns `native_session_state=not_started`. It does not prove account access or that the requested model can run. |
| `task.dispatch` | One prompt, one `cmd -p` child, one terminal result. The prompt combines the controller-frozen `task_snapshot` and optional dispatch text. The bridge writes an operation-keyed admission record before spawn, then stores the NDJSON stream and final `run.json` under a hashed operation directory. |
| `agent.reconcile` | Reads only the saved target operation (`input.operation_id`). It echoes `details.target_operation_id` and, when evidence exists, separately reports the saved target result under its original Operation ID. It never resends a prompt. |
| `agent.refresh` | Reads the sessionless module snapshot. |
| `agent.send`, configure, goal, attach, resume, steer, reply, recover and result pages | Unavailable. The CLI invocation is a one-shot process with no established cross-turn session binding or result-page interface. |

The module keeps `native_root_id`, `native_scope_key`, `turn_id`, and `native_input_id` unset. A session ID emitted by Command is retained only as per-run evidence. Requested model is the exact route `modelId`; effective model stays `null`/`unknown` because the documented result/event evidence does not expose a verified effective-model identifier.

## Outcome evidence

`Applied` requires a final native `success` result, exit code 0, no signal, and no recorded protocol gaps or anomalies. A validated native `error` or `max_turns` result is `Rejected`. Missing result, interrupted/failed spawn, a result/exit disagreement, stream gaps, or any other ambiguous terminal is `Unknown`; exit 0 by itself never completes the Task. Raw `result_subtype`, exit facts and anomalies are retained in the outcome.

The controller receives a stable adapter-owned `batch_run_id` derived from the Operation ID (not a native run ID), opaque `control_record_ref` and `result_ref`, prompt digest/UTF-8 byte count, result digest/byte count, requested/effective model facts, and allowlisted artifact references. The complete native text and NDJSON remain in the local `controlRoot` record; host result-page retrieval is unavailable in this artifact. An admission marker without a terminal record is reconciled as `Unknown` and is never replayed automatically.

## Local module setup

Copy `module.example.json` to a private path and replace its host endpoint, credential file, executable and control root. The route in `config/controller.example.toml` must use artifact `.glue.2`, an explicit model ID, and an absolute `workspaceRoot`.

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

The fixtures validate model-argument pass-through, parser/evidence handling, mod readbacks, idempotent saved-result readback, admission-only no-replay, and `module.hello/next/outcome/observe` wiring against a local JSON-RPC host. They make no vendor model calls. `vendor.lock` pins the exact bridge/glue digests, shared transport dependency and documented vendor sources; changing a pinned unit requires a new artifact and updated pin.
