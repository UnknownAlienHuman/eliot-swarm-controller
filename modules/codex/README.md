# Codex app-server bridge

This module has two explicit modes over an operator-owned Codex app-server:
the `describe` / `open` / `snapshot` observer CLI and the registered Rust-host
controller. The controller can create a thread with the route's exact
`modelProvider` and `model`, submit one user input, steer one verified active
turn, read native history, reconcile a saved operation, and explicitly resume
a saved thread. It never starts or stops the shared app-server.

## Setup

Create a module-local Python environment and install only the pinned
requirements:

```powershell
cd modules/codex
py -3 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe verify_vendor.py
```

Copy `module.example.json` outside the repository and set `endpoint`,
`hostEndpoint`, and `credentialFile`. The referenced app-server is already
running and remains operator-owned. `tokenEnv`, when set, names an environment
variable containing its WebSocket bearer token; the token is never stored in
the config file or logged. The host injects `ELIOT_SWARM_MODULE_STATE` and
`ELIOT_SWARM_MODULE_OWNER` for the controller's private operation checkpoint.

## Observer CLI

```powershell
.venv\Scripts\python.exe bridge.py --config C:\SwarmState\codex-module.json describe
.venv\Scripts\python.exe bridge.py --config C:\SwarmState\codex-module.json open --thread <native-thread-id>
.venv\Scripts\python.exe bridge.py --config C:\SwarmState\codex-module.json snapshot --limit 20
```

The observer attaches passively. Its client allowlist permits only
`initialize`, `initialized`, `thread/read`, and `thread/list`.

## Registered controller

The operator launches the bridge through the managed module owner. Use the
absolute paths of the pinned module-local Python interpreter and bridge:

```powershell
swarm module-run --state-dir C:\SwarmState\codex-owner --command ABSOLUTE_PYTHON_PATH -- ABSOLUTE_BRIDGE_PATH --config C:\SwarmState\codex-module.json module
```

The route must select `runtime = "codex"`, artifact
`codex-sdk-18194bf-bridge.3`, and explicit `native_options.modelProvider`,
`native_options.model`, and absolute `native_options.workspaceRoot`. Model
configuration and effort changes are unavailable; the controller never
silently chooses a model. `agent.open` records the requested route separately
from the model/provider returned by native `thread/start`. Served model and
billing remain `unknown` unless native evidence says otherwise.

`task.dispatch` and `agent.send` record the operation identity before issuing
`turn/start` or `turn/steer`. They use the operation ID as
`clientUserMessageId` and confirm the unique native user item, its content
digest, its actual item ID, and associated turn ID from history. A lost
turn-input acknowledgment is reconciled by reading history; the input is never replayed.
Steering requires the exact active turn ID. Resume is explicit through
`agent.recover`; reconnecting the WebSocket alone does not resume a thread.

Native child enumeration follows explicit `parentThreadId` links from
paginated `thread/list` responses. Pagination does not provide an atomic
family snapshot, so observations always report family completeness as
`partial`; auxiliary-provider affinity is unavailable. A bounded lifecycle
event window records native thread, turn, and item IDs/statuses observed on
the current connection, without treating notifications as complete history.

`agent.result` can publish bounded pages of native history for an exact prior
input operation. It revalidates the acknowledged user item and completed
native turn, and for direct-child results requires both the child parent link
and its activity item in the exact parent turn. The result projection keeps
native item IDs and allowlisted tool call names, arguments, and results while
omitting reasoning. Root and child thread-configured provider/model fields are
reported separately; per-turn served inference and billing remain `unknown`.
Pages are capped at 64 KiB. This reports an allowlisted projection of observed
native history only; it does not claim complete family coverage or execute
tools.

`thread/start` has no caller-selected correlation ID. A lost creation
acknowledgment therefore leaves `agent.open` unknown and reserves that opening;
the bridge cannot safely find a replacement thread or repeat creation. Native
readiness is the last recorded observation until a subsequent successful read
re-establishes it after a socket disconnect.

This bridge does not implement dynamic tools, file/command execution, goal
controls, artifacts, or background work. App-server approval requests are
answered with their schema's explicit deny/refuse form; a request the bridge
does not recognize fails closed and does not receive an empty success object.
Unsupported features are reported unavailable, not simulated.

## Fixture verification

The scripted peer fixtures validate the pinned SDK's actual JSON-RPC request
and response models; they are synthetic and do not establish live provider
availability or model service identity. A focused controller check is:

```powershell
.venv\Scripts\python.exe -m unittest test_bridge.BridgeFixtureTests.test_controller_exact_model_and_dual_native_identity_readback test_bridge.BridgeFixtureTests.test_server_requests_get_schema_shaped_non_granting_replies -v
```

The vendored `openai/codex/sdk/python` unit is kept byte-identical to upstream.
All transport and controller adaptation is ELIOT-owned code outside
`vendor_bridge/`.
