# Codex app-server bridge

This module directory documents the passive observer and two separately
versioned controller artifacts over an operator-owned Codex app-server. The
legacy Python controller is artifact `codex-sdk-18194bf-bridge.3`; the
standalone Rust controller is artifact `codex-rust-controller.1`, current
descriptor version `11.2`, sourced from `crates/swarm-adapter-codex`. Both attach
to the existing app-server and never start or stop it. Their result contracts
are documented separately below.

## Setup

The Python setup below is historical donor tooling, not required by the current Rust controller. The Rust route attaches to an already configured native app-server; each omitted model/provider selector uses that harness's default. Version 11.2 retains v11's native defaults and v11.1's acknowledged opening proof, and declares the native experimental API capability required by its paginated history reads. Older bindings and receipts retain their original identity and installed bytes.

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

The setup and observer commands in this section belong to the legacy Python
bridge artifact `.3`. The standalone Rust artifact `.1`, current descriptor
version `11.2`, uses its own package and descriptor under
`crates/swarm-adapter-codex`; it does not use
this module-local Python environment or `vendor_bridge`.

## Observer CLI

```powershell
.venv\Scripts\python.exe bridge.py --config C:\SwarmState\codex-module.json describe
.venv\Scripts\python.exe bridge.py --config C:\SwarmState\codex-module.json open --thread <native-thread-id>
.venv\Scripts\python.exe bridge.py --config C:\SwarmState\codex-module.json snapshot --limit 20
```

The observer attaches passively. Its client allowlist permits only
`initialize`, `initialized`, `thread/read`, and `thread/list`.

## Legacy Python registered controller (`codex-sdk-18194bf-bridge.3`)

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

The legacy `.3` controller enumerates native child threads through explicit
`parentThreadId` links from
paginated `thread/list` responses. Pagination does not provide an atomic
family snapshot, so observations always report family completeness as
`partial`; auxiliary-provider affinity is unavailable. A bounded lifecycle
event window records native thread, turn, and item IDs/statuses observed on
the current connection, without treating notifications as complete history.

The legacy `.3` `agent.result` can publish bounded pages of native history for
one exact prior input operation. It revalidates the acknowledged user item and
completed native turn. For direct-child results, it requires both the child
parent link and its activity item in the exact parent turn. The result
projection keeps native item IDs and allowlisted tool call names, arguments,
and results while omitting reasoning. Root and child thread-configured
provider/model fields are reported separately; per-turn served inference and
billing remain `unknown`.
Pages in the legacy `.3` controller are capped at 64 KiB. This reports an
allowlisted projection of observed native history only; it does not claim
complete family coverage or execute tools.

## Standalone Rust controller (`codex-rust-controller.1`, current descriptor version `11.2`)

The standalone Rust adapter is a separate registered artifact from the legacy
Python bridge. Its route uses `runtime = "codex"` and the exact artifact
`codex-rust-controller.1`; its descriptor is
`crates/swarm-adapter-codex/module-descriptor.template.json`. The current
v11.2 descriptor retains v11's normalized dispatch/result schema pair,
TaskPrompt v1 and Goal continuation admission/terminal evidence schemas.
The route contract requires
`workspace_option = "workspaceRoot"` and absolute `workspaceRoot`.
For current v11.2 bindings, `modelProvider` and `model` are independently optional
`thread/start` overrides. Omit either field to use its installed harness default;
each supplied field must match native readback. The pinned native
`ThreadStartParams` permits independent omission
(`model: str | None = None`, `modelProvider: str | None = None`);
per-turn `model` is likewise omitted when no override was requested. The
adapter never infers a provider from a model string, never reconfigures the
provider, and never substitutes a proxy/delegation model ID. `agent.open`
records each requested selector (null when omitted) separately from the
effective pair from native `thread/read` readback; served model and billing
remain `unknown` unless native evidence says otherwise. The result selector remains:

```json
{"kind":"codex_assistant_result","input_operation_id":"<exact task.dispatch operation ID>"}
```

The v8 result path retains the v4 boundary: one root-thread final assistant
response. It checks the Store-sealed target dispatch and producer, finds the
unique native user item for that exact dispatch, reads the exact root turn,
requires completed status without an error, and selects the final assistant
response from that turn. It reports `execution_complete = false`,
`task_completion = "unknown"`, and `native_replay = false`; it does not claim
Task completion or acceptance.

The verified native turn ID is retained on the exact parent `task.dispatch`
Operation and is used for readback validation. The normalized page source
retains the response item identity and the sealed parent Operation identity;
it does not fabricate a separate self-contained turn ID. The v8 adapter accepts
at most 24 KiB per page and 4 MiB for the complete response. The shared Store
artifact envelope is 64 KiB, but that larger envelope is not the v8 adapter's
accepted page limit.

The current v8 adapter does not enumerate child threads, verify `parentThreadId` or
`subAgentActivity`, or project child history and allowlisted tool items. The
legacy `.3` child/history/tool projection above remains available only through
that legacy artifact. No v8 selector or descriptor row requires child ancestry;
v8 must return an unsupported or unavailable result rather than infer child
causality from session, cwd, turn order, or family enumeration.

The current descriptor also declares `agent.shutdown` with only
`owned_scope = "external_attach"`. It releases the adapter-owned connection,
retains intent and uncertain effects, and drains saved outcomes through Store
ACKs without native input replay. Selected intent recovery checks the retained
native scope before any journal mutation. It never stops the shared app-server
or claims Task/family completion. See the [scope contract](../../docs/agent_swarm.module-contract-v2.md).

`thread/start` has no caller-selected correlation ID. A lost creation
acknowledgment therefore leaves `agent.open` unknown and reserves that opening;
the bridge cannot safely find a replacement thread or repeat creation. Native
readiness is the last recorded observation until a subsequent successful read
re-establishes it after a socket disconnect.

Neither controller artifact implements dynamic tools, file/command execution,
goal controls, artifacts, or background work. App-server approval requests are
answered with their schema's explicit deny/refuse form; a request the bridge
does not recognize fails closed and does not receive an empty success object.
Unsupported features are reported unavailable, not simulated.

## Fixture verification

The scripted peer fixtures validate the legacy `.3` bridge's pinned SDK
JSON-RPC request and response models; they are synthetic and do not establish
live provider availability or model service identity. A focused controller check
is:

```powershell
.venv\Scripts\python.exe -m unittest test_bridge.BridgeFixtureTests.test_controller_exact_model_and_dual_native_identity_readback test_bridge.BridgeFixtureTests.test_server_requests_get_schema_shaped_non_granting_replies -v
```

The vendored `openai/codex/sdk/python` source and generated models retain the
pinned upstream bytes. The dev/test generator pin and lockfile carry the
[documented security update](VENDOR-SECURITY-PATCHES.md) to `0.81.0`.
`SHA256SUMS` verifies the complete unit with that delta. All transport and
controller adaptation is ELIOT-owned code outside `vendor_bridge/`.
