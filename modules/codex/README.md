# Codex app-server integration

The current executor is the [Rust Codex adapter](../../crates/swarm-adapter-codex/src/lib.rs),
identified by [its descriptor](../../crates/swarm-adapter-codex/module-descriptor.template.json)
as `codex-rust-controller.1`, version `11.4`. It attaches to an existing native
app-server. Provider authentication, configuration and the native model loop
remain with Codex; the adapter owns its connection and controller Operations.

Use the [module installation guide](../../docs/agent-operations/module-installation.md)
for configuration and registration. This directory's Python bridge and vendored
SDK are retired implementation material; they are not dependencies of the Rust
adapter. New bindings to `codex-sdk-18194bf-bridge.3` are rejected by
[`require_new_binding`](../../crates/swarm-kernel-host/src/store/task_prompt.rs).

## Current behavior

| Boundary | Rust owner behavior |
| --- | --- |
| Connection | Attach to the configured app-server; do not start or stop that shared service. Reconnection does not replay input or resume a thread implicitly. |
| Native defaults | `workspaceRoot` is the admitted absolute workspace. Omitted `modelProvider` and `model` use native defaults; supplied values must match native readback. Observed values do not become requested route settings. |
| Input | `task.dispatch` consumes the Store-produced TaskPrompt. Dispatch/send record intent before native input, correlate `clientUserMessageId` with the Operation and read back the native user item and turn. Steering addresses the active turn. |
| Result | Read the final assistant response from the exact completed root turn. Preserve its input/turn/item association and explicit missing-result diagnostics. Child-history/tool projection from the old Python bridge is outside this Rust result path. |
| Shutdown | `agent.shutdown` releases the adapter connection and drains saved outcomes. It does not terminate the external app-server or declare Task acceptance. |

The result selector is:

```json
{"kind":"codex_assistant_result","input_operation_id":"<task.dispatch operation ID>"}
```

Current source caps one result page at 24 KiB and the complete response at 4 MiB.
The Store's larger artifact envelope does not increase those adapter limits.
`thread/start` has no caller-selected correlation ID: a lost creation ACK leaves
the opening outcome unknown, so the controller must not create another thread
to guess the result. Native approvals are refused according to their request
schema; unsupported commands remain unavailable.

## Retired Python path

The Python controller, passive observer, fixture suite and SDK vendor tree remain
in source, and the existing Justfile/CI still exercise them. Their presence does
not establish a supported second executor or a requirement to install Python for
Codex integration. Historical source details are recorded in [UPDATE.md](UPDATE.md)
and Git history.

The [executor retirement plan](../../docs/agent-operations/implementation.md#current-format-and-existing-data)
removes this old execution/package/CI path. If concrete saved work needs it,
finish or reconcile that work once and export the required results; archive
completed records without keeping the old controller in the normal runtime.
The small [host receipt decoder](../../crates/swarm-kernel-host/src/runtime/codex.rs)
is a separate current source dependency, not a reason to retain the whole Python
SDK. Remove its special cases with the relevant data transition.

## Verification

Follow the [project's scoped checks](../../docs/ci-scoped-builds.md) for the
changed Rust boundary. Existing adapter fixtures exercise protocol and result
behavior; native service coverage belongs to the recorded scope in
[implementation status](../../docs/implementation-status.md). Documentation
changes do not require a Python environment, adapter build or model run.
