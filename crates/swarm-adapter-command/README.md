# Standalone Command adapter

This crate has two separately identified profiles: sessionless native Command Batch and native ACP sessions. The installer descriptor and route are disabled by default. The Batch v4 and ACP v1 descriptors below describe their intended contracts; the current Rust source has integration gaps listed in each profile, so neither new descriptor is runtime-qualified by this checkout.

## Batch artifact v4 target

`module-descriptor.template.json` retains artifact ID `eliot-command.rust-headless.1` and declares version `4`. It binds the normalized result schemas plus `swarm.task_prompt@1`, `swarm.task_dispatch_context@1`, and `swarm.task_dispatch_admission@1`. The intended v4 dispatch consumes the exact Store-produced TaskPrompt envelope, with its context and admission receipt; it must not render a prompt from an independently read Task snapshot.

The current Rust source now declares `Profile::BatchV4`, selects version 4, and makes `src/module_host.rs` require the same exact command and event schema sets as this descriptor. The Batch consumer is still an integration gap: `adapter::run` sends only ACP to the ACP path, while Batch `task.dispatch` still derives its prompt through `native::prompt_for` and does not validate or consume the Store TaskPrompt envelope. The source's config-profile match also has branches only for BatchV3 and ACP. Keep this descriptor disabled and do not select it until the Rust owner connects Batch v4 dispatch to the exact TaskPrompt/context/admission data and completes the profile wiring. Contract negotiation alone does not prove prompt consumption; no Rust build was run here.

The current Batch v3 native configuration example is `config/swarm-adapter-command-v3.example.json`. Its shape is:

```json
{
  "module_artifact_id": "eliot-command.rust-headless.1",
  "command": "C:\\ELIOT\\bin\\command.exe",
  "command_args": [],
  "mod_path": "C:\\ELIOT\\mods\\command\\.3\\command.js",
  "run_timeout_ms": 1800000
}
```

That native JSON is operator-maintained, external to the descriptor and install receipt. Replace sample paths with absolute local paths. The Command mod must match the digest enforced by the selected Batch binary. `command_args` is bounded and cannot override adapter-owned print, model, workspace, or mod flags. Native credentials stay in the native tool's existing local credential store; do not put them in the descriptor, route, or adapter JSON.

The Batch descriptor uses a typed `module_host_config_path` argument for the supervisor's private binding config, followed by `--config` and the absolute operator config path. Keep the typed marker intact. The installer supplies the executable path and digest and binds the exact installed descriptor. It does not install or attest the operator-maintained native Command files. Version 2 remains available through the untouched `module-descriptor-v2.template.json` and `config/swarm-adapter-command.example.json`; existing bindings are not rewritten.

The v3 normalized result path reads bounded status or captured stdout/stderr pages and checks them against the sealed Store digest. Raw CLI bytes do not establish assistant identity or Task completion. Incomplete or truncated captures cannot be promoted as complete candidates; Task acceptance remains a separate Manager decision. Unknown native work is not replayed to manufacture a result.

## ACP artifact v1

`module-descriptor-acp.template.json` and `route-acp.example.json` use the source identifiers `eliot-command.acp-rust.1` / version `1`. The ACP native config is a separate file and must have this shape:

```json
{
  "module_artifact_id": "eliot-command.acp-rust.1",
  "command": "<ABSOLUTE_NATIVE_COMMAND_EXECUTABLE>",
  "command_args": ["acp"]
}
```

The ACP config parser requires an absolute native executable and exactly the fixed `acp` argument; it rejects a mod path and Batch timeout. The descriptor launch still passes the supervisor's typed private host-config marker and the absolute path to this operator-maintained ACP config. The route uses `workspaceRoot` and an explicit operator-selected `modelId`; the workspace is admitted by Store and supplied to the adapter. The example has no account, credential, or model pin. Keep the route disabled until the ACP contract gap below is resolved.

Store owns route selection, Task identity, TaskPrompt construction, dispatch context, and admission. The adapter owns the local ACP child process and its stdio connection, registers a `SessionNotification` receiver on the ACP connection, and retains bounded notification text and stderr under the binding's private `command-acp-runs-v1` evidence directory; there is no separate ACP subscription RPC. Those notifications are observations, not completion proof. A successful `session/prompt` response establishes native turn completion; Task completion remains unknown.

At connection start the adapter negotiates ACP v1 and reads whether `session/close` is advertised. For a new session it reads `session/new` config options, verifies that the exact routed model is available, sets it if needed, and checks the returned config options for exact model readback. `agent.configure` only confirms the already selected model; changing it requires a new route and a new verified session. The adapter does not infer account identity or permission from model metadata.

The adapter journals the native session and per-Operation TaskPrompt identity before native prompt dispatch. If the native or module connection ends, it retains the capture and marks the session recovery-required; a retained native session prevents opening a second session, and an uncertain prompt is never replayed. `native.command.close_session` is optional in the host matcher and should be advertised only when the ACP agent reports `session/close` support.

### ACP runtime qualification still required

The current `src/module_host.rs` requires the exact ACP capability set containing `agent.reply` and `agent.result` (plus the optional `native.command.close_session`) and the exact four command / three event schemas. The current ACP command path returns `NO_PENDING_ACP_PERMISSION_REQUEST` for every `agent.reply` and `CAPABILITY_UNAVAILABLE` for `agent.result` because result-page projection is not wired. This template therefore omits those unsupported capabilities rather than claiming permission-reply or result support; as a result, the current host matcher will reject it. Keep the descriptor and route disabled until the Rust owner either implements those operations or revises the host contract to the actually supported capability set.

Source inspection also found an unfinished permission-notification path in the active ACP source: the connection builder calls `handle_permission_request` and passes permission broker arguments to `process_command`, but no handler definition is present and the current `process_command` signature does not accept those arguments. No Rust compile or runtime qualification was run for this documentation-only slice. The ACP artifact, notification delivery, and model readback remain unqualified in an installed runtime.
