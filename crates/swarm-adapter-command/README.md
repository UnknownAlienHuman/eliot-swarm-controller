# Standalone Command adapter

This crate has two separately identified profiles: sessionless native Command Batch and native ACP sessions. Installer descriptors and example routes remain disabled by default. The Rust source includes the Batch v4 TaskPrompt dispatch and ACP v1 session, permission, cancellation, and bounded-result paths described below. Neither profile is runtime-qualified: connected Clippy, full checks, and native execution remain pending.

## Batch artifact v4

`module-descriptor.template.json` retains artifact ID `eliot-command.rust-headless.1` and declares version `4`. It binds the normalized result schemas plus `swarm.task_prompt@1`, `swarm.task_dispatch_context@1`, and `swarm.task_dispatch_admission@1`. The v4 dispatch consumes the exact Store-produced TaskPrompt envelope and context, then builds dispatch admission from that data; it does not render a prompt from an independently read Task snapshot.

The Rust source declares `Profile::BatchV4`, selects version 4, and makes `src/module_host.rs` require the exact command and event schema sets in this descriptor. The Batch v4 `task.dispatch` branch consumes the Store-produced TaskPrompt envelope and context through `acp_prompt::prepare`, then derives the exact prompt identity and dispatch admission before native invocation; it does not render a prompt from an independently read Task snapshot. Profile-aware config validation includes BatchV4. Keep this descriptor disabled pending connected Clippy, full checks, and native execution qualification; contract negotiation alone does not qualify an installed runtime.

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

The ACP config parser requires an absolute native executable and accepts either exactly `command_args: ["acp"]` or exactly `["<ABSOLUTE_BOUNDED_NATIVE_ENTRYPOINT>", "acp"]` for direct Node/npm CLI invocation. It does not invoke a shell or accept additional flags, a mod path, or a Batch timeout. The descriptor launch still passes the supervisor's typed private host-config marker and the absolute path to this operator-maintained ACP config. The route uses `workspaceRoot` and an explicit operator-selected `modelId`; the workspace is admitted by Store and supplied to the adapter. The example has no account, credential, or model pin. Keep the route disabled pending runtime qualification.

Store owns route selection, Task identity, TaskPrompt construction, dispatch context, and admission. The adapter owns the local ACP child process and its stdio connection, registers a `SessionNotification` receiver on the ACP connection, and retains bounded notification text and stderr under the binding's private `command-acp-runs-v1` evidence directory; there is no separate ACP subscription RPC. Those notifications are observations, not completion proof. A successful `session/prompt` response establishes native turn completion; Task completion remains unknown.

At connection start the adapter negotiates ACP v1 and reads whether `session/close` is advertised. For a new session it reads `session/new` config options, verifies that the exact routed model is available, sets it if needed, and checks the returned config options for exact model readback. `agent.configure` only confirms the already selected model; changing it requires a new route and a new verified session. The adapter does not infer account identity or permission from model metadata.

The adapter journals the native session and per-Operation TaskPrompt identity before native prompt dispatch. If the native or module connection ends, it retains the capture and marks the session recovery-required; recovery loads only the exact retained session and reads back its routed model, a retained native session prevents opening a second session, and an uncertain prompt is never replayed. `agent.reply` is bound to the exact live permission request fingerprint and one advertised native option; persistent permission choices require a separate policy mutation and readback. `native.command.cancel_turn` targets the exact retained session and active turn. `agent.result` returns bounded pages from the exact retained assistant capture and applies a completeness gate. The host matcher accepts the base ACP capability set with or without `native.command.close_session`; runtime close support is advertised only when the ACP agent reports `session/close`.

### ACP runtime qualification still required

`src/module_host.rs` accepts the exact sorted ACP capability set with `agent.reply` and `agent.result`, plus the optional `native.command.close_session`, and requires the four command / three event schemas declared by this descriptor. The ACP command path includes permission reply, retained-session load, exact active-turn cancellation, and bounded result-page handling. Runtime qualification remains pending: connected Clippy, full checks, and native execution have not been completed, and the descriptor and route stay disabled until those gates are satisfied.
