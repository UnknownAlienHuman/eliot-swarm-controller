# Standalone Command adapter

This crate has two separately identified profiles: sessionless native Command Batch and native ACP sessions. Current Batch v5 and ACP v2 bindings may leave `modelId` absent (or null at the input boundary) so Command uses its native default; explicit per-invocation model selection remains supported. Store routes should omit an unselected value. Batch v3/v4 and ACP v1 retain their required-model behavior and exact versioned receipt namespaces. Installer descriptors and example routes remain disabled by default. Runtime qualification is still pending.

## Current Batch artifact v5

`module-descriptor.template.json` declares artifact ID `eliot-command.rust-headless.1`, version `5`, and the existing normalized result and TaskPrompt schemas. The v5 dispatch consumes the exact Store-produced TaskPrompt envelope and context, then builds dispatch admission from that data; it does not render a prompt from an independently read Task snapshot. When the immutable route leaves `native_options.modelId` absent or null, the adapter omits `--model` from native argv and retains `requested_model: null`. Complete native model request events may provide `effective_model` readback; an observed model never becomes a requested route constraint.

Batch v3 and v4 remain explicit-model profiles. Each artifact version keeps its original run journal directory so prior admissions and receipts retain their original requested-model identity.

The native configuration shape, shown in `config/swarm-adapter-command-v3.example.json`, remains the same across Batch profiles:

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

The Batch descriptor uses a typed `module_host_config_path` argument for the supervisor's private binding config, followed by `--config` and the absolute operator config path. Keep the typed marker intact. The installer supplies the executable path and digest and binds the exact installed descriptor. It does not install or attest the operator-maintained native Command files. Versions 2, 3, and 4 remain supported for their registered bindings; existing bindings are not rewritten.

The v3 normalized result path reads bounded status or captured stdout/stderr pages and checks them against the sealed Store digest. Raw CLI bytes do not establish assistant identity or Task completion. Incomplete or truncated captures cannot be promoted as complete candidates; Task acceptance remains a separate Manager decision. Unknown native work is not replayed to manufacture a result.

## Current ACP artifact v2

`module-descriptor-acp.template.json` and `route-acp.example.json` use the source identifiers `eliot-command.acp-rust.1` / version `2`. The ACP native config is a separate file and must have this shape:

```json
{
  "module_artifact_id": "eliot-command.acp-rust.1",
  "command": "<ABSOLUTE_NATIVE_COMMAND_EXECUTABLE>",
  "command_args": ["acp"]
}
```

The ACP config parser requires an absolute native executable and accepts either exactly `command_args: ["acp"]` or exactly `["<ABSOLUTE_BOUNDED_NATIVE_ENTRYPOINT>", "acp"]` for direct Node/npm CLI invocation. It does not invoke a shell or accept additional flags, a mod path, or a Batch timeout. The descriptor launch still passes the supervisor's typed private host-config marker and the absolute path to this operator-maintained ACP config. The route requires `workspaceRoot`; `modelId` may be absent or null to use Command's native default, or supplied for an explicit selection. Effort and mode remain optional exact advertised values. The workspace is admitted by Store and supplied to the adapter. The example has no account, credential, or model pin. Keep the route disabled pending runtime qualification.

Store owns route selection, Task identity, TaskPrompt construction, dispatch context, and admission. The adapter owns the local ACP child process and its stdio connection, registers a `SessionNotification` receiver on the ACP connection, and retains bounded notification text and stderr under the binding's versioned private evidence directory (`command-acp-runs-v1` or `command-acp-runs-v2`); there is no separate ACP subscription RPC. Those notifications are observations, not completion proof. A successful `session/prompt` response establishes native turn completion; Task completion remains unknown.

At connection start the adapter negotiates ACP v1 and reads whether `session/close` is advertised. For an explicit `modelId`, a new session must advertise that value in one typed `model` selector; the adapter applies it with `session/set_config_option` and requires exact readback. When `modelId` is absent or null, it makes no model-setting call and records the current typed model value as observed readback when the session exposes one unambiguously. The requested model remains absent in the route and receipts. Requested effort uses the typed `thought_level` selector and exact readback. For mode, the adapter uses `session/set_mode` when the session advertises that API. If it does not, one typed `mode` config selector may be applied with `session/set_config_option` and must return exact readback. These checks complete before any prompt is sent. The artifact does not advertise a mid-session `agent.configure` command. Unknown route options fail before ACP process startup, and unsupported requested values fail before prompt input. The adapter retains bounded typed option IDs, categories, allowed values and current values. It does not infer account identity or permission from model metadata.

The adapter journals the native session and per-Operation TaskPrompt identity before native prompt dispatch. ACP v2 stores sessions under `command-acp-runs-v2`; ACP v1 keeps its original `command-acp-runs-v1` bytes and required-model identity. If the native or module connection ends, the adapter retains the capture and marks the session recovery-required; recovery loads only the exact retained session and reads back the model plus any selected effort and mode. For an omitted model, recovery compares any previously observed effective value with the loaded session while keeping the requested value absent. It uses `SessionModeState` when returned and otherwise requires the typed `mode` config selector when available; legacy mode notifications remain a readback path when no typed selector was advertised. A retained native session prevents opening a second session, and an uncertain prompt is never replayed. `agent.reply` is bound to the exact live permission request fingerprint and one advertised native option; persistent permission choices require a separate policy mutation and readback. `native.command.cancel_turn` targets the exact retained session and active turn. `agent.result` returns bounded pages from the exact retained assistant capture and applies a completeness gate. The host matcher accepts the base ACP capability set with or without `native.command.close_session`; runtime close support is advertised only when the ACP agent reports `session/close`.

### ACP runtime qualification still required

`src/module_host.rs` accepts the exact sorted ACP capability set with `agent.reply` and `agent.result`, plus the optional `native.command.close_session`, and requires the four command / three event schemas declared by this descriptor. ACP v1 remains accepted with its explicit-model and v1 receipt rules. The ACP command path includes permission reply, retained-session load, exact active-turn cancellation, and bounded result-page handling. Runtime qualification remains pending; the descriptor and route stay disabled until the root-owned checks and native qualification are complete.
