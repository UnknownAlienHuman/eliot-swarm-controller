# OpenCode V2 module adapter

eliot-opencode-v2.rust-http.1 is the standalone OpenCode adapter. The prepared registration contract is artifact version 0.3.0, config schema opencode-v2-native-options@2 (SHA-256 7fc3136219b20d00570b65e5d4fe533e3ea042dadf53be3fdcdfa9781cf0eb68), and protocol 1.0. The descriptor is disabled by default and activates on demand; a trusted catalog and an explicitly selected route are still required.

The descriptor declares agent.open, agent.reconcile, agent.result, agent.send/next_turn, and task.dispatch. Its four native MCP command capabilities are native.mcp.arm, native.mcp.install, native.mcp.observe, and native.mcp.read. It binds the swarm.native_mcp_command@1, swarm.normalized_result_context@1, swarm.runtime_command@1, and swarm.task_dispatch_context@1 command schemas, plus the normalized result page, runtime outcome, and task dispatch admission event schemas.

## Service lifecycle

The adapter retains the external-attach path when a route omits owned_service. In this mode, the route supplies the exact service_id, existing connection file, expected server version, workspace, and provider/model/variant. The adapter uses that connection and never starts, stops, adopts, or restarts the native OpenCode service. An unavailable attached service is not replaced by an implicit fresh owner.

A route can request a fresh owned service only with an explicit owned_service declaration whose origin is fresh_owned_service. The Store admission and retained owner intent provide the exact task, binding, workspace, and nonce scope before the adapter receives private owner options. The adapter writes its operation intent before it starts the pinned child. This owner path uses the pinned Bun 1.4.0 runtime, repository-pinned OpenCode 2.0.7 server, and project-local plugin source, with pinned executable hashes and a separate fresh state root. It makes one launch attempt for that fresh state root; uncertain launches are not replayed or adopted. A partial or invalid owner declaration fails closed rather than falling back to external attach.

The example model is the exact selected route inclusionai/ling-3.1-flash: providerID is inclusionai, id is ling-3.1-flash, and variant must be copied exactly from the selected Manager route. Owner credentials are referenced only through an opaque protected credential_ref. The host-side protected mapping must resolve that reference to the same exact provider ID. Examples contain no credential values or auth-file paths.

The pinned MCP plugin code prepares a bounded config in the fresh owner's private OpenCode config. It does not install a global plugin, issue an MCP effect by itself, or prove that OpenCode loaded the plugin. The four native MCP commands remain individually descriptor- and Store-gated.

## Normalized result boundary

agent.result is declared with swarm.normalized_result_page@1 and requires an exact selector containing the dispatch operation, native session, and assistant message IDs, validated against the saved dispatch receipt. This is a bounded normalized-result consumer; the declaration and selector checks do not qualify runtime result-body causality. Dispatch-side proof that the selected body is the causal result remains pending, so this package description makes no end-to-end runtime qualification or task-completion claim.

Artifact version 0.3.0 is selected explicitly. The retained legacy v1 artifact is a separate registration target, not a fallback when v0.3.0 is disabled, unavailable, or rejected.

## Registration examples

- registration/route.example.json shows external attach.
- registration/route.fresh-owner.example.json shows the explicit fresh-owner declaration and opaque protected provider reference. Its path and digest placeholders must be replaced with locally reviewed values; the example remains disabled.
- registration/binding-launch-values.example.json shows the seven required route fields for the external-attach path. Owner fields are supplied only when the explicit owner declaration is admitted.
- registration/select-route.request.example.json selects artifact 0.3.0.

The adapter launch argument remains --config <absolute-host-connection-config-path>. That route-neutral file contains only HostConnectionConfig (schema_version, host_data_dir, and bounded IPC settings); descriptor-declared route values are supplied separately by the supervisor. The adapter was not built or tested as part of this documentation packet.