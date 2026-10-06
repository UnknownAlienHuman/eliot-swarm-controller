# Standalone Antigravity Rust adapter `.1`

Status: integrated production source in the controller workspace. The current
descriptor update still requires its compiler gate and native Antigravity
qualification; source integration does not establish installed or live behavior.

The binary is `swarm-antigravity`; this source declares immutable artifact identity `eliot-antigravity.rust-headless.1`, version `4`. Existing version 1, 2, and 3 descriptors remain retained and immutable. Version 4 adds the exact normalized-result command/event schema pair to the bounded Operation status-page contract. That schema pair admits a typed result request; it does not promise a productive assistant body. It is a separate crate with no root-library, Store, database, provider, or MCP-server dependency. Its Cargo paths target the shared `swarm-client`, `swarm-contracts`, and `swarm-process` crates after the listed PR25 package merge.

## Identity, admission, and rights

The adapter requires the guarded module-owner environment and verifies that its own process is a distinct live member of the exact non-killing owner group before connecting. The owner supplies `ELIOT_SWARM_MODULE_CONTRACT`; the adapter parses that bounded descriptor-derived claim and checks it against the exact `.1` module, artifact, protocol, capabilities, and runtime schemas. It sends the claim through the existing authenticated `module.hello`. Store negotiation remains the authority; descriptor capabilities do not grant method rights. Only the current authenticated module credential and binding scope admit `module.next`, `module.outcome`, and `module.observe`.

On a first hello the adapter may have no local native root, so it sends both native identity fields as null. The authenticated hello response carries the Store-owned `binding_id`, generation, route, retained `native_root_id` and `native_scope_key`, `recovery_required`, and negotiated descriptor. The adapter accepts the returned root only with the exact local Antigravity scope. If it already has a root, reconnect must return that exact pair. The Store hunk in `root-integration/module-hello-null-pair.patch` permits both request fields absent/null while returning the retained Store pair; it rejects partial or mismatched claims and prevents root adoption by a rootless `.1` binding.

Every retained outcome contains `details.module_receipt`, a shared `ModuleReceiptIdentity` built from the exact Store-supplied `input_sha256`. `agent.reconcile` queues a separate `Unknown` outcome for the exact target Operation using `target_input_sha256`; its `details.reconcile_operation_id` identifies the current admitted reconcile Operation. The separate `Unknown` outcome for the reconcile Operation uses its own `input_sha256` and does not carry that relation field. The shared Store helper validates both outcomes independently against the exact binding, retained descriptor, Operation ID, and canonical original request, then validates the target-to-reconcile relation. No nested target receipt is used. Already-accepted terminal target outcomes are replayed byte-identically without retrofitting the relation; the Store's exact duplicate path preserves the terminal fact. The `.2` Node artifact keeps its pre-versioned receipt path because it has no descriptor selector.

## Native behavior and restart semantics

The exact supported native model ID is `gemini-3.8-flash-high`; the selected route must specify that value. Only `agent.open` launches the configured absolute `agy` executable. Sequential `task.dispatch` and `agent.send` write one native user message only after `module.next` durably admits the Operation. An uncertain host reply to `module.next` or native write is never retried as a new Operation or prompt. Native `init` and terminal-result events are consumed from the current child stream; the reducer reports terminal facts only when the parsed result has a fingerprint and current observation ID.

The public headless CLI contract exposes stream-json events and prompt writes but no documented exact conversation-history GET or recovery method. Consequently a new process can show the root retained by its own authenticated hello, but cannot claim that the prior native transcript or an uncertain prompt was read back. `agent.refresh` returns a current-process stream snapshot only when this boot observed the exact root; otherwise it reports `Unknown` with `NATIVE_HISTORY_UNAVAILABLE`. `agent.reconcile` uses the exact target receipt path above and does not start a model, resume a conversation, or replay a prompt. No recovery completion is claimed. Version 4 `agent.result` has two bounded paths. An `antigravity_status` selector names a terminal `task.dispatch` or `agent.send` on the same retained binding session and publishes one bounded JSON page from the Store-retained target Operation outcome and diagnostic; native terminal failures are shown only when the exact typed outcome contains the matching target/session failure receipt. The normalized-result path accepts the Store-sealed origin and verifies its binding, target, dispatch, and receipt identity, then returns `RESULT_BODY_UNAVAILABLE`: the native result exposes response text, conversation ID, and a local ordinal but no native request, item, assistant-message, or turn parent. Conversation plus ordinal cannot prove input causality. The status page contains no prompt or response text, does not invent a native message/turn identity, sets `native_response_identity` to `unavailable`, and keeps `execution_complete: false` and `task_completion: "unknown"`. Neither path accepts a Task, replays a native prompt, or qualifies productive result readback.

Unsupported permission/configuration replies, goals, background work, and `agent.recover` return bounded rejection receipts. The adapter creates no Task, changes no owner decisions, starts no worker except the manager-admitted `agent.open`, and adds no method privilege beyond the negotiated `agent.result` status-page method.

## Trusted registration and route

`src/contract.rs::template` declares the descriptor contract: module `antigravity`; artifact `eliot-antigravity.rust-headless.1` version `4`; protocol `1.0`; capabilities `agent.open`, `agent.reconcile`, `agent.refresh`, `agent.result`, `agent.send/next_turn`, `task.dispatch`; command schemas `swarm.normalized_result_context@1`, `swarm.runtime_command@1`, and `swarm.task_dispatch_context@1`; event schemas `swarm.normalized_result_page@1`, `swarm.runtime_outcome@1`, and `swarm.task_dispatch_admission@1`; and workspace option `/workspaceRoot` with `replace_with_admitted_absolute_workspace` semantics. The local `claim()` reads the exact manager-provided contract and rejects any capability/schema drift. A trusted installer or supervisor must register the descriptor and select version `4` for new bindings; the adapter claim cannot register or select itself. The optional `build_id` is taken from the trusted launcher claim and participates in Store negotiation and receipts.

The existing `.2` artifact, route, and binary are left intact. `root-integration/warm-stream-compat.patch` adds `.1` to the same validated terminal warm-stream contract and returns the exact route artifact from recovery metadata. The transaction-bus receipt validator must be merged before this crate; its selector-pinned call validates each typed `.1` outcome and leaves legacy `.2` unversioned behavior unchanged.

Example local configuration, supplied outside this package:

```json
{
  "hostDataDir": "C:\\SwarmState",
  "credentialFile": "C:\\SwarmState\\antigravity-module.credential.json",
  "nativeExecutable": "C:\\Tools\\Antigravity\\agy.exe",
  "moduleArtifactId": "eliot-antigravity.rust-headless.1"
}
```

The route must use `runtime = "antigravity"`, the exact artifact ID, an absolute `native_options.workspaceRoot`, and `native_options.modelId = "gemini-3.8-flash-high"`. Invoke only through the existing guarded module runner; a direct local launch lacks the manager-supplied contract and owner membership.

## Source anchors and validation boundary

- Producer and effect handler: `src/main.rs::run`, `wait_for_open`, `wait_for_terminal`, and `agent.result`; bounded status page and truthful normalized-result capability boundary: `src/result_page.rs::build` and `build_normalized`.
- Descriptor claim and registration contract: `src/contract.rs::template/claim`; authenticated handshake and negotiated-response reader: `src/ipc.rs::ManagerLink::hello/validate_negotiation`.
- Store-supplied identity reader: `src/main.rs::HostSession::accept_hello`, `controller_from_hello`, and `src/controller.rs::Controller::new`.
- Per-operation and target receipt producer: `src/module_receipt.rs::for_command/for_reconcile_target/serialize_outcome`, `src/controller.rs::record_outcome/read_command` (including target `details.reconcile_operation_id`), and `src/wire.rs::OperationIdentity::try_from`.
- Existing host handlers and rights: `src/store/mod.rs::call` (`module.hello`, `module.next`, `module.outcome`, `module.observe`, `module.result`) and `src/store/runtime.rs::hello/next/outcome/observe`. Store status provenance is `src/store/results.rs::antigravity_status_snapshot/validate_antigravity_status_source`; the generic validator is `src/store/runtime.rs::validate_module_receipt_for_operation` from the transaction-bus recovery package; current and target receipts are checked independently.
- Native consumer and result parser: `src/process.rs::OwnedNativeSpawner::spawn_owned`, `NativeMembershipGuard::verify`, `src/stream.rs::NativeLineReader::next_line/StreamState::consume_line/apply_result`, and `src/controller.rs::settle_terminal`.
- Exact route compatibility and recovery artifact identity: `root-integration/warm-stream-compat.patch`.

The patch set is source-reviewed only. No Cargo, formatter, build, tests, native/provider/API call, database operation, or Git mutation was run. Runtime behavior, Windows/Linux builds, trusted descriptor installation, manager IPC, live Antigravity availability, served-model identity, productive result-body readback, and whole-family supervisor departure remain unverified.
