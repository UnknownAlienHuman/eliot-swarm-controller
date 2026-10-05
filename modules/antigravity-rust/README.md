# Standalone Antigravity Rust adapter `.1`

Status: private source overlay based on the preserved `b7201c1` Rust draft plus the current shared contracts, client, process-owner, handshake, and Store-receipt proposals. Root owns the only tracked-source integration. This package has not been built, tested, installed, or run against Antigravity.

The binary is `swarm-antigravity`; its immutable artifact identity is `eliot-antigravity.rust-headless.1`, version `1`. It is a separate crate with no root-library, Store, database, provider, or MCP-server dependency. Its Cargo paths target the shared `swarm-client`, `swarm-contracts`, and `swarm-process` crates after the listed PR25 package merge.

## Identity, admission, and rights

The adapter requires the guarded module-owner environment and verifies that its own process is a distinct live member of the exact non-killing owner group before connecting. The owner supplies `ELIOT_SWARM_MODULE_CONTRACT`; the adapter parses that bounded descriptor-derived claim and checks it against the exact `.1` module, artifact, protocol, capabilities, and runtime schemas. It sends the claim through the existing authenticated `module.hello`. Store negotiation remains the authority; descriptor capabilities do not grant method rights. Only the current authenticated module credential and binding scope admit `module.next`, `module.outcome`, and `module.observe`.

On a first hello the adapter may have no local native root, so it sends both native identity fields as null. The authenticated hello response carries the Store-owned `binding_id`, generation, route, retained `native_root_id` and `native_scope_key`, `recovery_required`, and negotiated descriptor. The adapter accepts the returned root only with the exact local Antigravity scope. If it already has a root, reconnect must return that exact pair. The Store hunk in `root-integration/module-hello-null-pair.patch` permits both request fields absent/null while returning the retained Store pair; it rejects partial or mismatched claims and prevents root adoption by a rootless `.1` binding.

Every retained outcome contains `details.module_receipt`, a shared `ModuleReceiptIdentity` built from the exact Store-supplied `input_sha256`. `agent.reconcile` queues a separate `Unknown` outcome for the exact target Operation using `target_input_sha256`; its `details.reconcile_operation_id` identifies the current admitted reconcile Operation. The separate `Unknown` outcome for the reconcile Operation uses its own `input_sha256` and does not carry that relation field. The shared Store helper validates both outcomes independently against the exact binding, retained descriptor, Operation ID, and canonical original request, then validates the target-to-reconcile relation. No nested target receipt is used. Already-accepted terminal target outcomes are replayed byte-identically without retrofitting the relation; the Store's exact duplicate path preserves the terminal fact. The `.2` Node artifact keeps its pre-versioned receipt path because it has no descriptor selector.

## Native behavior and restart semantics

The exact supported native model ID is `gemini-3.8-flash-high`; the selected route must specify that value. Only `agent.open` launches the configured absolute `agy` executable. Sequential `task.dispatch` and `agent.send` write one native user message only after `module.next` durably admits the Operation. An uncertain host reply to `module.next` or native write is never retried as a new Operation or prompt. Native `init` and terminal-result events are consumed from the current child stream; the reducer reports terminal facts only when the parsed result has a fingerprint and current observation ID.

The public headless CLI contract exposes stream-json events and prompt writes but no documented exact conversation-history GET or recovery method. Consequently a new process can show the root retained by its own authenticated hello, but cannot claim that the prior native transcript or an uncertain prompt was read back. `agent.refresh` returns a current-process stream snapshot only when this boot observed the exact root; otherwise it reports `Unknown` with `NATIVE_HISTORY_UNAVAILABLE`. `agent.reconcile` uses the exact target receipt path above and does not start a model, resume a conversation, or replay a prompt. No recovery completion is claimed.

Unsupported permission/configuration replies, goals, background work, result paging, and `agent.recover` return bounded rejection receipts. The adapter creates no Task, changes no owner decisions, starts no worker except the manager-admitted `agent.open`, and adds no method privilege.

## Trusted registration and route

`src/contract.rs::template` declares the descriptor contract: module `antigravity`; artifact `eliot-antigravity.rust-headless.1` version `1`; protocol `1.0`; capabilities `agent.open`, `agent.refresh`, `agent.reconcile`, `agent.send`, `task.dispatch`; and the shared runtime command/outcome schemas. The local `claim()` reads the exact manager-provided contract and rejects any capability/schema drift. A trusted installer or supervisor must register the descriptor and select it for new bindings; the adapter claim cannot register or select itself. The optional `build_id` is taken from the trusted launcher claim and participates in Store negotiation and receipts.

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

- Producer and effect handler: `src/main.rs::run`, `wait_for_open`, and `wait_for_terminal`.
- Descriptor claim and registration contract: `src/contract.rs::template/claim`; authenticated handshake and negotiated-response reader: `src/ipc.rs::ManagerLink::hello/validate_negotiation`.
- Store-supplied identity reader: `src/main.rs::HostSession::accept_hello`, `controller_from_hello`, and `src/controller.rs::Controller::new`.
- Per-operation and target receipt producer: `src/module_receipt.rs::for_command/for_reconcile_target/serialize_outcome`, `src/controller.rs::record_outcome/read_command` (including target `details.reconcile_operation_id`), and `src/wire.rs::OperationIdentity::try_from`.
- Existing host handlers and rights: `src/store/mod.rs::call` (`module.hello`, `module.next`, `module.outcome`, `module.observe`) and `src/store/runtime.rs::hello/next/outcome/observe`. The generic validator is `src/store/runtime.rs::validate_module_receipt_for_operation` from the transaction-bus recovery package; each RuntimeOutcome is validated separately.
- Native consumer and result parser: `src/process.rs::OwnedNativeSpawner::spawn_owned`, `NativeMembershipGuard::verify`, `src/stream.rs::NativeLineReader::next_line/StreamState::consume_line/apply_result`, and `src/controller.rs::settle_terminal`.
- Exact route compatibility and recovery artifact identity: `root-integration/warm-stream-compat.patch`.

The patch set is source-reviewed only. No Cargo, formatter, build, tests, native/provider/API call, database operation, or Git mutation was run. Runtime behavior, Windows/Linux builds, trusted descriptor installation, manager IPC, live Antigravity availability, served-model identity, and whole-family supervisor departure remain unverified.
