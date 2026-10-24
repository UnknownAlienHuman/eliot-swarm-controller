# Standalone Antigravity Rust adapter `.1`

Status: integrated production source in the controller workspace. The current
descriptor update still requires its compiler gate and native Antigravity
qualification; source integration does not establish installed or live behavior.

The binary is `swarm-antigravity`; the current source descriptor is `eliot-antigravity.rust-headless.1`, version `6`. Versions 1 through 5 remain retained identities. Version 6 permits omitted model selection. A1b removed the mandatory dispatch `agent.refresh`, `AntigravityDispatchSessionProof`, latest-refresh lookup and assignment/lease/session digest packet. Store now revalidates the current Task/Attempt, binding generation, route, current boot/root, successful Open model evidence and workspace authority directly; the adapter still rechecks command binding/root before native I/O. `agent.refresh` remains an optional current-process readback action, not a dispatch prerequisite. The normalized result contract introduced by version 4 returns bounded captured assistant output when this process retains the exact Operation-to-turn mapping. Source support does not establish installed-native qualification. The adapter has no Store, database, provider-auth or MCP-server ownership. A2a adds bounded private exact Store-delivery and selected-body retention under the
module state directory. It stores no prompt intent. A prior-boot outcome remains held
until exact Store reconciliation proves the owner transition; normal IPC reconnect
retries identical bytes. Store-authoritative aggregate page coverage and local source
release remain A2b. Installed/native qualification remains pending.

## Identity, admission, and rights

The adapter requires the guarded module-owner environment and verifies that its own process is a distinct live member of the exact non-killing owner group before connecting. The owner supplies `ELIOT_SWARM_MODULE_CONTRACT`; the adapter parses that bounded descriptor-derived claim and checks it against the exact `.1` module, artifact, protocol, capabilities, and runtime schemas. It sends the claim through the existing authenticated `module.hello`. Store negotiation remains the authority; descriptor capabilities do not grant method rights. Only the current authenticated module credential and binding scope admit `module.next`, `module.outcome`, and `module.observe`.

On a first hello the adapter may have no local native root, so it sends both native identity fields as null. The authenticated hello response carries the Store-owned `binding_id`, generation, route, retained `native_root_id` and `native_scope_key`, `recovery_required`, and negotiated descriptor. The adapter accepts the returned root only with the exact local Antigravity scope. If it already has a root, reconnect must return that exact pair. The integrated Store `module.hello` path permits both request fields absent/null while returning the retained Store pair; it rejects partial or mismatched claims and prevents root adoption by a rootless `.1` binding.

Every retained outcome contains `details.module_receipt`, a shared `ModuleReceiptIdentity` built from the exact Store-supplied `input_sha256`. `agent.reconcile` queues a separate `Unknown` outcome for the exact target Operation using `target_input_sha256`; its `details.reconcile_operation_id` identifies the current admitted reconcile Operation. The separate `Unknown` outcome for the reconcile Operation uses its own `input_sha256` and does not carry that relation field. The shared Store helper validates both outcomes independently against the exact binding, retained descriptor, Operation ID, and canonical original request, then validates the target-to-reconcile relation. No nested target receipt is used. Already-accepted terminal target outcomes are replayed byte-identically without retrofitting the relation; the Store's exact duplicate path preserves the terminal fact. The `.2` Node artifact keeps its pre-versioned receipt path because it has no descriptor selector.

## Native behavior and restart semantics

No controller-side model default exists. The route may omit `native_options.modelId`; the launch then passes no `--model` flag and the installed harness default applies, mirroring the Node bridge's conditional `--model` (`bridge.mjs`, `bridge-v4.mjs`). An explicit `modelId` override is passed exactly and the native `init` readback must equal it, otherwise open fails with a mismatch; without an explicit selection the effective model is reported as unknown unless the native stream observes it. Only `agent.open` launches the configured absolute `agy` executable. Sequential `task.dispatch` and `agent.send` write one native user message only after `module.next` durably admits the Operation. An uncertain host reply to `module.next` or native write is never retried as a new Operation or prompt. Native `init` and terminal-result events are consumed from the current child stream; the reducer reports terminal facts only when the parsed result has a fingerprint and belongs to the exact pending Operation and current stream generation.

The public headless CLI contract exposes stream-json events and prompt writes but no documented exact conversation-history GET or recovery method. Consequently a new process can show the root retained by its own authenticated hello, but cannot claim that the prior native transcript or an uncertain prompt was read back. `agent.refresh` returns a current-process stream snapshot only when this boot observed the exact root; otherwise it reports `Unknown` with `NATIVE_HISTORY_UNAVAILABLE`. `agent.reconcile` uses the exact target receipt path above and does not start a model, resume a conversation, or replay a prompt. No recovery completion is claimed. Version 4 introduced two bounded `agent.result` paths. An `antigravity_status` selector publishes the Store-retained target outcome and diagnostic for the exact binding/input. Its status page contains no prompt/response text, does not invent a native message/turn identity, and retains its conservative completion projection.

The normalized path returns the captured assistant body through `swarm.normalized_result_page@1`. `Controller::prepare_prompt` records one pending Operation, conversation, pre-prompt ordinal and stream generation before the write. `settle_terminal` accepts the first matching terminal only with continuous classified stream coverage and the same generation, then retains the Operation → receipt/conversation/ordinal/boot/generation mapping separately from the ACK outbox. `build_normalized` verifies the Store-sealed origin and exact receipt, looks up that Operation, and validates the returned body digest. A missing per-message parent pointer alone does not reject this single-flight association.

Unknown conversations, ordinal gaps, multiple matching results and stale generation remain ambiguous rather than selecting a latest response. Missing retained bytes produce `RESULT_BODY_UNAVAILABLE`; this boot cannot reconstruct earlier-process history. Neither result path accepts a Task or replays a native prompt. Current source support does not qualify installed/live productive readback.

Unsupported permission/configuration replies, goals, background work, and `agent.recover` return bounded rejection receipts. The adapter creates no Task, changes no owner decisions, starts no worker except the manager-admitted `agent.open`, and adds no method privilege beyond its negotiated result contract.

## Trusted registration and route

`src/contract.rs::template` declares the descriptor contract: module `antigravity`; artifact `eliot-antigravity.rust-headless.1` version `6`; protocol `1.0`; capabilities `agent.open`, `agent.reconcile`, `agent.refresh`, `agent.result`, `agent.send/next_turn`, `task.dispatch`; command schemas `swarm.normalized_result_context@1`, `swarm.runtime_command@1`, and `swarm.task_dispatch_context@1`; event schemas `swarm.normalized_result_page@1`, `swarm.runtime_outcome@1`, and `swarm.task_dispatch_admission@1`; and workspace option `/workspaceRoot` with `replace_with_admitted_absolute_workspace` semantics. The local `claim()` reads the exact manager-provided contract and rejects any capability/schema drift. A trusted installer or supervisor must register the descriptor and select version `6` for new bindings; the adapter claim cannot register or select itself. The optional `build_id` is taken from the trusted launcher claim and participates in Store negotiation and receipts.

The existing `.2` artifact, route and binary retain their own identity. The integrated Store runtime and recovery paths recognize `.1` through its selected descriptor and shared receipt validation; legacy `.2` retains its unversioned behavior. The earlier `root-integration` patch handoff is historical and is not a current build or merge prerequisite.

Example local configuration, supplied outside this package:

```json
{
  "hostDataDir": "C:\\SwarmState",
  "credentialFile": "C:\\SwarmState\\antigravity-module.credential.json",
  "nativeExecutable": "C:\\Tools\\Antigravity\\agy.exe",
  "moduleArtifactId": "eliot-antigravity.rust-headless.1"
}
```

The route must use `runtime = "antigravity"`, the exact artifact ID, and an absolute `native_options.workspaceRoot`. `native_options.modelId` is an optional exact override: when absent no `--model` flag is passed and the installed harness default applies; when present it is validated as an exact bounded string and verified against the native `init` readback at `agent.open`. Invoke only through the existing guarded module runner; a direct local launch lacks the manager-supplied contract and owner membership.

## Source anchors and validation boundary

- Producer and effect handler: `src/main.rs::run`, `wait_for_open`, `wait_for_terminal`, and `agent.result`; bounded status/normalized pages: `src/result_page.rs::build`, `build_normalized`, and `build_normalized_page`; exact captured-body lookup: `src/controller.rs::retained_response_for_operation`.
- Descriptor claim and registration contract: `src/contract.rs::template/claim`; authenticated handshake and negotiated-response reader: `src/ipc.rs::ManagerLink::hello/validate_negotiation`.
- Store-supplied identity reader: `src/main.rs::HostSession::accept_hello`, `controller_from_hello`, and `src/controller.rs::Controller::new`.
- Per-operation and target receipt producer: `src/module_receipt.rs::for_command/for_reconcile_target/serialize_outcome`, `src/controller.rs::record_outcome/read_command` (including target `details.reconcile_operation_id`), and `src/wire.rs::OperationIdentity::try_from`.
- Existing host handlers and rights: `src/store/mod.rs::call` (`module.hello`, `module.next`, `module.outcome`, `module.observe`, `module.result`) and `src/store/runtime.rs::hello/next/outcome/observe`. Store status provenance is `src/store/results.rs::antigravity_status_snapshot/validate_antigravity_status_source`; the generic validator is `src/store/runtime.rs::validate_module_receipt_for_operation` from the transaction-bus recovery package; current and target receipts are checked independently.
- Native consumer and result parser: `src/process.rs::OwnedNativeSpawner::spawn_owned`, `NativeMembershipGuard::verify`, `src/stream.rs::NativeLineReader::next_line/StreamState::consume_line/apply_result`, and `src/controller.rs::settle_terminal`.
- Exact route compatibility and recovery artifact identity: integrated `crates/swarm-kernel-host/src/store/runtime.rs` and the selected module descriptor; the old patch bundle is not an active source dependency.
- Optional model override boundary: `src/launch.rs::selected_model_id/build_launch_spec` (absent `modelId` omits `--model` for the harness default; present value validated exactly), `src/controller.rs::begin_open/complete_open` (explicit selection requires exact `init` readback match, `verified`; no selection records `native_default` with unknown effective unless observed), and `src/main.rs::HostSession::accept_hello` (absent override accepted, malformed override rejected).

This guide was reconciled with A1b source `3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb` on 2026-10-24. Run `40131522836`, job `120984044559`, passed affected rustfmt and warnings-denied Clippy; no native/provider call, database migration, integration test or installed/live-model qualification was run. Existing evidence remains bound to its recorded source/artifact; installed Antigravity behavior, served-model identity, durable result delivery and whole-family departure need their own evidence.
