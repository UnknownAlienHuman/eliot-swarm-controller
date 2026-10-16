# Updating modules/command

The active artifact is `command-mod-0.1.0-glue.6`; the new-binding path requires the exact trusted descriptor in `module-descriptor-glue.6.template.json`. Keep the `.5` descriptor and its schema-3 records as exact, read-only `.5` history. Keep `.4` admission/run records schema-2-readable; the `.2` and `.3` records remain immutable history.

## Contract invariants

1. The supervisor supplies the descriptor-derived `ELIOT_SWARM_MODULE_CONTRACT` at launch. The bridge enforces the 64 KiB limit, exact `.6` module/artifact/protocol/capability/schema claim, and managed-owner context. It sends that claim under the existing `module.hello.module_contract` field and verifies the Store negotiation before reading commands. It never registers, selects, or synthesizes a claim.
2. A selected `task.dispatch` descriptor carries `swarm.runtime_command@1`, `swarm.task_dispatch_context@1`, and `swarm.task_prompt@1`; and `swarm.runtime_outcome@1` plus `swarm.task_dispatch_admission@1`. The bridge's hello claim must exactly match that supervisor-owned descriptor.
3. Only the Store-provided `task_prompt.prompt` reaches the native CLI. Validate closed envelope/context shapes, task and Attempt identity, task revision and snapshot digest, source-text digest/bytes, exact prompt SHA-256/UTF-8 bytes, worker/binding/Operation identity, and `command_core_binding` before admission or spawn. Never accept snapshot/canonical-snapshot prompt fields or reconstruct prompt text locally.
4. Persist schema-3 `.6` admission before spawn. Retain the contract revision, prompt digest/bytes, task/revision/Attempt/snapshot identity, and exact dispatch context in admission and run evidence; echo them in the Operation outcome. Keep the native prompt itself out of admission metadata.
5. Keep `task.dispatch` one-shot and keyed by Operation ID. A saved admission without valid terminal proof is `Unknown`; reconciliation is readback-only. Validate saved `.5` schema-3 evidence against its original `.5` receipt identity for readback only; never admit it for `.6` execution. Keep `.4` schema-2 and `.2`/`.3` history under their original rules.
6. Preserve raw NDJSON lines and frame format with semantic projections. Validate the saved event sequence, summary, native request-model evidence, terminal result, and process exit. Do not infer effective model/provider identity from route/model aliases or process exit.
7. Keep the Node bridge launch boundary and native CLI boundary fixed. The supervisor launches the exact bridge with the private config; the bridge starts the configured absolute Node/native entrypoint without a shell wrapper and strips `ELIOT_*`, `SWARM_*`, and capture-named variables from the native child.

## Artifact pin and verification

Refresh `vendor.lock` with the `.6` artifact id and canonical LF source digests for the changed units before packaging or activation. No Cargo, test, or model execution was performed for this slice. Native `.6` qualification remains a separate gate.
