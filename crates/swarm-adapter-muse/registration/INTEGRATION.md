# Rust Muse worker integration

`muse-rust-controller.1@0.1.2` has an integrated executable and a disabled
[descriptor](descriptor.template.json). The default route still uses the
[Node bridge](../../../modules/muse/README.md). Refactoring selects one executor
after comparing required consumers and total Rust/driver/glue code, then retires
the replaced path under the
[central transition plan](../../../docs/agent-operations/implementation.md#current-format-and-existing-data).

## Integrated source

- `module_runtime::run_owned` uses the shared authenticated Host link and an
  external custodian for the native child. Connection restart does not resend
  native input; the custodian survives consumer cancellation until shutdown.
- `driver`, `commands` and `journal` retain the admitted command and native ACK
  needed for explicit reconciliation. Provider credentials, settings and the
  native model loop stay in Muse. Omitted model/provider/effort selectors use
  native defaults.
- `results` consumes `swarm.normalized_result_context@1` and produces
  `swarm.normalized_result_page@1`. It reads the requested session/item/revision
  and checks its association with the original input before returning bounded
  bytes. A child relationship alone does not attribute a result to the Task.
- `observations` and `streams` expose available native state and gaps. An absent
  child inventory remains unavailable; an empty cursor remains distinct from a
  missing cursor or native end marker. MCP configuration admission does not
  prove that a native tool inventory was observed.

Current launch code uses the existing executable at the configured absolute
path, with `ELIOT_SWARM_MUSE_NATIVE_EXECUTABLE` available during configuration.
The package builder recognizes `swarm-adapter-muse` / `swarm-muse-adapter`;
building does not enable its descriptor. Use the
[installation guide](../../../docs/agent-operations/module-installation.md)
for the current registration path.

## Current transition gaps

Node and Rust use different private journals. Rust currently reads only its own
scope marker and checkpoint; it does not import Node state. In addition,
[`normalized_result::same_module`](../../swarm-kernel-host/src/store/normalized_result.rs)
compares complete artifact identities, including version, between dispatch and
result receipts. Consequently an old Rust dispatch is not automatically readable
through a newer artifact even when its native session still exists.

These source constraints are refactoring gaps. They do not require permanent
readers or executable copies for every old build. Keep original dispatch
provenance separate from the supported result schema and the current authorized
reader. A dependency or build update alone does not create a new protocol;
version the schema when its data or meaning becomes incompatible.

For concrete saved work, finish/reconcile it with its existing owner, or perform
an explicit data transition/export before retiring that executor. Archive
completed results that must survive. A different journal name is not evidence
of a user-data obligation; do not create a compatibility framework in advance.
Do not rewrite old receipts to impersonate a newer build or repeat an uncertain
native input to manufacture a result.

## Verification evidence

The [2026-10-17 native receipt](../../../docs/remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json)
records a successful 0.1.2 dispatch and durable revision-1 result on the existing
Core `7edacae6` qualifier: public artifact reads returned 1,023 bytes. A separate
revision-2 lookup remained unavailable because live and reconstructed native
revisions differed; no input was replayed. This verifies that result path on the
recorded Core, not the complete current product. The receipt still lists broader
method coverage, family recovery and M6/Forge publication as outstanding.

Existing synthetic transport, journal, result and runtime fixtures remain
available for relevant changes. Follow [scoped checks](../../../docs/ci-scoped-builds.md)
and record current results in [implementation status](../../../docs/implementation-status.md).
Do not rerun a native/model campaign for documentation or dependency bookkeeping.
