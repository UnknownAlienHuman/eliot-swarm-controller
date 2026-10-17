# Prospective Rust Muse worker — integration gates

This disabled new artifact does not alter bridge.13 or its checkpoint format.
The ten handlers are implemented behind an activation gate; this is not a full-parity or native qualification
claim. Do not register/enable it until these dependencies and verifiers pass.

Root owns `lib.rs`, Cargo, lockfile and integration of parallel modules:

- Export `driver` and `module_runtime`; wire `streams` and the reviewed
  `native_owner` **c177a1c18c0225a3a41f6ae23cb587deffe43677** or its qualified
  descendant. `module_runtime` calls its exact `spawn_owned(&mut custodian,
  spec, router)` API. Never substitute 80a's blocking Drop.
- Add binary `swarm-muse-adapter` (`src/main.rs`), `swarm-client` path dependency,
  `jsonrpsee-types = "=0.26.1"`, UUID v7 minting (`uuid`, features `v7`), and
  Tokio features `process`, `net`, `signal`, `rt-multi-thread` in addition to the
  existing IO/sync/time features. Root owns version/lock resolution.
- For connected fixtures only, add `#[cfg(test)] pub(crate)` wrappers:
  `MspTransport::attach_fixture<R,W>(input, output, RpcModule<()>, Options)`
  delegating to its private `attach` with the same bounds/return type; and
  `Journal::create_fixture(&Path, Scope, String)` delegating to `create_at`.
  Production continues through `attach_owned_stdio` and `create_owned`.

The descriptor uses `ModuleHostConfigPath`, protected membership/credential
delivery and the immutable route directly. `config_schema` is null: the current
supervisor has no generic Muse launch mapper. The separate route schema is
documentation for the worker's closed serde input, not a claim that Core
implements such a mapper. The default executable is only the already installed
`C:\Tools\MuseCode\muse-bin-1.4.0-R4161.1.exe`. An installer may pin a different
absolute existing executable with descriptor literal environment
`ELIOT_SWARM_MUSE_NATIVE_EXECUTABLE`; no PATH search, installation, auth, HOME or
provider settings are performed. Omitted native selectors remain omitted.

**Production entry is explicitly gated with `MUSE_HOST_MULTIPLEX_API_REQUIRED`.**
The Store replaces the active `module_link_id` on each hello
(`store/runtime.rs::hello_internal`); `scope(check_link=true)` rejects another
connection. The shared `swarm-client::Client::exchange` and `ModuleLink` require
`&mut self`, and `module.next` can remain pending for ten seconds. Opening a
publisher/reporter hello on another connection would fence the consumer. Root
must provide correlated concurrent calls on the SAME authenticated physical
link, retaining the outstanding next future through selects. Its publisher
must also accept that shared link instead of serializing behind pending next.
No new credentials, link authority or parallel hello workaround is introduced.
`run_owned_with` retains a sequential qualification seam; it does not close this
production dependency. The normal executable cannot start a native child yet.

## Existing component extensions still required

These remain explicit closed diagnostics in `driver.rs`, not parallel storage:

1. `Journal::record_native_ack` rejects public MSP 1.3 required ACK fields:
   `TurnStartResult.disposition/startedNewTurn` and
   `SessionSetApprovalModeResult.applyOutcome`. Extend the bounded allowlist to
   retain full validated ACKs. Do not remove these fields to make tests pass.
2. `NativeInvocation::new` rejects the public `session/setModel` `model` object,
   `session/start.config.mcpServers`, and replies targeting observed children.
   The driver reports `MUSE_JOURNAL_{MODEL,MCP,CHILD_REPLY}_EXTENSION_REQUIRED`
   before those native writes; never flatten, strip or retarget the payload.
   `queue_result` also rejects native child-session result pages even after
   exact family authorization; its source fence must be extended deliberately.
3. The journal needs a typed retained command/TaskPrompt/selector/work-fence
   context, lookup and restoration contract. Until then cross-worker mutation
   and ambiguous root-creation reconciliation fail closed with
   `MUSE_DURABLE_{CONTEXT_EXTENSION,RECONCILIATION_CONTEXT}_REQUIRED`.
   Connection-local same-command reconciliation uses the original receipt,
   native UUIDv7, method and exact payload, fresh fences and existing ledger.
4. Projection checkpoints, child-discovery opaque continuation and pending
   inventory persistence need the same journal extension. Current observations
   remain partial; process loss never fabricates family completion. No old Node
   marker/checkpoint is read, adopted or rewritten.
5. MSP exposes its serialized writer close barrier and notification consumer,
   not a separate physical stdout-EOF receipt. Custodian join retains the real
   child and stderr drain with Tokio alive. Direct exit/EOF is never family-gone
   or native NoEffect evidence.

Stream source mapping comes only from the exact Task admission ACK's `turnId`
and full native item identity, never ordering/current idle/delta text. Production
notifications enter a bounded channel; `Driver::publish_next_stream` delegates
to current `streams::publish_notification` but requires Root's shared-link
integration above. It is not currently called by the gated production loop.
Logical sequence is reserved before queue admission,
so dropped queue/link/ACK records cannot be replayed as new continuous content.
Pre-ACK/unknown-item events remain source gaps. Stream publication errors are
local loss and never escape into native work or the durable outbox. Root's
current Host Task-revision/evicted-revision/initial-gap fixes remain separate
evidence; their tests do not qualify this absent caller integration.

## Root-owned verifiers (not run by this author)

After component/test-seam wiring and ACK allowlist repair:

```text
cargo test -p swarm-adapter-muse muse_driver_ -- --nocapture
cargo clippy -p swarm-adapter-muse --all-targets -- -D warnings
```

The connected driver fixture uses only a synthetic MSP pipe and synthetic Host
IPC; it does not qualify a real account, native process family, crash recovery,
all ten native methods, or productive Task/result/acceptance parity. Full
`run_owned`/safe-custodian watchdog and Root's later native qualification remain
required. Preserve this disabled descriptor until the remaining full goal is met.
