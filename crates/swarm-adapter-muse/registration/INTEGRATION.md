# Rust Muse worker integration and qualification

The new `muse-rust-controller.1@0.1.1` descriptor remains disabled pending final
platform and controlled installed-native qualification. The executable's normal
`run_owned` entry now uses the integrated shared-link loop and external native
custodian. Explicit operator registration/route activation remains separate from
source integration and candidate packaging.

Version 0.1.1 accepts an empty native lifecycle `viewCursor` as the pinned MSP
schema's required string, preserving its exact value across journal reopen.
Missing or non-string cursors and foreign session IDs remain invalid. On
2026-10-17 the installed Muse 1.4.0 returned an empty cursor for an exact
read-only session lookup after the 0.1.0 candidate left its open outcome unknown.
This repair does not settle that earlier operation, replay its start or qualify
model output, recovery, process-family completion or M6. Changed executable
bytes require a new installed artifact coordinate; existing 0.1.0 receipts and
bindings remain historical.

The current installed Node bridge.13 and its private state are separate. This
artifact reads only its own Rust scope marker and versioned journal. It neither
adopts nor rewrites a Node checkpoint.

## Integrated source

- `swarm-muse-adapter`, `driver`, `module_runtime`, `streams`, the pinned generated
  schema validator and the reviewed external native custodian are wired. The
  custodian retains the real child before spawn, outside the consumer task.
- Cloned `Client` and `ModuleLink` handles share one authenticated physical
  connection, one hello barrier, bounded pending calls and exact reply IDs.
  `run_owned_with` retains the outstanding `module.next` future while delivery
  and optional stream publication proceed. A new hello is only a reconnect;
  it never resends native input.
- `OwnedModuleRun` must stay outside abortable consumers and be joined before
  Tokio teardown. Its control handle may stop or abort the consumer; cancellation
  of `join(&mut self)` retains the owner for a subsequent join. Shutdown closes
  native stdin and joins notification/stdio/custodian work even after consumer
  panic. Direct exit or EOF never proves process-family departure.
- The journal retains complete validated native ACKs, original RuntimeCommand,
  receipt, TaskPrompt, selectors, work fences and native UUIDv7 independently of
  Host/RPC IDs. Explicit reconciliation reconstructs the original context and
  revalidates current native facts. Missing context remains unavailable.
- `Journal::native_invocation` requires the actual current initialize grant for
  session MCP configuration. The closed retention profile rejects inline MCP
  headers/environment and credential-bearing URLs before checkpoint publication.
  Model objects and admitted native payloads remain exact. Provider authentication,
  HOME/XDG, native provider settings and conversation history stay in the harness.
- Shared projections, paired opaque discovery boundaries and stable pending
  inventories use the same journal. Restarted projections are stale historical
  facts; fresh native reads are required for current child/reply/result authority.
  Result reads check the opaque authority before and after native IO and again
  before retaining a page.
- Native notifications feed the bounded optional publisher. Task stream identity
  comes from the original Task admission ACK and exact native item/turn facts.
  Provisional pre-ACK item metadata grants no authority; prefix loss remains
  visible after ACK. Equivalent missing deltas do not create per-token durable
  Observations. Sequence reservation precedes queue admission; lost publication
  stays a gap and cannot stop native/control work.

The native executable must already exist at the descriptor's pinned absolute
path, or an installer must pin another existing absolute path with
`ELIOT_SWARM_MUSE_NATIVE_EXECUTABLE`. No PATH lookup, installation, login,
provider credential setup or global configuration change is performed. Omitted
native model/provider/effort selectors remain omitted.

The existing `build-module-package.ps1` policy recognizes the exact
`swarm-adapter-muse` package and `swarm-muse-adapter` target. This permits a
source-pinned candidate build after the source gates pass. The builder still
generates no descriptor, installs or registers nothing, and enables no route;
use the disabled template and explicit native qualification gates below.

## Root verification

Root runs Cargo and formatting sequentially. The coherent local library pass on
2026-10-17 completed 131 tests with three explicit subprocess-entry fixtures
ignored. It includes transport, journal, codec, schema, Driver, runtime lanes and
the native-custodian external watchdog. Separate common-client transport and
authenticated actual Host concurrency cases passed. Strict scoped all-target
Clippy passed; later semantic changes require their relevant verifier.

```text
cargo test --locked -p swarm-adapter-muse --lib
cargo clippy --locked -p swarm-adapter-muse -p swarm-client --all-targets -- -D warnings
cargo fmt -p swarm-adapter-muse -p swarm-client -- --check
```

Scoped CI now runs the selected Muse library on Windows and Linux. These
synthetic tests do not establish an installed native account run, native family
recovery, all ten methods against the real executable, or the two-adapter M6
productive delivery chain. Native MCP inventory remains unavailable in the
inspected public SDK; a requested/granted config or external tools listing does
not prove native ingestion. Retain that closed capability diagnostic and every
uncertain historical effect. Registration/activation and native qualification
remain outstanding.
