# Shared package boundaries

This is the compact target ownership map, not a claim that the current Cargo graph
already matches it. [Implementation](implementation.md) owns connected migrations and
deletions. Add or enlarge a library only when current callers use the same behavior
and the replaced implementations disappear in the same slice.

## 1. Retained shared packages

| Package | Owns | Does not own |
| --- | --- | --- |
| `swarm-contracts` | Current semantic command/outcome/result/observation DTOs, public method/catalog metadata and bounded field types. | Vendor RPC/state, artifact-version profiles, Task database, process custody or universal proof packets. |
| `swarm-client` | Authenticated bounded local IPC, request multiplexing, endpoint discovery and `ModuleLink`; no blind replay after an uncertain response. | Task state, native session/recovery, scheduler or delivery database. |
| `swarm-process` | OS process groups/Jobs, PID + birth/image identity, bounded pipe capture, private bounded atomic files and module-owner bootstrap. | Provider session semantics, model retry policy or package compatibility matrix. |
| `swarm-store` | SQLite/WAL connection and transaction mechanics, application/schema identity primitives. | Domain policy, domain migrations, authorization or generic repository/EAV layer. |
| `swarm-kernel` | Pure Task/dispatch/review/acceptance rules and bounded writer/read primitives that genuinely have several callers. | Host IPC, provider types, SQL domain ownership or workflow engines. |
| `swarm-checks` | One selected bounded Check execution primitive and result shape where standalone Check worker remains. | Duplicate host executor, Task acceptance or arbitrary shell service. |
| `swarm-scripts` | One Script manifest/work/result/typed-action wire shared by host and selected worker. | Second interpreter, permissions registry or workflow language. |
| `swarm-telemetry` | Optional structured diagnostics, filtering, bounded sinks/redaction and loss counters. | Required Governor state, business receipts or durable event authority. |

`swarm-kernel-host` owns authentication, domain authorization/transactions, Store
routing and the single control loop. It must not link adapter or effect-worker
implementations after their selected contracts move. Adapters do not depend on one
another or import host internals. Frontends call the same application methods through
`swarm-client`.

Current `swarm-bus`, `swarm-automation`, `swarm-observer`, `swarm-supervisor` and
standalone workers are evaluated responsibility by responsibility. Package existence
is not proof that a separate process or permanent package remains necessary.

## 2. Contract boundaries

| Boundary | Retained rule | Target owner |
| --- | --- | --- |
| Public application methods | One current name/schema catalog; profile exposure narrows, exact domain authorization decides. | `swarm-contracts` + owning Store domain |
| Adapter commands | Closed semantic actions and supported scopes; vendor methods remain adapter-local. | `swarm-contracts` + selected adapter |
| Hello/bootstrap | Authenticated binding/generation/boot, protocol major and required capability subset. | `swarm-process`, `swarm-client`, Store handshake |
| Package/catalog | Installed executable/resources/current descriptor for inventory and route selection. Reading catalog launches nothing and grants nothing. | Generic installer + supervisor catalog |
| Store delivery | Exact already-formed outcome/result records may be retained until ACK; observations use replaceable source checkpoints. | Route-local adapter first; shared only after two users delete duplicates |
| Native effects | Strongest provider identity, one native state owner and exact readback. | Selected adapter only |
| Process custody | PID + OS birth/image/group/Job and conservative adoption/termination. | `swarm-process` / supervisor |
| Database | One writer, short transactional domain mutation, bounded WAL reads, numeric application migrations. | Host Store + `swarm-store` mechanics |
| Diagnostics | Redaction/bounds/loss counters; no business ACK semantics. | `swarm-telemetry` / optional observer |

Do not make artifact version, build ID, compiler, checkout, schema inventory, document
hash or TaskPrompt digest a behavioral API or permission. Verify selected package
bytes at install/launch and immutable result/artifact bytes at storage boundaries.

Current full descriptor/schema echo, `ModuleReceiptIdentity`, Task dispatch hash echo
and `swarm.task_prompt@1` are transitional consumers. Migrate them route by route to
the semantic protocol/TaskPacket and remove their old caller/schema/profile in the
same slice.

## 3. Process and worker boundaries

A separate process is justified by native SDK isolation, replaceable adapter, external
I/O/custody or fault containment—not by a small pure helper.

Target examples:

- one selected adapter process per required native ownership scope;
- selected Check/Script/Forge/GitHub effect worker only where real external work or
  process custody benefits from isolation;
- one host time-admission loop for Store bookkeeping;
- optional diagnostic recorder independent of required local collectors;
- one single-tenant gateway per Eliot identity/profile.

Remove parallel lifecycle owners and hidden host executors as current callers switch.
A wrapper around the old implementation is not extraction.

Conditional donors do not become dependencies in this map. For example,
`cccc-windows-process` remains a possible child-first Windows launch primitive only if
a proven path needs it and it deletes current code. The existing owner-first Job
boundary stays until a connected replacement is demonstrated.

## 4. Data and authority placement

- Store commits domain state and its durable receipt/observation together.
- Bus/notification distributes facts and wake hints; it owns no database.
- Queue, lane, provider state and capacity are read projections/current facts, not
  shared packages with their own lifecycle.
- Workspace lease is controller write admission, not an OS sandbox.
- Mailbox is normalized recipient-indexed data; optional thread groups messages but
  grants nothing.
- Provider/process/worktree/remote collectors feed one Governor projection; telemetry
  is not that projection.
- Effect workers receive a closed admitted plan and return bounded evidence. They do
  not receive Manager/operator credentials or decide Task acceptance.

## 5. Dependency and build rules

1. Host/kernel/frontends do not link adapter implementations.
2. Adapters do not link other adapters or host.
3. Shared packages use the smallest dependency/features needed by actual callers.
4. One workspace lockfile records dependencies; default members remain normal small
   entrypoints, not every adapter/test fixture.
5. Test-only fake runtimes stay in tests/dev support, not production binaries.
6. Adapter-only changes should not rebuild/restart unrelated adapters or host unless a
   genuinely shared contract changed.
7. Build provenance is diagnostic; runtime compatibility is current protocol and
   connected behavior.

## 6. Completion test for extraction

An extraction is complete only when:

- at least two current callers use the shared behavior, or one well-defined public
  package is the natural existing owner;
- previous copies, wrappers, config, fixtures and dependency edges are removed;
- retry/lifecycle/durable-state ownership remains singular;
- automatic positive behavior remains connected;
- affected format/Clippy/build and one connected behavior case pass;
- net production Rust/JS/glue is measured separately from tests.

Moving files, creating a crate or changing language without deleting a responsibility
is not simplification.
