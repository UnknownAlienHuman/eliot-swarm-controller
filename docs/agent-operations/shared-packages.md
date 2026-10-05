# Shared package boundaries

The first PR25 extraction uses one workspace and lockfile. The root remains the
default CLI/kernel entrypoint; it consumes the implementations below rather than
having the new packages import the old controller library.

| Package | Implementation | Local dependencies |
|---|---|---|
| `swarm-contracts` | Credential, JSON-RPC Request, runtime command/outcome, error classification and module catalog metadata | None |
| `swarm-client` | Authenticated bounded sequential local IPC client and endpoint computation | `swarm-contracts` |
| `swarm-process` | OS process/group ownership, identity/departure checks, private-file primitives | `swarm-contracts` |
| `swarm-store` | SQLite connection opening, schema identity/digest and transaction primitives | None |
| `swarm-telemetry` | Bounded metadata producer, lazy stderr writer and local drop/failure counters | None |

The host still owns authentication policy, the Store writer actor, domain transactions,
server IPC, dispatch and installed runtime selection. Compatibility paths in
`config`, `model`, `runtime`, `ipc` and `platform` preserve existing host callers;
they map the shared error to the host error once at the boundary. Shared
contracts have no SQLite, native vendor DTO or process dependency.

The module catalog validates data and selects an exact artifact with compatible
protocol/capabilities. It reads no files and starts no process. Launch metadata
keeps protected references for later trusted resolution; unknown prior launch
evidence permits readback only. Metadata IDs are opaque, not filesystem path
components: supervisors must encode or hash them, including Windows drive-like
names. Exact `.` and `..` identifiers/versions are rejected. These contracts do
not change the current Store handshake or prove an installed binary's identity.

The client retains the existing named-pipe/Unix-socket discovery convention,
hello handshake and bounded request/reply exchange. Establishment retry happens
before an application request; uncertain application traffic is never replayed.
A malformed/mismatched/lost response poisons its link. Its two existing poison
checks now belong to the client package; the host keeps its handshake and Store
connection-service checks.

The process package preserves serialized owner receipts and check/script
cancellation. Module-purpose groups reject explicit cancellation and remain
nonkilling on owner loss. `spawned_identity`/`spawned_departed` are existing
check-worker primitives; they do not prove departure of an arbitrary module
child. Exact process identity is not native-session identity. Launch/recovery
policy remains with the caller.

`module_child_belongs_to_owner` checks a distinct native child's exact image and
birth receipt against an existing live module owner and its nonkilling Job or
process group. Unknown OS reads return errors. It neither adopts nor starts a
process and does not prove whole-family departure; the existing departure scan
remains required before replacement. A worker cannot use this child API to
validate itself as the owner.

The Store package opens its writer from the existing owner thread while that
thread retains the DataRoot lock. Base schema initialization and the kernel's
credential checks, extensions and restart reconciliation share one Immediate
transaction. The kernel callback's errors pass through unchanged. The status
reader remains read-only/query-only and retains host principal checks. Parsed
JSON digest comparison and both existing empty-database identity cases are
preserved. No migration SQL, authorization or domain handler moved into shared
contracts, and no second database or writer was introduced.

Store disconnect failures use only known client/link identifiers and fixed
metadata categories, without raw error messages, paths, credentials or native
frames. Queued records and bytes are bounded, writer failure counts losses, and
the Store returns its original error. Queue acceptance is a diagnostic result,
never acknowledgement of business work. This initial stderr producer does not
provide M5 logging reload, retention, live monitoring or metrics RPCs.
Its local sequence identifies emission attempts, including losses; concurrent
writers can queue them in another order. It is not a durable journal cursor or
proof of complete event history.

Format and check explicit packages; do not blanket-format the Atlas donor.
For a changed package, the intended developer commands are:

```sh
cargo fmt -p swarm-client -- --check
cargo clippy --locked -p swarm-client --lib --bins --no-deps -- -D warnings
cargo build --locked -p swarm-client --profile iterate
```

The `iterate` profile keeps line-table debug information and incremental
compilation; release packaging remains separate. Its presence is not a speed
benchmark. No `cargo clean` or repeated full workspace/release gate is required
for a writer fragment. Shared changes also require checking actual reverse
dependencies. See [the modularity program](modularity.md) for M2–M6 acceptance.

Current verification: workspace/lock metadata and explicit package formatting
pass; source review is separate from compilation and native interoperability.
These five packages do not complete the Store/kernel, bus, supervisor or adapter
split. They create the shared boundary those modules can consume independently.
