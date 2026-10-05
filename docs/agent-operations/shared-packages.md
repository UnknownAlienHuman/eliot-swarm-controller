# Shared package boundaries

The first PR25 extraction uses one workspace and lockfile. The root remains the
default CLI/kernel entrypoint; it consumes the implementations below rather than
having the new packages import the old controller library.

| Package | Implementation | Local dependencies |
|---|---|---|
| `swarm-contracts` | Credential, JSON-RPC Request, runtime command/outcome and receipt identity, error classification, module catalog and typed hello claim | None |
| `swarm-client` | Authenticated bounded sequential local IPC client, endpoint computation and typed `ModuleLink` | `swarm-contracts` |
| `swarm-checks` | Kernel-admitted bounded native check executor and authenticated CheckRun status readback | `swarm-client`, `swarm-contracts`, `swarm-process` |
| `swarm-process` | OS process/group ownership, identity/departure checks, private files and scoped module-owner bootstrap | `swarm-contracts` |
| `swarm-store` | SQLite connection opening, schema identity/digest and transaction primitives | None |
| `swarm-kernel` | Single bounded writer actor, readiness and contiguous typed job batching | None |
| `swarm-telemetry` | Bounded metadata producer, lazy stderr writer and local drop/failure counters | None |

The host still owns authentication policy, domain transactions,
server IPC, dispatch and installed runtime selection. Compatibility paths in
`config`, `model`, `runtime`, `ipc` and `platform` preserve existing host callers;
they map the shared error to the host error once at the boundary. Shared
contracts have no SQLite, native vendor DTO or process dependency.

The root Store uses `swarm-kernel` for its existing single writer thread and
bounded FIFO queue. The DataRoot lock stays on that thread through database
initialization and shutdown. Readiness returns the original initializer error;
only contiguous typed message jobs are batched, with the same limit and pending
non-message job ordering. Root supplies database initialization and domain
callbacks, response channels and close/join handling. No second writer, database
or scheduling policy was added. Kernel domain extraction remains unfinished.

The module catalog validates data and selects an exact artifact with compatible
protocol/capabilities. It reads no files and starts no process. Launch metadata
keeps protected references for later trusted resolution; unknown prior launch
evidence permits readback only. Metadata IDs are opaque, not filesystem path
components: supervisors must encode or hash them, including Windows drive-like
names. Exact `.` and `..` identifiers/versions are rejected. These contracts
do not prove an installed binary's identity.

The Store's trusted descriptor registry uses its existing writer and metadata
table. A host-generated supervisor credential has a positive one-method scope
for registration; it cannot use normal writer or Manager methods. Registration
does not inspect or launch an executable. The trusted installer/supervisor must
first establish the installed artifact's provenance.

Managers select registered versions for their own future bindings. The stable
caller and route identify a selection, with catalog-revision CAS; no current-GM
session is required. Admission captures the selected immutable descriptor in the
binding. `ModuleLink` supplies a typed claim that Store compares with that exact
selector. An unselected legacy binding is explicitly unverified and cannot accept
a self-asserted versioned claim. Negotiated capabilities remain compatibility
metadata rather than effect authority.

Module IDs remain opaque identities. The configured runtime label is checked
by the host's route contract, and selection binds its exact artifact to the
trusted descriptor. The four standalone `.1` route contracts validate their
native options and forward only the workspace already admitted by the host.
A cold hello may omit both native identity fields and receives the retained
Store-owned pair; a supplied pair must match it exactly.

Runtime commands include the canonical original Operation request digest before
dynamic enrichment. For selected bindings, one Store validator checks every
outcome's `ModuleReceiptIdentity` against the retained descriptor and exact
Operation/binding/generation. A reconcile target has its own receipt and digest,
distinct from the reconcile Operation. Registered recovery uses the retained
descriptor's reconcile and target-method capabilities as compatibility gates.
Store checks the exact admitted request before accepting a fresh reconcile-linked
target outcome, after its byte-identical duplicate path. Reconcile cannot report
resolution before the target is terminal and cannot authorize another native
effect. Adapter registration and supervisor activation still need their real
host integrations.

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

The existing CheckRun worker consumes `swarm-checks::execute_with_plan`: it
publishes the same exact worker owner and waits for durable Store Go before
kernel-side source resolution and command launch. Output capture is bounded and
the pipes remain drained. Cancellation targets that check's group only. Process
observation errors retain ownership until whole-family emptiness is known;
the existing Store supervisor validates private diagnostics and projects fixed
metadata through authorized `check.get`. Runtime diagnostics do not change the
immutable check/cache input identity. This is an executor extraction inside the
existing worker, not an independently deployed checks process.

The `swarm-module-owner` binary accepts one absolute private plan and an explicit
resolver map. Each invocation owns its own nonkilling group and OS lock, publishes
the existing v1 owner envelope and an exact distinct adapter image receipt, then
retains ownership through direct-child exit and native-family drain. Its plan
contains scope, artifact/version, boot identity and protected references; the
resolver passes existing credential-file paths, without serializing token bytes
or placing them in argv. Ambient module identity/credential environment is
cleared before the selected scope is supplied. Explicit path metadata checks
reject symlink/reparse components; they do not provide a same-user sandbox or
an atomic filesystem-race guarantee.

A resolver/launch-validation error before child spawn writes a boot/owner/scope
bound `launch-result.json` with `not_started`, only after an empty-family check.
The supervisor must still verify that exact wrapper/family departed before a
replacement. A missing, stale or post-spawn result proves no absence of native
effects and permits no input replay. This shared bootstrap is source integration;
its installation, live OS behavior and supervisor/adapter activation remain
separate work.

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

Current verification: workspace/lock metadata, explicit package formatting and
source review pass. Current root source `042d8b6` passed scoped production
Clippy on Windows and remote Ubuntu in
[run 37318364299](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37318364299).
Full tests, release packaging and native interoperability remain separate.
The writer actor correction `025282a` also passed scoped format and strict
production Clippy on Windows and remote Ubuntu in
[run 37322606976](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37322606976).
The scoped module-owner bootstrap `cd88746` passed formatting and strict
production Clippy on Windows and remote Ubuntu in
[run 37326559091](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37326559091).
This is compiler evidence for the helper source, not installed-process or
native-family qualification. Its scoped gate ran no integration targets.
Full tests, release packaging and native qualification remain pending.
The new handshake/receipt/checks batch has source hashes, formatting and metadata
shape checks; its combined compiler gate remains pending at this publication.
These seven packages do not complete the Store/kernel, bus, supervisor or adapter
split. They create the shared boundary those modules can consume independently.
