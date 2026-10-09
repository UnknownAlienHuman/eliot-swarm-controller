# Modular Runtime — Build, Process and Authority Boundaries

Owner direction: 2026-10-05. **Implementation specification, not an implementation claim.**
Source reviewed: `2aec51bb1e3c8122da7d496142ac8e25a21969a3`.
[Review and defects](modularity-review-2026-10-05.md) records what exists;
[Observability](observability.md) defines logging/live monitoring;
[Donor map](donor-map.md) records inspected mechanisms and rejected assumptions.

## 1. Required outcome

An ordinary registered manager can plan and run its work without borrowing the
operator credential or obtaining repeated approvals. An adapter, script bundle,
bus dispatcher or recorder can be built and replaced without rebuilding every
other component. Unused optional modules have no running worker, polling timer,
network connection or loaded native model. One failed optional worker does not
stop the host or unrelated agents. Recovery restarts software, not the original
model input or an uncertain external write.

Preserve the existing durable Operations, Task/Attempt snapshots, mailbox,
independent review/acceptance and exact-candidate publication. Do not replace
these with a new scheduler, broker product, model loop or general workflow DSL.

Three boundaries are different: a Rust source module is not a compilation unit;
a Cargo package is not a fault-isolated process; a restarted adapter process is
not a resumed native execution. Implement all three deliberately.

## 2. Package and process map

Use one Cargo workspace and lockfile. Names below are target package names;
they do not imply that these crates or commands exist at the reviewed revision.
Keep platform-specific unsafe code inside the small platform package.

| Package / responsibility | Existing source to extract | Runtime boundary |
|---|---|---|
| `swarm-contracts` | Common DTOs from `src/model.rs`, `src/runtime/mod.rs`, IPC envelopes | Small data-only library: IDs, commands, events, outcomes, capabilities and protocol versions. No vendor DTOs, DB, process launch or runtime singleton. |
| `swarm-client` | Client side of `src/ipc.rs` and framing | Library shared by CLI, MCP and worker binaries. Connection/authentication/framing only; no Task policy. |
| `swarm-store` | DB writer, migrations, receipts, artifact metadata | Library inside the kernel process. One authoritative database/writer. Transaction API rather than unrestricted DB handles in plugins. |
| `swarm-kernel` | Generic admission, Task/Attempt, review/acceptance and routing state from `src/store/` | `swarm-kernel` process. Owns committed state, not native transports. Organize domain handlers internally; no crate per SQL table. |
| `swarm-bus` | Shared observation routing, subscriptions, consumer/cursor delivery | Independently built dispatcher process. Reads/acknowledges through kernel IPC; never owns a second authoritative event database. |
| `swarm-process` + `swarm-supervisor` | `src/platform/`, generic ownership from `src/launcher/`, `src/host.rs` | Small platform library and supervisor binary. Supervise kernel/selected workers; do not link provider implementations. |
| `swarm-adapter-<runtime>` | Each `src/runtime/<runtime>` plus its owned `modules/<runtime>` glue and vendor-specific Store interpretation | One independently built executable/package per adapter. Multiplex compatible bindings in one service worker where its native contract permits. |
| `swarm-scripts` | `src/scripts/`, effect execution from `src/store/scripts.rs` | On-demand runner. Each versioned script bundle is a separately replaceable module; one-shot invocations are transient, not permanent daemons. |
| `swarm-automation` | Timers/rules/hooks/Goal coordination from existing automation/scheduler/hooks units | Optional worker using the existing typed action handlers. One due queue/shared source readers, not one timer process per rule. |
| `swarm-checks`, `swarm-forge` | Existing check process and Git/GitHub execution boundaries | Separate on-demand execution workers. Admission, acceptance and unknown-effect state stay in the kernel. |
| `swarm-telemetry`, `swarm-observer` | Existing diagnostic emission, reports and live projections | Small producer library; independently replaceable recorder/live-query worker. No mandatory cloud backend. |
| `swarm-cli`, `swarm-mcp`, `swarm-gateway` | `src/main.rs`, `src/mcp*`, `src/gateway*` | Independent frontends using `swarm-client`. CLI keeps the public `swarm` executable. Gateway is optional and off until configured. |

Extraction follows responsibility, not file ownership alone: `store/scripts.rs`
contains both transactional decisions and external execution; move the latter,
not the whole DB authority, to a worker. Do the same for checks, Forge and native
adapters. Do not ship a crate that imports the original monolithic library as a
shortcut: that retains both the compilation dependency and old failure domain.

Dependency direction:

```text
contracts <- client <- CLI / MCP / gateway / bus / optional workers / adapters
contracts <- store <- kernel domain handlers
contracts <- process primitives <- supervisor / owned execution workers
telemetry producer <- components that emit diagnostics
observer <- client + telemetry decoding; not kernel or adapter implementations
```

`swarm-kernel`, `swarm-supervisor` and frontends must have **no Cargo dependency
on any `swarm-adapter-*` package**. Adapter A must not depend on adapter B.
The kernel does not link the bus/recorder executable implementation either.
A shared contract change legitimately rebuilds its Rust consumers; an internal
adapter change must not relink the kernel, CLI or another adapter.

### 2.1 Bus modularity without broken transactions

The durable journal append remains a Store operation in the **same transaction**
as the state change/receipt. The bus module owns delivery, fanout and consumer
scheduling, not that database. Losing the bus cannot turn a committed command
into an unsaved fact. A queued command remains queued, not falsely delivered.

Consumers use the existing atomic admission pattern through a narrow kernel
request: identify consumer + expected cursor + source occurrence + requested
ordinary action(s); persist the resulting Operation(s) or bounded pending state
and advance the cursor together. The authenticated consumer context, not an
arbitrary `run_as` string, determines authority. No arbitrary SQL or callback is
accepted across IPC. Enforce bounded batches and existing semantic dedup slots.

A bus restart resumes retained cursors; volatile notifications are only hints.
Direct status/readback and already-admitted native work remain usable during a
bus outage. A missing recorder never prevents state transitions. A broken
journal/DB does prevent new durable admission; restarting cannot manufacture a
commit. Keep this irreducible storage/kernel fault domain explicit.

## 3. Module protocol and registration

Extend the current bounded IPC/RuntimeCommand boundary rather than introducing
an in-process Rust dynamic-library ABI. Each installed module has local metadata:
module ID, artifact/build identity, executable and separate argv, config schema,
protocol major/minor range, command/event schemas, capabilities and lifecycle
ownership (`external_attach`, `owned_service`, `one_shot`). Secrets are references
to local protected configuration, never manifest values exposed to agents.

This is a registry of locally installed modules, not a remote package catalogue, installer or second activation ledger. The operator registers an installed executable/location once. A manager selects
an admitted route or script by ID; normal dispatch does not require another
installation approval. Catalogue reads use retained descriptors and do not spawn
a process, start WSL, load models or probe every provider. Distinguish installed,
enabled, active, unhealthy and unavailable in the catalogue.

Handshake binds module ID/version, fresh worker boot ID, supported protocol and
exact assigned bindings/generations. A compatible additive schema change does
not require replacing every process. Reject incompatible messages with a bounded
capability error before effect; unknown optional event fields are retained only
under a bounded extension envelope, never interpreted as new authority.

The normalized dispatch pair is an explicit descriptor opt-in: a module advertises
`swarm.task_dispatch_context@1` as a command schema together with
`swarm.task_dispatch_admission@1` as an outcome schema. Store adds the context
to that exact `task.dispatch` command with operation, binding and generation,
worker boot, Attempt and Task identity, Task revision, immutable snapshot digest,
and original source text digest and byte count. The adapter returns the context
unchanged, echoes the strict `ModuleReceiptIdentity`, and records the digest and
byte count of the exact native payload it submitted plus an optional native input
ID. Store recomputes the context from the original Operation and retained Attempt,
requires the outer identity and native input ID to match, and records an existing
Attempt producer for `Accepted` or `Applied`. `Unknown` remains unresolved and
cannot mark the Task complete. The original dispatch context is retained before
the command leaves Store, so recovery validates the original admitted worker
boot rather than a replacement boot. Descriptors without both schemas retain
the legacy codec and runtime-specific producer path.

Current TaskPrompt artifacts also advertise `swarm.task_prompt@1`. Store builds
one exact prompt envelope from the retained Task snapshot and source text; it
contains Task/Attempt/revision identity, the snapshot digest, prompt bytes and
their digest. The adapter validates the envelope against the dispatch context
and submits its prompt unchanged. Legacy snapshot renderers remain only behind
historical artifact contracts, never as a fallback for a current artifact.

The adapter owns native schemas, workspace-field mapping, connection identity,
capability interpretation, command translation and normalization of observations.
The kernel owns command identity, authorization, durable ordering, deduplication
and state transitions. Move `reserve_workspace_open`'s runtime-name switch and
OpenCode-specific evidence interpretation out of Store. An adapter validates
its own native options and returns a normalized prepared launch/observation;
Store validates generic identity/digests and capability contract, not vendor keys.

Prepared launch metadata is not a promise that a process already ran. Preparing
an adapter may start its worker, but native execution begins only for the exact
admitted launch. Stale replies from an older boot, configuration, generation or
closed connection cannot attach a second native owner.

Target local configuration shape (illustrative, not accepted by current Config):

```toml
[modules.opencode]
executable = "<private-module-root>/opencode/<artifact>/swarm-adapter-opencode"
args = []
enabled = true
activation = "on_demand"
ownership = "external_attach"
idle_timeout_seconds = 60

[modules.opencode.restart]
policy = "on_failure"
max_starts = 5
window_seconds = 60
```

`enabled` permits demand-driven technical activation; it does not enable any
manager automation or install/update the executable. Idle timeout applies only
when the worker has no session/operation/stream/owner obligations. Windows uses
its native executable path and private Named Pipe, not a forced Unix launcher.
Schema errors keep the last valid configuration and identify the affected
module; a partially applied update must not restart unrelated services.

## 4. Less authorization ceremony, one effective policy

**Local trust model:** one user-owned controller, not a multi-tenant cloud IAM
product. Authenticate once per connection and keep stable client identity across
reconnect. Register/assign the manager once; its ordinary planning, observations,
peer coordination and execution do not require per-step Root approval. Automatic
worker recovery is infrastructure, not opt-in business automation.

Use one positive method policy shared by CLI/MCP, manual calls, scripts and
on-behalf automation. Generate discovery/schema classifications from that
registry; a profile can narrow it, not grant new rights. Keep deferred tool
loading as UX, not another authorization/activation bureaucracy.

Required first correction: replace the operator-only gates in
`src/store/tasks.rs::{create,revise}` with the ordinary **Operator or Manager**
policy, including the designated GM. Do not substitute the broader
`require_writer()` predicate: it is not an explicit Manager allowlist.
Planning a Task must not depend on an installed runtime, workspace launch,
provider quota, MCP proof or observer readiness. Native prerequisites apply to
that native action when it is actually requested.

For revision, retain expected-revision checks and frozen old Attempts. Resolve
an active Attempt's existing owner: an ordinary manager cannot alter another
manager's live assignment; current GM/operator or an explicit existing ownership
handoff handles that case. Do not introduce a new grant database as a condition
for making local manager Task commands work. Remote/restricted entrypoints keep
their explicit existing scope/profile limits; broad local trust is not public
anonymous access.

Retain only checks that protect a real boundary: actor identity, current resource
ownership, stale revisions/generations, secret/endpoint protection, independent
acceptance, and exact candidate/GM epoch for publication. For deferred external
effects, recheck relevant revocation/ownership/epoch immediately before effect.
That is a machine check, not another human approval or new credential exchange.
Late authenticated results still need to be collected after the actor loses the
right to start **new** work.

Participant, hook-source, module and observer credentials remain scoped; never
pass the operator/manager bearer to a model, script, child or remote gateway.
Missing an optional capability stops only its affected action. Every denial has
one actionable reason and exact next action; do not return a list of invented
approval stages or repeatedly ask the manager for unchanged information.

## 5. Demand-driven lifecycle and recovery

The supervisor keeps cheap installed/configured metadata. Optional workers start
only for an admitted command, a configured due action, or an explicit live
subscription. One single-flight start per module/service scope coalesces parallel
demand. A worker preparing to stop and a concurrent start use one generation/
state transition; a late startup response cannot resurrect a disabled worker.

Disabled/unused modules have no task loop, periodic provider read, extra socket,
model process or recorder thread. Stored descriptors and the small core registry
still consume memory; do not advertise literal zero system overhead. Long-lived
native sessions are active resources even between turns; an idle timeout must
not kill them or forget children/unknown effects. A configured continuous hook
or selected live monitor is intentional demand, not an unused component.

One-shot script bundles capture their version, inputs and allowed effects before
execution. Editing a script/config changes subsequent invocations only. A script
built as Rust has its own package/executable; existing typed-action bundles are
data, not a reason to invent another language or interpreter. Optional user
Python/PowerShell extensions remain external and are not mandatory internals.

### 5.1 Restart policy

| Failure | Required behavior |
|---|---|
| Optional worker error/panic/exit | Record worker/boot/exit evidence, restart **that** worker if still needed, reconnect and read back pending Operations. Kernel/listener and other modules stay running. |
| IPC disconnect, process still alive or identity unknown | Reconnect/read-only reconciliation. Preserve the new `2aec51bb` unknown-outcome transition; disconnect is not proof that the process died. |
| Adapter worker dead, native execution survives | Restart adapter and attach only through its supported recorded-session contract. Do not recreate the native family or replay model input. |
| Proven native interruption | Preserve exact terminal evidence; a new work attempt is a separate manager decision unless an existing enabled policy explicitly requests it. |
| Uncertain native/remote effect | Recover by exact readback. Only the affected operation/resource remains unresolved; unrelated work continues. Never convert a timeout into success, failure or safe retry without evidence. |
| Invalid config, incompatible protocol, repeated identical startup failure | Isolate module, expose concrete error and recovery control; wait for changed config/artifact/evidence rather than hot-looping. No global stop. |
| Kernel/Store failure | Stop new durable admissions, retain native process ownership, restart kernel under its lock and reconcile. Never initialize an empty replacement database over retained state. |
| Recorder/monitor failure or disk quota | Degrade diagnostics with visible loss counters/fallback; do not stop agents or the event journal. |

Proposed default for transient worker crashes: exponential delay from 250 ms to
30 s with jitter, at most five starts in 60 s; reset budget after 60 s healthy.
Budget exhaustion isolates that module with a visible reason; a changed valid
configuration/artifact or explicit restart resets it. These are configurable
starting defaults, **not measured reliability figures**. A library timeout must
not secretly retry a non-idempotent POST underneath this policy.

Restarted supervisor/kernel must adopt verified existing owners under OS locks,
not duplicate them. Retain Windows Job/process birth and Unix process-group
identity; PID alone is insufficient. Preserve owner-first non-killing native
ownership. Bound worker shutdown waits, but do not kill unproven native work to
make shutdown appear clean. A plugin process boundary contains a crash; it is
not an OS sandbox against arbitrary same-user code or a hard memory/CPU quota.

### 5.2 Independent update/build

Place executable artifacts in versioned module directories. Validate compatibility
and atomically change the selected version for **new** admissions. Existing
bindings retain their old artifact until drain or a supported attachment handover.
Keep the prior version for rollback; do not overwrite a running Windows binary.
Compatible recorder/adapter updates never require restarting kernel or unrelated
native sessions. Kernel/schema upgrades still require their own controlled
handover and supported database backup/migration, not a claim of universal hot
swap. Snapshot bytes include any required WAL state; never copy only a live DB
file and call it a verified backup.

Cargo workspace `default-members` must be the small CLI/kernel entrypoints,
not every adapter. Shared root dependency declarations disable unwanted features
at the workspace level as needed by the actual pinned toolchain. Do not adopt a
newer Cargo-doc feature without checking that compiler's support.

Use a fast local profile; keep optimized release packaging separate. Example
**target root-manifest settings**, not a change installed by this PR:

```toml
[profile.iterate]
inherits = "dev"
opt-level = 1
debug = "line-tables-only"
strip = "none"
incremental = true
lto = "off"
```

After extraction, a manager's final scoped pass is, for example:

```sh
cargo fmt -p swarm-adapter-opencode -- --check
cargo clippy --locked -p swarm-adapter-opencode --lib --bins --no-deps -- -D warnings
cargo build --locked -p swarm-adapter-opencode --profile iterate
```

No automatic `cargo clean`, repeated full release build or `--workspace` pass
for every script/adapter edit. Preserve vendored donor bytes; do not include
upstream snapshots in blanket formatting. One manager owns one worktree and
integrates writers before the scoped pass. CI follows changed packages plus
actual reverse dependencies; contracts/lockfile/platform changes widen that
set. Keep existing checks; add PR triggers and coverage of `tests/**`, `tools/**`
and build recipes. A docs-only PR needs docs validation, not native model calls.

Measure cold build, warm no-op, adapter-only edit, kernel-only edit and shared
contract edit separately, recording toolchain/profile/target and built packages.
Acceptance is absence of unrelated rebuilds/restarts, not an invented speedup.

## 6. Implementation sequence and completion criteria

This is the implementation order for the existing O1–O11 program, not a second
Task planner. Reuse landed code and keep one owner/candidate per delivery unit.
Instrumentation starts immediately so the refactor's own failures are visible.

| Step | Concrete change and initial owners | Completion condition |
|---|---|---|
| M1 — usable manager and visible failures | `src/store/tasks.rs`, `src/model.rs`, shared method authorization; `Store::disconnected`, host error handling and small telemetry producer | Manager and current GM create/revise without operator token; observers/modules cannot. An ordinary Manager cannot revise another Manager's unreleased Attempt; the explicit current-GM/Operator path validates the same exact scope and revision. A disconnect persistence error is visible, not coerced into `false`. No unrelated native prerequisites on planning. |
| M2 — real build boundaries | Root manifest/Justfile/CI; extract contracts/client/store/kernel/process/telemetry, preserve public CLI/serialized data | Explicit package graph with no adapter-to-kernel reverse dependency; existing database/receipts still readable. Independent build recipe works on Windows and Linux. |
| M3 — bus and one complete adapter | Generic module descriptor/handshake in supervisor; extract OpenCode translation plus vendor Store paths; independent bus dispatcher and atomic cursor admission | Adapter replacement leaves kernel hash/PID unchanged; bus restart resumes cursor without duplicate command; a second minimal executor needs registration, not a new Store runtime-name switch. |
| M4 — remaining executors and lazy workers | Extract each real adapter, then checks/Forge/scripts/automation and optional frontends | Each has a named owner, demand predicate, bounded queues and local restart. No empty supervisor loops for disabled modules; no Rust migration claim while stateful owned JS/Python control remains. |
| M5 — recorder and live view | Complete `swarm-observer`, log configuration/reload and projections in [Observability](observability.md) | Show manager/agent/module/operation timeline, exact crash/restart facts, lag/loss and selected active resource metrics; changing depth does not restart work. |
| M6 — integrated product qualification | Existing productive O7/O11 delivery path using real manager identity, two distinct adapters and the same common contracts | Task → dispatch → result → independent audit → correction → re-audit → acceptance → exact publication; optional failure and restart do not repeat effects or stop unrelated agents. Record exact evidence and unresolved native gaps. |

M4 workers may be extracted independently after M2/M3 contracts stabilize; do not
make every optional provider a blocker for the first complete productive path.
M1/M2 include basic logging, not a requirement to finish a dashboard first.
Broad fault/load/native testing comes after the relevant product path is complete;
minimal formatting/Clippy and source review are the development gate. No repeated
large fixture campaign for every writer fragment. Preserve existing test assets.

Final negative cases must cover old-worker replies, two simultaneous starts,
revoke/disable versus launch, crash after external send but before outcome commit,
slow/failed recorder, saturated log queue, bus cursor catch-up, native child
survival, stale GM publication and a supported kernel upgrade. Report an unknown
outcome as unknown; fixture success is not native recovery qualification.

## 7. Explicit non-goals

No Kubernetes, Kafka/NATS/Redis/Temporal deployment, second Task DB, generic DAG
language, one always-on process per agent/script, dynamic Rust ABI, mandatory
remote telemetry, new OAuth/RBAC stack, repeated Root gates or model heartbeat.
Do not remove receipts, candidate identity or independent acceptance in the name
of simplicity. Do not equate same-user IPC authentication with sandboxing.

References for package/profile behavior: [Cargo workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html)
and [Cargo profiles](https://doc.rust-lang.org/cargo/reference/profiles.html),
read 2026-10-05. These justify the mechanism, not a benchmark or installed version.
