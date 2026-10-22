# Modular Runtime — build, process and authority boundaries

This is the compact target, not a claim about current source. The earlier
[review](modularity-review-2026-10-05.md) remains historical evidence;
[implementation](implementation.md) owns migration and deletion order;
[status](../implementation-status.md) records landed code.

## 1. Required outcome

A registered Manager agent can plan, execute, review and deliver work without the
operator credential or repeated human approvals. Optional adapters and effect workers
can be built, started, replaced and failed independently. Recovery restarts software
and reads retained/native state; it does not replay uncertain model or external input.

Keep one durable Task/Attempt/Operation authority, one addressed mailbox, one review/
acceptance path and one local Governor view. Do not replace them with another broker,
workflow language, compatibility service or model-driven heartbeat.

Three boundaries are different:

- source module versus Cargo package;
- package versus process/fault boundary;
- restarted adapter versus resumed native session/effect.

Use a boundary only when it isolates a retained responsibility and deletes concrete
duplication.

## 2. Package and process map

Use one Cargo workspace/lockfile. The target process map is:

| Responsibility | Target owner/boundary |
| --- | --- |
| Durable domain authority | `swarm-kernel-host` with Store, one writer and bounded WAL readers. `swarm-kernel` keeps pure Task/review/acceptance rules; `swarm-store` keeps SQLite mechanics. |
| Time admission | One in-process host loop computes current due work and admits ordinary Operations. No standalone scheduler service for Store bookkeeping. |
| Process custody | `swarm-supervisor` + `swarm-process` own exact executable, PID/birth, Job/process group, private bootstrap and lifecycle. |
| Native execution | One selected `swarm-adapter-*` implementation per retained harness. Adapter owns native transport/session/readback, never a second Task database. |
| External effects | On-demand selected executables for checks, scripts and remote Git/GitHub only where real external I/O or process custody justifies a process. |
| Bus/observation | Store commits facts/cursors with domain state. `swarm-bus`/observer components may distribute or record them but own no second database or Task lifecycle. Required provider/process/Git collectors run independently of optional diagnostics. |
| Frontends | `swarm-cli`, `swarm-mcp` and optional single-tenant `swarm-gateway` call the same application methods through `swarm-client`. |

Current `swarm-automation`, legacy optional workers and built-in adapters remain
migration surfaces, not permanent target processes merely because a package exists.

Dependency rules:

1. Host/kernel, supervisor and frontends do not link adapter implementations.
2. Adapters do not depend on one another or on host internals.
3. Host does not link effect-worker implementations after their selected contracts are
   moved to shared owners.
4. Shared libraries never import the host or encode vendor state machines.
5. Public command parsing belongs to `swarm-cli`; host keeps daemon/bootstrap and only
   genuinely local administration.

### 2.1 Bus modularity without broken transactions

Domain state and its durable observation/receipt are committed in the same Store
transaction. A bus can wake consumers and deliver retained facts; it cannot be the
place where a Task transition becomes durable.

A consumer supplies its authenticated identity, expected Store cursor/current subject
and requested ordinary actions. Store authorizes and admits bounded Operations, then
advances durable cursor/occurrence identity where needed. No arbitrary SQL, closure or
`run_as` string crosses IPC.

A bus/notification restart resumes from Store. Volatile notifications are coalesced
wake hints. Missing recorder or bus does not stop already admitted work, direct
readback or state transitions. Store failure does stop new durable admissions; another
process cannot manufacture the missing commit.

## 3. Module protocol and registration

Use the existing bounded IPC and replace the current string/vendor proof matrix with
one current typed semantic protocol.

An installed package records only:

```text
module/package inventory identity
executable + argv/resources
current protocol major
required semantic actions
lifecycle ownership
bounded typed configuration
selected byte identity verified at install/launch
```

Build commit/toolchain may be diagnostic. Artifact version, full command/event schema
arrays and source checkout are not runtime authorization or behavior switches.

At hello:

1. `swarm-process` verifies the supervisor-owned process, binding, generation, boot,
   client credential and private state directory.
2. Adapter reports protocol major and compiled semantic capabilities.
3. Store verifies the selected required subset and marks the authenticated link ready.

Do not echo/compare the entire descriptor on both sides. One package/version profile
ladder per new capability is a failure of the common boundary.

The semantic action set is owned by
[module API](module-api.md): Open, Dispatch, Send, Reply, Configure, Goal, Background,
Cancel, Recover, Refresh, Result, Reconcile and Shutdown with explicit supported
scopes. Vendor method names, local database shape and native recovery stop at the
adapter.

Store retains Task/Attempt/Operation/binding identity. The adapter receives an
Operation reference plus a compact rendered TaskPacket where needed. Current
`swarm.task_prompt@1`, full dispatch context and repeated receipt hashes remain
transitional formats for old consumers; they are not the target. Preserve the frozen
per-requirement evidence plan in Store and render its meaningful verification criteria
for the agent. Do not replace useful criteria with controller proof JSON.

A normal outcome returns Operation ID, stale generation/boot guard, native identity,
disposition and bounded action details. Result pages retain byte range/content digest.
Package/protocol/request/Task hashes are not repeated in every receipt.

Terminal/result delivery to Eliot is separate from native effect recovery. Adapters
may retain already-formed Store deliveries until ACK. They never persist/replay native
prompt in the common delivery spool. Observation is a replaceable current source
checkpoint with explicit coverage, not an infinite event mirror.

Registration/catalog reads use retained installed metadata and do not spawn providers,
load models or contact accounts. Selection affects future bindings. A module catalog
entry grants no Store action right.

## 4. Less authorization ceremony, one effective policy

This is a local user-owned controller, not a multi-tenant cloud IAM product.
Authenticate once per connection and keep stable client identity across reconnect.
Register/assign Manager once; ordinary planning, execution, observation and peer work
do not require repeated Root/operator approval.

Use one positive method policy shared by CLI, MCP, helpers and scripts. Frontend
profiles may narrow exposure; they never grant new rights. Each retained domain owns:

```text
typed parse
exact role + object + revision/generation checks
state/effect handler
```

An enabled helper acts on behalf of its current Manager and rechecks the same object
rights at admission/effect. It does not receive a manager bearer or create another
credential exchange. Governor and Manager are agent roles; direct command does not
mean human approval.

Preserve real boundaries:

- actor identity and exact Task/Attempt/candidate/resource scope;
- current owner/revision/generation/boot;
- secret and endpoint protection;
- independent review and requirement-linked acceptance evidence;
- exact external target and readback after ambiguous write;
- late result collection without granting new work.

Remove document-edition hashes, package-version policy, preview digests and historical
contract matrices as permissions. Missing optional capability stops only that action
with one exact next step. Known native permission rules answer authorized requests;
unresolved judgment is routed once to the Governor agent. Operator input is for
missing credentials/authority or owner intent not represented in policy.

Participant/reviewer/module/observer/gateway identities remain scoped. Never pass
operator or Manager credentials to a model, script, native child or remote bridge.

## 5. Demand-driven lifecycle, unified supervision and recovery

Each process/service scope and native session has one owner. Reuse `ModuleSupervisor`
and `swarm-process`; remove parallel supervisors and legacy worker slots as their
retained responsibilities move.

Optional components start only for:

- an admitted command/effect;
- configured due work;
- an explicitly enabled source collector/live subscription.

Pure Store bookkeeping stays in the host. Do not register a process/credential for
every helper.

Single-flight start coalesces parallel demand. A close and concurrent start share one
generation/state transition; a late startup cannot resurrect a disabled component.
Shared native services are not duplicated per session. Adapter disconnect does not
prove native process/session death and must not call native close by default.

Long-lived native sessions, Goals, children, pending requests and uncertain effects
remain active obligations even between model turns. Idle timeout applies only when
none exists. Observation never restarts or kills work merely to make status green.

### 5.1 Restart policy

| Failure | Required behavior |
| --- | --- |
| Optional effect worker exits | Record boot/exit; restart only if current work still needs it; read back its exact Operation. Unrelated work continues. |
| Eliot IPC disconnect, process/native state may survive | Reconnect and drain retained Store deliveries; preserve unknown native outcome. |
| Adapter dead, native session survives | Verify previous owner departure and resume/attach only through the selected native contract; never replay input. |
| Proven native interruption | Retain exact evidence; policy may create a separate retry/Attempt after disposition. |
| Uncertain native/external write | Exact readback or visible unknown; isolate only its resource. |
| Invalid config/protocol/startup loop | Isolate selected module and wait for changed config/artifact/evidence; no global stop or hot loop. |
| Host/Store restart | Stop new admissions, preserve process custody, reopen retained DB under lock and reconcile. Never initialize an empty replacement over retained state. |
| Collector/diagnostic failure | Keep last fact stale with gap/loss counters; never stop agents or erase fleet. |

Restart/adoption requires PID + OS birth/image/group/Job identity, not PID alone. An
identity mismatch clears stale ownership but kills nothing. Uninspectable remains
unknown. Bound shutdown waits; do not kill unproven native work to make shutdown clean.

A route may define bounded restart/backoff defaults, but timeout does not authorize
retry of a non-idempotent native request.

### 5.2 Independent update, build and production source hygiene

Keep large integration suites and fake runtimes outside production `src/` or behind
`#[cfg(test)]`; do not expose production internals merely to relocate tests. Test
relocation is source hygiene, not runtime simplification by itself.

Install executables in versioned private package directories. Validate current
protocol/actions and selected bytes, then atomically select the package for new
bindings. Existing work drains under its selected package or uses a supported attach
handover. Do not overwrite running Windows binaries or keep historical versions as
permanent compatibility branches.

Cargo workspace `default-members` should be the small normal entrypoints. Adapter-only
changes should not relink host or unrelated adapters. Build provenance is diagnostic;
checkout/compiler/cache-path equality is not protocol compatibility.

Measure cold build, warm no-op, adapter-only, kernel-only and shared-contract edits
separately. Acceptance is absence of unrelated rebuild/restart and connected behavior,
not an invented speedup.

## 6. Implementation ownership

[Implementation](implementation.md) owns the only work order and exact source slices.
This document owns boundaries only. Historical M1–M6 programs, current package
existence and old descriptor schemas do not add work to the target.

A modularity slice is complete when:

- the selected owner is connected end-to-end;
- previous owner/callers/config/fixtures/package branches are removed;
- retained automatic behavior still works without routine human intervention;
- unknown effects remain honest;
- minimal affected format/Clippy/build and one connected behavior check pass;
- net production Rust/JS/glue is reported separately from tests.
