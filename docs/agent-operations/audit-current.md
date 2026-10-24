# Current audit register

This is the active continuation register. AR-001–078 remain in the
[historical register](audit-register.md). Later ranges are frozen at their merge
checkpoints:

- AR-079–088 at [`b7a5fe1`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b/docs/agent-operations/audit-current.md);
- AR-089–099 at [`e42c464`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e42c464d58db770cc5490f46f391843f9fec8df2/docs/agent-operations/audit-current.md);
- AR-100–114 at [`e99c06b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e99c06bb490fe630eec70337ce5137a26e145d84/docs/agent-operations/audit-current.md);
- AR-115–126 at [`053f1e0`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/053f1e0fb15b3d40c00918f4a25ec8f9e4969e02/docs/agent-operations/audit-current.md);
- AR-127–141 at [`c73c36c`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c73c36c72d2c86cf710fa69e39ab07e2052c1f5e/docs/agent-operations/audit-current.md).

[Audit index](audit-index.md) maps every range to immutable evidence;
[implementation](implementation.md) owns source changes;
[observability](observability.md) owns the target Governor view; and
[status](../implementation-status.md) owns landed behavior.

## Resume here

- Checkpoint: **2026-10-24 / local Governor observation and source ownership**.
- Source inspected at **`c73c36c72d2c86cf710fa69e39ab07e2052c1f5e`**.
  Runtime source for this chain remains the code through
  **`c92120ed70ca7e625440a8a19517c481e73b13c9`**; later merged changes are
  documentation plus the disconnected GitHub cleanup in `98b350e`.
- Connected chain:
  binding-rooted `agent.list/state/family` and `runtime.catalog.get` →
  `module_observations::{live_reads,reads_for_manager}` → provider usage/condition
  retention and route admission → module-supervisor observation/public projection →
  explicit `swarm-observer` process metrics → workspace lease/lifecycle projection →
  Forge-only remote-ref readback → report/observer/frontend surfaces.
- Overall audit: **incomplete, 159 entries**. AR-142–159 are recorded below; prior
  dispositions remain in immutable checkpoints.
- Preserved in this pass: current binding/boot/observation fences; explicit gaps rather
  than invented values; provider condition sequence/evidence checks; PID birth and
  owner-membership validation in selected process samples; database-only workspace
  release conservatism; remote exact readback after an ambiguous push; passive
  observation that never accepts, reassigns, restarts or kills work.
- Open in this pass: discovery begins at assigned ready bindings; unassigned native
  sessions and surviving processes are invisible; route catalog keeps one newest
  Manager-owned binding per alias; provider/account facts are entangled with bindings;
  process metrics is a caller-selected Windows one-shot path; public supervisor state
  omits concrete process identity/liveness; workspace view is launch-time authority,
  not live Git; remote state exists only inside publication; no single joined
  source/subject/assignment projection exists.
- **Next exact chain:** remote gateway:
  `crates/swarm-gateway::{main,config,gateway}` → credential/bearer file loading →
  loopback/Host/Origin/auth/body/session bounds → MCP facade and fixed profile → timeout
  as unknown → kernel-host compatibility launcher → disconnect and admitted Store work.
- Runtime implementation order remains **A1b–A3, then B–V**. Source-audit order does
  not reorder implementation.
- PR #190 is merged at `c73c36c`. Exact head `a93ab20`, workflow run `40090027947`,
  passed changed-path and Markdown validation. Its final diff was documentation-only;
  the selected J0 MCP lock-release fix is still **not landed**.
- Still unqualified: independent provider readers, real process enumeration on Windows
  and Linux, host restart rediscovery, live Git/remote collection, partial-source
  resumption, viewer-count independence, PID reuse, scale/fairness and the installed
  joined Governor view.

On resume, compare live `main` with the inspected head, reopen only changed owners and
consumers, then continue the named gateway chain. Documentation or a retained binding
snapshot is not live fleet qualification.

<a id="findings-ar-142159"></a>
## Findings AR-142–159

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-142"></a>AR-142 | Discovery root | **Open architectural defect.** `agent.list/state/family` enumerate Store bindings. A provider session, native child or process without a current binding cannot enter the inventory. | Discover subjects from stable provider/process/worktree sources; assignment is an edge, not the discovery root. |
| <a id="ar-143"></a>AR-143 | Manager native evidence scope | **Preserve authorization, repair visibility.** `live_reads` selects only ready, unreleased bindings linked to the requesting Manager's unreleased Attempt for the current open Task revision and excludes competing live Attempts. | Keep transcript/object disclosure scoped, but expose unmatched subject existence/state through a separate inventory grant. |
| <a id="ar-144"></a>AR-144 | Route catalog selection | **Open loss.** `reads_for_manager` reduces retained observations to the newest matching generation per configured route alias. Surviving older sessions, transferred work and shared service subjects disappear or are represented through one arbitrary binding. | Source/subject inventory independent of alias; show all current subjects and assignment/replacement edges. |
| <a id="ar-145"></a>AR-145 | Configuration versus observation | **Preserve distinction.** Runtime catalog explicitly keeps configured target, descriptor declarations, observed model and gaps separate; it refuses to claim installation, native capability or account identity from configuration. | Reuse this honesty in the joined view; replace route-shaped discovery, not evidence labels. |
| <a id="ar-146"></a>AR-146 | Provider condition ownership | **Open entanglement.** Retained provider conditions carry route, binding/generation and native scope along with service/provider/account/model. Admission consumes these rows directly, so a shared source is copied and lifecycle-coupled to its producer binding. | Stable source/account/service facts first; route admission derives a decision from the current applicable source facts. |
| <a id="ar-147"></a>AR-147 | Provider freshness and supersession | **Open cross-binding boundary.** Current rows preserve source epoch/sequence/evidence and explicit invalidation, but same-source successor relationships are not a first-class source identity. A stale binding copy can remain applicable beside a fresher observation. | Source-owned epoch/cursor and same-source supersession; independent quota windows remain separate constraints. |
| <a id="ar-148"></a>AR-148 | Process metrics path | **Open product gap.** `ProcessMetricsSampler` samples at most eight caller-selected identities, has no process enumeration/timer/service and produces real metrics only on Windows. | One host process collector discovers/refreshes known subjects independent of viewer calls and persists bounded current facts. |
| <a id="ar-149"></a>AR-149 | Process identity cost | **Preserve safety, reduce repetition.** Selected host/child samples revalidate PID birth, image identity and module-owner membership; unknown/changed identity remains unavailable. Image/path hashing and membership work are repeated around samples. | Keep PID+birth/image/group checks for custody; verify package bytes at install/launch and avoid whole-image hashing per ordinary sample. |
| <a id="ar-150"></a>AR-150 | Supervisor public process state | **Open projection gap.** Supervisor observation retains lifecycle/boot/error/unresolved Operation evidence, but public `agent.state` projection does not provide the concrete PID, OS birth identity, process group/Job relation and current liveness sample required to identify the process. | Join supervisor lifecycle to the process subject; status-only observations never substitute for process identity. |
| <a id="ar-151"></a>AR-151 | Workspace authority versus live Git | **Preserve authority, repair observation.** Workspace lease/lifecycle owns assigned repository handle, baseline, branch, scope and conservative release. It is explicitly database-only and never inspects the worktree. | Show assigned facts beside independently collected repository identity, actual branch/HEAD/dirty/upstream/push target and observed time. |
| <a id="ar-152"></a>AR-152 | Remote repository view | **Open coupling.** Exact remote-ref readback exists inside Forge publication/reconciliation and correctly settles ambiguous pushes. There is no independent repository/ref/head/time/error cache for ordinary Governor observation. | Small read-only remote-ref collector independent of publication and push authority. |
| <a id="ar-153"></a>AR-153 | Shared process/account subjects | **Open duplication risk.** Binding-shaped views can represent one provider service/account/native process once per binding or hide it behind one selected route row. | Stable source and subject IDs; one shared process/account/service fact with many assignment/session edges. |
| <a id="ar-154"></a>AR-154 | Joined Governor view | **Open fragmentation.** Runtime catalog, `agent.state/family`, attention/capacity/delta, provider conditions, workspace leases, process-metrics CLI and Forge readback each own a partial projection. No single cut answers who/what/where/waiting/unknown. | One read-only WAL snapshot joining source records, current subjects and assignment edges with per-source coverage/gaps. |
| <a id="ar-155"></a>AR-155 | Collector independence | **Open.** Required facts are produced by binding callbacks, explicit CLI ticks or effect execution. Viewer count and Task/binding lifecycle still determine what is collected. | Collect once per configured source independent of viewer count, Task ownership and optional diagnostics. |
| <a id="ar-156"></a>AR-156 | Freshness and absence semantics | **Preserve law, missing common owner.** Existing components distinguish stale/current, partial/unavailable and identity-changed states. No common source record owns last attempt/success, epoch, cursor, coverage and gap. | SourceRecord per provider/process/worktree/remote source; failed/partial scans retain last facts and never prove disappearance. |
| <a id="ar-157"></a>AR-157 | Passive observation | **Preserve.** Observation/status paths do not accept Tasks, retry prompts, reassign work or infer native death from a missing status sample. Workspace release also requires positive retained disposition. | Keep observation side-effect free; policy acts separately through ordinary commands with exact authority. |
| <a id="ar-158"></a>AR-158 | Observation authorization | **Open model boundary.** Current binding ownership protects private native facts, but there is no separate grant for fleet existence/state versus Task relation, transcript/result bytes and host-private paths. | Authorize inventory, subject detail, Task edges, content and private locators separately. Unassigned existence need not disclose prompts/output. |
| <a id="ar-159"></a>AR-159 | Replaced observation products | **Open deletion boundary.** Binding-only catalogs, duplicated provider-condition lifecycle, explicit process-metrics product CLI, static lease-as-live claims, publication-only remote status and parallel report reducers remain current consumers. | Move consumers to the joined projection, then remove old methods/storage/CLI/reducers in the same slices. |

<a id="trace-2026-10-24-governor-observation"></a>
## Source trace at c73c36c: local Governor observation

Inspected 2026-10-24. This is source evidence, not a live fleet/process/Git/remote
qualification.

### AR-142–145 — assignment currently controls discovery

The current native inventory begins at bindings. `module_observations::live_reads`
queries only bindings that are ready and unreleased, joined to an unreleased Attempt
owned by the requesting Manager, joined to the current open Task revision, with no
second unreleased Attempt for that Task. Foreign state JSON is deliberately excluded in
SQL before parsing. That is a useful content-disclosure fence, but it also means a
session/process vanishes from the inventory when its Task, Attempt, owner or binding no
longer matches, even if the native subject survives.

`reads_for_manager` then maps retained observations to configured routes and keeps one
newest generation per alias. `runtime.catalog.get` projects route configuration,
registered descriptor metadata and that one selected live observation. It correctly
labels native catalog/capability/account/installation gaps and does not turn configured
values into observed facts. Preserve the evidence distinction; replace route alias as
the discovery key.

Target relation:

```text
source -> native/process/worktree/remote subject
subject <-> binding/generation <-> Operation <-> Attempt <-> Task/revision
```

An absent assignment edge means unassigned, not invisible.

### AR-146–147 — provider facts are typed but binding-owned

`provider_conditions` retains typed condition, source sequence kind, native sequence,
connection/evidence/auth references, module sequence, source epoch, observation ID and
digest. Invalid or stale collections do not manufacture `Available`; account/connection
invalidation is explicit. These are useful mechanisms.

The retained `ResourceScope`, however, also contains route alias, binding ID/generation
and native scope key. Scope derivation starts from route/binding; rows are then consumed
by route admission. Service/provider/account/model coordinates do not have an
independent source lifecycle. The target promotes actual account/service/store to the
source identity and relates the producing binding as evidence. Same-source successors
supersede stale copies; independent buckets/windows remain simultaneously applicable.

### AR-148–150 — process qualification is not fleet collection

`swarm-observer::ProcessMetricsSampler` accepts a caller-selected list of at most eight
subjects. It enumerates no processes, runs no timer and keeps only CPU baselines. On
non-Windows platforms it returns unsupported samples. Host/child constructors and each
sample validate PID birth/image identity and, for children, module-owner membership.
This is valuable custody/qualification logic.

It is not a current fleet source. Selection must already know the process, and ordinary
samples repeat image/membership work. Supervisor observation supplies phase, boot,
errors and unresolved Operation IDs, but the public binding projection does not expose
the concrete PID+birth+group/Job+liveness tuple. A joined view needs one process subject
per actual process, linked to supervisor/module/native relations. Shared services are
counted once.

### AR-151–152 — workspace and remote facts are effect evidence

`workspace_lifecycle` explicitly performs database-only reconciliation. It protects
leases conservatively using Task/Attempt/Operation/binding/producer evidence and never
inspects or deletes a worktree. The retained branch/baseline/clean-state fields are
assignment/admission authority, not current filesystem truth.

No current product source answers actual repository identity, branch/HEAD/dirty state,
upstream or push target with observation time. Exact remote-ref reads exist in Forge and
are correctly used to settle a publish/reconcile effect, but there is no independent
remote view when no publication runs. Local tracking refs cannot be relabelled as fresh
remote facts.

### AR-153–156 — one source/subject model, not another graph framework

Current facts are split across binding observations, provider-condition rows, process
sampling output, workspace leases, Forge results and several report/catalog reducers.
A shared account/process/service can be copied per binding or represented only through
the selected route. The compact target needs only:

```text
SourceRecord
  stable actual source locator/kind
  collector epoch/cursor
  last attempt/success
  freshness + full|partial|unknown coverage
  explicit gap/error

CurrentSubject
  source + native/process/worktree/remote identity
  latest normalized state + native sequence/time

AssignmentEdge
  subject <-> binding/Operation/Attempt/Task relation
```

This is not a universal entity graph or telemetry warehouse. Reuse existing IDs and the
smallest indexed representation needed by inventory/state consumers. Collect once per
source; viewer reads use committed facts only.

### AR-157–159 — preserve passive law and separate disclosure

Observation cannot accept a Task, prove output capture, restart a provider, kill a
process or replay an uncertain effect. Missing rows in a partial scan, old activity,
low CPU and parent idle do not prove completion/death. Actions remain ordinary
Store-authorized commands after policy decisions.

A Governor may need to know that an unmatched session/process exists without receiving
its transcript, result bytes or host-private path. Split those grants. After consumers
move to the joined projection, remove binding-only catalogs, duplicated provider
condition lifecycle, the process-metrics product CLI, static lease-as-live claims,
publication-only remote status and parallel report reducers. Do not leave another
observer database or compatibility facade.

## Inspected source boundaries

| Source | Read boundary |
| --- | --- |
| `store/module_observations.rs` | Binding/Task/Manager-scoped retained native evidence and current-fence checks. |
| `store/runtime_catalog.rs` | Per-route reduction and explicit configured/descriptor/native/account/installation gaps. |
| `store/provider_conditions.rs` | Typed source evidence plus binding-shaped resource scope and admission projection. |
| `store/module_supervisor_observation.rs`, `operations::get_binding_public` | Supervisor lifecycle/status retention and public projection boundary. |
| `swarm-observer/process_metrics.rs`, `metrics_cli.rs` | Caller-selected bounded Windows samples and one-shot product surface. |
| `store/workspace_lifecycle.rs`, workspace lease views | Database-only authority and conservative release; no filesystem/Git observation. |
| `store/forge.rs`, `swarm-forge-worker` | Publication/reconciliation remote-ref readback only. |
| `agent.state/family`, runtime catalog and report/observer surfaces | Fragmented consumers/projections without one source cut. |
| [Observability target](observability.md) | R0–R6 source/subject/edge/collector/join/deletion program. |

Selected paths, not whole-project certification. Provider-local store formats, live
Windows/Linux process discovery, host restart, Git/remote behavior and scale remain
unexecuted.

## Next work

Continue the remote gateway chain named in [Resume here](#resume-here). Runtime
observation work remains [implementation R](implementation.md#r--local-governor-observation):
source registry, provider readers, process collector, Git/remote collectors, joined WAL
projection, one read/subscription model and deletion of replaced surfaces.
