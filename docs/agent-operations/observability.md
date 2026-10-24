# Observability — local Governor truth

Observation is core product behavior. The Governor must be able to determine who is
working, where, on what, through which native session/process/worktree and what is
waiting, uncertain or broken without calling a provider harness or starting a model.

[Implementation R](implementation.md#r--local-governor-observation) owns source
changes; [current audit AR-142–159](audit-current.md#findings-ar-142159) records source
evidence; [status](../implementation-status.md) records landed behavior. The current
source trace was inspected at `c73c36c72d2c86cf710fa69e39ab07e2052c1f5e` on
2026-10-24. This document defines the target and does not claim that R0–R6 are landed.

## 1. Three data classes

| Class | Authority and lifetime |
| --- | --- |
| Business facts | Store Tasks, Attempts, Operations, results, reviews, acceptance and immutable artifact references. Log policy cannot disable them. |
| Diagnostic records | Optional bounded local logs. Loss is counted. A log line is never an action receipt. |
| Current Governor view | A read-only join of retained business facts and independently collected native/process/worktree/provider facts, with source time, freshness, coverage and gaps. |

Do not duplicate transcripts, Task state or workflow state in an observer. Silence,
idle, disconnect, stale data, confirmed exit and completion are different states. None
is inferred from age alone.

## 2. Current source audit

The current implementation does **not** provide the joined Governor view described
below.

### 2.1 Inventory starts from bindings

`agent.list` enumerates Store bindings. `agent.state` addresses one exact
binding/generation. `agent.family` reads one retained binding observation. These
surfaces cannot enumerate a provider session or native process that has no current
binding.

`store/module_observations.rs::live_reads` narrows further: it reads only ready,
unreleased bindings linked to one unreleased Attempt owned by the requesting Manager,
for the current open Task revision and configured route alias, with no competing
unreleased Attempt for the same Task. `reads_for_manager` then selects the newest
matching generation per route alias.

Consequences:

- unmatched native sessions are invisible;
- surviving processes without a current Eliot binding are invisible;
- older or transferred sessions can disappear behind the newest route generation;
- a shared provider service is represented through whichever binding supplied the
  selected row, rather than once as a source;
- losing or transferring a Task can remove native evidence from the route catalog even
  when the native session/process remains alive.

The current fence is useful for transcript/private-state disclosure. It is not a valid
discovery root. Assignment is a relation applied to discovered subjects.

### 2.2 Process facts are outside Store observation

`swarm-observer::process_metrics` is an explicit one-shot path. It accepts at most eight
caller-selected identities, performs no Store join and reports that its cut is
independent of Store state. Actual metric collection is Windows-only.

The sample path revalidates PID birth/image identity and module-owner membership around
each read. This is useful custody/qualification machinery, not a fleet collector. It
also repeats package/image work that should be installation or launch evidence rather
than ordinary sample cost.

`module_supervisor_observation` retains lifecycle phase, boot, errors and unresolved
Operation IDs. Its public binding projection does not provide the concrete PID, OS
birth identity, process group/Job relation and current liveness sample required to
identify the process. Supervisor status is not process identity.

### 2.3 Workspace projection is admission evidence, not live Git state

A held workspace lease retains assigned repository handle, baseline, branch, worktree
handle, scope and clean-state evidence captured during preparation.
`workspace_lifecycle` is explicitly database-only and never inspects the filesystem or
worktree.

Current projections therefore do not answer:

- actual repository identity at the path;
- actual checkout branch, detached or unborn state;
- current HEAD;
- tracked/untracked changes;
- upstream and push target;
- when each Git fact was observed.

Assigned branch/baseline/scope remains valuable authority. Display it beside, never as
a substitute for, current Git observation.

### 2.4 Remote repository state exists only inside effects

Exact remote-ref readback exists in Forge and correctly classifies an uncertain push
without replay. It runs inside publication/reconciliation work. There is no independent
bounded remote repository/ref cache for a worktree that has not published.

The current system therefore cannot show last observed remote head, source time or read
error independently of a publication Operation. Local tracking refs are not fresh
remote facts.

### 2.5 Provider usage and availability are binding-owned

`native_usage` is retained inside one binding observation and read by
binding/generation. `provider_conditions` retains useful typed condition, source
sequence/evidence, epoch and explicit invalidation, but durable rows still carry route,
binding/generation and native scope and are consumed directly by route admission.

Availability, usage, account/service identity and binding lifecycle are therefore
entangled. A shared source can be copied across bindings, and a stale producer binding
can remain applicable beside a fresher observation.

Target provider observations are source-scoped facts first. Admission derives a
decision from current applicable source facts; it does not own another lifecycle
database. Independent buckets/windows remain separate constraints.

### 2.6 Diagnostics and reports are parallel surfaces

`swarm-observer` has no global subscriber or required host collector. Process sampling
is caller selected; local file following polls only while its CLI command is active.
Optional diagnostics are not the required product observation path.

`report.attention`, `report.capacity`, `report.delta`, `agent.state`, `agent.family`,
runtime catalog, provider conditions, workspace lease views, process metrics and Forge
readback each assemble a different partial projection. No single retained current-state
join owns the answer. A frontend cannot safely repair this by reducing native events.

## 3. Required Governor view

The current projection is keyed by discovered subject and source, not only by binding.
A bounded page answers which facts are known and which source failed.

| Group | Required facts |
| --- | --- |
| Assignment | Manager/agent, Task/revision, Attempt, Operation, binding/generation and historical replacement links. Unassigned subjects remain visible. |
| Native work | Source/service/store, session/root, input/turn, child relations, active tool, pending question/approval/child wait, terminal/error and last content activity. Containment and spawning are separate relations. |
| Process | Module/native/shared-service process, PID, OS birth identity, image identity, group/Job relation, liveness/exit, observed time and collection error. A shared process is counted once. |
| Workspace | Trusted repository, worktree, assigned branch/baseline/write scope plus observed repository identity, actual branch/HEAD/dirty state, upstream and push target. |
| Remote | Repository/ref/head, observation time, method, freshness and error. Publication is optional. |
| Provider | Local provider store/service reference, native IDs, epoch/cursor/offset, observed time, usage/availability and collection gap. |
| Coverage | Per-source collector health, last successful/attempted read, truncation/overflow and whether more subjects may exist. |

The same source/session/process/worktree may relate to several Operations over time. The
join preserves those relations rather than copying the subject once per Operation.

## 4. Target records

### SourceRecord

```text
source_id
kind = provider_store | provider_service | process | worktree | remote_ref
stable native/local locator
collector_epoch
last_attempt_at
last_success_at
freshness
coverage = full | partial | unknown
cursor/offset/revision?
gap/error?
```

A source ID is stable for the actual native store/service/process/worktree/ref. It is
not an artifact version, route alias or binding ID. Host-private locators are not copied
into broad public views.

### NativeSubject

```text
source_id
native_subject_id
kind = session | child | turn | input | pending_request
parent/containing relations
latest normalized state
native sequence/cursor
observed_at
```

Adapters own vendor parsing. Store never learns vendor method matrices.

### ProcessSubject

```text
source_id
pid + OS birth identity
image identity
parent/group/Job/shared-service relation
alive | exited | unknown
cpu/memory/io sample?
observed_at
```

Package bytes are verified at installation/launch. Ordinary samples do not hash the
entire executable again. Control authority still requires exact custody evidence.

### WorktreeSubject

```text
source_id
repository identity
worktree identity/path reference
actual branch/HEAD/dirty
upstream/push target
observed_at
```

Absolute paths remain host-private. Public views expose opaque handles and authorized
relative facts.

### RemoteRefSubject

```text
source_id
repository identity
remote name / ref identity
head object id or explicit absent
observed_at
method
freshness
error/gap
```

Remote observation grants no push/publication authority.

### AssignmentEdge

```text
native/process/worktree subject
<-> binding/generation
<-> Operation
<-> Attempt
<-> Task/revision
relation kind + observed/admitted time
```

An absent edge means unassigned, not invisible. Reassignment/replacement changes edges,
not subject identity.

## 5. Collection ownership

Collect once per source, independent of viewer count.

| Source | Collector owner | Does not own |
| --- | --- | --- |
| Provider store/service | Selected adapter/provider-local reader | Task assignment, acceptance or generic telemetry history |
| OS process | One host process collector | Native completion inference or package installation |
| Local worktree | One host Git collector | Workspace write admission or automatic cleanup |
| Remote ref | One bounded read-only remote collector | Push/publication intent |
| Current joined state | Store current-source/subject/edge tables | Provider calls, filesystem/network reads or another workflow lifecycle |

A lost Eliot control connection does not disable a provider-local reader. A failed
collector preserves the last fact as stale and records a coverage gap. Host restart
changes the collector epoch and invalidates old connection freshness; surviving native
processes remain visible after rediscovery.

Bound bytes, rows, subjects and elapsed work. Per-pass limits retain a continuation and
explicit partial coverage; they do not silently discard later subjects. Coalesce change
hints with periodic local rechecks. Ordinary viewer reads make no provider, model,
filesystem or network call.

## 6. Authorization

Authorize these separately:

```text
source inventory/existence
subject current state
assignment/Task relation
transcript/result bytes
host-private locator/path
control action
```

A Governor may know that an unmatched session/process exists without receiving its
prompt, output, environment or private path. A participant/reviewer may read its exact
assigned content without gaining fleet administration. Observation never grants
control.

Keep current binding/object grants for private native content until their consumers move
to explicit subject/content grants. Do not widen access merely to make the joined view
easier to implement.

## 7. Implementation sequence R0–R6

```text
R0  Source registry
    Add stable SourceRecord identities, collector epoch/cursor/freshness/coverage and
    bounded current subject tables. Keep event history separate from current state.

R1  Provider-local readers
    Run configured readers without Task/binding. Normalize sessions/children/activity,
    pending input and provider usage/availability. Preserve unmatched subjects and
    source gaps. Remove vendor method/artifact matrices from Store.

R2  OS process collector
    Use one selective system refresh for known/observed processes. Retain PID + birth
    identity + image/group relation, shared-service ownership and bounded metrics.
    Remove per-sample executable hashing and the separate receipt-driven metrics CLI
    as the product path.

R3  Git and remote collectors
    Read actual worktree repository/branch/HEAD/dirty/upstream/push target. Maintain a
    small independent remote-ref cache with source time/error. Do not require a
    publication Operation.

R4  Joined Store projection
    Join SourceRecord/subjects to binding, Operation, Attempt and Task edges in one
    short WAL snapshot. Include assigned and unassigned subjects. Do not copy provider
    state into every binding.

R5  One read/subscription model
    `agent.state`/inventory, family views, reports and subscriptions read the same
    current projection. Store events are addressed invalidations with a cursor, not a
    second frontend reducer. Exact Task/transcript access still follows object grants.

R6  Delete replaced paths
    Remove binding-only runtime catalog reads, duplicate provider-condition lifecycle,
    process-metrics product CLI, static lease-as-live-status claims, publication-only
    remote status and parallel observer/report reducers after their consumers move.
```

Do not create a universal entity graph, event broker, telemetry warehouse, ORM,
transcript mirror or one process per source.

## 8. Slice completion rules

Each R slice names:

- exact existing source/caller and new current consumer;
- stable identity and freshness/coverage semantics;
- authorization split;
- collection bounds and failure behavior;
- old product method/storage/config/fixtures removed with cutover;
- minimal affected format/Clippy/build;
- one connected positive case plus explicit unqualified live boundaries.

R0 alone is not progress if no collector or joined reader uses it. A new collector is
not complete while the old binding/report consumer remains authoritative. Shared tables
must be the smallest indexed representation needed by current consumers.

## 9. Diagnostic logging

Diagnostic logging remains optional and independent of required observation.

Levels are `off/error/warn/info/debug/trace`; metadata is default. Selected local
module-supervisor records may include bounded `redacted_text`. Trace never silently
enables prompts, tool arguments, environments, credentials or raw native frames.

The existing recorder keeps bounded records/bytes/segments/retention and explicit loss
counters. A broken optional sink cannot block control or business receipts. Redact
complete bounded logical records before persistence/fanout; split chunks need a
buffered boundary. Protocol stdout stays separate.

Manager `logging.get/set` may alter only its authorized scoped diagnostic policy and
cannot broaden operator restrictions. Policy reload starts no native session and is not
a fleet revision/ACK protocol.

## 10. Failure semantics

Keep these states distinct:

- source current, subject active;
- source current, subject idle;
- source stale or unreadable;
- Eliot link lost while provider-local source remains readable;
- process alive but no recent native progress;
- process exact exit observed;
- subject disappeared from a partial scan;
- terminal native fact observed;
- uncertain native write awaiting readback.

Low CPU, old activity, unchanged Git, parent idle, missing row in a partial scan or a
new PID never proves completion/death. Observation does not kill, restart, reassign,
accept or replay work.

Provider-local history may classify an uncertain native input only when the adapter's
native contract supplies exact identity/readback. It never accepts a Task.

## 11. Connected verification

Minimum connected proofs:

- a session/process without a binding is visible as unassigned;
- assignment and replacement update edges without deleting the subject;
- one shared provider process/account/service is counted once;
- host restart marks old collector epochs stale and rediscovers surviving subjects;
- a partial/failed scan never claims absent subjects are gone;
- current branch/HEAD/dirty can differ from assigned branch/baseline and both display;
- remote head remains visible without publication and carries source time/error;
- ordinary reads make no provider/model/filesystem/network call;
- viewer count does not multiply collection;
- slow subscriber gets a cursor gap and fresh snapshot, not silent loss;
- inventory permission reveals no transcript/result/private path;
- PID reuse never grants control or kills an unrelated process;
- removed binding/report/process/remote products leave no active method, storage,
  profile/subscription, CLI or fallback reader.

Runtime implementation starts only after retained schemas/queries name the actual
owners above. Documentation or synthetic fixtures alone do not qualify live Windows,
Linux, provider restart, Git/remote behavior or scale.
