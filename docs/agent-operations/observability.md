# Observability — local Governor truth

Observation is core product behavior. The Governor must be able to determine who is
working, where, on what, through which native session/process/worktree and what is
waiting, uncertain or broken without calling a provider harness or starting a model.

[Implementation](implementation.md) owns source changes. This document defines the
required boundary and records the current source audit through `cefd74e` on
2026-10-20.

## 1. Three data classes

| Class | Authority and lifetime |
| --- | --- |
| Business facts | Store Tasks, Attempts, Operations, results, reviews, acceptance and immutable artifact references. Log policy cannot disable them. |
| Diagnostic records | Optional bounded local logs. Loss is counted. A log line is never an action receipt. |
| Current Governor view | A read-only join of retained business facts and independently collected native/process/worktree/provider facts, with source time, freshness, coverage and gaps. |

Do not duplicate transcripts, Task state or workflow state in an observer. Silence,
idle, disconnect, stale data, confirmed exit and completion are different states.
None is inferred from age alone.

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
for the current open Task revision and configured route alias. `reads_for_manager`
then selects the newest matching generation per route.

Consequences:

- unmatched native sessions are invisible;
- surviving processes without a current Eliot binding are invisible;
- a shared provider service is represented through whichever binding supplied the
  selected row, rather than once as a source;
- losing or transferring a Task can remove native evidence from the route catalog even
  when the native session/process remains alive.

Assignment is a relation applied to discovered subjects. It cannot be the discovery
root.

### 2.2 Process facts are outside Store observation

`swarm-observer::process_metrics` is an explicit one-shot CLI path. It accepts at most
eight caller-selected identities, performs no Store join and reports that its cut is
independent of Store state. Actual metric collection is Windows-only.

The sample path revalidates process image/membership around each read and the host path
recomputes executable image identity. This is useful qualification machinery, not a
fleet collector. It also repeats package/image work that should be installation or
custody evidence, not a per-sample metric cost.

`module_supervisor_observation::public_projection` exposes helper phase, boot, error
and unresolved Operation IDs, but drops PID, OS birth identity, group/Job identity and
actual liveness sample. Therefore `agent.state` cannot join the supervisor lifecycle
with the concrete process it describes.

### 2.3 Workspace projection is admission evidence, not live Git state

A held workspace lease retains assigned repository handle, baseline, branch,
worktree handle and the clean-state evidence captured during preparation.
`get_lease_view` simply rereads those durable fields. `workspace_lifecycle` is
explicitly database-only and never inspects the filesystem or worktree.

Current projections therefore do not answer:

- actual checkout branch or detached/unborn state;
- current HEAD;
- tracked/untracked changes;
- upstream and push target;
- whether the worktree path still identifies the expected repository;
- when each Git fact was observed.

The assigned branch/baseline remains valuable authority. It must be displayed next to,
not substituted for, current Git observation.

### 2.4 Remote repository state exists only inside effects

Exact remote-ref readback exists in Forge and is correctly used to classify an
uncertain push without replay. It runs inside publication/reconciliation work. There
is no independent bounded remote repository/ref cache for a worktree that has not
published.

Consequently the current system cannot show the last observed remote head, source time
or read error independently of a publication Operation. Local tracking refs do not
repair this gap.

### 2.5 Provider usage and availability are workflow ledgers

`native_usage` is retained inside one binding observation and read by
binding/generation. Its public result explicitly reports account overlap as unknown.
The parser hard-codes current Codex/Muse artifact families.

`provider_conditions` stores source coordinates, but its durable rows still carry the
source binding/generation and are used directly by route admission. Availability,
usage, account/service identity and binding lifecycle are therefore entangled. A stale
source binding can hold later route admission even when the underlying account/service
is shared.

Target provider observations are source-scoped facts first. Admission derives a
decision from a current source fact; it does not own another lifecycle database.

### 2.6 Diagnostics and reports are parallel surfaces

`swarm-observer` explicitly has no global subscriber or host service. Store readback is
one-shot; local file following polls only while its CLI command is active. It is an
optional diagnostic recorder, not the required collector.

`report.attention`, `report.capacity`, `report.delta`, `agent.state`, `agent.family`,
runtime catalog and process metrics each assemble different partial projections. MCP
and observers can page or poll them, but no single retained current-state join owns
the answer. A frontend cannot safely repair this by reducing native events itself.

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
| Provider | Local JSON/JSONL/SQLite source reference, native IDs, epoch/cursor/offset, observed time, usage/availability and collection gap. |
| Coverage | Per-source collector health, last successful/attempted read, truncation/overflow and whether more subjects may exist. |

The same source/session/process/worktree may relate to several Operations over time.
The join preserves those relations rather than copying the subject once per Operation.

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
not an artifact version or binding ID.

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
image/path identity
parent/group/Job/shared-service relation
alive | exited | unknown
cpu/memory/io sample?
observed_at
```

Package bytes are verified at installation/launch. Ordinary samples do not hash the
entire executable again.

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

### AssignmentEdge

```text
native/process/worktree subject
<-> binding/generation
<-> Operation
<-> Attempt
<-> Task/revision
relation kind + observed/admitted time
```

An absent edge means unassigned, not invisible.

## 5. Collection ownership

Collect once per source, independent of viewer count.

- Selected adapter: provider-native events and local provider JSON/JSONL/SQLite.
- Host process collector: OS process identity/liveness and bounded metrics.
- Host Git collector: local repository/worktree facts.
- Remote-ref collector: bounded exact remote repository/ref readback with cache.
- Store: source registry, normalized current facts, assignment edges and authorized
  joined projections.

A lost control connection does not disable provider-local readers. A failed collector
preserves the last fact as stale and records a coverage gap. Host restart changes the
collector epoch and invalidates old connection freshness; surviving native processes
remain visible.

Bound bytes, rows, subjects and elapsed work. Per-pass limits retain a continuation and
explicit partial coverage; they do not silently discard later subjects. Coalesce
change hints with periodic local rechecks. Ordinary viewer reads make no provider,
model, filesystem or network call.

## 6. Implementation sequence R0–R6

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

Do not create a universal event broker, telemetry warehouse, ORM, transcript mirror or
one process per source.

## 7. Diagnostic logging

Diagnostic logging remains optional and independent of required observation.

Levels are `off/error/warn/info/debug/trace`; metadata is default. Selected local
module-supervisor records may include bounded `redacted_text`. Trace never silently
enables prompts, tool arguments, environments, credentials or raw native frames.

The existing recorder keeps bounded records/bytes/segments/retention and explicit
loss counters. A broken optional sink cannot block control or business receipts.
Redact complete bounded logical records before persistence/fanout; split chunks need a
buffered boundary. Protocol stdout stays separate.

Manager `logging.get/set` may alter only its authorized scoped diagnostic policy and
cannot broaden operator restrictions. Policy reload starts no native session and is
not a fleet revision/ACK protocol.

## 8. Failure semantics

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

## 9. Authorization and verification

Authorize source inventory, subject details, Task relations, transcript/results and
private paths separately. A Governor may know that an unmatched session exists without
receiving its prompt or output.

Minimum connected proofs:

- a session/process without a binding is visible as unassigned;
- assignment and replacement update edges without deleting the subject;
- one shared provider process/account is counted once;
- host restart marks old collector epochs stale and rediscovers surviving subjects;
- a partial/failed scan never claims absent subjects are gone;
- current branch/HEAD/dirty can differ from assigned branch/baseline and both display;
- remote head remains visible without publication and carries source time/error;
- ordinary reads make no provider/model/filesystem/network call;
- viewer count does not multiply collection;
- slow subscriber gets a cursor gap and fresh snapshot, not silent loss;
- PID reuse never grants control or kills an unrelated process.

Runtime implementation starts only after the retained schemas/queries name the actual
owners above. Documentation or synthetic fixtures alone do not qualify live Windows,
provider restart or scale behavior.
