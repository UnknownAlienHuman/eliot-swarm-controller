# Implementation status

## Current state

### Current queue source closeout — 2026-10-06

The owner requested stopping development and saving/pushing the current work.
The queue is **not fully complete**. The source closeout includes shared mailbox,
Goal V5, managed-Issue-label GitHub projection with stale queued-operation
rejection/fair drain, Thread V4, immutable contract proposals/responses,
advisory Scope intents, integration acknowledgements/agreements and their
18-method Store routing. Final frontend state and the exact publication are
recorded in [the restart checkpoint](restart-checkpoint-2026-10-06.md).

Thread IDs now follow `coord-<uuid>`. Retained Thread/cell history uses actual
Task/Attempt relationships and currently registered Manager/GM/Operator or exact
retained-owner authority; the former GM session and old Attempt liveness do not
lock out history. Current writes still require their exact active scope. Own
rejected ACK receipts return before cell lookup; stale agreements cannot become
`peer_agreed`.

The automatic GitHub action is limited to one configured managed Issue label.
Only provably unsent stale queued Operations may be rejected; sending/uncertain
writes remain readback-only. Goal Accepted/Applied outcomes require exact native
admission; Rejected/Unknown outcomes retain errors without success evidence.
Truthful terminal producer coverage remains limited to Codex/OpenCode.

Contract `ratify/reject` is **not integrated or publicly exposed**. Frozen V1
and incomplete private V2 candidates are preserved for continuation. I6 Git
inspection, the remaining Thread lifecycle, cross-adapter Goal parity, broader
automatic GitHub effects and native qualification remain unfinished. No further
source slices or verification are started after this closeout.

Current checks are source inspection, exact hashes, owned-file formatter parsing
and whitespace checks only. **No current compiler/build, tests or native/provider/
model calls ran.** Product status remains `PARTIAL_PROGRESS`; historical checks
below apply only to their recorded commits.

### Source delivery and remaining qualification — 2026-10-06

The product source integration, physical Kernel relocation, five Rust adapter
paths and public CLI/host coordinates are present. Strict production Clippy at
`6074dcf3be886d33a6cac12f6dab3f3a1bce10cd` checked all 24 selected packages,
including the five Rust adapters, with `-D warnings` and passed without warnings.
Four focused writer readiness/failure regression cases passed in one run at
that same commit, including a deterministic initializer-error case after
admission closure and confirmed writer exit. Source closure preceded these
tests.

The first Store failure selector stopped during test compilation. The test-only
batch at `9b5acc98243bb9419046546e3743b3a60b6b2b56` repaired the stale bootstrap
arguments and writer owner without weakening authorization or failure assertions.
Six exact Store selectors then passed: terminal error/event propagation, worker
panic without replay, writer join after reader panic, retained restart hold,
bootstrap credential validation and module handshake rejection. Two release
process cases also passed for bounded probe/descendant cleanup and retained
Task/Operation recovery after host interruption. Together with the four writer
cases, these are 12 focused cases, rather than a full suite or native qualification.

One consolidated release build of 17 production binary packages passed at
`9b5acc9`. It reported two warnings in debug-only OpenCode resource code. The
current source scopes those declarations to debug builds. Formal
packaging exposed strict-mode accesses to absent optional policy fields and an
IPC provenance path left behind by Kernel relocation. Source now handles optional
policy fields, rejects reparse traversal before build activity and reads the
actual Kernel IPC source. Dependency provenance distinguishes absent fields
from valid empty arrays and nullable sources. These repairs precede further
packaging and tests. Native CLI metadata was read without model
inference. Command Code's bundled model selector is
`inclusionai/ling-3.1-flash:free`; upstream execution remains unverified.
The product remains `PARTIAL_PROGRESS`. Installed adapter qualification and the
complete productive workflow remain pending. Historical binaries and receipts
retain only their recorded scope.

The integrated Concilium source delivery covers eight typed methods across wire,
Store, MCP and CLI, with actor/slot authorization, blind Round 1 positions and
shared request bounds (one participant is valid; there is no 64-actor cap).
The five exact
mutation kinds—`concilium.propose`, `concilium.open`,
`concilium.position.submit`, `concilium.round.advance` and `concilium.close`—use
the existing controller Operation/Observation provenance. Sponsor identity is
retained provenance; mutations still check current authority.
Scoped Managers list their exact current Attempt by default; historical sponsor
reads remain available, while the current GM or local Operator may list the Task.
A Round 3 merged proposal digest is a manager-attested reference, not proof of
content. Doctor
adds only finite method/state and malformed-projection counts. Participant
subscriptions remain unavailable because that profile does not expose
`report.delta`.

The automation v6 source correction is integrated in the source worktree and
passed its source audit: it revalidates exact ScriptRun event holds and repairs
Goal source-gap retention/classification, pending/fresh fairness, and the rule
that only incoming contiguous events advance the source cursor. The combined
source remains uncompiled and untested, with no fresh runtime, model or provider
qualification. Concilium methods start no model or native adapter and mutate no
Task state; close is advisory. The historical checks above retain their narrow
scopes, and product status remains `PARTIAL_PROGRESS`.

RepairDispatch-only entries are now discovered by both the paginated and
wraparound review-result cursor queries. They reach the existing typed repair
consumer, which still validates exact applied feedback, current Task/Attempt,
candidate and manager authority before admitting a correction. This source
repair has not been compiled or tested.

MCP `automation.config.preview/apply` now describes bounded change items and the
existing event-rule forms, including exact generic ScriptRun selectors and the
applied-submission ReviewDispatch selector. Partial patches may reuse the entry's
existing steps; absent, null and empty event-rule arrays retain their distinct
Store semantics. This is input-schema source delivery, not runtime qualification.

Descriptor-backed demands now bypass the optional OpenCode owned-service
projection when their retained route declares no such service. Their validated
native options reach the selected adapter unchanged. Routes declaring an owned
OpenCode service retain the existing strict service, workspace and route checks.
This closes a source blocker for adapter-specific workspace option shapes; it
does not qualify a new adapter.

`agent.inspect` now includes a bounded projection of retained Manager attention
for the Attempt's exact binding ID and generation. Missing bindings, source gaps,
additional pages and detached items keep coverage explicitly incomplete and
provide the existing `swarm.exceptions.get` requery. The public page size remains
50 and the internal raw attention scan remains 200. These source changes have
not been compiled or tested.

Shared `coordination_limits` are present for the integrated Thread, peer-contract and
integration-acknowledgement source implementations. They retain the
64 KiB request bound, bounded UTF-8 fields and read pages, without adding a roster
count cap. The exact delivered methods and remaining public frontend work are recorded in
the current checkpoint; constants alone do not qualify runtime behavior.

The dependency source repair moves the selected rustls lock from `0.23.44` to
`0.23.45` for GHSA-2mjx-qc3c-rqvc. The registry checksum and unchanged dependency
requirements were checked against official metadata; only the version and
checksum change. Cargo has not resolved or compiled this source repair.
The 2026-10-06 dependency source audit found three unresolved dependencies in
the optional installable OpenCode service bundle: `sprintf-js`,
`http-cache-semantics`, and `braces`. Their current advisories publish no patched
version, so the pinned bundle remains unchanged and is not dependency-qualified.
This audit establishes package inclusion, not an exploit path. The nine Python
alerts concern development-only `datamodel-code-generator` in the retained
Codex SDK donor; the Rust adapter does not use that environment. Its fix belongs
to a deliberate donor refresh with updated upstream identity and checksums,
not an isolated edit that would invalidate the retained donor provenance.

The actual Kernel now lives in `crates/swarm-kernel-host`: it owns Store, the
database lock and authenticated IPC. The root library is a compatibility
facade, and public `swarm-host` launches the Kernel sibling. The independent
`swarm-supervisor` owns module lifecycle through authenticated IPC and has no
database access. Installation verifies the complete declared sibling chain.
CLI MCP commands use the separate installed `swarm-mcp` executable. The default
build recipe uses one invocation for these five packages and one external
shared target; it does not allocate checkout targets or worktrees.

OpenCode's standalone Rust artifact is
`eliot-opencode-v2.rust-http.1@0.3.0`, with native-options schema version `2`
and exact schema SHA-256
`7fc3136219b20d00570b65e5d4fe533e3ea042dadf53be3fdcdfa9781cf0eb68`.
Its source owns fresh native service launch, selected-provider authorization,
pinned plugin preparation, and the four typed native MCP methods
(`install`, `observe`, `arm`, `read`). Store queues their retained C8 phase
Operations through the exact internal caller; adapter intake validates the
common DTO and immutable parent/input linkage before effects. The service-ready
receipt is now consumed into the existing `owned_service_starts` reservation,
including actual process identity and bounded proof. Recording this fact reads
retained launch/actor/lease history and survives a later Manager handover or
release. Fresh launch projection remains reserved-only; subsequent commands
can derive native options from an exactly validated observed-service row.
The readiness proof keeps `dispatch_permitted=false` until the separate C8
phase requirements are satisfied.

The common normalized result contract is connected to Store and the Rust
OpenCode, Codex, Command, Claude and Antigravity adapters. Immutable page
origins, duplicate acknowledgement, page assembly and exact native identities
are preserved. Pinned OpenCode 2.0.7 exposes no immutable assistant-to-input
parent join: missing parent evidence records
`NATIVE_ASSISTANT_PARENT_UNAVAILABLE`, an `Unknown` adapter outcome and the
Manager's `OUTCOME_UNKNOWN` projection. Antigravity reports the deterministic
`RESULT_BODY_UNAVAILABLE` rejection. Neither path invents a body, response
parent or Task completion from message order, timestamps or latest-message
selection. Status/error readback remains available.

Universal event routing retains the technical executor, effective Manager and
immutable launch/module cause independently of current action authority.
Manager-configured selectors/actions use the existing event intake, cursor and
Operation path for hooks, submissions, replies, failures and other admitted
events. Scoped logging and read-only monitoring are connected in source;
selected metadata/redacted content policy is resolved before capture. Raw
native-frame capture remains unavailable.

Kernel writer failure now publishes a retained watch snapshot that wakes the
host even when no request is active. Shutdown joins the original writer before
recording its final lifecycle result through the existing Store. That sequential
recovery retains the same locked File, opens only the existing database and
does not replay work or advance the host epoch. Primary startup errors survive
cleanup with at most two distinct safe secondary codes. The lifecycle receipt
and terminal observation retain those codes without replacing the primary
failure. A refresh racing with writer exit cannot suppress the retained watch
notification, and initialization failures keep their original readiness/join
identity. Host shutdown consumes module and managed-bus supervisor join errors.
Readiness commits and watch publication share the existing status mutex, so
late readiness cannot overwrite an already published writer failure. Public
pre-readiness admission closure does not misclassify an initializer failure.

Standalone and managed-bus outer health persistence errors now propagate
through their existing result paths. Standalone emits a bounded safe secondary
diagnostic when a reconciliation failure already supplies the primary. The
structured async host reader drains stderr continuously and retains only the
bounded safe error projection, including distinct secondary codes. A later
plain terminal code cannot erase the same primary's structured diagnostics.
Per-slot managed-bus retries retain their existing child handles on Store
outages.

The public host and CLI wrappers drain a child's stderr independently of its
inherited output sink. A bounded queue and one post-exit deadline keep blocked
output from delaying return of the child's actual exit status. Safe structured
failure projection survives discarded raw chunks; envelope delivery remains
best effort and is not asserted when its acknowledgement is absent.

Unadmitted pending ScriptRun causes now rebuild or clear their successor
consumer context during automation transfer. The original producer cause,
observation and admitted Operation identities remain unchanged; normal current
authorization and source checks still control release.

Visible Operation diagnostics now use the existing authenticated Manager or
local Operator read scope. The retained native-MCP, workspace and Participant
issuance facts do not require live GM authority. Current action projections and
new effects keep their authority checks. The canonical Module API and recovery
documents describe this read boundary and the generic event contract.

Installed OpenCode resources now have a locked npm dependency closure, atomic
resource publication, exact package/build sidecar identity and a shared bounded
tree verifier used by Kernel and the standalone adapter. Source-tree resources
require an explicit debug opt-in. These are source deliveries; installation and
native launch remain unqualified.

Typed supervisor child identity, exit and stop receipts are integrated in
source. The authenticated `module.event` path accepts descriptor-backed closed
metadata and uses the existing observation/cursor transaction. Only eligible
Module facts wait for missing source proof; release recognizes the exact
`SYSTEM_EVENT_SOURCE_PROOF_PENDING` reason. Statusless events gain no invented
status or occurrence. The method is internal and has no MCP exposure.

The supervisor child lease now retains the exact child handle through health
write failures and coordinator unwinds. Before creating a process, the host
persists and reads back `MODULE_SUPERVISOR_SPAWN_PENDING` in existing worker
health. Restart preserves that intent and the child receipt; replacement needs
confirmed departure or matching PID, birth and image evidence. Missing evidence
remains a Manager-visible error. Rejected health writes do not become durable
receipts, and secondary health failures block a fresh replacement. The trusted
supervisor registration upgrades the prior nine-capability record to the current
ten-capability record with the internal retained-health reader. Detached reaping
depends on the current runtime and does not claim child departure after runtime
loss. These source changes precede compiler and failure qualification.

The native qualification script now validates the
public CLI, host wrapper, actual Kernel and supervisor coordinates separately,
records the actual Kernel PID, and closes only its own Kernel stdin for shutdown.
Claude's qualification entrypoint now uses its existing version-4 typed
host-config route and requires an explicit exact model ID for its single input.
Remaining acceptance work follows compiler source closure: a scoped compiler
gate, then core failure/recovery and adapter runs.
The selected model is `inclusionai/ling-3.1-flash`; Bunny is disabled. Local
models, Linux/WSL and Zed remain deferred. The active desktop Codex/OpenCodex
process and configuration are preserved; Claude qualification follows the other
routes because its quota is limited. Productive OpenCode body readback needs a
pinned native contract exposing the missing causal link. It is unavailable on
the current pinned projection, rather than an inferred completion.

The dated sections below retain source history and exact earlier receipts.
Their historical `src/**` anchors now reside under
`crates/swarm-kernel-host/src/**` after the physical relocation.

### Shared positive method policy — 2026-10-05

Store admission/classification and MCP discovery/profiles now consume the same
data-only `swarm-contracts::method_policy` registry. It covers the current 129
typed MCP methods and 22 additional Store/internal methods, including scoped
logging, monitoring, source capture and existing result handling. Profiles can
narrow this inventory; handler ownership, revision, assignment, independent
review and publication checks still decide the actual resource request.
This source change has not been compiled or tested.

### Historical event provenance and current script action scope — 2026-10-05

The shared event dispatcher now retains a Module observation's exact binding,
descriptor/event-contract digest, opening Operation and linked Task/Attempt
origin separately from the current action scope. Closing or revising a Task,
releasing an Attempt/binding, or disabling a descriptor does not erase the cause
of an already committed event. Task actions still require current authority;
valid historical events can use the existing taskless Manager notification path.
Manager-created event bodies remain private and bound to their existing owner,
project, name, key and payload digest. Source/kind matching continues to use the
existing generic selector and cursor, without a new event allowlist or ledger.
This source increment has not been compiled or tested.

### Retained source completion and scoped diagnostic controls — 2026-10-05

The source-capture completion path now validates the immutable admission saved
before Git/file I/O. It can retain the exact captured source snapshot after a
Task closes, an Attempt is released, or manager/lease ownership changes. Current
principal, assignment and held-lease checks still run before starting capture.
Recording that snapshot does not submit or accept the Task.

Ordinary registered Managers now have `logging.get` and `logging.set` for their
own client, retained Operation, binding/module, or exact current Task/Attempt
scope. The policy uses existing Store metadata and reloads the existing Producer
after commit. Metadata and redacted-text content, scope level and live expiry
are now connected to the Producer and observer. The observer carries bounded
Atlas-redacted supervisor diagnostic text through its existing queue and
recorder when the effective scope permits it.
Raw native-frame capture remains unavailable.

The independent `swarm-supervisor` binary and strict private-stdin bootstrap are
implemented over authenticated kernel IPC, including retained hello identity and
scoped health readback. Host now starts that executable through the bounded
private handoff; runtime process isolation remains unqualified.

These are source increments. Per owner direction, required code is completed
before compiler, test, build, or model execution. Current native qualification
and complete-product acceptance remain pending.

### Supervisor control and publication demand — 2026-10-05

Source `3755bc1` exposes the supervisor's existing admission, demand, binding,
credential, recovery, observation and health operations through authenticated
kernel IPC. `swarm-supervisor` uses a typed client and receives no Store or
database handle. The existing exact supervisor credential is migrated at
initialization; runtime calls require its complete bounded capability list.
The lifecycle actor and its configuration now run in the standalone supervisor
child; Host retains the process handoff and committed failure handling.

Automatic reconciliation prepares Forge executable images only when its bounded
publication page contains due work. A publication that becomes due after this
read-only preflight remains retryable pending until preparation is available;
it cannot create a new unpinned publication intent. Existing immutable executor
pins and historical legacy readback remain intact.

These are source changes. Current compiler, test and native qualification gates
are deferred until the required implementation is complete.

Participant discovery now includes `source.capture`. Its current assignment
must own the exact held launch workspace for that Task, revision and Attempt.
Store checks the retained lease and launch authority without filesystem I/O;
the existing file-I/O lane resolves the repository before reading Git. Ordinary
and extended Windows disk/UNC paths share that lexical scope. Manager and
Operator capture retain their existing general path. Collection of an in-flight
capture after assignment or lease release validates its saved admission.

`monitor.snapshot` and `monitor.follow` use the existing status reader and
visibility-filtered Store projections. The snapshot captures a committed
journal cut in the same read transaction as its state; subsequent pages resume
from that cut and report visibility, lag and gaps. Ordinary registered Managers
use their stable client identity. The public CLI now forwards `swarm monitor
snapshot/follow` through authenticated IPC; MCP discovery exposes both methods.
Native metrics without retained evidence remain unavailable.

Claude result admission now seals the exact dispatch, immutable Attempt,
descriptor, SDK input and payload identity. Page collection and assembly use
that saved origin after binding release or descriptor changes. The authenticated
Module carrier is checked against the result's binding rather than the Manager
who admitted its Operation. Current submission and acceptance authority remain
separate. This implements the Store path in source; it has no current SDK or
provider qualification.

### Kernel, independent frontends and managed services — 2026-10-05

Store now uses the actual bounded `KernelHost` queue and one writer thread for
database initialization and callbacks. Authenticated `host.status` exposes its
admission, lifecycle and fault snapshot through the existing read-only reader.
Shutdown wakes the same writer without polling and closes its queue even when
client facades retain sender handles. The status reader receives a Stop job
through its existing queue; late facade submissions return `STORE_CLOSED`.
Task specification, source-index, revision, claim and release validation now use
`swarm_kernel::tasks`. Structured Review results, requirement coverage and
disposition validation now use `swarm_kernel::reviews`; Store retains
authorization, SQLite and receipts. `swarm_kernel::acceptance` now validates
serialized policy and deterministic candidate/check/review facts at the existing
Store boundaries. Store retains current GM/epoch and file-evidence checks. The
independent kernel executable remains unfinished.

`swarm-mcp`, `swarm-cli` and `swarm-gateway` are real independent Cargo packages.
Their resolved dependency closures contain no controller, adapter, Store, kernel
or SQLite dependency. MCP integration fixtures use the extracted implementation.
The CLI supports authenticated status, generic calls, Task operations and agent
readback; HookSource retains its existing durable-ack retry contract. The public
`swarm` executable now belongs to `swarm-cli`; local runtime/admin commands use
the separate `swarm-host` sibling. Ordinary requests do not start it. Gateway
remains optional and disabled by default; launcher failures retain their exit
status and installation guidance.
The public CLI reports host launch failures separately from a missing sibling
and records unsuccessful child exits. Windows exception exit codes remain
failures rather than being clamped to a successful zero exit.

Frontend tooling records each artifact's own source, dependency and image pins.
Sibling compatibility checks the actual IPC protocol, target triple and launch
arguments rather than requiring unrelated artifacts to share a repository SHA.
Offline workspace resolution preserved all 240 existing registry checksum pins.
No frontend installation or native invocation has qualified this source yet.

The managed bus has explicit opt-in and current durable demand, a dedicated
Module identity, a positive Store generation and protected configuration/image
bindings. Store outages retain the existing child handle; unknown ownership
holds the scope without replacement. Restart adoption now verifies the retained
configuration, credential, image, OS birth identity and service-group membership
before resuming readback. Replacement requires committed whole-family departure;
host shutdown leaves an admitted live family available for adoption. Legacy
receipts without these proofs remain held. These source changes do not start the
bus and have no current native qualification.
Manager attention now includes recorded host, optional-worker and managed-bus
failures. Stale module readback and startup/ownership errors retain bounded typed
stages through the existing observations. Attention does not authorize replay or
claim departure. Descriptor-selected observations compare the retained module
selector and artifact rather than equating an opaque module ID with a harness
label.
Optional-worker health also retains the last bounded failure and restart count
after the actor returns to Running. A new host demotes stale readiness while
preserving failure history and the original timing of a valid degraded row.
Manager attention exposes this history without granting retry authority.

Command version 3 provides exact durable status/failure readback. It continues to
report `execution_complete=false`, `native_response_identity=unavailable` and
`task_completion=unknown`; status alone does not qualify an assistant result.
Manager admission captures the selected status page in the existing result
Operation. Delivery and acknowledgement reuse these bytes even if the original
dispatch is later reconciled. A sealed `outcome_unknown` page reports the saved
state without promoting it to completion or authorizing replay.
The adapter also exposes bounded `command_output` pages for retained stdout
and stderr captures. Store seals the stream and process facts at admission,
verifies page receipts against that snapshot, and permits complete untruncated
output to become an Attempt-bound candidate. Empty streams are valid captures.
Missing durable capture facts remain unavailable; output retrieval does not
invent an assistant response identity or establish Task acceptance.
Module diagnostics correlate the exact event, module, artifact, build and boot.
Task/Attempt attribution requires the retained transactional binding. Manager
attention and observer timeline gaps preserve bounded typed metadata without raw
payloads; telemetry schema versions 1 and 2 remain supported.

The optional recorder now applies an Info baseline and bounded live overrides
for an exact module, client or Operation, including Off. Operation overrides
take precedence over client and module overrides. Absolute expiration survives
restart, and invalid updates retain the last valid policy. The existing lazy
writer and reload loop apply filters without restarting producers. File schema
1 remains readable; schema 2 adds scoped policy without changing diagnostic
record schemas 1, 2 or 3. Recording currently supports metadata only. Manager
logging controls and actual redacted content producers are still in progress.

The Claude version-4 standalone Rust controller source is integrated with a
provider-neutral retained `pre_input_open` contract and exact first-dispatch
identity adoption. Rust owns IPC and durable operation receipts; the pinned
Node SDK shim owns its process-local query objects. The supervisor materializes
the module IPC connection file in the exact launch state directory through the
typed argv marker. Command version 3 uses the same host connection mechanism.
`agent.refresh` reads a bounded cache for the exact SDK session. The SDK bridge
now captures bounded task and subagent lifecycle metadata with exact parent links
when the SDK supplies them. Cache and page limits report truncation. Claude
remains disabled and reports partial family completeness. The adapter retains a
bounded terminal SDK result body, correlates its actual input UUID and submitted
payload digest to the retained dispatch, and exposes exact result pages through
`agent.result`. Store capture and Participant candidate provenance now validate
the retained native admission and result pages. An SDK invocation result does not complete the Task or prove process
family departure. The source descriptor and disabled
route identify `claude-agent-sdk-0.3.287-rust-controller.4` (version 4).
This status does not assert an installed image, selected version-4 route, or
native execution receipt; those still require separate evidence. The current
native qualification entrypoint also blocks Claude as unsupported until its
version-4 route and receipt contract is integrated.

The source also records a bounded cancellation-control diagnostic in the existing
ScriptRun Operation result. It binds the private receipt to the exact run,
worker token and process identity and preserves the safe projection through
unknown, incomplete and completed readback. It does not prove family departure.

Manager-owned system-event ScriptRuns can now apply `manager_notification` and
`task_create` without a synthetic Task. Both effects use the existing Store
transactions and captured project/owner authority. Task creation retains its
child Operation and deterministic identity; effect history validates the exact
effect kind and preserves cancellation diagnostics.

The standalone `swarm-automation-worker` source is integrated behind an explicit
default-off flag and current due/future demand. It uses one shared wait loop and
the existing Store reconcilers for interval schedules, calendar actions, Goal
reminders and check readback. Registration retains the canonical private-config
digest, exact service generation and process owner. The host records readiness
only after the worker enters its service group. These handlers retain their own
transactions; the shared due page does not create a cross-source transaction.
The package builder and adjacent, create-only installer now cover the standalone
automation worker, retaining its own build and image provenance.

CheckRun admission retains the selected standalone executor in the existing
immutable run specification. The worker reconstructs that selection after a
host restart instead of switching queued work to the current configuration.
Forge publication now captures Git and worker image pins through the file-I/O
lane before admission, and stores them in the existing immutable Operation.
Execution rechecks those images; recovery can validate a departed worker's
saved identity even when its installed image was removed or replaced. Direct
and automatic publication keep the existing Store transactions and journals.
Historical rows without an executor retain their legacy route. Supervisor
descriptor replacements inherit the same per-service demand gate; slow
readiness and ownership checks no longer hold the registry-wide mutex.
The bus installer follows the existing adjacent, create-only worker contract.
Scoped CI and explicit release builds use validated external Cargo targets;
only the preserved Atlas snapshot bypasses Cargo package seed resolution.

An authenticated Manager can publish `event.emit` with bounded immutable payload,
project, cause and deduplication identity. The existing Observation/Operation
transaction records one event; other Managers cannot read its raw private body.
Configured ScriptRun rules use the common bus and safe metadata projection.

Codex, OpenCode, Command, Antigravity and Claude now implement the descriptor's
normalized Task dispatch admission pair. Each records the exact submitted native
payload digest and bytes, echoes the retained module/Task/Attempt context, and
preserves unresolved outcomes without replay. Input admission alone does not
establish assistant completion or a deliverable candidate.

An ordinary Participant can submit the candidate for its exact current Task
revision and Attempt. Scoped Operation and artifact reads expose only its own
source snapshot or retained native candidate origin. Sponsored reviewers keep
their separate review scope. Submission does not grant Task acceptance or
native runtime control; collection of already-published bytes survives handover.
Participant context reads now project the retained runtime MCP capability
receipt with bounded launch ancestry. Missing or incomplete capability evidence
is reported as a gap and does not become dispatch permission. Sponsored
reviewers retain their separate scope.

Per owner direction, remaining code is completed before test and model runs.
This source batch has not passed a current compiler gate or native qualification. CI
[37374690883](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37374690883)
for `6e7c2a6` passed formatting, documentation and tooling checks, then failed
strict Clippy on two needless returns. Both are corrected in this source batch;
that failed run did not execute integration tests.

New OpenCode and Command Code qualification runs select
`inclusionai/ling-3.1-flash`. Bunny is disabled for new runs. Provider/model
availability must be verified for each harness before invocation; historical
Bunny receipts remain dated evidence of their original runs.

### PR #25 controller effects and exact failure readback — 2026-10-05

An automatic ScriptRun can apply the existing `task_owner_message` effect through
the ordinary Store `message.send` transaction. Store derives a private admission
from the retained run, enabled bundle grant, effective Manager and exact source
Task/Attempt; the worker does not receive Manager credentials. The child Operation,
mailbox delivery, causal attribution and parent completion commit together.
Repeated completion readback returns the retained receipt without another effect.
Admission still checks current scope; retained receipt visibility survives Task
completion, Attempt release, later revisions and entry disable.
Child-operation visibility now checks the exact retained invocation cause rather
than generic Task/Attempt columns that `message.send` does not populate.

Ordinary Managers now manage their own script catalogs without current-GM
designation. Ownership is checked for validation, activation, direct runs,
artifact access and retained-operation replay; listing filters before pagination.
Manager MCP discovery advertises these existing script methods. Cross-owner
access still requires the existing GM/Operator authority, and direct Task-bound
runs still check the exact active Attempt owner. This is an implemented source
policy; its new fixtures have not been executed in this batch.

ScriptRun recovery now requires an exact retained process identity and whole-family
departure before terminal settlement or effect application. Missing/corrupt work
files use the durable identity. Pre-Go settlement also requires absence of Go,
interpreter-start and plan markers. A live or unproven family remains visibly
unknown and holds the affected run; recovery does not replay an effect.
The worker error path repeats cancellation scans within its own process group
until emptiness is proven, matching the normal drain path before settlement.

Module observations now create `module.ready`, `module.start_failed` and
`module.family_exited` facts in the same Store transaction. Family exit requires
the same retained boot, a prior ready observation and the supervisor's typed
exact-worker/whole-family departure proof. A plain exited state or IPC disconnect
cannot create that fact. Automation intake and bus admission validate the source
receipt and retained binding before advancing the cursor.
The same transactional source also records nonterminal `module.recovery_blocked`,
`module.identity_unknown` and `module.owner_retained` facts with a bounded error
class. They do not imply family departure or Task completion.

Codex adapter descriptor version 3 preserves the requested provider/model and
reports finite attach/credential/RPC failures. After a durable send marker it
uses exact user-item and linked-turn readback, including bounded turn pagination.
Failed/interrupted turns expose a bounded failure class; missing or uncertain
readback stays unknown without sending again. This attaches to the existing
app-server and adds no API-key fallback or process restart.

OpenCode input-status readback now follows the locked `@opencode/server` 2.0.7
public projection, which omits `sessionID`. The exact session-scoped GET and
retained input marker/digest remain mandatory; a present conflicting session
is rejected. HTTP failures expose only a closed error-tag class and status,
with at most 8 KiB examined and no response text retained. The public assistant
projection has no input-parent identity, so asynchronous assistant/provider
completion is still unqualified; timeline ordering is not treated as proof.

The changed production libraries and binaries passed scoped Clippy with warnings
denied for ten packages on 2026-10-05. The later four-file shared-cache change
also passed the root library/binary gate; the other nine packages were unchanged.
Native qualification and the new test fixtures remain pending. CI
[37363042453](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37363042453)
for `7917956` stopped on the four scripts import/type errors corrected below;
its integration tests did not run. It does not qualify this newer source.

### PR #25 compiler closure and core fault harness — 2026-10-05

The first scoped local Clippy pass on clean `7917956` reached newly extracted
packages and stopped on import/type errors in scripts, observer, supervisor and
Antigravity. The source fixes missing public event exports and macro imports,
descriptor ownership, metrics error conversion, Antigravity receipt borrowing
and bounded page types. The supervisor cleanup callback now has an explicit
boxed `Send` boundary for its recursive lifecycle replacement. No native work
or permission behavior changes in these compiler corrections. The corrected
source passed the scoped production compiler gate.

Further scoped compiler passes corrected production root import, JSON type and
ownership errors and strict warnings, including argument grouping, row type
aliases and private-interface alignment. The corrected ten-package production
Clippy pass completed successfully with warnings denied. Codex and Antigravity
warning fixes are integrated. No full workspace build, native model call or test
execution has been performed for this candidate.

Cargo JSON check profiles can now explicitly configure an existing absolute
`CARGO_TARGET_DIR`; both check executors use the same guarded resolver. The path
must contain no symlink or Windows reparse point. Ambient inherited target paths
remain rejected. The configured value participates in the existing hashed input
identity. Profiles using one cache must select the same resource lease identity.
Without this explicit configuration, the existing per-resource target remains.
See [CheckRunner configuration](check-runner.md#resource-lease).

The shared contracts also define closed `BusConsumer` and
`AutomationScheduler` service purposes with an exact service ID and positive
generation. Their process-owner primitive preserves non-killing ownership and
checks the declared purpose during departure readback. The managed bus and
scheduler Store/host integration remains a separate private candidate; adding
this ABI does not start either service or grant application authority.

`tools/qualification/Invoke-CoreFailureQualification.ps1` provides source for
fresh private-host lost-caller-ACK, durable request-conflict and optional
HookSource deduplication cases. A separate Manager must prove the exact admitted
Task/Operation before graceful host restart and read the same IDs afterward.
A lost caller ACK is not presented as a native-effect `outcome_unknown`.
The harness has not been executed and does not prove optional-worker crash or
uncertain external-effect recovery. It requires separately pinned `swarm-host`
and public `swarm` CLI images; the host is started directly and the public CLI
performs ordinary RPC and readback. No tests or model calls ran in this batch.

### PR #25 durable bus wiring and launch/install closure — 2026-10-05

Authenticated Store and Manager MCP routes now expose bounded bus readback and
admission. A Module consumer has only its enrolled page/admit rights; its
credential is not a Manager bearer. The existing O1 scanner remains active and
shares the cursor and pending-intent journal with the dispatcher. Admission
rereads source scope and records the cursor/action transition in one transaction.
The typed ScriptRun continuation revalidates its owning Manager, enabled entry
and source Task/Attempt. Ordinary owning-Manager bus operations do not require
current-GM authority. Managed dispatcher liveness and automatic controller
effects remain in progress.

Ten legacy host reconcilers now run under one demand coordinator. Disabled,
dormant workers do not start. Exact built-in OpenCode selection excludes the
standalone module route. Top-level worker failures and panics receive bounded
retry/isolation status through `host.status.optional_workers`; Store/kernel
failure remains fatal. Shutdown signals workers and awaits active work.
ScriptRun start and queued cancellation now create exact transactional facts
with Run/Operation/Task scope and cancellation-alias deduplication.

An owning Manager can select its own configured MCP profile for a native launch
with an explicitly valid Participant surface. The issuer creates a fresh scoped
Participant credential; it does not copy Manager groups or manual tools. Existing
Participant and AssignedReviewer selection remains supported.

Supervisor diagnostics now emit only after a new durable observation commits.
Retries emit nothing. Correlation includes the retained binding generation;
diagnostic schema 2 preserves observer read compatibility for schema 1. Uncertain
ownership does not become a proven stop. Diagnostics remain separate from the
transactional event journal and Task authority.

The enabled observer can publish an exact private host-image receipt while the
Store holds its DataRoot lock. Receipt failure is nonfatal. Metrics can use this
receipt by default; shutdown removes only the matching receipt before Store
close. The adapter installer now validates the optional descriptor workspace
contract, fixing rejection of the new templates. Script worker installation and
manual host source-provenance packaging are available through the existing
shared target/build-manifest path; neither installer changes configuration.
The manual workflow exposes eleven actual package/bin pairs, including the
auto-discovered `swarm-checks` binary, and builds one selected package only.
Local Cargo recipes and scoped/full verification now require an explicit shared
target path. The manual full workflow uses one external runner target for both
Clippy and tests, preventing checkout-local build-cache duplication.

Antigravity descriptor version 3 adds bounded `agent.result` readback for the
exact retained target Operation. Store verifies the target digest, binding,
generation, session and result-page bytes. The native CLI does not provide a
proven assistant response identity; this page exposes status/failure only and
keeps execution and Task completion unknown. Existing installed versions remain
retained. The source adds no Cargo dependency and still needs native qualification.

Forge and GitHub PR recovery can perform exact remote readback after validating
the retained plan and proving whole-family departure even if the private worker
result is absent or malformed. Wrong-scope evidence blocks readback with a
durable reason. Unknown native writes are never replayed. The source-only
`tools/qualification/New-NativeQualification.ps1` harness verifies explicit
host, public CLI, and module image/manifest pins and trusted descriptors before
a fresh Manager workflow. The independently pinned public CLI communicates with
the directly started host. For OpenCode, its `agent.open` path checks the current
native model catalog against the exact route provider/model/variant before
creating a session; qualification still requires a successful native run and
Manager readback. The harness source does not establish a successful
qualification run.

CI [37356657538](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37356657538)
for `410c327` passed formatting, documentation and tooling checks, then stopped
on one strict Clippy `map_or` diagnostic in `swarm-bus`. This update fixes that
diagnostic and adds the source wiring above. Its compiler and native acceptance
remain pending; no local full build, provider call or Codex restart was performed.

### PR #25 Forge, ScriptRun and explicit native readback — 2026-10-05

Forge Git/GitHub effects now use a separate one-shot `swarm-forge-worker`
process. Store admission and the durable pre-effect authorization still own
the write boundary. The worker enters the retained process group, reports its
exact identity and waits for authorization. Uncertain native effects are not
resent. A create-only installer places the validated worker beside an explicitly
selected host executable.

The workspace includes shared `swarm-bus` and `swarm-scripts` contracts and a
standalone `swarm-script-worker`. An optional `[scripts.executor]` image pin is
retained in each new ScriptRun receipt. The worker waits for ACK/Go and verifies
the plan materialized after Go; an invalid selected image cannot fall back to
the legacy worker. Durable bus enrollment and host wiring remain in progress.

Selected adapter descriptors declare a bounded JSON Pointer for the admitted
workspace path. Old selected descriptors can use their explicit configured
route field. New descriptor versions are Codex 2, Command 2, Antigravity 2 and
OpenCode 0.2.0; retained installed coordinates keep their existing identities.

OpenCode `agent.result` supports exact `input_status` readback for a retained
Operation and native input identity. Its durable result page reports only
`native_input_admitted`, `execution_complete:false` and unknown Task completion.
The native source does not provide a proven assistant identity linked to that
input. C8 provider/context metadata is projected into bounded schemas and the
immutable Task packet; model consumption remains unverified.

The explicit observer metrics command samples exact Windows process receipts
after identity and process-group checks. It does not enumerate processes or
change process state. Automatic host receipt production is still in progress.

Combined-source CI [37353200179](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37353200179)
failed on three observer strict Clippy diagnostics. Those diagnostics are fixed
in this source update. Formatting and dependency resolution passed locally with
all 240 prior registry version/checksum pins preserved; the new compiler gate,
full core failure checks and current-source native qualification remain pending.
No local build, model call or current Codex restart was performed for this update.

### PR #25 adapter processes, supervisor and diagnostics — 2026-10-05

The workspace now contains independent production binaries for Codex,
OpenCode, Command Code and Antigravity, plus the `swarm-checks` executor.
Their shared protocol carries exact Operation, binding, generation, artifact
and receipt identities. Selected-module admission rejects a command missing
from the retained descriptor before queuing a native effect; reconcile checks
the original command and delivery mode. Existing unselected bindings retain
their legacy path.

The optional `swarm-supervisor` actor connects durable Store demand to the
standalone `swarm-module-owner` process. Startup is single-flight per retained
module scope. An adapter becomes running after Store accepts its typed hello,
not merely after process creation. Host restart reads retained work and scoped
process evidence before reopening starts; it does not resend unknown input.
Scoped failure observations reach `agent.state`, `report.delta` and Manager
attention. Store failures keep recovery closed rather than acknowledge an
uncommitted transition. This actor is outside the required host worker set;
the remaining legacy optional worker extraction is still unfinished.

The checks executable uses the existing CheckRun admission, process group and
ACK/Go barrier. An explicitly configured executable must match its pinned image;
artifact labels are retained configuration metadata. An invalid selected executor
cannot fall back to another worker.
Plans resolve only after Store Go, and pending output readers retain the run.

`swarm-observer` adds lazy, bounded diagnostic recording and the CLI commands
`observer snapshot` and `observer follow`. The recorder is optional and starts
only on the first emitted record. Invalid recorder bounds disable that recorder
with a named diagnostic while the Store retains its ordinary telemetry producer.
Recorder saturation and shutdown expose loss and pending counters. These files
are diagnostic metadata, not transactional receipts or Task authority.
An optional scoped JSON file can reload severity/category filters and retention
settings on the existing lazy writer. Valid versions replace the whole snapshot;
invalid updates retain prior settings and report bounded failure counters.

Manual package build and create-only module installation tools are under
`tools/ci` and `tools/modules`. They preserve immutable artifacts and publish
the installed descriptor last. The package workflow builds one selected
executable package only when explicitly dispatched; normal source pushes run
the existing scoped compiler checks. Library-only packages are not installable
binary artifacts.

This entry records integrated source, not live deployment. The preceding
`d36358cb7f4bcbb502572328923c4abd500d35a4` passed scoped formatting and strict
production Clippy on Windows and remote Ubuntu in
[run 37344640764](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37344640764).
The new combined source requires its own compiler gate. Native models,
process-failure injection, independent installation and full qualification
remain unverified. No owner-machine build, model call or Codex restart was
performed for this integration.

### PR #25 scoped child environment — 2026-10-05

The module-owner wrapper clears the adapter child's inherited environment.
Windows carries an explicit OS/home/config baseline, then the validated plan's
literals and protected file references. Arbitrary ambient provider credentials
are excluded. This changes future child launches only; operator services and the
running Codex process retain their environment. Independent source review passes;
live launch and provider behavior remain unqualified.

### PR #25 adapter host contracts and capture diagnostics — 2026-10-05

The host validates native options and forwards its admitted workspace for the
separately packaged Codex, OpenCode, Command and Antigravity `.1` artifacts.
Their configured runtime label remains separate from the trusted descriptor's
opaque module ID; selection still checks the enabled route and exact artifact.
A cold hello can omit both native identity fields and receive Store's retained
pair. It cannot create or replace that identity.

Checks now keep polling when their owned process group is empty but output
readers still await EOF. A bounded `CHECK_OUTPUT_DRAIN_PENDING` diagnostic reaches
authorized `check.get`; pending readers cannot produce completion or resource
release. The wait neither signals another process nor adds an execution timeout.
The shared/host error boundary preserves error code, message and rejection class.

Frozen source review and scoped formatting pass. The previous compiler run
reported seven root type/import errors; this source corrects that complete seam.
The corrected compiler gate remains unverified at publication. Standalone binary
integration, supervisor activation and actual provider qualification remain open.
No local compilation, test, native or model call was performed.

### PR #25 trusted module handshake and shared checks executor — 2026-10-05

The Store now registers immutable module descriptors through a reserved local
supervisor credential whose only method is `module.descriptor.register`.
Manager catalog reads redact launch details. Ordinary Managers and the Operator
can select an exact registered version for their own future bindings through
`module.route.select`; selection uses stable caller identity and catalog revision,
without a current-GM prerequisite. Existing bindings retain their exact selector.
Typed hello compares the claim with that retained descriptor before and after
OS-owner preflight. Descriptor capabilities grant no Store or native-effect rights.

Shared runtime commands carry the canonical retained Operation request digest.
Every outcome for a versioned binding must carry its own matching typed receipt,
including a reconcile target's separate Operation digest. Legacy bindings retain
their explicit unverified handshake and previous receipt behavior. Root still
owns admission and domain handlers; standalone adapter registration and
activation remain separate integration work.

Registered bindings now expose readback for the exact unresolved Operation when
their retained descriptor supports reconcile and that operation kind. Admission
and fresh target receipts verify the binding, generation and admitted reconcile
request; reconciliation stays readback-only. Each outcome has its own receipt,
and an already accepted terminal receipt can be retransmitted unchanged.

The existing CheckRun worker now consumes `swarm-checks`. It publishes its exact
process owner and waits for Store's Go before resolving source inputs or launching
the native command. Unknown process-group observation retains the resource;
sanitized drain/control diagnostics reach authorized `check.get` readback. The
current worker process remains host-owned; an independent checks executable is
unfinished. Frozen source hashes, scoped formatting and metadata shape pass.
The combined production compiler gate and live behavior remain unverified at
this publication. No local build, test or native/model call was performed.

### PR #25 scoped module-owner launcher — 2026-10-05

`swarm-process` now provides the independently built `swarm-module-owner`
bootstrap and typed adapter membership proof. A dedicated wrapper holds each
module scope's nonkilling process group and lock, preserves the v1 owner record,
publishes the distinct adapter image/boot/artifact receipt and waits for the
entire native family. Explicit private plans use opaque protected references
resolved to existing credential-file paths. An independent source review found
ambient module credentials could be inherited; the wrapper now clears that
namespace before supplying the exact selected scope.

Before-spawn resolver/validation failures publish a scoped negative launch
receipt after an empty-family check. Replacement still requires exact wrapper
departure; absent or post-spawn proof remains unknown and cannot replay input.
Source mapping and explicit formatting pass. Source `cd88746` passed scoped
formatting and strict production Clippy on Windows and remote Ubuntu in
[run 37326559091](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37326559091).
The gate did not run full tests or produce a release binary.
Installation, live process ownership, dynamic registration and actual
supervisor/adapter wiring remain unfinished. The current Codex process and
installed launcher have not been replaced or restarted.

### PR #25 kernel writer actor — 2026-10-05

The root Store now uses the independent `swarm-kernel` package for its single
bounded FIFO queue and `swarm-store` writer thread. The existing DataRoot lock,
initializer readiness/error propagation, contiguous message batching and pending
job ordering are preserved. Root still owns domain transactions, per-request
responses, status-reader startup and close/join handling. This is the actor
boundary of M2; kernel domain handlers and the process split remain unfinished.

Source review, explicit formatting and offline locked workspace metadata are
the current checks. A missing function-signature delimiter in the draft was
corrected before publication. CI for `8e056a9` compiled the shared actor but
reported an ambiguous root callback type on both platforms; the callback now
names its existing `RunJob` type explicitly. Corrected source `025282a` passed
the scoped format and strict production Clippy gate on Windows and remote Ubuntu
in [run 37322606976](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37322606976).
Full tests, release and native qualification remain separate. No local
compilation, tests, model calls or native process launches were performed.

### PR #25 Store primitives and module child identity — 2026-10-05

The root now consumes `swarm-store` for SQLite opening, schema identity/digest
validation and transaction primitives. The existing writer thread retains the
DataRoot lock; base initialization and all kernel bootstrap, extension and
restart updates remain in one Immediate transaction. Root initializer errors,
parsed JSON digest semantics and accepted empty-database identity cases are
preserved. Status reads remain read-only/query-only with the existing principal
policy. At this checkpoint, Store job ordering and MessageSend batching were
host-owned; their later actor extraction is recorded above. Domain admission
remains host-owned. This is a staged storage extraction, not the complete kernel split.

Shared contracts now include a data-only module catalog: exact artifact
selection, protocol/capability compatibility, protected launch references and
unknown-launch readback decisions. Metadata inspection starts nothing. The live
Store handshake still checks artifact identity only; descriptor registration,
full handshake and supervisor activation are separate integration work.

`swarm-process` adds a read-only check for a distinct native child's exact
identity and membership in its recorded live, nonkilling module owner. It does
not authorize adoption, launch or input replay. A direct child exit and this
membership observation do not prove whole-family departure.

The preceding shared-package correction `43a9b78` passed strict production
Clippy on remote Ubuntu in [run 37314690538](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37314690538).
Windows reported an unused Linux-only helper; that helper is now compiled only
for Linux. Store extraction also left two production imports unused; their
scope is corrected. Current source `042d8b6` passed the scoped production
compiler/Clippy gate on Windows and remote Ubuntu in
[run 37318364299](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37318364299).
Metadata, formatting and source review also pass; native qualification remains
pending. This gate did not run full tests or produce a release binary.
M2 kernel extraction and M3–M6 remain open. No owner-machine Linux/WSL or local
native/model execution was performed for this increment.

### PR #25 shared packages and initial structured diagnostics — 2026-10-05

The root now consumes four independent Cargo packages: `swarm-contracts`,
`swarm-client`, `swarm-process` and `swarm-telemetry`. Contracts contain the
existing transport/runtime DTOs and shared error classification, without
database, process or provider dependencies. The client owns the existing
authenticated bounded IPC exchange. Process ownership and private-file
primitives have moved to their package; root compatibility facades preserve
host error types. Module groups reject cancellation; check/script cancellation
and serialized process receipts retain their existing behavior. The two existing
transport-poisoning tests moved with their implementation; handshake/server
tests remain in the host.

A failed disconnect transaction emits a bounded metadata record with the known
client/link and fixed failure category before returning its original error.
The producer starts no worker at construction and uses a bounded nonblocking
queue; stderr I/O runs on its lazy writer thread. Recorder failure/drop counters
remain local diagnostics. This initial producer does not implement logging
reload, recorder rotation, metrics RPCs or a live observer.

Workspace metadata and lock resolution pass without compilation, and the lock
adds only the four local packages. Explicit package formatting and source review
were the publication gate. Later scoped compilation and its correction are
recorded above; native interoperability remains unqualified. Kernel, generic
supervision, the full bus/adapter split and M5/M6 remain open.
See [shared package boundaries](agent-operations/shared-packages.md).

### PR #25 M1 source: Manager planning and disconnect persistence — 2026-10-05

The modular-runtime specification is merged in `5a35389`. Its first source
slice admits authenticated Manager and Operator identities to `task.create`
and `task.revise`. Revision keeps the existing Task compare-and-swap and
validates the current unreleased Attempt. An ordinary foreign Manager is
denied; its owner or the explicit current-GM/Operator path may proceed without
transferring Attempt ownership or changing its frozen snapshot. Native readiness
does not gate planning.

`Store::disconnected` now returns its persistence error instead of coercing it
to an unchanged result. IPC drains admitted requests and its response writer,
then propagates that error to the connection owner. The committed-change wake,
exact old-link protection and conservative unknown outcome remain intact.

CLI read classification now uses the canonical application registry, fixing
`report.capacity` and `report.attention` and avoiding a separate read-method
list. Source review covers this classification and the Manager policy. Published
`4057404` passed complete Windows and remote Ubuntu CI in
[run 37307959079](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37307959079).
No local tests/builds were run for that increment; hosted native qualification
remains pending. The later package/telemetry extraction above has its own
verification boundary. Full independent packages, optional-worker isolation,
configurable live monitoring and the complete hosted workflow remain unfinished.

### Module disconnect and rootless-open readback — 2026-10-05

The Store implements a durable transition: when the exact module binding
disconnects, move its in-flight
`sending`/`native_accepted` Operations to `outcome_unknown` in the disconnect
transaction, reusing migration `010`'s existing event and bounded
current-manager Operation readback. For an unresolved rootless `agent.open`,
permit only exact-route `agent.reconcile` tied to that Operation and binding
generation at both admission and dispatch. This does not replay input, infer a
result or cause, or claim owner departure; the existing replacement-boot and
verified-departure path remains authoritative for that claim.

The production source and documentation are published in `2aec51bb` and passed
independent source review and complete Windows and remote Ubuntu CI in
[run 37298710850](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37298710850).
No local compilation or test run was requested for this increment. Its Windows
release executable was retrieved from that exact CI run without rebuilding or
replacing the installed launcher; native model/workflow qualification remains
unestablished.


### Universal cancellation events and adapter diagnostics — 2026-10-05

New Operation cancellations enter the shared durable event bus in the same
transaction as the target state change. Migration `011` identifies the
cancelled target and emits a bounded `operation.cancelled` occurrence for the
ordinary manager-configured ScriptRun path. Historical cancellations are not
backfilled; retries and reconciliation retain one occurrence. Taskless actions
use this path without creating a Task or Attempt.

OpenCode now retains bounded terminal stage and native failure code in exact
root and child execution evidence. Command Code preserves validated native
exit categories in manager-readable Operation details. Antigravity handles
asynchronous stdin failure, settles pending sends once as uncertain and blocks
future writes to the failed stream. Independent Luna production source audits
passed. Gate `a9b2cb4e-9c53-4b02-99db-8f384756661d` passed all three affected
checks: actual Store cancellation through ScriptRun, root/child OpenCode
failure projections and exact child producer evidence. Formatting, strict
production Clippy, compilation of 331 library checks and the debug build
passed; all 211 source pins remained unchanged. Offline gate
`d1481ed4-e0d6-44ad-8c63-d0e082839c17` passed Command Code glue/bridge fixtures
and both changed adapters' syntax, including native auth-error diagnostic
readback. Antigravity's codec self-test also passed; it does not exercise the
new stdin-error path. Published `db69c178` passed complete Windows and remote
Ubuntu CI in
[run 37293756291](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37293756291).
Full hosted native workflow qualification remains pending.

Published baseline `821251f` passed complete Windows and remote Ubuntu CI in
[run 37284559675](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37284559675).
C35 run `a030e80a-eb0b-4b6c-ac9e-c295ec1f1def` passed at that exact baseline:
nine native MCP tools, challenge and manager readback were observed; restart
did not respawn the service, and cleanup proved its departure. This was native
inventory/recovery qualification and sent no model request.

C24 v5 stopped before host readiness because its model configuration omitted
the required variant. C24 v6 corrected that field and started the owned native
service, then stopped with `NATIVE_MCP_INVENTORY_READBACK_INVALID`. Retained
Store readback contains zero `task.dispatch` and zero `agent.send` commands.
Its retained native inventory has nine tools and valid sequence zero with both
hook statuses unknown. The private scenario incorrectly required a positive
sequence; the maintained native proof producer and Rust reader already accept
the correct hook-derived sequence. This diagnosis does not establish inference.
Both attempts and their execution namespaces remain consumed and retained.
Hosted Bunny inference and the complete O7 work/review workflow remain
unqualified. The installed launcher and current Codex/OpenCodex were preserved;
local Linux/WSL and all local models remain deferred. **PARTIAL_PROGRESS**.

### Hook lifecycle events and closed workspace admission — 2026-10-05

Committed `controller:hook-source` / `hook.source.setup` and
`hook.source.revoke` observations now enter the ordinary ScriptRun path. The
closed adapter validates their retained source, client, identity and project,
then projects setup as `applied` and revoke as `invalidated`. Current
Manager/GM and project scope still govern admission. A delayed setup fact
remains readable after revocation; the disabled credential remains disabled.
No synthetic Operation, Task or Attempt is created for these events, and script
input omits repository, actor, token, token hash and raw payload.

The Windows Git adapter checks normalized repository and worktree `.git` paths
against its 220-byte UTF-8 policy before `worktree add`. The Store closes
`WORKSPACE_GIT_PATH_TOO_LONG` only for a queued, unbound launch with a preparing
lease. It commits a blocked launch, settled Operation, stale lease and
`not_attempted` evidence through the existing action/event path. Scoped current
Manager readback exposes the exact code and the configuration repair. A legacy
uncertain launch remains uncertain, retaining its first available failure even
when a later observation reports that path code.

Independent Luna source and integration reviews passed. Gate
`6cedc71e-18a7-4116-8e2d-032b7ca0c239` passed all three affected checks: real
HookSource setup/revoke writers and safe admission, normal/verbatim Windows
UTF-8 paths at 220/221 bytes, and the actual closed/legacy Store failure writer
with Manager readback. Crate formatting, strict production Clippy, compilation
of all 328 library checks and the debug build passed; all 209 source pins
remained unchanged during the final gate. Fresh full CI for this increment
remains pending. The preceding `92efc38` source passed complete Windows and
remote Ubuntu CI in run 37278544536.

Fresh C35 native recovery and C24 v5 hosted Bunny remain unclaimed and
unqualified. The v5 scenario corrects the artifact-read API mismatch in the
unexecuted v4 plan. A complete O7 workflow harness and additional native error
projection are private preparations. The consumed C34 attempt is retained;
the installed launcher and current Codex/OpenCodex were preserved. Local
Linux/WSL and all local models remain deferred. **PARTIAL_PROGRESS**.

### Native failure events and readable integrity holds — 2026-10-05

Native MCP retry and stale-hold writers now commit a closed
`controller:native-mcp` / `native.mcp.failure` observation in the same
transaction as their existing recovery marker. Ordinary configured event rules
consume it through the common ScriptRun path. Stable occurrence keys prevent
unchanged recovery ticks from duplicating the event. A retained launch remains
a valid historical link after cancellation; a missing Operation omits an
unlinkable fact, while a wrong method remains an integrity error. Existing
current-manager and Operation visibility checks still govern invocation.

Script admission holds a corrupt retained on-behalf link per entry while a
healthy neighbor progresses. Authorized `automation.config.explain` remains
readable: only its affected linked-operation projections become explicitly
degraded and truncated, with a fixed integrity category and no unvalidated
Operation IDs or causes. The retained held reason remains visible. Apply and
execution keep their strict link checks; unrelated Store errors propagate.

Workspace launch diagnostics retain the first available failure alongside the
latest readback observation. Legacy rows seed that first record from the only
remaining observation, without reconstructing lost history. Current-manager
`operation.get` exposes their bounded codes and classifications for unknown
workspace effects. Ordinary Operation readers retain their existing projection;
the uncertain launch and lease are preserved without replay.

Independent Luna source reviews passed. Gate
`f41fab57-84f0-4b1e-a28a-2fe3e4b80afd` passed five affected Store checks and
identified the unreadable explain result. After the production readback fix,
gate `2680aa83-0ee4-48bf-b305-37e1c539114a` passed the corrected integrity check
and healthy successor-manager history check. These are seven distinct passing
affected checks. Crate formatting, strict production Clippy and compilation of
all 325 library checks and the debug build passed. All 209 source pins were
unchanged during the final gate. Complete CI for published source
`92efc384991057c3c2d2ce39b4154fa40a8ce0dd` passed on Windows and remote Ubuntu in
[run 37278544536](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37278544536),
including the Rust checks, offline adapters and release builds. These Store and
CI checks do not establish native model use.

The consumed C34 launch remains retained. A separate fresh offline Git
reproducer with matching path lengths returned exit 128 and
`fatal: '$GIT_DIR' too big`; it does not recover the lost first C34 error.
Windows path admission, fresh C35 recovery and C24 v4 hosted Bunny inference
remain unqualified. Hook setup/revoke event adapters and the complete O7
workflow are being prepared separately. The installed launcher is unchanged;
local Linux/WSL and local models remain deferred. **PARTIAL_PROGRESS**.

### Terminal event adapters and restart isolation — 2026-10-05

The common event path now distinguishes retained ScriptRun results, host
termination and provider command acceptance. Script callbacks project the
exact run's completed, failed or incomplete state after checking the linked
Operation/result and optional Task tuple. The host writes graceful `host.exit`
and failed `host.exit`/`host.failed` facts in the receipt transaction; failure
views coalesce by host epoch and expose only a closed failure category and
fixed supervisor name. Tokio task IDs preserve that name through join errors.
Detected interruption remains a separate phase. None of these lifecycle
results establishes Task completion.

The accepted runtime adapter emits a statusless `native_input_accepted`
projection after validating the historical observation/Operation/binding and
module-owner tuple. SQLite reads the closed envelope fields without copying
private details into Rust or selector input. A statusless rule matches; a
completed filter does not. Large private details no longer remove legitimate
Accepted or Unknown occurrences. The source key is format-checked and serves
as identity, not authentication; its payload digest is not recomputed.

Restart reconciliation now handles `STALE_LAUNCH` per queued launch. A coherent
retained identity permits bounded retry; an unknown identity stays held.
Waiting ticks preserve the stored deadline and counter, the scanner continues,
and the historical C8 receipt is unchanged. Restored authority still requires
the live snapshot and complete record validation before recognizing observed
proof without a new challenge. Storage errors remain errors of the supervisor.

Independent Luna source reviews passed. Gate
`89e034d1-c2e3-424d-94e5-b4663090d33b` passed 15 affected checks; its new Accepted
fixture required the existing script schema and then the shared JSON-null
representation of an absent status. Those fixture corrections changed no
production authorization or event semantics. Final gate
`aee88ba5-b857-419c-ba3c-165217e79d9b` passed the corrected check, crate formatting,
strict production Clippy, library compilation and debug build with all 208
source pins unchanged. Together these are 16 distinct passing affected checks;
the library compilation contains 320 checks. Complete CI for
`485cb28624eab204645127646cbbf0358b77a221` passed on Windows and remote Ubuntu in
[run 37269940554](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37269940554),
including the Rust tests, offline adapter checks and release builds.

The Store restart fixture proves continuation through two stale rows, stable
retry state, retained proof bytes and SQLite error propagation. Fresh live
ready recovery/no-replay remains unqualified. Fresh C34 run
`dca276b9-0622-4b34-b2bd-d9286de2ec28` stopped at `service_start` with
`WORKSPACE_EFFECT_UNKNOWN_READBACK_REQUIRED`. Its retained launch and lease
are `outcome_unknown`; the latest safe workspace diagnostic is
`WORKSPACE_PATH`. No binding or owned service start was admitted, and no
inference or Task dispatch occurred. Source, candidate, installed launcher
and protected processes were preserved. The consumed attempt remains
retained; its initial workspace failure is still under investigation.
Hosted Bunny inference remains unqualified.
Native MCP retry/failure markers are currently readable through manager
diagnostics; publishing those transitions as ordinary bus events remains the
next adapter increment. The installed launcher is unchanged. Local Linux/WSL
and local models remain deferred. The program remains **PARTIAL_PROGRESS**.

### Universal event routing and independent failure isolation — 2026-10-05

Manager-configured ScriptRun rules now select any bounded exact source/kind
through the shared durable observation reader. Optional supported status
filters use safe adapter projections. There is no global kind whitelist or
provider-specific runner. The normal host automation cycle drains retained
pending causes into ordinary script Operations after its Store transaction
commits; bundle preparation stays outside the database owner transaction.
Current manager, script revision and source visibility are rechecked at
admission and start. Cursors, semantic deduplication and pending history survive
restart and explicit entry transfer.

Migration 009 allows either an entirely absent Task/Attempt tuple or the real
complete tuple in the existing script-run table. It preserves the immutable
004 contract, rows, indexes and foreign keys. Every open checks live canonical
DDL and SQLite metadata against the same-engine reference schema, with foreign
keys explicitly enabled. Taskless runs receive zero controller-effect grants;
direct manual calls still require exact Task/Attempt scope.

Safe producers cover committed messages/replies, coordination answers, native
terminal outcomes, validated result pages and detected host interruption.
Raw/normalized aliases coalesce by phase and occurrence; two distinct phases
of a consult remain distinct. Result-page EOF does not prove Task completion,
and interruption does not identify its cause.

Additive SQLite migration `010` now captures every committed Operation insert
or state transition into `rejected` or `outcome_unknown` with an
`AFTER INSERT`/`AFTER UPDATE OF state` trigger in the same transaction. This
applies across action/provider writers, uses one stable Operation-and-phase
occurrence, and emits only a closed bounded phase/status/error-category DTO.
It copies no request/result body, free-form error or credential and performs no
historical backfill. The raw `runtime.outcome` adapter maps only exact
`unknown` to the matching `operation.outcome_unknown` occurrence; it does not
call unknown work completed. Existing applied/rejected aliases are preserved.

The normalized event bus remains distinct from addressed mailbox delivery.
`controller:messages` observations are excluded from `report.delta` and
`message.read`, so a send still has one raw mailbox timeline entry. Other
Operation-linked observations are SQL-filtered by the existing Operation ACL
before pagination and their exact Operation link is revalidated afterward.
Event rules see safe metadata only; retained diagnostics remain available to a
currently authorized manager through scoped Operation reads.

Forge serializes an exact repository/ref target while independent targets
progress concurrently. Independent persisted keyset cursors for reconciliation
and dispatch prevent one old held target from starving later targets.
Readback-only recovery of uncertain writes remains unchanged.

Script admission now isolates a damaged retained revision per automation entry.
Its exact revision and bounded error category are held in the existing durable
journal while healthy entries continue. The same revision is not retried;
activating a valid new revision permits ordinary admission revalidation.
Retained bundle bytes, JSON and artifact metadata use this same path; unrelated
Store and I/O failures still propagate. `automation.config.explain` exposes the
existing `script_run` cursor, pending/history and held reason to its authorized
manager without creating a journal or advancing a cursor.

Legacy submission rules again match their registered source namespace and assign
the real reviewer. Explicit automation transfer reports six core state ledger
families and a seventh only when a ScriptRun journal was actually relocated.
The generic keyset wrap includes its anchor once, so a released held entry can
progress without losing a reconciliation cycle.

The earlier focused gate `2be0e438-a561-483f-9d9d-d683ed2cc412` recorded
formatting, strict production Clippy, library compilation and debug build as
passing with 203 source pins unchanged. The retained focused gates recorded 28
distinct library checks plus one Windows `check_probe` pass. These earlier
results remain historical evidence; they do not qualify the current full-CI
batch. The installed launcher and current Codex/OpenCodex processes were not
replaced or restarted.

Full CI for predecessor `3741c16` passed the complete Ubuntu pipeline and
Windows library checks, then failed the Windows orphan fixture in run
`37254527962`. The fixture correction passed its stated focused gate. The
historical full CI run `37258458104` for `e4dfb9b` reports **FAIL 298/7 on both
Windows and Ubuntu**. Its seven failures led to the Operation ACL, legacy rule,
ledger-count and committed-event contract corrections above. One full local
library pass recorded 310 passing cases and three failures; the affected checks
then passed after the transfer count and keyset corrections. Three additional
retained-bundle corruption cases cover bytes, JSON and valid JSON metadata of
the wrong shape. The schema still rejects malformed metadata JSON.

Final targeted gate `da68f2f4-121a-4075-829e-e47f80b3134b` records formatting,
strict production Clippy, library compilation and the corrected metadata
scenario passing. Debug build also passed with all 207 source pins unchanged;
the installed launcher hash is unchanged. Full CI for `064315ea08b9f0c47c2b5393e2988311f3957213`
passed both Windows and remote Ubuntu in
[run 37263693234](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37263693234),
including Rust tests, offline native adapter fixtures and release builds.

The subsequent fresh Windows C33 native run
`c86f1118-07bb-41ae-b938-19acac2e86c1` failed at `fresh_host_restart`.
Its retained lifecycle receipt records `STALE_LAUNCH` at host epoch 2, 15 ms
after startup. Migrations succeeded; the MCP tools selector then validated
current pre-dispatch authority before inspecting retained observed proof.
Startup had set the binding to `reconciling`, and the domain stale classifier
did not include `STALE_LAUNCH`, so the entry error terminated its supervisor.
The consumed C33 run remains retained. Source, candidate, installed launcher
and protected processes were preserved; no inference or Task dispatch occurred.

Fresh hosted Bunny inference remains unqualified. Local Linux/WSL and local
models remain deferred. The complete program remains **PARTIAL_PROGRESS**.

### PR description actions and successor recovery — 2026-10-04

Two manual actions now use the ordinary Store Operation contract:
`github.pull_request.update_description` and
`github.pull_request.reconcile_description`. Updates require the current
GM/Operator, an applied accepted-candidate publication and an exact PR target;
recovery is GET-only. Unresolved effects own the repository/PR slot across head
changes. Pre-write authority/candidate failure rejects only an exact queued
Operation. Current scoped GM can cancel a predecessor's unsent PR action after
Task revision without rewriting its original caller/request. Authenticated
readback from an already-authorized GET survives GM handover.

Formatting, strict production Clippy and library compilation passed gate
`bc0ea0be-e525-4ea1-bdf0-740b1aced918`; its only remaining failure was an
incorrect root test-filter path, before any test ran. The unchanged compiled
binary then passed eight checks in `06e90512-93b4-49e0-9e5e-277a8f0c330f`.
Its one Store scenario initially stopped on a missing fixture registration
request ID. Root corrected the test helper and the fake preflight PR identity,
preserving the real authorization and resource guards. The corrected scenario
and debug build passed gate `4d5189e7-95df-4ae2-9d5f-ae6b4dce4f04` with all
source pins unchanged. These are nine distinct focused checks. The fake
transport tests do not execute Git publication or live GitHub writes.

Full CI for the preceding managed-label revision `e679f55` passed on Windows
and remote Ubuntu in run `37251900292`. Full CI for this PR-action revision
failed the Windows orphan fixture in run `37254527962`; the full Ubuntu
pipeline and Windows library checks passed. The universal-event batch above
includes a locally verified fixture correction. The complete program is
**PARTIAL_PROGRESS**.


### Managed-label successor recovery — 2026-10-04

`github.effect.reconcile_managed_label` creates a normal GM/Operator readback
Operation for an exact retained unknown label effect. It performs GET only;
source, repository, Issue, label and the desired-state slot are verified before
readback and again when its outcome is committed. The original actor and input
remain unchanged. Historical Task revision/selection changes do not block the
read; a handover during an already-authorized GET does not discard its exact
observed result. Starting another GET checks the new caller's current rights.

The restricted GM MCP profile exposes the method; other restricted profiles
do not. Formatting, strict production Clippy, library compilation, both
managed-label Store fixtures, the profile boundary, tool registry and closed
schemas passed gate `5f973029-d45d-4192-95df-8b9100406880`. The debug build
also passed with all 195 source pins unchanged. This is five focused checks;
the fake provider fixtures send no live GitHub write. Integration corrected
an owned-ID capture and removed an unused production test-transport wrapper;
the authorization and exact-resource guards remain in place.

Full CI run `37251900292` passed on Windows and remote Ubuntu for published
managed-label revision `e679f55ad31f13f61f03cf1bde58d9b24c9b138d`. Generic script/event and nullable-scope
schema overlays and per-target Forge concurrency
remain separate in-progress implementation; the complete program is
**PARTIAL_PROGRESS**.

### Kernel recovery and review-result watches — 2026-10-04

The current integration adds `submission_reviewed` one-shot watches for the
exact Task revision, Attempt, submission and candidate. Admission checks the
current subject; a subsequently released Attempt or superseded Task does not
discard the authenticated late result for that retained subject. The notice
contains bounded review metadata and creates no model turn or new work.

For verified module bridge boot changes, current/successor Manager
`operation.get` exposes `module_recovery_action_required` with the exact
reconciliation request for Command Code .3/.4, Codex .3 and Antigravity .2.
Original caller, request and result remain intact. Unknown native effects stay
unknown and the projection never authorizes replay or infers a crash cause.

Formatting, strict production Clippy and library compilation passed. Gate
`66653236-c426-4939-8f80-10e570546839` passed the closed MCP schema check,
the module recovery/manager handover fixture and the historical late-review
fixture. After correcting only an invalid Participant fixture override, final
gate `0fe637cb-ab4a-486b-92ec-81d9540fb920` passed restart/deduplication
readback and debug build with unchanged source. These are four distinct new
passing library checks. The review fixtures seed the settled submission facts;
the recovery fixture uses a synthetic departed-owner proof. Neither establishes
new native provider execution.

The Windows output-limit/deadline/descendant fixture passed separately in
gate `79cd72e4-e94f-4909-8380-adad9b559664`. Its overflow source is now the
already-built native CLI, avoiding unrelated PowerShell cold-start time while
preserving product deadlines and cleanup assertions. Full CI run `37249166766`
passed on Windows and remote Ubuntu for published revision
`e9c02690b8b02a51e1d81d34a9af305c9560d5cb`. The complete program remains
**PARTIAL_PROGRESS**.

The owner clarified the universal transactional kernel and adapter contract in
[Architecture](agent-operations/architecture.md). Manager-configured scripts on
**any system event**, including events without Tasks, are required. The general
selector, invocation context, additive migration and safe event producers are
being implemented in private Luna overlays; the currently published
submission/review rule is not full any-event script support. The exact PR-write
and successor label recovery batches are described above. Linux/WSL and local models remain
deferred on the operator's computer.

### Native owned-service inventory and restart qualification — 2026-10-04

Fresh C32 run `2acba7ba-5f09-434f-b2bd-6daf9789c026` passed on published
`e9c02690b8b02a51e1d81d34a9af305c9560d5cb`, with candidate SHA-256
`C9C1A513A2BF70409662E90A851EEB02E37119F9667C1C5555AB15B3484CAAFE`.
It observed nine native MCP tools and the exact challenge on an isolated
OpenCode 2.0.7 service. Unknown hook statuses correctly produced sequence zero.
Nine native-axis diagnostic readbacks matched the current Manager projection.
The C8 post-effect failure branch had no matching failure and validated no
failure readback; this run does not qualify that unobserved branch.

Graceful shutdown and host restart verified the exact owned process, listener,
runtime and connection had departed and the service did not respawn. The
workspace lease remained held. All 533 tracked file pins, the candidate and
installed launcher stayed unchanged; all 30 protected processes were preserved.
The claim is consumed and must not be replayed. No Task dispatch or inference
was requested; model consumption and provider-request hooks remain unknown.
Hosted Bunny execution is therefore not established by this qualification.

### Five-module integration — 2026-10-04

The integrated source adds five independently authored Luna slices:

- **O4:** manual `github.effect.managed_label` for one `eliot-*` label on a
  selected exact-revision Task's mapped Issue. Ordinary Operation and semantic
  slot precede the single write; unknown outcomes reconcile by readback only.
  PR creation/update and Check Run writes remain separate work.
- **O5:** `hook.emit` retries only `HOST_UNAVAILABLE` and `OUTCOME_UNKNOWN`, at
  most three attempts with the same source/commit identity. Store deduplication
  returns the original observation. This does not add a durable retry queue.
- **O6:** one declared invocation-scoped `task_owner_message` effect uses the
  normal `message.send` path. Completion rechecks current Manager, active script
  revision and exact Task/Attempt; revoked grants produce `effects_incomplete`.
  An existing caller/request Operation cannot be relabeled as a script effect.
- **O8:** a closed `task.submission` / `applied` / `review_dispatch` rule routes
  through existing admission and semantic slots. Empty rules disable automatic
  ReviewDispatch, including hook-assisted joins; fact intake, other selected
  actions and direct manual review remain available.
- **O9:** manager-enabled shared Goal progression admits one ordinary
  `agent.goal` continuation for a verified completed OpenCode turn. Exact Goal,
  Task/Attempt, binding generation, current rights and terminal EventRef are
  checked; explicit automation transfer preserves its cursor and history.
  A queued continuation proves admission intent, not native execution or Goal
  achievement. The native continuation boundary is `native_input_admitted`.

Production formatting and strict Clippy passed gate
`eea45400-176d-41a3-8053-29173ca88915`. The initial batch retained 44 passing
checks. The corrected HookCommit fixture passed gate
`c258f2ab-c641-44d0-9749-4d942327b7cc`; the complete Goal
admission/transfer/EventRef-replay fixture and debug build passed final gate
`68eff712-1ec0-43c2-9492-4b6fb42c9473` with unchanged source. This is 46
distinct focused checks, including MCP contracts, script grants and collision
rejection, and ambiguous GitHub write recovery without resending. The Goal
fixture's sealed-record corrections passed an independent source audit; no
production guard was weakened and the 45 other passing checks were retained.
Full CI run `37244945925` passed on remote Ubuntu. On Windows, formatting,
Clippy, build and all 282 library tests passed; the output-overflow integration
fixture timed out starting PowerShell before emitting output. A test-only
correction uses the already-built CLI as the output source; product deadlines
and process cleanup remain unchanged. These slices remain **PARTIAL_PROGRESS**
for the complete program. Subsequent recovery and review-watch work is recorded
above; remaining Luna implementation covers PR description updates and general
event-driven script invocation.

### Native RPC schema compatibility and actionable errors — 2026-10-04

The pinned OpenCode 2.0.7 RPC decoder cannot compile the JSON Schema
`pattern` keywords used by the observer's arm/read inputs. An offline decode
of the retained C29 request failed before the handler; its envelope, field
types and registered RPC identity matched. C29's original HTTP body was not
retained, so this establishes the schema defect without inventing its response.

The wire schema now uses exact UUID/SHA lengths and bounded session lengths.
The handler enforces UUID, lowercase SHA-256 and strict full-string `ses_`
validation before MCP access. Session IDs with a trailing Unicode line
terminator are rejected. Existing assignment, process, module, scope, replay,
capacity and TTL checks remain in place. The actual pinned decoder and plugin
fixture passed once on the final source, with 36 assertions covering activation,
valid arm/read, malformed inputs before MCP reads and duplicate-arm deduplication.

Native arm/read HTTP errors now retain an optional closed `rejection_class`:
`invalid_input`, `method_not_found`, `unavailable`, `invalid_output`, `internal`
or `unclassified`. Only those exact RPC POST routes and bounded 400/500
envelopes are classified; raw message/data/credentials are not retained.
Existing status-derived error codes and unknown effect handling are unchanged.
C8 persists the class and current-Manager `operation.get` projects it alongside
the safe stage/code/time. Legacy records still project with their extra private
fields stripped. Invalid classes produce a nested corruption diagnostic while
preserving the original Operation receipt. The real Store successor-Manager
fixture passed for both valid classes and malformed-class cases.

Root gate `b7957556-d613-48cf-9ec7-e2c6523737b0` passed formatting, strict
production Clippy, both focused Rust fixtures and build with unchanged source.
The final JS-only correction passed gate
`final-cbcca750-95ce-49b0-b643-9f9368736ae1`; every other source pin and the
candidate remained identical. Candidate SHA-256:
`5D79C31E552CB4E6FF4DCD1E97E05D8A1E3FA9EBC869CED68DC346C04DC68EAA`.
C30 ended with `NATIVE_MCP_INVENTORY_READBACK_INVALID` in retained run
`9bec4e52-d5c2-45f2-aabd-a3f857b7aa1c`; its claim is consumed and must not be
replayed. The service returned nine observed MCP tools and a valid inventory
digest. Both model-dependent hook statuses were unknown, so their sequence was
correctly zero; the harness had incorrectly required a positive sequence.
The fresh C31 preparation corrects that predicate and namespace labels and
passes its bounded offline regression and independent source review. C31
ended at `launch_readback` with `LAUNCH_STARTUP_ONLY_STATE_NOT_RETAINED`.
Run `8278392b-5e9d-4362-8b51-beb7a665178d` is consumed and must not be replayed.
Independent read-only inspection found the launch still queued with a ready
binding, `launch_state: "awaiting_native_mcp"`,
`native_mcp_capability_state: "unknown"`, `task_dispatch: "not_started"`, and
`dispatch_permitted: false`. Those fields match the producer; the harness
expected the obsolete `awaiting_capability` / `capability_state` shape and
stopped before its intended Manager readbacks. This establishes a harness
contract mismatch, not a completed native lifecycle or model execution. Fresh
preparation must check the remaining assertions against current contracts.
Full native lifecycle and hosted Bunny execution remain unqualified. All ten
protected Codex process identities and the installed launcher were unchanged.

Full Windows and remote Ubuntu CI succeeded for this RPC source increment,
`f338d4ed2cbf6a463b0de8d0ea377bef49391bc3`, in
[run 37240206914](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37240206914).
This historical CI does not qualify the newer module batch above.

### Manual automation invocation and correction cycle — 2026-10-04

`schedule.run_now` is implemented for the authenticated Manager's saved
CheckRun action. Its closed request contains `client_request_id`, `project_id`
and `automation_id`. The action uses normal CheckRun planning and admission,
rechecks the exact saved entry and current Attempt/source in the committing
transaction, and retains a durable manual invocation marker. The same request
reads back its original receipt after the entry changes. Recurrence may stay
disabled; the invocation neither creates a calendar occurrence nor advances
cron cursors. Independent Luna production and regression reviews passed.

One real-Store regression exercises disabled recurrence, one queued CheckRun,
exact receipt replay after removing the selected step, unchanged cron state and
foreign-Manager rejection. A second real-Store scenario creates one Task and
Attempt, submits A, records its assigned review and return, submits correction B,
records B's distinct passing review, rejects acceptance of stale A and accepts
only B. Candidate artifacts are fixture inputs; submissions, reviews and
acceptance use the actual Store handlers. Both Store scenarios passed. All 34
distinct MCP checks passed across the final gates, including the new closed
request and Manager/GM discovery contract. Strict production Clippy passed in
`b515f416-e657-4e58-b096-831622840ec3`; subsequent changes corrected only test
fixtures/contracts, with exact production equivalence checked. Final gate
`cf1b8530-6ffc-44b1-ae24-4cabf208f64e` passed formatting, the corrected profile
contract and build with unchanged source, retaining the two Store checks and
33 other MCP passes. The new debug candidate SHA-256 is
`A26E038B93A766E55FFA708B21F56213B4BD4A68678F6C3EE8A25825A6F205A0`.
Full CI for source `836c938123714392d75b57158c777973aa6d8c07` passed on Windows
and remote Ubuntu at
[37237925409](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37237925409);
verified 2026-10-04. The RPC correction above requires its own full CI.
These scenarios do not establish native correction delivery, checker execution
or publication. Typed event rules and shared Goal progression remain incomplete.

### Plugin activation context correction — 2026-10-04

The pinned plugin supervisor activates effects with Scope and logging services;
it does not provide the internal MCP service tag. The observer looked up that
tag before registering hooks or RPC handlers. A controlled offline invocation
of the actual effect failed before any registration; providing only the MCP tag
made the same effect register all three hooks and both RPC methods. This proves
the activation-time dependency defect. C28's raw native error was not retained.

The observer now resolves MCP inside the `arm` RPC handler, after the existing
challenge, replay, session and capacity guards. The request location supplies
that service in OpenCode 2.0.7. The native tools snapshot, source identity,
schemas and all input bounds remain unchanged. Independent Luna review passed.
One offline behavioral regression passed (19 assertions): bare activation,
scoped native tool readback, invalid-scope rejection before MCP reads, and
duplicate-arm deduplication. It preserves unknown context/provider/model state.
CI now runs this regression with its existing pinned Bun runtime.

At activation-fix source `2f9e24136024da6654ec447762cc1b3c69f69e62`, all 188
other source pins and the previous debug binary were unchanged. Exact-source
Windows and remote Ubuntu CI
[37235180822](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37235180822)
passed Clippy, Rust tests, offline module checks and build, verified 2026-10-04.

C29 run `4877b0e9-d11d-44cd-b81f-e4ac3dbdb9d2` ended once (`exec54614`, exit 1).
Plugin identity, active server and source preflight passed. The first observed
retained failure was `NATIVE_REJECTED` at `challenge`, after challenge effect
reservation; later read-only recovery retained the same code at `tools_readback`.
The outer `OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start` does not
identify the rejected RPC's cause. The raw rejection class/body was not retained,
and bounded input metadata inspection found no proven field mismatch. The
harness validated four current-Manager C7 readbacks but made no C8 Manager
readback for this post-reservation code; it does not qualify C8 delivery for C29.
Preserve the consumed claim and uncertain arm without replay. Native MCP proof
and model execution remain unqualified; the project remains **PARTIAL_PROGRESS**.

### Windows plugin configuration path correction — 2026-10-04

The pinned OpenCode 2.0.7 loader failed to resolve the server entrypoint for the
canonical Windows verbatim package path. An offline comparison resolved and
loaded the expected plugin from an ordinary absolute spelling of the same
canonical directory. The correction projects Windows drive and UNC prefixes
only at the configuration boundary. The producer and every exact configuration
comparison use the same helper; canonical filesystem, source digest, wrapper
digest and scope checks retain their previous validation.

Final source gate `3afdc876-1786-4e55-960a-1807e37ebf29` passed formatting, both
focused path/config regressions and debug build with unchanged source. Strict
production Clippy passed in `68b94ca2-fe9c-49a6-8995-98cdd2a7c52b`; the final
change corrected only the regression fixture's serialized input shape, with
production source equivalence independently checked. Independent Luna review
passed at the final file hashes. The new candidate SHA-256 is
`867E9FF4107DDB558612C38D3210486963783E39490328479FC24517421DB5AE`.

C28 run `b7b1f673-03c0-400b-9f2e-a954fdbdb22f` used this candidate and ended with
`NATIVE_MCP_PROOF_PLUGIN_STATE_MISMATCH` at `challenge_preflight` (`exec24279`,
exit 1). The expected plugin ID passed the missing/nonunique guards, but active
server state did not pass. The current-Manager error projection validated once
with zero failures or corruption; C7 readbacks validated twice with zero failures.
The challenge remained prepared and no effect was reserved. Plugin activation
is under a bounded source audit; the exact native error body was not retained.
Preserve the consumed C28 claim without replay. C29's terminal evidence is
recorded above.

The offline probe and source gate do not prove plugin activation, callable MCP
tools or model execution. Hosted Bunny qualification follows successful native
MCP qualification. The installed controller and all protected Codex processes
were unchanged after C28 cleanup. Local Linux, WSL and local models remain
deferred. The project remains **PARTIAL_PROGRESS**.

### Latest qualified source and C27 terminal outcome — 2026-10-04

Source `60398688823c07c9d796bc1df6dd5b7c3e2abf6a` is published on main.
Final gate `211c07a6-a6be-4fe2-85a6-84ddbbd46ec5` passed formatting, the
corrected C8 Store regression and debug build with unchanged source. Strict
production Clippy and six other successful Manager regressions are retained
from `d2102fff-eded-40b8-962c-f9bda23ae425` by exact source equivalence; only
the test fixture changed. Seven distinct regressions passed across those gates.
Independent Luna review passed. The
qualified candidate SHA-256 is
`62564B7EB8DD574A39AF6284422504BA97E13A525DD9B28970975696936063E7`.
Full Windows and remote Ubuntu CI [37228196378](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37228196378)
passed, verified on 2026-10-04.

Current-manager `operation.get` includes `native_mcp_tools_readback` for the
exact launch, exposing validated failure code, stage and time plus scheduler
state. Historical diagnostics omit private paths, configuration, schemas,
proof and credentials, and remain readable after Manager handover. A
readback-only action for an interrupted unknown start grants no retry.

C27 run `b967289b-c085-48f0-9464-d882b2a1213c` ended at
`native_mcp_tools_manager_readback` (`exec1819`, exit 1). Its actual retained C8
failure was `NATIVE_MCP_PROOF_PLUGIN_MISSING` at `challenge_preflight`, recorded
at `1791143831520`, after `agent.open` at `1791143730468` and first-ready at
`1791143749914`; no challenge effect was reserved. The corrected harness
validated the exact current-Manager C8 readback (1 valid, 0 failed), C7
readbacks (2 valid, 0 failed), and six runtime snapshots (zero failures or
gaps). It produced no corruption or private-read error. This closes live error
delivery for this exact C8 failure, but native MCP proof and model execution
remain unqualified. C15 and C8 are auditing the plugin registration/loader
cause at this historical checkpoint. The subsequent bounded, offline probe of
the pinned OpenCode 2.0.7 loader proved that the exact Windows verbatim package
path (`\\?\` prefix) resolves no server entrypoint, while an ordinary absolute
path to the same canonical directory loads the expected plugin. This probe did
not activate the plugin or start a service. The source correction projects only
the serialized configuration path and shares that projection with all exact
config consumers; canonical source, digest and scope checks remain unchanged.
Its fresh source gate and native qualification are recorded in the next
checkpoint. Preserve the consumed C27 claim without replay.

C26's earlier projection mismatch was a harness alias-collision defect, not a
product diagnosis: aliases generated from `assignment_type`,
`last_error_type` and `challenge_type` were checked as extracted values rather
than JSON type aliases. The corrected C27 parser did not reproduce that error.
The installed controller and active Codex/OpenCodex remain unchanged; local
Linux, WSL and local models remain deferred. The project remains
**PARTIAL_PROGRESS**.

### Previous qualified source and C25 evidence — 2026-10-04

Source `22420c899c337b4e5b8186e5f97abb70f3b8dc70` is published on main. Gate
`00a31d17-a9a5-43ea-8da4-ad701aaa98a1` passed formatting, strict production
Clippy, ten focused tests, debug build and independent Luna review with
unchanged source. The candidate SHA-256 is
`1E9021E0EA2F85A1C8FAE9F56E2D967563E92C6C4953DDCAF1F44EC2F6973DAD`.
Full Windows/Ubuntu CI [37225573566](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37225573566)
completed successfully for this source.

C25 run `06375985-3de6-4f3b-9bae-2715121fbb25` ended with outer result
`OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`. C7's owned-service
readback was retained twice. The run recorded 23 successful runtime snapshots,
zero gaps or axis failures, and four successful current-manager C7 readbacks.
C8 then recorded `NATIVE_MCP_PROOF_SOURCE` at `challenge_preflight`, before
effect reservation. The participant was installed, registered and connected;
the challenge was prepared, but no effect was reserved and tool readback
remained null. The exact failed predicate is unproven because the source merged
multiple guards and did not retain the native response. At the C25 source, the
C8 safe error was not visible through current-manager `operation.get` or
`swarm.exceptions.get`. No native MCP proof or model call occurred; preserve
the consumed run without replay.

The increment above adds the current-manager C8 projection, precise source
codes and interrupted-start readback action. It does not invent a crash cause,
write a new native observation or replay an effect. The action retains exact
binding references and current-GM authority independently of the former chat.
The C26 harness defect and C27 terminal result are recorded in the latest
checkpoint above. C24 hosted Bunny remains frozen private preparation and must
not execute before full native MCP proof. The project remains **PARTIAL_PROGRESS**.
The installed `C4A28DA` controller and running
Codex/OpenCodex remain unchanged; local Linux, WSL and local models remain
deferred.

### Previous qualified source and C23 evidence — 2026-10-04

Source `251dd55ddd817460d5816a80964abd0d864e0610` was pushed to main after
final gate `d312a3a6-36ac-47c4-a1aa-5526931ada52`: formatting, strict production
Clippy, eight focused regression tests and debug build passed. The final test
fixture change preserved production source equivalence with the initial Clippy
gate. Exact-commit Luna review passed. The qualified debug candidate SHA-256 is
`10A9526215BB07DD02B802DD7AE57D6E1B52F916AEFA2F0267BDE393731A77E0`.
Full Windows and remote Ubuntu CI
[37224289253](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37224289253)
passed, verified on 2026-10-04. No local Linux or WSL environment was used.

C23 run `7ed16c9d-bc85-4b05-867a-45499ac3a707` ended with the outer result
`OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`. The exact retained
current-manager readback successfully validated ten times and exposed
`NATIVE_MCP_ROUTE_MISMATCH` at
`native_capability_readback`; this is the proven blocker, not an inference from
the harness timeout or cleanup. Pre-dispatch scope validation passed. The
registered enabled participant and current binding were ready and connected,
with 22 successful native snapshots. The owned route omitted
`native_options.service_id` and `native_options.expected_version`, while the validator
looked for those values in external options. Native MCP proof was not
confirmed; no model prompt or inference ran.

The route repair passed the source gate and CI recorded above; C25 exercised it
and exposed the separate C8 blocker described in the current checkpoint. The
source gate for 251 includes atomic
participant identity promotion on successful issuance and passive stale-status
repair only after full pre-dispatch validation and compare-and-set. It also
retains participant preparation, credential-issue and commit failures as a
durable parent diagnostic with current-manager readback; persistence and
selector errors propagate to the host. Their closed stages are
`participant_issuance_prepare`, `participant_credential_issue` and
`participant_issuance_commit`. Preserve the consumed C23 and C25 runs without
replay. Native MCP proof remains unqualified. The project remains
**PARTIAL_PROGRESS**. The installed controller and running Codex/OpenCodex
remain unchanged; local Linux, WSL and local models remain deferred.

### Previous qualified source and C22 evidence — 2026-10-04

Source `71f76a46a48df81d45de8f5697f389bce74f36ac` passed formatting,
warnings-denied production Clippy, focused producer/Manager regressions, debug
build and independent review. Full Windows and remote Ubuntu CI
[37221493452](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37221493452)
passed. The debug candidate SHA-256 was
`44AC5642E861DA1C3E5CA9DFC94DC83BC2713895C257310CFC2F5447CF5D2750`.

C22 run `b78151ea-e516-4a4a-be75-64b00c2e02bc` ended with
`NATIVE_MCP_SCOPE_MISMATCH` at `launch_snapshot_validate`. Actual current-manager
`operation.get` receipts validated; actual snapshots were read. Native MCP proof
was not confirmed and no model prompt ran. The cause was traced to the manifest
MCP identity status remaining `assignment_template` after participant
registration, while validation required `registered_enabled_participant`.

### Previous verified source and C21 evidence — 2026-10-04

Source `4aa85e52d8dfef82431ac9b85537a29bf628341c` passed full Windows and
remote Ubuntu CI in [37219293550](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37219293550),
verified on 2026-10-04. Its production Clippy, focused regressions and debug
build also passed; the qualified debug candidate SHA-256 is
`BD576B1B29A28002F3BF015EDB6AA5DA6D17C487C86ACEC7E5854E0555AAEEEF`.

C21 run `d88bf29c-b6e5-49a0-9b0f-01ec73a065af` ended with
`OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`. `agent.open` had
settled as `native_session_created`; the live binding was ready and connected,
and 22 actual snapshots completed without axis failures. Native MCP proof was
never confirmed and no Bunny prompt or inference ran. The fifth C7 marker
attempt reported `assignment_scope_unavailable`; its exact cause remains
unknown. The later `native_unavailable`/`reconciling` state came from scoped
harness cleanup and is not evidence of the startup cause. Preserve the consumed
run without replay.

### Earlier source and qualification checkpoints

The latest core reliability increment keeps mandatory root validation and moves
independent optional snapshot reads under one bounded concurrent deadline. A
slow catalog or child log no longer erases completed axes or starves the other
pipeline. Configuration and goal share one instruction-entry response. Partial
read failures are visible to Managers with bounded safe diagnostics; failed goal
readback reports unknown presence rather than confirmed absence.

Finished OpenCode workers now produce durable safe failure history for their
exact active scope before replacement, retain unknown in-flight input, and
preserve current-GM readback without tying it to a previous chat. Supervisor
Store errors propagate to the host after all owned workers have been stopped and
joined. Snapshot readiness restoration excludes recovery-required bindings and
unresolved input; Store read errors no longer become empty child sets.

On 2026-10-04, package formatting, strict production Clippy, eight focused
snapshot/worker/Manager/recovery regressions and debug build passed with unchanged
source. Three held-HTTP tests verify retained evidence and independent child-log
and pending-question progress. The first test compilation exposed a test-only
`Vec<Value>` formatting error, corrected before the successful gate; production
Clippy was retained by exact source equivalence. Independent Luna review found
and closed fatal worker-drain, diagnostic-null and unknown-goal concerns. The
debug candidate SHA-256 is
`BD576B1B29A28002F3BF015EDB6AA5DA6D17C487C86ACEC7E5854E0555AAEEEF`.
At this earlier checkpoint, C21 and full CI were still pending; both have since
completed as recorded above. The installed controller and the user's running
Codex/OpenCodex remain unchanged. Local Linux, WSL and local models remain
deferred. The project remains **PARTIAL_PROGRESS**.

The runtime admission increment now retains a separate bounded
`runtime_dispatch_action_required` diagnostic for the exact launch/open pair.
Deterministic opening-actor validation failures settle the queued operation as
rejected before dispatch; infrastructure errors remain errors. Current-GM
readback survives handover, parent rejection and service departure, preserves
the launch admission receipt, and keeps post-send outcomes unknown.

Windows production Clippy with warnings denied, two runtime admission tests,
four public Store failure/readback tests, formatting and debug build passed on
2026-10-04. Production stayed unchanged after Clippy; the final test/build gate
retained unchanged source. The qualified debug candidate SHA-256 is
`74B869D818794AEF3242C74863744212996C67A20E3846BBDADEA7B9FC928F9E`.
Full Windows and remote Ubuntu CI for source
`2ec9952af65b69977a6489fe7043407208ae71b6`
[37212505477](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37212505477)
passed, verified on 2026-10-04. No local Linux or WSL was used.

C18 run `79a8236e-4bbb-498b-a26e-5ea455a27692` stopped at
`runtime_command_select` with `OWNED_SERVICE_RECEIPT_CORRUPT`. Both
`operation.get` and `swarm.exceptions.get` returned the same retained manager
action diagnostic. The exact open remained queued and unsent; no model call
occurred. This qualifies actual manager error delivery for that startup failure,
while hosted-model execution and the full native delivery cycle remain
unqualified. Preserve the consumed run without replay.

The failure was traced to a producer/consumer mismatch: the canonical Direct
actor manifest omits `effective_manager_id`, but the owned-service validator
required it before selecting the actor kind. The compatibility fix retains
Direct requester/manager/caller checks and requires the explicit manager field
plus retained operation link for WorkDispatch. Package formatting, strict
production Clippy, the actor regression, the real-Store WorkDispatch regression
and debug build passed on unchanged source on 2026-10-04. Independent Luna
review found no authority regression. The resulting debug candidate SHA-256 is
`FB792948EF9908D34CBC0F9CC7A03FF17D495EA0E11E10D20A753ADD42236298`.
Full Windows and remote Ubuntu CI for source
`6ba14b9a56f1de653fcbdb3fe0c9427fba180c81`
[37213706029](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37213706029)
passed, verified on 2026-10-04.
C19 run `0dbbc8c9-e5d5-454d-b4e2-fe21342651de` confirmed the Direct
`agent.open` as settled/applied with `native_session_created`. The owned service
and native session were observed, but native MCP tool proof was not observed
before `OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`. No model call
occurred. This qualifies session creation, while native capability and the full
launch remain unqualified. Preserve the consumed run without replay; the missing
MCP proof is being investigated.

The subsequent reliability increment prevents a failed
OpenCode reconcile target load from claiming completed native readback, and
retains manager-visible `latest_native_failure` history across connection checks
and shutdown. C19 proved that transport status updates could erase the exact
snapshot failure before later inspection. Package formatting, strict production
Clippy, two actual Store/Manager failure regressions, the existing shared-reader
host-recovery regression and debug build passed on unchanged source on
2026-10-04. The candidate SHA-256 is
`243FFA9571BDF69FAB496F064F4C941713C31C7C1800A7F5C1EFA6CAA1B78A6B`.
The initial fixture gates exposed a retained Store sender preventing test
shutdown and a completed JoinHandle being polled twice; both were corrected in
test code. Production stayed unchanged after the successful Clippy gate.
Independent Luna review found no actionable issue in the failure-history
projection. C20 run `cfd5dcc6-c0b9-42a8-a78b-88088fef0b5f` stopped at
`native_snapshot_readback` with `NATIVE_SNAPSHOT_TIMEOUT`. Current-manager
`agent.state` returned the exact binding's retained error and matching timestamp.
This qualifies actual native failure delivery to the manager, while native MCP
proof and model execution remain unqualified. No model call occurred. Preserve
the consumed C20 run without replay.

CI for source `094b0bbb24d6f0300c6a5cacd7880cd21e9af307`
[37216299415](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37216299415)
completed on 2026-10-04: Windows passed; the remote Ubuntu job passed its Rust
and earlier checks, then failed on an unhandled `ECONNRESET` during Muse fixture
teardown. The fixture correction is qualified on Windows; remote CI verification
remains pending. No local Linux or WSL was used.

The previous core increment is `3c1a93b476fc31a4d60345fcac627291e9fe4e54`.
Full Windows and Ubuntu CI for that source
[37208920531](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37208920531)
completed successfully, verified on 2026-10-04. The Windows debug candidate
used by C17 has SHA-256
`D2C2979E1414871DB3D475580147C25CA473E2223A61C3B99DBF6CA2A8B39B68` and was
qualified from source `be2054f`. The installed controller remains
`7061e455f04a76bfaa19699edba7f58c14a27748`; it does not contain this source
increment.

C17 run `e126db26-c70d-4a93-9f37-15aca367ff29` ended with
`OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`. Service observation
and proof were recorded and binding opening was reached, but `agent.open`
remained queued and was never admitted. No model call occurred. The exact
underlying failure reason is unproven; the new admission diagnostic replaces
silently discarded selector errors. Preserve this run without replay. Local Linux, WSL and all local
model/inference work remain deferred.

The source includes durable host lifecycle and interruption receipts, typed
pre-dispatch IPC failures, current-GM startup failure projections, bounded
HookCommit intake and private hook issuance, an immutable script bundle
registry and owned-process runner, task-scoped Goal reminders, and GitHub
Issue intake/work-pool handling. The script registry and direct run/readback
path exist; scoped script controller API grants/triggers and full native
qualification remain incomplete. Goal-driven progression and broader GitHub
effect integrations also remain incomplete. See [host recovery](host-recovery.md)
for the failure/readback contract. Earlier checkpoints below are retained as
dated historical evidence; this current summary supersedes their older source,
gate and native-run state.

The previous delivered and installed source is
`7061e455f04a76bfaa19699edba7f58c14a27748`.
It implements successor-GM continuation of the retained work, including exact
repair dispatch across explicit automation transfers. Full Windows/Ubuntu CI
[37195035427](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37195035427)
passed. The matching controller was installed on 2026-10-04 at 10:25 UTC;
its SHA-256 is `C4A28DA65495FAC229EE1202DA7F4E75EA642F4B909F6F375DE0CED07102EF9E`.
The prior installed controller is preserved in the private installation receipt.

The current source increment `8ae72e3c1330a603d413b345336dbeddee979c26`
adds manager-owned calendar CheckRun automation,
Command native event envelopes (`command-mod-0.1.0-glue.4`), and bounded parent-side
OpenCode startup failure observations. The Command fixtures passed 17 glue checks
and the bridge checks; that artifact has no new native qualification yet. The
Rust increment passed warnings-denied production Clippy, the real Store
admission/coalescing/foreign-owner/transfer/restart regression, three calendar
boundary/DST checks, exact Command artifact admission, and debug build on
2026-10-04 at 11:37 UTC. The successful Clippy was retained after verifying
that the subsequent Windows fixture path correction changed test code only.
All source stayed unchanged during the final gate. Candidate SHA-256 is
`BC74D592B6C422E82B0E1E9277C0D1C33DA59E037D1A92A88C95F0E6F5C55910`.
Luna's bounded authority/consumer and native-diagnostic reviews passed after
the transferred-CheckRun allowlist and coalesced-receipt corrections. The
current source passed its local gates. Full CI
[37199464664](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37199464664)
completed on 2026-10-04: Windows passed; Ubuntu had 247 passing Rust tests and
one failure in `current_gm_transfer_preserves_all_entry_journals_and_retires_old_owner`.
The assertion expected four relocated ledgers and observed five after the new
calendar ledger; its source contract and fixture correction are being reviewed.
The fresh one-attempt C16 run `b93b7821-e6ce-409d-88f4-d1f6f51a3ab9`
ended at `owned_service_start_bootstrap` with `NATIVE_REJECTED` before a model
call. The retained parent diagnostic identifies the bootstrap stage; it does
not establish credential validity or the exact rejected API contract. The
OpenCode bootstrap implementation is being investigated against the pinned
native source. Preserve the consumed run claim and unknown operation without
replay. The installed controller is still the prior qualified `7061e45` candidate.
The project remains **PARTIAL_PROGRESS**, with the full O7 workflow, the remaining
automation programs and cross-environment qualification outstanding. All local
model and inference work remains deferred.

**Source checkpoint:** GM continuity and host-crash submission readback are saved in main. Their production Clippy, debug build and 13 focused GM/Store checks passed. The subsequent MCP table corrections and OpenCode Windows path fix passed full Windows and Ubuntu CI [37186030488](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37186030488) for source 8f3998ffb5da24deaac8445f883fe19780b02f3b, verified on 2026-10-04. Explicit automation transfer now passes production Clippy, its real-Store preservation regression, all 29 MCP checks, the Windows input-probe integration check and debug build. Its qualification boundary and candidate are recorded below. Successful live OpenCode startup, effective hosted-model identity and the full O7 cycle remain unqualified.

C10 adds the optional exact `launch_operation_id` to the actual `task.dispatch` schema and Store contract for launch-owned Attempts. The Store checks the retained parent, exact Task/Attempt/binding/lease lineage, immutable prompt packet, and current C8 MCP capability proof before native input. The owned-provider path accepts one explicitly configured provider credential source and reports `stored_unverified` only for credential metadata; that does not prove key validity or provider/model consumption.

The prior native run `bb070791-ba7c-4c71-9cc7-660fbb531418` ended `OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`, after repeated `OWNED_SERVICE_SCOPE_STALE` and before an owned-service row, MCP proof, Bun call, or model call. Its failure was traced to comparing manifest `runtime.route` metadata as an object against an alias string before start reservation; the route repair is published in `e637d45`. Historical CI 37172541315 failed the Windows `check_probe` lifetime step; the focused regression now passes with `/D` disabling ambient CMD AutoRun startup hooks in the fixture, but the precise historical cause is unproven.

Native run 46cef212-aa65-418a-a911-b54502fe9fd7 timed out at service_start after 240 BINDING_NOT_READY observations; normal stdin EOF closed the host, and the run remains retained without replay. Its opening-fence audit found that database module IDs matched the child while get_binding omitted instance and artifact IDs.

Earlier native run e16291b7-0913-43b0-bf72-35fa409ab4da ended with OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT at service_start. The start outcome remains unknown; final private-audit SHA-256 is 1CA31BA6A1BD6A093BC32FB5CCA5A06C9D1AC4647A4C4717FB35EC45274ADD42. No MCP proof, owner-ready receipt, connection receipt, or family-stop receipt exists. It remains retained without replay, and process=NULL does not establish whether Bun exists.

C11 wires RepairDispatch and acceptance consumers to typed ledgers/cursors, exact same-slot reuse, GM epoch and byte-verification checks, structured requirement reviews, and manager history/visibility. Reviewer independence, GM-ownership precondition, and reviewer-equals-writer checks are fixed; C11 full CI is recorded above.

C12 publishes the owner-sponsored acceptance route to an independent GM; the real-Store regression covered owner-positive, GM-self, and Operator-negative cases. Source and CI gates are listed above.

OpenCode owned-startup diagnostic e2814fe9-ac32-42be-90b5-7ea4173ee2c2 admitted one launch and reached the helper. Its journal recorded process_identity_failed / OWNED_SERVICE_PROCESS_IDENTITY_UNAVAILABLE with a spawnedPID field. The outcome remains unknown, with no ready or native-proof receipt, and the launch is retained without replay. Host EOF completed normally; departure and helper-family stop proof are not yet known. Do not infer a Bun-exit cause.

An earlier Command preflight stopped before host or model startup with RUN_ROOT_ACL_FAILED while autoloading Get-Acl; an ACL-only private probe passed. Later Command run f413bd5d-149d-4344-972b-125b6b3bf23a passed one bounded Bunny task.dispatch: marker 50 bytes, exit 0, no timeout. Requested route was stealth/space-bunny-alpha; effective model was null/unknown, and the native result exposed no upstream provider/model identity. Proof summary SHA-256 is 1C71DAB9592AE72752032556251A602BD5D268F9BEE42ED06BFCB944196453D2. Module owner family was empty and owner exit was 0; host 50688 exited on EOF with code 0, current owned PIDs were absent, and four Codex processes retained their same birth identities.

This single Command result does not establish the full O7 cycle, OpenCode service/MCP readiness or served-model identity. R6 was unchanged at that run's historical checkpoint; the current installation is recorded above. All local model/inference work, including PR24/Kilo, remains deferred.

### Fresh OpenCode diagnostic — 2026-10-04 07:25 UTC

Run aa62853f-8722-48e8-b6ef-eec20c42cf6b admitted exactly one fresh launch after
the private ACL readback was corrected. The actual child exited with code 1;
the retained receipt reports process_gone/process_exit_race. Its complete,
untruncated 62-byte diagnostic says `Refusing a redirected directory path`.
The retained workspace uses a Windows extended-length DOS path; OpenCode's
owner comparison treated that spelling as different from the ordinary realpath.
The module source correction is saved in main 8f3998ffb5da24deaac8445f883fe19780b02f3b; syntax and four extracted path comparisons passed. A successful native restart is not yet qualified. This run has no ready
or native MCP proof and no model turn. Its Operation remains unknown and is
retained without replay. This new cause does not establish the cause of older
unknown runs.

### C13/C14 saved implementation checkpoint — 2026-10-04 05:46 UTC

C13 implements an explicit accepted-candidate publication consumer, typed current-GM authority, shared manual/automatic effect slots, no-effect slot release after handover, and scoped operation/history/explanation readback. C14 adds bounded private startup diagnostics: a closed process-identity failure class, the actual immediate child exit observation, and redacted stderr metadata. These are implemented source paths; live automatic publication and successful owned OpenCode startup are not qualified.

Production formatting, warnings-denied Clippy (15.19 s) and debug build (37.78 s) passed. The candidate SHA-256 is 984B38A3567E572D14EE1D01BADBD343C1FE1575BB6ECEB75AD2A55598B6371B. The existing owner-sponsored acceptance/history check passed. The dedicated publication regression exposed an incomplete synthetic submission result, an outdated explain method name, and an incorrect successor-manager history-read expectation. All three are corrected; the final corrected regression and pending Forge endpoint check have not yet run. No production permission check was weakened.

The later full CI for 509715b passed both publication and Forge endpoint regressions. Later GM continuity source replaces the historical successor-manager denial with the explicit continuity requirement below. The C14 fresh-start diagnostic has now run once; its failure and source correction are recorded above. Preserve all unknown runs without replay. The full native O7 cycle remains incomplete.

### GM continuity correction — 2026-10-04

The accepted requirement is that losing a GM chat must not strand project work.
Same-credential reconnect already preserves the durable client identity. The
source now implements different-successor control of current Attempts,
native admission and operational history, former-owner automation readback, and
same-client binding changes that previously rotated the GM epoch. Original owner,
caller, workspace and producer history remain retained. It also preserves verified
submission artifacts when GM authority changes during local publication. The
source increment is saved in f3eda2883474b28b8d3e8c6e587e696fad5477f6.
Production Clippy (16.10 s), debug build (35.66 s), and 11 focused GM/Store
regressions passed on 2026-10-04 at 06:23 UTC. The source remained unchanged during
these gates; candidate SHA-256 is 6AC53A11511AF1C3A34A7571D5B3A505E66CA50AC471E70E7A3EC9D748FECDBA.
Cross-platform CI [37182521189](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37182521189)
completed successfully on Windows and Ubuntu, verified at 06:37 UTC. The requirement is saved in ac6691c and
described in [GM session continuity](gm-session-continuity.md).

The saved source increment implements `task.submit.recover` and
`swarm task recover-submission <operation_id>` for an exact prior submission
left unknown by a host crash. It verifies the existing deterministic artifact
without publishing or native replay, keeps original caller/submitted-by, checks
the actual current GM again at finalization, and retains stale submission history
without changing the Attempt. A missing file keeps the original target unknown.
An already-settled target returns its stored result without duplicate artifacts.
Source ab8f7aed314b59bd64a05266c1124e3a9412b32c passed all 13 focused GM/Store
regressions (0.17 s execution, 40.33 s including compilation) and the debug build
(39.08 s), verified on 2026-10-04 at 07:04 UTC. Production Clippy passed in 15.14 s
on d6af378; source hashes prove that only the recovery test changed afterward.
The new candidate SHA-256 is
128DC8791F31E29ED18FB2FFC4DF411F896658C1591461116F9C3FC8FEBC022F.
Cross-platform CI [37184545179](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37184545179)
failed solely on the outdated MCP table contract, repaired in 2f3e22a. The focused table guard passes; subsequent full Windows and Ubuntu CI37186030488 passed for source 8f3998f. The installed R6 binary and running native environments have
not been replaced or restarted by these source gates.

**Resume here:** qualify owned native startup and the complete local O7 cycle.
Successor RepairDispatch on the retained Attempt's binding now passes its Store gate.
Use the latest built candidate recorded
below when a fresh harness requires this code. Preserve earlier unknown
runs without replay. Transferring every former manager's entry automatically
remains separate from explicit per-entry transfer.

### Automation transfer — qualified source increment, 2026-10-04

The source adds explicit `automation.config.transfer` to the current GM,
with revision checks, retired source identity, preserved typed journals,
historical operation links and exact queued-operation continuation. The GM MCP
profile exposes former-owner configuration reads and transfer. The real-Store
regression passed, including all four cursors and nonempty pending journals,
unchanged historical configuration Operation, denied unrelated/stale transfers,
and blocked former-owner reenablement. All 29 MCP checks, the Windows input-probe
integration check, warnings-denied production Clippy and debug build passed.
The production source was unchanged after Clippy; only the test's protected-record
reads were corrected before the successful test. The final source stayed unchanged
during the remaining gates. Luna's bounded authority/effect audit found no further
blocker in this slice. Candidate SHA-256 is
`000B92BE2CDF4A0FE789A25CB021603607138F0CD35B9845D14462D1A3FD77C0`.
This is a built candidate, not a newly installed or live-native-qualified binary.

Transferred WorkDispatch workspace reconciliation has a distinct readback-only
path for `workspace_effect_unknown`. It records an observed held lease and keeps
the parent unknown; it cannot claim, open, start or replay. Other uncertain native
start phases retain their existing recovery contracts.

Full Windows and Ubuntu CI for source `21343e7afca61d66b0330e4deb7e6a0c3548d4f7`
passed in [37189940977](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37189940977),
verified on 2026-10-04 at 09:16 UTC.

### Successor review and correction — source increment, 2026-10-04

After explicit entry transfer, the current GM can assign a new review of the
former owner's exact current Attempt. A later successor can consume that review
through the sealed transfer lineage. The actual review sponsor, original Attempt
owner and submission author remain recorded; the current GM is the acceptance
or correction decision actor, and feedback still addresses the Attempt owner.
Historical direct and automatic assignments use their retained assigning
Operation and actual sponsorship, including after a second GM handover.

Warnings-denied production Clippy, the A→B→C Store regression, the successor-GM
continuation regression, legacy owner-sponsored acceptance compatibility, and
debug build passed on 2026-10-04. Production stayed unchanged after Clippy; only
the new fixture's assertions were aligned with the retained queued receipt.
The final gate source stayed unchanged. Luna ratified the retained sponsorship
and authority predicates. Built candidate SHA-256 is
`6B5978C85B5715E06B8A9C88FB97409D2763A0534CFEB1DDADFF9DED08623764`.
The gate covers A-owned Attempts, B-sponsored independent reviews, then C's exact
correction and queued acceptance after a second transfer. Acceptance reservation
does not establish the final artifact check or accepted Task transition. Fresh
RepairDispatch on the original owner's binding is qualified in the next source
increment below; the complete native O7 cycle remains unqualified.

Full Windows and Ubuntu CI for source
`4ce438b10d5facb82188feea34daeba36bd8216f` passed in
[37192501762](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37192501762),
verified on 2026-10-04 at 09:49 UTC.

### Successor repair continuation — qualified source increment, 2026-10-04

After explicit A→B→C entry transfer, C can queue a fresh correction on A's
retained ready binding. Attempt owner and feedback recipient A, retained review
sponsor, disposition decision manager B, and current automation manager C stay
distinct. The validator preserves the decision manager's committed correction
reason while checking the original reviewer provenance separately. Historical
manager slots are checked across the complete transfer lineage before a new
slot is admitted. Existing direct slots are retained for readback; an exact
queued unsent automation slot uses its explicit transfer continuation.

The queued effect retains its captured GM epoch. A designation change away from
C and back to C invalidates that old effect without overwriting its Operation or
duplicating its slot. Canonical current Attempts are derived from unreleased
Attempt records rather than an absent Task column.

Warnings-denied production Clippy passed (18.96 s). The real-Store A→B→C repair,
duplicate-consumption and C→B→C epoch regression passed (0.11 s execution;
44.10 s including compilation), followed by debug build (39.77 s), verified on
2026-10-04 at 10:19 UTC. Production source stayed unchanged after Clippy; only
the fixture's canonical request read was corrected. All source stayed unchanged
during the final gate. Luna's bounded authority audit found no remaining blocker.
Built candidate SHA-256 is
`C4A28DA65495FAC229EE1202DA7F4E75EA642F4B909F6F375DE0CED07102EF9E`.
The gate qualifies retained Store admission and pre-effect checks. It does not
establish native delivery or the full O7 cycle. The previous GM chat is
unnecessary for handover and continuation.

Full Windows and Ubuntu CI for source
`7061e455f04a76bfaa19699edba7f58c14a27748` passed in
[37195035427](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37195035427),
verified on 2026-10-04 at 10:40 UTC.

### Installed controller and fresh OpenCode startup — 2026-10-04

The qualified `7061e45` candidate above was installed at
`C:\Users\kleym\.cargo\bin\swarm.exe` at 10:25 UTC. Installed byte hash and
`swarm --version` passed readback. The prior R6 binary was preserved for rollback;
no active controller process existed, and no service, PATH, hook, or provider
credential was changed by this update.

Fresh native run `f5ad880d-c691-4134-82e3-4784c9384158`, launch
`8f988bea-bec1-4882-aa67-60dcd37914e2`, progressed beyond the previous Windows
path failure. Retained native receipts report Bun PID 55076 with the pinned
executable identity, owner `ready`, then a completed `stdin-eof` stop. The
controller never retained a ready/native MCP proof and the harness timed out at
`service_start`. The Operation remains unknown and is retained without replay.
The source review found that parent-side startup errors lost their stage before
normal helper EOF cleanup. The next increment retains a bounded stage/code
observation before that cleanup. At this historical checkpoint, C16 was expected
to check the change; the subsequent C17 result is summarized in Current State.
The historical C15 cause remains unknown. No model request was made. All six protected
Codex processes retained their birth identities.

### Prior C7/C8 evidence

C7 full Rust CI run 37153513585 passed all Ubuntu and Windows steps for exact CI commit 8570dae7f478b6dd2b604727b34c285a86ee9acc. C8 run 37157609062 exposed projection fixtures missing 002workspace; repair 7d518ef4edb84c5e8ce677fafa778de914abed30 passed the focused projection filter 6/6 and full Ubuntu/Windows CI run 37158328828. C4's 221 Rust tests apply only to 2607c8858e573ae40459c27d76d8ae9e1ca9f8fc.
## Critical path to a local reviewed delivery

```text
O1 manager ownership and admission + O2 durable intake and readback
    -> productive launch and owned workspace
    -> local applied submission -> assigned review -> return/correction
    -> fresh review -> exact acceptance (optional local publication)
```

GitHub, scripts, cron and external hooks are optional to the local audit path.
The first priority is a complete manager-owned local cycle; broader integrations
and qualification follow it.

## Remaining delivery blocks

1. **Peer autonomy beyond consultation — Partial.** C6 authors passive watches
   for `operation_terminal`, `contract_revision_changed`,
   `task_revision_changed`, `attempt_disposition_changed` and
   `exact_deadline_reached`, plus `coordination.sync_integration` and recomputed
   `swarm.overlap.check`. Broader integration-cell negotiation, assumptions,
   negotiated contracts and unsupported watch predicates remain. Durable
   Concilium rounds, their retained scoped reads and their shared event-bus
   translation are integrated in source as described in Current State; current
   compilation and runtime qualification remain pending.

2. **O1 manager-owned automation actions — Partial.** Owner-scoped configuration get/preview/apply/explain, WorkDispatch, ReviewDispatch, bounded ReviewDisposition, typed manager authority, and shared manual/automatic semantic slots are wired. C10 publishes the launch-parent dispatch schema and Store gate. C11 adds RepairDispatch and acceptance consumers on typed ledgers/cursors, with same-slot reuse, GM epoch and byte-verification checks, structured reviews, and manager history/visibility. C12 publishes owner-sponsored acceptance to an independent GM. C13 implements automated accepted-candidate publication; live publication qualification and GitHub projection remain gaps.

3. **O2 durable intake and shared monitoring — Partial.** The dispatcher consumes bounded shared observations through generic source/kind rules, safe adapter projections, durable cursors and shared journal readback. Current source covers messages, coordination answers, Operation failures, native acceptance/terminal outcomes, ScriptRun results, host termination and native MCP failures. Taskless sources use the same kernel without invented Task records. Participant credential issuance and authenticated configured/connect readback are implemented. Additional adapter coverage and fresh native end-to-end qualification remain partial; see Current State above.

4. **Productive launcher, workspace ownership and complete local O7 cycle — Partial.** Queue/context/overlap projections, launch preview, async lease, exact Task claim, agent.open, Participant context, WorkDispatch admission and C10 launch-linked task.dispatch gate are implemented. The same-Task/Attempt Store return/correction/fresh-review/exact-B-acceptance chain now passes as recorded above. Native service capability and productive correction delivery remain unqualified. Workspace enforcement remains database-backed, with a separate retained-proof service-departure fence. Full native workflow and publication qualification remain.
5. **O3 Rust adapters and provider lifecycle — Partial.** Rust OpenCode V2 and
   Zed paths exist alongside JavaScript/Python module bridges. C10 publishes the
   exact provider credential gate; `stored_unverified` denotes credential
   metadata only, not key validity or model consumption. Earlier failed runs produced no new credential observation. Native plugin loading, callable tools and provider/model capability remain unqualified; current native evidence is in Current State above.
6. **O4 Git/GitHub intake, work pools and distribution — Partial.** Local
   non-force Git ref publication exists as a first slice, with live Git/remote
   qualification pending. GitHub Issue GET intake/reconciliation and work-pool
   handling exist. Manual managed-label writes now have the bounded source
   flow recorded above. Broader GitHub write effects, PR/check effects and shared
   bounded Git-scope inspection remain.

7. **O5 Rust hook observation — Partial.** Authenticated bounded HookCommit
   intake and private hook issuance/install/readback exist in source. Runtime
   callback retry now shares exact source/commit deduplication. Further runtime
   integrations and end-to-end native qualification remain.

8. **O6 optional script bundles and runner — Partial.** Immutable bundle
   registration/revision, interpreter capture, activation, direct authorized
   run, owned-process handling, bounded output and durable readback exist.
   One invocation-scoped Task-owner message grant is now implemented with
   completion-time revocation checks and a retained child Operation. Broader
   declared effects and full native qualification remain. Event triggers use
   the common bus and may select any bounded source/kind, including future
   kinds; current source visibility and action rights remain separate checks.

9. **O8 cron/typed rules and O9 shared Goal progression — Partial.**
   Manager-owned calendar CheckRuns now share the legacy scheduler, entry
   enablement, durable occurrence identities, normal CheckRunner and explicit
   transfer/restart paths. The current source gates are recorded above.
   Manual `schedule.run_now` admission with disabled recurrence now passes the
   Store and MCP source checks recorded above. Closed applied-submission review
   rules and selected shared Goal continuation are implemented as the current
   bounded slices above. Remaining rule types and editor/client parity remain.
   Task-scoped Goal reminders exist independently of progression. OpenCode has
   a controller-recorded Goal; its continuation path still needs native workflow
   qualification, and native Goal APIs in other adapters remain incomplete.

10. **O10 cross-contract parity and O11 integrated qualification — Partial /
    qualification pending.** Current source gates and retained native evidence are
    recorded in Current State above. No listed CI run proves live MCP/model
    capability or the complete manager-owned workflow.
The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
