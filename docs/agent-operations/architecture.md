# Eliot Swarm Controller — Architecture

This manifesto defines the product and constrains its code. Keep it in the lead
developer's context. [Implementation](implementation.md) owns source changes and
donor choices; [status](../implementation-status.md) records what exists. This is the
target, not a claim that the refactor is complete.

## Purpose

Eliot carries assigned work through native coding agents to an attributable result,
independent review, correction and acceptance. It tells the Governor who is working,
where, on what, through which native session and process, in which worktree and Git
branch, and what is waiting, uncertain or broken.

The product is valuable because it preserves identity, ownership and recoverable
outcomes while many agents work concurrently. Activity, code volume, process count
and elaborate evidence machinery are not measures of completion.

## Product boundary

The compact core owns:

- Tasks, Attempts and Operations;
- manager and participant authority;
- route selection and admission;
- workspace/write ownership;
- semantic harness control;
- results, review and acceptance;
- selected schedules and local observation.

GitHub publication, hooks, scripts, remote access and diagnostics are integrations.
Council rounds, generic ratification engines, automatic Issue-label publication,
historical execution profiles and compatibility with abandoned experiments are not
core requirements.

A queue, lane, dashboard, provider status and capacity report are **views over current
facts**. They are not additional workflow entities or databases.

## One semantic language, thin adapters

The controller gives every harness the same semantic actions. An adapter translates
those actions into native calls and translates native facts back. Vendor method
names, payloads, protocol versions, local database shapes and recovery details stop
at that boundary.

The common language covers opening or resuming a session, assigning work, sending or
steering input, replying to a pending request, configuring supported options,
controlling native Goal, moving eligible work to background, cancelling an exact
turn or queued input, recovering a known session, refreshing safe native facts,
retrieving output, reconciling an uncertain action and releasing an owned resource.

Capabilities describe which actions and scopes an adapter implements. They route
commands; they never grant rights. Store authorization remains the permission
boundary. Package versions, build IDs and schema inventories are installation facts,
not a permanent behavioral matrix in Core.

Each native session or shared service scope has one state owner that orders commands
and events. A shared provider connection is not duplicated per session. Common
adapter plumbing may own authenticated Eliot IPC and delivery of already formed Store
records. It must not own provider state, native recovery policy, Task state or a
scheduler.

## Goals

Harness Goal is the vendor-native mechanism and the default when available. Eliot
Goal is an independent plugin that sends ordinary new or repeated messages. The
Governor may use either or combine both, with different objectives. Their controls
are independent. Neither native Goal completion nor an Eliot reminder accepts a Task.

## Routes, provider state and capacity

A route alias names one explicit harness configuration. Automatic fallback is an
ordered list of route aliases chosen by the manager. Eliot never silently changes
provider, model, effort, permissions or workspace policy.

Provider availability is a time-stamped observation, not a permission proof. A fresh
negative fact may hold or reject new work. Absence of a provider fact permits the
first bounded probe. A transient limit may permit one new probe after its retry/reset
time; it does not remain a permanent ban merely because the observing binding ended.

Capacity counts owned roots or genuinely active native work. Readback, result
retrieval, refresh and reconciliation do not consume execution capacity. Admission
checks and reserves capacity in one Store transaction; a second ledger does not copy
the lifecycle of every Operation.

## Progress, native effects and Store delivery

An authorized request can start assigned work directly. Preview is optional. A
prepared workspace and usable native session permit the first input; optional MCP
discovery, telemetry and diagnostic probes cannot block it.

Every logical input has one admitted Operation even when the harness performs several
physical attempts. Use the harness's durable command, inbox, message or turn identity
and native readback whenever available. A transport request ID is not automatically
an idempotency key. Unknown delivery or output stays unknown and is never resent by
guess.

Delivery back to Eliot is separate. Terminal outcomes and result pages may be
retained as exact immutable envelopes until Store acknowledges them. Live
observations are replaceable latest-value checkpoints per source with explicit gaps;
they are not an infinite event mirror. Delivery storage never decides native retries,
reconstructs vendor sessions or interprets Task completion.

Complete result bytes remain attached to the original Task, Attempt, Operation and
candidate. Late results do not change ownership or acceptance.

## Workspaces and concurrency

One manager owns one worktree. A Task's repository-relative path scope becomes the
Attempt's write lease. The lease is the write authority and the concurrency guard;
advisory symbol/interface proposals are not another lock system.

Two overlapping write leases cannot launch. Review-only work requires no write lease.
Intentional overlap is resolved by revising the work split or settling one Attempt,
not by a generic override protocol.

## Observation

Observation is core functionality, not optional telemetry. The Governor can read
local state at any time without querying the provider harness or starting a model.
Eliot correlates assignments, bindings, native sessions and provider-local
JSON/JSONL/SQLite with OS processes, worktrees and GitHub refs. Unassigned and
unmatched native sessions remain visible.

Each native source owns its cursor and coverage. Store owns a separate observation
journal cursor for Governor readers. A source gap, expired cursor or failed scan
remains visible and triggers bounded reconciliation; it is never represented as an
empty fleet.

Process liveness, native activity, connection health, Task completion, Git state, run
usage and account quota remain separate facts. Unknown is not converted to idle,
dead, complete, available or zero.

## Scheduling

Time and wake notifications are hints; durable current state is authority. One
in-process control loop computes the next due time, reacts to Store changes and
performs bounded admissions. Restart or a lost wake recomputes from Store.

Do not create a process, credential, owner protocol, cursor family or polling loop for
each schedule domain. Timers admit ordinary Operations; effect workers and adapters
remain responsible for the actual external work.

## State, trust and recovery

Store is the single durable authority for admitted work, rights and outcomes. Each
external effect has one retry/readback owner. Commit related local state atomically;
perform native, file, Git and network I/O outside Store transactions.

Authenticate participants and protect credentials at access boundaries. Use stable
IDs, revisions and native identities for relationships. Hashes are reserved for
credentials, selected executable/package bytes and retained artifact content. They
are not permissions, approvals, health checks, provider availability or
compatibility systems.

A restart reconstructs state from retained Operations and current local sources. An
uncertain external action blocks conflicting work until readback or explicit
disposition. It does not justify retaining every historical executor forever.

## Build the smallest complete slice

Implementation proceeds vertically from one admitted command through one selected
adapter to readback/result, while deleting the replaced path. The manager reads
historical evidence and gives workers one compact current slice. Shared contracts and
state transitions have one editor.

Code comes before another planning layer. A new abstraction must have current users
and remove concrete duplication in the same slice. During construction use the
minimal affected format and Clippy/build checks; broader verification follows the
connected product path.

## Simplicity is a design constraint

Support one current protocol, implementation and storage format per retained
runtime. Preserve only concrete required work through a finite transition, then
delete obsolete producers, readers, fixtures, configuration and compatibility
branches.

Delete responsibilities before extracting packages. Reuse an existing owner or a
maintained native API instead of adding another framework, workflow language,
registry, broker, proof system or database. A shared implementation abstraction is
justified only when at least two current callers use the same behavior and the
replaced code disappears in the same change.

## Completion

A manager can select work, reserve a non-overlapping workspace, choose an explicit
route, control the chosen harness, observe it locally, obtain complete output,
request independent review, return corrections and accept the resulting candidate.
Native and Eliot Goals work independently. Restart exposes known outcomes and
unresolved work. Replaced paths are absent.
