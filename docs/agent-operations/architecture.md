# Eliot Swarm Controller — Architecture

This manifesto defines the product and constrains its code. Keep it in the lead
developer's context. [Implementation](implementation.md) owns source changes,
algorithms and donor choices; [status](../implementation-status.md) records what
exists. This is the target, not a claim that the refactor is complete.

## Purpose

Eliot carries assigned work through native coding agents to an attributable result,
independent review, correction and acceptance. It tells the Governor who is working,
where, on what, with which native session, process, worktree and GitHub branch, and
what is waiting or broken. Activity, code volume and elaborate evidence machinery
are not measures of completion.

## Product boundary

The compact core owns assignments, Operations, agent control, results, review,
acceptance, scheduling selected helpers and local observation. Management is manual
by default. A manager may enable helpers that invoke the same authorized actions as
direct commands.

GitHub publication, hooks, scripts, remote access and diagnostics are optional
integrations. Council rounds, generic agreement/ratification engines, automatic
Issue-label publication and compatibility with abandoned internal experiments are
not core requirements.

## One semantic language, thin adapters

The controller gives every harness the same semantic actions. An adapter translates
those actions into native calls and translates native facts back. Vendor method
names, payloads, protocol versions, storage formats and recovery details stop at
that boundary.

The common language covers opening or resuming a session, assigning work, sending
or steering input, replying to a pending request, configuring supported options,
controlling a native Goal, moving eligible work to background, cancelling an exact
turn or queued input, recovering a known session after owner loss, refreshing safe
native facts, retrieving output, reconciling an uncertain action and releasing owned
resources.

Capabilities describe which semantic actions and scopes an adapter implements.
They route commands; they never grant rights. Store authorization remains the only
permission boundary. Package versions, build IDs and schema inventories are not a
behavioral API and must not become a permanent compatibility matrix in the core.

Each native session or shared service scope has one state owner that orders its
commands and events. A shared native service connection is not duplicated per hosted
session. Shared adapter plumbing may own authenticated Eliot IPC, hello, command
pulling and re-delivery of already-retained Eliot records. It must not own vendor
state, native recovery policy, Task state or another scheduler.

## Goals

Harness Goal is the vendor's native mechanism and the default when available.
Eliot Goal is an optional Eliot-side plugin that sends ordinary new or repeated
messages. The Governor may use either or combine both, with different objectives.
Their controls are independent. Neither a native Goal completion nor a repeated
Eliot message accepts a Task.

## Progress and results

An authorized request can start assigned work directly. Preview is optional.
A prepared workspace and usable native session permit the first input; optional MCP
discovery, telemetry or diagnostic probes cannot block it.

Every input has one admitted Operation. The adapter associates native turns and
output with that Operation even when the harness omits repeated parent IDs. Unknown
delivery or output stays unknown and is read back; it is never resent by guess.
Complete result bytes are retained once and remain attached to the original Task,
Attempt and candidate. Late results do not change ownership or acceptance.

Stopping future continuation, cancelling a turn, removing queued input and closing
a process are different actions with explicit subjects and observable outcomes.

## Native effects and Store delivery

Use the harness's durable command, inbox, message or turn identity and native
readback whenever it exists. A transport request ID is not automatically an
idempotency key. When the harness lacks safe replay/readback, the adapter retains the
smallest exact prepared effect needed to classify the outcome; it never grows a
second Task store.

Delivery back to Eliot is a different responsibility. One small durable outbox may
retain exact outcome, observation and result envelopes until Store acknowledges
them. It does not decide native retries, reconstruct vendor sessions or interpret
Task completion. There is no universal journal of every provider event.

## Observation

Observation is core functionality, not optional telemetry. The Governor can read
local state at any time without querying the provider harness or starting a model.
Eliot correlates assignments, bindings, native sessions and provider-local
JSON/JSONL/SQLite with OS processes, worktrees and GitHub refs. Unassigned and
unmatched native sessions remain visible.

Local collection continues through loss of the native control connection. Each
source has its own observation time, coverage and errors. Process liveness, native
activity, connection health, Task completion, Git state and account quota remain
separate facts. Unknown is not converted to idle, dead, complete or zero.

## State, trust and recovery

Store is the single durable authority for admitted work, rights and outcomes. Each
external effect has one retry/readback owner. Commit related local state atomically;
perform native, file, Git and network I/O outside Store transactions.

Authenticate participants and protect credentials at access boundaries. Use stable
IDs, revisions and native identities for relationships. Hashes are reserved for
credentials, selected executable/package bytes and retained artifact content. They
are not permissions, approvals, runtime health checks or compatibility systems.

A restart reconstructs state from retained Operations and current local sources. An
uncertain external action blocks conflicting work until readback or explicit
disposition. It does not justify retaining every historical executor forever.

## Build the smallest complete slice

Implementation proceeds vertically from one admitted command through its selected
adapter to readback/result, while deleting the replaced path. The manager reads the
historical evidence and gives workers a compact current slice; workers do not receive
the entire archive. Shared contracts and state transitions have one editor.

Code comes before another planning layer. A new abstraction must have current users
and remove concrete duplication in the same slice. During construction use only the
minimal affected format/Clippy checks; broader verification follows the connected
product path.

## Simplicity is a design constraint

Support one current protocol, implementation and storage format per retained
runtime. Preserve only concrete required work through a finite transition, then
delete obsolete producers, readers, fixtures, configuration and compatibility
branches.

Delete unnecessary responsibilities before extracting packages. Reuse an existing
owner or maintained library instead of adding another framework, workflow language,
registry, broker or database. A shared implementation abstraction is justified only
when at least two current callers use the same behavior and the replaced code is
removed in the same change.

## Completion

A manager can assign work, control the chosen harness, observe it locally, obtain
complete output, request independent review, return corrections and accept the
resulting candidate. Native and Eliot Goals work independently. Restart exposes
known outcomes and unresolved work. Replaced paths are absent, and verification
covers the changed boundary rather than recreating a second product around it.
