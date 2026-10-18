# Eliot Swarm Controller — Architecture

This manifesto defines the product and constrains its code. Keep it in the lead
developer's context. [Implementation](implementation.md) owns source changes and
donor choices; [status](../implementation-status.md) records what exists. This is the
target, not a claim that the refactor is complete.

## Purpose

Eliot carries assigned work through native coding agents to an attributable result,
independent review, correction and acceptance. It tells the Governor who is working,
where, on what, through which native session and process, in which worktree and Git
branch, and what is waiting, uncertain or broken. Activity, code volume and elaborate
evidence machinery are not measures of completion.

## Product boundary

The compact core owns assignments, Operations, agent control, results, review,
acceptance, selected helper schedules and local observation. Management is manual by
default. Optional helpers invoke the same authorized actions as direct commands.

GitHub publication, hooks, scripts, remote access and diagnostics are integrations.
Council rounds, generic ratification engines, automatic Issue-label publication and
compatibility with abandoned internal experiments are not core requirements.

## One semantic language, thin adapters

The controller gives every harness the same semantic actions. An adapter translates
those actions into native calls and translates native facts back. Vendor method
names, payloads, protocol versions, storage formats and recovery details stop at
that boundary.

The language covers opening or resuming a session, assigning work, sending or
steering input, replying to a pending request, configuring supported options,
controlling a native Goal, moving eligible work to background, cancelling an exact
turn or queued input, recovering a known session after owner loss, refreshing safe
native facts, retrieving output, reconciling an uncertain action and releasing an
owned resource.

Capabilities describe which actions and scopes an adapter implements. They route
commands; they never grant rights. Store authorization remains the permission
boundary. Package versions, build IDs and schema inventories are installation facts,
not a permanent behavioral matrix in Core.

Each native session or shared service scope has one state owner that orders commands
and events. A shared provider connection is not duplicated per hosted session.
Common adapter plumbing may own authenticated Eliot IPC and delivery of already
formed Store records. It must not own provider state, native recovery policy, Task
state or another scheduler.

## Goals

Harness Goal is the vendor-native mechanism and the default when available. Eliot
Goal is an independent plugin that sends ordinary new or repeated messages. The
Governor may use either or combine both, with different objectives. Their controls
are independent. Neither native Goal completion nor an Eliot reminder accepts a Task.

## Progress and results

An authorized request can start assigned work directly. Preview is optional. A
prepared workspace and usable native session permit the first input; optional MCP
discovery, telemetry and diagnostic probes cannot block it.

Every input has one admitted Operation. One Operation represents one logical input
even when the harness performs several physical attempts. The adapter associates
native turns and output with that Operation. Unknown delivery or output stays unknown
and is read back; it is never resent by guess.

Complete result bytes are retained once and remain attached to the original Task,
Attempt, Operation and candidate. Late results do not change ownership or acceptance.

Stopping future continuation, cancelling a turn, removing queued input and closing a
process are different actions with explicit subjects and observable outcomes.

## Native effects and Store delivery

Use the harness's durable command, inbox, message or turn identity and native readback
whenever they exist. A transport request ID is not automatically an idempotency key.
When a harness lacks safe readback, the adapter retains only the smallest prepared
effect needed to classify the outcome; it never grows a second Task store.

Delivery back to Eliot is separate. Terminal outcomes and result pages may be
retained as exact immutable envelopes until Store acknowledges them. Live
observations are replaceable latest-value checkpoints per source with explicit gaps;
they are not an infinite event mirror. Delivery storage never decides native retries,
reconstructs vendor sessions or interprets Task completion.

## Observation

Observation is core functionality, not optional telemetry. The Governor can read
local state at any time without querying the provider harness or starting a model.
Eliot correlates assignments, bindings, native sessions and provider-local
JSON/JSONL/SQLite with OS processes, worktrees and GitHub refs. Unassigned and
unmatched native sessions remain visible.

Each native source owns its cursor and coverage. Store owns a separate observation
journal cursor for Governor readers. The two are never conflated. A source gap,
expired cursor or failed scan remains visible and triggers bounded reconciliation;
it is never represented as an empty fleet.

Local collection continues through loss of the native control connection. Process
liveness, native activity, connection health, Task completion, Git state, run usage
and account quota remain separate facts. Unknown is not converted to idle, dead,
complete, available or zero.

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

Implementation proceeds vertically from one admitted command through one selected
adapter to readback/result, while deleting the replaced path. The manager reads
historical evidence and gives workers one compact current slice. Shared contracts and
state transitions have one editor.

Code comes before another planning layer. A new abstraction must have current users
and remove concrete duplication in the same slice. During construction use the
minimal affected format and Clippy/build checks; broader verification follows the
connected product path.

## Simplicity is a design constraint

Support one current protocol, implementation and storage format per retained runtime.
Preserve only concrete required work through a finite transition, then delete obsolete
producers, readers, fixtures, configuration and compatibility branches.

Delete responsibilities before extracting packages. Reuse an existing owner or a
maintained native API instead of adding another framework, workflow language,
registry, broker or database. A shared implementation abstraction is justified only
when at least two current callers use the same behavior and the replaced code is
removed in the same change.

## Completion

A manager can assign work, control the chosen harness, observe it locally, obtain
complete output, request independent review, return corrections and accept the
resulting candidate. Native and Eliot Goals work independently. Restart exposes known
outcomes and unresolved work. Replaced paths are absent, and verification covers the
changed boundary rather than recreating a second product around it.
