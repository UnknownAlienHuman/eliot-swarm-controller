# Eliot Swarm Controller — Architecture

Keep this manifesto in the lead developer's context.
[Implementation](implementation.md) owns changes and donors;
[delivery](delivery.md) defines the work cycle;
[status](../implementation-status.md) distinguishes plans from working code.

## Purpose

Eliot enables agents to develop, review, correct and deliver working software with
minimal human involvement. Governor and Manager are agent roles, not human approval
stations. Success is attributable accepted work, not prompts, paperwork or lines.
The Governor can always see who works where, what changed and what needs a decision.

## Product boundary

Core owns Tasks, Attempts, Operations, scoped authority, workspaces, harness control,
results, review, messages, scheduling and observation. GitHub, scripts, hooks, gateway
and diagnostics integrate through those owners. Remove duplicate workflow engines,
not their useful automated behavior. A queue, lane or dashboard is a view.

## One semantic language, thin adapters

Governor issues the same meaningful actions to every harness. Adapters translate
commands, results and native facts; native model loops, tools and subagents stay
native. Unsupported behavior is explicit, not silently approximated. Capabilities
route actions; Store grants rights. One owner controls each native session, including
when several routes share a service.

## Goals

Harness Goal is native and the default where available. Eliot Goal sends ordinary
new or repeated messages. Governor may combine both; their controls remain separate.
Neither completes a Task by itself. Native-manager delegation and controller dispatch
are alternative owners of the initial input, not competing starts of the same work.

## Routes, provider state and capacity

Governor selects routes, budgets, continuation and fallbacks. Policy-approved changes
need no human click; unapproved substitutions are not hidden. Quota, concurrency,
activity and connection are different facts. Reuse suitable sessions and count actual
reserved work. Readback must remain possible when new execution has no capacity.

## Progress, native effects and Store delivery

Enabled policy connects selection, execution, result capture, independent review,
correction and acceptance. Helpers use the same commands as agents. An explicit
command means accountable intent, not manual human execution. Preview is optional.

Separate native execution from returning its result. Reconnect or restart must recover
retained results without replaying input. Unknown effects prompt bounded readback and
one addressed Governor decision, while unrelated work continues.

## Workspaces and concurrency

One manager owns one mutable worktree and its Git operations. Leaf writers receive
non-overlapping edits. Review sees a frozen candidate, not a changing checkout.
Controller leases coordinate Eliot's writers; they do not sandbox unrestricted native
shells. Native permissions remain visible and configurable.

## Messages and coordination

Use one addressed mailbox. Consultation is a message, review is review, and a thread
is grouping rather than a negotiation language. Preserve authentication, scoped
participant access and historical recipients while removing duplicate registries,
cards, ratification and council machinery.

## Observation

Governor reads locally collected provider JSON/JSONL/SQLite, native events, process
identity, actual worktree/branch and remote-ref facts without querying a harness.
Unmatched subjects remain visible. Each source reports freshness and gaps; collectors
run independently of viewers, automation and optional logs. A stale source does not
mean an idle agent or an empty fleet.

## External sources and effects

Collection records external facts; selected policy imports requirements; a separate
Operation performs a write. These steps can all run automatically. Keep accepted
candidate, push, merge and Issue closure distinct. One effect owner handles each
domain, using native identity and readback rather than guessed retries.

## Scheduling

One host loop wakes on changes or due time and admits ordinary Operations from current
state. No model performs heartbeat or bookkeeping. Lost wakes cause a bounded rescan,
not a new workflow database. Routine questions go to configured policy or Governor;
humans resolve only missing authority, access or genuinely undecided intent.

## Remote access

One small gateway maps a remote agent to one Eliot identity/profile. Credentials use
the shared private-file boundary; edge TLS stays external. Gateway failure does not
cancel admitted work. It is not another identity platform or controller.

## State, trust and recovery

Store owns durable work, rights and outcomes. Commit related facts together; perform
external I/O outside transactions. Stable identities and revisions protect scope;
cryptographic checks belong to real credential/package/artifact boundaries, not every
transition. Preserve original authors, candidates and source boots during recovery.

## Build the smallest complete slice

Read the existing owner, implement one usable path and remove its duplicate. Give leaf
agents compact assignments and required sources, not the audit archive. Use minimal
affected Clippy/build checks during construction; qualify connected behavior when
ready. A blocked optional integration must not stop unrelated implementation.

## Simplicity is a design constraint

Use maintained native APIs and existing owners. Extract shared code when actual users
remove duplication. Preserve concrete unsettled work through a finite transition,
then delete obsolete formats and executors. Every removed mechanism needs a working
replacement for retained behavior, not an instruction that the human do it instead.

## Completion

An enabled agent-managed work cycle reaches a verified candidate and selected
publication without routine operator intervention. Native and Eliot Goals coexist;
local observation survives control disconnects; restart recovers retained results and
exposes unresolved effects. Documentation states actual gaps, not unconditional
claims of completeness.
