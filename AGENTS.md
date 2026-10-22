# Working on Eliot Swarm Controller

Read the owner's current task and the short
[architecture](docs/agent-operations/architecture.md) first. Use
[implementation](docs/agent-operations/implementation.md) as the single work order
and source-owner map. [Findings](docs/agent-operations/refactoring.md) explain current
excess; [status](docs/implementation-status.md) separates implemented facts from
plans. Historical specifications, audits and Claude scripts are evidence for the
manager, not additional worker instructions. The owner's latest explicit decision
wins.

## Start with the requested code

A runtime task starts by reading the exact current callers, owners and symbols and
editing them. Do not produce another design report, issue series, compatibility plan
or framework before the requested source change. An audit/documentation task stays in
that scope.

The manager gives each coding worker one compact slice:

- observable behavior and completion condition;
- exact source owners and symbols;
- retained requirements and unsupported/uncertain cases;
- old callers, config, fixtures and branches to delete;
- one named donor/native mechanism when relevant;
- minimal affected format/Clippy/build command;
- report: behavior, deletions, net owned-code change and unresolved facts.

Do not send a worker the full Issue history, audit archive or repository-wide roadmap.
Use current symbols and paths, not stale line numbers. Changed product meaning creates
one current Task/Attempt revision or a new slice, not an addendum chain.

## Preserve autonomous behavior

Governor and Manager are agent roles. A direct or explicit command is an attributable
Store action, not a mandatory human click. Removing a workflow, journal or policy
layer is valid only when the same slice connects the retained positive path through an
existing owner.

For every removed mechanism, name and implement:

```text
trigger/current fact
-> authorized ordinary command
-> state/effect owner
-> readback/result
-> next automatic action or one addressed Governor decision
```

Do not replace automation with “the operator handles it”, “the manager decides” or
“unsupported” unless the remaining case genuinely lacks owner authority, credentials
or product intent. Routine route choice, permission handling, review dispatch,
all-findings correction, acceptance, publication and recovery should run under the
configured agent policy. Unknown effects remain visible and block only their exact
resource while unrelated work continues.

A task for an implementation agent must state what to build, not only what is
forbidden. Prefer the maintained native capability. When a harness cannot perform the
requested semantic action, fail at routing with the exact unsupported scope and keep a
working fallback only when policy selected one; never silently weaken guarantees.

## Keep ownership and slices small

One manager owns one worktree and integrates its candidate. Shared contracts, Store
state transitions and files touched by several slices stay with that manager. Delegate
narrow non-overlapping leaf edits. Do not split one atomic transition across workers
or give several workers independent Git-index ownership.

Use one current implementation and storage format per retained responsibility. Remove
replaced callers, config, compatibility branches and tests with the new path. A
semantic product action may have one real consumer. A shared implementation
abstraction requires two current consumers and must delete both duplicates in the same
change.

Do not add a crate, service, registry, workflow language, generic repository,
compatibility facade, generated protocol family or wrapper for hypothetical future
use. Prefer existing packages and maintained native APIs.

## Do not persist a view

Queue, lane, dashboard, provider status, capacity and work pool are projections over
current Tasks, Operations, bindings, workspaces and source observations. Do not turn
them into another state machine, cursor backlog or database.

A blocked item stays in its owning domain; the next pass queries current state.
Notifications and timers are wake hints. Lost/coalesced wakeups are recovered by a
bounded current-state query, not per-domain daemons.

Capacity counts actual reserved roots/effects, not API calls. Readback, result,
refresh and reconcile do not acquire model capacity. Route fallback is an explicit
ordered policy; never silently substitute provider, model, effort, permissions or
workspace policy.

## Effects, recovery and evidence

Keep admission atomic and external I/O outside Store transactions. Use native durable
command/message identity and readback before inventing local retry. Never replay an
uncertain prompt because a socket closed or an ACK was lost.

Common adapter code may own authenticated Eliot IPC and exact already-formed Store
deliveries. It must not become a universal effect journal, native session store,
scheduler or recovery coordinator. Vendor effect recovery stays in the selected
adapter; Store remains Task/Operation authority.

Check caller and exact object rights at the boundary. Package versions, document
hashes, compiler paths and historical editions do not grant execution authority.
Keep every result attached to its original Task, Attempt, Operation, candidate and
origin boot.

## Requirements, prompts and review

Preserve the current per-requirement evidence plan and candidate-specific acceptance
semantics. A compact TaskPacket renders goal, requirements, selected sources, write
boundary, deliverable and verification for the model; Store keeps identity and
verification policy. Do not put controller proof JSON into the prompt, and do not
remove executable requirement criteria as “bureaucracy”.

Return all actionable review findings in one correction. Preserve current missing or
inconclusive reviewer replacement. An unchanged source can gain relevant evidence; a
fresh ID or unrelated edit is not progress. Bounded no-progress escalates once to the
Governor agent with concrete choices, not repeatedly to a human.

## Integrate and verify

Publish directly to `main` when the owner requests it. Review the combined
caller-to-result path and delete superseded code before completion. One independent
verifier checks the changed boundary after manager integration.

During construction run formatting and the minimal affected warnings-denied Clippy or
syntax check from [scoped checks](docs/ci-scoped-builds.md). Do not build a broad test
campaign around an incomplete product path. Run focused connected cases when the
slice closes and full installed/native/load qualification when the product path is
ready.

A slice is complete only when new behavior is connected, the old path is absent,
uncertain outcomes remain honest, routine automation has not been replaced by human
work, and owning docs/status are updated. Report net production Rust, JS and glue
separately from tests. Keep one implementation queue.
