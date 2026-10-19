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

A runtime task starts by reading the exact current source owners and editing them.
Do not produce another design report, issue series, compatibility plan or framework
before the requested source change. An audit/documentation task stays in that scope.

The manager gives each coding worker one compact slice:

- objective and observable behavior;
- exact source owners and symbols;
- unsupported/uncertain cases;
- old callers, config, fixtures and branches to delete;
- one named donor mechanism/native API when relevant;
- minimal format/Clippy/build command;
- required report: behavior, deletions, net owned-code change and unresolved facts.

Do not send a worker the full Issue thread, old audits, all donor documents or a
repository-wide roadmap. Use current symbols/paths, not stale line numbers. Changed
policy creates a new Task/Attempt revision or slice, not an addendum file.

## Keep ownership and slices small

One manager owns one worktree and integrates the candidate. Shared contracts, Store
state transitions and files touched by several slices stay with that manager.
Delegate narrow non-overlapping leaf work. Do not split one atomic transition across
workers.

Use one current implementation and storage format per retained responsibility.
Remove replaced callers, config, compatibility branches and tests with the new path.
A semantic product action may have one real consumer. A shared implementation
abstraction requires two current consumers and must delete both duplicates.

Do not add a crate, service, registry, workflow language, generic repository,
compatibility facade, generated protocol family or wrapper merely for future work.
Prefer existing packages and maintained native APIs.

## Do not persist a view

Queue, lane, dashboard, provider status and capacity are projections over current
Tasks, Operations, bindings, workspaces and source observations. Do not turn them into
another state machine, cursor backlog or database.

A blocked item stays in its owning domain; the next pass queries current state.
Notifications and timers are wake hints. Lost/coalesced wakeups are recovered by a
bounded current-state query, not by per-domain daemons.

Capacity counts actual roots/effects, not API calls. Readback, result, refresh and
reconcile do not acquire model capacity. Route fallback is an explicit ordered list;
never silently substitute provider/model/effort.

## Effects, recovery and evidence

Keep admission atomic and external I/O outside Store transactions. Use native durable
command/message identity and readback before inventing local retry. Never replay an
uncertain prompt because a socket closed or an ACK was lost.

Common adapter code may own authenticated Eliot IPC and exact already-formed Store
deliveries. It must not become a universal effect journal, native session store,
scheduler or recovery coordinator. Vendor effect recovery stays in the adapter;
Store remains Task/Operation authority.

Check caller/object rights at the boundary. Package versions, document hashes,
compiler paths and historical editions do not grant execution authority. Keep every
result attached to its original Task, Attempt, Operation and candidate.

## Integrate and verify

Publish directly to `main` when the owner requests it. Review the combined
caller-to-result path and delete superseded code before completion. One independent
verifier checks the changed boundary after manager integration.

During construction run formatting and the minimal affected warnings-denied Clippy
or syntax check from [scoped checks](docs/ci-scoped-builds.md). Do not build a broad
test campaign around an incomplete product path. Run focused connected cases when the
slice closes and full qualification when the product is ready.

A slice is complete only when new behavior is connected, the old path is absent,
uncertain outcomes remain honest and owning docs/status are updated. Report net owned
Rust + JS + glue separately from tests. Keep one implementation queue.
