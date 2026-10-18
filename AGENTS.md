# Working on Eliot Swarm Controller

Read the owner's current task and the short
[architecture](docs/agent-operations/architecture.md) first. Use
[implementation](docs/agent-operations/implementation.md) as the single work order
and source-owner map. [Findings](docs/agent-operations/refactoring.md) explain the
current excess; [status](docs/implementation-status.md) separates implemented facts
from plans. Historical specifications, audits and Claude scripts are evidence for
the manager, not additional instructions for every worker. The owner's latest
explicit decision resolves conflicts.

## Start with the requested code

A runtime implementation task starts by reading the exact current source owners and
then editing them. Do not produce another design report, issue series, compatibility
plan or framework before the requested source change. An audit or documentation task
stays in that scope.

The manager gives each coding worker one compact slice packet:

- objective and observable behavior;
- exact source owners and symbols to change;
- unsupported and failure cases that must remain explicit;
- old callers, configuration, fixtures and branches to delete;
- one named donor mechanism or native API when relevant;
- the minimal format/Clippy/build command for the affected package;
- required report: behavior, deletions, net owned-code change and unresolved facts.

Do not send a worker the full Issue thread, old audits, all donor documents or a
repository-wide roadmap. Use symbol names and current paths, not stale line numbers.
Changed policy creates a new Task/Attempt revision or slice packet; it does not create
an addendum file or hidden correction loop.

## Keep ownership and slices small

One manager owns one worktree and integrates the candidate. Shared contracts, Store
state transitions and files touched by several slices stay with that manager. Delegate
only narrow, non-overlapping leaf work. A weak-model microtask should normally cover
one local responsibility, exact symbols and one observable completion condition; do
not split one atomic state transition across workers.

Use one current implementation and storage format per retained responsibility. Remove
replaced callers, configuration, compatibility branches and tests with the new path.
A semantic product action may be justified by one real consumer. A shared
implementation abstraction requires at least two current consumers and must delete
the duplicated mechanism in the same change.

Do not add a crate, service, registry, workflow language, generic repository,
compatibility facade, generated protocol family or wrapper merely to prepare future
work. File moves, language rewrites and vendoring are not simplification by
themselves. Prefer existing packages and maintained native APIs.

## Effects, recovery and evidence

Keep admission atomic and external I/O outside Store transactions. Use the native
harness's durable command/message identity and readback before inventing local retry
machinery. Never replay an uncertain prompt because a socket closed or an ACK was
lost.

Common adapter code may own authenticated Eliot IPC and an exact outbox of already
retained `module.outcome`, `module.observe` and `module.result` deliveries. It must
not become a universal effect journal, native session store, scheduler or recovery
coordinator. Vendor effect recovery remains in the selected adapter; Store remains
the Task/Operation authority.

Check actual caller and object rights at the boundary. Package versions, document
hashes, compiler paths and historical editions do not grant execution authority.
Keep every result attached to its original Task, Attempt, Operation and candidate.

## Integrate and verify

Publish work directly to `main` when the owner requests it; do not create a pile of
PRs. Review the combined caller-to-result path and delete superseded code before
calling the slice complete. Do not run two full reviews of the same candidate; one
independent verifier checks the changed boundary after the manager integrates it.

During construction run formatting and the minimal affected warnings-denied Clippy
or syntax check from [scoped checks](docs/ci-scoped-builds.md). Do not build a broad
test campaign around an incomplete product path. Run focused connected tests when
the path is complete and full qualification when the product is ready.

A slice is complete only when the new behavior is connected, the replaced path is
absent, uncertain outcomes remain honest, and the owning documentation/status is
updated. Report net owned Rust + JS + glue separately from tests. Keep one
implementation queue; do not add a parallel program or approval stage.
