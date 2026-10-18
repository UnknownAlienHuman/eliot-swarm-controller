# Working on Eliot Swarm Controller

Read the owner's current task and the [architecture](docs/agent-operations/architecture.md)
before planning. Keep that short manifesto in the lead developer's context.
Use [implementation](docs/agent-operations/implementation.md) for the single work
order, source owners and donor choices; read the relevant source before editing.
[Findings](docs/agent-operations/refactoring.md) explain current excess and
[status](docs/implementation-status.md) records revision-specific evidence.
Historical specifications are not additional requirements.

## Implement the requested responsibility

- An audit or documentation task stays within that scope. Start runtime changes
  when the owner asks for implementation, not to make a preparation report look complete.
- Use one implementation and current format per retained responsibility. Remove
  replaced callers, configuration, compatibility branches and tests together.
  Handle identified required data through the implementation's finite transition.
- Reuse existing packages and donor APIs. A new abstraction must replace a concrete
  mechanism. File moves, wrappers, vendoring and language changes alone do not simplify it.
- Keep admission atomic, external I/O outside Store transactions, and each result
  attached to its original work. Check actual caller/object rights at the boundary.
  Document hashes and historical editions do not grant rights.

## Integrate and verify

One manager owns one worktree and integrates the branch. Delegate narrow,
non-overlapping assignments and review the combined caller-to-result path.
Coordinate shared files and builds. Follow the owner's chosen agent models.

Use the existing [scoped checks](docs/ci-scoped-builds.md) for changed behavior and
its dependencies. Documentation needs content/link checks. Stop when the relevant
risk is checked; do not start unrelated build, native or model campaigns.

Update the owning document and report the behavior and net owned-code change,
including SDK drivers/glue and separating tests. Keep one implementation queue;
do not add a parallel program, approval stage or compatibility registry.
