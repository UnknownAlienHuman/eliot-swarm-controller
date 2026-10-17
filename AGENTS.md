# Working on Eliot Swarm Controller

## Read the active contract

Start with the owner's current task, [V3 architecture](docs/agent-operations/README.md),
and the [current refactoring map](docs/agent-operations/refactoring.md). Read only
the relevant architecture/module sections and source for the assigned slice.
[V18](docs/agent_swarm.md) and the [module contract](docs/agent_swarm.module-contract-v2.md)
retain the shared Task/Attempt/Operation, ownership and recovery invariants.
V3 controls the specified refactoring changes. Historical audits and qualification
receipts describe their recorded revisions; check current call sites before
reopening a finding. Frozen policy editions and retained Attempts keep their identities.

## One manager, one worktree

One manager owns the branch, mutable worktree and integrated candidate. Delegate
small, non-overlapping implementation or review assignments in that worktree.
Each assignment names the behavior, source owner, affected files, replacement/deletion
and completion condition. Writers do not run Cargo or publish fragments. The manager
reviews each returned diff and integrates the complete producer-to-consumer path.

## Keep one implementation of each responsibility

- Store owns durable admission, rights, reservations, Operations and committed events.
  Native/network I/O runs outside its short writer transactions.
- Use the existing contracts, clients, process primitives and schema installer.
  Extract shared DTOs into their existing contract owner; workers and adapters stay
  independently compiled. Do not add host-to-worker or host-to-adapter dependencies.
- Use the required donor at its actual protocol boundary. A library reference in
  documentation or a transitive lockfile entry is not integration. Connect the real
  caller and remove the replaced mechanism in the same slice.
- Return captured results through native input identity or the V3 serialized-turn
  contract. A missing optional parent pointer alone is not a result rejection.
  Keep actual ambiguity, stream gaps and missing bytes explicit; never replay an
  uncertain prompt to manufacture a result.
- Implement the V3 direct launch path with asynchronous native MCP observations;
  retain ordinary workspace, Task, binding and caller checks.
- Keep one database writer and use pooled read-only snapshots for migrated query
  families. Domain collections migrate to indexed tables through the existing
  Store schema/data owners; reads never perform migration or advance durable cursors.
- Preserve protocol-specific identity bytes and historical decoders. Use the
  [schema/digest rules](docs/agent-operations/architecture.md#schema-and-identity-boundaries)
  rather than global replacements of serde attributes or hash validators.
- New abstractions need concrete consumers and a deletion. Extend an existing
  package for a small primitive. Keep adapter-specific journal/recovery semantics
  with the adapter; share mechanics already owned by `swarm-process`.
- Keep new integration suites/fixture executables outside production `src/`.
  Move existing suites alongside the relevant extraction without exposing private
  production APIs solely for test access. Small unit tests may remain next to code.

## Source first, scoped verification

Complete the connected source slice before qualification. The manager runs scoped
formatting and minimal warnings-denied Clippy once after integration, using an
external shared target and actual reverse dependencies from Cargo metadata.
Follow [scoped checks](docs/ci-scoped-builds.md) and the existing `just` recipes.
Never use root `cargo fmt --all`: it includes preserved donor sources. Writers run
no Cargo; avoid repeated full builds, `cargo clean`, test campaigns or native/model
qualification during source preparation. Product tests and live qualification are
the later completion phase. Documentation-only work uses the existing documentation
gate and needs no Rust build.

Update the current contract/status entry when behavior changes. Keep receipts in
the existing evidence location and link their source/scope. Do not append each
attempt log to normative specifications or create parallel documentation-only
handoff PRs for the same work.
