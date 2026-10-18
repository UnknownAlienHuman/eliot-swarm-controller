# Working on Eliot Swarm Controller

Start with the owner's current task and the [refactoring map](docs/agent-operations/refactoring.md),
then read the source and contract for that responsibility. An audit/preparation
request stays an audit: update findings and the reduction plan; do not silently
start runtime implementation or a build campaign. Historical specifications and
qualification logs describe their revisions, not cumulative instructions.

## Reduce responsibilities before extracting code

- Keep the compact product: manager assignments, agent execution, attributable
  results, independent review, acceptance, observation and selected Goal continuation.
  The [communication contract](docs/agent-communication-program.md) owns peer messaging.
- Give each responsibility one implementation. Reuse existing contracts, Store,
  clients and process primitives; keep worker/adapter implementations out of the host's
  dependency graph. A new abstraction or donor integration needs a real caller and
  a replaced mechanism. Do not implement an unrelated feature to complete a donor list.
- Support one current executor and format. Old experiments do not create permanent
  compatibility obligations. For actual data that must survive, use the
  [finite transition](docs/agent-operations/refactoring.md#retire-executors-without-losing-history),
  then remove the old path. Do not add fallback runtimes, aliases or dual writes by default.
- Keep Store admission and changes atomic; native/network I/O stays outside short
  writer transactions. Authorized reads use bounded snapshots. Results remain tied
  to their admitted Operation/turn; an uncertain input is not replayed to obtain an answer.
- Check the actual caller, candidate, binding and process being acted on. Document
  hashes, checkout/compiler equality and archive formats are not another rights system.
  Keep credentials private and authenticate late results against their original work.
- Count net owned code across Rust, SDK drivers and glue, with tests separately.
  File moves, minification, vendoring and a new DSL do not remove a responsibility.

## Integrate and verify the requested change

One manager integrates a branch; delegate non-overlapping file assignments and
review the combined caller-to-result path. Coordinate shared files and builds.
For source changes, use the existing [scoped checks](docs/ci-scoped-builds.md) for
the affected behavior and dependencies; avoid repeated full builds and blanket
formatting of vendor trees. Documentation changes need link/content checks, not Cargo.

Update the owning document. Keep one work queue in the refactoring map and
revision-specific results in the existing status/evidence location. Do not create
another approval stage, compatibility registry or parallel implementation program.
