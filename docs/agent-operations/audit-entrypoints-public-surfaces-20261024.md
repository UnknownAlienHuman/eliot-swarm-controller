# Entrypoints and public surfaces — source-audit checkpoint

Status: audit-evidence

Inspected source: merged `main` at `797ea93f1cfcaad8c8dde2629d666e64efb131c2`.
Documentation branch changes do not alter the inspected Rust sources.
Current authority: [documentation map](documentation-map.md),
[source-audit coverage](source-audit-coverage.md), [architecture](architecture.md),
[implementation status](../implementation-status.md). Not worker instructions.

Machine inventory: [`source-surface-inventory.md`](source-surface-inventory.md) and
[`source-surface-inventory.json`](source-surface-inventory.json). The combined source
SHA-256 for the five inspected clusters is
`c55118a3580219f5fd1f4a90082eb51da3bc718e9d8516fac648007859d3d9f7`.

## Scope actually read

- root `src/{main,lib}.rs`;
- `crates/swarm-cli/src/{main,lib}.rs` and its delegation/method-construction path;
- all five `crates/swarm-kernel/src/*.rs` files plus Cargo consumers;
- all `crates/swarm-bus/src/**/*.rs` files plus active installer/package consumers;
- `crates/swarm-telemetry/src/lib.rs` plus direct producer/observer consumers;
- the host's `legacy_optional_workers` and `legacy_worker_demand` activation boundary;
- current ScriptRun executor-selection evidence needed to distinguish coordination from execution.

This checkpoint does not claim that every downstream Store method or every runtime case
was qualified. It records confirmed boundaries, refuted suspicions and the remaining
exact chains.

## CP-001 — the root compatibility path is active, not dead

**Confirmed.** `src/lib.rs` is a compatibility facade over `swarm-kernel-host`.
`src/main.rs` is a `swarm-host` wrapper that locates and starts the sibling
`swarm-kernel-host`. The public `swarm` CLI delegates its host-owned command family to
that sibling wrapper. Integration tests still import the root crate, and build/
qualification tooling explicitly expects the three artifacts `swarm`, `swarm-host`
and `swarm-kernel-host`.

**Consequence.** An implementation agent must not delete the root package or wrapper as
“legacy” merely because the real host lives in another crate. Retirement requires one
finite cut which updates the public CLI delegation, integration-test imports, packaging,
qualification provenance and installed artifact layout together. Until then it is an
active compatibility boundary, not a second Store authority.

**Mapped prevention:** PF-10, PF-11, PF-16. No closure claim.

## CP-002 — the public CLI is a monolithic method surface, but not a Store owner

**Confirmed.** `swarm-cli` contains 33 top-level command variants and approximately
2,187 lines in `main.rs`. Host/worker commands delegate to `swarm-host`; MCP delegates
to `swarm-mcp`; the remaining commands construct method/parameter values and use the
thin authenticated IPC client in `lib.rs`. The crate has no Cargo dependency on the
root controller, Store or `swarm-kernel`.

The generic `Call` command accepts a method string and JSON parameters. The CLI performs
only narrow local handling for private source setup and retry-safe hook emission; the
host remains responsible for method existence, authorization, object rights and
mutation/read classification.

**Risk, not yet a defect.** The large manual command-to-method match can drift from the
host method registry, MCP catalog and public/private method policy. The generic call
path means the enum alone is not a closed public-method inventory. A later audit must
generate and compare the exact CLI method set, generic-call policy, host dispatch table,
method policy and MCP catalog at one revision. Do not “fix” this by moving authority or
Store logic into the CLI.

**Mapped prevention:** PF-02, PF-08, PF-11, PF-12. Status: open boundary inventory.

## CP-003 — `swarm-kernel` has one production package consumer

**Confirmed.** The package contains one bounded writer actor/lifecycle implementation
and provider-neutral Task, acceptance, review and dispatch validators. The inspected
modules contain no database, provider, adapter or process implementation. Cargo
metadata shows one production dependent: `swarm-kernel-host`; direct Rust token users
are all inside that host plus package tests.

**Consequence.** No duplicate authority was found: Store still performs authorization,
SQL and effect ownership around these pure predicates. However, this is currently a
one-consumer extraction containing both generic actor primitives and domain policy. It
cannot be used as proof that a reusable “kernel framework” exists. A refactor packet
must either name a second current consumer and remove duplicate implementations in the
same slice, or treat the package as a host-owned internal boundary and evaluate whether
inlining/splitting reduces code without changing semantics.

**Mapped prevention:** PF-10, PF-11, PF-12. Status: architectural disposition open; no
runtime bug asserted.

## CP-004 — Bus routing and ScriptRun execution are distinct responsibilities

**Refuted suspicion:** the inspected paths do not show two active script interpreters.
`swarm-bus` is a pure payload-free selector/router used by `swarm-kernel-host` and
`swarm-scripts`. Its active `swarm-bus-dispatcher` binary enrolls a scoped consumer and
polls Store events that may create ScriptRuns. The host's legacy optional-worker slot
for `Scripts` starts `Store::supervise_scripts`, which reconciles durable ScriptRuns.
Current ScriptRun execution requires a pinned standalone executor; the retained
compatibility launcher delegates to that executor.

**Confirmed remaining problems.** The coordinator is still named and grouped as a
legacy worker, and a run may be durably admitted before an absent executor pin is
reported as `SCRIPT_EXECUTOR_UNSELECTED`. The finite cut and all restart/cancel/readback
paths remain part of AR-071–078. The bus dispatcher, ScriptRun reconciler and standalone
executor must stay separately named in task packets so an agent does not merge their
authority or delete one based on the shared word “worker”.

**Mapped prevention:** AR-071–078; PF-05, PF-08, PF-11, PF-14. Status: mixed/open.

## CP-005 — telemetry is diagnostic-only in the inspected boundary

**Confirmed.** `swarm-telemetry` has two production dependents:
`swarm-kernel-host` and `swarm-observer`. It exposes a bounded, closed diagnostic
record schema and a non-blocking stderr writer. The public schema has no Store
acknowledgement, business transition or arbitrary credential/path field. Optional text
requires both a configured redactor and a live capture policy; the default data path is
metadata-only.

**No duplicate durable state or execution authority was found** in the inspected
package. Its 1,719-line single file is a maintainability concern, not evidence of a
second observability database. The remaining audit must trace producer loss accounting,
observer line parsing, live policy refresh, persisted diagnostic retention/disclosure
and shutdown behavior end to end. A split is valid only if it preserves the same closed
schema and has current consumers; it must not create a logging service or business
event bus.

**Mapped prevention:** AR-142–159; PF-02, PF-08, PF-11, PF-12. Status: source boundary
partially traced; disclosure/retention chain open.

## CP-006 — size is not itself a correctness finding

The inventory found large surfaces (`swarm-cli/main.rs`, `swarm-kernel` policy files and
`swarm-telemetry/lib.rs`). This is useful for review planning but does not prove a bug,
authority duplication or a valid package split. Any change must be justified by exact
current consumers and net deletion, not by line count alone.

## Confirmed non-findings

- `swarm-cli` does not import the Store, root controller or kernel implementation.
- `swarm-kernel` does not own SQLite, provider calls or native process effects.
- `swarm-bus` routing does not own durable events, cursors or acknowledgements.
- `swarm-telemetry` is not a business-event or Store-acknowledgement channel.
- The bus dispatcher, ScriptRun reconciliation and standalone ScriptRun executor are
  different stages; no second interpreter was established by this audit.

These statements are limited to the inspected revision and source paths.

## Remaining exact chains before this cluster is closed

1. Generate the complete CLI command/method map and compare it with host dispatch,
   `method_policy`, MCP catalog and private/internal method sets.
2. Trace every root-facade and `swarm-host` consumer through installed packaging and
   define the finite retirement/data-disposition cut; do not delete it piecemeal.
3. Map each public `swarm-kernel` function to its exact host caller and classify it as
   writer lifecycle, Task policy, acceptance policy, review policy or dispatch fact.
4. Trace bus consumer registration/poll/cursor/action admission through
   `host_bus_supervisor` and Store, including restart and revoke.
5. Trace telemetry emission → stderr observer → parse/redact/policy → persisted
   diagnostic readback, including dropped records, writer failure and shutdown.

Until those chains are recorded, this cluster is `partial source trace`, not complete.
