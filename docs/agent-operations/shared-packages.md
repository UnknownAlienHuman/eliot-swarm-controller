# Shared package boundaries

This is the V3 ownership map, not a claim that the target Cargo graph is complete.
Use the [refactoring map](refactoring.md#remove-worker-implementation-edges) for current
edges and their removal. One workspace lockfile records resolved dependencies.
Add a library where its API has a real consumer and replaces an existing mechanism.

| Package | Owns | Local / donor dependencies at the target boundary |
| --- | --- | --- |
| `swarm-contracts` | Shared wire DTOs, request/outcome/receipt identity, method policy and module catalog. No SQLite, process or vendor runtime types. | `serde`, `serde_json` |
| `swarm-client` | Authenticated bounded local IPC, endpoint discovery and typed `ModuleLink`. | `swarm-contracts` |
| `swarm-checks` | Bounded admitted native-check execution and authenticated status readback. Store retains admission/custody; host sheds the executor dependency. | `swarm-client`, `swarm-contracts`, `swarm-process` |
| `swarm-process` | OS ownership, exact birth/departure checks, private durable files and module-owner bootstrap. | `swarm-contracts`, `cccc-windows-process`, selective `sysinfo` observation |
| `swarm-store` | WAL connection/transaction mechanics and database identity checks. Store owns domain DDL/conversion. | `rusqlite` |
| `swarm-kernel` | Existing pure Task/dispatch/review/acceptance policy, bounded writer actor/readiness/batching, and target bounded reader dispatch. Host Store retains DB/rights/receipts. | `swarm-store`, `swarm-contracts` |
| `swarm-telemetry` | Structured events, reloadable filters, redaction, bounded sinks and local loss counters. | `tracing`, `tracing-subscriber`, `tracing-appender`, `atlas-redact` |

`swarm-kernel-host` owns authentication policy, domain transactions, server IPC and
routing. It must not link `swarm-adapter-*` or worker implementations: automation,
bus, checks, forge, observer, script-worker or supervisor. Shared packages never
import the host or adapters. Move shared DTOs to their existing owner; do not add a
new general-purpose crate for each extraction. Package movement reduces coupling;
the [deletion queue](refactoring.md#code-reduction-candidates) separately reduces code.

## Contract boundaries

| Boundary | Preserve | Canonical detail |
| --- | --- | --- |
| Module catalog | Data-only selection; exact admitted descriptor, catalog-revision CAS and receipt identity. Catalog lookup neither reads an executable nor proves its provenance. IDs are opaque; path components require encoding. | [Module protocol](modularity.md#3-module-protocol-and-registration) |
| Client | Named-pipe/Unix-socket discovery and hello; retries only before application traffic. An uncertain/malformed reply poisons the link and permits no blind request replay. | [Module contract](../agent_swarm.module-contract-v2.md), [`swarm-client`](../../crates/swarm-client/src/lib.rs) |
| Process owner | Nonkilling module groups on owner loss; check/script cancellation remains separately scoped. Exact process birth is not native-session identity; unknown reads are not departure. | [Lifecycle](modularity.md#5-demand-driven-lifecycle-unified-supervision-and-recovery), [`swarm-process`](../../crates/swarm-process/src/lib.rs) |
| Check execution | Durable Store Go precedes source resolution/launch, bounded capture keeps pipes drained, and uncertain family disposition retains custody. Diagnostics do not change check/cache input identity. | [CheckRunner](../check-runner.md) |
| Module bootstrap | Absolute private plan, explicit resolver, cleared child environment and exact owner/artifact receipts. Pre-spawn `not_started` still requires exact owner/family readback before replacement. | [Module installation](module-installation.md), [ModuleLink contract](../agent_swarm.module-contract-v2.md) |
| Database | DataRoot lock remains on the writer through initialization/shutdown. Domain schema and reconciliation share existing transactional initialization; callback errors pass through unchanged. Preserve parsed-JSON digest comparison and both existing empty-database identity cases. Reads authorize and project in one short read-only snapshot. | [Store boundary](architecture.md#12-transaction-relational-sqlite-and-wal-read-pool-boundaries), [migration detail](refactoring.md#relational-conversion-detail) |
| Diagnostics | Redact before persistence/fanout; bound records and bytes, count sink loss independently. Diagnostic acceptance/sequence is neither a business ACK nor a durable event cursor. | [Observability](observability.md) |

The process child-membership helper checks an exact distinct native child against
an existing live module owner; it does not adopt/start a process, validate the
worker as its own child or prove whole-family departure. Preserve the existing
family scan before replacement. Private-path checks do not claim an atomic race
barrier or a same-user sandbox.

Implementation status and historical receipts belong in [Implementation status](../implementation-status.md).
Build/delegation rules belong in [AGENTS.md](../../AGENTS.md) and [scoped builds](../ci-scoped-builds.md).
Large integration suites move with their real seam; small local unit tests may
remain beside code. Neither moving tests nor splitting files deletes production
behavior or demonstrates a performance improvement.
