# Agent Operations

The V3 target is a compact controller for native-agent work. Implementation is partial;
[status](../implementation-status.md) separates landed code, source evidence and
unexecuted qualification. The documentation authority, failure-prevention and packet
handoff are structurally ready for controlled slice-by-slice work. The whole-project
source audit and runtime qualification remain **incomplete** and are reported
separately.

## Read in this order

1. [Documentation authority map](documentation-map.md): what is current, evidence or historical.
2. [Architecture](architecture.md): product purpose and non-negotiable boundaries.
3. [Failure-prevention register](failure-prevention-register.md): prior failures and mandatory gates.
4. [Implementation slice register](implementation-slice-register.md): which slice is assignable now.
5. One `APPROVED` packet from the [task-packet index](task-packets/README.md).
6. The domain contract and exact current source named by that packet.

[Implementation](implementation.md) is the strategic dependency/order map. It is not a
self-contained worker assignment. Coding agents receive one narrow slice and do not
receive the whole audit archive or repository-wide A–V roadmap.
[AGENTS.md](../../AGENTS.md) owns collaboration and verification practice.

## Current readiness

The generated [documentation readiness report](documentation-readiness-report.md) and
[machine inventory](documentation-inventory.json) check authority classification,
required handoff documents, approved packet presence and local links. The structural
verdict is independent from the whole-project source-audit verdict.

Controlled implementation is allowed only for a slice marked `APPROVED_PACKET` in the
[slice register](implementation-slice-register.md) and backed by an exact packet in
[`task-packets/`](task-packets/README.md). Every other slice remains blocked by a named
dependency or `AUDIT_REQUIRED`. No document may describe unmerged code as landed.

## Resume research after an interruption

Start at [current audit / Resume here](audit-current.md#resume-here). It names the exact
inspected source basis, latest connected trace and next audit chain. Compare live
`main` first; reopen changed owners and consumers. Do not replace the audit task with an
implementation slice.

The original [AR-001–078 register](audit-register.md) and later immutable checkpoints
are retained as evidence. [Audit index](audit-index.md) maps every AR range to its
snapshot and target owner. The [failure-prevention register](failure-prevention-register.md)
then maps every range to a current invariant and required closure evidence. Do not
infer a runtime repair from documentation, a search hit or a test that was only read.

## Current contracts

| Document | Responsibility |
| --- | --- |
| [Implementation status](../implementation-status.md) | Landed behavior, source-audit state and qualification limits. |
| [Current audit](audit-current.md) | Active source findings, exact resume point and untraced boundaries. |
| [Audit index](audit-index.md) | Immutable evidence checkpoints and source/target navigation. |
| [Failure prevention](failure-prevention-register.md) | AR/PF invariants, implementation owner and closure evidence. |
| [Implementation slice register](implementation-slice-register.md) | Assignment eligibility, dependency blocks and handoff order for A–V. |
| [Task-packet index](task-packets/README.md) | Approved, blocked and superseded exact worker packets. |
| [Source-audit coverage](source-audit-coverage.md) | Package/non-workspace coverage, untraced boundaries and next audit sequence. |
| [Task packet template](task-packet-template.md) | Required assignment format; not an assignment by itself. |
| [Delivery](delivery.md) | Assignment, capture, review, correction, acceptance and publication cycle. |
| [Configuration](configuration.md) | Manager policy and the target current-state loop. |
| [Module API](module-api.md) | Semantic harness actions, result delivery and recovery. |
| [Launcher/Queue](../swarm-launcher-assignment-context.md) | Selection, admission and resource lifetime. |
| [Store evolution](store-evolution.md) | Numeric schema identity, finite v1 bridge, bounded postconditions and product-storage retirement. |
| [IPC/MCP](ipc-mcp.md) | Multiplexed transport, connection slots, exact Tasks reads, one wake pump and WAL read routing. |
| [GitHub sources](github-sources.md) | Read-only collection, explicit Task import and independent remote observation. |
| [External effects](external-effects.md) | One owner per remote Git/GitHub mutation and exact readback. |
| [CheckRunner](../check-runner.md) | Immutable source, process evidence, cache and acceptance handoff. |
| [ScriptRunner](../script-runner.md) | One script execution, indexed typed actions and child Operations. |
| [Communication](../agent-communication-program.md) | One addressed mailbox and direct collaboration. |
| [Observability](observability.md) | Independent local Governor truth and source gaps. |
| [Modularity](modularity.md), [shared packages](shared-packages.md) | Package, process and authority boundaries. |
| [Donor map](donor-map.md) | Reusable mechanisms, anti-donors and version/provenance limits. |
| [Module installation](module-installation.md) | Package/register/select path and transition gaps. |
| [Source findings](refactoring.md) | Structural excess and deletion boundaries. |

## Authority by question

| Question | Read |
| --- | --- |
| What may an agent treat as current? | [Documentation authority map](documentation-map.md) |
| What must the product do? | [Architecture](architecture.md), [delivery](delivery.md) |
| What is the strategic order? | [Implementation runtime order](implementation.md#5-runtime-order) |
| Which slice may be assigned now? | [Implementation slice register](implementation-slice-register.md) |
| What may this worker change now? | Its exact approved packet from [task-packets](task-packets/README.md) |
| Which previous failures constrain this slice? | [Failure prevention](failure-prevention-register.md) |
| How does Store schema compatibility work? | [Store evolution](store-evolution.md) |
| How do frontend concurrency and subscriptions work? | [IPC/MCP](ipc-mcp.md) |
| Where did source research stop? | [Current audit](audit-current.md#resume-here) |
| Which packages remain untraced? | [Source-audit coverage](source-audit-coverage.md) |
| What evidence supports a finding? | [Audit index](audit-index.md#evidence-checkpoints) |
| What can be reused or must be requalified? | [Donor map](donor-map.md) and [donor inventory](../agent_swarm.donors-20260929.toml) |
| Why did an old design exist? | Historical/evidence documents classified by the authority map; never resume them as current work orders. |

Update the owning contract, task packet, status and affected audit/prevention entries in
the same change. Supporting documents cannot reintroduce a duplicate workflow engine,
compatibility facade, frontend state store, separate execution authority or routine
human handoff in place of connected automation.
