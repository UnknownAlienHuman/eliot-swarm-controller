# Agent Operations

The V3 target is a compact controller for native-agent work. Implementation is partial;
[status](../implementation-status.md) separates landed code, source evidence and
unexecuted qualification. The whole-project source audit and documentation-readiness
gate are **not complete**.

## Read in this order

1. [Documentation authority map](documentation-map.md): what is current, evidence or historical.
2. [Architecture](architecture.md): product purpose and non-negotiable boundaries.
3. [Failure-prevention register](failure-prevention-register.md): prior failures and mandatory gates.
4. One approved [task packet](task-packet-template.md) for the assigned exact source revision.
5. The domain contract and current source named by that packet.

[Implementation](implementation.md) is the strategic dependency/order map. It is not a
self-contained worker assignment. Coding agents receive one narrow slice and do not
receive the whole audit archive or repository-wide A–V roadmap.
[AGENTS.md](../../AGENTS.md) owns collaboration and verification practice.

## Current readiness

The generated [documentation readiness report](documentation-readiness-report.md)
and [machine inventory](documentation-inventory.json) check local links, explicit
incompleteness markers and classification. They do not prove source or runtime
correctness.

Until the report and [implementation status](../implementation-status.md) both record a
completed whole-project source audit, agents may implement only explicitly approved
slices with exact current symbols and acceptance evidence. No document may describe
unmerged code as landed.

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
| [Source-audit coverage](source-audit-coverage.md) | Package/non-workspace coverage, untraced boundaries and next audit sequence. |
| [Task packet](task-packet-template.md) | Exact assignment format for one implementation slice. |
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
| What may this worker change now? | Its approved [task packet](task-packet-template.md) |
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
