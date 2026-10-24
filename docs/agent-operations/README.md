# Agent Operations

The V3 target is a compact controller for native-agent work. Implementation is partial;
[status](../implementation-status.md) separates landed code, source evidence and
unexecuted qualification.

## Read in this order

1. [Architecture](architecture.md): product purpose and non-negotiable boundaries.
2. [Implementation](implementation.md): the single implementation order and owners.
3. The contract for the assigned slice and its current source.

Coding agents receive one narrow slice. They do not receive the whole audit archive.
[AGENTS.md](../../AGENTS.md) owns collaboration and verification practice.

## Resume research after an interruption

Start at [current audit / Resume here](audit-current.md#resume-here). It names the exact
inspected head, latest connected trace and next chain. Compare live `main` first; reopen
only changed owners and consumers.

The original [AR-001–078 register](audit-register.md) and later immutable checkpoints
are retained as historical evidence. [Audit index](audit-index.md) maps every range to
its snapshot and current target owner. Do not infer a runtime repair from documentation,
a search hit or a test that was only read.

## Current contracts

| Document | Responsibility |
| --- | --- |
| [Implementation status](../implementation-status.md) | Landed behavior, source-audit state and qualification limits. |
| [Current audit](audit-current.md) | Active findings, exact resume point and latest source trace. |
| [Audit index](audit-index.md) | Immutable evidence checkpoints and source/target navigation. |
| [Delivery](delivery.md) | Assignment, capture, review, correction, acceptance and publication cycle. |
| [Configuration](configuration.md) | Manager policy and the target current-state loop. |
| [Module API](module-api.md) | Semantic harness actions, result delivery and recovery. |
| [Launcher/Queue](../swarm-launcher-assignment-context.md) | Selection, admission and resource lifetime. |
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
| What must the product do? | [Architecture](architecture.md), [delivery](delivery.md) |
| What should be implemented next? | [Implementation runtime order](implementation.md#5-runtime-order) |
| Where did source research stop? | [Current audit](audit-current.md#resume-here) |
| What evidence supports a finding? | [Audit index](audit-index.md#evidence-checkpoints) |
| What can be reused or must be requalified? | [Donor map](donor-map.md) and [donor inventory](../agent_swarm.donors-20260929.toml) |
| Why did an old design exist? | Historical program/specification files; never resume them as current work orders. |

Update the owning contract and current register in the same change. Supporting documents
cannot reintroduce a duplicate workflow engine, compatibility facade, separate execution
authority or routine human handoff in place of connected automation.
