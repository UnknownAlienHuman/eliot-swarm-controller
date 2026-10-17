# Agent Operations — Manager-Owned Automation (Architecture v3)

Revision 13 · 2026-10-17 · Canonical Version 3 specification.

V3 is the required target for ongoing development. [Current source and evidence](../implementation-status.md)
remain separately scoped. Start source work with [AGENTS.md](../../AGENTS.md) and the
[refactoring map](refactoring.md), then read the contract for the assigned boundary.
The map contains the near-term queue; this index does not create another backlog.

## Product rule

The manager works manually by default and enables selected automations on their
own behalf. Manual and automatic callers use the same authorization, reservation,
Operation and result path. Enabling one helper does not enable other delivery steps.
There is no global operating mode, second activation registry or additional Root
approval for actions already permitted to that manager.

## Required outcomes

1. Return complete attributable model output through exact native input identity
   or the [serialized-turn contract](architecture.md#41-deterministic-turn-scoped-result-correlation-agentresult).
   A missing optional parent pointer alone does not justify discarding a result.
2. Deliver `workspace lease -> agent.open -> task.dispatch` directly; native MCP
   discovery is [asynchronous telemetry](architecture.md#13-direct-launch-pipeline-and-non-blocking-mcp-readiness).
3. Keep one durable Store authority, indexed relational domain records and bounded
   concurrent WAL reads. Preserve rights and data in the same read snapshot.
4. Use independently built workers and adapters, shared contracts and existing
   donor primitives. Replacements remove their old mechanism and real dependency
   edges; moving files or adding wrappers does not establish this outcome.
5. Keep observations, late results and manual tools available with automation off.
   Disabling future effects does not erase running work or authorize uncertain replay.

## Read by responsibility

| Document | Sole documentation responsibility |
| --- | --- |
| [AGENTS.md](../../AGENTS.md) | Worktree, delegation and scoped verification rules. |
| [Refactoring map](refactoring.md) | Current source gaps, connected candidates and deletions. |
| [Architecture](architecture.md) | Durable execution, rights, identity, Store, result correlation and recovery. |
| [Configuration](configuration.md) | Editable fields, enabling, revisions, ownership transfer and settings effects. |
| [Delivery](delivery.md) | Assignment, submission, audit, return, repair and publication transitions. |
| [Module API](module-api.md) | Bounded call shapes and their execution boundaries. |
| [Modular Runtime](modularity.md) / [shared packages](shared-packages.md) | M1–M6, Cargo/process boundaries and the package ownership map. |
| [Donor map](donor-map.md) | Inspected donor behavior, actual integrations and replacement limits. |
| [Module installation](module-installation.md) | Build, package, register and select current artifacts. |
| [Observability](observability.md) | Diagnostic depth, redaction, storage bounds and live projections. |
| [Implementation](implementation.md) | O1–O11 scope and final acceptance scenarios. |
| [Implementation status](../implementation-status.md) | Revision-specific implementation and qualification evidence. |

## Boundaries retained

Rust owns the control plane and durable work. A thin official-SDK driver may own
native handles under the [donor boundary](donor-map.md#9-official-vendor-sdks-and-native-protocol-boundaries);
it must not introduce another Eliot lifecycle or replay owner. Choose compatible
maintained software; exact local artifact verification and retained identity bytes
remain separate from future protocol compatibility.

V18, the module contract and accepted policy editions retain unaffected invariants.
Do not infer new caller rights from a tool schema or rewrite frozen Attempts.
Private endpoints, credentials and paths stay local; public projections expose
only authorized, redacted data. Historical audits and run logs are evidence at their
recorded revisions, not additional agent instructions.
