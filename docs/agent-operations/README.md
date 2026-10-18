# Agent Operations

The V3 target is a compact controller for native-agent work. The implementation
is partial; [status](../implementation-status.md) records the available evidence.

## Read in this order

1. [Architecture](architecture.md): the short product manifesto. Keep it in the
   lead developer's context; it explains purpose, boundaries and principles.
2. [Implementation](implementation.md): the single work order, actual source
   owners, replacements, donor choices and relevant behavior checks.
3. The contract and source for the assigned responsibility. Read the
   [source findings](refactoring.md) when checking why a mechanism should disappear.

[AGENTS.md](../../AGENTS.md) owns collaboration and verification practice.
Older O/M/C/I programs, policy editions and prototype specifications describe
their revisions; they do not add requirements to this target.

## Supporting documents

| Document | Responsibility |
| --- | --- |
| [Configuration](configuration.md) | Editable settings, ownership, enabling and effects of changes. |
| [Delivery](delivery.md) | Assignment, submission, independent review, correction and acceptance. |
| [Communication](../agent-communication-program.md) | Addressed messages supporting assigned work. |
| [Module API](module-api.md) | Concrete request/result shapes for retained capabilities. |
| [Modularity](modularity.md), [shared packages](shared-packages.md) | Existing protocol, process and package boundaries. |
| [Donor map](donor-map.md) | Inspected upstream behavior, actual integrations and replacement limits. |
| [Module installation](module-installation.md) | Current package/register/select commands and transition gaps. |
| [Observability](observability.md) | Diagnostic depth, redaction and bounded views. |
| [Source findings](refactoring.md), [implementation status](../implementation-status.md) | Why current code needs reduction; what source or runtime evidence exists. |

Implementation owns changes to the target. Supporting documents provide detail
for a retained responsibility; they cannot reintroduce a removed product engine,
permanent compatibility path or separate execution authority.
