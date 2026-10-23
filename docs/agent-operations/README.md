# Agent Operations

The V3 target is a compact controller for native-agent work. Implementation is partial;
[status](../implementation-status.md) separates landed code, open source audit and
unexecuted runtime qualification.

## Read in this order

1. [Architecture](architecture.md): short product manifesto for the lead developer.
2. [Implementation](implementation.md): the single work order, source owners,
   replacements, donors and relevant behavior checks.
3. The assigned contract and current source. [Source findings](refactoring.md) explain
   why existing machinery should be reduced.

## Resume research after a lost chat

Start at [audit register / Resume here](audit-register.md#resume-here). It names the
verified head, last connected source trace and exact unfinished chain. Compare live
`main` before continuing; a documentation-only interval does not fix runtime findings.

Use the [audit index](audit-index.md) for direct evidence checkpoints, stable AR links,
source symbols, target owners, historical requirements/donors and remaining coverage.
Finding dispositions live only in the register; implementation order lives only in
implementation. The index is navigation, not another approval system or worker pack.

Update the checkpoint and owning contract with the actual findings. Preserve evidence,
qualifications and refuted claims. Do not infer completion from old chat summaries,
a successful docs workflow, a search hit or a test that was merely read.

[AGENTS.md](../../AGENTS.md) owns collaboration and verification practice. Coding
workers need their compact assigned slice, not the audit archive. Older O/M/C/I
programs and prototype specifications describe their revisions; they do not add
requirements to the current target.

## Supporting documents

| Document | Responsibility |
| --- | --- |
| [Audit register](audit-register.md) | Finding dispositions, inspected revisions and exact resume point. |
| [Audit index](audit-index.md) | Direct evidence/source/target navigation and unclosed coverage. |
| [Implementation status](../implementation-status.md) | Landed behavior and its qualification limits. |
| [Configuration](configuration.md) | Editable policy/settings, ownership and effects of changes. |
| [Delivery](delivery.md) | Assignment, capture, review, correction and acceptance. |
| [Communication](../agent-communication-program.md) | Addressed messages supporting assigned work. |
| [Module API](module-api.md) | Retained semantic actions, delivery, result sources and recovery. |
| [Launcher/Queue](../swarm-launcher-assignment-context.md) | Current-state selection, admission and resource lifetime. |
| [Modularity](modularity.md), [shared packages](shared-packages.md) | Protocol, process and existing package boundaries. |
| [Donor map](donor-map.md) | Useful mechanisms, anti-donors, selected routes and source limits. |
| [Module installation](module-installation.md) | Current package/register/select commands and transition gaps. |
| [Observability](observability.md) | Independent local Governor observation plus optional bounded diagnostics. |
| [Source findings](refactoring.md) | Structural excess and deletion boundaries from earlier review. |

Supporting documents cannot reintroduce a removed product engine, permanent
compatibility path, separate execution authority or routine human handoff in place of
working automation.
