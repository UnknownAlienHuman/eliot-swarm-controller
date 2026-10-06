# Agent Operations — Manager-Owned Automation

Revision 10 · 2026-10-05 · current source baseline `f51cf1b`; runtime qualification remains pending.

**Status: partial implementation.** Current source includes manager-owned action
admission and transfer, durable intake and peer coordination, the reviewed
return/repair cycle, calendar CheckRuns and manual run-now, bounded hooks,
invocation-scoped script effects, typed review rules and shared Goal
continuation. Each implemented slice uses the same Store and ordinary Operation
path. Source checks, exact CI commits, native failures and remaining gaps are
recorded in [Implementation status](../implementation-status.md); successful
source tests do not establish productive native/model execution. The documents
below retain the complete requirements, including work still outstanding.

The current source integration includes the physical Kernel move and all five
Rust adapter paths. The latest scoped production Clippy `--keep-going` run
failed with compile/lint errors across the adapters, supervisor, observer,
Forge, bus and checks; the integrated source batch addresses those defects. No tests, full
builds or native/provider/model calls have run in this source phase. Productive
qualification remains pending. OpenCode without exact assistant-parent proof
remains `Unknown`, Antigravity's unavailable result body remains unavailable,
and frozen historical qualification receipts are unchanged.

## Current operability and modularization

The historical [2026-10-05 recheck](modularity-review-2026-10-05.md) reproduced
the operator-only Task create/revise defect at its recorded revision. Later
source admits ordinary authenticated Managers to planning, retains optional
supervisor failures, and separates CLI, MCP, gateway and module supervisor
packages/processes. Scoped logging and monitoring are connected in source.
OpenCode native ownership/MCP effects and physical Kernel extraction are
delivered in source. The Kernel owns Store/IPC in `crates/swarm-kernel-host`,
the public host is a launcher, and the standalone supervisor has no database
access. Store retains typed native-start success against the original launch
history after Manager/lease changes. The installer validates the complete
sibling chain. These source changes have no current runtime qualification; see the
exact boundaries in [Implementation status](../implementation-status.md).

[Modular Runtime](modularity.md) owns the requested package/process boundaries,
demand-driven activation, simpler ordinary manager authority and local restart.
[Observability](observability.md) owns adjustable diagnostic depth and live views.
These are implementation contracts, not another activation ledger or approval
phase. Existing O1–O11 work is retained; M1–M6 specifies its extraction order and
concrete completion conditions. Finish required source before tests and native
qualification, using scoped Clippy to close compile defects during development.

## Product rule

**The manager works manually by default and enables whichever automations help. Enabled automations perform the selected actions on that manager's behalf. Manual commands remain available.**

One entry has a stable identity, manager owner, scope, trigger or preset, selected actions/settings, revision and `enabled`. New entries default off; the manager can configure and enable an entry in one request. There is no global manual/assisted/delegated mode, second activation registry or additional Root approval for capabilities that manager already has.

```text
manager's direct command ----------------------------+
                                                    |
manager-enabled automation -> eligible fact/time ----+
                                                    v
                              same Rust authorization/action handler
                                                    |
                                     retained Operation and result
```

Enabling audit assignment does not enable distribution, repair or push. Importing Issues, discovering a plugin or increasing the agent count enables nothing. Automations are tools for the manager, not new managers.

## Read by responsibility

| Document | Owns |
|---|---|
| [Configuration](configuration.md) | The editable schema, enable/disable, preferences and effects of changing settings. |
| [Module API contracts](module-api.md) | Current bounded GitHub, hook, script, typed-rule and Goal call shapes and execution boundaries. |
| [Architecture](architecture.md) | On-behalf execution, durable dispatch, monitoring, hooks, cron, Goal, scripts and recovery. |
| [Delivery](delivery.md) | Queue assignment, submission, audit, return, repair and GitHub effects. |
| [Donor map](donor-map.md) | Source observations and precisely limited reuse. |
| [Implementation](implementation.md) | Shared ownership, O1–O11 production work and qualification scenarios. |
| [Modular Runtime](modularity.md) | Independent Cargo/process boundaries, bus transaction seam, module lifecycle, authority simplification and M1–M6 extraction order. |
| [Module installation](module-installation.md) | Build, install, register and select one local Rust adapter with current config fields. |
| [Observability](observability.md) | Log levels/content depth, correlated failures, live monitoring, bounded storage and future chart/export data. |
| [Source recheck](modularity-review-2026-10-05.md) | Reproduced manager defect, source-confirmed failure/build seams, retained working invariants and remaining risk checks. |

PR #22 supplies peer coordination, Participant identity, watches, launcher and deferred MCP. Implement those once. The canonical assigned-reviewer surface uses `review.submit`, with exact-assignment `review.get` and linked `operation.get` reads after release while authenticated and unrevoked; no historical context/list, artifact or evidence reads. A null preregistered `review_assignment_id` is unusable until atomic server binding. A legacy `Reviewer` profile may remain explicitly named for compatibility. Review submission never grants manager disposition rights.

## Everyday use

The manager sees their entries with owner, enabled flag, selected actions, scope, preferred profiles, last/next invocation and concrete waiting reasons. "Manual control; audit handoff enabled" is a valid presentation.

Useful independent choices include commit-to-auditor notification without a model launch, automatic review assignment with manual repair/publication, queue distribution with manual review, and a complete selected delivery pipeline within existing rights.

Observation, streams, peer mail, requested reminders and late result collection work without enabled automations. A normal manager decision, unavailable optional integration or pending capacity is not a code defect and must not generate repetitive prompts.

## Small implementation, complete behavior

- One Store, Operation receipt path and effective authorization policy. Scheduling/ownership responsibilities remain unique but move into separately built modules/processes; they are not required to share one failure domain. In-memory notifications accelerate discovery; they never replace durable work.
- One manual/automatic action reservation. Duplicate hooks, overlapping automations and reconnects cannot independently launch the same work or publish the same candidate again.
- Each selected delivery step reads its own committed prerequisites. A repaired submission can be audited again; general loop suppression must not discard legitimate descendants of the workflow.
- The technical executor and effective manager are both recorded. Committed events, native-start receipts and result pages retain their original cause after Manager handover, session loss or lease release; current authority is checked before new effects. Auditor results retain their actual author. No reusable manager credential is copied to a script or forged into a public request.
- Settings changes affect subsequent admissions; retained requests are not rewritten. Removing an action or disabling its entry blocks unstarted/follow-up effects. Running work remains visible until completion or a separate supported cancellation.
- Enabled settings survive ordinary manager disconnect and host restart. Reconcile current rights and uncertain effects before proceeding; no reboot approval ceremony and no fallback to Root.

## Boundaries retained

**Rust internals:** all owned host/Store, native adapters, monitoring, GitHub, hooks, distribution, review, cron, Goal, MCP/gateway, configuration and script-runner code is Rust. Python/PowerShell are optional external extensions. Existing owned non-Rust bridges are migration inputs; vendor executables and native Git remain external tools behind typed Rust adapters.

**No prescribed software pins:** choose maintained libraries and compatible dependency requirements. Accept installed runtimes by documented protocol/capability support, not equality to an old release. Evidence SHAs, request/configuration revisions and retained run bytes do not freeze future compatible software. No callback-time installer or silent downgrade.

**One manager, one mutable worktree and one in-flight product candidate.** Internal writers work on non-overlapping parts of that Issue, do not run Cargo and do not independently publish their fragments. Candidate-bound review and protected acceptance remain distinct from self-report.

**Privacy:** local configuration owns endpoints, credentials and private paths. Repository examples use placeholders. Stream and GitHub projections expose only the authorized, redacted data required for their audience.

## Source compatibility to implement, not assume

| Existing anchor | Current fact | Required change or reuse |
|---|---|---|
| `crates/swarm-kernel-host/src/store/submissions.rs::reserve/finish` | Queued admission precedes applied `task.submission`. | Audit only retained applied submissions. |
| `crates/swarm-kernel-host/src/store/submissions.rs::request_changes` | Exact anchors and frozen-policy-dependent feedback; owner-policy-v2 supports scoped manager disposition. | Preserve the landed policy checks; do not reimplement them as an unfinished grant system. Audit results and repair delivery remain separate. |
| `crates/swarm-kernel-host/src/model.rs::Principal::owns` | Internal Scheduler has a special ownership path for existing scheduled checks. | Do not generalize that exception to manager-owned automation. Use explicit current-manager authorization. |
| `crates/swarm-kernel-host/src/store/schedules.rs` | Cursor and check admission are transaction-coupled. | Preserve that property for new triggers and preserve old receipts. |
| `crates/swarm-kernel-host/src/policy.rs`, `docs/owner-decisions.md` | Accepted policy identities and GM/epoch restrictions exist. | Adopt deliberate new policy with code while retaining historical Attempts; do not edit away old evidence. |
| `crates/swarm-kernel-host/src/mcp/subscriptions.rs` | Bounded committed-fact polling with lag/resync. | Share projectors and retain durable cursors; live native text is a separate stream. |
| `docs/forge-publication.md` | Accepted-candidate non-force push and uncertain-effect readback. | Reuse; upload and PR merge are separate effects, not stronger guarantees inferred from a local lock. |

## Delivery scope

The current Kernel, standalone supervisor, typed adapter handoff and event/result ownership changes are delivered as source. Compiler, core failure/recovery and native qualification remain separate acceptance work. The historical bounded recheck retains its recorded source and IPC scope; its temporary-host observation does not qualify the current implementation. See the current source block in [Implementation status](../implementation-status.md) before interpreting older receipts.
