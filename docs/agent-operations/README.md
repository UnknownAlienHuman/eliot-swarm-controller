# Agent Operations — Manager-Owned Automation (Architecture v3)

Revision 11 · 2026-10-17 · Canonical Version 3 specification.

**Engineering principle:** The architecture must enable fast, reliable, productive execution of agent tasks—never paralyze work with artificial deadlocks, brittle internal hash ceremonies, or refusals to return model output. Historical CI runs and past remediation notes live in [Implementation status](../implementation-status.md); the documents in `docs/agent-operations/` define the clean, normative Version 3 contract.

## Core anti-sabotage guarantees

1. **Productive result collection without artificial parent-edge refusals:**
   When a native runtime protocol provides an explicit request/input parent ID on assistant items (such as Codex JSON-RPC), the adapter records that exact ID. When a native stream is session- or turn-scoped without a cryptographic input-parent field on every chunk (such as OpenCode HTTP/SSE, Antigravity ACP/headless, or Claude stream-json), the adapter correlates assistant output deterministically using the **serialized active turn / busy window** `(binding_id, binding_generation, native_session_id, active_turn_id)` sealed to the admitted `task.dispatch` or `agent.send` Operation. Adapters must assemble and return the assistant result body through `swarm.normalized_result_page@1`—never refuse a valid model response with `RESULT_BODY_UNAVAILABLE` or permanent `Unknown` status merely because the vendor wire format omits a parent pointer.
2. **Direct launch and dispatch without pre-prompt MCP deadlocks:**
   The productive launch sequence is `workspace lease -> agent.open -> task.dispatch`. Native MCP capability discovery (`tools/list`, participant connection readback) is asynchronous readiness telemetry. A session that does not need MCP tools for its first turn—or a batch/headless runtime that connects to MCP only when invoking a tool—must never be blocked from receiving `task.dispatch` by an `awaiting_native_mcp` gate.
3. **Donor-first implementation (no reinvented wheels):**
   Use mature Rust crates (`rmcp`, `agent-client-protocol`, `octocrab`, `croner`, `sysinfo`, `notify`, `tracing`, `tracing-subscriber`, `tracing-appender`, `atlas-redact`, `cccc-windows-process`) and official vendor SDK contracts (`@muse-code/sdk`, `@anthropic-ai/claude-agent-sdk`, `@opencode/server`, `openai-codex`) directly for their intended purpose. Do not write custom HTTP/GitHub stacks, custom cron parsers, custom process-tree pollers, custom logging frameworks, or from-scratch reimplementations of vendor SDK state machines.
4. **Relational SQLite with WAL read concurrency:**
   The Store uses a single serialized writer for state transitions and a **read-only WAL connection pool** for all queries (`*.get`, `*.list`, `dashboard`, `explain`, `catalog`). Domain entities, cursors, pending work, and capacity reservations live in indexed relational tables—never as hex-encoded keys or multi-kilobyte JSON blobs inside the scalar `meta` table.
5. **True modularity and clean compilation boundaries:**
   [Modular Runtime](modularity.md) defines strict package and process boundaries: `swarm-kernel` / `swarm-kernel-host` must never link adapter crates or optional worker implementations (`swarm-automation`, `swarm-bus`, `swarm-checks`, `swarm-observer`, `swarm-script-worker`, `swarm-supervisor`), and production `src/` trees must not embed massive integration test suites or mock binaries.
   [Observability](observability.md) defines adjustable diagnostic depth and live views built on `tracing` and `atlas-redact`.

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

**Rust core and donor reuse:** all owned host/Store, IPC, supervision, monitoring, GitHub, hooks, distribution, review, cron, Goal, MCP/gateway, configuration and script-runner orchestration is Rust. Within Rust, mandatory donor crates (`rmcp`, `agent-client-protocol`, `octocrab`, `croner`, `sysinfo`, `notify`, `tracing`, `tracing-subscriber`, `tracing-appender`, `atlas-redact`, `cccc-windows-process`) provide protocol, GitHub, scheduling, OS observation, logging and redaction primitives. For external runtimes whose official protocol is defined by a vendor SDK (`@muse-code/sdk`, `@anthropic-ai/claude-agent-sdk`, `@opencode/server`, `openai-codex`), the Rust adapter owns `ModuleLink`, supervision, digests and Store receipts while delegating native session/stream protocol handling to the official Rust protocol crate or a thin stateless vendor SDK driver—never reimplementing vendor state machines from scratch. Python/PowerShell remain optional external user script extensions.

**No prescribed software pins:** choose maintained libraries and compatible dependency requirements. Accept installed runtimes by documented protocol/capability support, not equality to an old release or hardcoded executable SHA-256. Evidence SHAs, request/configuration revisions and retained run bytes do not freeze future compatible software. No callback-time installer or silent downgrade.

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

All Version 3 architecture documents in `docs/agent-operations/` are normative for ongoing development and refactoring. Historical CI runs, qualification logs and remediation records are tracked separately in [Implementation status](../implementation-status.md).
