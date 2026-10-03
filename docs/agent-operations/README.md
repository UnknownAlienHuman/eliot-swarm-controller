# Agent Operations — Manager-Owned Automation

Revision 5 · 2026-10-03 · design baseline `36cfb652` plus the active implementation increment.

**Status: partial implementation, not a completed or live-qualified program.** From baseline `36cfb652`, implementation commit `2607c8858e573ae40459c27d76d8ae9e1ca9f8fc` wires 29 additional methods and 83 `ToolSpec` entries through the library/Store/host/model/CLI with live authorization and role-core checks. Backend dev build, Clippy, 221 Rust tests, Windows probe and final Muse fixture passed for that implementation; actual native-MCP harness loading remains unknown. Participant scoped coordination/cards/mailbox, assigned-review slots/history, explicitly selected owner-policy-v2 feedback, automation config, durable `review_dispatch`, deferred MCP search and bounded launcher read projections are partial slices. Launch, watch/consult, integration/scope-Git, Concilium, cron, Goal and native-Rust conversion remain planned gaps. The six documents describe the intended larger Rust system; they are not proof those paths work.

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
| [Architecture](architecture.md) | On-behalf execution, durable dispatch, monitoring, hooks, cron, Goal, scripts and recovery. |
| [Delivery](delivery.md) | Queue assignment, submission, audit, return, repair and GitHub effects. |
| [Donor map](donor-map.md) | Source observations and precisely limited reuse. |
| [Implementation](implementation.md) | Shared ownership, O1–O11 production work and qualification scenarios. |

PR #22 supplies peer coordination, Participant identity, watches, launcher and deferred MCP. Implement those once. The canonical assigned-reviewer surface uses `review.submit`, with exact-assignment `review.get` and linked `operation.get` reads after release while authenticated and unrevoked; no historical context/list, artifact or evidence reads. A null preregistered `review_assignment_id` is unusable until atomic server binding. A legacy `Reviewer` profile may remain explicitly named for compatibility. Review submission never grants manager disposition rights.

## Everyday use

The manager sees their entries with owner, enabled flag, selected actions, scope, preferred profiles, last/next invocation and concrete waiting reasons. "Manual control; audit handoff enabled" is a valid presentation.

Useful independent choices include commit-to-auditor notification without a model launch, automatic review assignment with manual repair/publication, queue distribution with manual review, and a complete selected delivery pipeline within existing rights.

Observation, streams, peer mail, requested reminders and late result collection work without enabled automations. A normal manager decision, unavailable optional integration or pending capacity is not a code defect and must not generate repetitive prompts.

## Small implementation, complete behavior

- One Store, Operation receipt path, authorization evaluator, scheduler and process owner. In-memory notifications accelerate discovery; they never replace durable work.
- One manual/automatic action reservation. Duplicate hooks, overlapping automations and reconnects cannot independently launch the same work or publish the same candidate again.
- Each selected delivery step reads its own committed prerequisites. A repaired submission can be audited again; general loop suppression must not discard legitimate descendants of the workflow.
- The technical executor and effective manager are both recorded. Auditor results retain their actual author. No reusable manager credential is copied to a script or forged into a public request.
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
| `src/store/submissions.rs::reserve/finish` | Queued admission precedes applied `task.submission`. | Audit only retained applied submissions. |
| `src/store/submissions.rs::request_changes` | GM/operator feedback, exact anchors, mail rather than native input. | Add intended scoped manager rights in the shared handler; audit results and repair delivery remain separate. |
| `src/model.rs::Principal::owns` | Internal Scheduler has a special ownership path for existing scheduled checks. | Do not generalize that exception to manager-owned automation. Use explicit current-manager authorization. |
| `src/store/schedules.rs` | Cursor and check admission are transaction-coupled. | Preserve that property for new triggers and preserve old receipts. |
| `src/policy.rs`, `docs/owner-decisions.md` | Accepted policy identities and GM/epoch restrictions exist. | Adopt deliberate new policy with code while retaining historical Attempts; do not edit away old evidence. |
| `src/mcp/subscriptions.rs` | Bounded committed-fact polling with lag/resync. | Share projectors and retain durable cursors; live native text is a separate stream. |
| `docs/forge-publication.md` | Accepted-candidate non-force push and uncertain-effect readback. | Reuse; upload and PR merge are separate effects, not stronger guarantees inferred from a local lock. |

## Delivery scope

This PR changes documentation only. It runs no model, script, hook, scheduler or publication, installs nothing and changes no machine or repository settings. Static review is not runtime, security or fleet qualification.
