# Audit index

Navigation for continuing source research. Finding dispositions live only in the
[audit register](audit-register.md#findings-and-coverage);
[implementation](implementation.md) owns source changes and
[status](../implementation-status.md) owns landed behavior.

## Start after an interruption

1. Read [Resume here](audit-register.md#resume-here).
2. Compare its inspected source head with live `main`.
3. Reopen changed callers/consumers only.
4. Follow the named exact chain and update the existing AR register.

Source for the current checkpoint was inspected at
`5b6b5bee92123ce7d3252054c939fb18619b790f` on 2026-10-23. The comparison from runtime
commit `c92120ed70ca7e625440a8a19517c481e73b13c9` is 20 documentation-only commits.
The AR-065–070 trace was published in `d4f7e5b3845470c9880248590490f96c4095bec7`.
No runtime repair or executed qualification is implied.

## Evidence checkpoints

The active register overrides older wording when a later pass narrows or refutes it.

| Entries | Immutable detailed snapshot | Subject |
| --- | --- | --- |
| [AR-001–005](audit-register.md#ar-001) | [`c3e331a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md) | Antigravity reporting, body lifetime, identity, restart and admitted pages. |
| [AR-006–008](audit-register.md#ar-006) | [`70b99db`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md) | Continuation results, assembly and authentication/completion boundary. |
| [AR-009–011](audit-register.md#ar-009) | [`1c61629`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md) | Send attribution, repair classification and same-Attempt correction. |
| [AR-012–016](audit-register.md#ar-012) | [`ec01905`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/docs/agent-operations/audit-register.md) | Revision/release fences, cleanup, stable credential and old boot. |
| [AR-017–019](audit-register.md#ar-017) | [`f5ccc41`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f5ccc418ba401733d1f778619abeec5c0554416b/docs/agent-operations/audit-register.md) | Recovery queue, supervisor scope, replay ACK and inconclusive lookup. |
| [AR-020–024](audit-register.md#ar-020) | [`ac3389c`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ac3389c013840b02a78dc310bcee481d24086402/docs/agent-operations/audit-register.md) | Late terminal, first historical capture and automatic readback. |
| [AR-025–031](audit-register.md#ar-025) | [`9c47d88`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/9c47d88a09a543e9f0ea74228370833f14a6c619/docs/agent-operations/audit-register.md) | Cross-adapter source/delivery; Store-owned aggregate coverage. |
| [AR-032–035](audit-register.md#ar-032) | [`a0bc1dc`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/a0bc1dcb69fa4a2d3dea1e51e3c5b0d148bde1bb/docs/agent-operations/audit-register.md) | Historical source guards, read amplification, Muse discovery and grants. |
| [AR-036–040](audit-register.md#ar-036) | [`c0d1b91`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c0d1b91f15636982f977768474f7246b97308d61/docs/agent-operations/audit-register.md) | Capacity history, default model, saturation, stale quota and dedup. |
| [AR-041–044](audit-register.md#ar-041) | [`c2b998b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c2b998b079fe8517c592192c18d6ecbfb6b26aae/docs/agent-operations/audit-register.md) | Quota bypass, cancelled unbound launch, rejected Open and root limit. |
| [AR-045–047](audit-register.md#ar-045) | [`3c4c859`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3c4c8599f1614460fe69d54d0b59715a26ada797/docs/agent-operations/audit-register.md) | Shutdown readiness, detach demand and service/binding/workspace retirement. |
| [AR-048–050](audit-register.md#ar-048) | [`73ba700`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/73ba7009bbc5c5515fe4c5f85743066e944106ef/docs/agent-operations/audit-register.md#trace-2026-10-23-retirement-admission) | Missing finalizer, route-edit accounting and common claim/Open transaction. |
| [AR-051–058](audit-register.md#ar-051) | [`4af640e`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/4af640ea6edc190d58a7320674c85cbc5bd3c6b3/docs/agent-operations/audit-register.md#trace-2026-10-23-goal-policy) | Goal actor/phase/budget/transfer, terminal identity and native capability. |
| [AR-059–064](audit-register.md#ar-059) | [`5b6b5be`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/5b6b5bee92123ce7d3252054c939fb18619b790f/docs/agent-operations/audit-register.md#trace-2026-10-23-review-acceptance) | Reviewer replacement, correction, pending fairness, dependency isolation and acceptance provenance. |
| [AR-065–070](audit-register.md#ar-065) | [`d4f7e5b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/d4f7e5b3845470c9880248590490f96c4095bec7/docs/agent-operations/audit-register.md#trace-2026-10-23-checkrun) | Fixed source, CheckRun custody/recovery, parser evidence, cache and executor consolidation. |

There are 70 entries. Some preserve correct behavior or narrow a prior claim.

## Source and target map

Symbols are entry points, not whole-file certification. Use the inspected revision named
by the checkpoint.

| Responsibility / AR | Current source entry points | Target contract |
| --- | --- | --- |
| Antigravity result/recovery, 001–004, 016–019, 024 | [Rust adapter](../../modules/antigravity-rust/src/main.rs), [controller](../../modules/antigravity-rust/src/controller.rs), [Store validator](../../crates/swarm-kernel-host/src/runtime/warm_stream.rs) | [Module API](module-api.md), implementation A |
| Continuation/source/late pages, 005–006, 009–014, 020–024 | [runtime](../../crates/swarm-kernel-host/src/store/runtime.rs), [normalized result](../../crates/swarm-kernel-host/src/store/normalized_result.rs), [producers](../../crates/swarm-kernel-host/src/store/producers.rs) | [Module API](module-api.md), [Delivery](delivery.md) |
| Result storage/grants, 007–008, 031, 035 | [results](../../crates/swarm-kernel-host/src/store/results.rs), [object scope](../../crates/swarm-kernel-host/src/store/object_scope.rs) | [Module API](module-api.md) |
| Adapter result sources, 025–030, 032–034 | [Claude](../../crates/swarm-adapter-claude/src/lib.rs), [Codex](../../crates/swarm-adapter-codex/src/lib.rs), [OpenCode](../../crates/swarm-adapter-opencode/src/lib.rs), [Command](../../crates/swarm-adapter-command/src/adapter.rs), [Muse](../../modules/muse/results.mjs) | [Donor map](donor-map.md), implementation M–Q |
| Queue/provider/root, 036–041, 044, 049–050 | [capacity](../../crates/swarm-kernel-host/src/store/capacity.rs), [provider conditions](../../crates/swarm-kernel-host/src/store/provider_conditions.rs), [WorkDispatch](../../crates/swarm-kernel-host/src/store/automation_work_dispatch.rs), [launcher](../../crates/swarm-kernel-host/src/store/launcher.rs) | [Launcher/Queue](../swarm-launcher-assignment-context.md), implementation B |
| Cancellation/retirement, 042–048 | [operations](../../crates/swarm-kernel-host/src/store/operations.rs), [workspace lifecycle](../../crates/swarm-kernel-host/src/store/workspace_lifecycle.rs), [owned service](../../crates/swarm-kernel-host/src/store/launcher_owned_service.rs), [module demand](../../crates/swarm-kernel-host/src/store/module_demand.rs) | [Launcher/Queue](../swarm-launcher-assignment-context.md), implementation B–D |
| Goal/calendar/native capability, 051–058 | [Goal progression](../../crates/swarm-kernel-host/src/store/automation_goal_progression.rs), [calendar](../../crates/swarm-kernel-host/src/scheduler/calendar.rs), [runtime](../../crates/swarm-kernel-host/src/store/runtime.rs), [Muse Goal](../../modules/muse/settings.mjs) | [Configuration](configuration.md), [Module API](module-api.md), implementation E/N/P/Q |
| Review/correction/acceptance, 011, 052, 059–064 | [submissions](../../crates/swarm-kernel-host/src/store/submissions.rs), [reviews](../../crates/swarm-kernel-host/src/store/reviews.rs), [review disposition](../../crates/swarm-kernel-host/src/store/review_disposition.rs), [acceptance](../../crates/swarm-kernel-host/src/store/acceptance.rs) | [Task policy](../task-policy.md), [Delivery](delivery.md), implementation G |
| Fixed source/CheckRun/cache, 065–070 | [Store checks](../../crates/swarm-kernel-host/src/store/checks.rs), [source](../../crates/swarm-kernel-host/src/checks/source.rs), [worker](../../crates/swarm-kernel-host/src/checks/worker.rs), [standalone host](../../crates/swarm-kernel-host/src/checks/standalone_host.rs), [executor](../../crates/swarm-checks/src/lib.rs) | [CheckRunner](../check-runner.md), implementation K |
| ScriptRun next chain | [Store scripts](../../crates/swarm-kernel-host/src/store/scripts.rs), [runner](../../crates/swarm-kernel-host/src/scripts/runner.rs), [worker](../../crates/swarm-script-worker/src/main.rs), [wire](../../crates/swarm-scripts/src/lib.rs) | [Configuration](configuration.md), implementation K |
| Observation | [module observations](../../crates/swarm-kernel-host/src/store/module_observations.rs), [process metrics](../../crates/swarm-observer/src/process_metrics.rs), [workspace](../../crates/swarm-kernel-host/src/store/workspace.rs) | [Observability](observability.md), implementation R |
| Mailbox/deletion | [mailbox](../../crates/swarm-kernel-host/src/store/mailbox.rs), [coordination](../../crates/swarm-kernel-host/src/store/coordination.rs) and sibling products | [Communication](../agent-communication-program.md), implementation S/T |
| GitHub/Forge effects | [GitHub Store](../../crates/swarm-kernel-host/src/store/github.rs), [label effects](../../crates/swarm-kernel-host/src/store/github_effects.rs), [PR effects](../../crates/swarm-kernel-host/src/store/github_pr_effects.rs), [Forge](../../crates/swarm-kernel-host/src/store/forge.rs) | implementation K/U |
| Gateway | [gateway](../../crates/swarm-gateway/src/gateway.rs), [config](../../crates/swarm-gateway/src/config.rs) | [Gateway](../gateway.md), implementation V |

## Coverage still open

This is a research index, not another implementation queue.

| Area | Remaining connected boundary |
| --- | --- |
| A, M–Q: adapters | Durable StoreDelivery, historical first capture, continuation sources, installed provider restart and exact version qualification. |
| B–D: launch/resources | Attached-binding retirement, cancellation disposition, route-edit budget identity and live transaction/custody races. |
| E: scheduler/Goals | One ordinary action loop, finite no-progress/usage budgets, native/Eliot Goal combination and restart. |
| F–G: Task/review/acceptance | TaskPacket renderers, automatic replacement, finite correction, per-subject isolation and common review/finding provenance. |
| H: permissions/API | Exhaustive method/role/object/revision matrix and deletion of public internal/vendor surfaces. |
| I: Store | Reader/writer pairs, numeric migrations, partial migration and corruption behavior. |
| J: IPC/MCP | Disconnect/replay boundary, slow consumers, fairness and writer occupancy. |
| K: effects | ScriptRun is next; then Check standalone cut, Forge/GitHub effect ownership and exact successor readback. |
| R: observation | Independent provider/process/Git/remote collectors, unmatched subjects, gaps and one joined read model. |
| S/T: deletion/mailbox | Recipient-indexed mailbox transition and concrete disposition before deleting coordination products. |
| V: gateway | Private secret loading, single-principal session limits, disconnect and compatibility launcher deletion. |
| Installed/native/Windows/load | Separate executed qualification; source evidence does not substitute for it. |

## Historical requirements and donors

| Material | Use |
| --- | --- |
| [Original specification at `f1b854b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md) | Intent/requirements source, not current code truth. |
| [2026-10-09 remediation audit](../remediation/2026-10-09/CURRENT-AUDIT.md) | Historical findings/fixes tied to its named revision. |
| [Claude-script donor review](../remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md) | Named private-ZIP mechanisms and limits; not freshly executed. |
| [Donor map](donor-map.md) | Current mechanisms, anti-donors, source links and version limits. |
| [Refactoring findings](refactoring.md) | Structural/deletion boundaries, not finding-status authority. |
| [Implementation status](../implementation-status.md#required-behavior-to-preserve) | Current behavior-to-owner summary. |

## Maintenance rule

Keep AR IDs and anchors stable. Add evidence with an inspected revision and exact
symbols. Preserve refuted claims and why they were narrowed. A docs commit never closes
a runtime defect, and an Issue/PR title never replaces source readback.
