# Audit index

Navigation for continuing source research, not another work order. Findings and their
current dispositions live only in the [register](audit-register.md#findings-and-coverage).
[Implementation](implementation.md) owns source changes; [status](../implementation-status.md)
owns landed behavior. An index entry is not a new inspection or qualification.

## Start after an interruption

Read [Resume here](audit-register.md#resume-here), compare its verified head with live
`main`, then follow the named unfinished chain. Recheck changed callers/consumers;
do not restart the whole archive or infer progress from a chat response.

Inspected repository head for this index: `73ba7009bbc5c5515fe4c5f85743066e944106ef`,
2026-10-23. The comparison from runtime commit
`c92120ed70ca7e625440a8a19517c481e73b13c9` contains 18 documentation-only commits.
No runtime repair or newly executed verification is implied by indexing them.

## Evidence checkpoints

These immutable register snapshots contain the detailed inspected paths, reasoning,
qualifications and preserved behavior. The current register overrides an older claim
when it narrows or refutes it. Each link goes directly to the evidence rather than
requiring a walk through every preceding checkpoint.

| Register entries | Detailed evidence snapshot | Subject |
| --- | --- | --- |
| [AR-001–005](audit-register.md#ar-001) | [c3e331a](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md) | Link failure, body lifetime, exact identity, restart and admitted page completion. |
| [AR-006–008](audit-register.md#ar-006) | [70b99db3](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md) | Continuation results, assembly and completion versus authentication. |
| [AR-009–011](audit-register.md#ar-009) | [1c61629b](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md) | Send attribution, repair classification and same-Attempt resubmission. |
| [AR-012–016](audit-register.md#ar-012) | [ec019052](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/docs/agent-operations/audit-register.md) | Revision/release fences, GM cleanup, stable credential and old-boot import. AR-016 is narrowed in the next snapshot. |
| [AR-017–019](audit-register.md#ar-017) and AR-016 correction | [f5ccc418](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f5ccc418ba401733d1f778619abeec5c0554416b/docs/agent-operations/audit-register.md) | Recovery queue, supervisor scope, replay ACK and inconclusive lookup. |
| [AR-020–024](audit-register.md#ar-020) | [ac3389c](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ac3389c013840b02a78dc310bcee481d24086402/docs/agent-operations/audit-register.md) | Late terminal, first historical capture and automatic capture/readback. AR-024's local range-ledger proposal is rejected in the next snapshot. |
| [AR-025–031](audit-register.md#ar-025) and AR-024 correction | [9c47d88](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/9c47d88a09a543e9f0ea74228370833f14a6c619/docs/agent-operations/audit-register.md) | Cross-adapter sources/delivery; Store owns coverage, not another adapter ACK ledger. |
| [AR-032–035](audit-register.md#ar-032) | [a0bc1dcb](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/a0bc1dcb69fa4a2d3dea1e51e3c5b0d148bde1bb/docs/agent-operations/audit-register.md) | Historical source guards, Codex read amplification, Muse discovery and object grants. |
| [AR-036–040](audit-register.md#ar-036) | [c0d1b91](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c0d1b91f15636982f977768474f7246b97308d61/docs/agent-operations/audit-register.md) | Capacity history, default model, pending saturation, stale quota and useful dedup/fairness. |
| [AR-041–044](audit-register.md#ar-041) | [c2b998b](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c2b998b079fe8517c592192c18d6ecbfb6b26aae/docs/agent-operations/audit-register.md) | Quota bypass, cancelled unbound launch, rejected Open and lowered root limit. |
| [AR-045–047](audit-register.md#ar-045) | [3c4c859](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3c4c8599f1614460fe69d54d0b59715a26ada797/docs/agent-operations/audit-register.md) | Shutdown readiness, intentional detach versus demand, service/binding/workspace retirement. |
| [AR-048–050](audit-register.md#ar-048) | [73ba700](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/73ba7009bbc5c5515fe4c5f85743066e944106ef/docs/agent-operations/audit-register.md#trace-2026-10-23-retirement-admission), inspected at `b2519aa` | Missing attached-binding finalizer, route-edit census limits, preserved common claim/Open transaction. No oversubscription or live-concurrency claim. |
| [AR-051–058](audit-register.md#ar-051) | [Current detailed trace](audit-register.md#trace-2026-10-23-goal-policy), inspected at `73ba700` | Goal actor/disable/phase/budget/transfer gaps; preserved terminal and calendar identity; current versus upstream native Goal. No installed execution/restart claim. |

There are 58 entries, not 58 defects or a percentage of the repository. Some preserve
correct behavior, narrow a claim, or explicitly record a partial trace.

## Source and target map

Relative source links open the current tree. Use the checkpoint's inspected revision
for evidence; line numbers from another revision are not authority. Symbols are search
entry points, not claims that the whole file was read.

| Responsibility / register entries | Current source entry points | Owning target |
| --- | --- | --- |
| Antigravity stream/result: 001–004, 016–019, 024 | [adapter](../../modules/antigravity-rust/src/main.rs); [warm_stream](../../crates/swarm-kernel-host/src/runtime/warm_stream.rs); `Controller::read_command`, `validate_outcome` | [Module API](module-api.md), implementation A |
| Continuation/source/late pages: 005–006, 009–014, 020–024 | [runtime](../../crates/swarm-kernel-host/src/store/runtime.rs) `user_command_with_actor`, `next_internal`; [normalized_result](../../crates/swarm-kernel-host/src/store/normalized_result.rs) `admitted_origin`; [producers](../../crates/swarm-kernel-host/src/store/producers.rs) | [Module API](module-api.md), [Delivery](delivery.md) |
| Result storage/grants: 007–008, 031, 035 | [results](../../crates/swarm-kernel-host/src/store/results.rs) `prepare`, `record`; [object_scope](../../crates/swarm-kernel-host/src/store/object_scope.rs) `resolve_artifact_read`, `resolve_assembly_sources` | [Module API](module-api.md) |
| Claude/Codex/OpenCode result delivery: 025–028, 032–033 | [Claude](../../crates/swarm-adapter-claude/src/lib.rs) `handle_result`; [Codex](../../crates/swarm-adapter-codex/src/lib.rs) `build_normalized_result_page`; [OpenCode](../../crates/swarm-adapter-opencode/src/lib.rs) `handle_assistant_result` | [Donor map](donor-map.md), implementation M/N/Q |
| Command/Muse sources: 029–030, 034 | [Command](../../crates/swarm-adapter-command/src/adapter.rs); [Muse result reader](../../modules/muse/results.mjs) `readItem`, `readResult`; [Muse bridge](../../modules/muse/bridge.mjs) | [Donor map](donor-map.md), implementation O/P |
| Queue/root/quota: 036–041, 044, 049–050 | [capacity](../../crates/swarm-kernel-host/src/store/capacity.rs) `root_claims_for_route`; [provider_conditions](../../crates/swarm-kernel-host/src/store/provider_conditions.rs) `check_route`; [WorkDispatch](../../crates/swarm-kernel-host/src/store/automation_work_dispatch.rs) `reconcile_entry` | [Launcher/Queue](../swarm-launcher-assignment-context.md), implementation B |
| Launch cancellation/retirement: 042–048 | [operations](../../crates/swarm-kernel-host/src/store/operations.rs) `cancel`; [launcher](../../crates/swarm-kernel-host/src/store/launcher.rs) `fail_launch`; [workspace lifecycle](../../crates/swarm-kernel-host/src/store/workspace_lifecycle.rs) `workspace_resource_use`; [owned service](../../crates/swarm-kernel-host/src/store/launcher_owned_service.rs) | [Launcher/Queue](../swarm-launcher-assignment-context.md), implementation B/C/D |
| Shutdown/control demand: 045–047 | [runtime](../../crates/swarm-kernel-host/src/store/runtime.rs) `validate_agent_shutdown_request`, `disconnected`; [module_demand](../../crates/swarm-kernel-host/src/store/module_demand.rs) `operation_candidates`, `scope_readback`; [supervisor](../../crates/swarm-supervisor/src/standalone.rs) `reconcile_demands` | [Module API](module-api.md), [Launcher/Queue](../swarm-launcher-assignment-context.md) |
| Goal admission/disable/phase/budget/transfer: 051–056 | [automation](../../crates/swarm-kernel-host/src/store/automation.rs) `apply`; [progression](../../crates/swarm-kernel-host/src/store/automation_goal_progression.rs) `prepare`, `require_current_for_dispatch`, `reconcile_entry`, `relocate_state`; [runtime](../../crates/swarm-kernel-host/src/store/runtime.rs) `next_internal`, `codex_goal_terminal_evidence`; [goals](../../crates/swarm-kernel-host/src/store/goals.rs) `progression_target` | [Configuration](configuration.md), [Delivery](delivery.md), implementation E/G |
| Calendar occurrence/loop: 057 | [calendar](../../crates/swarm-kernel-host/src/scheduler/calendar.rs) `generation_digest`, `occurrence_id`, `latest_due`; [cron](../../crates/swarm-kernel-host/src/store/automation_cron.rs) `configure_activation`; [scheduler](../../crates/swarm-kernel-host/src/scheduler.rs) `run` | [Current CheckRun schedules](../schedules.md), implementation E |
| Native Goal capability: 058 | [Codex](../../crates/swarm-adapter-codex/src/lib.rs) `next_observation`; [Muse](../../modules/muse/settings.mjs) `goalCommand`; [OpenCode wrapper](../../crates/swarm-kernel-host/src/runtime/opencode_v2/goal.rs) `execute_goal`; [pinned official sources](donor-map.md#native-goal-and-eliot-goal) | [Module API](module-api.md#harness-goal-and-eliot-goal), implementation N/P/Q |
| Candidate/review/correction/acceptance: next source chain | [submissions](../../crates/swarm-kernel-host/src/store/submissions.rs) `finish`, `request_changes_core`; [reviews](../../crates/swarm-kernel-host/src/store/reviews.rs) `reserve_assign`, `replacement_class`, `reserve_submit`; [disposition](../../crates/swarm-kernel-host/src/store/review_disposition.rs); [repair](../../crates/swarm-kernel-host/src/store/automation_repair.rs); [automatic acceptance](../../crates/swarm-kernel-host/src/store/automation_acceptance.rs); [acceptance](../../crates/swarm-kernel-host/src/store/acceptance.rs) | [Task policy](../task-policy.md), [Delivery](delivery.md), implementation G |

## Coverage still open

This is an index of research boundaries, not an additional implementation schedule.
The next concrete chain remains in the register.

| Existing implementation area | Coverage to carry forward / next unresolved boundary |
| --- | --- |
| A, M–Q: adapter results/recovery | Detailed selected traces above; not every retained command/installed contract. Historical capture across revocation, first-page discovery and crash/power-loss remain open. |
| B–D: launch/resource lifetime | AR-048 confirms missing finalizer; AR-049 bounds route-edit accounting; AR-050 traces shared claim/Open transaction. Retirement implementation, crash-only departure and installed concurrency/restart remain open. |
| E: scheduling and Goals | AR-051–058 traces selected policy through command selection/terminal consumer, disable/transfer and calendar identity. Implement ordinary bounded messages and native mappings; qualify installed Goals/restart. General action loop, scripts and all policy combinations remain open. |
| F–G: TaskPacket/review/acceptance | Next connected source pass: submitted candidate through reviewer assignment/replacement, all-findings correction, corrected submission and exact acceptance gate. AR-053 phase gap is established, not a full review/acceptance audit. |
| H: public/object permissions | Selected grants inspected, not an exhaustive method/role/object matrix. Preserve history/disclosure distinctions; do not grant blanket Operator rights to helpers. |
| I: Store formats | Structural review only. Actual reader/writer pairs and partial migration/corruption behavior remain incomplete; no compatibility framework is prescribed. |
| J: IPC/MCP | Inherited selected traces. Complete disconnect/replay/live boundary, slow consumers, fairness and writer occupancy. |
| K/U: Check/Script/Forge/GitHub | Inherited ownership review. Trace each retained external write through ambiguous response, exact readback and automatic successor action. |
| R: Governor observation | Source-backed target, not implemented joined fleet. Verify provider-local/OS/Git/remote joins, unmatched subjects, source gaps and independent collectors. |
| S/T: deletion/mailbox | Require positive replacements and concrete unsettled-data disposition; finish recipients, handover, cancellation and historical grants. |
| V: gateway | Targeted prior review; finish secret loading, principal mapping, disconnect and limits. |
| Installed/native/Windows/load | Separate unexecuted qualification. Neither an index entry nor a source finding is runtime evidence. |

## Historical requirements and donors

| Material | Use and evidence limit |
| --- | --- |
| [Original specification at f1b854b6](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md) | Requirements/intent source, not current code truth. The owner's later independent-Goals decision overrides older exclusive-owner wording. |
| [Retained remediation audit](../remediation/2026-10-09/CURRENT-AUDIT.md) | Historical findings/fixes. Follow its named source revision; it does not certify current `main`. |
| [Claude-script donor review](../remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md) | Named script paths and lessons from the private ZIP. Indexing this review is not reopening or executing that ZIP. |
| [Current donor map](donor-map.md) | Linked mechanisms, anti-donors and route choices. Selected official APIs/releases and exact native Goal source contracts checked 2026-10-23; locked packages, current adapter mappings and installed capability remain distinct. |
| [Refactoring findings](refactoring.md) | Larger reduction/deletion boundaries from earlier work; not a second finding-status authority. |
| [Preservation matrix](../implementation-status.md#historical-behavior-preservation-matrix) | 29 selected requirements/lessons with historical locators, target owner and gap. Not exhaustive traceability for every historical field or script. |

## Keep the index useful

Keep AR IDs and `ar-NNN` anchors stable. Add new evidence with inspected revision and
symbols; retain a refuted claim and its reason instead of silently deleting it. Link a
new checkpoint directly here when its detailed trace would otherwise require chasing
history. Do not rewrite unrelated findings, spawn an issue per row, or create a new
runtime registry for this document. A docs commit never closes a runtime defect.
