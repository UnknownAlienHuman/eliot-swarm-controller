# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS_WORKING_DRAFT / RUNTIME_REFACTOR_IN_PROGRESS.**

Repository inspected on **2026-10-23** at
`73ba7009bbc5c5515fe4c5f85743066e944106ef`. The latest runtime change is
`c92120ed70ca7e625440a8a19517c481e73b13c9` (2026-10-21). Comparing those revisions
shows **18 documentation-only commits**. This update also changes documentation only.

The previous checkpoint indexed AR-001–050. This pass continued the Goal policy chain
through admission, runtime selection and terminal evidence; it added AR-051–058 and
checked calendar occurrence continuity plus exact official Codex/Muse Goal sources.
The target now states phase-gated ordinary messages, finite budgets, disable/transfer
semantics and capability limits that a coding agent can implement and verify. No
runtime defect is marked repaired.

- [Resume point and 58 finding records](agent-operations/audit-register.md#resume-here).
- [Evidence/source index](agent-operations/audit-index.md): exact traces and remaining coverage.
- [Document index](agent-operations/README.md): current contracts versus historical evidence.
- [Implementation](agent-operations/implementation.md): the only work order.
- [Donor register](agent-operations/donor-map.md): what to reuse, source links and version limits.

The records include defects, preserved mechanisms and partial boundaries, not 58 bugs
or a measured audit percentage. Historical requirement mapping is selected, not an
exhaustive reread of all archives. The private Claude-script ZIP was not re-executed.

### GitHub work at the inspected checkpoint

No open PRs were returned. The following open Issues are evidence/remaining work, not
an alternative to the implementation order:

| Issues | Retained scope |
| --- | --- |
| [#1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1), [#3](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/3), [#4](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/4), [#5](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/5) | OpenCodex composition; OpenCode restart; family discovery; Muse native/Windows evidence. |
| [#140](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/140), [#159](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/159) | Antigravity and Claude/Command retained native/custody qualification. |
| [#11](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/11), [#153](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/153) | Quiet-machine load evidence and the historical main audit. |

Issue text can reference an old artifact or closed PR. Reopen the source named in this
status before coding; do not revive that branch or infer current qualification from it.

## Landed runtime changes

| Change | Retained evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains. |
| Active Antigravity route aligned on v6 | `cba98b7` | Not installed native-turn qualification. |
| Terminal receipt decoupled from required observation | `eeea355`, recorded net −265 production Rust lines for that commit | Dispatch proof and crash-unsafe delivery remain. |
| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930`, connected regression and minimal Store Clippy | Scoped patched-runner evidence; not rerun by this checkpoint and not crash/installed/scale proof. |

```text
A0   LANDED                  active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
B–V  TARGET WORK             largely unreduced current runtime; see implementation
```

A2 must connect adapter capture, ordinary Store settlement, exact page delivery,
historical completion authority and complete-source retention. A file spool alone
cannot repair queue/boot/release barriers. No native prompt is replayed and old output
keeps its original Operation/Attempt/boot. Unacknowledged Antigravity receipts and
uncaptured bodies remain at risk in current code.

## What the recent audit established

| Boundary | Recorded result | Not yet established |
| --- | --- | --- |
| Result/continuation/restart, AR-001–035 | Exact selected caller/Store/adapter traces, useful assembly/grants, cross-route delivery and source-cost findings | Every command/installed contract, first historical capture across revocation, live restart/power-loss. |
| Queue/root/provider, AR-036–044, 049–050 | History-dependent admission, pending saturation, native-default mismatch, stale precedence and route-edit accounting; common claim/Open transaction preserved | Installed restart/concurrency and corrected budget continuity. No over-admission demonstrated. |
| Shutdown/resource retirement, AR-045–048 | Non-ready shutdown barrier, intentional-detach/demand mismatch and missing attached-binding finalizer confirmed through the running sweep | Implementation of eventual retirement; actual detach/Goal/child and crash-only behavior. |
| Goal policy/admission/transfer, AR-051–056 | OpenCode actor rejection before dispatch; Codex policy-edit rejection of queued input; submitted/correction eligibility, missing finite bounds and unclaimed-event cursor gap. Exact terminal identity/atomic deduplication preserved | Ordinary Send replacement, phase/budget fixes and installed disable/transfer/restart. Native terminal still does not prove result capture or acceptance. |
| Calendar/native Goals, AR-057–058 | Current CheckRun occurrence continuity and latest-only catch-up; current native mappings separated from pinned upstream Goal contracts | General automation loop, Codex native Goal mapping and selected installed-native qualification. Muse's SDK lock does not identify its native binary. |
| Other implementation areas | Inherited structural and selected source reviews | Remaining chains listed in the audit index; no whole-project completion claim. |

No AR entry is closed merely because its target documentation was corrected. Preserve
refuted/narrowed conclusions: committed old-boot outcomes can replay-ACK; root records
already deduplicate; removing source identity checks cannot recover missing bytes.

## Historical behavior preservation matrix

Sources: [v18 specification](agent_swarm.md), its [reference examples](agent_swarm.spec-v18/README.md),
[owner decisions](owner-decisions.md), and [Claude-script review](remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md).
Section/example names below locate historical intent. Current owner decisions and the
linked target contract govern implementation. “Retained” means represented in that
target, not runtime-complete; current gaps remain explicit.

| Required behavior / historical locator | Target contract and source owner | Disposition / current gap |
| --- | --- | --- |
| Common commands; native model/tool loops — v18 §§1, 3 | [Module API](agent-operations/module-api.md#semantic-actions); `swarm-contracts`, selected adapter | Retained; vendor-method and executor duplication remains. |
| Exactly one initial input — v18 §4; `single-initial-dispatch.json` | [Assignment](task-policy.md#6-assignment-and-initial-input); `operations`, `Attempt.start_owner` | Preserve existing controller/native-manager guard. Follow-up Send is separate. |
| One native control owner — v18 §4; `native-owner-collision.json` | [Native ownership](agent-operations/module-api.md#capabilities-hello-and-native-owner); binding/native owner | Retained; no exclusivity claim against unrelated external clients. |
| Complete agent-managed work cycle — v18 §§10, 12; script review | [Delivery](agent-operations/delivery.md#1-one-connected-cycle); existing Store domain handlers | Separate workflows and automatic capture/readback gaps remain. |
| Independent native and Eliot Goals — later owner clarification; script review | [Goal policy](agent-operations/configuration.md#goal-message-and-native-goal); adapter + ordinary Send | Retained; old exclusive-driver rule superseded. AR-051/058 distinguish failed OpenCode automatic input and unimplemented Codex native mapping from real native Goal. |
| Bounded continuation/progress/rotation — script review | [Goal limits](agent-operations/configuration.md#goal-message-and-native-goal), [route/session policy](agent-operations/configuration.md#5-route-dependency-session-and-permission-policy); launcher/automation | Finite occurrence/no-progress contract specified; current Goal settings lack these bounds (AR-054). Phase guard gap AR-053; no age-only default. |
| Exact permission/question answers — v18 §§6–8; script review | [Semantic Reply](agent-operations/module-api.md#semantic-actions); pending-request adapter + Governor | Retained; capability-specific. Neither first-option/blanket-deny nor Send-as-Reply. |
| Scheduled work, same-occurrence dedupe, missed-tick policy — v18 §10; `schedule-catchup.json` | [Triggers](agent-operations/configuration.md#4-trigger-and-loop); existing schedule/Operation owners | Preserve current CheckRun generation/occurrence continuity and latest-only catch-up (AR-057); general Goal/action loop remains target work. |
| Corrections/unfinished work before new work without starvation — script review | [Queue](swarm-launcher-assignment-context.md#2-queue-is-a-projection); launcher | WorkDispatch copied state and 128-subject intake barrier remain. |
| Preparatory work with honest dependencies — `dependency-revalidation.json` | [Dependencies](task-policy.md#4-dependencies-start-policy-versus-acceptance-truth); claim/acceptance | `hold` or explicit preparatory mode; current claim gates every dependency. |
| Explicit route/model/effort/fallback — v18 §9 | [Route policy](agent-operations/configuration.md#5-route-dependency-session-and-permission-policy); launcher | Retained; no silent substitution. AR-049 records edit-accounting gap. |
| Reuse/drain/retire actual resources — v18 §§4, 8; script review | [Launcher lifecycle](swarm-launcher-assignment-context.md#scoped-shutdown-and-retirement); runtime/workspace | Attached-binding finalizer absent, AR-048. Closing a link is not native death. |
| All findings together, missing reviewer replaceable — script review | [Correction](agent-operations/delivery.md#9-review-correction-and-resubmission); reviews/submissions | Existing replacement classes stay; RepairDispatch and wrong no-effect classification remain. |
| Every requirement and exact candidate verified — v18 §§11–12; R10/R11 | [Evidence](task-policy.md#3-requirements-and-evidence); reviews/checks/acceptance | Retained; common direct/automatic gate still requires source work. |
| Compact TaskPacket with real criteria — v18 R10; script review | [TaskPacket](task-policy.md#taskpacket); `task_prompt`, Task/Attempt owners | Prompt proof/hash envelope remains; do not delete executable criteria. |
| Quota, activity, connection and capacity separate — v18 §§8–9, 14 | [Provider facts](swarm-launcher-assignment-context.md#6-provider-conditions-and-non-work-commands); provider_conditions/capacity | Stale-copy precedence and history-dependent ledgers remain. |
| Passive joined observation, unmatched subjects and gaps — v18 §8; script review | [Observability](agent-operations/observability.md); adapter/OS/Git/remote collectors | Joined fleet is target work; no empty-fleet inference from a failed source. |
| Background, cancel, detach and child completion differ — v18 §§4, 8 | [Module actions](agent-operations/module-api.md#semantic-actions); selected adapter | Retained only where native capability supports exact semantics. |
| Uncertain native/external effects never blindly replay — v18 §§7, 13 | [Readback](agent-operations/delivery.md#8-readback-of-uncertain-effects); selected effect owner | Automatic readback gaps remain; unknown fences exact resource only. |
| Complete attributable results survive failure — v18 §§7, 13; retention example | [Capture](agent-operations/delivery.md#7-complete-result-capture); results/artifacts + adapter | A2 and cross-route source/continuation/successor-delivery gaps remain. |
| One manager/worktree, disjoint writers, frozen review — owner policy; v18 §11 | [Workspace](swarm-launcher-assignment-context.md#7-workspace-and-assignment); lease + manager Git owner | Retained; old main-only and separate code-scope machinery superseded. |
| Exact checks with scoped resource ownership — v18 §11; check-resource example | [CheckRunner](check-runner.md); Store checks + selected worker | Preserve implemented ownership; cache/native/load proof is separate. |
| Required script controls under ordinary rights — script review | [Script policy](agent-operations/configuration.md#7-script-policy); `swarm-scripts` + worker | Current returned-action enum covers message/notification/task-create only. |
| Peer questions without council platform — v18 §§10, 16 | [Communication](agent-communication-program.md); mailbox/GM | Keep recipients and object grants; coordination/global-timeline machinery remains. |
| GitHub collection/import/write and acceptance distinct — v18 §13 | [Publication](agent-operations/delivery.md#10-acceptance-and-publication); GitHub owners + Forge | Duplicate writers/projection workflows remain; policy automation retained. |
| Handover keeps work and historical recipients — owner decisions §5 | [Communication](agent-communication-program.md), [policy transfer](agent-operations/configuration.md#8-transfer-and-disable); client/GM/mailbox/automation owners | Stable client and historical owner stay; AR-052/055 distinguish admitted intent from a still-unclaimed blocked Goal subject. Normalization/cleanup gaps remain. |
| Restart cannot kill unrelated PID or duplicate owner — v18 §§8, 15 | [Process custody](agent-operations/implementation.md#d--process-custody); `swarm-process` | PID/birth/image/Job checks retained; full race matrix unqualified. |
| Independent module update; install is not execution — v18 §§3, 15 | [Installation](agent-operations/module-installation.md), [modularity](agent-operations/modularity.md); installer/supervisor | Selected current route only; no automatic transfer of live bindings. |
| One fixed remote identity/profile — v18 §§2, 15 | [Gateway](agent-operations/implementation.md#v--small-single-tenant-gateway); `swarm-gateway` | Target explicit; secret loading/compatibility launcher still need source work. |

Old work orders, mandatory main-only checkout, exclusive Goal driver, age-based kill,
proof hashes as authority and routine human approval are not preserved requirements.
Their useful behavior is assigned above to existing owners. Removing machinery must
connect that positive path in the same slice. This selected map does not certify every
historical field, test or script.

## Source audit versus runtime qualification

A completed source audit needs a named revision and evidence for retained actions:
caller → parse → exact authority → Store transition → effect/readback → consumer,
including cancellation, late result, restart, handover and partial writes. It also
needs historical requirement mapping, agreement between active contracts and concrete
replacement/disposition for each deletion. These remain incomplete globally.

Executed qualification is a separate record: privilege/migration/corruption cases,
installed provider contracts, Windows custody, fairness/load and autonomous end-to-end
work. Most of that remains unexecuted. Source inspection cannot substitute for it, and
missing a live machine does not excuse leaving source paths untraced.

## Next work

**Audit:** the selected Goal-policy/terminal/disable/transfer and calendar occurrence
chains are source-traced. Resume candidate submission → reviewer assignment/replacement
→ all-findings correction → corrected submission → acceptance in
[audit-register](agent-operations/audit-register.md#resume-here). The
[audit index](agent-operations/audit-index.md) keeps other untraced boundaries visible.

**Runtime:** implementation still starts with the unfinished Antigravity A1b–A3 path;
B–V remain the single subsequent order. Use existing owners, remove replaced machinery
and keep ordinary automation connected. During construction run minimal affected
format/Clippy/build; connected and installed/load checks follow the usable path.

This checkpoint changes documentation only: production Rust/JS/glue and tests are
unchanged. Content, local links/anchors and TOML syntax are checked separately from
runtime qualification; no new Clippy/test/benchmark or installed-runtime pass is claimed.
