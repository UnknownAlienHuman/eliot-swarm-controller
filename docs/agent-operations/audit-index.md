# Audit index

Navigation for source research. [Current audit](audit-current.md) owns the active resume
point and newest findings. The frozen [AR-001–078 register](audit-register.md) and the
immutable snapshots below retain earlier detailed evidence. [Implementation](implementation.md)
owns source changes; [status](../implementation-status.md) owns landed behavior.

## Start after an interruption

1. Read [Resume here](audit-current.md#resume-here).
2. Compare its inspected head with live `main`.
3. Reopen changed callers/consumers only.
4. Continue the named exact chain and update the current register plus owning contract.

Current checkpoint: source inspected at
`38db378c559dd0d95cd60db26702ffa73a954258` on 2026-10-24. Runtime source remains
`c92120ed70ca7e625440a8a19517c481e73b13c9`; the 22 intervening commits are
documentation-only. No runtime repair or executed qualification is implied.

<a id="evidence-checkpoints"></a>
## Evidence checkpoints

| Entries | Immutable detailed snapshot | Subject |
| --- | --- | --- |
| AR-001–005 | [`c3e331a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md) | Antigravity reporting, body lifetime, identity, restart and admitted pages. |
| AR-006–008 | [`70b99db`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md) | Continuation results, assembly and authentication/completion. |
| AR-009–011 | [`1c61629`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md) | Send attribution, repair classification and same-Attempt correction. |
| AR-012–016 | [`ec01905`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/docs/agent-operations/audit-register.md) | Revision/release fences, cleanup, stable credential and old boot. |
| AR-017–019 | [`f5ccc41`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f5ccc418ba401733d1f778619abeec5c0554416b/docs/agent-operations/audit-register.md) | Recovery queue, supervisor scope and inconclusive lookup. |
| AR-020–024 | [`ac3389c`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ac3389c013840b02a78dc310bcee481d24086402/docs/agent-operations/audit-register.md) | Late terminal, historical capture and automatic readback. |
| AR-025–031 | [`9c47d88`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/9c47d88a09a543e9f0ea74228370833f14a6c619/docs/agent-operations/audit-register.md) | Cross-adapter sources/delivery and Store-owned coverage. |
| AR-032–035 | [`a0bc1dc`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/a0bc1dcb69fa4a2d3dea1e51e3c5b0d148bde1bb/docs/agent-operations/audit-register.md) | Historical guards, read amplification, Muse discovery and grants. |
| AR-036–040 | [`c0d1b91`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c0d1b91f15636982f977768474f7246b97308d61/docs/agent-operations/audit-register.md) | Capacity history, default model, saturation, stale quota and dedup. |
| AR-041–044 | [`c2b998b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c2b998b079fe8517c592192c18d6ecbfb6b26aae/docs/agent-operations/audit-register.md) | Quota bypass, cancelled launch, rejected Open and root limit. |
| AR-045–047 | [`3c4c859`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3c4c8599f1614460fe69d54d0b59715a26ada797/docs/agent-operations/audit-register.md) | Shutdown readiness, detach demand and resource retirement. |
| AR-048–050 | [`73ba700`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/73ba7009bbc5c5515fe4c5f85743066e944106ef/docs/agent-operations/audit-register.md#trace-2026-10-23-retirement-admission) | Missing finalizer, route budgets and common launch transaction. |
| AR-051–058 | [`4af640e`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/4af640ea6edc190d58a7320674c85cbc5bd3c6b3/docs/agent-operations/audit-register.md#trace-2026-10-23-goal-policy) | Goal actor/phase/budget/transfer and native capability. |
| AR-059–064 | [`5b6b5be`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/5b6b5bee92123ce7d3252054c939fb18619b790f/docs/agent-operations/audit-register.md#trace-2026-10-23-review-acceptance) | Review replacement, correction, fairness and acceptance provenance. |
| AR-065–070 | [`8d8a446`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/8d8a44652d110f29031b2ddfdf6db98a5fc7001f/docs/agent-operations/audit-register.md#trace-2026-10-23-checkrun) | Fixed source, CheckRun custody, parser evidence, cache and executor cut. |
| AR-071–078 | [`38db378`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/38db378c559dd0d95cd60db26702ffa73a954258/docs/agent-operations/audit-register.md#trace-2026-10-23-scriptrun) | ScriptRun custody/recovery, actions, readback, cancellation and transfer. |
| [AR-079–088](audit-current.md#findings-ar-079088) | [Current trace](audit-current.md#trace-2026-10-24-forge-github-effects) | Forge lease/readback, GitHub label/PR ownership and automation recovery. |

There are 88 entries. Some preserve correct behavior or narrow an earlier claim; they
are not 88 defects or a completion percentage.

## Source and target map

| Responsibility / AR | Current source entry points | Target contract |
| --- | --- | --- |
| Antigravity/result recovery, 001–035 | `runtime/warm_stream`, Antigravity controller, normalized result, result/object-scope owners and selected adapters | [Module API](module-api.md), implementation A/M–Q |
| Queue/provider/resources, 036–050 | `store/{capacity,provider_conditions,launcher,operations,workspace_lifecycle,module_demand}` | [Launcher/Queue](../swarm-launcher-assignment-context.md), implementation B–D |
| Goal/scheduling, 051–058 | `automation_goal_progression`, `scheduler/calendar`, runtime and adapter Goal mappings | [Configuration](configuration.md), implementation E/N/P/Q |
| Review/correction/acceptance, 059–064 | `store/{submissions,reviews,review_disposition,acceptance}` | [Delivery](delivery.md), implementation G |
| Fixed source/CheckRun, 065–070 | `store/checks`, `checks/{source,worker,standalone_host}`, `swarm-checks` | [CheckRunner](../check-runner.md), implementation K |
| ScriptRun/actions, 071–078 | `store/scripts`, `scripts/runner`, `swarm-script-worker`, `swarm-scripts`, automation intake/dispatch | [ScriptRunner](../script-runner.md), implementation K |
| Forge/GitHub effects, 079–088 | `store/forge`, `swarm-forge-worker`, `store/{github_effects,github_pr_effects,automation_publication,automation_github_projection}` | [External effects](external-effects.md), implementation K/U |
| GitHub collection/import next chain | `github/{client,observer,pulls}`, `store/github` and work-pool/import consumers | implementation U0–U2/U5 |
| Observation | module observations, process metrics, workspace/Git and remote collectors | [Observability](observability.md), implementation R |
| Mailbox/deletion | mailbox and coordination products | [Communication](../agent-communication-program.md), implementation S/T |
| Gateway | `swarm-gateway` and compatibility launcher | [Gateway](../gateway.md), implementation V |

## Coverage still open

| Area | Remaining connected boundary |
| --- | --- |
| A, M–Q adapters | Durable StoreDelivery, first historical capture, continuation sources and installed restart/version qualification. |
| B–D launch/resources | Attached-binding retirement, cancellation disposition, stable route-budget identity and live custody races. |
| E scheduler/Goals | One ordinary loop, finite no-progress/usage budgets, native/Eliot combination and restart. |
| F–G Task/review | TaskPacket, automatic replacement, finite correction, subject isolation and common review/finding provenance. |
| H permissions/API | Complete method/role/object/revision matrix and deletion of internal/vendor public surfaces. |
| I Store | Reader/writer pairs, numeric migrations, partial migration and corruption behavior. |
| J IPC/MCP | Disconnect/readback, slow consumers, fairness and writer occupancy. |
| K/U effects | Script action lifecycle/cancel; Check executor cut; Forge process-hold disposition; one GitHub API owner; automatic unknown readback; collector/import separation. |
| R observation | Independent provider/process/Git/remote collectors, unmatched subjects, source gaps and one joined view. |
| S/T mailbox/deletion | Recipient-indexed mailbox and concrete transition before coordination deletion. |
| V gateway | Private secret loading, session limits, disconnect and launcher deletion. |
| Installed/native/Windows/load | Separate executed qualification; source evidence is not a substitute. |

## Historical requirements and donors

- [Original specification at `f1b854b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md)
  is intent evidence, not current code truth.
- [Remediation audit](../remediation/2026-10-09/CURRENT-AUDIT.md) and
  [Claude-script donor review](../remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md)
  remain tied to their named revisions.
- [Donor map](donor-map.md) owns current reuse/version decisions.
- [Refactoring findings](refactoring.md) owns structural deletion boundaries, not
  finding dispositions.

## Maintenance rule

Keep AR IDs stable. Record inspected revision, exact symbols, preserved behavior,
uncertainty and the next chain. Update the owning target contract in the same change.
Do not create an Issue, workflow or worker per finding.
