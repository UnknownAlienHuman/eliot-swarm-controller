# Lessons from v1

v1's frozen pre-reset commit is `dc6f70aa`; L0.1 creates tag `v1-final` there before
deleting v1 from `main`. Measured sizes below were read from that tree on 2026-10-25.
Operational observations are owner/v1 records unless later re-qualified.

## How the project rotted

| What happened | v2 prevention |
| --- | --- |
| About 533k Rust lines and 1,502 commits in 26 days; largest crate about 365k lines and largest file 9,105, while core live paths remained unqualified. | Small crate budgets, connected vertical tasks and no abstraction without current callers. Implement first with scoped Clippy; run one complete qualification campaign after feature completion. |
| About 61k documentation lines: remediation trees, audit registers, packets, readiness reports, generated inventories and verdict-grep CI. | One current spec per topic, one roadmap and compact donor/research references. No reports about reports. |
| Agents consumed contradictory addenda and huge histories; removed rules remained alive elsewhere. | Replace the owning text in place; worker reads vision → architecture → roadmap → linked spec/donor only. |
| Symbols, receipts, grep hits and compiled fixtures were reported as product completion. External audit disputed most “done” claims, especially fallback/compatibility paths, code without callers and weakened guarantees. | During implementation claim only connected behavior and the compile gate actually run. Product readiness requires L7 real-path qualification, not paperwork. |
| Thousands of merges and hundreds of Issues still left most work partial. | Count completed connected roadmap tasks, not commits/PRs. One manager owns one worktree/PR. |
| Several executors survived per harness. | One selected plugin route. Replacement deletes the old route in the same PR. |
| Identity/proof machinery spread everywhere: document hashes, policy editions, receipts echoing Store identity, GM epochs and handovers. | Copy donor boundaries only: stable Agent, one generation fence, exact turn/request, explicit principal and ordinary SQLite transactions. No binding revision/proof platform. |
| Core absorbed Task/Attempt/review/acceptance/publication/CheckRun engines never requested by the product. | No workflow engine. Automation actions are ordinary commands; rules/timers/Goals keep only their minimal current projections. |
| One-shot CI workflows edited code and deleted themselves. | Normal branch/worktree/PR writes. Implementation gate is fmt + changed-package Clippy; tests become one later qualification system. |
| Blanket prohibitions lacked a positive path. | Every constraint names the permitted mechanism: exact readback, generation fence, cursor/gap, explicit unsupported result or degraded custody. |

## Design mistakes not to repeat

| v1 / over-correction | v2 rule |
| --- | --- |
| Queues, lanes, dashboards, capacity and schedules each became separate state machines/databases. | Agent/command/event projections answer status; coordination and rule/timer/Goal current state get small tables only. |
| Adapter persistence became universal journals, outboxes, tombstones and result assembly. | Native systems keep detailed history; Eliot stores bounded facts, current projections and opaque cursors. |
| Later v2 drafts replaced that with binding revisions, source epochs, activation hierarchies and universal receipts. | Use the donor-proven middle: one generation, one family owner, one cursor, one current+retiring rule definition, write id bound directly to principal/method/payload. |
| Version/provenance gates blocked work without preventing semantic failure. | Versions are displayed/recorded with fixtures, never authority gates. |
| One-Issue policy became Store transitions. | Project policy is rules/scripts/Agents; core does not know Issues/reviews. |
| “Strict command order” put future sends ahead of current-turn control. | Readiness lanes: ready native calls serialize, queued future sends leave the lane; unknown input/turn blocks only turn-producing work. |
| A shared plugin/Job was treated as exact per-Agent custody. | Process-owning harnesses use per-Agent plugins; shared mode needs proven native session isolation. |
| Live stream, transcript and hooks could all emit the same lifecycle. | One source owner per family/generation; cursor handoff or explicit gap. |
| No incarnation fence let late old-process output mutate a resumed session. | Stable Agent row plus one monotonic generation, copied from Agent Sessions/sandbox-driver patterns. |

## Runtime facts retained

| Field fact | v2 consequence |
| --- | --- |
| Lost reply is not evidence of non-application; replay duplicated prompts and pushes. | `unknown`, exact readback/manual resolution and no automatic retry—including interrupt/stop. |
| Queue acceptance, turn start and turn result are different. | Separate linked input/turn projections; unknown input blocks later send/configure/Goal. |
| Silence, parent idle, wrapper exit or tunnel loss did not prove terminal state; children continued unseen. | Stall is a flag; stream break is stale; process exit drains authoritative source before loss; children are independent Agents. |
| Late output from an older controller woke/completed replacement work. | Generation on every command/event/turn/request/claim/Goal; stale input is historical only. |
| PIDs were reused and descendants survived claimed stops. | `(host_boot_id,pid,start_time)` identity; exact Job verification; Unix honestly degraded. |
| Git snapshots around every tool contended on the index; owner-observed OpenCode timings improved sharply when disabled. | Monitoring is read-only with `--no-optional-locks`; progress fingerprint never writes index/objects. |
| Cron inside a chat never fired; maintenance vanished after restart. | Timers belong to daemon, persist slots and reconcile from Store. |
| Requested settings differed from effective and often applied later. | `configure` reports application point; requested/effective facts remain separate. |
| 429, quota, model removal and auth were conflated. | Distinct provider-limit kinds and durable account/route holds; no silent fallback. |
| Stopped sessions woke from late results/background/native Goals. | Stop binds generation/turn, clears native Goal where needed; old events/commands stale. |
| Long Muse sessions degraded. | Rotation is an explicit rule: checkpoint, settle children/requests, then new generation. |
| Status/failure chatter recursively spent tokens. | Status stays on bus; addressed mail; coalesced generic wake; rule self/depth/rate guards. |
| Tests left huge orphan processes/storage. | Exact Windows Jobs, degraded-Unix disclosure, bounded artifacts/streams and later doctor/load qualification. |
| Windows broke on live script replacement, BOM/CRLF, long argv, PATH casing, console/handle inheritance and interactive Git. | Frozen script bytes/digest, UTF-8/LF, stdin/private files, explicit handles/argv and no Git prompts/editor. |
| Cleanup deleted native history/branches needed for recovery/open PRs. | Eliot never generically deletes vendor stores; worktree removal is explicit clean-only and preserves branch. |
| Tiny exact assignments succeeded more reliably than broad Issue prompts. | Roadmap tasks are small connected implementation units; no packet bureaucracy or placeholder micro-PRs. |

## What v1 may donate

Only the narrow helpers named in [donors](donors.md): Windows Job/process identity,
bounded pipe capture, local IPC/private-file helpers, protocol edge cases and optional
redaction. Never port v1 Store/workflow schema, proof/receipt types, adapter journals,
compatibility executors, audit/remediation machinery or CI patchers.
