# Lessons from v1

v1's frozen pre-reset commit is `dc6f70aa`; roadmap L0.1 creates tag `v1-final` there
before deleting v1 code. Until then use the commit directly. Sizes below were measured
on that tree on 2026-10-25. Operational observations are risk evidence, not fresh
qualification.

## A. How the project rotted

| What happened | v2 rule | Enforced by |
| --- | --- | --- |
| About 533k Rust lines and 1,502 commits in 26 days; largest host crate about 365k lines and largest file 9,105 lines, while core live paths were not qualified. | Small crate budgets, connected vertical slices and no abstraction without current callers. Implement first with scoped Clippy; qualify the complete path once feature-complete. | [roadmap](../roadmap.md), [AGENTS](../../AGENTS.md) |
| About 61k documentation lines: remediation trees, audit registers, packets, readiness reports, generated inventories and CI that checked verdict phrases. | One current spec per topic, one roadmap, compact donor/research references. No reports about reports. | [D-15](../decisions.md), AGENTS |
| Agents consumed huge histories and contradictory addenda; deleted rules remained active elsewhere. | Replace the owning text in place. Read vision → architecture → roadmap → linked spec only. | docs index, AGENTS |
| Symbols, proof-receipts, grep hits, compiled fixtures and one-shot patches were reported as product completion. | Runtime claims require L7 real-route qualification. A narrow external WriteId receipt is only client idempotency data; it is not proof of product correctness. | roadmap L7, core WriteId contract |
| Thousands of merges and hundreds of Issues still left most work partial. | Track connected roadmap tasks, not commit/PR count. One manager/worktree/PR owns a task. | roadmap, AGENTS |
| Multiple executors survived for each harness. | One selected plugin route per harness. Replacement deletes the old route in the same PR. | plugin protocol, AGENTS |
| Identity/proof ceremony spread everywhere: Markdown hashes, policy editions, handover epochs and receipts echoing Store rows. | Keep only identities that close a concrete race: external WriteId, generation/turn/request, source open/sequence/native receipt and OS custody identity. No proof-document layer. | core/plugin/monitoring specs |
| Core absorbed Task/Attempt/review/acceptance/publication/CheckRun engines not requested by the product. | No workflow engine. Rules/scripts/Agents plus the minimum run/action journal needed for crash safety. | D-06, automation |
| One-shot CI workflows edited code remotely and deleted themselves. | Normal worktree/branch/PR. L0–L6 gate is fmt + scoped Clippy; qualification later. | AGENTS, roadmap |
| Blanket prohibitions without a positive path caused agents to stop or invent exceptions. | Every constraint names the valid path: exact readback, explicit unknown, source gap, reduced routing/authority or unsupported result. | specs |

## B. Design mistakes not to repeat

| v1 mistake | v2 rule |
| --- | --- |
| Queues, dashboards, capacity and schedules each became separate product state machines. | Add compact projections only where restart/readback requires them: bindings, commands, source watermarks/native receipts, input/turn/edit/workspace, mail/vars/claims and automation runs. No Task workflow. |
| Adapter persistence became universal journals, outboxes, tombstones and result assembly. | Plugin persists only its bounded open/sequence/spool/registration facts. Store owns normalized projections; native stores keep detailed history. |
| Version/provenance gates blocked work without preventing semantic failure. | Versions are reported/recorded; installed behavior and live qualification prove semantics. |
| `rusqlite` features pulled in bindgen/libclang for every build. | Bundled SQLite without unnecessary bindgen hooks. |
| One-Issue workflow policy became Store transition machinery. | Project policy remains rule/script/Agent behavior. |
| “Strict command order” became one blocking line, so a future send obstructed current turn control. | Readiness lanes: queued inputs FIFO; exact controls/answers proceed. |
| One shared plugin/Job was treated as exact per-Agent force-stop. | Process-owning harness per-Agent. Shared control only with native per-session isolation; shared PIDs stay service-owned. |
| Live stream, transcript and hooks could publish the same lifecycle. | One family owner; predecessor-aware atomic handoff or explicit gap. |
| Source sequence was treated as sufficient vendor dedupe. | Source sequence handles lost Store acknowledgement; strict vendor cursor or native-event receipt separately prevents replay after event retention. |
| Generation implied a usable Agent token. | Direct routing is explicit `routing_enabled`; parent-mediated native children may have generation/control without a token. |
| Project profile replacement could smuggle another route/account/environment. | Project profile is a narrowing overlay on an owner-defined global profile only. |

## C. Runtime facts worth preserving

| Field fact | v2 consequence |
| --- | --- |
| Lost reply is not evidence of non-application; replay duplicated prompts/pushes. | Unknown + exact readback/owner assertion; no automatic retry of possible effect. |
| Inbox/HTTP acceptance did not prove a turn, and turn association did not prove completion. | Separate command, input and turn projections; optional native input id; exact command→turn mapping. |
| Silence, parent idle, wrapper exit or tunnel loss did not prove terminal state; children continued unseen. | Stall is a flag. Lost requires source/session/control evidence; degraded custody may remain `orphan_possible`. |
| Late results from an earlier binding woke replacement work. | Monotonic generation, immutable Agent binding, exact turn/request and stale rejection. |
| PIDs were reused and stop claimed success while descendants survived. | Process identity is `(host_boot_id,pid,start_time)`; exact custody verifies container membership/empty state. |
| Unix process groups neither die with daemon nor prevent `setsid` escape. | Exact Linux requires cgroup/equivalent + guardian; process-group fallback is degraded and cannot claim exact force/all descendants dead. |
| Git snapshots around every tool contended on the index. | Monitoring is read-only/no-optional-locks; progress hashes already-dirty paths without index/object writes. |
| Cron written in chat never ran; maintenance vanished after reboot. | Timers/Goals/rules use durable trigger identities and startup reconciliation. |
| Settings often applied later; requested model differed from effective. | Configure reports application point; frozen requested binding and effective readback remain separate. |
| 429/quota/model removal/auth were conflated. | Typed limitation, scoped persisted hold, explicit user probe only. |
| Stopped sessions woke from late results/native Goals; managers shared one worktree. | Stop binds exact generation and clears native driver; one manager owns one worktree. |
| Long Muse sessions degraded. | Explicit checkpoint/settle/rotate to new generation; no timer magic. |
| Status/failure messages caused recursive model turns/token storms. | Status on bus; queue wake is coalesced; routing-disabled children use parent relay/queue-only; automation has depth/rate guards. |
| Tests left orphaned processes/storage. | Windows Job exact; Linux exact/degraded honest; bounded artifacts/spools/WAL and cleanup qualification. |
| Windows broke on live replacement, BOM/CRLF, long argv, PATH casing, console/handle inheritance and interactive Git. | Immutable/versioned artifacts, UTF-8/LF, stdin/files, explicit argv/handles, case-insensitive env merge, no editor/console. |
| Cleanup deleted native data/branches needed for recovery/open PRs. | Never generically clean vendor stores; worktree removal is explicit, clean-only and branch-preserving. |
| Tiny exact tasks worked better than broad prompts. | Small connected roadmap changes with exact owner/spec boundary, not packets or scaffolding-only micro-PRs. |

## D. What v1 may donate

Only the narrow mechanisms named in [donors](donors.md): Windows process helpers,
private files/IPC, bounded capture, redaction and protocol edge cases. Re-derive Linux
exact custody, source ownership, routing identity and Store schema from v2 specs; do not
port v1 workflow state, proof packets, compatibility executors or CI patchers.
