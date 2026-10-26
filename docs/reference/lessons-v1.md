# Lessons from v1

v1's last pre-reset commit is `dc6f70aa` and is archived as `v1-final`. Measured sizes
below were read from that tree on 2026-10-25. Operational observations come from v1
records/owner runs unless explicitly re-qualified; they are requirements/risk evidence,
not fresh benchmark claims.

## A. How the project rotted

| What happened | v2 rule | Enforced by |
| --- | --- | --- |
| About 533k Rust lines and 1,502 commits in 26 days; largest host crate about 365k lines and largest file 9,105 lines, while core live paths were not qualified. | Small crate budgets, connected vertical slices, no abstraction without current callers. Implement first with scoped Clippy; run one broad qualification campaign only after feature completion. | [roadmap](../roadmap.md), [AGENTS](../../AGENTS.md) |
| About 61k documentation lines: remediation trees, audit registers, packets, readiness reports, generated inventories and CI that checked verdict phrases. | One current spec per topic, one roadmap, compact donor/research references. No reports about reports. | [D-15](../decisions.md), AGENTS |
| Agents consumed huge histories and contradictory addenda; deleted rules remained active elsewhere. | Replace the owning text in place and delete superseded rules. Worker read order is vision → architecture → roadmap → linked spec only. | docs index, AGENTS |
| Symbols, receipts, grep hits, compiled fixtures and one-shot patches were reported as product completion. An external audit of the old swarm disputed most "done" claims, especially fallback/compatibility paths, code without callers and weakened guarantees. | During L0–L6 claim only implemented behavior plus scoped compile facts. Runtime guarantees are claimed only after L7 end-to-end/live qualification through the real route. No shim or fixture substitutes for it. | roadmap L7, AGENTS |
| Thousands of merges and hundreds of Issues still left most work partial. | Track finished connected roadmap tasks, not commit/PR count. One manager/worktree/PR owns one task. | roadmap, AGENTS |
| Multiple executors survived for each harness (Node/Python/Rust/host paths). | One selected plugin route per harness. Replacement deletes the old route in the same PR. | plugin protocol, AGENTS |
| Identity/proof ceremony spread everywhere: Markdown hashes, receipts echoing Store identity, executable/provenance hashes and handover epochs. | Plain logical ids, session generation, exact turn/request ids and SQLite transactions. Crypto only at a real trust boundary. | [core](../spec/core.md), [D-07](../decisions.md) |
| Core absorbed Task/Attempt/review/acceptance/publication/CheckRun engines not requested by the product. | No workflow engine. Use small rules/scripts/agents plus the minimal run/action journal needed for crash safety. | [D-06](../decisions.md), [automation](../spec/automation.md) |
| One-shot CI workflows edited code remotely and then deleted themselves. | Normal branch/worktree/PR writes through GitHub. Implementation gate is fmt + changed-package Clippy; qualification is a later explicit phase. | AGENTS, roadmap |
| Blanket prohibitions without a positive implementation path caused agents to stop or delegate routine choices. | Every constraint names the permitted path: exact readback, source ownership, explicit unsupported result, per-agent custody, or shared native service. | specs |

## B. Design mistakes not to repeat

| v1 mistake | v2 rule |
| --- | --- |
| Queues, lanes, dashboards, capacity and schedules each became separate state machines/databases. | Agent/command/event projections answer status. Only coordination data, goals/timers and automation run/action state get small dedicated tables. |
| Adapter persistence became universal journals, outboxes, tombstones and result assembly. | Native systems keep detailed history. Eliot stores bounded normalized facts plus native ids/pointers and exact unknown outcomes. |
| Version/provenance gates blocked work without preventing semantic failure. | Versions are reported and recorded with fixtures, not used as authorization gates. |
| `rusqlite` features pulled in bindgen/libclang for every build. | Use bundled SQLite without unnecessary bindgen hooks. |
| One-Issue workflow policy became Store transition machinery. | Project policy is an agent/rule/script concern; core does not know Issues/reviews. |
| "Strict command order" became a single blocking line, so a future queued send could obstruct control of the current turn. | Readiness lanes: ready native calls serialize, queued sends remain FIFO, exact current-turn/request controls may proceed. |
| A shared plugin/Job was treated as if it could force-stop one agent. | Process-owning harnesses use per-agent plugin custody; shared mode is only for native services with exact session isolation. |
| Live stream, transcript and hooks could all report the same lifecycle. | One authoritative source per event family/generation; cursor handoff or explicit gap. |

## C. Runtime facts worth preserving

| Field fact | v2 consequence |
| --- | --- |
| Lost reply is not evidence of non-application; replay duplicated prompts and pushes. | `unknown`, exact readback/manual resolution, no automatic retry—including stop/interrupt. |
| Silence, parent idle, wrapper exit or tunnel loss did not prove terminal state; children could continue unseen. | Stalled is a flag based on missing progress; lost needs exact custody/session evidence; children are independent agents. |
| Late results from an earlier binding could wake/complete replacement work. | Monotonic Eliot generation, rotated token, exact turn/request binding; stale input never mutates current projection. |
| PIDs were reused and stop helpers claimed success while descendants survived. | Identity is `(pid,start_time)`; custody enumerates and verifies the exact process group. |
| Git snapshots around every tool contended on the index; owner-observed OpenCode timings improved sharply when disabled. | Monitoring is read-only with `--no-optional-locks`; progress fingerprint hashes only already-dirty paths without index/object writes. |
| A cron written into a chat never ran; maintenance vanished after reboot; OS task lifetime diverged from controller ownership. | Timers belong to the daemon, have durable firing keys and visible in-flight runs; autostart does not imply session continuity. |
| Settings often applied only at next turn/session; requested model differed from effective. | `configure` reports application point; profiles/show keep requested and effective facts separate. |
| 429, quota exhaustion, model removal and auth failure were conflated. | Distinct provider-limited kinds and holds scoped to actual profile/account/proxy; no silent fallback. |
| Stopped sessions woke from late results, idle steer, background completion or native goals; multiple managers ended in one worktree. | Stop binds exact generation, clears native goal where needed, and old events/commands are stale. One manager owns one worktree. |
| Long Muse sessions degraded in owner runs. | Rotation is explicit: checkpoint, settle children/requests, then new generation; no timer-based core magic. |
| Status/failure messages caused recursive model turns and token storms in other systems. | Source status is on the bus, mail wakes are coalesced/recipient-controlled, automation has self/depth/rate guards. |
| Tests left orphaned large processes and storage. | Exact Jobs/process groups, bounded artifacts, read-only doctor and late qualification cleanup checks. |
| Windows broke on live script replacement, BOM/CRLF, long argv, PATH casing, console/Job inheritance and interactive Git. | Versioned scripts, UTF-8/LF, stdin/files for long input, explicit argv/handles, case-insensitive env merge and no editor/console. |
| Cleanup deleted native data/branches needed for recovery/open PRs. | Eliot never cleans vendor stores; worktree removal is explicit, clean-only and preserves branch. |
| Tiny exact tasks succeeded more reliably than broad Issue-sized prompts. | Roadmap tasks are small connected vertical changes with exact owner/spec boundaries, not packets or micro-PRs containing only scaffolding. |

## D. What v1 is allowed to donate

Read only the narrow mechanism named in [donors](donors.md): protocol parsing edge
cases, Windows process helpers, redaction and recorded vendor behavior. Do not port
v1's Store schema, workflow state machines, audit/remediation structure, proof packets,
compatibility executors or CI patchers.
