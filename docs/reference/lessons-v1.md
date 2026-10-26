# Lessons from v1

v1's last commit before the reset is `dc6f70aa`; roadmap L0.1 tags it `v1-final`. Numbers
in section A marked "measured" were taken from that tree on 2026-10-25; the rest come
from v1's own records (`git show dc6f70aa:docs/lessons-learned.md`), which are
owner-machine reports, not re-run tests.

## A. How the project rotted

| What happened | Rule for v2 | Where enforced |
| --- | --- | --- |
| (measured) 533,000 lines of Rust and 1,502 commits in 26 days for a controller whose core paths were never qualified on a real harness. Largest crate (`swarm-kernel-host`) 365k lines, 300k without test files; largest file 9,105. | Code budgets per crate; a layer is done only when its exit test passes. | [roadmap](../roadmap.md#code-budgets), CI `just loc` |
| (measured) 61,000 lines of docs: 177 remediation files, an audit register with 170 findings, slice registers, task packets, readiness reports, generated inventories, and a CI job that grepped for the phrase "STRUCTURALLY READY FOR CONTROLLED PACKETED IMPLEMENTATION". Work became writing documents about documents. | Docs are spec + roadmap only. Status = roadmap checkboxes + tests. | [D-15](../decisions.md), [AGENTS.md](../../AGENTS.md) |
| Agents received huge histories, old queues and contradictory addenda; a rule removed in one file stayed alive in another. | One current spec per topic; change it in place; delete superseded text. | AGENTS.md |
| A symbol, a receipt, a grep hit or a compiled fixture was reported as "done". In the script-driven swarm run of 12.09–06.10 that Eliot was built to replace, an external auditor refuted 218 of 243 "done" claims; the top defect classes were fallback/compat instead of the real guarantee (113), code without a caller (81), weakened guarantee (76). | Done = the layer's end-to-end test passes through the real path. No shims, no dead code, no fixture standing in for the real path. | roadmap exit tests |
| Same swarm run: 2,721 merges over 545 Issues, but only 37 Issues closed after 22.09 and 470 stayed PARTIAL. More PRs did not mean more finished work. | Count finished roadmap tasks, not PRs. One task per PR. | roadmap |
| Several executors per harness survived every migration (Python bridge, Node bridge, built-in host executor, standalone Rust adapter). | One plugin per harness. Replacing one means deleting the old one in the same PR. | AGENTS.md |
| Identity and proof ceremony everywhere: owner policy pinned by SHA-256 of a Markdown section inside a test, receipts echoing Store identity, provenance and executable hashing per operation, GM epochs and handovers. | Plain ids and SQLite transactions. No document hashes. Crypto only at a real trust boundary (none inside one host). | [D-07](../decisions.md), [core](../spec/core.md) |
| The core grew a Task/Attempt/review/acceptance/publication/CheckRun/ScriptRun engine (e.g. `store/checks.rs` 8.5k lines, `store/coordination.rs` 8.9k) that the product description never asked for. | No workflow engine in the core; workflows are rules + scripts + agents. | [D-06](../decisions.md) |
| Agents patched `main` through "one-shot" CI workflows that edited code remotely, then removed them. | Changes go through normal commits and PRs that pass `just check` locally and in CI. | AGENTS.md |
| Blanket "do not" rules without the positive path made agents stop or hand routine work back to a human. | Every spec says what to build; unsupported cases fail fast with a reason. | specs |

## B. Design mistakes not to repeat

| v1 mistake | v2 rule |
| --- | --- |
| Queues, lanes, dashboards, capacity and schedules each became their own state machine and database. | Views are queries over agents/events/commands. Besides those, only automation (timers, goals) and coordination (messages, vars, claims) keep small tables. |
| Adapter persistence grew into universal journals, outboxes, tombstones and result paging (24 KiB pages, assembly, coverage). | Plugins keep native state natively; Eliot stores bounded final text plus a pointer to the native transcript. |
| Version/provenance gates (expected versions, descriptor hashes) blocked work without preventing failures. | Versions are reported, never gates ([D-17](../decisions.md)). |
| `rusqlite` with `preupdate_hook` required `libclang` for every build. | Use bundled SQLite without bindgen features. |
| Policy for one Issue at a time was encoded in Store transitions. | Policy belongs in rules and config, not schema. |
| A process and Job shared by many sessions was treated as if it could force-stop one agent exactly. | One plugin process and one Job per agent ([D-03](../decisions.md)). |

## C. Runtime facts worth keeping

These were observed in the field and still shape the specs.

| Fact | Consequence in v2 |
| --- | --- |
| Lost reply ≠ not applied. Replaying an uncertain prompt duplicated expensive work and pushes. | `unknown` state, readback only ([D-14](../decisions.md)). |
| Silence, parent idle, wrapper exit, tunnel loss are not terminal evidence. A parent vanished from "active" while its children kept working; Codex reported 0 children while 4 ran. | `stalled` is a flag; `lost` needs process evidence; children are tracked as agents. |
| PIDs are reused; a stop helper reported success while Cargo descendants survived. | Process identity = PID + start time; `stop --force` verifies. |
| Unix process groups neither die with the controller nor stop a child that calls `setsid`. | Unix custody is reported `degraded`; only Windows Jobs (and later cgroups) are exact. |
| Git snapshots before/after every tool competed for the index; disabling them cut median read time 14 s → 1.9 s and edit time 42 s → 4.9 s (owner-reported, OpenCode 2.0.7). | Git monitor uses `--no-optional-locks`, never writes, rate-limited per worktree. |
| A cron set inside the GM's chat never fired; a maintenance runner was forgotten after reboot; a Windows scheduled task outlived its owner. | Timers belong to the daemon and are visible in `eliot timer list`. |
| Settings often apply only to the next session (e.g. Codex `agents.max_concurrent_threads_per_session` is read at `thread/start`). Requested model ≠ effective model. | `configure` reports `applies`; profiles show requested and effective values. |
| 429 is a rate limit, not quota; models disappeared overnight (404); free tiers share account limits. | `provider.limited.kind` distinguishes them; no global "wait one hour" rule. |
| Stopped sessions were woken by late results, steer into idle sessions, background shell completion, or a server-side Codex goal — two managers ended up in one worktree. | A stopped agent accepts no `send`/`steer`; stopping Codex also clears its native goal and interrupts the turn. |
| Long sessions degraded: a Muse session with 7 subagents grew to 233 MB in 6.5 h; throughput 7.8 Issues/h right after a restart vs 1–3/h late in a session. | Rotation is a rule (`on` turns/age → checkpoint + `resume` or new agent), not core magic. |
| Status and failure comments triggering full agent turns caused ping-pong loops (Multica: ~18M tokens). | Self-trigger guard and loop depth limit in [automation](../spec/automation.md#guardrails). |
| Test runs left orphaned `surreal.exe` (~4 GB each) and `git fsmonitor` processes; a WAL grew to 109 GB; build dirs to 143 GB. | Job object kill-on-close; orphans and disk usage in `eliot doctor`. |
| Windows: editing a running script caused syntax errors; BOM/CRLF, escaping, an interactive Git editor and long argv broke tooling; UAC/Job/console settings failed differently. | Scripts are files never edited while running; UTF-8 without BOM; long input via files; `GIT_TERMINAL_PROMPT=0`, no editor; no console windows for children. |
| Cleanup ran in the wrong order: deleting Codex rollouts before archiving left 1,841 threads Codex could not archive; "delete all branches" would have closed open PRs. | Eliot never deletes native data; cleanup of native stores is the harness's own command. Worktree removal keeps the branch. |
| A one-line micro-task (1 file, 1 test, exact command) succeeded 10/10 first time on weak models; Issue-sized tasks on weak models produced rethinking and PARTIAL. | Roadmap tasks are PR-sized with exact exit tests. |
