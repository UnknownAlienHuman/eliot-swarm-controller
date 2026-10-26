# Decisions

Short records of choices not obvious from the specs. Change an entry in the PR that
changes the product; never append a contradictory addendum.

| ID | Decision | Why |
| --- | --- | --- |
| D-01 | **v2 is a fresh layered build.** v1's frozen pre-reset ref is commit `dc6f70aa`; L0.1 creates `v1-final` there before deleting v1 from `main`. v1 is a narrow donor, not a refactor base. | v1 reached about 533k Rust lines and 61k process/audit docs without qualified core paths. |
| D-02 | One `eliot` binary contains CLI, daemon entry, built-in plugin entry, hook shim and optional MCP entry. | Simple installation/versioning; no sibling-artifact provenance platform. |
| D-03 | Plugins speak JSON-RPC over stdio. A plugin that owns native processes runs **per agent generation**; shared mode is allowed only for a native service that already isolates sessions and can stop one without killing the service. | Exact custody and honest force-stop matter more than one small controller process. Fake and service-backed harnesses can still multiplex. |
| D-04 | One SQLite database in WAL mode, one writer task, numbered `user_version` migrations. | Enough for one host/millions of rows; no database server or schema-hash ceremony. |
| D-05 | Persist bounded normalized events with global `seq`; deltas/samples are ephemeral. Native transcripts keep full detail. | Replayable state without token-stream storage. |
| D-06 | Core has no Task/Issue/review/acceptance/publication engine. | Not requested; it was v1's largest source of code/bureaucracy. Workflows are rules/scripts/agents. |
| D-07 | One local OS user is the trust boundary. Agent tokens provide current-generation attribution/routing, not isolation from that user. | Agents can already read the same local files. Real remote/multi-user auth belongs to a later gateway. |
| D-08 | Agent control is `self`, `tree` or `project`; owner and automation have owner authority. | Covers manager/child/project operation without GM epochs/handovers. |
| D-09 | Eliot Goal is available for any harness with budgets/no-progress. Native Goal is used where qualified; one of each may coexist. | Owner requirements need automatic continuation; guardrails replace the old blanket prohibition. |
| D-10 | Rules are strict TOML in project/data-dir, hot-reloaded; `every`, cron and one-shot timers are daemon-owned. | Programmability/cron are explicit owner requirements. |
| D-11 | `who` combines live Git facts, generation-matched native file changes and advisory claims; history is provenance, not authority. | Answers the requested question without pretending Git knows uncommitted authors in a shared checkout. |
| D-12 | Claims are advisory and never lock files. | Eliot cannot enforce a lock against an unrestricted native shell; a fake lock is worse than a visible warning. |
| D-13 | First real harness is Codex app-server; first proxy is OpenCodex. | Codex exposes threads/turns/steer/interrupt/requests/goals over a documented headless protocol and fits a Rust first slice. |
| D-14 | **No unknown native/external effect is retried automatically—not even interrupt or stop.** | A late retry can hit a newer turn/session; exact readback or owner resolution is the only safe settlement. |
| D-15 | `docs/` contains current specification, roadmap and compact evidence references only. Status is roadmap checkboxes during implementation and executable L7 qualification after feature completion. | v1 spent most later work writing readiness/audit documents about other documents. |
| D-16 | Documentation is English; the owner's source statement in vision remains verbatim Russian. | Implementers/vendor docs use English; original intent stays exact. |
| D-17 | Harness/software versions are reported/recorded with fixtures, never authorization gates. | Version/provenance gates blocked work but did not establish semantics. |
| D-18 | Early stdio sessions are daemon/per-agent-plugin owned and become lost if that custody dies. Daemon stop refuses active work unless drain/force. Service mode comes only after exact re-attach is qualified. | Orphan-free first; continuity later through a real service, not detached ambiguous processes. |
| D-19 | Native and Eliot goals are independent. `on_idle` Eliot goal waits while native goal actively drives; explicit `every` may still send. | Preserves native autonomy and owner choice without two accidental idle loops. |
| D-20 | Commands use readiness lanes, not one fake total order. Ready native calls serialize; queued sends stay FIFO but do not block exact control/answer/configuration that can apply now. | Future work must not obstruct control of the current turn. |
| D-21 | Per-profile/proxy `max_live` queues launches. | Subscription/quota/capacity is account/route scoped; Eliot does not overload or silently reroute it. |
| D-22 | Every incarnation has monotonic Eliot `generation`. Tokens, commands, events, turns and requests carry it. Resume increments/rotates; exact re-attach to the same live incarnation preserves it. | Late process/output cannot command or complete replacement work. This preserves the useful v1 binding invariant without proof packets. |
| D-23 | One authoritative source owns each normalized event family per agent generation. Source handoff uses native cursor/watermark or reports a gap. | Duplicate completion would corrupt state/run rules twice; silence/gaps remain explicit. |
| D-24 | Automation provides exactly-once **run/action admission**, not exactly-once external execution. Dispatched/unknown actions use exact readback/manual resolution and are never blindly rerun. | Commands, scripts, Git and webhooks need crash semantics, but not a workflow platform. |
| D-25 | A Store commit failure is exposed in memory/logs, blocks new writes and retains the same pending result. Recovery persists one bounded outage record; the failed commit cannot persist its own failure. | This is the only implementable boundary and avoids repeating external effects. |
| D-26 | Automation resolves selectors and freezes concrete target generation/turn/request at firing. A delayed run never retargets whatever agent/session is current later. | Durable automation otherwise turns an old event into control of a replacement session. |
| D-27 | L0–L6 are implementation-first: format + smallest changed-package Clippy. The broad deterministic/live/platform/load campaign starts once the feature-complete product exists, in L7. | Rebuilding test infrastructure after every partial layer would recreate v1 ceremony; runtime claims still require one complete real-path qualification before release. |

## Defaults

| Topic | Default |
| --- | --- |
| L4.7 harnesses (Command Code, Kilo, Zed) | Do not build until a concrete need. |
| Git identity | Per agent (`<name>.<id>@agents.eliot`); `git_identity="inherit"` opts out. |
| Remote access / multi-host / GUI | Not planned; each needs an owner decision. |
| Eliot Memory OS integration | Not built until explicitly requested. |
