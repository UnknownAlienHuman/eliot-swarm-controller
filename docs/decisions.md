# Decisions

Short records of choices that are not obvious from the specs. Change a decision by
editing its entry in a PR that the owner approves; do not add addenda.

| ID | Decision | Why |
| --- | --- | --- |
| D-01 | **v2 is a fresh build in layers; v1 is archived at tag `v1-final`.** v1 code is a donor to read, not a base to refactor. | v1 grew to 533k lines of Rust and 61k lines of docs/audits while core paths stayed unqualified; refactoring it produced audit cycles, not features ([lessons](reference/lessons-v1.md)). |
| D-02 | One binary `eliot` (CLI, daemon, built-in plugins, hook shim, MCP). | Light install, one version, no sibling-artifact provenance checks. |
| D-03 | Plugins are separate processes speaking JSON-RPC over stdio. A plugin that launches an agent-owned native process runs **per agent**; a shared plugin is allowed only when the native service already isolates sessions and stopping one agent never requires killing the shared process. | Crash isolation and exact process custody matter more than saving one small plugin process. This keeps `stop --force` honest while still allowing a shared fake plugin and service-backed harnesses to multiplex thousands of records. |
| D-04 | One SQLite file, WAL, one writer task, numbered migrations via `user_version`. | Enough for one host and millions of rows; no server; v1's schema hashing and reference-DDL checks added nothing. |
| D-05 | Events persisted with a global `seq`; streaming deltas and samples are ephemeral. | Replayable history without storing token streams. Native transcripts remain the full record. |
| D-06 | The core has no Task/Issue/review/acceptance/publication engine. | Not in the owner's statement; it was v1's largest source of code and bureaucracy. Build such workflows from rules, scripts and agents. |
| D-07 | Local trust model: one OS user owns everything; agent tokens are for attribution and routing, not security. | Agents run as the same user and can read the same files; pretending otherwise produced v1's grant/epoch/scope machinery. Remote access (L9) will need real auth. |
| D-08 | Control model is `self` / `tree` / `project` per agent; the owner and rules can do everything. | Covers "main agent commands others" and "children of any depth" with three values instead of GM epochs and handovers. |
| D-09 | Eliot Goal loop is allowed for any harness, with budgets (`max_turns`, `max_cost_usd`) and `no_progress` detection. Native goal is used where available. | Owner point 6. Supersedes v1 owner decision #3 ("no automatic re-prompt loop for OpenCode"); the risk it addressed (token ping-pong) is handled by the guardrails. |
| D-10 | Rules are TOML files (project or data dir), hot-reloaded; the owner or an agent creates one by writing a file; cron expressions are supported. | Owner point 7. Supersedes v1 owner decision #4 (configuration-only, no cron in the first slice). |
| D-11 | `who` is answered from Git facts (dirty files, branch diff, history), the agents' own file-change events, and advisory claims. | Owner point 10. Supersedes v1's "Git history is not ownership": it is not authority, but it is the right answer to "who works here". |
| D-12 | Claims are advisory; Eliot does not lock files. | Eliot cannot sandbox a native shell; a lock that does not hold is worse than an announcement. |
| D-13 | First real harness is Codex (app-server, Rust plugin); OpenCodex is the first proxy. | Most complete documented control protocol (threads, turns, steer, interrupt, approvals, goals) over stdio; OpenCodex targets Codex; no Node/Python needed for L2. |
| D-14 | **No unknown native or external effect is retried automatically, including `interrupt` and `stop`.** Read back the exact session/turn/request or leave it `unknown`. | A late retry can hit a newer turn or session. Duplicate prompts and pushes were among v1's costliest failures; allegedly idempotent control verbs are not harmless without an exact generation and target. |
| D-15 | Documentation in `docs/` is specification and roadmap only; status is the roadmap checkboxes plus tests. No audit, report, inventory, packet or readiness documents in the repo. | v1 spent most of its later effort producing documents about documents. |
| D-16 | Docs are in English; the owner statement in [vision](vision.md) stays verbatim in Russian. | English is what implementing agents and vendor docs use; the original wording remains the source of truth. |
| D-17 | Harness versions are reported and displayed, never used as gates. | v1 version/provenance gates blocked work without preventing failures. |
| D-18 | Sessions started over stdio end with the daemon or their per-agent plugin (they become `lost`; `resume` continues them). `daemon stop` refuses to cut running turns unless `--drain` or `--force`. Server-capable harnesses get service mode (L7.6) so upgrades and crashes can preserve turns. | Simple and orphan-free first; long-lived work gets a real service and exact re-attach, not detached processes with ambiguous ownership. |
| D-19 | Native goal and Eliot goal are independent; one of each may be active on an agent. While a native goal is active, an `on_idle` Eliot goal waits (the native goal drives); an `every` cadence still sends. | Owner decision of 2026-10-18 (v1 `agent_swarm.md` §10): the native goal is the default, the Eliot goal sends ordinary messages, and either or both may be used. |
| D-20 | Commands use explicit readiness lanes, not a fake total order. Commands that are ready for native I/O are serialized per agent; queued `send`s remain FIFO but do not block controls for the current turn or a `configure` that can apply now. Every dispatched command is bound to the current generation and, where relevant, the exact turn/request. | This preserves `configure` → `send` ordering without making a future-turn request block `steer`, approval, or interrupt of the turn already running. A queued command never crosses a session generation. |
| D-21 | Per-profile `max_live` queues launches instead of overloading an account. | Quotas and subscriptions are per account. v1 field data: 7 of 10 lines ran on one model route that was about to be withdrawn; 429s and weekly quota windows stalled lines. |
| D-22 | Every live incarnation has a monotonically increasing `generation`. Agent tokens, plugin messages, commands, pending requests and turn events carry it. A resume rotates the token and increments the generation; re-attaching the same still-live incarnation preserves it. | Late output or a stale process must not command, complete, interrupt or wake a replacement session. This keeps the useful v1 binding-generation invariant without proof packets, receipts or policy hashes. |
| D-23 | For each agent and normalized event family, one source is authoritative at a time. Live streams, transcripts and hooks may complement one another, but they never independently publish the same lifecycle event. Source hand-off uses a cursor/watermark and reports gaps. | Duplicate `turn.completed` events would run rules twice and corrupt state. Silence is still not completion; an uncovered interval remains explicit. |
| D-24 | Rule firing is exactly-once **admission**, not exactly-once external execution. A small durable run/action journal records rendered actions and their states. After `dispatched` or `unknown`, an action is read back or resolved; it is never blindly run again. | Commands are already durable, but scripts, Git and webhooks also need crash semantics. This is execution bookkeeping, not a Task/workflow engine. |
| D-25 | Store failure cannot be reported durably through the failing store. During an outage the daemon logs and exposes in-memory health, stops admitting writes, and retains pending commit results; after recovery it persists one bounded outage/recovery event. | Claiming a persisted `source.health` event on a failed commit is impossible. The specification must describe an implementable failure path. |

## Defaults

Choices made so work never waits. Change them by editing this table.

| Topic | Default |
| --- | --- |
| L4.7 harnesses (Command Code, Kilo, Zed) | Not built until someone needs them. |
| Git identity of agent commits | Per agent (`<name>.<id>@agents.eliot`); `git_identity = "inherit"` opts out. |
| Remote access (L9) | Not planned. |
| Eliot Memory OS integration | Not built until the owner asks (L9). |
