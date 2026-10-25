# Decisions

Short records of choices that are not obvious from the specs. Change a decision by
editing its entry in a PR that the owner approves; do not add addenda.

| ID | Decision | Why |
| --- | --- | --- |
| D-01 | **v2 is a fresh build in layers; v1 is archived at tag `v1-final`.** v1 code is a donor to read, not a base to refactor. | v1 grew to 533k lines of Rust and 61k lines of docs/audits while core paths stayed unqualified; refactoring it produced audit cycles, not features ([lessons](reference/lessons-v1.md)). |
| D-02 | One binary `eliot` (CLI, daemon, built-in plugins, hook shim, MCP). | Light install, one version, no sibling-artifact provenance checks. |
| D-03 | Plugins are separate processes speaking JSON-RPC over stdio; one process per harness kind serves many agents when the native side allows it. | Crash isolation; any language during bring-up (R11); one protocol for built-in and external plugins; scales by multiplexing, not by process count. |
| D-04 | One SQLite file, WAL, one writer task, numbered migrations via `user_version`. | Enough for one host and millions of rows; no server; v1's schema hashing and reference-DDL checks added nothing. |
| D-05 | Events persisted with a global `seq`; streaming deltas and samples are ephemeral. | Replayable history without storing token streams. Native transcripts remain the full record. |
| D-06 | The core has no Task/Issue/review/acceptance/publication engine. | Not in the owner's statement; it was v1's largest source of code and bureaucracy. Build such workflows from rules, scripts and agents. |
| D-07 | Local trust model: one OS user owns everything; agent tokens are for attribution and routing, not security. | Agents run as the same user and can read the same files; pretending otherwise produced v1's grant/epoch/scope machinery. Remote access (L9) will need real auth. |
| D-08 | Control model is `self` / `tree` / `project` per agent; the owner and rules can do everything. | Covers "main agent commands others" and "children of any depth" with three values instead of GM epochs and handovers. |
| D-09 | Eliot Goal loop is allowed for any harness, with budgets (`max_turns`, `max_cost_usd`) and `no_progress` detection. Native goal is used where available. | Owner point 6. Supersedes v1 owner decision #3 ("no automatic re-prompt loop for OpenCode"); the risk it addressed (token ping-pong) is handled by the guardrails. |
| D-10 | Rules can be created by files and CLI; cron expressions are supported. | Owner point 7. Supersedes v1 owner decision #4 (configuration-only, no cron in the first slice). |
| D-11 | `who` is answered from Git facts (dirty files, branch diff, history), the agents' own file-change events, and advisory claims. | Owner point 10. Supersedes v1's "Git history is not ownership": it is not authority, but it is the right answer to "who works here". |
| D-12 | Claims are advisory; Eliot does not lock files. | Eliot cannot sandbox a native shell; a lock that does not hold is worse than an announcement. |
| D-13 | First real harness is Codex (app-server, Rust plugin); OpenCodex is the first proxy. | Most complete documented control protocol (threads, turns, steer, interrupt, approvals, goals) over stdio; OpenCodex targets Codex; no Node/Python needed for L2. |
| D-14 | Never auto-retry a non-idempotent command whose outcome is unknown; resolve by readback. | Duplicate prompts and pushes were among v1's costliest field failures. |
| D-15 | Documentation in `docs/` is specification and roadmap only; status is the roadmap checkboxes plus tests. No audit, report, inventory, packet or readiness documents in the repo. | v1 spent most of its later effort producing documents about documents. |
| D-16 | Docs are in English; the owner statement in [vision](vision.md) stays verbatim in Russian. | English is what implementing agents and vendor docs use; the original wording remains the source of truth. |
| D-17 | Harness versions are reported and displayed, never used as gates. | v1 version/provenance gates blocked work without preventing failures. |
| D-18 | Sessions started over stdio end with the daemon or their plugin (they become `lost`, `resume` continues them); `daemon stop` refuses to cut running turns unless `--drain` or `--force`; server-capable harnesses get a service mode (L7.6) so upgrades and crashes do not interrupt turns. | Simple and orphan-free first; v1 showed that controller restarts must not kill accepted work, so the long-lived case gets a real server, not detached plugin processes. |
| D-19 | Native goal and Eliot goal are independent; one of each may be active on an agent. | Owner decision of 2026-10-18 (v1 `agent_swarm.md` §10): the native goal is the default, the Eliot goal sends ordinary messages, and either or both may be used. |
| D-20 | Commands to one agent run strictly in order; `interrupt`/`stop` skip the line. | v1 counterexample H18-05: `configure(max)` then `configure(high)` then `send` must not run the task on the wrong settings. |
| D-21 | Per-profile `max_live` queues launches instead of overloading an account. | Quotas and subscriptions are per account. v1 field data: 7 of 10 lines ran on one model route that was about to be withdrawn; 429s and weekly quota windows stalled lines. |

## Defaults

Choices made so work never waits. Change them by editing this table.

| Topic | Default |
| --- | --- |
| L4.7 harnesses (Command Code, Kilo, Zed) | Not built until someone needs them. |
| Git identity of agent commits | Per agent (`<name>.<id>@agents.eliot`); `git_identity = "inherit"` opts out. |
| Remote access (L9) | Not planned. |
| Eliot Memory OS integration | Not built until the owner asks (L9). |
