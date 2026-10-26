# Decisions

Short records of non-obvious product choices. Change the owning entry in the PR that
changes behavior; never append a contradictory addendum.

| ID | Decision | Why |
| --- | --- | --- |
| D-01 | v2 is a fresh layered build. v1 frozen commit is `dc6f70aa`; L0.1 creates `v1-final` before deletion. | v1 grew to roughly 533k Rust lines and 61k process/audit docs without qualified core paths. |
| D-02 | One `eliot` binary provides CLI, daemon, built-in plugin/guardian, hook and optional MCP entries. | One install/version and small internal helpers; no sibling-artifact platform. |
| D-03 | Process-owning plugins run per agent generation. Shared control requires exact per-session identity, environment/tool attribution, attach/readback and isolated stop. | Multi-session support alone does not provide correct attribution/custody. |
| D-04 | One SQLite WAL database, one writer, numbered migrations and signed-64-bit bounded ids. | Enough for one host; avoids server/schema-hash machinery and impossible persistent `u64` claims. |
| D-05 | Persist bounded normalized events; deltas/samples remain ephemeral. | Replayable facts without storing full native streams. |
| D-06 | No Task/Issue/review/acceptance/publication engine in core. | Not requested and was v1's largest growth source. |
| D-07 | One OS user is the trust boundary. Tokens are high-entropy generation routing handles, stored hashed and pattern-masked, not multi-user security. | Same-user agents can already access local resources; remote auth is a later boundary. |
| D-08 | Control is `self|tree|project`; every principal still passes the same router/bindings. | Covers managers/children without epochs or policy receipts. |
| D-09 | Native and Eliot goals may coexist; generation change pauses them until explicit rebind. | Old goals must not drive replacement sessions. |
| D-10 | Rules are strict versioned TOML; daemon owns durable event cursors/timers and gap-free revision handoff. | Hot reload/restart must neither miss nor duplicate trigger admission. |
| D-11 | `who` uses Git facts, durable current edit attribution and advisory claims; history is provenance only. | Git cannot attribute shared-checkout dirty bytes by itself. |
| D-12 | Claims are advisory, never locks. | Native shells can ignore fake locks. |
| D-13 | First real harness is Codex app-server; first provider proxy is OpenCodex. | Rich native control protocol and direct Rust first path. |
| D-14 | No unknown native/external effect is retried automatically, including start, input, interrupt and stop. | Late retry can hit a newer turn/session or duplicate costly work. |
| D-15 | Docs contain current specs, roadmap and compact references only. | No return to readiness/audit bureaucracy. |
| D-16 | Docs are English; owner source statement remains verbatim Russian. | Exact owner intent plus implementer/vendor language. |
| D-17 | Versions are reported/recorded, not authority gates. | Version equality does not prove semantics. |
| D-18 | Service continuity is added only after exact attach is qualified. Early stdio generations remain daemon-owned. | No detached ambiguous processes. |
| D-19 | `on_idle` Eliot goal waits while native goal drives; explicit cadence may intentionally queue another turn. | Prevent accidental dual loops. |
| D-20 | Commands use readiness lanes: queued future sends do not block exact current-turn controls/answers/config. | Future work must not obstruct current control. |
| D-21 | Capacity is shared by explicit account plus optional profile/proxy narrowing; scoped holds persist. | Multiple profiles can spend one subscription. |
| D-22 | Every native incarnation has monotonic Eliot generation; token, commands, events, turns and requests carry it. | Late output/process cannot act on replacement work. |
| D-23 | One authoritative source owns each lifecycle family; handoff uses native cursor or reports a gap. | Duplicate completion corrupts state and fires rules twice. |
| D-24 | Automation guarantees exactly-once admission, not external execution. Unknown actions require readback/manual resolution. | Crash safety without a workflow engine. |
| D-25 | Store commit failure blocks new writes, retains the same pending result where possible and never replays its external cause. | A failing DB cannot durably report its own failure. |
| D-26 | Automation freezes concrete target generation/turn/request at firing. | Old events must not control replacement sessions. |
| D-27 | L0–L6 are code-first with fmt + smallest scoped Clippy; broad qualification starts at feature-complete L7. | Avoid repeated ceremony while still requiring real-path proof before release. |
| D-28 | Session launch/resume and optional first prompt are separate linked commands/outcomes. Ready proves only session readiness. | Lost start reply must not prove/repeat initial input. |
| D-29 | One native subject maps to one canonical Agent row. Attach/detach changes control on that row; observers subscribe. | Duplicate rows duplicate lifecycle/automation ownership. |
| D-30 | Environment defaults to a minimal allowlist. Inherit-all is explicit global-owner opt-in; named credentials resolve only at execution. | Inherit-all leaks unrelated secrets into every harness. |
| D-31 | Event rules, timers, goals, unread-mail wakeups, workspace/launch queues and provider holds reconcile from durable current state after restart. | Volatile callback/timer edges can be lost. |
| D-32 | Custody is honest per platform: Windows Job exact; Linux exact only with cgroup/equivalent + owner-death guardian; process-group fallback is degraded. | Unix process groups neither auto-die with daemon nor prevent `setsid` escape. |
| D-33 | Persisted plugin facts use commit-acknowledged requests and a contiguous per-source sequence with one unacknowledged event. Store keeps one watermark/digest per source epoch. | Lost replies remain idempotent after event pruning without an unbounded receipt row per event. |
| D-34 | Current edit attribution and turn results are compact projection tables, not reconstructed from retained events. | Retention/restart must not erase `who`, wait or result. |
| D-35 | Global and project automation are distinct principals. Project rules/scripts cannot escape their registered project or become owner by omitting a token. | “One local user” does not justify accidental cross-project control. |
| D-36 | Persisted events carry a bounded event-time subject snapshot; rules never join a later mutable Agent row. | Replay after labels/project/state changes must produce the same decision. |
| D-37 | Active rule cursors bound retention; disabled rules advance, and excessive lag records an explicit skipped range and releases retention. | Durable triggers and bounded storage must coexist without silent loss or indefinite disk pinning. |
| D-38 | Worktree creation/setup is a journaled pre-start phase. Exact Git effects are read back; unknown setup never reruns blindly. | `launch --worktree` otherwise repeats non-idempotent setup or starts half-built. |
| D-39 | Mail uses capability-aware wake plus immutable inbox-batch acknowledgement. | Stored ≠ presented ≠ safely received; lost reply must not consume mail. |
| D-40 | Historical replay exposes an explicit gap when requested events were pruned. | Silent jump from an old cursor falsely claims complete history. |
| D-41 | Command admission and turn outcome are separate. A missing terminal result becomes an unknown turn that blocks further work in that generation until exact readback or constrained owner resolution. | `send applied` proves turn association, not completion; reconnect must neither invent success nor wedge recovery permanently. |
| D-42 | Plugin Store idempotency is a compact source watermark: one stable source key, contiguous sequence and last-envelope digest. Native cursor still suppresses vendor replay. | A lost `event.put` acknowledgement after event retention must not fire automation twice or grow O(events) dedupe state. |
| D-43 | No-token owner fallback requires a live, birth-pinned OS check proving the peer is outside every current Eliot custody; ambiguity rejects. | Process registration can lag execution, so a projection miss is not proof that an Eliot-managed child is an owner process. |
| D-44 | Routing-token presence follows control lifecycle, not generation alone. Terminal/detached/observe-only rows revoke by clearing the hash while preserving monotonic generation. | Detach must invalidate credentials without rewriting identity history. |
| D-45 | A deterministic event-rule firing error is a durable failed run with no actions and advances the rule cursor atomically; only Store failure retries the event. | Missing placeholders or invalid actions must not wedge all later events behind one permanent error. |
| D-46 | Script environment references are principal-scoped. Project scripts require both global automation approval and the project's own allowlist before any value is read. | A global credential allowed elsewhere must not leak into a project script before returned-action validation. |
| D-47 | Editing an enabled event rule creates an old→new high-water handoff: old revision drains through H, new revision starts after H. Disable/pause instead records and skips through H. | Simply starting every new revision “at now” can lose events between the old cursor and reload boundary. |

## Defaults

| Topic | Default |
| --- | --- |
| Environment | `minimal`; `inherit` requires a trusted global profile. |
| Script environment | empty global allowlist; project scripts use global∩project. |
| Linux custody without exact backend | degraded; graceful control allowed, exact force rejected. |
| New event-rule id | start at current high-water; no implicit historical replay. |
| Edited enabled event rule | old revision drains to captured H; new revision starts after H. |
| Paused/disabled event rule | record skipped range and advance cursor without firing. |
| Project automation | project-scoped principal. |
| Unknown live turn | block new send/goal; exact readback or owner resolution after native-not-active proof. |
| L4.7 harnesses | Do not build until concrete need. |
| Git identity | Per agent; explicit inherit opts out. |
| Remote/multi-host/GUI/Memory OS | Not built without owner decision. |
