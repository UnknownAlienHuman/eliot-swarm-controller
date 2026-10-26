# Decisions

Short records of choices not obvious from the specs. Change an entry in the PR that
changes the product; never append a contradictory addendum.

| ID | Decision | Why |
| --- | --- | --- |
| D-01 | **v2 is a fresh layered build.** v1 frozen commit is `dc6f70aa`; L0.1 creates `v1-final` there before deletion. | v1 reached roughly 533k Rust lines and 61k process/audit docs without qualified core paths. |
| D-02 | One `eliot` binary provides CLI, daemon, built-in plugin/guardian, hook and optional MCP entries. | One install/version and small internal helpers; no sibling-artifact platform. |
| D-03 | Process-owning plugins run per agent generation. Shared control is allowed only when the native service provides exact per-session identity, environment/tool attribution, attach and isolated stop. | Exact attribution/custody is more important than saving one process. |
| D-04 | One SQLite WAL database, one writer task, numbered migrations and signed-64-bit bounded ids. | Sufficient for one host; avoids server/schema-hash machinery and impossible `u64` storage claims. |
| D-05 | Persist bounded normalized events with global sequence; deltas/samples remain ephemeral. | Replayable state without full native token streams. |
| D-06 | No Task/Issue/review/acceptance/publication engine in core. | Not requested and was v1's largest source of growth. |
| D-07 | One OS user is the trust boundary. Tokens are generation routing/attribution handles, stored hashed and masked, not multi-user security. | Same-user agents can already access local resources; remote auth is a later boundary. |
| D-08 | Control is `self|tree|project`; owner/automation use owner authority but still pass the same router/bindings. | Covers managers/children without epochs and policy receipts. |
| D-09 | Native and Eliot goals may coexist with independent budgets; generation changes pause them until explicit rebind. | Prevents old goals driving replacement sessions. |
| D-10 | Rules are strict versioned TOML; daemon owns event cursors and timers. | Hot reload and restart must neither miss nor duplicate work. |
| D-11 | `who` uses Git facts, durable current edit attribution and advisory claims; history is provenance only. | Git alone cannot attribute shared-checkout dirty bytes. |
| D-12 | Claims are advisory, never locks. | Native shells can ignore fake locks. |
| D-13 | First real harness is Codex app-server; first provider proxy is OpenCodex. | Rich native control protocol and Rust-compatible first path. |
| D-14 | **No unknown native/external effect is retried automatically, including start, input, interrupt and stop.** | A late retry can hit a newer turn/session or duplicate costly work. |
| D-15 | Docs contain current specs, roadmap and compact evidence references only. | No return to readiness/audit bureaucracy. |
| D-16 | Docs are English; owner source statement remains verbatim Russian. | Exact owner intent plus implementer/vendor language. |
| D-17 | Versions are reported/recorded, not authority gates. | Version equality does not prove semantics. |
| D-18 | Service continuity is added only after exact attach is qualified. Early stdio generations remain daemon-owned. | No detached ambiguous processes. |
| D-19 | `on_idle` Eliot goal waits while native goal drives; explicit cadence may intentionally queue another turn. | Prevent accidental dual loops. |
| D-20 | Commands use readiness lanes: ready calls serialize, queued future sends do not block exact current-turn controls/answers/config. | A future request must not obstruct the current turn. |
| D-21 | Capacity is shared by explicit account plus optional profile/proxy narrowing; holds persist with reported limitation scope. | Multiple profiles can spend one subscription; profile-only caps are insufficient. |
| D-22 | Every native incarnation has monotonic Eliot generation; token, commands, events, turns and requests carry it. | Late output/process cannot act on replacement work. |
| D-23 | One authoritative source owns each lifecycle family; handoff uses native cursor or reports a gap. | Duplicate completion corrupts state and fires rules twice. |
| D-24 | Automation guarantees exactly-once **admission**, not external execution. Unknown actions require readback/manual resolution. | Crash safety without workflow-engine bloat. |
| D-25 | Store commit failure blocks new writes, retains the same pending result where possible and never replays its external cause. | A failing DB cannot durably report its own failure. |
| D-26 | Automation freezes concrete target generation/turn/request at firing. | Old events must not control replacement sessions. |
| D-27 | L0–L6 are code-first with fmt + smallest scoped Clippy; broad qualification starts at feature-complete L7. | Avoid repeated ceremony while still requiring real-path qualification before release. |
| D-28 | Launch/resume session start and optional first prompt are separate linked commands/outcomes. `session.ready` proves only session readiness. | A lost start reply must not incorrectly prove or repeat the initial prompt. |
| D-29 | One native subject maps to one canonical Agent row. Attach/detach changes control on that row; observers subscribe instead of creating duplicates. | Duplicate rows would duplicate lifecycle automation and ownership. |
| D-30 | Environment defaults to a minimal allowlist. Full daemon environment inheritance is explicit opt-in; passed credential names are resolved only at execution. | Inherit-all silently leaks unrelated API keys/tokens to every harness. |
| D-31 | Event rules, timers, goals, unread-mail wakeups and launch/provider queues reconcile from durable current state after restart. | Volatile callbacks/timers can be lost between commit and reaction. |
| D-32 | Custody capability is honest per platform: Windows Job exact; Linux exact only with cgroup/equivalent + owner-death guardian; process-group fallback is degraded and cannot claim exact force. | Unix process groups neither auto-die with daemon nor prevent `setsid` escape. |
| D-33 | Native lifecycle duplicates are rejected by Store uniqueness after source/generation/turn validation. | Plugin-only deduplication is not a sufficient integrity boundary. |
| D-34 | Current edit attribution is a compact projection table, not reconstructed from retained event history. | Retention/restart must not erase `who` for long-running agents. |

## Defaults

| Topic | Default |
| --- | --- |
| Environment | `minimal`; `inherit` requires explicit trusted profile. |
| Linux custody without exact backend | degraded; graceful control allowed, exact force rejected. |
| Event-rule first load/edit | start at current event high-water; no implicit historical replay. |
| L4.7 harnesses | Do not build until concrete need. |
| Git identity | Per agent; explicit inherit opts out. |
| Remote/multi-host/GUI/Memory OS | Not built without owner decision. |
