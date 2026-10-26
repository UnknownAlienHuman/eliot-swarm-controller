# Decisions

Short records of non-obvious product choices. Change the owning entry in the PR that
changes behavior; never append a contradictory addendum.

| ID | Decision | Why |
| --- | --- | --- |
| D-01 | v2 is a fresh layered build. v1 frozen commit is `dc6f70aa`; L0.1 creates `v1-final` before deletion. | v1 grew to roughly 533k Rust lines and 61k process/audit docs without qualified core paths. |
| D-02 | One `eliot` binary provides CLI, daemon, built-in plugin/guardian, hook and optional MCP entries. | One install/version and small internal helpers; no sibling-artifact platform. |
| D-03 | Process-owning plugins run per Agent generation. Shared control requires exact per-session identity, environment/tool attribution, attach/readback and isolated stop. Control attach also requires a frozen profile/capacity binding. | Multi-session support alone does not provide correct attribution, route accounting or custody. |
| D-04 | One SQLite WAL database, one writer, numbered migrations and signed-64-bit bounded ids. | Enough for one host; avoids server/schema-hash machinery and impossible persistent `u64` claims. |
| D-05 | Persist bounded normalized events; deltas/samples remain ephemeral. | Replayable facts without storing full native streams. |
| D-06 | No Task/Issue/review/acceptance/publication engine in core. | Not requested and was v1's largest growth source. |
| D-07 | Every connected API caller presents an explicit principal credential. Owner CLI uses explicit owner mode and a private owner token; missing credentials reject. Credentials are attribution inside the one-OS-user trust boundary, not multi-user security. | Eliminates the no-token-owner/PID-classification race and a large unnecessary authority subsystem. |
| D-08 | Control is `self|tree|project`; every principal still passes the same router/bindings. | Covers managers/children without epochs or policy receipts. |
| D-09 | Native and Eliot goals may coexist; generation change pauses them until explicit rebind. | Old goals must not drive replacement sessions. |
| D-10 | Rules are strict versioned TOML; daemon owns durable event cursors/timers and gap-free activation handoff. | Hot reload/restart must neither miss nor duplicate trigger admission. |
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
| D-22 | Native incarnation generation and direct routing are separate. Every live Agent has a generation; only an Agent with an independently injectable environment has `routing_enabled` and a token hash. | Native children can be controllable through a parent plugin without pretending they can authenticate their own CLI. |
| D-23 | One authoritative source owns each lifecycle family; handoff uses an exact cursor/gap boundary. | Duplicate completion corrupts state and fires rules twice. |
| D-24 | Automation guarantees exactly-once admission, not external execution. Unknown actions require readback/manual resolution. | Crash safety without a workflow engine. |
| D-25 | Store commit failure blocks new writes, retains the same pending result where possible and never replays its external cause. | A failing DB cannot durably report its own failure. |
| D-26 | Automation freezes concrete target generation/turn/request at firing. | Old events must not control replacement sessions. |
| D-27 | L0–L6 are code-first with fmt + smallest scoped Clippy; broad qualification starts at feature-complete L7. | Avoid repeated ceremony while still requiring real-path proof before release. |
| D-28 | Authoritative source epochs open before any native start/input effect. Launch/resume and optional first input are separate outcomes; input-created sessions perform one linked start+input but ready still proves only session control. | No first frame may exist outside the durable source sequence, and a lost start reply must not prove/repeat input. |
| D-29 | One native subject maps to one canonical Agent row. Observe attach uses a synthetic observe profile; control attach installs/reuses an exact frozen profile and capacity binding on that row. | Duplicate rows duplicate lifecycle/automation; unbound attach loses route/account/environment semantics. |
| D-30 | Environment defaults to a minimal allowlist. Inherit-all is explicit global-owner opt-in; named credentials resolve only at execution. | Inherit-all leaks unrelated secrets into every harness. |
| D-31 | Event rules, timers, goals, unread-mail wakeups, workspace/launch queues and provider holds reconcile from durable current state after restart. | Volatile callback/timer edges can be lost. |
| D-32 | Custody is honest per platform: Windows Job exact; Linux exact only with cgroup/equivalent + owner-death guardian; process-group fallback is degraded. | Unix process groups neither auto-die with daemon nor prevent `setsid` escape. |
| D-33 | Persisted plugin facts use commit-acknowledged source sequence. Every persisted lifecycle fact also has a stable native event id with a compact receipt surviving event pruning. | Source sequence handles lost Store replies; native receipts separately prevent vendor replay after retained event rows disappear. |
| D-34 | Current edit attribution and turn results are compact projection tables, not reconstructed from retained events. | Retention/restart must not erase `who`, wait or result. |
| D-35 | Global and project automation are distinct principals. Project rules/scripts cannot escape their project or become owner by omitting a token. | “One local user” does not justify accidental cross-project control. |
| D-36 | Persisted events carry bounded event-time subject snapshot; rules never join a later Agent row. | Replay after label/project/state changes must make the same decision. |
| D-37 | Active rule cursors bound retention; disabled rules advance, excessive lag records skipped range and releases retention. | Durable triggers and bounded storage must coexist without silent loss/disk pinning. |
| D-38 | Worktree creation/setup is journaled pre-start. Exact Git effects are read back; unknown setup never reruns blindly. | `launch --worktree` otherwise repeats non-idempotent setup or starts half-built. |
| D-39 | Queue mail keeps one boolean `wake_on_mail`: routed Agents may receive a coalesced inbox notice; routing-disabled native children use routed-parent relay or remain queue-only. Full-content delivery requires an explicit inject/send/steer command with control rights. Inbox acknowledgement is separate and durable. | Stored ≠ presented ≠ safely received; “run inbox” is useless to an Agent with no token, while an automatic full-message fallback would bypass sender authority. |
| D-40 | Historical replay exposes explicit gap when requested events were pruned. | Silent jump falsely claims complete history. |
| D-41 | Command admission and turn outcome are separate. Missing terminal result is unknown turn blocking more work until readback/resolution. | `send applied` proves association, not completion. |
| D-42 | Source sequence stores the last Store envelope; native-event receipts store `(agent,generation,kind,native_id,payload_digest,first_seq)` until replay can no longer target that generation. | These are different idempotency boundaries and neither can replace the other. |
| D-43 | No-token connected requests are invalid. Owner use is explicit (`eliot --owner` or an owner credential supplied by a direct client); OS process custody never grants API authority. | Simpler than scanning every Job/cgroup and consistent with the declared one-user trust boundary. |
| D-44 | Routing-token presence follows `routing_enabled`, control lifecycle and live state—not generation alone. Terminal/detached/observe-only/routing-disabled rows have no hash while preserving generation. | Detach and parent-routed native children must not violate the schema. |
| D-45 | Deterministic rule firing error is a durable failed run/no actions and advances cursor atomically; only Store failure retries. | Invalid template must not wedge later events. |
| D-46 | Script environment is principal-scoped; project script requires global∩project allowlist before value lookup. | Global credential must not leak to another project. |
| D-47 | Editing enabled event rule uses old→new high-water handoff; disable/pause records/skips through boundary. | Starting every edit “at now” loses backlog. |
| D-48 | Synchronous hook decision is a deadline-bound, non-dispatchable proposal usable only by the immediate hook response. | Queued approval can outlive request deadline. |
| D-49 | Degraded backend may prove session control lost while custody remains `orphan_possible`; never claim all descendants died or exact force. | Avoid both permanent wedge and false containment. |
| D-50 | Provider probe is an explicit owner/current-agent send or linked launch/resume input. Automation/wake/hidden calls cannot probe. Exact pre-provider no-effect releases only the probe claim; possible provider I/O keeps it reserved. | Recovery must not invisibly spend tokens or permanently wedge after a local failure. |
| D-51 | Claim exists only while exact generation live and controlled; terminal/resume/detach releases transactionally. | Detached/lost agent must not advertise current ownership. |
| D-52 | Every external daemon mutation uses canonical ULID receipt. Pruning advances monotonic floor; retries below it reject. | Bounded retention must not turn an old lost-reply retry into a new effect. |
| D-53 | External receipt and command identity are separate. External command uses `id=receipt_id=WriteId`; linked/selector/automation uses derived id/no receipt. | Universal receipt FK breaks internal automation and projection retention. |
| D-54 | Routing tokens are deterministically re-derived from private `routing.key` and `(agent,generation,token_version)`; Store keeps only hash, and plugins persist only nonsecret registration metadata. | Lost child/reattach reply must return the same token without plaintext-at-rest. |
| D-55 | `source.open` uses stable plugin `open_id`; initial open or replacement atomically installs family owners. `process.put` only registers verified per-agent descendants; OS monitor owns lifecycle. | Lost open reply, overlapping sources or shared-service PID attribution must not duplicate/mis-own events. |
| D-56 | Selector fanout, plugin discovery, snapshots and histories are bounded and paginated; parent result is count/digest, not an unbounded array. | Large fleets/native stores must not exceed one API/plugin frame or partially admit. |
| D-57 | Rule definition digest and activation identity are distinct. Duplicate unchanged reload is a no-op. Every semantic activation persists compiled definition and immutable content-addressed artifacts until its interval/replay closes. | A→B→A must not collide/reset cursors, and mutable checkout files must not rewrite admitted work. |
| D-58 | Idempotency window is minimum retention. A receipt referenced by a retained domain row is not deleted until that row is pruned. | SQLite restrictive FK and longer command retention otherwise make pruning impossible. |
| D-59 | Worktree admission resolves requested base ref to immutable local commit OID; dispatch never re-resolves/fetches it. Setup and system-note bytes are immutable admission artifacts. | Moving refs/files must not change queued workspace or prompt/setup behavior. |
| D-60 | Automation run journal is generic. Goal `on_idle` rounds use a durable eligibility sequence; cadence rounds use exact slots. Failed/unknown round blocks or terminates explicitly rather than silently reusing one trigger. | Goals are not fake rules; a constant/implicit trigger either duplicates after restart or prevents later continuation. |
| D-61 | Project profiles are monotonic overlays on owner-defined global profiles. They cannot change harness/plugin/account/proxy/argv/inherit policy or broaden permissions/control/environment. | A name allowlist alone lets a project smuggle a different route or credential set. |
| D-62 | Controller fanout/depth guards block new Eliot-created work but never erase an already-created native child. Guard-exceeding native children are recorded with reduced authority/routing/wake. | Eliot cannot undo a vendor child that already exists; rejection would create a monitoring blind spot. |
| D-63 | Plugin frame, inline start data, event data, final text and page sizes have explicit limits. Immutable notes/scripts travel by artifact reference. `native_input_id` is optional when a vendor exposes only exact command→turn association. | “Bounded” without compatible numbers is not implementable, and a 1 MiB note cannot fit inside a 1 MiB frame. |
| D-64 | L0.1 deletes v1 implementation assets but preserves the already-v2 docs workflow and both v2 validation scripts. | Broad deletion of `tools/`/workflows would remove the only executable checks on the new specification. |
| D-65 | Native source replacement is one atomic predecessor-aware open/handoff operation. Separate close then open is allowed only for an explicitly gapped boundary. | The old promise of transactional family handoff was not expressible by the protocol. |

## Defaults

| Topic | Default |
| --- | --- |
| Environment | `minimal`; `inherit` requires a trusted global profile. |
| Owner API | explicit `eliot --owner`; missing credential rejects. |
| Native child without isolated environment | `routing_enabled=false`; with `wake_on_mail=true`, notify a routed parent or leave queue-only. |
| Script environment | empty global allowlist; project uses global∩project. |
| External idempotency window | 90-day minimum; referenced receipts may live longer; floor monotonic. |
| Selector fanout | 10,000 targets maximum per admission; no logical fleet limit. |
| Discovery/snapshot page | at most 1,000 items and below plugin frame limit. |
| Persisted event data | 64 KiB; larger native detail is truncated with a pointer. |
| Linux custody without exact backend | degraded; graceful control only; possible orphan. |
| New event-rule id | start current high-water; no implicit replay. |
| Unchanged rule reload | no-op; no activation or timer boundary. |
| Edited enabled event rule | fresh activation; old drains to H, new starts after H. |
| Paused/disabled event rule | record skipped range and advance without firing. |
| Project profile | narrowing overlay on a global profile only. |
| Project automation | project-scoped principal. |
| Provider probe | explicit owner/current controlled-agent request only; exact pre-provider no-effect releases claim. |
| Unknown live turn | block new send/goal; exact readback/resolution after not-active proof. |
| L4.7 harnesses | Do not build until concrete need. |
| Git identity | Per agent; explicit inherit opts out. |
| Remote/multi-host/GUI/Memory OS | Not built without owner decision. |
