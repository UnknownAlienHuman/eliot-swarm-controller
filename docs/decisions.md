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
| D-22 | Every native incarnation has monotonic Eliot generation; token, commands, events, turns and requests carry it. | Late output/process cannot act on replacement work. |
| D-23 | One authoritative source owns each lifecycle family; handoff uses native cursor or reports a gap. | Duplicate completion corrupts state and fires rules twice. |
| D-24 | Automation guarantees exactly-once admission, not external execution. Unknown actions require readback/manual resolution. | Crash safety without a workflow engine. |
| D-25 | Store commit failure blocks new writes, retains the same pending result where possible and never replays its external cause. | A failing DB cannot durably report its own failure. |
| D-26 | Automation freezes concrete target generation/turn/request at firing. | Old events must not control replacement sessions. |
| D-27 | L0–L6 are code-first with fmt + smallest scoped Clippy; broad qualification starts at feature-complete L7. | Avoid repeated ceremony while still requiring real-path proof before release. |
| D-28 | Launch/resume and optional first input are separate outcomes. Sources open before any native effect. A harness that creates its subject only from input performs one linked start+input, commits ready when control exists, then publishes the separate input association. | The common session-first ordering is impossible for input-created sessions; ready still must not prove or repeat input. |
| D-29 | One native subject maps to one canonical Agent row. Attach/detach changes control on that row; observers subscribe. | Duplicate rows duplicate lifecycle/automation ownership. |
| D-30 | Environment defaults to a minimal allowlist. Inherit-all is explicit global-owner opt-in; named credentials resolve only at execution. | Inherit-all leaks unrelated secrets into every harness. |
| D-31 | Event rules, timers, goals, unread-mail wakeups, workspace/launch queues and provider holds reconcile from durable current state after restart. | Volatile callback/timer edges can be lost. |
| D-32 | Custody is honest per platform: Windows Job exact; Linux exact only with cgroup/equivalent + owner-death guardian; process-group fallback is degraded. | Unix process groups neither auto-die with daemon nor prevent `setsid` escape. |
| D-33 | Persisted plugin facts use commit-acknowledged requests and contiguous per-source sequence with one unacknowledged envelope. | Lost replies remain idempotent after event pruning without O(events) receipts. |
| D-34 | Current edit attribution and turn results are compact projection tables, not reconstructed from retained events. | Retention/restart must not erase `who`, wait or result. |
| D-35 | Global and project automation are distinct principals. Project rules/scripts cannot escape their project or become owner by omitting token. | “One local user” does not justify accidental cross-project control. |
| D-36 | Persisted events carry bounded event-time subject snapshot; rules never join later Agent row. | Replay after label/project/state changes must make same decision. |
| D-37 | Active rule cursors bound retention; disabled rules advance, excessive lag records skipped range and releases retention. | Durable triggers and bounded storage must coexist without silent loss/disk pinning. |
| D-38 | Worktree creation/setup is journaled pre-start. Exact Git effects are read back; unknown setup never reruns blindly. | `launch --worktree` otherwise repeats non-idempotent setup or starts half-built. |
| D-39 | Mail uses capability-aware wake plus immutable inbox-batch acknowledgement. | Stored ≠ presented ≠ safely received. |
| D-40 | Historical replay exposes explicit gap when requested events were pruned. | Silent jump falsely claims complete history. |
| D-41 | Command admission and turn outcome are separate. Missing terminal result is unknown turn blocking more work until readback/resolution. | `send applied` proves association, not completion. |
| D-42 | Plugin Store idempotency is compact source watermark: source key, contiguous sequence and last-envelope digest; native cursor suppresses vendor replay. | Lost acknowledgement after event pruning must not fire automation twice. |
| D-43 | No-token owner fallback requires live birth-pinned OS proof peer is outside every current custody; ambiguity rejects. | Registration can lag execution. |
| D-44 | Routing-token presence follows control lifecycle, not generation alone. Terminal/detached/observe-only revokes hash while preserving generation. | Detach must invalidate credential without rewriting identity history. |
| D-45 | Deterministic rule firing error is durable failed run/no actions and advances cursor atomically; only Store failure retries. | Invalid template must not wedge later events. |
| D-46 | Script environment is principal-scoped; project script requires global∩project allowlist before value lookup. | Global credential must not leak to another project. |
| D-47 | Editing enabled event rule uses old→new high-water handoff; disable/pause records/skips through boundary. | Starting every edit “at now” loses backlog. |
| D-48 | Synchronous hook decision is deadline-bound, non-dispatchable proposal usable only by immediate hook response. | Queued approval can outlive request deadline. |
| D-49 | Degraded backend may prove session control lost while custody remains `orphan_possible`; never claim all descendants died or exact force. | Avoid both permanent wedge and false containment. |
| D-50 | Provider probe is an explicit owner/current-agent send or linked launch/resume input. Automation/wake/hidden calls cannot probe. Exact pre-provider no-effect or pre-dispatch cancellation releases only the probe claim, not the hold; possible provider I/O keeps it reserved. | Recovery must not invisibly spend tokens or permanently wedge after a local failure. |
| D-51 | Claim exists only while exact generation live and controlled; terminal/resume/detach releases transactionally. | Detached/lost agent must not advertise current ownership. |
| D-52 | Every external daemon mutation uses canonical ULID receipt. Pruning advances monotonic floor; retries below it reject. | Bounded retention must not turn old lost-reply retry into new effect. |
| D-53 | External receipt and command identity are separate. External command uses `id=receipt_id=WriteId`; linked/selector/automation uses derived id/no receipt. | Universal receipt FK breaks internal automation and projection retention. |
| D-54 | Routing tokens are deterministically re-derived from private `routing.key` and `(agent,generation,token_version)`; Store keeps only hash, and plugins persist only nonsecret registration metadata. | Lost child/reattach reply must return the same token without plaintext-at-rest. |
| D-55 | `source.open` uses stable plugin `open_id`; Store records open digest and one current owner row per event family. `process.put` only registers verified per-agent descendants; OS monitor owns lifecycle. | Lost open reply, overlapping sources or shared-service PID attribution must not duplicate/mis-own events. |
| D-56 | Selector fanout is transactionally captured, bounded by configurable target limit and paginated; parent result is count/digest, not an unbounded array. | `@all` over large fleets must not explode one API frame/receipt or partially admit. |
| D-57 | Rule definition digest and activation identity are distinct. Duplicate unchanged reload is a no-op. Every semantic activation persists its compiled definition and immutable content-addressed script/note artifacts until its interval/replay closes. | A→B→A must not collide/reset cursors, restart must drain old behavior, and mutable checkout files must not rewrite admitted work. |
| D-58 | Idempotency window is a minimum retention. A receipt referenced by retained command/domain row is not deleted until that reference is pruned. | SQLite restrictive FK and longer command retention otherwise make pruning impossible. |
| D-59 | Worktree admission resolves requested base ref to immutable local commit OID; dispatch never re-resolves/fetches it. Setup and system-note bytes are immutable admission artifacts. | Moving refs/files must not change queued workspace or prompt/setup behavior. |
| D-60 | Automation run journal is generic `(kind,owner activation,trigger)` and rate limits are durable across restart. | Goals are not fake rules; crashes must not reset safety limits. |

## Defaults

| Topic | Default |
| --- | --- |
| Environment | `minimal`; `inherit` requires trusted global profile. |
| Script environment | empty global allowlist; project uses global∩project. |
| External idempotency window | 90-day minimum; referenced receipts may live longer; floor monotonic. |
| Selector fanout | 10,000 targets maximum per admission; no logical fleet limit. |
| Linux custody without exact backend | degraded; graceful control only; possible orphan. |
| New event-rule id | start current high-water; no implicit replay. |
| Unchanged rule reload | no-op; no activation or timer boundary. |
| Edited enabled event rule | fresh activation; old drains to H, new starts after H. |
| Paused/disabled event rule | record skipped range and advance without firing. |
| Project automation | project-scoped principal. |
| Provider probe | explicit owner/current controlled-agent request only; exact pre-provider no-effect releases claim. |
| Unknown live turn | block new send/goal; exact readback/resolution after not-active proof. |
| L4.7 harnesses | Do not build until concrete need. |
| Git identity | Per agent; explicit inherit opts out. |
| Remote/multi-host/GUI/Memory OS | Not built without owner decision. |
