# Donor map

Borrow a mechanism for a retained responsibility and delete replaced Eliot code in the
same slice. A donor is not a reason to import an application. [Implementation](implementation.md)
owns adoption; [audit register](audit-register.md) records source limits. The selected
result-path trace at `9c47d88a` is inherited from an earlier review. The 2026-10-23 metadata
refresh is separate and does not qualify an installed SDK, CLI, native service or
changed source revision.

## 1. Existing Eliot owners to keep

| Responsibility | Existing owner |
| --- | --- |
| Authenticated Store IPC | `swarm-client::{Client,ModuleLink}` |
| Process family/private files/atomic durability | `swarm-process` |
| Public contracts and bounded DTOs | `swarm-contracts` |
| Task/Attempt/Operation/authority/results | kernel Store |
| Immutable page/artifact publication and assembly | Store `results` / `artifacts` |
| Object disclosure and assembly rights | Store `object_scope` |
| Native session/readback | selected adapter only |
| Workspace/Git write authority | Store workspace plus bounded host Git effect |
| Checks | `swarm-checks::execute_with_plan` with one selected execution owner |
| Scripts | `swarm-scripts` plus one selected worker wire |

No new adapter framework, broker, universal journal, EffectRunner or result database.

## Current donor/source register

These entries name bounded mechanisms and the Eliot owner that may use them. Upstream
release metadata is a freshness check, not an instruction to upgrade. Eliot package
locks and inspected source commits are recorded separately from the installed native
CLI/service version, which remains unknown unless an actual launch reports it. Checked
2026-10-23.

| Donor / source | Reuse and exact inspection point | Eliot state; current upstream fact | Owner and limit |
| --- | --- | --- | --- |
| [Muse SDK](https://github.com/meta-models/muse-code-sdk) | Official rolling [`turn/start`](https://meta-models.github.io/muse-code-sdk/next/generated/msp/methods/turn-start/), [`view/page`](https://meta-models.github.io/muse-code-sdk/next/generated/msp/methods/view-page/) and [`item/readOutput`](https://meta-models.github.io/muse-code-sdk/next/generated/msp/methods/item-readoutput/): command identity, cursor and output-ref/range. | Eliot locks `@muse-code/sdk` 1.3.0 (`modules/muse/package-lock.json`); inherited schema/source review is pinned to [a7c10c5](https://github.com/meta-models/muse-code-sdk/tree/a7c10c5dd3f66be412077d29f9d11111af70317b). Rolling `/next` docs checked 2026-10-23 do not qualify every field for 1.3.0. Native Muse version is runtime-reported, not inferred from the lock. | `modules/muse/bridge.mjs`: retain Operation-derived command IDs before submission; use exact output-ref IDs, not URIs, and bounded cursors. The rolling contract caps `item/readOutput` at 6 MiB and allows short view pages; also bound total discovery work. No third executor or repeated unbounded source discovery. |
| [Codex app-server](https://github.com/openai/codex) | V2 turn identity and history in [`turn.rs`](https://github.com/openai/codex/blob/2b21129c6b8120e63f9ac510fe99e8b02ef43292/codex-rs/app-server-protocol/src/protocol/v2/turn.rs). `clientUserMessageId` correlates input with history; `expectedTurnId` scopes active-turn steering. | Eliot Rust adapter talks to app-server; no locked SDK/runtime version establishes the installed native Codex revision. Upstream `turn.rs` last checked at commit [`2b21129`](https://github.com/openai/codex/commit/2b21129c6b8120e63f9ac510fe99e8b02ef43292), 2026-10-23. | `crates/swarm-adapter-codex`: use identity for correlation and readback while retaining exact pre-IPC page persistence. Neither field is a durable idempotency guarantee for retry; the upstream [open idempotency proposal #32254](https://github.com/openai/codex/issues/32254) requests that extension and is not runtime proof. Do not replay an uncertain prompt. Historical Python bridge is provenance only. |
| [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk-typescript) | SDK hooks/control and terminal result frames; retain selected body and ready page before their only copy disappears. [Release history](https://github.com/anthropics/claude-agent-sdk-typescript/releases). | Eliot locks `@anthropic-ai/claude-agent-sdk` 0.3.287 and matching platform packages in `modules/claude/package-lock.json`. SDK launches its bundled Claude Code binary by default; a configured `pathToClaudeCodeExecutable` may override it. The actual native version is reported by the init frame. Latest official release checked: 0.3.309, published 2026-10-22 22:18:33 UTC. | `crates/swarm-adapter-claude` and `modules/claude`: defer/resume is a native control seam, not durable Store delivery. Upstream issues [#474](https://github.com/anthropics/claude-agent-sdk-typescript/issues/474) (reports against .251) and [#395](https://github.com/anthropics/claude-agent-sdk-typescript/issues/395) (reports against .220) remain unverified against Eliot’s .287 and upstream .309; do not generalize them as confirmed behavior. |
| [OpenCode](https://github.com/anomalyco/opencode) | Eliot endpoint wiring in [the standalone adapter](../../crates/swarm-adapter-opencode/src/native.rs) and its [source review](../../crates/swarm-adapter-opencode/README.md). | Host `runtime/opencode_v2` remains active until standalone cutover. The module-local package lock 2.0.7 does not identify the current external service; the built-in route reports its version and offers an optional `expected_version` check. [R6 evidence](../qualification-2026-10-03.md) stays scoped to the recorded built-in installation. Official V2 docs were captured 2026-10-01; inherited source review is at `930c0c8b6a3c41ceb73b9372f878b5a1186b4d4e`. | Target owner `crates/swarm-adapter-opencode`: select and qualify the actual native contract exposing durable inbox/input identity and assistant `parentID`. Its older selected projection currently rejects result reads without that parent edge. Local endpoint code alone does not establish installed API support. |
| [Paseo](https://github.com/getpaseo/paseo) | Timeline live/canonical split, ranges/gaps, declaration-first child discovery; prior exact review [`v0.10.3`](https://github.com/getpaseo/paseo/tree/v0.10.3). | Pattern only, no import. Latest stable release checked: v0.12.0 (2026-10-19). Historical reviewed code remains v0.10.3, not current source review. | Existing Eliot projection/observation owners. Transfer semantics only; no Paseo authority, storage, UI or app. |
| [ACPX](https://github.com/openclaw/acpx) | Optional ACP owner/queue/watch and session continuity; reviewed source is [v0.19.3](https://github.com/openclaw/acpx/tree/v0.19.3) at `ee6090df63afa9d6f7fc1c091e1961e50f2f1476`. | No code installed. v0.19.4 (`8e396609238086dee6a407fdb3b3ac46dbdedd70`) was a resolved candidate whose source review remains owed. Latest release metadata checked: v0.20.0 (2026-10-20), not reviewed here. | `crates/swarm-adapter-command` owns selected ACP backend. ACPX is conditional for a real consumer; its queue/watch is not a general replay key or another Store. |
| [Waku](https://github.com/egoist/waku) | External-service ownership, one sequential worker, reconnect distinct from process exit, terminal from execution outcome; historical source [`v0.1.20`](https://github.com/egoist/waku/tree/10bcd728c9b2f07d664cfece3c8c870d3418ddd2). | Pattern only, no import. Current upstream main checked 2026-10-23 at [`10bcd72`](https://github.com/egoist/waku/commit/10bcd728c9b2f07d664cfece3c8c870d3418ddd2); GPL-3.0-only. | `crates/swarm-adapter-opencode`: semantics only; do not vendor/link Waku or import its desktop product. Reject same-ID fresh-session fallback and ignored settings failures. |

### Native Goal and Eliot Goal

The native interfaces are execution controls; Eliot Goal remains a scoped
Task/Attempt objective. Eliot Goal completion reads the exact Store Task
acceptance record; neither a native Goal nor Eliot Goal itself grants acceptance.
They may be enabled together.
`goal_message` is a normal attributable `agent.send`; it remains separate from a
native Goal and is not a required continuation wrapper.

| Harness | Current upstream Goal capability and limit | Eliot implementation status |
| --- | --- | --- |
| Codex app-server | Upstream source inspected at [`970111f`](https://github.com/openai/codex/tree/970111f6cb62cc02fe36a82ea7d71b37a2889c97): use [`thread/goal/set\|get\|clear`](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/src/request_processors/thread_goal_processor.rs), with `status=active\|paused` for resume/pause. Active goals may inject into a running turn or start an idle turn; the native Goal extension continues an active goal when its thread becomes idle and restores its runtime on resume ([`extension.rs`](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/ext/goal/src/extension.rs)). This requires the Goals feature and an allowed model provider; protocol statuses also include blocked, usage-limited, budget-limited and complete ([`thread.rs`](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server-protocol/src/protocol/v2/thread.rs)). | The current Rust adapter marks `agent.goal` unsupported (`crates/swarm-adapter-codex/src/lib.rs`). Mapping this API is a target capability, not current support or installed-runtime qualification. Native Codex version is not established by a package lock. |
| Muse | Upstream SDK source inspected at [`09d67ec`](https://github.com/meta-models/muse-code-sdk/commit/09d67eca616b7b02a2d060271123c1fedbdb8fb7): [`goal.rs`](https://github.com/meta-models/muse-code-sdk/blob/09d67eca616b7b02a2d060271123c1fedbdb8fb7/rust/clients/sdk-rs/src/msp/method/goal.rs) describes `goal/set\|edit\|pause\|resume\|clear`. Set/edit need a nonempty trimmed objective and command ID; idle set/edit/resume may wake a fresh goal turn, while busy calls' optional `turnId` names only the admission-time turn and promises no delivery. Pause/clear never name or wake a turn. | `modules/muse/settings.mjs` maps all five operations; bridge.13 journals and submits them natively. The lock is SDK 1.3.0; newer upstream source does not prove the installed Muse runtime supports its full contract. Native Muse version and live Goal behavior remain unqualified. |
| OpenCode | No native Goal API is established by the reviewed service contract. | `crates/swarm-kernel-host/src/runtime/opencode_v2/goal.rs` stores Eliot's `eliot.goal` instruction entry and activates it with one ordinary prompt admission. Automatic GoalProgression's intended single continuation is blocked: its generated `agent.goal` caller misses the `agent.send`-only actor guard in `runtime::next_internal`, reaches absent client registration, and commits the Operation as rejected without selecting a native command ([AR-051](audit-register.md#ar-051)). This is separate from the direct wrapper; neither path grants Task acceptance. |

Reuse native Goal's own objective state, pause/resume, and native continuation when
the selected adapter exposes and qualifies those controls. Keep Eliot's exact
Task/Attempt scope, ordinary `goal_message`, and acceptance checks independent;
do not duplicate native turn-by-turn progression as an Eliot proof/cursor wrapper
or default to unbounded repeated sends.

To refresh this register, inspect the Eliot lock/manifest and adapter call sites, then
the canonical upstream release/source at an exact commit. Record the date, release tag
and full commit separately; distinguish a rolling docs page from a tagged schema, a
locked library from the native runtime, and current release metadata from the source
revision actually reviewed. Keep old fixture/qualification evidence attached to its
original artifact; never mark it newly passed because upstream advanced. Update the
specific owner and capability limits above only when the inspected contract changed.

## 2. Result source and Store-delivery matrix

Share delivery of a ready Store record, not native history/body/file ownership.

| Route | Source and delivery today | Keep | Delete / repair |
| --- | --- | --- | --- |
| Antigravity | Boot-local sequential body; memory outcome queue; page not durable | Exact Operation/conversation/ordinal/content identity | Memory-only body, count eviction and report failure stopping native reducer |
| Claude Rust | SDK result body in NativeControl; intent durable, ready page sent directly | Frame/input/body checks; Applied inconclusive Reconcile | Persist source and ready page; remove intent-without-bytes dead end and Dispatch-only target |
| Codex Rust | App-server history; exact ready page persisted/replayed | Saved-page retry, exact user/turn/final-response identity | Full checkpoint/tombstones, Dispatch-only result, repeated full history/body read per new 24-KiB page |
| OpenCode standalone | Session/message history, exact page outbox | Input/assistant parent, deterministic binding root, exact replay, completed inconclusive readback | Permanent result-source retention graph and Dispatch-only result |
| Muse Node | `view/page` exact revision, `item/readOutput` ranges; ready page checkpoint | Output-ref ID rather than URI, range/digest checks, exact delivery | Unbounded repeated item-discovery walk; broad checkpoint/proof duplication |
| Command | Captured files; exact saved page and ACK | Local source custody and exact page retry | Public release-evidence Operation, tombstones and cleanup/proof machine |

### Common extraction boundary

After two routes delete their old loops, common StoreDelivery contains only method,
existing delivery identity, exact params including page bytes, and original source
metadata. Atomically publish before IPC, repeat identical params until exact ACK, then
remove that delivery. Interrupted local removal is safe through Store replay ACK.
Conflicting bytes fail; no separate local ACK tombstone or JSON proof family is needed.
Native intent, native-write recovery and replaceable latest observations stay distinct.

### Source lookup is not page delivery

Adapter-local source is a body/file, an exact provider locator, or explicit unavailable/
expired/gap. Prefer real native ranges. When an API only returns a whole final output,
materialize that selected immutable result once with existing private-file primitives,
then slice it. This temporary body is not a transcript mirror or generic cache service.
Do not reread/hash the full thread/answer for every small page.

Bound locator discovery across total pages/bytes/time and retain its exact continuation.
One bounded native page does not bound an entire history walk. Reuse the item revision,
message/turn/output ref and relevant cursor instead of rediscovering from the newest
history page on each range. Scope remains the original provider/service/account and
binding; an unrelated current root never supplies missing authority.

Store owns aggregate registered-page coverage. Its target ACK/replay returns complete
coverage or the next missing range for the exact source. The adapter retains source
until complete capture or explicit disposition and never maintains another range union.
An outcome/page ACK alone cannot release the whole body. Optional assembly happens from
Store pages; Bytes and Assemble permissions remain separate.

### Anti-donors inside Eliot

Do not extract OpenCode's journal/reference graph/startup census, Codex's full snapshot
and ACK tombstone tree, Command's release-proof lifecycle, Claude's intent without ready
bytes, or Antigravity's native-reducer-coupled reporting. Also do not copy repeated whole
history lookup or remove every root check under the label of historical recovery.

## 3. Route decisions

### Antigravity

Use the installed serialized warm stream. One input owns one terminal result window.
First slice: durable source body, exact StoreDelivery and Store-authoritative coverage.
Keep unsupported actions explicit; no generic framework before the path works.

### Claude

Keep one Rust Agent SDK adapter. Persist the selected SDK terminal body and ready page
before their only copy can disappear. Current result lookup is NativeControl-backed;
calling Resume or weakening the current-session guard cannot reconstruct lost bytes.
A completed inconclusive readback settles Reconcile `resolved:false`. No reviewed native
Goal in the selected implementation; Eliot Goal remains ordinary input.

### Codex

Keep one Rust app-server owner and exact pre-IPC page persistence/restart replay. Saved
page retry is already separate from a new history read. Keep user/thread/turn identity.
The current Rust adapter rejects `agent.goal`; native Codex Goal mapping is a target
capability only (see [Native Goal and Eliot Goal](#native-goal-and-eliot-goal)). Replace
the full checkpoint/tombstones with lifetime-specific records.

The reader currently verifies current root/model/workspace before reading historical
output. Separate immutable source identity from active execution only through an exact
Store-authorized historical reader. Keep service/account/workspace custody; never simply
delete root checks or reconfigure/resume a session to fetch old bytes. Materialize one
selected final body or use a supported range read, not a full history walk per page.

### OpenCode

Converge on standalone `swarm-adapter-opencode`; delete the built-in host executor and
legacy worker/MCP ownership after finite disposition. Keep durable input ID,
first-admission-wins, assistant parent, wake coalescing and read-only Reconcile.
`set_root_hint` fixes native root per binding generation; that guard is not proof of an
arbitrary root-swap defect. Historical capture retains its original authorized source.
Replace permanent result-source retention with Store coverage release, not another graph.

### Muse

Keep active Node over the official SDK during refactor. It already has Operation-derived
command ID, exact revision/output-ref reads and checkpointed delivery. Retain native
`item/readOutput` ranges but replace `readItem`'s unbounded repeated history discovery
with bounded continuation and locator reuse. Preserve membership/source authorization.

Retire the custom Rust candidate after finite old-work disposition. Revisit the official
Rust SDK only when it preserves pre-effect command identity and deletes more code than
it adds. Do not add a third executor or mirror a provider conversation.

### Command Batch/ACP

Keep one Batch and one stable ACP v1 route. Retain captured output and exact saved-page
delivery. Store-complete coverage permits local source cleanup without another public
release Operation or proof packet. Queue admission, prompt start and terminal differ.

## 4. External donor patterns

### Paseo

Borrow a small semantic session port, optional capabilities, coalesced identity loading
and conservative process cleanup: mismatch clears a record and kills nothing;
uninspectable remains unknown. Do not import UI, timeline, JSON stores or daemon product.

### Waku

Borrow one ordered mutable-state owner, closed commands and one settlement point.
Stream break is not session death. Earlier review identifies GPL-3.0; do not copy/link
source without the required licensing decision.

### ACPX

Borrow opaque source cursors, replay/live boundary, one session owner and viewer
non-ownership. Do not import rotating journal, flows, Task product or storage. It may
replace the ACP backend, not sit beside it. Unknown execution remains unknown.

### Claude Swarm scripts

Keep corrections/unfinished work ahead of fresh work, deterministic rank reasons, one
Manager/worktree, explicit route/model/effort, compact task cards, all-findings correction
and bounded no-progress escalation. Do not copy marker files, lockdirs, newest-log
ownership, hidden fallback, fixed-age restart, infinite lane runners or another queue.

## 5. Native effects and recovery

- OpenCode durable input identity/write-ahead claim is useful; its retention graph is not.
- Codex reset time permits a probe, not a claim of recovered quota. Native Goal stays native.
- Muse usage/result is point-in-time source/cursor evidence.
- Claude callbacks/session state are not durable Store delivery.
- Command/ACP queued cancellation, turn cancellation and session close differ.
- Across routes, uncertain response does not authorize replay; viewer/link detach is not cancellation.

## 6. Observation and cursors

- Codex: bounded rollout reads, not compressed-history hot polling.
- Claude: bounded metadata where available; parent-linked JSONL may require whole-file work.
- OpenCode: WAL-visible read-only indexed SQLite/sequence reads; revisit in-place updates,
  never mark a live DB immutable.
- Muse: session/workspace/branch/activity; `notLoaded` is one host view, not global death.
- Process: one selectively refreshed `sysinfo::System`, PID plus OS birth, no per-sample hash.
- Git: assigned branch, checkout, upstream, push target and remote head are distinct.

One MCP pump/session and Store cursor suffice. Notifications coalesce wakeups;
`monitor.follow` / `report.delta` replay retained facts. `stream.*` is a separate bounded
ephemeral content ring outside the Store writer.

## 7. Checks, scripts, Forge and GitHub

Select one Check executor around `swarm-checks::execute_with_plan`. Keep
`swarm-script-worker` as new-run interpreter and `swarm-scripts` as shared wire. One
Forge owner serializes exact remote repository/ref writes and readback; local Git reads
remain separate. GitHub collection/import/writes have separate ownership; remove copied
projection workflows, not enabled automation. Reuse `swarm-process` custody/files.

## 8. Provider state, capacity and scheduling

Normalize source epoch/sequence, time, freshness, coverage and honest account/service/
connection scope. Unknown is not zero or available. Count/reserve real roots in the
writer transaction; result/readback/control do not acquire model capacity.

Timers/notifications wake one host current-state loop for dispatch, capture, reconcile,
review/correction/acceptance and due messages. Query fairness must survive blocked first
pages. Do not add workflow cursors or scheduler databases.

## 9. Store/API/tooling

Use SQLite application identity, numeric schema versions and transactional migrations;
delete SQL-text hashes/reference-DB DDL checks. Keep public/internal/adapter protocols
distinct. MCP exposure is frontend policy; object authorization stays Store domain logic.
A broken transport retires the link without replaying native mutations.

Verify package bytes at install/launch. Runtime custody uses installed coordinate, path
containment and OS identity, not per-Operation executable hashing. Reuse object grants;
receipt access, Bytes and Assemble are not interchangeable permissions.

## 10. Adoption gate

Adopt only when a current caller uses the mechanism, replaced code/config/tests disappear,
one lifetime/retry owner remains, source limits stay explicit and code decreases or
behavior becomes materially clearer. Routine automation cannot become human work.
Do not vendor an application, extract for one caller or retain an old executor solely
because historical records mention it.
