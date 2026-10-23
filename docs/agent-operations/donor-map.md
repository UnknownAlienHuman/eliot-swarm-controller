# Donor map

Borrow a mechanism only for a retained responsibility and delete replaced Eliot code
in the same slice. A donor pattern is not a dependency, qualification result or reason
to import an application. [Implementation](implementation.md) owns adoption;
[audit register](audit-register.md) records source limits.

## 1. Existing Eliot owners to keep

| Responsibility | Existing owner |
| --- | --- |
| Authenticated Store IPC | `swarm-client::{Client,ModuleLink}` |
| Process family/private files/atomic durability | `swarm-process` |
| Public contracts and bounded DTOs | `swarm-contracts` |
| Task/Attempt/Operation/authority/results | kernel Store |
| Immutable page/artifact publication and assembly | Store `results` / `artifacts` |
| Native session/readback | selected adapter only |
| Workspace/Git write authority | Store workspace plus bounded host Git effect |
| Checks | `swarm-checks::execute_with_plan` with one selected execution owner |
| Scripts | `swarm-scripts` plus one selected worker wire |

No new adapter framework, broker, universal journal, EffectRunner or result database is
required.

## 2. Result source and Store-delivery matrix

The adapters do not have one native result source. Share delivery of a ready Store
record; do not force native history/body/file ownership into one schema.

| Route | Native/result source | Store delivery today | Keep | Delete / repair |
| --- | --- | --- | --- | --- |
| Antigravity | Boot-local serialized response body; no history API | Outcome memory queue; result page not durable; boolean ACK | Sequential Operation/conversation/ordinal/digest identity | Memory-only body, count eviction, report failure stopping native loop, no complete-source ACK |
| Claude Rust | Boot-local SDK result frame/body | Result intent durable; page bytes sent directly; ambiguous ACK becomes Unknown and replay is refused | Exact frame/input/body validation; Applied `resolved:false` Reconcile semantics | Persist ready page before IPC; remove intent-without-bytes dead end and Dispatch-only target |
| Codex Rust | App-server thread/turn/final assistant history | Exact complete `module.result` params persisted before IPC and replayed after restart | Exact ready Store delivery and exact user/turn/final-assistant read | Full checkpoint/tombstones; current-root-only historical gate; Dispatch-only result |
| OpenCode standalone | Provider session/message history with assistant parent edge | Exact params/outbox persisted, ACKed and replayed | Exact delivery, native input ID, assistant parent, Applied `resolved:false` Reconcile | Permanent `result_source`, reverse-reference/retention graph, Dispatch-only normalized result |
| Muse Node | Provider `view/page` and `item/readOutput` by item revision/cursor/output ref | Checkpointed outcome/page retried exactly | Stable locator, bounded range/digest read, exact ready page | Broad checkpoint copies/proof duplication; never mirror transcript |
| Command | Local captured output files | Exact saved page → exact ACK → delete | Local source custody and exact page retry | Release-evidence command, tombstones and cleanup/proof state machine |

### Common extraction boundary

Only this is common enough to share after two live routes delete their old loops:

```text
StoreDelivery
  method = module.outcome | module.result
  immutable delivery key / params digest
  exact serialized params, including ready page bytes
  source Operation + binding/generation + immutable origin metadata needed by Store
  atomic publish before IPC
  resend identical params until exact ACK
  remove after ACK; uncertain local removal is safe because replay is idempotent
```

No durable local ACK tombstone is necessary. If Store committed but the adapter crashed
before deleting the pending file, replaying identical params must return the same ACK.
Different bytes under the same identity fail.

`ResultSource` remains adapter-local:

- durable local body/file;
- provider locator plus session/item/turn/message/cursor/digest;
- or an explicit expired/unavailable/gap state.

Store owns immutable pages and aggregate coverage. The exact `module.result` ACK/replay
must return the registered artifact and either complete coverage or the next missing
range for the same source identity. The adapter does **not** maintain another range
union. It retains local source bytes/locator until Store reports complete coverage or
explicit retention policy disposes them.

Observation is a separate replaceable latest checkpoint. Native intent and native
write recovery stay adapter-specific.

### Anti-donors inside Eliot

Do not extract these into a shared crate:

- OpenCode full operation journal, reverse references, address memos, startup census,
  retention cursor and permanently pinned result sources;
- Codex full snapshot plus page/outcome ACK tombstone tree;
- Command public/private release-proof lifecycle;
- Claude result intent that omits the actual ready page bytes;
- Antigravity memory-only delivery coupled to the native reducer.

## 3. Route decisions

### Antigravity

Use the installed serialized warm stream. One input owns one terminal result window.
First vertical slice: durable local body + exact StoreDelivery + Store-authoritative
coverage/release. Keep unsupported actions explicit. Do not build a generic framework
before this path works.

### Claude

Keep one Rust Agent SDK adapter. Native SDK frame/history control stays local. Persist
ready result params before IPC; never repeat the SDK/model action because ACK was lost.
A completed inconclusive readback settles the Reconcile Operation `resolved:false`.
No reviewed native Goal; Eliot Goal is ordinary input.

### Codex

Keep one Rust app-server owner. Borrow its exact pre-IPC `module.result` persistence and
restart replay. Keep thread/user-message/turn identity and native Goal. Delete the full
checkpoint/tombstone structure after replacing it with lifetime-specific records.
Do not require “current root” when an immutable historical source locator and authorized
completion are sufficient.

### OpenCode

Converge on standalone `swarm-adapter-opencode`; delete built-in host executor and
legacy worker/MCP ownership after finite disposition. Keep durable input ID,
first-admission-wins, exact assistant parent, wake coalescing and read-only Reconcile.
Replace permanent `result_source` retention with Store-complete coverage release. Do
not copy its startup inventory or retention graph.

### Muse

Keep active Node bridge over the official SDK during refactor. It already has explicit
Operation-derived command ID, exact session/item/cursor result reads and checkpointed
Store delivery. Retire custom Rust candidate after finite old-work disposition. Revisit
official Rust SDK only when it preserves pre-effect command identity and deletes more
code than it adds. Do not add a third executor.

### Command Batch/ACP

Keep one Batch profile and one stable ACP v1 route. Retain local captured output and
exact saved-page delivery. When Store reports complete coverage, delete the source file
locally; do not require a second public release Operation, tombstone family or proof
packet. ACP queue acceptance, prompt start and terminal stay distinct.

## 4. External donor patterns

### Paseo

Borrow:

- small semantic session port;
- optional capabilities separated from provider implementation;
- coalesced load by identity;
- exact process owner and conservative cleanup: mismatch clears record, kills nothing;
- uninspectable process remains unknown.

Do not copy UI, timeline, JSON stores, provider registry, daemon or plugin product.

### Waku

Borrow one ordered owner of mutable native state, closed command enum and one settle
point. Stream break is not process/session death. Waku is GPL-3.0; do not copy/link
source without a licensing decision.

### ACPX

Borrow opaque source-scoped cursor, replay/live boundary, one owner per session and
viewer non-ownership. Unknown owner outcome stays unknown. Do not import rotating
journal, flows, Task product or storage. ACPX may replace the current ACP backend, not
sit beside it.

### Claude Swarm scripts

Keep correction/unfinished work ahead of fresh work, deterministic rank reasons, one
Manager/worktree, explicit route/model/effort, compact task card, all findings in one
correction and bounded no-progress escalation.

Do not copy marker files, lockdirs, newest-log ownership, hidden fallback, fixed-age
restart, infinite lane runners, raw controller JSON prompts or another queue database.

## 5. Native effect and recovery lessons

- **OpenCode:** durable input identity and write-ahead claim are native-effect donors;
  its result retention graph is not.
- **Codex:** reset time permits one probe; unavailable quota is not recovered. Native
  Goal remains native.
- **Muse:** usage/result data is point-in-time “as of” native source/cursor.
- **Claude:** SDK callback/session state is not durable Store delivery.
- **Command/ACP:** queued cancel, turn cancel and session close are different.
- **All routes:** lost response stays uncertain; viewer/link detach does not cancel work;
  no prompt replay from a Store-delivery retry.

## 6. Observation and cursor donors

- Codex: bounded rollout reads; compressed history is not a hot polling source.
- Claude: bounded metadata where available; parent-linked JSONL reconstruction can
  still require whole-file work.
- OpenCode: read-only WAL-visible SQLite/indexed sequence reads; revisit open messages
  that update in place; no `immutable=1` on a live DB.
- Muse: session log/workspace/branch/content activity; `notLoaded` is one host view, not
  global death.
- Process: one `sysinfo::System`, selective refresh, PID + OS birth identity; no
  per-sample executable hash.
- Git: machine formats; assigned branch, checkout, upstream, push target and remote head
  remain distinct facts.

One MCP pump/session and one Store cursor are enough. Notification is a coalesced wake
hint; `monitor.follow` / `report.delta` is authoritative replay. `stream.*` remains a
separate bounded ephemeral content ring outside the Store writer.

## 7. Checks, scripts, Forge and GitHub

- Select one Check executor around `swarm-checks::execute_with_plan`; remove the other.
- `swarm-script-worker` stays the new-run interpreter; `swarm-scripts` owns shared wire.
- Keep one Forge remote-write owner with exact repository/ref serialization and
  remote-ref readback. Local workspace Git reads are separate.
- GitHub intake, requirement import and writes are separate owners. Remove automatic
  label/publication projection state machines, not selected automatic behavior.
- Reuse `swarm-process` groups, capture and private files.

## 8. Provider state, capacity and scheduling

Normalize provider facts at the adapter with source epoch/sequence, observed time,
freshness, coverage and honest account/service/connection scope. Unknown is not zero
or available.

Capacity is a writer-transaction count of actual root claims. Result, refresh,
reconcile and other readback/control do not consume model capacity.

Timers and notifications are wake hints. One host loop queries current state for
`dispatch_once`, `capture_once`, `reconcile_once`, review/correction/acceptance and due
messages. Do not create per-domain workflow cursors or scheduler databases.

## 9. Store/API/tooling discipline

Use SQLite `application_id`, numeric `user_version`, one small migration ledger and
`BEGIN IMMEDIATE`; delete SQL-text hashes and reference-DB/tokenized-DDL checks.

Keep public, internal-host and adapter protocols distinct. MCP exposure is frontend
policy; Store object authorization remains domain logic. Reuse `swarm-client` bounded
multiplex; a transport failure retires the link but never replays the request.

Verify package bytes once at install/launch. Runtime custody uses installed coordinate,
path containment and OS identity, not per-Operation executable hashing.

## 10. Adoption gate

A donor lands only when:

1. a current caller uses it;
2. replaced Eliot code/config/tests disappear in the same slice;
3. one owner remains for retry, lifetime and recovery;
4. source limits stay explicit;
5. total owned Rust/JS/glue decreases or behavior becomes materially clearer;
6. routine automation is not replaced by human work.

Do not vendor an application to claim integration. Do not extract for one caller. Do
not retain an old adapter merely because historical records name it.
