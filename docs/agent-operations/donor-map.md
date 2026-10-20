# Donor Map

Use a donor only for a retained responsibility and delete replaced Eliot code in the
same slice. A pattern is not a dependency or qualification result.
[Implementation](implementation.md) owns the adoption boundary.

## 1. Shared adapter boundary

### Paseo — semantic session port and conservative custody

Use as patterns:

- small session interface with semantic start/steer/reply/interrupt/close/configure;
- optional capabilities separated from provider implementation;
- provider events translated at the driver boundary;
- exact persisted process owner; identity mismatch clears record but kills nothing;
- uninspectable process remains unknown.

Do not copy Paseo UI, timeline, JSON stores, provider registry, daemon or plugin
product.

### Waku — one ordered native owner

Use as a pattern: one owner of mutable native state, closed command enum, serialized
command/event handling, one settle point, and stream break not equated with process or
session death. Waku is GPL-3.0; do not copy/link source without a licensing decision.

### Existing Eliot owners

| Code | Reuse |
| --- | --- |
| `swarm-process::module_owner` | Process membership, private bootstrap and exact OS custody. |
| `swarm-client::{ModuleLink,Client}` | Authenticated Eliot IPC and existing bounded multiplex. |
| `swarm-contracts` | Current DTO owner; replace string/vendor proof matrices with semantic types. |
| `swarm-process::{Group,capture,permissions}` | OS process family, bounded pipes and private atomic files. |
| `swarm-checks::execute_with_plan` | One admitted Check command execution; select one host/standalone owner, not both. |
| `swarm-scripts` | Existing Script domain/types; make it the single host/worker wire owner. |

No new adapter framework, broker, EffectRunner or universal journal crate is required.

## 2. Harness mappings

| Harness | Native mechanism to use | Boundary |
| --- | --- | --- |
| Codex | current app-server thread/turn/account protocol and native Goal | one logical Eliot input, exact thread/turn IDs, separate account limit and turn usage |
| OpenCode | durable inbox/message identity, process-global Session owner, next-turn/steer, readback | no public Eliot loop-step/MCP phases; one Operation may contain physical Attempts |
| Muse | current Node bridge over official low-level SDK Connection; native sessions/turns/Goal/recovery/usage | keep explicit Eliot-derived command ID; retire custom Rust candidate after finite transition |
| Claude | current Agent SDK session/query and official local readers | one adapter; no reviewed native Goal; no transcript mirror solely for monitoring |
| Command ACP | selected current stable ACP v1 generation | translate prompt/reply/cancel/close inside Command; ACP is not Eliot's bus |
| Antigravity | installed CLI warm stream | one serialized input/result window; unsupported actions remain explicit |

Harness Goal is default where supported. Eliot Goal is an independent ordinary-message
plugin.

## 3. Muse decision and official Rust SDK

### Current decision

Keep `modules/muse` Node bridge.13 as the only new-work Muse executor during the
refactor. It is the active/default route and already implements:

- explicit Operation-derived command ID persisted before native I/O;
- session start/resume with no fresh-session fallback;
- native turn/start and turn/steer with exact expected turn;
- reply, configure, native Goal, refresh, reconcile and result;
- point-in-time usage and child/session observation;
- same-bridge Host reconnect without replaying native input.

Retire `crates/swarm-adapter-muse` after concrete 0.1.2 work is finished/exported. Its
custom Rust path duplicates MSP wire/schema, transport, session fold, effect journal,
process custody, observations and results. The following source files alone exceed
605 KB before the remaining observation/result/owner modules:

- `journal.rs`: 226,845 bytes;
- `driver.rs`: 143,763 bytes;
- `commands.rs`: 78,709 bytes;
- `msp.rs`: 56,192 bytes;
- `module_runtime.rs`: 54,776 bytes;
- plus tests and the other runtime files.

The Rust descriptor is disabled and its recorded qualification covers one bounded
result path, not the complete default route.

### Official `muse-code-sdk` Rust crate

The upstream Rust SDK at reviewed source
`22a8a56103c106009f7452c4ef5631e58cb8485c` is a real future replacement
candidate:

- `muse-code-sdk` 1.4.11, MIT, Rust 1.97+;
- spawn/attach, session start/resume, typed turns, approvals, fold, gap recovery and
  bounded shutdown;
- generated MSP types instead of Eliot's copied wire schema;
- exact SDK/host version pairing is required because protocol can change across
  minor releases.

It is **not** added beside the Node route now. The high-level
`Session::send_user_turn` mints the command ID internally. Eliot currently needs the
Operation→native command ID established before uncertain submission. The low-level
constructor with a custom minter does not perform the normal handshake. Re-evaluate
only when upstream exposes the production seam Eliot needs, or when a small adapter
using public low-level Connection/generated types can delete more code than it adds.

Do not:

- add a third Muse executor;
- translate Node and Rust private journals;
- upgrade Node SDK/host in the same change as executor consolidation;
- relabel old Rust receipts as Node or newer SDK receipts;
- use high-level SDK recovery as a second authority beside Store Operations.

### Node simplification target

Split the current checkpoint into only:

```text
SessionRecord
NativeEffectRecord per unresolved command
StoreDelivery for exact already-formed outcome/result
latest replaceable Observation checkpoint
```

Keep the native transcript and provider credentials in Muse. Keep explicit command
IDs, targeted reconciliation and native Goal through the same connection. Use
module-owner process custody and bounded close. Remove full descriptor/schema equality,
repeated Task/source hashes, artifact allowlists and duplicate delivery/proof state.

## 4. Native effect and recovery lessons

- **OpenCode:** adopt durable inbox identity, first-admission-wins, wake coalescing,
  write-ahead execution claim and replay/live watermark. Do not copy its runner or
  history product.
- **Codex:** `ordinary_usage_allowed=null` is unavailable, not recovered. Reset time
  opens one probe; it is not an availability fact.
- **Muse:** usage is point-in-time “as of” data. Preserve native source/cursor and the
  command ID actually correlated to the Eliot Operation.
- **ACP/ACPX:** queue acceptance, native prompt start and terminal are different;
  queued cancel differs from turn cancel; lost reply stays uncertain; viewer detach
  does not cancel work.

ACPX is acceptable only as a complete optional backend replacing the current ACP
owner/queue. Do not layer it beside the existing owner. Request ID is not a universal
idempotency key; do not import ACPX flows, Task product or storage.

## 5. Observation and cursor lessons

### Provider-local sources

- Codex: bounded incremental rollout reads; compressed history may require whole-file
  decode and is not a hot poll source.
- Claude: metadata can be bounded, but parent-linked transcript reconstruction may
  parse whole JSONL; page size does not bound underlying work.
- OpenCode: one read-only WAL-visible SQLite connection, indexed sequence reads and
  revisit open messages that update in place; no `immutable=1` on live DB.
- Muse: durable session log plus workspace/branch/content activity; `notLoaded` is one
  host view, not global death.
- Processes: one `sysinfo::System`, selective refresh, exact OS birth identity for
  control, no per-sample executable hash.
- Git: machine formats for worktrees/status/refs; assigned branch, checkout, upstream,
  push target and remote head remain distinct.

### ACPX watch — opaque cursor and non-owning observer

Reviewed `openclaw/acpx@40ecc17ec17cd8abf4eb311bbc13ba635657f798`:

- one watcher replays bounded retained events and then follows live;
- cursor is opaque, source-scoped and exclusive on resume;
- malformed/foreign/future/expired cursor fails explicitly;
- watcher disconnect never cancels work;
- unknown owner outcome remains unknown;
- a slow observer whose retained history rotated receives cursor-expired, not silent
  skipping.

Borrow cursor/replay-live/non-ownership semantics. Do not copy ACPX's rotating journal:
Eliot already has durable Store observations.

### Eliot MCP target

Use one pump per MCP session and one Store cursor. Notification is a coalesced wake
hint containing the newest known head; `monitor.follow` / `report.delta` is the
authoritative replay. A slow client replaces the pending hint rather than forcing
exact dropped-item scans. Reconnect resumes from the client's last Store cursor; MCP
session state itself is not replayed.

`stream.*` is different: bounded ephemeral native content. Keep it separate from
Store observation history while moving ring work off the writer.

## 6. External-effect workers

### Checks

`swarm-checks::execute_with_plan` already owns one bounded command, process Group,
stdout/stderr capture, cancellation and family-departure facts. The host and
standalone routes both call it. Keep standalone for new CheckRuns, with old host
receipts read back finitely.

Move source/tool-version and Git-hook probes to the selected check executable before
deleting the hidden host entrypoint. Do not create a probe daemon.

### Scripts

`swarm-script-worker` is already the only new-run interpreter owner. Host keeps
admission and settlement. Use `swarm-scripts` for shared Work/plan/start/completion
wire types instead of duplicated compatible structs. Keep Script-specific input pipe,
controller effects and result-schema validation.

### Forge and GitHub

Remote push is uncertain external I/O and needs exact repository/ref serialization
plus remote-ref readback. Keep one standalone Forge worker. Local workspace Git
reads/preparation are separate bounded host operations.

Reuse `swarm-process` Group/capture/private files. Remove automatic label/publication
projection and duplicate Forge installer. PR-description mutation remains only with a
current explicit caller.

### Package integrity

Verify selected package bytes once at install/launch. Runtime ownership uses installed
coordinate, path containment and OS birth/image identity. Do not rehash the full
worker/Git executable for every Operation or ask the child to verify the same package.

## 7. Claude Swarm operational lessons

Keep corrections/unfinished work ahead of fresh work, deterministic rank reasons, one
manager per worktree, explicit route/model/effort, actual native/process facts, one
compact Task card, all findings in one correction and bounded no-progress escalation.

Do not copy marker files, lockdirs, newest-log ownership, route editing, hidden model
fallback, fixed-age restart, infinite lane runners, raw Store/proof JSON in prompts,
per-finding repair loops or another queue database.

## 8. Provider state, capacity and current-state automation

Normalize provider facts at the adapter with honest account/service/connection scope,
source epoch/sequence, observation time, freshness and coverage. No fact permits one
bounded probe; transient retry/reset permits one probe; hard negative requires new
source/config or explicit fallback; unknown/stale holds automation.

Capacity is one writer transaction counting actual root claims. Readback/control
methods consume no model capacity.

Use OpenCode's wake pattern only as a pattern: repeated wakes coalesce and the worker
reads current durable state. Eliot stores due occurrence, uses nearest timer plus slow
safety wake, and admits ordinary idempotent Operations. Review/correction/acceptance
query current candidate state rather than copy it into workflow journals.

## 9. SQLite evolution

Use SQLite `application_id`, application-owned `user_version`, one small numeric
migration ledger and `BEGIN IMMEDIATE`. Explicit migration pre/postconditions plus
bounded `foreign_key_check`/`quick_check` are enough. Do not compare exact SQL text,
build a reference DB, tokenize DDL or add an ORM/migration framework.

## 10. Public API and IPC discipline

Keep application, internal-host and adapter protocols distinct. Retained public
domains own typed parse + exact authorization + handler. MCP exposure is frontend
policy; Store object authorization remains domain logic. Do not generate a policy DSL.

`swarm-client::Client` already supplies bounded multiplex and correct unknown-outcome
semantics. Reuse it directly; a frontend mutex must not serialize the full awaited
RPC. A transport failure may retire the exact current connection but never replay the
request.

Host IPC already has bounded connections, in-flight requests, frames and write
timeout. Do not add a broker. Move pure reads to WAL readers and keep high-volume
ephemeral rings outside the writer.

## 11. Current library integrations

| Library | Keep for | Do not add |
| --- | --- | --- |
| `rmcp` | MCP transport/tool routing | second framing/replay layer |
| `agent-client-protocol` | selected Command ACP v1 types | ACP around every harness |
| `croner` | cron parsing | per-rule timers/scheduler DB |
| `atlas-redact` | complete supplied logical records | cross-chunk claims without buffering |
| `tokio`, `rusqlite`, `serde` | runtime, WAL Store, typed protocol | broker, ORM, EAV, compatibility generator |
| `tracing` | diagnostics | universal business-event schema |
| `notify` | optional wake hints | event-as-authoritative-state |
| official Muse Rust SDK | future Muse replacement candidate | concurrent third executor or hidden command-ID loss |

`octocrab` is optional only if a retained requirement and net deletion justify
replacing `gh`; never keep both.

## 12. Integration discipline

A donor is integrated only when a current caller uses it, replaced Eliot code is
removed in the same slice, total owned Rust+JS+glue decreases or behavior becomes
materially clearer, retry/lifecycle/durable ownership remains singular, and source
limits stay explicit.

Do not vendor an application to claim integration. Do not extract for one caller. Do
not preserve an old adapter because history names it.