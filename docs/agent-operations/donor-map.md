# Donor Map

Use a donor only for a retained responsibility and remove the Eliot mechanism it
replaces. A source pattern is not a dependency, qualification result or reason to
import another product. [Implementation](implementation.md) owns adoption.

## 1. Adoption classes

| Class | Meaning |
| --- | --- |
| Maintained API/package | Call the donor's current supported interface directly. Do not wrap it in another private protocol. |
| Pattern | Reimplement a small documented mechanism independently. Copy no code. |
| Source reuse | Copy/link a named unit only after license/notices review and with a deletion plan for the Eliot code it replaces. |

Reject a donor when it adds another Task store, scheduler, session owner, transcript
mirror, workflow language or compatibility matrix.

## 2. Semantic adapter boundary

### Paseo — semantic session port, pattern only

Reviewed:
[`AgentClient` / `AgentSession`](https://github.com/getpaseo/paseo/blob/f7a0e2cbea875d44d36ac4801bfe5467fa17cdac/packages/server/src/server/agent/agent-sdk-types.ts).

Useful:

- one small session interface;
- start turn, exact steer, reply, interrupt, close and configure;
- optional capabilities separate from provider implementation;
- provider events converted at the driver boundary.

Use the shape. Do not copy Paseo's daemon, UI timeline, registry, plugin lifecycle,
persistence or catalog.

### Waku — one ordered native owner, pattern only

Reviewed:
[`driver/mod.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/mod.rs)
and
[`driver/opencode.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs).

Useful:

- one worker owns mutable native session state;
- closed command enum;
- serialized command/event handling;
- one turn settle point;
- stream break is not automatically session/process death;
- event storage and wake notification are separate.

Waku is GPL-3.0 and its provider drivers are large. Pattern only; no copy/link without
a separate license decision.

### Existing Eliot owners

| Existing code | Reuse |
| --- | --- |
| `swarm-process::module_owner` | Verified adapter membership and private state root; add one common bootstrap reader only after two routes delete duplicate parsers. |
| `swarm-process::permissions` | Atomic private create/replace/read/remove and durability barriers. |
| `swarm-client::ModuleLink` | Authenticated hello and module calls. Add common delivery only after two real adapters share the exact same semantics. |
| `swarm-contracts` | Current semantic actions and bounded normalized facts; remove vendor proof/profile matrices. |

No adapter-framework crate is needed.

## 3. Store delivery: donors and anti-donors

### Minimal mechanism to reuse

The useful common mechanism is only:

```text
persist already-produced Store params
send them
delete after exact Store ACK
```

Use one immutable file per terminal outcome/result page and one replaceable latest
checkpoint per observation source. Existing private-file primitives are sufficient.

### Existing Eliot journals are anti-donors as whole units

- OpenCode `journal.rs` combines native intent, result selectors, outcome revisions,
  torn-tail evidence, startup census, retention cursors, references, checkpoints,
  cleanup and Store outbox.
- Claude `journal.rs` combines native intent/outcome, work queues, tombstones, boot
  markers, references, cleanup and Store delivery.
- Command `journal.rs` combines batch/ACP effect state, captured files, release
  evidence and delivery.

Do not extract these files into a shared framework. Reuse only the exact-envelope
persistence semantics after separating native-effect ownership from Store delivery.

No common payload digest is required merely to replay a private retained JSON value.
Keep content hashes for actual result/artifact bytes.

## 4. Official ACP — adapter protocol, not Eliot's bus

Reviewed the official
[ACP v2 migration guide](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/docs/protocol/v2/migration.mdx)
and current schema source.

Useful semantics:

- `session/prompt` response is admission/acceptance, not terminal completion;
- progress and terminal state arrive through ID-addressed state updates;
- updates are upserts, not an append-only business journal;
- capabilities declare optional operations;
- unknown/extensible protocol values are tolerated at the ACP boundary;
- session cancel and close remain distinct.

The project's current `agent-client-protocol` dependency and upstream ACP v2 draft
belong to different package/protocol generations. Do not expose both generations in
Core or copy upstream's compatibility policy. Command selects one maintained ACP
generation during its route migration, maps it to Eliot actions, drains concrete old
work and deletes the old profile.

ACP stays inside the Command adapter.

## 5. ACPX — owner, queue and passive watch patterns

Reviewed:
[`shared.ts`](https://github.com/openclaw/acpx/blob/0d874f9a98de2a82ed8c5a4746053bd0a46052c3/src/runtime/shared.ts)
and
[`session-watch.md`](https://github.com/openclaw/acpx/blob/0d874f9a98de2a82ed8c5a4746053bd0a46052c3/docs/session-watch.md).

Useful:

- one queue owner for one named session scope;
- shared controls do not create a competing fallback connection;
- queue acceptance differs from native prompt start;
- targeted cancellation names the accepted request;
- detaching a client/watch does not cancel work or extend owner lifetime;
- lost owner before terminal becomes explicit unknown;
- passive watch replays retained events then follows;
- cursor is opaque, exclusive and source-scoped;
- expired/foreign cursor is explicit, never a silent skip;
- replay/live handoff has a synchronization boundary.

Important exclusions:

- ACPX request ID is not a native idempotency key;
- shared runtime does not provide universal steering;
- flows/workflow language are not needed;
- do not layer ACPX beside the existing ACP owner.

Adopt ACPX only as a complete optional backend replacing equivalent Command ACP
ownership, or use the above behavior as a pattern.

## 6. OpenCode — durable logical input and gap-safe stream

Reviewed:
[current v2 session contract](https://github.com/anomalyco/opencode/blob/da9d436c36e0145110531ffcd7b15e50016fdd82/specs/v2/session.md).

Useful:

- inbox item is durable before advisory execution;
- stable matching inbox ID converges on first admission;
- cross-session/type ID reuse fails;
- process-local busy state is not durable liveness;
- execution claim supports recovery but does not promise exactly-once external tools;
- one logical step may contain several physical attempts;
- ambiguous external effects are not replayed;
- session log is durable while live deltas are advisory;
- subscribe-before-replay plus a sync watermark closes the replay/live race.

Use stable native input ID per Eliot Operation, inspect inbox/message/log before retry,
and expose physical attempts as observation under one logical input. Do not copy the
entire OpenCode event architecture or its journal into Core.

## 7. Codex — exact logical input, turn guards and account snapshots

Reviewed current app-server protocol sources for turn start/steer and account rate
limits.

Useful:

- `clientUserMessageId` identifies one logical user input;
- thread and turn IDs identify native execution;
- `expectedTurnId` guards exact steering;
- account-rate-limit snapshots are separate from run usage;
- rolling notifications may omit account metadata and must not clear previously
  retained metadata.

Use these identities inside the Codex adapter. JSON-RPC request ID is
connection-scoped, not durable dedupe. Store one source-scoped current account
snapshot, not one copy per Codex session.

## 8. Other harness mappings

| Harness | Native responsibility |
| --- | --- |
| Muse | Durable command ID, native Goal, turns, explicit recovery, usage and local session log. Select one Node/Rust executor. |
| Claude | SDK session/resume, permission/reply/configure/interrupt and official local session readers. No reviewed native Goal; Eliot Goal remains available. |
| Command batch | One admitted process/run window and captured output; no fake live controls. |
| Antigravity | One serialized input/result window. Unsupported reply/configure/Goal/steer remains absent. |

Harness Goal is default where supported. Eliot Goal remains an independent ordinary
message plugin.

## 9. Local observation donors

### Two cursor levels

Provider readers own provider cursors. Store owns `observation_id` for
`monitor.snapshot`/`monitor.follow`. Never pass a Store cursor into a provider reader
or treat provider retention loss as a Store-journal gap.

### Gap-safe read pattern

Where the provider supports replay plus live subscription:

1. obtain source epoch;
2. subscribe before or with replay;
3. read after the retained cursor to a watermark;
4. apply ID-addressed upserts;
5. commit normalized facts, next cursor and coverage;
6. continue live updates.

For JSONL, SQLite and polling sources, bound bytes/records/time and revisit mutable
open records. A truncated tail, WAL gap, expired cursor or source replacement becomes
an explicit coverage gap and bounded rescan.

### Codex

Use thread identity, `Thread/GitInfo`, bounded rollout JSONL reads and the session
index. Compressed history may require whole-file decoding; do not poll it frequently.
Index mtime is not work activity. Ephemeral threads may have no durable log.

### Claude

Official session listing can use bounded head/tail reads; full transcript
reconstruction parses JSONL and parents. Page size does not bound underlying I/O.
Observe known changed files frequently and inventory less often. Do not add a
transcript mirror only for monitoring.

### OpenCode

Use one adapter-owned read-only WAL-visible SQLite connection. Query indexed new
sequences and revisit known open messages that update in place. `data_version` is an
unchanged-pass hint on the same connection, not a durable cursor. Never use
`immutable=1` for a live database.

### Muse

Use the native durable log, workspace/worktree/branch facts and content-activity
events. `parentSessionId` may be a containing root, not the direct spawning parent.
`notLoaded` is one host's view, not proof of death.

### Processes, Git and GitHub

Use selective `sysinfo` refresh but preserve stronger OS birth/handle identity for
controls. CPU needs two samples. Do not hash executable bytes on every sample.

Use `git worktree list --porcelain -z`,
`git status --porcelain=v2 --branch -z` and `git for-each-ref`. Observation never
changes checkout, index or remote config. Assigned branch, actual checkout, upstream,
push target and remote GitHub ref are separate facts.

## 10. Quota and provider availability

Native quota protocols are adapter code. The shared snapshot contains source/epoch,
monotonic sequence, time/coverage, actual scope, normalized availability and bounded
native-labelled buckets/windows/credits.

Do not put in the universal contract:

- Codex or Muse method-name allowlists;
- an enum of currently known vendors;
- artifact-version allowlists;
- an SHA of authentication context as authority;
- provider-specific permission fields;
- duplicate binding/route/model provenance already known to Store.

If no stable account identity exists, keep a connection-scoped snapshot and do not
merge it. Missing quota is unavailable. Run usage, account capacity and provider
availability remain separate.

## 11. Claude-script experience

Preserve behavior, not scripts:

| Old mechanism | Product rule |
| --- | --- |
| Compact `TASK.md` | one current slice packet with objective, symbols, deletion list and check |
| `Agent-Watch.py`/vendor readers | passive joined projection with independent activity, connection, process and source freshness |
| queues and `issue_settled.py` | do not wake work without new actionable input |
| retained refuted findings | do not repeat disproved investigations |
| candidate-specific evidence | Store Operation and exact candidate, not marker files |
| manager branches/microtasks | one worktree per manager; narrow leaf tasks |
| provider error tables | typed route condition and capacity, not generic “429=quota” |
| task card renderer | concise goal/requirements/paths/commands, not raw Store JSON and digest packets |

Do not port the scripts as wrappers, compatibility daemons, marker files or a generic
workflow language.

## 12. Existing libraries

| Library | Keep | Do not add |
| --- | --- | --- |
| `rmcp` | MCP transport/tool routing | another MCP framing layer or readiness gate |
| `agent-client-protocol` | selected Command ACP wire generation | ACP wrappers around other harnesses |
| `croner` | calendar evaluation | per-rule timers or another scheduler DB |
| `tokio`/`rusqlite`/`serde` | runtime, WAL Store and current typed records | broker, ORM, EAV store or protocol generator |
| `tracing` | targeted diagnostics | second logging framework or universal business event schema |
| `notify` | filesystem hints | event-as-state assumptions |

Current GitHub effects use `gh`. Add `octocrab` only if one retained requirement and
measured deletion justify replacing `gh`; never keep both.

## 13. Integration discipline

A donor is integrated only when:

1. exact source/version and license class are recorded;
2. a current caller uses the mechanism;
3. replaced Eliot code is removed in the same connected slice;
4. total Rust+JS+glue falls or a named missing behavior is delivered;
5. retry, lifecycle and durable-state ownership remain singular;
6. unsupported cases and source limits are explicit;
7. the worker receives the named mechanism, not the donor's entire architecture.
