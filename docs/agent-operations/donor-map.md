# Donor Map

Use a donor only for a retained responsibility and delete the Eliot mechanism it
replaces. A useful pattern is not a reason to import another product. The concrete
adoption boundary belongs in [implementation](implementation.md).

## 1. Adoption classes

| Class | Rule |
| --- | --- |
| Maintained API/package | Call the current supported interface directly. Do not wrap it in another private protocol. |
| Pattern | Reimplement the small behavior independently; copy no source. |
| Source reuse | Copy/link a named unit only after license/notices review and with an exact deletion plan. |

Reject a donor when it introduces another Task store, scheduler, session owner,
acceptance authority, transcript mirror, workflow language or compatibility matrix.

## 2. Small semantic adapter port

### Paseo — pattern only

Reviewed:
[`AgentClient` / `AgentSession`](https://github.com/getpaseo/paseo/blob/f7a0e2cbea875d44d36ac4801bfe5467fa17cdac/packages/server/src/server/agent/agent-sdk-types.ts).

Useful:

- one semantic session interface;
- start turn, exact active-turn steer, reply, interrupt, close and configuration;
- optional capabilities separate from provider implementation;
- provider events converted at the driver boundary.

Use this shape for Eliot actions/capabilities. Do not import Paseo's daemon, UI,
provider registry, persistence, plugin lifecycle or catalog.

### Waku — ordered owner pattern only

Reviewed:
[`driver/mod.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/mod.rs)
and
[`driver/opencode.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs).

Useful:

- one owner of mutable native session state;
- closed command enum;
- serialized command/event handling;
- one turn settle point;
- stream break does not imply session/process death.

Waku is GPL-3.0 and its provider drivers are themselves substantial. Use the behavior
pattern; do not copy/link the implementation without a separate license decision.

## 3. ACP and ACPX

### Official ACP semantics

ACP cancellation targets the active prompt turn. Cancellation request and terminal
resolution are separate; updates may still arrive before the prompt resolves with a
cancelled stop reason. Eliot therefore distinguishes:

- queued controller input;
- queue-owner acceptance;
- native prompt start;
- active-turn cancel request;
- terminal cancellation.

Do not report a turn cancelled when only the notification was sent.

### ACPX — optional backend or pattern, not another layer

Reviewed current shared runtime:
[`src/runtime/shared.ts`](https://github.com/openclaw/acpx/blob/0d874f9a98de2a82ed8c5a4746053bd0a46052c3/src/runtime/shared.ts)
and
[`shared-sessions.md`](https://github.com/openclaw/acpx/blob/0d874f9a98de2a82ed8c5a4746053bd0a46052c3/docs/shared-sessions.md).

Useful:

- one running queue owner for a named session;
- shared controls refuse a direct fallback that could create a competing provider
  connection/session;
- queue acceptance and native prompt start are separate signals;
- cancellation targets one accepted request ID;
- client detach/shutdown does not cancel admitted work;
- passive watch uses retained cursor/result state;
- missing owner is explicit `backend unavailable`, not silent reconnection through a
  second owner.

Limits:

- `requestId` identifies queue/control status; it is not documented as a universal
  idempotent native replay key;
- shared turns deliberately exclude steering and callback-driven permission handling;
- ACPX flows, checkpoints and TypeScript workflow language are not Eliot
  requirements.

Adopt ACPX only as a complete optional ACP backend that replaces Eliot's equivalent
ACP owner/queue. Otherwise use the ownership/admission/cancel semantics as a pattern.
Do not run ACPX beside a competing Eliot ACP session owner.

## 4. Existing Eliot owners

| Existing code | Reuse boundary |
| --- | --- |
| `swarm-process::module_owner` | Verify supervisor-owned adapter process, binding scope and private state. Extend with one small bootstrap reader only after Antigravity and Claude use it. |
| `swarm-process::permissions` | Atomic private create/replace/read/remove and durability boundaries. Use for Antigravity's route-local Store-delivery outbox; no adapter database. |
| `swarm-client::ModuleLink` | Authenticated hello and module RPC calls. Replace private IPC wrappers after hello is simplified. Do not move native policy into it. |
| `swarm-contracts` | Current shared semantic action/record types. Remove vendor proof structs and historical profile matrices. |

### Extraction thresholds

- **Bootstrap:** Antigravity plus Claude `module_runtime::OwnedBootstrap` can prove one
  common owner/config reader. Delete their duplicate env/path/protocol/claim parsers
  in the same change.
- **Store-delivery outbox:** keep it Antigravity-local first. Move mechanics into
  `swarm-client` only when a second migrated route uses identical boot/lane/ACK
  semantics and both old loops are removed.
- **Command is not a proof route:** its Batch/ACP profile ladder, journal and session
  state would distort the abstraction.
- **Codex and Muse are not first:** their native recovery/Goal/session state machines
  are too large to define a delivery-only shell.

No new adapter-framework crate is justified.

## 5. Native identity and replay

Prefer the harness's durable identity/readback over an Eliot-side generic journal.

| Harness | Native mechanism | Eliot rule |
| --- | --- | --- |
| Muse | MSP session/turn, UUIDv7 `commandId`, durable event log/cursor, native Goal | Persist exact command ID before send. Replay only when MSP permits the same ID with identical params. ACK is admission, not terminal. |
| OpenCode | durable inbox/message identity and session SQLite/log | Retain one native item ID per Operation. Reconcile through inbox/message/session state before retry. Live event stream is observation, not recovery. |
| Codex | `clientUserMessageId`, thread/turn IDs, `expectedTurnId`, turn history | Use exact thread/turn readback and steer guard. JSON-RPC request ID is connection-scoped, not durable dedupe. |
| Claude | SDK session/resume identity and official local history | Serialize per session. Retain only the smallest prepared effect when the SDK cannot classify an uncertain mutation. |
| ACP/ACPX | queue request, prompt-start and retained result/watch | Do not equate queue request ID with native idempotent replay. Unknown submission remains unknown. |
| Antigravity/Command batch | one process/run and captured output | Admit one serialized effect window; never restart/resend to guess whether input ran. |

Native effect state and Store delivery are separate:

- native identity/readback decides whether a provider action happened;
- delivery outbox only resends already-produced Eliot envelopes until Store ACK.

## 6. Vendor mappings

| Harness | Semantic actions retained |
| --- | --- |
| Codex | `Open`, `Dispatch`, `Send`, `Reply`, `Configure`, native `Goal`, `Cancel`, `Recover`, `Refresh`, `Result`, `Reconcile` |
| OpenCode | `Open`, durable `Dispatch/Send`, steer, reply, cancel, background, refresh/result/reconcile; MCP setup remains internal |
| Muse | native `Goal`, queued/steered input, explicit recover/cancel, result and usage; select one Node/Rust executor |
| Claude | open/resume, send, reply/permission, configure, interrupt, result and local session/subagent readers; no reviewed native Goal |
| Command ACP | prompt/send, permission reply, active-turn cancel and session close through the existing ACP crate |
| Antigravity | one serialized `Open`, `Dispatch`, `Send(next_turn)`, `Refresh`, `Result`, `Reconcile`; unsupported controls are absent, not placeholder handlers |

Harness Goal is the default where supported. Eliot Goal remains an independent
ordinary-message plugin.

## 7. Local observation donors

<a id="local-observation-sources"></a>

### Codex

Use native thread identity, `Thread/GitInfo`, bounded rollout JSONL reads and the
append-only session index. Plain and `.jsonl.zst` are two representations of one
logical rollout. Compressed historical reads may decode the whole file and are not a
high-frequency live poll. Index mtime is not work activity; ephemeral threads may
have no durable log.

### Claude

Official SDK session listing can use bounded head/tail metadata reads; transcript
reconstruction can parse full JSONL and parent links. Page size therefore does not
bound underlying I/O. Observe known changed files frequently and run inventory less
often. Reuse maintained SDK summaries; do not create a transcript mirror.

### OpenCode

Use one adapter-owned read-only SQLite connection, query indexed new sequences and
revisit known open messages that update in place. `PRAGMA data_version` is only an
unchanged-pass hint on the same connection, not a durable cursor/activity fact. Use
read-only URI mode; never `immutable=1` for a live WAL database or copy only the main
`.db` file.

### Muse

Use the durable session log, workspace/worktree/branch and content-activity events.
`parentSessionId` may be a containing top-level session rather than the spawning
parent. `notLoaded` is one host's view, not proof of global death.

### Processes, Git and GitHub

Use selective `sysinfo` refresh but preserve Eliot's stronger OS birth/handle
identity for controls. CPU needs two samples and can exceed 100% across cores. Do not
hash executable bytes on every sample.

Use machine Git formats: `git worktree list --porcelain -z`,
`git status --porcelain=v2 --branch -z` and `git for-each-ref`. Observation never
changes checkout/index/remotes. Assigned branch, actual checkout, upstream, push
destination and remote GitHub ref are separate facts. Coalesce one bounded remote-ref
read per repository/ref through the existing `gh` owner; do not require a PR.

## 8. Old Claude scripts as field evidence

The scripts are not code donors. Preserve only verified mechanisms:

| Old behavior | Product rule |
| --- | --- |
| compact `TASK.md` microtasks | one manager-created slice packet with objective, symbols, deletion list and minimal check |
| `Agent-Watch.py` / `oc_busy.py` cross-source join | one passive projection keeping activity, connection, process, assignment and freshness separate |
| settled/review-waiting work omitted from reminders | do not keep waking agents without new actionable input |
| refuted findings retained | preserve verified/refuted investigation results |
| branch/candidate-specific evidence | Store Operation and candidate-specific evidence with consumer ACK |
| one manager per lane/worktree | manager integrates its candidate; cross-owner integration is separate |
| quota/model error tables | distinguish rate limit, exhausted quota, unavailable model and transport failure |

Reject:

- PID-only or newest-file ownership;
- age/low-CPU/mtime kill decisions;
- automatic destructive restart without confirmation;
- first-answer permission/form automation;
- marker files as authority;
- copied reminder/recovery daemons;
- addendum chains as policy;
- forced rotation unrelated to native state.

The old migration principle remains sound: compare the controller and script on the
same facts, then delete the script. Do not preserve scripts as compatibility wrappers.

## 9. Build and tooling donors

### Cargo metadata

Use Cargo's maintained `cargo metadata --locked --no-deps` for workspace package and
local dependency ownership. Do not maintain a second parser for Cargo.lock or infer
runtime compatibility from build-cache paths.

A small changed-package selector may consume metadata and Git diff. It must remain
smaller than the custom classifier it replaces and include real local reverse
dependencies. Do not introduce another build orchestrator solely to replace a script.

### Filesystem primitives

The existing `swarm-process::permissions` implementation already provides bounded
private regular-file checks, atomic same-directory publication, durability barriers
and exact-byte convergence. Reuse it for the Antigravity delivery outbox and small
adapter records. Do not add SQLite, RocksDB or a generic persistence crate.

### Package installation

Use one package manifest and generic installer behavior. Per-artifact PowerShell
functions, exact schema arrays and build-environment equality are not donor-worthy
safety. Keep path containment, reparse protection, private credential references,
atomic publication and selected byte verification.

## 10. Current library integrations

| Library | Keep for | Do not add |
| --- | --- | --- |
| `rmcp` | MCP protocol/transport and tool routing | another framing layer or dispatch readiness gate |
| `agent-client-protocol` | Command ACP wire types | ACP wrappers around all harnesses |
| `croner` | calendar parsing/evaluation | per-rule timers or another scheduler database |
| `atlas-redact` | redaction of complete supplied logical records | cross-chunk claims without buffering |
| `tokio` / `rusqlite` / `serde` | runtime, WAL Store, current typed records | broker, ORM, EAV store or generated compatibility family |
| `tracing` | targeted diagnostics and filtering | second logging framework or universal business-event schema |
| `notify` | optional filesystem change hints | event-as-state assumptions; reconciliation remains required |

Current GitHub effects use `gh`. `octocrab` is optional only if a retained requirement
and measured deletion justify replacement. Never add both clients.

## 11. Integration discipline

A donor is integrated only when:

1. exact source/version and license class are recorded;
2. one current product caller needs the behavior;
3. the replaced Eliot mechanism is removed in the same connected slice;
4. total owned Rust + JS + glue falls or a named missing behavior is delivered;
5. retry, lifecycle and durable-state ownership remain singular;
6. unsupported cases and source limits are explicit;
7. the worker receives the named mechanism, not the donor's entire architecture.

Do not vendor or wrap a donor to claim progress. Do not create a shared implementation
abstraction for one caller. Do not preserve an old adapter because a historical
record names it.
