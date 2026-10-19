# Donor Map

Use a donor only for a retained responsibility and remove the replaced Eliot code.
A source pattern is not a dependency, qualification result or reason to import the
donor's product. [Implementation](implementation.md) owns the concrete adoption
boundary.

## 1. Adoption classes

Every donor use is one of three things:

| Class | Meaning |
| --- | --- |
| Maintained API/package | Call the donor's current supported interface directly. Do not wrap it with another private protocol. |
| Pattern | Reimplement one small mechanism from documented behavior. Copy no code. |
| Source reuse | Copy/link a named unit only after license/notices review, exact version selection and deletion plan for the Eliot code it replaces. |

Reject a donor when it introduces another Task store, queue authority, scheduler,
session owner, acceptance authority, transcript mirror or compatibility matrix.

## 2. Adapter and session boundaries

### Paseo — semantic port and stable workspace identity, pattern only

Reviewed current source:

- `AgentClient` / `AgentSession` semantic methods;
- `docs/data-model.md` project/workspace/agent ownership;
- managed-process bootstrap reconciliation.

Useful mechanisms:

- one semantic session interface: start turn, steer, reply, interrupt, close,
  configure;
- optional capabilities separate from provider implementation;
- persisted `workspaceId` is ownership; runtime does not infer ownership from `cwd`;
- execution `cwd` and backing `worktreeRoot` are distinct facts;
- store methods own atomic persistence instead of exposing read/merge/write loops;
- process cleanup starts from records the product wrote: dead records disappear, PID
  identity mismatch kills nothing, uninspectable state remains for later, only exact
  owned leftovers are terminated.

Use these patterns for Eliot's typed adapter port, workspace ownership and restart
reconciliation. Do not copy Paseo's daemon, UI timeline, JSON persistence, provider
registry, plugin system, relay, schedules or broad feature surface.

### Waku — one ordered command/event owner, pattern only

Reviewed source:

- `crates/waku-core/src/driver/mod.rs`;
- `crates/waku-core/src/driver/opencode.rs`.

Useful mechanisms:

- one worker owns mutable native session state;
- a closed command enum;
- commands and provider events are serialized;
- one idempotent turn settle point;
- event payload and wake notification are separate;
- stream break is not automatically process/session death.

Waku is GPL-3.0 and its provider drivers are themselves substantial. Use only the
behavior pattern unless a separate license decision approves exact source reuse.

### Existing Eliot owners

| Existing code | Reuse |
| --- | --- |
| `swarm-process::module_owner` | Exact supervisor-owned process membership and private state. Simplify plan/resolver identity; do not add another launcher crate. |
| `swarm-process::permissions` | Atomic private create/replace/read/remove and durability barriers for small prepared-effect and Store-delivery records. |
| `swarm-client::ModuleLink` | Authenticated hello, `module.next`, outcome/observation/result delivery. Extend only after two current adapters delete duplicate loops. |
| `swarm-kernel-host::store::workspace` | Existing worktree registration, lease and launch-time conflict check. Replace advisory code scopes rather than adding another lease owner. |
| `swarm-contracts` | Current shared types. Replace string/vendor proof matrices with one current typed semantic protocol. |

No new adapter-framework, queue, lane or code-scope crate is needed.

## 3. Native identity and replay

Prefer the harness's durable identity/readback over Eliot-side replay machinery.

| Harness | Reviewed mechanism | Eliot rule |
| --- | --- | --- |
| Muse | MSP sessions/turns, command identity, durable event cursor and native Goal | Persist exact command identity before send. Replay only under documented MSP same-ID rules. ACK is admission, not terminal. |
| OpenCode | v2 durable inbox row precedes execution; same session/type/item ID is idempotent; execution owner is process-local per Session ID | Retain one native inbox identity per Operation. Same-session wakes may coalesce; different sessions run concurrently. Read inbox/message/session history before retry. |
| Codex | `clientUserMessageId`, exact thread/turn IDs, `expectedTurnId` steering, app-server Goal and account notifications | Logical input uses client message identity; physical attempts/turns are observations. JSON-RPC request ID is not durable dedupe. |
| Claude | Agent SDK session/resume and official local session history | Serialize per session. Retain only the smallest prepared effect when official readback cannot classify it. |
| ACP / ACPX | queue request, prompt-start admission, state updates and retained watch/result | Queue acceptance is not prompt admission; prompt admission is not terminal. Request ID is not a universal replay key. Unknown submission remains unknown. |
| Command batch / Antigravity | process/run identity and captured output | One serialized effect window; retain exact prepared input/output; never restart a process to guess whether input ran. |

One Eliot Operation represents one logical input. Provider retries or continuation
attempts do not create another Operation unless Eliot admitted another input.

## 4. Queue, lane and workspace donors

### Claude Swarm scripts — operational evidence, not code

The old scripts showed useful policy:

- rank returned corrections before new work;
- finish started work before opening more work;
- keep route/model/effort/fallback policy together;
- pause on rate limits and stop new admissions on exhausted quota;
- use non-overlapping work ownership;
- render a compact current task instead of dumping the issue/audit history;
- keep one worktree per manager;
- distinguish active work, waiting, quota and dead process.

They also showed what not to port:

- one daemon per lane;
- file lockdirs and marker files as authority;
- newest-log/title/path ownership guesses;
- fixed-age rotations or restarts;
- automatic form answers;
- queue copies that drift from the Task source;
- broad process-name cleanup.

Eliot keeps the policy as a current-state queue projection and launch transaction.
The UI may call one configured dispatch view a lane, but Core does not persist a new
lane state machine.

### OpenCode — current-state execution coordination

Reviewed v2 session design:

- durable inbox admission precedes advisory execution;
- a stable inbox item ID provides native idempotency for matching session/type;
- `SessionExecution` is process-global and keyed by Session ID;
- repeated same-session wakes coalesce;
- explicit resume joins the active session execution;
- different sessions may run concurrently;
- process-local execution claim is recovery evidence, not proof of exactly-once
  provider/tool effects.

Use the admission/wake separation and one owner per session. Do not import OpenCode's
Session event store, runner, tool orchestration, instruction system or retry engine.

### Paseo — workspace authority

Use the distinction between stable workspace ownership and mutable Git facts:

- ownership uses an explicit workspace/Attempt relation, never cwd inference;
- exact execution directory and backing worktree root remain separate;
- reconciliation updates mutable branch/status facts without changing ownership;
- process cleanup requires positively matched owned identity.

This supports Eliot's existing workspace lease and local observation. It does not
justify a second workspace registry.

## 5. ACP and ACPX

### Official ACP

Useful protocol semantics:

- prompt response means admission, not terminal completion;
- foreground state and terminal stop reason arrive as state updates;
- updates are ID-addressed upserts;
- cancel and close are different operations;
- capabilities describe optional surfaces;
- one connection negotiates one protocol version.

Keep ACP inside the selected Command adapter through the existing
`agent-client-protocol` crate. Core sees semantic Eliot actions, not ACP versions.
Choose one current ACP generation, finish concrete old work and delete the old
profile; do not add v1/v2 branching to Core.

### ACPX — ownership/watch pattern or whole optional backend

Useful mechanisms:

- one shared queue owner for an exact named session scope;
- shared controls do not create fallback competing connections;
- queue acceptance, native prompt start and terminal result are separate;
- cancellation targets a specific queued/active request;
- watcher detach does not cancel work;
- passive watch replays retained history then follows live events;
- cursor is opaque, source-scoped and exclusive; expired/foreign cursor is explicit;
- owner disappearance before settlement reports unknown.

Do not copy ACPX flows, workflow language, Task model or storage beside Eliot Store.
Adopt ACPX only as an optional backend that replaces our equivalent ACP owner/queue,
not as a new layer around it.

## 6. Windows process donors

### CCCC suspended child — conditional source/package reuse

`cccc-windows-process` provides one small primitive:

- spawn child with `CREATE_SUSPENDED` while preserving caller flags;
- recover/retain the primary thread handle;
- assign external ownership before resume;
- dropping an unresumed child kills/reaps it.

Current Eliot uses owner-first Job/process-group topology and therefore already avoids
the pre-assignment race this primitive solves. Do not add CCCC now. Reconsider only
for a demonstrated child-first launch path that cannot be assigned before user code
runs. Never import CCCC scheduler, ledger, UI or observer-loss stop policy.

## 7. Local observation sources

<a id="local-observation-sources"></a>

### Codex

Use native thread identity, `Thread/GitInfo`, bounded rollout JSONL reads and the
append-only session index. Plain and compressed rollouts are representations of one
logical history. Do not run full compressed-history decoding as a high-frequency
poll. Session-index mtime is not work activity; ephemeral threads may have no log.

### Claude

Official SDK session listing can use bounded metadata reads while transcript
reconstruction parses full JSONL/parents. Page size does not bound underlying I/O.
Observe known changed files frequently and run scoped inventory less often. Reuse SDK
summaries when already maintained; do not add a transcript mirror.

### OpenCode

Use one adapter-owned read-only SQLite connection, indexed sequence queries and
revisit known open messages that update in place. `PRAGMA data_version` is only an
unchanged-pass hint on the same connection. Never use `immutable=1` for a live WAL
store or copy only the database file while WAL may contain committed data.

### Muse

Use native durable session log, workspace/worktree/branch facts and content activity.
`parentSessionId` may be a containing top-level session rather than spawning parent.
`notLoaded` is one host's view, not proof of global death.

### Processes, Git and GitHub

Use selective `sysinfo` refresh but preserve stronger OS birth/handle identity for
controls. CPU needs two samples and may exceed 100% across cores. Do not hash the
executable on every sample.

Use machine Git formats: `git worktree list --porcelain -z`,
`git status --porcelain=v2 --branch -z` and `git for-each-ref`. Observation never
changes checkout/index/remotes. Assigned branch, actual checkout, upstream, push
destination and independently observed remote ref remain distinct. Coalesce one
bounded remote-ref read per repository/ref through the existing GitHub owner.

## 8. Cursor and delivery patterns

Provider cursor and Store cursor are separate:

- provider cursor: JSONL byte boundary, SQLite sequence, inbox ID, native event
  cursor and source epoch;
- Store cursor: retained observation ID consumed by `monitor.snapshot/follow`.

For replay + live sources, subscribe before or together with replay, read bounded
history after source cursor, apply ID-addressed upserts, reach a watermark, then
commit normalized state + new cursor + coverage atomically. Cursor loss becomes an
explicit gap and bounded rescan.

Terminal Store deliveries use immutable exact envelopes. Live observations use one
replaceable checkpoint per source. Do not derive a universal journal from current
Command/Claude/OpenCode operation journals.

## 9. Current library integrations

| Library | Keep for | Do not add |
| --- | --- | --- |
| `rmcp` | MCP protocol/transport and tool routing | another framing layer or dispatch readiness gate |
| `agent-client-protocol` | selected Command ACP wire types | ACP wrappers around all harnesses or Core ACP version branches |
| `croner` | calendar parsing/evaluation | per-rule timers or another scheduler database |
| `atlas-redact` | redaction of complete supplied logical records | claims of cross-chunk security without a buffer |
| `tokio` / `rusqlite` / `serde` | async runtime, WAL Store and typed current protocol | another broker, ORM, EAV store or generated compatibility layer |
| `tracing` family | targeted diagnostics and dynamic filtering | second logging framework or universal business-event schema |
| `notify` | optional filesystem change hints | event-as-state assumptions; reconciliation remains required |

Current GitHub effects use `gh`. `octocrab` is optional only if a retained
requirement and measured code deletion justify replacement. Never add both clients.

## 10. Store and scheduling patterns

- `tokio::sync::watch` is latest-value wakeup, not an event log. Store facts/cursors
  remain durable; notify after commit.
- A queue is a current read projection. Do not tail Task observations into another
  pending queue.
- A lost/coalesced dispatch wake is harmless when the next tick queries current state.
- Disabling a dispatcher stops future admissions and does not cancel active work.
- Route condition, host capacity and workspace conflict are separate blockers.
- Helper execution identity is the manager on whose behalf Store admitted the action;
  editing config does not transfer ownership.

## 11. Integration discipline

A donor is integrated only when:

1. exact source/version and license class are recorded;
2. a current caller uses the mechanism;
3. replaced Eliot code is removed in the same connected slice;
4. total owned Rust + JS + glue falls or a named missing behavior is delivered;
5. retry, lifecycle and durable-state ownership remain singular;
6. unsupported cases/source limits are explicit;
7. the worker receives only the named mechanism, not the donor's architecture.

Do not vendor or wrap an entire donor to claim progress. Do not add a shared
abstraction for one implementation caller. Do not preserve an executor because a
historical record names it.
