# Donor Map

Use a donor only for a retained responsibility and remove the replaced Eliot code.
A source pattern is not a dependency, qualification result or reason to import the
donor's product. [Implementation](implementation.md) owns the concrete adoption
boundary.

## 1. Adoption classes

Every donor use is one of three things:

| Class | Meaning |
| --- | --- |
| Maintained API/package | Call the donor's current supported interface directly. Pin only where reproducibility requires it. Do not wrap it with another private protocol. |
| Pattern | Reimplement the small mechanism independently from documented behavior. Copy no code. |
| Source reuse | Copy/link a named unit only after license/notices review, exact version selection and a deletion plan for the Eliot code it replaces. |

A donor is rejected when it introduces another Task store, scheduler, session owner,
acceptance authority, transcript mirror or compatibility matrix.

## 2. Shared adapter boundary

### Paseo — semantic session port, pattern only

Reviewed current source:
[`AgentClient` / `AgentSession`](https://github.com/getpaseo/paseo/blob/f7a0e2cbea875d44d36ac4801bfe5467fa17cdac/packages/server/src/server/agent/agent-sdk-types.ts).
Paseo is Apache-2.0 apart from separately licensed third-party components.

Useful mechanism:

- one semantic client/session interface;
- start turn, exact active-turn steer, reply, interrupt, close and configuration;
- optional capabilities declared separately from provider implementation;
- provider events converted at the driver boundary;
- out-of-band controls do not have to cancel the foreground turn.

Use the shape, not the product. Do not copy Paseo's daemon, UI timeline, provider
registry, plugin lifecycle, persistence, catalog or broad feature surface. Eliot
retains Store Operations, binding/generation and result attribution.

### Waku — one ordered command/event owner, pattern only

Reviewed source:
[`driver/mod.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/mod.rs)
and
[`driver/opencode.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs).
Waku is GPL-3.0; do not copy or link its implementation into Eliot without a separate
license decision. It is a behavior-pattern donor only.

Useful mechanism:

- one worker owns mutable native session state;
- a closed command enum (`Prompt`, `Steer`, `Cancel`, `Respond`, options,
  `Shutdown`);
- commands and provider events are serialized;
- one idempotent turn settle point;
- a stream break is not automatically process/session death;
- event payload and wake notification are separate, so wakes may coalesce without
  dropping events.

Waku's current provider drivers are themselves large. Do not import them or infer
that a driver file per provider is automatically compact architecture.

### ACPX — exact shared owner semantics, optional backend or pattern

Reviewed:
[`shared-sessions.md`](https://github.com/openclaw/acpx/blob/8e396609238086dee6a407fdb3b3ac46dbdedd70/docs/shared-sessions.md).

Useful mechanism:

- exact `(agent command, cwd, name)` scope selects one session record and queue owner;
- missing owner does not create a competing connection for shared controls;
- queue acceptance differs from native `promptStarted` admission;
- targeted cancellation distinguishes a queued request from the active session turn;
- detaching a viewer/client does not cancel admitted work;
- a lost response remains uncertain and is inspected, not automatically retried.

Important exclusion: ACPX request IDs are queue/status identity, explicitly not an
idempotent replay API. Adopt ACPX only as a whole optional ACP backend that replaces
our equivalent ACP owner/queue code. Otherwise use these semantics as a pattern; do
not layer ACPX beside the current ACP adapter.

### Existing Eliot owners

The smallest reusable implementation already has owners:

| Existing code | Reuse |
| --- | --- |
| [swarm-process module owner](../../crates/swarm-process/src/module_owner.rs) | Verify supervisor-owned adapter process, scope and private state; add one common bootstrap reader. |
| [swarm-process private files](../../crates/swarm-process/src/permissions.rs) | Atomic private create/replace/read/remove and durability barriers. Use for a small Store-delivery outbox and bounded adapter effect records; do not add an adapter database. |
| [swarm-client ModuleLink](../../crates/swarm-client/src/module_link.rs) | Authenticated hello, `module.next`, outcome/observation/result delivery; add one exact `DeliveryOutbox`. |
| [swarm-contracts](../../crates/swarm-contracts/src/lib.rs) | Replace string/JSON commands and vendor proof/profile matrices with current typed semantic actions. |

No new adapter-framework crate is needed.

## 3. Native identity and replay donors

Prefer the native harness's durable identity and readback over Eliot-side replay
machinery.

| Harness | Reviewed mechanism | Eliot rule |
| --- | --- | --- |
| Muse | MSP sessions/turns, UUIDv7 command identity, durable cursor-bearing event log and native Goal | Persist the exact command ID before send. Replay only the same ID and identical params when the MSP contract allows it. ACK is admission, not terminal. |
| OpenCode | [v2 session contract](https://github.com/anomalyco/opencode/blob/da9d436c36e0145110531ffcd7b15e50016fdd82/specs/v2/session.md): durable inbox row precedes execution; matching inbox item ID is idempotent | Derive/retain one stable native item ID per Operation. Reconcile through inbox/projected message/session log before retry. The global event stream is live observation, not durable recovery. |
| Codex | Current app-server [turn protocol](https://github.com/openai/codex/blob/8c076942cb70fba8e5c4732c23f1faabc4aed484/codex-rs/app-server-protocol/src/protocol/v2/turn.rs) and [Goal processor](https://github.com/openai/codex/blob/8c076942cb70fba8e5c4732c23f1faabc4aed484/codex-rs/app-server/src/request_processors/thread_goal_processor.rs) | Use `clientUserMessageId`, exact thread/turn IDs, `expectedTurnId` steer and thread/turn history where documented. A JSON-RPC request ID is connection-scoped, not durable dedupe. |
| Claude | Current Agent SDK session/resume and permission interfaces | Reuse SDK session identity and official local history. No reviewed universal durable mutation key: serialize per session and retain only the smallest prepared effect when readback cannot classify it. |
| ACP / ACPX | Queue request identity, prompt-start admission, retained watch/result | Never equate request ID with idempotent retry. Unknown submission remains unknown. |
| Command batch / Antigravity | Process/run identity and captured output | Admit one effect window, retain exact local intent/output, and do not restart the process to guess whether input ran. |

A donor's retry rule is not generalized to other harnesses. The common layer delivers
already-produced Eliot records only.

## 4. Vendor harness mappings

These capabilities do not require every adapter to expose the same feature.

| Harness | Eliot mapping |
| --- | --- |
| Codex | `Open`, `Dispatch`, `Send`, `Reply`, `Configure`, native `Goal`, `Cancel`, `Recover`, `Refresh`, `Result`, `Reconcile`; account-rate-limit snapshots remain separate from turn usage. |
| OpenCode | Durable prompt/inbox delivery, steering, permission/form reply, interrupt, background and local state/result reads. Public `native.opencode.loop_step` and `native.mcp.*` are unnecessary. |
| Muse | Native `Goal`, queued/steered input, explicit recovery and turn/queued-input controls, local session log and account usage. Select one Node/Rust executor. |
| Claude | Open/resume, send, reply/permission, configure, interrupt, result and local session/subagent readers. Reviewed interface has no native Goal, so Eliot Goal remains available. |
| Command ACP | Open, dispatch/send, permission reply, turn cancel and session close through the existing `agent-client-protocol` crate. ACP is an adapter protocol, not Eliot's universal bus. |
| Antigravity | One sequential input/result window. Unsupported reply/configure/Goal remains explicit. Readiness/model facts are observations, not shared proof packets. |

Harness Goal is the default where supported. Eliot Goal remains an independent
ordinary-message plugin on every adapter with suitable `Send` support.

## 5. Claude-script experience as a donor

The old scripts are field evidence, not code to port. Preserve these mechanisms:

| Script behavior | Product mechanism |
| --- | --- |
| `make_task.py` produced compact current `TASK.md` instead of a 95-KB Issue thread | One manager-created slice packet with objective, symbols, constraints, deletion list and check command. |
| `Agent-Watch.py`, `oc_busy.py` and vendor logs joined several local facts | One passive projection that keeps activity, connection, process, assignment and source freshness separate. |
| `issue_settled.py` omitted delivered/parked/review-waiting work until inputs changed | Do not keep waking agents whose work has no new actionable input. |
| `state.py` retained refuted claims | Preserve verified/refuted findings so another agent does not repeat the same investigation. |
| `pr_evidence.py` and delivery queues bound work to exact candidates | Store Operation + candidate-specific evidence + durable acknowledgment, not flag files. |
| manager branches and microtasks | One worktree per manager; managers integrate; weak workers receive narrow leaf work. |
| resource and rotation scripts | Capacity and source facts, not agent count or age alone, drive scheduling/recovery. |

Do not port the scripts as wrappers, compatibility daemons, marker files or a generic
workflow language. Their knowledge becomes current adapter fixtures, Store rules and
slice instructions. Policy changes are versioned current work, not broadcast
addenda.

<a id="local-observation-sources"></a>

## 6. Local observation donors

### Codex

Use native thread identity, `Thread/GitInfo`, bounded rollout JSONL reads and the
append-only session index. Plain and `.jsonl.zst` are two representations of one
logical rollout. A compressed historical read may decode the whole file; do not run
it as a high-frequency poll. Session-index mtime is not work activity. Ephemeral
threads may have no durable log.

### Claude

The official SDK's session listing can use bounded head/tail metadata reads, while
transcript reconstruction parses full JSONL and parent links. Page size therefore
does not bound underlying I/O. Observe known changed files frequently and run scoped
inventory less often. Reuse existing SDK summaries when maintained; do not add a
transcript mirror solely for monitoring.

### OpenCode

Use one adapter-owned read-only SQLite connection, query indexed new sequences and
revisit known open messages that may update in place. `PRAGMA data_version` is an
unchanged-pass hint on the same connection, not a durable cursor or activity fact.
Use read-only URI mode; never `immutable=1` for a live WAL database or copy only the
`.db` file while committed WAL data exists.

### Muse

Use the native durable session log, workspace/worktree/branch facts and content
activity events. `parentSessionId` may be a containing top-level session rather than
the spawning parent. `notLoaded` is one host's view, not proof of global death.

### Processes, Git and GitHub

Use selective `sysinfo` refresh but preserve Eliot's stronger OS birth/handle
identity for controls. CPU needs two samples and may exceed 100% across cores. Do not
hash executable bytes on every sample.

Use machine Git formats: `git worktree list --porcelain -z`,
`git status --porcelain=v2 --branch -z` and `git for-each-ref`. Observation never
changes checkout, index or remote config. Assigned branch, actual checkout, upstream,
push destination and independently observed remote GitHub ref are separate facts.
Coalesce one bounded remote-ref read per repository/ref through the existing `gh`
owner; do not require publication or a PR.

## 7. Current library integrations

| Library | Keep for | Do not add |
| --- | --- | --- |
| `rmcp` | MCP protocol/transport and tool routing | another MCP framing layer or readiness gate before dispatch |
| `agent-client-protocol` | Command ACP wire types | ACP wrappers around every other harness |
| `croner` | calendar parsing/evaluation | per-rule timers or another scheduler database |
| `atlas-redact` | redaction of complete supplied logical records | claims of cross-chunk protection without a buffer |
| `tokio` / `rusqlite` / `serde` | async runtime, WAL Store and typed current protocol | another broker, ORM, EAV store or generated compatibility layer |
| `tracing` family | targeted diagnostics and dynamic filtering | second logging framework or universal business-event schema |
| `notify` | optional filesystem change hints | event-as-state assumptions; periodic reconciliation remains required |

Current GitHub effects use `gh`. `octocrab` is optional only if a retained requirement
and measured code deletion justify replacement. Never add both clients.

## 8. Store and scheduling patterns

- `tokio::sync::watch` is a latest-value wakeup, not an event log. Store observations
  and cursors remain durable; notify after commit.
- Temporal/Windmill provide only a pattern for schedule vs admitted run,
  overlap/catch-up and execution identity. Do not import either service, database or
  workflow language.
- Disabling a schedule stops future admissions; it does not cancel started work.
- Helper execution identity is the current manager on whose behalf Store admitted
  the action; editing config does not silently transfer ownership.

## 9. Integration discipline

A donor is integrated only when:

1. the exact source/version and license class are recorded;
2. a current caller uses it;
3. the replaced Eliot mechanism is removed in the same connected slice;
4. total owned Rust + JS + glue falls or a named missing behavior is delivered;
5. retry, lifecycle and durable-state ownership remain singular;
6. unsupported cases and source limits are explicit;
7. the worker receives only the named mechanism, not the donor's whole architecture.

Do not vendor or wrap an entire donor to claim progress. Do not add a shared
abstraction for one implementation caller. Do not preserve an old adapter because a
historical record names it.
