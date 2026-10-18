# Donor Map

Use a donor only for a retained responsibility and remove the replaced Eliot code.
A source pattern is not a dependency, qualification result or reason to import the
donor's product. [Implementation](implementation.md) owns the concrete adoption
boundary.

## 1. Shared adapter boundary

### Paseo — semantic session port

Inspected source:
[`AgentClient` / `AgentSession`](https://github.com/getpaseo/paseo/blob/2a9e86b542326e62abea5c062bae3ff3c191c3bf/packages/server/src/server/agent/agent-sdk-types.ts).

Useful mechanism:

- one small client/session interface;
- semantic methods such as start turn, steer, reply, interrupt, close and configure;
- optional capabilities declared separately from provider implementation;
- provider-specific events converted at the driver boundary.

Use this shape for Eliot's typed `AdapterCommand` and capability profile. Do not copy
Paseo's UI/timeline, persistence, provider implementations or broad feature flags.
Eliot retains Store Operations, binding/generation and result attribution.

### Waku — one ordered command/event owner

Inspected source:
[`opencode.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs).

Useful mechanism:

- one worker owns mutable native session state;
- a closed command enum (`Prompt`, `Steer`, `Cancel`, `Respond`, options,
  `Shutdown`);
- command and event handling are serialized;
- one idempotent turn settle point;
- a stream break is not automatically process/session death.

Apply inside each adapter. Do not replace Eliot Store with Waku state or use the
pattern to create another cross-adapter scheduler.

### Existing Eliot owners

The smallest reusable shell already has owners:

| Existing code | Reuse |
| --- | --- |
| [swarm-process module owner](../../crates/swarm-process/src/module_owner.rs) | Verify supervisor-owned adapter process, scope and private state. Extend with one common bootstrap reader. |
| [swarm-client ModuleLink](../../crates/swarm-client/src/module_link.rs) | Authenticated hello, `module.next`, outcome/observation/result delivery. Extend with one adapter loop after two Rust adapters share it. |
| [swarm-contracts](../../crates/swarm-contracts/src/lib.rs) | Current typed semantic command/outcome/capability types. Remove vendor proof structs and historical schema/version ladders. |

No new adapter-framework crate is needed.

## 2. Vendor harness mappings

These are current native capabilities, not requirements that every adapter expose the
same feature.

| Harness | Native source | Eliot mapping |
| --- | --- | --- |
| Codex | App-server thread/turn protocol and [Goal API](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex); protocol types under [`thread.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs) and [`account.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/account.rs) | `Open`, `Dispatch`, `Send`, `Reply`, `Configure`, native `Goal`, `Cancel`, `Refresh`, `Result`, `Reconcile`; account-rate-limit snapshots remain separate from turn usage. |
| OpenCode | Current [v2 API](https://dev.opencode.ai/v2/docs/api) and session SQLite schema [`sql.ts`](https://github.com/anomalyco/opencode/blob/dev/packages/core/src/session/sql.ts) | Prompt/inbox delivery, steering, permission/form reply, interruption, background and local state/result reads. Public `native.opencode.loop_step` and `native.mcp.*` are unnecessary. |
| Muse | MSP [sessions and turns](https://meta-models.github.io/muse-code-sdk/next/guides/msp-concepts/sessions-and-turns/), [Goal set](https://meta-models.github.io/muse-code-sdk/next/generated/msp/methods/goal-set/), [usage](https://meta-models.github.io/muse-code-sdk/next/generated/msp/types/subscriptionusage/) | Native `Goal`, queued/steered input, explicit turn/queued-input controls, local session log and account usage. Select one Node/Rust executor. |
| Claude | Published Agent SDK session/query types and [session documentation](https://code.claude.com/docs/en/agent-sdk/sessions) | Open/resume, send, reply/permission, configure, interrupt, result and local session/subagent readers. Reviewed interface has no native Goal, so Eliot Goal remains available. |
| Command ACP | [ACP prompt lifecycle](https://agentclientprotocol.com/protocol/v1/prompt-turn) through the existing `agent-client-protocol` crate | Open, dispatch/send, permission reply, turn cancel and session close. ACP is an adapter protocol, not Eliot's universal bus. |
| Antigravity | Installed CLI warm stream | One sequential input/result window. Unsupported reply/configure/Goal remains explicit. Native readiness/model facts are observations, not shared proof packets. |

Native Goal is the default where supported. Eliot Goal remains an independent ordinary-message plugin on every adapter with suitable `Send` support.

## 3. Local observation donors

### Codex

Primary sources:

- [`Thread` / `GitInfo`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread_data.rs);
- rollout [line/compression reader](https://github.com/openai/codex/blob/main/codex-rs/rollout/src/compression.rs);
- bounded [prefix/seek reader](https://github.com/openai/codex/blob/main/codex-rs/rollout/src/seekable_reader.rs);
- append-only [session index](https://github.com/openai/codex/blob/main/codex-rs/rollout/src/session_index.rs).

Use known thread/path identity and bounded incremental JSONL reads. Plain and
`.jsonl.zst` are two representations of one logical rollout. A compressed seekable
history read can decode the whole file; do not use it as a high-frequency live poll.
Session-index rename time is not work activity. Ephemeral threads have no durable log.

### Claude

The official Python SDK
[`sessions.py`](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/_internal/sessions.py)
shows:

- session metadata can be read from bounded head/tail chunks;
- transcript reconstruction parses the whole JSONL and follows parent links;
- worktree enumeration is separate;
- subagent files and parent sidecars are separate sources;
- list pagination can happen after per-session reads.

Therefore page size does not guarantee bounded underlying I/O. Use known changed
files for frequent observation; run scoped inventory less often. Reuse SessionStore
summaries only when the selected SDK path already maintains them. Do not add a
transcript mirror solely for monitoring.

### OpenCode

The current SQLite schema has indexed session/message sequence identity. Use one
adapter-owned read-only connection, query new sequences and revisit known open
message IDs that can update in place. SQLite
[`PRAGMA data_version`](https://sqlite.org/pragma.html#pragma_data_version) is only an
unchanged-pass hint on the same connection, not a durable cursor or activity fact.
Use real [read-only URI mode](https://sqlite.org/uri.html); do not use `immutable=1`
for a live database or copy only the `.db` while WAL contains committed data.

### Muse

Use the native session's durable log path, workspace/worktree/branch facts and
content-activity events where supplied. `parentSessionId` may identify a containing
top-level session rather than the spawning parent. `notLoaded` is one host's view,
not proof of global death.

### Processes

[`sysinfo`](https://docs.rs/sysinfo/latest/sysinfo/) supplies selective process
refresh and cross-platform CPU/RSS facts. Use only the `system` feature with default
features disabled. Keep one `System` instance; refresh selected PIDs and enumerate
for discovery separately. `ProcessRefreshKind::nothing()` includes Linux tasks, so
use `without_tasks` when thread inventory is unnecessary.

`sysinfo` start time is not Eliot's full PID-reuse identity. Retain the existing
OS birth/handle identity for controls. CPU requires two samples and may exceed 100%
across cores. Remove the current per-sample executable SHA checks; executable bytes
are verified at launch/attachment, not every metric sample.

### Git and GitHub

Use the existing bounded Git runner and machine formats:

- [`git worktree list --porcelain -z`](https://git-scm.com/docs/git-worktree);
- [`git status --porcelain=v2 --branch -z`](https://git-scm.com/docs/git-status);
- [`git for-each-ref`](https://git-scm.com/docs/git-for-each-ref) for upstream/push
  targets.

Observation never changes checkout, index or remote config. Assigned branch, actual
checkout, upstream, push destination and remote GitHub ref are different facts.
Coalesce one bounded remote-ref read per repository/ref through the existing `gh`
owner; keep source time/error. Do not require publication or a PR.

## 4. Current library integrations

| Library | Keep for | Do not add |
| --- | --- | --- |
| `rmcp` | MCP protocol/transport and tool routing | another MCP framing layer or readiness gate before dispatch |
| `agent-client-protocol` | Command ACP wire types | ACP wrappers around every other harness |
| `croner` | calendar parsing/evaluation | per-rule timers or another scheduler database |
| `atlas-redact` | redaction of complete supplied logical records | claims of cross-chunk protection without a buffer |
| `tokio` / `rusqlite` / `serde` | async runtime, WAL Store, typed current protocol | another broker, ORM, EAV store or generated compatibility layer |
| `tracing` family | target structured diagnostics and dynamic filtering | a second logging framework or a universal business-event schema |
| `notify` | optional filesystem change hints | event-as-state assumptions; periodic reconciliation remains required |

### GitHub client

Current code uses `gh`. `octocrab` is optional only if a retained requirement and net
code reduction justify replacement. A transport swap must delete `run_gh_api` and
keep one effect/readback owner. Never add both clients.

## 5. Store and scheduling patterns

- `tokio::sync::watch` is a latest-value wakeup, not an event log. Store observations
  and cursors remain durable; notify after commit.
- Temporal/Windmill patterns are useful only for the distinction between schedule,
  admitted run, overlap/catch-up and execution identity. Do not import either service,
  their databases or workflow languages.
- One disabled schedule stops future admissions; it does not cancel already started
  work. Eliot uses latest-only catch-up unless a concrete helper requires otherwise.
- Helper execution identity is the current manager on whose behalf the admitted
  action runs; editing a script/config does not silently transfer ownership.

## 6. Contract and receipt simplification

Do not copy donor protocol/version matrices. Eliot protocol 2 uses current typed
commands and a small capability profile. The package descriptor is installation and
launch metadata. It is not a second authorization system.

Outcome trust comes from the authenticated `ModuleLink`, retained binding/generation
and Store Operation. Keep content hashes on actual immutable artifact bytes. Do not
repeat module/artifact/version/protocol and request hashes in every receipt or create
vendor-specific readiness proof structs in shared contracts.

## 7. Integration discipline

A donor is integrated only when:

1. a current caller uses it;
2. the replaced Eliot mechanism is removed;
3. total owned Rust + JS + glue is reduced or the retained behavior is materially
   clearer;
4. retry, lifecycle and durable-state ownership remain singular;
5. source limits and unsupported cases are documented honestly.

Do not vendor or wrap an entire donor to claim integration. Do not add a shared
abstraction for one caller. Do not preserve an old adapter because a historical
record names it.
