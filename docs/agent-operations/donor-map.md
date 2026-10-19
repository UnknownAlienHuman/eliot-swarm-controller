# Donor Map

Use a donor only for a retained responsibility and remove the replaced Eliot code.
A source pattern is not a dependency, qualification result or reason to import the
donor's product. [Implementation](implementation.md) owns the adoption boundary.

## 1. Shared adapter boundary

### Paseo — semantic session port

Inspected source:
[`AgentClient` / `AgentSession`](https://github.com/getpaseo/paseo/blob/2a9e86b542326e62abea5c062bae3ff3c191c3bf/packages/server/src/server/agent/agent-sdk-types.ts).

Use as a **pattern**:

- small session interface;
- semantic start/steer/reply/interrupt/close/configure actions;
- optional capabilities separate from provider code;
- provider events translated at the driver boundary.

Do not copy Paseo UI, timeline, persistence, provider registry or plugin product.
Eliot retains Operations, binding/generation and result attribution.

### Waku — one ordered native owner

Inspected source:
[`opencode.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs).

Use as a **pattern**:

- one owner of mutable native state;
- closed command enum;
- serialized command/event handling;
- one idempotent settle point;
- stream break does not automatically mean session/process death.

Waku is GPL-3.0. Do not copy/link source without an explicit licensing decision. Do
not import its product or create another cross-adapter scheduler.

### Existing Eliot owners

| Existing code | Reuse |
| --- | --- |
| `swarm-process::module_owner` | Process membership, private bootstrap and exact OS custody. Simplify its plan; do not add another process framework. |
| `swarm-client::ModuleLink` | Authenticated Eliot IPC. Extend only after two routes delete the same delivery-loop duplication. |
| `swarm-contracts` | Current shared DTO owner. Replace string/vendor proof matrices with current typed semantic commands. |
| `swarm-process::permissions` | Atomic private durable files for an exact Store-delivery envelope/checkpoint. |

No new adapter-framework or journal crate is required.

## 2. Vendor harness mappings

| Harness | Native source | Eliot mapping |
| --- | --- | --- |
| Codex | Current app-server thread/turn/account protocol and [Goal API](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex) | Open, dispatch/send, reply, configure, Goal, cancel, recover/readback/result. Account usage and turn usage remain separate. |
| OpenCode | Current [v2 session contract](https://github.com/anomalyco/opencode/blob/afc68054c3b39cbc0d580a92184e00e2b8a97ae9/specs/v2/session.md) | Durable inbox identity, next-turn/steer, pending input, interruption, background, result and local state. Public Eliot loop-step/MCP phases are unnecessary. |
| Muse | MSP sessions/turns, Goal and subscription usage | Native Goal, queued/steered input, explicit recovery/cancel, local session log and point-in-time account usage. Select one executor. |
| Claude | Current Agent SDK session/query interfaces and official local session readers | Open/resume, send, reply/permission, configure, interrupt, result and local family observation. No reviewed native Goal. |
| Command ACP | Current selected `agent-client-protocol` generation | Prompt/reply/cancel/close translated inside Command adapter; ACP is not Eliot's universal bus. |
| Antigravity | Installed CLI warm stream | One serialized input/result window and exact output capture. Unsupported actions remain explicit. |

Harness Goal is default where supported. Eliot Goal remains an independent ordinary
message plugin.

## 3. Native effect and recovery lessons

### OpenCode

The current v2 contract provides:

- durable inbox admission before execution;
- idempotent reuse of the same inbox item ID within the same session/type;
- `resume` as scheduling, not durability;
- one process-global execution owner keyed by session ID;
- same-session resumes join; repeated wakes coalesce;
- write-ahead execution claims for bounded startup recovery;
- one logical Step with several physical Attempts;
- durable replay with subscribe-before-replay synchronization watermark.

Adopt native inbox/message identity and wake coalescing. Do not recreate OpenCode's
runner, history or retry scheduler in Eliot. One Eliot Operation remains one logical
input; physical provider attempts are observations.

### Codex

Current app-server account API distinguishes:

- `ordinary_usage_allowed: true|false|null`;
- one or several native rate-limit buckets;
- optional account identity;
- reset times and credits.

`null` means unavailable; clients must not infer recovery from percentages or reset
times. Preserve that rule. Eliot may open one probe after a transient retry/reset,
but the timestamp is not an `available` fact.

### Muse

Current MSP `SubscriptionUsage` is point-in-time. `observedAtMs` is the host arrival
stamp; render “as of” and never imply live data. Preserve native fields and source
cursor. Do not derive admission from a stale percentage.

Muse SDK `Connection` accepts caller command IDs; use native idempotency where
documented. Do not place the high-level SDK recovery coordinator beside Eliot
Operation readback.

### ACP / ACPX

Useful semantics:

- queue acceptance is not native prompt start;
- queued cancel differs from active-turn cancel;
- detach/viewer disconnect does not cancel admitted work;
- lost reply leaves the effect uncertain;
- watch replays retained history, emits a synchronization cut, then follows live;
- cursors are source-scoped and expired/foreign cursors fail explicitly.

ACPX is acceptable only as a complete optional backend that replaces the current ACP
owner/queue. Do not layer it beside the existing owner. Request IDs are not universal
idempotency keys. Do not import ACPX flows/workflow language or its Task product.

## 4. Local observation sources

<a id="local-observation-sources"></a>

### Codex

Use known thread/path identity and bounded incremental rollout reads. Plain and
compressed logs are representations of one rollout. A compressed seek can decode the
whole history; do not use it as a high-frequency poll. Ephemeral threads may have no
durable log.

### Claude

Official SDK readers show that metadata can be read from bounded head/tail chunks,
while full transcript reconstruction follows parent links and may parse the whole
JSONL. Page size does not guarantee bounded I/O. Use known changed files for frequent
observation; scoped inventory less often.

### OpenCode

Use one adapter-owned read-only SQLite connection, indexed sequences and revisit
known open messages that can update in place. `PRAGMA data_version` is an
unchanged-pass hint only on the same connection. Do not use `immutable=1` on a live
database or copy only the main DB while WAL may contain committed data.

### Muse

Use the durable session log, workspace/branch facts and content-activity events.
`parentSessionId` may mean containing top-level session, not spawning parent.
`notLoaded` is one host's view, not global death.

### Processes

[`sysinfo`](https://docs.rs/sysinfo/latest/sysinfo/) supplies selective CPU/RSS
refresh. Keep one `System`, refresh selected PIDs and enumerate discovery separately.
Retain exact OS birth/handle identity for control. Remove per-sample executable
hashing.

### Git and GitHub

Use existing bounded Git runner with machine formats:

- `git worktree list --porcelain -z`;
- `git status --porcelain=v2 --branch -z`;
- `git for-each-ref`.

Assigned branch, actual checkout, upstream, push target and remote GitHub head are
different facts. Observation changes none of them.

## 5. Queue and route policy lessons from Claude Swarm

The old scripts are behavior evidence, not code donors.

Useful lessons:

- prioritise returned corrections and unfinished work before fresh work;
- show `why_ranked`;
- one manager owns one worktree;
- explicit route/model/effort policy;
- 429/overload is transient; quota exhaustion/model disappearance may select an
  explicitly configured fallback;
- actual runner/process state is separate from the last log line;
- compact current Task context outperforms raw snapshot/digest dumps.

Do not copy:

- `SECTORS.json` as a second authority;
- infinite lane runners;
- marker files, lock directories or newest-log ownership;
- route disabling by editing files;
- automatic model substitution not selected by the manager;
- fixed age/rotation heuristics;
- another queue database.

In Eliot, a lane is a view over one automation policy, ordered route aliases, current
queue, provider/capacity facts and active bindings/workspaces.

## 6. Provider state and capacity

### Source scope

Normalize provider observations at the adapter:

- account scope only with stable native account identity;
- service scope only with explicit service identity;
- otherwise connection scope.

Store retains source key/epoch/sequence, observed time, freshness/coverage,
availability and native-labelled usage buckets. Native method names and auth hashes
do not enter the common DTO.

### Probe behavior

- no observation ever: one capacity-bounded probe is allowed;
- transient negative before retry/reset: hold;
- after retry/reset: one probe, not global recovery;
- hard negative: explicit fallback or config/source change;
- unknown/stale: automatic hold, explicit manager probe allowed.

The probe is not another entity. Its admitted root/open Operation is the in-flight
guard.

### Capacity

Use the Store writer transaction to count unresolved root opens and unreleased roots
and admit the next one. Do not maintain a second capacity lifecycle for every
command. Readback/control commands do not consume model capacity.

## 7. Scheduling patterns

OpenCode's execution coordinator demonstrates the right wake semantics: repeated
wakes coalesce, current durable state is read at drain, same-session work joins and
different sessions may proceed independently.

Use this pattern for Eliot's **in-process admission loop**, not OpenCode's execution
engine:

- durable schedule/occurrence in Store;
- wake notification plus nearest due timer;
- slow safety wake;
- bounded current-state reconciliation;
- ordinary idempotent Operation admission.

Do not create a scheduler process, service credential, process group, source digest
or RPC merely to calculate time. External effect workers remain separate only for
real external I/O/custody.

## 8. Process custody

### Paseo

Useful pattern from managed-process reconciliation:

- persisted exact ownership record;
- dead PID → delete record;
- PID identity mismatch → delete record, kill nothing;
- uninspectable process → keep record and retry;
- terminate only a positively matched owned leftover.

Use with Eliot's stronger binding/generation and OS birth identity. Do not copy
Paseo's JSON stores, daemon product or plugin lifecycle.

Paseo also treats workspace ID as durable ownership and never infers ownership from
cwd. Eliot should likewise use workspace lease identity; cwd is an observation.

### CCCC

`cccc-windows-process` provides a correct small suspended-child primitive: create
suspended, assign external ownership, resume; dropping before resume terminates and
reaps. Eliot's current owner-first topology already avoids that race, so importing it
now adds code without deleting a defect. Reconsider only for a proven child-first
launch boundary.

## 9. Current library integrations

| Library | Keep for | Do not add |
| --- | --- | --- |
| `rmcp` | MCP transport/tool routing | another framing layer or dispatch readiness gate |
| `agent-client-protocol` | selected Command ACP wire types | ACP wrappers around every harness |
| `croner` | cron parsing/evaluation | per-rule timers or another scheduler DB |
| `atlas-redact` | redaction of complete supplied records | security claims across arbitrary chunks |
| `tokio`, `rusqlite`, `serde` | runtime, WAL Store, typed protocol | broker, ORM, EAV store or generated compatibility family |
| `tracing` | diagnostics | universal business-event schema |
| `notify` | optional filesystem wake hints | treating an event as authoritative state |

`octocrab` is optional only if a retained requirement and net deletion justify
replacing `gh`. Never add both clients.

## 10. Integration discipline

A donor is integrated only when:

1. a current caller uses it;
2. replaced Eliot code is removed in the same slice;
3. total owned Rust+JS+glue decreases or retained behavior is materially clearer;
4. retry, lifecycle and durable-state ownership remain singular;
5. unsupported cases and source limitations remain explicit.

Do not vendor an application to claim integration. Do not extract a shared
abstraction for one caller. Do not preserve an old adapter because history names it.
