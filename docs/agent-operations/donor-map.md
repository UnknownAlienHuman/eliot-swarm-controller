# Donor Map

Use a donor only for a retained responsibility and delete the replaced Eliot code in
the same slice. A pattern is not a dependency or qualification result.
[Implementation](implementation.md) owns the adoption boundary.

## 1. Shared adapter boundary

### Paseo — semantic session port

Inspected:
[`AgentClient` / `AgentSession`](https://github.com/getpaseo/paseo/blob/2a9e86b542326e62abea5c062bae3ff3c191c3bf/packages/server/src/server/agent/agent-sdk-types.ts).

Use as a pattern:

- small session interface;
- semantic start/steer/reply/interrupt/close/configure actions;
- optional capabilities separate from provider code;
- provider events translated at the driver boundary.

Do not copy Paseo UI, timeline, persistence, provider registry or plugin product.

### Waku — one ordered native owner

Inspected:
[`opencode.rs`](https://github.com/egoist/waku/blob/10bcd728c9b2f07d664cfece3c8c870d3418ddd2/crates/waku-core/src/driver/opencode.rs).

Use as a pattern:

- one owner of mutable native state;
- closed command enum;
- serialized command/event handling;
- one settle point;
- stream break is not session/process death.

Waku is GPL-3.0. Do not copy/link source without an explicit licensing decision. Do
not import its product or create another scheduler.

### Existing Eliot owners

| Code | Reuse |
| --- | --- |
| `swarm-process::module_owner` | Process membership, private bootstrap and exact OS custody. Simplify its plan; do not add a process framework. |
| `swarm-client::ModuleLink` | Authenticated Eliot IPC. Extend only after two routes delete identical delivery-loop duplication. |
| `swarm-contracts` | Current shared DTO owner. Replace vendor/string proof matrices with typed semantic commands. |
| `swarm-process::permissions` | Atomic private files for an exact Store-delivery envelope/checkpoint. |

No new adapter-framework or universal journal crate is required.

## 2. Vendor harness mappings

| Harness | Native source | Eliot mapping |
| --- | --- | --- |
| Codex | Current app-server thread/turn/account protocol and [Goal API](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex) | Open, dispatch/send, reply, configure, Goal, cancel, recover/readback/result. Account limits and turn usage remain separate. |
| OpenCode | Current [v2 session contract](https://github.com/anomalyco/opencode/blob/afc68054c3b39cbc0d580a92184e00e2b8a97ae9/specs/v2/session.md) | Durable inbox identity, next-turn/steer, pending input, interruption, background, result and local state. Public Eliot loop-step/MCP phases are unnecessary. |
| Muse | MSP sessions/turns, Goal and subscription usage | Native Goal, queued/steered input, explicit recovery/cancel, local session log and point-in-time usage. Select one executor. |
| Claude | Current Agent SDK session/query interfaces and official local session readers | Open/resume, send, reply/permission, configure, interrupt, result and family observation. No reviewed native Goal. |
| Command ACP | Current selected `agent-client-protocol` generation | Prompt/reply/cancel/close translated inside Command; ACP is not Eliot's universal bus. |
| Antigravity | Installed CLI warm stream | One serialized input/result window and exact output capture. Unsupported actions stay explicit. |

Harness Goal is default where supported. Eliot Goal remains an independent ordinary
message plugin.

## 3. Native effect and recovery lessons

### OpenCode

Useful native mechanisms:

- durable inbox admission before execution;
- idempotent reuse of one inbox item ID within its native scope;
- one process-global execution owner per Session ID;
- same-session resumes join and repeated wakes coalesce;
- write-ahead execution claims for bounded startup recovery;
- one logical Step with several physical Attempts;
- replay plus live synchronization watermark.

Adopt native inbox/message identity and wake coalescing. Do not recreate OpenCode's
runner, history or retry scheduler. One Eliot Operation remains one logical input;
physical attempts are observations.

### Codex

Current account API distinguishes `ordinary_usage_allowed: true|false|null`, native
rate-limit buckets, optional account identity, reset times and credits. `null` means
unavailable. A reset time may open one bounded probe; it is not an `available` fact.

### Muse

`SubscriptionUsage` is point-in-time. Render “as of”; never imply live data. Preserve
native fields/source cursor. SDK `Connection` accepts caller command IDs; use native
idempotency where documented. Do not import its high-level recovery coordinator beside
Eliot Operation readback.

### ACP / ACPX

Useful semantics:

- queue acceptance is not native prompt start;
- queued cancel differs from active-turn cancel;
- viewer detach does not cancel work;
- lost reply leaves effect uncertain;
- watch replays retained history, emits a synchronization cut, then follows live;
- cursors are source-scoped and expired/foreign cursors fail explicitly.

ACPX is acceptable only as a complete optional backend replacing the current ACP
owner/queue. Do not layer it beside the existing owner. Request IDs are not universal
idempotency keys. Do not import ACPX flows, Task product or storage.

## 4. Local observation sources

<a id="local-observation-sources"></a>

### Codex

Use known thread/path identity and bounded incremental rollout reads. Plain and
compressed logs are representations of one rollout; compressed history may decode the
whole file and is unsuitable for high-frequency polling. Ephemeral threads may have no
durable log.

### Claude

Official readers show bounded metadata head/tail reads, but full transcript
reconstruction follows parents and may parse whole JSONL. Page size does not guarantee
bounded I/O. Use known changed files frequently and scoped inventory less often.

### OpenCode

Use one adapter-owned read-only SQLite connection, indexed sequences and revisit known
open messages that update in place. `PRAGMA data_version` is an unchanged-pass hint
only on the same connection. Do not use `immutable=1` on a live DB or copy only the
main file while WAL may contain committed data.

### Muse

Use durable session log, workspace/branch facts and content activity. `parentSessionId`
may mean containing top-level session rather than spawning parent. `notLoaded` is one
host's view, not global death.

### Processes

[`sysinfo`](https://docs.rs/sysinfo/latest/sysinfo/) provides selective CPU/RSS
refresh. Keep one `System`, refresh selected PIDs and enumerate discovery separately.
Retain exact OS birth/handle identity for control. Remove per-sample executable hash.

### Git and GitHub

Use the existing bounded Git runner with machine formats:

- `git worktree list --porcelain -z`;
- `git status --porcelain=v2 --branch -z`;
- `git for-each-ref`.

Assigned branch, actual checkout, upstream, push target and remote GitHub head are
separate facts. Observation changes none of them.

## 5. Queue, prompt and correction lessons from Claude Swarm

Old scripts are behavior evidence, not code donors.

Keep:

- corrections and unfinished work before fresh work;
- deterministic `why_ranked`;
- one manager per worktree;
- explicit route/model/effort policy;
- actual process/native state separate from log age;
- one compact current Task card;
- complete findings returned in one correction round;
- bounded no-progress escalation.

Do not copy:

- `SECTORS.json` as authority;
- infinite lane runners;
- marker files, lock directories or newest-log ownership;
- route disabling by editing files;
- unselected model fallback;
- fixed age/rotation heuristics;
- raw Store/proof JSON in model context;
- per-finding correction loops;
- another queue database.

A lane remains a view over one policy, ordered route aliases, current queue,
provider/capacity facts and active bindings/workspaces. A model-visible `TaskPacket`
contains only current actionable work; Store identity remains outside the prompt.

## 6. Provider state and capacity

### Source scope

Normalize at the adapter:

- account scope only with stable account identity;
- service scope only with explicit service identity;
- otherwise connection scope.

Store retains source key/epoch/sequence, observed time, freshness/coverage,
availability and native-labelled usage. Native method names and auth hashes do not
enter the common DTO.

### Probe behavior

- no observation: one capacity-bounded probe;
- transient negative before retry/reset: hold;
- after retry/reset: one probe, not global recovery;
- hard negative: explicit fallback or config/source change;
- unknown/stale: automation holds; explicit manager probe remains possible.

The admitted root/open Operation is the in-flight guard. No extra probe entity.

### Capacity

Use one Store transaction to count unresolved root opens/unreleased roots and admit
the next. Do not mirror every command lifecycle. Readback/control commands consume no
model capacity.

## 7. Scheduling and current-state automation

OpenCode's coordinator demonstrates the useful wake pattern: repeated wakes coalesce,
current durable state is read when draining, same-session work joins, different
sessions can proceed independently.

Use this for Eliot admission—not OpenCode execution:

- durable schedule/occurrence in Store;
- wake notification plus nearest due timer;
- slow safety wake;
- bounded current-state query;
- ordinary idempotent Operation admission.

Review/return-changes/accept follow the same pattern: query current candidate state
and invoke the normal Store transition. Do not copy a candidate into another workflow
journal.

## 8. Process custody

### Paseo

Useful pattern:

- persisted exact owner record;
- dead PID → delete record;
- PID identity mismatch → delete record, kill nothing;
- uninspectable process → keep unknown and retry;
- terminate only positively matched owned leftover.

Use with Eliot binding/generation and OS birth identity. Do not copy Paseo JSON stores,
daemon or plugin lifecycle. Paseo's workspace ID reinforces the same rule: cwd is an
observation, not ownership.

### CCCC

`cccc-windows-process` provides create-suspended → assign external ownership → resume;
drop before resume terminates/reaps. Eliot owner-first topology already avoids that
race, so importing it now adds code without deleting a defect. Reconsider only for a
proven child-first boundary.

## 9. SQLite schema evolution

Primary SQLite mechanisms:

- [`application_id` and file identity](https://www.sqlite.org/fileformat2.html#the_database_header);
- application-owned [`user_version`](https://www.sqlite.org/pragma.html#pragma_user_version);
- atomic [`BEGIN IMMEDIATE`](https://www.sqlite.org/lang_transaction.html#deferred_immediate_and_exclusive_transactions);
- [`foreign_key_check`](https://www.sqlite.org/pragma.html#pragma_foreign_key_check)
  and [`quick_check`](https://www.sqlite.org/pragma.html#pragma_quick_check) for
  bounded integrity checks.

Use these directly with existing `rusqlite`; no migration library is required.

Target pattern:

```text
application_id identifies Eliot
user_version + schema_migrations identify current version
BEGIN IMMEDIATE
  inspect current version
  run ordered missing SQL
  run explicit migration postconditions
  record version
COMMIT
```

Do not compare exact bundled SQL text on every open, build a reference DB, tokenize
DDL or create one Rust installer module per migration. Bridge only concrete current
legacy marker states, then delete the bridge.

## 10. Public API routing discipline

No external router/policy framework is needed. Existing Rust enums and domain modules
are sufficient.

Retained pattern:

```text
central transport selects public | internal-host | adapter protocol
public domain parses typed request
public domain authorizes exact object/revision
public domain executes read/mutation
```

MCP tool exposure is a frontend concern; Store object authorization remains domain
logic. Do not combine both into a generated policy language. Normalize Operation
scope so generic visibility does not branch on every historical method name.

## 11. Current library integrations

| Library | Keep for | Do not add |
| --- | --- | --- |
| `rmcp` | MCP transport/tool routing | another framing layer or dispatch readiness gate |
| `agent-client-protocol` | selected Command ACP wire types | ACP wrappers around every harness |
| `croner` | cron parsing/evaluation | per-rule timers or scheduler DB |
| `atlas-redact` | redaction of complete supplied records | claims across arbitrary chunks |
| `tokio`, `rusqlite`, `serde` | runtime, WAL Store, typed protocol | broker, ORM, EAV or compatibility generator |
| `tracing` | diagnostics | universal business-event schema |
| `notify` | optional wake hints | event-as-authoritative-state |

`octocrab` is optional only if a retained requirement and net deletion justify
replacing `gh`. Never keep both.

## 12. Integration discipline

A donor is integrated only when:

1. a current caller uses it;
2. replaced Eliot code is removed in the same slice;
3. total owned Rust+JS+glue decreases or retained behavior is materially clearer;
4. retry, lifecycle and durable-state ownership remain singular;
5. unsupported cases and source limits stay explicit.

Do not vendor an application to claim integration. Do not extract for one caller. Do
not preserve an old adapter because history names it.
