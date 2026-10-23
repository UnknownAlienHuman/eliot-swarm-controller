# Module API

Target Store/harness boundary. [Architecture](architecture.md) owns purpose,
[implementation](implementation.md) owns source changes,
[delivery](delivery.md) owns the autonomous cycle, and the
[audit register](audit-register.md) records source evidence. Rust types in
`swarm-contracts` own field bounds; do not duplicate schemas here.

## Semantic actions

Governor, Managers and enabled helpers call the same Store handlers. Store authorizes
current actor/object scope, retains an Operation and delivers a typed action. An
adapter has binding-scoped credentials, never Manager authority over Tasks.

| Store method | Adapter action | Meaning |
| --- | --- | --- |
| `agent.open` | `Open` | Open, resume or attach selected native session; no silent replacement. |
| `task.dispatch` | `Dispatch` | First controller-owned logical input of an Attempt. |
| `agent.send` | `Send(next_turn | steer)` | Follow-up input with declared timing/target semantics. |
| `agent.reply` | `Reply` | Answer one exact pending question/form/permission. |
| `agent.configure` | `Configure` | Apply supported model/mode/effort/options; report effective values. |
| `agent.goal` | `Goal` | Native set/edit/pause/resume/clear where supported. |
| `agent.background` | `Background` | Background eligible native work. |
| `agent.cancel` | `Cancel(turn | queued_input)` | Cancel exact native scope; unsent Store work uses `operation.cancel`. |
| `agent.recover` | `Recover` | Resume known session after supported checks, never replay input. |
| `agent.refresh` | `Refresh` | Explicit safe readback, not a prerequisite for local state. |
| `agent.result` | `Result` | Read attributable output through bounded normalized pages. |
| `agent.reconcile` | `Reconcile` | Read back one uncertain action, never repeat it. |
| `agent.shutdown` | `Shutdown` | Release exact owned resource, not external shared service. |

Preserve `Attempt.start_owner`: native-manager delegation reserves before spawn;
controller Dispatch must not race the initial input before producer registration.

Remove public vendor/internal methods during route migration:

- `native.opencode.loop_step` → only an actually supported Send timing;
- `native.command.cancel_turn` / `close_session` → semantic Cancel / Shutdown;
- `native.command.release_evidence` → local source cleanup after Store coverage;
- `native.mcp.*` → OpenCode-internal setup/observation.

Do not rename a weaker inbox/loop-boundary action as exact-turn steer. Unsupported scope
fails before native I/O.

## Capabilities, hello and native owner

Capabilities describe supported actions, input timing, cancel scopes, Goal verbs,
background control and local activity/result/quota sources. They route actions; Store
grants rights. Hello checks authenticated binding, current protocol major and required
capability subset. Package/build/schema inventories are not another permission system.

One owner orders mutable native state per session/shared-service scope. Native SDK
retries must not create another Eliot Operation for the same logical input. Queue
admission, native execution, terminal, complete output capture, submission and Task
acceptance remain separate facts.

## Native identity

| Harness | Strongest relevant identity/readback |
| --- | --- |
| Muse | command ID, session/item revision, view cursor and output ref |
| OpenCode | durable input/message ID, session history and assistant parent edge |
| Codex | `clientUserMessageId`, thread, turn and `expectedTurnId` |
| ACP/ACPX | queue request, prompt-start and terminal watch; not a general replay key |
| Claude | SDK session/input/result-frame identity and supported history/control |
| Command / Antigravity | serialized run/input window and captured output |

RPC ID, PID and ACK time are not universal idempotency keys. Preserve native identity
before a write where required. Lost response causes bounded readback, never another
prompt or replacement session under the old Operation.

## Exact Store delivery

Native effect recovery and delivery of an already formed Store record are different.
The common code may own only the latter.

```text
StoreDelivery
  method = module.outcome | module.result
  immutable delivery key / params digest
  exact serialized params, including ready page bytes
  source Operation + binding/generation + immutable origin metadata
  atomic publish before IPC
  resend identical params until exact Store ACK
  remove after ACK
```

If Store committed but local removal was interrupted, replay identical params. Store
returns replay ACK. Conflicting bytes under the same identity fail. No local ACK
tombstone, universal journal, native prompt intent, Task state, scheduler or transcript
belongs here.

A shared implementation is extracted only after two current routes delete their old
delivery loops in the same change.

## Adapter-local result source

The source of a page is not common:

- durable local response body;
- captured output file;
- provider session/message/item/turn locator;
- or explicit unavailable/expired/gap.

The selected adapter owns native reading, lifetime and readback. A provider-history
route stores stable locators/identity, not a transcript copy. A no-history route must
persist complete body bytes before they are the only copy. Storage pressure stops new
admissions instead of evicting uncollected output.

## Store-authoritative page coverage

Store owns registered pages and aggregate source coverage. Extend exact
`module.result` ACK/replay to return:

```text
recorded
artifact_ref
source_identity_digest
coverage_complete
next_missing_offset / next_missing_length  # null when complete
```

The values are derived from retained Store pages for the exact source identity, not
caller claims. The adapter keeps local body/file/locator until `coverage_complete=true`
or explicit retention policy disposes it. It does **not** maintain another union of
ACKed ranges.

Outcome ACK releases only the outcome envelope. Page ACK releases only that page
envelope. EOF by itself is not complete coverage. Complete Store coverage permits
local source release; assembly can happen later from Store pages.

Current Store already retains each page's offset/total/EOF and operation-level
`next_offset_bytes`; the target connects this into aggregate replay-safe ACK instead
of adding a second range ledger.

## Adapter restart and old-boot delivery

Normal replacement reuses stable binding-scoped credentials. After custody proves the
previous owner departed, a new accepted boot may deliver complete retained records but
is not the executor that produced them.

Recovery checks:

```text
current authenticated delivery actor
+ proven prior-owner departure
+ exact original Operation/binding/request
+ immutable execution origin boot/native identity
+ complete retained delivery bytes
```

Keep ordinary current-boot validation for live outcomes. Recovery is a narrow Store
derived completion authority; it grants no new native command, no prompt replay and no
transfer to another Attempt. Missing/partial capture remains unknown. A mutable single
`previous_bridge_boot_id` is not durable origin across several replacements.

Supervisor operation readback stays status-only process/demand custody. It neither
imports native bytes nor settles targets.

## Current-state correctness loop

Result capture and readback are correctness duties, not optional workflow products.
One bounded host loop queries authoritative current state.

### `capture_once`

1. Select attributable terminal source with result capability and incomplete Store
   coverage.
2. Resolve immutable Task/Attempt/source scope. Current assignment is required for new
   execution, not historical output completion.
3. Ask Store coverage for the first missing range.
4. Admit at most one deterministic ordinary `agent.result` for source + range.
5. Wait for its settled/unknown readback before another range.
6. On complete coverage, optionally assemble and submit under selected policy.

No capture cursor/table/daemon. Page events cannot trigger the first page; the query
starts from source Operations and retained pages.

### `reconcile_once`

1. Select one exact unresolved target whose route declares readback and has no active
   reconcile child.
2. Admit one deterministic ordinary `agent.reconcile`.
3. Adapter performs read-only inspection; never native replay.
4. Found evidence settles target first, then lookup `resolved:true`.
5. Complete inconclusive inspection settles lookup Applied with `resolved:false`; the
   target remains unknown.
6. Retry only after relevant source epoch/cursor change or bounded due time.

Unchanged inconclusive readback coalesces rather than spinning. Unrelated work proceeds.

## Result origin and continuation

A result belongs to its actual source input Operation, not invariably the Attempt's
first Dispatch. This includes task-linked correction/Goal Send.

At source admission retain exact Task, Task revision, Attempt, binding and native
input/turn relation on the ordinary Operation. Do not infer it later from the newest
Task on a session. Keep `Attempt.start_owner` and `start_operation_id` unchanged.

Use the same path for Dispatch and attributable Send:

```text
source Operation
-> sealed source origin
-> adapter page
-> module.result
-> immutable page artifact
-> optional assembly
-> submission
```

Extend common source validation through page/candidate/submission consumers. Merely
adding `agent.send` to one allowlist is not connected. Taskless input remains explicit
and cannot be silently attached to a Task. A steer may contribute to an existing turn
instead of owning another terminal; preserve the real relation and avoid double count.

## Historical completion authority

A first Result may need admission after Task revision or Attempt release. Authorize it
from the immutable terminal source Operation:

```text
source Operation + exact Task/Attempt/binding relation
+ native result identity + retained result capability
+ current authorized capture actor
-> bounded result bytes only
```

It grants no prompt, Task mutation or completion of a newer revision. Existing sealed
pages continue through their narrow completion path and exact replay ACK. Deliberately
taskless output follows explicit retention policy; ambiguous source stays unknown.

## Cancellation, revision and release

Address unsent controller Operation, queued native input, active turn, result capture
and owned process separately. Socket close, timeout or dropped future proves none was
cancelled.

Task revision/release:

- cancels unsent old-revision task-linked native mutations;
- preserves sent/accepted/unknown effects for exact readback;
- preserves or admits required result capture;
- keeps late evidence historical;
- never completes a newer Task revision from old evidence.

Continuation/start authority requires exact current Task/Attempt. The current scoped
GM may reconcile/release superseded historical work without becoming its original
owner. Release remains blocked on unresolved effects; terminal alone is not proof of
complete output capture.

## Harness Goal and Eliot Goal

Harness Goal is native and default where supported. Native set/resume may start turns
linked to initiating Operation/current assignment; never duplicate initial input.

Eliot Goal independently sends ordinary messages on selected events/due occurrences.
Both may run together. One occurrence deduplicates its message; a later occurrence may
repeat text. Pausing/clearing either does not pause the other or cancel admitted input.
Neither accepts a Task.

## Local observation and account state

`agent.list/state/family/usage` reads joined local projection, not provider calls. Keep
assigned and unmatched subjects, current input/turn/pending requests, process
birth/liveness, assigned versus actual Git/worktree, remote-ref time/error and usage
separate. Source failures retain last facts with freshness/gaps.

Provider cursor belongs to native reader; Store observation cursor belongs to Governor
snapshot/replay. Expired/replaced/truncated source causes bounded rescan, not an empty
fleet.

Adapters normalize native-labelled quotas/availability by stable source,
epoch/sequence, observed time, coverage and honest account/service/connection scope.
Unknown is not zero or available.

## Artifacts, review and acceptance

Preserve current page registration/replay, Bytes authorization and
`ArtifactFiles::assemble`: same source identity, contiguous ranges from zero, no gaps or
overlap, correct total/EOF, verified page bytes and full digest. A valid empty body is
not missing. `artifact.assemble` returns admission receipt; settled `operation.get`
proves completion.

Assigned auditor submits candidate-specific verdict/evidence. Enabled return policy
calls ordinary `task.request_changes` and admits one correction message; direct callers
use the same transition. If no live binding exists, `needs_correction` remains current
for next resume.

Manual and automatic acceptance use one evidence gate. Candidate B cannot reuse A's
review. Preserve unanswered/inconclusive reviewer replacement. An unchanged candidate
can gain relevant evidence; a new ID alone is not progress.

## Optional integrations

Hooks, scripts and GitHub integrations call ordinary authorized Store commands. They
may run automatically without another Task authority. Diagnostic disable never
disables required observation, result capture or readback. Native SDK permission rules
handle authorized operations; unresolved judgment goes to Governor with exact request
identity, not a generic human queue.
