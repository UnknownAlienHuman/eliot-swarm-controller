# Module API

This is the target Store/harness boundary. [Architecture](architecture.md) owns
purpose, [implementation](implementation.md) owns source changes, and
[delivery](delivery.md) describes enabled autonomous handoffs. Rust types in
`swarm-contracts` own field bounds; do not duplicate schemas here.

## Store methods and semantic actions

Governor, managers and enabled helpers call the same Store handlers. Store authorizes
current actor/object scope, retains an Operation and delivers a typed action. The
adapter has binding-scoped credentials, not manager rights to create or accept Tasks.

| Store surface | Adapter action | Meaning |
| --- | --- | --- |
| `agent.open` | `Open` | Open, resume or attach the selected session. Resume never silently creates a replacement. |
| `task.dispatch` | `Dispatch` | First assigned logical input of a controller-started Attempt. |
| `agent.send` | `Send` | Next-turn input or supported steering with declared timing/target semantics. |
| `agent.reply` | `Reply` | Answer one current pending question/form/permission. |
| `agent.configure` | `Configure` | Apply supported model/mode/effort/options and report effective values. |
| `agent.goal` | `Goal` | Native set/edit/pause/resume/clear as supported. |
| `agent.background` | `Background` | Move an eligible native foreground run/tool to background. |
| `agent.cancel` | `Cancel` | Cancel the exact turn or queued input; unsent controller work uses `operation.cancel`. |
| `agent.recover` | `Recover` | Restore a known session after supported loss checks, without replaying input. |
| `agent.refresh` | `Refresh` | Safe explicit native readback, not a prerequisite for ordinary local state reads. |
| `agent.result` | `Result` | Read attributable output through normalized pages. |
| `agent.reconcile` | `Reconcile` | Read back an uncertain action; never repeat it. |
| `agent.shutdown` | `Shutdown` | Release/stop the explicitly owned resource, not an externally owned shared service. |

Preserve `Attempt.start_owner`: native-manager claims reserve before native spawn;
controller dispatch must not race that first input while producer binding is pending.
The existing `START_OWNED_BY_MANAGER` guard is retained.

Remove public vendor/internal names during route migration:

- `native.opencode.loop_step`: remove the public method; translate retained callers to
  an actually supported `Send` timing, not automatically to exact-turn steering;
- `native.command.cancel_turn` / `close_session`: semantic Cancel / Shutdown;
- `native.command.release_evidence`: private cleanup;
- `native.mcp.*`: OpenCode internal setup/observation.

Native inbox steering and atomic expected-turn steering are different guarantees.
Current host OpenCode code rejects the latter when it lacks the native guard. Do not
rename the weaker action as exact, nor remove useful native controls merely because
another provider implements stronger ones. Report requested/effective semantics and
reject only an unsupported guarantee. A future additional delivery mode needs a real
caller, not placeholder handlers across adapters.

## Capabilities and hello

Describe supported actions, input timing, cancel scopes, native Goal verbs, background
control and available local activity/result/quota sources. Capabilities route; Store
grants rights. The installed descriptor selects executable/configuration and required
actions. Hello checks authenticated binding, current protocol major and capability
subset. Package/build/schema inventories are not a second runtime permission system.

## One native state owner

One owner orders state for each native session/shared service scope. Share a native
connection where supported. The common Eliot link does not mutate native state.
Queue admission, physical execution, terminal result and Task acceptance remain
separate. Native SDK retries must not create another Eliot Operation for the same
logical input.

## Logical input and native identity

| Harness | Strongest relevant identity/readback |
| --- | --- |
| Muse | Native command ID and session event cursor |
| OpenCode | Durable inbox/message ID and session history |
| Codex | `clientUserMessageId`, thread/turn and `expectedTurnId` |
| ACP/ACPX | Queue request plus prompt-start/terminal watch; not a general replay key |
| Claude | Session/resume identity and selected SDK history/control interface |
| Command batch / Antigravity | Serialized run/input window and captured output |

RPC ID, PID and ACK time are not universal idempotency keys. Preserve native identity
before an effect where necessary. Lost response leads to bounded readback, not another
prompt or a replacement session under the old Operation.

## Store delivery storage

Native effect recovery and delivery of already formed Store records are distinct.
The following is A2 target behavior, not a claim that current adapters implement it.
Concrete evidence and remaining traces are in the [audit register](audit-register.md).

### Immutable deliveries

Retain complete terminal bytes and exact `module.outcome` / `module.result` parameters
with Operation, original binding/generation, origin boot and item/page identity in
private atomic storage before IPC. Same-boot reconnect sends the same parameters.
Keep content integrity on actual artifact bytes, not another proof layer for each
private JSON value.

An ACK releases only the item it acknowledges:

- outcome ACK releases that outcome envelope, not its full source response;
- page ACK releases that page envelope, not every remaining page;
- complete durable Store capture, or explicit retention/release policy, permits
  deleting the source body. An EOF page does not alone prove complete earlier coverage.

Serve delayed and repeated page reads from the same retained body. Memory cache
turn-count eviction cannot delete the only copy of an uncollected result. Reserve
bounded storage for admitted output, pause new admissions when full and surface a
specific storage fault rather than silently dropping data. Partial/oversized native
output remains explicitly incomplete. This is result retention, not a transcript
mirror or a new generic journal.

### Restarted adapter

A2 also covers process loss. The existing reconciliation owner fences the previous
delivery owner, loads complete retained records and submits them for their original
Operations under current authorized recovery. Store verifies original scope and
receipt identity and deduplicates delivery. Origin boot is immutable; the successor
is a recovery actor, not the original executor.

This does not revive an old live binding or transfer a result to a new Attempt. If the
assignment was superseded, keep late evidence as historical without changing current
completion or acceptance. Missing/partial storage requires native readback or honest
unknown. A crash before durable capture is not recoverable by pretending the spool
contains the result.

Reuse the current normalized completion boundary in `runtime::admitted_result_scope`,
`normalized_result::validate_source` and `results::prepare/record`. It already handles
exact previously admitted pages without ordinary live-link/release gating and ACKs
identical registered-page replay. It does not establish the entire successor or
transport-revocation contract: those paths must be connected without removing scoped
authorization. Do not invent another general receipt/recovery subsystem.

### Replaceable observation checkpoint

Retain only the latest pending normalized state per source, with epoch, source cursor
and coverage/gap. New unsent state can replace older state. Store's journal records
accepted checkpoints; a lost intermediate native frame is a coverage issue, not a
reason for an infinite provider event mirror.

### Link loss is not native cancellation

Keep reducing native events and durably capturing terminal output while Eliot IPC is
unavailable. One bounded in-flight delivery or a small delivery task can run alongside
the existing native owner; preserve its state rather than cancelling and recreating a
write on every event-loop iteration. Do not await a whole reconnect/backoff loop inside
the only native-event handler.

Retry only saved Store deliveries. Hold new work on delivery/storage pressure, but do
not close native stdin, terminate the session or forget terminal facts merely because
an observation could not be posted. If an explicit authorized stop drains a terminal,
that terminal follows the same durable capture path. True ownership loss or revoked
authority is handled by its own policy; a network error is not evidence of either.
No additional service, broker or scheduler is needed.

### Common exclusions

Delivery storage contains no native prompt intent, retry policy, session reconstruction,
Task state or transcript mirror. A replacement boot cannot silently adopt old files.
Authorized reconciliation above is an explicit recovery path, not a new broker or
journal framework. Share implementation only after two current routes delete their
previous delivery loops.

## Outcome and result identity

Ordinary outcomes name Operation, binding/boot guard, native IDs, disposition and
bounded action details. Store loads its retained request instead of demanding package,
protocol, Task/source/prompt hashes again. Recovery additionally preserves origin
identity as described above.

Warm-stream terminal evidence may include conversation, result ordinal, status and
response-content hash. It does not require an observation row duplicating the result.
Result pages name source Operation, byte range, final/total markers and content hash.

## Cancellation, recovery and shutdown

Address unsent controller Operation, queued native input, active turn and owned process
separately. Socket close, timeout or a dropped future proves none of those cancelled.
Recover/reconcile are ordinary commands that Governor or authorized policy can invoke.
Readbacks have bounded retries; unchanged uncertainty coalesces rather than triggering
repeated model prompts. Independent work proceeds.

## Harness Goal and Eliot Goal

Harness Goal is native and the default where supported. Native set/resume can start
turns linked to its initiating Operation and current assignment; do not duplicate the
initial input.

Eliot Goal is an independent plugin sending ordinary messages on selected events or
due occurrences. Both mechanisms may run together with different objectives. One due
occurrence deduplicates its message; a later occurrence may repeat the same text.
Pausing/clearing either mechanism does not pause the other or cancel already admitted
input. Neither accepts a Task.

## Local state and source cursors

`agent.list/state/family/usage` read the joined local Store projection, not the provider.
Keep assigned and unmatched native subjects, current input/turn/pending requests,
process birth/liveness, assigned versus actual worktree/Git, remote-ref time/error and
usage/limits separate. Source failures retain last facts with explicit freshness/gaps.

Provider cursor belongs to its native reader; Store `observation_id` belongs to
Governor replay/snapshot. Close replay/live races with a native watermark or bounded
subscribe-before-replay where supported. Expiry, source replacement, truncated JSONL
or missing WAL coverage cause bounded rescan, not an empty fleet. Native readers must
not fabricate events from changed schemas.

## Account capacity

Adapters normalize native-labelled buckets/windows/credits and availability by stable
source, epoch/sequence, observed time, coverage and real account/service/connection
scope. Core validates that boundary, not vendor method/artifact allowlists or auth
proof hashes. Share one account snapshot where actual account identity is available;
otherwise keep connection scope. Invalid optional quota cannot discard valid terminal
or activity facts. Unknown is not zero or available.

## Results and artifacts

```text
agent.result -> module.result -> immutable artifact -> artifact.read/assemble -> submission
```

Use explicit native parent/input identity, or an actually verified serialized window.
An independent concurrent input or unexplained source gap invalidates ordering-only
association; never pick the newest assistant message. For selected current OpenCode,
use its exact assistant parent edge when available.

Store publishes immutable bytes before their Store receipt. A file with no committed
receipt is not a published result. Keep the page until ACK and its source body until
complete capture/release; reuse `artifact.assemble` rather than adding another
assembler. Availability is not submission or acceptance.

## Review and acceptance

The assigned auditor submits candidate-specific verdict/evidence. Enabled return policy
calls ordinary `task.request_changes` and admits its correction message transactionally;
manual callers use the same transition. Native delivery remains a separate external
effect after commit. If no live binding exists, correction remains current Attempt
state for the next dispatch.

Manual and automatic acceptance use one evidence gate. Candidate B cannot reuse A's
review. Existing unanswered/inconclusive reviewer replacement stays available. An
unchanged candidate can gain relevant missing evidence, but a new ID alone is not
progress. Details are in [delivery](delivery.md).

## Optional integrations

Hooks, scripts and GitHub integrations call ordinary authorized Store commands. They
may run automatically under selected policy without acquiring another Task authority.
Diagnostic disable never disables required local observation. Native SDK permission
rules handle permitted operations; unresolved questions go to Governor with exact
request identity, not to a generic human approval queue.
