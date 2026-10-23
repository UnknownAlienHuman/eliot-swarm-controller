# Module API

This is the target Store/harness boundary. [Architecture](architecture.md) owns
purpose, [implementation](implementation.md) owns source changes,
[delivery](delivery.md) owns the autonomous cycle, and the
[audit register](audit-register.md) records source evidence. Rust types in
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
| `agent.background` | `Background` | Move eligible native foreground work to background. |
| `agent.cancel` | `Cancel` | Cancel exact turn/queued input; unsent controller work uses `operation.cancel`. |
| `agent.recover` | `Recover` | Restore a known session after supported loss checks, without replaying input. |
| `agent.refresh` | `Refresh` | Safe explicit native readback, not a prerequisite for local state reads. |
| `agent.result` | `Result` | Read attributable output through normalized bounded pages. |
| `agent.reconcile` | `Reconcile` | Read back one uncertain action; never repeat it. |
| `agent.shutdown` | `Shutdown` | Release/stop an explicitly owned resource, not an external shared service. |

Preserve `Attempt.start_owner`: native-manager claims reserve before native spawn;
controller dispatch must not race that first input while producer binding is pending.

Remove public vendor/internal names during route migration:

- `native.opencode.loop_step`: map retained callers only to an actually supported
  `Send` timing, not automatically to exact-turn steering;
- `native.command.cancel_turn` / `close_session`: semantic Cancel / Shutdown;
- `native.command.release_evidence`: private cleanup;
- `native.mcp.*`: OpenCode-internal setup/observation.

Native inbox steering and atomic expected-turn steering are different guarantees.
Report requested/effective semantics and reject only an unsupported guarantee.

## Capabilities and hello

Describe supported actions, input timing, cancel scopes, native Goal verbs, background
control and available local activity/result/quota sources. Capabilities route actions;
Store grants rights. Hello checks authenticated binding, current protocol major and
capability subset. Package/build/schema inventories are not a second permission system.

One owner orders state for each native session/shared-service scope. Queue admission,
physical execution, terminal result, complete output capture and Task acceptance are
separate. Native SDK retries must not create another Eliot Operation for the same
logical input.

## Logical input and native identity

| Harness | Strongest relevant identity/readback |
| --- | --- |
| Muse | Native command ID and session event cursor |
| OpenCode | Durable inbox/message ID and session history/parent edge when exposed |
| Codex | `clientUserMessageId`, thread/turn and `expectedTurnId` |
| ACP/ACPX | Queue request plus prompt-start/terminal watch; not a general replay key |
| Claude | Session/resume identity and selected SDK history/control interface |
| Command batch / Antigravity | Serialized run/input window and captured output |

RPC ID, PID and ACK time are not universal idempotency keys. Preserve native identity
before an effect where necessary. Lost response leads to bounded readback, not another
prompt or replacement session under the old Operation.

## Store-delivery storage

Native effect recovery and delivery of already formed Store records are distinct. The
following is A2 target behavior, not a claim about current adapters.

### Immutable deliveries

Before IPC, retain complete terminal/source bytes and exact `module.outcome` /
`module.result` parameters with source Operation, binding/generation, origin boot and
item/page identity in private atomic storage. Same-boot reconnect resends unchanged
parameters. Content integrity belongs to actual artifact bytes, not proof wrappers for
every private JSON value.

An ACK releases only its exact delivery envelope:

- outcome ACK releases the outcome envelope;
- page ACK releases that page envelope;
- source body remains until Store-ACKed ranges cover `[0,total)` or explicit retention
  policy disposes it;
- EOF alone does not prove earlier ranges.

Persist a bounded per-source coverage record with the body:

```text
source Operation + body digest + total bytes
+ normalized union of ACKed [offset,end) ranges
```

ACK replay is idempotent. Complete page coverage permits body deletion; assembly may
happen later from Store pages. Do not add a transcript mirror or universal journal.
Reserve storage before new work and surface pressure instead of evicting uncollected
output.

### Restarted adapter and delivery actor

Normal replacement reuses the stable binding-scoped credential. After custody proves
the prior owner departed, a new accepted boot may deliver complete retained records but
is not the executor that produced them. Keep the original outcome/body and origin boot
unchanged. Recovery linkage belongs to Store completion context, not a rewritten
outcome.

Connect through existing owners:

1. Adapter startup loads complete private captures before any native open. A fresh
   in-memory Controller is insufficient.
2. Store retains execution origin on the source Operation. A mutable single
   `previous_bridge_boot_id` is not durable origin across several replacements.
3. Ordinary live settlement keeps current-boot validation. Recovery adds a narrow
   Store-derived completion authority checking current delivery actor, proven departure,
   original Operation/binding/request and immutable origin boot.
4. Exact duplicate committed outcomes/pages retain replay ACK. Conflicting bytes never
   replace known evidence.
5. Missing/partial capture stays unknown; recovery starts no native process and sends no
   prompt.

Supervisor operation readback is status-only process/demand custody. It does not import
native outcome bytes or create Result/Reconcile Operations.

### Authentication versus admitted completion

New connections authenticate normally and reject disabled credentials. New native
commands require current execution authority. A previously admitted normalized page
has narrow exact completion authority even after live-link/release changes. It grants
no arbitrary history read and no new prompt.

A first historical capture created after Task supersession needs similarly narrow
source-Operation authority. It must not depend on the Task remaining current, but it
must prove the original Task/Attempt/binding/native source and expose bytes only through
existing page/artifact authorization.

### Replaceable observation checkpoint

Retain only the latest pending normalized state per source, with epoch, source cursor
and coverage/gap. New unsent state can replace older state. Store's journal records
accepted checkpoints; a lost intermediate native frame is a coverage issue, not a
reason for an infinite provider-event mirror.

### Link loss is not native cancellation

Keep reducing native events and durably capturing terminal output while Eliot IPC is
unavailable. Do not await reconnect/backoff inside the only native-event reducer. Retry
only saved Store deliveries. Hold new work on storage/delivery pressure; do not close
native stdin, terminate session or forget terminal facts because observation delivery
failed.

## Outcome, terminal and result identity

Ordinary outcomes name Operation, binding/boot guard, native IDs, disposition and
bounded action details. Store loads retained request/Task/Attempt facts instead of
requiring package, protocol and prompt hashes again.

Warm-stream terminal evidence may contain conversation, result ordinal, status and
response digest. It does not require an observation row duplicating the result. A
terminal `agent.send` remains evidence for that source Operation; it must not be
converted into a second initial producer.

Result pages name source Operation, byte range, total/final marker and content digest.
Different page-read Operations are deliveries of one source body, not different native
answers.

## Current-state correctness loop

Result capture and readback are not optional workflow steps. One bounded host loop
queries authoritative current state.

### `capture_once`

1. Select a terminal attributable source with retained-result capability and incomplete
   Store coverage.
2. Resolve immutable Task/Attempt/source scope; current assignment is required for new
   execution, not historical completion.
3. Compute first missing byte range from retained pages.
4. Admit one deterministic ordinary `agent.result` for that source/range.
5. Wait for its settled/unknown readback before another range.
6. On complete coverage, optionally assemble/submit under selected policy.

No capture cursor/table is needed. Page-created events cannot trigger the first page;
the query starts from source Operations and retained artifacts.

### `reconcile_once`

1. Select one exact unresolved target with declared readback and no active reconcile
   child.
2. Admit one deterministic ordinary `agent.reconcile`.
3. Adapter performs read-only inspection, never native replay.
4. Found evidence settles target first, then lookup `resolved:true`.
5. Complete inconclusive inspection settles lookup `resolved:false`; target remains
   unknown.
6. Retry only on relevant source epoch/cursor change or bounded due time.

Unchanged inconclusive results coalesce instead of spinning. Unrelated work proceeds.

## Cancellation, recovery and shutdown

Address unsent controller Operation, queued native input, active turn, result capture
and owned process separately. Socket close, timeout or dropped future proves none was
cancelled.

Task revision/release:

- cancels unsent task-linked native mutations;
- preserves sending/accepted/unknown mutations for exact readback;
- preserves or admits required result capture;
- keeps late evidence historical;
- never changes old evidence into completion of a newer revision.

Recover/Reconcile are ordinary commands that Governor or the correctness loop can
invoke. Readbacks have bounded retries; unchanged uncertainty blocks only its exact
resource.

## Harness Goal and Eliot Goal

Harness Goal is native and default where supported. Native set/resume can start turns
linked to initiating Operation and current assignment; do not duplicate initial input.

Eliot Goal independently sends ordinary messages on selected events or due
occurrences. Both may run together. One occurrence deduplicates its message; a later
occurrence may repeat text. Pausing/clearing either does not pause the other or cancel
already admitted input. Neither accepts a Task.

## Local state and source cursors

`agent.list/state/family/usage` read joined local Store projection, not provider calls.
Keep assigned and unmatched native subjects, current input/turn/pending requests,
process birth/liveness, assigned versus actual worktree/Git, remote-ref time/error and
usage/limits separate. Source failures retain last facts with freshness/gaps.

Provider cursor belongs to native reader; Store `observation_id` belongs to Governor
snapshot/replay. Expiry, source replacement, truncated JSONL or missing WAL coverage
cause bounded rescan, not an empty fleet.

## Account capacity

Adapters normalize native-labelled buckets/windows/credits and availability by stable
source, epoch/sequence, observed time, coverage and real account/service/connection
scope. Core validates this boundary, not vendor method/artifact allowlists or auth proof
hashes. Unknown is not zero or available.

## Results and artifacts

```text
source input Operation -> agent.result -> module.result -> immutable page
                      -> artifact.read/assemble -> submission
```

A result belongs to its actual logical input, including task-linked continuation. At
admission retain the source Operation's Task/Attempt and native input/turn relation.
Do not infer it later from current session or newest Task. Keep
`Attempt.start_operation_id` unchanged.

Use explicit native parent/input identity or a verified serialized window. Independent
concurrent input or unexplained source gap invalidates ordering-only association. A
steer may contribute to current turn rather than own another terminal.

Preserve existing page registration/replay, `artifact.assemble`, byte reads and
candidate validation. Availability, complete capture, submission and acceptance are
different facts. Assembly returns admission receipt; settled `operation.get` proves
completion.

## Review and acceptance

Assigned auditor submits candidate-specific verdict/evidence. Enabled return policy
calls ordinary `task.request_changes` and admits one correction message; direct callers
use the same transition. If no live binding exists, `needs_correction` remains current
for next resume.

Manual and automatic acceptance use one evidence gate. Candidate B cannot reuse A's
review. Preserve unanswered/inconclusive reviewer replacement. An unchanged candidate
can gain relevant evidence; a new ID alone is not progress.

## Optional integrations

Hooks, scripts and GitHub integrations call ordinary authorized Store commands. They
may run automatically without acquiring another Task authority. Diagnostic disable
never disables required local observation or capture. Native SDK permission rules
handle permitted operations; unresolved judgment goes to Governor with exact request
identity, not a generic human queue.
