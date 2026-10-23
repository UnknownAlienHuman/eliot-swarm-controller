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

Normal process replacement reuses the stable binding-scoped credential; it does not
mint another identity or rotate its token. After existing custody checks establish
prior-owner departure, the accepted successor boot can deliver retained evidence but
is not the executor that produced it. Keep original outcome/body bytes and origin boot
unchanged. Recovery linkage belongs to the delivery context, not a rewritten outcome.

Connect A2 through the existing owners:

1. Adapter startup/`Controller::read_command` loads complete private captures before
   requiring native open. Current startup already waits for `agent.open` to spawn;
   preserve that. No native process, prompt or model call is needed to drain files.
2. Store retains the execution boot with the original input Operation at dispatch.
   Reuse the existing dispatch context where present; ordinary Send needs the same
   attributable origin without a second producer ledger. A mutable
   `previous_bridge_boot_id` alone is insufficient after two replacements. Missing
   historical attribution stays unknown rather than being inferred from the current
   root, file timestamp or latest boot.
3. `outcome_with_artifacts` uses one settlement path with Store-derived live or retained
   completion authority. The latter checks current delivery actor, custody and exact
   original Operation/binding/origin. Keep ordinary live boot checks; do not replace
   them with a blanket current-or-previous test. Already committed identical outcomes
   retain replay ACK behavior; conflicting terminal bytes never replace them.
4. `next_internal` permits authorized completion-only result/readback work while new
   execution remains fenced by `recovery_required`. `admitted_result_scope`,
   `normalized_result::validate_source` and `results::prepare/record` remain the page
   owners. Complete required historical ranges under the original source scope; do not
   clear recovery or claim a live native session merely to retrieve saved bytes.

The existing `module.supervisor.recovery.reconcile` only reconciles automation/Hook
source intake; it is not a terminal importer. Do not route native evidence into that
journal or add a recovery daemon. The successor path must explicitly connect the
adapter capture, Store settlement and existing page delivery.

A superseded assignment receives historical evidence, not completion of a new Attempt.
An existing page completion grant is not permission to create arbitrary historical
reads. Missing page admissions need exact completion authority for the retained source,
not a new prompt or broad access to old Tasks. Missing/partial bytes require bounded
native readback or visible unknown. A crash before durable capture remains a real limit.

### Authentication versus admitted completion

New connections authenticate normally and reject disabled credentials. New native
commands require current execution authority. A previously admitted normalized page
has a narrow registered-binding/Operation completion path on an authenticated carrier;
it does not grant arbitrary reads or new work after release/link changes.

Reuse the stable credential for ordinary replacement. Credential revocation is a
separate policy: never bypass authentication, reinterpret old bytes as a new identity,
or rotate automatically to make recovery succeed. Any exceptional completion actor
must be explicitly scoped through existing host authority. Revocation side effects
and released-binding recovery still need connected implementation/qualification.

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

A completed readback and its target's outcome are separate. If an exact local lookup
finishes but finds no usable terminal, settle the Reconcile Operation with
`resolved:false` and a bounded reason; the original target remains unknown. Only an
interrupted/uncertain read itself stays pending or unknown. When evidence is found,
commit/replay the original target first, then settle the summary with `resolved:true`.
This reuses the existing target-before-summary validation and batch readback pattern.
Neither summary permits prompt replay, Task acceptance or automatic native resume.

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
source input Operation -> agent.result -> module.result -> immutable page
                      -> artifact.read/assemble -> submission
```

### Initial input and continuation use the same result path

A result belongs to its actual logical input, not invariably to the Attempt's first
Dispatch. This includes next-turn corrections and Eliot Goal messages. Current generic
admission is narrower: `runtime::user_command_with_actor` and
`normalized_result::admitted_origin` require `task.dispatch`; the strict Antigravity
alternative returns status, not the complete answer. See AR-006 in the register.

Extend the existing path, not a parallel Send-result API:

1. At admission retain the continuation Operation's authorized Task/Attempt and native
   input/turn relation. Reuse ordinary Operation fields and the selected adapter's
   native facts; do not infer assignment later from the current session or newest Task.
2. Resolve result origin from that source Operation and its retained execution facts.
   Keep `Attempt.start_owner` and `start_operation_id` unchanged. A correction does not
   need a new Attempt or a second initial dispatch just to make its result readable.
3. Use the same page registration, exact replay, byte reads and assembly as Dispatch.
   Historical results retain their original scope and cannot complete a newer Attempt.
4. Steering may contribute to the current native turn rather than produce a separate
   terminal response. Retain the actual turn relation; do not manufacture an independent
   answer for every steer or count the same terminal twice.

The connected change includes `runtime::user_command_with_actor`, the normalized-origin
admission/retained-source validators and artifact/candidate consumers, plus the selected
adapter's body lookup. Merely accepting another method string leaves the dispatch
producer and first-start assumptions behind. Keep taskless or ambiguous multi-Attempt
commands distinct until their actual source scope is established; do not silently
attach their output to a Task. No new Task ledger, producer workflow or proof family.

Use explicit native parent/input identity, or an actually verified serialized window.
An independent concurrent input or unexplained source gap invalidates ordering-only
association; never pick the newest assistant message. For selected current OpenCode,
use its exact assistant parent edge when available.

### Capture and assembly stay automatic and reuse existing owners

Enabled result handling retrieves missing pages of the retained source, uses the
existing whole-result path when needed, and advances to submission only after complete
capture. The Governor is not expected to manually collect page IDs or repeat prompts.
Current Store facts and the existing policy loop determine the next missing action;
do not add a result-collection service or duplicate workflow state.

Preserve `artifacts::assembly::plan/ArtifactFiles::assemble`: matching source identity,
contiguous non-overlapping ranges from zero through total length, correct EOF, verified
page bytes and full payload digest. Different page-read Operation IDs are not different
source inputs. A valid empty body is not a missing result. Object authorization remains
separate from content integrity; metadata access alone does not grant bytes.

Store publishes immutable bytes before their receipt. A file with no committed receipt
is not a published result. Assembly returns an admission receipt; its settled
`operation.get` outcome establishes success, not the returned `queued` receipt. Release
the source body only after complete capture for that source is recorded, or explicit
retention/release policy. Reuse `artifact.assemble` and existing registration/replay;
do not add another assembler, release-proof engine or receipt database. Availability,
submission and acceptance remain distinct.

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
