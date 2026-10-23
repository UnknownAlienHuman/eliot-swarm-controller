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

- `native.opencode.loop_step` -> only an actually supported Send timing;
- `native.command.cancel_turn` / `close_session` -> semantic Cancel / Shutdown;
- `native.command.release_evidence` -> local source cleanup after Store coverage;
- `native.mcp.*` -> OpenCode-internal setup/observation.

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
  immutable delivery key / exact params identity
  exact serialized params, including ready page bytes
  source Operation + binding/generation + immutable origin metadata
  atomic publish before IPC
  resend identical params until exact Store ACK
  remove the acknowledged delivery
```

If Store committed but local removal was interrupted, replay identical params. Store
returns replay ACK. Conflicting bytes under the same identity fail. No local ACK
tombstone, universal journal, native prompt intent, Task state, scheduler or transcript
belongs here. Preserve content hashes on actual bytes, not another private-JSON proof.
A shared implementation requires two current routes deleting their old delivery loops.

## Adapter-local result source

The source is adapter-local: durable response body, captured output file, exact provider
session/message/item/turn locator, or explicit unavailable/expired/gap. Separate three
operations: locate one attributable output, read its bytes, deliver a ready Store page.

Use native byte ranges where actually supported. If the native API returns a whole
final response, materialize that selected immutable output once and page it locally;
do not reread a whole thread/response for every small Store page. A bounded private
selected-result file is permitted, not a transcript mirror or cache service. Use
existing file/delivery owners and release after Store-complete coverage. A no-history
route must persist bytes before they become the only copy. Storage pressure pauses new
admissions instead of evicting existing uncollected output.

Locator discovery matches the original input/parent and exact output revision. Bound
aggregate pages, bytes and elapsed work, not only one response size. A budget stop
retains an exact native continuation in the existing source/read action and reports
partial coverage; it is not absence or success. Reuse the selected locator for subsequent
ranges. Never use the newest message, arbitrary output URI or another session as fallback.
A complete native error can have useful partial output without becoming a successful
candidate. Completeness and execution disposition remain separate.

### Historical source is not the current executor

Bind reads to the original Operation, binding generation, native output and approved
provider/service/account/workspace scope. A new active session or changed model does
not relabel old bytes. Do not invoke Open, Resume or Configure just to make capture pass.

Do not indiscriminately remove current-root checks: OpenCode fixes its root per binding
generation, and Muse's family predicate limits accessible sessions. A historical reader
needs Store-derived source authority and an appropriate existing read-only connection;
it does not receive arbitrary old-session access or silently inherit another binding.
Claude/Antigravity memory loss requires retained bytes, not weaker identity checks.
Already prepared Store pages replay without rediscovering the provider source.

## Store-authoritative page coverage

Store owns registered pages and aggregate source coverage. Extend exact
`module.result` ACK/replay to return the registered artifact, source identity,
`coverage_complete` and the next missing range (null when complete). This is target
behavior; today's per-page `next_offset_bytes` is not aggregate coverage.

Source identity means original input plus selected native output/revision and content
identity. Per-read Operation IDs, discovery/pagination cursors and mutable producer
lifecycle fields do not split one immutable output into different sources. Distinct
outputs or revisions must not be unioned because their text/hash happens to match.
Pin available total length/digest and expose conflict or expiry instead of mixing pages.

Derive coverage from committed pages for that identity, not caller claims or an
adapter-local range union. Outcome ACK releases its envelope; page ACK releases that
page envelope. EOF alone does not prove earlier coverage. A valid empty complete body
has explicit zero-length coverage. Complete Store coverage permits local source release;
assembly can happen later from retained Store pages. Otherwise retain the source until
capture completes or an explicit retention policy disposes it with a visible gap.

## Adapter restart and old-boot delivery

Normal replacement reuses stable binding-scoped credentials. After custody proves the
previous owner departed, a new accepted boot may deliver complete retained records but
is not their original executor.

```text
current authenticated delivery actor
+ proven prior-owner departure
+ exact original Operation/binding/request
+ immutable execution origin boot/native identity
+ complete retained delivery bytes
```

Keep live current-boot validation. Recovery is narrow Store-derived completion
authority, not new native command authority or permission to transfer output to another
Attempt. Missing/partial capture stays unknown. One mutable previous-boot field cannot
retain origin across several replacements. Supervisor readback stays status-only
process/demand custody; it neither imports bytes nor settles targets.

## Current-state correctness loop

Capture and readback are correctness duties, not optional workflow products. One
bounded host loop uses existing Operations, retained source facts and registered
artifacts. No capture table, copied Task, per-domain cursor or daemon.

### `capture_once`

The candidate projection is:

```text
input Operation + attributable output-ready fact / retained source
  minus complete registered coverage or explicit source disposition
  minus an active page/discovery/reconcile read for the same source
  filtered by current capture authority, read capability and retry due
  -> bounded fair candidate page -> one ordinary agent.result admission
```

An Operation being settled can mean input admission, not output readiness. Start from
source facts, not a page event that cannot exist before the first read. A first range
may have unknown total length and need bounded adapter-local locator discovery; an
OpenCode assistant message or Muse item revision is not invented by Store.

Reuse the existing writer transaction to recheck source identity, authority, coverage
and duplicate child before admission. Helpers and direct callers converge on the same
retained read; concurrent wakes do not create competing page work. Keep range and source
keys queryable in existing Operation/artifact owners. Audit the query plan/index with
the implementing slice rather than scanning all historical JSON on each wake.

Process a bounded amount per source and retain the host loop's fair scan position;
blocked/expired sources cannot occupy the first page forever. Native/file I/O runs
outside Store, consumes no model/root slot and yields to replies/control. Notifications
are hints; restart can recompute missing coverage from retained facts.

For an admitted read with unknown Store ACK, replay its saved exact page. A completed
failed read gets no second execution under its immutable failed request key. If a later
source change or due policy permits another read, retain a new ordinary read attempt
linked to the same source/range; never resend the native prompt. Coalesce unchanged
failure into one addressed Governor decision while other sources progress.

On complete capture, enabled policy may assemble and submit using current grants and
candidate rules. Historical bytes do not complete a newer Attempt. The Governor does
not manually collect page IDs; no new result workflow owns this sequence.

### `reconcile_once`

Select an exact unresolved readable target with no active reconcile child. Admit one
ordinary Reconcile, then inspect without native replay. Found evidence settles the
target before lookup `resolved:true`. Completed inconclusive inspection settles the
lookup Applied with `resolved:false`; target stays unknown. Retry only after relevant
source change or bounded due time, coalescing unchanged uncertainty.

## Result origin and continuation

A result belongs to its actual source input, including task-linked correction/Goal
Send, not invariably the first Dispatch. Retain Task/revision/Attempt/binding and native
input/turn relation on the ordinary Operation. Do not infer it from the newest Task on
a session. Keep `Attempt.start_owner` and `start_operation_id` unchanged.

```text
source Operation -> sealed origin -> adapter page -> module.result
                 -> immutable artifact -> optional assembly -> submission
```

Extend origin validation through page/candidate/submission consumers, not one method
allowlist. Taskless input stays explicit. A steer can contribute to an existing turn
without owning another terminal; preserve that relation and avoid duplicate capture.

## Historical completion authority

First Result admission can occur after revision/release. Derive it from the immutable
source Operation, exact Task/Attempt/binding, native output/retained capability and a
currently authorized capture actor. Reuse `object_scope` retained-identity and scoped
Manager/GM policy owners; no synthetic Operator or new permission registry.

Keep three permissions separate: persist this previously admitted source, disclose its
bytes to a reader, and assemble/submit it. Operation receipt visibility is not a Bytes
grant. Current exact-result-caller access is capped at Bytes, not Assemble; a historical
reviewer receipt does not reopen candidate content. Assembly intersects source grants.
Do not widen these ceilings merely to automate collection.

Capture admits no prompt and mutates no newer Task. Existing sealed pages keep their
narrow completion/replay path. Taskless output follows explicit source retention scope;
ambiguous provenance stays unknown. Revocation does not justify bypassing credentials.

## Cancellation, revision and release

Address unsent controller Operation, queued native input, active turn, result capture
and owned process separately. Socket close, timeout or dropped future proves none was
cancelled.

Revision/release cancels unsent old-revision native mutations, preserves sent/accepted/
unknown effects for readback, and preserves or admits required capture. Late evidence
stays historical. Current Task/Attempt authority is needed for new work; current scoped
GM may dispose superseded work without becoming its original owner. Unresolved effects
still fence release; terminal alone does not prove complete output capture.

## Harness Goal and Eliot Goal

Harness Goal is native and default where supported. Native set/resume may start turns
linked to initiating Operation/current assignment; never duplicate initial input.
Eliot Goal independently sends ordinary messages on selected events/due occurrences.
Both may run together. One occurrence deduplicates its input; a later occurrence may
repeat text. Pausing/clearing one does not pause the other or cancel admitted input.
Neither accepts a Task.

## Local observation and account state

`agent.list/state/family/usage` reads joined local projection, not provider calls. Keep
assigned/unmatched subjects, input/turn/pending requests, process birth/liveness,
assigned versus actual Git/worktree, remote-ref time/error and usage separate. Source
failures preserve last facts with freshness/gaps. Provider cursor belongs to its reader;
Store cursor belongs to Governor replay. Expiry/truncation causes bounded rescan, not an
empty fleet.

Adapters normalize native-labelled quotas/availability by source, epoch/sequence,
observed time, coverage and honest account/service/connection scope. Unknown is not zero
or available. Invalid optional usage cannot discard valid terminal evidence.

## Artifacts, review and acceptance

Preserve page registration/replay, Bytes authorization and `ArtifactFiles::assemble`:
same source identity, contiguous coverage, total/EOF, page integrity and full digest.
Assembly returns admission, not completion; inspect settled `operation.get`.

Assigned auditor submits candidate-specific verdict/evidence. Enabled return policy
calls ordinary `task.request_changes` and admits one correction message; direct callers
use the same transition. Without a live binding, `needs_correction` remains for resume.
Manual/automatic acceptance use one gate. Candidate B cannot reuse A's review. Preserve
unanswered/inconclusive reviewer replacement; relevant evidence can advance unchanged
code, but a new ID alone is not progress.

## Optional integrations

Hooks, scripts and GitHub use ordinary authorized commands, automatically when enabled,
without another Task authority. Diagnostic disable never disables observation, capture
or readback. Native permission rules handle authorized operations; unresolved judgment
goes to Governor with exact request identity, not a generic human queue.
