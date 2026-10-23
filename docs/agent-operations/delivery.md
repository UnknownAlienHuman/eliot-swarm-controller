# Delivery — autonomous work through ordinary commands

This is the target work cycle, not a claim that all handoffs are implemented.
[Implementation](implementation.md) owns source changes;
[status](../implementation-status.md) records landed behavior;
[audit register](audit-register.md) records inspected evidence and open gaps.
Governor and Manager are agent roles. The owner supplies scope, budgets, credentials
and exceptional intent; routine operation must not fall back to a human.

## 1. One work cycle, one set of owners

An enabled policy connects:

```text
select current work
-> claim/resume one Attempt
-> admit one native input
-> capture attributable complete output
-> submit a frozen candidate
-> independent review
-> return all findings together
-> correct and resubmit
-> accept through one evidence gate
-> selected publication
-> next eligible work
```

Task, Attempt, Operation, binding, candidate, review and artifact records already own
this cycle. Helpers query current facts and call the same Store handlers as agents.
Do not add another workflow ledger, repair queue, continuation registry, result queue or
model-driven heartbeat.

`explicit` means attributable Store intent with current authority. It does not mean a
manual click. Unknown effects block only their exact resource while unrelated work
continues.

## 2. Automatic and direct equivalents

| Current fact | Enabled ordinary action | Completion / next decision |
| --- | --- | --- |
| Eligible work plus route/workspace capacity | `dispatch_once` claims and launches or resumes | One initial-input owner and one admitted Operation |
| Native question/permission request | Native permission rule or addressed Governor `Reply` | Exact request answered |
| Attributable terminal with incomplete Store coverage | `capture_once` admits the next missing `agent.result` range | Complete immutable pages retained |
| Uncertain exact native effect with declared readback | `reconcile_once` admits one `agent.reconcile` | Target resolved or lookup settled `resolved:false` |
| Complete pages needing one candidate artifact | Ordinary `artifact.assemble` | Settled whole artifact or exact failure |
| Complete candidate available | `task.submit` | Candidate-specific submission applied or historical |
| Review missing | `review_once` | Independent assignment retained |
| Review requests changes | `task.request_changes` plus one correction `Send` | All actionable findings delivered together |
| Required evidence ready | `accept_once` calls ordinary `task.accept` | Candidate-specific acceptance |
| Accepted work and publication selected | Forge/GitHub ordinary effect | Exact remote readback, then configured closure/next work |

Automation receives no extra Task authority. Configuration affects future admissions;
it does not rewrite already admitted effects or evidence.

Result preservation and readback of uncertain admitted effects are correctness duties,
not optional automation products. Policy still decides whether taskless output is kept,
which complete artifact is submitted and whether accepted work is published.

## 3. Initial input, continuation and native ownership

Preserve `Attempt.start_owner`:

- `native_manager` reserves before native delegation and later binds the observed
  producer;
- `controller` permits one `task.dispatch`;
- the interval before producer registration never permits a second initial prompt.

A follow-up `Send`, correction, Goal continuation, Reply or steer is not a new initial
Dispatch. One native owner orders each session. Queue acceptance, native execution,
terminal output, complete capture, submission and Task acceptance are separate facts.

Task-linked input retains exact Task, Task revision and Attempt on its existing
Operation at admission. Result attribution follows that source Operation and its native
input/turn facts. Do not derive ownership from the newest Task on a session.

Some session input may be intentionally taskless. That is a distinct explicit policy
and authority, not a fallback when current Task/Attempt selection fails. A stale
unreleased Attempt must never grant accidental taskless Send authority. Participant
control is not widened merely to attach provenance.

## 4. Task revision and Attempt supersession

Task revision changes product intent. It does not mutate the immutable old Attempt,
its owner, source snapshot or already observed effects. The revision transaction must
classify pending work instead of leaving it to a later binding-level guess.

| Old-revision work at revision/supersession | Required treatment |
| --- | --- |
| Queued, unsent task-linked prompt/config/reply/Goal mutation | Cancel or mark superseded without native I/O. |
| Sending, native-accepted or outcome-unknown mutation | Preserve exact Operation; bounded readback/reconcile only. |
| Terminal outcome or late native evidence | Retain under the old Task/Attempt; never complete the new revision. |
| Missing complete result coverage | Keep or admit exact historical capture until bytes are durable or explicitly disposed. |
| Admitted `agent.result`, page delivery or reconcile | Keep deliverable until its exact completion settles. |
| Deliberately taskless session control | Follow its own explicit scope and lifecycle. |

Every new task-linked native effect rechecks:

```text
Task open + exact revision
+ Task.current_attempt_id
+ Attempt unreleased/current state
+ exact binding generation
+ current actor/object authority
```

Repair and Goal continuations already perform this style of exact revalidation; retain
that behavior when their duplicate workflow records are removed.

### Disposing superseded Attempts

Continuation authority and cleanup authority are different.

- Continuing/starting work requires the exact current Task revision and Attempt.
- The current scoped GM may reconcile and release a superseded historical Attempt after
  checking immutable Task/project scope and unresolved effects.
- Sent/unknown effects require readback before release.
- Unsent old-revision effects are cancelled by the supersession transition.
- Terminal execution does not imply that complete output bytes were captured.
- Original owner identity remains history; it is not rewritten to the current GM.
- Routine cleanup must not require the former Manager or local Operator.

A new claim is allowed only after the old Attempt has a valid disposition. Do not solve
this with mutable owner transfer, a second assignment ledger or forced process kill.

## 5. Queue, route and workspace

Rank corrections and unfinished work before new work, with project priority and stable
tie-breaking. Filter eligible rows before limiting or continue a bounded keyset scan;
blocked first-page rows must not hide later eligible work.

Reuse a compatible idle binding when selected policy permits. Idle, disconnected,
stale, terminal and dead are different. Drain stops new assignments while allowing
Reply, Result, Reconcile and capture. Rotation is an explicit post-disposition action,
not an age-based kill.

One Manager owns the mutable worktree and Git index/commit. Leaf writers may edit
non-overlapping assigned paths, but do not receive independent Git-index ownership.
Workspace leases coordinate Eliot writers; they are not an OS sandbox.

## 6. TaskPacket and candidate freeze

TaskPacket renders only what the model needs:

- goal and current phase;
- requirements and executable evidence plan;
- selected source revisions/content gaps;
- correction findings when applicable;
- write boundary and forbidden paths;
- deliverable and verification.

Store retains identity, authority and policy. Do not inject controller proof JSON into
the model context, and do not delete real acceptance criteria as bureaucracy.

Freeze the intended tree, baseline and relevant untracked files before review. Branch
name, model statement or one result page is not a frozen code candidate. Review of
candidate A remains about A after B exists.

A source snapshot is a valid code-candidate path. Reconstructing model prose is not an
extra prerequisite unless the Task explicitly requires that output.

## 7. Results, continuation output and complete capture

```text
source Operation
-> attributable native terminal/body
-> agent.result / module.result pages
-> immutable page artifacts
-> complete page set or artifact.assemble
-> task.submit
```

Use explicit native parent/input identity or an actually verified serialized input
window. Independent concurrent input or unexplained source gap invalidates
ordering-only association. Never select the newest assistant message.

The common result-origin path supports attributable task-linked `agent.send` as well as
the initial Dispatch. Extend its source-Operation model through:

- admission and retained Task/Attempt relation;
- adapter body lookup;
- sealed source validation;
- `validate_page_artifact` / `validate_candidate_origin`;
- submission authorization.

Do not change `Attempt.start_operation_id`, create a new Attempt or relabel a
continuation as the initial Dispatch. A steer may contribute to an existing native turn
instead of owning a separate terminal response; preserve that real relationship and do
not count the terminal twice.

### Current-state capture owner

No configured workflow step is needed. One bounded host pass derives work from current
Operations and artifacts:

1. Select an attributable terminal source with retained-result capability and no proven
   complete Store coverage.
2. Resolve its immutable Task/Attempt/source scope. Current assignment is required for
   new execution, not for finishing historical result capture.
3. Compute the first missing byte range from retained pages; do not persist another
   capture cursor.
4. Admit at most one ordinary `agent.result` with deterministic identity derived from
   source Operation + range.
5. After its settled readback, repeat for the next gap. A pending/unknown Result blocks
   only that source.
6. When coverage is complete, optionally assemble and submit under selected policy.

A page-created event is too late to trigger the first page. The correctness loop queries
current source Operations directly and coalesces duplicate wakeups.

### Historical completion authority

A Result admitted before revision/release already has a sealed completion path. The
same exact source authority is needed when complete capture starts only after terminal:

```text
terminal source Operation
+ immutable Task/Attempt/binding relation
+ native result identity
+ authorized current capture actor
-> bounded result read only
```

This grants no new prompt, Task mutation or completion of a later revision. Deliberately
taskless output follows explicit retention policy. Ambiguous source ownership remains
unknown.

### Page and body lifetimes

Native effect identity, source body and Store-delivery envelopes have separate
lifetimes:

- outcome ACK releases only its outcome envelope;
- page ACK releases only that page envelope;
- source body remains until exact Store-ACKed ranges cover `[0,total)` or explicit
  retention policy disposes it;
- an EOF page alone does not prove earlier coverage.

Persist with each source body only its immutable source identity, total/digest and the
union of ACKed ranges. ACK replay is idempotent. Do not create a transcript, generic
journal or Store-side duplicate of the bytes.

Retain body bytes under bounded storage before delivery. On pressure, stop admitting
new work and expose the storage fault; never evict an uncollected result to make room.
Partial or oversized capture remains explicitly incomplete.

Preserve the existing assembler: common source identity, ranges contiguous from zero,
no overlap/reordering/gaps, correct total/EOF, verified page bytes and full payload
digest. A valid empty body is not missing. Object authorization is separate from
content integrity. Metadata access does not grant bytes.

`artifact.assemble` returns an admission receipt. The settled `operation.get` outcome
proves assembly completion. Complete page coverage is already durable source capture;
assembly provides the convenient whole artifact and canonical whole-digest proof.

### Attempt release and late evidence

Release must continue blocking unresolved native effects/producers, but terminal alone
is not proof that output is safe to discard. Cancel unsent native mutations; preserve
or admit exact Result/Reconcile/capture work required for Task-linked terminal output.
Evidence arriving after supersession stays historical and cannot complete the new
Attempt.

## 8. Readback of uncertain effects

`reconcile_once` is a bounded current-state operation, not an AutomationStep:

1. Select an exact `sending` / `native_accepted` / `outcome_unknown` target whose route
   declares readback and has no active reconcile child.
2. Admit one deterministic ordinary `agent.reconcile`.
3. Adapter performs read-only native/local inspection; it never replays input.
4. If target evidence is found, record the target outcome first, then settle lookup
   `resolved:true`.
5. If inspection completed without evidence, settle lookup `resolved:false`; target
   remains unknown.
6. Retry only after relevant source epoch/cursor change or bounded due time. An
   unchanged inconclusive lookup does not spin.

Supervisor operation readback only tracks unresolved IDs/process lifecycle. It does not
settle targets and must not become a result-recovery daemon.

## 9. Review, correction and resubmission

Use one review slot bound to Task revision, Attempt and candidate. Retain sponsor,
assigned reviewer and actual author separately. Preserve existing replacement classes
for unanswered, inconclusive and returned-for-correction reviewers. A late old review
is historical, not the replacement verdict.

The auditor returns verdict, all actionable findings and requirement-linked evidence
in one result. Infrastructure failure or incomplete coverage is inconclusive, not pass
or code failure.

`task.request_changes` applies the exact current findings package. If the current owner
has a live binding, admit one next-turn correction in the same local action; otherwise
`needs_correction` remains current and the next resume renders the package. No separate
RepairDispatch backlog is required.

### Delivery is not correction success

Normalize the exact correction input facts:

| Observed fact | Meaning |
| --- | --- |
| Native input admission confirmed | Delivered; await terminal/output, never resend. |
| Attributable terminal observed | Execution reached terminal; retain success/failure/cancel separately and capture output. |
| Confirmed pre-input rejection or unsent cancellation | No native execution; policy may choose another authorized action. |
| Response/effect uncertain | Exact bounded readback; no blind retry. |

Do not require one provider-specific native input ID or completion string across all
harnesses. A terminal Rejected/Cancelled state may follow real execution and is not
proof of no effect. Remove duplicate RepairDispatch readers when common Operation facts
serve this decision.

The same unreleased `needs_correction` Attempt may submit the corrected candidate using
expected Task revision and prior submission. A published stale submission is retained
as history but not applied to current work. Do not create a second repair-submit state
or a replacement Attempt solely to submit correction output.

Bound no-progress rounds and cost. Escalate once to the Governor with concrete options:
narrow work, approved route change, worker/reviewer replacement, missing evidence or
deferral. Do not make the human perform routine Manager work.

## 10. Acceptance and publication

Manual `task.accept` and `accept_once` use the same candidate-specific evidence gate.
A review of A cannot accept B. Missing required checks, unresolved findings or an
unrelated new submission ID do not satisfy acceptance. An unchanged candidate may gain
relevant previously unavailable evidence.

Upload, push, merge, Issue closure and Task acceptance are distinct authorized effects.
An enabled policy may perform them automatically. Persist external intent before the
write; on timeout, read exact remote state before another mutation. A fresh request ID
does not bypass an unresolved target.

Collection, requirement import and external writes have different owners. Imported
source changes do not silently overwrite frozen working requirements. Preserve remote
identity, gaps and conflicts without creating an external work queue inside Eliot.

## 11. Restart, credential and old-boot delivery

A binding-scoped module credential is stable across adapter process restarts. The
successor normally uses the same deterministic module client identity and protected
credential file with a new process/boot identity. Missing registered credential files
fail closed; do not rotate silently or invent a second credential system.

After the old owner is proven departed, hello may install a new boot and mark unresolved
Operations for recovery. Retained outcomes/results created by the previous boot keep
their immutable origin boot. The new boot is only the authenticated delivery actor.

The successor-delivery boundary requires:

```text
same binding-scoped credential
+ new accepted boot/link
+ proven previous owner departure
+ retained original Operation execution boot
+ complete immutable old-boot delivery bytes
```

Store records evidence for the original Operation and deduplicates exact delivery. It
grants no native replay, no new command and no transfer to another Attempt. Ordinary
live outcomes still require current-boot identity; recovery is an explicit narrow path,
not a weakened validator. Missing or partial bytes remain unknown.

Supervisor startup recovery provides status-only unresolved IDs. Adapter startup loads
its own complete private captures before native open; Store settlement and page owners
validate them. Do not route native bytes through supervisor automation/Hook journals.

Control-link failure alone is not native cancellation. Continue reducing native events
and capturing terminal output while Store IPC is unavailable. Retry only saved Store
deliveries. Hold new work on delivery/storage pressure; do not close native stdin or
forget terminal facts because an observation could not be posted.

## 12. Connected verification

A complete connected proof covers:

1. initial assignment and exactly one input owner;
2. question/Reply;
3. automatic terminal capture from offset zero through complete coverage;
4. candidate A submission and independent review with all findings;
5. one correction Send and attributable correction output;
6. same-Attempt candidate B submission;
7. replacement reviewer where required;
8. acceptance and selected publication;
9. Task revision while old Send is queued/sent/unknown;
10. release that cancels unsent effects but preserves/finishes output capture;
11. GM handover and cleanup of superseded foreign Attempt without human fallback;
12. IPC disconnect, adapter restart, old-boot delivery and exact dedupe;
13. complete inconclusive Reconcile that settles the lookup without retry spin;
14. unrelated work progressing while one effect remains unknown.

During construction run formatting and minimal affected warnings-denied Clippy/build.
Focused connected cases run when a vertical slice closes; installed Windows/native/load
qualification follows the completed product path. Documentation and synthetic fixtures
alone do not prove runtime behavior.
