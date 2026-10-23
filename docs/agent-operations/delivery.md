# Delivery — autonomous work through ordinary commands

Target cycle, not a claim that all handoffs are implemented.
[Implementation](implementation.md) owns source changes;
[status](../implementation-status.md) records landed behavior;
[audit register](audit-register.md) records evidence and gaps.
Governor and Manager are agent roles. Routine work must not fall back to a human.

## 1. One connected cycle

```text
select current work
-> claim/resume one Attempt
-> admit one initial input owner
-> capture attributable complete output
-> submit frozen candidate
-> independent review
-> return all findings together
-> correct and resubmit
-> accept through one evidence gate
-> selected publication
-> next eligible work
```

Task, Attempt, Operation, binding, candidate, review and artifact records already own
this cycle. Helpers query current facts and call the same Store handlers. No second
workflow ledger, repair queue, continuation registry, result queue or model heartbeat.

`explicit` means attributable Store intent, not a manual click. Unknown effects block
only their exact resource while unrelated work continues.

## 2. Direct and automatic equivalents

| Current fact | Ordinary action | Completion / next decision |
| --- | --- | --- |
| Eligible work plus route/workspace capacity | `dispatch_once` | One initial owner and admitted Operation |
| Native question/permission | Native rule or Governor `Reply` | Exact request answered |
| Terminal source with incomplete Store coverage | `capture_once` → next `agent.result` range | Complete immutable pages |
| Uncertain effect with declared readback | `reconcile_once` → `agent.reconcile` | Target resolved or lookup settled inconclusive |
| Complete pages needing one artifact | `artifact.assemble` | Whole artifact or exact failure |
| Complete candidate | `task.submit` | Current or historical submission |
| Missing review | `review_once` | Independent assignment |
| Changes requested | `task.request_changes` + one correction `Send` | All findings delivered together |
| Required evidence ready | `accept_once` → `task.accept` | Candidate-specific acceptance |
| Accepted work + publication policy | Forge/GitHub effect | Exact remote readback and selected closure |

Result preservation and uncertain-effect readback are correctness duties. Policy still
decides which taskless output to retain, which complete candidate to submit and whether
to publish.

## 3. Initial input and continuation

Preserve `Attempt.start_owner`:

- `native_manager` reserves before native delegation and later binds producer;
- `controller` permits one `task.dispatch`;
- the pre-bind interval never permits a second initial prompt.

Follow-up Send, correction, Goal continuation, Reply or steer is not another initial
Dispatch. One native owner orders each session.

Task-linked input retains exact Task/revision/Attempt/binding on its ordinary Operation.
Result attribution follows that source Operation and native input/turn facts, not the
newest Task on a session.

Some session input may be deliberately taskless. That requires explicit authority and
retention policy. It is not the fallback when current Task selection fails.

## 4. Task revision and supersession

Task revision changes intent but does not rewrite the immutable old Attempt or observed
effects. The revision transaction classifies pending work immediately.

| Old-revision work | Disposition |
| --- | --- |
| Queued unsent task-linked prompt/config/reply/Goal | Cancel/supersede without native I/O |
| Sending/native-accepted/outcome-unknown mutation | Preserve exact Operation; readback only |
| Terminal/late evidence | Keep under old Task/Attempt; never complete new revision |
| Incomplete Store output coverage | Preserve/admit exact historical capture |
| Admitted Result/Reconcile/page delivery | Keep deliverable until exact completion |
| Explicit taskless control | Own scope/lifecycle |

Every new task-linked native effect rechecks Task open/revision/current Attempt,
unreleased Attempt state, binding generation and actor/object authority.

### Cleanup authority

Continuing current work and cleaning historical work are different:

- continuation/start requires exact current Task/Attempt;
- current scoped GM may reconcile/release a superseded Attempt after checking
  Task/project, immutable Attempt and unresolved effects;
- sent/unknown effects require readback;
- unsent effects are cancelled by supersession;
- terminal does not prove complete output capture;
- historical owner identity remains unchanged.

Routine cleanup must not require former Manager or local Operator.

## 5. Queue, route and workspace

Rank corrections and unfinished work before fresh work, with project priority and
stable tie-break. Filter eligible rows before limit or continue bounded keyset scan;
blocked first-page rows cannot hide later work.

Reuse compatible idle binding when selected policy permits. Idle, stale, disconnected,
terminal and dead differ. Drain stops new assignments while allowing Reply, Result,
Reconcile and capture. Rotation is post-disposition, never age-only kill.

One Manager owns mutable worktree and Git index/commit. Leaf writers may edit disjoint
paths but do not get independent Git-index ownership. Leases coordinate Eliot writers;
they are not an OS sandbox.

## 6. TaskPacket and frozen candidate

TaskPacket contains goal, requirements/evidence plan, selected source revisions and
gaps, current findings, write boundary, deliverable and verification. Store retains
identity/authority. Do not place proof JSON in model context or remove real criteria as
bureaucracy.

Freeze intended tree, baseline and relevant untracked files before review. A branch
name, model claim or one page is not a frozen code candidate. Review A stays about A
after B exists. Source snapshot is a valid code candidate; model prose is required only
when the Task says so.

## 7. Complete result capture

```text
source input Operation
-> attributable native terminal/body
-> agent.result / module.result pages
-> Store-authoritative complete coverage
-> optional artifact.assemble
-> task.submit
```

Use explicit native parent/input identity or a verified serialized input window.
Concurrent independent input or source gap invalidates ordering-only association.
Never choose the newest assistant message.

Dispatch and attributable task-linked Send use the same source-Operation path through
admission, adapter lookup, source validation, page/candidate validation and submission.
Do not rewrite `Attempt.start_operation_id`, create another Attempt or relabel Send as
Dispatch. Steer may contribute to an existing turn instead of owning another terminal.

### `capture_once`

One bounded host pass:

1. Finds attributable terminal source with result capability and incomplete Store
   coverage.
2. Resolves immutable Task/Attempt/source scope. Current assignment is for new
   execution, not historical completion.
3. Reads first missing range from Store pages.
4. Admits one deterministic `agent.result` for source + range.
5. Waits for settled/unknown readback before another range.
6. On complete coverage, optionally assembles/submits under policy.

No capture cursor/table/daemon. Page-created events cannot trigger the first page; the
query begins from source Operations and pages.

### Historical completion

A first Result may be admitted after revision/release from immutable terminal source:

```text
source Operation + exact Task/Attempt/binding
+ native result identity + current authorized capture actor
-> bounded result bytes only
```

No prompt, Task mutation or completion of newer revision. Ambiguous source remains
unknown; taskless output follows explicit retention policy.

### Ready Store delivery

Before IPC, adapter atomically saves exact `module.result` params including ready page
bytes. ACK loss retries identical params only. Store replay ACK deduplicates. This is
separate from native reading/effect recovery.

### Source lifetime and coverage

Result source may be local body/file or provider locator. Store owns aggregate page
coverage. Exact ACK/replay returns registered artifact plus:

```text
coverage_complete
next_missing_offset / next_missing_length
```

Adapter keeps source until complete coverage or explicit disposal. It does not maintain
another ACK-range union. Outcome ACK releases only outcome envelope; page ACK releases
only page envelope; EOF alone is not full coverage.

No-history routes persist body before it is the only copy. Provider-history routes keep
stable locators/digest and capture promptly before provider retention expires. Storage
pressure stops new work; never evict uncollected output.

Preserve existing assembler: same source identity, ranges contiguous from zero, no
gaps/overlap/reordering, correct total/EOF, verified page bytes and full digest. Empty
complete body is valid. `artifact.assemble` returns admission receipt; settled
`operation.get` proves completion.

## 8. Readback of uncertain effects

### `reconcile_once`

1. Select exact unresolved target with declared readback and no active reconcile child.
2. Admit one deterministic `agent.reconcile`.
3. Adapter performs read-only inspection, never native replay.
4. Found evidence settles target first, then lookup `resolved:true`.
5. Completed inconclusive inspection settles lookup Applied `resolved:false`; target
   remains unknown.
6. Retry only after relevant source epoch/cursor change or bounded due time.

Unchanged inconclusive lookup coalesces. Supervisor readback remains process/status
custody and does not settle semantic targets.

## 9. Review, correction and resubmission

Use one review slot bound to Task revision, Attempt and candidate. Retain sponsor,
reviewer and actual author separately. Preserve unanswered/inconclusive/
returned-for-correction replacement. Late old review is historical.

Auditor returns verdict, all actionable findings and requirement-linked evidence in one
result. Infrastructure failure/incomplete coverage is inconclusive, not pass or code
failure.

`task.request_changes` applies exact current findings package. With live owner, admit
one next-turn correction in same local action; otherwise `needs_correction` remains for
next resume. No RepairDispatch backlog.

Delivery facts:

| Fact | Meaning |
| --- | --- |
| Native input admitted | Delivered; await terminal/output; never resend |
| Attributable terminal | Execution ended; preserve success/failure/cancel and capture output |
| Confirmed pre-input rejection / unsent cancellation | No native execution; policy may choose another action |
| Uncertain effect | Exact readback; no blind retry |

Do not require one provider-specific input ID/completion string. Rejected/Cancelled may
follow real execution and is not automatically “no effect”.

Same unreleased `needs_correction` Attempt may submit corrected candidate against
expected revision/prior submission. Stale publication stays historical. No second
repair-submit state or replacement Attempt solely for result submission.

Bound no-progress/cost and escalate once to Governor with concrete options, not a human
routine-work queue.

## 10. Acceptance and publication

Manual `task.accept` and `accept_once` use one candidate-specific evidence gate. Review
of A cannot accept B. Missing checks/unresolved findings/new unrelated ID do not pass;
unchanged candidate may gain relevant evidence.

Upload, push, merge, Issue closure and acceptance are distinct authorized effects which
policy may automate. Persist external intent before write; timeout requires exact
remote readback before another mutation. Fresh request ID does not bypass unresolved
state.

Collection, requirement import and writes have different owners. Imported changes do
not silently overwrite frozen working requirements.

## 11. Restart and old-boot delivery

Binding-scoped credential remains stable across adapter restarts. Successor uses same
module identity with new process/boot. Do not rotate silently.

After old owner departure is proven, new boot may deliver complete retained records but
is not their execution origin:

```text
same binding credential
+ accepted new boot/link
+ proven prior-owner departure
+ retained original Operation execution origin
+ complete immutable delivery bytes
```

Store records original evidence and deduplicates. No native replay, new command or
transfer to another Attempt. Ordinary live outcome still requires current boot; narrow
recovery keeps old origin boot immutable. Missing/partial bytes remain unknown.

Adapter startup loads private captures before native open. Supervisor supplies
status-only unresolved IDs, not native bytes. Control-link failure alone is not native
cancellation: continue native reduction/capture, retry only saved Store deliveries and
hold new work on pressure.

## 12. Connected verification

A complete proof covers:

1. exactly one initial input owner;
2. question/Reply;
3. automatic complete capture from first missing range to Store-complete coverage;
4. candidate A, independent review and all findings;
5. one correction Send and attributable correction output;
6. same-Attempt candidate B;
7. reviewer replacement where needed;
8. acceptance and selected publication;
9. Task revision with queued/sent/unknown old input;
10. release cancelling unsent effects but preserving/finalizing capture;
11. GM cleanup of superseded foreign Attempt without human fallback;
12. IPC disconnect, restart, old-boot delivery and exact dedupe;
13. inconclusive Reconcile settling lookup without spin;
14. unrelated work progressing while one target is unknown.

During construction run format and minimal affected warnings-denied Clippy/build.
Focused connected cases run when a vertical path closes; installed Windows/native/load
qualification follows the completed product. Documentation is not runtime proof.
