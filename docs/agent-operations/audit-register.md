# Audit register

Durable resume point for the source audit. It records the inspected commit, exact
paths, confirmed/refuted findings, preserved mechanisms and the next untraced chain.
It is not another runtime registry, implementation queue or approval gate.
[Implementation](implementation.md) remains the work order;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / automatic result capture, historical admission and body release**.
- Inspected runtime head: **`f5ccc418ba401733d1f778619abeec5c0554416b`**.
- Overall audit: **incomplete**. No measured completeness percentage is claimed.
- Newly traced:
  - warm terminal settlement through `producers::record_task_dispatch` and
    `record_warm_terminal`;
  - `attempt.release` unresolved-effect/producer gates and queued-operation cancellation;
  - normalized-result admission versus already-admitted page completion;
  - Store automation actions and current automatic consumers;
  - module-supervisor status readback and startup recovery projection;
  - Antigravity result-page ACK handling and response-body eviction.
- New findings: **AR-020–AR-024**.
- **Next exact chain:** compare result/delivery ownership in current Codex, OpenCode,
  Command, Muse and Claude routes: source-body lifetime, page/outcome outbox, ACK
  coverage, restart import and release. Then define the route-independent
  `capture_once` / `reconcile_once` Store query and exact historical object authority.
  Reuse existing adapter mechanisms only where they delete route-local code.
- This pass changes documentation only. No runtime code, Clippy, tests, installed
  harness or independent verifier ran. Source tests and historical receipts are not
  new executed evidence.

On resume compare changed source paths with this head. Recheck changed callers and
consumers, not the entire archive. Update this register with the owning target
contract. An interrupted chat does not clear findings or make inherited claims current.

## Evidence labels

- **source-confirmed**: the named path was read and the consequence follows from it;
  this is not live reproduction.
- **preserve**: useful current behavior, not a deletion target.
- **partial**: the exact untraced boundary is named.
- **inherited**: earlier source evidence not rechecked in this pass.

Source review, implementation and executed verification are separate. A search hit is
not a completed trace; reading a test is not running it.

## Findings and coverage

| ID | Inspected requirement/path | Evidence and disposition | Remaining boundary |
| --- | --- | --- | --- |
| AR-001 | Antigravity report failure → native shutdown | **Source-confirmed, open.** Report retry runs in the terminal reducer; failure reaches stdin shutdown and drained facts can remain memory-only. | Independent bounded Store delivery and continuous native draining. |
| AR-002 | Complete result retention | **Source-confirmed, open.** The 32-response cache can evict the only uncollected body. | Durable body lifetime, admission pressure and complete-capture release. |
| AR-003 | Exact result identity | **Preserve.** Exact Operation/receipt/conversation/generation checks and explicit unavailable oversized output. | Keep causal identity while deleting duplicate package proofs. |
| AR-004 | Adapter restart without native replay | **Source-confirmed gap, open.** New Antigravity Controller has empty body/delivery caches. | Original-Operation successor delivery. |
| AR-005 | Previously admitted page completion | **Preserve.** Sealed normalized origin permits narrow late page completion and exact replay ACK. | First-time historical admission and partial storage. |
| AR-006 | Complete continuation result | **Source-confirmed, open, route-scoped.** Generic origin/candidate validators require initial `task.dispatch`; strict Antigravity retains Send output but exposes status only. | Extend the same source-Operation/page/candidate path to attributable Send. |
| AR-007 | Assembly and artifact reads | **Preserve.** Publish/register separation, contiguous coverage, digest checks and Bytes authorization. | Crash/power-loss and remaining grant/cache races. |
| AR-008 | Authentication versus admitted completion | **Source-confirmed boundary.** Disabled credentials fail new handshake; an authenticated carrier has narrow prior-page completion. | Revocation and successor delivery. |
| AR-009 | Continuation Task/Attempt attribution | **Source-confirmed, mixed.** Selected Manager, Repair and Goal paths retain scope; binding-only input may be deliberately taskless. | Make task-linked versus taskless authority explicit. |
| AR-010 | Repair delivery readback | **Source-confirmed defect, open.** Repair reader requires one provider-specific input ID/condition and treats all Rejected/Cancelled as no effect. | Consume common admission/terminal facts. |
| AR-011 | Correction submission | **Preserve.** Same unreleased `needs_correction` Attempt can resubmit; stale publication remains historical. | Extend continuation origin through candidate validators. |
| AR-012 | Task revision fences input | **Source-confirmed defect, open.** Revision preserves old Attempt; direct Send can become accidentally taskless or dispatch without current-revision recheck. | Supersede unsent old-revision effects and recheck exact Task/Attempt. |
| AR-013 | GM disposes superseded work | **Source-confirmed defect, open.** Current GM can revise beneath another owner but then cannot release the old Attempt. | Separate current execution authority from scoped historical cleanup. |
| AR-014 | Release preserves result capture | **Source-confirmed defect, open.** Release cancels queued `agent.result` with every non-check Operation. | Classify cancellation by effect semantics. |
| AR-015 | Module credential on restart | **Preserve.** Binding-scoped client/credential is reused; implicit rotation is forbidden. | Live restart/revocation qualification. |
| AR-016 | Old-boot first delivery | **Source-confirmed defect, open.** Unseen old-boot terminal fails current-boot validation; exact already-committed outcome may receive earlier replay ACK. | Separate immutable execution origin from delivery actor. |
| AR-017 | Capture while execution is recovery-fenced | **Source-confirmed defect, open.** `recovery_required` filters sealed Result before completion-only exception. | Permit exact completion/readback without declaring session ready. |
| AR-018 | Supervisor recovery meaning | **Preserve/correction.** `module.supervisor.recovery.reconcile` handles automation/Hook intake, not native outcomes. | Keep native import in adapter/Store owners. |
| AR-019 | Complete inconclusive readback | **Source-confirmed defect, open.** Antigravity Reconcile reports its own lookup Unknown even after a completed inconclusive inspection. | Settle lookup `resolved:false`; retain target unknown and bounded retry. |
| AR-020 | Terminal evidence versus Attempt release | **Source-confirmed boundary.** Warm dispatch terminal can update its immutable old-revision Attempt while unreleased; release blocks unresolved Operations/producers. Once terminal disposition is recorded, release no longer requires complete response capture. | Add complete-output obligation without weakening producer/release fences. |
| AR-021 | First historical result admission | **Source-confirmed defect, open.** New normalized Result requires current open unreleased Task/Attempt; only a Result admitted earlier can finish after release/revision. | Admit capture from immutable terminal source Operation and historical object scope. |
| AR-022 | Automatic Result/Reconcile owner | **Source-confirmed defect, open.** Current automation steps/consumers create neither `agent.result` nor `agent.reconcile`; Doctor only recommends an explicit command. | One bounded current-state host loop using ordinary Operations; no new workflow ledger. |
| AR-023 | Supervisor operation readback | **Source-confirmed scope correction.** Supervisor readback retains unresolved IDs and lifecycle gates only; it neither settles targets nor creates Result/Reconcile. Antigravity constructs a fresh Controller without consuming startup recovery records. | Use supervisor for custody/demand only; connect semantic recovery in Store/adapter. |
| AR-024 | Page ACK and source-body release | **Source-confirmed defect, open.** Antigravity checks only `recorded:true`; it records no ACKed byte ranges and has no source-release path. Bodies disappear only through count eviction/truncation. | Track exact Store-ACKed coverage and release only after full durable coverage. |

## Current pass details

### AR-020 — terminal settlement is not complete output capture

Sources:

- `store/producers.rs::record_task_dispatch` and `record_warm_terminal`;
- `store/tasks.rs::release`;
- `runtime::outcome_with_artifacts`;
- `modules/antigravity-rust/src/controller.rs::terminal_outcome`.

Warm dispatch settlement updates the exact retained producer on its immutable Attempt.
It deliberately does not require the Task to remain the current revision, so late
terminal evidence for a revised Task can remain historical. Preserve this. It does
require the Attempt to remain unreleased; normal release separately blocks unresolved
Operations and unresolved producer dispositions. Do not “fix” recovery by allowing
arbitrary producer mutation after release.

The missing condition is output capture. A terminal producer disposition is enough for
release even when the only complete response body still lives in adapter memory. For a
terminal `agent.send`, Store retains the terminal on the source Operation and creates no
new Attempt producer, so the same distinction is even clearer.

Target facts remain separate:

```text
native input admitted
native execution terminal
complete result bytes durably captured
candidate submitted
Task accepted
```

Release may dispose execution ownership only after exact unresolved effects are handled;
it must not silently destroy a pending complete-output obligation for Task-linked work.

### AR-021 — late completion works only if Result was already admitted

Sources:

- `store/normalized_result.rs::admitted_origin` and
  `validate_admitted_operation`;
- `store/runtime.rs::admitted_result_scope`;
- `store/tasks.rs::release`.

`admitted_origin` requires the Task to be open at the Attempt revision, the Attempt to
be current and unreleased, and the target to be the first normalized Dispatch producer.
After that origin is sealed, page validation intentionally no longer requires current
Task/binding lifecycle and exact pages can finish or replay. This is a good split, but
it leaves no way to create the first capture Operation after revision/release.

The compact replacement uses the immutable terminal source Operation:

1. Task-linked terminal fact identifies source Operation, Task, Attempt, binding and
   native result identity.
2. `capture_once` derives the next missing bounded range and admits one ordinary
   `agent.result` under historical completion authority.
3. Historical authority grants only result bytes for that source; it cannot send input,
   mutate the old Attempt or complete a newer Task revision.
4. Existing `module.result`, page registration, replay, assembly and artifact grants
   remain the only byte path.

No continuation registry or second result database is needed.

### AR-022 — autonomous capture/readback is absent

Sources:

- `automation/actions.rs::AutomationStep` and `supported_action_for`;
- Store automation source search for `agent.result` / `agent.reconcile`;
- `Store::reconcile_automations_once`;
- `doctor.rs` outcome-unknown recommendation;
- `automation_intake` / `native.result.available`.

Current automatic domains cover work/review/repair/acceptance/publication/check/GitHub/
Goal/script, but none owns result-page creation or uncertain native-effect readback.
`native.result.available` is emitted only after a page was already registered, so it
cannot trigger the first page. Doctor is read-only and tells a caller to issue
`agent.reconcile`; this is diagnosis, not autonomous recovery.

Add no optional `ResultCapture` workflow step. Result preservation and uncertain-effect
readback are correctness obligations of the existing host loop:

```text
capture_once:
  terminal attributable source without complete Store coverage
  -> derive first missing range from current pages
  -> admit one deterministic agent.result
  -> repeat only after settled page readback
  -> assemble/submit only when selected policy requires it

reconcile_once:
  unresolved target with declared exact readback and no active reconcile child
  -> admit one deterministic agent.reconcile
  -> complete lookup as resolved:true or resolved:false
  -> retry only after source epoch/cursor change or bounded due time
```

A complete inconclusive lookup settles its own Operation and leaves the target unknown.
One target has at most one active capture range and one active readback. Unrelated work
continues. The loop queries current state; it stores no copied pending subjects/cursors.

### AR-023 — supervisor readback is custody, not semantic recovery

Sources:

- `store/module_demand.rs::scoped_operation_readback` and
  `startup_recovery_projection`;
- `swarm-supervisor::SupervisorRegistry::apply_operation_readback` and
  `Service::apply_readback`;
- `modules/antigravity-rust/src/main.rs::controller_from_hello`.

The Store projection contains bounded status-only unresolved IDs. Supervisor replaces
its unknown-ID set, clears/retains lifecycle readback gates and may keep a worker runner
attached. It has no inputs, result bytes or authority to settle an Operation.
Antigravity hello currently constructs a fresh in-memory Controller from binding/root
identity and does not restore those unresolved records into result/outcome delivery.

Preserve supervisor process custody and demand. Do not turn it into a universal result
importer. Adapter startup loads its private complete captures; Store owns historical
settlement and page admission; the host correctness loop owns current-state scheduling.

### AR-024 — ACKed page coverage is not retained

Sources:

- Antigravity `main.rs` `agent.result` branch;
- `store/results.rs::record`;
- Antigravity `stream.rs::retain_response`;
- Antigravity Controller completed-turn retention.

Store registers one immutable page and returns `recorded:true` plus its artifact ref.
The adapter knows that page's source, offset, byte length and total from the exact
request it built, but currently discards the ACK after checking `recorded`. No method
records acknowledged ranges or removes the source body after full capture. The only
normal deletion is the 32-entry eviction; oversized/truncated bodies are dropped.

The minimal local record is per retained source body:

```text
source Operation / body digest / total bytes
+ union of exact Store-ACKed [offset,end) ranges
```

ACK replay is idempotent. Release the body only when the union covers `[0,total)` or an
explicit retention policy disposes it. EOF without earlier ranges is insufficient.
Assembly is optional after complete page coverage; its existing verifier remains the
canonical whole-artifact path. Persist the coverage with the body in A2 so restart does
not forget which bytes Store accepted. This is not a transcript or general journal.

## Preserved boundaries from this pass

- Late terminal evidence may remain attached to the immutable old Attempt while it is
  unreleased; it must never complete the new Task revision.
- Release must continue blocking unresolved native effects and producers.
- Already-admitted normalized pages remain finishable through their narrow completion
  authority and exact replay ACK.
- Supervisor status readback remains bounded and payload-free.
- Page publication/registration and assembly remain the Store byte authority.

## Inspection log for this pass

| Source | Inspected portion |
| --- | --- |
| `store/producers.rs` | normalized dispatch producer creation, terminal update and release guards. |
| `store/tasks.rs` | release unresolved checks and broad queued-operation cancellation. |
| `store/normalized_result.rs` | current admission versus immutable admitted-page validation. |
| `store/runtime.rs` | ordinary/admitted result scopes and outcome settlement ordering. |
| `store/results.rs` | page registration, replay ACK, result event and next-offset receipt. |
| `automation/actions.rs` | all configured automatic steps and supported actions. |
| Store automation sources | no `agent.result` or `agent.reconcile` action consumer. |
| `doctor.rs` | read-only manual reconciliation recommendation. |
| `store/module_demand.rs` | unresolved status projection and startup recovery DTO. |
| `swarm-supervisor/supervisor.rs` | status-only operation readback application. |
| Antigravity `main.rs` | fresh Controller startup, result-page send/ACK handling. |
| Antigravity `controller.rs` / `stream.rs` | completed-turn/body ownership and count eviction. |

## Remaining coverage map

| Area | Next unclosed boundary |
| --- | --- |
| Antigravity A | Implement/verify AR-001/002/004/006/010/016/017/019/021/024 and delete proof packet after positive path works. |
| Cross-adapter result delivery | Compare Codex/OpenCode/Command/Muse/Claude ACK, body lifetime, restart and release; extract only proven shared code. |
| Queue/routes/provider/capacity B | Root reservation, fairness and shared-account source scope. |
| Workspace/process C–D | Shared worktree release, PID reuse and exact successor custody. |
| Scheduler/Goals E | One correctness loop plus policy actions; due dedupe and drain. |
| TaskPacket/review F–G | Frozen evidence plan, reviewer replacement, all-findings correction and common acceptance. |
| Permissions H | Historical result completion and explicit taskless Send object authority. |
| Store formats I | Reader/writer pairs, numeric migration and partial-upgrade/corruption cases. |
| IPC/MCP J | Slow consumers, disconnect, replay/live boundary and writer occupancy. |
| Check/Script/Forge/GitHub K | Exact effect owner and ambiguous-write readback. |
| Other adapters M–Q | Selected installed contract, one executor and connected result/restart path. |
| Governor R | Provider/OS/Git source join and unbound inventory. |
| Deletions S | Positive replacement and unsettled-data disposition for every removed product. |
| Mailbox/GitHub/gateway T–V | Scoped recipient/read, external write recovery and remote limits. |
| Installed/Windows/load | Separate unexecuted qualification; not a substitute for source audit. |

## Earlier detailed traces

- [result retention/control failure at `c3e331a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md)
- [continuation result/assembly at `70b99db3`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md)
- [continuation attribution/repair at `1c61629b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md)
- [revision/release/old-boot at `ec019052`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/docs/agent-operations/audit-register.md)
- [successor delivery/readback at `f5ccc418`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f5ccc418ba401733d1f778619abeec5c0554416b/docs/agent-operations/audit-register.md)

A finding remains open until a runtime change and exact evidence are linked. A docs
commit is not a repair. Preserve refuted claims and useful mechanisms so later agents
do not rebuild the same bureaucracy or delete a working boundary.
