# Audit register

Durable resume point for the source audit. It records the inspected commit, exact
paths, confirmed/refuted findings, preserved mechanisms and the next untraced chain.
It is not another runtime registry, implementation queue or approval gate.
[Implementation](implementation.md) remains the work order;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / successor delivery, recovery queue and readback completion**.
- Inspected head: **`ec0190526508af1daf0da3a1aae51ee425b6ab04`**. Its diff from
  `1c61629b` changes only audit-register/delivery documentation. Earlier source evidence
  remains unchanged; that comparison does not constitute a new audit of those paths.
- Overall audit: **incomplete**. No measured completeness percentage is claimed.
- Newly traced: `module.outcome` scope, receipt/recovery-link validation, duplicate ACK,
  terminal settlement; Antigravity boot validation; hello boot replacement; the
  `module.next` recovery predicate; supervisor recovery routing; adapter startup and
  `Controller::read_command` reconciliation.
- New rows: **AR-017–AR-019**. AR-016 is narrowed: first-time old-boot import is blocked,
  but exact already-recorded outcomes have an earlier replay-ACK path when preceding
  scope/receipt checks succeed. Do not reintroduce the blanket rejection claim.
- **Next exact chain:** retained outcome -> `producers::record_task_dispatch` /
  `record_warm_terminal` -> superseded or released Attempt; then missing historical
  `agent.result` admission -> `normalized_result::admitted_origin` -> page registration
  -> automatic capture/release. Establish how to retain original evidence without
  requiring a current Task or leaving a second result collector. Also trace whether
  any current consumer automatically schedules/coalesces Antigravity Reconcile calls;
  AR-019 does not claim a reproduced retry loop.
- No runtime code, Clippy, tests, installed harness or independent verifier ran.
  Documentation review/publication uses the GitHub connector; local clone failed DNS.

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
| AR-001 | Antigravity report failure → native shutdown | **Source-confirmed, open.** Report retry runs in the only terminal-wait reducer; failure reaches stdin shutdown, and facts observed during drain can remain only in memory. | Independent bounded Store delivery and continuous native draining; host-down case. |
| AR-002 | Complete result retention | **Source-confirmed, open.** The 32-response cache can evict the only uncollected body on the 33rd completion. Outcome ACK is not body capture. | Durable source-body lifetime, pressure before new admission and complete-capture release. |
| AR-003 | Exact result identity | **Preserve.** Operation/receipt/conversation/generation checks and explicit unavailable oversized bodies. | Keep causal identity while deleting duplicate package proofs. |
| AR-004 | Adapter restart without native replay | **Source-confirmed gap, open.** A new Antigravity Controller has empty response/delivery caches. | Original-Operation successor delivery; no replay or origin relabeling. |
| AR-005 | Previously admitted normalized-page completion | **Preserve.** Exact registered module tuple and sealed origin allow narrow late page completion without ordinary current-link/release gating; identical registered pages receive replay ACK. | Successor delivery and partial-storage cases; AR-017 shows a separate earlier queue barrier. |
| AR-006 | Complete continuation results | **Source-confirmed, open, route-scoped.** Generic origin and candidate validators require the initial `task.dispatch`; strict Antigravity can retain a Send body but its public fallback exposes status only. | Extend the same source-Operation/page/candidate path to attributable continuation input. |
| AR-007 | Full assembly and artifact reads | **Preserve.** Page publication/registration, contiguous range and digest checks, complete assembly and Bytes-level authorization. | Grant/cache races and crash/power-loss qualification. |
| AR-008 | Authentication versus finishing admitted output | **Source-confirmed boundary.** Disabled credentials fail a new handshake; an already-authenticated carrier has a narrow path to finish its prior page. | Revocation side effects and post-restart delivery. |
| AR-009 | Continuation Task/Attempt attribution | **Source-confirmed, mixed.** Selected Manager, Repair and Goal paths can write Operation scope; ordinary binding-level sends may be deliberately taskless. | Require an explicit distinction instead of accidental fallback. |
| AR-010 | Repair delivery readback | **Source-confirmed predicate mismatch, open.** Repair readers require applied + native input ID + `native_input_admitted`; Antigravity terminal facts use another condition and no native input ID. Rejected/cancelled is also treated as no effect even after execution. | Consume common admission/terminal facts; distinguish pre-input rejection from executed terminal failure. |
| AR-011 | Correction submission and historical recovery | **Preserve.** The same unreleased `needs_correction` Attempt may submit against expected revision/prior submission; stale published submissions remain historical. | Extend continuation origin through existing candidate validators. |
| AR-012 | Task revision fences new work | **Source-confirmed defect, open.** `task.revise` preserves the old Attempt and does not classify its queued Operations. Direct Manager Send can fall through binding-level ownership with no current Attempt, becoming taskless; queued task-linked Send can dispatch without a Task-revision recheck. Repair/Goal perform current-subject checks. | Supersede unsent old-revision mutations and recheck exact Task/Attempt before task-linked effects. |
| AR-013 | Current GM can dispose superseded work | **Source-confirmed defect, open.** Revision beneath another Manager's live Attempt makes foreign-GM `require_attempt_control` fail while `task.claim` sees the unreleased Attempt. | Separate continue/start authority from scoped historical reconcile/release authority; preserve original owner. |
| AR-014 | Release preserves admitted result capture | **Source-confirmed defect, open.** Release cancels queued non-check Operations including admitted `agent.result`; normalized validators reject cancelled and closed-binding selection requires queued. | Cancel unsent native mutations, not required result/readback/capture. |
| AR-015 | Module credential across process restart | **Preserve.** Binding-scoped client/credential is reused and implicit rotation is forbidden. A successor normally has the same credential and a new boot. | Live restart and explicit revocation policy. |
| AR-016 | Old-boot terminal delivery | **Source-confirmed defect, open, narrowed.** An unseen terminal from the departed boot fails current-boot validation. An identical already-committed outcome can reach an earlier replay ACK after scope/receipt checks. | Separate delivery actor from immutable execution origin; survive multiple successor boots without using only the mutable previous-boot field. |
| AR-017 | Capture while native execution is recovery-fenced | **Source-confirmed defect, open.** `next_internal` selects only Recover/Reconcile while `recovery_required`; even sealed, previously admitted Result is filtered before its completion exception. | Permit exact completion-only result/readback without declaring the native session ready. |
| AR-018 | Meaning of supervisor recovery | **Source-confirmed scope correction / preserve.** `module.supervisor.recovery.reconcile` reconciles automation/Hook intake, not adapter files or native terminals. Startup already spawns native only on Open. | Connect capture loading and Store completion in existing adapter/result owners, not that event journal or another daemon. |
| AR-019 | A completed lookup is not an unknown lookup | **Source-confirmed mismatch, open.** Antigravity Reconcile always emits Unknown for the lookup Operation, even when its local inspection completed. Target uncertainty and readback completion are conflated. | Settle the lookup with `resolved:false` when complete but inconclusive; retain the target unknown and bounded retry policy. Automatic retry-loop reachability remains untraced. |

## Retained revision/release trace

The following AR-012–AR-015 evidence was established at `1c61629b`; source is unchanged
at this pass. Full prior notes remain in the [preceding register](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/docs/agent-operations/audit-register.md).

### AR-012 — revision leaves two unsafe interpretations of Send

Sources: `store/tasks.rs::revise`, `store/runtime.rs::current_attempts_for_binding`,
`user_command_with_actor`, `next_internal`; `automation/repair.rs::require_current_for_effect`;
`store/automation_goal_progression.rs::require_current_for_dispatch`; public Send shape.

`task.revise` updates Task revision/spec and reports `existing_attempt_preserved`;
it neither releases the Attempt nor classifies queued Operations. For direct Manager
Send, current-Attempt selection requires matching Task revision, but the fallback
`owns` query accepts any unreleased owned Attempt on that binding. After revision,
Send can therefore be admitted without Task/Attempt columns. An already linked queued
Send also encounters the binding-level shortcut at dispatch without an independent
Task-revision check. Repair and Goal perform stricter exact-subject checks; preserve
those semantics when deleting their duplicate workflow machinery.

Target: revision atomically supersedes unsent task-linked native mutations, retains
sent/unknown effects for readback and preserves capture. New task-linked input needs
one exact current subject; deliberately taskless input needs explicit authority.
Use existing Operation scope and dispatch transaction, not a continuation ledger.

### AR-013 — revision can strand an Attempt behind the original owner

Sources: `tasks.rs::authorize_foreign_live_attempt_revision/revise/claim/release` and
`gm.rs::require_attempt_control`.

Current GM may revise another Manager's live Task, but foreign Attempt control then
fails the Task/Attempt revision equality check. The old owner remains immutable and
new claim is blocked by that unreleased Attempt. Permit current scoped GM disposition
of exact superseded work without granting continuation rights or rewriting owner.
Unsent mutations are cancelled; sent/unknown effects still require readback. This
routine recovery belongs to the Governor, not a mandatory human/operator.

### AR-014 — release cancels queued capture

Sources: `tasks.rs::release`, `normalized_result.rs::validate_admitted_operation`,
`runtime.rs::next_internal` admitted-result-only branch.

Release blocks unresolved sent effects, then cancels queued non-check Operations,
including admitted Result. Cancelled is outside normalized-result validation and
closed-binding selection; new result admission requires a current unreleased Attempt.
Classify by effect: cancel unsent native mutations, preserve evidence collection,
readback and reconciliation, and retain uncertain effects without replay. Do not keep
the broad `method <> 'check.run'` cancellation predicate.

### AR-015 — reuse the binding credential

`store/module_credential.rs` derives/reuses the same registered private credential
for its binding/descriptor scope; missing registered files fail rather than silently
rotating. Hello establishes the new delivery boot/link after departure checks. New
credentials are not the normal restart solution. Revocation is a separate policy.

## Current pass details

All links in this section pin the inspected head, not a moving branch.

### AR-016 — distinguish first import, duplicate ACK and repeated replacement

Sources: [scope and recorder entry](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/runtime.rs#L2747-L2765),
[duplicate/settlement ordering](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/runtime.rs#L3080-L3260),
[terminal validator](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/runtime/warm_stream.rs#L39-L167),
[hello replacement](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/runtime.rs#L957-L1045).

`outcome_with_artifacts` first calls live `scope(check_link=true)`, checks retained
receipt/dispatch facts, then searches exact prior outcome observations. That early
branch can ACK an already committed outcome before `warm_stream::validate_outcome`.
The previous blanket claim that every old-boot retry is rejected was too broad.

A never-recorded terminal reaches the warm validator and fails when its origin boot
is not the current binding boot. Ordinary outcome scope additionally rejects released
bindings/stale links before duplicate lookup; page completion has a different narrow
scope. A recovery path must address both layers rather than remove one comparison.

`validate_registered_module_recovery_link` already checks an exact admitted reconcile
and forbids a resolved summary before the target is terminal. Reuse this ordering.
Do not inject new reconciliation fields into an immutable saved outcome merely to
make a different byte sequence pass; delivery context and execution evidence differ.

Hello overwrites `previous_bridge_boot_id` on each accepted replacement. It is a
one-hop recovery hint, not durable evidence of which boot executed every Operation.
`next_internal` retains boot in the task-dispatch context, but no equivalent ordinary
Send context is written in that inspected branch. Keep execution origin with the
original Operation at dispatch, reusing existing fields; do not build a boot-chain
proof registry. Legacy missing attribution remains unknown.

### AR-017 — recovery fences capture as well as execution

Source: [next selection](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/runtime.rs#L1260-L1405),
[resume settlement](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/runtime.rs#L3423-L3440).

The query requires either `recovery_required = false` or method Recover/Reconcile.
It excludes Result before later `normalized_result_admitted` exceptions run. The
closed-binding branch can find a queued sealed Result and still lose it at the same
query. This is independent of AR-014's cancellation problem.

For warm-stream input, recording terminal evidence does not establish that the old
native session was resumed. The ordinary flag-clearing path requires an applied
Recover confirming `native_session_resumed`; Antigravity capture must not depend on
inventing that capability. Native readiness and local evidence delivery are separate.

Permit exact authorized capture/readback while native mutation stays fenced. Reuse
existing page admission/registration/assembly; do not set ready just to read files,
launch a replacement session, or require a human to export result pages. First-time
historical page admission after supersession remains the next explicit source trace.

### AR-018 — supervisor recovery is not an outcome importer

Sources: [supervisor handler](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/mod.rs),
[source-intake helper](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/mod.rs#L1129-L1146),
[adapter startup/Open](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/modules/antigravity-rust/src/main.rs#L47-L158).

The supervisor method accepts no target or capture data. It calls
`reconcile_module_recovery_journal_page`, which reconciles automation source intake
and registered HookCommit events. It does not read terminal spool files, establish
native outcome evidence or settle the target Operation. Its `reconciled:true` means
that bounded intake pass ran, not that native work was recovered.

Preserve adapter startup's existing positive boundary: hello and a Controller are
created before any native process; `spawn_owned` occurs only in Open. Add capture
loading/draining there and reuse ordinary Store result owners. Do not expand the
supervisor event-intake RPC into another recovery product.

### AR-019 — completed readback versus unresolved target

Sources: [Antigravity read_command](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/modules/antigravity-rust/src/controller.rs#L573-L672),
[Store recovery-link validation](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/runtime.rs#L2470-L2600),
[settlement](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/crates/swarm-kernel-host/src/store/runtime.rs#L3220-L3260).

Reconcile checks the in-memory pending target. When missing, it records an Unknown
target; regardless of the lookup outcome it records the Reconcile Operation itself
as Unknown with `native_history_unavailable`. Store persists that as `outcome_unknown`.
A completed local inspection has therefore been confused with an interrupted request.
This is a source-confirmed classification gap, not a demonstrated infinite retry loop.

The existing batch/Zed readback path already separates an applied readback summary
from `resolved:false`. Use that semantic pattern: record/replay target evidence first;
settle completed lookup with resolved true/false; keep the original target unknown
when evidence is missing. Infrastructure failure remains separate. Preserve bounded
policy and exact target identity; a resolved readback never accepts a Task or grants
new execution. Do not create a second recovery-state machine.

## Earlier findings retained

Detailed prior traces are preserved in repository history:

- [result retention and control-link failure at `c3e331a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md);
- [continuation results and assembly at `70b99db3`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md);
- [continuation attribution and repair readback at `1c61629b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md).

Those pages are evidence, not additional implementation programs. AR-001–AR-015 stay
open/preserved as classified above; AR-016's first-import finding remains open with
its duplicate-ACK claim corrected in this pass.

## Inspection log for this pass

| Source | Inspected portion |
| --- | --- |
| `AGENTS.md`, audit register, module API | Current task/ownership instructions, checkpoint and target delivery contract. |
| `implementation.md` | Retained behavior, owners and A0–A3; the program is not a claim of landed A2. |
| `store/runtime.rs` | Hello replacement; recovery queue predicate; dispatch-origin storage; typed receipt, recovery-link, duplicate and terminal outcome ordering/settlement. |
| `runtime/warm_stream.rs` | Complete production terminal validator and its current-boot check; test source not executed. |
| `store/mod.rs` | Supervisor recovery routing and source-intake helper, not a reread of the entire Store. |
| `swarm-supervisor/control.rs` | Supervisor recovery method/DTO boundary; no native capture importer established. |
| Antigravity `main.rs` | Startup through native Open and Dispatch; no spawn at constructor/hello. |
| Antigravity `controller.rs` | Completed body lookup and full Refresh/Reconcile read_command path. |
| Prior task/release and module credential sources | Inherited at unchanged source revision, not relabeled as newly inspected. |

## Remaining coverage map

| Area | Next unclosed boundary |
| --- | --- |
| Antigravity A | Original-Operation producer settlement after supersession; historical page admission and automatic capture. Implement/qualify AR-001/002/004/006/010/016/017/019. |
| Queue/routes/provider/capacity B | Root reservation, fairness and shared-account source scope. |
| Workspace/process C–D | Shared worktree release, PID reuse and exact successor custody. |
| Scheduler/Goals E | Due occurrence dedupe, drain and no-progress across selected routes. |
| TaskPacket/review F–G | Frozen evidence plan, reviewer replacement, all-findings correction and common acceptance gate. |
| Permissions H | Retained public actions and exact object authorizers, including explicit taskless Send and historical completion. |
| Store formats I | Reader/writer pairs, numeric migration and partial-upgrade/corruption cases. |
| IPC/MCP J | Slow consumers, disconnect, replay/live boundary and writer occupancy. |
| Check/Script/Forge/GitHub K | Exact effect owner, ambiguous response and readback for each write. |
| Other adapters M–Q | Selected installed contract, result/restart path and one-executor deletion per route. |
| Governor R | Provider-local/OS/Git source join and unbound inventory. |
| Deletions S | Positive replacement and unsettled-data disposition for every removed product. |
| Mailbox/GitHub/gateway T–V | Scoped recipient/read, external write recovery and remote limits. |
| Installed/Windows/load | Separate unexecuted qualification; not a substitute for source audit. |

## Checkpoint discipline

A finding remains open until a runtime change and exact evidence are linked. A docs
commit is not a repair. Keep refuted claims and preserved behavior so a later agent
does not rebuild the same bureaucracy or delete a useful boundary. Resume with the
historical producer/capture chain above; update this register and the owning target
contract together.
