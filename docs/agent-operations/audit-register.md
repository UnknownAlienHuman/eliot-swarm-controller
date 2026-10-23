# Audit register

Durable resume point for the source audit. It records the inspected commit, exact
paths, confirmed/refuted findings, preserved mechanisms and the next untraced chain.
It is not another runtime registry, implementation queue or approval gate.
[Implementation](implementation.md) remains the work order;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / Task revision, stale queued input and adapter successor**.
- Inspected runtime head: **`1c61629b49e8d6b9763dc7f5be32a0135509d7e3`**.
- Overall audit: **incomplete**. No measured completeness percentage is claimed.
- Newly traced:
  - `task.revise` while an unreleased foreign Attempt exists;
  - `attempt.release` cancellation of queued Operations;
  - direct Manager, Repair and Goal `agent.send` admission and dispatch guards;
  - current-GM control of a superseded Attempt;
  - normalized-result state requirements after release;
  - stable binding-scoped module credentials across process restarts;
  - module hello boot replacement and Antigravity old-boot outcome validation.
- New findings: **AR-012–AR-016**.
- Next exact chain: `module.outcome` admission in `store/runtime.rs` through
  `runtime/warm_stream.rs::validate_outcome`, `operations` settlement and
  `module.supervisor.recovery.reconcile` / `agent.reconcile`. Define the minimum
  successor-delivery seam that accepts retained old-boot bytes for the original
  Operation without allowing a new native effect or rewriting the origin boot.
- This pass changed documentation only. No runtime code, Clippy, tests, installed
  harness or independent verifier ran. Existing test source is not a new test result.

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
| AR-005 | Previously admitted normalized-page completion | **Preserve.** Exact registered module tuple and sealed origin allow narrow late page completion without ordinary current-link/release gating; identical registered pages receive replay ACK. | Successor delivery and partial-storage cases. |
| AR-006 | Complete continuation results | **Source-confirmed, open, route-scoped.** Generic origin and candidate validators require the initial `task.dispatch`; strict Antigravity can retain a Send body but its public fallback exposes status only. | Extend the same source-Operation/page/candidate path to attributable continuation input. |
| AR-007 | Full assembly and artifact reads | **Preserve.** Page publication/registration, contiguous range and digest checks, complete assembly and Bytes-level authorization. | Grant/cache races and crash/power-loss qualification. |
| AR-008 | Authentication versus finishing admitted output | **Source-confirmed boundary.** Disabled credentials fail a new handshake; an already-authenticated carrier has a narrow path to finish its prior page. | Revocation side effects and post-restart delivery. |
| AR-009 | Continuation Task/Attempt attribution | **Source-confirmed, mixed.** Selected Manager, Repair and Goal paths can write Operation scope; ordinary binding-level sends may be deliberately taskless. | Require an explicit distinction instead of accidental fallback. |
| AR-010 | Repair delivery readback | **Source-confirmed predicate mismatch, open.** Repair readers require applied + native input ID + `native_input_admitted`; Antigravity terminal facts use another condition and no native input ID. Rejected/cancelled is also treated as no effect even after execution. | Consume common admission/terminal facts; distinguish pre-input rejection from executed terminal failure. |
| AR-011 | Correction submission and historical recovery | **Preserve.** The same unreleased `needs_correction` Attempt may submit against expected revision/prior submission; stale published submissions remain historical. | Extend continuation origin through existing candidate validators. |
| AR-012 | Task revision fences new work | **Source-confirmed defect, open.** `task.revise` preserves the old Attempt and does not classify its queued Operations. Direct Manager `agent.send` can then pass a binding-level ownership shortcut while no current Attempt is selected, becoming taskless; an already queued task-linked Send can also reach dispatch without a Task-revision recheck. Repair and Goal paths do perform exact current-subject checks. | Atomically supersede unsent old-revision mutations and recheck exact Task/Attempt before every task-linked native effect. |
| AR-013 | Current GM can dispose superseded work | **Source-confirmed defect, open.** A current GM may revise a Task beneath another Manager's live Attempt. After the revision, `require_attempt_control` rejects that GM because Task/Attempt revisions differ; `task.claim` still sees the unreleased Attempt. The original owner or local Operator is then required to release it. | Separate authority to continue current work from authority to reconcile/release a superseded historical Attempt. |
| AR-014 | Release must preserve admitted result capture | **Source-confirmed defect, open.** `attempt.release` cancels every queued non-check Operation for the Attempt, including an already admitted `agent.result`. Normalized-result validation rejects `cancelled`, and the closed-binding result-only path can select only queued Operations. | Cancel unsent native mutations, but retain exact result/readback/reconcile Operations needed to capture already admitted evidence. |
| AR-015 | Module credential across process restart | **Preserve; prior open question resolved.** The module client ID and private credential are deterministic for the binding/descriptor scope and are reused; implicit credential rotation is forbidden. A successor process normally uses the same binding-scoped credential with a new boot, not a different credential. | Live restart qualification and explicit credential revocation policy. |
| AR-016 | Old-boot terminal delivery | **Source-confirmed defect, open.** Hello records the previous boot and changes the current boot after proven owner departure, but Antigravity terminal validation requires the receipt boot to equal the current binding boot. A correctly retained old-boot terminal therefore cannot be imported by the successor. | Validate the immutable origin boot against the retained recovery transition while authenticating the new boot only as delivery actor. |

## Current pass details

### AR-012 — revision leaves two unsafe interpretations of Send

Sources:

- `store/tasks.rs::revise`;
- `store/runtime.rs::current_attempts_for_binding`;
- `store/runtime.rs::user_command_with_actor`;
- `store/runtime.rs::next_internal` dispatch guard;
- `automation/repair.rs::require_current_for_effect`;
- `store/automation_goal_progression.rs::require_current_for_dispatch`;
- `model.rs` public `agent.send` shape.

`task.revise` updates Task revision/spec and explicitly reports
`existing_attempt_preserved`; it neither releases the Attempt nor classifies queued
Operations. The old Attempt remains unreleased with its frozen old revision.

For a direct Manager Send, the public request has no Attempt ID. Admission first asks
for current Attempts on the binding using `attempt.task_revision = task.revision`.
After revision this returns none. A second `owns` query is weaker: any unreleased
Attempt owned by that Manager on the binding is enough. Therefore the request is not
rejected and no Task/Attempt is written to the Operation. This is accidental taskless
continuation authority derived from stale ownership.

An Operation admitted before revision retains the old Task/Attempt, but the ordinary
dispatch guard uses the same binding-level ownership shortcut and does not independently
require that Task revision and current Attempt still match. Thus Task revision is not a
reliable unsent-effect fence.

This does **not** apply equally to every caller. Repair revalidates current Task,
revision, Attempt, candidate, `needs_correction` state and binding before effect. Goal
progression reconstructs its retained link and rechecks the current target before
dispatch. Preserve those checks while removing their duplicate workflow machinery.

Target behavior:

```text
Task revision commits
  -> old Attempt remains immutable historical work
  -> queued unsent task-linked native mutations are superseded/cancelled
  -> sent/accepted/unknown effects remain attributable and readback-only
  -> result/readback/capture Operations remain deliverable
  -> new task-linked input must name one exact current Task/Attempt
  -> deliberately taskless session input requires explicit separate authority
```

Do not add a continuation ledger. Use Operation `task_id`/`attempt_id`, Task revision,
Attempt binding and the existing dispatch transaction.

### AR-013 — revision can strand an Attempt behind a human/operator

Sources: `store/tasks.rs::authorize_foreign_live_attempt_revision`, `revise`, `claim`,
`release`; `store/gm.rs::require_attempt_control` and handover contract.

The current GM is allowed to revise a Task owned by another live Manager. Handover and
revision deliberately do not rewrite Attempt owner. After revision, however, the same
current GM fails `require_attempt_control` because that helper grants foreign control
only when Task revision still equals Attempt revision. A new claim is blocked by the
unreleased old Attempt. No normal owner-transfer mutation closes the gap.

That leaves release dependent on the original Manager or local Operator. It is exactly
the kind of routine recovery incorrectly shifted to a human.

The compact fix is not mutable Attempt ownership. Keep historical owner identity.
Split permissions:

- **continue/start** requires the exact current Task revision and current Attempt;
- **reconcile/release superseded** permits the current scoped GM after checking the
  exact Task/project, immutable Attempt and unresolved effects;
- sent/unknown effects require readback before release;
- unsent effects are cancelled by the supersession transition;
- unrelated Managers receive no access.

### AR-014 — release currently destroys queued result collection

Sources: `store/tasks.rs::release`,
`store/normalized_result.rs::validate_admitted_operation`,
`store/runtime.rs::next_internal` admitted-result-only branch.

Release blocks on `sending`, `native_accepted` and `outcome_unknown`, then cancels all
remaining queued non-check Operations for the Attempt. This includes a normalized
`agent.result` that was already admitted but not yet handed to the adapter. A cancelled
result Operation is outside the normalized state vocabulary and is not selected by the
closed-binding result-only branch. A new result request cannot reconstruct the same
admission after release because new origin admission requires the current unreleased
Task Attempt.

Classify queued Operations by effect semantics:

| Operation at supersession/release | Required disposition |
| --- | --- |
| Unsent task-linked prompt/config/reply/Goal mutation | Cancel/supersede without native effect. |
| Sending/accepted/unknown native mutation | Preserve exact Operation and require readback/reconcile. |
| Admitted result page, result-body capture, readback or reconcile | Preserve and deliver; late evidence remains historical. |
| Unrelated/taskless session control | Apply its own explicit authority and lifecycle. |

Do not keep the current broad `method <> 'check.run'` cancellation predicate.

### AR-015/016 — stable credential exists; old-boot delivery does not

Sources:

- `store/module_credential.rs`;
- `store/runtime.rs::hello_internal` and `admitted_result_scope`;
- `store/module_demand.rs::startup_recovery_projection`;
- `runtime/warm_stream.rs::validate_outcome`;
- `swarm-contracts/runtime.rs::ModuleReceiptIdentity`.

Credential provisioning derives one stable `module_client_id` from binding generation,
selected descriptor and protected credential reference. Repeated starts reuse the same
private file/token; missing registered files fail rather than rotating silently. This
is the correct process-restart boundary and removes the earlier assumption that A2
needs a different credential.

Hello verifies the prior process owner departed, records `previous_bridge_boot_id`,
marks unresolved effects unknown, and installs the new `bridge_boot_id`. Startup
recovery then scopes the unresolved list to the new accepted link/boot.

The missing seam is delivery of bytes created by the old boot. `ModuleReceiptIdentity`
is Operation/binding/request identity and contains no boot. Antigravity's terminal
fingerprint does contain the origin boot, but `warm_stream::validate_outcome` compares
it only to the **current** binding boot. Once a successor hello succeeds, a retained
outcome from the previous boot is necessarily rejected.

A2 therefore needs one explicit recovery envelope/Store path:

```text
same stable binding credential + new authenticated boot
+ proven previous owner departure
+ retained previous boot transition
+ exact original Operation/binding/request
+ immutable old-boot outcome/result bytes
-> Store records original evidence; new boot is delivery actor only
```

The path grants no new native command, does not change the outcome's origin boot and
cannot transfer evidence to another Attempt. Missing or partial bytes remain unknown.
Do not weaken ordinary live outcome validation or add a second credential system.

## Earlier findings retained

Detailed prior traces are preserved in repository history:

- [result retention and control-link failure at `c3e331a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md);
- [continuation results and assembly at `70b99db3`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md);
- [continuation attribution and repair readback at `1c61629b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md).

Those pages are evidence, not additional implementation programs. AR-001–AR-011 stay
open/preserved as classified in the table above.

## Inspection log for this pass

| Source | Inspected portion |
| --- | --- |
| `store/tasks.rs` | `revise`, foreign live-Attempt authorization, `claim`, `release` and queued-operation cancellation. |
| `swarm-kernel/src/tasks.rs` | revision transition validation. |
| `store/runtime.rs` | direct/Repair/Goal admission, current-Attempt selection, Task/Attempt attachment, module next/dispatch guard, hello recovery. |
| `automation/repair.rs` | current-subject checks at admission/effect. |
| `store/automation_goal_progression.rs` | retained continuation link and current dispatch validation. |
| `store/gm.rs` | handover invariants and current-GM Attempt control. |
| `store/normalized_result.rs` | admitted state vocabulary and immutable source validation. |
| `store/module_credential.rs` | stable credential derivation, registration, reuse and no implicit rotation. |
| `store/module_demand.rs` | startup recovery scope and current boot/link fence. |
| `runtime/warm_stream.rs` | Antigravity terminal validation against current boot. |
| `swarm-contracts/runtime.rs` | module receipt identity and boot-independent fields. |
| Existing native-stream tests | revision invalidates current stream scope while preserving historical receipts; read only, not executed. |

## Remaining coverage map

| Area | Next unclosed boundary |
| --- | --- |
| Antigravity A | Implement and verify AR-001/002/004/006/010/016; remove dispatch proof only after positive paths are connected. |
| Queue/routes/provider/capacity B | Root reservation, fairness and shared-account source scope. |
| Workspace/process C–D | Shared worktree release, PID reuse and exact successor custody. |
| Scheduler/Goals E | Due occurrence dedupe, drain and no-progress across selected routes. |
| TaskPacket/review F–G | Frozen evidence plan, reviewer replacement, all-findings correction and common acceptance gate. |
| Permissions H | Retained public actions and exact object authorizers, including explicit taskless Send. |
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
old-boot outcome chain named above, then update this register and the owning target
contract together.
