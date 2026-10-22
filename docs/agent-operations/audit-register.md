# Audit register

Durable resume point requested by the owner: inspected requirements, source paths,
confirmed/refuted findings and unanswered questions. This is not another runtime
registry, implementation queue or approval gate. [Implementation](implementation.md)
remains the work order; [status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-22 / continuation attribution through submission and repair readback**.
- Inspected head: **`70b99db3d8732d10a69d03d1dbe83afdd39a7229`**. Compared with
  `c3e331a`: only audit-register/module-API documentation changed. Earlier runtime
  evidence at `3b9c21c` therefore remains unchanged; it is not new executed evidence.
- Overall audit: **incomplete**; no measured completeness percentage.
- Newly traced: ordinary Operation insertion; direct Manager, Repair and Goal
  continuation admission; exact Task/Attempt column writes; pre-effect dispatch guards;
  normalized candidate validation into submission reserve/begin/finish; existing
  submission recovery; RepairDispatch delivery classification.
- AR-009 records existing attribution to reuse. AR-010 identifies a repair-readback
  predicate incompatible with Antigravity terminal receipts. AR-011 records the
  already-working same-Attempt submission design to preserve.
- **Next exact chain:** queued direct `agent.send` after Task revision, release or
  reassignment: `tasks.rs` transitions/cancellation -> `operations.rs` release/cancel
  -> `runtime::next_internal`. The latter has a binding-level `owns` shortcut, but
  transition-side cancellation was not traced here; do not yet claim a reachable
  stale-send defect. Then trace adapter-restart successor credentials and old-boot
  outcome import, distinct from already implemented submission recovery.
- No runtime code changed. No Clippy, tests, installed harness or independent verifier
  ran in this pass. Local clone/download was unavailable; review and publication use
  the GitHub connector. Existing tests remain source evidence only.

On resume compare source changes with this head. Recheck changed callers/consumers,
not the entire archive. Update this register with the owning target document. A search
hit locates code; reading a test is not running it. Earlier findings are not cleared by
an interrupted chat or a green documentation workflow.

## Evidence labels

`source-confirmed`: the named path and stated consequence were read, not reproduced
live. `preserve`: useful existing behavior, not a deletion target. `partial`: the exact
untraced boundary is named. `inherited`: earlier evidence not rechecked in this pass.
Review status, implemented repair and executed verification are separate.

## Findings and coverage

| ID | Inspected requirement/path | Evidence and disposition | Remaining boundary |
| --- | --- | --- | --- |
| AR-001 | Antigravity report failure -> native shutdown | **Source-confirmed, open.** `wait_for_terminal` awaits `flush_reports`; delivery failure reaches stdin shutdown. Terminal facts read during drain can remain only in memory. | Independent bounded Store delivery and continuous native draining; host-down case. |
| AR-002 | Complete result retention | **Source-confirmed, open.** A 32-response cache evicts the oldest uncollected body/mapping on the 33rd completion. Outcome ACK is not body ACK. | Durable source-body lifetime, pressure before new admission, complete-capture release. |
| AR-003 | Exact result identity | **Preserve.** Operation/receipt/conversation/generation checks; separate post-ACK mapping; oversized output is not presented as complete truncated text. | Keep causal identity while removing package-proof copies. |
| AR-004 | Adapter restart without native replay | **Source-confirmed gap, open.** New Controller has empty caches; reconcile cannot reload terminal bytes. | Original-Operation successor import; no native launch or origin-boot relabeling. |
| AR-005 | Previously admitted normalized-page completion | **Preserve.** Exact registered module tuple and sealed origin allow completion without ordinary live-link/release gating; identical registered pages receive replay ACK. | Different-credential successor, old-boot outcomes, partial storage. |
| AR-006 | Complete continuation results | **Source-confirmed, open, route-scoped.** Generic body admission and validators require `task.dispatch`/first producer; strict Antigravity's fallback is status only although the adapter can retain Send output. | Extend the same source-Operation path through candidate consumers, not just its method allowlist. |
| AR-007 | Full assembly and artifact reads | **Preserve.** Publish then register; source identity, contiguous ranges, EOF/total and digests; Bytes grants before reads. | All grant/cache races, publication primitives and crash cases remain incomplete. |
| AR-008 | Authentication versus finishing admitted output | **Source-confirmed boundary.** Disabled credentials fail new handshakes; a still-open authenticated carrier has a narrow prior-page completion path. | Revocation side effects and successor provisioning; not a claim of unrestricted revoked access. |
| AR-009 | Continuation Task/Attempt attribution | **Source-confirmed, mixed.** Selected direct Manager, Repair, Goal and recognized direct correction paths already write Operation scope. Ordinary operator/unselected binding-level sends need not have it. Participant Send is denied by public policy. | Resolve task-linked input at admission; preserve explicit taskless input; audit stale queued direct sends separately. |
| AR-010 | Repair delivery readback | **Source-confirmed predicate mismatch, open.** Reader requires applied + nonempty native input ID + `native_input_admitted`; Antigravity terminal uses another condition and no such ID. It also maps all rejected/cancelled rows to NoEffect. | Normalize real delivery/terminal facts, distinguishing pre-input rejection from executed terminal failure. No live reproduction claimed. |
| AR-011 | Correction submission and historical recovery | **Preserve.** Same unreleased `needs_correction` Attempt may submit with expected revision/prior submission; normalized candidate calls the common origin validator. Published stale submissions are retained but not applied to current work. | Extend continuation origin in the existing validators; candidate/permission/crash matrix not exhausted. |

## Earlier result traces retained

Detailed prior trace: [register at 70b99db3](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md).
The conclusions and remaining gaps below remain active; the historical page is evidence,
not an additional implementation program.

### AR-001 — reporting failure changes native lifecycle

[Antigravity main](../../modules/antigravity-rust/src/main.rs): `run`,
`wait_for_terminal`, `flush_reports`, `HostSession::request_saved`,
`NativeSession::shutdown_and_drain`. The timer awaits report retries while the reducer
is not running; stdout has a 16-frame queue. Failure closes stdin and waits for child
exit. The two-second tail deadline applies after exit, not to that preceding wait.
This does not prove remote work was killed, but reporting changed native lifecycle.

Original [architecture §4](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md#4-владение-task-native-сессия-и-процесс-не-одно-и-то-же)
requires admitted work to continue across host IPC disconnect. A2 keeps one native
reducer alongside bounded Store delivery, retries saved records rather than prompts,
and pauses new admission on pressure. Explicit stop, revoked authority and custody
loss have their own policy. No broker is needed.

### AR-002 / AR-003 / AR-004 — body, effect and delivery lifetimes

[Controller](../../modules/antigravity-rust/src/controller.rs): `settle_terminal`,
`remember_completed_turn`, `retained_response_for_operation`, `new`, `read_command`;
[stream](../../modules/antigravity-rust/src/stream.rs) response bounds;
[retention cases](../../modules/antigravity-rust/tests/sequential_retention.rs).

The existing test completes and outcome-ACKs 33 turns, then expects the oldest body
lookup to fail. It was read, not executed. No process crash is required for this loss.
An outcome ACK releases its envelope; a page ACK releases that page. Keep source bytes
until complete capture or explicit retention/release, not an EOF page alone. Reserve
bounded space before new work instead of silently evicting uncollected output.
Causal identity survives simplification; hashes cannot recreate missing bytes.

### AR-005 / AR-007 / AR-008 — completion, storage and access

[Runtime scope](../../crates/swarm-kernel-host/src/store/runtime.rs):
`admitted_result_scope`; [origin validators](../../crates/swarm-kernel-host/src/store/normalized_result.rs):
`validate_admitted_operation`, `validate_source`; [registration](../../crates/swarm-kernel-host/src/store/results.rs):
`prepare`, `record`; [Store callers](../../crates/swarm-kernel-host/src/store/mod.rs):
`persist_result`, `assemble_artifact`, `read_artifact`; [assembler](../../crates/swarm-kernel-host/src/artifacts/assembly.rs):
`plan`, `assemble`; [IPC](../../crates/swarm-kernel-host/src/ipc.rs): `serve_connection`.
These paths were inspected at `c3e331a` and are unchanged at this pass's head.

Previously admitted normalized pages use exact credential/binding/Operation origin
rather than a live-link gate. Registration revalidates inside its transaction and ACKs
only identical settled artifacts. This is not general historical-read or new-command
permission. New authentication rejects disabled credentials; the existing carrier's
completion path is a distinct boundary. Do not break admitted-result delivery with a
blanket new-command authorization check, or bypass authentication for recovery.

Assembly verifies same source, ordered continuous coverage from zero, total/EOF,
actual page bytes and full digest, including a complete empty body. File publication
precedes Store registration. The public call returns an admission receipt;
`operation.get` establishes processing success. Bytes-level grants remain separate
from metadata access. Preserve this code, not a second assembler or receipt database.
Full cache invalidation, revocation races, successor credentials and power-loss
behavior are not established by these source reads.

### AR-006 — extend origin all the way to the consumer

[Admission](../../crates/swarm-kernel-host/src/store/runtime.rs):
`user_command_with_actor`; [normalized results](../../crates/swarm-kernel-host/src/store/normalized_result.rs):
`admitted_origin`, `validate_admitted_operation`, `validate_source`, `producer_origin`,
`validate_page_artifact`, `validate_candidate_origin`; [Antigravity page builders](../../modules/antigravity-rust/src/result_page.rs).

The generic path requires a source `task.dispatch`, `Attempt.start_operation_id` and
normalized dispatch producer. Strict Antigravity's alternative returns status, not the
answer. Changing only the first method check leaves the same assumptions in retained
origin validation and in submission via `validate_candidate_origin`.

Resolve an actual source Operation and its already-authorized assignment plus native
input/turn/body facts. Keep first-start ownership unchanged; do not insert a fake
initial producer or create another Attempt just to make a correction readable. Use the
same page/assembly/submission path. Steer may contribute to an existing turn and must
not manufacture a second terminal. This is not a finding that all vendor history APIs
lose Send output.

## Current pass: attribution and correction consumers

### AR-009 — useful scope is already stored, but not for every binding command

Pinned evidence: [ordinary insertion](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/mod.rs#L6120-L6410),
[actor selection](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/runtime.rs#L5220-L5510),
[scope persistence](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/runtime.rs#L5815-L5875),
[direct correction](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/automation_repair.rs#L2234-L2345).

Ordinary insertion creates a queued Operation without Task/Attempt columns; the domain
handler supplies scope in the same transaction. There is no need for another
continuation table to hold the same relationship.

| Caller | Current retained relationship |
| --- | --- |
| Direct Manager with selected explicit or unique current Attempt | `manager_attempt` writes Task/Attempt into Operation. |
| Automatic Repair | Validated correction context writes its exact Task/Attempt; public request need not carry a caller-chosen Attempt. |
| Automatic Goal progression | Validated context selects `manager_attempt` and persists it; pre-effect validation uses the retained cause. |
| Recognized direct Manager correction | `retain_direct_admission` fills missing Task/Attempt only if existing columns are absent or agree. |
| Ordinary Operator / direct Manager without selected Attempt | Binding-level admission does not guarantee Task/Attempt columns. The Manager path can continue when it owns work on the binding but selection is ambiguous. |
| Participant | Public `method_policy` denies `agent.send`; do not diagnose its internal match arm as a publicly reachable permission defect. |

Target: task-linked Send resolves an exact authorized Attempt at admission and stores
that relationship on its Operation. If inference is ambiguous, the Governor supplies
the target; do not guess from the newest Task. Deliberately taskless session input
remains unassigned, not retrospectively attached to a Task. Preserve existing rights;
provenance does not justify granting more roles control.

[Pre-effect dispatch](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/runtime.rs#L1263-L1775)
rechecks Repair/Goal contexts, but direct commands can take a binding-level `owns`
shortcut. Whether all Task transitions cancel stale queued sends before that point is
**not yet traced**. That exact cancellation chain is the next source review, not a
confirmed privilege escalation or a reason to add a new authorization framework.

### AR-010 — delivery reader assumes one harness receipt shape

Pinned evidence: [consumer routing](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/automation_repair.rs#L480-L560),
[delivery_state](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/automation_repair.rs#L2860-L2975),
[historical direct readback](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/automation_repair.rs#L815-L900),
[Antigravity terminal](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/modules/antigravity-rust/src/controller.rs#L968-L1021).

Both repair readers recognize delivery only for `applied`, a nonempty native input ID
and `completion_condition == native_input_admitted`. A settled Antigravity terminal
uses `native_terminal_result_observed` and `native_input_id = None`, so that exact
record yields `native_delivery_receipt_is_not_sufficiently_proved`. A successful
terminal is not a missing ACK. This is a source-confirmed classifier mismatch, not a
claim that a full configured native repair run was executed.

The same readers classify every `rejected`/`cancelled` row as no effect. Antigravity
also uses Rejected for observed ERROR/CANCELED/INTERRUPTED terminal events. The enum
alone therefore cannot prove input never reached the harness. No duplicate prompt is
claimed to have occurred in this review; the false no-delivery classification is the
finding.

Remove these duplicate readers with G's RepairDispatch deletion. The replacement
ordinary correction flow consumes adapter-normalized admission/terminal evidence:
confirmed admission establishes delivery; an attributable terminal also establishes
delivery while retaining its success/error/cancel facts. Only confirmed pre-input
rejection or unsent cancellation establishes no native execution. Unknown stays
unknown. None of these facts proves body capture, successful repair or Task acceptance.
Do not add native method/version allowlists or fabricate native IDs for this purpose.

### AR-011 — preserve same-Attempt submission instead of adding another repair state

Pinned evidence: [current scope/candidate](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/submissions.rs#L16-L140),
[reservation and retained document](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/submissions.rs#L630-L747),
[begin/finish](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/submissions.rs#L1160-L1290),
[normalized candidate](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/crates/swarm-kernel-host/src/store/normalized_result.rs#L435-L748).

`current` already accepts unreleased `needs_correction` and `recovery_pending`, checks
revision and expected prior submission, and checks caller scope. `reserve` invokes
candidate validation and freezes the submission document. `begin` rechecks current
scope before publication. `finish` validates retained provenance, applies only if still
current, and retains stale published evidence without changing the new work. Submission
is not acceptance.

For normalized output, both ordinary and Participant candidate checks use the common
origin validator; it revalidates every page, source identity, full payload/coverage
and assembly Operation. Keep these consumers in the continuation-origin change.
A source snapshot is a separate existing candidate path: do not require reconstruction
of a model's prose as a new prerequisite for a valid frozen code candidate.

Existing `task.submit.recover` verifies a published submission under scoped current
GM/Operator authority while preserving the original submitter and historical target.
That useful downstream recovery does **not** implement Antigravity A2's upstream
capture or new-credential adapter recovery. Do not conflate them or recreate both in
one generic recovery service. Claims that correction inherently requires a new Attempt
or a new repair-submit state are rejected by this source.

## Inspection inventory

Only named portions were inspected; this is not whole-file coverage.

| File | Current pass coverage |
| --- | --- |
| `AGENTS.md`, architecture, audit register, delivery | current instructions/target; prior baseline comparison |
| `store/mod.rs` | Operation insertion/apply/receipt transaction; selected visibility/discovery code encountered while locating it |
| `store/runtime.rs` | selector and actor admission; persisted scope; queued command selection/pre-effect guards |
| `store/normalized_result.rs` | page/candidate/assembly validation, producer identity and Attempt scope |
| `store/submissions.rs` | current/candidate/Participant checks; retained submission; reserve; recovery begin and selected finish; normal begin/finish |
| `store/automation_repair.rs` | context/cause selection, consumer and transferred readback; direct/automatic admission links; delivery_state |
| `automation/repair.rs` | request shape and context construction through current binding selection; not every validator |
| `method_policy.rs` | exact Send access entry located; full registry not re-audited |
| Antigravity `controller.rs` | terminal outcome shape and bounded receipt recording |

## Remaining coverage map

These are inherited/open areas, not additional completed audits or another schedule.

| Area | Next boundary / existing owner |
| --- | --- |
| Requirements/autonomy | Historical preservation matrix; remaining old specs and original Claude archive versus real callers. |
| Antigravity A | AR-001–006 and AR-010; dispatch proof, durable capture and successor chain remain open. |
| Queue/routes/provider/capacity B | Root reservation, fairness, shared accounts, preparatory dependencies. |
| Workspace/process C–D | Shared worktree, release, PID reuse and successor fencing. |
| Scheduler/Goals E | Occurrence identity, native Goal plus Eliot messages, drain/no-progress and native turn attribution. |
| TaskPacket/review F–G | AR-009–011; queued correction freshness, reviewer replacement and acceptance evidence. |
| Public/internal permissions H | Actual retained-action/object authorizers; do not infer reachability from internal branches. |
| Store formats/migrations I | Readers/writers, partial transactions, corruption and concrete migration states. |
| IPC/MCP J | Slow consumers, disconnect, replay/live, writer occupancy and pending-request reads. |
| Check/Script/Forge/GitHub K | Admitted effects, current authority, cancellation, ambiguous response and readback. |
| Shared shell L / adapters M–Q | Prove two matching consumers; refresh selected route versions and result/restart behavior. |
| Governor R / deletions S | Source/unbound join; every deletion needs a positive replacement and unsettled-work disposition. |
| Mailbox/handover T / integrations U–V | Recipients/grants/successors, collector-import-effect split, gateway credentials/timeouts. |
| Installed/Windows/load | Unexecuted environment qualification, separate from source review. |

## Publication rule

Findings remain open until a repair and its evidence are linked. Update the register
and owning contract together; preserve refuted claims and useful existing code. This
pass updates [delivery](delivery.md#5-submission-and-source-freeze) and its correction
semantics, not runtime implementation. Resume at the exact chain at the top.
