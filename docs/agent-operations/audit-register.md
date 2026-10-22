# Audit register

This is the audit's durable resume point, requested by the owner. It records inspected
requirements, source paths, findings and unanswered questions. It is not another
implementation queue, runtime registry or approval gate. [Implementation](implementation.md)
remains the work order; [status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-22 / continuation results, assembly and completion authority**.
- Inspected head: **`c3e331a37ef58b3b98f7859bfb9e40935501e106`**. Comparison with the
  preceding source baseline `3b9c21c35e24096c3e9b2dfcb0055aa58acb0801` showed only
  documentation/instruction changes; the earlier runtime evidence below is unchanged.
- Overall audit: **incomplete**. No measured completeness percentage is available.
- Newly traced: result admission for continuation `agent.send`; Antigravity status/body
  alternatives; page publication/registration; assembly coverage/digest checks; public
  artifact read entry; new-connection authentication versus admitted-page completion.
- **AR-006 is now source-confirmed for the selected strict Antigravity route**, not a
  claim that every provider's send output is inaccessible. AR-007 records reusable
  assembly behavior; AR-008 records the precise completion/authentication boundary.
- Next exact trace: continuation Task/Attempt association in
  `runtime::user_command_with_actor`, `operations.rs` request admission, and repair/Goal
  callers; then `normalized_result::producer_origin/validate_page_artifact` into
  submission. Establish which retained source fields can replace the dispatch-only
  origin without rewriting `Attempt.start_operation_id` or attaching output by session.
- Still open: a different-credential successor, old-boot outcome import, page admission
  after assignment supersession, complete object-grant/cache invalidation branches and
  crash/partial-storage behavior. These are not closed by the assembly review.
- This pass changed documentation only. No runtime code, Clippy, tests or installed
  harness were executed. Existing tests are source evidence, not new test results.

On resume compare source changes with the inspected head. Recheck changed callers or
consumers, not the entire archive. Update this register with the owning document.
An interrupted pass neither erases earlier evidence nor makes inherited claims current.

## Meaning of the evidence

`source-confirmed` means the named path was read and the stated consequence follows
from it; it is not a live reproduction. `partial` names the untraced boundary.
`inherited` means earlier findings have not been rechecked in this pass. `preserve`
marks useful existing behavior. Review status, implementation and executed evidence
are separate. A search hit locates code; reading a test is not running it.

## Findings and coverage

| ID | Requirement / inspected path | Evidence and disposition | Still needed |
| --- | --- | --- | --- |
| AR-001 | Control disconnect must not stop admitted native work. `wait_for_terminal` → `flush_reports` → `request_saved`; `run` error → `shutdown_and_drain` | **source-confirmed, open**: report error reaches stdin shutdown; terminal facts read while draining remain in memory and the error return performs no further delivery. | Independent bounded Eliot delivery and continuous native draining; host-down-during-turn case. |
| AR-002 | Complete results survive until durable capture/release. `settle_terminal`, `remember_completed_turn`, response lookup and result handler | **source-confirmed, open**: 32-response cache; 33rd completion evicts the oldest mapping even without full Store capture. Outcome ACK is not body/page ACK. | Body lifetime tied to complete capture; pressure stops new admission, not retention of existing results. |
| AR-003 | A result belongs to its exact input. `retained_response_for_operation` and sequential-retention cases | **preserve**: Operation/receipt/conversation/generation checks, separate post-ACK mapping and explicit unavailable oversized bodies. | Keep causal IDs and incomplete-output semantics while deleting repeated package proofs. |
| AR-004 | Restart recovers retained data without native replay. `Controller::new/read_command` and A2 target | **source-confirmed gap, open**: new Controller has empty caches; reconcile cannot reload terminal bytes. | Authorized successor import for original Operations; no origin-boot relabeling or native launch. |
| AR-005 | Reuse existing normalized completion. `admitted_result_scope` → `validate_source` → `prepare` → `record` | **source-confirmed, preserve**: previously admitted pages have a credential-tuple completion path without ordinary live-link/release gating; exact settled-page replay is ACKed. AR-007/008 extend the traced boundary. | Different-credential successor and old-boot outcome import are not established; platform crash behavior is unexecuted. |
| AR-006 | Continuation output must be retrievable. `runtime::user_command_with_actor` → generic origin, strict Antigravity fallback → adapter result handler | **source-confirmed, open, route-scoped**: generic body admission rejects a source `agent.send`; strict Antigravity's alternative returns status only. Its adapter can retain that Send's body, but the public full-body path cannot reach it. | Generalize the existing source-Operation origin and consumers; keep first-start guard and exact Task/Attempt scope. No second dispatch or result framework. |
| AR-007 | Whole capture must prove all bytes, not only EOF. `Store::persist_result/assemble_artifact/read_artifact`, Store assembly and `ArtifactFiles::assemble` | **preserve, source-confirmed**: publish then transactionally register; assembly checks source identity, contiguous ranges, total/EOF and digests; artifact bytes require object authorization. | Full grant/cache cases, restart replay and all source-page publish primitive branches remain unexecuted/not fully traced. |
| AR-008 | Completion authority differs from new command authority. `ipc::serve_connection` → `Store::authenticate/call` → `persist_result` → `admitted_result_scope` | **source-confirmed boundary**: disabled credentials fail a new handshake; an already-authenticated still-open carrier has a narrow path to persist its previously admitted normalized page. | Do not call this unlimited revoked access. Trace revocation side effects and successor credential provisioning before changing the policy. |

### AR-001 — reporting failure changes native lifecycle

Evidence at the previous unchanged source baseline:
[dispatch/error handling](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L38-L270),
[request/report delivery](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L695-L860),
[drain and terminal wait](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L1030-L1240).

The timer awaits report retries while the native reducer is not running; stdout has a
16-frame queue. Failure closes stdin and waits for direct-child exit. This does not
prove remote work was killed, but it changes lifecycle because reporting failed. The
two-second tail deadline applies after exit, not to the preceding exit wait.

Original [architecture §4](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md#4-владение-task-native-сессия-и-процесс-не-одно-и-то-же)
requires admitted work to continue across host IPC disconnect. A2 keeps one native
reducer running alongside bounded Store delivery; retain in-flight delivery state,
retry saved records rather than prompts, and pause new admissions on storage pressure.
Explicit stop, revoked authority and custody loss are separate policies. No broker is
needed.

### AR-002 — outcome ACK is not complete body capture

Evidence: [retention](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/controller.rs#L450-L590),
[bounds](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/stream.rs#L10-L28),
[eviction case](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/tests/sequential_retention.rs#L465-L505).

The test completes and outcome-ACKs 33 turns, then expects the oldest body lookup to
fail. It was read, not run. The product gap is that this cache can be the sole copy of
an uncollected answer; no process crash is required.

Keep native effect identity, source body and delivery-envelope lifetimes separate.
Outcome ACK releases its envelope; page ACK releases that page envelope. Retain the
body until complete durable capture or explicit retention/release. An EOF page does
not prove earlier coverage. Reserve bounded storage before new work; do not evict an
uncollected body to make room. AR-007 identifies existing whole-capture checks to reuse.

### AR-005 — reuse normalized completion rather than rebuilding it

Evidence: [scope](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/runtime.rs#L728-L807),
[sealed origin](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/normalized_result.rs#L212-L435),
[registration](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/results.rs#L1320-L1450).

`admitted_result_scope` checks Module role, registered module identity, exact binding,
generation, admitted result Operation and retained Task/Attempt. Unlike ordinary
`runtime::scope`, it does not require a live link or unreleased binding. `prepare`
further limits states and validates origin; `record` repeats this inside its transaction.
Identical settled artifact/digest/metadata returns replay ACK. A new page registers
artifact, settles the result Operation and writes observation before ACK.

This is permission to finish a specific prior result, not to create a new result read
for arbitrary history. Same-credential completion does not by itself implement a
new-credential recovery actor or import old terminal outcomes.

### AR-006 — continuation body exists but the common admission rejects it

Evidence at inspected head:
[caller and selector routing](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/store/runtime.rs#L5220-L5509),
[origin and source checks](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/store/normalized_result.rs#L80-L435),
[adapter status/body alternatives](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/modules/antigravity-rust/src/result_page.rs#L22-L292).

The selected generic result path rejects any source method other than `task.dispatch`
before native delivery. `admitted_origin` additionally requires the source to equal
`Attempt.start_operation_id` and a normalized dispatch producer. Later validators
repeat the dispatch-only assumption.

Strict Antigravity's non-generic path permits only `antigravity_status`. Its body is a
Store-derived status document, not the response text. The adapter's sequential body
cache and terminal reducer can record both Dispatch and Send, but a result request for
a Send cannot obtain the normalized origin needed by `build_normalized`.

Consequence: an initial answer can have a full-body path while an ordinary next-turn
correction cannot use it. Increasing cache size or retrying `agent.result` cannot fix
an admission rejection. Do not resend the initial Task, create a replacement Attempt
just to read output, or label the continuation as the original dispatch.

Minimum replacement is specified in [module API](module-api.md#results-and-artifacts):
retain the continuation's actual Operation-to-Task/Attempt relation at admission,
retain its native input/turn/body identity, and reuse the existing page/assembly path.
Preserve `start_owner` and the first `start_operation_id`. A steer may contribute to an
existing turn rather than own a separate terminal. Its result relation must reflect
that native meaning. This pass does not establish parity for every vendor-specific
history API or all taskless/multi-Attempt Send cases.

### AR-007 — existing assembly proves complete capture

Evidence: [page persistence](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/store/mod.rs#L2710-L2837),
[assembly admission](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/store/assembly.rs),
[byte plan and publication](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/artifacts/assembly.rs#L408-L658),
[public assembly/read caller](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/store/mod.rs#L3283-L3433).

The page path prepares/validates, publishes bytes outside Store, then rechecks and
registers. Assembly authorizes source pages at admission and begin. `plan` starts at
zero, compares stable source identity, rejects gaps/overlaps/reordering, checks total
length and EOF, and handles an empty complete body explicitly. Per-page request IDs
and mutable producer lifecycle fields are not mistaken for a different source.

`assemble` verifies actual page bytes, the assembled digest and available normalized
payload/native digest. It syncs the temporary file, publishes without replacement,
compares an existing destination on recovery, and syncs the parent directory. Store
registration is separate. Public reads resolve current principal and Bytes-level
object grants before filesystem I/O; metadata alone is not a bytes grant.

Preserve these mechanisms. They do not mean all grant branches, cache invalidation,
revocation races, publication primitives or power-loss scenarios were tested. Assembly
admission is not assembly completion: the caller returns its stable admission receipt;
`operation.get` carries the processing outcome. Automatic capture must inspect that
outcome and source identity before releasing the adapter body or submitting a candidate.

### AR-008 — handshake, command admission and completion are distinct

Evidence: [IPC carrier](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/ipc.rs#L105-L204),
[authentication/routing](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/store/mod.rs#L1836-L2056),
[admitted scope](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/crates/swarm-kernel-host/src/store/runtime.rs#L728-L807).

A new connection authenticates token and rejects `disabled`. The connection retains
its authenticated Principal. `module.result` routes directly to `persist_result`; the
normalized completion path checks that carrier's registered role and exact binding /
Operation origin, not ordinary current-link/release/disabled gating.

Therefore an already-authenticated, still-open carrier can finish its admitted page
under the narrow completion contract. This is not unrestricted historical read access,
new native authority or evidence that disabled credentials reconnect successfully.
Do not add an unconditional `current_principal` check here as an alleged security fix
without deciding how admitted outputs still reach Store. Conversely, do not bypass
authentication to recover an old body. Successor credentials need the explicit scoped
reconciliation described by A2. Revocation-command side effects and that successor path
remain open. No exploit or live revocation behavior is claimed.

## Inspection log

These are inspected portions, not a claim of whole-file or runtime coverage.

| Source | Coverage |
| --- | --- |
| `AGENTS.md`, architecture, audit register, module API | current instructions/targets; prior architecture content unchanged |
| `implementation.md` | existing A2/result target inherited; runtime order unchanged |
| Antigravity `main/controller/stream.rs`, `tests/sequential_retention.rs` | preceding AR-001–004 source trace retained; no new execution |
| Antigravity `result_page.rs` | status/body selector split and complete normalized-body lookup |
| `store/runtime.rs` | this pass: user-command result routing, actor/Attempt selection, recovery/status alternatives and effective identity; prior scope/hello evidence retained |
| `store/normalized_result.rs` | dispatch-only new/retained origin and page validation from previous unchanged source; candidate consumer still open |
| `store/mod.rs` | authenticate, call routing, persist_result, assemble_artifact, read_artifact and current_principal; adjacent lifecycle code does not imply a full audit |
| `ipc.rs` | connection authentication, cloned carrier and Store call dispatch; test module not executed |
| `store/assembly.rs` | reserve/begin/finish/parts and source-authorization calls |
| `artifacts/assembly.rs` | request/manifest shapes, plan, digest checks, no-replace publication and selected read setup; caches not fully audited |
| `store/results.rs` | previous prepare/record/replay and metadata checks retained; no new full-file claim |
| Original architecture and Claude history | earlier requirement evidence retained; archive not reopened |

## Remaining coverage map

Rows below are **inherited/open** unless a concrete finding above narrows them. They
index implementation slices, not a second schedule.

| Area / implementation | Next boundary |
| --- | --- |
| Requirements/autonomy | Remaining old specs/Claude archive requirements to map to live callers. |
| Antigravity A | AR-001–006 defects, initial/continuation result relation and successor recovery. |
| Queue/routes/provider/capacity B | Root reservation, fairness, shared accounts and preparatory dependencies. |
| Workspace/process C–D | Shared worktree, release, PID reuse and successor fencing. |
| Scheduler/Goals E | Occurrence dedupe, both Goals, drain/no-progress and continuation output. |
| TaskPacket/review F–G | Frozen evidence, reviewer replacement, all-findings correction, corrected result and acceptance. |
| Permissions H | Retained actions and full object authorizers; AR-008 is only one completion boundary. |
| Store I | Reader/writer pairs and partial migrations/corruption. |
| IPC/MCP J | Slow consumers, replay/live, writer occupancy and exact pending-request reads. |
| Effect owners K | Admission, recheck, cancel, ambiguous response and readback. |
| Shared shell L | Two complete callers with matching lifetimes before extraction. |
| Adapters M–Q | Selected versions, actions, result/restart; old findings remain inherited. |
| Governor R | Local sources/unbound join and no per-viewer provider calls. |
| Deletions S | Connected positive replacement and concrete unsettled-data disposition. |
| Mailbox/handover T | Recipients/grants, cancel/reply, successor and wake. |
| GitHub/gateway U–V | Collector/import/effects, moved pages, credentials/timeouts/sessions. |
| Installed/Windows/load | Separate unexecuted qualification, not a gate before useful source work. |

## Claims ruled out or deliberately not made

- No blanket claim that all Send outputs are inaccessible: AR-006 is scoped to the
  traced generic admission and strict Antigravity body alternative.
- No claim that release always blocks admitted pages: AR-005 shows the opposite.
- No claim that completion on an old authenticated link grants arbitrary access.
- No new assembler or proof ledger is justified: AR-007 identifies existing checks.
- An admitted assembly, EOF page, green docs CI or inspected test is not complete runtime
  qualification. Findings remain open until actual fixes and evidence are linked.
