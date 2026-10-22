# Audit register

This is the audit's durable resume point, requested by the owner. It records inspected
requirements, source paths, findings and unanswered questions. It is not another
implementation queue, runtime registry or approval gate. [Implementation](implementation.md)
remains the work order; [status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-22 / result retention and control-link failure**.
- Source baseline: **`3b9c21c35e24096c3e9b2dfcb0055aa58acb0801`**.
- Overall audit: **incomplete**. No measured completeness percentage is available.
- Traced: Antigravity terminal/cache/delayed result; report failure/native shutdown;
  Store normalized-page scope, sealed-origin validation and exact registration replay.
- Next exact trace: `runtime.rs` caller of `normalized_result::admitted_origin` for
  `agent.result`, including continuation `agent.send`; then artifact publication,
  authorized body read and `artifact.assemble` coverage. AR-006 is not yet a complete
  caller-to-consumer finding.
- Still open within AR-005: transport authentication/revocation, authorized successor
  with a different credential, old-boot outcome import, and incomplete on-disk capture.
- This pass changed documentation only. No runtime code, Clippy, tests or installed
  harness were executed. Existing test source was read, not run.

On resume, read this section and the open finding before rereading the archive. Compare
changed source paths with the baseline. Unchanged evidence remains valid at its named
head; changed callers or consumers require rechecking that row. Update this register
in the same publication as related docs. An interrupted pass does not erase prior
findings or make inherited claims current.

## Meaning of the evidence

`source-confirmed` means the named control path was read and its consequence follows
from the code; it is not a live reproduction. `partial` names the untraced boundary.
`inherited` means earlier findings have not been rechecked in this pass. `preserve`
marks useful existing behavior rather than a deletion target. Source-review status,
implementation status and executed-check evidence are separate. A search hit locates
code; it is not a completed trace. Reading a test is not running it.

## Current pass: exact paths and results

Current-source evidence below is pinned to the baseline above.

| ID | Requirement and inspected path | Evidence/result | Still needed |
| --- | --- | --- | --- |
| AR-001 | Control disconnect must not stop admitted native work. `wait_for_terminal` → `flush_reports` → `request_saved`; `run` error → `shutdown_and_drain` | **source-confirmed, open**: report error reaches stdin shutdown; terminal facts read during draining remain in memory and the error return performs no further delivery. | Independent bounded Eliot delivery and continuous native draining; host-down-during-turn case. |
| AR-002 | Complete results survive until durable capture/release. `settle_terminal`, `remember_completed_turn`, response lookup and result handler | **source-confirmed, open**: 32-response cache; 33rd completion evicts the oldest mapping even without full Store body capture. Outcome ACK is not body/page ACK. | Source-body lifetime tied to complete capture; storage pressure stops new admission, not retention of existing results. |
| AR-003 | A result belongs to its exact logical input. `retained_response_for_operation` and sequential-retention cases | **preserve**: Operation, receipt, conversation and stream generation checks; ACK does not immediately remove the separate mapping; oversized body is not returned as complete truncated text. | Keep causal IDs and explicit incomplete output while removing duplicate package proofs. |
| AR-004 | Restart recovers retained data without native replay. `Controller::new/read_command`, same-process caches, A2 target | **source-confirmed gap, open**: a new Controller has empty caches; reconcile can report history-unavailable but cannot reload terminal bytes. | Authorized successor import for original Operations; no relabeling origin boot or native launch. |
| AR-005 | Reuse existing completion/replay behavior. `admitted_result_scope` → `validate_source` → `prepare` → `record` | **source-confirmed, preserve**: previously admitted normalized pages have an exact credential-tuple completion path without ordinary live-link/release gating; sealed origin is checked, registration rechecks it, identical settled page returns replay ACK. | Full transport/revocation/successor and file/body-consumer trace remains open; this is not unrestricted historical access. |
| AR-006 | Continuation messages need attributable complete results too. `normalized_result::admitted_origin/validate_source` | **partial**: both inspected generic-origin paths require source method `task.dispatch`, not `agent.send`. The current target describes results for logical inputs more broadly. | Trace caller selection and other result paths before deciding which extension is required. Do not claim all send results are unavailable. |

### AR-001 — a reporting failure changes native lifecycle

Sources: [run and dispatch error handling](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L38-L270),
[request/report delivery](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L695-L860),
[drain and terminal wait](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L1030-L1240).

The timer handler awaits `flush_reports`. While it awaits host retries, the native
receiver is not being reduced; stdout can queue only 16 frames before backpressure.
On failure, the caller closes stdin and waits for direct-child exit. This does not
prove remote work was killed, but control-link failure has changed native lifecycle.
The pre-exit wait has no deadline here; the two-second tail deadline applies only
after exit. A terminal observed during drain is still only in memory.

Original [architecture §4](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md#4-владение-task-native-сессия-и-процесс-не-одно-и-то-же)
requires a long-lived bridge to continue admitted work across host IPC disconnect.
Current [architecture](architecture.md#progress-native-effects-and-store-delivery)
also separates execution from delivery.

A2 keeps one native-state owner reducing events while a bounded Eliot delivery is
pending. It pauses new admissions on storage/delivery pressure and retries saved
records, not prompts. Keep the in-flight delivery state across event-loop iterations;
do not repeatedly cancel/recreate native writes. Ordinary transport failure is not a
shutdown command. Explicit stop, revoked authority and failed custody have their own
policy. No new broker or process is needed.

### AR-002 — an ACKed outcome is not a retained complete answer

Sources: [retention/result lookup](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/controller.rs#L450-L590),
[32-response bound](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/stream.rs#L10-L28),
[eviction case](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/tests/sequential_retention.rs#L465-L505).

The existing test completes and outcome-ACKs 33 turns and expects the oldest lookup to
return `NATIVE_RESULT_BODY_UNAVAILABLE`. This establishes the current cache contract,
not an executed product test in this pass. The product gap is relying on that cache as
the only source of an uncollected complete result. A crash is unnecessary.

Three lifetimes must stay distinct: native effect identity; complete source body;
and exact Store-delivery envelope. Outcome ACK releases only its envelope. Page ACK
releases only that page envelope. Keep the source body until complete durable Store
capture or explicit retention/release. An EOF page alone is not proof of preceding
coverage. Bound bytes and reserve capacity before admission; never silently discard
uncollected bodies to admit more work. Use the existing result boundary, not a new
transcript store or journal framework.

### AR-005 — existing normalized completion is a reusable boundary

Sources: [scope](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/runtime.rs#L728-L807),
[sealed-origin validation](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/normalized_result.rs#L212-L435),
[registration/replay](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/results.rs#L1320-L1450).

Unlike ordinary `runtime::scope`, `admitted_result_scope` does not demand a current
module link or unreleased binding. It checks Module role, registered module identity,
exact binding/generation, result Operation and retained Task/Attempt origin. The later
`prepare` restricts accepted Operation states further and invokes sealed-source checks.
This narrow completion path is not permission to launch new work or read arbitrary
historical data. New-connection authentication/revocation was not audited here.

`record` reruns preparation inside its transaction. An already settled page is ACKed
only when artifact identity, digest and metadata match. A new page registers artifact,
settles its result Operation and writes observation before commit and ACK. This should
be reused, not replaced with another receipt store. The separate filesystem publish,
full assembly and successor credential path remain to be traced.

### AR-006 — selected origin is narrower than the target API

[New origin admission](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/normalized_result.rs#L80-L137)
and [source validation](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/crates/swarm-kernel-host/src/store/normalized_result.rs#L316-L395)
require `task.dispatch`. Before broadening this, trace the actual `agent.result`
caller and selected source for a continuation Send. Preserve the initial-start guard;
a continuation must never be relabeled as the Attempt's first dispatch. This row is
an open coverage question, not a completed diagnosis of every result route.

## Inspection log

This lists inspected portions, not whole-file or runtime coverage.

| Source | Read in this pass |
| --- | --- |
| `AGENTS.md`, architecture, status, module API, Agent Operations README | complete current contents |
| `implementation.md` | A1/A2 and adjacent B–E instructions |
| Antigravity `main.rs` | run/dispatch/result, HostSession retry, report delivery, channel setup, terminal wait and shutdown/drain |
| Antigravity `controller.rs` | fields/new, open identity, terminal capture tail, completed mapping, read/reconcile/result lookup |
| Antigravity `stream.rs` | bounds and response shape; full parser not reviewed |
| `tests/sequential_retention.rs` | ACK/page/identity/oversize/eviction cases; not run |
| `store/results.rs` | selected status/legacy-origin paths, normalized prepare, record/replay, get and metadata authorization; body/assembly consumer remains open |
| `store/runtime.rs` | dispatch context, selected receipt sections, scope/admitted-result scope and hello recovery; full admission routing remains open |
| `store/normalized_result.rs` | new origin, admitted origin/source and page byte-range validation; final artifact/candidate consumers remain open |
| Original `agent_swarm.md@f1b854b6` | purpose/module/ownership through §4 |
| [Rust File durability API](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all), [Tokio select semantics](https://docs.rs/tokio/1.53.1/tokio/macro.select.html) | documentation only; no platform power-loss or installed-version qualification |

## Remaining coverage map

All rows below are **inherited/open**, not completed by this pass. They index existing
findings and implementation slices, not another schedule. Split a row only when a
concrete path is inspected.

| Area / implementation | Existing evidence and next boundary |
| --- | --- |
| Requirements/autonomy | Preservation matrix in status; remaining old specs and Claude archive to map to live callers. |
| Antigravity A | AR-001–006; dispatch proof and full result/successor chain remain open. |
| Queue/routes/provider/capacity B | Root reservation, fairness, shared accounts and preparatory dependencies. |
| Workspace/process C–D | Shared worktree, release, PID reuse and successor fencing. |
| Scheduler/Goals E | Occurrence dedupe, native Goal plus Eliot messages, drain and no-progress. |
| TaskPacket/review F–G | Frozen evidence plan, reviewer replacement, all-findings correction and acceptance. |
| Public/internal permissions H | Retained actions and actual object authorizers, not just registry entries. |
| Store formats/migrations I | Reader/writer pairs and partial transaction states. |
| IPC/MCP J | Slow consumers, disconnect, replay/live, writer occupancy and exact pending-request reads. |
| Check/Script/Forge/GitHub K | Admitted effects, authority recheck, cancel, ambiguous responses and readback owners. |
| Shared shell L | Two complete callers must establish matching lifetime/ownership before extraction. |
| Other adapters M–Q | Refresh selected versions, actions, result/restart paths; older findings are inherited. |
| Governor R | Local sources/unbound join; qualify no per-viewer provider calls. |
| Deletions S | Positive replacement and concrete unsettled-data disposition. |
| Mailbox/handover T | Recipient identity, grants, cancel/reply, successor manager and wake. |
| GitHub/gateway U–V | Collector/import/effect split, moved pages, credentials, HTTP timeouts and sessions. |
| Installed/Windows/load | Separate unexecuted qualification; not a gate before useful source changes. |

## Checkpoint discipline

Findings stay open until a fix and exact evidence are linked. Documentation changes
are not runtime repairs. Keep refuted claims so later passes do not resurrect them.
Do not mark a subsystem complete from selected symbols or a green docs workflow.
Resume at AR-006 and the remaining AR-005 boundaries; update this register and the
owning target document together.
