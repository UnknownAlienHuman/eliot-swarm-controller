# Audit register

This is the audit's durable resume point, requested by the owner. It records inspected
requirements, source paths, findings and unanswered questions. It is not another
implementation queue, runtime registry or approval gate. [Implementation](implementation.md)
remains the work order; [status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-22 / result retention and control-link failure**.
- Source baseline: **`3b9c21c35e24096c3e9b2dfcb0055aa58acb0801`**.
- Overall audit: **incomplete**. No measured completeness percentage is available.
- Last completed trace: Antigravity native terminal → in-memory retention → delayed
  `agent.result`; and observation delivery failure → native stdin shutdown.
- Next exact trace: `store/runtime.rs::admitted_result_scope` →
  `store/normalized_result.rs::validate_source` → `store/results.rs::prepare` and
  registration/replay → artifact publication/read. Determine which released-link,
  original-boot and successor cases already work before adding recovery machinery.
- No runtime code, Clippy, tests or installed harness were executed in this pass.

On resume, read this section and the open finding before rereading the archive. Compare
changed source paths with the baseline. Unchanged evidence remains valid at its named
head; changed callers or consumers require rechecking that row, not a claim that the
whole audit remains current. Update this file in the same publication as related docs.

## Meaning of the evidence

`source-confirmed` means the named control path was read and its consequence follows
from the code; it is not a live reproduction. `partial` names the boundary not yet
traced. `inherited` means earlier findings exist but have not been independently
rechecked in this pass. `preserve` is verified useful behavior, not a deletion target.
Runtime fixes and executed checks are recorded separately from these review labels.
A search hit locates code; it is not a completed trace. Reading a test is not running it.

## Current pass: exact paths and results

All current-source links below are pinned to the baseline above.

| ID | Requirement and inspected path | Evidence/result | Still needed |
| --- | --- | --- | --- |
| AR-001 | Control disconnect must not stop admitted native work. `main.rs::wait_for_terminal` → `flush_reports` → `HostSession::request_saved`; `run` error branch → `NativeSession::shutdown_and_drain` | **source-confirmed, open**: a report error propagates into stdin shutdown. Native events consumed during drain can create in-memory terminal receipts, then `run` returns an error without another delivery attempt. | Implement control-link retry independently of native draining and durable capture; exercise host-down-during-turn. |
| AR-002 | A completed result remains retrievable until durable capture/release. `Controller::settle_terminal/remember_completed_turn/retained_response_for_operation`; stream bounds; `main.rs` result handler | **source-confirmed, open**: body/mapping retention is a 32-response in-memory window. The 33rd completion evicts the oldest mapping even when its full body has never reached Store. An outcome ACK is not a body/page ACK. | Tie source-body release to complete durable capture, not number of subsequent turns; exercise delayed/multi-page reads. |
| AR-003 | Result identity must not be inferred from equal prompt hashes or newest output. `Controller::retained_response_for_operation`; sequential-retention cases | **preserve**: exact Operation and retained receipt are compared; conversation/stream generation are checked. ACK removes pending outcome but does not immediately remove the separate completed-turn mapping. Oversized bodies are not presented as complete truncated text. | Simplification must keep causal IDs and explicit oversized/incomplete results while removing repeated package proof fields. |
| AR-004 | Retained data must survive adapter restart without native replay. `Controller::new/read_command`, same-process caches, A2 target | **source-confirmed gap, open**: a new Controller has empty caches; current `agent.reconcile` can emit history-unavailable, not reload old terminal bytes. A2 is a target, not present code. | Finish Store-side successor trace below; connect authorized import of original evidence without relabeling boot or starting native work. |
| AR-005 | Existing Store completion paths should be reused, not replaced blindly. `results.rs::prepare`; `runtime.rs::admitted_result_scope` search/contract | **partial**: normalized pages already select a dedicated admitted-result scope; its stated purpose includes released/link-changed completion. Do not claim all historical pages are rejected. | Read the full scope and normalized-origin validators, duplicate ACK path and artifact consumer before selecting the minimum extension. |

### AR-001 — a reporting failure changes native lifecycle

Sources: [run and dispatch error handling](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L38-L270),
[request and report delivery](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L695-L860),
[drain and terminal wait](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/main.rs#L1030-L1240).

The timer branch awaits `flush_reports`. While that branch awaits host retries, the
native-event receiver is not being reduced; the stdout task can queue only 16 frames
before backpressure. If reporting fails, the caller invokes `shutdown_and_drain`, which
closes stdin and waits for direct-child exit. This is not proof that the remote work
was killed, but it is a concrete violation of control-disconnect independence. The
pre-exit wait has no deadline in this function; the two-second tail deadline applies
only after exit. A terminal observed during drain is still only in memory.

Retained requirement: original [architecture §4](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f1b854b6fd058dc2440d3848fc95acc279657d3f/docs/agent_swarm.md#4-владение-task-native-сессия-и-процесс-не-одно-и-то-же)
says the long-lived bridge continues admitted work when host IPC disconnects. The
current [architecture](architecture.md#progress-native-effects-and-store-delivery)
also separates execution from delivery.

A2 must keep the native reducer reading while one bounded Eliot delivery attempt is
pending, stop taking new work on delivery/storage pressure, and retry saved deliveries
with bounded backoff. An ordinary network/readback error is not a shutdown command.
Keep one native-state owner; do not solve this with another service or broker.

### AR-002 — an ACKed outcome is not a retained complete answer

Sources: [retention and result lookup](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/controller.rs#L450-L590),
[32-response bound](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/src/stream.rs#L10-L28),
[existing eviction case](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/3b9c21c35e24096c3e9b2dfcb0055aa58acb0801/modules/antigravity-rust/tests/sequential_retention.rs#L465-L505).

The existing test deliberately completes and outcome-ACKs 33 turns and expects the
oldest lookup to return `NATIVE_RESULT_BODY_UNAVAILABLE`. The test was read, not run.
This confirms a current cache contract; the product defect is relying on that cache as
the only source of an uncollected complete result. A crash is not required.

Keep body identity/storage lifetime separate from delivery-envelope ACK lifetime. A2
must persist the complete source body before forgetting it and serve all pages from
that stable body. Delete it only after complete durable Store capture or explicit
retention/release policy, not an outcome ACK, first-page ACK or later turn count.
Bound bytes and apply admission backpressure rather than silently evicting uncollected
results. No second transcript store is needed: this is the existing result boundary.

## Inspection log (not a claim to have read each whole file)

| Source | Read in this pass |
| --- | --- |
| `AGENTS.md`, `architecture.md`, `implementation-status.md` | complete current contents |
| `implementation.md` | A1/A2 and adjacent B–E source/target instructions |
| `modules/antigravity-rust/src/main.rs` | run/dispatch/result, HostSession retry, report delivery, channel setup, terminal wait and shutdown/drain |
| `modules/antigravity-rust/src/controller.rs` | fields/new, open identity, terminal capture tail, completed mapping, read/reconcile/result lookup |
| `modules/antigravity-rust/src/stream.rs` | bounds and response data shape; full parser not reviewed |
| `modules/antigravity-rust/tests/sequential_retention.rs` | ACK/page/identity/oversize/eviction scenarios; not executed |
| `store/results.rs` | cache preamble, status snapshot, Claude-specific origin, admitted result selection and prepare; final registration/read consumer not yet traced |
| `store/runtime.rs` | dispatch context and selected receipt sections; admitted-result scope body remains next |
| Original `agent_swarm.md@f1b854b6` | purpose/module/ownership requirements through §4 |
| Rust `File::sync_all` documentation | stable file durability API; no platform power-loss guarantee inferred |

## Remaining coverage map

The rows below are **inherited/open**, not completed by this pass. They index existing
findings and the implementation slice; they do not invent percentages or another
schedule. Split a row only when a concrete path is actually inspected.

| Area / implementation | Existing evidence and next boundary |
| --- | --- |
| Requirements/autonomy | Preservation matrix in status; remaining original specs and Claude archive must be mapped to live callers. |
| Antigravity A | AR-001–005 above; dispatch proof and full result/successor chain still open. |
| Queue/routes/provider/capacity B | Existing findings; recheck root reservation, fairness, shared account and prepared-work dependencies. |
| Workspace/process C–D | Existing findings; trace shared worktree, release, PID reuse and successor fencing. |
| Scheduler/Goals E | Current policy documents; trace occurrence dedupe, native Goal plus Eliot messages, drain and no-progress. |
| TaskPacket/review F–G | Existing findings; trace frozen evidence plan, missing-reviewer replacement, all-findings correction and acceptance. |
| Public/internal permissions H | Method registry is not proof of object authorization; enumerate retained actions and actual authorizers. |
| Store formats/migrations I | Readers/writers and partial transaction states still need path-specific review. |
| IPC/MCP J | Disconnect, slow consumers, replay/live boundary, writer occupancy and exact pending-request reads. |
| Check/Script/Forge/GitHub K | Each admitted effect, authority recheck, cancellation, ambiguous response and readback owner. |
| Shared shell L | No extraction until two complete callers establish the same lifetime/ownership. |
| Claude/Codex/Command/Muse/OpenCode M–Q | Earlier adapter findings are inherited; refresh exact selected version, actions, results and restart paths per route. |
| Governor R | Provider-local/OS/Git source join and unbound inventory; measure no per-viewer provider calls after implementation. |
| Deletions S | Each removal needs a connected positive replacement and concrete unresolved-data disposition. |
| Mailbox/handover T | Recipient identity, scoped read, cancel/reply, replacement manager and coalesced wake. |
| GitHub/gateway U–V | Collector/import/effect split; page movement, credential scope, HTTP timeout and session limits. |
| Installed/Windows/load qualification | Separate unexecuted work; not a reason to postpone useful source/document corrections. |

## Checkpoint discipline

A source-confirmed finding stays open until a fix and its exact evidence are linked.
A documentation correction is not a runtime fix. Record refuted claims too, so another
pass does not reintroduce them. Do not mark a whole subsystem complete from a handful
of symbols or a green documentation workflow. Resume with AR-005, then update this
register and the owning target document together.
