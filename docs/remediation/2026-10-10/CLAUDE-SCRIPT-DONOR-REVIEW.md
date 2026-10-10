# Claude-authored swarm scripts: useful mechanisms for ELIOT

Review date: 2026-10-10. Compared source baseline: `fdc8736428df981670a93023890bcc5acbde2df0`.

## Source and scope

Requested source: the owner's `eliot-swarm-scripts-2026-10-10.zip`, SHA-256 `85ee1f2ec0afa5b088791e4b43059dd67a324d29d9d697158dbe963381c0a7e7`. It contains 157 files: one index, 122 Python scripts, 18 PowerShell scripts, 13 shell scripts and three CMD files, totaling 15,308 decoded source lines. All files were inventoried; close reading focused on the mechanisms and function ranges below, not a claim that every line passed a complete security audit.

The owner reports that these scripts worked in practice. The archive contains code and dated incident comments, not the deleted operational state/log databases needed to reproduce its historical measurements. Its INDEX says the old issues/workers/conveyor data was removed on October 6. This review does not reactivate those directories, run the scripts, access vendor databases, launch models, answer callbacks, merge branches or delete anything.

Despite the owner calling them Claude scripts, this is a multi-harness operations toolkit: OpenCode, Codex, Muse and Command lanes, mostly around the earlier `eliot-memory-os` workflow. Treat it as a source of proven operational ideas, not as a drop-in Claude Agent SDK implementation. All source coordinates below are relative to `eliot-swarm-scripts/` inside that exact ZIP; raw scripts, machine-specific paths and credentials are not republished here.

This is implementation assistance, not a new governance programme. No mandatory gates, global development freeze, new task store, automatic supervisor, compulsory audit or additional approval process follows from this document.

## 1. Give the agent the current task, not the entire conversation

**Source:** `v2/make_task.py:1-16,72-156`; especially its selected comments, REMAINING/AUDIT/RESCUE sections and checklist ID/status projection.

**Mechanism:** compose one task view from the issue body, relevant remaining work, selected source comments, known unused implementation and a compact checklist. Do not repeatedly load every progress report or repeat an audit already included elsewhere. `build()` specifically avoids including the same audit in both REMAINING and AUDIT, and preserves checklist IDs instead of copying large evidence blobs.

**Existing ELIOT owner:** `TaskSpec::brief()` in `crates/swarm-kernel-host/src/model.rs`, the frozen Attempt brief, and `crates/swarm-kernel-host/src/store/task_prompt.rs`. A Store-built TaskPrompt already exists; do not create another `TASK.md` renderer in adapters.

**Production change in this PR:** `swarm_kernel::tasks::validate_spec` now rejects exact duplicate `source_refs` with the existing `INVALID_PARAMS` envelope. The set borrows original text, does not reorder/normalize references and does not treat the same ref in `source_refs` and the richer `source_index` as a conflict. Existing empty/non-string rejection is unchanged. The existing `TaskSpec::validate` and mutation validation reach this function for new create/revise inputs; no new endpoint, DTO or dependency is added.

Example: `["issue:7/body", "issue:7/body"]` is rejected before a new specification can produce repeated legacy gaps. `["issue:7/body", "issue:8/body"]` keeps its order. Case-distinct or whitespace-distinct nonempty references are not silently merged. Selected text and its UTF-8 digest are unchanged.

**Important limit:** this is the new-input portion of #46, not its complete migration. Historical duplicate refs still need a projection-only repair in `TaskSpec::brief`: make its existing membership set mutable and insert a ref when appending its first legacy gap. Keep richer source-index entries and first-occurrence order. Do not rewrite an existing Attempt snapshot or a retained TaskPrompt. A new claim that revalidates a malformed old Task may require an explicit Task revision, not an invisible data repair. #46 stays open.

**Do not copy literally:** the script's regex filtering by comment headings can hide an actual instruction. Current ELIOT should use explicitly selected source IDs/revisions/digests and visible gaps; an assistant must not decide that an owner's comment is merely noise. The script's modification-time refresh is a convenience, not authoritative source identity.

## 2. Do not send already handled rows to the same worker again

**Source:** `v2/issue_settled.py:1-8,29-99`.

**Mechanism:** implementation queues omit work already delivered, parked with a reason, or waiting for acceptance. A later return/refutation makes it relevant again. The comments explain why repeatedly handing each new session the same full queue caused repeated no-change reviews.

**Transfer:** derive a bounded actionable queue from existing Task/Attempt/submission/review facts. Keep `implemented`, `waiting_for_review`, `blocked_on_exact_dependency` and `accepted` separate. A partial or blocked result is not completion, but need not be re-evaluated until its relevant input changes. A worker can proceed with another independent task.

**Owner to inspect:** existing queue projection in `store/launcher.rs` and its Task/Attempt consumers. Reuse them; no new `state.json`/SECTORS/Markdown queue authority. Invalidate a parked decision by exact dependency/source revision or feedback identity, not guessed timestamps. This review does not claim a new queue implementation has landed.

## 3. Remember useful refutations without turning them into permanent bans

**Source:** `v2/state.py:650-731`, `checklist_hashes`, `_blob`, `record_refutation`.

**Mechanism:** preserve the rejected claim together with both the claim identity and source evidence. The script records a normalized checklist hash, commit and implementation/caller blob IDs, so reformatting the same claim is not new evidence and changed source can be reconsidered.

**Transfer:** existing review/feedback records can supply the next agent with: exact finding, why it was rejected, candidate/source coordinates and the condition that changed. This saves repeated investigation of the same false premise. Runtime schema/config/feature changes must also invalidate a reused conclusion where relevant; a single unchanged file is not enough to prove a whole behaviour unchanged.

**Do not copy:** a second REFUTED database, regex-based caller proofs or a permanent blacklist. A cached refutation is advisory evidence, not permission to reject a different candidate or close an Issue automatically.

## 4. Check a cited symbol once and show the result to subsequent agents

**Source:** `v2/make_task.py:40-69`, `cited_missing`.

**Mechanism:** collect referenced names and search the selected repository revision in one bounded pass. The resulting task tells the agent which alleged existing symbols could not be found instead of making every writer rediscover the same stale premise.

**Transfer:** a source-preparation diagnostic naming the queried revision, names, search scope and failures. Use the existing captured candidate/source index. Do not invoke a second repository scan on every native send.

**Limit:** zero textual matches means only that this search found no match. Renaming, generated code, macros, another crate or deliberate not-yet-written functionality are possible. It is not proof of dead code, permission to delete a requirement or a task-admission blocker.

## 5. Observe tools and child ownership, not only manager silence

**Source:** `v2/Agent-Watch.py:70-147,149-216`.

**Mechanism:** a manager waiting on a foreground subagent has an open assistant message, but work is still running. Shared Codex daemon logs contain events from several lanes: the script corrects its earlier overcount by matching each child to the actual manager parent rather than counting every visible thread for every lane.

**Transfer:** bounded read-only projections of known active operation/tool, pending callback, current parent-child identity, observed progress and observation coverage. Distinguish waiting on a tool from no observed activity. Native session/child facts remain adapter-owned; current parentage must match the binding/generation.

**Where:** existing `agent.refresh`, attention/family and monitor projections. Reuse authenticated native events and Store readback; do not add private-vendor-database reads to the kernel.

**Do not copy:** age thresholds, edit/read ratios or guessed lane names as proof of idleness or authority to kill/restart a session. A long read-only audit can be productive. Missing local visibility stays unknown.

## 6. Measure time and context volume separately

**Source:** `root-tools/tooltime.py:20-81`, `root-tools/ctxfill.py:22-78`, `v2/oc_timeline.py:53-87`.

**Mechanism:** group actual tool activity into source reading, edits, compilation, Git/network and waiting; separately count output volume by source category. The tool-time script deliberately avoids counting the parent subagent wait as another copy of the child's work.

**Transfer:** first expose already recorded span/operation durations and bounded byte counters. Useful questions are whether repeated Cargo builds, full issue-thread reads, huge tool responses or transport waits dominate a particular task. Only add instrumentation at a concrete missing boundary, using the existing telemetry owner.

**Measurement limits:** `ctxfill` counts characters, not exact tokens, and its characters/4 estimate is not a billing or cache metric. `oc_timeline` attributes an inter-message interval to the first tool; this is an approximation, not measured exclusive CPU/model time. `tooltime` subtracts summed tool durations from a message span; overlapping tools require interval-union treatment for wall time. Report wall time, cumulative agent/tool time, sample window and incomplete observations separately. Do not claim all context is billed anew, or infer low intelligence from these counters.

## 7. Read a running service without using a CLI that can restart it

**Source:** `v2/oc_http.py:1-25,83-121`; `v2/codex_as.py:58-166`.

**Mechanism:** monitoring uses the existing service connection; slow reads time out instead of invoking CLI recovery. Codex has one reader that separates correlated responses from notifications. The archive comments describe incidents where CLI health probing restarted a shared OpenCode service.

**Current reuse:** the standalone OpenCode adapter already has HTTP-native access and a HostSession-owned module link; Codex owns a response/event demultiplexer. Preserve and improve those owners rather than copying Python WebSocket framing or adding another client lifecycle. The archive's endpoint names are historical, not current wire contracts.

**Do not copy:** the unbounded queue/frame accumulation and broad exception swallowing in the old Codex client. Observation must not restart external services. A lost effect response stays unknown/readback-only, not a request to retry the POST.

## 8. A queued steer and a blocked foreground tool are different problems

**Source:** `v2/oc_http.py:16-25,96-122`; `v2/answer_forms_http.py:16-60`.

**Mechanism:** identify the exact current root and pending work before sending another instruction. More steers do not necessarily unblock a manager waiting for a tool/question. The scripts restrict intervention to current managers and their children instead of waking old sessions indiscriminately.

**Transfer to #43/#42:** fresh typed pending callback readback, exact request digest, explicit reply, and the current native background capability. Retain current OpenCode loop-step/queue distinction; callback ACK is not task completion. Background is an addressed operation with native readback, not implicit cancellation or a new prompt.

**Do not copy:** `answer_forms_http.py` choosing a Recommended/first answer automatically, old form endpoints, old session-name guesses or recurring reminders to an idle session. These encode specific past operator choices, not the current owner's blanket consent. No automatic answering/steering is installed by this PR.

## 9. Schedule ready work by actual dependencies and locality

**Source:** `v2/plan_next.py:1-6,37-65`; `v2/state.py:1169-1196` (`blocker_fanin`).

**Mechanism:** later-wave work becomes ready when its actual dependencies and overlapping earlier work are resolved. Prefer a capable available lane already familiar with the files; show which blocker frees several downstream tasks.

**Transfer:** optional ranking/explanation over existing Task dependencies and code scopes, for the manager to choose from. Independent work need not wait for an entire wave or unrelated audit. Keep one actual owner and the existing launch-admission check.

**Do not copy:** `plan_next.paths/deps` treating malformed JSON as an empty set; snapshot absence from an open-Issue export as proof of completion; `blocker_fanin` following only the first downstream dependency as if that covered the full graph. Traversal needs bounded cycle-aware treatment and truthful incomplete coverage. Do not build a new auto-scheduler from TSV files.

## 10. Reuse implementation that was written but never delivered

**Source:** `root-tools/write_rescue.py:1-25,27-40`.

**Mechanism:** an agent sees exact surviving heads and what they still add, rather than writing the same fix from scratch. Sibling snapshots of one attempt are not counted as several separate implementations.

**Transfer:** put a short source-head/path/reason note in the existing relevant Issue/PR. Compare against current source and carry only still-applicable changes. Do not merge an old branch wholesale, rewrite history, resurrect a deleted local directory, or take ownership from another manager. This can assist the existing backlog review without making that review a prerequisite for code.

## 11. Keep evidence tied to the delivered candidate

**Source:** `v2/pr_evidence.py:1-11,39-80`; `v2/conveyor_task.py:662-710`.

**Mechanism:** prefer a submission's exact SHA-specific checklist, verify the result actually names a commit and show a numeric execution outcome rather than confusing planned/pending checks with completed runs. The scripts distinguish a worker result from the later owner's integration.

**Transfer:** use existing candidate/submission/CheckRun identities and GitHub run IDs; include only evidence that covers that candidate and selected scope. This is useful reporting, not another acceptance service.

**Do not copy:** falling back to an unversioned CHECKLIST for another SHA, repairing malformed JSON silently, trusting prose containing `exit 0`, or reverting files outside scope automatically. A recorded command string is not independent execution evidence. No auto-close or merge behaviour is imported.

## 12. Avoid rebuilding unrelated packages

**Source:** `v2/revdeps.py:1-35` and `Launch-Manager.ps1:11-34`.

**Mechanisms:** compute changed packages plus reverse local dependencies rather than rebuilding the workspace on every review; spread expensive starts of one shared service rather than triggering a startup storm.

**Already in ELIOT:** the current Rust workflow calls `tools/ci/package-scope.ps1` to classify changes and resolve local reverse dependencies. Reuse this implementation rather than adding the donor script as a second classifier. The existing launcher/provider/resource mechanisms own admission; a hard-coded three-minute file lock is not a replacement for them.

**Limits:** metadata failure cannot become an empty successful check set. Workspace config/codegen/lock changes can require wider coverage than direct source packages. Historical start intervals and models are not current defaults. This PR adds no throttling or concurrency restriction.

## What not to reactivate

The archive also includes cleanup, session deletion, restarts, automatic form replies, merge/closure scripts and machine-setting changes. Their names or historical success do not authorize running them now. Do not copy their absolute paths, service ports, credentials discovery, package/model pins, default mutation modes or old lane-state files into current runtime code. Source bytes are research input only.

## Delivery and next useful work

This PR delivers one small production change in the existing TaskSpec validator plus this donor map. It does not claim that the other eleven mechanisms are newly implemented, nor that the whole #46 handoff is complete. Proposed follow-ups use existing owners and are not blockers for another agent's current task.

The next small context improvement is the two-line historical `TaskSpec::brief` membership-set correction described in section 1. The next observational improvement should be selected from a concrete missing current tool/callback/span fact, not a new dashboard framework. The queue ideas belong to the existing queue/review consumers after their current implementations are checked.

Verification for this source slice is the existing scoped Rust workflow: formatting and compiler/Clippy. No archive scripts, local/native agent sessions, test suites, accounts, service recovery or load runs are executed. Actual CI state is reported on the PR, not predeclared here.
