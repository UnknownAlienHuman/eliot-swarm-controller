# ELIOT Swarm Controller — execution map, 2026-10-09

**Verified base:** main c82c54a72f3bed39f45078ffb12d97ed4b9a454b (PR #104 merged).
**Status:** actionable research and implementation plan; NOT a claim of release readiness.
**Source of evidence:** full Master Audit v26, accepted owner decisions, existing R01–R51 handoffs, and GitHub PR/Issue status checked on 2026-10-09.

**Current execution, 2026-10-10:** [C7 standalone handoff](C7-MCP-REPAIR-EVIDENCE.json) and [public ACK/restart harness](CORE-ACK-HARNESS-REPAIR-EVIDENCE.json) source repairs are verified at their recorded scope. Affected production Clippy passes on Windows/Linux; affected library cohorts retain 399/412 unique passing tests. [M installed execution](EXECUTION-RESULTS-M.json) completed on eight exact `694794f` packages; both native runs remain unqualified with zero dispatch. [The corrected public core contour](EXECUTION-RESULTS-M-CORE-CORRECTION.json) observed all three manager/hook cases against those exact binaries; optional-worker fault injection remains unavailable. C8 first-dispatch proof and Antigravity MCP scope require source investigation. Historical receipts and upstream result/dependency limits are not promoted by source review.

## Supplied source input: HIGH appendix

The phrase **"full HIGH appendix"** in the master audit refers to **section 8, "Приложение: полный реестр доказанных дефектов, не вошедших в §1–§4"**, of the owner's source document **"Что чинить + полный реестр.md"** (historical source baseline `40591a2`). That section contains 61 source-labelled **[HIGH]** entries, 136 [MED], 82 [LOW] and 128 ungraded entries. The original source was retained outside this repository and has now been supplied locally. The companion **"Реестр подозрений.md"** is a different, explicitly **НЕ доказано** hypothesis inventory. The audit v26/v27 and this map do not replace the original sources.

The owner supplied the original registry and extracted appendix on 2026-10-09 in `C:/Users/kleym/Downloads/Swarm V27 audit map`. All 61 HIGH rows and the additional MED/HIGH row are now classified exactly once in [HIGH-VERDICTS.json](HIGH-VERDICTS.json), with source hashes and comparison evidence in [CURRENT-AUDIT.md](CURRENT-AUDIT.md). The source-access prerequisite is satisfied. Two explicit runtime evidence gaps remain; uncommitted implementation does not upgrade a confirmed defect to a qualified fix. Historical merged fixes are not reimplemented.

## 0. Ground truth before any code

On this base: **37 open PRs, all documentation-only; 8 open Issues.** No open PR contains production code. A docs-only branch is a specification, not a fix.

Already merged, DO NOT reimplement: #75 SQLite bootstrap; #77 changed-line Clippy; #80 host events; #81/#82/#85 scheduler and launcher retry; #83 crash-repairable state marker; #86 request IDs; #87 live MCP authorization; #88 native command closed set; #89–#91 provenance/Task/dispatch receipts; #92 catalog context; #93 OpenCode paging; #94–#98 code-scope and participant context; #100 proposal revision validation; #101 watch deadline; #102 worktree policy; #103 role-specific watch indexes; #104 mailbox delivery identity and typed digest.

**Canonical instructions:** [owner decisions](../../owner-decisions.md), [modularity](../../agent-operations/modularity.md), [old readiness index](../2026-10-07/README.md) (historical only), [donor registry](../../agent_swarm.donors-20260929.toml). A full unabridged audit v26 and updated v27 accompany this map outside the repository.

Work boundaries:

- One manager = one worktree; assigned writers share that manager-owned tree and get disjoint file ownership.
- Writers change production code, never independently merge/close/edit policy, and never run Cargo. Manager performs smallest rustfmt/Clippy gate after the connected code slice is complete.
- Explicit route to native subscription; no arbitrary model/SDK version pin, downgrade, API billing switch, global install or vendor service changes.
- Unknown native effect is not a retryable transport failure. Need exact evidence/readback before replay.
- No second State Store, broker, generic IAM, agent framework, in-memory authoritative scheduler or generic lifecycle DSL.

## 1. Dependency graph (shared-file serialization)

| Track | Ordered connected slices | Why / shared ownership |
|---|---|---|
| A Store authority | #56 → #49 → #51 → #52; #48 after review/check fact identity | kernel-host Store/GM/Operation readers, no competing IAM |
| B process + journals | #65 → #61 → #27 → #71/#72; #62 uses #65/#61 | swarm-process, group/Job, journal/owner |
| C coordination | #34 mailbox; #58 after #100/#56; #33/#35 → #47 | coordination.rs, coordination_threads.rs, reviews.rs |
| D automation/resources | #60 → #68 → Issue #79 → #74 → #64 → #39; #67 after #61 | one Store writer, launcher/automation transition states |
| E native | Codex #29 → #41/#66; OpenCode #28 → #43 → #69; Claude #42; Command #44 → #70; Antigravity #45; OpenCodex #73 | each adapter separate, native protocol owns execution |
| F cleanup | #46 prompt after native contract; #40 MCP after live surface; #59 donor docs; #24 optional | no premature schema/digest changes |

This is an integration/rebase order for overlapping source, not a requirement to block independent non-overlapping work.

## 2. Priority P1 work packages — exact code seam and method

### A1 / #56: typed GM designation and monotonic epoch

**Code:** crates/swarm-kernel-host/src/store/gm.rs (require_authority/handover), Forge, automation/authorization and publication, launcher/native MCP, Operation visibility. Inspect every raw meta(gm) query, epoch unwrap_or(0), target role denylist.

**Fix:** one Store-private validated CurrentGM: registered, enabled Role::Manager, positive epoch, optional non-authoritative binding pair. Monotonic high-water is derived from valid historical immutable gm.handover Operations; A→B→A rotates the fence, valid same-manager session rebind retains it. Missing-after-history / damaged / stale registration distinguishable; no silent reset to epoch=1. Replace duplicate readers then delete them. Unauthorized former GM cannot publish/transfer.

**Donor:** existing immutable Operations + typed Store readers. Do not introduce new IAM engine.

### A2 / #49 #51 #52 (+#48): object authorization

**Code:** Operation get/list/delta; Task/Attempt/submission/CheckRun/family readers; artifact get/read/parts/assemble.

**Fix:** exact positive relation from verified retained Task/Attempt/Operation/candidate/assignment fields and independently authenticated principal. Handler result_json and integrity SHA are not access grants. Separate current action authority from authorized historical read. Reuse a narrow resolver across the three readers; preserve legitimate fleet manager scope. #48 ties Requirements to exact independent review/CheckRun proof; no self-claimed pass as acceptance.

**Reuse:** merged #89/#90/#91 and #95–#100 provenance/context. No parallel ACL implementation for each reader.

### B1 / #65: shared durable journal and publication

**Code:** swarm-process file helpers; swarm-adapter-opencode/src/journal.rs; Claude journal; internal Command journal example.

**Fix:** classify complete history / valid prefix + torn last record / damaged interior. An unresolved input intent must survive, not be silently deleted or POST-replayed. Use temp private file, sync file, exact no-clobber or replace policy, parent directory durability on Unix. One outcome authority and exact version/ack, not independent journal plus outbox truth.

### B2 / #61: finite process cleanup/capture

**Code:** crates/swarm-checks/src/lib.rs, swarm-script-worker, check-input ProbeOwner, Store completion, legacy kernel-host scripts/runner.rs.

**Fix:** independent DirectExit / FamilyDeparture / Capture facts. Finite execution, cancellation grace and capture drain bounds. After timeout retain exact Job/group and CleanupPending, do not claim resource_released. Retained stdout bytes and truncation/error remain observable after partial I/O; cannot map all errors to bytes_written=0. No infinite wait inside Drop. Standalone ScriptRun is sole execution path after actual cutover; remove the legacy duplicate.

**Donors:** swarm-process existing group owner; Tokio process semantics; Ractor stop-and-wait pattern. No PTY for normal finite check commands.

### B3 / #27 (+#71, #62, #72): module supervisor

**Code:** swarm-supervisor/src/supervisor.rs monitor_owner_helper, confirm_module_hello, update_status; descriptor.rs; process owner.

**Fix:** old verified owner receipt and newly spawned helper are separate generations; old owner.json is not a foreign new owner. Cached worker must be validated when the new exact owner arrives. Store-confirmed module.hello gates Ready; same-boot live poll cannot regress ProcessRunning to Starting. Use Tokio watch::Sender::send_if_modified with transition checks, no snapshot clone lost updates. Installed module runtime validity follows installed bytes/receipt/root, not mutable build source file. #71 isolates optional supervisor; #62 Git hook phased Installing/Installed/Revoking; #72 Zed finite one-shot ownership.

**Donors:** merged #83 state marker, Tokio watch, Command durable publication.

### C1 / #34: mailbox sequencing, resumable paging, bounded subscriptions

**Code:** kernel-host/store/mod.rs Observation insertion, store/coordination.rs mailbox writes/inbox, swarm-mcp/src/mcp/subscriptions.rs.

**Done:** #101 exact deadline, #103 role-specific watch indexes, #104 mailbox typed digest/collision/duplicate fail-closed lookup. Do NOT rewrite them.

**Fix:** timestamp + UUID key is not insertion ordering. Bind exact committed monotonic Observation/mailbox sequence to scoped delivery in same Store transaction (e.g. row-specific INSERT RETURNING observation_id; no global MAX). Scan cursor = last examined authoritative position. Do not consume first valid item excluded by byte/item budget; stale window returns partial+gap+resumable cursor; DB error never empty page. New cursor-less live subscription obtains authorized high-water for same Principal/visibility before first poll and returns it in ACK. Slow subscriber: finite overflow cut, coherent examined_through/matched_dropped/reached_cut/failure; covered cursor only after gap marker enqueued. Bind both byte and item capacity end-to-end.

**Donors:** SQLite AUTOINCREMENT/RETURNING, CCCC unread-tail locality, Paseo owned subscription lifecycle. Never adopt another ledger or cancel Task on observer detach.

### C2 / #58: current contract decision producer

**Code:** store/coordination_threads.rs validate_contract_ratification, Store contract routes/parser, method_policy and MCP catalog.

**Done:** #100 one verified proposal revision loader/digest.

**Fix:** expose callable coordination.contract.ratify/reject with exact Thread/Task/Attempt/revision/current proposal/digest/current authority; persist immutable decision and minimal Observation; authorize read/list; resolved contract Thread requires ratified current revision, not arbitrary Operation. Reject does not automatically resolve. One revision cannot later be both rejected and ratified. Do not add second proposal loader or LLM consensus.

### C3 / #35 #33 #47: Review and Concilium correctness

**Code:** store/reviews.rs, store/concilium.rs, submission feedback/repair.

**Fix:** reviewer replacement by exact current assignment/slot CAS even after ReturnForCorrection or unanswered reviewer; late results historical only. Review listing must be SQL-bounded scan, not full-history materialization followed by LIMIT. Auth errors distinct from Store failure. Verify exact review assignment link for owner/successor reads. Concilium closes rounds and slots consistently and separates latest positions from round history. Multi-finding correction: one ordered exact findings package digest → one feedback Operation → one native repair delivery. Delete single-finding consumers after real migration.

**Donor:** existing immutable reviews/dispositions and State Store, not Magentic-One consensus judge.

### D1 / #60 + Issue #79: independent automation domains

**Code:** Store::reconcile_automations_once, automation_work_dispatch::recheck_pending, publication, goal progression, GitHub projection, review disposition.

**Fix:** independent domain transactions and domain-specific Applied/Pending/Skipped/Quarantined classification. Quarantine retains exact identity/digest and advances cursor only atomically with disposition. Unknown/SQLite/savepoint/commit error remains hard; never blanket catch → Ok. Eliminate mem::take pending-vector truncation before introducing continue-on-error. Issue #79 independently isolates scheduler due source semantic failures. Already merged #81/#82/#85 pacing and retries are out of scope.

**Donor:** existing publication consume_event_isolated and rusqlite SAVEPOINT, not a second dead-letter queue.

### D2 / #39 #68 #74 #64 #67: lifecycle + resources

**Capacity:** store/capacity.rs malformed ledger is damaged evidence, not empty; terminal of same execution wins over prior started; exact resource lease/Attempt effect graph, not any unrelated queued Task Operation. Hold Unknown until verified release.

**Forge:** #68 exact remote ref readback under non-force contract; no force-with-lease shortcut that expands write authority. Unknown push requires readback before possible replay.

**Launcher:** #74 bounded preview is not full authoritative plan, #64 typed current provider condition at launch admission, no static model/version gate. #67 ScriptRun one immutable go/deny decision under cancel race; depends on #61.

## 3. Native harness implementation packages

| PR | Vendor boundary | Required completed connected slice | Do NOT |
|---|---|---|---|
| #29 | Codex app-server | Provisional Pending/Candidate/Active root; config/cwd/model readback; steer by expectedTurnId and exact persisted input evidence | Second thread/start when first effect may have happened |
| #41 | Codex and Muse | One continuous response/notification/server-request demux; full+delta usage by account/window/bucket; current role scope | Treat every grantedCapability as RPC; generic success as quota recovery |
| #66 | Codex Goal | Active-root predicate reused from #29; single Goal authority and historical decoder | Duplicate current Goal registers |
| #28 | OpenCode adapter | Durable torn-tail journal, one IPC hello per generation, maintain Child/stop ownership, correct result paging already #93 | Reconnect+blind native POST replay |
| #43 | OpenCode V2 | Durable history, queue/loop-step distinction, typed pending questions and replies, native background | Claim OpenCode HTTP steer has Codex target CAS |
| #42 | Claude Agent SDK | Callback/defer to exact reply; intent once + Unknown once; terminal SDK bridge retirement + proved family departure before replacement | Hard-coded SDK release or same-process root reopen |
| #44 | Command ACP | Owned interactive session, model/options, permissions, recover/readback; separate existing one-shot -p path | Full replacement SDK/PTY or ChildGuard that kills unknown owner |
| #70 | Command output | Durable result artifact before capture release, retain partial capture errors | Empty diagnostic after late failure |
| #45 | Antigravity stream-json | Model selected from current native route, exact init/conversation, one active turn; cumulative usage snapshots/deltas | REQUIRED_MODEL_ID or pretend credits field exists in warm stream |
| #73 | OpenCodex | Positive native config/readback before Applied | No-effect/Unknown reported as configured |
| #69 | Adapter journals | Reclaim only acknowledged/unreferenced evidence | LRU deletion of unresolved intent |

Official protocol sources: [Codex](https://github.com/openai/codex), [Muse SDK](https://github.com/meta-models/muse-code-sdk), [OpenCode V2](https://github.com/anomalyco/opencode), [ACP Rust SDK](https://github.com/agentclientprotocol/rust-sdk), [Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk/overview). Use current installed subscription and actual advertised capabilities; donor SHA is research coordinate, not version pin.

## 4. Modularity, TaskPrompt and optional work

**#46:** Store operations::dispatch builds one exact versioned TaskPrompt byte envelope: Task/Attempt/revision, snapshot digest, canonical brief, prompt bytes/digest. Convert active adapter consumers; delete divergent raw-snapshot renderers AFTER cutover.

**#40:** remove production swarm-kernel-host → swarm-mcp dependency. Shared data-only METHOD_REGISTRY and immutable schema objects, preserving digest preimage exactly. Current authorization remains dynamic each request. No new proxy/search server. Donors RMCP schema memo and MCPProxy exact describe.

**#59:** verified donor inventory, field reports and implementation playbook; no automatic dependency adoption. **#24:** optional WSL2/Kilo/vLLM local-model program, not core release blocker.

## 5. Donor map: exact reuse versus anti-pattern

| Donor | Reusable unit | Consumers | Reject |
|---|---|---|---|
| Internal ELIOT Store/Operations | One transaction, immutable receipt/Observation, exact readback | A–D | Second truth or broker |
| Internal swarm-process | Group/Job/marker owner, installed immutable evidence | B/E | Private process registry clone |
| [SQLite](https://www.sqlite.org/lang_returning.html) | Per-row sequence, keyset, savepoint | #34/#60/#35 | Global MAX or second database |
| [Tokio watch](https://docs.rs/tokio/latest/tokio/sync/watch/struct.Sender.html#method.send_if_modified) | Atomic status update, bounded Tokio channel | #27/#61/#41 | PID exit as family proof |
| [CCCC](https://github.com/ChesterRa/cccc) | Latest/unread-tail bounded scan locality | #34 | CCCC ledger replication |
| [Paseo](https://github.com/getpaseo/paseo) | Source-owned subscriptions / unsubscribe | #34 | Observer disconnect stops admitted native Task |
| [Ractor](https://github.com/slawlor/ractor) | Supervision priority and stop-and-wait | #27/#61 | New actor runtime / implicit child-stop proof |
| [Goose](https://github.com/aaif-goose/goose) | Validated schedule recipe, exact bytes and path | #38/#46/#62 | Goose scheduler or infinite retries |
| [Restate](https://github.com/restatedev/sdk-rust), [DBOS](https://github.com/dbos-inc/dbos-transact-py) | Correlation before external effect, durable readback | #28/#42/#68 | Second workflow engine or exactly-once illusion |
| [ACP Rust SDK](https://github.com/agentclientprotocol/rust-sdk) | Complete typed transport/session permissions | #44 | Wrapper PID/process-group ownership mismatch |
| [Kingfisher](https://github.com/mongodb/kingfisher) | Offline scanning and structured redaction evidence | optional diagnostics | Scan verdict as admission authority or unapproved live validation |
| [RMCP](https://github.com/modelcontextprotocol/rust-sdk) / [MCPProxy](https://github.com/smart-mcp-proxy/mcpproxy-go) | Schema memo, discovery → exact describe | #40 | Duplicate MCP frontend and mutated v1 digest |
| [Magentic-One](https://github.com/microsoft/autogen) | Separate critique from verified acceptance | #47/#48 | LLM progress or consensus as controller authority |

New donor API: record exact upstream code/semantics, licensing+native dependency closure, applicability to existing consumer, counterexample, and old ELIOT writer to delete. No whole framework import just for one helper.

## 6. All 37 open PRs — one authoritative owner list

A: #56 #49 #51 #52 #48.
B: #65 #61 #27 #71 #62 #72.
C: #34 #58 #33 #35 #47.
D: #60 #68 #74 #64 #39 #67.
E: #29 #41 #66 #28 #43 #42 #44 #70 #45 #73 #69.
F: #46 #40 #59 #24.

Every item above is currently **docs-only**. Use that PR's handoff as task context, then submit a narrow code PR from latest main. Do not merge outdated Markdown branches as product implementation.

## 7. All 8 open Issues — acceptance

[#99](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/99) cross-Task relation contract (explicit authority/revision and both sides of delivery before code); [#79](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/79) scheduler due-source isolation; [#18](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/18) operator migration procedure; [#11](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/11) quiet-host 1,000 events/s benchmark still unproven; [#5](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/5) live Muse/Windows qualifications; [#4](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/4) native child discovery; [#3](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/3) OpenCode service restart under in-flight inputs; [#1](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/1) composed Codex/OpenCodex route qualification.

## 8. Agent work-unit template

For each task send the agent ONLY:

    Owning PR and exact base main SHA
    Source paths/functions (write-owned vs read-only)
    Current producer → effect → durable fact → consumer trace
    One valid input, one invalid input, one crash/Unknown case
    Expected minimal diff and deletion target
    Existing helper / approved narrow donor
    Forbidden changes / one current-authority invariant
    Output: changed code + exact source evidence + commit/Clippy status

Quality workflow: manager owns one worktree, writers do not run Cargo, changed Rust is formatted and targeted Clippy is manager-only. No comprehensive tests until completed product phase, but code PR never claims a skipped integration/live test passed. Review native side effects and principal grants independently from implementation self-report.

**Final completion definitions:** (1) audit: all confirmed HIGH traced and owned/refuted on current main; no floating defects or contradictory handoffs; (2) core code: connected producer→Store→consumer for required tracks and duplicate implementations removed; (3) release: real Windows/Linux/native/account/load/fault qualification. These are three different milestones.
