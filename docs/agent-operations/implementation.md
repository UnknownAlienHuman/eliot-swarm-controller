# Implementation Plan — Rust Operations on Behalf of the Manager

Revision 21 · 2026-10-17 · Canonical Version 3 Implementation Plan.

**Current implementation status:** See [Implementation Status](../implementation-status.md) for historical CI runs, commit receipts, and qualification logs. This plan owns the normative O1–O11 implementation blocks and works together with M1–M6 in [Modular Runtime](modularity.md).

For the current source delta and candidate order, use the [refactoring map](refactoring.md). O1–O11 and M1–M6 remain the scope map; completed source is not reopened as an empty implementation task.

Read [README](README.md), then the relevant [Configuration](configuration.md), [Architecture](architecture.md), [Delivery](delivery.md) and [Donor map](donor-map.md).

## 1. Delivery discipline

Use one Store (1 serialized `IMMEDIATE` writer + concurrent WAL read pool), one authorization evaluator, one Task/Attempt/Operation lifecycle, one unified `croner`-backed scheduler, one watch service, one dashboard projection, and one unified `ModuleSupervisor`. Do not create parallel versions of the same service or store relational collections as JSON blobs in the `meta` table.

One manager owns one mutable worktree and one product candidate. Internal writers work on non-overlapping parts of that Issue and do not run Cargo or publish independent fragments. The manager integrates/reviews, then runs scoped formatting and minimal warnings-denied Clippy once on the final candidate. Keep integration test suites and mock executables in `tests/` or `swarm-test-support`, never inside production `src/` trees.

Implement and maintain a complete, productive local `manager -> workspace -> agent.open -> task.dispatch -> turn-correlated agent.result -> task.submit -> review -> repair -> acceptance` path before adding optional features. Each increment includes producer, handler, registration and result reader. No generic workflow DSL or framework-only milestone without a working consumer.

Manual operation is the default; selected automations run for their manager. No global mode/control API, second activation ledger or extra Root approval for already permitted actions.

## 2. Dependencies

```text
O1 manager-owned definitions + common authorization/admission + relational Store tables
  -> O2 durable intake/routing + WAL read-pool monitoring projections
       -> O3 runtime adapters (Rust + official SDK drivers) / direct control / turn-correlated results
       -> O4 optional GitHub/Git mapping (`octocrab`) and pool distribution
       -> O5 Rust hook observation
  -> O6 optional script bundles / runner (`swarm-script-worker`)

O1 + O2 + actual local Task/launcher/candidate path
  -> O7 manual review/repair first, manager-enabled handoffs alongside it
     O4 is required only for its GitHub effects, not local audit

O1 + O2 -> O8 cron (`croner`) / typed rules -> O9 server Goal / shared reminders
O10 completes shared MCP/CLI parity and cross-PR contract integration
O11 integrated qualification and installation handoff
```

O3/O4/O5 can proceed independently after shared types are agreed, with non-overlapping file ownership. O7 does not require an interpreter, a GitHub App or O6. Expose usable methods with each production slice; O10 is not the first call site.

## 3. O1 — Ownership, configuration and shared actions

Existing anchors: `src/model.rs`, `src/policy.rs`, `src/config.rs`, `src/store/mod.rs`, `src/store/operations.rs`, submissions/acceptance/forge, and MCP profiles.

Work:

- Define one per-automation owner/scope/trigger-or-preset/actions/settings/revision/enabled record in a dedicated relational SQLite table (never as hex-encoded keys in `meta`). Default false; explicit manager save may enable in one request.
- Implement config get/preview/apply and explain; preview is optional assistance. Apply validates expected revisions, explicit patch semantics and only named entries. Disable-only works locally even when dependencies are unavailable.
- Implement `include_existing` and a transactionally recorded activation cut. Ordinary edits preserve unaffected cursors and slots; omitted entries/steps do not become new enabled work.
- Derive manager ownership and internal execution context from authenticated/retained state, not caller-supplied `run_as`. Do not forge Principal/caller IDs or distribute reusable manager tokens.
- Audit `Principal::owns` and the special internal Scheduler path. Preserve legacy scheduled-check receipts without using its cross-owner exception for new automations.
- Reuse current manager action rights and existing applicable grants. No new mandatory automation grant; no service-only permission bypass. Custom role labels cannot inherit generic writer rights.
- Make on-behalf operations readable/explainable/cancellable by the authorized owning manager through retained linkage, not only technical `caller_id`. Execute all read queries (`*.get`, `*.list`, `explain`) through the Store's read-only WAL connection pool and scope before pagination.
- Shared semantic slots across manual/automatic callers coalesce equivalent actions and expose incompatible choices. Automation IDs, edits and event timestamps cannot bypass those slots.
- Preserve admitted original/effective parameters. Current enabled/rights/allowed-target checks precede effect start; current preferences resolve only for new admissions.
- Serialize disable/narrowing with effect start. Manual replacement of a provably never-started action retains both receipts in one slot; unknown/sending work needs readback.
- Retain late trusted outcomes separately from new-effect authorization. Revocation does not justify dropping already observed effects or accepting unauthenticated evidence.

Done: one manager can enable only an audit helper in one call, use manual commands alongside it, find its Operations via concurrent WAL reads, and stop future helper starts without losing active results.

## 4. O2 — Durable intake and shared observation

Reuse Store events/deduplication, change signals and MCP lag/resync. V3 migrates capacity to relational tables while retaining active-plus-reserved accounting from the Agent of Empires pattern.

- Normalize source/native identity, source cursor/epoch, controller cursor, work/binding scope and coverage. Source failure is unknown, not empty success.
- Commit verified intake before durable ACK. Dispatcher cursor advancement and Operation/reservation or pending-subject state commit together in one short transaction.
- Treat `watch`/MCP notifications as lossy/coalesced hints. After reconnect/startup, read every unprocessed committed fact through a bounded high-water cut. No lost work between committing a cursor and enqueuing an in-memory command.
- Retain waiting subjects and wake conditions in relational tables; dependency/capacity changes re-evaluate them without producing failed jobs every tick or requiring another source event.
- Share readers and projectors over the WAL read pool. Snapshot plus after-cursor reads must cover changes racing with subscription. No full history scan or model status query per viewer.
- Separate terminal/control evidence from live presentation. Slow streams cannot starve completion, permission replies or manual actions.
- Bound parser/OS/Git blocking work, queue bytes and serialized output. Keep owner/route fairness and shared-account capacity in indexed SQL columns (`active_slots`, `reserved_slots`), never in a single `CapacityLedger` JSON blob.
- Use `notify` for event-driven parent-directory file watching when observing local configuration or workspace hints.

Done: monitoring works with no enabled automation, dropped notifications recover without lost/duplicate work, and one malformed source or waiting queue cannot block unrelated owners.

## 5. O3 — Runtime adapters, streams, turn-correlated results and direct control

- Use official Rust protocol crates for their actual protocols (`agent-client-protocol` for ACP-speaking adapters, `rmcp` for MCP), documented native contracts (Codex app-server, OpenCode HTTP/SSE), and official vendor SDKs where needed (`@muse-code/sdk` for Muse, `@opencode/server` for OpenCode, `@anthropic-ai/claude-agent-sdk` for Claude). Follow the [donor boundary](donor-map.md#9-official-vendor-sdks-and-native-protocol-boundaries). The Rust adapter owns `ModuleLink`, supervision, request/prompt digests and Store receipts. A thin SDK driver may retain native protocol/session handles; durable ELIOT work and retry policy stay in Rust/Store.
- Qualify installed protocol/capabilities independently of historical release equality. Preserve integrity checks on the selected local artifact. Apply the [schema and identity rules](architecture.md#schema-and-identity-boundaries): additive metadata compatibility, strict required authority fields, normalized declared SHA inputs, and unchanged retained byte contracts.
- Keep the launch path direct and non-blocking: `workspace lease -> agent.open -> task.dispatch`. Pre-populate MCP server endpoints and scoped participant credentials in the launch config before `agent.open`, and treat native MCP connection / `tools/list` readback as asynchronous readiness telemetry—**never block `task.dispatch` in an `awaiting_native_mcp` state**.
- Implement deterministic **turn-scoped result correlation** (`swarm.normalized_result_page@1`) across all adapters: use explicit native `parent_input_id` where provided by the protocol (Codex), and use the **serialized active turn / busy window** `(binding_id, binding_generation, native_session_id, active_turn_id)` sealed to `task.dispatch` / `agent.send` where the native stream is session/turn-scoped (OpenCode, Antigravity, Claude — following the Waku/Paseo pattern). Never refuse a completed model response with `RESULT_BODY_UNAVAILABLE` or permanent `Unknown` status due to a missing per-chunk parent pointer.
- Maintain native binding/generation/session/turn/family and actual process ownership (`cccc-windows-process` + `sysinfo`). Never restart/adopt by heuristic merely to activate a new adapter.
- Wire direct inspect/steer/reply and supported cancellation with exact semantics/readback. Unsupported active-turn input is not silently queued into another task.
- Stream native-supplied text/reasoning/tools with bounded single-copy storage, cross-chunk `atlas-redact` secret scrubbing, and item/final-delta reconciliation (Paseo pattern). Hidden reasoning remains unavailable.
- Record requested/effective route/model/options and real shared-account usage basis. A failed catalogue/probe is a gap, not an empty supported inventory.

Done: a team of agents across all supported runtimes can be launched, dispatched, observed, and collected (`agent.result` returning the complete assistant output) manually or automatically without pre-dispatch deadlocks or lost children.

## 6. O4 — Optional GitHub/Git and distribution

- Use the `octocrab` donor crate as the single maintained Rust GitHub client and rate-pacing boundary with private handles and actual endpoint permissions. Configure read retries explicitly; ambiguous non-idempotent writes must never be invisibly replayed beneath Store. Do not maintain a second handwritten HTTP/GitHub stack.
- Verify raw webhook HMAC/context, durably intake, dedupe and reconcile conditional paged reads. Preserve selected source comments and unknown gaps.
- External item identity maps to one Task; source revision is distinct from updated timestamp. Correlated ELIOT projection echoes do not revise requirements or retrigger the same effect.
- Pool import/queue read never starts work. Manual/automatic dispatch share current Task/Attempt/manager/workspace reservations and manager-selected order.
- Readiness precedes page limits; stalled account/capacity does not hide unrelated runnable work. No heartbeat ownership stealing or silent fallback.
- Reuse the launcher and bounded assignment/peer context. Git hints require readback; current ownership is ELIOT state, not blame or branch text.
- Keep all costly/network/Git work outside Store transactions and per-target final serialization, not one global merge lock.

Done: manager imports/reads a pool via `octocrab`, selects work manually or enables its distributor; local work remains possible when GitHub is unused/unavailable.

## 7. O5 — Hooks

- Inventory event/schema/phase/veto/lifetime/install readback for each actual runtime/plugin path.
- Rust `swarm hook emit` or adapter ingress authenticates source and bounds payload.
- Setup preserves existing hooks; service/global-setting changes are separately explicit.
- Observation alone never starts an auditor/script/model. Enabled entries map facts to actions through O1/O2.
- Async after-hooks cannot veto past work. Long actions stay off callback paths. Optional telemetry backlog cannot block compaction.
- Missing Rust-reachable integration is a named capability gap. Mandatory action safety is independent from optional automation.

Done: an observed commit updates state, and an explicitly enabled manager helper can notify an auditor without a new control mode or inline model call.

## 8. O6 — Optional script bundles

- Reuse artifact/process owners (`swarm-process`, `swarm-script-worker`); add complete bounded bundle publication rather than untracked filesystem writes. Validate support files, paths and input/result schemas (Goose recipe capture pattern).
- Content activation is not execution. Authorized direct runs and manager-enabled triggers use one runner.
- Manager-configured event triggers accept any exact source/kind, including new kinds, with optional supported status predicates. Submissions, hooks, messages, answers, completion and detected failures are examples, not a fixed whitelist. Reuse the committed event ledger, per-entry cursor, pending records and normal action admission; no script-specific bus or second scheduler.
- Support events without Task/Attempt using a versioned invocation context and additive verified schema migration. Preserve deployed migration digests and real Task scope; Task-owner API effects remain unavailable without an actual authorized Attempt.
- Project closed authorized event metadata and causal references. Never forward legacy payload bodies, native detail or credentials; normalized and legacy views of one occurrence share action deduplication identity.
- Commit exact pending intent with cursor progress; retain it across restart and explicit owner transfer. Before starting, recheck current entry, owner, script and applicable scope. Empty rules disable automatic runs without disabling manual invocation.
- Resolve prepared installed interpreters and capture effective bundle/environment for each admission; no callback installer, fixed release or mid-run mutable dependency substitution.
- JSON stdin/separate argv, bounded output, actual descendant ownership and evidence validation. Exit zero alone is not complete success.
- Scope invocation API rights and on-behalf/cause identity. No manager token or permission to enable arbitrary workflows.
- Distinguish trusted-local OS rights from actual isolation. Job Objects are lifecycle tools, not sandboxes. No silent trust downgrade.
- Disable prevents future independent actions, not fictional cancellation of already running OS code.

Done: user/authorized agent scripts are optional; normal native delivery never depends on their interpreters.

## 9. O7 — Local reviewed delivery and selected automation

- Deliver direct `review.assign/get/submit`, applicable manager feedback, repair and acceptance first or alongside automatic callers.
- Applied retained submission, not queued submit/commit prose, creates review eligibility. Multiple matching entries/manual requests reserve one declared review slot.
- Bind result permission to the assigned auditor/attempt; sponsor identity is not verdict authorship. Replacements retain old review attempts and require observed prior disposition.
- Keep pass/actionable changes/inconclusive, coverage and exact source/evidence distinct. Do not convert infrastructure failure to code repair or disregard a demonstrated defect hidden in remarks.
- Aggregate audited state locally; it is neither a label nor acceptance. Same-content resubmission cannot erase an unresolved finding; changed relevant source remains reviewable even with unchanged checklist bytes.
- Implement the Delivery transition matrix, not one cursor over a presumed fully automatic pipeline. Manual prerequisite completion can enable a selected later step.
- Allow legitimate repaired-submission descendants. Suppress unchanged repeated work/notification echo, not every event carrying the same automation ancestry.
- Apply manager feedback through current candidate checks; separately deliver correction to the actual owner. Define the source-freeze to correction-phase transition so the rule protecting candidate A does not permanently prevent fixing it.
- Local audit has no first-push dependency. Optional review upload is a distinct permitted effect; acceptance/publication retain exact candidate, current rights and GM/epoch checks.
- GitHub labels/Checks/summary/closure are separate effects. Middleware replay, projection echoes, uncertain creates and append-style annotations must not create duplicated work.
- Merge head, integration base, queue acknowledgement and landed result are different. Preserve actual protection/API limitations; no force/protection bypass or required native GitHub merge queue.

Done: one complete manual return/repair/re-review cycle works, and enabling only review assignment or fuller selected delivery changes the caller, not the correctness guarantees.

## 10. O8 — Cron and typed event rules

Consolidate scheduling into one unified scheduler in `swarm-automation` / Store using `croner` and relational SQLite rows; do not maintain parallel interval/cron engines.

- Editors update the same per-entry owner/enabled/settings. No double switches.
- Use `croner` for complete cron expression evaluation, explicit timezone/DST/overlap/misfire semantics and next occurrences. Ordinary latest-only catch-up; backfill only when selected and safe.
- Separate calendar identity, considered occurrence and invocation. Preference/label/enable edits do not recreate consumed slots; old interval identities and receipts remain recognized.
- Cursor/occurrence disposition and action/pending record commit together. Dependence on volatile notification delivery is forbidden.
- Direct run-now is one manual action with recurrence still disabled. Restart reconciles saved enabled intent; intentional re-enable uses its chosen inclusion policy.
- Typed rule validation plus domain progress/causality checks; a failed notification does not spawn an explanation agent. Respect real capability and account backoff rather than retrying every tick.
- Keep generic event selection separate from typed action validation. New provider/harness adapters translate the common command/event contracts; they do not acquire a separate automation engine or redefine shared action correctness.

Done: schedules/rules can be saved, enabled once by their manager, stopped and recovered without lost occurrences, burst replay or duplicate effects.

## 11. O9 — Goal and reminders

- Reference existing assigned Tasks/evidence. No new Task graph or hidden replacement model loop.
- Tracking starts no work; selected progression uses the manager entry and ordinary action rights.
- One actual continuation owner. Native Goal/live children require supported reconciliation before a competing server continuation.
- Achievement uses declared evidence/evaluator, not token/commit count or prose.
- Reuse shared watches/subject indexes/timer; direct notices are not assignments, approvals or new model turns.

Done: tracking, one-shot reminders and manager-enabled progression coexist without competing owners or compulsory automation.

## 12. O10 — Shared MCP/CLI and cross-PR consistency

Keep #22's small eager cores and deferred groups powered by `rmcp`. Wire exact schemas to implemented handlers and WAL read-pool readers; no generic dispatcher, redundant public aliases, CLI subcommand duplication between `swarm-cli` and `swarm-kernel-host`, or mode/control API.

Reconcile these interfaces in the same implementation increment:

| Interface | Combined contract |
|---|---|
| Assigned reviewer | Canonical `review.submit`, exact-assignment `review.get` and linked `operation.get` may finish/read after release while authenticated and unrevoked. `task.request_changes` stays manager disposition; explicit legacy `Reviewer` compatibility remains separate. |
| Participant registration | Sponsored review scope may submit its exact review slot; an ordinary current Attempt owner or `producer_ref` Participant may submit only its exact current Task revision and Attempt. Neither basis acquires Task creation, claim, acceptance, manager runtime or GM recovery rights. |
| Operations/status | Manager sees authorized on-behalf actions despite service requester identity; result ingestion and new-effect permission differ. |
| Launcher | One candidate/worktree owner, direct `workspace -> agent.open -> task.dispatch` path without `awaiting_native_mcp` deadlock, turn-correlated `agent.result`. |
| Watches | One service and one enabled/owner record for recurring actions; one-shot notices create no model work. |
| Catalog | Profile permission differs from schema loading; list changes do not prove model consumption. |

Register authorization, input validation, dispatch, durable links, result projection, CLI/MCP schema, profile tests and current documentation together. A missing optional source produces a scoped gap, not a disabled manual toolbox. `automation.config.explain` uses normal reasons such as waiting for capacity/manager or an existing result, not a fabricated failure.

## 13. O11 — Qualification

| Case | Required outcome |
|---|---|
| Default and one-call enable | New/imported entries are off; manager enables only audit in one request without Root/mode/grant ceremony. |
| Manual coexistence | Full manual path works; selected automation neither hides manual tools nor enables omitted steps. |
| Direct launch & turn-correlated result | `agent.open -> task.dispatch -> agent.result` completes without `awaiting_native_mcp` deadlock and returns the full assistant result body across Codex, OpenCode, Antigravity, Command, and Claude. |
| Current rights | Service uses manager scope, not Scheduler's legacy exception; no fabricated Principal or privileged fallback. |
| Operation visibility & WAL read concurrency | Authorized manager can read/cancel/explain on-behalf work via concurrent WAL reads without blocking behind writer transactions or leaking another manager's jobs. |
| Commit/signalling crash | Crash after commit before notification loses no audit; replay causes no duplicate. |
| Capacity wait | Considered subject resumes on relational SQL capacity/prerequisite change without a new submission or failed-job storm. |
| Activation cut | Existing-work evaluation races safely with new events; omitted inclusion never drains old backlog. |
| Config edit | New admissions use new preferences; retained jobs keep inputs; removed actions cannot start. |
| Disable/start | Disable-first holds; start-first remains in-flight/readback; no pretend recall. |
| Manual/automatic race | One semantic slot; incompatible choice exposes conflict; unknown effect is not replayed. |
| Review repair | A -> findings -> applied return -> corrected B -> fresh audit works with the same enabled automation. |
| Loop suppression | Echoed events/new IDs on unchanged defective source do not consume more model work; genuine correction can progress. |
| Review replacement | Inconclusive slot can be deliberately reassigned after prior disposition; late result cannot author the new attempt. |
| Late/revoked result | Trusted old evidence retained historically; new effects remain forbidden; unauthenticated verdict rejected. |
| Source freeze | Candidate A remains immutable while owner can enter the explicitly returned correction phase safely. |
| Native/capability truth | Shared-family counts scoped; listed schemas not claimed model-loaded; no extra model launch for a checkbox. |
| GitHub (`octocrab`) | Projection echoes do not revise Tasks; uncertain writes are not duplicated by transport middleware. |
| Cron/restart (`croner`) | Old slots recognized; calendar edit, toggles, DST and restart do not duplicate consumed work. |
| Scale | Slow streams and busy owners cannot starve completions/control; no per-participant polling or full-history scans. |

Run synthetic registered populations separately from paid active models. Measure bounded Store latency, WAL read concurrency, source gaps, routing fairness, RSS/handles (`sysinfo`), token/tool context, repeated-work rate and manager attention. Stage live runs on small teams before larger fleets; registration count is not active-turn capacity.

No paid model runs, live repository writes, service installation or owner-machine cleanup in CI.

## 14. Migration and completion

Use forward relational SQLite migrations for required entry, cursor, pending-subject, review/occurrence/slot, capacity reservation and on-behalf linkage tables and indexes. Operations remain execution authority; current indexes do not become another job state machine, and the `meta` table is never used for collections or JSON blobs. Preserve historical policy, legacy scheduler receipts and unknown effects.

The proposed old PR #23 global modes/control API were never deployed: do not implement aliases for them. Likewise do not silently loosen current GM-only code because a future document names a manager tool. Adopt the explicit new accepted policy with the common handler and retain earlier evidence.

Setup collects trusted runtime/source/credential/trust values locally and creates no live automation as a demonstration. Ordinary crash recovery preserves owned saved choices; untrusted/unattributable imports do not constitute enabling consent. No fixed software pins, silent installation or heuristic live-session adoption.

Completion means a manager can operate manually, enable only chosen helpers, understand what ran on their behalf and complete a real integrated `launch -> dispatch -> result -> submission -> review -> repair -> acceptance -> publication` cycle without duplicated authority, lost events or bureaucratic intermediate gates.
