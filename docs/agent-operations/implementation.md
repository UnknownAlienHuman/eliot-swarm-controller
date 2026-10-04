# Implementation Plan — Rust Operations on Behalf of the Manager

**Current implementation status:** See [Implementation Status](../implementation-status.md) for current source, gate, native qualification, and remaining-work facts. This plan owns the O1–O11 implementation blocks.

### Historical C7/C8 qualification snapshots
Revision 20 · 2026-10-04 · current source 4ecc030e072be1b3fdf39e2b3ead122953e4de82; prior C9 CI baseline 2e609ecf7d826da7019fe5e5f2ed397a992bc45a.

Read [README](README.md), then the relevant [Configuration](configuration.md), [Architecture](architecture.md), [Delivery](delivery.md) and [Donor map](donor-map.md). The program remains partial. C4's 221 Rust tests apply only to `2607c8858e573ae40459c27d76d8ae9e1ca9f8fc`.

Historical C7/C8 CI details and exact source pins are in [Implementation Status](../implementation-status.md).
The runtime `0.160.0` schema and three DTOs are repaired, and Node `--check` passed without model execution; native-compose/trial remains unqualified. All local model/inference execution is deferred by owner. The C6 catalog had 91 `ToolSpec` entries and includes five passive watch kinds, integration sync, recomputed overlap, manager-admitted asynchronous workspace lease, exact Task claim and `agent.open`.

## 1. Delivery discipline

Use one Store, authorization evaluator, Task/Attempt/Operation lifecycle, scheduler, watch service, dashboard and launcher. PR #22 supplies proposed peer/Participant/MCP primitives; inspect what actually landed before adding files. Do not create parallel versions of the same service.

One manager owns one mutable worktree and one product candidate. Internal writers work on non-overlapping parts of that Issue and do not run Cargo or publish independent fragments. The manager integrates/reviews, then runs scoped formatting/minimal warnings-denied Clippy once on the final candidate. Broad tests/load/live qualification follow the completed production path or explicit acceptance phase.

Implement a complete local manager -> submission -> audit -> return -> correction path using the smallest necessary O1/O2/O3/O7 work before adding every optional provider, GitHub feature or script trigger. Each increment includes producer, handler, registration and result reader. No generic workflow DSL or framework-only milestone without a working consumer.

Manual operation is the default; selected automations run for their manager. No global mode/control API, second activation ledger or extra Root approval for already permitted actions.

## 2. Dependencies

```text
O1 manager-owned definitions + common authorization/admission
  -> O2 durable intake/routing + monitoring projections
       -> O3 Rust native adapters / direct control / streams
       -> O4 optional GitHub/Git mapping and pool distribution
       -> O5 Rust hook observation
  -> O6 optional script bundles / runner

O1 + O2 + actual local Task/launcher/candidate path
  -> O7 manual review/repair first, manager-enabled handoffs alongside it
     O4 is required only for its GitHub effects, not local audit

O1 + O2 -> O8 cron / typed rules -> O9 server Goal / shared reminders
O10 completes shared MCP/CLI parity and cross-PR contract integration
O11 integrated qualification and installation handoff
```

O3/O4/O5 can proceed independently after shared types are agreed, with non-overlapping file ownership. O7 does not require an interpreter, a GitHub App or O6. Expose usable methods with each production slice; O10 is not the first call site.

## 3. O1 — Ownership, configuration and shared actions

Existing anchors: `src/model.rs`, `src/policy.rs`, `src/config.rs`, `src/store/mod.rs`, `src/store/operations.rs`, submissions/acceptance/forge, actual #22 code and MCP profiles.

Proposed units, reused when already present:

```text
src/automation/actions.rs
src/automation/config.rs
src/authorization.rs
src/store/automation.rs
src/store/configuration.rs
src/runtime/profiles.rs
```

Work:

- Define one per-automation owner/scope/trigger-or-preset/actions/settings/revision/enabled record. Default false; explicit manager save may enable in one request.
- Implement config get/preview/apply and explain; preview is optional assistance. Apply validates expected revisions, explicit patch semantics and only named entries. Disable-only works locally even when dependencies are unavailable.
- Implement `include_existing` and a transactionally recorded activation cut. Ordinary edits preserve unaffected cursors and slots; omitted entries/steps do not become new enabled work.
- Derive manager ownership and internal execution context from authenticated/retained state, not caller-supplied `run_as`. Do not forge Principal/caller IDs or distribute reusable manager tokens.
- Audit `Principal::owns` and the special internal Scheduler path. Preserve legacy scheduled-check receipts without using its cross-owner exception for new automations.
- Reuse current manager action rights and existing applicable grants. No new mandatory automation grant; no service-only permission bypass. Custom role labels cannot inherit generic writer rights.
- Make on-behalf operations readable/explainable/cancellable by the authorized owning manager through retained linkage, not only technical `caller_id`. Scope before pagination.
- Shared semantic slots across manual/automatic callers coalesce equivalent actions and expose incompatible choices. Automation IDs, edits and event timestamps cannot bypass those slots.
- Preserve admitted original/effective parameters. Current enabled/rights/allowed-target checks precede effect start; current preferences resolve only for new admissions.
- Serialize disable/narrowing with effect start. Manual replacement of a provably never-started action retains both receipts in one slot; unknown/sending work needs readback.
- Retain late trusted outcomes separately from new-effect authorization. Revocation does not justify dropping already observed effects or accepting unauthenticated evidence.

Done: one manager can enable only an audit helper in one call, use manual commands alongside it, find its Operations and stop future helper starts without losing active results.

## 4. O2 — Durable intake and shared observation

Reuse Store events/deduplication, change signals, capacity and MCP lag/resync.

```text
src/monitoring/events.rs
src/monitoring/projection.rs
src/monitoring/pump.rs
src/store/monitoring.rs
```

Historical C6 source status: commit `e035c0c3fe855490863be81902c5152b548c42cd`
wires the dispatcher to consume shared bounded intake and journal readback for
the local `controller/task.submission` producer once per Store reconciliation
transaction. C7 commit `2f00c2b3d7862788ca8a6bead4645d3ce64dc0ea` adds
Participant credential issuance at launch admission, partial authenticated
configured/connect readback, and a durable bounded review-disposition consumer.
Its database-only workspace lease includes stale-fence handling. Focused
frozen-source reviews of unchanged privacy, authority, disposition and lifecycle
paths at `588a5ca21fbcbd5434540c011846535af7647f35` passed; `feedback_audit`
passed exact review of all three Windows-delta files in the final commit
(`.local/qualification/r7-build-gate/windows-probe-lifecycle-audit.md`).
Other source adapters, tool/model capability proof and productive dispatch
remain absent; this is not a multi-source intake service or a complete
manager-owned cycle.

- Normalize source/native identity, source cursor/epoch, controller cursor, work/binding scope and coverage. Source failure is unknown, not empty success.
- Commit verified intake before durable ACK. Dispatcher cursor advancement and Operation/reservation or pending-subject state commit together.
- Treat `watch`/MCP notifications as lossy/coalesced hints. After reconnect/startup, read every unprocessed committed fact through a bounded high-water cut. No lost work between committing a cursor and enqueuing an in-memory command.
- Retain waiting subjects and wake conditions; dependency/capacity changes re-evaluate them without producing failed jobs every tick or requiring another source event.
- Share readers and projectors. Snapshot plus after-cursor reads must cover changes racing with subscription. No full history scan or model status query per viewer.
- Separate terminal/control evidence from live presentation. Slow streams cannot starve completion, permission replies or manual actions.
- Bound parser/OS/Git blocking work, queue bytes and serialized output. Keep owner/route fairness and shared-account capacity separate from configured concurrency.
- Future file-managed configuration is not implemented in this slice. Before imports can be enabled, authenticated source registration must bind canonical path, ACL and reparse proof; apply uses revision/owner-epoch CAS, and import failure never falls back to Root.

Done: monitoring works with no enabled automation, dropped notifications recover without lost/duplicate work, and one malformed source or waiting queue cannot block unrelated owners.

## 5. O3 — Rust adapters, streams and direct manager control

Read existing `src/runtime/owner.rs`, `src/runtime/warm_stream.rs`, `src/runtime/opencode_v2/*`, `src/platform/process_group.rs`, Doctor and module mappings.

C6 added manager-admitted asynchronous workspace leasing, exact Task claim,
agent.open, and bounded local TaskSubmission intake. C7 added Participant
credential issuance with private assignment context, authenticated
configured/connect readback, and bounded review-disposition handling. C8 added
manager-authorized WorkDispatch, the shared launch slot, and MCP install/proof
handlers. C7 and repaired C8 full-CI evidence is summarized above.

C9 source `2e609ecf7d826da7019fe5e5f2ed397a992bc45a` includes fresh-owned
OpenCode startup/readback, plugin-directory index and pinned 2.0.7 syntax
preparation, plus bounded service-departure reconciliation. Workspace leases
remain database-backed; departure proof is a separate service-specific fence,
not an OS-level workspace lock. C9 CI 37168223030 passed Windows/Linux
formatting, Clippy, tests, build and native offline fixtures for that exact
source. The native overlay fix had not passed a live retest; earlier runs
f368339d and 114d4ca7 remain retained as unknown, with no replay.

C10 source `cea63dde1d923f821c436c61f2561bcfb6a4bb0d` adds the optional exact
`launch_operation_id` to the actual `task.dispatch` schema and binds launch-owned
dispatch to retained Task/Attempt/binding/lease ancestry. Store retains and
rechecks an immutable prompt packet and current C8 MCP capability proof before
native input. Its provider-auth path accepts one explicit host-side credential
source; `stored_unverified` means metadata was observed, not that the key works
or was consumed by a model. That source passed CI 37170376636 on Windows and
Ubuntu. The route-shape repair is published as `e637d45` and passed independent
source audit (report SHA-256
`A4089D435AC3DE11D1DFE1A12DED3FEC147FAE21520068697B02ABE98E734BB1`),
formatting, production Clippy (14.67 s) and debug build (35.37 s; candidate
SHA-256 `DEDBF403020780A35ED0141A31EF8A43654073B9B2A96CC4BF88AC77B462B528`).
CI 37172541315 failed the Windows `check_probe` lifetime step after Ubuntu and
Windows unit tests passed; the focused fixture uses /D to disable ambient CMD AutoRun; the historical cause remains unproven.

Native run `bb070791-ba7c-4c71-9cc7-660fbb531418` ended
`OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`, after repeated
`OWNED_SERVICE_SCOPE_STALE` and before any owned-service row, MCP proof, Bun
call or model call. Its cause was a manifest-route object/alias-string
comparison before start reservation. The three-file follow-up repair is
published as `e637d45` and passed independent source audit (report SHA-256
`A4089D435AC3DE11D1DFE1A12DED3FEC147FAE21520068697B02ABE98E734BB1`),
formatting, Clippy and debug build. CI 37172541315 failed Windows
`check_probe` lifetime after Ubuntu and Windows unit tests passed; the
the focused fixture uses /D to disable ambient CMD AutoRun; the historical cause remains unproven. That prior host exited by normal EOF; no
replay occurred.

Native run `46cef212-aa65-418a-a911-b54502fe9fd7` ended at `service_start` after
240 `BINDING_NOT_READY` observations, before an owned-service row, MCP proof,
native tool call, or model call. Normal stdin EOF closed the host; the run is
retained without replay. The opening-fence audit found that database module
IDs matched the child but `get_binding` omitted instance and artifact IDs. The
published C11 projection correction advances module IDs past reservation.
Current retained-run evidence and native qualification are maintained in [Implementation Status](../implementation-status.md). C11 now wires
RepairDispatch and acceptance consumers to typed ledgers/cursors, exact
same-slot reuse, GM epoch checks, off-database-bytes verification, structured
requirement reviews, and manager history/visibility. The reviewer-equals-writer
consumer defect is patched (snapshot `4E903...`); both bounded exact-source
audits passed. C11 source is published, while native tool loading, productive dispatch,
provider/model use and the complete manager-owned cycle remain unqualified.
All local model/inference execution, including PR24/Kilo, remains deferred.
- Port owned transport/translation to Rust through documented protocols or maintained libraries; preserve vendor loops externally. Do not silently retain a mandatory internal JS/Python service.
- Qualify installed protocol/capabilities, not exact release equality. Preserve unknown consequential values and safe additive data.
- Maintain native binding/generation/session/turn/family and actual process ownership. Never restart/adopt by heuristic merely to activate a new adapter.
- Wire direct inspect/steer/reply and supported cancellation with exact semantics/readback. Unsupported active-turn input is not silently queued into another task.
- Use a shared selective metrics sampler and concrete producers before generalizing broad traits. PID, wrapper exit and absent event are not terminal evidence.
- Stream native-supplied text/reasoning/tools with bounded retention, cross-chunk redaction and item/final-delta reconciliation. Hidden reasoning remains unavailable.
- Record requested/effective route/model/options and real shared-account usage basis. A failed catalogue/probe is a gap, not an empty supported inventory.
- Capability receipts distinguish configured/listed/harness-acknowledged/successfully-used evidence. Do not invent model-context visibility from a server `tools/list`, or launch a model solely for readiness bookkeeping.

Done: a small team can be observed and controlled manually in Rust, without cron or a delivery workflow and without losing children when the viewer exits.

## 6. O4 — Optional GitHub/Git and distribution

```text
src/github/client.rs
src/github/observer.rs
src/github/webhook.rs
src/github/work_pool.rs
src/git_inspect.rs                 shared with #22
src/automation/distribution.rs
src/store/github.rs
```

- One maintained Rust client/rate boundary, private handles and actual endpoint permissions. Review middleware retry behavior; ambiguous writes cannot be independently replayed beneath Store.
- Verify raw webhook HMAC/context, durably intake, dedupe and reconcile conditional paged reads. Preserve selected source comments and unknown gaps.
- External item identity maps to one Task; source revision is distinct from updated timestamp. Correlated ELIOT projection echoes do not revise requirements or retrigger the same effect.
- Pool import/queue read never starts work. Manual/automatic dispatch share current Task/Attempt/manager/workspace reservations and manager-selected order.
- Readiness precedes page limits; stalled account/capacity does not hide unrelated runnable work. No heartbeat ownership stealing or silent fallback.
- Reuse the launcher and bounded assignment/peer context. Git hints require readback; current ownership is ELIOT state, not blame or branch text.
- Keep all costly/network/Git work outside Store transactions and per-target final serialization, not one global merge lock.

Done: manager imports/reads a pool, selects work manually or enables its distributor; local work remains possible when GitHub is unused/unavailable.

## 7. O5 — Hooks

- Inventory event/schema/phase/veto/lifetime/install readback for each actual runtime/plugin path.
- Rust `swarm hook emit` or adapter ingress authenticates source and bounds payload.
- Setup preserves existing hooks; service/global-setting changes are separately explicit.
- Observation alone never starts an auditor/script/model. Enabled entries map facts to actions through O1/O2.
- Async after-hooks cannot veto past work. Long actions stay off callback paths. Optional telemetry backlog cannot block compaction.
- Missing Rust-reachable integration is a named capability gap. Mandatory action safety is independent from optional automation.

Done: an observed commit updates state, and an explicitly enabled manager helper can notify an auditor without a new control mode or inline model call.

## 8. O6 — Optional script bundles

```text
src/scripts/manifest.rs
src/scripts/registry.rs
src/scripts/runner.rs
src/scripts/protocol.rs
src/store/scripts.rs
```

- Reuse artifact/process owners; add complete bounded bundle publication rather than untracked filesystem writes. Validate support files, paths and input/result schemas.
- Content activation is not execution. Authorized direct runs and manager-enabled triggers use one runner.
- Resolve prepared installed interpreters and capture effective bundle/environment for each admission; no callback installer, fixed release or mid-run mutable dependency substitution.
- JSON stdin/separate argv, bounded output, actual descendant ownership and evidence validation. Exit zero alone is not complete success.
- Scope invocation API rights and on-behalf/cause identity. No manager token or permission to enable arbitrary workflows.
- Distinguish trusted-local OS rights from actual isolation. Job Objects are lifecycle tools, not sandboxes. No silent trust downgrade.
- Disable prevents future independent actions, not fictional cancellation of already running OS code.

Done: user/authorized agent scripts are optional; normal native delivery never depends on their interpreters.

## 9. O7 — Local reviewed delivery and selected automation

```text
src/automation/delivery.rs
src/review.rs
src/store/review.rs
src/github/projection.rs           only for optional remote projection
src/github/pulls.rs                only for optional PR effects
```

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

Extend existing scheduler/Store; do not import a parallel durable engine.

- Editors update the same per-entry owner/enabled/settings. No double switches.
- Maintained Rust cron evaluator, explicit timezone/DST/overlap/misfire semantics and next occurrences. Ordinary latest-only catch-up; backfill only when selected and safe.
- Separate calendar identity, considered occurrence and invocation. Preference/label/enable edits do not recreate consumed slots; old interval identities and receipts remain recognized.
- Cursor/occurrence disposition and action/pending record commit together. Dependence on volatile notification delivery is forbidden.
- Direct run-now is one manual action with recurrence still disabled. Restart reconciles saved enabled intent; intentional re-enable uses its chosen inclusion policy.
- Typed rule validation plus domain progress/causality checks; a failed notification does not spawn an explanation agent. Respect real capability and account backoff rather than retrying every tick.

Done: schedules/rules can be saved, enabled once by their manager, stopped and recovered without lost occurrences, burst replay or duplicate effects.

## 11. O9 — Goal and reminders

- Reference existing assigned Tasks/evidence. No new Task graph or hidden replacement model loop.
- Tracking starts no work; selected progression uses the manager entry and ordinary action rights.
- One actual continuation owner. Native Goal/live children require supported reconciliation before a competing server continuation.
- Achievement uses declared evidence/evaluator, not token/commit count or prose.
- Reuse shared watches/subject indexes/timer; direct notices are not assignments, approvals or new model turns.

Done: tracking, one-shot reminders and manager-enabled progression coexist without competing owners or compulsory automation.

## 12. O10 — Shared MCP/CLI and cross-PR consistency

Keep #22's small eager cores and deferred groups. Wire exact schemas to implemented handlers and readers; no generic dispatcher, redundant public aliases or mode/control API.

Reconcile these interfaces with #22 in the same implementation increment:

| Interface | Combined contract |
|---|---|
| Assigned reviewer | Canonical `review.submit`, exact-assignment `review.get` and linked `operation.get` may finish/read after release while authenticated and unrevoked. `task.request_changes` stays manager disposition; explicit legacy `Reviewer` compatibility remains separate. |
| Participant registration | Sponsored review scope may submit its exact slot; ordinary workers do not acquire review/Task rights. |
| Operations/status | Manager sees authorized on-behalf actions despite service requester identity; result ingestion and new-effect permission differ. |
| Launcher | One candidate/worktree owner, separate native continuation identity, real reporting capability evidence. |
| Watches | One service and one enabled/owner record for recurring actions; one-shot notices create no model work. |
| Catalog | Profile permission differs from schema loading; list changes do not prove model consumption. |

Register authorization, input validation, dispatch, durable links, result projection, CLI/MCP schema, profile tests and current documentation together. A missing optional source produces a scoped gap, not a disabled manual toolbox. `automation.explain` uses normal reasons such as waiting for capacity/manager or an existing result, not a fabricated failure.

## 13. O11 — Qualification

Future tests, not results of this documentation pass:

| Case | Required outcome |
|---|---|
| Default and one-call enable | New/imported entries are off; manager enables only audit in one request without Root/mode/grant ceremony. |
| Manual coexistence | Full manual path works; selected automation neither hides manual tools nor enables omitted steps. |
| Current rights | Service uses manager scope, not Scheduler's legacy exception; no fabricated Principal or privileged fallback. |
| Operation visibility | Authorized manager can read/cancel/explain on-behalf work without another manager's service jobs leaking. |
| Commit/signalling crash | Crash after commit before notification loses no audit; replay causes no duplicate. |
| Capacity wait | Considered subject resumes on capacity/prerequisite change without a new submission or failed-job storm. |
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
| GitHub | Projection echoes do not revise Tasks; uncertain writes are not duplicated by transport middleware. |
| Cron/restart | Old slots recognized; calendar edit, toggles, DST and restart do not duplicate consumed work. |
| Scale | Slow streams and busy owners cannot starve completions/control; no per-participant polling or full-history scans. |

Run synthetic registered populations separately from paid active models. Measure bounded Store latency, source gaps, routing fairness, RSS/handles, token/tool context, repeated-work rate and manager attention. Stage live runs on small teams before larger fleets; registration count is not active-turn capacity.

No paid model runs, live repository writes, service installation or owner-machine cleanup in CI.

## 14. Migration and completion

Use forward migrations for required entry, cursor, pending-subject, review/occurrence/slot and on-behalf linkage indexes. Operations remain execution authority; current indexes do not become another job state machine. Preserve historical policy, legacy scheduler receipts and unknown effects.

The proposed old PR #23 global modes/control API were never deployed: do not implement aliases for them. Likewise do not silently loosen current GM-only code because a future document names a manager tool. Adopt the explicit new accepted policy with the common handler and retain earlier evidence.

Setup collects trusted runtime/source/credential/trust values locally and creates no live automation as a demonstration. Ordinary crash recovery preserves owned saved choices; untrusted/unattributable imports do not constitute enabling consent. No fixed software pins, silent installation or heuristic live-session adoption.

Completion means a manager can operate manually, enable only chosen helpers, understand what ran on their behalf and complete a real integrated work/review/correction/delivery path without duplicated authority, lost events or bureaucratic intermediate gates.
