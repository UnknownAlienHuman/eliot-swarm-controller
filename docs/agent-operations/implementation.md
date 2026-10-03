# Implementation Plan — Manual-First Rust Agent Operations

Revision 3 · 2026-10-03 · source baseline `504199d14135c030ad3951a3c5023a098a3d03f0`.

This is a delivery plan, not completed code. Read [README](README.md), then the relevant [Configuration](configuration.md), [Architecture](architecture.md), [Delivery](delivery.md) and [Donor map](donor-map.md). Internal systems are Rust; optional external scripts are Python/PowerShell. No software-version pin is prescribed.

## 1. Shared ownership and execution policy

PR #22 supplies proposed Participant identities, peer communication/cards/cells, watches, launcher and deferred catalogue concepts. This program supplies observation, manual management and optional server automation. Check what actually landed before creating files.

Implement one authorization evaluator, Task/Attempt lifecycle, watch service, dashboard, launcher and effect path. Presets, Goals and manual commands use existing Operations, not parallel job/task stores. Control revisions and delivery phases are metadata/projections over that authority.

One manager owns one worktree/candidate; writers receive non-overlapping assignments, not independent product submissions. Writers do not run Cargo. The manager reviews/integrates the complete Issue candidate and runs scoped formatting/minimal warnings-denied Clippy once. Broad tests/load/live qualification follow the complete path or an explicitly advanced acceptance phase.

Every slice includes producer, consumer, registration and result reader. Manual operation is delivered before or with its optional automatic caller. Do not finish a workflow engine while basic manager commands still require that engine to be enabled.

## 2. Dependency sequence

```text
O1 shared Rust authority + configuration + default-manual control + action admission
  -> O2 event intake / source health / projections independent of control mode
       -> O3 Rust native adapters / monitoring / streams / manual management
       -> O4 GitHub/Git pool mapping / manual selection / opt-in distribution
       -> O5 Rust hook observation and explicit setup
  -> O6 optional external-script registry / one-shot runner

O1 + O2 + O4 + actual runtime/launcher
  -> O7 manual submit/review/return/publication, then selected automatic transitions

O1 + O2
  -> O8 inactive-by-default cron / rules / manual run-once / explicit activation
       -> O9 observational Goal / selected continuation / shared reminders

O10 complete deferred MCP/CLI/control visibility
  -> O11 integrated qualification and installation handoff
```

O3/O4/O5 may proceed in parallel only with agreed envelopes and non-overlapping file ownership. Ordinary delivery does not depend on O6 or an interpreter. O10 completes surface parity; it is not the first call site for earlier production code.

## 3. O1 — Authority, configuration and manual/automatic control

### Existing anchors

`src/model.rs`, `src/policy.rs`, `src/config.rs`, `src/store/mod.rs`, `src/store/operations.rs`, `src/store/submissions.rs`, `src/store/acceptance.rs`, `src/store/forge.rs`, `src/mcp/profiles.rs`, Owner Decisions and the actual #22 implementation.

### Proposed units

```text
src/automation/actions.rs
src/automation/config.rs
src/automation/control.rs
src/authorization.rs              reuse an existing shared evaluator if present
src/store/automation.rs
src/store/configuration.rs
src/runtime/profiles.rs
```

### Work

- Audit all role/owner/GM checks. New roles must not inherit writer rights through a negative not-observer check.
- Separate config-edit permission, control-management permission, direct action permission and standing automatic execution grant. Only management authority over the exact scope enables/widens unattended execution.
- Preserve old owner-policy snapshots/editions; do not replace a current digest and make all prior Attempts unrecognized. Update accepted policy and implementation together when delegated behavior lands.
- Preserve existing GM/operator paths. Add narrow scoped manager/workflow review-disposition and optional acceptance/publication paths to the same guarded handlers, not GM credentials for auditors.
- Implement `automation.config.get/preview/apply`, `automation.explain`, runtime catalogue/profiles and `automation.control.get/preview/apply` with exact canonical names.
- Saving definitions/profiles is not activation. Defaults are manual mode, empty automatic stages/definition selections and `resume_after_restart=false`.
- Add explicit manual/assisted/delegated mode, pause, management scope, selected stages/definitions, manual item holds, activation cut and control revision. No agent-count heuristic changes them.
- Enable/resume/widen requires a manager preview and explicit control document. A restrictive pause/manual change remains local and must not require healthy external systems or a valid proposed config.
- Derive manual versus automatic origin from authenticated admission/cause. Script/event input cannot forge manual authority.
- Use one internal typed action handler; no recursive public IPC while holding Store transaction and no call-any-method action.
- Manual and automatic callers reserve the same domain action slot. Do not include origin, request transport or incidental config revision to evade uniqueness.
- Resolve active settings for each new invocation, retain that execution snapshot and return the old receipt on identical retry.

### Control/effect start contract

Store serializes a restrictive control change with each autonomous worker's effect-start transition. If the control change wins, retain the queued action as held before I/O. If start wins, report it as in-flight; do not pretend pause can recall bytes already or imminently sent under that start.

Every separate downstream effect rechecks current stage/grant/control. An already-started action may finish its declared bounded operation and record/read back results. Completion does not authorize the next disabled workflow stage. Holds are projection reasons, not fabricated success or another execution state machine.

### Done when

A manager can perform a direct valid action with all automation off, save preferences without starting work, activate a chosen stage only with scope authority, and return to manual without killing work or losing results. An executor/config editor cannot reactivate it. Historical policy/evidence remains readable.

## 4. O2 — Observation and current projections

### Anchors and units

Reuse Observation insertion/deduplication, Store change signals, `src/store/projection.rs`, capacity and `src/mcp/subscriptions.rs`.

```text
src/monitoring/events.rs
src/monitoring/projection.rs
src/monitoring/pump.rs
src/store/monitoring.rs
```

### Work

- Define verified source, native cursor/event ID, source epoch, controller sequence, work/binding scope, evidence class and coverage/gaps.
- Separate durable terminal/control evidence from live text. Native/tool events cannot assert acceptance or forge success.
- Maintain shared readers/projectors and scoped snapshot/cursor/deltas in every control mode. No global dump followed by client filtering.
- Preserve lag/resync while removing redundant per-viewer scans. Reserve capacity for manual control and native replies independently from optional telemetry/Git/scripts.
- Keep failed enumeration/API queries unknown, not healthy empty sets. Bound parsing, bytes and blocking workers.
- Watch trusted definition files through validation. Invalid input retains last valid definitions; no file/template/plugin reload changes effective control or removes a manual hold.
- Include effective mode, selected stages, held/unstarted actions, in-flight/unknown effects and external continuation coverage in dashboard/explain.

### Done when

Manual mode retains passive monitoring and late result ingestion. A paused scope is visible as an intentional state, not a broken worker. A slow viewer, malformed source or file-save race cannot block manager control or native permission replies.

## 5. O3 — Rust adapters, streams and direct manager control

### Anchors

`src/runtime/owner.rs`, `src/runtime/warm_stream.rs`, `src/runtime/opencode_v2/*`, `src/platform/process_group.rs`, Doctor and existing module behavior as migration evidence.

### Work

- Implement owned transport/control/translation in Rust using documented protocols or suitable whole Rust libraries. Vendor executable internals remain external.
- Use protocol/capability compatibility rather than exact version equality. Report unknown consequential behavior; tolerate safe additive data. No copying commercial SDK internals.
- Preserve binding, native input/turn, child-family, delivery uncertainty and process ownership across migration. Do not restart live sessions to activate the new path.
- Make direct inspect/steer/reply and supported cancellation controls usable without automation. Unsupported operations report limits rather than silently queueing a different kind of input.
- Use one selective maintained Rust metrics sampler and at least two actual native event producers before broad stream abstraction. No old library requirement or hidden toolchain downgrade.
- Add scoped text/tool/native-provided reasoning, redaction, retained part identity and bounded history. Hidden/encrypted reasoning is unavailable.
- Record requested/effective model/options, shared account quota, catalogue freshness and real MCP capability receipt.
- Track external/native continuation separately from ELIOT control. Manual mode is not proof that a native Goal has stopped; expose exact supported clear/cancel/readback actions.

### Done when

A manager observes and controls a small manually launched team through Rust without a delivery loop, cron, Goal or Python/Node bridge. Viewer exit does not kill work. Unknown prior family disposition prevents only conflicting replacement.

## 6. O4 — GitHub/Git and optional distribution

### Units

```text
src/github/client.rs
src/github/observer.rs
src/github/webhook.rs
src/github/work_pool.rs
src/git_inspect.rs                share with #22
src/automation/distribution.rs
src/store/github.rs
```

### Work

- Use one maintained Rust API/rate boundary and trusted local repository/credential handles. Inventory actual endpoint permissions without exposing private values.
- Verify raw webhook signatures/context, durably ingest, deduplicate, conditionally reconcile missing/out-of-order data and bound account-level rate usage.
- Preserve one external-item Task origin; selected source changes create revisions. No automatic source-comment-to-policy conversion.
- Implement pool preview/apply and explainable queue reads. Import, readiness and increased capacity never auto-start in manual mode.
- Direct manager selection and enabled `work_dispatch` use the same atomic Task/Attempt/manager/workspace reservation. Resolve ambiguous active control ownership instead of choosing arbitrarily.
- Prepare bounded context via the launcher; native effects start only after O1 revalidation outside Store transactions.
- Use one shared Git watcher/inspection path. Verify actual OID/worktree state after hints; no per-token status scan or author-name ownership guess.

### Done when

A manager can import/read 1,000 Issues while zero work is auto-assigned. Enabling selected distribution consumes eligible items without duplicates, and disabling it leaves explicit single-item dispatch usable. Manual and automatic requests racing for one item share one reservation.

## 7. O5 — Hooks without default execution

- Report native event/schema/phase/veto/lifetime/install status accurately.
- Provide Rust `swarm hook emit`/adapter ingress with authenticated source scope and bounded data.
- Use a real Rust-reachable native interface. Missing non-Rust-only plugin capability remains a gap, not a hidden TS/Python service.
- Setup preview/apply preserves user/managed hooks; restart and global configuration changes are separate explicit operations.
- Observational callbacks record facts in manual mode. No auditor/script/model is launched merely because a commit/tool event occurs.
- New associated rules default inactive and need O1 manager control. Mandatory pre-effect authorization remains enforced for manual actions independently of automation.
- Keep long work off callback paths and preserve controlled forge hook suppression. Optional telemetry backlog cannot block productive work or compaction.

Done when an installed commit hook improves visibility with every automatic stage off, and only an explicitly enabled rule can admit its named action.

## 8. O6 — Optional scripts and one-shot execution

### Units

```text
src/scripts/manifest.rs
src/scripts/registry.rs
src/scripts/runner.rs
src/scripts/protocol.rs
src/store/scripts.rs
```

- Reuse artifacts and process ownership where semantics match CheckRunner/forge.
- Register bounded bundles/support files and schemas; reject traversal/escaping links/unrestricted upload.
- Content activation does not run code. Direct authorized `script.run` works with automation off; an unattended run needs a selected definition/stage/grant.
- Resolve active content and installed prepared interpreter environment for new runs; retain original bytes/receipt for an admitted run. No software pin or package installation in a hook/timer.
- Pass JSON stdin and separate argv; bound output and own descendants. Validate typed output/evidence without mistaking exit zero for full completion.
- Preserve actual trusted-local OS rights versus enforceable isolation. No silent downgrade or default ExecutionPolicy Bypass.
- Retain automatic provenance in child effects. A script cannot enable rules, clear manual holds or call protected writes by declaring itself manual.
- Pause does not kill a started script; report it until completion or separately authorized cancellation/readback. Unknown surviving writers hold the relevant mutation scope.

Done when an authorized agent authors/runs an optional script manually, a manager may separately schedule it, and ordinary delivery works without either interpreter installed.

## 9. O7 — Manual review/delivery first, opt-in preset second

### Anchors and units

Reuse submissions, acceptance, forge, source/check capture and Task dispatch.

```text
src/automation/delivery.rs
src/review.rs
src/store/review.rs
src/github/projection.rs
src/github/pulls.rs
```

### Work

- Implement direct `review.assign/get/submit` and manual feedback/continuation/publication through normal guards. No enabled automation object is a prerequisite.
- Applied submission makes a candidate reviewable; only the enabled review stage dispatches automatically. Queued/failed submission does not.
- Reserve exact review slots, validate coverage/evidence and separate pass/actionable changes/inconclusive. Preserve independent source/permission lineage and immutable candidate evidence.
- Aggregate local audited state in every mode. A recorded review result does not itself apply Task feedback, deliver native input or write GitHub state.
- Use the O1 narrow feedback branch only on explicit manager request or enabled `review_disposition`. Repair dispatch is separately selected/requested.
- Re-review after repair requires a current manager choice or enabled review stage. Do not carry an old activation through a new manual boundary.
- Preserve original-owner preference/fallback limits; reconcile native/lease disposition. Corrected source can be reviewed with unchanged checklist text.
- Add `reviewed_delivery` as a saved inactive preset; its automatic call sites delegate to the already usable manual handlers. Implement each selected stage independently, including final manager gate versus automatic publication.
- Manual and automatic commands share semantic review/publication slots. A manual takeover does not introduce a second queued audit/push.
- Reuse accepted non-force publication; add optional exact review-branch upload without protected-output/acceptance rights. Local-first review has no push/audit cycle.
- Make labels/summary/Checks a separate requested/enabled write path. Inconclusive is not neutral/skipped success. Retain remote IDs/correlation and append-batch identity/readback.
- Distinguish expected PR head, integration base, queue acknowledgement and actual merge. Use actual repository/API capabilities; no native merge-queue prerequisite or protection bypass.
- Observe pending external merge/workflow operations after local pause. Offer supported cancellation separately and do not report remote quiescence without evidence.
- Separate readback/result recording from optional next effects/bookkeeping. A label error cannot repeat push or return valid code for reimplementation.

### Done when

The complete launch -> submission -> audit -> return -> corrected submission -> acceptance/publication path works manually with all automatic stages off. The manager can then enable only distribution/review, or fuller eligible delivery, with exactly the same correctness checks and no internal scripting dependency.

## 10. O8 — Cron and rules

Extend existing scheduler/config/Store. No second daemon or unrestricted workflow language.

- New definitions are inactive. Saving/enabling a definition does not bypass the scope's current manager control and stage selection.
- Use a maintained complete Rust evaluator with explicit timezone/DST/misfire/overlap preview. No exact-release prescription.
- Preserve old schedule receipts and explicitly attributable legacy intent; unknown legacy activation imports inactive with an adoption preview.
- Separate definition, occurrence, action slot and invocation. Content/control edits do not replay consumed slots.
- Direct `schedule.run_now` is a one-shot manual command even while its recurrence is paused; no implicit unpause or exemption from normal authority/resource guards.
- Recheck control at effect-start, not just admission. Pausing holds old unstarted automatic invocations while started work/readback remains visible.
- Resume uses explicit future-only/current-eligible semantics, latest-only normal catch-up and deliberate bounded backfill; do not replay every event accumulated during a pause.
- Hot file reload cannot undo a stop or widen selected definitions/effects. Bound causal ancestry and automatic error handlers.

Done when a manager can prepare timers/rules without any execution, run one selected occurrence manually, explicitly enable recurrence and stop future starts without destroying the running invocation.

## 11. O9 — Goal and requested reminders

Use shared Goal/watch modules and #22's timer/subject indexes.

- Goal creation defaults draft/observational and references existing Tasks/evidence. Textual objectives do not start models or enlarge work scope.
- Automatic progression requires selected Goal definition and action stages/grants/capacity.
- Preserve one continuation owner. Pausing ELIOT does not falsely claim native Goal/children stopped; no competing continuation until exact reconciliation.
- Manual progress evaluation and one-step manager commands work without a server continuation loop.
- Requested one-shot watches and direct mail remain usable in manual mode. Unsolicited repeated nudges or auto-answers need explicit manager-selected automation.
- Notification delivery never replaces the current Task or proves the model consumed it. No per-agent poller, implicit native restart or timed grant escalation.

Done when monitoring a Goal is distinct from delegating it and a small manual team can request reminders without enabling unattended work.

## 12. O10 — MCP/CLI and control UX

- Expose exact typed methods through the shared catalogue; no duplicate public aliases or generic dispatcher.
- Keep manager core small and manual launch/inspect/steer/review/forge tools usable when automation is off.
- Add deferred control/config/runtime-profile groups with proper current-role and scope checks in discovery and pre-dispatch.
- Show actual mode/stages, control owner/revision, pending manual actions, holds, draining operations, external continuations and coverage.
- `automation.explain` must distinguish awaiting-manager by choice from missing authority/evidence or actual failure. Do not spam escalation for intentional manual waiting.
- Restrictive local control works even if optional sources are down or a config proposal is invalid.
- Assisted suggestions are deterministic/paged; acting on one invokes its ordinary specific tool. Listing/search/preview never dispatches.
- A control/profile change does not inflate the eager catalogue or make cached schemas authorized. Tool loading cannot reactivate automation.
- Native notification gaps remain visible. Observation disconnect does not stop intentional existing work.

No deployment domain or live service setup is part of ordinary wiring.

## 13. O11 — Qualification matrix

These are required future tests, not results of this documentation review.

| Area | Required case |
|---|---|
| Default state | Fresh project, preset selection, queue import, new role/grant, plugin install and large fleet all leave unattended starts at zero. |
| Manual completeness | Manager executes one full code/review/repair/publication cycle with every automation stage off and no delivery-preset requirement. |
| Hybrid control | Only selected dispatch/review runs automatically; manual repair/acceptance/publication await the manager without false failure. |
| Configuration | Saved definitions/model profiles affect future requested work but cannot enable, unpause or widen control. |
| Manager authority | Executor, script author and auditor cannot activate automation; a scoped manager cannot modify another pool. |
| Race: pause wins | An old queued automatic Operation is held before I/O; no stale admission ticket starts it. |
| Race: start wins | Pause returns the started/unknown effect; no false cancellation or fresh conflicting writer. |
| Race: manual vs auto | Same launch/review/publication slot yields one effect or retained result, not two different origin-keyed jobs. |
| Resume | Manually handled/current-stale work is not repeated; default resume does not drain historical events. |
| Restart | New/unknown state is manual; default restart holds automation; explicit restart continuity revalidates its existing consent/grant. |
| External continuation | Native Goal/queued remote merge stays visible after local pause until completion or supported cancellation readback. |
| Observation | Streams/dashboard/results still work while manual/paused; source gap is not an empty successful inventory. |
| Hooks | Observational commit hook updates state only; async callback is never a pre-effect veto. |
| Scripts | Manual one-shot works; automatic children retain provenance; pause cannot pretend a live trusted-local process is sandboxed/stopped. |
| Review | Late A result cannot alter B; corrected source with unchanged checklist can progress; inconclusive never becomes pass. |
| GitHub | HMAC/dedupe/paging and exact readback; projection error does not retry publication; local switch does not change repo settings. |
| Permissions | Manual mode bypasses no verification/GM/host/resource gate; deferred tool visibility never grants authority. |

Stage synthetic load separately from active paid models: thousands of registrations/definitions, bounded viewers and event bursts, zero automatic model calls while manual. Measure Store latency, control-start ordering, RSS/handles, source gaps and resource fairness. Then qualify intentionally activated small fleets before increasing live concurrency. Registration count is not active-turn capacity.

No paid model run, owner-machine cleanup, hook installation or service mutation in CI.

## 14. Persistence, migration and setup

Use forward migrations for config/control records, source routing, review slots, due occurrences and effect-start linkage. Existing Operations remain execution authority. Index current state and next-due/subject routing; do not rebuild full history per observer or store all control scopes in one hot value.

Persist restrictive control separately from editable definition files so reload, rollback or configuration restore cannot accidentally unpause. Recover historical policy/evidence and old schedule receipts. Migrate proven intentional old schedules with their actual scope; absent activation evidence requires manager adoption, not inference from `enabled=true` alone.

Default `resume_after_restart=false`; explicitly enabled continuity may survive host restart after source/grant/owner reconciliation. Preserve unknown effects and retained manual decisions. Old binaries must reject unsupported schema rather than silently ignoring these controls.

Setup collects local trusted executors, source roots, credentials, quotas, retention and script trust as private values. It starts in manual mode and creates no live cron/Goal/rule merely to demonstrate features. Present optional presets as inactive examples. Existing live sessions are not restarted or adopted by heuristic.

Separate live setup qualification verifies native capability receipts, hidden Windows process behavior, webhook authentication, manual commands, optional activation, pause/drain/restart and privacy. Do not claim it was done from static source review.

## 15. Completion definition

The delivered product must let a manager observe and control a small team fully manually, then delegate selected transitions without moving to another architecture. Turning automation off retains observations, explicit tools, results and safety; turning it on requires genuine manager intent and can never happen from fleet size or a file reload.

Rust internals, direct peer cooperation, candidate-bound audit, exact effect readback, scoped authority, no software pins and deferred MCP usability remain required in both modes.
