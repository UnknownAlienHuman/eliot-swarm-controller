# Implementation Plan — Rust Operations on Behalf of the Manager

Revision 4 · 2026-10-03 · source baseline `504199d14135c030ad3951a3c5023a098a3d03f0`.

Read [README](README.md) and the relevant [Configuration](configuration.md), [Architecture](architecture.md), [Delivery](delivery.md) and [Donor map](donor-map.md). This is proposed work, not implementation evidence.

## 1. Execution policy and shared ownership

PR #22 supplies Participant identities, peer cards/cells, shared watches, launcher and deferred MCP concepts. Reuse what actually exists when implementation starts. There is one authorization evaluator, Store/Task/Attempt/Operation lifecycle, dashboard, watch service and action path.

One manager owns one worktree and one in-flight product candidate. Writers receive non-overlapping assignments, not independent product submissions. Writers do not run Cargo. The manager integrates/reviews the completed Issue candidate, then runs scoped formatting and minimal warnings-denied Clippy. Broad tests/load/live qualification follow the established completed-product/acceptance phase, not every writer edit.

Each implementation slice includes its producer, consumer, registration and result reader. Direct manager commands must work before or with their optional automatic callers. Do not leave the useful manual path disconnected until O10.

The product has no global manual/assisted/delegated enum, no separate activation ledger and no mandatory `automation.control.*` API. Manual work is the baseline. Each automation has an owning manager and an enabled flag; the manager can save and enable it in one authorized configuration call.

## 2. Dependency sequence

```text
O1 manager-owned definitions + shared on-behalf authorization/admission
  -> O2 observation / source health / projections
       -> O3 Rust adapters / native streams / direct manager controls
       -> O4 GitHub/Git mapping / manual selection / optional distribution
       -> O5 Rust hooks / event intake
  -> O6 optional external-script registry / Rust runner

O1 + O2 + O4 + actual runtime/launcher
  -> O7 manual reviewed delivery + manager-enabled handoffs

O1 + O2
  -> O8 cron / typed rules
       -> O9 server Goal / shared reminders

O10 complete deferred MCP/CLI/configuration parity
  -> O11 integrated qualification and installation handoff
```

O3/O4/O5 may proceed in parallel only after agreeing shared envelopes and file ownership. O7 ordinary delivery does not depend on Python, PowerShell or O6. No worker independently changes the shared action/authorization contract.

## 3. O1 — Manager-owned definitions and common admission

Existing anchors: `src/model.rs`, `src/policy.rs`, `src/config.rs`, `src/store/mod.rs`, `src/store/operations.rs`, submissions/acceptance/forge, MCP profiles and actual #22 code.

Proposed units, reused where already present:

```text
src/automation/actions.rs
src/automation/config.rs
src/authorization.rs
src/store/automation.rs
src/store/configuration.rs
src/runtime/profiles.rs
```

Work:

- Audit role/owner/GM checks; a new role must not inherit rights through a negative not-observer test.
- Add per-definition owner, scope, trigger/preset, enabled, settings and revision. Default new entries off; explicit manager `enabled=true` may save and activate in one call. No second stage/control switch.
- Implement `automation.config.get/preview/apply`, `automation.explain` and runtime profile/catalogue paths. Preview is optional effect-free assistance; apply always validates current rights and expected revisions.
- Derive ownership from authenticated manager authority, not a public `run_as` or supplied role. A legitimately appointed AI manager has management rights for its assigned scope regardless of launch origin.
- Create trusted internal execution context carrying requester, effective manager, automation and cause. Reuse manager action checks without forging Principal/caller IDs or copying manager tokens to scripts.
- Automation is no stronger than its manager and its configured actions/targets. Existing grants are evaluated when applicable; do not demand a new automation-specific grant for an already permitted action.
- Keep independent reviewer attribution. Handoff sponsor is not author of `review.submit` or verifier evidence.
- Update accepted Owner Decisions and shared manager feedback/publication checks where new scoped rights are intended; preserve GM-only/epoch requirements otherwise. No service-only bypass.
- Preserve old policy editions and immutable Attempt evidence when adding support. Do not replace a document digest and make old Attempts unrecognized.
- Shared manual/automatic semantic slots prevent duplicate launch/review/publication across callers. Incidental config revision and origin are not separate slots.
- New runs resolve active definitions; identical request retry returns the prior receipt. Store per-run snapshots are evidence, not software pins.

Disable and effect start serialize through short Store transactions. Disable-first holds unstarted automatic work; start-first reports in-flight/unknown until observed. Manual takeover of a provably never-started held action atomically replaces that attempt in the same slot, keeping both receipts. Never replay a sending/unknown effect under a new ID.

Done when a scoped manager can configure/enable an audit handoff in one request and execute manual commands alongside it, with no global mode or Root approval for existing rights. Role escalation, duplicate effects and forged ownership remain rejected.

## 4. O2 — Observation and projections

Reuse Observation dedupe/change signals, capacity, Store projections and MCP lag/resync.

```text
src/monitoring/events.rs
src/monitoring/projection.rs
src/monitoring/pump.rs
src/store/monitoring.rs
```

- Normalize verified source/native IDs, source epoch/cursor, controller sequence, binding/work scope and coverage.
- Separate durable terminal/control evidence from live text. Native events cannot assert acceptance or forge success.
- Shared readers maintain scoped snapshots/deltas; a viewer does not poll every model or scan all history.
- Source/API failure is unknown, not an empty healthy set. Bound parsing, bytes and blocking work.
- Reserve manual control/native reply/completion capacity independently from optional telemetry, Git scans and scripts.
- Show owner, enabled automations, next/last invocation, cost basis, pending decisions, in-flight effects and exact gaps without a global mode.
- Trusted configuration-file imports use O1 revision checks; invalid saves keep last good state and stale snapshots cannot reverse a newer disable.

Done when zero enabled automations still gives complete monitoring and result collection; malformed source, slow readers and file-save races do not stall ordinary work.

## 5. O3 — Rust adapters and direct management

Anchors: `src/runtime/owner.rs`, `src/runtime/warm_stream.rs`, `src/runtime/opencode_v2/*`, `src/platform/process_group.rs`, Doctor and existing module mappings as migration evidence.

- Implement owned transport/control/translation in Rust through documented protocols or suitable whole libraries. Keep vendor model loops external.
- Use actual capabilities, not exact CLI release equality. Safe additive native data is tolerated; unsupported consequential operations are reported.
- Preserve binding/generation/native input/turn, family ownership and unknown outcomes during migration. Do not restart existing sessions to activate a new adapter.
- Wire direct inspect/steer/reply and supported cancellation. A missing active-turn capability cannot silently become an unrelated next-turn prompt.
- Add shared selective process metrics and at least two actual supported stream producers before generalizing broad traits.
- Stream provider-exposed text/reasoning/tools with part identity, redaction and bounded retention; hidden reasoning remains unavailable.
- Record requested/effective models/options, shared account quotas, real tool capability receipt and routing gaps.
- Treat external/native continuation separately from ELIOT automation settings. Owner disconnect or wrapper exit is not proof of child termination.

Done when a manager observes/controls a small team through Rust with no delivery-loop/cron dependency. Viewer exit leaves native work intact.

## 6. O4 — GitHub/Git and optional distribution

```text
src/github/client.rs
src/github/observer.rs
src/github/webhook.rs
src/github/work_pool.rs
src/git_inspect.rs                  share with #22
src/automation/distribution.rs
src/store/github.rs
```

- Use one maintained Rust client/rate boundary and local credential/repository handles; inspect actual endpoint permissions.
- Verify raw webhook authentication/context, durably adopt and deduplicate, then reconcile missed/ordered data with bounded conditional reads.
- Stable external-item origin maps to one Task. Selected source edits make revisions; prose cannot rewrite workflow policy.
- Pool preview/apply selects work but never launches by itself. Direct commands and enabled distribution use the same atomic current Task/Attempt/manager/workspace reservation.
- Evaluate readiness before page limits. Preserve manager order unless an explicitly chosen ranking applies; no heartbeat-based stealing or hidden model rerouting.
- Use the shared launcher for bounded assignment/peer context. No native effect inside Store transactions.
- Confirm Git object/worktree facts after change hints. No per-agent full status loop, `git blame` ownership inference or script-based GitHub controller.

Done when an imported queue can be handled manually or by the enabling manager's distribution automation, without duplicate ownership or Root relaying every item.

## 7. O5 — Rust hook integration

- Report native event/schema/phase/veto/lifetime/install readback accurately.
- Provide Rust `swarm hook emit` and direct typed ingress with authenticated scope and bounded input.
- Missing Rust-reachable functionality remains a capability gap, not an undisclosed JS/Python internal daemon.
- Setup preserves existing hooks. Native service restart and global installation remain separate explicit actions.
- Observation records verified commit/tool events even when no automation uses them. Only a configured enabled entry maps them to actions.
- Keep heavy review/script/publication outside callbacks. Async after-events never provide pre-effect veto.
- Mandatory safety policy is independent of optional automation. Keep controlled Git hook suppression in forge.

Done when hook observation is useful alone and an enabled manager entry can add a notification/audit without another mode switch or callback blocking productive work.

## 8. O6 — Optional script bundles and runner

```text
src/scripts/manifest.rs
src/scripts/registry.rs
src/scripts/runner.rs
src/scripts/protocol.rs
src/store/scripts.rs
```

- Reuse artifacts and process ownership where semantics match CheckRunner/forge.
- Register bounded bundles/support files and typed inputs/results. Reject traversal, escaping links and arbitrary upload paths.
- Content activation selects future runnable content. Manual run works independently; cron/hooks select it through the owner's enabled automation.
- Resolve installed prepared Python/PowerShell environments, not fixed releases or callback-time package installs.
- Pass JSON stdin/separate argv, bound stdout/stderr and own descendants. Valid JSON/exit zero alone is not full success evidence.
- Record run owner and technical execution identity. Invocation API credentials are scoped; no manager/GM bearer is inherited.
- Preserve actual trusted-local rights versus enforced isolation. Job Objects are not security sandboxes; missing isolation never silently downgrades.
- Disable holds future invocations/independent follow-ups; started OS work remains visible until settled or separately cancelled.

Done when optional scripts can be authored/run and attached to enabled entries, while all normal internal delivery works without an interpreter.

## 9. O7 — Review, repair and publication

Reuse submissions, acceptance, forge, source/check capture and Task dispatch.

```text
src/automation/delivery.rs
src/review.rs
src/store/review.rs
src/github/projection.rs
src/github/pulls.rs
```

- Deliver direct `review.assign/get/submit`, feedback, continuation and publication first or together with automatic callers.
- Review only an applied retained submission. Reserve exact review slots once across manual and automated requests.
- The handoff is on behalf of the owning manager; review evidence remains authenticated to the assigned auditor, exact candidate and required coverage.
- Aggregate local audited state from evidence, not labels or majority prose. Preserve pass/actionable change/inconclusive distinctions and real producer/reviewer separation.
- Apply scoped feedback through the same accepted manager action path. Stored mail is not native delivery; repair is separately requested or included in the automation.
- Implement preset steps as choices within one enabled definition. No global stage registry and no second activation round.
- Keep original-owner repair preference, explicit route alternatives and native/lease reconciliation. Corrected source remains eligible with unchanged checklist text.
- Preserve exact-candidate acceptance/publication/epoch guards; rights come from the owner, not the workflow service.
- Keep local-first review before push; optional review-namespace upload is a distinct effect, not protected publication.
- Use Rust GitHub Checks/labels/summaries with remote IDs and append-batch readback. Inconclusive is not neutral/skipped success; label failure does not repeat push.
- Respect actual merge endpoint/head/base/repository capabilities and external writers. Native GitHub merge queue is optional; queued/accepted remote requests are not merged results.
- Observe already-issued external merges after local disable. No automatic repository-settings edits or privileged workflow workaround.

Done when the whole manual cycle works, and the manager can enable just audit assignment or selected fuller delivery on their behalf without changing handlers or weakening verification.

## 10. O8 — Cron and typed rules

Extend existing scheduler/config/Store; no second daemon or unrestricted workflow DSL.

- Schedule/rule editors expose the same manager owner and enabled value; do not duplicate them in a separate project control record.
- Use a complete maintained Rust cron evaluator, explicit timezone/DST and next-occurrence preview.
- Preserve attributable legacy schedule intent/receipts; unowned imported definitions default disabled and can be adopted by a manager without rewriting history.
- Distinguish definition, due occurrence and invocation. Edits/restarts do not create new identities for consumed slots or semantic events.
- Direct run-now executes one occurrence while recurrence is disabled, through normal action checks.
- Effect start rechecks enabled entry and manager authority. Restore saved enabled choices after recovery; no compulsory reauthorization on every reboot.
- Intentional re-enable defaults to future events, with optional current-eligible selection. Normal outage catch-up is latest-only unless a safe bounded replay is selected.
- Scope loop prevention, error handlers and budgets to the real definition; a failed notification must not spawn another explanatory agent.

Done when the manager configures/enables a cron or event automation in one interface and it persists, can be disabled and remains attributable to that manager.

## 11. O9 — Server Goal and shared reminders

- Reference existing assigned Tasks and evidence; no new Task graph.
- Goal observation may work without progression. Its automatic actions use one manager-owned enabled definition and existing action rights.
- Maintain exactly one continuation owner for each assignment. Native Goal or unfinished children cannot be displaced by local configuration alone.
- Do not infer achievement from token/commit count or free-text completion claims.
- Reuse #22 watches/subject indexes/shared timer. Direct one-shot reminders and peer mail require no global activation.
- Recurring model nudges or continuation use explicitly selected actions; neither notice nor tool discovery grants execution authority.

Done when the manager can choose tracking, reminders and automatic progression independently without creating competing continuations.

## 12. O10 — MCP/CLI parity and usability

- Keep the role-specific eager cores small; new details are deferred tools.
- Expose `automation.config.get/preview/apply` and `automation.explain`; do not implement the removed `automation.control.*` proposal.
- Existing schedule/rule/Goal surfaces update the same definition and owner. No generic arbitrary-method executor.
- UI lists automations, owners, enabled flags, scope, last/next run, limits and errors. No global assisted/delegated switch.
- A manager can enable one or several entries in a bounded atomic configuration edit; preview is optional assistance, not approval from a second actor.
- Manual controls are never disabled by an automation choice. Show conflicts/unknown in-flight actions with exact references.
- Model/executor preferences use current discovery and change future work; installed capability receipts remain truthful.
- Cached schemas and manual tool calls still pass role/object authorization. Search, list, status and preview never start a model.

## 13. O11 — Qualification cases

These are future execution tests, not results of this documentation review.

| Area | Required case |
|---|---|
| Default | Empty configuration, plugin discovery, new roles and imported queue start no automation. |
| Simple enable | A scoped manager saves `enabled=true` for audit dispatch in one request without mode selection, another manager approval or a new grant object. |
| Mixed work | Audit is automatic; distribution/repair/publication stay manual; the manager's direct commands keep working. |
| On behalf | Event intake selects the configured owner; invocation records that manager and technical executor without sharing tokens. |
| Real manager | An appointed AI manager can configure own automations regardless of launch origin; a role-name-only executor cannot. |
| Current rights | Owner permission loss blocks affected new effects; no Root/App privilege fallback or automatic ownership replacement. |
| Review integrity | Manager-owned handoff cannot author the auditor's result or bypass required independent evidence. |
| Concurrent work | Manual/automatic requests and overlapping automations coalesce the same logical slot or return a precise incompatible-request conflict. |
| Disable wins | Old queued automatic work cannot cross effect-start after the entry is disabled. |
| Start wins | Disable reports in-flight/unknown work, not fictional cancellation; next disabled step does not start. |
| Manual takeover | A never-started held action can be replaced in its slot, preserving both receipts; sending/unknown work cannot be replayed. |
| Configuration | Missing enabled is false for new entries and unchanged on patches; stale file/import cannot reverse a newer toggle. |
| Restart | Saved enabled/disabled choices survive disconnect/reboot; prior effects/ownership are reconciled before continuation. |
| Owner transfer | Explicit transfer validates the new manager; old operations retain original attribution and epoch restrictions. |
| Native/remote owner | Native Goals and remote queued merges remain visible after disable until actual completion/cancel readback. |
| Scope isolation | One manager's toggle does not suspend another manager or unrelated action. |
| Catalogue | Preferred route/model choices are dynamic, bounded and explicit; no fixed software-release requirements. |
| Evidence | Late A cannot mutate B; corrected source can progress; label/projection errors never rerun publication. |

Use simulated large definition/registration populations separately from paid active models. Measure Store/action latency, source gaps, fairness, RSS/handles, event dedupe and control responsiveness. No per-participant pollers or source-wide history scans. Stage live qualification on small fleets first; registration count is not active-turn capacity.

No paid model run, installation, owner-machine cleanup or live repository mutation in CI.

## 14. Migration and setup

Forward migrations add only needed indexed manager-owned configuration, origin/work-pool, review, due and action-slot metadata. Keep existing Operations/Observations as execution/history authority. Preserve historical policy and unresolved evidence.

There are no deployed PR #23 mode/control APIs to keep compatible: remove those proposed names/types from this design instead of building unused aliases. Existing real v1 schedules retain their attributable configuration and receipts. Templates/untrusted imported definitions default off; ordinary recovery of a saved owned enabled definition is not a template import.

Setup gathers private local runtime/source/credential/trust values. It does not auto-create live rules or Goals just to demonstrate a feature. Existing native sessions are not restarted or adopted by heuristic. Libraries and software updates are compatibility-based, not fixed-version prescriptions.

The product is complete when a manager can run everything manually, enable only the helpers they choose, and always understand which actions ran on their behalf, without a second operating mode, duplicate authority or unsafe effect replay.
