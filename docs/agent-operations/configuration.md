# Configuration — Automations Enabled by Their Manager

Revision 8 · 2026-10-03 · WorkDispatch field schema follows `src/automation/config.rs` and `src/automation/work_dispatch.rs`; C8 source commit `a1577aee63094e6fcb3feea6fc6079d1a8454850`; fixture repair commit `7d518ef4edb84c5e8ce677fafa778de914abed30`. C8 source passed owned-crate formatting, warnings-denied Clippy (12.46 s), and the bounded StoreAPI regression (1/1; 0.06 s; build 35.09 s). C8 CI run `37157609062` failed in both OS jobs on three projection fixtures: the fixture database installed schema `001core` but omitted `002workspace`, so `workspace_leases` was absent. Fix commit `7d518ef4edb84c5e8ce677fafa778de914abed30` installs `002workspace`; the targeted projection filter passed 6/6 (0.05 s; build 37.36 s; `.local/qualification/r7-build-gate/projection-c8-ci-repaired.log`). Full Rust CI run `37158328828` passed all Ubuntu and Windows job steps for repair commit `7d518ef4edb84c5e8ce677fafa778de914abed30`, including formatting, Clippy, Rust application/protocol tests, native offline fixtures, and release build. C7 full Rust CI run `37153513585` remains historical evidence for exact CI commit `8570dae7f478b6dd2b604727b34c285a86ee9acc`.

[Architecture](architecture.md) owns execution; [Delivery](delivery.md) owns the shared work handlers.

## 1. One control model

**The manager works manually and enables individual automations that act on their behalf.** Each entry has one manager owner, scope, trigger/preset, selected actions/settings, revision and `enabled`. New entries default to false; an explicit manager request can create and enable one in the same call.

No global mode, additional per-stage activation registry or Root approval is required for actions that manager already controls. Manual commands, observation, peer mail and requested one-shot watches remain available alongside automations. Fleet size, queue import, plugin discovery and model output do not enable entries.

## 2. One editing interface

```text
swarm.tools.search -> automation.config.get + runtime.catalog
                  -> automation.config.preview (optional)
                  -> automation.config.apply
```

- `automation.config.get` returns scoped, paged entries, editable settings, revisions, effective owner, profiles and diagnostics; no secrets or global roster.
- `automation.config.preview` is an effect-free validation/diff, useful for a bulk change. It is not mandatory approval or a second authority.
- `automation.config.apply` atomically applies the explicitly requested changes, including `enabled`, using caller-owned request identity and expected entry revisions.
- `automation.explain` shows why an entry/action is waiting or running, on whose behalf, its source cause, retained inputs and next useful action. It calls no model.

The discarded `automation.control.*` proposal has no compatibility aliases. `runtime.profile.get/list/preview/apply` edits model/executor preferences; owned profile changes may be included in one configuration transaction without a separate call per field.

### 2.1 Patch rules

Use `expected_revision=0` only to create an absent entry. For an existing entry match the returned revision. A manager-local `automation_id` is stable within project/owner; edits do not create a new identity.

Known object fields are patched recursively; explicitly supplied arrays replace the whole array. Missing fields remain unchanged. `steps=[]` therefore removes all selected steps, while omitting `steps` does not. Missing `enabled` is false on creation and unchanged on update. Reject unknown fields, duplicates and null values except where the field schema explicitly permits null. Do not infer a toggle from a preset name.

Only named entries are changed. Omitted entries are not removed or disabled. Bulk updates are bounded and all-or-nothing. Stale revisions return the current scoped values and a useful conflict; identical request retry returns its original receipt even after later changes. A supplied optional preview digest must still match; apply always validates for itself.

A disable-only patch is a local operation. It must not depend on GitHub, model discovery, script validation or a healthy proposed replacement configuration. It returns the new revision and references to unstarted, in-flight and uncertain work. No external I/O occurs in its Store transaction.

### 2.2 Enabling and current work

A change may include `include_existing` alongside `patch`. It defaults false and is meaningful only when enabling an entry or adding executable steps/trigger coverage. With true, the manager requests evaluation of currently eligible work in that scope as well as future facts. It is not permission to replay historical deliveries.

The apply transaction records the committed event cut for the newly enabled coverage. Unchanged steps keep their cursors. A bounded current-state evaluation covers selected waiting work at that cut; events after the cut are handled by the normal durable reader. Both paths share semantic action slots, so boundary races cannot lose or duplicate an audit.

Turning an entry off and on, renaming it, changing a preferred model or importing the same template does not reset completed-action identity. Future-only re-enable does not silently release an old held backlog; those items require the explicit current-work choice or a manual command.

## 3. Owner and current permissions

The server derives `owner_manager_id` from the authenticated creating manager. A submitted owner, role or `run_as` string is not authority. Managers edit their own entries within their managed scope; executors/auditors may prepare proposals without enabling work on someone else's behalf.

An actually appointed AI manager has the same configuration path regardless of how its session was launched. Profile titles and script role names do not appoint managers. Stable manager identity is separate from a native session and an ephemeral MCP connection.

Effective automatic authority is:

```text
manager's current action/object rights
  intersect configured actions and targets
  intersect host, project, candidate and resource policy
```

Existing applicable grants are evaluated internally. Do not require a new automation-grant object for a capability the manager already has. Conversely, an enabled flag cannot make a forbidden publication or another manager's work available. The technical service never falls back to a GM credential.

Record the manager, technical requester/executor, automation/revision and exact trigger/work references. Audit verdicts remain the assigned auditor's evidence, not the handoff sponsor's. GitHub retains its actual authenticated App/user identity.

## 4. Example: automatic audit assignment only

This manager already has review-assignment rights. One request enables that helper and includes currently waiting submissions in the project scope:

```json
{
  "client_request_id": "enable-my-submission-audit",
  "project_id": "project-a",
  "changes": [
    {
      "automation_id": "submission-audit",
      "expected_revision": 0,
      "include_existing": true,
      "patch": {
        "enabled": true,
        "preset": "reviewed_delivery",
        "scope": {"work_pool_id": null},
        "steps": ["review_dispatch"],
        "review": {"profile": "auditor", "required_reviewers": 1}
      }
    }
  ]
}
```

Only applied immutable submissions qualify. Queue launch, return, repair, acceptance and publication remain manual. No extra global switch, Root decision or grant-creation round follows.

To prevent future automatic audits:

```json
{
  "client_request_id": "disable-my-submission-audit",
  "project_id": "project-a",
  "changes": [
    {
      "automation_id": "submission-audit",
      "expected_revision": 1,
      "patch": {"enabled": false}
    }
  ]
}
```

The revision assumes no intervening edit; real calls use the current returned value. Running audits can finish and report. `review.assign` remains available manually.

### 4.1 WorkDispatch: explicit manager-authorized launch settings

The current typed `work_dispatch` object is a complete launch-settings bundle,
not a route-only switch. Replace the illustrative route/profile/surface names
below with values from the manager's current `runtime.catalog`. Use
`automation.config.preview` to check the patch and revision effects; each exact
Task still goes through the canonical launcher preview and can wait on route,
workspace, MCP, capacity or other readiness. C8 source commit
`a1577aee63094e6fcb3feea6fc6079d1a8454850` wires manager-authorized automatic
WorkDispatch and passed a bounded StoreAPI regression; C8 hosted CI has not yet
run. This does not qualify productive dispatch or a complete local cycle. All local model/inference
execution remains deferred by owner.

The authenticated current Manager enables one entry for one project. The server
derives `owner_manager_id` from that Manager; do not submit an owner, role or
`run_as`. Admission and pending-work rechecks require that owner to remain a
registered, enabled Manager and recheck the current entry. The technical actor
does not fall back to GM authority.

```json
{
  "client_request_id": "enable-manager-work-dispatch",
  "project_id": "project-a",
  "changes": [
    {
      "automation_id": "manager-work-dispatch",
      "expected_revision": 0,
      "include_existing": false,
      "patch": {
        "enabled": true,
        "scope": {"work_pool_id": null},
        "steps": ["work_dispatch"],
        "work_dispatch": {
          "route": "route-from-runtime-catalog",
          "agent_profile": "agent-profile-from-runtime-catalog",
          "mcp_profile": "work-participant",
          "mcp_surface": "participant-core",
          "workspace_policy": "manager_owned_worktree",
          "requested_model": null,
          "requested_effort": null,
          "budget": {
            "max_turns": null,
            "max_duration_ms": null,
            "max_cost_units": null
          },
          "stop_conditions": [],
          "purpose": "implementation"
        }
      }
    }
  ]
}
```

`expected_revision: 0` creates a new entry; existing entries use the latest
revision returned by `automation.config.get`. `include_existing: false` is the
future-only choice. `scope` currently accepts only `work_pool_id`; a non-null
pool has no committed membership reader and returns a visible scope gap rather
than widening access. Keep it `null` for the current project-wide Task reader.
The settings object denies unknown fields and requires `route`, `agent_profile`,
`mcp_profile`, `mcp_surface`, `workspace_policy`, nullable
`requested_model`/`requested_effort`, all three nullable budget fields,
`stop_conditions` (at most 16 strings), and `purpose`. No route, profile,
workspace policy, model, effort, budget, or stop-condition default is inferred.
`workspace_policy` currently uses `manager_owned_worktree`. `null` budget values
mean no requested numeric cap for that field; choose bounds required by the
manager's policy instead of treating null as a safe default.

The C8 source path reads only committed local controller Task
facts (`task.create`, `task.revise`, `task.claim`) and checks the exact current
Task revision/Attempt before normal launch preview and manager-authorized
admission. It does not consume GitHub/webhook events. At activation, Store
records the current observation high-water as `activation_cut`. With
`include_existing: false`, the durable per-entry cursor starts at that cut and
processes later facts only. With `true`, it performs bounded catch-up only
through the captured cut, then follows new facts; it does not replay arbitrary
remote deliveries. Cursor, pending readiness state, semantic launch-slot
reservation and admitted Operation are committed through the Store path. The
bounded C8 StoreAPI regression passed. CI run `37157609062` failed because the test fixture omitted `002workspace`; repair `7d518ef4edb84c5e8ce677fafa778de914abed30` adds it, the focused projection filter passed 6/6, and CI run `37158328828` passed all Ubuntu and Windows steps for repair commit `7d518ef4edb84c5e8ce677fafa778de914abed30`.

The `automation.explain` `work_dispatch` projection contains:
`cursor`, `activation_cut`, optional `catch_up_until`, retained `pending`
subjects with readiness reason/wake conditions, and a bounded `recent` window.
Pending readiness is distinct from a source-integrity `gap`; gaps are surfaced
in recent dispositions and are not silently treated as empty success. The
cursor and source observations remain durable, while the recent diagnostic
window is bounded. Unavailable manager authority remains a pending reason; it
does not switch to another owner. This readback is part of the committed C8
source; the bounded regression is not a substitute for hosted CI or end-to-end
qualification.

The C8 source has manual and automatic launch admission share the
manager-scoped immutable semantic slot for the exact Task revision/Attempt and
launch parameters. The bounded StoreAPI regression verified exact slot reuse,
conflict handling, idempotent receipt and one Operation for its covered case.
The existing `swarm.launch` semantic-reuse acknowledgment is immutable and does
not copy mutable progress or result data. Its receipt shape is:

```json
{
  "operation_id": "<operation-id>",
  "operation_state_at_receipt": "queued",
  "receipt_recorded_at_ms": 0,
  "current_state_read_method": "operation.get",
  "semantic_reuse": true
}
```

Treat `operation_state_at_receipt` as a historical acknowledgment field. Read
current progress/result using `operation.get` with `operation_id`; the intended
WorkDispatch recent entry carries the admitted Operation ID and semantic slot
ID. In the example, timestamp `0` is a numeric placeholder; the server supplies
the actual receipt time. Current progress and result come from live
`operation.get`, not the immutable receipt snapshot. C8 hosted CI has not yet
run; productive dispatch and the full cycle remain unqualified.

## 5. Preset steps and prerequisites

`steps` selects actions, not an executable array order. Each action has a typed trigger and committed prerequisites:

| Step | Required fact before eligibility |
|---|---|
| `work_dispatch` | A committed local Task fact identifies a current ready Task revision/Attempt; current Manager, workspace and capacity admission are available. |
| `review_dispatch` | Applied retained submission exists and its required review slot is unfilled. |
| `review_disposition` | Assigned actionable findings apply to that exact current submission. |
| `repair_dispatch` | Guarded feedback was applied and its current owner/continuation can receive the correction. |
| `acceptance` | Exact candidate satisfies the configured audit and acceptance policy. |
| `publication` | Exact acceptance exists and the selected target/effect is permitted. |
| `github_projection` | The corresponding audit/acceptance/publication fact exists; only named projections are written. |

Missing steps are manual. Manual completion of a prerequisite can make an enabled later step eligible; it does not need an earlier step to have been automated. Selecting publication alone cannot manufacture acceptance. Remote-review upload, Issue closure and cleanup remain explicitly configured action choices, not implied publication side effects.

A preset may contain several steps in one enabled entry. Cron, event rules and Goal use the same owner/enabled model with registered typed actions. There is no second `script_run` or `goal_continue` switch to maintain.

Multiple entries may observe the same scope. Equivalent requests share a semantic slot; incompatible auditor/profile/target choices return a specific conflict and the existing reservation. They do not create duplicate work or block unrelated Tasks. The first committed valid reservation is retained; a settings edit is not permission to displace it.

## 6. Settings edits and already admitted work

A saved Operation's original request and effective inputs are immutable. State the boundary explicitly:

| Change | Effect |
|---|---|
| Description/display metadata | Changes presentation, not execution identity or cursors. |
| Preferred model, route, script content or budget | New admissions resolve the new values. Previously admitted work retains its recorded values. |
| Removed step, narrowed target or `enabled=false` | An unstarted action outside the new allowed set is held; later steps cannot start. |
| Added step/trigger coverage | Establishes its activation cut; current work is included only when requested. |
| Changed calendar | Future due selection follows the new calendar; retained occurrences/runs are not reinterpreted. |

Before effect start, recheck the current manager rights and whether the already resolved action is still allowed by the current entry. A changed display revision alone must not stop valid work. Never apply old parameters under a newly substituted target/model without recording a new admission.

To replace a provably unstarted admission, use the ordinary action's explicit replacement/takeover path and the same semantic slot. Keep both receipts. Sending/uncertain work needs readback first. Do not surprise the manager with a running-model swap or silently discard a valid queued assignment after a harmless profile edit.

## 7. Files and defaults

A complete manually operated project may have profiles and no automations:

```toml
schema = "eliot-agent-operations"
project = "project-a"
automations = []

[profiles.writer]
role = "executor"
when_to_use = "Implement the manager's assigned non-overlapping change."
apply_changes = "next_assignment"
fallback_on = []

[[profiles.writer.candidates]]
route = "writer-primary"
model = "MODEL_FROM_WRITER_CATALOG"

[profiles.writer.candidates.native_options]
effort = "EFFORT_FROM_WRITER_CATALOG"
```

Placeholders must resolve to actual supported values. Runtime profiles are preferences, not security roles.

Store owns active entries; TOML/JSON is import/export. File-managed imports are not implemented in the first slice. Before enabling them, a future authenticated source registration must bind the canonical path, effective ACL and reparse-point proof to the manager; imported edits must use revision/owner-epoch compare-and-swap through the same apply path. A file merely found in a writer checkout is not authorization, and failure never falls back to Root.

MCP and file editing share one revision authority. A stale file cannot reverse a newer MCP disable. Invalid/partial saves keep the last valid values and show field errors. Watch parent-directory replacements and reconcile missed events in Rust. Imports create disabled entries unless the manager explicitly enables the named entries; a restore/import is not ordinary crash recovery of the same Store.

## 8. Restart, disable and ownership

Saved enabled/disabled choices survive ordinary host restart and manager-client disconnect. Reconcile unresolved effects, current owner rights and target identity before proceeding. No compulsory re-enable ceremony, implicit fallback owner or history replay.

Disable stops new starts and separate follow-ups, not running agents, native Goals or remote writes. Manual tools keep working and share the same reservations. Results/readback from already started work remain recordable; revocation never justifies forging new output or losing observed effect evidence.

Revoked/deleted ownership blocks only new affected actions. Restored rights can make a still-enabled entry eligible; explicitly disabled entries stay disabled. Owner transfer is a future explicit authorized management action, not implemented in this slice. Before enabling it, compare-and-swap the owner epoch; it is never a side effect of editing, last-editor identity, session silence or queue balancing. Preserve old invocation attribution and reconcile old uncertain effects before replacing anything. GM handover does not automatically transfer every manager's automations or old epoch-fenced publication.

## 9. Dynamic runtime preferences

`runtime.catalog` returns scoped installed/configured routes, actual model IDs/aliases, native options, capability evidence, trust, account-capacity grouping and freshness. Keep role, route, provider/billing, native options, MCP surface and OS identity separate.

Ordered candidates are complete route/model/options tuples. Do not equate effort labels across harnesses. Record requested/effective values and the reason for selection. Unknown capability or quota remains a gap, not a made-up supported value.

New admissions use current authorized preferences. Running jobs are not silently migrated. Compatible CLI/library updates require protocol/capability support, not equality to a fixed release. No runtime installer, callback-time download, automatic downgrade or prescribed old version.

Fallback is opt-in by cause and candidate. Unknown delivery, authentication failure or absent mandatory protection is not permission to try another provider. Routes sharing an account share its real capacity/budget; duplicate aliases do not create quota. A manager can select a permitted route for one manual launch without enabling automation.

## 10. Scripts, Goal and MCP

Script activation chooses future runnable content, not a trigger. An authorized `script.run` is one invocation. A manager enables a specific cron/hook entry to repeat it. Captured bundles/environment preserve admitted-run meaning; an invocation gets only declared API effects, not manager credentials or the right to enable more automations.

Goal tracking starts no work. Its selected progression uses one enabled manager entry and one actual continuation owner. Requested one-shot watches are available without recurring automation; a notice is not a task or approval-prompt answer.

Keep #22's small eager cores. Config get/preview/apply/explain and detailed runtime, hook, script, schedule, Goal, review and forge methods are deferred groups. Schedule/rule/Goal editors update the same entry and enabled flag, not parallel records. Before exposing them, wire their real application handler and result reader.

For review, the normal assigned-auditor result tool is `review.submit`; `task.request_changes` remains the guarded manager disposition. Scoped Manager feedback is available only when a TaskSpec explicitly selects [owner-policy-v2](../owner-policy-v2.md) for a new Attempt. Existing v1 Attempts and their feedback rights/evidence remain frozen and unchanged. Existing legacy MCP schemas do not silently acquire new rights. `automation.explain` and manual review/context reads must find on-behalf Operations through their owner linkage even though the service is their technical requester.
