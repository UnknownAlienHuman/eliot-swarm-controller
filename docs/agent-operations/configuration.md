# Configuration — Automations Enabled by Their Manager

Revision 5 · 2026-10-03 · proposed Rust application/MCP schema, not fields already accepted by main.

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

This manager already has review-assignment rights. One request enables that helper and includes currently waiting submissions:

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
        "scope": {"work_pool_id": "my-work-pool"},
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

## 5. Preset steps and prerequisites

`steps` selects actions, not an executable array order. Each action has a typed trigger and committed prerequisites:

| Step | Required fact before eligibility |
|---|---|
| `work_dispatch` | Current selected Task is ready; owner/workspace/capacity admission is available. |
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

Store owns active entries; TOML/JSON is import/export. A manager may explicitly authorize a local file-managed source under their identity. Its edits use the same revisioned apply path, including enabled changes. A file merely found in a writer checkout is not such authorization.

MCP and file editing share one revision authority. A stale file cannot reverse a newer MCP disable. Invalid/partial saves keep the last valid values and show field errors. Watch parent-directory replacements and reconcile missed events in Rust. Imports create disabled entries unless the manager explicitly enables the named entries; a restore/import is not ordinary crash recovery of the same Store.

## 8. Restart, disable and ownership

Saved enabled/disabled choices survive ordinary host restart and manager-client disconnect. Reconcile unresolved effects, current owner rights and target identity before proceeding. No compulsory re-enable ceremony, implicit fallback owner or history replay.

Disable stops new starts and separate follow-ups, not running agents, native Goals or remote writes. Manual tools keep working and share the same reservations. Results/readback from already started work remain recordable; revocation never justifies forging new output or losing observed effect evidence.

Revoked/deleted ownership blocks only new affected actions. Restored rights can make a still-enabled entry eligible; explicitly disabled entries stay disabled. Ownership transfer is an explicit authorized management action, not a side effect of editing, last-editor identity, session silence or queue balancing. Preserve old invocation attribution and reconcile old uncertain effects before replacing anything. GM handover does not automatically transfer every manager's automations or old epoch-fenced publication.

## 9. Dynamic runtime preferences

`runtime.catalog` returns scoped installed/configured routes, actual model IDs/aliases, native options, capability evidence, trust, account-capacity grouping and freshness. Keep role, route, provider/billing, native options, MCP surface and OS identity separate.

Ordered candidates are complete route/model/options tuples. Do not equate effort labels across harnesses. Record requested/effective values and the reason for selection. Unknown capability or quota remains a gap, not a made-up supported value.

New admissions use current authorized preferences. Running jobs are not silently migrated. Compatible CLI/library updates require protocol/capability support, not equality to a fixed release. No runtime installer, callback-time download, automatic downgrade or prescribed old version.

Fallback is opt-in by cause and candidate. Unknown delivery, authentication failure or absent mandatory protection is not permission to try another provider. Routes sharing an account share its real capacity/budget; duplicate aliases do not create quota. A manager can select a permitted route for one manual launch without enabling automation.

## 10. Scripts, Goal and MCP

Script activation chooses future runnable content, not a trigger. An authorized `script.run` is one invocation. A manager enables a specific cron/hook entry to repeat it. Captured bundles/environment preserve admitted-run meaning; an invocation gets only declared API effects, not manager credentials or the right to enable more automations.

Goal tracking starts no work. Its selected progression uses one enabled manager entry and one actual continuation owner. Requested one-shot watches are available without recurring automation; a notice is not a task or approval-prompt answer.

Keep #22's small eager cores. Config get/preview/apply/explain and detailed runtime, hook, script, schedule, Goal, review and forge methods are deferred groups. Schedule/rule/Goal editors update the same entry and enabled flag, not parallel records. Before exposing them, wire their real application handler and result reader.

For review, the normal assigned-auditor result tool is `review.submit`; `task.request_changes` remains the guarded manager disposition. Existing legacy MCP schemas do not silently acquire new rights. `automation.explain` and manual review/context reads must find on-behalf Operations through their owner linkage even though the service is their technical requester.
