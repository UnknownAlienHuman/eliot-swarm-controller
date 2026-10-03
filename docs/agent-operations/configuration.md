# Configuration — Automations Enabled by Their Manager

Revision 4 · 2026-10-03 · proposed Rust application/MCP contract.

[Architecture](architecture.md) defines execution and authorization. [Delivery](delivery.md) defines the shared work handlers. Examples are proposed schemas, not fields already accepted by main.

## 1. The entire control model

**The manager works manually and can enable individual automations. Enabled automations act on that manager's behalf.**

There is no global `manual/assisted/delegated` mode, separate stage-activation registry or required second control API. Zero enabled automations is the default. A manager can keep assigning work manually while using an automatic audit handoff, reminder or cron job.

Each automation has one owning manager, a scope, a trigger or preset, its selected actions/settings, a revision and `enabled`. New entries default to `enabled=false`; an explicit manager request may create them enabled in the same call. Preset discovery, role registration, queue import and fleet size do not enable anything.

Enabling is the manager's standing instruction to perform those selected actions. It requires no additional Root decision for actions already allowed to that manager. It cannot confer an action, repository or workspace right the manager does not have.

## 2. One configuration interface

```text
swarm.tools.search -> automation.config.get + runtime.catalog
                  -> automation.config.preview (optional dry run)
                  -> automation.config.apply
```

- `automation.config.get` returns the caller's entries, editable settings, revisions, effective owner, profiles and bounded diagnostics. Lists are paged and scoped; secrets and the global roster are absent.
- `automation.config.preview` validates a proposed change and explains its effects without execution. It is useful for a bulk change, not another permission authority or mandatory approval ceremony for a toggle.
- `automation.config.apply` validates and atomically applies the explicitly requested changes, including `enabled`, using caller-owned request identity and expected entry revisions. It may save and enable in one manager request.
- `automation.explain` reports why an entry or work item is waiting/running, on whose behalf, its triggering cause and the next concrete action. It calls no model.

The unimplemented `automation.control.*` proposal is removed; do not ship compatibility aliases for it. Ordinary typed manual tools remain unchanged and callable regardless of enabled automations.

`runtime.profile.get/list/preview/apply` edits reusable model/executor preferences. It does not turn automations on or off. A configuration change may include owned profile updates in the same transaction rather than require a tool round trip per field.

### 2.1 Update rules

Use `expected_revision=0` only to create an absent entry; otherwise match its current revision. A manager-local `automation_id` is stable within its project and owner, not a new identity on every edit. Missing `enabled` means false on creation and unchanged on a patch; reject null or a misspelled key rather than guess intent.

A stale edit returns current revisions and a useful diff. Retrying the same request returns its original receipt, even after later edits. A preview may return a plan digest that the apply optionally requires to remain current; apply always performs its own validation.

A disable-only patch is local: it must not wait for GitHub, a runtime catalogue, scripts or a valid replacement definition. Return held/not-started and in-flight/unknown action references. Do not hold a Store transaction open while waiting for external I/O.

Only explicitly named entries are changed. Omitted entries are not deleted, disabled or enabled. Bulk operations have a bounded list and explicit all-or-nothing validation, so an agent cannot accidentally replace another manager's catalogue.

## 3. Manager ownership and execution attribution

The server assigns `owner_manager_id` from the authenticated manager creating the entry. A submitted owner/role string is not authority. A manager can edit their own entries within their current managed scope. An executor/auditor may prepare a proposal, but does not thereby enable an automation on someone else's behalf.

A real AI manager has the same management path whether its session was started manually or by another authorized manager. Check its assigned role and object rights, not whether a human clicked its launch. Runtime profile names alone do not appoint managers, and scripts do not receive management credentials.

For a triggered action, effective authority is:

```text
owning manager's current action/object permissions
  intersect automation's explicitly configured actions and targets
  intersect existing host, project, resource and candidate policy
```

The service records `on_behalf_of_manager_id`, automation/revision, trigger/cause and technical execution identity. No reusable manager token is copied to executors/scripts. Existing scoped grants, where the product already uses them, are evaluated internally; the manager is not required to create a separate automation-grant object just to use a capability they already possess.

If the manager cannot directly publish that candidate, enabling publication reports the missing right. It does not invoke a GM service account to bypass the restriction. Independent audit results are still attributed to the auditor; manager ownership of the handoff is not authorship of the verdict.

## 4. Example: enable only automatic audit assignment

A manager who already has audit-assignment rights can make this one explicit request:

```json
{
  "client_request_id": "enable-my-submission-audit",
  "project_id": "project-a",
  "changes": [
    {
      "automation_id": "submission-audit",
      "expected_revision": 0,
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

The preset's review trigger is an applied immutable submission, not a commit string or queued submit request. The owner is server-derived. No global mode, separate activation request, named grant or Root approval is needed.

This enables audit assignment only. The manager still launches work, decides how to apply findings, starts repair, accepts and publishes manually. Adding selected steps is another ordinary manager configuration edit; it does not require a different operating mode.

To stop future automatic audits:

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

The example revisions assume no intervening edit. Real calls use the revision returned by the controller. An already running audit can finish and report; manual `review.assign` remains available.

## 5. Select useful actions, not modes

The standard `reviewed_delivery` preset offers these independently selectable steps:

| Step | Action performed on behalf of the manager |
|---|---|
| `work_dispatch` | Reserve/start eligible work in the selected pool. |
| `review_dispatch` | Assign the configured auditor(s) to applied submissions. |
| `review_disposition` | Apply actionable review feedback through the guarded existing Task transition. |
| `repair_dispatch` | Send the selected correction to its current owner through an authorized work command. |
| `acceptance` | Accept the exact eligible candidate if the manager holds that capability. |
| `publication` | Publish that accepted candidate through the configured permitted route. |
| `github_projection` | Update selected managed labels, Checks or summaries. |

These are steps of that automation, not a second project-wide enablement table. Missing steps stay manual. The old `manager_gate/auto_after_audit` mode switch is unnecessary: omitting or selecting publication expresses the manager's choice. Publication always retains actual candidate, audit, acceptance and repository requirements.

Cron/rule/Goal definitions use the same owner-plus-enabled rule and contain their own typed action list. There is no requirement to enable a matching global `script_run` or `goal_continue` stage as well. Notification, check, script and continuation remain registered typed action kinds, not arbitrary method names from input text.

A manager may enable a preset containing several steps at once. The service validates the selected targets, rights, profiles and shared capacity. Disabling one entry changes no other manager's entry or unrelated work.

## 6. File configuration and simple defaults

An empty `automations` list is a valid complete manually operated project. Profiles may exist without any automation:

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

Catalogue placeholders must be replaced with actual supported values. Runtime profiles are preferences, not security roles or automation switches.

Store owns active entries. TOML/JSON is their import/export representation. A manager can select a trusted file-managed configuration source tied to their identity; edits through that source use the same revisioned validation/apply path, including explicitly changing `enabled`. Merely discovering a file in a writer's repository does not establish such trust.

MCP and file editing share one revision authority. After an MCP disable, a stale file snapshot cannot overwrite it: report a revision conflict and require the current edit. Invalid/partial saves preserve the last valid entry. Observe parent-directory atomic replacement and reconcile missed events; do not spawn another configuration service.

Template import creates disabled entries unless the authenticated manager explicitly requests enabling the listed entries. Import cannot change owners by supplying a user ID. Restoring definitions and reauthorizing them is separate from ordinary crash recovery of the same Store.

## 7. Disable, restart and ownership changes

Disabling an entry prevents its future starts and follow-up steps; it does not kill running models, clear native Goals or recall remote writes. Architecture defines serialization with effect start and manual replacement of a never-started pending action. Readback and result recording continue.

Manual actions are always available under the manager's normal rights. If an equivalent automatic operation already runs or has an unknown outcome, return its reference rather than start a duplicate. A manager can manually take a pending never-started action in the same logical slot, without turning the automation back on.

Enabled settings belong to the stable manager, not a live MCP connection. Manager disconnect and routine host restart do not disable or re-enable them. Recover retained requests, verify the owner's current rights/targets and reconcile previous effects, then continue the saved enabled choices. Do not make every restart require another manager approval.

When enabling/re-enabling an event automation, default to future facts. The manager may explicitly include currently eligible waiting items; record the cut and deduplicate against already handled work. Ordinary outage recovery uses the saved cursor and current relevance, not a replay of all old messages. Cron catch-up follows its explicit policy, normally latest-only.

A revoked/deleted manager or lost permission blocks new affected actions with a precise explanation, not a fallback to Root or another credential. Rights restoration can make a still-enabled entry eligible again; an entry explicitly disabled by its owner stays disabled. Explicit ownership transfer uses existing management/handover authority, validates the new owner's scope and preserves history. Do not transfer because of client silence or fleet reassignment heuristics.

## 8. Runtime and model preferences

`runtime.catalog` returns bounded installed/configured executors, actual model IDs/aliases, native options, capabilities, trust, shared account capacity and freshness. Keep role, executor route, model/provider/billing path, native options, MCP surface and OS identity separate.

Ordered profile candidates are complete route/model/options tuples. Native effort values are not assumed equivalent across harnesses. Selection uses existing host/project policy, the manager's profile and authorized per-Task overrides. It records requested/effective values and the selection reason.

Changes affect new assignments. A compatible CLI update or provider alias change does not require a frozen software release; a live assignment is not silently migrated to a different model. Unsupported consequential capabilities are explicit gaps. No runtime installer, unchecked wildcard package download, automatic downgrade or fixed crate/CLI/model prescription is introduced.

Fallback is opt-in by cause and eligible route. Unknown input delivery, authentication failure or missing protection is not permission to start another provider. Accounts shared by several routes share their real quota/budget. The manager can choose an allowed different route for a new manual action without enabling automation.

## 9. Scripts, schedules and Goals

Script authoring and selecting active runnable content remain distinct from running it. An authorized manual `script.run` executes one requested invocation. To run it on cron or a hook, the manager enables that specific automation; no second global switch is required.

Future runs resolve the active script/profile. Retain the actual bundle/environment used by an admitted run so retries cannot change their meaning. A script may perform the bounded actions declared for that invocation; it does not inherit all manager credentials or gain a right to enable other automations.

A Goal may be tracked without automatic continuation. Enabling its selected continuation/actions is the manager's ordinary on-behalf instruction. Native and server Goal remain separate capabilities with one active continuation owner. Explicit one-shot watches remain available to authorized participants; notices are not new assignments or permission-prompt approvals.

## 10. MCP and implementation ergonomics

Keep #22's small eager cores. The deferred automation configuration group contains `automation.config.get`, `automation.config.preview`, `automation.config.apply` and `automation.explain`. Runtime profiles, scripts, hooks, schedules, Goals, review and forge retain their typed groups.

Existing schedule/rule/Goal editors may expose `enabled` directly, but they update the same underlying automation entry. Do not create two enabled fields that can disagree or require users to maintain parallel definitions.

Return owner, enabled state, scope, next eligible action, last result and concrete gaps. Use descriptions such as "Manual control; audit assignment enabled" rather than mode enums. A normal pending manager decision is not a failure. Search, previews, recommendations and dashboards do not start models.
