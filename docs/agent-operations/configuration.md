# Configuration Contract — Manual First, Manager-Enabled Automation

Revision 3 · 2026-10-03 · proposed Rust application/MCP contract.

[Architecture](architecture.md) owns admission and recovery; [Delivery](delivery.md) owns work transitions. Examples below are design schemas, not claims that current main implements these fields. Runtime/model placeholders must be resolved from the installed catalogue.

## 1. Three independent decisions

```text
configuration     how an action would run
permission        whether this principal may request the action
control           whether the manager wants this scope to act unattended now
```

A saved profile, installed plugin, granted capability, imported queue or available preset does not activate automation. Agent count and queue length never select a mode automatically.

New projects/scopes default to `manual`, with no automatic stages or enabled automation definitions. Missing legacy activation evidence also defaults to manual pending a manager decision. The product must work usefully in that state; it is not a setup error.

| Mode | Automatic execution | Manager experience |
|---|---|---|
| `manual` | None | Choose each action directly; dashboard, streams, peer collaboration and retained results remain available. |
| `assisted` | None | Manual controls plus optional bounded deterministic recommendations; no extra model call to produce them. |
| `delegated` | Only explicitly selected stages/definitions | The server executes eligible transitions inside the chosen scope and grant. Every other stage remains manual. |

`paused` suspends unattended execution without erasing the saved mode or selected stages. Setting `mode=manual` returns the scope to manual operation. Neither action kills active agents.

## 2. Configure without starting work

The definition-editing path remains small:

```text
swarm.tools.search
  -> automation.config.get + runtime.catalog
  -> automation.config.preview
  -> automation.config.apply
```

`automation.config.get` returns the active definition, revision, editable fields, provenance, available presets and bounded gaps. It also references the separate effective control state. It exposes no secrets or global fleet dump.

`automation.config.preview` validates desired definitions, profiles and routing against current scope, capabilities and permission. It returns an effective diff, validation errors, future-versus-active effects and plan digest. It launches nothing, installs nothing and spends no model tokens. A requested catalogue refresh is a qualified read-only query.

`automation.config.apply` requires desired definition, `expected_revision`, preview digest and caller-owned request ID. It atomically saves the valid definition. **It does not enable, resume or expand automation.** Same-request retry returns the original receipt. A stale preview returns current revisions and an actionable conflict.

`runtime.profile.get/list/preview/apply` edits execution preferences, not security profiles or control state. Project-owned profiles may be included in one configuration plan; no separate approval round is required for each harmless field. Only future assignments use a changed default.

An executor/auditor may propose settings or edit explicitly delegated fields. Only a manager with control authority over the scope, or an authorized operator/current GM, changes effective unattended behavior. Authoring a rule/script and activating its unattended execution are different capabilities.

## 3. Explicit control path

Use `automation.control.get`, `automation.control.preview` and `automation.control.apply` in a small deferred MCP group.

A control scope identifies one project and the manager-owned work pool or managed project area. A Task/action has at most one effective automation owner. Overlapping active ownership is reported for that item, not resolved by whichever rule runs first. Separate managers can keep their pools in different modes. A project-wide operator stop remains an upper bound.

The control record contains:

```text
scope_id / project_id / work_pool_id when applicable
control_revision / management authority / sponsor
mode: manual | assisted | delegated
paused and reason
selected automatic_stages
selected rule_ids / schedule_ids / goal_ids
execution_grant and bounded target/effect envelope
manual Task/stage holds
activation cut and inclusion choice
resume_after_restart
last control Operation
```

These are per-scope indexed records, not one fleet-wide hot JSON object. IDs and revisions are work/control identity, not software-version pins.

### 3.1 Activate or widen

`automation.control.preview` resolves the exact requested change against current definitions, grants, pool membership and native/external execution state. It shows what may start, what remains manual, affected held/queued work, cost/capacity limits, restart behavior and any unobserved external continuations.

`automation.control.apply` for enable/resume/widen requires the matching preview digest, expected control/configuration revisions, caller-owned request ID and an explicit desired control document. A single manager decision can enable multiple listed stages; there is no per-event Root approval afterward. Unknown or unauthorized fields/actions are rejected, not silently ignored.

Example **explicit opt-in request**, after the definition has been saved:

```json
{
  "client_request_id": "enable-selected-delivery-stages",
  "scope_id": "project-a-managed-pool",
  "expected_control_revision": 1,
  "expected_config_revision": 2,
  "plan_digest": "sha256:FROM_CONTROL_PREVIEW",
  "desired": {
    "mode": "delegated",
    "paused": false,
    "automatic_stages": ["work_dispatch", "review_dispatch"],
    "rule_ids": [],
    "schedule_ids": [],
    "goal_ids": [],
    "execution_grant": "project-a-delivery",
    "resume_after_restart": false
  },
  "start_from": "current_eligible",
  "reason": "Delegate queue distribution and audit assignment; keep repair and publication manual."
}
```

`start_from` is `future_only` or `current_eligible`. It is required when enabling/resuming; the preview names its current eligible work and source cut. Neither value replays every historical event. The manager may intentionally include current waiting submissions instead of waiting for a new commit/event. Dispatch still rechecks current identity, ownership, dedupe and capacity.

### 3.2 Disable or pause immediately

A restrictive `automation.control.apply` may set `mode=manual`, `paused=true`, remove allowed stages/definitions, or add a manual hold without a remote preview. It requires current scope authority and `expected_control_revision`, but must not depend on a healthy GitHub connection, runtime catalogue or valid proposed configuration file.

Return the new control revision plus held-not-started, started, outcome-unknown and externally delegated action references. A revision conflict returns the current local control view so the manager can retry the restriction. Do not run event scripts or notifications requiring remote work inline with this operation.

Pausing and manual control block future unattended effect starts. They do not revoke direct authorized manager commands. `host.mode`/`new_work=disabled`, security policy, resource exclusion and candidate checks remain independent bounds; switching to manual is not a way around them.

## 4. Select stages, not all-or-nothing autonomy

All automatic stages default off. The closed registry initially distinguishes:

| Stage | Unattended action permitted when selected |
|---|---|
| `work_dispatch` | Assign/start eligible work from the manager-selected pool. |
| `review_dispatch` | Assign an auditor for an applied submission, including an eligible corrected submission. |
| `review_disposition` | Apply actionable review feedback through the guarded Task transition. |
| `repair_dispatch` | Deliver the correction to the owner/start the authorized repair work. |
| `acceptance` | Apply exact-candidate acceptance under a separate acceptance grant. |
| `publication` | Perform configured upload/push/merge effects within their exact effect grants. |
| `github_projection` | Update the specifically configured managed labels, summaries and Checks. |
| `scheduled_check` | Start a configured CheckRunner action from an unattended trigger. |
| `script_run` | Start a named authorized external script from an unattended trigger. |
| `goal_continue` | Start an additional supported native input under server continuation ownership. |
| `notification` | Send configured unsolicited automation notices; not ordinary direct mail or an explicitly requested watch. |

An automatic effect from a preset, rule, schedule, Goal or script child must satisfy its stage and current grant. Selecting a rule does not bypass an off publication stage; selecting a stage does not enable every existing rule/schedule/Goal. Non-preset trigger definitions must also be explicitly selected and enabled. Administration, changing control and expanding grants are not automatic action kinds.

Hybrid examples:

- automatic work/audit assignment; manager decides returns, continuation, acceptance and publication;
- manager assigns all work; only auditor dispatch is automatic;
- all delivery transitions manual; one selected diagnostic schedule is enabled by an explicit manager control decision;
- fully delegated eligible delivery, with human/GM gates only where the actual project policy requires them.

The third example is `delegated` for the selected scheduled-check stage, not falsely displayed as globally manual. Dashboard shows effective stages, not just a friendly preset name.

`publication.mode=manager_gate` is an effect policy. It does not disable automatic upstream distribution or repair. `auto_after_audit` likewise cannot activate publication on its own.

## 5. Manual configuration example

The first-run configuration contains no activation or enabled field:

```toml
schema = "eliot-agent-operations"
project = "project-a"
preset = "manual_control"

[work]
order = "manager_order"
max_in_flight_per_manager = 1

[review]
required_reviewers = 1
coverage_policy = "project-current-phase"

[publication]
mode = "manager_gate"
review_transport = "local_candidate"
close_issue = false
```

The control record is separately `manual`, with empty automatic stages/definitions. A standalone manually launched Task does not require a pool, an automation execution grant or a saved delivery preset. Its ordinary role, Task policy and action authority still apply.

To prepare optional larger-team delivery, save a definition with `preset=reviewed_delivery`, the selected pool, manager/writer/auditor profiles and desired capacities. This does not change control state. Runtime profiles may be included:

```toml
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

[profiles.auditor]
role = "auditor"
when_to_use = "Review the captured candidate against the assigned requirements."
apply_changes = "next_assignment"
fallback_on = []

[[profiles.auditor.candidates]]
route = "review-primary"
model = "MODEL_FROM_REVIEW_CATALOG"

[profiles.auditor.candidates.native_options]
effort = "EFFORT_FROM_REVIEW_CATALOG"
```

These are syntactically valid fragments of the typed definition, but model/effort placeholders are not launchable IDs. Catalogue selection replaces them. The one-in-flight value is the manager's ownership invariant; other capacities are explicit editable choices, not a reason to change mode.

## 6. Files, templates and upgrades cannot reactivate automation

Store is the authority for saved definitions and control decisions. TOML/JSON is their import/export format, not a second scheduler database. MCP-managed or trusted file-managed definition editing is explicit per project. Parent-directory watching survives atomic replacement; invalid input retains the last valid definition and records a field-level error.

Watched files edit definitions only. They cannot enable/resume control, remove manual holds, expand its selected effect envelope or override a newer stop decision. Installing a plugin, creating a rule, copying an example or restoring a configuration backup does not constitute activation. A writer's repository change remains a proposal unless its edit rights explicitly cover that field.

Compatible definition/model/script changes within an already enabled envelope affect future admissions. A new action kind, target, trigger definition, grant, cost ceiling or publication scope requires a manager control preview. Existing selected rules cannot quietly broaden their effects through a hot reload.

Migration preserves legacy schedule receipts and records, but a legacy `enabled=true` without an attributable manager activation is imported inactive with `activation_required`. Setup may offer a manager preview to adopt it; it must not silently start it or destroy its history.

## 7. Pause, manual takeover and restart

Control changes serialize with the effect-start boundary described in Architecture. Queued autonomous actions that have not crossed it are held; their Operations/evidence are not deleted. An action that already crossed it is listed as in-flight and may complete. No promise is made that a remote write already in transit can be recalled.

A manager may execute a specific held transition through its normal typed manual command. It competes for the same semantic action/review slot as automation. The source `manual` versus `automatic` is not a new dedupe identity. The caller cannot forge that source; it is derived from authenticated request/cause lineage.

Manual execution authorizes only the named action and its documented finite prerequisites, not the entire downstream workflow. A manual review assignment can complete and record evidence, but cannot silently start repair or publication. A manual launch can prepare its workspace and start its selected agent without requiring automation to be enabled.

Task/stage manual holds narrow enabled control without stopping unrelated pool work. Resume/release of a hold requires a manager decision and fresh eligibility; already manually handled work is not repeated. Expired deadlines are not approvals, and held work never becomes successful merely because the manager waits.

Manager-client disconnect does not undo an intentionally active delegation. Host restart is separate: `resume_after_restart=false` is the default and produces `paused_for_restart`. The manager may explicitly enable unattended restart continuity. Then only still-valid recorded control/grants/targets resume; unresolved effects are read back and stale work is not relaunchable. An uncertain restored control record needs confirmation, not inferred consent.

Native Goals, pending native inputs and external GitHub auto-merge/queue requests have their own execution owners. A local pause reports them. Cancellation/clear is a separately authorized supported action with readback; an unavailable cancel path leaves a visible `external_continuation_unresolved` restriction on that scope. Do not label a takeover complete while such activity can still mutate it.

## 8. Runtime preferences without software pins

`runtime.catalog` returns bounded installed/configured executor models, aliases, native options, capabilities, trust, shared account-capacity group and freshness. Discover from qualified native protocols/manifests rather than embedding a permanent model list.

Keep role, executor route, provider/model/billing, native options, MCP security profile/surface and OS identity distinct. An ordered candidate entry is a complete route/model/options tuple; never blend fragments across providers. Native `high`, `xhigh` and `max` are not assumed equivalent.

Resolution uses the host security ceiling, project policy, role profile and authorized Task overrides. `when_to_use` guides selection but grants no rights. Models/aliases are resolved for each new assignment; live work is not silently switched. Fallback is off unless named causes and alternatives are configured. Authentication failure, unknown input outcome or missing security capability is not permission to reroute. No implicit billing change, credit purchase or reduced review coverage.

Installed executable handles come from trusted local discovery. Accept newer compatible runtimes by protocol/capability evidence, not version equality; preserve observed versions for diagnosis. Additive native data is tolerated where safe; consequential unsupported behavior is reported. No exact dependency/CLI/model pins, runtime wildcard downloads, automatic downgrades or installer activity in hook/timer paths.

Preference changes affect the next assignment. In manual mode, the manager can select an allowed explicit route for the one launch without enabling fallback or automation. A supported live model/options change remains a separate explicit `agent.configure` with actual readback.

## 9. Scripts, Goals and reminders

New rules/schedules/Goals are inactive. Script activation selects runnable content, not a trigger; an authorized manual `script.run` or `schedule.run_now` executes only that invocation even when autonomous scheduling is paused. A Goal created to track progress stays draft/observational until the manager explicitly delegates continuation.

Schedules normally select the active script definition. Each occurrence records resolved bytes/environment; updates do not mutate active runs or replay old receipts. Script authors may edit within their grant, but cannot enable a schedule or widen control. An automatic script's child actions retain automatic lineage and pass the same current stage/control checks.

An explicitly requested one-shot `coordination.watch.create` is a bounded notification request and works in manual mode. Ordinary peer mail and delivery of already assigned results also remain available. Unsolicited recurring nudges, model wake, auto-answering permission prompts and scheduled prompts are not default reminders. They need their specific manager-selected action/definition and applicable capability.

## 10. MCP and explanations

Keep #22's small manager/participant cores. Deferred groups include:

| Group | Methods |
|---|---|
| automation-config | `automation.config.get/preview/apply`, `automation.explain` |
| automation-control | `automation.control.get/preview/apply` |
| runtime-profiles | `runtime.catalog`, `runtime.profile.get/list/preview/apply` |
| review-delivery | `review.assign/get/submit`, ordinary guarded feedback/acceptance methods |
| hooks / scripts / schedules / goals | Typed management and one-shot execution methods |
| GitHub / forge | Read-only work context and separately authorized remote effects |

Slash notation denotes separate exact tools, not a generic dispatcher. Loading a deferred tool cannot activate automation. Disabling automation cannot hide or disable an otherwise authorized manual tool.

`automation.explain` reports mode, management owner, configuration/control revision, selected stage, grant, relevant manual hold, native/external execution, next manual action and exact missing prerequisite. Use `awaiting_manager`/`held_by_control`, not a code-defect status, for deliberately manual steps. Dashboard distinguishes zero automation by choice from broken automation.

Preview/assisted recommendations are deterministic bounded reads. Clicking a specific next action issues that action's normal typed command; there is no generic `execute_any_action` MCP endpoint.
