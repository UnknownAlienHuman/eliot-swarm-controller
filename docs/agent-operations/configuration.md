# Configuration — Automations Enabled by Their Manager

**Current operator status:** See [Implementation Status](../implementation-status.md) for published revisions, gate results, and native qualification. This page owns the manager-facing configuration contract.

### Historical C7/C8 WorkDispatch qualification
Historical C7/C8 CI details are in [Implementation Status](../implementation-status.md).

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
- `automation.config.explain` shows why an entry/action is waiting or running, on whose behalf, its source cause, retained inputs and next useful action. It calls no model.

An invalid retained on-behalf link leaves the authorized entry explanation
readable. Affected `linked_operations` or `linked_operation_history` collections
are withheld with `truncated: true` and a `closed` marker containing
`status: degraded`, `error_code: AUTOMATION_LINK_CORRUPT` and
`category: automation_link_corrupt`. The held ScriptRun reason remains readable.
An empty degraded collection is incomplete history. A missing link, invalid
link identity, or corrupt/unsupported sealed link record closes only that
diagnostic collection; it never supplies authority for an effect.

The `affected_work[].work` projection returned by `automation.config.apply`
uses the same closed representation. A link-integrity failure in this derived
report does not roll back an otherwise valid disable or narrowing change.
Manager authority, expected revisions, configuration validation and any
include-existing action admission remain mandatory. Action execution still
requires its valid retained links. SQLite, transaction and commit failures
remain errors rather than successful applies with degraded diagnostics.

The discarded `automation.control.*` proposal has no compatibility aliases. `runtime.profile.get/list/preview/apply` edits model/executor preferences; owned profile changes may be included in one configuration transaction without a separate call per field.

### 2.1 Patch rules

Use `expected_revision=0` only to create an absent entry. For an existing entry match the returned revision. A manager-local `automation_id` is stable within project/owner; edits do not create a new identity.

Automation IDs are case-sensitive. Operation-impact reads use an exact binary
key-prefix range: `audit_one`, `auditXone` and `Audit_one` remain separate
identities. An underscore in an ID is literal, not a search wildcard.

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

For recovery, `automation.config.get` and `automation.config.explain` accept an
optional `owner_manager_id` read selector. Omission selects the authenticated
manager. The designated current GM may inspect another manager's retained entries,
cursors and linked history in the requested project. This read preserves the
original owner and does not enable, copy, transfer or reset an entry. The GM
profile permits `automation.config.get`, `automation.config.explain` and the
explicit transfer below; `automation.config.preview` and
`automation.config.apply` remain Manager-only. See
[GM session continuity](../gm-session-continuity.md).

### 3.1 Explicit transfer to the current GM

`gm.handover` changes the designated GM but does not transfer automation
entries. After handover, the current GM or a local Operator may call
`automation.config.transfer` for one entry. The destination is always the
current GM; the request identifies only the project, former owner, automation
and expected source revision:

```json
{
  "client_request_id": "transfer-submission-audit",
  "project_id": "project-a",
  "former_owner_manager_id": "GM-previous",
  "automation_id": "submission-audit",
  "expected_revision": 3
}
```

The call atomically retires the former-owner source and transfers that entry to
the current GM. A stale `expected_revision` or an existing conflicting target
entry is refused; transfer does not overwrite or merge the target. Cursors,
pending slots and linked history are preserved. Transfer does not grant
unrelated action rights, rewrite prior actors or replay pending work. While the
successor remains the current GM and the transferred entry and exact Task and
Attempt scope remain current, its explicitly selected `review_dispatch`,
`review_disposition` and `acceptance` steps may continue that Attempt. The
Attempt owner and submission author remain unchanged; new review assignments
use the successor as sponsor, while the assigned reviewer supplies the review
evidence. A resulting correction is attributed to the successor and addressed
to the original Attempt owner. A later transfer does not rewrite an existing
assignment sponsor; its result remains usable only when that sponsor is in the
validated transfer lineage.

`repair_dispatch` is a separate opt-in step. It can queue one bounded next-turn
correction only after the exact assigned finding has an applied return-for-
correction decision, and only while the exact current Task, Attempt,
submission, candidate, ready binding generation and transfer chain remain
valid. The original Attempt owner remains the delivery recipient and binding
owner; the review sponsor and correction decision manager remain their
recorded actors; a later successor owns only the new semantic repair slot.
`agent.send` staying queued does not establish that a native delivery occurred,
and this step does not grant general `agent.send` authority. Use the current
source revision from `automation.config.get` and a stable `client_request_id`
for the explicit mutation.

The CLI form is `swarm automation config transfer --file <json>`, with
`--request-id <stable-id>` supplying `client_request_id`. A GM may inspect the
former owner's entry and transfer it, but the preview/apply configuration
mutations remain available only through the Manager profile. If the transfer
outcome is unknown, resolve it through readback only; do not issue another
transfer mutation.

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

Only applied immutable submissions qualify. This `review_dispatch`-only example leaves queue launch, return, repair and acceptance manual; other effects require their explicit automation step and guards. Publication also requires the manager to explicitly select and configure the Publication step below. No extra global switch, Root decision or grant-creation round follows.

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
WorkDispatch and passed a bounded StoreAPI regression. Hosted CI evidence is recorded in the linked Implementation Status. This does not qualify productive dispatch or a complete local cycle. All local model/inference
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

The `automation.config.explain` `work_dispatch` projection contains:
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
`operation.get`, not the immutable receipt snapshot. productive dispatch and the full cycle remain unqualified.

### 4.2 Publication: explicit accepted-candidate target

Publication settings are optional. The project must also have an enabled local
Forge mapping for the same project and target. Omitting `publication` when
creating an entry leaves it unset; `publication: null` explicitly clears it.
If `publication` is selected without settings, the entry reports
`publication_settings_required` and reserves no Forge Operation.

The manager supplies one exact target and one expected remote state. Set
`expected_create: true` with `expected_old_ref: null` only for an explicit
branch create. For an update, set `expected_create: false` and provide the full
expected old object ID. The two forms are mutually exclusive:

```json
{
  "client_request_id": "enable-accepted-candidate-publication",
  "project_id": "project-a",
  "changes": [
    {
      "automation_id": "publish-reviewed-candidate",
      "expected_revision": 0,
      "include_existing": false,
      "patch": {
        "enabled": true,
        "scope": {"work_pool_id": null},
        "steps": ["publication"],
        "publication": {
          "target_ref": "refs/heads/release",
          "expected_old_ref": null,
          "expected_create": true
        }
      }
    }
  ]
}
```

`include_existing: false` starts at the activation cut and considers later
acceptance facts. Setting it to `true` requests bounded catch-up only through
the captured cut; it does not replay arbitrary remote history. An eligible
accepted candidate reserves the normal immutable `forge.publish_ref` action
under the current manager and GM epoch. Operation admission is separate from
Git effect execution; this example makes no live publication qualification
claim.

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

Revoked/deleted ownership blocks only new affected actions. Restored rights can make a still-enabled entry eligible; explicitly disabled entries stay disabled. `automation.config.transfer` is the explicit, atomic compare-and-swap path for moving one entry from `former_owner_manager_id` to the current GM. It retires the source entry, preserves cursors, pending slots and linked history, refuses a conflicting target, and leaves authority and historical actors unchanged. It is never a side effect of editing, last-editor identity, session silence, queue balancing or `gm.handover`; a GM handover does not automatically transfer automations or old epoch-fenced publication. Resolve an unknown transfer outcome through readback only.

### Module disconnect behavior

A disconnect atomically marks in-flight `sending`/`native_accepted`
Operations on the exact binding generation `outcome_unknown` and record the
binding as disconnected, reusing the existing durable unknown-Operation event.
The current authorized manager receives bounded Operation readback. If an
unresolved `agent.open` has no native root, only exact `agent.reconcile`
readback for that Operation, generation, and supported route is allowed at
admission and dispatch. Never replay the original input or infer verified owner
departure from disconnect; the existing verified-departure path remains
separate. This is core behavior and requires no configuration switch.


## 9. Dynamic runtime preferences

`runtime.catalog` returns scoped installed/configured routes, actual model IDs/aliases, native options, capability evidence, trust, account-capacity grouping and freshness. Keep role, route, provider/billing, native options, MCP surface and OS identity separate.

Ordered candidates are complete route/model/options tuples. Do not equate effort labels across harnesses. Record requested/effective values and the reason for selection. Unknown capability or quota remains a gap, not a made-up supported value.

New admissions use current authorized preferences. Running jobs are not silently migrated. Compatible CLI/library updates require protocol/capability support, not equality to a fixed release. No runtime installer, callback-time download, automatic downgrade or prescribed old version.

Fallback is opt-in by cause and candidate. Unknown delivery, authentication failure or absent mandatory protection is not permission to try another provider. Routes sharing an account share its real capacity/budget; duplicate aliases do not create quota. A manager can select a permitted route for one manual launch without enabling automation.

## 10. Scripts, Goal and MCP

Script activation selects future runnable content. An authorized `script.run`
is one invocation. The manager can configure a script action on **any system
event**, including submissions, hooks, answers, messages, completion, errors
and detected interruption, as well as a calendar occurrence. These examples
are not a trigger whitelist. Event rules select exact source and kind with
optional supported status filtering; a future configured kind waits for an
authorized occurrence instead of requiring a new per-kind execution engine.

All triggers use the same durable event reader, enabled entry, action admission
and script runner. Captured bundles/environment preserve admitted-run meaning;
an invocation gets only declared API effects, not manager credentials or the
right to enable more automations. Explicitly empty script rules disable those
automatic invocations while direct authorized runs remain available.

An event need not have a Task or Attempt. Pass real scope when one exists;
never fabricate a Task to run a hook or handle a host interruption. Task-owner
effects require an actual authorized Task/Attempt. Event inputs are bounded
authorized metadata and references, not raw message bodies, native results or
secrets. A detected interruption with unknown cause must not be reported as a
verified crash. Pending exact occurrences survive restart and explicit entry
transfer; starting them rechecks the current owner, settings, script and scope.
See [Implementation Status](../implementation-status.md) for the delivered
subset; this paragraph specifies the complete configuration requirement.

### Generic system-event ScriptRun

The manager configures exact `source_id` and `event_kind`, with an optional
normalized status. The selector reads metadata rather than arbitrary payload
fields. Provider/harness adapters translate native facts into this common
event contract; cursor management, action admission and execution belong to
the shared transactional kernel.

For example, this automation patch selects committed message sends:

~~~json
{
  "enabled": true,
  "steps": ["script_run"],
  "script_run": {"script_id": "on_message_sent"},
  "event_rules": [
    {
      "source_id": "controller:messages",
      "event_kind": "message.sent",
      "status": "sent",
      "action": "script_run"
    }
  ]
}
~~~

Pass the patch through `automation.config.preview` and
`automation.config.apply` for the selected project and entry. The script must
have an active immutable revision owned by the authorized manager. Rules
match only an authorized retained occurrence. A future source/kind can be
configured before its adapter is available; a status filter waits for that
adapter's supported safe projection. Raw and normalized views of the same
phase/occurrence produce one invocation.

The current normalized producers include message sends/replies, coordination
answers, observed native terminal outcomes/result pages, host interruption,
and Operation terminal facts. Additive SQLite migration `010` installs
`AFTER INSERT` and `AFTER UPDATE OF state` triggers: every Operation first
persisted or transitioned as `rejected` or `outcome_unknown` creates one
bounded `controller:operations` fact in the same transaction, regardless of
which action or provider wrote it. Its stable occurrence identifies the
Operation and phase. The closed projection contains only the phase, status and
fixed error category; it never copies request/result bodies, arbitrary error
text or credentials. The migration does not backfill historical Operations.

Migration `011` applies the same rule to new cancellations. Select
`controller:operations` / `operation.cancelled` with optional status
`cancelled` to react to the target Operation's committed cancellation. The
event identifies the target rather than the cancellation request and supports
taskless actions. Historical cancellations are not added at installation;
repeated readback and the same request do not create a second occurrence.

Provider adapters may expose a safe alias of the same fact. In particular,
only an exact raw `runtime.outcome` of `unknown` aliases the corresponding
`operation.outcome_unknown` phase and occurrence; it is never labeled as a
completed native operation. Applied and rejected native outcomes retain their
existing terminal aliases, and accepted or invalid raw outcomes add no
terminal projection. A validated accepted acknowledgement has the distinct
`native_input_accepted` phase and no normalized status: a rule without a status
filter can select it, while a `completed` filter cannot. Private receipt details
are not selector data, and their size does not impose a separate event filter.
Exact phase/occurrence identity coalesces duplicate views.

The `controller:scripts` adapter projects `script.completed`, `script.failed`
and `script.incomplete` from the exact retained ScriptRun, Operation and result.
Status describes the ScriptRun result, including callbacks carried under
`script.completed`; it does not complete the associated Task.

The `controller:host-lifecycle` adapter projects a graceful `host.exit` as
completed host lifecycle and a failed exit as failed lifecycle. `host.failed`
and the corresponding raw `host.exit` share `host_terminal_exit_observed` and
the `host-terminal-exit:<epoch>` occurrence, so selecting both invokes the
script once. Failure metadata contains a closed `failure_category` and, when
known, the fixed `failed_supervisor` name. A later detected interruption remains
a separate `host_interruption_observed` occurrence. Host lifecycle events are
taskless and do not establish Task completion.

### Native MCP failure selector

C7/C8 native-MCP readback failures use the ordinary generic ScriptRun source
and kind `controller:native-mcp` / `native.mcp.failure`. Omit `status` to select
both normalized `failed` and `unknown` facts. Set `status` to `failed` or
`unknown` to select only that normalized state; these values describe the
failure observation, not Task completion. For example, this enabled entry
selects either kind of native-MCP readback failure through the common
`script_run` action:

~~~json
{
  "enabled": true,
  "steps": ["script_run"],
  "script_run": {"script_id": "on_native_mcp_failure"},
  "event_rules": [
    {
      "source_id": "controller:native-mcp",
      "event_kind": "native.mcp.failure",
      "action": "script_run"
    }
  ]
}
~~~

To narrow that selector, add `"status": "failed"` or `"status": "unknown"`.
The event's safe projection contains the normalized status, bounded error code,
closed failure category, fixed supervisor name, and occurrence identity. It does
not forward native response text, credentials, artifact paths, or other private
readback details. Review those diagnostics through the existing current-rights
`operation.get` read. This is an ordinary generic event/action route: it adds no
provider-specific runner or service. A launch event may be taskless; when Task
scope exists, the usual exact Task/Attempt checks still apply, and the event
never completes a Task.

### HookSource administration selectors

Use `controller:hook-source` with `hook.source.setup` or `hook.source.revoke`
and the ordinary `script_run` action. Omitting `status` selects either kind;
setup projects `applied`, while revocation projects `invalidated`. For example:

~~~json
{
  "enabled": true,
  "steps": ["script_run"],
  "script_run": {"script_id": "on_hook_source_change"},
  "event_rules": [
    {"source_id": "controller:hook-source", "event_kind": "hook.source.setup", "action": "script_run"},
    {"source_id": "controller:hook-source", "event_kind": "hook.source.revoke", "action": "script_run"}
  ]
}
~~~

Both committed facts may be drained after revocation. The retained source and
disabled client must still agree, and current Manager/GM and project checks
still govern admission. Safe input contains the occurrence identity and status;
repository, actor, credentials and raw payload are withheld. Revoked source
credentials remain disabled. Rules for new events may activate without
`include_existing`; enabling historical catch-up first drains its fixed cut
before advancing to later events.

The normalized `controller:messages` lifecycle facts feed event selection;
they do not create a second addressed mailbox delivery in `report.delta` or
`message.read`. Operation-linked observations are filtered by the existing
Operation ACL in SQL before pagination and their exact Operation links are
revalidated after paging. A selected rule receives only its authorized safe
metadata. Detailed diagnostics remain available through ordinary current-rights
Operation reads, not as a broadcast event payload. A retained script error or
hold can be inspected through `automation.config.explain` when its bounded
existing journal is present; that same-owner/current-GM read returns `null`
when absent and does not create a journal or advance a cursor. See
[Implementation Status](../implementation-status.md) for the current
qualification result.

An integrity failure in one retained script revision holds only that entry.
Its bounded history includes the captured revision and error category, while
healthy entries continue. The same revision remains held across reconciliation
and restart; a valid newly active revision permits ordinary revalidation.
Underlying Store and I/O failures are still reported as supervisor errors.

Goal tracking starts no work. Its selected progression uses one enabled manager entry and one actual continuation owner. Requested one-shot watches are available without recurring automation; a notice is not a task or approval-prompt answer.

Keep #22's small eager cores. Config get/preview/apply/explain/transfer and detailed runtime, hook, script, schedule, Goal, review and forge methods are deferred groups. Schedule/rule/Goal editors update the same entry and enabled flag, not parallel records. Before exposing a new method, wire its real application handler and result reader.

For review, the normal assigned-auditor result tool is `review.submit`; `task.request_changes` remains the guarded manager disposition. Scoped Manager feedback is available only when a TaskSpec explicitly selects [owner-policy-v2](../owner-policy-v2.md) for a new Attempt. Existing v1 Attempts and their feedback rights/evidence remain frozen and unchanged. Existing legacy MCP schemas do not silently acquire new rights. `automation.config.explain` and manual review/context reads must find on-behalf Operations through their owner linkage even though the service is their technical requester.

## 11. Fresh-owned OpenCode service

This host/operator route is separate from a manager-owned automation definition. A route declaration is not a launch request: Store starts the service only after an exact Manager/Operator admission for the opening binding, Task revision, Attempt, and held workspace lease. Use the explicit fresh-owned origin and the exact OpenCode V2 runtime/artifact.

~~~toml
[[routes]]
alias = "opencode-owned"
runtime = "opencode_v2"
module_artifact_id = "eliot-opencode-v2.http.2"
enabled = true
native_options = {}

[routes.owned_service]
origin = "fresh_owned_service"
service_id = "opencode-owned"
model = { id = "approved-model-id", providerID = "approved-provider", variant = "approved-variant" }
model_catalog = "offline"
bun_executable = "C:/approved/runtime/bun.exe"
bun_sha256 = "<64 hexadecimal characters: SHA-256 of the Bun executable>"
server_program = "C:/approved/repo/modules/opencode/serve.mjs"
server_program_sha256 = "<64 hexadecimal characters: SHA-256 of the pinned serve.mjs>"
state_root = "C:/approved/private-state/opencode"
port = 0
~~~

This is a schema example only. Replace the generic paths and hashes with approved values. The Bun path must identify a canonical regular file with the supplied matching SHA-256; the running Bun version is checked as 1.4.0. The server program must be the canonical repository-pinned modules/opencode/serve.mjs at the build's compile-time repository root, and its configured SHA-256 must match. The existing state_root must be a canonical, non-reparse directory with access restricted to the service owner. Port 0 requests an OS-selected loopback port; the actual bound endpoint and process identity are read back.

The owned service uses repository-pinned @opencode/server 2.0.7. This differs from the separately installed global OpenCode 2.0.22; this route does not reuse the global installation's profile, databases, credentials, or receipts. The private config contains one plugin-directory entry for the pinned package directory. Its exact index.mjs wrapper re-exports native-mcp-proof.mjs; the server's 2.0.7 input is the singular plugin tuple [package-directory, options]. Configuration/index preparation and source hashes do not prove that the native server loaded the plugin or exposed callable tools.

model_catalog accepts offline or refresh. offline disables the bundled snapshot and fetching. refresh uses the bundled snapshot and fetches public Models.dev metadata. Neither choice authenticates a provider or executes a model call. Historical provider receipts remain historical and separately scoped; they are not proof for this new service. All local model/inference execution remains deferred, including PR24/Kilo.

### Explicit hosted-provider credential source

An owned `opencode-go` route can name a host-only credential source. Add
`credential_ref = "hosted-bunny"` inside its `routes.owned_service` table and
configure the matching source in the original controller configuration:

~~~toml
[opencode_provider_auth_sources.hosted-bunny]
provider_id = "opencode-go"
auth_file = "C:/approved/user-data/opencode/auth.json"
~~~

The model's `providerID` must be `opencode-go`; keep its exact approved model
ID and variant. The source must be an absolute `auth.json` file containing the
selected provider's `type = "api"` entry. The host reads that entry before the
startup boundary and sends the key once to the newly owned service's native
integration endpoint. It does not copy another profile or use an ambient auth
source. The source registry is omitted from serialized Config and generated
participant configurations; the original host configuration remains the source
of that mapping.

The retained proof says `stored_unverified`: native credential metadata was
observed for the exact service process and model. It does not establish key
validity or model execution. Uncertain startup is read back without another
credential POST. A route without `credential_ref` keeps the existing startup
behavior and receives no provider credential.

Each admitted launch receives its own retained owner nonce and private state directory under state_root/launches/<owner_nonce>. Do not copy global OpenCode state into it or manually reuse a nonce directory. The controller embeds a dedicated helper child and keeps its stdin open while the service is owned. Closing that stdin sends EOF as the graceful-stop signal; the helper must exit successfully and produce exact owner/stop receipts. Do not substitute a kill or launch retry.

Before spawn, Store reserves one durable start. A proven pre-helper-spawn NoEffect is recorded only after an exact empty-unknown compare-and-swap. If spawn was attempted or the result is uncertain, the disposition remains outcome_unknown and must not be replayed; recovery is readback-only. Operation readers accept only the two exact negative-proof shapes with NULL process fields, and reserved cancellation writes a canonical bounded negative proof. A retained service that cannot be proved stays fenced.

Workspace cancellation/release remains blocked while the start is outcome_unknown or service_observed, even if the Task revision, Attempt, or current Manager changes. Bounded reconciliation reconstructs the original route and workspace from immutable launch/lease provenance, then requires matching clean server-stop receipts, the exact Bun PID birth/image to be absent, and a matching helper-family stop receipt proving no children remain. Only an exact durable compare-and-swap marks service_departed and removes this service-specific fence; it does not claim a general OS workspace lock.

For current retained-run and qualification evidence, see [Implementation Status](../implementation-status.md). The execution and no-replay rules above remain normative.

### Windows Git metadata path admission

Choose repository and workspace roots whose normalized `.git` paths fit the
workspace adapter's 220-byte Windows policy. Count UTF-8 bytes after supported
verbatim-path normalization. This policy follows the explicit `GIT_DIR` startup
guard in [Git for Windows v2.55.0.windows.5](https://github.com/git-for-windows/git/blob/v2.55.0.windows.5/setup.c#L1071-L1084)
and the [MinGW-w64 `PATH_MAX` definition](https://github.com/mingw-w64/mingw-w64/blob/master/mingw-w64-headers/crt/limits.h#L18);
it is not a portable filesystem limit. Review the policy when changing the Git
integration. Non-Windows admission is unchanged.

An over-limit path returns `WORKSPACE_GIT_PATH_TOO_LONG` before `worktree add`.
Only a proven queued, unbound, preparing launch becomes a closed admission
failure. Current Manager `operation.get` reports the exact code and the action
to configure shorter roots, then admit a fresh launch. The rejected launch has
no attempted native effect. A previously uncertain launch keeps its retained
first failure and exact-readback requirement; shortening a root does not replay
it. `core.longpaths` and global Git configuration are not changed by admission.
