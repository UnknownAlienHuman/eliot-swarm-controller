# Configuration — Manager-Owned Automation and Route Policy

This page owns the manager-facing target configuration. Current source still contains
the v1 WorkDispatch cursors, duplicated launch bundle, transfer relocation and
overlapping schedulers described in
[findings](refactoring.md). [Implementation](implementation.md) owns their removal.

## 1. One entry, one owner, no hidden global mode

A manager enables optional helpers that invoke the same authorized Store actions as
manual commands. An entry has:

```text
entry_id                 immutable internal identity
automation_id            manager-facing name
owner_manager_id
project_id
revision
enabled
trigger                   event | interval | cron
selected typed actions/settings
```

New entries are disabled unless the creating request explicitly enables them. No
Root approval, global workflow mode, lane daemon or second capability grant is
created for an action the manager already controls.

Disabling an entry prevents future admissions. It does not cancel an admitted
Operation, native turn, review, check or publication effect.

## 2. Public editing surface

```text
automation.config.get
automation.config.preview       optional read-only validation/diff
automation.config.apply
automation.config.explain
automation.config.transfer
```

- `get` returns scoped entries, revision and current blockers.
- `preview` is optional and grants nothing.
- `apply` performs one bounded expected-revision transaction.
- `explain` reads current facts and calls no model/provider.
- `transfer` changes owner/revision of the same internal entry after current
  authorization; it does not clone the entry or move journals.

Patch rules:

- `expected_revision=0` creates an absent entry;
- existing entries require their current revision;
- omitted fields remain unchanged;
- supplied arrays replace the complete array;
- unknown fields reject;
- identical request retry returns the original Operation;
- disable-only changes require no external I/O;
- bulk changes are bounded and atomic.

Remove caller-supplied preview digests. Expected entry revision and current object
state provide concurrency control.

## 3. Trigger model

A retained helper uses one trigger:

```text
event {
  source_id
  event_kind
  optional status/filter from a closed typed vocabulary
}

interval {
  anchor_ms
  period_ms
}

cron {
  expression
  timezone
  catch_up = latest_only
}
```

A trigger admits an occurrence; it does not execute external work. Store retains
occurrence identity and the resulting Operation. Replaying one occurrence returns
the same Operation.

Use one in-process timer/admission loop. Do not create one task/process/credential or
cursor family per entry. A Store change notification wakes evaluation; the nearest
due time drives the timer; a slow safety wake recovers a lost notification.

## 4. Typed actions only

Retained entries select closed product actions, not arbitrary Store method names or
JSON workflows:

```text
work_dispatch
review_dispatch
review_disposition
repair_dispatch
acceptance
check_run
goal_message
script_run
explicit publication (optional integration)
```

Automatic Issue-label projection, Concilium and generic ratification are outside the
compact core.

Each action consumes current Store facts. It does not maintain a private copy of the
Task/candidate/provider lifecycle.

## 5. Work dispatch and route policy

### Target shape

```json
{
  "automation_id": "implementation-dispatch",
  "expected_revision": 0,
  "patch": {
    "enabled": true,
    "trigger": {"kind": "event", "source_id": "controller", "event_kind": "task.changed"},
    "actions": ["work_dispatch"],
    "work_dispatch": {
      "routes": ["codex-primary", "muse-fallback"],
      "agent_profile": "implementation"
    }
  }
}
```

The ordered aliases are explicit manager policy. Each route alias in host
configuration owns its exact:

- harness/runtime;
- native service/account/model/effort/options;
- adapter artifact/current protocol;
- root capacity;
- credential reference.

WorkDispatch does not duplicate MCP profile/surface, workspace policy, purpose,
budget and stop conditions. Participant tool profile derives from admitted role.
Workspace policy is the single manager-owned worktree policy. Task/host policy owns
requirements and bounds.

Automatic dispatch chooses the first currently eligible alias and writes that alias
into the launch Operation. It never silently substitutes a provider/model/effort or
changes permissions. Manual launch names exactly one alias.

A lane is a UI/Governor view over this entry, its route list, current queue,
provider/capacity state and active bindings/workspaces. There is no lane table or
daemon.

### Route configuration

A route keeps the current small admission field:

```toml
[[routes]]
alias = "codex-primary"
runtime = "module"
module_artifact_id = "current-codex-adapter"
enabled = true

[routes.admission_policy]
max_concurrent_roots = 4
```

Do not add a second capacity object for every command. Route root capacity is checked
transactionally against unresolved root opens and unreleased bindings.

Provider availability is observed, not configured as truth. Explicit fallback aliases
handle hard provider/model failures.

## 6. Route decision

Automatic selection uses:

1. route enabled/current configuration;
2. required adapter capability;
3. workspace/write availability;
4. current root limit;
5. provider availability observation.

Decision behavior:

| State | Result |
| --- | --- |
| no provider fact ever | one root-capacity-bounded probe may start |
| available | eligible |
| rate-limited/overloaded before retry | held |
| quota exhausted before reset | held |
| transient fact after retry/reset | one probe may start; timestamp is not recovery proof |
| auth/data-policy/model gone | unavailable; use explicit fallback/config change |
| fresh unknown or stale/gapped source | automatic hold; explicit manager probe allowed |

The admitted probe itself is the in-flight guard. Do not create a probe table.

## 7. Current-state dispatch

Enabling dispatch optionally asks for one immediate current-state evaluation.
There is no `include_existing` event backlog, activation cut or copied pending list in
the target.

```text
enable/apply
  -> dispatch_once(entry)
  -> current ranked queue
  -> first eligible Task and configured route
  -> one launch Operation
```

Future relevant Store changes wake another current-state query. A safety wake handles
lost/coalesced notifications.

Delete current WorkDispatch state:

- global and per-entry cursors;
- `activation_cut`, `catch_up_until`, `pending`, `recent`;
- pending recheck rotation and 128-item cap;
- semantic launch slots and duplicate operation links;
- duplicated launch settings;
- quarantine for ordinary blockers.

`automation.config.explain` reads live queue/route/workspace/provider facts and reports
why no admission occurred.

## 8. Goal message

Harness Goal is configured through the selected adapter and is not an automation
entry.

The Eliot Goal helper is an ordinary message action:

```json
{
  "actions": ["goal_message"],
  "trigger": {"kind": "interval", "anchor_ms": 0, "period_ms": 900000},
  "goal_message": {
    "task_id": "task-id",
    "text": "Continue the assigned goal and report blockers."
  }
}
```

It admits an ordinary `agent.send` Operation. A later occurrence may intentionally
repeat the text. Disabling this helper does not pause native Harness Goal.

## 9. Review, repair, checks and acceptance

- `review_dispatch` selects one current submission/candidate and one configured
  reviewer profile.
- `review_disposition` and `repair_dispatch` consume all retained actionable findings
  for that candidate; do not create one manager round per finding.
- `check_run` names an exact candidate and current check profile revision.
- `acceptance` invokes the same candidate-specific Store gate used manually.

The helper does not manufacture review proof. Candidate B requires B's own retained
review/check evidence.

## 10. Script actions

A script revision receives only explicitly configured closed grants. It may invoke
typed manager actions already allowed to its owner. It cannot name arbitrary Store
methods, change its owner, acquire GM authority or create a workflow language.

If a recurring behavior is small and common—dispatch, Goal message, review, repair,
check or acceptance—prefer the built-in typed action over a script.

## 11. Ownership transfer

Target transfer:

```text
UPDATE automation_entry
SET owner_manager_id = current_GM,
    revision = revision + 1
WHERE entry_id = ? AND revision = ?
```

State and occurrences reference `entry_id` and do not move. Prior Operations keep
their admitting actor and input. Pending future effects recheck current owner rights.

Delete:

- disabled former-owner copies;
- successor pointers and ancestry walks;
- six journal relocators and cron relocation;
- A→B→A conflict caused by owner-dependent keys.

## 12. Storage

Use indexed tables for:

- entry identity/owner/project/revision/enabled;
- trigger and next due/last occurrence;
- action selection;
- occurrence → admitted Operation;
- current error/gap where needed.

Bounded action settings may remain versioned JSON in the entry row. Do not use EAV,
key-prefix scans or a table per field. One explicit migration replaces the current
`automation:v1:*` meta families, then old codecs/readers are removed.

## 13. Explainability

Every automatic admission records:

- entry ID/revision and effective manager;
- trigger occurrence/current-state cause;
- Task/Attempt/candidate;
- selected route;
- resulting Operation;
- current blocker when no Operation is admitted.

A blocker is a current fact, not a copied waiting object. Explanations are read
projections and never grant authority.

## 14. Verification

- stale entry revision conflicts without partial changes;
- disabling prevents new Operations and preserves in-flight work;
- same occurrence admits once;
- lost wake is recovered by current-state query;
- route fallback uses only configured aliases;
- 128 blocked Tasks do not hide an eligible Task;
- transient provider reset opens one probe, not all routes;
- transfer preserves entry/occurrence identity;
- no `automation_scheduler` process/credential/page-admit protocol remains after
  scheduler consolidation.
