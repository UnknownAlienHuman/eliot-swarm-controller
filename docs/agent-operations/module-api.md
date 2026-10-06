# Module API contracts

This page documents bounded, existing Store contracts. A manual effect call,
script grant, hook source or Goal record does not itself enable a background
action. Automation remains manager-owned and must select an enabled entry and
its typed step; see [automation configuration](configuration.md).

## M1: ordinary Manager Task planning

`task.create` and `task.revise` are local planning methods for an authenticated
Manager or the pinned local Operator. A current GM already has the ordinary
Manager role for creation and owner-scoped revision, so those calls do not
require the Operator credential or a separate GM gate. The narrow foreign-live
Attempt case below still requires current GM authority. The existing Manager
and GM MCP profiles expose these methods; the Store policy remains authoritative
for every caller.

Create preserves the existing TaskSpec validation and origin-key deduplication.
It starts no worker or native effect. Revision remains compare-and-swap on
`expected_revision`, preserves the prior Attempt and its frozen Task snapshot,
and clears current acceptance for the new revision. The current owner may
revise its live Attempt's Task. A different Manager may revise a Task with an
unreleased Attempt only while designated current GM; the local Operator may
also do so. Observer, Participant, hook-source and Module identities remain
denied. Task planning does not imply Task claim, execution, acceptance or
publication.

The Store derives the live scope from the Task's current unreleased-Attempt
pointer and verifies that the Attempt still names this Task and has not been
released, after the Task revision compare-and-swap check. A Manager who is
neither that Attempt's owner nor the current GM is denied with `FORBIDDEN`;
GM-profile tool visibility alone does not satisfy this check. No caller-supplied
Attempt ID can widen the revision scope, and revision never transfers Attempt
ownership.

Negative acceptance: an ordinary Manager must receive `FORBIDDEN` when revising
another Manager's Task while its authoritative Attempt remains unreleased.
The current GM or local Operator may handle that same foreign scope only with
the current Task revision; a stale revision remains rejected.

## O2: one passive notice for an exact reviewed submission

`coordination.watch.create` accepts `watch_kind: "submission_reviewed"` with
this closed address:

```json
{"submission_ref": "submission-17", "candidate_ref": "candidate-17"}
```

A Manager supplies the exact `task_id`, `task_revision`, and `attempt_id`;
a Participant is constrained to its authenticated scope. Admission requires
that exact current applied submission and candidate. The request also requires
`delivery: "mailbox_header"`, `one_shot: true`, a `client_request_id`, and
`expires_at_ms` in the future, at most 30 days ahead.

Matching verifies the retained assignment and settled `review.assign` and
`review.submit` Operations for that same subject. The notice contains only the
assignment ID, verdict and review time. An authorized watch can collect the
exact late result after the subject becomes historical; a new Task revision
does not turn an already assigned review into a different subject. Existing
credential and watch-scope revocation still apply. Reconciliation after a host
restart recovers the retained result and coalesces the one-shot notice. This
queues no Task, native input, model call or automation.

## O3: readback after a verified bridge-owner departure

Current-GM or local-Operator `operation.get` can return
`module_recovery_action_required` for an unresolved Operation after a verified
bridge-owner departure and new boot. It identifies the original Operation,
exact binding generation, module artifact and old/new boot IDs. A successor GM
can inspect the same action; the original caller and receipt remain intact.

The cause and native effect remain unknown. The action gives an exact
`agent.reconcile` request template and sets `retry_authorized: false`;
readback does not replay the input. Command bridges `.3` and `.4` support only
`agent.open` and `task.dispatch` targets. Codex controller bridge `.3` and
Antigravity bridge `.2` also support `agent.send` targets. Antigravity's missing
prior-boot journal entry can remain unknown. See [host recovery](../host-recovery.md)
for the other durable error receipts and recovery boundaries.

## O4: one source-mapped GitHub Issue label

`github.effect.managed_label` is a manual-only method in the Manager and GM MCP
profiles. A local Operator may call it; a Manager must have current scope for
the exact Task and name its current revision. The GM profile controls tool
visibility; it does not provide credentials or elevate caller authority.
The method sets or removes one reserved Eliot label for a Task still selected
in the registered source work pool and mapped to that GitHub Issue. It is not
an automation step or a bulk-label operation.

```json
{
  "client_request_id": "label-release-ready-17",
  "source_id": "gh-source-1",
  "task_id": "task-17",
  "expected_task_revision": 4,
  "label": "eliot-release-ready",
  "present": true
}
```

Labels are 10–50 bytes, lowercase ASCII `eliot-*` using letters, digits and
hyphens, and cannot end in a hyphen. `present: false` requests removal. The
call retains a normal Operation. The receipt records a durable desired-state
Operation; inspect `operation.get` for its retained state. A queued Operation
is not evidence
that GitHub changed. Once a write may have started, recovery is exact Issue
readback only. Do not resend an uncertain write. The effect uses the existing
configured `gh` account and never sends a model request.

`github.effect.reconcile_managed_label` exposes a separate manual recovery
Operation in the GM MCP profile. The current GM or a local Operator must be
authorized to see the retained unknown Operation:

```json
{
  "client_request_id": "reconcile-label-17",
  "operation_id": "retained-unknown-label-operation"
}
```

It derives the original source, project, repository, Issue and desired label
from that Operation and verifies its exact semantic slot. It performs GET
readback only. A released/superseded Task or changed work-pool selection does
not block this historical read. A mismatch or unavailable read leaves the
original effect unknown; no second label write is sent.

The final transaction checks the same immutable target, desired state and slot
before recording the observation. A GM handover after the authorized GET starts
does not discard its exact observed result. Original caller/request/effective
input remain unchanged; the new readback Operation adds its own provenance.
An identical reconciliation request returns its retained receipt without
another GET. Starting a new readback still checks current rights.

## O4: exact PR description actions and recovery

`github.pull_request.update_description` updates the title/body of one existing
open, unmerged PR. It requires current direct GM or local Operator authority,
an applied accepted-candidate `forge.publish_ref` Operation, and exact retained
project/source/repository, PR ID/number, published head branch/SHA and base ref.
The Manager and GM MCP profiles can explicitly load these manual-only methods;
tool visibility does not grant GM authority.

```json
{
  "client_request_id": "describe-published-candidate-17",
  "publication_operation_id": "confirmed-publication-operation",
  "pull_request_id": 12345,
  "pull_request_number": 17,
  "base_ref": "refs/heads/main",
  "title": "Implement the accepted candidate",
  "body": "The description for this exact published candidate."
}
```

This admits a normal durable Operation. Before its single PATCH, the Store
rechecks current authority and candidate scope. Title/body are UTF-8 strings
bounded to 256 KiB each; title must contain non-whitespace text. The action
does not create, retarget, merge, close or change draft state. A successful
transport response alone does not prove application: exact PR readback must
confirm both desired fields and immutable target identity.

The desired-state slot belongs to `(repository_id, pull_request_id)` across
head changes. An uncertain write retains that slot and cannot be replayed or
bypassed by publishing a new head. A failed pre-write fence rejects only its
exact still-queued Operation; cancellation or an already-started effect wins
the state check. Current scoped GM can use `operation.cancel` for a predecessor's
exact queued PR action even after its Task/Attempt becomes historical. Original
caller/request identity stays intact; sending and unknown effects require
readback rather than cancellation.

`github.pull_request.reconcile_description` admits a separate GET-only
recovery Operation:

```json
{
  "client_request_id": "read-back-description-17",
  "operation_id": "retained-unknown-description-operation"
}
```

Current direct GM/Operator rights and ordinary Operation visibility are checked
at admission and before GET. Desired fields and target provenance come from
the retained original request/publication, not from this reconciliation input.
Historical Task revisions or accepted-candidate selection changes do not block
the read. Its final transaction rechecks the immutable Operation, publication,
resource/slot and desired fields. An authorized GET that crosses a GM handover
can still record its exact observed outcome; beginning another GET requires
the new caller's current authority. Mismatch/unavailable readback preserves the
original unknown outcome. Exact reconciliation replay returns the saved
receipt without another GET. This method never sends another PATCH.


## O5: verified post-commit facts

The repository-local hook calls `hook.emit` with only the setup-issued source
and captured full commit ID. The Store verifies that ID against the source's
registered repository and records `git.post_commit` as a durable observation.
The identity is `{source_id, commit_oid}`; identical replay returns the
retained observation instead of adding a second fact. The callback has no
review, check or commit-veto authority. See [Repository commit hook](../hooks.md)
for installation/readback and retry details.

```text
swarm hook emit --source-id 8e9d2c2f-2dd2-4d3b-bd1d-000000000001 --commit-oid FULL_COMMIT_OBJECT_ID
```

A successful acknowledgment includes `event: "git.post_commit"`, the same
source and commit, `readback_verified: true`, an observation ID, and exactly
one of `recorded: true` or `duplicate: true`. The CLI uses up to three total
attempts (initial plus two retries), only for `HOST_UNAVAILABLE` or
`OUTCOME_UNKNOWN`, with the same source/commit identity. When retries are
exhausted, the foreground CLI reports the last bounded error and exits
nonzero; no durable retry queue is created. The installed detached callback
suppresses output, so its error cannot undo Git's commit.

## O8: event rules and hook-assisted joins

The legacy typed event rules are `task.submission` + `applied` +
`review_dispatch` and `task.submission` + `applied` + `script_run`. Each action
must also appear in the selected `steps`; unknown rule fields/actions and
duplicate rules are rejected. At most 16 rules are accepted. For old entries,
`event_rules` absent or `null` preserves the legacy
TaskSubmission-to-ReviewDispatch route. An explicit empty array means no
automatic event route:

```json
{
  "enabled": true,
  "steps": ["review_dispatch"],
  "event_rules": []
}
```

That entry will not automatically dispatch ReviewDispatch from a submission,
and it will not use `hook_commit` to join a verified commit to an applied
submission for ReviewDispatch. It also will not start the selected ScriptRun
from a submission. The `hook.emit` source remains authenticated; verified hook
observations and TaskSubmission intake continue to be retained. Manual
`review.assign`, direct authorized `script.run`, and other independently
authorized actions remain available. To turn on the typed review route,
select it explicitly:

```json
{
  "enabled": true,
  "steps": ["review_dispatch"],
  "event_rules": [
    {"source": "task.submission", "predicate": "applied", "action": "review_dispatch"}
  ]
}
```

Where `hook_commit` is configured, the join still requires the exact selected
source, verified full commit, matching project/repository registration, and an
applied submission. It creates no review from a commit alone. See
[automation configuration](configuration.md) and [hook routing](../hooks.md).

To select the legacy submission-triggered script invocation, the entry
must also name one script and select the matching closed rule:

```json
{
  "enabled": true,
  "steps": ["script_run"],
  "script_run": {"script_id": "checked_bundle"},
  "event_rules": [
    {"source": "task.submission", "predicate": "applied", "action": "script_run"}
  ]
}
```

This uses the normal `script.run` admission path and one bounded O1 cursor
independent of ReviewDispatch. The trigger must still match the current
Manager, script owner and active immutable revision, and exact current
Task/Attempt when the run reaches its start gate. Ownership transfer moves the
cursor and exact pending causes atomically; unstarted causes remain held until
the successor's current rights are revalidated. The trigger grants no reusable
Manager credential and cannot edit another automation.

ScriptRun also accepts bounded exact `source_id`/`event_kind` selectors with
an optional normalized `status`. Any committed event kind can select this
action; an unknown future selector waits for an authorized matching fact.
See [generic event configuration](configuration.md#generic-system-event-scriptrun).
Provider adapters normalize metadata into the shared event contract, and the
existing runner receives a bounded `system.event` input. Task scope is either
the real complete Task/Attempt tuple or entirely absent; a taskless invocation
receives no controller-effect grants.

### Descriptor-backed generic Module events

An authenticated Module may publish a generic operationless event through
`module.event` only when its retained descriptor opts into
`swarm.module_event_metadata@1` in `event_schemas`. This is an internal Module
method, not an MCP tool. The request has only `event_id`, `event_kind`, and
`metadata`; the Store derives the source client, binding ID and generation from
the authenticated connection and checks that exact retained descriptor.
`event_id` is nonempty, at most 512 bytes, and contains no control characters.
`event_kind` has the same character rule and a 256-byte maximum; the reserved
`runtime.outcome` and `runtime.state` kinds remain on their existing codecs.

The closed `metadata` object contains `schema_id` (exactly
`swarm.module_event_metadata`), `schema_version` (1), and the same `event_kind`,
plus optional `status`, `occurrence_phase`, and `occurrence_id`. Unknown fields
are rejected. Status is one of `applied`, `completed`, `failed`, `incomplete`,
`cancelled`, `rejected`, `sent`, `answered`, `invalidated`, or `unknown`.
`occurrence_phase` and `occurrence_id` must either both be absent or both be
present; their respective limits are 128 and 256 bytes. The metadata event
kind, phase and occurrence ID use nonempty ASCII alphanumeric characters plus
`._:-/@`. Omitting `status` leaves the event statusless; it does not imply
completion. Omitting the occurrence pair does not create one. These fields
carry bounded selector metadata, not an arbitrary payload, Task, Attempt or
Operation.

The Store appends the canonical metadata to the existing observations stream
under the authenticated Module source and exact binding generation. Reusing an
`event_id` with identical binding and metadata is idempotent; reusing it for
different retained facts conflicts. Generic Module events enter the existing
event selector and admission path only after descriptor, source, Manager and
configured-rule checks succeed. If source proof is temporarily unavailable,
the cause remains in the existing bounded pending journal under
`system_event_source_proof_pending`; ordinary revalidation seals that exact
proof and any verified Task scope before release. This source-proof release
path applies only to that exact hold reason. It does not synthesize a Task,
Operation, status or occurrence, and it does not create a new replay path.

Automation transfer preserves causality. An event whose ScriptRun Operation
was already admitted is retained in transfer history with the original cause
and Operation ID; transfer does not rewrite that admitted cause for the new
owner. A still-pending, unadmitted event is moved under the existing transfer
hold and must pass the successor's current actor revalidation before admission.
The successor consumer context is rebuilt only when it selects the same pending
ScriptRun identity; otherwise the stale context is cleared while the original
cause remains held. Transfer alone does not release pending work.

Event phases preserve subject boundaries. A provider's
`native_input_accepted` acknowledgement has no normalized status and can match
a statusless rule; it cannot match a completed filter. Script terminal statuses
describe the exact ScriptRun. `host_terminal_exit_observed` describes graceful
or failed host termination; raw `host.exit` and normalized `host.failed` views
coalesce by the same host epoch occurrence. The script receives only safe
metadata, including a closed host failure category and fixed supervisor name
when present. None of these observations establishes Task completion.

`automation.config.explain` returns the existing bounded ScriptRun journal in
`script_run`, including exact pending causes, `held_reason` and recent error
details. It returns `null` when the entry has never had that journal. Reading
this field creates no journal and advances no cursor. The existing owner and
current GM recovery visibility checks apply. A damaged active bundle holds
only its entry; its exact failed revision remains readable while other entries
progress. A valid replacement active revision can release the retained cause.

## O9: one Goal continuation for one completed terminal EventRef

Select `goal_progression` on an enabled manager-owned entry and name one
existing shared Goal. The Goal must select the same project and exact active,
incomplete Task/Attempt as the terminal source. Current Manager scope, the
selected Goal revision and the ready OpenCode V2 binding generation are
rechecked at admission.

```json
{
  "enabled": true,
  "scope": {"work_pool_id": null},
  "steps": ["goal_progression"],
  "goal_progression": {"goal_id": "release-goal-17"}
}
```

The source must be a settled `task.dispatch`, `agent.send`, or `agent.goal`
`continue` Operation with synchronized execution evidence. Goal set and pause
Operations are not terminal sources. Evidence must report
`disposition: "completed"`, `terminal.outcome: "completed"`, and the exact
terminal EventRef `{id, seq, sha256}` with a nonempty ID, positive sequence, and
valid SHA-256 digest. The retained cause also binds that event
to its source Operation/observation, native session and input, Task revision,
Attempt, binding ID and generation, and selected Goal revision. A single-use
semantic slot is derived from those identities. Reconciliation admits or
reuses at most one normal `agent.goal` Operation with `action: "continue"` for
that exact terminal reference; another pass is readback/deduplication, not a
second continuation.

`native_input_admitted` means only that a native input was admitted. It is not
proof that execution started, the turn completed, the Task was accepted or the
Goal was achieved. Goal progression waits for the later synchronized
completed terminal event. This step admits only the linked `agent.goal`
continuation; it does not infer or enable `agent.send`, create a reminder, or
grant another selected action.

The entry owner must remain the current Manager with authority for the exact
Task scope. `gm.handover` alone does not move the entry. To continue as a new
current GM, explicitly call `automation.config.transfer` for that entry using
the current source revision. Transfer is compare-and-swap, refuses a
conflicting target, and preserves the cursor, pending slots and linked
history; it neither changes historical actors nor replays already admitted
work. See [explicit transfer](configuration.md#31-explicit-transfer-to-the-current-gm).

## O6: one granted message to the exact Attempt owner

A script revision may explicitly declare at most one controller effect:

```json
"controller_effects": ["task_owner_message"]
```

A script run may request the effect only in its result envelope:

```json
{
  "protocol_version": 1,
  "operation_id": "<invocation-operation-id>",
  "run_id": "<invocation-run-id>",
  "result": {"status": "checked"},
  "effects": [
    {"effect": "task_owner_message", "text": "The bounded check completed."}
  ]
}
```

The text must be nonempty and no more than 4096 UTF-8 bytes. Store checks the
request against the exact retained script revision and its immutable grant,
then rechecks the current Manager, script owner, active revision, Task revision
and Attempt. The recipient is taken from that Attempt's owner; the script
cannot choose an arbitrary recipient or invoke another Store method. Store
adopts the message through the ordinary `message.send` Operation and retains
its link to the `script.run`; the run readback reports the effect as applied or
rejected. `script.activate` selects content but starts no run or trigger.
Repeated execution requires a separately selected, enabled trigger.

## O7: taskless event notification to the configured Manager

A script revision may instead declare the closed `manager_notification` grant:

```json
"controller_effects": ["manager_notification"]
```

Only a taskless, event-triggered ScriptRun receives this grant. Store derives
the recipient and sender from the exact retained automation owner; the script
supplies only bounded text. The effect uses the ordinary `message.send` path,
so the Manager receives it through the existing addressed mailbox. A
Task-scoped or direct `script.run` receives no `manager_notification` authority.

The event cursor commits with its durable pending trigger before the script is
run. On completion, the child `message.send` Operation, its provenance link,
and the parent ScriptRun outcome commit atomically. Replaying the same
completion returns the retained result without a second delivery. If the
Manager disables or changes the event route before completion, Store records a
rejected effect and retains the run history without sending a new message.

## Taskless event Task creation from a Manager-owned automation

A script revision can instead declare the closed `task_create` grant:

```json
"controller_effects": ["task_create"]
```

Its result effect is `{"effect":"task_create","spec":{...}}`, where `spec`
uses only the existing strict TaskSpec fields: required `objective`, `phase`,
and `requirements`; optional/defaulted `acceptance`, `dependencies`, `scope`,
`source_refs`, `owner_policy_id`, `source_index`, and
`baseline_candidate_ref`. The canonical spec is capped at 256 KiB. Unknown
fields are rejected by the Store's `deny_unknown_fields` TaskSpec parser.
Only a taskless, event-triggered ScriptRun owned by the current registered
Manager can receive this grant. The script cannot choose a Manager, project,
Task ID, origin key, or request ID: the Store takes the project from the exact
retained automation entry and derives the deterministic child request ID from
the parent Operation, run, and effect tag. It admits the child through the
ordinary `task.create` Operation and receipt path, without assigning an owner
or Attempt. The child Task, effect link, and parent ScriptRun completion share
the same transaction; exact completion replay returns the retained result.
Task-scoped and manual `script.run` calls receive no `task_create` authority.
