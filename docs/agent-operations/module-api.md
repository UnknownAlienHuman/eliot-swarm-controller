# Module API contracts

This page documents bounded, existing Store contracts. A manual effect call,
script grant, hook source or Goal record does not itself enable a background
action. Automation remains manager-owned and must select an enabled entry and
its typed step; see [automation configuration](configuration.md).

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

## O8: closed `event_rules` and hook-assisted joins

The current typed event rule is exactly `task.submission` + `applied` +
`review_dispatch`. The action must also appear in the selected `steps`; unknown
rule fields/actions and duplicate rules are rejected. At most 16 rules are
accepted. For old entries, `event_rules` absent or `null` preserves the legacy
TaskSubmission-to-ReviewDispatch route. An explicit empty array means no event
route:

```json
{
  "enabled": true,
  "steps": ["review_dispatch"],
  "event_rules": []
}
```

That entry will not automatically dispatch ReviewDispatch from a submission,
and it will not use `hook_commit` to join a verified commit to an applied
submission for ReviewDispatch. The `hook.emit` source remains authenticated;
verified hook observations and TaskSubmission intake continue to be retained.
Manual `review.assign` and other explicitly selected, independently authorized
actions remain available. To turn on the typed route, select it explicitly:

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
