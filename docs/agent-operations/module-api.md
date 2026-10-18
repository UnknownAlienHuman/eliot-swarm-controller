# Module API contracts

This page explains the retained Store boundaries. The [shared method catalog](../../crates/swarm-contracts/src/mcp_catalog.rs)
and owning parsers define request fields and bounds; do not maintain another schema
here. [Implementation](implementation.md) owns changes, [delivery](delivery.md)
owns the work cycle, and [configuration](configuration.md) owns enabled automation.
Source observations below apply to `311b770`; target changes are named explicitly.

## Common harness commands

The target controller uses one semantic vocabulary. Reuse
[RuntimeCommandKind](../../crates/swarm-contracts/src/module_command.rs), the method
catalog and existing Store admission. Adapter declarations describe actual support;
neither the manager nor frontends construct vendor RPC payloads.

| Action | Shared boundary and target meaning |
| --- | --- |
| Open or resume | `agent.open` resolves the selected session; `task.dispatch` starts the assigned Task/Attempt with Store's task prompt. |
| Give input | `agent.send` with `next_turn` delivers another input; `steer` addresses active execution. Unsupported steering must not silently become queued input. |
| Answer | `agent.reply` answers the exact observed pending question or approval. It neither changes the general permission mode nor starts unrelated work. |
| Configure | `agent.configure` selects supported model/mode/options from the runtime catalog. Return requested and effective values; vendor configuration stays in the adapter/profile. |
| Harness Goal | `agent.goal` controls the native goal through common actions mapped by the adapter. Native Goal is the default where supported. Its objective and controls are independent of Eliot Goal. |
| Eliot Goal | `goal.*` retains the Eliot objective; its selected plugin sends ordinary `agent.send` messages. The Governor may use it alone or alongside Harness Goal; see [Goal continuation](#goal-continuation). |
| Read | `agent.state` and `agent.usage` read local projections, with no request to the harness. `agent.refresh` is an explicit collection command that may use supported native reads outside Store transactions; it is never an automatic prerequisite for viewing state. |
| Get output or resolve uncertainty | `agent.result` retrieves attributable output; `agent.reconcile` reads back the original uncertain action. Neither repeats its effect. |
| Cancel work | **New target method:** `agent.cancel` names an existing turn/input through its retained Operation, with explicit `turn` or `queued_input` scope. Reuse `operation.cancel` for unsent controller input; replace native-specific turn controls with adapter mappings. Background jobs and process shutdown are outside these two scopes. |
| Release a connection/process | Existing `agent.shutdown` retains its explicit owned-resource scope. A turn-cancel request is not process shutdown. |

Admission, native input acceptance, turn termination and Task acceptance are separate
facts. An applied input Operation can mean the native queue accepted it; it does
not promise a finished turn. Preserve native queue item/turn handles in the adapter
and expose their disposition through that Operation, without copying the native queue.

## Activity and usage reads

Extend the existing projections rather than adding `agent.status` or a quota service.
Current `agent.state` returns a [binding projection](../../crates/swarm-kernel-host/src/store/operations.rs);
supervisor phases describe worker lifecycle, not native agent activity.
In the target, `agent.state` / `agent.family` also accept the source/native identity
returned by inventory, so an observed unbound session can be read locally.

| Readable fact | Target meaning |
| --- | --- |
| Inventory in `agent.list` / family in `agent.family` | Include observed sessions and native children in the authorized source scope even without an Eliot binding, Task or registered producer. The Governor's operational read uses current GM authority; Task/transcript grants retain their own scope. Discovery does not attach, launch or assign work. |
| Activity in `agent.state` | `idle`, `running`, `waiting` or `unknown`, with current Operation/turn, pending request/reason and last native work activity. Connection health and freshness stay separate. A heartbeat or quota refresh is not work activity. |
| Identity and location in `agent.state` | Known Task/Attempt/binding links, native session/child relationships, process identity/liveness, worktree and local provider source. Separate assigned branch, actual checkout, upstream, push target and last remote repository/ref/head. Keep source times/gaps; ownership can be unknown while a process remains observable. |
| Run usage | Native run/session tokens, cost and context occupancy. Attribute to an individual input only when the source supplies that scope. Cumulative snapshots are not additive deltas. |
| Account limits in `agent.usage` | Reuse [NativeUsageSnapshot](../../crates/swarm-contracts/src/native_usage.rs): scoped buckets/windows, reset time, available credit/limit fields, source and freshness. Preserve unavailable values; context size and Goal budget do not establish account quota. |

The [usage reader](../../crates/swarm-kernel-host/src/store/native_usage.rs) already
retains the latest snapshot and makes it stale after connection/boot/release changes.
The startup connection reset currently misses `opening/reconciling` bindings;
fix it so an old `connected` flag cannot keep their quota fresh after host restart.
It currently admits Codex and enumerated Muse Node bridges; the Rust Muse producer
is [incorrectly rejected](refactoring.md#current-source-gaps). Fix this same path;
one connection ID is not an account ID, and several agents do not multiply a shared
account's allowance. Report `unsupported`, `not_observed` and stale evidence honestly.
Fold sparse updates once in the adapter, retain the age of omitted fields, and never
infer renewed permission from a reset clock or lower percentage alone.

The target [passive collectors](implementation.md#passive-agent-observation) update
this view from native events, local files/databases, OS process facts and local Git.
An unavailable source leaves its last observation and an explicit gap; it does not
erase the agent. Provider-local history is useful observation even when the control
connection is down. It does not by itself prove that an uncertain input was accepted
or authorize a resend. [Observability](observability.md#5-live-monitoring-and-future-charts)
defines the Governor's combined read.

`monitor.follow` retains addressed journal history and wakes a live view. Preserve
binding/generation or source identity in its envelope. A later journal cursor may
contain an older native sequence; coalesce local projection reads after notification
instead of treating raw payloads as authoritative state deltas. No provider call is
needed. These inventory/read changes remain [implementation work](implementation.md#passive-agent-observation).

## Task planning

`task.create` and `task.revise` accept a registered Manager or local Operator.
Creation validates the TaskSpec and deduplicates its origin; it starts no worker.
Revision checks `expected_revision`, preserves prior Attempt snapshots and clears
current acceptance. It never transfers ownership.

For a live unreleased Attempt, Store derives its owner from the Task's current
Attempt pointer. The owner may revise; another Manager needs current GM authority.
The Operator may handle that scope. A caller-supplied Attempt or GM-profile tool
visibility cannot widen rights. These checks already exist in
[Store tasks](../../crates/swarm-kernel-host/src/store/tasks.rs).

## Results and artifacts

| Boundary | Existing owner and meaning |
| --- | --- |
| `agent.result` | Admits a bounded result-page command for the named native input/selector. A result request is an Operation, not Task acceptance. |
| `module.result` | The authenticated adapter supplies that Operation's immutable page. [Store::persist_result](../../crates/swarm-kernel-host/src/store/mod.rs) validates source/range and publishes the file before committing artifact metadata, Operation outcome and observation. |
| `artifact.read` | Reads authorized retained artifact bytes through Store's verified file/cache path. A native status or missing file is not replacement body content. |
| `artifact.assemble` | Uses registered pages of one result to produce a retained complete body. [Assembly](../../crates/swarm-kernel-host/src/store/assembly.rs) and [normalized-result validation](../../crates/swarm-kernel-host/src/store/normalized_result.rs) enforce source identity, contiguous offsets, final coverage and full payload identity. |

The adapter retains the same pending page until acknowledgment; retrying it must
not request a new native effect. A conflicting page for a settled Operation fails.
The file and SQLite writes are ordered, not atomic together: a crash between them
can leave an orphan file. [ArtifactFiles::publish](../../crates/swarm-kernel-host/src/artifacts.rs)
verifies an existing deterministic file on retry. A file without its committed
receipt is not an available result.

A whole-body page may be submitted directly. Otherwise assemble the pages first;
[submission validation](../../crates/swarm-kernel-host/src/store/submissions.rs)
already rejects partial normalized candidates. Preserve native input/turn scope
and distinguish model output from captured source and independent check evidence.
The planned missing-parent association change belongs to
[launch and results](implementation.md#launch-and-results).

## Review and acceptance

`review.assign` names one current submission/candidate and assigned auditor.
`review.submit` stores that auditor's verdict, findings, coverage and required
per-requirement evidence. It does not apply feedback or accept the Task.
`task.request_changes` applies an authorized manager disposition; native repair
delivery is a separate action.

The [review owner](../../crates/swarm-kernel-host/src/store/reviews.rs) retains late
results against their original assignment. Historical read/result access stays
scoped to that assignment and does not grant new work or generic artifact access.

**Current gap:** direct `task.accept` accepts caller-authored `reviews` with the
required requirement IDs; the assigned-pass prerequisite exists only in the
automatic producer. The [common-gate replacement](implementation.md#automation-and-delivery)
must make manual and automatic acceptance consume the same retained review/check
evidence. Keep one acceptance Operation and the manager's decision; remove the
second review-proof payload. See [delivery](delivery.md#8-acceptance-upload-and-publication)
for candidate replacement and completion.

## Readback and cancellation

`operation.get` reports the retained outcome and supported next action. A verified
bridge-owner departure can expose an exact `agent.reconcile` request for the
original Operation/binding; it grants no retry. Adapter support comes from the
selected runtime contract, not a universal bridge-version table. See
[host recovery](../host-recovery.md) and the runtime's guide.

Cancellation has a concrete subject. Cancelling queued controller work, declining
a pending native user-input request, cancelling a turn, and stopping an owned
process are different operations. Muse `userInput/cancel` only addresses its
matching pending request. Dropping a response future, closing IPC or reaching a
timeout does not establish native cancellation. Preserve uncertain work and use
the existing lifecycle owner's [stop/readback behavior](implementation.md#recovery-and-progress).

## Passive review notice

`coordination.watch.create` with `watch_kind: "submission_reviewed"` addresses an
exact applied submission/candidate. It requires current scope, one-shot mailbox
delivery and a bounded expiry. Matching verifies the retained assignment and
settled review Operations, then emits only assignment ID, verdict and review time.

An admitted watch can retain an exact late result after its candidate becomes
historical, subject to credential/scope revocation. Restart reconciliation
deduplicates its one notice. It grants no Task, native input or automation action.
Reuse the existing [watch owner](../../crates/swarm-kernel-host/src/store/coordination_watch.rs);
do not add a council or review-notification engine.

## Explicit GitHub effects

These optional commands use the existing configured `gh` account and ordinary
durable Operations. The [GitHub effect owners](../../crates/swarm-kernel-host/src/store/github_effects.rs)
and [PR effects](../../crates/swarm-kernel-host/src/store/github_pr_effects.rs)
retain intent before external I/O.

| Method family | Admission and recovery |
| --- | --- |
| `github.effect.managed_label` | One registered source-mapped Issue selected in the current Task work pool, one bounded `eliot-*` label and desired presence. Manager scope or local Operator authority is required. |
| `github.effect.reconcile_managed_label` | Current GM/Operator reads the original unknown label Operation's exact Issue and desired state. GET only; historical Task selection does not prevent authorized readback. |
| `github.pull_request.update_description` | Current GM/Operator, an applied accepted-candidate publication, and the exact open PR/repository/head/base. One title/body update; no merge or retarget. |
| `github.pull_request.reconcile_description` | Current GM/Operator reads the retained target and desired fields. GET only; another head or request ID cannot bypass an uncertain PR slot. |

A queued receipt is intent. Readback must confirm the exact requested effect.
An uncertain write holds its semantic resource slot; reconciliation never sends
another write. Replaying reconciliation returns its saved receipt. A GET admitted
before a GM handover may record the exact observed result while retaining original
actor/input history; a new GET checks current rights. Preserve unrelated remote
content. These methods do not require publication automation or a second HTTP client.

## Hook and module facts

`hook.emit` accepts the registered source and captured full commit ID. Store
verifies the repository object and deduplicates `(source_id, commit_oid)` before
acknowledging `git.post_commit`. The hook records a fact; it does not submit work,
start review or veto Git. [Hooks](../hooks.md) owns installation and bounded retry.

`module.event` accepts a descriptor-declared metadata schema from the authenticated
module. Store derives source and binding generation. Same event ID and facts
deduplicate; conflicting reuse fails. Selector metadata is bounded and closed,
not an arbitrary Task/Operation payload. Status/occurrence are optional observed
facts: native input admission does not mean turn or Task completion.

Enabled rules consume committed facts through existing admission. A temporarily
unprovable source remains a scoped pending cause; it cannot invent Task scope or
authorize replay. Duplicate host-terminal views share an occurrence identity.
[Configuration](configuration.md#generic-system-event-scriptrun) owns selectors;
[observability](observability.md) owns diagnostic content.

## Selected actions and script grants

Manual local methods need no automation object. A selected helper calls those
same methods on behalf of its current manager. A verified commit alone cannot
stand in for an applied submission. Persist each cursor with its pending cause
or admitted action, preserving original Operations across owner transfer.

The current parser still has legacy absent/null `event_rules` behavior.
The [current-format conversion](implementation.md#current-format-and-existing-data)
removes that branch with its callers; new guidance uses explicit selected rules.
A transfer changes future authority, not the identity of completed work.
[Stable entry identity](implementation.md#automation-and-delivery) replaces
cursor relocation and successor-copy machinery.

Scripts use immutable activated revisions and the selected runner. A revision may
declare one of these closed controller-effect grants:

| Grant | Scope and existing handler |
| --- | --- |
| `task_owner_message` | A Task-scoped invocation can return bounded text to the current Attempt owner through `message.send`; it cannot choose another recipient. |
| `manager_notification` | Only a taskless event-triggered invocation can notify its retained automation owner through `message.send`. |
| `task_create` | Only a taskless event-triggered invocation can return a bounded strict TaskSpec to `task.create`; Store derives project, identity and origin, and creates no Attempt. |

Store validates the exact invocation, active revision, current owner and selected
grant at effect admission. Child Operation, effect link and parent completion
commit together; exact completion replay reuses that result. A changed or disabled
route can reject a new effect while retaining the script outcome. Activating a
revision starts no run, and a result envelope grants no general Store credential.
The [script owner](../../crates/swarm-kernel-host/src/store/scripts.rs) implements
these paths; keep format validation there without adding generic compatibility rules.

## Goal continuation

**Current:** `goal.*` manages a Task/Attempt-scoped objective, completion evidence
and optional reminder. Its CRUD/enable calls do not directly submit model input;
separately enabled progression can act on that record. Binding-scoped `agent.goal`
has `set/edit/pause/resume/clear/continue`. Production
[Goal progression](../../crates/swarm-kernel-host/src/store/automation_goal_progression.rs)
chooses OpenCode `agent.goal continue` or Codex `agent.send next_turn`; both retain
the original terminal EventRef and Task/Attempt/Goal linkage. This is not yet the
general Eliot message plugin; its settings have no recurring-message policy.

**Target:** Harness Goal and Eliot Goal are independent and can coexist.

| Mechanism | Meaning and control |
| --- | --- |
| Harness Goal | The native session objective, set and controlled through `agent.goal`. The adapter maps common actions and exposes native state. The Governor uses it by default where supported; no Eliot Goal ID is required. Unsupported native control is reported, not silently emulated. |
| Eliot Goal | The Eliot-side plugin uses its retained objective and selected event/cadence to send new or repeated messages through `agent.send`. To the harness these are ordinary inputs; they do not create or replace a native Goal. |

The Governor may give the native Goal the main implementation objective and have
the Eliot plugin send a periodic checkpoint request, or choose either mechanism
alone. Enabling the plugin requires no native pause or driver handover. Native set
or resume can start work; record the resulting turns against the initiating
Operation and avoid accidentally duplicating the initial dispatch. Deliberate
later messages remain permitted.

Each plugin occurrence gets an ordinary message Operation. Transport replay must
reuse it; a later scheduled occurrence may intentionally repeat the same text.
Disabling the plugin stops its future messages. Native pause/clear affects the
native Goal only. Cancelling an active turn or queued input remains a separate
addressed command. Neither Goal's completion claim replaces review and acceptance;
late results retain their original Task/Attempt where assigned.

Remove the host's vendor message branches and OpenCode pseudo-Goal `continue`
when those callers move to the plugin. Keep native revision handling in adapters.
[Implementation](implementation.md#harness-goals-and-the-eliot-goal-plugin) owns
the source change; [configuration](configuration.md) owns plugin enablement.
