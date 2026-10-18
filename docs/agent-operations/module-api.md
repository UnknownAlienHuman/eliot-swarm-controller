# Module API

This page defines the current target boundary between Store and native harness
adapters. [Architecture](architecture.md) owns purpose;
[implementation](implementation.md) owns source changes. Request field bounds live
in `swarm-contracts`; do not duplicate their schema here.

## Store methods and adapter actions

Managers and frontends call Store methods. Store authenticates the caller, checks
object scope, admits an Operation and sends one typed semantic action to the selected
adapter. The adapter never receives a manager credential and cannot create Tasks,
change owners, submit review or accept work.

| Store surface | Adapter action | Meaning |
| --- | --- | --- |
| `agent.open` | `Open` | Open, resume or attach the selected native session. The request states the intended mode; absence of a session never silently creates a replacement. |
| `task.dispatch` | `Dispatch` | Deliver the immutable Task/Attempt prompt for the first assigned input. Admission is not execution or Task completion. |
| `agent.send` | `Send` | Deliver `next_turn` input or exact active-turn `steer`. Unsupported steering does not degrade into queued input. |
| `agent.reply` | `Reply` | Answer one observed pending question, form or permission request with a stale-request guard. It does not change global permissions. |
| `agent.configure` | `Configure` | Apply a supported model, mode, effort or option at its native scope. Return requested and effective values. |
| `agent.goal` | `Goal` | Control the vendor's native Harness Goal: set/edit/pause/resume/clear as declared by the adapter. |
| `agent.background` | `Background` | Move a specifically eligible native foreground tool/run to background. Optional; not a generic process detach. |
| `agent.cancel` | `Cancel` | Cancel one exact native turn or queued input. Unsent controller work uses `operation.cancel`. |
| `agent.refresh` | `Refresh` | Explicitly request supported safe native readback. Ordinary state reads never require this call. |
| `agent.result` | `Result` | Retrieve output attributable to one admitted input/turn through the normalized page contract. |
| `agent.reconcile` | `Reconcile` | Read back one uncertain admitted action. It never repeats the action. |
| `agent.shutdown` | `Shutdown` | Release or stop an explicitly owned native resource. It is not turn cancellation. |

The internal protocol uses a closed `AdapterCommand` enum. Vendor RPC names and
payloads are adapter code. The following old public/common methods are removed during
protocol-2 conversion:

- `native.opencode.loop_step` → `agent.send` with steering semantics;
- `native.command.cancel_turn` → `agent.cancel` with `turn` scope;
- `native.command.close_session` → `agent.shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.*` → OpenCode adapter-internal setup/observation;
- `agent.recover` → adapter reconnect/`agent.open` resume; uncertain effects use
  `agent.reconcile`.

## Capabilities

An adapter declares the semantic actions and scopes it actually implements:

```text
actions
input delivery: next_turn, exact steer
cancel scopes: turn, queued_input
native Goal actions
background control
local activity/result/account-usage sources
```

Capabilities route commands and produce clear unsupported results. They are not
permissions. Store authorization, Task scope and current ownership are checked before
the adapter sees a command.

The descriptor selects required actions and launch configuration. Package version,
build ID and schema lists are inventory/installation facts, not branches in command
behavior. Hello negotiates one current protocol major and the selected subset of
compiled capabilities; it does not echo and compare the entire descriptor.

## Command and result identity

Store retains the original Operation, request, binding and generation. The
`ModuleLink` credential authenticates the adapter. A protocol-2 outcome therefore
returns only:

- Operation ID;
- current adapter boot/generation needed to reject stale reports;
- disposition (`accepted`, `completed`, `failed`, `cancelled`, `unknown`, etc.);
- optional native input/turn/request identifiers;
- bounded action-specific details.

The adapter does not repeat module/artifact/protocol identity and the canonical
request hash in every receipt. Its local journal may use an internal digest to reject
a conflicting replay of the same Operation. Store validates the reported Operation
against the authenticated link and retained request.

Task/Attempt identity is supplied once with `Dispatch` and retained by Store. Result
pages reference the original admitted Operation. Hashes remain on actual immutable
result/artifact bytes, not on every status or observation.

## Input, cancellation and recovery

Native input acceptance, model execution, turn terminal and Task completion are
separate facts. A command may finish when the native queue accepts the input while
the turn remains active.

Cancellation names a concrete subject:

- controller Operation not yet sent;
- native queued input;
- native active turn;
- owned process/session shutdown.

A transport close, dropped response future or timeout proves none of the latter
three. If native cancellation is uncertain, preserve the original work and use
`Reconcile` or local observation. Do not resend input merely because the adapter
reconnected.

The common adapter shell reconnects Eliot IPC and redelivers retained Eliot records.
The vendor adapter owns native reconnect, session resume and uncertain native-effect
readback. There is no universal recovery command or hidden retry layer below Store.

## Harness Goal and Eliot Goal

Harness Goal is native and controlled by `agent.goal`. It is the default where the
adapter supports it. Native set/resume may start a turn; that turn remains associated
with the initiating Operation and assigned Task/Attempt where applicable.

Eliot Goal is a Store plugin. A selected event or schedule sends an ordinary
`agent.send` message. The harness sees only a normal input. The Governor may use
Harness Goal, Eliot Goal or both.

Each Eliot Goal occurrence has one message Operation. Replaying the same occurrence
returns that Operation; a later occurrence may intentionally repeat the same text.
Disabling the plugin does not pause the native Goal. Pausing/clearing the native Goal
does not disable future Eliot messages. Neither mechanism accepts a Task.

## Local state and usage reads

`agent.list`, `agent.state`, `agent.family` and `agent.usage` read the Store's local
projection. They do not contact the provider harness.

| Projection | Meaning |
| --- | --- |
| Inventory | Bound and unbound native sessions/children from authorized configured sources. Discovery does not attach, assign or grant control. |
| Activity | Native `idle`, `running`, `waiting`, terminal or `unknown`; pending request/tool/child reason; current Operation/turn and last native work activity. Connection health and source freshness remain separate. |
| Process | Adapter and external service PID/birth identity, liveness/exit, CPU/RSS and ownership confidence. Shared services are counted once. |
| Workspace/Git | Assigned worktree/branch/baseline; actual checkout branch/HEAD/changes; upstream and push target; last independent GitHub repository/ref/head observation. |
| Provider source | Local JSON/JSONL/SQLite reference, native identity, cursor/offset, source time and explicit coverage gap. |
| Run usage | Session/input tokens, cost and context occupancy when supplied by the native source. Cumulative snapshots are not additive deltas. |
| Account limits | Native buckets/windows/reset/credits with account scope and freshness through `NativeUsageSnapshot`. Missing evidence remains unavailable. |

A provider-local transcript or alive PID does not settle an uncertain command by
itself. A quota update does not refresh work activity. A source failure retains the
last facts with a gap; it does not erase the agent.

## Results and artifacts

The retained path is:

```text
agent.result -> adapter page -> module.result -> artifact.read/assemble -> submission
```

The adapter associates output with an explicit native parent/input ID where present,
or with one serialized admitted-turn window. A second independent input, changed
adapter generation or unclassified stream gap requires readback; never choose the
latest assistant message by guess.

A result page carries source Operation, byte range, total/final markers and content
hash. Store publishes the immutable file before committing its receipt. A crash can
leave an orphan file; a file without a committed receipt is not a result. The adapter
retains the same page until acknowledgment. `artifact.assemble` remains the only
multi-page assembler.

Result availability does not submit, review or accept a Task.

## Review and acceptance

`review.assign` names one candidate and auditor. `review.submit` stores that
independent auditor's verdict and requirement/check evidence. `task.request_changes`
records the manager's disposition; delivery of a repair message is separate.

`task.accept` and automatic acceptance use the same Store gate. The gate resolves
retained review/check evidence for the exact current candidate and frozen
requirements. Caller-authored duplicate review-proof arrays are removed. Candidate B
cannot reuse candidate A's review. The manager's acceptance decision/reason remains.

## Optional integrations

Hooks, scripts, GitHub effects and diagnostics call the same Store handlers and use
ordinary Operations. They do not add adapter actions or another Task authority.

A Git hook records a bounded repository fact. A script revision receives only its
selected closed grants. GitHub writes retain one effect/readback owner. Diagnostic
logging can be disabled without disabling local agent observation.
