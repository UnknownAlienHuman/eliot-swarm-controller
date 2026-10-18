# Module API

This page defines the target boundary between Store and native harness adapters.
[Architecture](architecture.md) owns purpose;
[implementation](implementation.md) owns source changes. Rust types in
`swarm-contracts` own field bounds; do not duplicate their schema here.

## Store methods and adapter actions

Managers/frontends call Store. Store authenticates the caller, checks object scope,
admits an Operation and sends one typed semantic action to the selected adapter. The
adapter never receives a manager credential and cannot create Tasks, change owners,
submit review or accept work.

| Store surface | Adapter action | Meaning |
| --- | --- | --- |
| `agent.open` | `Open` | Open, resume or attach the selected native session. Absence never silently creates a replacement in attach/resume mode. |
| `task.dispatch` | `Dispatch` | Deliver the immutable Task/Attempt prompt for the first assigned input. Admission is not execution or Task completion. |
| `agent.send` | `Send` | Deliver `next_turn` input or exact active-turn `steer`. Unsupported steer never degrades into queued input. |
| `agent.reply` | `Reply` | Answer one observed pending question/form/permission with a stale-request guard. It does not change global permissions. |
| `agent.configure` | `Configure` | Apply a supported model, mode, effort or option at its native scope. Return requested and effective values. |
| `agent.goal` | `Goal` | Control native Harness Goal: set/edit/pause/resume/clear as declared by the adapter. |
| `agent.background` | `Background` | Move one specifically eligible native foreground tool/run to background. Optional; not generic process detach. |
| `agent.cancel` | `Cancel` | Cancel one exact native turn or queued input. Unsent controller work uses `operation.cancel`. |
| `agent.recover` | `Recover` | Re-establish one known session/binding after confirmed owner/control loss, without replaying admitted input. |
| `agent.refresh` | `Refresh` | Explicit safe native readback. Ordinary state reads do not require it. |
| `agent.result` | `Result` | Retrieve output attributable to one admitted input/turn through normalized pages. |
| `agent.reconcile` | `Reconcile` | Read back one uncertain admitted action; never repeat it. |
| `agent.shutdown` | `Shutdown` | Release or stop one explicitly owned native resource. It is not turn cancellation. |

The internal protocol is a closed `AdapterCommand` enum. Vendor RPC names/payloads
stay in adapter code. Remove during protocol-2 conversion:

- `native.opencode.loop_step` → `agent.send` steer;
- `native.command.cancel_turn` → `agent.cancel` turn scope;
- `native.command.close_session` → `agent.shutdown`;
- `native.command.release_evidence` → adapter cleanup;
- `native.mcp.*` → OpenCode adapter setup/observation.

`agent.recover` remains because Muse and other durable-session routes need an explicit
no-replay restoration action. `agent.reconcile` remains separate readback for an
uncertain effect.

## Capabilities

An adapter declares semantic support, not package history:

```text
actions
input delivery: next_turn, exact steer
cancel scopes: turn, queued_input
native Goal actions
background control
local activity/result/account-usage sources
```

Capabilities route commands and produce clear unsupported results. They are not
permissions. Store checks caller rights, Task scope and current ownership before the
adapter sees a command.

The descriptor selects launch configuration and required actions. Package version,
build ID and schema inventory are installation/diagnostic facts, not branches in
command behavior. Hello negotiates one current protocol major plus a capability
subset; it does not echo/compare the entire descriptor.

## One native owner and ordered session state

One adapter state owner orders commands and events for each native session or shared
service scope. A shared Codex/OpenCode service connection is not duplicated for every
hosted session. Session state is private to the adapter; the common Eliot link cannot
mutate it.

Queue admission, native input admission, model execution start, native terminal and
Task completion are separate facts. A socket ACK or `200` response proves only the
boundary documented by that native method.

## Native effect identity

Store retains the original Operation, request, binding and generation. Before native
I/O the adapter chooses/retains the strongest native identity available:

| Harness | Native identity/readback |
| --- | --- |
| Muse | durable command ID and session event cursor |
| OpenCode | durable inbox/message ID and session log |
| Codex | client user-message ID, exact thread/turn IDs and exact steer guard |
| ACP/ACPX | queued request identity plus prompt-start/result watch; not idempotent replay |
| Claude | session/resume identity and official history; local prepared effect only where readback is insufficient |
| Command/Antigravity | one serialized run/input window and exact captured output |

Do not promote transport IDs into durable identity. JSON-RPC request IDs, HTTP
request IDs, PID, connection generation and ACK timestamps are not universal replay
keys.

For a harness with native idempotency, retry only the same native ID under that
harness's documented rules. Otherwise retain one small closed prepared-effect record
and serialize conflicting work. A lost response remains unknown until native readback
or explicit disposition. Never start another process/session merely to retry a prompt.

## Store delivery outbox

Native effect state and delivery back to Store are different responsibilities. The
shared `DeliveryOutbox` contains exact already-produced Eliot envelopes:

```text
operation_id
kind = outcome | observation | result
adapter boot/generation
exact bounded encoded envelope
optional content digest
```

The outbox retries only `module.outcome`, `module.observe` or `module.result` until
Store acknowledges the identical envelope. It then removes that delivery using the
existing private durable file primitives. It never repeats native input, invents a
result, changes Task state or reconstructs a vendor session.

One adapter may retain vendor-specific effect state beside the outbox, but that state
is not a common journal or another Store.

## Outcome and result identity

A protocol-2 outcome returns only:

- Operation ID;
- adapter boot/generation needed to reject stale reports;
- disposition (`accepted`, `completed`, `failed`, `cancelled`, `unknown`, etc.);
- optional native input/turn/request IDs;
- bounded action-specific details.

The adapter does not repeat module/artifact/protocol identity and canonical request
hash in every receipt. Store validates the Operation against the authenticated link
and its own retained request. An adapter-local digest may reject conflicting reuse of
the same Operation; it does not become another Store contract.

Task/Attempt identity is supplied once with `Dispatch` and retained by Store. Result
pages reference the original Operation. Hashes remain on actual immutable
result/artifact bytes, not every state or observation.

## Input, cancellation and recovery

Cancellation names one subject:

- controller Operation not yet sent;
- native queued input;
- native active turn;
- owned session/process shutdown.

A transport close, dropped future or timeout proves none of the native subjects. If
cancel outcome is uncertain, preserve original work and use `Reconcile` or local
observation.

The common adapter shell reconnects Eliot IPC and drains retained Store deliveries.
The vendor adapter owns native reconnect/session resume/effect readback. `Recover` is
an explicit Store-admitted request after required owner-loss/readback checks; it is
not an automatic retry and never resends input.

## Harness Goal and Eliot Goal

Harness Goal is native and controlled by `agent.goal`. It is the default where
supported. Native set/resume may start a turn; that turn remains associated with the
initiating Operation and assigned work where applicable.

Eliot Goal is a Store plugin. A selected event or schedule sends an ordinary
`agent.send` message. The Governor may use Harness Goal, Eliot Goal or both.

Each Eliot occurrence has one message Operation. Replaying the same occurrence
returns that Operation; a later occurrence may intentionally repeat the text.
Disabling the plugin does not pause native Goal. Pausing native Goal does not disable
future Eliot messages. Neither mechanism accepts a Task.

## Local state and usage reads

`agent.list`, `agent.state`, `agent.family` and `agent.usage` read Store's local
projection without provider calls.

| Projection | Meaning |
| --- | --- |
| Inventory | Bound and unbound native sessions/children from authorized configured sources. Discovery grants no control. |
| Activity | Native idle/running/waiting/terminal/unknown; pending request/tool/child reason; current Operation/turn and last content activity. Connection health/source freshness are separate. |
| Process | Adapter/external service PID + OS birth identity, liveness/exit, CPU/RSS and ownership confidence. Shared services count once. |
| Workspace/Git | Assigned worktree/branch/baseline; actual branch/HEAD/changes; upstream/push target; last independent remote ref. |
| Provider source | Local JSON/JSONL/SQLite reference, native identity, cursor/offset, source time and coverage gap. |
| Run usage | Session/input tokens, cost and context occupancy when supplied. Cumulative snapshots are not deltas. |
| Account limits | Native buckets/windows/reset/credits with account scope and freshness. Missing evidence is unavailable. |

A transcript or alive PID does not settle an uncertain command. Quota updates do not
refresh work activity. Source failure retains last facts with a gap; it does not erase
the agent.

## Results and artifacts

The single retained path is:

```text
agent.result -> adapter page -> module.result -> artifact.read/assemble -> submission
```

Associate output with an explicit native parent/input ID where present, or one
serialized admitted-turn window. A second independent input, changed generation or
unclassified stream gap requires readback; never choose the latest assistant message
by guess.

A result page carries source Operation, byte range, total/final markers and content
hash. Store publishes immutable bytes before committing its receipt. A file without a
committed receipt is not a result. The exact page remains in `DeliveryOutbox` until
acknowledgment. `artifact.assemble` is the only multi-page assembler.

Result availability does not submit, review or accept a Task.

## Review and acceptance

`review.assign` names one candidate and auditor. `review.submit` stores that auditor's
verdict and requirement/check evidence. `task.request_changes` records manager
disposition; delivery of a repair message is separate.

`task.accept` and automatic acceptance use the same Store gate. It resolves retained
review/check evidence for the exact current candidate and frozen requirements.
Caller-authored duplicate review arrays are removed. Candidate B cannot reuse A's
review. The manager's acceptance decision/reason remains.

## Optional integrations

Hooks, scripts, GitHub effects and diagnostics call the same Store handlers and use
ordinary Operations. They add neither adapter actions nor another Task authority.

A hook records a bounded repository fact. A script revision receives only its closed
grants. GitHub writes retain one effect/readback owner. Disabling diagnostics cannot
disable required local agent observation.
