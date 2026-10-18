# Module API

This page defines the target boundary between Store and native harness adapters.
[Architecture](architecture.md) owns purpose;
[implementation](implementation.md) owns source changes. Rust types in
`swarm-contracts` own field bounds; do not duplicate schemas here.

## Store methods and semantic actions

Managers and frontends call Store. Store authenticates the caller, checks object
scope, admits an Operation and sends one typed action to the selected adapter. The
adapter never receives manager credentials and cannot create Tasks, change owners,
submit review or accept work.

| Store surface | Adapter action | Meaning |
| --- | --- | --- |
| `agent.open` | `Open` | Open, resume or attach the selected native session. Attach/resume never silently creates a replacement. |
| `task.dispatch` | `Dispatch` | Deliver the immutable Task/Attempt prompt for the first assigned logical input. |
| `agent.send` | `Send` | Deliver `next_turn` input or exact active-turn `steer`. Unsupported steer never degrades into queued input. |
| `agent.reply` | `Reply` | Answer one observed pending question/form/permission with a stale-request guard. |
| `agent.configure` | `Configure` | Apply supported model/mode/effort/options at their native scope and report effective values. |
| `agent.goal` | `Goal` | Set/edit/pause/resume/clear native Harness Goal as declared by the adapter. |
| `agent.background` | `Background` | Move one specifically eligible native run/tool to background. |
| `agent.cancel` | `Cancel` | Cancel one exact native turn or queued input. Unsent controller work uses `operation.cancel`. |
| `agent.recover` | `Recover` | Restore one known session after confirmed owner/control loss without replaying input. |
| `agent.refresh` | `Refresh` | Explicit safe native readback. Ordinary local state reads do not require it. |
| `agent.result` | `Result` | Retrieve output attributable to one admitted logical input through normalized pages. |
| `agent.reconcile` | `Reconcile` | Read back one uncertain admitted action; never repeat it. |
| `agent.shutdown` | `Shutdown` | Release or stop one explicitly owned resource. It is not turn cancellation. |

The internal protocol is a closed semantic enum. Vendor RPC names and payloads stay
inside adapters. Route migration removes:

- `native.opencode.loop_step` → `Send { steer }`;
- `native.command.cancel_turn` → `Cancel { turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → private cleanup;
- `native.mcp.*` → OpenCode setup/observation.

A route declares only supported actions. Unsupported behavior fails in Store routing,
not through placeholder adapter handlers.

## Capabilities and hello

Capabilities describe:

```text
supported actions
input delivery: next_turn and/or exact steer
cancel scopes: turn and/or queued_input
native Goal verbs
optional background control
available local activity/result/account-capacity sources
```

Capabilities route commands; they are not permissions. Store checks caller rights,
Task scope and current ownership before adapter delivery.

The descriptor selects launch configuration and required actions. Package version,
build ID and schema inventory remain installation/diagnostic facts. Hello verifies
the authenticated binding, one current protocol major and the required capability
subset; it does not echo and compare the full descriptor.

## One native state owner

Each native session or shared service scope has one adapter state owner. A shared
Codex/OpenCode service connection is not duplicated per hosted session. The common
Eliot link cannot mutate native state.

Queue acceptance, native input admission, physical model attempt, native terminal
and Task completion are separate facts. A socket ACK or HTTP response proves only
the boundary documented by that native method.

## Logical input and native identity

One Eliot Operation represents one logical controller/user input. A harness may
perform several physical provider attempts for that input. Physical attempt IDs,
retry state and current attempt are observations; they do not create new Task
Operations or multiple accepted results.

Use the strongest native logical identity:

| Harness | Native identity/readback |
| --- | --- |
| Muse | durable command ID and session event cursor |
| OpenCode | durable inbox/message ID and session log |
| Codex | `clientUserMessageId`, exact thread/turn IDs and `expectedTurnId` steer guard |
| ACP/ACPX | queue request plus prompt-start/terminal watch; not an idempotent replay key |
| Claude | session/resume identity and official history; a small prepared effect only where readback is insufficient |
| Command batch/Antigravity | one serialized run/input window and captured output |

Transport RPC IDs, PID, connection generation and ACK time are not universal replay
keys. Lost response remains unknown until native readback or explicit disposition.
Never create a replacement process/session merely to retry a prompt.

## Store delivery storage

Native effect state and delivery to Store are different responsibilities.

### Immutable deliveries

Terminal outcomes and result pages are already complete before delivery. Retain one
exact private record per item:

```text
sequence
method = module.outcome | module.result
Operation lane
exact params
binding generation and adapter boot scope
```

Persist before IPC. After Eliot-link reconnect, send the retained params again.
Remove only after that method's exact Store acknowledgment. Do not add a payload hash
merely to replay a private retained JSON value; keep hashes on actual result/artifact
bytes.

### Replaceable observation checkpoint

Observation is latest-value state, not an infinite delivery queue. Retain one pending
checkpoint per source:

```text
source ID and epoch
source sequence/cursor
coverage or gap
latest normalized state
```

A newer unsent checkpoint may replace an older one. Store journals each accepted
checkpoint. Skipped native intermediate events are represented by source coverage.

### Common exclusions

Delivery storage never:

- stores or repeats native input;
- chooses recovery or reconciliation;
- reconstructs a provider session;
- owns Task state;
- mirrors the provider transcript/event stream;
- lets a replacement boot silently adopt predecessor files.

A route-local implementation remains local until a second migrated adapter proves
identical semantics and both old delivery loops are deleted.

## Outcome and result identity

Store retains the original Operation, request, binding and generation. The
authenticated binding-scoped link identifies the adapter.

An outcome therefore returns:

- Operation ID;
- adapter boot/generation needed to reject stale reports;
- disposition;
- optional native logical-input/turn/result IDs;
- bounded action-specific details.

The adapter does not repeat module/artifact/protocol identity, request hash and
Task/source hashes in every receipt. Store validates against its retained Operation.

For a serialized warm stream, a self-contained terminal outcome may include session,
boot, result ordinal, native status and response-content hash. It must not require a
separate observation row containing the same result.

Result pages reference the original logical-input Operation and carry byte range,
total/final markers and content hash.

## Cancellation, recovery and shutdown

Cancellation names one subject:

- controller Operation not yet sent;
- native queued input;
- native active turn;
- owned session/process shutdown.

A closed transport, dropped future or timeout proves none of the native subjects.
Use `Reconcile` or local observation when cancel outcome is uncertain.

The common Eliot shell reconnects IPC and drains retained Store deliveries. The
vendor adapter owns native reconnect, session resume and effect readback. `Recover`
is explicit, supported per adapter, and never resends input.

## Harness Goal and Eliot Goal

Harness Goal is native and controlled by `agent.goal`; it is the default where
supported. Native set/resume may start work, and resulting turns remain associated
with the initiating Operation and assigned Task/Attempt.

Eliot Goal is a Store plugin that sends an ordinary `agent.send` message on an event
or schedule. The Governor may use either mechanism or both.

Each Eliot occurrence has one message Operation. Replaying the same occurrence
returns it; a later occurrence may intentionally repeat the same text. Disabling the
plugin does not pause native Goal, and pausing native Goal does not disable Eliot
messages. Neither mechanism accepts a Task.

## Local state and source cursors

`agent.list`, `agent.state`, `agent.family` and `agent.usage` read Store's local
projection without provider calls.

| Projection | Meaning |
| --- | --- |
| Inventory | Bound and unbound native sessions/children from configured sources. Discovery grants no control. |
| Activity | Native idle/running/waiting/terminal/unknown; pending reason; logical input, physical attempt/turn and last content activity. |
| Process | Adapter/shared-service PID plus OS birth identity, liveness/exit, CPU/RSS and ownership confidence. |
| Workspace/Git | Assigned and actual worktree/branch/HEAD, upstream/push target and last independent remote ref. |
| Provider source | Local JSON/JSONL/SQLite identity, source epoch/cursor, source time and coverage/gap. |
| Run usage | Tokens/cost/context occupancy for one run/session. Cumulative snapshots are not deltas. |
| Account capacity | Current source/account availability, buckets/windows/reset/credits and freshness. Missing evidence is unavailable. |

There are two cursor domains:

1. provider/source cursor, opaque to Core and interpreted by the adapter reader;
2. Store `observation_id`, used by `monitor.snapshot` and `monitor.follow`.

The adapter closes replay/live races through a source watermark or subscribe-before-
replay pattern where available. Cursor expiration, source replacement, truncated
JSONL and WAL gaps become explicit source coverage gaps and bounded rescan. Store
monitor cursor remains independent.

A transcript or alive PID does not settle an uncertain command. Quota updates do not
refresh work activity. Watcher disconnect does not affect native work or owner TTL.

## Account capacity

Adapters fold native quota method names and rolling patches into a normalized current
snapshot:

```text
source key and epoch
monotonic source sequence
observed time and coverage/freshness
actual account/service/connection scope
availability and optional retry/reset
bounded native-labelled buckets/windows/credits
```

Store validates the authenticated source, bounds and sequence. It does not maintain
Codex/Muse native method allowlists, artifact-version allowlists, authentication
digests or provider-specific permission fields in the universal contract.

One account/service snapshot is retained once and referenced by relevant routes. If
the provider supplies no stable account identity, keep it connection-scoped and do
not merge it. Invalid optional quota cannot discard valid activity/result facts.
Unknown is neither zero nor available.

## Results and artifacts

The single path is:

```text
agent.result -> adapter page -> module.result -> artifact.read/assemble -> submission
```

Associate output with an explicit native logical-input ID where present or one
serialized admitted window. A second independent input, changed generation or
unclassified source gap requires readback; never select the newest assistant message
by guess.

Store publishes immutable bytes before committing their receipt. A file without a
committed receipt is not a result. Exact pages stay in delivery storage until ACK.
`artifact.assemble` is the only multi-page assembler.

Result availability does not submit, review or accept a Task.

## Review and acceptance

`review.assign` names one candidate and auditor. `review.submit` stores that auditor's
verdict and requirement/check evidence. `task.request_changes` records manager
disposition; sending repair instructions is separate.

Manual and automatic acceptance use the same Store gate and retained evidence for the
exact candidate and frozen requirements. Caller-authored duplicate review arrays are
removed. Candidate B cannot reuse A's review.

## Optional integrations

Hooks, scripts, GitHub effects and diagnostics call the same Store handlers and use
ordinary Operations. They add neither adapter actions nor another Task authority.
Disabling diagnostics cannot disable required local agent observation.
