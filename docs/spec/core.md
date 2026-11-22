# Core model

`eliot-core` owns pure data/reduction; `eliot-store` owns allocation, SQLite and queries.
Wire names are snake_case; timestamps are UTC Unix milliseconds; stored integers fit
signed 64-bit. This is the target contract: add later fields/projections with their
first roadmap caller, not as empty L1 subsystems.

## Identifiers

| Identity | Meaning |
| --- | --- |
| `AgentId = a<positive i64>` | stable Eliot Agent row |
| `generation` | nonnegative incarnation fence; 0 before first native dispatch |
| `CommandId` | external ULID or bounded derived id |
| `seq` | globally ordered persisted event id; never reuse a committed value |
| `MessageId = m<positive i64>` | concrete mail delivery row |
| `(native_scope,native_ref)` | opaque native thread/session in its store/account/server scope |
| `turn_id`, `request_id` | exact native execution/request within an Agent generation |
| `native_id` | stable normalized native fact identity within its source |

`command.get`, `command.children`, `command.cancel` and `command.resolve` validate
command references with Core's [`validate_command_id`](../../crates/eliot-core/src/command.rs):
nonblank and at most [`MAX_COMMAND_ID_BYTES`](../../crates/eliot-core/src/limits.rs)
bytes, including linked `<id>/send` and selector child IDs. Preserve the exact bytes
without normalization; external mutation IDs remain ULIDs.

One native subject maps to one canonical Agent row. Attach/detach/resume update it;
a native fork has another subject and Agent. Optional names are unique and cannot look
like Agent ids/selectors. Public numeric identities must not be recycled after cleanup.

## Generation fence

First launch/attach targets generation 1. Resume, detach and a new/uncertain control
takeover target current + 1. Exact reconnect to the same live ownership may preserve it.
Every changing command stores expected and target generation.

Before native I/O, one dispatch transaction advances the fence, revokes old plugin/source
identity and token, stales old queued work, releases claims, pauses Goals and commits
target source ownership. When the target supports its own environment, create its one
random token in this transaction, before native tools can run.

Success installs the requested projection. Definite first-start/resume failure is failed;
definite takeover/detach failure is lost from Eliot's perspective because the previous
controller was revoked. After native dispatch, unknown keeps the target fence/token and blocks another
transition and ordinary input until exact readback/resolution. Never roll generation
back or rotate another token merely because a reply disappeared.

Only readback proving no process/session received the environment may clear an uncertain
target token. Exact attachment may carry a live native turn/request into the new
projection; otherwise it remains unknown. Old-generation facts are historical/debug
input, not current reducer input or automation triggers.

## External mutation ids

External mutations use a canonical ULID created before send. Identity binds id to
principal, method and canonical semantic payload; exclude JSON-RPC id, presentation,
paging and wait timeout. Same binding returns its retained result; a reused id with
different binding fails `id_conflict`. Absent ids outside the supported retry window
(default 24 h, shorter than retention) fail `id_expired`.

Commands store this directly; other writes use the API's small `writes` table. One
writer checks both namespaces. A compound write may share its receipt ID with the
command it admits. Agent deletion preserves that receipt and records a tombstone only
for a command ID without a receipt. Derived ids identify linked inputs, selector
children, rule actions and Goal rounds. There is no universal receipt/floor subsystem.

### Memory physical calls

M1 adds stable inference clients and physical calls beside `writes` through the
same writer. The client owns a persistent monotonic signed-64 admission sequence;
rotation does not change ownership/reset it. One admission transaction binds receipt,
sequence and call. Private content stays outside SQLite/events. State is queued,
sending, succeeded, failed, cancelled, unknown or released. The exact
[Core DTOs](../../crates/eliot-core/src/api/inference.rs) own types/bounds. Only a
committed queued-to-sending transition grants one native attempt; unknown is never
replayed. Release fences publication; cleanup completion is separate readback.
Ordinary retention excludes inference writes. Dedicated inference retention advances
monotonic call/write cuts under Memory's safe dependency assertion, pruning bounded
released, cleanup-complete metadata and expired unreferenced write bindings. Inventory
above each cut is complete; crossing a cut fails with `history_gap`. Neither allocator
resets on pruning. The [API](api.md#memory-one-shot-inference-m1-199) owns backup cuts,
restore readback and credentials/release.

## Agent

The [`Agent`](../../crates/eliot-core/src/lib.rs) and
[`AgentSummary`](../../crates/eliot-core/src/api/agent_views.rs) DTOs own the public
Agent field catalog.

Snapshots contain bounded nonsecret values and admitted file references, not resolved
secrets. External Agents are observe-only until exact control attach. Native children
remain visible above controller fanout guards; limited children have no widened control
or independent identity. `own_identity=false` is never owner authority.

`Agent.project` is a descriptive label, not project authority. Project-scoped automation
uses the owner-registered checkout and repository identity read back through Core
[`ProjectRegistrationView`](../../crates/eliot-core/src/api/agent_views.rs); it never
derives authorization from a later Agent-row label.

The optional public `workspace_phase`, `branch`, `base_ref` and `base_oid` are read from
the sole managed workspace row, including when another Agent shares its path. They are
not duplicated Agent columns. No managed row means these projections are absent.

### Process custody identity

The Store's current custody namespace is minted for its stable Store identity and
canonical data-directory path. Opening a backup or moved Store at a different canonical
directory re-mints that namespace before new generations can dispatch. The stable Store
identity (also used by inference clients) does not change. Canonical aliases of the same
directory keep the binding. In-memory Stores retain their own independent binding.

Start dispatch commits the current namespace on the exact Agent generation before any
plugin process is created. Read-only restart checks use that recorded namespace even
after a directory move or copy. Force-stop may terminate that named Job only when the
recorded namespace equals the Store's current `custody_namespace()`. Otherwise it
returns `process_custody_foreign`, leaves the Job untouched and settles unknown without
replaying the command. Agent Jobs are kill-on-close and only their owning daemon holds
custody, so a live Job under an older namespace belongs to another running daemon.

The local Owner gate uses the custody of the Agent generations started by this
daemon. The host registers their Job or Linux process-group identity as part of
spawn and keeps it while it owns the child. Unavailable custody identity fails
startup before plugin initialization, with the usual cleanup and uncertainty
rules. Windows checks exact Job membership.
Linux checks the owned group and observable parent lineage and retains the existing
degraded custody guarantee. Owner checks wait for in-flight custody registration,
then inspect OS membership outside the custody registry lock.
Neither another daemon's custody nor periodic process
samples decide local Owner authority. See [local principals](api.md#transport-and-principal-identity).

No-Agent discovery (catalogue, quota, profile-store and installation/auth probes)
and inference use the same scoped process-custody API. On Windows each helper
family is assigned to a daemon-owned kill-on-close Job before it resumes, with
no breakaway. Native helpers must select private/standalone execution rather
than a shared service that can start outside that Job. A successful Windows read
requires owned family closure; unconfirmed cleanup stays unknown. See
[Windows exact custody](monitoring.md#windows-exact).

The first v52 open always mints a new current namespace, including on the original
production directory: a v51 backup cannot be distinguished from the original. Migration
first records findable existing generations' legacy namespace (the stable Store identity),
preserving their names for read-only restart probing. This re-mint does not alter their
recorded namespace or their live Jobs. A legacy generation without a recorded/findable
name keeps custody unknown.

A Job creation conflict (`process_custody_conflict`), validation, environment or directory
preparation error before OS spawn is a definite failed start with origin `swarm`, its
original typed reason and a plain explanation in `why`. The host carries the no-spawn
proof separately from generic errors; Store checks the exact dispatched start and
generation before failing it, closing unstarted sources and releasing its reservation
in the same transaction. Errors after a child may have been created retain their
ordinary uncertainty until owned process exit and reader drain are proven. A conflicting
existing Job is never adopted or terminated by this failed start.
If Store rejects the no-spawn proof, ordinary plugin-error settlement records unknown
and retains capacity; a concurrent authoritative terminal settlement is preserved.
A successful no-spawn settlement clears that generation's recorded custody name, so it cannot
authorize a later force-stop of the conflicting Job.

### States and reducer

| Input | Result |
| --- | --- |
| first launch/attach admission | created, generation 0 |
| controlled start dispatch | target generation, starting |
| proven refusal before any plugin process is spawned | start command failed, Agent failed; retire unstarted sources and release capacity atomically |
| exact controlled ready snapshot | reported idle/busy/waiting |
| observe attach dispatch and exact identity readback | target generation, unknown until current native state is proven |
| lost start reply without proven endpoint and owner cleanup | starting, start command unknown |
| proven native endpoint exit plus completed owner cleanup | lost, start command remains unknown |
| proven native endpoint exit while controlled, after bounded native tail and plugin owner cleanup | lost unless already terminal; preserve terminal turn evidence, otherwise unresolved turn unknown |
| exact turn start | busy with captured active turn |
| first request / last resolved request | waiting / busy |
| exact active turn terminal result | idle, clear that turn's pending requests |
| exact native terminal of a turn the monitor settled unknown (same live generation and send) | that turn takes the native outcome, `turn.resolved` with `resolution_source=native`; projection unchanged |
| normal session end / fatal error / proven loss | stopped / failed / lost |
| restart of an idle generation with a retained native session, prior owned exit and reader drain proven, no unsettled work | stopped, `agent.stopped {reason: owned_exit_before_restart}` |
| restart of a remaining live generation | lost, open turn and dispatched effects become unknown |
| exact controlled producer generation ends (native observed child) | stopped, `agent.stopped {reason: ended_with_parent}` |

The Owner inbox projects existing Store facts. Each item identifies its source
subject and revision so a later decision
can be checked against the exact generation, turn, hold revision or other
source version displayed. Choices name existing actions and carry their
arguments. Swarm derives those choices from retained facts. Snooze
suppression ends on the next event for that subject.

Core's pure `mover(object)` liveness classification covers every non-terminal
Agent state, command, Goal, hold, launch intent, assignment wait, harness
request and timer. It reports `Moving` with the expected mover and deadline,
`Asked` with the matching Owner inbox item, `Long` when a live item exceeds its
kind-specific state-duration threshold, or `Stuck` with a reason when neither
movement nor an Owner question exists. State handling is exhaustive: matching
has no default arm, so adding a non-terminal state requires defining its mover.
Existing reaction ladders own repair and preserve unknown outcomes.
[Core mover](../../crates/eliot-core/src/liveness.rs)

A controlled loss records its cause on `agent.lost` and, when a turn is open,
on that turn's `turn.unknown`: `root_exited` for a spontaneous plugin root
exit (the drain detail stays on `source.gap`), `daemon_restart` for restart
recovery, `owner_marked_lost` only for an explicit Owner assertion of
loss, and otherwise the settlement path's own cause (reader EOF or failure,
owner-channel closure, daemon force-stop, startup cleanup, or the stop's own
exit; see [Process observation and terminal ordering](monitoring.md#process-observation-and-terminal-ordering)).
Readers accept persisted rows carrying any of these reasons.

With the default [restart policy](configuration.md#agent-restart-policy), the
daemon reopens an eligible retained idle session after restart through a
generation-bound `resume` without text. Eligibility requires the prior
generation's exact custody closure and no unsettled work. The new generation
keeps the same native subject. An interrupted turn or uncertain dispatched
effect stays `unknown` with the restart reason until it can be settled through
the normal recovery path. A terminal `send` refusal identifies the next
explicit action for that Agent.

An open stop/transition is a command beside the observed state, not another state
machine. Generation transition blocks ordinary turn work. A stop in `dispatched`
or `unknown` fences sends, active-turn injection, turn controls, and native
configuration for its generation. Its own interrupt is the only turn control
that can dispatch during the stop. Resume of the terminal generation is fenced by
a `dispatched` stop, and by an `unknown` stop until that generation's owned exit
and reader drain are proven; after that proof Resume may open the next generation
while the stop stays `unknown` and is never reissued. Terminal settlement cancels undispatched work,
revokes token/own identity, releases claims and pauses Goals atomically.

The handoff brief gate preserves withheld successor SENDs across automatic native loss
and restart until Owner resolution or cancellation; explicit lifecycle cancellation
retains its ordinary no-effect behavior (see [Handoff](coordination.md#handoff)).
If a running turn has no terminal evidence, retain it as `unknown` before clearing the
active-turn pointer; do not erase or fabricate its result. Dispatched/unknown commands
remain historical and are never reissued.

A dispatched stop captures the generation's active turn, including when the
caller omits `expected_turn_id`. A graceful stop makes one interrupt attempt
under the stop's authority and binds it to that captured turn. After the
attempt, wait up to five seconds for committed readback that the turn is no
longer running or the generation has ended. A cancellation terminal settles
`interrupted`, while natural completion/failure keeps its actual outcome. A
pre-native refusal skips the wait and proceeds to process custody closure. An
interrupt acknowledgement or timeout supplies no turn result.

Stop is `applied` only after two independent, bounded facts are committed for
the exact generation: its owned process custody is empty and its authoritative
native source is drained. The plugin's stop reply or a native `session.ended`
terminal is retained when observed; either may be absent after process/source
proof, in which case the stop outcome records
`native_session_end_observed=false`. Missing process-exit or source-drain proof
keeps the stop `unknown` with the missing fact named. Stop never retries an
uncertain native call.
An actor that retires control without proof of exit records the Agent as lost
and keeps the custody reservation for reconciliation.

When an Owner-requested stop proves both facts, its captured turn becomes
`interrupted` with `resolution_source=owner` and reason `stopped_by_owner` if no
terminal native result arrived. A SEND dispatched before the stop can supply
its exact turn binding while the Owner stop is in flight. A drained native terminal result, including its
status, outcome, text and usage, remains authoritative. A turn not captured by
that stop, or a non-Owner stop without terminal evidence, stays `unknown`.
Force stop skips the cooperative interrupt window and uses the same process and
source proof requirement.

Diagnosis of a stopped, failed or lost Agent names retained unknown commands and
turns across all generations, including history outside the recent-turn page.
It points to `swarm cmd resolve` or `swarm turn resolve` for an exact Owner
assertion. For historical stops, `swarm cmd settle-stopped` previews one page of
unknown stop commands with committed ownership exit and reader drain. Run
`swarm cmd settle-stopped --apply <review>` to settle that reviewed page as applied,
mark proven current lost generations stopped, and mark any exact, still-unknown
turn named by an Owner stop as interrupted.
Use the same `--after` value on preview and apply when following a page's `next`
cursor. The page also lists candidates awaiting proof. Start a new preview
without `--after` after their process and reader evidence arrives. An effect
whose outcome requires other evidence remains unknown.

Stall is a flag only for busy work with a healthy authoritative source. Waiting on a
request is waiting, not stalled solely through silence. Native progress clears a stall;
CPU/Git/mail/health noise does not. Stale source means progress unknown.

`unknown` is a nonterminal Agent state for observe-only rows. It is independent of
whether the reader is attached: successful identity attachment can leave native status
unknown, and a detached row retains that status. Reader-local `notLoaded` is not native
idle or stopped evidence. Observation failure and restart retain unknown/stale coverage;
they do not take the controlled-generation `agent.lost` transition.

Native observe-only children also expose the read-only `Agent.activity` projection
(`NativeChildActivity`): `running` when a committed child turn is open; `lost` when
that turn remains open but its child, immediate parent or exact producer source is
closed, gapped, unhealthy or superseded; `idle` after the latest terminal turn, or
`interrupted` for an interrupted/cancelled terminal turn; `unknown` without sufficient
turn facts. Partial message facts without a terminal receipt also leave an observed
turn open; source loss/restart and event retention do not silently clear that work.
Open turns take precedence over terminal history. `has_open_turn` is a bounded
existence fact, never a count of unresolved turns. Progress comes only
from committed native turn/message/tool/request facts, never CPU, Git or mail.
The projection is derived from existing turns, events and source/registration rows,
with no new activity table. Retention can leave the progress timestamp unknown.
`ls`, `show`, `top` and MCP reads expose this field next to unchanged lifecycle
`state=unknown`; activity grants neither a principal nor child control verbs.

`unknown` lasts only while the child's source can still be live. A native child ends
with its exact controlled producer generation: when that generation becomes stopped,
failed or lost, or is superseded, every still-`unknown` registered child of it
(including nested ones) becomes `stopped` in the same Store transaction, with
`agent.stopped {reason: "ended_with_parent", producer, producer_generation,
producer_state}` (`superseded` when the producer generation moved on). This is proven
end of observation, not a guess about native completion: the child's grant requires
that live producer generation, so it can never publish again. A turn still `running`
is retained `unknown`, never completed; completed turns and `activity` keep their
facts, and source rows stay with the producer's retirement
(`mark_native_children_unavailable`, which owns the reader-closure proof). Restart
recovery applies the same rule to retained rows and never resets an ended child to
`unknown`. An ended child opens no `children_unattended` incident. Detached external
observers keep their `unknown` state.
For a reusable native child, one terminal turn settles its activity, while the
harness can still send it more work. Only the end of the child session or its
producer grant is a lifecycle terminal.

Observe attachment records `origin=external`, `observe_only=true`, `own_identity=false`
and no token. The generation/source fence commits before reader I/O. Owner-issued native
control commands reject as well. Detach closes only owned reader resources and source
lifecycle; it preserves the canonical native subject, history and unknown native state.
A detached unknown row with closed sources is not a live controlled-Agent drain blocker.
Unresolved transitions and owned reader custody still block clean drain as appropriate.
The same rule covers the command side: a stored `unknown` command with no live
process, turn or reader behind its generation (for example an observer attach
fenced by a later detach) does not block drain; the drain receipt lists it as
retained-unresolved instead, and the stored outcome stays `unknown`.

Retirement follows the same evidence. Archive preserves retained unknown evidence while
delete removes the tree's turns and events without resolving them. An ended native
observed child (`stopped`, grant revoked, custody drained) retires through
`agent.archive` or `agent.delete` with its unknown turn retained as ended-observation
evidence under archive, never resolved or completed; delete removes it with the tree
instead. A detached external observer (`unknown`, no live reader, custody drained)
retires the same way without taking a native lifecycle state.

Live observation (an attached observer with an open source, or a native child whose
producer generation is still live) still blocks both.
Compute `apply(state,input) -> (next_state,effects)` without I/O or mutating published state.

The Store commits event, next projection and derived facts atomically, then publishes.

## Profile

```text
Profile {
  harness, extends?, cmd?, model?, effort?, permissions [workspace-write],
  args [], env {}, pass_env [], secret_env [], inherit_env [false], proxy?,
  workspace_mode [shared], control [self], git_identity [agent],
  system_note?, labels {}, stall_after [5m], max_live?, account?, billing?,
  setup [], scratch ["target/","node_modules/"],
  notify_parent [true], wake_on_mail [true]
}
```

Model/effort are native values. Display requested/effective values separately. The frozen
profile snapshot carries an optional shared Core `RouteBinding` and a vector of
[`CapacityScope` values](../../crates/eliot-core/src/routing.rs); their field catalog
is canonical there. Admission checks every configured profile/account/proxy scope
before controlled dispatch. An
omitted `max_live` is unlimited and zero disables that scope. These are Eliot workload
limits, not vendor entitlement or quota. Reservations survive restart when closure is
uncertain and release only after the owned generation is proven closed. Provider holds
are durable local scope state, separate from capacity caps and vendor entitlement. The
canonical Core types (`ProviderRestrictionKind`, `ProviderRestriction`,
`ProviderHoldState`, `ProviderHold`) in
[`routing.rs`](../../crates/eliot-core/src/routing.rs) define their shared shapes.
A hold is created only from an optional structured restriction on an exact
generation/turn-bound controlled `turn.completed`; generic errors, native-error text,
HTTP status, and vendor catalog metadata are not restriction evidence.
RateLimit and Quota may use the explicit probe path; Auth and ModelRemoved require owner
correction and cannot be cleared by a probe or elapsed time. Native Codex classification
is limited to the three pinned structured codes and four typed 429 variants in
[monitoring](monitoring.md#provider-holds); there is no 404 or message-text model-removal
classification.

`SubmitCommand` and persisted `Command` carry probe intent as a default-false field.
False is omitted from serialization so legacy request bytes and digests remain unchanged.
True is valid only for SEND and is not added to native text or arguments. The Store
persists the intent in the new command column with default 0.

A non-empty `model` field reported by a native `assistant` message in the `message.completed`
event updates only `Agent.effective.model` within that event's handler, after existing
live-turn and generation bindings are verified. All other effective fields, the requested
profile alias, and control state remain unchanged. Missing or null native model stays
unobserved. A reported model change does not create generation, authorization or control
gaps.

## Events and authoritative sources

The [`Event`](../../crates/eliot-core/src/lib.rs),
[`NativeEvent`](../../crates/eliot-core/src/lib.rs),
[`SourceOwner`](../../crates/eliot-core/src/api/agent_views.rs) and
[`SourceState`](../../crates/eliot-core/src/api/agent_views.rs) DTOs own the event
and source field catalog.

`NativeEvent.ts` is optional original native Unix epoch milliseconds. `None` is omitted
from the wire and preserves the legacy semantic digest and ingestion-time `Event.ts`;
`Some(ts)` is included in the native semantic digest and becomes the persisted `Event.ts`.
This row timestamp is distinct from optional lifecycle times in `data`; it is never a
substitute for a missing turn start or completion time.

Private authenticated `source.state` may include a derived `observer_ready` proof;
this field is not part of the public Agent `source_states` projection. An open source
or dispatched attach does not establish readiness.

`subject` is the event-time post-transition projection used for rule matching. Persist
the captured [`Event.subject`](../../crates/eliot-core/src/lib.rs) with the event and
replay it unchanged; a missing subject remains missing. Its canonical type is
[`EventSubject`](../../crates/eliot-core/src/lib.rs), while event identity is carried by
[`Event.agent` and `Event.generation`](../../crates/eliot-core/src/lib.rs). Never look up
a later Agent row to replace the captured subject. Future hierarchy or labels require
real L5 sources and callers before they are added to this projection. The optional
[`Event.cause`](../../crates/eliot-core/src/lib.rs) uses canonical `EventCause`. Store
persists it only from verified command provenance; arbitrary native `data.cause` is
ignored, and historical events are never reconstructed from a later Agent row. Ordinary
proven controller commands start at depth 0. Rule, timer and manual-timer commands use
their own command identity and depth 1 when no triggering cause exists, or one greater
than the verified triggering cause. The existing `commands.cause` storage carries typed
cause JSON for rule parents and children, each bound to that row's own command ID.
An authenticated HTTP bridge may add the bounded nonsecret `RemoteCallContext`
to that existing cause during ordinary command admission. Store commits it with
the command and admission events; retained retries never replace it. It is call
provenance, not an Agent identity or a credential. Native event payloads cannot
supply or override it.

Native families are session/turn/message/tool/request/child/goal/usage/provider/edit.
Exactly one source owns each implemented family per Agent generation. Daemon-owned
Agent/command/mail/rule/Git/process facts and `agent.lost` cannot be supplied as
arbitrary plugin lifecycle. Persist bounded final facts; deltas/samples stay ephemeral.
Mask known secrets in native output; reject known secrets in caller durable text.

`turn.notice` is a non-progress native fact in the turn family. It carries one exact
`turn_id` and a normalized retrying/reconnecting/error kind; the reducer checks the
current generation and live turn and leaves lifecycle state and model progress intact.
It cannot create a provider hold or trigger a retry policy.

An observe-only live source may commit normalized source-owned facts only after settled
`observer_ready` proof and an explicit source handoff. Store rechecks the exact subject,
readiness and current family owner in the transaction. Observed batches cannot carry
`command_id`, `cause` or `request_id`; they populate read-only event/turn views without
entering controlled `apply`, changing the Agent's `unknown` state or identity, or settling
commands/inputs. Private history pages remain read-only and nonpublishing.

Within the request family, `request.resolved` remains bound to an exact owner reply
command. Native cleanup uses `request.closed {request_id,turn_id}` with no `command_id`.
It requires the current generation, the source owning the request family, the exact
live turn and an existing pending row with that Agent/generation/request/turn binding.
It removes only that pending request, deriving waiting while others remain and busy
otherwise. The durable request row becomes `resolved`; this records closure, never
application or acknowledgement of a reply command. Any unknown reply stays unknown.
Request closure neither creates nor resolves a source gap.

A harness may re-open its exact pending id with updated details proven from its
authentic native stream (same turn and kind); the projection and durable row adopt
the current offer. For multi-stage offers the harness normalizes an integer
`offer_seq` in pending details and records the decided integer as
`decided_offer_seq` in each Applied reply outcome. Unknown and inflight replies
always fence; a definite Applied stops fencing only once the persisted current
`offer_seq` is genuinely fresh past every Applied decision. Harnesses without
multi-stage offers omit both fields and keep the single-reply fence.

Lifecycle, health and gap metadata are independent. Handoff changes named family
ownership, not predecessor lifecycle. Cursor-backed recovery resumes strictly after
its opaque committed cursor. No cursor means live-edge recovery plus explicit gap,
not historical republication. The protocol owns exact batch/gap/handoff behavior.

### One-row commit acknowledgement

A source has at most one unacknowledged native-record batch. Daemon computes canonical
digests and commits the complete batch, cursor, projections and last-batch acknowledgement
together. Accept wholly new input or an exact duplicate of the last batch; changed id
payload/cursor or mixed new/duplicate events is `source_integrity`.

The last-batch acknowledgement survives event pruning. It is not a native-effect retry
or general event journal. A source with no stable native identity/cursor cannot safely
retry a lost acknowledgement and must report a gap/reconcile current state instead.

## Verbs

| Verb | Meaning |
| --- | --- |
| launch | Agent/session start; optional prompt is a linked send |
| attach / detach | exact subject control/observation transition, generation-fenced |
| send | input-to-turn association; terminal result is separate |
| steer / interrupt | exact captured active turn, no implicit fallback |
| inject | context at its requested next-input or exact-turn safe point; report the actual application point |
| stop | end the controlled generation/session; not merely stop watching it |
| resume | reopen the retained native subject as a new generation; new text only when explicitly supplied |
| approve / deny / answer | exact pending request |
| configure | report now/next_tool/next_turn/next_session application |
| goal.* | native/Eliot goal, added with automation |
| cancel | withdraw undispatched work |
| compact / fork / background | only supported native mechanisms |

Text is at most 512 KiB and must also fit the encoded plugin frame. Unsupported later
verbs are not advertised. No fake success or compatibility executor.

### Context injection

The canonical `InjectArgs`, `InjectPoint`, `ContextItem` and `ContextOutcome` DTOs are
defined in [Core context types](../../crates/eliot-core/src/context.rs); this spec owns
their command and effect semantics. `inject` uses the existing `SubmitCommand` with
`args` decoded as `InjectArgs`. The point is `next_input`, `turn_safe_point`, or
`active_turn`, and `expires_at_ms` is optional.

Store binds each admitted context command to the target's current generation. A
`turn_safe_point` or `active_turn` command also captures the exact active turn at
admission; it cannot float to another turn or fall back to a send. `next_input` has
no turn binding and is eligible only for the next send in that same generation.
Queued pending contexts (`next_input`, `turn_safe_point`) are claimed FIFO from
existing command rows; they do not add a table or journal. An `active_turn`
command is dispatched directly through the ordinary router: one command is one
immediate native effect with no queue consumer. Admission or queue acceptance is
not application.

For `next_input`, the router claims eligible, unexpired items as part of dispatching the
next ordinary SEND and the adapter prepends them inside that same native send. An item that
is unfit even alone is rejected with `frame_too_large` so it cannot wedge the queue; anything
that fits alone but not with the current batch ends the entire scan and stays pending for a
later send, preserving FIFO order. The mandatory `turn.started` receipt holds one outcome per
claimed item in the exact shape the adapters persist, with fields fixed only at native
dispatch worst-cased by their enforced bounds, so a dispatch never carries text the Store
could not associate. Context
commands become applied only when that original send is associated with its exact native
turn. For `turn_safe_point`, an authenticated native hook claims eligible items for the
captured turn; application is reported only at the requested native point. For
`active_turn`, the command itself dispatches through the ordinary router to the
plugin's `agent.command` inject, which applies the text through the adapter's actual
native turn/steer on the captured exact turn (not a hook callback and never labeled
`turn_safe_point`). The exact-turn ack carries the retained typed outcome
(`codex.turn/steer` or `muse.turn/steer`, application point `active_turn`, consumer
the original inject command id), and the command settles with the standard
Applied/Rejected/Unknown readback. The retained
typed outcome identifies the mechanism, actual application point and opaque consumer;
it is not a second receipt. A missing native mechanism returns `unsupported`, never a
weaker SEND.

Before dispatch, an expired item or one whose generation or captured turn is stale is
cancelled. Once claimed/dispatched, an uncertain native effect is `unknown` and is never
requeued. A late hook response is discarded rather than carried into a later callback or
turn.

## Commands and narrow gates

The [`Command`](../../crates/eliot-core/src/lib.rs) DTO owns the command field catalog.
state = accepted -> [queued ->] dispatched -> applied|rejected|failed|unknown
        accepted|queued -> cancelled
```

The pure `command_transition_allowed(from,to) -> bool` predicate governs ordinary
command transitions. `classify_dispatched_error(code:&str) -> CommandState` returns
`unknown` by default; only the explicit `native_not_dispatched` code proves no native I/O
and returns `rejected`.
Pre-dispatch `busy` rejections retain their redacted error message with the reason;
plugin errors after dispatch retain code and message without changing this classification.
Generic transport, Store or RPC errors never become `failed`. A separately validated,
authoritative plugin result state follows its explicit result path, not this error-code
classifier.

The separate pure `handoff_brief_resolution_allowed` predicate permits an Owner
assertion of `failed -> applied` only after Store proves the exact handoff brief.
Ordinary settlement does not acquire this edge; no native receipt or replay results.

Workspace preparation is the narrow pre-native exception: an exact command-bound
workspace marker permits `accepted|queued -> failed|unknown` without advancing the
Agent generation or creating a native token/source. The general settlement API does
not acquire this permission. For first launch, preparation failure is `failed` at
generation 0; uncertainty retains `created` at generation 0 with an unknown command.
Pre-native resume failure/uncertainty preserves the prior terminal Agent projection. For a
retained managed worktree, a known no-native-effect rejection, failure, cancellation or
withdrawal restores the exact prior `ready` marker at generation `N`, including its completed
setup index and retained identity, only after Store proves this exact undispatched Resume owns
the `N+1` marker and the retained workspace/preparation binding is unchanged. This restores
Ready metadata only; Store does not assert filesystem contents are unchanged. A later Resume
still requires ordinary exact Git/worktree identity readback before native dispatch. Do not
remove or recreate the worktree or replay setup. A dispatched/unknown Resume or unconfirmed
custody/cleanup remains unresolved and fenced until exact readback; do not restore readiness or
replay Resume.
After Store proves the exact preparation marker, generation and verb, it uses the
separate pure `workspace_preparation_transition_allowed(from,to) -> bool` predicate for
those existing transitions. This does not widen ordinary command settlement.

Preparation binds the start command, target generation, repository/common-directory
identity, worktree path, branch, base OID and immutable setup entries. Each setup
program and explicit file argument carries its original canonical owner-side path,
size and SHA-256. Literal and file arguments are distinct tagged values. These
references are not implicitly copied or redirected into the prepared checkout.
Store checks shape/bounds; the daemon verifies the frozen files before execution.

Native dispatch requires the exact ready preparation and command, with target
generation equal to current + 1, in the same transaction that advances the fence.
Unknown creation accepts exact fully initialized worktree readback and may return the
still-undispatched command to accepted. Absent/partial/conflicting readback stays
unknown. This never authorizes repeating `worktree add`. A creation whose branch or
worktree registration already exists while its marker proves no Git write was reserved
is `failed` (`workspace_target_exists`), not unknown. Unknown setup retains its
index and cannot authorize another spawn. Restart leaves setup-ready/completed entries
intact; a resumed ready worktree retains its completed setup index and only revalidates
its retained identity before native dispatch.

A pre-native stop is bound to an admitted stop command as well as the start command
and target generation. Its dispatch latches cancellation before custody cleanup and
blocks setup/native dispatch. Exact stopped custody settles that stop applied, the
start cancelled, and first-launch Agent stopped at generation 0. Uncertain cleanup
settles both commands unknown and keeps the first-launch Agent created at generation
0. Resume cancellation preserves the prior terminal projection. No native request
answer is treated as permission to acknowledge this cleanup.

`worktree.remove` is a normal command for a terminal owner, including failed/stopped
generation-0 Agents. Its dispatch also verifies that no other nonterminal Agent
references the path, then commits removing before Git I/O. Managed-path launch/resume
admission and dispatch require ready, so concurrent use cannot pass this marker.
Restart/lost removal results become unknown and permit exact readback only, never a
second removal effect. Exact absence plus retained branch settles removed; the row
remains to prevent resume/shared-path fallback into a deleted workspace. No branch is
implicitly deleted. Native Agent state/generation are unchanged by removal.
An intact checkout refused before destructive effects restores the validated prior
preparation phase and setup index while failing only the removal command. Partial,
uncertain or cleanup-unconfirmed removal remains unknown; it cannot claim this
no-effect restoration.

Launch/resume text creates a linked `<id>/send`. The daemon commits both commands
together before acknowledging `command.submit`. An applied start records native
readiness. The linked input dispatches as a generation-bound `SEND` with its own
outcome. An uncertain native effect stays `unknown` pending resolution. See
[coordination](coordination.md#claims-and-hierarchy) for the authority over child
launches and their first input.

Selector expansion creates a Store-only parent plus deterministically ordered
concrete children; parent applied means expansion,
not children finished. Later automation uses the same parent/ordinal representation
with `link_kind=rule_action`; only its ordered actions wait for the preceding action.
Selector children remain independent. No second run/action journal is introduced.

Queued future sends are FIFO but do not block current-turn control/request replies.
An unresolved dispatched/unknown input without exact turn association blocks later
send/configure/Goal continuation. Active/unknown turn blocks the next turn; unknown
transition blocks another takeover. A dispatched/unknown configure holds later configure,
new input and native/Eliot Goal start/resume until exact settings/outcome readback or
explicit resolution. These gates are predicates over command/turn rows, not separate
queues or tables. Stop/interrupt/readback and valid exact request replies remain available.

`rejected` proves no native I/O. `failed` means an authoritative unsuccessful completed
attempt and may include partial side effects; it is not rollback or permission to retry.
`unknown` means outcome cannot be established. An exact negative readback may explicitly
prove no effect; generic errors/timeouts may not. Restart makes dispatched work unknown;
only still-current undispatched work may continue. No external effect is automatically
re-executed merely because it failed or became unknown.

`Error` keeps its caller-facing `{code,message}` and may carry an optional bounded
`diagnostics` list. Each `StderrDiagnostic` identifies `plugin` or `harness` and contains
a `StderrSnapshot {text,truncated,eof,read_error}`. The list has at most one entry per
source and at most two entries total; each redacted text tail is at most 16 KiB. The
encoded diagnostics array is capped at 16 KiB aggregate, including escaping. Error
serialization uses only the remaining space under its 64 KiB encoded bound after the
primary code/message. These fields are non-authoritative and never change command/source
settlement.

`SecretPolicy` is the canonical literal-secret match/redaction policy for diagnostics and
credential-bearing values. It ignores empty values, deduplicates and matches longer known
secrets first, including short secrets; call sites do not set their own minimum length.
Streaming redaction withholds a possible secret prefix across read chunks. Apply the same
policy before stderr bytes enter a retained tail or log; never persist credentials in
semantic event data. `contains_value` checks object keys and string values;
`redact_values` changes string values but never rewrites keys or immutable identifiers.
`logging::record(&SecretPolicy,code:&str,ids:&[(&str,&str)],error:Option<&Error>)->()`
receives bounded identifiers and an optional `Error`, but records only the code, those
identifiers and structured sanitized tails, applying that policy to the code and ID
values. It does not log `Error.message`, request data or environment values. Failure to
record a diagnostic cannot alter the external-effect result.

### Failure location and origin

Every settled failure carries a typed location and an origin. `where` is a stable
dotted `component.phase[.detail]` path (`store.dispatch.workspace_gate`,
`plugin.opencode.start.auth_transfer`, `provider.request`): lowercase
alphanumerics, dots and underscores, anything else folds to `_`, empty folds to
`unknown`. `origin` names the layer at fault, never the messenger:
`swarm` (our core, bus and plugin implementation), `harness` (the native harness), `harness_contract_mismatch`
(the harness behaved differently from its documented protocol), `provider` (the
model service), `environment` (OS, disk, network, credentials outside every
adapter), or `unknown` with an evidence note saying what is missing. Structured
signals only: a provider error classifies from status and machine-readable error
codes, never from message text. Exit codes ride alongside (`exit_code`, plus an
`ntstatus` hex string for negative process codes such as `0xC0000005`).

| Evidence | Origin |
|---|---|
| Swarm validation, schema, queue or implementation failure | `swarm` |
| Loss of a live generation during daemon restart | `swarm` at `daemon.restart` |
| Swarm observer, monitoring attachment or pipe setup failure before the plugin handshake | `swarm` at the failing `swarm.start.*` phase |
| Owned plugin process loss during runtime or cleanup after attempting the plugin handshake | `plugin.exit`, with the producer's origin or `harness` by default |
| Missing executable; OS, disk, network or external credential failure | `environment` |
| Native harness refusal, including an unavailable model without upstream evidence | `harness` |
| Structured upstream rate, quota, auth or model error | `provider` |
| Observed contradiction of a documented harness protocol | `harness_contract_mismatch`, with `docs/harnesses/<h>.md#section` |
| Insufficient evidence | `unknown`, stating the missing evidence |

The restart's `agent.lost` and `turn.unknown` events carry `origin: swarm` and
`where: daemon.restart` beside `reason: daemon_restart`. A spontaneous plugin
root exit, reader EOF without a session terminal, or loss of the owned plugin
generation after attempting the handshake carries `where: plugin.exit`. The
producer's origin is retained when supplied, with `harness` as the default.
Handshake cleanup may terminate the owned process itself after an error.
Closure of the daemon's own control channel alone does not establish plugin
failure. A retained loss without attribution uses its typed reason code
([`swarm_loss_location`](../../crates/eliot-core/src/lib.rs)).

Redact complete messages and evidence before imposing their persistence/wire bounds.
Constructors retain complete text until the owning policy is available. No minimum
secret or truncated-prefix length is a confidentiality boundary. A failed-start
conversion preserves the typed origin, location and evidence; proof of no effect
does not imply a Swarm origin. A plugin panic is diagnostic evidence only from the
exact owned process pipe of that spawn: the daemon writes a per-spawn nonce as the
plugin's first stdin line (never argv or environment), and only a stderr line
`eliot_crash <nonce> <owned PID> <marker>` counts. Plugins write no data-directory
crash files and receive no data-directory path in their environment; the daemon
records each verified marker in its own crash directory, which its entry point
sets explicitly, for `daemon.status`. Its
summary augments the recorded evidence while preserving the original command error,
and when matched on a plugin exit or startup failure classifies `origin: swarm`
(using `where: plugin.exit` when no transport location applies).

Native mappings are versioned at their owning adapters: Codex 0.167.1
`CodexErrorInfo` upstream variants/statuses; Claude stream-json structured
`assistant.error` ([vendor enum](https://code.claude.com/docs/en/agent-sdk/typescript#sdkassistantmessage));
OpenCode 2.2.7 `Session.StructuredError` provider types/statuses from its installed
`/openapi.json`; Muse 1.4.0-R4161.1 `authRequired` from its exported MSP schema.
AGY 1.5.9 ACP exposes no documented structured provider error, so native refusals
remain `harness` rather than guessing from model text. Verified 2026-11-16.

Non-success turn outcomes link their cause: `Turn.preceded_by` holds the command
whose terminal event settled the turn, `None` for success, running turns and rows
settled before predecessors were recorded. The store schema migrates forward;
legacy rows read back as `None`, never as a guessed predecessor.

### Causal chains (`diagnose`)

`diagnose` and `swarm why` assemble the existing records above into a bounded
response (at most 16 steps, one 512-character summary, per-field byte caps);
they keep no store of their own. The chain is command outcome, related turn
outcomes with `preceded_by`, process exits with codes, the last stderr lines,
and the previous-run crash marker, included only when the terminal failure has
no classified origin. The summary always starts with the failure origin.
Agent diagnosis joins the exact generation's launch/resume even when no turn
exists, and the failed turn's input and `preceded_by` commands. Native terminal
diagnostics come from that exact retained turn event; missing retained links are
left missing, never inferred by timestamps.
Admission preflight can refuse before command admission with a typed location
and origin: a reserved capacity bound (`swarm` at `store.admission.capacity`) or
a missing explicit profile executable (`environment` at
`router.admission.profile_cmd`). Only an error built by that preflight
constructor carries the private in-process refusal marker; a later error with
the same code, location and origin is never recorded. `diagnose` can explain that exact request ID
from a `command.refused` operational record while it remains in the bounded
rotated log. This admission step carries structured
code/location/origin/evidence, no free-text error message. It is optional
diagnostic evidence and creates neither command state nor replay authority;
rotation or log loss leaves the unadmitted ID `not_found`.

## Storage

Use bundled SQLite (built with `SQLITE_DEFAULT_MEMSTATUS=0`, set in `.cargo/config.toml`: its process-wide memory-statistics mutex otherwise serializes every allocation), WAL, synchronous FULL, one writer and bounded read queries. An
uncertain commit outcome blocks new writes/dispatch and never repeats the external
cause. Agent deletion also fences the writer if rollback cannot be verified. The SQL below
is a compact view of current table shapes, not a migration script or a requirement to
create unused subsystems. Exact schema evolution lives in numbered migrations.

Already queued `var.set` writes may share one bounded Store transaction (see
`MAX_VAR_WRITE_GROUP` in Store coordination). They execute in queue order with
individual authority/generation, idempotency and CAS checks. A domain rejection
rolls back only that request's savepoint; a persistence failure rolls back the
new writes together and faults the writer. Publish ordered events and acknowledge
each successful request only after the shared FULL-WAL commit. Exact retained
retries remain readable after a writer fault. Grouping adds no wait timer and
never groups native effects or changes source-batch cursor/ACK boundaries.
The bounded queue has one owned consumer. Shutdown closes its admission and joins
all admitted writes before replying or releasing daemon ownership. Queued Agent
writes recheck current generation at the Store boundary; new effects also require
nonterminal state. A retained exact retry may still return its earlier result after
a same-generation stop. An unexpected consumer exit closes admission and reports
undelivered outcomes as unknown, never replaying them.

Public numeric ids use `INTEGER PRIMARY KEY AUTOINCREMENT`. Ordinary `INTEGER PRIMARY
KEY` can reuse a deleted high rowid and break replay cursors. Never reset sqlite_sequence
or explicitly reuse old ids; overflow fails. This uses SQLite's existing allocator, not
a custom identity service ([SQLite documentation](https://sqlite.org/autoinc.html)).

```sql
CREATE TABLE agents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  generation INTEGER NOT NULL DEFAULT 0,
  name TEXT UNIQUE, project TEXT NOT NULL,
  parent INTEGER REFERENCES agents(id), root INTEGER NOT NULL, depth INTEGER NOT NULL,
  origin TEXT NOT NULL, harness TEXT NOT NULL, profile TEXT NOT NULL,
  profile_snapshot TEXT NOT NULL,
  native_scope TEXT, native_ref TEXT,
  workspace TEXT, workspace_phase TEXT, branch TEXT, base_ref TEXT, base_oid TEXT,
  state TEXT NOT NULL, active_turn TEXT, stalled_since INTEGER, last_progress_at INTEGER,
  pending TEXT NOT NULL DEFAULT '[]', goal TEXT, effective TEXT,
  labels TEXT NOT NULL DEFAULT '{}', control TEXT NOT NULL DEFAULT 'self',
  observe_only INTEGER NOT NULL DEFAULT 0, own_identity INTEGER NOT NULL DEFAULT 0,
  token TEXT UNIQUE, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX agents_native ON agents(native_scope,native_ref)
  WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL, kind TEXT NOT NULL, family TEXT,
  agent INTEGER, generation INTEGER, source TEXT NOT NULL,
  native_id TEXT, cursor TEXT, payload_digest BLOB,
  cause TEXT, subject TEXT, data TEXT NOT NULL,
  CHECK(payload_digest IS NULL OR length(payload_digest)=32)
);
CREATE INDEX events_agent ON events(agent,seq);
CREATE INDEX events_kind ON events(kind,seq);
CREATE UNIQUE INDEX events_native ON events(agent,generation,source,native_id)
  WHERE native_id IS NOT NULL;
CREATE TABLE source_owners (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL,
  family TEXT NOT NULL, source TEXT NOT NULL, mechanism TEXT NOT NULL,
  PRIMARY KEY(agent,generation,family)
);
CREATE TABLE source_state (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL, source TEXT NOT NULL,
  cursor TEXT, last_store_seq INTEGER,
  last_batch_id TEXT, last_batch_digest BLOB,
  last_batch_first_seq INTEGER, last_batch_last_seq INTEGER,
  state TEXT NOT NULL, health TEXT NOT NULL,
  gap_id TEXT, gap_digest BLOB, gap_from TEXT, gap_to TEXT, gap_seq INTEGER,
  last_handoff_digest BLOB, last_handoff TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,source),
  CHECK(state IN ('open','closed')),
  CHECK(health IN ('ok','slow','stale','error')),
  CHECK(last_batch_digest IS NULL OR length(last_batch_digest)=32),
  CHECK(gap_digest IS NULL OR length(gap_digest)=32),
  CHECK((last_handoff IS NULL AND last_handoff_digest IS NULL)
        OR (last_handoff IS NOT NULL AND last_handoff_digest IS NOT NULL
            AND length(last_handoff_digest)=32))
);
CREATE TABLE commands (
  id TEXT PRIMARY KEY,
  principal TEXT NOT NULL, method TEXT NOT NULL, payload_digest BLOB NOT NULL,
  target INTEGER, selector TEXT, expected_generation INTEGER, generation INTEGER,
  parent_command TEXT REFERENCES commands(id), link_kind TEXT, ordinal INTEGER,
  turn_id TEXT, request_id TEXT,
  verb TEXT NOT NULL, args TEXT NOT NULL, origin TEXT NOT NULL, cause TEXT,
  state TEXT NOT NULL, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  CHECK((parent_command IS NULL AND link_kind IS NULL AND ordinal IS NULL) OR
        (parent_command IS NOT NULL AND link_kind IS NOT NULL
         AND ordinal IS NOT NULL AND ordinal >= 0))
);
CREATE INDEX commands_target ON commands(target,created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');
CREATE UNIQUE INDEX commands_children ON commands(parent_command,ordinal)
  WHERE parent_command IS NOT NULL;
CREATE TABLE turns (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL, turn_id TEXT NOT NULL,
  start_command_id TEXT,
  state TEXT NOT NULL, status TEXT, outcome TEXT, native_status TEXT,
  resolution_source TEXT,
  final_text TEXT, usage TEXT, started_at INTEGER, completed_at INTEGER,
  PRIMARY KEY(agent,generation,turn_id)
);
CREATE TABLE session_usage (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL,
  snapshot TEXT NOT NULL, observed_at INTEGER NOT NULL CHECK(observed_at >= 0),
  PRIMARY KEY(agent,generation)
);

`turns.start_command_id` retains the exact SEND command that started a controlled
turn. It has no foreign key to `commands`, so command retention cannot erase the
binding. Store writes it with the turn projection; migration backfills only a
unique, consistent retained event or command fact. Missing or conflicting old
evidence stays unbound, and event retention does not pin `turn.started` for this link.

`turns.outcome` is the canonical task outcome (`success` | `failure` |
`interrupted` | `unknown`) decided at the plugin boundary from the adapter's
native terminal vocabulary; `turns.native_status` preserves the raw native
reason next to it. Lifecycle (`state`) and outcome stay separate facts. Rows
written before canonical outcomes existed carry no stored outcome and read
back through explicit harness-aware legacy classification: Claude rows carry
the raw terminal reason (`success` to success, `cancelled`/`interrupted` to
interrupted, `error`/`failed`/`failure` and the `api_error*` family to
failure, anything else to unknown); every other harness carries the
normalized lifecycle status (`completed` to success, `failed`/`error` to
failure, `interrupted`/`cancelled` to interrupted, anything else to unknown),
never defaulting to success. No stored outcome is backfilled: NULL-outcome
rows keep classifying at read time.
CREATE TABLE pending_requests (
  agent INTEGER NOT NULL REFERENCES agents(id), generation INTEGER NOT NULL,
  request_id TEXT NOT NULL, turn_id TEXT NOT NULL, kind TEXT NOT NULL,
  details TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','resolved','stale')),
  PRIMARY KEY(agent,generation,request_id)
);
CREATE INDEX pending_requests_open
  ON pending_requests(agent,generation,turn_id,request_id) WHERE state='pending';
CREATE TABLE git_workspaces (
  workspace TEXT PRIMARY KEY, head TEXT, updated_at INTEGER NOT NULL
);
```

`turns.usage` is the nullable serialized `TurnUsage` attributed at terminal time from
`turn.completed` `data.usage`: `scope` names the native attribution (`message` for one
assistant message with its `message_id`, `main_loop_turn` for one turn's main-loop-only
usage with `message_id` null, `turn` for one turn's aggregate or per-turn-delta
usage with `message_id` null). Store retains the adapter counters and projects
them on reads using one accounting basis, including for retained history.
`input_tokens` is uncached input, including cache creation. `cache_read_tokens`
is cached input. `output_tokens` includes reasoning, and `reasoning_tokens`
names that part of output when supplied. `cache_write_tokens` describes the
portion written to cache. `total_tokens` is uncached input + cached input +
output when all three are proven. Reported subtotals carry explicit coverage.
When a harness includes cache hits in its input total or reports cache
creation separately, the corresponding cache counter establishes the input
adjustment. With a missing split, the observed input count stays available
and the cache counter and complete total stay unavailable. This also applies
to retained history.
Context occupancy and cumulative cost belong to session facts. A terminal
without native turn counters or a first-call measurement keeps null usage.

`first_call_input_tokens` counts the first model response's input, including
cache hits and cache creation once on the normalized basis. Codex uses the first
response-bearing token update bound to the turn. The shared runtime keeps a bounded
set of early samples by native turn ID until the exact input is bound. Claude uses
the first native main-thread assistant response. OpenCode uses the earliest assistant
message in the exact user-to-idle slice. Missing counters, including OpenCode's
cache-write input, leave the first-call value unavailable.
Session snapshots and whole-turn usage supply their own counters.

`session_usage` retains only the latest serialized `SessionUsage` snapshot
per Agent generation, committed from `session.usage` `data.usage` (`source`
names the adapter and native signal, `observed_at` is native epoch
milliseconds defaulted at the owning boundary). A strictly newer snapshot
replaces the retained one; an older or equal-timestamp snapshot or a
byte-identical duplicate is ignored, so replays and out-of-order deliveries
can never inflate or regress the readback. `context_used_tokens` carries an actual
per-call context count and may be retained without a known capacity;
`context_window_tokens` is optional. Shared usage logic reuses an observed window
when the adapter binds the reading to the same model or native turn. Ratio-based
occupancy is available only for a positive window with a value between zero and
that window; an absolute profile ceiling can compare a valid used count without it.
`cumulative_tokens` projects input/output/cache counters on the same
accounting basis as turns, and `cumulative_cost` carries the
verbatim native amount with its verbatim native unit (for example
`usd_estimate` for an estimated USD total), never converted and never
assumed to be money. There is no history table and no rollup: only the
latest snapshot reads back, through `agent.get`/`agent.stats`
`session_usage` in its own session-scope section.

`usage.summary` is the one bounded totals read over one scope: one Agent
(all retained generations, or one named generation, with observed native
descendants registered under those generations, across each descendant's
retained generations), one project label, or one tree (a root Agent and its
descendants). Scope expansion respects the caller's read authority, and each
included (Agent, generation) pair counts once. Subtotals carry explicit coverage. Turn
expenditure sums each `TurnUsage` counter over retained terminal turns that
supply it, grouped by native scope (`turn`, `main_loop_turn`, `message`,
`unknown`): scopes never merge into a cross-scope total. The response carries
`turns_counted`, `turns_without_usage`, and per-counter `turns_missing`
gaps, plus the retained non-terminal turns the sums skip
(`turns_unknown_excluded`, `turns_running_excluded`): an unknown turn may
have spent tokens the readback cannot see. Session facts contribute per-Agent
resume-chain deltas: generations of one Agent continue the same native subject, so
each snapshot adds its signed advance past the previous retained cumulative for
the same normalized provider namespace (the snapshot source up to any event
suffix, so Muse `session/tokenUsage` and `session/contextUsage` share one
baseline; a first observation for a session counts from zero, an advance adds
its delta, and a lower observation in the same session is a correction that
retracts the over-count by moving the baseline down, contributing no new usage;
only a genuinely new native subject starts a fresh baseline; coverage still
reads back where a snapshot is missing), group cost by verbatim unit without
converting, and never sum context occupancy.
`history_complete` is false with its reason when the retained-event
watermark (`rule_history_coverage.pruned_through`) shows history was pruned;
scope expansion stops at the hierarchy fanout bound with `truncated` set and
the Agent count included. No new aggregate table backs this read: it scans
the retained `turns` and `session_usage` rows on their existing indexes.
`agents.pending` is the serialized
pending-request projection; `pending_requests` stores
indexed request lifecycle rows. These are separate current Store structures, not
interchangeable schemas. `git_workspaces` stores the workspace Git-head cache. The
`last_handoff_digest`/`last_handoff` pair retains only the latest canonical handoff
digest and serialized `SourceHandoffAck` on the predecessor source row. Store transactions
validate complete batch, gap and handoff bindings; SQLite checks digest lengths and the
paired-null handoff columns, but not every semantic binding. This handoff readback is
available through the authenticated private `source.state` request only; public Agent
`source_states` owner rows and their DTO do not expose the ACK or digest.

## Retention

The [physical retention increment](../roadmap.md#l7--qualification-repair-and-release)
is implemented by daemon maintenance through the existing Store writer. The following
is its contract; bounded read pages alone do not bound stored history.

Cleanup uses bounded batches through the existing Store writer. Delete only an expired,
contiguous event prefix below every enabled current/retiring rule cursor. Rules
traverse the global event log; their cursors protect unconsumed history until
advancement or the ordinary backlog skip/pause transition. Per-Agent sources
(including Git and stream) protect only their own Agent/generation's history,
never another Agent's rows. An open current-generation Git source with an
established cursor protects its unconsumed native file-change hints after that
cursor. A source with no established cursor holds no cursor pin: failed probes
and non-Git workspaces cannot pin history. Prefix pruning stops before the first
protected row; it does not treat a per-Agent cursor as a global history floor.
Only the latest batch of a source can be re-delivered
idempotently: a new batch replaces only its last-batch acknowledgement, and an
older batch is rejected with `source_integrity`. So for an open source, only
events with `seq >= source_state.last_batch_first_seq` (and anything after that)
are pinned; older native events of an open source may be pruned, and one old
native event of a long-lived source never blocks pruning. An open source with
no committed batch yet (`last_batch_first_seq IS NULL`) pins from its first
native event. Native-bound events of closed sources may go. Retained gap
events are pinned while live: pruning keeps every event at or after the
oldest `gap_seq` of an open current-generation source whose `gap_from` still
equals its cursor, so the Git coverage check can still re-read its gap event
afterwards; a superseded gap pins nothing. A lagging rule
releases its pin only through the ordinary atomic backlog skip/pause transition. Advance
`rule_history_coverage.pruned_through` in the same transaction as deletion, so replay and
explicit catch-up return `history_gap` instead of silently omitting events. Never reset
the event id allocator or reconstruct old subjects from current Agent state.

Current projections and source recovery state outlive ordinary event retention. A new
batch replaces only its last-batch acknowledgement. Retained event/native-id history is
not an unlimited replay permit: cursor/no-cursor recovery obeys the protocol.
Do not discard native deduplication bindings while an accepted replay/recovery path can
still refer to them. Preserve each source's latest committed batch ACK/digest, cursor,
gap and handoff ACK even when ordinary event rows expire.

Retain parent/child command sets while required by open work or the supported history
window; prune the set transactionally, children before parent, so callers never see a
partly pruned expansion. Keep any set containing accepted, queued, dispatched or unknown
work. Each pass resumes a persistent keyset cursor past the last examined expired
root (wrapping only once the eligible set is exhausted) instead of restarting at
the oldest root, and examines at most a bounded number of candidates per pass;
each candidate walks one bounded breadth-first subtree over the
`commands_prune_roots` index with a per-parent limit, skipping kept or oversize
sets in place, so no pass revisits every kept root, rewinds the root index, or
materializes an oversize tree into an automatic index before the outer limit.
A pass deletes at most 128 commands and 128 writes. A command set larger than
128 is kept intact. Retain command bindings/results in `commands` and other mutation bindings/results
in `writes` for at least the supported retry window plus the allowed clock-skew
margin. Cleanup must not make an accepted id look new or replay an external effect.
An unresolved Running Goal-round reservation remains pinned in `writes` beyond
the age cutoff: the existing round commit/error paths still need that row to
finalize and release their marker after proven cleanup. The owning stored DTO is
in [Store Goals](../../crates/eliot-store/src/goals.rs). Exclude these pins before
selecting the bounded deletion page, so eligible expired replies behind them can
still be pruned. Finished/Failed round replies use the ordinary age/retry floors;
an unproven-custody marker independently retains single-flight even if its expired
Failed reply is pruned. This does not extend retention of completed replies or
change absent-id age validation.
Once a complete expansion is gone, `command.children` returns `history_gap`.

Prune delivered completion digests under the commands window: one bounded
batch per pass of rows with `delivered_at` older than the `commands_days`
window, oldest delivery first. Never prune a pending digest (`delivered_at
IS NULL`), however old: an undelivered digest is still live coordination.
A re-registration of a pruned id reads `id_expired` through the write-id age
rule, never a new row. An unset `commands_days` disables digest pruning.

Prune acknowledged mail only: one bounded batch per pass of rows with
`read_at` older than the `mail_days` window, oldest acknowledgement first.
Never prune unread mail, however old. Mail rows always outlive the retry
window plus twice the allowed clock-skew margin regardless of `mail_days`,
so a removed delivery admitted with a skew-bounded write id reads
`history_gap` through the shared absent-id age rule on its own mailbox's
read paths; a retried ack over it succeeds as already acked. An unset
`mail_days` disables mail pruning.

Prune exited child process rows using the independent `processes_seconds` window.
Each pass deletes a bounded batch of `processes` rows with `state = 'exited'`,
`role = 'child'` and `updated_at` older than the cutoff. Plugin and harness rows,
running or unknown rows, and rows needed by retained generation custody stay available
to the owned-exit proof and restart recovery. Historical child process events expire
through the ordinary event prefix; the observer suppresses new child start and exit
events.

The daemon releases the Store writer between retention batches. An event pass
deletes up to 512 entries from the eligible prefix. Each maintenance epoch
checkpoints the WAL outside the Store writer lock, even when no rows expire.
A passive checkpoint that copies all frames is followed by a truncate
attempt when the WAL exceeds 4 MiB. Both checkpoints use a zero busy timeout.
If a reader or writer keeps the WAL in use, maintenance reports the busy
checkpoint and retries on a later epoch. New Stores use
incremental auto-vacuum to return up to 32 free pages per pass. Existing Stores with
auto-vacuum disabled reuse freed pages during live retention. An explicit [startup
conversion](configuration.md#daemon-and-automation-settings) rebuilds an older file before
serving and allows future incremental reclamation.

## Explicit Agent tree presentation and removal

`archived` is a durable presentation flag independent of lifecycle state; archiving
never stops a generation. Archive/unarchive requires idle, ended (stopped, failed or
lost), an external observer in `unknown` state, or an ended native observation, with
no active turn, pending request, or open command. Unknown turns and commands can
remain in an archived ended controlled Agent's history when all their exact
generations have committed ownership exit and reader drain. Archive never hides custody the owner
must still resolve (an unexited owned process, `unknown` artifact or open native
source): archiving an ended member or external observer in `unknown` state requires
its custody resolved. The default list still shows an archived ended Agent while
its custody is unresolved. Eliot's own `os`, `git` and `workspace_process` observers
of a resumable Agent may stay open during archive. Delete's drain waits for them.
An idle member keeps its live custody. Unarchive skips the custody check.
A refusal names the Agent and its first blocker. Reattaching an archived
observer opens a new `observe` source and clears the flag, so a live reader is never hidden.
Send, resume, steer and inject admission atomically clears the target's archive flag.
Deletion requires every descendant stopped or failed, a detached external observer, or
an ended native observation, all drained, and shares retention's leaf-first command row
cleanup. Archive preserves retained unknown evidence; delete removes the tree's turns
and events without resolving them: no resolution is recorded and no `turn.resolved`
event is emitted. The Store still removes dependent records in one transaction and
retains mutation replies plus deleted command identities through the retry window. It advances
the conservative retained-history floor before removing event rows, refusing a live rule
cursor pin or an active or uncertain cleanup attempt. An unapplied cleanup review's
items for the deleted tree are superseded. Other Agents' Git/stream sources
cannot pin the selected tree's events. An attached observer with an open source still
prevents deletion under the drained-custody guard.
Process records are removed across every generation; consumed cleanup-plan replay
remains retained. No uncertain native effect is replayed.
Neither operation touches native stores, workspaces or branches; artifact cleanup
precedes record deletion when wanted. See [API](api.md) for DTOs and bounds.

`chat` identifies an Agent without a project (`project` is the empty string).
The daemon creates its private workspace from the transaction-allocated ID before
native I/O. Chat descendants retain that classification. `quota.observed` is a
session-family, generation-bound watch fact carrying a bounded typed
`QuotaObservation`; it does not alter turn state or token-spend accounting.
