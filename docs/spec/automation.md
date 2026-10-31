# Automation

Rules and timers submit ordinary commands through the existing router. This contract
does not add a second executor or a generic run/action graph.

## Rule files and authority

Global rules load from `<data_dir>/rules/*.toml`. Project rules load only from the
owner-registered canonical checkout's `.eliot/rules/*.toml`, under project-scoped
authority and its approved profiles/actions. Connected rule loading watches those files
for changes. Invalid reload keeps the last valid definition and reports the error through
bounded daemon diagnostics.

```toml
[[rule]]
id = "reply-on-fake-result"
on = { event = "turn.completed", match = { "subject.harness" = "fake" } }
do = [{ action = "send", target = "${event.agent}", expected_generation = "${event.generation}", text = "rule-send-echo" }]
limit = { max = 3, per = "agent", window = "1h" }
```

Required keys are `id`, `on`, `do`; optional keys are `enabled`, `limit`, `catch_up` and
`allow_self`. `catch_up` configures timer-slot recovery; committed-history catch-up is an
explicit owner request described below. Unknown keys and duplicate ids are errors. A
project rule cannot widen project, command, credential or route authority.

## Project registration and rule authority

Only owner-global `[projects.ID]` configuration registers a project. Its `path` names the
checkout; `profiles` and `actions` are explicit allowlists over existing global profiles
and supported actions. Missing or empty allowlists deny all. `project list` and
`project show ID` are owner-only readbacks of the canonical registration; there is no
project-registration mutation command in this increment.

Load project rules only from the registered canonical checkout's `.eliot/rules/*.toml`,
never from an Agent worktree. Their principal is `project:ID`. Router authorization and
Store admission/dispatch fences revalidate the registration's canonical checkout and
repository common directory, profile allowlist and action allowlist before effects.
`Agent.project` is a label and never establishes project authority. Project rules use
global profile definitions; project files cannot define or override native profiles,
interpreter executables or fixed argv prefixes, model/effort, route/account, credentials,
permissions, workspace mode or Git identity. A project `run.env` may supply only literal
values for owner-approved names under [project registration](configuration.md#project-registration-and-rule-authority);
it cannot select or override credentials, routes, accounts or reserved identity.
Project request-reply actions are granted per verb: a rule requires the exact `approve`,
`deny` or `answer` entry in `actions`; one reply grant does not authorize another.

Owner `rule.get`, `rule.catch_up`, `timer.get` and `timer.run` accept an optional project
scope. Supplying one selects its project principal even though the caller is Owner; an
unscoped call remains global-owner scope. There is no owner fallback when project
authority is absent or the selected project is not permitted.

## Event cursor and compact definition handoff

Rules read committed events in bounded pages. Matching/rendering use the event-time
`subject` stored with the event, not a later Agent row. A rule has one current definition
and at most one retiring definition:

```text
RuleState {
  principal, rule, revision, current_definition, current_cursor,
  retiring_revision?, retiring_definition?, retiring_cursor?, retire_through?,
  enabled, paused_reason?, updated_at
}
```

New rule: start at current event high-water H. Semantic edit: atomically retire the old
definition through H and start the new definition after H. While retirement remains,
defer another edit and report it; reread the latest valid file when retirement finishes.
Identical reload is a no-op. This is not a definition/activation hierarchy.

For each event, atomically either admit deterministic command rows and advance the
cursor, advance without a match, or record a deterministic validation failure and advance.
Store failure rolls back both. Disable/pause explicitly records the skipped range and
advances without firing. No silent history reinterpretation.

Referenced script changes are semantic edits. Frozen content/path validation catches
missed watcher notifications. Already admitted commands retain their original inputs.

### Retention and bounded backlog

Enabled current/retiring cursors pin the events they still need. Disabled rules pin
nothing. Daemon startup validates `automation.max_event_backlog` before readiness. The
rule worker reloads it during startup and each two-second reconciliation, applying the
current value to both global and registered-project rule lanes. An invalid runtime reload
reports a bounded diagnostic and retains the last valid value; an invalid initial value
prevents startup. When high-water exceeds the oldest enabled current/retiring cursor by
more than the configured limit, atomically record the skipped range, advance to captured
high-water, clear retirement and pause the rule with `backlog_exceeded`. Existing
commands continue. Explicit catch-up fails `history_gap`
when its requested history has already been pruned. A lagging rule cannot pin storage
indefinitely.

During explicit history catch-up, project Run actions use the same freeze path as live firing,
capturing each matching event and the validated current rule revision. Project authorization
preflights the complete rendered request set; deterministic per-event preparation refusals emit
`rule.rejected`. The retained catch-up parent remains `applied` and lists only admitted
children. Store or persistence failure rolls back the whole request, leaving no partial parent or
child set. A retained same-ID request returns its parent before current gates, as documented for
`rule.catch_up` in the API.

## Triggers

| Trigger | `on` fields | Durable key |
| --- | --- | --- |
| persisted event | `event`, optional `match` | event seq + definition revision |
| interval | `every="30m"`, optional `anchor` | anchored slot + schedule revision |
| cron | five-field `cron`, optional `tz` (UTC default) | scheduled UTC instant + schedule revision |
| once | `at` RFC3339 timestamp | UTC instant + schedule revision |
| manual | `timer run ID` | external mutation id |

Ephemeral samples never trigger durable actions. Intervals use `anchor + k*period`.
Missed ticks coalesce to `latest` or `none`, never burst. Cron evaluates in its configured
timezone; a repeated DST hour has distinct UTC instants and a missing local time is
coalesced to the first valid instant afterward. Persist the chosen instant, not local
text alone. Do not stack timer firings while the previous command set is open/unknown.

Schedule edits increment schedule revision and start after the edit boundary; action-
only edits preserve the schedule. Reconcile timers from stored state on restart.

## First connected timer slice

Global and registered-project timers use their namespace's startup-loaded rule files and
the existing command router. The connected timer slice admits ordinary `stop`, `send` and
`resume` Agent actions, plus targetless `launch` and `run` children for global and
registered-project interval, cron and once firings, including manual `timer.run`. See
[Timer Run invocation context](#timer-run-invocation-context).
Timer `interrupt`, `steer` and request-reply actions remain unsupported for scheduled or
manual timer runs.
Other action verbs remain future work.

```toml
[[rule]]
id = "periodic-send"
on = { every = "30m", anchor = 1800000000000 }
catch_up = "none"
do = [{ action = "send", target = "a1", expected_generation = 1, text = "scheduled echo" }]
```

`eliot timer run periodic-send` manually invokes the same SEND action with a generated
external ULID; `eliot --id ULID timer run periodic-send` supplies one explicitly.
Scheduled and manual timer actions have no event or subject context, so Agent-action
`target` and `expected_generation` values are literal. For example, a timer rule may use
`do = [{ action = "resume", target = "a1", expected_generation = 4, text = "Continue" }]`;
`target` names the retained Agent and `expected_generation` is its literal old generation
`N`. Timer `send` requires literal `text`; timer `resume` text is optional and literal.
Neither text value can use event or subject templates.
A timer `launch` also requires literal profile and workspace values; its optional text is
literal and cannot use event or subject templates.

An interval `every` value uses the humantime duration grammar and must be positive with
whole-millisecond precision; it is normalized to signed `period_ms`. An explicit interval
`anchor` is a signed Unix epoch millisecond integer. Registration captures one
signed epoch-millisecond sample and uses that same value to resolve an omitted interval
anchor, compute the initial `next_due_at`, and initialize Store state. Store commits the
effective anchor and initial due time with the timer row. Slots are `anchor + k*period_ms`,
starting at `k=1`. Existing timer state reuses its persisted anchor, `last_key` and
`next_due_at`; a restart never recalculates defaults from a new clock sample. A once `at`
value is RFC3339 with an explicit offset and whole-millisecond precision; it is normalized
to signed Unix epoch milliseconds. Cron has exactly five fields; `tz` is an IANA timezone
name and defaults to `UTC`. Persist due instants and numeric `last_key` as signed Unix
epoch milliseconds; the key denotes the selected UTC slot. Cron and once have no
user-specified anchor; their persisted `anchor` is the registration boundary used to seed
slot selection. It is never recaptured on restart.

For a newly registered timer, the first eligible slot is strictly after its captured
registration time. An explicit historical interval anchor establishes phase, not
pre-registration catch-up. A once instant at or before registration starts with no
`next_due_at`; `timer.run` remains available.
Repeated DST wall times have distinct UTC keys, while a missing wall time coalesces to the
first valid instant after the gap. Clock rollback never replays a slot at or before
`last_key`.

`catch_up` accepts `latest` or `none` and defaults to `none`. It applies to missed slots of
persisted timers on restart, not slots before initial registration. On restart, `none`
advances past missed slots without firing; `latest` coalesces missed slots to one firing at
the most recent due instant. A normally delayed wake for a timer that remained armed in
the live daemon still fires its due slot under either policy. Do not burst missed slots or
stack another firing while the previous action children are open or unknown.

Timer `stop`, `send` and `resume` actions freeze their literal `target` and
`expected_generation`; they do not look up an Agent row to fill either value or refresh the
generation. Resume's literal is the old generation `N`, which ordinary command admission
must match before advancing the retained Agent. A timer `launch` is targetless and fixes
`expected_generation` at 0. Timer invocations have no event or subject, so a template that
requires `${event...}` or `${subject...}` fails admission. `timer.get` is
an owner-only read of the canonical Core timer view. `timer.run ID` is an owner-only
manual firing keyed by an external ULID. Reusing that ULID returns the retained parent
before checking current enabled/paused/drain/rate gates and never dispatches a second
child. A new manual run obeys those gates and does not change scheduled `last_key` or
`next_due_at`. Its external ULID remains the parent command ID and never becomes a
scheduled `last_key`.

Restart resumes persisted schedules and retirement progress. A trigger, interval anchor,
period, timezone or `catch_up` edit increments `schedule_revision` and selects the first
slot strictly after the captured edit sample, preserving explicit interval phase without
pre-edit debt. An action-only edit preserves `schedule_revision`, anchor, `last_key` and
`next_due_at`; admitted commands keep their frozen inputs. Use the same one-current,
one-retiring handoff above and defer a second semantic edit until retirement completes.
An unknown or otherwise open timer action child blocks the next firing. The canonical
timer wire types are in [Core `timers.rs`](../../crates/eliot-core/src/api/timers.rs).

## Connected event rules

Global rules load from `<data_dir>/rules/*.toml` at startup and use committed events;
the connected event-rule worker watches the same files for semantic updates. Ephemeral
samples never trigger actions. `on.event` is a literal static committed event kind,
including kinds beyond `turn.completed`. Matching uses the event-time `subject`
persisted with the event, never a later Agent-row lookup. The canonical event and subject
shapes remain in [Core](../../crates/eliot-core/src/lib.rs).

Scalar `match` values compare exactly. String values prefixed `glob:` or `re:` use bounded
glob or regex matching; `{ one_of = [...] }` accepts a list of scalar values, and
`{ exists = true|false }` checks path presence. Conditions combine with AND. A missing
field does not match an ordinary condition; `exists = false` explicitly matches absence.
For example:

```toml
[[rule]]
id = "stop-on-launch"
on = { event = "command.applied", match = { "event.kind" = "glob:command.*", "event.data.state" = "re:^applied$", "event.agent" = { one_of = ["a1", "a2"] }, "event.generation" = { exists = true } } }
do = [{ action = "stop", target = "${event.agent}", expected_generation = "${event.generation}" }]
```

Whole-value rendering preserves JSON type. Templates cannot choose the action, action
name, executable, environment, credential or authority. For event-triggered `stop`,
`send`, `interrupt`, `steer`, `approve`, `deny` and `answer`, the rendered target and
`expected_generation` must exactly match the captured `Event.agent` and `Event.generation`;
an absent or different binding is rejected before action-child admission. Event-rule
`interrupt` and `steer` must also bind `expected_turn_id` to that event's nonempty string
`data.turn_id`. Store revalidates that exact turn at admission and at the dispatch effect
boundary; a historical or queued action whose turn is no longer active is refused without
retargeting current work. SEND and STEER text is rendered once and frozen from that captured
event, with no later Agent-row lookup to fill or retarget the command. An unresolved
placeholder fails admission.

`approve`, `deny` and `answer` are event-only request replies and require
`on.event = "request.opened"`. Each binds `expected_turn_id` to the captured event's
nonempty string `data.turn_id` and `request_id` to its nonempty string `data.request_id`;
both may be literal values or whole-value templates, but must equal the captured fields.
Rule rendering uses only that event and never fills a missing id or retargets another
request; Router/Store revalidate the exact current request before the effect. `approve`
and `deny` produce empty command args. `answer` accepts a literal answers map, passes it unchanged
under `SubmitCommand.args.answers`, and does not template-expand question ids or values.
Each question id maps to an object containing its native `answers` array. For one answer,
`SubmitCommand.args` is `{"answers":{"question-id":{"answers":["Continue"]}}}`.

```toml
[[rule]]
id = "answer-open-request"
on = { event = "request.opened" }
do = [{ action = "answer", target = "${event.agent}", expected_generation = "${event.generation}", expected_turn_id = "${event.data.turn_id}", request_id = "${event.data.request_id}", answers = { "question-id" = { answers = ["Continue"] } } }]
```

Global request-reply rules use Owner authority. Project request-reply rules require the
matching per-verb project action grant described above. An `applied` reply child records
that exact native write; the rule parent's `applied` still means admission only. Core's
`request.resolved` event remains bound to the reply command.
`request.closed` is cleanup without `command_id`, and `turn.completed` is a
separate turn fact; neither substitutes for reply acknowledgement. An unknown reply is
never replayed. Request replies are not available to timer rules or script stdout records.
The action definition is owned by [the rule definition module](../../crates/eliot-daemon/src/automation/definition.rs).
One firing atomically admits its Store-only parent and ordinary action child; the
parent's fixed child set is durable, and `applied` means admission rather than task completion.

Global event-rule STEER uses Owner authority. Project event-rule STEER requires an
explicit `steer` entry in the registered project's action allowlist; a `send` or `interrupt`
grant does not authorize it. Revalidate current project authority and the captured
Agent/generation binding before effects. STEER uses the existing Router control path,
including its harness capability, text validation, provider-hold and exact active-turn
checks; it supplies input to that captured turn, without starting a new turn. An applied
STEER does not assert task completion. See [Store command admission and dispatch](../../crates/eliot-store/src/lib.rs).

Each committed event retains its event-time subject snapshot (or absence) and any
Store-verified cause. Depth 0 denotes a proven ordinary controller command; rule, timer
and manual-timer commands carry depth 1 or one greater than their triggering verified cause. Native
`data.cause` is not trusted and old events are never reconstructed from current Agent
state. A verified cause depth above 5 silently advances the cursor without admitting a
parent/action or emitting another event, so suppression cannot create a cause loop.

New rules seed their cursor at captured event high-water `H` in the same transaction as
the definition insert. A semantic edit captures `H`, retires the current definition
through `H`, and starts the new revision after `H`. Keep at most one current and one
retiring definition. Defer another semantic edit until retirement completes, then reread
the latest valid file. Identical reload is a no-op. An invalid reload retains the last
valid definition and reports the error. Each event's match/validation result and cursor
advance commit atomically with any admitted command set; Store failure commits neither.
Disabled rules advance their skipped range without firing.

Backlog, explicit history catch-up, and rate-limit semantics are defined in
[Retention and bounded backlog](#retention-and-bounded-backlog).
This connected event-rule subset supports event actions `stop`, `send`, `launch`, `resume`,
`interrupt`, `steer`, `approve`, `deny` and `answer`, plus project event-rule `run` during live
evaluation and explicit history catch-up. Event-rule `resume` is available in live evaluation
and finite `rule.catch_up`; both use the captured event binding and ordinary command admission.
The global event-rule `run` contract below uses owner-global approval and the shared frozen
execution path. Other full L4.2 action requirements remain cumulative. SEND uses the
existing Router path, keeps its exact target generation and turn association, and
`applied` does not assert task completion. `allow_self` defaults to false; see [Guardrails](#guardrails)
for the same-principal self-cause behavior for SEND and Run. Goal actions and project
configuration overlays remain outside this connected event-rule contour.
Cause-depth suppression is silent and does not emit an endlessly retriggerable rejection event.

### Targetless launch

The `launch` action submits the existing ordinary launch command with flat fields:

```toml
[[rule]]
id = "launch-on-result"
on = { event = "turn.completed", match = { "subject.harness" = "fake" } }
do = [{ action = "launch", profile = "fake", workspace = "/work/eliot", text = "Review ${data.status}" }]
```

`profile` is a nonempty literal profile name and `workspace` is a nonempty literal path
to an existing workspace. Neither accepts templates. `text` is optional; for event rules,
its value is rendered once from the captured event context and frozen into the launch
command. Timer launch uses the same action shape, but its optional text must be literal.
Launch text follows the ordinary launch/linked-send 64 KiB persisted-text bound. A
nonblank text value uses the normal linked `send` admission and dispatch path. The launch
and its linked initial `send` are one configured launch action and consume one configured-
action counter; the generated `send` is not a second configured action.

Launch readiness, linked-input application and turn completion are separate outcomes of the
existing command and turn path, not additional wire statuses. The ordinary launch result
reports session readiness. The linked `<id>/send` uses its existing command state: `applied`
means its input was associated with the exact turn, and its existing `command.applied`
event/readback carries that association. It does not mean the turn completed; read the turn's
terminal result separately through the existing turn readback. While the linked `send` is
`accepted`, `queued`, `dispatched` or `unknown`, hold the next timer firing and any later
ordered action. `applied` closes the input-application gate without waiting for the turn to
finish. A linked send that is `rejected`, `failed` or `cancelled` cancels undispatched ordered
successors under the existing sibling semantics. `unknown` remains blocking and is never
replayed.

The emitted command has `target = null`, `expected_generation = 0`, and no turn or request
binding. Event Agent/generation fields do not select or retarget the new Agent. The rule
worker and Store retain the authoritative triggering event identity and verified cause
with the action admission; they are not copied into launch args. For the linked initial `send`,
verify causal ancestry from the launch command to that `send`; `EventCause.rule` alone does not
prove the relationship. No `project` field grants authority: `Agent.project` remains descriptive.

Owner rules use Owner authority. Project launch requires all of the explicit `launch` action
grant, an approved named profile, and revalidation of the current canonical project registration
and checkout. Nonblank initial text additionally requires the explicit `send` grant and creates
the ordinary linked send. The selected profile supplies model, effort, permissions,
executable and other profile settings; a rule cannot override them. A profile that requires
setup is refused. Workspace paths are checked by normal launch admission. Managed
worktree/base fields and per-action overrides are unsupported and rejected; unrecognized
fields remain errors rather than being discarded.

For an existing request ID, return the retained admission/result before resolving or freezing
current profile or workspace values. This replay does not re-dispatch an uncertain external
effect or silently choose another profile or workspace.

### Resume a retained Agent

The `resume` action submits the existing ordinary `resume` command through the Router/Store
path:

```toml
[[rule]]
id = "resume-after-known-stop"
on = { event = "command.applied", match = { "event.data.command_id" = "01ARZ3NDEKTSV4RRFFQ69G5FAV" } }
do = [{ action = "resume", target = "${event.agent}", expected_generation = "${event.generation}", text = "Continue: ${event.kind}" }]
```

The match value must be the exact known Stop command ID. Store's `command.applied` event
provides `event.data.command_id`; this example assumes no `event.data.verb`, does not filter on
a post-stop `subject.state`, and does not depend on `agent.stopped` being emitted. This is one
post-stop trigger example, not a restriction on event kinds; other committed events can trigger
resume when their captured Agent/generation and current Store admission are valid.

The ordinary command DTO is [Core `CommandSubmitRequest`](../../crates/eliot-core/src/api/mutations.rs).
For event rules, `target` and `expected_generation` resolve exactly to the captured
event-time Agent and old generation `N`. For scheduled and manual timer rules, both are
configured literal values: the target names the retained Agent and the generation is its
literal old `N`. Timer Resume has no event or subject binding and never refreshes `N` from
live state; ordinary command admission rejects a stale value. In either context, the
ordinary command has no turn/request binding. Store advances that same Agent to `N+1` only
if it is stopped, failed or lost, has no active turn, and has a retained subject resumable by
its harness. No replacement Agent is created. Native Codex resume keeps its pinned native
binding and uses normal native readback. The tinyFake verifier uses only a synthetic retained
subject for deterministic transport exercise; it makes no native L2 qualification claim and
never substitutes for Codex. Subject grammar remains plugin-owned. Resume keeps the retained
subject, profile and workspace, replays no old input, and accepts no profile, workspace,
worktree, base, setup, model, effort or permission override.

`text` is optional. Event text is rendered once and frozen under the ordinary persisted-text
bound; timer text is literal. Nonblank text creates the normal linked `<resume-id>/send` for
generation `N+1`; it is one configured `resume` action, and the linked SEND keeps its own
command ID and verified cause ancestry. Apply the linked-input ordering gate above: accepted,
queued, dispatched or unknown input holds later timer/action work; applied confirms
input-to-turn association, while the turn's terminal result remains separate. An unknown
resume or SEND is never replayed.

Owner rules use Owner authority. Project event and timer rules require an explicit `resume`
action grant and the current canonical registration, checkout and approval for the retained
profile. Nonblank text also requires an explicit `send` grant. `Agent.project` remains
descriptive and never establishes project authority.

Rate accounting uses the configured `RuleLimit` scope. With `per = "agent"`, targetless `run`
and `launch` have no target Agent and contribute no Agent-scope count; never borrow the
triggering event's Agent. Event `resume` uses its exact captured Agent; timer `resume` uses
its configured literal target. With `per = "rule"`, each configured launch or resume counts
once; a linked initial SEND is not counted again.
The existing global action guard also counts each configured action once. Only the selected
`RuleLimit` scope applies, so `per = "agent"` does not additionally
impose a per-rule count. The `RuleLimit` configuration is unchanged, and this introduces no
new quota or accounting key.

## Matching and rendering

`on.event` names a literal static committed event kind; ephemeral samples are not
triggers. Match paths resolve only against `{event, subject, data}` using the persisted
event-time snapshot. `subject` may be absent for non-Agent facts, and there is no
undocumented `agent.*` alias.

Scalar values compare exactly. String values beginning `glob:` use glob matching and
strings beginning `re:` use regex matching. `{ one_of = [...] }` accepts scalar choices;
`{ exists = true|false }` tests whether a path is present. Conditions combine with AND.
Missing paths do not match ordinary conditions; `exists = false` is the explicit missing
path check. Invalid regex/glob syntax rejects the file. An unresolved action placeholder
fails admission.

Substitution fills argument values, never executable/action kind, option name,
environment name, credential or authority scope. Workspace/ref/remote substitutions
still pass their normal validation. Selectors resolve once at firing to concrete
Agents/generations; delayed steer/interrupt/replies also retain exact turn/request ids.
Dispatch revalidates that binding without retargeting newer work.

## Actions are commands

A firing has a Store-only parent command and ordered children using existing command
fields: `parent_command`, `link_kind=rule_action`, `ordinal`. Parent outcome identifies
the fixed child set; its applied status means admission, not completion of the workflow.
Child ids derive from principal/rule/revision/trigger/index. No additional run tables.

For `rule_action` siblings, dispatch waits for the preceding sibling to be applied.
Rejected/failed/cancelled cancels undispatched successors; unknown holds them until
resolution. Selector children remain independent and are not accidentally serialized by
this rule. The renderer defines the nesting for a selector action before any dispatch;
its parent is complete for ordering only when every concrete child is applied.

Applied `send` means exact input-to-turn association, not successful task completion.
To react to a result, use a later rule on that turn's terminal event/cause. Do not add a
hidden wait/acceptance state to the router.

| Action | Existing implementation path |
| --- | --- |
| Agent verb | router with generation/turn/request binding |
| `launch` | targetless ordinary launch command at generation 0; optional nonblank text creates the normal linked `send` |
| `msg.send`, `var.set` | scoped coordination command; available after L5 |
| `run` | targetless `SubmitCommand` via Router; bounded child under saved authority; emitted commands become ordered children |
| `notify`, `emit` | bounded log/desktop or approved named webhook; persisted `x.user.*` event |
| `git.fetch`, `git.push`, `git.worktree` | optional native Git wrapper with frozen intent/readback below |

Scripts/commands that finish unsuccessfully can have partial effects. `failed` does not
mean rollback; uncertain outcomes stay `unknown`, and neither is automatically rerun.

## Git actions

Implement these wrappers only with a concrete automation recipe. They do not intercept
Git used by a coding harness, and do not block the first Codex or rule/timer path.
There is no generic built-in pull; project integration belongs to an explicit script.

Validate an owner-registered repository, approved remote, full refs and exact OIDs.
Reject option/control-character injection and custom unapproved helper schemes.
Read raw fetch URL and pushurls separately; applicable `insteadOf`/`pushInsteadOf`
requires direct approved endpoints rather than guessing a second rewrite.

### Isolated transport view

Use one private bare transport view for a command, with frozen endpoints and approved
credential settings. Disable system/global/XDG config, repository hooks, aliases,
rewrites, filters, fsmonitor, external diff and textconv in that view. Share only the
object database through standard Git object-directory/alternate mechanisms. Revalidate
its saved config before network dispatch/readback. No Git workflow service or global
artifact store is needed.

### Frozen desired state

Before an effect, retain:

- fetch: direct endpoint, source ref and advertised OID, destination tracking ref and
  expected old OID;
- push: frozen local OID, destination ref, pre-push remote observation and all approved
  push endpoints;
- worktree: exact repository/path/branch/base OID.

Fetch to a private ref, verify the advertised OID, then CAS-update the real tracking ref
with hooks disabled. A moved remote fails without updating the real ref. Push uses
`<frozen-local-oid>:<destination-ref>`, never a mutable source branch and never force.

### Unknown readback

After proving the process stopped, read back through the same frozen configuration.
Fetch is settled by the target tracking OID; push by the destination at every endpoint;
worktree by exact registered path/branch/intended OID. Readback proves the desired state,
not unique authorship of an indistinguishable concurrent change. No proof means unknown.
Hold later writes to the same repository behind an unknown write; unrelated work runs.

## Frozen script inputs

A `run` action (global or project) is an ordinary targetless `SubmitCommand` in existing
`commands`, with `verb="run"` and no Agent target or generation/turn/request binding. It
uses the existing Router for admission and dispatch; it adds no executor, run table or
workflow. Its raw input and frozen command args use Core `RunDefinitionArgs` and
`FrozenScriptRun`, respectively, defined in [the Core script DTO module](../../crates/eliot-core/src/api/scripts.rs).
Those DTOs own their field catalogs; this spec defines their run semantics.

For project event rules, `RunDefinitionArgs.cmd` is a literal argv vector: element 0 is
the exact owner-approved key in `interpreters`; element 1 is a literal relative entry
path below the registered checkout's `.eliot/scripts/`; later elements are literal script
arguments. The entry must resolve to a canonical regular file under that directory, with
no path escape, symlink/reparse point, device or alternate stream. Before spawn, authorize
the project `run` action and resolve element 0 to its approved interpreter. The executable
path and fixed argv prefix come only from owner configuration. Event data cannot change
argv or become a shell command string. Eliot invokes the executable directly with the
configured prefix, the script-entry argument from the frozen canonical receipt via the
[setup::file_argument helper](../../crates/eliot-daemon/src/setup.rs), and the remaining
literal arguments. On Windows only, the helper may normalize argv spelling after
link/reparse checks and a canonical round-trip prove it resolves to the same file; the
frozen canonical entry receipt and digest remain unchanged. Event-derived values are
available on stdin only. See [Project registration and rule authority](configuration.md#project-registration-and-rule-authority).

For global event rules, `cmd[0]` is an exact key in owner-private
`[automation.run].interpreters` ([owner approval](configuration.md#global-script-run-approval)). `cmd[1]` is a
literal relative entry below the fixed private `<data_dir>/scripts/` root; later elements are
literal script arguments. The entry must resolve to a canonical regular file below that
root with no path escape, symlink/reparse point, device or alternate stream. Any eligible
canonical regular entry under the root may be selected with an approved global Run and
interpreter; rules have no per-entry or per-rule file allowlist. Event data cannot alter
argv or become a shell command string.

`cwd` for project Run defaults to the registered canonical project checkout. A supplied
project cwd must be a literal relative path resolving inside that checkout after
canonicalization and link/reparse checks. Global event Run defaults to the canonical
private `<data_dir>/scripts/` root. A safe relative global cwd resolves against that root
and must remain inside it after canonicalization and link/reparse checks. An explicit
absolute literal cwd is allowed only for global owner rules and must resolve to an existing
safe canonical directory with link/reparse checks; project cwd remains checkout-confined.

`timeout` uses the humantime duration grammar and positive-millisecond validation already
used for timer intervals. It must represent a positive whole number of milliseconds and
allow a checked runtime deadline; when omitted it defaults to `10m`. This contract adds
no fixed run-specific maximum. `env`, when present, is a bounded map of literal nonsecret strings. Project Run names
must be in the registered project's `run_env`; global Run names must be in owner
`[automation.run].run_env`. A missing or empty scope-specific allowlist permits no
action-supplied environment keys. No template substitution is allowed in `cmd`, `cwd`,
`timeout` or `env`. Names also follow the credential, route, identity and reserved-variable
exclusions in [Environment and secrets](configuration.md#environment-and-secrets).

### Timer Run invocation context

Timer Run applies to global and registered-project interval, cron and once rules, and to
manual `timer.run` invocations of any existing timer. Manual invocation is not a
`TimerSchedule` variant; the existing schedule model remains authoritative, including
`Once` with its required `at: i64`. Timer Run uses the same scope-specific interpreter,
entry, cwd, environment, process-custody, output-validation and no-replay rules above.

Core represents Run context as either the existing Event context or a typed Timer context.
Keep the current Event-mode JSON shape and frozen Run-args digests unchanged. Timer context
has no synthetic Event, subject or clock timestamp. It carries the persisted
`schedule_revision`, existing `TimerSchedule`, persisted anchor and an invocation:
`Scheduled { scheduled_at_ms }` uses the actual selected slot; `Manual { parent_command_id }` uses the
real external ID of its `timer.run` parent. The enclosing rule ID and definition revision are
distinct from `schedule_revision`. Core script and timer DTOs own these shapes; see
[Core script DTOs](../../crates/eliot-core/src/api/scripts.rs) and
[Core timer DTOs](../../crates/eliot-core/src/api/timers.rs).

For scheduled firings, the existing `rule` parent args bind the rule, schedule revision and
selected slot. For manual firings, the parent is the existing `timer.run` command, whose ID is
the supplied external ULID; its request args and payload digest stay unchanged. Store
validates each Timer context against the authoritative timer row, rule/revision and actual
slot or manual parent, then stores the authoritative snapshot beside the children in the
existing timer-parent outcome. At dispatch and output admission, verify the frozen Timer
context against that stored parent snapshot. Derive principal and scope from the stored
parent/project binding, never from Timer context. Store keeps pure context, authority and
cursor validation inside its existing transactions/effect boundaries; script execution
and filesystem/vendor I/O remain outside Store locks.

Timer stdin uses the typed Timer/rule context and minimal command ID/verb projection; it
contains no Event or subject. The Run command itself keeps its own assigned ID; a manual
Timer context refers to the separate external `timer.run` parent ID. Timer Run uses the
saved Owner scope for global timers and project scope for project timers after its
command-scoped token is revoked. Existing `timer.run` replay returns the retained parent before
current gates; an Unknown Run is never respawned.
 `cmd` and `env` reuse existing process/config validation and bounds; do not add a
script-specific numeric limit registry. Implementers should use the shared Core constants
`MAX_FRAME_BYTES`, `MAX_TEXT_BYTES`, `MAX_EVENT_BYTES` and `MAX_COMMAND_ID_BYTES` from
[Core limits](../../crates/eliot-core/src/limits.rs), and the existing config validators
and constants `MAX_CMD_LEN`, `MAX_ENV_NAME_LEN` and `MAX_PASS_ENV_ENTRIES` from
[daemon config](../../crates/eliot-daemon/src/config.rs), as applicable. The setup-only
`MAX_TIMEOUT_MS` does not set a Run timeout maximum. Timer duration parsing is in
[the existing schedule module](../../crates/eliot-daemon/src/automation/schedule.rs).

The ordinary command args store Core `FrozenScriptRun`, which carries the resolved
interpreter snapshot, entry receipt and frozen event/rule context. Entry and executable
receipts reuse Core `FrozenSetupFile`; see [the Core model](../../crates/eliot-core/src/lib.rs).
Before spawn, dispatch revalidates the selected owner grant and both file receipts. Project
Run rechecks its current registration. Global Run rechecks the current owner
`[automation.run]` grant for the selected interpreter reference, program, argv prefix and every
action-supplied environment name. Any changed selected grant, executable or entry fails
closed rather than replacing frozen inputs. This freezes explicit references, not an
interpreter's imported dependency graph.

The controller serializes bounded JSON before spawn and rejects input larger than 1 MiB.
Core `ScriptInputContext` carries the existing Event, event-time subject and rule snapshot for
Event Run. That Event-mode JSON shape and the frozen Run-args digest stay unchanged. Timer
Run uses the typed Timer/rule context described above and has no Event or subject. In
either mode, `ScriptCommandContext` is the minimal Run command ID/verb projection assembled
after Store assigns the ID; it never serializes the full Core `Command` or its args.
The input excludes recursive stdin/context payload, environment, principal/scope and
bearer values, including the Run token.

The ephemeral Run token permits only `command.get` for its own Run ID; the RPC returns the
standard Core `Command` DTO with normal secret-free semantic args. The token is absent from
persisted command args and stdin and is revoked on process exit/cleanup or restart.

Its stdout is UTF-8 NDJSON, at most 1 MiB including line endings: each nonempty
line is one JSON object, LF or CRLF terminated; a final complete object may end at EOF.
Empty lines, malformed JSON, invalid UTF-8, truncated capture or excess output invalidate
the entire stream. Each line must deserialize as the strict Core `ScriptActionRecord`
DTO; that type owns the closed flat record field catalog and converts to an ordinary
`SubmitCommand`. Event-rule STEER, resume and request-reply actions do not extend the script
stdout action set. A `resume` stdout record is invalid under the closed Core DTO; it is never
silently dropped.
The effective send-text limit is Core `MAX_EVENT_BYTES` (64 KiB) from
[Core limits](../../crates/eliot-core/src/limits.rs). Unknown, missing, `null`, nested,
extra or wrong-type fields fail validation. Targets and generation/turn bindings are
literal and must pass ordinary command validation. Records cannot supply command `id`, `principal`, `parent_command`, `ordinal`,
`executable`, `cmd`, `cwd`, `timeout`, `env` or token fields.

After successful exit, validate each actually emitted record separately against its saved
scope: project Run uses the project action allowlist and Router generation/turn fences;
global Run uses ordinary Owner Router checks. All records must pass before atomic admission;
the rule loader does not statically analyze possible script output. Admitted children use the
saved rule principal—Owner for global Run, project principal for project Run—even after the
Run token is revoked.

The Core [`ScriptRunApplied` outcome](../../crates/eliot-core/src/api/scripts.rs) carries
the current child ID list and a per-run [`ScriptProcessResult`](../../crates/eliot-core/src/api/scripts.rs).
Its direct-exit state distinguishes an unobserved exit from an observed signal exit with
no numeric code. Its custody value is platform-specific: Windows reports `exact` only
after the owned Job is empty; Linux may report `degraded` when the direct exit is known
zero and the owned process group is observed empty. Degraded Linux quiescence does not
prove every descendant exited: a descendant may escape the process group with `setsid`.
Do not claim those descendants were killed or classify that limit alone as `unknown`.
See [existing process custody](../../crates/eliot-daemon/src/platform/custody.rs).

Before child admission, require direct exit code zero, complete valid stdout NDJSON and
quiescence proven by the platform's custody guarantee. Exact and degraded quiescence
both permit an applied result when the direct exit and stdout are proven; unknown custody
does not. Then admit all validated records atomically as ordered command children; the
applied outcome retains their IDs, not recursive Command objects. Empty stdout admits an
empty child set.

The same `ScriptProcessResult` is used for process-backed failed-capture diagnostics;
the existing failure code/cause remains authoritative, with no separate stderr DTO.
Its optional bounded Core [`StderrSnapshot`](../../crates/eliot-core/src/diagnostics.rs)
is diagnostic only. A stderr read/truncation condition does not invalidate an otherwise
known direct exit, complete stdout result and custody result.

A known unsuccessful direct exit, including timeout/cancellation with a confirmed
unsuccessful exit and quiescence, or a conclusive output-limit violation or fully observed
invalid stream with confirmed quiescence is `failed` and may follow partial script
effects. A truncated/lost required stdout stream, unobserved exit, unproven platform
quiescence or indeterminate final child-set commit is `unknown`. A known child-set
rejection/rollback is `failed`. Failed and unknown runs are never replayed; retrying the
same command id never starts the script again.

The contract is interpreter-agnostic; this Windows example uses the owner's PowerShell
interpreter approval, a registered script entry and one stdout record:

```text
# <data_dir>/config.toml
[projects.app]
path = "C:/src/app"
profiles = ["codex-luna"]
actions = ["run", "send"]
run_env = ["CI"]

[projects.app.interpreters.pwsh]
program = "C:/Program Files/PowerShell/7/pwsh.exe"
argv_prefix = ["-NoProfile", "-NonInteractive", "-File"]

# <checkout>/.eliot/rules/check.toml
[[rule]]
id = "check-completed-turn"
on = { event = "turn.completed" }
do = [{ action = "run", cmd = ["pwsh", ".eliot/scripts/check.ps1", "-Changed"], cwd = ".", timeout = "10m", env = { CI = "true" } }]

# stdout from check.ps1 (one NDJSON record)
{"action":"send","target":"a1","expected_generation":7,"text":"check completed"}
```

## Approvals and hooks

Async approval rules use exact Agent generation, turn and request. For synchronous
hooks, return only an immediate decision that committed while the request/deadline was
valid. On timeout/Store failure return the safe native default; discard late results,
never enqueue a later approval. Build the shim only with a harness that needs it.

## Goal loop

A Goal is keyed by `(agent,generation,mode,revision)`; native and Eliot modes are
independent. `set`, definition changes, pause, clear and resume advance revision.
Generation change pauses the old Goal. `on_idle` waits while an active native Goal drives.

Round key is an exact idle event seq or anchored cadence slot. Before work, read state,
requests, source health and unknown gates. Perform the bounded check and read-only
workspace fingerprint, then conditionally commit **all** results only if generation,
revision and round still match: completion/block, counters, budget or one ordinary send.
No stale round can publish a successful check or continuation. Incomplete fingerprints
cannot prove no progress. Exact status/budgets and limits are visible in `goal show`.

Checks are arbitrary explicit commands, not magically read-only. Fingerprinting itself
is read-only. Check outcomes use the same frozen inputs/custody/no-blind-retry rules.

## Durable tables

Add these in L4, not L1:

```sql
CREATE TABLE rule_state (
  principal TEXT NOT NULL, rule TEXT NOT NULL, revision INTEGER NOT NULL,
  current_definition TEXT NOT NULL, current_cursor INTEGER NOT NULL,
  retiring_revision INTEGER, retiring_definition TEXT, retiring_cursor INTEGER,
  retire_through INTEGER, enabled INTEGER NOT NULL, paused_reason TEXT,
  updated_at INTEGER NOT NULL, PRIMARY KEY(principal,rule)
);
CREATE TABLE timers (
  principal TEXT NOT NULL, rule TEXT NOT NULL, schedule_revision INTEGER NOT NULL,
  trigger TEXT NOT NULL, anchor INTEGER NOT NULL, last_key INTEGER, next_due_at INTEGER,
  PRIMARY KEY(principal,rule)
);
CREATE TABLE goals (
  agent INTEGER NOT NULL, mode TEXT NOT NULL, generation INTEGER NOT NULL,
  revision INTEGER NOT NULL, definition TEXT NOT NULL, state TEXT NOT NULL,
  last_round_key TEXT, updated_at INTEGER NOT NULL, PRIMARY KEY(agent,mode)
);
```

Goal definition/state retain text/check/cadence, budgets, counters and previous progress
observations. Action state lives in commands. Rate accounting counts already admitted
ordinary action child command rows once; Store-only admission parents are excluded.

## Guardrails

Only Store-verified causes control depth; a rule never trusts native `data.cause` or
reconstructs old provenance from a current Agent row. Cause depth above 5 silently
advances the cursor without a parent/action command or a new event, so suppression is
quiescent. Default per-rule limit is 10/minute, global
120 actions/minute; configured windows must fit retained accounting history. SEND
Self-triggering SEND and Run default off. Store suppresses a SEND or Run only when
Store-verified cause and origin command/parent rows prove the same principal and rule;
it checks Run before invoking the script. `EventCause.rule` alone is not authority.
`allow_self=true` permits intentional SEND or Run feedback, still subject to the existing
cause-depth and rate limits.
Draining or paused automation admits no new firings. Every action uses normal router
checks. These are execution limits, not a task approval process.
