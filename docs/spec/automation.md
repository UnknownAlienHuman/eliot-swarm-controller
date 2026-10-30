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
the existing command router. The connected subset supports interval, once, five-field
cron and owner-triggered manual runs with one ordinary `stop` or `send` action. The
event-rule `interrupt` action is not supported for scheduled or manual timer runs. Other
action verbs remain future work.

```toml
[[rule]]
id = "periodic-send"
on = { every = "30m", anchor = 1800000000000 }
catch_up = "none"
do = [{ action = "send", target = "a1", expected_generation = 1, text = "scheduled echo" }]
```

`eliot timer run periodic-send` manually invokes the same SEND action with a generated
external ULID; `eliot --id ULID timer run periodic-send` supplies one explicitly.
Scheduled and manual timer actions have no event or subject context, so target, generation
and SEND text are literal values.

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

The timer action freezes its literal `target` and `expected_generation`; it does not look
up an Agent row to fill either value. Timer invocations have no event or subject, so a
template that requires `${event...}` or `${subject...}` fails admission. `timer.get` is
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
`send` and `interrupt`, the rendered target and `expected_generation` must exactly match
the captured `Event.agent` and `Event.generation`; an absent or different binding is
rejected before action-child admission. An event-rule `interrupt` must also bind
`expected_turn_id` to that event's `data.turn_id`. Store revalidates that exact turn at
admission and dispatch; a historical event for a turn that is no longer active is
rejected without retargeting current work. SEND text is rendered and frozen from that
event, with no later Agent-row lookup to fill or retarget the command. An unresolved
placeholder fails admission.
One firing atomically admits its Store-only parent and ordinary `stop`, `send` or
event-rule `interrupt` child; the parent's fixed child set is durable, and `applied`
means admission rather than task completion.

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
This connected event-rule subset supports ordinary `stop`, `send` and event-only
`interrupt`, plus project event-rule `run` during both live evaluation and explicit history
catch-up; both paths use the frozen inputs in [Frozen script inputs](#frozen-script-inputs).
Each catch-up Run freezes the exact captured event and validated current rule revision before
ordinary command admission. Run actions on timer rules and global event rules remain unsupported
in this first slice. The full L4.2 action contract is cumulative: global `run` and the other
actions listed below remain required, and this project contour does not complete or remove that
requirement. SEND uses the existing Router path, keeps its exact target generation and turn
association, and `applied` does not assert task completion. `allow_self` defaults to
false; see [Guardrails](#guardrails) for the same-principal self-cause behavior for SEND
and Run. Goal actions and project configuration overlays remain outside
this connected event-rule contour.
Cause-depth suppression is silent and does not emit an endlessly retriggerable rejection event.

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

`cwd` defaults to the registered canonical project checkout. A supplied literal relative
path must resolve inside that checkout after canonicalization and link/reparse checks.
`timeout` uses the humantime duration grammar and positive-millisecond validation already
used for timer intervals. It must represent a positive whole number of milliseconds and
allow a checked runtime deadline; when omitted it defaults to `10m`. This contract adds
no fixed run-specific maximum. `env`, when present, is a bounded map of literal strings
whose names are in the owner's `run_env` allowlist. No template substitution is allowed
in `cmd`, `cwd`, `timeout` or `env`; an absent or empty `run_env` permits no
action-supplied environment keys. Names also follow the credential, route, identity and
reserved-variable exclusions in
[Environment and secrets](configuration.md#environment-and-secrets).

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
Before spawn, dispatch revalidates the owner project grant and both file receipts. A
changed registration, executable or entry fails closed instead of replacing queued
inputs. This freezes explicit references, not an interpreter's imported dependency graph.

The controller serializes bounded JSON before spawn and rejects input larger than 1 MiB.
The child stdin uses the Core `ScriptInputContext` DTO from [the Core script DTO module](../../crates/eliot-core/src/api/scripts.rs).
Its event, event-time subject and rule id/revision are the captured snapshots. The
`ScriptCommandContext` member is the minimal run command id/verb projection assembled
after Store assigns the id; it never serializes the full Core `Command` or its args.
The input excludes recursive stdin/context payload, environment, principal/scope and
bearer values, including the Run token. Its stdout is UTF-8 NDJSON, at most 1 MiB including line endings: each nonempty
line is one JSON object, LF or CRLF terminated; a final complete object may end at EOF.
Empty lines, malformed JSON, invalid UTF-8, truncated capture or excess output invalidate
the entire stream. Each line must deserialize as the strict Core `ScriptActionRecord`
DTO; that type owns the closed flat record field catalog and converts to an ordinary
`SubmitCommand`. The effective send-text limit is Core `MAX_EVENT_BYTES` (64 KiB) from
[Core limits](../../crates/eliot-core/src/limits.rs). Unknown, missing, `null`, nested,
extra or wrong-type fields fail validation. Targets and generation/turn bindings are
literal and must pass ordinary command validation. Records cannot supply command `id`, `principal`, `parent_command`, `ordinal`,
`executable`, `cmd`, `cwd`, `timeout`, `env` or token fields.

After successful exit, validate each actually emitted record separately against the
project action allowlist, Router authorization and generation/turn checks. All records
must pass before atomic admission; the rule loader does not statically analyze possible
script output.

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
