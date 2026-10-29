# Automation

Delivered in L4, after the Codex command path and monitoring work. Rules, timers,
scripts and Goals submit ordinary commands through the existing router. There is no
Task/review engine, second executor or generic run/action graph.

## Rule files and authority

Global rules: `<data_dir>/rules/*.toml`. Project rules: the registered checkout's
`.eliot/rules/*.toml`, with project-scoped authority and approved profiles/actions.
Invalid reload keeps the last valid definition and reports the error.

```toml
[[rule]]
id = "push-writer-commits"
on = { event = "git.commit", match = { "subject.labels.role" = "writer" } }
do = [{ action = "git.push", workspace = "${subject.workspace}" }]
limit = { max = 3, per = "agent", window = "1h" }
```

This example requires a Git event with an unambiguous Agent `subject`; unattributed
shared-worktree commits do not match it. Required keys are `id`, `on`, `do`; optional
keys are `enabled`, `limit`, `catch_up`, `allow_self`. Unknown keys and duplicate ids
are errors. A project rule cannot widen project, command, credential or route authority.

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
nothing. At `automation.max_event_backlog` (default 100,000), atomically record the
skipped range, advance to captured high-water, clear retirement and pause the rule with
`backlog_exceeded`. Existing commands continue. Explicit catch-up fails `history_gap`
when its requested history has already been pruned. A lagging rule cannot pin storage
indefinitely.

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

Global timers use the same startup-loaded rule files and the existing command router.
The connected subset supports interval, once, five-field cron and owner-triggered manual
runs, each with one ordinary `stop` action. Other action verbs remain future work.

```toml
[[rule]]
id = "periodic-stop"
on = { every = "30m", anchor = 1800000000000 }
catch_up = "none"
do = [{ action = "stop", target = "a1", expected_generation = 1 }]
```

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

Restart resumes unchanged schedules. The schedule-edit semantics above remain the
target contract; startup changes to an existing schedule are rejected as unsupported in
this slice. Schedule replacement and hot reload remain future work. The canonical timer
wire types are in [Core `timers.rs`](../../crates/eliot-core/src/api/timers.rs).

## First connected event slice

The initial L4.1 path loads owner-approved global rules from
`<data_dir>/rules/*.toml` at daemon startup. It supports only the persisted
`turn.completed` trigger, exact equality match, and one ordinary `stop` action. The
match uses the event-time `subject`, not a later Agent read:

```toml
[[rule]]
id = "stop-on-result"
on = { event = "turn.completed", match = { "subject.harness" = "fake" } }
do = [{ action = "stop", target = "${event.agent}", expected_generation = "${event.generation}" }]
```

Whole-value rendering preserves the source JSON type, so `expected_generation` remains
an integer. Templates cannot choose the action, action name, executable, environment or
authority. The stop target and `expected_generation` must equal the triggering event's
`agent` and `generation`; another Agent or generation is rejected. A firing atomically
admits one Store-only parent and one ordinary stop child;
the parent's fixed child set is durable, and `applied` means admission rather than
completion. A new rule's captured high-water cursor is seeded atomically with its state.
Restart keeps the stored definition and cursor for an unchanged definition. A changed
valid definition for an existing rule is rejected as unsupported, leaving the stored
definition and cursor in force; backlog is never reinterpreted. Disabled rules advance
the skipped range without firing.

The default per-rule limit is 10 firings/minute and the global limit is 120 actions/minute.
Rate accounting counts each already admitted ordinary rule-action child command once and
excludes Store-only admission parents. Both limits are enforced atomically with command
admission and cursor advancement. A limit hit records the deterministic skip in a
Store-only rejected parent and advances without admitting an action child. Explicit
custom `limit` and `catch_up` values are unsupported here and must be rejected rather
than ignored.

Other event triggers, project rules, non-exact match operators, multiple actions,
scripts, Goals and hot reload remain unsupported. This stop-only action cannot create a
new turn event, so the current slice cannot loop on its own `turn.completed`; the target
cause-depth guard remains required before adding actions that can produce their own
triggering events. The L4.1 target contract above remains in force.

## Matching and rendering

The only template roots are `{event, subject, data}`. `subject` may be absent for
non-Agent facts. Exact, `glob:`, `re:`, one-of and `exists` matches combine with AND.
Missing required fields do not match; an unresolved action placeholder fails admission.
There is no undocumented `agent.*` alias.

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
| `run` | bounded child process under the rule's authority |
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

A project script is inside registered `.eliot/scripts` and uses an approved interpreter.
Capture small entry-file bytes or canonical allowed path/size/digest at admission.
Verify before execution; changed bytes fail rather than silently replacing queued work.
Do not claim that this freezes an arbitrary interpreter's entire dependency graph.

`run` fields are `cmd:[executable,arg,...]`, `cwd?`, `timeout?`, `env?`; project rules
resolve these through the approved interpreter/script/root policy, not arbitrary event
text. Default timeout is 10 minutes; output and environment are bounded.

`run` receives bounded JSON `{event,subject,rule,command}` on stdin and may emit at most
1 MiB of NDJSON actions. Wait for successful exit, validate all actions under the parent
principal, then admit them atomically as ordered command children. Execute no action
while stdout is still incomplete. A scoped run token never falls back to owner.
Timeout terminates owned custody; an uncertain script result is not rerun.

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

Cause depth above 5 suppresses firing. Default per-rule limit is 10/minute, global
120 actions/minute; configured windows must fit retained accounting history. Own-event
send/steer/inject requires `allow_self=true`. Draining or paused automation admits no new
firings. Every action uses normal router checks. These are execution limits, not a task
approval process.
