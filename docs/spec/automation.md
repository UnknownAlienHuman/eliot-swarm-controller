# Automation

Rules and timers submit ordinary commands through the existing router. This contract
does not add a second executor or a generic run/action graph.

## Rule files and authority

Global rules load from `<data_dir>/rules/*.toml`. Project rules load only from the
owner-registered canonical checkout's `.eliot/rules/*.toml`, under project-scoped
authority and its approved profiles/actions. Connected rule loading watches the rules
directory non-recursively (plus the config source that also feeds generated rules)
for changes, with periodic reconciliation as the fallback. Managed worktrees, plugin
data and other subtrees are never watched. Invalid reload keeps the last valid definition
and reports the error through bounded daemon diagnostics.

```toml
[[rule]]
id = "reply-on-codex-result"
on = { event = "turn.completed", match = { "subject.harness" = "codex" } }
do = [{ action = "send", target = "${event.agent}", expected_generation = "${event.generation}", text = "rule-send-echo" }]
limit = { max = 3, per = "agent", window = "1h" }
```

### Rule file editing

The Owner can read, save and delete a top-level `.toml` source file in either approved
rule root. Read supplies the exact UTF-8 text and a lowercase SHA-256 revision of the
file bytes. Save creates a file only when the expected revision is absent and the file
does not exist. Replace and delete require the current revision. Save uses the same
parser and namespace validation as the loader. Read and save refuse content
containing secrets known to the daemon. Delete can remove such a file when the
Owner supplies its current byte revision. Writes use ordinary rule reconciliation.
Use the `source_file` returned by `rule.list` to locate an existing file. A filename
can contain several `[[rule]]` entries, and each revision covers the whole file.
The daemon serializes file API writes. For direct edits by other processes, the
filesystem conflict checks have the same limits as the script editor below.
Save acknowledges the file's publication. `rule.list` and `timer.get` show the
effective state after the existing worker reconciles the change.

Edit `enabled` and save to disable or reenable a rule. Deleting a file removes all of
its definitions from the source, while the ordinary retirement rules still govern
retained history and already-admitted commands. Saving never invokes Run now.

### Script editing

Owner-only `script.list`, `script.read` and `script.save`
([`scripts/edit.rs`](../../crates/eliot-daemon/src/scripts/edit.rs)) edit existing script
entries only under `<data_dir>/scripts/` and registered canonical checkouts'
`.eliot/scripts/` directories. An entry is addressed by `project` (absent = global)
and a relative `/`-separated path. Windows editing supports local drive volumes;
UNC and mapped remote roots are refused (the network rename protocol cannot honor
this RootDirectory contract). No `..`, links, reparse points, devices or alternate
streams are allowed. Opened regular files must have exactly one hard link. Valid existing
dotfiles can be edited; interrupted-save `.tmp` files are omitted from listing.

`script.list` takes an optional `cursor` and returns `roots` plus `next_cursor`.
Continue until `next_cursor` is absent, including after an empty page (withheld root).
Each RPC scans one root, at most 500 filesystem directory entries in total (directories,
refused entries and temporaries count), to depth six. It stops immediately at the scan
budget, sorts only that bounded selection and pages it within the encoded-byte budget
in [`script_edit.rs`](../../crates/eliot-core/src/api/script_edit.rs). A root can span
several pages. `truncated` means the scan budget was reached; further entries may exist
and require an external editor. Cursors are numeric root/entry positions, contain no
names and are best-effort continuations, not snapshots. Directory/registration changes
between pages can change the selection; refresh to restart. The desktop fetches all pages.
Complete entries (project id, relative path, display path and returned fields) are checked
against the current secret policy; unsafe entries/roots are withheld rather than redacted
into an editable identity. A read with an unsafe response identity is `not_allowed`.

`script.read` opens through verified directory handles and reads at most the 256 KiB
editor limit plus one byte from that same file handle. Oversized files are refused
(`not_allowed`), without a full-file read or revision hash. Otherwise it returns the
SHA-256 revision of the actual bytes and UTF-8 text without a byte-order mark. Non-UTF-8,
NUL-containing or secret-bearing content is withheld with a reason. Secret checks inspect
raw text and, when parseable as TOML or JSON, decoded keys and values as well. This does
not interpret arbitrary programming languages. Read and save use the same content gate;
a withheld entry cannot be overwritten through this editor. Save also checks the encoded
BOM/line-ending-preserved bytes. Known secrets and redaction markers are never saved.

`script.save` never executes anything and never adds, renames or deletes a user entry.
It replaces an existing entry after checking `expected_revision`, retaining its BOM and
CRLF convention (mixed endings become CRLF). The daemon serializes saves with one mutex.
It pins every directory component from the volume/filesystem root, creates and syncs a
new temporary relative to the pinned parent, then reopens the target relative to that
parent and rechecks file identity, link count and bytes immediately before replacement.
On Windows, directory handles use open-reparse/backup semantics and relative opens use
`NtCreateFile`, `OBJECT_ATTRIBUTES.RootDirectory` and `OBJ_DONT_REPARSE`. Directory delete
sharing is denied while pinned; file reads deny write sharing. Replacement
uses the native `NtSetInformationFile(FileRenameInformationEx)` form of
`FileRenameInfoEx` with the parent `RootDirectory` (the Win32 wrapper rejected this
relative form with error 87 on the qualification host); no path-based fallback is used.
Global temporaries inherit the private data-directory boundary. On Unix, existing libc
`openat(O_NOFOLLOW)` / `renameat` keep operations relative to retained file descriptors.
A replacement is staged on the same filesystem until rename. Before writing
into the opened staging directory, the daemon verifies that its own user owns
the directory and that it has `0700` access bits, allowing inherited set-group-ID.
The staged file stays private through the final conflict check. A save is
refused if the staged inode would change the original owner or group. The
replacement keeps the existing owner's, group's and others' read, write and
execute bits and clears set-user-ID, set-group-ID and sticky bits. New private
artifacts keep their restrictive creation mode.
Unix reopens the installed target after rename and verifies its inode, bytes,
owner, group and all `0o7777` mode bits. A mismatch or inability to verify is
`unknown`. The parent directory is synced after rename. A failed staged file
is removed through the held private directory. The daemon removes the stage
by name only if the parent is owned by the daemon user, group and others lack
write access, and the name still resolves to the held stage. Shared parents
can retain an empty private stage for owner cleanup.

The conflict guarantee is daemon-save exclusion plus an immediate handle-based recheck,
not atomic compare-and-swap against every external writer. Windows must permit target
delete sharing for replacement; an external rename/removal can race the last check.
Unix cannot exclude external writes, temporary-name substitution or directory moves
with these APIs; post-rename verification detects a substituted source but cannot undo
its effect. It is not a source-inode-bound atomic rename guarantee. Pinned handles
prevent a junction/symlink swap from redirecting temporary creation or replacement to a
newly substituted directory, but Unix may still operate on an approved directory moved
by another process. Coordinate external editing with saves. Windows does not promise
power-loss durability of the directory rename; Unix sync failure after replacement is
`unknown`. No uncertain save is automatically retried.

A Run admitted before a save fails its digest check at dispatch (`source_integrity`)
instead of executing new bytes. Run remains the existing Owner `timer.run` path for a
timer whose `run` action names the entry; there is no standalone Owner `run`.

The desktop's Open in editor uses literal argv and an absolute existing `.exe` from the
supported GUI-editor names in [`scripts.rs`](../../apps/swarm-desktop/src/scripts.rs)
(Notepad, Notepad++, VS Code/Insiders, VSCodium, Sublime Text, gVim, Zed, Notepad4).
A positive list prevents ordinary interpreter aliases from executing scripts through
this action. It validates user intent, not the authenticity of a renamed executable.
The child starts with `env_clear()` and restores only `SystemRoot`, `WINDIR`, `PATH`,
`TEMP`, `TMP`, `USERPROFILE`, `APPDATA`, `LOCALAPPDATA`, `HOMEDRIVE`, `HOMEPATH` when present.
`COMSPEC`, provider credentials and Eliot credentials are not inherited. An existing
editor process may retain the environment it was originally launched with.

`rule.list` reads one namespace's stored rule states, effective timer states,
canonical definitions and membership in the reconciled source for owner screens.
Rules-file enablement comes from the rule file. Assignment enablement comes from
the settings key below. The daemon alone sets `paused_reason` (the backlog policy below).

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

The [`RuleStateView`](../../crates/eliot-core/src/api/reads.rs) DTO owns the public
rule-state field catalog.

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

The [Core retention contract](core.md#retention) makes enabled current/retiring cursors
pin the events they still need; disabled rules pin nothing.
Physical retention is implemented by bounded Store cleanup during daemon maintenance.
The same bounded pass also runs on demand through owner-only `maintenance.run`,
which returns the pruned counts; see [api](api.md#methods).
The implemented backlog policy bounds rule work and releases retention pins; it does not
itself delete history. Daemon startup validates `automation.max_event_backlog` before readiness. The
rule worker reloads it during startup and each two-second reconciliation, applying the
current value to both global and registered-project rule lanes. An invalid runtime reload
reports a bounded diagnostic and retains the last valid value; an invalid initial value
prevents startup. When high-water exceeds the oldest enabled current/retiring cursor by
more than the configured limit, atomically record the skipped range, advance to captured
high-water, clear retirement and pause the rule with `backlog_exceeded`. Existing
commands continue. Explicit catch-up fails `history_gap`
when its requested history has already been pruned. A lagging
rule cannot pin expired storage indefinitely. Rate-limit windows longer than
command retention count only retained history.

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
`resume` Agent actions, plus targetless `launch`, `run`, `mail.send` and `var.set` children
for global and registered-project interval, cron and once firings, including manual
`timer.run`. See
[Timer Run invocation context](#timer-run-invocation-context).
Timer `interrupt`, `steer`, `inject` and request-reply actions remain unsupported for
scheduled or manual timer runs; `inject` requires an event trigger with exact turn binding.
Other action verbs remain future work.

```toml
[[rule]]
id = "periodic-send"
on = { every = "30m", anchor = 1800000000000 }
catch_up = "none"
do = [{ action = "send", target = "a1", expected_generation = 1, text = "scheduled echo" }]
```

`swarm timer run periodic-send` manually invokes the same SEND action with a generated
external ULID; `swarm --id ULID timer run periodic-send` supplies one explicitly.
Scheduled and manual timer actions have no event or subject context, so Agent-action
`target` and `expected_generation` values are literal. For example, a timer rule may use
`do = [{ action = "resume", target = "a1", expected_generation = 4, text = "Continue" }]`;
`target` names the retained Agent and `expected_generation` is its literal old generation
`N`. Timer `send` requires literal `text`; timer `resume` text is optional and literal.
Neither text value can use event or subject templates.
A timer `launch` uses the same literal profile or role and workspace fields; its optional
text is literal and cannot use event or subject templates. Folder assignment is supported
only by the settings-generated timer path. Timer `mail.send` and `var.set` use the same
literal targetless coordination DTOs as event rules (no templates; bounded, secret-checked
payloads) through the existing CAS and mailbox path with rule principal attribution and
rule-action limits.

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

Native before-tool callbacks are normalized as committed `hook.before_tool` events on
the existing event bus. Rules observe them with the ordinary `on.event` trigger and
existing event/subject matcher. The synchronous hook response carries only context items
already claimed at that native point; rule actions run through the ordinary command path
and cannot decide or alter the in-flight callback. There is no `on.hook` trigger,
`hook.decide` action, or separate matcher/policy engine. Permission decisions continue
through the existing exact approval callbacks.

Scalar `match` values compare exactly. String values prefixed `glob:` or `re:` use bounded
glob or regex matching; `{ one_of = [...] }` accepts a list of scalar values, and
`{ exists = true|false }` checks path presence. Conditions combine with AND. The
[matching contract](#matching-and-rendering) also covers `any_of`, `not` and missing fields.
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
and `deny` accept optional `choice` (whole-value template allowed, rendered to `choice_id`) and
optional `feedback` (text template allowed, rendered to `feedback`); neither defaults to any
choice. `answer` accepts a literal answers map, passes it unchanged
under `SubmitCommand.args.answers`, and does not template-expand question ids or values.
Each question id maps to an object containing its universal `answers` array under the
shared [`Answers`](../../crates/eliot-core/src/requests.rs) shape. For one answer,
`SubmitCommand.args` is `{"answers":{"question-id":{"answers":["Continue"]}}}`.
Store admission parses the shared `AnswerArgs` before persistence, so rules cannot
bypass answer syntax; each plugin translates the universal answer to its native write.

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
STEER does not assert task completion. When retention has pruned a hold's probe accepted
event, that hold's terminal scan fails with `source_integrity` instead of clearing on
incomplete history; the owner corrects it with the existing `provider clear-hold`.
See [Store command admission and dispatch](../../crates/eliot-store/src/lib.rs).

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
`interrupt`, `steer`, `approve`, `deny`, `answer`, `mail.send`, `var.set` and `inject`, plus
project event-rule `run` during live
evaluation and explicit history catch-up. Event `mail.send` and `var.set` are literal
targetless coordination actions; event `inject` binds its Agent and turn exactly from the
captured event (see below). Event-rule `resume` is available in live evaluation
and finite `rule.catch_up`; both use the captured event binding and ordinary command admission.
The global event-rule `run` contract below uses owner-global approval and the shared frozen
execution path. Other full L4.2 action requirements remain cumulative. SEND uses the
existing Router path, keeps its exact target generation and turn association, and
`applied` does not assert task completion. `allow_self` defaults to false; see [Guardrails](#guardrails)
for the same-principal self-cause behavior for SEND and Run. Goal actions and project
configuration overlays remain outside this connected event-rule contour.
Cause-depth suppression is silent and does not emit an endlessly retriggerable rejection event.

### Targetless launch

Candidate selection and its recorded reason follow the configuration routing contract at
first admission. Project/child authority is intersected before selection; retries retain
the original choice.

Queue assignment is configured in the unified settings file and runs through the existing
timer and ordinary `launch` path. Each firing reads the next bounded `.md` brief by
filename and emits one routing-aware launch. In a `worktree`-mode project that launch
gets the standard managed worktree an interactive top-level launch would get, frozen by
the daemon and prepared by Store in the launch admission; a rule cannot name a worktree
or base itself. `max_busy` counts unresolved launches,
linked initial sends and live Agents; an unknown command holds its slot. Store advances
the filename cursor only in the transaction that admits the durable launch command.
An unknown launch is never rerouted or replayed. No brief is moved or marked started in
the filesystem, so a crash or restart leaves the durable cursor and command as authority.
This is an automation increment, not a Task/Issue/acceptance engine.

The owner configures a global assignment under `[automation.assignments.<key>]`:

```toml
[automation.assignments.muse-backlog]
folder = "C:/Development/Rust/agent-briefs/20261114/backlog"
workspace = "C:/Development/Rust/worktrees/main-20261113"
role = "worker"
max_busy = 2
interval = "30s"
enabled = true
```

`key` is a stable identity; changing it starts a separate cursor. On each interval the
daemon selects the lexicographically first regular `.md` file whose filename is after the
retained cursor. New queue entries must sort after the cursor. The folder scan is bounded
to 500 entries per firing; overflow fails closed rather than skipping an unseen earlier
brief. Each brief is
limited to the ordinary 64 KiB launch-text bound. Unsafe, oversized or invalid UTF-8
briefs fail the slot visibly. An unreadable or missing briefs folder fails only that
assignment's slot with its code. A slot with no next brief advances to the next interval.
Every scheduled timer slot that is skipped or rejected (no next brief is `not_found`, a
routing miss is `routing_unavailable`) commits that code as the timer's `last_outcome`
with the slot, and an admitted slot clears it, so timer reads show why the last check
started nothing. A manual run changes neither `last_outcome` nor `last_key`.
A capacity, provider, workspace or transfer wait keeps the due slot and its brief cursor.
Settings generate a global interval timer
rule and an ordinary completion rule. Before admitting cleanup, Store verifies that
the captured Agent/generation was launched by this assignment's own launch for the
same key in the global Owner principal namespace. A matching `event.cause.rule` name
alone grants no cleanup authority; project rules with the same name remain separate.
The completion rule stops only that captured Agent/generation,
releasing native live capacity after custody closes. Native `data.cause` is not a match
authority. Assigned Agents own one brief. Without `goal` that is one-turn work,
retired after its terminal turn. With `goal = { check = [...], max_rounds, max_turns,
max_without_progress, check_timeout_ms }` it is Goal-owned work: the launch freezes
the brief's Goal (text naming the brief, the check run in the Agent's workspace), and
the admission transaction installs it bound to the generation that launch starts,
before any native dispatch. The ordinary Goal loop then rounds on idle. When that Goal
completes or blocks, its round commit emits `goal.changed {status, reason}` carrying
the launch's cause, and the generated completion rule matches `goal.changed` instead
of `turn.completed`. Store authorizes the retirement through the same launch-ancestry
check, and refuses a `turn.completed`-caused retirement while that generation's
assignment Goal is still open, so switching an assignment between modes never cuts a
running Goal. Like any cadence-less Goal, an assignment Goal needs `max_turns` or
`max_rounds`. A cleared assignment Goal retires nothing; the Owner stops that Agent. A
handoff of an assignment Goal does not carry the launch cause to the destination, whose
Goal is then ordinary.
When the assignment sets `cleanup_on_retire = true` (default false), the
applied stop also cleans that Agent's build, temp and managed-worktree
artifacts inline in the stop worker: same authority check, same quiescence
gate and guarded deletion, with retained receipts, and no second executor.
`enabled = false` (default true) pauses the assignment at timer admission before
`settings.save` acknowledges it. An unrelated rules-file parse error can retain the
previous rule snapshot, and the saved pause still fences fresh scheduled and manual
admissions. Effective timer reads report the pause. The folder cursor and schedule
position are retained. The completion rule stays enabled, so Agents already running
still retire.
Manual timer runs use the same admission and cursor checks. Multiple firings race
through the same retained cursor and are serialized by Store's cursor compare-and-swap.
The ordinary command ID remains the timer child ID, preserving exact same-ID replay.

The existing Owner timer worker arbitrates due assignments and explicit interactive
launch intents. It uses the oldest eligible due timer or enqueued intent within each
project. A timer with an open action keeps its due slot and yields to other work in
its project until the action settles. Ties between
assignment timers use their rule IDs. An interactive intent wins an exact timestamp
tie with a timer. Briefs within an assignment remain in filename order. The cursor
advances in the same Store transaction as successful admission. A head waiting on
capacity, a provider, a workspace or transfer keeps its position within its project
and other projects still get a chance.

After choosing a project, routing selects the first policy-compatible installed
candidate. Capacity or provider holds keep that route waiting. On subsequent
attempts the selected model, account and route bindings must still match. The
manager's Owner-granted priority orders fresh heads from highest to lowest. Within
each priority tier, projects rotate after the last successful admission. At 30 seconds
of waiting, heads take precedence in a single project rotation, so repeated priority
changes cannot keep overtaking them. The selected brief filename, previous cursor, assignment
key and capacity bound accompany the normal launch command for Store validation. Store
maintains unresolved assignment membership transactionally with command and turn changes.
It counts indexed unresolved membership, including unresolved launches,
linked initial sends, and starting, running or unknown native turns. Counting stops
at `max_busy` and excludes completed history. A terminal turn releases the slot;
process liveness by itself does not establish completion. Store checks the cursor,
all applicable capacity bounds and provider holds in the normal launch transaction.
A full scope keeps the due slot available for the next arbitration pass. Capacity waits
re-arbitrate on committed `capacity.released` (not on `stop` acceptance), and a
`capacity_unavailable` probe neither advances the rotation cursor nor lets a later
project in the same pass claim a slot on the blocked shared scope ahead of the waiting
project. An already-dispatched or unknown command retains its reservation until verified closure.

The `launch` action submits the existing ordinary launch command with flat fields. An
explicit `profile` or a routing `role` may be supplied; they are mutually exclusive.

```toml
[[rule]]
id = "launch-on-result"
on = { event = "turn.completed", match = { "subject.harness" = "codex" } }
do = [{ action = "launch", profile = "codex-luna", workspace = "/work/eliot", text = "Review ${data.status}" }]
```

`workspace` is a nonempty literal path to an existing workspace. `profile`, when present,
is a nonempty literal profile name; `role`, when present, is a nonempty literal routing
role. Neither accepts templates. Both may be omitted to use the routing default.
`text` is optional; for event rules,
its value is rendered once from the captured event context and frozen into the launch
command. Timer launch uses the same action shape, but its optional text must be literal.
Settings-generated assignment timers add `assignment = { key, folder, max_busy }`; this
form requires a role and obtains `text` from the next brief at timer admission.
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

When [Agent deletion](api.md#agent-and-daemon-read-views) removes the full command set of a rule
or timer launch, the rule cursor and timer schedule state stay durable. A consumed
slot stays consumed, and a deleted manual firing ID remains bound through the retry
window.

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

Managed project automation uses the existing timer, routing and command path.
Project-owned rules keep their registered policy. Manager-directed work must
carry the current [assignment](coordination.md#project-manager-assignment) binding
through admission and dispatch, including queued launch. An epoch change does not
rewrite pending work or replay an uncertain effect. Project capacity and fair
selection extend this same path when those increments are implemented.

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
binding and uses normal native readback. Subject grammar remains plugin-owned. Resume keeps the retained
subject, profile and workspace, replays no old input, and accepts no profile, workspace,
worktree, base, setup, model, effort or permission override.

`text` is optional, but a supplied value must be nonblank. Event text is rendered once and
frozen under the ordinary persisted-text bound; ordinary command validation rejects blank
rendered text. Timer text is literal, and supplied blank or whitespace-only text is rejected;
omission remains valid. Nonblank text creates the normal linked `<resume-id>/send` for
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

### Inject context at the safe point

The `inject` action submits the existing ordinary `inject` command through the
Router/Store path with no new executor:

```toml
[[rule]]
id = "hook-context"
on = { event = "hook.before_tool" }
do = [{ action = "inject", target = "${event.agent}", at = "turn_safe_point", text = "Context: ${event.data.tool}" }]
```

`target` renders from the captured event and must equal `event.agent`;
`expected_generation` binds the captured event's generation. For
`at = "turn_safe_point"` the rule also binds `expected_turn_id` to the captured
event's nonempty `data.turn_id`. Store enforces these bindings and the live Agent
generation at admission. For `at = "next_input"` the command carries no turn
binding and is claimed by the next send in that generation.
`at` is a literal `turn_safe_point` or `next_input` (no templates; `active_turn` is not
a rule action). `text` renders once from the captured event and is frozen under the
512 KiB text bound. Store revalidates the exact Agent and (for safe-point) turn
at admission and at the dispatch effect boundary; adapters whose effective
`context_points` lack the requested point return their typed `unsupported`, never a
weaker send. `inject` requires an event trigger and is unsupported for timers.

## Outcome chaining

Run settlements use `command.applied`, `command.failed` and `command.unknown` with
`data.verb = "run"`, `data.state` and the exact `data.command_id`. A project Run also
has `data.rule_project`. Match it with `event.cause.rule` to select a rule within its
namespace. An observed script exit is in `data.outcome.process.direct_exit`, including
its `exit_code` when available. An applied Run has a successful exit and admitted
script output. The `script_exit_failed` cause selects an observed nonzero exit.

`turn.completed` has the native terminal outcome in `data.outcome` (`success`,
`failure`, `interrupted` or `unknown`). For a turn started by a rule's SEND,
`data.command_id` identifies that SEND and `event.cause.rule` identifies the rule.
A launch with text uses its linked initial SEND, which carries the launch rule's
cause. `turn.unknown` records an uncertain terminal with `data.outcome = "unknown"`
and the SEND ID when Store can prove that binding.

To launch a fixer when a project check exits unsuccessfully, put the timer and follow-up
definitions in the registered project's rule directory. In this example the project
`app` approves `run`, `launch`, `send`, the `pwsh` interpreter and the `fixer` profile.
`check.ps1` is under the project's `.eliot/scripts/` directory. Use the registered
checkout path as returned by `project.get` for `workspace`.

```toml
[[rule]]
id = "quality-check"
on = { every = "1h" }
do = [{ action = "run", cmd = ["pwsh", ".eliot/scripts/check.ps1"] }]

[[rule]]
id = "fix-quality-check"
on = { event = "command.failed", match = { "data.verb" = "run", "data.rule_project" = "app", "event.cause.rule" = "quality-check", "data.outcome.cause" = "script_exit_failed" } }
do = [{ action = "launch", profile = "fixer", workspace = "/work/app", text = "Investigate check exit ${data.outcome.process.direct_exit.exit_code}." }]
```

To notify on success and resume once on a known unsuccessful terminal turn, match the
producer `fix-quality-check`. A terminal turn leaves the live Agent idle. To resume its
native session into a new generation, first stop the exact generation and use the applied
Stop from that rule as the trigger for `resume`. For project rules, grant `notify`,
`stop`, `resume` and `send` explicitly and use rule IDs unique to the intended chain.

```toml
[[rule]]
id = "notify-fixer-success"
on = { event = "turn.completed", match = { "event.cause.rule" = "fix-quality-check", "data.outcome" = "success" } }
do = [{ action = "notify", message = "The fixer task completed successfully." }]

[[rule]]
id = "retire-fixer-for-resume"
on = { event = "turn.completed", match = { "event.cause.rule" = "fix-quality-check", "data.outcome" = { one_of = ["failure", "interrupted"] } } }
do = [{ action = "stop", target = "${event.agent}", expected_generation = "${event.generation}" }]

[[rule]]
id = "resume-fixer-once"
on = { event = "command.applied", match = { "event.cause.rule" = "retire-fixer-for-resume" } }
do = [{ action = "resume", target = "${event.agent}", expected_generation = "${event.generation}", text = "Investigate the known unsuccessful result and continue once." }]
```

The resumed turn has the retry rule's cause, so its terminal cannot reenter the stop
rule. Unknown turns and unknown Stop outcomes require readback before further action.

## Matching and rendering

`on.event` names a literal static committed event kind; ephemeral samples are not
triggers. Match paths resolve only against `{event, subject, data}` using the persisted
event-time snapshot. `subject` may be absent for non-Agent facts, and there is no
undocumented `agent.*` alias.

Scalar values compare exactly. String values beginning `glob:` use glob matching and
strings beginning `re:` use regex matching. `{ one_of = [...] }` accepts scalar choices;
`{ exists = true|false }` tests whether a path is present. Conditions combine with AND.
Missing paths do not match equality, choices or patterns. Use `exists = false` to check
for absence. A field can have `{ not = CONDITION }` with any one of these conditions.
`not` complements the result, including when the source path is absent.

Use `any_of` inside `match` for OR across fields. It takes 1 to 32 nonempty field
tables. Each table is an AND group with the same paths and field operators. The top-level
field conditions and at least one alternative must match. The file-size and pattern
bounds also apply to the alternatives. `any_of` groups and `not` operators are each
one level deep.

```toml
on = { event = "turn.completed", match = { "data.outcome" = { not = "success" }, any_of = [{ "subject.harness" = "claude" }, { "subject.profile" = "batch" }] } }
```

An `if` builder needs OR across different paths and a way to complement a comparison.
Overlapping alternatives admit a single firing of the rule. Separate OR rules could
each admit an action, and `exists = false` only tests absence. Invalid pattern syntax
rejects the file. Unresolved action placeholders fail admission.

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
| `mail.send` | targetless ordinary command; Store atomically commits one scoped queued-mail fanout with its Applied outcome |
| `var.set` | targetless ordinary command; Store atomically commits one versioned variable write with its Applied outcome |
| `inject` | Agent-bound ordinary command through the existing `inject` verb; durable pending context claimed at `turn_safe_point` or `next_input` with exact event turn binding; unsupported points return typed `unsupported`, never a weaker send |
| `run` | targetless `SubmitCommand` via Router; bounded child under saved authority; emitted commands become ordered children |
| `notify` | targetless ordinary command; Store atomically commits one persisted `x.user.notify` event with its Applied outcome (local log only) |
| `emit` | targetless ordinary command; Store atomically commits one persisted `x.user.<topic>` event with its Applied outcome (local log only) |
| `git.fetch` | targetless ordinary `SubmitCommand` through Router/Store with frozen intent and readback |
| `git.push`, `git.worktree` | unsupported in this fetch-only increment |

Scripts/commands that finish unsuccessfully can have partial effects. `failed` does not
mean rollback; uncertain outcomes stay `unknown`, and neither is automatically rerun.

`notify` and `emit` are local-only in this increment: no desktop delivery, no
webhook, and no external message is sent. Both are targetless ordinary commands
(`target = null`, no generation/turn/request binding) whose Core
[`NotifyDefinitionArgs`/`EmitDefinitionArgs`](../../crates/eliot-core/src/api/notify.rs)
are validated literally at admission (no templates; bounded, secret-checked
payloads). Execution commits exactly one persisted event — `x.user.notify`
with `{message,title?}` or `x.user.<topic>` with `{data}` — together with its
Applied settlement in a single Store transaction, then publishes the committed
batch (the user event first, then `command.applied`) on the bus; the Applied
readback carries that event's kind and seq as the standard command readback,
and a commit failure settles Unknown. A future desktop or
approved-webhook increment must add its own delivery binding and must not
reuse this local path as a transport.

`mail.send` and `var.set` are scoped coordination actions through the existing
Router/admission path, not a new executor. Both are targetless ordinary
commands (`target = null`, no generation/turn/request binding) whose Core
[`MailDefinitionArgs`/`VarDefinitionArgs`](../../crates/eliot-core/src/api/coordination.rs)
are validated literally at admission (no templates; bounded, secret-checked
payloads); the rule file also accepts `msg.send` as a compatibility alias for
`mail.send`, and the canonical verb is `mail.send` (the CLI spells the same
effect `msg send`). Admission freezes recipient/project scope under the saved
originating principal: Owner reaches any existing Agent and names its variable
scope explicitly, while a project principal `project:ID` reaches only Agents in
its own project and writes only its own `project:ID` scope (an omitted scope
defaults to it). Project registration must grant each canonical verb explicitly in `actions`;
the captured registration revision is checked at dispatch and effect commit.
Execution reuses the existing authorization, payload validation, mailbox limits,
compare-and-swap/version semantics and
delivery/acknowledgement semantics; a stale or unauthorized destination fails
explicitly. Each admitted rule child commits its effect together with its
Applied settlement in a single Store transaction without entering the public
write-id namespace, then publishes the committed batch in order while still
serialized with the writer: `mail.send` publishes `command.applied` carrying
the delivery readback and fans out mailbox wakes only after commit through the
existing coordination wake path; `var.set` publishes `var.changed` first, then
`command.applied` carrying the write readback. These rule-produced events carry
the exact child command's verified cause, preserving the depth-five guard;
standalone coordination events keep their existing semantics. Mail delivery IDs
use `rule:<ULID>/<recipient>` outside the public write namespace: the ULID's
timestamp is the child's persisted admission time and its entropy is derived
deterministically from the child identity. Pruned deliveries therefore retain
the ordinary idempotent acknowledgement age semantics. Definite validation,
authority, mailbox-capacity and CAS refusals settle Rejected with their code;
uncertain persistence or commit failures remain Unknown.
Malformed action fields report the recognized action, field path and reason
without including rejected values or rule source excerpts.
Replayed or restarted source events never duplicate the effect: an
already Applied child returns its recorded result without a new delivery or
write. Direct control actions stay bound to their event subject, and scripts
are not turned into Owner RPC clients.

## Git actions

This increment connects only the concrete `git.fetch` recipe below, as a native effect on
the existing Router/Store command path. Owner-global and registered-project event/timer
actions use the same targetless `git.fetch` command; it is not a `run` wrapper or a second
executor. Project authority comes from the saved principal and canonical registration, not
from action args. The literal `repository` names an owner registration ID; Owner rules may
select any registration, and a project principal `project:ID` must match that ID. These
actions do not intercept Git used by a coding harness. There is no generic built-in pull;
merge, rebase and repository integration remain explicit script/workflow work, while
`git.fetch` is the bounded project fetch wrapper. `git.push` and `git.worktree` remain
unsupported here. Both action callers use the existing Router/Store admission path and
ordinary `Store::dispatch`/`Store::settle` methods; no parallel admission API is added.

Validate an owner-registered repository, approved remote and exact OIDs. `source_ref` is a
full valid ref. `tracking_ref` must be a full valid ref under `refs/remotes/` with a
nonempty valid suffix; refs under `refs/heads/` or `refs/tags/` are rejected. Freeze also
rejects a symbolic tracking target. Reject option/control-character injection, credentials
embedded in URLs, unresolved `insteadOf`/`pushInsteadOf` rewrites and unapproved helper
schemes. The repository's raw
fetch URL must exactly match the approved direct endpoint. Read pushurls separately and
never substitute a pushurl for the fetch endpoint.
This first connected fetch supports approved HTTPS/HTTP/native Git/file URLs and absolute
local paths; SSH is unsupported until a selected owner-approved frozen SSH-settings caller
exists, with no ambient SSH configuration fallback.

The raw arguments are Core [`GitFetchDefinitionArgs`](../../crates/eliot-core/src/api/git.rs);
all supplied values are literal and do not accept event or subject templates. Optional
`timeout` uses humantime, must be positive with whole-millisecond precision and fit a
checked platform `Instant` deadline (unrepresentable values are `invalid`, never panic),
and defaults to 10 minutes when omitted. This registered-project timer example uses the
`app` owner configuration in [configuration](configuration.md#project-registration-and-rule-authority).

```toml
[[rule]]
id = "refresh-origin-main"
on = { every = "1h" }
do = [{ action = "git.fetch", repository = "app", remote = "origin", source_ref = "refs/heads/main", tracking_ref = "refs/remotes/origin/main" }]
```

Before Store admission, Router resolves the registered canonical checkout/common directory,
the saved registration and remote binding, direct endpoint, and the current target tracking
OID. A read-only `git ls-remote --refs` observation freezes the exact advertised source OID.
The resulting Core [`FrozenGitFetch`](../../crates/eliot-core/src/api/git.rs) is persisted
in the ordinary command args, binding the selected registration and transport snapshot,
advertised source, expected target state and timeout before admission; no execution table
or API method is added. When
`expected_old_oid` is `None`, the target ref must be absent and the update is create-only
compare-and-set.

### Isolated transport view

Use one private bare transport view for a command, with frozen endpoints and approved
credential settings. Disable system/global/XDG config, repository hooks, aliases,
rewrites, filters, fsmonitor, external diff and textconv in that view. Share only the
object database through standard Git object-directory/alternate mechanisms. Revalidate
its saved config before network dispatch/readback. No Git workflow service or global
artifact store is needed.

The selected transport config must match the frozen endpoint/helper values and the fixed
safe-config builder. Revalidate repository identity and that config before dispatch and
readback. Use explicit Git argv, bounded stdout/stderr and the existing
[`WorkspaceGit::repository_identity`](../../crates/eliot-daemon/src/workspace_git.rs),
[`git_path_argument`](../../crates/eliot-daemon/src/git_monitor.rs) and
[`OwnedChild::spawn`](../../crates/eliot-daemon/src/platform/custody.rs) helpers. The
optional `manager` credential helper is an explicit canonical file receipt, runs headless,
and reuses only the existing noninteractive manager login. Its receipt is never persisted
as a secret or sent on stdin/logs; omission disables helpers and there is no authentication
fallback or global/auth configuration change.

### Frozen fetch state and ref update

Core [`FrozenGitFetch`](../../crates/eliot-core/src/api/git.rs) owns the frozen intent; this
spec does not copy its field catalog. Fetch into a private ref, verify the received OID is
the frozen advertised OID, then CAS-update the real tracking ref with hooks disabled and
`git update-ref --no-deref`. `expected_old_oid = None` means create only if the tracking
ref is absent. Freeze rejects a symbolic tracking target, and `--no-deref` prevents a
later symbolic ref from redirecting the CAS. A moved remote or failed CAS leaves the real
tracking ref unchanged; `HEAD`, local branches and the worktree are unchanged.

### Failed transfer and unknown readback

A definite failed transfer is distinct from an uncertain effect. After the interruption
and custody checks, `exit_code = Some(nonzero)` is handled before the private fetched ref
is required: when exact tracking-ref readback still equals the frozen old state, the
command settles `failed` (`git_fetch_source_moved_or_failed`). A missing private ref is
expected when the source ref disappears or the transfer fails; the fetch writes only the
private ref, so its absence does not imply that the later tracking-ref CAS ran. Partial
object downloads do not imply rollback and do not make a known failed transfer `unknown`.
A missing exit status is not a definite nonzero exit: it still requires the
private-ref/OID validation, and an unreadable private ref stays `unknown`. Private-view
cleanup follows durable settlement through the existing Router/Store path.

After proving the process stopped, read back the tracking ref through the same frozen
configuration and canonical common directory. Unknown settlement is read-only: it never
fetches again, performs another CAS, or retries the effect. Only readback confirming the
frozen advertised OID may produce Core [`GitFetchOutcome`](../../crates/eliot-core/src/api/git.rs);
ordinary command state and error remain authoritative for failed/unknown results. Readback
proves desired state, not unique authorship of an indistinguishable concurrent change; no
proof means unknown. The common-directory fence applies symmetrically to native controller
Git mutations: `git.fetch` and native worktree add/remove share one fence across all
worktrees of that repository, indexed by `$.common_dir`. Fetch uses its ordinary durable
command dispatch state; worktree add/remove use a minimal durable native-Git-dispatch marker
in existing Core [`WorkspaceCreationOutcome`/`WorkspaceRemovalOutcome`](../../crates/eliot-core/src/lib.rs),
settled by the existing [Store workspace path](../../crates/eliot-store/src/lib.rs) and run
by [`WorkspaceManager`](../../crates/eliot-daemon/src/workspaces.rs). Unknown from retained-
Ready Resume identity readback or pre-mutator source drain/inspection is read-only and does
not mark a possible Git write. New worktree intents and pre-native outcomes persist an
explicit no-dispatch fact; the actual add/remove boundary durably marks may-have-begun before
spawn, and settlement preserves it. An absent marker on a legacy Unknown creation/removal is
ambiguous and remains fenced unless existing `reuse`/`resume_previous` or exact read-only
facts prove no write. A persisted marker after its native phase is complete does not keep a
fence active; Store uses the current workspace phase/stage. See the Core workspace outcomes
and [workspace preparation](../../crates/eliot-core/src/lib.rs), Store settlement, and
[`WorkspaceManager`](../../crates/eliot-daemon/src/workspaces.rs). Git invoked inside
harnesses or explicit Run/setup scripts is not intercepted by this fence. A marked possible
write that remains unknown blocks later native controller Git mutations in both directions
for that common directory; unrelated repositories continue. Store owns the transaction
fence; Git I/O stays outside Store locks.

## Frozen script inputs

A `run` action (global or project) is an ordinary targetless `SubmitCommand` in existing
`commands`, with `verb="run"` and no Agent target or generation/turn/request binding. It
uses the existing Router for admission and dispatch; it adds no executor, run table or
workflow. Its raw input and frozen command args use Core `RunDefinitionArgs` and
`FrozenScriptRun`, respectively, defined in [the Core script DTO module](../../crates/eliot-core/src/api/scripts.rs).
Those DTOs own their field catalogs; this spec defines their run semantics.

For project event rules, `RunDefinitionArgs.cmd` is a literal argv vector: element 0 is
the exact owner-approved key in `interpreters`; element 1 is a literal path relative
to the registered checkout and must select an entry below `.eliot/scripts/`. Later elements
are literal script arguments. The entry must resolve to a canonical regular file under that directory, with
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
scope and the admission rate policy frozen in the committed rule parent: project Run uses
the project action allowlist and Router generation/turn fences;
global Run uses ordinary Owner Router checks. For parents with frozen policy this
validation must not depend on the definition revision remaining retained; rows
committed before the parent policy freeze still resolve their limit through the
existing retained-definition lookup. All records must pass before atomic admission;
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

Async approval callbacks use exact Agent generation, turn and request. The system context
hook is separate: it returns only FIFO context already committed for the exact generation
and captured turn before its deadline. It does not approve or deny a tool. On timeout,
cancellation or Store failure, return no context; discard late context and never enqueue
it for a later callback or turn. Its first native consumer is Claude Code 2.1.278
`PreToolUse.additionalContext`; the source schema is confirmed and runtime application is
still pending qualification.

## Goal loop

A Goal is keyed by `(agent,generation,mode,revision)`; native and Eliot modes are
independent. `set`, definition changes, pause, clear and resume advance revision;
an identical `set` on the bound generation is a no-op. Generation change pauses
the old Goal on its next round attempt, and `resume` on a drifted generation is
refused. Native mode parses but is `unsupported`: the installed harness plugins
expose no Goal protocol. Observe-only Agents take no Goals. Automatic `on_idle`
driving needs a bound: `set` refuses a Goal without a cadence unless `max_turns`
or `max_rounds` is set. Cadenced Goals round only on explicit slots and need
none; explicit manual rounds never need one either.

[Task handoff](coordination.md#handoff) transfers only a quiescent Eliot Goal,
clears A and installs B paused at a new revision. Round markers and continuation
identities never transfer. A private additive turn baseline preserves all consumed
turns while charging B's subsequent turns immediately; resetting a definition resets
that baseline. Explicit resume is required. Cadence re-anchors at transfer time.

Goal set, pause, clear and resume each take a required external mutation id
(see Mutation ids in api.md); the DTO link in Core api/goals.rs is the only
field catalog. The CLI supplies the global --id to all four. Each applies its
state change and retains the original GoalView under the same id in one
IMMEDIATE transaction: an exact retry returns the stored view without
re-applying the transition, so the stored view survives intervening
resume/set/clear; a different binding is id_conflict. The stored view is
allowed MAX_FRAME_BYTES (its definition alone may reach 64 KiB); the
persisted-event MAX_EVENT_BYTES budget never caps the stored reply, and a
serialization over the frame bound fails before any write.

Rounds name a key in one of two forms, and the form must match the Goal.
Idle keys name the exact idle event seq the caller observed; slot keys name
an anchored cadence slot of a cadenced Goal. Before work, the round reads
Agent state, pending requests, source health and unknown gates, and refuses
unless the Agent is cleanly idle at that key: idle state, no active turn, no
pending requests, every open harness source healthy (`daemon`/`os`/`git`/
`workspace_process` monitors stay advisory), no unknown command, no open
command, and the key is fresh — an idle key names an idle event of that
Agent generation and is still its latest, a slot key names a due slot. A
turn budget already spent outside any round blocks without running the check. A
budget spent solely by the Goal's own last admitted continuation is not outside
any round: that round still runs its final check, and the postcheck budgets
decide (success completes even on the spent budget; a failed check admits no
further continuation and is decided under the existing postcheck ordering:
round budget, then turn limit, then no progress).

Before the check runs, the round claims a durable single-flight marker for
`(agent,generation,mode,revision,round_key,round_key_kind)` in the same
Immediate transaction that re-validates the probe. A second `goal round` for
the same Goal while a marker is running is refused without running anything.
The final conditional commit requires that exact marker. Before any external check,
the claim transaction also reserves the request id in the existing `writes` table,
bound to the principal, `goal.round` and canonical Agent/mode/key inputs. It checks
both command/write namespaces and validates the age of an absent id. The reservation
captures the generation, revision, key, holder and a distinct continuation ULID.
The existing `<id>/send` admission requires a command parent; a round reservation
is a write, so its continuation uses the frozen ULID through ordinary SEND admission.
The [Goal DTOs](../../crates/eliot-core/src/api/goals.rs) bind the response to the
round id and the optional continuation separately.

Retained round replies and errors are resolved before current Goal gates: an exact
retry never reruns the check or dispatches a continuation again, even after later
Goal changes. A live duplicate is `busy`; a different binding is `id_conflict`.
The final transaction consumes the marker, updates the Goal, admits any continuation
and replaces the reservation with the full original response rendered from that
same transaction connection. Stored round records use `MAX_FRAME_BYTES`, including
their response; they are not subject to the smaller persisted-event budget.

The check is the frozen explicit argv from `goal set`, run through the shared
owned-process custody outside the Store lock with a bounded timeout, a small
round-context stdin and the daemon environment. Both check streams are
secret-redacted into one bounded observation: labelled `[stdout]` and `[stderr]`
sections, each label stating that stream's own completeness (stderr is the
capture's bounded tail), and each section keeping its beginning and end around an
explicit omission marker when it exceeds its share. The output digest covers
both streams, because many checks report only on stderr; same output counts
toward no-progress only when both streams were captured in full. A continuation presents
the observation as diagnostic data, never as authority to change the Goal, its
check or the task scope, and never asks the Agent to rerun the check to read
it. The commit then
applies **all** results in one transaction only if generation, revision and
round still match — the key unconsumed, no newer Agent event (past an idle
key, or since the claim for a slot), sources still healthy, no open or
unknown command, no active turn, no pending request — together with an
optional ordinary `send` continuation admitted by the same transaction:
completion on check exit 0, `round_budget`, `turn_limit` or `no_progress`
block, or one continuation. Success always completes, even on a spent budget.
If the Goal's own continuation is rejected before native dispatch, that
rejection and any resulting context rejections cannot supply a new idle key
for the same Goal. A later eligible Agent event can still drive a round.
An unknown check outcome (spawn/timeout/custody failure, or a check with no
exit code such as a signal kill) commits no Goal progress or continuation, and
neither does a commit that finds the Agent left idle during the check: the marker goes `unknown`
and that round key is never rerun — the next round needs a new key. After a
crash the marker stays `unknown`. Abandonment burns its marker and retains the
original sanitized error in one transaction. A reservation from a crashed holder
is durably resolved to `unknown` on readback, without another check. Claim also
preflights a bounded unknown recovery record. If the full error record exceeds
the frame bound, abandonment retains and returns that unknown result atomically
with the marker burn, rather than leaving a live running marker. Cancellation
uses the existing owned-process cleanup before abandonment. The claimed-round guard
owns the same check future passed to the existing process supervisor. Caller/RPC
drop signals per-round cancellation and transfers that future to the finalizer,
which awaits process/custody settlement before burning the marker; it never starts
another invocation. Global cancellation is forwarded to the same per-round channel.
While the check or its cleanup remains in flight, the running marker excludes every
other round, including another due cadence slot. If cleanup reports unproven
quiescence, both normal completion and the drop finalizer retain the original
request error in `writes.result` without burning the running marker. Exact retries
return that retained error; other rounds stay excluded even across later Goal
changes. Request outcome and process-family closure are independent. The current
process API provides no later closure receipt after an unproven outcome, so the
live holder conservatively keeps exclusion; returning from the supervisor is not
closure proof. A proven closure may burn the marker without replacing the retained
error. Drop while awaiting the final Store lock also carries the observed custody
fact and cannot turn an unproven outcome into a release. If runtime shutdown prevents finalization,
a new holder resolves the reservation to `unknown`; this is not cleanup proof.
A persistence failure faults the Store and leaves the reservation unresolved; it
never authorizes rerun. Neither a failed check nor a missing
terminal fact authorizes replay: a refused or unknown round admits no send.

**Change scope.** A Goal may declare `scope` (globs of the paths its work may change)
and `check_inputs` (globs of the check's own inputs). Both use the claim glob syntax,
relative to the Agent workspace, which must be its exact Git worktree root. With
either one, `goal set` records the starting point before the Store write: HEAD and a
content digest of every path that already differs from it (index, worktree and
untracked files; ignored files excluded; renames as both paths). More than 512 such
paths are refused rather than recorded incompletely. A changed submodule refuses the
starting point, or leaves a round's change set unverified, rather than hiding edits. Files named by the check argv
inside the workspace join the check inputs. An exact retry is answered from the
retained result before the workspace is read again. Each round, before the check, the
daemon compares the workspace with that starting point. The Goal's work is the paths
committed since the starting HEAD, the paths changed now, and every pre-existing change
whose content moved; a pre-existing change left unchanged and uncommitted is not the
Goal's work. A change outside `scope` blocks the round as `scope_violation` without
running the check or admitting a continuation. A passing check is measured again, so
files a check writes count too; they belong in `scope` or in Git ignore rules. The pass
completes only when that change set is verified, stays inside `scope` and leaves every
check input untouched. Otherwise it blocks as `scope_violation`, as `checker_changed`
with the changed inputs listed, or as `scope_unverified` with the reason, so the Owner
reviews it separately. The observation lists at most 20 paths. A
failing check continues as before. Its continuation repeats the `scope` line, so the
limit survives every round. Swarm never reverts, resets or deletes anything for a
scope finding. Pause and resume keep the starting point. A blocked Goal continues only
through a new `set` after the Owner reviews the finding; that `set` records a new one.
`goal show` reports the starting HEAD, the number of pre-existing changes and the
argv-named check files.

**Cost budget.** `max_cost {unit, amount_micros}` budgets the Agent's native
session cost since `set`, in that verbatim unit (Claude reports `usd_estimate`; units
are never converted, and subscription quota is a separate resource). At `set` the Store
records a baseline from the generation's latest `session_usage` snapshot in that unit.
An Agent that has run no turn starts from zero and binds the first session it sees.
`set` is refused as `unsupported` when the harness reports no session cost, reports
another unit, or ran turns without one. After each failed check, the round commit reads
the latest snapshot. The spent cost is that cumulative amount minus the baseline. It
counts only for the same native session and unit, from a snapshot observed no earlier
than the last completed turn, and never with a decreasing cumulative. Anything else is
`cost_unverified`, never zero, and blocks the Goal: an Owner budget that cannot be
checked does not silently become unlimited. A spent cost at or above the budget blocks
as `cost_limit`. A passing check still completes. The budget is checked between turns,
so it limits new continuations but cannot cap a turn already running. Snapshot and
turn times share the host clock: plugins are local processes, and Claude commits
`session.usage` after its `turn.completed`. A snapshot without a source cannot prove the
same session against a non-zero baseline. A handoff does not carry the baseline,
because it belongs to the source Agent's session: the destination's next failed round
blocks as `cost_unverified` until the Owner sets the Goal again.

**Diagnosis and escalation.** A failed round whose fully captured check output repeats the
previous round's counts toward `consecutive_same_failure`, whatever the workspace did.
A new output resets it. An output captured only in part resets it too, and clears the
compared digest, so neither this streak nor the no-progress streak spans the gap. The second identical failure in a row
means two approaches failed the same way. That round's continuation, once per Goal
revision, is the diagnosis step: compare the attempts, name the shared cause, run one
observation that tells the remaining causes apart, then take one different approach.
A Goal may carry one Owner-authorized `escalation {model?, effort?, reason}`. `set`
refuses it unless the Agent's harness can apply that `configure`. When the same failure
repeats after the diagnosis step, the round commit admits that one `configure` (Owner
principal, the Goal's generation) immediately before the continuation, in the same
transaction. The settings lane therefore dispatches the configure first, and the
continuation names the requested change and the Owner's reason (a refused configure
leaves the old model, and the text says the switch applies only if accepted). An
escalation the Store cannot admit at that round blocks as `escalation_refused`. That
round still commits, so its budgets advance. It happens at most once per revision. There is no model ladder, no change of authority fields, and nothing to
replay: an `unknown` configure holds the lane as usual. The Router derives the stage
from the counters bound to the claimed revision, and the commit re-derives it,
refusing a mismatch as `stale_round`. The output digest is the stage's notion of "the
same failure": a check that prints a changing value, such as a timestamp, never repeats.

`goal show` reports the exact status, counters (with a live turn count and the spent
cost), budgets, limits and the unresolved round marker if any. `cost_status` is
`untracked` without a budget, otherwise the spent and budgeted amounts or why the spent
cost is unverified.

A cadenced Goal carries `cadence_ms` (one minute to seven days) and rounds
only on anchored slots. The anchor is captured when the cadence is set, stays
stable across pause and resume, and is re-captured by a new definition. Slots
are 1-based: slot `k` starts at `anchor + k * cadence_ms`, so the first slot
starts one period after the set. A slot round names a due slot (its start
passed); future slots are refused, and the last committed slot is persisted:
any slot at or below it is refused at probe, claim and commit, and each
burned slot still needs a newer key next time. Slot rounds are explicit; no
time-based driver fires them.

The daemon also drives active Eliot-mode Goals automatically on idle: when a
generation commits an idle event, the driver starts one round with the event
seq as an idle key through the same `goal round` path — same probe, gates,
check custody and conditional commit — under a fresh owner mutation id. It
skips cadenced Goals (slot-keyed), paused or terminal Goals, drifted
generations, and anything while automation is draining; it waits while an
active native Goal drives. Each automatic round runs in its own task, at most
one per Goal and four Goals at once, so a slow check never stalls another
Goal's round; a newer event for a Goal with a round in flight is driven as
a follow-up when that round resolves. Bus events dropped past the cap or to
bus lag are logged, as are queued waits and stale rounds. Other races
against explicit rounds resolve through the
single-flight marker and the burn and stay silent. Shutdown cancels every
in-flight automatic check through its custody cancellation and waits only for
the rounds to resolve their markers (`unknown`, never rerun).

Setting or resuming an enabled idle-keyed Goal also reconciles the
retained exact-generation idle subject: the set/resume wakes the driver,
which sweeps every active non-cadenced Goal bound to its Agent's current
generation and reserves a round on the latest event only when that row
still names an idle event of that generation. The sweep uses the same
reservation as a bus event — same scope, enablement and single-flight —
without re-offering mail, and waits for a real event when the
subject is missing (pruned, superseded or never retained), leaving the
Goal's marker and counters untouched on the readback. A spent subject is
skipped before taking a scheduler slot, so burned keys cannot starve
eligible Goals past the cap; a retained subject that finds its Goal busy
or the cap full waits in revision-bound order and is revalidated when a
round resolves. A busy Agent or a
drifted generation skips silently; a set/resume never resumes a lost
Agent, and only the round path itself may pause a drifted Goal. Startup
performs no sweep: the restart marks every owned session lost, so no
retained subject is eligible.
A consumed idle subject rearms only through a new Goal revision: a
definition change clears the committed key, so the next sweep may round
the still-current subject again. Unknown-burned keys never rerun within
the generation, and the same revision never rerounds a consumed key —
repeated set/retry, reads and daemon restart cannot create another
round.

No-progress compares check output digests and the workspace fingerprint
together. The fingerprint is HEAD plus the Git monitor's content `dirty_digest`
over the dirty set (path, status and file content of files Git does not ignore,
within the monitor's caps), read from the round's workspace while the Agent is
idle. Just before each check the daemon reads it and compares it with the read
taken right after the previous failed check, so a shell edit of an already dirty
file counts as progress even when no edit is attributed, and files the check
itself writes never do. A failed read, a read past the caps or a workspace that
is not an exact Git root is incomplete; past the caps every round is incomplete
and only the round and turn budgets stop the Goal. Without a Git binding for the
Agent's generation the streak falls back to check output alone. An incomplete
fingerprint never proves no progress: it resets the streak instead of extending
it. A change by any process to a file Git does not ignore counts; the streak
exists to stop blind repeats, not to attribute work.

Checks are arbitrary explicit commands, not magically read-only. Check outcomes
use the same frozen inputs/custody/no-blind-retry rules. Incomplete
observations never prove no progress.

## Durable tables

The [Store migrations](../../crates/eliot-store/src/schema.rs) own the
`rule_state`, `timers`, `goals` and `goal_markers` schemas. They persist
revision-bound rule traversal, scheduling and Goal round reservations.

`goal_markers` holds one row per unresolved round: `running` while its check
runs, `unknown` once the round ended without a commit. The final commit
consumes its marker; an unknown marker burns its round key, which is never
rerun. The burn ignores revision but separates key forms: a burned idle seq
never blocks the same slot number. A `running` row whose holder is gone
(crash) reads as `unknown`.

Goal definition/state retain text/check, budgets, counters, the cadence
anchor, the last committed slot and the last workspace fingerprint alongside
previous progress observations. Action state lives in commands. Rate accounting counts already admitted
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
