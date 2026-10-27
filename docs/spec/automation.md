# Automation

Automation turns committed events and time into ordinary Eliot commands. It has no
Task/review/workflow engine and no generic run/action state machine.

The implementation follows two donor patterns:

- controller-runtime: callbacks/restart enqueue a stable reconciliation key; every pass
  rereads durable current state and one key never reconciles concurrently with itself;
- Petri: validate current state, commit the event/commands, then apply the pure reducer.

## Rule files and authority

Rules live in:

- `<data_dir>/rules/*.toml`: global owner automation;
- `<registered-project>/.eliot/rules/*.toml`: project principal, limited to that
  project and its allowed global profiles/actions.

```toml
[[rule]]
id = "push-writer-commits"
on = { event = "git.commit", match = { "agent.labels.role" = "writer" } }
do = [{ action = "git.push", workspace = "${agent.workspace}" }]
limit = { max = 3, per = "agent", window = "1h" }
```

Required keys: `id`, `on`, `do`; optional: `enabled`, `limit`, `catch_up`,
`allow_self`. Unknown keys and duplicate ids are load errors. Invalid reload keeps the
last valid definition active and reports config health.

A project rule cannot target another project, global vars, arbitrary cwd/executable,
an unapproved webhook, account, proxy or profile route.

## Event cursor and compact definition handoff

Event rules read the persisted event log in bounded pages. Matching and rendering use
the event's stored `subject`, never the current Agent row.

One `rule_state` row keeps the current definition and, only during an edit, one retiring
definition:

```text
RuleState {
  principal, rule,
  revision,
  current_definition, current_cursor,
  retiring_revision?, retiring_definition?, retiring_cursor?, retire_through?,
  enabled, paused_reason?, updated_at
}
```

- new rule: capture current event high-water H; current cursor starts at H;
- semantic edit: in one transaction capture H, move old definition/cursor to retiring
  through H, install new definition with cursor H and increment revision;
- retiring definition processes `(retiring_cursor,H]`; current processes events after H;
  when retiring reaches H it is discarded;
- identical reload is a no-op;
- disable/pause records one skipped range and advances relevant cursors without firing;
- Store failure advances nothing.

Semantic edits are serialized per `(principal,rule)`. While a retiring definition
exists, another changed file is not installed and no third definition row appears. The
config source reports `deferred`; after retirement the reconciler rereads the latest
valid file and performs one new handoff. Intermediate file versions may coalesce, but no
retained event is reinterpreted by an arbitrary newer definition.

The watcher tracks every frozen note/script referenced by current or retiring
definition. Size/digest change is a semantic edit and uses the same serialized handoff.
A missed watcher event is caught by mandatory dispatch-time digest verification.

This is the whole edit model—no activation graph, compiled-definition hierarchy or run
journal.

For each event under its owning definition:

- no match: advance cursor;
- valid firing: create all deterministic command rows and advance cursor in one
  transaction;
- deterministic render/authority/capability/fanout error: commit one `rule.failed` and
  advance cursor;
- Store failure: rollback and retry that event after Store recovery.

### Retention and bounded backlog

Event retention may delete only through the minimum cursor needed by every enabled
current/retiring definition. A retiring definition pins only through its captured
`retire_through` boundary. Disabled rules pin nothing because disable advances and
records the skipped range.

`automation.max_event_backlog` (default 100,000 events) bounds one rule's lag. When any
required cursor falls more than that many retained events behind current high-water, one
transaction:

1. records `rule.suppressed reason=backlog_exceeded` with exact skipped range;
2. advances current and retiring cursors to the captured high-water;
3. clears the retiring definition;
4. sets the rule disabled/paused with `paused_reason=backlog_exceeded`.

Already admitted commands continue; no new firing is created. Retention is then free to
advance. Owner must inspect and explicitly re-enable or run an explicit retained-range
catch-up; catch-up fails `history_gap` if the range no longer exists. There is no silent
skip and no indefinitely pinned database.

## Triggers

| Trigger | Key |
| --- | --- |
| persisted event | event `seq` + definition revision |
| `every` | anchored schedule slot + schedule revision |
| cron | scheduled UTC instant + schedule revision |
| `at` | UTC instant + schedule revision |
| manual | external mutation id |

Ephemeral events cannot trigger durable work.

Intervals use `anchor + k·period`; missed slots coalesce to latest or none, never burst.
Cron persists UTC instants and handles repeated/skipped DST local times explicitly.
Timer rows reconcile on startup; one rule does not stack another timer firing while its
previous commands are queued/dispatched/unknown.

Changing a schedule increments its revision and starts after reload boundary. Changing
actions only keeps schedule and applies the new definition to later slots. Schedule edit
is serialized behind an existing retiring definition.

## Matching and rendering

`match` uses dotted paths in committed `{event,subject,data}`: exact, glob, regex,
one-of and presence. All entries must match.

`${path}` may fill an argument value only. It cannot choose executable, action kind, cwd
root, environment name, option name, credential, project or control scope. Unknown
placeholder is a deterministic failed firing.

Selectors resolve transactionally at firing to concrete Agent ids and captured
`generation`; delayed work never retargets a replacement. Steer/interrupt and request
replies also freeze exact turn/request id.

## Actions are commands

Each action is a command with id:

```text
rule:<principal>/<rule>@<revision>:<trigger-key>/<index>[/<agent>]
```

Repeated reconciliation therefore returns the same command rows. Actions execute in
listed order: later commands stay queued until the previous command is applied;
rejected/failed/unknown stops the remainder. `Applied` keeps the underlying command
meaning—an applied send is associated with a turn, not proof the turn succeeded. A
workflow needing its result uses a later rule on exact `turn.completed`/cause; Eliot
adds no hidden wait/workflow state.

| Action | Implementation |
| --- | --- |
| any core verb | normal generation/turn/request-bound command |
| `git.fetch|pull|push|worktree` | daemon command with frozen desired state and verb-specific readback |
| `msg.send`, `var.set` | normal coordination mutation under rule principal |
| `run` | bounded managed process command |
| `notify` | bounded log/desktop or owner-configured named webhook command |
| `emit` | persisted `x.user.*` event command |

### Git actions

Arguments are data, not command-line syntax:

- repo/workspace resolves to an owner-registered project path allowed to the rule;
- remote is an owner-configured remote name, never a URL from event data. Reject empty,
  leading `-`, control characters, `::`, `ext::` and option-like values;
- at admission freeze effective endpoints. Fetch/pull inspect every
  `git remote get-url --all`; push separately inspects every
  `git remote get-url --push --all`, because `pushurl` may differ. Every URL must use an
  owner-approved scheme and no unapproved/custom helper. Revalidate unchanged before
  dispatch;
- refs are full `git check-ref-format` names and may not begin `-`; OIDs are exact
  hexadecimal object ids read from Git;
- put `--` before pathspecs where supported and use explicit refspec argv;
- monitoring/action Git calls disable hooks, fsmonitor, external diff and textconv.

Each command stores one immutable desired-state intent before process start:

- fetch: configured full fetch refspecs, exact advertised remote OID per selected source
  ref and corresponding local tracking ref. Fetch exact frozen OID/refspec, not a later
  moving ref;
- pull: current branch, upstream full ref and intended upstream OID; only fetch +
  fast-forward to that OID, never merge/rebase/another branch;
- push: source full ref, its local OID, destination full ref and every effective push
  URL. If omitted, resolve current branch/upstream; detached HEAD, missing/ambiguous
  upstream or non-fast-forward intent fails. Use explicit
  `<source-ref>:<destination-ref>`, never force;
- worktree: repository, exact path, branch and base OID.

Unknown is never rerun. After custody is gone, readback proves applied only when:

| Action | Exact applied state |
| --- | --- |
| fetch | every intended tracking ref equals its frozen remote OID |
| pull | intended local branch HEAD equals frozen upstream OID |
| push | destination ref at every frozen push URL equals frozen local OID |
| worktree | exact path is registered on frozen branch at intended OID |

Otherwise action remains unknown unless native/process result proves no effect. Push
readback uses frozen URLs directly, not mutable remote name. A narrow repository writer
hold blocks later writes behind an older unknown write. Generic scripts/webhooks without
exact readback remain unknown.

## Frozen script inputs

A definition stores every referenced project note/script as:

```text
FrozenFile { canonical_path, size, digest, allowed_root }
```

Reject `..`, symlink/reparse escape, alternate streams and devices. Dispatch verifies
size/digest; mismatch fails. Small generated text may be copied into definition row. No
global artifact CAS/GC.

A project `run` uses a frozen script under registered `.eliot/scripts` plus an
owner-approved interpreter; it cannot choose arbitrary executable. Global rules use only
owner-configured executable/argv.

`run` receives one JSON object on stdin and may return bounded NDJSON actions. Daemon
buffers all output, validates under parent rule principal, then creates command rows
atomically. Process receives scoped run token; losing it never becomes owner. Timeout
kills/verifies custody. Lost outcome is unknown and script is not rerun.

## Approvals and hooks

Asynchronous request rules create exact-generation ordinary commands. Synchronous hook
policy runs inside native deadline. Non-default decision is returned only after its
immediate durable record commits while the same request is pending. Timeout/Store failure
returns safe default; late result is discarded and never queued for another request.

## Goal loop

A Goal is bound to `(agent,generation,mode,revision)`:

```text
Goal {
  agent, generation, mode: native|eliot, revision,
  text, check?, cadence,
  status, max_turns, turns_used, max_cost_usd?, cost_used?, no_progress,
  last_round_key?, last_fingerprint?, last_check_hash?, updated_at
}
```

Generation change pauses Goal. Every set/resume or definition change increments
revision; pause/clear also increment it so stale rounds cannot publish.

Round identity:

- `on_idle`: exact committed `agent.idle` event seq;
- cadence: exact anchored slot.

Command id: `goal:<agent>@<generation>:<revision>:<round-key>`.

Before a round:

1. reread generation/revision/state, pending requests, source health, unknown input/turn/
   commands and native Goal status;
2. skip/block if gated; on-idle waits active native Goal;
3. run bounded read-only check/fingerprint;
4. compute pass/budget/no-progress/continuation outside Store;
5. conditionally reread generation/revision/round key in one transaction;
6. only if unchanged commit **any** result—done/blocked, counters/fingerprints/budget or
   ordinary continuation send. Otherwise discard stale result.

Incomplete fingerprint never counts as no progress. Unknown input/turn blocks
continuation. Checks/scripts follow same frozen-file/custody rules.

## Durable tables

```sql
CREATE TABLE rule_state (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  revision INTEGER NOT NULL,
  current_definition TEXT NOT NULL,
  current_cursor INTEGER NOT NULL,
  retiring_revision INTEGER,
  retiring_definition TEXT,
  retiring_cursor INTEGER,
  retire_through INTEGER,
  enabled INTEGER NOT NULL,
  paused_reason TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(principal,rule)
);

CREATE TABLE timers (
  principal TEXT NOT NULL,
  rule TEXT NOT NULL,
  schedule_revision INTEGER NOT NULL,
  trigger TEXT NOT NULL,
  anchor INTEGER NOT NULL,
  last_key TEXT,
  next_due_at INTEGER,
  PRIMARY KEY(principal,rule)
);

CREATE TABLE goals (
  agent INTEGER NOT NULL,
  mode TEXT NOT NULL,
  generation INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  definition TEXT NOT NULL,
  state TEXT NOT NULL,
  last_round_key TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,mode)
);
```

Action state lives in commands; there are no automation run/action tables. Recent
rule events plus current commands provide history/rate accounting. Current keys are
level-reconciled after restart.

## Guardrails

- cause depth > 5 suppresses firing;
- default per-rule 10/minute and global 120 actions/minute, reconstructed from retained
  events/current commands;
- configured `limit.window` may not exceed retained rule-event accounting horizon;
- self-send/steer/inject from own event requires `allow_self=true`;
- draining/paused automation admits no new firing/Goal continuation;
- every action passes normal router at dispatch.

Workflow examples remain small rule/script recipes, never core product states.
