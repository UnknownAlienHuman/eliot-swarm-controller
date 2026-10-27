# Automation

Automation turns committed events and time into ordinary Eliot commands. It has no
Task/review/workflow engine and no generic run/action state machine.

The implementation follows two donor patterns:

- controller-runtime: restart and callbacks enqueue a stable reconciliation key; each
  pass rereads durable current state and one key never reconciles concurrently with
  itself;
- Petri: validate against current state, commit the record, then apply the pure reducer.

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
  enabled, updated_at
}
```

- new rule: capture current event high-water H; current cursor starts at H;
- semantic edit: in one transaction capture H, move old definition/cursor to retiring
  through H, install new definition with cursor H and increment revision;
- retiring definition processes `(retiring_cursor,H]`; current processes events after H;
  when retiring reaches H it is discarded;
- identical reload is a no-op;
- disable/pause records one skipped range and advances the relevant cursor without
  firing;
- Store failure advances nothing.

Semantic edits are serialized per `(principal,rule)`. While a retiring definition
exists, another changed file is **not** installed and no third definition row is
created. The config source reports the rule as `deferred`; after retirement the
reconciler rereads the latest valid file and performs one new handoff. Intermediate file
versions may therefore coalesce, but no retained event is reinterpreted by an arbitrary
newer definition.

This is the whole edit model—no activation graph, compiled-definition hierarchy or
run journal.

For each event under its owning definition:

- no match: advance cursor;
- valid firing: create all deterministic command rows and advance cursor in one
  transaction;
- deterministic render/authority/capability/fanout error: commit one `rule.failed` and
  advance cursor;
- Store failure: rollback and retry the same event after Store recovery.

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
Timer rows are reconciled on startup; one rule does not stack another timer firing while
its previous commands are queued/dispatched/unknown.

Changing a schedule increments its revision and starts after the reload boundary.
Changing actions only keeps the schedule and applies the new definition to later slots.
A schedule edit is also serialized behind an existing retiring definition.

## Matching and rendering

`match` uses dotted paths in the committed `{event,subject,data}` snapshot: exact,
glob, regex, one-of and presence. All entries must match.

`${path}` may fill an argument **value** only. It cannot choose executable, action kind,
cwd root, environment-variable name, option name, credential, project or control scope.
Unknown placeholder is a deterministic failed firing.

Selectors resolve transactionally at firing to concrete Agent ids and captured
`generation`; delayed work never retargets a replacement session. Steer/interrupt and
request replies also freeze exact turn/request id.

## Actions are commands

Each action is a command with id:

```text
rule:<principal>/<rule>@<revision>:<trigger-key>/<index>[/<agent>]
```

The same trigger can therefore be reconciled repeatedly without duplicate effects.
Actions execute in listed order: later commands stay queued until the previous command
is applied; rejected/failed/unknown stops the remaining commands.

| Action | Implementation |
| --- | --- |
| any core verb | normal generation/turn/request-bound command through the router |
| `git.fetch|pull|push|worktree` | daemon command with explicit repo/remote/ref/OID/path and verb-specific readback |
| `msg.send`, `var.set` | normal coordination mutation under rule principal |
| `run` | bounded managed process command |
| `notify` | bounded log/desktop or owner-configured named webhook command |
| `emit` | persisted `x.user.*` event command |

Git action arguments are data, not command-line syntax:

- `repo`/`workspace` resolves to an owner-registered project path allowed to the rule;
  project rules cannot select arbitrary cwd;
- `remote` is an owner-configured remote name, never a URL from event data. Reject empty
  names, leading `-`, control characters, `::`, `ext::` and any value whose exact
  `git remote get-url --all` result uses an unapproved remote-helper scheme;
- refs are full validated refnames (`git check-ref-format`) and may not begin with `-`;
  OIDs are exact hexadecimal object ids read from Git, never free-form placeholders;
- commands place `--` before pathspecs where supported and use explicit refspec argv;
- a placeholder-filled remote/ref/path must pass the same validation after rendering;
- monitoring/action Git calls disable hooks, fsmonitor, external diff and textconv as
  specified in [monitoring](monitoring.md#git-and-filesystem-monitor).

No unknown command is run again. Git push readback compares the exact intended local OID
with the exact remote ref after the process tree is gone; it never pushes again merely
to discover the answer. Generic scripts/webhooks without exact readback remain unknown.
A narrow repository writer hold prevents a later write from passing an older unknown
write.

## Frozen script inputs

A rule definition stores for every referenced project note/script:

```text
FrozenFile { canonical_path, size, digest, allowed_root }
```

Paths must remain under the registered project/global script root and may not traverse
`..`, symlink/reparse-point escape, alternate streams or devices. Dispatch rereads and
verifies size/digest; mismatch fails rather than running different bytes. Small generated
system text may be copied directly into the definition row. There is no global artifact
CAS or garbage collector.

A project `run` uses a frozen script under the registered `.eliot/scripts` root and an
owner-approved interpreter; it cannot choose an arbitrary executable. A global rule may
use only an executable/argv declared in owner configuration.

A `run` command receives one JSON object on stdin and may return bounded NDJSON action
objects. The daemon buffers all output, validates every action under the parent rule
principal, then creates their command rows atomically. The process receives a scoped
run token; losing it never becomes owner. Timeout kills/verifies its custody. Lost
outcome is unknown and the script is not rerun.

## Approvals and hooks

Asynchronous `approval.requested`/`question.asked` rules create ordinary exact-generation
request commands. A stale generation/request is rejected.

Synchronous hook policy is evaluated inside the native deadline. A non-default decision
is returned only after its immediate durable decision record commits while the same
request remains pending. Timeout/Store failure returns safe native default and a late
result is discarded; it never becomes a queued approval for another request.

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

Generation change pauses the Goal. Every `goal.set`, `goal.resume`, replacement of its
text/check/budget/cadence, and explicit rebind increments `revision`. Pause/clear also
increment it so an in-flight round can never publish a late result.

Round identity:

- `on_idle`: the exact committed `agent.idle` event seq;
- cadence: the exact anchored schedule slot.

Command id is `goal:<agent>@<generation>:<revision>:<round-key>`. Restart cannot send the
same round twice; a later real idle event/slot can create another.

Before a round:

1. reread current generation, Goal revision, Agent state, pending requests, source
   health, unknown input/turn/commands and native Goal status;
2. skip/block if any exact gate forbids work; `on_idle` waits for active native Goal;
3. run bounded check/fingerprint read-only;
4. compute pass/budget/no-progress/continuation outcome without mutating Store;
5. in one conditional transaction, reread generation, Goal revision and round key;
6. only when all still match may the transaction commit **any** round result: `done`,
   `blocked`, progress counters/fingerprints, budget usage or the ordinary continuation
   send. Otherwise the stale result is discarded without changing the replacement Goal.

An incomplete fingerprint never counts as no progress. A dispatched/unknown input or
turn blocks continuation. Goal checks/scripts use the same frozen-file and custody
rules as other managed commands.

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

Action state lives in `commands`; no `automation_runs` or `automation_actions` tables.
Recent `rule.fired|failed|suppressed` events provide history/rate accounting. Current
rule/timer/Goal keys are level-reconciled after restart.

## Guardrails

- cause depth > 5 suppresses firing;
- default per-rule 10 firings/minute and global 120 actions/minute, reconstructed from
  retained events/current command rows;
- self-send/steer/inject from the Agent's own event requires `allow_self=true`;
- draining/paused automation admits no new firing/Goal continuation;
- every action passes the normal router again at dispatch.

Workflow examples remain small rule/script recipes, never core product states.
