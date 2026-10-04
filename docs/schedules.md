# Scheduled CheckRuns

The controller keeps the existing operator-authored interval registry and
manager-owned calendar automations on one Store scheduler loop. Both admit only
the closed `check_run` action through the normal CheckRun path. Neither accepts
an arbitrary method, shell command, model prompt, or runtime-selected target.

## Manager-owned calendar automation

A registered manager configures a calendar through `automation.config.apply`.
The automation entry's existing `enabled` field is the only enable switch;
`cron` contains the calendar and the exact CheckRun action:

```json
{
  "project_id": "project-id",
  "changes": [{
    "automation_id": "weekday-source-check",
    "expected_revision": 0,
    "include_existing": false,
    "patch": {
      "enabled": true,
      "steps": ["check_run"],
      "cron": {
        "calendar": {
          "expression": "0 0 9 * * 1-5",
          "timezone": "America/New_York",
          "anchor_ms": 1790899200000
        },
        "action": {
          "kind": "check_run",
          "attempt_id": "01930000-0000-7000-8000-000000000001",
          "expected_task_revision": 4,
          "candidate_ref": "source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "profile_id": "strict",
          "profile_revision": "v1"
        }
      }
    }
  }]
}
```

The expression uses Croner's six-field syntax, including seconds. The timezone
must be an IANA timezone known to the timezone database. `anchor_ms` is an
inclusive lower boundary: occurrences before it are ignored, and a
non-second-aligned anchor advances to the first matching whole-second
occurrence after the boundary. `automation.config.explain` returns the calendar
generation, next due instant, a read-only preview of the next three occurrences,
the last considered occurrence and its Operation receipt.

Calendar evaluation and timezone/DST behavior come from the maintained Croner
and `chrono-tz` libraries, not a controller-specific cron parser. A fixed local
time in a spring-forward gap runs at the first valid local instant after the
gap. A fixed local time in a fall-back overlap runs only at its first matching
instant. Expressions that match repeated wall-clock times, such as every
minute, follow Croner's occurrence behavior for each real instant.

The manager's selection pins one current open Attempt, task revision, captured
source snapshot and exact check-profile revision. The scheduler validates that
target before planning and again in the final admission transaction. The
normal CheckRun worker rechecks the committed automation and target before it
starts. A different Attempt, candidate, task revision or profile revision
requires an explicit automation config edit.

`include_existing` controls the activation cut for a newly enabled calendar or
changed selected action. `false` starts at the apply time and skips already-due
occurrences. `true` permits only the latest occurrence already due at that cut.
It never replays a backlog. Disabling the automation removes its due index;
reenabling it applies the same `include_existing` rule. An in-flight or
outcome-unknown CheckRun remains under the normal Store readback path and holds
later calendar work. The scheduler does not resend an unknown effect.

The occurrence identity is derived from the original manager, project,
automation, calendar generation and intended UTC due instant. Config revisions,
display labels, and the current successor manager do not change that identity.
The Operation receipt, on-behalf attribution, occurrence record and cursor are
committed atomically. Explicit A→B→C automation transfer relocates only the
current due-index owner; it preserves the original manager provenance, cursor,
pending work and historical Attempt owner. A different manager's own entry
cannot gain access to that Attempt merely by selecting it in its calendar.

## Operator interval registry

The existing `[[schedules]]` TOML entries remain available for fixed-time and
fixed-interval CheckRuns:

```toml
[[schedules]]
schedule_id = "nightly-candidate-check"
enabled = true
anchor_ms = 1790899200000
period_ms = 86400000

[schedules.action]
kind = "check_run"
attempt_id = "01930000-0000-7000-8000-000000000001"
expected_task_revision = 4
candidate_ref = "source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
profile_id = "strict"
profile_revision = "v1"
```

`anchor_ms` is a Unix epoch-millisecond timestamp. Omitting `period_ms` makes
the entry one-shot; an interval must be positive and remains anchored to the
configured instant. IDs are unique lowercase identifiers, bounded to 64
entries. An ID's anchor, period and action are immutable after first use; define
a new ID for different semantics. The existing interval slot and receipt
identity format is unchanged. Operator intervals do not accept cron
expressions; manager calendars use the automation entry described above.

## Catch-up, persistence and supervision

Both schedule types consider only the latest due occurrence. Missed intervals
or calendar instants collapse to one latest-only candidate. Wall time chooses
the due instant; a monotonic timer waits for it, and a bounded clock recheck
observes wall-clock jumps. Every pass rereads Store state before admission.

Manager calendar definitions, per-generation cursors, due indexes, held
occurrences and per-occurrence records live in versioned Store `meta` records.
No second scheduler service or cron-specific database is added. Changes and
terminal CheckRun readback wake the shared scheduler so held work can be
re-evaluated promptly. `host.status.schedules.items` continues to report the
operator interval registry; `automation.config.explain.cron` reports one
manager entry's calendar state without advancing its cursor or admitting work.

The existing CheckRun supervisor remains responsible for worker launch,
completion and readback. The scheduler only admits typed Operations and
retains their receipts. It is not a GM, observer, runtime module, second broker,
or model timer.
