# Scheduler source isolation

Interval schedules, manager calendars and Goal reminders retain separate due
outcomes. Cursor advancement establishes progress; invoking a reconciler alone
does not. Recognized subject damage carries an exact source pointer and digest.
Independent due work runs before CheckRun recovery. Four closed recovery
receipt errors return a bounded degraded diagnostic; its `error_digest`
fingerprints the error and its `boundary` names recovery, without claiming a
specific CheckRun row.

Cron candidate classification uses the committed-entry producer codes. Missing
prerequisites remain held with their original due time. Recognized corruption
retains a digest of the actual stored JSON bytes and removes that exact due row
by compare-and-swap in the domain transaction. Unknown errors, SQL failures,
transaction failures and errors with secondary causes remain fatal.

On 2026-10-10 the frozen five-file candidate passed scoped rustfmt, whitespace
checks and Windows Rust 1.98.1 host production Clippy with `-D warnings`.
Host test targets compiled with zero errors and 21 existing warnings. Added
regressions cover pending retry, exact noncanonical JSON evidence, CAS conflict
rollback, fatal infrastructure errors and truthful source outcomes. Execution
awaits the complete source and Clippy assembly. Product status remains
`PARTIAL_PROGRESS`; full and native/model qualification are pending.
