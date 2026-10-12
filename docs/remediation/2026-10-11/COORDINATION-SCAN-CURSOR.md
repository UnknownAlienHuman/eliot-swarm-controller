# Bounded coordination list continuation

`coordination.thread.list` and `coordination.watch.list` accept `after_scan_cursor`
and return `next_scan_cursor` when a bounded index window has more rows. Send the
returned token unchanged on the next call with the same authenticated caller,
Task and Attempt scope. Prefer this cursor to the legacy Thread/watch ID cursor:
it can advance past missing, unauthorized or damaged entries that have no usable
retained ID. Supply one cursor kind per call.

The token is a read position, not a capability. Store validates its current caller
and exact index prefix, then checks every projected record with the existing read
authority rules. Tokens from another actor or index scope are rejected. Tokens
are versioned, canonical UTF-8 hex and bounded to 16 KiB; no cursor state is
written or retained. Changing state/topic filters starts a new traversal unless
the caller intentionally wants to continue after the same index position.

A malformed own watch list key produces a row-local damaged-entry gap. A window
with later rows still returns `next_scan_cursor`, even when all examined entries
were damaged. Thread scan windows similarly advance past unprojectable entries.
Legacy `after_thread_id`/`after_watch_id` remain valid for retained authorized IDs.
Their response counterparts may be null when the last scanned position has no
such ID. Thread byte-budget trimming rolls both cursor forms back to the last
returned item, so it cannot skip a byte-excluded item.

This is bounded source behavior. Full installed and model qualification remains
tracked in the existing V27 qualification plan.
