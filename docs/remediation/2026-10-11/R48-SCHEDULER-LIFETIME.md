# Independent scheduler lifetime and installed U observation

Fresh installed U ran once with binary-source commit
`9f2c789c7b1808998e8776b678a0232345b44305` and sidecar commit
`4585f158277de90639f335bf0fcd75cdac6ff5cb`. It completed at
`2026-10-11T09:34:30Z` with exit 0. Bootstrap, current-owner readiness,
linked one-shot CheckRun admission and host liveness passed. Applied-fault and
no-replay qualification remains **UNKNOWN**. The receipt is retained unchanged
at SHA-256 `f02516c1dabe9350ed4869647f544a3362a7ef4636d165094087d9866d07b98e`.
U, T and T2 are not replayed or retrospectively promoted.

U's first admit intent was written at `09:31:10.1539644Z` by worker PID 8232.
Store retained the one-shot CheckRun observation at `09:31:10.159Z` and the
automation worker's clean `dormant` state at `09:31:10.403Z`, with no error,
failure or restart. The CheckRun completed at `09:31:11.749Z`. No first-admit
terminal diagnosis, applied witness or second-admit marker exists. The owner
retained the same controller UUID, host epoch 2 and worker group PID, with
phase `departed`. Direct process readback found PID 8232 absent by `09:33:23Z`.

The source path explains this ordering. Scheduler demand previously depended
only on the next due-source timestamp. Consuming the last one-shot cleared
that timestamp before `automation.scheduler.admit` finished its reconcilers.
The optional-worker coordinator observed false demand and immediately signalled
stop; the scheduler wrapper killed its child. This cancelled the caller before
it could observe the matched reply and record qualification evidence. Independent
Luna Max review confirmed this P2 path at the unchanged U host source.

The repair selects the explicitly enabled independent scheduler for the host's
configured service lifetime. A source cursor is no longer its process-lifetime
predicate. The existing worker owns its outstanding calls and shared timer;
an empty page uses the existing 60-second cap. Disabled configuration retains
the existing compatibility path. Host shutdown, authentication, Store-owned
admission, source cursors and the client fault seam are unchanged. No arbitrary
grace period or new timer is added.

A focused regression obtains a real authoritative empty due-page projection
and requires the enabled service to remain selected; disabling it removes
selection. Strict production Clippy passes in both modes on Windows/Linux,
followed by **16/16 affected scheduler tests in each Windows mode** and
**17/17 in each Linux mode**. Independent Luna Max review of the runtime patch
found no additional actionable P1/P2. A final follow-up corrects only the stale
two-line demand comment; passed behavior checks are retained. Fresh installed
qualification of the repaired source is still required.
