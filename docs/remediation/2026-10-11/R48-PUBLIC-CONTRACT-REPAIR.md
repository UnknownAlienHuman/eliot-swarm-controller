# Installed R48 public contract repair

Status: source verified; installed R48 remains unqualified.

The latest [installed V and completion evidence repair](R48-COMPLETION-EVIDENCE-REPAIR.md)
retain an applied fault witness with exact owner/config binding. V's original
exit 1 and blocked receipt remain unchanged: a dictionary/object mismatch
prevented final no-replay evidence persistence. The current regression suite
passes 36/36 on both platforms; all older fixture counts below are historical.

The fresh [installed U observation](R48-SCHEDULER-LIFETIME.md) verified four
of five observations but exposed demand withdrawal killing the first admit's
caller. U's applied-fault/no-replay result remains UNKNOWN; source lifetime
repair and fresh installed proof are tracked separately.

The [CLI custody follow-up](R48-CLI-CUSTODY-REPAIR.md) stops only the exact owned
short-lived public CLI process and retains unconfirmed handle/PID ownership
without replacing the original unknown outcome. Its current fixture passes
33/33 on Windows and Linux. The 30-case edition below is retained history.

The [bounded witness and producer diagnostic repair](QUALIFICATION-OBSERVATION-REPAIR.md)
passes the current public fixture **30/30 on both platforms**. It retains a
bounded wait for delayed applied evidence and a separate feature-only first-RPC
diagnosis. The 18-case and older hashes below are historical editions. T2's
missing applied witness remains UNKNOWN until a fresh installed run proves it.

The fresh T2 run `r48-91c5d1b40b3b4e568e04d5f8630bc6b6` completed at
`2026-10-11T05:08:41Z` with exit0. Bootstrap, current-owner readiness, linked
one-shot CheckRun admission and host liveness are verified (4/5 observations).
The applied-fault witness is absent, so uncertain-admit no-replay stays UNKNOWN.
The first admission-intent marker proves that the actual worker armed the
qualification flag; absence of the applied marker does not prove otherwise.
No second intent marker exists, but that alone does not establish the fault.
The first admit RPC's terminal outcome and witness-write result were not
retained. A single witness sample immediately after CheckRun admission is also
insufficient to exclude a delayed reply. The independent diagnosis separates
that observer defect from the unknown producer outcome. Retained receipt
SHA-256: `7adee78e7ef582b3b678e3073ff1063ca613ed61ab1ffaf35150caaf2337970c`;
diagnosis SHA-256: `41b7089a38ad801c61a3dfdb7a2bd58cec64a87d021c84d897c2625e26b11a53`.
The receipt is not promoted to full installed R48 qualification.

## Fresh source3449 installed observation and admission repair

The fresh T run `r48-0cfa2b5cbf32463393a9cc664d2c9162` used the rebuilt
host/worker pair from `3449eefa32cb2043d95b7ebee2fd47cfa84d7f60`. Bootstrap
seed/capture and epoch2 current-owner readiness are verified. The actual worker
was running, current, active and ready with a validated owner receipt and group
identity. The due schedule created Operation
`84f9e28f-1f18-4efc-b10f-3e4a550d0649` and Check
`997b6ffd-1de0-4156-9a15-3dd86dd2cee9`.

The harness stopped at `RECEIPT_MISMATCH:operation_not_durably_queued_or_settled`
because that Operation had the valid state `native_accepted`. The production
`accept_check` path in `store/checks.rs` retains this state after recording the
real child identity. The durable-admission assertion now accepts the actual
closed states `queued`, `sending`, `native_accepted` and `settled`; invented
`sent`/`succeeded` aliases were removed. Unknown and rejected outcomes remain
failures, and all Operation/Check/Attempt/candidate/profile links remain checked.

The current public-contract fixture passes **18/18 on Windows and Linux**,
including native acceptance and explicit unknown/rejected rejection. Both logs
have SHA-256 `59ee3226384392756191af89c586360372102a379f3eda510ecb5a1bce7db0e2`.
Current sidecar SHA-256:
`7d4133b53e2b024bbb2e2fa5c1ed2dc8505c2fb46cdf5d8feb59198eae33e6b9`.
Current fixture SHA-256:
`e4e379995d6680f1793749d988e9e50fef677d601ecba77fc2a6a30667dc9c9e`.

T cleanup confirmed host exit. Its historical receipt remains unchanged; the
one-shot admission, uncertain-admit no-replay and complete liveness subgates are
not retroactively promoted. T2 bound the new sidecar to the retained T preparation
in a separate create-only binding receipt. This PowerShell-only follow-up required no binary rebuild or
repeat of the already-passed Rust gates. The older hashes and observations below
describe the preceding edition only.

The real installed runs exposed public entry and transport mismatches that the
older 52-case helper fixture did not exercise. The sidecar now preserves bound
Run parameters, reads the canonical UUID controller identity, supplies the real
Task revision to claim/capture, sends a params document to the public CLI, and
keeps the outer Operation when its value contains a nested result. Check profile
identity is read through `check.get` and bound to the exact Operation, Attempt
and candidate. Fresh private directories receive their restrictive permissions
at creation on Windows and Linux.

Owned host cleanup closes stdin and retains an unconfirmed handle. Confirmed
exit clears the handle before disposal, so repeated cleanup cannot replace the
original worker-readiness failure with a disposed-handle error.

`tools/qualification/Test-R48PublicContract.ps1` passes 15/15 on Windows and
Ubuntu/PowerShell. It exercises the real public script entry in a separate
process, params-only CLI input, nested Operation preservation, rejected foreign
Check links/profiles, real process liveness, private directory permissions, an
actual child's EOF exit and idempotent cleanup. AST parsing and `git diff --check`
pass. This public-contract slice changes no Rust source; it does not repeat Rust gates from the
delivered source95 feature/default build.

Verified sidecar SHA-256:
`d426a20811fdae27b28d2708276bb6942dc8c43b52e9fe5757504c248a5536d9`.
Verified public-contract fixture SHA-256:
`195a53218658f5d1a6ca32c490e945317cfca597eba6795cef1243d877e77bfd`.

Independent Step 5 source review accepted these exact script/fixture hashes
with no actionable P1/P2. The review checks UUID/revision wire contracts,
Operation/Check projection and current-GM Evidence authorization, params-only
CLI transport and retained ownership of unconfirmed process handles. It makes
no installed-execution claim. Its report SHA-256 is
`9fe0c97f62aa79350cf3c5a429cd65058f70bd3bbe3bc36f2da2ec3a24400539`.

## Retained installed observations

The immutable source95 preparation remains at commit
`95b69d1d0e37487648df0161282551e2315ac18a`. Four actual attempts are retained;
none is replayed or rewritten:

| Attempt | Observed result |
| --- | --- |
| Original delivered sidecar | `MISSING_RUN_PARAMETERS`, before effects |
| `r48-c30800277b4b48b1b369c23ff082d4ce` | Host startup rejected the workspace ACL |
| `r48-cb5c7bc1dc5a4aeabf989dd6a557b4b4` | `HOST_READY_TIMEOUT`, malformed CLI params input |
| `r48-042655a5b2984a6d8586686f7b7414b4` | Real bootstrap seed/capture verified; epoch2 scheduler isolated before owner readiness |

The last run retained the same controller UUID across host epochs1 and2. It
created a real Task, Attempt and capture Operation and verified candidate
`source-66de7de102cf55484b51b31f3bdbb0c2be47448e3b32eb122b454c1f186f3b45`.
The actual failure was
`OWNER_READINESS_INCOMPLETE:worker_not_running,owner_not_active,owner_not_ready,group_identity_missing`.
The historical receipt's final `OWNED_HOST_STOP_UNCONFIRMED` masks that failure
because its already-disposed bootstrap handle was cleaned up again. The
cleanup repair above is verified separately; it does not change that receipt.
Its SHA-256 remains
`11cff5eac181b6a329857aad6e6b40c52f3dcfae0095bdd76153d784ce17dad2`.

Current-owner readiness, the real one-shot CheckRun, uncertain-admit no-replay
and the complete liveness contour still need installed evidence. Read-only
diagnosis found the source95 host places the qualification flag before `run`;
the worker parser requires `run --config <path>` first. The installed host/worker
hashes match source95 build manifests. Store retained
`AUTOMATION_WORKER_READY_INVALID`, five failures and four restarts, followed by
isolation and a departed owner. Exact child argv, stderr and numeric exit status
were not retained; `INVALID_PARAMS` and exit2 are source-derived predictions.
Arming the fault itself affects only an admit reply and does not block readiness.
The diagnosis report SHA-256 is
`4c0320cc9fed8214edbbb82c43d78714877603877d15eafa02305e08e7d85895`.
The host/worker argument contract needs repair before a fresh run. Preparation receipts,
P/Q/R, OC3 and Antigravity S2 remain unchanged.
