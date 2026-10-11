# Installed R48 public contract repair

Status: source verified; installed R48 remains unqualified.

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
