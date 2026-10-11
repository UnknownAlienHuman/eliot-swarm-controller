# Qualification observation repair

Status: affected source checks pass; installed and model qualification remains incomplete.

The installed R48 T2 run admitted a real CheckRun, but its observer sampled the
fault witness only once. The sidecar now waits within the existing timeout,
checks its owned host remains alive, and retries only an absent witness. A
monotonic deadline propagates the remaining budget to the public CLI readback,
including its output drain; successful validation after the deadline is
rejected. Every observation rechecks the captured controller UUID, owner
controller/epoch scope and running worker. Liveness samples bind to that same
captured controller. A present foreign or invalid witness fails immediately.
The missing-witness note
separates arming, the RPC result and witness publication rather than claiming
that the qualification flag was not set. The public fixture passes **30/30 on
Windows and Linux**, including a real child publishing a delayed witness.

The non-default `qualification-faults` worker now retains two bounded,
create-only diagnostic documents for the first marked admit: the actual RPC's
closed terminal category, then the existing frame parser and witness writer's
actual outcome. Both bind to the live worker PID and exact private config
digest. They contain no request/response body, credentials or raw error message.
They do not accept an admission, create a retry, replace the RPC result or
substitute for the applied-fault witness. A cancelled/incomplete RPC leaves
terminal evidence absent. Default builds omit the diagnostic paths.
`STORE_CLOSED`, including a lost database response, is conservatively
diagnosed as transport uncertainty.

Rust **1.98.1** strict production Clippy (`--lib --bins --no-deps -- -D warnings`)
passes for `swarm-automation` in default and qualification modes on Windows and
Linux. After those four gates, the library/worker tests with the feature pass:
**9 Windows / 10 Linux**, including four new diagnostic regressions. They cover
closed category and identity, create-only publication, invalid input refusal,
real witness writer success/failure, parser absence, redaction and preservation
of the original RPC result. After the `STORE_CLOSED` review correction, only
the changed worker's strict Clippy (both modes/platforms) and two feature tests
(both platforms) were refreshed. Unchanged library checks remain retained.
No full workspace or installed claim follows from these package checks.

The frozen OpenCode T run already retained an exact rejected `agent.open` with
`NATIVE_MODEL_UNAVAILABLE / model_absent` and zero dispatches. Its old observer
waited for applied readiness until timeout. The current native harness stops
immediately on the matching terminal rejection and reports only a safe code
and closed catalog category. It requires the exact Operation, prerequisite,
Task, Attempt, method, state and rejected outcome. Foreign and malformed scope
does not trigger this shortcut. The integrated readiness fixture passes
**101 assertions on Windows and Linux**. The frozen T receipt is unchanged.

[The evidence manifest](QUALIFICATION-OBSERVATION-REPAIR.json) binds these checks
to exact source and logs. The unchanged 101-assertion readiness checks were
retained after confirming their source/log hashes. Only the changed R48
fixture was repeated after the review repairs; its earlier 21-case logs remain
historical evidence.

R48 applied-fault/no-replay still needs a fresh installed host/worker pair and
new RunId. OpenCode needs factual selected-server catalog coordinates before
another native run. Antigravity T's dispatch/model-init observations do not
establish a causally bound result body or Task acceptance. Historical runs,
receipts, frozen checkouts and diagnostic failure logs remain unchanged.
