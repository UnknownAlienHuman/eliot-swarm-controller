# Installed R48 W: five bounded observations verified

Fresh run `r48-c0e4642174d842b5b11f59edbb74f39e` completed once at
`2026-10-11T10:20:43.8764460Z` with exit 0. All **five bounded observations**
are verified: bootstrap seed, current scheduler owner readiness, eligible
one-shot CheckRun admission, applied uncertain-admit fault without replay, and
host liveness. The sidecar still reports overall R48 `not_qualified`; these
observations do not qualify the entire optional-worker lifecycle or V27.

The five runtime packages were built from
`ef3a4b3e64fb2d4aae87dda7f71fd5b87c57a480`, tree
`e2fa1c5b7cf3a94b1c6e0158882972020973d740`. The sidecar is pinned separately
to the [completion repair](R48-COMPLETION-EVIDENCE-REPAIR.md) at
`b342f9993ee6b16d0f3ba92298bef1551aaa850e`. Git comparison confirms no
Cargo or runtime input changed between those commits. Existing verified
packages were installed into W's fresh private root; no Rust rebuild, model
call or historical-run replay occurred.

The actual worker withheld an 892-byte `automation.scheduler.admit` reply.
Its witness PID 40276 matched the validated current, active, ready Store-owned
worker. The private config digest
`85ef92e9fe3ed439bbd793f2e1d169f710fec7235428980235a6b06b2154081e`
matched the worker config, with controller
`d06431cc-5f93-43b7-86a0-2ac0f2b5e38b` and host epoch 2 unchanged. Three
public liveness observations retained the same host identity. After confirmed
graceful host stop, the completion consumer persisted exactly one admission
intent, no second intent, the observation boundary and validated injection.

Independent process readback at `2026-10-11T10:20:53.7032630Z` found bootstrap
PID 53840, final host PID 13360 and worker PID 40276 absent. No second-intent
marker exists. The original qualification receipt is retained at SHA-256
`5402c3f90d117d771a3be4a765e5043adaa3960476f4fa928a50c1581ed62fa5`;
the execution log is
`75f5498ff69df99f0139d3ee087291507afb067306087bc96ff2036efa18b09e`.

The failed V receipt and older U/T/T2 observations remain unchanged. W's
CheckRun evidence establishes exact admission links; it does not establish
Task acceptance. Native Step 5 free/Gemini 3.8 execution and causal result
consumption, independent Task acceptance, broader platform/crash/load
qualification, and the remaining dependency advisories stay open.
