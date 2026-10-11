# R48 applied witness and completion evidence repair

The subsequent [fresh installed W result](R48-INSTALLED-W.md) verifies the
repaired consumer in the installed contour with five bounded observations and
exit 0. V's failed receipt below remains unchanged.

Installed V used five verified packages built from commit
`ef3a4b3e64fb2d4aae87dda7f71fd5b87c57a480`, tree
`e2fa1c5b7cf3a94b1c6e0158882972020973d740`, and ran once as
`r48-c6967d4a3fe14868a36b4a2acb57feda`. It ended at
`2026-10-11T10:09:44.8116281Z` with **exit 1**. Its original receipt remains
blocked and unchanged at SHA-256
`10f80ee50159e9a06d24fb022ff03794cb30881a23a0e0bf240f7ed696fb2f22`.
V is not replayed or retrospectively promoted.

V proves the scheduler lifetime repair reached the actual fault seam. Worker
PID 24216 wrote the first admit intent at `10:09:24.0438587Z`, the
`reply_withheld` terminal diagnosis at `10:09:24.2885714Z`, and the applied
witness at `10:09:24.2935721Z`. The withheld reply was 892 bytes. The witness
matched the live current, active, ready Store-owned worker, its private config
digest `cfea2c880d0516df38f5c2d8ab7f4d211127fcda810dce487156b6d7345f765c`,
controller `ddd88d5d-bd23-4b8a-bd4f-da40f73fad46`, and host epoch 2. Bootstrap,
owner readiness, linked one-shot CheckRun admission and three host-liveness
samples were recorded as verified. No second admit marker was found.

After the final host's confirmed stop, the completion consumer attempted to
pass `Test-QualificationAdmitAttempts`'s `PSCustomObject` directly to
`Add-SubgateResult`'s `IDictionary` evidence parameter. Parameter binding
failed before the no-replay result could be appended. The retained V receipt
therefore keeps that subgate `not_verified`, despite retaining the validated
fault injection separately. This failure is in receipt construction; it does
not erase the observed applied witness or turn exit 1 into a successful run.

The repair explicitly projects the validated admission count, second-intent
flag, confirmed-stop observation boundary and injection into an ordered
dictionary. It preserves the evidence parameter type and every witness,
owner, config, replay and shutdown guard. The regression executes the actual
completion branch after a real witness writer exits, reads its persisted JSON,
checks the injection scope, then proves a second marker refuses verification.
The complete affected public-contract fixture passes **36/36 on Windows and
Linux**. Both logs have SHA-256
`36cd6a7d54033cd24bc440ecf56003954a3dc34cefb74e2cf980d717be58abf1`.
Independent Luna Max review of both exact final source hashes found no
actionable P1/P2.

Only PowerShell changed. The existing strict Rust Clippy and scheduler gates
remain applicable; no Rust rebuild or repeated Rust gate is required. W now
verifies the fresh installed sidecar contour separately. Full R48,
native/model qualification and the full V27 release remain unproven.
