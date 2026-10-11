# R48 public CLI process custody

The qualification sidecar now retains ownership of every short-lived public CLI
child until its exit is confirmed. A timed-out call stops only its exact owned
`Process`, waits at most five seconds for exit, and disposes the handle only
after confirmation. An unconfirmed handle and PID remain in the scenario's
pending-call collection and are checked again during final cleanup. The
original CLI timeout or process error remains unchanged. Long-lived host
processes continue through their existing separate cleanup path.

`Test-R48PublicContract.ps1` passes **33/33 on Windows and Linux**. The three
new cases exercise a real child timeout, refusal to dispose an unconfirmed
process, and caller retention of the handle/PID with the original unknown
outcome. Both log files have SHA-256
`f02ceaad20fb4082b8ba4351937cd949328348a91ff7f33ee58857735e57d26b`.
PowerShell AST parsing and `git diff --check` pass. No Rust source changed, so
the previously passed strict production Clippy and affected Rust tests are
retained without repetition.

| File | SHA-256 of Windows working-copy bytes |
| --- | --- |
| `Invoke-R48SchedulerQualification.ps1` | `d6c78aee84bc309ceb6fa3ac9d28ccd00fe466ee9607c23524341dac66cfe411` |
| `Test-R48PublicContract.ps1` | `733bcd099dd3641654945def410739736200b89c96eaf3ef0d61341cb98b20dd` |

Independent Luna Max source review of these exact hashes found no actionable
P1/P2. This source/test result does not establish installed R48 qualification.
The fresh U packages are pinned
to binary-source commit `9f2c789c7b1808998e8776b678a0232345b44305` and tree
`b6870f6e32fd8bba61d6ea4532d71b4dd61645eb`; the separately bound final
sidecar consumes them through a fresh private output root. Historical T/T2
receipts and unknown applied-fault/no-replay outcomes remain unchanged.
