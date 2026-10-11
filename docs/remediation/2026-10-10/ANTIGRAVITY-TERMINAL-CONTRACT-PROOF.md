# Antigravity normalized terminal receipt repair

Status: source repair verified on Windows/Linux; installed qualification remains pending.

## CodeUnderstandingProof

- Task goal: make a successful normalized Antigravity dispatch receipt consumable by the real Store without claiming unsupported execution or Task completion.
- Domain concept: normalized dispatch admission and current-process terminal observation.
- Project/module: `eliot-swarm-controller`, `swarm-antigravity-adapter`, `swarm-kernel-host`.
- Candidate files/symbols: `modules/antigravity-rust/src/controller.rs::terminal_outcome`; `crates/swarm-kernel-host/src/store/runtime.rs::validate_task_dispatch_admission`; `crates/swarm-kernel-host/src/store/producers.rs::record_task_dispatch`; `modules/antigravity-rust/src/result_page.rs::build_normalized`.
- Graph findings: Codebase Memory CLI `search_code --project eliot-swarm-controller --pattern terminal_outcome` found the real reducer and terminal producer. Indexed line metadata is stale; current lexical source is authoritative.
- Exact anchors: the adapter previously emitted `native_terminal_result_observed`; Store permits `native_input_admitted` or `native_result_observed` for admission-only facts, and requires explicit completed-turn facts for `native_turn_completed`.
- Execution path: `begin_prompt` validates the Store-produced prompt/context; one pending prompt retains its receipt; `settle_terminal` requires the same conversation, a newer terminal ordinal and a response digest; `terminal_outcome` supplies the typed receipt; Store validates exact Operation/Attempt/prompt/module identity and retains the producer transactionally.
- State reads/writes: adapter bounded pending/outbox state; immutable Store dispatch context and Attempt; Store producer and Operation settlement. Existing retained qualification databases are not edited.
- API/event surface: `RuntimeCommand`, `RuntimeOutcome`, `TaskDispatchAdmissionReceipt`, `task.dispatch`, module report.
- Diagnostics before: AG S2 retained worker diagnostic is `TASK_DISPATCH_ADMISSION_INVALID`. The completion-marker mismatch is source-confirmed. It is not proof that this was the sole cause of that installed run.
- Invariants: no prompt resend, no native turn/input identity invented, no Task acceptance, no result body qualified from conversation plus local ordinal. Rejected or unknown dispatches do not gain a normalized admission receipt.
- Blast radius: successful normalized Antigravity dispatch terminal details and Store producer recording. The exact warm-stream receipt validator and legacy/agent.send/rejected markers are preserved. A normalized dispatch retains one producer, with its terminal receipt on the Operation.
- Verifier: strict production Clippy for the adapter and host, then a regression using the actual adapter Controller and actual Store report path on Windows/Linux.
- Metadata/diagnostics: Rust 1.98.1; both packages use edition 2024 and MSRV 1.89. `cargo metadata --no-deps --locked` resolves their real library/binary targets. AST-grep finds the terminal producer. Live LSP tooling is unavailable; Cargo diagnostics are the current compiler evidence.
- Unknowns: exact S2 native consumption, recoverability after the original worker exited, productive response body, and installed qualification.
- Edit allowed: yes; files, anchors and verifier are established. Eliot compile-packet tooling is unavailable; standalone work is allowed by `docs/eliot-memory-os-integration.md`.

The adapter preserves `native_terminal_result_observed` for the exact warm-stream validator and adds `execution_complete:false` and `task_completion:"unknown"` to successful normalized dispatches. Store admits that marker only when its caller supplies the already validated warm producer. It retains one canonical `native_input_admitted` producer and the original terminal receipt on the Operation. A local terminal status and digest remain bounded observations; the native protocol does not expose the response's input-parent edge.

## Verification

Strict production Clippy passed on Windows and Linux with Rust 1.98.1:

```text
cargo +1.98.1 clippy --locked -p swarm-kernel-host -p swarm-antigravity-adapter --lib --bins --no-deps -- -D warnings
```

Only then, the affected cohort passed 14/14 on each platform:

```text
cargo +1.98.1 nextest run --locked -p swarm-kernel-host --lib -E 'test(module_bridge_recovery_tests) | test(receipt_tests::warm_) | test(runtime::warm_stream::tests)'
```

`antigravity_terminal_receipt_needs_store_observation_and_keeps_task_unresolved` drives the actual Controller with a Store-produced command and uses the real Store observation/outcome path. It checks older/foreign terminal observations, a replayed same-conversation result that must remain admission-only, forged observation rejection with rollback, exact prompt/module digests, one producer and an unresolved Task. A separate test process receives its public descriptor claim through `Command::env`; the parent process environment is not mutated.

The production source hashes remained unchanged between Clippy and test completion. Independent exact-source review accepted the three production files with no remaining P1/P2. [The bounded completion evidence](ANTIGRAVITY-TERMINAL-REPAIR-EVIDENCE.json) records commands, source/log hashes and the remaining installed qualification scope. Existing passing suites are retained rather than repeated.
