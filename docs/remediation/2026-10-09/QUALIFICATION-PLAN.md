# V27 candidate qualification

Status, 2026-10-10: installed execution and resulting bounded repairs in progress. Strict production Clippy passed for all 24 owned packages on Windows and Linux before the late Windows permissions and supervisor-startup repairs; those require their own affected gates. Host fixture coverage is a retained per-case union (Windows 371 PASS / 1 ignored; Linux 385 PASS / 1 ignored), rather than a new full run on the delivery SHA. The five MCP fixture gaps have exact retained PASS evidence on both platforms. The full installed `b0ebe26` host-load contour passed with 200 clients, 200 Tasks, 3165 matching admitted/drained messages and exit 0. Core faults and native adapters remain unqualified; both native receipts retain zero dispatches. See [execution evidence](EXECUTION-RESULTS.json). This plan uses the actual checkout after upstream TaskPrompt PR #107; historical audit verdicts are not qualification results.

## Connected source gate

Run production-only Clippy for all owned workspace packages, without dependencies and with warnings denied. Format exact owned files; preserve the vendored Atlas snapshot. Record the committed candidate, commands, toolchain and source hashes. Native packages must use their current descriptor coordinates, not historical installed versions.

## Rust and public-path evidence

Run the owned workspace's full Rust targets once after the connected source gate. Repair failures at their actual cause and rerun affected tests when the repair changes source. Include the following meaningful gaps through the existing fixtures:

| Boundary | Required observation | Existing fixture |
|---|---|---|
| MCP identity | Mismatched configured/credential client ID returns `PROFILE_MISMATCH` | `mcp_integration_tests/mod.rs`, `profiles.rs` |
| MCP local rejection | Forbidden Observer calls and requests without caller IDs do not forward application methods; allowed authorization reads are distinguished | `mcp_integration_tests/profiles.rs` |
| MCP inventory | All Observer pages contain exactly the allowed core tools and read-only hints | `mcp_integration_tests/profiles.rs` |
| MCP Tasks | Returned Task ID resolves to the real `agent.open` Operation, including retry | `mcp_integration_tests/tasks.rs` |
| Concilium cause | Valid retained source dispatches one ScriptRun; altered Operation/Task/revision/Attempt causes dispatch none | `store/automation_dispatch.rs`, `automation_dispatch_bus.rs` |
| Supervisor fault | Failed child-identity capture or health write retains custody; restart does not spawn a duplicate | `host_module_supervisor.rs`, real owned child and temporary Store/IPC |
| Resource root claim | Only the exact pristine excluded queued launch is omitted; damaged JSON, changed client ID or route holds admission | `store/capacity_tests.rs` |
| Producer release | Terminal evidence preserves the original start observation/event and dispatch identity, then releases the matching active use | `store/capacity_tests.rs` |
| Workspace admission | Proved held workspace remains committed when the following live route check becomes Hold/Unavailable; no native binding is created | `store/work_dispatch_regression.rs`, workspace evidence fixtures |
| Dispatch recovery | Original worker context survives replacement; each changed immutable context field is rejected | `store/module_bridge_recovery_tests.rs`, `store/runtime.rs` |
| TaskPrompt | Deterministic saved envelope binds original source, frozen brief and typed launch packet/contract; mutation is rejected | `store/task_prompt.rs`, `runtime/batch.rs`, Codex adapter fixtures |
| OpenCode loop-step result | Exact current descriptor permits `input_status`; old/wrong schema, artifact, capability, binding, session, digest or receipts reject it | `store/opencode/tests.rs`, `store/module_handshake.rs` |
| Scope uncertainty | Mixed definite and unsupported glob intersections reject acceptance before any active scope override | `store/code_scopes.rs` fixtures |
| Owned-service departure | Exact Rust-module artifact participates in departure; malformed route/proof gets a durable bounded failure disposition and retains its resource fence | `store/launcher_owned_service.rs` fixtures |
| OpenCode deadlines | Parent and helper agree on startup deadline; stop timeout retains the exact handle for later family-departure proof | `runtime/opencode_v2/owned_service.rs` fixtures and owned children |
| OpenCode checkpoint | More than 16 unresolved execution periods survive the writer/restore contract without terminal or release inference from overflow | `runtime/opencode_v2/execution.rs`, `snapshot.rs` fixtures |
| Claude result recovery | Lost result ACK yields retained Unknown/reconnect; identical intent is idempotent and a different intent never overwrites the original | Claude journal and result fixtures |
| Command ACP admission | The first exact retained command admits one TaskPrompt; extra pre-admission effect evidence, altered commands and wrong admission revisions reject. Invalid TaskPrompt is rejected before session creation; retained admission never replays a native prompt | Command ACP journal and native protocol fixture |
| Scheduler contention | Typed SQLite Busy/Locked preserves scheduler and host IPC with paced readback; corruption and other Store failures remain hard errors | scheduler/Store execution fixture |
| Bus isolation and custody | Damaged scope construction leaves other scopes supervised; missing receipt cannot start a replacement before exact departure proof | `host_bus_supervisor.rs` execution fixture |
| Legacy shutdown | Bounded foreground shutdown reports unfinished worker custody; an unidentified scheduler launch settles only after exact family-departure proof | legacy worker and owner execution fixtures |
| Check start and lock loss | Missing start permission settles without native execution; a missing or unusable worker lock preserves the exact owner and withholds resource release until family departure is proven | legacy check worker and standalone-host fixtures |
| Git hook recovery | Wrapper publication failure resumes the same installation; interrupted revoke resumes cleanup while preserving a user-modified hook and the original backup | `hooks/git.rs` fixture and public hook setup/revoke callers |
| ModuleRun cleanup | CLI returns owner-bound CleanupPending/Unknown after a finite post-bridge grace while the separate owner retains its OS lock and family; stale receipts never permit replacement | public ModuleRun and exact native-family fixtures |

The two UNKNOWN audit rows, source lines 146 and 362, require the corresponding execution evidence above. Source review alone cannot change their verdicts.

## Fixtures, custody, faults and load

Run the existing JS bridge fixtures, Codex Python vendor/bridge fixtures and OpenCode fixtures using their documented local dependencies. Python here is an existing test driver, not a production service. Cover native finite-process capture, cancellation, family departure, durable journals and restart on Windows and Linux. Linux builds use the repository's pinned Rust toolchain and explicit system C compiler when the ambient `cc` resolves to another tool.

Before Unix Zed unit fixtures, build the same source's `swarm-kernel-host` executable and set the test-only `ELIOT_ZED_TEST_HOST_EXECUTABLE` to its absolute path. The fixtures launch the real hidden batch worker and preserve its ready/go and frozen-context checks; a libtest executable cannot dispatch that worker command. Production executable selection remains `current_exe` and never reads this test variable.

Run the existing public core-failure harness and quiet-host load contour against exact candidate binaries and build manifests. Record counts, event integrity, latency and custody disposition; a host-only 200-client contour does not prove 200 native agents or the throughput targets.

## Native model evidence and delivery

Build clean committed candidate packages through the existing provenance builders. Retain their exact policy, manifest and executable hashes. Use actual advertised Step 5 free model coordinates and Antigravity Gemini 3.8; catalogue visibility is a prerequisite, not proof of execution. Run the native qualification harness through public launch, dispatch, readback and result paths. Do not rotate accounts/models, bypass provenance checks, replay uncertain effects or substitute an unrequested paid model.

Record unavailable native prerequisites and unexecuted cases explicitly. Update the portable audit with observed results, push checked changes to GitHub `main` without force, and verify the remote commit. Keep the Goal active while requested acceptance work remains.

The contract check dated 2026-10-09 found no exact assistant-to-input parent field in the project-pinned OpenCode server/client 2.0.7 or its current public OpenAPI message projection. Preserve `NATIVE_ASSISTANT_PARENT_UNAVAILABLE`; neither response order nor the newest assistant message proves dispatch causality. Antigravity's corresponding result-body gap remains `RESULT_BODY_UNAVAILABLE`. Execute and report supported native launch, dispatch and readback components separately; full result qualification requires the missing public identity evidence. Public `operation.get` deliberately redacts private `result.details`, so qualification consumers must use its published fields and cannot demand a private typed receipt from that response.
