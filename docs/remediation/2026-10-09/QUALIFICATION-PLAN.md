# V27 candidate qualification

Status: planned, not executed. Production code and warnings-denied Clippy must finish before the test phase. This plan uses the actual checkout after upstream TaskPrompt PR #107; historical audit verdicts are not qualification results.

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

The two UNKNOWN audit rows, source lines 146 and 362, require the corresponding execution evidence above. Source review alone cannot change their verdicts.

## Fixtures, custody, faults and load

Run the existing JS bridge fixtures, Codex Python vendor/bridge fixtures and OpenCode fixtures using their documented local dependencies. Python here is an existing test driver, not a production service. Cover native finite-process capture, cancellation, family departure, durable journals and restart on Windows and Linux. Linux builds use the repository's pinned Rust toolchain and explicit system C compiler when the ambient `cc` resolves to another tool.

Run the existing public core-failure harness and quiet-host load contour against exact candidate binaries and build manifests. Record counts, event integrity, latency and custody disposition; a host-only 200-client contour does not prove 200 native agents or the throughput targets.

## Native model evidence and delivery

Build clean committed candidate packages through the existing provenance builders. Retain their exact policy, manifest and executable hashes. Use actual advertised Step 5 free model coordinates and Antigravity Gemini 3.8; catalogue visibility is a prerequisite, not proof of execution. Run the native qualification harness through public launch, dispatch, readback and result paths. Do not rotate accounts/models, bypass provenance checks, replay uncertain effects or substitute an unrequested paid model.

Record unavailable native prerequisites and unexecuted cases explicitly. Update the portable audit with observed results, push checked changes to GitHub `main` without force, and verify the remote commit. Keep the Goal active while requested acceptance work remains.
