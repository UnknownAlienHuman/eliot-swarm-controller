# Current audit register

This is the active continuation register. AR-001–078 remain in the
[historical register](audit-register.md). Later ranges are frozen at their merge
checkpoints:

- AR-079–088 at [`b7a5fe1`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b/docs/agent-operations/audit-current.md);
- AR-089–099 at [`e42c464`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e42c464d58db770cc5490f46f391843f9fec8df2/docs/agent-operations/audit-current.md);
- AR-100–114 at [`e99c06b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e99c06bb490fe630eec70337ce5137a26e145d84/docs/agent-operations/audit-current.md);
- AR-115–126 at [`053f1e0`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/053f1e0fb15b3d40c00918f4a25ec8f9e4969e02/docs/agent-operations/audit-current.md);
- AR-127–141 at [`c73c36c`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c73c36c72d2c86cf710fa69e39ab07e2052c1f5e/docs/agent-operations/audit-current.md);
- AR-142–159 at [`b61c0bd`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b61c0bd76fc3c16f8e28c262f7607ace2426469b/docs/agent-operations/audit-current.md);
- AR-160–170 at [`0a228cd`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/0a228cd3a2fde9f38be136485b53b88c7e0e5c84/docs/agent-operations/audit-current.md).

[Audit index](audit-index.md) maps every range to immutable evidence;
[implementation](implementation.md) owns source changes;
[gateway architecture](../gateway.md) owns the selected gateway target;
[gateway dependency register](gateway-dependency-register.md) owns inspected reusable
mechanisms; and [status](../implementation-status.md) owns landed behavior.

## Resume here

- Checkpoint: **2026-10-24 / Antigravity A1b runtime reduction**.
- Runtime source changed from gateway-audit head
  `0a228cd3a2fde9f38be136485b53b88c7e0e5c84` to
  **`3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb`**.
- Connected chain:
  `launcher_dispatch::require_current_antigravity_scope` → retained descriptor and
  current binding/Open validation → immutable launch-dispatch packet → adapter
  `Controller::complete_open/read_command` → qualification readiness. The mandatory
  `agent.refresh` Operation, `AntigravityDispatchSessionProof`, latest-refresh query,
  snapshot validator and assignment/lease/session digest packet are removed.
- Overall source audit remains **incomplete, 170 entries**. A1b is an implementation
  checkpoint, not another finding range or a whole-project completion claim.
- Preserved: exact Task/Attempt/current-revision and launch-parent checks; binding
  generation and current route; current boot/root/connection/recovery fences;
  explicit-model readback or honest native-default unknown; workspace authority;
  adapter command-time binding/root checks; no replay after uncertain input.
- `agent.refresh` remains a real optional current-process readback action. It no longer
  authorizes Dispatch or emits a duplicate proof packet.
- Verification: run
  [40131522836](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/40131522836),
  job `120984044559`, passed affected rustfmt and warnings-denied Clippy. No tests,
  native/provider calls, installed harness, crash, restart or load qualification ran.
- Recorded change: **−305 production Rust lines, −20 qualification-glue lines, +3 test
  lines**.
- **Next exact chain — implementation A2:** adapter
  `Controller::{record_outcome,pending_receipts,completed_turns}` and normalized result
  pages → `HostSession::request_saved/flush_reports` → exact `module.outcome` /
  `module.result` ACK and replay → private atomic retained outcome/body/page files →
  verified prior-owner departure and successor delivery for the original Operation.
  Preserve origin boot and source identity; never put native prompt intent in the
  delivery spool or replay input.
- Runtime implementation order is now **A2–A3, then B–V**.
- Still unqualified: crash before/after durable capture, power loss, old-boot delivery,
  source-pressure behavior, installed Antigravity, live restart and scale.

On resume, compare live `main` with `3c7a84f`, reopen only changed A2 owners/consumers,
and continue the named delivery chain. Do not recreate the deleted refresh/proof gate.

<a id="findings-ar-160170"></a>
## Findings AR-160–170

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-160"></a>AR-160 | One gateway principal/profile | **Preserve.** Configuration selects one profile; `selected_tool_profile` binds its expected client ID to the loaded credential and `profiled_facade` rejects mismatch before bind. `full` is rejected. HTTP requests cannot select another credential/profile. | Keep this fixed process identity while adding capacity and secret-file repairs; prove concurrent sessions remain one Store principal. |
| <a id="ar-161"></a>AR-161 | Secret path and file boundary | **Open.** Config resolves relative credential/bearer paths. Credential uses unbounded `std::fs::read`; bearer uses capped ordinary `File::open`. Both may follow a link/reparse target or open a non-regular object and neither verifies private owner/ACL/mode. | One shared absolute, bounded, no-follow, regular, private-file reader based on `swarm-process::permissions`; verify opened identity and permissions before returning bytes. |
| <a id="ar-162"></a>AR-162 | Credential/bearer shape and bounds | **Mixed.** Credential JSON is closed (`deny_unknown_fields`) and requires nonempty client ID/token length ≥32, but token/file have no upper bound before whole-file allocation. Bearer is capped and validated as 32–512 ASCII graphic bytes; later validation rejects commas. | Strict credential file/token upper bounds before allocation; retain closed JSON and bearer checks; never log secret material. |
| <a id="ar-163"></a>AR-163 | HTTP request boundary | **Preserve.** Loopback bind, exact `/mcp`, no query, one Authorization header, syntax checks, constant-work bearer comparison, bounded headers/body and disabled HTTP keep-alive are connected before Store forwarding. | Connected negative tests, streaming/body exhaustion and exact error/readback behavior. |
| <a id="ar-164"></a>AR-164 | Host and browser Origin | **Preserve plus open configuration.** RMCP owns loopback Host validation and present-Origin validation. Gateway hard-codes only localhost/loopback Origins; missing Origin remains valid. | Configure exact external-edge Origins with validation still enforced for an empty list. Prove browser/tunnel behavior without widening Host or identity. |
| <a id="ar-165"></a>AR-165 | Connection versus session capacity | **Open.** A hard-coded semaphore caps 32 live TCP connections, but shared RMCP `LocalSessionManager` has an unbounded session map and inserts without a count gate. Sequential initialize requests can create more sessions than the connection limit; every session constructs another facade/IPC/subscription state. | Configured independent `max_connections` and `max_sessions`; reject before facade construction and release exactly once on every terminal/failure/idle/shutdown path. |
| <a id="ar-166"></a>AR-166 | RMCP session cleanup | **Scope correction / preserve.** Pinned RMCP 3.5.0 is not an infinite-session implementation: default init timeout is 60 seconds, idle timeout five minutes, completed stream cache 60 seconds, and the tower closes the session after its worker ends. | Do not claim dependency defaults as Eliot policy. Configure/pin the required idle/init bounds and prove cleanup under the bounded gateway manager. |
| <a id="ar-167"></a>AR-167 | Request/SSE lifetime | **Open distinction.** `request_timeout_seconds` covers body intake and dispatch until an HTTP response is created. It does not bound an already-created SSE body, while the connection permit remains held until the connection task ends. | Explicit session/stream/idle policy and slow-client cases; retain connection cap and RMCP cancellation without pretending one timeout covers all lifetimes. |
| <a id="ar-168"></a>AR-168 | Unknown outcome and disconnect | **Preserve boundary.** `profiled_facade` drops failed IPC links and does not replay; gateway timeout returns an explicit unknown-outcome warning; shutdown aborts HTTP waits while Store Operations remain authoritative. | Execute timeout-after-commit, client disconnect, gateway restart and original-request readback. Ensure aborted transport never cancels/retries admitted work. |
| <a id="ar-169"></a>AR-169 | Compatibility launcher | **Open duplication.** Kernel-host still exposes `Gateway`, finds a sibling by filename, forwards paths and waits for exit. It verifies no installed coordinate/image and owns no gateway recovery. Build tooling asserts that this compatibility source remains. | Standalone cutover followed by deletion of CLI branch, `run_gateway_binary`, diagnostics, build assertions and fixtures together. |
| <a id="ar-170"></a>AR-170 | Gateway qualification | **Open.** Source contains no connected proof covering secret permissions, fixed-principal HTTP behavior, session capacity/cleanup, timeout/readback, shutdown or standalone cutover. Existing comments/defaults are not execution evidence. | Minimal rustfmt/warnings-denied Clippy/build during V0–V3 plus focused connected security/capacity/readback cases and later live edge/tunnel qualification. |

<a id="trace-2026-10-24-gateway"></a>
## Source trace at `b61c0bd`: remote gateway

Inspected 2026-10-24. This is source evidence, not a live listener, browser/tunnel test,
secret ACL qualification or Store mutation exercise.

### AR-160 — principal selection is correctly outside the request

`Config::load` selects one named MCP profile. Before binding the socket, `run` calls
`selected_tool_profile(profile, credential.client_id)` and constructs the same
`profiled_facade` used by local MCP. Mismatch fails, and both config validation and
runtime reject the unrestricted Full profile. No HTTP path/header/body can name a
credential or profile.

Each RMCP session gets a new facade instance, but every instance is constructed with
the same fixed credential and profile. Therefore several sessions are several transport
sessions of one Eliot principal, not tenants. Preserve this property and make it
explicit in capacity tests.

### AR-161–162 — secret parsing is stricter than secret opening

Credential parsing uses the closed `Credential` type, but calls `std::fs::read` before
any size/regular/link/private-permission check. A very large regular file can allocate
before rejection, and a link/device/FIFO is not excluded by source. Bearer loading uses
`.take(515)` and then validates the trimmed token, but the ordinary open has the same
path/type/permission problem and may block on a special file.

`swarm-process::permissions::read_private_durable` is the nearest current owner for
bounded regular/no-follow/identity-safe reads. It accepts relative paths and does not
validate permissions on an already existing secret; `private_permissions` applies only
when Eliot creates a selected file. V0 therefore needs a narrow shared secret wrapper,
not a claim that the existing helper already completes the boundary.

### AR-163–164 — transport checks are useful but edge policy is not configurable

Gateway rejects a non-loopback bind, a non-`/mcp` path, any query, absent/duplicate/
malformed Authorization and wrong bearer before calling RMCP. Hyper applies fixed
header limits/timeouts; RMCP streams the body under Eliot's configured cap. RMCP's
Host allowlist remains loopback-scoped.

Origin validation is explicitly enabled, but the allowlist is compiled from loopback
patterns. RMCP accepts a missing Origin and rejects a present nonmatching value. This is
correct for non-browser clients and insufficient for a browser client behind a named
edge. The target is a closed configured list, not disabling Origin validation.

### AR-165–167 — RMCP cleanup does not provide Eliot admission capacity

Eliot's semaphore limits concurrent accepted sockets only. RMCP 3.5.0 stores sessions
in a `HashMap`; `create_session` inserts without a maximum. It does clean up through
worker termination and finite default init/idle timeouts, so the correct finding is
unbounded active count within that lifetime rather than permanent leakage.

A session owns a facade with a tool IPC connection slot, separate subscription pump
slot and subscription hub. Session admission must therefore acquire a bounded Eliot
permit before constructing the facade. TCP connection permits cannot substitute for
that count because a client can close and create another session while old sessions
remain resumable.

The outer request timeout ends when the HTTP response is available. An SSE body can
outlive it, so connection, session and stream/idle lifetimes must remain separate.

### AR-168 — Store work survives transport loss

`swarm-mcp` connects lazily. Transport-class error clears its connection slot and
returns the error without retrying. Restricted mutations/effects require a
caller-owned `client_request_id`, and the gateway rejects Full. Gateway timeout tells
the caller that Store may already have committed. Ctrl-C cancels RMCP and aborts
connection tasks; it never invokes `operation.cancel` or rewrites Store state.

This is the right semantic boundary. It still needs a connected case where the HTTP
reply is lost after Store admission and the same request identity resolves the existing
Operation without a second mutation.

### AR-169–170 — extraction remains incomplete

The standalone process exists, but kernel-host still publishes a compatibility
`Gateway` command and a sibling filename launcher. The build package script searches
source text for that launcher and exact forwarded arguments. Removing the duplicated
surface requires updating callers/build policy in the same slice.

No current gateway test proves secret-file permissions, HTTP security ordering,
session admission/cleanup, unknown-outcome readback or standalone-only packaging.
Construction should use minimal affected checks; installed edge/tunnel qualification
comes only after V0–V3 are connected.
