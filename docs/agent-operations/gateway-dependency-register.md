# Gateway dependency register

This register owns the exact reusable mechanisms and limits for the optional remote
MCP gateway. It is subordinate to [gateway architecture](../gateway.md), the
[current audit](audit-current.md) and [implementation V](implementation.md#v--small-single-tenant-gateway).
A dependency is retained only for the mechanism named here; it does not acquire Eliot
authority or justify another service/database.

Inspected **2026-10-24** against Eliot source
`b61c0bd76fc3c16f8e28c262f7607ace2426469b`.

## Selected mechanisms

| Dependency / owner | Exact inspection point | Reuse | Limit and required Eliot action |
| --- | --- | --- | --- |
| [official MCP Rust SDK / RMCP](https://github.com/modelcontextprotocol/rust-sdk) | Eliot pins `rmcp = 3.5.0`. Official tag `rmcp-v3.5.0` resolves to commit [`0cde3c5`](https://github.com/modelcontextprotocol/rust-sdk/tree/0cde3c5cf3e6aff0cc852ce6045f107e95991f48). Reviewed `streamable_http_server/{tower.rs,session/local.rs}`. | Streamable HTTP framing; loopback Host validation; configurable Origin validation; bounded request body; cancellation; in-memory session routing. `LocalSessionManager` removes a session when its worker ends. `SessionConfig` defaults to 60-second initialize timeout, 5-minute inactivity timeout and 60-second completed-stream cache. | The manager has an unbounded `HashMap` and `create_session` has no count admission. RMCP idle cleanup is not a gateway capacity policy. Eliot must add an independent `max_sessions`, explicitly configure the idle lifetime, retain the existing connection cap and keep timeout/disconnect as unknown rather than replay. Do not add an external session database. |
| `swarm-process::permissions` | `read_private_durable`, exact-file identity checks and no-link/reparse path traversal at Eliot `b61c0bd`. | Bounded regular-file opening, no-follow identity checks and exact byte reread across the durability boundary. | The helper accepts relative paths by resolving `current_dir` and does not validate the permissions of an already existing secret file. Gateway needs a small shared secret-reader wrapper that requires an absolute path and verifies current-user/private owner/mode/ACL before returning bounded bytes. Do not keep gateway-local `std::fs` readers. |
| `swarm-mcp::profiled_facade` | `crates/swarm-mcp/src/mcp/mod.rs` at Eliot `b61c0bd`. | One configured credential and expected client ID select one restricted profile. Tool and subscription IPC connections use the same fixed principal. Transport-class failure drops the link; the failed request is never silently replayed. | One HTTP/MCP session still creates one facade and its local IPC/subscription state. Gateway capacity must bound sessions without adding another principal registry. Store remains the only action authority. |
| `swarm-client` | Current authenticated local IPC client. | Existing identity, bounded framing and outcome-unknown semantics. | Gateway timeout or disconnect may drop the HTTP wait after Store admission. Reconnect/readback uses the original caller-owned request/Operation identity; never resend a mutation by guess. |

## Rejected donors and duplicate owners

- The kernel-host `gateway` subcommand is a compatibility launcher, not process
  supervision. It locates a sibling executable, forwards two paths and waits for exit.
  Remove it after callers use the standalone binary.
- Do not add OAuth, remote identity mapping, a gateway database, a reverse proxy,
  durable RMCP session store or another request journal. The external edge owns TLS,
  public routing and user identity.
- Do not rely on RMCP defaults as Eliot product policy. Pin the required connection,
  session, body, initialization and inactivity bounds in gateway configuration and
  connected checks.

## Refresh rule

When the RMCP pin changes, inspect the exact tag/commit and recheck session admission,
worker cleanup, idle timeout, Host/Origin behavior, body streaming, cancellation and
protocol mode. A newer release number alone never closes an Eliot finding. Changes to
`swarm-process::permissions` or `swarm-mcp::profiled_facade` require the same
caller-to-effect review before the gateway claims the new behavior.
