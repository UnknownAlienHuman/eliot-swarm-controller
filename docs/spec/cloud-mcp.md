# Cloud MCP runtime

This specification owns the local HTTP MCP child and its dedicated Cloudflare
Tunnel lifecycle. [`api.md`](api.md) owns the MCP call contract; the HTTP worker
reuses that handler and the authenticated local daemon API.

## Boundary

The daemon starts one managed `swarm mcp --http --port 0 --managed-child`
process, with the configured `https://<hostname>/mcp` public URL and the same
private data directory as the daemon. It reads one newline terminated
[`McpHttpReady`](../../crates/eliot-plugin/src/protocol.rs) frame, bounded to
256 bytes and decoded by the shared protocol type. The address must be a bound
loopback socket; it proves only that the local listener bound.

After readiness, the daemon writes a unique private cloudflared YAML file under
the private data directory. The file contains only tunnel, credentials-file
path, hostname, loopback origin base (`http://<bound-address>`), and Cloudflare
Access metadata. The ingress forwards the request path to the origin, so `/mcp`
remains the HTTP handler route. The Access `audTag` is a YAML sequence. The file
never contains credential contents or OAuth/JWT values. The runtime starts the
configured cloudflared executable with explicit argv, cleared environment,
explicit working directory, and `OwnedChild` custody. Access is configured for
native `originRequest.access` JWT validation using the supplied team and
audience tag. Cloudflare Access Managed OAuth remains the external identity
provider; Eliot adds no OAuth framework.

The module owns exactly these two children. It never discovers, adopts, signals,
or stops a pre-existing `mcp-bridge` or other tunnel. HTTP stdin stays open
while serving; shutdown closes it to deliver EOF, then waits for graceful exit
before termination. If either child exits unexpectedly, the sibling is stopped
and reaped. Windows reports exact Job custody. Unix process-group custody is
degraded and must remain reported as such.

## Health and identity

The in-memory service snapshot is the nullable `cloud_mcp` member of Core's
`DaemonStatusResponse`. It reports the configured public URL, pair-level
startup/running/stopping/failure state, and custody guarantee. `Running` means
both managed child leaders are alive. It does not prove edge connectivity,
Access policy propagation, or a successful authenticated remote MCP call. With
no `[cloud_mcp]` table the member is `null`.
The snapshot exposes no child PID and does not create a local Agent, fake
cloud-Agent PID, persistence table, or generic service registry.

Cloudflare Access authenticates the remote caller. The HTTP child reuses the
existing MCP handler and its owner service principal; it does not derive an
Agent identity from `clientInfo` or process metadata. Any remote
identity/connection/call binding belongs to the routed MCP/API integration,
outside this process module. End-to-end authenticated-call qualification
remains integration work.
