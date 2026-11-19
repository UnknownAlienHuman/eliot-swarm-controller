# Cloud MCP runtime

This specification owns the local HTTP MCP child and its dedicated Cloudflare
Tunnel lifecycle. [`api.md`](api.md) owns the MCP call contract; the HTTP worker
reuses that handler and the authenticated local daemon API.

External hosted custom connectors using Cloudflare Access Managed OAuth dynamic
client registration must have the caller's exact hosted OAuth redirect URI in the
Access application's `allowed_uris`; loopback-only callbacks are insufficient. The
Meta Muse callback `https://agent.meta.ai/api/hatch/oauth/callback` is a documented
example, not a value to infer from the current UI origin. Before changing this list,
obtain the actual `redirect_uri` from the current supported connector authorization
flow and verify that exact value against the Access application.

### Hosted connector credential recovery

Keep these credentials and sessions distinct:

- A browser `CF_Authorization` cookie is a browser Access session. It is not the
  hosted connector's Managed OAuth credential and does not replace it in Meta's
  Muse auth store.
- The hosted Meta Muse connector stores an opaque Managed OAuth client credential
  for this MCP resource. Cloudflare resolves it and forwards a signed Access
  assertion to the origin; Eliot validates that assertion as described below.
- Eliot's local daemon credentials and any standalone OAuth helper state belong to
  separate clients. A helper can refresh only credentials it owns; it does not
  update Meta's auth store unless Meta explicitly provides that supported flow.

When the hosted connector's credential is rejected or its refresh grant expires,
reauthorize that same connector for the same MCP resource, preserving the user's
existing Access rights and policy. Start from Meta/Muse's first-party connector flow
and its supported reconnect/consent action; recreate the connector only if that flow
requires it. Follow its current browser authorization flow; do not begin by
disconnecting the connector or copy browser cookies or bearer credentials between
stores. A helper failure such as `502 token refresh not allowed
through egress proxy` is a client-side egress/refresh-path failure. Restarting the
Eliot daemon, copying browser cookies, or adding a second Eliot OAuth server cannot
repair Meta's stored credential. Do not bypass the proxy or extend token expiry as
a workaround. [Cloudflare's Managed OAuth documentation](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/managed-oauth/),
updated 2026-11-03, describes short-lived access tokens with refresh grants and
requires IdP reauthentication after the grant expires.

After reauthorization, acceptance requires a fresh MCP `initialize` followed by a
harmless `daemon.status` call through the repaired hosted connector. A successful
browser login, OAuth discovery response, or daemon `Running` state alone does not
prove the connector credential works end to end.

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

The HTTP child independently verifies the signed Access application assertion at
the origin before SDK dispatch, using the same configured team and audience. The
native cloudflared check protects the tunnel route; it cannot authenticate a direct
loopback request. Both managed and manual HTTP fail closed without origin auth.

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

Cloudflare Access authenticates the remote caller and forwards its signed assertion.
The HTTP verifier binds the verified subject, and the delegated grant selected by
the connector URL's `manager` query, to the SDK session and carries nonsecret
connection/call provenance through the existing authenticated Router path, as owned
by [the API](api.md#mcp-streamablehttp-server). It does not derive identity from
`clientInfo` or process metadata. The process module owns custody, not an OAuth or
principal registry. End-to-end qualification of the corrected origin and caller
binding remains required by L6.7.
