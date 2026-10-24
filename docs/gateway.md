# Remote Agent Gateway

The gateway is an optional, small, single-tenant bridge from one remote MCP client to
one fixed Eliot client identity and one fixed restricted MCP profile. It is an
integration, not Core.

[Implementation V](agent-operations/implementation.md#v--small-single-tenant-gateway)
owns source changes; the [current audit](agent-operations/audit-current.md) owns finding
dispositions; the [dependency register](agent-operations/gateway-dependency-register.md)
owns exact reusable mechanisms and upstream limits.

## Product boundary

```text
external TLS / tunnel / edge identity
            |
            v
loopback-only swarm-gateway
  fixed bearer
  fixed Eliot credential
  fixed restricted MCP profile
  bounded connections and sessions
            |
            v
local authenticated Eliot IPC
```

One gateway process/listener represents one Eliot `client_id` and profile. Multiple
TCP connections and MCP sessions authenticated with that bearer are transport sessions
of the same principal. Run another process with a different private credential/profile
when another remote principal is required.

The gateway is not:

- a public Internet listener or TLS terminator;
- an OAuth/identity broker or credential minting service;
- a multi-tenant API platform;
- a general reverse proxy;
- another authorization or request database;
- a durable RMCP session store;
- a replacement for Store Operation/readback semantics.

## Current source result at `b61c0bd`

### Useful behavior to preserve

Current `crates/swarm-gateway` already provides:

- standalone `swarm-gateway` process;
- loopback-only bind validation;
- exact `/mcp` path and no query string;
- one configured credential/profile with `expected_client_id` validation;
- rejection of the unrestricted `full` profile;
- duplicate/malformed Authorization rejection and constant-work bearer comparison;
- RMCP Host validation and explicit Origin validation;
- bounded streamed request body and bounded header count/size/read time;
- a 32-connection semaphore and HTTP keep-alive disabled;
- timeout response that states mutation outcome may be unknown;
- MCP forwarding that drops a broken IPC connection but never silently replays the
  failed request;
- shutdown/disconnect that terminates HTTP waits without cancelling already admitted
  Store work.

These are real retained mechanisms. They do not establish safe secret loading,
configurable edge origins, bounded MCP session count or standalone-only launch.

### Secret files are unsafe at the intended boundary

`main.rs` loads the Eliot credential through unbounded `std::fs::read` and the bearer
through ordinary `File::open`. The bearer has a 515-byte read cap and a final 32–512
ASCII-graphic token check; the credential file has no pre-read byte cap and its token
has only a lower bound. Both readers can follow a link/reparse target or open a
non-regular file. Neither verifies current-user/private owner, ACL or Unix mode.

`config.rs` also resolves relative credential/bearer paths against the config
directory, while the selected target requires explicit absolute secret paths. The two
files must differ, but path inequality alone is not opened-file identity.

The credential JSON type correctly rejects unknown fields. Preserve that closed shape,
then enforce strict file/token bounds before allocation.

### Connection capacity is not session capacity

The hard-coded semaphore limits 32 simultaneous TCP connections. RMCP 3.5.0 uses a
shared `LocalSessionManager` whose session map has no maximum and whose
`create_session` inserts without a capacity check. A client with the bearer can create
sessions sequentially faster than they expire; every session constructs another
profiled facade with local IPC/subscription state.

RMCP does provide useful cleanup: its selected 3.5.0 `SessionConfig` defaults to a
60-second initialize timeout, five-minute inactivity timeout and 60-second completed
stream cache, and the tower worker calls `close_session` when service execution ends.
Therefore the source does **not** prove permanent zombie sessions. It still lacks an
Eliot-owned maximum session count, configured idle lifetime and observable admission
failure when the limit is reached.

The request timeout covers body intake and creation of the HTTP response. It does not
bound the lifetime of an already-created SSE response body. Connection/session bounds
and RMCP idle cleanup remain separate controls.

### Origin policy is fixed rather than operational

The gateway fixes allowed Origins to loopback `http`/`https` patterns. Missing Origin
is accepted by RMCP, so ordinary non-browser MCP clients remain usable. A browser client
arriving through a real external edge Origin cannot be configured without a source
change. Exact external Origins must be explicit configuration; an empty configured
list must still reject every present Origin.

Host validation remains RMCP-owned and loopback-scoped. The external edge may preserve
a loopback Host or use the configured local authority; it must not be able to choose an
arbitrary Host/profile/credential through the request.

### Compatibility launcher is duplicate product surface

`swarm-kernel-host gateway` still locates a sibling `swarm-gateway`, forwards
`--config`/`--data-dir` and waits for its exit. It is not process custody: it verifies
no installed coordinate or image and owns no recovery state. Build tooling explicitly
checks that this launcher remains present, so removing only the enum branch is
insufficient.

After callers use the standalone frontend, delete the kernel-host subcommand,
`run_gateway_binary`, forwarded diagnostics, build-policy assertions and compatibility
fixtures together.

## Target configuration

One current schema should contain only operational fields needed by this bridge:

```toml
[gateway]
enabled = true
bind = "127.0.0.1:8787"
profile = "gateway-observer"
credential_file = "ABSOLUTE_PRIVATE_FILE"
local_bearer_file = "ABSOLUTE_PRIVATE_FILE"
max_connections = 8
max_sessions = 8
max_body_bytes = 1048576
request_timeout_seconds = 60
idle_session_seconds = 900
allowed_origins = ["https://edge.example:443"]
```

The exact defaults are implementation decisions, but every bound must be validated and
finite. `allowed_origins = []` means reject every request carrying Origin, not disable
validation. One process restart is required to change credential/profile/bearer.

Provider secrets never enter gateway configuration. The fixed Eliot credential remains
`{client_id, token}` and the bearer remains one opaque local secret.

## V0 — one private bounded secret reader

Use a small shared primitive based on `swarm-process::permissions`:

1. require an absolute configured path;
2. reject traversal and link/reparse components, including final target;
3. open the exact regular file without following a link;
4. verify current-user/private ownership and ACL/mode;
5. read under a strict cap before parsing;
6. recheck opened-file identity while reading;
7. validate closed credential JSON and token upper/lower bounds;
8. never log secret bytes or include them in errors.

`read_private_durable` already supplies useful bounded regular-file/no-follow/identity
mechanics, but it accepts relative paths and does not validate permissions of an
existing file. Extend or wrap the existing owner; do not create two gateway-local
readers.

## V1 — explicit single tenancy and bounded sessions

- one process/listener = one credential + one restricted profile + one bearer;
- cap TCP connections and active RMCP sessions independently;
- configure RMCP initialization and inactivity timeouts explicitly rather than relying
  on dependency defaults;
- reject new session creation before constructing a facade when at capacity;
- release the session permit on every worker termination, DELETE, failed initialize,
  idle timeout and shutdown path;
- keep request body/header/time bounds;
- preserve exact `/mcp`, Host, Origin and bearer checks before Store IPC;
- expose no request field that selects Eliot identity/profile;
- disconnect cancels only transport waiting; admitted Store Operations continue;
- timeout/disconnect never authorizes mutation replay.

Do not add a durable session store merely to implement a count. An in-process bounded
manager around RMCP's existing local manager is sufficient for the single process.

## V2 — exact edge policy

Keep the listener loopback-only. Configure the exact allowed browser Origins expected
from the external edge. Missing Origin remains valid for non-browser MCP clients;
present Origin must match exactly under RMCP's scheme/host/port semantics.

The external edge owns TLS, public ingress, global rate limits and any mapping from
external users to gateway processes. It does not receive an Eliot credential and does
not turn several users sharing one bearer into distinct Eliot principals.

## V3 — standalone entrypoint only

Retain `swarm-gateway` as the sole launcher. Delete the kernel-host compatibility
launcher and its build assertions after concrete callers/package instructions use the
standalone executable. Gateway opens Eliot IPC through `swarm-client`; it never embeds
Store or host runtime code.

## Unknown outcome and shutdown

A request timeout, HTTP disconnect or gateway stop can occur after Store admitted a
mutation. The caller must retain its `client_request_id` and resolve the exact Operation
through ordinary readback. Restricted MCP profiles already require caller-owned IDs for
mutations/effects; do not weaken that rule for remote convenience.

Gateway shutdown cancels RMCP/HTTP transports and may make current replies unknown. It
does not cancel, accept, retry or transfer Store work. A restarted gateway uses the
same configured principal only when the operator intentionally starts the same
configuration; it has no transport-session continuity promise.

## Connected verification

Minimum implementation evidence:

- relative, link/reparse, non-regular, over-limit and non-private secret files reject;
- credential and bearer token upper/lower bounds hold without secret logging;
- wrong/duplicate bearer, wrong Host, wrong Origin, query and wrong path reject before
  Store IPC;
- fixed credential/profile mismatch rejects before bind; `full` is impossible;
- max connections and max sessions are independent and every terminal/failed/idle path
  releases exactly one permit;
- configured idle timeout closes abandoned sessions; no session map grows past the
  configured maximum;
- request body/header/read and dispatch timeouts hold under streaming input;
- timeout after Store commit is recoverable through original request identity and is
  never replayed;
- concurrent HTTP/MCP sessions are demonstrated as one Eliot principal;
- stopping gateway leaves admitted Store work readable and does not cancel it;
- standalone binary works without kernel-host compatibility command;
- kernel-host launcher/build-policy references are absent after cutover.

During construction run rustfmt and minimal warnings-denied Clippy/build for
`swarm-gateway` and affected shared packages. Live tunnel/browser/slow-client and
shutdown qualification follows the connected product path; documentation and RMCP
defaults are not execution proof.
