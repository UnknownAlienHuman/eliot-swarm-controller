# Remote Agent Gateway

The gateway is an optional, small, single-tenant bridge from one remote MCP client to
one fixed Eliot client identity and MCP profile. It is an integration, not Core.

Source owners:

- `crates/swarm-gateway` — standalone process;
- `swarm-mcp::profiled_facade` — MCP surface;
- `swarm-client` — Eliot IPC;
- `swarm-process::permissions` — target private-file loader.

## Boundary

```text
external TLS / tunnel / edge identity
            |
            v
loopback-only swarm-gateway
  fixed bearer
  fixed Eliot credential
  fixed MCP profile
            |
            v
local Eliot IPC
```

One gateway process/listener represents one remote agent identity/profile. Multiple
TCP/MCP sessions using its bearer are not distinct Eliot principals and share the same
request namespace. Run another gateway instance for another identity/profile.

The gateway is not:

- a public Internet listener;
- TLS termination;
- an identity broker;
- a multi-tenant API platform;
- a general reverse proxy;
- an Eliot credential minting service;
- a second authorization database.

## Current useful behavior

The current standalone process already enforces useful limits:

- loopback bind only;
- fixed profile chosen at startup;
- application bearer authentication;
- Host and Origin validation;
- bounded request body;
- bounded TCP connections;
- request timeout;
- no remote-supplied Eliot credential or profile.

A timeout is not proof that an Eliot mutation failed. The Store request may have
committed. The caller must use the original request identity/readback and never resend
an ambiguous mutation by guess.

## Confirmed source gaps

### Secret files are not yet safely loaded

Current credential loading uses an ordinary whole-file read. Bearer loading caps bytes
but still uses ordinary file open. The source does not yet enforce all properties that
the operational documentation requires:

- absolute/canonical path;
- no symlink or Windows reparse traversal;
- regular file only;
- private owner/ACL/mode;
- strict maximum size before allocation/read;
- upper as well as lower token length.

A FIFO/device/link can therefore violate the intended boundary even though the parsed
content is validated afterward.

### Connection limit is not a session/identity model

`max_connections` limits TCP sockets. It does not make clients distinct or bound total
MCP session lifetime. All successful sessions use the same Eliot credential/profile.
The product must state this explicitly and bound active sessions plus idle lifetime.

### A compatibility launcher remains

Kernel-host still exposes a compatibility `gateway` command that starts the standalone
binary. This keeps duplicate CLI/config/error paths. After a finite CLI transition,
`swarm-gateway` is the sole entrypoint.

## Target configuration

Keep only operational fields needed by the bridge:

```toml
bind = "127.0.0.1:PORT"
credential_file = "ABSOLUTE_PRIVATE_FILE"
bearer_file = "ABSOLUTE_PRIVATE_FILE"
profile = "manager|reviewer|observer|..."
max_connections = 8
max_sessions = 8
max_request_bytes = 1048576
request_timeout_seconds = 60
idle_session_seconds = 900
allowed_origins = ["https://edge.example"]
```

These are examples of shape, not a compatibility promise. One current schema replaces
the existing one during V0–V2; do not add a version matrix.

## V0 — private bounded file loading

Use one shared helper from `swarm-process::permissions` for credential and bearer:

1. require absolute path;
2. reject traversal/symlink/reparse components;
3. open exact regular file without following an unsafe final link;
4. verify private ownership/ACL/mode;
5. read under a strict byte cap;
6. validate the closed JSON/text shape and token length;
7. never log path contents or secret values.

The Eliot credential remains `{client_id, token}`. Bearer remains a single opaque
secret. Do not copy provider credentials into gateway configuration.

## V1 — explicit single tenancy and bounded sessions

- one process = one Eliot client ID + one MCP profile;
- one bearer authenticates that identity only;
- cap TCP connections and active MCP sessions independently;
- expire idle sessions;
- reject profile/credential changes without process restart;
- disconnect cancels only the transport wait, not an already admitted Store mutation;
- no replay on reconnect.

The external edge owns TLS, public routing, rate limiting and any user identity mapping.
It may start multiple gateway processes with distinct private credentials when several
remote agents are required.

## V2 — standalone entrypoint only

Retain `swarm-gateway` as the only launcher. Delete kernel-host gateway command,
forwarded flags, compatibility diagnostics and fixtures after concrete callers move.
The gateway opens Eliot IPC through the normal client; it does not embed Store or host
runtime code.

## Verification

Minimum connected checks after implementation:

- non-loopback bind is rejected;
- symlink/reparse, non-regular, over-limit and non-private secret files are rejected;
- token lower/upper bounds are enforced without logging secret data;
- wrong bearer, Host or Origin is rejected before Store IPC;
- remote input cannot choose Eliot credential/profile;
- max connections, max sessions, body size, request timeout and idle expiry hold;
- concurrent sessions are documented/tested as one Eliot principal;
- timeout after Store commit remains recoverable through original request identity;
- disconnect/reconnect never resends an ambiguous mutation;
- kernel-host compatibility launcher is absent after cutover.

Do not claim public-Internet safety from this process alone. That boundary belongs to
the external edge/tunnel.
