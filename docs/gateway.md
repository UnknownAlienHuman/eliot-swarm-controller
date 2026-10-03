# Local Remote Agent Gateway

`swarm gateway` serves Streamable HTTP MCP on a loopback address and forwards
calls through the existing authenticated ELIOT IPC client. It does not open the
database, start native agents, or add a shell or generic method passthrough.

The gateway is disabled by default. When enabled, its local configuration fixes
one MCP profile, one ELIOT credential file, and one local bearer-token file for
the process lifetime. An HTTP caller cannot choose a client ID or profile. The
selected profile must be configured under `[mcp.profiles]` and cannot use
`full`; both the MCP profile filter and ELIOT application authorization apply.

## Local setup

Copy [`config/gateway.example.toml`](../config/gateway.example.toml) to a
user-private location outside the repository. Replace both `C:/REPLACE/...`
paths with absolute paths in that private location. Keep the ELIOT credential
and bearer token in separate files with user-only permissions. The example
uses the read-only `observer` profile and a dedicated client ID.

Start the local ELIOT host with that configuration, then register the
dedicated observer credential while the host is running:

```text
swarm --config <private-config.toml> client-create remote-gateway-observer --role observer --out <private-credential.json>
```

Set `gateway.credential_file` to the output file. Create a separate random
bearer token of 32 to 512 printable ASCII characters and store it in the file
named by `gateway.local_bearer_file`. The optional final line ending is
ignored. Do not put either secret in TOML, a command argument, source control,
or logs.

Run the gateway in the foreground:

```text
swarm --config <private-config.toml> gateway
```

The running command refuses a disabled configuration, a non-loopback bind,
the `full` MCP profile, a missing credential or bearer file, or a credential
whose client ID does not match the configured profile. It also rejects the
global `--credential` and `--request-id` overrides. The bearer token maps only
to the configured ELIOT credential and profile.

## Limits and authentication boundary

- `gateway.bind` must be a loopback socket address; the default is
  `127.0.0.1:8787`.
- `gateway.max_body_bytes` defaults to 1 MiB and cannot exceed 1 MiB or the
  configured local IPC frame limit. It cannot be set below 1 KiB.
- `gateway.request_timeout_seconds` defaults to 30 and is bounded from 1 to
  300 seconds.
- The local bearer file is a temporary local authentication mapping. It is
  not Cloudflare Access JWT validation, OAuth, or a claim about an external
  identity provider. Keep this listener private and loopback-only.
- The gateway uses only the configured local credential for IPC. Profile
  filtering blocks hidden MCP methods before IPC, while ELIOT still enforces
  application roles, object scope, request identity, and GM epoch.

The example's `observer` profile exposes only the read surface documented in
[`mcp-profiles.md`](mcp-profiles.md). Mutations require a separately reviewed
configuration change and a dedicated named client; incoming requests still
cannot widen the selected profile.
