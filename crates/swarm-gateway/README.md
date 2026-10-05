# `swarm-gateway`

`swarm-gateway` is the optional loopback Streamable HTTP transport extracted
from the controller's `src/gateway.rs`. It depends on `swarm-mcp` for the
session-fixed MCP facade and profile checks, and on the shared client/contracts
for local IPC types and credentials. It does not depend on the monolithic
controller, kernel, adapters, or a database.

The existing `swarm gateway` command remains available as a compatibility
launcher when `swarm-gateway` is installed beside `swarm`. It forwards only
the selected `--config` and `--data-dir`. The standalone command accepts those
same options:

```text
swarm-gateway --config <private-config.toml>
```

The loader reads the current TOML shape and defaults only the fields used by
the frontend: `[gateway]`, `[storage].data_dir`, `[ipc]`, and `[mcp]`. Relative
credential and bearer paths resolve against the config file directory, and
relative data directories follow the shared `swarm-mcp` loader. A missing
gateway section defaults to disabled. Unknown root controller tables are
ignored; unknown `[gateway]` fields are rejected.

At startup, the configured credential and bearer file are loaded once. The
bearer is only a local HTTP transport gate; it maps every request to that fixed
credential and one configured restricted MCP profile. HTTP metadata cannot
select a principal, profile, ELIOT method, or request identity. The shared MCP
facade enforces its profile, and the Store remains authoritative for
application authorization, operation admission, idempotence, receipts, and
readback. The gateway never retries an MCP request after an uncertain dispatch.

The original bounds are retained: loopback-only bind, maximum 1 MiB body
bounded by IPC frame size, a 1–300 second dispatch deadline, 32 concurrent
connections, 16 KiB HTTP headers/64 headers, 10 second header-read timeout,
RMCP body/origin/Host/session enforcement, and one HTTP request per
connection. Dropping the HTTP response does not cancel admitted host work.

`Cargo.toml` pins the same hyper/RMCP versions as the extracted source and
depends on sibling `swarm-mcp`, `swarm-client`, and `swarm-contracts` packages.
The crate has no nested workspace; root integration adds one workspace member
and root's existing `swarm` CLI forwards to the adjacent gateway executable.
Root lockfile resolution and binary packaging remain integration work.

No build, test, HTTP server, host IPC, database, credential, bearer-token, or
native process was accessed while preparing this source candidate.
