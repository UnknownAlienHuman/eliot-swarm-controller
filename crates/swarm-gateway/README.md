# `swarm-gateway`

`swarm-gateway` is the optional loopback Streamable HTTP frontend for one fixed Eliot
credential and one fixed restricted MCP profile. It depends on `swarm-mcp` for the
profiled facade and on `swarm-client`/`swarm-contracts` for authenticated local IPC.
It does not embed the Store, adapters or a gateway database.

The normative target and current source findings are in:

- [`docs/gateway.md`](../../docs/gateway.md);
- [`docs/agent-operations/audit-current.md`](../../docs/agent-operations/audit-current.md#findings-ar-160170);
- [`docs/agent-operations/gateway-dependency-register.md`](../../docs/agent-operations/gateway-dependency-register.md).

## Current command

```text
swarm-gateway --config <config.toml> [--data-dir <directory>]
```

`swarm-kernel-host gateway` still exists as a compatibility launcher that locates a
sibling executable and forwards the two paths. It is a retirement target, not process
custody. The standalone executable becomes the only entrypoint in implementation V3.

## Current retained behavior

- loopback-only bind;
- exact `/mcp` path and no query;
- one configured credential/profile, with expected client ID validation;
- rejection of the unrestricted Full profile;
- one Authorization bearer gate with constant-work comparison;
- RMCP Host and Origin validation;
- maximum 1 MiB streamed request body and bounded HTTP headers/read time;
- 32 concurrent TCP connections and HTTP keep-alive disabled;
- timeout/disconnect reported as potentially unknown without silently replaying the
  Store request;
- admitted Store work survives HTTP disconnect and gateway shutdown.

Every successful HTTP/MCP session uses the same Eliot principal. HTTP metadata cannot
select another credential, profile or Store role.

## Current gaps

The current source is not the completed V0–V3 product:

- credential and bearer paths may be relative;
- credential uses unbounded `std::fs::read`; both secret readers follow ordinary file
  semantics and do not verify regular/no-link/private owner/ACL/mode;
- credential token/file have no upper bound before allocation;
- allowed browser Origins are fixed to loopback patterns;
- connection count is hard-coded and there is no independent RMCP session cap;
- RMCP 3.5.0 provides finite default cleanup, but gateway does not configure the
  initialization/inactivity policy as product settings;
- request timeout does not bound an already-created SSE response lifetime;
- the compatibility launcher and build-policy assertions remain;
- no connected gateway security/capacity/readback or live edge qualification is
  recorded.

Do not treat these gaps as operating instructions. Implement the vertical V0–V3 slices
through the shared owners named in the target documents, then delete replaced readers,
launcher code and compatibility checks in the same cut.

## Dependency boundary

The project pins official RMCP 3.5.0. Its local session manager removes a session after
worker termination and defaults to a 60-second initialize timeout, five-minute idle
timeout and 60-second completed-stream cache. Its session map has no maximum. Eliot must
add a separate active-session admission bound rather than importing a session database
or assuming dependency defaults are sufficient policy.

The selected private-file donor is `swarm-process::permissions`. Its bounded no-follow
exact-file mechanics are reusable, but the gateway wrapper must additionally require an
absolute path and verify permissions of an existing secret file.

## Verification boundary

This documentation checkpoint did not build or run the gateway. During implementation,
run rustfmt and minimal warnings-denied Clippy/build for `swarm-gateway` and affected
shared packages, then focused connected cases for secret files, Host/Origin/bearer,
connection/session release, timeout-after-commit readback, shutdown and standalone
cutover. Live browser/tunnel and slow-client qualification follows the connected path.
