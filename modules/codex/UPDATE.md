# Codex bridge update and rollback

## Pinned provenance

| Fact | Value |
|---|---|
| Donor | `codex-python-sdk` (`docs/agent_swarm.donors-20260929.toml`) |
| Upstream repo / unit | `https://github.com/openai/codex` / `sdk/python` |
| Upstream commit | `18194bfd3534ca567d886eac454028dafaa68b6c` |
| Package / version | `openai-codex`, `0.0.0-dev` (vendored source, not installed from a registry) |
| Matching binary pin | `openai-codex-cli-bin==0.153.4` (donor manifest provenance; this bridge attaches to an existing server) |
| License | Apache-2.0 (`vendor_bridge/LICENSE`) |
| Bridge artifact | `codex-sdk-18194bf-bridge.2` |
| Runtime dependencies | exact pins in `requirements.txt` |

## ELIOT-owned changes in bridge.2

The observer CLI remains read-only by default. The registered controller uses
a separate native method allowlist and durable operation checkpoint. It
selects only the exact route `modelProvider` and `model`, uses the host's
canonical task snapshot bytes for dispatch prompt construction, and records
both the native turn ID and native user-item ID. A missing acknowledgment is
resolved by unique `clientUserMessageId` history readback, never by replay.
Reconnect attaches to the existing endpoint and validates the saved
server/version scope; it does not start or stop the server or resume a thread.

The `.159` installed app-server schema was checked for server-initiated
requests. The bridge explicitly declines execution/file-change/patch
approvals and elicitation, returns an empty permission profile, declines
dynamic-tool execution, and sends no answers to user-input prompts. Unknown
server request methods fail the local reader closed; they do not receive an
empty object that could be mistaken for successful handling.

`vendor_bridge/` is not edited. Its donor source, generated models, and pins
remain unchanged; `verify_vendor.py` continues to hash the donor bytes.

## Updating the donor

1. Select an upstream commit and matching generated server protocol on
   purpose. Review `sdk/python` source, generated protocol models, license,
   and donor pairing before changing any pins.
2. Replace `vendor_bridge/` wholesale with that upstream unit plus its license;
   update `UPSTREAM_COMMIT` and regenerate `SHA256SUMS` from donor bytes.
3. Update runtime dependencies from the donor's lockfile. Re-derive the
   transport adapter and server-request reply formats from the new client and
   protocol schemas; do not change the donor to make the bridge pass.
4. Update the pins above, bridge constants, module example, and README
   together. Bump the artifact ID when the module contract changes.
5. Run the vendor verifier and fixture tests. Fixture responses must validate
   against the new generated models; update captured synthetic fixtures when
   the schema changes rather than loosening validation.
6. Treat fixture success and live app-server/provider qualification as
   separate evidence.

## Rollback

Reverting the ELIOT-owned `modules/codex/` change restores the prior module
artifact. It does not alter the shared app-server, shared Codex home, or
controller database. The bridge closes only its own client connection.
