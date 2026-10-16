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
| Legacy Python bridge artifact | `codex-sdk-18194bf-bridge.3` |
| Standalone Rust artifact | `codex-rust-controller.1` |
| Standalone Rust descriptor version | `11` (current; prior versions remain historical contracts) |
| Standalone Rust package / binary | `swarm-adapter-codex` / `swarm-codex-adapter` |
| Standalone Rust source | `crates/swarm-adapter-codex` |
| Runtime dependencies | exact pins in `requirements.txt` |

The donor and bridge.2/bridge.3 sections below describe only the legacy Python
bridge artifact `.3` under `modules/codex`. The standalone Rust artifact
`.1`, current descriptor version `11`, is a separate package and does not vendor
or update this Python SDK unit.

## Legacy Python bridge artifact `.3`: ELIOT-owned changes in bridge.2

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

## Legacy Python bridge artifact `.3`: ELIOT-owned changes in bridge.3

The legacy `.3` controller reads native child threads only through
`thread/list` parent IDs and preserves an explicit partial-family status
because pagination is not an atomic family snapshot. It can publish bounded
native turn-history result pages for a previously acknowledged input operation
after validating the exact thread, turn, completion status and child-to-parent
activity link. Native tool item IDs, names, arguments, and results are retained
in the allowlisted result projection; reasoning content is omitted. Native
lifecycle notifications are a bounded current-connection observation window and
do not claim complete family or result coverage. Dynamic tool execution and
auxiliary-provider affinity remain unavailable.

The legacy `.3` donor source and generated models retain the pinned upstream
bytes. On 2026-10-10 the dev/test `datamodel-code-generator` pin and lockfile
were updated to `0.81.0` to address the audited security advisories, including
the newly reported `GHSA-77xj-x4rm-935c`. The binary pin
remains `openai-codex-cli-bin==0.153.4`. The
[security patch record](VENDOR-SECURITY-PATCHES.md) retains the original and
patched hashes; `verify_vendor.py` checks the complete patched unit.

## Standalone Rust controller artifact `.1`, current descriptor version `11`

The Rust adapter is separate from the legacy Python bridge and is sourced from
`crates/swarm-adapter-codex`, with artifact `codex-rust-controller.1` and
descriptor version `11`. It retains the v4 normalized dispatch/result schema
pair, adds TaskPrompt v1 and Goal continuation admission/terminal evidence
schemas, and uses the exact route contract `runtime = "codex"`,
`workspace_option = "workspaceRoot"`, and native options `modelProvider`,
`model`, and `workspaceRoot`. Model and provider overrides are independently
optional: omitted values inherit the native harness settings, while explicit
values are verified against native readback. Provider/model values must be actual native
Codex options; proxy or delegation IDs are not interchangeable. The selector
remains:

```json
{"kind":"codex_assistant_result","input_operation_id":"<exact task.dispatch operation ID>"}
```

The current controller retains the v6 result boundary described below; the
descriptor version change does not add child-history or tool-item projection.

The v6 result path retains the v4 scope: it reads one root-thread final
assistant response for the Store-sealed target `task.dispatch` Operation. It verifies the unique native
user item, the exact root turn and completed status, then selects the final
assistant response. The v6 result path does not enumerate child threads,
verify
`parentThreadId` or `subAgentActivity`, project child history, or retain
allowlisted tool-call items. The legacy `.3` child/history/tool behavior above
is not a v6 capability; v6 returns unsupported or unavailable rather than
inferring child causality. The v6 descriptor's capability/schema rows and
selector do not require a child ancestry field.

The verified native turn ID remains on the exact parent dispatch Operation and
is used for readback. The v6 normalized page source retains the response item
identity and sealed parent Operation identity; it does not fabricate a
self-contained turn ID. V6 pages are capped at 24 KiB and the complete
response at 4 MiB. The shared Store envelope is 64 KiB, but that is not the
v6 adapter's accepted page limit. V6 result pages report unknown Task
completion and no execution-complete or replay claim.

## Updating the donor

The following donor procedure applies only to the legacy Python bridge `.3`.
It does not update or roll back the standalone Rust `.1` package.

1. Select an upstream commit and matching generated protocol on purpose.
   Review `sdk/python` source, generated protocol models, license,
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

Reverting the ELIOT-owned `modules/codex/` change restores the prior legacy
Python `.3` module artifact. It does not alter the standalone Rust `.1`
package, the shared app-server, shared Codex home, or controller database.
The legacy bridge closes only its own client connection.
