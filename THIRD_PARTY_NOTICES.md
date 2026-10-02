# Third-party source and runtime notices

## Atlas redaction — complete selected Rust unit

Upstream: `pacifio/atlas`, commit **a34a6d44bf37d26d9a6f8f6fe1fab5ce0a92d8d1**. Selected unit: the complete `crates/atlas-redact` directory, version 0.1.0. Its source, rules, README, tests, fixtures, script and manifest are retained **unchanged** under `vendor/atlas/crates/atlas-redact/`.

Copyright 2026 Adib Mohsin. Atlas is Apache-2.0; the complete [upstream license](vendor/atlas/LICENSE) accompanies this source and binary distribution. Bundled betterleaks rules retain their separate [MIT license and copyright notice](vendor/atlas/crates/atlas-redact/LICENSE-betterleaks). Do not drop either license when redistributing the binary or source.

`vendor/atlas/UPSTREAM_COMMIT` identifies the source snapshot; `vendor/atlas/SHA256SUMS` covers every imported file. `modules/atlas-redact/Cargo.toml` is **ELIOT's separate build wrapper**, not an edited upstream manifest. It resolves upstream `regex` and `serde_json` dependencies through the controller's ordinary Cargo.lock and exposes the original lib.rs without Atlas's unrelated workspace. `src/redaction.rs` is ELIOT-owned integration glue. The donor processes copies of bounded native diagnostic/question payloads, never rewrites prompts or the vendor's inference stream. Detection limits and false positives still require qualification.

## Muse SDK — official external package

The existing Muse module uses the complete `@muse-code/sdk` 1.3.0 package through its module-local lockfile. Canonical donor source is `meta-models/muse-code-sdk@a7c10c5dd3f66be412077d29f9d11111af70317b` (MIT). SDK installation and its package notices stay module-local; the source and binary archives do not bundle node_modules. See [Muse setup](modules/muse/README.md) and [JavaScript provenance](docs/javascript-provenance.md).

## rmcp — MCP facade dependency

The C02 MCP facade uses the official `rmcp` 3.5.0 crate (modelcontextprotocol/rust-sdk, Apache-2.0) as an ordinary Cargo dependency with only the `server` and `transport-io` (stdio) features enabled; it is not vendored or modified, and no optional MCP features are enabled by default. The exact version and checksum are locked in `Cargo.lock`.

## Codex Python SDK — pinned donor unit under `modules/codex/vendor_bridge`

The Codex bridge keeps the complete upstream Python SDK unit (`sdk/python` from `openai/codex` at `18194bfd3534ca567d886eac454028dafaa68b6c`, package `openai-codex`, version `0.0.0-dev`, Apache-2.0) byte-identical under `modules/codex/vendor_bridge`, with the upstream repo-root license, an `UPSTREAM_COMMIT` file and a `SHA256SUMS` manifest covering every vendored file (`modules/codex/verify_vendor.py` checks both the file set and the hashes). The SDK is a dev snapshot and is used from source in the module-local environment; it is never installed from a registry. The donor's matching binary pin is `openai-codex-cli-bin==0.153.4`. ELIOT's WebSocket transport adaptation for the shared app-server lives entirely outside the donor, in `modules/codex/bridge.py`. See [Codex bridge](modules/codex/README.md) and its [update record](modules/codex/UPDATE.md).
## Claude Agent SDK — official external package, Commercial Terms

The Claude module uses the complete `@anthropic-ai/claude-agent-sdk` **0.3.287** package through its module-local lockfile, together with the SDK's matching bundled native binary packages `@anthropic-ai/claude-agent-sdk-<platform>` at the same version **0.3.287**. The package is © Anthropic PBC and is used under Anthropic's Commercial Terms (`SEE LICENSE IN README.md` inside the package; legal agreements at https://code.claude.com/docs/en/legal-and-compliance). It is **not** permissive open-source code and must not be relabelled as such; no SDK source is copied into this repository. SDK installation and its package notices stay module-local; the source and binary archives do not bundle node_modules. See [Claude setup](modules/claude/README.md), its [update contract](modules/claude/UPDATE.md) and [JavaScript provenance](docs/javascript-provenance.md).

## OpenCodex — documented Management API contract, not vendored

The OpenCodex module (`modules/opencodex/`) is an original, dependency-free client written against the documented Management API of `lidge-jun/opencodex` **v2.73.0** (commit `569e3e7dae48bafc54b8a1a7e3a85129befe2d98`). OpenCodex is **MIT**, © 2026 opencodex contributors. No upstream source is copied, vendored or bundled by this repository, so no license text ships with the module; the upstream license governs the service itself, which the operator installs and runs separately. See [OpenCodex setup](modules/opencodex/README.md) and its [update record](modules/opencodex/UPDATE.md).

Other ordinary Rust dependencies are identified, versioned and checksum-locked in `Cargo.lock`; their package licenses remain with their upstream distributions. No new CCCC, ACP, agent scheduler, UI or model loop was copied in the OpenCode HTTP implementation.
