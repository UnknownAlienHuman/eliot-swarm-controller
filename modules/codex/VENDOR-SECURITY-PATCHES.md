# Codex SDK donor security delta

The base is `openai/codex/sdk/python` at
`18194bfd3534ca567d886eac454028dafaa68b6c`. On 2026-10-10 ELIOT updated the
dev/test `datamodel-code-generator` dependency from `0.31.2` to `0.64.0`.
This version is outside all nine affected ranges in the repository's
2026-10-10 Dependabot snapshot. The stock uv resolver retained every other
package version and updated the lock format and applicable dependency markers.

The source and generated SDK models retain their upstream bytes. The two
dependency files have these original and patched SHA-256 values:

| File | Upstream SHA-256 | Patched SHA-256 |
| --- | --- | --- |
| `pyproject.toml` | `da5e5b8eca67f727c49befc1bec71ed72957c1358b3c055433af97a5c0bd4354` | `abedd9d3f6e32761239dfa7bcb25094c3675176302e32ba16bfc064ea866e6f4` |
| `uv.lock` | `ca4f631445c81b74b11b05305cc580fa399f1f9bfe80b12acea610ce331e0354` | `d758f8a09697c24368f0bd527925d2da5fad2bff4366897927e72c1c4b878fae` |

`vendor_bridge/SHA256SUMS` covers all 89 files in the patched unit.
`verify_vendor.py` verifies both file coverage and hashes. `UPSTREAM_COMMIT`
identifies the base snapshot; this record identifies its local delta.

Validation covered lock consistency, the installed generator version and all
16 CLI arguments used by `scripts/update_sdk_artifacts.py`, in an isolated uv
environment. Generated output compatibility remains unverified because SDK
regeneration was outside this dependency update. The matching native binary
pin remains `openai-codex-cli-bin==0.153.4`.
