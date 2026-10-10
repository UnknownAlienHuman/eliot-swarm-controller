# Codex SDK donor security delta

The base is `openai/codex/sdk/python` at
`18194bfd3534ca567d886eac454028dafaa68b6c`. On 2026-10-10 ELIOT updated the
dev/test `datamodel-code-generator` dependency from `0.31.2` to `0.81.0`.
The initial `0.64.0` update closed the nine original pip alerts. GitHub then
reported `GHSA-77xj-x4rm-935c`, published 2026-10-08, whose affected range is
`>=0.59.0, <=0.80.0`; the first patched version is `0.81.0`.
The reviewed advisory registry returned no affected advisories for `0.81.0`
at 2026-10-10 23:39 UTC, with complete pagination.

The stock uv resolver updated the generator and `typing-extensions` from
`4.15.0` to `4.16.0`, required by the generator's `typing-extensions<5,>=4.16`
constraint. Every other package version remained unchanged. No manual lockfile
edit, dependency override or extra pin was introduced.

The source and generated SDK models retain their upstream bytes. The two
dependency files have these original and patched SHA-256 values:

| File | Upstream SHA-256 | Patched SHA-256 |
| --- | --- | --- |
| `pyproject.toml` | `da5e5b8eca67f727c49befc1bec71ed72957c1358b3c055433af97a5c0bd4354` | `c9e5edab50ef054a99533857dc1e1046bf2b31a1736cbc39ea8f318e9a58cbf5` |
| `uv.lock` | `ca4f631445c81b74b11b05305cc580fa399f1f9bfe80b12acea610ce331e0354` | `41873846d00a9b5744cc12ad8efe776bdd0ce588c8923ab23790f38a05f1bee2` |

`vendor_bridge/SHA256SUMS` covers all 89 files in the patched unit.
`verify_vendor.py` verifies both file coverage and hashes. `UPSTREAM_COMMIT`
identifies the base snapshot; this record identifies its local delta.

Validation covered lock consistency, the installed generator version and all
16 CLI arguments used by `scripts/update_sdk_artifacts.py`, including actual
argument-parser execution, in an isolated uv environment on Python `3.13.15`.
The release and project metadata both retain Python `>=3.10`; actual Python
3.10 execution remains unverified. Generated output compatibility remains unverified because SDK
regeneration was outside this dependency update. The matching native binary
pin remains `openai-codex-cli-bin==0.153.4`.

The intermediate `0.64.0` hashes and verification remain historical in
[the original repair record](../../docs/remediation/2026-10-10/GENERATOR-SECURITY-REPAIR.json).
