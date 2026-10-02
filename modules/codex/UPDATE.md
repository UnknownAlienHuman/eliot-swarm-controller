# Codex bridge — update and rollback

## Pins

| Fact | Value |
|---|---|
| Donor | `codex-python-sdk` (`docs/agent_swarm.donors-20260929.toml`) |
| Upstream repo | `https://github.com/openai/codex` |
| Source unit | `sdk/python` |
| Upstream commit | `18194bfd3534ca567d886eac454028dafaa68b6c` (`vendor_bridge/UPSTREAM_COMMIT`) |
| Package / version | `openai-codex`, `0.0.0-dev` (dev snapshot — never installed from a registry) |
| Matching binary pin | `openai-codex-cli-bin==0.153.4` (the donor's own paired pin; names the server generation this SDK snapshot was generated against — it is server-side provenance, the bridge installs no binary) |
| License | Apache-2.0 (`vendor_bridge/LICENSE`, upstream repo-root license) |
| Bridge artifact | `codex-sdk-18194bf-bridge.1` |
| Runtime deps (`requirements.txt`) | `pydantic==2.13.4`, `packaging==26.2` (exact versions from the donor's own `uv.lock` at the pin), `websockets==17.1` (transport library, local choice) |

## Local changes to the donor

None. `vendor_bridge/` is byte-identical to the upstream unit (plus the
repo-root `LICENSE`, `UPSTREAM_COMMIT` and the generated `SHA256SUMS`);
`python3 verify_vendor.py` proves it. The WebSocket transport adaptation
is entirely in ELIOT-owned `bridge.py`: `SharedCodexClient` subclasses
the pinned `CodexClient` and overrides only `start`, `close`,
`_start_reader_thread`, `_write_message` and `_read_message`. The
read-only method allowlist and the decline-all approval handler also live
in `bridge.py`. If a future pin moves those touch-points, the adaptation
is re-derived there, never by editing the donor.

## Update procedure

1. Choose the new upstream commit deliberately; read its `sdk/python`
   changelog surface (`pyproject.toml`, generated protocol diff) — a new
   server generation pairs with a new matching-binary pin in the donor
   manifest.
2. Replace `vendor_bridge/` wholesale with the new `sdk/python` unit plus
   the repo-root `LICENSE`; write the new commit into `UPSTREAM_COMMIT`;
   regenerate `SHA256SUMS` (every file except `SHA256SUMS` itself).
3. Update `requirements.txt` from the new unit's `uv.lock` (pydantic /
   packaging) and re-check `bridge.py`'s transport touch-points and the
   read-only allowlist against the new `client.py`.
4. Update the pins table above, `bridge.py`'s pin constants,
   `module.example.json`'s `moduleArtifactId`, the README and
   `THIRD_PARTY_NOTICES.md` in the same change.
5. Re-run: `python3 verify_vendor.py`, the fixture unittest suite, and
   the fixture CLI smoke (`describe` / `open` / `snapshot`). Fixture
   payloads must validate against the *new* generated models; where they
   no longer do, the schema changed and the fixture is updated to the new
   schema — never loosened to accept both.
6. Live qualification against an installed server is a separate step and
   does not follow from fixture success (matrix:
   `installed_runtime_verified`).

## Rollback

The module is self-contained: reverting the commit that changed
`modules/codex/` (and the matching `THIRD_PARTY_NOTICES.md` entry)
restores the previous pin and bridge together. No controller data,
migration or server state is involved; the bridge writes nothing to the
shared server in this slice, so rollback has no server-side effect.
