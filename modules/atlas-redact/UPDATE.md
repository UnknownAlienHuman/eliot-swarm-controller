# Atlas redaction donor — update and rollback

Contract sources: module contract §11 and
[THIRD_PARTY_NOTICES.md](../../THIRD_PARTY_NOTICES.md). This donor is a
library unit compiled into the host, not a runtime module: it has no
bindings, no route and no per-binding activation.

## Pins

| Fact | Value |
|---|---|
| Upstream | `pacifio/atlas`, commit `a34a6d44bf37d26d9a6f8f6fe1fab5ce0a92d8d1` (`vendor/atlas/UPSTREAM_COMMIT`) |
| Selected unit | Complete `crates/atlas-redact` plus the upstream root `LICENSE`, retained unchanged under `vendor/atlas/` |
| Integrity | `vendor/atlas/SHA256SUMS` covers every imported file except itself; CI verifies the file set and every hash |
| Build wrapper | `modules/atlas-redact/Cargo.toml` — ELIOT's own manifest pointing into the vendor tree; it is not an edited upstream manifest |
| Local glue | `src/redaction.rs` only. Donor files are never edited; behavior changes arrive as a new snapshot or stay in the glue |

## Update procedure

1. Choose the new upstream commit deliberately and replace
   `vendor/atlas/` wholesale with the new selected unit plus the
   upstream root license. No file of the old snapshot is carried over
   by hand.
2. Write the new commit into `vendor/atlas/UPSTREAM_COMMIT` and
   regenerate `SHA256SUMS` over every imported file except
   `SHA256SUMS` itself.
3. Update the Atlas section of `THIRD_PARTY_NOTICES.md` (commit, unit,
   licenses) in the same change. Keep the wrapper unchanged unless
   the unit's layout moved; if it did, adjust only the wrapper path.
4. Verify with the same check CI runs (the "Verify Atlas donor
   snapshot" step of `.github/workflows/rust.yml`), then
   `cargo build --locked --release --bin swarm`.

Hashes prove the snapshot is the chosen upstream unit, nothing more:
changed detection rules or false-positive behavior are a qualification
matter and are not established by this procedure.

## Activation and rollback

Activation is the host build that carries the new snapshot; there is
no binding migration and no controller data involved. Rollback is
reverting the snapshot change — the previous commit, sums and notices
return together, and no Store state has to be repaired.
