# OpenCode V2 adapter — update and rollback

Contract sources: architecture §§8 and 15, implementation plan C04,
module contract §§3 and 11, and the [module README](README.md). This
file is the per-module record those documents require: pins, what may
change, how a change is verified, how the new version is activated for
new bindings, and how to roll back.

## Pins

| Fact | Value |
|---|---|
| Adapter artifact | `eliot-opencode-v2.http.1` — the compiled `ARTIFACT_ID` constant in `src/runtime/opencode_v2.rs`; the route `module_artifact_id` must equal it exactly or the host rejects the configuration |
| Contract basis | Official [V2 API](https://opencode.ai/v2/docs/api) and [OpenAPI document](https://opencode.ai/v2/openapi.json), captured 2026-10-01 |
| Reviewed upstream | `anomalyco/opencode@4c0d0ff478ca9150c163fb8b04a76395e4dccafe`; the exact reviewed files are enumerated in the module README |
| Server | External and operator-owned. The route's `expected_version` must equal the installed server's `/api/info` version exactly. Server version, adapter artifact and controller version are three separate facts |
| Atlas donor | Used to scrub retained native copies; governed by [its own UPDATE.md](../atlas-redact/UPDATE.md) and never changed as a side effect of an adapter update |

## What may change

- `src/runtime/opencode_v2.rs` and `src/runtime/opencode_v2/` — the
  adapter mappings, in the controller crate.
- `src/store/opencode.rs` and its tests — binding selection and
  recovery for this runtime.
- The module README, the configuration examples and this file.

The adapter never installs, starts, restarts or upgrades the OpenCode
server, and never launches the OpenCode CLI. An advertised or captured
schema does not install a new server binary (architecture §15). The
server's lifecycle belongs to its owner; a server-side upgrade is a
separate, coordinated event, not part of adapter activation.

## When the upstream contract moves

1. Re-capture the V2 API/OpenAPI and re-review the affected native
   sources at the new upstream commit. Update the reviewed-commit
   references in the module README in the same change; the old commit
   references are not left standing as if still reviewed.
2. Adjust only the mappings whose contract actually changed. Unknown
   new optional fields are preserved/ignored per schema rules and do
   not disable the backend; a changed mandatory field makes only the
   affected operation unconfirmed, not the whole route (module
   contract §11).
3. Bump `ARTIFACT_ID` (`http.1` → `http.2`) in the same change
   whenever the adapter's contract basis changes, and update the
   configuration examples and README artifact references with it.
   Never change adapter behavior under an existing artifact id.

## Verification (before activation)

```bash
cargo fmt -p eliot-swarm-controller -- --check
cargo clippy --locked --lib --bins --no-deps -- -D warnings
cargo build --locked --release --bin swarm
cargo test --locked --lib opencode   # local evidence; CI defers tests by repository policy
```

Fixture servers are not OpenCode: passing fixtures prove the mapping
against the recorded contract, not a live service. Live qualification
against an installed server of the exact `expected_version` remains a
separate step and is never implied by these checks.

## Activation

The adapter is compiled into the host, so activation is a host-build
plus route change:

1. Deploy the new host build and set the route `module_artifact_id`
   to the new `ARTIFACT_ID`. Configuration load rejects any other
   value, so a mismatched pair never half-activates.
2. New bindings record the new artifact id. Bindings are selected for
   attach and recovery by exact artifact id, so this build does not
   adopt bindings recorded under the previous artifact: they remain
   recorded and unreleased in the Store, served only by a build that
   carries their artifact id. Nothing is deleted or silently migrated.
3. Treat an artifact bump as a boundary: let work on the old artifact
   finish or release first, or keep the previous host build available
   for those bindings until it does.

## Rollback

Redeploy the previous host build and restore the previous route
artifact id; new bindings then use the old adapter again. Rollback
does not rewrite binding records and does not undo native effects
already performed through the new adapter (module contract §11).
Sessions created on the server remain native facts and are reconciled
by GET/readback only — never replayed and never deleted to make a
rollback look clean.
