# R13 companion. Owned-service departure: one artifact classifier and no swallowed reconciliation errors

**Status:** implementation handoff. Production owned-service rows, processes, bindings and workspace leases are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

Every already-admitted owned service reaches one truthful resource state:

```text
reserved
→ outcome_unknown | service_observed | failed_no_effect
→ departure_pending | service_departed | identity_unknown/corrupt
→ binding/capacity/workspace release only after exact departure
```

The same selected artifact predicate is used for admission, readback and departure. One corrupt candidate cannot masquerade as a successful reconcile pass or erase the primary error.

No new process registry, second capacity ledger or generic workflow engine is added.

## 2. Confirmed HIGH artifact split

The current opening path accepts an owned OpenCode module when:

```rust
owned_opencode_artifact(artifact)
```

which recognizes both:

```text
runtime::opencode_v2::ARTIFACT_ID
config::OPENCODE_RUST_ARTIFACT_ID
```

The departure reconstruction path later requires:

```rust
binding.module_artifact_id == runtime::opencode_v2::ARTIFACT_ID
```

It excludes the Rust artifact that the opening path explicitly admitted.

Consequences for a Rust-module owned service in `outcome_unknown` or `service_observed`:

```text
departure_source → LAUNCH/owned-service corruption
→ exact route/workspace not returned
→ observe_departure never called
→ row never reaches service_departed
→ binding/capacity/workspace remain held
```

The resource leak is permanent even after the real process exits.

## 3. Confirmed error swallowing

`departure_batch` currently performs:

```rust
let source = departure_source(db, &row).ok();
```

Every error — damaged retained data, SQLite failure, missing Operation, mismatched artifact, missing lease — becomes `(None, None)`.

The async reconciliation loop then nests most subsequent work under `if let Ok(...)` and `if let Some(...)`. Any failure simply falls through to `touch_departure_candidate`, whose result advances the candidate's timestamp/cursor for another later pass.

The method returns `Ok(departed_count)` even when every candidate was corrupt or unreadable. Manager/host sees a successful pass with zero departures and no reason.

Related loss:

- `close_owned_service_and_reconcile` discards both `close_gracefully` and reconciliation errors;
- the candidate touch/CAS result is not surfaced as a disposition;
- nested route/proof/readback errors have no bounded retained diagnostic;
- repeated permanent corruption burns writer turns forever.

## 4. One selected artifact classifier

Define one closed data-only classification at the narrow config/runtime owner:

```rust
enum OwnedServiceArtifactKind {
    LegacyBuiltinOpenCode,
    RustOpenCodeModule,
}
```

or reuse an existing exact selected-descriptor type if R31/R01 already supplies it.

The classifier must be consumed by:

- opening/admission;
- `owned_service_for_binding`;
- departure source reconstruction;
- process readback/departure observation;
- capability/route projection where the artifact changes behavior.

Unknown artifact is unsupported before admission. Historical known artifacts remain decodable. Do not replace two literals with a third duplicated allowlist.

Artifact kind is compatibility/effect-shape metadata, not Manager authority.

## 5. Typed departure-source load

Replace `.ok()` with an exhaustive internal result:

```rust
enum DepartureSourceLoad {
    Ready {
        artifact: OwnedServiceArtifactKind,
        route: OwnedServiceRoute,
        workspace_directory: PathBuf,
        retained_proof: Value,
    },
    Pending {
        reason: DeparturePendingReason,
    },
    Damaged {
        code: SafeErrorCode,
        evidence_digest: String,
    },
}
```

SQLite/read/commit errors remain `Err` and abort the transaction/pass as hard uncertainty; they are not `Damaged` business facts.

Classification examples:

- exact retained source valid, process still live → Pending;
- exact process departed → Departed after proof validation;
- current Task/manager/lease changed but immutable historical provenance is complete → still Ready for cleanup;
- malformed immutable row/manifest/link → Damaged, no process action;
- missing external dependency/readback that may recover → Pending;
- unsupported artifact admitted historically → explicit unsupported historical gap, not silent None.

Retained cleanup authority comes from immutable launch/binding/lease provenance. It does not require the original Manager still to be current.

## 6. Per-candidate reconciliation disposition

Return and persist one truthful result per candidate:

```text
Departed
StillLive
ReadbackPending
Damaged
StaleAlreadyClosed
```

The pass aggregate reports:

```text
progressed
idle
or degraded { counts by safe code }
```

Rules:

- `Departed` only after exact process birth/image/family departure proof and row CAS;
- `StillLive`/`ReadbackPending` retains the candidate without pretending progress;
- `Damaged` writes one bounded diagnostic/quarantine fact and does not hot-loop every pass;
- SQLite or failed CAS remains a hard error or exact concurrent-progress readback;
- no branch reports success for a write it did not perform;
- no age-based release or best-effort cleanup.

Use R34's general principle of explicit dispositions, but do not import its automation transaction types into this resource domain.

## 7. Close path

`close_owned_service_and_reconcile` must return a structured result to its caller:

```text
close requested / response
family departure evidence
departure reconciliation disposition
primary error
secondary diagnostic failure if any
```

Do not discard either error.

If graceful close fails or its response is lost:

- do not replay an unsafe stop blindly;
- observe exact retained process/family;
- settle same row from departure readback if it left;
- otherwise retain cleanup pending/attention;
- preserve the primary close/readback error if status persistence also fails.

The Store never kills an external/shared service; only the explicitly owned service route/process contract can authorize lifecycle actions.

## 8. Release integration

After `service_departed` commits, use the existing exact resource owners to converge:

```text
owned_service_starts row
→ binding state/release
→ capacity ledger exact scope
→ workspace lease lifecycle
→ launch/Attempt progress
```

R13's bidirectional roster and workspace predicate must treat a damaged/pending departure as a retained hold with an exact reason. It must not infer release from process absence in an eventually consistent list.

Do not add a second release path in this companion. Call the one live resource transition producer and delete dead/weaker alternatives named in the main R13 handoff.

## 9. Exact fixtures

### Artifact parity

- `legacy_owned_opencode_artifact_departure_reconciles`
- `rust_module_owned_opencode_artifact_departure_reconciles`
- `opening_and_departure_use_same_closed_artifact_classifier`
- `unknown_artifact_is_rejected_before_owned_service_admission`

### Error containment

- `damaged_candidate_is_reported_degraded_not_successful_idle`
- `sqlite_failure_aborts_pass_and_does_not_touch_cursor`
- `one_damaged_candidate_does_not_hide_healthy_departed_candidate`
- `permanent_damage_is_not_retried_every_tick_without_changed_evidence`
- `departure_touch_cas_miss_is_classified_as_concurrent_progress`

### Exact provenance

- changed current Manager does not erase historical cleanup authority;
- wrong launch/open/binding/lease tuple fails closed;
- wrong process birth/image cannot release;
- already closed row is idempotent and not counted as this pass's departure.

### Close and release

- `lost_close_response_uses_departure_readback_without_second_close`
- `close_failure_is_not_discarded_when_reconcile_also_fails`
- `service_departure_releases_binding_capacity_and_workspace_once`
- `still_live_or_unknown_service_retains_all_resource_holds`

Tests must execute the Store reconciliation entrypoint and inspect retained rows/resources, not only the classifier helper.

## 10. Simplification and deletion

After migration, delete:

- `owned_opencode_artifact` plus the contradictory departure literal, replacing both with one classifier;
- `.ok()` around `departure_source`;
- nested `if let Ok` chains that erase error classes;
- `close_owned_service_and_reconcile` return-value discard;
- timestamp touching used as a substitute for a disposition;
- duplicate/dead resource-release primitives after the one live transition is connected.

Keep exact historical provenance and bounded diagnostics.

## 11. Ownership and ordering

R13/#39 owns this resource/departure companion together with capacity/workspace release.

R01/#27 owns generic module process identity and family departure. R35/#61 owns neutral finite process completion. R31/#57 owns closed RuntimeCommand registry, not owned-service artifact lifecycle. R17/#43 changes OpenCode controls and must consume the same selected artifact classifier rather than defining another.

Shared launcher/resource files require one manager/worktree. Recommended order:

```text
1. selected owned-service artifact classifier
2. typed departure-source load
3. per-candidate dispositions/diagnostics
4. truthful close result
5. connect exact resource release
6. delete swallowed/duplicate paths
7. public Store fixtures
```

## 12. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Then exact process/resource fixtures above. Broad native/load qualification remains later.
