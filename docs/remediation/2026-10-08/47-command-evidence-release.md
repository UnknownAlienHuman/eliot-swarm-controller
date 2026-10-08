# R47. Command evidence release: persist result first, then remove native capture through one recoverable release Operation

**Status:** implementation handoff. Production Command captures, Store artifacts and active routes are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

Large Command `stdout.ndjson` / `stderr.txt` / run receipts are reclaimed only after Store proves that every retained byte needed by the product has been persisted as an immutable artifact or was explicitly discarded by an authorized owner policy.

```text
Command run evidence retained
→ exact module.result page persisted and acknowledged
→ Store verifies artifact/digest/target Operation coverage
→ durable evidence-release Operation admitted
→ adapter writes release intent
→ target run directory renamed to releasing tombstone
→ large evidence removed
→ exact release outcome delivered
→ host acknowledgement
→ release tombstone removed durably
```

Outcome acknowledgement alone is insufficient: Command supports a later `agent.result`, and that path reads the original run directory. R47 therefore does not reuse the OpenCode/Claude terminal-journal rule from R46.

## 2. Confirmed current retention shape

Each Command Operation owns a private directory under `command-adapter-runs-v3/op-<digest>` containing some combination of:

```text
admission.json
dispatch-admission.json
run.json
stdout.ndjson
stderr.txt
outcome.json
ack.json
result-page.json
result-page-ack.json
```

`pending_outcomes` and `pending_result_pages` scan every retained operation directory. No directory-removal path exists.

`module.outcome` acknowledgement proves only that Store recorded the Operation outcome. It does **not** prove that native stdout/stderr is no longer needed: `agent.result` may arrive later and `result_page::build` reads exact retained target evidence.

After `module.result` acknowledgement, Store returns an immutable `artifact_ref`; at that point the exact page bytes/provenance are retained outside the Command run directory. That is the first safe automatic release anchor.

## 3. Do not delete on age, terminal state or binding stop

Forbidden release predicates:

- file age or mtime;
- Operation merely `settled`;
- outcome merely acknowledged;
- binding/session/process stopped;
- Attempt released without persisted result artifact;
- disk pressure alone;
- user interface disconnect;
- absence from an eventually consistent listing.

All can occur while native capture is still the only source for a later exact `agent.result`.

## 4. One closed internal release command

Add one closed RuntimeCommand kind through R31/#57's canonical registry. Keep it vendor-specific until a second adapter has the same evidence lifecycle:

```text
native.command.release_evidence
```

It is not exposed as a general public MCP mutation. Store creates it from a retained release Operation only after the artifact condition below passes.

Suggested typed request:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandEvidenceReleaseRequest {
    pub schema_version: u16,
    pub release_operation_id: String,
    pub target_operation_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub target_input_sha256: String,
    pub target_outcome_sha256: String,
    pub result_operation_id: String,
    pub result_page_sha256: String,
    pub artifact_ref: String,
    pub artifact_sha256: String,
    pub release_reason: CommandEvidenceReleaseReason,
}

pub enum CommandEvidenceReleaseReason {
    ArtifactPersisted,
}
```

Do not add `AgeExpired`, `BestEffort`, `DiskPressure` or arbitrary strings.

An explicit owner-discard reason may be designed later with its own authorization and irreversible-data-loss warning. It is not silently included in this first slice.

## 5. Store release gate

One Store-private constructor derives the release request from retained facts. It must require:

1. target Operation is the exact Command `task.dispatch`/supported result-producing command under the same binding generation;
2. immutable target admission/dispatch receipt and input digest are valid;
3. target outcome is terminal and exactly acknowledged by the adapter receipt chain;
4. exact `agent.result` Operation points to that target Operation/input;
5. result page Operation is settled/applied;
6. its `artifact_ref`, page digest, content digest, byte length and provenance match the target Command capture/result identity;
7. artifact exists in Store and exact bytes verify through the existing artifact reader;
8. no unresolved result-page Operation for the same target remains;
9. no other retained unresolved Operation names the target directory as its evidence source;
10. release semantic slot is vacant or identifies the exact same immutable request.

The constructor returns a typed request or a specific hold reason. It never treats missing/damaged evidence as releasable.

## 6. One durable release Operation

Allocate a normal caller-owned semantic release Operation before adapter mutation:

```text
method: native.command.release_evidence
completion_condition: exact_target_directory_released
replay_policy: readback_only
```

The semantic identity binds:

```text
target operation
result operation
artifact ref + digest
target outcome digest
binding generation
```

Changed artifact/result evidence conflicts. Lost adapter reply causes exact readback of the release tombstone; it never regenerates the result or reruns Command.

Do not fold release into `agent.result` settlement transaction: filesystem cleanup is an external effect and must retain its own intent/outcome/readback.

## 7. Adapter release state machine

### 7.1 Preflight

The adapter validates:

- current binding/generation and selected Command artifact;
- release Operation module receipt;
- exact target directory name from target Operation ID;
- admission and dispatch receipts;
- saved target outcome digest and exact acknowledgement;
- saved result page and exact result-page acknowledgement;
- request artifact/result fields equal the retained acknowledgement/provenance.

Any missing/mismatched record rejects before mutation.

### 7.2 Durable intent and atomic retirement

Inside the run-store root:

1. write a private `release-intent.json` in the target directory containing the exact request digest;
2. sync file;
3. atomically rename:

   ```text
   op-<target> → releasing-<target>-<release-op-digest>
   ```

4. sync run-store parent directory;
5. after rename, new result-page reads for the target fail with explicit `EVIDENCE_RELEASED`, not generic NOT_FOUND;
6. delete large capture/evidence files from the renamed directory;
7. keep only bounded release intent/outcome plus the minimum exact digests needed for readback;
8. sync directory state;
9. deliver release outcome.

The rename is the irreversible ownership transition. Before rename, target evidence is fully available. After rename, the release Operation owns cleanup and exact readback.

Use R41 durable file/rename/remove primitives. Do not implement another platform publication helper.

### 7.3 Readback and completion

On retry/restart:

- original target directory present, no matching releasing dir → effect not started;
- exact matching releasing dir present → resume cleanup or return retained release outcome;
- neither present but acknowledged release record remains → released;
- conflicting releasing directory/intent → damaged, no deletion;
- both original and releasing directories present → damaged/ambiguous, no automatic choice.

After Store acknowledges the release outcome, delete the bounded releasing tombstone durably. If that final deletion fails, report retention-degraded but never restore/recreate native evidence.

## 8. Result/read behavior after release

Store must not issue new `agent.result` for a target whose evidence-release Operation is applied.

Public/read projections return exact retained artifact references already committed before release. They do not ask the adapter to reconstruct deleted stdout/stderr.

If a result request races with release:

- release gate rechecks no unresolved/new result Operation immediately before command admission;
- adapter serializes result read and release under the single run-store owner;
- one wins; the loser receives a typed conflict/pending readback;
- no partially deleted output is returned.

## 9. Bounded cleanup without recursive retention

The release Operation itself uses a small release directory/tombstone only until its outcome is acknowledged. It contains no stdout/stderr or copied result page body.

After acknowledgement, durable removal leaves no per-run adapter state. Historical proof remains in Store:

- target Operation/outcome;
- result Operation;
- artifact record/bytes;
- release Operation/outcome.

Thus cleanup does not replace one forever-retained large directory with another forever-retained small directory.

## 10. Accounting and attention

Project bounded aggregates:

```text
command_run_directories
command_capture_bytes
release_eligible_count
release_pending_count
release_degraded_count
oldest_unreleased_terminal_at_ms?
```

No filenames, prompts or raw output in routine projection.

A terminal acknowledged target with no persisted artifact is not auto-deleted; surface `result_artifact_not_persisted` so the owner understands why bytes remain.

## 11. Simplification/deletion

After migration remove:

- any future age/LRU cleanup prototype for Command runs;
- scans of acknowledged/released target directories after tombstone cleanup converges;
- duplicate result-read paths that bypass exact target admission/provenance;
- stale run directories whose applied release Operation and Store artifact have been verified by a one-time bounded migration;
- adapter-local filesystem helpers replaced by R41.

Do not delete current historical directories in bulk without matching Store release evidence.

## 12. Donor mechanisms

- Emdash: durable tombstone, host effect, readback, then tombstone removal;
- Bazel REAPI/CAS: immutable artifact digest is the retained product result, not the worker's temporary execution directory;
- Petri: one outstanding release command and deterministic readback;
- current ELIOT Operation/artifact path: remains authority.

No donor runtime or second artifact store is imported.

## 13. Exact fixtures

Store gate:

- `command_outcome_ack_without_result_artifact_is_not_releasable`
- `command_exact_result_artifact_makes_target_releasable`
- `command_changed_artifact_digest_conflicts_release`
- `command_unresolved_result_operation_holds_release`
- `command_release_operation_is_idempotent_by_exact_semantic_identity`

Adapter:

- `command_release_rename_precedes_capture_deletion`
- `command_release_crash_after_rename_resumes_without_result_replay`
- `command_release_lost_reply_reads_same_tombstone`
- `command_conflicting_releasing_directory_is_never_deleted`
- `command_result_vs_release_race_has_one_winner`
- `command_release_ack_removes_bounded_tombstone`
- `command_release_never_reruns_native_command`

End-to-end:

- `command_result_bytes_remain_readable_from_store_after_adapter_release`
- `command_released_target_rejects_new_native_result_read`
- `command_release_reclaims_stdout_stderr_bytes`

Use real run directories and public Store→RuntimeCommand→adapter→Store paths.

## 14. Ownership and order

- R31/#57: closed RuntimeCommand registry.
- R35/#61: finite Command process/capture truthfulness where shared.
- R41/#65: durable filesystem operations.
- R47: exact Store release gate and Command run-directory state machine.
- R46/#69: OpenCode/Claude metadata journal retention; no Command deletion.

Recommended order:

```text
R31 command registry
→ R41 durable rename/remove
→ Store release gate + one release Operation
→ adapter release/readback
→ projections and bounded migration
```

One manager owns Command adapter journal/adapter and Store release handler. Other writers rebase; no parallel cleanup path.

## 15. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-contracts -p swarm-process -p swarm-adapter-command -p swarm-kernel-host --lib --bins -- -D warnings
```

Then the exact file/race/public-path fixtures above. A paid Command model run is not required to test release against recorded fixture evidence.
