# Native candidate O execution results

## Outcome

Edition O produced one native OpenCode run and no Antigravity run. The OpenCode run passed every
stage through `module_open_and_hello` and then had its single `task.dispatch` operation rejected
deterministically before admission. Final status is `PARTIAL_PROGRESS`.

Two things are recorded separately and neither replaces the other. What the harness wrote into the
live receipt is `single_task_dispatch` status `unknown`, code `STALE_LAUNCH_DISPATCH_SCOPE`, and
`dispatch_operation_id` null. What the Store actually contains, verified post-receipt by reading the
run Store directly, is operation `49fef96d-c152-4f16-9f5a-e6119be3ca6a` in state `rejected` with the
same code. `dispatch_outcome_unknown` is a historical harness misclassification of that rejection and
is not the verdict of this record. The live receipt file was not modified and still hashes to
`11c1d4acbe7d92618bc46eb3bf8db5a6d0ffedf0027f8d4bf0d7d2ddf5105149`.

No task dispatch was ever admitted, so no model consumption and no task completion is claimed for
either adapter.

## Candidate source

| Field | Value |
| --- | --- |
| Commit | `1ad420977f994150dc12ea73709201b46af7f83b` |
| Tree | `8e6f9402a34d50cf8e577508e006680a8b744ca5` |
| Clean | yes |
| Origin | `https://github.com/UnknownAlienHuman/eliot-swarm-controller.git` |
| Supervisor source sha256 | `8a541c0190e20187f569d43e305b0633e9d7be128e53e7b0020b424dda46602f` |

The build-time clean source is recorded in `o-clean-candidate-source-readback.json`.
The parent subsequently restored the historical checkout to this exact commit after
archiving an unaccepted harness patch outside the checkout. The publication-time
tracked source is clean; `o-source-frozen-harness-restoration.json` records that
single-file restoration. The eight built packages and original run receipts are unchanged.

## Provenance

| Field | Value |
| --- | --- |
| Packages | 8 |
| Release build | PASS |
| Installed file hashes verified | 16 |
| Independent kilo/step5 high provenance review | PENDING |

Every package manifest and binary was re-hashed against the on-disk files while producing this record,
and all 8 pairs matched, each with the declared byte count. The 16 declared installed host-stack file
hashes were re-verified the same way, with 0 mismatches. The independent review field is left
`PENDING` because no such review artifact for edition O exists; it is not carried over from N.

The packages are `host_runtime`, `host_supervisor`, `host_launcher`, `public_cli`,
`process_owner_helper`, `opencode_module`, `antigravity_module` and `swarm_mcp`, all built at
release profile with toolchain `1.98.1`.

### Provenance basis

Each manifest sha256, binary sha256 and byte count was re-hashed live against the on-disk files while
producing this record: 8 manifest and binary pairs, all matched. Commit, tree, profile, toolchain
channel, lock hash, cargo toml hash, toolchain file hash and target dir hash come from the O run
receipt's own preflight build facts, which is the O harness recording its own candidate. Nothing from
N is reused, and the record says so explicitly in `package_provenance_basis`. The per-package
`clean_before` and `clean_after` flags are null instead of asserted, because the O flow did not record
those gates per package; the source state is recorded once in `candidate_source`.

## Requested models

| Adapter | Requested model |
| --- | --- |
| OpenCode | `opencode/step-5-preview-free`, variant high |
| Antigravity | `gemini-3.8-flash-high` |

Model execution is not established by either request.

## OpenCode run d44e1238-9599-4c30-a287-1cd20167ea96

Passed stages: `preflight`, `isolated_host`, `manager_admission`, `trusted_descriptor`,
`route_configuration`, `manager_route_selection`, `task_create`, `task_claim`, `launcher_preview`,
`module_open_and_hello`. The module supervisor reached phase `ready` and the store accepted the
handshake as a ready observation.

The single failing stage is `single_task_dispatch`. The harness reported it as `unknown` with code
`STALE_LAUNCH_DISPATCH_SCOPE` and `dispatch_operation_id` null. The Store row for the very same
dispatch tells a definite story:

| Field | Value |
| --- | --- |
| Operation id | `49fef96d-c152-4f16-9f5a-e6119be3ca6a` |
| Method | `task.dispatch` |
| State | `rejected` |
| Error code | `STALE_LAUNCH_DISPATCH_SCOPE` |
| Error message | launch parent, Task, Attempt, or ready binding is no longer exact |
| Client request id | `native-qualification-80e1d251d3cf42fe826be7f6f4632973` |
| Observation | `operation.rejected`, `operation_rejected`, `OPERATION_REJECTED`, at ms 1791674548100 |

This is a deterministic pre-admission rejection, persisted before any module-side intent existed.
The caller was the run manager `native-qualification-5dadb602f15c446792166796508fdf37`, and the
client_request_id `native-qualification-80e1d251d3cf42fe826be7f6f4632973` is unique to this dispatch,
so the row is attributable without inference.
The attempt `5e7a8a4f-1f4f-410e-81a8-c8d9425d6db9` has `start_operation_id` null with progress
`task_dispatch` `not_started`, and the adapter owner journal directory holds 5 journals, all
`native.mcp.arm`, `native.mcp.install`, `native.mcp.observe`, `native.mcp.read` and `agent.open`.
No native `task.dispatch` journal exists.

## Binding state and the guard

Binding `314cb683-fad4-431d-8244-08131ca1079b` generation 1 is state `reconciling` with
`connection` `disconnected`, `route_alias` `native-prereq-opencode`, and
`native_root_id` `ses_swarm_c4efab4dcfb81fae7647c256e4d89d6820016735bee5684bedae28bd4a367174`.

Reading the predicate list honestly requires separating what the post-run readback shows from what the
rejection actually evaluated. The post-run scope readback shows nine of the ten visible predicates
matched: launch parent queued, manifest `awaiting_native_mcp` with `dispatch_permitted` false, task,
attempt and binding identity triples exact, `task_dispatch` `not_started` with `start_operation_id`
null, Task open at revision 1, and Attempt reserved and controller-owned and unreleased. The one
mismatch it shows is `binding['state'] != 'ready'`. That is an observed post-run mismatch, not proof
of the failed predicate.

The rejection itself did not retain its transaction-time predicate snapshot, and
`STALE_LAUNCH_DISPATCH_SCOPE` is emitted for a disjunction of parent, manifest, Task, Attempt and
binding conditions. The known host EOF occurred after the rejection. The timing of
any earlier module-link close is unverified, and a later disconnect could have
produced the observed mismatch.
The predicate that failed inside the rejecting transaction is therefore recorded as `unverified`.
The separate review `.local/manager-audit/o-module-link-cause-review.json` reaches the same
conclusion: high confidence in the post-run mismatch, medium confidence that the same mismatch
existed inside the transaction, and low confidence in the event that caused it.

What is settled is the guard itself. `validate_parent_tuple` is unchanged and correct as written: it
refused a dispatch whose live scope was not exact and complete. The strict guard is retained. No guard
relaxation, retry, or synthesized ready state is proposed.

The harness's reporting of the rejection as `outcome_unknown` is a diagnostic loss rather than a
Store defect, and it stays out of the verdict: the harness derives the operation id only from a
successful CLI stdout parse, and its fallback filter requires the `task_id` and `attempt_id` columns
that a pre-admission rejected row never carries. The historical classification lacked a projected
request id. The current repair in main adds that key to the existing Store projection
only for the producing caller, and uses it for exact harness readback. The dispatch
guard and read grants remain unchanged. The Store rejection is terminal and definite, so the
verified outcome of this run is `dispatch_rejected_before_admission`, with
`harness_reported_status` `dispatch_outcome_unknown` retained beside it as the receipt verbatim.

## Why the binding left ready, left open

The retained signature is `connection=disconnected` with no `native_transport_error`,
`latest_native_failure`, or `recovery_required` key. Two source paths write exactly that key set:
the host startup recovery sweep at `store/mod.rs:3196-3242` and `runtime::disconnected` at
`store/runtime.rs:3439-3481`. The run retains exactly one host lifecycle record with
`host_epoch` 1 and final state `failed`, so no second epoch exists that would prove a restart.
The cause is therefore recorded as `UNDETERMINED` and no restart cause is claimed here.

This section also does not settle the earlier question of whether the binding was already
non-ready when the dispatch was rejected. The rejection kept no predicate snapshot, and both the
known host EOF at ms 1791674732349 postdates the rejection observation at
ms 1791674548100. No retained timestamp establishes a module-link close before
that rejection, so the observed mismatch cannot be pinned to the transaction.
The owned-service departure issue observation recorded at ms 1791674548151, about 52 ms after the
rejection, carries disposition `retained_uncertain` with `OWNED_SERVICE_DEPARTURE_NOT_PROVED` and does
not update binding state, so it is not evidence of a departure. A module-link close before the
rejection is a plausible cause, not a proven one.

## Host shutdown

Kernel process 53700 exited after a graceful EOF was sent, with `force_terminated` false.
The lifecycle record shows `host_epoch` 1, state `failed`, child exit category `exited` and
child exit code 0.

## Antigravity run 5800ed83-b94b-4fbc-81b5-943cd3977804

NOT STARTED. The run id is preselected and bound in the O preparation manifest and in the
`native-configs-d44e1238...-5800ed83...` host-config directory, but the run root contains only the
OpenCode run. There is no Antigravity receipt, state root, Store, or observation. Its module is
installed at `module-b` with executable sha256 `fc3c184d3a9bbcfcb58d459854e213e7ea8ba4b5c9852307242dda47750f3c9a`
and six declared capabilities, none of them an MCP capability.

## What remains open

- The predicate that failed inside the rejecting transaction is unverified; the retained mismatch is a post-run observation only.
- The binding lifecycle cause needs a source-level trace that the retained run cannot settle.
- An independent provenance review for edition O has not been done.
- R48 optional-worker supervision and the Antigravity assigned-session MCP schema gap are unchanged.
- 13 upstream dependency alerts remain open.
- No task dispatch was admitted on either adapter, so causal native results are not established.

## Record basis

This record was produced read-only. No native, model, build, or process command was run, no receipt
or worktree was edited, and no hash source artifact was modified. All package and installed hashes
were re-verified by hashing the files as they currently sit on disk.
