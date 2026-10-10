# R45. Automatic effect slots: one Operation lifecycle parser, truthful terminal disposition

**Status:** implementation handoff. Production Operation states, automatic consumers and Store schema are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

Acceptance, Forge publication and managed GitHub projection stop interpreting the same retained Operation state in three incompatible ways.

```text
exact semantic request identity
→ load one retained Operation through one typed lifecycle parser
→ validate method/request/scope/link
→ validate state-specific result/effect evidence
→ domain policy returns truthful disposition
→ no duplicate external effect
```

A terminal Operation is never reported merely as “coalesced success”. `cancelled`, `rejected` and `settled` remain distinct and require exact domain evidence. This slice does not create a global retry engine or automatically repeat a cancelled/rejected external effect.

## 2. Confirmed divergence

### 2.1 Automatic acceptance treats `cancelled` as corruption

`automation_acceptance::reserve_automatic_acceptance` accepts an existing semantic Operation only in:

```text
queued | sending | outcome_unknown | settled | rejected
```

A correctly retained `cancelled` Operation returns `AUTOMATION_OPERATION_CORRUPT`.

The same function then reconstructs one success-shaped receipt from either the old admission receipt or current result, overwrites `coalesced=true`, and carries only a boolean `task_accepted`. It does not establish a closed distinction between:

- admission still pending;
- external/Store effect may have started;
- acceptance applied;
- acceptance rejected;
- acceptance cancelled before effect;
- terminal result damaged.

### 2.2 Publication and GitHub projection accept more states but flatten them

`automation_publication::reserve_automatic_publication` and `automation_github_projection::reserve_projection` both accept:

```text
queued | sending | outcome_unknown | settled | rejected | cancelled
```

They return:

```text
status = coalesced
coalesced = true
publication/effect_started = state in queued|sending|outcome_unknown
receipt = whatever result_json retained
```

Thus `rejected` and `cancelled` are not corruption, but are still projected through a generic “coalesced” path. A caller can no longer tell that no publication/label effect completed, and the source cursor may advance as though the semantic slot were satisfied.

### 2.3 Repair semantic slots expose state but do not classify it at the resolver

`automation_repair::resolve_slot_parts` validates slot/request/scope identity and returns:

```text
Existing { operation_id, operation_state }
Conflict { operation_id, operation_state }
```

for any retained state. Some callers subsequently inspect delivery evidence, while others can reason from a success-shaped existing slot. The common resolver does not establish what the state proves.

R45 does not migrate RepairDispatch in the first code slice, but it creates the narrow typed lifecycle parser that R21/#47 must consume and explicitly forbids state-only reuse.

### 2.4 Cancellation already has a real state transition

`operations::cancel` changes only an exact queued target to `cancelled` and stores a cancellation receipt. For `sending | native_accepted | outcome_unknown`, it returns `OUTCOME_UNKNOWN` rather than inventing cancellation.

This is useful evidence:

```text
cancelled queued target = no native/effect dispatch through that target
outcome_unknown         = cancellation not proven; readback required
```

The automatic consumers currently fail to preserve that distinction consistently.

## 3. One private Operation lifecycle parser

Add one closed Store-private enum beside the authoritative Operation row loader:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationLifecycleState {
    Queued,
    Sending,
    NativeAccepted,
    OutcomeUnknown,
    Settled,
    Rejected,
    Cancelled,
}
```

One parser:

```rust
fn operation_lifecycle_state(value: &str) -> Result<OperationLifecycleState>;
```

Unknown strings are retained-data corruption, not a new implicit state.

This enum says only where the generic Operation state machine is. It does **not** say whether a domain effect applied, can retry or satisfies a semantic slot.

Delete local string allowlists as each connected consumer migrates.

## 4. One exact existing-Operation loader for the three sibling consumers

Add a private typed row in the narrow automation/Store owner, for example:

```rust
struct ExistingAutomaticOperation {
    operation_id: String,
    method: String,
    caller_id: String,
    client_request_id: String,
    original_request: Value,
    effective_request: Value,
    state: OperationLifecycleState,
    result: Option<Value>,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    sent_at_ms: Option<i64>,
    settled_at_ms: Option<i64>,
}
```

The loader must:

- parse JSON once;
- distinguish absent result from malformed result;
- validate timestamp/state coherence;
- retain exact caller/request identity;
- not authorize or classify the domain itself.

Do not move this DTO into public contracts. It is an internal persisted-row decoder.

## 5. State is one axis; domain outcome is another

Each migrated domain implements a small exhaustive classifier over:

```text
OperationLifecycleState
+ exact method
+ exact original request
+ exact on-behalf link/context
+ state-specific result/receipt
```

Common output shape may be private and small:

```rust
enum ExistingEffectDisposition {
    InFlight { operation_id: String },
    ReadbackRequired { operation_id: String },
    Applied { operation_id: String, receipt: Value },
    CancelledBeforeEffect { operation_id: String, receipt: Value },
    RejectedBeforeEffect { operation_id: String, code: String, receipt: Value },
    TerminalNotApplied { operation_id: String, code: String, receipt: Value },
}
```

Do not add a generic `Retry` variant. Whether a new semantic identity may be created is domain policy and requires a new source/config revision or explicit owner action.

Any state/result combination outside an exhaustive domain mapping is corruption/hard error.

## 6. Generic state evidence rules

These rules are shared and mechanical:

### Queued

- no `sent_at_ms`;
- no terminal timestamp;
- may retain an admission receipt;
- effect has not been dispatched by the generic Store runtime.

Disposition: `InFlight`, unless the domain proves a malformed admission.

### Sending / NativeAccepted

- effect may have begun;
- never create a replacement;
- read current Operation/native evidence.

Disposition: `InFlight`.

### OutcomeUnknown

- effect may have happened;
- no replay and no replacement;
- exact domain readback required.

Disposition: `ReadbackRequired`.

### Cancelled

- must have a valid cancellation receipt and terminal timestamp;
- for the current `operation.cancel` path, queued→cancelled proves that target was not sent through the generic dispatcher;
- no automatic retry under the same semantic request ID.

Disposition: `CancelledBeforeEffect` only when the exact state transition/evidence proves it. Otherwise corruption.

### Rejected

- must retain a structured reason/code and terminal timestamp;
- classifier must distinguish rejection before effect/admission from a domain failure after an attempted effect;
- arbitrary error text is not a no-effect proof.

Disposition: `RejectedBeforeEffect` or `TerminalNotApplied`, chosen by the domain's structured receipt.

### Settled

- generic terminal state alone proves nothing about applied success;
- method-specific result must validate exact operation ID, outcome, completion condition and domain identity;
- a failed/stale/superseded settled result is not `Applied`.

Disposition: `Applied` or `TerminalNotApplied`.

## 7. Acceptance classifier

For `task.accept` require:

- exact automation technical caller and semantic request ID;
- original request byte-equivalent to the current expected request;
- exact Task/Attempt columns;
- exact `automation_on_behalf` link;
- exact acceptance Operation/decision relation;
- current result identity tied to the same operation.

Map:

| State/evidence | Result |
|---|---|
| queued/sending | `acceptance_in_flight` |
| outcome_unknown | `acceptance_readback_required` |
| settled + `task_accepted:true` and exact accepted Task state | `acceptance_applied` |
| settled + explicit stale/superseded/non-applied outcome | `acceptance_not_applied` with code |
| rejected + structured pre-effect rejection | `acceptance_rejected` |
| cancelled + exact queued cancellation receipt | `acceptance_cancelled` |

Do not turn cancelled/rejected into `AUTOMATION_OPERATION_CORRUPT`. Do not report them as applied or create a new acceptance automatically.

The result returned to `consume_review_result_for_entry` must be truthful enough for R34 disposition:

- applied → `Applied`;
- in flight/readback → `Pending`;
- cancelled/rejected/non-applied → explicit domain `Skipped` or `Quarantined` only through an exhaustive error/code policy, never generic “coalesced”.

## 8. Forge publication classifier

For `forge.publish_ref` validate existing `PublicationContext`, exact target/ref/candidate/GM epoch and immutable request.

Map:

| State/evidence | Result |
|---|---|
| queued/sending | `publication_in_flight` |
| outcome_unknown | `publication_readback_required` |
| settled + exact remote readback/applied publication receipt | `publication_applied` |
| settled + stale epoch/superseded/no-write structured result | `publication_not_applied` |
| rejected | `publication_rejected` |
| cancelled | `publication_cancelled` |

`publication_started` is true only for an actual attempted/in-flight/unknown effect, not for every queued Operation and not for a terminal rejection/cancellation.

A terminal not-applied publication does not silently create another push. A later explicit owner/config/source revision may generate a new semantic identity under the publication domain's policy.

## 9. Managed GitHub projection classifier

For the managed-label Operation require the exact GitHub projection link and target.

Use the same lifecycle parser but a GitHub-specific result validator:

- `effect_applied` only with exact target label/revision/readback;
- queued/sending/unknown remain unresolved;
- rejected/cancelled are explicit terminal not-applied states;
- settled stale/no-write does not satisfy the projection.

The acceptance event cursor may advance past a deliberate terminal skip only when the domain writes an explicit disposition explaining why this projection will not be attempted again under the current automation revision. Otherwise retain pending/attention. Do not equate terminal Operation with satisfied projection.

## 10. Cancellation and semantic-slot closure

When a domain semantic slot points to a cancelled/rejected Operation:

- retain the slot and terminal receipt as history;
- do not overwrite the slot with another Operation under the same semantic identity;
- do not leave a sibling admission/alias Operation queued solely because its target was cancelled;
- if the domain has a paired local admission wrapper, settle/cancel that exact wrapper in the same transaction or retain an explicit pending closure reconciler;
- never cancel an already sent/unknown external effect merely by changing the sibling row.

Inventory exact paired writers while implementing the three migrated consumers. Do not add a generic sibling graph.

## 11. Repair, Goal and WorkDispatch consumers

R45 exports only the private lifecycle parser/typed loader where crate visibility requires it. Existing domain owners consume it in their own implementation PRs:

- R21/#47 Repair: `Existing` cannot mean reusable without delivery-state/readback classification; remove state-only success paths.
- R43/#66 Goal: terminal event/slot classification remains exact to Goal admission/evidence; use typed Operation state instead of string sets.
- R12/#38 + R34/#60 WorkDispatch: cancelled/rejected launch slots must not be silently reused or retried; pending/unknown remains held with pacing.
- R27/#53 and R28/#54 Task semantic reuse: reuse no-effect receipts separately from the one original native effect.

They do not need another lifecycle enum.

## 12. Simplification/deletion

After the three sibling consumers migrate, delete:

- their independent state string allowlists;
- `coalesced:true` as the only terminal semantic signal;
- boolean `*_started` projections derived only from generic state;
- branches treating cancelled as corruption in one domain and success-shaped coalescing in another;
- result fallback that picks any object without validating method-specific identity;
- duplicate state parsers introduced by downstream PRs.

Do not delete immutable historical Operation rows or receipts.

## 13. Donor use

No external dependency is needed.

Useful mechanisms:

- Restate: terminal vs retryable is explicit, not inferred from error text;
- Petri: state transition and command/effect evidence are separate axes;
- Kubernetes controller: reconciliation reads current truth and forgets backoff only after progress.

Do not import a workflow engine, generic saga framework or infinite retry loop.

## 14. Exact fixtures

Common/parser:

- `operation_state_unknown_value_is_corrupt`
- `operation_cancelled_requires_exact_terminal_receipt`
- `operation_settled_without_domain_result_is_not_applied`
- `operation_outcome_unknown_never_authorizes_replacement`

Acceptance:

- `cancelled_automatic_acceptance_is_truthful_not_corrupt`
- `rejected_acceptance_is_not_task_accepted`
- `settled_nonapplied_acceptance_does_not_satisfy_slot`
- `acceptance_unknown_stays_pending_without_duplicate_operation`

Publication:

- `cancelled_publication_is_not_coalesced_success`
- `rejected_publication_is_not_replayed`
- `settled_stale_epoch_publication_is_not_applied`
- `publication_unknown_requires_exact_remote_readback`

GitHub projection:

- `cancelled_label_effect_does_not_claim_projection_applied`
- `rejected_label_effect_retains_explicit_terminal_disposition`
- `settled_no_write_does_not_advance_as_success`
- `projection_pending_does_not_hot_loop_or_advance_cursor`

Cross-domain:

- `same_operation_state_maps_differently_only_by_validated_domain_result`
- `paired_local_wrapper_does_not_remain_queued_after_target_cancelled`

Use public Store/reconcile paths and inspect exact Operation/Observation/cursor rows; helper-only enum tests are insufficient.

## 15. Ownership and implementation order

- R45 owns `OperationLifecycleState`, the exact existing-row loader and migration of acceptance/publication/GitHub projection.
- R34/#60 owns subject disposition/quarantine and per-domain transactions.
- R21/R43/R12/R27/R28 consume the lifecycle parser in their domain paths.
- R30/R23 own authority/read scope, not effect outcome classification.

Recommended connected commits in one PR:

```text
1. typed lifecycle parser + loader with current callers
2. acceptance classifier and truthful consumer disposition
3. publication classifier
4. GitHub projection classifier
5. delete three old state/result fallbacks
6. paired local-wrapper closure fixtures
```

One manager owns `operations.rs` and the three automation files. Other PRs rebase; no parallel lifecycle enum.

## 16. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Then the exact Store/reconcile fixtures above. Broad GitHub/native qualification remains the final project phase.
