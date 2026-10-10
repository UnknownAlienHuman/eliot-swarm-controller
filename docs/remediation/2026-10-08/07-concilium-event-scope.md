# R07 companion. Concilium event proof must carry its exact Task/Attempt scope into ScriptRun

**Status:** implementation handoff. Production Concilium events, ScriptRun causes and automation cursors are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Confirmed HIGH defect

`automation_dispatch_bus::concilium_event_source_proof` performs a substantial immutable-source validation and returns:

```rust
ConciliumEventSourceProof {
    operation_id,
    task_id,
    task_revision,
    attempt_id,
}
```

It verifies:

- exact observed event and retained settled Concilium Operation;
- exact mutation receipt;
- originating `concilium.propose` Operation and link;
- Concilium ID and manager lineage;
- Task/Attempt/revision identity;
- Task project membership;
- optional expected project.

But its public sibling currently has the signature:

```rust
validate_retained_concilium_event_source(...) -> Result<()>
```

It discards the verified proof.

`automation_dispatch::validate_retained_script_event_cause` then:

```text
validates the Concilium source
→ records only retained_concilium_source = true
→ skips the ordinary Operation scope branch
→ keeps task_id/task_revision/attempt_id read from the retained cause
→ emits those cause-supplied values into ScriptRun input
```

For the Concilium branch, the cause scope is checked only for all-or-none shape. It is not compared with the scope that the bus validator just proved.

Therefore a retained cause can name another Task/Attempt/revision in the same project, pass source validation, and start a ScriptRun whose input claims the wrong work scope.

This is not a poison-fact availability problem. It is a lost authorization/identity proof between producer and consumer.

## 2. Required invariant

For every accepted Concilium-derived ScriptRun cause:

```text
cause observation and Operation
    == exact immutable Concilium mutation

ScriptRun Task/Attempt/revision scope
    == exact scope proven by that mutation and its originating proposal
```

The immutable proof may outlive current Task state, Attempt liveness, registration or GM designation. Historical readback must not reopen current authority. Current Manager/source rights remain checked at ScriptRun admission and start gate.

## 3. Minimal implementation

Return the proof instead of a boolean/`()`. Use one narrow private type owned by the Concilium event-source validator:

```rust
pub(super) struct ConciliumEventSourceProof {
    pub(super) operation_id: String,
    pub(super) task_id: String,
    pub(super) task_revision: i64,
    pub(super) attempt_id: String,
}

pub(super) fn validate_retained_concilium_event_source(
    db: &Connection,
    event: &ObservedEvent,
    project_id: &str,
) -> Result<ConciliumEventSourceProof>;
```

Then in `validate_retained_script_event_cause`:

1. keep `Option<ConciliumEventSourceProof>`, not `retained_concilium_source: bool`;
2. if the retained cause contains a Task scope, require exact equality with the proof;
3. if a historical cause omits the scope entirely, reconstruct the safe invocation input from the proof without rewriting the retained cause;
4. never use cause-supplied scope as authority;
5. require `event.operation_id == proof.operation_id` through the existing event identity path;
6. remove the empty `if retained_concilium_source { /* already checked */ }` branch;
7. skip the generic Operation scope query only after the typed Concilium proof has supplied the exact replacement scope.

A small helper is acceptable:

```rust
fn exact_or_historical_concilium_scope(
    cause_scope: Option<ScriptTaskScope>,
    proof: &ConciliumEventSourceProof,
) -> Result<ScriptTaskScope>;
```

Do not add a generic event authority framework. The same helper gets one real Concilium caller in this slice.

## 4. Producer-side seal

Find the current producer that writes/seals the `system_event` cause for Concilium mutations.

For new causes:

- copy Task/Attempt/revision only from `ConciliumEventSourceProof` or the same validated stored receipt;
- do not re-read them from caller input;
- preserve exact `operation_id`, observation ID, occurrence identity and project;
- include the normalized scope in the cause so ordinary readback is self-describing.

The consumer still revalidates retained proof. A correctly written cause is not trusted merely because the producer is currently unique.

Historical causes remain immutable. Scope omission may be reconstructed from exact proof; mismatched supplied scope is corruption, not silently overwritten.

## 5. Error and isolation semantics

Map an identity mismatch to the existing closed retained-data error:

```text
AUTOMATION_LINK_CORRUPT
retained Concilium event Task/Attempt differs from immutable source proof
```

R34/#60 decides how one corrupt cause is quarantined without aborting unrelated subjects. R07 must supply a truthful classified error; it must not catch and continue execution.

Database/read/commit uncertainty remains a hard error. Missing proposal/Attempt/Task source evidence remains unauthorized/corrupt according to the current validator; do not fabricate scope.

## 6. Exact public-path fixtures

Use the normal Concilium mutation → observation/intake → ScriptRun cause → retained readback/start path.

1. `concilium_script_cause_exact_scope_round_trips`
   - valid proposal and mutation;
   - exact Task/revision/Attempt appears in retained cause and ScriptRun input.

2. `concilium_script_cause_cannot_substitute_task_in_same_project`
   - event proves Task A;
   - retained cause names Task B in the same project;
   - no ScriptRun admission/effect.

3. `concilium_script_cause_cannot_substitute_attempt`
   - same Task/revision, another Attempt;
   - fail closed.

4. `concilium_script_cause_cannot_substitute_task_revision`
   - same Task/Attempt, wrong revision;
   - fail closed.

5. `historical_concilium_cause_without_scope_uses_immutable_proof`
   - old all-null scope shape;
   - safe input is reconstructed from validated proposal/mutation proof;
   - retained cause bytes are not rewritten.

6. `historical_concilium_scope_survives_task_close_and_gm_change`
   - immutable source remains readable;
   - current admission/start authority is checked separately;
   - no false requirement that historical manager is still current.

7. `concilium_source_proof_is_bound_to_exact_observation_and_operation`
   - reuse proof with another observation/operation fails.

8. `corrupt_concilium_cause_is_quarantined_without_running_script`
   - after R34 integration, unrelated due subjects still progress;
   - wrong-scope cause creates no ScriptRun effect.

Assertions must inspect the actual ScriptRun input/Operation, not only helper return values.

## 7. Simplification and deletion

After migration, delete:

- `validate_retained_concilium_event_source -> Result<()>`;
- `retained_concilium_source: bool`;
- the empty “validator already checked” branch;
- any second Task/Attempt extraction for Concilium ScriptRun causes;
- comments implying a discarded proof protects downstream scope.

Keep one immutable stored Concilium receipt/link chain and one consumer validator.

## 8. Ownership and ordering

R07/#33 owns:

- Concilium proposal/mutation receipt coherence;
- this Concilium event-source proof and its exact ScriptRun scope handoff;
- terminal Concilium lifecycle.

R34/#60 owns per-subject automation quarantine/transaction isolation in adjacent functions. Because both may touch `automation_dispatch.rs`, use one manager/worktree and rebase/serialize; do not create local duplicate classifiers.

R44/#67 owns the later ScriptRun allow/deny start decision. A correct start gate does not repair a wrong admitted source scope.

R06/#32 owns general coordination work context. This fix uses already retained Concilium Task/Attempt identity and does not wait for a new global context type.

Recommended order:

```text
1. return typed ConciliumEventSourceProof
2. seal new causes from proof
3. validate/reconstruct retained cause scope
4. connect R34 quarantine disposition
5. delete bool/empty branch and duplicate extraction
6. public-path fixtures
```

## 9. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Full test/native qualification remains later. The scoped fixtures above are the behavioral acceptance for this slice.
