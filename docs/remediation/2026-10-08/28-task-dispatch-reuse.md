# R28. `task.dispatch` reuse: своя settled receipt, один effect owner

**Статус:** implementation handoff. Текущий diff содержит только это задание; Store/runtime-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Результат

У одной controller-started Attempt остаётся ровно одна начальная effect Operation — `attempts.start_operation_id`. Новый `task.dispatch` с другим `client_request_id`, но тем же exact Attempt/input/prerequisite/launch parent, не создаёт второй native input. Он получает **собственную settled Operation receipt**, которая:

- имеет собственный `operation_id`;
- связана колонками с exact Task/Attempt;
- явно ссылается на прежнюю start Operation;
- не получает binding columns и не создаёт второй capacity reservation;
- не копирует Task prompt/snapshot;
- не попадает к native dispatcher;
- не выдаёт mutable state старой Operation за состояние новой.

```text
first request
  -> Operation A queued
  -> Attempt.start_operation_id = A
  -> единственный native effect owner

second exact semantic request, new client_request_id
  -> Operation B settled
  -> B.task_id / B.attempt_id = exact Attempt
  -> B.semantic_reuse_of_operation_id = A
  -> no native command / no capacity entry

same client_request_id replay
  -> original immutable receipt
  -> no new Operation B
```

Изменённый text/prerequisite/launch parent конфликтует. Released Attempt и cancelled start slot не получают success-shaped coalesced receipt.

## 2. Нормативная документация

[Implementation v6, §5–6](../../agent_swarm.implementation-v6.md) задаёт точный смысл:

- `start_operation_id` — единственная начальная доставка controller-start Attempt;
- второй caller получает сохранённый start handle;
- его собственная request receipt — settled/coalesced;
- эта receipt никогда не поступает native dispatcher;
- изменённый target/payload при занятом slot — conflict;
- start slot не сбрасывается;
- после cancellation/смены исполнения нужен обычный release/new Attempt;
- доказанно непринятый input разрешается через ту же effect Operation/readback, не второй dispatch.

Следствие: применять к coalesced receipt все admission gates нового эффекта неправильно. `execution_mode.new_work`, current Task revision/state и ready binding защищают **новый initial effect**, а не историческую request receipt о уже занятом slot.

## 3. Текущая цепочка

### 3.1 Generic mutation wrapper

`store/mod.rs::mutate_in_transaction_with_authority`:

```text
validate request
→ exact client_request_id replay
→ INSERT new Operation { state=queued, scope columns=NULL }
→ SAVEPOINT mutation_effect
→ operations::dispatch
→ queued=false: settle the new Operation
→ insert observation keyed by new Operation ID
```

Exact replay того же `client_request_id` уже корректно возвращает retained receipt до создания новой строки. R28 касается другого request ID, который семантически попадает в занятый start slot.

### 3.2 Task dispatch

`store/operations.rs::dispatch`:

```text
load Attempt
→ require current Attempt control
→ require start_owner=controller
→ if Attempt.start_operation_id exists:
     load prior method/original request
     launcher_dispatch::validate_coalesced_dispatch
     compare text/prerequisite/launch parent
     return {operation_id: PRIOR, attempt_id, coalesced:true}, queued=false
→ otherwise run real admission and create first effect owner
```

### 3.3 Launch-specific validator

`store/launcher_dispatch.rs::validate_coalesced_dispatch`:

- validates launch ancestry and immutable packet when a retained launch parent exists;
- for legacy direct dispatch checks only absence of launch-only evidence;
- does not return a typed retained start record;
- does not validate the generic direct-dispatch Operation tuple/state;
- does not reject a released Attempt.

`validate_retained_link` is much stronger for launch-linked dispatch: it checks method/state, Task/Attempt/binding columns, packet and private link. That logic should remain launch-specific, not become the only validator of the generic start slot.

## 4. Подтверждённые дефекты

### D1. Result identity принадлежит старой Operation, observation — новой

On coalesced success `operations::dispatch` returns:

```json
{
  "operation_id": "<prior start Operation>",
  "attempt_id": "...",
  "coalesced": true
}
```

Generic wrapper then settles **the current newly inserted Operation** and inserts an observation whose relational `operation_id` is current, while payload `operation_id` is prior.

Consequences:

- caller cannot identify/read their own request receipt;
- `operation.get(returned operation_id)` opens the effect owner, not this call;
- generic invariant `result.operation_id == containing Operation.operation_id` is broken;
- observation identity and payload identity disagree;
- consumers that trust payload and consumers that trust row can attribute the same fact differently.

### D2. Current settled Operation remains objectless

The coalesced branch does not update current Operation columns. The row remains:

```text
task_id = NULL
attempt_id = NULL
binding_id = NULL
binding_generation = NULL
effective_request_json = { receipt only }
```

This becomes a correctness break once R23/#49 removes default-open Operation visibility: the receipt has no positive Task/Attempt relation from which the reader can derive authority.

### D3. Direct/legacy path does not validate the retained start tuple

The launch-linked path eventually calls `validate_retained_link`; direct dispatch does not verify that the prior Operation row still names the exact:

```text
Task
Attempt
binding generation
prerequisite Operation
allowed lifecycle state
```

`Attempt.start_operation_id` is authoritative but not sufficient when the pointed row is damaged or mislinked.

### D4. Released Attempt still receives a coalesced success

The occupied-slot branch executes before the first-dispatch guard:

```text
Attempt.state == reserved
Attempt.released_at_ms == null
```

Some bypass is intentional for a no-effect receipt, but `released_at_ms` is not. After release, callers have `operation.get` for history; they must not create a fresh success-shaped `task.dispatch` receipt on a terminal Attempt.

### D5. Cancelled state differs by ancestry and surfaces as corruption

For launch-linked dispatch `validate_retained_link` excludes `cancelled`, producing launch-link corruption. For direct legacy dispatch no equivalent state check exists. One product condition therefore depends on whether a launch parent existed.

Cancelled start is not corrupt ancestry. It is a terminal start slot that requires release/new Attempt. Return one typed conflict before any native effect.

### D6. Existing regression test certifies only “one queued effect”

`store/gm_continuation_tests.rs::successor_gm_continues_exact_attempt_without_restarting_dispatch` asserts:

```text
coalesced.operation_id == prior start_operation_id
queued task.dispatch count == 1
```

It never inspects the second settled Operation. Therefore it passes while that row is unscoped and its result identity contradicts its own row/observation.

## 5. Audit correction: what is **not** a defect

The original suspicion also listed bypasses of:

- `execution_mode.new_work`;
- current Task state/revision;
- ready binding.

Do **not** add these checks blindly to the coalesced path.

A semantic reuse receipt performs no native write. It must still work after:

- `new_work` was disabled after the first admission;
- the binding entered reconciling after the first input;
- Task revision advanced while the old Attempt remains retained;
- the first start Operation reached sending/native_accepted/outcome_unknown/settled/rejected.

The receipt reports the already occupied slot; it does not re-authorize or replay that effect. Current caller authority and exact retained identity are still required.

## 6. Минимальный внутренний тип

Do not add a public DTO. Add one private typed record near `operations::dispatch`, for example:

```rust
struct ExistingTaskDispatch {
    operation_id: String,
    state: String,
    task_id: String,
    attempt_id: String,
    binding_id: String,
    binding_generation: i64,
    prerequisite_operation_id: Option<String>,
    original: Value,
    effective: Value,
}
```

A loader reads one exact row by `Attempt.start_operation_id` and validates closed shape/identity. Keep JSON only at the existing Store boundary; subsequent checks use named fields.

Recommended functions:

```rust
fn load_existing_task_dispatch(
    tx: &Transaction<'_>,
    start_operation_id: &str,
) -> Result<ExistingTaskDispatch>;

fn validate_task_dispatch_reuse(
    tx: &Transaction<'_>,
    existing: &ExistingTaskDispatch,
    request: &Value,
    attempt: &Value,
) -> Result<DispatchReuse>;

fn retain_task_dispatch_reuse_receipt(
    tx: &Transaction<'_>,
    current_operation_id: &str,
    reuse: &DispatchReuse,
) -> Result<()>;
```

`DispatchReuse` is private and contains only fields needed to stamp/result the current receipt.

## 7. Exact validation algorithm

### 7.1 Current request

Before slot handling keep:

- closed request fields;
- nonempty `attempt_id` and `text`;
- exact current Principal refresh performed by outer Store;
- `gm::require_attempt_control`;
- `Attempt.start_owner == controller`.

If `start_operation_id` exists:

1. require `Attempt.released_at_ms == null`;
2. load the exact start Operation;
3. require method `task.dispatch`;
4. require state in:

```text
queued
sending
native_accepted
outcome_unknown
settled
rejected
```

5. `cancelled` → typed `ATTEMPT_START_CANCELLED` / conflict instructing release/new Attempt;
6. any unknown state → `ATTEMPT_START_CORRUPT`;
7. require Operation columns equal exact Attempt Task/Attempt/binding tuple;
8. require original request `attempt_id` exact;
9. compare current and prior `text`, `prerequisite_operation_id`, `launch_operation_id` exactly, including absence vs presence;
10. require prior prerequisite column equals prior original request value;
11. validate launch extension as §7.2;
12. return reusable start identity/state.

Do not compare current `client_request_id` to the old one. Different ID is the reason a new receipt exists.

### 7.2 Launch extension

Refactor existing `launcher_dispatch::validate_coalesced_dispatch` into a narrowly named extension validator, for example:

```rust
pub(super) fn validate_reused_launch_dispatch(
    db: &Connection,
    existing: &ExistingTaskDispatchView<'_>,
    request: &Value,
    attempt: &Value,
) -> Result<Option<LaunchReuseEvidence>>;
```

It owns only:

- retained launch parent resolution;
- exact requested parent;
- immutable launch packet/packet digest;
- private ancestry link;
- parent progress relation.

Generic Operation state/Task/Attempt/binding checks belong to the generic resolver and must not be hidden inside launch-only code.

For no parent, reject any retained or requested launch-only evidence. For a parent, return bounded evidence such as parent ID and packet digest for the reuse receipt.

Do not re-run current C8 capability readback and do not rebuild the packet: no new input is being admitted.

## 8. Current Operation receipt

On successful semantic reuse, update the **current** Operation inside the mutation savepoint:

```sql
UPDATE operations
SET task_id = :task_id,
    attempt_id = :attempt_id,
    prerequisite_operation_id = :prerequisite,
    effective_request_json = :reuse_effective
WHERE operation_id = :current
  AND method = 'task.dispatch'
  AND state = 'queued';
```

Require rowcount `1`.

### 8.1 Intentionally leave binding columns NULL

The current receipt is not a native admission. If it copied binding columns, the existing `capacity::sync_operation` would treat every coalesced `task.dispatch` as another reserved capacity entry until Attempt resolution.

Therefore:

```text
current receipt task_id/attempt_id = exact
current receipt binding_id/generation = NULL
prior start Operation retains binding/effect ownership
```

This is intentional and must be asserted. Do not “fix” capacity by teaching it to infer aliases from arbitrary result JSON.

### 8.2 Minimal effective projection

Do not copy raw Task snapshot, TaskPromptEnvelope, route or launch packet into the reuse receipt. They belong to the effect owner.

Recommended retained object:

```json
{
  "semantic_reuse": {
    "kind": "task_dispatch_start_slot",
    "start_operation_id": "...",
    "start_operation_state_at_receipt": "queued",
    "native_effect_created": false,
    "current_state_read_method": "operation.get",
    "launch_operation_id": null,
    "launch_packet_digest": null
  }
}
```

The generic wrapper adds `receipt` afterwards. Task/Attempt/prerequisite identities live in columns; do not duplicate them into another JSON authority.

### 8.3 Public result

Return:

```json
{
  "operation_id": "<current settled receipt Operation>",
  "task_id": "...",
  "attempt_id": "...",
  "coalesced": true,
  "semantic_reuse": true,
  "start_operation_id": "<prior effect Operation>",
  "start_operation_state_at_receipt": "queued",
  "native_effect_created": false,
  "current_state_read_method": "operation.get"
}
```

The row state is settled by the existing generic wrapper. Observation relational ID and payload `operation_id` now agree.

Do not copy the prior Operation result into this response. Mutable state is read through `operation.get(start_operation_id)`.

## 9. Why not use a pre-insert alias

`swarm.launch` and direct repair have pre-insert alias records. They are useful internal donors for the response fields:

```text
operation_state_at_receipt
current_state_read_method
semantic_reuse
```

But `task.dispatch` documentation explicitly requires the second caller's own settled/coalesced receipt. Keep that audit Operation; make it correct rather than replacing it with a new alias table/meta family.

No external idempotency engine, DBOS/Restate service, extra outbox or public method is needed.

## 10. Interaction with R20 Task Prompt v2

R20/#46 owns prompt creation for the first effect Operation.

R28 rules:

- semantic reuse receipt stores no `task_snapshot` and no `task_prompt`;
- exact input comparison uses the original request fields already retained on the start Operation;
- the effect owner remains the only Operation carrying TaskPromptEnvelope/native payload digest;
- after R20, the reuse resolver may verify the prior prompt contract through the prior Operation's typed validator, but must not rebuild or resend it;
- no parallel writer in `operations.rs` while #46 is implementing its Store producer.

## 11. Interaction with R23/R25/R27

- R23/#49 derives Operation visibility from positive retained relations. R28 supplies Task/Attempt columns for this receipt; R23 must not infer them from `result.start_operation_id`.
- R25/#51 uses the same exact TaskGraphIdentity.
- R27/#53 owns `task.create`, `task.claim`, `task.release`; it does not modify dispatch.
- R13/#39 owns capacity. R28 intentionally avoids a second capacity entry instead of adding alias heuristics to the ledger.
- R05/#31 owns normalized native admission receipt identity. The coalesced receipt has no new native admission and must not synthesize one.

## 12. Tests that must replace the current weak assertion

Use public Store mutation/read paths, not helper-only tests.

### T1. Same `client_request_id`

- exact retry returns original receipt;
- no new Operation row;
- no new observation;
- no new native command.

### T2. New ID, exact semantic reuse

- first dispatch returns Operation A queued;
- second returns Operation B settled;
- B.result.operation_id == B.operation_id;
- B.result.start_operation_id == A.operation_id;
- B.task_id/attempt_id exact;
- B.binding columns NULL;
- only A is queued/effect-owning;
- no second RuntimeCommand;
- no second capacity ledger entry.

### T3. Observation consistency

The controller observation for B has relational `operation_id=B` and payload `operation_id=B`; its payload refers to A only through `start_operation_id`.

### T4. GM handover

Update `successor_gm_continues_exact_attempt_without_restarting_dispatch`:

- successor's receipt Operation is inspected directly;
- prior start remains the only queued dispatch;
- current GM gets a scoped settled receipt without replay;
- unrelated Manager remains forbidden.

### T5. Lifecycle table

Table-drive prior states:

| Prior state | Expected |
|---|---|
| queued | settled reuse receipt |
| sending | settled reuse receipt |
| native_accepted | settled reuse receipt |
| outcome_unknown | settled reuse receipt; no retry |
| settled | settled reuse receipt |
| rejected | settled reuse receipt pointing at rejected effect owner; no replay |
| cancelled | typed conflict; release/new Attempt required |
| malformed/unknown | corruption error |

### T6. Released Attempt

Release the Attempt after first dispatch, then send exact new request ID. It must not return coalesced success and must not create a native command.

### T7. Intentional no-effect bypass

After first admission, independently:

- disable `new_work`;
- make binding reconciling;
- revise Task while retaining old Attempt.

An exact new request still receives a settled reuse receipt because no effect is admitted. This test prevents a future “fix” from reapplying new-admission gates.

### T8. Exact conflict dimensions

Changed text, changed prerequisite, added/removed/different launch parent each reject and never alter A/Attempt start slot.

### T9. Damaged prior tuple

Mismatch prior Operation Task/Attempt/binding/prerequisite or missing row → exact corruption error; no success-shaped reuse.

## 13. Code locations

Primary:

- `crates/swarm-kernel-host/src/store/operations.rs::dispatch`;
- `crates/swarm-kernel-host/src/store/launcher_dispatch.rs::validate_coalesced_dispatch` and `validate_retained_link`;
- `crates/swarm-kernel-host/src/store/mod.rs::mutate_in_transaction_with_authority` generic settlement remains unchanged;
- `crates/swarm-kernel-host/src/store/gm_continuation_tests.rs`;
- add a focused task-dispatch reuse test module if the GM fixture becomes overloaded.

Read-only cross-check:

- `crates/swarm-kernel-host/src/store/capacity.rs::sync_operation/derive_operation`;
- `docs/agent_swarm.implementation-v6.md`, “Origin и один начальный dispatch”.

## 14. What to delete

After migration:

- result shape that returns prior start ID as `operation_id`;
- generic direct-dispatch path that validates only “no launch fields”;
- ambiguous `validate_coalesced_dispatch` ownership of both generic and launch concerns;
- test assertion `coalesced.operation_id == start_operation_id`;
- comments implying current settled receipt is the native effect Operation.

Do not add a compatibility branch preserving both result shapes.

## 15. Implementation order

One manager/worktree; writers receive non-overlapping files and do not run Cargo.

1. Add private typed prior-start loader and validation in `operations.rs`.
2. Narrow/refactor launch-specific reuse validation.
3. Stamp current settled receipt Task/Attempt/prerequisite and minimal effective reuse object.
4. Return current operation ID + separate start operation ID.
5. Replace GM continuation assertion and add lifecycle/capacity/observation scenarios.
6. Rebase with R20/#46 if it has begun changing `operations.rs`; one manager resolves the seam.
7. Scoped formatting and minimal Clippy.

No broad/native tests until the code slice is complete.

## 16. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then run only the exact Store test targets containing the new public-path scenarios. Broad workspace/native qualification remains the final phase.

In the PR report include:

- base/head SHA;
- exact prior-start loader and current-receipt stamp symbols;
- result/effective schema actually implemented;
- deleted old assertion/path;
- proof that only one RuntimeCommand and one capacity entry exist;
- scoped Clippy result;
- remaining live/native qualification.

## 17. Non-goals

- replaying or retrying the prior native input;
- reopening a cancelled/released Attempt;
- changing Task Prompt content;
- introducing a new alias table/meta family;
- changing generic exact `client_request_id` replay;
- changing launch C8 admission;
- copying binding columns to the no-effect receipt;
- changing capacity policy;
- inferring authority from result JSON;
- compatibility union for old/new coalesced result shapes;
- modifying credentials, routes, models or running services.
