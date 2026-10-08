# R30. GM designation fencing: один валидатор, монотонная epoch, безопасный handover

**Статус:** implementation handoff. Текущий diff содержит только это задание; production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленные участки не переписывать.

## 1. Результат

Current GM designation имеет одно authority-определение во всём Store:

```text
registered enabled Manager client
+ exact positive monotonically increasing GM epoch
+ optional non-authoritative session binding pointer
```

Один typed loader используется GM methods, Forge, automation transfer/acceptance/publication, MCP/read visibility и launch authority evidence. Повреждённая запись никогда не трактуется как `None`, epoch `0` или просто совпавший `client_id`.

`gm.handover`:

- первое designation получает epoch 1;
- смена client identity увеличивает high-water ровно на 1;
- rebind/detach того же valid client сохраняет epoch;
- возврат A → B → A получает новую epoch, а не возвращает старую;
- потерянная current pointer после исторических handover восстанавливается локальным Operator как новая epoch;
- malformed current record не сбрасывает fence в 1;
- target обязан быть registered enabled `Role::Manager`;
- optional binding остаётся session hint и не создаёт authority.

Никакой новой IAM-системы, event store, workflow engine или public recovery method.

## 2. Нормативная граница

`docs/owner-decisions.md` и `docs/gm-session-continuity.md` задают:

- тот же durable manager client может reconnect/rebind без epoch rotation;
- designation другого client вращает epoch;
- former GM теряет GM-only authority;
- queued Forge publication сохраняет admitted epoch и сравнивает её с current epoch;
- новый successor — registered manager;
- local Operator выполняет recovery/handover, если previous GM недоступен;
- Operations и receipts authoritative и не подлежат automatic eviction.

Следовательно, `epoch` является fencing token, а не необязательным display counter. Повтор значения после уже выданной epoch нарушает contract даже тогда, когда другой guard случайно блокирует конкретный эффект.

## 3. Подтверждённые расхождения

### 3.1 `require_authority` читает только client ID

`store/gm.rs::require_authority` принимает Manager, если raw `meta["gm"]["client_id"]` совпадает. Оно не требует:

- object shape;
- positive integer epoch;
- current registration role `manager`;
- enabled registration;
- binding-pair consistency.

Обычный facade часто вызывает `current_principal` раньше, но helper является самостоятельной authority boundary и используется во многих Store/domain paths. Он не должен зависеть от неявного порядка вызывающего.

### 3.2 `handover` превращает damage в epoch 0

Текущий код:

```rust
let previous_epoch = previous
    .as_ref()
    .and_then(|gm| gm["epoch"].as_i64())
    .unwrap_or(0);
```

Следствия:

- missing/null/string/negative epoch на client rotation даёт новую epoch 1;
- same-client rebind сохраняет 0;
- client-only authority продолжает работать, пока Forge/automation readers требуют `epoch > 0`;
- ранее использованная epoch может быть выдана повторно.

Это не corruption-tolerance. Это fail-open downgrade одного fencing fact.

### 3.3 Один record имеет несколько несовместимых readers

На baseline существуют по меньшей мере следующие определения:

1. `gm::require_authority`: `client_id` equality, epoch не читается.
2. `store/forge.rs::current_gm_epoch`: `gm::record` + positive epoch, client отдельно.
3. `automation/publication.rs::current_gm_epoch_for`: raw SQL tuple `(String, i64)`, exact owner + epoch > 0.
4. `automation/authorization.rs::current_gm_epoch_for` и transfer continuation: ещё один raw SQL tuple.
5. `automation/acceptance.rs::current_gm`: собственный `CurrentGm` parser.
6. `store/automation_acceptance.rs` и `store/automation_transfer.rs`: прямые SQL tuple readers.
7. `launcher_native_mcp.rs`: raw `meta("gm")` поля входят в authority digest без общей валидации.
8. `OPERATION_VISIBILITY_SQL`: current-GM shortcut сравнивает raw JSON `client_id`, не positive epoch/registration.

В результате одна запись может одновременно означать:

```text
GM authority = yes
Forge/automation authority = no
Operation read shortcut = yes
launch authority digest = malformed/null
```

### 3.4 Handover принимает не-Manager target

Target role проверяется denylist'ом внутренних ролей. `Observer`, `Participant` и local `Operator` не запрещены.

Но:

- owner decisions и recovery docs описывают registered successor Manager;
- automation transfer требует registered Manager;
- manager Task/Attempt control требует `Role::Manager`;
- Observer/Participant intentionally have no generic writer authority.

Такой target создаёт designation, которое часть методов принимает по client ID, а остальные поверхности отвергают по role. Это ещё один split-brain contract.

### 3.5 Missing current record после handover забывает history

`None` всегда означает «GM never designated» и следующая epoch начинается с 1. Но Operations/receipts сохраняются authoritative. Если current pointer потерян после epoch N, новый handover не должен повторно выдать 1.

## 4. Один internal type

Добавить в `store/gm.rs` data-only authority projection:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CurrentGm {
    client_id: String,
    epoch: i64,
    binding: Option<GmBindingRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GmBindingRef {
    binding_id: String,
    generation: i64,
}

#[derive(Debug, Clone)]
enum GmDesignationState {
    NeverDesignated,
    Current(CurrentGm),
    MissingAfterHistory { epoch_high_water: i64 },
    Damaged { code: &'static str, epoch_high_water: i64 },
    StaleRegistration { current: CurrentGm, epoch_high_water: i64 },
}
```

Names may differ, semantics may not.

Do not deserialize the full current JSON envelope into a second public DTO. Authority needs only:

- non-empty bounded client ID;
- positive epoch;
- binding fields both absent/null or both exact typed values.

Handover/resync receipt metadata remains retained JSON and is not copied into every consumer.

## 5. Exact helpers

All GM consumers use these helpers or equivalent typed forms:

```rust
pub(super) fn designation_state(db: &Connection) -> Result<GmDesignationState>;
pub(super) fn current(db: &Connection) -> Result<Option<CurrentGm>>;
pub(super) fn current_epoch(db: &Connection) -> Result<i64>;
pub(super) fn require_current_manager(db: &Connection, manager_id: &str) -> Result<i64>;
pub(super) fn require_authority(db: &Connection, principal: &Principal) -> Result<()>;
pub(super) fn authority_facts(db: &Connection) -> Result<Value>;
```

Rules:

- `current_epoch == 0` only when no current designation **and** no historical successful handover;
- structural damage never maps to zero;
- `require_current_manager` checks designation, client registration, disabled=false and role=manager;
- `require_authority`:
  - Operator: exact local-operator identity;
  - Manager: refreshed/registered Manager matching current designation;
  - all other roles: forbidden;
- missing/damaged current after historical epochs is not a valid GM authority;
- session binding does not grant or revoke GM authority.

`record()` must no longer be the generic internal reader. If host status needs the full retained envelope, expose a separately named bounded diagnostic projection; authority code cannot call it.

## 6. Epoch high-water from existing Operations

Do not create a second epoch table or counter. The immutable Operation ledger already retains every successful `gm.handover`.

Add one private query/helper used by designation classification and handover:

```rust
fn handover_epoch_high_water(db: &Connection) -> Result<i64>;
```

A historical row counts only when all are exact:

```text
method = gm.handover
state = settled
result_json is object
result.operation_id == operation_id
result.client_id is non-empty string
result.gm_epoch is positive integer
original_request_json.client_id == result.client_id
```

A settled `gm.handover` row with malformed/mismatched result is `GM_EPOCH_HISTORY_DAMAGED`, not silently skipped. Rejected Operations do not allocate an epoch.

Implementation may use one aggregate validity query plus `MAX(gm_epoch)`; it must not deserialize every unrelated Operation or add a new unbounded Rust-side history object.

High-water resolution:

```text
valid current epoch > history max      allowed legacy/current authority
valid current epoch == history max     ordinary current authority
valid current epoch < history max      damaged/regressed current pointer
no current + history max == 0          never designated
no current + history max > 0           missing current pointer, operator recovery only
malformed current                      damaged, operator recovery only
```

Because Operations are retained by owner policy, this is the already-existing durable fence history. Restate/DBOS/Temporal are not imported.

## 7. Handover algorithm

### 7.1 Caller

At the start of `handover`:

1. refresh caller registration or use a caller already refreshed in the same transaction;
2. local Operator may always enter recovery path;
3. ordinary Manager must pass strict `require_authority` against a valid current designation;
4. Observer/Participant/Module identities cannot initiate handover even if raw client ID matches a damaged record.

### 7.2 Target

Target must be:

```text
registered
not disabled
role == manager
```

Do not use a denylist. Exact allowlist is the product contract.

Optional binding:

- pair is all-or-nothing;
- binding row exists and is not released at handover;
- it remains a session pointer only;
- no Task/Operation/GM right is inferred from binding ownership;
- binding later closing does not erase Manager authority.

A future exact manager-session binding contract may strengthen this pointer separately. R30 must not invent ownership evidence that the schema does not retain.

### 7.3 Epoch choice

Let `H` be validated historical high-water.

```text
NeverDesignated:
  target epoch = 1

Current(valid) and same target client:
  target epoch = current.epoch

Current(valid) and different target client:
  target epoch = max(current.epoch, H) + 1

MissingAfterHistory / Damaged / StaleRegistration:
  only local Operator
  target epoch = H + 1
  recovery is authority_changed=true even if the surviving client_id text matches
```

Every addition is checked; overflow returns `EPOCH_OVERFLOW` without mutation.

When valid current epoch is greater than history because it predates retained result shape, the next authority change starts at `current.epoch + 1`. Same-client rebind preserves it.

### 7.4 Recovery receipt

Keep existing response fields and add explicit bounded facts:

```json
{
  "designation_recovered": true,
  "recovery_reason": "missing_after_history | damaged_current | stale_registration",
  "epoch_high_water_before": 7,
  "gm_epoch": 8
}
```

Ordinary handover/rebind returns `designation_recovered:false` and null reason.

Do not copy malformed raw designation into the result. Previous client/binding fields are emitted only when validated; otherwise explicit nulls.

## 8. Replace duplicate readers

### Store

- `store/gm.rs` — owner of all parsing, role checks and epoch high-water.
- `store/forge.rs` — delete local `current_gm_epoch`; call `gm::current_epoch/require_current_manager`.
- `store/automation_transfer.rs` — delete raw SQL `GmScopeRow`; use strict helper.
- `store/automation_acceptance.rs` — delete direct tuple reader.
- `store/launcher_native_mcp.rs` — use `gm::authority_facts`; malformed designation makes launch snapshot stale/blocked rather than hashing null or wrong types.
- `store/mod.rs` Operation visibility/current-GM paths — bind a typed current-GM identity computed before SQL; do not inspect `meta.gm.client_id` in authorization SQL.

### Domain automation

- `automation/publication.rs` — delete local `current_gm_epoch_for`.
- `automation/authorization.rs` — delete duplicate reader; transfer continuation uses strict helper.
- `automation/acceptance.rs` — delete private `CurrentGm/current_gm` parser; import Store-owned projection through a narrow Store helper, or move the provider-neutral identity type to one shared data-only module if dependency direction requires it.

Do not move SQLite access into `swarm-contracts`. The type may be data-only; record loading stays Store-owned.

### Frontend/read policy

R23/#49 and R24/#50 change adjacent Operation/MCP authorization. They must consume the typed helper and must not retain raw SQL/client-ID-only GM shortcuts after rebase.

## 9. Status and diagnostics

Host/doctor must remain usable when designation is damaged.

Preferred public projection:

```json
{"state":"none","client_id":null,"epoch":0}
{"state":"current","client_id":"manager-a","epoch":7,"binding_id":null,"binding_generation":null}
{"state":"damaged","error_code":"GM_DESIGNATION_DAMAGED","epoch_high_water":7}
{"state":"missing_after_history","error_code":"GM_DESIGNATION_MISSING","epoch_high_water":7}
```

Do not expose raw malformed JSON or turn one damaged GM record into failure of all `host.status` fields. Mutation authority remains fail-closed.

## 10. Operation visibility and MCP authorization

A raw client-ID comparison is not authority.

For Operation/list/report/MCP authorization:

1. compute strict current GM once from the same read transaction;
2. pass `Option<&str>`/epoch into query parameters or Rust relation resolver;
3. current-GM grants require role manager and exact valid designation;
4. damaged designation yields no GM grant and a diagnostic gap/error where the API supports it;
5. local Operator remains independently authorized.

Do not duplicate JSON type predicates in every SQL clause. One typed pre-read feeds the predicates.

## 11. Tests

Use public Store paths for end-to-end authority tests; small helper tests are additional, not substitutes.

### Epoch

- [ ] First designation is epoch 1.
- [ ] A → B → A produces 1 → 2 → 3.
- [ ] Same-client rebind/detach preserves 3.
- [ ] Exact same target/binding is conflict and allocates nothing.
- [ ] Epoch overflow rejects without changing designation or Operation history.
- [ ] Rejected handover does not advance high-water.

### Damage/recovery

- [ ] `epoch:null`, string, zero and negative do not map to 0/1.
- [ ] Valid client ID with malformed epoch grants no GM method/read shortcut.
- [ ] Delete current `gm` after epoch 3; local Operator recovery emits epoch 4.
- [ ] Same damaged client recovered by Operator still rotates to high-water+1.
- [ ] Ordinary Manager cannot recover damaged/missing-after-history designation.
- [ ] One malformed settled handover receipt returns `GM_EPOCH_HISTORY_DAMAGED`.
- [ ] Host status remains readable and marks the designation damaged.

### Roles

- [ ] Manager target succeeds.
- [ ] Observer, Participant, Module, Scheduler, HookSource, ModuleSupervisor and Operator targets reject before designation mutation.
- [ ] Disabling/removing/changing role of current Manager revokes GM-only mutations.
- [ ] Local Operator can designate another enabled Manager after stale registration.

### Consumers

- [ ] Forge admission/pre-write compares the exact shared epoch.
- [ ] Acceptance/publication/transfer use the same helper and error classification.
- [ ] Operation list/get and MCP catalog/call do not grant current-GM scope for malformed designation.
- [ ] Launch native-MCP authority digest uses the exact shared client/epoch projection.
- [ ] No production `SELECT ... FROM meta WHERE key='gm'` remains outside `store/gm.rs` and diagnostic migration/tests.

## 12. Files and symbols

Primary implementation:

- `crates/swarm-kernel-host/src/store/gm.rs`
- `crates/swarm-kernel-host/src/store/forge.rs`
- `crates/swarm-kernel-host/src/store/automation_transfer.rs`
- `crates/swarm-kernel-host/src/store/automation_acceptance.rs`
- `crates/swarm-kernel-host/src/store/launcher_native_mcp.rs`
- `crates/swarm-kernel-host/src/store/mod.rs`
- `crates/swarm-kernel-host/src/automation/authorization.rs`
- `crates/swarm-kernel-host/src/automation/acceptance.rs`
- `crates/swarm-kernel-host/src/automation/publication.rs`

Tests/docs:

- existing `gm.rs` tests;
- `store/security_tests.rs` Forge epoch tests;
- GM continuation/program tests;
- MCP profile/authorization tests after #50 rebase;
- `docs/gm-session-continuity.md` and owner-decision implementation note only if actual behavior wording needs correction.

## 13. What to delete

After migration:

- `gm::record` as a generic authority reader;
- `unwrap_or(0)` epoch fallback;
- role denylist for target designation;
- `store/forge.rs::current_gm_epoch` duplicate;
- both `current_gm_epoch_for` copies;
- automation acceptance's private `CurrentGm` parser;
- `GmScopeRow` direct tuple reader;
- launcher raw `meta("gm")` read;
- current-GM raw JSON subqueries in authorization SQL.

Search gate:

```text
FROM meta WHERE key='gm'
meta(db, "gm")
["epoch"].as_i64().unwrap_or(0)
```

Production occurrences outside the single owner must be zero.

## 14. Implementation order

One manager/worktree. Files overlap R23/#49 and R24/#50; no parallel writers in `store/mod.rs` or MCP authorization seams.

1. Add typed designation state + exact registration validation in `store/gm.rs`.
2. Add validated Operation-history high-water.
3. Rewrite `handover` and its focused tests.
4. Replace Forge/automation duplicate readers.
5. Replace launcher authority projection.
6. Replace Operation/MCP raw GM shortcuts; rebase #49/#50 as one manager.
7. Update status diagnostics and delete old readers.
8. Scoped formatting and minimal Clippy.

No broad native tests until the production slice is connected.

## 15. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-mcp \
  --lib --bins -- -D warnings
```

Then exact Store/MCP tests named in the implementation report. Broad/native qualification later.

## 16. Non-goals

- redesign Manager/Operator roles;
- bind GM authority to a native session;
- auto-handover on disconnect;
- rewrite historical owner/caller identities;
- adopt/replay old Forge effects;
- add a new GM recovery method;
- new table, event store, counter service or workflow engine;
- hide damaged designation as no GM;
- change Task ownership;
- grant a successor arbitrary historical mailbox access;
- infer current GM from latest chat, binding, timestamp or process liveness.
