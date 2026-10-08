# R27. Task coalescing: новая Operation сохраняет точный Task/Attempt scope

**Статус:** implementation handoff. Текущий diff содержит только это задание; production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Результат

Каждая новая успешно завершённая Operation, даже когда она не создаёт новый Task/Attempt и только подтверждает существующее состояние, сохраняет точную объектную связь в собственных колонках `task_id` / `attempt_id`.

```text
task.create(origin already exists)
  -> current Operation.task_id = existing Task

task.claim(existing matching Attempt)
  -> current Operation.task_id/attempt_id = existing Task/Attempt

task.release(already released with same outcome)
  -> current Operation.task_id/attempt_id = released Task/Attempt
```

Changed terminal outcome после release отвергается. Неверная binding pair у claim отвергается до semantic-coalesce decision. Exact replay того же `client_request_id` по-прежнему обслуживается общим Operation idempotency path.

Это исправление объектной identity и readback. Оно не создаёт новый Task state machine, alias table, migration или второй authorization layer.

## 2. Подтверждённые дефекты

### 2.1 `task.create`: result называет Task, Operation — нет

`store/tasks.rs::create_validated` при найденном `origin_key` возвращает:

```json
{
  "operation_id": "current-operation",
  "task_id": "existing-task",
  "created": false,
  "reason": "origin_already_exists"
}
```

Но ветка не выполняет `UPDATE operations SET task_id=...`. Общий mutation wrapper затем сохраняет успешный result/receipt, а `operation.get`, object authorization, reports и future positive relation resolvers видят `task_id = NULL`.

Глобальная uniqueness `origin_key` **не является дефектом этого PR**. Миграция и GitHub work-pool прямо используют origin как стабильную imported-root identity, включая перенос/rename. R27 исправляет только связь текущей receipt Operation с уже существующим Task.

### 2.2 `task.claim`: coalesced Attempt не связывается с новой Operation

`claim_with_authority` при совпадении owner/revision/start_owner/binding возвращает `created:false` до обычного `UPDATE operations SET task_id,attempt_id`.

Дополнительно `validate_claim_binding_pair` вызывается только после ветки existing Attempt. Запрос, содержащий только `binding_id` либо только `binding_generation`, поэтому может получить `TASK_ALREADY_OWNED` вместо закрытой ошибки формы. Coalescing не должен обходить parser/invariant запроса.

### 2.3 `task.release`: повтор допускает другой terminal outcome

Если `released_at_ms` уже задан, `release`:

1. заново вызывает `prepare_owned_service_attempt_release`;
2. не сравнивает requested `outcome` с retained `attempt.state`;
3. возвращает `changed:false` без `task_id/attempt_id` в текущей Operation.

Следствия:

- `failed` Attempt можно повторно «release as cancelled» и получить успешную квитанцию;
- harmless readback может неожиданно упасть на позднем owned-service fence, хотя исходный release уже завершён;
- Operation без object relation сложнее авторизовать fail-closed.

## 3. Нормативная граница idempotency

Различать три случая.

### A. Exact request replay

Тот же caller + `client_request_id` + byte-identical original request. Общий mutation layer возвращает сохранённую receipt до нового handler effect. R27 это не меняет.

### B. Semantic no-op под новым request ID

Новый Operation подтверждает уже существующий объект:

- Task того же origin;
- matching unreleased Attempt;
- already released Attempt с тем же outcome.

Новая Operation получает собственный `operation_id`, сохраняет object relation и заканчивается `settled` с `created:false` / `changed:false`. Она не выдаётся за исходную Operation и не повторяет внешний эффект.

### C. Semantic conflict

Под тем же объектом запрошено несовместимое состояние:

- claim binding shape malformed;
- existing Attempt tuple differs;
- released Attempt outcome differs.

Возвращается typed conflict/validation failure; текущая Operation остаётся rejected по обычному wrapper contract.

## 4. Один private helper, без нового framework

Добавить в `store/tasks.rs` один private helper, например:

```rust
fn attach_operation_scope(
    tx: &Transaction<'_>,
    operation_id: &str,
    task_id: &str,
    attempt_id: Option<&str>,
) -> Result<()>;
```

Требования:

1. загрузить `method`, `state`, retained `task_id/attempt_id` текущей Operation;
2. требовать ожидаемый Task-family method и `state='queued'`;
3. разрешать только `NULL -> expected` либо already-exact value;
4. несовпавшая non-null identity → `STORE_INVARIANT`/typed conflict;
5. выполнить guarded UPDATE и проверить rowcount = 1;
6. не менять caller/client_request/original/effective request и binding columns;
7. не принимать Task/Attempt из handler result JSON как authority — helper получает уже проверенные IDs.

Не выносить generic `OperationScopeFramework` в новый crate. Другие домены имеют иные relation requirements; повторное использование возможно позднее только после второго одинакового caller.

## 5. `task.create` implementation

В `create_validated`, ветка existing origin:

1. прочитать exact retained `task_id`;
2. вызвать `attach_operation_scope(tx, id, &task_id, None)`;
3. вернуть текущую форму result.

Не обновлять retained Task, project или spec. Не менять global origin index. Изменённый imported source должен идти через `task.revise`, а не скрыто mutate existing Task при повторном create.

Проверить, что caller current Operation получает `task_id` до generic receipt/observation/capacity readback.

## 6. `task.claim` implementation

### 6.1 Parse binding pair до Store coalescing

Сразу после базовых scalar fields:

```rust
validate_claim_binding_pair(v.get("binding_id"), v.get("binding_generation"))?;
```

Затем один раз нормализовать requested binding в private enum/value:

```rust
enum RequestedBinding<'a> {
    Unbound,
    Bound { binding_id: &'a str, generation: i64 },
}
```

Это предлагаемая local representation, не public API. Explicit `(null,null)` и omission оба означают `Unbound`; половина пары invalid. Bound values проходят `model::text/positive` один раз.

### 6.2 Existing Attempt comparison

Сравнивать exact:

```text
owner_id
Task revision
start_owner
binding_id + generation as one pair
released_at_ms is null (already guaranteed by current pointer, but validate retained Attempt)
```

При полном совпадении:

1. `attach_operation_scope(tx, id, task_id, Some(existing_attempt_id))`;
2. return `created:false`.

Не повторять baseline freeze, dependency resolution, capacity reservation или binding readiness. Это подтверждение уже созданного Attempt, не новая admission.

При различии вернуть существующий `TASK_ALREADY_OWNED` с exact retained Attempt ID. Не выбирать новый owner/binding и не mutate Attempt.

### 6.3 New Attempt path

Reuse уже разобранный `RequestedBinding`; убрать второй raw match по JSON. Только bound new claim читает binding и требует ready. Обычный insert и Operation scope остаются одним путём через тот же helper либо текущий guarded UPDATE — не два расходящихся implementations.

## 7. `task.release` implementation

После current authority и загрузки Attempt:

### Already released

1. получить retained state;
2. require `requested_outcome == retained_state`;
3. при mismatch вернуть новый bounded code, например:
   - `ATTEMPT_RELEASE_CONFLICT`,
   - message: `Attempt was already released with a different outcome`;
4. **не** вызывать `prepare_owned_service_attempt_release` второй раз;
5. `attach_operation_scope(tx, id, task_id, Some(attempt_id))`;
6. вернуть:

```json
{
  "operation_id": "current-operation",
  "task_id": "...",
  "attempt_id": "...",
  "released": true,
  "changed": false,
  "outcome": "retained-outcome"
}
```

Reason текущего no-op request не заменяет historical release reason. Historical reason остаётся в исходной Operation; Attempt schema этим PR не расширяется.

### First release

Сохранить текущие acceptance/resource/unresolved-producer guards и owned-service fence. После успешной transition связать текущую Operation тем же helper. Existing queued-operation cancellation и capacity sync не менять в R27.

## 8. `task.dispatch` сознательно не входит

`task.dispatch` при existing `start_operation_id` возвращает prior Operation identity, проверяет launch ancestry и имеет другой effect/replay contract. Его нельзя механически преобразовать в обычную settled no-op Operation в R27.

Связанные owners:

- R20/#46 меняет dispatch prompt/effective request;
- launcher dispatch хранит C8 ancestry/packet;
- Operation alias/read scope меняется в R23/#49.

R27 добавляет отдельный audit note: current `task.dispatch` semantic reuse требует последующего решения — либо pre-insert alias receipt по образцу launch/repair, либо current Operation relation + explicit `semantic_reuse_of_operation_id`. Не смешивать это с тремя простыми Task mutations.

## 9. Доноры и почему новый engine не нужен

ELIOT уже реализует нужный основной механизм: Operation создаётся до mutation, exact `client_request_id` replay возвращает retained receipt, transaction коммитит object relation и receipt вместе.

Restate/DBOS patterns «identity before external effect» полезны для внешних calls, но здесь внешнего эффекта нет. Добавление workflow engine, outbox или второй idempotency table только продублирует существующую Operation.

Использовать собственные guarded-update patterns из:

- `operations::dispatch`;
- review/acceptance producers;
- coordination thread scope stamping.

Не копировать их целиком; взять только rowcount-checked exact relation update.

## 10. Files and symbols

Primary:

- `crates/swarm-kernel-host/src/store/tasks.rs`
  - `create_validated`;
  - `claim_with_authority`;
  - `release`;
  - new private `attach_operation_scope` / binding parser.
- `crates/swarm-kernel/src/tasks.rs`
  - optional provider-neutral helper for already-released outcome comparison only if it has a second consumer or materially simplifies Store;
  - otherwise keep the comparison locally and do not create an unused API.

Read-only projections/authorization are not rewritten here. R23/#49 and R25/#51 consume the corrected relation later.

## 11. Criteria

### Create

- [ ] New origin creates Task and links Operation.task_id.
- [ ] Same origin under a new request ID returns `created:false` and links current Operation to the existing Task.
- [ ] Exact client_request replay returns original receipt and creates no Operation.
- [ ] Global origin semantics/project are unchanged.

### Claim

- [ ] New claim creates Attempt and links current Operation Task/Attempt.
- [ ] Same matching claim under a new request ID returns `created:false` and links current Operation.
- [ ] Only binding_id or only generation rejects as INVALID_PARAMS before TASK_ALREADY_OWNED.
- [ ] Different owner/revision/start/binding returns TASK_ALREADY_OWNED and does not alter existing Attempt.
- [ ] Semantic no-op does not rerun baseline/dependency/native binding effects.

### Release

- [ ] First release retains current behavior and links current Operation.
- [ ] Same terminal outcome under a new request ID returns changed:false with exact Task/Attempt/outcome.
- [ ] Different outcome returns ATTEMPT_RELEASE_CONFLICT.
- [ ] Already-released no-op does not re-enter owned-service release fencing.
- [ ] Exact request replay still returns original receipt.

### Readback/security

- [ ] `operation.get` for each new semantic no-op has exact task/attempt columns.
- [ ] report/list grant resolvers can derive positive relation without parsing handler result JSON.
- [ ] No new global read permission is introduced.

## 12. Implementation order

One manager/worktree; writers receive non-overlapping symbols and do not run Cargo.

1. Add private operation-scope helper.
2. Move claim binding-pair parsing before coalescing and reuse normalized pair.
3. Connect create/claim no-op branches.
4. Correct already-released outcome semantics and connect release branches.
5. Remove duplicate raw Operation UPDATE statements in these three paths if helper replaces them.
6. Scoped formatting and minimal Clippy.

No DB migration, new method, descriptor or public DTO.

## 13. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Broad tests are a later phase. In the PR report include base/head SHA, exact modified branches, removed duplicate UPDATEs, Clippy result and remaining `task.dispatch` alias question.

## 14. Non-goals

- change global `origin_key` semantics;
- migrate Task tables/indexes;
- alter Task revision or acceptance;
- new Operation alias table;
- task.dispatch/launch packet refactor;
- new authorization framework;
- infer relation from result JSON;
- rewrite historical Operations;
- compatibility union or hidden fallback;
- run native models or services.
