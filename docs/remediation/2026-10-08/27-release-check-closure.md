# R27 companion. Attempt release должен разбудить уже существующий CheckRun cancellation path

**Статус:** обязательная часть implementation handoff #53. Production-код ещё не изменён. База `40591a295af94b1541ec2ba30afe8e3247701a71`.

## 1. Audit correction

Старое подозрение «`task.release` оставляет queued `check.run` навсегда; потребителя `cancel_requested` нет» слишком широкое.

Потребитель есть:

```text
Store::supervise_checks
  -> checks::next
  -> select queued CheckRun with cancel_requested
  -> preflight_error = CHECK_CANCELLED
  -> worker::failure
  -> checks::finish
  -> CheckRun terminal + check.run Operation settled
```

`checks::next` намеренно выбирает cancellation независимо от normal admission condition:

```sql
json_extract(c.spec_json,'$.cancel_requested') IS NOT NULL
OR (checks enabled AND capacity available ...)
```

Следовательно, новый cancellation state machine не нужен и прямое ручное settlement в `task.release` было бы второй расходящейся реализацией.

## 2. Подтверждённый разрыв

`task.release` для queued checks выполняет:

```sql
UPDATE check_runs
SET spec_json=json_set(
  spec_json,
  '$.cancel_requested',
  'attempt released before execution'
)
WHERE attempt_id=? AND state='queued';
```

и оставляет связанную `check.run` Operation в `queued`, чтобы существующий worker создал корректный terminal artifact/coverage/cancellation receipt.

Но worker lifecycle не видит эту новую обязанность при выключенной функции.

### 2.1 Demand predicate

`store/legacy_worker_demand.rs::snapshot` начинает checks worker, когда:

```text
config.checks.enabled
OR CheckRun state running/reconciling
OR CheckRun queued + Operation sending/outcome_unknown
```

Он **не** учитывает:

```text
CheckRun queued
+ Operation queued
+ spec.cancel_requested present
```

При `config.checks.enabled=false` demand остаётся false.

### 2.2 Wake predicate

Legacy worker coordinator при отсутствии demand и active slots не поллит timer. Он ждёт `Store.changed`.

`Store::call` включает в `wake_dispatch` `check.run` и `check.cancel`, но не `task.release`. Поэтому после release:

```text
demand до mutation = false
coordinator quiescent
Task.release writes cancel_requested
Store.changed не обновляется
coordinator не перечитывает demand
queued cancellation остаётся незавершённой
```

Она может случайно завершиться только после повторного включения checks либо другой mutation, которая разбудит coordinator. Это скрытая зависимость от unrelated traffic.

## 3. Почему release разрешён до terminal CheckRun

Queued check не имеет process/resource effect. `task.release` проверяет:

- no claimed CheckRun resource;
- no sending/native_accepted/outcome_unknown Operation;
- no unresolved producer.

Поэтому release может atomically попросить отмену ещё не запущенной проверки и завершиться. Check cancellation receipt закрывается асинхронно существующим worker. Это не ослабляет fence для running/reconciling CheckRun: ресурсный hold по-прежнему блокирует release.

## 4. Минимальная реализация

### 4.1 Расширить только exact demand

В `legacy_worker_demand::snapshot` расширить checks query:

```sql
SELECT EXISTS(
  SELECT 1
  FROM check_runs AS c
  JOIN operations AS o ON o.operation_id=c.operation_id
  WHERE c.state IN ('running','reconciling')
     OR (c.state='queued' AND o.state IN ('sending','outcome_unknown'))
     OR (
          c.state='queued'
      AND o.state='queued'
      AND json_extract(c.spec_json,'$.cancel_requested') IS NOT NULL
     )
)
```

Не считать demand любой queued check при disabled config: обычная новая работа должна оставаться выключенной. Новый predicate относится только к уже принятой cancellation obligation.

Не принимать одно поле `cancel_requested` без row relation: `check_runs.operation_id`, exact queued states и valid joined row обязательны.

### 4.2 Разбудить coordinator после release

Добавить `task.release` в существующий `wake_dispatch` set после успешной Store mutation. Это не запускает модель и не создаёт check process само по себе; watcher перечитывает durable demand.

Более сложный новый notification channel или result flag не нужен. `Store.changed` уже является общей wake-подсказкой для worker demand; correctness остаётся в durable query.

Wake после release допустим и без queued checks: это bounded hint. Не выполнять предварительный отдельный SELECT только ради подавления дешёвого wake — он создаст вторую проверку и TOCTOU без выигрыша.

### 4.3 Сохранить существующий terminal path

Не изменять:

- `checks::next` cancellation precedence;
- `Work.preflight_error`;
- `worker::failure`;
- `checks::finish`;
- CheckRun result/coverage/artifact contract;
- `check.cancel` public method.

`task.release` не должен напрямую выставлять CheckRun `cancelled`, `failed` или Operation `settled`: это обойдёт единый producer terminal evidence.

## 5. Повреждение и ошибки

- Valid queued cancellation держит demand до terminal settlement.
- Если cancellation row повреждена, worker error создаёт существующий incident/backoff; demand не исчезает молча.
- Storage error demand query остаётся fatal для coordinator snapshot, как и сейчас; не трактовать как no demand.
- Missing linked Operation/invalid state не надо «исправлять» UPDATE-ом из release. Это Store damage, не cancellation no-op.
- `cancel_requested` без `cancel_request` допустим именно для release-origin cancellation: `checks::next` использует reason для preflight failure, а direct `check.cancel` хранит дополнительно typed CancelRequest.

## 6. Tests

### T1. Audit correction: active worker path

С `checks.enabled=true`:

1. queue CheckRun;
2. release Attempt;
3. assert `cancel_requested` written;
4. supervise one cycle;
5. CheckRun and `check.run` Operation terminal;
6. no process execution/native command.

### T2. Disabled checks still close accepted cancellation

С `checks.enabled=false` и coordinator initially quiescent:

1. seed/admit a queued CheckRun created before disable, or create then switch exact test config/state;
2. call public `task.release`;
3. assert demand snapshot `checks=true` solely because queued cancellation exists;
4. assert `Store.changed` wakes coordinator;
5. existing cancellation path settles the check;
6. after settlement and no other obligations demand becomes false.

The test must not call `checks::next` directly as its only proof. It must enter through public mutation plus coordinator/demand path.

### T3. No accidental normal admission

With `checks.enabled=false`:

- ordinary queued check without `cancel_requested` does not demand/start worker;
- queued cancelled check does;
- another ordinary queued check remains queued while cancellation closes.

### T4. Running resource remains fenced

Running/reconciling check with claimed resource still makes `task.release` fail with `CHECK_RESOURCE_HELD`; new wake/demand does not bypass this guard.

### T5. Direct check.cancel unchanged

Public `check.cancel` on queued check still wakes and settles exactly once. Exact retry returns retained receipt; release-origin cancellation does not create a fake `check.cancel` Operation.

### T6. Restart

Persist queued cancellation, restart host with checks disabled. Initial demand snapshot starts checks worker and terminally closes the obligation without enabling normal checks.

## 7. Files and ownership

Modify in the same #53 implementation:

- `crates/swarm-kernel-host/src/store/tasks.rs::release` — retain current cancellation marker; no second settlement path;
- `crates/swarm-kernel-host/src/store/legacy_worker_demand.rs::snapshot` — exact obligation predicate;
- `crates/swarm-kernel-host/src/store/mod.rs::Store::call` — include `task.release` wake;
- focused Store/host worker tests.

R12/#38 owns generic scheduler source isolation, not this exact worker activation fact. R13/#39 owns capacity/resource ledger. R22/#48 owns requirement/check evidence. Do not move this correction into those PRs.

## 8. Deletion / non-goals

No new public DTO, table, cancellation status, direct terminal writer, background service or external donor.

Do not:

- settle CheckRun directly in `task.release`;
- enable all checks when config is disabled;
- spawn a worker merely because an ordinary check is queued;
- fabricate a `check.cancel` Operation;
- poll when the coordinator has no demand;
- add another watch channel;
- treat Store errors as false demand.

## 9. Minimal gate

After the complete #53 code slice:

```sh
cargo clippy --locked \
  -p swarm-kernel \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then exact public-path tests from §6. Broad workspace/native checks remain later.
