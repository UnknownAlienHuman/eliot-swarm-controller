# R05. Результат принадлежит ожидаемой Attempt, а не просто согласован сам с собой

**PR #31 · задание уточнено 7 октября 2026; код R05 ещё не изменён.**
Основание: AUD-004/023; production-снимок `40591a295af94b1541ec2ba30afe8e3247701a71`, прочитанный в ветке задания `3f8597480e18518cd027d6cecf860235248c068c`. Перед реализацией сверить текущий head PR. Исправлять в этой ветке, не создавать PR отдельно на helper и его подключение.

## Результат и первый шаг

Participant может читать/сдавать только результат своей ожидаемой Task revision + Attempt. Самосогласованная квитанция прошлой Attempt на том же binding не проходит. Внешние и вложенные поля общего dispatch receipt совпадают.

Начать с `authorize_participant_candidate` → Claude-ветка → `authorize_claude_result_candidate`. Сегодня после этой ветки стоит `continue`, а ожидаемая Attempt в helper не передаётся. **Не писать заново проверку Claude-протокола:** нужный sealed-origin reader уже существует.

## Читать только относящиеся разделы

- [Owner decisions](../../owner-decisions.md), §1.2–1.4: manager/worktree, exact candidate, поздние исторические данные, фаза проверки.
- [Modularity](../../agent-operations/modularity.md), §3, абзац normalized dispatch pair: Store вычисляет context, адаптер эхо-возвращает, Store сверяет с ожидаемой командой.
- [Module contract](../../agent_swarm.module-contract-v2.md), §4: результат, native admission и unknown outcome не являются принятием Task.
- [Источник submissions.rs](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-kernel-host/src/store/submissions.rs#L245-L580) — полный caller/helper/read-consumer, не одна строка раннего выхода.

## Существующие функции: использовать, а не дублировать

Пути ниже относительно `crates/`.

| Функция | Роль и требуемое изменение |
|---|---|
| `swarm-kernel-host/src/store/submissions.rs::candidate_for_attempt` | Проверяет полноту кандидата, source snapshot, normalized origin и применимый binding. Не превращать общую Manager/Operator policy в Participant policy. |
| `submissions.rs::authorize_participant_candidate` | Общий Participant gate. Передать expected Attempt в Claude helper; сохранить проверку каждой страницы и запрет повторяющихся page refs. |
| `submissions.rs::authorize_claude_result_candidate` | Уже сверяет SDK frame, artifact, source, result Operation. Добавить именно связь с expected Attempt. |
| `store/results.rs::load_claude_result_origin` | Уже читает sealed origin, пересчитывает original-request digest, сверяет dispatch admission, producer, descriptor и native identity. Переиспользовать возвращённый origin. |
| `results.rs::validate_sealed_module_receipt` / `validate_claude_assistant_result_source` | Сохранить проверки receipt и source/frame; новый scope guard их не заменяет. |
| `store/normalized_result.rs::validate_candidate_origin` | Существующий независимый normalized-result путь; не ослаблять и не перенаправлять в legacy helper. |
| `submissions.rs::authorize_participant_artifact_read` | Второй реальный consumer того же candidate gate. Поправка должна защищать чтение, а не только `task.submit`. |
| `swarm-contracts/src/runtime.rs::TaskDispatchAdmissionReceipt::validate` / `context` | Общая structural validation и получение context. В `validate` добавить три равенства outer/inner, без нового wire-типа. |

## Порядок реализации

### 1. Expected context передаётся от вызывающего

Из текущей, уже авторизованной Attempt получить непустые `task_id`, `attempt_id`, положительную `task_revision`. Не извлекать expected values из проверяемой страницы/receipt: это превратит проверку в тавтологию.

Изменить private helper так, чтобы он получал expected Attempt. Его нынешние `source` и `page_id` уже выводятся из `page.metadata["source"]` и `page.artifact_id`; убрать избыточный аргумент вместо нового подавления `too_many_arguments`. Возможный **новый, ещё не существующий** вариант сигнатуры: `(db, expected_attempt, page, result_operation_id, dispatch_operation_id, dispatch)`. Это локальный рефакторинг, не новый authorization framework.

### 2. Проверить принадлежность после существующей проверки origin

После `load_claude_result_origin` сравнить:

| Проверенное поле origin | Независимое ожидаемое поле Attempt |
|---|---|
| `target_task_id` | `task_id` |
| `target_attempt_id` | `attempt_id` |
| `target_task_revision` | `task_revision` |

Несовпадение — `CANDIDATE_SCOPE`. Ошибки чтения Store не преобразовывать в «не та Attempt». Далее сохранить действующие проверки sealed receipt, frame, page identity, digest, byte count и applied `agent.result`.

Важная граница: текущий код **специально допускает Claude-ветку без общей bound-only проверки**. Не копировать в неё условие «любая null binding запрещена» без проверки producer-контракта. Expected Task/Attempt/revision обязательны независимо от binding; явно назначенная пара binding/generation должна совпадать. Не считать session ID заменой Attempt.

Не требовать автоматически заполненных task/attempt колонок у всякой исторической `agent.result`: сначала проверить её настоящий admission writer. Для данной починки связь с рабочей попыткой уже доступна через проверенный target dispatch и `origin.target_*`. Исторический `admitted_claude_result_scope` не заменять live `runtime::scope`: это сломает сбор уже принятого результата после release/reconnect.

### 3. Замкнуть общую квитанцию

В `TaskDispatchAdmissionReceipt::validate` после существующих проверок добавить:

```text
outer.operation_id       == module_receipt.operation_id
outer.binding_id         == module_receipt.binding_id
outer.binding_generation == module_receipt.binding_generation
```

Эти поля уже существуют: формат JSON и schema ID не меняются. Это самосогласованность, **не полномочие**; Store всё равно сравнивает receipt с authenticated command и immutable context. Найти реальные constructors через `git grep -n 'TaskDispatchAdmissionReceipt' -- crates modules`; исправить затронутые producers в этом же PR, не создать vendor-копию валидатора.

### 4. Проверить обоих consumers и сузить diff

Проследить `task.submit` и `authorize_participant_artifact_read` до изменённого helper. Single-page и assembled candidates должны проверять каждую исходную страницу. Не менять acceptance policy, current-principal проверки и повторную сверку перед фиксацией submission. Обновить документацию результата только там, где раньше описывалась более слабая гарантия.

## Доноры и что не переносить

Главный донор здесь — **собственный** [sealed Claude origin](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-kernel-host/src/store/results.rs#L206-L410): почти вся проверка уже написана. Использовать результат этого reader, не строить ещё одну систему подписей или доказательств.

Принцип Goose «передавать проверенный вход целиком» применим как приём проектирования, не как библиотечная зависимость. Имя `Validated...` не доказывает проверку; важен constructor и вызов от настоящего caller. Дополнительный SDK, новый crate, таблица и изменение model/runtime конфигурации R05 не нужны.

## Критерии итогового кандидата — пока не выполнены

| Сценарий | Ожидаемый исход |
|---|---|
| Полный Claude page своей Attempt | Сохранены разрешённые чтение и submit. |
| Корректная старая Attempt, тот же binding/generation | `CANDIDATE_SCOPE` и на чтении, и на submit. |
| Другие Task/revision; assembled candidate с чужой страницей | Отказ до публикации submission; никакой частичной приёмки. |
| Отсутствующие expected IDs или malformed origin | Явная ошибка, не равенство двух null/default значений. |
| По одному изменены outer operation/binding/generation | `validate` отвергает каждое расхождение. |
| Весь receipt согласован, но относится другой команде | Внешний Store expected-command guard отвергает. |
| Normalized-result и законный исторический Claude readback | Нет регрессии из-за добавленной live-only проверки. |

Это сценарии будущей проверки, а не названия уже существующих тестов. На текущей фазе сначала закончить код; после интеграции менеджер выполняет scoped formatting и минимальный gate:

```sh
cargo clippy --locked -p swarm-contracts -p swarm-kernel-host --lib --bins -- -D warnings
```

Широкие tests/native — итоговая фаза. В сдаче: exact SHA, caller → helper → reader, diff общего validator, реальные результаты gate и остаток. Сам факт Markdown/зелёного docs CI не закрывает AUD-004/023.

## Владение и зависимости

Один manager/worktree; writers без Cargo. R05 владеет provenance в `submissions.rs` и receipt validation в contracts. R09/#35 — review replacement, R06/#32 — coordination context; их функций не переписывать. От R06 эта починка не зависит: expected Attempt уже есть у caller. Compiler fixes #26 не копировать; блокер проверки указывать по конкретной ошибке/пакету, не останавливать из-за него чтение и реализацию.
