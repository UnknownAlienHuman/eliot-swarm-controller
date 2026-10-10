# R01. Модули: согласованная identity, restart и готовность одного boot

**PR #27 · задание уточнено 7 октября 2026; код R01 ещё не изменён.**
Основание: AUD-005/037/038/041; production-снимок `40591a295af94b1541ec2ba30afe8e3247701a71`, прочитан в ветке задания `4d9d75df181bb69f4200a37841d120790dcbb1b2`. Ниже указан существующий код и конкретные изменения; предлагаемые новые типы не выдаются за готовые API.

## Результат и первый шаг

Установленный модуль не зависит от сохранности build-cache. Супервизор различает проверенного прежнего владельца и нового helper, проверяет worker после появления нужных receipts и сохраняет ProcessRunning после same-boot hello.

Начать с пары `swarm-process::process_birth_identity` / `supervisor::identity_from_process_image`, затем пройти `run_lifecycle` → `wait_for_prior_owner_to_depart` → `monitor_owner_helper`. Не начинать с нового actor-framework или переписывания всего supervisor.

## Читать адресно

- [Owner decisions](../../owner-decisions.md), §1.2–1.4 и §2.2: manager/worktree, этап проверки, live work и retention.
- [Modularity](../../agent-operations/modularity.md), §2–3: process ownership, boot/handshake и самостоятельные пакеты.
- [Module contract](../../agent_swarm.module-contract-v2.md), §2–4: lifecycle и unknown effect.
- [Module installer](../../../tools/modules/README.md), descriptor-last publication, install receipt и Handoff and limits: source provenance отличается от installed artifact.

## Существующие функции и ответственность

Пути относительно `crates/`.

| Функции | Что использовать / менять |
|---|---|
| `swarm-process/src/process_group.rs::{process_birth_identity,process_image_identity}` | Producer OS evidence. Сохранить точные типы/поля платформы; ошибки доступа не превращать в exited. |
| `swarm-supervisor/src/supervisor.rs::{capture_identity,identity_from_process_image,process_identity_is_live}` | Согласовать retained projection с live birth. Сохранить capture-before/image/capture-after и отдельную проверку image. |
| `wait_for_prior_owner_to_depart`, `clear_prior_helper_result`, `run_lifecycle` | Передать доказанную prior-owner identity в новый цикл; не удалить нужный checkpoint owner. |
| `monitor_owner_helper`, `validate_worker_receipt`, `validate_launch_result` | Разделить прочитанный и проверенный receipt; одна согласованная логика обычного poll и final read после exit. |
| `confirm_module_hello`, `update_status` | Guarded same-boot transition без lost update и обратного перехода Running → Starting от liveness-poll. |
| `swarm-process/src/module_owner.rs::run_module_with_resolver` | Реальный writer owner/worker receipts и gate checkpoint recovery. Читать вместе с monitor, а не менять consumer отдельно. |
| `swarm-supervisor/src/descriptor.rs::load_installed_descriptor` | Удалить зависимость runtime-загрузки от существования source EXE; установленный artifact продолжать проверять. |

## 1. Согласовать birth identity, не подменить её текущим PID

`identity_from_process_image` вручную строит birth JSON. В Windows-ветке теряется `platform`, тогда как live birth и его consumers используют это поле. Image receipt — другая форма и сам platform не обязан содержать. Не добавлять поле произвольно во все wire-сообщения.

Сделать один маленький проверенный constructor/projection для соответствующей платформенной birth-формы в `swarm-process` и подключить producer/consumer. Если потребуется новый `birth_identity_from_image`, это **новая private/shared функция**, а не существующий API; не заводить отдельный crate. Сохранить представление `creation_filetime` и остальных birth-полей, не сводить числа/строки через defaults.

Недопустимое сокращение: вместо проверки retained receipt просто вызвать `capture_identity(pid)` и принять новый результат. PID мог быть переиспользован; новая identity не доказывает старую. Сохранить `module_child_belongs_to_owner`, image digest/path и exact descriptor/boot checks.

## 2. Передать доказанный prior owner через restart

`wait_for_prior_owner_to_depart` сегодня возвращает `()`, теряя уже проверенный receipt. Изменить внутренний результат так, чтобы он переносил точный prior owner + token после `departed_empty`; для действительно свежего состояния — None. Возможный новый private тип `VerifiedPriorOwner` обозначает именно пройденную проверку, не произвольный JSON.

Передать его через `run_lifecycle` в monitor. Пока новый helper ещё не опубликовал owner.json, точное совпадение с этим старым receipt — ожидание, а не identity mismatch и не доказательство нового владельца. Иной owner остаётся ошибкой. После принятия нового receipt возврат к старому не разрешён.

Не удалять старый `owner.json`: `run_module_with_resolver` требует его для checkpoint recovery. Сохранять существующий порядок проверки departure и удаления только прежних worker/launch-result записей. Timeout технической фазы не разрешает новый spawn/kill; helper handle и uncertain evidence должны оставаться у живого владельца.

## 3. Прочитанный worker ещё не проверенный worker

Дополнительная source-level гонка в том же AUD-037:

```text
monitor читает owner.json → файла ещё нет
helper публикует owner.json, затем worker.json
monitor читает worker.json → сохраняет Some(record), owner в памяти ещё None
следующий poll получает owner, но worker_record.is_none() уже false
→ validate_worker_receipt не вызывается; worker_identity остаётся None
```

Это возможно при правильном порядке записи helper; повреждение файла не требуется. Сценарий пока не исполнялся.

Отделить cached raw receipt от validated identity. Как только есть проверенный новый owner и raw worker, выполнить `validate_worker_receipt`, даже если worker прочитан в прошлом poll. Обновить **обе** ветки: обычный poll и final read после `helper.try_wait`. Не устранить гонку путём принятия raw worker без проверки. Аналогично сохранить взаимное исключение worker-started и certified pre-spawn failure для одного boot.

## 4. Готовность меняется транзакцией над текущим status

Заменить clone → mutate → `send_replace` в `update_status` на изменение текущего значения через Tokio watch. Для безусловной публикации подходит существующий `send_modify`; для подавления одинаковых значений — `send_if_modified`. Один transport-primitive не заменяет таблицу допустимых переходов.

| Текущие факты | Переход |
|---|---|
| Worker live, hello ещё не подтверждён | Starting; alive не равно protocol ready. |
| ProcessRunning того же boot и exact worker | Обычный live-poll сохраняет Running. |
| Hello для другого boot/worker либо после несовместимого terminal transition | Отказ без изменения status/readback flags. |
| Helper exited, family всё ещё существует | OwnerGroupRetained/unknown; не Completed и не разрешение replacement. |
| Certified pre-spawn failure и departure проверены | Различать NotStarted и смерть уже запущенного adapter. |

OS/file I/O выполнять вне watch write-lock. После I/O повторно сравнить boot/worker и допустимое текущее состояние **внутри** обновления. Согласовать `readback_required` с реально принятым переходом; не сбрасывать atomic flag до проверки, которая может отказать. Независимое параллельное изменение другого поля не теряется.

## 5. Установленная версия независима от source build-cache

В `load_installed_descriptor` убрать повторное открытие/хэширование `receipt.source_file` как условие загрузки. Сохранить историческое значение поля и нужную проверку его формата без требования, что старый файл всё ещё существует.

Не убирать `checked_file_under_root`, descriptor validation, actual installed EXE hash, descriptor hash, согласованность module/artifact/build и source/staged/installed digests внутри receipt. Receipt unsigned и не становится trust root. `checked_existing_file` удалять только после проверки остальных callers. Исправить installer README: source проверяется при установке, installed artifact — при загрузке.

## Доноры и подводные камни API

[Tokio watch 1.53.1, Sender](https://docs.rs/tokio/1.53.1/tokio/sync/watch/struct.Sender.html) уже соответствует используемому механизму. `send_if_modified` меняет значение на месте; **возврат false не откатывает выполненные мутации**, а лишь подавляет notification. Поэтому guard должен стоять до изменения, а результат closure отражать действительное изменение. Не удерживать `watch::Ref` через await. Watch остаётся volatile status, не новым долговечным доказательством.

Для publication/recovery смотреть существующие `write_launch_attempt` и writer в `module_owner.rs`, не переносить новый журнал из чужого runtime. Ractor/ACP не требуются: добавление actor tree не исправляет неправильную receipt identity и может принести несовместимое kill-on-drop поведение.

## Критерии итоговой квалификации — ещё не выполнены

| Сценарий | Требуемый результат |
|---|---|
| Windows/Linux producer → receipt → live comparison | Совпадает одна incarnation; чужие birth/image/family не принимаются. |
| Old owner наблюдается до нового | Ожидание только для exact verified prior receipt; неизвестный owner отвергается. |
| Worker попал в cache до owner | После появления owner worker проверяется, в том числе на final exit read. |
| Hello → несколько polls, конкурентное изменение status | Running сохранён для того же boot; другие поля не затёрты. |
| Поздний чужой hello / helper с живыми descendants | Нет ложной readiness, completion или replacement. |
| Source EXE удалён/пересобран, installed EXE неизменен | Загрузка installed версии работает; изменение installed bytes отвергается. |

Всё это один PR: нельзя поставить constructor без consumers или новую очередь без исправленного monitor. Один manager/worktree, writers без Cargo. После кода — scoped formatting и минимальный gate:

```sh
cargo clippy --locked -p swarm-supervisor -p swarm-process --lib --bins -- -D warnings
```

Tests/native/load — итоговая фаза. Сдача: exact SHA, обе стороны receipt, переходы status, реально выполненный gate и остаток. R02/#28 владеет NativeOwner внутри OpenCode, R14/#40 — frontend extraction; их функции не переносить сюда. #26 не копировать; проверять реальные зависимости gate, не объявлять все задачи заблокированными красным main.
