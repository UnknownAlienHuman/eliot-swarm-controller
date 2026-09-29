# ELIOT Swarm — проверка собственных дыр v18

**29.09.2026. Объект: architecture v17, implementation-v5, module-contract-v1 и их reference DDL.**
Это проверка ещё не реализованного проекта. Ниже различаются воспроизведённые свойства SQL, недоопределённые контракты и предлагаемые исправления. Это не список обнаруженных аварий работающего сервиса.

## Результат

Девять содержательных находок. Изменена initial reference-схема; семь native harness и donor pins не переаттестованы и не заменены. Новых постоянных процессов, таблиц, брокеров или модельных контроллеров нет. Затраты реализации локализованы в Store, scheduler, CheckRunner и существующей границе RuntimePort.

| ID | Найдено | Основание | Исправление |
|---|---|---|---|
| H18-01 | Разные Task IDs одной внешней Issue обходят ownership | SQL-контрпример | `tasks.origin_key`, unique на canonical imported root work |
| H18-02 | Старый queued prompt может пережить revise/drain | Недостающий dispatch guard | Повторная проверка на COMMIT `queued→sending`, не лишь при приёме |
| H18-03 | `incomplete` освобождает target-dir при неизвестных процессах | SQL-контрпример | Resource claim/release независимо от verdict |
| H18-04 | Global CheckRun dedupe и единственный attempt owner не согласованы | SQL + API-модель | Active dedupe только внутри Attempt; completed cache связывается заново |
| H18-05 | Старое applied Max допускает запуск после смены на high | Последовательный контрпример | Native per-turn settings либо короткий prepare/admission barrier |
| H18-06 | Dependency проверена при запуске, но позднее отозвана | Контрактная щель | Pinned dependency acceptance и адресная revalidation до приёмки |
| H18-07 | Retention удаляет idempotency/evidence, скрытые в JSON | SQL подтверждает отсутствие FK-связи | Простая политика хранения evidence; автоудаление только disposable |
| H18-08 | Reconnect identity и полномочия GM описаны декларативно | Недоопределённость Application | Stable principal + meta GM epoch + небольшая проверка ролей |
| H18-09 | Новый request ID повторяет начальную доставку claimed Attempt | Контрактная щель | Единственный `Attempt.start_operation_id`, не только request-id dedupe |

Исходные материалы сохранены в [review-v18/source-v17](review-v18/source-v17/agent_swarm.md). [SQL-воспроизведения](review-v18/sql-counterexamples.json), [проверки reference-переходов](review-v18/transition-checks.json) и исполняемые локальные scripts входят в комплект.

## H18-01. Уникальный owner не спасает от двух Task одной Issue

**Вход:** DDL v17 `tasks` уникализирует только `task_id`. `one_owner_per_task` проверяет `attempts.task_id`. Application `task.create` не определяет dedupe внешней root work.

**Контрпример:** два импортёра создают Task A и Task B для одной и той же Issue. У каждого свой UUID, затем свой Attempt. Оба ограничения выполняются, два писателя получают одну работу. В in-memory SQLite v17 обе записи приняты.

**v18:** `origin_key` разрешается forge adapter по идентичности объекта в authority domain; uniqueness не включает изменяемый project alias или phase. Внешнюю ссылку нельзя считать canonical identity по одному номеру `#42`. Повтор импорта возвращает существующую Task, изменение текста становится новой revision. Для обычных внутренних задач origin nullable; подзадачи с отдельной целью могут ссылаться на тот же источник без копирования root identity. Архивирование не освобождает origin.

**Граница:** индекс не угадывает семантическую одинаковость текста и не проверяет корректность ключа вместо adapter. Source-derived IDs и перенос/alias объекта сверяются по данным forge; не декодируем opaque IDs вручную. GitHub документирует object ID отдельно от display URL [R1].

## H18-02. Приём до изменения задачи не равен праву исполнить её после изменения

**Вход:** implementation-v5 §5: `reserve_attempt` проверяет current revision/dependencies/owner, `begin_send` перечисляет queued/due, binding, prerequisite. Явного повторного guard Task/owner/drain/authority перед отправкой нет.

**Контрпример:** O ставится в очередь при Task r1. Пока backend занят, Task становится r2 либо линия получает drain. O продолжает ссылаться на тот же live binding и успешно законченный setup. Проверки из прежней строки begin_send недостаточны, чтобы не послать старое задание.

**v18:** проверка кратких актуальных данных в той же транзакции, которая переводит O в sending. Только после её COMMIT выдаётся право native-send. До этой точки revoke выигрывает; после неё требуется считать эффект возможным и разрешать его исход. Native inbox/ACK не откатываются удалением местного Operation.

[Reference SQL](agent_swarm.spec-v18/transactions/begin-initial-send.reference.sql) показывает только initial dispatch. Authority, settings, dependency policy и типизированный prerequisite проверяются Store в той же транзакции. Replies и результаты уже выполняющейся работы не попадают под blanket new-work block.

**Дополнительное уточнение:** SQLite RETURNING выдаёт изменённую строку до COMMIT внешней транзакции. Воспроизведено: получить строку, ROLLBACK, увидеть queued. Native вызов внутри обработки RETURNING до commit был бы новым нарушением той же границы [R2].

## H18-03. Законченный отчёт проверки не доказывает освобождения процессов

**Вход:** `one_check_writer_per_resource` в v17 включал только `running/reconciling`. `incomplete/error/failed` из него исключались.

**Контрпример:** проверка получила incomplete после сбоя чтения/закрытия pipe, судьба её потомков неизвестна. Изменение verdict снимает индексную защиту, и следующий CheckRun занимает тот же target-dir. В SQL v17 этот переход и второй writer действительно разрешены; реальный процесс для воспроизведения не запускался.

**v18:** отдельные `resource_claimed_at_ms/resource_released_at_ms`. Unique живёт до release, а не до любого verdict. Claim фиксируется вместе с допуском spawn; неизвестный launch/result не освобождает его. Known failed-before-spawn освобождается явно. `passed` требует release, complete output и остальных условий, не только exit 0.

В Windows нужно учитывать принадлежащую job семью, не только верхний PID. Microsoft также предупреждает, что часть Job notifications не гарантирована; отсутствие события не доказательство отсутствия процесса. Job handle не универсальный сигнал обычного завершения всех процессов [R3]. Чужой native shared server этому lifecycle не подчиняется.

**Граница:** исправлен reference-index. Код наблюдения Windows Job и вызов release ещё не написаны. Удерживается только спорный resource_key; другие проверки не замораживаются.

## H18-04. Дедупликация не должна присваивать чужой CheckRun

**Вход:** v17 API обещает вернуть существующую одинаковую активную проверку. Индекс глобален по cache_key, но CheckRun имеет ровно один `attempt_id` и одну execution Operation. Кто принимает результат и отменяет shared job, не определено.

**Воспроизведение:** в v17 второй Attempt с тем же cache_key не может получить собственную active CheckRun row. Это подтверждает несогласованность модели, но не доказывает, что несуществующий Store уже выдавал ложный PASS.

**v18 — более простое решение:** active reuse только `(attempt_id, cache_key)`. Cross-attempt running-job sharing не входит в первый прототип: не добавляем subscribers, reference-counted cancellation и очередной журнал интересов. Cargo target остаётся warm и сериализованным; повтор своей проверки не запускает compiler заново.

Завершённый воспроизводимый machine cache можно применить с новым CheckRun binding/cached_from для нового Attempt, после проверки точных входов и его требований. Semantic review одной Task не переносится по совпадению SHA на другую. Отключение waiter не отменяет общий для своей Attempt job.

## H18-05. Applied-настройка может уже не действовать

**Вход:** module-contract-v1 §6 требует дождаться успешного configure перед стартом. Он защищает ACK→applied, но не последующее изменение настроек.

```text
A: configure Max → applied
B: configure high → applied
A: send полного задания → runtime использует high
```

Все прежние prerequisites A выполнены. Это последовательный контрпример к достаточности этих guards, не live-замер Muse.

**v18:** per-turn options используются там, где их поддерживает native contract. Иначе существующая per-target сериализация держит короткий prepare/admission barrier для операций, меняющих или использующих те же session-wide defaults. В `binding.state_json` достаточно revision/effective values и владельца подготовки. Никакой глобальной блокировки на всю модельную работу.

Если runtime применяет settings динамически к будущим model calls, adapter не может заявить изоляцию лишь по первому ACK: конфликтующее configure переносится на допустимую native-границу. Reply/read и независимые roots продолжаются. После out-of-band изменения учитываем доступные факты; внешнего full-access клиента эта кооперация физически не запрещает.

## H18-06. Разблокированная зависимость может утратить основание

**Вход:** required revision/phase описаны, reserve_attempt проверяет readiness. В `accept_task` нет самостоятельной явной проверки использованного dependency acceptance.

**Контрпример:** A принята, B начинает использовать A. Затем A опровергнута независимой проверкой. Собственная revision B не изменилась; старого ready-факта недостаточно для её приёмки.

**v18:** Attempt фиксирует exact dependency acceptance/candidate в snapshot. Перед приёмкой B проверяется её действительность. Revoked A создаёт адресную revalidation; B не убивается, её код/диагностика не выбрасываются.

Не всякое появление A r2 отзывает корректную A r1. Pinned r1 разрешается по сохранённой истории, пока её evidence не отозвано и требования B допускают r1. Новые требования ко всей системе принимаются новой revision, а не незаметным cascade rewrite всех старых результатов. Отдельный dependency engine не добавлен.

## H18-07. Обычная очистка может стереть смысл durable state

Две разные проблемы:

- Удалить старую завершённую Operation — значит удалить её уникальный request ID. Следующая доставка с тем же ID может снова пройти как новая.
- Исторические artifact refs находятся также внутри observations/result JSON. Foreign key не разбирает JSON и не удерживает такой файл/row. SQL-воспроизведение удаляет artifact, несмотря на ссылку внутри payload.

**v18:** до явного архивирования проекта сохраняются компактные idempotency receipts, input identity и evidence. Автоочистка только явно disposable telemetry и закрытых tmp, не generic «нет FK значит мусор». В C01–C09 не создаём универсальный граф сборки мусора. Истёкший/архивный запрос не должен молча переисполняться; новый namespace/проект имеет явную границу.

Секретные credentials и разрешённый процессу env не сериализуются целиком в durable request: сохраняются refs и не секретные влияющие настройки, значения разрешаются перед запуском. Это не обещание автоматически обнаружить любой секрет в произвольном пользовательском prompt.

## H18-08. Stable caller и GM authority не следуют из Windows SID

**Вход:** v17 пишет «caller выводится из клиента» и «старый GM теряет права», но не связывает эти положения с reconnect/idempotency и Application methods.

Если считать client равным link/PID, повтор после reconnect меняет idempotency scope. Если считать всех клиентов одним Windows SID, исчезает различие случайной команды writer и решения GM.

**v18:** небольшой host-issued client principal, стабильный в reconnect; role/binding и GM epoch в meta. Application проверяет уже существующие методы по фиксированной таблице. GM-epoch относится к GM-only решениям, не отменяет разрешённую работу менеджеров линий. Непринятые устаревшие решения нового GM не исполняются автоматически; допускается явное adopt либо cancel.

Никакого отдельного auth-server, signing ceremony и ограничения shell пользователя. При полном доступе это защита штатных путей от ошибок, не изоляция враждебных процессов одного пользователя.

## H18-09. Request-id dedupe не равен однократному старту задания

**Вход:** API разрешает task.dispatch на уже claimed Attempt того же owner. С уникальным `(caller_id, client_request_id)` два разных ID всё ещё означают две принятые команды.

**Контрпример:** GM сделал dispatch, получил timeout, новая CLI-команда с новым UUID опять указывает claimed Attempt. Task owner по-прежнему один, но prompts могут быть два. Это не опровергает уже описанный безопасный transport retry: это другой путь повторного запроса.

**v18:** `attempts.start_operation_id` присваивается одной транзакцией. При существующем initial start повтор возвращает его handle; новый request имеет coalesced receipt и не dispatchable. Иное initial payload/target — явный конфликт. `begin_send` сверяет start slot. Уже связанная native producer-работа не получает начальный prompt повторно. Follow-ups идут как сообщения, не как task.dispatch.

Дополнительный SQL пример допускает только canonical start, даже когда в fixture существуют две Operation одного Attempt. Это не перенос полного Store в SQL и не запрет менеджеру создавать несколько разных Task.

## Что не считаем новой находкой

В v17 уже явно покрыты parent idle с живыми детьми; late terminal/ack; partial snapshots; возможный native admission при timeout; bridge disconnect; отсутствие live steer у batch; scope приёмки и отдельный Task release. Эти положения сохранены, а не переписаны как ещё девять новых «критических дыр».

Полная независимая проверка произвольного кода по символам, universal crash-free SDK и реальная вместимость сотен моделей по подписке не доказаны. Они не становятся обещаниями новой документации.

## Статус комплекта и проверок

При монтировании отдельные checkpoint и donor metadata снова оказались старыми (v14 и v12), тогда как полный ZIP был v17. Архитектура/plan/module совпали с ZIP. Обе версии metadata сохранены; обновление опирается на пакет v17. Это проблема согласованности входных материалов, не новый runtime-дефект.

Выполнены reference DDL/counterexample проверки в Python SQLite 3.46.1, узкий SQL begin_send и последовательные модели правил. Их точный перечень — [validation](agent_swarm.spec-v18/validation-results.json). Нет Rust Store/API, Windows IPC/Jobs, подключений SDK, реальных потоков/процессов и model calls. Initial `user_version=1` остаётся будущим номером первой схемы; старые reference DB не мигрируются автоматически.

Изменения записаны в [архитектуру v18](agent_swarm.md), [план v6](agent_swarm.implementation-v6.md) и [контракт модулей v2](agent_swarm.module-contract-v2.md). Первый кодовый срез по-прежнему C01→C02→Muse→OpenCode V2.

## Источники, перечитанные в этом проходе

[R1] GitHub — object IDs и direct lookup: https://docs.github.com/en/graphql/guides/using-global-node-ids

[R2] SQLite RETURNING, §2.3 ACID Changes: https://www.sqlite.org/lang_returning.html

[R3] Microsoft Job Objects, accounting/notifications/handle lifetime: https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects

[R4] SQLite partial indexes — membership определяется WHERE: https://www.sqlite.org/partialindex.html

[R5] SQLite FK — объявленные отношения columns: https://www.sqlite.org/foreignkeys.html

[R6] Tokio Notify 1.53.1 — notification не queue counter: https://docs.rs/tokio/1.53.1/tokio/sync/struct.Notify.html

Источники прочитаны 29.09.2026. Наши corrections — проектные решения, не утверждения о новых ошибках vendor harness.
