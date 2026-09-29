# ELIOT Swarm — план реализации v6 к архитектуре v18

**29.09.2026. Проект контрактов и последовательности работ. Rust-сервис ещё не реализован.**

Заменяет implementation-v5 как действующий план. Входная v17 сохранена в review-v18/source-v17. [Контракт модулей](agent_swarm.module-contract-v2.md) — общая граница adapters; vendor mappings из v16 сохраняются. [Reference v18](agent_swarm.spec-v18/README.md) уточняет initial SQL: origin, canonical dispatch и resource release; [проверка всех семи native-контрактов](agent_swarm.runtime-contract-audit-v16-20260929.md) уточняет mappings. [Brief](MANAGER-BRIEF.md) остаётся источником reported deployment, не заменяет документацию vendor.

## 1. Решения не открываем заново

Один собственный Rust crate; Tokio; одна DB-thread с rusqlite; user-host; небольшой CLI/MCP; готовые native SDK за сменяемой границей. Muse Max и прямой OpenCode V2 — первые runtime. Нет UI, отдельного broker, workflow-языка, ORM, своего model loop, PKI или обязательного полного Eliot.

Модули checks/forge/reports/doctor первоначально обычные Rust-модули. Внешняя process-граница нужна native SDK, а не каждому файлу приложения. Один manager ведёт несколько Task через native детей. Main-only без Git worktrees сохраняется. Это архитектура прототипа, а не попытка перенести весь production Governor.

Изменения v18: origin-уникальность Task, однократная начальная доставка, dispatch guards, действующие настройки, resource release и ясная граница retention. Сохраняются native mappings v16 и девять таблиц. Новых scheduler/services/build frameworks нет.

## 2. Файлы одного crate

```text
src/main.rs, lib.rs          entrypoints и ручная композиция
src/model.rs                собственные типы, ошибки, состояния
src/config.rs               конфиг, immutable resolved routes, activation
src/api.rs                  общие методы CLI и MCP
src/ipc.rs                  framing, session handshake, reader/writer
src/store.rs                конкретные SQL-транзакции через DB-thread
src/scheduler.rs            due Operations и ресурсный admission
src/runtime/mod.rs          RuntimePort и реестр действующих instances
src/runtime/external.rs     bridge transport и reconciliation
src/runtime/opencode_v2.rs  direct HTTP, service discovery, события
src/checks.rs               один command runner + Cargo parser
src/forge.rs                Git/gh, readback и source export
src/doctor.rs                incident, диагностика, локальный recovery
src/reports.rs              короткие projections и artifact ranges
src/mcp.rs                  rmcp-фасад
src/platform/windows.rs    OS identity, pipes, запуск и Jobs
src/service.rs              позднее: тонкий SCM wrapper
modules/muse/              bridge + pinned SDK
modules/codex/             bridge + local Python env
modules/command/           mod + небольшой glue
migrations/001_core.sql
```

Не создавать пустые реализации будущих adapters. `RuntimePort` — один реальный seam. `Store` — конкретные методы, не `Repository<T>`. Компилируемые Rust-типы станут источником JSON-контракта; до их появления примеры — проектные данные, не второй SDK.

## 3. Сущности и их границы

| Сущность | Инвариант |
|---|---|
| Task | Одна текущая revision, phase, requirements и зависимости с требуемым результатом |
| Attempt | Неизменяемый task snapshot; один текущий owner Task до explicit release |
| Binding | Одна native generation root-линии; несколько Attempt; server lifecycle owner и connection owner разделены |
| ProducerRef | Root/child, связанный с Attempt через assignment и наблюдённую native identity |
| Operation | Одно сохранённое намерение и известный исход его доставки/исполнения |
| CheckRun | Один профиль на зафиксированных inputs, отдельный от worker |
| Artifact | Завершённый файл; ссылка не является свидетельством истинности содержимого |
| Incident | Один случай и его накопленные evidence, а не новая задача на каждое повторение |

Состояние Task — `open/accepted/archived`. Статус работы выводится из Attempt/check. Принятие относится к revision и phase; GitHub Issue автоматически не закрывается.

Attempt: `reserved → running → submitted → accepted`; после отклонённой сдачи `needs_correction → running/submitted`. `recovery_pending` — необходимость сверки, не смерть. `failed/cancelled/superseded` фиксируют известную диспозицию, не разрешают автоматически забыть возможные эффекты.

`released_at_ms` — отдельное решение. SQL unique index удерживает Task даже у accepted/failed Attempt, пока release не подтверждён. Не требуется закрывать детей, работающих над другими Task того же manager.

Зависимость содержит `task_id + required_revision + required_phase`; при назначении фиксируется точная использованная acceptance/candidate identity в task snapshot. Любой PR, промежуточный merge или acceptance другой фазы не удовлетворяет её молча. Изменение зависимой specification показывает необходимость перепланирования; не переписывает уже запущенный Attempt. Циклические группы GM объединяет/переформулирует, не требуется общий DAG-language.

TaskSpec.scope: `initial_paths`, `forbidden_paths`, `prerequisite_policy`.
При разрешении `owner_module_prerequisites` manager может дописать необходимый контракт/producer
в модуле владельца без новой Issue; проверяются пересечения, сохраняются пути/причина в assignment facts.
Цель/acceptance остаются неизменными. Изменение реального запрета — task.revise. Это заранее разрешённая
работа, не обход immutable snapshot. Docs bundle расширяется адресно.

## 4. Public API: убрать неоднозначность назначения

| Метод | Семантика |
|---|---|
| `agent.open` | Резервирует lane binding, запускает подготовку root через Operation. Возвращает handle, не ждёт работы модели. |
| `agent.attach` | Подключает явно указанную существующую root-сессию после сверки владельца; не создаёт новую при ошибке. |
| `task.create` | Сохраняет specification; imported root work дедуплицируется по разрешённому origin_key. Native effects отсутствуют. |
| `task.revise` | CAS expected_revision, полная новая specification и сброс текущей acceptance; сохраняет прежний snapshot/history, не запускает нового writer. |
| `task.claim` | Только резервирует Attempt. Менеджер может затем использовать native spawn; claim не отправляет второй prompt. |
| `task.dispatch` | Атомарно закрепляет единственный start_operation_id Attempt. На claimed Attempt тот же owner; повтор первоначального назначения возвращает прежний start, даже с новым client_request_id. |
| `attempt.bind_producer` | Связывает наблюдённого native child/root с Attempt; не запускает исполнение. |
| `agent.send/configure/reply` | Адресная команда конкретному binding; проверяется действительная native capability. |
| `task.submit` | Сохраняет immutable submission и candidate. Не освобождает производителя автоматически. |
| `task.accept` | CAS по revision/Attempt/candidate и требуемым checks; GM или разрешённая политика принимает phase. |
| `attempt.release` | Завершает Task-specific ownership после disposition производителя и unresolved effects. |
| `message.send/read/reply` | Durable directed mailbox; чтение не удаляет сообщение. |
| `check.run` | Сохраняет запрос; возвращает одинаковый активный CheckRun только своей Attempt либо новый. Cross-attempt reuse — только завершённый machine cache с новой привязкой. |
| `operation.get`, `agent.state/family` | Read-only. Status указывает наблюдаемую свежесть и gaps. |
| `report.delta`, `doctor.inspect` | Read-only default, не model calls и не vendor lifecycle. |

CLI и MCP используют один API. MCP не обязан публиковать каждую внутреннюю операцию отдельным tool: небольшие тематические tools могут вызывать типизированные методы. Не добавлять универсальный shell tool для обхода состояний.

Мутация получает `client_request_id`; `caller_id` — стабильный выданный host client principal, не link/PID и не доверенное поле тела. Повтор транспорта сохраняет principal/request ID. Отдельный вызов CLI без прежнего ID — новый запрос; domain origin/start uniqueness всё равно не позволяет создать дублирующую root Task/старт.

Роль клиента хранится в meta вместе с binding, а текущий GM — с отдельным epoch. Application проверяет права и при приёме, и при begin_send: manager работает со своими назначениями/children; submitter не вызывает accept/revise acceptance за себя; GM/local operator задаёт план, приёмку и административные изменения; observer читает. GM handover не меняет owner существующих manager Attempts и не запускает их заново. Очередные GM-only действия старого epoch удерживаются как stale до явного adopt/cancel, уже допущенные native эффекты сверяются. Нет отдельного RBAC/PKI-сервиса. При полном доступе к общему пользователю это механизм координации, не доказательство недоступности чужого credential через shell.

`task.dispatch` не означает создать новый manager. Root готовится один раз через open/attach. Нужные native children менеджер запускает сам. Повтор того же dispatch возвращает ту же Operation/Attempt, даже если ответ был потерян.

## 5. Хранилище и локальная идемпотентность

[001_core.sql](agent_swarm.spec-v18/migrations/001_core.sql) — новый reference DDL девяти таблиц. Runtime migrations ещё не реализованы. JSON shape и переходы проверяет Store; не делаем вторую state machine из SQL-триггеров.

`bindings` имеет ключ `(binding_id, generation)`: старое поколение не исчезает при замене процесса. `native_scope_key` вместе с `native_root_id` имеет partial unique index для unreleased rows: два lane aliases не получают второго control owner одной сессии. Namespace разрешает adapter по реальному runtime/store/account domain, не по lane/model/port/PID. Два поля либо NULL вместе, либо заданы вместе. Если exact identity недоступна, не обещать safe attach. Unique index `one_live_root_per_lane` включает `opening` и `reconciling`, не только ready. `attempts` имеет один unreleased owner на Task независимо от revision. NULL-pairs и accepted candidate проверяются явно.

Все durable mutations, включая create/claim/submit/reply/check/config, используют один `(caller_id, client_request_id)`. Нормализованный **исходный** JSON запроса сохраняется после проверки его формы, до разрешения alias/defaults. Нормализация сортирует object keys, сохраняет порядок arrays и содержимое строк; не меняет смысл отсутствующих полей по новым defaults. Для equality достаточно сравнения canonical bytes, не подписи/MAC.

Повтор: сначала lookup старого ключа, затем сравнение прежнего запроса. Совпало — прежний результат и прежний resolved route. Не совпало — `REQUEST_ID_CONFLICT`. Только новый запрос разрешает текущие aliases. Transport RPC id и logical request id различаются.

| Store method | Атомарный участок |
|---|---|
| `reserve_binding` | Idempotency → отсутствие другого unreleased root → binding opening + open Operation |
| `record_native_identity` | Scope/root identity → CAS текущего binding → unique native owner; collision не создаёт дубль и не останавливает чужую сессию |
| `reserve_attempt` | Idempotency → current revision/dependencies/owner/scope → Attempt; фиксирует dependency acceptance identity. Dispatch создаёт единственный start slot в той же транзакции. |
| `revise_task` | Idempotency/CAS → previous/new specification в истории → новая revision и cleared acceptance; живой Attempt не переписывается |
| `begin_send` | CAS queued/due → sending после current Task/owner/desired mode/authority/settings guards, prerequisite outcome и start slot; native ID сохранён; отправка только после COMMIT |
| `record_runtime_fact` | Dedupe настоящего source ID → факт + projection/operation update → commit |
| `record_submission` | Проверка owner/revision → ссылки candidate/submission + запрошенный CheckRun |
| `claim_check_resource` | Queued CheckRun + свободный resource_key → resource claim + Operation sending в одной транзакции, затем spawn |
| `record_check_result` | Exit/coverage/result artifact → CheckRun outcome; resource release только при известном disposition собственного Job, не по verdict |
| `accept_task` | Current revision + exact submitted candidate + актуальность dependency receipts + policy checks/decision owner → accepted reference |
| `release_attempt` | Task-specific producer disposition и отсутствие нерешённых мутаций → released_at_ms |
| `upsert_incident` | Один incident, обновление evidence/counter, не более одной совпавшей pending action |
| `record_publication` | Exact remote result/readback + сохранённый publication intent → applied fact; bookkeeping/cleanup отдельными Operations |
| `admit_scheduled_slot` | meta last slot + существующий ключ schedule/slot → не более одного queued Operation; пропуски объединяются |

Существование файла или пустой findings недостаточны для `accept_task`. Внешние code/SDK не пишут SQLite напрямую. После commit обновляется memory projection; если notification потерялось, projection перечитывает сохранённую revision.

Reference SQL обеспечивает cardinality/FK/shape, **но не** содержание approval, source completeness или независимость review. Например, соответствие `accepted_attempt_id` текущему Task/revision обязано проверяться Store; один внешний FK этого не доказывает.

### Origin и один начальный dispatch

`tasks.origin_key` — optional canonical identity только корневой импортированной работы; пример синтетический: `github:github.example:Issue:9042`. Forge resolver использует серверную идентичность и сохраняет aliases отдельно, не декодирует opaque IDs и не доверяет одному URL/номеру. Для импортированной Issue поле обязательно на уровне Application. Повтор с другим project alias возвращает существующую Task/сообщает конфликт binding, не создаёт дубль. Архивирование не освобождает origin; повторное открытие меняет прежнюю Task. Внутренние подзадачи имеют собственные IDs, source reference — не root identity. Автоматически угадывать semantic duplicate произвольных текстовых задач не нужно.

`attempts.start_operation_id` — единственная начальная доставка. Начальная отправка допускается только из reserved; если bind_producer уже установил native работу и running, initial dispatch отвергается, используются сообщения существующему producer. Транзакция reserve/dispatch проверяет этот slot. Первый caller создаёт start Operation и указатель, второй получает уже сохранённый start handle; его собственный request receipt — settled/coalesced и никогда не попадает к native dispatcher. Изменившийся target/payload при занятом slot — конфликт, не скрытая повторная выдача. Start slot не сбрасывается после возможного исполнения. После доказанного непринятия retry продолжает ту же Operation; после отмены/смены исполнения новый Attempt требует обычного release. Коррекции, ответы и goal controls не притворяются новым task.dispatch.

## 6. Dispatch: точная граница повтора

```text
queued → sending → native_accepted → settled
            └──────────→ outcome_unknown
queued → cancelled
queued/sending → rejected: только когда непринятие действительно известно
```

`settled` означает известный исход **этой команды**. `result_json` отдельно хранит success/failure/cancelled и evidence. Для correction settlement может означать native admission доставки, не исправление кода; для Task результат определяется сдачей и проверками.

Один dispatch lane на native target/order scope. Ready targets обходятся round-robin; quota/dependency waits не удерживают global permits. Штатный target key не lane alias, а разрешённая identity. После CAS только победитель посылает команду; ноль обновлённых строк означает «ничего не отправлять». DB-сохранение и network send не объявляются общей транзакцией. Crash после sending, но до ответа всегда требует reconciliation. Read-only lookup и ответы на уже принятые native вопросы при этом разрешены.

При наличии атомарного native initial-config+prompt используем его, не искусственную цепочку. Иначе многошаговая подготовка `open → model → effort → goal` не скрывается под одним повторяемым native request. Каждый изменяющий шаг получает отдельную Operation/native ID; `prerequisite_operation_id` задаёт короткую фиксированную последовательность. Это не пользовательский workflow engine. Каждая Operation хранит `completion_condition` в effective_request_json. ACK configure не выполняет условие native_applied: нужен его mapped outcome/readback. Settled failure/rejected/cancelled не удовлетворяет prerequisite. Только последний подготовленный шаг начинает model work. Ошибка effort не повторяет уже успешный open.

### Guard перед отправкой и граница revise

Admission сохраняет запрос, но не обещает бессрочно исполнить его после изменения работы. `begin_send` в одной DB transaction сверяет: unreleased текущий Attempt, его Task revision/phase, start slot для начального назначения, exact binding, desired admission mode, current actor permission/GM epoch для GM-only операций, необходимые dependency/setup facts. Cooperative independent work других roots не блокируется.

Новая информация может прийти между проверкой и socket write. Обещаем точку допуска на COMMIT `queued→sending`, а не невозможную атомарность SQLite+vendor. До неё revoke/revise/drain отменяет или отклоняет queued работу с явной причиной. После неё возможный эффект учитывается как sending/unknown и, если требуется, исправляется адресной отдельной командой. Native inbox также является уже допущенным эффектом; нельзя удалить местную запись и обещать retract без native подтверждения.

`UPDATE ... RETURNING` не является COMMIT. Statement вычитывается/закрывается; Transaction::commit завершается; только после этого выдаётся внутренний DispatchTicket. При commit error никакого native send нет; при неопределённом состоянии БД — recovery без повтора. Ticket — данные передачи, не ещё одна таблица. Пример SQL в spec ограничен task.dispatch; остальные methods проверяют свои scopes, не копируют его вслепую.

### Settings, которые ещё действуют

В `bindings.state_json` хранится revision наблюдённых effective settings. Setup-result содержит нужные значения и revision. Последующий configure того же scope делает старое доказательство устаревшим для нового запуска; успешный прежний prerequisite не решает эту гонку. Использовать per-turn native options там, где возможно. Иначе per-binding prepare→input admission является короткой эксклюзивной последовательностью для конфликтующих configure. Read/reply/steer без изменения этих settings обслуживаются независимо. Если runtime читает настройку на каждом model step, несовместимое configure ждёт подходящую native-границу; нельзя отпустить barrier лишь по ACK и обещать изоляцию всех будущих calls. Out-of-band изменения фиксируются при наличии telemetry; защиту от полноправного внешнего клиента не заявляем.

Native terminal может опередить response: запись факта идёт сразу по доступным IDs, дальнейший admission ack только добавляет ссылки. Settled не откатывается в running. Если ID ещё нельзя сопоставить, событие сохраняется как unmatched evidence и разрешается после ответа; по одному названию модели его не приписывают.

Отмена queued выигрывает тем же CAS против begin_send. После отправки cancellation — отдельная адресная Operation. Потеря клиента/ожидающего future не является native cancel. Неопределённый исход запрещает лишь несовместимое продолжение данного target, не чтение и не весь рой.

Повтор одного Operation не начинает новый Attempt. Новый Attempt появляется при осознанной смене исполнения/передаче с sealed handoff, а не на каждый timeout. Остальные независимые задачи линии продолжаются.

## 7. IPC и владение связью

Named Pipe byte-stream, JSON-RPC 2.0, UTF-8, одно JSON-сообщение на строку. Логи идут отдельно. Own RPC ids — непустые строки; native IDs остаются в SDK. Own batch RPC не нужен первой версии.

Один постоянный reader и один writer на соединение. Не делать `read_line()` внутри отменяемого `select!`: возможна потеря частично прочитанной строки. Выбран `FramedRead + LinesCodec::new_with_max_length` из tokio-util; writer сериализует frame целиком и не бросает половину frame при timeout. После транспортной ошибки закрывается link и запускается сверка, не SDK [N1–N3].

Own limit относится к metadata-frame, не native token/context/output. Большое тело — artifact handle с range-read. Невалидный/oversized frame фиксируется как protocol gap; ошибка не означает успешный пустой ответ и не приводит к убийству vendor. Decoder limit не ограничивает encoder автоматически — размер исходящего frame также проверяется.

`mpsc::send(Ok)` не durable ack. Перегруженный pre-admission endpoint возвращает busy либо ждёт место без потери исходного request. После DB commit работа остаётся в outbox, даже если уведомление worker потерялось. Command queue не хранится вторым authoritative экземпляром в SDK [N2].

### Идентичность

| Поле | Когда меняется |
|---|---|
| `controller_id` | Только новый data-dir/новый prototype instance |
| `host_epoch` | Каждый успешный старт единственного host |
| `module_instance_id` | Зарезервированный host экземпляр bridge; OS-lock не допускает его двойной запуск |
| `bridge_boot_id` | Каждый настоящий запуск bridge-процесса |
| `link_epoch` | Новое принятое IPC-соединение этого bridge; старое отключается |
| `(binding_id, generation)` | Новый native owner; обычный IPC reconnect generation не меняет |

Сначала OS singleton по каноническому data-dir, затем БД и увеличение host_epoch. Discovery-файл/PID — подсказки, не замена OS-lock и process creation identity. Alive lock не удаляется по возрасту. `module.hello` проверяет ожидаемый instance/artifact и сообщает native bindings. Host возвращает новые link credentials/epoch в рамках same-user IPC, без PKI.

Один link владеет отправкой control на bridge. Команды старой link epoch, ещё не допущенные bridge, отклоняются. Уже принятая native работа не отменяется задним числом из-за смены host. Bridge не соединяется с двумя управляющими hosts одновременно.

До завершения сверки binding — reconciling; новые несовместимые mutations не допускаются. Native reader и ответы, уже разрешённые сохранённым profile, продолжаются. Неизвестный содержательный вопрос хранится как pending: автоматический approve-all не выбирает за пользователя смысл ответа.

Bridge stdin не держится родительским host как life support. SDK.close вызывается только по явному lifecycle. Host crash может быть пережит bridge; bridge crash может потребовать native resume. Это разные failure boundaries, не обещание абсолютной непрерывности.

Падение bridge не доказывает смерть его native child. Replacement bridge сначала работает в recovery-режиме и сверяет прежний process/session identity; constructor не должен немедленно запускать второго SDK owner. Если прежний stdio-owner жив, но недоступен для attach, состояние остаётся unknown и выбирается явный disposition. Только OS-lock bridge от такого orphan не защищает.

## 8. События и snapshot: не откатывать состояние назад

У каждого факта есть source stream, native generation и source cursor/event ID, если его действительно выдаёт backend. Нельзя использовать content hash одинакового текста как identity события. Для собственного link bridge назначает монотонный module_seq в рамках bridge_boot_id; это локальный порядок, не доказательство native causality.

Snapshot указывает scope, `complete/partial/unavailable`, native cursor или интервал `module_seq_at_start/end`. Частичная/недоступная выборка не удаляет старых членов. Complete list active sessions означает полноту **active list**, не полноту всех детей: отсутствие в ней не доказывает terminal. Требуется snapshot нужной области либо отдельный native факт о судьбе ребёнка.

При чтении snapshot продолжают приниматься live events. На backend с cursor snapshot+replay применяется по native порядку. Без такого контракта adapter сохраняет события, пришедшие за окно чтения, и не затирает изменённые ими сущности поздним snapshot. Неоднозначные сущности остаются partial/unknown до адресной сверки. Не изобретать atomic snapshot у API, который его не обещает.

Terminal того же turn не возвращается в working из старого snapshot. Новое turn/continuation identity — другое исполнение, а не запрещённый регресс. История native children и их terminal dispositions сохраняется для завершения Task, даже если список активных детей стал пустым.

При отключении host bridge держит known operation outcomes до ACK **после DB commit**, pending native questions и latest state. Repeated telemetry можно объединить. При переполнении журнала промежуточных фактов ставится явный gap; не хранится весь transcript в RAM. Неподтверждённые command outcomes не вытесняются из ограниченного количества admitted operations. Недостающую историю нельзя позднее выдать за полное покрытие аудита.

## 9. Task acceptance, producer release и main-only

Сдача — immutable candidate/submission. Проверки могут идти, пока manager работает над другими Issue. Принимается только candidate, соответствующий текущей task revision и нужной phase. Новая сдача создаёт новый reference, а не меняет старый artifact.

Release требует Task-specific закрытия assignment и разрешения возможных эффектов. Для child это наблюдённый terminal/cancel disposition именно его задания. Для root, ведущего несколько Task, — явное закрытие assignment менеджером плюс отсутствие относящихся pending операций; не требуем terminal всей root. Это cooperative evidence, а не OS-доказательство, что full-access агент физически не сможет позже изменить файл. При неясной связи удерживается только спорная работа/область.

Acceptance dependency проверяется снова в `accept_task`, не только в reserve_attempt. Если её отозвали, результат consumer получает needs_revalidation по конкретной причине: существующий diff и check сохраняются, писатель не перезапускается автоматически. Более новая producer revision сама по себе не уничтожает старую принятую revision: допустимость pinned dependency берётся из сохранённой истории/политики Task. Результат, требующий именно новой версии, должен иметь соответствующую requirement revision. Никакого blanket cascade revoke всех уже принятых consumers. Минимальная materialized карта dependencies строится из текущих Task, не новый DAG-store.

Правка strategy не меняет specification. Новое требование — новая revision; прежняя acceptance не наследуется молча. Обновление Task с живым Attempt не запускает второго writer: сначала disposition старой работы, затем новое назначение. Уже пригодные artifacts сохраняются.

В общем main атомарность Git index не решает конфликты редактирования. Сериализуются общие интерфейсы, индекс/commit/publish и короткая согласованная граница подготовки candidate. Участники остальных независимых Task не останавливаются. Никто не делает `git add .` над чужими dirty changes; при смешанном patch scope решает manager, не эвристическое разрезание кода.

Canonical workspace/path identity разрешает реальные пути и aliases до сравнения; casing и reparse/junction особенности локализованы в Windows helper. String-prefix без разделителя не определяет вложенность. Это защита от случайного двойного назначения, не файловая песочница и не guard на каждое чтение.

Family completeness относится к transport/подписке: root-only feed общего server не становится
complete-family. В `state_json` хранится provenance/freshness источников; отсутствие SUBAGENTS-file
или ошибка его parsing даёт unknown. Historical roots/children сверяются при attach/startup, без
создания новых allowed duplicate bindings. `turn_context` может дать native-resolved model конкретного
ребёнка; заголовок родителя и requested route остаются отдельными сведениями.

## 10. Checks, artifacts, миграции

CheckSpec: explicit executable/argv/cwd/env, candidate/source manifest, toolchain, features/targets, profile revision, parser и cache policy. Сохраняется Operation до запуска процесса. Один активный check на `(attempt_id, cache_key)`; один владелец resource_key target-dir до explicit release. Отдельные Attempts не делят один исполняемый CheckRun: это убирает необходимость generic subscriber/cancel machinery. Повтор своей проверки возвращает handle, а disconnect waiter не отменяет job. Для уже законченного пригодного machine cache допускается новый собственный CheckRun с result_ref/cached_from и заново проверенными acceptance requirements. Semantic verdict чужой Task по совпадению source digest не переносится.

`resource_claimed_at_ms/resource_released_at_ms` независимы от state результата. Atomic claim до spawn исключает второго writer; launch-unknown оставляет claim до reconciliation. Failed/error/incomplete могут удерживать ресурс. `release_check_resource` требует known failed-before-spawn либо проверенного отсутствия owned job members; верхний PID/EOF/timeout и отсутствие уведомления Job не достаточны. Не ждём Job handle как универсальный all-processes-ended сигнал: используем accounting/process identity по Windows contract. Explicit kill-on-close для собственных checks не распространяется на native manager. `passed` разрешён только после завершения исполнения, обработки output и release. При недоступном OS evidence держим один спорный ресурс, не весь пул [N10].

Cache разрешён только для объявленных воспроизводимых проверок при совпавших существенных inputs. Наличие network/time/external state без их версии выключает cache reuse, но не запрещает запуск диагностики. Новый profile revision меняет cache key; повтор старого запроса использует старый resolved profile.

Changed+reverse compile scope вычисляется из versioned metadata для source/profile. `--no-deps`
возвращает `resolve=null`; допустима conservative workspace declaration closure с корректными
rename/path/build/target edges. Unknown graph не равен пустому. Shared config/codegen inputs расширяют
scope. Неоднозначность → широкий разрешённый lib/bin профиль или явно diagnostic результат.
Recheck остаётся на warm target-directory, но получает новый exact candidate/check identity.

Source export берётся из exact commit, не из движущегося checkout. Submodules/LFS/untracked/config внешних сборок учитываются явно. Если check может переписать свои inputs, проверяется соответствие исходному manifest после прогона; иначе результат diagnostic/incomplete. Formatter, изменивший код, создаёт новый candidate. Не вводим глобальный CAS/build system или snapshots на каждый tool call. Export должен воспроизводить
выбранную wrapper/config policy: иначе перенос из каталога с локальным sccache override меняет сборку.
RUSTC_WRAPPER и RUSTC_WORKSPACE_WRAPPER учитываются раздельно; env задаётся реальному Cargo, не proxy.

Publication: persist intent → remote send/readback → record_publication → finalize/cleanup/notify.
Фактический merge/push и Task acceptance — разные результаты. После сбоя cleanup не запускать повторно
writer/merge. Post-actions имеют собственные IDs и повторяются отдельно. Не создавать общий transaction
между GitHub и SQLite. Exact PR head не фиксирует base; при неразрешённом drift нужен действующий
recheck профиль. Legacy CLAIM/PUSHED не управляют новой ownership БД.

Result хранит exit code, process disposition, parser status, coverage, findings и output references. Exit 0 с parser failure или недостающим обязательным output не PASS. Non-JSON строки Cargo допустимы и сохраняются; JSON распознаётся как объявленный Cargo message, а не каждая строка принудительно парсится.

После exit дренируются streams. Если унаследованный потомком pipe не закрывается, после transport drain timeout фиксируется output incomplete; это не успешное завершение и не причина убивать чужую агентную семью. Политика завершения дерева check задаётся отдельно от долгоживущего native backend.

Artifacts: own temporary file → завершить запись/flush → publish в новое неизменяемое имя без перезаписи → DB reference. Повтор имени допустим только для тех же проверенных байтов. Store принимает собственные artifact handles, не произвольный путь с выходом из artifact root. Это порядок снижения риска, не общая атомарная транзакция filesystem+SQLite. Перед использованием load-bearing artifact проверяется его доступность и digest, когда digest входит в идентичность candidate/cache. Потеря файла после crash даёт evidence gap, не fabricated PASS. Evidence, snapshot submissions и Operation receipts сохраняются; FK не видит ссылки в JSON. В C01–C09 не реализуем generic graph-GC: автоматический retention только у явно отмеченной disposable telemetry и закрытых tmp, с grace и проверкой активного writer. Request-id records не удаляются вместе с payload логами: их потеря снова разрешила бы исполнить старый запрос. Для live проекта сохраняются compact identity/outcome и исходный нормализованный запрос; закрытый проект архивируется явно. Новому data-dir не разрешён автоматический replay старых request IDs. Ни секреты, ни полный унаследованный env не копируются в эти records: persisted routes содержат credential refs и не секретные влияющие настройки.

SQLite `foreign_keys=ON` задаётся до транзакции и проверяется readback. WAL/FULL — для prototype-owned локальной БД, не для чужого OpenCode/codebase-memory. Проверяется фактически связанная SQLite с исправлением WAL-reset; библиотека host ещё не выбрана/собрана [N4–N5].

Backup — SQLite backup API, не копия одного живого `.db` без WAL. Миграция выполняется единственным host до рабочего admission, после backup. Бинарник не открывает более новую неизвестную schema. Rollback binary разрешён при совместимой schema; откат старой DB при живых bridges запрещён без reconciliation сохранённых внешних эффектов. Не строим универсальный автоматический downgrade [N6].

## 11. Конкретные обязанности семи runtime-модулей

Норматив этой редакции — [аудит контрактов](agent_swarm.runtime-contract-audit-v16-20260929.md) и его sources. Это список нужных adapter mappings, не обязательный обход всех функций каждого CLI при старте. Scope C01–C06 и девять таблиц остаются прежними.

| Срез / файл | Что написать | Проверка именно этой границы |
|---|---|---|
| C03 `modules/muse/vendor_bridge` | Публичные SDK Connection/spawn, commandId, один pump, настройки до goal; reply, view cursor и child provenance | Правильные admission/outcome, opaque replay gap, model/effort readback; SDK.close не на конце turn |
| C04 `runtime/opencode_v2.rs` | One HTTP reader/service; inbox send, конкретный model variant, parent-filtered pages; event gap recovery | Active drains не вся семья; unknown schema не empty list; no CLI recovery; background только supported tools |
| C07 `modules/codex/vendor_bridge` | Existing shared WebSocket attach, типы версии server; root/child/goal и ответы RPC | JSONL SDK не подходит proxy без transport adaptation; read не resume; sourceKinds/family filters по installed schema |
| C08 `modules/claude` | Native stream + minimum hooks; complete child messages и content blocks | Не терять block с повторным message.id; init failures отдельно; system-prompt snapshot не объявлять обновлённым при resume |
| C08 `modules/command` | Native mod readiness, queueMessage, events, set tools/model | Void queue не durable applied; mod_error не process death; definition refresh не mod reload; actual tool argument schema |
| C08 `modules/antigravity` | Native event codec, warm sequential input, result/step_update, conversation ID | Claude control messages не отправляются; soft-denied tool не пропадает при SUCCESS; child idle ≠ released |
| C11 `runtime/zed` | Batch executor и result artifacts с явной границей | Нет выдуманных live goal/steer; native Zed не external ACP editor и не Delta |

Все mappings используют один RuntimePort. Runtime-specific параметры живут в модуле и сохранённом route, не разносятся по scheduler/Task. Функция, не нужная текущему заданию, не становится обязательным gate.

### Транспорт Codex: не оставлять несовместимость на усмотрение следующего агента

Pinned Python CodexClient читает/пишет JSONL stdio; это подтверждённая реализация, а не неизвестность. У выбранного пользователем shared server proxy — WebSocket поверх stdio. Прямая замена argv **не работает как транспортная интеграция**.

C07 использует готовый WebSocket transport в одном внешнем bridge. SDK routing/types переиспользуются там, где позволяют интерфейсы; transport adaptation проверяется отдельно. Не импортировать codex-core в Rust-host, не писать RFC6455, не запускать новый постоянный общий proxy service. Owned JSONL app-server остаётся отдельным явно выбранным режимом, не fallback при ошибке attach. Общий `.codex` и server lifecycle не принадлежат bridge.

### Configuration activation и наблюдения

`describe` хранит entrypoint/version + конкретные функции. `configure` возвращает actual application boundary и supported readback, а не только «файл записан». Vendor options выбираются из native catalogue, не из общего enum low/high/max. Неизвестные optional fields не блокируют остальные операции.

Native event ID трактуется на правильном уровне (event/item/block). Token delta без child ID не приписывается детям. Inventory запроса содержит фильтры и page completeness. Error/read failure не очищает прежний known state. `resume` и `respawn` нельзя вызывать из read-only doctor, когда они могут загрузить/запустить работу.

Уже имеющийся RuntimeCommand может нести native background action; scope берётся из модуля. Это не generic process detach. Переход на alternative native supervisor/ACP runtime выполняется только новым route/binding.

`RuntimeObservation` usage payload содержит basis/scope/units/observed timestamp. Reports обновляет cumulative snapshot или sum неперекрывающихся deltas; не суммирует всё подряд и не объявляет estimate балансом подписки. Источник quota/meter указан, отсутствие не превращается в ноль.

### GM wake

C05 сначала использует выбранный native input. Claude Channels — документированная opt-in MCP-extension для GM, не обязательный новый plugin всем workers. Generic rmcp notification не считается wake. Если entrypoint не имеет подтверждённого push, явно checkpoint mode без скрытых model polls. Native background supervisor Claude не принимается автоматически: отдельная workspace/cost/respawn/family qualification.

### Windows и доноры

Не менять source pins автоматически. Source/SDK package/native binary — разные версии. Код SDK сохраняется целой пригодной единицей с notices, изменения transport локальны. Named Pipe собственного bridge остаётся byte-stream NDJSON; vendor протокол это не меняет. Python Proactor не является сам по себе готовым pipe-client API, `multiprocessing.connection` не raw NDJSON.

LaunchProfile сохраняет `console_policy`, `server_lifecycle`, explicit stdio/Job/env. Политика current shared server из brief не мигрируется на другой launcher без проверки. Ни UAC/ACL repair, ни global PATH, ни новая compaction не входят в doctor. PATH/Path объединяются case-insensitively; cwd/env/account относятся к настоящему executor. Delta terminal shell setting не настраивает agent shell; у каждого runtime свой путь.

### Версионированная граница модуля

`RuntimePort` и [контракт v2](agent_swarm.module-contract-v2.md) не импортируют vendor types.
Восемь методов остаются прежними; `native.*` регистрируется одним адаптером с input/effect/outcome
контрактом, не становится untyped universal shell. Required capabilities задаются ролью, а не
жёстким требованием всех функций ко всем routes. Batch route не изображает long-running manager.

Module delivery: whole pinned SDK/crate + small vendor_bridge + fixtures + UPDATE.md + notices.
Новый model alias не требует rebuild; новый протокол меняет adapter. Manifest не имеет права
выдать documented capability за observed. Unknown optional fields не отключают backend целиком;
сломанный mandatory mapping делает недоступной только затронутую операцию. Existing stable routes
не меняются ради необязательной новой функции. Живые bindings остаются на своих artifact revisions.

## 12. Нагрузка, отчёты и recovery без дополнительной модели

У каждой external connection независимые reader/writer, у каждого target — dispatch lane. Reader не ждёт места в telemetry-очереди или reporter; reply-required/control имеет отдельный быстрый путь. Ограниченная порция приоритетных сообщений не должна навсегда вытеснять обычную ready-команду. Ограничения памяти не являются лимитами работы агента. Status читается из общей projection. Большие exports/parsing/hash не исполняются на DB-thread. `spawn_blocking` только для короткой конечной работы под отдельным ресурсным допуском; abort запущенного closure не является остановкой. Long SDK readers остаются своими threads/processes. В памяти хранятся active state и bounded queues, не вся история разговоров.

DB очередь обслуживает короткие транзакции. Если после commit не удалось разбудить dispatcher, он подберёт durable queued запись следующим проходом. Drain queue и deadlines не запускают отдельный polling process на каждого child.

Периодические технические interval используют явно `Skip`/`Delay`, не default `Burst`. Wall-clock due хранится для restart, локальное ожидание — monotonic. После sleep/clock jump сначала reconciliation, затем один актуальный catch-up; срок сам по себе не делает старую отправку безопасной для повтора. Это existing scheduler, не новая service.

Read-only `report.delta(after)` и `message.read(after)` не продвигают общий cursor. Ответ возвращает next_cursor; каждый consumer хранит свой. Delivery/ack/use/полезность — разные факты. Expired cursor обозначается явно, без молчаливого пропуска истории. GM change сохраняет pending decisions; старый GM теряет право новых команд через штатный API, но не отменяет уже допущенную работу.

Automatic nudge отправляется только при новой полезной причине, actual target и разрешённой native boundary. Incident cooldown устраняет повторы; возраст alone не запускает kill/reassign. Native goal исключает параллельный таймер «продолжай». Unknown квота не превращается в ноль и не останавливает независимые accounts. Условие исключительной
остановки ребёнка по политике владельца проверяет и отсутствие ответа, и продвижения; mtime недостаточно.
Drain сохраняет запрет новых assignments до сообщений, учитывает native goal и прежние root families.
Own check Job и общий native server имеют разные правила отмены.

Reports/maintenance/task refresh — existing scheduler: meta хранит schedule/last admitted slot,
Operation использует устойчивый schedule+slot key. После reboot сначала сверка активных operations,
затем один актуальный catch-up; не burst всех пропусков. Ошибка части report сохраняет остальные
разделы с partial marker. No second cron service, no LLM для механической пересылки.

При отказе DB host прекращает новый side-effect admission, но сообщает деградацию и не уничтожает живые native sessions. Bridge с разрешённым static profile продолжает обслуживать protocol requests; содержательные вопросы ждут доступного manager. Сервис не должен имитировать сохранение, если commit не выполнен.

200 observed agents / 1 000 small events per second / p95 host <100 ms — прежние **цели измерения**, не benchmark. Счётчики host, bridges, native runtimes, Cargo и model quota измеряются отдельно.

## 13. C01–C11: законченные возможности, не каркас

| Пакет | Результат | Проверяемая граница |
|---|---|---|
| C01 | model/config/Store + initial 001 schema v18 | create→origin/claim/start slot→один native dispatch; guards/reconnect/confликты ID |
| C02 | host/CLI/IPC | singleton, durable ack/status, reader/writer, disconnect клиента |
| C03 | Muse SDK bridge | Max/setup, family, reply/steer, reconnect без SDK.close |
| C04 | OpenCode V2 | та же API-семантика, volatile events+snapshot, без CLI observer |
| C05 | mailbox/reports/GM wake | адресный вопрос/ответ; независимые cursors и delta |
| C06 | command-check/Cargo | fixed candidate, dedupe, error coverage, возврат владельцу |
| C07 | Codex shared attach | proxy/WebSocket, goal/turn/family, readback модели/effort, версия actual executor |
| C08 | Claude/Command/agy | по одному native adapter, честные возможности |
| C09 | forge/doctor/artifacts | remote applied отдельно от bookkeeping, narrow audit, safe recovery |
| C10 | updates/logon/SCM | versioned modules, console/Job profile, persistent schedule catch-up и drain |
| C11 | qualification/export | нагрузка и реальные incidents; Zed/ACP по готовности |

Донор fixtures сохраняются. При реализации — сначала рабочий путь, форматирование и минимальный Clippy; широкие тесты после готового среза. Никаких worktrees для собственной разработки. Нельзя отмечать C01 готовым только по существованию reference SQL: требуются реальные Store и API.

### Обязательные ситуации перед допуском соответствующей линии

Повтор request с тем же/другим payload; crash до/после send; terminal до ack; stale snapshot при новом event; parent idle с живым child; late reply от прошлой link; reconnect без нового native owner; смена model alias при старом request; сдача и release отдельной Task при других детях; parse error check с exit 0; остановка observer без остановки executor.

Дополнительно по новому brief: parent-конец при старом child; root-only feed; model metadata родителя
против child turn_context; пробная command/MCP цепочка без окон; completed remote publication с упавшим
cleanup; пропущенный schedule после reboot; metadata без resolve; fixed-source build с правильным
sccache override. Проверяется конкретный модуль при его допуске, не весь стек на C01.

Это сценарии проверки реализованного пути, не требование сначала построить большой test harness. Где нет native cursor/API, отсутствие доказательства остаётся видимым integration gap.

## 14. Область проверки этой редакции

v18 проверяет собственные v17/plan-v5/module-v1 и конкретные reference SQL-сценарии. Не повторный общий поиск vendor-платформ. В DDL по-прежнему девять таблиц; добавлены origin_key, start_operation_id, resource claim/release. Runtime migrations отсутствуют: это изменение initial reference ещё не реализованного продукта.

SQL и небольшие последовательные модели проверены отдельно; их результаты не являются тестами многопоточного Rust-host, Windows Job, native SDK или API провайдера. Нет установки доноров, модельных вызовов, изменений GitHub/текущего роя. Пины сохранены. [Review v18](agent_swarm.design-review-v18-20260929.md) содержит конкретные исходные места и воспроизведения.

## Источники

Исторические основания и координаты brief: [review v15](agent_swarm.brief-review-v15-20260929.md), W1–W10.
Сохраняемые технические основания:

[N1] Tokio AsyncBufReadExt: https://docs.rs/tokio/latest/tokio/io/trait.AsyncBufReadExt.html

[N2] Tokio mpsc Sender: https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html

[N3] tokio-util LinesCodec: https://docs.rs/tokio-util/latest/tokio_util/codec/struct.LinesCodec.html

[N4] SQLite WAL: https://www.sqlite.org/wal.html

[N5] SQLite foreign_keys: https://www.sqlite.org/foreignkeys.html

[N6] SQLite Backup API: https://www.sqlite.org/backup.html

[N7] Microsoft Named Pipe Security: https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-security-and-access-rights

Ссылки — источники контрактов, не подтверждение installed versions и не обязательное чтение всех
материалов каждым агентом. Scope C01–C06 не расширен новой платформой; уточнены уже требуемые методы.

[N8] [Контрактный аудит семи harness](agent_swarm.runtime-contract-audit-v16-20260929.md), [реестр](agent_swarm.runtime-sources-v16.json), [матрица](agent_swarm.runtime-matrix-v16.json). Новые версии UI/docs не объявляются свойствами установленного CLI.

[N9] [Контракт модулей v2](agent_swarm.module-contract-v2.md), technical sources T1–T5 и [проверка v18](agent_swarm.design-review-v18-20260929.md).

[N10] Проверенные технические основания v18: [SQLite RETURNING](https://www.sqlite.org/lang_returning.html) — строка результата ещё не commit; [Windows Jobs](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects) — process group, accounting и ограничения notifications; [SQLite FK](https://www.sqlite.org/foreignkeys.html) — отношения только объявленных columns; [GitHub node identities](https://docs.github.com/en/graphql/guides/using-global-node-ids) — object lookup, не display names. Прочитано 29.09.2026.
