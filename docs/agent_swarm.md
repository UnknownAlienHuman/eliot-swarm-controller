# ELIOT Swarm Prototype — архитектура v18

**29 сентября 2026. Проект реализации. Rust-сервис ещё не написан.**

Headless Rust-контроллер над родными executors. Назначение — устойчиво обслуживать реальный рой, а проверенные модули и результаты перенести в Eliot Memory OS. Не новый model harness и не универсальная агентная платформа.

**Действующий комплект:** эта архитектура → [контракт модулей](agent_swarm.module-contract-v2.md) → [план реализации v6](agent_swarm.implementation-v6.md) → [reference DDL/примеры](agent_swarm.spec-v18/README.md). [Checkpoint](agent_swarm.checkpoint.md) фиксирует фактическую готовность. [Результаты v18](agent_swarm.design-review-v18-20260929.md) — журнал проверки, не дополнительные правила для workers.

Native факты из [v16](agent_swarm.runtime-contract-audit-v16-20260929.md) сохраняются с прежними источниками и границами. Последнее исследование harness принято как [отдельный источник идей](agent_swarm.harness-intake-20260929.md); его GUI/phone/лимиты не переопределяют наши требования.

## 1. Что делает систему универсальной

**Универсальны ответственность и результат операции; внутренности harness остаются родными.**

GM решает, кто и что делает. Контроллер назначает, доставляет, наблюдает, запускает проверки и возвращает результаты. Менеджеры линий используют native subagents. Harness владеет model loop, контекстом, tools, авторизацией и способом reasoning.

Не расходуем модель на heartbeat, таймер, пересылку stderr и подсчёт детей. Не согласовываем каждое чтение/правку/обсуждение. Полные разрешения сохраняются; контроллер не изображает файловую песочницу. Не подменяем Muse Code Max, бесплатный OpenCode или подписочный agy более удобным API-маршрутом.

Качество измеряется принятым результатом, переделками и затратами, не числом prompts/PR/строк. Старый timestamp не доказывает смерть. Неизвестный исход нельзя повторять вслепую. Наблюдатель не убивает исполнителя ради зелёного индикатора.

**Добавляем только механизм с конкретным отказом, владельцем и проверяемым результатом.** Не требуются ещё один broker, workflow-язык, PKI, свой inference proxy и LLM-модератор. Инциденты из brief — требования к устойчивости, а не повод загрузить всем агентам историю аварий [S1, S17].

## 2. Физическая система

```text
General Manager ─ swarm MCP ─┐
                             ├─ local IPC → Rust host → runtime modules
Managers/scripts ─ swarm CLI┘               │              │
                                            │         native subagents
                                     SQLite / mailbox
                                     checks / GitHub
                                     doctor / reports

Windows service wrapper → health host; не task DB и не второй scheduler.
```

Один собственный crate, library + binary `swarm.exe`, одна SQLite, каталог артефактов. Один Tokio runtime и DB-thread. CLI/MCP вызывают одни application methods, не открывают рабочую БД. Закрытие MCP/GM не отменяет принятую работу.

Native SDK может требовать Node/Python. Это локальная зависимость конкретного runtime-модуля; не установка в глобальную среду и не основание переписывать SDK на Rust. Первый запуск — обычный user-host; logon task и тонкий SCM wrapper добавляются к работающему пути [S12].

Первый полезный срез: Muse Max + OpenCode V2, задания, native family, вопросы/коррекции, сдача, простой command-check и отчёт GM. Codex, Claude, Command, agy, GitHub/Cargo/recovery следуют на том же контракте. Zed native/ACP остаются нужными направлениями, но не блокируют первый запуск.

## 3. Модуль — граница изменения, не обязательно процесс

| Модуль | Первая реализация | Владелец |
|---|---|---|
| Core/API/Store | Rust внутри host | Task/Attempt/Operation и каноническое состояние прототипа |
| OpenCode V2 | Native HTTP adapter | Наш клиент существующего сервиса, не lifecycle server |
| Muse | Node bridge + whole official SDK | Native MSP connection и его subprocess |
| Codex | Bridge настоящего native transport | Клиент shared server; owned stdio — другой профиль |
| Claude/Command/agy | Native bridge/mod | Только выбранное исполнение и возможности |
| Checks/forge/quality/doctor/reports | Внутренние Rust-модули | Каждый своя функция, без лишних постоянных демонов |

Core зависит от `RuntimePort`, adapter — от SDK. Vendor types, tool names и особенности протокола не проникают в scheduler/Store. Не вводим `if harness == ...` в общем контроллере. Внутри adapter такие различия нормальны.

Восемь базовых операций: `describe/open/attach/snapshot/send/configure/reply/shutdown`. Редкая функция регистрируется в `native.*` конкретного модуля с input schema и семантикой эффекта; не становится неограниченным JSON passthrough и не раздувает MCP-каталог. Точный контракт — [module v2](agent_swarm.module-contract-v2.md).

Внешняя граница обязательна там, где она изолирует SDK/обновление. Встроенные отчёты не нуждаются в IPC сами с собой. Нет Rust DLL, DI framework, generic `Repository<T>` или десятков пустых adapter traits.

На старте один bridge может обслуживать одну root-линию; children не получают наши отдельные процессы. Контракт допускает несколько bindings на module instance, когда реальный SDK это позволяет. Универсальный pool соединений заранее не пишется.

## 4. Владение: Task, native-сессия и процесс не одно и то же

| Объект | Для чего |
|---|---|
| Task | Цель, revision, phase, требования и зависимости |
| Attempt | Исполнение зафиксированной Task с одним текущим владельцем |
| Binding | Native root/route/generation; может выполнять несколько Attempt |
| ProducerRef | Реальный root/child, связанный с конкретным assignment |
| Operation | Сохранённое намерение и наблюдённый исход |
| CheckRun | Проверка конкретного кандидата и профиля |

Manager вправе вести несколько Issue. Принятие одной не ждёт всех посторонних детей; замена самой root требует сверки её семьи и continuation. Child→Attempt связывается assignment и native identity, не названием процесса или текстовым сходством. Пока связи нет, нельзя выдумывать disposition конкретного producer.

**Импортированная корневая Issue имеет один `origin_key`, независимо от project alias и локального Task ID.** Повтор импорта возвращает существующий Task, не создаёт второго writer. Ключ разрешается по идентичности объекта forge; отображаемый URL/номер без repository scope не годится. Подзадачи с другими целями создаются отдельно и ссылаются на исходную Issue, не копируют её root identity.

**Две независимые проверки уникальности:** один live root на lane и один control owner на `native_scope_key + native_root_id`. Второе закрывает подключение той же сессии через разные aliases/линии. Scope разрешает реальное пространство native IDs, не модель, порт, PID или label. Read-only consumers используют существующий binding, не создают нового владельца.

Проверка и закрепление native identity идут в Store-транзакции до дальнейших управляющих действий. Она защищает прототип от своих дублей, не запрещает чужому full-access клиенту открыть ту же беседу. При конфликте с внешним владельцем нужна сверка, не силовой takeover.

`owned_native`: bridge/SDK владеет своим subprocess. `external_attach`: bridge владеет соединением/proxy, не общим server. `shutdown` не превращается в безусловное «убить дерево линии».

Native pipes, созданные SDK, принадлежат SDK. Поэтому bridge долгоживущий и подключается к host по отдельному IPC; disconnect не вызывает `SDK.close()`. Он продолжает принятую работу, держит незаквитированные исходы в пределах admitted window и затем сверяется. Native history используется там, где доступна. Падение самого bridge может требовать resume; бесшовность после logoff/reboot не обещается.

Replacement bridge сначала проверяет прежний native owner. Constructor не запускает второй subprocess вслепую. Batch/legacy stdio допускается с честно более слабой границей отказа [S2–S3, S24].

## 5. Состояние и транзакции

Девять таблиц: `meta/tasks/attempts/bindings/operations/observations/artifacts/check_runs/incidents`. Native payload — JSON в существующих records. Не таблица на каждое SDK-событие и не второй event store.

Task: `open/accepted/archived`. Revision и принятие изменяются CAS. Attempt хранит прежний immutable snapshot; правка Task не запускает нового writer и не переписывает прошлое доказательство. Рабочее состояние выводится из Attempt/CheckRun, не дублируется в трёх mutable полях.

```text
reserved → running → submitted → accepted
                    ↖ needs_correction
          ↘ recovery_pending / failed / cancelled / superseded
```

`released_at_ms` отдельно от terminal/accepted: возможные task-specific эффекты разрешаются до передачи владения. Accepted относится к точной phase/revision/candidate, не закрывает GitHub Issue автоматически. Зависимость указывает нужную revision/phase; любой merge не удовлетворяет её молча.

`task.claim` только закрепляет. `task.dispatch` также сохраняет доставку. `Attempt.start_operation_id` назначается один раз: новый request ID не разрешает повторить первоначальный prompt той же Attempt. Совпадающее назначение возвращает первоначальный handle; дополнение — `agent.send`, смена исполнения — новая Attempt после disposition. `agent.open/attach` готовит root отдельно. Повтор client request сравнивается с исходным нормализованным JSON **до** нового разрешения alias/defaults; возвращает прежний результат и route. Другой payload под тем же ID — conflict.

Одна `rusqlite::Connection` на DB-thread; короткие reads/writes, никаких HTTP/Git/LLM в транзакции. Статус читается из общей memory projection. WAL/FULL, foreign_keys включены до transaction; выбирается bundled SQLite с требуемым исправлением WAL-reset [S7–S8, S16].

Новый [reference DDL](agent_swarm.spec-v18/migrations/001_core.sql) добавляет origin identity, initial-dispatch pointer и отдельное владение ресурсом CheckRun. Active-check dedupe ограничен одной Attempt. Девять таблиц сохранены. Это поправка initial schema ещё не реализованного продукта, не миграция рабочей БД. Подробности Store/C01 — в [плане v6](agent_swarm.implementation-v6.md).

## 6. Шина и управление нагрузкой

Команды/ответы, необходимые факты, telemetry и большие outputs обрабатываются раздельно. Внутри — `mpsc/oneshot/watch`; снаружи — local JSON-RPC/UTF-8 frames. Один постоянный reader и writer, готовый codec; native transport разбирается собственным SDK, не нашим JSONL [S6, S14–S15].

Reader не ждёт окончания модели, аудитора, большого экспорта или заполненной telemetry-очереди. Иначе reply-required запрос provider останется за шумом. Ответы/control получают приоритет, но обычные команды не голодают. Большие outputs идут по handles/ranges.

Отправка несовместимых команд последовательна в их order scope; ожидание полного model turn не занимает этот слот. `send().await` в Tokio не означает durable admission. Команда принята после DB commit; потерянный внутренний сигнал dispatcher восстанавливается из outbox.

Ready targets обходятся round-robin, ожидающие quota/dependency targets не занимают global permit. Cargo, тяжёлые hash/export и native starts имеют разные ресурсные очереди. Не нужны weighted scheduler и дополнительный broker до измеренной потребности.

Перегрузка откладывает новые starts/scans/optional audits. Принятые команды не вытесняются; telemetry может coalesce с явным gap. Размеры лимитируют память служебной шины, не контекст модели, число её действий или длину Issue.

`spawn_blocking` — для короткой ограниченной работы. Бесконечный SDK reader и ожидание долгого процесса не прячутся туда: начатый blocking closure не отменяется через abort [S24].

## 7. Доставка и готовность без ложного success

```text
queued → sending → native_accepted → settled
            └────────→ outcome_unknown
queued → cancelled
```

`begin_send` — CAS; только победитель отправляет. DB commit и native send не являются одной транзакцией. Timeout после возможного admission даёт unknown, а не повтор. `retryable` недостаточно: повтор mutation разрешается по доказанному непринятию или реальной native-idempotency со старым ID.

Adapter сохраняет небольшой `OperationContract` в effective request: область/порядок, нужный смысл завершения, replay policy, использованный fallback и revision контракта. Без собственного workflow-языка.

**ACK настройки не открывает запуск, пока не выполнено её условие применения.** Если configure требует `native_applied`, последующий goal ждёт соответствующего факта. Проверка prerequisites требует успешного типизированного результата, а не одного `state=settled`. Noop годится только при правильном effective setting.

Где backend умеет initial settings+prompt атомарно, используем это. Где нет — короткая сохранённая подготовка `open → configure → observed → start`; шаги имеют собственные IDs и не повторяют успешный open при ошибке effort.

Terminal может прийти до admission-response. Он сохраняется по доступным IDs; поздний ACK добавляет связи, не откатывает known terminal. Неопознанное событие остаётся unmatched до корректной correlation, не назначается по бренду модели.

Отмена queued соревнуется CAS с отправкой. Отмена принятого turn — новая адресная Operation. Disconnect клиента или abort локального future не являются отменой native-работы. Unknown удерживает лишь несовместимое продолжение этой работы, не весь рой.

Перед `queued → sending` Store заново проверяет актуальные Task revision/owner, допуск новых заданий, exact binding и существенные settings, а не только факт прежнего admission. Это точка допуска native-send; она возвращает право отправки **после commit**, не после первой строки SQL RETURNING. Если revise/drain победил раньше — отправки нет. Если send уже допущен — его результат выясняется обычным reconciliation; изменение записи не отменяет внешний эффект [S26].

Применённая настройка не вечная: `configure(max) → configure(high) → send` не должно запускать Max-задачу на high. Предпочтительны native per-turn options. Иначе короткая последовательность prepare→native admission защищена от конкурирующих configure в том же scope; изменение позже идёт на разрешённой native-границе. Reader/reply и независимые сессии не блокируются. Внешние полноправные клиенты остаются границей гарантии, а не оправданием выдуманного applied.

## 8. Наблюдение и восстановление

Независимые оси: connection, execution, family, goal и проверенный результат. Не создаём enum всех сочетаний. PID жив — только process fact; parent idle с живыми детьми — не свободная линия.

Snapshot имеет scope/generation/completeness и cursor либо окно чтения. Partial/unavailable не удаляет прежние сущности. Complete active list не доказывает terminal отсутствующего child. Поздний snapshot не перетирает новые live-изменения; без native atomic snapshot остаются явно неоднозначные сущности.

Host epoch, bridge boot, link epoch и native generation меняются по разным причинам. IPC reconnect не новая сессия. Старый link не допускает новых команд; уже принятая работа не отменяется задним числом. Startup сверяет и прежние незакрытые семьи, не только последний лог.

Event identity определяется native контрактом: один message ID может иметь несколько content blocks. Dedupe по строке текста или общему ID может терять tools. Usage имеет delta/cumulative/window, scope и единицы; root totals и дети не складываются повторно. Estimate не invoice; unknown не ноль [S23].

После gap считываются только relevant snapshots/pending requests и нужный участок history. Ошибка parser не превращается в пустой inventory. Восстановление не запускает CLI, который сам чинит сервис. OpenCode V2 остаётся direct HTTP [S5, S17].

Doctor выдаёт причину и доступный следующий шаг: reconnect read path, адресная сверка, продолжение поддержанным resume либо task handoff после разрешения старых эффектов. Unknown не скрывается вечным зелёным running и не разрешает blind reassign.

## 9. Маршруты и настройка

```text
route → module artifact + native entrypoint + account/billing
      + model/native options + workspace/env + нужные возможности
```

Модельный alias — настройка. Native API — ответственность одного adapter. Muse subscription Max, Muse Go и PAYG различаются. Нет тихого fallback, подмены названия или снижения effort. Каталог и metadata не доказывают исполнение.

Готовность определяется ролью: batch writer не обязан иметь live steer; manager требует достаточное family/reply управление. Missing optional meter не выключает пригодный route. `describe` и compatibility facts относятся к exact entrypoint, версии и fresh/resume отдельно.

`native_options` локальны адаптеру и валидируются его схемой. Новое необязательное vendor поле не ломает весь host; изменение обязательного смысла делает неподтверждённой затронутую операцию. Schema fingerprint служит сигналом сравнить, не общим kill switch.

| Runtime | Выбранная граница | Что не обещаем |
|---|---|---|
| Muse | Official SDK Connection/MSP, single inbound pump | ACK=applied, parent-end=семья окончена |
| OpenCode V2 | Direct HTTP existing service | V1 API, recovery через CLI, active roots=children |
| Codex | Existing shared native WebSocket attach; SDK types/router где применимы | JSONL SDK подключится к WS одной заменой argv |
| Claude | Native stream + нужные scoped hooks | Token deltas дают все children; resume заменит system-prompt snapshot |
| Command | Headless + mod readiness/events | queueMessage:void — подтверждение исполнения; mod reload без рестарта |
| agy | Собственный warm sequential stream | Claude control messages и TUI slash input совместимы |
| Zed | Отдельный native eval-cli batch | Persistent control API Delta/editor автоматически доступен |

Точные facts и sources — [аудит v16](agent_swarm.runtime-contract-audit-v16-20260929.md), не новая runtime-квалификация [S23]. Для Codex shared transport берётся готовая WS-библиотека внутри bridge, не собственный RFC 6455 и не ещё один server. Owned JSONL остаётся другим профилем.

## 10. Goal, общение и General Manager

Continuation имеет одного владельца: native goal либо явно разрешённый controller fallback. Установка goal может начать модельную работу; это не healthcheck. Новое содержательное событие не равно дополнительному циклу «продолжай».

Manager получает цель, requirements IDs, phase, sources, открытые findings и способ сдачи. Не всю историю аварий, не каталог всех tools и не несколько конфликтующих briefs. Native инструкции и небольшой skill содержат только реально нужные особенности интерфейса.

Scope: initial paths — старт, forbidden paths — запреты, prerequisite policy — заранее разрешённые необходимые дополнения в модуле владельца. Реальную коллизию с другим writer разрешает manager. Новая цель/приёмка — task.revise, обычная корректировка стратегии — сообщение. Полные разрешения не доказывают существование отсутствующих receipts [S17].

Сообщения адресуются Task/Attempt и сохраняются до ответа. Caller не ждёт peer response. Task-level вопрос может перейти новому владельцу после проверки актуальности; старый turn command молча не переносится. У каждого reader свой cursor; read не удаляет общий inbox. Delivery, ack, применение и полезность — разные факты [S11].

Когда нет полезной параллельной работы, ожидание child/CI/quota нормально. Проснувшийся manager получает готовый diff, вопрос или новый finding, не периодическую просьбу имитировать занятость. Native goal/schedule и controller timer не должны дублировать продолжение.

GM wake квалифицируется: native input либо поддержанный Channels-путь; ordinary MCP notification не гарантирует запуск хода. Иначе режим явно checkpoint-poll. Смена GM не теряет задачи/решения и лишает старый binding новых управляющих команд через наш API. Full-access shell не превращается в security tenant.

Client principal стабилен между переподключениями и не равен PID/link ID. Identity клиента и роль GM — разные вещи: в meta хранится текущий GM binding/epoch, а Application проверяет методы по роли и ownership. Writer не принимает собственную сдачу через `task.accept`; доверенный локальный operator и GM используют свои явные bindings. Это защита от случайных команд через штатный API, не sandbox от того же пользователя с полным shell-доступом.

## 11. Проверки и общая рабочая копия

CheckRunner — один обычный process executor. Cargo — профиль/parser, не новая build system. CheckSpec фиксирует candidate, profile/toolchain, features/targets и существенное env. Active dedupe — `(attempt_id, check_input_key)`: разные Task не делят один mutable CheckRun. Для одинакового повторного запроса своей Attempt возвращается существующий handle. Завершённый машинный cache может переиспользоваться по exact inputs, но создаёт отдельный результат-привязку новой Attempt; её требования проверяются заново. Network/time/external inputs без версии не выдаются за воспроизводимые.

Завершение process, exit code, parser status, coverage и findings независимы. `build-finished` не конец всего `cargo run/test`; stderr и non-JSON строки не исчезают. Error/incomplete не PASS при exit 0. В output drain timeout не убивается чужая агентная семья [S9].

Build resources: один writer target-dir, согласованные jobs Cargo; warm target может использоваться новым CheckRun. `resource_claimed_at_ms/resource_released_at_ms` отдельно от failed/error/incomplete. Пока неизвестна судьба принадлежащих проверке процессов, target-dir не освобождается; остальные каталоги работают. Завершение только верхнего PID или отсутствие job notification не доказывает конец Job [S19, S26]. Changed+reverse scope консервативен, включает общие config/codegen dependencies. `cargo metadata --no-deps` даёт declarations, не resolve; неизвестный граф не равен пустому [S18].

Работа прототипа — **main-only, без Git worktrees**. Параллельные writers допустимы в непересекающихся областях; общий интерфейс/index/commit/publish согласуются. Не `git add .` над чужой работой. Кооперативное владение не OS-песочница; смешанный diff решает manager, не эвристическое разрезание патча.

Зачётная проверка использует fixed source export exact candidate, не постоянно меняющийся checkout. Submodules/LFS/untracked/private build config учитываются явно; git archive не добавляет их автоматически. Важное effective env переносится в настоящий executor, иначе исчезнет nested wrapper override и вернётся sccache. `RUSTC_WRAPPER` и `RUSTC_WORKSPACE_WRAPPER` различаются [S17, S21].

Проверка живого checkout полезна как diagnostics, не immutable proof. Formatter изменил код — новый candidate. История источников сохраняется редкими savepoints по сдаче/рисковой операции/запросу, не перед каждым tool.

Писатель возвращает candidate, проверка — result, decision owner принимает phase. Сначала код и минимальный Clippy согласно кампании; broad tests после готового среза. Сборка одного пакета не выдаётся за проверку всего продукта.

Для зависимости фиксируется acceptance identity (revision/phase/candidate), а не только «было ready». Перед приёмкой потребителя проверяется, не отозвана ли она и не требуется ли адресная перепроверка. Пересмотр upstream не убивает выполняющихся писателей и не уничтожает полезные artifacts. Уже принятый результат потребителя остаётся историческим фактом; новая обязательная ревизия рассматривается явно, не каскадным стиранием всей приёмки.

## 12. Качество без микроменеджмента

Дешёвые detectors: duplicate owner, потеря наблюдения, repeated error с теми же входами, неподтверждённая обязательная настройка, пропущенный requirement, повтор CheckRun, незакрытый вопрос. Никакого LLM на каждый tool call.

Каждый incident содержит evidence, scope и следующее действие. Повтор обновляет счётчик, не порождает prompt-шторм. Missing family data не превращается в «у тебя 0 детей». Не судим о полезности по росту stdout/числу edits; ожидание инструмента не считается тупостью модели.

Semantic reviewer получает один вопрос и соответствующие requirements/hunks/dependencies/diagnostics, затем расширяет evidence адресно. 2–4k tokens — ориентир для micro-audit, не правило отрезать важное. Кэш зависит от проверенных inputs. Ошибка reviewer и пустые findings раздельны. Существующий candidate перепаковывается без повторного writer, если испорчен только отчёт.

Самопроверка рабочей модели, controller checklist и независимый review — разные уровни; наличие символа не доказывает корректную вертикаль. Входное исследование harness даёт полезное разделение, но не навязывает нам 25-turn cap или дополнительные permissions [S25].

## 13. GitHub, артефакты и recovery

Git/gh используют существующую авторизацию. Main publication — exact commit и non-force push; legacy PR — только по разрешённой project policy. При потере ответа сначала readback, не повтор publish.

```text
intent → remote effect/readback → durable publication fact
       → acceptance/bookkeeping → cleanup/notification
```

Сбой cleanup не отменяет уже применённый push/merge и не запускает writer заново. Post-actions — собственные Operations. Exact PR head не фиксирует base; локальный lock не запрещает изменения извне [S17, S22].

Artifact: own temp → finish/flush → immutable publication без overwrite → DB reference. Это не общая transaction filesystem+SQLite. Missing файл означает gap; повторное имя допустимо лишь для тех же байтов. Обычная автоочистка удаляет только явно disposable telemetry и закрытые tmp. Ссылки на evidence внутри JSON не защищаются SQL FK: generic «нет FK → удалить» запрещён. История acceptance, незакрытые вопросы и operation idempotency не удаляются вместе с логами. В первом срезе компактные request/outcome records сохраняются до явного архивирования проекта; не строим generic graph-GC. Секреты для запуска разрешаются по ссылкам в памяти, не копируются в persisted env и отчёты [S26].

Backup через SQLite Backup API. Migration до admission под singleton; неизвестную новую schema старый binary не открывает. Откат БД не безопасен при живых внешних effects: нужен reconciliation, не универсальный downgrade [S16].

Restore сначала сохраняет текущее и проверяет task-owned пути; чужие изменения не перетираются. Нет blanket reset/clean, чужого WAL/gc и мнимого отката внешних эффектов через Git.

## 14. Быстро, но с честной границей масштаба

Считаем roots/children/turns/bridges/builds отдельно. Один Tokio runtime обслуживает I/O; DB-thread только короткие операции; тяжёлая работа вынесена из обоих. Блокирующий donor не считается cancellable только из-за async wrapper [S24].

Status/report читают одну projection. Один native reader обслуживает service instance, если это позволяет протокол; не отдельный CLI polling на каждого child. Native auxiliary/model calls остаются внутри harness. Host измеряет их расход только по доступным фактам.

Quota/rate-limit/overload/auth/transport не смешиваются. Bucket привязан к реальному account/billing namespace. Неизвестный meter не ноль; policy определяет допуск новой работы. Полный RPM скрытых native запросов не обещается.

Технические deadlines реализованы общей due queue. Tokio intervals получают явный Skip/Delay, не default Burst; после sleep/restart — сверка и один актуальный catch-up. Durable slot и Operation уже хранятся в meta/operations, второй scheduler не нужен [S24].

Профилировочные цели остаются: 200 наблюдаемых root/child, 1 000 малых events/s, p95 own admission/status <100 ms; ориентир own host RSS до 256 MiB без histories. Это цели квалификации, не измеренные характеристики. Реальные SDK/модельные/компиляторные расходы считаются отдельно. Масштаб контроллера не обещает столько же одновременных model calls на любом тарифе.

## 15. Doctor, упаковка и обновления

Doctor читает уже собранные факты. Known-safe repair — observer reconnect или восстановление конкретного adapter после проверки ownership. Неизвестный дефект — один repair-task с exact versions, операцией, несколькими очищенными frames и нужными файлами. Не модель-супервайзер, не рекурсивный рой ремонтников.

Host/bridge/external server имеют разные failure domains. Потеря наблюдения не разрешает kill. Разрешённое исключение для неотвечающего и не продвигающегося child требует обоих условий и текущей политики, не одного возраста [S17].

Drain: запрет новых assignments сохраняется первым; затем согласуется native continuation, завершается/передаётся относящаяся работа. Reader и результаты не отключаются первыми. Обновление adapter даёт новой версии новые bindings; старые остаются на прежнем artifact. Нет горячей миграции SDK heap и перезаписи выполняющихся scripts.

Готовый SDK/crate берётся целиком на проверенной границе с fixtures/notices, local patches отдельно. Для Rust — штатные path/[patch]; Node/Python — module-local. Не собираем весь desktop CCCC/Atlas ради helper. [Донорский реестр](agent_swarm.donors-20260929.toml) — source inventory, не подтверждение установки [S24].

`UPDATE.md` каждого модуля: источник контрактов, изменяемые файлы, проверка через реальный adapter, активация нового binding, откат. При изменении CLI не надо читать все остальные integrations. Source/SDK/server/executable versions различаются; advertised schema не устанавливает новый binary автоматически.

Windows: explicit user-only Named Pipe ACL, remote clients off; один небольшой helper, не своя identity-платформа [S10]. Executable/argv/cwd/env задаются раздельно. PATH/Path объединяется case-insensitively, pwsh 7 выбирается по реальному пути. System PATH не меняется.

Token, Job, console, stdio и lifecycle owner — разные свойства. Hidden console не DETACHED_PROCESS; флаги не складываются вслепую. Reported-working WMI bootstrap не становится hot-path для каждого tool. Long SDK owners не в краткоживущем kill-on-close Job; checks — собственный управляемый Job [S17, S19–S20]. Doctor не меняет UAC/ACL/auth и не включает compaction.

## 16. Реализация и перенос в Eliot

| Срез | Рабочий результат |
|---|---|
| C01–C02 | Task/Attempt/Operation + Store/IPC/CLI; single owner, durable ack, статус без дублей |
| C03–C04 | Muse Max и OpenCode V2: два разных native lifecycle на одном RuntimePort |
| C05–C06 | Mailbox/GM wake, отчёт, command-check/Cargo, адресный возврат ошибки |
| C07–C08 | Shared Codex goal/family, Claude/Command/agy — по одному настоящему adapter |
| C09–C10 | GitHub/recovery/doctor, versioned activation, logon/SCM |
| C11 | Нагрузка, долгие сессии, экспорт результатов; Zed/ACP по готовности |

Сначала рабочий вход→выход, fmt и минимальный Clippy. Donor fixtures сохраняются; lifecycle-проверки выполняются при допуске соответствующей интеграции. Broad test/load framework не строится раньше feature-complete среза. Не нужно одновременно написать семь пустых adapters.

Core не меняется ради очередного native метода. Локальный patch donor допускается при конкретном дефекте; не открывает повторный конкурс всего стека. Не запрещаем нужные будущие extensions, но не включаем их раньше реального потребителя.

Семантика Eliot I10.15/17/18 сохраняется: наш RuntimePort → ExternalAgentAdapter/WorkExecutor, mailbox → PeerChannel, checks → InstrumentRunner, lifecycle → Kernel/AdapterSupervisor [S11]. Нет преждевременного полного Governor и production-системы receipts/подписей.

При передаче домена Eliot его Task authority становится единственным. Prototype scheduler для домена выключается, native modules могут остаться executors. Локальная БД — история экспериментов, не второй канонический store. Переносятся исправленные adapters, протокольные cases и измеренные failure boundaries, не утверждение, что весь прототип production-ready.

## Источники и проверенная область


[S1] Ранее изученные `MANAGER-BRIEF(1).md`, `eliot-swarm-control-20260929-2.zip`; исторические операционные случаи не являются кодом новых скриптов. Новый источник и актуализация — S17.

[S2] Muse SDK snapshot `a7c10c5d…`: [public exports](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/index.ts), [facade client](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/client.ts), [Connection и retries](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/connection/connection.ts), [workspace metadata](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/package.json).

[S3] Codex SDK snapshot `18194bfd…`: [client start/close/router](https://github.com/openai/codex/blob/18194bfd3534ca567d886eac454028dafaa68b6c/sdk/python/src/openai_codex/client.py), [public exports](https://github.com/openai/codex/blob/18194bfd3534ca567d886eac454028dafaa68b6c/sdk/python/src/openai_codex/__init__.py). Lower-level client не экспортируется через package root; это осознанная изолируемая зависимость, не стабильность, придуманная нами.

[S4] [Codex app-server](https://learn.chatgpt.com/docs/app-server) — прочитано 29.09.2026; новые docs не подменяют схему реально запускаемого binary.

[S5] [OpenCode V2 API](https://opencode.ai/v2/docs/api): `session.create`, `event.subscribe`; stream volatile. Совместимость установленной версии ещё не квалифицирована.

[S6] [Tokio channels](https://tokio.rs/tokio/tutorial/channels), [Named Pipe ServerOptions](https://docs.rs/tokio/latest/tokio/net/windows/named_pipe/struct.ServerOptions.html). Чтение docs не является benchmark.

[S7] [SQLite WAL](https://www.sqlite.org/wal.html), [SQLite release 3.51.3](https://www.sqlite.org/releaselog/3_51_3.html), [CREATE TABLE / NULL constraints](https://www.sqlite.org/lang_createtable.html).

[S8] [rusqlite Connection](https://docs.rs/rusqlite/latest/rusqlite/struct.Connection.html): `Send`, не `Sync`; backup API имеет отдельную feature.

[S9] [Cargo external tools](https://doc.rust-lang.org/cargo/reference/external-tools.html): versioned metadata, JSON diagnostics, граница build-finished.

[S10] [Microsoft Named Pipe security](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-security-and-access-rights).

[S11] Eliot [I10.15](https://github.com/UnknownAlienHuman/eliot-memory-os/blob/3d2e9d1ddaf44d62baa5d6162dc6534034e7739a/docs/architecture/I10-15-agent-execution-fabric-and-durable-swarm.md), [I10.17](https://github.com/UnknownAlienHuman/eliot-memory-os/blob/3d2e9d1ddaf44d62baa5d6162dc6534034e7739a/docs/architecture/I10-17-adapter-subsystem.md), [I10.18](https://github.com/UnknownAlienHuman/eliot-memory-os/blob/3d2e9d1ddaf44d62baa5d6162dc6534034e7739a/docs/architecture/I10-18-mailbox-blackboard-live-peer-delivery-and-anchored-review.md).

[S12] [Microsoft interactive services](https://learn.microsoft.com/en-us/windows/win32/services/interactive-services).

[S13] [rmcp features](https://github.com/modelcontextprotocol/rust-sdk/blob/main/crates/rmcp/Cargo.toml), прочитанный blob `772844d864c954f989ef7607e6e6616258378a45`. Это feature-audit, не новый проверенный dependency lock.

[S14] [Tokio AsyncBufReadExt — cancel safety](https://docs.rs/tokio/latest/tokio/io/trait.AsyncBufReadExt.html), [LinesCodec](https://docs.rs/tokio-util/latest/tokio_util/codec/struct.LinesCodec.html).

[S15] [Tokio mpsc send/reserve](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html).

[S16] [SQLite Backup API](https://www.sqlite.org/backup.html), [foreign_keys вне транзакции](https://www.sqlite.org/foreignkeys.html).

[S17] Новый [MANAGER-BRIEF.md](MANAGER-BRIEF.md), 795 физических строк (индекс Files: 796 с завершающей пустой строкой), snapshot SHA-256 `bd9192cdfcc5b9e1810effac315e0b377ac8b593042cf656032b8be9b9e32600`. Последние события — 29.09 до 15:33 по журналу автора; timezone в журнале не уточнялась. Это report Claude, не наша live-проба. Координаты и границы — [review v15](agent_swarm.brief-review-v15-20260929.md).

[S18] [Cargo metadata](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html): resolve=null при --no-deps, declarations и target/features.

[S19] Microsoft: [Process Creation Flags](https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags), [Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects).

[S20] Microsoft: [Win32_ProcessStartup](https://learn.microsoft.com/en-us/windows/win32/cimwin32prov/win32-processstartup), [STARTUPINFOW](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/ns-processthreadsapi-startupinfow).

[S21] Cargo: [configuration](https://doc.rust-lang.org/cargo/reference/config.html), [environment variables](https://doc.rust-lang.org/cargo/reference/environment-variables.html).

[S22] [gh pr merge](https://cli.github.com/manual/gh_pr_merge), [git push](https://git-scm.com/docs/git-push): exact head и exact expected-ref; не атомарность всех GitHub metadata.

[S23] [Контракты семи harness](agent_swarm.runtime-contract-audit-v16-20260929.md), [источники](agent_swarm.runtime-sources-v16.json), [матрица](agent_swarm.runtime-matrix-v16.json). Проверка предыдущего прохода; её native-факты в этом проходе повторно не квалифицированы.

[S24] [Контракт модулей v2](agent_swarm.module-contract-v2.md) и его T1–T5: Tokio channels, spawn_blocking, missed ticks, Cargo overrides, SQLite partial indexes. Технические источники перечитаны 29.09.2026.

[S25] [Входное исследование harness](Harness_and_OpenCode_Go_master_2026-09-29_rev5.md) и [его intake](agent_swarm.harness-intake-20260929.md). Источник идей, не приказ перевести рой на один harness или принять чужие лимиты.

**Область v18:** перепроверены собственные границы v17 и reference DDL на контрпримерах. Новых vendor installs/builds, Windows/IPC-проб, model calls, GitHub-изменений и load benchmark не было. Старые source pins сохранены. Проверка артефактов описана в [validation](agent_swarm.spec-v18/validation-results.json), не является проверкой работающего сервиса.

[S26] Технические основания v18, прочитано 29.09.2026: [GitHub object identity](https://docs.github.com/en/graphql/guides/using-global-node-ids), [SQLite partial indexes](https://www.sqlite.org/partialindex.html), [RETURNING не commit](https://www.sqlite.org/lang_returning.html), [FK](https://www.sqlite.org/foreignkeys.html), [Microsoft Jobs](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects). Конкретные новые дефекты и отличия от уже покрытых случаев — [review v18](agent_swarm.design-review-v18-20260929.md).
