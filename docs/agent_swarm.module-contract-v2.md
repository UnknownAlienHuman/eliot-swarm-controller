# ELIOT Swarm — контракт модулей и качества интеграции v2

**Уточнено 02.10.2026 (Фаза A Documentation Program, PR #13). Контракт к архитектуре v18. Реализация существует: baseline `main` = `b0d27f4` — ядро и Store, встроенный OpenCode V2 adapter, Muse SDK bridge, модули Codex (без controller route), Claude, Command и Antigravity, Zed как отдельный unit без wiring в операции контроллера (issue #12), OpenCodex slices 1–2, MCP-фасад и GM/doctor реализованы и в основном проверены на фикстурах. Live-квалификации у большинства native поверхностей нет: она не следует из наличия кода, фикстур или успешной сборки и для каждой поверхности указывается отдельно по §5.**

Назначение: один разработчик должен подключить очередной native harness, не изменяя scheduler, Task/Attempt и остальные adapters. Этот документ определяет нашу границу. Он не переписывает протоколы производителей и не утверждает, что все перечисленные возможности есть у каждого harness.

## 1. Универсальность — одинаковые обязательства, не одинаковые возможности

Контроллер унифицирует назначение работы, адресацию, наблюдение, результаты и исходы команд. Он не унифицирует внутренние tools, model loop, billing, effort vocabulary и фоновые механизмы разных производителей.

В коде одна зависимость: `api/scheduler → RuntimePort → implementation`. Ни core, ни Store не импортируют vendor SDK и не ветвятся по `if runtime == Muse/Codex/...`. Ветвление по смыслу операции и её проверенному контракту допустимо. Идентификатор runtime — открытая строка; набор собственных базовых операций — небольшой типизированный enum.

### Store и prerequisites: исключение снято (историческая справка)

Норма абзаца выше действует без исключений. До Фазы B существовало одно именованное исключение, зафиксированное по факту кода, а не молча: bounded prerequisite-парсер OpenCode configure→input жил в `src/store/prerequisites.rs`, импортировал `runtime::opencode_v2`, разбирал contract revisions `opencode-configure-prerequisite-v1`, `opencode-session-agent-state-v1`, `opencode-session-model-state-v1` и форму native snapshot OpenCode (включая native model ref `{id, providerID, variant}`). Фаза B (слайс S1, R3) это исключение сняла: вся vendor-интерпретация prerequisite-свидетельств вынесена за границу adapter — в реестр валидаторов по runtime kind (`src/runtime/prerequisites.rs`; реализация OpenCode — `src/runtime/opencode_v2/prerequisites.rs`). Store оставляет только generic receipt/barrier: владение Operation и binding/generation, порядок «prerequisite — более ранняя Operation», order-scope контракта, generic envelope результата (identity исхода и native root/scope), равенство digest/revision по generic evidence, проверку поздних конфликтующих свидетельств и свежесть наблюдений. Store не именует vendor-типов, kinds и contract revisions; runtime без зарегистрированного валидатора prerequisites не реализует (`UNSUPPORTED_PREREQUISITE`), а свидетельство, выставленное под чужим adapter revision, валидатором не принимается.

Заодно закрыт §5.2 review программы: один `prerequisite_operation_id` упорядочивает один шаг, но не доказывает всю подготовку целиком. При settlement шага configure adapter эмитит bounded setup snapshot `{setup_digest, conditions:[{scope, revision|desired_digest}...]}` — плоский список условий, не DAG и не workflow-язык; жёсткие caps: 160 условий и 64 КиБ канонических байт, `setup_digest` — SHA-256 канонического списка условий. Store хранит snapshot как generic evidence в binding state и перед admission перепроверяет весь bounded набор: поздняя Operation на любом scope из snapshot (pending → gate pending; settled с иным эффективным состоянием → `PREREQUISITE_STALE`), а достаточно свежее наблюдение перепроверяет каждое условие через валидатор. Изменения на scopes вне snapshot запуск не блокируют.

Статус: извлечение — `implemented` (Фаза B, S1; baseline-исключение `b0d27f4` — история). Обещание нейтральности теперь выполнено по факту кода, а не с оговоркой.

`native.*` — расширение одного adapter, а не команда «исполнить произвольный JSON». Adapter регистрирует имя, input schema, область эффекта, порядок и способ подтверждения; host ведёт его как обычную Operation. Новые инструменты этого пространства не загружаются всем моделям в контекст. Справка по ним выдаётся адресно.

Одного названия `supports_steer` недостаточно: важно, какой шаг можно поправить, нужна ли active turn identity и что подтверждает ответ. Поведение не выбирается угадыванием по бренду CLI.

### Классы доставки input

Доставка input заявляется одним из классов, а не общим флагом steer:

| Класс | Смысл | Статус по факту кода |
|---|---|---|
| `next_turn` | Input для следующего turn; не исправляет уже идущий | `implemented`: OpenCode inbox, Muse bridge |
| `native_expected_target` | Точный steer активного turn по native identity ожидаемого turn (`expected_turn_id` / `expectedTurnId`) | `implemented`: Muse bridge (`turn/steer`); у OpenCode отклоняется (`UNSUPPORTED_EXACT_TURN_STEER` — нет атомарного expected-turn guard); SDK Codex метод имеет, но controller route нет — через операции ELIOT `unavailable`. Само обозначение класса — термин программы (R19), поле и семантика в Muse существуют в коде |
| `queue` | Постановка input в очередь native runtime | Не эквивалент steer и не замена `native_expected_target`. Как нативный исход `next_turn` при занятом turn — `implemented` в Muse bridge (`ifBusy=queue`); как самостоятельный обещанный класс доставки — не заявляется |
| `unavailable` | Adapter честно отклоняет доставку этого класса | `implemented` как поведение отклонения (OpenCode steer, неподдержанные delivery отклоняются валидацией) |

ACK транспортной или native границы подтверждает только эту документированную границу — admission, — а не чтение input моделью, не start исполнения и не terminal. Foreground wait, pending form, очередь и native execution требуют разных действий (см. также подраздел об attention в §7). Неизвестная доставка не превращается в отказ и не повторяется слепо (см. replay policy в §4).

## 2. Один контракт с четырьмя реализациями подключения

| Топология | Кто держит native transport | Что даёт общий RuntimePort |
|---|---|---|
| Native HTTP/SSE service | Внешний сервис; наш adapter — клиент | Operation, события и scoped snapshot |
| SDK-owned bridge | Долгоживущий bridge + SDK | Те же команды по local IPC, private vendor stdio |
| Existing shared backend | Внешний владелец server; bridge владеет клиентом/proxy | Управление только своими bindings |
| Batch executable | ProcessJob этого запуска | Запуск и результат; live-функции только если реально существуют |

Не добавляем другой coordinator для batch. Batch — ограниченный runtime-профиль. Read-only observer может читать существующий binding, но не создаёт второй control binding. Подключение ребёнка как независимого root не допускается, пока он принадлежит управляемой семье: используется её binding и адрес ребёнка. Иначе разные session IDs обошли бы уникальность владельца корня.

Первый bridge можно запускать на одну root-линию. Контракт содержит binding IDs и не требует отдельного process на каждого child. Multiplex нескольких roots в одном bridge разрешается только если конкретный SDK его поддерживает; отдельный универсальный connection-pool заранее не пишется.

Сбой adapter не равен сбою server. Если SDK владеет subprocess, его падение может оборвать соединение — это явная граница, а не обещание, которое исправляется словом «модульность».

## 3. Пакет модуля

```text
modules/<name>/<artifact-version>/
  module.toml            запуск, protocol major, возможности реализации
  bridge / executable    готовый SDK и небольшой facade
  vendor.lock            точные source/package/native references, без секретов
  fixtures/              сохранённые upstream-примеры и минимальные наши mappings
  UPDATE.md              что менять, как проверить, как активировать и откатить
  LICENSES/              notices и лицензии взятой единицы кода
```

Это формат поставки, не шесть новых обязательных служб. Встроенный OpenCode adapter может иметь такую же логическую информацию, оставаясь Rust-модулем.

`module.toml` описывает установленную реализацию. Результаты наблюдения host хранятся отдельно в существующих binding/observations. Правка manifest на `supported=true` не становится доказательством работы. Исследовательская runtime matrix — справочник, не install manifest.

Три изменения разделены:

| Изменение | Минимально нужная работа |
|---|---|
| Новая модель, уже поддержанная harness | Route alias/native model reference/options, проверка каталога и применения; без сборки ядра |
| Новый native метод/вариант протокола | Один adapter, types/fixtures и локальный patch; core не меняется |
| Новое общее свойство исполнения | Явное изменение RuntimePort только после двух реальных потребителей |

Не требуется два потребителя для новой vendor-функции: она остаётся в `native.*`. Правило ограничивает расширение общего ядра, а не доступ к новым функциям.

## 4. Восемь базовых операций

| Операция | Обязательство adapter | Что ответ не означает |
|---|---|---|
| `describe` | Фактический entrypoint, runtime/server versions, поддержанные операции и известные ограничения | Рабочую модельную сессию или гарантированную доступность подписки |
| `open` | Один конкретный запуск/создание, identity или явный unknown | Готовность всех settings и выполнение Task |
| `attach` | Проверить уже существующую identity, не создавать новую вместо отсутствующей | Возобновление/новый prompt без отдельного намерения |
| `snapshot` | Область, полнота, порядок/cursor и факты | Полную семью по root-only списку или жизнь по PID |
| `configure` | Выбранные settings, момент/область применения, evidence | Применение только потому, что файл записан или ACK получен |
| `send` | Точную доставку input/goal action по поддержанной границе | Исправление кода, чтение модели или принятую Issue |
| `reply` | Ответ текущему native request со stale-guard | Право угадать смысл ответа или ответить по уже устаревшему request |
| `shutdown` | Явный scope и реальный disposition собственного ресурса | Разрешение остановить общий server или чужих детей |

Малый `OperationContract` сохраняется в `effective_request_json`: `effect_scope`, `order_scope`, `completion_condition`, `replay_policy`, `fallback_used`, `contract_revision`. Это обычные данные решения adapter, не язык выполнения произвольных workflows. Scope задаёт binding/session/turn/request либо shared service, если операция действительно глобальная.

### Scoped delivery (R22)

Адресная доставка между участниками выражается данными существующего `OperationContract` и существующего mailbox, а не новым ledger и не вторым хранилищем доставок. Полный набор полей доставки: `delivery_id`, source/target scope, actor identity + generation, payload digest, admission/delivery/reply deadlines, `reply_to` исходной доставки, cancellation, ссылающаяся на исходную identity и digest.

Статус по факту кода: `implemented` частично — `message.send` сохраняет durable mailbox-сообщение, где `message_id` совпадает с `operation_id` (это и есть delivery identity), получатель — зарегистрированный client (target scope), отправитель берётся из аутентифицированного principal, а `in_reply_to` проверяется по исходному сообщению (совпадение sender/recipient обязательно). `proposed` — payload digest, deadlines, actor generation и cancellation по исходной delivery: в текущих полях mailbox их нет, и свободный текст/напоминание не становится workflow transition и не вызывает модель автоматически.

### Replay policy по операциям

Какая identity и какой payload могут повторяться и на каком основании:

| Операция | Повтор допустим | Основание |
|---|---|---|
| Любая мутация с `client_request_id` | Тот же caller + тот же ID + тот же метод + байт-в-байт тот же payload возвращает сохранённый receipt; тот же ID с другим payload — `REQUEST_ID_CONFLICT` | `implemented` (Store) |
| `open` / create native session | Native мутация не повторяется по generic retry; потерянный ответ разрешается readback либо остаётся `outcome_unknown`/incident | Норма `implemented` на путях с `replay_policy=readback_only_no_mutation_replay` (OpenCode configure/goal, configuration Operations OpenCodex); как универсальная таблица для всех adapters — норма контракта |
| `send` / prompt | Тот же input повторно не отправляется из-за потерянного ACK; повтор — только явный same-ID replay конкретного adapter (Muse: `agent.reconcile` с исходным command ID и неизменными параметрами) | `implemented`: Muse reconcile, OpenCode GET-only reconciliation; blanket replay при reconnect запрещён |
| `configure` | Повтор незавершённого шага подготовки, не успешно применённого (см. §6) | `implemented` |
| `reply` | Не повторяется на завершённый или изменившийся native request; stale-guard по ID и текущему содержанию обязателен | `implemented` (OpenCode: exact request ID + fingerprint текущего pending body) |
| Reads (`snapshot`, `result`, `family`, `report.delta`) | Повторяются свободно; read не является мутацией | `implemented` |

### Проекции: bounded preview, лимиты и gap (R6/R22)

Проекция (snapshot, timeline, report) — не сами данные. Bounded preview не равен полным artifact bytes: полные байты выдаются только immutable artifact с digest и читаются постранично (offset/length, ограниченный размер страницы); preview не подменяет эту выдачу и не выдаётся за неё. Лимиты проекции применяются после проекции, к её результату: число source rows, число projected items, serialized bytes всего ответа и bytes одного item; усечение при записи лога вместо этих лимитов не засчитывается.

Live notification path не равен authoritative fetch: живой поток даёт свежесть, авторитет даёт точное сохранённое чтение. В OpenCode volatile event feed уже не используется как доказательство — авторитетен durable execution log (`implemented`). Неполнота проекции выражается явно: `gap` — отсутствующий диапазон, а не пустое место; подписка, отставшая от потока, помечается `lagged`, что семантически тот же gap: авторитет восстанавливается точным read (`report.delta`, exact operation/result reads), а не доверием к хвосту кэша. Статус: cursor-чтения `report.delta`/`message.read` и пометки `gaps` в наблюдениях — `implemented`; MCP subscriptions и timeline-проекция по паттерну Paseo — `proposed`, подписок в фасаде сейчас нет.

Начало Task имеет одного владельца, выбранного до работы: controller либо native_manager. В первом случае start идёт через сохранённую Operation; во втором manager использует native spawn, а наш send не дублирует старт даже до bind_producer. Исполнитель, получивший controller-start, по-прежнему вправе запускать native детей.

Короткая admission-фаза не держит target заблокированным до конца model run. Ожидаемый смысл результата определён методом: установка effort ждёт факта применения; доставка сообщения может закончиться на native admission, не подтверждая его использование.

## 5. Готовность маршрута — по роли, а не один зелёный индикатор

Различаются `implemented`, `documented`, `observed`, `unavailable`, `unknown`. Последние сведения имеют версию/entrypoint и source. Достаточно существующего JSON, отдельного сервиса сертификации не требуется.

### Лестница доказательств и классы свидетельств

Ни один результат не повышает свой уровень доказательств по косвенным признакам. Лестница границ для одного действия:

```text
intent persisted
→ transport attempted
→ native admission established
→ native execution started
→ exact execution terminal observed
→ result bytes captured and pinned
→ configured check passed against exact candidate
→ acceptance committed and still current
```

`idle`, process exit, тишина, возраст, успешный HTTP status, имя ветки, отчёт модели или отсутствие элемента в частичной проекции не поднимают действие на следующую ступень. Каждая ступень подтверждается своим свидетельством.

Словарь готовности выше не заменяется словарём программы; они соотносятся так:

| Программа (§0/§0.2) | Этот контракт (§5) |
|---|---|
| `implemented` | `implemented` — код пути существует на baseline |
| `fixture_checked` | `implemented` + `observed` на фикстурах; live не наблюдалось |
| `live_observed` | `observed` на живой native поверхности |
| `qualified` | `observed` + ступень check/acceptance лестницы для exact candidate пройдена и текуща |
| `unavailable` | `unavailable` |
| `unknown` | `unknown` |

Отдельно от готовности каждое утверждение в документации несёт класс свидетельства происхождения: `CODE` (прочитанный код), `DOC` (документация/контракт), `RELEASE` (релиз/пин донора), `ISSUE` (зафиксированный issue), `USER` (решение/сообщение пользователя), `INFERENCE` (вывод) или `UNVERIFIED`. Reviewed pin донора, установленный pin и live-квалификация — три разных факта и не подменяют друг друга.

Требования задаёт выбранная роль:

- Batch writer: корректное назначение, fixed inputs, результат и известная граница владения.
- Native manager: дополнительно нужное наблюдение детей, доставка вопросов/ответов и управление текущей сессией.
- Reviewer: выбранные read-only возможности, корректные candidate inputs и отчёт о покрытии; ОС-изоляция не выдумывается по имени роли.

Отсутствие квотного API не запрещает запуск при разрешённой unknown-quota policy. Отсутствие live steer не запрещает batch task, но не скрывается при назначении manager, которому live steer необходим.

Чистый readback применённого параметра — native evidence, не независимая аттестация фактического inference. Для Max сохраняется смысл: `requested` → `native_effective` → при отдельно выполненной пробе `execution_observed`. Контроллер не должен требовать packet capture для каждой обычной задачи и не должен выдавать первый уровень за третий.

## 6. Подготовка без гонки ACK и запуска

```text
open admitted
→ native session identity получена и закреплена
→ configure model/effort/tools
→ наблюдён нужный результат configure
→ передано полное задание / активирован согласованный goal
```

Промежуточное `accepted` не открывает следующий шаг, если его `completion_condition=native_applied`. `record_ack` и `record_runtime_fact` не заменяют друг друга.

`prerequisite_operation_id` уже есть. Проверка prerequisites читает успешный **типизированный результат нужного шага**, не только `state=settled`. `settled/rejected/cancelled` и failure не могут удовлетворить настройку по факту окончания.

Для backend, где initial settings и prompt принимаются атомарно одним методом, adapter использует этот готовый путь. Не заставляем его выполнять пять лишних roundtrips. Многокомандная подготовка — только там, где native contract её требует. Возобновление после ошибки повторяет незавершённый шаг, а не успешно открывшуюся сессию.

Неприменённая обязательная настройка задерживает только запуск этой подготовляемой работы. Действующие unrelated turns не прерываются и не переводятся молча на другую модель.

Прежнее applied остаётся историческим фактом, но не вечным условием запуска. Для конфликтующих session-wide settings модуль сохраняет effective settings revision и привязывает setup к ней. Per-turn options предпочтительны; иначе короткий prepare/admission barrier исключает interleaving `set max → set high → start Max task`. При динамическом применении settings на следующих model calls совместимость определяется native lifecycle, не названием config setter. Другие bindings и protocol replies не ждут этого barrier.

### Typed condition evidence (R3)

Условие готовности шага подготовки — типизированное свидетельство, а не строка и не сам факт окончания операции. Digest внутри такого свидетельства описывает наблюдённое состояние (definition/variant digests конфигурации), но не создаёт native CAS: совпадение digest не даёт права считать, что native поверхность применит или удержит состояние, и не заменяет readback результата configure. Admission barrier вокруг конфликтующих session-wide settings короткий: он закрывает interleaving подготовки и запуска одной работы, а не удерживается на время model run.

Статус: `implemented` единственной реализацией — OpenCode configure→input, но validation живёт в adapter, а не в Store: `src/runtime/opencode_v2/prerequisites.rs` за реестром валидаторов `src/runtime/prerequisites.rs` (Фаза B, S1; именованное исключение §1 снято). Gate в Store сверяет generic evidence: типизированный результат шага, order-scope контракта, bounded setup snapshot всей подготовки и актуальный native snapshot через валидатор. Обобщённая форма typed evidence для других вендоров — тот же реестр: новый vendor добавляет валидатор на стороне runtime, Store при этом не меняется.

## 7. Одна native-сессия — один control owner, независимо от aliases

Ключ root состоит из `native_scope_key + native_root_id`. Scope — не произвольное имя route; adapter разрешает реальное пространство native IDs: runtime/account/store namespace, при необходимости user/machine domain. Секреты в ключ не входят. Порт, PID, model, lane label и имя bridge не являются заменой устойчивого namespace.

Пример: два aliases одной Codex home/shared-server session должны дать одинаковый ключ; разные независимые stores с одинаковым `ses-1` — разные. Смена модели не меняет владельца беседы.

После native identity выполняется `record_native_identity` в одной Store-транзакции. Partial UNIQUE index удерживает один unreleased binding на ключ. Старая проверка `one_live_root_per_lane` остаётся: это другая коллизия. До команды `attach`, способной менять native subscription/lease, claim известной identity делается заранее; простой read-only observer использует existing binding.

Внутри одного controller это устраняет alias-based двойное управление. Не блокирует чужой full-access CLI и не подменяет native session lock. При обнаружении такого конфликта фиксируется факт; контроллер не делает автоматический takeover.

Если backend не предоставляет устойчивую identity, adapter заявляет ограничение. Batch job остаётся учтённым собственным Operation/ProcessJob; точное повторное attach без evidence не объявляется поддержанным.

Старт новой native-сессии и начальная доставка уже claimed Task — разные idempotency scopes. Host не посылает второй initial dispatch одной Attempt лишь из-за нового client_request_id. Принятый модулем request не создаёт нового task owner. Перед отправкой host проверяет dispatch guards; module проверяет exact binding/generation/link и native preconditions. Revise после допуска не отменяет уже полученный vendor input; нужен реальный unqueue/cancel, поддержанный native контрактом, либо обычная коррекция.

### ProducerRef относится к активации, не только к беседе

Каждый task-specific ProducerRef связывает assignment_id с native session и конкретным run/turn/child-run ID или иной документированной correlation. Сессия может использоваться повторно. Поздний terminal предыдущего run сохраняется в его истории и не закрывает новое задание. При неполной корреляции adapter возвращает unknown, а manager явно закрывает assignment; синтетический локальный ID сам по себе не доказывает происхождение native event. Это использует существующий producers_json, не новый task store.

### Family: declaration-first и частичные наблюдения

Идентичность ребёнка начинается с авторитетного native объявления/run identity, а не с догадки по времени, cwd или заголовку. Aliases и parent links сохраняются рядом с этой identity. Raw native status, нормализованное отображение и разрешённое lifecycle-действие хранятся раздельно; terminal или idle родителя не закрывает детей, а неполное наблюдение не превращается в пустое множество: отсутствие элемента в неполном наблюдении не означает его terminal disposition, а недоступное наблюдение — не пустая семья.

Статус: `implemented` на пути OpenCode — snapshot перечисляет детей по native списку с проверкой `parentID`, `agent.family` постранично читает retained observation и честно возвращает `available:false, family_completeness:unknown, items:null` при отсутствии наблюдения; consumer-уровень `family_complete` остаётся `false`. Как универсальная норма для всех adapters — обязательство контракта; у adapters без native объявления детей семья заявляется `unknown`, а не пустой.

### Attention и native background (R12)

Attention item адресует текущий native request/tool и Task: форма, approval, foreground wait, blocked dependency или missing result. Ответ (`reply`) проверяет тот же request ID и текущее содержание запроса; повторные решения на завершённый request не отправляются. Неизвестная схема или неизвестное решение не угадываются: не выбирается автоматически first/recommended/cancel — вопрос передаётся менеджеру как задача выбора. Недостаток writers вычисляется только для разрешённой работы и известной capacity, с учётом pending native admissions; неизвестный roster не превращается в команду «запусти N».

Отдельная операция перевода foreground backgroundable tool в native background (`session.background`) заявляется только adapter, у которого endpoint подтверждён native контрактом. Она работает с foreground-инструментами, допускающими background, а не с любой активностью; ей не приписывается выдуманный expected-turn CAS, и она не считается прочитанным steer. Успех подтверждается native границей и последующим наблюдением, а не фактом отправленного запроса.

Статус: адресный reply со stale-guard — `implemented` (OpenCode: exact request ID, fingerprint текущего pending body, проверенная принадлежность корню семьи); обобщённая модель attention item — норма контракта, единого отдельного реестра attention в коде нет. `session.background` — `proposed`: ни один adapter её сейчас не реализует (в OpenCode она не заявлена как capability).

### Lifecycle, drain и postconditions (R13)

Завершение бывает разных видов, и они не подменяют друг друга: detach (контроллер отпускает наблюдение/управление, native работа может продолжаться), stop собственного ресурса (process job/bridge, которым владеет контроллер) и release (освобождение binding/claim в Store). Ни одно из них не останавливает shared native server и чужих детей; read-only observer сервер не запускает и не перезапускает.

Draining запрещает новый workload, но позволяет получить результаты детей, ответить на текущие вопросы и довести незавершённые операции до известного disposition. Parent idle и выход клиента сами по себе не освобождают native goal/детей; прекращение цели, interrupt, detach и release — разные явные действия с собственными postconditions: после stop известен disposition собственного процесса, после release binding не принимает новых операций, после detach native состояние остаётся таким, каким его показывает следующее наблюдение.

Неизвестный исход create/prompt/control разрешается предусмотренным native readback либо остаётся адресным incident: generic retry мутацию только из-за потерянного ответа не повторяет (см. replay policy в §4).

Статус: release binding/attempt и stop собственных process jobs — `implemented`; detach read-only observer без второго control binding — `implemented` (§2); единая drain-операция как метод контракта — `proposed`, отдельного drain-метода в API сейчас нет.

## 8. Шина без общего тормоза

Протокольный reader не ждёт аудитора, записи большого лога или окончания другого задания. Входящее сообщение сначала классифицируется как reply-required/control, важный факт либо telemetry. Крупное тело хранится отдельно.

Между core и модулями уже достаточно адресных `mpsc`, `oneshot` и `watch` [T1]. Один bounded канал со всеми payload не решает качество: `send().await` при заполнении остановит его producer. Поэтому native reader не может ждать на заполненной telemetry-очереди, пока следующее сообщение provider требует reply.

Диспетчер обходит ready targets round-robin; FIFO сохраняется внутри order scope. Не удерживает global permit, ожидая quota/capacity другого runtime. Раздельные control и тяжёлые queues не требуют отдельного broker. Приоритет native reply не означает вечного голодания остальных команд: после ограниченной порции control обслуживается готовая обычная работа.

`spawn_blocking` применяется к короткой ограниченной CPU/блокирующей работе. Долгий SDK reader — собственный процесс/thread; его нельзя «отменить» через `JoinHandle::abort()` и считать завершённым. Начатая blocking-задача Tokio этим не останавливается [T2].

При перегрузке сначала откладываются новые starts, повторные scans и optional audit. Принятые результаты не исчезают; работающие tools не убиваются ради CPU-порога. Pending operations ограничиваются до admission. При долгом отсутствии host промежуточная telemetry может потеряться с явным gap; бесконечная история при конечной RAM без диска не обещается.

## 9. Таймеры, ожидание и продолжение

Отчёты/reconcile — технические ticks, не model prompts. Для Tokio interval явно выбирается `Skip` или `Delay`: по умолчанию `Burst` воспроизводит пропущенные ticks, а первый tick срабатывает немедленно [T3]. Это не заменяет сохранение slot/Operation в SQLite.

Сроки хранятся как wall-clock для restart; локальное ожидание использует monotonic clock. После sleep/clock jump/restart выполняется сверка и один актуальный catch-up, не копия всех пропущенных напоминаний. Native schedule, already-admitted goal и controller timer не становятся тремя владельцами одного continuation.

Когда действительно вся полезная работа зависит от результата ребёнка/CI/quota, manager может ждать события без модельного polling. При готовом diff, вопросе, разблокированной задаче или новом finding он получает одно предметное событие. Это не разрешение игнорировать готовую работу; бессмысленная занятость не считается прогрессом.

## 10. Качество без проверяющей модели на каждом инструменте

Детерминированно проверяем только то, что действительно доступно: ownership, exact candidate, exit/coverage, недостающий requirement, неприменённую настройку, повтор одной ошибки при тех же входах. Сложный смысл и полнота интеграции требуют адресного reviewer. Наличие символа не доказывает рабочую вертикаль.

Результат проверки: `execution_status + coverage + findings`. Required checks и политика независимости принадлежат Task/профилю, не writer. Они не редактируются автоматически под текущий результат. Отсутствующий output, parse failure и unsupported verifier не превращаются в PASS. Feedback привязан к exact submission, а отзыв acceptance — к operation ID решения; reviewer старой сдачи не может отменить новое решение с тем же candidate SHA. Приёмка и request_changes принадлежат decision owner, не runtime-парсеру.

Аудитору передаётся один вопрос с относящимися sources/hunks/diagnostics. Scope расширяется по доказательной необходимости; полный transcript не становится стартовым пакетом. Исправление формата сдачи при сохранённом кандидате не запускает writer повторно. Спорный finding не становится безусловным приказом переделать всё: GM получает конкретное противоречие и исходные anchors.

Очередной self-report не создаёт нового уровня доказательств. Именно эта разница отдельно сформулирована в входном исследовании harness (§2.5); его ограничения 25 ходов, UI-требования и иерархия предпочтений не переносятся на наш прототип автоматически.

## 11. Обновление без слияния чужих внутренностей

Берём целый SDK/crate/backend на выбранной границе. В `vendor_bridge` остаются imports, codec и version-specific mapping; core их не видит. Предпочтителен публичный API. Если его недостаточно — один помеченный private import или локальный patch на pinned source, а не неявная зависимость десятка модулей.

Патчи хранятся отдельно от неизменённого upstream snapshot. Для Rust используем штатную path dependency / `[patch]`, а не свой package resolver [T4]. Node/Python остаются module-local; никаких runtime `@latest`, глобального pip/npm и auto-install при чтении статуса.

Проверка совместимости ограничена используемыми операциями. Неизвестное новое native поле сохраняется/игнорируется по правилам схемы и не отключает весь backend. Изменение смысла обязательного поля делает неподтверждённой затронутую возможность. Read-only и остальные пригодные пути сохраняются. Schema fingerprint — повод сравнить, не глобальная авария.

Разработка сначала: реализовать путь → fmt/минимальный Clippy → использовать сохранённые fixtures и короткую проверку соответствующего adapter. Не создавать отдельный load framework до готового среза. Перед рабочим допуском проверяется реальный маршрут, а не только специально написанная probe-команда.

Новая версия получает новые bindings; старая завершает существующие. Если vendor обновляет общий server сразу для всех, drain нашего bridge не обещает защиты от этого — режим vendor update согласуется отдельно. Откат binary не откатывает уже совершённые remote effects.

Для собственных batch/check jobs результат и владение ресурсом раздельны. `failed/incomplete` не сообщает, что все дочерние процессы прекратили работу. Adapter возвращает process disposition, control host освобождает ресурс только по нему. Для `external_attach` не применяется завершение shared server. Результат проверки сохраняется даже при неудачной очистке, а повторный writer target-dir не запускается на неизвестной старой работе. Повторное использование законченного machine check обозначается cached_from_check_id и не выдаётся за новый process run: не создаёт PID, собственного exit code или фиктивного resource release.

### Install/update: согласие по preview и fingerprint

Установка и обновление модуля следуют паттерну preview/fingerprint/reapproval: точный source и package разрешаются заранее; вычисляются fingerprint-ы source/manifest/lock/build/capability; delta показывается до действия; одобрение относится к этому fingerprint; перед применением разрешение повторяется, устаревший fingerprint отклоняется; новая generation ставится отдельно, её postcondition проверяется, а старая generation снимается только после доказательства владения. Расширение permissions или capability требует повторного одобрения (reapproval), даже если остальная delta пуста.

Статус: для установки/обновления модулей — `proposed`: автоматической установки модулей и сервисов в контроллере нет. Тот же паттерн `implemented` на соседней границе — конфигурационные Operations OpenCodex (preview + `planFingerprint`, пара both-or-neither, `409` при stale preview, сверка readback) — но это изменение конфигурации внешнего сервиса, а не install модуля, и одно за другое не выдаётся.

## 12. Проверяемая граница качества

| Ситуация | Ожидаемый результат реализованного пути |
|---|---|
| Один native root через два aliases | Один control owner; read-only watcher допустим |
| Effort ACK, но применения ещё нет | Новая model work не начата; ожидается apply/result |
| Reporter перестал читать | Protocol replies и native execution продолжаются |
| Parent idle, дети работают | Root не освобождается; готовая отдельная Task может сдаваться |
| Timeout после возможного prompt admission | `outcome_unknown`, адресная сверка; не новый prompt |
| Удалён необязательный usage field | Missing meter, не нулевая квота и не остановка всей линии |
| CPU-нагрузка | Меньше новых тяжёлых jobs, не kill работающей семьи |
| Сломан формат отчёта writer | Repair packaging на сохранённом candidate |
| Restart после пропуска отчётов | Один актуальный catch-up, не spam из старых slots |
| Смена profile alias | Старый binding сохраняет профиль; новые используют новую revision |
| Task revised while input queued | Старый input не отправляется после проигранного dispatch guard; already-admitted outcome отдельно |
| Max применён, затем настройки заменены | Прежний apply не открывает запуск на другой конфигурации |
| Check incomplete, descendants unknown | Target-dir удерживается; соседние ресурсы свободны |
| Native spawn уже произошёл, bind_producer ещё нет | Native-manager claim не получает второй initial dispatch |
| Child session повторно использована | Старое completion не закрывает новую activation |
| Review пришёл после новой сдачи/приёмки | Исторический finding; актуальный pointer не меняется |
| Reuse process check из cache | Собственная привязка к Task, без выдуманного process/resource lifecycle |
| Старый HOLD/возврат пришёл на новую сдачу той же ветки | Старая сдача историческая; новый submission и его owner не меняются |
| Shared stream несёт детей трёх managers | Scoped roster/metrics каждого manager без чужих детей в count/usage/quota |
| Preservation при untracked source-файле в worktree | Cleanup удерживается, пока требуемые bytes не сохранены; сохранённый HEAD не равен сохранённому worktree |
| Cargo-профиль изменён вне Git source | Старое check evidence не переиспользуется без соответствующего контракта входов |

Это критерии реализации, не результаты выполненных runtime-тестов. Первая обязательная пара исполнителей — Muse Max и OpenCode V2; batch и shared Codex проверяют остальные особенности того же контракта по мере подключения. Нет требования одновременно дописать все семь harness до первого полезного результата.

## 13. Источники и область проверки

Внутренние правила выше — проектные решения этого прохода. Native mappings — ранее сохранённый [аудит семи harness](runtime-notes.md), не новый blanket-вердикт об их версиях. Входные наблюдения — [lessons](lessons-learned.md) и [candidate notes](candidate-notes.md) с ссылками на исходники в Git history.

Внешние технические источники прочитаны 29.09.2026; это не версии библиотек установленного прототипа:

[T1] Tokio channels и backpressure: https://tokio.rs/tokio/tutorial/channels

[T2] Tokio 1.53.1, spawn_blocking/cancellation и long-lived workloads: https://docs.rs/tokio/1.53.1/tokio/task/fn.spawn_blocking.html

[T3] Tokio interval/MissedTickBehavior: https://docs.rs/tokio/1.53.1/tokio/time/enum.MissedTickBehavior.html

[T4] Cargo dependency overrides: https://doc.rust-lang.org/cargo/reference/overriding-dependencies.html

[T5] SQLite partial unique indexes: https://www.sqlite.org/partialindex.html

Историческая оговорка (ревизия 29–30.09.2026): повторная проверка всех vendor docs, сборка доноров, Windows execution, платные пробы и нагрузка тогда не выполнялись. Пример устранённой alias-коллизии относился к reference DDL, не к уже существовавшему Rust-сервису. Эта оговорка описывает область проверки той ревизии, а не текущий статус: актуальная готовность на 02.10.2026 — заголовок документа и §5, native mappings выше по-прежнему опираются на источники 29.09.2026 и заново не переаттестовывались.

[T6] Уточнения v18 относятся к собственному протоколу и reference-схеме: [сохранённые контрпримеры](lessons-learned.md#3-исправленные-ошибки-собственной-спецификации), [plan-v6](agent_swarm.implementation-v6.md). Native mappings и donor pins не переаттестованы.
