# ELIOT — проверка native-контрактов семи harness

**29.09.2026 · основание архитектуры v16. Документация и выбранные исходники, не runtime qualification.**

Проверены OpenCode **V2**, Codex, Claude Code, Meta Muse Code, Command Code, Antigravity и Zed; Delta рассмотрена отдельно от native Zed. Для каждого сопоставлены транспорт, сессии/дети, коррекции, goal, настройки/tools, usage и восстановление. Это проверка нужных нашему контроллеру разделов, не заявление о прочтении всей документации всех продуктов.

Действующие решения: небольшой Rust-host, готовые native SDK/исполнители, одна SQLite, существующая шина. Этот проход не добавляет новый процесс, broker, UI или обязательный этап аудита каждого tool call.

## 1. Как читать результаты

| Основание | Что оно доказывает |
|---|---|
| Официальная web-reference на дату чтения | Описанный разработчиком контракт; не наличие операции в локальном binary |
| Pinned source/schema | Поведение прочитанной реализации; не опубликованного пакета другой версии |
| MANAGER-BRIEF | Наблюдения и решения Claude на машине владельца; не наша повторная проба |
| Решение ELIOT | Выбранная интеграция; ещё должна быть написана |

Везде различаем **не найден поддержанный контракт**, **явно не поддержано выбранным режимом**, **не проверено на установленной версии**. Это три разных результата.

Источники с точными URLs и разделами: [реестр](agent_swarm.runtime-sources-v16.json). Сводка 7 × 7 направлений: [матрица](agent_swarm.runtime-matrix-v16.json). Матрица не включает runtime-функции и не заменяет результаты `describe`/handshake.

## 2. OpenCode V2: использовать сервис, а не опрашивающий CLI

**Документировано.** HTTP reference помечена experimental. `POST /api/session` создаёт сессию; `/prompt` принимает inbox input, а `resume:false` не планирует исполнение. Ответ — не результат модели. `delivery` различает queue/steer. `/background` переводит поддержанные foreground tools в background. `/session/active` описывает drains данного процесса; потомки выбираются отдельно по `parentID` с pagination. `/api/event` не даёт durable replay. Reload location затрагивает все загруженные locations и pending вопросы. `/experimental/session/stats` возвращает статистику, не подтверждённый баланс подписки. [OC-API]

**Инструкции.** V2 использует `AGENTS.md`, не fallback на `CLAUDE.md`. Ambient и уже загруженные nested-файлы обновляются по-разному; `instructions[]` в config не загружает указанные files/globs/URLs. [OC-INSTR]

**Модель.** Root default не удерживает variant. Доступность зависит от проекта; неизвестные metadata не становятся измеренными capabilities. [OC-MODELS]

**Правка адаптера.** Один HTTP client/reader на существующий service instance. Сохранять input/native IDs, проверять admission после неопределённого ответа. При gap перечитать нужную семью, не запускать CLI recovery. `/background` подключить как адресное native-действие, а не как автоматический «детач всего». Задание передавать явно; запись пути в config не отмечать как загрузку инструкции. Сохранить requested/applied model и variant.

**Осталось проверить на 2.0.7 из brief:** доступность этих конкретных endpoints/схем, безопасная boundary background, current family state и errors. Новая web-reference не обновляет работающий сервис автоматически.

## 3. Muse: готовый MSP даёт больше, чем прежняя оболочка

**Основание:** MSP v1 `a7c10c5d…`, не номер установленного Muse binary.

**Документировано.** `view/subscribe` — наблюдение без загрузки/lease сессии; `viewCursor` непрозрачен, gaps разрешаются через native replay/re-anchor. Нельзя считать порядок доставки `view/gap` полным native-порядком. `approval/decide` адресуется текущим requirement и available choice; child approval имеет `subagentOrigin`. `task/background` адресуется item, не произвольному PID. `subagent/readResult/sendMessage/followupTask` и goal-команды имеют собственные исходы. `usage/read` возвращает последнее наблюдение; проценты могут превысить 100, timestamps имеют native единицы. Raw token counters и counted-once totals различаются. [MC-SCHEMA]

В maintainer-пояснении к issue #6 per-turn effort и session default названы исправленными в 1.2.1; отдельная настройка host из `settings.json` не получила той же гарантии. Сохраняем точное версионирование этого свидетельства. [MC-EFFORT]

**Правка адаптера.** Использовать SDK transport/Connection, не писать MSP. Один pump; глобальные usage/health не отбрасываются из-за отсутствия sessionId. Настроить Max до первого запуска работы и проверить native readback. Родительский idle не закрывает bridge; child item, goal и turn наблюдаются отдельно. Reply означает ответ конкретному запросу, а не «approve всё по одинаковой строке».

**Не добавляем:** второй полный fold transcript, зеркало native DB, LLM для heartbeat. Остаются проверки SDK build, deployed CLI, fresh/resume и real provider effort; документационный fixture не является такой пробой. Подписочный и API-маршрут не смешиваются. [MC-SDK, MC-PLAN]

## 4. Codex: отдельно транспорт клиента и возможности сервера

**Документировано.** Direct stdio app-server принимает JSONL; WebSocket использует сообщения/Upgrade, включая Unix socket путь. WebSocket в публичной reference остаётся experimental. `thread/read` не загружает и не подписывает нить; resume делает другое. Default `thread/list` фильтрует источники cli/vscode; для семьи важны `sourceKinds` и документированные experimental `parentThreadId`/`ancestorThreadId`. Goal и `turn/steer(expectedTurnId)` независимы. Rate-limit read/update имеют собственные windows и единицы reset. [CX-API]

**Проверенный дефект прежнего выбора донора.** Pinned Python `CodexClient` реализует JSONL stdio. `launch_args_override` меняет запуск, не кодек. Значит, он **не подключается как есть** к WebSocket-over-stdio proxy из brief. [CX-SDK]

**Правка адаптера.** Выбранный shared server сохраняется `external_attach`. Закрытие нашего proxy не разрешает остановить server. Транспортная seam C07 использует готовую WebSocket-библиотеку; source routing/types можно переиспользовать только после адаптации, не объявлять SDK plug-and-play. Не писать RFC 6455 и не возвращать неявно server-per-lane.

Проверять схему самого 0.159.0 перед применением новых family filters. SUBAGENTS — возможное неполное дополнительное свидетельство, не причина навсегда игнорировать native family API. Status использует read, не resume. Настройки применяются до active goal; текущий turn ID обновляется на каждом продолжении.

**Не повторено локально:** запуск, скрытая консоль, общий transport, модель детей и её режим. Эти результаты известны только из brief (§13).

## 5. Claude Code: stream, hooks, goal и supervisor — разные контракты

### Основной маршрут

Headless stream содержит init/capabilities, ошибки загрузки integrations и native retry events. Завершение процесса с нулём не доказывает готовности всех нужных MCP. На resume отчёт стоимости может относиться к накопленному разговору. [CL-HEADLESS]

**Два критичных нюанса парсера:** token `StreamEvent` относится к main и не даёт parent_tool_use_id детей. Полные сообщения дают эту связь, но несколько content blocks могут иметь один `message.id`. Dedupe всего сообщения по одному ID потеряет часть текста/tools. [CL-STREAM]

CLI сохраняет system-prompt snapshot; изменение `--system-prompt` при resume не является немедленной заменой уже действующей инструкции. `--bare` удаляет нужные интеграции вместе с шумом; `--bg` — отдельный режим, не синоним `-p`. [CL-CLI]

**Правка адаптера.** Извлекать lifecycle из полных сообщений, не включать thinking/token firehose ради подсчёта детей. Сохранять блоки/SDK-структуру и родство. Новая Task revision доставляется отдельной коррекцией; compaction не включать самовольно. Model/tool APIs подключать по выбранному streaming-контракту, не именовать любое stdin-сообщение live steer. [CL-INPUT]

### Hooks и goal

HTTP поддержан не для каждого hook: `SessionStart`/`Setup` допускают command/mcp_tool, а не HTTP. Не нужен новый controller MCP у каждого worker: сначала stream, короткий command-hook только на отсутствующее нужное событие. [CL-HOOKS]

Goal использует модельную Stop-проверку; фоновые задачи откладывают её, headless и interactive check-in различаются. Это не бесплатный detector. Оставить один continuation owner и отдельно принимать результат Task. [CL-GOAL]

### Что уже можно взять готовым, не расширяя прототип

Native agent-view supervisor имеет `claude agents --json` и управление background roots. Но root inventory не включает native детей; respawn может повторить исходный prompt при отсутствии истории. Также есть отдельная worktree-политика и модельные summaries. Это возможный **отдельный route**, не безопасный restart-healer для основного stream. [CL-SUPERVISOR]

**Channels** — реальная opt-in MCP push-extension, поддерживающая `-p`. Она может стать способом разбудить **только GM**, без пользовательского чата и без подключения MCP всем писателям. Обычная MCP notification этой гарантии не имеет. Research Preview/allowlist/installed support остаются отдельной qualification; в первый срез канал не обязателен. [CL-CHANNELS]

### Подписка

Официальная Help-страница содержит UPDATE о приостановке 15 июня ранее объявленного изменения SDK billing. Нельзя брать нижний исторический текст той же страницы как действующий тариф. Наличие API key также может сменить billing path. Native auth сохраняем, автоматического финансового fallback нет. [CL-BILLING, CL-AUTH]

## 6. Command Code: готовый mod не является надёжной очередью сам по себе

**Документировано.** `queueMessage` возвращает `void`; steer применяется после текущего tool batch, followup — на другой границе. Mod exception может дать `mod_error` при продолжающемся CLI. `turn_end` предшествует commit; round/run usage вложены. Есть get/set active tools. Перезагрузка mod перезапускает процесс. [CC-MODS]

**Agent definitions обновляются иначе:** перечитываются перед следующим turn. Если `tools` опущено, набор пустой. Текущая документация показывает `background:true`, а brief 1.66 — `run_in_background`; совместимость названий ещё не установлена. Детям не даётся рекурсивный native agent tool. [CC-AGENTS]

Goal сохраняется после resume неработающим; его roundtrip budget не равен `-p --max-turns`. Документация TUI-команды не доказывает наличия headless goal setter в выбранном mod. [CC-GOAL, CC-HEADLESS]

**Правка адаптера.** Mod-ready и mod-error отдельны от process alive. Ответ очереди нашего bridge означает лишь локальную постановку, пока нет native evidence. Не отмечать `void` как прочтение/применение; не ожидать, что `/reload` сохранит живую сессию. Использовать exact resume ID, фактические tool schemas и существующий native PowerShell путь. Не накладывать сверху новые таймауты и не урезать выбранные разрешения. [CC-CLI, CC-SESSIONS, CC-WIN]

**Квоты:** в прочитанной странице usage-limit суммы Go расходятся между прозой и таблицей. Поэтому константы денег в scheduler не переносим. Run usage доступен; машинный остаток подписки в выбранном headless/mod-контракте не подтверждён. [CC-USAGE]

## 7. Antigravity: не использовать формат Claude по сходству названия stream-json

**Документировано.** Warm CLI принимает последовательные сообщения с discriminator `event`, не Claude `type`. `control_request/control_response` и slash input в этом режиме не поддержаны. После EOF канал заканчивается. `step_update` содержит tool и subagent сведения; resume адресуется conversation ID. `result SUCCESS` не доказывает успех всех tools: возможны мягкие отказы. Каталог запрашивается через `agy models`. [AG-HEADLESS]

Native дети и сообщения коллегам уже существуют; idle ребёнка не означает невозможность его последующего пробуждения. Fresh/shared/branch workspace — отдельный выбор. [AG-CHILDREN]

**Правка адаптера.** Собственный native codec внутри agy-модуля; общая шина остаётся прежней. Для выбранного входа коррекция — next-turn, пока не подтверждён более ранний способ доставки. Полные разрешения владельца сохраняются, но parser не замалчивает фактический отказ инструмента. Поле goal не выдумывается. Наблюдение детей связывается с conversation_id, не случайным текстом stdout.

`/usage` — интерфейс с refresh конфигурации/квоты, не подтверждённый стабильный JSON endpoint и не строка для warm input. Hooks имеют native схему и расположение, а не Claude names. API provider/auth отдельно от существующего подписочного входа. [AG-USAGE, AG-HOOKS, AG-AUTH]

**Остаётся:** installed version, native question delivery, полнота дерева и exact resume. ACP — отдельный последующий профиль, не доказательство возможностей этого CLI.

## 8. Zed и Delta: не смешивать batch, ACP-client и командную среду

Pinned `eval-cli` использует native `NativeAgent + AcpThread`, получает задание и сохраняет result/thread artifacts. Это headless evaluation binary. Exit 0 означает окончание агента, не acceptance Issue. В рассмотренном контракте нет поддержанного постоянного управляющего server/steer API. [ZD-EXEC]

Редактор Zed как клиент external ACP не становится от этого server для своего native агента. Native provider-интеграция OpenCode не даёт бесплатные модели так же, как родной OpenCode-клиент. [ZD-EXTERNAL, ZD-PROVIDERS]

Delta предоставляет собственные роли, изоляцию/синхронизацию и межагентные сообщения. Но **`native.default_shell` относится к интерактивным терминалам, не shell executor агента**. Поддержанный headless API Delta для полного нашего lifecycle в прочитанных разделах не установлен. [DL-CHILDREN, DL-SETTINGS]

**Правка адаптера.** Оставить native Zed отдельным batch-модулем C11. Не обещать persistent steer/goal, не управлять private DB и GUI. Delta-профили не переносить как параметры `eval-cli`. Платный model route, Windows build и child semantics проверяются отдельно. Основной рой от готовности этого модуля не зависит.

## 9. Что это меняет в собственном протоколе

Новых обязательных сервисов, таблиц и общих runtime-команд нет. Уточняются данные уже существующих `describe`, `send`, `configure`, `snapshot` и observations.

| Было недостаточно точно | Решение |
|---|---|
| «Harness поддерживает steer» | Привязка к entrypoint, версии, target и реальной delivery boundary |
| «Настройка записана» | requested / applied / unverified; когда вступает в силу и что затрагивает |
| «Нет активных roots» | Отдельная полнота семейства и незавершённых заданий |
| «Сессия восстановлена» | Отдельно transcript continuity, runtime residency и восстановленные функции |
| «Пришёл result» | Тип команды, terminal turn, родство и оставшаяся native работа |
| «Есть message ID» | Семантика ID источника: frame/item/content block, не произвольный hash текста |
| «У клиента есть MCP» | Подтверждённый native wake/Channels либо явный checkpoint polling |
| «Usage вырос на N» | Scope, единицы, delta/cumulative/window, источник, observed time; не суммировать перекрытия |

### Минимальная проверка обновления одного адаптера

1. Закрепить installed entrypoint/schema и сохранить новый vendor shape в его каталоге.
2. Изменить mapping только данного runtime; общей Task/Store модели обычно не касаться.
3. Проверить matching ACK, события и ошибки на существующих native fixtures без вызова модели.
4. Отдельно проверить необходимую live-функцию на **новой** разрешённой сессии; fresh/resume различаются.
5. Переключить новые bindings; старые продолжат работать прежней версией.

Это порядок будущей реализации, не список выполненных здесь runtime-проб. `doctor.inspect` не ставит goal, не вызывает model `/usage`, не делает resume/respawn и не reload-ит integrations.

## 10. Что исправлено в материалах

Архитектура v16 заменяет общие обещания native-возможностей ссылкой на точный контракт. План v4 локализует транспорт Codex, native event identity, configuration activation и coverage в файлах adapters/reports. SQL девяти таблиц остаётся прежним; меняются описание и reference JSON-поля. Донорские пины не обновлены автоматически.

Отдельно исправлена упаковка: корневой checkpoint оказался v14 при архитектуре v15, header donor inventory — v12. Это не изменение библиотек: теперь актуальные документы согласованы по revision. Публикуется один текущий комплект без вложенных исторических ZIP.

**Не выполнено:** Windows/native SDK build, авторизация/чтение пользовательских квот, пробные prompts, проверка Max на машине, load benchmark, изменения GitHub и исходных скриптов Claude. Документационная проверка не превращается в `runtime_qualified=true`.

## Источники

Список ниже — источники проверки, не обязательное чтение всех страниц каждым исполнителем. Область прочитанного и version basis находятся в JSON-реестре.

[OC-API] OpenCode V2 HTTP API — https://opencode.ai/v2/docs/api

[OC-INSTR] OpenCode V2 Instructions — https://opencode.ai/v2/docs/instructions

[OC-MODELS] OpenCode V2 Models — https://opencode.ai/v2/docs/models

[OC-AGENTS] OpenCode V2 Agents — https://opencode.ai/v2/docs/agents

[OC-CONFIG] OpenCode V2 Configuration — https://opencode.ai/v2/docs/config

[MC-SCHEMA] Muse Session Protocol v1 declarations — https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/msp.d.ts

[MC-SDK] Muse SDK public repository — https://github.com/meta-models/muse-code-sdk/tree/a7c10c5dd3f66be412077d29f9d11111af70317b

[MC-EFFORT] Muse SDK issue 6, maintainer correction — https://github.com/meta-models/muse-code-sdk/issues/6

[MC-PLAN] Muse Code subscriptions — https://dev.meta.ai/docs/muse-code/subscriptions

[CX-API] Codex App Server — https://learn.chatgpt.com/docs/app-server

[CX-SDK] Codex Python client source — https://github.com/openai/codex/blob/18194bfd3534ca567d886eac454028dafaa68b6c/sdk/python/src/openai_codex/client.py

[CL-HEADLESS] Claude Code headless — https://code.claude.com/docs/en/headless

[CL-CLI] Claude Code CLI reference — https://code.claude.com/docs/en/cli-reference

[CL-STREAM] Claude Agent SDK streaming output — https://code.claude.com/docs/en/agent-sdk/streaming-output

[CL-INPUT] Claude Agent SDK streaming input — https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode

[CL-HOOKS] Claude Code hooks reference — https://code.claude.com/docs/en/hooks

[CL-GOAL] Claude Code goal — https://code.claude.com/docs/en/goal

[CL-CHILDREN] Claude Code subagents — https://code.claude.com/docs/en/sub-agents

[CL-SUPERVISOR] Claude Code agent view — https://code.claude.com/docs/en/agent-view

[CL-CHANNELS] Claude Code channels — https://code.claude.com/docs/en/channels

[CL-BILLING] Claude Agent SDK with Claude plan — https://support.claude.com/en/articles/15036540-use-the-claude-agent-sdk-with-your-claude-plan

[CL-AUTH] Using Claude Code with Pro or Max — https://support.claude.com/en/articles/11145838-using-claude-code-with-your-pro-or-max-plan

[CC-MODS] Command Code Mods — https://commandcode.ai/docs/mods

[CC-AGENTS] Command Code custom agents — https://commandcode.ai/docs/agents

[CC-HEADLESS] Command Code headless — https://commandcode.ai/docs/headless

[CC-CLI] Command Code CLI reference — https://commandcode.ai/docs/reference/cli

[CC-SESSIONS] Command Code sessions — https://commandcode.ai/docs/sessions

[CC-GOAL] Command Code goal — https://commandcode.ai/docs/goal

[CC-WIN] Command Code Windows — https://commandcode.ai/docs/windows

[CC-USAGE] Command Code usage limits — https://commandcode.ai/docs/resources/usage-limits

[AG-HEADLESS] Antigravity CLI headless — https://antigravity.google/docs/cli/headless/

[AG-CHILDREN] Antigravity subagents — https://antigravity.google/docs/subagents/

[AG-HOOKS] Antigravity hooks — https://antigravity.google/docs/hooks/

[AG-USAGE] Antigravity /usage — https://antigravity.google/docs/cli/commands/usage/

[AG-AUTH] Antigravity CLI install — https://antigravity.google/docs/cli/install/

[ZD-EXEC] Zed eval-cli README — https://github.com/zed-industries/zed/blob/7604aa3f19cef0c4d8be2bb3335c24acd788ccb1/crates/eval_cli/README.md

[ZD-EXTERNAL] Zed external agents — https://zed.dev/docs/ai/external-agents

[ZD-PROVIDERS] Zed model API access — https://zed.dev/docs/ai/use-api-access

[DL-SETTINGS] Delta settings — https://delta.dev/docs/configuration/settings

[DL-CHILDREN] Delta subagents — https://delta.dev/docs/agents/subagents
