# Native runtime notes

**Статус документа: research evidence.** Сведено 30.09.2026; основные сведения исследованы 29.09.2026 по датированным источникам ниже. §3.1 отдельно проверен по официальным страницам OpenAI 30.09.2026. Это не новая квалификация всех harness или Windows и не описание текущей готовности: датированные research-факты ниже не переписываются в текущие утверждения. Текущая готовность (implemented / fixture-checked / live-qualified) — в [README](../README.md) и в module guides; локальное ядро и native-адаптеры уже реализованы и подключены в объёме, указанном там. Точный entrypoint важнее названия CLI.

**Как читать эти заметки.** Record, ACK, start и terminal — разные факты: сохранённая запись не доказывает admission, ACK не доказывает начало исполнения, а начало не доказывает terminal outcome. Observer не равен owner: наблюдатель не запускает и не перезапускает внешний server и не приобретает прав владельца сессии от одного факта наблюдения. Статусные пометки по секциям ниже разводят исследованную native-возможность и то, что реально реализовано в адаптере.

[Матрица](agent_swarm.runtime-matrix-v16.json) и [реестр 40 источников](agent_swarm.runtime-sources-v16.json) относятся к прежнему исследованию; новые SIWC-источники приведены отдельно ниже. Коды вида `OC-API` относятся к этому реестру. Он хранит URL, раздел и основание; движущаяся web-reference не доказывает наличие функции у установленного binary. [Пины доноров](agent_swarm.donors-20260929.toml) — source inventory, не installation lock.

## 1. OpenCode V2

**Статус реализации:** документированный ниже `/background` исследован как native API и реализован в адаптере операцией `agent.background` (контракт и честная квалификация — fixture-уровень, live против установленного сервиса не проведена — в [modules/opencode/README.md](../modules/opencode/README.md)); `agent.send` поддерживает только `delivery=next_turn`, а exact-turn steer отклонён (см. тот же README). До live-квалификации `/background` не заявляется как live-доступная операция установленного сервиса.

**Документированный путь (OC-API/INSTR/MODELS):** direct HTTP существующего service; session creation, inbox prompt, `resume:false`, queue/steer и адресный `/background` для поддержанных foreground tools. `/session/active` — drains процесса, не полная семья. Потомки — отдельная paginated выборка по `parentID`; event stream volatile. Experimental session stats не равны балансу подписки.

**Существенные границы:** V1/V2 endpoints не смешивать. Location reload затрагивает несколько locations и pending requests. `instructions[]` в рассмотренной V2-reference не загружает перечисленные files/globs/URLs; nested AGENTS и ambient instructions обновляются по-разному. Root model default не удерживает variant; доступность зависит от проекта.

**Reported deployment B:** версия 2.0.7; создание и сообщения переведены на HTTP после restart storm от CLI. `service.json` сообщает endpoint/PID/auth; пароль не логируется. Snapshots были отключены по решению владельца. Эти факты не устанавливают настройки другого компьютера.

**Для C04:** один client/reader на service instance, отдельный учёт roots/children, native admission после неизвестного POST outcome, snapshot после gap; не запускать CLI recovery. Проверить нужные схемы именно installed service. Private DB чтение остаётся версионированным supplemental fallback, не нашим writable store.

## 2. Meta Muse Code

**Статус реализации и нормы R18:** текущая реализация и её границы — в [modules/muse/README.md](../modules/muse/README.md); там же нормы R18 по семантике command/replay/gap: identity команды создаётся один раз и сохраняется до native I/O, replay — только с тем же command ID, gap и host-death не превращаются в выдуманный terminal. Приведённое ниже — research-основание SDK, а не отдельное утверждение о live-квалификации.

**Основание:** официальный SDK/MSP snapshot `a7c10c5dd3f66be412077d29f9d11111af70317b`, а не номер native binary. MC-SCHEMA/SDK/EFFORT/PLAN.

**Готовое:** SDK transport/Connection и generated types, viewCursor/replay, pending approvals/input, child provenance, task/background, subagent result/message/followup, goal и usage. `view/subscribe` — наблюдение, не implicit session load/lease. ACK command — admission, не outcome; действующие available choice/requirement IDs нужны для approval. Raw usage и counted-once totals различаются.

**Для C03:** один inbound pump; global usage/health не теряются из-за отсутствия sessionId. Max задаётся до задания/active goal с native readback. Родительский idle/turn-end не закрывает SDK. Отдельно goal/turn/native family, gaps и pending questions. Не складывать второй полный transcript поверх native history.

**Ограничение свидетельства:** maintainer issue связывал исправление per-turn/session effort с Muse Code 1.2.1, но не давал той же гарантии host settings.json. SDK build, actual subscription/Max, fresh/resume и long-run recovery ещё требуют собственной квалификации. Native-подписку не заменять API-route по совпадению имени модели.

## 3. Codex

**Статус реализации и seam R19:** текущий read-only срез, граница shared app-server и seam R19 (allowlist до записи в сокет, shared server никогда не запускается/не останавливается модулем, `thread/read` не равен resume) — в [modules/codex/README.md](../modules/codex/README.md). Ниже — research-основание; модуль пока standalone, controller route через RuntimePort/Operation не зарегистрирован.

**CX-API/SDK:** owned app-server stdio использует JSONL; WebSocket/proxy — другой transport. Pinned Python `CodexClient` `18194bfd3534ca567d886eac454028dafaa68b6c` читает JSONL; `launch_args_override` меняет команду, не codec. Shared attach требует готовой WS-библиотеки внутри модуля, не самодельного RFC 6455 и не молчаливого нового server-per-lane.

**Сессии:** `thread/read` не равен resume/подписке. Default sourceKinds и experimental parent/ancestor filters нужно учитывать для family inventory; root subscription не даёт автоматически всех детей. `turn/steer` привязан к актуальному turn ID; goal continuation меняет его. Active goal может начать работу сразу: полное задание/settings должны быть уже связаны с запуском.

**Reported B §13/14:** после reboot использован общий пакет 0.159.0, прямой `app-server --listen unix://` и WebSocket-over-stdio proxy. Автор сообщил работающие goal/steer и codebase-memory; модель детей уточнял по `turn_context`, не parent-like session header. Это report, не повторённая нами проба. Shared `.codex` и account не разделяются автоматически.

**Для C07:** `external_attach` владеет client/proxy, не общим server. Actual server/client/adapter versions, startup cwd/env, token, Job и console mode различаются. Закрытие proxy не даёт права stop shared server. Read/status не устанавливает goal, не делает resume/respawn/auto-update. Fork для вопроса — другая оплачиваемая беседа, а не ответ живого manager и не применение correction.

**Provider notes из H §24:** транспорт, полноценный model catalog, GUI picker и mixed-provider native children — отдельные проверки. В source Codex 0.155.0 роль меняла model, но не независимый provider; gateway мог обойти это общим endpoint, с отдельными ограничениями v1/v2 encrypted delegation. Эти исторические ограничения не объявляются вечным контрактом новых Codex и не делают gateway обязательным для прототипа.

### 3.1. ChatGPT plan usage — отдельный OAuth-маршрут

**Источник, а не live-проба:** указанный владельцем [SIWC app-server guide][SIWC-APP] и связанные страницы прочитаны 30.09.2026. Номер совместимого Codex binary эта страница не фиксирует. Она описывает отдельную авторизацию OSS/local приложения для eligible Responses-запросов за счёт ChatGPT plan; доступ к разговорам ChatGPT этим не предоставляется [SIWC-OVERVIEW].

**Конкретный пример OpenAI:** приложение передаёт свой OAuth `access_token` только в environment дочернего app-server (`ACCESS_TOKEN`). Provider `openai_chatgpt_plan` направлен на `https://api.openai.com/v1`, использует `env_key`, `wire_api="responses"`, `requires_openai_auth=false`, `supports_websockets=false`. Отдельный Codex login для этого примера не требуется. Управление — `--listen stdio://`, JSONL, успешный initialize → initialized → thread/start → turn/start. Сохраняется thread ID; terminal учитывается по status, не имени события. `clientInfo.name` согласуется с `agent_name_hint`, а не меняется на каждой линии [SIWC-APP].

| Путь | Controller → app-server | App-server → модель / auth owner |
|---|---|---|
| Текущий целевой shared Codex | WebSocket через native proxy; external_attach | Существующие provider/login и внешний владелец сервера |
| Пример SIWC | Собственный app-server, JSONL stdio | HTTP/SSE Responses с OAuth-токеном, refresh принадлежит приложению |

**Не смешивать два транспорта.** `supports_websockets=false` выбирает upstream HTTP, не меняет framing control socket. Он не превращает существующий WebSocket proxy в JSONL и сам по себе не отключает `turn/steer`. Общая RPC-reference требует active `expectedTurnId`; steer не принимает model/cwd/sandbox overrides. Схемы actual binary всё равно проверяются отдельно [SIWC-LIMITS, CX-CURRENT].

**Авторизация:** dynamic_agent_client нужен только для первой регистрации; сохраняется выданный client ID с проверенной identity и стабильным host ID. Нужны PKCE/state/nonce, проверка ID token и реально предоставленного `chatgpt.tokens.use.direct`. Это не чтение чужого auth.json и не выдача новых клиентов на каждый child. UUID host ID поддержан; новый PKI-механизм не требуется [SIWC-SIGNIN, SIWC-OVERVIEW].

**Возможности маршрута:** account-specific `/v1/models` и app-server model/list не равнозначны: RPC может вернуть встроенный каталог. Запрос модели должен действительно завершиться; начало stream не доказывает доступ или успех [SIWC-MODELS]. Preview требует store=false/stream=true; это не удаляет локальную историю thread/resume. Локальные shell/MCP и child agents через function/custom tools разрешены. Hosted MCP, Responses tool_search и top-level multi_agent не поддержаны; отсутствие последнего поля не запрещает native детей. Проверять весь реально используемый путь, не только простой текст [SIWC-LIMITS].

**Refresh и долгие сессии:** token-reference указывает access lifetime 1 час и rotating refresh lifetime 30 дней; реализация использует возвращённые expiry, а не собственный таймер жизни Task. Refresh одного renewable session сериализуется, замена token set сохраняется атомарно. Для env_key-примера OpenAI прямо требует restart app-server с новым token и thread/resume. Горячая замена токена этим guide не обещана [SIWC-TOKENS, SIWC-ACCOUNTS, SIWC-APP]. Общая RPC-reference отдельно описывает experimental chatgptAuthTokens/refresh; его совместимость с данным SIWC grant/provider не установлена и не заменяет указанную процедуру без проверки [CX-CURRENT].

**Ошибки:** subscription_sharing_usage_limit_exceeded может означать лимит конкретного приложения, не исчерпание всего плана; reset time из одного кода не выводится. usage_unavailable и временный network/5xx не основание стирать credentials. Unsupported capability требует исправить точный несовместимый параметр, а не бесконечно повторять input или молча менять billing. Ошибка может прийти после начала stream; сохраняются status, код, request ID и неопределённость результата [SIWC-ERRORS, SIWC-MODELS].

**Решение ELIOT:** это дополнительный явно выбираемый owned-native профиль, не замена существующего shared server и не обязательный новый OAuth-сервис перед C03/C04. Токены не идут в Task, Operation, route JSON или общий inherited env; сохраняются ссылки на protected credentials. Для включения профиля нужны фактическая авторизация и проверка refresh/resume с детьми. Restart процедуры относится только к принадлежащему профилю процессу; нельзя перезапускать общий сервер или повторять первоначальную Task. UAC/ACL/console, goal/family completion и отсутствие duplicate-send этот SIWC-пример не квалифицирует. Родные Muse/OpenCode/Antigravity маршруты и порядок разработки не меняются.

## 4. Claude Code

**Статус реализации:** Claude Agent SDK bridge.1 реализован и fixture-checked; live Claude session/resume и Windows native launch не квалифицированы — см. [modules/claude/README.md](../modules/claude/README.md). Ниже — research по native CLI/stream.

**CL-HEADLESS/STREAM/INPUT/CLI:** native stream и exact resume; init/capabilities и integration errors нужно читать независимо от exit code. Token StreamEvent не является inventory детей. Полные сообщения дают parent_tool_use_id; несколько content blocks могут иметь один message.id — dedupe по нему целого сообщения теряет tools. Resume cost может быть накопленной стоимостью разговора, не новой дельтой.

**Настройки:** system-prompt snapshot при resume не равен немедленной замене через новый CLI argument. `--bare` убирает не только шум; `--bg` не синоним `-p`. Ни самовольной compaction, ни общего копирования CLI flags всем линиям.

**CL-HOOKS/GOAL:** поддержанные транспорты зависят от конкретного hook; рассмотренные SessionStart/Setup не HTTP. Сначала native stream, hook — только для недостающего факта. Goal Stop-check расходует модель; один continuation owner, а Task acceptance отдельно.

**CL-CHANNELS/SUPERVISOR:** Channels — opt-in MCP push, возможный wake для GM, не гарантия обычной MCP notification. Background supervisor roots не включают детей; respawn без истории может повторить исходный prompt. Это другой qualified route, не стандартный healer. **CL-BILLING/AUTH:** прежняя help-страница содержала UPDATE, меняющий нижний исторический billing-текст; API credential мог менять способ оплаты. Не переносить исторический тариф/фразу в финансовый fallback.

## 5. Command Code

**Статус реализации:** модуль (native mod + headless glue) реализован и fixture-verified; установленный Command Code binary с этим модулем не квалифицирован — см. [modules/command/README.md](../modules/command/README.md). Ниже — research по native поведению.

**CC-MODS:** `queueMessage` возвращает void, steer применяется после tool batch; followup — другая boundary. Mod exception может оставить живой CLI; нужны mod-ready/mod-error. `turn_end` может предшествовать commit. Active tools доступны native API. Reload mod перезапускает процесс: это не безвредный refresh наблюдателя.

**CC-AGENTS/GOAL/SESSIONS:** definitions перечитываются перед следующим turn; omitted tools в прочитанном контракте дают пустой набор. Docs показывали `background:true`, brief 1.66 — `run_in_background`: совпадение не установлено. Точный resume ID обязателен. Goal сохраняется после resume неработающим; roundtrip budget не равен --max-turns. TUI `/goal` не доказывает setter в выбранном headless/mod route.

**Для C08:** различать локальную постановку, native admission и применение. Не закрывать SDK/mod при завершении отдельного prompt; не хранить все версии событий как новые tokens. Run/round usage могут перекрываться; published usage-page имела расходящиеся суммы, поэтому не переносить деньги/RPM в scheduler константами.

## 6. Antigravity

**Статус реализации:** warm-stream bridge реализован и проверен на fixtures; live Antigravity и Windows native launch не квалифицированы — см. [modules/antigravity/README.md](../modules/antigravity/README.md). Ниже — research по native CLI.

**AG-HEADLESS/CHILDREN:** warm CLI stream использует `event`, не Claude `type`; последовательные prompts, EOF закрывает input. Claude control messages и slash input не поддержаны рассмотренным режимом. `step_update` несёт tool/subagent сведения; exact conversation resume. SUCCESS может включать мягко отказанные tools.

**Для C08:** собственный codec в adapter; next-turn delivery, пока не подтверждена более ранняя. Полные разрешения не отменяют учёт реально отказанного tool. Idle ребёнка не исключает его последующее пробуждение. Fresh/shared/branch workspace — выбор route, не автоматическая миграция наших правил.

**AG-USAGE/HOOKS/AUTH:** `/usage` — интерфейс с refresh, не подтверждённый JSON quota poll и не команда warm input. Hooks имеют свои схему и расположение. ACP, native CLI и paid API квалифицируются отдельно; один успешный route не аттестует остальные.

## 7. Zed / Delta

**Статус реализации:** batch executor unit реализован и протестирован (`src/runtime/zed.rs`), но **wiring gap**: через controller operations он не достижим — sessionless batch execution не подключено к session-oriented operations; отдельного `modules/zed/` нет. Открытый вопрос подключения — issue #12 (W1–W4). Ниже — research по eval-cli контракту, а не утверждение о подключённой операции.

**ZD-EXEC:** `eval-cli@7604aa3f19cef0c4d8be2bb3335c24acd788ccb1` использует NativeAgent/AcpThread и сохраняет result/thread artifacts. В прочитанном контракте это batch evaluation executor, не persistent control server; exit 0 — окончание run, не acceptance. Редактор как внешний ACP-client не становится server своего native loop.

**ZD-PROVIDERS/EXTERNAL:** native Go provider, external OpenCode agent и бесплатный OpenCode — разные маршруты; бесплатный доступ не переносится автоматически через API. **DL-CHILDREN/SETTINGS:** Delta имеет роли, peers и синхронизацию, но её `native.default_shell` — interactive terminal, не agent executor. Delta не приравнивается к исходникам Zed; headless API полного lifecycle в прочитанных разделах не установлен.

**Для C11:** отдельный batch route; Windows build/provider/child semantics квалифицируются по entrypoint. Не строить GUI clicks/private DB control как скрытый adapter. Дополнительные характеристики Delta — [candidate notes](candidate-notes.md), не обязательные требования к прототипу.

## Общие проверяемые различия

При изменении одного adapter проверять только используемую границу: версия и профиль; native IDs/порядок; ACK/outcome; fresh/resume; children; settings activation; requests/replies; usage basis/units/scope; платёжный маршрут. Successful help/catalog не доказывает model run. Проба active goal или fork расходует модель и не выполняется обычным doctor.

Полные прежние разборы: [R16][R16], [B][B], [R15][R15], [H][H]. Их ссылки сохраняют evidence, но worker не обязан читать историю целиком. Действующие переходы — в module-contract-v2/implementation-v6, не в этих заметках.

[R16]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/agent_swarm.runtime-contract-audit-v16-20260929.md
[B]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/MANAGER-BRIEF.md
[R15]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/agent_swarm.brief-review-v15-20260929.md
[H]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/Harness_and_OpenCode_Go_master_2026-09-29_rev5.md

## Источники дополнения SIWC

Прочитано 30.09.2026: страницы без зафиксированной версии Codex. Это основание проектного профиля, не installed-version/live evidence.

[SIWC-APP]: https://developers.openai.com/siwc/token-sharing-open-source/codex-app-server
[SIWC-OVERVIEW]: https://developers.openai.com/siwc/token-sharing-open-source
[SIWC-SIGNIN]: https://developers.openai.com/siwc/token-sharing-open-source/sign-in
[SIWC-ACCOUNTS]: https://developers.openai.com/siwc/token-sharing-open-source/profiles-and-sessions
[SIWC-MODELS]: https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference
[SIWC-LIMITS]: https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations
[SIWC-TOKENS]: https://developers.openai.com/siwc/token-sharing-open-source/token-reference
[SIWC-ERRORS]: https://developers.openai.com/siwc/token-sharing-open-source/errors-and-recovery
[CX-CURRENT]: https://learn.chatgpt.com/docs/app-server
