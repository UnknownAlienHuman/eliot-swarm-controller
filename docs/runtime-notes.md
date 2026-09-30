# Native runtime notes

**Сведено 30.09.2026; сведения исследованы 29.09.2026.** Выжимка прежних материалов, не новая web/source- или Windows-квалификация. Точный entrypoint важнее названия CLI. Реализация прототипа ещё отсутствует.

[Матрица](agent_swarm.runtime-matrix-v16.json) и [реестр 40 источников](agent_swarm.runtime-sources-v16.json) сохранены без изменения. Коды вида `OC-API` ниже относятся к этому реестру. Он хранит URL, раздел и основание; движущаяся web-reference не доказывает наличие функции у установленного binary. [Пины доноров](agent_swarm.donors-20260929.toml) — source inventory, не installation lock.

## 1. OpenCode V2

**Документированный путь (OC-API/INSTR/MODELS):** direct HTTP существующего service; session creation, inbox prompt, `resume:false`, queue/steer и адресный `/background` для поддержанных foreground tools. `/session/active` — drains процесса, не полная семья. Потомки — отдельная paginated выборка по `parentID`; event stream volatile. Experimental session stats не равны балансу подписки.

**Существенные границы:** V1/V2 endpoints не смешивать. Location reload затрагивает несколько locations и pending requests. `instructions[]` в рассмотренной V2-reference не загружает перечисленные files/globs/URLs; nested AGENTS и ambient instructions обновляются по-разному. Root model default не удерживает variant; доступность зависит от проекта.

**Reported deployment B:** версия 2.0.7; создание и сообщения переведены на HTTP после restart storm от CLI. `service.json` сообщает endpoint/PID/auth; пароль не логируется. Snapshots были отключены по решению владельца. Эти факты не устанавливают настройки другого компьютера.

**Для C04:** один client/reader на service instance, отдельный учёт roots/children, native admission после неизвестного POST outcome, snapshot после gap; не запускать CLI recovery. Проверить нужные схемы именно installed service. Private DB чтение остаётся версионированным supplemental fallback, не нашим writable store.

## 2. Meta Muse Code

**Основание:** официальный SDK/MSP snapshot `a7c10c5dd3f66be412077d29f9d11111af70317b`, а не номер native binary. MC-SCHEMA/SDK/EFFORT/PLAN.

**Готовое:** SDK transport/Connection и generated types, viewCursor/replay, pending approvals/input, child provenance, task/background, subagent result/message/followup, goal и usage. `view/subscribe` — наблюдение, не implicit session load/lease. ACK command — admission, не outcome; действующие available choice/requirement IDs нужны для approval. Raw usage и counted-once totals различаются.

**Для C03:** один inbound pump; global usage/health не теряются из-за отсутствия sessionId. Max задаётся до задания/active goal с native readback. Родительский idle/turn-end не закрывает SDK. Отдельно goal/turn/native family, gaps и pending questions. Не складывать второй полный transcript поверх native history.

**Ограничение свидетельства:** maintainer issue связывал исправление per-turn/session effort с Muse Code 1.2.1, но не давал той же гарантии host settings.json. SDK build, actual subscription/Max, fresh/resume и long-run recovery ещё требуют собственной квалификации. Native-подписку не заменять API-route по совпадению имени модели.

## 3. Codex

**CX-API/SDK:** owned app-server stdio использует JSONL; WebSocket/proxy — другой transport. Pinned Python `CodexClient` `18194bfd3534ca567d886eac454028dafaa68b6c` читает JSONL; `launch_args_override` меняет команду, не codec. Shared attach требует готовой WS-библиотеки внутри модуля, не самодельного RFC 6455 и не молчаливого нового server-per-lane.

**Сессии:** `thread/read` не равен resume/подписке. Default sourceKinds и experimental parent/ancestor filters нужно учитывать для family inventory; root subscription не даёт автоматически всех детей. `turn/steer` привязан к актуальному turn ID; goal continuation меняет его. Active goal может начать работу сразу: полное задание/settings должны быть уже связаны с запуском.

**Reported B §13/14:** после reboot использован общий пакет 0.159.0, прямой `app-server --listen unix://` и WebSocket-over-stdio proxy. Автор сообщил работающие goal/steer и codebase-memory; модель детей уточнял по `turn_context`, не parent-like session header. Это report, не повторённая нами проба. Shared `.codex` и account не разделяются автоматически.

**Для C07:** `external_attach` владеет client/proxy, не общим server. Actual server/client/adapter versions, startup cwd/env, token, Job и console mode различаются. Закрытие proxy не даёт права stop shared server. Read/status не устанавливает goal, не делает resume/respawn/auto-update. Fork для вопроса — другая оплачиваемая беседа, а не ответ живого manager и не применение correction.

**Provider notes из H §24:** транспорт, полноценный model catalog, GUI picker и mixed-provider native children — отдельные проверки. В source Codex 0.155.0 роль меняла model, но не независимый provider; gateway мог обойти это общим endpoint, с отдельными ограничениями v1/v2 encrypted delegation. Эти исторические ограничения не объявляются вечным контрактом новых Codex и не делают gateway обязательным для прототипа.

## 4. Claude Code

**CL-HEADLESS/STREAM/INPUT/CLI:** native stream и exact resume; init/capabilities и integration errors нужно читать независимо от exit code. Token StreamEvent не является inventory детей. Полные сообщения дают parent_tool_use_id; несколько content blocks могут иметь один message.id — dedupe по нему целого сообщения теряет tools. Resume cost может быть накопленной стоимостью разговора, не новой дельтой.

**Настройки:** system-prompt snapshot при resume не равен немедленной замене через новый CLI argument. `--bare` убирает не только шум; `--bg` не синоним `-p`. Ни самовольной compaction, ни общего копирования CLI flags всем линиям.

**CL-HOOKS/GOAL:** поддержанные транспорты зависят от конкретного hook; рассмотренные SessionStart/Setup не HTTP. Сначала native stream, hook — только для недостающего факта. Goal Stop-check расходует модель; один continuation owner, а Task acceptance отдельно.

**CL-CHANNELS/SUPERVISOR:** Channels — opt-in MCP push, возможный wake для GM, не гарантия обычной MCP notification. Background supervisor roots не включают детей; respawn без истории может повторить исходный prompt. Это другой qualified route, не стандартный healer. **CL-BILLING/AUTH:** прежняя help-страница содержала UPDATE, меняющий нижний исторический billing-текст; API credential мог менять способ оплаты. Не переносить исторический тариф/фразу в финансовый fallback.

## 5. Command Code

**CC-MODS:** `queueMessage` возвращает void, steer применяется после tool batch; followup — другая boundary. Mod exception может оставить живой CLI; нужны mod-ready/mod-error. `turn_end` может предшествовать commit. Active tools доступны native API. Reload mod перезапускает процесс: это не безвредный refresh наблюдателя.

**CC-AGENTS/GOAL/SESSIONS:** definitions перечитываются перед следующим turn; omitted tools в прочитанном контракте дают пустой набор. Docs показывали `background:true`, brief 1.66 — `run_in_background`: совпадение не установлено. Точный resume ID обязателен. Goal сохраняется после resume неработающим; roundtrip budget не равен --max-turns. TUI `/goal` не доказывает setter в выбранном headless/mod route.

**Для C08:** различать локальную постановку, native admission и применение. Не закрывать SDK/mod при завершении отдельного prompt; не хранить все версии событий как новые tokens. Run/round usage могут перекрываться; published usage-page имела расходящиеся суммы, поэтому не переносить деньги/RPM в scheduler константами.

## 6. Antigravity

**AG-HEADLESS/CHILDREN:** warm CLI stream использует `event`, не Claude `type`; последовательные prompts, EOF закрывает input. Claude control messages и slash input не поддержаны рассмотренным режимом. `step_update` несёт tool/subagent сведения; exact conversation resume. SUCCESS может включать мягко отказанные tools.

**Для C08:** собственный codec в adapter; next-turn delivery, пока не подтверждена более ранняя. Полные разрешения не отменяют учёт реально отказанного tool. Idle ребёнка не исключает его последующее пробуждение. Fresh/shared/branch workspace — выбор route, не автоматическая миграция наших правил.

**AG-USAGE/HOOKS/AUTH:** `/usage` — интерфейс с refresh, не подтверждённый JSON quota poll и не команда warm input. Hooks имеют свои схему и расположение. ACP, native CLI и paid API квалифицируются отдельно; один успешный route не аттестует остальные.

## 7. Zed / Delta

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
