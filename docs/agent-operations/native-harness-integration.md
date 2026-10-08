# Нативная интеграция harness: управление, наблюдение и квоты

**Проверено 7 октября 2026.** ELIOT: `40591a295af94b1541ec2ba30afe8e3247701a71`. Это source-based план интеграции, не установленная capability matrix и не отчёт о проверке аккаунтов. Работающий сервис, его точная схема и выбранный artifact должны квалифицироваться отдельно. Старые полевые аудиты дают сценарии, но не заменяют текущую документацию вендора.

**Решение:** сохранять нативный model loop, инструменты, историю и управление детьми; ELIOT ведёт собственные Operations, Task/Attempt, authority и доказательства внешнего эффекта. Не сводить все возможности к `send(text)`, но и не открывать произвольный vendor RPC через универсальный passthrough.

## 1. Что соединять

| Система | Предпочтительная граница | Что не смешивать |
|---|---|---|
| Codex | Одна долгоживущая app-server connection с разбором ответов, уведомлений и server requests | `exec` для batch не заменяет управление thread; app-server не равен OpenCodex proxy. |
| Muse | Существующий SDK/MSP bridge и публичные request/command/view API | Читать child без writer lease; управление child через owner-plane, не через присвоение чужой сессии. |
| OpenCode V2 | Location-scoped HTTP, общий connection pool/SSE по service namespace; durable session log для точного readback | V1 `/session/...` и V2 `/api/session/...` — разные контракты; agent catalog — определения агентов, не список запущенных детей. |
| Claude Agent SDK | Долгоживущий query с streaming input/output, штатные permissions/hooks/subagents | Очередной streaming input не даёт сам по себе atomic expected-turn steer. SDK estimated cost не остаток подписки. |
| OpenCodex | Отдельный Management adapter к явно выбранному внешнему proxy | Сессиями/turns управляет harness. Provider routing, auth и spend — отдельные наблюдаемые факты. |
| Command / Antigravity / Zed / Kilo | Пока сохранить границы конкретных ELIOT artifacts; сверить их собственные native contracts до расширения | В этом проходе их новые native API не проверены. Resume-флаг или warm stdin не доказывает exact steer. Batch-возможности не объявлять интерактивными. |

Исходные ELIOT границы: [Codex UPDATE][e-codex], [OpenCode guide][e-oc], [Muse bridge][e-muse], [OpenCodex guide][e-ocx]. Полный паритет всех harness этим исследованием не заявлен.

## 2. Capability описывает гарантию, а не название кнопки

В существующем module descriptor/contract для подключаемой возможности фиксировать: native method, target scope/ID, effect boundary, guard, application timing, replay/readback и observed support. Не создавать второй независимый реестр. Server version, schema revision, declared capability и успешная квалификация — разные факты.

| Нужное действие | Нативный контракт | Отображение в ELIOT |
|---|---|---|
| Точный steer | Codex `turn/steer {threadId, expectedTurnId, input}`; Muse `turn/steer` с его собственной схемой | Только заявленный exact-target класс. Codex steer не принимает model/cwd/sandbox overrides и не создаёт нового turn/started. |
| Доставка на шаге цикла | OpenCode V2 `session.prompt`, `delivery:"steer"`; pending input можно переводить через inbox update | Отдельная семантика от atomic expected-turn: в проверенной V2 форме такого turn guard нет. Не ослаблять существующий exact-steer и не скрывать полезный native вариант. |
| Убрать foreground-блокировку | OpenCode `session.background`; Muse `task/background` | Адресное действие под policy. У Muse taskId — itemId нативного toolCall, не ELIOT Task ID. Background не означает отмену. |
| Остановить сейчас / больше не продолжать | Codex `turn/interrupt` отдельно от `thread/goal/clear` или pause; Muse различает priority interrupt, обычный cancel и goal | Явный запрос владельца: запрет нового продолжения и остановка текущего исполнения — разные эффекты/readback. Disconnect не является stop. |
| Управлять ребёнком | Muse `subagent/sendMessage`, `followupTask`, `interrupt`, `stop`, `close`, `resume`, `reopen` | Parent sessionId + observed durable subagentId и точная роль owner. Не превращать все verbs в `agent.send`. |
| Читать ребёнка | Codex scoped thread listing/read; Muse session/read + view/subscribe | Без автоматического resume/load writer. Partial family не пустая family; статический catalog не runtime roster. |

Источники: [Codex app-server][c-doc], [Muse pinned schema][m-pin], [OpenCode V2 API][o-api]. Native ACK означает соответствующую стадию допуска; terminal, result и Task acceptance подтверждаются отдельно. Не копировать всё руководство в каждый агентский brief: исполнитель получает один выбранный verb и его контракт.

### Практический порядок controls

Сначала обнаружить текущие root/child/native task IDs и причины ожидания. Затем выбрать ровно поддержанный control с нужным guard и сохранить Operation до вызова. После ACK читать адресное доказательство; потеря ответа не разрешает второй input. Если API не даёт идемпотентности или точного readback, удержать unknown, не считать generic HTTP 200 доказательством исполнения.

Для approvals хранить исходный request ID и connection/boot, а также native stage guard. В Muse `requirementId`/`choiceId` защищают этап; `terminal:false` у ack допускает следующий этап. `RequestReceipt {}` — представление, не согласие. В Codex JSON-RPC ответ возвращается исходному request ID; его нельзя после reconnect приписать другому запросу. Политика разрешения задаётся владельцем; подпись варианта «Recommended» не даёт authority.

Native hooks/skills должны оставаться нативными: Muse `skill/list` + typed skill input, Codex native catalogs/configuration, Claude hooks/permission callback. Не копировать весь skill text в повторный системный prompt и не выполнять неизвестный tool от имени оператора. Доступность browser/MCP зависит от конкретного harness/runtime environment; наличие той же модели не переносит инструменты из local в cloud.

## 3. Что уже теряется в наших адаптерах

### Codex standalone Rust — не весь продукт Codex

`NativeClient::receive_response` читает socket во время ожидаемого RPC. Frames с method без id не обрабатываются и пропускаются; request allowlist не включает account API. Поэтому одно добавление `account/rateLimits/read` не подключает поток обновлений. `decline_server_request` явно отклоняет approvals/elicitation и возвращает пустые ответы на пользовательские вопросы. Это нынешняя политика адаптера, не ограничение app-server. [Client source][e-codex-client].

Нужен один непрерывный read-pump: response demultiplexing по request ID; typed notifications; отдельная очередь server requests. Response/control traffic не ждать освобождения очереди bulk text. После разрыва unresolved mutations остаются unknown; новый connection generation не принимает старый pending response. Для quota-среза не менять политику approvals одновременно; для будущего reply-среза нужен полный host attention → native reply путь.

`attach` сейчас объявляет `experimentalApi:false`, но вызывает `thread/items/list` и `thread/turns/list`, которые текущая документация помечает experimental. Это проверка совместимости с поддерживаемым server schema, а не указание включить все экспериментальные функции. Старый server может игнорировать незнакомый capability flag; флаг не доказывает наличие метода. R03/#29 исправляет конкретный history-dependent steer отдельно.

У Python bridge.3 есть child/history возможности, которых Rust descriptor v4 не заявляет. Удалять Python executor можно после конкретного parity/перехода, а не только из-за языка. Сохранение historical reader не требует сохранения старого executor навсегда. [Artifact split][e-codex].

### Muse — не писать второй SDK

Pinned SDK 1.3.0 уже описывает `usage/read`, `usage/changed`, адресные subagent controls, `task/background` и skill catalog. У существующего bridge есть notification callback и запись usage update, но root refresh читает session и pending approvals, не начальную usage snapshot. Доработать существующие callbacks и readback, не запускать inference для «проверки квоты» и не добавлять отдельный poller на каждого ребёнка. [Pinned usage/controls][m-pin] · [Bridge][e-muse].

Сохранять MSP view/gap, sourceRange и re-anchor semantics; raw-log API нельзя выдумывать из одного sourceRange. Бounded overview не заменяет постраничный exact result. `subagent/readResult` находится на command-plane и меняет состояние: для наблюдения использовать существующий view/item result path. Условие «нет прочитанных детей» не означает complete empty.

### OpenCode — использовать существующий богатый код

Built-in `runtime/opencode_v2` уже реализует forms, permissions, background, инструкции, agent/model readback и execution log. Standalone adapter не является полным паритетом этого пути. Переносить проверенные единицы, не писать третий экземпляр. [ELIOT OpenCode guide][e-oc].

Native V2 prompt может запланировать выполнение; `resume:false` и очередь имеют свой эффект. Inbox исчезновение не доказывает успешный terminal. Связывать exact input ID с durable log; для volatile SSE gap нужен readback. `/api/session/active` описывает foreground execution, не все живые background jobs/descendants. Session delete удаляет детей, location reload отменяет pending interactions — не использовать их как refresh/stop. Полной provider quota endpoint в исследованной V2 ссылке не установлено; локальные session token statistics не подставлять вместо неё. [V2 API][o-api].

## 4. Квота: хранить разные оси отдельно

| Источник | Чтение и push | Поля / ограничения |
|---|---|---|
| Codex app-server | `account/read` без refreshToken; `account/rateLimits/read`, `account/rateLimits/updated`; `account/usage/read` там, где supported | Bucket limitId, primary/secondary usedPercent/windowDurationMins/resetsAt. `resetsAt` — секунды. Credits hasCredits/unlimited/balance отдельно; balance — строка, не объявлять USD. Новые optional spend controls учитывать как reported/unknown, не как false по умолчанию. |
| Muse MSP pin 1.3.0 | `usage/read`; `usage/changed` | `{usage?}`: отсутствует → no observation. tier, weekly/window.usedPercent, resetsAtMs, windowDurationMins, observedAtMs. Reset в миллисекундах; процент может быть >100. Это last-observed subscription snapshot, не live token meter. |
| Claude Agent SDK | result usage/modelUsage/total_cost_usd | Usage main loop и cumulative call/model totals имеют разный scope; resumed call может включать прошлую стоимость. Стоимость SDK — estimate, не billing invoice и не subscription balance. Не суммировать каждый cumulative result плюс дочерние totals. |
| OpenRouter | `GET /api/v1/key`; отдельно `/api/v1/credits` с management key | Key cap и account funds различны. Проверять limit_source/error metadata и headers. HTTP 429 не доказывает quota exhaustion; 404 не всегда ModelGone. Management credential не выдавать агенту и не извлекать из native auth store. |
| OpenCodex | Management observations, reported provider quota | Схему и внутренний эффект конкретного read проверить по источнику; quota read может быть refresh/auth action, см. ниже. Routing affinity и auth mode не выводятся из model name. |

Источники: [Codex current rate-limit type][c-quota], [credits type][c-credits], [official app-server][c-doc], [Muse schema][m-pin], [Claude costs][a-cost], [OpenRouter limits][r-limits]/[credits][r-credits]. Новые поля текущего Codex не объявлены доступными у установленного .159 server; нужны per-capability availability и fallback только к доказанной старой read-форме, без смены auth/model.

### Правила нормализации

- Не объединять quota buckets по одному названию модели/провайдера. Источник: server/service namespace + наблюдаемый auth/account context + bucket/window. Если общая account identity не доказана, не суммировать такие snapshots и не утверждать точную дедупликацию.
- Native usedPercent, observedAt и collectedAt — разные данные. Новый GET старой snapshot не делает её свежей. Изменение/reset подтверждать источником, а не только настенными часами.
- Обновление одного bucket не удаляет остальные; явный null внутри текущей полной формы означает unknown, а не сохранение прежнего якобы актуального значения. После auth change прежние данные исторические.
- Token usage, цена, остаток денег, subscription window, concurrency slots, throttling и earned reset credits — разные величины. 100% окна не всегда запрещает работу при наличии отдельно разрешённых credits; доступность расходования не означает разрешение ELIOT их тратить.
- RateLimited, QuotaExhausted, AuthRequired, ModelUnavailable, DataPolicyRequired, Overloaded и Unknown различать по структурированным признакам конкретного вендора. Retry-After — подсказка ожидания, не доказательство no-effect; не повторять unknown prompt.
- В `capacity::note_outcome` сейчас Accepted/Applied закрывает quota incident независимо от конкретного bucket/новой usage evidence. Исправить этот узкий consumer, не переписать resource ledger и не объявлять локальный успешный read доказательством восстановления лимита. [Source][e-capacity].

## 5. Внешний GET не всегда пассивное наблюдение

OpenCodex current source `250f17a` связывает `/api/provider-quotas` с provider collectors. Muse collector `fetchMuseKeyQuotaSnapshot` вызывает `mintMuseApiKey`, а force refresh и reset poller обсуждаются как его callers. Код ограничивает успешные/неуспешные mint по времени и объединяет concurrent calls, но это всё равно auth-plane mint, не простой cached GET. **Не подключать этот путь автоматически к read-only report.** Не переносить чужую заявленную «read» классификацию без проверки вызываемого эффекта. [Collector][x-mint] · [Router][x-router].

Сначала использовать MSP last-observed usage либо endpoint с проверенной cached-only гарантией; в отсутствие такого endpoint показывать unavailable/stale. Явный auth-refresh, если он действительно нужен продукту, оформляется отдельной разрешённой Operation, не скрытым вызовом из Doctor. GET не даёт общего разрешения читать секреты, менять account pool или обходить лимит другого аккаунта.

OpenCodex README также оговаривает исключения из thread/account affinity при failover/исключении/expiry/ошибках. Поэтому ELIOT хранит requested provider отдельно от реально reported account/wire/served-model и не обещает вечную pinned affinity. [Upstream README][x-readme]. Существующий ELIOT Management adapter остаётся detach-only; auto pool rotation и оплату этот план не включает.

Codex `account/rateLimitResetCredit/consume` расходует отдельный earned reset credit; `account/sendAddCreditsNudgeEmail` отправляет письмо. Ни то ни другое не quota-read. Не вызывать автоматически для прохождения gate. Claude authentication для интегрируемого SDK должна соответствовать официальной документации и доступному разрешению, не extraction личных credentials. [Codex account API][c-doc] · [Claude SDK overview][a-overview].

## 6. Порядок поставки

1. **R03/#29:** исправить exact Codex steer, не ожидая всего нового quota/control слоя.
2. **R15:** native quota snapshots Codex/Muse → bounded validated projection → авторизованный manager read, плюс прекращение ложного quota recovery. Полное задание: [15-native-usage](../remediation/2026-10-07/15-native-usage.md).
3. **Адресный control parity:** после source/schema проверок доводить по harness один законченный verbs→attention/receipt→readback блок. Для Muse — owner-plane children/background; для Codex — pending questions и штатные configuration fields; для OpenCode — отдельно заявленный loop-step delivery с existing forms/background/log, не R02 journal repair.
4. **Quota-aware admission/route change:** отдельное принятое правило владельца после появления достоверных snapshots. До этого не включать billing actions, failover, session rotation или arbitrary manager limits. Старый полевой пример «2 менеджера/7 детей» не native hard limit.

Один manager/worktree; source docs → код → минимальный scoped gate, широкие tests/native позже. Приёмка каждого блока проверяет отсутствие input replay, exact target, auth change, stale read, disconnect, gap и mismatch. Данный документ не меняет SDK pins, активные конфигурации, процессы или owner-decisions.

## Проверенные источники

[e-codex]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/codex/UPDATE.md
[e-codex-client]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-codex/src/lib.rs#L1598-L1830
[e-oc]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/opencode/README.md
[e-muse]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f269b3d4dea2c5e15754feba9971b1f1c1b20c2b/modules/muse/bridge.mjs
[e-ocx]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/opencodex/README.md
[e-capacity]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-kernel-host/src/store/capacity.rs#L600-L750
[c-doc]: https://learn.chatgpt.com/docs/app-server
[c-quota]: https://github.com/openai/codex/blob/82e70121f86bc1f6fea7f2bb7bbc169d259b3c6b/codex-rs/app-server-protocol/schema/typescript/v2/RateLimitSnapshot.ts
[c-credits]: https://github.com/openai/codex/blob/82e70121f86bc1f6fea7f2bb7bbc169d259b3c6b/codex-rs/app-server-protocol/schema/typescript/v2/CreditsSnapshot.ts
[m-pin]: https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/msp.d.ts
[m-current]: https://github.com/meta-models/muse-code-sdk/blob/912061bb125b4bff60c9f3089a21cd23b53c6b4f/schema/msp/msp.d.ts
[o-api]: https://opencode.ai/v2/docs/api
[a-overview]: https://code.claude.com/docs/en/agent-sdk/overview
[a-stream]: https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode
[a-cost]: https://code.claude.com/docs/en/agent-sdk/cost-tracking
[r-limits]: https://openrouter.ai/docs/api_reference/limits
[r-credits]: https://openrouter.ai/docs/api/api-reference/credits/get-remaining-credits
[x-readme]: https://github.com/lidge-jun/opencodex/blob/250f17afd8ff44c93c620d87c1f346ef56f64fb4/README.md
[x-router]: https://github.com/lidge-jun/opencodex/blob/250f17afd8ff44c93c620d87c1f346ef56f64fb4/src/server/management/provider-routes.ts
[x-mint]: https://github.com/lidge-jun/opencodex/blob/250f17afd8ff44c93c620d87c1f346ef56f64fb4/src/providers/muse-key-quota.ts
