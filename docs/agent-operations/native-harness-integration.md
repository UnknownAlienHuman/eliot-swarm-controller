# Нативные harness: подключение по контракту, без второго model loop

**Перепроверено 08.10.2026.** Это план интеграции и справочник доказательств, не матрица уже реализованных функций. Нормы ELIOT задают authority/ownership; официальный протокол производителя задаёт смысл native API. Скрипты прежней кампании не являются спецификацией. Внешние версии не закреплять, обновления не отключать, подписочное подключение отдельным inference API не заменять. SHA источника — координата проверки, не условие запуска.

## 1. Источники и совместимость

Читать [Module contract](../agent_swarm.module-contract-v2.md), [Modularity](modularity.md), [Owner decisions](../owner-decisions.md), затем официальные docs/schema нужного метода и реальные определения используемого SDK. Реестр доноров — inventory. Проверять актуальный selected runtime через документированный handshake/readback; не придумывать универсальный endpoint discovery.

Не склеивать разные публикации в одну якобы установленную схему. В этом проходе рассмотрены Codex `ea27864f99f0b086cec2f9f0251b7190fb9844f1` и Muse `912061bb125b4bff60c9f3089a21cd23b53c6b4f`; live Muse `/next` показывает также более широкую поверхность. Ссылка из `/next` не доказывает, что её новые методы есть в указанном stable source или на машине владельца. Отдельно записывать source revision, документированную стабильность и наблюдённую поддержку.

Совместимое обновление не отклоняется из-за номера release. Неизвестные optional поля не запрещают независимые функции. Отсутствие обязательной гарантии ограничивает конкретную операцию. Новую сессию подключать к текущей выбранной установке; уже работающую сессию не перезапускать и её runtime не подменять.

## 2. Общая граница ELIOT

ELIOT сохраняет свою Operation до внешнего эффекта; проверяет caller, binding/generation, native target, source/boot и current authority; различает ACK, сохранённый input, terminal, result и Task acceptance. Unknown разрешается readback, не повторным prompt. Native sessions, tools, permissions, skills, children и model loop принадлежат harness.

Для функции описывать native method, exact target, обязательные поля/guards, эффект, ACK boundary, readback, replay policy, freshness. Это содержание существующего descriptor/контракта, не новый независимый DSL или реестр. Vendor-only функции остаются `native.*`; общее свойство ядра вводится только для реальных потребителей.

| Действие | Не смешивать |
|---|---|
| Next turn / queue / loop-step steer / exact-target steer | Очередь не текущий turn; atomic target guard не гарантия durability. |
| Background / interrupt / cancel / retract | Background не отменяет; ACK stop не доказывает departure. |
| Goal / обычный input | Продолжение цели может жить после disconnect клиента. |
| Reply существующему вопросу | Не новый prompt и не разрешение на persistent policy change. |
| Child control / family observation | Наблюдение не получает writer ownership ребёнка. |
| Usage / quota / credits / rate-limit | Расход, остаток, throttle и право оплатить — разные оси. |

## 3. Codex: app-server и три ступени steer

Один adapter-owned connection reader разделяет `response{id}`, `notification{method}` и `request{id,method}`. Ответы, terminal и вопросы не блокируются очередью bulk deltas. Новый connection generation не принимает старые ответы; reconnect не повторяет mutation. Account updates идут тем же reader, без quota-only клиента на каждый вызов.

**Нативная граница:** `expectedTurnId` атомарно защищает цель `turn/steer`, но RPC отвечает раньше hooks, rollout persistence и sampling. `clientUserMessageId` связывает наблюдённый input с запросом; его наличие само по себе не гарантирует дедупликацию повторной отправки. Успешный ACK с нужным turnId может предшествовать сохранению user item.

```text
ACK steer → решение принять в указанный turn
exact persisted user item + correlation/content → доказанная доставка
terminal этого turn → исход исполнения, не приёмка Task
```

В ELIOT `reconcile_send` уже ищет unique exact input и сверяет digest/bytes/turn. Сохранять это; в R03 убрать лишний `active_turns` history-preflight, а не post-send readback. `accepted_after_exact_input` сохраняет различие доставленного input и неизвестного terminal. История, недоступная из-за неподходящего handshake, не является доказательством отсутствующего input. Подробнее [R03](../remediation/2026-10-07/03-codex-steer.md).

Steer не принимает model/cwd/sandbox/outputSchema overrides нового turn и не создаёт обязательный новый `turn/started`. `turn/interrupt` и `thread/goal/clear` — раздельные намерения. Sandbox/approval policy и текущая подписочная авторизация не меняются ради починки transport.

**Квоты:** `account/rateLimits/read` — snapshot, `account/rateLimits/updated` — sparse notification. `limitId`/bucket, окна, credits, spend controls и plan metadata не смешивать. Null metadata в rolling update не обязательно стирает прошлое; null в полном read и смена auth context имеют другой смысл. Refetch разрешает неоднозначность без model call. Алгоритм по полям — в [R15](../remediation/2026-10-07/15-native-usage.md).

Источники: [app-server docs](https://learn.chatgpt.com/docs/app-server), [TurnSteerParams](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/schema/typescript/v2/TurnSteerParams.ts), [Core boundary](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/core/src/session/turn_input.rs#L1-L13), [rolling notification](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/schema/typescript/v2/AccountRateLimitsUpdatedNotification.ts). [#40805](https://github.com/openai/codex/issues/40805) — чужой reproduction, не наш live-прогон.

## 4. Muse: не превращать grantedCapabilities в список RPC

**Исправление прежнего AUD-047:** правило «подключать только методы, имена которых есть в grants» неверно. В рассмотренном stable source `CapabilityName` называет `userShell`, `sessionMcp`, `sessionListStream`, `feedback` и допускает расширение. Это именные привилегии, а не перечень всех методов. Отсутствующий объект client capabilities означает defaults.

Различать четыре оси:

| Ось | Проверка |
|---|---|
| Базовый метод и wire-форма | Реальный метод/ответ текущей поверхности; не `method ∈ grantedCapabilities`. |
| Именная дополнительная привилегия | Запрашивать/проверять именно её там, где официальный контракт метода/поля её требует. |
| Experimental surface | Отдельный `experimentalApi` и требуемые формы; не выводить opt-in из версии пакета или имени grant. |
| Возможность клиента показать вопрос | `userInputDialogs`: absent означает capable; это прямо **не** grant. |

Для `usage/read` и обычного `turn/steer` не добавлять придуманные grants с такими именами. Пустой список именных grants сам по себе не делает эти методы unavailable. Сохранение grant snapshot полезно для реально потребляемых privilege-gated extensions, но текущий аудит не доказал, что отсутствие этой проекции ломает базовую квоту или steer. Исходное общее P1-обвинение снято, а не объявлено исправленным кодом.

Полный SDK остаётся владельцем MSP parsing/fold. Existing `launchConnection/onNotification` подключить к initial `usage/read` и `usage/changed`. Одинаковая SubscriptionUsage-форма; absent usage — not_observed; observedAtMs не обновляется повторным чтением; weekly/window раздельны; >100% допустимо. Не создавать child poller, второй SDK или новые auth/model calls.

Документированные controls: exact `turn/steer`, priority `turn/interrupt`, normal-lane `turn/cancel`, goals, адресные `subagent/*`, `task/background|stop`, model/skill catalogs. Native taskId инструмента — не ELIOT Task; parent session + subagentId — не новый control binding ребёнка. `subagent/readResult` — command-plane, не безобидное чтение observer. Решение approval остаётся отдельным от `{}` RequestReceipt.

### SDK как донор завершения, не ещё одна самодельная машина

Публичный `Session.turn(id).completed` возвращает `TurnOutcome`: `completed`, `unqueued`, `terminalUnknown`. У reclaimed queued turn не будет `turn/completed`; ожидание только этого события зависнет. `isLaunchFailure` проверяет native failed/launchError, а не отсутствие замеченного turn/started: клиент мог подключиться позже.

Использовать facade `Session`/`Turn` целиком в подходящем control slice, а не импортировать private `TurnHandle` mutators или копировать его ветки в Store. Correlation с ELIOT Operation остаётся нашим обязательством. В прочитанном SDK `PushStream` имеет растущий buffer без byte cap: один постоянно читающий bridge consumer и bounded downstream fanout лучше iterator на каждого медленного viewer. При завершении наблюдения закрывать iterator (`return()`/выход из for-await); это не отмена native turn. Отмена и retirement очереди не входят в quota PR R15.

Источники: [stable declarations, CapabilityName/ClientCapabilities](https://github.com/meta-models/muse-code-sdk/blob/912061bb125b4bff60c9f3089a21cd23b53c6b4f/schema/msp/msp.d.ts#L238-L255), [TurnOutcome/Turn facade](https://github.com/meta-models/muse-code-sdk/blob/912061bb125b4bff60c9f3089a21cd23b53c6b4f/clients/sdk-ts/src/facade/turn-handle.ts#L1-L230), [официальный queue/steer cookbook](https://meta-models.github.io/muse-code-sdk/next/cookbook/queue-steer-and-reclaim-turns/), [usage/read](https://meta-models.github.io/muse-code-sdk/next/generated/msp/methods/usage-read/). Новые `/next` поля отдельно сверять с действующим подключением.

## 5. OpenCode: богатый V2 HTTP, ACP — отдельная топология

Для выбранного shared service использовать прямой HTTP: durable prompt admission с `delivery=steer|queue`, отдельные forms/questions/reply, background, сообщения/context/instructions и адресный log readback. Loop-step steer не имеет Codex expected-turn CAS и не должен его изображать; это не причина запрещать полезную native функцию. Background не cancel, warming с model call не healthcheck.

`opencode acp` поднимает private server внутри ACP process, а не прозрачно подключается к shared service binding. Выбор topology явный. Прямой adapter не вызывает service-discovering CLI как health probe. Forms/background/log primitives уже есть в `runtime/opencode_v2`: переносить проверенный путь в standalone adapter и удалять дубль после переключения callers, не писать третий.

Источники: [V2 API](https://dev.opencode.ai/v2/docs/api/), [prompt](https://dev.opencode.ai/v2/docs/api/session/v2-session-prompt/), [background](https://dev.opencode.ai/v2/docs/api/session/v2-session-background/), [ACP](https://opencode.ai/v2/docs/cli/acp/). Session token/cost — не остаток provider account. Новый machine-readable quota collector здесь не доказан.

## 6. Claude: live callback и persisted defer — разные режимы

Сохранять выбранную подписочную авторизацию. `ANTHROPIC_API_KEY` может переключить её на API billing; subscription route не инъецирует такой override или другой endpoint. Незаданный permission mode отражать как inherited до native effective readback, не присваивать default по памяти о старом SDK.

Официальный permission path различает hooks, deny, ask, mode, allow/native auto-approval и unresolved `canUseTool`. Callback не вызывается для всякого tool; требуемая проверка до инструмента использует соответствующий `PreToolUse` hook, без самовольной смены permission policy.

В [R16](../remediation/2026-10-07/16-claude-interactions.md) live callback хранит resolver в живом Node и разрешается один раз через scoped agent.reply. Official durable defer использует native persisted session/resume, когда поддержан; он не восстанавливает JavaScript Promise. Driver loss, host disconnect и abort различны. AskUserQuestion возвращает original questions + answers, не новый prompt; документированные ограничения subagent support не обходить названием callback. Allow-always меняет политику и не включён в первый reply slice.

Источники: [permissions](https://code.claude.com/docs/en/agent-sdk/permissions), [user input/defer](https://code.claude.com/docs/en/agent-sdk/user-input), [environment](https://code.claude.com/docs/en/env-vars). В этом проходе Claude заново не квалифицирован; сохраняется scope R16.

## 7. Command: интерактивный ACP и headless — разные профили

Official `cmd acp` предоставляет session/tools/skills, permissions/questions, model/effort, streamed events, context meter, reopen/close и использует existing login. Один процесс — один project directory и несколько threads; это не разрешает перемешать разные worktrees. Файловые действия контролирует native permission engine, не воображаемая ACP client sandbox.

`cmd -p` также имеет durable session и continue/resume; не называть его sessionless. Для интерактивного manager выбирать профиль с нужными controls; batch/CI может использовать headless. Новый `-p --resume` input не является exact steer. ACP generic prompt сам по себе также не доказывает expected-turn CAS или replay-safe submission.

Источники: [ACP](https://commandcode.ai/docs/acp), [sessions](https://commandcode.ai/docs/sessions), [headless](https://commandcode.ai/docs/headless), [permissions](https://commandcode.ai/docs/permissions). Это основание будущего whole-backend slice, не требование оборачивать остальные harness в ACP.

## 8. Antigravity: один поток и накопительная usage

Official stream-json даёт init/step_update/result и tool/subagent сведения, при input-format stream-json обслуживает несколько turns. Чтение stdout постоянно; EOF stdin означает выбранное владельцем завершение session, а не probe. Модель/effort/agent/permission поступают из выбора владельца и сверяются с native init; REQUIRED_MODEL_ID нельзя заменить следующей константой.

Usage/turns/duration накопительны: заменять snapshot или вычислять проверенную разность одного поколения, а не суммировать results. Slash `/usage` и `/model` не посылать в рабочий JSON-поток. Interactive credits UI не доказывает machine-readable balance; отсутствие quota поля не отключает runtime и не переводит его на другой SDK/API-account.

Источники: [headless](https://www.antigravity.google/docs/cli/headless/), [credits](https://www.antigravity.google/docs/cli/credits/). Новые API Kilo/Zed и их паритет этим проходом не подтверждены.

## 9. Доноры и границы повторного использования

| Единица | Применить | Ограничение |
|---|---|---|
| Muse публичные Session/Turn/fold | Native lifecycle, queued retirement, late attach, unknown host death | Не private mutators; не durable Store и не bounded UI-buffer автоматически. |
| Codex `merge_rate_limit_fields` | Образец раздельной обработки окон и nullable metadata | Private Core helper: не импортировать весь core. В нём нет ELIOT auth/bucket scope guard; универсальный merge из него не следует. |
| ACP Rust SDK/Conductor/TCK, ACPX | Целая подходящая ACP-единица для реального consumer | Не обязательный proxy для MSP/app-server/shared HTTP, не новая Task authority; ownership/kill policy проверять отдельно. |
| RMCP | ELIOT MCP frontend, immutable schema reuse | Не native model control plane; schema cache не authority cache. |
| CCCC/Paseo | Locality inbox, owner наблюдательной подписки | Не второй ledger и не отмена Task на disconnect. |
| Restate/DBOS | Correlation до эффекта, memo/readback boundaries | Не новый engine; внешний эффект до записи результата остаётся возможен. |
| Tokio | Owned channels и cancel-safe wait | Не автоматическая kill/restart policy. |
| Kingfisher/Atlas | Bounded diagnostic detection/redaction | Не permission boundary; не raw unlimited transcript в Store writer. |

Codex donor source: [set_rate_limits и merge_rate_limit_fields](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/core/src/state/session.rs#L350-L462). Остальные подробные source reviews и пределы заимствования — в едином аудите/реестре, не независимые новые требования установки.

## 10. Порядок поставки и критерии

R03: убрать history-preflight и согласовать нужный readback. R15: continuous reader Codex, два quota producers, Store/reader, incident. R16: Claude loader/auth и reply. Далее законченные OpenCode controls и Command ACP slices; Antigravity model/usage correction. Управление маршрутом/кредитами — отдельная policy после достоверных фактов, не побочный эффект чтения.

Каждая функция имеет реального caller в той же поставке и проверяет exact target, generation, missing capability, unknown outcome, late event, retained question и observer disconnect. Свой input хранить до эффекта; не вводить новый model loop, эвристику по приватной БД или универсальный retry.

После целого кода — scoped gate менеджера; broad/native qualification в итоговой фазе. Настоящая редакция меняет документацию, не callbacks, subscriptions, SDK dependencies, credentials, процессы или main.
