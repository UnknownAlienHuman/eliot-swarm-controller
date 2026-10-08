# Нативная интеграция harness: управление, наблюдение и квоты

**Обновлено 7 октября 2026.** ELIOT `40591a295af94b1541ec2ba30afe8e3247701a71`. Исследование source/docs, не qualification установленного runtime и не проверка пользовательских аккаунтов. Различать native capability, реализацию выбранного ELIOT artifact и фактически выполненный gate. Версии публичных документов не дают автоматического права менять SDK, auth или работающую сессию.

**Решение:** нативный model loop, инструменты, история и делегирование остаются у harness. ELIOT владеет своими Task/Attempt, Operations, полномочиями и evidence внешнего эффекта. Не сводить всё к send(text), не строить второй runtime и не открывать произвольный vendor RPC passthrough.

## 1. Как подключаться

| Система | Граница интеграции | Существенное ограничение |
|---|---|---|
| Codex | Одна долгоживущая app-server connection, RPC replies + notifications + server requests | Batch exec и provider proxy не заменяют владельца thread. |
| Muse | Имеющийся SDK/MSP bridge | Child read без writer lease; owner-plane control с нативными IDs. |
| OpenCode V2 | Location-scoped HTTP, общий pool/SSE на service namespace, durable log | V1 и V2 не взаимозаменяемы; agent catalog не runtime roster. |
| Claude | Существующий Rust adapter + Node SDK driver, один живой streaming Query | SDK heap/Promise остаются в Node; настройки и callback coverage нужно учитывать явно. |
| Command | Сохранить batch artifact; для интерактивного сценария исследован публичный cmd acp | ACP-путь — кандидат, не уже реализованный ELIOT runtime. См. §5 и gate #19. |
| Antigravity | Существующий CLI warm stream-json | Не управляющий JSON-RPC; Python SDK с API auth — отдельный продуктовый маршрут. |
| OpenCodex | Management adapter к явно выбранному proxy | Управляет провайдерами/конфигурацией, не нативными Task/turns. |
| Zed / Kilo | Пока существующие artifact-контракты | Их новые интерфейсы этим проходом не квалифицированы. |

Исходные границы: [Codex][e-codex], [Muse][e-muse], [OpenCode][e-oc], [Claude][e-claude], [Command][e-command], [Antigravity][e-agy], [OpenCodex][e-ocx]. Не удалять старый executor до конкретного parity; возможность читать старые receipts сама по себе не требует сохранять executor навсегда.

## 2. Контроль — это точный эффект, а не похожее имя

В существующем descriptor/contract для каждого verb нужны native method, target ID/scope, effect boundary, guard, replay/readback и доказанная поддержка. Новый registry не нужен.

| Действие | Нативная семантика | Что обязан сохранить адаптер |
|---|---|---|
| Exact steer | Codex turn/steer с expectedTurnId; Muse turn/steer по его схеме | Guard caller target; нельзя выбирать последний ход вместо него. Codex не создаёт новый turn/started и не принимает start-only overrides. |
| Loop-step delivery | OpenCode V2 session.prompt delivery:steer | Не обещать atomic expected-turn guard, которого проверенная форма не содержит; не выбрасывать сам полезный способ доставки. |
| Background | OpenCode session.background; Muse task/background | Адресный supported tool/task, не отмена. Muse taskId — native tool item, не ELIOT Task ID. |
| Stop continuation / interrupt | Codex goal clear/pause отдельно от turn/interrupt; Muse разные cancel/interrupt/goal verbs | Остановка текущей работы и запрет следующей — разные эффекты. Disconnect не stop. |
| Child control | Muse subagent/sendMessage, followupTask, interrupt, stop, close, resume, reopen | Parent session + observed durable subagentId; не присвоение чужой session writer lease. |
| Question/permission reply | Muse stage IDs; Claude live callback; Codex исходный server request | Ответ именно ожидающему запросу; представление карточки и принятие решения не одно. |

Источники: [Codex app-server][c-doc], [Muse pin schema][m-pin], [OpenCode V2][o-api], [Claude callback][a-input]. Native ACK не является terminal, result completeness или Task acceptance.

Общий порядок: проверить target/authority → записать exact Operation → вызвать поддержанный verb → прочитать адресное evidence. Lost reply не разрешает новый input. Имя «Recommended» не право на автоматический ответ. Native hooks/skills и MCP configuration использовать на штатном уровне; не копировать весь каталог/skill body в каждый prompt. Browser/MCP возможности определяются конкретной средой harness, не только названием модели.

## 3. Что исправлять в существующих адаптерах

### Codex: один читатель соединения

`NativeClient::receive_response` в standalone Rust читает socket только при ожидаемом RPC и пропускает no-id notifications; account API нет в allowlist. `decline_server_request` отказывает approvals/elicitation и возвращает пустые user answers. Это adapter gap, не предел app-server. [Источник][e-codex-client].

R15 подключает owned continuous reader с demux по request ID, выбранными notifications и отдельным server-request каналом. Control/terminal сообщения не ждут bulk text. Старые unresolved mutations после разрыва остаются unknown; новый connection generation не принимает старый response. Не менять политику approvals одновременно с quota transport.

`attach` объявляет experimentalApi:false, но вызывает history methods, обозначенные experimental в текущей документации. Проверить выбранную schema; не включать все экспериментальные функции вслепую. R03/#29 устраняет лишнюю зависимость steer от полной истории отдельно. Python bridge.3 и Rust v4 имеют разный parity; замену исполняющего пути завершать по возможностям, не по языку.

### Muse: initial usage на существующей connection

В pin SDK 1.3.0 уже есть usage/read, usage/changed, background и child controls. Bridge имеет notification callback, но root refresh читает session/pending, не initial usage. Добавить один readback-путь без нового SDK, polling каждого child и inference. Native view/gap/sourceRange сохранять как evidence; не выдумывать raw-log endpoint. subagent/readResult — command-plane, для наблюдения использовать existing view/item result path. [Schema][m-pin], [bridge][e-muse].

### OpenCode: не писать третий вариант

Built-in runtime/opencode_v2 уже реализует forms, permissions, background, инструкции, readback выбранного агента/модели и execution log. Standalone adapter — неполный parity. Переносить работающие единицы вне host, не повторять их с нуля. [Реализация][e-oc].

Input ID связывать с durable log; исчезновение inbox entry и foreground idle не доказывают завершение. /api/session/active не полный список background jobs. Session delete и location reload имеют побочные эффекты — это не refresh. Provider quota endpoint в рассмотренной V2 справке не установлен; session tokens не заменяют quota. [V2 API][o-api].

### Claude: реальный default и живой callback

**AUD-045:** наш pin — 0.3.287. NativeOptions.permission_mode допускает None, оба Node driver опускают незаданный option, а modules/claude/README.md называет это default. Официальные документы указывают изменение с 0.3.286: отсутствие поля допускает native выбор режима, включая settings/auto. Поэтому requested=inherited и observed effective mode различать; не утверждать default по отсутствию параметра. Это code/doc mismatch, не наблюдавшийся обход полномочий. [Код][e-claude-config], [driver][e-claude-driver], [native modes][a-permissions].

Нынешний canUseTool сразу отказывает; Rust descriptor не заявляет agent.reply, а Node handle знает только prepare/send/stop. Удерживать разрешённый pending Promise у существующего Node-владельца и отвечать через него — естественнее, чем новый prompt или новый permission engine. Но ранее auto-approved инструменты callback минуют, а dontAsk не передаёт ему ожидающие вопросы; callback не универсальный firewall. [Driver][e-claude-driver], [descriptor][e-claude-module], [SDK flow][a-permissions].

Выделен [R16 / PR #42][r16]: pending request → scoped attention → exact agent.reply → один callback; AbortSignal, late reply и driver loss имеют явные исходы. AskUserQuestion сохраняет исходные questions и карту answers; это не streaming-message shortcut. Поддержку вопросов subagents от Agent tool текущая документация исключает. Полный sdk.d.ts этого прохода не извлечён, поэтому используемые fields/execution details исполнитель сверяет с pin перед кодом. [Input contract][a-input].

## 4. Квота, расходы и доступность — разные оси

| Источник | Доступные данные | Что не выводить автоматически |
|---|---|---|
| Codex | account/read без forced refresh; rateLimits/read и updates; optional account/usage при поддержке | limitId buckets отдельно; resetsAt в секундах; credits balance строка без угаданной валюты. Старому server новые поля не обязательны. |
| Muse | usage/read + usage/changed, weekly/window, native observedAtMs/resetsAtMs | Нет usage ≠ ноль. Процент >100 допустим. Новое чтение старого snapshot не делает его свежим. |
| Claude | SDK usage/modelUsage/total_cost_usd | Cumulative estimate не invoice и не subscription balance; не суммировать parent/children/cumulative snapshots повторно. |
| Command | ACP context/cost; CLI /usage показывает лимиты/остатки | Context meter не subscription quota API. См. неоднозначность docs в §5. |
| Antigravity CLI | Per-step usage и cumulative session result usage | Не сумма всех result + всех step values. CLI /usage не команда рабочего JSON-потока. |
| OpenRouter | key limits отдельно от account credits с management credential | HTTP 429 не доказательство exhaustion, HTTP 404 не обязательно ModelGone. |
| OpenCodex | Reported provider observations | Внутренний collector может выполнять auth effect, см. ниже. |

Sources: [Codex types][c-quota]/[credits][c-credits], [Muse][m-pin], [Claude costs][a-cost], [Command][q-usage], [Antigravity stream][g-headless], [OpenRouter limits][r-limits]/[credits][r-credits]. Числа текущего аккаунта этим документом не получены.

Нормализация: source namespace + проверенный auth context + bucket/window; без доказанного общего account нельзя ни суммировать budgets, ни обещать точную дедупликацию. Update одного bucket не стирает остальные, explicit null не превращается в current zero; auth change делает прежнюю запись исторической. Отдельно native observed time, collected time, completeness и support. Token usage, денежный остаток, throttling, concurrency и spend permission не взаимозаменяемы.

`capacity::note_outcome` сейчас закрывает quota incident от Applied/Accepted без evidence восстановления нужного окна. R15 меняет этот узкий consumer; resource ledger R13 не переписывает. Наступивший reset_at — причина прочитать состояние, не доказательство восстановления. [Source][e-capacity].

### Внешнее чтение может иметь скрытый эффект

OpenCodex `fetchMuseKeyQuotaSnapshot` вызывает mintMuseApiKey; rate limits и объединение concurrent calls не делают mint пассивным. Такой collector не вызывается из read-only Doctor/agent.usage. Предпочтительны MSP last-observed usage или проверенный cached-only endpoint; иначе unavailable/stale. [Router][x-router], [collector][x-mint].

Codex reset-credit consume расходует отдельный credit; add-credits email отправляет письмо. Не включать их как восстановление collector. Provider proxy может менять affinity при failover; requested/wire/served/account — разные evidence, не вечная гарантия pinned account. Не выдавать management credential модели и не читать private auth store ради квоты. [Codex account API][c-doc], [OpenCodex][x-readme].

## 5. Command: конкретный ACP-кандидат, не предположение о batch

Публично документирован cmd acp: initialize → session/new(cwd) → session/prompt, поток tool updates, permission/questions, model/effort и повторное открытие thread. Процесс обслуживает одну project directory; файл/terminal IO выполняет собственный engine, не услуги editor. Поэтому different manager worktrees нельзя бесконтрольно объединять одним process owner, а клиентские file callbacks не являются sandbox. Эти сведения из live docs, не protocol/runtime qualification ELIOT. [ACP][q-acp].

**Предложение для выбранного сценария:** Manager ELIOT управляет интерактивной Command-сессией в своём worktree через этот native ACP. Это конкретный кандидат consumer для [issue #19][acp-gate], но gate не снят автоматически: принятой программой требуется проверка полного ACPX v0.19.4 на указанном там SHA. Не подменять её уже исследованным Rust Conductor и не строить второй generic ACP runtime. Наличие native server ещё не подтверждает exact steer или идемпотентный replay prompt.

Batch остаётся полезным для одиночной работы. --resume <id> выбирает exact history, а --continue может выбрать последнюю cwd session или начать новую при отсутствии прежней — не использовать как восстановление неопределённого эффекта. Возобновление через новый процесс не доказывает управление активным ходом. [Headless][q-headless].

Quota docs показывают rolling windows, extra credits и UI /usage, но Go table расходится с пояснением: 2/5 против 3/6. **Не зашивать ни один набор чисел в контроллер по этой странице.** Machine-readable account endpoint в рассмотренных страницах не подтверждён. ACP cost/context не принимать за баланс. Не пробовать неизвестные slash commands как harmless quota probe: ACP может передать их модели. [Usage][q-usage], [ACP][q-acp].

## 6. Antigravity: warm-stream не переносимый control bus

Headless --input-format stream-json исполняет по одному полному ходу на user input. EOF завершает процесс после текущего хода, не немедленно. control_request/control_response и slash commands вроде /usage или /model в этом потоке не поддерживаются и заканчивают сессию ошибкой; их нельзя использовать как probes. Response относится текущему ходу, usage/num_turns/duration — cumulative session. [Native stream][g-headless].

Штатный CLI /usage имеет backend refresh, но это другой интерфейс. Не выводить из его наличия стабильный passive JSON endpoint и не включать CLI-зонд в Store reader. Отдельный native root/step/subagent_info можно наблюдать; нет доказательства, что любой log_uri разрешено открывать или что строка результата — exact native turn ID. [Quota command][g-usage].

Python google-antigravity SDK — новый agent runtime; quickstart использует Gemini API key, Enterprise — GCP/Vertex auth. Это **не** attach к имеющейся subscription CLI conversation. Перевод пользователя на него изменяет auth/execution route и требует отдельного запроса/контракта; ради дополнительных controls такой перевод не выполнять. Сохранять текущий warm CLI там, где он достаточен. [SDK][g-sdk].

## 7. Порядок исполнения и остаток

1. R03/#29: компактное исправление Codex exact steer, не ждёт quota/control parity.
2. [R15][r15]: Codex/Muse quota producer → Store → scoped readback + точный quota incident. Command/Claude/Antigravity collectors не добавлены в его scope.
3. [R16/#42][r16]: Claude root permissions/questions через имеющийся SDK callback и truthful mode projection. Shared schema extraction остаётся R14.
4. Command ACP: named consumer → gate #19/ACPX source review → отдельный согласованный vertical slice. Поддержка cancel/load/model не доказывает все verbs и отсутствие replay.
5. Muse child/background и OpenCode loop-step parity: отдельные законченные срезы в действующих adapters, не новая глобальная orchestration layer. Zed/Kilo новые surfaces остаются непроверенными.

Один manager/worktree, source → код → минимальный scoped Clippy, broad/native tests в итоговой фазе. Никаких model calls, account reads, key mint, оплаты, автоматической ротации, SDK updates или merge этим исследованием не выполнено. Ошибка получения полного SDK reference не заменена догадками: изучены focused official pages и конкретные ELIOT callers. В документе нет обещания применённых возможностей по одному docs CI.

## Источники

[e-codex]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/codex/UPDATE.md
[e-codex-client]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-codex/src/lib.rs#L1598-L1830
[e-muse]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f269b3d4dea2c5e15754feba9971b1f1c1b20c2b/modules/muse/bridge.mjs
[e-oc]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/opencode/README.md
[e-claude]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/claude/UPDATE.md
[e-claude-config]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-claude/src/config.rs
[e-claude-driver]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-claude/sdk-harness/bridge.mjs
[e-claude-module]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-claude/src/module_runtime.rs
[e-command]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/command/UPDATE.md
[e-agy]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/antigravity/README.md
[e-ocx]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/opencodex/README.md
[e-capacity]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-kernel-host/src/store/capacity.rs#L600-L750
[c-doc]: https://learn.chatgpt.com/docs/app-server
[c-quota]: https://github.com/openai/codex/blob/82e70121f86bc1f6fea7f2bb7bbc169d259b3c6b/codex-rs/app-server-protocol/schema/typescript/v2/RateLimitSnapshot.ts
[c-credits]: https://github.com/openai/codex/blob/82e70121f86bc1f6fea7f2bb7bbc169d259b3c6b/codex-rs/app-server-protocol/schema/typescript/v2/CreditsSnapshot.ts
[m-pin]: https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/msp.d.ts
[o-api]: https://opencode.ai/v2/docs/api
[a-permissions]: https://code.claude.com/docs/en/agent-sdk/permissions
[a-input]: https://code.claude.com/docs/en/agent-sdk/user-input
[a-cost]: https://code.claude.com/docs/en/agent-sdk/cost-tracking
[q-acp]: https://commandcode.ai/docs/acp
[q-headless]: https://commandcode.ai/docs/headless
[q-usage]: https://commandcode.ai/docs/resources/usage-limits
[g-headless]: https://antigravity.google/docs/cli/headless/
[g-usage]: https://antigravity.google/docs/cli/commands/usage/
[g-sdk]: https://antigravity.google/docs/sdk/overview/
[r-limits]: https://openrouter.ai/docs/api_reference/limits
[r-credits]: https://openrouter.ai/docs/api/api-reference/credits/get-remaining-credits
[x-readme]: https://github.com/lidge-jun/opencodex/blob/250f17afd8ff44c93c620d87c1f346ef56f64fb4/README.md
[x-router]: https://github.com/lidge-jun/opencodex/blob/250f17afd8ff44c93c620d87c1f346ef56f64fb4/src/server/management/provider-routes.ts
[x-mint]: https://github.com/lidge-jun/opencodex/blob/250f17afd8ff44c93c620d87c1f346ef56f64fb4/src/providers/muse-key-quota.ts
[r15]: ../remediation/2026-10-07/15-native-usage.md
[r16]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/42
[acp-gate]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/19
