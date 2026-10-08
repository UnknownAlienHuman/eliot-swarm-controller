# Нативная интеграция harness: официальные контракты, capability negotiation и ELIOT authority

**Перепроверено 8 октября 2026.** Нормативная основа этого документа — документация ELIOT и официальные контракты производителей. Скрипты прошлой кампании не определяют API, гарантии или архитектуру; они могут использоваться только как регрессионные сценарии после того, как соответствующее поведение подтверждено документацией harness.

Исторические SHA и номера релизов ниже — координаты прочитанного source. Они не являются allowlist запуска, требованием downgrade или запретом штатных обновлений. Для нового подключения ELIOT обязан наблюдать фактический runtime и согласовать требуемые возможности, а не сравнивать release с одной строкой.

## 1. Иерархия источников

При конфликте использовать следующий порядок:

1. [Контракт модулей ELIOT](../agent_swarm.module-contract-v2.md), [модульность](modularity.md), [решения владельца](../owner-decisions.md): authority, Operation, ownership, replay/readback и границы Store.
2. Официальная документация, опубликованная wire-schema и публичные типы конкретного harness.
3. Фактический `initialize`/capability response, server information и адресный readback текущего подключения.
4. Полный публичный пакет донора, когда он действительно является выбранной границей интеграции.
5. Исторические журналы и скрипты — только regression corpus. Они не разрешают выдумывать метод или ослаблять официальный контракт.

Реестр доноров остаётся inventory, а не install lock. Его собственная политика различает reviewed source, установленную реализацию и live qualification; runtime по умолчанию — установленный native harness с уже авторизованным аккаунтом.

## 2. Что унифицирует ELIOT

ELIOT не переписывает model loop, permissions, skills, tools, сессии и подагентов производителя. Он унифицирует только собственные обязательства:

- caller-owned Operation ID и сохранённое намерение до внешнего эффекта;
- binding/generation, native root/turn/request identity и один control owner;
- проверку текущих полномочий непосредственно перед эффектом;
- точную семантику ACK, terminal evidence и result bytes;
- `outcome_unknown → readback`, а не повтор input;
- bounded observations с freshness/coverage/gaps;
- отдельные Task acceptance и review — native success их не заменяет.

Vendor-функция может оставаться `native.*`, если общей семантики для двух реальных harness нет. Новый универсальный enum или workflow engine заранее не создаётся.

## 3. Capability — это гарантия, а не имя кнопки

Для каждой подключённой функции descriptor/handshake должен зафиксировать:

| Поле | Смысл |
|---|---|
| `native_method` | Официальный метод или endpoint, который реально вызывается. |
| `target` | Session/thread/turn/tool request/subagent/task и обязательные native IDs. |
| `effect_boundary` | Read, admission, priority interrupt, permission decision, configuration или billing/auth mutation. |
| `guard` | Expected turn, command ID, request fingerprint, connection generation, capability grant и current authority. |
| `ack_means` | Что именно подтверждает ответ: принято, применено, остановлено, сохранено или только представлено. |
| `terminal/readback` | Каким официальным событием/чтением завершается неизвестный исход. |
| `replay_policy` | Idempotent same-ID, readback-only либо unavailable. |
| `freshness` | Native observed time, local collection time и условие инвалидирования. |

Нужные классы управления различаются:

- `next_turn` — новый ход после текущего;
- `queue` — сохранённый input, ожидающий допуска;
- `loop_step_steer` — вмешательство на следующей безопасной границе цикла без exact-turn CAS;
- `native_expected_target` — точная коррекция конкретного активного turn;
- `background` — убрать foreground-блокировку, не отменяя работу;
- `interrupt` — priority stop request, terminal подтверждается отдельно;
- `cancel/retract` — обычная отмена/удаление admitted input по native правилам;
- `goal` — долговечное native continuation state;
- `permission/question reply` — ответ существующему native request, не новый prompt;
- `child control` — действие над observed child identity, не превращение child в новый root;
- `usage/quota` — наблюдение аккаунта/окна, не разрешение покупать, менять аккаунт или модель.

## 4. Codex: app-server как единственный control plane binding

### Официальная поверхность

Использовать app-server JSON-RPC, а не `exec` и не разбор rollout-файлов. Текущий публичный protocol source содержит `turn/steer`, `turn/interrupt`, `thread/goal/clear`, `account/rateLimits/read` и `account/rateLimits/updated`. ChatGPT-plan авторизация app-server поддерживается официальным OAuth-путём; отдельный inference API key не является обязательным условием этой интеграции.

Требуемая архитектура клиента:

1. Один владелец transport на connection generation.
2. Непрерывный reader демультиплексирует responses по JSON-RPC ID, notifications и server requests.
3. Mutation intent сохраняется до отправки; reconnect не повторяет prompt.
4. `turn/steer` получает caller-selected `expectedTurnId`; длинная history page не является prerequisite.
5. `turn/interrupt` и `thread/goal/clear` — разные Operations. Выход клиента не доказывает прекращение server-side goal.
6. Account updates входят тем же reader; quota-only connection на каждое чтение не создаётся.

`turn/steer` не принимает произвольные параметры `turn/start`. Противоречивый success либо lost reply остаётся unknown. Не ожидать новый `turn/started` как обязательное доказательство steer: коррекция относится к уже существующему turn.

Rate-limit update является sparse patch: доступные значения объединяются с последним `account/rateLimits/read` либо snapshot перечитывается. Nullable metadata в sparse update не стирает ранее наблюдённое значение. Поля `limitId`, окна, credits, spend-control и plan type остаются разными осями.

Официальные источники:

- [Codex app-server с ChatGPT-plan OAuth](https://developers.openai.com/siwc/token-sharing-open-source/codex-app-server)
- [текущий registry app-server methods](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/src/protocol/common.rs)
- [RateLimitSnapshot](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/schema/typescript/v2/RateLimitSnapshot.ts)

## 5. Muse: MSP negotiation вместо догадок по версии

### Официальная поверхность

MSP `initialize` возвращает `experimentalApi` и `grantedCapabilities`; grants фиксированы для жизни connection. Доступность операции определяется grant + schema/response текущего подключения, а не release number.

Текущая публичная схема описывает:

- `turn/start`, `turn/steer`, `turn/interrupt`, `turn/cancel`;
- `subagent/sendMessage`, `followupTask`, `interrupt`, `stop`, `resume`, `reopen`, `close`, `readResult`;
- `task/background`, `task/stop`, `task/stopAll`;
- `goal/set`, `edit`, `clear`, `pause`;
- `model/list`, `skill/list`;
- `usage/read`, `usage/changed`.

ELIOT подключает только методы из фактических grants. Отсутствующая grant делает конкретную capability `unavailable`; open/read других частей сессии продолжает работать.

`turn/interrupt` подтверждает admission priority interrupt, а завершение — `turn/completed` с terminal `cancelled`. `turn/cancel` идёт по обычной command lane и имеет другую семантику. `subagent/readResult` — command-plane метод и не используется как безобидный family observer. Для наблюдения применяются session/view events и exact result references.

`usage/read` даёт initial snapshot, `usage/changed` — последующее point-in-time наблюдение. `observedAtMs` остаётся временем host observation; повторный GET не делает его новым. Один subscription account не умножается на число children.

Официальные источники:

- [текущая MSP schema](https://github.com/meta-models/muse-code-sdk/blob/912061bb125b4bff60c9f3089a21cd23b53c6b4f/schema/msp/msp.d.ts)
- [stable JSON schema](https://github.com/meta-models/muse-code-sdk/blob/912061bb125b4bff60c9f3089a21cd23b53c6b4f/schema/msp/stable/msp.schema.json)

Не писать второй MSP state machine в Rust. Использовать полный публичный SDK на bridge boundary, оставляя Store vendor-neutral.

## 6. OpenCode: HTTP V2 и ACP — разные топологии

### Shared HTTP service

Для уже управляемого background service естественная граница — V2 HTTP API:

- `POST /api/session/{sessionID}/prompt` durably admits input, `delivery=steer|queue`, `resume` управляет запуском цикла;
- `POST .../background` переводит backgroundable foreground tools в наблюдение и является no-op для idle session;
- forms/questions имеют отдельные list/state/reply/cancel endpoints;
- сообщения, context, instructions и experimental durable session log читаются адресно;
- session records содержат parentID, agent/model, tokens/cost; это не доказательство полного child tree без coverage.

`delivery=steer` — полезный `loop_step_steer`, но не `native_expected_target`: endpoint не принимает expected active-turn identity. Нельзя либо запретить его из-за отсутствия Codex-гарантии, либо назвать atomic exact-turn steer.

Не использовать `opencode run`/`opencode api` как lifecycle probe рабочего сервиса: официальная CLI-документация предупреждает, что API-команда может обнаружить или запустить background service. Adapter должен подключаться к уже выбранному endpoint напрямую. Session warming отправляет реальные model requests и не является healthcheck.

### OpenCode ACP

`opencode acp` запускает private server внутри ACP process; он не подключается к shared background service. Это отдельная topology/capability profile, а не прозрачный новый transport существующего binding. Один ACP process может обслуживать несколько ACP sessions, но его ownership и directory rules отличаются от shared HTTP service.

Официальные источники:

- [V2 HTTP API](https://dev.opencode.ai/v2/docs/api/)
- [durable prompt admission](https://dev.opencode.ai/v2/docs/api/session/v2-session-prompt/)
- [background](https://dev.opencode.ai/v2/docs/api/session/v2-session-background/)
- [ACP support](https://opencode.ai/v2/docs/cli/acp/)

## 7. Claude Agent SDK: permission flow и долговечное ожидание

### Авторизация

Сохранять существующую subscription route. Официальная документация указывает: `ANTHROPIC_API_KEY`, если он присутствует, заменяет Pro/Max/Team/Enterprise subscription, а в non-interactive mode используется всегда. Adapter не должен инъецировать такой ключ или отдельный endpoint, если выбран subscription harness.

### Permissions

Фактический порядок:

1. hooks;
2. deny rules;
3. ask rules;
4. permission mode;
5. allow rules и auto-approved native calls;
6. `canUseTool`, только если запрос ещё не разрешён.

Следствия:

- `canUseTool` не является аудитом каждого tool call;
- для правила, которое обязано исполняться всегда, нужен `PreToolUse` hook;
- `AskUserQuestion` и user-interaction MCP tools доходят до callback по своим правилам; в `dontAsk` они отклоняются;
- незаданный permission mode — `inherited`, пока native effective mode не наблюдён;
- после SDK 0.3.286 отсутствие option не эквивалентно явному `default`.

Официальный callback может ждать неограниченно. Если процесс не должен жить до человеческого ответа, документация рекомендует `PreToolUse` hook с `defer`, чтобы процесс завершился и позднее продолжил persisted session. Поэтому R16 должен поддержать два честных режима:

- live callback: Node owner остаётся жив, один callback resolver получает exact reply;
- durable defer: hook фиксирует request, native session приостанавливается/сохраняется по официальному механизму, а продолжение выполняется через documented resume — не восстановлением JavaScript Promise из JSON.

`AskUserQuestion` возвращается через original `questions` и `answers`; 1–4 questions, 2–4 options; текущая документация прямо говорит, что он недоступен в subagents, запущенных через Agent tool. Не обещать child question support без отдельного native интерфейса.

Официальные источники:

- [permission evaluation](https://code.claude.com/docs/en/agent-sdk/permissions)
- [approvals and user input](https://code.claude.com/docs/en/agent-sdk/user-input)
- [environment variables / subscription override](https://code.claude.com/docs/en/env-vars)

## 8. Command Code: ACP — богатый интерактивный control plane

Официальный `cmd acp` сохраняет инструменты, skills, permission engine и sessions Command Code. Existing `cmd login` переиспользуется; отдельный model API account не требуется. ACP предоставляет streamed replies, tool calls/diffs, permission prompts, questions, modes, model/effort picker, context meter, session list/reopen/close и MCP.

Каждый ACP thread — настоящая Command session. Один `cmd acp` process обслуживает один project directory и множество threads; другую directory он отвергает, пока текущие threads не закрыты. Это хорошо совпадает с manager/worktree ownership ELIOT.

Headless `cmd -p` тоже создаёт настоящую durable session и может продолжаться через `--continue` либо exact `--resume`; он не «без сессии». Но это single-query process boundary: live approval/control между запусками слабее ACP. Выбор topology делается по требуемым capabilities:

- интерактивный manager с approvals/subagents/session controls → ACP;
- ограниченный batch/CI с exact session resume → headless profile.

Не строить pseudo-steer из нового `-p --resume` prompt. Если ACP не объявляет exact active-turn steer, capability остаётся next-turn/prompt, а не получает более сильное имя.

Официальные источники:

- [Command ACP](https://commandcode.ai/docs/acp)
- [Sessions and checkpoints](https://commandcode.ai/docs/sessions)
- [Headless mode](https://commandcode.ai/docs/headless)
- [Permissions](https://commandcode.ai/docs/permissions)

## 9. Antigravity: stream-json как штатный многоповоротный transport

Официальный headless contract:

- cached subscription credentials;
- `stream-json` выдаёт `init`, `step_update`, `result`;
- init содержит cwd/tools/permission mode/model/agent;
- step updates несут conversation/step state, tool/subagent info, text delta и usage;
- при `--input-format stream-json` один process обслуживает несколько turns;
- stdout читается непрерывно, stdin закрывается только когда владелец действительно завершает session;
- `/model` и `/usage`, посланные в stream-json, ломают JSON flow; они запускаются отдельно как CLI actions.

Session usage/turn/duration в многоповоротном режиме cumulative; не суммировать каждый result. Интерактивная credits panel доказывает subscription quota UI, но не machine-readable field в active stream. Пока официальный transport не выдаёт баланс структурированно, quota report честно `unavailable`, а рабочий runtime остаётся доступным.

Модель, effort, agent и permission mode выбираются route/config и подтверждаются `init`; нельзя держать одну `REQUIRED_MODEL_ID` константу или молча переключать модель.

Официальные источники:

- [Antigravity headless mode](https://www.antigravity.google/docs/cli/headless/)
- [credits and quotas](https://www.antigravity.google/docs/cli/credits/)

## 10. Доноры: брать механизм только в подходящей границе

| Донор | Применение | Не переносить |
|---|---|---|
| `agentclientprotocol/rust-sdk` | ACP Client/Agent/Proxy/Conductor, framing, initialize/capability flow; TCK для Command/OpenCode ACP profile | Не оборачивать MSP, Codex app-server и OpenCode shared HTTP в ACP ради унификации. Conductor process lifetime не заменяет ELIOT Operation ownership. |
| ACPX | Опциональный owner backend после появления конкретного consumer и повторного source review | Не обязательный runtime для всех harness, не второй Task/Store/workflow authority. |
| RMCP | MCP frontend ELIOT и schema utilities там, где это уже MCP | Не использовать MCP как control plane Codex app-server или как вторую authority. |
| CCCC / Paseo | Inbox locality и lifecycle ownership observer subscriptions | Не новый durable ledger и не отмена native Task при disconnect. |
| Restate / DBOS | Идеи: durable request ID до эффекта, awakeable correlation, step memo | Не новый server/engine; внешняя closure всё равно может исполниться до сохранения результата. |
| Tokio | Owned channels, cancellation-safe wait, bounded task ownership | Не actor framework, не автоматическая политика kill/restart. |
| Kingfisher / Atlas | Bounded diagnostic secret detection/redaction после отдельной квалификации | Не permission boundary и не обработка raw unlimited transcript в Store writer. |

Reviewed source SHA донора — evidence coordinate. Runtime dependency выбирается обычным проектным решением и совместимостью, не вечным pin пользователя.

## 11. Рекомендуемая архитектура адаптера

Один адаптерный vertical slice:

```text
official initialize / capability grants
  → adapter-owned connection and bounded demux
  → native typed command
  → persisted ELIOT Operation before effect
  → native ACK/event/readback
  → bounded typed observation
  → Store authority-protected reader/action
```

Общие transport primitives выносятся в adapter SDK только после двух реальных consumers и без vendor state machine. Каждый новый public method имеет production caller в том же PR. Store не содержит release allowlist и не парсит vendor payload вне зарегистрированного validator.

## 12. Порядок реализации

1. R03: Codex exact steer без history pagination.
2. R15: Codex/Muse usage по существующим connections и исправление ложного quota recovery.
3. R16: Claude permission/question flow с live callback и официальным durable defer.
4. OpenCode parity: forms/questions, loop-step steer, background и log readback из уже существующего V2 runtime; удалить дубли после parity.
5. Command ACP vertical slice: initialize, one project owner, session open/resume, prompt, permissions/questions, agent/subagent observations, result/readback.
6. Antigravity: убрать model equality gate; нормализовать init/step/result и cumulative usage, не выдумывать quota RPC.
7. Затем route availability/admission policy — только после достоверных ProviderCondition/usage facts и отдельного решения владельца.

## 13. Критерии качества

Для каждого harness проверить записанным официально-совместимым transcript и затем native qualification:

- compatible update без release-number rejection;
- exact capability absent/present;
- current auth/account и отсутствие silent backend switch;
- input intent до эффекта, lost reply и reconnect без replay;
- target mismatch/late event/old connection generation;
- pending question и permission reply;
- child/family partial coverage;
- interrupt/cancel/background с правильным terminal readback;
- quota snapshot freshness и sparse update;
- disconnect observer не прекращает accepted work;
- никакой status/Doctor read не делает prompt, login, purchase, key mint, restart или model switch.

Исторические скрипты можно превратить в fixtures конкретных ловушек, но они не участвуют в определении expected wire contract. Документ не изменяет runtime, credentials, SDK dependencies или рабочие сервисы.