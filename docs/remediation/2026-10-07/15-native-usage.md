# R15. Нативная квота Codex/Muse: два producers, один Store reader

**PR #41 · перепроверено 08.10.2026 · production-код R15 ещё не написан.** ELIOT source `40591a295af94b1541ec2ba30afe8e3247701a71`. Реализация в этой ветке, без отдельного PR на неподключённые DTO. Ни внешние release, ни модели не закреплять; сохранять существующую подписочную авторизацию и штатные обновления.

## Результат

Менеджер видит наблюдённые окна/credits с правильными scope и свежестью. Нет model prompts, новых аккаунтов, отдельного inference API, покупки кредитов, model switch или перезапуска ради измерения. Посторонний Applied/Accepted больше не закрывает quota incident.

Срез: Codex continuous reader + snapshot/updates; Muse initial read + updates на существующей connection; bounded typed Store fact; current-authority read; коррекция incident. Cost Claude, UI-квоты Command/Antigravity, OpenCode session tokens и OpenCodex collectors не добавляются как третьи producers.

## Документация и точки изменения

Читать [Module contract](../../agent_swarm.module-contract-v2.md) §1/4/7, [Owner decisions](../../owner-decisions.md) §1.2–1.4/2.2 и [integration guide](../../agent-operations/native-harness-integration.md) §3–4/9. Первичные поля и donor semantics — ссылки в конце, не скрипты прошлой кампании.

| Существующий участок | Изменение |
|---|---|
| `crates/swarm-adapter-codex/src/lib.rs::NativeClient::{attach,request,receive_response}` | Read-pump живёт независимо от ожидающего RPC; replies, notifications и server requests раздельны. |
| Codex `send_operation` и callers `NativeClient::attach` | Один фактический transport owner вместо нового quota-клиента на вызов; не менять replay неизвестного input. |
| `decline_server_request` | Сохранить существующую выбранную policy при transport refactor. Новые approval replies — не скрытая часть R15. |
| `modules/muse/bridge.mjs::{launchConnection,onNotification,observation,report}` | Initial `usage/read` и `usage/changed` через общий validator на имеющейся connection, не child poller. |
| `crates/swarm-kernel-host/src/store/runtime.rs`, настоящий `module.observe` writer | Типизированный quota fact под существующими binding/artifact/boot/sequence guards. |
| `store/capacity.rs::{quota_code,reset_evidence,note_outcome,open_quota_incident}` | Разделить provider condition; resolution только по относящемуся evidence. Resource ledger R13 не переписывать. |
| `swarm-contracts` registry, host dispatch, MCP/CLI | Один scoped read и его parser/schema/authority/caller в той же поставке. |

## 1. Codex connection owner

В существующем adapter владеть одним reader и pending RPC map по connection generation + request ID. Неподдержанный notification не принимается как response; duplicate/unknown ID не разрешает другую Operation. Очереди bounded; account/terminal/reply frames не ждать за бесконечным bulk text. Отдельный sync Mutex не удерживать через await.

После initialize/initialized получить поддержанный `account/rateLimits/read` и доступный account context без forced login/token refresh. Optional `account/usage/read` не prerequisite квоты. Reconnect получает новую initial snapshot, старую помечает stale; ранее отправленный prompt не повторяется. Observer read только возвращает Store-проекцию.

R03 сохраняет exact input readback после steer: успешный RPC не означает persistence. Transport R15 не должен заменить `reconcile_send` ACK-only веткой либо скопировать mutation в retry queue.

## 2. Rate-limit snapshot и rolling patch — разные входы

`account/rateLimits/updated` несёт `rateLimits`; full read может содержать `rateLimitsByLimitId` и legacy view. Не считать map и legacy view двумя бюджетами. Ключ проекции включает доказанный service/auth context и bucket; unknown account overlap не превращать в сумму независимых лимитов.

Два явных typed входа в projector: full snapshot и rolling update. Не общий рекурсивный JSON merge с политикой «null всегда удаляет» или «null всегда сохранить».

| Поле / событие | Требование |
|---|---|
| Credits, plan/account metadata отсутствуют в rolling update | Документированная недоступность не стирает последнее observed значение. Отметить provenance/freshness отдельно, не объявлять старое значение свежим. |
| `spendControlReached:null` | Недоступность, **не восстановление spend-control**. Не превращать в false и не закрывать incident. |
| Primary/secondary window отсутствует или изменился reset | Не копировать безусловно старое окно по правилу metadata. Использовать контракт данного поля; при неоднозначности refetch, old evidence отдельно. |
| Full read возвращает unavailable/null | Не заливать его старым current из другого read; отсутствие наблюдения сохраняется явно. |
| Update одного limitId | Остальные buckets не удаляются; данные другого account/generation не подставляются. |
| Native limitId отсутствует | Применять только подтверждённое правило default bucket конкретного Codex протокола, не общий fallback всех harness. |
| Смена auth context | Прежние metadata/buckets исторические; новый аккаунт не наследует его credits. |

Сохранять известные `limitId/limitName/normalModelSlug`, окна, credits, individualLimit, spendControlReached, planType и rateLimitReachedType без изменения единиц. В рассмотренной форме reset окна — seconds; checked conversion. Balance-string не объявлять USD и не переводить без необходимости в f64. Проценты/деньги/token usage не складывать.

**Донор:** Codex Core `SessionState::set_rate_limits → merge_rate_limit_fields` сохраняет отдельные nullable metadata, но не является универсальным transport collector и не имеет нашего auth/bucket guard. Использовать как контрпример к blind merge, не импортировать private helper вместе с core. Public schema и свежий readback важнее подразумеваемой гарантии имени функции.

Old initial read, завершившийся после event, не откатывает snapshot: local read generation / evidence marker, либо conflict + refetch. JSON-RPC ID не задаёт native временной порядок. Время collection не подменяет время native observation.

## 3. Muse: базовый usage не требует выдуманного grant

**Исправлено прежнее задание:** `grantedCapabilities` — именные дополнительные привилегии, не список всех RPC. В рассмотренной stable схеме это userShell/sessionMcp/sessionListStream/feedback с открытым расширением. Не запрашивать строки `usage/read` или `turn/steer` как grants и не отключать эти методы из-за пустого списка.

Initial `usage/read` идёт после успешного обычного handshake по реально поддержанной wire-форме. `usage/changed` поступает в существующий callback; оба используют один validator. Отсутствие действительного метода ограничивает quota report, но не рабочую session. Отсутствие unrelated optional grant не ограничивает квоту.

Именной grant проверяется только для той операции/поля, которым он действительно требуется. `experimentalApi` отдельно. `userInputDialogs` — способность клиента, не grant; absent означает capable. Не терять вопросы из-за квотного read или неверной интерпретации handshake. Если новый consumer реально использует privilege-gated extension, сохранить его grants под connection generation в этом consumer; универсального нового permission framework не нужно.

SubscriptionUsage: absent usage → not_observed; weekly/window раздельны; observedAtMs — native arrival stamp; resetsAtMs — ms; usedPercent finite и может превышать 100. Не менять observedAtMs на Date.now повторного GET. Usage event не child activity, terminal или разрешение расходовать credits. R04/#30 pending-generation fixes не переписывать.

## 4. Fact, Store и авторизованное чтение

Минимальный новый внутренний `NativeUsageSnapshot` (имя предлагаемое, ещё не API) содержит source runtime/service/binding/generation/connection, известный auth-context ref либо unknown, buckets/units, native observation marker, collected_at, freshness/completeness и evidence ref. Source-specific поля валидирует adapter; Store не разбирает произвольный vendor payload и не принимает модельный текст за account identity.

Raw credentials, tokens, email и headers не публиковать. Не копировать account snapshot за каждого child. Equal observation коалесцируется; health нового collection отдельно от semantic reset. Retention подчиняется существующим классам evidence, не новому произвольному LRU.

Принять fact через настоящий authenticated module.observe с проверками artifact, binding/generation, boot и порядка observation. Найти и подключить реальные constructors, не альтернативный meta writer. Read проверяет текущий Principal и scope до формирования ответа.

`capacity_report(db,limit,after)` не получает Principal: account/credit data туда не добавлять. Предлагаемый `agent.usage {binding_id,generation}` — новое имя, проверить его отсутствие и подключить registry/parser/handler/schema/CLI-MCP caller вместе. Participant не получает account-wide data; Manager/GM/Operator видят только разрешённую область. Проверить raw вложенные observation readers, чтобы обход через agent.state/report.delta не раскрыл скрытое.

Refresh, если он нужен продукту, реализуется адресным native read adapter, не запросом к модели и не сменой авторизации. Панель чтения не запускает коллекцию тайно на каждом отображении.

## 5. Incident: нужное evidence, не случайный успешный RPC

Убрать unconditional resolution из `capacity::note_outcome`. Различать structured RateLimited, QuotaExhausted, AuthRequired, Overloaded, ModelUnavailable, DataPolicyRequired и Unknown. HTTP429 без native классификации не равен exhaustion; success configure/refresh/reply или позднего старого turn не доказывает новый budget.

Resolution требует относящегося к тому же auth/bucket/window нового доказательства либо явно записанного допустимого решения. `reset_at` делает read своевременным, но не доказывает восстановление. Sparse patch другого bucket, null spend state и credits presence не дают этого доказательства. Legacy ambiguous facts остаются ambiguous.

Нет route fallback, concurrency caps, automatic account rotation, purchase, reset-credit consume, email или нового billing route в этом PR. Policy применения квоты — отдельный контракт после достоверной проекции.

## 6. Сценарии итогового кандидата — ещё не исполнены

| Вход | Ожидаемый результат |
|---|---|
| Update при отсутствии ожидаемого RPC | Reader принимает, Store получает fact. |
| Null rolling metadata / null spend-control / полный unavailable read | Разные семантики; нет стёртых metadata или ложного восстановления. |
| Два buckets, auth change, old read после event | Нет cross-account подстановки, потери второго bucket и отката свежести. |
| Muse grants пусты, usage/read поддержан | Usage работает; отсутствующий unrelated grant не новый запрет. |
| Muse method действительно недоступен / usage отсутствует / 105% | Unsupported / not_observed / валидная snapshot соответственно. |
| Вопрос одновременно с quota read | Callback/attention не теряется; userInputDialogs не зависит от grants. |
| Несколько children на одной подписке | Квота не умножается; неизвестное overlap обозначено. |
| Quota incident и посторонний Applied | Incident остаётся unresolved. |
| Forbidden binding или обход raw observation | Account-wide data не раскрыта. |
| Все reads и reconnect | Нет prompts, повторов input, новых paid API, login, purchase или остановки. |

## 7. Источники, порядок, сдача

- [Codex notification](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/schema/typescript/v2/AccountRateLimitsUpdatedNotification.ts), [RateLimitSnapshot](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/schema/typescript/v2/RateLimitSnapshot.ts), [Core field merge](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/core/src/state/session.rs#L350-L462).
- [Muse CapabilityName/ClientCapabilities и usage types](https://github.com/meta-models/muse-code-sdk/blob/912061bb125b4bff60c9f3089a21cd23b53c6b4f/schema/msp/msp.d.ts), [usage/read docs](https://meta-models.github.io/muse-code-sdk/next/generated/msp/methods/usage-read/). `/next` и stable source не объявляются одним установленным build.

Порядок: Codex transport owner → projector → Muse projector → shared bounded fact/Store → scoped reader → incident. Все callers в одном PR. Один manager/worktree; writers без Cargo. После законченного кода scoped formatting и:

```sh
cargo clippy --locked -p swarm-adapter-codex -p swarm-contracts -p swarm-kernel-host -p swarm-mcp -p swarm-cli --lib --bins -- -D warnings
node --check modules/muse/bridge.mjs
```

Broad tests/native — итоговая фаза. R03 владеет steer admission/readback; R04 — pending requests; R13 — resource ledger; R14 — поздний schema extraction; R16 — Claude replies. Сдать SHA, связанные producer/reader, удалённые лишние connections, реальный gate и gaps. Docs CI и эта редакция не квалифицируют ещё не написанный collector.
