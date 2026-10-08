# R15. Codex/Muse usage: official native protocol → Store → scoped reader

**PR #41 · переработано 8 октября 2026 · production-код ещё не изменён.** Source ELIOT: `40591a295af94b1541ec2ba30afe8e3247701a71`. Внешние release numbers и SHA — координаты документации, не условия запуска.

## Результат

Менеджер читает фактически наблюдённые окна и credits текущего Codex/Muse подключения с явной свежестью и scope. Collector не отправляет model prompt, не меняет аккаунт/модель, не покупает credits и не требует отдельный API key. Успешная посторонняя Operation больше не закрывает quota incident.

Этот PR включает только:

1. постоянный Codex app-server reader + initial/sparse rate-limit observations;
2. Muse `usage/read` + `usage/changed` на существующей MSP connection;
3. один bounded typed fact в Store и один current-authority reader;
4. исправление причинно неверного quota incident resolution.

Command/Antigravity interactive usage panels, Claude estimated cost, OpenCode per-session token/cost и OpenCodex provider collectors не подменяют Codex/Muse account snapshots и не входят в R15.

## Нормативные источники

- [Module contract](../../agent_swarm.module-contract-v2.md): identity, Operation, replay/readback, observations.
- [Native harness integration](../../agent-operations/native-harness-integration.md): capability classes и official-doc source hierarchy.
- [Codex app-server ChatGPT-plan path](https://developers.openai.com/siwc/token-sharing-open-source/codex-app-server).
- [Codex current method registry](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/src/protocol/common.rs) и [RateLimitSnapshot](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/schema/typescript/v2/RateLimitSnapshot.ts).
- [Muse current schema](https://github.com/meta-models/muse-code-sdk/blob/912061bb125b4bff60c9f3089a21cd23b53c6b4f/schema/msp/msp.d.ts) и stable JSON schema.
- [Owner decisions](../../owner-decisions.md), §1.2–1.4/2.2.

Исторические управляющие скрипты не являются source этого контракта. После реализации из их инцидентов можно сделать regression fixtures, но expected fields/semantics берутся из официальных протоколов.

## Существующие участки кода

| Участок | Изменение |
|---|---|
| `crates/swarm-adapter-codex/src/lib.rs::NativeClient::{attach,request,receive_response}` | Один connection owner и непрерывный reader. Сейчас no-id notifications пропускаются при ожидании RPC. |
| Codex `send_operation` и все callers `NativeClient::attach` | Использовать один transport owner; quota read не создаёт второй controller connection и не повторяет mutation. |
| `decline_server_request` | Transport refactor не изменяет permission policy. Входящие approvals/questions — отдельный законченный блок. |
| `modules/muse/bridge.mjs::{launchConnection,onNotification,observation,report}` | Проверить `grantedCapabilities`; initial `usage/read`, затем `usage/changed`. |
| `crates/swarm-kernel-host/src/store/runtime.rs` / фактический `module.observe` writer | Принять typed bounded fact с binding/artifact/boot/sequence guards. |
| `store/capacity.rs::{quota_code,reset_evidence,note_outcome,open_quota_incident}` | Разделить provider condition и quota evidence; убрать unconditional resolution на `Applied|Accepted`. Resource ledger R13 не переписывать. |
| `swarm-contracts::method_policy`, Store dispatch, `swarm-mcp`, `swarm-cli` | Один scoped read и связанные parser/schema/caller. Не добавлять DTO без production consumer. |

Перед правкой найти все реальные `module.observe`/Codex adapter observation constructors и reader surfaces через `git grep`; таблица не разрешает создавать параллельный meta writer.

## 1. Codex transport: reader существует независимо от RPC caller

`receive_response` не должен быть единственным циклом чтения socket. Нужен adapter-owned pump:

```text
WebSocket/stdio frame
  → validate frame bound
  → response{id} → exact pending request
  → notification{method} → typed projector
  → server request{id,method} → bounded attention/refusal path
```

Требования:

- pending requests принадлежат connection generation;
- duplicate/unknown response ID не связывается с новой Operation;
- control/terminal/account frames не блокируются bulk deltas;
- reconnect создаёт новое generation и initial reads, но не повторяет input;
- disconnect сохраняет last-known snapshot как stale, не zero;
- no-id notification не теряется только потому, что сейчас нет ожидаемого RPC.

Не переносить app-server core в Store. Adapter интерпретирует native payload и эмитит общий bounded fact.

## 2. Codex rate-limit semantics

На той же connection после `initialize/initialized` выполнить поддерживаемый `account/rateLimits/read`. `account/read` используется только для разрешённого account context без forced token refresh. `account/usage/read` optional и не является prerequisite основной snapshot.

`account/rateLimits/updated` — sparse update. Официальная schema требует:

- merge только доступных значений с последним read либо refetch;
- nullable account metadata в rolling update может быть unavailable и **не очищает** прежнее observed значение;
- `rateLimitsByLimitId`, если присутствует, задаёт отдельные buckets; legacy top-level snapshot не считается вторым бюджетом того же окна;
- update одного bucket не стирает остальные;
- account/auth generation change инвалидирует старый current view.

Минимальные native поля сохраняются без смены смысла:

- `limitId`, `limitName`, `normalModelSlug`;
- `primary`, `secondary` windows;
- `credits`;
- `individualLimit`, `spendControlReached`;
- `planType`, `rateLimitReachedType`;
- native observed marker, если он есть, и local collected time.

Не объявлять строковый credits balance долларами без native definition. Не считать 100% окна доказательством запрета продолжения, если native snapshot отдельно показывает credits; ELIOT всё равно не получает автоматического права расходовать их.

Для old initial read, завершившегося после update, использовать local read generation. Если native protocol не даёт полного ordering, возвращать conflict/stale и refetch, а не назначать порядок по JSON-RPC request ID.

## 3. Muse: capability grant и point-in-time usage

После MSP initialize проверить `grantedCapabilities`, фиксированные для connection lifetime. `usage/read` вызывается только когда поддержан текущей connection; отсутствие grant ограничивает quota projection, но не ломает session/open/attention.

`usage/read` и `usage/changed` несут одну `SubscriptionUsage` форму. Требования:

- initial read и subsequent notifications проходят один validator/merge path;
- отсутствующий `usage` = `not_observed`, не 0%;
- `weekly` и current window независимы;
- `observedAtMs` — native host arrival stamp, не время нашего повторного read;
- `resetsAtMs` сохраняется в миллисекундах;
- `usedPercent` проверяется как конечное число, но значение >100 допустимо;
- update account/session connection generation не сливается со старым current snapshot;
- usage event не считается child activity или доказательством terminal turn.

Использовать существующий SDK callback и connection. Никакого child poller, второго SDK, login probe или model turn.

R04/#30 владеет pending request freshness. Интеграция R15 принимает его актуальный код и не переписывает question inventory.

## 4. Общий fact без потери native semantics

Предлагаемый внутренний тип, **ещё не существующий public API**:

```text
NativeUsageSnapshot {
  source: {runtime, service_scope, binding_id, generation, connection_generation},
  account_context: Known(ref) | Unknown,
  buckets: [NativeUsageBucket],
  native_observed_at?,
  collected_at,
  status: Fresh | Stale | NotObserved | Unsupported | InvalidOrConflicting,
  completeness,
  evidence_ref,
}
```

`NativeUsageBucket` хранит native ID/name, окна с единицами, credits/spend-control отдельно и opaque vendor additions только после bounded validation. Не сводить всё к `remaining_percent`.

Raw bearer, API key, refresh token, email, full account payload и headers не попадают в Store projection. Account context ref должен быть получен от authenticated adapter/service identity; модельный текст и route label не являются account proof.

Один account может обслуживать parent/children/несколько bindings. Без доказанного общего account identity snapshots нельзя ни складывать, ни объявлять независимыми. Reader показывает overlap unknown.

## 5. Store: один writer и текущая authority

Fact принимается через существующий authenticated adapter/module path. Проверяются:

- artifact/adapter identity;
- binding/generation и current boot/connection sequence;
- monotonic adapter observation sequence;
- payload byte/count bounds;
- source-specific validator revision;
- current authorization перед чтением.

Хранить last-known current и нужную короткую историю изменения, а не копию snapshot на каждый token event. Коалесцированный equal snapshot обновляет collection health отдельно, не чеканит новый semantic quota reset.

`capacity_report(db, limit, after)` не принимает Principal. Account/credit данные туда не добавлять. Предпочтительный новый read surface — `agent.usage {binding_id,generation}`; имя необходимо проверить на конфликт перед реализацией. Method registry, parser, Store handler, MCP/CLI caller и docs поставляются в одном PR.

Participant не получает account-wide credits через generic `agent.state` или raw module observation. Manager видит только binding/project scope, который ему разрешён; GM/Operator — по действующей policy. Raw nested payload reader не должен обходить эту проверку.

Reader никогда не вызывает native endpoint. Explicit refresh, если нужен, — отдельная read Operation через adapter и с собственным rate bound; он не является model prompt или auth mutation.

## 6. ProviderCondition и quota incident

Текущий `capacity::note_outcome` причинно неверен: любой `Applied|Accepted` на scope может закрыть quota incident. Исправить:

- `RateLimited`, `QuotaExhausted`, `AuthRequired`, `Overloaded`, `ModelUnavailable`, `DataPolicyRequired`, `Unknown` — разные условия;
- generic 429 без структурированного класса не доказывает `QuotaExhausted`;
- success configure/refresh/reply/старого turn не доказывает восстановление окна;
- reset timestamp лишь делает due новый read, а не закрывает incident;
- resolution требует новой snapshot того же account/bucket/window либо explicit operator resolution с основанием;
- sparse patch другого bucket не закрывает incident исходного bucket;
- legacy ambiguous incidents остаются исторически ambiguous.

Не включать в R15 route fallback, concurrency caps, purchases, reset-credit consumption, email или account rotation. Эти policies требуют отдельного owner-approved contract после появления достоверных observations.

## 7. Что намеренно не собирать в R15

| Источник | Почему не входит |
|---|---|
| Command `/usage`/ACP context meter | Официальные docs подтверждают UI/session cost и плановые ошибки, но R15 не добавляет third producer. Сначала отдельный machine-readable ACP contract. |
| Antigravity `/usage`/credits panel | Slash command нельзя посылать в active stream-json; официальный stream не обещает remaining balance. |
| Claude `total_cost_usd`/usage | Это SDK estimate/cumulative usage, не subscription remaining quota. |
| OpenCode session tokens/cost | Per-session usage, не provider account balance. |
| OpenCodex provider quota collector | Требуется отдельный анализ эффекта выбранного management endpoint и auth refresh; не скрытый Doctor call. |

## 8. Итоговые сценарии — пока не выполнены

| Сценарий | Требуемый исход |
|---|---|
| Codex update при отсутствии pending RPC | Pump принимает его и Store обновляет snapshot. |
| Sparse update с null account metadata | Bucket fields merge; metadata не стирается как будто native заявил null-current. |
| Initial read завершился после update | Snapshot не откатывается; refetch/conflict отражён явно. |
| Reconnect и account/auth change | Old snapshot становится stale/history; input не повторяется. |
| Muse capability отсутствует | Quota `unsupported`, остальные negotiated capabilities продолжают работать. |
| Muse usage absent / 105% / два окна | `not_observed`, допустимое число, раздельные buckets. |
| Один account, parent + children | Один бюджет; нет умножения процентов/credits. |
| Quota incident + unrelated success | Incident остаётся unresolved. |
| Запрещённый binding/raw observation | Account data не раскрывается. |
| Все reads | Ноль prompts, login, model switch, purchase, mint, restart или stop. |

## 9. Порядок реализации

1. Codex connection owner/read-pump без изменения mutation semantics.
2. Codex initial read + sparse update projector.
3. Muse capability-aware initial/event projector.
4. Общий bounded contract и authenticated Store writer.
5. Scoped `agent.usage` reader и frontend mapping.
6. ProviderCondition/quota incident correction.
7. Adapter fixtures из официальных schemas; затем native qualification текущих installations.

Один manager/worktree; writers получают непересекающиеся symbols и не запускают Cargo. После полного vertical slice:

```sh
cargo clippy --locked -p swarm-adapter-codex -p swarm-contracts -p swarm-kernel-host -p swarm-mcp -p swarm-cli --lib --bins -- -D warnings
node --check modules/muse/bridge.mjs
```

Broad tests/native/account qualification — итоговая фаза. В сдаче: exact candidate SHA, producer → Store → reader, фактический gate, official schema used и remaining unsupported capabilities. Документационный commit не квалифицирует collector.