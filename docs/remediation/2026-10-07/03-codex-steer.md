# R03. Codex: убрать лишний history-preflight, сохранить доказательство доставки

**PR #29 · перепроверено 08.10.2026 · production-код R03 ещё не изменён.** AUD-012, ELIOT `40591a295af94b1541ec2ba30afe8e3247701a71`; проверенный исходник ветки `cbbb49b8207ee935d700478bf001ab9a69067aba`. Реализация — в этой ветке, producer и readback вместе.

## Результат

Длина завершённой истории не запрещает steer в правильный активный turn. Нативный ACK не выдаётся за сохранённый input, завершённую работу или право повторить запрос. Существующая подписочная авторизация, выбор модели и настройки владельца сохраняются. Внешние версии не закреплять: SHA ниже — координаты исследования, не launch allowlist.

## Читать адресно

- [Module contract](../../agent_swarm.module-contract-v2.md), §1 и §4: классы доставки, ACK и readback неизвестного эффекта.
- [Owner decisions](../../owner-decisions.md), §1.2–1.4; [Modularity](../../agent-operations/modularity.md), §3; `modules/codex/UPDATE.md` для различия исполнителей, не предписаний удерживать старый native release.
- [Официальный app-server](https://learn.chatgpt.com/docs/app-server): Steering, initialize/experimental API, paginated history.
- [TurnSteerParams](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/app-server-protocol/schema/typescript/v2/TurnSteerParams.ts): required `expectedTurnId`, `threadId`, `input`, optional `clientUserMessageId`.
- [Core turn-input boundary](https://github.com/openai/codex/blob/ea27864f99f0b086cec2f9f0251b7190fb9844f1/codex-rs/core/src/session/turn_input.rs#L1-L13): ответ после решения start/steer/reject, **до** ожидания hooks, rollout persistence и sampling.
- [Upstream #40805](https://github.com/openai/codex/issues/40805): пример ACK при ещё volatile pending input. Это чужое воспроизведение; текущий исходник выше подтверждает границу, но не частоту сбоя установленного runtime.

## Существующие функции и точное изменение

Все symbols — `crates/swarm-adapter-codex/src/lib.rs`.

| Функция | Действие |
|---|---|
| `send_operation` | Удалить только проверку полноты `active_turns` из steer admission. Root/scope, route, workspace и caller target сохранить. |
| `NativeClient::active_turns` | Сейчас первая страница из 20 turns даёт `page_limited`; это не доказательство неверного target. После удаления caller проверить другие применения; мёртвый helper удалить, не подавлять lint. |
| `native_send_payload` | Только нативные поля steer; не переносить model/cwd/sandbox/outputSchema overrides из turn/start. |
| `OperationRecord::intent`, `Journal::save` | Сохранить exact request correlation, prompt digest/bytes и expected target до native I/O. |
| `NativeClient::{attach,request,receive_response}`, `NativeError` | Согласовать реально используемый handshake/readback; сохранять нужную bounded error evidence, не трактовать любой RPC error как доказанный no-effect. |
| `reconcile_send`, `NativeClient::read_history`, `read_turn` | Существующий безопасный путь оставить: unique exact input, затем отдельное turn evidence. Не заменять его одним ACK. |
| `accepted_after_exact_input`, `unknown_send` | Accepted возможен после exact input даже при неполной информации о turn. Нет exact input — unknown/readback-only, не автоматический retry. |

## Порядок реализации

### 1. Упростить admission, а не гарантии

В `send_operation` сохранить непустой `expected_turn_id`, exact binding/root и проверки текущей конфигурации. Удалить steer-ветку `limited || active.len()!=1 || target mismatch` на основе `active_turns`; нативный `expectedTurnId` проверяет цель при самой отправке. Для next-turn сохранить его собственную idle-предпосылку.

Не сканировать всю историю перед каждым steer и не выбирать последний ID вместо caller target. `expectedTurnId` не делает остальные настройки атомарными; совпадение target не доказывает модель или workspace.

### 2. Разделить три ступени подтверждения

```text
RPC success + expected turnId
  → нативное решение принять steer в этот turn
exact persisted user item + correlation + content
  → доказанный input admission для ELIOT
native terminal соответствующего turn
  → исход исполнения; Task acceptance остаётся отдельным
```

`clientUserMessageId` — корреляция. Рассмотренная форма запроса сама не обещает идемпотентный повтор одинакового ID. Не импортировать сюда гарантии Muse `commandId`.

ACK без user item сохраняется как известный ACK и неподтверждённая долговечная доставка. Не генерировать user item/turn start в локальной проекции. Новый `turn/started` от steer не требуется. Локальный `returned_turn_status="inProgress"`, синтезируемый из ACK, не должен стать независимым native execution evidence.

Сохранить нынешние проверки `reconcile_send`: одна совпавшая correlation, digest и длина текста, expected turn, consistency с returned turn. `NATIVE_ITEM_NOT_OBSERVED` означает отсутствие доказательства на этом чтении, не доказанную потерю/отклонение ввода. Существующий readback после ACK — правильная часть кода.

### 3. Не потерять отказ и не придумать его

Сейчас `NativeError::Rejected` оставляет лишь RPC code. `send_operation` записывает его и идёт в readback, который при отсутствии item возвращает Unknown. Поэтому обещание «все native stale-target ошибки уже точно отклоняются» неверно.

В той же поставке определить, какие **документированные метод-специфические** error data доказывают отказ до input admission. Для них сохранить точный отказ. Если доступен только общий code, transport failure или неоднозначная форма — оставить Unknown, не классифицировать по человеческому message/регулярке. Изменение error type провести через decoder, record и readback; новый enum без caller не поставлять.

Противоречивый success с чужим `turnId` после возможного эффекта — unknown/integrity failure. Не отправлять fallback `turn/start`, `thread/resume` или второй steer. Повтор ELIOT request использует сохранённый outcome/readback.

### 4. Довести используемый readback до совместимого handshake

Текущий `attach` отправляет `experimentalApi:false`; используемые `thread/items/list` и `thread/turns/list` текущая app-server документация относит к experimental. Нельзя убрать первый history check, а обязательный `read_history` оставить на заведомо несовместимом handshake и объявить результат готовым.

Проверить контракт текущего выбранного сервера. Если нужный официальный read требует experimental opt-in, объявить/согласовать его для этого профиля и этих реально используемых readers; это не sandbox bypass и не разрешение произвольных mutations. Не встраивать release allowlist и не переключать модель. При отсутствии поддержанного readback честно обозначить предел гарантии; несохранённый input не повторять.

Границу readback исправлять в R03, не откладывать её на quota PR. Постоянный notification pump и аккаунтная проекция — R15/#41; нужны отдельно, но не требуют переписать весь transport здесь. После его интеграции readers пользуются общим owner.

## Критерии итоговой квалификации — пока не исполнены

| Сценарий | Результат |
|---|---|
| 21+ старых turns, правильный active target | Один steer без полного history-preflight. |
| Native ACK, sampling ещё идёт, user item отсутствует | ACK сохранён; durable admission не выдуман; повторного input нет. |
| Позднее появился один exact user item | Связывается с исходной Operation; нет нового turn/start и второго steer. |
| Exact user item найден, turn page недоступна | Accepted input, неизвестное исполнение — как у существующего helper. |
| Явный документированный pre-admission отказ / общий RPC error | Точный отказ в первом случае; сохранённая неопределённость во втором. |
| Потеря ACK; повтор caller request; conflicting payload | Readback старого ID; конфликт не меняет старую запись и не посылает input. |
| Readback требует opt-in / метод реально отсутствует | Handshake согласован либо конкретная capability gap; не вечный ложный `item missing`. |
| Корректный target, но чужой input digest/returned turn | Mismatch, не подтверждённая доставка. |

## Проверка и сдача

Один manager/worktree; writers без Cargo. Сначала законченный код, затем scoped formatting и:

```sh
cargo clippy --locked -p swarm-adapter-codex --lib --bins -- -D warnings
```

Tests/native — итоговая фаза. Сдать candidate SHA, removed preflight, payload/ACK/error/readback callers, фактический gate и remaining gaps. #26 не копировать; Python executor, goals, approvals, quota policy и SDK/native upgrades не включать. Эта редакция меняет задание, не код и не работающие сессии.
