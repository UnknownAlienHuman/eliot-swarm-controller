# R15. Нативные квоты Codex/Muse: read → event → Store → авторизованная проекция

**Draft-задание, 7 октября 2026. Production-код этого блока ещё не написан.** Основа ELIOT `40591a295af94b1541ec2ba30afe8e3247701a71`. Не меняет модель, оплату, число агентов или разрешение на автоматические повторы.

## Результат и границы

Менеджер видит фактически reported окна и свежесть квоты своего harness, а не догадку по 429. Codex/Muse snapshots поступают без model prompt. Успешный unrelated RPC больше не закрывает quota incident. Не строить новый capacity ledger и не превращать quota observation в policy engine.

Только Codex и Muse как producers, один общий consumer/readback. Claude cost, OpenCode provider integrations и OpenCodex quota mint изучены в [нативном integration guide](../../agent-operations/native-harness-integration.md), но **не являются новыми collectors этого PR**. Расширять общий transport только настолько, насколько необходимо постоянно получать выбранные notifications; новый approval/control набор поставляется отдельно.

## Читать перед кодом

- [Integration guide](../../agent-operations/native-harness-integration.md), §2–5: различия native effects, scope, квот и скрытых действий внешнего GET.
- `modules/codex/UPDATE.md`: Python bridge.3 и standalone Rust v4 не одна реализация. Работать в Rust; не удалить Python без parity.
- `modules/muse/UPDATE.md`: SDK 1.3.0 и обязательная новая artifact identity для изменённого bridge.
- `docs/owner-decisions.md`, §1.2–1.4/2.2: manager/worktree, no heuristic kill, read-only и retention.
- Первичные native schema/types указаны в integration guide с SHA. Текущая документация не доказывает поддержку старым установленным сервером.

## Существующие точки входа

| Путь / symbols | Что делать |
|---|---|
| `crates/swarm-adapter-codex/src/lib.rs::NativeClient::{attach,request,receive_response}`, `decline_server_request` | Один owned native connection/read-pump; отделить ответ RPC, выбранное notification и server request. Сейчас no-id notifications пропускаются. Не потерять нынешнюю refusal policy при transport refactor. |
| Тот же `lib.rs`: `send_operation`, вызывающие NativeClient::attach пути | Реально подключить общий transport owner вместо дополнительного quota-only socket на каждую операцию. Reconnect не повторяет native mutation. R03 меняет только steer admission, не этот owner. |
| `modules/muse/bridge.mjs::{launchConnection,onNotification,observation,report}` | Использовать действующий SDK callback; добавить initial/readback usage после успешного initialize и typed handling usage/changed. Не помещать account read в child refresh loop. |
| `crates/swarm-kernel-host/src/store/runtime.rs` — действующий module.observe path | Найти реального writer observation, сохранить binding/artifact/boot/sequence guards, добавить bounded typed quota projection и источник. Не обходить через произвольный meta writer. |
| `crates/swarm-kernel-host/src/store/capacity.rs::{quota_code,reset_evidence,note_outcome,open_quota_incident}` | Развести provider condition и quota evidence; убрать unconditional resolution на Applied/Accepted. Не менять reserve/active/release ledger R13. |
| `crates/swarm-contracts/src/method_policy.rs`, host dispatch, `swarm-mcp`, `swarm-cli` | Один scoped read нового значения, связанные schema/parser/authorization и caller в том же PR. Не глобальный рефакторинг каталога. |

`capacity_report(db,limit,after)` сегодня не получает Principal. **Не дописывать в него account/credit данные и считать их автоматически авторизованными.** Предпочтительный узкий новый read — `agent.usage {binding_id, generation}`; это предлагаемое имя, не существующий API. Проверить отсутствие конфликта имени; реализовать точную область и отдельный typed response вместе с registry. Cash/account-wide детали доступны только при отдельно допустимой роли/области; обычному участнику полный account inventory не выдаётся.

## 1. Минимальный typed fact и authority

Предлагаемые внутренние `NativeUsageSnapshot` и `ProviderCondition` — новые типы, не готовая библиотека. Snapshot переносит: source runtime/service, проверенный auth context ref или явную неизвестность, bucket ID, native observation time, collection time, completeness/freshness, числовые window fields с единицами, отдельно credits/spend controls и evidence ref.

Общий тип содержит только общую семантику; native raw error/protocol детали остаются в ограниченной adapter-проекции. Секреты, bearer, raw headers, email и полный account payload в публичный snapshot не включать. Identity нельзя вычислять из модельного текста или бесконтрольно доверять произвольному account ID, присланному модулем: связывать с выбранным native service и разрешённым credential/auth context источника. При неизвестном account не суммировать snapshots разных bindings как независимые бюджеты.

Snapshot observation и collector support разделены: unsupported / not_observed / stale / fresh / invalid_or_conflicting. Пустой объект, null, timeout, auth error — не нулевая квота. Не заменять старое usable evidence «свежим нулём»; сохранять его как stale и показывать ошибку нового read. При auth change старое значение не current.

## 2. Codex: непрерывное чтение, без второго управляющего клиента

Владелец connection демультиплексирует RPC IDs, notifications и server requests. Pending RPC ожидание не должно становиться единственным местом чтения socket. Ограничить очереди и размер frames; control replies/terminal/account snapshots не блокировать bulk output. Отбрасывание неподдержанного delta явно не означает потерю terminal/quota evidence.

После initialize получить поддержанный account context (`account/read` без forced token refresh) и `account/rateLimits/read`. Optional `account/usage/read` вызывать только при доказанной поддержке; его отсутствие не ломает основную quota snapshot. Новые optional fields не требовать у старого сервера. Аккаунт API может быть недоступен API-key-only backend: вернуть unavailable, не пробовать чужую auth route.

Слушать `account/rateLimits/updated` постоянно. `rateLimitsByLimitId` при наличии задаёт buckets; legacy `rateLimits` не считать второй независимой квотой. Update одного limitId не стирает другие buckets. Null текущего поля сохраняет unknown по native контракту. Преобразование resetsAt seconds → milliseconds проверяется на переполнение; native units/evidence сохраняются. Credits balance остаётся decimal string/opaque reported unit, не f64-USD по предположению.

Свежесть read, пришедшего после более нового event, проверять по available native marker/collector generation. Когда native total ordering отсутствует, не придумывать его из client request ID: conservative stale/conflict лучше неверного current. На reconnect получить новый initial snapshot; не сбрасывать usage в ноль и не re-send сохранённый prompt. Installed schema/experimental flag проверяется отдельно; не включать все experimental methods для обхода ошибки.

## 3. Muse: использовать штатный SDK, не переизобретать collection

Сразу после успешного initialize читать `usage/read` на уже существующей connection; `usage/changed` идёт через нынешний onNotification. Отдельный `refreshUsage` может быть private helper; он не требует root model turn и не должен падением прерывать agent.open/ответ на вопрос. No observation в `{usage?}` — штатный unknown.

Сохранять observedAtMs от native host; не заменять его Date.now каждого GET. Проверять типы и nonnegative finite значения, но **не запрещать usedPercent >100**. Weekly и current window раздельны. Новый usage update не считается активностью конкретного writer/child. SDK pin уже имеет этот API — upgrade ради одного read не нужен.

PR #30 владеет pending-request race fixes. Принять его актуальную реализацию перед интеграцией, не переписать/отменить её. Там остаётся отдельно отмеченный blocked module.example.json update; не обходить инструментальный запрет. Новую shipped bridge revision, examples/UPDATE и checkpoint compatibility согласовать по правилам модуля; никогда не переименовывать старый checkpoint в новый artifact. Публикация docs не активирует мост.

## 4. Store и reader — один законченный путь

Typed snapshot принимается через реальный authenticated module observation. Для standalone Codex проверить, как current adapter сообщает module observations, и подключить producer к этому пути; новый struct без caller не считается поставкой. Хранить последнее проверенное значение с source/evidence в существующем Store, а не ещё одну БД. Не записывать полный многократный snapshot каждого токена; значимые изменения коалесцировать, сохранять отчёт о gaps и last-known evidence.

`agent.usage` выполняет current Principal / permitted binding-generation проверку **до** проекции; server calls из этого reader запрещены. Operator может видеть разрешённые account details; обычный Manager — только явно предоставленный ему scope, Participant не получает account-wide баланс через generic agent.state/report.delta. Найти все raw observation readers и не обходить новый guard вложенным исходным payload. Новый метод, schemas, CLI mapping и docs связаны в этом PR.

На один общий account/bucket показывать одну snapshot или явно unknown overlap; не складывать один balance за каждого child. При несовпадающих источниках не выбирать произвольно первый binding. Snapshot cursor/freshness не равны праву исполнить действие.

## 5. Не закрывать quota incident случайным успешным исходом

В note_outcome убрать правило «всякий Applied/Accepted закрывает quota». OpenCode GET, model configure, late success ранее начатого turn или unrelated reply не доказывают восстановление исчерпанного окна.

Разделить как минимум throttling, subscription exhaustion, auth, overload и unknown provider failure, сохраняя typed source. Конкретная HTTP429 без дополнительной информации не доказывает exhaust; HTTP404 без provider/model proof не доказывает исчезновение модели. Старый ambiguous quota incident пометить исторически неоднозначным, не переписать задним числом в точный новый класс.

Resolution требует новой подтверждённой evidence нужного auth/bucket/window либо отдельного явного операторского решения с записанным основанием. При отсутствии такого доказательства отчёт остаётся unresolved/unknown. Наступление предполагаемого reset_at позволяет запланировать read, не объявить «квота восстановилась». Эта поставка **не** включает новые launch limits, смену модели, recharge, reset-credit consumption, email, mint или account-pool rotation.

## Итоговые сценарии, пока не выполненные

| Вход | Требуемый исход |
|---|---|
| Native quota event, когда нет ожидаемого RPC | Snapshot обновляется; событие не теряется в receive_response. |
| Старый read завершился после нового event; reconnect; auth changed | Нет регрессии в якобы fresh snapshot и подмены account. |
| Два limitId; patch одного; explicit null | Другой bucket сохранён; unknown не превращён в старое current/zero. |
| Muse no usage, weekly/reset ms, percent 105 | Честный unknown либо корректные значения без запрета >100. |
| 429; quota exhausted; unrelated successful RPC | Разные состояния; успех не закрывает quota без evidence. |
| Один account обслуживает parent/children/два bindings | Нет умножения денег/процентов на число агентов. |
| Запрещённый binding / Participant / raw nested observation | Account информация не раскрывается обходным reader. |
| Reader/Doctor и поток квоты | Ноль prompts, answers, restarts, key mint, recharge и иных billing/auth mutations. |

## Сдача и интеграция

Один manager/worktree. Внутренние assignments: Codex transport, Muse collection, общий contract/Store/reader — непересекающиеся symbols; общий DTO согласует manager. Writers без Cargo. После целого продукта manager выполняет scoped formatting и минимальный Clippy затронутых Rust packages, плюс `node --check modules/muse/bridge.mjs`; broad tests/native/account qualification — отдельная финальная фаза. Новые dependencies по умолчанию не нужны.

R03/#29 использует read/request transport, но владеет exact steer admission; не блокировать его этим PR. R04/#30 владеет pending freshness. R13/#39 — capacity resource ledger и lease, здесь только quota section. R14/#40 позже переносит shared schemas: добавления не начинают его extraction заново. No merge/активация по одному docs CI.

Сдать exact SHA, connected producer → Store → reader, удалённые повторные connections, реальные gates, remaining unavailable capabilities. В этой редакции опубликованы только source-backed guide и исполнимое по объёму задание, не работающий quota collector.
