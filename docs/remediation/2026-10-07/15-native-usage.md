# R15. Квоты существующих подписок Codex/Muse: native → Store → manager

**PR #41 · исправлено 8 октября 2026 · пока задание, не реализация.** Проверенный source ELIOT: 40591a295af94b1541ec2ba30afe8e3247701a71. SHA — источник проверки, не требование установки.

## Результат и обязательная граница

Показать менеджеру фактические окна квоты и их свежесть из уже используемых подписочных harness. Не требовать API key, отдельный inference billing, новый аккаунт, закреплённый SDK/CLI release или отключение обновлений. Нативный harness продолжает пользоваться существующей авторизацией и своим обычным auth refresh.

Рабочая основа: аудит владельца ELIOT-Swarm-AUDIT-2026-10-06(2).md §§B5.3–B5.4/B6; muse_as.py и codex_as.py там описывают уже работающие MSP/app-server пути. [Integration guide](../../agent-operations/native-harness-integration.md) содержит границы доступа к исходным script-архивам, политику без пинов и точные source-level gates, которые ещё предстоит удалить.

Этот PR — только два producers Codex/Muse и один законченный reader. Не включать сюда новый quota collector каждого вендора, model routing, оплату, автоматическую смену аккаунта и полный набор agent controls.

## Существующие функции и первый шаг

| Участок | Изменение |
|---|---|
| `crates/swarm-adapter-codex/src/lib.rs::NativeClient::{attach,request,receive_response}` | Соединение читается постоянно, а не только внутри ожидаемого RPC. Ответы по ID, notifications и server requests разделяются. |
| `send_operation`, действующие callers NativeClient::attach | Использовать владельца этого соединения, не создавать второй quota-only client на каждую операцию. Не повторять потерянную mutation. |
| `decline_server_request` | При transport refactor не менять самовольно выбранную policy ответов. Полноценный attention/reply остаётся отдельным блоком, не незаметным auto-allow. |
| `modules/muse/bridge.mjs::{launchConnection,onNotification,observation,report}` | Initial usage/read и usage/changed на имеющемся SDK/MSP link. Не child poller и не model prompt. |
| `crates/swarm-kernel-host/src/store/runtime.rs`, действующий module.observe | Принять bounded typed quota fact с текущими binding/boot/sequence проверками. |
| `store/capacity.rs::{quota_code,reset_evidence,note_outcome,open_quota_incident}` | Не закрывать quota incident от unrelated Applied/Accepted; источник и окно должны соответствовать. Resource ledger не переписывать. |
| `swarm-contracts` registry, host dispatch, MCP/CLI | Подключить один scoped reader вместе с реальными parser/schema/caller. Новый неподключённый DTO не считается результатом. |

Начать с receive_response и реального lifetime NativeClient: добавление account endpoint в allowlist без постоянного reader оставит updates потерянными. Общий reader не является новым агентным framework.

## 1. Совместимость без release allowlist

Использовать установленный runtime и актуальные определения его интерфейса. Reported version записывается отдельно от подтверждённых capabilities. Старые номера SDK/CLI в UPDATE описывают code snapshot, не ограничивают допустимый release. Не переносить жёсткое сравнение версии в новый collector и не заменять его другим постоянным диапазоном.

Проверить конкретные required methods/fields через существующие handshake/schema и обычные readback-запросы. Опциональное неизвестное поле не ломает базовую snapshot; отсутствующая account capability ограничивает quota report, не запрещает уже рабочую подписочную сессию. Нельзя выбирать другой тариф/модель, чтобы получить красивые цифры отчёта. Не устанавливать или обновлять пакеты из status/Doctor.

## 2. Codex

На той же app-server connection читать поддержанные account/read и account/rateLimits/read; постоянно принимать account/rateLimits/updated. Никакого forced нового login или токена от пользователя. Optional account/usage/read применять только при реальной поддержке, не требовать его у любого сервера.

Pending RPC по ID и current connection generation; bounded queues. Bulk text не блокирует replies/terminal/request handling. Разрыв не доказывает, что native input не был принят. После reconnect восстановить наблюдение, а не отправлять prompt повторно.

В изученных нативных формах есть legacy rateLimits и bucket map rateLimitsByLimitId: не считать их двумя независимыми бюджетами. Обновление одного bucket не стирает остальные. Explicit unknown/null не заменяется старым якобы current числом. Использовать единицы именно полученного контракта; в рассмотренной форме resetsAt — секунды. Checked conversion, не молчаливое переполнение.

Credits и subscription windows раздельны. Строковый balance не объявлять USD без нативного определения единицы. 100% окна не доказывает прекращение исполнения, если harness использует уже разрешённые владельцем credits; ELIOT сам разрешение расходовать их не выдаёт.

Старый initial read, пришедший после нового event, не должен откатывать current snapshot. При отсутствии native total ordering использовать локальное окно чтения/поколение и помечать неопределённость, не придумывать порядок по request ID. Auth change переводит старые account snapshots в исторические.

## 3. Muse

Добавить initial usage/read после успешного initialize в существующую connection и объединить с onNotification для usage/changed. Методы присутствовали уже в ранее исследованной схеме; это не инструкция удерживать ту SDK-версию. Не создавать второй SDK и не опрашивать каждую child session.

Отсутствующий usage — no observation. Weekly/current window раздельны; observedAtMs и resetsAtMs сохраняют native units. Нельзя заменять observedAtMs временем повторного GET или запрещать usedPercent >100. Невозможность прочитать quota не должна терять pending question либо провоцировать новый model turn.

Согласовать с актуальной веткой R04/#30, не отменяя pending-generation fixes. Идентичность поставленного ELIOT adapter должна соответствовать его bytes; это не pin пользовательского Muse. Не переименовывать старый checkpoint под новую сборку и не перезаписывать живой bridge.

## 4. Один typed fact и действительная область чтения

Минимальная проекция: source runtime/service; известный native account context либо unknown; bucket/window; native observed time и collected time; fields с единицами; freshness/completeness; evidence ref. Имена NativeUsageSnapshot/ProviderCondition — предлагаемые внутренние типы, не готовые библиотеки. Raw credentials/email/headers не публиковать.

Принять fact через реальный authenticated module.observe и хранить в существующем Store. Значимое изменение коалесцировать; не дублировать полный account snapshot на каждый token/child. Не суммировать один лимит за parent, children и несколько bindings. Неизвестное account overlap показывать как unknown.

`capacity_report(db,limit,after)` не принимает Principal. Не дописывать туда account-wide данные, считая их автоматически авторизованными. Предлагаемый узкий метод `agent.usage {binding_id,generation}` ещё не существует: проверить имя, подключить registry, parser, current ownership, schema и один frontend caller в той же поставке. Повторно проверить raw nested observation readers, чтобы они не обходили scope.

Reader возвращает уже полученную проекцию. Отдельный native refresh — нативное чтение через adapter, без inference и изменения user settings. Нормальная авторизация транспорта остаётся делом harness; запрет нового платного API не означает запрет его HTTP/MSP/app-server интерфейсов.

## 5. Исправить ложное восстановление квоты

`note_outcome` не должен закрывать incident только потому, что пришёл Applied/Accepted на том же binding. Успешный refresh, настройка или позднее окончание прежнего хода не доказывают сброс нужного окна.

Различать нативные rate limiting, exhausted subscription window, auth problem, overload и неизвестный отказ по структурированным данным. HTTP429 без подробностей не доказывает exhaustion. Resolution — новое доказательство для того же account/bucket/window либо отдельное явное решение с основанием. Предполагаемый reset time позволяет прочитать состояние, но не фабрикует восстановление.

Исторический неоднозначный incident не переписывать задним числом в точную новую категорию. Никаких auto purchases, reset-credit consumption, account switching или обхода ограничений. Отдельная quota snapshot не меняет политику запуска и число агентов.

## Итоговые сценарии — ещё не выполнены

| Сценарий | Требуемый исход |
|---|---|
| Native update без ожидаемого RPC | Принят reader, доставлен в Store; не потерян. |
| Нормальное обновление совместимого harness | Подключение не отклонено по старому release number; capabilities перечитаны. |
| Новый необязательный field / нет quota method | Остальные функции работают; missing quota честно unknown. |
| Старый read после нового event; reconnect/auth change | Нет ложной свежести и подмены account. |
| Два buckets; Muse 105%; отсутствующий usage | Раздельные окна, допустимый процент, отсутствие не ноль. |
| Quota failure, затем unrelated successful RPC | Incident не закрывается без релевантного evidence. |
| Несколько children на одной подписке | Один бюджет, не сумма копий. |
| Запрещённый binding/read через raw observation | Нет обхода текущего scope. |
| Все quota reads | Ноль prompts, новых paid-inference маршрутов, покупок и остановок сессии. |

## Сдача

Один manager/worktree; writers без Cargo. Реализация в этом PR producer → Store → reader целиком; затем scoped formatting и минимальный Clippy затронутых Rust packages, node --check изменённого Muse bridge. Broad tests/native/account qualification — итоговая фаза, не выполнена этой документацией.

R03/#29 не ждёт R15 и владеет steer admission; R04/#30 — Muse pending; R13/#39 — resource ledger; R14/#40 — последующий schema extraction. Исторические CI результаты не переносятся на новый код. В сдаче указать candidate SHA, connected callers, удалённые лишние connections, реальный gate и remaining gaps. Production-код и пользовательская конфигурация этим обновлением не менялись.
