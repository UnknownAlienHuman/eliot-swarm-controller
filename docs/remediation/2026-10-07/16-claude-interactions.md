# R16. Claude: нативный запрос → attention → точный ответ в живой callback

**Draft-задание · 7 октября 2026 · production-код не изменён.** Исследован ELIOT `40591a295af94b1541ec2ba30afe8e3247701a71`. Область: standalone Rust Claude adapter и его уже существующий Node SDK driver. Не второй Claude executor и не полный control parity всех harness.

## Результат и первый шаг

Менеджер через существующий `agent.reply` отвечает на реально ожидающее разрешение инструмента или корневой `AskUserQuestion`. Ответ разрешает ровно исходный callback, не посылает текст новым ходом. Неотвеченный, отменённый и уже отклонённый запросы различимы. Сохраняются нативный permission engine, выбранные настройки и собственные Operation receipts ELIOT.

Начать с `sdk-harness/bridge.mjs::prepare`: сейчас `canUseTool` сразу возвращает deny. `handle` понимает только prepare/send/stop. В Rust `module_runtime.rs::CAPABILITIES` не содержит agent.reply. Один новый обработчик в Node без descriptor/Store/reader не является поставкой.

## Документация перед кодом

- [Owner decisions](../../owner-decisions.md), §1.2–1.4: manager/worktree, этап проверки, explicit controls и no heuristic kill.
- [Module contract](../../agent_swarm.module-contract-v2.md), command delivery, observation, unknown/replay; [Claude UPDATE](../../../modules/claude/UPDATE.md), pin/artifact и rootless startup.
- [Claude permission evaluation](https://code.claude.com/docs/en/agent-sdk/permissions), Evaluation flow и Permission modes; [callback/user input](https://code.claude.com/docs/en/agent-sdk/user-input), Signal, response format, limitations. Прочитаны 07.10.2026; runtime-контракт перед реализацией сверить с установленным `sdk.d.ts` **0.3.287**. В этом проходе полный файл SDK types не извлечён.

## Проверенная особенность SDK — не писать обходы поверх неё

С версии TS SDK 0.3.286 пропущенный permissionMode уже не равнозначен explicit default. У ELIOT pin 0.3.287, оба Node пути условно опускают поле, а legacy README всё ещё обещает прежний default. Это AUD-045 — drift документации/ожидаемого режима, не доказательство несанкционированного действия на машине владельца.

Сохранить native inheritance для незаданного поля, но честно отображать `requested: inherited`, а после настоящего init — effective mode. Explicit route mode передавать неизменным. Исправить README; не подставлять default в evidence и не включать bypass ради отсутствующего callback. Политику ограничения действий задаёт выбранная конфигурация, а не название режима в отчёте.

`canUseTool` вызывается не для каждого инструмента: ранее разрешённые вызовы его минуют, `dontAsk` не передаёт ему оставшиеся вопросы. Для проверки каждого вызова существует PreToolUse, но установка новых глобальных запретов не входит в R16. `allowedTools` не равен полному списку доступных инструментов. Не оформлять штатный пропуск callback как отказ транспорта.

## Существующие участки — один владелец каждой границы

Пути относительно `crates/swarm-adapter-claude/`, кроме явно названных host-файлов.

| Участок | Использовать / изменить |
|---|---|
| `sdk-harness/bridge.mjs::prepare`, `handle`, `pump`, `safeSdkFrame` | Вместо мгновенного deny удержать допустимый unresolved callback; добавить закрытые private pending/decision frames, сохранить отдельный reader команд. |
| `sdk-harness/prepared-query.mjs::prepareQuery` | Сохранить одноразовый WarmQuery claim; reply не начинает query/startup заново. |
| `src/sdk_harness.rs::NativeHarness`, `HarnessFrame` | Передать новые frames через существующий транспорт; не запускать дополнительный Node на вопрос. |
| `src/lib.rs::handle_command`, `CommandInvocation`, `open_link` | Обработать agent.reply с текущей native identity и журналом; reconnect не создаёт второй callback. |
| `src/native_state.rs::NativeControl` | Сохранить точную текущую pending projection отдельно от истории отказов; не объявлять historical permission_denied ожидающим ответом. |
| `src/journal.rs::OperationJournal`, `src/receipt.rs` | Прежние intent/receipt/outcome механизмы; один immutable payload на Operation ID, не повтор решения после потери evidence. |
| `src/config.rs::NativeOptions`, `src/module_runtime.rs::CAPABILITIES` и `capabilities_match` | Версия нового artifact, negotiated reply capability, requested/effective permission отдельно. |
| Host `store/module_handshake.rs::native_command_capability`, `store/runtime.rs`, `store/capacity.rs` | Найти действующий agent.reply admission, observation writer и attention projector; подключить один Claude request-kind и closed reply schema, не общий vendor passthrough. |

Host-путь и MCP schema проверить до публикации через `git grep -n 'agent.reply' -- crates/swarm-kernel-host crates/swarm-contracts crates/swarm-mcp crates/swarm-cli`. Готового `runtime/claude.rs` на этой базе нет; не использовать выдуманный файл. Generic reply endpoint уже существует; новый public метод на каждый SDK tool не нужен.

## Последовательность реализации

### 1. Идентифицировать живой запрос, а не сериализовать Promise

Node хранит bounded map unresolved callbacks. Ключ включает текущий driver boot, фактический toolUseID и локальный callback ID; локальный ID явно помечен как controller-generated, не native turn/request ID. Session и binding берутся из текущего подтверждённого контекста, не из произвольного ответа менеджера. До native root adoption не приклеивать root ID от другой сессии.

Сохранить исходный toolName/input в Node и digest точного input; передать безопасную ограниченную карточку с fingerprint и стадией в Rust. Promise/resolve и raw secrets не помещать в Store. Усечение/редактирование отображаемого текста не меняет original input и не оправдывает allow неизвестного действия. При недостатке разрешённой информации сообщить incomplete/unanswerable; не терять вопрос молча.

### 2. Вывести attention без взаимной блокировки

Не await-ить решение менеджера в reader команд. Асинхронный SDK callback может ждать, пока существующий Rust/Node транспорт продолжает принимать reply, observation ACK и readback. Bulk transcript и pending control не делят неограниченную очередь. Если pending budget исчерпан, явный bounded failure/deny по техническому контракту вместо невидимого зависания или auto-allow.

Store принимает карточку через authenticated module.observe с прежними boot/binding/sequence проверками. Использовать существующий scoped attention/agent read; проверить также generic raw observation readers. Чужой Manager/Participant не получает tool input из-за знания request ID. Отказ SDK, current pending и отмена — разные состояния.

### 3. Ответить через тот же callback

Закрытая reply-форма содержит request ref, exact fingerprint и allow-once/deny либо ответ на исходные вопросы. Проверить current authority, binding/generation/boot, request still pending и неизменность input до воздействия. Сначала записать Operation intent, затем передать решение Node. Сам Node повторяет identity/state guard перед однократным resolve.

Для обычного allow-once вернуть в SDK исходный input, не произвольную подмену из клиентского JSON. Редактирование tool input — отдельная возможность, не свободное поле R16. Не применять `suggestions`/permission updates и не сохранять allow-always по умолчанию. Выбор deny — успешно доставленное решение, а не обязательно Rejected самой ELIOT Operation.

Для AskUserQuestion использовать нативный формат: original questions и answers по тексту вопроса; допустимые free-text/multi-select формы брать из pinned types. Не заменить это `agent.send`. Повторяющиеся question keys, скрыто усечённые options и schema mismatch не должны давать неверный ответ. По текущей документации этот инструмент недоступен детям, запущенным Agent tool: child support не заявлять по наличию parent callback. Обычные tool permission callbacks ребёнка атрибутировать только по реально предоставленным SDK IDs.

### 4. Отмена, replay и утрата callback

Следить за AbortSignal конкретного callback и завершением driver/query; снять listener после разрешения/отмены. Reply и abort состязаются за одно состояние. Поздний ответ не возрождает запрос, повтор identical decision возвращает сохранённый результат, conflict меняет только ответ вызывающему, не нативную работу.

IPC-разрыв с живым Node не удаляет pending. При гибели Node его Promise не восстановим из JSON: старую карточку пометить недействующей/unknown, не выпускать новую query и не присваивать её новому callback. История решения сохраняется, но не является полномочием повторить его.

Нативное принятие решения отделить от исполнения инструмента. После потери ответа Node нельзя объявить ни success, ни no-effect без доказательства. Использовать exact retained decision/readback; без него сохранить unknown. Никаких retries prompt, automatic interrupt или restart.

### 5. Закончить артефакт и документацию

Обновить только выбранный Rust+Node artifact и реальные installers/descriptor schemas/callers; legacy bridge.3 не превращать во второй параллельный новый executor. В его README исправить неточное описание default и обозначить оставшийся old capability. Для нового артефакта согласовать IDs/digests/examples по UPDATE; SDK pin не повышать без необходимости. Не переписывать активные конфигурации или чужие checkpoint.

## Итоговые сценарии — не выполнены этой документацией

| Сценарий | Ожидаемый исход |
|---|---|
| Незаданный mode / explicit default / auto-approved tool | Честная configured/effective projection; отсутствие callback не выдумывается. |
| Root permission и AskUserQuestion | Scoped attention → ответ → один исходный callback, без нового model input. |
| Cancel до reply / reply до cancel / одинаковый повтор | Один settled callback; поздний не меняет новый запрос. |
| Тот же toolUseID в другом boot либо изменённый input | Отказ по identity/fingerprint до resolve. |
| Host reconnect при живом driver / driver loss | Pending переживает первое, не фабрикуется после второго. |
| Reader медленный, очередь/размер исчерпаны | Явный gap/отказ, не потерянный запрос и не auto-allow. |
| SDK принял allow, tool ещё работает | Решение доставлено; task/child completion не заявлена. |

## Сдача и непересечения

После кода manager выполняет scoped formatting и минимальный Clippy затронутых packages, плюс `node --check crates/swarm-adapter-claude/sdk-harness/bridge.mjs`. Broad tests/native — итоговая фаза. Сдать exact SHA, callback→Store→reply caller chain, фактический gate и unsupported cases. Будущие критерии не отмечать выполненными по syntax/docs CI.

R05/#31 владеет candidate provenance — не менять его acceptance/origin checks. R15/#41 — Codex/Muse quota и transport, не расширять его до Claude permissions. R14/#40 позже переносит schema data; здесь только необходимые additions. Один manager/worktree; writers без Cargo. Новые lifecycle repair, goals, общий ACP runtime и quota collectors не входят в этот блок.
