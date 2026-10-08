# R16. Claude: рабочая подписочная сессия → вопрос → точный ответ

**PR #42 · исправлено 8 октября 2026 · реализация ещё не завершена.** Source review ELIOT: 40591a295af94b1541ec2ba30afe8e3247701a71. Это координата проверенного кода, не версия, на которой владелец должен оставаться.

## Результат и границы

Менеджер отвечает через agent.reply исходному живому permission/question callback. Нативная подписочная авторизация, настройки, инструменты и model loop остаются прежними. Не требовать отдельный API key, новый billing route, фиксированный SDK/CLI release или downgrade. Отсутствие нужного интерфейса у одной библиотеки не означает отсутствие работающего подписочного Claude Code.

Работать в существующем Rust adapter + Node driver, а не создавать второй executor. До расширения driver проверить, что он использует именно текущую авторизованную нативную установку. Если прежняя SDK-граница сама подменяет этот путь, исправить границу подключения к тому же harness; не компенсировать её отдельным inference API. Не выдумывать новый CLI control verb.

## Первый участок и источники

Открыть `crates/swarm-adapter-claude/sdk-harness/bridge.mjs::prepare`, затем `handle` и реальные Rust consumers. Сегодня prepare отвергает packageInfo.version !== '0.3.287', затем проверяет sdk.startup; canUseTool немедленно отвечает deny, handle поддерживает prepare/send/stop, CAPABILITIES не содержит agent.reply.

Читать [модульный контракт](../../agent_swarm.module-contract-v2.md) для identity/replay, [owner decisions](../../owner-decisions.md) §1.2–1.4 для manager/worktree, и актуальную [документацию native permissions](https://code.claude.com/docs/en/agent-sdk/permissions)/[user input](https://code.claude.com/docs/en/agent-sdk/user-input) вместе с определениями фактически установленного интерфейса. Старое требование сверять только pin 0.3.287 удалено. Полные SDK types этим аудитом не были извлечены; их поддержку нельзя угадать по имени функции.

Материалы владельца: ELIOT-Swarm-AUDIT-2026-10-06(2).md §§B4.2/B5/B9. В них Claude Code — действующий root, а новый controller adapter имеет отдельный, неполный статус. Не приписывать ему паритет с работающей сессией.

## Существующая цепочка

| Участок | Изменить / сохранить |
|---|---|
| `sdk-harness/bridge.mjs::prepare`, `sdkImportEntry` | Убрать единственный release-equality gate. Проверить имя выбранного пакета, реальный export/используемые формы; reported version оставить наблюдением. Не подмена следующей константой. |
| `prepare.canUseTool`, `handle`, `pump`, `safeSdkFrame` | Живой pending callback и closed pending/reply/cancel frames; reader команд не блокируется ожиданием решения. |
| `sdk-harness/prepared-query.mjs::prepareQuery` | Сохранить одноразовую семантику native prepared handle, когда этот интерфейс реально предоставлен. Reply не запускает query повторно. |
| `src/sdk_harness.rs::NativeHarness`, `HarnessFrame` | Новые сообщения по существующему Node/Rust транспорту, без Node-процесса на каждый вопрос. |
| `src/lib.rs::handle_command`, `CommandInvocation`, `open_link` | Существующий agent.reply, durable Operation и exact request/boot; reconnect не повторяет prompt. |
| `src/native_state.rs::NativeControl` | Current pending отдельно от historical denial/cancelled; не показывать разрешённый callback как всё ещё ожидающий. |
| `src/journal.rs::OperationJournal`, `src/receipt.rs` | Повтор неизменной операции читает сохранённый исход; конфликт не разрешает native callback ещё раз. |
| `src/config.rs::NativeOptions`, `src/module_runtime.rs::CAPABILITIES`/`capabilities_match` | Объявить только действительно подключённый reply; current model/permission из выбранного маршрута, не исторические числа. |
| Host `store/module_handshake.rs::native_command_capability`, `store/runtime.rs`, `store/capacity.rs` | Подключить observation → scoped attention → admission; необходимые schema/registry/frontend consumers в том же PR. |

Готового runtime/claude.rs на этой базе нет. Найти реальные callers через `git grep -n 'agent.reply' -- crates`; новый public endpoint на каждый SDK tool не требуется.

## 1. Текущий runtime вместо замороженного номера

Смена совместимого установленного SDK не должна сама давать SDK_VERSION_MISMATCH. Проверять обязательные exports и формы выбранных операций. Неподдержанный метод возвращает точную capability gap; не пытаться запускать старый bundled executable или платный API ради её устранения. Существующее приложение и нормальная авторизация не переключаются.

Одного удаления if недостаточно: проверить package-loading, использованные native options, truthful executor-version projection и callers. Старые 0.3.287 в artifact IDs/отчётах — сведения о той сборке, не подтверждение версии текущего executor. Не подделывать reported version и не изменять работающую сессию на месте. UPDATE/package metadata должны перестать предписывать freeze пользователю; идентичность фактически поставленных bytes ELIOT сохраняется.

Для незаданного permissionMode показывать requested=inherited, effective — только после native observation. Явную настройку владельца передавать неизменной. Не включать bypass/default ради прохождения теста. canUseTool не обязательно вызывается для каждого native tool; отсутствие callback не является transport failure. История уже выданных deny не превращается в pending.

## 2. Удержать один живой запрос

Node хранит bounded map callback resolvers. Native toolUseID, локальный callback ID, current boot, подтверждённая session и fingerprint исходного input — разные поля. Локальный ID не объявлять native turn ID. Оригинальный input и Promise остаются у владельца; Store получает разрешённую компактную карточку и reference.

Для допустимого интерактивного пути callback ждёт, но reader команд, stream pump и IPC продолжают работать. Не await-ить решение в цикле чтения команд. Усечение карточки не меняет original fingerprint; incomplete/unanswerable явно показывается вместо молчаливого исчезновения вопроса или auto-allow. Budget — техническая граница канала, не новый лимит агентской работы.

## 3. Провести ответ через всю цепочку

Authenticated module.observe с existing binding/boot/sequence проверками → scoped attention → current-authority agent.reply → OperationJournal intent → Node decision frame → повторная проверка callback identity/state → один resolve. Новый helper без production caller не поставляется.

Closed reply содержит request reference, fingerprint и allow-once/deny либо ответ исходным вопросам. Разрешение обычного tool использует сохранённый input; произвольное редактирование аргументов и persistent allow-always не включать в этот блок. Осмысленный deny — доставленное решение, не обязательно Rejected ELIOT Operation.

AskUserQuestion отвечать по реально предоставленной native форме questions/answers, не новым prompt. Проверить repeated question keys, free-text/multi-select, усечённые варианты и возможности детей на текущем интерфейсе; не обещать child support по наличию parent callback. Raw input/credentials не раскрываются через generic observation reader.

## 4. Отмена и неопределённость

AbortSignal, reply и закрытие native query состязаются за одно pending state. Поздний reply не возрождает callback. Одинаковый повтор возвращает записанный результат; изменённый payload — конфликт. Listener снимается после settle/cancel.

Host IPC disconnect при живом Node не уничтожает pending. После гибели Node старый Promise нельзя восстановить из JSON: отметить потерю живого callback, сохранить известный outcome/unknown и не отправлять решение или prompt заново. ACK callback не означает завершение tool, child или Task.

Не смешивать исправление с новым native stop/restart, goal engine или quota collector. Живые сервисы и user credentials не меняются автоматически.

## Итоговые критерии — пока не выполнены

| Сценарий | Ожидаемый результат |
|---|---|
| Совместимый runtime обновился | Нет отказа только из-за release number; реальная версия и capabilities наблюдаются заново. |
| Подписочный запуск через существующую авторизацию | Нет требования отдельного API key/счёта и silent backend switch. |
| Вопрос/разрешение root | Одна карточка → один ответ → исходный callback без нового model input. |
| Inherited/explicit permission mode; auto-resolved tool | Requested/effective/history честно разделены. |
| Reply/abort в обоих порядках, duplicate/conflict | Один settle; новый запрос не затронут. |
| Тот же toolUseID в другом boot; изменённый input | Guard отвергает до эффекта. |
| Host reconnect / Node loss | Pending сохранён в первом случае; во втором не фабрикуется и не replay-ится. |
| ACK allow, инструмент ещё работает | Решение доставлено, завершение Task не заявлено. |

## Сдача

Один manager/worktree, writers без Cargo. Реализацию добавлять в этот PR целиком; scoped formatting, минимальный Clippy затронутых packages и node --check изменённого driver после кода. Broad/native tests — итоговая фаза. Сдать SHA, removed version gates, подтверждённый путь подписки, connected callback→reader→reply и реальный gate.

R05/#31 владеет result provenance, R15/#41 — Codex/Muse quota, R14/#40 — schema extraction. Эта правка меняет задание; literal gate и новые replies ещё не исправлены в production. Прежний docs CI не подтверждает будущую реализацию.
