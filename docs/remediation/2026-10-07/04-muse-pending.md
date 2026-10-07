# R04. Muse: вопросы и approvals не теряются при refresh

**Статус: код bridge.8 добавлен в PR #30; поведенческая и native-квалификация ещё не выполнены. Draft.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточка: AUD-025.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Root/child attention сохраняет новый вопрос и не воскрешает уже разрешённый при гонке server request/notification с approval/listPending.

## Читать адресно

- [modules/muse/README.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/muse/README.md) — MSP transport, pending requests, observation/recovery.
- [modules/muse/UPDATE.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/muse/UPDATE.md) — закреплённый SDK и проверка native форм.
- [docs/agent_swarm.module-contract-v2.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent_swarm.module-contract-v2.md) — §7: attention, формы и exact reply.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`modules/muse/bridge.mjs`: `onNotification`, обработчик `onServerRequest`, `refreshRoot`, `refreshChild`, `sessionVersions`, `pendingRequests`. Чтение relevant SDK envelopes — адресно, без переписывания SDK.

## Что и как сделать

1. Сделать единый локальный путь учёта изменения pending inventory для server requests и notifications: оба меняют тот generation, который действительно сверяют refresh-функции.
2. Обработать создание, изменение и разрешение вопроса одинаково для root и наблюдаемого child; изменение общего revision не заменяет session freshness.
3. Применять полный inventory только при неизменном generation окна read. При гонке сохранять новые локальные факты и отмечать snapshot stale; не удалять вопрос только потому, что его нет в запоздавшем ответе.
4. Сохранить RequestReceipt {} как подтверждение представления, а не решение approval. Не отправлять автоматический ответ, не вызывать дополнительную модель; удалить дубли счётчиков только если их отдельные consumers больше не нужны.

## Критерии готовности

- [ ] Вопрос, пришедший между началом listPending и его старым ответом, остаётся в attention.
- [ ] Разрешённый в том же окне вопрос не возвращается из старого inventory.
- [ ] Одинаковое поведение для root/child и повторных событий.
- [ ] Наблюдение не вызывает approval decision, reply или новый model turn.

## Границы и интеграция

Это ошибка нашего моста; задание не утверждает утечку или однопоточность Muse. Не вводить ротацию по возрасту, отдельный poller, новый persistence layer или авторизацию Recommended.

Полностью независимый блок. Не менять общий method registry или Rust frontend.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
node --check modules/muse/bridge.mjs
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.

## Реализация и доказательства — 7 октября 2026

В `bridge.mjs` общий `sessionChanged` связывает server requests и notifications с
проверяемой версией снимка. Общий `replacePendingInventory` сначала проверяет
весь результат и его session/native IDs, затем синхронно заменяет только одну
сессию при неизменной версии. Дубли ID и повреждённая последняя запись не
позволяют частично стереть прежний inventory. Root/child публикуют отдельный
`pending_inventory_applied`. Частичный `approval/updated` сохраняет исходную
identity и заменяет поля этапа; отсутствующий optional `subagentOrigin` не
наследуется от прошлого этапа.

Проверен [первичный SDK 1.3.0, ApprovalRouter.updated](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/approval.ts):
использовано разделение исходного вопроса и обновления этапа, не сам router
автоматических решений. Новых зависимостей, polling-циклов или хранилищ нет.

По UPDATE.md новый код имеет ID `muse-sdk-1.3.0-bridge.8`; пример маршрута,
текущие README и два ID в существующем selftest согласованы. Публикация правки
`modules/muse/module.example.json` заблокирована инструментом; в PR этот файл
пока сохраняет bridge.7. До согласования примера PR нельзя активировать или
считать готовым. Исторический checkpoint fixture не переписан. Старые binding/checkpoint не мигрируются,
действующие сервисы не активировались и не останавливались.

Выполнено на локальном source-кандидате: `node --check` для `bridge.mjs` и
`selftest.mjs` (Node 22.16.0), `git diff --check`, синтаксический разбор JSON/TOML
примеров. Полный локальный кандидат имел согласованные ID; опубликованный
кандидат намеренно отмечен неполным из-за оставшегося примера bridge.7. Исходные файлы перед правкой сверены с Git blob
SHA. Это не запуск selftest, не SDK import и не end-to-end проверка гонок.
Rust-код не изменён; Cargo/Clippy и широкие тесты не запускались.

В итоговой фазе дополнительно проверить malformed-last-record, чужой session ID,
duplicate ID и multi-stage approval с удалённым optional origin. Чекбоксы выше
не отмечены выполненными по одному чтению кода или проверке синтаксиса.
