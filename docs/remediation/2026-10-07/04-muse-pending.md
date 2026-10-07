# R04. Muse: вопросы и approvals не теряются при refresh

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
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
