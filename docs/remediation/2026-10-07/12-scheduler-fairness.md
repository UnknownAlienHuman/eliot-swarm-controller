# R12. Планировщик: изоляция источников и честное продвижение очереди

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-030, AUD-032.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Ошибка одного due-source не блокирует обработку независимых источников; stale/no-progress ответы не вызывают горячий retry. Выдача credentials продвигает cursor по рассмотренной работе и не делит pacing между Stores.

## Читать адресно

- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §2.1 и optional automation: один due worker, атомарный cursor/action.
- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §4: stable schedule slots, coalesce_latest, admission и drain.
- [docs/swarm-launcher-assignment-context.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/swarm-launcher-assignment-context.md) — participant credential issuance и retained launch context.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/automation_scheduler.rs::call`; `crates/swarm-automation/src/bin/worker.rs::run_worker/call_once`; `store/launcher_issuance.rs` page/cursor/backoff/tick ownership. Вложенные calendar/goal/check reconcilers — только нужные error/result seams, без рефакторинга всего домена.

## Что и как сделать

1. Отделить предметную ошибку источника/записи от неисправности Store. Независимые due sources обрабатывать с отдельными bounded outcomes; использовать существующие transaction/savepoint boundaries там, где они требуются. DB/commit failure не скрывать.
2. Для read и admit ввести единый no-progress backoff. Сбрасывать его по реальному продвижению cursor/state, не по любому Ok. page_stale/already_observed без прогресса не образуют tight loop.
3. Повтор после uncertain admission начинается с authoritative readback; не повторять прежний effect. Reconnect/IPC ownership упорядочить внутри имеющегося worker, не заводить таймер/процесс на правило.
4. Выдачу credentials ограничивать бюджетом реально рассмотренных элементов: cursor не перескакивает хвост выбранной страницы. Wrap явно определён. Unknown регистрации остаются readback-only; не создавать новые credentials вместо неизвестного результата.
5. Убрать process-global OnceLock cursor/backoff из этого workflow в worker/Store-owned transient state. Сохранить первоначальную ошибку даже при отказе записи diagnostics. В конечной очереди старый алгоритм всё же завершает работу — не оформлять это как доказанную вечную потерю.

## Критерии готовности

- [ ] Ошибка одного calendar/source оставляет достижимым независимый goal reminder; DB failure не рисует success.
- [ ] Повтор stale/no-progress приводит к bounded retry, а реальный прогресс снимает pacing.
- [ ] Очередь больше tick budget обслуживается с явным wrap; новые поступления и повторяющийся failure не скрывают хвост.
- [ ] Два экземпляра Store в одном процессе не используют общий cursor/backoff; unsure credential issuance не выполняется заново.

## Границы и интеграция

Не добавлять deadlines для model task, лимит «две попытки работы», новый scheduler или storage. Native unknown отличается от безопасной повторной доставки сохранённого receipt. Cron DST из чужого реестра не чинить без отдельного доказательства.

Самостоятельный блок, не требует R10/R11. R02 меняет adapter IPC, здесь другой владелец automation IPC. При изменении ответа scheduler обновить producer/worker consumer совместно.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-automation --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
