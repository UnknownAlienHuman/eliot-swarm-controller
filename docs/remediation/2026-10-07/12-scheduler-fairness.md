# R12. Планировщик: изоляция источников и честное продвижение очереди

**Статус: задание на реализацию; текущая поставка содержит только спецификацию.**
Основа: аудит редакции 3, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки AUD-030/AUD-032 и подтверждённый DST-boundary defect.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Ошибка одного due-source не блокирует обработку независимых источников; stale/no-progress ответы не вызывают горячий retry. Выдача credentials продвигает cursor по рассмотренной работе и не делит pacing между Stores. Valid calendar instant внутри DST overlap не превращается в ошибку собственной ELIOT-предобработкой.

## Читать адресно

- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §2.1 и optional automation: один due worker, атомарный cursor/action.
- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §4: stable schedule slots, coalesce_latest, admission и drain.
- [docs/swarm-launcher-assignment-context.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/swarm-launcher-assignment-context.md) — participant credential issuance и retained launch context.
- [DST companion](../2026-10-08/12-calendar-dst.md) — точная Chrono/Croner boundary, минимальный код и fold fixtures.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторические SHA/версии фиксируют источник, но не ограничивают используемые updates.

## Участок кода

`crates/swarm-kernel-host/src/store/automation_scheduler.rs::call`; `crates/swarm-automation/src/bin/worker.rs::run_worker/call_once`; `store/launcher_issuance.rs` page/cursor/backoff/tick ownership; `scheduler/calendar.rs::local_second_at` и его четыре callers. Вложенные goal/check reconcilers — только нужные error/result seams, без рефакторинга всего домена.

## Что и как сделать

1. Отделить предметную ошибку источника/записи от неисправности Store. Независимые due sources обрабатывать с отдельными bounded outcomes; использовать существующие transaction/savepoint boundaries там, где они требуются. DB/commit failure не скрывать.
2. Для read и admit ввести единый no-progress backoff. Сбрасывать его по реальному продвижению cursor/state, не по любому Ok. page_stale/already_observed без прогресса не образуют tight loop.
3. Повтор после uncertain admission начинается с authoritative readback; не повторять прежний effect. Reconnect/IPC ownership упорядочить внутри имеющегося worker, не заводить таймер/процесс на правило.
4. Выдачу credentials ограничивать бюджетом реально рассмотренных элементов: cursor не перескакивает хвост выбранной страницы. Wrap явно определён. Unknown регистрации остаются readback-only; не создавать новые credentials вместо неизвестного результата.
5. Убрать process-global OnceLock cursor/backoff из этого workflow в worker/Store-owned transient state. Сохранить первоначальную ошибку даже при отказе записи diagnostics. В конечной очереди старый алгоритм всё же завершает работу — не оформлять это как доказанную вечную потерю.
6. В calendar helper округлять миллисекунды на UTC timeline **до** timezone conversion. Удалить post-timezone `DateTime<Tz>::with_nanosecond(0)`, которое remap-ит local wall time и возвращает None в DST fold. Не менять Croner semantics, occurrence identity, Store cursor или dependency version ради этой правки.

## Критерии готовности

- [ ] Ошибка одного calendar/source оставляет достижимым независимый goal reminder; DB failure не рисует success.
- [ ] Повтор stale/no-progress приводит к bounded retry, а реальный прогресс снимает pacing.
- [ ] Очередь больше tick budget обслуживается с явным wrap; новые поступления и повторяющийся failure не скрывают хвост.
- [ ] Два экземпляра Store в одном процессе не используют общий cursor/backoff; unsure credential issuance не выполняется заново.
- [ ] `latest_due`, `next_due_at_ms` и preview, вызванные в обеих сторонах New York/Berlin fold, не ошибаются и выдают строго возрастающие UTC instants.
- [ ] Fixed-time one-shot и wildcard real-instant semantics совпадают с документированным Croner contract; spring-gap regression остаётся зелёным.

## Границы и интеграция

Не добавлять deadlines для model task, лимит «две попытки работы», новый scheduler или storage. Native unknown отличается от безопасной повторной доставки сохранённого receipt. DST fix не является своим cron engine, timezone policy или dependency pin.

R34/#60 владеет inner automation poison-fact isolation и per-domain transactions. R12 владеет независимыми due sources, worker pacing, issuance fairness и calendar input boundary. R02 меняет adapter IPC — это другой владелец.

## Проверка и сдача

Один manager и один worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для helper/tests/calendar. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-automation --lib --bins -- -D warnings
```

Предметный календарный test target на итоговой фазе:

```sh
cargo test --locked -p swarm-kernel-host scheduler::calendar
```

Полные tests, native/live и нагрузочные прогоны — отдельная итоговая фаза. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, удалённую неверную local-time boundary, результат gate и оставшуюся неопределённость. Draft не переводить в Ready, пока здесь только задание.
