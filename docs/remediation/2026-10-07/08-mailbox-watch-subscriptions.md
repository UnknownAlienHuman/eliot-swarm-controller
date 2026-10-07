# R08. Доставка: монотонный inbox, deadline-watch и точный subscription cutoff

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-006, AUD-007, AUD-009, AUD-010, AUD-014.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Сохранённые сообщения не теряются за курсором; live subscription начинается с объявленного cutoff без чтения всей истории; watch корректно обрабатывает deadline и поздний тик.

## Читать адресно

- [docs/agent-communication-tool-contracts.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-tool-contracts.md) — §6.5 и §7.1–7.2: delivery identity и bounded cursor reads.
- [docs/agent-communication-peer-autonomy.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-peer-autonomy.md) — watch, deadline, expiry и отсутствие автоматического model wake.
- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §2.1: durable sequence/cursor, volatile hints и атомарность.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/coordination/mod.rs::mailbox_key`; `store/coordination.rs` inbox/send indexing; `store/coordination_watch.rs` create/reconcile; `crates/swarm-mcp/src/mcp/subscriptions.rs` open/poll/scan_to_head/lagged. Только необходимые Store IPC/schema additions. Миграция затронутых индексов — в существующей БД.

## Что и как сделать

1. Заменить timestamp+UUID ordering на существующий observation/mailbox sequence, выделяемый в той же transaction, что доставка. Timestamp оставить отображением; повтор client_request_id не выдаёт новый delivery identity.
2. Развести cursor последней просмотренной записи и последнего выданного сообщения. Полностью stale-окно возвращает partial/gaps и продвигаемый scan cursor, если история продолжается. Не переносить LIMIT перед auth и не выдавать пустую страницу за конец.
3. Версионировать wire cursor и привязать к scope. Для старого cursor дать явный resync, сохранив payload/дедупликацию; не трактовать timestamp как новый sequence. Согласовать writer/index/reader в одном PR.
4. При cursor-less subscribe получить bounded Store high-water и вернуть в ACK. Покрыть промежуток регистрации listener/cutoff; explicit after сохраняет replay. scan_to_head возвращает совместный progress cursor+dropped+reached_head+error, чтобы частичный сбой не удваивал drops.
5. Определить deadline отдельно от expiry наблюдения/доставки: equal boundary и delayed tick имеют предметную семантику. Проверка current authority обязательна до раскрытия. Не менять два if местами для всех watch вслепую; доставку сохранить one-shot/idempotent.

## Критерии готовности

- [ ] Позднее сообщение той же миллисекунды и откат wall clock не пропускаются последующей страницей.
- [ ] За полным stale scan window доступно следующее валидное сообщение; revoked scope ничего не раскрывает.
- [ ] Live subscribe с большой историей делает ограниченную начальную работу; событие после cutoff доставлено либо явно покрыто gap/resync.
- [ ] Частичный lagged scan не повторно считает пройденные записи; expiry <, =, > deadline и поздний тик имеют задокументированные исходы.

## Границы и интеграция

Доноры: CCCC — locality, Paseo — subscription ownership/detach; не переносить их ledger, TS manager или отдельный broker. Закрытие подписки освобождает наблюдение, не native Task. Никаких model calls из watches.

R06 владеет work context и participant listing; здесь только inbox/send-index, watch и subscriptions. Можно готовить независимо; при интеграции принять типы R06, не создавать копию. R14 не переносит subscriptions до завершения этого блока.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-mcp --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
