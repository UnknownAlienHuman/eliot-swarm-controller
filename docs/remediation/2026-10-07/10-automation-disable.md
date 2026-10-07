# R10. Автоматика: точный ID и надёжное выключение

**Статус: реализация добавлена в PR #36; Draft до минимальной компиляционной проверки.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-039, AUD-040.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Валидное enabled=false фиксируется независимо от полноты affected-work диагностики; допустимые automation IDs никогда не смешиваются SQL wildcard matching.

## Читать адресно

- [docs/agent-operations/configuration.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/configuration.md) — automation.config.preview/apply, revision guards, disable/narrowing и affected work.
- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §4: effective policy и уменьшение церемонии.
- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §1.4: observation не меняет уже принятую работу.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/automation.rs`: operation_impacts, apply, explain; `src/automation/config.rs`: entry_operation_prefix, ID validator; mutation savepoint в `store/mod.rs` читать как границу, не переписывать весь dispatcher.

## Что и как сделать

1. Заменить LIKE по необработанному automation_id на точную индексируемую prefix range либо корректное literal escaping. Проверить underscore и ASCII-case: поиск не должен объединять audit_one/auditXone и отличающиеся регистром ID, если identity регистрозависима.
2. Сохранить повторную сверку реального on-behalf link с owner/project/automation. Исправление — не убрать guard, а не подбирать чужую запись.
3. Отделить обязательные owner/revision/config checks от необязательного affected_work readback. Ошибка конкретной производной ссылки/диагностического участка не откатывает уменьшение полномочий.
4. Фиксировать disable и его durable receipt; affected_work с неполнотой возвращать отдельным typed partial/error section. Ошибку БД/commit нельзя подавлять и выдавать за успешное выключение; raw повреждённый link не копировать в публичный ответ.
5. Сохранить порядок: новые admissions после disable запрещены, уже принятый неизвестный внешний эффект не отменяется этим действием. Удалить дублирующую диагностическую зависимость transaction от полного history read.

## Критерии готовности

- [ ] При audit_one и auditXone с активными операциями выборка каждого ID содержит только его записи.
- [ ] Повреждение derived link не откатывает разрешённый enabled=false; ответ явно сообщает partial диагностику.
- [ ] Неверный owner/revision и настоящий отказ commit не дают успешного receipt.
- [ ] После disable новый admission невозможен; native работа не убивается и uncertain effects не повторяются.

## Границы и интеграция

Без массовой замены всех LIKE: hashed owner/project prefixes не тот же дефект. Без нового ACL, DB writer или отключения auth checks. Проверить SQL план точного запроса, не обещать throughput по исходнику.

Независимый блок. R11 владеет event payloads, R12 — scheduler pacing. Их завершения для исправления выключения не требуется.

## Реализация 7 октября 2026

В `store/automation.rs` запрос `operation_impacts` использует бинарный диапазон
ключей вместо `LIKE`. Отсутствующая, несовместимая или повреждённая ссылка
закрывает весь диагностический список через существующий `closed`-контракт;
проверенные до неё элементы не выдаются как полный результат. `apply` и
`explain` используют один путь. Проверки authority/revision, запись конфигурации,
допуск действий и commit не ослаблены. Контракт уточнён в текущем
`docs/agent-operations/configuration.md` §2–2.1.

Проверены точные исходные blob SHA и `git diff --check`. План SQL исследован
отдельно на пустой схеме SQLite; это не прогон Store и не замер производительности.
Clippy не запущен: в рабочем контейнере отсутствует `cargo` (exit 127).
Поведенческие критерии выше ещё не исполнены; тесты и native-вызовы не запускались.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
