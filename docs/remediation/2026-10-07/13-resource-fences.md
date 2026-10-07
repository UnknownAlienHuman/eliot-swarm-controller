# R13. Ресурсы: capacity evidence и освобождение exact workspace lease

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-016, AUD-017, AUD-022, AUD-031.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Capacity не превращает повреждение в нулевую загрузку; точный terminal освобождает своё исполнение; новая заявка той же Task не удерживает ресурс старой Attempt. Collision reports честно различают filesystem competition и integration risk.

## Читать адресно

- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §1.2–1.4 и §2.2: worktree ownership, exact evidence и no eviction.
- [docs/agent-communication-tool-contracts.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-tool-contracts.md) — §9.2–9.5 и §10.6: accepted/advisory scope, release, coverage.
- [docs/swarm-launcher-assignment-context.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/swarm-launcher-assignment-context.md) — workspace lease, prepared launch и binding identity.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/{capacity.rs,workspace.rs,workspace_lifecycle.rs}`: load_ledger/derive_operation/sync, reject_scope_conflicts, has_unresolved_native_work, transition_for; `store/code_scopes.rs` только active/conflict readers и expiry projection. Использовать общий work context R06, не менять его формат.

## Что и как сделать

1. load_ledger должен различать отсутствие ledger и повреждённую существующую форму. Не заменять второе пустым и не перезаписывать прочие reservations при следующем sync. Восстановление только из проверенных retained facts с явным результатом по scope.
2. Свести приоритет native evidence для одного exact execution: его terminal не затеняется ранним start. Чужой/несопоставимый terminal остаётся unknown; Applied admission не подменяет execution completion.
3. Заменить task-wide удержание lease на проверенную resource attribution: exact lease/binding/generation/Attempt/process family и операции, которые действительно могли использовать старый ресурс. Новая queued swarm.launch без этого ресурса не старый владелец.
4. Проследить обе стороны reserve/release: не освобождать preparing/outcome_unknown/stale по TTL. Требуется подтверждённый no-effect/departure либо поддержанная адресная operator recovery с сохранёнными доказательствами.
5. Согласовать code-scope advisory и workspace lease collision domains. Общий mutable worktree — write conflict; независимые worktrees одного repo — integration risk, не автоматически физический lock. Истёкший active advisory не подтверждает уход writer; ответ содержит область поиска и partial/unknown coverage. Удалить только дубли predicates, относящиеся к той же гарантии.

## Критерии готовности

- [ ] Повреждённый ledger не становится пустым/available и не уничтожает соседние reservations.
- [ ] Start + terminal одной execution дают корректное освобождение; чужой terminal не освобождает её.
- [ ] Новый queued launch без старого resource не удерживает старую lease; реальный unknown owner продолжает её защищать.
- [ ] Shared worktree, отдельные worktrees, read-only review и expired active scope представлены различимо, без false clean.

## Границы и интеграция

Полный deadlock AUD-031 пока условен: проверить внешний launcher перед заявлением о вечной блокировке. Capacity в аудите не доказан как admission limiter — не вводить произвольные лимиты или route failover. Не переписывать весь ledger в новую БД.

Интегрировать после R06 (общий context и code-scope accept). Здесь владелец resource/collision readers, R06 — proposer/accept identity. R01/R02 сохраняют процессные доказательства, их API не переопределять.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
