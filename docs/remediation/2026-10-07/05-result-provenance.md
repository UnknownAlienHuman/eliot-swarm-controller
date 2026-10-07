# R05. Результаты: exact Attempt и согласованный dispatch receipt

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-004, AUD-023.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Кандидат Participant принадлежит именно ожидаемой Task revision/Attempt; вложенные dispatch receipts согласованы между собой и с authenticated command.

## Читать адресно

- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §1.3: submission/review identity.
- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §3: normalized dispatch pair и Store expected identity.
- [docs/agent_swarm.module-contract-v2.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent_swarm.module-contract-v2.md) — §4: результаты, evidence и неизвестные исходы.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/submissions.rs`: `candidate_for_attempt`, `authorize_participant_candidate`, Claude origin branch; `store/results.rs`: Claude origin helpers; `store/normalized_result.rs` — образец уже работающей expected-Attempt проверки. `crates/swarm-contracts/src/runtime.rs::TaskDispatchAdmissionReceipt::validate` и реальные constructors/callers.

## Что и как сделать

1. Передавать ожидаемые Task ID/revision, Attempt, binding/generation в legacy Claude candidate validation. Сверять их с retained provenance до принятия candidate, включая путь с ранним continue.
2. Не заменять это проверкой самосогласованности одного чужого receipt: binding/session могут обслуживать разные Attempts. Сохранить текущий normalized-result validator, не создавать альтернативный слабый путь.
3. В TaskDispatchAdmissionReceipt::validate/constructor добавить равенство повторённых operation_id, binding_id и generation outer/inner частей. Сохранить внешнюю Store-проверку на authenticated command и immutable Task snapshot.
4. Согласовать реальные producers, затронутые усилением validate, в этом же diff. Не глобализовать vendor-specific origin DTO в contracts; общий тип содержит только общий контракт.

## Критерии готовности

- [ ] Корректный Claude result своей Attempt остаётся допустимым.
- [ ] Результат старой Attempt на том же binding/generation не принимается за новую.
- [ ] Несовпадение каждого повторённого outer/inner ID отвергается до persisted admission.
- [ ] Самосогласованный receipt другой операции всё равно отвергается expected-command проверкой.

## Границы и интеграция

Это усиление конкретных consumers, не доказанный универсальный exploit. Не заменять candidate identity branch/session ID, не изменять acceptance policy и не отключать legacy history reads без версии декодера.

Самостоятельный контрактный блок. R02/R03 не меняют поля этого DTO; при необходимости исправления их constructors согласовать точечно, без параллельной правки одного участка. Минимальный gate включает затронутых consumers, а не весь workspace.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-contracts -p swarm-kernel-host --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
