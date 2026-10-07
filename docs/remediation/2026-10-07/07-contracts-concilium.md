# R07. Контракты и Concilium: proposal → решение → терминальное обсуждение

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-003, AUD-018, AUD-019, AUD-020.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Proposal revision читается тем же контрактом, которым записывается; manager ratification замыкает resolved thread; закрытый Concilium имеет согласованные terminal states и различимую историю раундов.

## Читать адресно

- [docs/agent-communication-tool-contracts.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-tool-contracts.md) — §7.3, §8.1–8.3, §11.4–11.6: thread closure, ratify, rounds, advisory result.
- [docs/agent-communication-concilium.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-concilium.md) — proposal/ratification и terminal/round contracts.
- [docs/agent-communication-implementation-issues.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-implementation-issues.md) — блок contract decision; continuation материалы — только черновики, не готовый handler.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/{coordination.rs,coordination_threads.rs,concilium.rs}`: proposal writer, `load_proposal_revision`, `validate_contract_ratification`, `close/submit_position`; соответствующие `src/coordination/{contract.rs,concilium.rs,thread.rs}`. Только добавления методов в существующие METHOD_REGISTRY/MCP/CLI. Проверить `docs/continuation/2026-10-06/contract-decisions-v2/`: .rs.txt не исполняемый producer.

## Что и как сделать

1. Использовать единый versioned ProposalRevision DTO с определёнными именем digest и preimage. Согласовать writer, persistent record, thread reader и operation readback; старые записи не переименовывать вслепую.
2. Проверить наличие production ratify/reject пути. Незавершённый нормативный contract-decision V2 довести producer → parser/registry → transactional handler → receipt → thread consumer, используя существующие документы/черновик как материал, не как доказательство реализации.
3. Ratify проверяет revision/digest proposal, текущие Task/Attempt/affected-scope revisions, current manager authority и отсутствие superseding decision; запись decision и события атомарна. Reject и stale-сценарии не фабрикуют ratification. Contract thread resolved только с точным settled decision.
4. Закрытие Concilium согласует record, round, slots и active-subject index в одной транзакции. После close новое содержимое позиции не меняет результат/не открывает record; исторический точный replay не получает новых полномочий.
5. По §11.6 сохранить все valid positions и dissent. Не объявлять их наличие багом: AUD-019 — неоднозначность summary. Отметить round/revision provenance и отделить выбранную recommendation от истории, не заменяя её голосованием или скрывая старые позиции.

## Критерии готовности

- [ ] Записанная proposal revision проходит linked send/read/resolve без расхождения digest.
- [ ] Реальный ratify делает допустимым exact resolved thread; произвольная Operation или иной proposal — нет.
- [ ] Close оставляет согласованную terminal projection; новая позиция после close её не оживляет.
- [ ] Раунды/valid history/выбранная recommendation различимы; advisory результат не принимает Task и не запускает модель.

## Границы и интеграция

Не новый consensus engine. AUD-020 был source gap, не доказательством отсутствия всех generators; проверка production wiring обязательна до правки. Не ослаблять validator до любой settled операции и не переносить Claw workflow store.

Интеграция после R06: использовать его work context/fingerprint/current-scope projection. Здесь владельцы proposal/decision/Concilium lifecycle, не mailbox и не code-scope accept. С R14 согласовать только registry additions; перенос frontend выполнять позднее.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-contracts -p swarm-mcp -p swarm-cli --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
