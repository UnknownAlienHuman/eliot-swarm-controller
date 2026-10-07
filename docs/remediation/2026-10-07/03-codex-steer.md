# R03. Codex: точный steer в долгоживущем thread

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточка: AUD-012.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Steer текущего expected_turn_id работает независимо от количества завершённых ходов и сохраняет native atomic target guard.

## Читать адресно

- [docs/agent_swarm.module-contract-v2.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent_swarm.module-contract-v2.md) — §1 «Классы доставки input», §4 send и replay policy.
- [modules/codex/UPDATE.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/codex/UPDATE.md) — различие Rust controller и Python bridge; текущие capabilities.
- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §3: adapter owns native interpretation.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-adapter-codex/src/lib.rs`: reader истории, вычисление `page_limited`, admission/translation `agent.send` с steer и `expected_turn_id`. `module_contract.rs` менять только если действительная wire-форма меняется; не переносить весь Codex transport.

## Что и как сделать

1. Проследить exact steer путь от RuntimeCommand к native turn/steer. Убрать использование ограниченной исторической страницы как доказательства отсутствия активной цели.
2. Подтверждать нужный текущий turn адресным native read/существующим current-state полем и передавать обязательный expectedTurnId серверу. Историю читать только при реальной необходимости; не сканировать весь thread на каждый steer.
3. Развести diagnostics: неактуальная цель, неполное наблюдение, неподдержанная capability и uncertain native effect. Не превращать неизвестный ответ в next-turn input.
4. Сохранить внешний descriptor gate и отдельность Python/Rust artifacts. Goal continuation wiring и удаление старого bridge не входят в эту задачу; они не доказаны текущей карточкой как тот же дефект.

## Критерии готовности

- [ ] При 21+ завершённых ходах и корректном активном expected_turn_id запрос достигает native exact-steer.
- [ ] Смена активного turn между read и write отвергается native guard; новый turn не создаётся.
- [ ] Для завершённой цели/неполного необходимого доказательства нет ложного Applied.
- [ ] Потеря native reply сохраняет unknown и не порождает второй input.

## Границы и интеграция

Не добавлять новые Codex API по памяти: использовать схему поддерживаемого app-server из модуля; новые внешние формы проверять по первичной документации. Не менять модель, sandbox, approvals и goal owner под видом исправления steer.

Самостоятельный adapter-local PR; не зависит от Muse или OpenCode. #26 уже содержит локальные compiler fixes этого пакета — не копировать их сюда.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-adapter-codex --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
