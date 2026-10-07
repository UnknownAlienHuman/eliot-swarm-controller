# R11. События: один host-failure fact и точные runtime aliases

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-035, AUD-036.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Реальный отказ host с дополнительной диагностикой остаётся routable; legacy runtime outcomes доходят до правильного codec без обхода descriptor/provenance проверки.

## Читать адресно

- [docs/agent-operations/observability.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/observability.md) — durable facts, диагностика, безопасные проекции.
- [docs/agent-operations/architecture.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/architecture.md) — event intake и typed action consumers.
- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §2.1 и §3: atomic event/cursor, module metadata contract.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/{host_lifecycle.rs,automation_intake.rs,automation_dispatch.rs}`: retain_exit, host_terminal_exit_projection, safe_event_projection, script_event_projections_with_alias. В `store/mod.rs` только producer insert_safe_host_terminal_failure_event. Существующие ModuleEventMetadata/RuntimeOutcome contracts читать, не заменять.

## Что и как сделать

1. Создавать host.exit/host.failed из одного safe terminal fact/общей сериализации, с exact epoch и occurrence identity. Подробный secondary_codes остаётся в diagnostic exit receipt; не подмешивать произвольные diagnostics в ScriptRun input.
2. Согласовать reader с writer одновременно: allowlist и sibling equivalence должны проверять одну безопасную форму. Добавления одного поля в allowlist недостаточно, если paired payloads по-прежнему различны.
3. Роутить по source-family+kind. accepted runtime.outcome сохраняет уже работающую раннюю native_input_accepted проекцию. Legacy applied/rejected/unknown идут в их provenance-validated codec/alias path, не перехватываются общим module metadata return.
4. Произвольные module events допускаются только по descriptor-admitted metadata envelope. Не делать общий fallback при None; runtime.state не пропускать без его собственного валидатора.
5. Согласовать semantic occurrence dedup обеих проекций с существующим cursor admission; хранение нескольких views одного факта не создаёт несколько model/script actions.

## Критерии готовности

- [ ] Host failure с непустым secondary_codes сохраняет диагностический receipt и одну логическую trigger occurrence.
- [ ] Чужой epoch, source или несовпавшая identity отвергаются.
- [ ] Accepted работает прежним direct путём; legacy Applied/Rejected/Unknown видны только с валидным provenance.
- [ ] Ложный/незаявленный metadata envelope не получает доступа через fallback; двойные views не удваивают действие.

## Границы и интеграция

Не вся event-система сломана: AUD-036 касается конкретного legacy alias пути. Не менять бизнес-правила автоматизации и не добавлять новый event store. Тип размещать в минимальном общем codec; vendor данные в contracts не переносить.

Независим от R10/R12. В store/mod.rs править только named event producer, не общий mutation dispatcher. R07 может добавить своё event family отдельно через существующий registry.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
