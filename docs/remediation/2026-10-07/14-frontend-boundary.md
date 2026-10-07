# R14. Frontend: убрать обратную зависимость kernel-host → MCP

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-034.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Kernel-host не зависит от production swarm-mcp; method policy и общие wire schemas имеют одного data-only владельца, а CLI/MCP остаются самостоятельными frontends.

## Читать адресно

- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §2, §3, §4: dependency direction и общий method policy.
- [docs/agent-operations/shared-packages.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/shared-packages.md) — data-only shared packages и границы extraction.
- [docs/agent_swarm.module-contract-v2.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent_swarm.module-contract-v2.md) — §1–3: generic contracts отдельно от native/transport.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/{Cargo.toml,src/mcp.rs}` и его реальные production callers; `crates/swarm-contracts/src/method_policy.rs::METHOD_REGISTRY`; общие schema/profile producers в `crates/swarm-mcp/src/mcp/`; точные CLI callers. Не переносить Store/adapter домены и не переписывать subscriptions.

## Что и как сделать

1. Построить точный граф production использования host MCP facade: method classification, participant tool contracts, launch profile surface, legacy entrypoints. Разделить данные протокола, transport и test harness.
2. Перевести host classification на существующий METHOD_REGISTRY; общие request/schema/role-surface descriptors разместить в минимальном data-only contract owner и подключить обе стороны в том же diff. Не импортировать весь MCP frontend в contracts.
3. Устранить реальный caller legacy host MCP entrypoint через действующий самостоятельный frontend/CLI путь. Удалить compatibility facade после перевода caller, не оставить мёртвый shim/dead_code. Нужные интеграционные зависимости могут остаться dev-only.
4. Для реально переносимых DTO использовать единый Serde contract и, если оправдано, schema generation. Schemars, даже имеясь транзитивно в lockfile, требует явной direct dependency для импорта; новый dependency не добавлять молча. Не генерировать бизнес-authority из JSON Schema.
5. Удалить ставшие ненужными hand-written duplicates, сохранить public method names/role restrictions и authority перед эффектом. Новые методы R07/R09 и cursor DTO R08 должны быть представлены тем же registry, не вторым каталогом.

## Критерии готовности

- [ ] В production dependency graph kernel-host больше нет swarm-mcp, включая транзитивный возврат через helper.
- [ ] Host и MCP согласны по classification и shared shapes; Store остаётся окончательной границей authority.
- [ ] CLI/MCP вызывают реальные самостоятельные entrypoints; удалённый facade не оставляет потерянного caller.
- [ ] Изменение внутренней реализации MCP не требует компиляции host через обратную зависимость; проверка относится к graph, не обещанной скорости сборки.

## Границы и интеграция

Не новый универсальный Method DSL, не crate на каждый DTO и не массовая замена Value. Извлечь один завершённый production seam, не переносить прежний монолит под другое имя. Пример Schemars не разрешает изменение frozen schema digest без версии.

Выполнять последним из frontend-затрагивающих блоков: после интеграции R07/R08/R09. Это порядок предотвращения конфликтов, не причина блокировать backend/adapter PR. Общие файлы переносит только владелец R14.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-contracts -p swarm-mcp -p swarm-cli --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
