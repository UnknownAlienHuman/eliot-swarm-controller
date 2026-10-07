# R14. Frontend: убрать обратную зависимость kernel-host → MCP

**Статус: задание на реализацию. Уточнено по исходникам ELIOT и RMCP 07.10.2026; код этого блока пока не менялся.**
Основа: AUD-034 единого аудита, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Перед работой сравнить актуальный main с этим SHA. Уже исправленное не переписывать; аудит не является новой owner policy.

## Результат

Kernel-host не зависит от production swarm-mcp. Method policy и общие wire schemas имеют одного data-only владельца, CLI/MCP остаются самостоятельными frontends. Поиск одного инструмента не пересоздаёт неизменяемые схемы всего каталога; актуальная авторизация остаётся отдельной проверкой каждого запроса.

## Читать адресно

- [Modularity](../../agent-operations/modularity.md), §2–4: dependency direction и общий method policy.
- [Shared packages](../../agent-operations/shared-packages.md): data-only shared packages и границы extraction.
- [Module contract](../../agent_swarm.module-contract-v2.md), §1–3: generic contracts отдельно от native/transport.
- [MCP catalog](../../mcp-tool-catalog-and-loading.md): deferred discovery, видимая поверхность и права исполнения — разные факты.
- [Owner decisions](../../owner-decisions.md), §1.2–1.4 и §2.2: worktree, минимальный gate, live ownership и retention.

## Участок кода

`crates/swarm-kernel-host/{Cargo.toml,src/mcp.rs}` и реальные production callers; `crates/swarm-contracts/src/method_policy.rs::METHOD_REGISTRY`; общие schema/profile producers в `crates/swarm-mcp/src/mcp/`; точные CLI callers. В `catalog.rs` — `search_catalog`, `surface_view`, `digest_entries`, `digest_surface`, `digest_tool_schema`. В `mcp/mod.rs` — `input_schema`, `output_schema`, `refine_input_schema`. Не переносить Store/adapter домены и не переписывать subscriptions.

## Что и как сделать

1. Построить точный граф production использования host MCP facade: method classification, participant tool contracts, launch profile surface, legacy entrypoints. Разделить protocol data, transport и test harness.
2. Перевести host classification на существующий METHOD_REGISTRY; общие request/schema/role-surface descriptors разместить у минимального data-only владельца. Подключить обе стороны в том же diff. Не импортировать MCP frontend в contracts.
3. Переключить реального caller legacy host MCP entrypoint на действующий самостоятельный frontend/CLI. Удалить compatibility facade после переключения; не оставить мёртвый shim/dead_code. Нужный integration harness может остаться dev-only.
4. Для переносимых DTO использовать единый Serde contract и, если оправдано, генерацию схем. Schemars в транзитивном lockfile не даёт права импортировать его без direct dependency или явно поддержанного re-export. Согласовать dependencies; не выводить business authority из JSON Schema.
5. Сконструировать неизменяемые схемы и сериализованные bytes один раз для конечного набора registry methods и реальных wire-вариантов. Подробности и ограничения ниже. Удалить дубли hand-written форм после подключения действующих consumers.
6. Сохранить public method names, role restrictions, scope и authority перед эффектом. Additions R07/R09 и cursor DTO R08 включать в тот же registry, не создавать новый каталог.

## Проверенный дорогой путь и точный предел оптимизации

На `40591a2` `search_catalog` вызывает `surface_view` до exact-method фильтра и `max_results`. `digest_entries` собирает и сериализует schemas всех разрешённых методов; `digest_surface` повторяет это для видимых. Лимит ответа не ограничивает эту подготовительную работу. Это source-level вывод, не измерение latency/throughput.

`digest_tool_schema` вызывает `input_schema(spec, read_only, profile != Full)`; `read_only` фиксирован методом, а `output_schema` зависит только от метода. Для текущего статического registry достаточно **не более двух input-вариантов на метод** (Full/restricted) и одного output-варианта. Идентичные варианты можно разделять. Ключ не должен включать client ID, Task ID, search query или auth revision: это создаст растущий кэш того, что не является schema data.

Хранить immutable Arc/schema и точные сериализованные bytes. На каждый запрос по-прежнему вычислять текущие allowed entries, object authorization и surface из актуальной authority. Не переставлять LIMIT перед authorizer и не возвращать metadata запрещённых методов. Если registry станет динамическим, lifetime/cache invalidation должны быть связаны с его реальной ревизией; process-global eternal cache тогда не подходит.

Сохранить прежний digest preimage: порядок metadata/entries, имена методов, description, framing `update_field`, input/output bytes и пустой output marker. Замена schema bytes хэшем схемы **меняет v1 digest** — это отдельная версия и явный cursor/catalog resync, не скрытая оптимизация.

Кэш байтов убирает повторную генерацию и сериализацию, но при сохранении v1 digest запрос всё ещё хэширует схемы выбранного каталога. Не заявлять O(1), bounded total work или проценты ускорения без измерения исправленного пути.

## Доноры: применить механизм, не принести новый frontend

- [RMCP schema utilities, `08e021153ef0530aeb0bb406ebb360a38cfb8ee4`](https://github.com/modelcontextprotocol/rust-sdk/blob/08e021153ef0530aeb0bb406ebb360a38cfb8ee4/crates/rmcp/src/handler/server/common.rs): `schema_for_type`/`schema_for_input` возвращают Arc и кэшируют по TypeId **локально для потока**, не глобально для процесса. Полезен принцип immutable schema reuse. Это не готовый кэш ELIOT bytes и не причина добавлять rmcp в kernel-host.
- `schema_for_input` у этого донора также удаляет top-level title/description и использует JSON Schema 2020-12. Слепая замена нынешней factory на него может изменить contract/digest. Проверить pinned dependency и сравнить реальные bytes, не переносить результат старой квалификации.
- [MCPProxy describe/check, `fa4a8a4cc09da1927eff7829d8919e437e129b9d`](https://github.com/smart-mcp-proxy/mcpproxy-go/blob/fa4a8a4cc09da1927eff7829d8919e437e129b9d/internal/server/mcp_describe_check.go): полезны явные reason/retryable/action и отказ от неподдержанного expect_hashes. Не переносить Go proxy, Bleve или второй Store. Preflight ready не авторизует поздний вызов; ELIOT сохраняет `harness_acknowledgement:unknown` и честное требование reconnect поверхности.

SHA доноров фиксируют прочитанный source, не утверждают установленную версию и не требуют upgrade/downgrade зависимостей.

## Критерии готовности

- [ ] В production dependency graph kernel-host больше нет swarm-mcp, включая транзитивный возврат через helper; реальные callers переключены.
- [ ] Host и MCP согласны по classification/shared shapes; Store остаётся окончательной границей authority.
- [ ] Повторный exact lookup с `max_results=1` не запускает schema factory/serialization заново для уже подготовленных вариантов; кэш ограничен registry, не числом клиентов/Task.
- [ ] Для неизменённых методов совпадают input/output bytes и catalog/surface v1 digests; required client_request_id и различие Full/restricted сохранены.
- [ ] После отзыва прав/смены Task свежая проверка не раскрывает прежние инструменты, даже когда schema уже закэширована.
- [ ] Измерения различают schema generation, serialization, hashing и authorization. Изменение графа сборки не выдаётся за измеренное ускорение.

## Границы, порядок и сдача

Не новый Method DSL, не crate на каждый DTO, не массовая замена Value и не перенос монолита под другое имя. Выполнять после R07/R08/R09, чтобы не переносить одновременно меняющиеся frontend формы; остальные adapter/backend PR этого блока не ждут.

Один manager/worktree, writers без Cargo. Реализацию вести в этом же PR; после итогового кода — scoped formatting и минимальный gate:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-contracts -p swarm-mcp -p swarm-cli --lib --bins -- -D warnings
```

Полные tests/native/load — итоговая фаза, автоматически сейчас не запускать. В сдаче указать exact SHA, switched callers, удалённые дубли, фактический gate и ограничения. Эта правка изменяет только задание; schema cache, dependency extraction и их проверки ещё не реализованы.
