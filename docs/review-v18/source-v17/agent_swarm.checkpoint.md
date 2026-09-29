# ELIOT Swarm — checkpoint

**29.09.2026. Завершён проход документации v14. Код Rust-сервиса не написан.**

## Действующие файлы

| Файл | Состояние |
|---|---|
| `agent_swarm.md` | Архитектура v14; исправлены неоднозначности v13 |
| `agent_swarm.implementation-v2.md` | Действующий план C01–C11, API/Store/IPC и границы проверки |
| `agent_swarm.spec-v14/` | НОВЫЕ reference SQL, TOML и JSON; не восстановление старого ZIP |
| `agent_swarm.documentation-review-v14-20260929.md` | Двенадцать основных изменений, основания и граница проверки |
| `agent_swarm.donors-20260929.toml` | Metadata синхронизирована с v14; source pins не менялись |
| `review-v14/source/` | Нетронутые входные v13, plan-v1, donor inventory и checkpoint |
| `agent_swarm.before-v14-20260929.md` | Побайтовая копия исходной v13 |

Старый `agent_swarm.implementation-v1.zip` по-прежнему не восстановлен. Не объявлять новые SQL/config его исходными файлами. Прежний recovery ZIP сохранён отдельно; новая поставка не затирает его.

## Что закончено

Прочитаны v13 и implementation-v1 целиком. Уточнены: claim/dispatch/open, task-specific release, CAS begin_send, отдельные native setup шаги, IPC boot/link/native identities, stale snapshot и coverage, non-destructive mailbox cursors, artifact/backup/migration recovery.

Сохранены выбранный стек, девять таблиц, native Muse Max/OpenCode V2, простые модули и политика main-only. Новых сервисов/broker/PKI/слоёв orchestration нет.

В `agent_swarm.spec-v14/validation-results.json`: 42/42 структурных проверок. SQLite 3.46.1, одна in-memory connection; не production SQLite selection, не WAL/Windows/runtime qualification.

## Что НЕ сделано

Нет `src/*.rs` нового приложения, собранного `swarm.exe`, Cargo.lock, установленных в этом проходе SDK, Windows-проб или модельных вызовов. Нет benchmark сотен агентов. Рабочая машина/рой и GitHub не изменены.

## Следующее незавершённое действие

**C01:** написать `model.rs`, `config.rs`, `store.rs` и подключить новую reference migration. Реальные методы: create → claim/dispatch → durable Operation → повтор того же ID без дубля; invalid payload/owner/revision не создают частичную запись.

**C02:** host singleton, Named Pipe API и CLI, короткий durable ack/status; независимые reader/writer. Затем C03 Muse и C04 OpenCode V2, а не очередной выбор общей платформы.

Во время реализации — main, без worktrees; рабочий code path, форматирование, минимальный Clippy. Объёмные тесты и нагрузка после готового среза. Существующие donor fixtures не удалять. Reference DDL не заменяет Store и не означает «C01 готов».

## Если чат снова прервался

Проверить существующие файлы и `agent_swarm.doc-status-v14.json`, а не только последний ответ. Контрольные суммы — `agent_swarm.docs-v14-SHA256SUMS.txt`. После каждого законченного изменения сохранять файл и checkpoint. Не пересоздавать отсутствующие артефакты молча и не повторять платные пробы для проверки документации.
