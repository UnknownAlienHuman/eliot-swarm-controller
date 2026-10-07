# ELIOT — серия PR-заданий от 7 октября 2026 года

**Созданы 14 draft PR: #27–#40. Это задания на реализацию, не заявления об исправленном коде.**
Каждый PR содержит самостоятельную спецификацию в `docs/remediation/2026-10-07/`; код нужно добавлять в тот же PR. Этот README — навигация, не отдельный этап и не блокер.

Исходный main: `40591a295af94b1541ec2ba30afe8e3247701a71`. Все 14 веток созданы непосредственно от него и направлены в main; цепочек PR-на-PR нет. Перед реализацией сверить актуальную базу и уже внесённые изменения.

Основа — единый аудит редакции 3. Сопоставлены 40 его самостоятельных AUD-карточек; это не означает 40 доказанных уязвимостей. Условные риски, source gaps и продуктовые расширения отмечены в заданиях. Исходный «Реестр подозрений» не стал обязательным списком исправлений.

## 1. Задания и результат

| Блок | PR | Законченный результат | AUD-карточки |
|---|---|---|---|
| R01 | [#27](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/27) | Identity → helper restart → same-boot hello; установленная версия независима от build-cache. | 005, 037, 038, 041 |
| R02 | [#28](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/28) | Журнал/outbox → IPC reconnect → result paging → stop с сохранённым owner. | 011, 027, 028, 033 |
| R03 | [#29](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/29) | Exact expected-turn steer независимо от длины истории. | 012 |
| R04 | [#30](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/30) | Вопросы и approvals не теряются и не воскресают при refresh. | 025 |
| R05 | [#31](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/31) | Кандидат exact Attempt; outer/inner dispatch identity согласованы. | 004, 023 |
| R06 | [#32](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/32) | Единый work context; code-scope admission; явная разрешённая связь двух заданий. | 001, 002, 008, 013, 015, 021 |
| R07 | [#33](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/33) | Proposal digest → manager ratification → terminal thread/Concilium. | 003, 018, 019, 020 |
| R08 | [#34](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/34) | Durable sequence → inbox cursor → watch → subscription cutoff и resync. | 006, 007, 009, 010, 014 |
| R09 | [#35](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/35) | Замена exact reviewer, исторические late results, bounded review.list. | 026, 029 |
| R10 | [#36](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/36) | Точная выборка automation ID; диагностика не откатывает disable. | 039, 040 |
| R11 | [#37](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/37) | Host failure и runtime aliases проходят соответствующие проверенные codecs. | 035, 036 |
| R12 | [#38](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/38) | Независимые due sources, no-progress backoff, справедливая issuance очередь. | 030, 032 |
| R13 | [#39](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/39) | Capacity evidence и exact lease release; корректная область collision report. | 016, 017, 022, 031 |
| R14 | [#40](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/40) | Удаление production зависимости kernel-host → MCP без потерянных callers. | 034 |

## 2. Порядок реализации без искусственной общей блокировки

Существующий [PR #26](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/26) отвечает за compiler baseline. Не копировать его исправления в 14 веток и не считать его body доказательством нового успешного Clippy. Для затронутого пакета взять актуальный исправленный main либо явно фиксировать существующий compiler blocker; незатронутые пакеты не ждут весь backend.

**Независимые работы:** R01–R06, R08–R12. Это возможность параллельной подготовки на непересекающихся символах, а не требование запустить одиннадцать менеджеров. По приоритету сначала R01, R05, R06, R10; adapter-local R02–R04 могут идти рядом.

**Действительные связи:** R06 → R07 и R06 → R13. Эти consumers используют итоговый work context; второй context/fingerprint не создавать. R08/R09 готовятся на своих функциях, при интеграции принимают общий тип R06.

**Порядок предотвращения конфликтов, не runtime-зависимость:** R07/R08/R09 → R14. Перенос frontend-границы выполняется после стабилизации их новых методов/схем. Остальные блоки ждать R14 не должны.

R10, R11 и R12 не зависят друг от друга: конфигурация, event projection и scheduler pacing имеют разных владельцев. Нумерация R01–R14 не означает обязательное последовательное выполнение всех четырнадцати.

## 3. Владение общими участками

| Общий участок | Единственный владелец конкретного изменения |
|---|---|
| `store/coordination.rs` | R06: context/registration/fingerprint/participant listing/normalize_send. R07: proposal/decision. R08: inbox и delivery index. R09: только review-specific pending/bind guards. |
| `store/code_scopes.rs` | R06: propose/accept identity и expiry-before-override. R13: active/conflict readers, collision domain. R07 использует current-scope projection, не переписывает её. |
| `store/submissions.rs` | R05: candidate provenance. R09: только review replacement/disposition seam. |
| `swarm-contracts/src/runtime.rs` | R05: согласованность dispatch receipt. Adapter PR не создаёт вторую версию этого валидатора. |
| `store/mod.rs` | Только необходимые named dispatch/codec hooks. R11: host terminal event producer. Глобальное форматирование/перестановки запрещены в параллельных ветках. |
| `swarm-supervisor` / OpenCode owner | R01: общий supervisor/helper. R02: Child и native-owner состояние внутри OpenCode adapter. |
| MCP/CLI и METHOD_REGISTRY | R07/R08/R09: необходимые additions без переноса файлов. R14: последующий перенос общего data-only seam и удаление фасада. |

Если двум заданиям действительно нужна одна функция, изменения этой функции делает один владелец; второй использует согласованный результат и перебазирует ветку. Совпадение имени файла само по себе не повод объявить весь PR заблокированным. Несовместимые изменения интерфейса нельзя маскировать shim или резервной копией.

## 4. Как исполнять и сдавать

Один manager — один worktree. Manager может делегировать непересекающиеся внутренние участки; writer не запускает Cargo. Задание включает producer, handler, persisted fact и reader — нельзя сдать отдельно новый DTO без подключённых потребителей.

В каждом файле указаны документация с адресным разделом/темой, символы, 4–5 шагов, критерии поведения, ограничения и минимальная команда gate. Читать сначала эти источники, не весь архив аудита. Документация с закреплённым SHA фиксирует доказательство, не требует downgrade native runtime.

Сначала код, затем scoped formatting и минимальный warnings-denied Clippy на итоговом кандидате; для чисто JS-изменения Muse — проверка синтаксиса. Полные тесты, native/live и нагрузочные прогоны относятся к итоговой фазе и сейчас автоматически не запускаются. Будущие критерии поведения не отмечать выполненными без фактического доказательства.

Сдача в том же PR: exact candidate SHA, какие producer/consumer теперь связаны, что удалено как дублирование, реальный вывод gate, оставшаяся неопределённость. Пока в diff только Markdown-задание, сохранять Draft и не сливать его как исправление. Ни один из этих PR не включает автоматическое закрытие старых Issues, изменение labels или merge.

## 5. Что намеренно не включено

Не переносить в реализацию опровергнутые обвинения: обычный non-force push не затирает расходящуюся ветку; review assignment уже имеет role/sponsor guard; accepted runtime outcome имеет рабочий direct codec; конечная issuance очередь не доказана как навсегда потерянная.

Не вводить новый workflow engine, broker, IAM, второй Store, массовый `Value`-рефакторинг или автоматические kill/rotation по молчанию. Не удалять authoritative evidence по LRU. Рекомендации расширения recovery/межзадачного сотрудничества обозначены как расширения, а не как якобы уже существующая политика.

Доноры — конкретные механизмы: собственный Command journal для публикации файлов; имеющийся Tokio watch для атомарного status update; CCCC для locality чтения; Paseo для ownership подписки. Перенос их целых runtime/store не требуется.

## 6. Источники и статус публикации

[Owner decisions](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md), [модульная архитектура](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) и узкие ссылки внутри заданий определяют границы. Frozen policy text не переписывается ради обхода проверки.

На момент создания серии: опубликованы только спецификации; исправления продукта, Rust-сборка, Clippy, тесты и native-квалификация в этой работе не выполнялись. Main и существующие PR #24/#26 не изменялись. Каждый PR и его ветка возвращены GitHub API; список #27–#40 повторно прочитан после создания.
