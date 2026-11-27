# Индекс и навигация

> Документация Swarm собрана из целевой архитектуры v2.6 (27.11.2026). Этот файл генерируется скриптом [tools/build_index.py](tools/build_index.py); правь скрипт, а не файл.

## Как пользоваться

1. Всегда читай [ARCHITECTURE.md](ARCHITECTURE.md): суть, роли, правила, модель. Это минимум контекста.
2. Возьми рецепт для своего типа задачи ниже: 1–3 файла блока. Каждый файл начинается со шапки (зачем, владеет, вход, выход, инварианты, зависит от, статус) и дальше содержит дословный текст v2.6.
3. Ссылка «раздел N» в тексте — номер раздела v2.6; стрелка ↗ рядом ведёт туда, где он теперь. Раздел, разнесённый по нескольким файлам, — в карте разделов ниже.
4. Добавляешь зависимость, таблицу или модуль — [GRAPH.md](GRAPH.md). Опираешься на механизм — метка в [STATUS.md](STATUS.md). Старое слово из кода или разговора — словарь в [core.md](implementation/core.md#словарь-старое-слово--имя-в-спецификации).
5. Следующая задача — первый неотмеченный пункт [backlog.md](backlog.md); там же контекст этапа и открытые вопросы.
6. [archive/](archive/HISTORY.md) — история версий, доноры, инвентаризация старой документации, полный текст спецификации v2.6 ([archive/spec-v2.6.md](archive/spec-v2.6.md)). Исполнителю не нужен.
7. Остальное в `docs/` вне `docs/swarm/` (vision, architecture, roadmap, decisions, spec, harnesses, reference) описывает **текущий** код до миграции. Где оно расходится с этими документами, правы эти документы; отменённые решения отмечены в `docs/decisions.md`. Контракт desktop — `docs/design/fluent-design-system.md`, он действует.

## Рецепт контекста задачи

Всегда плюс ARCHITECTURE.md (10 КБ). Размер — сумма с ним; файлы с «+» и «<X>» не считаются.

| Задача | Файлы | КБ |
| --- | --- | --- |
| Store: таблица, схема, admission, транзакция | [implementation/store.md](implementation/store.md), [implementation/core.md](implementation/core.md), [GRAPH.md](GRAPH.md) | 75 |
| Переход задачи, детектор, разбор, бюджет, gate | [implementation/task-controller.md](implementation/task-controller.md), [implementation/automata.md](implementation/automata.md), [implementation/config.md](implementation/config.md) | 90 |
| Супервизия, захват, поколения, сверка | [implementation/daemon.md](implementation/daemon.md), [implementation/store.md](implementation/store.md) | 53 |
| Runtime агента, доставка сообщений | [implementation/runtime.md](implementation/runtime.md), [implementation/core.md](implementation/core.md) | 58 |
| Интегратор: merge, проверки, push, чистка | [implementation/integrator.md](implementation/integrator.md), [implementation/plugins/checks.md](implementation/plugins/checks.md), [implementation/plugins/github.md](implementation/plugins/github.md) | 49 |
| Хуки и доставка в ход | [implementation/hooks.md](implementation/hooks.md), [implementation/runtime.md](implementation/runtime.md), + плагин харнесса | 46 |
| Плагин харнесса X (новый или правка) | [implementation/plugins/README.md](implementation/plugins/README.md), implementation/plugins/<X>.md, [implementation/runtime.md](implementation/runtime.md) | 42 |
| Маршруты, квоты, ошибки провайдера, Hold | [implementation/routing.md](implementation/routing.md), + плагин харнесса | 19 |
| Облачный исполнитель | [implementation/plugins/cloud-agents.md](implementation/plugins/cloud-agents.md), [implementation/integrator.md](implementation/integrator.md), [implementation/plugins/github.md](implementation/plugins/github.md) | 62 |
| Удалённый MCP | [implementation/remote-mcp.md](implementation/remote-mcp.md), [implementation/cli-mcp.md](implementation/cli-mcp.md), [implementation/security.md](implementation/security.md) | 48 |
| Команда CLI или MCP, реестр | [implementation/cli-mcp.md](implementation/cli-mcp.md), [GRAPH.md](GRAPH.md) | 38 |
| Секреты и права агентов | [implementation/security.md](implementation/security.md), [implementation/integrator.md](implementation/integrator.md) | 52 |
| Конфигурация конвейера (workflow, routes, triggers) | [implementation/config.md](implementation/config.md), [implementation/task-controller.md](implementation/task-controller.md) | 55 |
| Эксплуатация: наблюдение, учёт, диск, обновление | [implementation/operations.md](implementation/operations.md), [implementation/daemon.md](implementation/daemon.md) | 36 |
| Этап миграции N | [implementation/migration.md](implementation/migration.md), [STATUS.md](STATUS.md), + файлы блоков этапа | 46 |
| Сквозной тест или поведение при сбое | [implementation/scenarios.md](implementation/scenarios.md), + файл блока | 24 |
| Решение по архитектуре, новый модуль или сущность | [GRAPH.md](GRAPH.md), [implementation/decisions.md](implementation/decisions.md), [STATUS.md](STATUS.md) | 63 |

## Файлы

| Файл | Что внутри | КБ |
| --- | --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Swarm: суть и правила | 10 |
| [GRAPH.md](GRAPH.md) | Граф: сущности, модули, кто что пишет | 15 |
| [STATUS.md](STATUS.md) | Что проверено и что гипотеза | 20 |
| [backlog.md](backlog.md) | Бэклог реализации | 17 |
| [implementation/automata.md](implementation/automata.md) | Контракты задачи и автоматы | 35 |
| [implementation/cli-mcp.md](implementation/cli-mcp.md) | CLI-маршрутизатор, реестр, малый MCP, eliot-client | 13 |
| [implementation/config.md](implementation/config.md) | Конфигурация конвейера: workflow.toml, routes.toml, triggers.toml | 8 |
| [implementation/core.md](implementation/core.md) | Ядро: модель и сообщения (eliot-core) | 26 |
| [implementation/daemon.md](implementation/daemon.md) | Демон: супервизия, захват, сверка (eliot-daemon) | 19 |
| [implementation/decisions.md](implementation/decisions.md) | Правила и решения полностью | 18 |
| [implementation/hooks.md](implementation/hooks.md) | Hooks и injects: доставка, даже если агент не хочет | 14 |
| [implementation/integrator.md](implementation/integrator.md) | Интегратор: очередь merge, squash, push, чистка | 24 |
| [implementation/migration.md](implementation/migration.md) | Путь из текущего кода: этапы и удаления | 16 |
| [implementation/operations.md](implementation/operations.md) | Эксплуатация: наблюдение, учёт, диск, обновление | 6 |
| [implementation/plugins/README.md](implementation/plugins/README.md) | Плагины: общий контракт | 10 |
| [implementation/plugins/acp.md](implementation/plugins/acp.md) | Общий ACP-плагин (Gemini CLI, Goose и другие, первая волна) | 5 |
| [implementation/plugins/antigravity.md](implementation/plugins/antigravity.md) | Плагин Antigravity CLI (вторая волна) | 3 |
| [implementation/plugins/checks.md](implementation/plugins/checks.md) | Проверки и линтеры (cargo, clippy, тесты, любой CLI) | 6 |
| [implementation/plugins/claude-code.md](implementation/plugins/claude-code.md) | Плагин Claude Code (нативный, первая волна) | 15 |
| [implementation/plugins/cloud-agents.md](implementation/plugins/cloud-agents.md) | Облачные исполнители (cloud agent, вторая волна) | 19 |
| [implementation/plugins/codex.md](implementation/plugins/codex.md) | Плагин Codex (нативный, app-server, первая волна) | 8 |
| [implementation/plugins/github.md](implementation/plugins/github.md) | Forge-плагин GitHub | 7 |
| [implementation/plugins/muse-code.md](implementation/plugins/muse-code.md) | Плагин Muse Code (локальный харнесс, вторая волна) | 5 |
| [implementation/plugins/notify.md](implementation/plugins/notify.md) | Плагин notify (уведомления владельцу) | 2 |
| [implementation/plugins/opencode.md](implementation/plugins/opencode.md) | Плагин OpenCode (нативный, первая волна) | 5 |
| [implementation/remote-mcp.md](implementation/remote-mcp.md) | Удалённый MCP: внешний агент вызывает Swarm | 7 |
| [implementation/routing.md](implementation/routing.md) | Маршруты, провайдеры, квоты и Hold | 9 |
| [implementation/runtime.md](implementation/runtime.md) | Runtime агента и eliot-plugin | 22 |
| [implementation/scenarios.md](implementation/scenarios.md) | Сквозные сценарии отказов | 13 |
| [implementation/security.md](implementation/security.md) | Секреты, права агентов и полномочия | 17 |
| [implementation/store.md](implementation/store.md) | Store: единственная правда (eliot-store) | 23 |
| [implementation/task-controller.md](implementation/task-controller.md) | Task controller: цикл задачи до готовности | 37 |
| [tools/README.md](tools/README.md) | Скрипты проверок | 3 |
| [tools/build_backlog.py](tools/build_backlog.py) | генерация backlog.md из таблицы этапов | 7 |
| [tools/build_index.py](tools/build_index.py) | генерация этого файла | 10 |
| [tools/check.py](tools/check.py) | проверка автоматов по таблицам | 5 |
| [tools/coverage.py](tools/coverage.py) | проверка, что каждая строка v2.6 есть в документах | 3 |
| [tools/deps.py](tools/deps.py) | запрещённые рёбра крейтов (храповик) | 2 |
| [tools/ratchet.py](tools/ratchet.py) | храповик для CI: deps, scc, check с порогами | 2 |
| [tools/ratchet.toml](tools/ratchet.toml) | пороги храповика (текущие числа main) | 1 |
| [tools/scc.py](tools/scc.py) | самый большой цикл модулей (храповик) | 3 |
| [tools/split_v26.py](tools/split_v26.py) | сборка этой документации из v2.6 | 77 |
| [archive/DONORS.md](archive/DONORS.md) | Архив: доноры, проверенные по коду | 22 |
| [archive/HISTORY.md](archive/HISTORY.md) | Архив: история версий архитектуры | 47 |
| [archive/INVENTORY.md](archive/INVENTORY.md) | Архив: инвентаризация старых возможностей | 9 |
| [archive/spec-v2.6.md](archive/spec-v2.6.md) | Eliot Swarm Controller: целевая архитектура v2.6 | 355 |

## Карта разделов

Куда попал каждый раздел v2.6 (по числу перенесённых строк). Каждая непустая строка v2.6 есть в документах дословно — это проверяет [tools/coverage.py](tools/coverage.py).

| Раздел v2.6 | Где теперь |
| --- | --- |
| 1. Суть: конвейер, а не память | [STATUS.md](STATUS.md), [implementation/decisions.md](implementation/decisions.md) (по нескольку строк: ARCHITECTURE, checks, README, notify, security) |
| 1а. Единая модель: шестнадцать сущностей | [implementation/core.md](implementation/core.md) (по нескольку строк: routing, remote-mcp) |
| 2. Ядро: передача сообщений | [implementation/core.md](implementation/core.md), [implementation/store.md](implementation/store.md) |
| 3. Супервизия и восстановление | [implementation/daemon.md](implementation/daemon.md), [implementation/runtime.md](implementation/runtime.md) (по нескольку строк: notify, acp) |
| 4. Хранение: что где лежит | [implementation/store.md](implementation/store.md), [implementation/security.md](implementation/security.md), [implementation/config.md](implementation/config.md), [implementation/task-controller.md](implementation/task-controller.md), [implementation/plugins/claude-code.md](implementation/plugins/claude-code.md) (по нескольку строк: github, codex, hooks, opencode, checks, cloud-agents) |
| 5. Цикл задачи до готовности | [implementation/task-controller.md](implementation/task-controller.md) (по нескольку строк: claude-code) |
| 6. Интегратор: ветки, squash, merge, чистка | [implementation/integrator.md](implementation/integrator.md), [implementation/plugins/github.md](implementation/plugins/github.md) (по нескольку строк: checks) |
| 7. Смена провайдера и харнесса | [implementation/routing.md](implementation/routing.md) (по нескольку строк: muse-code, antigravity, claude-code) |
| 8. CLI-маршрутизатор и малый MCP | [implementation/cli-mcp.md](implementation/cli-mcp.md) (по нескольку строк: daemon) |
| 9. Hooks и injects: доставка, даже если агент не хочет | [implementation/hooks.md](implementation/hooks.md), [implementation/plugins/claude-code.md](implementation/plugins/claude-code.md), [implementation/plugins/opencode.md](implementation/plugins/opencode.md), [implementation/plugins/codex.md](implementation/plugins/codex.md), [implementation/plugins/acp.md](implementation/plugins/acp.md) |
| 10. Плагины | [implementation/config.md](implementation/config.md), [implementation/plugins/README.md](implementation/plugins/README.md), [implementation/runtime.md](implementation/runtime.md), [implementation/plugins/checks.md](implementation/plugins/checks.md), [implementation/plugins/cloud-agents.md](implementation/plugins/cloud-agents.md) (по нескольку строк: codex, acp, muse-code, github, notify, antigravity, claude-code) |
| 10а. Облачные агенты и удалённый MCP | [implementation/plugins/cloud-agents.md](implementation/plugins/cloud-agents.md), [implementation/remote-mcp.md](implementation/remote-mcp.md), [implementation/plugins/muse-code.md](implementation/plugins/muse-code.md) (по нескольку строк: github) |
| 11. Ручной и автоматический режим | [implementation/task-controller.md](implementation/task-controller.md) (по нескольку строк: config) |
| 11а. Эксплуатация и возвращённые возможности | [implementation/cli-mcp.md](implementation/cli-mcp.md) |
| — Команды владельца | [implementation/cli-mcp.md](implementation/cli-mcp.md) |
| — Права агентов и харнессов | [implementation/security.md](implementation/security.md) (по нескольку строк: claude-code, codex) |
| — Рамки протокола | [implementation/runtime.md](implementation/runtime.md) |
| — Квоты, маршруты и аккаунты | [implementation/routing.md](implementation/routing.md) (по нескольку строк: claude-code, muse-code, codex, antigravity) |
| — Харнессы: версии, вход, Muse и Antigravity | [implementation/plugins/README.md](implementation/plugins/README.md) (по нескольку строк: muse-code, antigravity, acp) |
| — Дочерние задачи и пробуждение | [implementation/task-controller.md](implementation/task-controller.md) |
| — Триггеры: расписание и события | [implementation/operations.md](implementation/operations.md) (по нескольку строк: config) |
| — Наблюдение и атрибуция | [implementation/operations.md](implementation/operations.md) |
| — Проекты и полномочия | [implementation/security.md](implementation/security.md) (по нескольку строк: config) |
| — Учёт, хранение и диск | [implementation/operations.md](implementation/operations.md) (по нескольку строк: store) |
| — Рестарт, обновление, откат, упаковка | [implementation/operations.md](implementation/operations.md) (по нескольку строк: github) |
| 12. Путь из текущего кода и что удаляется | [implementation/migration.md](implementation/migration.md), [implementation/store.md](implementation/store.md), [implementation/daemon.md](implementation/daemon.md), [implementation/task-controller.md](implementation/task-controller.md), [implementation/security.md](implementation/security.md), [implementation/integrator.md](implementation/integrator.md), [implementation/plugins/claude-code.md](implementation/plugins/claude-code.md), [implementation/cli-mcp.md](implementation/cli-mcp.md) (по нескольку строк: cloud-agents, hooks, runtime, github, remote-mcp, backlog) |
| 13. Принятые допущения | [implementation/decisions.md](implementation/decisions.md) (по нескольку строк: cloud-agents) |
| 13а. Что проверено запуском и что остаётся гипотезой | [STATUS.md](STATUS.md), [implementation/plugins/claude-code.md](implementation/plugins/claude-code.md), [implementation/plugins/acp.md](implementation/plugins/acp.md) (по нескольку строк: cloud-agents, checks, opencode, remote-mcp, codex, antigravity) |
| 13б. Решения, которые меняют старые правила | [implementation/decisions.md](implementation/decisions.md), [implementation/plugins/cloud-agents.md](implementation/plugins/cloud-agents.md) |
| 14. Контракты | [implementation/automata.md](implementation/automata.md) |
| — Модули | [GRAPH.md](GRAPH.md), [implementation/runtime.md](implementation/runtime.md), [implementation/cli-mcp.md](implementation/cli-mcp.md) (по нескольку строк: daemon, integrator, core, store) |
| — Конверт | [implementation/core.md](implementation/core.md) (по нескольку строк: integrator, automata) |
| — task.md | [implementation/automata.md](implementation/automata.md) (по нескольку строк: integrator) |
| — Состояние задачи (строка Task в Store) | [implementation/automata.md](implementation/automata.md) (по нескольку строк: integrator) |
| — События Task | [implementation/automata.md](implementation/automata.md) |
| — Переходы Task | [implementation/automata.md](implementation/automata.md), [implementation/integrator.md](implementation/integrator.md) (по нескольку строк: cloud-agents, runtime, core, config) |
| — Жизненные циклы остальных сущностей | [implementation/automata.md](implementation/automata.md), [implementation/plugins/cloud-agents.md](implementation/plugins/cloud-agents.md), [implementation/runtime.md](implementation/runtime.md), [implementation/integrator.md](implementation/integrator.md), [implementation/core.md](implementation/core.md) |
| — Проверка автоматов | [implementation/automata.md](implementation/automata.md) |
| — Запись реестра | [implementation/cli-mcp.md](implementation/cli-mcp.md) |
| — Протокол runtime агента (NDJSON по stdio и сокет) | [implementation/runtime.md](implementation/runtime.md) (по нескольку строк: hooks) |
| — API Store для задач и сообщений | [implementation/store.md](implementation/store.md) |
| — Журнал интегратора | [implementation/integrator.md](implementation/integrator.md) (по нескольку строк: automata, core, github) |
| — Stop (причина остановки) | [implementation/automata.md](implementation/automata.md) (по нескольку строк: integrator, core) |
| 14а. Граф: сущности, модули и правила против распада | [GRAPH.md](GRAPH.md), [implementation/daemon.md](implementation/daemon.md) (по нескольку строк: remote-mcp, core) |
| 15. Сквозные сценарии: проверка v2.6 | [implementation/scenarios.md](implementation/scenarios.md) |
| Приложение А. Доноры: что проверено по коду и что берём | [archive/DONORS.md](archive/DONORS.md) |
| Приложение Б. История версий | [archive/HISTORY.md](archive/HISTORY.md) |
| Приложение В. Скрипты проверок | [tools/check.py](tools/check.py), [tools/scc.py](tools/scc.py), [tools/deps.py](tools/deps.py) (по нескольку строк: README) |
| Приложение Г. Инвентаризация: старые возможности и их судьба | [archive/INVENTORY.md](archive/INVENTORY.md) |
