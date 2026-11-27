# Бэклог реализации

> Сгенерировано скриптом [tools/build_backlog.py](tools/build_backlog.py) из таблицы этапов v2.6 (раздел 12, теперь [migration.md](implementation/migration.md)). Тексты пунктов, удалений и критериев — дословно из спецификации.

**Как брать задачу.** Этапы идут по порядку; внутри этапа пункт — одна задача или одна ветка с PR. Контекст задачи: [ARCHITECTURE.md](ARCHITECTURE.md) плюс файлы из строки «Контекст». Каждый срез обязан уменьшить хотя бы одно число храповика ([GRAPH.md](GRAPH.md), [tools/ratchet.toml](tools/ratchet.toml)) или не трогать его; что удаляется — удаляется в том же этапе. Открытый вопрос этапа решается в начале этапа, решение записывается в файл блока (implementation/), а не в отдельный отчёт.

**Отметка.** Сделанный пункт отмечается [x] в том же PR, где он сделан; этот файл — единственный список статуса.

## Этап 0. Правила, тесты, опасное

Контекст: [security.md](implementation/security.md), [claude-code.md](implementation/plugins/claude-code.md), [migration.md](implementation/migration.md), [GRAPH.md](GRAPH.md).

- [ ] удалить plugins/claude/src/quota.rs (читает учётные данные Claude) — первым, до остального
- [x] AGENTS.md под правила раздела 1
- [ ] узкие тесты: reducer переходов eliot-core и Store на временном SQLite
- [ ] desktop перестаёт запускать и держать демон
- [x] в CI — deps.py, scc.py и check.py из приложения В с порогами по текущим числам (храповик)
- [ ] desktop ходит в демон через новый крейт eliot-client

**Удаляется в этом этапе.** plugins/claude/src/quota.rs (читает учётные данные Claude) — первым; запрет тестов; отменённые D-12, D-14, D-18, D-21, D-28, D-34

**Критерий приёмки.** тесты и три скрипта проходят в CI; desktop не зависит от eliot-daemon; в коде нет чтения файлов авторизации харнессов; в AGENTS.md нет запрета тестов

## Этап 1. Узкий реестр

Контекст: [cli-mcp.md](implementation/cli-mcp.md), [core.md](implementation/core.md), [automata.md](implementation/automata.md), [GRAPH.md](GRAPH.md).

- [ ] трейт Checked в eliot-core
- [ ] таблица переходов Task генерируется из reducer и проверяется check.py
- [ ] у каждой таблицы Store назван модуль-владелец, тест «один писатель» с сокращающимся списком исключений
- [ ] clippy disallowed-methods на запуск процессов
- [ ] из METHODS генерируются матч диспетчера, список ролей (по principal) и MCP (search, run)
- [ ] коды ошибок в enum, строки на проводе те же

**Удаляется в этом этапе.** ручной список ролей в router/auth.rs, нынешний MCP-инструмент без схем

**Критерий приёмки.** новая операция добавляется одной записью и видна в RPC и MCP; CLI и desktop переходят на сгенерированное постепенно; CLI-грамматика, обработчики и UX из реестра не генерируются

## Этап 2. Task и Attempt для брифа папки

Контекст: [store.md](implementation/store.md), [automata.md](implementation/automata.md), [daemon.md](implementation/daemon.md).

- [ ] в существующем Store: Task с ревизией, исходным запросом, kind, бюджетом
- [ ] Attempt с поколением
- [ ] захват одной IMMEDIATE-транзакцией с admission (как launch\_project\_manager)
- [ ] task.edit пишет новую ревизию и экспорт в work-truth.git

**Удаляется в этом этапе.** — (старый путь ещё работает для остальных брифов)

**Критерий приёмки.** бриф папки создаёт Task; правка задачи даёт новую ревизию; захват двумя процессами — один исполнитель

## Этап 3. Task controller для этого сценария

Контекст: [task-controller.md](implementation/task-controller.md), [automata.md](implementation/automata.md), [config.md](implementation/config.md).

- [ ] контроллер с ключом по задаче на событиях Store (приём run\_goal\_driver)
- [ ] result kinds change, no\_change, report
- [ ] детектор no\_progress
- [ ] разбор
- [ ] blocked возвращает к прежнему этапу
- [ ] бюджет порциями и held(budget)

**Удаляется в этом этапе.** для мигрированных брифов: правила таймера и завершения (reload.rs:80, 97-98), assignment\_busy (schema.rs:1232-1318), привязка через Goal, переключение аудита (goal\_audit.rs:293)

**Критерий приёмки.** задача брифа доходит до кандидата без старых правил; ложное «готово» дважды уходит на разбор; исчерпанный бюджет ставит held(budget), а не продолжает

**Открытые вопросы спецификации (найдены прогоном задач на свежих агентах).**

- Какой из лимитов порции (attempts_left, wall_left_min, usd_left) вызывает budget_exhausted и как списывается расход при usd_left = unknown: инвариант запрещает считать неизвестное нулём, но правило списания не записано.
- Как продолжается работа после budget_extended из running: в переходах Attempt нет stopped → running; записать, что это новый Runtime того же Attempt через resume (как при lost) или новый Attempt.
- Куда возвращается held(budget), пришедший из integrating через pending: prev после исхода шага не определён.

## Этап 4. Кандидат и журнал интегратора

Контекст: [integrator.md](implementation/integrator.md), [automata.md](implementation/automata.md).

- [ ] кандидат (ревизия, gen, база, дерево и коммит) с правилом эквивалентности
- [ ] журнал шагов с remote, ref, ожидаемым старым и новым OID
- [ ] доказательство merge-base --is-ancestor
- [ ] очередь встаёт, если remote не читается

**Удаляется в этом этапе.** ветвь интеграции по трейлерам как доказательство

**Критерий приёмки.** 10 merge подряд без ручного шага; kill -9 интегратора на каждом шаге — 0 потерь и 0 дублей; правка критериев после ревью снимает допуск

**Открытые вопросы спецификации (найдены прогоном задач на свежих агентах).**

- origin_at_old: таблица IntegrationStep ведёт шаг в failed; записать, что дальше — новый шаг той же очереди или возврат Task в queued.
- Сеть без ответа после sending: классы исхода говорят «повтор с backoff», таблица — unknown; записать, что после sending всегда unknown и чтение origin, а backoff — только до sending.

## Этап 5. Публикация и проверки

Контекст: [integrator.md](implementation/integrator.md), [github.md](implementation/plugins/github.md), [checks.md](implementation/plugins/checks.md), [security.md](implementation/security.md).

- [ ] вход публикации — кандидат
- [ ] forge-push с токеном GitHub App, ключ App у отдельной учётной записи
- [ ] ruleset на main с обходом только для App
- [ ] пустой GIT\_CONFIG\_GLOBAL в раннере интегратора
- [ ] gitleaks на каждом новом коммите агента
- [ ] сеть check runner отрезана на Linux, способ для macOS и Windows выбран

**Удаляется в этом этапе.** публикация из грязного worktree, её GitPublishTrigger и авто-правило; выдача токенов forge агентам

**Критерий приёмки.** против настоящего GitHub: потеря ответа после push закрывается проверкой достижимости; канарейка с ключом не доходит ни до ревьюера, ни до remote; push в main с токеном пользователя отклонён ruleset

## Этап 6. Runtime агента и служба ОС

Контекст: [runtime.md](implementation/runtime.md), [daemon.md](implementation/daemon.md).

- [ ] отсоединённый runtime по контракту A
- [ ] KillMode=process в юните systemd
- [ ] дедлайны хода и остаток wall-бюджета в runtime
- [ ] владение Windows Job в runtime
- [ ] переподключение по PID и времени старта
- [ ] остановка поколения: отзыв, запрос, подтверждение, карантин
- [ ] второй харнесс

**Удаляется в этом этапе.** убийство групп при рестарте демона, пометка агентов lost до проверки, loss\_recovery в нынешнем виде

**Критерий приёмки.** kill -9 демона и systemctl restart при 3 живых агентах на Linux и Windows: все 3 подхвачены, ни один не заменён; при остановленном на час демоне зависший ход прерван runtime; смерть runtime гасит потомков; смена поколения не теряет незакоммиченное и не пишет WIP поверх живого процесса

## Этап 7. Владелец, Fluent, завершение миграции

Контекст: [task-controller.md](implementation/task-controller.md), [cli-mcp.md](implementation/cli-mcp.md).

- [ ] gate, swarm inbox, swarm approve, budget\_extend
- [ ] экраны Fluent на новых данных
- [ ] оставшиеся брифы и цели на Task

**Удаляется в этом этапе.** old owner inbox, mail notices, completion digests, старые таблицы holds, attention, incidents — когда у них не осталось вызывающих; экраны Material заморожены

**Критерий приёмки.** при всех gate = owner ни один необратимый переход не случается без approve; поиск по коду: у старых путей нет вызывающих

## Этап 8. Хуки

Контекст: [hooks.md](implementation/hooks.md), [runtime.md](implementation/runtime.md), [claude-code.md](implementation/plugins/claude-code.md), [codex.md](implementation/plugins/codex.md).

- [ ] swarm hook через runtime
- [ ] Stop hook со счётчиком в Store
- [ ] перевыдача задачи после компакции
- [ ] onFailure там, где харнесс его поддерживает

**Удаляется в этом этапе.** —

**Критерий приёмки.** агент с красными проверками возвращается к работе, на 4-й раз решает детектор; SessionStart(compact) отдаёт текущую ревизию task.md; хуки держат ×10 нагрузки (замер) или переведены на постоянное соединение

**Открытые вопросы спецификации (найдены прогоном задач на свежих агентах).**

- Определить «отпечаток» для детектора прогресса (дерево коммита, ветка, что исключается) и ключ исхода проверок.
- Что делает Stop hook при мёртвом демоне со счётчиком блокировок (runtime отвечает из журнала, но кто считает блокировки).

## Этап 9. Эволюция писателя Store

Контекст: [store.md](implementation/store.md).

- [ ] замер ожидания писателя в нынешнем Store
- [ ] по результату — групповая фиксация шире пачек инструментов, физическое разделение базы, cache.db

**Удаляется в этом этапе.** глобальные блокировки там, где замер показал очередь

**Критерий приёмки.** решение по замеру, а не по счёту store.lock; p99 подтверждения ≤ 50 мс на целевой машине

## Этап 10. Секреты второго уровня

Контекст: [security.md](implementation/security.md), [claude-code.md](implementation/plugins/claude-code.md).

- [ ] маска файлов Claude Code на Linux и WSL2, запрет чтения на macOS
- [ ] шлюз подстановки для остальных харнессов — только после замера

**Удаляется в этом этапе.** файлы авторизации с сырым ключом там, где шлюз встал

**Критерий приёмки.** в окружении и файлах агента нет настоящего ключа провайдера; отражённый провайдером заголовок вычищен из ответа

## Этап 11. Облако и удалённый MCP

Контекст: [cloud-agents.md](implementation/plugins/cloud-agents.md), [remote-mcp.md](implementation/remote-mcp.md), [github.md](implementation/plugins/github.md).

- [ ] облачный плагин для Claude Code on the web и Jules: Runtime kind = remote, маркер сессии, кандидат по OID из refs/swarm/cloud/
- [ ] удалённый MCP через Access и cloudflared с проверкой JWT и Grant

**Удаляется в этом этапе.** общий токен удалённого MCP, открывавший весь реестр

**Критерий приёмки.** облачный push в main отклонён; потерянный ответ отправки не даёт второй сессии; принципал remote не видит методов без флага remote; без JWKS — 503

**Открытые вопросы спецификации (найдены прогоном задач на свежих агентах).**

- Перечень состояний сессии Jules и Claude Code on the web, терминальные значения и поиск сессии по маркеру — сверить по документации провайдеров на дату этапа.

## Этап 12. Вторая волна и эксплуатация

Контекст: [operations.md](implementation/operations.md), [muse-code.md](implementation/plugins/muse-code.md), [antigravity.md](implementation/plugins/antigravity.md), [config.md](implementation/config.md).

- [ ] плагины Muse Code и Antigravity CLI
- [ ] триггеры cron и on\_event
- [ ] наблюдение сессий
- [ ] учёт расхода
- [ ] хранение и диск
- [ ] обновление с откатом
- [ ] Docker

**Удаляется в этом этапе.** остатки старых триггеров и watchers

**Критерий приёмки.** каждый пункт раздела 11а работает на одном проекте; откат версии без потери задач

Canonical JSON и sha256 команд не удаляются: на них стоит семантический digest admission (command\_payload\_digest, admission.rs:52-107) и ответ id\_conflict (admission.rs:116-123). Квитанции источников (source\_state) и events.put с commit-before-ack остаются границей эффекта плагина (раздел 10).
