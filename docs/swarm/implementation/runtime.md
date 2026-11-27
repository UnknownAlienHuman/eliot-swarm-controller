# Runtime агента и eliot-plugin

> Источник фактов: целевая архитектура v2.6 (27.11.2026). Живой контракт; исходная версия сохранена в archive/spec-v2.6.md; «раздел N» и «приложение X» — номера v2.6, ссылка ↗ ведёт туда, где он теперь. Общая суть — [ARCHITECTURE.md](../ARCHITECTURE.md), связи — [GRAPH.md](../GRAPH.md), навигация — [INDEX.md](../INDEX.md).

**Зачем.** Рестарт демона сейчас убивает агентов: харнесс — ребёнок плагина, плагин выходит по EOF pipe хоста (в прототипе ребёнок умер через 1 с после kill -9 родителя). Runtime агента — отсоединённый процесс на агента, который переживает рестарт демона (контракт A).

**Владеет.** Процесс харнесса и custody его семейства (владение Windows Job, process group на Unix); stdio и адаптер харнесса; журнал доставки с fsync; сокет агента (attach, write, signal, hook, console); дедлайны хода и инструмента, остаток wall-бюджета и daemon_lost_s по монотонным часам; переподключение к демону по PID + времени старта (+ boot id на Linux). Общая библиотека eliot-plugin: Peer, PendingCalls, EarlyEventBuffer, native_lifecycle, retry.

**Вход.** Протокол NDJSON от демона: describe, start, send(конверт), cancel, status; события харнесса; вызовы swarm hook и команд агента через сокет.

**Выход.** events.put (ACK только после COMMIT в Store), delivery.applied/unknown, stopped {turn, processes_exited, stream_drained}, exit, error {class, effect, structured}.

**Инварианты.**
- Runtime служит ровно одному Attempt; новый Runtime только после подтверждённой смерти старого.
- sending пишется в журнал с fsync до записи в харнесс; рестарт при sending → unknown.
- Смерть runtime гасит потомков (Windows: KILL_ON_JOB_CLOSE остаётся, handle держит runtime); на Unix смерть потомков подтверждается чтением.
- Запись журнала не удалась — CLI агента получает ошибку, ложного успеха нет.
- Кадр от плагина ≤ 8 МиБ, к плагину ≤ 1 МиБ.
- Неподдержанная возможность — явный отказ, а не тихий no-op.

**Зависит от.** [core.md](core.md) (конверт, Delivery), [store.md](store.md) (events.put), [automata.md](automata.md) (Attempt); адаптеры — [README.md](plugins/README.md); хуки — [hooks.md](hooks.md).

**Статус.** Гибель харнесса вместе с цепочкой доказана; контракт A — критерий этапа 6 на Linux и Windows; перенос владения Windows Job — вывод по коду и документации, запуском не проверен; контракт B отложен.

---

### Runtime агента: какую гарантию покупаем

Сейчас процесс харнесса — ребёнок плагина, а плагин держится на pipe хоста: хост держит pipe плагина (plugin\_host/host.rs:2112-2114), плагин выходит по EOF (eliot-plugin/src/peer.rs:179-183). В прототипе ребёнок умер через 1 с после kill -9 родителя. Значит, рестарт демона сейчас убивает агентов, и это надо чинить на всех ОС.

Есть два контракта отказа, и цена у них разная:

| Контракт | Форма | Цена |
| --- | --- | --- |
| A. Пережить рестарт демона; при смерти runtime продолжить из нативной сессии и Git | один отсоединённый процесс на агента: нынешний per-agent плагин, вынесенный из-под демона; держит адаптер, stdio, сокет, журнал доставки и custody | отдельного моста shim ↔ плагин нет; смерть runtime = смерть харнесса, ход прерывается, задача продолжается через resume |
| B. Пережить ещё и смерть адаптера без остановки харнесса | независимый владелец процесса и транспорта плюс заменяемый адаптер | второй процесс — настоящая граница отказа: нужно восстановить незавершённые RPC и привязки ходов, обработать поздние ответы, определить unknown; сырых байтов stdio и PID для этого мало |

Первая волна — контракт A. Он закрывает найденный отказ (рестарт демона убивает агентов) и не требует второй границы. Контракт B откладывается, пока данные не покажут, что смерть адаптера при живом харнессе случается достаточно часто, чтобы окупить восстановление RPC. Требование v2.3 «kill плагина — харнесс жив» поэтому снято с критериев первой волны.

Runtime собирается из существующего платформенного кода: отсоединённый запуск (windows.rs:201-237), запасной путь без breakaway (windows.rs:476-491), suspend-assign-resume (custody.rs:567-612), остановка дерева через TerminateJobObject. Демон поднимается и переподключается к живым runtime по PID плюс время старта процесса (Windows creation FILETIME, windows.rs:824-852; Linux /proc starttime плюс boot id, unix.rs:358-374). Команды агента, пока демон лежит (done, note, send, blocked), runtime пишет в свой журнал с fsync на каждую запись.

Windows: владение Job переносится, KILL\_ON\_JOB\_CLOSE не удаляется. Сейчас Job принадлежит OwnedChild на стороне демона (custody.rs:30-34), флаг стоит в windows.rs:1219, а Drop у Job завершает семейство и закрывает handle (windows.rs:1298-1307). По документации Microsoft процессы Job завершаются, когда закрыт последний handle. Если единственный долговременный handle держит runtime, рестарт демона его не закрывает, а смерть runtime гасит потомков: это нужно, чтобы после смерти runtime никто не писал в worktree, который отдадут другому. Условия: runtime сам вне своей Agent Job, унаследованных и дублированных handle не осталось, отделение от внешней Job подтверждено. На Windows runtime запускается с DETACHED\_PROCESS, CREATE\_NEW\_PROCESS\_GROUP и CREATE\_BREAKAWAY\_FROM\_JOB; breakaway возможен, только если Job родителя разрешает JOB\_OBJECT\_LIMIT\_BREAKAWAY\_OK, а при вложенных Job может быть неполным, иначе демон предупреждает и предлагает запуск службой. На Unix смерть владельца сама потомков не гасит: process group даёт более слабую гарантию, и смерть потомков подтверждается чтением, а не предполагается. Убийство групп при рестарте демона (router.rs:1335-1340) убирается, в systemd-юнит демона добавляется KillMode=process. Сейчас его там нет: юнит из linux.rs содержит Restart=on-failure и StartLimitBurst, но не KillMode, поэтому systemctl restart гасит всю cgroup вместе с агентами; это пункт этапа 6. Это вывод по API и исходникам; квалификация на Windows — критерий этапа 6.

## Контракт: протокол runtime агента (раздел 14)

### Протокол runtime агента (NDJSON по stdio и сокет)

Здесь `gen` — `Task.gen`, номер executor Attempt. Для local запуска Store при dispatch сохраняет в Runtime отдельную неизменную пару `agent_id + agent_generation`; адаптер получает её из принятой команды. Resume создаёт новый Runtime и native generation того же Attempt, сохраняя `Task.gen`. Remote использует `external_ref`. Эти значения нельзя взаимозаменять; [канонический контракт](automata.md#жизненные-циклы-остальных-сущностей).

- describe → имя, тип, версия харнесса, kind (local | remote), операции для реестра, согласованные возможности (resume, steer, approval, hooks, mask; для remote — cancel, follow\_up, webhook, max\_session\_min).
- start {task, rev, gen, route, worktree, env\_allowlist, deadlines} → {runtime\_id, session\_id | external\_ref}. deadlines = {turn\_max\_s, tool\_max\_s, wall\_left\_s, daemon\_lost\_s}: runtime сам держит дедлайны хода и инструмента и остаток wall-бюджета по монотонным часам и прерывает ход по истечении, даже если демон лежит (сессия сохраняется для resume). Демон недоступен дольше daemon\_lost\_s (по умолчанию 10 минут) — runtime даёт доиграть текущий ход и останавливает харнесс до возвращения демона.
- send {конверт} → stored; дальше [Delivery](automata.md#переходы-delivery): stored → sending → applied | unknown. При sending сохраняется `runtime_id`; delivery.applied {id, turn} и delivery.unknown {id} получают эту привязку от аутентифицированного канала адаптера. ACK старого Runtime не закрывает отправку нового Runtime с тем же Envelope id; старый unknown разрешается сверкой, без слепого повтора.
- cancel {mode: interrupt | stop} → запрос; подтверждение остановки — отдельное событие stopped {turn, processes\_exited, stream\_drained}.
- status → {alive, session\_id | external\_ref, last\_event\_at}.
- events.put {пачка} → ACK только после COMMIT в Store; квитанция источника в source\_state.
- События: turn\_start, turn\_end {usage, outcome}, tool\_start, tool\_end, commit, delivery.applied, delivery.unknown, error {class, retry\_after, structured}, stopped, exit {code, stderr\_tail}. Класс ошибки — из общей таблицы: transient, permanent, quality, network, process\_died; отдельно поле effect: none | applied | unknown; structured = true, только если класс взят из кода или поля протокола, а не из текста.
- Сокет (unix socket или named pipe): attach (поток событий с курсора), write (stdin харнесса), signal (interrupt, stop), hook (запрос swarm hook), console.
- Журнал: сигналы агента, пока демон мёртв, fsync на каждую запись, предел по байтам; демон дочитывает после переподключения и подтверждает курсором. Запись не удалась (диск полон, предел журнала) — CLI агента получает ошибку, а swarm done не считается принятым; ложного успеха нет.
- Хуки отвечает runtime из своего журнала и кэша, не дожидаясь демона: непрочитанные durable-сообщения лежат в журнале runtime, поэтому Stop hook укладывается в свой таймаут и под нагрузкой. Таймаут хука записывается как Event hook\_timeout; сообщение уходит первым входом следующего хода.

Методы нынешнего протокола плагинов (plugin-protocol.md:584-598) и их судьба:

| Сейчас | В v2.6 |
| --- | --- |
| events.put | сохраняется, commit-before-ack |
| source.state, source.gap | сохраняются внутри events.put как квитанция источника и признак разрыва |
| source.health | заменяется status |
| source.handoff | заменяется пакетом передачи при resume и replace (раздел 4 [↗](store.md)) |
| process.put | событиями tool\_start и tool\_end с pid группы |
| child | дочерние Task (раздел 11а [↗](../INDEX.md#карта-разделов)); нативные субагенты запрещены |

### Рамки протокола

Кадр NDJSON от плагина к демону — не больше 8 МиБ, от демона к плагину — 1 МиБ. Превышение — отказ кадра с классом permanent. Большие данные идут ссылкой на файл в рабочем каталоге, а не в кадре.

## Общая реализация плагинов (раздел 10)

Плагин — отдельный процесс, NDJSON по stdio; в первой волне это и есть runtime агента (раздел 3 [↗](daemon.md), контракт A). Новый протокол и новый SDK не пишем. Общая реализация уже есть, и её сохраняем: Peer используют все пять плагинов (codex, claude, opencode, antigravity, muse), общие PendingCalls (native\_transport.rs:895), EarlyEventBuffer (event\_buffer.rs:27), native\_lifecycle, capabilities и retry::wait\_until (retry.rs:10) живут в eliot-plugin. Остаются соединение и буфер событий, рамки протокола (protocol.rs:51-174), crash-nonce и подъём плагина хостом (host.rs:2085-2148).

Подтверждения не удаляются. В 2.3 было написано, что уходят «events.put, settle, readback и proof». Это ошибка: settle, readback и proof — не методы протокола, а имена функций (retry::settle\_readiness, stop\_readback::settle); методы протокола — events.put, source.state, source.health, source.gap, source.handoff, process.put, child (plugin-protocol.md:584-598). events.put с commit-before-ack (plugin-protocol.md:687-693) и квитанции источников (source\_state: курсор, id, digest и границы последней пачки, schema.rs:532-546, source\_batches.rs:1722-1735) — это и есть граница эффекта между плагином и Store. Её смысл сохраняется; повторный код подтверждений сводим в общий runtime, а не удаляем. Сейчас при потерянном подтверждении клиент выводится из работы и возвращает unknown без повтора (peer.rs:442-445): это правильное поведение, его и оставляем.

Сокращаем за счёт общих рамок, порядка жизненного цикла, ACK и курсора, доставки. Нативные id сессий, подтверждение interrupt и неподдержанные операции остаются в адаптере: HTTP/SSE OpenCode, JSON-RPC Codex и stream-json Claude требуют разных правил перевода. Проверка мелкого «упрощения» дала отказ: общий тип TerminalDeadline обернул бы Option<Instant>, ожидание уже вынесено в retry::wait\_until, а моменты старта и прогресса у адаптеров разные; refresh\_terminal\_deadline остаётся локальным.

Сообщения протокола в целевом виде: describe (имя, версия, kind local или remote, возможности конкретной версии харнесса, операции для реестра), start, send(конверт), cancel, status, events.put и поток событий; судьба нынешних методов — таблица в разделе 14 [↗](../INDEX.md#карта-разделов). Возможности согласуются: resume, steer, approval у харнессов разные, и неподдержанное остаётся явным отказом, а не тихим no-op. Намерения (intents) вместо команд — позже, когда они понадобятся двум плагинам.

Граница с харнессом: Swarm владеет задачей, назначением и допуском внешнего результата; харнесс — диалогом, циклом инструментов и компакцией. Его собственный retry не дублируется повторной отправкой Swarm.

## Переходы Attempt

Состояния, native generation и условия resume — [automata.md](automata.md#жизненные-циклы-остальных-сущностей).

## Этапы миграции

Порядок и критерии — [migration.md](migration.md); отметки выполнения — [backlog.md](../backlog.md).

## Модули (раздел 14)

| Модуль | Что в нём | На чём строится |
| --- | --- | --- |

| agent runtime | отсоединённый процесс на агента: адаптер харнесса, stdio, журнал доставки, сокет, custody и владение Job, дедлайны; для kind = remote — облачный плагин без своего состояния | нынешний per-agent плагин плюс custody.rs, windows.rs, unix.rs |

| eliot-plugin | общий Peer, PendingCalls, EarlyEventBuffer, native\_lifecycle, retry | нынешний eliot-plugin/1 |
