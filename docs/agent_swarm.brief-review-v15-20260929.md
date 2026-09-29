# Проверка обновлённого MANAGER-BRIEF — v15

**29.09.2026. Проверка документа, а не повторная квалификация рабочего роя.**

## Основания и граница проверки

Прочитан целиком [MANAGER-BRIEF.md](MANAGER-BRIEF.md): 795 физических строк (индекс Files: 796 с завершающей пустой строкой),
SHA-256 `bd9192cdfcc5b9e1810effac315e0b377ac8b593042cf656032b8be9b9e32600`.
Сравнение с прежним 729-строчным [MANAGER-BRIEF(1).md](MANAGER-BRIEF%281%29.md) сохранено в
[brief-delta.patch](review-v15/brief-delta.patch). Архитектура v14 и implementation-v2 проверены полностью.
Новые реализации `codex_as.py`, `Merge-Daemon-v5.sh`, reverse-dependency helper и `Prune-Branches.py`
в этом сообщении не приложены. Старые ZIP не доказывают содержимое новых скриптов.

Обозначения: **Бриф** — сообщение Claude о своей машине; **проверено** — текст/байты либо внешняя
документация; **вывод** — предлагаемый контракт прототипа. Числа скорости/процессов из брифа не наши замеры.

## 1. Что действительно изменилось

| Изменение | Источник в новом brief | Как учитывать |
|---|---|---|
| После перезагрузки общий Codex server запущен, пакет 0.159.0 | §13, строки 610–637; §14, 776–778 | Уже не «ждём reboot». Состояние reported-working; actual binary/schema проверяются перед присоединением. |
| Сервер запускают напрямую `app-server --listen unix://` в скрытой консоли, без постоянного daemon updater | 625–632 | Это внешний общий процесс и отдельный profile; не запускать свой сервер на каждую линию. |
| Proxy переносит WebSocket поверх stdin/stdout; goal/steer опробованы | 633–637 | Указание `stdio` не означает JSONL. Меняются transport и scope наблюдения, не только executable. |
| OpenCode запускают скрыто, приложение подключают потом; восстановилось 25 сессий | 638–641, 777 | Сначала сверка восстановленных roots/children, потом новые назначения. |
| v5 делает локальный recheck на прогретом слоте и сокращает compile scope | 355–358 | Полезная оптимизация; полноту scope надо проверить отдельно по helper. |
| Branch cleanup включён в обслуживание; 303→201 refs по отчёту | 536–543, 780 | Сохранять schedule после reboot; pruning — отдельная явно разрешённая операция. |
| Merge #4120 завершён, а запись Task сорвалась на CLAIM-папке | 781 | Приоритетный пример восстановления bookkeeping без повторного исполнения. |

## 2. Находки и конкретные решения

### B01 — Несколько взаимоисключающих runbooks в одном brief

**Проверено:** рабочий §4 всё ещё содержит `opencode run` и Codex exec без steer (148, 174–182).
§12 и §13 продолжают рекомендовать `opencode api` (600–602, 643–644), хотя 760 запрещает такие
API-вызовы, а 764 переносит создание сессии на HTTP. При этом 764 отдельно оставляет CLI для
продолжения через `-Session`: это не устранённое исключение, которое нужно согласовать с описанным
риском любого CLI-старта; нельзя утверждать, что brief уже запретил и этот путь. §14 предлагает
Answer-Forms (675), тогда как 587 запрещает его запуск. Это противоречие документа, не доказательство
выполнения старых команд текущими скриптами.

**Исправить brief:** обновить примеры непосредственно в §4/7/12/13; старые команды пометить историческими
в §14. В шапке заменить 26.09 на фактическую дату ревизии. Оставить одну короткую таблицу текущих
entrypoints; не вводить ещё один обязательный файл правил в контекст всех агентов.

**В прототипе:** runtime profile — единственный источник команд запуска. История инцидентов не исполняется.

### B02 — Запуск общего Codex: конфигурация окружения теперь важнее PID клиента

**Бриф:** неповышенный процесс, отдельный пакет, WMI, user socket и вручную запускаемый shared server
(610–637, 776–777). Запуск updater-сервиса специально заменён прямым listener.

**Вывод:** разделить в route: server owner, transport, connection/client и startup package.
Для текущей машины Codex C07 присоединяется к уже работающему shared server. Owned stdio остаётся
отдельным, явно выбранным профилем для изолированной пробы, не fallback при ошибке attach.
Разрыв proxy закрывает connection, а не весь сервер. Shared server не входит в Job одной линии.
Не запускать `daemon start`, установщик или updater из status/doctor.

Сохранять resolved executable/version **сервера**, client/adapter version, startup cwd/env, user/session
и способ запуска. Путь `current` — alias, не immutable version. Глобальный `.codex` не разделять.
Внешняя проверка в этой итерации не установила состояние установленного 0.159.0 и его совместимость с
pinned SDK; новая reported version не повод молча обновить все donor pins.

### B03 — «Скрытая консоль» не равна detached и не гарантируется одним флагом на всех потомков

**Бриф:** 0 видимых окон в 60-секундном наблюдении после WMI ShowWindow=0 (615–624, 777).
Это полезный локальный результат, но не гарантия на все способы spawn.

**Проверено [W1–W4]:** `CREATE_NEW_CONSOLE`, `DETACHED_PROCESS`, `CREATE_NO_WINDOW` имеют разные
контракты. `CREATE_NO_WINDOW` игнорируется вместе с NEW_CONSOLE/DETACHED.
`STARTF_USESHOWWINDOW + SW_HIDE` задаёт видимость создаваемого окна. WMI `ShowWindow` отображается
на `wShowWindow`; Microsoft отдельно отмечает иной Job association для Win32_Process.Create.

**Решение:** небольшой Windows LaunchProfile, а не один глобальный `detached=true`.
`hidden_console` — совместимый с текущим runbook режим; `no_window` — отдельный проверяемый режим
собственных фоновых команд. Token, Job, console и stdio — независимые измерения.
WMI сохранить как разрешённый bootstrap-путь из текущего ограниченного launcher, не вызывать для каждого
tool и не обещать, что WMI само понижает integrity. У user-host вне чужого Job обычный Win32 spawn
предпочтительнее дополнительной цепочки PowerShell/WMI.

Наличие дочернего `conhost.exe` не универсальный критерий. Проверять реальные tool/MCP/cargo-запуски,
отсутствие видимых окон, пригодность stdio и token. Выборочная проверка окон может пропустить короткое
всплытие — записывать метод измерения. Установку UAC/ACL не автоматизировать.

### B04 — JSONL клиента и WebSocket proxy нельзя соединить сменой argv

**Бриф:** JSON-строки в `app-server proxy` не работали; добавлен RFC 6455 клиент (776).
Рукописный WebSocket не проверен: его обновлённого исходника здесь нет.

**Решение:** собственный host↔bridge IPC остаётся NDJSON. Native transport описывается явно:
`stdio_jsonl` или `websocket_over_proxy_stdio`; SDK должен действительно поддерживать выбранный transport.
Простой `launch_args_override = proxy` у клиента, ожидающего JSONL, не является адаптацией.
Использовать готовый WebSocket transport/donor, включая Upgrade, masking, fragmentation, ping/pong,
close и ограничения буферов. Не добавлять самодельный RFC 6455 в Rust-core и не ставить новый постоянный
сервер-прослойку. Эта seam локализована в Codex-модуле; не расширяет C01–C06.

### B05 — Полнота семьи изменилась при переходе на shared server

**Противоречие:** 753 требует считать все чужие threadId в логе линии, а 633–637 сообщает, что события детей
туда больше не приходят и используется SUBAGENTS. Последний вариант надо описать как конкретный источник,
а не как гарантированную полноту native семьи.

**Решение:** family observation квалифицируется для transport/connection scope. Root subscriptions,
server-wide inventory и производный SUBAGENTS — не одно и то же. Side-file не создаёт authority;
его отсутствие, старая mtime или ошибка чтения дают unknown/partial, не zero.
На startup сверять и прежние roots с незакрытыми детьми: старый родитель может снова проснуться (775).
Новые дубли не создаются; найденные старые дубли описываются наблюдениями, не запихиваются в индекс
`one_live_root_per_lane` как два новых разрешённых владельца.

### B06 — Модель ребёнка должна иметь evidence конкретного исполнения

**Бриф:** заголовок `session_meta` содержит родительскую модель, а 11 детей имели Luna max в `turn_context`
(778). Это исправляет ложную атрибуцию, но не предоставляет прямую provider-side аттестацию.

**Решение:** requested model/effort отдельно от native-resolved model/effort с source и native turn.
`turn_context` — более точный native execution record для описанного формата; не универсальное поле
всех harness и не доказательство фактического model serving за непрозрачным proxy. Child не наследует
reported effective model родителя автоматически. Полный перечень моделей в каждом prompt не нужен.

### B07 — Fork для вопроса не является наблюдением и не исправляет живого менеджера

**Бриф:** `codex_ask.py` fork read-only (770) заменён `codex exec fork` (642).
**Проверено [W5]:** fork создаёт другую нить с копией сохранённой истории; read может читать без resume.

**Решение:** сначала status/check artifacts/read. Fork только для содержательного ограниченного аудита,
с отдельными модельной стоимостью и источниками. При переходе на exec fork явно сохранить read-only
профиль прежнего helper; строка новой команды его не показывает. Ответ копии не является ответом
живого manager и не подтверждает применения correction. Самостоятельный STOP/goal старой нити
по такому ответу не выдавать. Совместимость наследования goal/config проверять в C07, не предполагать.

### B08 — Оптимизация reverse dependencies полезна; `--no-deps` требует аккуратного контракта

**Бриф:** affected+dependents, fallback на workspace при >60%, recheck на том же warm slot (355–358).
**Проверено [W6]:** `resolve` равен null при `cargo metadata --no-deps`, но declarations
`packages[].dependencies` остаются. Значит, такой helper **может** строить консервативный граф
workspace declarations; объявлять его сломанным только из-за --no-deps было бы неверно.

**Проверить в новом helper:** aliases/rename, path/package identity, build/proc-macro edges,
optional/target dependencies, изменения manifests/lockfiles и shared codegen/config inputs.
`resolve=null` не превращать в пустой доказанный граф. `clippy.toml`, toolchain, `.cargo/config*`, build.rs
и внешние inputs могут влиять шире ближайшего crate. 60% — tuning переключения, не доказательство полноты.

**В прототипе:** выбирать самый простой sound scope для текущего candidate/profile; metadata cache и
консервативная reverse closure, не собственная build system. В спорном случае широкий разрешённый
lib/bin профиль либо явно diagnostic result. Cache/warm directory сохранять, повторная проверка не
создаёт новый assignment. Десять admission slots текущего роя не становятся default десяти Cargo jobs.

### B09 — Source export может вернуть sccache и неправильное окружение

**Бриф:** nested `.cargo/config.toml` отключил унаследованный wrapper в дереве eliot-swarm (762).
**Проверено [W7–W8]:** Cargo конфигурация зависит от cwd/предков; env может переопределять config;
пустой RUSTC_WRAPPER сбрасывает wrapper. RUSTC_WORKSPACE_WRAPPER — отдельная настройка.

**Решение:** CheckSpec явно сохраняет effective cwd/env/toolchain/target-dir/wrapper policy.
Экспорт источника вне прежнего дерева не наследует этот фикс автоматически. Применять scoped overlay
именно к процессу Cargo, а не менять глобальные настройки или только env proxy Codex.
Не отключать все wrappers по умолчанию: воспроизводить выбранный профиль и проверять actual command.

### B10 — Post-merge ошибка не должна стирать факт публикации

**Бриф, 781:** PR #4120 влит, PUSHED снят, затем os.remove(CLAIM-папка) завершился ошибкой.
Повтор конкретного finish_review автор проверил вручную; безопасный повтор произвольного скрипта
из этого не следует.

**Решение:** на existing operations:
`publish intent → remote effect/readback → record publication → acceptance/bookkeeping → cleanup/notification`.
External publication и local acceptance независимы. Remote applied не откатывается в not-sent из-за
сбоя очистки. Если commit БД после merge не удался, прежний intent остаётся на reconciliation.
Уведомления/очистка повторяются отдельно; новый writer и повторный merge не запускаются.
CLAIM/PUSHED — только legacy projections; входной malformed marker не превращается в кодовую ошибку.
Не удалять произвольную CLAIM-папку рекурсивно; принадлежащую control root запись можно сохранить и
сбросить после записи факта. В новой реализации marker-файлы не нужны для ownership.

### B11 — Короткий merge lock: полезное изменение, но тело runbook всё ещё описывает старое

§8 v4 говорит, что bookkeeping вне lock (360), а §8.2 всё ещё выполняет `state.py merged; munlock` (396).
§8.3 требует --all-targets (413–415) вопреки новым lib/bin правилам. §8.4 не показывает
`--match-head-commit`, хотя его наличие утверждается ранее (63). Без новых скриптов нельзя объявить
точный runtime-дефект или его исправление.

**Исправить описание и проверить код:** merge шагает по exact candidate head; форматирование меняет
кандидат и требует соответствующего результата. `--match-head-commit` фиксирует PR head, не base [W9].
Локальный lock не блокирует сторонний push/merge на GitHub. После смены base требуется установленная
политика актуальности проверенного результата; reports и cleanup не держат serialization.
Не добавлять mandatory branch protection или broad tests к текущему порядку владельца.

### B12 — Drain должен закрывать приём работы, а не только гасить launcher

**Бриф:** родитель снова проснулся от старого ребёнка; 34 check-процесса пережили остановку демонов (775).

**Решение:** `new_work=draining` сохраняется до уведомлений; native goal не должен снова брать очередь.
Сверяются старые roots/дети, unresolved operations и check jobs. Закрытие listener/runner не считается
завершением сервисов. Own short check Job можно завершить по разрешённой отмене, записав interrupted;
same Job-policy нельзя применять к общему Codex/OpenCode или их чужим сессиям [W4].
Завершённая в GitHub публикация восстанавливается readback, а не доказательством «есть открытый PR».

### B13 — Расписание должно переживать GM и reboot без нового scheduler-сервиса

**Бриф:** обслуживание пропущено после reboot (656–658, 780); циклы §12 живут в сессии root (573).

**Решение:** existing scheduler хранит due и последний admitted slot в `meta`; запуск — обычная Operation,
ключ `(schedule_id, due_slot)`. После reboot пересверить текущие jobs, затем объединить пропуски в один
актуальный проход. Не проигрывать все просроченные напоминания и не запускать cleanup до проверки
живых owners. Ошибка секции статистики (779) выдаёт partial report, не уничтожает остальные результаты.
Model-assisted maintenance, если выбрана, видна как реальный model run; список состояний бесплатный.

### B14 — Cleanup refs: возраст не защищает от гонки с новым коммитом

**Бриф:** полезны archive ancestry, сохранение refs, учёт PR/ожидающих сдач и существующих checkouts (536–543).
Условия актуальности нужно проверять непосредственно перед удалением.

**Решение:** план содержит exact old tip, archive proof и protected references. Перепроверка полного
нужного inventory + условная ref-операция с ожидаемым SHA (семантика exact force-with-lease [W10]).
Новый tip отменяет только это удаление. После неизвестного исхода — readback. Git ref CAS не делает
проверку GitHub PR и удаление одной транзакцией: внешние участники остаются границей гонки, поэтому
уборка только в согласованном ownership-домене. Прототип не создаёт worktrees и новые ветки ради этого;
операции над legacy-ветками — поздний opt-in, не начальная обязанность C01–C06.

### B15 — Разрешённые предпосылки больше не должны упираться в искусственный path-забор

**Бриф:** 771 разрешает дописать тип/порт/producer/caller в модуле владельца за пределами начальной Issue-area;
перед писателями делается READINESS. Старое «scope — только другое Issue» больше не описывает всю политику.

**Решение:** в TaskSpec отличить initial_paths от explicit forbidden_paths и разрешённой prerequisite_policy.
Исправление необходимой предпосылки в заранее разрешённом домене не требует нового human approval
или отдельной Issue. Перед изменением общего owner — согласование с текущим исполнителем и сохранение
пути/причины в assignment facts; остальная работа продолжается. Если меняются цель/acceptance/реальный
запрет, нужна task revision. Нельзя придумывать missing receipt/данные или переименовывать redesign
в prerequisite. Documentation bundle расширяется по новым реально затронутым путям без перечитывания
всей истории и без тяжёлого универсального compiler.

### B16 — Ранее найденные проблемы остаются в тексте; не выдавать их за новые code findings

- `wait_agent(timeout=0)` всё ещё указан (298); приоритет — schema/tools конкретного binary, не эта строка.
- `after` выполняется по любому merge (213–215); это не доказательство конкретной требуемой предпосылки.
- ограничители 90 минут/два возврата (61) против отмены (308–309).
- stop никогда (35–36) против исключения ребёнка без ответа и прогресса за 30 минут (307, 725).
- интервалы reminders 15/30/60 минут и старые CLI routes смешаны (600–606, 756).
- reachability считает неизвестное/лимит положительным исходом (452–475); это bounded heuristic, не proof.
- срок CLAIM встречается как 3, 6 и 8 часов; новый owner нельзя назначить по одной давности.
- `v2` + перенос + `evdeps.py` (357–358) — реально разорванный inline-путь; точное имя проверить по файлам.

Это перечень точечных правок brief. В прототип не переносятся автоматические timeboxes, зелёный PASS
из UNKNOWN или новые жёсткие permission gates. Исключение остановки конкретного ребёнка сохраняется
как явная политика владельца с проверкой отсутствия **и** ответа, **и** продвижения; не смерть по mtime.

## 3. Что меняется в архитектуре, а что остаётся

**Меняется:** deployment profile; provenance модели/семьи; раздельные publication/finalization;
консервативный compile scope; restart/catch-up; начальная область и разрешённые предпосылки.
**Остаётся:** один crate, одна SQLite (9 таблиц), один scheduler/outbox, Muse+OpenCode первые,
Codex C07, no UI/broker/новый watchdog-stack, full-access harness, main-only для нашей разработки.
Новые Windows flags и vendor protocol не попадают в model prompt.

В C01 не добавлен запуск Codex: typed JSON runtime settings и existing Store/Operation достаточны.
C06 проверяет CheckSpec и ресурсную очередь; C07 — actual shared transport/family; C09 — external effect
и bookkeeping; C10 — schedule/logon/drain. Это локальные изменения в уже выбранных пакетах.

## 4. Что потребуется получить для code-аудита рабочего роя

Новые `codex_as.py` и launcher; текущие v5/Review/Make-PR/reverse-dependency helper;
Prune-Branches и stop-helper; свежие SECTORS/TEMPLATE и небольшой post-reboot лог.
До их чтения нельзя утверждать, что повторные тайм-ауты, WebSocket, child tracking, Clippy exit-code
и merge-ref checking исправлены именно в установленном коде. Это граница доказательств, не блокировка
написания прототипа.

## Внешние источники, прочитанные 29.09.2026

[W1] Microsoft — Process Creation Flags: https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags

[W2] Microsoft — STARTUPINFOW: https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/ns-processthreadsapi-startupinfow

[W3] Microsoft — Win32_ProcessStartup: https://learn.microsoft.com/en-us/windows/win32/cimwin32prov/win32-processstartup

[W4] Microsoft — Job Objects, включая WMI spawning: https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects

[W5] OpenAI — App-server, thread read/fork/subscriptions: https://developers.openai.com/codex/app-server/

[W6] Cargo metadata, --no-deps/resolve: https://doc.rust-lang.org/cargo/commands/cargo-metadata.html

[W7] Cargo configuration hierarchy: https://doc.rust-lang.org/cargo/reference/config.html

[W8] Cargo environment/wrappers: https://doc.rust-lang.org/cargo/reference/environment-variables.html

[W9] gh pr merge, --match-head-commit: https://cli.github.com/manual/gh_pr_merge

[W10] Git push, exact --force-with-lease: https://git-scm.com/docs/git-push

Дополнительно просмотрены snippets текущих upstream Codex README/CLI. README и web-docs не дали
в выбранных разделах актуального proxy runbook; версия установленного сервера и ручной RFC 6455
остаются сведениями brief. Не подменяем этот пробел зеркалами документации или утверждением о 0.159.0.
