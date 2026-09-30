# Выводы из прежних запусков и проверок

**Сведено 30.09.2026 из материалов 26–29.09.2026.** Это справочник причин принятых решений, не ещё один brief и не новые требования к каждому worker. Действующий порядок — [архитектура](agent_swarm.md), [план реализации](agent_swarm.implementation-v6.md), [контракт модулей](agent_swarm.module-contract-v2.md).

**Достоверность:** B — отчёт Claude о его машине, без нашей повторной runtime-пробы; R — прежний разбор документации и выбранных исходников; D — проверка проекта/SQL, не работающего сервиса. Наблюдения привязаны к указанным версиям. Здесь не проведена новая квалификация harness.

## 1. Инфраструктура иногда мешала сильнее модели

| Случай из источника | Сохранённый вывод для реализации | Где учтено |
|---|---|---|
| B §14: повторные CLI-проверки OpenCode при нагрузке запускали recovery общего сервиса и теряли писателей. Даже отправка напоминания через CLI могла сделать это. | Наблюдение, создание и доставка — через direct HTTP существующего V2-сервиса; observer не становится вторым lifecycle-owner. Reported helper ещё оставлял CLI-resume: его нельзя автоматически считать проверенным безопасным путём. | C04; runtime-notes §1 |
| B §14: родитель исчезал из active, его дети продолжали работать; новая root запускалась, а старая затем просыпалась от результатов. | Сверять root, native children, continuation и прежние незакрытые семьи. Timestamp, отсутствие в active и PID клиента не доказывают завершения assignment. | C03/C04/C07; Binding/ProducerRef |
| B §14: у Codex показывали 0 детей при работающих 4; после shared-server перехода прежний event-source перестал давать полное семейство. | Полнота привязана к transport/entrypoint и scope подписки. SUBAGENTS — дополнительное свидетельство, не абсолютная истина. При gaps — unknown/partial, не ноль. | C07; snapshot coverage |
| B §14: Muse view перестала обновляться, wrapper ждал одно terminal event; некорректные ответы на approvals оставляли детей ждать. | Отдельные reader, pending native requests, view health и targeted reconciliation. Ответить текущими choice/requirement IDs; parent turn не закрывает SDK. | C03; MSP pump |
| B §14: OpenCode Beta повторяла ответы на исчезнувшие permissions, получая поток 404; перезапуск клиента с разрешения владельца прекратил его без остановки сервиса. | Отличать UI/client, observer и executor. Устаревший запрос не лечится бесконечным retry; локализовать отказ в инициаторе. | Doctor; reply stale guards |
| B §14: Git-снимки перед/после tools конкурировали за общий индекс. После отключения snapshots в OpenCode 2.0.7 автор сообщил о снижении медиан чтения 14→1,9 с и правки 42→4,9 с. | Измерять queue delay и tool execution отдельно. Не вводить свои repository snapshots на каждый tool. Эти числа — локальный reported результат, не benchmark всех OpenCode. | C04/C09; rare savepoints |
| B §14: 18 Cargo и множество клиентов sccache ждали общий wrapper; scoped override восстановил прямой compiler. | Проверять actual executor/env и descendants, а не только число запущенных jobs. Перенос source checkout может потерять вложенный Cargo config; wrapper policy передавать конкретному CheckRun. | C06; CheckSpec |
| B §8/14: проверки и bookkeeping под merge lock ограничивали throughput; больше слотов не снимало сериализацию. | Только короткий необходимый участок publication сериализован. Аудит, сборка, отчёт и cleanup — вне него; сохранять warm target, учитывать потребителей изменённых пакетов. | C06/C09 |
| B §14: PR #4120 уже влит, но local finalization упала на CLAIM-каталоге после снятия PUSHED. | Remote applied, task acceptance и cleanup — разные состояния. Сохранить intent/readback; повторять незавершённый post-action, не writer и не merge. | C09; publication recovery example |
| B §14: cron в сессии GM не сработал; после reboot был забыт maintenance runner. | Due work принадлежит host, а не беседе GM. Slot и Operation сохраняются вместе; missed reminders объединяются, модель не вызывается ради таймера. | C05/C10 |
| B §14: stop-helper сообщил успех без совпавших PID; Cargo-потомки пережили остановку wrapper. | Проверять exact process creation identity и реальный disposition. Check resource остаётся занят до release; shared native server не входит в Job одной линии. | C06/C10 |
| B §13/14, R B02–B04: UAC, inherited Job и console mode давали разные отказы; JSONL не подходил WebSocket proxy. | Token, Job, console, stdio и transport — независимые настройки. WMI ShowWindow=0 — reported bootstrap, не универсальная гарантия и не команда на каждый tool. Не менять UAC/ACL автоматически. | C07/C10; runtime-notes §3 |
| B §14: правка выполняющегося Bash-файла давала syntax error; Windows escapes, BOM/CRLF, интерактивный Git-editor и длинный argv ломали служебные операции. | Versioned immutable scripts; явная кодировка и argv/cwd/env; файлы для длинных body; Git без ожидающего редактора. Не превращать строковое имя или возраст каталога в доказательство владения. | platform/forge/update |

Источники: [B — поздний brief][B], [B-old — прежний brief][B-old], [R — разбор deployment и операций][R]. Поздние записи B расходятся с его ранними командами; выводы выше сохраняют причины отказов, а не все исторические способы исправления.

## 2. Качество работы и расход контекста

| Наблюдение/ошибка прежнего процесса | Что сохраняем; чего не переносим |
|---|---|
| Много PR и строк при небольшом числе завершённых Issue; искусственные 90 минут/два возврата поощряли переключения. | Мерить принятый результат, переделки и цену. Отменённые timebox, PARTIAL как штатная цель и гонка за числом PR не становятся правилами прототипа. |
| Старые комментарии, 80-строчные очереди и ненужные plugins забивали контекст. B сообщает сокращение одного пакета Issue со 101 до 26 КБ. | Выдавать цель, актуальные требования/находки и exact sources; фильтровать историю прогресса, не нормативные требования. Короткий результат со ссылками. Не включать compaction или снижать effort без решения владельца. |
| Изменение файла brief не означало, что живой агент его перечитал. В одном документе оставались одновременно запрещённые и рекомендуемые команды. | Один действующий контракт, отдельная native-доставка коррекций, известный момент применения settings. Снятые правила удаляются из рабочего дерева, а не переопределяются ещё одним абзацем. |
| Expected checklist копировался из предыдущего неполного checklist; Addendum/AUD-пункты могли исчезнуть совсем. | Полноту сравнивать с независимым набором требований Task revision, включая принятые нормативные комментарии. Не загружать всю историю комментариев вместо такой выборки. |
| Существование receipt/символа принимали за соответствие операции; bounded reachability heuristic считала неопределённость успехом. | Exact candidate, содержание evidence, заданная гарантия и независимая приёмка. Символ/grep — дешёвая подсказка, не доказательство вертикали; timeout поиска — unknown, не PASS. Проверка должна соответствовать типу файла и фазе. |
| Reviewer иногда ошибался в координатах/счётчиках; один finding заставлял повторять большой аудит. | Требовать конкретный норматив, anchor и контрпример. Перепроверять затронутый вывод; не начинать заново writer при сломанной упаковке сохранённого кандидата. Два самоотчёта одной модели не заменяют независимую проверку. |
| BLOCKED-BY scope скрывал отсутствующий producer/порт, необходимый самому заданию. | Initial paths — начало; реальные запреты и заранее разрешённые prerequisites — отдельно. Дописать необходимое в модуле владельца с согласованием пересечений, не выдумывать отсутствующее evidence. Изменение цели — новая revision. |
| Один foreground child занимал менеджера; неверные счётчики вызывали повторные напоминания. | Native background только при поддержке, подсчёт реальной семьи. Когда есть готовый результат — предметный nudge; когда полезной независимой работы нет — event wait, не LLM-polling ради занятости. |
| Квоту определяли по цитате из PR; временное ограничение переписывало постоянный active-флаг. | Только native structured error/usage с account/window/scope. Временная недоступность отдельно от желаемой конфигурации. Не переносить старое универсальное «каждый 429 — ждать час» как контракт всех провайдеров. |
| Родительские настройки принимали за фактическую модель ребёнка. | Requested, native-effective и наблюдённое исполнение — отдельно, со своим источником; имя файла/модели не подтверждает billing и режим. |

Основания: [B §6/14][B], [R B06–B16][R], [H §2.5 и техническое приложение][H]. GUI/phone-first рейтинг H, его обязательные ограничения и предпочтение конкретного GM не переопределяют headless-многолинейный прототип.

## 3. Исправленные ошибки собственной спецификации

Это сохранение причин изменений v18, **не список новых runtime-багов**. D различает SQL-контрпримеры и недоопределённые переходы. Исполнение reference-кода было in-memory, single-connection SQLite 3.46.1, не Windows/SDK/многопоточный Rust.

| ID | Контрпример → действующее решение | Reference |
|---|---|---|
| H18-01 | Две Task одной Issue имеют разных owners → canonical `origin_key`. | [Origin](agent_swarm.spec-v18/examples/task-origin-deduplication.json) |
| H18-02 | Queued revision пережила revise/drain → повторная проверка на COMMIT `queued→sending`; RETURNING не COMMIT. | [Guard](agent_swarm.spec-v18/examples/dispatch-guard-races.json) |
| H18-03 | Incomplete report освободил target-dir при живых/неизвестных процессах → resource claim/release отдельно от verdict. | [Resource](agent_swarm.spec-v18/examples/check-resource-release.json) |
| H18-04 | Общий активный CheckRun имел единственного неоднозначного owner → active dedupe внутри Attempt; reuse готового результата связывается заново. | [Check dedupe](agent_swarm.spec-v18/examples/active-check-deduplication.json) |
| H18-05 | Max применён, затем заменён high до prompt → native per-turn options либо короткий prepare/admission barrier. | [Settings](agent_swarm.spec-v18/examples/settings-interleaving.json) |
| H18-06 | Использованная dependency acceptance отозвана → адресная revalidation; новая revision сама по себе не отменяет старую принятую. | [Dependency](agent_swarm.spec-v18/examples/dependency-revalidation.json) |
| H18-07 | Retention стёр request-id/evidence внутри JSON → компактные effect/idempotency records хранятся до явного архивирования; автоматом только disposable. | [Retention](agent_swarm.spec-v18/examples/retention-boundary.json) |
| H18-08 | Reconnect менял caller; один SID смешивал роли → stable principal и GM epoch, без обещания security tenant при full access. | [Caller](agent_swarm.spec-v18/examples/caller-reconnect.json) |
| H18-09 | Новый request ID повторил первоначальный prompt той же Attempt → один `start_operation_id`, correction отдельна. | [Dispatch](agent_swarm.spec-v18/examples/single-initial-dispatch.json) |

Ранее закрытые границы сохранены в текущих контрактах: native root collision через aliases; terminal до ACK; stale/partial snapshots; reader/reply без ожидания model turn; host-link disconnect не SDK.close; одной Task не мешают посторонние дети менеджера. Исходные [D — review v18][D] и [counterexamples][SQL] доступны в истории, текущие examples и DDL остаются в дереве.

## 4. Что удалено и где осталось существенное

| Прежние материалы | Куда перенесено |
|---|---|
| MANAGER-BRIEF.md и MANAGER-BRIEF(1).md | §1–2 этого файла; reported deployment — [runtime notes](runtime-notes.md). Старые команды/пути не install defaults. |
| brief-review-v15 | §1–2, runtime notes и уже действующие platform/forge/CheckSpec-контракты. |
| runtime-contract-audit-v16 | [Runtime notes](runtime-notes.md); полные идентификаторы источников и матрица сохранены отдельно. |
| Harness/OpenCode Go master rev5 и harness-intake | [Candidate notes](candidate-notes.md); нужные native-различия — runtime notes. |
| design-review-v18 и review-v18/ | §3 и текущие reference examples; предыдущие схемы, патчи и воспроизведения — Git history. |
| checkpoint, package manifest и прежний validation report | Текущая точка входа — README/implementation. Граница прежней проверки — [spec README](agent_swarm.spec-v18/README.md) и frozen history. |

Чистка меняет только документацию, ссылки и состав рабочего дерева. Runtime-контракты v18/v6/v2, source pins и текущие SQL/JSON/TOML-примеры не расширяются. Нет нового обязательного свода инструкций: этот справочник читается по соответствующему incident.

**История до очистки:** `b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62`. Исходник любого удалённого документа доступен по `git show <этот SHA>:docs/<прежний путь>`. История Git не переписывается. Это удаление из текущего дерева, не стирание ранее опубликованных сведений из истории.

[B]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/MANAGER-BRIEF.md
[B-old]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/MANAGER-BRIEF(1).md
[R]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/agent_swarm.brief-review-v15-20260929.md
[H]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/Harness_and_OpenCode_Go_master_2026-09-29_rev5.md
[D]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/agent_swarm.design-review-v18-20260929.md
[SQL]: https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/review-v18/sql-counterexamples.json
