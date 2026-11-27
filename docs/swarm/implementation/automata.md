# Контракты задачи и автоматы

> Источник фактов: целевая архитектура v2.6 (27.11.2026). Живой контракт; исходная версия сохранена в archive/spec-v2.6.md; «раздел N» и «приложение X» — номера v2.6, ссылка ↗ ведёт туда, где он теперь. Общая суть — [ARCHITECTURE.md](../ARCHITECTURE.md), связи — [GRAPH.md](../GRAPH.md), навигация — [INDEX.md](../INDEX.md).

**Зачем.** Точные форматы и переходы, на которые опираются контроллеры, runtime и интегратор. Это единственное место, где перечислены события Task, все переходы и причины остановок. Скрипт [check.py](../tools/check.py) читает таблицы прямо из этого файла.

**Владеет.** Формат task.md; строка Task в Store; таблица событий Task; переходы Task, Attempt, Delivery, IntegrationStep, Gate; жизненные циклы CheckRun и Workspace; закрытые списки видов Gate, вердиктов разбора и причин Stop; правила времени (монотонные часы).

**Вход и выход.** Сейчас это проект контракта будущего reducer в eliot-core. `check.py` проверяет только структуру Markdown. Генерация из Rust и сравнение со спецификацией ещё не реализованы (этап 1); сама генерация ожидаемой таблицы без отдельного сравнения не является проверкой.

**Инварианты.**
- Переходы со старым gen или старой ревизией отвергаются до таблицы; события в финальных состояниях — игнор с записью Event.
- Для пары (состояние, событие) guards должны быть взаимоисключающими и покрывать допустимые входы. Различие текста условий этого не доказывает.
- Живой Attempt — starting или running; не больше одного Runtime в running на Workspace.
- Виды Gate и причины Stop — контракт для будущего реестра reducer; текущий check.py не проверяет эти идентификаторы.

**Зависит от.** [core.md](core.md) (сущности). Читают: [task-controller.md](task-controller.md), [runtime.md](runtime.md), [integrator.md](integrator.md).

**Статус.** Структурная проверка — [сводка ниже](#проверка-автоматов). Исполнение guards и поведение продукта этим скриптом не проверяются.

---

## 14. Контракты

Здесь форматы и переходы, на которые ссылаются разделы 2–11. Имена полей — целевые; точные типы фиксируются в реестре на этапе 1.

### task.md

```
+++
id = "T-42"
rev = 5
goal = "G-7"
kind = "change"                     # change | report: ждём ли изменения кода
scope = ["crates/eliot-store/**"]   # пусто = сначала разведка только на чтение
checks = ["cargo clippy --locked -p eliot-store -- -D warnings"]
review = true                       # по политике проекта, можно выключить для задачи
depends = ["T-40"]
+++
# Исходный запрос
Слова владельца как есть.
# Цель
Что должно быть сделано и зачем.
# Критерии
- C1: проверяемое утверждение
- C2: ...
```

Критерии имеют id, чтобы ревьюер ставил вердикт по каждому. task.md — экспорт ревизии Task из Store в work-truth.git; копия в .swarm/task.md внутри worktree только для чтения агентом.

### Состояние задачи (строка Task в Store)

```toml
status = "running"   # ready | claimed | running | checking | review | queued | integrating | held | done | cancelled
rev = 5
kind = "change"               # change | report: что задача сдаёт
review = true                 # нужно ли ревью; по политике проекта, задача может переопределить
result = ""                   # последний принятый результат: change | no_change | report; сброс при новой работе
prev_status = "running"       # куда вернуться из held
held_reason = ""              # причина из списка Stop, если status = held
gate_wait = { kind = "", to = "", gen = 0 }   # переход, который ждёт решения Gate
pending = []                  # отложенные намерения во время шага интегратора: cancel | edit | budget
gen = 3                       # n текущего Attempt роли executor; не Agent.generation
parent = "T-7"                # родитель: цель или задача, которую разделили
children = []                 # дочерние задачи после split
depends = ["T-40"]
workflow_rev = 12             # последняя применённая ревизия workflow; правило смены в config.md
route = "codex/gpt-x/acct-1"
branch = "swarm/T-42-g3"
candidate = { rev = 5, gen = 3, base = "c3d4...", tree = "e5f6...", commit = "a1b2...", source = "local" } # gen = Task.gen / Attempt.n
hard = false
budget = { attempts_left = 2, wall_left_min = 180, usd_left = "unknown" }   # usd_left = число или "unknown" (подписка)
counters = { false_done = 0, review_rejects = 0, stop_blocks = 0, replacements = 0, no_progress = 0, silence = 0, restarts_10min = 0, cant = 0 }
merged_oid = ""
```

Время. Все сроки (TTL, дедлайны хода, wall-бюджет) считаются по монотонным часам процесса, который их держит; в Store пишется прошедшее время на момент записи, а не стенное время истечения. Скачок системных часов сроки не сдвигает. После рестарта демона срок продолжается от записанного остатка.

Группы состояний в таблице: АГЕНТ = claimed, running, checking, review, queued; РАБОЧИЕ = АГЕНТ и integrating; НЕФИНАЛЬНЫЕ = ready, РАБОЧИЕ и held. «=» — состояние не меняется (переход записывается как Stop или Event). «prev» — значение prev\_status. «игнор» — событие принято и записано в Event, состояние не меняется, причина в последней колонке. Строка с gate в режиме owner вместо целевого состояния ведёт в held(gate:\<вид>) и пишет gate\_wait; одобрение выполняет записанный переход.

### События Task

| Событие | Кто порождает | Где может прийти |
| --- | --- | --- |
| `claim` | планировщик | ready |
| `launch_ok` | runtime: харнесс начал первый ход | claimed |
| `launch_failed` | runtime или supervisor | claimed |
| `claim_ttl` | таймер claim\_ttl | claimed |
| `result_change` | агент: swarm done для kind = change | running |
| `result_no_change` | агент: swarm done --no-change с обоснованием | running |
| `result_report` | агент: swarm done для kind = report | running |
| `checks_red` | check runner или gitleaks | checking |
| `checks_green` | check runner | checking |
| `check_infra` | раннер упал или вышел check\_ttl | checking |
| `review_accept` | ревьюер | review |
| `review_reject` | ревьюер | review |
| `review_ttl` | таймер review\_ttl | review |
| `queue_turn` | очередь интегратора | queued |
| `step_merged` | интегратор: OID достижим из origin/main | integrating |
| `step_rejected` | интегратор: конфликт, красная проверка после одного повтора или policy_changed до sending | integrating |
| `step_lease_lost` | интегратор: fast-forward push отклонён, main сдвинулся | integrating |
| `step_cancelled` | интегратор: шаг был planned и отменён по pending | integrating |
| `step_unknown` | интегратор: исход неизвестен после сверки | integrating |
| `step_unexpected` | интегратор: ответ вне известных классов | integrating |
| `integrate_ttl` | таймер integrate\_ttl | integrating |
| `edit` | task.edit владельца или менеджера | НЕФИНАЛЬНЫЕ |
| `cancel` | swarm cancel владельца | НЕФИНАЛЬНЫЕ |
| `blocked` | агент: swarm blocked \<тип> | running |
| `budget_exhausted` | task controller | РАБОЧИЕ, held |
| `dep_cancelled` | task controller: зависимость cancelled | ready, held |
| `provider_hold` | supervisor: Hold на route, разрешённой замены нет | claimed, running |
| `runtime_died` | supervisor: runtime или харнесс умер, остановка подтверждена | АГЕНТ, integrating, held |
| `silence` | детектор | running |
| `no_progress` | детектор | running |
| `turn_deadline` | runtime или демон: дедлайн хода | running |
| `tool_deadline` | runtime или демон: дедлайн инструмента | running |
| `gate_approved` | владелец или auto | held |
| `gate_rejected` | владелец | held |
| `unblocked` | владелец или событие: причина blocked снята | held |
| `budget_extended` | Gate budget\_extend | held |
| `dep_resolved` | Gate dependency: зависимость убрана или задача поправлена | held |
| `hold_lifted` | supervisor: Hold снят | held |
| `verdict` | разборщик (Verdict kind = analysis) | held |
| `children_done` | task controller: все дочерние done | held |
| `child_cancelled` | task controller: дочерняя cancelled | held |
| `reconcile_reachable` | сверка: IntegrationStep.new_oid достижим из целевого remote ref | held |
| `reconcile_retryable` | сверка: разрешён повтор того же frozen шага по контракту интегратора | held |
| `infra_fixed` | владелец или проба раннера | held |

### Переходы Task

«После этого» означает после однократного учёта текущего события в той же транзакции Store: повтор id не увеличивает счётчик ещё раз. Возврат к работе снимает прежний result; новый принятый результат записывает его явно. Guards обязаны быть взаимоисключающими по значениям, не только по написанию.

| Из | Событие | Условие | В | Gate | Что пишется |
| --- | --- | --- | --- | --- | --- |
| ready | `claim` | все depends в done; область не пересекается с идущими; есть route без Hold и в пределах max\_live; attempts\_left > 0; обратное давление в норме | claimed | claim | одна транзакция: Task, Attempt n+1 в starting, admission запуска, workflow\_rev |
| claimed | `launch_ok` | — | running | — | Attempt → running |
| claimed | `launch_failed` | — | ready | — | Attempt → lost; n использован, attempts\_left не списан (хода не было); Hold на route только при ошибке класса route (auth, quota) |
| claimed | `claim_ttl` | — | ready | — | Attempt → stopping; runtime, поднявшийся позже, получает отказ по gen |
| running | `result_change` | есть новый коммит на ветке поколения | checking | — | result = change; Candidate {rev, gen, base = merge-base с origin/main, tree, commit}; пишет task controller |
| running | `result_change` | нового коммита нет; false\_done < 2 после этого | = | — | Stop false\_done, steer «коммита нет»; false\_done+1 |
| running | `result_change` | нового коммита нет; false\_done >= 2 после этого | held(analysis) | — | Stop false\_done; false\_done+1; Attempt → stopping |
| running | `result_no_change` | — | review | — | result = no\_change; Candidate снят; ревью обязательно при любом review |
| running | `result_report` | review = true | review | — | result = report; Candidate не создаётся; ревью читает отчёт |
| running | `result_report` | review = false | done | accept\_review | result = report; приёмка по gate |
| checking | `checks_red` | false\_done < 2 после этого | running | — | Stop false\_done; false\_done+1; агент получает вывод проверок без секретов |
| checking | `checks_red` | false\_done >= 2 после этого | held(analysis) | — | Stop false\_done, action = analysis; Attempt → stopping |
| checking | `checks_green` | review = true | review | — | CheckRun passed на (Task, rev, gen, base, tree) и замороженной конфигурации проверки; commit — происхождение |
| checking | `checks_green` | review = false | queued | — | то же |
| checking | `check_infra` | первая ошибка на кандидате | = | — | новый CheckRun |
| checking | `check_infra` | вторая ошибка | held(infra) | — | Stop infra; владелец получает уведомление |
| review | `review_accept` | kind = change и result = change | queued | accept\_review | Verdict review на этом кандидате |
| review | `review_accept` | result = report или result = no\_change | done | accept\_review | Verdict review |
| review | `review_reject` | review\_rejects < 3 после этого | running | — | Stop review\_rejected; замечания уходят агенту |
| review | `review_reject` | review\_rejects >= 3 после этого | held(analysis) | — | Stop review\_rejected, action = analysis; Attempt → stopping |
| review | `review_ttl` | — | = | — | другой ревьюер |
| queued | `queue_turn` | candidate.base = origin/main или merge-tree без конфликта и проверки результата зелёные | integrating | merge | IntegrationStep planned |
| integrating | `step_merged` | в pending нет cancel и edit | done | — | merged\_oid = IntegrationStep.new_oid |
| integrating | `step_merged` | в pending есть edit, нет cancel | done | — | merged\_oid; правка становится новой задачей с parent; владелец получает уведомление |
| integrating | `step_merged` | в pending есть cancel | done | — | merged\_oid; отмена не откатывает main; pending edit при cancel не создаёт новую задачу; уведомление |
| integrating | `step_rejected` | reason не policy_changed; в pending нет cancel и edit | running | — | причина агенту; Candidate снят |
| integrating | `step_rejected` | reason = policy_changed; planned-шаг отменён; в pending нет cancel и edit | checking | — | Candidate сохранён; пригодные CheckRun используются, недостающие допуски получаются заново |
| integrating | `step_rejected` | в pending есть cancel | cancelled | — | ветка в archive-ref |
| integrating | `step_rejected` | в pending есть edit (без cancel) | running | — | rev+1 |
| integrating | `step_lease_lost` | в pending нет cancel и edit | queued | — | в конец очереди; кандидат перепроверяется на новом main |
| integrating | `step_lease_lost` | в pending есть cancel | cancelled | — | ветка в archive-ref |
| integrating | `step_lease_lost` | в pending есть edit (без cancel) | running | — | rev+1 |
| integrating | `step_cancelled` | в pending есть cancel | cancelled | — | шаг до sending снят |
| integrating | `step_cancelled` | в pending есть edit, нет cancel | running | — | rev+1 |
| integrating | `step_unknown` | — | held(integration\_unknown) | — | задача выходит из очереди |
| integrating | `step_unexpected` | — | held(integration\_unexpected) | — | задача выходит из очереди |
| integrating | `integrate_ttl` | — | = | — | сверка шага с origin |
| ready, claimed, running | `edit` | правка не добавляет невыполненных depends | = | — | rev+1; допуски сняты; running получает команду revision.changed с ack и обязательной перечиткой task.md |
| claimed, running | `edit` | правка добавляет невыполненную зависимость | игнор | — | отказ клиенту: сначала остановить или дождаться; правка не применена |
| checking, review, queued | `edit` | — | running | — | rev+1; допуски Candidate/CheckRun сняты; открытые Gate superseded; проверки и ревью заново |
| integrating | `edit` | — | = | — | pending += edit; применяется после исхода шага |
| held | `edit` | held\_reason = blocked:unclear | prev | — | rev+1; если prev — checking, review или queued, то running |
| held | `edit` | held\_reason = gate:\<вид> | prev | — | rev+1; открытые Gate superseded; если prev — checking, review или queued, то running |
| held | `edit` | причина integration_*; шаг sending или unknown | = | — | pending += edit; ревизия шага сохраняется до его исхода |
| held | `edit` | другие причины, нет шага sending или unknown | = | — | rev+1; причина остаётся |
| ready, claimed, running, checking, review, queued | `cancel` | — | cancelled | — | Attempt → stopping (из starting тоже), ветка в archive-ref |
| integrating | `cancel` | — | = | — | pending += cancel; применяется после исхода шага |
| held | `cancel` | причина integration_*; шаг sending или unknown | = | — | pending += cancel; сверка продолжается |
| held | `cancel` | нет шага sending или unknown | cancelled | — | Attempt → stopping; архив после подтверждённой остановки |
| running | `blocked` | не «не могу» второй раз подряд | held(blocked:\<тип>) | — | Stop blocked:\<тип>; сессия сохраняется для resume |
| running | `blocked` | «не могу» второй раз подряд | held(analysis) | — | Stop blocked:cant, action = analysis; Attempt → stopping |
| claimed, running, checking, review, queued | `budget_exhausted` | — | held(budget) | — | Stop budget; для running ход доигрывается до конца, затем остановка с сохранением сессии |
| integrating | `budget_exhausted` | — | = | — | pending += budget; после исхода шага, если задача не в финале, — held(budget) |
| held | `budget_exhausted` | причина integration_*; шаг sending или unknown | = | — | pending += budget; сверка продолжается |
| held | `budget_exhausted` | нет шага sending или unknown | = | — | остаток бюджета обновлён; существующая причина удержания сохраняется; перед новой работой бюджет перепроверяется |
| ready | `dep_cancelled` | — | held(dependency) | — | Gate dependency владельцу |
| held | `dep_cancelled` | — | = | — | причина меняется на dependency, если была другая, prev сохраняется |
| claimed | `provider_hold` | — | ready | — | Attempt → stopping; n использован |
| running | `provider_hold` | — | held(provider) | — | ход прерывается, сессия сохраняется; другой аккаунт сам не выбирается (раздел 7 [↗](routing.md)) |
| claimed | `runtime_died` | — | ready | — | как launch\_failed |
| running | `runtime_died` | restarts\_10min < 3 после этого | = | — | resume той же сессии тем же Attempt (lost → running), новый Runtime |
| running | `runtime_died` | restarts\_10min >= 3 после этого | held(analysis) | — | Stop restart\_limit; route получает Hold cooldown |
| checking, review, queued, integrating, held | `runtime_died` | — | игнор | — | агент в этом состоянии не нужен; Attempt восстанавливается при возврате в running |
| running | `silence` | silence < 2 после этого | = | — | Stop silence, action = ping |
| running | `silence` | silence = 2 после этого | = | — | Stop silence, action = interrupt, затем resume |
| running | `silence` | silence >= 3 после этого | held(analysis) | — | Stop silence, action = analysis |
| running | `no_progress` | no\_progress < 2 после этого | = | — | Stop no\_progress, action = steer |
| running | `no_progress` | no\_progress >= 2 после этого | held(analysis) | — | Stop no\_progress, action = analysis |
| running | `turn_deadline` | — | = | — | Stop turn\_deadline, action = interrupt, затем resume с сообщением |
| running | `tool_deadline` | — | = | — | Stop tool\_deadline: группа процессов инструмента снимается, агент получает сообщение |
| held | `gate_approved` | held\_reason = gate:\<вид> | gate\_wait | — | выполняется записанный переход; для replace\_executor — gen+1 через протокол остановки |
| held | `gate_rejected` | вид claim | cancelled | — | причина владельца в Stop |
| held | `gate_rejected` | вид budget\_extend | held(budget) | — | цель сохранена; новое финансирование не выделено |
| held | `gate_rejected` | вид accept\_review или merge | running | — | причина агенту; Candidate снят |
| held | `gate_rejected` | вид replace\_executor | running | — | тот же gen, resume; причина в Stop |
| held | `gate_rejected` | вид analysis\_verdict | held(analysis) | — | вердикт отклонён, нужен другой разборщик |
| held | `unblocked` | held\_reason = blocked:\<тип> | prev | — | ответ владельца доставляется агенту |
| held | `budget_extended` | held\_reason = budget; есть одобрение владельца на конкретную добавку | prev | — | budget += одобренный объём ровно один раз по id операции |
| held | `dep_resolved` | held\_reason = dependency | ready | — | depends исправлены; если была правка — rev+1 |
| held | `hold_lifted` | held\_reason = provider | prev | — | resume на том же route |
| held | `infra_fixed` | held\_reason = infra | checking | — | новый CheckRun |
| held | `verdict` | held\_reason = analysis; Verdict = continue, edit, mark\_hard | running | analysis\_verdict | continue: steer с подсказкой; edit: rev+1; mark\_hard: hard = true |
| held | `verdict` | held\_reason = analysis; Verdict = replace или restart\_clean | running | replace\_executor | gen+1 через протокол остановки; restart\_clean — новая ветка от main, старая в archive-ref |
| held | `verdict` | held\_reason = analysis; Verdict = split | held(children) | analysis\_verdict | дочерние задачи с parent; Attempt → stopping |
| held | `verdict` | held\_reason = analysis; Verdict = cancel | cancelled | analysis\_verdict | ветка в archive-ref |
| held | `verdict` | held\_reason = integration\_\*; прежний шаг разрешён без эффекта; Verdict = requeue | queued | analysis\_verdict | действие по прочитанному origin |
| held | `children_done` | held\_reason = children | done | — | done родителя без собственного OID |
| held | `child_cancelled` | held\_reason = children | held(dependency) | — | Gate dependency: убрать дочернюю, поправить или отменить |
| held | `reconcile_reachable` | held\_reason = integration\_unknown | done | — | merged\_oid = IntegrationStep.new_oid; pending обрабатывается как при step_merged |
| held | `reconcile_retryable` | held\_reason = integration\_unknown; условия retry_same выполнены | integrating | — | тот же IntegrationStep и new_oid; новая сборка кандидата не создаётся |

После исхода IntegrationStep приоритет намерений: при успешной публикации — done с фактическим new_oid; при cancel уведомить без отката и без нового поручения из edit. Без cancel успешный edit создаёт новую Task. При доказанном отсутствии эффекта cancel предшествует edit; затем budget удерживает любой нефинальный результат, сохраняя в prev_status именно состояние после исхода шага. Пока шаг sending/unknown, pending не меняет его rev/gen/OID и не освобождает очередь. Одно лишь чтение старого ref не разрешает шаг.

Финальные состояния: done и cancelled. События, пришедшие в финальное состояние, — игнор с записью Event (запоздалые сигналы, старый gen). Переходы со старым gen или старой ревизией отвергаются до таблицы. hard — флаг после 3 замен: любой следующий Stop с action = analysis идёт к разборщику другой семьи моделей, это не продлевает бюджет; budget\_extend всегда требует владельца.

done ставят три пути, и других нет: интегратор по доказательству достижимости OID (kind = change), приёмка по gate accept\_review (kind = report и no\_change), task controller для родителя, у которого все дочерние done. Разборщик done не ставит.

Разбор всегда идёт через held(analysis): исполнитель останавливается до вердикта, поэтому в worktree во время разбора нет второго писателя.

Зависимости. depends проверяются на циклы при task.create и task.edit. Зависимость в held — просто ожидание. Правка задачи в done не допускается: продолжение — новая Task с parent.

Виды Gate — закрытый список: claim, accept\_review, merge, replace\_executor, budget\_extend, dependency, analysis\_verdict, cleanup, deploy. Добавление вида — правка контракта и соответствующих типов reducer при реализации.

Правило применения новой версии workflow едино и задано в [config.md](config.md#смена-workflow-раздел-14). Оно повторно проверяется до planned → sending, а не после внешнего эффекта.

Verdict → переход (одна таблица вместо двух словарей): continue, edit и mark\_hard — running с тем же gen; replace и restart\_clean — running с gen+1 через gate replace\_executor; split — held(children); cancel — cancelled; requeue — только для held(integration\_\*). Stop.action (ping, steer, interrupt, resume, analysis, hold) — это реакция детектора и runtime на событие, а не исход разбора.

### Жизненные циклы остальных сущностей

Живой Attempt — в состоянии starting или running. stopping не живой: он не мешает создать gen+1, но Workspace не отдаётся новому Attempt, пока старый не stopped или Workspace не ушёл в quarantine. Инвариант: не больше одного Runtime в running на Workspace.

`Task.gen` — номер (`n`) executor Attempt, используется в Task transition guard, ветке и Candidate. Он не равен локальному `Agent.generation`, которое относится к жизненному циклу существующего Agent и увеличивается при его запуске/resume ([`../../../crates/eliot-store/src/dispatch.rs`](../../../crates/eliot-store/src/dispatch.rs)). У каждого local Runtime есть неизменное `agent_id + agent_generation`, впервые записываемое в транзакции dispatch; возобновление того же Attempt создаёт новый Runtime и новую привязку. У remote Runtime есть `external_ref`, а локальной пары нет. Сигналы Agent валидируются по Runtime binding, сигналы Task/Candidate — по `Task.gen` и `rev`.

#### Переходы Attempt

| Из | Событие | Условие | В | Gate | Что пишется |
| --- | --- | --- | --- | --- | --- |
| starting | `launch_ok` | — | running | — | Runtime {kind; local: agent\_id, agent\_generation, pid/boot id; remote: external\_ref} |
| starting | `launch_failed` | — | lost | — | причина запуска |
| starting | `stop_requested` | — | stopping | — | отмена во время запуска; runtime, поднявшийся позже, получает отказ по gen |
| running | `runtime_restarted` | — | = | — | новый Runtime того же Attempt после resume |
| running | `stop_requested` | — | stopping | — | отзыв разрешения записан до запроса |
| running | `runtime_unreachable` | — | lost | — | local: runtime не отвечает; remote: опрос провайдера не удаётся N раз |
| stopping | `stop_confirmed` | — | stopped | — | ход закончен, процессы вышли, поток дочитан; remote: провайдер вернул терминальное состояние |
| stopping | `stop_timeout` | — | lost | — | Workspace → quarantine; remote без отмены — сессия помечена orphan |
| lost | `resumed` | старые процессы подтверждённо вышли, сессия та же, Task.gen не отозван | running | — | новый Runtime того же Attempt/n; local Agent binding отражает его тогдашний Agent.generation |
| lost | `confirmed_gone` | — | stopped | — | сверка: процессов нет (local) или провайдер вернул терминальное состояние (remote) |

#### Переходы Delivery

| Из | Событие | Условие | В | Gate | Что пишется |
| --- | --- | --- | --- | --- | --- |
| stored | `pick` | — | sending | — | delivery_id и неизменная привязка к runtime_id; sending с fsync до записи в харнесс |
| stored | `stale` | старый gen или ревизия | rejected | — | — |
| stored | `deadline` | у сообщения есть deadline, и он прошёл | expired | — | — |
| sending | `harness_ack` | ACK сопоставлен с delivery_id и Runtime, начавшим эту доставку | applied | — | id хода или ответ RPC; для облака — «провайдер принял» |
| sending | `ack_lost` | — | unknown | — | — |
| sending | `runtime_restarted` | — | unknown | — | после рестарта нельзя знать, ушла ли запись; повтор — новый delivery_id с тем же Envelope.id; старый unknown сохраняется; только если протокол идемпотентен по Envelope.id |
| unknown | `reconciled_applied` | доверенная сверка нашла точное подтверждение исходной delivery_id/runtime/native binding | applied | — | исход подтверждён; identity записи сохраняется; новая Delivery не затрагивается |

Поздний ACK старого Runtime не применяется к новой Delivery с тем же Envelope id: отправляющий Runtime определяется из сохранённой привязки и канала, а не из произвольного поля агента. Unknown остаётся неизвестным до сверки; это не разрешение повторить неидемпотентный эффект.

Ephemeral-сообщения (прогресс, подсказки) в Delivery не входят: они живут в памяти runtime, при переполнении отбрасываются, в Store не пишутся.

#### Переходы IntegrationStep

| Из | Событие | Условие | В | Gate | Что пишется |
| --- | --- | --- | --- | --- | --- |
| planned | `send` | актуальные Task/rev/gen, Candidate и допуски по применённой workflow; нет pending cancel/edit | sending | — | маркер sending с fsync до сетевого вызова |
| planned | `pending_cancel` | в Task pending есть cancel или edit | cancelled | — | шаг не начат |
| planned | `policy_changed` | прежних допусков недостаточно; нет pending cancel/edit | cancelled | — | эффект не отправлен; Task получает step_rejected с reason = policy_changed |
| sending | `ok` | — | done | — | новый OID |
| sending | `rejected_known` | конфликт, красная проверка, lease\_lost | failed | — | класс исхода |
| sending | `no_answer` | — | unknown | — | — |
| unknown | `origin_reachable` | — | done | — | OID достижим из origin/main |
| unknown | `origin_at_old` | ref на ожидаемом старом OID | = | — | эффект пока не наблюдён; это не доказательство отказа старого отправителя |
| unknown | `retry_same` | local sender завершён; remote прочитан; ref = expected_old_oid; нет pending cancel/edit и отзыва допуска; тот же frozen new_oid и endpoint | sending | — | повтор того же шага; для main только fast-forward; неизвестный исход снова unknown |
| unknown | `remote_unreadable` | — | = | — | очередь стоит |

Классы исхода шага и переход Task: ok — step\_merged; conflict и red — step\_rejected; lease\_lost — step\_lease\_lost; auth и network без ответа — step\_unknown после сверки; всё прочее — step\_unexpected.

#### Переходы Gate

| Из | Событие | Условие | В | Gate | Что пишется |
| --- | --- | --- | --- | --- | --- |
| open | `approve` | — | approved | — | кто решил: auto или владелец |
| open | `reject` | — | rejected | — | причина |
| open | `superseded` | новая ревизия, кандидат, gen или затронутая политика workflow | superseded | — | — |

#### Состояния CheckRun и Workspace

| Сущность | Состояния | Переходы | Финальные |
| --- | --- | --- | --- |
| CheckRun | queued, running, passed, failed, error | error (раннер упал или вышел check\_ttl) даёт один новый CheckRun; failed после одного повтора — красная проверка; source = local или ci | passed, failed, error |
| Workspace | active, quarantine, archived, removed | active → archived (archive-ref) → removed; active → quarantine (остановка не подтверждена); quarantine → archived после сверки; kind = local (worktree) или remote (ветка провайдера, заморожена по OID) | removed |

### Проверка автоматов

Скрипт [check.py](../tools/check.py) читает таблицы этого файла. Для Task каталог событий сравнивается с переходами, для остальных автоматов проверяется только структура таблицы. Проверки: достижимость каждого состояния; выход из каждого нефинального; отсутствие переходов из финальных; путь к финалу из любого состояния; полнота — каждое событие из таблицы событий в каждом состоянии, где оно может прийти, имеет строку перехода или игнор; простые повторы guards — сравнение строк и соседство безусловной строки с условной (не доказательство детерминизма); переходы через Gate и prev разворачиваются в рёбра held. Результат на 27.11:

| Автомат | Состояний | Строк | Проблем |
| --- | --- | --- | --- |
| Task | 10 | 90 | 0 |
| Attempt | 5 | 10 | 0 |
| Delivery | 6 | 7 | 0 |
| IntegrationStep | 6 | 10 | 0 |
| Gate | 4 | 3 | 0 |

Границы: каталог событий и объявленные состояния сверяются отдельно только у Task. У четырёх остальных автоматов события заданы строками переходов, поэтому опечатка не сравнивается с производителем события. Смысл guards и Rust-поведение не проверяются. Этап 1 должен добавить экспорт из reducer и сравнение с контрактом; до этого «0» означает только отсутствие обнаруженных структурных проблем. Дубли таблиц теперь завершают проверку ошибкой.

### Stop (причина остановки)

```toml
id = "S-901"
task = "T-42"
attempt = 3
reason = "false_done"  # закрытый список ниже
value = 2
action = "analysis"    # ping | steer | interrupt | resume | analysis | hold
origin = "detector"    # detector | agent | owner | runtime | integrator | reconcile | provider
evidence = "красные проверки после swarm done, кандидат a1b2"
stderr_tail = ""       # последние 4 КиБ stderr харнесса, если причина в процессе
```

| reason | Кто ставит | action | Переход Task |
| --- | --- | --- | --- |
| false\_done | task controller | steer; на 2-й раз analysis | running или held(analysis) |
| review\_rejected | ревью | steer; на 3-й раз analysis | running или held(analysis) |
| no\_progress | детектор | steer; на 2-й раз analysis | = или held(analysis) |
| silence | детектор | ping, interrupt, на 3-й раз analysis | = или held(analysis) |
| turn\_deadline | runtime или демон | interrupt, resume | = |
| tool\_deadline | runtime или демон | interrupt инструмента | = |
| harness\_died | supervisor | resume | = (тот же gen) |
| restart\_limit | supervisor: 3 падения за 10 минут | analysis | held(analysis) |
| blocked:\<тип> | агент; blocked:permission и blocked:owner\_stop — Swarm | hold | held(blocked:\<тип>) |
| budget | task controller | hold | held(budget) |
| dependency | task controller | hold | held(dependency) |
| provider | supervisor: Hold без разрешённой замены; или текстовая подсказка | hold | held(provider) |
| infra | check runner: две ошибки раннера | hold | held(infra) |
| gate:\<вид> | task controller | hold | held(gate:\<вид>) |
| children | task controller после split | hold | held(children) |
| integration\_unknown | интегратор | hold | held(integration\_unknown) |
| integration\_unexpected | интегратор | analysis | held(integration\_unexpected) |
| hook\_timeout | runtime | — | =, только запись |

Названия действий: ping — короткое сообщение «ты здесь?» без смены задачи; steer — подсказка в текущий или следующий ход; interrupt — прервать ход, сессия остаётся; resume — продолжить сессию; analysis — остановить исполнителя и поставить задачу разборщику; hold — ждать снятия причины.
