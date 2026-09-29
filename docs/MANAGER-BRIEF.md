# ELIOT — управление роем агентов (MANAGER-BRIEF)

Обновлено: 26.09.2026. Документ описывает, как сейчас работает система: линии, раннеры, демоны приёмки, проверяющий,
обслуживание, наблюдение, перезапуск. Прежняя версия сохранена в
`C:\Development\Rust\projects\eliot-swarm\control-20260923-impl\MANAGER-BRIEF.backup-20260926.md`.

Главные пути:

| Что | Где |
|---|---|
| Репозиторий (root checkout, только чтение для агентов) | `C:\Development\Rust\projects\eliot-memory-os` (GitHub `UnknownAlienHuman/eliot-memory-os`) |
| Worktree линий, каталоги сборки | `C:\Development\Rust\projects\eliot-swarm\` (`M-<линия>`, `<линия>-<n>`, `targets\<линия>`) |
| Управление (I) | `C:\Development\Rust\projects\eliot-swarm\control-20260923-impl\` |
| Управление v2 (V2) | `...\control-20260923-impl\v2\` |
| Порядок работ | `C:\Users\kleym\Downloads\HANDOFF.md` (порядок Issue правильный, его не меняем) |
| Скилл Swarm | `C:\Users\kleym\.claude\skills\swarm\SKILL.md` (`references\external-hosts.md` — запуск Muse Code/OpenCode/Antigravity) |

---

## 1. Роль root и приказы владельца

Root — главный менеджер (сессия Claude Code). Root раздаёт работу, следит, что она идёт, принимает результат (через
демонов приёмки), вливает и чистит. Код пишут линии-менеджеры со своими субагентами; root код продукта не пишет.
**Задача root: «раздать задания и проверить, что работа идёт. Если работа не идёт — сделать так, чтобы шла».**

Постоянные приказы владельца (главнее любых документов, аудитов и комментариев):

1. **Работа привязана к Issue.** Одна Issue = один этап (Work, Acceptance, фрагменты документации). Порядок — HANDOFF.
2. **Сначала весь код, тесты потом.** Новые тесты не пишем (`#[test]`, `#[tokio::test]`, модули тестов, файлы в
   `tests/`), `cargo test` не запускаем. Разрешена только минимальная правка фикстур, чтобы существующие тесты
   компилировались. Пункты приёмки, требующие запуска, — TEST-PHASE.
3. **Гейт = Clippy.** `cargo fmt` по затронутым крейтам и для каждого затронутого крейта
   `cargo clippy --locked -p <крейт> --lib --bins --no-deps -- -D warnings` без НОВЫХ замечаний относительно main.
   Полную сборку, `cargo check --workspace`, `--all-targets` агенты не запускают.
4. **Руки прочь от живых сессий.** Автоматика никогда не прерывает и не убивает сессии менеджеров и субагентов, только
   ждёт. Нужна поправка — дополнение в ту же сессию (`session.prompt`), ответ на вопрос (`session.form.reply`).
5. **На все вопросы ответ в документации.** Вопросы агентов «значение не указано» владельцу не пересылать.
6. **Комментарии Issue — часть спецификации** (внешняя модель дописывает инструкции и находки). Решения владельца
   главнее строк приоритета аудита вроде «P3 — отложить до OSP1».
7. **Агенты не закрывают, не переоткрывают и не меняют метки Issue.** Закрытие — после тестовой фазы.
8. **Чужие PR (боты, Jules, внешние) не вливать.** Root вливает только PR своей приёмки.
9. **Диск:** один `CARGO_TARGET_DIR` на линию (`eliot-swarm\targets\<линия>`, максимум ещё `<линия>-2`),
   `CARGO_INCREMENTAL=0`, никаких `--target-dir` и каталогов сборки в `%TEMP%`/`C:\Temp`.
10. **429 «Rate limit exceeded» — это частота запросов, не квота:** ждать час. Квота кончилась только при
    «usage limit/quota exceeded/insufficient_quota/Individual quota».
11. **Проверки не чаще раза в 15 минут**; отчёт root — раз в 30 минут.
12. **WAL codebase-memory не трогать** («Ничего не делать»). `.swarm\`, `AGENTS.md` root checkout не трогать.
    `git gc`/`git prune`, `git stash` запрещены (`refs/stash` общий для всех worktree).

Решения владельца по продукту и процессу:

| Дата | Решение |
|---|---|
| 24.09 | Приоритет — порядок HANDOFF; линии — менеджеры с субагентами; каждой линии свои задачи |
| 25.09 | Аудит PR #2505: «сначала код, тесты потом» остаётся; весь HANDOFF (без заморозки D2–D5); вливание #2501–#2505 |
| 25.09 | Вход OSP1 = новый стек: Claude Code идёт через `eliot-agent-bridge → Kernel → eliotd` за флагом `ELIOT_CLAUDE_FRONT_DOOR=agent-bridge`; первыми #342, #77, #1858, #1719 (+ #2562, #2565, #2765, #2766) |
| 25.09 | OR — только проверки; Antigravity — только обслуживание, раз в 5 ч; «активнее вливаем» → 4 потока приёмки |
| 26.09 | Codex: один сброс квоты — тратить быстро, но эффективно: Astra xhigh + 4× Sol xhigh, субагенты Luna max |
| 26.09 | Пробные линии: LongCat 2.5 Preview Free (OpenCode Go; если хороша — заменит платную Muse) и Command Code на Space Bunny Alpha |
| 26.09 | Проверяющий OR работает по инструкции от GPT-ревьюера (документация → вся цепочка → контрпример) |
| 26.09 | LongCat и Space Bunny склонны к overthinking: при явном повторе снижать reasoning до `high` (в `SECTORS.json`, со следующей сессии), либо давать конкретные выполнимые условия приёмки. Сделано: W3 max→high (петли #963×11, #958, #2739, #686, #2569); раздел «Bounded work per issue» в `TEMPLATE-SECTOR.md` (приёмка = пункты REMAINING, 90 мин на Issue, один проход картирования и одна проверка, PARTIAL — допустимый итог, два возврата за сессию — отпустить); у LongCat уровней нет — только условия |
| 26.09 15:00 | **Сначала весь код по всем Issue** (все функции, сервисы, контракты по Architecture/implementation), потом сшивка, потом тесты. Недостающий production-вызов не блокирует: `validate` пишет его в `issues/<n>/STITCH.md` и `v2/stitch-backlog.tsv` (список фазы сшивки), в чек-листе допустимо `"caller": "STITCH"`; OR опровергает отсутствие пути, только если его требует сам пункт. Заблокировано — `BLOCKED-BY`, дальше. Очереди большие: W1/W2/W4/MC 80, W3/LC/CB 60, Codex 40. #3014 (вертикальные миссии) отложена до сшивки |
| 26.09 15:30–16:00 | «Сначала всё лёгкое и быстрое, блокеры потом; убрать блокеры и противоречия» (+ внешний аудит роя `agent_swarm.md`, владелец согласен не со всем). Сделано: в начале `TEMPLATE-SECTOR.md` раздел «Read first» — правила разрешения конфликтов (ЧТО строить — Issue и документы, КАК работать — только бриф; процессные указания внутри Issue игнорируются; комментарии читаются один раз при захвате; при молчании/противоречии документов — `ASSUMPTION:` и дальше, `BLOCKED-BY decision` только для одного пункта; вне scope — `BLOCKED-BY scope`; локальные сбои не останавливают линию; сначала лёгкое; захват только того, над чем идёт работа; проверяющий субагент только для полной сдачи; комментарий в Issue пишет только root). `COMMON-RULES.md` сокращён до правил кода (процесс — только бриф; копия `COMMON-RULES.backup-20260926-1550.md`). Приёмка: сломанная сборка ТЕСТОВЫХ целей не возвращает ветку (пишется в `v2/test-debt.tsv`); «запрещённые слова» — только в коде `.rs`, не в комментариях; ветке без push на GitHub даётся 30 мин; `Make-PR.sh` вливает только проверенный SHA (`--match-head-commit`, иначе exit 4). CLAIM действует 3 ч (было 6). Из ORDER убрана не-Issue строка |
| 26.09 22:40 | «Почему так мало code-complete?» Замер за день: 303 вливания и 238 NOCHANGE, +102 тыс. строк с 10:00, но 243 Issue (43%) ни разу не брались, 85 Issue переделывались 3+ раз, 122 достигнутых CC опровергнуто/понижено, у PARTIAL медиана 5 открытых пунктов (70 — не больше 2). Сделано: `sector-queue` упорядочивает очередь по группам (порядок HANDOFF внутри группы): блокеры → «almost done» (1–2 пункта) → «never started» → остальное; колонка `rem=`; Issue с `noprogress>=2` обычным линиям не предлагается (строка «Not offered … two deliveries in a row without progress») |
| 26.09 22:50 | «Сам внимательнее проверяй работу агентов; OR ловит расхождения с документацией и косяки, без дрочева и колхоза; не нужна криптография на каждом участке и 10000 лимитов». Замер: после 15:00 OR подтвердил 26 из 36 вердиктов (до — 18 из 62), перепроверенные опровержения верны (#1108, #958); плотность хешей/квитанций в коде идёт в основном из спецификаций Issue; лимиты разумные (256 КБ, 64 КБ). Сделано: в `TEMPLATE-CCV.md` раздел «What you catch, and what you must NOT demand» (опровержение только с цитатой нарушенного предложения; без лишних подписей/MAC/хешей/квитанций/fence/лимитов/таймаутов; простейшая буквальная реализация = CONFIRMED), в `TEMPLATE-SECTOR.md` правило 9 «Simplest correct code». Root в каждом отчёте сам читает 2 влитых дифа и перепроверяет 2 опровержения OR |
| 26.09 23:00 | **«Делаем согласно документации. Новые гениальные идеи будем внедрять после тестов.»** Строим ровно то, что требует документация (включая её квитанции/хеши/fence), ничего сверх. Новые Issue внешнего агента: если чинят/реализуют документированный контракт — в очередь после родителя; если предлагают новый механизм/процесс/редизайн, которого нет в документации, — в очередь не ставить (как #3014), отметить «после тестов» |
| 26.09 23:10 | «Сам периодически смотри, что пишут агенты, без масштабных аудитов; можно субагентом; оптимизируй демона и проверяющего». Проверка кода: root каждые ~30 мин запускает фонового субагента (Agent, general-purpose, sonnet, только чтение) на 3–4 свежих вливания: соответствие Issue/документации, баги, заглушки, лишние механизмы; ответ ≤25 строк. Демон: `Review-Branch2.sh` собирает рабочее пространство только lib+bin (тестовые цели не проверяются), main для сравнения собирается лишь когда результат слияния не собирается (кеш `ws2-<sha>`), clippy/fmt на main — только для пакетов с замечаниями в ветке; ненулевой код `cargo check` без «could not compile» = поломка (fail closed). OR: очередь 10 Issue на сессию (было 6); в `TEMPLATE-CCV.md` раздел Budget — перепроверять только ранее опровергнутые пункты и пункты с изменёнными файлами, TEST-PHASE — только наличие кода, ~10 мин на Issue |
| 27.09 08:45 | **Codex: оставить 2 линии Sol xhigh + субагенты Luna max (CS1, CS2), остальные передают работу.** CX (Astra), CS3, CS4 — `active: false` в `SECTORS.json`: текущая сессия доделывается (прервать/написать в идущую сессию Codex нельзя), затем раннер выходит; их сектора становятся общими. CS2 получила `escalated_first` вместо CX |
| 27.09 08:55 | **Приёмка — 4 потока (слоты 1–4); при росте очереди увеличивать.** Слоты 5 и 6 остановлены `v2\Stop-DaemonV3-Safely.ps1 -Match "REVIEW_SLOT=[56] "` (в безопасной точке), их захваты в `reviews\claims` сняты. Правило: если в двух отчётах подряд в очереди приёмки больше 10 сдач — снова запустить слот 5 (и 6), каталоги сборки `targets\ROOT-review5/6`, `ROOT-main5/6` сохранены |

---

## 2. Карта файлов управления

```text
control-20260923-impl\
  COMMON-RULES.md            общие правила исполнителей (Issue-bound, без тестов, гейт clippy)
  Launch-Manager.ps1         запуск OpenCode-сессии (opencode run --auto)
  Review-Branch2.sh          проверка результата вливания ветки (используется демоном v3)
  Make-PR.sh                 PR по шаблону → squash → журнал → удаление ветки → комментарий в Issue
  merged-ledger.tsv          журнал вливаний: время, Issue, PR, sha main, ветка, head
  reviews\<ветка>.txt        отчёт проверки ветки; reviews\cache\ (результаты main по SHA); reviews\claims\<n>\ (захват)
  cleanup\                   обслуживание: Run-Maintenance.ps1, MAINT-BRIEF.md, Fold-ArchiveWip.sh, ledger.tsv, runs\
  v2\
    ORDER.txt                единая очередь в порядке HANDOFF: "<n> | after: <зависимости> | <заметки root>"
    state.json               состояние каждой Issue (state, attempts, merged, noprogress, remaining, continue_branch, history)
    state.py                 машина состояний и все проверки (команды — раздел 5)
    SECTORS.json             линии: движок, модель, уровни, сектора, длина очереди, active, роль
    sectors.py               начальное распределение Issue по секторам (по разделу HANDOFF и префиксу заголовка)
    insert_after.py          вставка новых Issue сразу после родительской с её сектором
    TEMPLATE-SECTOR.md       бриф менеджера линии (раздел 6)
    TEMPLATE-CCV.md          бриф проверяющего OR (раздел 9)
    Run-Sector.ps1           раннер линии (раздел 7)
    Launch-MuseCode.ps1 / Launch-Codex.ps1 / Launch-CommandCode.ps1   лаунчеры синхронных движков
    Merge-Daemon-v5.sh       демон приёмки, 10 потоков (раздел 8; v3/v4 — прежние версии)
    apply_ccv.py             применение вердиктов OR + перенос проверок при исчерпании квоты OR
    ingest_pr_review.py      находки внешнего ревью из тела PR владельца (#2492)
    Answer-Forms.ps1         ответы на вопросы-формы в сессиях без оператора
    Lane-Report.sh / Lane-Status.ps1   30-минутный отчёт (только чтение)
    issues\<n>\              CLAIM, PUSHED, NOCHANGE, CHECKLIST.json, CHECKLIST.prev.json, REMAINING.md, REPORT.md,
                             CCV.json, REFUTED-CHECKLISTS.txt, PR-REPORT.md — служебные файлы Issue (в Git не коммитятся)
    workers\<линия>\         sector.log (раннер), STATUS.md (журнал менеджера), SECTOR-BRIEF-*.md, logs\*.stdout.jsonl, current.pid
    daemon.log               журнал демонов приёмки; ccv-applied.log, pr2492-ingest.log, forms-answered.log
```

---

## 3. Линии

Состав по приказу владельца 28.09 10:55: **3 линии OpenCode Go, 1 просто OpenCode, 1 OpenRouter, 1 Command Code,
1 Muse Code и Codex.** Каждая держит 4 субагента ВСЕГДА (Muse Code — 8), менеджер сам проверяет работу.

| Линия | Движок и модель | Сектора / роль | Статус |
|---|---|---|---|
| W1 | OpenCode Go `opencode-go/muse-spark-1.3-contributor#xhigh` (платная Go) | AGENTS, PLATFORM | работает |
| W2 | OpenCode Go `opencode-go/space-bunny-free#high` | COGNITION, BUILD | работает |
| W4 | OpenCode Go `opencode-go/space-bunny-free#high` | PLATFORM, STORAGE | работает |
| W3 | OpenCode `opencode/space-bunny-free#high` | STORAGE | работает |
| OR | OpenRouter `openrouter/stealth/space-bunny-alpha#max` | только проверки (`verify_only`) | работает |
| CB | Command Code CLI `stealth/space-bunny-alpha` | BUILD, COGNITION | работает |
| MC | Meta Muse Code CLI `muse-spark-1.3-contributor` max, 8 субагентов | BUILD, AGENTS | работает |
| CS1 | Codex `gpt-6-sol` xhigh + `gpt-6-luna` max, `profile: hard` | ESCALATED, AGENTS, COGNITION | работает |
| CS2 | Codex `gpt-6-sol` xhigh + `gpt-6-luna` max, `profile: hard` | ESCALATED, PLATFORM, STORAGE | работает |
| CL1 | Codex `gpt-6-luna` max менеджер + Luna max субагенты, `profile: hard` (проба) | ESCALATED, BUILD, COGNITION | работает |
| LC | была LongCat, затем Space Bunny | — | выключена 28.09 10:55 (дорабатывает сессию) |
| CX, CS3, CS4 | Codex Astra / Sol | — | выключены 27.09 08:45 |
| AG | Antigravity `agy` `gemini-3.8-flash-high` | обслуживание раз в 5 ч (раздел 11) | — |

- Сектора пересекаются намеренно: двойную работу исключает `CLAIM` (раздел 5). У каждой линии своя голова очереди.
- Модель, уровни, сектора и длина очереди меняются в `SECTORS.json` и действуют со следующей сессии линии (раннер
  перечитывает файл перед каждой сессией). `active=false` — линия останавливается после текущей сессии.
- Уровни рассуждения всегда явно: без `#вариант` OpenCode запускает `default` (поверхностная работа). Варианты модели:
  `opencode api model.list` → `variants`. Muse в OpenCode — максимум `xhigh`; в Muse Code — `max` (есть и `ultra`).
  Codex (`~/.codex/models_cache.json`): Astra/Sol — `low…max` + `ultra` (max с авто-делегированием); у Luna `ultra` нет.
- В глобальном конфиге Codex `[agents] max_threads = 4` (не больше 4 субагентов на менеджера); не менять без владельца.

---

## 4. Движки: запуск, живость, особенности

Все запуски делает лаунчер линии (раннер вызывает его сам); ручной запуск нужен только для пробы.

**OpenCode 2** (`opencode.exe` 2.0.x, фоновый сервис `opencode-cli.exe serve --service` запускается десктоп-приложением):

```powershell
$oc = 'C:\Users\kleym\AppData\Roaming\npm\node_modules\@opencode\cli\bin\opencode.exe'
# запуск (из worktree репозитория, иначе сессия уйдёт в чужой проект)
& $oc run --auto --model opencode-go/space-bunny-free#max --format json --title W2-sector "Read the brief file <путь> ..."
# НЕ вызывать `& $oc api ...` из автоматики root (29.09): каждый запуск CLI проверяет живость сервиса и под нагрузкой
# перезапускает его — гибнут все субагенты. Вместо этого:
python v2\oc_http.py active                                          # живые сессии (GET /api/session/active)
python v2\oc_http.py prompt <sessionID> "<одна строка>"              # дополнение (POST /api/session/<id>/prompt)
python v2\oc_busy.py <sessionID>                                     # busy/idle по базе (сессия + субагенты)
# сообщения, субагенты, время — читать из ~\.local\share\opencode\opencode.db (только чтение): session_v2, session_message
```

- Клиент `opencode run` может выйти (`{"type":"error","message":"Transport"}` или `Session interrupted: shutdown`), а
  сессия продолжает работать в сервисе. Линия занята, пока активна сессия менеджера **или любая её дочерняя сессия**
  (`parentID`). У менеджера `time.updated` не меняется, пока работают субагенты.
- `--auto` обязателен (иначе ждёт подтверждения). Первая строка stdout содержит `sessionID`.

**Meta Muse Code** (`C:\Tools\MuseCode\muse.cmd`, синхронный):

```powershell
& 'C:\Tools\MuseCode\muse.cmd' exec --json --yolo --no-foreign-personal-context --workspace <M-MC> `
  --model muse-spark-1.3-contributor --reasoning-effort max --prompt-file <brief.muse.md>
```

- `--yolo` = без подтверждений, без песочницы, доверенный workspace (без него нет субагентов `muse.subagent_*`).
- stdout буферизуется; живой журнал — `%USERPROFILE%\.local\share\muse\sessions\<дата>\<id>\session.jsonl`
  (ищется по `M-MC` в начале). Живость — процесс из `workers\MC\current.pid`.
- Пишет файлы через Windows PowerShell 5 — возможен BOM (раздел 8 снимает).

**Codex** (самый новый `%LOCALAPPDATA%\OpenAI\Codex\bin\<hash>\codex.exe` — приложение обновляет CLI в новый каталог):

```powershell
codex exec -m gpt-6-sol -c model_reasoning_effort="xhigh" -c agents.default_subagent_model="gpt-6-luna" `
  -c agents.default_subagent_reasoning_effort="max" --enable multi_agent --disable browser_use --disable computer_use `
  --disable image_generation --dangerously-bypass-approvals-and-sandbox --json -C <M-CSn> -o <last.md> "<сообщение>"
```

- Синхронный; stdin перенаправлен из пустого файла. Дополнений в идущую сессию нет — влиять только брифом следующей.
- Квота: событие `{"type":"error","message":"You've hit your usage limit ... try again at ..."}`.

**Command Code** (`command-code.cmd` 1.66, npm; всегда `--no-auto-update`, иначе CLI обновляется посреди работы):

```powershell
command-code -p "<сообщение>" --model stealth/space-bunny-alpha --yolo --trust --no-auto-update --tools-all `
  --max-turns 3000 --output-format json -n <имя>
```

- Субагенты: инструмент `agent` (`subagent_type` general/explore/plan, `run_in_background`), результаты — `agent_output`.
- Последняя строка NDJSON — `{"type":"result","subtype":"success|error",...}`; выход 8 — исчерпан `--max-turns`.
- Модели: `command-code --no-auto-update --list-models`.

**Antigravity** (`C:\Users\kleym\AppData\Local\agy\bin\agy.exe`): `agy -p "<сообщение>" --model gemini-3.8-flash-high
--effort high --dangerously-skip-permissions --add-dir <каталог> … --output-format stream-json --print-timeout 240m`.

---

## 5. Очередь, состояние, захват

**Порядок** — `ORDER.txt`: разделы §0 (вход OSP1), §1–§3 HANDOFF, §5 — все прочие открытые Issue по возрастанию.
Новые Issue внешнего агента ставятся сразу после родительской:

```bash
cd v2 && python insert_after.py <родитель> "<n> | after: - | external issue <дата> (parent #<родитель> ...)"
```

`insert_after.py` вставляет строку, запускает `state.py init` и копирует сектор родителя. Родителя ищем по самым частым
ссылкам `#NNNN` в теле Issue. Закрытую владельцем/внешним агентом Issue убрать: `python state.py set <n> CLOSED`.

**Состояния** (`state.json`): `TODO → (IN-PROGRESS) → PUSHED/NOCHANGE → проверка → PARTIAL | CODE-COMPLETE | ESCALATED`;
`CLOSED` — закрыта на GitHub. Зависимость `after:` выполнена, если производитель CODE-COMPLETE/CLOSED или влит хотя бы
раз (`merged ≥ 1`).

**Команды `state.py`** (запускать из `v2`):

| Команда | Что делает |
|---|---|
| `init` | добавляет в state.json новые строки ORDER.txt |
| `sector-queue <линия>` | очередь линии (таблица для брифа). С 26.09 14:40: Issue, чей последний REPORT.md говорит `BLOCKED-BY #x`, не предлагается, пока в #x не влит новый код (или #x не code-complete); её блокер из секторов линии ставится первым с пометкой `(blocker of #n)`; под таблицей строка «Not offered …» (W3 брала те же 4 заблокированные Issue в каждой сессии) |
| `cc-queue` | CODE-COMPLETE, у которых CHECKLIST.json новее CCV.json (до 6) — очередь проверяющего |
| `review-list2` | что ждёт приёмки: строки `n|NOCHANGE|-` (первыми), затем `n|PUSHED|ветка@sha` |
| `nochange <n>` / `merged <n> <pull/N>` | проверка чек-листа после «всё уже на main» / после вливания (`finish_review`) |
| `hold <n> <ветка> <причина>` | возврат исполнителю с причиной в REMAINING.md |
| `set <n> <state>`, `summary`, `validate <n>` | ручная правка, сводка, проверка чек-листа без смены состояния |

**Как строится очередь линии** (`cmd_sector_queue`): список секторов = `*ESCALATED*` (если `escalated_first`) + свои
сектора + `fallback` + сектора неактивных линий. По ORDER.txt берутся Issue в состоянии TODO/PARTIAL/ESCALATED (и
свои IN-PROGRESS), кроме: чужой `CLAIM` моложе 6 ч; уже ждёт приёмки (есть PUSHED/NOCHANGE). Колонки: зависимости
(ready/NOT-READY), ветка продолжения (`continue_branch` из state или строка `CONTINUE <ветка>@<sha>` первой строкой
REPORT.md), ссылка на REMAINING.md, заметки root. У проверяющего с `verify_only` очередь всегда пустая.

**Захват:** менеджер пишет имя линии в `issues\<n>\CLAIM` перед работой и удаляет после сдачи. Очередь — снимок на
момент старта сессии, поэтому перед каждой Issue менеджер заново проверяет CLAIM и метку code-complete.

**Закрепление Issue за линией (28.09 21:45, владелец: «чтобы работа шла эффективно, а не тратилось время на хуйню»).**
Замер: за сутки 98 Issue брали 2–6 линий, почти всегда с итогом NOCHANGE. Причин было три:
1. Очередь одинаковая для всех.
2. Codex-сессии живут часами на снимке очереди с момента старта.
3. Четыре линии с `escalated_first` получали одни и те же тяжёлые Issue.

Теперь `state.py` (`lane_owner()` по строкам `#<n> CLAIM|PUSHED|MERGED|NOCHANGE|BLOCKED-BY` в `workers\*\STATUS.md` активных линий за 24 ч) решает так:

| Случай | Условие | Что делает очередь |
|---|---|---|
| Своя Issue | линия сдала по ней код за последние 8 ч | строка **YOURS** наверху: доделать |
| Чужая Issue | другая линия трогала её за 8 ч | не выдаётся («Not offered … keeps it») |
| Воронка | два исхода без кода (NOCHANGE/BLOCKED, разные 10-минутные окна) за 24 ч, после них кода нет | не выдаётся никому; разбирает root (`python state.py sinks`) |

- `escalated_first` снят у MC, CS1, CS2 и CL1. ESCALATED-Issue идут в разбор root; блокеры по-прежнему выдаются в начале очереди.
- Менеджер обязан брать свежую очередь (`state.py sector-queue <линия>`) перед каждой новой Issue.
- Чеклист начинается с копии `CHECKLIST.prev.json`.
- Циклы блокеров (#a ждёт #b, #b ждёт #a) делает одна линия, обе Issue сразу.

---

## 6. Бриф менеджера (`TEMPLATE-SECTOR.md`) — что требуется от линии

Раннер подставляет `{WORKER}`, `{SECTORS}`, `{MODEL}`, `{ISSUES}`, `{WD}`, `{QUEUE}`. Лаунчеры синхронных движков
дописывают раздел «Host: …» (инструменты субагентов, путь worktree, каталог сборки, UTF-8 без BOM). Содержание:

- менеджер сам ведёт очередь (параллелит, делит), сам читает и пишет код, лично проверяет каждый дифф субагента;
  задания субагентам полные (Issue, пункты дословно, документация, файлы, условие готовности, запреты, гейт);
- решения владельца главнее строк аудита «P3 — отложить до OSP1»; Issue не закрывать/не переоткрывать;
- **без тестов**; доказательства в чек-листе — `путь.ext::символ` (не номера строк); TEST-PHASE обязан называть
  существующий подключённый код; нельзя «создать адаптер и выбросить результат»; **root автоматически проверяет
  достижимость** (раздел 8.5);
- без `git stash`; служебные файлы только в `issues\<n>\`, никогда в worktree и не в коммитах; вопросов не задавать;
- протокол Issue: CLAIM → комментарии Issue + фрагменты документации + REMAINING.md/CHECKLIST.prev.json → worktree
  `eliot-swarm\<линия>-<n>` от свежего `origin/main`, ветка `<kind>/<n>-<slug>` (влитая ветка закончена — новая попытка
  идёт новой веткой) → реализация по COMMON-RULES → CHECKLIST.json (все пункты Work и Acceptance дословно, id не
  исчезают) → субагент-проверяющий → гейт clippy → квитанции `docs_read.py` в `issues\<n>\` → rebase, push,
  **PUSHED** = одна строка `<ветка>@<полный sha>`
  (или **NOCHANGE**), REPORT.md, комментарий в Issue, удалить CLAIM;
- сдавать каждый прошедший гейт инкремент, даже если Issue не закончена; небезопасный для вливания checkpoint —
  строкой `CONTINUE <ветка>@<sha>` в REPORT.md; PUSHED/NOCHANGE — одноразовые сообщения, не восстанавливать;
- после опровержения OR — исправить пункты из REMAINING.md и написать новый чек-лист (тот же байт-в-байт отклоняется);
- диск: только `targets\<линия>` (+`-2`), в одном каталоге одновременно собирает один worktree, после смены worktree
  `cargo clean -p <крейт>` перед зачётным clippy;
- журнал `workers\<линия>\STATUS.md`: `<время> #<n> CLAIM|PUSHED|NOCHANGE|BLOCKED-BY …`.

**Действующие правила владельца (27–29.09), все записаны в `TEMPLATE-SECTOR.md`:**
- **Писатели только пишут код, сборку делает менеджер** (владелец 29.09 00:10: «Пусть менеджер делает сборку и
  проверяет ошибки. Писатели пусть пишут код»). Писатели не запускают cargo (build/check/clippy/fmt): пишут, коммитят,
  возвращают кусок. Менеджер прогоняет fmt + clippy по вернувшемуся куску и отдаёт точные ошибки писателю или правит
  сам. Причина: писатели запускали clippy 6–10 раз за 30 минут (у W2 50 минут сборок за полчаса) в общей папке
  сборки линии из разных рабочих копий.
- **4 субагента ВСЕГДА** (Muse Code — 8, OR — 4): как только один вернулся, сразу запускать следующего; каждый на своей
  взятой Issue (или непересекающихся файлах) в своём worktree.
- **МЕНЕДЖЕР НИКОГДА НЕ ЖДЁТ — ОН РАБОТАЕТ** (владелец 28.09 20:45: «Менеджер не должен ждать! Он должен делать
  работу! Проверять, дорабатывать, контролировать агентов»). Субагенты запускаются в фоне; ни одного ожидающего вызова;
  между своими делами менеджер смотрит статус субагентов:
  | Движок | Запуск | Проверка статуса (без ожидания) |
  |---|---|---|
  | OpenCode | `subagent` с `"background": true` (без него вызов блокирует менеджера) | `subagent` по его `sessionID` |
  | Codex | `spawn_agent` | `wait_agent` только с тайм-аутом 0 (забрать готовое); `send_input` — перенаправить |
  | Muse Code | `muse.subagent_spawn` | `muse.subagent_status` / `subagent_read_result`; `subagent_wait` запрещён |
  | Command Code | `agent` с `run_in_background: true` | `agent_output` |
  Своя работа менеджера, пока субагенты работают: построчно проверять каждый вернувшийся дифф против пунктов Issue и
  документации; исправлять сам или точно возвращать брак; гонять fmt + clippy по готовым кускам; сводить, rebase,
  сдавать (PUSHED, чек-лист, REPORT); читать следующую Issue, её комментарии, AUDIT.md и документацию и готовить
  следующее точное задание; заглядывать в worktree молчащего субагента (`git status`, `git diff`) и поправлять курс.
- **Менеджер сам контролирует и проверяет работу и не сидит без дела:** опрашивает субагентов, построчно читает
  каждый дифф против пунктов Issue и документации, возвращает брак или правит сам, сводит и сдаёт, готовит следующее
  задание. Субагент, который 30 минут не отвечает и не продвигается, можно остановить и отдать его кусок новому.
- **Взятая Issue доделывается целиком**; PARTIAL только если пункту нужен код другой открытой Issue (`BLOCKED-BY #x`).
  Никаких лимитов по времени, «одного прохода», «двух попыток».
- **Вопросов владельцу нет:** ответы — в Issue, комментариях, документации и коде; иначе `ASSUMPTION:` + реализовать.
  `BLOCKED-BY decision` / «owner supply» запрещены.
- **Не подменять заявленную гарантию слабой проверкой** (правило 10): содержимое receipt сверяется с операцией;
  контрольная сумма — исходная, а не пересчитанная; удалять/переиспользовать только своё (владение, а не имя);
  проверка полноты — против независимого ожидаемого набора, не против копии того же списка.
- **Замечания внешнего аудитора (`issues\<n>\AUDIT.md`) — это спецификация:** на каждое — пункт чек-листа
  `AUD<k>` с `"audit": "<id комментария>"`; без них Issue не станет готовой.
- **Сборка один раз, когда код Issue дописан** (не после каждой правки).

Формат чек-листа:

```json
{"issue": 1858, "items": [
  {"id": "W1", "text": "<пункт дословно>", "status": "MET", "impl": "crates/x/src/a.rs::func", "caller": "bins/y/src/main.rs::dispatch"},
  {"id": "A1", "text": "...", "status": "TEST-PHASE", "impl": "crates/x/src/a.rs::func", "reason": "нужен запуск на установке"}]}
```

---

## 7. Раннер линии (`v2\Run-Sector.ps1 -Worker <линия> [-WaitForPid <pid>]`)

Отдельный скрытый процесс `pwsh` на каждую линию (не зависит от сессии root). Цикл:

1. `STOP`-файл в V2 или `active=false` → выход.
2. Проверяющий (`role=verifier`): если `state.py cc-queue` не пуст — сессия проверки по `TEMPLATE-CCV.md`; иначе
   (без `verify_only`) — обычная очередь. Остальные: `state.py sector-queue <линия>`; пусто → ждать 5 мин.
3. Рабочая копия `eliot-swarm\M-<линия>` переводится на свежий `origin/main` (detached); бриф
   `workers\<линия>\SECTOR-BRIEF-<время>.md`.
4. Лаунчер по `engine`: `opencode` → `Launch-Manager.ps1`; `muse` → `Launch-MuseCode.ps1`; `codex` →
   `Launch-Codex.ps1` (model/effort/sub_model/sub_effort из SECTORS.json); `cmdcode` → `Launch-CommandCode.ps1`.
   Лаунчеры синхронных движков ставят `CARGO_TARGET_DIR=targets\<линия>` и `CARGO_INCREMENTAL=0`.
5. Ждёт выхода процесса (PID в `current.pid`). Для OpenCode затем ждёт, пока сессия и все её дочерние уйдут из
   `session.active` (`Wait-SessionIdle`, опрос раз в минуту, **без прерываний**).
6. Разбор последних 40 строк журнала — **только настоящие события ошибок** (`"type":"error"`, `run.terminal.failed`,
   `"subtype":"error"`), не вывод инструментов:
   - «Rate limit exceeded» без слов о квоте → ждать 60 мин и продолжить;
   - «usage limit / insufficient_quota / quota exceeded / Individual quota» → `active=false`, выход;
   - сессия короче 5 мин → ждать 30 мин (защита от горячего цикла при ошибке хоста/входа).
7. Пауза 15 с → следующая свежая сессия со свежей очередью.

При старте раннер сначала ждёт окончания последней сессии линии (для OpenCode — по `sessionID` из журнала; для
синхронных движков — `-WaitForPid <pid живого процесса>`), поэтому перезапуск раннера живую сессию не трогает.

---

## 8. Приёмка: демоны `v2\Merge-Daemon-v5.sh` (10 потоков с 29.09 04:09)

**v5 (29.09 04:09):** если во время разбора main сдвинулся в тех же крейтах, тот же слот сразу разбирает ветку заново на своей прогретой папке сборки (до 3 попыток, строка `RECHECK` в `daemon.log`), а не возвращает её в общую очередь (`REQUEUE` — только после 3 попыток). **Разбор (`Review-Branch2.sh`, 03:45):** проверка сборки — только затронутые пакеты и все, кто от них зависит (`v2
evdeps.py` по `cargo metadata --no-deps`; при охвате больше 60 % — всё пространство), а не всё пространство на каждый разбор: у 10 слотов папки сборки отстают от main на ~9 слияний, и полная проверка пересобирала несвязанные крейты. В файле разбора — строка `compile scope:`. Остановка v5 — `v2\Stop-DaemonV5-Safely.ps1`.

**v4 (29.09):** блокировка слияния держится только на проверку «main сдвинулся в моих крейтах» и слияние на GitHub (`Make-PR.sh`). Отчёт PR, название Issue, валидация чеклиста `state.py merged` (~53 с) и метка идут вне блокировки. В v3 всё это было под блокировкой: ~3,5 мин на слияние, потолок ~17 слияний в час при любом числе слотов. Остановка — `v2\Stop-DaemonV4-Safely.ps1` (как V3, по имени v4). После остановки проверить, что живых демонов нет, затем удалить `reviews\claims\*` и `v2\merge.lockdir`.

### 8.1 Запуск

Четыре экземпляра с 27.09 08:55 (было шесть с 26.09 18:00; при росте очереди — снова 5–6), различаются `REVIEW_SLOT` (пусто, `2`…`6`); каталоги сборки новых слотов засеяны копией `targets\ROOT-review4`/`ROOT-main4`. Каждый — долгий процесс bash:

```bash
V2=/c/Development/Rust/projects/eliot-swarm/control-20260923-impl/v2
REPORT_EVERY=100000 REVIEW_SLOT=   bash $V2/Merge-Daemon-v5.sh >> $V2/daemon.log 2>&1   # поток 1
REPORT_EVERY=100000 REVIEW_SLOT=2  bash $V2/Merge-Daemon-v5.sh >> $V2/daemon.log 2>&1   # потоки 2..10 так же
```

Слот определяет рабочие копии и каталоги сборки: поток 1 — `eliot-swarm\ROOT-review` / `ROOT-main-d866`, каталоги
`targets\ROOT-review` / `targets\ROOT-main`; поток N — `ROOT-review<N>` / `ROOT-main<N>` и одноимённые `targets`.
Рабочие копии создаются сами при первой проверке. В журнале у каждой строки метка `[rv1]…[rv4]`.
**Правило:** работающий bash-скрипт не править (bash читает его по ходу) — новая версия новым файлом; остановка только
в безопасной точке (идёт проверка, а не `Make-PR.sh`/`gh`/`state.py merged`).

### 8.2 Цикл одного прохода

```text
snapshot = state.py review-list2 | tr -d '\r'          # NOCHANGE первыми, затем PUSHED
для каждой строки n|kind|ref:
  claim n: mkdir reviews/claims/<n> (захват старше 90 мин считается брошенным)
  перепроверка: строка n в свежем review-list2 должна совпасть со снимком, иначе пропуск (другой поток уже сделал)
  NOCHANGE  → state.py nochange n → при CODE-COMPLETE label_done
  PUSHED    → ветка = ref до '@'
     нет на origin (ls-remote)       → state.py hold "branch not found" (если ветка уже влита — указание начать новую)
     нет коммитов сверх main         → EMPTY-PUSH: nochange, удалить ветку с GitHub и чистый worktree
     REVIEW_SLOT=<слот> bash Review-Branch2.sh <ветка>   → reviews/<ветка>.txt
     разбор отчёта → причины возврата (8.3); только формат → авто-rustfmt; иначе → HOLD->BACK (state.py hold)
     mlock: mkdir v2/merge.lockdir (брошенный старше 30 мин снимается)
     main сдвинулся после проверки (main= в отчёте ≠ ls-remote) и задеты те же крейты/Cargo.* → REQUEUE (проверка заново)
     PR-REPORT.md из CHECKLIST.json и REMAINING.md
     заголовок: "<kind>: <заголовок Issue без [метки]> (#n)", kind из ветки (feat|fix|refactor|docs|chore|test|perf|build)
     Make-PR.sh → должно вывести "merge: MERGED", иначе hold "PR/merge failed"
     state.py merged n pull/N → finish_review (8.5); munlock; при CODE-COMPLETE label_done
  unclaim n
после прохода: выход, если нет ни одного раннера линий ("workers gone"); иначе sleep 90
```

`label_done` ставит метку `code-complete` и пишет в Issue комментарий с таблицей чек-листа.

### 8.3 Что проверяет `Review-Branch2.sh` (результат вливания, а не голую ветку)

1. `git fetch --prune`; ветка — в `refs/remotes/origin/<ветка>` (не `FETCH_HEAD`: он общий для потоков); `MAIN_SHA`.
2. Шапка отчёта: `branch=… head=… main=<MAIN_SHA>`, коммиты, `mergeable-with-main` (`git merge-tree`), diffstat.
3. В `ROOT-review<слот>`: checkout `MAIN_SHA`, `git merge --no-ff <head>`; конфликт → `MERGE-CONFLICT with main`.
4. Пакеты: для каждого изменённого файла ближайший вверх `Cargo.toml` → имя пакета. Нет пакетов →
   `no cargo packages touched` (документы/скрипты) — дальше не проверяется.
5. **Красные флаги** в добавленных строках: новые `#[test]`/`#[tokio::test]`, новые файлы в `tests/`, служебные файлы
   (PUSHED, CLAIM, CHECKLIST*.json, REPORT*.md, …), `todo!`, `unimplemented!`, `NotYetImplemented`,
   `allow(dead_code)`, `InMemory*`, `NoOp*`, `shim`, `compat alias`.
6. `cargo check --locked -p <пакеты> --all-targets` (ошибки — в отчёт).
7. **Несобирающиеся таргеты всего workspace**: `cargo check --locked --workspace --all-targets --keep-going` в
   результате вливания и в main (результат main кэшируется в `reviews\cache\ws-<MAIN_SHA>.txt`);
   `NEW broken targets` = есть после вливания, нет в main.
8. **Clippy по каждому пакету отдельно** после `cargo clean -q -p <пакет>` (иначе кэш даёт ложь):
   `cargo clippy --locked -p <p> --lib --bins --no-deps -- -D warnings`, ошибки нормализуются в `файл|сообщение`;
   сторона main кэшируется в `reviews\cache\lint-<MAIN_SHA>-<пакет>.txt`; `NEW clippy findings` = прирост счётчика по
   паре (файл, сообщение).
9. `cargo fmt -p <пакеты> -- --check -l` в обоих деревьях → `NEW unformatted files`.
10. `review-done`.

**Причины возврата** (демон): `conflict-with-main(rebase-and-resolve)`; `review-incomplete` (нет `review-done`);
`new-broken-targets:[…]`; `new-clippy:[…]`; `red-flags(…)`. Только `NEW unformatted` → демон сам запускает
`rustfmt --edition 2024` на этих файлах на голове ветки, коммитит «style: rustfmt files touched by #n» и пушит.

### 8.4 `Make-PR.sh <ветка> <worktree> <report> <review> <title> <refs> <issue>`

1. `HEAD_SHA` (ls-remote), `BASE_SHA` (origin/main).
2. Квитанции `docs_read.py` (`docs-read-receipt*.json`) ищутся по порядку: `v2\issues\<n>\` (линия копирует их туда
   при сдаче), `.eliot\` worktree ветки, `.eliot\` любого worktree с HEAD = голова ветки. Нет нигде → PR не
   создаётся, выход 3 с сообщением «NO DOCUMENTATION READ RECEIPT …», Issue возвращается исполнителю (26.09: 7 из 40
   PR были влиты с пустыми полями, см. #2965).
   Тело PR по шаблону репозитория: квитанции (route/read receipt, маршруты, хэш bundle, аттестация), owning work,
   source identity (base/candidate, предок ли main), чек-лист из PR-REPORT.md, выдержка отчёта проверки, остаток,
   гигиена, подпись.
3. Если у ветки уже открыт PR — он переиспользуется (заголовок/тело обновляются), иначе `gh pr create`.
4. `gh pr merge --squash --subject "<title> (#PR)"` — до 4 попыток с паузой 20 с (GitHub отвечает «Base branch was
   modified» сразу после вливания другим потоком); успех, если `state = MERGED`.
5. Строка в `merged-ledger.tsv`; удалить ветку на GitHub; удалить чистые worktree ветки и локальную ветку;
   fast-forward root main; комментарий в Issue «Влито в main … (PR #…)».

### 8.5 Проверка чек-листа: `state.py validate` и `finish_review`

`validate(n)` читает `issues\<n>\CHECKLIST.json` на `origin/main`:

- нет файла / неверный JSON / нет пунктов → проблема;
- пункты пропали по сравнению с `CHECKLIST.prev.json` → проблема;
- статус не MET/TEST-PHASE → «осталось»; TEST-PHASE без `reason` или без `impl` → проблема;
- ссылки `путь.ext::символ` (любой тип файла): файл есть на main, символ в нём есть; `caller` не в тестовом коде;
- **достижимость** (с 26.09): для `caller` пункта MET и `impl` пункта TEST-PHASE, если это функция в `.rs`, обход
  вверх по местам вызова (`git grep -w`, функция-обёртка каждого места, до `fn main`). Считается достижимым: ссылка
  на уровне модуля (таблица регистрации, static, макрос), метод трейта (его может вызывать serde/std/tokio),
  реализация в тестовом пути, исчерпанный бюджет (25 узлов на символ, 45 с на чек-лист — проверка идёт под блокировкой
  вливания). Иначе проблема «has no production call site … wire it into a live path»;
- старые ссылки `путь:строка`: строка существует, не пустая и не комментарий, вызывающий не в `tests/`,
  не в `#[cfg(test)]`, не в заглушке `cfg(not(windows))`.

```python
# state.py (сокращённо): поиск пути до fn main
def production_refs(sym, where='origin/main', max_nodes=25):
    seen, frontier = {sym}, [sym]
    while frontier:
        nxt = []
        for s in frontier:
            for path, line in _prod_ref_sites(s, where):      # git grep -w, без определения, комментариев, тестов
                fn = _enclosing_fn(path, line, where)          # по балансу скобок вверх от места ссылки
                if fn is None or fn == 'main' or _in_trait_context(path, line, where):
                    return 1                                   # достижимо (или нельзя доказать обратное)
                if fn not in seen:
                    seen.add(fn); nxt.append(fn)
        frontier = nxt
    return 0                                                   # доказано: никто не вызывает
```

`finish_review(n, merged|nochange)`: `validate` + защита «тот же опровергнутый чек-лист» (sha256 файла совпадает с
`CCV.json` REFUTED или с `REFUTED-CHECKLISTS.txt`) → нет CODE-COMPLETE. Прогресс = было вливание или стало меньше
«осталось + проблем»; 3 раза подряд без прогресса → **ESCALATED** (первыми берут MC, CX, CS1). Не закончена →
CHECKLIST.json переезжает в CHECKLIST.prev.json, REMAINING.md переписывается (`- <пункт>`, `- CHECK: <проблема>`).

`hold(n, ветка, причина)`: PARTIAL, `continue_branch` = ветка (если она ещё не влита), причина дописывается в
REMAINING.md, PUSHED/NOCHANGE удаляются.

`review-list2` перед каждым проходом: у CODE-COMPLETE/CLOSED удаляет устаревшие PUSHED/NOCHANGE; снимает BOM с
PUSHED/NOCHANGE/CHECKLIST*/CCV/CLAIM; из первой строки PUSHED берёт `<kind>/<имя>@<sha>` в любом месте строки.

---

## 9. Перекрёстная проверка: OR и `apply_ccv.py`

- Раннер OR берёт `state.py cc-queue` (CODE-COMPLETE, чей CHECKLIST.json новее CCV.json, до 6) и запускает сессию по
  `TEMPLATE-CCV.md`: только чтение; `main_sha`; сначала документация (Issue + комментарии + post-merge review,
  `docs_read.py`, каждый обязательный фрагмент) и цепочка «норматив → требование → владелец → production-путь →
  расхождение»; проверка всей цепочки вызовов; контрпример на каждое опровержение; самопроверка (открыть каждый
  символ на main, `путь::символ`, пересчитать числа, только существующие команды); непроверенное — в `remarks`.
- Результат — `issues\<n>\CCV.json`:
  `{"issue","verdict":"CONFIRMED|REFUTED","main_sha","refuted_items":[{"id","why","evidence","counterexample","norm"}],"remarks","checked_at","checklist_sha256"}`.
- `apply_ccv.py` (цикл раз в 15 мин): каждый вердикт применяется один раз (`ccv-applied.txt`). REFUTED у
  CODE-COMPLETE → PARTIAL, раздел в REMAINING.md (id, why, evidence, контрпример, норматив), хэш чек-листа в
  `REFUTED-CHECKLISTS.txt`, CHECKLIST.json → prev, снять метку, комментарий в Issue (`--body-file`).
- Перенос проверок: если OR стал `active=false`, `apply_ccv.py` ставит W4 `role=verifier` (гибрид: сначала проверки,
  потом свой сектор). Вернуть OR: `active=true`, у W4 убрать `role`/`verifier_for`, запустить раннер OR.
- Качество OR (26.09): бывали сдвинутые координаты и неверные счётчики — поэтому самопроверка в брифе; сами дефекты
  чаще подтверждаются. Исполнители перепроверяют каждое опровержение по свежему main.

---

## 10. Внешний ревьюер и Issue

- Внешняя модель (GPT) пишет новые Issue (дочерние к только что влитым), комментарии и post-merge review в PR.
  Её инструкция лежит у владельца; наш OR — отдельный проверяющий.
- Новые Issue → `insert_after.py` сразу после родительской. Связанные задачи с одним общим файлом делаются одним
  проходом одного исполнителя (пример: #2702+#2703+#2705 в `improvement_pipeline.rs` — пометка BUNDLE в ORDER.txt и
  подробный REMAINING.md у каждой; PUSHED только в первой, остальные — NOCHANGE после вливания).
- Находки из тела PR #2492 (`| P1 | [#n: title](url) | repair |`) применяет `ingest_pr_review.py 2492` (тот же цикл
  15 мин): находка первой строкой REMAINING.md, PARTIAL, метка снята.
- Закрытую владельцем/ревьюером Issue (например, дубликат) — `state.py set <n> CLOSED`; закрытую агентом — переоткрыть.

---

## 11. Обслуживание: диск, worktree, ветки (Antigravity раз в 5 ч)

- `cleanup\Run-Maintenance.ps1 -FirstAt "<дата ЧЧ:ММ>"` — отдельный процесс; слоты по 5-часовой сетке
  (`cleanup\maint.log`). На каждый слот — `agy` по `cleanup\MAINT-BRIEF.md`, отчёт `cleanup\runs\<время>\REPORT.md`,
  журнал удалений `cleanup\ledger.tsv`.
- Фаза 1 — мусор: простаивающие ≥ 2 ч и не используемые процессами каталоги сборки в `%TEMP%\opencode`,
  `eliot-swarm\targets\*` (кроме каталогов линий и `ROOT-review*`/`ROOT-main*`), `%TEMP%\eliot-*`, `C:\Temp`, каталоги
  `target*` внутри worktree, БД codebase-memory удалённых проектов. WAL живой базы не трогать.
- Фаза 2 — лишние worktree: влитые (по `merged-ledger.tsv`) — удалить без push; незакоммиченное/невлитое — сначала в
  `archive-wip/<каталог>-<дата>`. Сохраняются: root, `M-*`, `ROOT-review*`, `ROOT-main*`, worktree с живым CLAIM,
  изменённые за 3 ч.
- Сразу после прогона `cleanup\Fold-ArchiveWip.sh` сворачивает все `archive-wip/*` в архивный коммит ветки
  `archive/wip-preserved-20260925` (дерево = main, родители = прежняя голова архива + головы WIP), проверяет
  `merge-base --is-ancestor` для каждой и только потом удаляет ветки с GitHub (локально — `refs/archive/wip-<время>/*`).
- **Ветки GitHub — `cleanup\Prune-Branches.py --apply --fold-stale`** (владелец 29.09: «300+ веток, многовато»; Run-Maintenance вызывает его
  после Fold-ArchiveWip на каждом слоте). Защищены: `main`, `archive/*`, `archive-wip/*`, головы открытых PR, ветки
  в PUSHED/NOCHANGE, `continue_branch` из state.json, ветки с коммитом моложе 12 ч. Удаляются влитые: вершина в main,
  или равна `branch_sha` из `merged-ledger.tsv` (squash, после вливания ничего), или `git merge-tree --write-tree
  origin/main <вершина>` даёт дерево main. Невлитые, не упомянутые ни в одном REPORT/REMAINING/TASK, не выгруженные
  ни в один worktree и старше 24 ч — сворачиваются в архивный коммит `archive/wip-preserved-20260925` (как
  archive-wip) и после проверки `merge-base --is-ancestor` удаляются. Журнал `cleanup\pruned-branches.tsv`, локальные
  копии `refs/archive/pruned-<время>/*`, `refs/archive/stale-<время>/*`. 29.09 15:12: 303 → 201 (49 удалено, 53 свёрнуто).

---

## 12. Наблюдение root (раз в 30 минут)

```bash
bash /c/Development/Rust/projects/eliot-swarm/control-20260923-impl/v2/Lane-Report.sh
```

Отчёт (только чтение): место на диске; сводка состояний; последние MERGED и HOLD; применённые вердикты OR; по каждой
линии — журнал/сессия/активность, последняя ошибка, хвост STATUS.md; живые раннеры; Issue, закрытые за 35 мин; новые
Issue и чужие PR; число веток на GitHub; каталоги сборки в `%TEMP%\opencode`.

Что делать по отчёту:

| Видно | Действие |
|---|---|
| Новые Issue | `insert_after.py` после родителя |
| Закрыта Issue | кто закрыл (`gh api .../events`): агент — переоткрыть; владелец/ревьюер — `state.py set <n> CLOSED` |
| Линия пропала из раннеров | лог `workers\<линия>\sector.log`: квота или ложная остановка → раннер заново |
| Сессия «running» без движения | вопрос-форма? (`session.form.list`) — ответить; не прерывать |
| Много HOLD «no remote branch» | линия пишет PUSHED неверно/для влитой ветки — дополнение в сессию |
| STATUS говорит PUSHED, а файла нет | сдача невидима — дополнение с абсолютными путями |
| Очередь приёмки растёт | больше потоков приёмки (CPU позволяет) |
| Каталог сборки в `%TEMP%` | дополнение линии о правиле диска |
| Много code-complete опровергается | выборочно проверить код самому (так найдена проблема достижимости) |

Отчёт владельцу — кратко: статусы, вливания, возвраты, опровержения, новые Issue, линии, диск/ветки.

Фоновые циклы root (запускаются задачами в сессии root и умирают вместе с ней — после потери сессии запустить заново):

```bash
V2=/c/Development/Rust/projects/eliot-swarm/control-20260923-impl/v2
# внешние находки (PR #2492), комментарии аудитора, вердикты OR — раз в 15 мин
while true; do r=$(cd $V2 && PYTHONIOENCODING=utf-8 python ingest_pr_review.py 2492 2>&1); [ "$r" != "no new findings" ] && echo "$(date -Is) $r" >> $V2/pr2492-ingest.log; r3=$(cd $V2 && PYTHONIOENCODING=utf-8 python ingest_audit.py 2>&1); [ "$r3" != "no new audit comments" ] && echo "$(date -Is) $r3" >> $V2/audit-ingest.log; r2=$(cd $V2 && PYTHONIOENCODING=utf-8 python apply_ccv.py 2>&1); [ "$r2" != "nothing to apply" ] && echo "$(date -Is) $r2" >> $V2/ccv-applied.log; sleep 900; done
# напоминание линиям с < 4 субагентами — раз в 15 мин (при сворачивании остановить)
while true; do sleep 900; (cd $V2 && PYTHONIOENCODING=utf-8 python Remind-Subagents.py > /dev/null 2>&1); done
# PARTIAL, которые уже проходят на main — раз в час
while true; do sleep 3600; (cd $V2 && PYTHONIOENCODING=utf-8 timeout 1800 python revalidate_partial.py >> $V2/revalidate.out 2>&1); done
# пересборка изменившихся TASK.md — раз в 30 мин
while true; do sleep 1800; (cd $V2 && PYTHONIOENCODING=utf-8 python make_task.py --open > $V2/make_task.last 2>&1; tail -1 $V2/make_task.last >> $V2/make_task.log); done
# отчёт владельцу — цепочка раз в 30 мин
bash $V2/Report30.sh 1800
# Answer-Forms.ps1 НЕ запускать: он вызывает CLI opencode, а CLI перезапускает сервис OpenCode (§14)
```

---

**Как root следит за агентами (с 28.09; «следить за агентами — твоя главная задача»):**
- Каждые 30 минут `v2\Report30.sh` (цепочка фоновых запусков; задания CronCreate в этой среде НЕ срабатывают):
  сначала `v2\Agent-Watch.py` — по каждой линии работающие/молчащие субагенты ТЕКУЩЕЙ сессии менеджера, правки,
  сборки, чтения за 30 минут и флаги («one sub-agent at a time», «manager BLOCKED on foreground subagent call»,
  «rebuild loop», «reading, not writing», молчащие >20 мин). Для MC/CB/Codex — реестр `workers\<линия>\logs\steer\SUBAGENTS`.
- **Статус читать по-настоящему, прежде чем называть линию зависшей:** открытое сообщение менеджера с работающим
  инструментом (например, `subagent`) — это работа, а не зависание; смотреть, какой инструмент открыт и когда было
  последнее сообщение у его субагентов (БД `~/.local/share/opencode/opencode.db`, только чтение).
- Каждые 15 минут `v2\Remind-Subagents.py`: линия с меньше чем 4 работающими субагентами (MC — 8) или с молчащими
  >30 мин получает напоминание (не чаще раза в 30 мин): OpenCode — `opencode api session.prompt` с
  `"delivery":"steer"` в живую сессию; Codex, Muse Code, Command Code — файл во входящие `logs\steer\*.txt`.
- **Сообщение в идущую сессию** (steer) есть у всех движков: OpenCode — `session.prompt`+`delivery:steer`;
  Codex — `v2\codex_as.py` (свой `codex app-server`, `turn/steer`); Muse Code — `v2\muse_as.py` (`muse serve`,
  MSP `turn/steer`); Command Code — мод `v2\cc-root-inbox.ts` (`cmd.queueMessage deliverAs steer`). Отправить
  вручную: `python v2\codex_as.py steer --inbox v2\workers\<линия>\logs\steer --text "..."` (то же для любой из трёх).

## 13. Перезапуск (перезагрузка хоста или потеря сессии root)

0. **UAC (с 29.09).** Владелец включает UAC на минимальном уровне файлом `Downloads\Enable-UAC-Minimal.cmd`
   (копия в `v2\`): `EnableLUA=1`, `ConsentPromptBehaviorAdmin=0`, `PromptOnSecureDesktop=0`, действует после
   перезагрузки. Root системные настройки безопасности сам не меняет. После перезагрузки процессы по умолчанию без
   повышения (`whoami /groups` → `Medium Mandatory Level`); права на `C:\Development`, `C:\Tools`, `~\.codex`
   проверены 29.09: владелец `kleym`, у Authenticated Users — Modify, запись не теряется.
   - **Сервисы только со СКРЫТОЙ консолью** (владелец 29.09 14:50: «без стреляющей консоли»). Процесс без консоли
     (запущенный приложением или отсоединённым демоном) даёт каждой своей команде (`cmd`, `pwsh`, MCP-серверы) новую
     консоль, и Windows 11 открывает её окном Windows Terminal. Запуск через WMI со `ShowWindow = 0`:
     ```powershell
     $si = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]0 }
     Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = '<команда>'; CurrentDirectory = $env:USERPROFILE; ProcessStartupInformation = $si }
     ```
     Проверка: у процесса сервиса есть дочерний `conhost.exe`; окна считать `root-tools\count_windows.ps1`
     (видимые окна классов `ConsoleWindowClass` / `CASCADIA_HOSTING_WINDOW_CLASS`,
     опрос раз в 0,5 с 60 с) — должно быть 0.
   - **Codex:** `"~\.codex\packages\app-server-daemon\current\bin\codex.exe" app-server --listen unix://` (скрытая
     консоль, штатный сокет `~\.codex\app-server-control\app-server-control.sock`). НЕ `codex app-server daemon start`:
     демон отсоединяется от консоли (окно на каждую команду) и держит `daemon pid-update-loop` (автообновление, сам
     перезапускает демон). Пакет `current` ставит один раз `codex app-server daemon start` отдельного CLI
     (`%LOCALAPPDATA%\Programs\OpenAI\Codex\bin\codex.exe`; встроенный в приложение — «no complete local package»;
     из оболочки root — только через WMI, объект-задание Claude запрещает отсоединённые процессы), затем
     `daemon stop`. 29.09 пакет = 0.159.0. Проверка: WebSocket-рукопожатие через `codex app-server proxy` (101 + ответ
     на `initialize`).
   - `v2\codex_as.py` — клиент этого сервера: `codex app-server proxy` + WebSocket RFC 6455 поверх stdio; настройки линии
     из `~\.codex\lane.config.toml` в `thread/start.config`. Проверено: codebase_memory ready, цель ставится, steer
     доходит (`root.steer` в журнале), субагенты — Luna max (модель смотреть в `turn_context` файла сессии субагента,
     в заголовке стоит модель родителя). События субагентов на общий сервер не приходят в журнал линии — состав
     субагентов берётся из `steer\SUBAGENTS`.
   - **OpenCode:** закрыть OpenCode Beta и `opencode-cli.exe`; скрыто запустить
     `"%APPDATA%\ai.opencode.desktop.beta\cli\2.0.7\opencode-cli.exe" serve --service`; дождаться нового pid в
     `~\.local\state\opencode\service.json`; затем открыть приложение `explorer.exe shell:AppsFolder\ai.opencode.desktop.beta`
     — оно подключается к этому сервису (второго сервиса нет). Сервис сам поднимает прерванные сессии (29.09: 25).
   - Вопросы менеджерам Codex — `codex exec fork <id> "вопрос"`; `v2\codex_ask.py` устарел.
1. Не запускать менеджеров сразу. Сервис OpenCode (десктоп-приложение) сам поднимает прерванные сессии:
   `opencode api session.active`. Синхронные движки (Muse Code, Codex, Command Code) после перезагрузки мертвы.
2. Грязные worktree линий: `wip(#n): preserve …` (без служебных файлов) и push в ветку Issue; если ветка на GitHub
   разошлась — в `<ветка>-local-<дата>`, запись в REMAINING.md.
3. Раннеры всех линий с `active=true` в SECTORS.json (скрыто):
   ```powershell
   $V2='C:\Development\Rust\projects\eliot-swarm\control-20260923-impl\v2'
   Start-Process pwsh -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',"$V2\Run-Sector.ps1",'-Worker','W1' -WindowStyle Hidden
   # синхронный движок с живым процессом: добавить '-WaitForPid',<pid из workers\<линия>\current.pid>
   ```
   Сначала проверить, что раннер линии не запущен (иначе будут две сессии): процессы `pwsh` с `Run-Sector.ps1 -Worker <линия>`.
4. Демоны приёмки — 10 потоков v5 (8.1). Перед запуском: процессов `Merge-Daemon-v3/4/5.sh` нет; `v2\merge.lockdir` удалить,
   если демонов нет; брошенные `reviews\claims\*` снимутся сами через 90 мин.
5. Циклы раздела 12 и раннер обслуживания (скрыто): `Start-Process pwsh -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass',
   '-File',"<cleanup>\Run-Maintenance.ps1",'-FirstAt',"\"<ГГГГ-ММ-ДД ЧЧ:ММ>\"" -WindowStyle Hidden` (29.09 после перезагрузки
   его забыли — слот 15:00 пропущен; поднят в 15:13, первый слот 15:23).
6. Первый `Lane-Report.sh`, затем раз в 30 мин.

Остановка: файл `v2\STOP` (раннеры выходят после текущей сессии; демоны выходят, когда раннеров нет);
`cleanup\STOP` — раннер обслуживания.

---

## 14. Ловушки (что уже случалось и как решено)

**Сессии и движки**

| Ловушка | Решение |
|---|---|
| Модель без `#вариант` → `default`, поверхностная работа | всегда `provider/model#xhigh|#max` |
| Клиент OpenCode выходит (`Transport`, `shutdown`), сессия живёт в сервисе | линия занята, пока активна семья сессий (менеджер + `parentID`) |
| Автоматические прерывания «зависших»/«дублей» рвали живую работу | запрещено; `Watch-Lanes`/`Dedupe-Sessions` отключены (`.DISABLED`); только ожидание |
| Вопрос-форма в сессии без оператора: сессия висит «running» | `Answer-Forms.ps1` раз в 15 мин; в брифах «вопросов не задавать» |
| Поиск «usage limit» во всём хвосте журнала ложно остановил OR (текст из комментария PR) | квота только по событиям ошибок хоста |
| 429 принимался за квоту | 429 = частота: ждать час |
| `muse exec` буферизует stdout | живой журнал в `~/.local/share/muse/sessions` |
| Codex/Command Code обновляют CLI сами | Codex — самый новый `codex.exe`; Command Code — `--no-auto-update` |
| У `codex exec` нет дополнений | влиять брифом следующей сессии |
| Запуск сессии не из worktree репозитория | сессия попадает в чужой проект OpenCode |

**Сдача и приёмка**

| Ловушка | Решение |
|---|---|
| Самоотчёт принимался на веру (26 из 27 первых code-complete опровергнуты) | перекрёстная проверка другой моделью (OR) + автопроверки чек-листа |
| «Вызывающий» никем не вызывается (22 из 99 code-complete 26.09; `commit_canonical_and_refresh`, `backup_dispatch_*`) | проверка достижимости до `fn main` в `validate`; 22 Issue возвращены |
| TEST-PHASE «на всё» без кода; ссылки `путь:строка` устаревали | TEST-PHASE называет код; `путь::символ` для любых файлов |
| Служебные файлы в коммитах (#2493–#2497) | красный флаг в проверке ветки |
| Два демона одновременно — двойное вливание | потоки v3 с захватом `reviews\claims` и общей `merge.lockdir` |
| BOM в файлах управления (MC) — `review-list2` падал | чтение `utf-8-sig` + снятие BOM |
| CRLF от Python в bash — сравнение не совпадало | `tr -d '\r'` |
| `FETCH_HEAD` общий для потоков | ветка в `refs/remotes/origin/<ветка>` |
| «Base branch was modified» → ложный «merge failed» | переиспользование PR + 4 попытки |
| PUSHED в свободной форме («push: ветка@sha»), файлы по неверному пути | разбор `kind/имя@sha` в любом месте строки; дополнение с абсолютными путями |
| Линии «восстанавливали» PUSHED влитых веток и опровергнутые чек-листы | PUSHED/NOCHANGE одноразовые; защита от того же чек-листа; ESCALATED при повторе |
| Длинный `gh issue comment --body` (33 тыс. символов) — `WinError 206` | только `--body-file` |
| Checkpoint-ветки без PUSHED; root сдал незавершённый #2731 — не собрался | сдавать прошедшее гейт; незавершённое — `CONTINUE` в REPORT.md; root сам не сдаёт |
| Очередь сессии устаревает (взята уже code-complete) | перед каждой Issue проверять CLAIM и метку |
| Опровержения OR со сдвинутыми координатами | самопроверка в брифе OR; исполнители перепроверяют |

**Сборка и диск**

| Ловушка | Решение |
|---|---|
| Проверка только затронутых пакетов пропускала поломки других | несобирающиеся таргеты всего workspace «вливание против main» |
| Кэш clippy давал ложь; один clippy на несколько `-p` скрывал зависимые | `cargo clean -p` и clippy по каждому пакету |
| Один каталог сборки у двух worktree — фантомные E0425/E0433 | один worktree на каталог одновременно, `-2` для второго |
| ~910 ГБ мусора от каталогов сборки агентов | правило одного каталога; уборка раз в 5 ч |
| 187 веток на GitHub | архивный коммит + удаление; автоматический фолд `archive-wip/*` |
| `git stash` общий для worktree | запрещён; `wip:`-коммит |

**Инструменты root**

| Ловушка | Решение |
|---|---|
| Правка работающего bash-скрипта | новая версия новым файлом; остановка в безопасной точке |
| Правка `Make-PR.sh`/`Review-Branch2.sh`, пока демон их выполняет: bash дочитывает файл по старому смещению → `syntax error`, PR не создан (#342, 26.09 12:05) | писать во временный файл и `mv` поверх (старый inode остаётся у запущенного bash); провал по вине root — вернуть PUSHED самому и сообщить линии |
| Квота Codex кончилась у всех линий сразу (27.09 09:02, «You've hit your usage limit»): раннеры сами пишут `quota stop -> marked inactive` в `SECTORS.json` и выходят | после пополнения владельцем: вернуть `active: true` нужным линиям и запустить раннеры (`Start-Process pwsh … Run-Sector.ps1 -Worker <линия>`), проверить новые журналы на «usage limit». Выключенным линиям передать работу: незакоммиченное — `wip:`-коммит в их ветку + push, `continue_branch` в state, снять их CLAIM |
| Отчёт показывал линии Codex «running» после остановки по квоте: проверка смотрела только последнюю строку `sector.log` на «inactive in SECTORS», а при квоте раннер пишет `quota stop -> marked inactive` | живость линии = наличие процесса `Run-Sector.ps1 -Worker <линия>`; отчёт теперь в `v2\Report30.sh` (runners up/DOWN + счётчик «usage limit» в новейших журналах CS1/CS2) |
| `Stop-DaemonV3-Safely.ps1 -Match "REVIEW_SLOT=[56] "` ответил «all v3 daemons stopped», а слоты 5 и 6 работали ещё 16 часов: их обёртки уже умерли, в командной строке самого скрипта демона нет `REVIEW_SLOT`, фильтр ничего не нашёл и вышел с успехом | `-Match` без совпадений теперь выход 2; слот определять по времени создания процесса против строки `[rvN] acceptance daemon v3 started … <время>` в `daemon.log`, останавливать `-Pids` через `powershell -Command "& '…\Stop-DaemonV3-Safely.ps1' -Pids a,b"` (через `-File` массив не передаётся); после остановки проверить, что осталось ровно N процессов `v2/Merge-Daemon-v3.sh` |
| Сервис OpenCode 12 раз за 09:36–09:45 (27.09) признан «unresponsive» клиентами (`Background service is unresponsive` в `~/.local/share/opencode/log/opencode.log`, role=cli) и перезапущен: у W3 и W4 погибли фоновые субагенты-писатели. Совпало с одновременным стартом сессий нескольких линий (MCP-подключения, сканы ~110 worktree) при CPU ~93% | `Launch-Manager.ps1` разносит старты сессий OpenCode минимум на 3 минуты через общий `v2\oc-start.lock` (ждать не дольше 15 мин, потом стартовать). Правка в лаунчере, а не в раннере: раннер вызывает лаунчер заново на каждую сессию, перезапуск раннеров не нужен. Меньше слотов приёмки = меньше CPU |
| Владелец 27.09 10:20: «Пусть ждут!!! Кто им сказал перезапускать?» — в `TEMPLATE-SECTOR.md` стояло моё указание менеджерам «субагент завис/зациклился/простаивает — останови и перезапусти свежим» | указание удалено; теперь: субагентов, сессии и сервис OpenCode никогда не останавливать, не убивать и не перезапускать, `opencode` CLI для управления сессиями не вызывать, ждать сколько нужно. Очередь стартов в `Launch-Manager.ps1` ждёт без лимита. Бэкап `TEMPLATE-SECTOR.backup-20260927-1025.md` |
| Владелец 27.09 10:30 уточнил: «Если суб агент завис, пусть опрашивают его. Если не отвечает в течение полчаса - можно убивать.» | в `TEMPLATE-SECTOR.md`: зависший на вид субагент опрашивать (статус/вывод или вопрос о ходе работы); остановить можно только того, кто 30 минут не отвечает и не продвигается, его кусок отдать свежему субагенту. Медленного, но отвечающего не трогать. Сервис OpenCode и сессии других линий не перезапускать и не убивать |
| Владелец 27.09 11:00 принял сравнение моделей и велел перераспределить задания | `SECTORS.json`: W2, W4 → `opencode-go/space-bunny-free#high`; LC `profile: easy` (только Issue с 1–2 оставшимися пунктами или чистые правки оформления `- CHECK:`, со всех участков); CS1/CS2 `profile: hard` (эскалации → блокеры других Issue → те, где другие линии спотыкались → нетронутые → прочие → почти готовые последними). Логика в `state.py` `cmd_sector_queue`/`_easy()`, строка профиля выводится в начале очереди в задании. Действует со следующей сессии каждой линии |
| Владелец 27.09 11:40: «запусти Луну с суб агентами. Luna 6 Max» | новая пробная линия **CL1** в `SECTORS.json`: `engine codex`, менеджер `gpt-6-luna` effort `max`, субагенты `gpt-6-luna` `max`, `profile: hard`, участки BUILD+COGNITION; раннер `Run-Sector.ps1 -Worker CL1` (скрытый pwsh). Сравнивать с CS1/CS2 (менеджеры Sol) по слияниям, закрытым Issue, возвратам и опровержениям. Добавлена в `Lane-Status.ps1` и `Report30.sh` |
| Владелец 27.09 12:15: внешний аудитор забраковал часть работы — «заявленная гарантия подменяется более слабой проверкой» (существование receipt вместо привязки содержания, пересчёт checksum вместо проверки исходного, предсказуемое имя папки вместо владения). Его отказы (комментарии в Issue от UnknownAlienHuman) никто не читал: 37 Issue оставались CODE-COMPLETE | `v2\ingest_audit.py` в 15-минутном цикле: комментарий аудитора → `issues\<n>\AUDIT.md` + `AUDIT-IDS.txt`, CC → PARTIAL (если комментарий новее последнего CC; иначе полная перепроверка OR); `state.audit_problems()` не даёт CC без пунктов чек-листа `"audit": "<id>"`. Правило 10 в `TEMPLATE-SECTOR.md`, «Guarantee substitution» в `TEMPLATE-CCV.md`. Новые Issue аудитора вне ORDER → `audit-unknown.tsv`, вставлять `insert_after.py` (проверить, что это не PR: #2956 был PR) |
| Владелец 27.09 15:35: «Meta Code скорее всего скоро исчерпает квоту… пусть закруглялись и передавали работу»; «Long cat давай переведем на space bunny» | В идущую сессию Muse Code сообщение не передать: `muse exec` безголовый, `muse session-message` на Windows = `session_messaging_unavailable`. Рычаг — CLAIM: в 56 свободных Issue очереди MC записан CLAIM `WINDDOWN …` (список `v2\winddown-MC.txt`); менеджер MC по протоколу пропускает чужой CLAIM, а `state.claim_of()` возвращает для `WINDDOWN` «не занято», поэтому другие линии их берут. MC `active: false` — новой сессии не будет; текущая доделывает свои #1841 #2729 #370. После конца сессии — подобрать хвосты (WIP-коммит+push, `continue_branch`, снять CLAIM MC). LC: `opencode/space-bunny-free#high`, профиль easy снят, со следующей сессии LC |
| 27.09 17:31 сессия MC завершилась, раннер вышел («inactive in SECTORS.json») | Хвосты: 4 коммита MC не в main (проверка `git cherry origin/main HEAD`: `+` = нет в main) → запушены в новые ветки `fix/<n>-mc-winddown-wip` (#2663, #2798, #909, #1813; в влитую ветку не дописывать); `continue_branch` для свободных, для занятой #1813 — только заметка в REMAINING. После выхода MC CLAIM `WINDDOWN` снять (менеджеры других линий читают CLAIM напрямую): снято 39, остальные 17 уже взяли другие линии. `MC-2968`, `MC-852-tmp` — не worktree, git в них видит корневой `C:\Development\Rust`: не трогать |
| Владелец 27.09 21:00: «Откуда берутся эти 180 PR? Я просил issue делать. Почему готовых так мало?» — 182 PR с 10:00 затронули 132 Issue, готовы только 20: мои правила (90-минутный лимит, PARTIAL — нормальный итог, «лёгкое сначала», «важен объём») заставляли сдавать куски и переключаться | Режим ДОДЕЛЫВАНИЯ: `state.py` очередь = блокеры → начатые Issue по числу оставшихся пунктов (свои участки раньше чужих) → нетронутые; `TEMPLATE-SECTOR.md`: «One claimed issue = the whole issue», PARTIAL только для BLOCKED-BY #x / decision, промежуточный push раз в ~90 мин без смены Issue. Бэкап `state.backup-20260927-2100.py`. В отчётах считать законченные Issue, а не PR |
| Владелец 28.09 06:00: «Пусть документацию читают!!! Какие нахер у них там вопросы???» — MC сдала 16 NOCHANGE с «BLOCKED-BY decision / owner supply», а я предложил собрать вопросы владельцу | Вопросов владельцу нет. Из `TEMPLATE-SECTOR.md` убрана лазейка `BLOCKED-BY decision` во всех местах (правила 4, 8, 9, «Blocked item», «Finish each issue», «headless») и из заметок Command Code: PARTIAL только `BLOCKED-BY #x` (код другой открытой Issue); документы молчат → самый конкретный документ + `ASSUMPTION:` + реализовать. 121 Issue с такими ссылками получили заметку в REMAINING и сброс noprogress. Никогда не предлагать владельцу «список вопросов» |
| Владелец 28.09 10:15: «А почему так долго? Суб агенты работают? Менеджеры чем занимаются? Очень долго и очень низкое качество!» — замер по `~/.local/share/opencode/opencode.db` (read-only, `session_v2`/`session_message`: время модели = streamed−created, время инструмента = completed−ran): у линий OpenCode обычно ОДИН субагент за раз (W1: 46 за 10 ч), менеджер ждёт; у субагентов ~половина времени — «думает» модель, clippy до 230–240 раз за 10 ч (LC 4 ч сборок), чтения в 5–8 раз больше правок; CPU 90–96% | В `TEMPLATE-SECTOR.md` («Supervising»): несколько писателей параллельно на разных Issue в своих worktree; сборка один раз, когда код Issue дописан. Качество: слабые места — бесплатные модели; предложено владельцу отдать контрактные Issue Sol/Luna (платно) |
| Владелец 28.09 10:45: «Скажи менеджерам, чтобы держали по 4 агента!!! (Muse Code - 8). Это их работа!!! Сделай напоминание!!!» и «Менеджер сам контролирует, сам проверяет работу!!! Он не должен сидеть и нихуя не делать!!!» | `TEMPLATE-SECTOR.md` («Supervising»): 4 субагента ВСЕГДА (MC 8), менеджер сам опрашивает, построчно проверяет каждый дифф по пунктам и документации (4 шаблона подмены правила 10), возвращает брак, интегрирует, сдаёт — не простаивает. То же в `TEMPLATE-CCV.md` (OR: 4), `Launch-MuseCode.ps1` (8), `Launch-Codex.ps1` (4 Luna), `Launch-CommandCode.ps1` (4). Напоминание: `v2\Remind-Subagents.py` каждые 15 мин — считает РАБОТАЮЩИХ субагентов линии OpenCode (сообщение за 20 мин, не завершён) по `opencode.db`; меньше 4 → `session.prompt` живой сессии менеджера (не чаще раза в 30 мин), журнал `v2
eminders.log`. Codex/MC/CB в идущую сессию не принимают сообщений — приказ у них в заметках запуска |
| Владелец 28.09 12:00: «там должна быть steer отправка сообщений!!!» — `codex queue` кладёт сообщение в `~/.codex/queue_1.sqlite`, но `codex exec` забирает его только между ходами (не дошло за час); общий демон `codex app-server daemon start` отказывается стартовать из повышенного процесса (explorer/runas тоже High) | Линии Codex запускаются через `v2\codex_as.py run` (`Launch-Codex.ps1`): свой `codex app-server` по stdio, thread/start + turn/start, каждые 10 с входящие `workers\<линия>\logs\steer\*.txt` уходят в идущий ход через `turn/steer` (файл → `steer\sent`), текущий поток — `steer\CURRENT`. Отправить: `python v2\codex_as.py steer --inbox workers\<линия>\logs\steer --text "..."`. Проверено пробой (ответ «DONE BANANA»). CS1/CS2/CL1 перезапущены на новый запуск в 12:11. `Remind-Subagents.py` напоминает и линиям Codex через steer |
| Владелец 28.09 12:15: «делай аудит своей системы контроля агентов, изучай функционал Muse Code, OpenCode 2, Codex, Command Code. Дорабатывай сервисы/демоны/контролеры» | Итог аудита (исследование 4 субагентами, только чтение):
**Управление по движкам.** OpenCode: `opencode api session.prompt --data {"text","delivery":"steer"}` в живую сессию; сообщения читать `session.message.list` (не `session.messages` — такой операции нет); субагенты `session.list --param parentID=`; сервис перезапускает сам клиент после 3 неудачных проверок связи — опросы держать редкими. Codex: `v2\codex_as.py` (свой `codex app-server`, `turn/steer`, отвечает на серверные запросы — иначе ход висит навсегда; квота по `codexErrorInfo usageLimitExceeded`; реестр субагентов из `subAgentActivity`). Muse Code: `v2\muse_as.py` через `muse serve` (MSP: `turn/steer`, `session/read`, `userInput/request` → отмена). Command Code: мод `v2\cc-root-inbox.ts` (`--mod`, `cmd.queueMessage deliverAs steer`, реестр из `subagent_start/stop`). У всех трёх входящие `workers\<линия>\logs\steer\*.txt`, признак живой сессии `steer\CURRENT`, реестр `steer\SUBAGENTS`. Каждый проверен пробой («DONE BANANA»).
**Контролёр.** Отчёты по расписанию (CronCreate 13,43 * * * *), не ручным перезапуском. `Agent-Watch.py`: только текущая сессия менеджера, без выключенных линий, флаг «STUCK MODEL STREAM», реестры MC/CB/Codex. `Remind-Subagents.py`: OpenCode + Codex + Muse (8) + Command Code, список молчащих >30 мин в тексте. `Answer-Forms.ps1`: экранирование кавычек для PowerShell 5 (формы с пробелами не закрывались). MC и CB станут управляемыми со следующей сессии (перезапуск — только с разрешения владельца) |
| 28.09 19:42: задание по расписанию (CronCreate 13,43) за 7 часов не сработало ни разу — root 7 ч не отчитывался; автоматика (приёмка, напоминания, формы, аудит) работала. MC простояла 90 мин: ход завершился в 18:12, но `muse_as.py` ждал только `turn/completed`, а поток событий упал (`session/viewHealthChanged unavailable`); кроме того `approval/decide` отвергался (нужны `choiceId` + `requirementId`), субагенты MC ждали разрешений | Отчёты снова цепочкой фоновых `Report30.sh 1800` (перезапуск после каждого). `muse_as.py`: раз в минуту `session/read` (idle + нет activeTurnId = ход окончен), переподписка `view/subscribe` при падении потока, правильный `approval/decide` (approvedForSession), реестр убирает завершённых. MC перезапущена 19:45 |
| 28.09 20:20 root назвал W1 «зависшей: ответ модели не приходит 31 мин». Владелец: «Что значит ответ не приходит??? Проверяйте идёт работа или нет. Опрашивайте модели!!! … статусы читать и правила написать для менеджеров!!!» На деле сообщение менеджера было открыто, потому что шёл синхронный вызов `subagent`, а субагент работал | Флаг в `Agent-Watch.py`: «зависание» только если нет работающего инструмента; новый флаг «manager BLOCKED on foreground subagent call». Правило запуска субагентов в фоне и опроса — по всем движкам в `TEMPLATE-SECTOR.md` и в разделе 6; разослано всем 10 менеджерам (OpenCode steer, остальные через входящие) |
| Code-complete при неполном чек-листе: линия кладёт в `items` только сделанное, остальное — в отдельное поле (`items_not_taken`, #1141) или просто не включает (раздел Addendum, #1845) | `validate` с 27.09 03:10 считает незавершённым чек-лист с непустыми полями `not_taken/deferred/out_of_scope/skipped/excluded/not_done`; бриф требует все пункты Issue (включая Addendum) в `items`. Пропуск пунктов без отдельного поля ловит только проверка кода (субагент/OR) |
| 26.09 20:49–21:18 сервис OpenCode перезапускался по кругу (до 5 экземпляров `serve --service` сразу; в журнале `opencode.log` — «Background service is unresponsive; recovery…» от CLI-клиентов); субагенты линий терялись. Причина: `Wait-SessionIdle` в каждом из 6 раннеров раз в минуту вызывал `opencode api session.get` для КАЖДОЙ активной сессии сервиса (десятки процессов `opencode.exe` в минуту) | `Run-Sector.ps1`: кеш родителя сессии (один `session.get` на новую сессию), опрос раз в 2 мин. Раннеры OpenCode перезапускать с `-AdoptSession <sid>` (sid — первая строка последнего `logs\<линия>-sector-*.stdout.jsonl`): новый раннер сначала ждёт текущую сессию, второй не запускает. Свои вызовы `opencode api` в такой момент не делать. После перезапусков сервиса остаются «зомби»-субагенты: сервис показывает их running, но они не обновляются (LC 21:28–22:25 ждала «Continue issue #2902») → с 22:25 раннер не ждёт субагентов без обновлений 30+ мин, если основная сессия линии завершилась (не прерывает их, только не ждёт) |
| 26.09 ~20:00 на `main` появилась защита ветки (обязательная проверка `merge-compile` из #3004, strict) при красной проверке (`clippy.toml` без маршрута документации) → ни один PR не вливался (`mergeStateStatus BEHIND`, «merge: OPEN NOT MERGED») | признак: `gh api repos/…/branches/main/protection` не 404. Приёмку остановить `v2\Stop-DaemonV3-Safely.ps1` (каждый демон в безопасной точке), спросить владельца; настройки репозитория root не меняет. Владелец снял защиту → убрать `v2\merge.lockdir` и `reviews\claims\*`, вернуть PUSHED сдачам с «merge failed», перезапустить 6 потоков |
| `session.prompt` в уже завершённую сессию OpenCode **запускает её заново** → у линии две сессии сразу (W1/W2/W3, 26.09 15:15) | ID для дополнения: `sessionID` из первой строки САМОГО СВЕЖЕГО `workers\<линия>\logs\<линия>-sector-*.stdout.jsonl` (строки «waiting for session» в `sector.log` для новой сессии может ещё не быть), и перед отправкой проверить, что он есть в `session.active` |
| Проверка достижимости отбрасывала строки кода, начинающиеся со `*` (`*flight = …f(..)` — разыменование), как продолжение блочного комментария → ложное «нет вызывающих» (#1115, нашла W4) | комментарием считаются только `//`, `/*`, `* текст`, `*/`; все отклонения перепроверены, ложным было только #1115 — возвращена в code-complete с комментарием |
| Command Code (CB) не пишет квитанции документации и использовал `git stash` | правила в заметках `Launch-CommandCode.ps1`; дополнение — дописать в его `*.cmdcode.md` (API дополнений нет), но бриф он не перечитывает: #1813 сдан без квитанции после дополнения. Рабочий канал — заметка в начале `issues\<n>\REMAINING.md` каждой Issue его очереди (её он читает перед работой) |
| Python-строки с путями Windows (`\r`, `\t`, `\2` съедаются) | raw-строки, `chr(92)` или Write/Edit; проверять файл на управляющие символы |
| Скрипт находит сам себя по строке в своей командной строке | шаблон поиска собирать из частей |
| PowerShell: `$i` = `$I`, `$home` только чтение, `"$w:"` | разные имена, `${w}` |
| MSYS превращает `/doc` в путь Windows | вызывать такие API из PowerShell |
| **Мало готовых Issue при многих PR** (владелец 28.09 21:00: «десятки агентов закрывают 1-2 issue в час???»). С 12:23 до 20:49 смёрджено 100 PR, но полностью закрыли Issue только 30. Остальные: 36 PR оставили 3+ пункта, 17 — 1–2 пункта, 16 — все пункты есть, но сломан чеклист (пропавшие id, символы не на main). Ещё 18 CC снято перекрёстной проверкой OR. Итог CC +23 за 8,5 ч. По линиям: W2 24 PR / 14 готовых; CB 20 / 2; CS1 2 / 0; CS2 6 / 0; CL1 0 PR. Codex-линии парковали пункты через `BLOCKED-BY #x` / `BLOCKED-BY scope`, а CL1 шесть раз за 40 минут перепроверял #897 на main без единой строки кода | Как мерить: в `state.json` история `merged pull/N -> <state> remaining=<k> problems=<p>` с момента T, линия — по суффиксу ветки (`gh pr list --state merged --search "merged:>=T"`). Правило в TEMPLATE-SECTOR «a blocker is WORK»: `BLOCKED-BY #x` принимается только при чужом CLAIM на #x младше 3 ч, иначе менеджер сам берёт #x и пишет недостающее; `scope` — только если код назван в Work другой Issue; перепроверка main без кода запрещена; id из чеклиста не удалять. Разослано всем 9 менеджерам |
| Agent-Watch и напоминалка показывали у Codex **0 субагентов**, хотя у каждой линии работало 4, — и каждые 30 минут слали им ложные напоминания | app-server пишет все потоки (включая субагентов) в собственный лог линии `codex_as.py` (`*.stdout.jsonl`). Считать потоки с событием за 20 мин (`params.startedAtMs/completedAtMs`, `threadId`) минус поток менеджера из `steer\CURRENT`; правки = `item/completed` `fileChange`, сборки = `commandExecution` с `cargo`. Rollout-файлы `~\.codex\sessions` — только запасной вариант |
| **Повтор 28.09 21:13–22:17:** сервис OpenCode перезапускался 12 раз за 80 минут. Субагенты получали «Execution was interrupted repeatedly and will not be resumed automatically»: W2 потеряла 4 писателей, W3 дважды потеряла блокер #962, W4 — ещё двух. За 80 минут было 538 вызовов `opencode api`: `session.get` 234 и `session.active` 193 от раннеров, несмотря на кеш, и `session.form.list` 111 от Answer-Forms, который с 27.09 12:41 не нашёл ни одной формы. Каждый вызов — отдельный процесс CLI, он проверяет сервис; при CPU ~90 % проверка не проходит, и CLI перезапускает сервис («Background service is unresponsive; recovery …» в `~\.local\share\opencode\log\opencode.log`) | **Автоматика root не вызывает `opencode api` для наблюдения, только читает базу:** `v2\oc_busy.py <sid>` (busy/idle по `session_v2.time_idle` и свежести `session_message`) в `Wait-SessionIdle` раннера; `Remind-Subagents.py` определяет активные сессии по базе. Цикл Answer-Forms остановлен. `opencode api session.prompt` — только для адресных сообщений. Раннеры W1–W4 и OR перезапущены с `-AdoptSession`, сессии не тронуты. Как считать перезапуски: смена `run=` у строк `spawning process` в `opencode.log` |
| **Агенты OpenCode 74% времени ждали инструменты** (28.09, замер по `session_message`: время части `tool` от `time.created` до `time.completed`). Медианы: чтение 14 с, поиск 14 с, правка 42 с, оболочка 33 с. У Codex на той же машине чтение 0,3 с. Причина — **снимки OpenCode**: перед и после каждого вызова инструмента `git ls-files --others` / `diff-files` / `write-tree` (иногда `add --all`) по всей рабочей копии в общем теневом репозитории `~\.local\share\opencode\snapshot\<проект>\<копия>`. Это 81% всех процессов сервиса (~195 в минуту) и 454 сбоя `index.lock` в час; агенты линии стояли в очереди за одной блокировкой | В `~\.config\opencode\opencode.jsonc` задано `"snapshots": false` (ключ OpenCode 2.0.7; резервная копия `opencode.jsonc.bak-20260928-2250-snapshots`). Сервис перечитал конфиг на ходу (`config.updated`), без перезапуска. Через 4 минуты: чтение 1,9 с, поиск 3,8 с, правка 4,9 с, оболочка 5,1 с; git-снимков 0. Отмена правок внутри OpenCode больше не работает, агенты ею не пользуются. Задержка инструментов — строка `TOOL LATENCY` в Agent-Watch |
| **Контекст агентов забит мусором** (владелец 28.09 23:15: «Разбирайся с тем, что грузится агентам в контекст! Туда должна идти только полезная инфа!»). Замеры: менеджеры OpenCode слали модели 260–360 тыс. токенов на каждый шаг (сжатие только у края окна в 1 млн: `buffer` 20 тыс.); писатели разрастались до 110–180 тыс.; нить Codex CS1 — 1,28 млрд входных токенов за сессию: 4 субагента жили с 12:11 и получали Issue за Issue (контекст 160–210 тыс. из 258). Каждая нить Codex стартовала с 24–28 тыс. токенов: 17 плагинов (gmail, slack, drive, office, browser, computer-use…) и `memories`. Ветки комментариев Issue — до 95 КБ старых отчётов «Прогресс … попытка N» и служебных комментариев root. Задание менеджера — 27,7 КБ правил плюс очередь на 80 строк (46 КБ у W2) | **`issues\<n>\TASK.md`** (`v2\make_task.py`, цикл раз в 30 мин по изменившимся): тело Issue + комментарии со спецификацией (без отчётов о прогрессе, служебных комментариев root, битой кодировки и уже вошедших в AUDIT.md) + REMAINING + AUDIT + id последнего чеклиста. Агенты читают его вместо `gh issue view --comments`; для #18 это 101 → 26 КБ. **Задание** переписано: 9 КБ только действующих правил, очередь 12 строк (`limit` в `SECTORS.json`), менеджер берёт свежую очередь перед каждой Issue. Правила: один субагент — одна задача со свежим контекстом; ответ писателя не длиннее 20 строк; узкое чтение. **Сжатие контекста (limit.input / model_auto_compact_token_limit) включал 28.09 23:25 — владелец: «Я не просил включать сжатие… я просил оптимизировать инструкции и выгребать свой срач» — ОТКАТАНО в 23:32 (OpenCode — из `opencode.jsonc.bak-20260928-2355-compaction`, Codex — из `codex_as.py`). Не включать без просьбы. **Codex** (`codex_as.py`, только сервер линий): выключены 15 ненужных плагинов, `node_repl`/`github_oauth` и `memories` (`context7` через `-c` не выключать — ломает `thread/start`). **Свой мусор root вычищен:** строки «- OWNER …» из 48 REMAINING.md (копия в `backup-remaining-20260929`), дубль аудита в TASK.md, напоминания — одна строка, не чаще раза в час и только после двух низких проверок подряд (было 109 сообщений на 68 тыс. символов за 16 ч в OpenCode и по ~12 тыс. каждой линии Codex), заметки запуска Muse/CB/Codex — только особенности движка, бриф OR 9 → 4 КБ, правила кода из COMMON-RULES перенесены в бриф (один источник). Менеджерам Codex велено сдать текущие задачи, закрыть субагентов и начать чистую сессию |
| Десктопное приложение **OpenCode Beta** (работало с 27.09 15:29) слало сервису ~257 запросов в секунду `POST /api/session/<s>/permission/<p>/reply` на 646 разрешений, потерянных при перезапусках сервиса; все получали 404. Лог вырос с 28 до 63 МБ за 25 мин, сервис был загружен. Нашёл по `opencode.log` (`http.status=404` на `…/permission/…/reply`) и `Get-NetTCPConnection` к порту сервиса: единственный клиент — `OpenCode Beta.exe` | С разрешения владельца (28.09 23:00) приложение закрыто и запущено заново. Сервис отделён от приложения (его запустил CLI при «восстановлении»), поэтому остался жив, PID тот же, сессии не пострадали. Поток прекратился. Проверка: число `permission/.*/reply` в минуту в хвосте `opencode.log` |
| Владелец 28.09 23:05: «Конфигурации исправь на полные разрешения». В `~\.config\opencode\opencode.jsonc` правила `shell` и `edit` были `"effect": "ask"` | Оба правила переведены в `"allow"` (резервная копия `opencode.jsonc.bak-20260928-2310-permissions`), сервис перечитал конфиг на ходу. Остальные движки уже на полном доступе: Codex — `approval_policy="never"`, `sandbox_mode="danger-full-access"` в `~\.codex\config.toml` и в `codex_as.py`; Muse — `approvalMode allowAll`, `--disable-sandbox --trust-workspace`; Command Code — `--yolo --trust --tools-all` |
| **Два менеджера одной линии сразу** (W2 28.09: сессия 19:09 и новая 22:18). Старый раннер счёл сессию завершённой (её не было в `session.active`, хотя её субагенты работали) и запустил новую. Старую потом будили уведомления о завершении её субагентов. Обе сессии — W2, и новая очередь не мешала им взять одну Issue | С 22:40 раннер ждёт по `oc_busy.py`: пока работают субагенты, линия считается занятой, и новая сессия не стартует. Если дубль уже есть: старой сессии сообщением велено доделать своих писателей и закончить, новой — не брать её Issue |
| 29.09 00:17: сервис OpenCode перезапустился из-за `opencode api session.prompt` напоминалки (`opencode.log`: «cli starting … session.prompt», затем «unresponsive» и новый `serve --service`). Любой запуск CLI проверяет, жив ли сервис, и под нагрузкой может его перезапустить, а с ним погибают все писатели OpenCode | **Сообщения в OpenCode — только прямым HTTP к сервису, без CLI:** `python v2\oc_http.py prompt <sessionID> <text>` (`POST /api/session/<id>/prompt {"text","delivery":"steer"}`), чтение — `python v2\oc_http.py active`. Адрес, PID и пароль берутся из `~\.local\state\opencode\service.json` (их пишет сам сервис; пароль не печатать). HTTP-запрос не может перезапустить сервис. `Remind-Subagents.py` переведён на `oc_http.prompt`; проверено 29.09 01:05 на W3, W4 и OR: сообщения приняты, сервис не перезапускался. **`opencode api …` из автоматики root не вызывать вообще** |
| Приёмка не успевает (29.09 00:46: ждут 23 PR, за 30 мин слито 4; в 01:19 уже 33) — после ускорения инструментов агенты сдают быстрее. Разбор одной ветки — 9–17 мин: `Review-Branch2.sh` делает `cargo check --workspace` результата слияния (в своей рабочей копии и папке сборки слота, пересобирается изменённое и зависимые) и clippy по каждому затронутому пакету | Добавлены слоты 5, 6, 7 (`REVIEW_SLOT=5/6/7`); CPU был 26–40 %, свободно 51 ГБ. Владелец 29.09 01:35: «Сделай 10 веток приемки. Активнее работаем!» — работают слоты 1–10 (8–10 с 01:38; их первый разбор долгий: пустые `targets\ROOT-review8..10`). Длительность разбора считать по свежим файлам `reviews/*.txt` (время создания у переиспользованных файлов старое). Проверку всего пространства не убирать: она ловит поломку зависимых крейтов |
| **Все сборки роя стояли в очереди к одному серверу `sccache`** (29.09 01:50). `C:\Development\Rust\.cargo\config.toml` задаёт `rustc-wrapper = "sccache"` для всего дерева Rust. С 10 слотами приёмки и линиями: 18 процессов cargo, ~90 клиентов `sccache`, у сервера (работает с 24.09) всего 7 компиляций, `rustc` вне его — 0. Cargo по 12–15 мин без компиляции; слияний 4 за 30 мин при очереди 39. Попаданий в кеш 13 % (35 тыс. / 231 тыс. промахов, 790 тыс. некешируемых вызовов) | `C:\Development\Rust\projects\eliot-swarm\.cargo\config.toml` с `[build] rustc-wrapper = ""`: Cargo сливает конфиги по вложенности, более глубокий главнее, так что всё под `eliot-swarm` (рабочие копии линий, `ROOT-review*`) собирается без обёртки, а корневой конфиг не тронут. Проверено на тестовом крейте (`cargo build -v`: `sccache rustc` → `rustc`) и на живых процессах (дочерний сразу `clippy-driver`). Как проверить заново: дочерние процессы cargo (`sccache.exe` против `rustc`/`clippy-driver`) и `sccache --show-stats` |
| Git-процессы `rebase --continue` / `commit -e` висели 66–72 ч в `W1-1787x` и `W1-2564` (ждали редактора сообщения коммита; агентов давно нет) | Сняты 29.09 01:30. Агентам в git — только неинтерактивные команды (`-m`, `GIT_EDITOR=true`) |
| **29.09 04:18:58 сервис OpenCode перезапустил сам раннер линии.** Старт новой сессии командой `opencode run --auto` (CLI) → через 7 с «Background service is unresponsive; recovery» → новый `serve --service`. Погибли писатели всех линий, провал 04:20–04:40: чтение 14–15 с, поиск до 41 с. Кроме того, `--auto` означает, что этот клиент сам отвечает на запросы разрешений: у агентов по умолчанию `external_directory: ask`, а писатели работают вне папки сессии. Когда клиент отваливался (Transport), отвечало приложение OpenCode Beta, а после перезапусков сервиса оно слало ответы на исчезнувшие запросы — поток 404 | **Сессии линий создаются по HTTP:** `Launch-Manager.ps1` → `python v2\oc_run.py` (`POST /api/session {title, model:{id,providerID,variant}, location:{directory}, permissions}`, затем `POST /api/session/<id>/prompt {text}`). Первая строка лога — `{"type":"session.created","sessionID":…}`. Процесс живёт, пока сессия и её субагенты заняты (по базе), в конце копирует ошибку последнего ответа как `{"type":"error"}`. CLI остаётся только для продолжения существующей сессии (`-Session`). **Полные разрешения в конфиге** (`opencode.jsonc.bak-20260929-0515-permissions`): `*`, `external_directory` и `read` — `allow`, запросов разрешений больше нет. Как смотреть задержку инструментов: `ran → completed` (выполнение) против `created → ran` (очередь) у частей `tool` в `session_message` |
| Менеджеры OpenCode заканчивают ход («awaiting worker completion notifications»), пока их писатели работают: W1–W4 в 22:30. Это ожидание, а владелец запретил ждать | `Remind-Subagents.py` раз в 30 мин подталкивает такого менеджера через `session.prompt`: проверить диффы работающих писателей, подготовить следующую Issue из свежей очереди. Шлёт только при работающих писателях: тогда `oc_busy` показывает, что линия занята, и раннер не заводит вторую сессию |
| Незаконченные Issue, у которых сделаны все пункты и которые проходят проверку на текущем main, **никто не перепроверял**. #1780, #1781, #1796, #1867 и #1871 брали по 3–4 линии и сдавали NOCHANGE | `v2\revalidate_partial.py` (цикл раз в час): берёт PARTIAL с одними строками `- CHECK:` в REMAINING, без чужого CLAIM и без ожидающей приёмки. Сначала прогоняет `validate` на копии последнего чеклиста в TEMP, затем для прошедших — `finish_review(n,'revalidated')`, метку и комментарий, как `label_done` демона. 28.09 это дало +5 CC (#1781 #1867 #1796 #1871 #829). Журнал — `revalidate.log` |
| Проверка чеклиста требовала `файл.rs::символ` и вызывающий код в продакшене для пунктов про `Cargo.toml`, `Cargo.lock`, документацию, скрипты и C#. Такие пункты не могли пройти никогда: #829 перебрали 6 линий | `validate()`: если `impl` — файл не на Rust (toml/lock/md/json/yml/ps1/py/sh/cs/csproj/xaml/sln/txt/sql, допускается `::…`/`#…`), доказательство — наличие файла на main (`git ls-tree`), вызывающий код не требуется |
| Узловые блокеры: #18 держит 10 Issue, #962 — 5, #1746, #1790 и #866 — по 4. Циклы: #18↔#1719, #1790↔#1929, #787↔#866, #1724↔#1725, #2561↔#2729 | Считать по `BLOCKED-BY #x` в REMAINING/REPORT открытых Issue. Раздавать узлы сильным линиям явным указанием; цикл отдавать одной линии целиком |
| Две линии делают одну Issue: MC взял #838, когда у W2 с 20:02 уже работал писатель W2-838 (CLAIM-файла W2 не было, была только строка в STATUS.md) | перед захватом проверять `issues\<n>\CLAIM` и строки `#<n> CLAIM` в `workers\*\STATUS.md` за 3 ч; MC получил указание снять дубль |
| Вопрос в идущий поток Codex (steer «ROOT ANSWER») CS1 и CS2 дважды проигнорировали: продолжали отчитываться о работе | `python v2\codex_ask.py <threadId> <файл-вопрос>`: отдельный `codex app-server`, `thread/fork` потока менеджера (весь его контекст), sandbox read-only, вопрос копии, печать ответа, копия архивируется. Живой поток не затрагивается. threadId — первая строка `thread.started` в `workers\<линия>\logs\*.stdout.jsonl` |
| Codex давал в 3–14 раз больше кода на закрытый пункт и почти не закрывал пункты. Копии потоков CS1/CS2 (29.09 07:40) объяснили: пункт требует сквозной путь через владельцев и данные, которых нет на main (производитель запроса Human, исходные байты Observe в ORS, исходная квитанция операции), а «Exclusive mutable scope» Issue (такая оговорка в 65 TASK.md) запрещает трогать чужие файлы; подмена запрещена — линия пишет инфраструктуру и оставляет PARTIAL/BLOCKED. То же у OpenCode: W3 #952 «RetainedArchiveMember is constructed NOWHERE» | Владелец 29.09 08:00 «делай». `TEMPLATE-SECTOR.md` разд. 3: недостающая предпосылка (тип, порт, данные владельца, производитель, вызов) — работа линии, в крейте владельца, даже вне области Issue; область Issue — место начала, не забор; `BLOCKED-BY #x` только при чужом CLAIM < 3 ч и только для этого пункта. Разд. 5 шаг 2: проверка готовности (`READINESS` в REPORT.md) до запуска писателей — недостающее становится первыми задачами. Разд. 1: `docs_read` один раз на Issue по всем путям, писатели получают готовый пакет. Дополнение отправлено живым W1–W4 (`oc_http.py prompt`), MC, CB, CS1, CS2 (steer-входящие) |
| У линий Codex не было codebase-memory: `C:\Development\Rust\.codex\config.toml` не действует, потому что cwd линии — worktree (`M-CS1`, `CS1-<n>`) со своим `.git`, на нём поиск настроек проекта останавливается. В потоке CS1 стартовали только cloudflare-api, codex_apps, cua_repl и eliot (failed) | `v2\codex_as.py` задаёт `mcp_servers.codebase_memory.*` через `-c` (command, cwd, env `CBM_CACHE_DIR`, enabled_tools). Проверено пробным потоком: `codebase_memory ready`; индекс — main (отстаёт на 1 коммит, обновляется сам). Действует для сессий Codex, запущенных после 29.09 08:00; живые не перезапускались. Отключить cloudflare-api через `plugins."cloudflare@openai-curated[-remote]".enabled=false` не вышло: имя плагина не совпало |
| 29.09 09:00 владелец: «подключаться к app-server штатным образом». Документация: сообщение в идущий ход есть только у app-server (`turn/steer`); `codex exec` его не принимает, `codex queue` = следующий ход (проверено: в exec не дошло); общий демон `codex app-server daemon start|bootstrap` на Windows стартует только из НЕповышенного терминала («start the Windows daemon from a non-elevated terminal»). На машине UAC выключен (`EnableLUA=0`), все процессы High — демон невозможен; `app-server --listen unix://` из повышенного процесса: «socket directory is not private to the current user» | Владелец 29.09: «Включи UAC тогда. Используем штатный демон». UAC включает владелец сам (системная настройка безопасности, root её не меняет) + перезагрузка. После неё: демон `codex app-server daemon start` из обычного терминала, линии Codex — потоки этого демона (видны в `codex agents` и `codex --remote`), указания — `turn/steer` через `codex app-server proxy`, вопросы — `codex exec fork`, настройки линий — профиль `~\.codex\lane.config.toml` (`-p lane`). Данные линий от приложения не отделять (владелец: «Не трогать») |
| 29.09 09:30 PR #4030 (#3004) и #4033 (#835) владельца: `merge-compile` красный. Причины: (1) шаг «Verify shared documentation evidence» требует в теле PR блок `eliot-doc-read-evidence:v2` И Issue из `.github/work-unit-cohort.toml` (11 записей, «Do not hand-edit»), иначе `CHECKLIST_REQUIRED_MISSING`; (2) на main MergeCompile (`scripts/verify.ps1`) падает на `dependency-policy-offline` с 2026-08-22: на раннере CI нет `.eliot/dependency-policy/surrealdb/` (#1229/#3004). Защиты веток и rulesets нет; демон вливает с той же красной проверкой (#4034–#4036). В #4033 фраза «does not close #835» считалась закрывающей | Субагент проверил по существу (верно, без заглушек), root влил по приказу владельца: `dd8b8d29`, `4a50413c`; тело #4033 → «Implements #835», #835/#3004 открыты. Поставка входов SurrealDB на раннер — открытая работа (HANDOFF §4 п.8) |
| 29.09 11:15 сворачивание: после `WIND-DOWN done` всех линий в OpenCode осталась активной сессия-субагент (#1727) СТАРОЙ сессии W2 (её менеджер закончил в 09:12, раннер уже запустил новую); когда субагент закончил, OpenCode вернул результат родителю и старая сессия-менеджер W2 проснулась (clippy по `eliot-context-assembly`). `Stop-DaemonV5-Safely.ps1` останавливает только корневые процессы демонов — их `Review-Branch2.sh` + cargo (34 процесса) продолжали работу без хозяина | Сворачивание проверять по `oc_http.py active-ids` (а не только по STATUS.md): указание отправлять и живым субагентам, и старым родительским сессиям (ждать `oc_busy.py` = idle). После остановки демонов завершать деревья `Review-Branch2.sh`, у которых нет родителя (проверки, ничего не вливают), и удалять `merge.lockdir`. Прерванный Make-PR (#844 → PR #4090) безопасен: Make-PR берёт открытую PR ветки |
| 29.09 14:25 после перезагрузки с UAC: `codex app-server daemon start` (1) встроенный CLI приложения — «this CLI has no complete local package»; (2) отдельный CLI из оболочки root — «cannot launch detached daemon; … Access is denied» (объект-задание Claude); (3) `codex app-server proxy` на JSON-строки молчит — сокет ждёт WebSocket. Раннеры W2–W4 7+ минут не начинали сессию: `oc_busy.py` при занятой базе только что запущенного сервиса OpenCode отвечает busy (перепроверка раз в 2 мин) | (1) отдельный CLI `%LOCALAPPDATA%\Programs\OpenAI\Codex`; (2) запуск через WMI `Win32_Process.Create`; (3) в `codex_as.py` клиент RFC 6455 поверх stdio proxy. Раннерам — просто ждать |
| 29.09 14:40 владелец: «сумасшедший спам консолью». После перезагрузки сервис OpenCode поднял root через приложение (`OpenCode Beta.exe` → `opencode-cli serve`, без консоли), демон Codex — `daemon start` (отсоединён): каждая команда агента и каждый MCP-сервер открывали окно Windows Terminal. До перезагрузки сервис OpenCode стартовал из скрытого окна раннера и команды наследовали скрытую консоль | 14:50–14:55 (разрешение владельца на перезапуск): Codex — `app-server --listen unix://` и OpenCode — `opencode-cli serve --service` запущены через WMI с `ShowWindow = 0`; приложение OpenCode открыто после и подключилось к сервису; `daemon pid-update-loop` и прокси прежних клиентов CS1/CS2 завершены. Итог: 0 видимых окон консоли за 60 с опроса; OpenCode поднял 25 сессий; CS1/CS2 начали новые потоки; все 9 линий с субагентами (§13 п.0) |
| 29.09 14:50 «субагенты Codex на Sol»: в заголовке файла сессии субагента (`session_meta`) записана модель родителя; фактическая модель — в `turn_context` | Проверять модель только по `turn_context`: все 11 субагентов CS1/CS2 — `gpt-6-luna`, effort max |
| 29.09 14:48 в отчёте `Agent-Watch.py` раздел TOOL LATENCY падал («'int' object has no attribute 'execute'»): подсчёт закрытых пунктов в разделе OUTPUT затирал переменную `cur` (курсор БД OpenCode) | переменная переименована (`left`), раздел снова считает медианы |
| 29.09 15:10 владелец: «Для Antigravity стоит запуск для обслуживания? … На Github уже 300+ веток». Раннер обслуживания умер при перезагрузке (его не было в плане перезапуска HANDOFF §3), слот 15:00 пропущен; удаление веток было ручной работой root, Antigravity ветки GitHub трогать запрещено | `cleanup\Prune-Branches.py` (§11) встроен в Run-Maintenance после Fold-ArchiveWip; раннер поднят; 303 → 201 ветка. В плане перезапуска (§13 п.5, HANDOFF §3 п.7) раннер обслуживания записан явно |
| 29.09 15:33 в `daemon.log` Traceback: `state.py merged 1935` → `PermissionError: Access is denied` на `issues\1935\CLAIM` — линия W1 создала CLAIM ПАПКОЙ (`CLAIM\W1`), `os.remove` на папке падает; PR #4120 влит, но результат не записан (PUSHED уже снят). Не последствие UAC | `cmd_clear_flags` удаляет и папку; `state.py merged 1935 pull/4120` выполнен повторно (finish_review падал на первом шаге — двойной записи нет) → PARTIAL remaining=1. При Traceback в отчёте смотреть последнюю строку и повторять `state.py merged <n> pull/<pr>` для влитых |

---

## 15. Кандидаты для Swarm manager в Eliot

- Рабочая единица — Issue с машинно проверяемым чек-листом (MET/TEST-PHASE, пункты не исчезают, `путь::символ`).
- **Проверка достижимости** заявленного кода до точки входа — дешёвый фильтр самой частой лжи исполнителей.
- Единый порядок с зависимостями + сектора + захват; линия, вставшая по квоте, отдаёт сектор; очередь — свежий снимок.
- Исполнитель — новая сессия на каждый снимок; всё состояние в файлах/БД, не в контексте модели.
- Приёмка по результату вливания (дельты таргетов/clippy/fmt, красные флаги), параллельные потоки с захватом и общей
  блокировкой вливания, кэш результатов main по SHA, перепроверка при сдвиге main в тех же крейтах.
- Перекрёстная проверка другой моделью с контрпримерами; опровержение возвращает Issue автоматически.
- Никаких прерываний живых сессий: только ожидание, дополнения и ответы на вопросы-формы.
- Обслуживание диска и веток по расписанию с журналом и архивом WIP.
