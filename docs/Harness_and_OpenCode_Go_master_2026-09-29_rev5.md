# Harness для менеджера агентов: Windows, OpenCode Go, Goal, субагенты и телефон

**Редакция 5: 29 сентября 2026.** Пятый проход сфокусирован только на самостоятельных harness/runtime, без бонусов за VS Code. Добавлены и проверены по исходникам **CodeGraff (graff) + Harness, Sinew, Pi-Go и Tatsu Code**. Главная находка — CodeGraff: маленькое Zig-ядро с реальным controller-driven `/goal`/`/loop`, process-isolated subagents, MCP, approvals и локальными wakeups; отдельный Harness даёт Rust/GPUI desktop и headless daemon. Главный блокер — текущий generic router умеет только Chat Completions и потому не подключает Muse Spark 1.3 Contributor через OpenCode Go Responses.

**Целевая задача:** самостоятельный Windows harness удерживает долгую цель, делегирует работу, получает и проверяет результаты, умеет ждать, восстанавливаться и подключать OpenCode Go / Meta Muse. GUI полезен только если принадлежит самому runtime или является его first-party клиентом. Предпочтительны прозрачное Rust/Go/Zig-ядро, fail-closed routing, один writer в `main`, read-only auditors и отсутствие скрытой модификации системы.

## 1. Исправленный вывод после пятого прохода

### 1.1. Самый интересный новый кандидат — CodeGraff + Harness

CodeGraff — не очередной frontend к OpenCode. Это самостоятельный Zig harness с собственным model/tool loop. В нём подтверждены:

- `/goal`, `/loop`, pause/resume/status/clear;
- controller-authorized continuation с hard cap 25 ходов на автономный run;
- persistent objective/checklist epoch и двухступенчатый completion gate;
- one-level foreground/background subagents, workflows, per-child model/provider/effort;
- enforcement read-only children, approvals и запрет self-privilege-escalation;
- MCP, ACP, HTTP/NDJSON serve и headless execution;
- durable relative one-shot schedule records.

Отдельный **Harness** — first-party Rust/GPUI desktop-клиент с Windows portable build, локальным режимом без аккаунта и `harness headless`. Это наиболее близкая из новых систем к требованию «нормальный строгий harness, GUI, Goal, subagents, timers, phone-ready».

Но текущую задачу он ещё не закрывает: `.graff/.config.router` всегда строит `/chat/completions`, тогда как Muse Contributor в OpenCode Go требует `/responses`. Кроме того, completion всё ещё не проходит независимый protected verifier; run credits/deadline не переживают restart автоматически; полноценный recurring cron отсутствует.

### 1.2. Sinew — лучший новый Rust desktop prototype, но Goal зависит от открытого GUI

Sinew имеет Tauri/Rust desktop, persistent Goal state, configurable subagents, flat swarm 2–8 peers, MCP и encrypted remote PWA. Однако automatic Goal continuation запускает React-effect открытого `ChatPane`, а не backend daemon. Закрытый/неактивный GUI не является always-on runtime. Completion сообщает та же рабочая модель, общего Goal budget не найдено, custom OpenAI-compatible provider/OpenCode Go пока отсутствует.

### 1.3. Pi-Go и Tatsu полезны, но не являются искомым manager

- **Pi-Go** — чистый Go single-binary harness с сильными process subagents: single/parallel/chain, до восьми, optional worktree isolation. Его OpenCode Go provider действительно понимает разные wire protocols, но каталог hard-coded и Muse Spark 1.3 отсутствует. Persistent Goal и scheduler не найдены.
- **Tatsu Code** — аккуратный Windows desktop harness с permission system и Task Agents, но закрытый, без MCP, persistent Goal, scheduler, phone и OpenCode Go/Muse path.

### 1.4. Новый расклад

| Задача | Текущий лидер | Почему | Блокер |
|---|---|---|---|
| **Общий standalone manager независимо от провайдера** | **CodeGraff + Harness** | Самый цельный набор Goal/controller, subagents, approvals, MCP, schedule, headless и native desktop | Нет OpenCode Go Responses/Muse; нет independent protected verifier; молодой runtime |
| **Обязательный OpenCode Go + Muse** | **Codex + OpenCodex** | Наиболее глубоко исследован transport, model catalog, session identity и mixed routing | Proxy complexity; v2 delegation/mobile/provider edge cases |
| **All-in-one open Rust desktop** | **Sinew** | Tauri/Rust, Goal, swarm, MCP, remote PWA, normal GUI | Goal continuation UI-driven; нет Go/custom provider; молодая реализация |
| **Интерактивный GUI parent→workers→review** | **Delta** | Собственный native harness, custom profiles, excellent review/history, documented Go provider | Нет Goal/scheduler/MCP/permissions/sandbox; closed/unfree runtime |
| **Durable local wakeups/recurring cron** | **Kilo CLI** | Goal связан с wakeup/cron и session tree | Self-reported completion; TS/Bun; Windows без sandbox |
| **Родной Muse runtime** | **Muse Code + Helicon** | Native Muse Goal/loop/workflows | Лучше подтверждён при прямом Meta-доступе, не как universal Go adapter |

### 1.5. Практический вывод для текущей задачи

**Переезд с Codex/OpenCodex пока не оправдан**, если обязательны OpenCode Go и Muse Contributor. Но CodeGraff + Harness теперь главный кандидат на замену после появления Responses/custom-provider adapter. Архитектурно он ближе всего к требуемому manager: one-level delegation достаточно, read-only workers enforceable, Goal controller имеет finite run semantics, а desktop/headless принадлежат тому же продукту.

## 2. Как оценивались функции

Метки доказательств:

- **CODE** — прочитан конкретный путь исходников.
- **DOC** — функция описана в официальной документации.
- **ISSUE** — есть конкретное пользовательское воспроизведение или maintainer issue.
- **INFERENCE** — вывод из нескольких подтверждённых механизмов; не выдаётся за выполненный нами E2E.

### 2.1. Настоящий Goal

Goal считается полноценным только когда:

1. objective сохраняется отдельно от текущего prompt;
2. после idle runtime решает, продолжать ли работу;
3. Pause/Stop имеет ясную семантику;
4. compaction и resume не теряют objective;
5. есть пределы токенов/ходов/времени;
6. после restart явно известно: продолжится, станет paused или останется stranded;
7. completion не равен простой фразе модели «готово» без оговорки.

### 2.2. Настоящий менеджер агентов

Менеджер должен иметь инструменты:

- spawn;
- status/list;
- steering/follow-up;
- result retrieval;
- cancellation;
- model/role/permission selection;
- контроль общего лимита fan-out.

Параллельные сессии, которые запускает человек, и multi-run одной задачи — полезны, но это не model-driven management.

### 2.3. Таймеры

Различаются:

- sleep/reminder внутри процесса;
- durable one-shot wakeup;
- recurring cron;
- scheduler, создающий новую сессию;
- облачная automation на другом checkout.

Для исходной задачи наиболее ценен **durable local wakeup, связанный с тем же Goal**.

### 2.4. Самостоятельный harness

Продукт считается harness, только если он сам владеет model request loop, tools, session state, cancellation и subagent lifecycle. Editor panel, ACP client или task board не считается harness, когда фактическое исполнение принадлежит Codex, Claude Code, OpenCode или другому нижнему runtime.


### 2.5. Независимая приёмка результата

Ни Codex, ни Kilo, ни Sinew, ни текущий CodeGraff не дают полной гарантии «готово только после protected verifier». Различаются три уровня:

1. **self-report** — рабочая модель сама ставит `complete`;
2. **controller gate** — harness требует checklist/evidence/double-check, но окончательное заявление всё ещё делает та же модель;
3. **protected acceptance** — отдельный controller запускает неизменяемые implementer-агентом тесты/политику и только затем принимает результат.

CodeGraff сейчас находится на втором уровне. Реальный reference третьего уровня найден в benchmark-harness **SWE-agent + SWE-bench**: агент лишь submits patch, а отдельный evaluator запускает held-out FAIL_TO_PASS/PASS_TO_PASS tests в изолированном окружении. Это полезный design reference, но не подходящий пользователю Windows desktop manager: Python/research stack, нет нормального GUI/phone и универсального local project workflow.

## 3. Сводная матрица только самостоятельных harnesses

| Harness | Собственный loop | Goal | Subagents | Timers | MCP | OpenCode Go / Muse | Security | Windows / phone |
|---|---|---|---|---|---|---|---|---|
| **CodeGraff 0.0.302.10 + Harness 0.2.99** | **Да, Zig** | **Persistent objective + controller loop + checklist/double gate** | **One-level foreground/background, workflows, per-model/provider** | Durable relative one-shot wakeups; не recurring cron | **Да** | Custom router только `/chat/completions`; **Muse не работает** | Approvals, enforced read-only children, policy-file protection; нет OS sandbox | Rust/GPUI Windows desktop, headless; iOS/sync есть, remote laptop viewer ещё развивается |
| **Sinew 0.1.51** | **Да, Rust/Tauri** | Persistent state, но continuation выполняет открытый React UI; self-report | Configured subagents + flat swarm 2–8 | Scheduler не найден | **Да** | Нет custom provider/Go; feature request открыт | Tool toggles/checkpoints; PowerShell не sandboxed | Native Windows GUI + encrypted PWA relay |
| **Delta 0.17.1** | **Да** | Нет отдельного persistent objective/evaluator | Worker/Scout/Reviewer/custom profiles | Нет scheduler/cron | Roadmap | Documented Go provider; exact Muse transport opaque | **Нет permissions и sandbox** | Native standalone + web/mobile sync/control surfaces |
| **Codex + OpenCodex** | Да | Native Goal | Native agents | Desktop automations, не Goal-linked cron | Да | **Наиболее глубоко адаптирован** | Codex sandbox/permissions; proxy отдельно | Native Codex app + native remote control |
| **Kilo CLI** | Да | **Persistent `/goal`** | Foreground/background/nested | **Durable wakeup + recurring cron** | Да | Session-header paths fixed; exact Muse E2E ещё ограничен | Windows без sandbox | CLI/TUI + mobile control |
| **Muse Code** | Да | Native Goal | Nested agents | `/loop`/cron | Да | Direct Meta лучше подтверждён, чем Go | Closed runtime | Native CLI/TUI; Helicon GUI/web |
| **Pi-Go 0.2.4** | **Да, Go** | Нет | **Single/parallel/chain, separate processes, max 8** | Нет | Да | Multi-protocol Go provider, но hard-coded catalog без Muse; session header не найден | Rooted file tools; shell требует policy | Windows single binary/TUI; phone нет |
| **Tatsu Code** | Да | Нет persistent Goal | До 5 Task Agents | Нет | **Нет, deliberate native plugins** | Нет Go/Muse path | Сильные permissions/guardrails | Windows portable GUI; phone нет; closed source |
| **OpenChamber** | OpenCode loop + Goal wrapper | Есть, lifecycle спорный | OpenCode agents | Cron/new sessions | Через OpenCode | Chat works; auxiliary paths historically failed | Нет strong isolation contract | Electron + Android/iOS/PWA |
| **jcode** | Да | Durable initiatives; auto-loop не доказан | Да | Ограниченно | stdio | Muse protocol blocker upstream | Rust, maturity lower | Windows beta/TUI |
| **Goose** | Да | Recipes, не persistent Goal | Да | Scheduler | Да | Mixed-protocol blocker | Rust | Desktop |
| **Oh My Pi** | Да | Goal state/prompt | Strong nested/background | Нет confirmed calendar scheduler | Да | Требует E2E | Child yolo/routing bugs требуют проверки | TUI |
| **Crush** | Да | Нет | Да | Нет | Да | Generic provider only | Go | TUI |
| **VT Code** | Да | Automation policy, не Goal | Да | Scheduled runs | Да | Не подтверждён | Rust | TUI/ACP |

## 4. CodeGraff (graff) + Harness: сильнейшая новая архитектура manager

### 4.1. Что это

**CodeGraff/graff** — самостоятельный Zig harness. Текущий проверенный stable — `v0.0.302.10` от 29 сентября 2026. Windows x64/ARM64 archives имеют размер около 3.5–3.8 MB и не требуют Python, Node или JVM. Он поддерживает интерактивный TUI, one-shot/headless prompt, JSON event stream, HTTP/NDJSON `serve`, ACP и MCP server surfaces.

**Harness** — отдельный first-party desktop/control application того же проекта: Rust + GPUI, local-first, Windows portable `.exe`/`.zip`, macOS/Linux builds и `harness headless`. Текущий release — `0.2.99`; Windows x64 artifact опубликован. Это не editor extension и не Electron wrapper.

### 4.2. Goal действительно controller-driven

Graff различает standing objective и конкретный autonomous run:

- `/goal <objective>` сохраняет objective и запускает autonomous run;
- `/goal pause`, `/goal resume`, `/goal status`, `/goal clear` управляют lifecycle;
- `/loop <prompt>` запускает тот же controller без standing objective;
- optional duration (`/goal 30m ...`, `/loop 2h ...`) ограничивает wall-clock run;
- controller, а не обычный model stop, решает, авторизовать ли continuation;
- hard guard — максимум **25 continuation turns** на один run;
- при активной background shell/subagent работе loop переходит в `holding` и продолжает после completion wake;
- deadline передаётся children с reserve margin для интеграции результата.

Persistent Goal state содержит objective, status и checklist epoch. Чтобы закрыть Goal, модель должна иметь актуальный checklist без open items. Если checklist отсутствует/неполон, первая попытка completion отклоняется и требует second check; только повторное подтверждение может завершить run.

Это существенно строже, чем простое «модель вызвала complete». Но **независимого verifier нет**: checklist и second completion всё ещё контролирует implementer-модель. Issues с durable attempt ledger и protected acceptance contract закрыты как not planned/backlog boundary, а не shipped feature.

### 4.3. Restart semantics — важное ограничение

Objective/checklist/session сохраняются. Но `LoopClock`, continuation credits и текущий `holding` — run-local memory. После process restart standing Goal можно увидеть/resume, однако interrupted run не продолжает работу сам как durable job state machine.

Следовательно:

```text
persisted objective ✅
automatic active-run recovery after crash ❌
```

`harness headless` способен держать engine живым, но это operational mitigation, не crash-safe attempt ledger.

### 4.4. Таймеры

Graff пишет one-shot schedule records в:

```text
.graff/schedule/<id>.json
```

`/schedule` и tool `schedule_task` принимают relative delay в секундах/минутах/часах (до семи дней). Когда due time наступает, prompt инжектируется как новый user turn. Claim записывается на диск, чтобы один due item не выполнился дважды.

Ограничения:

- это **one-shot wakeup**, не recurring cron expression;
- stopped process Windows сам не запускается: overdue record будет claimed после следующего старта/idle tick;
- schedule root-only;
- отдельная Windows Task Scheduler/service integration не найдена.

### 4.5. Subagents и model routing

Подтверждены:

- один уровень children — достаточный для схемы manager→workers;
- foreground и detached/background execution;
- stable job IDs, status/result retrieval и feedback steering;
- per-spawn/per-persona model, provider, effort, role и isolation;
- queue rather than fail when concurrency cap занят;
- workflows с parallel tasks и последовательными phases;
- shared cwd или isolated worktree;
- explicit cross-provider subagents.

Read-only child enforced runtime-gate:

- блокируются write/edit/imagegen/peer mutators;
- shell допускается только для распознанных read-only commands;
- destructive Git для children запрещён;
- model не может отредактировать `.harness/settings.json`, чтобы расширить собственные approvals.

Для режима пользователя «только `main`» оптимальная схема:

```text
root manager          shared cwd, no direct edits unless needed
  ├─ scout            read_only=true, shared cwd
  ├─ reviewer         read_only=true, shared cwd
  └─ writer           shared cwd, единственный mutating child
```

### 4.6. Permission model и security

Плюсы:

- root approval gate перед writes, non-read-only shell и external MCP actions;
- plan mode технически запрещает mutations;
- pre-approved tools/commands отдельно конфигурируются;
- harness policy file защищён от model writes;
- provider API keys теперь удаляются из environment tool children;
- current `shell` alias проходит тот же approval/destructive-git gate, что legacy `bash`;
- private reusable templates проходят local secret scan и отдельное consent.

Критические defects с inherited provider keys, shell approval bypass, parallel-shell heap corruption и truncated parallel tool calls были публично зарегистрированы и закрыты до текущего release. Это полезный audit trail, но одновременно показывает молодость runtime. Остаётся открытым report о SIGSEGV ACP process при concurrent network activity; OS-level sandbox нет.

### 4.7. OpenCode Go / Muse — текущий блокер

Graff поддерживает workspace router через `.graff/.config.router`, но source жёстко строит:

```text
<base_url>/chat/completions
<base_url>/models
```

Credentials берутся из environment/secure store, inline secrets отвергаются. Для обычного OpenAI-compatible endpoint это аккуратно, но **Muse Spark 1.3 Contributor требует `/responses`**, поэтому текущий router непригоден.

Итого:

- heterogeneous providers/subagents внутри Graff — да;
- OpenCode Go chat-models через generic router — потенциально да;
- Muse Contributor через OpenCode Go — **нет в текущем коде**;
- нужен per-model transport (`chat`/`responses`/`messages`) и stable `x-opencode-session` на всех inference/auxiliary paths.

### 4.8. Harness desktop, headless и телефон

Harness работает локально без аккаунта. Engine запускается внутри app или через `harness headless`; account sync включается отдельно. Provider credentials остаются на host device, но при sync chat/workspace metadata отправляются на `edge.codegraff.com`.

Есть iOS companion и SwiftUI code, однако полноценный remote-control viewer существующей laptop session (`list machines → attach → send/answer/cancel/reattach`) всё ещё отслеживается отдельным epic. Нельзя считать текущий iPhone client равным зрелому Codex/Kilo remote control. Android не подтверждён.

Trusted remote device потенциально может читать/писать workspace, а option **Show ignored files** раскрывает ignored files, включая `.env`. Эту функцию нельзя включать по умолчанию.

### 4.9. Зрелость и вердикт

Проект появился в июне 2026, быстро выпускает `0.0.x`, имеет сотни stars, но мало независимых long-run reports. Open issue set содержит реальные race/ACP/background-job defects. Non-standard modified AGPL license требует отдельной оценки для коммерческого использования.

**Вердикт:** лучший новый general-purpose standalone manager architecture. Для пользователя он становится первым кандидатом **после** добавления native OpenCode Go Responses/Muse adapter. До этого — полезен для других providers, но не заменяет текущий Codex/OpenCodex path.

## 5. Sinew: open Rust/Tauri desktop с Goal и swarm

### 5.1. Сильные стороны

Sinew `0.1.51` — MIT/open-source Tauri 2 + Rust desktop для Windows/macOS/Linux. У него есть:

- Act, Goal и Plan modes;
- отдельная модель для каждого mode;
- per-subagent models;
- configurable subagents;
- flat peer swarm 2–8 agents с task board, dependencies и messages;
- MCP lazy tool loading;
- skills, AGENTS.md/DESIGN.md, compaction и rollback/checkpoints;
- нормальный Monaco/xterm UI;
- optional encrypted remote PWA/relay.

### 5.2. Goal persistent, но не backend-owned

Goal status/objective сохраняются вместе с conversation в SQLite (`Idle/Active/Paused/Complete`). Working model получает continuation prompt и может вызвать `update_goal`.

Однако следующий turn запускает React effect `ChatPane`, когда UI видит:

```text
Goal = Active
session = idle
не было continuation для текущего history key
```

Это означает:

- app и relevant view должны быть живы;
- backend daemon/restart recovery не подтверждены;
- закрытый GUI не продолжает Goal;
- completion self-reported;
- общий hard turn/time/token budget Goal не найден.

Состояние persistent, execution lifecycle — нет.

### 5.3. Remote и security

Remote relay описан как outbound WebSocket с end-to-end encrypted frames, pairing QR и self-host option. Это лучше открытого local port, но полный cryptographic/security audit не выполнялся.

File operations проверяют workspace root, snapshots/rollback присутствуют. Shell на Windows запускает PowerShell 7 и способен обращаться вне workspace; OS sandbox не найден. Tool toggles уменьшают surface, но не эквивалентны Codex sandbox.

README проекта прямо предупреждает, что Claude Code/Antigravity subscription OAuth reserved for first-party clients; проект старается выглядеть как first-party session. Для строгой sanctioned configuration такой путь неприемлем — использовать только официально разрешённый login/API route.

### 5.4. OpenCode Go

Поддержаны Anthropic, OpenAI, Google, Kimi и OpenRouter. Custom OpenAI-compatible provider/OpenCode Go отсутствует; feature request открыт. Поэтому Sinew не участвует в финальном Go/Muse shortlist, несмотря на хороший architecture/GUI.

### 5.5. Вердикт

**Лучший новый open-source Rust desktop prototype**, особенно если нужен flat swarm и нормальный GUI. Но он молодой, Goal UI-driven, no scheduler и no Go/Muse. Следить за backend-owned Goal runner, custom provider и hard budgets.

## 6. Pi-Go: чистый Go harness, но не manager и не Muse client

Pi-Go `0.2.4` — cross-platform single Go binary с checksums/SBOM. Подтверждены sessions, MCP, hooks, skills, compaction, provider abstraction и strong subagent tool:

- `single`;
- `parallel` до восьми tasks;
- `chain` до восьми stages;
- отдельный process на child;
- optional isolated worktree;
- status/session identifiers и concurrency pool.

OpenCode Go provider действительно выбирает transport per model (`chat`, `responses`, `messages`) вместо одного universal endpoint. Но catalog hard-coded и Muse Spark 1.3 Contributor отсутствует; unknown models rejected. В source также не найдено явного `x-opencode-session` implementation, поэтому нельзя автоматически приравнивать Pi-Go к официально validated client `Pi`.

Persistent Goal, scheduler/cron, desktop GUI и phone client не найдены. **Verdict:** хороший low-level fast worker harness, не верхний manager.

## 7. Tatsu Code: аккуратный Windows harness без автономного control loop

Tatsu Code — closed-source Windows portable application. По официальной документации:

- signed single executable, no installer/admin/registry modification;
- providers: ChatGPT, Claude CLI, OpenRouter и local models;
- up to five parallel Task Agents с per-agent models;
- Cross-Agent Relay/Cross-Talk через loopback;
- granular permission model, read-before-edit и destructive-command guards;
- local encrypted credentials, no advertised vendor telemetry/server dependency.

Ограничения для текущей задачи:

- persistent Goal/evaluator не найден;
- scheduler/cron не найден;
- MCP deliberately replaced by built-in plugins;
- CLI-backed providers cannot be Task Agents из-за невозможности надёжно ограничить tools;
- OpenCode Go/Muse path отсутствует;
- phone/mobile отсутствует;
- source audit невозможен.

**Verdict:** clean interactive Windows harness, но не autonomous manager.

## 8. Delta: отдельный native harness, а не Zed editor

### 8.1. Что это на самом деле

Delta — отдельное приложение, построенное вокруг **thread = agent conversation + repository checkout**. Оно использует DeltaDB для непрерывной записи разговора, edits и review history. Public beta вышла 16 сентября 2026; текущая стабильная версия — **0.17.1 от 28 сентября 2026**. Есть отдельные native builds для Windows x64/ARM64, macOS и Linux.

Это не «Zed с вкладкой AI». Zed остаётся редактором, а Delta — conversation-first agent environment. Открыть checkout в Zed можно, но исполнение агента не зависит от Zed или VS Code.

### 8.2. Собственный harness подтверждён

Официальная команда публикует результаты, где Delta обозначен отдельным harness и сравнивается с Codex, Claude Code, Pi, OpenCode и другими. На Terminal-Bench 2.1 их собственные прогоны показали сопоставимую или лучшую pass rate для нескольких моделей. Это подтверждает наличие собственного system prompt/tool loop, но **не является независимым benchmark**: Delta runs выполняла сама команда Zed, а результаты конкурентов брались из FrontierHarness.

### 8.3. Subagents реализованы полноценно

Delta имеет built-in profiles:

- **Worker** — implementation с полным набором tools;
- **Scout** — research по code/web;
- **Reviewer** — проверка уже сделанных изменений.

Parent может делегировать сам либо только по явной просьбе. По умолчанию разрешены четыре concurrent subagents на parent и восемь на процесс. У каждого profile настраиваются model, thinking effort, prompt/system prompt и mode:

- `isolated` — отдельная копия, automatic merge только после successful completion;
- `shared` — работа прямо в checkout parent.

Parent получает final result, видит statuses, может отправить follow-up и остановить конкретного subagent. Agents могут обмениваться сообщениями между threads.

Существенная оговорка: Scout и Reviewer не имеют file-edit tool, но могут менять файлы через terminal. Следовательно, они **не read-only** и не подходят как независимые auditors без внешней filesystem/shell policy.

### 8.4. Модели и OpenCode Go

Delta официально поддерживает API-key connections для Anthropic, OpenAI, OpenRouter, OpenCode Zen и **OpenCode Go** через `OPENCODE_GO_API_KEY`. Profile может иметь собственную модель; provider-specific choice имеет приоритет. Ключи, подключённые напрямую, посылают inference request непосредственно provider, хотя repository/thread sync всё равно использует Delta cloud.

Что ещё не доказано:

- публичного source path transport adapter не найдено;
- нет собственного packet capture или независимого E2E именно `muse-spark-1.3-contributor`;
- поэтому корректные Responses semantics, `x-opencode-session`, compaction и service/background paths для Muse считаются **документированными, но не проаудированными**.

Есть visible automatic fallback: при отказе Claude Fable Delta может переключить thread на Claude Opus 5 и продолжить. Это не скрытая подмена, но это также не fail-closed routing. Общего выключателя такого fallback в проверенной документации не найдено.

### 8.5. Чего нет

У Delta не найдено эквивалента Kilo `/goal`, Codex Goal или Muse Goal:

- thread и conversation сохраняются;
- один agent turn может быть длинным и иметь subagents;
- но отдельный persistent objective, независимый completion evaluator и idle→continue loop не документированы.

Также отсутствуют shipped:

- one-shot scheduler/cron;
- documented headless agent-run CLI/API;
- MCP — ещё `In Progress`;
- persistent remote/cloud runtime — ещё `In Progress`;
- WSL support — `Up Next`.

Background terminal живёт только пока работает запустивший его Delta process; закрытие приложения останавливает процесс. Поэтому browser/mobile сейчас лучше считать review/control surface, а не доказанным always-on runtime.

### 8.6. Security — главный блокер unattended use

Официальная документация прямо указывает:

- agent permission system отсутствует;
- агент не спрашивает перед tool calls, включая destructive calls;
- sandbox отсутствует, агент имеет unrestricted device access;
- `.agents/prepare`/`.delta/prepare` может исполняться автоматически;
- `.envrc`, rules и skills из repository могут влиять на runtime.

Для автономного manager на основном Windows-компьютере это хуже Codex и недостаточно для production. Secret redaction полезен, но не заменяет sandbox и permission gate.

### 8.7. DeltaDB и cloud boundary

При добавлении repository Delta сохраняет локально и на своих серверах:

- Git objects, commits и file contents;
- thread messages и fine-grained worktree deltas;
- repository/thread metadata.

Backend работает на Cloudflare. Локальное удаление thread не гарантирует удаление server history; account deletion требует обращения к privacy contact. Это не скрытая утечка, а официальный продуктовый контракт, но для private repositories он принципиально отличается от чисто локального harness.

### 8.8. Конфликт с правилом «только main, без worktrees»

Delta по умолчанию создаёт isolated checkout для thread и isolated copies для Worker/Reviewer. Это не Git worktree в буквальном смысле, а DeltaDB-managed checkout, однако архитектурно это всё равно параллельная копия с последующим merge.

Работать прямо в основном checkout можно:

1. выбрать **Existing Local Checkout**;
2. установить нужным profiles `worktree = "shared"`;
3. разрешить запись только одному agent одновременно.

Но тогда Delta не предоставляет file-level lease: parent, Scout и shared workers способны столкнуться в одном checkout. Поэтому для пользовательского правила лучший режим — **один shared writer, остальные isolated или без shell**, а не полностью shared swarm.

### 8.9. Auditability и лицензия

Delta использует native/Rust technology, но это не означает открытый runtime. В публичной организации найден официальный `delta-nix` только для упаковки binaries; его Nix metadata помечает license как `unfree`. Публичного repository с исходниками приложения Delta/DeltaDB не найдено.

Следовательно:

- Zed editor source можно читать;
- Delta binary и provider/subagent internals нельзя приравнивать к открытым исходникам Zed;
- security и OpenCode Go transport приходится подтверждать документацией и black-box tests.

### 8.10. Мнение пользователей и зрелость

Независимых длительных production reports пока мало: public beta существует меньше двух недель. Внешняя дискуссия в основном отмечает три вещи:

- сильный UX и связь code ↔ conversation;
- сомнение, помогает ли полный transcript reviewer или добавляет noise;
- тревогу из-за загрузки repository/history в vendor cloud и слабого удаления данных.

Zed сообщает о 570 changes, landed 33 сотрудниками после отключения PR в собственном Delta repository. Это серьёзный dogfood signal, но не независимая проверка на чужих monorepos и Windows machines.

Был публичный report старой nightly 0.1.1 о runaway sync loop и 9–14 GB RSS. Issue закрыли как размещённый не в том tracker, а не как fixed. Текущая версия 0.17.1 значительно новее, поэтому этот report не доказывает наличие того же bug сейчас; он лишь показывает beta maturity и отсутствие прозрачного public tracker/source audit.

### 8.11. Вердикт

**Delta — реальный, сильный, самостоятельный harness и обязательный кандидат для проверки.** Для интерактивной работы parent → workers → reviewer он сейчас выглядит лучше многих TUI wrappers.

Но Delta пока не заменяет long-running manager:

- нет persistent Goal/evaluator;
- нет scheduler;
- нет MCP;
- нет permissions/sandbox;
- remote runtime не shipped;
- source audit невозможен;
- repository/history уходит в vendor cloud.

Итого: **первое место среди interactive standalone GUI harnesses; не квалифицируется как unattended Goal runtime.**

## 9. Kilo CLI: сильный autonomous harness без VS Code

### 9.1. Goal реализован как runtime, а не надпись в UI

**DOC + CODE.** `/goal` хранит objective, умеет pause/resume/clear и продолжает сессию между ходами. Код Goal связан с session state, очередью prompt, background processes и wakeup registry.

Семантика важнее наличия команды:

- только root Goal worker может вызвать `goal_report`;
- delegated workers возвращают результаты root;
- Stop или `/goal pause` останавливают активную Goal-работу;
- active Goal после backend restart становится **paused**, то есть не запускает неожиданные действия после старта;
- completion означает **отчёт рабочей модели**, а не независимую проверку auditor-моделью или тестовым runtime.

Последний пункт — главный недостаток. Kilo требует concrete reason/evidence в prompt, но та же модель, которая делала работу, является authority на `complete`.

### 9.2. Таймеры — один из лучших контрактов среди проверенных систем

**CODE.** Kilo имеет два отдельных durable механизма:

- `schedule_wakeup` — одноразовое пробуждение;
- `cron_create` — повторяемое расписание.

Они хранятся через runtime storage, восстанавливаются при запуске instance и интегрированы с Goal status `waiting`. Для Goal «подождать CI/deploy/build» agent обязан сначала создать wakeup/cron/background wait вместо бессмысленного polling.

Ограничители:

- до 10 pending wakeups на session;
- до 10 cron tasks;
- горизонт до 7 дней;
- missed recurring occurrence не воспроизводится много раз задним числом;
- next occurrence сохраняется до выполнения, чтобы crash не породил повтор.

Это существенно лучше, чем внешний cron, который каждый раз создаёт новую несвязанную сессию.

### 9.3. Для этого отбора считается только собственный CLI runtime

**Task subagents в Kilo CLI:**

- foreground или `background: true`;
- отдельный context;
- тот же checkout, без обязательного Git worktree;
- nested delegation при разрешённой глубине;
- custom agent может иметь собственную модель, prompt и permissions;
- результат возвращается parent;
- root и descendants могут обмениваться сообщениями через Swarm board.

VS Code Agent Manager действительно умеет отдельные top-level sessions и worktrees, но **в редакции 5 он не даёт Kilo дополнительных баллов**: пользователь исключил VS Code как класс продукта. Поэтому Kilo оценивается как CLI/TUI harness с мобильным клиентом, Goal, timers и task-subagents. Это всё ещё сильный кандидат, но не лучший standalone desktop GUI.

### 9.4. Остановка дерева агентов реализована

**CODE.** `cancelTree` рекурсивно собирает descendants и отменяет root и children. Это исправляет один из главных рисков multi-agent systems: Stop parent не обязательно оставляет workers жить отдельно.

Однако исторический issue показывал, что foreground child мог заблокировать TUI при зависшем model stream. Современный tree-cancel путь есть, но это не означает, что каждый старый UI path уже идеален.

### 9.5. Где orchestration всё ещё незрелая

Три открытых upstream issue задают честную границу:

1. **Agent Manager не умеет полноценно выбрать registered agent/mode для каждой spawned session.** Модель можно выбрать, но role-specific system prompt/tools/permissions часто приходится имитировать task prompt.
2. Полноценный reusable orchestrator с planning → implementation → escalation → review и hard global limits всё ещё proposal.
3. Kilo Swarm остаётся experimental: board работает, но identity/state/read-evidence/guidance ещё дорабатываются. Board post не будит, не назначает и не останавливает agent.

Итого: **Kilo уже manager**, но ещё не завершённая «организация из строго типизированных ролей».

### 9.6. Телефон

Официальные iOS/Android приложения умеют:

- подключаться к local CLI/extension sessions;
- читать streaming transcript;
- отправлять follow-ups в очередь;
- выполнять slash commands;
- видеть и управлять `/goal`;
- создавать новую session в том же workspace.

Это самый цельный mobile-path среди Windows-native кандидатов. Но он зависит от Kilo account/Gateway remote infrastructure, а компьютер и runtime должны оставаться online.

Исторические mobile bugs существовали: например pending user question не отображался и session выглядел idle. Документация текущей версии обещает более полный control, но закрытие старого issue как not planned не является доказательством, что все типы prompt теперь покрыты.

### 9.7. OpenCode Go / Muse

Kilo использует catalog metadata на уровне модели. Для Muse Spark 1.3 Contributor metadata указывает:

```text
provider: opencode-go
protocol: openai-responses
base URL: https://opencode.ai/zen/go/v1
context: 1,048,576
output: 131,072
```

Source также добавляет стабильный `x-opencode-session` для `opencode` и `opencode-go`. После первоначального partial fix отдельный merged PR добавил header в memory, roll-call и expand-prompt paths; пользователь подтвердил исчезновение memory error.

Но официальная compatibility-страница OpenCode Go всё ещё явно подтверждает **Kilo CLI**, а не весь VS Code extension path. Поэтому статус:

- **Kilo CLI + Go/Muse:** подтверждённый strong candidate;
- **Kilo VS Code + Go/Muse во всех служебных paths:** source fixes есть, но не считать полностью validated;
- **Kilo mobile → local extension → Go/Muse:** зависит от предыдущего слоя.

### 9.8. Security и stack

- основной runtime — TypeScript/Bun/OpenCode fork;
- Windows sandboxing отсутствует;
- permissions есть, но shell или другой разрешённый tool может обходить узкий read denial;
- Keep Awake может сохранять agent access к files/network/credentials при locked screen;
- remote mobile account становится control surface компьютера.

**Вердикт:** функционально лучший all-in-one candidate, но не строгий Rust/Go sandboxed harness.

## 10. Codex + OpenCodex: по-прежнему основной консервативный путь

### 10.1. Сильные стороны

- Codex core на Rust;
- native Goal, MCP, skills и subagents;
- OpenCodex даёт единый model catalog и route-by-model;
- Go-specific session affinity и protocol transforms;
- mixed OpenAI-manager → Muse-workers через v1 surface;
- штатный Codex Remote позволяет управлять host из ChatGPT mobile без отдельного HAPI.

### 10.2. Что изменилось с предыдущей редакции

**OpenCodex 2.70.0** теперь публикует отдельные Windows x64 MSI/ZIP, SHA-256 и signatures. Desktop app не требует глобального Node/Bun. Это лучше старой npm-only схемы.

Но security/source audit проводился для **2.58.0**. Новые native packages и код 2.70.0 — другой объект. Нельзя переносить verdict автоматически.

### 10.3. Оставшиеся предметные дефекты

1. **Mobile thread visibility.** Provider-table form, в том числе при client compaction, способен разделить history tags `openai`/`opencodex`. В открытом issue ChatGPT Android remote не показывал часть threads, хотя Desktop видел их.
2. **Mobile-created routed threads.** Старые отчёты показывали: thread, созданный локально, можно продолжать с телефона, но новый mobile thread с routed model мог пройти account-model validation до local proxy.
3. **Muse tool names >64.** OpenCodex aliasing guard в исследованном коде применяется к прямому `api.meta.ai`; автоматическое покрытие `opencode.ai/zen/go` не подтверждено.
4. **Version churn.** Новые private Codex request fields уже ломали строгий Muse endpoint до adapter patch.

### 10.4. Phone-control

Приоритетный phone path — native Codex Remote. HAPI остаётся альтернативой для shared app-server sessions, но добавляет Bun/control-plane layer.

`clientCompaction=true` нельзя больше считать безусловно правильным default: переносимая plaintext compaction может конфликтовать с provider identity и mobile thread listing.

**Вердикт:** лучший вариант, если важнее строгий Rust harness и сохранение экосистемы Codex, чем единый продукт «всё из коробки».

## 11. Muse Code + Helicon

Muse Code — единственный проверенный runtime, где Muse является не сторонней моделью, а родным продуктом.

Подтверждены:

- native `/goal`;
- `/loop` с interval/cron;
- nested subagents;
- configurable execution capacity;
- workflows с dependent/parallel stages;
- MCP, hooks, skills;
- родной Muse Session Protocol.

Helicon использует MSP, а не parsing terminal text. Есть projects, sessions, Goal controls, diffs, approvals и web frontend.

Ограничения:

- Helicon stack — Tauri/Rust shell + Node daemon + React;
- mobile relay не считать shipped полноценным приложением;
- официальный Muse Code hosted path — Meta auth/subscription;
- community `endpoint_transport` показывает возможность custom Responses endpoint, но опубликованный example proxy не передаёт Go session header и не является полноценным production adapter.

**Вердикт:** лучший вариант при прямом Meta-доступе. Для обязательного OpenCode Go остаётся research path.

## 12. OpenChamber 2.0.4: лучший mobile UI, но Goal runtime остаётся проблемой

### 12.1. Обновление версии

Свежая проверенная версия — **2.0.4**, с OpenCode client/schema 2.0.18.

Issue о Go small-model path закрыт: maintainer считает исправление обязанностью upstream `/api/experimental/generate`. Однако код OpenChamber 2.0.4 по-прежнему вызывает `client.generate.text(...)` без session-bound request. Поэтому статус — **reported fixed through dependency**, а не independently proven fix в OpenChamber.

### 12.2. Goal races не исчезли из shipped code

Issue #3279 перечислял:

- stale continuation после Pause/Clear/new user message;
- lost wakeup, когда новый idle event приходит во время inflight tick;
- stranded active Goal после restart;
- increment `turnsUsed` до подтверждённой отправки continuation;
- отсутствие strong revision/CAS для Goal metadata.

Issue закрыт, но основной hardening PR #3285 закрыт **без merge**. В v2.0.4 runtime всё ещё:

- event-driven, без restart scan;
- использует `timers` + `inflight`;
- `writeGoal` проверяет Goal id, не полную revision;
- continuation flow сохраняет счётчик до admission proof.

Поэтому OpenChamber нельзя считать unattended manager только потому, что UI показывает Active Goal.

### 12.3. Что остаётся сильным

- отличный desktop/web/mobile GUI;
- agent session-control tool;
- scheduler;
- Android/iOS/PWA;
- E2EE relay действительно реализован через ECDH → HKDF → AES-GCM с monotonic counters.

**Вердикт:** сильный remote interface/control plane, но не основной long-running Goal-owner.

## 13. JetBrains Air

Air хорошо решает:

- параллельные local tasks;
- review/diff;
- несколько проектов;
- ACP agents, включая OpenCode/Muse bridges;
- Windows professional IDE UX.

Но:

- persistent Goal принадлежит подключённому harness;
- список tasks не равен model-driven manager;
- cloud automations используют JetBrains AI credits/providers, а не локальный BYOK/Go;
- cloud task работает в отдельной branch/environment;
- local ACP capabilities теряются, если ACP не умеет Goal/workflow extension;
- mobile для local sessions не подтверждён как готовый продукт.

**Вердикт:** один из лучших интерфейсов, но не управляющий runtime.

## 14. Vicoa: лучший neutral remote supervisor, не Goal-manager

Vicoa поддерживает desktop/web/mobile и подключает разные harnesses. Проверены:

- native mobile client;
- session chat, terminal, files, diffs;
- task board;
- one-time и 5-field cron automations с timezone;
- automation хранит prompt + agent/model + machine/folder и создаёт новую agent session;
- CLI, через который agent теоретически может управлять tasks/automations.

Не найдено:

- persistent Goal, который сам продолжает одну session до completion;
- model-driven manager, который динамически раздаёт разные subtasks workers;
- independent completion auditor.

Stack — Python backend + TypeScript frontend + Flutter mobile. Поэтому Vicoa — **верхний UI/scheduler слой**, а качество manager зависит от Claude/Codex/OpenCode/Kilo внутри.

## 15. Agent Teams AI: настоящий team orchestrator, но без phone/Goal package

Agent Teams AI действительно глубже обычного Kanban:

- lead agent;
- task assignment;
- peer messaging;
- review obligations;
- stall monitoring/nudges;
- provider-aware launches;
- main checkout или worktrees;
- OpenCode/Codex/Claude integrations.

Однако:

- persistent Goal loop не подтверждён;
- local durable scheduler не подтверждён;
- готового phone client нет;
- приложение тяжёлое: Electron, Node 24, pnpm, native rebuilds/postinstall;
- Go/Muse contract не подтверждён отдельно от bundled OpenCode runtime.

**Вердикт:** сильный desktop «agent company» UI, но не лучший fit под строгую Windows/mobile/Go задачу.

## 16. Rust/Go-кандидаты: почему язык ядра пока не выигрывает

### jcode

Плюсы:

- Rust;
- durable initiatives/goals с milestones, blockers и checkpoints;
- swarm/subagents;
- небольшой runtime footprint.

Минусы:

- initiative storage не доказывает auto-continuation Goal-loop;
- upstream OpenCode Go route всё ещё hardcodes `/chat/completions` в исследованном path;
- Muse direct и Muse subagents имеют открытые protocol issues;
- working Responses patches живут в forks/floating commits, не release upstream.

### Goose

Плюсы:

- Rust core;
- Windows Desktop;
- native scheduler;
- MCP и parallel subagents.

Минусы:

- Goal собран из recipes/hooks, а не единый native Goal state machine;
- OpenCode Go provider получает общий catalog, но один API format; issue открыт;
- manual/smart approval несовместимы с subagents в исследованном режиме.

### Crush

Go, быстрый TUI, MCP HTTP/stdio/SSE и subagents. Но нет подтверждённого пакета Goal + durable scheduler + phone GUI.

### VT Code

Rust single binary и scheduled runs, но Windows support best-effort и полноценный GUI/mobile отсутствуют.

### amux

Сильный Rust fleet manager с board, schedules и phone/web, но требует tmux и ориентирован на macOS/Linux. На Windows это снова WSL/remote host.

**Итог:** pure Rust/Go shortlist пока проигрывает не по скорости, а по отсутствию одного из критических control layers.

## 17. Телефон: что реально управляет локальной работой

| Путь | Управляет той же локальной session? | Может approve/steer? | Главный риск |
|---|---:|---:|---|
| **Kilo mobile** | Да, для remote CLI/extension sessions | Да, follow-ups/slash/Goal | Kilo account/Gateway control surface; extension Go path не fully validated |
| **Codex native Remote** | Да | Да | OpenCodex provider-tag/model validation edge cases |
| **OpenChamber mobile** | Да | Да | Goal runtime races; relay trust surface |
| **Vicoa mobile** | Да, для Vicoa-managed harness sessions | Да | Не Goal-owner; Python/cloud account infrastructure |
| **HAPI** | Да, для HAPI-owned/resumed Codex execution | Да | Дополнительный Bun hub/runner; не attach к любой Desktop session |
| **OpenCode Mobile / Pilot** | Да, к self-hosted OpenCode | Да | Community remote layer; security/tunnel config; не добавляет Goal сам |
| **Helicon web** | Да, к Muse daemon | Да | Нет mature native mobile relay/app |
| **Air web** | В основном cloud tasks | Ограниченно | Другие credits/environment/branches |

Телефон, способный approve shell/tool, фактически является remote control компьютера. QR/token и revoke недостаточны без transport/auth/session isolation.

## 18. Рекомендованные архитектуры

### A. Текущая production-схема с обязательным Muse/OpenCode Go

```text
Codex Goal owner
  ↓
OpenCodex model-aware routing
  ├─ OpenAI manager/reviewer
  └─ OpenCode Go → Muse workers
```

Оставить Codex/OpenCodex, пока не появится другой harness с доказанными `/responses`, stable per-session header, auxiliary-call compatibility, tools и mixed-provider subagents. Один writer в `main`; остальные workers read-only/research/review.

### B. Лучший general manager без требования Muse: Graff + Harness

```text
Harness desktop/headless
  └─ graff standing Goal
       ├─ scout       read-only
       ├─ reviewer    read-only
       └─ writer      единственный mutator
```

Use `/goal` для standing objective, finite `/loop` run для конкретного sprint, `/schedule` только как local wakeup. Не считать completion independent verification: deterministic checks запускать отдельным root-side command/hook и не позволять writer менять acceptance files.

### C. Rust GUI prototype: Sinew

Использовать только с открытым app/view и explicit provider path. Не считать persisted Goal always-on. Swarm ограничить 2–4 peers и одним writer; shell отключить у auditors, поскольку workspace-root check не является sandbox.

### D. Interactive review: Delta

Delta остаётся сильным для human-in-the-loop parent→workers→review. Не использовать как unattended manager до появления Goal, scheduler, MCP, permissions и sandbox. Automatic model fallback и auxiliary model roles должны быть явно проверены.

### E. Design reference для независимой приёмки

Для serious unattended workflow заимствовать SWE-agent/SWE-bench pattern:

```text
implementer produces candidate
  → separate controller
  → protected tests/policy outside writer scope
  → accept or return bounded failure evidence
```

Ни один текущий top candidate не реализует этот contract полностью для произвольного local repository.

## 19. Финальный порядок кандидатов

### При обязательном OpenCode Go + Muse Contributor

1. **Codex + OpenCodex** — текущий основной путь.
2. **Kilo CLI** — более цельный Goal/timer runtime, но exact Muse/tool E2E и Windows sandbox слабее.
3. **Delta** — documented Go provider и отличный interactive GUI, но opaque Muse transport и нет Goal/security layer.
4. **CodeGraff + Harness** — архитектурно лучший manager, но текущий router несовместим с Muse Responses.
5. **Sinew / Pi-Go / Tatsu** — не имеют готового Muse path.

### General-purpose standalone manager, если provider можно выбрать другой

1. **CodeGraff + Harness** — лучший aggregate Goal/subagent/MCP/approval/headless/desktop package.
2. **Kilo CLI** — лучший durable wakeup/cron lifecycle.
3. **Sinew** — лучший open Rust desktop/swarm prototype, но UI-owned Goal.
4. **Codex** — самый строгий core, но менее гибкая heterogeneous orchestration без gateway.
5. **Muse Code** — сильный native runtime при direct Meta access.
6. **Delta** — лучший interactive/review UI, но не manager lifecycle.

### Строгость и безопасность

1. **Codex** — sandbox/permissions и наиболее зрелый auditable core.
2. **CodeGraff** — strong runtime gates/read-only children/approvals, но без OS sandbox и с молодыми native defects.
3. **Kilo CLI** — rich policy/runtime, но Windows sandbox отсутствует.
4. **Sinew** — open code и configurable tools, но shell unsandboxed.
5. **Delta** — closed runtime, cloud replication, no permissions/sandbox.

### Итог

**На сегодня ничего не вытеснило Codex+OpenCodex из обязательного Go/Muse path.** Но Graff+Harness впервые выглядит как реальный более универсальный manager, а не frontend. Нужный следующий технический milestone для него узкий и понятный: first-class OpenCode Go provider с per-model protocol, stable session identity и Muse tool/schema compatibility.

## 20. Что отслеживать дальше

### CodeGraff/Harness

1. Native `responses`/`messages` transport в custom router.
2. OpenCode Go client identity + stable `x-opencode-session` на root, child, compaction/title/eval paths.
3. Muse Spark tool schemas и 64-char names.
4. Crash-safe autonomous-run ledger, не только persistent objective.
5. Recurring cron/OS wake/service integration.
6. Protected acceptance contract/immutable verifier.
7. Fix open ACP concurrent-network SIGSEGV и finite-background-job wait classification.
8. Mature iOS remote viewer; Android client.
9. License implications and reproducible release provenance.

### Sinew

1. Move Goal continuation из React UI в Rust backend/daemon.
2. Hard run budget, restart policy и independent completion gate.
3. Official custom provider/OpenCode Go.
4. Sandboxed shell или enforceable per-agent permissions.
5. Independent remote-relay security review.

### Existing leaders

- OpenCodex: mobile provider identity, Muse long tool names, mixed v2 delegation.
- Kilo: exact Muse Responses E2E, role/mode selection, independent verifier, Windows sandbox.
- Delta: Goal/scheduler/MCP/permissions/sandbox, fail-closed routing, public audit surface.
- Muse Code: official custom-provider/Go contract.
- Pi-Go: dynamic model catalog, Muse Responses and stable session header.

## 21. Источники редакции 5

### CodeGraff / Harness

- https://github.com/justrach/codegraff — Zig harness source and issues.
- https://github.com/justrach/codegraff/releases/tag/v0.0.302.10 — verified Windows/macOS/Linux binary release.
- https://github.com/justrach/codegraff/blob/main/docs/goals.md — Goal lifecycle and checklist semantics.
- https://github.com/justrach/codegraff/blob/main/src/goal_state.zig — persistent Goal state/checklist epochs.
- https://github.com/justrach/codegraff/blob/main/src/loop_run.zig — 25-turn controller, hold/wake semantics.
- https://github.com/justrach/codegraff/blob/main/src/schedule.zig — durable one-shot schedule store.
- https://github.com/justrach/codegraff/blob/main/src/subagent.zig — one-level background/foreground workers.
- https://github.com/justrach/codegraff/blob/main/src/agent_tool_gate.zig — approvals, read-only children, policy protection.
- https://github.com/justrach/codegraff/blob/main/src/router_config.zig — custom router is Chat Completions only.
- https://github.com/justrach/codegraff/issues/1193 — open ACP concurrent-network SIGSEGV.
- https://github.com/justrach/codegraff/issues/1267 — fixed provider-key inheritance.
- https://github.com/justrach/codegraff/issues/1292 — fixed shell approval bypass.
- https://github.com/justrach/codegraff/issues/1166 — fixed parallel-shell heap corruption report.
- https://github.com/justrach/codegraff/issues/1218 — fixed parallel tool argument corruption.
- https://github.com/justrach/codegraff/issues/219 — durable attempt ledger not shipped.
- https://github.com/justrach/codegraff/issues/220 — protected acceptance contract not shipped.
- https://github.com/justrach/harness — Rust/GPUI desktop/headless source.
- https://github.com/justrach/harness/releases/tag/v0.2.99 — Windows executable/portable build.

### Sinew

- https://github.com/Paseru/sinew — Rust/Tauri source and README.
- https://github.com/Paseru/sinew/releases/tag/v0.1.51 — current checked release.
- https://github.com/Paseru/sinew/blob/main/crates/sinew-app/src/agent/mode.rs — Goal modes/instructions.
- https://github.com/Paseru/sinew/blob/main/crates/sinew-app/src/store.rs — persisted conversation/Goal state.
- https://github.com/Paseru/sinew/blob/main/src/components/chat/ChatPane.tsx — UI-driven Goal continuation.
- https://github.com/Paseru/sinew/blob/main/remote/README.md — encrypted remote relay design.
- https://github.com/Paseru/sinew/issues/27 — custom provider request.

### Pi-Go / Tatsu / verifier reference

- https://github.com/dimetron/pi-go — Go harness.
- https://github.com/dimetron/pi-go/blob/main/internal/provider/opencode.go — hard-coded multi-protocol Go catalog.
- https://github.com/dimetron/pi-go/blob/main/internal/tools/subagent.go — single/parallel/chain process workers.
- https://www.tatsucode.com/ — official Tatsu Code product/docs.
- https://github.com/SWE-agent/SWE-agent — RetryAgent/reviewer and SWE-bench evaluator design reference.

## 22. Источники редакции 4

### Delta

- https://zed.dev/blog/introducing-delta — отдельное приложение, DeltaDB и исходная архитектура.
- https://zed.dev/blog/delta-public-beta — public beta, Windows/Linux/macOS/web/mobile surfaces.
- https://delta.dev/docs/agents/subagents — built-in/custom profiles, isolation, messaging и stop.
- https://delta.dev/docs/configuration/settings — concurrency, model override, TOML profiles, rules и prepare scripts.
- https://delta.dev/docs/agents/models-and-providers — OpenCode Go API key, roles, fallback и compaction.
- https://delta.dev/docs/privacy-and-security/agentic-safety — отсутствие permissions и sandbox.
- https://delta.dev/docs/privacy-and-security/data-storage — server-side repository/thread storage и deletion boundary.
- https://delta.dev/roadmap — MCP, remote runtime, sandbox и WSL ещё не shipped.
- https://delta.dev/blog/beyond-pass-rate — vendor-run harness benchmarks и их методология.
- https://github.com/zed-industries/delta-nix — официальный binary packaging; public application source здесь отсутствует.
- https://github.com/zed-industries/delta-nix/blob/main/flake.nix — binary package marked `unfree`.
- https://news.ycombinator.com/item?id=49727245 — внешняя дискуссия: privacy, CI/gating и ценность полного history.

## 23. Источники редакции 3

### Kilo Code

- [Session Goals](https://kilo.ai/docs/code-with-ai/agents/goals)
- [Agent Manager](https://kilo.ai/docs/automate/agent-manager)
- [Custom Subagents](https://kilo.ai/docs/customize/custom-subagents)
- [Tool Use / Kilo Swarm / Agent Manager](https://kilo.ai/docs/automate/tools)
- [Mobile Apps](https://kilo.ai/docs/code-with-ai/platforms/mobile)
- [Keep Awake](https://kilo.ai/docs/getting-started/settings/keep-awake)
- [Settings / Windows sandbox limitation](https://kilo.ai/docs/getting-started/settings)
- [Goal source instructions](https://github.com/Kilo-Org/kilocode/blob/main/packages/opencode/src/kilocode/session/goal/instructions.ts)
- [Wakeup registry source](https://github.com/Kilo-Org/kilocode/tree/main/packages/opencode/src/kilocode/wakeup)
- [Tree cancellation source](https://github.com/Kilo-Org/kilocode/blob/main/packages/opencode/src/kilocode/session/prompt.ts)
- [Agent Manager role-selection gap #13827](https://github.com/Kilo-Org/kilocode/issues/13827)
- [Prompt-driven orchestration proposal #13745](https://github.com/Kilo-Org/kilocode/issues/13745)
- [Kilo Swarm epic #13673](https://github.com/Kilo-Org/kilocode/issues/13673)
- [OpenCode Go session header #13723](https://github.com/Kilo-Org/kilocode/issues/13723)
- [Merged extra header paths #14015](https://github.com/Kilo-Org/kilocode/pull/14015)
- [Hybrid OpenCode v1/v2 architecture #12887](https://github.com/Kilo-Org/kilocode/issues/12887)

### Codex / OpenCodex

- [OpenCodex repository and current Windows packages](https://github.com/lidge-jun/opencodex)
- [OpenCodex mobile provider-tag issue #5848](https://github.com/lidge-jun/opencodex/issues/5848)
- [OpenCodex Go/Muse security and compatibility review](OpenCodex_security_review_2026-09-18.md)
- [Verified Windows install instructions](OpenCodex_Windows_agent_install_verified_2026-09-18.md)

### OpenChamber

- [OpenChamber repository](https://github.com/openchamber/openchamber)
- [Goal runtime](https://github.com/openchamber/openchamber/blob/v2.0.4/packages/web/server/lib/session-goal/runtime.js)
- [Goal lifecycle issue #3279](https://github.com/openchamber/openchamber/issues/3279)
- [Unmerged hardening PR #3285](https://github.com/openchamber/openchamber/pull/3285)
- [OpenCode Go small-model issue #3950](https://github.com/openchamber/openchamber/issues/3950)

### Muse / Air / control planes

- [Muse Code official docs](https://dev.meta.ai/docs/muse-code)
- [Helicon](https://github.com/HarjjotSinghh/helicon)
- [JetBrains Air](https://www.jetbrains.com/air/)
- [muse-acp](https://github.com/BrokkAi/muse-acp)
- [Vicoa](https://github.com/vicoa-ai/vicoa)
- [Agent Teams AI](https://github.com/777genius/agent-teams-ai)

### Rust/Go candidates

- [jcode](https://github.com/1jehuang/jcode)
- [jcode OpenCode Go protocol issue #1224](https://github.com/1jehuang/jcode/issues/1224)
- [jcode subagent protocol issue #1225](https://github.com/1jehuang/jcode/issues/1225)
- [Goose OpenCode Go issue #11980](https://github.com/aaif-goose/goose/issues/11980)
- [Crush](https://github.com/charmbracelet/crush)
- [VT Code](https://github.com/vinhnx/VTCode)
- [amux](https://github.com/mixpeek/amux)

## 24. Историческое приложение: Codex + OpenCode Go

Ниже сохранён прежний технический разбор транспортов, model catalog и OpenCodex. Даты и версии в нём относятся к 18 сентября 2026 и не переопределяют выводы редакции 5.

## OpenCode Go в Codex: выводы чата и проверенный путь интеграции

**Проверено: 18 сентября 2026.** Исследованы Codex `0.155.0` и OpenCodex `2.58.0`, опубликованные 17 сентября. Для ключевых выводов использованы исходники и документация этих тегов. Версии установленного у пользователя Windows-приложения и его backend неизвестны. Установка и платные API-вызовы не выполнялись. [H2] [H11]

### 1. Что решили за весь разговор

Это требования и наблюдения пользователя, а не рекламные характеристики продуктов.

- Нужен **готовый менеджер coding-агентов**: сохраняемый Goal с критериями завершения, проверка результата, subagents, расписания/таймеры, MCP, skills, hooks, управление инструментами и понятный GUI. Одного уровня `manager → workers` достаточно.
- Целевой провайдер — **OpenCode Go**, прежде всего **Meta Muse Spark 1.3 Contributor**. Желательна схема: сильная основная модель через обычный ChatGPT/Codex-вход, дешёвые Muse-workers через Go. Полностью Go-сессия тоже полезна, но это другой сценарий.
- Нужен **обычный model picker**, а не постоянные запуски с разными `--profile`, набор отдельных терминалов или Muse, замаскированный под GPT.
- Среда — **Windows**. Предпочтительны строгие Rust/Go-инструменты, прозрачные зависимости и обратимые настройки. Нельзя молча устанавливать глобальные Python-пакеты, менять PATH, включать службы или добавлять скрытые платные fallback-маршруты.
- **Hermes исключён окончательно** из-за описанного пользователем поведения установки. Antigravity устраивал, но квота недостаточна. В опробованном OpenCode V2 агент не обнаружил требуемых Goal и таймеров.
- Claude Code, OpenCode, Pi/OMP, Kilo, Goose, jcode, VT Code и внешние менеджеры обсуждались, но не прошли общую практическую проверку требований. Сейчас приоритет — **качественная интеграция в уже выбранный Codex**, не очередная миграция.

Работа в репозиториях — только `main`, без worktrees. Предлагаемый старт: один менеджер и 3–4 исполнителя; одновременно изменяет файлы один исполнитель, остальные исследуют и проверяют. Ранее приведённые рейтинги моделей не доказывают, какая лучше управляет именно этим workflow.

### 2. Главный технический вывод

**Подключение endpoint, появление модели в picker и сохранение агентского функционала — три разные задачи.**

| Путь | Что решает | Что не решает автоматически |
|---|---|---|
| **Штатный custom provider + корректный каталог Codex** | Прямой доступ к Go без дополнительного runtime; выбор моделей внутри этого провайдера | Общий provider-aware GUI для ChatGPT и Go; отдельный provider у native subagent |
| **Локальный model-aware gateway + каталог Codex** | Один endpoint для Codex, маршрутизация разных моделей, возможность сохранить native login и Go вместе | Ограничения конкретного Desktop picker, несовместимость encrypted v2 delegation, полное равенство backend-функций |
| **Патч/форк Codex или Desktop** | Может менять отсутствующий клиентский функционал | Это уже не штатная интеграция; появляется собственное сопровождение |

Основание: конфигурация транспорта, загрузчик каталога, применение agent roles и документация существующего gateway. **Первый путь наиболее нативный. Второй — наиболее предметный из исследованных для смешанного ChatGPT → Muse без изменения бинарника Codex.** [H3] [H4] [H5] [H7] [H13]

### 3. Что подтверждено про прямое подключение

OpenCode Go официально перечисляет Codex среди validated clients. Muse 1.3 Contributor доступна через **Responses API**; весь каталог Go не однороден: часть других моделей использует Messages или Chat Completions. Codex `0.155.0` поддерживает `wire_api = "responses"`; нельзя подключать весь Go-каталог одним протоколом без проверки каждой модели. [H1] [H3]

Минимальная транспортная конфигурация для **отдельной Go-сессии**, не для смешанного ChatGPT-parent:

```toml
# Пользовательский %USERPROFILE%\.codex\config.toml.
# Это пример необходимых значений, НЕ замена существующего файла.
# Корневые ключи должны находиться перед секциями [tables].
model_provider = "opencode-go"
model = "muse-spark-1.3-contributor"

[model_providers.opencode-go]
name = "OpenCode Go"
base_url = "https://opencode.ai/zen/go/v1"
env_key = "OPENCODE_GO_API_KEY"
wire_api = "responses"
requires_openai_auth = false
```

Это **только транспорт**, а не законченная GUI-интеграция. Provider-настройки должны находиться в пользовательском конфиге: project-local настройки, перенаправляющие credentials/provider, Codex игнорирует. Ключ передаётся локально; не помещать его в репозиторий или переписку. [H4]

Официальный API подтверждает ID `muse-spark-1.3-contributor`. Префикс `opencode-go/` — имя маршрута внутри клиента/роутера, а не ID, который нужно безусловно отправлять в Go API. [H1] [H18]

### 4. Как люди добиваются нормального model picker

#### Нужен каталог Codex, а не просто `/v1/models`

Codex читает **`ModelsResponse { models: ModelInfo[] }`**, а не обычный OpenAI-список `data[]`. Каталог содержит инструкции и метаданные возможностей, не только названия. В `0.155.0` десериализация требует `model_messages.instructions_template` либо совместимое legacy-поле `base_instructions`. `visibility` и API-совместимость также влияют на выбор модели. [H6]

Штатный `model_catalog_json` принимает абсолютный путь к локальному файлу и загружается при старте. Сам файл **не создаёт маршрутизацию по провайдерам**. В нём нельзя бездумно копировать GPT-контекст, reasoning tiers и native-only возможности на Muse: наличие поля не означает поддержку со стороны модели. [H3] [H5]

Именно генерация полноценного каталога описана разработчиками интеграций в issue **#37122**: обычный список давал ошибки, подходящий Codex-каталог решал проблему TUI. Это отчёт для `0.146.0`, а не доказательство поломки всех новых версий. [H9]

#### Desktop проверяется отдельно от CLI

В Windows issue **#19694** описан дополнительный фильтр моделей в renderer: backend возвращал модель, но GUI её скрывал. Issue закрыт; нельзя объявлять его текущим дефектом всех сборок. Однако документация OpenCodex `2.58.0` всё ещё предупреждает о таком фильтре в некоторых Desktop-релизах. [H10] [H13]

**Следствие:** успешный `/model` в CLI или правильный `model/list` у app-server ещё не подтверждает GUI. Необходима проверка именно установленного Windows-приложения. Обновление CLI тоже не доказывает обновление обслуживающего его app-server: релиз Codex отдельно учитывает их несовпадающие версии. [H2]

### 5. Почему «пропиши другой provider в subagent» недостаточно

В теге Codex `0.155.0` файл `core/src/agent/role.rs` применяет ограниченный набор overrides. Там есть `model`, reasoning, инструкции и некоторые другие параметры, **но нет `model_provider` и `model_providers`**. Родительская конфигурация клонируется, после чего применяются только разрешённые поля. Поэтому предыдущая рекомендация просто задать Go-provider внутри agent role не подтверждается реализацией. [H7]

У инструмента создания subagent предусмотрен выбор `model`, но не независимый `model_provider`. **Смена модели не равна смене endpoint/auth.** [H8]

Отсюда два разных случая:

```text
Без gateway:
Codex → один выбранный provider Go
        ├── модель-менеджер через Go
        └── Muse-workers через тот же Go

С общим маршрутизатором:
Codex → один локальный endpoint
        ├── native-модель → разрешённый ChatGPT/Codex route
        └── Muse-worker → OpenCode Go
```

Во втором случае Codex не обязан менять provider дочернего агента: endpoint общий, а роутер выбирает upstream по точному model ID. Это архитектурный вывод из ограничений роли, а не свидетельство успешно выполненного запуска на компьютере пользователя.

### 6. OpenCodex: подходящий кандидат, но прежние обещания были чрезмерными

**`lidge-jun/opencodex` — сторонний локальный gateway, не OpenAI и не OpenCode.** Проверенная версия — `2.58.0`. Он формирует каталог и использует штатную конфигурацию Codex. Это кандидат для требуемой смешанной схемы, а не новый harness. [H11] [H12] [H13]

В его провайдерной интеграции предусмотрен OpenCode Go, включая Muse Contributor, передачу session identity и чтение квот Go. Важен именно такой адаптер, а не произвольный proxy, который знает только Base URL. [H15]

#### Ограничения, которые нельзя скрывать

**Установка и доверие.** OpenCodex работает на Bun, устанавливается через npm/pnpm и требует Node; Bun подготавливается автоматически. Python не требуется, но это **не один Rust/Go executable без зависимостей**. Он изменяет конфиг Codex и создаёт собственное состояние; через него проходит модельный трафик. Есть также опциональные web/vision sidecars, способные расходовать ChatGPT-квоту. Их не включать без отдельного решения. Предусмотрены команды восстановления, но собственный backup и проверка diff обязательны. [H12]

**GUI не гарантирован.** Корректный каталог не отменяет Desktop allowlist. Документация также описывает reserve mode, в котором приложение переопределяет выбранную модель ещё до отправки запроса роутеру. Gateway не решает это автоматически. Не считать подмену Muse названием GPT приемлемым исправлением. [H13]

**Для ChatGPT → внешний worker исходная совместимая конфигурация — subagents v1.** OpenCodex рекомендует её по умолчанию: native v2-задачи могут приходить зашифрованными, а внешняя модель их не прочитает. Изменение режима применяется к новым сессиям. Это сохраняет классическое делегирование, но **не означает сохранение всех новых возможностей v2**. Простое разрешение пропуска encrypted payload не создаёт совместимость. [H14]

Релиз `2.58.0` содержит отдельные исправления steering, передачи результатов multi-agent и утечки private request-полей в сторонние upstream. Это подтверждает необходимость проверки версии и протокола, а не обещание абсолютной совместимости. [H11]

### 7. Какие функции надо сохранить и проверить

Цель — оставить инструменты, правила и управление работой **в Codex**, а не реализовывать второй agent loop внутри gateway.

| Слой | Критерий полноценной интеграции |
|---|---|
| Локальная работа | `AGENTS.md`, файловые операции, shell, тесты, skills, hooks и существующие MCP продолжают работать; permissions не расширены |
| Модели | Честные название/provider, корректный контекст и reasoning; выбранный маршрут сохраняется при перезапуске |
| Subagents | Действительно вызывается Muse; доступны результаты, steering, ожидание и отмена; нет fallback на дорогого parent |
| Goal | Невыполненный критерий вызывает продолжение работы; pause/resume и восстановление не теряют цель |
| Состояние | Tool-call IDs, reasoning/history, compaction, отмена и повторное подключение не повреждают диалог |
| Серверные функции | Web/voice/hosted tools и другие backend-зависимые функции проверяются отдельно; совместимость API не переносит их автоматически |
| Таймеры | Запуск по расписанию — на ожидаемой модели, с нужным cwd, без второго writer и без дублирующихся запусков |

Официальная документация подтверждает `/goal` в CLI и IDE. Текущая страница Scheduled относит интерфейс расписаний к desktop/web-приложению ChatGPT и прямо исключает его из CLI. **Не переносить это в обещание, что у любой установленной версии Codex Desktop есть тот же scheduler с custom-provider поддержкой.** Для локальных расписаний документация требует включённый компьютер и работающее приложение. [H16] [H17]

### 8. Практический порядок внедрения

Это предлагаемая приёмка, **ещё не выполненные тесты**.

1. **Зафиксировать среду:** версия Windows GUI, CLI и реально запущенного app-server; расположение `CODEX_HOME`; резервная копия конфигов и каталога. Не очищать login/history и не ставить ничего автоматически.
2. **Прямой Go-контроль:** проверить Responses-запрос и цикл «прочитать → изменить тестовый файл → запустить тест». Отдельно подтвердить фактический upstream и ID. Ответ «я Muse» доказательством не считать.
3. **Каталог и GUI:** собрать корректные `ModelInfo` под реальный backend; проверить `model/list`, CLI и Windows picker. Видимость, выбор и resume — три отдельных проверки.
4. **Смешанный маршрут:** только после анализа изменений gateway; сохранить native login, добавить один явный Go/Muse route. Локальный listener, изоляция credentials по адресату, без account rotation и подмен названий.
5. **Делегирование v1:** parent запускает двух Muse-workers на независимые read-only задачи; затем отдельный writer. Проверить модель, session IDs, результаты и cancel. После смены режима начинать новую сессию.
6. **Длительный тест:** Goal с намеренно падающим тестом; исправление, независимый review, compaction, перезапуск, потеря связи и 429. Ограничить бюджет и повторы. Ошибка должна быть видна, а не скрыта заменой модели.
7. **Расписание и откат:** подтвердить доступный Windows scheduler и модель scheduled-run; запретить конкурентную запись в `main`. После отката обычный Codex обязан работать без gateway и без потери пользовательского конфига.

В диагностике достаточно маршрута, модели, статуса, времени и usage; не сохранять токены доступа или весь код в незащищённые логи. Стабильность session identity для Go нужно соблюдать, а не подделывать общий ID на всех работников. [H15]

**Contributor — не приватный/ZDR-режим:** Go указывает использование prompts/completions для обучения Meta. Не отправлять секреты и не разрешённый для такой обработки клиентский код. Не включать расход Zen-баланса сверх Go-лимита незаметно. [H1]

### 9. Итоговое решение

**Сохраняем Codex как менеджер.** Сначала доказываем нативный прямой Go-маршрут и корректный каталог; для одновременного ChatGPT-manager и Muse-workers проверяем один прозрачный gateway, причём OpenCodex — конкретный кандидат, а не уже одобренная установка.

Если требование — **полностью штатный Windows picker + два независимых provider + все новые native v2-функции без изменений**, исследованные источники не дают готовой подтверждённой конфигурации. Нельзя выдавать `base_url + API key` за решение всей задачи.

**Приёмка завершена только тогда, когда нужные модели выбираются в реальном GUI, parent действительно делегирует Muse, Goal и инструменты проходят тесты, а цена и изменения системы контролируются.** До этого статус — исследованная схема интеграции, не production-ready подключение.

### Источники

Основные утверждения привязаны к источникам по месту. Исходники Codex и документация OpenCodex ниже зафиксированы на проверенных release tags; веб-документация провайдеров может обновляться.

- [S1 — OpenCode Go: клиенты, endpoints, privacy][H1]
- [S2 — Codex 0.155.0: релиз][H2]
- [S3 — Codex 0.155.0: schema конфигурации][H3]
- [S4 — OpenAI: advanced configuration][H4]
- [S5 — Codex 0.155.0: загрузка каталога моделей][H5]
- [S6 — Codex 0.155.0: ModelInfo / ModelsResponse][H6]
- [S7 — Codex 0.155.0: ограниченные agent-role overrides][H7]
- [S8 — Codex 0.155.0: schema multi-agent tools][H8]
- [S9 — Issue #37122: формат custom-provider каталога][H9]
- [S10 — Issue #19694: Windows Desktop picker][H10]
- [S11 — OpenCodex 2.58.0: релиз][H11]
- [S12 — OpenCodex 2.58.0: зависимости и изменяемые файлы][H12]
- [S13 — OpenCodex 2.58.0: picker, allowlist, reserve mode][H13]
- [S14 — OpenCodex 2.58.0: v1/v2 и encrypted task boundary][H14]
- [S15 — OpenCodex 2.58.0: провайдерные адаптеры, включая Go][H15]
- [S16 — OpenAI: Goal / long-running work][H16]
- [S17 — OpenAI: Scheduled tasks][H17]
- [S18 — Текущий публичный список моделей Go][H18]

[H1]: https://opencode.ai/docs/go/
[H2]: https://github.com/openai/codex/releases/tag/rust-v0.155.0
[H3]: https://github.com/openai/codex/blob/rust-v0.155.0/codex-rs/core/config.schema.json
[H4]: https://learn.chatgpt.com/docs/config-file/config-advanced
[H5]: https://github.com/openai/codex/blob/rust-v0.155.0/codex-rs/models-manager/src/manager.rs
[H6]: https://github.com/openai/codex/blob/rust-v0.155.0/codex-rs/protocol/src/openai_models.rs
[H7]: https://github.com/openai/codex/blob/rust-v0.155.0/codex-rs/core/src/agent/role.rs
[H8]: https://github.com/openai/codex/blob/rust-v0.155.0/codex-rs/core/src/tools/handlers/multi_agents_spec.rs
[H9]: https://github.com/openai/codex/issues/37122
[H10]: https://github.com/openai/codex/issues/19694
[H11]: https://github.com/lidge-jun/opencodex/releases/tag/v2.58.0
[H12]: https://github.com/lidge-jun/opencodex/blob/v2.58.0/docs-site/src/content/docs/getting-started/installation.md
[H13]: https://github.com/lidge-jun/opencodex/blob/v2.58.0/docs-site/src/content/docs/guides/codex-app-models.md
[H14]: https://github.com/lidge-jun/opencodex/blob/v2.58.0/docs-site/src/content/docs/guides/sub-agent-surface.md
[H15]: https://github.com/lidge-jun/opencodex/blob/v2.58.0/docs-site/src/content/docs/guides/providers.md
[H16]: https://learn.chatgpt.com/docs/long-running-work
[H17]: https://learn.chatgpt.com/docs/automations?surface=app
[H18]: https://opencode.ai/zen/go/v1/models

