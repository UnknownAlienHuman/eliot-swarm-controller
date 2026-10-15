# Optional Local Models + Kilo Code Design — llama.cpp, vLLM/WSL2, LM Studio, Unsloth

**Source baseline used:** `57b0ce4` (published `main` at task start); PR24 body/head/comments reread 2026-10-14.
**Status:** optional documentation/design delivery only. This program is not a prerequisite for core delivery, not an install procedure, and not a shipped local-model feature. L1–L7 are proposed; local backend configuration, a Kilo route, and native qualification remain **NOT_RUN**.
Release numbers and source SHAs below identify dated research evidence, not installation pins or compatibility promises.

## 1. Решение

Локальная модель подключается к выбранному агентному harness как provider. Не следует писать четыре новых агентных цикла или встраивать inference engine в Store.

```text
manager / его явно включённая автоматика
                  |
                  v
ELIOT Task / Attempt / route / Operations
                  |
                  v
выбранный harness, включая Kilo Code: tools, MCP, session, recovery
                  |
                  v
выбранный протокол и конкретная модель
       |              |              |               |
 llama-server      vLLM API       LM Studio API    Unsloth API
 Windows/Linux     WSL2/Linux    Windows/Linux    installed service
```

ELIOT-owned каталог, проверка конфигурации, HTTP-клиент управления, мониторинг, lifecycle и MCP — **Rust**. Внешние llama.cpp/vLLM/LM Studio/Unsloth используются целиком, на их собственных языках. Отдельный Python/JS-прокси внутри ELIOT, LiteLLM как обязательная прослойка и переписывание inference kernels не нужны.

Сервер модели не получает право назначать задания, запускать произвольные инструменты, принимать работу или публиковать код. Инструменты выполняет один выбранный harness; Task/acceptance остаются у ELIOT. У inference-only подключения LM Studio/Unsloth не включаются собственные агентные/MCP-интеграции.

**Решение владельца:** основной vLLM deployment теперь WSL2 на его Windows-компьютере. ELIOT, Git/worktree и первоначально Kilo остаются Windows-native; WSL обслуживает inference. Удалённый Linux endpoint остаётся альтернативой. Это план установки, не заявление о её выполнении.

[Kilo Code](kilo-code.md) потребует отдельного Rust runtime-модуля; Kilo — harness, не пятый inference engine. Этот маршрут предложен, но его нет в текущем runtime registry. [WSL2/vLLM](wsl-vllm.md) описывает выбранное владельцем возможное размещение, а не установку или действующий backend.

## 2. Граница существующего проекта

Существующая граница описана в [runtime contract](../../crates/swarm-contracts/src/runtime.rs), [module adapter boundary](../agent_swarm.module-contract-v2.md), [runtime notes](../runtime-notes.md), [manager configuration](../agent-operations/configuration.md) и [canonical MCP surfaces](../mcp-canonical-surfaces-and-topologies.md). Store owns Task, Attempt, Operation, assignment and acceptance. It builds the immutable dispatch prompt from the retained Attempt snapshot; a harness owns its native session/tool loop and cannot accept an ELIOT Task.

At `57b0ce4`, OpenCode V2 and Codex native routes exist. OpenCode's native model catalog and session configuration are scoped to that harness. The existing owned-service `model_catalog = "offline" | "refresh"` controls OpenCode's catalog source; it is not the PR24 multi-backend registry. The current MCP source catalog does not register `inference.backend.*`, `inference.model.*`, `runtime.catalog`, `runtime.profile.*`, or a `local-models` group. Manager configuration docs describe shared catalog/profile targets, but the exact method identifiers are not present in the registered MCP method catalog. Treat those names in this package as proposed integration targets, not available APIs.

The inspected crate manifests disable reqwest default features and do not select a TLS feature directly. Before claiming HTTPS support, verify the final binary's feature graph, TLS backend and certificate validation. This is a future transport qualification, not a reason to update dependency policy in this documentation PR.

## 3. Четыре системы и их роли

| Система | Начальный путь | Что требует отдельной проверки |
|---|---|---|
| llama.cpp | Подключение к существующему `llama-server`; Chat Completions для совместимого harness | Tool template/parser, Responses semantics, контекст/parallel slots, собственность процесса |
| vLLM | Windows-контроллер подключается к WSL2 на той же машине; remote Linux опционален | Hardware/backend, parser, chat template, Responses/tool round-trip, auth всех используемых маршрутов |
| LM Studio | Обычный inference API на Windows; llmster при явно выбранном headless-размещении | Auth, точный loaded model, JIT/auto-evict, API dialect; native MCP path отдельно |
| Unsloth | Готовый export через llama.cpp/LM Studio/vLLM **или** уже запущенный Studio inference API | Tokenizer/template/EOS, фактический downstream engine, отключённые server-side tools |

Поддерживаемый upstream endpoint не означает, что модель годится для длинной coding-задачи. Пригодность привязана к сочетанию harness, модели, tokenizer/template, parser, quantization, контекста и протокола. Ссылки и статус каждого утверждения — в [donors.md](donors.md).

## 4. Будущий ручной сценарий (design only)

1. В будущей реализации менеджер сможет описать уже запущенный backend через защищённую локальную connection reference; чтение каталога не должно запускать генерацию.
2. После реального native apply/readback менеджер сможет связать доступную модель с разрешённым harness route и предпочтениями для следующих admissions.
3. Обычная Task пойдёт через существующий Store/launcher path. Dashboard должен различать harness/session, backend/model, очередь генерации и полноту наблюдений.

Это целевой сценарий, а не текущая возможность. Он не включает автоматику: любое будущее подключение helpers остаётся действием менеджера от своего имени. Смена предпочтения должна влиять на новые admissions и не менять уже принятый запрос. Совместимость определяется фактически используемым протоколом и возможностями, не равенством номера релиза.

Для первого полезного подключения не обязательны model downloader, обучающий pipeline, новый agent loop, общий proxy или fleet benchmark. Уже работающий endpoint и совместимый harness достаточны. Начальная настройка должна занимать один scoped apply, а не цепочку разрешений Root.

## 5. Пример будущей конфигурации

**Это проект схемы расширения, а не конфигурация, которую текущий `main` уже принимает.** Connection values разрешаются локально, не из текста Task.

```toml
schema = "eliot-local-models-proposal-v1"

[[backends]]
id = "desktop"
kind = "lmstudio"
connection_ref = "local-models/desktop"
lifecycle_owner = "external"
execution = "inference_only"
resource_group = "workstation-model-memory"
locality = "local_machine"

[[backends]]
id = "wsl-inference"
kind = "vllm"
connection_ref = "local-models/wsl-inference"
lifecycle_owner = "external"
execution = "inference_only"
resource_group = "workstation-model-memory"
locality = "local_machine"
```

WSL distribution/user/service identity выбираются локально, не добавляются в публикуемый route как личные пути. Одинаковый resource_group в примере означает выбранный общий физический GPU/RAM pool, а не требование объединять независимые машины.

Connection file отдельно хранит разрешённый origin, dialect-specific base paths и credential reference. Реальные адреса, пароли, локальные usernames и абсолютные пути не публикуются. В профиле исполнителя — существующий route, backend ID, model ID из каталога и поддержанные options. Ни installer version, ни SDK version, ни модель-рекомендация не зашиваются как обязательные.

## 6. Предлагаемые increments — не реализованы в `main` `57b0ce4`

| Срез | Предлагаемый результат |
|---|---|
| L1 | Rust backend descriptors, строгая схема, scoped конфигурация, read-only каталог и состояния |
| L2 | Привязка backend/model к существующему runtime profile; адресный native apply/readback и сохранение действующих settings |
| L3 | Rust Kilo adapter по [kilo-code.md](kilo-code.md): attach/create/send/readback, scoped MCP; затем ручная Task с локальной моделью и обычным результатом |
| L4 | Общий мониторинг памяти/запросов, capacity grouping, отмена и unknown outcomes через существующих владельцев |
| L5 | Отложенная MCP-группа `local-models`, integrated dashboard и пояснение несовместимых возможностей |
| L6 | Опциональное owned управление загрузкой/службой; только разрешённые адаптером операции, без автоматической установки |
| L7 | WSL2 на Windows, Linux CPU/GPU и native Windows квалификация; Kilo tool/restart/concurrency/locality сценарии |

Одна менеджерская рабочая копия; изменения в существующих путях должны иметь настоящего потребителя. Программный код сразу на Rust, минимальный manager Clippy. Запрошенные владельцем Linux-интеграционные проверки — отдельное явно разрешённое исследование; WSL2 теперь выбран владельцем как целевое размещение, но его установка, GPU и сквозной Windows→WSL путь проверяются отдельно.

Все L1–L7 — **PROPOSED / NOT_IMPLEMENTED**; ни одна строка не является merge gate для core delivery. Native model, Kilo, WSL2/GPU, tool-round-trip и recovery qualification остаются **NOT_RUN**.

## 7. Навигация

- [contracts.md](contracts.md): права, API, lifecycle, protocol/capacity/retry, MCP, исходные файлы.
- [donors.md](donors.md): официальные источники, исходники доноров, точные единицы переиспользования.
- [qualification.md](qualification.md): что реально проверено, setup failure и воспроизводимый следующий прогон.
- [kilo-code.md](kilo-code.md): Rust runtime-модуль, API mapping, MCP, hooks и границы локального provider.
- [wsl-vllm.md](wsl-vllm.md): выбранное владельцем WSL2-размещение и безопасный setup/recovery.

**Доказанный результат:** историческая Linux CPU wheel попытка остановилась на DNS до загрузки. Ни inference, ни tool-calling, ни GPU throughput, ни Kilo/WSL2 workflow этим фактом не подтверждены. Эта документационная сверка сама не запускала установку, WSL, service или модель.
