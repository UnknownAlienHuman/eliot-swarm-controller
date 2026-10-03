# Local Models + Kilo Code — llama.cpp, vLLM/WSL2, LM Studio, Unsloth

**Дата проверки:** 3 октября 2026. **База:** `a0a931ef02aa4627a58f3076e17462272b13e755`.
**Статус:** программа расширения и контракты, не реализованный адаптер и не live-квалификация.
Номера релизов и source SHA в свидетельствах описывают наблюдавшееся состояние; они не ограничивают установку будущими/старыми версиями.

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

[Kilo Code](kilo-code.md) подключается отдельным Rust runtime-модулем; Kilo — harness, не пятый inference engine. [WSL2/vLLM](wsl-vllm.md) определяет размещение, драйверы, transport и lifecycle.

## 2. Граница существующего проекта

Сохраняются [RuntimeCommand/RuntimeOutcome](../../src/runtime/mod.rs), [runtime notes](../runtime-notes.md), [настройки менеджера](../agent-operations/configuration.md) и [канонические MCP-представления](../mcp-canonical-surfaces-and-topologies.md).

На проверенной базе уже есть Rust HTTP/SSE-зависимости, native runtime routes, immutable Operations и примеры явного `modelProvider` для Codex. Название модели с `/` не является командой сменить provider. Общий реестр backend-моделей и описанные ниже методы ещё не реализованы. На свежей базе уже появились coordination, automation, MCP catalog и read-only launch preview: расширение не должно повторно реализовывать их или считать productive launcher готовым по одному preview.

`runtime.catalog` и `runtime.profile.*` используются как единый интерфейс предпочтений, когда их production handlers подключены. В новом коде не создавать второй профиль автораспределения или отдельный scheduler локальных моделей. Сначала реализуется ручной путь; отсутствие cron/Goal/GitHub не должно его блокировать.

В текущем Cargo.toml reqwest настроен без default features и без явного TLS backend. Перед объявлением поддержки HTTPS на удалённом Linux нужно включить подходящий TLS backend и проверить сертификаты. Это адресное требование к транспорту, а не повод обновлять всё дерево зависимостей.

## 3. Четыре системы и их роли

| Система | Начальный путь | Что требует отдельной проверки |
|---|---|---|
| llama.cpp | Подключение к существующему `llama-server`; Chat Completions для совместимого harness | Tool template/parser, Responses semantics, контекст/parallel slots, собственность процесса |
| vLLM | Windows-контроллер подключается к WSL2 на той же машине; remote Linux опционален | Hardware/backend, parser, chat template, Responses/tool round-trip, auth всех используемых маршрутов |
| LM Studio | Обычный inference API на Windows; llmster при явно выбранном headless-размещении | Auth, точный loaded model, JIT/auto-evict, API dialect; native MCP path отдельно |
| Unsloth | Готовый export через llama.cpp/LM Studio/vLLM **или** уже запущенный Studio inference API | Tokenizer/template/EOS, фактический downstream engine, отключённые server-side tools |

Поддерживаемый upstream endpoint не означает, что модель годится для длинной coding-задачи. Пригодность привязана к сочетанию harness, модели, tokenizer/template, parser, quantization, контекста и протокола. Ссылки и статус каждого утверждения — в [donors.md](donors.md).

## 4. Удобный ручной сценарий

1. Менеджер добавляет профиль backend, выбирая уже запущенный сервис и защищённую локальную connection reference. Каталог читается без запуска генерации.
2. Из фактически доступных моделей выбирает executor/auditor profile и нужный runtime route. Поддержанный native provider регистрируется локально с readback; предпочтение применяется к следующим заданиям.
3. Запускает обычную Task через существующий launcher. В dashboard видны harness/session, backend/model, очередь генерации и точность наблюдения — отдельно.

Это не включает автоматику. Менеджер по желанию подключает уже существующие helpers **от своего имени**. Смена модели не перезапускает работающий агент и не меняет его уже принятый запрос. Установленные совместимые версии принимаются по протоколу/возможностям, не по равенству номера релиза.

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

## 6. Что реализовать

| Срез | Полный результат |
|---|---|
| L1 | Rust backend descriptors, строгая схема, scoped конфигурация, read-only каталог и состояния |
| L2 | Привязка backend/model к существующему runtime profile; адресный native apply/readback и сохранение действующих settings |
| L3 | Rust Kilo adapter по [kilo-code.md](kilo-code.md): attach/create/send/readback, scoped MCP; затем ручная Task с локальной моделью и обычным результатом |
| L4 | Общий мониторинг памяти/запросов, capacity grouping, отмена и unknown outcomes через существующих владельцев |
| L5 | Отложенная MCP-группа `local-models`, integrated dashboard и пояснение несовместимых возможностей |
| L6 | Опциональное owned управление загрузкой/службой; только разрешённые адаптером операции, без автоматической установки |
| L7 | WSL2 на Windows, Linux CPU/GPU и native Windows квалификация; Kilo tool/restart/concurrency/locality сценарии |

Одна менеджерская рабочая копия; изменения в существующих путях должны иметь настоящего потребителя. Программный код сразу на Rust, минимальный manager Clippy. Запрошенные владельцем Linux-интеграционные проверки — отдельное явно разрешённое исследование; WSL2 теперь выбран владельцем как целевое размещение, но его установка, GPU и сквозной Windows→WSL путь проверяются отдельно.

## 7. Навигация

- [contracts.md](contracts.md): права, API, lifecycle, protocol/capacity/retry, MCP, исходные файлы.
- [donors.md](donors.md): официальные источники, исходники доноров, точные единицы переиспользования.
- [qualification.md](qualification.md): что реально проверено, setup failure и воспроизводимый следующий прогон.
- [kilo-code.md](kilo-code.md): Rust runtime-модуль, API mapping, MCP, hooks и границы локального provider.
- [wsl-vllm.md](wsl-vllm.md): выбранное владельцем WSL2-размещение и безопасный setup/recovery.

**Текущий результат:** документация исследована; в Linux предпринята установка CPU wheel, но загрузка остановилась на DNS. Ни inference, ни tool-calling, ни GPU throughput этим проходом не подтверждены.
