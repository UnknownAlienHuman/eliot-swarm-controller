# Local Models — Проверки и Честный Статус

Дата: 2026-10-03. Эта запись описывает фактически доступное исследовательское окружение, а не Windows-машину владельца. См. [donors.md](donors.md) для upstream источников.

## 1. Сохранённая предыдущая попытка в Linux-среде ассистента

| Проверка | Наблюдение |
|---|---|
| ОС | Linux 6.18.44, x86_64, glibc 2.41 |
| CPU | 5 видимых CPU; AVX2 и AVX512F присутствуют |
| Python | 3.13.5 |
| GPU | NVIDIA/DRI device nodes отсутствуют |
| Native backend packages | vLLM и transformers не установлены |
| Среда установки | Создан отдельный virtualenv, без изменения системного Python |
| Wheel | Взята реальная ссылка CPU x86 asset из текущего release metadata |
| Попытка установки | Завершилась exit 2: DNS lookup failure до загрузки wheel |
| Inference / tools / restart / load | **НЕ ВЫПОЛНЕНО** |

Запрошен CPU wheel наблюдавшегося релиза vLLM `0.30.0` от 2026-09-22. Это факт попытки установки, не фиксированная требуемая версия. Правило `--no-deps` ограничивало первую проверку доступности wheel; успешная установка одного wheel всё равно не означала бы готовой среды. Реальный fetch не начался, поэтому vLLM engine не импортировался и не запускался.

Сокращённое исходное сообщение:

```text
error: Failed to fetch: <official vLLM CPU release asset>
Caused by: client error (Connect)
Caused by: dns error
Caused by: failed to lookup address information: Temporary failure in name resolution
```

Отдельные Git/curl/download проверки показали ту же недоступность сетевой загрузки из runtime. Web/connector-чтение документации работает по другому каналу и не даёт контейнеру установленных binaries.

Нет GPU — ограничение GPU-прогонов, но **не** объяснение невозможности CPU. CPU-path здесь остановился на сети. Mock-сервер, echo tool или обычный PyTorch calculation не выдаются за vLLM qualification. Никаких paid cloud/GPU ресурсов для обхода этого ограничения не создавалось.

## 2. Следующий реальный Linux CPU прогон

Запускается в Linux CI/runner с разрешённой загрузкой. Отдельно владелец выбрал WSL2 на Windows для рабочего vLLM deployment; CPU-прогон не заменяет проверку этого пути.

1. Прочитать текущую [официальную CPU-инструкцию](https://docs.vllm.ai/en/stable/getting_started/installation/cpu/), определить ISA/Python/glibc/memory. Создать isolated environment.
2. Получить current release metadata и выбрать CPU wheel по реальным supported tags; не зашивать release number и `manylinux` suffix. При необходимости использовать официальный CPU container channel, только когда Docker реально доступен.
3. Установить dependencies/официальный OpenMP runtime согласно **текущей** инструкции. Записать фактически установленные версии; не требовать их равенства в следующих запусках.
4. Взять небольшой, разрешённый владельцем, tool-capable model artifact. Выбор модели отдельный test input, не product default. Проверить template/parser/tokenizer и заранее ограничить download bytes, threads, context, output и duration.
5. Bind только loopback, auth включена. Не включать серверные MCP/shell tools. Не запускать HTTP service на машине пользователя.
6. Выполнить текст, streamed ответ, synthetic tool round-trip, malformed/schema failure, одновременные isolated conversations и disconnect/recovery. Synthetic tool возвращает локальную константу и ничего не пишет/не вызывает сеть.
7. Остановить только созданный этим прогоном service и проверить descendants/listener. Сохранить разрешённые журналы и result evidence.

Реальный tool test: заставить модель выбрать `inspect_contract`, получить завершённые валидные arguments, передать строго типизированный result с **тем же call ID**, затем получить итоговый ответ. Отсутствие такого цикла не позволяет назвать backend пригодным для executor, даже если текст генерируется.

## 3. Отдельные GPU и Windows проверки

GPU: конкретный accelerator/backend, model quant, actual context, batch/parallel/KV defaults, one/two/four concurrent requests, memory pressure, queue latency, cancellation. CPU throughput не переносится на GPU и наоборот.

Windows: llama-server, LM Studio и Unsloth при реально поддержанной установке. Проверить Unicode paths, скрытый process launch, сохранение external ownership, отсутствие окон/оставшихся процессов и совместимость с выбранным native harness. Эти проверки не выполнены в текущем Linux-контейнере.

### 3.1. WSL2 и Kilo

Рабочая топология и setup — [wsl-vllm.md](wsl-vllm.md). Проверить Windows→WSL loopback, выбранную distribution, GPU/driver и общий physical pool; suspend/resume и shutdown WSL не приравнивать к завершению Tasks. Установка на компьютере владельца ещё не выполнена.

Для [Kilo](kilo-code.md) отдельно: current schema, два worktree на общем server, async admission/readback, инструменты ELIOT MCP и возврат tool result к модели. Server health не доказывает прогресс agent loop. Проверить restart/gap, stable parent-child attribution, invalid auth, no cloud fallback и отсутствие двойного restart shared service. Нативный in-process JS/TS hook ABI не выдаётся за Rust integration или blocking veto.

В этом повторном проходе container-загрузка Kilo OpenAPI также вернула DNS `Temporary failure in name resolution`; source review выполнен через GitHub/web. Native Kilo binary не запускался. Это не новый inference benchmark.

## 4. Матрица допуска маршрута

Для каждой комбинации backend/model/harness использовать отдельную запись:

```text
DOCUMENTED
DISCOVERED
TEXT_OBSERVED
TOOL_ROUNDTRIP_OBSERVED
HARNESS_WORKFLOW_OBSERVED
RECOVERY_OBSERVED
LOAD_OBSERVED
FAILED
NOT_RUN
```

Не делать одну общую галочку на продукт. Разрешённый read-only анализ может быть полезен до полной coding-квалификации; конкретный неизвестный обязательный capability отображается пользователю и ограничивает только зависимую операцию.

## 5. Обязательные сценарии

| Граница | Что доказать |
|---|---|
| Discovery | Каталог не запускает model load, download, shell, global config mutation или новый агент |
| Model identity | В ответе выбранная served model; имя alias не скрывает другую модель/quant/remote route |
| Protocol | Chat, Responses, Messages квалифицируются отдельно; supported JSON schema реально принимается |
| Tools | Один round-trip, несколько вызовов если нужно, fragmented deltas, missing IDs, unknown tool, malformed arguments |
| Error | Ошибка после HTTP 200 и оборванный stream не становятся successful completion |
| Retry | Потерянный ответ не создаёт повторный Task/native prompt и не повторяет уже выполненный tool effect |
| Context | Модель получает маленький нужный toolset; overflow сообщает gap/error, не молча обрезает требования |
| Unsloth/LM Studio | Server-owned tool loop не выполняется параллельно выбранному harness |
| Network | Неверный key/certificate/redirect отвергается; private-network доступ не требует публичного домена |
| Capacity | Несколько aliases одного backend не создают независимые квоты; stats не дублируются на каждый dashboard |
| Residency | TTL/auto-eviction видны; устаревшее loaded state не считается работающей моделью |
| Lifecycle | Закрытие клиента не убивает чужой server; unload/stop не выполняются по age/silence |
| Preferences | Новые jobs выбирают новые настройки; старые сохраняют фактический execution route |
| Locality | Model/child/summary/rerank requests не уходят в cloud вне явно разрешённой policy |
| WSL2 | Query не запускает остановленную distribution; Linux worker survives wrapper exit -> unknown, не release; pool общий с Windows GPU clients |
| Kilo | Health != agent progress; prompt_async != mailbox; API directory/session scoped; Rust adapter и MCP не расширяют native-role права |
| ELIOT path | Прочитано задание → выполнен tool → изменён disposable candidate → обычный result/submission без ложной acceptance |

Prompt injection в модели, названиях tools, config.json/tokenizer metadata и server error text не меняет profile/credential/target. Тесты не используют пользовательские секреты или production репозиторий.

## 6. Evidence record

Минимальный результат — время, test source revision, OS/hardware, backend observed build, выбранная модель/quant/template/parser когда наблюдаемы, конкретные endpoint capabilities, команды и exit codes, terminal/tool correlation, latency/memory samples, gaps и cleanup result.

Модельные/конфигурационные identity фиксируют выполненный прогон. Это не global pin и не запрет обновлений. Сырые prompts с закрытым кодом, API keys, absolute private paths и дампы всего VRAM не требуются.

## 7. Проверка документационного пакета

В текущем проходе проверяются UTF-8, локальные markdown links, JSON/TOML-синтаксис примеров, отсутствие deployment identifiers и применимость additive patch. Это не проверка runtime handlers, моделей или конкурентного выполнения. Внутренний модуль/probe ELIOT в дальнейшем реализуется на Rust; временный скрипт подготовки этого документа не является компонентом продукта.
