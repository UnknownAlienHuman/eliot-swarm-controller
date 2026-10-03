# Local Models — Rust Integration Contract

Дата: 2026-10-03. Применяется совместно с [README](README.md) и [настройками автоматизации](../agent-operations/configuration.md). Новые имена методов ниже — проектируемый API.

## 1. Владельцы

| Объект | Владелец |
|---|---|
| Task, Attempt, assignment, candidate, acceptance | Существующий ELIOT Store |
| Контекст, tool loop, native session/turn, результат | Выбранный native harness |
| Tensor execution, batching, KV cache, model residency | Inference server |
| Предпочтения моделей/executors | Менеджер в пределах своих действующих прав |
| Чужой уже запущенный server/desktop application | Внешний владелец; ELIOT только подключён |
| Собственный явно запущенный ELIOT worker process | Существующий Rust process-owner с проверяемой lineage |

Несколько harness могут использовать один inference server, но не управлять одновременно одной native session. Model slot/KV entry не является Task/Attempt/session identity. Локальная модель не получает больше прав, чем облачная.

## 2. Backend descriptor и connection

Descriptor: `id`, `kind`, `connection_ref`, `lifecycle_owner`, `execution`, `resource_group`, `locality`, разрешённые management actions и revision.

`kind`: `llama_cpp`, `vllm`, `lmstudio`, `unsloth`. Kilo имеет отдельный runtime kind `kilo`; он не входит в этот enum inference backends.
`execution` начального расширения: только `inference_only`.
`lifecycle_owner`: `external` по умолчанию, `eliot_owned` только для реально зарегистрированного собственного запуска.

Connection содержит origin, base path каждого используемого dialect, TLS policy и credential reference. URL нормализуется **один раз**: origin, `/v1` и `/chat/completions` не должны конкатенироваться дважды. Отдельно валидируется возможность gateway с path prefix; обычный URL join с начальным `/` не должен стирать prefix.

Свободный URL от модели, Issue или ответа сервера не становится адресом подключения. Нет сетевого сканирования, пересылки credentials на redirect другого origin или SSRF к cloud metadata. Нужные LAN origins разрешаются при настройке — не запрещать частную сеть, которая и требуется vLLM.

Credentials остаются вне Task, prompts и обычных результатов. Для удалённого non-loopback HTTP нельзя предполагать конфиденциальность; использовать проверяемый TLS либо уже настроенный защищённый канал. Не обходить проверку сертификата ради зелёного статуса.

## 3. Что знает каталог

`runtime.catalog` агрегирует backend projection, но не делает generation запросов.

Каждая модель имеет:

```text
backend ID / actual served model ID
alias отдельно от фактической identity
artifact/source identity когда server сообщает её
downloaded / loaded / advertised — отдельные состояния
quantization / tokenizer / chat template / parser, если наблюдаемы
configured and effective context / output limits
protocol and per-operation capabilities
sampling defaults and explicit request overrides
observation time / coverage / gaps
```

`unknown` — допустимый честный ответ. `/v1/models` не доказывает tool support, loaded state у каждого backend, корректный Responses протокол или качество кода. Не скачивать model/config автоматически для заполнения недостающего поля.

Выбор `/v1/responses` отдельно от Chat Completions, Anthropic Messages и native vendor APIs. Общий порт и похожий JSON не означают взаимозаменяемые contracts.

## 4. Capability evidence

Вместо одной галочки `OpenAI compatible`:

```text
chat / responses / messages
streaming with terminal semantics
function tools / parallel tools / forced tool choice
structured output dialect
reasoning presentation fields
token usage semantics
request lookup / request cancellation
model list / load / unload
context and memory observations
server-side tool execution policy
```

Для каждого поля: `unknown`, `documented`, `observed_supported`, `observed_rejected`; ссылка на конфигурацию/наблюдение. Истечение времени не превращает supported в false, а изменённые существенные model/template/server settings помечают наблюдение stale.

Никаких fixed release gates. При обновлении сервиса проверять использующийся контракт, а не требовать прежний binary. Нельзя сохранять старое `observed_supported` для другой модели, quant или parser. При неизвестном обязательном tool dialect сообщить точную несовместимость этой роли; простые разрешённые text/read use cases не блокируются.

Проверочная генерация — явный manager-request или разрешённая им автоматика. Не запускать дополнительную модель перед каждой Task ради флага readiness.

## 5. Привязка к harness

Предпочтительный путь — существующий runtime route с отдельно выбранным provider, model и options.

- OpenCode: provider зарегистрирован в **установленном** service; route указывает его ID и model/variant. Общий web guide для OpenCode не доказывает, что его config JSON подходит к ELIOT V2. Проверить native config schema и readback. Не использовать CLI polling, способный восстановить/перезапустить общий сервис.
- Codex: поддержанный local-provider или зарегистрированный custom provider с фактически требуемым wire API. Проверять assistant items, tools, tool-result IDs и multi-turn continuation — не только `/models` и простой текст. Native app-server остаётся владельцем session; наличие endpoint `/responses` не доказывает полную Codex-совместимость.
- Kilo Code: отдельный Rust HTTP/SSE adapter, Kilo config/schema и directory identity по [kilo-code.md](kilo-code.md). Не переименовывать OpenCode V2 route: происхождение fork не доказывает одинаковый wire contract.
- Другой harness: использовать его существующий native local-provider путь, только после проверки полномочий и протокола. Не объявлять Muse/Claude подписочную модель локальной по совпавшему display name.

Предпочтительно per-launch/per-profile переопределение без записи глобальных settings. Если API требует редактировать файл, это явно запрошенное manager action с точным местом, readback и сохранением чужих настроек. Нельзя переписать auth.json, глобально заменить модель всем линиям или молча скопировать сырой ключ в Task.

Подмена модели действует на новые admissions. Уже начатая session не мигрирует незаметно. Для исправления неправильного provider у текущей работы менеджер использует штатный continuation/replacement путь с сохранением результата.

## 6. Протокольный поток

ELIOT не проксирует каждый токен между harness и inference server без потребности. Основное наблюдение берётся из native harness; server metrics — дополнительные сведения о compute.

Если adapter/probe непосредственно читает SSE:

1. Транспортные chunks не равны SSE events; корректно собирать UTF-8 и многострочный `data`.
2. Tool deltas собирать по choice/index/ID до завершённого вызова. Пустое имя промежуточного delta ещё не дефект; окончательно отсутствующее — дефект.
3. Проверять имя и аргументы против разрешённого каталога. Текст, похожий на JSON tool call, не исполнять.
4. EOF, HTTP 200 или `[DONE]` не заменяют проверку требуемого terminal/finish reason и полноты данных.
5. `length`, `cancelled`, malformed/incomplete tool output и server error не являются успешной сдачей.
6. Reasoning только из поддержанного опубликованного поля/события; отсутствие такого поля — gap, не повод реконструировать скрытые рассуждения.
7. Метрики, token usage, cached tokens и native reasoning budget не складывать без описанной counter semantics.

Не переносить provider SSE parser в Store. Владелец native tool loop отвечает за выполнение инструментов и продолжение разговора.

## 7. Retry и cancellation

Controller Operation ID, harness turn ID, HTTP request ID и inference response ID различаются.

`X-Request-Id` — корреляция, а не серверная идемпотентность. Потерянный POST/stream после возможного исполнения даёт unknown; не создавать второй Task или новый native prompt в догонку. Нативные retries harness фиксируются как его транспортное поведение; ELIOT не запускает ещё один независимый retry-loop сверху.

Если чистая генерация повторяется самим harness до исполнения tools, это не идентично повтору уже выполненного tool effect. Исполненные команды/файловые операции сохраняют свои receipts. Нельзя заново выполнить effect лишь потому, что provider вернул другой tool_call_id после retry.

Закрытие HTTP-соединения не доказывает остановку server compute. Native per-request cancellation используется только если операция/ID поддержаны и разрешены; иначе статус остаётся `cancellation_requested/unknown` и учитывается resource uncertainty. Не вызывать global abort, sleep, unload или stop shared server для отмены одного агента.

## 8. Память, batching и справедливость

Считать ресурс по физической группе, а не по числу aliases. Два profiles LM Studio/Unsloth, указывающие на один underlying процесс/GPU pool, не создают две независимые квоты.

Registry хранит endpoint/process identity где наблюдаема; configured `resource_group` позволяет объединить сервисы на одном GPU. Не считать один только hostname доказательством одинакового pool.

Dashboard показывает отдельно: active/waiting requests, model loaded/loading/evicted, observed memory/context limits, текущую очередь и пробелы измерения. Poll/read разово обслуживает весь service instance, а не каждый агент/наблюдатель.

Admission использует уже существующий capacity path. Не строить второй inference scheduler поверх vLLM scheduler. Batching и KV управляет server; ELIOT ограничивает отправку согласно manager settings и наблюдаемой ёмкости. Цифры тысяч registered agents не превращаются в тысячи concurrent generations.

Смена между моделями на одном GPU может приводить к постоянной загрузке/выгрузке. Разрешить manager выбрать предпочтение к уже загруженной модели **внутри разрешённого списка**, но не заменять нужную модель незаметно. Пауза между tool steps не означает завершённую Task и не даёт права выгрузить модель чужого клиента.

## 9. Lifecycle и безопасность backend

### llama.cpp

Подключение к существующему серверу не делает ELIOT его владельцем. Read-only health/catalog не меняют slots/models. Saved KV state не является durable conversation authority. Management endpoints и raw RPC не публикуются внешним агентам.

### vLLM

Основное размещение по решению владельца — vLLM в выбранном WSL2 distribution на той же Windows-машине; ELIOT и Kilo сначала Windows-native. Детали и границы установки — [wsl-vllm.md](wsl-vllm.md). Remote Linux остаётся допустимой альтернативой, но больше не является обязательным сценарием.

Locality не даёт lifecycle ownership. Вызов `wsl.exe --exec` способен запустить остановленную distribution: обычный каталог так не делает. Отсутствие listener возвращает unavailable/unknown; запускает только менеджер или его разрешённая автоматика. Windows launcher PID не доказывает состояние Linux workers. Нужны именованная distribution и точный guest service/process identity; нельзя выполнять `wsl --shutdown`, `--terminate` или глобальный kill ради одного запроса.

WSL GPU и Windows LM Studio/llama.cpp могут делить физическую память: общий resource group, ограничение context/batch, без двойного учёта capacity. Неполные GPU metrics остаются unknown, а не нулевой нагрузкой.

Документация текущего server предупреждает, что `--api-key` охраняет не все маршруты. Ограничить proxy/auth точным allowlist нужных endpoints, а не выставлять целый server наружу. `/invocations`, administration, profiler, weight-update, file/plugin paths не предоставлять inference credential.

### LM Studio

Нативный `/api/v1/chat` и API Responses с integrations могут сами исполнять MCP. В inference-only route такие integrations не подключаются. Никакого двойного исполнения одних tools в LM Studio и harness.

Auth требуется включить явно для shared/non-loopback deployments. JIT/TTL/auto-evict — свойства model residency, не Task lifecycle. Настройку человека не менять при discovery.

### Unsloth

Studio используется либо как внешний inference server, либо как источник уже экспортированных артефактов. Автоматическое обучение/переквантование при запуске Task не требуется.

В inference-only route **server-side code/search tools отключены на уровне процесса** там, где это поддержано. Loopback сам по себе не защита от обхода ELIOT tool grants. Уникальные API keys, которые CLI печатает при запуске, редактируются до долговременного сохранения stdout.

Экспорт должен сохранять согласованный tokenizer/chat template/EOS и правила LoRA. Выбор другого формата модели не разрешает произвольное `trust_remote_code`, исполняемый pickle или скрипт из репозитория модели. Download/export/execute остаются отдельными разрешёнными действиями.

## 10. MCP и настройки

Начальный deferred group `local-models` содержит только:

```text
inference.backend.list       bounded known endpoints/configuration summaries
inference.backend.get        state/capabilities/gaps for one allowed backend
inference.backend.configure  manager-scoped edit; no implicit process launch
inference.model.list         actual native catalogue, loaded/downloaded distinction
inference.probe.run          one explicit bounded protocol probe Operation
```

Результат probe читается через существующий `operation.get`; отдельный probe scheduler/result store не нужен. Selection выполняется единым `runtime.profile.*`, каталог агрегируется единым `runtime.catalog`. Не создавать дополнительные «режимы автоматизации локальных моделей».

Первые версии не обязаны предоставлять start/download/load/unload. Когда нужны, соответствующий native action включается в существующий action registry, с manager authority и проверенной process ownership. Unsupported native API не имитируется shell-скриптом.

У роли executor только нужные read/result инструменты. Model management и проверки, расходующие ресурсы, не входят в его eager toolbox. Местная маленькая модель может не поддерживать tool search: это capability harness, а не следствие MCP или OpenAI-compatible. Использовать уже спроектированный fixed small core/explicit group activation; не отправлять весь каталог.

## 11. Реализация по исходникам

| Unit | Изменение |
|---|---|
| `src/config.rs` | Добавить типизированные backend references и проверку connection policy; не смешивать секреты с route JSON |
| новый `src/inference/` | Rust descriptors, bounded metadata HTTP, capability/report types и probe workers; отдельно четыре native management adapters |
| `src/runtime/codex/`, `src/runtime/opencode_v2/` | Прочитать фактическую структуру при реализации; связать backend selection с уже существующим provider/model contract |
| новый `src/runtime/kilo/`, module guide `modules/kilo/README.md` при реализации | Rust client/DTO/event projection через RuntimePort; существующий ELIOT MCP остаётся отдельным направлением подключения |
| `src/runtime/mod.rs`, existing prepared/prerequisites | Сохранять command/outcome semantics; локальный provider не образует нового fake root/session |
| existing runtime profile + Store handlers | Одна revision/owner authority, retained requests, no external I/O внутри transaction |
| `src/mcp/catalog.rs`, `src/mcp/profiles.rs`, CLI | Зарегистрировать только реально подключённые методы и deferred permissions |
| существующие monitoring/capacity/process-owner | Общий service-level sampler и accounting, без thread/process на неактивного агента |
| `Cargo.toml` | Использовать имеющиеся reqwest/serde/SSE primitives; явно закрыть TLS gap без version pins |

Если текущий `runtime.profile.*` ещё проект, сначала дописать его общий владелец; не обходить его вторым local-models config store. Точное расположение файлов перепроверяется на свежем main: пути — точки интеграции, не указание переписывать существующие модули.

Выпускать вертикально: typed configure → native catalog → выбранный harness → tool round-trip → обычный result reader. Возможность только распечатать URL не является законченной интеграцией.
