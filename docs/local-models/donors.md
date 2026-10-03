# Local Models — Donor and Protocol Evidence

Проверено 2026-10-03. `DOC` — официальная документация, `CODE` — прочитанный источник, `OBSERVED` — реально выполненная локальная проверка. `DOC/CODE` не означают `LIVE`.

Указанные source revisions фиксируют источник исследования, не требуют установки фиксированного релиза. Интеграция должна обнаруживать доступные capabilities, а не откатывать совместимое новое ПО.

## 1. llama.cpp — использовать внешний сервер целиком

**DOC/CODE:** [server README](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md), [function calling](https://github.com/ggml-org/llama.cpp/blob/master/docs/function-calling.md).

Полезны CPU/GPU inference, batching, monitoring и общие API dialects. Но текущий `/v1/responses` описан как преобразование к Chat Completions: не наследовать полноценную persistent Responses semantics по одному имени пути.

Function calling зависит от подходящего template и parser. Native template предпочтительнее generic fallback; поддержка нескольких tools и parallel execution не следует из того, что один tool сработал. Агрессивная KV quantization требует отдельной проверки качества инструментов.

**Брать:** `llama-server` без встраивания в Rust-процесс ELIOT; read-only native metadata и точно поддержанные API.
**Не брать:** внутренние slots как agent sessions, неподтверждённый exact steer, raw RPC на общий публичный endpoint, авто-download при просмотре каталога.

## 2. vLLM — Linux inference service, не ещё один orchestrator

**DOC:** [OpenAI-compatible server](https://docs.vllm.ai/en/stable/serving/online_serving/openai_compatible_server/), [tool calling](https://docs.vllm.ai/en/stable/features/tool_calling/), [CPU install](https://docs.vllm.ai/en/stable/getting_started/installation/cpu/), [Codex integration](https://docs.vllm.ai/en/stable/serving/integrations/codex/).

**Брать:** внешний serving engine и его batching; protocol configuration конкретной model family; официальный native-harness маршрут. Для auto tool choice необходимы подходящие parser/template и параметры. Модель может задавать generation defaults, поэтому effective settings важнее одного запроса.

**Не брать:** весь Ray/Kubernetes/второй scheduler как обязательную зависимость ELIOT. Предпочтительное размещение владельца теперь WSL2; удалённый Linux API — альтернатива. Никакой второй копии ELIOT, Task DB или Git worktree в WSL для этого не нужно.

Security guide server отдельно предупреждает: API key не закрывает весь маршрутный набор. Корреляционный request header не доказывает dedup. Remote deployment использует отдельный минимальный authenticated inference surface; unknown outcomes не перезапускают весь service.

**OBSERVED release metadata:** при проверке `/releases/latest` ответил `v0.30.0`, опубликованный 2026-09-22. CPU x86 wheel реально имеет platform tag `manylinux_2_39`, тогда как пример CPU-guide строит URL с `manylinux_2_34`. Установка должна выбирать совместимый asset из release metadata/packaging tags, а не конструировать имя файла из старого примера. Это не рекомендация закрепить v0.30.0.

## 3. LM Studio — plain inference и native agent API разделяются

**DOC:** [REST API](https://lmstudio.ai/docs/developer/rest), [chat](https://lmstudio.ai/docs/developer/rest/chat), [authentication](https://lmstudio.ai/docs/developer/core/authentication), [headless](https://lmstudio.ai/docs/developer/core/headless), [TTL/auto-evict](https://lmstudio.ai/docs/developer/core/ttl-and-auto-evict).

**Брать:** REST model catalogue/management, локальный server или llmster, уже выбранный модельный backend. SDK на JS/Python для ELIOT не обязателен.

Endpoint comparison различает `/api/v1/chat`, Responses, Chat Completions и Messages. Native chat умеет stateful/MCP, но не заменяет произвольный custom-tool/messages request. У integrations нужны точные allowed tools; отсутствие списка не является узким разрешением.

Включённый по умолчанию JIT/auto-evict может вызывать переключение residency между запросами разных моделей. Manager должен видеть это, а не получать выдуманный постоянный loaded pool. Localhost endpoint по умолчанию без обязательной авторизации — не production policy для shared service.

**Не брать:** UI-состояние за durable ELIOT workflow, закрытие окна за смерть server, native MCP auto execution поверх уже работающего tool loop.

## 4. Unsloth — два полезных пути, не «только обучение»

**DOC:** [Studio](https://unsloth.ai/docs/new/studio), [API](https://unsloth.ai/docs/basics/api), [Codex](https://unsloth.ai/docs/basics/codex), [LM Studio export](https://unsloth.ai/docs/basics/inference-and-deployment/lm-studio), [inference troubleshooting](https://unsloth.ai/docs/basics/inference-and-deployment/troubleshooting-inference).

Современный Studio предоставляет inference API; GGUF-маршрут использует llama-server. Поэтому поддержать отдельно:

- уже готовый export → существующий llama.cpp/LM Studio/vLLM;
- attach к уже запущенному Unsloth inference API.

API-документация упоминает Responses во введении, но таблица маршрутов не полная. Матрицу конкретного установленного API подтверждать запросом и native Codex guide; не заменять пробу догадкой.

Server-side code/search tools на loopback могут быть включены по умолчанию. В inference-only route отключать их явно; иначе маленький toolset ELIOT не ограничивает инструменты самого Unsloth. Self-healing tool output — не право менять команды или target ради успеха.

Экспортный troubleshooting связывает бессмысленный/циклический output с несогласованными template/EOS. Сохранять model lineage/tokenizer/template, не советовать просто поднять max tokens.

**Не брать:** обучение при каждом запуске, бесконтрольную сборку downloaded remote code, automatic public tunnel и второй tool authority.

## 5. Goose — конкретный Rust-донор структуры provider

**CODE:** репозиторий теперь [aaif-goose/goose](https://github.com/aaif-goose/goose); старый block/goose перенаправляется.

Просмотренный research snapshot: `591edd47cf2cfea4957d720c607cf2a4def8673d`.
[LM Studio descriptor](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose-providers/src/declarative/definitions/lmstudio.json) использует общий OpenAI engine, dynamic model list и отдельный endpoint/auth configuration. Related units: `crates/goose-providers/src/openai.rs`, `crates/goose-providers/src/declarative.rs`.

**Брать:** декларативный descriptor + общий transport, отдельный native management adapter, модели из server catalogue.
**Не копировать вслепую:** `requires_auth=false` для LAN и сложение base path. В просмотренном descriptor `base_url` добавляет `/v1/chat/completions`, а UI placeholder уже содержит полный путь. Это повод проверить normalization, а не утверждение о воспроизведённом баге Goose.

Whole Goose остаётся optional external harness, если отдельно выбран менеджером; его session store/agent loop не встраиваются в Store ELIOT. Для metadata HTTP не нужна тяжёлая новая agent framework dependency.

## 6. OpenCode и Codex — существующие harness, не четыре новых runtime

**DOC:** [OpenCode providers](https://opencode.ai/docs/providers/), [Codex advanced configuration](https://developers.openai.com/codex/config-advanced/).

OpenCode документирует llama.cpp/LM Studio через custom provider с `baseURL`. Это готовый способ оставить shell/MCP/session loop на стороне harness. Обязательная оговорка: web-пример не разрешает записывать его в несовместимую схему установленного OpenCode V2; ELIOT проверяет native config/readback.

Codex документирует OSS/local provider и custom provider. Для Responses необходимо тестировать реальный tool cycle/continuation, не только старт ответа. Флаги конкретного binary обнаруживаются, не выводятся из старого номера CLI. Никакой замены ChatGPT auth или общего профиля других агентов.

## 7. Что переиспользовать внутри ELIOT

| Существующее | Использование |
|---|---|
| `src/runtime/mod.rs` | Command identity, admission/start/outcome разделены; inference не создаёт fake native session |
| `src/runtime/opencode_v2`, `src/runtime/codex` | Existing route/session/tool/result authority |
| `src/automation/config.rs` и общий runtime-profile contract | Manager preference, revision и next-admission settings; без отдельного enabled-mode |
| `reqwest`, `eventsource-stream`, `serde_json` | Metadata/probe transport; TLS/backend и bounded parsing проверяются отдельно |
| `src/mcp/catalog.rs` / profile checks | Deferred local-models group и реальные handler-to-tool mappings |
| Process owner / capacity / monitoring | Один sampler/accounting на physical backend pool |
| Existing artifacts/Operations | Результаты квалификации, error evidence и параметры реального запуска |

Наличие существующих exact dependency constraints не является разрешением добавлять новые version pins в это расширение. Изменения старой общей dependency policy не смешивать с подключением моделей.

## 8. Kilo Code и WSL2

**DOC/CODE:** [CLI runtime](https://kilo.ai/docs/contributing/architecture/cli-runtime), [local model providers](https://kilo.ai/docs/ai-providers/openai-compatible), [MCP CLI](https://kilo.ai/docs/automate/mcp/using-in-cli), [plugins](https://kilo.ai/docs/automate/extending/plugins). Конкретные paths, ограничения hooks, prompt queue и API mapping — в [kilo-code.md](kilo-code.md), без копирования другого Task store.

**DOC:** [vLLM GPU installation](https://docs.vllm.ai/en/stable/getting_started/installation/gpu/), [WSL networking](https://learn.microsoft.com/en-us/windows/wsl/networking), [CUDA on WSL](https://docs.nvidia.com/cuda/wsl-user-guide/index.html), [WSL systemd](https://learn.microsoft.com/en-us/windows/wsl/systemd). WSL2 поддерживает Linux serving рядом с Windows; driver/compute support и фактический endpoint должны проверяться отдельно. Порядок установки и process ownership описаны в [wsl-vllm.md](wsl-vllm.md).

## 9. Предел исследования

Документация и конкретный Rust-донор прочитаны; ни один backend не объявляется готовым по этому основанию. Failure modes превращаются в [проверки](qualification.md), а не в запрет подключать совместимое новое ПО.
