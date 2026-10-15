# Local Models — Donor and Protocol Evidence

Sources rechecked read-only on 2026-10-14 against source baseline `57b0ce4` (published `main` at task start). `DOC` — official documentation, `CODE` — source at an identified revision, `OBSERVED` — a historical local observation. `DOC/CODE` do not mean `LIVE` or shipped in ELIOT.

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

**Version check (2026-10-14):** the official [release page](https://github.com/vllm-project/vllm/releases) lists vLLM `v0.31.0` as latest, released 2026-10-05. The historical qualification record names `v0.30.0` (released 2026-09-22); that version's CPU wheel tag was reported as `manylinux_2_39` in [upstream issue #58270](https://github.com/vllm-project/vllm/issues/58270). The issue reports a compatibility fix and a maintainer said it would be included in the upcoming `v0.31.0`. This read-only check did not inspect the `v0.31.0` CPU asset, so it does not assert its tag or prove installation compatibility. A future qualification must inspect the actual release asset and guest packaging tags.

## 3. LM Studio — plain inference и native agent API разделяются

**DOC:** [REST API](https://lmstudio.ai/docs/developer/rest), [chat](https://lmstudio.ai/docs/developer/rest/chat), [authentication](https://lmstudio.ai/docs/developer/core/authentication), [headless](https://lmstudio.ai/docs/developer/core/headless), [TTL/auto-evict](https://lmstudio.ai/docs/developer/core/ttl-and-auto-evict).

**Брать:** REST model catalogue/management, локальный server или llmster, уже выбранный модельный backend. SDK на JS/Python для ELIOT не обязателен.

Endpoint comparison различает `/api/v1/chat`, Responses, Chat Completions и Messages. Native chat умеет stateful/MCP, но не заменяет произвольный custom-tool/messages request. У integrations нужны точные allowed tools; отсутствие списка не является узким разрешением.

Current REST documentation exposes `loaded_instances` and per-instance configuration such as context length. Authentication is disabled by default until explicitly enabled in server settings. A localhost URL therefore does not prove loaded state or provide a production policy for a shared service. JIT/auto-evict remains a model-residency property, not Task lifecycle.

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

Проверенный immutable research snapshot: `591edd47cf2cfea4957d720c607cf2a4def8673d`.
[LM Studio descriptor](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose-providers/src/declarative/definitions/lmstudio.json) использует общий OpenAI engine, dynamic model list и отдельный endpoint/auth configuration. Related units: `crates/goose-providers/src/openai.rs`, `crates/goose-providers/src/declarative.rs`.

**Брать:** декларативный descriptor + общий transport, отдельный native management adapter, модели из server catalogue.
**Не копировать вслепую:** `requires_auth=false` для LAN и сложение base path. В просмотренном descriptor `base_url` добавляет `/v1/chat/completions`, а UI placeholder уже содержит полный путь. Это повод проверить normalization, а не утверждение о воспроизведённом баге Goose.

Whole Goose остаётся optional external harness, если отдельно выбран менеджером; его session store/agent loop не встраиваются в Store ELIOT. Для metadata HTTP не нужна тяжёлая новая agent framework dependency.

## 6. OpenCode и Codex — существующие harness, не четыре новых runtime

**DOC, checked 2026-10-14:** [OpenCode providers](https://opencode.ai/docs/providers/), [Codex advanced configuration](https://developers.openai.com/codex/config-advanced/).

OpenCode documents custom providers through its `provider` configuration and `baseURL`; this is an upstream design option, not proof that the repository-pinned ELIOT OpenCode V2 server accepts that exact schema. Any future ELIOT configuration change must validate the installed native schema and read back the effective state. Keep the harness as owner of its shell/MCP/session loop.

Codex документирует OSS/local provider и custom provider. Для Responses необходимо тестировать реальный tool cycle/continuation, не только старт ответа. Флаги конкретного binary обнаруживаются, не выводятся из старого номера CLI. Никакой замены ChatGPT auth или общего профиля других агентов.

## 7. Current ELIOT boundaries checked on `main` `57b0ce4`

| Существующее | Использование |
|---|---|
| `crates/swarm-contracts/src/runtime.rs`, `crates/swarm-kernel-host/src/runtime/mod.rs` | Shared command/outcome contract; Store-owned operation and binding identity |
| `crates/swarm-kernel-host/src/store/task_prompt.rs`, `store/launcher.rs`, `store/operations.rs` | Immutable Attempt-snapshot prompt, exact launch ancestry and retained Operation readback |
| `crates/swarm-kernel-host/src/runtime/opencode_v2/`, `crates/swarm-kernel-host/src/runtime/codex.rs` | Existing native-harness routes; local backend registration is not part of these routes today |
| `crates/swarm-kernel-host/src/config.rs`, `crates/swarm-adapter-opencode/src/config.rs`, `runtime/opencode_v2/owned_service.rs` | Existing OpenCode owned-service config. `model_catalog = offline|refresh` controls that service's catalog source, not a PR24 backend registry |
| `crates/swarm-contracts/src/mcp_catalog.rs`, `crates/swarm-mcp/src/mcp/profiles.rs` | Current MCP method/profile registry; no local-models group or `inference.*` methods are registered |
| `crates/swarm-automation/src/lib.rs`, current Store automation handlers | Existing manager automation definition/configuration ownership; do not add a local inference preference store |
| `reqwest`, `eventsource-stream`, `serde_json` | Existing transport/serialization pieces; inspected reqwest declarations disable default features and select no TLS feature. Verify the resolved feature graph before claiming HTTPS support |
| Proposed `crates/swarm-kernel-host/src/inference/` and `runtime/kilo/` | These directories are absent at `57b0ce4`; future work only |

Existing dependency constraints do not authorize new version pins. The local-model descriptor and Kilo adapter do not exist at this baseline; this PR does not add dependencies or claim remote HTTPS qualification.

## 8. Kilo Code и WSL2

**DOC/CODE:** [CLI runtime](https://kilo.ai/docs/contributing/architecture/cli-runtime), [local model providers](https://kilo.ai/docs/ai-providers/openai-compatible), [MCP CLI](https://kilo.ai/docs/automate/mcp/using-in-cli), [plugins](https://kilo.ai/docs/automate/extending/plugins). Конкретные paths, ограничения hooks, prompt queue и API mapping — в [kilo-code.md](kilo-code.md), без копирования другого Task store.

**DOC:** [vLLM GPU installation](https://docs.vllm.ai/en/stable/getting_started/installation/gpu/), [WSL networking](https://learn.microsoft.com/en-us/windows/wsl/networking), [CUDA on WSL](https://docs.nvidia.com/cuda/wsl-user-guide/index.html), [WSL systemd](https://learn.microsoft.com/en-us/windows/wsl/systemd). WSL2 поддерживает Linux serving рядом с Windows; driver/compute support и фактический endpoint должны проверяться отдельно. Порядок установки и process ownership описаны в [wsl-vllm.md](wsl-vllm.md).

## 9. License and reuse boundary checked 2026-10-14

| Donor | Repository/app license evidence | Scope note |
|---|---|---|
| llama.cpp | [Repository license](https://github.com/ggml-org/llama.cpp/blob/master/LICENSE): MIT | Review each vendored third-party component separately. |
| vLLM | [Repository metadata](https://github.com/vllm-project/vllm/blob/main/pyproject.toml): Apache-2.0 | Do not treat model weights or optional plugins as covered by the engine license. |
| Goose | [Repository](https://github.com/aaif-goose/goose): Apache-2.0 | The reviewed LM Studio descriptor is an immutable source snapshot, not an ELIOT dependency. |
| Kilo Code | [Repository LICENSE at the reviewed SHA](https://github.com/Kilo-Org/kilocode/blob/76bcfd40be616a72f4697b3041565f322245b462/LICENSE): MIT | Product/service terms and later files may differ; the root license does not qualify a provider or runtime integration. |
| LM Studio | [Desktop App Terms](https://lmstudio.ai/app-terms): proprietary application terms | The REST API is documented; the app is not an OSS donor. |
| Unsloth | [Repository README](https://github.com/unslothai/unsloth): core package Apache-2.0; optional Studio UI AGPL-3.0 | Service terms and assets/models remain separate; no broader license claim. |

These are source-bound reuse notes, not legal review or permission to vendor. Documentation and source inspection do not qualify any backend. See [qualification.md](qualification.md) for the historical failure and explicit `NOT_RUN` boundaries.
