# Local Models — Backend and Donor Map

Reviewed 2026-10-03. `DOC` is current official documentation; `SOURCE` is an inspected file/excerpt; `ISSUE` is a reported case, not prevalence or proof of a present regression; `OBSERVED` refers only to the [executed test](qualification.md). Source commits/observed versions identify evidence, not installation requirements.

## 1. llama.cpp

**DOC/SOURCE:** [server README](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md), [function calling](https://github.com/ggml-org/llama.cpp/blob/master/docs/function-calling.md). Inspected blobs: server README `4a37bc2f3dbafdbf7e21ad48d0ee89bdc3a50db9`, function-calling guide `28eecbe25749637d766bc64c7b10c700bd5a8c01`.

The current server describes CPU/GPU inference, quantized models, continuous batching, parallel decoding, structured output and compatible Chat Completions, Responses and Anthropic Messages routes. Do not reduce it to a legacy text-completion-only service, but do not infer full wire equivalence from the feature list either.

The function-calling guide uses `--jinja`. Native and generic template handlers differ; generic support may consume more tokens and be less efficient. Parallel tool calling is model-dependent and the documented path requires an explicit request option. The claim that a generic handler exists for many models is not a coding-quality guarantee.

Useful native observations include health, properties, model inventory and metrics. The server documentation explicitly excludes `GET /health`, `/props`, `/models` and `/metrics` from model wake/idle-timer reset in its sleep behavior. Qualify the installed server/router mode; don't make a completion request for passive health.

**Take:** standalone native server as a whole external dependency, HTTP/SSE protocol, documented model/slot/status semantics. **Keep in Rust ELIOT:** route selection, auth, resource attribution, capability evidence and existing agent/task lifecycle. Don't embed libllama into Store merely to offer a local provider.

**Do not assume:** a slot is a durable agent session; theoretical model context equals available per-slot context; every template can use the full MCP catalog; a disconnected request is immediately cancelled. Model load/fit/parallel settings need effective readback, not fixed release recipes.

## 2. vLLM

**DOC:** [CPU installation](https://docs.vllm.ai/en/latest/getting_started/installation/cpu/), [tool calling](https://docs.vllm.ai/en/latest/features/tool_calling/), [Codex integration](https://docs.vllm.ai/en/latest/serving/integrations/codex/), [Claude Code integration](https://docs.vllm.ai/en/latest/serving/integrations/claude_code/).

**OBSERVED:** a current official CPU release completed the narrow Linux HTTP test in this PR. It did not test GPU, coding tools, Responses/Messages or full ELIOT integration.

Use Linux vLLM as an external inference service. A Windows ELIOT/harness connects over the selected API; no Windows-hosted vLLM, WSL installation or remote repository checkout is a prerequisite.

The official Codex guide configures `wire_api = "responses"` and a vLLM provider base. The Claude guide uses the Anthropic-compatible path and provider environment. These are different protocols. Verify main and auxiliary/subagent model mappings so a local route does not silently call the normal cloud provider.

Tool calls depend on the model, chat template and selected parser; automatic tool choice requires the documented serving configuration. A reasoning parser is a separate capability. Model-specific settings cannot be copied as one universal preset. Structured/forced tool output also does not prove that the model will select or use tools correctly in normal work.

**Take:** vLLM's batching/KV/cache scheduler and maintained HTTP server, not a second scheduler in ELIOT. Use current upstream CPU/GPU installation paths and actual capability checks; do not pin a research release or guess a wheel ABI filename. The added CPU job resolves the current official release asset dynamically and checks its upstream digest.

**Do not assume:** a metrics scrape gives exact agent-level VRAM; all models support all API dialects; response IDs survive process restart; an API key protects every native administration/status route; shutdown is clean because inference succeeded. The test's shutdown warnings remain open qualification evidence.

## 3. LM Studio / llmster

**DOC:** [REST API and comparison](https://lmstudio.ai/docs/developer/rest), [tool use](https://lmstudio.ai/docs/developer/openai-compat/tools), [authentication](https://lmstudio.ai/docs/developer/core/authentication), [headless service](https://lmstudio.ai/docs/developer/core/headless), [TTL/auto-evict](https://lmstudio.ai/docs/developer/core/ttl-and-auto-evict), [MCP via API](https://lmstudio.ai/docs/developer/core/mcp).

The current documented API split is important:

| Endpoint | Normal ownership/use |
|---|---|
| `/v1/chat/completions` | Caller/harness supplies history and executes returned custom tools. |
| `/v1/messages` | Anthropic-shaped caller/harness path. |
| `/v1/responses` | Supports custom tools and native MCP facilities; select the intended ownership path explicitly. |
| `/api/v1/chat` | Native stateful chat/MCP path, with native load/prompt-progress events; not the same custom-inline-tool/history contract. |
| `/api/v1/models` and model-management endpoints | Rich inventory and explicit load/unload/download administration. |

Do not send a Chat Completions body unchanged to native chat or claim native MCP support for every compatibility endpoint. The native tool host and a harness must not both execute the same call.

llmster provides a headless option; the GUI is not required for every deployment. ELIOT normally attaches to the installed service. JIT may make a downloaded model appear in the compatible model catalog and load it on demand. TTL and auto-eviction can unload/change residency between requests. These are material resource effects, not a reason to report all catalog entries ready or silently evict another agent's model.

**Take:** native inventory/load-state observations and compatible inference behind one Rust client boundary. Keep native management separate from normal participant use. Preserve actual authentication settings; a supplied `Authorization` header is not proof it is enforced. A read-only inventory should not implicitly start the daemon.

## 4. Unsloth: two distinct integration routes

**DOC:** [authenticated API](https://unsloth.ai/docs/basics/api), [Unsloth Start](https://unsloth.ai/docs/integrations/unsloth-start), [vLLM export](https://unsloth.ai/docs/basics/inference-and-deployment/vllm-guide), [GGUF export](https://unsloth.ai/docs/basics/inference-and-deployment/saving-to-gguf).

### A. Existing Studio/Desktop API

Current Unsloth documentation describes Desktop on Windows/macOS/Linux and an authenticated API backed by `llama-server` for loaded models, with Chat Completions, Responses and Messages integration. It is no longer accurate to describe Unsloth solely as a training library with no serving path.

Connect the selected loaded model through its actual endpoint and API key. Studio's built-in web search/code execution and tool-call healing are optional execution/translation layers, not automatic ELIOT permissions or proof of the original model's correctness.

The current API guide states that server-side tools may default on for loopback `unsloth run` and off for non-loopback. ELIOT must not inherit those defaults blindly. A harness-owned tool path needs server tools excluded by supported configuration/readback; changing a user's already running shared server is not a passive discovery action.

The guide's reasoning example labelled as disabling thinking passes `--reasoning on`; this is a documentation inconsistency, not a configuration to copy. Validate requested/effective settings against the installed interface. The documented quick-tunnel example also has a streaming limitation: do not silently create such a tunnel or treat it as proof of a qualified streaming remote path.

### B. Exported model supplied to another backend

A fine-tuned export may be served through llama.cpp/LM Studio, or as a supported merged/adapter model through vLLM. GGUF, merged HF weights and LoRA-only artifacts are not interchangeable.

Preserve base-model/adapter, tokenizer, chat template, EOS/BOS and quantization provenance when available. The Unsloth export guide warns that mismatched templates/stopping configuration can make another runner repeat text or produce bad output. Do not mark the export deployable merely because a weight file exists.

Training, conversion, download and model activation are separate explicitly requested operations. None is a prerequisite for connecting an already served model, and this extension does not add a trainer or automatically deploy every new checkpoint.

### Unsloth Start as a configuration donor

**SOURCE excerpt:** [`unsloth_cli/commands/start.py`](https://github.com/unslothai/unsloth/blob/8c344a6e74e237a0e0150bacb6b657b76bb5b272/unsloth_cli/commands/start.py), especially `_connect`, load-setting comparison and the preload check.

Useful mechanisms: whole named harness configuration, temporary/session-scoped provider overrides, attach versus start distinction, and checking a model is actually loaded before skipping load. The source explicitly warns that a listing can contain cached unloaded entries and that a load can evict the shared model.

Take those semantics into Rust route preparation. Do not invoke this Python CLI as ELIOT's hidden persistent controller, select the first catalog model, bootstrap keys in a logged health probe, use `--persist` to change the user's default configuration, or stop a temporary shared server merely because the first parent CLI exited. Some launcher/provider combinations are narrower than the general API; qualify the selected GGUF/backend/harness tuple.

## 5. Goose — relevant Rust implementation donor

**SOURCE:** [declarative LM Studio definition](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose-providers/src/declarative/definitions/lmstudio.json), inspected blob `273bbe8a952c69b18c9921e291635c570fe90c96`; [shared OpenAI-format Rust code](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose-provider-types/src/formats/openai.rs), inspected relevant search excerpts; [provider guide](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/documentation/docs/getting-started/providers.md).

Goose uses one shared wire engine plus a small declarative LM Studio definition, dynamic models and streaming. That is the useful reuse pattern: don't duplicate a complete HTTP client, schema parser and agent loop for every local brand.

The inspected Rust-format excerpts preserve different reasoning field names and discuss avoiding an arbitrary fixed completion limit for unknown local models. ELIOT should preserve the same field distinctions, but use the configured effective context/output budget rather than copy an unbounded default.

The inspected LM Studio definition composes a host with `/v1/chat/completions`, while a UI placeholder includes a full endpoint. This illustrates why ELIOT must type origin/API prefix/endpoint separately. Its `requires_auth=false` is not a suitable general remote-security rule.

Reuse a complete suitable Rust unit/library only after its license, dependencies and boundary are reviewed. This pass did not copy donor code or perform a complete Goose security audit. Goose may be an optional external Rust coding harness later; importing its whole orchestration/storage loop into ELIOT is unnecessary for these provider connections.

## 6. User reports that become negative cases

| Primary report | Observed/reported limitation | ELIOT consequence |
|---|---|---|
| [LM Studio #2411](https://github.com/lmstudio-ai/lmstudio-bug-tracker/issues/2411) | An OpenAI-compat plugin discards `reasoning_content`; this is a plugin-path report, not every LM Studio API. | Preserve supported streamed fields and record omissions; do not merge reasoning into final text by accident. |
| [LM Studio #1143](https://github.com/lmstudio-ai/lmstudio-bug-tracker/issues/1143) | Cline disconnect/prompt-processing behavior and bearer ignored when server authentication was disabled. | Test actual authentication and cancellation boundaries; a closed socket is not immediate compute termination. |
| [LM Studio #2376](https://github.com/lmstudio-ai/lmstudio-bug-tracker/issues/2376) | Reporter compares ROCm/Vulkan with the same model and a large tool catalog; final output differed/truncated. | Qualify model/backend/template/context as a tuple; don't infer catalog usability from model name or prescribe a global backend switch from one report. |
| [LM Studio #988](https://github.com/lmstudio-ai/lmstudio-bug-tracker/issues/988) | Historical reasoning-effort request/UI mismatch. | Requested, configured and observed behavior remain different evidence. No claim that this historical defect persists in all releases. |

The owner-provided swarm audits independently describe oversized tool contexts, health CLIs restarting shared services, duplicate native ownership and process-per-agent overhead. They support small role surfaces and shared observers. Their historical pins, fixed agent counts and old cleanup scripts are not copied into this program.

## 7. Reuse decision

**Use whole maintained inference products externally; reuse the existing Rust HTTP/SSE, authorization and runtime architecture internally.** Add narrow backend capability/model-state adapters, not four forks, four workflow systems or an automatic installation platform.

Direct vendor docs define the wire; donor source helps implementation; user reports define failure cases; only the recorded test demonstrates an executed contour. None of them establishes general coding quality, Windows/GPU parity or fleet scale for ELIOT.
