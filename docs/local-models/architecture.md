# Local Inference — Rust Integration Contract

Revision 1 · 2026-10-03 · proposed behavior. [Backend evidence](backends-and-donors.md) and [executed qualification](qualification.md) are separate from this design.

## 1. Three identities, not four new harnesses

Keep `agent runtime`, `inference endpoint` and `model selection` separate:

```text
Task / Attempt / manager / worktree
  -> native harness binding and generation
      -> endpoint identity / service instance
          -> selected model / effective serving configuration
```

The harness owns conversation, tools, native session/turn and child-agent behavior. The inference server owns weights, GPU/CPU execution, batching and its request lifecycle. ELIOT owns work admission, scoped authority, durable Operations and evidence.

Do not synthesize a native session from an HTTP connection, model name, vLLM request ID or llama.cpp slot. Do not mistake final generated text for completed coding work. Existing `RuntimeOutcome` accepted/applied/rejected/unknown semantics and exact Task submission/acceptance remain unchanged.

### Supported integration paths

1. **Harness-owned tools — first delivery.** Configure a compatible installed harness to use the selected server in a per-launch/per-binding configuration. Observe/control the harness through its existing adapter. Codex requires an actually compatible Responses path; Claude requires Messages; Chat Completions alone proves neither.
2. **Native server-owned tools — optional separate capability.** Where documented, the native service may execute selected MCP tools. It receives an explicitly scoped ELIOT tool connection, and its tool/request identity and results must be qualified. Do not simultaneously run the same call through a harness tool executor.
3. **Raw finite inference.** An explicitly requested model probe or non-agent inference can use a typed Rust HTTP client, but cannot claim file editing, agent recovery or Task completion. A future standalone Rust agent executor would be a distinct RuntimePort implementation, not a hidden loop added to Store for this extension.

No shared core branches on model brands. Common protocol handling lives behind the provider boundary; backend-specific reads/actions stay in their adapters. New model choices on an already compatible route are configuration, not another core build.

## 2. Minimal configuration

Use the #23 runtime-profile/configuration path for model/executor preferences. Add typed endpoint references to it rather than another profile service. Example below is a proposed schema, not accepted main configuration:

```toml
[inference.endpoints.desktop]
backend = "lm_studio"
connection_ref = "LOCAL_CONNECTION_HANDLE"
resource_pool = "desktop-accelerator"
lifecycle = "attach"

[profiles.local_writer]
role = "executor"
apply_changes = "next_assignment"
fallback_on = []

[[profiles.local_writer.candidates]]
route = "codex-local"
model = "MODEL_ID_FROM_ENDPOINT_CATALOG"

[profiles.local_writer.candidates.inference]
endpoint_id = "desktop"
wire_api = "responses"
tool_execution_owner = "harness"
```

The local connection record holds origin, API prefix, authentication reference and transport restrictions. Separate origin/API prefix/full endpoint path in types; reject ambiguous values instead of appending `/v1` twice. Use the explicit model chosen by the manager, not the first catalog entry.

Profile names are not permissions. Endpoint registration and discovery do not enable automations, load models or change a global harness configuration. An explicit launch may allow loading its selected model, but cannot evict unrelated work. Preview explains that consequence. Changes affect future admissions; running work retains its effective configuration.

A Linux inference node can serve a Windows harness. A configured URL does not confer remote shell, filesystem or container-control access. Local-only policy covers every main, child, summarization and auxiliary model route; an unqualified or unconfigurable auxiliary route remains a gap, never an implicit cloud fallback.

## 3. Capabilities and model inventory

Maintain one bounded inventory per configured endpoint/auth scope, not one catalog poller per Participant. Project it through `runtime.catalog` and the shared dashboard.

Keep catalog states distinct: `listed`, `downloaded`, `loadable`, `resident`, `warming`, `ready_for_profile`, `unavailable`, `unknown`. A compatible server's `/v1/models` does not universally mean resident models, complete capabilities or effective context length.

Describe capability axes independently:

- API dialect and actual endpoint: Chat Completions, Responses, Messages, native REST.
- Streaming text, usage, provided reasoning, tool arguments and terminal/error events.
- Named/automatic/required tool selection, multiple calls, parallel calls, schema/JSON support.
- Tool execution owner and whether server-side tools can be reliably excluded.
- Context length, effective per-request/slot limits, supported modalities and tokenizer/template information.
- Read-only model/service state, supported load/unload actions, request cancellation/readback and restart behavior.
- Harness requirements including auxiliary models, deferred tool search, role messages, multimodal input and continuation.

Evidence records `configured`, `documented`, `observed`, `probe_passed` and `unavailable/unknown`, with its scope/date. One successful hello request is not a coding-agent qualification. Never derive capability solely from a model-name prefix or `GET /health`.

Where available, retain the model alias plus artifact/base/adapter identity, quantization, tokenizer/template and server generation. Record effective parser/options and backend/device information once per material configuration. Do not hash huge weights on every request or require artifact identities a remote server cannot provide; report the evidence gap. These records identify a run and do not require frozen software releases.

### Probes are explicit effects

Catalog/health inspection must use endpoints documented not to warm/load models where available. `inference.probe` is a separate manager-authorized finite action: it may consume compute, load weights or allocate cache. Keep its synthetic input, limits and output separate from project Tasks. It must never secretly download a model, execute model-suggested tools or start a coding agent.

A probe profile states what it proves: transport/auth only, generation, streamed schema, harmless tool-call roundtrip, or full harness/ELIOT scenario. Missing capabilities make the affected profile unavailable, not the entire local stack.

## 4. Rust data path and wire semantics

Reuse the current `reqwest`, `eventsource-stream`, Tokio, serde and bounded artifact mechanisms. First deliver endpoint inspection and launch configuration; do not intercept every working harness request with an unnecessary proxy. If a controlled proxy is later required for attribution or policy, it must have explicit ownership and preserve the selected dialect.

The current manifest disables reqwest defaults and requests JSON/stream features only. Explicitly enable and verify an appropriate maintained TLS backend for approved HTTPS endpoints; do not assume remote TLS works merely because loopback HTTP does. Keep URL/auth policy outside model-provided input.

For any Rust-owned inference/probe stream:

1. Retain the request Operation and selected inputs before sending.
2. Parse bounded SSE records across arbitrary byte/UTF-8 boundaries with the maintained parser.
3. Associate text/reasoning/tool fragments with actual response, choice/item, part and tool-call indices/IDs.
4. Assemble a complete validated tool-call object before treating it as actionable. Partial arguments, repeated fragments, an empty name or ambiguous call ID are not executable.
5. Preserve native reasoning fields only where actually supplied. Do not reconstruct encrypted/hidden reasoning or use displayed reasoning as authority.
6. Handle terminal events, finish reason, final usage and errors separately. A `length` finish is transport-complete but may be an incomplete agent answer. EOF without the required terminal boundary is not success.
7. Reconcile delta versus final/cumulative content without double-counting. Retain only bounded diagnostics/artifacts; never an Operation per token.

Client-side tools require the chosen harness's complete request -> tool call -> authenticated execution -> tool result -> continuation path. Server-native tool execution requires its own provenance. Never execute tool-looking prose or infer a missing tool result from a final answer.

## 5. Context and local resource behavior

A route needs a usable context budget for instructions, current source, messages, loaded tool schemas, output and reasoning. Check the effective backend configuration, not only a model card's theoretical maximum. Prefer #22's small role surface and bounded assignment neighborhood.

Do not silently truncate requirements, change reasoning settings, enable compression, pick a smaller model or drop tool schemas to make a request fit. Return a precise context/capability gap; the manager chooses a supported adjustment. A small model unable to operate the required tool surface can still serve a bounded text task, but is not marked ready as a manager/writer.

Shared resource facts:

- A server/loaded model can serve multiple agents; registration does not create a process or weight copy per agent.
- Endpoints and aliases sharing the same accelerator use one configured resource pool. Separate server ports are not separate VRAM budgets.
- Leave continuous batching, KV allocation and prefill scheduling to the backend. ELIOT provides bounded/fair admission and reports observed capacity, not a competing GPU scheduler.
- Active request slots and model residency are separate. A waiting auditor retains its candidate, not an unnecessary active-inference reservation.
- Cold load, prefill and generation are distinct observable phases. Long prefill is not enough to declare a session dead.
- TTFT, throughput, queue delay and memory figures identify their source/window. Aggregated server metrics cannot be attributed to one agent without a matching request identity.

JIT loading, TTL, auto-eviction and external users can change residency. Do not assume an ELIOT reservation prevents an external UI from unloading a model. Report invalidated/unknown state and reconcile. Explicit unload/reconfigure/start/stop is manager-authorized, bounded to owned resources and blocked from disrupting known in-flight users; inability to establish that scope is a real limitation.

## 6. Lifecycle, cancellation and failure

Default `attach` manages the client connection only. An unreachable endpoint is not permission to run `lms`, `unsloth start`, Docker or an installer as a health check. Optional owned-service launch uses the existing Rust process owner and an explicit management action; it is not necessary to connect any already running backend.

Track endpoint/service observation separately from harness binding/session generation. A restarted inference server can invalidate native response IDs and caches without changing the harness's durable identity. No phantom session recovery or replay from a llama.cpp slot/cache filename.

Preserve rejection, failure and unknown outcome:

- Invalid model/context/schema/auth is a concrete refusal, not a reason to silently change provider.
- A dropped stream or timeout after possible receipt can leave computation or native tools running. Closing the HTTP client is not verified cancellation.
- Do not cancel all backend requests or kill a shared model server to cancel one assignment.
- Response retrieval/cancel works only for an endpoint whose exact ID, scope and lifetime are supported and qualified. Header/request IDs do not imply vendor idempotency or a replayable SSE log.
- Disable generic HTTP middleware retries on inference/native-tool mutations. Known pre-send failure or a separately proven repeat-safe pure probe may retry under its declared policy; unknown agent/tool effects require retained readback or a manager decision.
- No cloud fallback, model downgrade, resumed old agent, automatic reinstall or active-session migration on OOM, quota, parser mismatch or endpoint failure.

A request may be terminal while its server process remains alive, and a parent may exit while tool/child work remains. Existing family and workspace ownership rules still apply.

## 7. Tools, hooks and security

Set one tool owner explicitly for each route: normally `harness`. Server-side code execution/search/MCP must be disabled or verifiably excluded for that path. Unsloth's loopback defaults are not a reason to inherit extra code-execution tools. Do not silently rewrite settings of an existing shared server; use a suitably configured endpoint or expose the mismatch.

A native-hosted MCP route gets only the needed role/work-context tools and invocation-scoped credentials. Never copy the manager token or register the same live call with two executors. A raw server tool event has no power to accept, publish, assign work or enable automation.

Reuse harness hooks where supported. Backend token metrics and HTTP logs are observational facts, not equivalents of pre-tool veto or commit hooks. Unsupported hooks stay unsupported; no synthetic tool execution from generated JSON.

Local transport defaults to loopback. Approved remote transport uses authenticated, validated TLS/protected connectivity without embedding addresses in prompts. No arbitrary model-supplied URLs, redirects forwarding credentials, unrestricted proxy environment or network discovery scan. Distinguish configured bearer from actually enforced authentication; use an explicit negative probe when qualifying it.

Downloaded templates, model files and adapters are untrusted inputs. No automatic `trust_remote_code`, custom parser Python execution, package installation or model-repository script launch. Operator selection of a vendor capability remains explicit. Provider tokens, artifact paths and model names with private information are redacted in remote views.

## 8. MCP and control surface

Normal agents keep #22's small eager core. They do not need four vendor-specific tool catalogs merely because their model is local.

Use existing/planned `runtime.catalog`, `runtime.profile.*`, `swarm.launch.preview`, `swarm.launch`, `swarm.agent.inspect`, `stream.*` and `operation.get` for the ordinary flow. Add only a deferred `local-inference` group for bounded endpoint/model/capability inspection and explicit `inference.probe`. Configuration extends the single runtime/configuration authority.

Optional vendor-native load/unload/server operations are separately registered typed extensions with their own scopes and readback. A backend lacking those APIs must not get fake generic support. They are not exposed to normal Participants and cannot execute arbitrary endpoint paths or shell strings.

Enabling local model supply is not enabling an automation. If the manager later selects a local profile in their queue/audit/cron/Goal automation, the same on-behalf authority, current model capacity and unknown-effect rules apply; no second scheduler or local-model workflow engine.
