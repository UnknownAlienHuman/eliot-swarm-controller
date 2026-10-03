# Local Models — Implementation Plan

Revision 1 · 2026-10-03 · source baseline `36cfb6521a6fc2f8d3fbbd97fbcb8046fa61ce50`.

Read [README](README.md), the relevant [architecture](architecture.md) section and [backend evidence](backends-and-donors.md). The [qualification record](qualification.md) names what was actually executed; it is not proof these code paths exist.

## 1. Integrate with existing owners

| Existing unit | Required reuse/change |
|---|---|
| `src/runtime/mod.rs` | Preserve RuntimeCommand binding/generation/Operation and RuntimeOutcome identity. Inference IDs are not native agent turns. |
| `src/runtime/codex.rs`, `src/runtime/prepared.rs` and installed module adapters | Add verified per-launch provider configuration through the actual harness interface, without rewriting global settings or silently opening another native session. |
| `src/runtime/opencode_v2.rs`, `src/runtime/opencode_v2/` | Inspect the installed V2 model/provider/configuration interface; do not assume public OpenCode examples match that service. |
| `src/runtime/prerequisites.rs` | Keep backend-specific setup validation in adapters; generic Store only checks retained setup/identity evidence. |
| `src/runtime/owner.rs`, platform process ownership | Reuse only for explicitly owned launches; attaching to an inference endpoint does not transfer service ownership. |
| `src/config.rs`, #23 runtime profiles/configuration | One typed endpoint reference and model preference path, not another active configuration database. |
| `src/store/mod.rs`, Operations, artifacts, projections | Normal authorization, request receipts, outcome readback and bounded results; no vendor model loop in transactions. |
| `src/mcp.rs`, `src/mcp/profiles.rs`, #22 catalog/launcher | Deferred local-inference group and scoped capability data; no global startup catalog expansion. |
| `Cargo.toml` | Reuse reqwest/eventsource-stream/Tokio/serde; explicitly qualify HTTPS/TLS support. No new prescribed exact-version requirements. |

Current main still contains historical pins and some non-Rust owned bridges. This extension neither claims they vanished nor rewrites unrelated code. Apply the accepted Rust migration/dependency policy in the owning implementation work; do not add more frozen-release integration gates here.

## 2. Complete increments

### L1 — Endpoint model and bounded discovery

Proposed minimal units, reuse actual existing equivalents when present:

```text
src/inference/mod.rs
src/inference/config.rs
src/inference/catalog.rs
src/inference/backends.rs
```

Add endpoint/auth/resource references to the shared configuration path. Implement registered-endpoint inspection and bounded model/capability reads, scoped by caller rights. Parse the known native inventory differences for the four backends rather than treating every `/v1/models` record as loaded.

Passive inspection must not start a service, warm/download models, invoke a model, change global settings or enable automation. Invalid/dead endpoints affect their own routes only. Retain documented/observed/unknown evidence and freshness.

Done when the manager can select a real local model in the normal runtime catalog and see exactly which serving/harness requirements remain missing.

### L2 — One real local coding harness path

Select an already supported installed harness and verified wire. Prepare endpoint/model/auth/options per launch through Rust. First prove one complete path, such as Codex -> local Responses or the actual OpenCode service -> compatible local provider; don't fabricate Responses from a Chat-only test.

Freeze requested/effective route and configuration with the existing RuntimeCommand. Configure main, children and auxiliary paths according to the manager's locality policy. Register the ordinary scoped ELIOT tools through the existing launcher; preserve one manager worktree and native family owner.

Done when a manager launches one assignment, the local-backed harness invokes a harmless ELIOT read, returns an exact result, and existing submission/review handles it. Completing a raw prompt is insufficient. A missing tool path must be reported before presenting the agent as ready.

Do not make this increment depend on a new Goose module, server-side MCP host or all four backend lifecycle APIs.

### L3 — Shared wire observations and four-backend parity

Add reusable Rust HTTP/SSE normalization for explicit probes and only those observations not already supplied by the native harness. Reuse the maintained EventSource parser; don't write a parallel protocol parser per brand or proxy all harness traffic without a concrete need.

Implement the capability matrix for llama.cpp, vLLM, LM Studio and Unsloth using native backend reads where useful. Keep per-service generation, model/load configuration and API dialect separate. Preserve reasoning/tool fragments, partial/terminal states, context errors and actual usage. Handle a server returning catalog but not generation, and generation but not valid tools.

Done when the same application/profile flow selects each backend without core vendor branches and unsupported features remain precise gaps. A second actual consumer is required before expanding common RuntimePort semantics.

### L4 — Shared capacity and optional service/model management

Use the existing scheduler/capacity ownership, keyed to real shared resource pools. Limit concurrent inference independently from registered Participants; don't load duplicate weights merely because several agents share the model.

Optional native load/unload or owned-process start/stop requires typed methods, explicit manager request/on-behalf configuration, scoped ownership and readback. Attach-only endpoints remain fully usable without these capabilities. Current LM Studio/Unsloth UI activity and native TTL can invalidate residency; report that instead of assuming an unenforced global lease.

No hidden eviction, container creation, model downloads, network exposure, process killing or autorepair of the user's environment. Stop one request through a supported request boundary, not by stopping the whole server.

### L5 — Optional native tool-host route

Only when needed and qualified, wire a documented native MCP/tool-host path such as LM Studio's native API. Explicitly select that owner, scope its ELIOT credentials/catalog, capture actual call/result identity and prevent a second harness executor handling the same call.

Server-side tools beyond ELIOT's selected scope must not be inherited by default. A model-generated function call or a vendor-healed argument string still requires the normal tool authorization/validation boundary. No Task/acceptance authority is delegated by choosing local inference.

Native-host mode is not required for ordinary local coding through an existing harness.

### L6 — Qualification and setup handoff

Run exact positive/refusal/recovery cases from `qualification.md`, then a complete local model -> native harness -> ELIOT tool -> candidate -> independent review contour. Linux CPU serving is testable without the owner machine; GPU/Windows/large-model results require the actual corresponding environment.

Expose compact local-inference diagnostics and explicit finite probes through deferred MCP/CLI tools. Keep installation and download separate from passive discovery. Record exact observed inputs/results and resource gaps, not permanent required releases.

## 3. Failure cases the implementation must cover

- Listed/downloaded model is not resident; a read doesn't silently warm it.
- Same server/GPU reached through aliases does not multiply capacity.
- Chat success does not qualify a Responses/Messages or tool-using harness route.
- Unknown or incomplete streamed tool arguments never execute.
- Partial output, truncated reasoning budget and EOF are not complete Task results.
- Cancelled client with still-busy server keeps its uncertainty; unrelated requests survive.
- Server restart invalidates ephemeral response IDs without fabricating a new native agent root.
- Setting changes affect new work, not replayed request receipts or active model selection.
- Main/child/auxiliary inference remains local when that is the selected policy.
- Endpoint or credentials cannot be supplied by model output or GitHub comment text.
- Unsloth server tools and harness tools do not both execute.
- LM Studio auto-evict/load failure cannot silently redirect work to another model.
- Exported LoRA missing its base/tokenizer/template is not accepted as a complete standalone model.
- Disconnected monitoring neither restarts the server nor claims the agent died.

## 4. Delivery policy

One manager owns each implementation worktree and reviews/integrates non-overlapping writer edits. Writers do not run Cargo. Use the current scoped formatting/minimal Clippy manager gate; broad product qualification follows the completed path or an explicit acceptance request.

The Linux CPU experiment in this PR is the user's explicitly requested isolated backend test, not a production control loop or a new requirement to run live models on every commit. It never activates an owner-machine automation. Source paths, registrations, consumers and result readers must land in the same useful code increment; don't ship disconnected provider structs as a working feature.
