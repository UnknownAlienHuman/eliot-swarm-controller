# Local Models — llama.cpp, vLLM, LM Studio and Unsloth

Revision 1 · 2026-10-03 · source baseline `36cfb6521a6fc2f8d3fbbd97fbcb8046fa61ce50`.

**Status:** proposed integration contract plus an executed upstream vLLM CPU/HTTP smoke. This is not a claim that ELIOT local-model adapters, Windows GPU inference or agent tool loops are implemented or qualified.

## Product decision

Connect local inference to the existing agent system; do not build four more agent orchestrators.

```text
ELIOT manager, Tasks, roles and optional manager-owned automations
  -> existing agent harness / RuntimePort binding
      -> explicitly selected local inference endpoint and model
  <- native agent events, tool results and candidate evidence
```

The first useful path is a Windows coding harness using a Windows local server or an approved Linux inference node. Files, tools, worktree and manager remain with that harness. A Linux model server does not require Linux on the user's workstation or moving the repository to that server.

Raw Chat Completions is inference, not a coding agent, durable session, MCP client, Goal or Task acceptance. Where a backend offers its own tool/MCP execution, that is a different explicitly selected path with one tool-execution owner. The core must not grow an undocumented replacement model loop.

## Four targets

| Backend | Connection to implement | Distinct concerns |
|---|---|---|
| llama.cpp | Existing `llama-server`; discovered compatible wire plus native read-only status | GGUF, chat template, function parsing, effective per-request context and parallel slots. |
| vLLM | Existing CPU/GPU service, normally on Linux; Windows harness connects by HTTP(S) | Actual model/parser/wire support, shared accelerator capacity, batching, lifecycle and cancellation coverage. |
| LM Studio | Local API or llmster service; compatible API for harnesses, native API for model state | Downloaded is not resident; JIT/TTL/auto-evict; native MCP and caller-executed functions are different paths. |
| Unsloth | Existing authenticated Studio/Desktop API; alternatively exported artifacts served by another backend | Studio is now also an inference endpoint, not just training. Server-side tools, mutable model load state, GGUF versus merged weights/LoRA. |

Endpoint details and evidence live in [Backends and donors](backends-and-donors.md), not in hardcoded compatibility assumptions.

## Reuse and boundaries

- All ELIOT-owned adapters, configuration, discovery, HTTP/SSE handling, capacity projection, lifecycle admission and MCP tools are **Rust**. Vendor inference servers keep their own implementation languages. Optional user Python/PowerShell is not a required internal bridge.
- Reuse existing `RuntimeCommand`, `RuntimeOutcome`, Store/Operations, process owner, redaction, artifacts, launcher and MCP profiles. Do not modify Task semantics merely to change model supply.
- Reuse maintained complete components where appropriate; no prescribed old release, exact-version runtime gate or callback-time installer. Record tested versions and effective model inputs as evidence, not installation restrictions.
- Manager control stays manual by default. Registering an endpoint/model does not launch a server, download/load weights, run inference, enable an automation, change defaults or switch an active agent's model.
- Default lifecycle is **attach to the configured service**. Stopping one assignment does not stop a shared inference service or unload somebody else's model.
- Keep provider credentials distinct from ELIOT Participant/manager credentials. Local configuration contains real addresses and secrets; repository examples contain placeholders/opaque references only.

## Read by responsibility

| Document | Owns |
|---|---|
| [Architecture](architecture.md) | Provider/harness separation, typed configuration, capabilities, tool ownership, resource sharing and failure behavior. |
| [Backends and donors](backends-and-donors.md) | Current official API differences, concrete donor code, field reports and limits of reuse. |
| [Implementation](implementation.md) | Complete Rust implementation increments, existing code touchpoints and cross-PR integration. |
| [Qualification](qualification.md) | Actual Linux run and failed local-environment attempt, exact scope of evidence, retained warnings and remaining checks. |

[PR #22](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/22) owns peer coordination, launcher and deferred MCP. [PR #23](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/23) owns runtime preferences and manager-owned automation. Reuse their shared components when implemented; those unmerged programs are not assumed to exist on main.

## What was actually run

A disposable GitHub-hosted Ubuntu CPU job downloaded the then-current official vLLM CPU release and ran a small public model without provider accounts. Seven HTTP checks passed: catalog, authentication denial, generation, streaming terminal/usage, missing-model denial, two concurrent requests and context-overflow denial.

The shutdown log also reported forced engine cleanup and resource-tracker warnings. **HTTP success is not clean-shutdown qualification.** Tool execution, Responses/Messages compatibility, ELIOT integration, GPU, Windows, LM Studio and Unsloth live paths were not tested. See the full [qualification record](qualification.md).

The dedicated [workflow](../../.github/workflows/local-model-cpu-smoke.yml) runs on explicit changes to its research branch or manual dispatch; it is not a scheduled product automation or a normal main-build dependency. It changes no owner-machine configuration and has no repository-write permissions.
