# Local Models — Qualification Record

Recorded 2026-10-03. This separates an actual upstream CPU/HTTP experiment from the proposed ELIOT integration. Observed software versions are evidence of this run, not required installation versions.

## 1. Local sandbox attempt

The conversation's Linux sandbox had no GPU, vLLM, llama-server, LM Studio or Unsloth installation. Its external DNS failed for the package/model hosts needed for a real run. A bounded vLLM installation preflight failed before installation. That environment therefore supplied no inference evidence.

Rather than asking the user to install Linux or treating a mock HTTP server as vLLM, the test was moved to a disposable GitHub-hosted Linux runner in this repository. Nothing was installed, started or changed on the user's workstation.

## 2. Executed vLLM CPU contour

- [Workflow run 37133789908](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37133789908), job `vllm-cpu`, job ID `111234030402`.
- Workflow source commit: `e99ac53a46ea1ad4e96cdad3cfeb4c76b8ff3af3`.
- [Executed workflow source](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e99ac53a46ea1ad4e96cdad3cfeb4c76b8ff3af3/.github/workflows/local-model-cpu-smoke.yml).
- Recorded run: 2026-10-03, 15:35:37–15:36:51 UTC; GitHub conclusion `success`.
- Runner: Ubuntu 24.04.5 x86-64; four virtual CPUs reported as AMD EPYC 7763; AVX2; approximately 16 GB RAM. No GPU contour.
- Observed release: vLLM `0.30.0+cpu`, then-current official release published 2026-09-22. The workflow discovers the latest release asset rather than hardcoding that version or wheel ABI.
- Observed environment: Python 3.12.3, PyTorch 2.13.0+cpu, Transformers 5.18.0, uv 0.12.22. Installation resolution is recorded in job output; none is an ELIOT software pin.
- Model: `HuggingFaceTB/SmolLM2-135M-Instruct`, served under `eliot-cpu-smoke`. A small public model for protocol checks, not a recommended coding model.
- Limits: CPU bfloat16, context 512, at most two sequences, batch-token limit 512, eager execution and bounded request/process timeouts.
- Authentication: temporary random masked API key; loopback bind only.

The runner downloaded the official CPU wheel selected from the latest-release metadata and verified its reported SHA-256 before installing it in a temporary virtual environment. The observed wheel digest was `0ee75278b3626c5d0b7c310c6d62afae93e900f4eac339c91e333fde5108ed78`. This identifies tested bytes and is not a future install restriction.

The model repository revision/weight digest was not explicitly emitted by this smoke. Do not claim byte-identical model reproducibility or a benchmark from this record. A future qualification run should retain actual artifact/tokenizer/template identity where available without freezing future compatible model choices.

## 3. Seven observed positive/refusal checks

| Probe marker | Actual assertion | Result |
|---|---|---|
| `model_catalog` | Authenticated `/v1/models` contains the exact served alias. | PASS |
| `unauthorized_models` | The same model-list request without a key returns HTTP 401. | PASS |
| `chat_completion` | Real model returns nonempty assistant text, a recognized stop/length boundary and positive completion usage. | PASS |
| `streaming_terminal_and_usage` | SSE has a terminal finish reason, positive final usage and `[DONE]`. | PASS |
| `missing_model_rejected` | Unknown model returns HTTP 404 and an error object. | PASS |
| `two_concurrent_requests` | Two simultaneous authenticated requests both return nonempty responses. | PASS |
| `context_overflow_rejected` | An intentionally oversized input returns HTTP 400 instead of success. | PASS |

The sampled non-stream response reported `finish_reason=stop`, 37 prompt tokens and 11 completion tokens. These counts describe one tiny synthetic request; they say nothing about coding quality, fleet throughput or useful work per cost.

The log ends its probe section with:

```text
ELIOT_RESULT CPU_HTTP_SMOKE_PASS; tool execution, GPU performance and ELIOT integration NOT TESTED
```

The server route listing also advertised other endpoints. Merely appearing in that list is not functional qualification; only the requests above were exercised.

## 4. Shutdown caveat — not hidden by the green job

During cleanup, vLLM logged forced termination of a remaining EngineCore process. Python's resource tracker reported one leaked semaphore and two leaked shared-memory objects to clean up. The test owned and terminated its disposable process group, and GitHub performed normal runner cleanup afterward.

**Conclusion:** this run demonstrates the bounded HTTP/inference contour, not clean shutdown, leak-free repeated operation, independent request cancellation or recoverable long-lived service ownership. The resource warnings remain explicit failure-investigation/soak requirements. They are not evidence of damage on the user's machine or proof that all vLLM deployments leak.

A repeated lifecycle test must separately measure process descendants, shared memory, handles, RSS and restart behavior before that contour is called qualified. Do not redefine the HTTP PASS markers as lifecycle PASS.

## 5. What was not tested

No evidence is claimed for:

- llama.cpp, LM Studio/llmster or Unsloth live serving in this turn;
- GPU inference, Windows execution, large models or multi-GPU performance;
- Responses or Anthropic Messages behavior;
- tool selection, streamed tool argument assembly, tool execution or MCP;
- native harness completion, actual repository edits, ELIOT Task submission or audit;
- reasoning effort control, long contexts, multimodal requests or exported adapters;
- service restart during inference, fine-grained cancellation, exact replay or crash recovery;
- sustained fleet scale, leak-free teardown or production availability.

Backend documentation and donor source do not fill those gaps. No result was simulated to substitute for a real model.

## 6. Repeat and qualification sequence

The [dedicated workflow](../../.github/workflows/local-model-cpu-smoke.yml) has a research-branch/path trigger and manual dispatch, no schedule, no production/main-build trigger, no repository write permission and no user/provider credentials. Its shell/virtual-environment setup is disposable test scaffolding for a vendor Python server, not an internal ELIOT controller or a required user script. The ELIOT product integration remains Rust.

No pin/downgrade is used to force a green repeat. If the current compatible wheel, platform or dependency set changes, record the actual failure and inspect the supported upstream path. Preserve prior results as history, not as a permanent installation mandate.

Implementation qualification proceeds through:

1. Endpoint/auth/model-state reads, with no unintended JIT/model/service start.
2. Harmless named and automatic tool-call roundtrips with exact complete arguments, followed by malformed/refused cases. Keep tools synthetic/read-only; do not execute arbitrary generated code in a protocol test.
3. The actual required harness wire, including main/child/auxiliary locality and small deferred MCP surface.
4. One local-backed native agent reads a scoped ELIOT fact, performs its assigned disposable change and returns a candidate to the existing review path.
5. Context overflow, cold load, simultaneous requests, model-residency change, cancellation and interrupted SSE without duplicate execution.
6. Process/shared-resource teardown and server-restart recovery; then longer representative loads on each supported platform/device.

For every step record request/response disposition, source, model/configuration, platform, exact build under test and gaps. Configuration changes and automatic tasks stay under the manager's existing manual/on-behalf controls. A CPU smoke must never be presented as Windows/GPU or full agent-system qualification.
