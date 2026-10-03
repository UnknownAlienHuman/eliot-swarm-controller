# vLLM on WSL2 — Windows Deployment Contract

Revision 1 · 2026-10-03 · selected by the owner; installation and live qualification are not performed by this PR.

## 1. Placement

```text
Windows: ELIOT host + Kilo/other native harness + Git/worktree + scoped MCP
                                  |
                         private HTTP inference
                                  |
WSL2: selected distribution + isolated vLLM environment + model cache + GPU
```

WSL2 is the preferred vLLM placement for this workstation, replacing the earlier requirement for a separate Linux machine. Remote Linux remains an optional backend. No second ELIOT Store, Task graph, scheduler or cloned writer worktree is needed in the distribution.

Internal discovery/configuration/admission/monitoring and any owned launcher remain Rust. vLLM's Python implementation is an external inference engine, not a Python internal controller. Docker is optional, not a prerequisite for the first useful path.

## 2. Setup inputs, not fixed versions

Before installation collect the actual Windows/WSL mode, selected distribution/user, architecture, GPU/VRAM/driver, guest Python/packaging tags, disk space and existing GPU consumers. Do not assume that total system RAM is GPU memory or that CUDA-in-WSL support implies support by the current vLLM kernels.

Use the current [vLLM GPU installation guide](https://docs.vllm.ai/en/stable/getting_started/installation/gpu/) and [NVIDIA WSL guide](https://docs.nvidia.com/cuda/wsl-user-guide/index.html). No fixed vLLM, CUDA, Python, distribution or model version is prescribed. Select the current compatible distribution of the engine, recording actual installed values as evidence. Unsupported accelerator/backend combinations are reported; never install NVIDIA components for an AMD/Intel device.

For the NVIDIA path, the GPU driver belongs on **Windows**. Do not install Linux NVIDIA display drivers in WSL or a meta-package that pulls them in. Install a compatible toolkit only when required by the selected build path. A wheel install and an `nvidia-smi` response are not yet a model/tool test.

Create one named isolated environment and keep model/compilation caches in the guest Linux filesystem by default, outside the Git worktree. Respect disk limits and existing model-cache ownership. Downloads, driver changes, WSL installation/update, firewall changes and reboot are setup actions, not side effects of browsing the ELIOT catalogue.

## 3. Network: Windows to guest, not public exposure

[Microsoft networking documentation](https://learn.microsoft.com/en-us/windows/wsl/networking) distinguishes NAT and mirrored mode. Windows-to-WSL localhost forwarding is the first path to verify; mirrored networking is an optional deployment setting, not a compulsory repair.

1. Bind vLLM to guest `127.0.0.1` on one selected unused port.
2. Test that exact authenticated endpoint from the Windows host.
3. Distinguish wrong service/port, stopped guest, routing and authentication failures.
4. If forwarding is unavailable, diagnose actual WSL/network mode and obtain an explicit deployment change. Do not automatically bind `0.0.0.0`, create a public tunnel, add `portproxy` or disable a firewall.
5. Do not hardcode a transient guest IP or treat a failed probe as permission to switch hosts.

Keep the API key in protected local credentials/environment. The [vLLM security contract](https://docs.vllm.ai/en/stable/usage/security/) warns that API-key protection does not cover all routes. Loopback is not a complete boundary against untrusted local processes. A shared or remotely reachable deployment needs an authenticated allowlisted inference surface; do not expose profiler, weight update, collective RPC, management or raw internal distributed ports.

Inference traffic normally goes directly from the Windows harness to the chosen guest endpoint. ELIOT does not add a mandatory proxy on every token. An optional Rust gateway can enforce a needed exposure boundary; it must not duplicate the provider's agent loop.

## 4. Manual first startup and ordinary operation

The initial useful path attaches to a service deliberately started by its owner. Configure model, parser/template, context, output, concurrency and memory allocation for the actual hardware; do not let defaults occupy memory assumed available to LM Studio or llama.cpp.

Illustrative native command shape, after setup and with selected values (not an automatic installer):

```bash
VLLM_API_KEY="$LOCAL_INFERENCE_KEY" vllm serve "$MODEL_ID" \
  --host 127.0.0.1 --port "$INFERENCE_PORT"
```

Add the model-specific documented parser/template and batching options when needed. Environment values are local and not printed. The model must already be deliberately downloaded or the owner must explicitly permit the download. Do not pass a remote model name in a supposedly read-only probe and trigger a download.

Then register the existing endpoint through `inference.backend.configure`, read the catalogue and choose it in the common runtime profile. These are proposed extension methods until wired. Selecting a backend does not start an agent, enable cron/Goal, change billing or switch existing sessions. A manager may explicitly enable an already authorized startup/helper on their behalf, with the same rules as other automations.

## 5. Two lifecycle layers

Distribution availability and vLLM service availability are separate. An HTTP probe cannot prove that the guest was shut down, a process exited or a generation was cancelled.

- Read-only catalogue/dashboard does not execute `wsl.exe --exec`, because that may start a stopped distribution.
- Named distribution/user/service references are local installation data. Never guess the default distribution or run as root because one request failed.
- A foreground WSL launcher and a guest worker have different identities. Killing/observing the Windows wrapper is not proof of guest-worker disposition.
- An owned production launch needs exact guest process/service identity and readback. Stop only that owned service; no `wsl --shutdown`, distribution-wide `--terminate`, `killall` or global GPU reset for one request.
- A manually started or editor/operator-owned service remains external. Discovery cannot adopt it as owned.

[Microsoft's systemd guidance](https://learn.microsoft.com/en-us/windows/wsl/systemd) notes that systemd services alone do not keep the WSL instance alive. Service enabled, guest running, listener available and model ready must be observed separately. Initial foreground operation is sufficient for the pilot; unattended service/distro lifetime is a separate explicitly configured owner action, not a silent keepalive added to every reader.

On suspend/resume, Windows reboot, guest restart or listener replacement, re-establish endpoint/service identity and reconcile unfinished requests. No replay of all prompts and no false Task completion. Profile changes apply to new admissions, not an unannounced move of a live session.

## 6. Shared hardware and paths

Windows LM Studio/llama.cpp and WSL vLLM may use the same GPU and host RAM. Bind them to the selected common resource group. Two API ports, two aliases or Windows/guest process IDs do not create two copies of VRAM. Use observed capacities and manager-selected limits; expose missing NVML/WSL counters as unknown rather than zero load. Start with one real generation, then measure modest concurrency.

The initial harness stays on Windows, so its local MCP and filesystem tools keep Windows paths. A vLLM model sees prompt content and returns tokens; it does not need the project's Windows directory mounted as a tool workspace. A later Linux-hosted harness would need an explicit path/identity/credential topology and separate qualification. Do not silently rewrite `C:\\...` to `/mnt/c/...` in prompts or configuration.

## 7. Acceptance sequence

1. Confirm selected WSL2 distribution, supported GPU/build path and isolated environment; record actual values without converting them to future pins.
2. Verify guest endpoint and Windows-to-guest endpoint identity/authentication, with no LAN exposure.
3. Inspect catalogue, deliberately load the selected model, then run one explicit bounded text request.
4. Run a tool call/result/continuation round-trip through the chosen native harness; server text alone does not qualify coding.
5. Execute one disposable ELIOT Task using the scoped MCP core, then retain its normal result/submission.
6. Check WSL suspend/restart, connection loss after acceptance, port reused by another service, partial stream, wrong key and model eviction.
7. Check one/two/four concurrent requests and shared GPU pressure without auto-changing other engines.
8. Close only owned resources and verify guest workers/listeners; external services survive client shutdown.

This environment is a Linux container, not the user's WSL2 workstation. The prior failed CPU install and current DNS failure are documented in [qualification.md](qualification.md); neither establishes a passed WSL, GPU, Kilo or inference integration.
