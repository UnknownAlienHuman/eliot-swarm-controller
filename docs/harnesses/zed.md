# Zed

Swarm can drive Zed's native agent headlessly through the standalone `eval-cli`
batch runner. Each launch allocates one batch slot. The first send starts Zed's
agent with the requested model, runs one prompt and retains its result through
Swarm. The adapter requires a Linux runtime with private data directories.

## Vendor surface

Sources checked 2026-11-18. The installed Windows editor's product version is
`1.28.1+stable.390.e8bc16ace…`. Zed published
[v1.28.1](https://github.com/zed-industries/zed/releases/tag/v1.28.1) on
2026-11-11 at `e8bc16ace5ae1937600d571d441c4b5d7b0d288`. Repository
`main` was `5b282ac8bbc92aeda86fe4f7ba34bba4e55f7881` when inspected
on 2026-11-18.

| Surface | Finding |
| --- | --- |
| [Editor CLI](https://zed.dev/docs/reference/cli) and [Agent Panel](https://zed.dev/docs/ai/agent-panel) | The CLI opens workspaces in the editor. The panel hosts [Zed Agent](https://zed.dev/docs/ai/zed-agent) and drives native threads. No published CLI agent-server option was found. |
| [External Agents](https://zed.dev/docs/ai/external-agents) and [ACP v1 roles](https://agentclientprotocol.com/protocol/v1/overview) | Zed opens ACP connections to vendor agents as the client. External agents own their models and auth. |
| [NativeAgentServer](https://github.com/zed-industries/zed/blob/v1.28.1/crates/agent/src/native_agent_server.rs) and [agent connection](https://github.com/zed-industries/zed/blob/v1.28.1/crates/agent/src/agent.rs) | The server name refers to an in-process GPUI `AgentConnection` backed by Zed's `NativeAgent`. The inspected entry points expose no standalone ACP transport for it. |
| [Remote development](https://zed.dev/docs/remote-development) and [remote server source](https://github.com/zed-industries/zed/blob/v1.28.1/crates/remote_server/src/server.rs) | The headless SSH server owns the remote workspace and editor services. The local Zed client calls language models. No remote agent-driving endpoint was found. |
| [eval-cli 0.1.0](https://github.com/zed-industries/zed/tree/v1.28.1/crates/eval_cli) | A separately built headless executable creates one native thread, submits one instruction and exits. Its internal `NativeAgent`/`AcpThread` pipeline uses ACP types in-process, with no external ACP listener or multi-turn transport. The installed Windows directory and v1.28.1 release assets have no standalone runner. |

Swarm moved the Antigravity ACP transport into `eliot_plugin::acp`, preserving
its ordered prompt and load responses. The Zed plugin uses the shared child
transport and Zed's argv, stdin and output-file contract. It uses no ACP
wire messages. The archived v1 Zed batch doc at `dc6f70aa` corroborates the
one-invocation boundary. This implementation uses the current Swarm plugin
protocol and the pinned vendor source.

## Capabilities

| Capability | Mapping |
| --- | --- |
| Discovery | Resolve `eval-cli` on PATH or from an explicit profile. Require native executable identity, a static vendor marker and bounded `--help` output proving the batch flags before launch. The editor, CLI shim, remote server and script wrappers are excluded. The runner exposes no vendor release version flag. |
| Launch | Reserve one batch slot in the normal Agent generation. Native process and model verification begin with send. |
| Send and result | Send one instruction through stdin and close it. Read the bound process exit and `result.json`. On completion read final assistant text from bounded `thread.json` and commit it to Swarm. Further sends return `unsupported`. |
| Interrupt | Signal the exact owned Linux batch with SIGTERM. Return applied only after exit 3 and a matching `interrupted` receipt have committed. |
| Stop | Stop the owned batch and retire the Agent through the daemon's custody and source-drain path. An unfinished or forced batch without a valid terminal receipt remains unknown. |
| Configure, resume, steer, replies | Unsupported. Set the model on a new launch. |
| Approvals | The vendor runner sets tool permissions to `allow`. Named profiles require `dangerously_skip_permissions = true` and ad-hoc launch requires `--access full`. Swarm does not offer interactive approvals or a stricter Zed preset. |
| MCP | No Swarm MCP injection, policy filtering or effective server inventory. The profile must explicitly select `mcp = "inherit"` for native behavior within the private run. Workspace settings are still native inputs. Use trusted workspaces. |
| Models, usage and quota | No passive model catalogue or account quota endpoint. Supply the exact `provider/model`. Receipt model text identifies the requested route and is not a verified provider model readback. |

A missing, malformed or contradictory native receipt yields an unknown turn
and authorizes no replay. Timeout in a matching native receipt is a failed
turn. The thread may lack readable assistant text even when the receipt
proves completion. `swarm result <agent>` reads the committed Swarm result.
Native files live below that Agent's private plugin instance directory.

## Configuration

In the [Zed editor's model picker](https://zed.dev/docs/ai/llm-providers),
Zed-hosted models use the Zed account and billing. BYOK providers use native
provider credentials and models. [Ollama](https://zed.dev/docs/ai/use-a-local-model)
exposes discovered local models and uses `OLLAMA_API_KEY` when configured.

The standalone adapter accepts Anthropic, OpenAI, Google and Ollama provider/model IDs. Swarm forwards only
the selected provider's allowed credential variables from the daemon environment.
The supported variable names are `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GEMINI_API_KEY` or `GOOGLE_AI_API_KEY`, and optional `OLLAMA_API_KEY`.
Named profiles must list the variables they need in `pass_env`. Zed-hosted
editor sign-in and the desktop credential store are outside this route.
A local Ollama server is reachable only from the Linux daemon's network context.

```toml
[profiles.zed-batch]
harness = "zed"
cmd = "/absolute/path/to/eval-cli"
model = "openai/gpt-5-mini"
dangerously_skip_permissions = true
mcp = "inherit"
pass_env = ["OPENAI_API_KEY"]
```

An owner can also launch without a saved profile. In the shell below,
`$MODEL` is the provider/model you have access to and `$SWARM_DATA` is a fresh
Linux daemon data directory.

```sh
swarm --data-dir "$SWARM_DATA" harness list
swarm --data-dir "$SWARM_DATA" launch --harness zed --model "$MODEL" --access full --workspace "$PWD"
# Set AGENT to the returned Agent ID.
swarm --data-dir "$SWARM_DATA" send "$AGENT" -- "Reply with Zed batch works."
swarm --data-dir "$SWARM_DATA" result "$AGENT"
swarm --data-dir "$SWARM_DATA" stop "$AGENT"
```

## Install the standalone native runner

Windows Zed 1.28.1 and its launcher are insufficient. The pinned eval runner
offered no private data flag, and Zed's Windows
[config/data paths](https://github.com/zed-industries/zed/blob/v1.28.1/crates/paths/src/paths.rs)
use Windows Known Folders. Swarm rejects it on Windows, where environment
overrides cannot establish isolation. Use a separate Linux Swarm daemon, for
example in WSL2 or a Linux container, with no owner Zed configuration mounted.

On owner approval, build the runner from Zed v1.28.1 with the pinned Rust
1.98.1 toolchain and vendor [Linux build prerequisites](https://zed.dev/docs/development/linux).
From an isolated Linux build location:

```sh
git clone --depth 1 --single-branch --branch v1.28.1 https://github.com/zed-industries/zed.git zed-v1.28.1
cd zed-v1.28.1
cargo build --locked --release -p eval_cli
./target/release/eval-cli --help
stat -c '%s bytes' ./target/release/eval-cli
du -sh .git target
export PATH="$PWD/target/release:$PATH"
```

Zed also documents `crates/eval_cli/script/build-linux --output /absolute/path/to/eval-cli`
for a static Linux x86-64 build with Docker. Configure the resolved file in
Swarm or start the private daemon with its directory on PATH.

The release publishes no eval-cli binary or size. GitHub reported
556,197 KiB of repository storage for the full repository on 2026-11-18.
The shallow transfer, dependency downloads, build directory and runner size
are unmeasured. The v1.28.1 x86-64 Windows editor installer is 65,092,416 bytes,
and its Windows remote-server bundle is 37,885,055 bytes. Neither contains the
required interface.
