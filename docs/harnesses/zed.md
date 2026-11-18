# Zed native host, stage 1

Zed uses a separate `zed-agent-host.exe` built from the pinned Zed workspace. Eliot
owns the Agent generation and its Windows Job. The host owns one
`NativeAgentConnection`, `AcpThread`, and `ThreadStore` across successive sends.

## Capability matrix

| Surface | Stage 1 |
| --- | --- |
| Eliot `launch` | Opens one native session with the configured cwd, provider and model |
| Eliot `send` | Submits to the same native history and acknowledges after the `turn.started` event commits |
| Eliot `stop` | Requests session close and drains native output. The daemon verifies closure of the generation's Job |
| Host JSON-RPC 2.0 over stdio | `initialize`, `session.open`, `turn.send`, `session.status`, `session.close` |
| Native permissions | Denies every request and emits the denial |
| Native MCP and native subagents | Disabled |
| Steer, inject, interrupt, permission replies, resume, configure, model list, usage and quota | Typed `unsupported` |
| Swarm MCP, scripts, Goals and Delta qualification | Later stages |

The adapter binds ordered notifications to the Agent generation, native session,
command and turn. Terminal results follow earlier events. Loss before a correlated
native terminal leaves the turn outcome `unknown` and prevents replay.

## Profile and authentication

```toml
[profiles.zed-openai]
harness = "zed"
cmd = "C:/Development/Rust/targets/zed-host/release/zed-agent-host.exe"
model = "openai/<native-model>"
pass_env = ["OPENAI_API_KEY"]
```

Use an absolute path to the host and select a model from the pinned native OpenAI
provider catalogue. The native provider reads `OPENAI_API_KEY` from the configured
launch environment. Session readback reports `environment:OPENAI_API_KEY` when
the provider loaded the key and `unavailable` when it did not. Missing credentials
allow a session to open and cause sends to return `provider_auth_required`.
The host disables keychain reads and writes. Zed account and subscription sign-in are
outside stage 1.

## Build the Windows host

In a new vendor checkout, fetch and apply the patch series against the exact base.
The pinned Zed toolchain is Rust 1.98.1 for `x86_64-pc-windows-msvc`. The release
build uses Visual Studio 2022, CMake, and the x64 Spectre-mitigated MSVC v143 libraries.

```powershell
git init C:/Development/Rust/vendor/zed
Set-Location C:/Development/Rust/vendor/zed
git remote add origin https://github.com/zed-industries/zed
git fetch --depth 1 origin 761786a4ef7144de734e48cb2e8f99bc46b5061e
git checkout -b eliot/agent-host FETCH_HEAD
$patches = Get-ChildItem -LiteralPath C:/Development/Rust/vendor/zed-agent-host/patches -Filter '*.patch' |
    Sort-Object Name | Select-Object -ExpandProperty FullName
git am $patches
$env:CARGO_TARGET_DIR = 'C:/Development/Rust/targets/zed-host'
cargo build --release -p zed_agent_host
Get-FileHash (Join-Path $env:CARGO_TARGET_DIR 'release/zed-agent-host.exe') -Algorithm SHA256
```

**Binary SHA-256 unavailable.** The Windows build stopped in `msvc_spectre_libs 0.1.3`
because Visual Studio 2022 Build Tools lacks component
`Microsoft.VisualStudio.Component.VC.Runtimes.x86.x64.Spectre`.

The adapter checks protocol version 1, the pinned Zed revision, the Agent
generation binding and the resolved paths before opening a session. The host sets
Zed's custom data directory before bootstrap and resolves config, state and temp
under the private instance directory. Settings carry the explicit provider and
model. The adapter starts the configured host binary. The source and license
boundary is documented in [donors](../reference/donors.md).
