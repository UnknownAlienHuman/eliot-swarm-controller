# Eliot

Eliot is a local Rust controller for coding-agent harnesses.

## Current slice

L1 connects the CLI and daemon to the Store, event bus, and one `fake` plugin. The fake
uses the plugin JSON-RPC/NDJSON protocol to exercise turn handling; it never calls a
model. Direct Codex and Luna integrations are the next layer (L2) and are not implemented
yet.

## Quick start

From the repository root, build the binary and replace the workspace, data-directory, and
Agent placeholders with your values:

```powershell
cargo build --locked -p eliot
$eliot = '.\target\debug\eliot.exe'
$workspace = 'PATH_TO_EXISTING_WORKSPACE'
$dataDir = 'PATH_TO_PRIVATE_ELIOT_DATA_DIR'
& $eliot --data-dir $dataDir daemon
& $eliot --data-dir $dataDir status
& $eliot --data-dir $dataDir launch --profile fake --workspace $workspace 'hello-one' --wait
& $eliot --data-dir $dataDir ls
$agent = 'AGENT_ID_FROM_OUTPUT'
& $eliot --data-dir $dataDir send $agent 'hello-two' --wait
& $eliot --data-dir $dataDir watch $agent     # Ctrl+C stops watching
& $eliot --data-dir $dataDir events --since 0
& $eliot --data-dir $dataDir result $agent
& $eliot --data-dir $dataDir stop $agent
& $eliot --data-dir $dataDir daemon stop --drain  # or --force
```

On Unix, the built binary is `./target/debug/eliot`; pass the same `--data-dir` to each
invocation.

`launch` supports the `fake` profile and an existing workspace. `send --wait` and
`launch ... --wait` follow the exact turn to its terminal result. The bounded wait is
30 seconds by default; use `--timeout-seconds N` to change it. A timeout does not cancel
the admitted command.

Other read commands include `show AGENT` and `cmd show ID`. `watch [AGENT]` streams
committed events; `events --since SEQ` reads a page of event history.

## Data and credentials

The default private data directory is `%LOCALAPPDATA%\Eliot` on Windows and
`$HOME/.local/share/eliot` on Unix. Set `ELIOT_HOME` or pass
`--data-dir PATH_TO_PRIVATE_DATA_DIR` to choose another directory. The CLI loads the
local owner credential automatically; no owner token argument is needed.

## Build and check

The repository pins Rust 1.99.1 with rustfmt and Clippy. From the repository root:

```text
cargo +1.99.1 fmt --check
cargo +1.99.1 clippy --locked -p eliot -- -D warnings
cargo +1.99.1 build --locked -p eliot
```

The v1 implementation remains archived on `archive/v1-final`; do not restore its runtime.
