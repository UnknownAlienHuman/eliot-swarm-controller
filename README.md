# Eliot

Eliot is a local Rust controller for coding-agent harnesses.

## Current slice

L1 connects the CLI and daemon to the Store, event bus, and the `fake` harness.
`launch --profile` now takes a named profile. The Codex source exists under
`plugins/codex` and the CLI exposes hidden `plugin codex` plus exact
resume/steer/interrupt/approve/deny/answer controls on `command.submit`; live Codex
qualification is still pending. [Roadmap](docs/roadmap.md) owns the verified milestone status.

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

`launch` takes a named profile and an existing workspace; `fake` remains the
default L1 harness. `send --wait` and `launch ... --wait` follow the exact
turn to its terminal result. The bounded wait is 30 seconds by default; use
`--timeout-seconds N` to change it. A timeout does not cancel the admitted command.

Codex Agents use a configured profile such as `codex-luna`
(`config/controller.example.toml`). Exact controls:

```powershell
& $eliot --data-dir $dataDir launch --profile codex-luna --workspace $workspace 'hello-one' --wait
& $eliot --data-dir $dataDir send $agent 'hello-two' --wait
& $eliot --data-dir $dataDir steer $agent 'adjust course' --generation 1 --turn TURN_ID
& $eliot --data-dir $dataDir interrupt $agent --generation 1 --turn TURN_ID
& $eliot --data-dir $dataDir approve $agent --request REQUEST_ID
& $eliot --data-dir $dataDir deny $agent --request REQUEST_ID
& $eliot --data-dir $dataDir answer $agent --answers-file answers.json --request REQUEST_ID
& $eliot --data-dir $dataDir resume $agent 'follow-up' --wait
```

`--generation` maps to `expected_generation`, `--turn` maps to
`expected_turn_id`, and `--request` maps to `request_id`; a missing
request is captured by the Store only when exactly one native request is pending.
`answer --answers-file` submits the bounded parsed `{ question: { answers: [string] } }`
body, not a path. Secret-question answers stay unsupported before Store persistence.

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
