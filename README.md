# Eliot

Eliot is a local Rust controller for coding-agent harnesses.

## Current slice

The CLI and daemon connect to the Store, event bus, builtin `fake` harness, and
Codex app-server through `plugins/codex`.

On Windows on 2026-10-27, Codex 0.161.0 with `gpt-6-luna` completed three native
turns: create `hello.txt` containing `hello`, append `world`, then return exactly
`helloworld` after a graceful stop and resume of the same thread in generation 2.
Old turns were not replayed. Live watch observed all three results; final Agent stop
and daemon drain succeeded.

Three further native control exercises with Codex 0.161.0, Luna and effort `medium`
verified steer, approval, a retained same-ID approval retry without a second native
write, interruption with another callback pending, denial without executing the denied
command, and plan-mode question validation and answering. All three stopped the Agent
and exited the daemon, with sources closed/healthy, zero gaps or duplicate native IDs,
and no workspace files created.

Full L7 qualification remains pending. File-change approval was not live exercised;
requests carrying `grantRoot` remain unsupported. [Roadmap](docs/roadmap.md) owns milestone status.

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

`launch` requires a named profile and an existing workspace; `fake` is builtin.
`send --wait` and `launch ... --wait` follow the exact
turn to its terminal result. The bounded wait is 30 seconds by default; use
`--timeout-seconds N` to change it. A timeout does not cancel the admitted command.

For Codex, copy [the example profile](config/controller.example.toml) to
`<data_dir>/config.toml` and pass an existing authenticated `CODEX_HOME`.
The smoke verified effort `medium` in both generations. Example lifecycle:

```powershell
& $eliot --data-dir $dataDir launch --profile codex-luna --workspace $workspace 'Create hello.txt containing exactly hello without a newline.' --wait
$agent = 'AGENT_ID_FROM_OUTPUT'
& $eliot --data-dir $dataDir send $agent 'Append world to hello.txt without adding a newline.' --wait
& $eliot --data-dir $dataDir stop $agent
& $eliot --data-dir $dataDir resume $agent 'Read hello.txt and reply with exactly its contents.' --wait
& $eliot --data-dir $dataDir stop $agent
```

Turn controls require an active turn; request replies require a pending native request:

```powershell
& $eliot --data-dir $dataDir steer $agent 'adjust course' --generation 1 --turn TURN_ID
& $eliot --data-dir $dataDir interrupt $agent --generation 1 --turn TURN_ID
& $eliot --data-dir $dataDir approve $agent --request REQUEST_ID
& $eliot --data-dir $dataDir deny $agent --request REQUEST_ID
& $eliot --data-dir $dataDir answer $agent --answers-file answers.json --request REQUEST_ID
```

`--generation` maps to `expected_generation`, `--turn` maps to
`expected_turn_id`, and `--request` maps to `request_id`; a missing
request is captured by the Store only when exactly one native request is pending.
`answer --answers-file` submits the bounded parsed `{ question: { answers: [string] } }`
body, not a path. Secret-question answers stay unsupported before Store persistence.
For command approvals set profile `approval_policy = "on-request"`. For native questions
set optional `collaboration_mode = "plan"`; the question exercise used
`permissions = "read-only"`. Clearing pending requests during interrupt/stop or closing
a source proves cleanup, not acknowledgement of an answer.
`inject` and `configure` are currently unsupported.

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
