# Eliot

Eliot is a local Rust controller for coding-agent harnesses.

## Current slice

The CLI and daemon connect to the Store, event bus, builtin `fake` harness, and
Codex app-server through `plugins/codex`.

Requests carrying `grantRoot` are unsupported. [Roadmap](docs/roadmap.md) owns milestone
status.

`processes AGENT` pages OS-verified process identities and lifecycle records.
`stats AGENT` reports ephemeral CPU/RSS samples and model-progress age;
`show AGENT` includes the same observation view. Missing or stale counters are null,
and process activity never counts as model progress.

`discover --profile PROFILE [--scope SCOPE] [--cursor CURSOR] [--limit N]` reads a
bounded native session metadata page through the selected profile. It creates no
Agent and sends no prompt. Codex enumeration is state-DB-only; previews and transcripts
are excluded, and reader-local `notLoaded` remains unknown. Pass `next_cursor` unchanged
to continue; `coverage.complete` reports enumeration exhaustion, not live coverage.
`processes AGENT --scope workspace` selects the managed preparation's target generation,
including setup that runs before the Agent's native generation starts.

`attach --profile PROFILE --harness codex --scope SCOPE --ref REF` binds an
observe-only Agent to a discovered subject. `history AGENT --view turns|items|subject`
reads private bounded pages; `--source rollout` explicitly selects verified local
rollout reading. `detach AGENT` closes its reader. Observation has no Agent identity
or native control: status can remain `unknown`, and detach does not end the native
session. Historical pages do not enter the live event bus or acknowledge input.

`git-status AGENT` reads the cached Git view; `edits AGENT` pages durable edit
attribution. Check freshness (`available`, `age_ms`) separately from `complete` and
history coverage: stale or partial data does not prove a clean workspace. Attribution
requires a filesystem-validated native completed `fileChange` hint contained in the
admitted workspace; dirty files or ordinary shell writes alone never identify their
author. Git polling alone does not advance model progress.

Strict Git reads use a dedicated no-child fence: Windows limits each Git Job to one
process; Linux requires verified unprivileged nonroot credentials and irreversible
process-creation limits. Root, privileged Linux and other Unix contexts report unavailable
without an unfenced fallback. Normal harness/plugin custody is unchanged.
Native filesystem invalidation reports `filesystem_invalidation.state`; degraded or
unavailable watching retains polling. Neither a notification nor watcher health
establishes Git freshness or model progress.

Managed worktrees are opt-in: `launch --worktree BRANCH [--base REF]` uses a new branch
and freezes the local commit OID before creation. Explicit profile setup uses verified
file references and runs before native dispatch; an unknown setup result is never
repeated automatically. Resume retains the checkout, completed setup and legitimate
commits. `worktree remove AGENT` requires a terminal Agent and no borrowers or retained
source dependencies; it removes only clean worktrees plus permitted scratch and keeps
the branch. Existing-workspace launches remain the default.

`status.workspace_custody` reports the current host capability. Supported Linux workspace
custody is degraded and may leave escaped descendants unobserved.

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
Example lifecycle:

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

An optional managed checkout uses the source repository as `--workspace`:

```powershell
& $eliot --data-dir $dataDir launch --profile codex-luna --workspace $workspace --worktree codex/worker-demo --base HEAD 'Inspect the project without changing files.' --wait
$agent = 'AGENT_ID_FROM_OUTPUT'
& $eliot --data-dir $dataDir stop $agent
& $eliot --data-dir $dataDir worktree remove $agent
```

Use an unused branch name. Setup file-argument syntax and scratch policy are documented
in [configuration](docs/spec/configuration.md#owner-workspace-setup).

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
cargo +1.99.1 test --locked -p eliot --all-targets
cargo +1.99.1 clippy --locked -p eliot --all-targets -- -D warnings
cargo +1.99.1 build --locked -p eliot
```

The v1 implementation remains archived on `archive/v1-final`; do not restore its runtime.
