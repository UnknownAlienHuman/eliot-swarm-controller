# Eliot

Eliot is a local Rust controller for coding-agent harnesses.

## Current slice

The CLI and daemon connect to the Store, event bus, and the five native
adapters — Codex (`plugins/codex`), Claude (`plugins/claude`), OpenCode
(`plugins/opencode`), Muse (`plugins/muse`), and Antigravity
(`plugins/antigravity`). A launch names an explicit profile, or omits `--profile` to
route by `--role` through the ordered `[routing]` candidates (an omitted role means
`default`; see [routing](docs/spec/configuration.md#routing-policy-s3)). There is no
builtin fake profile and no fallback after dispatch; an explicit profile bypasses routing.
Native-store selectors are frozen into the profile snapshot at admission and
never reselected from ambient state.

Requests carrying `grantRoot` are unsupported. [Roadmap](docs/roadmap.md) owns milestone
status.

`processes AGENT` pages OS-verified process identities and lifecycle records.
`stats AGENT` reports ephemeral CPU/RSS samples and model-progress age;
`show AGENT` includes the same observation view. Missing or stale counters are null,
and process activity never counts as model progress.

`discover --profile PROFILE [--scope SCOPE] [--cursor CURSOR] [--archived] [--limit N]` reads a
bounded native session metadata page through the selected profile. It creates no
Agent and sends no prompt; `--archived` explicitly selects archived Codex subjects. Codex enumeration is state-DB-only; previews and transcripts
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

Hot start works out of the box: first use writes visible, editable native-default
profiles for the installed harnesses through the settings writer and registers the
opened Git checkout as a project automatically (see [native
profiles](docs/spec/configuration.md#native-profiles-all-five-adapters) and [project
registration](docs/spec/configuration.md#project-registration-and-rule-authority)).

All human and machine edits live in one place: `<data_dir>/config.toml` is the single
settings source, read and written through the revision-guarded `settings.read`/`settings.save`
API covering profiles, routing, projects, daemon, automation, mail, cloud MCP and desktop
sections (see [unified settings](docs/spec/configuration.md#unified-settings)).

Queue assignment keeps a configured number of Agents busy on a brief queue: each timer
firing emits one routing-aware launch for the next `.md` brief, with a completion rule
retiring the Agent after its terminal turn. It runs on the existing rules and timers,
not a task engine (see [queue assignment](docs/spec/automation.md#targetless-launch)).

## Quick start

From the repository root, build the binary and replace the workspace, data-directory, and
Agent placeholders with your values:

```powershell
cargo build --locked -p eliot
$swarm = '.\target\debug\swarm.exe'
$workspace = 'PATH_TO_EXISTING_WORKSPACE'
$dataDir = 'PATH_TO_PRIVATE_ELIOT_DATA_DIR'
& $swarm --data-dir $dataDir daemon
& $swarm --data-dir $dataDir status
& $swarm --data-dir $dataDir launch --profile codex-luna --workspace $workspace 'hello-one' --wait
& $swarm --data-dir $dataDir ls
$agent = 'AGENT_ID_FROM_OUTPUT'
& $swarm --data-dir $dataDir send $agent 'hello-two' --wait
& $swarm --data-dir $dataDir watch $agent     # Ctrl+C stops watching
& $swarm --data-dir $dataDir events --since 0
& $swarm --data-dir $dataDir result $agent
& $swarm --data-dir $dataDir stop $agent
& $swarm --data-dir $dataDir daemon stop --drain  # or --force
```

`watch` starts at the current journal tip. Use `watch [AGENT] --after SEQ` to replay
retained events after an exact exclusive cursor. A pruned cursor returns `history_gap`
and is never clamped; see [event replay](docs/spec/api.md#event-replay).

Linux is supported through one [Docker image](docker/README.md) (linux/amd64,
Debian bookworm). Build with `docker build -f docker/Dockerfile -t eliot:linux .`,
run with a private data volume and `--init`, then use `docker exec eliot swarm …`.
The guide documents pinned native CLIs, explicit credential mounts, container paths
and degraded process custody. The Windows desktop cannot attach to this daemon.

`launch` takes `--profile` for one of the five native adapters and an existing
workspace, or omits `--profile` to route by `--role` through the ordered `[routing]`
candidates (an omitted role means `default`; see
[routing](docs/spec/configuration.md#routing-policy-s3)). Profiles are defined in
`<data_dir>/config.toml` (see [the example](config/controller.example.toml));
the profile supplies the harness, exact model and native executable. A missing or
wrong-harness profile fails explicitly, and an empty candidate list fails as
`routing_unavailable`: Eliot never substitutes a fake, another model, route or account,
and never retries an uncertain outcome on another route.
`send --wait` and `launch ... --wait` follow the exact
turn to its terminal result. The bounded wait is 30 seconds by default; use
`--timeout-seconds N` to change it. A timeout does not cancel the admitted command.

For Codex, copy [the example profile](config/controller.example.toml) to
`<data_dir>/config.toml` and pass an existing authenticated `CODEX_HOME`.
Example lifecycle:

```powershell
& $swarm --data-dir $dataDir launch --profile codex-luna --workspace $workspace 'Create hello.txt containing exactly hello without a newline.' --wait
$agent = 'AGENT_ID_FROM_OUTPUT'
& $swarm --data-dir $dataDir send $agent 'Append world to hello.txt without adding a newline.' --wait
& $swarm --data-dir $dataDir stop $agent
& $swarm --data-dir $dataDir resume $agent 'Read hello.txt and reply with exactly its contents.' --wait
& $swarm --data-dir $dataDir stop $agent
```

Turn controls require an active turn; request replies require a pending native request:

```powershell
& $swarm --data-dir $dataDir steer $agent 'adjust course' --generation 1 --turn TURN_ID
& $swarm --data-dir $dataDir interrupt $agent --generation 1 --turn TURN_ID
& $swarm --data-dir $dataDir approve $agent --request REQUEST_ID
& $swarm --data-dir $dataDir deny $agent --request REQUEST_ID
& $swarm --data-dir $dataDir answer $agent --answers-file answers.json --request REQUEST_ID
```

`--generation` maps to `expected_generation`, `--turn` maps to
`expected_turn_id`, and `--request` maps to `request_id`; a missing
request is captured by the Store only when exactly one native request is pending.
`answer --answers-file` submits the bounded parsed `{ question: { answers: [string] } }`
body, not a path. The shared shape is owned by `eliot_core::requests`
(`AnswerArgs`/`Answers`); foreign answer shapes reject before persistence and each
plugin translates the universal answer to its native write. Secret-question answers
stay unsupported before Store persistence.
For command approvals set profile `approval_policy = "on-request"`. For native questions
set optional `collaboration_mode = "plan"`; the question exercise used
`permissions = "read-only"`. Clearing pending requests during interrupt/stop or closing
a source proves cleanup, not acknowledgement of an answer.
`inject` supports system context at `next_input`, `turn_safe_point`, and `active_turn`;
adapter capabilities determine which application points are supported. Acceptance is
not application: see the [injection contract](docs/spec/api.md) for exact turn binding
and outcomes. `configure` switches a live Agent's model and/or effort for its next turn
(see the [mid-run overlay](docs/spec/configuration.md#mid-run-modeleffort-overlay-configure)).

An optional managed checkout uses the source repository as `--workspace`:

```powershell
& $swarm --data-dir $dataDir launch --profile codex-luna --workspace $workspace --worktree codex/worker-demo --base HEAD 'Inspect the project without changing files.' --wait
$agent = 'AGENT_ID_FROM_OUTPUT'
& $swarm --data-dir $dataDir stop $agent
& $swarm --data-dir $dataDir worktree remove $agent
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

See AGENTS.md ## Work loop for scoped static checks (pinned Rust 1.99.1 with
rustfmt and Clippy).

The v1 implementation remains archived on `archive/v1-final`; do not restore its runtime.
