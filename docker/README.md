# Linux via Docker

The supported Linux distribution is this **linux/amd64 Debian bookworm image**.
Build from the repository root (Docker Desktop must use its Linux engine):

```sh
docker build --platform linux/amd64 -f docker/Dockerfile -t eliot:linux .
docker volume create eliot-data
docker run -d --init --name eliot --mount source=eliot-data,target=/data eliot:linux
docker exec eliot swarm status
```

The Rust 1.99.1 build stage compiles locked all-targets `eliot`, `eliot-daemon` and
`eliot-plugin-opencode` as UID/GID 10001. The runtime
contains the debug branch binary without debug symbols, Git and native CLIs, with no Rust toolchain. All
processes run as UID/GID 10001. Docker initializes an empty named data volume from
the image's owned mode-0700 `/data`; the daemon creates a mode-0600 Unix socket and
private files. Existing bind-mounted data roots must already have the correct owner
and permissions: unsafe roots fail, and the image never silently chmods host data.
Use `--init` to reap exited descendants. No privileged mode, Docker socket mount,
host PID namespace or cgroup delegation is needed.

For unattended startup, add `--restart unless-stopped` to `docker run`.
The Docker engine starts the container again after a failure or engine restart.
Use `docker stop eliot` to keep the container stopped.

## Frozen dependencies

Resolved from official sources on 2026-11-16; Dockerfile pins the amd64 manifests:

| Component | Version / digest |
| --- | --- |
| Official Rust bookworm | 1.99.1; `sha256:4715c3ad743951a3cea90e8b432f2a17e0ef5f3f3c3abf75108879e783b396f0` |
| Official Debian bookworm-slim | `sha256:c67e92c8099ba22853ab4cfda7cfb494d253d6012ca82dee98ba8976772af018` |
| Codex CLI | 0.167.1; [official versioned installer](https://learn.chatgpt.com/docs/codex/cli) |
| Claude Code | 2.1.325; [official versioned installer](https://code.claude.com/docs/en/setup#install-a-specific-version) |
| OpenCode | 2.2.7; [official v2 installer](https://opencode.ai/v2/install) |
| Muse | 1.4.0-R4161.1; [official Linux release manifest](https://lookaside.facebook.com/lookaside/muse/download/?channel=muse&version=1.4.0-R4161.1&file=manifest.json), SHA-256 checked |
| Antigravity | 1.5.9; [official Linux installer manifest](https://antigravity-cli-auto-updater-974169037036.us-central1.run.app/manifests/linux_amd64.json), frozen URL and SHA-512 checked |

Muse and Antigravity both publish Linux builds. Their native payloads are installed
directly using the vendor manifests, avoiding their moving launcher selection.
Vendor software keeps its own license/terms; no donor application/source is imported.
Debian security packages come from the configured apt repository at build time;
the base and harness pins do not make those packages byte reproducible.

## Credentials and workspaces

Credentials are absent from build arguments, build context and image layers. Mount
only credentials you intend to give to the native harness. Every process under the
container user can read those mounts; a coding agent can send their contents over
the network. Read-only mounts prevent host edits, not credential disclosure. Never
mount a Windows Eliot data directory into the Linux daemon.

Codex: mount an authenticated owner's `CODEX_HOME/auth.json` read-only at
`/home/swarm/.codex/auth.json`, and set `CODEX_HOME=/home/swarm/.codex` on `docker run`.
The surrounding container home remains writable for native sessions. A read-only
auth file cannot persist token refresh: refresh outside the container and recreate
the mount when necessary. Do not mount the owner's entire home/config: Windows
MCP command paths and unrelated secrets are not portable. For example, in PowerShell:

```powershell
$auth = Join-Path $env:USERPROFILE '.codex\auth.json'
docker run -d --init --name eliot --mount source=eliot-data,target=/data `
  --mount "type=bind,source=$auth,target=/home/swarm/.codex/auth.json,readonly" `
  -e CODEX_HOME=/home/swarm/.codex eliot:linux
```

Write `/data/config.toml` explicitly (before launch, or reload after editing):

```toml
[profiles.codex-luna]
harness = "codex"
cmd = "/usr/local/bin/codex"
model = "gpt-6-luna"
effort = "low"
permissions = "danger-full-access"
approval_policy = "never"
pass_env = ["CODEX_HOME"]

[profiles.claude-cheap]
harness = "claude"
cmd = "/usr/local/bin/claude"
model = "claude-haiku-5-5"
approval = "bypassPermissions"
pass_env = ["CLAUDE_CONFIG_DIR"]

[profiles.opencode-free]
harness = "opencode"
cmd = "/usr/local/bin/opencode"
model = "opencode/deepseek-v4.1-flash-free"
auth_integration = "opencode-go"
pass_env = ["XDG_DATA_HOME"]
```

These permission choices allow tool execution inside the container; mount only the
repositories the Agent should access. Use absolute **container** paths for `cmd`
and `--workspace`. Host drive letters and host executables cannot be used. A Git
worktree whose `.git` points outside a mount is incomplete; mount its common Git
directory too, or clone/init a self-contained repository inside the container.
Spaces/non-ASCII paths are supported. Keep the data root short: Linux's Unix socket
endpoint (`<data-dir>/daemon.sock`) is limited to 107 UTF-8 bytes.

Claude: mount the owner's `.claude/.credentials.json` read-only at
`/home/swarm/.claude/.credentials.json`, set
`CLAUDE_CONFIG_DIR=/home/swarm/.claude`, and allow that variable in the profile.
The writable container directory isolates native session/config writes. API keys,
when explicitly chosen instead, must be supplied at runtime and allowlisted in
`pass_env`; never put them in a Dockerfile or semantic configuration.

OpenCode v2: mount an owner-prepared **Linux native credential store** at
`/home/swarm/.local/share/opencode`, set `XDG_DATA_HOME=/home/swarm/.local/share`,
and pass that variable. The store must hold exactly one transferable `opencode-go`
connection. Use native `opencode auth export opencode-go --standalone` and
`opencode auth import --standalone` to prepare it; never hand-copy `auth.json`
or assume a Windows SQLite store is portable. A writable dedicated Docker volume
is suitable; a read-only store works only if the installed CLI can open/export it
without writing. The plugin exports only the selected integration and imports it
into its private per-Agent store before `serve`; see
[OpenCode authentication](../docs/harnesses/opencode.md#authentication).
Muse/Antigravity credentials are separate vendor stores, not Codex credentials;
their installation does not establish authenticated runtime qualification.

```sh
docker exec eliot git init /workspace/scratch
docker exec eliot swarm launch --profile codex-luna --workspace /workspace/scratch \
  'Run pwd and create hello.txt containing hello.' --wait
docker exec eliot swarm send AGENT_ID 'Read hello.txt and append world.' --wait
docker exec eliot swarm stop AGENT_ID
docker exec eliot swarm daemon stop --drain
docker rm -v eliot
docker volume rm eliot-data
docker image rm eliot:linux
```

Drain is explicit; an engine stop/kill is not a substitute for protocol drain.
The image's stop signal is SIGINT, matching the daemon's existing force-shutdown
handler. Use `swarm daemon stop --drain` first for orderly drain.

## Custody and client boundaries

`status.workspace_custody` is **degraded**, with possible escaped descendants.
Linux uses boot/start-time checked process identities, `/proc` observation and
process groups; a descendant can `setsid` and a daemon SIGKILL does not run Rust
cleanup. Docker's container cgroup v2 is not a delegated per-Agent cgroup and
does not upgrade Eliot custody to exact. Killing/removing the whole isolated
container tears down its PID namespace, but is not per-Agent custody proof.
Adopted-service isolation remains deferred with the local adopted backends.

The Windows desktop uses a Windows named pipe and cannot attach to this Unix
socket daemon. Use `docker exec … swarm` (or an in-container stdio MCP bridge).
Existing `swarm mcp --http` binds container loopback only. Docker port publishing
forwards to the container interface, so `-p 127.0.0.1:PORT:PORT` alone cannot reach
that listener. This image adds no listener, port publishing or authentication
surface; container-to-host cloud MCP exposure is not supported by this recipe.
