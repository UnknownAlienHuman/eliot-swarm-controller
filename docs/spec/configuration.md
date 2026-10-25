# Configuration

## Data directory

Default `%LOCALAPPDATA%\Eliot` (Windows) or `~/.local/share/eliot`; override with
`--data-dir` or `ELIOT_HOME`. The daemon creates it on first start and takes
`daemon.lock` (one daemon per data dir).

```text
<data_dir>/
  config.toml          global config (below)
  eliot.db             SQLite store
  daemon.json          pid, endpoint, version (written by the daemon)
  daemon.lock
  logs/daemon.log      rotated, bounded
  rules/*.toml         global rules
  plugins/<name>/      plugin private state (offsets, generated hook configs)
  worktrees/<agent>/   worktrees created by Eliot (when workspace_mode = worktree)
```

Project-level files live in the repository, so they are versioned with the code:

```text
<repo>/.eliot/
  config.toml          project profiles and settings (merged over global)
  rules/*.toml
  scripts/
```

## `config.toml`

```toml
[daemon]
retention.events_days = 30
limits.max_depth = 8
automation.max_actions_per_minute = 120

[process]
scan_interval = "2s"

[git]
poll_interval = "15s"
remote_poll = "off"            # or "10m": periodic ls-remote

[plugins.codex]                # built-in: no cmd needed
instances = 1
[plugins.claude]
cmd = ["node", "C:/src/eliot/plugins/claude/main.mjs"]   # external plugin, any language

[proxies.opencodex]
attach = "http://127.0.0.1:10100"     # use a running service ...
# launch = ["opencodex", "..."]       # ... or let Eliot start and supervise it
health = "/api/system/health"
api_key_env = "OPENCODEX_API_KEY"     # name of the env var holding the client key; never the key itself

[profiles.codex]
harness = "codex"
model = "<model id>"                  # passed through to the harness as is
effort = "high"
permissions = "workspace-write"
workspace_mode = "worktree"
labels = { role = "writer" }

[profiles.codex-opencodex]
extends = "codex"
proxy = "opencodex"

[profiles.manager]
harness = "claude"
permissions = "workspace-write"
control = "project"
labels = { role = "manager" }
```

Rules:

- Secrets never live in config, the Store, events or messages. Config names
  environment variables (`api_key_env`); the daemon passes them through to the
  harness process only.
- `extends` merges one profile over another (one level is enough; no chains needed).
- Unknown keys are an error at load time, with the file and line.
- `eliot status` shows the effective merged config source for each profile.

## Proxies

A proxy entry describes a provider proxy (OpenCodex or any OpenAI/Anthropic-compatible
gateway):

| Key | Meaning |
| --- | --- |
| `attach` | Base URL of an already running proxy. Eliot checks `health` and never stops it. |
| `launch` | Command to start it. Eliot supervises it as a service: starts before the first agent that needs it, health-checks, restarts with backoff, stops when no agent uses it for `idle_stop` (default never). |
| `health` | Path for the health check (HTTP 200 = up). |
| `api_key_env` | Env var with the key the harness must send to the proxy. |
| `base_url_for` | Optional per-harness override, e.g. `{ claude = "http://127.0.0.1:10100/anthropic" }`. |

How a profile's proxy reaches the harness is plugin knowledge (each plugin's manifest
`proxy` section and its [harness page](../harnesses/README.md)): e.g. Claude Code via
`ANTHROPIC_BASE_URL` + `ANTHROPIC_AUTH_TOKEN`, Codex via a `model_providers` entry,
OpenCode via a provider `options.baseURL`. The plugin reports the effective endpoint
in `agent.ready`.
