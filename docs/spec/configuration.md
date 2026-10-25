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

Project files are loaded only for repositories registered in the global config
(`[projects.<name>] path = "C:/src/app"`), and only from that path, never from an
agent's worktree. Agents can write files in the repo; a rule they add takes effect
only when it reaches the registered checkout, and every reload is reported as a
`source.health` event for `config` naming the changed files. The project name is the
default `project` of agents launched in that repo.

## `config.toml`

```toml
[daemon]
retention.events_days = 30
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
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

[projects.app]
path = "C:/src/app"

[proxies.opencodex]
max_live = 8                          # optional: cap live agents across all profiles using this proxy
attach = "http://127.0.0.1:10100"     # use a running service ...
# launch = ["opencodex", "..."]       # ... or let Eliot start and supervise it
health = "/api/system/health"
api_key_env = "OPENCODEX_API_KEY"     # name of the env var holding the client key; never the key itself

[plugins.fake]
verbs = ["launch", "send", "steer", "inject", "interrupt", "stop", "resume",
         "approve", "deny", "answer", "configure"]   # optional: narrow the declared verbs

[profiles.codex]
harness = "codex"
model = "<model id>"                  # passed through to the harness as is
effort = "high"
permissions = "workspace-write"
workspace_mode = "shared"             # "worktree" from L3.2
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

- Credentials (API keys, OAuth tokens) never live in config, the Store, events or
  messages. Config names environment variables (`api_key_env`); the daemon passes
  them through to the harness process only. Agent tokens are routing handles, not
  credentials ([D-07](../decisions.md)).
- `extends` merges one profile over another (one level; `extends` of an `extends` is
  an error). Missing fields take the [defaults](core.md#profile).
- Separate native accounts are separate profiles whose `env` points the harness at its
  own config/auth dir (`CODEX_HOME`, `CLAUDE_CONFIG_DIR`, ...). Combine with
  `max_live` to respect each account's limits. A launch must fit both its profile's
  and its proxy's `max_live`.
- After `provider.limited` of kind `rate_limit` or `quota`, new launches of that
  profile wait (agents already running are not touched). `retry_after` is not proof of
  recovery: after it passes, one launch runs as a probe and the rest wait until the
  probe's first turn succeeds.
- Harness processes inherit the daemon's environment, then the profile's `env`, then
  the Eliot variables. `env_clear = true` starts from an OS baseline instead (Windows:
  at least `SystemRoot`, `USERPROFILE`, `HOME`, `PATH`, `TEMP`; v1 found that a fully
  cleared environment breaks harness startup) plus the profile's `env`.
- Rust projects: give each worktree its own `CARGO_TARGET_DIR` (profile `env`); two
  live agents building in one target directory block or corrupt each other's builds.
  Eliot does not serialize Cargo for them.
- Windows: keep the data dir short (worktrees live under it and `MAX_PATH` is real);
  Eliot sets `core.longpaths=true` in worktrees it creates and fails with a clear error
  when a path is still too long.
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
in `session.ready`.
