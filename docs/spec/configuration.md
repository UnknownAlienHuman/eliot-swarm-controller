# Configuration

## Data directory

Default `%LOCALAPPDATA%\Eliot` (Windows) or `~/.local/share/eliot`; override with
`--data-dir` or `ELIOT_HOME`. The daemon creates it on first start, readable only by
the current user, and takes `daemon.lock` (one daemon per data dir).

```text
<data_dir>/
  config.toml          global config (below)
  eliot.db             SQLite store
  daemon.json          pid, endpoint, version (written by the daemon)
  daemon.lock
  logs/daemon.log      rotated, bounded
  rules/*.toml         global rules
  plugins/<name>/      plugin private state (generated hook configs)
  worktrees/<agent>/   worktrees created by Eliot (when workspace_mode = worktree)
```

Project-level files live in the repository, so they are versioned with the code:

```text
<repo>/.eliot/
  config.toml          project profile overlays and settings
  rules/*.toml
  scripts/
```

Project files are loaded only for repositories registered in the global config
(`[projects.<name>] path = "C:/src/app"`), and only from that path, never from an
agent's worktree. Agents can write files in the repo; a rule they add takes effect
only when it reaches the registered checkout, and every reload is reported as a
`source.health` event for `config` naming the changed files. A file that fails to load
keeps the previous version active. The project name is the default `project` of
agents launched in that repo.

## `config.toml`

```toml
[daemon]
retention.events_days = 30
retention.commands_days = 30
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
limits.max_selector_targets = 1000
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2       # goal checks running at once
automation.check_timeout = "10m"

[process]
scan_interval = "2s"

[git]
poll_interval = "15s"
remote_poll = "off"            # or "10m": periodic ls-remote

[plugins.codex]                # built-in: no cmd needed
[plugins.claude]
cmd = ["node", "C:/src/eliot/plugins/claude/main.mjs"]   # external plugin, any language

[projects.app]
path = "C:/src/app"

[accounts.codex-main]
max_live = 8                          # one subscription: cap live agents across every profile that uses it

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
effort = "high"                       # the harness's own value
billing = "subscription"
account = "codex-main"
pass_env = ["CODEX_HOME"]
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
  messages. A profile's literal `env` holds non-secret values only. Secrets are named:
  `api_key_env` (proxy key) and `pass_env` (variables passed from the daemon's
  environment); `secret_env` marks which `pass_env` names hold secrets, so their
  values are masked in events and refused in stored writes. The daemon reads the
  values only when it starts the process. Agent tokens are routing handles, not
  credentials ([D-07](../decisions.md)).
- `extends` merges one profile over another (one level; `extends` of an `extends` is
  an error). Missing fields take the [defaults](core.md#profile).
- Rust projects: give each worktree its own `CARGO_TARGET_DIR` (profile `env`); two
  live agents building in one target directory block or corrupt each other's builds.
  Eliot does not serialize Cargo for them.
- Windows: keep the data dir short (worktrees live under it and `MAX_PATH` is real);
  Eliot passes `core.longpaths=true` to its own `git` calls and to agent processes
  through `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_0`/`GIT_CONFIG_VALUE_0` (no config file is
  written) and fails with a clear error when a path is still too long.
- Unknown keys are an error at load time, with the file and line — except inside
  `[plugins.<name>]`, whose extra keys are passed to the plugin and validated by it at
  `initialize`.
- `eliot status` shows the effective merged config source for each profile.

## Project overlays

A project file can only **narrow** what the owner configured globally, because agents
can write to the repository. A project `[profiles.X]` must name a global profile `X`
(same name) and may set only: `labels` (added), `max_live` (not above the global
value), `stall_after`, `notify_parent`, `wake_on_mail`, `system_note`, `setup` and
`scratch` (paths inside the repository). Any other key — harness, cmd, args, model,
effort, permissions, control, env, pass_env, inherit_env, secret_env, account, proxy,
billing — is a load error naming the key; the owner adds a global profile instead.
Other project tables merge key by key, project over global, for the keys documented
as project-settable.

## Environment

A harness process gets a **minimal environment**, not the daemon's:

1. an OS baseline — Windows: `SystemRoot`, `SystemDrive`, `windir`, `ComSpec`,
   `PATHEXT`, `PATH`, `TEMP`, `TMP`, `USERPROFILE`, `HOMEDRIVE`, `HOMEPATH`,
   `APPDATA`, `LOCALAPPDATA`, `ProgramData`, `ProgramFiles`, `USERNAME`, `COMPUTERNAME`,
   `PROCESSOR_ARCHITECTURE`, `NUMBER_OF_PROCESSORS`; Unix: `HOME`, `PATH`, `USER`,
   `LOGNAME`, `SHELL`, `LANG`, `LC_*`, `TMPDIR`, `TERM` (v1 found that a fully
   cleared environment breaks harness startup);
2. the values of the variables named in the profile's `pass_env`;
3. the profile's literal `env`;
4. the proxy wiring the plugin needs (e.g. the variable named by `api_key_env`);
5. the Eliot variables (`ELIOT_HOME`, `ELIOT_AGENT_ID`, `ELIOT_AGENT_TOKEN`, `PATH`
   with `eliot`, Git identity, `GIT_CONFIG_*`).

Later steps override earlier ones; on Windows names compare case-insensitively.
`inherit_env = true` (global profiles only) starts from the daemon's whole environment
instead of step 1; `eliot show` warns about it, because every secret the daemon can see
then reaches the agent and everything it runs. Scripts get the same baseline plus the
rule's literal `env`; a global rule may also name `pass_env`.

## Accounts

A subscription or API key is shared by every profile that uses it, so limits live on
the account:

| Key | Meaning |
| --- | --- |
| `[accounts.<name>] max_live` | Cap on live agents across all profiles with `account = "<name>"`. |

A launch must fit its profile's, its account's and its proxy's `max_live`; it waits in
`created` until all three have a free slot. Separate native logins are separate
accounts whose profiles point the harness at their own config/auth dir (`CODEX_HOME`,
`CLAUDE_CONFIG_DIR`, ... via `pass_env` or `env`).

After `provider.limited` of kind `rate_limit` or `quota`, new launches and new sends of
that account wait (`hold`); agents already in a turn are not touched. `retry_after` is
not proof of recovery: after it passes, one queued launch or send runs as a probe and
the rest wait until the probe's turn succeeds. A probe whose outcome is unknown keeps
the hold. `model_gone` and `auth` holds clear only by a config change or
`eliot hold clear ACCOUNT`. Holds live in memory: after a daemon restart the first
launch or send of the account is the probe. A rule may react to `provider.limited`,
but Eliot itself never switches an agent to another account, model or route.

## Plugins

| Key | Meaning |
| --- | --- |
| `cmd` | Command of an external plugin. Omitted for built-in plugins. Global config only. |
| anything else | Passed to the plugin as `plugin_config` (e.g. the fake plugin's `verbs`). |

Whether a plugin runs one process per agent or one for many is the plugin's manifest
(`features.multi_session`), not configuration. A second entry can reuse a plugin under
another name (`[plugins.fake-narrow] cmd = ["eliot", "plugin", "fake"]`).

## Proxies

A proxy entry describes a provider proxy (OpenCodex, any OpenAI/Anthropic-compatible
gateway, or a [local model server](../harnesses/local-models.md)):

| Key | Meaning |
| --- | --- |
| `attach` | Base URL of an already running proxy. Eliot checks `health` and never stops it. |
| `max_live` | Optional cap on live agents across all profiles that use this proxy. |
| `launch` | Command to start it. Eliot supervises it as a service: starts before the first agent that needs it, health-checks, restarts with backoff, stops when no agent uses it for `idle_stop` (default never). |
| `idle_stop` | For `launch`ed proxies: stop after no agent used it for this long (default: never). |
| `health` | Path for the health check (HTTP 200 = up). |
| `api_key_env` | Env var with the key the harness must send to the proxy. |
| `base_url_for` | Optional per-harness override, e.g. `{ claude = "http://127.0.0.1:10100/anthropic" }`. |

How a profile's proxy reaches the harness is plugin knowledge (each plugin's manifest
`proxy` section and its [harness page](../harnesses/README.md)): e.g. Claude Code via
`ANTHROPIC_BASE_URL` + `ANTHROPIC_AUTH_TOKEN`, Codex via a `model_providers` entry,
OpenCode via a provider `options.baseURL`. The plugin reports the effective endpoint
in `session.ready`.
