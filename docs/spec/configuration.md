# Configuration

## Data directory

Default `%LOCALAPPDATA%\Eliot` on Windows or `~/.local/share/eliot`; override with
`--data-dir` or `ELIOT_HOME`. One daemon owns one data directory and holds
`daemon.lock`.

```text
<data_dir>/
  config.toml
  eliot.db
  daemon.json             pid, endpoint, version, start identity
  daemon.lock
  logs/daemon.log         rotated and bounded
  rules/*.toml
  plugins/<name>/         shared-plugin state
  plugins/<name>/<agent>-g<generation>/   per-agent plugin state
  services/<name>/        managed service endpoint/process state
  worktrees/<agent>/      worktrees created by Eliot
```

Project files are versioned in the registered repository:

```text
<repo>/.eliot/
  config.toml
  rules/*.toml
  scripts/
```

Project files are loaded only for repositories registered in global config and only
from that registered checkout, never from an agent worktree. A rule an agent writes in
a worktree takes effect only after it reaches the registered checkout. Every reload or
parse failure is reported by the config source.

## `config.toml`

```toml
[daemon]
retention.events_days = 30
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2
automation.check_timeout = "10m"

[process]
scan_interval = "2s"

[git]
poll_interval = "15s"
remote_poll = "off"                 # or "10m": periodic ls-remote
fingerprint_max_files = 5000
fingerprint_max_bytes = 67108864

# Built-in per-agent plugin. Session mode comes from its manifest, not config.
[plugins.codex]

# External per-agent plugin. The host starts this command once per live agent.
[plugins.claude]
cmd = ["node", "C:/src/eliot/plugins/claude/main.mjs"]

# Shared plugins may run a small pool; per-agent plugins reject `instances`.
[plugins.fake]
instances = 1
verbs = ["launch", "send", "steer", "inject", "interrupt", "stop", "resume",
         "approve", "deny", "answer", "configure"]

[projects.app]
path = "C:/src/app"

[proxies.opencodex]
max_live = 8
attach = "http://127.0.0.1:10100"    # attach to owner-run service
# launch = ["opencodex", "..."]       # or supervise a managed service
health = "/api/system/health"
api_key_env = "OPENCODEX_API_KEY"

[profiles.codex]
harness = "codex"
model = "<native model id>"
effort = "high"
billing = "subscription"
account = "main"
permissions = "workspace-write"
workspace_mode = "shared"
labels = { role = "writer" }

[profiles.codex-opencodex]
extends = "codex"
proxy = "opencodex"

[profiles.manager]
harness = "claude"
permissions = "workspace-write"
workspace_mode = "worktree"
control = "project"
labels = { role = "manager" }
```

## General rules

- Credentials never live in config values, Store, events or messages. Config names
  environment variables (`api_key_env`, `secret_env`); the daemon resolves them in
  memory for the native process.
- Agent tokens are generation-scoped routing handles. `ELIOT_AGENT_ID`,
  `ELIOT_AGENT_GENERATION`, `ELIOT_AGENT_TOKEN` and endpoint are injected after the
  profile environment; users do not configure them.
- `extends` merges one base profile (one level). Missing fields use
  [core defaults](core.md#profile).
- Separate native accounts are separate profiles whose environment points to distinct
  auth/config directories (`CODEX_HOME`, `CLAUDE_CONFIG_DIR`, ...). `max_live` applies
  per profile; a proxy may add another cap shared by profiles.
- Provider holds are scoped to the profile/account/proxy that produced the evidence.
  After rate-limit/quota `retry_after`, one launch is a probe; others remain queued
  until that generation reaches a successful first turn. A retry-after timestamp is
  not proof of recovery.
- Harness processes inherit daemon environment, profile `env`, then Eliot variables.
  `env_clear = true` starts from an OS baseline (on Windows at least `SystemRoot`,
  `USERPROFILE`, `HOME`, `PATH`, `TEMP`) plus profile/Eliot values.
- Rust worktrees use separate `CARGO_TARGET_DIR`s. Eliot does not serialize Cargo
  builds from different agents into one target directory.
- Keep the data path short on Windows. Eliot passes `core.longpaths=true` through
  process environment/config arguments for its own and agent Git calls; it fails
  clearly if a tool still cannot address the path.
- Project `[profiles.X]` replaces global `[profiles.X]` as a whole. Other tables merge
  key by key, project over global.
- Unknown keys are errors with file/line, except extra keys inside
  `[plugins.<name>]`, which are handed to and validated by that plugin at initialize.
- `eliot status` shows each effective value and source file; secret values are never
  shown.

## Plugins

| Key | Meaning |
| --- | --- |
| `cmd` | External plugin command. Omitted for built-in plugins. |
| `instances` | Pool size for a plugin whose manifest says `session_mode=shared` (default 1). Invalid for `per_agent`. |
| anything else | Plugin-private configuration passed at `initialize`. |

The manifest, not config, chooses `per_agent` or `shared`; an operator cannot weaken
custody by overriding it. A per-agent instance receives a private state directory for
its agent/generation. A shared instance receives its shared directory and must keep
its native-session mapping isolated internally.

A second plugin entry may reuse the same executable under another name and different
private config. It is still validated against the returned manifest.

## Proxies and managed services

A proxy entry describes OpenCodex, another compatible gateway or a
[local model server](../harnesses/local-models.md):

| Key | Meaning |
| --- | --- |
| `attach` | Base URL of an owner-run service. Eliot health-checks it and never stops it. |
| `launch` | Explicit argv for a managed service. Eliot starts it before first use, watches it, and may stop it after `idle_stop`. |
| `max_live` | Optional live-agent cap across all profiles using this service/account. |
| `idle_stop` | Managed service only; default never. |
| `health` | HTTP path whose 200 response means available. |
| `api_key_env` | Environment variable containing the client key. |
| `base_url_for` | Optional harness-specific base URLs, e.g. `{ claude = ".../anthropic" }`. |

A managed shared service has its own custody object, not an agent Job. Its endpoint and
process birth identity are persisted before an agent uses it. Attached services carry
no Eliot stop authority.

How a profile reaches a proxy is plugin knowledge and documented on the harness page:
Claude uses `ANTHROPIC_BASE_URL`, Codex uses a `model_providers` entry, OpenCode uses a
provider `baseURL`, and so on. The plugin reports the effective endpoint in
`session.ready`; requested URL alone is not proof that traffic used it.
