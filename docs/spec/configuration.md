# Configuration

## Private data directory

Default `%LOCALAPPDATA%\Eliot` (Windows) or `~/.local/share/eliot`; override with
`--data-dir` / `ELIOT_HOME`. Create the directory and files with current-user-only
permissions/ACL. Refuse symlink/reparse-point data roots and unsafe ownership. Logs are
rotated/bounded/private.

```text
<data_dir>/
  config.toml
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.json, daemon.lock
  logs/
  rules/
  plugins/<name>/...
  services/<name>/...
  worktrees/<agent>/...
```

Project `.eliot/{config.toml,rules/,scripts/}` is loaded only from a globally registered
checkout, never an agent worktree. Agent changes take effect only after reaching that
checkout. Reload errors keep the last valid config/rules and report source health.

## Example

```toml
[daemon]
retention.events_days = 30
store.wal_warn_bytes = 536870912
store.wal_hard_bytes = 2147483648
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2
automation.check_timeout = "10m"

[process]
scan_interval = "2s"
terminal_drain_timeout = "3s"
linux_exact_custody = "auto"      # require|auto|off; auto reports degraded fallback

[git]
poll_interval = "15s"
remote_poll = "off"
fingerprint_max_files = 5000
fingerprint_max_bytes = 67108864

[projects.app]
path = "C:/src/app"

[accounts.codex-main]
max_live = 8

[plugins.codex]

[plugins.claude]
cmd = ["node", "C:/src/eliot/plugins/claude/main.mjs"]

[plugins.fake]
instances = 1                       # only shared plugins accept this

[proxies.opencodex]
max_live = 8
attach = "http://127.0.0.1:10100"
health = "/api/system/health"
api_key_env = "OPENCODEX_API_KEY"

[webhooks.ops]
url_env = "ELIOT_OPS_WEBHOOK_URL"
secret_env = ["ELIOT_OPS_WEBHOOK_TOKEN"]

[profiles.codex]
harness = "codex"
model = "<native model id>"
effort = "high"
billing = "subscription"
account = "codex-main"
permissions = "workspace-write"
env_mode = "minimal"
pass_env = ["CODEX_HOME"]
workspace_mode = "shared"
labels = { role = "writer" }

[profiles.codex-opencodex]
extends = "codex"
proxy = "opencodex"
pass_env = ["CODEX_HOME", "OPENCODEX_API_KEY"]

[profiles.manager]
harness = "claude"
account = "claude-main"
permissions = "workspace-write"
env_mode = "minimal"
pass_env = ["CLAUDE_CONFIG_DIR"]
workspace_mode = "worktree"
control = "project"
labels = { role = "manager" }
```

## Environment policy

Default `env_mode="minimal"`; agents do **not** inherit the daemon's complete
environment. The baseline contains only OS essentials needed to spawn programs
(PATH, home/profile, system root, temp, locale and platform certificate variables).
Everything else is explicit:

- `env = { NAME="literal nonsecret value" }`;
- `pass_env = ["NAME"]` copies the daemon environment value at launch and treats it as
  secret for masking;
- `secret_env` marks literal `env` names whose values also require masking;
- proxy/account/plugin credential variables are automatically secret and passed only
  to the process/session that needs them;
- current agent token is automatically secret.

`env_mode="inherit"` is an explicit owner opt-in for a trusted profile. `eliot status`
warns that the full daemon environment reaches the harness. Empty/minimal is not
implemented by deleting Windows `SystemRoot`/profile/temp/PATH.

Known secret values are never stored in resolved config, command args, mail, vars,
rules or logs. Durable semantic payloads containing them are rejected; native output is
masked. Named environment references are persisted, not their values.

## Accounts, capacity and provider holds

A profile's `account` references `[accounts.<id>]`. Capacity is the minimum of:

- profile `max_live` (optional local narrowing);
- shared account `max_live`;
- proxy/service `max_live`.

Profiles with different model/permission settings but the same subscription/account
therefore share one cap. If `account` is omitted, the profile itself is the capacity
scope and status warns that cross-profile sharing is unknown.

Provider limitation records are persisted:

```sql
CREATE TABLE provider_holds (
  scope TEXT PRIMARY KEY,            -- exact route/model/account scope
  kind TEXT NOT NULL,
  coverage TEXT NOT NULL,
  retry_after INTEGER,
  probe_agent INTEGER,
  probe_generation INTEGER,
  state TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
```

The plugin reports limitation scope/coverage. Unknown coverage holds only the exact
profile route, not every account. `retry_after` permits one future **real user turn** as
a probe; Eliot never generates a hidden model call. The hold clears only after a
successful provider-backed turn/readback in that scope. Restart restores holds/probe
state.

## Profiles and merges

One-level `extends`; project profile replaces the global profile as a whole. Other
tables merge key-by-key. Unknown keys are errors except plugin-private keys validated
at initialize. Names, labels, argv/env counts and values are bounded.

`setup` and `scratch` entries are relative to the worktree, reject absolute/`..` paths,
and never authorize cleanup outside it. Rust worktrees use distinct
`CARGO_TARGET_DIR`s. Eliot does not serialize independent builds into one target dir.

## Plugins

`cmd` selects an external plugin. `instances` is valid only when the returned manifest
is `shared`; per-agent mode gets one private instance/data directory per generation.
The operator cannot override manifest custody mode. A shared controlling plugin must
prove per-session environment and isolated stop where tools require them.

## Proxies, services and webhooks

`attach` means external owner service: health-check/read-only management, never stop or
reconfigure. `launch` means managed service with separate custody, persisted endpoint
and process identity. Agent stop never stops a shared service.

Proxy API keys and webhook URLs/tokens come from named environment variables. Rule
actions store endpoint name (`ops`), never raw URL/header/secret. Effective proxy/model/
billing route is reported by the plugin; requested configuration alone is not proof.

## Store/WAL limits

The daemon periodically passive-checkpoints WAL, records size and identifies long
readers. API replays paginate and release read transactions between pages. At warning
size status/source health reports pressure. At hard size the daemon refuses new long
read/replay operations while continuing short control/result commits; it never deletes
rows, kills readers blindly or runs an unsafe truncate checkpoint.
