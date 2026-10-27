# Configuration

## Private data directory

Default `%LOCALAPPDATA%\Eliot` on Windows or `~/.local/share/eliot`; override with
`--data-dir` or `ELIOT_HOME`. The root and private files are current-user-only; reject
symlink/reparse-point roots and unsafe ownership.

```text
<data_dir>/
  config.toml
  owner.token            local attribution token used automatically by owner CLI
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.json, daemon.lock
  logs/daemon.log
  rules/*.toml
  plugins/<name>/...
  services/<name>/...
  frozen/<write-id>/...  bounded admitted note/setup/script copies when needed
  worktrees/<agent>/...
```

`owner.token` is high-entropy local attribution, not same-user isolation. It is not
stored in SQLite, logs or Agent environments. Ordinary CLI outside an Eliot Agent/rule
environment reads it automatically; there is no `--owner` ceremony and no no-token
owner fallback.

Project files are loaded only from owner-registered checkouts, never an Agent worktree:

```text
<registered repo>/.eliot/config.toml
<registered repo>/.eliot/rules/*.toml
<registered repo>/.eliot/scripts/*
```

Invalid reload keeps the last valid configuration and reports source health.

## Global configuration example

```toml
[daemon]
retention.events_days = 30
retention.commands_days = 30
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
limits.max_selector_targets = 1000
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2
automation.check_timeout = "10m"

[process]
scan_interval = "2s"
terminal_drain_timeout = "10s"

[git]
poll_interval = "15s"
remote_poll = "off"
fingerprint_max_files = 5000
fingerprint_max_bytes = 67108864

[plugins.codex]                    # built in
[plugins.claude]
cmd = ["node", "C:/src/eliot/plugins/claude/main.mjs"]

[projects.app]
path = "C:/src/app"

[accounts.codex-main]
max_live = 8

[proxies.opencodex]
max_live = 8
attach = "http://127.0.0.1:10100"
health = "/api/system/health"
api_key_env = "OPENCODEX_API_KEY"

[profiles.codex]
harness = "codex"
model = "<native model id>"
effort = "high"
permissions = "workspace-write"
billing = "subscription"
account = "codex-main"
pass_env = ["CODEX_HOME"]
secret_env = []
workspace_mode = "shared"
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

Unknown keys are load errors with file/line, except plugin-private keys inside
`[plugins.<name>]`, which the plugin validates at initialize. `extends` is one level.
`eliot status` shows effective profile sources without secret values.

## Authority and project overlays

Global config defines harness/plugin executable, account, proxy/service, environment,
permissions/control and base profiles. Project config can only narrow an existing
same-name global profile:

- add bounded labels;
- lower `max_live`;
- choose `stall_after`, `notify_parent`, `wake_on_mail`;
- reference project-local `system_note`, `setup` and relative `scratch` entries.

It cannot change/add harness, plugin/cmd/args, model/effort, permissions, control,
workspace mode, Git identity, literal/pass/secret/inherit environment, account, proxy,
billing or endpoint. An owner adds another global profile for a different route.

Project rules/scripts remain project principals and can target only that project.

## Frozen profile and admitted files

Admission stores one bounded `profile_snapshot` on the Agent/command. It contains:

```text
ProfileSnapshot {
  profile_name, harness, plugin,
  model?, effort?, permissions, control,
  args, literal_env, pass_env_names, secret_env_names, inherit_env,
  account?, proxy?, billing?, workspace_mode, git_identity,
  labels, max_live?, stall_after, notify_parent, wake_on_mail,
  system_note?, setup[], scratch, source_files
}
FrozenFile { canonical_path, size, digest, private_copy? }
```

Secrets remain variable names only. Existing Agents never silently adopt a config reload.
Resume/control takeover uses the stored snapshot unless the owner explicitly selects a
new profile, which creates a new generation.

For project notes/setup/scripts:

1. resolve under the registered allowed root;
2. reject `..`, symlink/reparse-point escape, device/alternate-stream forms;
3. bound size and compute digest at admission;
4. copy small text/scripts to private `frozen/<write-id>/` or store bounded bytes;
5. otherwise store canonical path+size+digest and verify immediately before dispatch;
6. mismatch fails the command rather than running different bytes.

This is not a global content-addressed artifact store and has no artifact graph/GC.
Frozen files are deleted when no retained open/unknown command or current profile
snapshot references them.

## Environment and secrets

Default process environment is minimal:

1. OS baseline required to start normal programs;
2. variables named by global profile `pass_env`;
3. literal nonsecret profile `env`;
4. account/proxy/plugin secret references resolved in memory;
5. Eliot Agent or scoped run variables, Git identity and CLI path.

Later layers override earlier ones. Windows keys compare case-insensitively.
`inherit_env=true` is global-profile-only and visibly warned because all daemon secrets
then reach the Agent/tools.

`secret_env` names must also appear in `pass_env`; resolved values are masked from native
output and rejected in caller-supplied durable semantic text. Raw owner, Agent and run
tokens never appear in config/events/messages/fixtures/logs.

A native child gets its own Agent token only when the plugin can inject an isolated
environment before tools run. Otherwise it has `own_identity=false` and no independent
CLI principal.

## Accounts, capacity and provider holds

A launch/control takeover must fit all named scopes:

- profile `max_live`;
- account `max_live` across every profile using it;
- proxy/service `max_live`.

Capacity is reserved atomically before native I/O and released only on exact no-session
failure or terminal observation. Observe-only attach uses no capacity; control attach
reserves before takeover. Detach of a still-live provider session does not pretend the
provider capacity vanished.

Provider holds are durable in the Store (schema in
[monitoring](monitoring.md#provider-holds)). `rate_limit|quota` may permit one explicit
probe after their boundary. Unknown probe remains reserved across restart. `auth` and
`model_gone` clear only after explicit owner/config evidence. Eliot never silently
changes account/model/provider/route.

## Plugins

`[plugins.<name>] cmd` names an external plugin; omitted means built-in. Extra keys are
private plugin configuration.

The plugin manifest chooses `per_agent|shared`; config cannot weaken it. A per-Agent
instance receives a private generation directory. A shared plugin must prove exact
session attribution/readback/isolated stop according to the plugin protocol.

The daemon obtains a manifest from a short initialize/shutdown probe and caches it until
restart or command/config revision changes. Launch preflight uses the cached availability
and capabilities; it does not create a native session.

## Proxies and services

A proxy/service entry is one of:

- `attach`: owner-run/adopted endpoint; health-check/read-only, never stopped or repaired;
- `launch`: daemon-managed service with separate service custody, endpoint/PID/birth
  identity persisted before use.

| Key | Meaning |
| --- | --- |
| `attach` | owner-run base URL |
| `launch` | explicit argv for managed service |
| `health` | bounded readiness probe |
| `max_live` | capacity across profiles using the service |
| `idle_stop` | managed only; default never |
| `api_key_env` | environment-variable name holding client key |
| `base_url_for` | optional per-harness explicit endpoint |

Agent stop never kills a shared service. Attached service is never killed by Eliot.
Requested URL is not proof of routing: plugin reports effective provider/endpoint and
live qualification verifies it.

## Paths and platform defaults

- keep Windows data/worktree paths short and pass `core.longpaths=true` without editing
  repository config;
- separate Cargo target directory per Agent worktree;
- resolve executable and argv separately;
- project cleanup/setup/scratch uses the path rules above;
- no implicit fetch during launch admission or workspace dispatch;
- configuration values/counts/strings are bounded before persistence or plugin framing.
