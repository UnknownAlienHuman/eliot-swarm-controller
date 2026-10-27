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
  frozen/<opaque-safe-id>/... bounded admitted note/setup/script/plugin/service copies
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
idempotency_window = "24h"
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
limits.max_selector_targets = 1000
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2
automation.check_timeout = "10m"
automation.max_event_backlog = 100000

[process]
scan_interval = "2s"
terminal_drain_timeout = "10s"

[git]
poll_interval = "15s"
remote_poll = "off"
fingerprint_max_files = 5000
fingerprint_max_bytes = 67108864

[plugins.codex]
[plugins.claude]
cmd = ["C:/Program Files/nodejs/node.exe", "C:/src/eliot/plugins/claude/main.mjs"]

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

`retention.commands_days` and non-command write retention must cover
`idempotency_window` plus allowed future clock skew; invalid shorter configurations are
rejected. Rule `limit.window` may not exceed retained rule-event accounting history.

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

## Frozen profile and route binding

Admission stores one bounded `profile_snapshot` on the Agent/command:

```text
ProfileSnapshot {
  profile_name, harness,
  plugin: PluginBinding,
  model?, effort?, permissions, control,
  args, literal_env,
  pass_env_values,
  secret_env_names,
  inherit_env,
  account: AccountBinding?,
  proxy: ProxyBinding?,
  billing?, workspace_mode, git_identity,
  labels, max_live?, stall_after, notify_parent, wake_on_mail,
  system_note?, setup[], scratch, source_files
}
PluginBinding {
  name,
  kind: builtin | external,
  cmd?,
  private_config, private_config_digest,
  executable?, referenced_files[]
}
AccountBinding {
  name, capacity_scope,
  auth_store_names,
  requested_billing
}
ProxyBinding {
  name,
  mode: adopted | managed,
  endpoint?,
  launch_cmd?,
  health_path,
  capacity_scope,
  api_key_env_name?,
  base_url_for?,
  config_digest,
  executable?, referenced_files[]
}
FrozenFile { canonical_path, size, digest, private_copy? }
```

### Plugin binding

For a built-in plugin, the route is its name inside the installed Eliot binary; its
reported version is diagnostic, not an authorization gate. For an external plugin,
admission freezes exact command argv and canonical private config. `cmd[0]` must resolve
to an absolute owner-controlled executable. Every local script/module path named by the
command is recorded as `FrozenFile` or copied privately. Mutable cwd-relative commands
and a command that resolves differently at dispatch are rejected.

Dispatch uses frozen `PluginBinding`, never current `[plugins.<name>]`. It rechecks
referenced paths/digests before start. Missing/changed old route fails before native I/O;
it never substitutes another binary or private setting. Manifest probe/cache is keyed by
binding digest. Version is display evidence, not an authorization gate.

### Account, proxy and service binding

A profile freezes account name/capacity scope and all nonsecret route/store selectors
such as admitted `CODEX_HOME`. Secret values remain execution-time references.

An adopted proxy freezes direct normalized endpoint, health path, per-harness base URLs
and key-variable name. A managed proxy freezes absolute launch argv, referenced files,
health path and endpoint metadata. Dispatch never rereads current
`[proxies.<name>]`.

Managed/adopted service identity is `(proxy name, config_digest)`. A live service is
reused only when that exact identity and custody/endpoint readback match. If config
changes while an old binding is in use, future admissions use the new binding but Eliot
does not mutate or kill the old service. Starting both versions under one conflicting
endpoint/name is refused; owner waits for old users to drain or defines another proxy
name. This is exact desired-state identity, not a provider-version gate.

Existing Agents never silently adopt config reloads. Resume uses its stored snapshot by
default. A control takeover may use stored snapshot or an explicitly selected owner
profile; either is frozen before admission and targets the next generation.

## Frozen files

For project notes/setup/scripts and external plugin/service files:

1. resolve under registered allowed root;
2. reject `..`, symlink/reparse-point escape, device/alternate-stream forms;
3. bound size and compute digest at admission;
4. copy small text/scripts to private `frozen/<opaque-safe-id>/` or store bounded bytes;
5. otherwise store canonical path+size+digest and verify immediately before dispatch;
6. mismatch fails rather than executing changed bytes.

Store allocates opaque path components from a filesystem-safe alphabet; they are not
derived from command strings. There is no global content-addressed artifact store or
artifact graph/GC. Frozen copies are removed only when no retained current snapshot/
definition or open/unknown command references them.

## Environment and secrets

Default process environment is minimal:

1. OS baseline required to start normal programs;
2. frozen values of nonsecret variables named by global profile `pass_env`;
3. frozen literal nonsecret profile `env`;
4. current values of names in `secret_env` and account/proxy/plugin secret references,
   resolved in memory immediately before process start;
5. target-generation Agent or scoped run variables, Git identity and CLI path.

At admission every `pass_env` name not listed in `secret_env` is read once, bounded and
stored as `pass_env_values`; this freezes route-bearing values such as `CODEX_HOME`.
`secret_env` must be a subset of `pass_env`; its values are never persisted. A missing
or changed secret may cause auth/readback failure but cannot silently alter the frozen
nonsecret store/endpoint/account binding. Explicit account/profile fields remain the
billing identity shown to the user.

Later layers override earlier ones. Windows keys compare case-insensitively.
`inherit_env=true` is global-profile-only and visibly warned because all daemon secrets
then reach the Agent/tools; an inherited profile freezes nonsecret route values needed
to identify store/account and treats remaining inherited values as execution-time
environment, never durable semantic data.

Resolved secret values are masked from native output and rejected in caller durable
semantic text. Raw owner, Agent and run tokens never appear in config/events/messages/
fixtures/logs.

A native child gets its own Agent token only when the plugin can inject an isolated
environment before tools run. Otherwise it has no child principal; calls from a shared
parent environment are attributed to the parent generation.

## Accounts, capacity and provider holds

A launch/control takeover must fit all named scopes:

- profile `max_live`;
- account `max_live` across every profile using it;
- proxy/service `max_live`.

Capacity is reserved atomically before native I/O and released only on exact no-session
failure or terminal observation. Observe-only attach uses no capacity; control attach
reserves before takeover. Detach of a still-live provider session does not pretend
provider capacity vanished.

Provider holds are durable in the Store (schema in
[monitoring](monitoring.md#provider-holds)). `rate_limit|quota` may permit one explicit
probe after their boundary. Unknown probe remains reserved across restart. `auth` and
`model_gone` clear only after explicit owner/config evidence. Eliot never silently
changes account/model/provider/route.

## Plugins

`[plugins.<name>] cmd` names an external plugin; omitted means built-in. Extra keys are
private plugin config. Secret values are referenced by names and resolved only in
memory; frozen private config contains no secret values.

Manifest chooses `per_agent|shared`; config cannot weaken it. A per-Agent instance gets
a private generation directory. A shared plugin must prove exact session attribution,
readback and isolated stop.

Daemon gets a manifest from short initialize/shutdown probe of current configured route
and caches by `PluginBinding` digest. Launch preflight uses the manifest matching the
admitted frozen binding; reload cannot replace it. Availability is rechecked at dispatch
without creating a native session.

## Proxies and services

A proxy entry is one of:

- `attach`: owner-run/adopted endpoint; health-check/read-only, never stopped/repaired;
- `launch`: daemon-managed service with separate service custody, endpoint/PID/birth
  identity persisted before use.

| Key | Meaning |
| --- | --- |
| `attach` | owner-run direct base URL |
| `launch` | explicit argv for managed service |
| `health` | bounded readiness probe |
| `max_live` | capacity across profiles using service |
| `idle_stop` | managed only; default never |
| `api_key_env` | variable name holding client key |
| `base_url_for` | optional per-harness direct endpoint |

Agent stop never kills a shared service. Adopted service is never killed by Eliot.
Requested URL is not proof of routing: plugin reports effective endpoint and L7 verifies
it.

## Paths and platform defaults

- keep Windows data/worktree paths short and pass `core.longpaths=true` without editing
  repository config;
- separate Cargo target directory per Agent worktree;
- resolve executable and argv separately;
- project cleanup/setup/scratch and external plugin/service files use path rules above;
- no implicit fetch during launch admission/workspace dispatch;
- values/counts/strings are bounded before persistence or plugin framing.
