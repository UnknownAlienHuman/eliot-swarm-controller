# Configuration

Implement the single global Codex profile first. Project overlays, external plugins,
services, mail and automation settings appear only with their owning roadmap layer;
this page is the final configuration contract, not an L1 checklist.

## Private data directory

Default `%LOCALAPPDATA%\Eliot` or `~/.local/share/eliot`; override with `ELIOT_HOME` or
`--data-dir`. Root and private files are current-user-only. Do not accept redirected
symlink/reparse roots or unsafe ownership.

```text
<data_dir>/
  config.toml
  owner.token
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.lock, daemon.json, logs/
  plugins/                 only instantiated plugins
  worktrees/               L3 opt-in workspaces
  services/                managed services, when needed
  rules/                   L4
  frozen/                  bounded admitted entry-file copies, when needed
```

The ordinary CLI reads `owner.token` automatically, only outside managed Agent/rule
environments. A missing managed token rejects, never becomes owner. This is attribution
within one OS user, not hostile-same-user isolation. Tokens never appear in semantic
payloads, recordings or logs.

## First profile

```toml
[profiles.codex-luna]
harness = "codex"
model = "gpt-6-luna"
permissions = "workspace-write"
pass_env = ["CODEX_HOME"]
```

Use the exact model advertised by the installed account's `model/list`; unavailable Luna
fails explicitly. [Codex](../harnesses/codex.md#first-useful-path) owns bring-up steps.
Existing workspace and owner login suffice; no proxy/account registry is mandatory.

## Later configuration example

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

[mail]
max_unread_bytes_per_agent = 10485760
max_unread_messages_per_agent = 10000

[process]
scan_interval = "2s"
terminal_drain_timeout = "10s"

[git]
poll_interval = "15s"
remote_poll = "off"
fingerprint_max_files = 5000
fingerprint_max_bytes = 67108864

[projects.app]
path = "C:/src/app"

[accounts.codex-main]
max_live = 8

[proxies.opencodex]
attach = "http://127.0.0.1:10100"
health = "/api/system/health"
api_key_env = "OPENCODEX_API_KEY"
max_live = 8

[profiles.codex-opencodex]
extends = "codex-luna"
proxy = "opencodex"
account = "codex-main"
```

The OpenCodex health path is an installed-version fact to verify, not a version gate.
Unknown keys are errors with location; plugin-private keys are validated by that plugin.
`extends` is one level. Missing fields use [core defaults](core.md#profile).
Command/write retention must cover the supported retry window and allowed clock skew.

## Authority and project overlays

Load project config/rules only from the owner-registered checkout, never an Agent
worktree. An overlay may narrow a global profile: lower capacity, choose liveness/wake
settings, add non-authority labels, reference local notes/setup and relative scratch.
It cannot replace harness/plugin/executable/args, model/effort, route/account/environment,
permissions/control, workspace mode or Git identity. Labels used by owner rules as an
authorization predicate are owner-defined and cannot be overridden by project config.

Project automation has project authority, not owner authority. Invalid reload retains
the last valid configuration. New profiles/routes are an owner config change; no approval
packet or per-command profile ceremony.

## Frozen profile and route binding

Admission captures the resolved nonsecret profile once. Running and queued work do not
follow reload. Resume uses the stored profile unless the owner explicitly selects a new
one for a generation-changing operation.

```text
ProfileSnapshot {
  profile_name, harness, plugin,
  model?, effort?, permissions, control, args,
  literal_env, pass_env_values, secret_env_names, inherit_env,
  account?, proxy?, billing?, workspace_mode, git_identity,
  labels, max_live?, stall_after, notify_parent, wake_on_mail,
  system_note?, setup[], scratch, source_files
}
PluginBinding { name, kind: builtin|external, cmd?, private_config,
                private_config_digest, executable?, referenced_files[] }
AccountBinding { name, capacity_scope, auth_store_names, requested_billing }
ProxyBinding { name, mode: adopted|managed, endpoint?, launch_cmd?, health_path,
               capacity_scope, api_key_env_name?, base_url_for?, config_digest,
               executable?, referenced_files[] }
FrozenFile { canonical_path, size, digest, private_copy? }
```

Builtin route is the plugin linked in the installed binary. External routes resolve an
absolute executable plus explicit argv/private config; capture local entry files used
by that command. Dispatch uses this binding, not a new config lookup. Changed/missing
captured files fail before execution. Manifest cache keys use binding digest; software
version itself is diagnostic, not authority.

Capture entry files and explicit inputs, not a universal hermetic build/runtime graph.
A script importing mutable dependencies is not fully reproducible merely because its
entry-file digest matches. Use a normal pinned package environment when that project
requires dependency reproducibility; do not invent an artifact platform in Eliot.

Account/store selectors such as `CODEX_HOME` are frozen nonsecret values. Credentials
are execution-time references. Adopted proxy freezes endpoint/health/base URLs/key name;
managed proxy also freezes launch argv and referenced files. Service reuse requires
matching `(name,config_digest)` and exact endpoint/custody. Config reload never mutates
or kills a service still used by old snapshots; conflicting endpoint reuse fails clearly.

## Frozen files

Resolve explicit notes/setup/scripts/entry files under allowed roots, reject escapes,
devices and alternate streams, bound size, capture bytes or canonical path/size/digest.
Verify path references immediately before dispatch. Small text may use an opaque safe
private path. Do not derive filesystem names from command ids containing separators.
Keep a copy while a retained current definition or open/unknown command needs it; reclaim
it afterward. No global content-addressed store, provenance graph or custom GC service.

## Environment and secrets

Build the native environment from OS baseline, admitted nonsecret `pass_env` values,
literal nonsecret `env`, named secrets resolved in memory, then Eliot identity/Git/PATH.
`secret_env` is a subset of `pass_env`; those values are never persisted. Missing named
required values fail clearly. Windows keys compare case-insensitively.

`inherit_env=true` is an owner/global opt-in with a visible warning: unrelated daemon
secrets then reach the process. It is not default. Route/account selectors still remain
explicit. Credential rotation may change authentication success but must not silently
select another account/provider.

Mask known secret values in native output and reject them in caller durable text.
Private routing tokens are handled separately from semantic profile data. Native
children receive their own identity only where isolated environment injection is possible;
otherwise their CLI calls remain attributed to the parent, as the plugin reports.

## Accounts, capacity and provider holds

All named profile/account/proxy caps apply. Reserve capacity before native launch/control
takeover, release on proven no-session failure or terminal observation. Observe-only
uses no control slot; detaching a still-live session does not pretend provider usage
vanished. Uncertain starts retain their reservation.

[Provider holds](monitoring.md#provider-holds) survive restart. Rate/quota may permit one
explicit eligible probe; unknown probe remains reserved. Auth/model removal requires
explicit corrective evidence. Reads, approvals, interrupt and stop remain available.
No automatic provider/model/account fallback.

## Plugins

`[plugins.<name>] cmd` selects an external executable; omitted means builtin. Extra keys
are private validated plugin config; secrets use names, never literal stored values.
Manifest chooses per-Agent/shared mode. Implement only per-Agent mode for the first
Codex path. Shared mode requires actual session/environment/stop isolation, not merely
several chats hosted in one process.

Probe/cache a manifest without creating a billed native session. Existing snapshots use
the matching binding. Recheck availability at dispatch without replacing its route.

## Proxies and services

`attach` adopts an already-running direct endpoint: health/read only, never stop/repair.
`launch` supervises an explicitly selected service in separate custody. Key fields:
`health`, `max_live`, managed-only `idle_stop`, `api_key_env`, optional `base_url_for`.
Agent stop never kills a shared/adopted service. First implement attach, then managed
launch only when needed. Effective routing may be partly unobservable; report unknown,
not the requested URL as verified fact.

## Paths and platform defaults

Keep Windows paths short; use long-path Git arguments/environment without rewriting
repository config. Use a separate Cargo target directory per worktree. Executable, argv,
cwd and env remain separate. Launch/workspace admission never performs an implicit fetch.
All persisted fields and frames are bounded. The roadmap determines when each section
becomes implemented; absent later features return `unsupported`, not a hidden substitute.
