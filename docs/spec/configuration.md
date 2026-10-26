# Configuration

## Private data directory

Default `%LOCALAPPDATA%\Eliot` (Windows) or `~/.local/share/eliot`; override with
`--data-dir` / `ELIOT_HOME`. Create current-user-only permissions/ACL and refuse
symlink/reparse-point roots or unsafe ownership. Logs are private, rotated and bounded.

```text
<data_dir>/
  config.toml
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.json, daemon.lock
  logs/, rules/, plugins/, services/, worktrees/
```

Project `.eliot/{config.toml,rules/,scripts/}` is loaded only from a globally registered
checkout, never an agent worktree. Agent edits take effect only after reaching that
checkout. Invalid reload keeps the last valid revision and reports health.

## Configuration authority

Global config owns daemon/process/Store/Git limits, projects, accounts, plugins,
proxies, services, webhooks, allowed environment names and base profiles.

Project config may only:

- define/replace profiles whose names are in that project's `profiles_allow`;
- narrow permissions, control, labels, workspace mode, model/effort and limits;
- select only that project's allowed global account/proxy/profile references;
- add nonsecret literal env values and `pass_env` names from the project's allowlist;
- set project-local setup/scratch/system-note paths.

Project config cannot define executable `cmd`, plugin instances/private config,
accounts/proxies/services/webhooks, `env_mode=inherit`, arbitrary `pass_env`, daemon
limits or another project path. Project rules/scripts execute under that project's
principal, never owner by omission.

A project agent/rule may launch only an effective profile named in
`[projects.<name>].profiles_allow`. The router checks this even when the profile exists
globally. Owner may launch any global profile. This prevents project control from
selecting a profile carrying another project's account, inherited environment, plugin
command or proxy credentials.

## Example

```toml
[daemon]
retention.events_days = 30
retention.turns_days = 90
store.wal_warn_bytes = 536870912
store.wal_hard_bytes = 2147483648
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2
automation.check_timeout = "10m"
automation.max_event_backlog = 100000

[process]
scan_interval = "2s"
terminal_drain_timeout = "3s"
linux_exact_custody = "auto"       # require|auto|off

[git]
poll_interval = "15s"
remote_poll = "off"
fingerprint_max_files = 5000
fingerprint_max_bytes = 67108864

[projects.app]
path = "C:/src/app"
profiles_allow = ["codex", "codex-opencodex", "manager"]
accounts_allow = ["codex-main", "claude-main"]
proxies_allow = ["opencodex"]
pass_env_allow = ["CODEX_HOME", "CLAUDE_CONFIG_DIR"]
webhook_allow = ["ops"]

[accounts.codex-main]
max_live = 8

[accounts.claude-main]
max_live = 4

[plugins.codex]
[plugins.claude]
cmd = ["node", "C:/src/eliot/plugins/claude/main.mjs"]
[plugins.fake]
instances = 1

[proxies.opencodex]
max_live = 8
attach = "http://127.0.0.1:10100"
health = "/api/system/health"
api_key_env = "OPENCODEX_API_KEY"

[webhooks.ops]
url_env = "ELIOT_OPS_WEBHOOK_URL"
secret_env = ["ELIOT_OPS_WEBHOOK_TOKEN"]
idempotency_header = "Idempotency-Key"

[profiles.codex]
harness = "codex"
account = "codex-main"
model = "<native model id>"
effort = "high"
permissions = "workspace-write"
env_mode = "minimal"
pass_env = ["CODEX_HOME"]
workspace_mode = "shared"
labels = { role = "writer" }

[profiles.codex-opencodex]
extends = "codex"
proxy = "opencodex"

[profiles.manager]
harness = "claude"
account = "claude-main"
permissions = "workspace-write"
pass_env = ["CLAUDE_CONFIG_DIR"]
workspace_mode = "worktree"
control = "project"
labels = { role = "manager" }
```

## Environment and secrets

Default `env_mode="minimal"`; do not inherit the daemon's complete environment.
Baseline contains only OS spawn essentials (PATH, home/profile, system root, temp,
locale and required certificate variables). Additional values are explicit:

- `env`: literal nonsecret values;
- `pass_env`: copy a globally/project-allowed daemon variable at execution and treat it
  as secret;
- `secret_env`: literal env names whose values require masking;
- proxy/account/plugin credentials: automatically secret and delivered only to the
  process/session needing them;
- agent/automation token: automatically secret.

`env_mode="inherit"` is global-owner opt-in for a trusted global profile and is shown
as a warning. Secret values shorter than the configured safe redaction minimum are
rejected for managed injection rather than pretending exact value redaction is
reliable. Agent tokens have a prefixed high-entropy format that is pattern-redacted
after restart.

Resolved secret values never enter config, Store, rule/action payload or logs. Named
references persist. Native output is masked; durable semantic text containing a known
secret is rejected.

## Accounts, capacity and holds

A profile references `[accounts.<id>]`. Atomic start reservation must fit profile,
shared account and proxy/service caps. Starting, live and unresolved-unknown start
incarnations retain reservations until exact terminal/readback; a crash cannot release
capacity merely by losing a process handle.

If account is omitted, profile becomes the scope and status warns that cross-profile
sharing is unknown. Project principals may use only allowed account/proxy references.

```sql
CREATE TABLE provider_holds (
  scope TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  coverage TEXT NOT NULL,
  retry_after INTEGER,
  probe_command TEXT,
  state TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
```

A hold blocks new provider-backed launch/send/goal continuation in its proven scope;
it never blocks stop/interrupt/request replies/readback or local checks. Unknown
coverage holds only the exact observed route/profile. After `retry_after`, the first
explicit queued/user send may atomically become the probe. Mail wake, parent notice,
timer or hidden model call is never selected as a probe. Probe clears hold only after
an exact successful provider-backed turn; unknown keeps the hold and probe command
visible. Auth/model-gone without recovery evidence requires an explicit owner/config
change rather than a timed probe. Restart restores all hold/probe state.

## Profiles and paths

One-level `extends`; project profile replaces the global profile as a whole after
project-authority validation. Unknown keys are errors except plugin-private global
keys validated at initialize. Names, labels, argv/env counts/values are bounded.

`setup`, `scratch`, scripts and system notes are repository-relative and reject
absolute paths, `..`, symlink escape and platform alternate-stream/device forms.
Different Rust worktrees use distinct `CARGO_TARGET_DIR`s.

## Plugins, services and webhooks

External plugin `cmd` is global-only. `instances` is shared-mode only; per-agent gets a
private instance/data directory per generation. Operator cannot override returned
custody mode. Shared control must prove per-session environment, identity, readback and
isolated stop.

`attach` service is owner-run and never stopped/reconfigured. `launch` service has
separate managed custody and persisted endpoint/process identity. Agent stop never
stops a shared service.

Webhook actions store a globally named endpoint, not URL/header/secret. Project
principal may use only its allowlist. The action id is sent through a configured
idempotency header when supported; otherwise a lost response remains unknown and is
never retried.

## Store/WAL limits

Daemon passive-checkpoints, records WAL/database size and tracks Eliot-owned readers.
Replays paginate/release transactions. Warning pressure is visible. At hard size reject
new long reads while preserving short control/result commits. Never delete evidence,
kill an unknown reader or blind-truncate with active readers.
