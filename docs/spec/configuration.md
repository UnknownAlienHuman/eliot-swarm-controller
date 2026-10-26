# Configuration

## Private data directory

Default `%LOCALAPPDATA%\Eliot` on Windows or `~/.local/share/eliot`; override with
`--data-dir` / `ELIOT_HOME`. Create current-user-only permissions/ACL and refuse
symlink/reparse-point roots or unsafe ownership. Logs are private, rotated and bounded.

```text
<data_dir>/
  config.toml
  routing.key
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.json, daemon.lock
  client-write-id.state, client-write-id.lock
  logs/, rules/, plugins/, services/, worktrees/
```

`routing.key` is atomically created before first database, never stored in SQLite/logs,
and used only to re-derive current routing tokens. SQLite stores nonsecret key identity
plus token hashes. File is current-user-only, never symlink-followed and never silently
regenerated for nonempty data dir.

Existing Store with missing/mismatched key enters `routing_key_error`: existing presented
tokens still verify against hashes and owner readback/stop/query remains, but launch,
resume, control reattach and child-token issuance block. Restore exact key or explicitly
stop/abandon all controlled generations before replacement/revocation.

`client-write-id.state` is monotonic ULID state only. CLI locks, re-reads, advances,
atomically replaces, unlocks and prints id before sending. Missing state seeds from
daemon floor plus randomness.

Project `.eliot/{config.toml,rules/,scripts/}` loads only from globally registered
checkout, never agent worktree. Invalid reload retains last valid revision.

## Configuration authority

Global config owns daemon/process/Store/Git limits, projects, accounts, plugins,
proxies, services, webhooks, base profiles and global automation env allowlist.

Project config may define/replace allowed profile names, narrow permissions/control/
labels/workspace/model/effort/limits, select allowed account/proxy/profile refs, add
nonsecret env plus allowed `pass_env`, and set project-local setup/scratch/note paths.

Project config cannot define plugin executable/instances/private config, accounts,
proxies, services, webhooks, `env_mode=inherit`, arbitrary `pass_env`, daemon limits or
another project path. Project rules/scripts keep project principal.

A project principal may launch only an effective profile in `profiles_allow`; every
`extends` base is also allowed. Router validates fully resolved profile at admission and
dispatch: effective inherit is owner-only, every effective pass-env name/project
account/proxy is allowed. An alias cannot smuggle another project's credentials.

## Example

```toml
[daemon]
retention.events_days = 30
retention.turns_days = 90
retention.commands_days = 30
retention.mail_days = 90
retention.process_days = 14
retention.automation_history_days = 30
retention.idempotency_days = 90
store.write_id_future_skew = "10m"
store.wal_warn_bytes = 536870912
store.wal_hard_bytes = 2147483648
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
limits.max_selector_targets = 10000
automation.max_runs_per_rule_per_minute = 10
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2
automation.check_timeout = "10m"
automation.max_event_backlog = 100000
automation.script_pass_env_allow = []
hooks.response_margin = "100ms"
sources.spool_max_events = 4096
sources.spool_max_bytes = 16777216

[process]
scan_interval = "2s"
terminal_drain_timeout = "3s"
linux_exact_custody = "auto"

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
max_live = 4
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

`max_selector_targets` is transaction guard, not logical fleet limit. Expansion above
it rejects before any row; caller narrows or batches. No partial fanout.

## Effective profile and launch binding

Profile inheritance/project override resolves one bounded canonical nonsecret binding:

```text
LaunchBinding {
  profile_name, profile_revision,
  harness, plugin_name,
  model?, effort?, permissions,
  args, env_mode, literal_env, pass_env_names, secret_env_names,
  workspace_mode, control, git_identity, system_note?, labels, setup, scratch,
  account_scope?, proxy_scope?, service_scope?,
  requested_endpoint?, billing?, source_config_files
}
```

Revision is BLAKE3-256 over effective profile/named refs, not resolved secrets. Binding
is immutable. Queued work cannot adopt config changes.

Dispatch revalidates prohibitions without substitution: project allowlists/pass-env,
owner-only inherit, plugin/service capabilities, paths, capacity, provider hold/probe,
and routing-key availability. Revoked binding fails before native I/O. Positive defaults
do not rewrite it. Secrets resolve once immediately before start. Resume gets new
binding; exact recovery attach reuses current generation binding.

## Frozen workspace binding

```text
WorkspaceBinding {
  project, repo_path, requested_branch,
  base_ref?, base_oid,
  worktree_path, setup_revision
}
```

Daemon resolves base ref read-only to exact local OID before admission; no implicit
fetch. Ref is diagnostic, OID authoritative. Missing/ambiguous rejects; later ref
movement cannot alter queued creation. Existing branch/path must match. Setup revision
digests frozen argv/scripts/named env refs.

## Environment and secrets

Default minimal baseline: PATH, home/profile, system root, temp, locale, required cert
vars. Additional values are explicit literal nonsecret env, allowed pass-env, secret-env
names, named proxy/account/plugin credentials and current scoped token.

Global scripts use only global script allowlist; project scripts use global∩project.
Validation occurs before value lookup/start; returned actions cannot add env refs.

Inherit is owner-only trusted profile and visibly warned. Too-short unmaskable secret
rejects managed injection. Tokens are prefixed/high-entropy. Persisted source spools are
masked with generation's frozen secret set; raw vendor detail stays private vendor
store. Resolved secrets never enter config/Store/rule/action/log. Durable semantic text
with known secret rejects.

## Capacity reservations and provider holds

Frozen binding names profile/account/proxy-service scopes. Start dispatch transaction
checks all limits and inserts reservations atomically.

```sql
CREATE TABLE capacity_reservations (
  scope TEXT NOT NULL,
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  command_id TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  released_at INTEGER,
  PRIMARY KEY(scope,agent,generation)
);
CREATE INDEX capacity_held ON capacity_reservations(scope,state);

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

Queue selects oldest command whose all scopes available; unrelated scope is not
head-of-line blocked. Existing reservations count after limit lowering. Starting/live/
unknown start retains reservation. Exact terminal/no-session failure releases. Detach
of still-live independent session does not free until observed terminal.

Hold blocks provider-backed launch/send/Goal in proven scope, not stop/interrupt/request
reply/readback/local checks. Unknown coverage holds exact route/profile. After boundary,
only explicit owner/current-agent `probe=true` send or linked launch/resume input can
claim one probe. Automation/wake/hidden calls cannot. Exact successful provider turn
clears; unknown keeps. Auth/model-gone requires explicit config change absent proof.

## Profiles, setup and paths

One-level extends; project replacement after validation. Unknown keys error except
plugin-private global keys. Values/counts bounded.

Project setup is versioned repository-relative `.eliot/scripts/`; global owner may name
configured executables. Setup gets project read-only automation token, minimal env and
global∩project allowlist. Setup/scratch/scripts/notes reject absolute, `..`, symlink
escape, alternate-stream/device forms. Rust worktrees use separate target dirs.

## Plugins, sources, services and webhooks

External plugin command is global-only. Instances is shared-only; per-agent gets private
dir. Custody mode cannot be overridden. Shared control proves per-session environment,
identity, readback and isolated stop.

Source spool limits are per key. Plugin backpressures or commits gap; never silently
drops lifecycle or grows unbounded.

Attached service is owner-run/never stopped. Managed service has separate custody.
Agent stop never stops shared service.

Webhook stores named endpoint only. Project must allow it. Action id uses configured
idempotency header when available; otherwise lost response stays unknown/no retry.

## Retention, idempotency and WAL limits

`retention.idempotency_days` is a **minimum eligibility age**, not a forced deletion
age. Receipt lookup precedes floor checks, so an existing receipt remains readable below
current floor.

A terminal receipt may be deleted only when:

- it is older than idempotency window;
- no retained domain row references it (including `commands.receipt_id`);
- no open/unknown linked effect needs its result;
- its bounded final result is no longer required by a supported readback window.

Thus command history configured longer than receipt window automatically retains its
referenced receipt longer. SQLite restrictive FK is intentional. Receipt/domain rows
are pruned in dependency order, and floor advances only through receipts actually
deleted in same transaction. Open/unknown receipts never age-prune.

Other domain history may prune only after compact projections/summaries preserve
required readback. Unread mail, open batches, unresolved input/turn/action, held
capacity, current source watermark, dirty edit attribution and `orphan_possible` custody
never age-prune.

Daemon passive-checkpoints, records WAL/database size and tracks Eliot readers. Replays
paginate/release transactions. At hard size, reject new work/long reads while reserving
short stop/interrupt/request/result commits. Real disk/SQLite failure enters Store error
boundary. Never delete evidence, kill unknown reader or blind-truncate active readers.
