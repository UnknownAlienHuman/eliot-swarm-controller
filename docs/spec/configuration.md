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

`routing.key` is generated atomically before first database creation, never written to
SQLite/logs and used only to re-derive current-version routing tokens. SQLite stores its
nonsecret key identity plus token hashes. The file is current-user-only, never followed
through a symlink and never silently regenerated for a nonempty data directory.

If an existing Store has missing/mismatched key, daemon enters `routing_key_error`:
existing presented tokens can still verify against hashes and owner readback/stop/query
remains, but launch/resume/control-reattach/child-token issuance is blocked. Restoring
the exact key clears it. Replacement is explicit owner recovery after stopping or
abandoning all controlled live generations and revoking hashes.

`client-write-id.state` contains monotonic ULID generator state only. CLI takes adjacent
lock, re-reads, advances, atomically replaces, releases and prints id before sending.
Missing state seeds from daemon floor plus fresh randomness; it is not a credential.

Project `.eliot/{config.toml,rules/,scripts/}` is loaded only from a globally registered
checkout, never an agent worktree. Edits apply only after reaching that checkout.
Invalid reload keeps last valid revision and reports health.

## Configuration authority

Global config owns daemon/process/Store/Git limits, projects, accounts, plugins,
proxies, services, webhooks, base profiles and global automation environment allowlist.

Project config may only:

- define/replace profiles whose names are in that project's `profiles_allow`;
- narrow permissions, control, labels, workspace mode, model/effort and limits;
- select only allowed global account/proxy/profile references;
- add nonsecret literal environment values and allowed `pass_env` names;
- set project-local setup/scratch/system-note paths.

Project config cannot define plugin executable/instances/private config, accounts,
proxies, services, webhooks, `env_mode=inherit`, arbitrary `pass_env`, daemon limits or
another project path. Project rules/scripts execute under that project principal.

A project agent/rule may launch only an effective profile in
`[projects.<name>].profiles_allow`. Every `extends` base must also be allowed. Router
validates the fully resolved profile at admission and dispatch: effective
`env_mode=inherit` is always owner-only, every effective `pass_env` name must be in the
project allowlist, and account/proxy references must be allowed. The owner may launch
any global profile. This prevents an allowlisted alias from smuggling another project's
credential/environment through inheritance.

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

`max_selector_targets` is an accident/transaction guard, not a logical agent limit.
Selector expansion above it fails before creating parent/child rows; callers narrow the
selector or submit explicit batches. No partial fanout is admitted.

## Effective profile and launch binding

Profile inheritance/project override resolves into one bounded canonical nonsecret
`LaunchBinding` at launch/resume admission:

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

`profile_revision` is BLAKE3-256 of canonical effective profile and named references,
never resolved secrets. Binding is stored with launch/resume and immutable. Queued work
cannot adopt new model/proxy/account/permission/environment/setup after reload.

At dispatch daemon revalidates current prohibitions without substituting values:

- project profile/base/account/proxy and every effective `pass_env` remain allowed;
- `env_mode=inherit` is not used by a project principal;
- plugin/service exists and declares required mode/capability;
- referenced paths remain safe;
- capacity policy admits reservations;
- provider hold permits operation or exact linked input is explicit probe;
- routing key is available when new/reissued token is required.

A revoked binding fails before native I/O. Positive default changes do not rewrite it;
caller cancels/submits new command. Secret references resolve immediately before start
from frozen names and are never stored. Requested/effective facts are compared after
start.

Resume creates new generation/binding from Agent profile (or explicit allowed owner
profile). Exact recovery attach to same controlled generation reuses existing binding.

## Frozen workspace binding

A worktree request is separate from profile and frozen at admission:

```text
WorkspaceBinding {
  project, repo_path, requested_branch,
  base_ref?, base_oid,
  worktree_path, setup_revision
}
```

Daemon resolves `base_ref` with read-only local Git to exact commit OID before
admission; no implicit fetch. `base_ref` is diagnostic, `base_oid` authoritative. A
missing/ambiguous ref rejects. Later ref movement cannot change queued creation.
Existing branch/path must match frozen OID or fail. Setup revision digests frozen argv/
script identities and named environment references.

## Environment and secrets

Default `env_mode="minimal"`; do not inherit daemon's full environment. Baseline has
only OS spawn essentials: PATH, home/profile, system root, temp, locale and required
certificate variables. Additional values are explicit:

- `env`: literal nonsecret values;
- profile `pass_env`: globally/project-allowed daemon value resolved at launch and
  treated as secret;
- script environment references: resolved only after principal validation;
- `secret_env`: literal names requiring masking;
- proxy/account/plugin credentials: automatically secret and delivered only where
  needed;
- agent/automation token: automatically secret.

Script rules are stricter:

- global scripts may reference only `daemon.automation.script_pass_env_allow`;
- project scripts require intersection with project `pass_env_allow`;
- validation occurs before reading daemon environment or starting process;
- returned script actions cannot request another environment reference.

`env_mode="inherit"` is global-owner opt-in for trusted global profile and shown as
warning. Secret values shorter than safe redaction minimum reject managed injection
rather than pretending masking is reliable. Agent tokens use prefixed high-entropy
pattern maskable after restart.

Persisted source-spool envelopes are masked with generation's frozen known-secret set
before spooling. Eliot does not copy raw vendor streams; full raw detail stays in
private vendor store. Resolved secrets never enter config, Store, rule/action payload
or logs. Native output is masked; semantic durable text containing known secret rejects.

## Capacity reservations and provider holds

Frozen binding names capacity scopes: `profile:<name>`, optional `account:<id>` and
`proxy|service:<id>`. In transaction dispatching start/moving generation, scheduler
checks limits and inserts reservation per scope atomically.

```sql
CREATE TABLE capacity_reservations (
  scope TEXT NOT NULL,
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  command_id TEXT NOT NULL,
  state TEXT NOT NULL,                 -- held|released
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

Queue selects oldest command whose all frozen scopes are available; blocked account does
not block unrelated scopes. Existing reservations count after limit lowering. Config
edits affect future dispatch, never move existing generation.

Starting/live and unresolved start/session outcomes retain reservations. Exact terminal
or definite no-session failure releases. Detach of independently live session does not
free capacity until observed terminal.

Provider hold blocks provider-backed launch, send and Goal continuation in proven scope,
not stop/interrupt/request reply/readback/local checks. Unknown coverage holds exact
route/profile only.

After `retry_after`, recovery requires explicit `probe=true` on exact send or
launch/resume with linked prompt (linked send is probe). Router atomically claims one
probe. Owner/current controlled agent may request; rules, timers, Goal, mail/parent wake
and hidden calls cannot. Hold clears only after exact successful provider turn. Unknown
keeps hold/probe. Auth/model-gone requires explicit config change absent recovery proof.

## Profiles, setup and paths

One-level `extends`; project profile replaces global after project validation. Unknown
keys error except plugin-private global keys. Names, labels, argv/environment counts and
values are bounded.

Project setup executables are repository-relative versioned `.eliot/scripts/` files;
global owner profiles may name configured resolved executables. Setup runs with project
principal's read-only automation token, minimal environment and global∩project allowlist.

`setup`, `scratch`, scripts and system notes are repository-relative and reject absolute
paths, `..`, symlink escape and alternate-stream/device forms. Rust worktrees use
distinct `CARGO_TARGET_DIR`s.

## Plugins, sources, services and webhooks

External plugin `cmd` is global-only. `instances` is shared-only; per-agent gets private
instance/data directory per generation. Operator cannot override custody mode. Shared
control proves per-session environment, identity, readback and isolated stop.

Source spool limits apply per source key. Plugin backpressures or commits explicit gap
at cap; never drops lifecycle silently or expands unboundedly.

Attached service is owner-run and never stopped/reconfigured. Managed service has
separate custody and persisted endpoint/process identity. Agent stop never stops shared
service.

Webhook action stores named endpoint, not URL/header/secret. Project principal may use
only allowlist. Action id is sent through configured idempotency header when supported;
otherwise lost response remains unknown and is never retried.

## Retention, idempotency and WAL limits

`retention.idempotency_days` is at least largest automatic retry/support window and at
least 30 days. Receipt lookup precedes floor checks, so existing receipt remains
readable below floor; only absent id at/below floor expires.

Terminal receipts expire by `admitted_at`; pruning advances floor transactionally.
Open/unknown receipts never age-prune. Domain history may use other retention only after
compact projections/receipt summaries preserve readback. Unread mail, open batches,
unresolved input/turn/action, held capacity, current source watermark, dirty edit
attribution and `orphan_possible` custody never age-prune.

Daemon passive-checkpoints, records WAL/database size and tracks Eliot-owned readers.
Replays paginate/release transactions. At hard size, reject new work/long reads while
reserving short commits for stop/interrupt/request replies/results. Real disk/SQLite
failure enters Store error boundary. Never delete evidence, kill unknown reader or
blind-truncate active readers.
