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

`routing.key` is generated atomically before the first database creation, never written
to SQLite/logs and used only to re-derive current-version routing tokens. SQLite stores
its nonsecret key identity plus token hashes. The file has current-user-only permissions
and is neither followed through a symlink nor silently regenerated for a nonempty data
directory.

If an existing Store has a missing/mismatched key, daemon enters `routing_key_error`:
existing raw agent tokens can still be verified against stored hashes, and owner
readback/stop/query remains available, but launch/resume/control-reattach/child-token
issuance is blocked. Restoring the exact key clears the condition. Replacing it requires
an explicit owner recovery that first stops or abandons every controlled live
generation and revokes their token hashes; Eliot never rotates them silently.

`client-write-id.state` contains only monotonic ULID generator state. CLI takes the
adjacent lock, re-reads state, advances it, atomically replaces the file, releases the
lock and prints the id before sending. Missing state is seeded from daemon floor plus
fresh randomness; it is not a credential.

Project `.eliot/{config.toml,rules/,scripts/}` is loaded only from a globally registered
checkout, never an agent worktree. Agent edits take effect only after reaching that
checkout. Invalid reload keeps the last valid revision and reports health.

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
`[projects.<name>].profiles_allow`. Router checks this at admission and dispatch. Owner
may launch any global profile. Thus project control cannot select a profile carrying
another project's account, inherited environment, plugin command or proxy credential.

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

`profile_revision` is a BLAKE3-256 digest of canonical effective profile and named
references, never resolved secrets. Binding is stored with launch/resume and immutable.
Queued work cannot silently adopt a new model/proxy/account/permission/environment name
or setup after reload.

At dispatch daemon revalidates current prohibitions without substituting values:

- project profile/account/proxy allowlists still permit frozen binding;
- plugin/service exists and declares required mode/capability;
- referenced paths remain safe;
- capacity policy admits reservations;
- provider hold permits operation or exact linked input is explicit probe;
- routing key is available when a new/reissued token is required.

A revoked binding fails before native I/O. Changed positive defaults do not rewrite it;
caller cancels and submits a new command. Secret references resolve once immediately
before start from frozen names and are never stored. Requested/effective facts are
compared after start.

Resume creates a new generation and binding from Agent profile (or explicit allowed
owner profile). Exact recovery attach to same controlled generation reuses its binding.

## Frozen workspace binding

A worktree request is separate from profile and is frozen at admission:

```text
WorkspaceBinding {
  project, repo_path, requested_branch,
  base_ref?, base_oid,
  worktree_path, setup_revision
}
```

The daemon resolves `base_ref` with read-only local Git to one exact commit OID before
admission; no implicit fetch occurs. `base_ref` is diagnostic, while `base_oid` is the
authoritative creation/readback target. A missing/ambiguous ref rejects. Later movement
of the branch/remote ref cannot change queued workspace creation. Existing branch/path
must match the frozen OID or fail visibly. Setup revision is a digest of frozen argv/
script identities and named environment references.

## Environment and secrets

Default `env_mode="minimal"`; do not inherit daemon's full environment. Baseline has
only OS spawn essentials: PATH, home/profile, system root, temp, locale and required
certificate variables. Additional values are explicit:

- `env`: literal nonsecret values;
- profile `pass_env`: globally/project-allowed daemon value resolved at launch and
  treated as secret;
- script environment references: resolved only after principal validation;
- `secret_env`: literal environment names requiring masking;
- proxy/account/plugin credentials: automatically secret and delivered only where
  needed;
- agent/automation token: automatically secret.

Script rules are stricter:

- global scripts may reference only `daemon.automation.script_pass_env_allow`;
- project scripts require intersection of that list and project `pass_env_allow`;
- validation occurs before reading daemon environment or starting process;
- returned script actions cannot request another environment reference.

`env_mode="inherit"` is global-owner opt-in for trusted global profile and is shown as
a warning. Secret values shorter than safe redaction minimum are rejected for managed
injection rather than pretending masking is reliable. Agent tokens use a prefixed,
high-entropy pattern that remains maskable after restart.

Normalized persisted source-spool envelopes are masked with the generation's frozen
known-secret set before durable spooling. Eliot does not copy raw vendor streams into
its spool; full raw detail stays in private vendor store. Resolved secret values never
enter config, Store, rule/action payload or logs. Native output is masked; semantic
durable text containing known secret rejects.

## Capacity reservations and provider holds

A frozen binding names capacity scopes: `profile:<name>`, optional `account:<id>` and
`proxy|service:<id>`. In the same transaction that dispatches start and moves Agent to
target generation, scheduler checks current limits and inserts a reservation per scope.
All checks/inserts are atomic.

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

Queue selects oldest command whose **all** frozen scopes are available; blocked account
does not head-of-line-block unrelated scopes. Existing reservations count after limit
lowering. Config edits affect future dispatch, never move existing generation.

Starting/live and unresolved start/session outcomes retain reservations. Exact terminal
or definite no-session start failure releases. Detach of independently live session
does not free reservation merely by surrendering control; exact observed terminal does.

Provider hold blocks provider-backed launch, send and Goal continuation in proven scope,
not stop/interrupt/request reply/readback/local checks. Unknown coverage holds exact
route/profile only.

After `retry_after`, recovery requires explicit `probe=true` on:

- send to exact generation; or
- launch/resume with linked prompt, where linked send—not session creation—is probe.

Router atomically claims `probe_command`; one probe exists. Owner/current controlled
agent may request. Rules, timers, Goal, mail/parent wake and hidden model calls cannot.
Hold clears only after exact successful provider turn. Unknown keeps hold/probe. Auth/
model-gone requires explicit config change absent recovery evidence.

## Profiles, setup and paths

One-level `extends`; project profile replaces global profile after project validation.
Unknown keys error except plugin-private global keys validated at initialize. Names,
labels, argv/environment counts and values are bounded.

Project setup executables are repository-relative versioned files under
`.eliot/scripts/`; global owner profiles may additionally name configured resolved
executables. Setup runs with project principal's scoped read-only automation token,
minimal environment and global∩project allowlist. It never gets owner by omission.

`setup`, `scratch`, scripts and system notes are repository-relative and reject absolute
paths, `..`, symlink escape and alternate-stream/device forms. Different Rust worktrees
use distinct `CARGO_TARGET_DIR`s.

## Plugins, sources, services and webhooks

External plugin `cmd` is global-only. `instances` is shared-only; per-agent gets a
private instance/data directory per generation. Operator cannot override custody mode.
Shared control proves per-session environment, identity, readback and isolated stop.

Source spool limits apply per authoritative source key. Plugin backpressures or commits
explicit gap at cap; it never drops lifecycle silently or expands without bound.

Attached service is owner-run and never stopped/reconfigured. Managed service has
separate custody and persisted endpoint/process identity. Agent stop never stops shared
service.

Webhook action stores named endpoint, not URL/header/secret. Project principal may use
only allowlist. Action id is sent through configured idempotency header when supported;
otherwise lost response remains unknown and is never retried.

## Retention, idempotency and WAL limits

`retention.idempotency_days` is at least largest automatic retry/support window and at
least 30 days. Receipt lookup precedes floor checks, so existing open/final receipt is
readable even below current floor; only absent id at/below floor is expired.

Terminal receipts expire by `admitted_at`; pruning advances floor through their ULID
timestamps transactionally. Open/unknown receipts never age-prune. Domain history may
use other retention only after compact projections/receipt summaries preserve readback.
Unread mail, open batches, unresolved input/turn/action, held capacity, current source
watermark, dirty edit attribution and `orphan_possible` custody never age-prune.

Daemon passive-checkpoints, records WAL/database size and tracks Eliot-owned readers.
Replays paginate/release transactions. At hard size, reject new work and long reads
while reserving short commits for stop/interrupt/request replies/results. Real disk/
SQLite failure enters Store error boundary; configured limit is not claimed to defeat
full disk. Never delete evidence, kill unknown reader or blind-truncate active readers.
