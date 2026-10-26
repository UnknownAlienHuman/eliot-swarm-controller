# Configuration

## Private data directory

Default `%LOCALAPPDATA%\Eliot` on Windows or `~/.local/share/eliot`; override with
`--data-dir` / `ELIOT_HOME`. Create current-user-only permissions/ACL and refuse
symlink/reparse-point roots or unsafe ownership. Logs are private, rotated and bounded.

```text
<data_dir>/
  config.toml
  owner.token
  routing.key
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.json, daemon.lock
  client-write-id.state, client-write-id.lock
  artifacts/<blake3-digest>/...
  logs/, rules/, plugins/, services/, worktrees/
```

`owner.token` is a replaceable high-entropy `eliot_ot_<base64url>` local attribution
credential. `eliot --owner` reads it explicitly. It is never stored in SQLite/logs and
never inherited by Agent/automation processes. If missing, a local operator may recreate
it atomically because principal identity remains `owner`; WriteId receipts still prevent
mutation replay. This token is not hostile same-user isolation.

`routing.key` is different: it deterministically re-derives current Agent routing
tokens. It is atomically created before the first database, never stored in SQLite/logs,
never symlink-followed and never silently regenerated for a nonempty data directory.

An existing Store with a missing/mismatched routing key enters `routing_key_error`:
currently presented tokens still verify against hashes and owner readback/stop/query
remains, but launch, resume, routing reattach and child-token issue block. Restore the
exact key or explicitly stop/abandon all routed generations before replacement.

`client-write-id.state` is monotonic ULID state only. CLI locks, re-reads, advances,
atomically replaces, unlocks and prints the id before sending. Missing state seeds from
daemon floor plus randomness.

`artifacts/` is current-user-only, content-addressed and immutable after publication. It
contains bounded nonsecret snapshots of rule/setup/check scripts, system notes and
other immutable implementation inputs. Temporary writes are fsynced/renamed atomically;
an existing digest is byte-compared before reuse. Store reachability controls pruning.

Project `.eliot/{config.toml,rules/,scripts/}` loads only from a globally registered
checkout, never an Agent worktree. Invalid reload retains the last valid revision.

## Configuration authority

Global owner config defines daemon/process/Store/Git/protocol limits, projects,
accounts, plugins, proxies, services, webhooks, executable routes and base profiles.

Project config may only narrow an allowed global profile through a same-name overlay.
Allowed overlay fields are:

- additional bounded labels;
- `max_live` no greater than the global profile value;
- `stall_after`;
- `notify_parent` / `wake_on_mail`;
- project-local immutable `system_note`, `setup` and relative `scratch` entries.

Project overlay cannot change or add harness, plugin, model, effort, permissions,
control, workspace mode, Git identity, args, executable, environment mode/values,
pass-env/secret-env, account, proxy, service, billing or endpoint. Owner defines another
global profile variant when one of those must differ. A project cannot create a profile
with no global base.

Router validates the fully resolved profile at admission and dispatch: profile name is
in `profiles_allow`, artifacts remain valid and the profile still references only the
project's allowed owner-defined account/proxy/webhook routes.

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
limits.max_artifact_bytes = 1048576
limits.inline_input_bytes = 524288
limits.event_data_bytes = 65536
limits.final_text_bytes = 65536
protocol.plugin_frame_bytes = 1048576
protocol.plugin_page_items = 1000
protocol.plugin_page_bytes = 921600
protocol.start_env_bytes = 131072
protocol.start_metadata_bytes = 131072
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
plugin = "codex"
account = "codex-main"
max_live = 4
model = "<native model id>"
effort = "high"
permissions = "workspace-write"
env_mode = "minimal"
pass_env = ["CODEX_HOME"]
workspace_mode = "shared"
control = "self"
labels = { role = "writer" }

[profiles.codex-opencodex]
extends = "codex"
proxy = "opencodex"

[profiles.manager]
harness = "claude"
plugin = "claude"
account = "claude-main"
permissions = "workspace-write"
pass_env = ["CLAUDE_CONFIG_DIR"]
workspace_mode = "worktree"
control = "project"
labels = { role = "manager" }
```

Example project overlay:

```toml
[profiles.codex]
max_live = 2
stall_after = "10m"
labels = { component = "api" }
system_note = ".eliot/notes/api.md"
setup = [[".eliot/scripts/setup-api.ps1"]]
scratch = ["target/", ".cache/"]
```

`max_selector_targets` is a transaction guard, not a logical fleet limit. Expansion
over it rejects before any row; caller narrows or batches. Plugin discovery/snapshot
pages obey both item and encoded-byte limits.

## Effective profile and binding revisions

Owner profile plus validated project overlay resolves one bounded canonical nonsecret
binding payload:

```text
AgentBindingPayload {
  mode: launch | resume | control_attach | observe_attach | detach_observe,
  profile_name, profile_revision,
  harness, plugin_name,
  model?, effort?, permissions,
  args, env_mode, literal_env, pass_env_names, secret_env_names,
  workspace_mode, control, git_identity,
  system_note_artifact?, setup_artifacts[], scratch,
  labels, notify_parent, wake_on_mail,
  account_scope?, proxy_scope?, service_scope?,
  requested_endpoint?, billing?, source_config_files
}
```

The payload is stored as an immutable `(agent,generation,binding_revision)` row. A
separate current pointer identifies the effective revision for that native incarnation.
Bindings are not overwritten when control changes.

- launch and resume create revision 1 for their target generation before dispatch;
- first observe attach creates synthetic `observe:<harness>` revision 1;
- control attach creates candidate `current+1`, superseding the current observe/detach
  revision; current pointer moves only after exact native attach/environment/readback;
- failed/unknown attach keeps the old pointer and immutable candidate linked to its
  command;
- detach success creates a new `detach_observe` revision and moves the pointer;
- control reattach creates another control revision; exact recovery of an already
  controlled session reuses the current revision.

`Agent.profile` is a projection of the current pointer. The start/attach/detach command
stores the exact candidate/current revision, so client disconnect or later config edits
cannot change the native effect being read back.

Observe attach has no credentials, account, routing or native mutation beyond read-only
attach. Control attach requires a real allowed global profile, capacity reservation and
exact native compatibility. Plugin never rewrites an existing session to pretend it
matches the requested route/workspace/permissions.

At revision creation, every project note/setup/check script is resolved from its allowed
root, read without following untrusted links, bounded, hashed and copied to immutable
artifact storage. `profile_revision` includes canonical effective profile plus artifact
digests and named references, never resolved secrets. A later checkout edit cannot
change the candidate/revision.

Dispatch revalidates prohibitions without substitution: project allowlist/overlay,
owner-only inherit, plugin/service capabilities, artifact presence/digest, safe paths,
capacity, provider hold/probe and routing-key availability where direct routing is
required. Secrets resolve immediately before native effect. Resume gets a new
generation/revision sequence; recovery attach reuses current revision.

## Frozen workspace binding

```text
WorkspaceBinding {
  project, repo_path, requested_branch,
  base_ref?, base_oid,
  worktree_path,
  setup_artifacts[], setup_revision
}
```

Daemon resolves base ref read-only to exact local OID before admission; no implicit
fetch. Ref is diagnostic, OID authoritative. Missing/ambiguous rejects; later ref
movement cannot alter queued creation. Existing branch/path must match. Setup revision
digests frozen argv, immutable artifacts and named env refs. Setup executes artifacts,
never mutable checkout files.

## Environment and secrets

Default baseline is minimal: PATH, home/profile, system root, temp, locale and required
certificate variables. Additional values are explicit literal nonsecret env, named
pass-env/secret-env, proxy/account/plugin credentials and current scoped token.

Global scripts use only global script allowlist; project scripts use global∩project.
Validation occurs before value lookup/start; returned actions cannot add env refs.

`env_mode=inherit` is an owner-defined trusted global profile and visibly warned. A
project overlay cannot enable it. Too-short unmaskable secret rejects managed injection.
Tokens are prefixed/high entropy. Persisted source spools are masked with the
binding revision's frozen secret-name/value set; raw vendor detail stays private.
Resolved secrets never enter config, Store, rule/action/artifact/log. Durable semantic
text containing a known secret rejects.

## Capacity reservations and provider holds

A control binding revision names profile/account/proxy-service scopes. Start or
control-attach transaction checks all limits and inserts reservations atomically.

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

Queue selects the oldest command whose complete scope set is available; unrelated
scopes are not head-of-line blocked. Existing reservations count after limit lowering.
Starting/live/unknown start retains reservation. Exact terminal/no-session failure
releases. An observe row has no control reservation. A control-attach candidate reserves
capacity before native I/O; definite failure releases, unknown retains until readback.
Exact attach success moves binding pointer and converts reservation to live. If capacity
is unavailable, no candidate/effect is admitted. Detach of a still-live provider
session does not pretend account capacity was freed until terminal observation.

A hold blocks provider-backed launch/send/Goal in proven scope, not stop/interrupt,
request reply, readback or local checks. Unknown coverage holds exact route/profile.
After retry boundary, only explicit owner/current directly routed Agent `probe=true`
send or linked launch/resume input may claim one probe. Automation/wake/hidden calls
cannot.

Probe state:

- exact successful provider-backed turn clears hold and claim;
- new provider limitation replaces/extends hold from exact response;
- unknown after possible provider I/O keeps hold/probe reserved;
- exact failure before any provider request or owner cancellation before dispatch
  releases the probe claim but keeps hold;
- time alone never releases in-flight/unknown probe.

For launch/resume only linked input is probe. Auth/model-gone requires explicit config/
recovery evidence rather than timed probe.

## Profiles, setup and paths

One-level `extends` exists only among global owner profiles. Project overlay applies
after global resolution and can only narrow the listed fields. Unknown keys are errors
except plugin-private global keys. Values/counts are bounded.

Project setup source is repository-relative `.eliot/scripts/`; global owner may name
configured executables. Script bytes become immutable artifacts at admission. Setup runs
with a project read-only automation token, minimal environment and global∩project script
allowlist. Setup/scratch/scripts/notes reject absolute, `..`, symlink escape,
alternate-stream/device forms. Rust worktrees use separate target dirs.

Interpreters/executables resolve to full configured paths. A queued binding revision
stores executable identity/path; incompatible replacement before dispatch fails rather
than selecting another executable.

## Plugins, sources, services and webhooks

External plugin command is global-only. `instances` is shared-only; per-Agent gets a
private directory. Custody mode cannot be overridden. Shared control proves per-session
environment, identity, readback and isolated stop.

Source spool limits are per source key. Plugin backpressures or commits gap; never
silently drops lifecycle or grows unbounded. `event_id` source receipts live separately
from event rows; `strict_cursor` requires proven vendor cursor semantics; `none` gaps on
reconnect.

Attached service is owner-run/never stopped. Managed service has separate custody.
Agent stop never stops a shared service.

Webhook stores a named endpoint only. Project must allow it. Action id uses configured
idempotency header when available; otherwise lost response stays unknown/no retry.

## Retention, artifacts, idempotency and WAL limits

`retention.idempotency_days` is minimum eligibility age. Receipt lookup precedes floor
checks. A terminal receipt may be deleted only when older than the window, no retained
domain row references it, no open/unknown linked effect needs it and final result is
outside supported readback. Rows prune in dependency order; floor advances only through
receipts actually deleted.

A native-event receipt in `event_id` mode outlives its event row. It may be removed only
after generation supersession/deletion, all source epochs/spools capable of replaying
that generation are closed and no retained readback depends on it. `strict_cursor`
watermarks remain until the same boundary. `none` retains the recorded gap boundary.

An artifact may be removed only after no Agent binding revision/workspace/rule
activation/run/action/check references its digest and no running process has it open.
Binding revisions referenced by current pointers or retained commands/receipts remain;
unreferenced failed candidates may prune only after their command history becomes
prunable. Artifacts are implementation inputs, not an audit-document hierarchy.

Other history may prune only after compact projections/summaries preserve readback.
Unread mail, open batches, unresolved input/turn/action, held capacity, current source
watermark, dirty edit attribution and `orphan_possible` custody never age-prune.

Daemon passive-checkpoints, records WAL/database size and tracks readers. Replays
paginate/release transactions. At hard size, reject new work/long reads while reserving
short stop/interrupt/request/result commits. Real disk/SQLite failure enters the Store
error boundary. Never delete evidence, kill an unknown reader or blind-truncate active
readers.
