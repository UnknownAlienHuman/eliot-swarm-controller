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
  daemon.lock, daemon.json, logs/daemon.log
  plugins/                 only instantiated plugins
  worktrees/               L3 opt-in workspaces
  services/                managed services, when needed
  rules/                   L4
  scripts/                 global script Run entries
  frozen/                  bounded admitted entry-file copies, when needed
```

The ordinary CLI reads `owner.token` automatically, only outside managed Agent/rule
environments. A missing managed token rejects, never becomes owner. This is attribution
within one OS user, not hostile-same-user isolation. Tokens never appear in semantic
payloads, recordings or logs.

After acquiring the data-directory lock, daemon startup calls
`logging::init(&DataDir,&DaemonLock)->Result<DaemonLog>` and owns the returned guard for
the run. `logs/daemon.log` and its rotation remain private to the data directory and
bounded. `logging::record(&SecretPolicy,code:&str,ids:&[(&str,&str)],error:Option<&Error>)->()`
writes only bounded codes/IDs, typed nonnegative numeric counts for daemon.retention_pruned under counts, and structured sanitized plugin/harness stderr tails,
applying the same policy to code and ID values. It omits `Error.message`, request
parameters and environment values. A record failure is diagnostic loss only; it cannot
change command settlement, source acknowledgement or process cleanup.

## Native profiles (all five adapters)

The current product has five native adapters: Codex, Claude, OpenCode, Muse
and Antigravity. Each profile selects one `harness` explicitly and an exact
`model`. Only Codex may omit `cmd`, in which case it launches `codex` by name;
the other four adapters require an explicit native executable in `cmd` in a
stored profile. There is no default profile, no builtin fake profile, and no
automatic harness/model/route/account fallback. Profiles stay the durable
explicit record: ad-hoc Owner launch (`swarm launch --harness ...`) freezes an
equivalent snapshot from built-in per-harness safe defaults plus flags (see
below). The harness pages
([Codex](../harnesses/codex.md), [Claude](../harnesses/claude-code.md),
[OpenCode](../harnesses/opencode.md), [Muse](../harnesses/muse.md),
[Antigravity](../harnesses/antigravity.md)) own their native mappings.
[The example config](../../config/controller.example.toml) shows explicit
Codex, Muse and Claude profiles; OpenCode and Antigravity follow the same
explicit-profile shape with their own harness-specific fields.

## First profile

```toml
[profiles.codex-luna]
harness = "codex"
model = "gpt-6-luna"
permissions = "workspace-write"
pass_env = ["CODEX_HOME"]
```

## Ad-hoc Owner launch (no stored profile)

`swarm launch --harness <codex|claude|muse|opencode|antigravity>
[--model M] [--effort E] --workspace W "prompt"` (and the same in
`command.submit` `launch` args) is an Owner-only tool that builds and freezes
a profile-equivalent snapshot exactly like a stored profile: built-in safe
defaults for that harness plus the supplied flags, reproducible and
replay-safe. Requested `model`/`effort` may be omitted; the freeze records the
requested value as absent separately from the effective value the harness
resolves at start (Codex `model/list` default entry and its default effort,
Muse `model/list` default, Claude CLI default with `--model`/`--effort`
omitted and effective read back, OpenCode configured default model), never a
silent substitution: an unavailable requested model fails honestly. Authority
fields default to the safe posture (Codex `workspace-write` + `never`,
Claude CLI-default permission mode, Muse and OpenCode current conservative
defaults); broader authority stays profile-only, owner-written choice. When
`cmd` is not given, the daemon resolves the installed native executable
deterministically and freezes the absolute path: Claude `claude.exe` on `PATH`
or the standard user install location (refusing `.cmd`/`.bat`/`.ps1`
wrappers), OpenCode by following the npm shim to the native `opencode.exe`,
Muse via the launcher dir version the launcher marks current, Codex via the
existing `codex` resolution, Antigravity via the installed app path (or
`unsupported` when the harness needs a different binary). A missing or
ambiguous executable is a clear `unsupported`/`profile_error` naming what was
searched. Agent principals keep the profile and `child_profiles` rules; they
cannot ad-hoc launch (`not_allowed`).

The profile author supplies an exact model string. Reasoning effort is optional
on every Codex route: when the profile omits `effort`, the plugin omits the
native effort wire fields (`thread/start`/`thread/resume` config and
`turn/start` effort) and preserves the provider/native default, reporting
the effective native readback separately; when a collaboration mode is
requested, its replacement settings carry the actual native readback effort
so the native selection is not cleared to the model default, while the
requested effort stays absent; no effort value is fabricated. A present effort is enforced exactly and a
missing model catalog entry on the direct route still fails startup
explicitly. On the direct ChatGPT Codex route, the Codex plugin uses native
`model/list` to look up that exact model and its supported reasoning efforts;
on an explicitly configured Responses route, the plugin skips that lookup and
passes through the explicit effort unchanged; provider rejection is surfaced
when the native operation reports it. Neither route issues a separate
inference or entitlement probe, or selects another model, route or account
automatically.
[`Codex`](../harnesses/codex.md#first-useful-path) owns native bring-up steps. Existing
workspace and owner login suffice; no proxy/account registry is mandatory.

The first native profile also accepts `cmd` (one executable, not a shell command),
`effort`, and `approval_policy` (`never`, the default, or `on-request`). `permissions`
selects the explicitly requested native sandbox: `read-only`, `workspace-write`, or
`danger-full-access`; the default remains `workspace-write`. `control` selects the
frozen command scope of launched Agents: `self` (the default), `tree`, or `project`.
These values are frozen at admission. The supported approval and question callbacks
are handled through exact pending request IDs; an unsupported callback receives an
error, never a default choice.

An owner may allow a profile's Agents to launch children on other configured
profiles of the same harness with `child_profiles`, e.g.
`child_profiles = ["codex-worker", "codex-review"]`. Cross-harness child profiles
are not supported and are rejected at config load and at launch. Names must exist
as configured profiles, and a profile must not list itself; absent or empty keeps
today's behavior (Agent children reuse the parent profile only). The parent's
launch captures the list in its frozen snapshot, so a later config edit cannot
widen an already-running manager. A child on an allow-listed profile carries that
profile's freshly frozen snapshot with only model and effort differing (control is
narrowed or equal under the separate check); the child inherits the parent's
captured credential bindings verbatim, and its capacity scopes keep every
non-profile scope of the parent with the same bounds. See the launch authority
check in [api](api.md) and [coordination](coordination.md#claims-and-hierarchy).

An optional `collaboration_mode` selects Codex's `default` or `plan` preset for each
turn. The selected mode is frozen with the profile; omission preserves the native
default. Plan mode enables the native user-question workflow. Model and effort stay
pinned when applying the preset, and the sandbox is configured independently. This
uses the installed experimental `turn/start.collaborationMode` field, so only profiles
that explicitly select a mode opt into `initialize.capabilities.experimentalApi`.
No global Codex configuration is changed.

An Antigravity profile selects `harness = "antigravity"` with an explicit native
`cmd`, an exact `model`, an optional `mode` (`plan`, `auto` or `accept-edits`)
and an optional `dangerously_skip_permissions` flag (default false). `effort`,
`approval`, `collaboration_mode`, `permissions` and `approval_policy` presets have
no mapping and are rejected. The effective permission mode and effective model come
from native init facts; init does not establish the served model. See the
[Antigravity page](../harnesses/antigravity.md).

A Muse profile selects `harness = "muse"` with an explicit native `cmd`, an exact `model`,
optional `effort`, optional `approval`, and `permissions` (`workspace-write`, the default,
or `danger-full-access`; `read-only` remains unsupported). Sandbox posture is fixed at
host birth (`--disable-sandbox` for `danger-full-access`); the MSP session wire has no
sandbox or permissions readback field and is not negotiable per session, so effective
permissions stay unknown (`null` / unverified) until a real native tool probe confirms
posture. `collaboration_mode` and `approval_policy`
presets stay rejected. See the [Muse page](../harnesses/muse.md).

### Worker MCP policy

Every profile selects a worker MCP inheritance policy in `mcp`: `inherit`
(the default) or `minimal`, plus an optional `mcp_allow` list of native
server names:

```toml
[profiles.codex-minimal]
harness = "codex"
model = "gpt-6-luna"
mcp = "minimal"
mcp_allow = ["owner-search"]
```

The resolved policy and allow list are frozen in the profile snapshot
(`mcp`, `mcp_allow`) and the requested policy is reported in secret-free
diagnostics and effective settings. Unknown policies are rejected at config
load, and a non-empty allow list requires `minimal`.

`inherit` keeps the current behavior: the worker receives Eliot's
Agent-scoped server in addition to the owner's native MCP surface.
`minimal` starts only the intended MCP surface: Eliot's Agent-scoped server
plus the explicitly allowed native servers. Eliot's scoped
`swarm mcp --require-agent` server is always present in both policies, and
no policy edits the owner's global native config, copies `auth.json`, or
changes native-store/observer bindings.

Only the Codex and Claude harnesses implement `minimal`, each with its
installed isolation mechanism; Muse, OpenCode and Antigravity keep
`inherit` only and reject `minimal` as unsupported. Only Codex implements
selective inclusion: a non-empty `mcp_allow` with `minimal` on any other
harness is rejected as `unsupported` at config load (Claude `minimal` is the
Eliot-only per-run config plus `--strict-mcp-config`, so an allow list
would be silently dropped; with `inherit` an allow list is already rejected
because it requires `minimal`). See the owning harness pages. Requested vs
observed: the
effective server set is reported from native readback only where the harness
supports it (`mcp_servers` with `mcp_servers_verified`); otherwise it stays
unknown (`null` / unverified), never an echo of the requested set.

## Owner workspace setup

Global profiles accept ordered setup entries only for an explicit `launch --worktree`.
The router rejects nonempty setup on a shared/existing workspace; profile loading never
runs a command. The raw owner format is:

```toml
[[profiles.codex-luna.setup]]
program = 'C:\Program Files\PowerShell\7\pwsh.exe'
args = ["-NoProfile", "-NonInteractive", "-File", { file = "scripts/setup.ps1" }]
timeout_ms = 120000
```

`program` is one explicit absolute executable. Arguments are literal strings or exactly
`{file="path"}`. There is no implicit shell, interpolation, per-entry cwd or environment.
An explicitly selected interpreter receives exactly the supplied argv. Unknown fields
are rejected. Limits are 16 entries, 64 arguments per entry, 4096 bytes per string,
128 KiB of raw/frozen setup metadata, and `timeout_ms` from 1 to 3600000 inclusive.

Admission freezes the selected executable and explicit files as
`{canonical_path,size,sha256}` using bounded streaming reads (128 MiB per file,
256 MiB total per plan). File references resolve against the admitted owner repository;
absolute file references must remain inside that same root. Selecting an executable
outside the repository does not extend the allowed root for `{file=...}` references.
Parent traversal, symlinks/reparse points, devices and alternate streams are rejected.

Before each spawn, verify the same canonical references, sizes and SHA-256 digests.
Missing or changed references fail before execution. Do not copy or substitute scripts
from the new checkout: file arguments become the admitted absolute paths, while cwd
is the verified worktree. These checks cover explicit entry files, not imported code or
an executable-provenance graph. The workspace coordinator owns the committed running
marker, process custody, deadlines and unknown/no-replay behavior.

Setup uses the native process's minimal baseline, `pass_env` name handling and captured
native-store selector inputs. It adds `ELIOT_AGENT_ID` and target
`ELIOT_AGENT_GENERATION`, without an Agent token. Managed identity names cannot be supplied
through `pass_env`; nested Eliot calls therefore see incomplete Agent credentials and
cannot silently fall back to owner authority. Stdin is closed and stdout/stderr piped.

Profiles also accept `scratch`; when omitted it defaults to
`["target/", "node_modules/"]` for every native profile. There is no builtin
fake profile supplying defaults. `scratch = []` disables all scratch deletion. Up to 64 bounded
relative directory entries are normalized to `/` separators and a trailing `/`; absolute
paths, dot traversal and alternate streams are rejected lexically. Execution must
recheck containment and symlink/reparse safety. Removal may delete only untracked or
ignored content within these frozen directories, never tracked dirt.

## Later configuration example

```toml
[daemon]
idempotency_window = "24h"
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
limits.max_selector_targets = 1000
automation.max_actions_per_minute = 120
automation.max_parallel_checks = 2
automation.check_timeout = "10m"

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
profiles = ["codex-luna"]
actions = ["run", "interrupt", "send", "stop", "git.fetch"]
run_env = ["CI"]

[projects.app.git]
fetch_remotes = { origin = "https://github.com/example/app.git" }
credential_helper = "manager"

[projects.app.interpreters.pwsh]
program = "C:/Program Files/PowerShell/7/pwsh.exe"
argv_prefix = ["-NoProfile", "-NonInteractive", "-File"]

[accounts.codex-main]
max_live = 8

[proxies.opencodex]
attach = "http://127.0.0.1:10300"
health = "/readyz"
base_url_for = { codex = "http://127.0.0.1:10300/v1" }
max_live = 8

[profiles.codex-opencodex]
extends = "codex-luna"
proxy = "opencodex"
account = "codex-main"
```

The implemented backlog setting is a root-level automation option:

```toml
[automation]
max_event_backlog = 100000
```

`max_event_backlog` is a positive signed 64-bit integer. It defaults to `100000`; zero,
negative, wrong-type and unknown automation values are invalid.

The implemented daemon retention setting is a `[daemon]` retention table:

```toml
[daemon]
retention.events_days = 30
retention.commands_days = 30
retention.mail_days = 30
```

All three windows are optional positive integer days; each defaults to unset,
which disables pruning for that side. A daemon without retention settings
deletes nothing; set a window to prune expired history for that side. Daemon
maintenance periodically prunes expired event history, closed command/child
sets and acknowledged mail older than `mail_days` through the Store writer;
see the [Core retention contract](core.md#retention). Command and write
bindings always outlive the 24-hour retry window plus the 5-minute allowed
clock-skew margin regardless of `commands_days`, so a pruned id resubmits as
`id_expired`, never as new. Mail rows outlive the retry window plus twice
that margin regardless of `mail_days`, so a removed delivery admitted with a
skew-bounded write id reads `history_gap` on its own mailbox's read paths;
a retried ack over it succeeds as already acked.

The automatic attention no-progress suspicion window is optional:

```toml
[daemon]
attention_suspicion_secs = 900
```

It defaults to 900 seconds and must be a positive signed 64-bit integer whose
millisecond value fits in `i64`. This setting only controls when a busy Agent
with a healthy native source and no known request/tool activity is diagnosed;
source-health failures are diagnosed promptly regardless of this window.

Cloud MCP is disabled when `[cloud_mcp]` is absent. When present, all fields are
required. The HTTP MCP executable is the running daemon executable (`current_exe`);
it is not a separate setting. The hostname is a bare DNS host; Eliot constructs
`https://<hostname>/mcp`. Tunnel and credential values are metadata only: Eliot
does not read the credential file contents. Cloudflared reads that file when it
starts.

Manual `swarm mcp --http` also requires these configured Access settings; it
never exposes an unauthenticated local Owner bridge. The HTTP origin verifies
the signed Access assertion with the configured team/AUD independently of
cloudflared. A missing verifier configuration or unavailable signing-key fetch
fails startup before the listener/readiness frame. Stdio and private IPC retain
their local principal resolution.

```toml
[cloud_mcp]
cloudflared_executable = "C:/Program Files (x86)/cloudflared/cloudflared.exe"
tunnel_id = "00000000-0000-4000-8000-000000000000" # replace with the provisioned tunnel UUID
credentials_file = "C:/Users/OWNER/.cloudflared/00000000-0000-4000-8000-000000000000.json"
hostname = "swarm.example.com"
access_team_name = "owner-team"
access_audience_tag = "0000000000000000000000000000000000000000000000000000000000000000"
```

`cloudflared_executable` and `credentials_file` must be absolute paths.
`tunnel_id` must be a UUID, `hostname` a lowercase DNS hostname,
`access_team_name` a lowercase Access team label, and
`access_audience_tag` a 64-character hexadecimal tag. These example identifiers
are placeholders, not a usable tunnel configuration.

The implemented unread mailbox bounds are a root-level mail table:

```toml
[mail]
max_unread_bytes_per_agent = 10485760
max_unread_messages_per_agent = 10000
```

Both bounds are positive signed 64-bit integers; each defaults to its
[coordination](coordination.md#mailbox) contract value when unset (10 MiB of
message text, 10,000 messages), and unlike retention windows they always
apply. Zero, negative, wrong-type and unknown mail values are invalid.

The implemented hierarchy fanout bounds are a `[daemon]` limits table:

```toml
[daemon]
limits.max_depth = 8
limits.max_children_per_agent = 20
limits.max_tree_agents = 100
limits.max_selector_targets = 1000
```

All four are positive signed 64-bit integers; each defaults to its
[coordination](coordination.md#claims-and-hierarchy) contract value when
unset, and like the mail bounds they always apply. The daemon loads them once
at startup before IPC binds. Zero, negative, wrong-type and unknown limit
values are invalid.

Inspection dated 2026-10-29 of installed OpenCodex 2.92.0 source
src/server/index/serve-options.ts shows an anonymous literal GET /readyz: readiness
returns HTTP 200 with {"status":"ready"}; pending or failed states return HTTP 503.
/healthz reports liveness only. Configure the readiness path explicitly; this is not a
version gate or a default.
Profile `extends` is a separate profile configuration feature. Project registration and
its rule allowlists are available to automation as described below.
Unknown keys are errors with location; plugin-private keys are validated by that plugin.
`extends` is one level. Missing fields use [core defaults](core.md#profile).
Command/write retention must cover the supported retry window and allowed clock skew.

## Global script Run approval

Global event and timer script Run approvals are owner-only in private `<data_dir>/config.toml` under
`[automation.run]`. The config loader exposes this grant as `config::RunGrantConfig` with
`interpreters` and `run_env`, via `config::load_automation_config(data_root)?.run`. Interpreter
map keys are exact literal references used by global event and timer rules; values use the Core
[`ScriptInterpreter` DTO](../../crates/eliot-core/src/api/scripts.rs). A missing or empty
`[automation.run]` table or `interpreters` map denies global Run. `run_env` is an exact
allowlist of literal nonsecret environment names; if absent or empty, no action-supplied
environment keys are allowed. Names follow [Environment and secrets](#environment-and-secrets).

The fixed private global script-entry root for event and timer Run is `<data_dir>/scripts/`. Any selected entry must
resolve to a canonical regular file below that root. The owner-approved interpreter and
Run action are the authorization boundary; there is no per-entry or per-rule file
allowlist. Project Run remains confined to its separate registered-checkout
`.eliot/scripts/` root.

## Project registration and rule authority

Only owner-global `[projects.ID]` entries register projects. `path` names the canonical
Git checkout; `profiles` and `actions` are explicit allowlists over existing global
profiles and implemented actions. Missing or empty allowlists deny all. Project
registration readback uses the canonical Core [`ProjectRegistrationView`](../../crates/eliot-core/src/api/agent_views.rs);
project identity includes the canonical checkout and Git common directory, not an Agent's
project label.

The optional owner-global `[projects.ID.git]` table binds fetch authority to that
registration. `fetch_remotes` maps exact remote names to approved direct endpoints without
embedded credentials; the repository's raw fetch URL must match the selected endpoint.
`credential_helper` may be omitted or set to `"manager"`; omission disables helpers and
does not fall back to ambient Git/auth configuration. `"manager"` serializes as Core
`GitCredentialHelper::Manager`; when selected, its executable is frozen as a canonical
file receipt, not secret material.
Core `ProjectRegistrationView.git` is `Option<ProjectGitView>`; `ProjectGitView` and
`GitCredentialHelper` live in
[`agent_views.rs`](../../crates/eliot-core/src/api/agent_views.rs) and are exported by
[`api.rs`](../../crates/eliot-core/src/api.rs). These approvals are part of the saved
registration revision and cannot be changed by project rule files.

For `git.fetch`, a project rule must be granted `git.fetch` and its literal `repository`
must equal the ID in its saved `project:ID` principal. Owner-global rules may select any
owner-registered repository and use that registration's saved Git approvals. Authority
comes from the saved principal and registration, never from action args.

Project run uses two additional owner-global approvals. Each
`[projects.ID.interpreters.NAME]` table requires an absolute `program` executable path
and a literal `argv_prefix` array; its name is the interpreter reference used by `cmd[0]`.
A missing or empty `interpreters` map denies project runs. `run_env` is an exact allowlist
of nonsecret environment names; missing or empty `run_env` permits no action-supplied
environment keys.

`cmd[1]` must be a literal relative path resolving to a canonical regular file below
the registered checkout's `.eliot/scripts/`. Reject escapes, symlinks/reparse points,
devices and alternate streams. Freeze the selected path, size and digest with the
command and verify them before spawn. Before spawn, authorize the `run` action in
`actions` and resolve `cmd[0]` to an approved interpreter. After successful exit, check
each emitted action separately against `actions` and the ordinary Router/Store fences
before atomic child admission. The interpreter and `run_env` approvals are part of the
canonical project registration snapshot/revision and are revalidated at admission and
dispatch.

Load project rules only from the registered checkout's `.eliot/rules/*.toml`, never from
an Agent worktree. Project rule files have principal `project:ID`. The owner registration
approves the profile, action, interpreter and `run_env` sets; project files cannot define
or override profiles, interpreter executables or fixed argv prefixes,
model/effort, route/account, credentials, permissions, workspace mode or Git identity.
A project rule may supply only literal `run.env` values for owner-approved names. A
project `.eliot/config.toml` overlay is unsupported. Admission and dispatch recheck the
registration and the target's approved profile, repository identity, workspace readiness,
and exact generation before an effect. Missing or changed authority fails closed; there
is no Owner fallback for a project-scoped request.

Project configuration changes remain owner edits to the private global config. Invalid
project rule reload retains the last valid definition and reports a bounded diagnostic.

## Frozen profile and route binding

Admission captures the resolved nonsecret profile once. Running and queued work do not
follow reload. Resume uses the stored profile. Explicit profile replacement for a
generation-changing operation remains part of the final contract; the current resume
command has no profile override.

New launch/attach admissions [capture](../../crates/eliot-daemon/src/config.rs) only
the nonsecret `CODEX_HOME` and `CODEX_SQLITE_HOME` inputs in
`profile_snapshot.pass_env_values`. Each selector is explicitly `null` when not
selected by `pass_env` or unset; a selected empty string remains distinct. Preserve raw
values without trimming or path normalization. Names compare case-insensitively on
Windows and exactly elsewhere. [Environment construction](../../crates/eliot-daemon/src/plugin_host/host.rs)
uses these captured inputs instead of looking them up again. This selector capture
leaves other `pass_env` resolution unchanged.

Retained mutation IDs hydrate their stored snapshot before any new capture. Admitted
selector inputs are frozen at admission and are neither rewritten nor backfilled:
an absent capture is a legacy snapshot, distinct from captured null/unset. A
recovered accepted controlled Codex launch without captured selector inputs
refuses before native control. The controller never performs an execution-time
`pass_env` lookup for native-store selection, never backfills those inputs, and
never reselects the store from ambient selectors. Resume
requires the retained effective `native_store_binding` and verifies it before native
control. Capture proves only the supplied inputs, not the effective native home,
database root or scope; native configuration can affect selection.

For a new Codex launch, resolve the effective store only after the actual workspace is
prepared, including setup. The owned read-only
[`profile.store`](../../crates/eliot-plugin/src/protocol.rs) reader uses that workspace
and the admitted inputs. This metadata reader issues no `model/list`, entitlement or
inference probe; direct ChatGPT control startup uses the catalog lookup described under
[First profile](#first-profile). Native app-server startup may make a vendor-owned read-only model-catalog GET,
normally to the selected provider base URL. A supported absolute `model_catalog_url`
can redirect that request to another host, potentially with native client authentication;
route admission must pin it to the selected provider or suppress it before process birth.
The metadata reader must not receive the client-key value; keep the resolved value in its
secret-redaction policy. The controlled process still requires the explicitly configured
client key. In the launch dispatch transaction, [Store](../../crates/eliot-store/src/lib.rs) appends the verified
[`NativeStoreBinding`](../../crates/eliot-core/src/native_store.rs) once to
`Agent.profile_snapshot.native_store_binding`. The admitted `Command.args` and its
payload digest remain unchanged. A retained binding cannot be replaced by rediscovery.
Codex resume reuses it and verifies the pinned store before native control; legacy
Agents without a binding refuse before control rather than selecting a store anew.

```text
ProfileSnapshot {
  profile_name, harness, plugin,
  model?, effort?, permissions, control, args, mcp, mcp_allow,
  literal_env, pass_env_values, secret_env_names, inherit_env,
  account?, proxy?, billing?, workspace_mode, git_identity,
  route_binding?, capacity_scopes[], labels, stall_after, notify_parent, wake_on_mail,
  system_note?, setup[], scratch, source_files
}
PluginBinding { name, kind: builtin|external, cmd?, private_config,
                private_config_digest, executable?, referenced_files[] }
FrozenFile { canonical_path, size, digest, private_copy? }
```

The optional `route_binding` and `capacity_scopes` values use the shared Core
[`RouteBinding` and `CapacityScope` DTOs](../../crates/eliot-core/src/routing.rs); that code
is their canonical field catalog. Snapshots preserve selected account/proxy names and
local cap-scope names. These names and caps do not establish vendor identity, entitlement
or quota. A route binding is captured at admission and is never inferred again after
reload.

Builtin plugins are linked in the installed binary. External plugin bindings resolve an
absolute executable plus explicit argv/private config; capture local entry files used by
that command. Dispatch uses this plugin binding, not a new config lookup. Changed or
missing captured files fail before execution. Manifest cache keys use the plugin binding
digest; software version itself is diagnostic, not authority.

Capture entry files and explicit inputs, not a universal hermetic build/runtime graph.
A script importing mutable dependencies is not fully reproducible merely because its
entry-file digest matches. Use a normal pinned package environment when that project
requires dependency reproducibility; do not invent an artifact platform in Eliot.

Credentials are execution-time references. Adopted proxy freezes endpoint/health/base
URLs/key name; managed proxy also freezes launch argv and referenced files. Service reuse requires
matching `(name,config_digest)` and exact endpoint/custody. Config reload never mutates
or kills a service still used by old snapshots; conflicting endpoint reuse fails clearly.

### Mid-run model/effort overlay (`configure`)

The frozen snapshot above never changes for a generation. `command.submit`
`verb:\"configure\"` records a separate overlay instead: the requested values,
the effective values the next native turn carries (each requested value, or
the frozen original when not requested), a monotonically increasing
`config_revision` for the Agent generation, and `application:\"next_turn\"`.
Only `model` and `effort` are supported; authority-adjacent fields are
rejected outright and anything else is `unsupported`. Only the Owner may
submit it.

Codex carries both values as per-turn `turn/start` overrides
(`TurnStartParams.model`/`effort`); Muse carries effort through per-turn
`reasoningEffort`, while a Muse `model` change needs a new native session
and is refused as `unsupported`, storing nothing. Claude, OpenCode and
Antigravity have no verified per-turn mechanism and refuse the command. A
catalog or quota failure at the next turn fails that turn honestly; it never
selects another model or account.

Each send keeps the revision current at its own dispatch; a send committed
before an overlay never runs under newer configuration, and a send admitted
after it dispatches under it. Each turn records the revision it ran under.
The settings lane is ordered by shared admission order: a configure does not
apply while an earlier-admitted send is still queued or undispatched, an
earlier queued send passes a later configure, and a later input waits for an
earlier configure; the Router redispatches on lane release. When the native
turn request carries a resolved effort the overlay left absent, the turn
association records it on the overlay's effective values while the requested
values stay exactly what the owner asked. Retention keeps every command a
retained overlay references: pruning never deletes a command named by the
overlay, so configuration provenance survives expiry. `swarm show` (the
`agent.get` view) reports the current effective values, the revision, and the
original frozen values together.

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
The managed Agent target environment sets `ELIOT_HOME` to the owned data directory,
overriding ambient/profile, alongside `ELIOT_AGENT_ID`/`ELIOT_AGENT_GENERATION`/`ELIOT_AGENT_TOKEN`.
It prepends the active controller executable's parent directory to `PATH`, preserving the
native baseline entries, so bare `swarm` resolves to the current controller before ambient/legacy bins.
A controller directory containing the OS `PATH` separator (`;` on Windows, `:` elsewhere)
is explicitly unsupported: native shells split `PATH` without quote awareness, so managed
start rejects it before native spawn as a definite rejected result (no native I/O dispatched),
instead of an unknown outcome or a silent legacy `swarm` resolution.
An adopted route stores only the optional client-key environment-variable name; the
profile must explicitly allow that name in pass_env. Its value is resolved at runtime
and is not sent with the unauthenticated readiness request.

`inherit_env=true` is an owner/global opt-in with a visible warning: unrelated daemon
secrets then reach the process. It is not default. Route/account selectors still remain
explicit. Credential rotation may change authentication success but must not silently
select another account/provider.

Project script `run.env` is separate from profile `env` and `pass_env`: the child receives
the minimal platform baseline, literal values for owner-approved `run_env` names and its
Run identity. On Windows the baseline reads `SystemRoot` and `PATHEXT` from the
daemon OS environment and fails before spawn if either is absent; it does not add
`PATH`. It does not inherit arbitrary owner/Agent environment, profile secrets or route/account
credentials and selectors. `run_env` cannot name Eliot/Agent/Run identity, credential,
route/account, native-store selector, Git identity, `PATH` or other reserved environment names;
names compare case-insensitively on Windows. Project values cannot override the
role-specific Run variables.

Mask known secret values in native output and reject them in caller durable text.
Private routing tokens are handled separately from semantic profile data. Native
children receive their own identity only where isolated environment injection is possible;
otherwise their CLI calls remain attributed to the parent, as the plugin reports.

## Accounts and configured capacity

Configured profile, account and selected-proxy `max_live` caps are local admission
limits. An omitted cap (`None`) is unlimited; `0` disables that scope. The snapshot
freezes the scope names and configured values. A full scope rejects before native
control with `capacity_unavailable`. That request is not admitted or persisted, so it
creates no command, Agent, event or reservation; the same ID may be submitted again
after capacity changes. An already admitted command retains its result under its ID, and
replay never starts another native generation or reservation. A capacity reservation is
released only after the owned generation is proven closed: the OS source closed
with every recorded process of the generation exited (at least one plugin or
harness), no native source open, and the plugin host's record that it saw the
plugin exit and drained its reader (a stop or lost transition closes sources
without a drain). The host record and the OS closure arrive in either order;
each re-checks the proof and the later one releases.
Uncertain starts retain their reservation across daemon replacement. These limits describe Eliot's admitted workload,
not vendor entitlements, account permissions or provider quotas. Named native
profiles may use named profile/account caps; a profile acquires a proxy route
only when it explicitly selects one.

Restart recovery reconciles reservations that a daemon replacement leaves behind. After
`recover_on_restart` and before IPC bind, the daemon reads every `lost` or
`stopped` (a forced stop records no `session.ended`) controlled generation whose `capacity_generation` still equals its generation, with the process
identities recorded for owner `agent:<id>#<generation>`. It checks custody outside the
Store lock, then commits the closure and `capacity.released` (or the transfer to an
admitted resume) in one Store transaction conditional on `capacity_generation` still
equaling that generation. Generations it cannot settle are rechecked every two seconds;
no new generation of this daemon is ever among them. The only persisted custody identity
added for this path is a random per-database namespace (`store_identity`): Windows names
each Agent generation's kill-on-close Job `Global\eliot-<namespace>-<agent>-<generation>`
before the suspended plugin starts, and never adopts an existing object of that name.

Closure and source coverage are independent. Restart already closes the lost
generation's native sources as `stale`; a successor's source handoff records any gap for
the lost tail, and release requires that no native source of that generation is open. A
gap, a stale/closed source, an Agent marked `lost`, a timeout or cleared provider hold
never proves custody closed or a turn complete. Release does not change any command or
turn: uncertain launch/send and unknown turns stay unknown and are never replayed.

Closure follows the [platform custody rules](monitoring.md#process-identity-and-custody).
A PID being absent and the estimated Windows boot marker are insufficient. On Windows the
named Job must be gone or open with no member, and every recorded process identity,
matched by PID and exact creation time, must have exited (`windows_job_closed`). The
name disappears when the last handle closes, which for this kill-on-close Job without
breakaway already terminates every member; a member never recorded in `processes` cannot
be checked afterwards and relies on that kernel termination. A generation started before
the namespace existed has no findable Job and stays retained. On Linux every recorded identity of the current boot must be gone
or a zombie and the recorded plugin leader's process group empty (`unix_group_empty`,
`orphan_possible: true`, since escaped descendants stay possible); identities from an
earlier boot prove closure by reboot (`host_rebooted`). Otherwise the reservation stays,
the first check records `capacity.retained` with the missing evidence, and a resume of
that Agent is rejected with `capacity_unavailable` naming the retained generation. Later
exact native terminal-session evidence accepted by existing settlement may still settle
it. An owner command/turn assertion is not a capacity override. Do not count unknown
reservations as free slots.

Provider holds are durable Eliot state over the named local scope derived from the frozen
route, account or profile. They do not infer vendor account identity, entitlement or
quota. A held scope blocks new launch/send and queued dispatch without native effects;
plain resume, reads, request replies, interrupt and stop remain available. Resume with
new text is gated as a linked send. Rate/quota probe and auth/model-removal correction rules are in
[monitoring](monitoring.md#provider-holds). Holds never trigger automatic route, model
or account fallback. Billing and usage discovery remain unsupported.

## Plugins

`[plugins.<name>] cmd` selects an external executable; omitted means builtin. Extra keys
are private validated plugin config; secrets use names, never literal stored values.
Manifest chooses per-Agent/shared mode. Implement only per-Agent mode for the first
Codex path. Shared mode requires actual session/environment/stop isolation, not merely
several chats hosted in one process.

Probe/cache a manifest without creating a billed native session. Existing snapshots use
the matching binding. Recheck availability at dispatch without replacing its route.

## Adopted proxy services

This increment supports only a named, already-running endpoint. Configure its endpoint,
readiness path and provider base URL explicitly; there is no default readiness path.
Eliot performs a bounded unauthenticated readiness read and never starts, stops or repairs
the adopted service. Agent stop owns only the Agent's controlled process tree. Managed
service launch and service-management APIs remain `unsupported`. Readiness and effective
configuration readback do not verify runtime authentication, inference or all traffic;
report those facts as unverified. The owner remains responsible for service lifecycle.

## Paths and platform defaults

Keep Windows paths short; use long-path Git arguments/environment without rewriting
repository config. Use a separate Cargo target directory per worktree. Executable, argv,
cwd and env remain separate. Launch/workspace admission never performs an implicit fetch.
All persisted fields and frames are bounded. The roadmap determines when each section
becomes implemented; absent later features return `unsupported`, not a hidden substitute.
