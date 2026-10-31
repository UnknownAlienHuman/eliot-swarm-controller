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
size-bounded. `logging::record(&SecretPolicy,code:&str,ids:&[(&str,&str)],error:Option<&Error>)->()`
writes only bounded codes/IDs and structured sanitized plugin/harness stderr tails,
applying the same policy to code and ID values. It omits `Error.message`, request
parameters and environment values. A record failure is diagnostic loss only; it cannot
change command settlement, source acknowledgement or process cleanup.

## First profile

```toml
[profiles.codex-luna]
harness = "codex"
model = "gpt-6-luna"
permissions = "workspace-write"
pass_env = ["CODEX_HOME"]
```

The profile author supplies an exact model string. Eliot does not call `model/list`,
check provider entitlement, or issue an inference probe to resolve it. An unavailable
model fails explicitly; it does not trigger route or account fallback.
[`Codex`](../harnesses/codex.md#first-useful-path) owns native bring-up steps. Existing
workspace and owner login suffice; no proxy/account registry is mandatory.

The first native profile also accepts `cmd` (one executable, not a shell command),
`effort`, and `approval_policy` (`never`, the default, or `on-request`). `permissions`
selects the explicitly requested native sandbox: `read-only`, `workspace-write`, or
`danger-full-access`; the default remains `workspace-write`. These values are frozen
at admission. The supported approval and question callbacks are handled through exact
pending request IDs; an unsupported callback receives an error, never a default choice.

An optional `collaboration_mode` selects Codex's `default` or `plan` preset for each
turn. The selected mode is frozen with the profile; omission preserves the native
default. Plan mode enables the native user-question workflow. Model and effort stay
pinned when applying the preset, and the sandbox is configured independently. This
uses the installed experimental `turn/start.collaborationMode` field, so only profiles
that explicitly select a mode opt into `initialize.capabilities.experimentalApi`.
No global Codex configuration is changed.

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

Profiles also accept `scratch = ["target/", "node_modules/"]`, the defaults shared by
the builtin fake profile. `scratch = []` disables all scratch deletion. Up to 64 bounded
relative directory entries are normalized to `/` separators and a trailing `/`; absolute
paths, dot traversal and alternate streams are rejected lexically. Execution must
recheck containment and symlink/reparse safety. Removal may delete only untracked or
ignored content within these frozen directories, never tracked dirt.

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
selector inputs are neither rewritten nor backfilled: an absent capture is a legacy
snapshot, distinct from captured null/unset. Existing legacy environment construction
may retain its established execution-time `pass_env` lookup; that compatibility does
not authorize ambient native-store selection. A recovered accepted controlled Codex
launch without captured selector inputs refuses before native control. The controller
neither backfills those inputs nor reselects the store from ambient selectors. Resume
requires the retained effective `native_store_binding` and verifies it before native
control. Capture proves only the supplied inputs, not the effective native home,
database root or scope; native configuration can affect selection.

For a new Codex launch, resolve the effective store only after the actual workspace is
prepared, including setup. The owned read-only
[`profile.store`](../../crates/eliot-plugin/src/protocol.rs) reader uses that workspace
and the admitted inputs. Eliot itself issues no model/list, entitlement or inference
probe. Native app-server startup may make a vendor-owned read-only model-catalog GET,
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
  model?, effort?, permissions, control, args,
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
released only after the owned generation is proven closed. Uncertain starts retain their
reservation across daemon replacement. These limits describe Eliot's admitted workload,
not vendor entitlements, account permissions or provider quotas. Fake profiles may use
named profile/account caps; Fake does not acquire a proxy route.

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
