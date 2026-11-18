# Configuration

### Artifact roots

`[daemon].artifact_roots` is an optional array of at most 32 absolute local
directories (default empty). Before native startup Swarm exclusively creates and
durably binds `<first-root>/<agent>-<generation>/`; without a configured root it
uses `<data-dir>/artifacts/<agent>-<generation>/`. `ELIOT_ARTIFACT_ROOT` hands this
path to the Agent. Registration accepts only strict descendants of that exact
generation root. Existing unbound directories are never adopted. Shared
configured roots, workspace paths alone and user/native configuration
remain protected. This does not scan or adopt existing unregistered files. See
[registered artifacts and cleanup](coordination.md#registered-artifacts-and-cleanup-k69).
The setting is available through the existing Daemon settings section. A folder assignment opts in with `[automation.assignments.<key>] cleanup_on_retire = true`
(default false): after an applied stop retires its Agent, the existing stop
worker cleans that Agent's build, temp and managed-worktree artifacts with
Owner authority and retained receipts. It is not a second executor.

Implement the single global Codex profile first. Project overlays, external plugins,
services, mail and automation settings appear only with their owning roadmap layer;
this page is the final configuration contract, not an L1 checklist.

## Private data directory

Default `%LOCALAPPDATA%\Eliot` or `~/.local/share/eliot`; override with `ELIOT_HOME` or
`--data-dir`. Root and private files are current-user-only. Do not accept redirected
symlink/reparse roots or unsafe ownership.

Linux is distributed as the [supported Docker image](../../docker/README.md).
It runs as UID/GID 10001 with `ELIOT_HOME=/data`, an owned mode-0700 volume;
existing unsafe mounts are refused without chmod/chown repair. Native executable
and workspace paths are absolute container paths. Credentials enter only through
explicit runtime mounts/environment allowlists, never image layers. The Docker
guide specifies per-vendor mounts and the risk of exposing credentials to Agents.
Keep the root short enough for `<data-dir>/daemon.sock` (107 UTF-8 bytes total).

On first use, create the directory with a protected current-user ACL before publishing
private files. Owner and ACL checks open the root by handle through the verbatim form,
so roots past the Win32 parse limit keep the same current-user-only protection.
An existing unsafe root is refused; never repair ownership, weaken the
check or copy its contents silently. The error offers one explicit command selecting
a fresh private per-user directory; the desktop offers the same remedy. The user must
choose it. Existing roots and configuration are not overwritten by bootstrap.

The desktop's explicitly chosen fresh root is retained in a private per-user pointer:
`%LOCALAPPDATA%\Eliot-bootstrap\selected-data-dir` on Windows, or
`$XDG_CONFIG_HOME/eliot-bootstrap/selected-data-dir` (otherwise
`~/.config/eliot-bootstrap/selected-data-dir`) on Unix. The pointer lives outside the
possibly unsafe default data root. CLI and desktop share resolution order:
`--data-dir`, `ELIOT_HOME`, selected-root pointer, then the platform default. The
pointer and target must pass the private-directory checks; failures never silently
select another root. Selection creates and validates the fresh root before publication.

After acquiring the data-directory lock and before serving, daemon startup
materializes the bundled `eliot-swarm` Agent skill under `skills/` with a
private ACL and atomic replace (see [Agent skill
delivery](plugin-protocol.md#agent-skill-delivery)). Every harness plugin
reads that copy through its own per-session mechanism; a failure to publish
fails startup.

Owner CLI callers start their private daemon when needed (`status` remains a read-only
probe). Agent and Run callers keep their inherited identity and dispatch directly;
they never probe Owner-only readiness or start a daemon with Owner authority.
The desktop starts the sibling `swarm.exe` and waits for daemon readiness before
loading settings. `swarm-desktop --workspace PATH` opens and registers that checkout;
without the flag it opens the working directory. A later launch in a different
checkout registers that workspace through the same machine-author settings writer.

```text
<data_dir>/
  config.toml
  owner.token
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.lock, daemon.json, logs/daemon.log
  skills/                daemon-materialized `eliot-swarm` Agent skill (private, rewritten at startup)
  plugins/                 only instantiated plugins
  worktrees/               L3 opt-in workspaces
  services/                managed services, when needed
  rules/                   L4
  scripts/                 global script Run entries
  frozen/                  bounded admitted entry-file copies, when needed
```

The ordinary CLI reads `owner.token` automatically, only outside managed Agent/rule
environments. A missing managed token rejects, never becomes owner. An inherited Agent
identity applies only to the daemon that issued it (see
[Environment and secrets](#environment-and-secrets)): calls naming another data directory
(`--data-dir`, else `ELIOT_HOME`, else default) connect as that directory's local Owner
instead of sending the foreign token. This is attribution
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
and Antigravity. Each profile selects one `harness` explicitly and may pin an exact
`model` or preserve the native default. Only Codex may omit `cmd`, in which case it launches `codex` by name;
the other four adapters require an explicit native executable in `cmd` in a
stored profile. First use writes visible, editable native-default profiles for
installed harnesses through the settings writer as `bootstrap`. On a fresh root,
the default is the first installed of `codex`, `claude` and `opencode` (the
harnesses that can disable native subagents per session), otherwise the first
installed profile in name order. It has `control = "tree"` and `child_profiles`
listing the other installed bootstrap profiles, so on those harnesses its omitted
`native_subagents` resolves to `deny`. A Muse or Antigravity default, which has
no per-session switch, is written with `native_subagents = "allow"` so the
bootstrap settings stay valid. The `worker`
routing role lists those profiles in name order (or the inherited default profile
when it is the only installed harness). This makes Swarm delegation available on
first use; existing profiles and routing choices are preserved. An omitted model
in these profiles requests the native default and retains requested/effective
readback separately, as for an ad-hoc launch. There is no builtin fake profile.
For Muse hot-start profiles the daemon enumerates installed `muse-bin-*.exe`
builds newest-first and probes with the existing read-only `model/list`
catalogue path (no session, turn or inference); it freezes the newest build whose
catalogue is non-empty and records skipped empty-catalogue builds
(`bootstrap.muse_empty_catalogue_skipped`). Selection stops at the first nonempty
catalogue, with an 8-second per-build probe and one 20-second selection budget.
When that budget expires, `bootstrap.muse_catalogue_budget_exhausted` is recorded
and startup continues without a Muse default. Each probe uses scoped process
custody and kill-on-drop, with at most 100 inbound frames, 1 MiB per line and
4 MiB total before parsing. The launcher-marked 1.4.20 build serves
an empty `bundledCatalog` and is skipped in favor of the 1.4.0 build with a live
catalogue; `--cmd` still pins an explicit binary for ad-hoc launch.
Before admission, an omitted profile selects the first available ordered role candidate. There is no post-dispatch harness/model/route/account
fallback, no retry of an uncertain outcome and no hold-triggered account switch.
Profiles stay the durable
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
[--model M] [--effort E] [--auth I] [--access ask|auto|full] [--cmd PATH] --workspace W "prompt"` (and the same in
`command.submit` `launch` args, where `cmd` and `access` ride beside `model`/`effort` in
the exact retry-bound payload) is an Owner-only tool that builds and freezes
a profile-equivalent snapshot exactly like a stored profile: built-in safe
defaults for that harness plus the supplied flags, reproducible and
replay-safe. Requested `model`/`effort` may be omitted; the freeze records the
requested value as absent separately from the effective value the harness
resolves at start (Codex `model/list` default entry and its default effort,
Muse server default (after proving the `model/list` catalogue is non-empty; Muse names no default marker), Claude CLI default with `--model`/`--effort`
omitted and effective read back), never a
silent substitution: an unavailable requested model fails honestly. Authority
fields default to the safe posture (Codex `workspace-write` + `never`,
Claude CLI-default permission mode, Muse current conservative
defaults); broader authority stays profile-only, owner-written choice. When
`--cmd` is not given, the daemon resolves the installed native executable
deterministically and freezes the absolute path. Ad-hoc Claude uses
`mcp = "minimal"` and selects the newest standalone CLI by `--version`
from `PATH` and standard user install locations. The Windows Desktop
bundle requires an explicit `--cmd` or profile `cmd`. See
[Claude Code discovery](../harnesses/claude-code.md#ad-hoc-launch).
OpenCode resolves the npm shim to the
native `opencode.exe` inside the package (consulting the
`USERPROFILE`-derived roaming root when `APPDATA` is stripped), Muse via the
launcher dir version the launcher marks current, Codex via the existing
`codex` resolution, Antigravity via the installed app path (or `unsupported`
when the harness needs a different binary). `--cmd` pins one explicit native
executable instead (e.g. a Muse binary version the launcher no longer marks
current). A missing or ambiguous executable is a clear
`unsupported`/`profile_error` naming what was searched. `--auth` is
OpenCode-only (other harnesses reject it as `unsupported`): an explicit
selector freezes verbatim like the profile's `auth_integration`, while an
omitted one defaults to the model provider prefix only when the native
`opencode auth list` holds an entry with exactly that id. Anything else
refuses before spawning with a typed `profile_error` naming `--auth` and
listing the available integration ids; another provider's credential is never
picked silently, and the plugin's pre-serve transfer gate is unchanged.
`--access` selects the Owner access level next to `harness`/`model`/`effort`
(`swarm launch --access …`, `swarm switch --access …`, `command.submit`
`launch` `access`). Omitted keeps the safe defaults. The level freezes its
native permission result in the snapshot and the effective settings report it
truthfully. `ask` routes every approval through the existing pending-request
path (`swarm approve`/`deny`). For a generation frozen with `access=ask`,
only the Owner may `approve`, `deny` or `answer`: self, manager and other
non-Owner replies refuse with `not_allowed` through CLI and MCP alike.
The Agent-scoped Swarm MCP tool may be auto-approved because it cannot
authorize these replies. A level a
harness cannot express refuses with a typed `unsupported` reason.

| Level | Codex | Claude | OpenCode | Muse | Antigravity |
| --- | --- | --- | --- | --- | --- |
| `ask` (Owner replies only) | `approval_policy=on-request` + `workspace-write` (pending-request) | `permission_mode=manual` (pending `can_use_tool`) | native permission-reply surface (pending) | `approval=onRequest` + `workspace-write` (pending `approval/request`) | `dangerously_skip_permissions=false` (ACP `session/request_permission`) |
| `auto` | `approval_policy=never` + `workspace-write` | `permission_mode=auto` | native permission-reply surface (pending) | `approval=promptUnmatched` + `workspace-write` | `dangerously_skip_permissions=false` (ACP permission path) |
| `full` | `approval_policy=never` + `danger-full-access` (exactly current full-access profiles) | `permission_mode=bypassPermissions` (full-access) | `unsupported` (no full-access mapping; refuses instead of widening) | `approval=allowAll` + `danger-full-access` (exactly current full-access) | `dangerously_skip_permissions=true` (exactly current full-access skip) |

Agent principals keep
the profile and `child_profiles` rules; they cannot ad-hoc launch
(`not_allowed`).

When supplied, the profile author's model string is exact. Reasoning effort is optional
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
These values are frozen at admission. The shared `approval` field has no Codex mapping and is rejected; use `approval_policy`. The supported approval and question callbacks
are handled through exact pending request IDs; an unsupported callback receives an
error, never a default choice.

An Owner may allow a profile's Agents to launch children on other configured
profiles of any harness with `child_profiles`, e.g.
`child_profiles = ["claude-worker", "muse-worker"]`. Names must exist
as configured profiles, and a profile must not list itself; absent or empty keeps
today's behavior (Agent children reuse the parent profile only). The parent's
launch captures the list in its frozen snapshot, so a later config edit cannot
widen an already-running manager. Naming a different harness is an explicit Owner
authority grant: the child carries that profile's freshly frozen harness, model,
permissions, MCP policy and credential references/selector bindings. Parent policy
names are never reinterpreted on the child's harness, and parent credential or
native-store bindings are never copied across harnesses. Same-harness children keep
the existing model/effort-only confinement and inherit captured credential bindings
verbatim. Control remains equal or narrower on every harness; workspace and project
remain inherited. A cross-harness child retains its own capacity scopes and every
non-profile scope of the parent with the same bounds; conflicting bounds reject.
The parent's own profile scope is replaced by the child's. See the launch authority
check in [api](api.md) and [coordination](coordination.md#claims-and-hierarchy).

A profile selects its harness-native subagent surface in `native_subagents`:
`allow` (the default without `child_profiles`) keeps today's behavior, while
`deny` asks the harness plugin to disable its native subagent tool for the
session so delegation goes through Swarm (`launch` with `role` or an allowed
profile, claims, mail, `wait`/`digest`). An omitted value defaults to `deny`
for profiles with a non-empty `child_profiles` allow-list (managers delegate
through Swarm) and to `allow` otherwise. The frozen value is captured in the
profile snapshot and reported truthfully in the effective settings. Explicit
`allow` on a manager preserves native subagents (audit profiles that rely on
Codex native subagents set `allow`). `deny` has a per-session mapping on
Codex, Claude and OpenCode only; on Muse and Antigravity the installed
binaries expose no verified switch, so `deny` is `unsupported` at config load.
On OpenCode the denial is a per-session `subagent` wildcard deny stored as
the final entry in the session's entire ruleset and verified by readback of that
exact session on create and resume: the server merges session rules after
every agent/project rule with last-match-wins evaluation, so the final deny
survives a project `subagent` allow that would beat the isolated per-Agent
config layer (which is kept only as a second layer). Existing session rules
are preserved and the deny is appended as final; the same final readback
sets the verification flag. Create and pre-readiness checks leave a dedicated
cleanup window. A missing or unreadable denial goes to cleanup before hooks
bind. A proved removal fails the start. An unproved removal leaves the start
unknown and retains the exact native identity in the live plugin, as described
in [Start ordering](plugin-protocol.md#start-ordering). A retained session that
predates the ruleset is repaired with its rules preserved and re-read before use.

An optional `collaboration_mode` selects Codex's `default` or `plan` preset for each
turn. The selected mode is frozen with the profile; omission preserves the native
default. Plan mode enables the native user-question workflow. Model and effort stay
pinned when applying the preset, and the sandbox is configured independently. This
uses the installed experimental `turn/start.collaborationMode` field, so only profiles
that explicitly select a mode opt into `initialize.capabilities.experimentalApi`.
No global Codex configuration is changed.

An Antigravity profile selects `harness = "antigravity"` with an explicit native
`cmd`, an optional exact `model`, an optional `mode` (`plan`, `auto` or `accept-edits`)
and an optional `dangerously_skip_permissions` flag (default false). `effort`,
`approval`, `collaboration_mode`, `permissions` and `approval_policy` presets have
no mapping and are rejected. The effective permission mode and effective model come
from native init facts; init does not establish the served model. See the
[Antigravity page](../harnesses/antigravity.md).

A Muse profile selects `harness = "muse"` with an explicit native `cmd`, an optional exact `model`,
optional `effort`, optional `approval`, and `permissions` (`workspace-write`, the default,
or `danger-full-access`; `read-only` remains unsupported). Sandbox posture is fixed at
host birth (`--disable-sandbox` for `danger-full-access`); the MSP session wire has no
sandbox or permissions readback field and is not negotiable per session, so effective
permissions stay unknown (`null` / unverified) until a real native tool probe confirms
posture. `collaboration_mode` and `approval_policy`
presets stay rejected. See the [Muse page](../harnesses/muse.md).

An OpenCode custom-provider profile points one frozen profile at a local
OpenAI-compatible endpoint instead of a canonical integration:

```toml
[profiles.opencode-local]
harness = "opencode"
cmd = "C:/Tools/OpenCode/opencode.exe"
model = "k173d-fixture/test-model"
pass_env = ["LOCAL_LLM_KEY"]

[profiles.opencode-local.local_provider]
id = "k173d-fixture"
package = "@ai-sdk/openai-compatible"
base_url = "http://127.0.0.1:1234/v1"
api_key_env = "LOCAL_LLM_KEY"
models = [{ id = "test-model", context_limit = 32768, output_limit = 4096 }]
```

`model` is the exact `provider/model` string (model IDs may contain slash
segments). Swarm generates the isolated
OpenCode provider entry from `local_provider`: `base_url` becomes
`settings.baseURL`, `api_key_env` becomes a `{env:NAME}` runtime reference,
and each model record supplies its native model limits. The typed shape is
validated at freeze, retained admission, and before every write; unknown
fields, literal credentials, URL userinfo/query/fragment components, and
substitutions are refused. `api_key_env` must be listed in `pass_env`, and
`auth_integration` must be absent, so no unrelated canonical credential is
imported. The adapter sets `OPENCODE_CONFIG_PROJECT_DISABLE` and strips
`OPENCODE_CONFIG`, `OPENCODE_CONFIG_CONTENT`, and `OPENCODE_CONFIG_DIR` for the
server and both preview readers. The plugin checks the exact model pair through the live `GET /api/model`
catalogue before creating a session. An unreachable endpoint can still be
listed and reports its failure on the native turn. Each named profile is a
separate picker preset with its frozen model and effort. OpenCode catalogue
failures share one bounded entry.
There is no harness/model/account fallback. Codex profiles against these
endpoints stay `unsupported`. See the
[OpenCode harness page](../harnesses/opencode.md#typed-local-providers).

### Worker MCP policy

Every profile selects a worker MCP policy in `mcp`: `minimal` (the default
where the harness maps it) or `inherit` (explicit opt-in, and the default for
Muse, which has no minimal mapping), plus an optional `mcp_allow` list of native
server names. Owner Swarm servers are identified by transport, not by name
(`eliot_core::is_owner_swarm_server`, shared by every harness plugin in
every policy, regardless of name or allow list): a stdio server whose
command resolves to a Swarm executable -- by file name `swarm`/`swarm.exe`
or the active controller binary path -- with arguments containing `mcp` but
not `--require-agent`, or an HTTP server whose URL host equals a configured
`cloud_mcp` hostname (the owner cloud endpoint carries Owner authority
through Access). The Agent-scoped server Eliot generates is the only Swarm
server a worker gets; it always carries `--require-agent`, so the identity
rule never matches it. Swarm MCP names (`eliot_swarm`, `eliot-swarm` and the
per-session prefix) stay reserved in every policy and can never be
allow-listed, but the rule applies whatever the entry is named: a renamed
owner entry is still the owner daemon identity. A worker that talks to the
owner daemon with the owner identity is an authority leak.

```toml
[profiles.codex-minimal]
harness = "codex"
model = "gpt-6-luna"
mcp = "minimal"
mcp_allow = ["owner-search"]
```

The resolved policy and allow list are frozen in the profile snapshot
(`mcp`, `mcp_allow`, plus the configured cloud hostname as
`mcp_cloud_hostname`; new freezes always record `mcp` explicitly) and the
requested policy is reported in secret-free diagnostics and effective
settings. Frozen snapshots without the key keep their recorded meaning
(`inherit`); snapshots that predate `mcp_cloud_hostname` read it back as
absent, leaving the identity HTTP arm inert. Unknown policies and reserved
Swarm names in `mcp_allow` are rejected at config load, and a non-empty
allow list requires `minimal`.

`inherit` is an explicit opt-in to the previous behavior: the worker receives
Eliot's Agent-scoped server in addition to the owner native MCP surface,
minus every owner Swarm identity. `minimal` starts only the intended MCP
surface: Eliot's Agent-scoped server plus the explicitly allowed native
servers. By harness:

- Codex enumerates the effective `mcp_servers` surface pre-spawn in both
  policies and disables every owner-identity match with a
  transport-carrying per-run `-c` override (exact owner names stay covered
  by synthetic transport-carrying stubs as backstop). An allow-listed name
  that matches the identity is rejected at launch (`profile_error`)
  instead of re-admitted. An entry whose argument vector the native layer
  does not report counts as unknown and fails closed toward suppression;
  an absent vector means the entry carries no argv. `minimal` additionally
  disables every other non-allowed entry, the known MCP-serving bundled
  plugins (`computer-use`, `unified-computer-use`, `codex-app-tools`) when
  present and enabled, and the ChatGPT-backed Apps runtime (`codex_apps`)
  by a synthetic transport-carrying disable. An explicit `mcp_allow` entry
  for a plugin-backed server, plugin, or `codex_apps` keeps it enabled.
- Claude `minimal` passes the Eliot-only per-run config plus
  `--strict-mcp-config`, which ignores all other MCP configurations.
  Claude `inherit` keeps the normal CLI precedence (installed 2.1.325
  offers no per-server disable or deny switch for the per-run surface),
  so the owner-side sources -- project `.mcp.json`, project and local
  `.claude/settings*.json`, user `~/.claude/settings.json` and legacy
  `~/.claude.json` -- are scanned pre-spawn for owner-identity entries
  and a match refuses the start (`profile_error`, naming the server and
  the source kind).
- OpenCode serves the isolated per-Agent config (only the Agent-scoped
  server) under isolated `XDG_CONFIG_HOME`/`XDG_DATA_HOME`, but the
  workspace project layer (`opencode.json`/`opencode.jsonc`) still merges
  natively on the cloud path, so every start scans that layer pre-create
  (pre-touch on resume) for owner-identity entries and refuses the start on
  a match. Fragment sessions additionally run under the native
  project-config disable switch, so project layers never load there; the
  file scan still runs as a fail-closed backstop.
  The settled inventory is then gated: `minimal` refuses any serving
  unexpected server, `inherit` refuses reserved Swarm names, and
  `mcp_servers_verified` is reported only after the gate passes with the
  Agent-scoped entry connected.
- Antigravity defaults to `minimal`. Before spawn the plugin writes only allowed
  native MCP definitions to an Agent-owned `<plugins>/<Agent>-agy/config/mcp_config.json`,
  selects that root with agy 1.5.10's `acp --gemini-dir`, and supplies the Swarm
  Agent bridge through ACP session setup. This native root persists across
  generations for exact session resume and is part of the native scope; legacy
  `inherit` sessions retain their original root. Sign-in uses the OS keyring
  without copying credentials. The MCP file is removed after native exit.
  `mcp_allow` selects enabled names from the native global/workspace MCP files;
  missing names or owner Swarm identities fail startup. Workspace definitions
  outside that list cannot be suppressed by these flags and reject as
  `unsupported` before spawn. Workspace plugin sources and personal customization
  directory redirects also reject as unsupported rather than claiming isolation
  over unenumerated sources. Explicit or legacy `inherit` keeps native discovery,
  with pre-spawn owner Swarm checks and refusal of unenumerated plugin sources.
  Policy and allow list are reported separately from the unknown ACP inventory.

The file-based MCP scans read regular files only, with a 256 KiB cap per source.
Missing sources contribute no entries. Unreadable, oversized or malformed sources
fail startup with `profile_error`. Plugin reads of materialized skill text use the
same regular-file check with a 64 KiB cap.

No policy edits the owner global native config or copies
`auth.json`; no project or user file is ever edited by these scans. No
prompt-surface flags are pinned: the policy is a tool-surface policy and
must not silently change worker capabilities or safety context. Eliot's scoped
`swarm mcp --require-agent` server is always present in both
policies. See the owning harness pages. Requested vs
observed: the
effective server set is reported from native readback only where the harness
supports it (`mcp_servers` with `mcp_servers_verified`); otherwise it stays
unknown (`null` / unverified), never an echo of the requested set. Codex
`minimal` verification fails closed on any unexpected server in any
non-disabled state: the synthetic `codex_apps` disable and the bundled
plugin suppressions make those runtimes report `disabled` (absent from the
inventory), so anything else unexpected fails the start.

## Owner workspace setup

Global profiles can define ordered setup entries. They run when an explicit
`launch --worktree` or a managed manager launch prepares a new worktree. Workers
borrowing a parent's workspace and explicit manager adoption reuse the existing
environment without running setup. Other launches with setup require a new
worktree. Profile loading performs no setup. The raw owner format is:

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
Missing or changed references fail before execution. The workspace root itself must
be startable as a Windows child-process working directory: roots at or past the
process-creation limit (259+ characters on the qualification host, os error 267)
fail the launch with the preserved OS error and no admitted command, before any
native spawn. Do not copy or substitute scripts
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
workspace = "worktree"

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
auto_cleanup = false
```

`max_event_backlog` is a positive signed 64-bit integer. It defaults to `100000`; zero,
negative, wrong-type and unknown automation values are invalid. `auto_cleanup` is an
Owner setting that defaults to `false`. When enabled, the existing automation
reconciliation timer performs bounded, paginated worktree selection at most once per
minute, applying eligible merged worktrees through the same cleanup plan and apply path
used by the cleanup UI and CLI. Eligibility and the durable removal fence are rechecked
before removal.

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

Retention is visible without changing its semantics. `daemon.status` reports
a retention view: the `eliot.db` main and WAL file sizes in bytes, the three
configured windows (`null` is unset, i.e. unbounded for that side), and the
retained-event watermark `pruned_through`. `unbounded` is true only when all
three windows are unset — the default daemon never prunes. When retention is
unbounded and the combined store size passes 100 MiB, the view carries a
bounded `size_advisory` telling the owner to set `daemon.retention` windows;
the hourly maintenance tick logs one `daemon.retention_unbounded_store_large`
line for the same condition. The sizes are informational and never gate reads;
changing retention windows is out of scope for this advisory.

The automatic attention no-progress suspicion window is optional:

```toml
[daemon]
attention_suspicion_secs = 900
```

It defaults to 900 seconds and must be a positive signed 64-bit integer whose
millisecond value fits in `i64`. This setting only controls when a busy Agent
with a healthy native source and no known request/tool activity is diagnosed;
source-health failures are diagnosed promptly regardless of this window.

Observe-what-matters thresholds are optional:

```toml
[daemon]
heavy_wall_secs = 30
heavy_cpu_secs = 60
heavy_rss_bytes = 1073741824
session_growth_ratio_permille = 800
session_file_bytes = 67108864
```

A child-process tree whose wall time, consumed CPU or peak RSS crosses any one
heavy threshold closes as one `activity.heavy` event (defaults: 30 s wall, 60 s CPU,
1 GiB RSS). Session growth warns at 0.8 context occupancy or a 64 MiB native session
file where the harness exposes a documented size source; there is no auto-compact.
`swarm doctor` compares live installed harness versions and contract markers against
the pinned `VERIFIED_HARNESSES` table without blocking launch.

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

Owner-global `[projects.ID]` entries register projects. On an Owner open or launch in
an unregistered Git checkout, S2 creates an entry through the settings writer,
recorded as a machine change. Its profile allowlist is the existing profiles;
its action allowlist grants ordinary launch/send only, without script/interpreter,
Git fetch or additional automation authority. For a linked worktree, the
canonical Git common directory identifies its repository registration. Discovery
prefers the registered primary checkout even when a legacy registration names the
worktree itself. Without a registered primary checkout, discovery can reuse a
sole registration for the common directory. Ambiguous matches refuse discovery.
Reuse returns that registration's checkout and exact allowlists, preserves the
Agent's workspace, and adds no settings entry. A launch records the selected
registration ID independently of its execution workspace and authorization scope.
Child launches inherit the parent's recorded project and authorization scope.
Workspace inventory is read-only. Daemon startup performs no project
discovery. Registration runs through an explicit Owner open (`project.discover`),
an Owner launch in the checkout, or the desktop's workspace checkout.
`path` names the canonical
Git checkout; `profiles` and `actions` are explicit allowlists over existing global
profiles and implemented actions. Missing or empty allowlists deny all.
`workspace` selects managed-worktree policy for top-level launches in the
project: `worktree` (the default for registered projects) prepares one standard
worktree per top-level launch without any flag, while `shared` keeps sharing
the registered checkout as before. Any other value is invalid. Project
registration readback uses the canonical Core [`ProjectRegistrationView`](../../crates/eliot-core/src/api/agent_views.rs);
project identity includes the canonical checkout and Git common directory. The registration ID names nothing a person reads: a project's name is the
owner's `[desktop] display_names` entry, else the repository name of its `origin` remote
(the readback's read-only `origin`, [api](api.md)), else the folder of the repository's
main checkout. The daemon readback identifies the main checkout even when Git metadata
lives in a separate directory. A linked worktree is a subproject of its repository,
named by the branch from the daemon's verified checkout readback when available.
The sidebar places an Agent by its recorded registration ID, including after its
worktree is removed. For legacy path labels it uses any retained managed-worktree
common directory or an exact, currently verified checkout path. Unresolved legacy
paths appear in **Other locations**. This is a display derivation and leaves
historical Agent rows intact.

The registration ID is the stable Project identity. Owner settings own its
registration lifecycle, display name, registered repository, allowed profiles and
actions, and future budget policy. The initial managed path uses one primary
repository, identified by its verified local Git common directory. Origin is display
metadata. Additional repositories and independent assignments sharing a repository
require explicit policy. The [manager assignment](coordination.md#project-manager-assignment)
is current Store state keyed by registration ID. Removing registration revokes new
scoped control and retains execution history. Display changes leave authority intact.
Until a registration is explicitly adopted, its Agents retain legacy authorization.

For future managed workspace selection, resolve an explicit permitted project,
then inherited assignment or parent, then an unambiguous verified repository match.
Ambiguity leaves work pending or unassigned until resolved. A private no-project
chat is an explicit execution mode.

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

Project configuration changes use the private global settings writer. History
attributes discovery to `project_discovery` and Owner cleanup of stale registrations
to `project_cleanup`. Invalid
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
leaves other `pass_env` resolution unchanged. When `pass_env` admits the
literal name `CARGO_TARGET_DIR`, admission also freezes its exact value in
`profile_snapshot.target_dir` (absolute local path only; skipped otherwise).
Cleanup backfills that frozen directory as a build artifact when it lies
outside the Agent workspace and inside its durably bound artifact root. A
shared daemon-wide target is kept rather than attributed to an Agent. Agents
should place their own target under `ELIOT_ARTIFACT_ROOT` and register it through
`artifact.register`; targets containing hard-linked files stay `keep(hard_link)`.

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

Claude and XDG store selectors and private provider selectors are retained
only in the opaque `native_route_identity`. The daemon verifies admitted
selector states against the private process environment before using a
captured route. Legacy snapshots lack that verification and bypass native
read caches. Codex retains its existing execution inputs in
`pass_env_values`. Public catalog scope omits store paths.

For a new Codex launch, resolve the effective store only after the actual workspace is
prepared, including setup. The owned read-only
[`profile.store`](../../crates/eliot-plugin/src/protocol.rs) reader uses that workspace
and the admitted inputs. For `minimal`, that same config response supplies the MCP
server/plugin projection; Store freezes it as command-bound `native_launch_metadata`
with the binding. It is consumed only by that launch, never cached across commands
or used to select a resume's store. A child's fresh generation-zero dispatch replaces
inherited parent launch metadata with its own command-bound projection; inherited
profile and store-selection inputs remain frozen. This metadata reader issues no `model/list`, entitlement or
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
  literal_env, pass_env_values, native_route_identity?, secret_env_names, inherit_env,
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
the settings for the next native turn, a monotonically increasing
`config_revision` for the Agent generation, and `application:\"next_turn\"`.
Only `model` and `effort` are supported; authority-adjacent fields are
rejected outright and anything else is `unsupported`. Only the Owner may
submit it.

Codex carries both values as per-turn `turn/start` overrides
(`TurnStartParams.model`/`effort`); Muse carries effort through per-turn
`reasoningEffort` and switches the live session model at configure time
(`session/setModel` with `session/read` proof, between turns only). Muse
keeps the last configured value for omitted fields, with initial values
from the native session and frozen profile. The frozen profile supplies
the native resume effort. Claude commits the accepted selector through
the stream-json `set_model` control request between turns, with a native
permission-mode check before and after the setter. Effort is launch-only.
Other omitted fields use the frozen original. OpenCode switches the
retained session model at configure time (`POST /api/session/{id}/model`
with a `session.get` proof; the requested model must be in the live model
catalogue or the command rejects before any write, and effort has no native
knob and is refused). Antigravity switches `model` and `thought_level` at
configure time (`session/set_config_option` with echo proof; unadvertised
values reject before any write). A mid-turn configure refuses before any
write. A catalog or quota failure at the next turn fails that
turn honestly; it never selects another model or account. The cached
readback behind that rule is
`harness.models` (bounded per-route/account catalogue with freshness and
credential identity) and the separate `harness.quota` (timestamp, scope and
an explicit supported/unsupported/unknown status with provider-verbatim
`details` when supported: Codex `account/rateLimits/read` plus
`account/usage/read` when useful, Muse `usage/read`; never secrets or
personal data, deduplicated per account); neither performs an inference
probe nor polls per Agent.

Each admitted command that executes natively under a configuration
(`launch`, `resume`, `send`) records which configuration it runs under: the
retained profile-snapshot identity plus the overlay `config_revision`
stamped on its command row (`launch` and `resume` without an overlay stay at
0; `send` freezes the revision current at its own dispatch). A send committed
before an overlay never runs under newer configuration, and a send admitted
after it dispatches under it. Each turn records the revision it ran under.
A config-file reload never rewrites the meaning of a retained generation, a
pending command, or an unknown-outcome resolution: effective configuration
always resolves from the retained command revision and the retained
`agent_config` rows, never from the reloaded file. The settings lane is
ordered by shared admission order: a configure does not apply while an
earlier-admitted send is still queued or undispatched, an earlier queued send
passes a later configure, and a later input waits for an earlier configure;
the Router redispatches on lane release. When the native turn request carries
a resolved effort the overlay left absent, the turn association records it on
the overlay's effective values while the requested values stay exactly what
the owner asked.

A `resume` starts its new generation by explicitly inheriting the previous
generation's latest overlay as its own revision 1 with provenance
`inherited_from` (same requested and effective values, `command_id` naming
the resume command); the frozen snapshot itself is unchanged. Generations
without an overlay start at revision 0 with no row. The inherited row is the
new generation's honest starting configuration: the next send runs under it,
and the readback shows it. On OpenCode and Antigravity the retained native
session already runs the inherited effective values (sticky native session
state), so resume carries them as the native realignment target and proves
the session runs the inherited revision instead of reverting to the frozen
snapshot; a resume without an overlay realigns to the frozen profile
exactly as before. Retention keeps every command a retained overlay
references, including the resume command of an inherited revision and the
source configure it inherited from: pruning never deletes a command named by
`agent_config`, so configuration provenance survives expiry. `swarm show`
(the `agent.get` view) reports the retained profile name, the launch-time
`profile_inherited` decision (recorded at launch freeze and exposed
independently of overlays: true only for an Agent child that reused its own
parent's snapshot verbatim, never for an Owner launch that resolves the same
profile), the overlay provenance (`configure` or `inherited_from`), the
revision, the requested overrides, the effective values, and the original
frozen values together; turn and command readbacks carry their retained
`config_revision` plus the full configuration view (requested and effective
model/effort, revision, provenance, profile name) resolved from the exact
retained generation and revision for the same original-request versus
resolved-configuration distinction.

## Unified settings

`<data_dir>/config.toml` is the single settings source. `settings.read` and
`settings.save` expose typed, explicitly named sections: profiles, routing, projects,
daemon (including limits and retention), automation, mail, cloud_mcp, desktop,
accounts and proxies.
Rules/scripts remain files owned by their existing APIs; settings references and
automation policy do not create a second executor. Desktop preferences use
`[desktop]`: `theme` (`system`, `light`, `dark`), `window` (`width`/`height`, at least
640 by 480), `startup` (`new_task`, or `last_conversation` to reopen the most
recently active Agent's conversation), `favorite_models` (bounded list of
`{harness, model, effort?}`, at most 50), `recent_models` (bounded most-recent-first
list of `{harness, model, effort?}`, at most 20), `collapsed_projects` (bounded
list of the sidebar's project keys, at most 100), `last_access` (`ask`, `auto` or `full`),
`model_visibility` (the picker's "Manage models" choices: a bounded list of
`{harness, model, visible}` or `{harness, provider, visible}`, at most 500; a model
entry overrides its provider's entry, and a model without either is shown),
`collapsed_model_groups` (bounded list of collapsed picker group keys, `<harness>` or
`<harness>/<provider>`, at most 100) and `display_names` (bounded list of
`{key, name}`, at most 200: `key` is `project:<sidebar project key>` or
`agent:<Agent id>`, `name` is the owner's display name of 1–120 characters; it
renames what the sidebar shows and changes no project registration or Agent record).
Picker strings are stored verbatim: harness and access values must match their
closed vocabularies exactly. Model ids, provider names, collapsed project and group
keys and display-name keys are 1–256 bytes, and effort is 1–128 bytes; these reject
controls and surrounding whitespace.
The desktop keeps no other preference store. The desktop writes `window` back once when the window closes, and only when the size changed.
The new-task composer writes `recent_models` after an admitted model launch,
`favorite_models` from the model picker (Ctrl+F or the row's star), `last_access`
from the access chip or Settings › Default access, and `collapsed_projects` when a
sidebar project is collapsed or expanded; a project key longer than 256
bytes is not remembered. The picker writes `collapsed_model_groups` when a group is
collapsed or expanded and `model_visibility` from Manage models (a group toggle is one
provider entry); the sidebar's Rename writes `display_names`.
If that table is absent, the desktop imports the legacy per-user
`swarm-desktop.json` preferences through the revision-guarded settings writer once.
It retires the legacy file only after successful publication; an existing `[desktop]`
table takes precedence.

The API reuses the profile editor's bounded source, SHA-256 revision, selected-section
splice, strict parser, secret checks, dry run and atomic private-file replacement.
Unselected sections keep their bytes and semantic values. A stale revision is
`conflict`; invalid or secret-bearing drafts never replace the file. Unsupported
later-layer fields fail validation. Legacy `config.profiles.*` uses this same writer.
Every proxy is validated independently, including unused routes. Candidate projects
resolve to canonical Git identities before both dry-run success and publication; a
candidate that cannot be resolved, such as a missing folder, is refused as `invalid`,
never as `unavailable`, because nothing was written.
Resolved registrations carry their source revision, rechecked under the settings
writer lock; publication and Store reconciliation hold that lock and the Store writer
without asynchronous Git work between them. A delayed save cannot restore older
project authority after a newer settings save. Automation saves update the
[assignment timer admission pause](automation.md#targetless-launch)
under the Store writer before acknowledgement.
Before publishing an Owner save that removes project registrations, the daemon
checks each removed registration under the same Store lock used by Agent admission.
The check covers the checkout, verified linked worktrees, retained managed-worktree
identity, and unsettled activity. A registration in use refuses the entire save with
`project_in_use` and leaves the settings revision and registrations intact.
The desktop's one-click removal of a missing checkout uses the project registration
candidate in `cleanup.plan` and `cleanup.apply`, which rechecks absence and usage
while holding the Store lock.
An uncertain post-replacement failure is `unknown`; read the revision before retrying.

A stale revision writes nothing. An editor such as the desktop sends only the sections
the owner edited, and it re-applies a change refused as `conflict` automatically only
when a fresh read shows every edited section still holding the value the edit started
from. Then only another writer's other sections moved, and they are kept: the merge
loses nothing. An edit made while the editor's own save of that section was in flight,
or of unknown outcome, started from that save's value: the read must hold exactly what
that save stored, as the store normalizes it. Having saved the section is not enough.
When an edited section itself changed, the change is never re-applied: the editor
shows the refusal and the stored values. The same rule applies when a read
shows that another writer changed a section with an unsaved edit: the edit is dropped
with the same refusal, so it can never overwrite that writer's value. A definite
refusal, such as an invalid value, leaves the revision unchanged. The assignment
form stays open until its save is confirmed and keeps its inputs beside a refusal.
Other forms show stored values under the error. A raw TOML or JSON editor keeps
its draft for correction.

Every actual save records old/new revisions, changed sections, time and authenticated
author in bounded Store history. Public callers cannot claim `bootstrap` or another
principal. Owner CLI and desktop identify their client source; Agent/automation
identity comes from authentication. Settings mutation remains Owner-only in S1;
machine bootstrap/project registration is an internal authorized operation. History
contains metadata, not raw settings, credentials or environment values. Retain at most
100 entries. Prepare a pending history row before file publication, then settle it
after replacement. Reconcile pending rows against the exact old/new file hashes;
an unrelated current revision leaves the outcome unknown. File publication and SQLite
are separate durability domains: failed history settlement is reported as unknown,
never as an unchanged file or permission to repeat a write blindly.

Profiles, projects and routing affect future admissions; running/queued work keeps its
frozen snapshot. Desktop preferences apply on read/save without daemon restart.
Retention is reread by maintenance. Automation backlog limits reload live and global
Run grants are reread before effects; automation changes require no daemon restart.
Daemon worker limits, mail and cloud MCP listener/auth settings require restart in S1: their workers, bounded
channels and listener ownership are constructed at startup. Save replies identify
sections requiring restart. Admitted profile/route snapshots remain frozen; live
authority checks continue to apply at their owning effect boundaries.

### Routing policy (S3)

`[routing]` holds ordered profile candidates by role (`default`, `manager`, `worker`,
`audit`, with additional named roles). Omitted `--profile` requests that policy.
Each role is an ordered array of existing profile names, for example:

```toml
[routing]
default = ["codex-luna", "muse"]
worker = ["codex-luna"]
```

Role names use letters, digits, `-` and `_`; each role allows at most 64 candidates and rejects duplicate or
unknown profile references. The `--role` launch argument selects the role only
when `--profile` is omitted; omission stays absent on the wire and means
`default`. An explicit profile continues to bypass routing.
At first admission, candidates must be installed, outside provider holds, not known
quota-exhausted where passive readback is supported, and within configured capacity.
Unknown quota is reported as unknown rather than fabricated availability. Freeze the
chosen profile and selection reason on the launch command/event before dispatch.
The retained [`RoutingDecision` and `RouteSkip` DTOs](../../crates/eliot-core/src/routing.rs)
carry the role, optional selected profile, source and reason, ordered typed skips and
quota readback status (`supported`, `unsupported` or `unknown`). Exhaustion is a
typed skip reason, separate from whether the provider supplied quota readback. A decision
with no selected profile lists the candidates that were skipped; an unconfigured
Owner role or an explicit nondefault child role remains a routing miss. A child
requesting an unconfigured default role uses its inherited profile as the sole candidate.
Child selection is intersected with the parent's frozen authorized child profiles
(#398), including the inherited profile where authorized. No candidate means a typed
`routing_unavailable` refusal with an `Error.routing` decision. The choice is retained
on the Agent as `profile_choice` for `show`, `result` and desktop API consumers even
after launch command history expires. `routing.preview` (Owner) runs the same
selection for a role without a command and returns its `RoutingDecision`; it admits,
freezes and reserves nothing, so the next launch evaluates again and may differ. The
desktop shows it on the composer's Auto chip. Same-ID replay returns the frozen choice;
failure, hold or unknown delivery never causes another candidate to run. A held
candidate never selects a different account or captured credential selector of the
same harness. Provider holds and capacity are rechecked transactionally at admission;
a concurrent state change can refuse an otherwise prepared choice before dispatch.

### CLI editing

`swarm settings read` returns the current revision, sections, restart requirements
and bounded metadata history. To change only desktop preferences, save a JSON request
with `swarm settings save request.json` (add `--dry-run` to validate only):

```json
{
  "expected_revision": "<revision from settings.read>",
  "sections": {"desktop": {"theme": "dark"}},
  "client": "cli"
}
```

Each named section is a complete replacement; leave unmodified sections out of the
request. The desktop Settings screen uses the same read/save contract and follows the
stale-revision rule of [unified settings](#unified-settings).
The desktop adds a project by choosing its checkout with the native folder picker;
it derives a valid, collision-free ID from the folder name and immediately saves
only the projects section through that same revision-checked writer. The Auto model
row links directly to Routing; reordering candidates uses the same immediate writer.

## Legacy profile-only editing (`config.profiles.*`)

`settings.save` is the primary editing path and covers every section; `config.profiles.*`
is the legacy profile-only path over the same writer (see
[unified settings](#unified-settings)). Launch admission resolves its profile from
`config.toml` at admission (above), so the saved file is what the next launch uses and
running or queued work keeps its frozen snapshot; only a successful `configure` changes
a live Agent's next-turn model/effort. Daemon settings loaded once at startup (`[daemon]`
worker limits and `[mail]`) still require a restart after the save (see [unified
settings](#unified-settings)).


`config.profiles.read` returns the file revision (lowercase hex SHA-256 of its bytes)
and the verbatim text of the `[profiles.*]` table blocks in file order. Every other
section stays server-side. Comment and blank lines that end a profile block and precede
another section belong to that section. The source is withheld (`editable=false` with a
reason) when the file is not valid TOML, when profiles are defined outside
`[profiles.*]` tables, when the profile text contains a value the daemon's secret
policy knows, or when the profiles fail the daemon's strict field and URL
validation (unknown fields and credential-bearing URLs are never handed to the
editor as safe source). Profile sections carry environment and credential names, never values.

[`config.profiles.save`](../../crates/eliot-daemon/src/config/profiles_edit.rs) takes
the full replacement text for those blocks, the revision it was read from, and
`dry_run`. It accepts a draft only when all of these hold:

- the draft holds only `[profiles.*]` tables;
- splicing it in place of the old blocks keeps the text of every other section in
  place (only boundary newlines may be added) and every non-profile value unchanged;
- the strict config parser, launch-time resolution of every profile (including child
  profiles and route binding) and project validation accept the result, within the
  128 KiB bound;
- the file still has the expected revision, otherwise `conflict`.

A draft carrying the redaction marker fails request validation (`invalid`). A known
secret value, in the raw text or in any decoded value, is `not_allowed`. A dry run
stops after validation. A save rechecks the revision immediately before it writes a
private temporary file in the data directory and renames it over `config.toml`. Only
saves inside one daemon are serialized, so an external writer in the instant between
that recheck and the rename can still be overwritten. A failure after the rename
reports `unknown`, never "unchanged". The draft keeps the file's line ending. A
leading UTF-8 BOM is preserved outside the profile blocks and is never handed
to the editor. An identical result writes nothing. An invalid draft is never written, so the file keeps
its last valid revision and the editor keeps the draft. The daemon keeps one revision-keyed validated snapshot per router: an admission
whose file revision matches reuses it, and any byte change re-reads and re-validates,
so the saved file is still what the next launch uses.

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
overriding ambient/profile, alongside `ELIOT_AGENT_ID`/`ELIOT_AGENT_GENERATION`/`ELIOT_AGENT_TOKEN`
and the normalized issuing data directory `ELIOT_AGENT_DATA_DIR`. The CLI uses the
inherited Agent identity only when the resolved target data directory (`--data-dir`, else
`ELIOT_HOME`, else default) equals `ELIOT_AGENT_DATA_DIR` (lexically normalized, compared
case-insensitively on Windows); otherwise it connects as the local Owner of the target
data directory and never sends the foreign token. An incomplete Agent identity for the
same data directory still rejects and never falls back to Owner authority.
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
freezes the scope names and configured values. Cross-harness children reserve
their own scopes and inherited parent
account/proxy scopes together, so multiple distinct scopes of one kind are allowed;
duplicate scope identities reject. The daemon-wide `global:swarm` scope, the per-registered-project
`project:<id>` scope and the per-project `manager_assignment:<id>` scope join
the profile, account and proxy scopes. The Owner sets their bounds in the one
config store:

```toml
[capacity]
global_max_live = 3

[capacity.projects.app]
max_live = 2
manager_max_live = 2
manager_min_live = 1
manager_priority_max = 3
```

A missing bound is unlimited; zero disables new admissions to that scope.
The Store loads the current daemon-owned limits inside the reservation
transaction for each fresh start, including resume, managed manager launch
and automated launch. This read follows workspace and rule preparation.
Profile, account and proxy bounds remain frozen in the native profile and
are the capacity bounds used in routing candidate checks.

The unit is a controlled generation or reservation, including an idle
manager. Every reservation counts in the fleet and its execution project.
Assignment usage follows the current assigned manager generation and its
team. Releasing the assignment removes that team's charge to the assignment
scope while its reservations still count in the project and fleet. Overview
uses the same reservation membership as admission.

A full scope refuses admission with `capacity_unavailable` and names the
blocking scope and its Owner setting in `config.toml`. A directly refused
request creates no command, Agent, event or reservation and can be submitted
with the same ID after capacity changes. A scheduled assignment or explicit launch
intent in the arbiter retains its place and wait reason. The wait readback names the
blocking scope and who can change it. Profile, account, proxy, project, global,
workspace and transfer gates are Owner-controlled. The current manager can change
a manager-assignment limit only within the Owner's delegated range.

`manager_min_live` enables adjustment within the inclusive range through
`manager_max_live`. A current manager can release unused budget or request an
increase when the project and daemon have room. `manager_priority_max` delegates
priority from zero through that value. The Store binds both settings to the exact
manager generation and verifies the current assignment revision. A budget change
affects future eligibility. A slot is reserved when a launch is admitted.

Lowering a limit keeps current generations running and blocks new admissions until
usage drops. Retained unknown reservations keep occupying their scopes. An already admitted command retains its result under its ID, and
replay never starts another native generation or reservation. A capacity reservation is
released only after the owned generation is proven closed: the OS source closed
with every recorded process of the generation exited (at least one plugin or
harness), no native source open, and the plugin host's record that it saw the
plugin exit and drained its reader (a stop or lost transition closes sources
without a drain). The host record and the OS closure arrive in either order;
each re-checks the proof and the later one releases.
`capacity.released` reports `custody: "exact"` with `orphan_possible: false` for
Windows live releases, and `custody: "degraded"` with `orphan_possible: true`
on Unix, where escaped descendants cannot be excluded. Restart releases report
the custody guarantee and `orphan_possible` established by their recorded
closure evidence.
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
no new generation of this daemon is ever among them. A random custody namespace is bound
to the stable Store identity and canonical data directory (`custody_binding`). A copied
or moved Store re-mints the current namespace on open. Dispatch records the namespace
on each Agent generation, so retained generations keep their original names across
restarts and re-binding. See [custody identity](core.md#process-custody-identity).
Windows names each generation's kill-on-close Job
`Global\eliot-<namespace>-<agent>-<generation>` before the suspended plugin starts,
and never adopts an existing object of that name.

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
or account fallback. Billing discovery remains unsupported; provider quota reads back through `harness.quota` and usage totals through `usage.summary`.

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

## Logging, crash capture and diagnosis

`log_level` (`standard`, `debug`) applies live through the daemon settings
section, never as a restart gate on its own. Standard mode keeps operational
lines: structured request/settlement codes and bounded IDs, typed `where`, origin,
classified sanitized origin evidence and approved sanitized stderr tails. Neither
level persists `Error.message` or other unclassified free text. Debug mode adds numeric
timing records: every RPC at the router choke point (`rpc.handled` /
`rpc.failed` with method, elapsed milliseconds and ok), every
daemon-to-plugin call (`plugin.call.ok` / `plugin.call.error`), and plugin
frame, channel, process start/exit and stderr records over `ELIOT_DEBUG`.
Plugin `ELIOT_DEBUG` lines, including malformed or oversized telemetry, are
excluded from retained diagnostic tails in both modes. Only parsed metadata is
routed to the bounded rotated daemon log when the current level is `debug`.
Debug never logs payloads: ids are our own vocabulary, messages stay in the
sanitized store, params and results never enter a record. Settings publication
and live log-level application share the Store writer lock, preserving order.

On panic the daemon writes a `daemon.panicked` marker and plugins write a
`plugin.panicked` marker. Arbitrary payloads and unclassified thread names are
omitted before either disk or stderr; source location, approved thread name and
payload-free backtrace are sanitized and bounded first. Publication uses the
existing private temporary-file/write/sync/atomic-rename primitive (bounded
file/count); readers ignore temporary or incomplete files. The previous daemon
marker is consumed once and reported in `daemon.status`, in `swarm daemon`
startup output, and in `swarm why` when the diagnosed failure is otherwise
unclassified. The desktop Agent details view shows the same one-line
diagnosis summary; the full chain lives behind `swarm why`.

## Paths and platform defaults

Keep Windows paths short; use long-path Git arguments/environment without rewriting
repository config. Use a separate Cargo target directory per worktree. Executable, argv,
cwd and env remain separate. Launch/workspace admission never performs an implicit fetch.
All persisted fields and frames are bounded. The roadmap determines when each section
becomes implemented; absent later features return `unsupported`, not a hidden substitute.

## Private chats root

`[daemon].chats_root` is an optional absolute local root (at most 4096 bytes),
defaulting to `<user home>/Eliot/chats`. `settings.read` exposes the effective
root and `settings.save` replaces it with the daemon section; both resolve the
explicit and the default root through the same validator, so a stored root
inside the owner chat tree is refused at readback exactly as at launch. The one
exception is an explicit root equal to the canonical owner chat tree itself: the
Owner may choose it for a non-default data directory, such as a production
controller. Derived defaults never resolve there. When
the owner home cannot be resolved for a non-default data directory, validation
fails closed instead of omitting the owner tree from the protected set. It must
be outside the daemon data directory, harness homes and user configuration
roots, and cannot contain those roots. Before canonicalization or admission,
every existing component of the supplied path is checked for links and Windows
reparse points, including ancestors of a nonexistent suffix. The daemon validates
private directory ownership, rejects links/reparse traversal and creates a new
`aN` child exclusively for an Owner `launch` with `chat:true` and no workspace.
Native stores and owner configuration are never created there by Eliot. Changing
this setting affects future admissions; existing chat Agents retain their exact
workspace and directory identity. Cleanup uses that registered identity and the
existing artifact plan/apply path before Agent record deletion. A chat directory
is kept while another Agent record references its workspace, including a stopped
child; apply rechecks this under the Store lock before removing the directory.
