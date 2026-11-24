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
`--data-dir`. The data root must be a real directory owned by the current OS user;
symlink and reparse traversal is refused. Private files remain current-user-only.
Windows directory ACLs follow the narrow rule below.

**Packaged-process rule (Windows).** Windows redirects AppData writes from a
process inside an MSIX package to that package's private cache, and a shell such
an app starts can be redirected with no package identity of its own. Only
processes in that tree see the result; a daemon or CLI outside it would use
different files. Swarm detects this from where a transient delete-on-close probe
file lands, once per process and only for a path under AppData. It probes the
folder the path belongs to, because each is redirected separately: Local, Roaming
and LocalLow to the same-named folder of the package's `LocalCache`, and the
`AppData` directory itself to `LocalCache`. The probe has three answers:

- **Redirected:** a package cache holds the probe. Every use of a data root under
  those folders is refused, including the default root, before any ACL diagnosis,
  and so is writing the selected-root pointer.
- **Not redirected:** every package cache was inspected and none holds the probe.
  Nothing is refused.
- **Indeterminate:** the probe could not be created, or a cache could not be
  inspected and none held the probe. Opening a root whose owner token exists and
  read-only calls proceed, so one unreadable package cannot disconnect the desktop
  or CLI. Creating a new data root, an existing root's missing owner token or a
  stand directory under those folders and writing the selected-root pointer are
  refused with the probe error. The cost is that those creations stay blocked
  until the cause is fixed, and that a redirected process whose probe fails can
  still open what it sees as an existing root.

If the account's known folders cannot be read, no path can be classified: a root
whose owner token exists still opens, and any new data root, owner token, stand
directory or pointer write is refused with the lookup error, wherever it is. The
lookup is retried on the next call.

Remedies for a redirected caller never name an AppData location: use `--data-dir`
or `ELIOT_HOME` with a directory outside AppData. The desktop's fresh-directory
choice is created in the account profile when its usual location would be
refused, and a failed profile lookup is reported rather than falling back beside
the refused directory.

Linux is distributed as the [supported Docker image](../../docker/README.md).
It runs as UID/GID 10001 with `ELIOT_HOME=/data`, an owned mode-0700 volume;
existing unsafe mounts are refused without chmod/chown repair. Native executable
and workspace paths are absolute container paths. Credentials enter only through
explicit runtime mounts/environment allowlists, never image layers. The Docker
guide specifies per-vendor mounts and the risk of exposing credentials to Agents.
Keep the root short enough for `<data-dir>/daemon.sock` (107 UTF-8 bytes total).

On first use, create the directory with a protected current-user ACL before publishing
private files. Windows checks every existing component of the data-root path for
reparse points, then checks the root's owner and DACL through a handle. Ordinary
ancestor ACLs are outside this check. Each checked directory must belong to the
current Windows user. Its DACL must be present and protected from inheritance.
It contains exactly one basic allow ACE for that user with `FILE_ALL_ACCESS` and
directory inheritance flags
`OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE`, plus zero or more basic allow ACEs for
capability SIDs matching `S-1-15-3-*`. Each capability ACE is explicit (flags zero)
and grants exactly `FILE_TRAVERSE | SYNCHRONIZE` (`0x00100020`). No other ACEs
may remain in a repaired DACL. The daemon removes other entries after verifying
ownership. A private file belongs to the current user and has a protected DACL
containing exactly one non-inheritable basic allow ACE for that user with
`FILE_ALL_ACCESS`. `owner.token` and `daemon.lock` retain this protected rule.
SQLite private files also accept an already-existing DACL with a sole inherited
basic allow ACE for the current user granting `FILE_ALL_ACCESS`. The daemon's
data-directory manager validates this accepted ACL and never rewrites SQLite file
ACLs. All other deviations on existing private files fail closed: changing an ACL
cannot establish that existing bytes retained confidentiality or integrity.

A Windows startup repair of an existing data root first verifies its current owner
and independently verifies the existing `owner.token` file as a private regular file:
current-user owner, protected DACL, and exactly one non-inheritable current-user basic
allow ACE with `FILE_ALL_ACCESS`. This gate reads the marker's security metadata.
Startup uses a non-reparse directory handle opened through the verbatim path to
replace and read back the protected DACL. The current-user baseline and exact
capability ACEs described above must pass readback. Ownership stays with the
current user. If ownership, the marker, or ACL repair cannot be verified, startup
reports the exact refusal, including the offending SID and rights where available.
The daemon records removed grants in `daemon.log` and commits Owner notices with
the SID, rights mask, and path. Successful removal logs and Owner notices are
deduplicated by path, SID, and rights mask for that daemon run. Linux continues to
refuse unsafe mounts without chmod or chown repair.

The selected-root pointer directory must already satisfy the same protected
directory ACE rule, but is not automatically repaired because it is not daemon-owned.
The pointer file remains subject to the strict private-file rule. Windows rejects
reparse traversal through the root and pointer paths; ordinary ancestor ACLs are not
rewritten just because their path components are checked.

After acquiring the data-directory lock and before serving, startup repairs the
daemon-owned `logs/` and `skills/` ACLs. The active timer checks the root and the
existing `logs/`, `plugins/`, and `worktrees/` parent directories. It checks
each path every second until a failure or a recurring grant raises that path's
retry delay, starting at two seconds and doubling up to 30 seconds. Log rotation requires a validated
`logs/` ACL. Managed plugin homes and worktrees are repaired only when exact daemon
creation or retained provenance identifies the path and a safe lifecycle check permits
repair. Adopted workspaces and active worktree fences are outside this scope. Existing
descendants change only at their owned lifecycle checks or through normal NTFS
inheritance; the daemon does not recursively rewrite them. Eliot private files are
validated under the file rules above; ordinary worktree files are not treated as
private data.
An isolated managed-folder repair failure leaves the daemon serving while its
root remains valid. The daemon logs failures at the retry cadence. It stops
serving on a root owner mismatch or an unrepairable root. Transient root failures
are retried with backoff. The daemon validates existing credentials, the lock,
and SQLite files before root ACL repair so directory inheritance cannot hide a
file deviation. Startup and desktop
surfaces report that repair completed or give the exact refusal reason.
Existing roots are never copied or silently replaced. On refusal, the CLI offers one
explicit command selecting a fresh private per-user directory; the desktop offers it
as the last, explicitly named choice of its startup failure view (below). The user
must choose it. Existing roots and configuration are not overwritten by bootstrap.

The desktop's explicitly chosen fresh root is retained in a private per-user pointer:
`%LOCALAPPDATA%\Eliot-bootstrap\selected-data-dir` on Windows, or
`$XDG_CONFIG_HOME/eliot-bootstrap/selected-data-dir` (otherwise
`~/.config/eliot-bootstrap/selected-data-dir`) on Unix. The pointer lives outside the
possibly unsafe default data root. CLI and desktop share resolution order:
`--data-dir`, `ELIOT_HOME`, selected-root pointer, then the platform default. The
pointer directory and pointer file pass their separate private checks, and the target
passes the applicable data-root checks; failures never silently select another root.
Selection creates and validates the fresh root before publication.

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
loading settings. A start whose daemon is refused by the data-directory lock (or
endpoint) as `daemon_already_running` waits for the daemon that holds the root.

An Owner `daemon.shutdown` (drain or force) writes the private `owner-stop` marker
while the daemon still serves; the next daemon start clears it after taking the
lock. After its first connection, a desktop read that fails asks `daemon.status`
(repeated failing reads ask with growing pauses). When nothing answers and the
marker is present, the desktop starts nothing: the main area says Swarm was stopped
and offers Start, and the list's cadence asks `daemon.status` so a daemon started
elsewhere is reconnected. When nothing answers and there is no marker, the daemon
failed: the desktop runs its startup path again (it starts a daemon only when none
is reachable). A restart is tried again only for `unavailable`, `platform_io`,
`data_dir_unavailable` and `daemon_already_running`, at most five attempts with 1,
2, 4 and 8 s pauses; a notice above the composer counts them and gives the previous
attempt's reason, and the polls and the open conversation wait and read again once
the daemon answers. Closing the window cancels a start before it starts a daemon.
Any other failure, the last attempt, or a failed first start shows the daemon's own
reason and the choices in this order: Retry on the same data directory (the
default), Repair settings, Open log, Open the data folder, Use another data
directory (an existing private Swarm root) and, last, a new empty data directory
after a confirmation. A chosen or new directory is started first and written to the
selected-root pointer only once its daemon answers. The desktop does not validate
the daemon's configuration itself. `swarm-desktop --workspace PATH` opens and registers that checkout;
without the flag it registers nothing: the folder it was started from never becomes
a project implicitly. A later launch in a different checkout registers that
workspace through the same machine-author settings writer.

```text
<data_dir>/
  config.toml
  owner.token
  eliot.db, eliot.db-wal, eliot.db-shm
  daemon.lock, daemon.json, logs/daemon.log
  desktop.log              the desktop's start-up errors and panics (bounded, restarts past 256 KiB)
  owner-stop             present from an Owner `daemon.shutdown` until the next start
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
(`--data-dir`, else `ELIOT_HOME`, else default) reject as cross-root Agent calls;
they never use that directory's Owner identity or token. Incomplete Agent identity for
the issuing data directory also rejects. This is attribution within one OS user, not
hostile-same-user isolation. Tokens never appear in semantic
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
`model` or preserve the native default. `cmd` is optional for every adapter. When
omitted, a launch uses that harness's normal native executable discovery, shared
with its ad-hoc launch. An explicit `cmd` is an Owner-selected native executable
pin. First use writes visible, editable native-default profiles for installed
harnesses through the settings writer as `bootstrap`, leaving `cmd` unset. On a
fresh root,
the managers are the installed ones of `codex`, `claude` and `opencode` in that
order (the harnesses that can disable native subagents per session), otherwise the
first installed profile in name order. Each manager has `control = "tree"` and
`child_profiles` listing the other installed bootstrap profiles. Bootstrap writes
`native_subagents = "deny"` for managers whose harness can enforce it. If
Muse or Antigravity is the fallback manager, bootstrap leaves the field
omitted and its effective value is `allow`. The `default`
routing role lists the managers followed by the other installed profiles in name
order, so routing passes over one whose quota is used up or whose provider is on
hold and starts the task on the next. The `worker` routing role and each manager's
`child_profiles` list the other profiles in that same order (the `worker` role names
the first default when it is the only installed harness). This makes Swarm delegation available on first use; existing
profiles and routing choices are preserved. An omitted model
in these profiles requests the native default and retains requested/effective
readback separately, as for an ad-hoc launch. There is no builtin fake profile.
Bootstrap detects the installed harnesses and writes profiles with `cmd` omitted.
Muse follows the launcher-current binary at launch. On Windows the adapter
also asks the vendor launcher to check for updates when its interval is due.
The next launch resolves the current binary again. An empty catalogue can
prevent model selection and is reported by the Muse launch path.
Before admission, an omitted profile selects the first available ordered role candidate.
The [routing policy](#routing-policy-s3) governs continuation after a confirmed provider
refusal. Uncertain outcomes retain their original command bindings.
Profiles stay the durable
explicit record: ad-hoc Owner launch (`swarm launch --harness ...`) freezes an
equivalent snapshot from built-in per-harness safe defaults plus flags (see
below). For both forms, executable discovery runs for the launch and records the
selected absolute native path and available version metadata in that generation's
snapshot; it never writes the result back into the saved profile. A later launch
from an omitted-`cmd` profile discovers again. The harness pages
([Codex](../harnesses/codex.md), [Claude](../harnesses/claude-code.md),
[OpenCode](../harnesses/opencode.md), [Muse](../harnesses/muse.md),
[Antigravity](../harnesses/antigravity.md)) own their native mappings.
[The example config](../../config/controller.example.toml) shows explicit
profiles for Codex, Muse, Claude, OpenCode and Antigravity; its omitted
`native_subagents` fields use the defaults described below.

## First profile

```toml
[profiles.codex-luna]
harness = "codex"
model = "gpt-6-luna"
permissions = "workspace-write"
pass_env = ["CODEX_HOME"]
```

A profile may set an absolute context ceiling in tokens. The field is omitted by
default, so it adds no automatic session action until configured:

```toml
[profiles.manager-codex]
harness = "codex"
context_ceiling_tokens = 150000
```

`context_ceiling_tokens` must be a positive integer. The daemon compares it with the
latest valid `session.usage.context_used_tokens` sample for that Agent generation;
it does not use cumulative input tokens or a fraction of the model window. Profile
resolution freezes the ceiling in the generation's `profile_snapshot`, so a config
edit affects a later generation. At the next idle boundary, the daemon compacts when
the harness advertises native compaction. Otherwise it uses the same-profile safe
session switch and A6 handoff brief when the source has a retained successful task.
The replacement must retain the source generation's authority after normal launch
profile preparation. The brief alone does not trigger another transfer.
The `session_growth.switch` opt-in applies to legacy ratio/file-size warnings; it does
not gate a configured context ceiling. The existing
`daemon.attention.session_growth.enabled` setting still gates automatic reactions.

## Launch instruction delivery

Every new launch records a bounded inventory of instruction files from the actual
execution checkout after workspace preparation and setup, before the native start
is dispatched. This timing means a managed launch reads its final worktree, including
its selected base and setup output. It never substitutes a file from the donor checkout.
The launch facts are committed once and retained across held retries; a retry does not
rescan a changed checkout. Legacy Agents keep a null fact column and omit this optional
readback.

The inventory is limited to these seven paths: AGENTS.md, AGENTS.override.md,
CLAUDE.md, CLAUDE.local.md, .claude/CLAUDE.md, .claude/CLAUDE.local.md, and
GEMINI.md. It reports each relative path, file status, byte size when available,
full SHA-256 when read within the bound, native-read status, and source-delivery status.
Catalog reads are capped at 8 MiB total and use the same canonical workspace containment
and 8 MiB per-file limit as normal source delivery. File contents are not stored in the
launch facts.

The daemon prefers AGENTS.md as the primary file, then AGENTS.override.md,
CLAUDE.md, .claude/CLAUDE.md, GEMINI.md and the local Claude instruction files.
When the primary file's native reading isn't established, the daemon attaches it
through the existing required-sources renderer to the launch's linked first input.
The renderer puts file content in a [quoted data fence](coordination.md#quoted-data)
and labels SHA-256, redaction and completeness outside the fence.
Explicit user sources are read at admission from their requested workspace. The
catalog reuses an explicit attachment with the same workspace-relative path and file
digest as the prepared primary file. Otherwise automatic delivery reads the prepared
execution checkout. If the workspace, file, input, or source budget prevents
a delivery, the typed facts report that outcome. Launches without a first input
record the catalog without starting a separate model turn.

Native-read status is evidence-labeled. Documented means the vendor documents the
file convention. Partial means the file exceeds the documented or profile-selected
native byte limit. Conditional means documented behavior depends on settings, precedence
or exclusions that this launch did not prove; not_native means the active Eliot
adapter configuration does not enable that reader; unknown means no supported
conclusion. These statuses describe the adapter's configured reading behavior.
The Claude adapter reports `AGENTS.md` as `unknown` because prelaunch readback
does not establish whether the native instruction loader will select it. The
inventory retains the installed Claude version where available as provenance.
The managed Claude adapter leaves project memory
enabled, so `CLAUDE.md`, `.claude/CLAUDE.md` and `CLAUDE.local.md` use documented
native delivery. Only the separate print inference path passes empty
`--setting-sources`. Muse's current launch args omit `--trust-workspace`, so its
root AGENTS.md status is not_native. Codex CLI 0.167.1 defaults to 32 KiB for
project instruction content. A Codex profile may set `project_doc_max_bytes`
to pass a native override. The provider-probe route uses zero. The inventory
compares each Codex instruction file with that limit and delivers an oversized
primary through sources. OpenCode reads CLAUDE.md as a fallback. Antigravity
does not return project-instruction readback, so its status remains unknown.

agent.get returns the retained object as optional instruction_delivery; swarm show
prints the same typed readback. Missing fields from older Store rows remain absent.

## Shared owner rules

`[owner_rules].text` in the unified settings file is the single shared owner-rule
source. `settings.read` exposes the unified settings-file revision beside that text.
A launch freezes the revision and SHA-256 of the text before native dispatch.
Queued launches use the rules available when they start. A resume reuses the
retained digest, and a legacy Agent can capture the current setting when explicitly
reopened. Delivery resolves that digest from settings. If the text changes before
delivery or native reopen, the operation reports that the retained rules are
unavailable. Settings saves reject values detected by the secret policy, redaction
markers, and common credential forms such as bearer tokens and private keys. The
same validation applies to text read for delivery.

The Agent's Store record contains delivery metadata. `agent.get` and
`swarm show` expose revision, digest, delivery method, status and delivered flag.
The owner rules text stays out of the Agent and profile readbacks. The unified
settings read warns above 8 KiB of UTF-8 owner rules. The human-readable Agent view
warns when readable project instruction files total more than 4 KiB of UTF-8 data.
The first-input fallback enforces the existing 64 KiB persisted-text budget.

Codex receives the frozen text through native thread developer instructions.
Claude uses `--append-system-prompt-file` with an owned instance file. OpenCode v2
loads the instance's isolated `AGENTS.md`. The Muse and Antigravity adapters declare
first-input delivery. The daemon appends a fenced owner rules block after any
quoted project-source data, including when the first input is a later `send`.

A launch records `pending` independently of the linked input. A definite launch
failure, rejection or cancellation settles an unbound receipt for that generation
as `not_delivered` in the same Store transaction. A successful launch without input
keeps the receipt pending until the first eligible send. That send records
`offered` before native dispatch, whether that send is linked or submitted later.
The committed native association of the exact rules-bearing send records
`delivered=true`. A rejected input records `not_delivered`. An uncertain delivery
retains `unknown` after an Owner assertion of success. An Owner resolution to
`failed` allows a later independent send to bind the rules. Same-ID retries reuse
the prepared input. Child, rule and switch launches use the same capture path.

`swarm doctor` compares known global instruction files with the current source after
removing a leading UTF-8 BOM, converting CRLF to LF, and ignoring trailing LF
characters. It reports the harness, path and match status. Symlinks, reparse points,
non-regular files, read failures and files beyond its fixed read bound report
`unreadable`. The bound derives from the 64 KiB owner-rules byte cap and leaves room for
CRLF expansion, an optional BOM and trailing newlines, so line-ending normalization
does not truncate equivalent text. The comparison makes zero configuration writes.

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
defaults); broader authority stays profile-only, owner-written choice. A missing
`--cmd` uses the same harness-specific discovery as a profile with omitted `cmd`.
Launch preparation resolves and freezes the
absolute selected path and available `cmd_version` in that generation's snapshot.
It does not rewrite the profile. `swarm models`, `swarm harnesses`, and `swarm
doctor` may inspect installed candidates but do not freeze a launch selection.
An explicit `--cmd PATH` overrides discovery for this Owner launch; an explicit
profile `cmd` is the saved-profile equivalent. Claude also accepts `latest` for
automatic selection. A missing pin refuses launch and
never falls back silently. A new launch from an omitted-`cmd` profile discovers
again; an existing generation keeps its captured executable.

The shared resolver applies adapter-specific rules: Codex uses its installed
`codex` resolution; Claude considers native versioned roots and selects the
newest candidate that passes its bounded version check; OpenCode resolves its
package shim to the native executable; Muse follows the installed launcher
selection (or the native Unix payload); Antigravity selects the `agy` CLI and
never substitutes its IDE executable. Each harness page owns the candidate
paths and native identity checks. An absent or ambiguous automatic candidate
returns a typed `unsupported`/`profile_error` naming what was searched. An
explicit path that no longer exists returns a profile command issue. The refusal
and `swarm doctor` give the discovered replacement path when available and ask
the Owner to remove the exact `profiles.<name>.cmd` setting to restore automatic
discovery. Human `swarm status` identifies the affected profile, harness, missing
configured path and setting; it does not probe installations, so it points to
`swarm doctor` for expected-version and installed-candidate findings. `swarm doctor`
shows the adapter's expected version and either the discovered installed candidate
path/version or that no candidate was found. A detected install whose replacement path
is unavailable is reported separately from no install. Repair removes the profile's
`cmd` key and saves the Profiles section; the next launch follows the installed vendor
executable, while an existing Agent generation retains its frozen executable. Automatic
profiles with no installed executable remain visible in doctor's per-harness installed
inventory; they have no stale `cmd` pin to report. Ad-hoc Claude uses `mcp = "minimal"`.
See
[Claude Code discovery](../harnesses/claude-code.md#ad-hoc-launch).

OpenCode's `--auth` is
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
requested effort stays absent. The direct ChatGPT route uses `model/list` for
the selected model's advertised efforts. The Responses route passes the
explicit effort through to the native operation. The [plugin protocol](plugin-protocol.md)
owns catalogue omissions and verification.
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
native-store bindings are never copied across harnesses. An approved same-harness
profile can set its model, effort, and daemon context ceiling. Same-harness children
inherit captured credential bindings and the frozen executable. Control remains equal or narrower on
every harness; workspace and project
remain inherited. A cross-harness child retains its own capacity scopes and every
non-profile scope of the parent with the same bounds; conflicting bounds reject.
The parent's own profile scope is replaced by the child's. See the launch authority
check in [api](api.md) and [coordination](coordination.md#claims-and-hierarchy).

A profile selects its harness-native subagent surface in `native_subagents`:
`deny` asks the harness plugin to disable its native subagent tool for the
session so delegation goes through Swarm (`launch` with `role` or an allowed
profile, claims, mail, `wait`/`digest`). When omitted, it defaults to `deny`
for Codex, Claude and OpenCode, and to `allow` for Muse and Antigravity,
independent of `child_profiles`. An
explicit `native_subagents = "allow"` opts that profile into native subagents
and remains valid for audit or other profiles that rely on them. The frozen
value is captured in the profile snapshot and reported truthfully in effective
settings, the desktop Profiles view and `swarm why`. Ad-hoc Owner launches use the same
default. Resume uses the retained snapshot unchanged; the new default applies
when a new launch snapshot is frozen, not when an existing Agent resumes.

Muse and Antigravity have no verified per-session switch. A launch with
`deny` on either harness returns `unsupported` before native process start,
with a reason surfaced by `swarm why`. Their desktop switch shows the effective
`allow` default and cannot be switched off; an explicit deny from TOML still
fails. Codex, Claude, and OpenCode enforce `deny` on the session.
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

An Antigravity profile selects `harness = "antigravity"` with optional native
`cmd`, an optional exact `model`, an optional `mode` (`plan`, `auto` or `accept-edits`)
and an optional `dangerously_skip_permissions` flag (default false). `effort`,
`approval`, `collaboration_mode`, `permissions` and `approval_policy` presets have
no mapping and are rejected. The effective permission mode and effective model come
from native init facts; init does not establish the served model. See the
[Antigravity page](../harnesses/antigravity.md).

A Muse profile selects `harness = "muse"` with optional `cmd`, exact
`model`, `effort`, `approval`, and `permissions` (`workspace-write`, the default,
or `danger-full-access`; `read-only` remains unsupported). Sandbox posture is fixed at
host birth (`--disable-sandbox` for `danger-full-access`); the MSP session wire has no
sandbox or permissions readback field and is not negotiable per session, so effective
permissions stay unknown (`null` / unverified) until a real native tool probe confirms
posture. An explicit model is selected at host startup so Muse can resolve
the provider profile, and resume also sends the exact selector to the retained
session. `collaboration_mode` and `approval_policy`
presets stay rejected. See the [Muse page](../harnesses/muse.md).

An OpenCode custom-provider profile points one profile at a local
OpenAI-compatible endpoint instead of a canonical integration:

```toml
[profiles.opencode-local]
harness = "opencode"
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
server and the preview reader. OpenCode previews query the same native model
inventory in a fresh launch-shaped store. A named profile uses its frozen
provider settings and selected integration. A general ad-hoc preview combines
inventories read with separate available integrations. Each launch uses one
integration and the vendor makes the final model selection. The live `GET /api/model` catalogue supplies
limits and variants for an exact model entry. An unreachable endpoint can still be
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
server names. Antigravity, Claude, and OpenCode scan MCP configuration as a
best-effort guard for direct Owner entries. The daemon enforces the
[local Owner process boundary](api.md#transport-and-principal-identity) using
its own Agent custody. Windows checks exact Job membership and Linux uses
degraded process-group and ancestry checks. The shared scan checks the
executable of a direct invocation for `swarm`, `swarm.exe`, or the active
controller binary, then
matches the `mcp` subcommand without `--require-agent`. Shell scripts, command
wrappers, and variable expansion are outside the scan.
HTTP matches the configured `cloud_mcp` hostname exactly, ignoring case and
trailing dots. With no configured hostname, it matches a parsed host
containing an exact DNS label `swarm`, `eliot-swarm`, or `eliot_swarm`.
The owner endpoint carries Owner authority through Access.
The server Eliot generates always carries `--require-agent`
and requires the generation-bound Agent credential. Swarm MCP names
(`eliot_swarm`, `eliot-swarm` and the per-session prefixes) are reserved and
cannot be allow-listed. A renamed Owner entry still matches by transport.

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
absent and use the Swarm service label check. Unknown policies and reserved
Swarm names in `mcp_allow` are rejected at config load, and a non-empty
allow list requires `minimal`.

`inherit` exposes native MCP configuration alongside Eliot's Agent-scoped
server, subject to each harness mapping below. `minimal` exposes Eliot's
Agent-scoped server and the explicitly allowed native servers.

- Codex enumerates the effective `mcp_servers` surface pre-spawn in both
  policies and disables every owner-identity match with a
  transport-carrying per-run `-c` override (exact owner names stay covered
  by synthetic transport-carrying stubs as backstop). An allow-listed name
  that matches the identity is rejected at launch (`profile_error`)
  instead of re-admitted. Matching a direct stdio transport requires a
  visible `mcp` subcommand. `minimal` additionally
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
- Antigravity bootstrap profiles use `inherit`. Explicit and legacy `inherit`
  preserve user and workspace discovery, including standard plugin sources,
  after the daemon checks enabled entries for direct Owner Swarm transports
  and reserved names. Without a native per-session exclusion, a match refuses
  startup and identifies the server and file. Uncheckable discovery redirects
  refuse startup and identify the declaring file. Swarm adds its
  generation-bound Agent bridge through ACP session setup. For `minimal`,
  the daemon resolves enabled `mcp_allow` entries from global and workspace MCP
  files and rejects missing names, Owner Swarm transports, and workspace
  entries that duplicate allow-listed global names. The daemon atomically
  publishes the prepared file. The plugin installs
  the prepared file under a generation-named Gemini root selected via
  `acp --gemini-dir`. Fresh launches create their own root and exact resume
  reuses the retained session's root. Exclusion of user MCP and plugin sources
  from the effective ACP inventory is unverified.
  A workspace MCP server outside the allow list, a workspace plugin source, or
  a personal customization directory redirect refuses startup with its source
  path when it cannot be filtered. The staged MCP file is removed after native
  exit. ACP does not expose the effective MCP inventory.

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

## Daemon and automation settings

Backlog and automatic cleanup are root-level automation options.

```toml
[automation]
max_event_backlog = 100000
auto_cleanup = true
auto_cleanup_after = "7d"
```

`max_event_backlog` is a positive signed 64-bit integer. It defaults to `100000`; zero,
negative, wrong-type and unknown automation values are invalid. Automatic cleanup
is written as enabled when Swarm creates a new configuration. An existing
configuration that omits the key runs plan-only sweeps. Explicit `true` applies
eligible cleanup after the first complete plan-only sweep of each daemon startup.
Set `auto_cleanup = false` to disable automatic planning and apply.
`auto_cleanup_after` defaults to seven days and accepts a positive duration
such as `"14d"`. Every automatic apply uses the
[cleanup retention policy](coordination.md#registered-artifacts-and-cleanup-k69) through
the same guarded plan and apply path as the cleanup UI and CLI.

Store retention uses the `[daemon.retention]` table.

```toml
[daemon.retention]
events_days = 30
processes_seconds = 86400
commands_days = 90
mail_days = 90
interval_seconds = 300
compact_on_start = false
```

Swarm writes the four history windows shown into the config it creates for a
new Store. Existing configs and existing Stores without a config keep their
settings. An omitted history window disables deletion for that class.
`interval_seconds` defaults to 300. `events_seconds` can specify the event
window in seconds instead of `events_days`. Set at most one of the two. The event
and exited-child process windows are positive integers. `interval_seconds` allows
1 through 86,400 seconds. Optional positive `commands_days` and `mail_days` windows
enable pruning of closed command trees and expired acknowledged or legacy
advisory mail, respectively.
The [Core retention contract](core.md#retention) defines the protected records.
Command and write
bindings always outlive the 24-hour retry window plus the 5-minute allowed
clock-skew margin regardless of `commands_days`, so a pruned id resubmits as
`id_expired`, never as new. Mail rows outlive the retry window plus twice
that margin regardless of `mail_days`, so a removed delivery admitted with a
skew-bounded write id reads `history_gap` on its own mailbox's read paths;
a retried ack over it succeeds as already acked.

`swarm status` and desktop show retention off when all four windows are unset.
`daemon.status` reports the main and WAL file sizes, retention windows, and
the retained-event watermark `pruned_through`. New Stores reclaim free pages
incrementally. Older Stores reuse freed pages while the daemon runs, so a
retention pass can leave `eliot.db` at its prior high-water size. To reclaim
space from an older Store, run `swarm maintenance compact` with the daemon
stopped. `swarm store compact` calls the same operation.
This owner-only local command takes the private DataDir lock, refuses to
operate while a daemon holds it, and does not start a daemon. Before SQLite
runs `VACUUM`, it reports the estimated required and available bytes and
requires free space of about twice the combined current `eliot.db` and WAL size.
`compact_on_start` remains opt-in and defaults to `false`; when enabled, startup
uses the same preflight before the daemon serves requests. Routine retention
does not run compaction in the live request path.

### Automatic attention settings

Automatic attention uses per-kind settings. Each listed attention kind is
enabled by default. Set its `enabled` value to `false` to disable it.
The daemon loads these settings at startup. No-progress attention queues a
question and notifies the live manager, or the Owner when no manager can act.
Automatic interruption and a
session-growth switch are additional opt-in steps.

```toml
[daemon.attention.no_progress]
enabled = true
threshold_secs = 900
interrupt = false

[daemon.attention.no_code_progress]
enabled = true
threshold_secs = 900

[daemon.attention.children_unattended]
enabled = true
threshold_secs = 900

[daemon.attention.session_growth]
enabled = true
switch = false
```

`no_progress.interrupt` and `session_growth.switch` default to `false`. Set
either to `true` to opt into that additional operation. `session_growth.switch`
controls the fallback for legacy ratio/file-size warnings; a configured profile
context ceiling uses its safe A6 transfer when native compaction is unavailable.
Reading settings emits these defaults while retaining any configured per-kind
threshold; when a per-kind threshold is omitted, the legacy
`attention_suspicion_secs` value continues to supply it.

Time windows must be positive integers whose millisecond value fits in `i64`.
An older `daemon.attention_suspicion_secs` supplies the default for omitted
per-kind windows. The configured `no_progress` and `no_code_progress`
windows, with a 900-second minimum, provide the fallback for a profile and
project with too few successful turns. See [automatic attention](monitoring.md#automatic-attention)
for when Swarm derives a larger window from the Store. Source-health failures
are observed promptly. The configured `no_progress` window also bounds
recovery for an unhealthy source ([plugin protocol](plugin-protocol.md)).

`agent_lost`, `source_unhealthy`, `provider_refusal`, `session_growth`, and
`repeated_command_failure` each have an `enabled` setting under
`[daemon.attention.<kind>]`. Repeated-command detection uses three matching
failures in one turn. Provider eligibility and budgets still follow
[routing](#routing) and provider holds. `on_restart` governs daemon restart
continuation independently of the `agent_lost` attention setting.

### Observation thresholds

The observation thresholds are optional.

```toml
[daemon]
heavy_wall_secs = 30
heavy_cpu_secs = 60
heavy_rss_bytes = 1073741824
session_growth_ratio_permille = 800
session_file_bytes = 67108864
background_wake_max_wait_secs = 1800
checkpoint_max_file_bytes = 16777216
checkpoint_max_total_bytes = 67108864
```

A child-process tree whose wall time, consumed CPU or peak RSS crosses any one
heavy threshold closes as one `activity.heavy` event (defaults: 30 s wall, 60 s CPU,
1 GiB RSS). Session growth warns at 0.8 context occupancy or a 64 MiB native session
file where the harness exposes a documented size source. A profile's optional
`context_ceiling_tokens` is an independent absolute per-call context limit. The
[session growth reaction](monitoring.md#automatic-reactions) uses a quiescent
session boundary. After a successful commandless native continuation, a growth
switch keeps the prior task command as its handoff anchor and includes the
latest turn's report. `background_wake_max_wait_secs` (1 to 86,400) bounds an
[automatic background-process wake](automation.md#automatic-background-process-wake).
`checkpoint_max_file_bytes` and `checkpoint_max_total_bytes` (at most 1 GiB) bound
[worktree checkpoints](monitoring.md#work-recorded-in-git) and apply to the next one.
`swarm doctor` shows each installed harness version with its self-check record
(installed version plus when that version last passed Swarm's own scenario,
#932) without blocking launch; no version gate pins executable selection.
It also reports every profile whose explicitly
pinned executable is missing, with the configured path, current replacement
path/version when available, and the exact `profiles.<name>.cmd` setting to remove.
The desktop reads
missing pins through `daemon.status` and displays the repair in Models & presets.

Cloud MCP is disabled when `[cloud_mcp]` is absent. When present, all fields are
required. The HTTP MCP executable is the running daemon executable (`current_exe`);
it is not a separate setting. The hostname is a bare DNS host; Eliot constructs
`https://<hostname>/mcp`. Tunnel and credential values are metadata only: Eliot
does not read the credential file contents. Cloudflared reads that file when it
starts. Swarm clears stale tunnel configuration files from its private data
directory after taking the daemon lock and before launching the next tunnel.

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

A registered project may define a static command refusal table with
[[projects.ID.command_guards]]. Each entry has a pattern glob and a literal,
single-line refusal. Globs match the full command supplied by a native
permission request and each trimmed segment separated by unquoted, unescaped
`;`, `|`, `&`, or a newline. Matching uses the supplied text without shell
expansion. Up to 64 entries are accepted; patterns are at most 1024 bytes and refusal
text at most 512 bytes. The daemon freezes the registered project's table into each
admitted launch after project discovery. It is evaluated only when a supported native
permission request exposes the command.

Owner-global `[projects.ID]` entries register projects. On an Owner open or launch in
an unregistered Git checkout, S2 creates an entry through the settings writer,
recorded as a machine change. Its profile allowlist is the existing profiles;
its action allowlist grants ordinary launch/send only, without script/interpreter,
Git fetch or additional automation authority. For a linked worktree, the
canonical Git common directory identifies its repository registration. Discovery
prefers the registered primary checkout even when a legacy registration names the
worktree itself. Without a registered primary checkout, discovery can reuse a
sole registration for the common directory. Ambiguous matches refuse discovery.
A checkout is compared canonically: the same directory reached through a
drive-letter path or Windows' verbatim `\\?\` form is one checkout, so an
unchanged registration is never refused as changed.
Reuse returns that registration's checkout and exact allowlists, preserves the
Agent's workspace, and adds no settings entry. A launch records the selected
registration ID independently of its execution workspace and authorization scope.
Child launches inherit the parent's recorded project and authorization scope.
Workspace inventory is read-only. Daemon startup performs no project
discovery. Registration runs through an explicit Owner open (`project.discover`),
an Owner launch in the checkout, or the desktop's `--workspace` checkout.
`path` names the canonical
Git checkout; `profiles` and `actions` are explicit allowlists over existing global
profiles and implemented actions. Missing or empty allowlists deny all.
`workspace` selects managed-worktree policy for top-level launches in the
project: `worktree` (the default for registered projects) prepares one standard
worktree per top-level launch without any flag, while `shared` keeps sharing
the registered checkout as before. Any other value is invalid.
`format` remains an optional explicit argv array (1–32 nonempty arguments, at most
4 KiB each); the daemon never parses it through a shell. The project's shared check
and Goal defaults live in its canonical `[projects.ID.policy]` settings block:
`policy.check.argv` is the explicit check argv. `policy.check.deadline_ms`
sets the optional bounded deadline. Omission uses the
[Goal check default](automation.md#goal-loop) for Goal rounds.
`policy.check.at_turn_end` reserves that check for the idle Goal driver when
true. The argv has the same 1–32 nonempty argument and 4 KiB per argument
limits. After a Git-confirmed `file.changed` hint or dirty `git.changed`
snapshot settles, the daemon runs `format` and then the Agent's resolved
launch Goal check with the Goal's argv and working directory. The fast check
uses 60 seconds when no deadline is set.

The Owner can enable `policy.check.check_timeout_retry_safe` for a pure
check that is safe to repeat. It authorizes the [Goal timeout retry](automation.md#goal-loop)
for the approved command.

`policy.budget.max_rounds` supplies the Goal default (3 when omitted; accepted range
1–16). `policy.budget.max_cost` is a `GoalCostLimit` pair `{ unit, amount_micros }`
using the Agent's verbatim native session-cost unit; it is never converted. The
policy's `scope` and `check_inputs` supply the corresponding Goal defaults. A policy
that sets a budget, scope, or check inputs requires a check. The Owner can override
each Goal field independently, including clearing nullable limits or scope lists.
Agent and project-rule launches that inherit policy can lower budgets and the check
deadline, add protected check inputs, and narrow scope. When the policy omits
its deadline, an Agent or project rule can set at most 60 seconds. A path must match both the
project scope and an explicit launch scope. Null limits, empty scope or check-input
lists, and a cadence that replaces idle checks require an explicit opt-out.
Executable check overrides and escalation require Owner authority.

`no_policy` requires a nonblank reason and skips both the policy-installed Goal and
the Agent's fast project check. The caller can still supply a separate Goal within
their existing authority. `swarm show` and the project's opt-out list show the
retained reason. An ordinary switch within the same project carries it forward.

The settings API reads and saves this same canonical policy with the other project
settings. Legacy flat `check`, `check_at_turn_end` and `max_rounds` values are accepted
as migration input; settings readback exposes the nested policy shape. The desktop's
project editor edits this block from the same settings data and saves it as one
revision-checked projects save; Validate is a dry run, and a refusal shows the
store's message. It shows `policy_usage` from `project.list`, read again while the
editor is open. Project registration readback uses the canonical Core [`ProjectRegistrationView`](../../crates/eliot-core/src/api/agent_views.rs);
project identity includes the canonical checkout and Git common directory. The registration ID names nothing a person reads: a project's name is the
owner's `[desktop] display_names` entry, else the repository name of its `origin` remote
(the readback's read-only `origin`, [api](api.md)), else the folder of the repository's
main checkout. The daemon readback identifies the main checkout even when Git metadata
lives in a separate directory. A registered linked worktree reads as its repository's
name and its branch from the daemon's verified checkout readback when available.
The sidebar's projects are the daemon's [`chat.organization`](api.md#chat-organization)
groups in the daemon's order, read in one bounded `chat.organization` read: each
registered project with its tasks and one trailing count, its tasks working now
over the listed rows' live states (the read's manager, assignment and totals stay
in the API, not in the sidebar), plus its queued
launches and automation waits with the wait reason and who can change it
(`launch.queue`, every page, polled independently); then the no-project bucket.
Registered projects with no tasks follow the populated ones and stay selectable.
A failed read keeps the last server grouping marked stale and synthesizes none;
`organization.changed` events refresh already loaded rows by id.
Each `agent.list` row carries the group the daemon reads it in, so the desktop groups
nothing by path, repository or name. An Agent whose execution label is not a
registration, such as a legacy `wt-*` path label, stays in the no-project bucket
under its retained folder name; only a server link places it under a project. This
leaves historical Agent rows intact.

The registration ID is the stable Project identity. Owner settings own its
registration lifecycle, display name, registered repository, allowed profiles,
actions and policy. The initial managed path uses one primary
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

The optional `[projects.ID.git.publication]` table maps literal aliases to direct
Owner-approved push endpoints. Endpoints are absolute local paths or direct HTTP, HTTPS,
Git, or file URLs without credentials, rewrites, helper schemes, query data, or fragments;
SSH and ambient remote selection are unsupported. This is an independent allowlist from
`fetch_remotes`: a fetch approval never authorizes a push. A project rule must also be
granted the `git.publish` action in `actions` and name an approved publication alias.

The optional `[projects.ID.git.publication.github_cli]` table selects one executable,
existing GitHub CLI config directory, host, repository (`owner/name`), and base ref. Every
field is literal and owner-configured. For PR creation the selected HTTPS publication
endpoint must match that host and repository. Eliot passes the config directory through
`GH_CONFIG_DIR` for the command lifetime and reads it in place; config contents and tokens
are never copied into settings, command args, events, or logs. The CLI executable and
config directory must be absolute existing paths without a symlink or reparse root. These
fields are part of the saved registration revision and are rechecked at publication.

An owner-approved publication rule is stored in the registered checkout's
`.eliot/rules/*.toml`:

```toml
[[rule]]
id = "publish-final-check"
on = { event = "project.check.passed" }
do = [{ action = "git.publish", repository = "app", remote = "qa" }]
```

The rule is opt-in per project. It always commits the exact checked managed-worktree
snapshot with the first line of the matching final answer. Omitting `remote` makes a local
commit only. Setting `create_pr = true` requires a named approved remote and the explicit
GitHub CLI integration above. The dedicated pass event comes only from successful final
project-check settlement; manual Goals and ordinary `goal.changed`/turn events do not
qualify. The event envelope and Core `ProjectCheckPassed` bind the project, retained turn,
Agent generation, workspace fingerprint, and commit subject before Store dispatch.

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

For an event-triggered Run, a captured `subject.workspace` selects the triggering
Agent's event-time worktree as the process working directory. This working directory
does not change the registered checkout's `.eliot/scripts/` entry root or its file and
interpreter validation. A project Run uses that worktree only when Git confirms it
shares the registered repository's common directory; a foreign repository is rejected
before spawn. A relative `cwd` is resolved and contained beneath the selected worktree.
An Owner-approved absolute `cwd` keeps its existing path policy. When the event has no
workspace, the Run's configured `cwd` policy applies. Script stdin presents Windows
verbatim workspace paths in display form; the persisted event and frozen command keep
the original path identity.

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
(`session/setModel` with the provider/profile binding and `session/read`
confirmation of selector admission, between turns only). Muse
keeps the last configured value for omitted fields, with initial values
from the native session and frozen profile. The frozen profile supplies
the native resume effort. Claude commits the accepted selector through
the stream-json `set_model` control request between turns, with a native
permission-mode check before and after the setter. Effort is launch-only.
Other omitted fields use the frozen original. OpenCode switches the
retained session model at configure time (`POST /api/session/{id}/model`
with a `session.get` proof) and applies effort through `Model.Ref.variant`.
Antigravity switches `model` and `thought_level` through
`session/set_config_option` with echo proof. Catalogue omissions follow the
[plugin protocol](plugin-protocol.md). A mid-turn configure refuses before any
write. A catalog or quota failure at the next turn fails that
turn honestly; it never selects another model or account. The cached
readback behind that rule is
`harness.models` (bounded per-route/account catalogue with freshness and
credential identity) and the separate `harness.quota` (timestamp, scope and
an explicit supported/unsupported/unknown status with provider-verbatim
`details` when supported: Codex `account/rateLimits/read` plus
`account/usage/read` when useful, Muse `usage --json`; never secrets or
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

Muse effort-only revisions inherit the preceding selector, starting with the
selector admitted at native session readiness. A turn stores the selector
from its bound revision as `effective_model`. The Agent shows that selector
with `model_verified: false` when the provider gives no independent model
evidence. An explicit mismatch clears the turn's effective model.
For an older turn with no stored model, Store fills the selector once when
the exact native start is retained, the selector is recoverable from that
generation and revision, and the journal contains no mismatch for that turn.
This applies at revision 0 and across running and terminal states. A missing
start or selector leaves `effective_model` absent.

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
accounts, proxies and owner_rules.
Rules/scripts remain files owned by their existing APIs; settings references and
automation policy do not create a second executor. Desktop preferences use
`[desktop]`: `theme` (`system`, `light`, `dark`, `amoled`, `purple`, `neon`,
`matrix`: the desktop's six palettes, `system` following Windows between
light and dark), `window` (`width`/`height`: the normal client size in DIPs, from
640 by 480 to 32768 by 32768; optional `x`/`y`: the normal outer top-left in physical
screen pixels, from -32768 to 32768; optional `maximized`; out of these bounds the
settings are refused and the desktop opens at its defaults), `startup` (`new_task`, or `last_conversation` to reopen the most
recently active Agent's conversation), `last_access` (`ask`, `auto` or `full`), and
these bounded lists: `favorite_models` (`{harness, model, effort?}`), `recent_models`
(most-recent-first `{harness, model, effort?}`), `collapsed_projects` (the sidebar's
project keys), `model_visibility` (the picker's "Manage models" choices:
`{harness, model, visible}` or `{harness, provider, visible}`; a model entry overrides
its provider's entry, and a model without either is shown), `collapsed_model_groups`
(collapsed picker group keys, `<harness>` or `<harness>/<provider>`) and
`display_names` (`{key, name}`: `key` is `project:<sidebar project key>` or
`agent:<Agent id>`, `name` is the owner's display name; it renames what the sidebar
shows and changes no project registration or Agent record). The `MAX_DESKTOP_*`
constants in [`config.rs`](../../crates/eliot-daemon/src/config.rs) own the list and
name-length bounds.
Picker strings are stored verbatim: harness and access values must match their
closed vocabularies exactly. Model ids, provider names, collapsed project and group
keys and display-name keys are 1–256 bytes, and effort is 1–128 bytes; these reject
controls and surrounding whitespace.
The desktop keeps no other preference store. The desktop writes `window` back once when the
window closes, and only when it changed. It reads `window` from the file before its window
exists, so the window opens at that size, place and maximized state with no move after the
first frame; a place no longer on an attached monitor is clamped into the nearest one's work
area, and without `x`/`y` Windows chooses the place.
When the daemon cannot start, the local Owner can open Settings (Repair
settings) from the startup failure view. Repair uses the daemon's settings reader and
revision-checked writer under the private daemon lock. After a repair save
succeeds and Settings reads the saved file back successfully, Settings offers
Retry startup through the existing startup action.
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
later-layer fields fail validation.
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
The check covers the registration's own unsettled commands and every Agent that
belongs to it: an Agent whose execution project is the registration ID, or, for a
legacy label that names no registration, whose label equals the registration's
checkout under the platform's checkout-key rule. Only those Agents count while they
are live, hold unsettled activity or retained unknown commands. An Agent of another
registration never counts, even when its workspace lies under this checkout, so a
parent folder registration is not blocked by its nested projects. A registration in
use refuses the entire save with `project_in_use` and leaves the settings revision
and registrations intact.
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
on_provider_failure = "next"
default = ["codex-luna", "muse"]
worker = ["codex-luna"]
```

Role names use letters, digits, `-` and `_`; each role allows at most 64 candidates and rejects duplicate or
unknown profile references. The `--role` launch argument selects the role only
when `--profile` is omitted; omission stays absent on the wire and means
`default`. An explicit profile continues to bypass routing.
An explicit profile with a missing pin refuses with the replacement guidance.
Role routing records a missing pin as `missing_profile_command` in the ordered
skips shown by `swarm why`.
At first admission, candidates must be installed, eligible under the
[provider hold gate](monitoring.md#provider-holds), not known quota-exhausted
where passive readback is supported, and within configured capacity. A fresh
launch with input can claim a due probe for an unknown-reset hold.
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
desktop shows it on the composer's Auto chip. Same-ID replay returns the frozen choice.
Provider holds and capacity are rechecked transactionally at admission;
a concurrent state change can refuse an otherwise prepared choice before dispatch.
Routing checks launch authority and holds before running native probes. A provider
switch keeps the executable bound to the source generation.

#### Owner quota reserve (#872)

Set `quota_reserve_percent` on a named profile to an integer from 0 through 100.
It defaults to 0. The recommended reserve is 20, which keeps the final 20
percentage points of each applicable provider window for the Owner. A new
automatic launch skips the route with `quota_reserve` when current usage of any
applicable window is strictly greater than `100 - quota_reserve_percent`.
`quota_unknown = "admit"` is the default. Set it to `"hold"` to skip with
`quota_unknown` when no current applicable numerical reading is available,
including when all observed windows are unmapped or the reader is unsupported.
`swarm quota` shows the route's reserve readback and unknown policy.
The current profile settings apply even when
a child inherits its manager's previously frozen native route.

The reserve applies to new Agent children, automated assignments, and rule and
timer launches, including explicit profile choices. Owner-initiated launches and
messages, in-flight turns, and Goal continuations of existing Agents are exempt.
Provider holds and real provider limits retain their existing force.
An automatic role with a configured quota policy tries eligible candidates with
headroom. It prefers the lowest observed weekly usage, using the most-used
applicable weekly window for a route. Candidates with a weekly reading precede
those without one, and configured order breaks ties. Roles with both default
settings throughout keep their existing ordering. Arbitration retains the
automation class of waiting work. A waiting automatic role launch evaluates
its configured candidates again on each attempt. A waiting explicit profile
launch keeps its original route binding.

`on_provider_failure` defaults to `stop`. With `next`, an Owner's role-routed
launch in a project freezes the configured `handoff_fallback` names. The daemon may continue
the original launch input after a structured quota, rate-limit or access refusal
when the adapter proves that no native work began for that exact turn. A stream
gap, uncertain delivery, tool or output activity, an explicit profile or a later
SEND leaves the ordinary outcome in place. An automatic successor must match the original frozen launch authority, including
the harness, access, permissions, approval, account and credential selectors.
An original launch with explicit overrides does not enter automatic failover.
The Codex adapter supplies this evidence for structured upstream refusals.
The daemon reuses the [switch and handoff](coordination.md#agent-switch-implemented)
transaction with fresh command IDs and the retained workspace. It chooses the first
eligible profile that is both in the current `handoff_fallback` order and still in the
frozen list. Each switch removes its selected profile from that list. `swarm why` reports
the refusal, skipped profiles and, when admitted, the successor and input command.
An automated launch's provider successor applies the same quota admission policy.

#### Automatic handoff (#918)

A session that can no longer continue its work hands it to another model through the
[ordinary switch](coordination.md#agent-switch-implemented). There is no separate
failover path, and a hold never makes work wait for its reset when a successor exists.

```toml
[routing]
auto_handoff = true                    # default; false turns automatic handoff off
handoff_fallback = ["claude-haiku", "opencode-free"]
default = ["codex-luna", "muse"]
```

The daemon evaluates this every five seconds for root and child Agents outside chats:

- a quota or rate-limit hold on an idle Agent that still owes work: its held turn did
  not finish, input is waiting or was refused by the hold, or a Goal is active;
- a lost or failed session of this daemon's lifetime with the same kinds of owed work,
  20 seconds after the loss so native process-loss recovery can reopen it first.

Automatic handoff uses `handoff_fallback` in configured order for every Agent. Role
lists choose initial launches. A lost session can reuse its profile if the fallback
list names it. Profiles this task already left and held profiles are skipped.
Each candidate passes installation, hold, capacity, quota reserve and the
[equal-or-narrower authority check](coordination.md#agent-switch-implemented). With no
eligible candidate the Agent keeps its work. The Owner inbox and `swarm why` report
an empty list or why candidates were skipped. Routing must succeed before the daemon
dispatches a stop to the source. A refused admission is tried again
after five minutes and a failed launch or
transfer after an hour. An unknown outcome is never retried. When a hold clears, the Agent stays on its new profile; moving back is an
ordinary request. Settings changes apply to the next evaluation.

Each continuation lineage admits at most three switch reservations initiated outside an
explicit Owner switch. This includes automatic held/lost/provider, rule and session-growth
switches, as well as manager-routed switches. The count survives successor Agents and
resumed generations and does not depend on retained events. An explicit Owner switch and
resume carry the lineage without spending this budget. A reservation charges once in the
same Store transaction as the switch command; retry/readback and later routing or settlement
do not charge again. When the budget is exhausted, the work stays with its current Agent
until the Owner moves it explicitly.

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
Desktop profile creation and duplication omit `cmd`. The Owner can add or remove
an executable override in the profile source. `swarm settings claude-cmd PROFILE
--cmd PATH` sets an explicit Claude override and `swarm settings claude-cmd PROFILE
--latest` restores automatic discovery through the revision-checked writer.

The desktop adds a project by choosing its checkout with the native folder picker;
it derives a valid, collision-free ID from the folder name and immediately saves
only the projects section through that same revision-checked writer. The Auto model
row links directly to Routing; reordering candidates uses the same immediate writer.

## Profile section

The `profiles` section of `settings.read`/`settings.save` carries the `[profiles.*]`
tables as JSON, under the file revision like every other section. The desktop renders
its TOML source view from that value. Form edits and duplication operate on parsed TOML
tables, preserving nested arrays such as `setup`. While the source draft is invalid,
the desktop retains the profile list and allows edits to independently parseable
profile blocks. Saving requires a valid full draft. Named and ad-hoc launches share the
frozen harness, model and effort representation. Named profiles apply common control,
MCP, executable and setup validation around the harness-specific field mappings.
Profiles resolve from `config.toml` at launch admission (above), so a saved
file is what the next launch uses; running or queued work keeps its frozen snapshot, and
only a successful `configure` changes a live Agent's next-turn model/effort. Daemon
settings loaded once at startup (`[daemon]` worker limits and `[mail]`) still require a
restart after the save.

The `settings.read` RPC reads and validates one file revision before locking the Store
for history. Bootstrap and project discovery also prepare settings snapshots before
locking the Store. Write-side revision checks and publication stay under the writer
lock. The reader parses valid TOML once for strict validation and section projection.
Located diagnostics and possible literal secrets require a table-header scan. Malformed
TOML uses the header spans to independently parse readable section blocks and their
validation dependencies.
The API returns every safely readable section plus bounded issues for sections
that fail parsing, validation or the daemon's redaction policy. A TOML syntax diagnostic
identifies the affected top-level section when the parser can locate it; other section
blocks remain available in Settings. A section withheld for safety never includes its
source or values. Diagnostics identify the section the daemon locates as their
source. The desktop can edit readable section drafts. The Remove action replaces
a section with an empty object when the daemon verifies a failure in that
section itself. The revision remains the hash of the complete `config.toml`,
including while another section is invalid.

`settings.save` keeps the complete-section replacement and revision-CAS contract. The
daemon validates the changed section and its dependencies with the same strict parser
and validators used for launch configuration. Parsed table headers identify source
blocks, so bracket-like text inside a value does not affect which bytes are replaced.
Profile blocks that cannot be separated safely are withheld with `not_allowed` and
must be repaired directly in the file before a profile replacement can proceed.
The writer preserves all unselected source blocks
and reports remaining issues on the next read. A section cannot be saved when its own
validation fails; an unrelated broken section does not hide or block a usable section.
The desktop performs no configuration validation. Profile source still follows the
existing safety rule: profiles defined outside `[profiles.*]`, known secret values,
unknown fields and credential-bearing URLs are never handed to the editor. Profile
sections carry environment and credential names, never values. The daemon keeps one
revision-keyed validated snapshot per router: an admission whose file revision matches
reuses it, and any byte change re-reads and re-validates, so the saved file is still
what the next task uses.

## Frozen files

Resolve explicit notes/setup/scripts/entry files under allowed roots, reject escapes,
devices and alternate streams, bound size, capture bytes or canonical path/size/digest.
Verify path references immediately before dispatch. Small text may use an opaque safe
private path. Do not derive filesystem names from command ids containing separators.
Keep a copy while a retained current definition or open/unknown command needs it; reclaim
it afterward. No global content-addressed store, provenance graph or custom GC service.

## Environment and secrets

Build the native environment from the OS baseline, admitted nonsecret `pass_env` values,
literal nonsecret `env`, named secrets resolved in memory, and the allowed Eliot shell
identity/Git/PATH. `secret_env` is a subset of `pass_env`; those values are never persisted.
Missing named required values fail clearly. Windows keys compare case-insensitively.
Names beginning with `ELIOT_`, compared case-insensitively, are reserved and cannot
appear in `pass_env`.

The managed Agent shell never receives `ELIOT_HOME`, `ELIOT_AGENT_DATA_DIR`, or Owner
token or data-path variables. Eliot supplies the owned data root to each managed
Agent-bound MCP integration through its scoped process arguments or MCP environment.
`ELIOT_HOME` stays inside that integration scope, and `ELIOT_AGENT_DATA_DIR` is
MCP-only. The client compares the resolved target (`--data-dir`, then `ELIOT_HOME`, then
the default) with the issuing directory after lexical normalization and case-insensitively
on Windows. The CLI refuses cross-root and unbound Agent credentials before connecting.
When `pass_agent_identity_to_shell` is enabled, the shell receives the Agent ID,
generation and token with a fingerprint of the issuing directory in
`ELIOT_AGENT_DATA_SCOPE`. A bare Agent CLI resolves the issuing directory from
its generation's private artifact root and checks the fingerprint before connecting.
An explicit directory must match the same binding.
Codex receives the Agent identity at its app-server boundary to configure the named
MCP transport, with its issuing directory supplied in the MCP environment.
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

Goal checks and Run scripts use one explicit platform base environment, built after
clearing the daemon environment. It includes `PATH`, temporary-directory variables and
the platform home/profile variables; Windows also carries `SystemRoot`, `SystemDrive`,
`WINDIR`, `PATHEXT` and `COMSPEC`, while Unix carries the user and locale variables.
Windows still requires `SystemRoot` and `PATHEXT` before spawn. The base does not copy
arbitrary daemon or Agent variables, profile secrets, or route/account credentials and
selectors. Project script `run.env` is separate from profile `env` and `pass_env`: only
Run scripts add literal values for owner-approved `run_env` names and their Run identity.
Goal checks receive no action-specific environment. `run_env` cannot name
Eliot/Agent/Run identity, credential, route/account, native-store selector, Git identity,
`PATH` or other reserved environment names; names compare case-insensitively on Windows.
Project values cannot override the role-specific Run variables.

Mask known secret values in native output and reject them in caller durable text.
Private routing tokens are handled separately from semantic profile data. Native
children receive their own identity only where isolated environment injection is possible;
otherwise their CLI calls remain attributed to the parent, as the plugin reports.

The profile switch `pass_agent_identity_to_shell` defaults to `true`. With `false`,
shell commands omit the Agent identity variables. A marker carrying no Agent
identity makes the shell CLI reject requests that would use Owner authority.
Codex supplies its named MCP server from the per-Agent app-server environment
and filters identity variables from shell tools. Claude, OpenCode and Antigravity
pass scoped identity directly to the managed MCP server. The MCP integration
receives its data root through its own arguments or environment even when shell
identity is disabled. Muse currently uses shell CLI identity, so disabling it
prevents Agent authentication from that shell.

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
file size and a separate count for each panic code). Readers ignore temporary
or incomplete files. Owner status reads consume validated plugin marker files
into a bounded view of the most recent plugin panics seen during this run.
The previous daemon marker is consumed once and reported in `daemon.status`, in `swarm daemon`
startup output, and in `swarm why` when the diagnosed failure is otherwise
unclassified. The desktop Agent details view shows the same one-line
diagnosis summary; the full chain lives behind `swarm why`.

## Paths and platform defaults

Keep Windows paths short; use long-path Git arguments/environment without rewriting
repository config. Use a separate Cargo target directory per worktree. Executable, argv,
cwd and env remain separate. Launch/workspace admission never performs an implicit fetch.
All persisted fields and frames are bounded. The roadmap determines when each section
becomes implemented; absent later features return `unsupported`, not a hidden substitute.

### Daemon autostart

On Windows, `swarm --data-dir DIR daemon install` registers a Task Scheduler
entry for the current OS user and selected private data directory. It starts at
user logon and restarts after failure up to three times at 60-second intervals.
After moving the executable, run `daemon install` from its new location to
refresh the verified task. `daemon uninstall` removes the verified task even
when the data directory is absent.

On Linux, `daemon install` writes a per-user unit under
`${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user` and enables it for the user's
`default.target`. The unit starts with the user systemd manager, runs the
canonical Swarm executable against the selected data directory, and restarts
after failure with a 60-second delay and a limit of four starts per four minutes.
The unit passes the
executable and data directory as literal arguments, including `$` and `%` in
paths. `daemon uninstall`
disables and removes only a unit carrying Eliot's ownership marker for that
data directory; it does not stop an already-running daemon. The user systemd
manager must be available when these commands are run.

Both commands accept `--dry-run`. Install previews include the Task Scheduler
XML or the complete systemd unit and systemctl commands. Uninstall previews
identify the task or unit selected for removal. Dry-run performs no scheduler, systemd, or
filesystem mutations. Installing, uninstalling and dry-run require an Owner
CLI outside an Agent session. The daemon lock admits one instance per data
directory. Windows demand-start detaches from the console and requests Job
breakaway. When a caller in a Job gets access denied with breakaway, demand-start
retries inside that Job. The daemon then shares the caller Job's lifetime.
Unix demand-start starts the daemon in a separate session with standard
streams disconnected.

### Agent restart policy

`[daemon].on_restart` takes `"continue"` (default) or `"hold"` and applies
at daemon startup. Continue has one automatic continuation rule: resume a
retained generation only when work was interrupted or input was still waiting
at restart. The restart event captures undelivered direct sends and unread
wake-enabled mail. Mail arriving later does not reopen an idle Agent.
An idle Agent without interrupted work or waiting input stays closed until a direct `send`,
which uses normal resume admission and links that new input. Archived Agents
are excluded, and an Owner-applied Stop remains a fence. Eligible generations
resume in batches of up to four commands within configured capacity. Roots
resume before their children. Descendants wait for ancestors with interrupted
work or undelivered input. A resumed generation with interrupted work opens with a
framed Swarm note naming the interrupted turn; no prior input is replayed.
Resume uses the Agent's frozen profile and any inherited effective
configuration from the retained generation. Goals and project assignments
follow the Agent to its new generation. A Goal with unresolved prior work
remains paused until that work is settled. Hold leaves those sessions for
explicit resume. Previously admitted commands still follow durable command
recovery. Interrupted turns and uncertain effects stay `unknown` under either
policy: they are named, never replayed.

`swarm daemon update --interrupt-on-deadline` uses the ordinary Stop custody
path for captured active turns after the wait expires. Under `continue`, an
applied Stop with proven process exit and native source drain allows resume with
the framed Swarm note after replacement or rollback, with interrupted parents before
their children. The old turn retains its observed outcome. `hold` keeps those
generations stopped for the Owner.

A refused automatic resume leaves an Owner notice and a reason in
`swarm why AGENT`. Address that reason before explicitly resuming the Agent.

### Private stand settings

`swarm stand up --settings FILE --profile NAME` selects a profile from the
file's `[profiles]` table. Repeat `--profile` to select several. Without `--settings`,
names resolve in the current data directory's `config.toml`. A single profiles-only
TOML path is also accepted as `--profile FILE`. When no profiles are supplied,
first-start discovery supplies the defaults. Imported profiles are written to the new
stand's private `config.toml`.

`swarm stand up --read-only-copy DATA_DIR` copies committed SQLite state without
importing config, native sessions or process custody. The offline copy is opened
through SQLite read-only connections for `status` and `ls`. The copied rows are
snapshot data and cannot admit Agents, perform cleanup against the source's
workspaces or start a daemon.

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
