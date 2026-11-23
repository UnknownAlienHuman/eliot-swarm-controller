# Monitoring

Monitoring reports bounded facts and freshness. A failed source never invents completion
or blocks an unrelated source/router path.

The design follows Waku and Paseo:

- one exact-session live stream for immediate facts;
- one authoritative source owner per lifecycle family;
- reconnect means readback/reconciliation, not process/session death;
- a replaying source needs an opaque cursor; otherwise the uncovered interval is a gap.

## Source ownership and health

The current `(agent,generation,family) -> source` map lives in
[`source_owners`](core.md#events-and-authoritative-sources). Typical assignment:

| Family | Controlled Agent | Observe-only live source |
| --- | --- | --- |
| session/turn/message/tool/request/child/goal/usage | native stream | transcript after explicit handoff for granted live families; native paged history stays private |
| synchronous hook decision | hook only | hook only |
| process | OS monitor | OS monitor |
| Git/filesystem | Git/fs monitor | Git/fs monitor |

A hook may own a fact absent from the primary stream; it does not mirror lifecycle the
stream already owns. Codex native history pages are private queries, not source owners.
The observe-only rollout reader is separate: after settled exact-subject readiness it
captures the live edge and commits an explicit `observe` to `transcript` handoff with
mechanism `rollout_tail` and a gap before publishing normalized live records.

Initial ownership is committed before native I/O. Dynamic handoff uses the plugin
protocol's single compare-and-swap request: verify predecessor, record final cursor or
`source.gap`, install successor. The successor publishes only after commit. There are no
source epochs or independent lifecycle projections.

Health states are `ok|slow|stale|error`. Event-stream silence is not stale by itself; a
lost transport is stale until reconnect. Stale authoritative lifecycle means current
turn progress is unknown, never idle/done.

`observer_ready` is a private attach/fence proof, independent of source-open and health:
it requires the current generation, exact native scope/ref, an applied attach, an
unknown observe-only Agent, an open observe source and no detach fence. Source health
updates do not grant readiness. A changed health value updates only health and its
timestamp; unchanged health emits no event. Health cannot clear a gap, move a cursor,
transfer ownership or reopen a closed source. Return to `ok` only after verified
live-edge recovery, retaining the historical gap.

Source lifecycle (`open|closed`), health and the latest committed gap boundary are
independent fields in the same `source_state` row. Recording a gap does not close the
source or transfer family ownership. After exact readback and live-edge recovery, health
may return to `ok` while the historical gap remains visible.

Muse view-health needs no extra state: the same committed journal signals
count unavailable-view episodes and their outcomes per agent. Count
`source.gap` events whose reason starts with the shared unavailable prefix,
`turn.unknown` events whose reason starts with the shared unknown prefix, and
`turn.completed` events carrying the fixed `view_recovery: "readback"` marker.

## Process identity and custody

Process identity is `(host_boot_id,pid,start_time)`, never PID alone. Windows birth time
comes from `GetProcessTimes`; Linux uses boot id plus `/proc/<pid>/stat` start ticks.

Custody owners are simple strings:

```text
agent:<agent>#<generation>
service:<name>
action:<command-id>
workspace:<agent>#<target-generation>
```

A process has one owner. Shared service PIDs belong to `service:<name>`, not every Agent
using that service.

### Windows exact

- create suspended, assign per-Agent/action Job without breakaway, then resume;
- daemon-owned stdio Jobs use kill-on-close;
- an Agent generation's Job has a name derived from its recorded custody namespace
  ([data-directory binding](core.md#process-custody-identity)), so a
  replacement daemon can prove it closed ([restart recovery](configuration.md#accounts-and-configured-capacity));
- services that must survive daemon restart use separate service custody;
- force-stop requires the generation's recorded namespace to equal the Store's current
  namespace. A mismatch returns `process_custody_foreign`, leaves the Job untouched and
  settles unknown without replay. Restart probing of the recorded name remains read-only.
  With that binding verified, force-stop directly terminates the generation's named Job,
  without waiting for the plugin's cooperative stop, recovery or command queue. Startup and this
  termination are serialized, including when no live runtime entry remains.
  It waits for the Job to become empty, verifies recorded process identities and
  drains the native source before settling applied and releasing custody. Missing
  Job names alone do not prove reader drain or process exit; unconfirmed closure
  stays unknown. A repeated force after verified closure is an applied no-op.
  Loss suppression belongs to the force attempt through successful Store stop
  settlement. Failure or cancellation clears it; if actor teardown already
  deferred proven control loss, reconcile that loss and any open turn as unknown.
- only processes created inside the Job are contained: the Store records PIDs
  verified inside Agent custody, and a process spawned outside the Job by a
  broker or service (or attributed to the wrong owner) is invisible to the
  Store and cannot be terminated safely by PID alone.

Every applied stop requires two independently committed facts for the exact
generation: process-custody exit and native-source drain. On Windows the
process fact requires the owned Job empty and all recorded process identities
exited; the source fact requires the authoritative reader drained and its
closure committed. The checks share one absolute stop deadline. A missing
process or source fact keeps the outcome unknown and names that missing proof.
An `agent.stop` reply or native `session.ended` is preserved when observed, but
neither is required once both process/source facts are proven. If native session
termination was not observed, the applied stop records
`native_session_end_observed=false`; it makes no claim about a remote session.

### Unix degraded

For Linux containers, we recommend a reaping init such as Docker --init.

A process group contains ordinary descendants but a same-user child can `setsid`, and
the group does not automatically die with the daemon. Until a qualified cgroup/equivalent
is implemented, capability is `degraded`:

- graceful stop and best-effort kill of known group members;
- no claim that every descendant died;
- `stop --force` reports possible orphans rather than clean exact success;
- degraded custody capability and the `orphan_possible` flag are typed surfaces of
  `daemon.status` (`workspace_custody`) and the `capacity.released` event.

The stop process proof on Unix covers the recorded process group and known
members. It retains the degraded custody label and does not assert that a child
which escaped with `setsid` is gone.

### Adopted/shared services

An owner-run service discovered from its registration is read-only/adopted and is never
signalled or repaired by Eliot. Managed service custody is separate and stopped only by
its service lifecycle. Stopping one Agent never signals the shared/adopted server.

`stop` may request an isolated native operation for that Agent's session, and
never signals an adopted/shared service. The stop is applied only after the
Agent's own process custody exits and its native source drains. If no terminal
session result arrives, the outcome keeps that fact unknown while reporting
`native_session_end_observed=false`. `detach` remains the separate operation
that releases Eliot control while leaving the session alive. Observe-only
attachment cannot issue either native stop or other control operations.

### Background-process wake candidates

The [automatic background-process wake](automation.md#automatic-background-process-wake)
reads custody only. A candidate is a running `child` of the Agent generation with
a complete fresh snapshot. Its OS start time (Unix ms) falls inside a native tool
call window of the completing turn.

Codex and Claude report their own tool start and end times (`native_at_ms`). When
both edges are native, a helper they start after the call ended is outside the
window under any daemon load, and the lower bound allows only for process start
rounding: 16 ms on Windows, one second on Linux (whole-second `btime`). Every other
call uses commit times: OpenCode, Muse and Antigravity, or a frame without a native
time. Its lower bound is one second further below the `tool.started` commit. Commit
lag has no fixed bound, so under heavy Store load a job spawned earlier than that
margin is missed, and a helper spawned after the tool ended but before
`tool.completed` commits is admitted. The wake row records which basis applied in
`tool_time_basis` (`native` or `commit`). A call with no recorded end covers one
second after its start, because a harness starts its own helpers at turn end.

While a harness still runs a call as its own background task (`native_background`
on `tool.started`), a process bound only to that call is not a candidate, because
the harness wakes itself when the task ends. When `tool.background_ended` commits,
the window closes at that time and a process that outlived the task becomes a
candidate. When Swarm cannot track the task it does not suppress: a call past the
plugin's bound of 64 tracked tasks carries no `native_background`, so it can get a
duplicate wake but not lose one. A daemon restart ends every live generation, so
tracking never outlives the plugin that holds it.

On Windows custody is the Agent's Job, so every member descends from the session
and a parent chain may break at an exited launcher, as a detached job's does. On
Linux custody is a process group, so the exact chain (at most 16 links) must reach
the live `harness`. Neither chain may pass through the `plugin`. A candidate must
also not be harness-owned: console hosts, harness runtimes, language
servers, or Swarm or configured MCP servers. A configured MCP server matches by its
program and arguments, never by a generic launcher name such as `npx` or `node`.
When a candidate's ancestor also qualifies, the ancestor wins. On Windows Swarm opens a query handle to the armed
identity and keeps it only once the wake commits, so the exact exit code survives the harness closing
its own handle. A refused or failed arm, a self-wake and every retirement close
it. Without a handle (after a restart or a failed open), each check probes the
exact boot, PID and start time directly and reopens the handle while the process
runs, so exit detection does not depend on the monitor target. An exit observed
without a handle has an unknown code unless custody recorded one; on Unix the
exit code is always unknown.

Known limits:

- The substring server exclusions in `is_harness_owned_process` can exclude a
  wrapped command such as `cargo test -p eliot-mcp`.
- A survivor whose Claude background task ends during a later turn is not woken,
  because the newer turn makes the earlier one ineligible to arm.

## Process observation and terminal ordering

The OS monitor is authoritative for process start/exit/stats. A plugin may register a
PID only if host verification places it inside that Agent custody. Enumeration and
plugin registration upsert one identity. Plugin, harness and workspace-runner rows also
append one `process.started` event and, on departure, one `process.exited` or
`process.unknown` event. An enumerated descendant (`child`) is a registry row only:
`processes AGENT` and stats show it, and a heavy one surfaces as `activity.heavy`. A
child promoted to harness gets its start event at promotion.

Process exit does **not** immediately imply session/turn loss. It races buffered stdout
and final native events:

1. mark the process exited and stop accepting new native work for that generation;
2. drain the authoritative source to EOF or one bounded terminal deadline;
3. commit any terminal native fact first;
4. if EOF/deadline provides no terminal fact, commit `source.gap` for the uncovered
   tail;
5. only then emit `agent.lost` when exact/shared readback proves control is gone.

The loss records its cause: `root_exited` for a spontaneous plugin root
exit (the observed process exit with its numeric code and redacted stderr
tail), `owner_marked_lost` only for an explicit Owner assertion of loss, and
otherwise the settlement path's own cause: `stream_eof_without_session_terminal`
when the reader reached EOF with no terminal fact and no observed exit,
`stream_task_failed` when the reader task itself failed,
`plugin_owner_closed_without_session_terminal` when the Owner channel closed,
`daemon_force_exit_without_session_terminal` for a daemon force-stop,
`plugin_startup_exit` for a failed start, or the stop's own exit
(`stop_exit_without_session_terminal`,
`force_stop_exit_without_session_terminal`) when the bounded process/source
proof did not complete and the generation is settled as lost. When both stop
proof facts commit, the Agent is stopped and the stop is applied even if no
native session terminal arrived.
The drain detail (which gap the reader hit, including
`root_exit_source_failed`, `root_exit_drain_timeout`,
`owned_exit_source_failed` and `owned_exit_drain_timeout`) stays on
`source.gap`. For a Swarm setup failure before the plugin handshake,
`plugin_startup_setup_failed` is reserved for source-gap diagnostics. The start
command carries `origin: swarm` and the failing `swarm.start.*` location.

For a controlled start/resume, a plugin may report a positively observed native endpoint
exit after its reader has closed and finished. The daemon preserves the command outcome
as `unknown`, drains the plugin source, and marks the Agent `lost` only after the owned
plugin process tree is confirmed cleaned up. An unconfirmed endpoint or cleanup leaves
the generation starting/unknown.

Closing or losing the Codex observer's held rollout reader is not native thread-terminal
evidence and does not emit `session.ended` or `agent.lost`. Controlled owned-process
loss still drains its captured generation and finalizes it through the controlled path;
observer attachment or reader health cannot suppress or manufacture that lifecycle.

A timer such as “three seconds without `session.ended`” is never terminal evidence by
itself. On degraded Unix, lost session control may coexist with `orphan_possible`.

Process stats are ephemeral and do not count as model progress. Unknown exit code stays
unknown, never zero.

The process view is available through `processes AGENT`, `stats AGENT` and
`show AGENT.observation`. CPU counters are nanoseconds; RSS is bytes. Samples carry
their timestamp, completeness and a four-second freshness window. Missing or stale
values remain null. Liveness reports committed native facts separately from OS samples;
waiting for an approval/question is not a stall caused by missing model output.

Managed preparation uses the same process registry with owner
`workspace:<agent>#<target_generation>` and source/family `workspace_process`.
`show` and `top` include it under `observation.workspace`; `processes --scope workspace`
pages the exact owner, including target generation 1 while the Agent remains 0.
Each Git/setup runner supplies initial and final OS samples from its captured custody.
An empty or complete snapshot for one runner never proves an absent historical process
exited. Persist only supplied verified records; restart retains unresolved rows as
unknown. The workspace source remains open across steps and later removal. Exact
removed/retired lifetime closes it only when no running/unknown rows remain; otherwise
the retained source is stale. Neither PID discovery nor preparation grants native control.

The Windows boot marker is currently an estimate captured once per daemon and retains
its `windows-boot-filetime-estimate` label. Exact creation FILETIME and verified Job
membership bind owned processes; the estimate is not used to adopt surviving processes
across daemon replacement. Observer query handles cannot extend owned execution.

```sql
CREATE TABLE processes (
  host_boot_id TEXT NOT NULL,
  pid INTEGER NOT NULL,
  start_time INTEGER NOT NULL,
  owner TEXT NOT NULL,
  role TEXT NOT NULL,
  state TEXT NOT NULL,
  exit_code INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(host_boot_id,pid,start_time)
);
CREATE INDEX processes_owner ON processes(owner,state);
```

### Sampling work

On Linux, each monitoring sweep shares one bounded host-process enumeration
across its current targets; elsewhere each target still samples independently.
Capture the target set first;
validate each captured group leader before and after enumeration, and defer new targets
to the next sweep. Retain full process identities, known-identity refresh and per-generation
publication. Apply the scan cap once and member caps per owner, retaining known identities
first. A scan failure remains unavailable; truncated/stat-error coverage is incomplete,
and an omitted process is not proof of exit. Keep timestamps truthful and release the
sweep snapshot after use. This does not strengthen Unix containment or replace Windows
Job observation.

Unchanged process observations still validate generation, source, ownership and
the full process identity, but do not rewrite process or source rows. Their durable
`updated_at` records the last changed process fact; exited child rows older than the
event retention window are pruned ([core](core.md#retention)); the monitor retains each
accepted sample and its freshness in memory. Process read views overlay the cached
sample timestamp only when identity, owner, role, state and exit code still match
the durable record, never advancing it past an actual accepted sample. Restart
has no cache and exposes the retained timestamps. State/role/exit-code changes
still commit before publication, and workspace-source closure is checked even
when the sampled records are unchanged.

## Starting processes

These rules apply to plugins, services, hooks, scripts, checks, Git and workspace setup:

- resolve executable to a full path; pass argv/cwd/env separately; never add an implicit
  shell;
- containment is installed before execution;
- only intended child handles are inheritable;
- long input uses stdin/private files, never argv;
- Eliot text is UTF-8 without BOM and LF;
- Git has prompts/editor disabled;
- environment follows the minimal/inherit policy and Windows keys merge
  case-insensitively;
- running script/hook/plugin paths are never overwritten;
- cleanup/fingerprint/setup resolves paths under an allowed root and rejects `..`,
  symlink/reparse-point escape, alternate streams and devices;
- Windows file/SQLite handles are closed before replace/delete in tests and cleanup.

## Workspace preparation

`launch [--worktree [BRANCH]] [--base REF]` is pre-start work over one
durable row. A requested `BRANCH` always wins; a bare `--worktree` generates
the standard branch below. A top-level Owner launch in a registered project
whose `workspace` setting is `worktree` (the default) gets its own prepared
worktree without any flag. Workers share their parent's worktree by default
and can explicitly request a [managed child worktree](coordination.md#claims-and-hierarchy).

```sql
CREATE TABLE workspaces (
  agent INTEGER PRIMARY KEY REFERENCES agents(id),
  start_command_id TEXT NOT NULL,
  target_generation INTEGER NOT NULL,
  project TEXT NOT NULL,
  repo_path TEXT NOT NULL,
  repo_common_dir TEXT NOT NULL,
  requested_branch TEXT NOT NULL,
  base_ref TEXT,
  base_oid TEXT NOT NULL,
  worktree_path TEXT NOT NULL UNIQUE,
  frozen_setup TEXT NOT NULL,
  phase TEXT NOT NULL,       -- creating|setup_ready|setup_running|ready|unknown|failed|removing|removed
  setup_index INTEGER NOT NULL DEFAULT 0,
  outcome TEXT,
  updated_at INTEGER NOT NULL
);
```

The row belongs to the generation that the pending launch/resume will dispatch, while
the Agent itself may still be generation 0 or the previous terminal generation.
Admission resolves `REF^{commit}` locally, without fetch, and freezes the resulting OID.
It also freezes each note/setup file as bounded bytes or canonical allowed
`path+size+digest`; dispatch verifies the digest before executing.
The current opt-in creates a new branch; an existing branch is rejected rather than
reset or renamed, as is a branch that collides with the branch namespace (an
existing `atlas` blocks `atlas/a1-task`, and vice versa). An explicit `--base`
wins; an omitted `--base` means local `HEAD`. Repository common-directory
identity is frozen alongside the source checkout.

One naming standard, implemented once in the shared core and finalized in the
Store with the admitted Agent ID:

- `project`: the project's logical name as a slug (the owner's `[desktop]
  display_names` entry, else the repository name of its `origin` remote,
  else the folder of the repository's main checkout); registration IDs name
  nothing a person reads and are never used;
- `branch`: `<project>/<agent>-<task-slug>`, where the task slug is an ASCII,
  bounded slug of the launch prompt's first line (`task` when empty);
- `worktree path`: `<data_dir>/worktrees/<project>/<agent>-<task-slug>`.

A requested branch name still wins, but even an explicit branch lands in the
standard directory. Generated names are readable, unique per
Agent, and never derived from caller command-id syntax: uniqueness comes from
the admitted Agent ID, which no caller supplies. Windows device names
(`aux`, `con`, `nul`, `com1` and the like) are escaped, and the complete
destination path stays inside the Windows length limit, so generated names
are always launchable. The daemon freezes a unique
readable placeholder under the same parent; the Store finalizes the exact
branch and leaf with the admitted Agent ID before the row is inserted, so
same-ID retries read back the retained preparation instead of preparing a
second worktree. `swarm show` carries the standard branch and worktree through
the Agent's retained `branch` and `workspace` fields.

Flow:

1. create/retain the Agent projection and workspace row for `target_generation` with
   phase `creating`;
2. run `git worktree add` against the frozen OID with sparse checkout disabled;
3. verify the exact path, branch, frozen HEAD and empty porcelain status
   including untracked and ignored paths. Set `phase=setup_ready`, `setup_index=0`;
4. before setup entry `i` starts, commit `phase=setup_running`, `setup_index=i` and its
   frozen argv/file identity; only then spawn it in
   `workspace:<agent>#<target_generation>` custody;
5. exact success commits `setup_ready,i+1`; after the last entry, commit `ready`;
6. only a `ready` row whose target generation matches the start command permits that
   generation's native dispatch.

A preparation error with an exact creation marker proving no Git write or setup
dispatch settles the command and workspace with the original error code and message.
A refused Resume restores its exact prior `ready` marker. Later Resume attempts
verify Git and worktree identity again before native dispatch.
A Ready Resume whose retained marker cannot prove a no-write refusal settles
unknown on a preparation error, retaining the error and fencing native dispatch.

A lost Git response is settled by exact worktree/path/branch/OID readback. A failed
checkout can leave its new Git branch after Git has removed the worktree. Cleanup
requires the creation command's branch reflog marker, the frozen tip and no other
registration. Rollback checks a newly created worktree with porcelain status and
removes it with ordinary Git removal before compare-deleting the exact ref.
With quiescent custody, a rollback refusal or failed readback settles the launch
as failed. The launch diagnostic retains the verification failure and any
separate rollback failure. Unremoved artifacts remain available for cleanup.
A failed checkout reports its exit code once readback proves that the checkout and
branch are absent. A setup entry
has no generic readback: after restart or lost process result, `setup_running` becomes
`unknown` unless exact platform evidence proves the process never started. It is never
rerun. Uncertain effects remain unknown pending supported Owner resolution. This needs
one row and one running marker, not a `workspace_steps` journal or artifact service.

Cancellation stops the current custody. Native start never begins from a non-ready or
wrong-target-generation workspace. Removal requires terminal Agents and a clean
checkout apart from allowed relative scratch paths. Worktree removal preserves
the local branch name and exact tip, including for an unmerged branch.
Cancellation is committed before signalling custody and serializes with process spawn.
Force shutdown records a diagnostic if a direct cancellation cannot be settled.
After restart, an exact start/stop cancellation binding on an unknown creation
settles when the durable `native_git_write_dispatched=false` marker rules out a Git
write and the creation stage rules out setup dispatch for that attempt. Recovery
retains uncertainty and the original bindings when Git or setup effects may have
occurred. It never repeats either effect. For cancelled creation with a possible Git
write, see [Owner resolution](api.md#input-turn-and-unknown-gates).
An invalid cancellation candidate remains unresolved and records
`workspace.cancel_recovery_skipped`. Recovery continues with the remaining rows.
On Windows, step completion waits for the owned Job to empty. On supported Linux,
successful leader completion and an empty observed process group permit degraded
completion; escaped descendants remain possible. A signal-zero group query is
observation only and never grants permission to signal a stale numeric PGID.
`daemon.status.workspace_custody` describes the current host's capability, not a
retrospective assertion about historical commands. Unknown prior Unix custody after
daemon replacement cannot be promoted to an exact stopped result.

Runtime operation slots retain current preparation/cancellation/removal evidence and
unconfirmed process quiescence. Obsolete or settled slots retire only after all
operation, cancellation and run references release them, under the same lookup and
startup/cleanup serialization used for registration. Durable removed rows remain;
they do not require an idle runtime slot. Recovery and FORCE cleanup consume bounded
pages of pending preparations, selecting actionable phases before allocating slots.

`worktree.remove` uses normal command admission. Cleanup planning and apply use
the same eligibility checks as direct removal. The Store checks the terminal owner,
overlapping borrowers, current assignment binding and settled preparation in the
transaction that sets `removing`. Admission to the checkout or its descendants
stops at that phase until settlement. Removal requires at least 60 seconds since
the last owner or borrower ended. Git must confirm the exact managed repository,
worktree and branch identity, plus a clean index and worktree apart from regular
untracked or ignored files in frozen scratch paths. A refused removal carries
its stable code, the originating bounded and sanitized message, and the failure
location in `where`.
The cached path inventory includes Git links. Cleanup accepts empty or absent
submodule directories and retains populated submodule checkouts.
Cleanup checks tracked state and separately streams the untracked and ignored
path inventories under the Git listing limits. When scratch is present, it
rechecks the inventories and worktree identity before authorizing deletion.

Once an exclusive filesystem write guard is established, apply rechecks Git,
deletes approved scratch files and runs ordinary non-force `git worktree remove`
with its final clean check. The existing Git mutation path sanitizes the effective
configuration of the source checkout and the target. A host that cannot establish
the write guard keeps the checkout. On Windows the existing removal row retains
the original directory permissions before the guard changes them. Recovery
restores those permissions for remaining directories using their retained identities.
Exact path and registration absence plus the retained branch settle `removed`.
Before dispatching Git removal, Swarm records the branch tip and requires the
same tip in the final readback, including after restart.
For an already missing checkout, Swarm requires two absence observations at
least 15 seconds apart from the same readable parent before removing the
stale Git registration or settling the row. An unavailable parent keeps the
row unsettled. When the registration and branch are already absent, verified
absence settles `removed` without dispatching Git writes. The outcome records
`directory_absent_before_remove`, `branch_absent_before_remove` and
`branch_retained: false`. A confirmed disappearance also includes the last
available retained Git observation, which can show that the checkout had
changes before it vanished. The daemon's missing-worktree reconciliation
admits one removal at a time and advances after the command settles.
A `worktree.remove` that a previous daemon left `queued` behind the
repository write lane is re-admitted by restart recovery, so it is
dispatched in order and never strands later launches in the same
repository.

The Owner can settle a legacy `worktree.remove` that is still unknown with
`source_integrity: removed workspace branch was not retained` using
`swarm cmd resolve ID --status completed --reason TEXT`. Swarm requires the
exact workspace and command to record that no Git write was dispatched.
It verifies the directory, registration and branch are absent, then settles
the command and workspace in one transaction.

Keep the row so a later resume cannot treat the deleted checkout as an
ordinary shared workspace. A deadline or failure after
possible mutation is `unknown` until exact readback proves removal. No effect is
replayed.
Before filesystem changes, wait for the checkout's terminal Git sources to drain and
close, including subdirectory borrowers. This orders final hint attribution before
removal cleanup and prevents a stale observation from recreating removed provenance.

A workspace identifies the concrete checkout, branch and frozen base OID, using
the existing preparation receipt when Swarm creates it. The receipt preserves the
creating Agent and generation. The current workspace binding to a manager
assignment is independent of that receipt. Workers share the manager's prepared
worktree through the parent link. Cleanup planning, cleanup apply and worktree
removal respect the current binding and nonterminal borrowers. Releasing the
assignment preserves the preparation receipt and any existing borrowers.

## Git and filesystem monitor

The current increment shares one Git monitor per admitted live worktree, which must
be the exact Git worktree root. Monitoring commands use a hermetic read-only Git invocation:

```text
git -c core.fsmonitor=false --no-optional-locks <command> ...
```

Current local monitoring invocations use dedicated leaf custody. Windows installs a private Job's
`ActiveProcessLimit=1` before resuming the suspended Git process. The runner resolves the
actual installed executable, such as `mingw64/bin/git.exe`, rather than a launcher that
needs a child process. Linux requires equal real/effective/saved nonroot UIDs, no
capabilities, `PR_SET_NO_NEW_PRIVS` and irreversible `RLIMIT_NPROC=0`. Root/privileged
Linux and other Unix contexts return explicit unavailable; there is no unfenced retry
or fallback. These restrictions apply to Git reads, not normal harness/plugin custody.

Diff/log commands additionally pass `--no-ext-diff --no-textconv`. Monitor-built Git
commands also set `core.safecrlf=false`, so a CRLF conversion warning for a dirty file
under `core.autocrlf` is not read as a failure by the leaf fence; none of them checks
files into the index, where that setting applies. The daemon sets
`GIT_TERMINAL_PROMPT=0`, disables editors and aliases, and never invokes repository
hooks, fsmonitor helpers, external diff drivers or textconv filters. It never permits an
unapproved/custom remote-helper scheme: any optional network read uses an
owner-configured remote whose complete effective URL set passed the validation in
[automation](automation.md#actions-are-commands). Approved built-in Git transports may
use Git's normal transport helper internally; “disabled remote helpers” never means
silently disabling the configured HTTPS/SSH transport itself.

- refresh on debounced filesystem invalidation and poll interval, at most once per 2 s;
- `status --porcelain=v2 --branch -z` for current state;
- index entries are parsed as a NUL-delimited stream under record-count,
  record-size, memory and time bounds. Listing-limit errors report the observed
  record and byte counts and the applicable limit. The daemon logs the first
  observation of a listing-limit error for an active worktree. Filter attributes
  are checked in bounded path batches and a second index digest fences the sample;
- bounded `log --no-ext-diff --no-textconv` on HEAD movement from the last stored HEAD,
  so commits made while daemon was down are recovered; no prior baseline yields an
  explicit gap;
- committed-but-unmerged paths are computed with
  `diff --no-ext-diff --no-textconv` against frozen `base_oid` when available;
- remote refs change only after explicit safe fetch or optional validated read-only
  `ls-remote` poll;
- slow status backs off and reports source health;
- monitor never writes index, refs or objects.
- an unchanged probe writes no Store rows: with no relevant native hints, Git events or
  attribution changes, the source cursor stays put (global event high-water is not
  persisted), and the workspace HEAD baseline changes only on a HEAD transition.
  Hint pages select that Agent generation's exact completed native file changes before
  applying their row and byte bounds; unrelated journal traffic cannot starve later hints.
  A process-local watermark rejects samples older than the last accepted observation,
  including no-op observations. After restart, retained Git views and HEAD transitions
  provide the durable lower bounds; an unchanged observation needs no durable heartbeat.

The current subset polls local state at least 2 s apart without submodule recursion;
slow/error probes back off up to 30 s. A binding's final read (its generation is
terminal and its native stream closed) bypasses that backoff but keeps the 2 s minimum,
and the stream's committed `source.closed` wakes the poller, so a stopped Agent's Git
source closes within one probe interval even when its workspace is not a Git
repository. CLI reads return the cache without forcing a scan.
The target requirements above remain binding. `git-status AGENT` / `agent.git` returns
the current generation's cached view; `edits AGENT` / `agent.edits` pages retained
attribution, including historical generations for an authorized owner.

Native filesystem invalidation watches the admitted root on Windows/Linux. Linux
registration disables symlink following; notify's option does not apply to the Windows
backend. Notifications never establish path containment: Git/path checks remain
authoritative. Notifications coalesce into wake hints; access-only events do not
trigger a read feedback loop. Backend errors or rescan notices latch degraded watcher
health, while periodic polling remains authoritative. Neither a healthy watcher nor a
notification proves fresh Git coverage or counts as model progress.

The Git view exposes `filesystem_invalidation.state`: `healthy` means an active watcher
without reported degradation; `degraded` means an active watcher reported an error/rescan;
`unavailable` means an active watch slot has no watcher; `inactive` means the
watch slot is inactive or absent. Nested `retry_after_ms` reports the remaining watcher
retry delay for degraded/unavailable states and is null or absent otherwise. This is
independent of the top-level Git probe retry delay and cache `available`/coverage fields;
watcher degradation alone does not invalidate a usable Git sample.

Freshness and coverage are independent. The view exposes `observed_at`, `age_ms`,
`ttl_ms` (4,000), `available`, `clock_regressed`, `retry_after_ms`, `error` and `source`.
No sample, a probe/monitor error, clock regression or an expired sample makes the view
unavailable; cached values may remain visible as diagnostics. An available sample can
still have `complete=false`. The persisted `git.changed` representation is budgeted to the
single-event bound: when the accepted snapshot cannot fit, the Store persists the
deterministically sorted dirty prefix (then committed paths) with `complete=false`, forcing
`base_complete=false` when committed paths were cut, so a large legitimate tree still
commits instead of failing every poll. The persisted view also carries `view_digest`, the
digest of the full canonical view, so a change outside the retained prefix still commits a
new view. Commit delivery uses a durable OID ledger scoped to one Agent generation,
independent of retained `git.committed` events and dirty coverage. Migration seeds it from
retained events for open Git sources on current Agent generations. The ledger and each
new commit event are written in the same Store transaction; event pruning does not permit
redelivery. An OID delivered before a rewind remains suppressed if it reappears later in
that generation. Source closure or a generation replacement clears its entries.
A partial persisted view neither replays nor gates a commit. A repeat poll compares equal
and writes nothing.

The cached `agent.git` view includes `line_counts` only when the workspace work probe
has complete status and a usable base. It reports files changed, insertions and deletions
against that workspace base; a missing or partial work probe leaves the field null.
These are workspace measurements, not per-Agent authorship facts when more than one
non-terminal Agent admits the checkout.

Executable Git filters are disabled, and applicable filter
attributes make raw status partial; bounded attribution-path coverage can also make a
snapshot partial. An empty partial dirty set does not prove the workspace clean.

The in-memory Git cache has no inactive diagnostic allowance: a workspace entry retires
after its last current or settling binding releases it. Final hint attribution and
source closure precede binding retirement. Registration and eviction share a registry
gate; watcher/backend disposal happens outside map and Store locks. A retired cache
reads as unavailable with inactive filesystem invalidation; its durable baseline and
edit attribution remain available from Store.

`history_complete` / `history_gap` describe commit coverage separately. Recovery uses
the persisted workspace HEAD baseline, with at most 64 commits per probe and bounded
time/output. No baseline or non-ancestor history records a gap; bounded/truncated history
must not be presented as complete. Current Git sets rebuild in memory after restart.
Per-Agent edit attribution is durable because Git cannot reconstruct authorship in a
shared checkout:

```sql
CREATE TABLE agent_edits (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  workspace TEXT NOT NULL,
  path TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,path)
);
```

Only an authoritative, generation-matched native completed `fileChange` tool event can
supply edit hints. Codex and Muse retain bounded path metadata, not patch contents; the daemon
validates each hint against the admitted workspace's filesystem containment. Store
verifies the exact native cause before publishing `file.changed` and upserting attribution.
A current dirty-path intersection is not required: native writes reverted before polling
are still consumed, then explicit clean proofs can remove their provenance. Dirty sets,
commits or shell activity alone never establish authorship. Truncated/redacted/unusable
hint metadata records a coverage gap.

Rows, including terminal/older generations, are removed only by explicit clean-path
proofs from a complete scan; ignored, skip-worktree or otherwise unproven paths do not
authorize cleanup. Git polling and `git.changed` / `git.committed` alone do not advance
OS samples or model progress. Validated `file.changed` retains its native cause's timestamp.
Exact managed-worktree removal also clears attribution for that absent path, in the
same transaction as the `removed` result. Failed or uncertain removal clears nothing.

On 2026-10-27, bounded Windows checks with Git 2.55.0.Windows.5 observed partial trap
status without authorship or trap execution; SHA-256 inventories of `.git/index`, `HEAD`,
refs and objects were unchanged. Removing the fixture traps produced complete dirty
status, still without attribution. Restart recovered the commit made while the daemon
was down. A native `note.txt` edit retained its original cause timestamp; generation-1
attribution survived immediate stop/resume to generation 2 with no additional turn.
It remained queryable after terminal settlement, then a native adapter's complete clean scan
removed it after explicit restoration. All exercise Agents stopped and owned daemons
exited. The Windows leaf-process regression passed 1/1: the fenced parent ran, while an
existing absolute child executable ran in the ordinary-custody control and was blocked
by the leaf Job. This evidence covers those paths on Windows, not full L7 qualification
or Unix.

### Read-only progress fingerprint

Goal progress uses:

```text
HEAD + sorted(dirty/untracked path, type, size, content hash)
```

Only already-dirty/untracked, nonignored, nonscratch paths are read. Symlink target text
is hashed without following it. File/byte caps return `unavailable`; incomplete data
never counts as no progress. No temporary index, `git add`, `write-tree` or object write.

## Provider holds

Provider holds are durable local coordination state. Their view and restriction shapes
are the shared Core [`ProviderHold` and `ProviderRestriction` DTOs](../../crates/eliot-core/src/routing.rs);
keep the serialized field catalog in Core. Derive scope only from the frozen profile snapshot: explicitly
selected account name first, otherwise exact profile name. A shared adopted proxy is a
capacity scope only, never a provider-hold scope: a per-turn restriction proves only
the admitted account/profile restriction, not that the whole proxy service is held.
Never infer a vendor account from credentials, endpoint responses or model metadata.
Existing `proxy:*` hold rows remain query-visible as historical scopes; no migration
or clearing is implied.

Create or update a hold only from a structured restriction attached to the exact
generation- and turn-bound controlled `turn.completed`. Generic failure, native error
text, HTTP status, or a vendor-owned model-catalog response is not a restriction and
does not create a hold.
Antigravity's explicitly documented exception is the complete native quota
warning matched at its plugin boundary when ACP supplies only `end_turn`;
see [the exact fallback and captured surface](../harnesses/antigravity.md#facts-and-recovery).
The plugin emits the same typed, exact-turn restriction; Store never parses text.

The pinned Codex 0.161 projection recognizes three exact structured codes:
`rateLimitExceeded` maps to rate-limit, `usageLimitExceeded` to quota, and
`unauthorized` to auth. Four typed `codexErrorInfo` variants map to rate-limit only
when their typed `httpStatusCode` is 429: `httpConnectionFailed`,
`responseStreamConnectionFailed`, `responseStreamDisconnected`, and
`responseTooManyFailedAttempts`. A bare HTTP status is not enough; only these exact
typed variants with `httpStatusCode:429` map to rate-limit. `ModelRemoved` remains a
valid structured Core restriction kind, but the
Codex 0.161 classifier does not infer it from an unrecognized native variant, HTTP 404
or message text.

A hold is `Held` or `Probing`. It blocks new provider-backed launch/send/steer work in
that scope, including queued dispatch. Future Goal work that admits provider-backed
commands uses the same gate. A launch with linked input must reject before either effect;
resume without new text, readback, request replies, interrupt and stop remain available.
Resume with text is a linked send and remains gated. Steer remains bound to its exact
active turn and generation.

The [Owner quota reserve](configuration.md#owner-quota-reserve-872) is a separate
daemon admission policy for new automatic launches. It uses fresh passive quota
readback and the existing routing and launch wait paths. Skipping a candidate on
reserve or unknown quota creates no provider hold, consumes no probe claim and
does not affect work already admitted.

Rate-limit and quota holds permit at most one SEND probe claim per frozen provider
scope in Store. An explicit `probe=true` SEND uses ordinary authorization and exact
target/scope/generation fences. With no known reset and an eligible deadline,
a fresh launch with input can claim its linked SEND as the probe. Routing reconsiders
the scope at that deadline. Admission commits the claim before native spawn and
dispatch rechecks the exact launch and SEND binding.

Daemon maintenance also probes Codex profiles
through the same SEND admission and native dispatch path, with a fixed minimal prompt
in an isolated private chat that has no project, registered worktree, or user task.
Its frozen profile snapshot sets `_provider_probe=true`, `permissions=read-only`,
`approval_policy=never`, `native_subagents=deny`, `mcp=minimal`, and `mcp_allow=[]`.
The native adapter skips the Swarm skill, shell/web/workspace instructions and Agent
credentials for this mode. Its linked SEND claim and launch commit atomically before
native spawn. The outer internal launch marker carries the frozen scope and cannot
be submitted by a client.

A provider-supplied `retry_after` is a known reset time. Bounded maintenance removes a
held rate-limit/quota row at or after that time and records `provider.hold_cleared`.
An unresolved `Probing` claim is never cleared by time. When no reset is supplied, the
Store persists `next_probe_at` and the completed-probe count. The initial probe is due
after one minute. Known failed probes back off to five minutes and then thirty minutes.
Each uninterrupted hold allows six automatic probe admissions. The final slot records
`provider.hold_probe_budget_exhausted`. The Owner can see the scope in
`daemon.status.provider_probe_attention` and use the existing hold controls.
A provider-supplied reset still clears the hold when due.
Each admitted probe records `provider.hold_probe_started` and known
completion records `provider.hold_probe_finished`. A successful exact-turn native
completion with no restriction clears the hold. A known failure keeps the hold,
records the reason, releases only its claim and persists the next backoff while
budget remains. Startup sweeps ended
maintenance Agents. Once the exact Agent's custody drains, an unresolved automatic
probe releases its claim, records `provider.hold_probe_outcome_unknown` and pauses
automation for Owner review. Maintenance also pauses a task-bearing launch's exact
unresolved probe once the bound generation ends and its native custody drains, or
after the Owner resolves the exact inactive turn. It preserves the Agent, turn and
workspace. An uncertain probe is never replayed.
If a minimal request cannot be prepared safely for the frozen profile, the daemon
paces background preparation with a five-minute defer. The persisted deadline
for a fresh launch and the admission budget stay available. A maintenance probe
that exceeds the turn wait budget stops its owned Agent
and retains the claim if the exact native outcome is unknown. When retention has
pruned a probe's admission event, the terminal
scan fails with `source_integrity` instead of clearing the hold on incomplete history;
the owner corrects the stuck hold with the existing `provider clear-hold`.

Auth and ModelRemoved holds are not probe-eligible and do not expire automatically. An
owner clears them with one of the closed corrective evidence values:
`credentials_updated`, `configuration_updated` or `owner_verified`.
Clear uses a same-scope compare-and-swap against the expected probe command, including
explicit null; it cannot clear an unresolved probe. The mutation ID binds the clear
request, so an exact retry returns the retained result and changed evidence conflicts.

`agent.list` and `agent.get` project the hold matching that Agent's frozen account or
profile as `AgentProviderHold { kind, state, retry_after, updated_at,
waiting_for_active_turn, next_probe_at, held_since }`; shared scope and probe-command
identity remain on the owner-only provider-hold read. They also project
the latest `turn.notice`, or the latest failed terminal `native_error`, as
`AgentNativeNotice { generation, turn_id, kind, message, truncated, observed_at }`.
The notice must match the row's generation and active or most recent turn. These views
read the existing hold row and committed native events; they add no timer or polling
source. A retry/reconnect notice is omitted from an active turn once native progress is
newer than the notice. `waiting_for_active_turn` is true for a probing hold only while
the active turn has no native progress newer than that hold's `updated_at`; the hold
itself remains visible because it still gates future dispatch in the matching scope.
When an Agent is idle, a failed provider notice is omitted after its matching hold has
been cleared. A newly started turn is projected only against its own turn binding.
`retry_after` is the provider's known reset. `next_probe_at` is shown as the next
automatic probe time when the reset is unknown. Neither field promises replay of an
uncertain probe.
The desktop uses the existing daemon attention incident for its attention tone:
the configured no-progress window (default 900 seconds) and source-health triggers are
unchanged.

## Liveness

Only generation-matched model facts update `last_progress_at`: turns, messages, tools,
plans, file edits and exact request/answer transitions.

| Observation | Conclusion |
| --- | --- |
| no progress while busy and authoritative source healthy | stalled flag only |
| waiting on a pending approval/question | waiting; not stalled solely because no model output |
| stream disconnected, process/service alive | source stale; state unchanged |
| process exited, source not drained | terminal pending; no loss inference yet |
| source drained/gapped + exact Agent custody empty or shared session absent | session lost for that generation |
| parent idle, child busy | parent idle; tree incomplete |
| bare provider HTTP 429 | no restriction inferred without a typed native variant |
| old generation fact | historical diagnostic only |

## Automatic attention

The daemon emits a typed `x.user.agent.attention` report when an authoritative
native lifecycle source becomes unhealthy or a validated `agent.lost` event is
committed.
Those triggers do not wait for the no-progress window. A busy Agent with a
healthy authoritative source becomes a no-progress suspicion after
the effective `no_progress` window. This starts the reaction ladder for the
affected turn.

The `no_progress` and `no_code_progress` windows use the 90th percentile
of the longest quiet gap per turn across the latest twenty successful turns
of the same profile and project. Turns with an attention incident or
an intervention are excluded. Each kind needs at least three samples with
retained source coverage. Its window is at least 900 seconds and at least the
configured threshold, which together determine the fallback. Reports include
the observed gap and history basis when available. `swarm doctor` shows the
windows and the number of successful turns that contributed to each one.
Stopped Agents do not open source-unhealthy incidents; existing ones retire
without a recovery claim, while no-progress may still recover on explicit stop.

Native child activity also feeds a manager-health incident (`children_unattended`).
The exact controlled producer of registered native descendants is their nearest
controlled ancestor, including nested observe-only children. Aggregate its current
generation's unfinished children: notify when the manager is idle or stopped, or
has no committed native progress through the suspicion window. A stalled or lost
child also warrants attention while the manager is busy. All-finished children,
children that ended with their producer generation (`ended_with_parent`,
[core](core.md)), and a progressing manager with healthy progressing children, open
no child incident.
An open turn with a lost/gapped source remains unfinished; silence is never completion.

These reports capture complete unfinished/lost/stalled counts and at most sixteen
child activity samples, with explicit truncation. Each report is committed as a
typed `x.user.agent.attention` event whose top-level Agent/generation binding and
event-time subject snapshot are captured from the current generation in the same
Store transaction as its Applied settlement; stale evidence (unknown Agent or
generation drift) is rejected and never mailed. The wire shape stays compatible:
kind `x.user.agent.attention` with the report at `event.data.data` and its exact
active turn at `event.data.turn_id` when available. A valid
explicit `on.event` rule can target the incident through that binding without
weakening stale-generation checks. Child-cohort reports go to the live manager
responsible for that cohort. Other reports go to the subject's live manager.
The Owner receives the notice when no such manager can act. The subject's
diagnostic facts stay in the structured event. Mail and Owner notices carry
the main fact and an actionable question. A no-code-progress notice uses
the elapsed time since the recorded code progress basis.
At most one incident and one
recovery notice are ever sent per incident, so an unchanged unread set cannot
generate further wakes. Binding an advisory attention event never starts a Goal
or reoffers mail; automatic idle/Goal/mail nudges ignore the attention kind.
Sweeps include idle/stopped managers with current native registrations;
the candidate lookup checks registration existence without traversing child turns.
Descendant capture visits at most `NATIVE_CHILD_ATTENTION_PAGE_SIZE` registrations
per Store lock, including finished/no-facts rows; the registration cursor advances
over every visited row. Store is released between pages and the manager generation
and parent turn are rechecked after each page and before publishing. Each capture
freezes its last registration ID so concurrent child creation cannot extend the pass
indefinitely. Open-turn reads stop at the first indexed match; latest terminal and
native-progress reads likewise use indexed limit queries, never per-child counts.
A transactional `agents.native_child_revision` stamp tracks relevant child
registration, turn/progress and native source changes, including pruned child
progress facts. Capture validates that same stamp across every page and final
readback. A mutation discards the capture for the next sweep. The stamp is checked
again under Store before freezing an incident or retiring its finished cohort, so
an omitted new child or a changing existing child cannot falsely clear/rearm it.
Unrelated mail, CPU and Git events do not invalidate this child-cohort stamp.

One frozen report covers an unfinished child cohort per manager generation. Parent
turn changes, mail wakes, manager progress and child fanout do not rearm it. It retires
only after no observed child turn remains open, without recovery mail that would wake
the manager again. Unknown source health alone never clears open work. This shares
the existing bounded incident set, reservations and exact-ID delivery retries; no
per-child incident registry or unbounded report fanout is introduced.

Before reporting, capture the Agent generation, active turn, pending requests,
source owners/health, and model-progress time from Store. Child-cohort reports
recheck the generation, turn and child summary before freezing. Controlled-Agent
source/no-progress reports release Store before the exact-generation
`agent.snapshot` readback, then recheck the generation and turn before publishing.
Readback is read-only and preemptible by a subsequent
host control. The daemon copies observation basis and freshness only when the
plugin explicitly supplies them and the exact generation/turn binding matches;
otherwise it reports `plugin_snapshot` and `freshness_unspecified` without
inferring semantics from the adapter name. A validated `process_exited` fact is
retained only from that exact snapshot binding.

Reports also reuse the existing current-generation monitor view for bounded OS
process records, sample age, completeness, coverage, and truncation. These are
cached process observations; CPU activity is not model-progress evidence.
Failed, preempted, stale, or binding-mismatched native readback remains unknown
and never changes Agent state.

Pending approvals/questions remain `waiting`; waiting for a user response alone
is not a no-progress suspicion. A known active tool is reported as
`active_tool` with progress `unknown`. It does not suppress suspicion
indefinitely: if that observation remains unchanged through the configured
window, report `active_tool` with progress `unknown`. Tool coverage that is missing,
gapped, or truncated remains unknown.
Only one report is emitted per active incident. Recovery requires positive
progress or state evidence; loss of observability or an unhealthy/unknown source
alone does not produce recovery. Superseded-generation incident state retires
without asserting recovery; any obsolete delivery that cannot be confirmed
remains explicitly unconfirmed. The bounded transient incident set is in-memory;
there is no separate status registry. Each incident/phase freezes its report
and mutation IDs until event and notice delivery settle, so retries reuse
the same facts and command bindings. Inbox/owner notices carry a short bounded
diagnosis and link to the structured report. A public `emit` naming the reserved
`agent.attention` topic is rejected; only the internal attention producer may
use the bound commit.
At startup attention drains retained loss evidence (currently-`lost` Agents
with their latest `agent.lost` sequence, most recent first) in bounded,
resumable pages through the existing `AgentLost` handling. Rows whose exact
`agent.lost` evidence was pruned carry no sequence and are skipped, never
synthesized. Loss-evidence reports derive their incident id deterministically
from the loss event sequence and Agent/generation, and their mutation ids
deterministically from that incident id, phase and the loss event timestamp,
so a post-restart re-report of the same loss reuses the same ids and the
existing command/delivery idempotency makes it a no-op without a second
incident ledger. This covers losses published before attention subscribed as
well as a crash between the recovery commit and consumer startup.
The same identity survives a saturated pending-check map at runtime: a loss
that cannot enter the full map parks its exact Agent/generation/sequence in
a bounded resumable loss queue instead of degrading to a generic sweep, and
the next reconcile promotes it through the existing `AgentLost` handling
(only advancing a loss once its key is retained in the map) and re-pages
retained loss evidence from the Store when the queue or a lagged event bus
may have missed it. Re-paging resumes from a retained cursor at the last
fully-queued row instead of restarting at the newest, so the oldest loss
cannot starve while newer rows refill freed slots. Sweeps skip Agents with
queued loss evidence, and a sweep that meets an unqueued lost Agent reports
through the same loss pipeline under the same deterministic identity (the
retained sequence is re-paged; pruned evidence is skipped, never
synthesized), so no generic check can preempt the loss-anchored report with
a different incident id. Only genuinely unrecoverable saturation (queue and
map both full) drops a loss, and the retained Store evidence re-pages it
from the cursor on the next reconcile. A delivered loss report leaves the
transient set at once: delivery is terminal for a lost generation (no
recovery follows), so its reservation slot is freed for retained losses
behind it. When a generic source-unhealthy incident is still open for a
generation that is then reported lost, any pending delivery settles first
and the entry is superseded by the deterministic loss report, so the loss
identity is persisted before any restart and a re-report stays a no-op.
Each attention escalation thread wakes its recipient only once per thread
(recorded durably when the notice is claimed); an unchanged unread incident
set cannot generate further wakes until new evidence arrives on a new thread.
Retained pre-fix subject copies (bare attention threads with no bound
incident event) never wake.

### Automatic reactions

The daemon uses the ordinary command router for each of these reactions. Settings
are per kind under [daemon attention](configuration.md#automatic-attention-settings).

| Incident | Reaction |
| --- | --- |
| `no_progress` | Read native status, queue one short question for the bound turn, and route one attention notice. With `interrupt = true`, wait another full quiet window before interrupting and routing the notice. A newer Owner send or steer bound to that turn vetoes the automatic interrupt. |
| `no_code_progress` | Ask once with the duration since the last worktree change and the available tool and message counts, then route one attention notice. |
| Process loss | After exact custody exit and reader drain, reopen the retained idle session without input once. Startup uses the existing promptless restart reconciliation and honors `on_restart`. Unresolved work stays fenced. |
| Provider refusal | Keep the existing provider hold, opt-in next-preset routing and bounded due probes. If the hold's automatic probe budget is exhausted or a probe stays unresolved, send one Owner notice. |
| `session.growth` | At an idle boundary, a configured profile context ceiling compares the latest actual per-call `context_used_tokens` sample with its absolute token limit. Allow one automatic native compaction per generation, claimed in retained `turn_attention_steps` before dispatch. If native compaction is unavailable, use the existing same-profile switch and A6 brief when the source is safe and its task succeeded, even when legacy `switch = false`. Route one attention notice; do not repeat ceiling effects or a later overlapping legacy warning in that generation. Without a ceiling claim, the legacy ratio/file-size reaction remains unchanged and switch fallback stays opt-in. |
| Repeated command failure | Three matching failures of the same command and output in one turn put one concise fact and question into that turn. |
| `children_unattended` | Queue one report for the manager. |

Reactions bind the incident's Agent generation and, where applicable, its exact
turn. On a hook-based adapter, the question waits for the next native safe point.
Ending the turn cancels a question that is still queued. Each step uses a retained
claim or command identity. Later timed stall episodes in the same turn share one
question and one routed notice. An uncertain effect stays unknown. `swarm why` shows
recent steps with command outcomes when retained.
Its JSON `attention_steps` includes the incident id, generation, turn, step, and
available command id.

### Git-aware work summary

The Git monitor computes a bounded per-Agent work summary exposed as `WorkSummaryView` ([observe.rs](../../crates/eliot-core/src/api/observe.rs)) on `agent get`, `agent stats` and the `swarm ls` WORK column.

WorkSummaryView carries, for one Agent generation versus its frozen base_oid:
- commits_ahead: commits ahead of base_oid;
- files_changed, insertions, deletions: git diff --numstat base_oid..HEAD across committed plus dirty work, plus bounded dirty/untracked path coverage;
- last_write_at: time of the last worktree write (from validated file.changed attribution or a dirty/digest change);
- last_commit_at: time of the last commit (from git.committed events or HEAD movement);
- tool_calls, messages: counts in the current turn.

When Git or a cap prevents observation, `available` is false with a reason and every Git counter is null, never a fake zero; only `tool_calls` and `messages` may still be present.

The work probe runs inside the ordinary Git probe: only after the workspace watcher reports a change, at most once per 2 s per workspace, under the probe's read deadline. It adds `merge-base --is-ancestor`, `rev-list --count` and one `diff --numstat` against `base_oid`, and hashes dirty files with bounded untracked coverage (512 KiB per file, 4 MiB in total). Past a cap the summary is `unavailable`, not partial.

### Git-aware attention: no_code_progress

A busy Agent whose tool and message activity continues but whose worktree
has had no write and no commit for the effective `no_code_progress` window
gets a distinct diagnostic. The window counts from the later of the last
write or commit and the active turn's start. Complete model inactivity uses
the `no_progress` window.
- suppressed for read-only and audit profiles (is_read_only_or_audit_profile) and for Agents without a worktree;
- requires a current Git observation: while the work probe is unavailable (for example a capped digest), the retained last write or commit time stays history only and no no_code_progress is raised from it;
- retires as soon as a write or commit lands (last_code_progress_at advances);
- reuses the existing attention machinery: a bounded incident per Agent/generation, no new timer service.

The report carries `code_progress_at` (that window's start) and the turn's `tool_calls` and `messages`, so an agent actively using tools but not writing code is clearly distinguished from an idle one. The desktop shows the open report on the Agent's Monitor row, its sidebar row and in Details ("Working without writing code for 14m") until the daemon retires it. The sidebar reads the Monitor's reports: the panel's refresh reads them while it is open, the sidebar's refresh runs the same incremental read while it is closed, never both.

## Restart reconciliation

Callbacks and timers only enqueue stable keys. Deduplicated reconciliation reads Store
and native current state, one key at a time:

- `agent:<id>`: custody, attach/snapshot, open commands/input/turn/request, source owners;
- `workspace:<id>`: target generation, phase/readback without repeating unknown setup;
- `service:<name>`: adopted/managed service state;
- `provider:<scope>`: hold/probe state;
- `git:<worktree>`: rebuild current facts;
- rule/timer/Goal/mail keys: their owning projections.

Dispatched effects become unknown until verb-specific readback. No reconcile pass blindly
repeats a native or external write.

Startup records an idle controlled generation as `stopped` when its native session
identity is retained, its generation has no open request, open or unknown turn, or
unsettled command, and prior owned-process exit and reader drain are durably proven.
Stop settlement and restart resume use the same generation proof. It requires
recorded exited processes, a closed OS source and closed native sources, backed
by the Host's owned-exit record or exact restart reconciliation. Recovery records
`agent.stopped` with reason `owned_exit_before_restart` and preserves the turn and
command history as recorded. A process still recorded as running at restart becomes
unknown. The daemon checks retained custody and the
[restart policy](configuration.md#agent-restart-policy) before reopening an
eligible idle session through normal resume admission. The new generation starts
without input.
Missing custody proof and unresolved effects leave the old generation fenced.

### Retained transcript generations

Committed `Turn` rows remain scoped to their exact Agent generation when the Agent
projection advances. The default `agent.result` read pages retained turns across those
generations in descending generation and immutable turn insertion order.
It reads Store history without reopening a native session or querying private
native history. [API contract](api.md#command-turn-and-result-reads)

Generation origin is committed with the transition. A daemon restart records
`restart_resume` and an Owner resume records `owner_resume`. Switch starts a
successor Agent with its own identity and records `switch`. A handoff to a prepared
generation with no retained turns can replace a launch or unknown origin with
`handoff`. Each transcript read stays within the requested Agent ID. Missing
historical origin is reported as `unknown`.

The desktop consumes the default cross-generation result and joins each turn to its
retained events using the exact generation and turn ID. The API supplies the markers
for the desktop renderer. It draws one quiet line, also its UIA name, where a
generation that did not start with the launch begins, for example "Continued after a
daemon restart" or "Resumed by you". A generation whose start lies in an unloaded
page shows no line.

## Views

The daemon uses the same supervisor for long-lived rule, Goal, retention,
process monitoring, Git, attention, mail reoffer and completion digest workers.
Each worker restarts independently after an unexpected failure. Retry delays
increase from 250 ms to at most 30 s. `swarm status` exposes worker health,
the last redacted failure cause and the number of scheduled restart attempts.
A run of at least one minute clears the consecutive-failure count. The fifth
failure in a row raises one Owner attention notice through the daemon's
notification path. Retries continue, and the log records any notice delivery
failure. Failure records identify the authenticated principal, an
unauthenticated caller, or the system producer, and name the RPC method or
internal operation/command. Each carries a bounded, redacted cause and available
typed failure diagnostics. Logs omit request payloads, credentials, and
unrecognized method text. Shutdown cancels pending retries. A worker restart
reconciles retained state and keeps the normal Store and router gates on effects.

`watch`, `top`, `show`, `daemon.status` query projections/events. They expose generation,
active/unknown turn/input/transition gate, source owner/cursor/gap, custody
capability/orphans, provider hold, workspace phase, requested/effective route and
Git/edit facts. A stale or unknown value is shown explicitly, never hidden or converted
to zero.

An Agent diagnosis includes its retained Eliot Goal status, pause or block reason,
generation binding when it differs from the current Agent, and any running or unknown
round marker. A blocked check includes its bounded observation and can explain the
diagnosis of an otherwise idle Agent.

The standard log records `daemon.command_storm` once when an authenticated
principal/method pair accumulates at least 32 refused submissions in its ten-second
window. Accepted attempts still contribute to the reported attempt and accepted
counts. Remote connection ids are retained only in a bounded in-memory set for
deduplication and never written to the log. The record includes numeric
`connection_count`, exact through 64; a value of 65 with `connections_overflow` set
means at least 65 distinct connections. One redacted representative rule id may use
the existing `source` field. Direct internal producers keep their method labels,
including internal mail and attribution notices, daemon archive, attention, and
admitted rule actions. The daemon
shares the existing 1024-entry repeated-failure counter and flushes qualifying storm
windows on closure or maintenance shutdown.

When the Router commits a third rejection of the same command verb for the same
Agent within one minute, it records `daemon.repeated_command_rejected` and one
daemon attention notice. Further matching rejections in that window add no
diagnostics. Each command keeps its own durable outcome and event.

Fleet usage cells (`agent.list`, `swarm ls`, `swarm top`, desktop Monitor)
sum uncached input, cached input and output on the [shared usage basis](core.md).
They prefer the newest terminal turn (`usage_scope: "turn"`) and fall back
to the latest cumulative snapshot (`usage_scope: "session"`) when that turn
has no usage. A subtotal missing any component is marked partial. Agent
usage summaries include usage from observed native descendants of the
requested generations within the caller's read scope. Child rows retain
their own usage and the project and tree summaries count each turn once.

The authenticated Swarm MCP tool boundary records semantic tool.started and
tool.completed events with the exact RPC method, call ID, origin, and exact
active Agent turn. The event data contains no RPC parameters, result text, or
error detail. A completed event uses status "refused" when the daemon RPC
returns an error; calls count from their start event. Only these daemon-origin
Swarm MCP events feed per-turn usage, so generic native harness MCP tool events
are never guessed or counted again. This covers Agent-authenticated stdio calls
across the harness adapters. Remote StreamableHTTP calls carry authenticated
remote Owner context but no local Agent generation or turn; their events remain
unbound and are excluded from per-turn usage.

The private crash directory holds up to 64 marker files of each kind.
Daemon startup consumes the previous `daemon.panicked` files and exposes the latest
as `previous_crash`. `daemon.status` consumes validated `plugin.panicked` files by
deleting them and keeps the latest eight consumed plugin markers for the current
daemon run. `swarm status` shows a summary for each.

`daemon.status` also exposes retention and drain information. Compact
`agent.list` reads and history-derived attention thresholds use SQLite read
snapshots outside the Store writer lock. Compact list requests wait for a reader
slot before scheduling blocking work, with up to four snapshots active at once.
Debug logs include attention sweep
duration and Store lock wait and hold times for retention and native event batches.
The
retention view reports the `eliot.db` main and WAL sizes, the configured
windows and `pruned_through`, along with the SQLite auto-vacuum mode,
total pages and reusable pages. The drain
report lists stored unknowns with no live process, turn or reader as
retained-unresolved — with resolvability and owner action — instead of
blocking on them; the stored outcome stays `unknown`.

## Swarm autonomy

[`autonomy.get`](api.md#agent-and-daemon-read-views) reports the Store evidence for
issue #883. Each row gives its source, counts and coverage (`complete`, `partial`
with a reason, or `unavailable` with a reason and no counts). Counts with partial
coverage describe observed evidence. List truncation is explicit. The report
contains no command text, message bodies, or thresholds for judging performance.

The window includes both endpoints. The Owner backlog is measured as of the
report end, using an age strictly greater than 24 hours regardless of the start
of the activity window. The daemon runs one report at a time on a separate
read-only SQLite snapshot. Indexed, capped source reads run outside the Store
writer lock. A source that exceeds its work cap reports partial coverage.

| Row | Evidence |
| --- | --- |
| Project policy | Retained Swarm Agent creation, project policy applied flag and explicit opt-out reason from admission (#868). |
| Steers | Retained steer commands classified by caller and automation cause. |
| Checks | Captured shell invocations and recorded Swarm Goal or fast project check runs. |
| Waits | Captured shell invocations matching wait commands. |
| Swarm fact notes | Deliveries, moves and cancellations of retained fact notes, with counts and confirmed corrections by kind. |
| Owner backlog | All current decision subjects, including snoozed revisions, with per-kind counts and ages from each source row's recorded transition time. |
| Usage | Retained normalized turn input and measured first-call context. The role is evaluated at turn completion. |
| Executors calling Swarm | Retained turn starts and authenticated Swarm MCP starts attributed to executor activity, grouped by profile. |

The attention worker samples liveness on startup and every three minutes (#871).
An unchanged Stuck candidate becomes reportable after two consecutive sweeps.
Swarm logs `daemon.liveness_stuck` once per episode and exposes the current
subject in the Owner inbox. Each command has at most one current command item.
The `swarm status` liveness summary includes the number of confirmed commands
without a dispatcher and the oldest command's ID and age, including snoozed
items (#929, #883).

For a command issued by an Agent, Swarm rechecks the stuck reason before
queuing a `#888` framed Swarm note to that issuer through addressed service
mail. The note identifies the command and reason and points to command
inspection and the Owner inbox. A retained delivery suppresses another note
for that command and reason. Existing mail presentation and wake rules apply.

`daemon.status` and `owner.inbox` project current Store facts on read. Parked is
separate from Moving and Long: only a current idle or waiting Agent with an
unexpired `agent_wakes` row bound to its exact generation and turn is Parked.
An idle Agent's bound turn must be completed; a waiting Agent's exact active
turn must still be running with its owned native source or process observed.
Seconds wakes are Parked only until their retained due time; other self-wakes
remain Parked until they settle or expire. A pending Owner request remains
Asked ahead of wake evidence. Unbound timers, stale or expired wakes, and a
waiting turn without an active wake keep their existing Moving/Long/Stuck
classification.

Stuck inbox rows keep `recommended` absent when Store facts do not prove a safe
correction; available inspection choices remain executable. A stuck managed
launch offers Stop when the exact generation is still Created and its workspace
is Creating with explicit proof that Git creation has yet to dispatch. Stop
uses the normal workspace-cancellation path and exact generation fence. The
same item offers command inspection. A Stuck undispatched `send` from
`swarm.handoff` offers an explicit `command.cancel` choice. The ordinary
cancel mutation rechecks its dispatch state and handoff binding.

For a Stuck busy or waiting Agent, the Owner-only `stop` recommendation is
present only when its current generation has no active-turn pointer, pending request, running or
unknown turn, or unsettled command, and Store has both an open native source and
a running owned plugin. The action carries that exact generation, and normal
command admission rechecks the inbox subject revision before admitting Stop;
Stop uses the existing native and process-custody path. Other direct Agent
Stuck cases offer `agent.get` only.

For a stuck active Goal (`active_goal_without_round_or_timer`, #871, #904),
`owner.inbox` offers a ready `goal.round` (`round`) recommendation alongside
`goal.get` (`inspect`) only when Store proves the current Goal is active and
the Agent is cleanly idle with an unconsumed idle key or due cadence slot.
Otherwise `recommended` remains absent and inspection stays available. Other
Stuck kinds expose only their existing readback choice when no safe corrective
action is available. The selected action is bound into the item's revision;
`owner.decide` reprojects that exact subject and revision before admitting it.
Snooze hides the current inbox revision until its subject changes. The sweep
prunes snoozes for subjects that no longer project.

Release qualification must include launching a real Agent in a newly managed
worktree and observing its first native turn. This checks the repository write
lane and dispatch path after the release starts (#929).

Shell matching counts one check per matching command segment and at most one wait
per captured shell-tool start. It uses the executable basename, ignoring case and
an optional `.exe` suffix. Assignments and leading `do`, `then`, `(`, or `{` are
skipped. Check patterns cover `cargo fmt`, `cargo build`, `cargo check`,
`cargo clippy`, and `cargo test` after Cargo global flags and an optional
`+toolchain`. The frozen project check matches the executable and its configured
argument prefix. Wait patterns cover `sleep`, `Start-Sleep`, and `timeout /t`.
The matcher splits at unquoted and unescaped line breaks, `;`, `&`, and `|`.
It unwraps command strings passed via `bash -lc`, `sh -c`, `powershell -Command`,
and `pwsh -c`. Counts are based on the captured invocation text. Conditional
segments can be counted even when the shell skips them.

Swarm checks count Goal exits and `project.check.finished` fast-check facts with
a proven direct exit. Before this release the fast-check worker did not retain
these facts. Swarm context injections use the timestamp of `command.applied`. Service mail
counts once per durable delivery. Normalized input sums uncached input and cache
reads. Average context uses one measured first-call input sample per retained
completed turn when available. Later calls lack a comparable retained sample.
An Agent is a manager for a turn if a retained child under its current parent
links was created at or before the turn's completion. The executor row counts
distinct Swarm-created Agents with qualifying turn starts or authenticated Swarm
MCP starts in the window. It uses the completion timestamp for a bound turn
and the start event timestamp for unfinished turns or unbound calls. A profile
can have zero calls. The Store keeps current parent links and profiles, so this
read cannot reconstruct historical reparenting.
If a current waiting item's transition event is gone, its last update provides
a lower bound on elapsed waiting time and the backlog row reports partial coverage.

Periodic retention can prune terminal commands. This can remove steers, frozen
launch checks, or principal attribution for applied injects. Those rows reflect
the retention risk because the Store has no command-prune timestamp watermark.
Unknown commands are protected by [retention](core.md#retention). Periodic
retention does not delete Agent or turn rows. Explicit Agent deletion removes
the Agent and its dependent history, so the policy, usage, and executor rows
describe the retained fleet. Event-dependent rows also report known event gaps.

## Heavy work, session growth and harness drift

Three event families capture heavy work, session growth, and vendor drift.

- `activity.heavy`: one event per command whose wall time, CPU or peak RSS
  crossed a `daemon.heavy_*` threshold. Attribution walks the observed tree to
  the process that consumed the resources: console hosts (`conhost`) and the
  harness shell wrapper (`powershell -NoProfile -Command ...`) never categorize,
  and the compiler/linker under its driver reads as the driver (`cargo build`,
  never `rustc` alone). Overlapping episodes from one command merge into one,
  and a closing tail waits while the same category still runs. The commitment
  keeps the program
  basename plus one safe subcommand token (for example `cargo build`), never full
  argv, paths or environment. Levels stay `debug`/`standard`; the pending log-level
  integration owns any promotion.
- `session.growth`: one warning per basis (context occupancy at
  `daemon.session_growth_ratio_permille`, native session file size at
  `daemon.session_file_bytes`, or a configured profile's absolute
  `context_ceiling_tokens`). A context ceiling uses the latest actual
  `context_used_tokens` count and does not require a known window; ratio warnings
  still require a valid used/window pair. The warning clears when the retained
  signal drops back under threshold. An absent or invalid context count cannot
  trigger a context reaction. Suppliers are harness-native only: Codex
  `thread/tokenUsage/updated` (latest context occupancy against `modelContextWindow`
  for the same turn or model), Claude stream-json plus the reported model window
  and transcript file size, Muse `session/contextUsage`, Antigravity ACP
  `usage_update`, and OpenCode's latest assistant-message input plus complete
  cache read/write counters from terminal readback. OpenCode has no capacity or
  session-file-size source, and the Muse event log exposes no size; those signals
  remain unavailable rather than guessed.
- `harness.drift`: one event per harness and installed surface while the installed
   version or contract marker differs from the diagnostic `VERIFIED_HARNESSES`
   baseline (`crates/eliot-core/src/observe.rs`). Versions compare normalized per
   harness (product prefix stripped, build metadata kept), so `codex-cli 0.167.1`
   matches `0.167.1` while `1.4.20` differs from `1.4.0-R4161.1`. This baseline
   does not pin the executable selected for a launch. Drift never blocks launch; it
   annotates `swarm doctor` and the launch flag only.

Readbacks: `observe.heavy` (windowed rollup, also `swarm heavy`), `harness.doctor`
(per-harness installed vs verified, also `swarm doctor`), `heavy`/`session_growth` on
`agent.stats`, the `OBS` column of `swarm top`, and the Heavy work section of the
desktop Details panel (shown only when an episode or growth signal was recorded).
Readbacks degrade to `None`/quiet text rather than failing the view.
