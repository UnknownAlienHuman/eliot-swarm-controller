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
- services that must survive daemon restart use separate service custody;
- force-stop waits for the Job to become empty and verifies recorded process identities.

### Unix degraded

A process group contains ordinary descendants but a same-user child can `setsid`, and
the group does not automatically die with the daemon. Until a qualified cgroup/equivalent
is implemented, capability is `degraded`:

- graceful stop and best-effort kill of known group members;
- no claim that every descendant died;
- `stop --force` reports possible orphans rather than clean exact success;
- `eliot doctor` surfaces them.

### Adopted/shared services

An owner-run service discovered from its registration is read-only/adopted and is never
signalled or repaired by Eliot. Managed service custody is separate and stopped only by
its service lifecycle. Stopping one Agent never signals the shared/adopted server.

`stop` requires an isolated native operation that ends that Agent's session and exact
readback of its terminal state. If the service cannot provide it, return `unsupported`;
abort-plus-detach or dropping a subscription is not successful stop. `detach` is the
separate explicit operation that releases Eliot control while leaving the session alive.
Observe-only attachment cannot issue either native stop or other control operations.

## Process observation and terminal ordering

The OS monitor is authoritative for process start/exit/stats. A plugin may register a
PID only if host verification places it inside that Agent custody. Enumeration and
plugin registration upsert one identity and one start event.

Process exit does **not** immediately imply session/turn loss. It races buffered stdout
and final native events:

1. mark the process exited and stop accepting new native work for that generation;
2. drain the authoritative source to EOF or one bounded terminal deadline;
3. commit any terminal native fact first;
4. if EOF/deadline provides no terminal fact, commit `source.gap` for the uncovered
   tail;
5. only then emit `session.lost` when exact/shared readback proves control is gone.

Closing or losing the Codex observer's held rollout reader is not native thread-terminal
evidence and does not emit `session.ended` or `session.lost`. Controlled owned-process
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

`launch --worktree BRANCH [--base REF]` is pre-start work over one durable row:

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
reset or renamed. Omitted `--base` means local `HEAD`. The destination is an opaque
directory below `<data_dir>/worktrees`, independent of caller command-id syntax.
Repository common-directory identity is frozen alongside the source checkout.

Flow:

1. create/retain the Agent projection and workspace row for `target_generation` with
   phase `creating`;
2. run `git worktree add` against frozen OID;
3. read back exact path, branch and HEAD; then set `phase=setup_ready`,
   `setup_index=0`;
4. before setup entry `i` starts, commit `phase=setup_running`, `setup_index=i` and its
   frozen argv/file identity; only then spawn it in
   `workspace:<agent>#<target_generation>` custody;
5. exact success commits `setup_ready,i+1`; after the last entry, commit `ready`;
6. only a `ready` row whose target generation matches the start command permits that
   generation's native dispatch.

A lost Git response is settled by exact worktree/path/branch/OID readback. A setup entry
has no generic readback: after restart or lost process result, `setup_running` becomes
`unknown` unless exact platform evidence proves the process never started. It is never
rerun. The owner removes/recreates or explicitly resolves the workspace. This needs one
row and one running marker, not a `workspace_steps` journal or artifact service.

Cancellation stops the current custody. Native start never begins from a non-ready or
wrong-target-generation workspace. Removal is explicit, terminal-Agent only and
clean-only except allowed relative scratch paths; the branch is retained.
Cancellation is committed before signalling custody and serializes with process spawn.
Preparation failure before first native dispatch can fail the generation-0 Agent;
uncertainty leaves it created with an unknown start command. Neither path installs
native authority or advances the generation. A resume preparation failure retains the
previous terminal generation.
On Windows, step completion waits for the owned Job to empty. On supported Linux,
successful leader completion and an empty observed process group permit degraded
completion; escaped descendants remain possible. A signal-zero group query is
observation only and never grants permission to signal a stale numeric PGID.
`daemon.status.workspace_custody` describes the current host's capability, not a
retrospective assertion about historical commands. Unknown prior Unix custody after
daemon replacement cannot be promoted to an exact stopped result.

`worktree.remove` uses normal command admission. No other live Agent may use that
checkout; the `removing` marker blocks concurrent launch into it. Cleanup may remove
only untracked/ignored entries under frozen scratch paths, never tracked changes or
escaping symlinks/reparse points. Ordinary Git removal performs its final clean check;
there is no force removal. Exact path/registration absence and retained branch settle
`removed`. Keep the row so a later resume cannot treat the deleted checkout as an
ordinary shared workspace. Unknown removal permits readback, never blind repetition.
Before filesystem changes, wait for the checkout's terminal Git sources to drain and
close, including subdirectory borrowers. This orders final hint attribution before
removal cleanup and prevents a stale observation from recreating removed provenance.

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

Diff/log commands additionally pass `--no-ext-diff --no-textconv`. The daemon sets
`GIT_TERMINAL_PROMPT=0`, disables editors and aliases, and never invokes repository
hooks, fsmonitor helpers, external diff drivers or textconv filters. It never permits an
unapproved/custom remote-helper scheme: any optional network read uses an
owner-configured remote whose complete effective URL set passed the validation in
[automation](automation.md#actions-are-commands). Approved built-in Git transports may
use Git's normal transport helper internally; “disabled remote helpers” never means
silently disabling the configured HTTPS/SSH transport itself.

- refresh on debounced filesystem invalidation and poll interval, at most once per 2 s;
- `status --porcelain=v2 --branch -z` for current state;
- bounded `log --no-ext-diff --no-textconv` on HEAD movement from the last stored HEAD,
  so commits made while daemon was down are recovered; no prior baseline yields an
  explicit gap;
- committed-but-unmerged paths are computed with
  `diff --no-ext-diff --no-textconv` against frozen `base_oid` when available;
- remote refs change only after explicit safe fetch or optional validated read-only
  `ls-remote` poll;
- slow status backs off and reports source health;
- monitor never writes index, refs or objects.

The current subset polls local state at least 2 s apart without submodule recursion;
slow/error probes back off up to 30 s. CLI reads return the cache without forcing a scan.
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
still have `complete=false`. Executable Git filters are disabled, and applicable filter
attributes make raw status partial; bounded attribution-path coverage can also make a
snapshot partial. An empty partial dirty set does not prove the workspace clean.

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
supply edit hints. Codex retains bounded path metadata, not patch contents; the daemon
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
It remained queryable after terminal settlement, then a live fake's complete clean scan
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
keep the serialized field catalog in Core. Derive scope only from the frozen profile snapshot: selected
proxy name first, otherwise configured account name, otherwise profile name. Never infer
a vendor account from credentials, endpoint responses or model metadata.

Create or update a hold only from a structured restriction attached to the exact
generation- and turn-bound controlled `turn.completed`. Generic failure, native error
text, HTTP status, or a vendor-owned model-catalog response is not a restriction and
does not create a hold.

The pinned Codex 0.161 projection recognizes three exact structured codes:
`rateLimitExceeded` maps to rate-limit, `usageLimitExceeded` to quota, and
`unauthorized` to auth. Four typed `codexErrorInfo` variants map to rate-limit only
when their typed `httpStatusCode` is 429: `httpConnectionFailed`,
`responseStreamConnectionFailed`, `responseStreamDisconnected`, and
`responseTooManyFailedAttempts`. A bare HTTP status is not enough; only these exact
typed variants with `httpStatusCode:429` map to rate-limit. `ModelRemoved` remains a
valid structured Core restriction kind exercised by the Fake policy fixture, but the
Codex 0.161 classifier does not infer it from an unrecognized native variant, HTTP 404
or message text.

A hold is `Held` or `Probing`. It blocks new provider-backed launch/send work in that
scope, including queued dispatch. A launch with linked input must reject before either
effect; resume without new text, readback, request replies, interrupt and stop remain
available. Resume with text is a linked send and remains gated.

Rate-limit and quota holds permit at most one explicit eligible SEND probe claim per
frozen provider scope in Store, acquired atomically by `probe=true` through the normal
SEND path. The probe uses ordinary SEND authorization and target/scope/generation fences;
it adds no owner-only modifier or parallel executor. `retry_after` controls probe
eligibility only; its expiry never clears a hold. Only a successful exact-bound probe
completion clears a rate-limit/quota hold. A known failed or interrupted probe leaves
the hold and releases only its definitive probe claim. An unknown probe retains both
hold and claim across restart; it is not retried automatically.

Auth and ModelRemoved holds are not probe-eligible and do not expire automatically. An
owner clears them with one of the closed corrective evidence values:
`credentials_updated`, `configuration_updated` or `owner_verified`.
Clear uses a same-scope compare-and-swap against the expected probe command, including
explicit null; it cannot clear an unresolved probe. The mutation ID binds the clear
request, so an exact retry returns the retained result and changed evidence conflicts.

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

## Views

`watch`, `top`, `show`, `daemon.status` query projections/events. They expose generation,
active/unknown turn/input/transition gate, source owner/cursor/gap, custody
capability/orphans, provider hold, workspace phase, requested/effective route and
Git/edit facts. A stale or unknown value is shown explicitly, never hidden or converted
to zero.
