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

| Family | Controlled Agent | Observe/recovery Agent |
| --- | --- | --- |
| session/turn/message/tool/request/child/goal/usage | native stream | transcript or native paged history |
| synchronous hook decision | hook only | hook only |
| process | OS monitor | OS monitor |
| Git/filesystem | Git/fs monitor | Git/fs monitor |

A hook may own a fact absent from the primary stream; it does not mirror lifecycle the
stream already owns. Transcript/history may privately track its cursor while the stream
owns lifecycle, but it publishes only after an explicit handoff.

Initial ownership is committed before native I/O. Dynamic handoff uses the plugin
protocol's single compare-and-swap request: verify predecessor, record final cursor or
`source.gap`, install successor. The successor publishes only after commit. There are no
source epochs or independent lifecycle projections.

Health states are `ok|slow|stale|error`. Event-stream silence is not stale by itself; a
lost transport is stale until reconnect. Stale authoritative lifecycle means current
turn progress is unknown, never idle/done.

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

A timer such as “three seconds without `session.ended`” is never terminal evidence by
itself. On degraded Unix, lost session control may coexist with `orphan_possible`.

Process stats are ephemeral and do not count as model progress. Unknown exit code stays
unknown, never zero.

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
  target_generation INTEGER NOT NULL,
  project TEXT NOT NULL,
  repo_path TEXT NOT NULL,
  requested_branch TEXT NOT NULL,
  base_ref TEXT,
  base_oid TEXT NOT NULL,
  worktree_path TEXT NOT NULL UNIQUE,
  frozen_setup TEXT NOT NULL,
  phase TEXT NOT NULL,       -- creating|setup_ready|setup_running|ready|unknown|failed
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

## Git and filesystem monitor

One monitor per distinct live worktree. Monitoring commands use a hermetic read-only Git
invocation:

```text
git -c core.fsmonitor=false --no-optional-locks <command> ...
```

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

Current per-worktree Git sets live in memory and rebuild after restart. Per-Agent edit
attribution is durable because Git cannot reconstruct authorship in a shared checkout:

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

Generation-matched `file.changed` upserts it; when Git proves a path clean, rows are
removed. Old generations are cleanup provenance, not current workers.

### Read-only progress fingerprint

Goal progress uses:

```text
HEAD + sorted(dirty/untracked path, type, size, content hash)
```

Only already-dirty/untracked, nonignored, nonscratch paths are read. Symlink target text
is hashed without following it. File/byte caps return `unavailable`; incomplete data
never counts as no progress. No temporary index, `git add`, `write-tree` or object write.

## Provider holds

Provider limitation state is durable before queues restart:

```sql
CREATE TABLE provider_holds (
  scope TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  retry_after INTEGER,
  probe_command TEXT,
  state TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
```

A hold blocks new provider-backed launch/send/Goal for that scope, not local readback,
request replies, interrupt or stop. One explicit eligible command may claim a probe.
Unknown probe remains reserved across restart; time alone does not clear it. Auth and
model removal require explicit owner/configuration evidence.

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
| provider 429 | rate limit unless native evidence proves quota |
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
