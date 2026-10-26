# Monitoring

Sources are independent and bounded. Failure reports health/gaps and never invents a
fact owned by another source.

## Source ownership

For each `(agent,generation,event family)` exactly one source is authoritative. The
owner and last committed native cursor/watermark are stored in the current source
projection. A backup may track raw position privately but does not publish duplicate
lifecycle.

Handoff begins strictly after the last committed cursor. An unprovable interval is a
visible gap. Wrong-generation, wrong-turn/request and duplicate native events are
bounded diagnostics, never normal lifecycle or automation triggers.

## Process identity and custody projections

Process identity is `(host_boot_id,pid,start_time)`, never PID alone. Linux uses kernel
boot id plus proc start ticks. Windows uses stable machine identity plus native boot
session/time identity plus `GetProcessTimes` creation time. Reboot cannot make an old
row match a new process.

A custody owner is one stable string:

```text
agent:<agent-id>#<generation>
service:<service-name>
action:<run-id>/<action-index>
workspace:<agent-id>/<step-index>
```

A process belongs to exactly one owner. Shared-service membership of agents is stored
as session metadata, not duplicate process ownership.

```sql
CREATE TABLE custodies (
  owner TEXT PRIMARY KEY,
  backend TEXT NOT NULL,              -- windows_job|linux_cgroup|process_group|external
  capability TEXT NOT NULL,           -- exact|degraded|external
  native_id TEXT,
  guardian_boot TEXT,
  guardian_pid INTEGER,
  guardian_start INTEGER,
  state TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE processes (
  host_boot_id TEXT NOT NULL,
  pid INTEGER NOT NULL,
  start_time INTEGER NOT NULL,
  owner TEXT NOT NULL REFERENCES custodies(owner),
  role TEXT NOT NULL,
  state TEXT NOT NULL,
  exit_code INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(host_boot_id,pid,start_time)
);
CREATE INDEX processes_owner ON processes(owner,state);
```

`process.put` from a plugin is converted to the current agent owner after the host
verifies boot/start identity. Internal Git/script/check/workspace runners register
their action owner directly. Restart never reconstructs custody from PID guesses.

### Peer caller classification

The persisted `processes` table is an optimization and audit projection, **not proof
that an unlisted peer is uncontrolled**. A child may connect before its process fact is
registered. Before any no-token owner fallback, the API server:

1. obtains the peer PID from the named pipe or Unix socket and pins its exact current
   boot/start identity;
2. looks for that exact identity in `processes`;
3. if no row exists, performs a live OS membership check against every current Eliot
   custody owned by the daemon:
   - Windows: open/pin the peer process and call `IsProcessInJob` for live Job handles;
   - Linux exact: read the pinned peer's cgroup membership and compare with registered
     Eliot cgroups;
   - degraded process-group mode: compare the pinned peer's PGID with registered live
     groups, without upgrading the backend's containment guarantee;
4. rechecks peer birth/liveness after classification;
5. grants no-token owner identity only when the peer is the local OS user and is
   **definitively not a member** of any current Eliot custody.

Membership maps the call to that custody principal and requires its matching scoped
token. Any access failure, PID/birth race, incomplete backend inventory or otherwise
ambiguous membership rejects the call as `caller_identity_unknown`; absence from the
projection never becomes owner authority. Containment is applied before child execution,
so registration lag cannot create an authority window.

A shared-service process still needs an exact per-session token because service custody
is not an agent identity. An arbitrary same-user process outside Eliot custody remains
inside the declared one-user trust boundary; this mechanism prevents accidental
principal escalation by Eliot-managed descendants.

## Process custody backends

### Windows exact

Per-agent plugin and descendants enter a no-breakaway Job before execution resumes.
Every script/Git/check/workspace action gets its own Job as well. Daemon-owned stdio
Jobs use kill-on-close; managed services use separate named non-killing Jobs. Force stop
verifies boot-scoped identities and zero active members.

### Linux exact and degraded

Exact custody requires delegated cgroup v2 (or a qualified equivalent) plus an internal
owner-death guardian. The guardian is outside the owned group, monitors daemon
boot/process identity and kills/verifies the group on abnormal daemon loss. Clean
shutdown disarms it only after the group is empty.

Guardian boot/PID/start and health are monitored. A missing or mismatched guardian
blocks new exact launches and visibly downgrades existing groups; the daemon never
silently keeps claiming owner-death cleanup.

Process-group fallback may observe and gracefully stop non-escaping members. A
same-user child can call `setsid`, so capability is degraded: exact force and
kill-on-owner-death are unsupported. Other Unix platforms remain degraded until an
exact backend is qualified.

### Shared services

A shared plugin/service is watched once. Agent stop uses exact native session control,
never server kill. An attached owner service is external and never stopped. Managed
service custody/process projection is separate from agent/action owners.

## Process observation and terminal settlement

- Plugins register PIDs through commit-acknowledged process facts; the host verifies and
  tags current boot id and exact start identity before commit.
- Exact custody enumerates the complete container; prior-boot rows are historical.
- Stats are ephemeral and never model progress.
- Unknown exit code stays unknown.
- Process exit and final native output may race. Drain the authoritative source to EOF
  or a bounded terminal deadline.
- Timeout is not terminal silence evidence. Mark a gap and emit lost only when exact
  custody is empty or exact shared-session readback says absent.
- A live shared server says nothing about one session.
- Late terminal data after source closure is diagnostic only.

## Starting processes

- Resolve the executable fully; argv/cwd/env are separate and bounded.
- Apply containment before execution.
- Only intended child handles are inheritable.
- Long input uses stdin/files; Eliot text is UTF-8 without BOM and LF.
- Git prompts/editor are disabled.
- Environment follows minimal/inherit policy; Windows key merge is case-insensitive.
- Running script/hook/plugin paths are immutable/versioned.
- Normalize paths; never follow an untrusted symlink/reparse point for cleanup,
  fingerprint or setup.

## Workspace preparation

`launch --worktree` is a durable pre-start phase:

1. create Agent generation 0 with `workspace_state=preparing` and deterministic path;
2. journal repository, branch, base and path;
3. run explicit `git worktree add` under `workspace:<agent>/git` custody;
4. read back worktree list, branch and HEAD;
5. run setup argv sequentially, each in its own workspace-action custody;
6. commit ready; only then allow native start.

A lost Git response is read back, never blindly repeated. Lost setup has no generic
readback: workspace becomes unknown, launch remains blocked, and the owner resolves or
removes/recreates explicitly. Setup is not assumed idempotent.

Cancel/stop during preparation terminates the current exact action. The Git step is then
read back; interrupted setup leaves workspace unknown. Native generation is never
started from a partially prepared workspace.

Collision or mismatch fails. Cleanup is explicit, stopped-agent only, exact path under
data root and Git-clean except allowlisted relative scratch. Scratch cleanup rejects
symlink, device and alternate-stream escape. The branch is retained.

## Git and filesystem monitor

One monitor exists per distinct live worktree:

- read-only `git --no-optional-locks status --porcelain=v2 --branch -z`;
- bounded log on HEAD change;
- base-vs-HEAD diff for still-unmerged paths;
- watcher debounce 500 ms, minimum refresh 2 s;
- slow status backoff plus health;
- remote refs only after explicit fetch or optional `ls-remote`.

The monitor never writes index, refs or objects.

### Durable edit attribution

```sql
CREATE TABLE agent_edits (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  workspace TEXT NOT NULL,
  path TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,path)
);
CREATE INDEX agent_edits_workspace ON agent_edits(workspace,path);
```

Generation-matched edit events upsert with the event. When Git proves a path clean,
remove rows. A terminal/superseded generation remains until its paths are clean;
restart never rebuilds attribution from event history. An unowned shared-checkout path
is `unattributed`.

### Read-only progress fingerprint

```text
HEAD + sorted(dirty/untracked path, file type, size, content hash)
```

Hash only dirty/untracked paths excluding ignored/scratch; hash symlink target text
without following. Cache until watcher/metadata change and enforce caps. Incomplete is
unavailable, never no-progress. No temporary index, `git add`, object writes or
`write-tree`.

## Liveness

Only exact-generation model progress updates `last_progress_at`.

| Observation | Conclusion |
| --- | --- |
| no progress while busy/waiting | stalled flag only |
| parent idle, child busy | parent idle; tree incomplete |
| process alive, stream disconnected | source stale; state unchanged |
| exact boot-scoped custody empty + source drained/gapped | lost for generation |
| shared server live, session readback unknown | stale/unknown, not lost |
| handoff gap | explicit gap; no inferred event |
| old boot/generation/duplicate | historical diagnostic only |

## Restart reconciliation

1. compute current boot id; prior-boot live rows cannot match;
2. reopen exact named custody/service/guardian identities for the current boot;
3. reattach shared/service sessions and snapshot commands, native inputs and turns;
4. settle missing owned groups only after source-drain rules;
5. reconcile preparing/unknown workspaces;
6. rebuild Git facts and load edit projection;
7. keep dispatched command/action/workspace effects unknown until readback.

## Views

Expose boot-scoped process identity, custody owner, generation/turn/input gate/open
stop/workspace state, exact/degraded custody/guardian, source gaps, model progress,
usage, branch/edit, provider holds, caller-classification failures and Store/WAL health.
