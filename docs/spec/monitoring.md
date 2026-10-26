# Monitoring

Sources are independent and bounded. Failure reports health/gaps and never invents a
fact owned by another source.

## Source ownership

For each `(agent,generation,event family)` exactly one source is authoritative. Owner
and last committed native cursor/watermark are stored in current source projection. A
backup may track raw position privately but does not publish duplicate lifecycle.

Handoff begins strictly after last committed cursor. Unprovable interval is a visible
gap. Wrong-generation, wrong-turn/request and duplicate native events are diagnostics,
never normal lifecycle/automation triggers.

## Process identity and custody projections

Process identity is `(host_boot_id,pid,start_time)`, never PID alone. Linux uses kernel
boot id + proc start ticks. Windows uses stable machine identity + native boot session/
time identity + `GetProcessTimes` creation time. Reboot cannot make old row match new
process.

`daemon.json`, process events, service state and stop/readback carry `host_boot_id`.

```sql
CREATE TABLE custodies (
  agent INTEGER,
  generation INTEGER,
  service TEXT,
  backend TEXT NOT NULL,              -- windows_job|linux_cgroup|process_group|external
  capability TEXT NOT NULL,           -- exact|degraded|external
  native_id TEXT,
  guardian_boot TEXT,
  guardian_pid INTEGER,
  guardian_start INTEGER,
  state TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK((service IS NULL AND agent IS NOT NULL AND generation IS NOT NULL) OR
        (service IS NOT NULL AND agent IS NULL AND generation IS NULL))
);
CREATE UNIQUE INDEX custody_agent ON custodies(agent,generation)
  WHERE agent IS NOT NULL;
CREATE UNIQUE INDEX custody_service ON custodies(service)
  WHERE service IS NOT NULL;

CREATE TABLE processes (
  agent INTEGER,
  generation INTEGER,
  service TEXT,
  host_boot_id TEXT NOT NULL,
  pid INTEGER NOT NULL,
  start_time INTEGER NOT NULL,
  role TEXT NOT NULL,
  state TEXT NOT NULL,
  exit_code INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(host_boot_id,pid,start_time),
  CHECK((service IS NULL AND agent IS NOT NULL AND generation IS NOT NULL) OR
        (service IS NOT NULL AND agent IS NULL AND generation IS NULL))
);
CREATE INDEX processes_agent ON processes(agent,generation,state);
CREATE INDEX processes_service ON processes(service,state);
```

A process belongs to one agent generation or one service, never both. Shared service
membership is represented by agent↔service session metadata, not duplicate process rows.
`process.put` updates projection with process event. Restart never reconstructs custody
from PID guesses.

## Process custody backends

### Windows exact

Per-agent plugin/descendants enter a no-breakaway Job before execution resumes.
Daemon-owned stdio Jobs use kill-on-close; managed services use separate named
non-killing Jobs. Force stop verifies boot-scoped identities and zero active members.

### Linux exact/degraded

Exact custody requires delegated cgroup v2 (or qualified equivalent) plus internal
owner-death guardian. Guardian is outside owned group, monitors daemon boot/process
identity, and kills/verifies cgroup on abnormal daemon loss. Clean shutdown disarms
only after group empty.

Guardian boot/PID/start and health are monitored. Missing/mismatched guardian blocks new
exact launches and visibly downgrades existing groups; daemon never silently keeps
claiming owner-death cleanup.

Process-group fallback may observe/gracefully stop non-escaping members. Same-user child
can `setsid`, so capability is degraded: exact force/kill-on-owner-loss unsupported.
Other Unix remains degraded until exact backend qualified.

### Shared services

Shared plugin/service is watched once. Agent stop uses exact native session control,
never server kill. Attached owner service is external/never stopped. Managed service
custody/process projection is separate from agents.

## Process observation and terminal settlement

- Plugins register PIDs through commit-acknowledged process facts; host verifies/tags
  current boot id and exact start identity before commit.
- Exact custody enumerates complete container; prior-boot rows are historical.
- Stats are ephemeral and never model progress.
- Unknown exit code stays unknown.
- Process exit/final native output may race. Drain authoritative source to EOF or
  bounded terminal deadline.
- Timeout is not terminal silence evidence. Mark gap and emit lost only when exact
  custody empty or exact shared-session readback absent.
- Live shared server says nothing about one session.
- Late terminal data after source closure is diagnostic only.

## Starting processes

- Resolve executable fully; argv/cwd/env separate/bounded.
- Apply containment before execution.
- Only intended child handles inheritable.
- Long input via stdin/files; Eliot text UTF-8 no BOM/LF.
- Git prompts/editor disabled.
- Environment follows minimal/inherit policy; Windows key merge case-insensitive.
- Running script/hook/plugin paths immutable/versioned.
- Normalize paths; never follow untrusted symlink/reparse for cleanup/fingerprint/setup.

## Workspace preparation

`launch --worktree` is durable pre-start:

1. create Agent generation 0, `workspace_state=preparing`, deterministic path;
2. journal repo/branch/base/path;
3. explicit `git worktree add`;
4. read back worktree list, branch, HEAD;
5. setup argv sequentially under custody;
6. commit ready, then allow native start.

Lost Git response is read back, never blindly repeated. Lost setup has no generic
readback: workspace unknown, launch blocked, owner resolves/removes/recreates. Setup is
not assumed idempotent.

Collision/mismatch fails. Cleanup explicit, stopped-agent only, exact path under data
root, Git-clean except allowlisted relative scratch; reject symlink/device/alternate-
stream escape. Branch retained.

## Git/filesystem monitor

One monitor per distinct live worktree:

- read-only `git --no-optional-locks status --porcelain=v2 --branch -z`;
- bounded log on HEAD change;
- base-vs-HEAD diff for still-unmerged paths;
- watcher debounce 500 ms, minimum refresh 2 s;
- slow status backoff + health;
- remote refs only after explicit fetch or optional `ls-remote`.

Never write index, refs or objects.

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

Generation-matched edit events upsert with event. When Git proves path clean, remove
rows. Terminal/superseded generation stays until paths clean; restart never rebuilds
from event history. Unowned shared-checkout path is `unattributed`.

### Read-only progress fingerprint

```text
HEAD + sorted(dirty/untracked path, file type, size, content hash)
```

Hash only dirty/untracked excluding ignored/scratch; hash symlink target text without
following. Cache until watcher/metadata change, enforce caps. Incomplete is unavailable,
never no-progress. No temporary index, `git add`, object writes or `write-tree`.

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
2. reopen exact named custody/service/guardian identities for current boot;
3. reattach shared/service sessions and snapshot commands/turns/inputs;
4. settle missing owned groups only after source-drain rules;
5. reconcile preparing/unknown workspaces;
6. rebuild Git facts/load edit projection;
7. keep dispatched effects unknown until readback.

## Views

Expose boot-scoped process identity, generation/turn/input gate/open stop/workspace,
exact/degraded custody/guardian, source gaps, model progress, usage, branch/edit,
provider holds and Store/WAL.
