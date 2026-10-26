# Monitoring

Sources are independent and bounded. A failed source reports health/gaps and never
invents facts owned by another source.

## Source ownership

For each `(agent,generation,event family)` exactly one source is authoritative. The
choice and native cursor/watermark are stored with the current agent source projection.
A backup may track raw position privately but does not publish duplicate lifecycle.

Handoff begins strictly after the last committed native event id/cursor. An unprovable
interval is a visible gap. Wrong-generation, wrong-turn/request and duplicate native
events are diagnostics only, not normal lifecycle events or automation triggers.

## Process custody backends

Process identity is native `(pid,start_time)` at full available resolution, never PID
alone.

### Windows

Per-agent plugin and descendants enter a Job object before execution resumes. Breakaway
is disabled. Daemon-owned stdio Jobs use kill-on-close; managed services use separate
non-killing Jobs. Force stop verifies Job membership and empty active-process count.

### Linux

Exact custody requires delegated cgroup v2 (or another explicitly qualified equivalent)
and an internal guardian process. The guardian is outside the owned group, monitors a
daemon-death pipe/parent identity, and kills/verifies the cgroup on abnormal daemon
loss. Clean shutdown disarms it only after the cgroup is empty.

A process-group fallback may be used for observation/graceful stop, with a guardian
that kills non-escaping members. Because a same-user child can call `setsid`, fallback
status is `degraded`; exact `stop --force` and kill-on-owner-loss are rejected as
unsupported rather than falsely claimed.

Other Unix platforms remain degraded until an equivalent exact backend is qualified.

### Shared services

A shared plugin/service is watched once. Agent stop uses exact native session control;
it never kills the server. Attached owner-run services are never stopped. Managed
service custody is separate from all agent generations.

## Process observation and terminal settlement

- Plugins register known PIDs; exact custody enumerates the complete container.
- Stats are ephemeral and never model progress.
- Exit code may be unknown; never substitute zero.
- A disappearing process during identity read is gone, not a monitor error.
- Process exit and final native output can race. When owned harness processes end, the
  daemon drains the authoritative plugin stream to EOF or a bounded
  `terminal_drain_timeout` (default 3 s). A matching terminal event wins.
- Drain timeout is **not** evidence by silence. After timeout, close/mark the source
  gapped and emit `session.lost` only when exact custody is empty or exact shared
  session readback says absent. A live shared server process alone says nothing about
  one session.
- A late terminal frame after source closure is diagnostic history and cannot change a
  newer/current state.

## Starting processes

- Resolve executable to a full path and pass argv separately; no implicit shell.
- Apply containment before execution (Windows suspended→Job→resume; Linux exact
  backend before exec).
- Only intended child handles are inheritable.
- Long input uses stdin/files. Eliot text is UTF-8 without BOM and LF.
- Git disables prompts/editor.
- Environment merging is case-insensitive on Windows and starts from the configured
  minimal/inherit policy.
- Never overwrite a running script/hook/plugin; install versioned files.
- Normalize paths consistently and never follow an untrusted symlink for cleanup or
  workspace fingerprinting.

## Git/filesystem monitor

One monitor per distinct live worktree:

- `git --no-optional-locks status --porcelain=v2 --branch -z` for current state;
- bounded log on HEAD change for commits;
- base-vs-HEAD diff for still-unmerged branch paths;
- filesystem watcher with 500 ms debounce and minimum 2 s refresh interval;
- slow status backs off to five minutes and reports health;
- remote refs change only after explicit fetch or optional read-only `ls-remote` poll.

The monitor never writes index, refs or objects.

### Durable edit attribution

Persisted `file.changed` history is not a safe long-term projection: retention can
remove it while an agent remains live. L3 therefore adds:

```sql
CREATE TABLE agent_edits (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  workspace TEXT NOT NULL,
  path TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent, generation, path)
);
CREATE INDEX agent_edits_workspace ON agent_edits(workspace, path);
```

Generation-matched native edit events upsert this table in the same transaction as the
event. When Git proves a path clean, remove its rows for that worktree. Terminal or
superseded generations are removed after their paths are clean; no event replay scan is
needed on restart. In shared checkouts, paths with no surviving attribution remain
`unattributed`.

### Read-only progress fingerprint

For a Git workspace:

```text
HEAD + sorted(dirty/untracked path, file type, size, content hash)
```

Hash only already-dirty/untracked paths; exclude ignored/profile scratch; hash symlink
target text without following it. Cache until watcher/metadata change. Enforce file/
byte caps. Incomplete fingerprint is `unavailable` and never counts as no progress.
No temporary index, `git add`, `hash-object -w` or `write-tree`.

## Liveness

Only generation-matched model progress updates `last_progress_at`: turn/message/tool/
plan/file/request/answer and a native heartbeat that proves loop advancement.

| Observation | Conclusion |
| --- | --- |
| no progress while busy/waiting past threshold | stalled flag only |
| parent idle, child busy | parent idle; tree incomplete |
| process alive, stream disconnected | source stale; state unchanged |
| exact custody empty + terminal source drained/gapped | lost for that generation |
| shared service live, session readback unknown | session unknown/stale, not lost |
| source handoff gap | explicit gap; no inferred event |
| old generation/native duplicate | diagnostic only |

## Restart reconciliation

On daemon start, current rows—not remembered wakeups—drive recovery:

1. re-open exact custody/service identities;
2. re-attach live shared/service sessions and snapshot open commands;
3. mark missing exact owned groups lost only after source drain rules;
4. rebuild Git state from Git and edit attribution from `agent_edits`;
5. leave every previously dispatched command/action unknown until exact readback.

## Views

`watch`, `top`, `show` and `daemon.status` expose generation, active turn, open stop,
exact/degraded custody, source owner/gaps, last model progress, processes, usage, branch,
dirty/edit attribution, account/provider holds and Store/WAL health. Views query current
projections and streams; they are not another state machine.
