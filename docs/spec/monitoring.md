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

## Process custody backends

Process identity is native `(pid,start_time)` at full available resolution, never PID
alone.

### Windows exact

Per-agent plugin and descendants enter a no-breakaway Job before execution resumes.
Daemon-owned stdio Jobs use kill-on-close; managed services use separate non-killing
Jobs. Force stop verifies Job membership and zero active-process count.

### Linux exact/degraded

Exact custody requires delegated cgroup v2 (or another qualified equivalent) plus an
internal owner-death guardian. The guardian is outside the owned group, monitors daemon
identity/death, and kills/verifies the cgroup on abnormal daemon loss. Clean shutdown
disarms it only after the group is empty.

Guardian PID/start identity and health are monitored. A missing guardian blocks new
exact-custody launches and visibly downgrades existing groups until repaired. The daemon
never silently continues claiming owner-death cleanup.

Process-group fallback may support observation/graceful stop with a guardian for
non-escaping members. Same-user children can `setsid`, so fallback is `degraded`:
exact force and kill-on-owner-loss are unsupported. Other Unix remains degraded until
an equivalent exact backend is qualified.

### Shared services

A shared plugin/service is watched once. Agent stop uses exact native session control,
never server kill. Attached owner services are never stopped. Managed service custody
is separate from agent generations.

## Process observation and terminal settlement

- Plugins register known PIDs through commit-acknowledged process facts; exact custody
  enumerates the whole container.
- Stats are ephemeral and never model progress.
- Unknown exit code stays unknown.
- Process exit and final native output may race. When owned harness processes end,
  drain the authoritative source to EOF or bounded `terminal_drain_timeout`.
- Timeout is not silence-based terminal evidence. Mark a source gap and emit lost only
  when exact custody is empty or exact shared-session readback says absent.
- A live shared server says nothing about one session.
- Late terminal data after source closure is diagnostic history and cannot mutate a
  newer/current generation.

## Starting processes

- Resolve executable fully; argv/cwd/env are separate and bounded.
- Apply containment before execution.
- Only intended child handles are inheritable.
- Long input uses stdin/files; Eliot text is UTF-8 no BOM/LF.
- Git disables prompts/editor.
- Environment follows minimal/inherit policy; Windows key merge is case-insensitive.
- Running script/hook/plugin files are immutable/versioned.
- Normalize paths and never follow an untrusted symlink/reparse point for cleanup,
  fingerprint or setup.

## Workspace preparation

`launch --worktree` is a durable pre-start phase, not ad-hoc preflight:

1. create Agent row generation 0 with `workspace_state=preparing` and deterministic
   path `<data_dir>/worktrees/<agent-id>`;
2. journal exact repository, branch, base and intended path;
3. run `git worktree add` with explicit argv;
4. read back `git worktree list --porcelain`, branch and HEAD;
5. run profile setup argv entries in order under process custody;
6. commit `workspace_state=ready`; only then may first native start reserve/dispatch.

Lost worktree command response is reconciled from exact path/branch/HEAD; it is never
blindly rerun. A lost setup command has no generic readback, so workspace becomes
`unknown`, launch remains blocked, and owner resolves or removes/recreates explicitly.
Setup steps are not assumed idempotent.

Branch/path collision or existing mismatched worktree fails. Cleanup is explicit,
stopped-agent only, exact path under data root, and requires Git-clean state except
allowlisted relative scratch paths. Scratch cleanup rejects symlink/device/alternate
stream escape. Branch is retained.

## Git/filesystem monitor

One monitor per distinct live worktree:

- `git --no-optional-locks status --porcelain=v2 --branch -z`;
- bounded log on HEAD change;
- base-vs-HEAD diff for still-unmerged paths;
- watcher debounce 500 ms, minimum refresh 2 s;
- slow status backs off to five minutes and reports health;
- remote refs change only after explicit fetch or optional read-only `ls-remote`.

Monitor never writes index, refs or objects.

### Durable edit attribution

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

Generation-matched native edit events upsert this projection with the event. When Git
proves path clean, remove rows for that worktree. Terminal/superseded generations stay
until their paths are clean; restart never reconstructs current attribution from event
history. Shared-checkout unowned paths are `unattributed`.

### Read-only progress fingerprint

```text
HEAD + sorted(dirty/untracked path, file type, size, content hash)
```

Hash only already-dirty/untracked paths, excluding ignored/scratch. Hash symlink target
text without following. Cache until watcher/metadata change and enforce file/byte caps.
Incomplete is unavailable and never counts as no progress. No temporary index,
`git add`, object writes or `write-tree`.

## Liveness

Only exact generation model progress updates `last_progress_at`: turn/message/tool/
plan/file/request/answer or a native heartbeat proving loop advancement.

| Observation | Conclusion |
| --- | --- |
| no progress while busy/waiting past threshold | stalled flag only |
| parent idle, child busy | parent idle; tree incomplete |
| process alive, stream disconnected | source stale; state unchanged |
| exact custody empty + authoritative source drained/gapped | lost for that generation |
| shared server live, session readback unknown | stale/unknown, not lost |
| handoff gap | explicit gap; no inferred event |
| old generation/duplicate | diagnostic only |

## Restart reconciliation

On daemon start:

1. reopen exact custody/service/guardian identities;
2. reattach live shared/service sessions and snapshot open commands;
3. settle missing owned groups only after terminal-source rules;
4. reconcile `preparing|unknown` workspaces from Git/worktree journal;
5. rebuild Git facts and load `agent_edits`;
6. keep previously dispatched command/action/workspace effects unknown until exact
   readback.

## Views

`watch`, `top`, `show`, `status` expose generation, active turn/open stop,
workspace preparation, exact/degraded custody/guardian health, source gaps, last model
progress, processes, usage, branch/dirty/edit attribution, provider holds and Store/WAL.
Views query projections/streams and are not another state machine.
