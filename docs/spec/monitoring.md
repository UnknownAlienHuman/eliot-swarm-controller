# Monitoring

Every source runs independently, emits normalized events on the bus and reports its
own health (`source.health`). A failing source never blocks another one or the router.

## Sources

| Source | What it sees | How | Events |
| --- | --- | --- | --- |
| Native stream | turns, messages, tools, approvals, children, goal, usage | plugin protocol (live) | `turn.*`, `message.*`, `tool.*`, `approval.*`, `child.spawned`, `goal.updated`, `usage.updated` |
| Native transcripts | the same, for sessions Eliot did not launch or after a restart | plugin tails native files | same kinds, `source=transcript` |
| Native hooks | tool calls, prompts, stops, subagent stops, compaction | hook shim → daemon | `tool.*`, `approval.requested`, `x.<harness>.*` |
| Processes | life of native processes and their descendants | `sysinfo` + plugin-registered PIDs | `process.started/exited`, `process.stats` (E), `session.lost` |
| Git | branch, HEAD, dirty files, new commits, ahead/behind, remote refs | `git` CLI, read-only flags | `git.status`, `git.commit`, `git.ref` |
| Filesystem | changes in agent workspaces | `notify` watcher, debounced | `fs.changed` (E), triggers Git refresh |
| Proxy/provider | proxy health, usage, limits | proxy management API when configured | `source.health`, `provider.limited` |
| Derived | stalls, idle time, turn durations | timers over the above | `agent.stalled/unstalled` |
| Store / config | failed commits; config reloads and parse errors | the daemon itself | `source.health` (`store`, `config`) |

## Process monitor

- Identity is `(pid, start_time)`, never PID alone (PIDs are reused). `start_time` is
  the OS creation time at full resolution (Windows `GetProcessTimes`, 100 ns; Linux
  `/proc/<pid>/stat` start ticks), not a seconds value. Linux start ticks count from
  boot, so there the identity also carries the kernel boot id
  (`/proc/sys/kernel/random/boot_id`); a row from an earlier boot never matches.
- **Custody** is the container that holds one agent's processes: the per-agent plugin,
  its harness and every tool they start. Descendants come from custody, not from
  walking parent PIDs (a parent chain breaks when an intermediate process exits): on
  Windows the Job's process list (`JOBOBJECT_BASIC_PROCESS_ID_LIST`), on Unix the
  process group. Scan every `process.scan_interval` (default 2 s). Plugins also report
  native PIDs they know; the monitor records only those inside the agent's custody.
  CPU/RSS sampling uses one `sysinfo::System`, refreshed only for watched PIDs.
- Windows custody is **exact**: a Job without breakaway contains every descendant.
  Unix custody is **degraded**: a process can leave its group (`setsid`) and a group
  does not die with the daemon. There `stop --force` kills what it can see and says
  what it could not prove; `eliot show` marks custody `degraded`, `eliot doctor`
  lists possible orphans. Exact Linux custody (cgroup v2) is L7.2.
- `process.exited` when a watched identity disappears. If all `harness`/`server`
  processes of an agent are gone and the plugin reported no `session.ended` within
  3 s, emit `session.lost` (the reducer then moves the agent to `lost`).
- `process.stats` (CPU, RSS) every 5 s for watched processes; ephemeral.
- A process that disappears while its identity is being read counts as gone (liveness
  loss), not as a monitor error.
- After a crash the exit code of a Job member may be unrecoverable: record it as
  `unknown` (never `0`); prove that a tree is gone by the Job's active-process count
  (Windows) or the process group being empty (Unix), not by one PID missing.
- Stop/kill: Windows Job object per agent (per plugin process for a `multi_session`
  plugin) with `KILL_ON_JOB_CLOSE` (if the daemon dies, the stdio sessions it launched
  die too — no orphans); Unix process group. Services (L7.6) get their own Job without
  kill-on-close. `stop --force` terminates the agent's process tree and verifies the
  identities are gone; if not, the agent stays `stopping` and an event says which PIDs
  survived. Processes Eliot only attached to (external services) are never killed.
- Shared processes (a proxy, a server several sessions use) belong to their service,
  not to any agent: they are watched once and never killed by stopping an agent.

## Starting processes

These rules apply to plugins, services, hook shims, scripts and git actions:

- Resolve the executable to a full path before spawning; pass argv as a list, never a
  shell string (scripts that want a shell say so in `cmd`).
- Windows: create console children with `CREATE_NO_WINDOW` (not `DETACHED_PROCESS`,
  which breaks console-dependent tools); put a child into its Job at creation
  (`CREATE_SUSPENDED` → assign → resume) so grandchildren cannot escape; when merging
  environment variables, treat `PATH`/`Path` case-insensitively; resolve PowerShell 7
  as `pwsh.exe` by full path, not `powershell.exe`.
- Long inputs (prompts, scripts) go through files or stdin, never argv (Windows
  command-line limit ~32 KiB).
- Text files Eliot writes are UTF-8 without BOM with LF line endings; Git always
  runs with `GIT_TERMINAL_PROMPT=0` and no editor.
- Never edit a script or hook file that may be running; write a new file and switch.
- Windows: a file cannot be replaced or deleted while any handle to it is open (also
  SQLite files with an open connection — close them first in tests).
- Paths are normalized before comparison (strip the `\\?\` verbatim prefix that
  `canonicalize` returns on Windows, unify separators, compare case-insensitively on
  Windows).

## Git monitor

Per distinct worktree used by any live agent:

- Refresh on `fs.changed` (debounce 500 ms) and every `git.poll_interval` (default
  15 s), never more often than every 2 s per worktree.
- Commands (always `git --no-optional-locks`, so the monitor never takes
  `index.lock` and never competes with agents):
  - `status --porcelain=v2 --branch -z` → branch, HEAD, upstream, ahead/behind, dirty set;
  - on HEAD change: `log --format=%H%x00%ae%x00%s --name-only <old>..<new>` → `git.commit`;
    after a daemon restart `<old>` is the `head` of the worktree's last stored
    `git.status` event, so commits made while the daemon was down are reported too
    (none stored → no baseline: `source.gap`, nothing invented);
  - committed-but-unmerged set (for `who`): paths in `diff --name-only <base>...HEAD`
    that still differ in `diff --name-only <base> HEAD` — after a squash merge the
    branch is not an ancestor of its base, and this drops the already-merged paths.
    `<base>` is the agent's `base_oid` (frozen at launch, below), else its `base_ref`,
    else `origin/HEAD` if it exists; with none of them there is no branch set.
- Remote refs change only through an explicit fetch (rule action or command) or the
  optional `git.remote_poll` (`ls-remote`, default off). `git.ref` events report them.
- Huge repos: if `status` takes longer than 2 s, the interval doubles (max 5 min) and
  `source.health` reports `slow`.
- The monitor keeps per-worktree sets in memory (`dirty`, `branch_changes`) for
  [`who`](coordination.md#who-works-on-this). They are rebuilt from Git after restart;
  they are not stored. The per-agent `edited` set is stored, because Git cannot
  rebuild it:

```sql
CREATE TABLE agent_edits (      -- L3.3 migration
  agent INTEGER NOT NULL REFERENCES agents(id), workspace TEXT NOT NULL,
  path TEXT NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY (agent, path)
);
CREATE INDEX agent_edits_path ON agent_edits(workspace, path);
```

  A `file.changed` event upserts its row in the same transaction; a row is deleted
  when Git shows the path clean.

## Workspaces

`launch --worktree BRANCH [--base REF]`:

1. At admission the daemon resolves `REF` (default: the repository's `HEAD`) to a
   commit OID with `git rev-parse --verify REF^{commit}` — read-only, no fetch — and
   stores it as the agent's `base_oid`. A ref that does not resolve fails `invalid`.
   A later move of the ref does not change a queued launch.
2. At start: `git worktree add` under `<data_dir>/worktrees/<agent>` — `-b BRANCH
   <base_oid>` if BRANCH does not exist, otherwise check it out (it must exist
   locally). Then read back the worktree's branch and HEAD.
3. Run the profile's `setup` argv lists once, in order, in the agent's custody and
   environment, token included (a new worktree has no ignored files, submodules or
   LFS content). A setup that fails fails
   the launch. A setup interrupted by a daemon crash has an unknown effect: it is not
   run again — the agent becomes `failed` (router input `start_failed`, reason
   `setup_unknown`) and the owner removes the worktree and launches again.
4. Only then `agent.start`.

Worktrees are never removed automatically: `eliot worktree remove A` removes a stopped
agent's worktree only if `git status --porcelain --ignored` lists nothing outside the
profile's `scratch` globs; the branch is kept.

## Liveness rules

| Observation | Conclusion |
| --- | --- |
| No events while `busy` for `stall_after` | `agent.stalled` (flag), nothing else |
| Parent idle, children busy | parent is idle; the tree is not done |
| Process alive, stream disconnected | source `stale`; agent state unchanged |
| Process gone, no terminal event | `session.lost` → `agent.lost` |
| Polled source (process, Git, proxy) not read for 3 × its interval | `source.health status=stale`; values shown as `unknown` |
| Event-driven source (native stream, transcript, hooks) lost its connection | `stale` until it reconnects; silence alone is not staleness |
| Provider 429 | `provider.limited kind=rate_limit`; not quota, model unchanged |

## Views

- `eliot watch` — stream with filters; text format
  `HH:MM:SS a12 writer  turn.completed  ok 2m14s 18k tok`.
- `eliot top` — refreshes every second: id, name, harness, state (+stalled), current
  turn time, tokens, cost, CPU, RSS, branch, dirty count, last event age. Sort and
  filter keys. Built on `agent.list` + `events.subscribe`; no separate state.
- `eliot show A` — one agent: profile, effective settings, pending requests, goal,
  processes, workspace, last 20 events (unread mail from L5.1).
- `daemon.status` — per-source health table. A stale source is shown as stale, never
  hidden.
