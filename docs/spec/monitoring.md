# Monitoring

Every source runs independently, emits bounded facts and reports its own health. A
failed source never blocks the router or another source. It also never gets to invent
facts owned by a healthier source.

## Sources

| Source | What it sees | How | Events |
| --- | --- | --- | --- |
| Native stream | session, turns, messages, tools, approvals, children, goals, usage | plugin protocol | `session.*`, `turn.*`, `message.*`, `tool.*`, `approval.*`, `child.spawned`, `goal.updated`, `usage.updated` |
| Native transcript | the same for discovered/observe-only sessions, or explicit stream recovery | plugin tails native files | only the event families assigned to it |
| Native hooks | synchronous decisions and facts missing from the primary stream | hook shim / SDK callback | assigned `approval.*`, tool/prompt facts, `x.<harness>.*` |
| Processes | life and resource use of owned processes | Job/process-group custody + registered PIDs | `process.started/exited`, `process.stats`, `session.lost` |
| Git | branch, HEAD, dirty files, commits, ahead/behind, remote refs | read-only Git CLI | `git.status`, `git.commit`, `git.ref` |
| Filesystem | workspace changes | `notify`, debounced | `fs.changed`, Git refresh |
| Proxy/provider | health, usage, limits | configured management API | `source.health`, `provider.limited` |
| Derived | stalls, durations, idle time | timers over progress facts | `agent.stalled/unstalled` |
| Store/config | commit availability and config reload | daemon | `source.health` |

## Source ownership

For each `(agent, generation, event family)`, exactly one source is authoritative.
Examples: the live Codex app-server stream owns turns/messages/tools; a transcript may
own those families for an observe-only thread; a hook may own permission decisions
while the stream owns tool lifecycle.

Rules:

1. `session.ready` records the selected owners and any native cursor/watermark in
   `effective.event_owners` when the harness has several possible sources.
2. A backup source does not publish the same normalized family concurrently. It may
   collect raw position privately.
3. Handoff starts after the last committed native id/cursor. If the old and new formats
   cannot prove a complete boundary, emit `source.health status=stale` with a bounded
   gap description; do not replay old completions or treat the missing interval as
   empty.
4. Duplicate native ids from reconnect/replay are suppressed by the plugin before the
   bus. The daemon additionally rejects wrong-generation input.
5. Hooks and transcripts may still emit harness-specific `x.<harness>.*` diagnostics,
   but those diagnostics never drive core state unless that family was assigned to the
   source.

This prevents a live stream plus transcript tail from firing two `turn.completed`
events and therefore two rules.

## Process monitor and custody

Process identity is `(pid, start_time)`, never PID alone. `start_time` is the OS
creation time at native resolution (Windows `GetProcessTimes`; Linux proc start ticks).

### Per-agent mode

A per-agent plugin and every native child it launches are placed in one agent-owned
Windows Job / Unix process group before the child can run:

- Windows: create suspended, assign to Job, then resume. The Job uses
  `KILL_ON_JOB_CLOSE` for daemon-owned stdio sessions.
- Unix: create the process group before exec. The plugin must exit when stdin closes;
  on plugin failure the daemon explicitly terminates and verifies the group.

The daemon owns the custody handle. A plugin exit causes it to stop the remaining
exact group and verify every known `(pid,start_time)` is gone. Only then may it emit
`session.lost`. `stop --force` targets this group and cannot affect another agent.

### Shared mode and services

A shared plugin may attach to a native service or launch one daemon-managed service.
The service is watched once and attributed to its users. Stopping one agent uses an
exact native session operation; it never kills the shared plugin/service. If the
native service has no isolated force operation, `stop --force` is `unsupported`.
Attached owner-run services are never killed.

Service mode (L7.6) uses a separate custody object without kill-on-close so a daemon
upgrade can re-attach. Its endpoint, process identity and native session refs are
persisted before use.

### Observation

- Plugins register every PID they know; custody enumerates descendants so an
  intermediate parent exiting does not hide grandchildren.
- Scan `process.scan_interval` (default 2 s). One `sysinfo::System` is refreshed only
  for watched PIDs; platform calls provide exact birth identity.
- `process.exited` is emitted when the identity disappears. Exit code may be unknown
  after a crash; never record unknown as 0.
- A process disappearing while identity is read counts as gone, not monitor failure.
- If all owned harness processes for a generation are gone and no `session.ended`
  arrived within 3 s, emit `session.lost`. For a shared service, plugin/session
  readback—not the shared process alone—decides which agents were lost.
- `process.stats` is ephemeral every 5 s. It never counts as model progress and never
  clears a stall.

## Starting processes

These rules apply to plugins, services, hook shims, scripts and Git actions:

- Resolve executables to full paths; pass argv as a list, never an implicit shell.
- Long input uses stdin or files, never argv.
- Windows console children use `CREATE_NO_WINDOW`, not `DETACHED_PROCESS`.
  Environment keys such as PATH are merged case-insensitively; PowerShell 7 is
  `pwsh.exe` resolved by path.
- Eliot-written text is UTF-8 without BOM and LF. Git always has
  `GIT_TERMINAL_PROMPT=0` and no editor.
- Do not replace a script/hook binary that may be running; install a new versioned
  path and switch future launches.
- Normalize paths before comparison: remove Windows verbatim prefixes, unify
  separators and compare case-insensitively on Windows.
- Windows file deletion/replacement can fail while a handle is open; tests close
  SQLite/files before cleanup rather than looping forever.

## Git monitor

One monitor exists per distinct worktree used by a live agent.

- Refresh on `fs.changed` (500 ms debounce) and every `git.poll_interval` (15 s), but
  no more often than every 2 s per worktree.
- All monitor commands use `git --no-optional-locks` and never write the index:
  - `status --porcelain=v2 --branch -z` → branch, HEAD, upstream, dirty set;
  - on HEAD change, bounded `log --format=%H%x00%ae%x00%s --name-only old..new` →
    `git.commit`;
  - current branch-change set = paths in `diff --name-only base...HEAD` that still
    differ in `diff --name-only base HEAD`.
- Remote refs change only after explicit fetch or optional `git.remote_poll`
  (`ls-remote`, default off).
- Dirty/branch sets live in memory for [`who`](coordination.md#who-works-on-this) and
  are rebuilt after restart.
- If status exceeds 2 s, back off the interval up to 5 minutes and report
  `source.health status=slow`.

### Read-only workspace fingerprint

The Eliot Goal loop needs a progress signal without `git add`, a temporary index or
writing objects into the repository.

For a Git workspace, the monitor exposes a fingerprint of:

```text
HEAD + sorted(dirty/untracked path, file type, size, content hash)
```

It hashes only paths already reported dirty/untracked, excludes ignored files and
profile `scratch` paths, reads symlink targets rather than following them, and caches
hashes until filesystem metadata/a watcher event changes. The content hash is computed
in Eliot (for example BLAKE3); `git hash-object -w`, `git add` and `write-tree` are
forbidden. A configurable byte/file cap protects huge workspaces. Crossing the cap
returns `unavailable`, never a fake stable fingerprint; a no-progress budget cannot
block solely on incomplete fingerprint data. Non-Git workspaces use the same bounded
file-set mechanism when the plugin supplies changed paths, otherwise check output is
the only progress signal.

## Liveness

Only progress-bearing native facts update `last_progress_at`: turn/message/tool/plan,
file-change, request/answer and explicit plugin heartbeat that proves the model loop
advanced. Process samples, Git polling, mail and source-health events do not.

| Observation | Conclusion |
| --- | --- |
| No progress fact while `busy`/`waiting` for `stall_after` | `agent.stalled` flag; no state change or automatic restart |
| Parent idle, children busy | parent idle; tree not done |
| Process alive, stream disconnected | source stale; state unchanged |
| Exact owned process group gone, no terminal event | `session.lost` for that generation |
| Polled source unread for 3 × interval | source `stale`; its values display unknown |
| Event-driven connection lost | source `stale` until recovery; silence itself is not terminal |
| Handoff cursor cannot cover an interval | explicit source gap; no inferred events |
| Provider 429 | `rate_limit`, not quota unless native evidence says quota |
| Old-generation notification | ignored from normal bus; counted/logged as stale input |

## Views

- `eliot watch`: filtered event stream, for example
  `12:14:03 a12#4 writer turn.completed ok 2m14s 18k tok`.
- `eliot top`: id/generation, name, harness, state + stalled/open stop, turn age,
  tokens/cost, CPU/RSS, branch, dirty count, source health and last-progress age. It is
  `agent.list` + event subscription, not separate state.
- `eliot show A`: profile/effective settings, generation/native refs, event owners and
  gaps, pending requests/commands, goal, processes, workspace and recent events.
- `daemon.status`: every source and plugin instance with health, custody mode, last
  successful read and gaps. Stale/unknown values are shown, never hidden.
