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

## Process monitor

- Identity is `(pid, start_time)`, never PID alone (PIDs are reused).
- Plugins register their native PIDs; the monitor adds descendants by walking the
  parent chain every `process.scan_interval` (default 2 s). One `sysinfo::System`
  instance, refreshed selectively for watched PIDs only.
- `process.exited` when a watched identity disappears. If all `harness`/`server`
  processes of an agent are gone and the plugin reported no `session.ended` within
  3 s, emit `session.lost` (the reducer then moves the agent to `lost`).
- `process.stats` (CPU, RSS) every 5 s for watched processes; ephemeral.
- Stop/kill: Windows Job object per plugin instance with `KILL_ON_JOB_CLOSE` (if the
  daemon dies, the processes it launched die too — no orphans); Unix process group.
  `stop --force` terminates the agent's process tree and verifies the identities are
  gone; if not, the agent stays `stopping` and an event says which PIDs survived.
  Processes Eliot only attached to (external services) are never killed.
- Shared processes (an app-server serving several agents, a proxy) are watched once
  and attributed to all their agents; they are not killed by stopping one agent.

## Git monitor

Per distinct worktree used by any live agent:

- Refresh on `fs.changed` (debounce 500 ms) and every `git.poll_interval` (default
  15 s), never more often than every 2 s per worktree.
- Commands (always `git --no-optional-locks`, so the monitor never takes
  `index.lock` and never competes with agents):
  - `status --porcelain=v2 --branch -z` → branch, HEAD, upstream, ahead/behind, dirty set;
  - on HEAD change: `log --format=%H%x00%ae%x00%s --name-only <old>..<new>` → `git.commit`;
  - `diff --name-only <base>...HEAD` → committed-but-unmerged set (for `who`).
- Remote refs change only through an explicit fetch (rule action or command) or the
  optional `git.remote_poll` (`ls-remote`, default off). `git.ref` events report them.
- The monitor keeps per-worktree sets in memory (`dirty`, `branch_changes`) for
  [`who`](coordination.md#who-works-on-this). They are rebuilt from Git after restart;
  they are not stored.
- Huge repos: if `status` takes longer than 2 s, the interval doubles (max 5 min) and
  `source.health` reports `slow`.

## Liveness rules

| Observation | Conclusion |
| --- | --- |
| No events while `busy` for `stall_after` | `agent.stalled` (flag), nothing else |
| Parent idle, children busy | parent is idle; the tree is not done |
| Process alive, stream disconnected | source `stale`; agent state unchanged |
| Process gone, no terminal event | `session.lost` → `agent.lost` |
| Source not read for 3 × its interval | `source.health status=stale`; values shown as `unknown` |
| Provider 429 | `provider.limited kind=rate_limit`; not quota, model unchanged |

## Views

- `eliot watch` — stream with filters; text format
  `HH:MM:SS a12 writer  turn.completed  ok 2m14s 18k tok`.
- `eliot top` — refreshes every second: id, name, harness, state (+stalled), current
  turn time, tokens, cost, CPU, RSS, branch, dirty count, last event age. Sort and
  filter keys. Built on `agent.list` + `events.subscribe`; no separate state.
- `eliot show A` — one agent: profile, effective settings, pending requests, goal,
  processes, workspace, last 20 events, unread mail.
- `daemon.status` — per-source health table. A stale source is shown as stale, never
  hidden.
