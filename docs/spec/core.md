# Core model

Types live in `crates/eliot-core` (pure, no I/O). Storage is in `crates/eliot-store`.
Field names below are the wire/JSON names; Rust uses the same names in `snake_case`.
All timestamps are Unix milliseconds (UTC).

## Identifiers

| Id | Form | Source | Notes |
| --- | --- | --- | --- |
| `AgentId` | `a<u64>` e.g. `a42` | Store sequence | Short so people and agents can type it. |
| `CommandId` | string (≤ 128 chars); a ULID when generated | Caller; the CLI generates one only when `--id` is not given and prints it. Derived ids: `<selector command id>/<agent id>` for selector expansions, `rule:<rule id>@<key>/<n>` for rule firings | Idempotency key: resubmitting the same id with the same verb and args returns the existing command; the same id with different content fails `invalid`. A retry after a lost reply must reuse the id (`--id`); a new id is a new command. |
| `seq` | `u64` | Store, per persisted event | Total order of persisted events. |
| `MessageId` | `m<u64>` | Store sequence | Mailbox ([coordination](coordination.md)). |
| `native_ref` | opaque string | Plugin | Native thread/session/conversation id. Eliot never parses it. |
| `native_scope` | `<harness>:<store or server id>` | Plugin | `native_ref` is unique only inside its scope (two `CODEX_HOME`s can both hold `ses-1`). At most one live, controlling agent per (`native_scope`, `native_ref`): attaching or resuming a session another live agent controls fails `not_allowed` (observe-only attach is fine). |

Agents also have an optional unique `name` (e.g. `writer-1`); every CLI argument that
takes an agent accepts id or name.

## Agent

```text
Agent {
  id, name?, project,                  // project: --project, else Git top-level dir name of the workspace, else "default"
  parent?, root, depth,                // tree; root = self for top-level agents
  origin: eliot | native | external,   // launched by us | subagent spawned by a harness | observed, not launched by us
  harness, profile,                    // plugin kind and profile name
  native_scope?, native_ref?,          // see Identifiers
  workspace?,                          // absolute path of the agent's checkout
  branch?, base_ref?,                  // set when Eliot created a worktree (or detected a branch); used by `who`
  state, stalled_since?,
  pending: [Request],                  // open approvals/questions: {request_id, kind, details, created_at}
  goal?,                               // last goal.updated data per mode: {native?, eliot?}
  effective?,                          // effective settings reported by the plugin (model, permissions, endpoint, ...)
  labels: map<string,string>,          // drive selectors and rules (role=writer)
  control: self | tree | project,      // what this agent may command (see coordination)
  observe_only: bool,                  // Eliot watches but cannot command it
  created_at, updated_at
}
```

`origin = native` agents are discovered children (Codex subagent threads, Claude
subagents, Antigravity subagents). They are full agents: watchable, and controllable
only through the verbs the plugin lists in `child_verbs`.

`origin = external` agents are sessions Eliot found but did not launch
(`eliot discover` / `attach`). They are `observe_only = true` unless the plugin could
attach for control; the router rejects commands to observe-only agents with
`not_allowed`.

### States

Live states: `starting`, `idle`, `busy`, `waiting`, `stopping`.

| From | Trigger | To |
| --- | --- | --- |
| — | `launch` accepted (router creates the row) | `created` |
| `created` | router input `start_dispatched` (it found a free launch slot: profile and proxy `max_live`, no provider hold) | `starting` |
| `created` | `stop` accepted (the queued launch is cancelled; no native I/O) | `stopped` |
| `starting` | `session.ready` | `idle` |
| `starting` | router input `start_failed` (`agent.start` error or timeout), or `session.error` | `failed` |
| `idle` | `turn.started` | `busy` |
| `busy` | `approval.requested` / `question.asked` | `waiting` |
| `waiting` | last pending request resolved (`approval.resolved` / `question.answered`) | `busy` |
| `busy`, `waiting` | `turn.completed` (any status) | `idle` |
| any live except `stopping` | `stop` accepted | `stopping` |
| `stopping` | `agent.stop` reply, or `session.ended` | `stopped` |
| any live | `session.ended` (the session ended natively) | `stopped` |
| any live | `session.error` (fatal for this agent) | `failed` |
| any live | `session.lost` (processes gone without `session.ended`, or re-attach failed) | `lost` |
| `stopped`, `lost`, `failed` | `resume` accepted | `starting` |

`detach` leaves the state as it is and sets `observe_only`. Anything else leaves the
state unchanged (and is a reducer test case). `stalled` is a
flag, not a state: `busy`/`waiting` with no event for `stall_after` sets
`stalled_since`; the next event of that agent clears it.

The reducer `fn apply(agent: &mut Agent, input: &Input) -> Option<AgentState>` in
`eliot-core` is the only code that changes `state`. `Input` is an event, an accepted
command (`stop`, `resume`), or a router input (`start_dispatched`, `start_failed`);
deciding launch slots and holds is the router's job, not the reducer's. It is pure and table-tested against the table above. Every state change is
published as a derived event `agent.<new state>`.

## Profile

A named launch recipe in config ([configuration](configuration.md)). Defaults in
brackets:

```text
Profile { harness, extends?, cmd?: [string],      // cmd: binary for plugins that launch any agent (ACP)
          model?, effort?, permissions [workspace-write]: read-only | workspace-write | full | ask,
          args [[]], env [{}], env_clear [false], secret_env [[]], proxy?,
          workspace_mode [shared]: shared | worktree,
          control [self]: self | tree | project, git_identity [agent]: agent | inherit,
          system_note?: path, labels [{}], stall_after [5m], max_live? [unlimited],
          billing?, account?, setup [[]],         // setup: argv lists run in a new worktree (submodules, LFS)
          scratch [["target/", "node_modules/"]],  // ignored paths `eliot worktree remove` may delete
          notify_parent [true], wake_on_mail [true] }   // parent notices; idle wake-up on new mail
```

`max_live` caps live agents of this profile (provider quota and subscription limits
are per account, not per agent). Launches beyond it wait in `created`, first in first
out, and start when a slot frees; `eliot ls` shows them as queued.

`launch --harness H` without `--profile` uses the profile named `H` if one exists,
else `{harness = H}` with defaults. A profile `fake` (`harness = "fake"`) always
exists.

`permissions` is normalized; each plugin maps it to native sandbox/approval settings
(see its harness page) and reports the effective value. Requested and effective values
are both shown; Eliot never pretends a request was applied.

`model` and `effort` are the harness's own values (see its harness page); Eliot never
maps between scales. `configure` with a value the plugin does not know fails `invalid`
before any native call.

`billing` (`subscription` | `api_key` | `free` | `proxy`) and `account` (a label) name
which budget the profile spends; `eliot show` and usage events carry both, and Eliot
never changes them ([architecture](../architecture.md#core-rules) rule 8).

## Events

```text
Event { seq?, ts, kind, agent?, source, cause?, data }
```

- `seq` is set only for persisted events.
- `source`: `plugin:<harness>` | `process` | `git` | `fs` | `hook` | `transcript` |
  `router` | `rule:<id>` | `timer:<id>` | `goal` | `cli` | `daemon`.
- `cause`: `{command?, rule?, depth}` — links effects to what triggered them; used for
  loop protection ([automation](automation.md#guardrails)).
- `data`: kind-specific JSON, bounded (default 64 KiB; larger text is truncated with
  `truncated: true` and a pointer to the native transcript when known).
- Before an event is persisted or fanned out, the daemon replaces the values of the
  credential variables it knows (each proxy's `api_key_env`, profile `env` entries
  listed in `secret_env`) with `***`. Agents echo secrets into commands and messages;
  the Store must not keep them. Pattern-based redaction is L7.

### Event kinds

P = persisted, E = ephemeral (fan-out only). Plugins may add `x.<harness>.*` kinds;
the core ignores them except for display and rule matching.

| Kind | P/E | Data (main fields) |
| --- | --- | --- |
| `session.ready` | P | `native_scope`, `native_ref`, `pids`, `effective` (model, permissions, `system_note: native|prepended`, `endpoint?`) |
| `session.ended` / `session.error` / `session.lost` | P | `reason`, `exit_code?` |
| `agent.created` / `agent.starting` / `agent.idle` / `agent.busy` / `agent.waiting` / `agent.stopping` / `agent.stopped` / `agent.failed` / `agent.lost` | P | `from`, `reason?` — derived by the reducer, `source=daemon` |
| `agent.stalled` / `agent.unstalled` | P | `since` |
| `turn.started` | P | `turn_id`, `input_ref?` |
| `turn.completed` | P | `turn_id`, `status: completed|interrupted|failed`, `usage?`, `final_text?` |
| `message.user` / `message.agent` | P | `text` (bounded), `turn_id` |
| `message.delta` | E | `text` |
| `tool.started` / `tool.completed` | P | `tool`, `summary`, `ok?` |
| `file.changed` | P | `path`, `change` (from native item or fs monitor) |
| `plan.updated` | P | `steps` |
| `approval.requested` / `approval.resolved` | P | `request_id`, `kind: command|file|network|other`, `command?` (kind=command), `path?` (kind=file), `details`, `decision?` |
| `question.asked` / `question.answered` | P | `request_id`, `text`, `options?` |
| `child.spawned` | P | `child` (AgentId), `native_ref` |
| `goal.updated` | P | `status: active|paused|done|blocked|cleared`, `mode: native|eliot`, `reason?` |
| `usage.updated` | E (P on turn end) | `tokens_in`, `tokens_out`, `cost_usd?`, `scope: turn|session` |
| `provider.limited` | P | `kind: rate_limit|quota|model_gone|auth`, `retry_after?` |
| `process.started` / `process.exited` | P | `pid`, `start_time`, `exe`, `role`, `exit_code?` |
| `process.stats` | E | `pid`, `cpu_pct`, `rss_bytes` |
| `git.status` | P (on change) | `worktree`, `branch`, `head`, `dirty: [path]`, `ahead`, `behind` |
| `git.commit` | P | `worktree`, `sha`, `author`, `subject`, `files` |
| `git.ref` | P | `remote`, `ref`, `old`, `new` |
| `fs.changed` | E | `worktree`, `paths` (debounced) |
| `command.accepted` / `command.queued` / `command.dispatched` / `command.applied` / `command.rejected` / `command.failed` / `command.unknown` / `command.cancelled` / `command.resolved` | P | `command`, `verb`, `reason?` (`resolved`: `as`, `by`, `reason`) |
| `rule.fired` / `rule.failed` / `rule.suppressed` | P | `rule`, `actions`, `reason?` |
| `mail.sent` / `mail.presented` / `mail.read` | P | `message`, `from`, `to` |
| `var.changed` | P | `scope`, `key`, `version` |
| `claim.added` / `claim.released` / `claim.conflict` | P | `glob`, `agents` |
| `source.health` | P (on change) | `source`, `status: ok|slow|stale|error`, `last_ok`, `error?` |
| `plugin.started` / `plugin.exited` / `daemon.started` | P | `name`, `version`, `reason?` |

## Verbs

The closed command language. A plugin declares which verbs it supports (`verbs`,
`child_verbs` in its manifest). The router rejects anything else before native I/O.

| Verb | Args | Semantics | Precondition |
| --- | --- | --- | --- |
| `launch` | `profile?`, `harness?`, `prompt?`, `workspace?`, `parent?`, `name?`, `project?`, `control?`, `labels?` | Create an agent and start a native session. | — |
| `send` | `text` | New user turn. If the agent is busy, the command waits in the Store as `queued` (no native I/O) and is dispatched when the agent becomes idle, in order. Survives plugin and daemon restarts. | live |
| `steer` | `text` | Add input to the **running** turn. | `busy`; native support |
| `inject` | `text` | Add context without starting a turn by itself; delivered at the next native safe point (hook `additionalContext`, pending-input queue, or prepended to the next `send`). | live; plugin decides mechanism and reports it |
| `interrupt` (CLI alias `abort`) | — | Abort the running turn; session stays alive. | `busy`/`waiting` |
| `stop` | `force?` | End the session. `force` kills the process tree. (The CLI's `--tree` is the selector `@tree:<agent>`.) | any live |
| `resume` | `prompt?` | Re-open a `stopped`/`lost`/`failed` session by `native_ref`. Never resends old input. | native support |
| `approve` / `deny` | `request_id?` (default: the only pending request), `remember?` | Answer a pending approval. | request pending |
| `answer` | `request_id?`, `text` or `choice` | Answer a pending question. | request pending |
| `configure` | `model?`, `effort?`, `permissions?` | Change settings; the reply says when it applies: `now`, `next_tool`, `next_turn`, `next_session`. | live |
| `goal.set` | `text`, `mode?: auto|native|eliot`, `check?`, `budget?`, `cadence?` | `native` = the harness's own goal; `eliot` = the Eliot Goal loop ([automation](automation.md#goal-loop)); `auto` (default) = native if supported, else eliot. One goal per mode; a native and an Eliot goal may run at the same time with different texts. | live |
| `goal.pause` / `goal.resume` / `goal.clear` | `mode?` (default: every active mode) | | goal exists |
| `cancel` | `command` | Withdraw a `queued` send that has not started (no native I/O). A started turn needs `interrupt`. | that command is `queued` |
| `detach` | — | Stop controlling the agent but leave the native session running (its goal, children and turns continue). The agent becomes `observe_only`. Available from L7.6. | live; plugin declares it (only sessions that live outside Eliot's process tree, e.g. a service or an owner-run server) |
| `compact` / `fork` / `background` | — | Native only (`background` moves the running foreground tool to the background where the harness supports it). | native support |

- `check`: `marker:<TEXT>`, or an argv list run without a shell in the agent's
  workspace (`["cargo", "test", "-p", "foo"]`; the CLI splits `--check "cargo test"`
  with shell-word rules but runs no shell). Wanting a shell means writing it:
  `["cmd", "/C", "..."]`. Output is capped and kept as the tail.
- `budget`: `{max_turns?, max_cost_usd?, no_progress?}`. `max_cost_usd` on a plugin
  that does not report cost (`features.cost = false`) is rejected `unsupported`.
- `steer` has no implicit fallback: `steer --or-send` sends when steering is
  impossible; without the flag the command is rejected.
- Handled by the daemon, never sent to a plugin: `cancel`, and `goal.*` in `eliot`
  mode. A plugin declares `goal.*` only for its native goal.
- A queued `send` runs with the settings in effect when it is dispatched (a later
  `configure` that already applied is in effect).

## Commands

```text
Command { id, target?: AgentId, selector?, verb, args, origin, cause?, state, outcome?, created_at, updated_at }
origin = owner | agent:<id> | rule:<id> | timer:<id> | goal:<agent>
state  = accepted → [queued →] dispatched → applied | rejected | failed | unknown
         queued → cancelled
```

- `launch`: the router first creates the agent row (`state = created`); the command
  targets that new agent and the submit result returns its id. Every other verb needs
  `target` or `selector`.
- Ordering: commands to one agent are dispatched to its plugin one at a time in
  acceptance order — the next one after the previous got its reply or became
  `unknown` — so `configure` then `send` cannot run on stale settings. `interrupt` and
  `stop` skip the line. A `queued` send does not block other verbs (a `steer` still
  reaches the running turn); queued sends leave in order when the agent is idle.
- A client that disconnects or times out never cancels an accepted command; read its
  state back with `eliot cmd show ID`.
- `rejected`: refused before any native I/O — by the router (invalid, unsupported,
  not allowed, wrong state) or by a plugin that knows no native request was sent.
  A plugin that is not sure returns `unknown`, never `rejected`. Router rejections are
  stored too (with their reason), so `cmd show` and the same-id rule work for them.
- `failed`: native side reported an error — nothing applied.
- `unknown`: may or may not have happened (timeout, plugin crash, lost reply). Never
  retried automatically. Only `interrupt` and `stop` are retried once after a plugin
  restart, because repeating them is harmless. After a plugin restart the host asks
  `agent.snapshot`: `applied` → `applied`; `not_applied` → `failed`
  (`reason = not_applied`); otherwise it stays `unknown`.
- A daemon restart turns every `dispatched` command into `unknown` (queued ones stay
  queued). Rule actions that were running (`run`, git actions) are settled only by
  readback after their process tree is proven gone, never by running them again.
- Only readback or the owner's `eliot cmd resolve ID --as applied|failed --reason
  TEXT` clears `unknown`; time never does. The resolution is recorded as an event.
- `launch` checks before creating the agent: the profile resolves, the workspace
  exists, the plugin is up and its manifest says `available: true`, the proxy (if
  any) is healthy, and the tree limits
  allow it. A failed check returns `invalid`, `unavailable` or `not_allowed` and
  creates nothing.
- Selectors: `@children` | `@parent` | `@siblings` | `@tree:<agent>` |
  `@project[:<name>]` | `@label:<key>=<value>` | `@all`, resolved relative to the
  caller (for the owner, `@children` = top-level agents). A selector command is stored
  with `selector` set and expands at submit time, in the same transaction, into one
  command per agent with id `<selector command id>/<agent id>` and
  `cause.command = <selector command id>`, so a repeated submit never duplicates them.
  The selector command is `applied` once all are created; its `outcome` lists them.

## Storage

`rusqlite` with the bundled SQLite: one writer thread that owns the write connection,
a small fixed set of read connections. One SQLite file `eliot.db` in the data dir,
WAL mode, `PRAGMA user_version` = schema
number, migrations are numbered SQL files applied in one transaction each. No schema
hashes. Open order matters: set `busy_timeout` first; check `application_id`
(`0x454C494F`, "ELIO", set by the first migration) and
`user_version` (refuse a foreign or newer database); only then set
`journal_mode=WAL` (persistent — never on a file that failed the check) and
`synchronous=FULL`; `foreign_keys=ON` on every connection; writes use
`BEGIN IMMEDIATE`; never create a database over an existing non-empty file. A new
file gets `application_id` and its schema from migration 1 in one transaction. No retry
loop around `SQLITE_BUSY`. One writer task owns the write connection and commits in
batches (≤ 10 ms or ≤ 500 statements); readers use separate connections.

Nothing is reported `accepted` or `applied` before its commit. A failed commit (disk
full, locked file) is reported as `source.health` for `store` with the scope it hit,
and the router admits no new command until commits succeed again.

Layer 1 schema (later layers add their own tables in their own migrations):

```sql
CREATE TABLE agents (
  id INTEGER PRIMARY KEY, name TEXT UNIQUE, project TEXT NOT NULL,
  parent INTEGER REFERENCES agents(id), root INTEGER NOT NULL, depth INTEGER NOT NULL,
  origin TEXT NOT NULL, harness TEXT NOT NULL, profile TEXT NOT NULL,
  native_scope TEXT, native_ref TEXT, workspace TEXT, branch TEXT, base_ref TEXT,
  state TEXT NOT NULL, stalled_since INTEGER,
  pending TEXT NOT NULL DEFAULT '[]', goal TEXT, effective TEXT,
  labels TEXT NOT NULL DEFAULT '{}', control TEXT NOT NULL DEFAULT 'self',
  observe_only INTEGER NOT NULL DEFAULT 0,
  token TEXT NOT NULL,   -- routing handle, not a credential (decisions D-07); never printed
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);
CREATE UNIQUE INDEX agents_native ON agents(native_scope, native_ref)
  WHERE observe_only = 0 AND state IN ('starting','idle','busy','waiting','stopping');

CREATE TABLE events (
  seq INTEGER PRIMARY KEY, ts INTEGER NOT NULL, kind TEXT NOT NULL,
  agent INTEGER, source TEXT NOT NULL, cause TEXT, data TEXT NOT NULL
);
CREATE INDEX events_agent ON events(agent, seq);
CREATE INDEX events_kind ON events(kind, seq);

CREATE TABLE commands (
  id TEXT PRIMARY KEY, target INTEGER, selector TEXT, verb TEXT NOT NULL,
  args TEXT NOT NULL, origin TEXT NOT NULL, cause TEXT, state TEXT NOT NULL, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX commands_target ON commands(target, created_at);
CREATE INDEX commands_open ON commands(state) WHERE state IN ('accepted','queued','dispatched','unknown');
```

`pending`, `goal` and `effective` are JSON written by the reducer in the same
transaction as the event that changed them.

Retention: events older than `daemon.retention.events_days` (default 30) are deleted
in small batches, except `command.*` and `agent.*` events of agents that still exist.
No other cleanup machinery.
