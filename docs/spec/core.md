# Core model

Types live in `crates/eliot-core` (pure, no I/O). Storage is in `crates/eliot-store`.
Field names below are the wire/JSON names; Rust uses the same names in `snake_case`.
All timestamps are Unix milliseconds (UTC).

## Identifiers

| Id | Form | Source | Notes |
| --- | --- | --- | --- |
| `AgentId` | `a<u64>` e.g. `a42` | Store sequence | Stable logical agent id; short enough to type. |
| `generation` | positive `u64` | Store, per agent | One native-session incarnation. Initial launch is generation 1; every explicit `resume` increments it. Re-attaching the same still-live incarnation preserves it. |
| `CommandId` | string (≤ 128 chars); a ULID when generated | Caller; the CLI generates one only when `--id` is absent and prints it. Derived ids: `<selector id>/<agent id>` and `rule:<rule id>@<key>/<n>` | Idempotency key for **admission**. Same id + same payload returns the existing command; same id + different payload fails `invalid`. It is not a vendor idempotency key. |
| `seq` | `u64` | Store, per persisted event | Total order of persisted Eliot events. |
| `MessageId` | `m<u64>` | Store sequence | Mailbox ([coordination](coordination.md)). |
| `native_ref` | opaque string | Plugin | Native thread/session/conversation id. Eliot never parses it. |
| `native_scope` | `<harness>:<store or server id>` | Plugin | `native_ref` is unique only inside its scope. At most one live controlling agent owns a (`native_scope`, `native_ref`); observe-only attachment is allowed. |

Agents may have a unique `name` (for example `writer-1`); every CLI agent argument
accepts id or name.

## Agent

```text
Agent {
  id, generation, name?, project,       // project: explicit, registered repo, Git top-level name, else "default"
  parent?, root, depth,
  origin: eliot | native | external,    // launched by us | native child | discovered/attached
  harness, profile,
  native_scope?, native_ref?,
  workspace?, branch?, base_ref?,
  state, active_turn?, stalled_since?, last_progress_at?,
  pending: [Request],                   // {request_id, generation, kind, details, created_at}
  goal?,                                // latest native/Eliot goal projection
  effective?,                          // effective model, permissions, endpoint, ...
  labels: map<string,string>,
  control: self | tree | project,
  observe_only: bool,
  created_at, updated_at
}
```

The daemon also stores one opaque routing token for the **current generation**. It is
never returned by list/show APIs. A launch or resume rotates it; a request carrying an
older token is rejected rather than being treated as the owner. This is an attribution
and stale-process fence, not a security boundary ([D-07](../decisions.md)).

`origin = native` agents are discovered children (Codex subagent threads, Claude
subagents, Antigravity subagents). They are first-class agents. They are controllable
only through the plugin's declared `child_verbs`.

`origin = external` agents come from `discover` / `attach`. They are
`observe_only = true` unless the plugin can attach for control. Commands to an
observe-only agent fail `not_allowed`.

### States

Live states are `starting`, `idle`, `busy`, `waiting`. An in-flight stop is shown by
the open `stop` command; it does not overwrite the last observed native state.

| From | Generation-matched input | To |
| --- | --- | --- |
| — | `launch` admitted | `created` (generation 0; no native I/O yet) |
| `created` | `start_dispatched` | `starting` and generation becomes 1 |
| `created` | `stop` admitted before dispatch | `stopped` (queued launch cancelled; no native I/O) |
| `starting` | `session.ready` | `idle` |
| `starting` | definite `start_failed` (plugin proved no native session was admitted), or fatal `session.error` | `failed` |
| `starting` | start timeout / lost reply | unchanged; launch command is `unknown` until exact readback |
| `idle` | `turn.started {turn_id}` | `busy`, `active_turn = turn_id` |
| `busy` | first pending approval/question | `waiting` |
| `waiting` | last pending request resolved | `busy` |
| `busy`, `waiting` | `turn.completed` for `active_turn` | `idle`, clear `active_turn` and pending requests for that turn |
| any live | `session.ended` or successful `agent.stop` readback | `stopped` |
| any live | fatal `session.error` | `failed` |
| any live | `session.lost` (owned processes gone, or exact re-attach failed) | `lost` |
| `stopped`, `lost`, `failed` | `resume_dispatched` | `starting`, generation increments, token rotates, active turn/pending clear |

`detach` preserves the observed state and sets `observe_only`. A notification or
command outcome with another generation never changes the row. A late
`turn.completed` whose turn id is not `active_turn` is historical input, not the end
of the current turn.

`stalled` is a flag, not a state. While `busy`/`waiting`, no **progress-bearing** event
for `stall_after` sets `stalled_since`; the next progress-bearing event clears it.
Progress includes native message/tool/plan/file/approval/question/turn events. CPU
samples, Git polling, source-health events and mail do not clear a stall.

`fn apply(agent: &mut Agent, input: &Input) -> Effects` in `eliot-core` is the only
state reducer. It is pure and table-tested, including duplicate, stale-generation and
wrong-turn inputs. The daemon persists the source event, updated row and derived
`agent.<state>` event in one transaction.

## Profile

A named launch recipe in config ([configuration](configuration.md)); defaults are in
brackets:

```text
Profile { harness, extends?, cmd?: [string],
          model?, effort?, permissions [workspace-write]: read-only | workspace-write | full | ask,
          args [[]], env [{}], env_clear [false], secret_env [[]], proxy?,
          workspace_mode [shared]: shared | worktree,
          control [self]: self | tree | project, git_identity [agent]: agent | inherit,
          system_note?: path, labels [{}], stall_after [5m], max_live? [unlimited],
          billing?, account?, setup [[]],
          scratch [["target/", "node_modules/"]],
          notify_parent [true], wake_on_mail [true] }
```

`max_live` caps live agents of a profile. Excess launches remain `created`, FIFO, and
appear as queued in `eliot ls`.

`launch --harness H` without `--profile` uses profile `H` when it exists, otherwise a
profile `{harness = H}` with defaults. A `fake` profile always exists.

Plugins map normalized permissions to native settings and report the effective value.
Requested and effective values are both displayed. Model and effort values are native
harness values; Eliot never maps between scales. An unsupported known value fails
before native I/O.

`billing` (`subscription` | `api_key` | `free` | `proxy`) and `account` identify the
budget used by a profile. Eliot reports them and never changes the route itself.

## Events

```text
Event { seq?, ts, kind, agent?, generation?, source, native_id?, cause?, data }
```

- `seq` exists only for persisted events.
- Every agent-scoped plugin/hook/transcript event carries `generation`.
- `native_id` is the stable vendor event/request/turn/item id when one exists. It is
  used for source hand-off and diagnostics, not synthesized from timestamps or text.
- `source`: `plugin:<harness>` | `process` | `git` | `fs` | `hook` | `transcript` |
  `router` | `rule:<id>` | `timer:<id>` | `goal` | `cli` | `daemon`.
- `cause`: `{command?, rule?, run?, depth}` links effects to their cause.
- `data` is kind-specific and bounded (default 64 KiB). Larger text is truncated with
  `truncated: true` and a native transcript pointer when known.
- Exactly one source owns each normalized event family for an agent at a time
  ([monitoring](monitoring.md#source-ownership)). A stale-generation event or an event
  from a non-owner source does not enter the normal bus; it is counted in source
  health and debug logs.
- Before persistence or fan-out, known credential values are replaced with `***`.
  Pattern redaction is added in L7.

### Event kinds

P = persisted, E = ephemeral. Plugins may add `x.<harness>.*`; the core displays them
and rules may match them, but they do not change core state.

| Kind | P/E | Main data |
| --- | --- | --- |
| `session.ready` | P | `native_scope`, `native_ref`, `pids`, `effective`; emitted exactly once for the generation before a successful start reply |
| `session.ended` / `session.error` / `session.lost` | P | `reason`, `exit_code?` |
| `agent.created` / `agent.starting` / `agent.idle` / `agent.busy` / `agent.waiting` / `agent.stopped` / `agent.failed` / `agent.lost` | P | `from`, `reason?`; derived by the reducer |
| `agent.stalled` / `agent.unstalled` | P | `since` |
| `turn.started` | P | `turn_id`, `input_ref?` |
| `turn.completed` | P | `turn_id`, `status: completed|interrupted|failed`, `usage?`, `final_text?` |
| `message.user` / `message.agent` | P | bounded `text`, `turn_id` |
| `message.delta` | E | `text`, `turn_id` |
| `tool.started` / `tool.completed` | P | `tool`, `summary`, `ok?`, `turn_id` |
| `file.changed` | P | `path`, `change`, `turn_id?` |
| `plan.updated` | P | `steps`, `turn_id` |
| `approval.requested` / `approval.resolved` | P | `request_id`, `turn_id`, `kind`, details, `decision?` |
| `question.asked` / `question.answered` | P | `request_id`, `turn_id`, `text`, `options?` |
| `child.spawned` | P | `child`, `native_ref`, `parent_generation` |
| `goal.updated` | P | `status: active|paused|done|blocked|cleared`, `mode: native|eliot`, `reason?` |
| `usage.updated` | E (P on turn end) | `tokens_in`, `tokens_out`, `cost_usd?`, `scope: turn|session` |
| `provider.limited` | P | `kind: rate_limit|quota|model_gone|auth`, `retry_after?` |
| `process.started` / `process.exited` | P | `pid`, `start_time`, `exe`, `role`, `exit_code?` |
| `process.stats` | E | `pid`, `cpu_pct`, `rss_bytes` |
| `git.status` | P on change | `worktree`, `branch`, `head`, `dirty`, `ahead`, `behind` |
| `git.commit` | P | `worktree`, `sha`, `author`, `subject`, `files` |
| `git.ref` | P | `remote`, `ref`, `old`, `new` |
| `fs.changed` | E | `worktree`, `paths` |
| `command.accepted` / `command.queued` / `command.dispatched` / `command.applied` / `command.rejected` / `command.failed` / `command.unknown` / `command.cancelled` / `command.resolved` | P | `command`, `verb`, `generation?`, `turn_id?`, `reason?` |
| `rule.fired` / `rule.failed` / `rule.suppressed` | P | `rule`, `run`, `reason?` |
| `action.queued` / `action.dispatched` / `action.applied` / `action.failed` / `action.unknown` / `action.resolved` | P | `run`, `index`, `kind`, `reason?` |
| `mail.sent` / `mail.presented` / `mail.read` | P | `message`, `from`, `to` |
| `var.changed` | P | `scope`, `key`, `version` |
| `claim.added` / `claim.released` / `claim.conflict` | P | `glob`, `agents` |
| `source.health` | P on change | `source`, `status: ok|slow|stale|error`, `last_ok`, `error?`, `gap?` |
| `plugin.started` / `plugin.exited` / `daemon.started` | P | `name`, `version`, `reason?` |

## Verbs

The command language is closed. A plugin declares `verbs` and `child_verbs`; the
router rejects anything else before native I/O.

| Verb | Args | Semantics | Precondition |
| --- | --- | --- | --- |
| `launch` | `profile?`, `harness?`, `prompt?`, `workspace?`, `parent?`, `name?`, `project?`, `control?`, `labels?` | Create an agent and start generation 1. | — |
| `send` | `text` | Start a new turn. While busy, queue FIFO for the **current generation**; it survives daemon/plugin restart but is failed as `stale_generation` if the agent is resumed/replaced first. | live |
| `steer` | `text` | Add input to the exact `active_turn`. | `busy`; native support |
| `inject` | `text` | Add context at the next native safe point or prepend it to the next send; bound to the current generation. | live; plugin reports mechanism |
| `interrupt` (`abort`) | — | Abort the exact active turn; session remains live. | `busy`/`waiting` |
| `stop` | `force?` | End the exact current generation. `force` is allowed only for an agent-owned process tree or a native API that isolates this session. | live, or `created` to cancel queued launch |
| `resume` | `prompt?` | Re-open a terminal agent by `native_ref` as a new generation; never resend old input. | `stopped`/`lost`/`failed`; native support |
| `approve` / `deny` | `request_id?`, `remember?` | Answer the exact pending request in this generation. | request pending |
| `answer` | `request_id?`, `text` or `choice` | Answer the exact pending question. | request pending |
| `configure` | `model?`, `effort?`, `permissions?` | Change settings; result says `now|next_tool|next_turn|next_session`. | live |
| `goal.set` | `text`, `mode?: auto|native|eliot`, `check?`, `budget?`, `cadence?` | Native goal where supported, otherwise Eliot goal. One of each may coexist. | live |
| `goal.pause` / `goal.resume` / `goal.clear` | `mode?` | Control existing goals. | goal exists |
| `cancel` | `command` | Withdraw a queued send before dispatch. | command is `queued` |
| `detach` | — | Stop controlling a service-backed/external session; keep it running and mark observe-only. | live; plugin declares it |
| `compact` / `fork` / `background` | native args | Native-only operations. | native support |

Text in `launch`, `send`, `steer` or `inject` is limited to 512 KiB UTF-8 so every
accepted command fits the plugin frame with its envelope. Longer input must be placed
in a file the agent can read and referenced by a short message.

`check` is `marker:<TEXT>` or an argv list run without a shell. `budget` is
`{max_turns?, max_cost_usd?, no_progress?}`; cost budgets require machine-readable
cost support. `steer` has no implicit fallback; `--or-send` is explicit. `cancel` and
Eliot-mode `goal.*` are daemon operations. A queued send uses settings effective at
dispatch, so a later `configure` that can apply now may intentionally precede it.

## Commands

```text
Command {
  id, target?: AgentId, selector?, generation?, turn_id?, request_id?,
  verb, args, origin, cause?, state, outcome?, created_at, updated_at
}
origin = owner | agent:<id>#<generation> | rule:<id> | timer:<id> | goal:<agent>
state  = accepted → [queued →] dispatched → applied | rejected | failed | unknown
         queued → cancelled
```

- `launch` creates the row and targets it. Every other verb needs a target/selector.
- Agent commands bind to the current generation at admission. `steer`/`interrupt`
  also bind to `active_turn`; approval/answer bind to `request_id`. The plugin rejects
  a mismatched binding before native I/O.
- Dispatch uses readiness lanes. Ready plugin calls are serialized per agent. Queued
  sends remain FIFO but do not block a control for the current turn, an answer to a
  pending request, or a configuration that can apply now. `interrupt` and `stop` may
  bypass an in-flight ordinary command only because they carry exact generation/turn
  bindings. A queued send never crosses a generation.
- Client disconnect/timeout never cancels an admitted command. Read it with
  `eliot cmd show ID`.
- `rejected`: no native I/O occurred. `failed`: the native side definitively rejected
  or failed and the plugin knows the effect did not apply. Any ambiguity is
  `unknown`.
- **No `unknown` command is retried automatically.** This includes `interrupt`,
  `stop`, launch and every input verb. On reconnect, `agent.snapshot` may settle an
  exact command as `applied` or `not_applied`; otherwise it stays `unknown`.
- A daemon restart changes `dispatched` to `unknown`; `queued` remains queued only if
  its generation is still current. A generation change fails all older accepted or
  queued agent commands with `stale_generation`; dispatched/unknown commands remain
  historical and are never reissued.
- Exact readback rules are conservative. An interrupt is applied only when the bound
  turn is known to have ended as interrupted; the same turn still active proves
  `not_applied`; another terminal outcome is not inferred. A stop is applied only
  when the bound generation is known ended; the same generation still live proves
  `not_applied`; a newer generation says nothing about the old outcome.
- Only exact readback or owner command
  `eliot cmd resolve ID --as applied|failed --reason TEXT` clears `unknown`. Time does
  not.
- `stop` being open does not change the agent state. `show`/`top` display the pending
  stop beside the last observed native state. This avoids a permanent synthetic
  `stopping` state after a rejected or lost request.
- Launch preflight resolves the profile/workspace, plugin availability, proxy health,
  tree limits and launch slots before creating the row. A definite preflight failure
  creates nothing.
- Selectors are `@children`, `@parent`, `@siblings`, `@tree:<agent>`,
  `@project[:<name>]`, `@label:<key>=<value>`, `@all`. They expand transactionally at
  submit time into child command ids `<selector id>/<agent id>`; repeating the parent
  id never duplicates them.

## Storage

Use bundled `rusqlite`: one writer task owns the write connection; a small fixed read
pool handles queries. The single `eliot.db` uses WAL, numbered SQL migrations and
`PRAGMA user_version`—no schema hashes.

Open order: set `busy_timeout`; verify `application_id = 0x454C494F` and reject a
foreign/newer database; then enable WAL and `synchronous=FULL`; enable foreign keys on
every connection. Writes use `BEGIN IMMEDIATE`. Migration 1 sets application id and
schema in one transaction. Never create over an existing non-empty file. There is no
blind retry loop around `SQLITE_BUSY`; one writer already serializes Eliot writes.
Group commit is bounded by 10 ms or 500 statements.

Nothing is reported accepted/applied before commit. On a commit failure, the daemon:

1. keeps the uncommitted result in the writer task when memory permits;
2. stops admitting new writes and exposes `store=error` through status/logging;
3. continues read-only queries from the last committed state;
4. retries only the same pending commit after an explicit health probe/backoff—never
   the external effect that produced it;
5. after recovery, persists one bounded `source.health` outage/recovery record.

It cannot claim to persist a health event through the commit that just failed.

Layer 1 schema (later layers add migrations):

```sql
CREATE TABLE agents (
  id INTEGER PRIMARY KEY, generation INTEGER NOT NULL DEFAULT 0,
  name TEXT UNIQUE, project TEXT NOT NULL,
  parent INTEGER REFERENCES agents(id), root INTEGER NOT NULL, depth INTEGER NOT NULL,
  origin TEXT NOT NULL, harness TEXT NOT NULL, profile TEXT NOT NULL,
  native_scope TEXT, native_ref TEXT, workspace TEXT, branch TEXT, base_ref TEXT,
  state TEXT NOT NULL, active_turn TEXT, stalled_since INTEGER, last_progress_at INTEGER,
  pending TEXT NOT NULL DEFAULT '[]', goal TEXT, effective TEXT,
  labels TEXT NOT NULL DEFAULT '{}', control TEXT NOT NULL DEFAULT 'self',
  observe_only INTEGER NOT NULL DEFAULT 0,
  token TEXT NOT NULL,                 -- current-generation routing handle; never printed
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX agents_parent ON agents(parent);
CREATE INDEX agents_root ON agents(root);
CREATE INDEX agents_state ON agents(state);
CREATE UNIQUE INDEX agents_native ON agents(native_scope, native_ref)
  WHERE observe_only = 0 AND state IN ('starting','idle','busy','waiting');

CREATE TABLE events (
  seq INTEGER PRIMARY KEY, ts INTEGER NOT NULL, kind TEXT NOT NULL,
  agent INTEGER, generation INTEGER, source TEXT NOT NULL,
  native_id TEXT, cause TEXT, data TEXT NOT NULL
);
CREATE INDEX events_agent ON events(agent, seq);
CREATE INDEX events_kind ON events(kind, seq);
CREATE INDEX events_native ON events(agent, generation, native_id)
  WHERE native_id IS NOT NULL;

CREATE TABLE commands (
  id TEXT PRIMARY KEY, target INTEGER, selector TEXT,
  generation INTEGER, turn_id TEXT, request_id TEXT,
  verb TEXT NOT NULL, args TEXT NOT NULL, origin TEXT NOT NULL,
  cause TEXT, state TEXT NOT NULL, outcome TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX commands_target ON commands(target, created_at);
CREATE INDEX commands_open ON commands(state)
  WHERE state IN ('accepted','queued','dispatched','unknown');
```

`pending`, `goal`, `effective`, source event and row changes are committed together.
Tokens are replaced on generation change in the same transaction as
`agent.starting`.

Retention deletes old events in small batches after
`daemon.retention.events_days` (default 30), except command/agent events still needed
for live rows or unresolved commands. No generic filesystem/native-store cleaner is
hidden in retention.
