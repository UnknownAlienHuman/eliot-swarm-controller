# Codex app-server

Current harness; see [L2](../roadmap.md#l2--first-useful-harness-codex-with-luna).
Use `plugins/codex`, Rust/built-in, with one plugin/app-server process per controlled
Agent generation.

On 2026-10-27, the Windows smoke with Codex 0.161.0 and actual `gpt-6-luna`
completed three native turns: generation 1 created `hello.txt` containing `hello`
and appended `world`; after an exact graceful stop, resume kept the same `native_ref`
in generation 2, which returned exactly `helloworld`. No old turns were replayed.
Live watch observed all three turn results. Final Agent stop and daemon drain succeeded.

Both generations verified provider `openai`, effort `medium` (`effort_verified=true`),
sandbox `workspaceWrite`, and approval policy `never`. Persisted events included five
messages, two tool starts, two tool completions, and three turn completions.

Three further native control exercises passed on 2026-10-27 with Codex 0.161.0,
`gpt-6-luna` and effort `medium`:

- With approval policy `on-request`, two callbacks were observed. Steer and approve
  reached `applied`; retrying the same approval mutation ID returned retained `applied`
  without a second native write. Interrupt while the second callback was pending
  ended the native turn as interrupted and cleared pending requests.
- Deny reached `applied`, the final reply was `DECLINED`, and the denied command did
  not execute.
- With `collaboration_mode = "plan"` and read-only permissions, native
  `request_user_input` opened a question. A reply containing an unoffered question ID
  was rejected; answering the offered question with `Continue` reached `applied`,
  followed by final reply `ACK`.

All three ended with the Agent stopped, daemon exited and sources closed/healthy
(`closed`/`ok`), with zero gaps, duplicate native IDs or workspace files.
File-change approval was not live exercised; file-change requests carrying `grantRoot`
remain unsupported. Full L7 qualification remains pending.
[Roadmap](../roadmap.md) owns milestone status.

In the installed 0.161.0 runtime, a stopped test thread with no turns returned
`thread not loaded` from `thread/read` and `thread/turns/list`; a retained thread with
a completed turn remained readable as `notLoaded`. Resume requires that exact native
readback. An unavailable empty subject fails without creating a replacement thread;
the managed-worktree resume exercise uses a completed turn before stopping.

## Sources

Checked against official documentation on 2026-10-27:

- [app-server transport, initialization, methods and schema generation](https://learn.chatgpt.com/docs/app-server);
- [available model names and rollout caveats](https://learn.chatgpt.com/docs/models);
- [upstream protocol definitions](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol/src).

The installed binary's generated schema controls wire names. Generate it once for the
version being integrated; update when that version changes, not before every command:

```text
codex app-server generate-json-schema --out <temporary-schema-directory>
```

Use typed messages for the implemented methods. Do not import the Codex harness/model
loop or make its unstable types part of `eliot-core`.

## First useful path

Use an existing authenticated `CODEX_HOME` and a disposable existing workspace.
No proxy, native Goal, subagent, worktree creation or transcript handoff is needed yet.

1. Spawn `codex app-server --listen stdio://` inside Agent custody and start its reader.
2. Send `initialize`, await success, then send `initialized`.
3. Read paginated `model/list`. Select the explicitly configured Luna model and an
   effort supported by that response. Apply the frozen profile effort on start and
   resume. Do not silently substitute another model/account.
4. `thread/start` without a prompt establishes the thread; commit `session.ready`.
5. Submit the linked input with `turn/start`; record the exact returned/notified turn
   id. Input application and terminal turn result are separate facts.
6. Stream text/tool/request events, answer exact native requests, and commit final text
   before the terminal turn. Expose the result through the existing CLI.

Official docs currently name Luna `gpt-6-luna`; availability depends on account/client.
Use that id only if the selected account's catalogue accepts it. This is configuration,
not a hardcoded alias in the plugin. No paid probe or alternate billing route is invented
when the requested model is unavailable.

Example Eliot config in `<data_dir>/config.toml`, using the smoke's effort and policy:

```toml
[profiles.codex-luna]
harness = "codex"
cmd = "codex"
model = "gpt-6-luna"
effort = "medium"
permissions = "workspace-write"
approval_policy = "never"
pass_env = ["CODEX_HOME"]
```

For command approval callbacks, change `approval_policy` to `"on-request"`. For native
questions, add optional `collaboration_mode = "plan"` to the profile; the live question
exercise used `permissions = "read-only"`. The mode also accepts `"default"`; omission
preserves the native default. Model and effort stay pinned, and permissions are configured
independently of the collaboration mode.

### Eliot MCP bridge

Each controlled Agent generation receives a private process-local stdio server named
eliot_swarm_<start-command-ulid>, using the active Swarm executable, that Agent's owned
Eliot data directory, and its selected workspace as the server cwd. Its only tool is
swarm, with the installed schema { method: string, params: object }. Use the advertised
Eliot method and its existing params DTO for Eliot status, control, or coordination.
Authentication is inherited by the server and must not appear in tool arguments. Use
normal Codex tools for workspace work. The per-generation config sets native
approval_mode to approve for this swarm tool only, so its calls do not request Codex
MCP approval when the profile policy is never. This leaves the profile approval policy
and workspace permissions unchanged; Eliot still authorizes every call as the exact
Agent generation.

### Worker MCP policy

The generated Agent MCP override adds one `mcp_servers.<name>` entry; it does
not suppress other native servers. Profiles therefore select an explicit
policy (`mcp = "inherit"` or `"minimal"`, default `inherit`) with an optional
`mcp_allow` server list, frozen in the profile snapshot. `inherit` keeps the
additive behavior above. `minimal` starts only Eliot's Agent-scoped server
plus the explicitly allowed servers:

- Before spawn, a short-lived metadata reader (initialize, one `config/read`
  with the worker cwd, close; no thread) enumerates the worker-visible
  `mcp_servers` keys. The reader is pinned to the frozen native-store
  binding the same way the worker launch is pinned, and the pin is verified
  before the enumeration is trusted. Every enumerated server outside the
  allow list gets its own `-c mcp_servers.<name>.enabled=false` override; a
  name no `-c` path can address (Codex splits override paths on literal dots
  before decoding) fails the start closed instead of silently inheriting.
- After initialize, the installed `mcpServerStatus/list` inventory is read
  back (names only, secret-checked) and reported as `effective.mcp_servers`
  with `mcp_servers_verified`. `minimal` fails closed when the visible
  surface cannot be verified, shows an enabled non-allowed server, or omits
  the Agent-scoped server (an empty inventory or a disabled own entry proves
  nothing); `inherit` performs no inventory readback on the readiness path
  and reports `null` / unverified.

Requested vs observed: `requested` carries `mcp_policy` and `mcp_allow`;
`effective` carries `mcp_policy`, `mcp_servers` (native-readback names, or
`null` when unobserved) and `mcp_servers_verified`. The requested set is
never echoed as observed. Servers added to the owner config between
enumeration and spawn are still suppressed by the per-server overrides only
when they were enumerated; the status/list verification fails the start when
it sees them.

Verified read-only against installed Codex 0.167.1 (`codex --help`,
`codex app-server --help`, `generate-json-schema`): `app-server` has no
`--ignore-user-config` or strict-MCP flag (`exec` has the former, the
app-server does not), `-c mcp_servers={}` merges with file config instead of
replacing it (an isolated-`CODEX_HOME` `mcp list --json` probe still showed
the file server), per-server `-c mcp_servers.<name>.enabled=false` disables
that server (probe showed `enabled: false`), and `mcpServerStatus/list`
exists in the app-server protocol (v2 schema: `{data: [{name,
runtimeStatus, ...}], nextCursor}`). The owner's global config is never
edited and `auth.json` is never copied.

Example lifecycle:

```powershell
& $swarm --data-dir $dataDir launch --profile codex-luna --workspace $workspace "Create hello.txt containing exactly hello without a newline." --wait
$agent = 'AGENT_ID_FROM_OUTPUT'
& $swarm --data-dir $dataDir result $agent
& $swarm --data-dir $dataDir send $agent "Append world to hello.txt without adding a newline." --wait
& $swarm --data-dir $dataDir stop $agent
& $swarm --data-dir $dataDir resume $agent "Read hello.txt and reply with exactly its contents." --wait
& $swarm --data-dir $dataDir stop $agent
```

Turn controls require an active turn; request replies require a pending native request.
The native exercises above used this control surface:

```powershell
& $swarm --data-dir $dataDir steer $agent "Adjust course." --generation 1 --turn TURN_ID
& $swarm --data-dir $dataDir interrupt $agent --generation 1 --turn TURN_ID
& $swarm --data-dir $dataDir approve $agent --request REQUEST_ID
& $swarm --data-dir $dataDir deny $agent --request REQUEST_ID
& $swarm --data-dir $dataDir answer $agent --answers-file answers.json --request REQUEST_ID
```

Flags map to `command.submit`: `--generation` to `expected_generation`,
`--turn` to `expected_turn_id`, `--request` to `request_id`.
A missing request is captured only for a single pending native request.
`answer --answers-file` submits the parsed bounded body, not a path, and failures
never echo file contents. `resume --wait` with a prompt follows the new linked
`<id>/send` in generation N+1, without it follows resume readiness. No old input
is replayed.

An empty pending set after interrupt/stop, or a source becoming closed, records cleanup.
Neither proves that an approval or answer was acknowledged. Request application must
be established by the retained command result for that exact request; the interrupted
second callback above was cleared without being answered.

The plugin uses `model/list` internally; a separate model-catalogue product/UI is not
required. Keep the model choice in the profile, outside core types and tests.

## Recipes

Three copy-pasteable Codex-profile recipes live in
[`config/recipes/`](../../config/recipes/). Each rule file or goal command is written for a
native Codex profile; the timer example uses the local checkout and fetch endpoint from
the example registration. CI never runs these against a fake profile.

### 1. Hourly `git.fetch` of a registered project

Copy `config/recipes/timer-git-fetch.toml` to `<data_dir>/rules/`, and register
the project in `<data_dir>/config.toml` (the file header shows an example
`[projects.app]` snippet). Verify, then fire immediately with the same action:

```powershell
& $swarm --data-dir $dataDir timer show refresh-origin-main
& $swarm --data-dir $dataDir timer run refresh-origin-main
```

### 2. Resume on an exact stop

Copy `config/recipes/event-resume-after-stop.toml` to `<data_dir>/rules/`,
replace `YOUR_STOP_COMMAND_ID` with a ULID, then stop with that id:

```powershell
& $swarm --data-dir $dataDir --id YOUR_STOP_COMMAND_ID stop $agent --generation 1
```

When that exact stop is applied, the rule resumes the agent into a new
generation. Verify with `rule show resume-after-known-stop`.

### 3. Bounded Eliot-mode Goal

```powershell
& $swarm --data-dir $dataDir goal set $agent --text keep-green --max-rounds 5 --check git --version
```

The daemon rounds automatically on the next idle event; `git --version` is an
exit-0 placeholder — replace it with a check that exits 0 when the goal holds.
Verify with `goal show $agent`.

## Identity, transport and ownership

Native subject is `(canonical Codex store/account scope, exact thread id)`. The native
wire is line-delimited JSON; it does not require a `jsonrpc:"2.0"` field. The Eliot
plugin envelope is separate and includes Agent/generation.

One app-server can host many threads, but per-Agent processes keep environment, Eliot
token, Git identity and custody separate. Native child threads share the parent
process environment and have no independent Eliot CLI principal unless isolation is
actually supported. They are added after the basic path, not emulated in L2.

Native subagents (`[agents]` / `multi_agent` in the owner's Codex config) arrive on
the same stream. A thread is a child when its `thread/started` names the owned thread
(or a confirmed child) in `source.subAgent.thread_spawn.parent_thread_id`, or when the
owned thread's `item/completed` for a `collabAgentToolCall` `spawnAgent` lists it in
`receiverThreadIds`. A child's first frames can precede both: while unconfirmed, frames
are held in a bounded early buffer; overflow drops the buffered frames and records a
child gap. Registration uses the shared
[`ChildParams`](../../crates/eliot-plugin/src/protocol.rs) and
[`ChildRegistration`](../../crates/eliot-core/src/native_child.rs) contract, and no
child facts publish until its
[`ChildRegistrationAck`](../../crates/eliot-core/src/native_child.rs) validates. A
registered child is tokenless and observe-only, with no independent Eliot principal and
no child control verbs. Child server requests stay unsupported and are rejected, never
forwarded to control. When the last spawn completes, the turn ends or the stream
closes, an unconfirmed thread is a source gap, as is any frame from a thread that is
neither owned nor confirmed outside that window.

Start owns one live stream. It publishes the implemented lifecycle families only; do not
install hooks or tail rollouts in parallel to duplicate those facts. The reader exists
before create/resume/input. A lost native write reply is readback/unknown, never a second
write. A lost Eliot event acknowledgement follows the common last-batch rule.

## Control mapping

| Eliot operation | Native mechanism and result |
| --- | --- |
| launch | `thread/start`; readiness proves a session, not prompt execution |
| send | `turn/start`; bind command to exact turn, then track completion separately |
| steer | `turn/steer` with `expectedTurnId`; stale turn rejects |
| inject | next_input via the next SEND prepend; active_turn via `turn/steer` on the exact captured turn, mechanism `codex.turn/steer`; other points unsupported, never a weaker SEND |
| interrupt | `turn/interrupt` for the captured turn; no blind retry |
| stop | end this Agent's owned session/process |
| resume | new Eliot generation/app-server plus `thread/resume` of retained thread; no old input replay |
| approve/deny/answer | reply to the exact offered native request; public details are the shared ApprovalRequest/QuestionRequest, answers are the universal answers map |
| configure | unsupported in the current slice |
| fork / native Goal / child control | later optional methods verified against installed schema, not advertised until implemented |

Read effective model/provider/cwd/permissions from documented native fields. When an
endpoint/billing fact is not exposed, report it as unverified rather than inventing
readback. An unexpected effective route does not authorize further input.

Controlled launch resolves the effective native store through a dedicated read-only
`profile.store` reader after workspace preparation, then commits its binding with launch
dispatch. The controlled app-server pins and verifies that same store before
`thread/start` or `thread/resume`. Resume reuses the retained binding; a legacy Agent
without one refuses before native control. This does not change admitted command
arguments/digests or replay input. See
[profile binding](../spec/configuration.md#frozen-profile-and-route-binding) and the
[shared binding type](../../crates/eliot-core/src/native_store.rs).

## Usage attribution

Verified read-only 2026-11-13 against installed Codex 0.167.1
(`codex app-server generate-json-schema`): `turn/completed` carries no usage
field (a Turn has id/items/status/error/timestamps only). Per-turn
expenditure arrives separately as `thread/tokenUsage/updated`
`{threadId, turnId, tokenUsage: {last, total, modelContextWindow}}`.
`tokenUsage.last` is the latest per-response delta for that exact `turnId`
(`inputTokens`, `cachedInputTokens`, `outputTokens`,
`reasoningOutputTokens`, `totalTokens`, `cacheWriteInputTokens`): usage
flows once per upstream model response inside a turn (the installed
`RawResponseCompletedNotification` is "the exact usage from one upstream
Responses API completion", keyed by turn), so one turn with tool calls sees
several updates. Turn expenditure is the cumulative `total` of the latest
update in the turn minus the turn-start baseline (`total` minus `last` of
the first update in the turn), computed per counter and mapped to
`TurnUsage` at `turn` scope, bound to the exact thread/generation/turn. Raw
`last` values are never summed, so a repeated notification holds the
difference instead of double counting; a counter present in only one
snapshot (or a backward snapshot) stays null, and a late/stale update for
another turn leaves the turn baseline alone so the terminal keeps null
usage rather than foreign counters; the session snapshot still merges after
exact thread validation. `tokenUsage.total` is a cumulative per-thread
snapshot and `modelContextWindow` is capacity: session-scoped facts, never
turn expenditure, and never mapped to `TurnUsage`. The adapter folds them
into the retained `SessionUsage` snapshot and commits it as `session.usage`
when accepted (source `codex:thread/tokenUsage/updated`), with `observed_at`
captured at acceptance: the cumulative `total` replaces
the retained input/output/cache counters (never a derived total) and
`modelContextWindow` replaces the retained capacity; account-level
cost/credits (`GetAccountTokenUsage`) stay account scope. Missing per-turn
counters stay null. Idle updates merge and commit without waiting for a
terminal.

## Recovery and later capabilities

Installed Codex 0.161.0 discovery uses `initialize`/`initialized` followed by bounded
`thread/list` with `useStateDbOnly=true`, explicit source kinds, updated-time ordering
and `archived` set only from the explicit caller selection (default `false`).
Before listing, a probe reads effective configuration and requirements for its actual
cwd, then closes. Configuration takes precedence over `CODEX_SQLITE_HOME`, then the
Codex home default. A second reader starts with the selected canonical SQLite directory
pinned in process-local argv; readback verifies its home/root and session-flags origin
before listing. Global configuration is unchanged. Unavailable or ambiguous selection,
exact SQLite requirements and higher-precedence legacy managed layers reject explicitly.
This readback does not lock external management policy against transient changes.
The verified default retains its Codex-home scope; another store gets a distinct scope.
It returns metadata without the thread preview. Cursors bind the verified store,
profile, environment and filters; state-DB-only enumeration covers only the SDK-returned
subjects (retained archives for archived selection) and does not claim complete rollout,
raw database/history-row, or capacity coverage.
The reader never calls `thread/resume` or `thread/start` to obtain visibility.
The installed stable and experimental schemas expose no passive subscription operation.
`thread/read`, `thread/turns/list` and `thread/items/list` are separate bounded historical
reads; metadata-only empty `turns` and reader-local `notLoaded` are not terminal evidence.

The stdio stream has no assumed replay cursor. If it breaks, mark a gap and use exact
thread/turn/request readback; do not republish old history as live completions. Owned
process loss ends that controlled generation after buffered output is drained.

Observe-only live reading is separate from private `agent.history` pages. It opens only
the exact rollout path returned for the attached `native_ref`, verifies its held-file
identity and `session_meta` header under the selected Codex home's `sessions` or
`archived_sessions` root. Metadata attachment and native `agent.history` pages do not
depend on the live rollout codec gate; opening the live tail remains pinned to Codex
0.161.0 and rejects other versions before reading records or establishing a live cursor.
It captures a bounded live-edge baseline. Only after private `source.state` proves
settled `observer_ready` does it commit a gap-backed `observe` to `transcript` handoff
(`rollout_tail`). Store
rechecks readiness, subject and ownership before accepting each observed batch. History
pages never publish events or settle commands.

The live cursor binds the exact Agent generation, native scope/ref and held file identity
to an exclusive byte offset. It advances only past complete newline-terminated records;
a partial tail does not advance and is not a complete/EOF page. Replacement, truncation,
header mismatch or unsupported normalization yields an explicit gap/unavailable result,
not an empty-complete page or a newly selected path. Each page requests at most one
normalized record, with bounded byte scanning and the observation page/cursor limits; it
does not scan the whole file in one call.

For the pinned Codex 0.161.0 rollout format at upstream commit
`979011409de0a60b52f179721948e65531d26144`, task/turn lifecycle uses canonical
`TaskStartedEventMsg`, `TaskComplete` and `TurnAbortedEventMsg` records. Only legacy
message/tool echoes are suppressed; `FinalAnswer` and EOF are not terminal evidence.
The outer JSONL record timestamp is retained as epoch milliseconds in `NativeEvent.ts`.
Optional lifecycle `data.started_at` and `data.completed_at` are integer epoch seconds
from the native payload, converted with checked multiplication to epoch milliseconds;
`data.duration_ms` is already milliseconds. Missing values remain absent, never filled
from row time or ingestion time. `TaskComplete.last_agent_message` supplies canonical
turn `final_text`; a present error or explicit abort is nonsuccess. Only a
`message.completed` with phase `final_answer` may fill turn `final_text`; commentary or
missing phase leaves it unknown. Observation never calls resume, fork, Goal or send to
obtain visibility. Parent idle does not prove children completed.

OpenCodex is added as a named provider route in L3, initially attached to an owner-run
service. Do not edit owner global Codex config. Keep client/admin credentials separate
and report only routing facts that native/readback or a real request demonstrates.

Add native Goals with L4 automation, exact native children with L5 hierarchy, and
other native controls only when selected. Hook lifecycle
must not duplicate the already-authoritative stream. Unknown request types receive a
typed error, never an invented default reply.

L7 exercises lost-start/input replies, stale generation/turn/request, final-output vs
exit ordering, requested/effective route, paginated recovery and child limitations.
Do not make that complete campaign a prerequisite to the first useful file edit.
