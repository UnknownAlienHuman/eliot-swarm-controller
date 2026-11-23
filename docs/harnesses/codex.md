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
not suppress other native servers. Profiles therefore select a policy
(`mcp = "minimal"` by default, or `"inherit"` as an explicit opt-in) with an
optional `mcp_allow` server list, frozen in the profile snapshot. `inherit`
keeps the additive behavior above, except owner Swarm entries: every
enumerated entry that carries the owner Swarm identity by transport always
takes a transport-carrying disable in both policies, because a worker that
talks to the owner's daemon with the owner's identity is an authority leak
(the owner file-config entry runs plain `swarm mcp` with no
`--require-agent`). Identity is the transport shape, not the entry name
(`eliot_core::is_owner_swarm_server`: a swarm executable -- file name
`swarm`/`swarm.exe` or the controller binary path -- selecting `mcp`
without `--require-agent`, or a URL on the configured `cloud_mcp`
hostname): a renamed owner entry is still suppressed, and the exact owner
names (`eliot_swarm`, `eliot-swarm`) stay covered by synthetic
transport-carrying stubs as backstop. Swarm names can never be
allow-listed in any policy, and an allow-listed name that matches the
identity is rejected before worker spawn (`profile_error`) instead of
re-admitted: minimal fresh launches reject an allow-listed match when the
surface is frozen pre-dispatch, and every other path rejects at launch.
Entries whose argument vector the native layer does not report count as
unknown and fail closed toward suppression; an absent vector means the
entry carries no argv. `minimal` starts only Eliot's
Agent-scoped server plus the explicitly allowed servers:

- For launch, the pre-dispatch `profile.store` reader projects the worker-visible
  `mcp_servers` entries (key plus `command`/`url` transport, except allow-listed
  servers which persist by name only) and `plugins` states
  (id plus `enabled` flag) from the same `config/read` response used for verified
  store selection, using the actual prepared workspace. Store freezes this projection
  with the exact launch command ID alongside the store binding at dispatch. The worker
  consumes only that command's projection; missing or mismatched metadata refuses
  before worker spawn. No configuration cache or ambient reselection is used.
  Resume instead enumerates through a reader pinned to its retained store, verifies
  the pin, and never reuses the old launch projection. Every enumerated
  server outside the allow list keeps its own transport for its
  `-c mcp_servers.<name>.enabled=false` override; a name no `-c` path can
  address (Codex splits override paths on literal dots before decoding), a
  server with no usable transport, or a disable transport carrying a known secret
  fails the start closed instead of silently inheriting. Allow-listed servers
  never freeze or emit a transport, so their transports never trip the secret check. Plugin-backed
  builtin servers never appear in `mcp_servers`, so each suppressed bundled
  plugin (`computer-use`, `unified-computer-use`, `codex-app-tools`) gets
  its own `-c plugins.<id>.enabled=false` override when present and
  enabled; the ChatGPT-backed Apps runtime (`codex_apps`, likewise absent
  from `mcp_servers`) gets a synthetic transport-carrying disable with a
  nonexistent command alongside `enabled=false`. An explicit `mcp_allow`
  entry for a plugin-backed server or `codex_apps` keeps it enabled. The
  MCP policy pins no prompt-surface flags: `include_*`
  instruction/environment text, `agents.enabled` / `features.multi_agent`
  (subagents), `web_search` and the other `features.*` were measured (see
  below) but are deliberately not applied — `mcp = "minimal"` is a
  tool-surface policy and must not silently change worker capabilities or
  safety context.
- After `thread/start` (or `thread/resume`) and before any input, the
  installed `mcpServerStatus/list` inventory is read back scoped to the
  owned thread (`threadId`; names only, secret-checked) and reported as
  `effective.mcp_servers` with `mcp_servers_verified`. The requested `mcp_allow`
can never name a Swarm server, so the readback enforcement doubles as the
authority check: an enabled owner `eliot_swarm` entry would fail `minimal`
closed. Thread scope is
  required: the installed 0.167.1 schema reports per-thread runtime state,
  and without a loaded thread every entry's status is unavailable, so a
  pre-thread read cannot prove the isolated surface. `minimal` fails closed
  when the visible surface cannot be verified, shows a serving unexpected
  server (`connected`, or a missing/null status), shows an unexpected server
  from the enumerated config surface in any non-disabled state, or omits
  the Agent-scoped server (an empty inventory or a disabled own entry proves
  nothing); the empty thread is retained like any stopped Agent's thread
  and never receives input. `inherit` performs no inventory readback and
  reports `null` / unverified. There is no tolerance for unexpected
  servers: the synthetic `codex_apps` disable makes the Apps runtime report
  `disabled` (absent from the inventory like any disabled entry), the
  suppressed bundled plugins leave the inventory the same way, and any
  other unexpected server in any non-disabled state fails the start closed
  — including a newly added owner server that is still starting — as does
  any unexpected server that is serving (`connected`, or a missing/null
  status).

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
a user-file-config server (probe showed `enabled: false`) only when the
touched table keeps a valid transport: a bare `enabled=false` breaks the
bootstrap for project/plugin-layer or builtin names (`invalid transport in
'mcp_servers.<name>'`), while re-emitting the effective `command`/`url`
alongside `enabled=false` disables those servers cleanly (probes disabled
project-layer `codebase_memory`/`context7`/`rust_lsp` this way).
Plugin-backed builtins (`cua_repl` from the computer-use plugins) are
absent from `mcp_servers`, so a bare `enabled=false` breaks the bootstrap
the same way (`invalid transport in 'mcp_servers.cua_repl'`); disabling
their bundled plugins (`-c plugins.<id>.enabled=false`, verified live:
`cua_repl` leaves the thread-scoped inventory) suppresses them instead,
and the ChatGPT-backed Apps runtime (`codex_apps`, reported by
`mcpServerStatus/list` but by neither `config/read` nor `codex mcp get`)
is suppressed by a synthetic table carrying a nonexistent command plus
`enabled=false` (verified live: the bootstrap accepts it and the inventory
reports `disabled`; the command can never run). `mcpServerStatus/list`
exists in the app-server protocol (v2 schema: `{data: [{name,
runtimeStatus, ...}], nextCursor}`). The owner's global config is never
edited and `auth.json` is never copied.

### Minimal-context measurement (k61, 2026-11-16, debug build SHA256 EB3EDE59...)

Private daemon, identical trivial turn (`Read question.txt ... reply with
exactly 42`), model `gpt-6.1-sol`, effort low, approval never. `inherit`
(explicit): 54,276 turn input tokens; `minimal` (default): 50,148 turn input
tokens with `effective.mcp_servers` verified as exactly the session-owned
`eliot_swarm_*` entry (owner `eliot_swarm` absent). A follow-up turn under
`minimal` called `api.identity` through the `swarm` tool and reported
principal `agent`. Single samples; prompt caching varied between runs.

## Minimal-context measurement (2026-11-14)

Same trivial prompt (`Reply with exactly 42 and nothing else.`), model
`gpt-6.1-sol`, effort `low`, per-turn `input_tokens` from the
`thread/tokenUsage/updated` readback (turn expenditure = latest `total`
minus the turn-start baseline; single-response turns report `last == total`).
Single samples; prompt caching varied between runs (`input_tokens` counts
cached tokens too) and the owner surface below is one machine's, so treat
deltas as indicative, not budgets.

Owner surface at the time: 106 effective config keys, 6 `mcp_servers`
(`node_repl`, `github_oauth`, `cloudflare-api`, `eliot-research-remote`,
`eliot_swarm`, `eliot-search`), 17 enabled `plugins`, 18 installed ChatGPT
account apps (17 enabled), `features.multi_agent/memories` on, all
`include_*` instruction flags on, `project_doc_max_bytes = 32768`.

| Variant | `input_tokens` | Δ vs baseline |
| --- | --- | --- |
| (a) `inherit` via Swarm daemon | 27,060 | — baseline |
| (a′) same prompt, raw app-server (no Eliot tool) | 26,971 | −89 (Eliot's own `swarm` tool schema) |
| 6 MCP servers disabled only, raw probe | 26,840 | −131 |
| 17 plugins disabled only, raw probe (all enumerated plugins) | 25,151 | −1,820 |
| (b) `minimal` via Swarm daemon after this change | 25,046 | −2,014 (MCP servers + 3 bundled plugins + synthetic `codex_apps` disable) |
| Prompt-surface flags only, experimental (5× `include_*=false`, `web_search="disabled"`) | 24,150 | −2,821, not applied by the policy |
| Agent/feature flags only, experimental (`agents.enabled=false`, 5× `features.*=false`) | 20,808 | −6,163, not applied by the policy |
| (c) all of the above combined, experimental (35 `-c` overrides) | 15,386 | −11,585 (−43%), not applied by the policy |

The (c) rows were measured with experimental per-run `-c` overrides through
a raw app-server probe and are deliberately not applied: hiding
`include_permissions_instructions` / `include_environment_context` removes
sandbox, approval and cwd rules from the model, and
`agents.enabled=false` / `features.multi_agent=false` (plus `web_search`
and the other `features.*`) remove capabilities the owner relies on. An MCP
policy must not make those calls.

Before the plugin/Apps suppression (#471), `minimal` could not start on
this surface at all: the thread inventory always shows the `codex_apps`
account-connector aggregator (568 tools, perpetually `starting`) plus the
plugin-contributed `cua_repl` (`connected`), so verification failed closed
with `plugin_outcome_unknown` (`profile_error`: unexpected enabled
server). Asking the model for its `adobe.*` tools returns NONE with and
without the strip, so `codex_apps` contributes zero model-side tokens. The
runtime inventory also explains the 2.2 MB readback that sized the 8 MiB
frame cap: disabled servers report `tools: 0`, while every listed server
embeds its full tool catalogs, dominated by the `codex_apps` entry.

Live proof on the merged code, private daemon (debug build of the merge):
trivial prompt under `minimal` returns `42` with 25,046 input tokens and
`effective.mcp_servers = ["eliot_swarm_<own>"]` (the synthetic disable
makes `codex_apps` report `disabled`, so it is absent like any disabled
entry), `mcp_servers_verified = true`; a real small coding task under
`minimal` (`Create hello.txt containing exactly hello without a newline`,
workspace-write) succeeds with the single Agent-scoped `eliot_swarm`
server present and verified, producing exactly `hello` with no newline.

The [shared 8 MiB inbound frame cap](../spec/plugin-protocol.md#lifecycle-hooks-and-qualification)
covers the minimal-policy
`mcpServerStatus/list` readback (Eliot requests `detail: toolsAndAuthOnly`,
`limit: 100`, scoped to the owned thread): the installed 0.167.1 schema offers only `full` and
`toolsAndAuthOnly`, and both embed every listed server's full tool catalogs
with input/output schemas, so the response scales with the owner's inherited
MCP surface even though Eliot retains names only. Live-observed 2026-11-14: a
minimal-policy inventory response reached 1,990,613 bytes and ended the 1 MiB
stream; 8 MiB covers that readback plus owner variance and envelope overhead.
The inventory must stay a whole-list read: proving no unexpected enabled
server requires seeing every entry, so per-server (`serverName`) reads cannot
replace it. A frame over the cap ends the session with an explicit
`source.gap` carrying the bounded diagnosis (`frame_oversized len=... limit=...
kind=response`); responses carry no method by construction.

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
& $swarm --data-dir $dataDir goal set $agent --text "Make the changed package build." --max-rounds 5 --check cargo build --locked -p eliot-daemon --all-targets
```

The daemon rounds automatically on the next idle event. Use a check that exits 0
exactly when the goal holds and says why it failed: both its stdout and stderr
reach the next continuation. Verify with `goal show $agent`.

## Identity, transport and ownership

Native subject is `(canonical Codex store/account scope, exact thread id)`. The native
wire is line-delimited JSON; it does not require a `jsonrpc:"2.0"` field. The Eliot
plugin envelope is separate and includes Agent/generation.

One app-server can host many threads, but per-Agent processes keep environment, Eliot
token, Git identity and custody separate. Native child threads share the parent
process environment and have no independent Eliot CLI principal unless isolation is
actually supported. They are added after the basic path, not emulated in L2.

Native subagents (`[agents]` / `multi_agent` in the owner's Codex config) arrive on
the same stream, verified against installed Codex 0.167.1
(`codex app-server generate-json-schema`). A thread is a child when its
`thread/started` names the owned thread (or a confirmed child) in
`source.subAgent.thread_spawn.parent_thread_id` **or** in the new top-level
`thread.parentThreadId` (set only for subagent threads), when the
owned thread's `item/completed` for a `collabAgentToolCall` `spawnAgent` lists it in
`receiverThreadIds` (v1), or when a `subAgentActivity` item with
`kind: "started"` on the owned thread (or a confirmed child, for depth 2) names it
in `agentThreadId` (v2). A 0.167.1 child's first frames can precede both the spawn
announcement and its own `thread/started`, so any frame naming a present non-owned
thread routes to the child path and is held as unconfirmed in the bounded early
buffer; a non-owned thread's frame is never validated as owned, and overflow drops
the buffered frames with a child gap. Registration uses the shared
[`ChildParams`](../../crates/eliot-plugin/src/protocol.rs) and
[`ChildRegistration`](../../crates/eliot-core/src/native_child.rs) contract, and no
child facts publish until its
[`ChildRegistrationAck`](../../crates/eliot-core/src/native_child.rs) validates. A
registered child is tokenless and observe-only, with no independent Eliot principal and
no child control verbs. Child server requests stay unsupported and are rejected, never
forwarded to control. When the last spawn completes, the turn ends or the stream
closes, a still-unconfirmed thread is a source gap. A `subAgentActivity` item with
any other `kind` (`interacted` | `interrupted` | `completed`) is a lifecycle fact
with no matching transition: it is ignored (no gap, no fabricated terminal), and it
never confirms an unknown thread. The new
`CollabAgentTool` values (`sendMessage`, `followupTask`, `interruptAgent`,
`listAgents`) ride the existing `collabAgentToolCall` tool family; only `spawnAgent`
opens a spawn window, and `wait` items carry no receivers and never confirm.
`world_state` is a rollout-history record only, not a stream
notification, and needs no handling.

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
| interrupt | `turn/interrupt` for the captured native turn. With no captured turn, read the owned thread to confirm idle. The [shared interrupt policy](../spec/plugin-protocol.md#host-requests) owns confirmation and escalation |
| stop | for a captured busy turn, send exact-thread/turn `turn/interrupt` once and wait up to the core stop interrupt bound for native `turn/completed`; only the terminal status proves interruption. Then end this Agent's owned session/process; missing evidence retains an unknown turn. Force skips the cooperative window |
| resume | new Eliot generation/app-server plus `thread/resume` of retained thread; no old input replay |
| approve/deny/answer | reply to the exact offered native request; public details are the shared ApprovalRequest/QuestionRequest, answers are the universal answers map |
| configure | model and effort ride the per-turn `turn/start` fields (`TurnStartParams.model`/`effort`), resolved from the retained revision at send dispatch; the frozen snapshot never changes |
| fork / native Goal / child control | later optional methods verified against installed schema, not advertised until implemented |

Read effective model/provider/cwd/permissions from documented native fields. The
`thread/start`/`thread/resume` request always names the selected model, so its echoed
`model` equal to that selection sets `model_verified`; `effort_verified` needs a
requested effort echoed on the ChatGPT route. When an endpoint/billing fact is not
exposed, report it as unverified rather than inventing readback. An unexpected effective route does not authorize further input.

Controlled launch resolves the effective native store through a dedicated read-only
`profile.store` reader after workspace preparation, then commits its binding with launch
dispatch, together with the minimal-policy MCP projection from the same config response.
Read-only readers deliver stdin EOF, then require process reap and complete output
and notification drainage within their cleanup deadline; unknown cleanup remains fenced.
An owned stop does the same for the controlled app-server: it drops stdin, and
`codex app-server` exits on EOF in well under a second (flushing alone left it waiting
for the 2 s kill grace on every stop). Descendants it leaves behind get a 1 s family
grace before the generation Job is terminated; a detached `git fsmonitor--daemon`
that a Git command started never exits on its own and is ended there.
The controlled app-server pins and verifies that same store before
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
`tokenUsage.last` reports the latest model response for that exact `turnId`
(`inputTokens`, `cachedInputTokens`, `outputTokens`,
`reasoningOutputTokens`, `totalTokens`, `cacheWriteInputTokens`): usage
flows once per upstream model response inside a turn (the installed
`RawResponseCompletedNotification` is "the exact usage from one upstream
Responses API completion", keyed by turn), so one turn with tool calls sees
several updates. The protocol can also send a synthetic window-fill update
with zero input and output counters. That update contributes no response
expenditure. A prior measured context pair stays available while its window
is unchanged. Turn expenditure is the cumulative `total` of the latest
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
the retained input/output/cache counters (never a derived total),
`last.totalTokens` projects to context occupancy for a model response
against `modelContextWindow` when it fits in the window. A response without
capacity can reuse a window observed for the same turn or associated model.
The Store removes cache hits from Codex's cache-inclusive
`inputTokens` when projecting usage reads. Account-level
cost/credits (`GetAccountTokenUsage`) stay account scope. Missing per-turn
counters stay null. Idle updates merge and commit without waiting for a
terminal. Registered native child threads use independent baselines bound to
their exact thread and active turn. Their terminal event carries the resulting
delta once into the child's retained turn row.

## Recovery and later capabilities

Installed Codex discovery (0.167.1) uses `initialize`/`initialized` followed by bounded
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

On 2026-11-14, real Windows Swarm observation qualification with installed Codex
0.167.1 rejected a global metadata-startup lock: 448 gated attempts produced 90
unknowns, including 84 lock-wait timeouts and two SQLite-init failures. The ungated
baseline had no unknowns in 443 attempts; a cold home separately reproduced the
initialization failure. The gated debug build's mean cycle was 15.5 seconds versus
4.9 seconds for the release baseline, so that timing includes a build-mode difference.
The lock has been removed. The underlying SQLite/OS error remains unknown; contention
is a hypothesis, and Eliot cannot coordinate other Codex processes sharing the home.

Each metadata process may retry initialization once after a short randomized backoff
defined by [the startup retry helper](../../plugins/codex/src/native/startup_retry.rs),
within the existing read deadline, only when its owned process independently exited
unsuccessfully and complete stderr matches the state-runtime initialization signature.
The retry boundary owns spawn and initialization: only `initialize` may have been sent
to the failed process; no successful initialization, configuration read, thread/session
request or binding on that process may precede a retry. An earlier selection probe may
have completed its configuration reads, but it never creates or resumes a thread.
Native initialization can touch SQLite internals; this proof is
about absence of session/thread effects, not absence of native database writes.
Cleanup and output drain must be confirmed before retry. An initial failure with a missing
signature, incomplete diagnostics, unexpected native activity or unconfirmed exit/cleanup
retains uncertainty and does not authorize retry. After that one retry, another failed
`initialize` RPC settles definitely only with complete diagnostics, confirmed cleanup and
no native activity or session/thread effect; its diagnostic text may differ. A received
successful initialization response or failed `initialized` notification never qualifies
for retry or this exhausted-start verdict. Later configuration, store verification and
history failures are never retried by this mechanism.

An exhausted eligible retry or a pre-spawn refusal returns a bound failed attachment
result with a short safe reason, settling the attach command definitely failed rather
than unknown. Generic transport loss and uncertain effects retain unknown outcomes.
The second startup and its exact home/root and `sessionFlags` proof remain required.

The subsequent eight-worker release Swarm sample applied 400/400 warm-home attaches,
with all native subject/turn/item history reads and detaches succeeding. A private
cold home copied only authentication and configuration, so its 400 attaches to refs
from the warm store retained 400 `unknown/history_gap` outcomes and 400 applied detaches;
these are absent-subject results, not successful attachment. Neither sample surfaced a
SQLite-init failure. Successful internal retries are not exposed by the settled readback,
so these observations do not prove that the retry branch ran or identify the SQLite cause.

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
`task_started`/`turn_started`, `task_complete`/`turn_complete` and
`task_aborted`/`turn_aborted` records. Persisted usage/thread metadata
(`token_count`, `thread_settings_applied`, `thread_goal_updated`,
`thread_rolled_back`, `item_completed`) and the upstream 0.167.1 persisted dynamic-tool catalog (`dynamic_tools_updated`, rollout `policy.rs`), legacy message/tool echoes and
legacy-mode records are ignorable without clearing binding; only
lifecycle records mutate binding. An aborted turn emits an interrupted
(`reason: interrupted`) or failed terminal turn with its native reason; the abort row itself still resolves to the aborted turn (a schema-valid abort carries no `turn_id` upstream), then it
clears the active binding and releases the aborted turn id, so a following
start is not an anomaly and later items cannot attach to the aborted turn.
Only legacy message/tool echoes are otherwise suppressed; `FinalAnswer` and EOF are not terminal evidence.
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

## Ad-hoc launch

`swarm launch --harness codex [--model M] [--effort E] [--cmd PATH] --workspace W "prompt"`
is Owner-only and freezes a profile-equivalent snapshot from safe defaults
(`workspace-write`, `never`, explicit MCP `inherit`, empty `pass_env`, default scratch,
no child allow-list) plus flags. An omitted `--model` selects the native
`model/list` default entry (`isDefault`); an omitted `--effort` preserves the
native default and reports the effective readback. An unavailable requested
model fails without substitution. An omitted profile `cmd` and an omitted
ad-hoc `--cmd` use the same installed `codex` lookup. Discovery freezes the
selected executable for that launch generation; explicit `--cmd PATH` is the
Owner's one-launch override. `swarm models --harness codex` returns the live
native catalogue.
## Agent skill delivery (`eliot-swarm`)

After the owned thread is live and before any input is sent, the plugin
calls `skills/extraRoots/set` with the daemon-materialized skill root
(`<data-dir>/skills/skills`). One app-server serves one Agent generation,
so the process-level extra roots are effectively per-session; the checkout
and the global skill locations stay untouched. A failed call fails the
start. Verified against the installed 0.167.1 schema
(`codex app-server generate-json-schema`): `SkillsExtraRootsSetParams`
carries `extraRoots`, the method replaces "the process-level extra roots
used to discover standalone skills without persisting them", and `skills/list`
exposes the catalog per `cwd`. Implementation:
[`plugins/codex/src/host.rs`](../../plugins/codex/src/host.rs)
(`deliver_swarm_skill`).

## Native subagents (`native_subagents` deny)

Verified against installed Codex 0.167.1's generated `ThreadStartParams`
and `ThreadResumeParams` schemas and upstream tag `rust-v0.167.1`
(`0a94398fa60c4fac0e7659e4213a0a36e613dd46`), 2026-11-16.
When the frozen profile carries `native_subagents = "deny"` (the default
with non-empty `child_profiles`), every `thread/start` and `thread/resume`
sends dotted keys `agents.enabled=false`, `features.multi_agent=false`
and `features.multi_agent_v2=false` in its `config` object. The same three
overrides precede `app-server` as process `-c` flags, including enumeration
probes. The thread overlay applies even without an explicit effort; an
omitted effort still preserves the native default. `allow` adds no native
subagent overrides and preserves native configuration.

The feature flags alone do not enforce deny: model metadata can select a
multi-agent backend before the legacy `multi_agent` feature is consulted.
[`Config::multi_agent_version_override` and `multi_agent_version_for_model`,
core/src/config/mod.rs:1687-1713](https://github.com/openai/codex/blob/rust-v0.167.1/codex-rs/core/src/config/mod.rs#L1687-L1713)
force `Disabled` when `agents.enabled=false`, provided v2 is also off.
[`collab_tools_enabled`, core/src/tools/spec_plan.rs:735-748](https://github.com/openai/codex/blob/rust-v0.167.1/codex-rs/core/src/tools/spec_plan.rs#L735-L748)
then excludes native collaboration tools.

App-server reloads thread configuration with the process overrides retained;
[`ConfigManager::load_with_validation`, app-server/src/config_manager.rs:704-725](https://github.com/openai/codex/blob/rust-v0.167.1/codex-rs/app-server/src/config_manager.rs#L704-L725)
appends request `config` overrides after those process overrides.
The [config loader's runtime layer, config/src/loader/mod.rs:426-433](https://github.com/openai/codex/blob/rust-v0.167.1/codex-rs/config/src/loader/mod.rs#L426-L433)
is above user/project configuration, so owner `[features]` values cannot
re-enable these overridden settings. Both protocol requests expose `config`
([start:100](https://github.com/openai/codex/blob/rust-v0.167.1/codex-rs/app-server-protocol/src/protocol/v2/thread.rs#L100),
[resume:410](https://github.com/openai/codex/blob/rust-v0.167.1/codex-rs/app-server-protocol/src/protocol/v2/thread.rs#L410)).
Implementation: `plugins/codex/src/native/types.rs`
(`NativeThreadConfigOverrides::from_profile`), `host.rs` (both thread
requests), and `route.rs` (`native_subagents_argv`). No vendor code is copied.

## Shell identity and command-guard limits

Verified 2026-11-21 with installed codex-cli 0.167.1 using a private CODEX_HOME and
an environment with credentials cleared. codex execpolicy check --rules <path> <argv>
evaluates a supplied rule file, but codex app-server --help has no rule-file launch
option and the installed app-server schema has no rule-path field. Official
Rules documentation describes startup-loaded files under active config layers,
including trusted project layers; it does not establish a private per-Agent app-server
attachment. Eliot therefore does not claim a guard for Codex profiles with
approval_policy set to never, which do not emit permission requests.

The installed app-server accepts thread config overlays. Official configuration
documentation defines `shell_environment_policy.filters` for variables passed to
spawned commands. When `pass_agent_identity_to_shell` is false, Eliot excludes the
Agent identity variables and keeps a marker that prevents shell CLI calls from
using Owner authority. Its managed MCP server retains the explicitly named Agent
`env_vars`. Live shell filtering remains subject to private-daemon
qualification.

## Access levels (`--access ask|auto|full`)

Ad-hoc mapping, frozen in the snapshot and reported in effective settings:
`ask` is `approval_policy=on-request` + `workspace-write` (tool calls produce
pending requests decided with `swarm approve`/`deny`); `auto` is
`approval_policy=never` + `workspace-write`; `full` is
`approval_policy=never` + `danger-full-access`, exactly the current
full-access profiles. Implementation:
[`crates/eliot-daemon/src/adhoc.rs`](../../crates/eliot-daemon/src/adhoc.rs)
(`apply_adhoc_access`).
