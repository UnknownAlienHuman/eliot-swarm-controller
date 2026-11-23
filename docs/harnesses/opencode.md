# OpenCode

L6 adapter for the installed OpenCode 2.2.7. Select one installed HTTP/SSE API
generation from its OpenAPI document (`GET /openapi.json` on installed 2.2.7); do not mix
generations after a 404. Source:
[anomalyco/opencode](https://github.com/anomalyco/opencode).

Waku's [service](https://github.com/egoist/waku/blob/main/crates/waku-core/src/opencode_service.rs)
and `driver/opencode.rs` are pattern-only references: subscribe before create, exact
session routing, adopted service ownership and reconnect by readback. No GPL code copying.

## Working profile

The working Eliot Swarm profile for the installed OpenCode 2.2.7 is:

```toml
[profiles.opencode-free]
harness = "opencode"
model = "opencode/deepseek-v4.1-flash-free"
auth_integration = "opencode-go"
```

`auth_integration` is required by the plugin at start, not optional: it is the
frozen nonsecret selector naming the one native integration whose canonical
credential is prepared before `serve`. An omitted `cmd` uses the same native
executable discovery as ad-hoc launch; discovery resolves the npm shim to the
installed package binary, never a script wrapper. An explicit profile `cmd`
pins a native executable. The model string is split at its first slash only
(see below).

The model provider and the transferred credential are independent: the
`opencode` provider carries its own server-side credential (it has no
canonical `auth list` entry), so any transferable canonical credential
satisfies the required pre-serve transfer. A provider-side refusal (for
example `provider.auth` 403) surfaces as the turn's native failure with its
message; the adapter never retries it on another provider or model.

## Authentication

Before `serve`, the plugin transfers only the selected canonical credential
into the isolated native store the server will use. `auth_integration` matches
by exact equality against a native integration's id OR display name; the
installed selector `openrouter` names ID `openrouter` (display name
`OpenRouter`). The canonical `auth list` must show precisely one matched
integration with `connections.len() == 1`; zero matches, several matches, or
any other connection count refuses without fallback, and never all
integrations are exported. Canonical list and the explicitly selected export
run concurrently as independent reads under the same start deadline; both
reader futures complete, including bounded cleanup attempts on failure,
before the gate permits any isolated import. There is no cross-launch
credential cache. The selected
document moves opaquely, in memory only, from canonical `auth export
<selector> --standalone` into isolated `auth import --standalone` under the
exact per-Agent `XDG_CONFIG_HOME`/`XDG_DATA_HOME` roots the server receives
(`native/auth.rs`, `native/server.rs`). The owner global auth store is never
modified: no login, switch, global flag, hand-copied `auth.json`, or edit of
owner global settings.

On explicit resume, the isolated `auth list` is read first and a correctly
present selection is reused without re-import; an ambiguous isolated state
refuses. If an import outcome cannot be proven, it settles to success only
after the original child is proven exited and an isolated-list readback proves
the selected state; otherwise it stays `unknown`, no automatic retry is made,
and no server is started. A transfer failure before any import write (refused or ambiguous canonical selection, unreadable export, or a missed deadline) is a definite failed start with a typed reason, never `unknown`.

Startup emits metadata-only `opencode.start_step` records through the existing
plugin debug channel: a static step name, wall-clock start and monotonic elapsed
microseconds, bound by the host to the Agent generation. These cover frozen
selector/environment admission, config/skill/hook files, each auth helper,
port reservation, serve spawn/readiness and session setup. Helper spans can
overlap; the enclosing transfer span measures the actual critical path.
Credentials, helper output, environment values and config contents never
enter these records. Standard logging discards them.

MCP inventory settlement polls the installed `GET /api/mcp` status with a
50 ms initial delay, doubling to a 1 s maximum for cold starts. Both reads
and sleeps share the earlier of the 20 s settlement cap and start deadline;
only a connected readback verifies MCP. A hot connection does not pay a
fixed one-second delay after its first pending observation.

## Native identity and process mode

Subject is `(canonical server/store identity, exact native session id)`. Use one server
per Agent unless the installed API demonstrates exact session event/request attribution,
isolated tool environment, isolated stop and restoration of that environment after
restart. Otherwise an owner-run shared session is observe-only.

The host-owned store root is passed at `initialize` as `store_root`; the plugin
derives the per-Agent store as `opencode-{agent_id}/data` under that root. It
equals the old `instance_dir` parent (`{data_root}/plugins`), so the on-disk
layout and per-Agent identity are unchanged across generations.

Server custody is separate from Agent custody. Adopted services are never signalled or
repaired. New/uncertain controller takeover increments Eliot generation; exact recovery
of the same live controller may preserve it.

## Transport

Subscribe before session create/input. The SSE reader bounds each raw line and each
assembled payload at 256 KiB, including the newline between successive data fields
in the payload. An oversized or malformed frame ends the stream. Route SSE by exact
session/directory, never
cwd/title/time heuristics. Missing identity is a visible gap. Session routes bind their
location from the session; requests outside a session pass the installed API's workspace
parameter. Read complete error bodies; only authoritative
rejection proves no effect. Timeout/ambiguous server response after a write is unknown.
OpenCode 2.2.7 can serve its bundled model snapshot while a fresh store fetches
the current catalogue and reloads its integrations and providers. An exact
`GET /api/model` row provides effort variants. An omitted model row produces
the advisory diagnostic defined in the [plugin protocol](../spec/plugin-protocol.md).

The native `x-opencode-directory` header is URI component encoded, including literal
percent signs, using the same encoder as query values. Installed OpenCode 2.2.7
decodes it once; raw UTF-8 headers turn non-ASCII directory names into mojibake and
fail readiness with location-not-found (observed in the Linux Docker image).

## Native mapping

| Eliot operation | Mechanism / settlement |
| --- | --- |
| launch | installed `POST /api/session` with the workspace directory, no input; per-Agent `serve` on loopback with a generated `OPENCODE_PASSWORD` and per-Agent `XDG_CONFIG_HOME`/`XDG_DATA_HOME`; before `serve`, the frozen nonsecret `auth_integration` selector's canonical credential transfers opaquely into the isolated store as described under [Authentication](#authentication), with no modification of owner global auth; exact session/model/directory echo required before Eliot `session.ready`; an exact `GET /api/model` row supplies model limits and allowed effort variants; a missing row is advisory. The installed server resolves inference availability on the turn. Readiness is the installed `GET /api/config` proving the live server honors the per-Agent roots (password plus our config document path), polled to a 60 s bound inside one 75 s start envelope shared with the transfer, spawn, subscribe, registration, and create phases, with every failure cleanup (event close plus owned-server terminate and diagnostics) clamped to the same envelope and 15 s reserved for the daemon's Job retirement and settlement, so a loaded cold start never outlives the daemon `agent.start` budget. Any failure before a native session or prompt exists is a definite failed start with a short typed reason and its processes reaped through the generation Job, except an uncertain auth import (the isolated credential store may already have been written and reconciliation failed), which stays `unknown`; `unknown` otherwise stays reserved for real uncertainty after a session or prompt may have been admitted |
| send | installed `POST /api/session/{id}/prompt`; the 200 inbox id is intermediate, applied needs the exact input-to-execution association on SSE (`session.inbox.enqueued`/`delivered` plus `session.execution.started`; turn id is the exact `started` event id) |
| steer | exact-turn `POST /api/session/{id}/prompt` with explicit `delivery: steer` (v2.2.7 `Session.Inbox.Delivery`); the exact-turn gate refuses a finished turn as stale without a write, applied needs the input delivered on SSE while the exact turn stays active (native joins it at a step boundary, never a new turn here); terminal-first, cancelled, unwatched, or timed-out consumption is unknown with an exact `DELETE .../inbox/{inboxID}` whose removal must then be proven by inbox-list absence plus a stream-observed cancel — anything unproven latches the session unknown so no later send/Goal dispatches behind the unresolved input (the native 204 also covers the no-op, so it proves nothing alone) |
| inject | `next_input` prepends inside the next `POST /api/session/{id}/prompt`; `turn_safe_point` claims through the authenticated `hook.event` channel (`hook=before_model`) and resolves to the vendor-native `context` hook (RPC `eliot-context` v1 `bind`/`next`/`resolve` over the existing authenticated HTTP transport) only after a verified v1 session binding; a hook never binds by copying its last-user message id without proving actual inbox delivery to the current turn, and only a confirmed `applied`/`empty` resolve emits a `hook.before_model` receipt; the poll paces repeat `next` offers of one still-pending hook with bounded backoff instead of hot-polling, and the first offer always forwards immediately |
| interrupt | installed `POST /api/session/{id}/interrupt` (`{"interrupted": bool}`); the exact-turn gate refuses a finished turn as stale without a write, `interrupted:true` applies and the terminal pair settles the turn |
| stop | ordinary busy Stop: one installed exact-session interrupt guarded by the captured turn, then a bounded wait for that turn's SSE terminal; interrupt acknowledgement alone is not evidence. Read-only idle-leg checks and `GET /api/session/{id}` prove retention, then the owned server ends through generation custody. Missing terminal evidence retains an unknown turn; failed idle/retention readback still proceeds to bounded owned-server cleanup and makes the stop unknown. Idle Stop retains the native quiesce preflight. Force skips the cooperative wait and retention readback. Neither stop deletes the retained native session or touches another Agent |
| detach | explicitly stop controlling/observing as defined, leaving native service/session alive |
| resume | installed exact native session reattachment: `GET /api/session/{id}` proves the retained session, then the live catalogue supplies metadata for the realignment target before any native interrupt or setter, then the interrupt-quiesce (`POST .../interrupt`, which aborts a running retained execution — refusing would wedge crash-recovery resume) plus the empty `inbox`/`permission`/`form` lists prove it idle (anything still pending refuses the resume); never replays the prompt |
| approve/deny/answer | installed `POST .../permission/{id}/reply` (`once`/`always`/`reject`; approve must name `once` or `always`, there is no default choice), `POST .../form/{id}/reply` (`answer` map), `DELETE .../form/{id}` for deny; the public request id is the native id, request.opened details are the shared ApprovalRequest/QuestionRequest, answers are the shared AnswerArgs converted to the field-typed native value (string, string array, number, boolean), choices stage against the offered schema (a miss rejects before any write), a lost reply is unknown and never rewritten |
| configure | Model and/or effort switch through `POST /api/session/{id}/model` with `Model.Ref.variant`. The catalogue supplies variants for the matching model row, and a missing row is advisory. Fresh `session.get` readback establishes `model_verified` and requested `effort_verified` before the Router commits the overlay. A model switch without effort uses the server default variant. Resume realigns the inherited or frozen settings before the same effective readback. Inference can still reject the selection on a turn. |
| compact | idle session `POST /api/session/{id}/compact` with queue delivery. Applied requires completion for the exact inbox receipt, verified through the native stream or the retained compaction message and idle readback. |
| fork/background | installed native method and exact resulting identity/state |
| goal | explicitly selected Eliot Goal; no assumed native Goal |

Native tool calls arrive as the installed 2.2.7 `Session.Event.Tool` SSE
events. `session.tool.input.started` carries the tool `name` (the only
tool event that does); the adapter remembers it by the tool part `id` for
the exact active turn. `session.tool.called` (input complete, native
execution starts) commits `tool.started`, and the durable
`session.tool.success`/`session.tool.failed` terminals commit
`tool.completed` (`status` `completed`, or `error`; a failure carries
the bounded redacted `Session.StructuredError` message when present —
the installed schema requires `error`, but its absence still commits
`status: error` without a message, never a stream loss), bound to the
exact turn and generation with the native part `id` as the item id. A
tool input string `command` on `called` (the shell tool) is added to
`tool.started` as `command`, secret-redacted and bounded (plugin
protocol). A structured nonzero shell exit retains a bounded redacted
output excerpt and truncation flag for repeated-failure diagnosis.
`session.tool.input.delta`, `input.ended` and `progress` stay unmapped
streaming detail. A terminal with no observed start is one tool-family
coverage gap, never a guessed tool row; replays of an already-committed
start or terminal never re-commit. Per-part tracking (name plus
committed start/terminal flags) retains the first 128 parts of a turn
without eviction and clears when the turn fences; facts for later parts
drop with one tool-family coverage gap per turn. These facts advance native liveness
progress exactly like the other adapters' tool events.

A contiguous `session.retry.scheduled` event for the active session and
Eliot turn provides a redacted `turn.notice` that records the native retry
decision.

Busy graceful stop first sends the exact-session native interrupt once, guarded
by the captured active turn, then waits up to the core stop interrupt bound for
that turn's `session.execution.interrupted` SSE terminal. HTTP acknowledgement
alone does not settle the turn. It then verifies retention and ends the owned
server; missing terminal evidence retains the unknown turn. Do not reissue the
interrupt during the subsequent stop cleanup. Force skips the cooperative window.

If a shared server has no isolated end operation, return `unsupported` for stop/force;
do not claim success after interrupt or kill the shared server. A per-Agent managed
server may be ended only through its owned custody.

Several inputs may join one execution. Persist all exact command associations; one
uncertain unassociated input holds the next input lane until association/drop/readback.

## Usage attribution

Terminal readback translates `TokenUsage.Info`
(`{input, output, reasoning, cache: {read, write}}`) from each distinct
assistant message strictly between the exact user input and selected idle.
OpenCode v2.2.7 records these counters from a model step's finish and
writes them to that step's assistant message. The terminal reports their
sum at `main_loop_turn` scope, with each counter available when every
included message supplies it. The Store folds cache creation into
uncached input and reasoning into output when projecting these counters.
The newest assistant message in that exact input-to-idle slice also supplies a
per-call context count when `input`, `cache.read` and `cache.write` are all present:
their checked sum is committed as `session.usage.context_used_tokens` with
`source = "opencode:assistant_message_tokens"` and no context window. Missing
counters stay unknown. This is one latest call sample, not a turn or session total;
it supports an absolute profile context ceiling, while context-ratio checks remain
unavailable without a reported window.

## Source ownership and recovery

Controlled exact-session SSE owns supplied lifecycle. Message/session reads repair
current state, not a second live event stream. On disconnect: mark stale, reconnect,
discard stale resync work, read permissions/forms/inbox/current execution, recover current
output by exact message/part ids, record the uncovered gap, then follow live events.
Terminal text comes from the final assistant message in the bound execution.
Native page position selects the message and its content array orders the parts.

A local reconnect counter may order concurrent snapshots; it is not another persisted
Agent epoch. No guaranteed exclusive native history cursor means no historical replay.
If a selected API supplies one, use its exact continuation contract. List absence,
update time or silence never proves terminal execution.

For an active turn, `agent.snapshot` verifies the exact session and reads its native
message list. Finding the bound user message without a newer native `idle` message
proves the turn is in progress. A failed read or missing bound user message leaves
the outcome inconclusive.

A restart of the externally owned service while input is queued or running is decided
when this harness is implemented: either proven continuation of that input on the same
store, or proven final interruption. Until one is shown, the input stays unknown and is
never replayed.

## Children, configuration and final checks

Page all native history/child lists and retain exact parent ids. Parent idle does not
finish children. Native database discovery, if needed, is read-only and not a parallel
publisher. Generate per-Agent/server provider config; never edit owner global settings.
The profile model is the exact native string `provider/model-id`. Split only its first
slash: `opencode/deepseek-v4.1-flash-free` selects provider `opencode` and model id
`deepseek-v4.1-flash-free`, without shortening or substituting it. This follows
[v2.2.7 `Model.Ref.parse`](https://github.com/anomalyco/opencode/blob/v2.2.7/packages/schema/src/model.ts#L27).
Empty slash segments, backslash separators, whitespace and control characters reject.
The native `#variant` suffix is not mapped by this slice and rejects; the frozen
profile `effort` (or ad-hoc `--effort`) selects the documented `Model.Ref.variant`
instead. `GET /api/model` compares both native fields independently without
trimming. A matching row supplies effort variants. The create echo verifies the
session selection, and the turn determines inference availability. Effective
output reports native `ModelRef.variant` as `model_variant` and,
mirroring that readback, as `effort` with `effort_verified` (an omitted effort
accepts the server default variant); `model_variant` alone stays readback-only and
does not change `model_verified` (provider/id), while a requested effort joins the
ready gate.

L7 verifies two-session isolation when shared mode is selected, adopted-service safety,
204-vs-execution, lost stream/input, snapshot races, pagination, exact controls and no
historical lifecycle duplication. Unsupported API semantics remain explicit.

## Typed local providers

One documented OpenCode profile points an owner-run OpenAI-compatible endpoint
at the existing isolated per-Agent root. Managed startup is deferred: only
already-running endpoints are supported, never an owned foreground server
launcher or a detached command under Agent custody. See
[configuration](../spec/configuration.md) for the frozen profile shape.

An OpenCode profile with `[profiles.X.local_provider]` selects one declared
model using `model = "<provider>/<model>"`. The table contains an `id` slug,
a syntax-validated npm `package`, an absolute HTTP(S) `base_url`, an optional
`api_key_env`, and a list of models. Each model has an `id` and optional integer
`context_limit` and `output_limit`. Model IDs may contain slash segments.
`base_url` rejects userinfo, queries, fragments, and substitutions. Swarm
rejects unknown fields at freeze and retained admission and validates the typed
provider before each isolated-config write.

Swarm generates the OpenCode `providers.<id>` entry with `settings.baseURL`,
the configured model limits, and `settings.apiKey = "{env:NAME}"` when a key
is configured. The variable must be explicitly listed in frozen `pass_env`
and cannot be reserved for the controller or adapter. Swarm checks the value
at runtime before writing the config. Local-provider profiles omit
`auth_integration`.

Local-provider sessions run under OpenCode's native project-config disable switch
(`OPENCODE_CONFIG_PROJECT_DISABLE`, verified against installed v2.2.7
`packages/cli/src/server-process.ts` and `packages/core/src/config.ts` /
`config/discovery.ts`): the serve and the model-reader processes skip the
whole project walk (ancestor and cwd `opencode.json`/`opencode.jsonc`,
`.opencode/` dirs, project `.agents`/`.claude` dirs) for the session
lifetime, including native reloads. The project layer otherwise ranks above
the isolated global config, so the switch — not a file scan — is the
boundary. Both native paths also strip `OPENCODE_CONFIG`,
`OPENCODE_CONFIG_CONTENT`, and `OPENCODE_CONFIG_DIR` from their environment.
Local-provider sessions skip the project MCP file scan and still check the
effective MCP inventory. Cloud sessions keep the workspace project layers.

The generated provider defines the models available to this frozen profile.
The live `GET /api/model` row for the exact provider/model pair contributes
context and output limits before session create. Missing limits stay unknown.
A missing row produces `catalogue_omits_model`. The session readback confirms
the selected identity, and inference reports any provider refusal.

Profile-scoped picker rows advertise only the profile's configured selection:
choosing the row launches the profile as configured. Named-preview failures
coalesce into one bounded catalogue entry naming the affected profiles, so
failing previews never invalidate the whole catalogue.

Several Agents may share one endpoint for inference while each keeps its own
native session, history, tool execution and approvals; stop ends only the owned
per-Agent server and never unloads a model or touches another Agent. Codex
profiles against these endpoints stay `unsupported`.

## Eliot MCP bridge

Each owned generation's per-Agent `opencode.jsonc` carries one
`mcp.servers.eliot-swarm` entry: a local stdio server running the active
Swarm executable with the Agent's owned Eliot data directory and
`mcp --require-agent` arguments. Its only tool is `swarm`, with the
installed schema `{ method: string, params: object }`. Use the advertised
Eliot method and its existing params DTO for Eliot status, control, or
coordination. Authentication is inherited by the server from the serve
process environment (the Agent's own identity) and must not appear in tool
arguments or in the config document: the file carries only the public
command, and `--require-agent` fails closed without the inherited
identity. Use normal OpenCode tools for workspace work.

Verified against installed OpenCode 2.2.7: `mcp add` writes
`mcp.servers.<name> = {type: "local", command: [...], environment?}`
(`environment` merges over the inherited process env, so no key is
needed); `GET /api/config` echoes the per-Agent document (proving the
server honors the isolated `XDG_CONFIG_HOME`); `GET /api/mcp`
(`mcp.list`) reports configured names with `connected` / `pending` /
`disabled` / `failed` / `needs_auth` status. A server started without the
Agent identity reports `failed` with the swarm refusal text instead of
serving as Owner. The owner's global config is never edited.

Mail wake text names this tool: an OpenCode Agent is told to use the
`eliot-swarm` MCP `swarm` tool (`mail.inbox`, then ack), not the shell
CLI.

Requested vs observed: `requested` carries `mcp_policy` (`inherit`
always); `effective` carries `mcp_servers` (installed-inventory names)
and `mcp_servers_verified`
(true only when the `eliot-swarm` entry reports `connected` and the winning effective `GET /api/config` document still carries the exact generated transport for that entry: a same-name replacement from any inherited layer refuses the start, whether that entry reports `connected` or `pending`). Only names
and the status word are retained — never the failure text. The requested
set is never echoed as observed. The applied per-Agent config document is
the qualified record; the inventory observation polls
until `connected`, a terminal refusal, or 20s — then the Worker MCP policy gate refuses the start on an unreadable inventory or an unexpected or owner-identity server, as does a serving managed entry whose winning transport differs from the generated argv.

## Worker MCP policy

Profiles default to `mcp = "minimal"`, frozen in the profile snapshot; the
owned per-Agent `serve` never sees the owner's global config (isolated
per-Agent `XDG_CONFIG_HOME`/`XDG_DATA_HOME`, verified on the installed
2.2.7 server), and the generated per-Agent config carries only Eliot's
Agent-scoped `eliot-swarm` server, so both policies serve that surface.
`inherit` is an explicit opt-in to the same behavior, minus owner Swarm
entries. A non-empty `mcp_allow` is rejected as `unsupported` at config
load because OpenCode has no selective-inclusion mapping. Workspace-inherited config (every ancestor directory's `opencode.json`/`opencode.jsonc` plus its `.opencode/` project directory, each read as a native document
(the `mcp.servers` table plus the legacy `mcp.<name>` server spelling, with
the native `mcp.timeout` exception: only its direct-legacy server shape
scans, while the number/record spellings normalize as the MCP timeout) still merges natively
into the owned serve despite the isolated XDG roots, so those layers are not
covered by the generated config: every start scans them pre-create
(pre-touch on resume) for owner-identity entries -- a swarm executable
selecting `mcp` without `--require-agent`, or a URL on the configured
`cloud_mcp` hostname -- and a match refuses the start (`profile_error`,
naming the server and layer) in every policy, whatever the entry is named; a degenerate ancestor depth fails the start closed instead of silently succeeding. Managed, system and global layers merge above those files, so the winning effective `GET /api/config` documents are scanned the same way once the session exists (pre-quiesce on resume): a differently named owner transport there refuses the start in every policy.
The settled inventory (`GET /api/mcp`) is then gated before the worker is
accepted: `minimal` refuses any serving (`connected`/`pending`) unexpected
server, so a project layer that defeats `minimal` fails the start instead
of widening the worker; `inherit` refuses reserved Swarm names, and any serving (`connected` or `pending`) `eliot-swarm` entry whose winning effective transport differs from the generated local argv. An
unreadable inventory refuses the start instead: without it neither the unexpected-server gate nor the effective-transport proof can run. The requested
policy is reported in `effective.mcp_policy`, the observed inventory in
`effective.mcp_servers`, and `mcp_servers_verified` only after the gate
passes with the Agent-scoped entry connected. No project file is ever
edited by the scan.

## Ad-hoc launch

Ad-hoc OpenCode launch (`swarm launch --harness opencode [--model
PROVIDER/MODEL] [--effort VARIANT] [--auth INTEGRATION] [--cmd PATH] --workspace W "prompt"`)
is Owner-only and freezes a profile-equivalent snapshot from conservative
defaults (explicit MCP `inherit`, since ad-hoc spans harnesses without a
minimal mapping) plus flags, with one extra rule: starting the
per-Agent server always transfers one stored auth integration into the
isolated native store, so the frozen snapshot always carries
`auth_integration`. An explicit `--auth` freezes verbatim like the profile
selector; an omitted one defaults to the model provider prefix, but only
when the native `opencode auth list` holds an entry with exactly that id.
Anything else refuses before spawning with a typed `profile_error` naming
`--auth` and listing the available integration ids — another provider's
credential is never picked silently. `--effort` freezes the installed
per-model variant id verbatim. Launch validates it against an exact
catalogue row when available. Omitted profile `cmd` uses this same discovery. The selected native
executable is frozen in the launch generation; explicit `--cmd PATH` is an
Owner-only override for that launch and a profile `cmd` is a saved pin. `cmd`
is discovered by following the npm shim to the native
`opencode.exe` inside the package (the shim target
`<shim-dir>/node_modules/@opencode/cli/bin/opencode.exe`, then the known
package-local paths under every roaming root including the
`USERPROFILE`-derived default when `APPDATA` is stripped); `--cmd` pins one
explicit install instead. On Linux the bare `opencode` binary is used instead
(`opencode` on `PATH`, `/usr/local/bin/opencode`, `~/.opencode/bin/opencode`,
`/opt/opencode/.opencode/bin/opencode`); the npm `.cmd` shim layout does not
exist there and is never consulted. Extensionless Unix candidates count as
native only after an ELF or Mach-O header check; scripts never qualify. `swarm models --harness opencode` lists the native
`opencode models --standalone` catalogue (`provider/model` ids verbatim) with each
model's installed `Model.Variant` ids overlaid as `efforts` from the
read-only `opencode api model.list --standalone` operation (`model.list`, the same
`GET /api/model` inventory the per-session path reads). Both use the installed
2.2.7 private-server flag (CLI help checked 2026-11-17), so their server descendants
inherit the daemon-owned kill-on-close reader Job without breakaway. Each command
runs in a separate reader lifetime: the host reaps the plugin, drains its reader
and waits for its Job family to empty (terminating a remaining server tail within
the bounded close budget) before starting the next command or returning a catalogue.
Unconfirmed family closure returns `unknown`, including for the optional variants
read; a failed variants read degrades to ids only after confirmed cleanup.
With a named profile, both reads use the same generated provider configuration,
target environment and selected auth as launch. Pass `--workspace` to read in
the intended launch directory. These results are previews, since the workspace
and live provider state can differ. An unscoped read uses the canonical native
configuration and is also a preview. Each reader's isolated credential store
lives under its private host-owned instance directory. The host verifies removal
after process-family closure on success, timeout and cancellation. Unconfirmed
removal returns `unknown`.

`swarm models --harness opencode --agent <id>` reads the owned generation's
`GET /api/model` through `agent.models`. It offers the model and
variant IDs the server reported. Missing or unready sessions and failed
or preempted reads return errors. The desktop fetches this catalogue whenever
the Agent picker opens. This read performs no inference.

Omitting the model leaves selection to OpenCode. The launch reports
`model_source: native_default` and verifies the selected identity only when
the session echoes one. Otherwise `native_default_model` reports the location
default from `GET /api/model/default` when available. This optional preview
runs before create when time remains ahead of the create request's RPC budget.
The native inference step supplies the model that actually ran. Catalogue
membership and a verified session selection do not establish provider inference
access. Failed turns retain the native
error message and the effective model where known. The chat's **Pick another
model** action opens the Agent's live picker without retrying the failed input.

## Agent skill delivery (`eliot-swarm`)

At spawn, the plugin copies the daemon-materialized canonical skill files
into the isolated per-Agent global skills directory
(`<instance>/config/opencode/skills/eliot-swarm/`, under the redirected
`XDG_CONFIG_HOME`), rewritten verbatim on every start like the
`eliot-context.ts` plugin file. The per-Agent server then loads
`eliot-swarm` natively with no project or user file touched. A missing
canonical copy fails the start. Implementation:
[`plugins/opencode/src/native/server.rs`](../../plugins/opencode/src/native/server.rs)
(`OwnedServer::spawn`).

## Minimal-context measurement (k61, 2026-11-16)

Private daemon, identical trivial turn, `opencode/deepseek-v4.1-flash-free`,
installed 2.2.7. Both `inherit` (explicit) and `minimal` (default) report
exactly `eliot-swarm`, verified, confirming the owned `serve` never sees the
owner global config (no owner MCP surface reaches the worker in either
policy). Turn usage: message input 129 + cache-read 8,320 + output 3 under
`inherit`; 122 + 8,320 + 3 under `minimal`. A follow-up turn under `minimal`
called `api.identity` through the `swarm` tool and reported principal
`agent`. `opencode --help` and `opencode serve --help` expose no
tool-deferral surface, so deferred loading is unsupported.

## Native subagents (`native_subagents` deny)

Per-session mapping, verified on the installed OpenCode v2.2.7 server
(`opencode --version` reports v2.2.7): the agent docs allow per-agent
`tools` / `permission` control, and the installed tool check matches rule
actions with wildcard patterns and evaluates session rules merged after
every agent/project rule (last-match-wins). The authoritative enforcement
is the verified per-session `subagent` wildcard deny stored as the last rule
of the entire session ruleset — any later rule, including a `*` allow, can
beat it — and proven by readback of that exact session before readiness; the
isolated global `permission.task` deny is only a secondary layer (a project
`subagent` allow beats it). The plugin owns an isolated per-Agent config root
(`XDG_CONFIG_HOME` / `XDG_DATA_HOME` under the instance directory; see
`plugins/opencode/src/native/server.rs`). When the frozen profile carries
`native_subagents = "deny"` (the default for profiles with a non-empty
`child_profiles`), the launch stores that final session deny (preserving
existing session rules) and keeps the generated isolated `opencode.jsonc`
`permission.task: deny` as the second layer. `allow` sends no session ruleset
and omits the key, preserving today's behavior. Implementation:
`plugins/opencode/src/native/server.rs` (`native_subagents_permission`,
`agent_config_json`, `ServerLaunch::native_subagents`) and
`plugins/opencode/src/host.rs` (`ParsedStart::native_subagents`,
`session_subagent_deny_verified`, `with_subagent_deny`).

## Access levels (`--access ask|auto`)

Ad-hoc `ask` and `auto` share the native permission-reply surface: permission
requests stay pending for the Owner (`swarm approve`/`deny`). `full` has no
verified full-access mapping and refuses with `unsupported` instead of silently
widening. The level freezes as `access` in the snapshot. Implementation:
[`crates/eliot-daemon/src/adhoc.rs`](../../crates/eliot-daemon/src/adhoc.rs)
(`apply_adhoc_access`).


## Read-only provider quota

`plugins/opencode/src/quota.rs` shells the installed
`api experimental.usage.limits --standalone` operation for the Console
sign-in path (pinned v2.2.7 `packages/protocol/src/groups/usage.ts`,
`packages/core/src/usage-limits.ts`, `packages/cli/src/commands/commands.ts`;
quota custody has no running serve, so `--standalone` runs the operation
against a private server). The native operation reads the active
`opencode` integration connection and resolves it (refreshing a stale
credential once the Console plugin has registered its OAuth refresh)
before deriving the workspace binding, so a stored export read directly
could fail where the native call succeeds. A one-shot cold boot can serve
the operation before that registration lands; that failure surfaces as
`quota_unavailable` (unknown), never as ready windows, and only an
explicit null answer (not signed in) reports `unsupported`. The adapter
projects Go rolling/weekly/monthly windows and one window per budget
spend share from the operation's answer. The pinned
[UsageLimits budget schema](https://github.com/anomalyco/opencode/blob/fedc6f2/packages/schema/src/usage-limits.ts)
also supplies member or pooled scope, all-model or exact model coverage,
and per-model spend when applicable. Routing uses this scope on the
verified Console route and keeps the returned Go allowance separate.
The legacy `opencode-go`
selector keeps the Go-only public-URL path (OpenCodex 2.84.0 pattern,
without copied code). Selectors without a verified read-only path report
`unsupported`. No inference probe, native auth import, login, switch or
global-config edit occurs. Failures retain supported error states and
provider/environment origin; native stderr is discarded.
