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
cmd = "<explicit native opencode executable>"
model = "opencode/deepseek-v4.1-flash-free"
auth_integration = "opencode-go"
```

`auth_integration` is required by the plugin at start, not optional: it is the
frozen nonsecret selector naming the one native integration whose canonical
credential is prepared before `serve`. `cmd` is the explicit native executable
(no script wrapper, no PATH default). The model string is split at its first
slash only (see below).

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
Catalogue preflight may retry only before any session/input may have been admitted.

The native `x-opencode-directory` header is URI component encoded, including literal
percent signs, using the same encoder as query values. Installed OpenCode 2.2.7
decodes it once; raw UTF-8 headers turn non-ASCII directory names into mojibake and
fail readiness with location-not-found (observed in the Linux Docker image).

## Native mapping

| Eliot operation | Mechanism / settlement |
| --- | --- |
| launch | installed `POST /api/session` with the workspace directory, no input; per-Agent `serve` on loopback with a generated `OPENCODE_PASSWORD` and per-Agent `XDG_CONFIG_HOME`/`XDG_DATA_HOME`; before `serve`, the frozen nonsecret `auth_integration` selector's canonical credential transfers opaquely into the isolated store as described under [Authentication](#authentication), with no modification of owner global auth; exact session/model/directory echo required before Eliot `session.ready`; an explicit model must also prove live `GET /api/model` catalogue membership as an exact `provider/model` pair — both native fields compared independently without trimming, exactly like the installed resolver — before create (the installed `Session.create` publishes its input unchanged and resolves availability lazily at inference, so the echo alone never proves a provider/model exists), and a requested effort must prove variant membership in that same catalogue row. Readiness is the installed `GET /api/config` proving the live server honors the per-Agent roots (password plus our config document path), polled to a 60 s bound inside one 75 s start envelope shared with the transfer, spawn, subscribe, registration, and create phases, with every failure cleanup (event close plus owned-server terminate and diagnostics) clamped to the same envelope and 15 s reserved for the daemon's Job retirement and settlement, so a loaded cold start never outlives the daemon `agent.start` budget. Any failure before a native session or prompt exists is a definite failed start with a short typed reason and its processes reaped through the generation Job, except an uncertain auth import (the isolated credential store may already have been written and reconciliation failed), which stays `unknown`; `unknown` otherwise stays reserved for real uncertainty after a session or prompt may have been admitted |
| send | installed `POST /api/session/{id}/prompt`; the 200 inbox id is intermediate, applied needs the exact input-to-execution association on SSE (`session.inbox.enqueued`/`delivered` plus `session.execution.started`; turn id is the exact `started` event id) |
| steer | exact-turn `POST /api/session/{id}/prompt` with explicit `delivery: steer` (v2.2.7 `Session.Inbox.Delivery`); the exact-turn gate refuses a finished turn as stale without a write, applied needs the input delivered on SSE while the exact turn stays active (native joins it at a step boundary, never a new turn here); terminal-first, cancelled, unwatched, or timed-out consumption is unknown with an exact `DELETE .../inbox/{inboxID}` whose removal must then be proven by inbox-list absence plus a stream-observed cancel — anything unproven latches the session unknown so no later send/Goal dispatches behind the unresolved input (the native 204 also covers the no-op, so it proves nothing alone) |
| inject | `next_input` prepends inside the next `POST /api/session/{id}/prompt`; `turn_safe_point` claims through the authenticated `hook.event` channel (`hook=before_model`) and resolves to the vendor-native `context` hook (RPC `eliot-context` v1 `bind`/`next`/`resolve` over the existing authenticated HTTP transport) only after a verified v1 session binding; a hook never binds by copying its last-user message id without proving actual inbox delivery to the current turn, and only a confirmed `applied`/`empty` resolve emits a `hook.before_model` receipt; the poll paces repeat `next` offers of one still-pending hook with bounded backoff instead of hot-polling, and the first offer always forwards immediately |
| interrupt | installed `POST /api/session/{id}/interrupt` (`{"interrupted": bool}`); the exact-turn gate refuses a finished turn as stale without a write, `interrupted:true` applies and the terminal pair settles the turn |
| stop | ordinary busy Stop: one installed exact-session interrupt guarded by the captured turn, then a bounded wait for that turn's SSE terminal; interrupt acknowledgement alone is not evidence. Read-only idle-leg checks and `GET /api/session/{id}` prove retention, then the owned server ends through generation custody. Missing terminal evidence retains an unknown turn; failed idle/retention readback still proceeds to bounded owned-server cleanup and makes the stop unknown. Idle Stop retains the native quiesce preflight. Force skips the cooperative wait and retention readback. Neither stop deletes the retained native session or touches another Agent |
| detach | explicitly stop controlling/observing as defined, leaving native service/session alive |
| resume | installed exact native session reattachment: `GET /api/session/{id}` proves the retained session, then the live catalogue proves an explicit realignment target with proven no effect before any native interrupt or setter (a miss fails the resume without touching the retained turn), then the interrupt-quiesce (`POST .../interrupt`, which aborts a running retained execution — refusing would wedge crash-recovery resume) plus the empty `inbox`/`permission`/`form` lists prove it idle (anything still pending refuses the resume); never replays the prompt |
| approve/deny/answer | installed `POST .../permission/{id}/reply` (`once`/`always`/`reject`; approve must name `once` or `always`, there is no default choice), `POST .../form/{id}/reply` (`answer` map), `DELETE .../form/{id}` for deny; the public request id is the native id, request.opened details are the shared ApprovalRequest/QuestionRequest, answers are the shared AnswerArgs converted to the field-typed native value (string, string array, number, boolean), choices stage against the offered schema (a miss rejects before any write), a lost reply is unknown and never rewritten |
| configure | mid-run model and/or effort switch at configure time: the requested `provider/model` must be in the live `GET /api/model` catalogue or the command rejects before any write, and a requested effort must be one of that model's installed `variants` ids (the TUI "Variant" switch; the installed server validates nothing and echoes even bogus switches, so the catalogue gate is the honest failure); the switch runs installed `POST /api/session/{id}/model` with the documented `Model.Ref.variant` and a fresh `session.get` effective readback (`model_verified` and, when an effort was requested, `effort_verified` only from that readback), then the Router stores the overlay — a refused or unproven switch stores nothing and never claims applied. A model switch without an effort runs the server default variant. Resume-time realign proves live catalogue membership for an explicit target model (plus variant membership when an effort is carried) before any installed `POST .../model` / `POST .../agent` setter against the retained session, then verifies with `session.get` effective readback (a refused switch fails the resume as unknown; the catalogue gate already ran before the interrupt-quiesce, so an unreadable catalogue that early and a catalogue miss both fail the resume with proven no effect), targeting the inherited overlay revision when a resume carries one |
| fork/background | installed native method and exact resulting identity/state |
| goal | explicitly selected Eliot Goal; no assumed native Goal |

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

Terminal readback maps the installed per-message `tokens`
(`TokenUsage.Info`: `{input, output, reasoning, cache: {read, write}}`) on
the newest assistant message strictly between the exact user input and the
selected terminal idle message, at `message` scope with that message id. No
assistant message in that slice (or no native tokens on the newest one)
means unavailable usage: never an earlier message's usage, never a sum,
never zero. The per-message counters' delta-vs-cumulative semantics are
unverified, so the adapter retains message scope and does not aggregate
across the slice. No verified session-scoped usage field exists on this
harness (per-message `tokens` are the only installed usage signal), so the
adapter commits no `session.usage` fact; `session_usage` stays null.

## Source ownership and recovery

Controlled exact-session SSE owns supplied lifecycle. Message/session reads repair
current state, not a second live event stream. On disconnect: mark stale, reconnect,
discard stale resync work, read permissions/forms/inbox/current execution, recover current
output by exact message/part ids (terminal text reverses the native newest-first page order with each message's content array order preserved; timestamps never reorder it), record the uncovered gap, then follow live events.

A local reconnect counter may order concurrent snapshots; it is not another persisted
Agent epoch. No guaranteed exclusive native history cursor means no historical replay.
If a selected API supplies one, use its exact continuation contract. List absence,
update time or silence never proves terminal execution.

A restart of the externally owned service while input is queued or running is decided
when this harness is implemented: either proven continuation of that input on the same
store, or proven final interruption. Until one is shown, the input stays unknown and is
never replayed.

## Children, configuration and final checks

Page all native history/child lists and retain exact parent ids. Parent idle does not
finish children. Native database discovery, if needed, is read-only and not a parallel
publisher. Generate per-Agent/server provider config; never edit owner global settings.
The profile model is the exact catalog string `provider/model-id`. Split only its first
slash: `opencode/deepseek-v4.1-flash-free` selects provider `opencode` and model id
`deepseek-v4.1-flash-free`, without shortening or substituting it. This follows
[v2.2.7 `Model.Ref.parse`](https://github.com/anomalyco/opencode/blob/v2.2.7/packages/schema/src/model.ts#L27).
Empty slash segments, backslash separators, whitespace and control characters reject.
The native `#variant` suffix is not mapped by this slice and rejects; the frozen
profile `effort` (or ad-hoc `--effort`) selects the documented `Model.Ref.variant`
instead. An explicit model must prove live `GET /api/model` catalogue membership as an exact `provider/model` pair — both native fields compared independently without trimming — before the session exists (the create echo alone never proves it); a requested effort must additionally prove variant membership in that exact catalogue row. Effective output reports native `ModelRef.variant` as `model_variant` and,
mirroring that readback, as `effort` with `effort_verified` (an omitted effort
accepts the server default variant); `model_variant` alone stays readback-only and
does not change `model_verified` (provider/id), while a requested effort joins the
ready gate.

L7 verifies two-session isolation when shared mode is selected, adopted-service safety,
204-vs-execution, lost stream/input, snapshot races, pagination, exact controls and no
historical lifecycle duplication. Unsupported API semantics remain explicit.

## Adopted local backends

One documented OpenCode + adopted-backend profile points an owner-run local
OpenAI-compatible endpoint at the existing isolated per-Agent root. Managed
startup is deferred: only already-running adopted endpoints are supported,
never an owned foreground `llama-server`/Strata launcher or a detached `lms`
command under Agent custody. See [configuration](../spec/configuration.md)
for the frozen profile shape.

The frozen model must use the dedicated `eliot-local` provider segment
(`eliot-local/<api-model-id>`), which has no canonical alias. Three frozen,
explicit auth choices exist: the existing native `auth_integration` path for
canonical integrations, an allowed `api_key_env` reference for backends that
need a key, or no credentials at all (explicit no-auth mode). Local branches
never import unrelated canonical credentials: `auth_integration` is rejected
on a local profile, the required key reference must resolve to a non-empty
runtime value before `serve` starts (a missing or empty key refuses, never
silently becomes no-auth mode), the isolated store must hold no credential
for `eliot-local`, and only the `{env:NAME}` reference — or no `apiKey` key
at all — is rendered. An unresolved `{env:NAME}` would become an empty
string and select `Auth.none` server-side, so Eliot rejects it first.

The per-Agent document renders the installed native schema (2.2.7) into
the existing isolated Agent root: `providers` (plural; the legacy singular
`provider` stays migrated by the server), `package`
`@opencode/ai/providers/openai-compatible`, `settings.baseURL` (plus
`settings.apiKey` only in env-key mode), and one `models` entry carrying
`tools: true` and the frozen `limit.context/output`. The built-in defaults
(200,000 context / 32,000 output with tools and image input assumed
supported) are never relied on: limits are explicit. Title and compaction
auxiliary requests run on the same selected local model: the generated
document sets only that model. Managed policy authority is preserved: no
owner or project file is ever edited.

Effective settings are read back, not assumed. The isolated XDG file is not
the final authority (workspace-inherited layers load after the isolated global config), so before
inference the plugin checks the exact provider through bounded native
`GET /api/provider/:providerID`, selects the exact frozen provider/model pair locally from
`GET /api/model` (the endpoint answers an object with a `data` array), and reads the backend's
own passive `GET {base_url}/models` inventory — which never generates text,
never sends `?reload=1` (that can unload running llama.cpp models), and
never touches load/switch selectors. This verification runs before
`POST /api/session` creates any native session: no session or prompt has
been dispatched yet, so every verification failure — a known mismatch
(foreign provider/package/endpoint, an absent catalogue pair, divergent
limits, or an inventory without the frozen id), an unreadable readback, or
an unreachable backend with no inventory at all — settles the launch as a
definite failure with a short safe reason (failed-state start result, never
unknown), and the daemon retires the plugin and releases capacity on proven
exit instead of pinning an unknown Agent. Unknown is reserved for failures
after a session or prompt may have been admitted. Project overrides that
retarget the route are rejected, never adopted. Absent provider observations
report unavailable honestly instead of failing, and only nonsecret
observations are retained — never a resolved key or raw configuration. The
readback is a snapshot, not a guarantee about every request.

Backend presets and metadata mappings are small presets through this same
provider path, not separate Agent executors. Ollama and vLLM reuse it with
an explicit local model.

| Backend | Integration boundary |
| --- | --- |
| **Strata** | Adopt its selected engine HTTP endpoint. The server also serves unknown requested names, so validate the canonical model/aliases from metadata instead of trusting a successful response. Health separates reachable service (`status`) from loaded model (`loaded`). |
| **Unsloth Studio** | Use its inference API with an explicit model and a scoped key reference, keeping the client's native function-tool loop. Owner tool/load policies (process-forced tool policy, model auto-switch) are respected and never changed; shared Agents keep one explicit model selector. |
| **LM Studio** | Use OpenAI-compatible inference plus native model metadata. Prefer one explicitly loaded instance shared by all Agents and read that instance's `config.context_length`, not the model-level maximum. JIT inventory can list unloaded models and auto-evict can unload the previous selection; TTL/load policy stays owner-controlled. |
| **llama.cpp** | Adopt a fixed-model server first, with the selected model's tool-capable template and function-call path. Keep discovery passive (props reads with `autoload=false`; never `/models?reload=1` for inventory) and configure the effective per-slot context, not pooled context. |
| **Ollama / vLLM** | Same OpenAI-compatible provider path with an explicit local model. Ollama Responses is stateless; vLLM tool calling depends on model/parser/template. Deployment, clustering and hardware tuning stay outside this increment. |

Several Agents may share one adopted server for inference while each keeps
its own native session, history, tool execution and approvals; stop ends
only the owned per-Agent server and never unloads a model or touches
another Agent. Disconnect behavior varies by route, and unloading a model
or killing its server is not turn cancellation. Codex profiles against
these backends stay `unsupported`: readiness, catalog/context, history and
exact tool formats are resolved separately before any Codex route is
advertised as supported.

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
per-model variant id verbatim (the live catalogue proves membership at
launch). `cmd` is discovered by following the npm shim to the native
`opencode.exe` inside the package (the shim target
`<shim-dir>/node_modules/@opencode/cli/bin/opencode.exe`, then the known
package-local paths under every roaming root including the
`USERPROFILE`-derived default when `APPDATA` is stripped); `--cmd` pins one
explicit install instead. `swarm models --harness opencode` lists the native
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
Both catalogue commands intentionally inherit the reader's environment and canonical
native configuration/auth stores, without the per-Agent XDG redirect used by
`server.rs` and `auth.rs`: discovery must see the owner's authenticated providers.
They create no session and import or modify no credentials. Process custody and
private standalone execution do not imply an isolated credential store.
A launch resolves
the full provider/model pair, an omitted model leaves the configured native
default, read back from the session echo. A stored profile may also carry
`effort`, frozen the same way.
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
`tools` / `permission` control with `task` as the subagent tool. The plugin
owns an isolated per-Agent config root (`XDG_CONFIG_HOME` /
`XDG_DATA_HOME` under the instance directory; see
`plugins/opencode/src/native/server.rs`). When the frozen profile carries
`native_subagents = "deny"` (the default for profiles with a non-empty
`child_profiles`), the generated isolated `opencode.jsonc` sets
`permission.task` to `deny`, disabling the task tool for the session.
`allow` omits the key and preserves today's behavior. Implementation:
`plugins/opencode/src/native/server.rs` (`native_subagents_permission`,
`agent_config_json`, `ServerLaunch::native_subagents`) and
`plugins/opencode/src/host.rs` (`ParsedStart::native_subagents`).

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
spend share from the operation's answer. The legacy `opencode-go`
selector keeps the Go-only public-URL path (OpenCodex 2.84.0 pattern,
without copied code). Selectors without a verified read-only path report
`unsupported`. No inference probe, native auth import, login, switch or
global-config edit occurs. Failures retain supported error states and
provider/environment origin; native stderr is discarded.
