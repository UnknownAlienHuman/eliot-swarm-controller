# OpenCode

L6 adapter for the installed OpenCode 2.2.7. Select one installed HTTP/SSE API
generation from its OpenAPI document (`GET /doc` where supported); do not mix
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
satisfies the required pre-serve transfer. Verified 2026-11-14 against
`opencode serve` v2.2.7 through the isolated per-Agent store: the
`opencode-go` credential transfers, and `opencode/deepseek-v4.1-flash-free`
completes a turn. The `opencode-go/*` models are refused upstream with
`provider.auth` 403 "Workspace access denied" on every directory and store
(including the native CLI outside Eliot), and the owner default
`opencode/muse-spark-1.3-contributor-free` is a stale alias the server
answers with "Model unavailable".

## Authentication

Before `serve`, the plugin transfers only the selected canonical credential
into the isolated native store the server will use. `auth_integration` matches
by exact equality against a native integration's id OR display name; the
installed selector `openrouter` names ID `openrouter` (display name
`OpenRouter`). The canonical `auth list` must show precisely one matched
integration with `connections.len() == 1`; zero matches, several matches, or
any other connection count refuses without fallback, and never all
integrations are exported. The selected
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
and no server is started.

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

Subscribe before session create/input. Route SSE by exact session/directory, never
cwd/title/time heuristics. Missing identity is a visible gap. Session routes bind their
location from the session; requests outside a session pass the installed API's workspace
parameter. Read complete error bodies; only authoritative
rejection proves no effect. Timeout/ambiguous server response after a write is unknown.
Catalogue preflight may retry only before any session/input may have been admitted.

## Native mapping

| Eliot operation | Mechanism / settlement |
| --- | --- |
| launch | installed `POST /api/session` with the workspace directory, no input; per-Agent `serve` on loopback with a generated `OPENCODE_PASSWORD` and per-Agent `XDG_CONFIG_HOME`/`XDG_DATA_HOME`; before `serve`, the frozen nonsecret `auth_integration` selector's canonical credential transfers opaquely into the isolated store as described under [Authentication](#authentication), with no modification of owner global auth; exact session/model/directory echo required before Eliot `session.ready` |
| send | installed `POST /api/session/{id}/prompt`; the 200 inbox id is intermediate, applied needs the exact input-to-execution association on SSE (`session.inbox.enqueued`/`delivered` plus `session.execution.started`; turn id is the exact `started` event id) |
| steer | exact-turn `POST /api/session/{id}/prompt` with explicit `delivery: steer` (v2.2.7 `Session.Inbox.Delivery`); the exact-turn gate refuses a finished turn as stale without a write, applied needs the input delivered on SSE while the exact turn stays active (native joins it at a step boundary, never a new turn here); terminal-first, cancelled, unwatched, or timed-out consumption is unknown with an exact `DELETE .../inbox/{inboxID}` whose removal must then be proven by inbox-list absence plus a stream-observed cancel — anything unproven latches the session unknown so no later send/Goal dispatches behind the unresolved input (the native 204 also covers the no-op, so it proves nothing alone) |
| inject | `next_input` prepends inside the next `POST /api/session/{id}/prompt`; `turn_safe_point` claims through the authenticated `hook.event` channel (`hook=before_model`) and resolves to the vendor-native `context` hook (RPC `eliot-context` v1 `bind`/`next`/`resolve` over the existing authenticated HTTP transport) only after a verified v1 session binding; a hook never binds by copying its last-user message id without proving actual inbox delivery to the current turn, and only a confirmed `applied`/`empty` resolve emits a `hook.before_model` receipt |
| interrupt | installed `POST /api/session/{id}/interrupt` (`{"interrupted": bool}`); the exact-turn gate refuses a finished turn as stale without a write, `interrupted:true` applies and the terminal pair settles the turn |
| stop | ordinary (non-force) Stop: installed interrupt-quiesce (`POST .../interrupt`) plus `GET /api/session/{id}` readback proving the session is retained for resume (proven retained applies; proven absent or ambiguous readback is unknown), then the owned per-Agent server ends through the generation's process custody; a failed quiesce/readback still proceeds to that bounded owned-server cleanup and settles as unknown. Force Stop skips that quiesce and retention readback and ends only the owned per-Agent server without proving retention, never touching another Agent; either way stop ends the directly spawned `serve` child while the generation Job owns any descendants it spawned; ordinary stop never DELETEs the session; abort-plus-detach is **not** stop |
| detach | explicitly stop controlling/observing as defined, leaving native service/session alive |
| resume | installed exact native session reattachment: `GET /api/session/{id}` proves the retained session, then the interrupt-quiesce (`POST .../interrupt`, which aborts a running retained execution — refusing would wedge crash-recovery resume) plus the empty `inbox`/`permission`/`form` lists prove it idle (anything still pending refuses the resume); never replays the prompt |
| approve/deny/answer | installed `POST .../permission/{id}/reply` (`once`/`always`/`reject`; approve must name `once` or `always`, there is no default choice), `POST .../form/{id}/reply` (`answer` map), `DELETE .../form/{id}` for deny; the public request id is the native id, request.opened details are the shared ApprovalRequest/QuestionRequest, answers are the shared AnswerArgs converted to the field-typed native value (string, string array, number, boolean), choices stage against the offered schema (a miss rejects before any write), a lost reply is unknown and never rewritten |
| configure | resume-time only: installed `POST .../model` / `POST .../agent` setters realign the retained session with `session.get` effective readback (a refused switch fails the resume as unknown); no mid-turn configure verb |
| fork/background | installed native method and exact resulting identity/state |
| goal | explicitly selected Eliot Goal; no assumed native Goal |

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
output by exact message/part ids, record the uncovered gap, then follow live events.

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
The native `#variant` suffix is not mapped by this slice and rejects; requests retain
`variant=None` and the existing exact requested/effective model comparison.
Effective output reports native `ModelRef.variant` as `model_variant` (null when
backend omits it); it is readback-only and does not change `model_verified` (provider/id).

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
the final authority (project config loads after global config), so before
inference the plugin checks the exact provider through bounded native
`GET /api/provider/:providerID`, selects the exact API model id locally from
`GET /api/model` (the endpoint answers an object with a `data` array), and reads the backend's
own passive `GET {base_url}/models` inventory — which never generates text,
never sends `?reload=1` (that can unload running llama.cpp models), and
never touches load/switch selectors. This verification runs before
`POST /api/session` creates any native session: no session or prompt has
been dispatched yet, so every verification failure — a known mismatch
(foreign provider/package/endpoint, an absent catalogue id, divergent
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

## Worker MCP policy

This harness keeps `inherit` only: the frozen profile always records
`mcp = "inherit"`, and a non-`inherit` policy is rejected as `unsupported`
at config load. No verified native MCP isolation mechanism was established
for the installed server (the plugin owns no MCP surface of its own), so no
`minimal` mechanism is implemented.

## Ad-hoc launch

`swarm launch --harness opencode [--model PROVIDER/MODEL] --workspace W
"prompt"` is Owner-only and freezes a profile-equivalent snapshot from
conservative defaults (MCP `inherit`) plus flags. An omitted `--model` omits
the config `model` key so the configured native default applies; the effective
value is read back from the session echo. `--effort` has no mapping and is
rejected. `cmd` is discovered by following the npm shim to the native
`opencode.exe` inside the package. `swarm models --harness opencode` lists
providers from native auth; full provider/model pairs resolve at launch.
