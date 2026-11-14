# Client API and CLI

Implement only the current [roadmap](../roadmap.md) surface. The first client supports
core Agent commands/events; coordination, automation and service methods are added with
their modules. Do not advertise unimplemented commands.

## Transport and principal identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows uses a current-user-only named pipe; Unix a
mode-0600 socket under the private data root. One daemon owns the data-directory lock.
`daemon.json` records boot id, PID/birth identity, endpoint and diagnostic version.

Every call has an explicit principal. Owner CLI outside a managed Agent/rule environment
reads the private owner token automatically. Agents send id/generation/token; managed
scripts use scoped run tokens; plugins have instance credentials. Missing/incomplete/
revoked credentials reject, never become owner. No manual `--owner` step is required.
This prevents accidental attribution errors; it is not isolation from hostile processes
of the same OS user. Reads are scoped as well as writes.

A running script has a distinct `Run` principal, bound to exactly one run command and
its saved originating principal/scope. The daemon creates a random 256-bit token for that
command and keeps only its SHA-256 hash in an in-memory registry with the command id and
saved scope. It is never persisted in command/event data or logs. The child receives
`ELIOT_RUN_COMMAND_ID` and `ELIOT_RUN_TOKEN`; its RPC `_auth` must contain exactly
`kind:"run"`, that `run_command_id` and the matching `credential`. A Run caller may call
only `command.get` for that exact command id while the token is registered, and may make
that its first RPC. That read returns the standard Core `Command` DTO for that id, as the
ordinary API/CLI `command.get` path does, including its usual secret-free semantic args.
This preserves the existing typed `swarm cmd show` response validation. The Run token and
secret environment values are never stored in command data. It cannot
submit commands or read its parent, siblings or another command. This is a scoped variant of the existing
auth envelope and Router, not a separate security platform.

If any Run identity variable is present, the complete Run pair is required. Partial,
mismatched, unknown or conflicting Run/Agent/Owner authentication fields fail closed;
they never retry as Owner. The token hash is revoked at direct script exit or cleanup and
is lost on daemon restart. Validated stdout actions are admitted by the controller under
the saved principal/scope after the process exits; they do not use the Run caller's RPC
authority.

Frames are at most 4 MiB, semantic fields smaller. Multiplex requests so one wait does
not block unrelated calls. Bound all lists and histories; never hold a read transaction
open while waiting for a client or native process. Each connection carries one request
and its response; the daemon bounds that write with `RESPONSE_WRITE_TIMEOUT`
([daemon.rs](../../crates/eliot-daemon/src/daemon.rs)), so a peer that stops reading
cannot hold its connection task past the deadline. An undelivered mutation result stays
readable by exact retry; the daemon never replays the effect to deliver it.

Background `daemon` startup reports `starting`, then waits up to 30 s for
`daemon.status` readiness and reports `ready` with the resolved data
directory and boot id, or the logged `daemon.failed` code with its bounded
sanitized diagnostics.
Windows starts that independent process without inheriting caller handles, so captured
CLI stdout/stderr reach EOF while the daemon remains alive. Its own private IPC and
owned plugin/workspace custody establish subsequent authority.

### MCP stdio server

`swarm mcp` is one local stdio server and never starts the daemon. It exposes exactly one
tool, `swarm`, with required `method` (non-empty string) and `params` (object), and
forwards calls through the existing authenticated local client. Ordinary mode uses
[`eliot_daemon::client_call`](../../crates/eliot-daemon/src/daemon.rs); `--require-agent`
uses [`eliot_daemon::client_call_with_agent_identity`](../../crates/eliot-daemon/src/daemon.rs).
Both reuse the daemon methods and shared Core DTOs. Discovery is available through
the same tool: start with `api.identity` and `{}`, then `api.methods` with optional
`query`, `after` and `limit` (default 20, maximum 50), and `api.help` with a canonical
`method`, for example `mail.send` or `var.set`. The MCP tool description advertises
these entry points so a fresh caller needs no private prompt or repository files.
Only `--require-agent` stdio includes standard `initialize.instructions`, directing the
client to the `swarm` tool, current `method`/`params` DTOs, and inherited Agent authentication.

Clients configure the stdio server using their native project-scoped configuration, such as
an Antigravity workspace plugin (`.agents/plugins/eliot-swarm/plugin.json` + `mcp_config.json`).
Principal resolution stays in the daemon client identity boundary and follows
[Transport and principal identity](#transport-and-principal-identity). Partial or conflicting
inherited Agent/Run/Owner identity fails closed; the server never downgrades to Owner.
MCP arguments cannot supply or override authentication. Credentials are never included
in tool arguments, results, diagnostics or logs.

Controlled Agent stdio clients may use `swarm mcp --require-agent`. This stdio-only guard
requires a complete inherited Agent ID/generation/token before the MCP server initializes
and before any Owner token lookup. Missing, partial, conflicting, Owner, and Run contexts
fail with `not_allowed`. Each tool call still uses the existing local client and Router path,
which validates the Agent credential and current generation. `--require-agent --http` is a
usage error; ordinary Owner MCP stdio and StreamableHTTP behavior are unchanged.

A mutation call may omit `params.id`: the MCP server mints one fresh ULID for that
call, forwards it, and returns it as top-level `id` in the tool result, so the caller
retains it for exact retry. An explicit id is forwarded untouched and still validated
by the daemon. A retry must carry the same id, principal, method and semantic payload;
a different binding is `id_conflict`, and an expired id is `id_expired`. Unknown outcomes
keep the existing exact readback/resolution contract. The server never automatically
retries uncertain calls and never treats cancellation as rollback. Id admission errors
(`invalid` shape, `id_expired`, future-skew `invalid`) name `params.id` with the ULID
format and the retry window, matching the DTO field errors.

The stdio transport MUST bound each frame to [`MAX_RPC_FRAME_BYTES`](../../crates/eliot-daemon/src/daemon.rs)
(4 MiB) using [`BoundedLineWriter`](../../crates/eliot/src/mcp/bounded_io.rs). Forwarded RPC data and list/history
results retain the existing API frame, pagination and encoded-response bounds. `--json`
is invalid with this command: stdout is MCP protocol only; diagnostics go to stderr.

### MCP StreamableHTTP server

`swarm mcp --http --port 0 [--public-url URL]` serves the same single `swarm` tool
and the same `method`/`params` argument contract over StreamableHTTP on loopback
`127.0.0.1` at `/mcp` ([http.rs](../../crates/eliot/src/mcp/http.rs)). It reuses the
shared handler, `eliot_daemon::client_call` path and existing methods and DTOs, and adds
the same bounded daemon discovery methods without another authentication framework.
POST carries JSON-RPC, GET is the
SSE stream and DELETE closes a session; request bodies are capped at
`MAX_RPC_FRAME_BYTES`. `--json` remains invalid; stdout carries exactly one
`McpHttpReady` frame after the local bind and nothing else. The frame is not MCP
JSON-RPC: it signals managed-child readiness (local listener bound), never public
tunnel or auth readiness. See [cloud MCP runtime](cloud-mcp.md) for the tunnel,
Access and child-lifecycle rules.

The deployment is an owner-only Access app. Every origin POST/GET/DELETE validates
the signed `Cf-Access-Jwt-Assertion` before MCP initialization or SDK dispatch.
Managed OAuth's opaque client access token is not that assertion. The verifier uses
the configured team issuer, fixed team signing-key endpoint, application AUD, RS256
signature, expiry/not-before, application-token type and nonempty subject. Missing
or invalid authentication is refused; key lookup failure fails closed. Manual HTTP
requires the same `[cloud_mcp]` Access configuration; stdio/local IPC are unchanged.
Host/Origin checks and cloudflared's own JWT validation remain additional checks.

Credential headers are stripped before request parts enter SDK extensions. The
verified identity binds each stateful SDK session; a different subject cannot use
its POST/GET/DELETE. Verified same-identity refresh monotonically extends the
session lease; an older concurrent assertion cannot shorten it. The existing
notification task retains its cursor, pauses polls while its lease is expired,
and uses the renewed expiry for subsequent authenticated reads. Its authorization
has a monotonic deadline and sticky expiry; wall-clock rollback cannot resume
polling without verified renewal. Expired sessions are reaped in bounded batches
before a new session acquires capacity; existing-session renewal precedes reaping.
Reservations also follow the actual SDK handler lifetime: idle/closed SDK sessions
release auth capacity independently of JWT expiry. Authentication, response cleanup
and bounded reaping reconcile absent SDK sessions, including SessionNotFound.
Each GET stream ends at the immutable monotonic credential deadline captured during
that request's verified authentication, even if the handler is delayed or the wall
clock changes. The renewable session lease cannot extend that GET. Stateless
requests carry fresh server-generated connection/call correlation without inventing
a session. `clientInfo` is untrusted and unused. No local Agent/PID or generation
is created for a remote caller.

The private owner credential authenticates the bridge's typed
[`RemoteCallContext`](../../crates/eliot-core/src/remote_call.rs) to Router. Router
revalidates its configured issuer/AUD, expiry and RPC call-id binding, applies the
existing Owner authority, and echoes the context with the RPC result; HTTP tool
results expose it as `structuredContent.remote_call`. This is provenance for the
existing owner-only app, not additional roles or per-user permissions. Standard
`command.submit` admission persists the context in the existing command cause and
every associated new admission event (including `agent.created`)
before commit and publication. Direct, live-rule, catch-up and script admission use
the same event-cause annotator; derived events inherit their verified child command,
rule, depth and remote cause. Retried admissions retain their original cause. Reads
do not create an audit event, and other RPC results carry call context without
claiming command-cause attribution. JWTs, OAuth tokens, cookies and owner credentials
never enter that context or semantic data.

Session handling is native rmcp negotiation with no override: pre-2026 clients get
stateful in-memory SDK sessions (`LocalSessionManager`); clients negotiating
2026-07-28 or later are always served statelessly per SEP-2567, with no session ID
and no standalone SSE/DELETE session. Each initialized stateful session advertises
`logging` and starts exactly one bounded owner-event delivery task
([mcp.rs](../../crates/eliot/src/mcp.rs) `spawn_owner_notify_task`). The task
long-polls `events.subscribe` from the current committed tip (limit 100, about 1 s
wait), so history is never replayed, and sends only normalized
`x.user.notify` and `x.user.agent.attention` payloads as `notifications/message`.
Unsolicited stream delivery exists only for supported persistent-session clients:
delivery is best-effort and unacknowledged (a send failure, closed transport or
daemon error ends the task without retry), and nothing here promises to wake a
stateless cloud LLM. `logging/setLevel` sets the session minimum severity
(default `info`); only `info`/`warning` owner notices are emitted, so `warning`
narrows the stream to attention incidents. Stdio neither advertises nor
implements it.

## Mutation ids

External writes carry a ULID generated/printed before send, except that MCP callers
may omit `params.id` and the MCP server mints and returns one per the
[MCP stdio server](#mcp-stdio-server) contract. Bind the id to principal,
method and canonical semantic payload. Exclude JSON-RPC id, output format, paging and
wait timeout. Same binding returns the retained result; different binding is
`id_conflict`; absent expired id is `id_expired`. A ULID more than the allowed
clock skew ahead of the daemon clock is `invalid`. Retry a lost client reply with the
same id, never a new native effect.

The CLI validates supplied ULIDs and sends them in uppercase. A direct RPC ID is used
literally by Router and Store as the mutation key; retry that same spelling.

Before printing either mutation reply or using it for exit/wait decisions, the CLI
validates the exact `command_id`, a nonempty response Agent, any explicit target, and a
nonnegative response generation without requiring universal equality to
`expected_generation`. Launch/resume with nonblank input requires the exact
`<id>/send` link. Shutdown validates the retained mode and reply shape; a mismatch is
`unknown` (exit 4), and even `applied` does not prove the daemon exited.

After a request is written, local response read/integrity failures are `unknown`
(mutation exit 4), never proof of non-execution. Pre-send validation and a valid,
matching daemon domain rejection retain their own codes; no automatic retry occurs.

Commands store the binding directly; other writes use:

```sql
CREATE TABLE writes (
  id TEXT PRIMARY KEY, principal TEXT NOT NULL, method TEXT NOT NULL,
  payload_digest BLOB NOT NULL, result TEXT NOT NULL, created_at INTEGER NOT NULL
);
```

The writer checks both command/write namespaces. Later linked inputs, selectors and
rule actions have bounded derived ids. No general receipt/floor framework.

`goal.round` reserves its id in this same write namespace with the durable round
marker before running its check. Its original response or sanitized error is
retained; exact retries resolve it before current Goal state, without repeating
external work. A live duplicate is `busy`; an abandoned holder reads `unknown`.
See [Goal loop](automation.md#goal-loop) and the canonical
[Goal request/response DTOs](../../crates/eliot-core/src/api/goals.rs). The round
response binds the original mutation id separately from the distinct continuation
command id frozen at claim. Stored Goal responses/round records use Core
[`MAX_FRAME_BYTES`](../../crates/eliot-core/src/limits.rs), not the event budget.

## Methods

| Method | Purpose |
| --- | --- |
| `api.methods/help/identity` | bounded authenticated method discovery, DTO schemas/examples, and credential-free caller scope |
| `daemon.status/shutdown` | owner-only current state/admission and exact drain/force |
| `agent.list/get/result` | scoped, paginated Agent state and exact results |
| `agent.processes/stats` | scoped process records and ephemeral OS samples |
| `agent.git/edits` | current Git snapshot and paginated native-attributed edits |
| `command.submit/get` | implemented immutable admission and command readback |
| `turn.get` | implemented running/completed/unknown turn read |
| `swarm.wait` | bounded wait over a frozen child command/turn set with all/any, timeout and honest per-entry outcomes |
| `swarm.digest` | durable digest over a frozen child set delivering one coalesced parent mailbox result with the standard mail wake |
| `command.resolve`, `turn.resolve` | constrained owner resolution of unknown outcomes; owner-only |
| `command.children` | implemented scoped, paged child readback for a retained expansion |
| `project.list/get` | owner-only read of registered project authority |
| `rule.get/catch_up` | owner-only rule read and finite committed-history catch-up |
| `timer.get/run` | owner-only timer read and idempotent manual firing |
| `goal.set/get/pause/clear/resume/round` | owner-only Eliot-mode Goal with explicit and automatic idle rounds plus explicit cadence slot rounds; automatic driving needs `max_turns` or `max_rounds`, native mode and cost limits are `unsupported` |
| `provider.holds/clear_hold` | owner-only provider hold readback and constrained correction |
| `events.bounds/read/subscribe` | journal bounds, persisted replay and bounded live delivery |
| `agent.discover` | owner-only native metadata discovery, without creating Agents |
| `agent.history` | owner-only bounded history of an exact observed subject |
| `mail.send/inbox/read/ack`, `var.set/get` | addressed queued mail with separate acknowledgement, shared variables with CAS |
| `who`, `claim.set/list/release` | current-worker file areas plus advisory claims with coalesced conflict notices |
| `rule.*` except `rule.get/catch_up` | later automation; effects remain commands |

Canonical public wire types are re-exported by
[`eliot_core::api`](../../crates/eliot-core/src/api.rs). These DTOs and their validators
define field names, JSON shapes and grammar; this spec owns behavior and authority. Principal
authentication precedes method parameter validation.

### Authenticated method discovery

`api.methods` lists only currently registered methods callable by the authenticated
principal. `query` is a case-sensitive substring of the canonical name or summary;
`after` is the last returned canonical name for the same selection. Results are sorted
by canonical name and contain at most 50 summaries; `next` is null at the end. Owner-only
methods are absent for Agents. Run credentials retain their existing `command.get`-only
restriction and cannot discover methods. Discovery never changes authorization checks.

`api.help` returns one visible method's request and response JSON Schemas, generated by
`schemars` from the shared Rust DTOs using deserialize/serialize settings respectively.
Custom field constraints and byte bounds are documented beside the owning DTO fields
(byte limits use `x-maxBytes` / `x-maxEncodedBytes`; `x-nonBlank` means nonempty after
Rust Unicode whitespace trimming). Router-local descriptors supply only
the summary, example, mutation rules, scope/capability limits and CLI aliases. CLI aliases
are not RPC names. Every registered Router method has a descriptor. Help is capped at
256 KiB; it never includes credentials or another principal's identity. Examples naming
existing resources require actual retained IDs/profile/source/view; new writes require
a fresh caller-retained mutation ULID rather than reusing the illustrative example ID,
or omit `id` on MCP so the server mints one.

`api.identity` returns the authenticated principal kind, Agent id/generation when present,
and its allowed scope patterns. An Agent's variable scope defaults to its actual project;
mailbox access is self-only. Owners explicitly select variable scopes and mailboxes.
Help's `scopes` describes only coordination authorization scopes relevant to the
requested method, rather than inheriting the caller-wide list from `api.identity`.
A mailbox scope denotes the `agent` mailbox selector, not a `scope` request field.
Other methods return an empty list; native, process and provider selector values
remain described by their owning request DTO fields.
Unsupported mail presentation modes, tree variable scopes and native Goal/cost controls
are described as unsupported, not advertised as available native capabilities. Use
`agent.get` for target-generation capabilities before `command.submit`.

Malformed fields return Core `Error` code `invalid`, with a bounded `details.field` path
and `details.expected` shape, repeated in the safe message for transports that only expose
code/message. DTO-owned validation retains its existing error code (including `unsupported`).
Nested action DTO failures retain `params.args.<field>` for command submission and
`rule[<index>].do[<index>].<field>` when loading authored rule files.
Rejected values, unknown user-supplied keys/variants and whole payloads are never echoed.
An unknown key is reported as `<unknown-field>` with the DTO's allowed field names,
so even a credential accidentally used as a key stays redacted.
The existing MCP error-diagnostic forwarding is a separate transport concern.

The method inventory includes later-layer methods. Later methods identified
in the Purpose column or by the roadmap are target APIs only and are not registered
by the current Router; calling one now returns `unsupported`. Their semantics below
preserve the intended future contract, but do not imply current availability.
Native hook callbacks are private plugin-protocol traffic over the authenticated
instance channel, not a client API method; the plugin protocol defines their normalized
event and context response.

| Surface | Canonical DTO definitions |
| --- | --- |
| Agent/daemon views and cursors | [agent_views.rs](../../crates/eliot-core/src/api/agent_views.rs) |
| Project registration view | [agent_views.rs](../../crates/eliot-core/src/api/agent_views.rs) |
| Nested observation/Git views | [monitor_views.rs](../../crates/eliot-core/src/api/monitor_views.rs) |
| Command, turn and result queries | [reads.rs](../../crates/eliot-core/src/api/reads.rs) |
| Command and turn owner resolution | [resolutions.rs](../../crates/eliot-core/src/api/resolutions.rs) |
| Rule reads and history catch-up | [reads.rs](../../crates/eliot-core/src/api/reads.rs), [rules.rs](../../crates/eliot-core/src/api/rules.rs) |
| Command submission and shutdown | [mutations.rs](../../crates/eliot-core/src/api/mutations.rs) and [Core `SubmitCommand`](../../crates/eliot-core/src/lib.rs) |
| Timer schedules and requests | [timers.rs](../../crates/eliot-core/src/api/timers.rs) |
| Goal definitions, views and rounds | [goals.rs](../../crates/eliot-core/src/api/goals.rs) |
| Discovery/history queries and pages | [native_requests.rs](../../crates/eliot-core/src/api/native_requests.rs), [native_pages.rs](../../crates/eliot-core/src/api/native_pages.rs) |
| Mail, variable, claim and `who` requests, deliveries and pages | [coordination.rs](../../crates/eliot-core/src/api/coordination.rs) |
| Event queries, filters and pages | [api.rs](../../crates/eliot-core/src/api.rs) |

Errors include
`not_found`, `unsupported`, `not_allowed`, `bad_state`, `stale_generation`, `invalid`,
`busy`, `unavailable`, `capacity_unavailable`, `provider_held`, `history_gap`, `source_integrity`,
`secret_in_payload`, `frame_too_large`, `mailbox_full`, `version_conflict`, `claim_limit`, `internal`.

`capacity_unavailable` is a pre-effect rejection from a retained prior-generation
reservation or a configured Eliot `max_live` scope, not a vendor quota or entitlement
result. A capacity-rejected request is not admitted:
there is no command row to read back, and the same ID may be submitted again once the
scope frees. Once a command is admitted, its same-ID readback returns the retained result
and never retries the native effect. Provider-hold methods are defined below;
service-management APIs remain `unsupported` in this increment.

[`Error.code`](../../crates/eliot-core/src/lib.rs) is an open string, not a closed wire
enum; plugin-specific codes may be preserved. Unfamiliar post-I/O error codes classify
as `Unknown` under [Core policy](../../crates/eliot-core/src/command.rs), never proof
of failure or permission to replay the effect.

## Agent and daemon read views

The view calls use the Core view DTOs linked above. `daemon.status` is owner-only
and accepts an empty object. Its bounded blocker/command lists retain truncation flags;
unknown commands remain unknown. Agent list observations appear only when requested.
Absent source cursors and sequence/acknowledgement values remain null.

`daemon.status` also reports bounded, redacted runtime status for the two
daemon-owned automation tasks (`rule_worker` and `goal_driver`) via Core
`WorkerTaskStatus`/`WorkerTaskHealth`. `starting` means spawned but not yet past the
task startup boundary; `running` means alive after that boundary and does not prove
rule/timer/goal delivery; `stopped` means the task exited without error (including
shutdown); `failed` carries only a bounded code (`worker_panicked`, `worker_cancelled`,
`worker_task_failed`, `worker_exited`, or the task's error code), never raw messages.
The rule worker propagates its actual exit reason from its admission-closed
return sites: an intentional admission/shutdown exit surfaces as `stopped`,
while any other `Ok(())` surfaces as `failed` (`worker_exited`).

`project.list` and `project.get` are owner-only readbacks of the owner-registered
canonical checkout, repository common directory, explicit profile/action allowlists and
optional Git fetch approval. They do not create or modify registrations. Their project
values use Core [`ProjectRegistrationView`](../../crates/eliot-core/src/api/agent_views.rs),
whose optional `git` value uses [`ProjectGitView` and `GitCredentialHelper`](../../crates/eliot-core/src/api/agent_views.rs).

`rule.get` uses `RuleGetRequest`/`RuleStateView`, is owner-only, and reads the selected
global or project rule's current cursor and any retiring definition's cursor/boundary. An
optional `project` selects that registered project principal; omission selects global
Owner scope. It does not reload configuration or advance either cursor.
`rule.catch_up` accepts `RuleCatchUpRequest` and returns the retained Core `Command` parent
for a finite committed-event range. Its ULID binds the principal and original semantic
request; the retained command records the revision and captured range selected on first
admission. Same-ID replay returns that result before current gates without repeating an
effect. See the exact range and retention semantics in
[automation](automation.md#retention-and-bounded-backlog).

For a rule `send`, the retained Core `Command.turn_id` identifies the exact associated
turn; read its result with `agent.result` or `turn.get`. Trigger-specific target binding,
project fences and `applied` semantics are defined in
[Connected event rules](automation.md#connected-event-rules).

`timer.get` uses `TimerGetRequest`/`TimerStateView` and reads the selected timer without
advancing its schedule. `timer.run` accepts `TimerRunRequest { id, rule, project? }`,
where `id` is the external ULID mutation ID and `rule` is the timer ID. An optional
`project` selects the registered project principal; omission selects global Owner scope.
It returns the existing Core `Command` parent bound to that timer and exact mutation ID.
An exact mutation-ID retry returns that parent without admitting or dispatching another
action child. Manual runs do not consume the scheduled slot. See the timer semantics in
[automation](automation.md#first-connected-timer-slice).

`agent.get` includes the current generation's source rows and the managed workspace
target generation's rows when it differs. If persisted state requires a live owner and
the corresponding source is absent, it returns `unknown` rather than asserting the
persisted state is current.

Request DTOs reject unknown fields. Required Agent IDs are nonempty after a trim check
and are kept as supplied. Optional `generation`, `after` and process `scope` members may
be omitted or null to mean absent; wrong types are `invalid`. `generation`, when present,
must be a nonnegative integer. `scope` accepts `agent` or `workspace`; absence/null means
`agent`. `limit` defaults to 100 only when omitted and must be 1–1,000; explicit null,
wrong types and out-of-range values are `invalid`. `observations` defaults to false only
when omitted and must be a boolean when present; explicit null is `invalid`. Absent
optional members are omitted when requests are serialized.

`agent.list` returns compact summaries ordered by Agent ID, never full Agent
records; full records stay available through `agent.get`. Optional `state`,
`project`, `harness`, `parent` and `root` filters are exact indexed matches:
wrong types and unknown state names are `invalid`, and a filter never matches
silently. The selective structural/scope index (`parent`/`root`/`project`)
leads when several filters are present. Each summary carries identity, state,
current turn, progress age, the last terminal outcome with its turn and
retained usage where available, child activity for native observed children,
and the last controller-observed write time. A missing outcome or progress
timestamp reads back absent, never zero or success; `updated_at` is freshness
metadata, not proof of liveness for a detached or restarted source. The
response carries the required matching `total` ignoring the cursor, the
required explicit `truncated`, and `next` at the last returned row on every
nonempty page (empty pages carry no cursor); continue through `next` until the
page is empty. A missing `total` or `truncated` never reads back as zero or
quiet. `truncated` set means further rows are known; unset on a full page
promises nothing further. Owner calls page the whole fleet. An Agent principal
pages only its frozen control scope: itself for `self`, its subtree for
`tree`, its project for `project`. Tree scope for listing, totals and direct
reads shares one bounded depth rule, so enumeration cannot list or count an
Agent a direct read would deny. A self-scope page holds at most its own row
with its cursor. Joined observations bind the captured summary generation; a
resume between the summary read and the observation fetch drops the new
generation's observations instead of attaching them to the old summary. Typed
response binding failures remain `unknown`; validate the reply before printing
it or advancing a cursor. `swarm ls` and `swarm top` render these summaries as
compact tables by default, labeling the active turn separately from the
last-terminal outcome/usage and reporting observation data availability only;
`--json` keeps printing the raw response.

For `agent.processes`, omitted generation selects the current Agent generation for
`scope:"agent"`, or the managed workspace target generation for `scope:"workspace"`.
The Router resolves and authorizes that exact target before checking the cursor; owners
may read retained historical Agent generations. `ProcessPageCursor` binds the exact
Agent, generation, scope and process birth identity and is limited to 2 KiB. Pass it
back unchanged. `next` is the last returned identity even on the last nonempty page;
continue until `processes` is empty. A different Agent,
generation or scope rejects before querying process rows. The workspace response includes
`scope:"workspace"`; the Agent-scope response omits `scope`.

`agent.stats` and `agent.git` read the current generation; an explicit generation is an
expectation and a mismatch is `stale_generation`. `agent.edits` defaults to the current
generation, while an authorized owner may request a retained historical generation.
Its `after` is a path cursor: nonempty, at most 64 KiB, and without NUL. Pass it through
unchanged; the CLI does not resolve or normalize it. Edit pages default to 100 rows,
with limits 1–1,000, and `next` is the last returned path even on a nonempty final page.

The canonical monitoring DTOs distinguish process records, ephemeral CPU/RSS samples,
liveness, workspace custody and Git coverage. Their unavailable variants retain the
reason and may retain cached data. Missing or stale metrics stay null/unknown, never
zero; process activity is not model progress. Git and Windows canonical paths remain
opaque to the CLI, without resolution or normalization. Filesystem `retry_after_ms`
distinguishes an absent watch slot from an inactive one.

Workspace sources carry family ownership; Git monitoring sources do not. Source
digests are nullable JSON byte arrays, not strings; cursor and acknowledgement values
remain nullable. Stale, partial, gap and unknown values never become clean, complete
or zero. See [monitoring](monitoring.md#process-observation-and-terminal-ordering) for freshness and
[Git coverage](monitoring.md#git-and-filesystem-monitor) semantics.

`agent.edits` attribution requires verified, generation-matched native file-change hints;
a Git dirty set alone never proves Agent authorship. Git polling alone updates neither
process statistics nor model progress.

## Command admission

`command.submit` uses `CommandSubmitRequest` (the Core `SubmitCommand` alias) and
`CommandSubmitResponse` from the canonical mutation DTOs. `id` and `verb` are required;
unknown fields and wrong types are `invalid`. Optional `target`, `selector`,
`expected_generation`, `expected_turn_id` and `request_id`
may be omitted or `null` to mean absent. `args` defaults to `{}` only when omitted and
must be a JSON object; explicit `null` or another type is `invalid`. Absent expectations
remain absent. All six response members remain present, including nullable binding,
outcome and linked-command values; `fanout` is not part of the current response. A
selector parent reply carries no agent, generation, or linked command. A
`verb:"run"` request is targetless: `target`, generation/turn/request bindings are absent
or `null`, and `probe` is false. Its script inputs stay in `args` and follow the owning
[automation contract](automation.md#frozen-script-inputs). A `verb:"answer"` request
carries its replies in `args` as the shared
[`AnswerArgs`](../../crates/eliot-core/src/requests.rs); Store admission parses that
shape before persisting the command, so foreign answer shapes reject as `invalid` and
raw callers cannot bypass answer syntax.

For `verb:"inject"`, `args` is the canonical Core `InjectArgs` DTO
([definition](../../crates/eliot-core/src/context.rs)); `point` is required and is either
`next_input`, `turn_safe_point`, or `active_turn`, with optional absolute
Unix-millisecond `expires_at_ms`. Store freezes the target generation and, for
`turn_safe_point` and `active_turn`, the exact active turn at admission. A supplied
`expected_turn_id` must match that turn; `next_input` has no turn binding and applies
to the next SEND in that generation. An `active_turn` command dispatches directly
as the plugin's `agent.command` inject to the native turn/steer effect on the
captured turn, expiry being revalidated at dispatch. An
adapter that cannot apply the requested point returns `unsupported`; it does not map
inject to SEND. The command's acceptance/queue state means only that the context was
admitted. `CommandSubmitResponse` retains its existing six fields; for this command its
`outcome` is the narrow Core `ContextOutcome` identifying the mechanism, actual
application point and opaque consumer ([definition](../../crates/eliot-core/src/context.rs)).
It adds no separate receipt or response field.

`verb:\"configure\"` switches a live Agent's model and/or effort for its next
turn without touching the frozen profile snapshot. `target` is required and
`expected_generation` is optional; `selector`, `expected_turn_id`,
`request_id` and `probe` are `invalid` on this verb. `args` carries only
`model` and/or `effort` (at least one; JSON `null` means absent). Any other
field is `unsupported`, while authority-adjacent fields (`permissions`,
`approval`, `mcp`, `env`, `route`/`route_binding`, `account` and the
catalog/provider/billing siblings) are rejected outright as `not_allowed`.
Only the Owner principal may submit it. Codex carries both values as per-turn
`turn/start` overrides and Muse carries effort the same way; a Muse `model`
change needs a new native session and returns `unsupported`
(`restart_required`) storing nothing, as do adapters without a verified
per-turn mechanism (Claude, OpenCode, Antigravity). The Router stores the
overlay after dispatch, then settles `applied` with the requested and
effective values, the monotonically increasing `config_revision` for the
generation, `application:\"next_turn\"`, and provenance `\"configure\"`.
Each admitted native command (`launch`, `resume`, `send`) records the
configuration it runs under as the retained profile-snapshot identity plus
its stamped `config_revision`; each turn records the revision it ran under.
`command.get` and `command.children` return the command's retained
`config_revision` plus its `config` view (requested and effective
model/effort, revision, provenance, profile name), and `turn.get` and
`agent.result` return the same view for each turn: the view resolves from
the command's or turn's exact retained generation and revision, never from
the current overlay or the live file. `launch` without an overlay stays at
0 with no view, `send` freezes the revision current at its own dispatch,
and `resume` carries its inherited revision. A send committed before an
overlay never runs under newer configuration, and a config-file reload
never rewrites a retained generation, a pending command, or an
unknown-outcome resolution: after revision 2 applies, a retained
revision-1 turn still reads back revision 1's configuration. Readback
validators reject a negative `config_revision` and a `config` view whose
revision does not match the stamped revision. A `resume` explicitly inherits
the previous generation's latest overlay as its new generation's revision 1
with provenance `\"inherited_from\"` (the frozen snapshot is unchanged;
generations without an overlay start at 0). The lane is ordered by shared admission order: a
configure does not apply while an earlier-admitted send is still queued or
undispatched, an earlier queued send passes a later configure, and a later
input waits for an earlier configure. When the native turn request carries a
resolved effort the overlay left absent (for example a model-only configure on
a profile with omitted effort), the turn association records it on the
overlay, so the turn and the generation readback show the effort actually
sent; the requested values stay exactly what the owner asked. Retention keeps
every command a retained overlay references, including inherited revisions
and their source configures: pruning never deletes a command named by
`agent_config`, so configuration provenance survives expiry. `agent.get`
distinguishes the original request (profile name and requested overrides,
plus the launch-time `profile_inherited` decision: true only for an Agent
child that reused its own parent's snapshot verbatim, never for an Owner
launch that resolves the same profile) from the resolved configuration
(effective values, provenance, and revision).

Resolve the target and validate principal/capability/state before persisting the command
binding and events. Each verb checks `expected_generation` at its defined admission or
dispatch fence; it is not a universal equality check against the current generation at
the RPC boundary. The applicable expected-generation/turn mismatch rejects before that
verb's native effect. A generation-changing verb can establish or update its command
generation at dispatch, so the response generation is not universally equal to
`expected_generation`. Interactive calls may omit expectations; delayed automation
supplies its captured expectations.

For `git.fetch`, mutation-ID binding uses Core
[`GitFetchDefinitionArgs::binding_args(timeout_ms)`](../../crates/eliot-core/src/api/git.rs#L43)
or [`FrozenGitFetch::binding_args()`](../../crates/eliot-core/src/api/git.rs#L114): timeout
is normalized to positive milliseconds, so omission and equivalent spellings bind the same
caller intent. The first-admission Core `FrozenGitFetch` snapshot is retained and never
rebinds that caller ID. Store's read-only
[`replay_git_fetch_submission`](../../crates/eliot-store/src/lib.rs#L679) runs before current
gates or native I/O; same-ID concurrent winner/error replay returns the retained command
result without refreezing or fetching again.

The request accepts one tree selector instead of a target: `selector:"tree:<agent>"`
with `send` or `stop`, no `target`, and no generation/turn/request/probe binding;
anything else combining a selector is `invalid`, other verbs with a selector are
`unsupported`, and unknown selector kinds are `unsupported`. The optional `probe`
intent defaults to false; false is omitted on the wire, and true is
valid only for `send`. It is control metadata, never native input text or arguments.
`provider_held` is a pre-effect rejection (CLI exit 3); queued sends also recheck the
hold at dispatch before native I/O. Over-limit hierarchy admission reads `fanout_limit`
(CLI exit 3) and persists nothing.

The current `command.submit` request has no `wait_ms`, and the daemon does not expose an
`agent.wait` method. CLI `--wait` follows command and turn readbacks with a bounded client
deadline; timeout does not cancel the admitted command. `swarm.wait` below is the
implemented bounded server wait over a frozen child command/turn set. Single-target
server-side wait semantics further below are retained for a future API layer, not part
of the current request shape.

Generation-changing dispatch follows [core](core.md#generation-fence): advance and
revoke before native I/O, no rollback, no replay after unknown. Terminal settlement
revokes the token even if degraded Unix custody reports possible escaped descendants.

Launch/resume with explicit text creates a start plus linked `<id>/send`. Without
`--wait`, return session readiness; with it, follow the linked input to its exact terminal
turn. An input-created native session still reports these distinct outcomes.

Worktree creation is opt-in on the same launch command:

```text
command.submit {id, verb:"launch",
  args:{profile?:NAME, harness?:HARNESS, model?:MODEL, effort?:EFFORT,
        workspace, worktree?:BRANCH, base?:REF, parent?:AGENT, text?}}
```

A parentless Owner launch names either a stored `profile` or an ad-hoc
`harness` (mutually exclusive); `model`/`effort` are ad-hoc only and require
`harness`. Ad-hoc launch is Owner-only and freezes a profile-equivalent
snapshot from built-in safe defaults plus flags (see
[configuration](configuration.md#ad-hoc-owner-launch-no-stored-profile));
Agent principals keep the profile and `child_profiles` rules and cannot
ad-hoc launch. Same-ID retries bind the exact requested payload: a changed
ad-hoc model is `id_conflict`, while the frozen snapshot stays replay-stable.

Without `worktree`, `workspace` remains the existing execution directory. With it,
`workspace` names the source repository; the daemon chooses the managed destination.
`base` requires `worktree`. Omitted `base` means local `HEAD`, resolved and frozen to a
commit OID by daemon admission without fetching. The CLI forwards branch/ref strings
unchanged and does not resolve refs, create directories or run Git. Same-ID retries reuse
the retained preparation and frozen base instead of resolving a moved ref or replaying
uncertain work. Native launch waits for preparation ready in the exact target generation;
the existing readiness/linked-input wait semantics above are unchanged.

With `parent`, the launch creates a child of that Agent: an Agent principal launches
only its own children (the Router defaults an absent parent to the caller), the child
inherits the parent's project label and tree root, and the configured fanout limits
are checked at admission. The parent must be controlled. An Agent-principal child
may omit `profile` or specify the parent's same profile name, with no `worktree`,
and reuses the parent's frozen profile snapshot
verbatim, without resolving a mutable named profile. It may instead name a profile
from the parent snapshot's captured `child_profiles` allow-list; the daemon resolves
that configured profile now and freezes its snapshot for the child. Any other name,
or a cross-harness profile, is rejected before any native effect. The child must
use the parent's harness: only model and effort may differ, the child inherits the
parent's captured credential bindings verbatim, and its capacity scopes keep every
non-profile scope of the parent with the same bounds. Then the authority check below
applies: control is narrowed or equal, the workspace is shared, and no worktree is
minted. An Owner child launch may instead supply `profile`, a different workspace,
and `worktree`, and remains unrestricted by that check
(see [coordination](coordination.md#claims-and-hierarchy)).

Removal uses the same command admission and readback path:

```text
command.submit {id, target:AGENT, verb:"worktree.remove",
  args:{}, expected_generation?:N}
```

The Router/Store enforce owner authority, the captured generation and terminal-only,
clean removal under the admitted scratch policy. The daemon selects the retained
managed worktree, preserves its branch and never removes a caller-supplied path. The
CLI offers no force/path argument and uses its normal ULID binding and exit codes.
It reads the exact command to settlement with a bounded 30-second wait; timeout does
not cancel or retry removal. Same-ID retries return the retained outcome even after
the directory has gone.

A tree selector snapshots its members once at admission: the parent snapshots/sorts
concrete ids in one bounded transaction (bounded by `max_selector_targets`) and reads
applied for the expansion itself; children settle independently with the ordinary
per-target gates, except `send` skips terminal members instead of failing on them
(an all-terminal send applies with no children), and `stop` skips observe-only
native children instead of failing on them. A tree `stop` parent outcome carries
the admitted `children` plus a bounded `skipped` list naming each skipped
observe-only member as `{"agent": AGENT, "reason": "observe_only"}`; their
lifecycle stays with their manager/native harness. Parent applied means expansion only.
`command.children` pages that fixed set; a child created after admission is never
part of it. Retain/prune parent sets transactionally so a caller never sees a partly
pruned expansion; after removal return `history_gap`. Ordered rule actions reuse the
same representation with a distinct link kind, not selector execution semantics.

`command.children` is implemented for retained expansions through Core
[`CommandChildrenRequest`/`CommandChildrenResponse`](../../crates/eliot-core/src/api/reads.rs).
`id` names the parent and `after` names the last child of the previous page; both are
kept verbatim and must be nonempty. `limit` defaults to 100 only when omitted and must
be 1–1,000; explicit `null`, wrong types, out-of-range values and unknown fields are
`invalid`. Children page in fixed ordinal order and `next` is the last returned child
id, or null on an empty page. Reads use the same principal scope as `command.get`: the
parent and every returned child must be readable by the caller, otherwise the whole
read is `not_allowed`. The parent is authorized before any history or cursor check.
An `after` naming no child of the requested parent is `invalid`.
An absent id whose ULID time is older than the retry window plus the allowed
clock skew may have been pruned and reads `history_gap`; any other absent id
reads `not_found`. An absent id has no parent row to authorize against, so only
the Owner — authorized for every command — learns the gap; any other caller
reads `not_found` for absent ids, whether pruned or never issued.

## Command, turn, and result reads

These are read-only lookups using the canonical read DTOs and Core `Command`, `Turn`
and `Agent` models. IDs never pass through a mutation call and cannot submit or replay
work. Richer command/resolution metadata remains deferred; turn usage is connected where
the harness reports it. `agent.result` requires
an array of turn objects and an object for Agent metadata, not positional records.

For every failed terminal turn, with or without a structured provider restriction, status
and exact command/turn binding remain authoritative when final text exceeds the event
budget. The event omits oversized final text rather than exposing a truncated string as
complete, and includes
`final_text_unavailable:"event_budget"` when representable. `Turn.final_text = None`
means the public final text is unavailable; full native text remains private history.
`Turn.usage = None` likewise means native token usage is unavailable. A present usage
carries nullable token members with `scope`/`message_id` attribution when the harness
reports it; null attribution means the native scope is unknown. Message scope is never
a turn or session total, and a populated native total must not be assumed a per-turn or
per-call total when scope is unknown.

`usage.summary` reads bounded usage totals with explicit coverage over
exactly one of `agent` (with an optional `generation`; omitted covers all
retained generations), `project`, or `root` (the root Agent and its
descendants). The same read authorization as the other Agent reads applies:
an Agent principal sees only what its frozen scope allows. The response
echoes the scope, reports the included Agent/generation counts with explicit
`truncated`, states `history_complete` with its reason and the
`pruned_through` watermark, groups turn expenditure by native scope with
`turns_counted`/`turns_without_usage`/per-counter `turns_missing` (no
cross-scope totals) plus the excluded unknown/running turns the sums skip,
and carries session subtotals with their supply gaps
and per-unit costs. `swarm usage` renders this response as a compact table;
`--json` keeps printing the raw response.

Required IDs must be strings whose trimmed value is nonempty. Validation does not trim,
normalize or impose a new length limit on them. `turn.get` requires a nonnegative integer
generation. For `agent.result`, omitted `generation` selects the current generation;
omitted `turn_id` requests turns for that generation; omitted `limit` defaults to 100.
Any supplied generation must be a nonnegative integer, and any supplied `turn_id` must
be a nonempty string. The limit must be 1–1,000 even when `turn_id` selects one exact
turn. Optional request members that are absent are omitted during serialization; do not
send `null`. Explicit `null`, wrong types, invalid values and unknown request fields are
`invalid`. Unknown RPC methods and command verbs retain their existing `unsupported`
behavior.

The existing principal checks, generation rules and unknown-state masking apply. An
explicit generation retains the existing principal-scoped historical read behavior.
`agent_metadata` is the current Agent object and may have a different generation from
the result when an explicit historical generation was requested.

Before printing a read response or using it to advance a wait, the client validates its
binding: `command.get` must return the requested command ID; `turn.get` must match the
requested Agent, generation and turn ID; and `agent.result` must match the requested
Agent, its requested generation (or the current metadata generation when omitted), and
the requested exact turn when present (exactly one matching turn). Every result turn
must match the response Agent and generation, and the response must respect the requested
limit. A mismatch yields only the fixed `unknown` error
`read response does not match the requested binding`, with no response data or
diagnostics exposed. A `command.children` response must hold at most `limit`
well-formed commands with `next` equal to the last child id, or null when the
page is empty; a mismatch is the same fixed `unknown` error.

`swarm cmd show`, `swarm cmd children` and `swarm result` exit 0 after a valid read
even if a returned command or turn is `unknown`; that state is not a successful task
outcome. A binding mismatch is unknown and must not print the mismatched response.
`swarm wait` prints its per-entry snapshot and exits 0 only when every waited entry
succeeded (`--all`) or at least one succeeded (`--any`); timeout or an uncompleted
snapshot exits 5, interrupted entries exit 7, `unknown`/`not_found`/`not_visible`
entries exit 4, and plain failures exit 1.

## Input, turn and unknown gates

Transport/inbox admission is not send applied. Bind input to an exact native turn, then
track its result independently. Unassociated dispatched/unknown input blocks later
send/configure/Goal continuation; active/unknown turn blocks next turn; unknown transition
blocks further takeover. Exact stop/interrupt/request replies/readback remain available.

A dispatched or unknown `configure` also holds the settings lane: do not dispatch a
later configure, new input or native/Eliot Goal start/resume until its exact outcome and
effective settings are read back or explicitly resolved. Timeout does not release this
hold, even when the observed Agent state is idle. Admission order decides both
directions of the lane: an earlier-admitted send passes a later configure, and
a later send waits for an earlier configure. This is a predicate over existing
command rows, not another queue/table. Stop/interrupt/request replies/readback still work.

Owner resolution of unknown outcomes is implemented and owner-only. Both calls take
a mutation id, bind it idempotently to the exact assertion payload, and record an
owner assertion, never a native receipt. Each releases only the resolved
command/turn gate: sibling unknown commands and turns stay fenced, and neither
assertion establishes process closure. A resolution never releases a
[capacity reservation](configuration.md#accounts-and-configured-capacity): a
retained reservation is released only by custody closure (restart recovery).
Known failure can include partial effects; it never implies rollback or automatic retry.
Observe-only Agents have no resolvable commands or turns; resolving any of
theirs is refused with `not_allowed`.

`command.resolve {id, command_id, as: applied|failed, reason}` requires the target
command to be stored `unknown`; dispatched, queued, accepted, and terminal
commands are refused with `bad_state`. `launch` and `resume` resolve as `failed`
only: `applied` is refused with `bad_state`, since an applied start without its
Agent transition is inconsistent. The command keeps any committed turn
association and gains no invented one — including a send whose native turn
association was lost, which resolves without creating a turn. The outcome
carries `resolution_source=owner`, as does the emitted `command.resolved`
event; no `command.applied`/`command.failed` receipt is emitted.
Future turn-result metadata retains the planned `command_ids` field; `usage` is
connected and stays null on owner-resolved turns without native usage.

`turn.resolve {id, agent, generation, turn_id, status: completed|failed|interrupted,
reason}` requires the turn to be stored `unknown`; running and terminal turns
are refused with `bad_state`. It further requires either a stored terminal
generation (`Lost`, `Stopped`, or `Failed` — Eliot no longer controls it) or a
native readback through the plugin (`agent.snapshot` for the exact turn)
proving the turn inactive. The readback proves the turn inactive only when it
binds this Agent generation, names the turn as its last turn with
`terminal: true` and a terminal status, and does not report it as the active
turn. The stored active turn alone is never proof: a live
generation without such a readback is refused with `bad_state`. A generation
that does not match the Agent's current generation is `stale_generation`. The
turn completes with the asserted status, and a `turn.resolved` event carrying
`resolution_source=owner` is emitted; no native `turn.completed` receipt is
invented and no command row is touched. The current `Turn` read object does not
expose the resolution source.

## Provider holds

`provider.holds` is owner-only and returns a bounded page ordered by local scope.
Optional `after` and `limit` select the page; the response provides `holds` and
`next`. The canonical Core `ProviderHoldsRequest`/`ProviderHoldsResponse` DTOs own
the wire shape.
`provider.clear_hold` is an owner-only idempotent write using
`ProviderClearHoldRequest`/`ProviderClearHoldResponse`. The corrective evidence is a
closed enum: `credentials_updated`, `configuration_updated`, or `owner_verified`;
free text is invalid. Its expected-probe binding is compare-and-swap, including explicit
null to mean no probe claim; a mismatch or unresolved probe cannot be cleared. See the canonical
[Core API DTOs](../../crates/eliot-core/src/api/provider.rs) rather than duplicating
their field list.

A provider scope comes from the frozen explicitly selected account, otherwise the
exact profile; a shared adopted proxy is only a capacity scope, never a provider
hold scope. Eliot never guesses a vendor account. A held scope rejects new launch/send before
native I/O and is rechecked when queued work dispatches. Launch with linked input and
resume with new text are gated as sends. Plain resume, reads, request replies, interrupt
and stop remain available. At initial admission, `provider_held` rejects before binding
and creates no command row; that ID may be submitted again after the hold is resolved.
An already queued command that becomes held is rejected at dispatch under its existing
binding, before native I/O.

Only an explicit `send --probe` can claim an eligible rate-limit/quota hold. It uses the
ordinary SEND authorization and target/scope/generation checks; it grants no owner
authority and adds no role flag. The Store atomically allows one claim per frozen
provider scope through the normal send path. Future bounded automation must use that
same path, not a parallel probe executor. The flag defaults false and does not enter
native input. Time makes a probe eligible but never clears a hold. A successful
exact-bound terminal probe clears rate-limit/quota holds; failed or interrupted probes
retain them, while unknown probes retain their claim across restart. Auth and ModelRemoved
holds require owner correction and are not probe-eligible. Provider restriction facts
are defined in [monitoring](monitoring.md#provider-holds).

## Waiting

`swarm.wait` is the implemented bounded completion wait over an explicitly captured
child set. The request carries `commands` (command ids) and/or `turns` (exact
`agent`/`generation`/`turn_id` bindings), `mode` (`all` completes when every entry is
terminal; `any` completes when any entry is terminal), and `wait_ms` (clamped to 30,000,
matching `events.subscribe`). The set is frozen at request time: later turns or new
children never join it. A child being idle is not success; each entry reports the
terminal outcome of its exact captured command or turn. Per-entry `status` is one of
`succeeded`, `failed`, `interrupted` or `unknown` (from the canonical `TurnOutcome`),
`still_running` at timeout, or `not_found`/`not_visible` for ids outside the caller's
scope. An Agent caller may wait only on what its frozen control scope permits, the same
rule as `agent.list` and direct reads; the set holds at most 256 entries. The daemon
reuses existing command/turn readback and its event bus for wakeups; there is no
scheduler or persistent workflow beyond the digest below. Run and project-rule principals are rejected;
Owner and Agent principals are allowed. Timeout never cancels admitted work. The CLI is
`swarm wait --all|--any --timeout-seconds N ID...`, where an `ID` containing `/` is an
exact `agent/generation/turn_id` turn ref and any other `ID` is a command id. Exit codes
follow the read section: 0 only when every entry succeeded (`--all`) or at least one
succeeded (`--any`), 5 on timeout, 7 when an entry was interrupted, 4 for
`unknown`/`not_found`/`not_visible`, 1 for plain failure.

`swarm.digest` is the durable completion digest over the same frozen child set.
The request carries `id` (caller ULID, stable effect identity), `to` (one parent
Agent mailbox), `mode` (`all`/`any`), and `commands`/`turns` (1..=256 entries,
frozen at admission like `swarm.wait`). An Agent caller may digest only to its own
mailbox and only entries within its frozen control scope; out-of-scope entries read
`not_visible` at registration and stay masked at delivery, which recomputes as
owner and never widens scope. When the mode condition holds over terminal entries
(`still_running` is the only non-terminal status; `not_found`/`not_visible` count
as settled honest outcomes), the daemon delivers exactly one coalesced `mail.send`
(queue) result to the parent: mail id is the digest id, thread is
`completion-<id-prefix>`, and the body lists every entry outcome. Same id plus same
payload replays the retained registration and mail without a second delivery; same
id plus a different payload or principal rejects `id_conflict`. Reconnect or restart
replays the same mail id, so the digest is never duplicated. The digest reuses the
ordinary mail wake (one generic coalesced notice per minute, at most one open notice
command) and emits no `x.user.agent.attention` event; a successful completion stays
distinct from an attention incident and no model call is needed to poll. The CLI is
`swarm digest --all|--any --to PARENT ID...` with the same `ID` shape as
`swarm wait`; it exits 0 when the digest is accepted (pending or delivered).
Neither the digest nor existing persistent HTTP notifications wake a
disconnected/stateless external client.

The following single-target server-side wait semantics are a future API contract; they
do not add `agent.wait` to the current method set. Current CLI `--wait` behavior is
described under Command admission.

Capture generation at call start. `turn --after-command ID` follows that input's exact
association/result; without an id, capture the current turn or the first after the call
high-water. `idle` requires no open input/turn/transition or unresolved settings gate.
`stopped` means terminal; `goal` captures its revision. Replacement returns
`stale_generation`; timeout never cancels admitted work. Several native inputs may
legitimately share one turn result.

## Discover, attach, detach and resume

Discovery is owner-only, read-only/paged and creates no Agent rows. It resolves the
required profile from current daemon configuration and derives its target environment;
a supplied harness must match that profile. It performs no native control operation.
Attachment uses one canonical native subject row. First attach targets generation 1; a
later ownership takeover gets the next generation. Validate before dispatch, commit
identity fence before native I/O, and install exact readback on success. Failure never
restores revoked control; unknown blocks another takeover.

`agent.discover` uses `AgentDiscoverRequest`/`DiscoverPage`; `agent.history` uses
`AgentHistoryRequest`/`ObservationPage` from the canonical native metadata DTOs.
`harness.list` (`HarnessListRequest`/`HarnessListResponse`, no params) reports
per-harness installation facts (installed, resolved `cmd`, `--version`);
`harness.models` (`HarnessModelsRequest`/`HarnessModelsResponse`) returns the
harness's own read-only catalogue (Codex/Muse native `model/list`, the OpenCode
provider/model list; Claude reports aliases plus the CLI default, Antigravity
reports no catalogue). An optional `profile` selects the native route/account
(the named profile must exist and its frozen harness must match); omitted it
means ad-hoc daemon defaults. The read is cached per native route/account
scope with a freshness timestamp (`fetched_at_ms`, `ttl_ms`) and a nonsecret
credential identity (a name, never a value); every response carries its
`scope`, `fetched_at_ms`, `ttl_ms` and `credential`. Native model ids are
retained verbatim and a failed read never selects another model or account.
`harness.quota` (`HarnessQuotaRequest`/`HarnessQuotaResponse`) is the separate
provider-supported account quota readback: timestamp (`fetched_at_ms`,
`ttl_ms`), `scope` and `credential` as above, plus an explicit `status` of
`supported`, `unsupported` or `unknown` naming the checked native surface.
The installed Codex/Muse/OpenCode schemas expose no read-only quota endpoint,
so they report `unsupported`; support stays provider-gated and never comes
from an inference probe or per-Agent polling. Both reads are owner-only,
read-only, deduplicated per account through the same bounded per-scope cache,
and start no model turn and no inference
(`swarm harnesses`, `swarm models --harness H [--profile P]`,
`swarm quota --harness H [--profile P]`).
`profile` and `id` are required nonempty strings. Present strings are checked for
nonemptiness after trimming but are preserved as supplied. For `agent.discover`, optional
`harness`, `native_scope` and `cursor` may be omitted or `null` to mean absent. For
`agent.history`, the same applies to optional `generation`, `turn_id` and `cursor`.
Wrong types are `invalid`. `limit` defaults to 100 only when omitted and must be an
integer from 1 through 1,000; explicit `null`, wrong types and out-of-range values are
`invalid`. Optional `archived` defaults to `false` when omitted; a non-boolean value is
`invalid`. `archived:true` is an explicit caller selection of archived native subjects;
the default selects nonarchived subjects. Unknown request fields are `invalid`.

Discovery scope is limited to 512 bytes and rejects control characters. Its cursor is
limited to 16 KiB and rejects control characters. Pass that opaque cursor unchanged; it
is bound to the selected profile, target environment, verified native store, scope,
method, fixed native-list filters (including `archived`), ordering and page size. Changed store selection
rejects before listing subjects. Unverified selection is an explicit error.
`coverage.complete` means this paged enumeration
has reached its native end, not that it is a consistent snapshot or a live subscription.
For Codex it covers only the subjects the SDK query returns — for archived selection,
retained archives — not all raw database or history rows and not a capacity proof.
Codex coverage is `state_db_only`; it never scans/repairs rollout storage to fill the
list. The Codex query selects archived threads only when `archived:true` is explicit
and nonarchived threads by default, and fixes the page size in its cursor binding;
a cursor from one selection never continues the other. Archived discovery returns
the same bounded metadata only and performs no unarchive, resume or control operation. Subject timestamps are milliseconds since the Unix epoch; native
seconds are converted before projection. Session previews, prompts and transcripts are
excluded. Native `notLoaded` is reported as unknown: a separate reader cannot prove
another process's session stopped.
Discovery installs no Agent identity and invokes no native start/resume/input operation.
Owned readers finish cleanup after a client disconnect; drain waits for them and force
cancels their own custody. New discovery rejects during shutdown.

The first observation mutations use the existing command pipeline:

```text
command.submit {id, verb:"attach", expected_generation?,
  args:{profile,harness,native_scope,native_ref,mode:"observe",project?}}
command.submit {id, target:AGENT, verb:"detach", expected_generation?, args:{}}
```

Require an explicit profile. Resolve the same mutation id before rereading mutable
configuration; retained retries keep their original subject/profile. Attachment reuses
the canonical scope/reference row. A controlled row or an active observer returns
`busy`; this increment does not convert control or grant a second identity. Attach
commits its generation/source fence before reader I/O and installs no Agent token.
All native control verbs reject observe-only targets, including an owner-issued
targeted stop; only a tree `stop` expansion skips them and reports them as skipped.
`mode:"control"` is unsupported. The vendor plugin method is `agent.attach`; it is not
a second public mutation method.

A reader may return a bound `failed` attach result only when preparation refused before
native startup or Codex reported `native_start_failed` under its proven no-effect startup
contract. The Router verifies the exact Agent generation, command, and native subject, and
accepts `failed` only after the owned reader is confirmed closed. Same-command retries replay
the retained failure; a changed binding conflicts. An uncertain startup or cleanup remains
`unknown`.

`agent.history` is a separate owner-only read. `source` is required and must be
`native` or `rollout`; `view` is required and must be `subject`, `turns` or `items`.
Omitting either required field, supplying `null`, or using a wrong type is `invalid`;
other source/view values are `invalid`, while unsupported source capabilities retain
their existing explicit error behavior. Optional `generation`, `turn_id` and `cursor`
may be omitted or `null` to mean absent; other wrong types are `invalid`. A supplied
generation must be a nonnegative integer (`invalid` otherwise) and equal the current
observer generation (`stale_generation` otherwise). `items` requires a nonempty `turn_id`;
`subject` and `turns` forbid it. The read targets an existing observe-only Agent with no
control identity and is bound to its current generation. The CLI defaults `--source` to
`native` and sends that value explicitly. Absent optional request members are omitted
when serialized, never sent as `null`.

Successful reader attachment can leave the Agent `unknown`. History pages are private
views: they neither replay lifecycle into the event bus nor acknowledge input, command
or request delivery, or write observation pages to Store. Bind every page/cursor to
Agent/generation, exact native subject, source/view, turn filter, ordering and selected
store. Items require an exact turn id. A supplied history `turn_id` is limited to 16 KiB
and rejects control characters. The `ObservationQuery` cursor bound is nonempty and at
most 16 KiB; it has no additional control-character rule. Pages contain at most 1,000
entries and are at most 900 KiB encoded. `entries` and `coverage` are opaque JSON values;
the wrapper does not reinterpret native/vendor records or coverage fields.

The Router rejects a non-null `next_cursor` equal to the request cursor, regardless of
source coverage. An absent cursor does not let a wrapper infer complete history: preserve
the opaque coverage object and do not synthesize completeness.
Unsupported source/paging stays explicit. Observation never resumes a thread to obtain
history. See the neutral
`ObservationPage` bounds in [plugin protocol](plugin-protocol.md#host-requests).
Observe detach fences new reads, closes only its reader and records source closure
after cleanup. It retains unknown native state and never reports native session end.

Controlled detach applies only to independently living sessions/services and remains
unsupported until that native path exists: revoke control and become observe-only on
exact success without killing the shared/adopted service. It does not settle an unknown
input. Controlled resume reopens the same native subject as a new generation and never
resends previous input.

## Event replay

Event requests, responses and `EventsFilter` are defined in the canonical Core API.

`events.bounds` accepts `EventsBoundsRequest` and returns `EventsBoundsResponse`
([Core DTOs](../../crates/eliot-core/src/api.rs)). Its optional `filter` has the same
grammar and caller-scope checks as replay. The numeric bounds describe the journal,
as does replay's `high_water`: `pruned_through` is the exclusive pruning floor and
`hi` is the allocated sequence high-water mark, including when every event row has
been pruned. This bounded metadata query reads no event page and requires no cursor.

CLI `watch [AGENT]` reads these bounds once and starts at `hi`, delivering new events.
`watch [AGENT] --after SEQ` instead uses that exact exclusive replay cursor. A lost
cursor, including explicit `--after 0` after pruning, returns `history_gap`; it is
never clamped or replaced by a new tip. MCP notification bootstrap also uses bounds
instead of replay from zero. A later retention gap still fails explicitly.

Before printing events or advancing a cursor, validate each response page against the
effective request cursor: `0 <= after <= next <= high_water`. Event `seq` values must be
strictly increasing, greater than `after`, and no greater than `next`. A malformed page
returns a constant `unknown` error. Do not clamp the response, default a bad cursor,
replay from another cursor, or skip forward.

`after` defaults to 0 only when omitted. If supplied, it must be a nonnegative `i64`;
`null`, negative, fractional, string, boolean and overflowing values are `invalid`.
`limit` defaults to 100 and must be an integer from 1 through 1,000. For both methods,
an omitted `filter` means an empty filter. Explicit `null`, an unknown request or filter
field, or any wrong field type is `invalid`. Each `agents`, `kinds` or `sources` array
may contain at most 1,000 raw entries; every entry must be a nonempty string. Agent filter
strings are compared verbatim rather than canonicalized, so a noncanonical value may
simply match no events. An Agent principal remains scoped to itself; an explicit nonempty
agent filter that excludes it returns `not_allowed`.

Only `events.subscribe` accepts `wait_ms`. It defaults to 0, must be a nonnegative `u64`
when supplied, and values above 30,000 are clamped to 30,000. Bad types, negative values
and overflow are `invalid`; `events.read` rejects `wait_ms` as an unknown field. This wait
fills an event page and does not wait for command or turn settlement.

Subscribe before reading the persisted high-water mark, replay through that mark, then
release buffered persisted events above it in order. `next` is the exclusive replay
cursor; `high_water` reports the persisted high-water observed by the read. A buffer lag
sets `lagged` and triggers persisted replay from the current cursor. `truncated` reports
page count/encoded-size limits. Ephemeral delivery is best effort; pruned history returns
`history_gap`, never silent skipping. Committed event sequence values are never reused.

For these known methods, malformed params, wrong types and unknown fields return
`invalid`. Unknown methods and unsupported command verbs remain `unsupported`; this keeps
request grammar errors distinct from feature availability.

## Mail acknowledgement

Mail belongs to the stable recipient Agent, while its presentation command remains
bound to an exact generation. A valid current Agent may explicitly read older unread
mail for that same Agent; responses show the original recipient generation and any stale
presentation. Resume does not resend the old prompt or retarget its presentation command.

`mail.inbox` returns at most 100 deliveries within the encoded response-frame limit
and marks nothing. `mail.read` is also read-only. After writing complete output, CLI sends:

```text
mail.ack { id: <new write id>, deliveries: [<exact delivery id>, ...] }
```

Authorize the current caller for that stable mailbox and acknowledge only those ids.
A lost response may redisplay mail, not lose it or acknowledge newly arrived messages.
See [coordination](coordination.md#mailbox) for bounds and presentation policy.

## Mail and variables

`mail.send` queues one message to 1–100 concrete Agents in a single bounded
transaction, snapshotting each recipient's current generation; unknown ids
never create Agents. Only `queue` mode is connected; `mail.send` presentation modes
`send`, `steer` and `inject` return `unsupported`. This limitation is specific to mail
presentation; the uniform system controls use `command.submit` and their native
application semantics. A current Agent may queue
addressed mail to any existing Agent, regardless of project label or worktree;
mail is communication only and does not grant command control over the
recipient. Its generation must still be current. A reply names the exact delivery being
answered and addresses only the original participants; the sender must be
one of them unless it is the owner. Mail text is at most 32 KiB and, like
every other coordination payload, rejects known credentials with
`secret_in_payload`.

`mail.inbox` pages unread rows oldest-first with a limit of 1–100 (default
100) and an `after` delivery cursor, truncated to the encoded response
bound; `next` is the last returned delivery id, or null on an empty page.
`mail.read` returns one exact row. Both are read-only. An Agent caller reads
only its own stable mailbox and may omit `agent`; the owner must name the
mailbox explicitly. `mail.ack` acknowledges only the listed exact delivery
ids, idempotently; an absent id that may have been pruned succeeds as
already acked, while any other unknown or foreign id rejects the whole
acknowledgement with `not_found` and marks nothing. An absent delivery whose
write-id ULID time is older than the retry window plus the allowed clock
skew may have been pruned and reads `history_gap` on read, inbox cursor
and reply reference; any other absent delivery reads `not_found` (`invalid`
for an inbox cursor). All three writes bind
their ULID idempotently to principal, method and semantic payload.

`var.set` and `var.get` address scopes encoded as `global` or
`project:<name>`; tree scopes are `unsupported` until hierarchy membership
exists. An omitted scope resolves to the Agent caller's project; the owner
must name the scope explicitly. Only the owner writes global variables;
Agents write only their own project's variables and read their project's
plus global ones. Values are JSON of at most 16 KiB encoded. An omitted
`expect_version` writes unconditionally; a supplied one must equal the
current version, where an absent row reads as 0, otherwise the write is
rejected with `version_conflict` and nothing changes. Every write
increments the version, records `set_by`, and emits `var.changed`.

The CLI supplies exactly one value source: inline `VALUE` or `--value-file
PATH`, mutually exclusive. The file is read as bounded UTF-8 (at most 16 KiB)
and its contents become the value; with `--as-json` the contents must parse
as JSON before admission. The file path is not part of command metadata;
only the resulting value is stored.

A routed recipient with `wake_on_mail` (profile snapshot, default true) may
receive one generic coalesced wake per minute with at most one open notice
command per recipient across mail and claims: `N unread messages; run swarm inbox, then ack processed delivery IDs`.
The notice never contains
the body, is sent through the ordinary owner `send` path bound to the
recipient's current generation, and is skipped — never retried — while a
notice command is open or unknown, a provider hold, pending request or
unknown command/turn stands, the daemon is draining, or no live plugin
owner exists. See [coordination](coordination.md#modes-and-wake).

## Work attribution

`claim.set` records one advisory claim — a project-relative glob with an
optional note and a TTL — owned by an Agent generation. Globs use `/`
separators with `*`, `**` and `?` only, at most 1024 bytes and at most 8
`**` segments (the cap keeps the overlap scan against every live claim
cheap); absolute paths, `..` segments, bracket/brace metacharacters and
a ninth `**` segment are `invalid`. The
TTL is 1 second to one day (default one hour). An Agent caller claims
only its own current generation; the owner names the Agent explicitly and
cannot claim for a terminal Agent (`bad_state`). One generation holds at
most 100 live claims; beyond that the write is rejected with
`claim_limit`. Re-setting the same glob refreshes its note and TTL. Both
writes bind their ULID idempotently to principal, method and semantic
payload; set and release each emit `claim.changed`. `claim.release`
removes one live claim; absent, expired and prior-generation claims read
`not_found`.

A claim is live only while its generation still matches the holder's
current generation, the holder is not terminal, and the TTL has not
passed. Generation change, terminal state and detach therefore release
by construction; expiry filters on read and is purged in a bounded batch
by the next claim write. A conflict — two globs that may overlap after
normalization (segments are compatible when equal or either side carries
a wildcard, with `**` realigning across directories), in the same project
— is reported in-band in the `claim.set` response and routes at most one
coalesced generic notice per holder: `1 overlapping work claim` /
`N overlapping work claims`, plus `; run swarm who`. Detection is
conservative: a false positive only adds an
advisory notice. The in-band list holds at most 100 conflicts;
`truncated` signals the scan or report cap engaged. The notice shares the
mail-wake gate (one per minute, at most one open notice command per
recipient across mail and claims, same unknown/hold/request suppression,
ordinary owner `send` bound to the holder's current generation); there is
no `wake_on_mail` flag for claims, and a replayed `claim.set` (no new
event) never re-notifies. A conflict is never a lock, a reassignment, or
a second ownership database. `claim.list` pages live claims, optionally
filtered by project and Agent; an Agent caller reads only its own project.

`who` composes existing L3 projections per current worker: the roster
holds every non-terminal Agent in scope (at most 100, oldest first, with
a `truncated` flag instead of a cursor). Each worker carries its
current-generation verified edits whose paths are still dirty in the
cached Git view, the dirty files of its worktree when exactly one
non-terminal Agent admits that checkout (counted across projects),
committed paths against the frozen base the workspace records, its live
claims, and optional bounded recent history (`history: true`). Old
generations and terminal Agents are never shown as current workers;
dirty files of a shared checkout stay unattributed under `shared`, which
lists only checkouts with two or more in-scope owners. An Agent caller
reads only its own project; the owner may filter by project or read all.
See [coordination](coordination.md#who-works-on-this) and
[coordination](coordination.md#claims-and-hierarchy).

## Drain and shutdown

`daemon.shutdown` uses the canonical `DaemonShutdownRequest`/`DaemonShutdownResponse`.

This owner-only request requires exactly an `id` and the `ShutdownMode` value `drain` or
`force`; unknown fields, missing fields, wrong types and nulls are `invalid`. Its
first reply is retained under the literal mutation ID. An exact retry returns that
same reply, including the original drain blocker snapshot; use owner-only
`daemon.status` for current state. Reusing the ID with another mode returns
`id_conflict`. A retained reply written before the drain report existed carries no
`drain` field and still validates; a present report always validates.
Force admission is monotonic; a later, in-flight, or replayed drain cannot downgrade it.

Drain is a pure read-side report: no effects run before the shutdown-ID claim, and
the pre-change effect behavior is unchanged. Drain returns `state:"applied"` with an
empty typed blocker list when no blockers were observed, otherwise
`state:"dispatched"` with the captured `StatusBlocker` list. Drain also returns a
bounded `drain` report reusing the existing blocker sources instead of
a new registry: running turns (Agents with a recorded `active_turn`), unsettled
commands, unknown commands and live owned processes. Live owned processes come from
the Store custody snapshot (process records not yet `exited`), never from routing
eligibility or workspace operation locks: a held workspace operation lock alone is
not a live child and never appears. Every category carries a count, a bounded list
and a truncation flag; the bound is 64 entries per list, and the flag is true
whenever any qualifying record was omitted for any reason (the list bound or the
Store page byte bound trimming the page). `AgentLive` blockers carry the
observed `active_turn` when present. An empty report never proves that no native
effect occurred: an unknown native effect remains unknown until evidence or an
explicit owner resolution, and neither "session never ready" nor an empty stderr
proves that no effect occurred. The daemon never auto-resolves unknown to finish
drain and never forces on the drain path. Force normally returns `state:"dispatched"`
and no blocker or drain list. If a Store write fault prevents retaining the force
reply, the fallback still dispatches force and returns `store_write_faulted:true` with
the dispatched state; that fallback is not a retained write result. When present,
`store_write_faulted` is true.

`daemon.status` carries the same bounded `drain` report; while draining it is the
current blockers snapshot, while open it reflects the current fleet.
`swarm daemon stop --drain` prints the retained `DaemonShutdownResponse` including
`blockers` and `drain`; `swarm status` prints the typed `DaemonStatusResponse`
including `blockers`, `unsettled_commands`, `unknown_commands` and `drain`.

Drain rejects new turn-producing work and new automation, while continuing pre-admitted
work and allowing exact stop/interrupt/request replies/cancel/detach/readback. Unknown
outcomes remain visible blockers. Command admission retains its early per-verb mode check
and repeats it after preparation under the same Store writer lock that changes shutdown
mode. Ordinary and dedicated `git.fetch` admission resolve an exact retained command
before this final check and before writer-health gates, including an identical admission
that won during preparation. Retained readback remains available after drain, force or
a writer fault when the Store can prove committed reads.

Force closes workspace and controlled-plugin startup admission before cleanup snapshots.
Startup/cleanup gates cover workspace effect ownership and the first controlled-plugin
filesystem effect through registration of its owned child; cleanup waits for an in-flight
handoff before enumerating children. Native readiness and I/O run outside the Store lock,
and the controlled-startup gate is released before waiting for native readiness. Effects
that already began retain their unknown-outcome semantics. Force terminates only owned
custody, never adopted services. Degraded Unix reports possible orphans, not exact clean
success.

## CLI

All commands accept `--json` and `--data-dir`; every mutation accepts `--id`.
Generation/turn/request flags are optional for immediate calls, explicit for delayed ones.
The synopsis is target grammar; later-layer forms are not an inventory of current help.

```text
swarm daemon [--foreground]
swarm daemon stop [--drain|--force]
swarm status
swarm launch --profile P --workspace PATH [--worktree BRANCH [--base REF]] [--parent AGENT] [PROMPT] [--wait] [--timeout-seconds N]
swarm worktree remove AGENT [--generation N]
swarm ls | show A | watch [A|SELECTOR] [--after SEQ] | top
swarm processes A [--scope agent|workspace] [--generation N] [--after CURSOR] [--limit N]
swarm stats A [--generation N]
swarm git-status A [--generation N]
swarm edits A [--generation N] [--after PATH] [--limit N]
swarm send A TEXT [--probe] [--wait] [--timeout-seconds N] [--generation N]
swarm steer A TEXT [--generation N] [--turn ID]
swarm inject A TEXT --point next_input|turn_safe_point|active_turn [--generation N] [--turn ID] [--expires-at-ms MS]
swarm interrupt|abort A [--generation N] [--turn ID]
swarm stop A [--force] [--tree | --generation N]
swarm resume A [PROMPT] [--wait] [--timeout-seconds N]
swarm wait --all|--any --timeout-seconds N ID...
swarm result A [--generation N] [--turns N]
swarm turn show|resolve ...
swarm approve|deny A [--request R] [--choice ID] [--feedback TEXT] | answer ... [--request R]
swarm configure A [--model MODEL] [--effort EFFORT] [--generation N]
swarm compact|fork|background ...
swarm cmd show|children|cancel|resolve ...
swarm events [--since SEQ] [--allow-gap]
swarm msg send ... | msg show ... | msg ack ... | inbox [AGENT] [--wait SECS]
swarm var set KEY [VALUE | --value-file PATH] [--as-json] [...] | var get ... | who [--project P] [--history] | claim set|list|release ...
swarm project list | show ID
swarm rule show ID [--project PROJECT]
swarm rule catch-up ID --after SEQ [--through SEQ] [--project PROJECT]
swarm timer show ID [--project PROJECT] | run ID [--project PROJECT]
swarm goal set A --text TEXT --check CMD [ARGS...] [--mode eliot|native] [--check-cwd PATH] [--check-timeout-ms MS] [--max-turns N] [--max-rounds N] [--max-without-progress N] [--cadence-ms MS]
swarm goal show|pause|clear|resume A [--mode eliot|native] [--reason R]
swarm goal round A (--idle-seq SEQ | --slot N) [--mode eliot|native]
swarm discover --profile P [--harness H] [--scope S] [--cursor C] [--archived] [--limit N]
swarm attach --profile P --harness H --scope S --ref R [--project LABEL]
swarm history A --view subject|turns|items [--source native|rollout] [--turn-id ID] [--cursor C] [--limit N]
swarm detach A [--generation N]
swarm provider holds [--after SCOPE] [--limit N]
swarm provider clear-hold SCOPE --corrective-evidence credentials_updated|configuration_updated|owner_verified [--expected-probe-command ID]
swarm plugin NAME | plugin-test ... | hook ... | mcp
```

In particular, `cmd cancel` is not an implemented CLI form. `cmd show`,
`cmd children`, `cmd resolve`, and `turn resolve` are available; `turn show`
remains unavailable. `swarm wait` implements only the frozen `--all`/`--any` set form
above; single-target `--for turn|idle|stopped|goal` forms remain future (`agent.wait`).

`swarm daemon` (background) prints `starting` with the resolved data directory
and log path, then polls `daemon.status` for up to 30 s. It prints `ready`
with the resolved data directory and boot id on success, or fails nonzero
with the logged `daemon.failed` code newer than this start plus the last
bounded sanitized diagnostics and the log path. The tail is read boundedly
(64 KiB, 64 records) from the resolved private data directory only, and only
codes, ids and the existing sanitized diagnostics are printed. `--foreground`
runs the daemon in this process and is unchanged. No ACL bypass and no
automatic store migration or repair occur.

`swarm status` and the start output include the resolved data directory, so
`--data-dir`, `ELIOT_HOME` and default-location differences are visible. When
a client cannot reach the daemon pipe, the error names whether daemon
metadata, lock and log files exist for that data directory and includes the
last `daemon.failed` code from the bounded log tail when present and recent.
It never prints secrets.

`resume --wait` accepts the same `--timeout-seconds` handling as
`launch`/`send`. When a launch or resume with new text fails and its linked
SEND is cancelled as a result, a prompted wait reports the parent start
failure rather than only the linked cancellation.

Only current-layer commands appear in help and Agent guidance. Exit codes: 0 success/
queued; 1 definite failure; 2 usage; 3 pre-effect rejection; 4 unknown; 5 wait timeout;
6 still dispatched/pending; 7 interrupted/cancelled. A queued/applied input is not
successful task completion.

`steer` and `interrupt` do not require `--generation`/`--turn`. When either flag
is omitted the CLI reads the Agent's current generation and active turn, prints
the exact target it captured (`<verb> target: agent=A generation=N turn=T` on
stderr), and submits exactly that binding; an explicit flag always wins over the
captured value, and when both flags are given the CLI submits them unchanged
with no extra read. A missing active turn fails clearly (exit 3) before any
submit. A captured submit that the Store rejects as `stale_generation` is
reported as a race (exit 3, `race:` message naming the submitted binding) and is
never retried against the new turn.

Prompted waits (`send --wait`, and `launch --wait` / `resume --wait` with a
prompt) return 0 only for a positively recognized `success` outcome. Lifecycle
(`Completed`) and outcome stay separate facts: a completed turn still exits
nonzero unless its canonical outcome is success. Supported native failures
(for example Claude `api_error`) exit 1, interruption or cancellation of the
waited task exits 7, and unknown or unclassified terminal statuses exit 4. A
successful `interrupt` command itself exits 0; waiting on the interrupted
original does not. Promptless `launch --wait` / `resume --wait` carry no turn
to judge: they wait for the root command to become `Applied` (readiness, not
task completion) and return 0 then, following the command exit codes above. A
queued/applied input is not successful task completion. Rows written before
canonical outcomes existed classify explicitly from their stored harness and
its recognized native reasons (Claude rows carry the raw terminal reason:
`success` to success, `cancelled`/`interrupted` to interrupted,
`error`/`failed`/`failure` and the `api_error*` family to failure, anything
else to unknown; every other harness carries the normalized lifecycle status:
`completed` to success, `failed`/`error` to failure,
`interrupted`/`cancelled` to interrupted, anything else to unknown); an
unrecognized value never defaults to success. The ACP normal stop reasons
`max_tokens` and `max_turn_requests` end the turn normally: they are success
with their native reason preserved, not task failure (and not Goal fulfilment
either).

`processes`, `stats`, `git-status` and `edits` make one read call and print the bounded
JSON response. They do not start a daemon or submit a mutation. The `--after` strings
for `processes` and `edits` are forwarded unchanged to the Router for validation.
