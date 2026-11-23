# Client API and CLI

Implement only the current [roadmap](../roadmap.md) surface. The first client supports
core Agent commands/events; coordination, automation and service methods are added with
their modules. Do not advertise unimplemented commands.

## Transport and principal identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows uses a current-user-only named pipe; Unix a
mode-0600 socket under the private data root. One daemon owns the data-directory lock.
`daemon.json` records boot id, PID/birth identity, endpoint and diagnostic version.

For the supported [Linux Docker image](../../docker/README.md), use
`docker exec <container> swarm …` to reach its private socket. The Windows desktop
uses named pipes and cannot attach. `workspace_custody` remains `degraded`;
container cgroup membership does not establish exact per-Agent custody.
The existing MCP HTTP listener binds container loopback, which ordinary Docker
port publishing cannot reach. The image exposes no host HTTP/authentication surface.

Every call has an explicit principal. Owner CLI outside a managed Agent/rule environment
reads the private owner token automatically. Agents send id/generation/token; managed
scripts use scoped run tokens; plugins have instance credentials. Missing/incomplete/
revoked credentials reject, never become owner. No manual `--owner` step is required.
Reads are scoped as well as writes.

Before granting Owner authority on a local connection, the daemon checks the OS-reported
client process against its own live Agent custody. Windows uses the client PID from
the accepted named pipe and checks its process handle against the daemon's Agent Jobs.
Linux uses Unix socket peer credentials and checks its Agent process groups and the
client's observable ancestry. Linux custody remains degraded if a descendant has left
its group and its ancestry is no longer observable. Agent tokens keep their ordinary
scope and generation checks.

A match rejects with `not_allowed` and names the Agent. If peer or custody identity
cannot be verified, the daemon also refuses Owner access. A valid Owner credential
proves possession only, so an inconclusive OS check cannot establish that the process
is outside Agent custody. Supported targets are Windows and the Linux Docker image.
macOS and BSD have no supported Owner IPC. Each daemon checks its own custody, so an
Agent can still own a separate private daemon. The local trust boundary remains one OS user.

On Windows, a valid Owner credential suffices when the daemon has no live Agent
custody. Otherwise `GetNamedPipeClientProcessId` must supply a nonzero PID. The
daemon tries to open it with `PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE`.
With a process handle, both zero-time liveness checks must return `WAIT_TIMEOUT`
and the daemon uses `IsProcessInJob` to verify membership. On `ERROR_ACCESS_DENIED`,
it uses the PID from the pipe and the daemon's exact Job member lists. Only a
complete list establishes absence from a Job. An unavailable PID, another open
failure, a failed liveness check, or unresolved membership leaves identity or
custody unknown. A confirmed Job match returns a refusal naming the Agent.

The same rules apply at every Windows integrity level. An elevated Owner
client proceeds when the checks establish that it is outside the daemon's Agent
Jobs. The credential-only path applies at every integrity level when the daemon
has no live Agent custody.

For Agent-originated commands, Store checks the caller's generation and live
identity again in the transactions that admit and dispatch work. A missing,
superseded or terminal generation rejects as `stale_generation`. A current
nonterminal generation without its own credential rejects as `not_allowed`.
Exact retained admission replay returns the existing command before the
new-admission gate. Credentials are still required to reach Store through the
API. Dispatch refusal leaves already-dispatched commands to their usual
settlement paths.

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

For local performance measurement the authentication envelope may additionally
carry `_timing: true` (boolean only). The response then includes a nonsemantic
top-level `_timing.handle_ns`: elapsed time from complete frame receipt through
request decode, authentication and handler completion, excluding response encoding
and transport. `client_call_measured` separately records connect, request/reply
and JSON decode durations. Timings do not enter method DTOs, retained writes or
acknowledgements; absent timing metadata means unmeasured, never zero backend time.
On Windows a busy connect waits for pipe availability within the original call
deadline instead of a fixed polling sleep. Availability never reserves a pipe:
only the connect may repeat, before any request bytes are sent. Send/response
failures retain their ordinary `unknown` outcome and never trigger a new call.

Background `daemon` startup reports `starting`, then waits up to 30 s for
`daemon.status` readiness and reports `ready` with the resolved data
directory and boot id, or the logged `daemon.failed` code with its bounded
sanitized diagnostics.
Windows starts that independent process without inheriting caller handles, so captured
CLI stdout/stderr reach EOF while the daemon remains alive. Its own private IPC and
owned plugin/workspace custody establish subsequent authority.

The desktop uses this owner-only startup path. If its launched daemon exits before
readiness with a configuration error, the startup view shows `config.toml`, the reported
line and a bounded source-free issue, with an action to open that file. Raw config lines
and parser source excerpts are never shown. A readiness response observed during a
concurrent start race takes precedence over the launched child's failure.

### Memory one-shot inference (M1, #199)

M1 implements a bounded subset of Memory Contract 0.19 at
[`c2b7402c`](https://github.com/UnknownAlienHuman/eliot-memory-os/blob/c2b7402c3fedc51ccb3fb881173e028ffcbdac44/docs/v3/INFERENCE-CONTRACT.md).
PR #5325 is merged; Memory main still has that contract blob (checked 2026-11-15).
The closed DTOs, syntax checks and finite limits live in
[`api/inference.rs`](../../crates/eliot-core/src/api/inference.rs), the field catalog.
This increment is Windows-only.

Use the existing private NDJSON transport with exactly
`_auth:{kind:"memory_inference",client_id,token_version,credential}` and
`_payload:<closed request DTO>`. The stable client is `memory:<installation ULID>`.
Persistent random controller id, stable client id, positive credential version and
raw bearer are distinct. Only this principal may call `client.identity`,
`inference.text`, `.get`, `.write.get`, `.high_water`, `.list`, `.result.read`,
`.cancel`, `.release` and `.retention.advance`.
It has no general Agent, command, daemon, config or coordination authority. No
Owner/Agent/Run/tokenless/other-controller fallback exists. Owner maintains credentials
but cannot read inference content through these APIs. Reads select only the
authenticated stable client, never a client argument.

The sole profile `memory-text-v1` maps to fresh Claude Code 2.1.325 print mode with
`claude-haiku-5-5`. Private `<data-root>/inference.json` contains only the absolute
`claude_executable`; the owner provisions a private native OAuth login under
`<data-root>/inference-native`. No API-key helper or vendor routing is inherited.
The Claude plugin owns fixed argv/result decoding; the daemon owns process custody.
Use safe mode, empty built-in tools, MCP tools denied, settings sources empty, one
turn, permission prompts disabled and no session persistence. Prompt goes to stdin.
Set `CLAUDE_CODE_MAX_RETRIES=0`, `CLAUDE_CODE_NONSTREAMING_TIMEOUT_RETRIES=0` and
`CLAUDE_CODE_DISABLE_REFUSAL_RETRY=1`, `CLAUDE_CODE_DISABLE_REFUSAL_FALLBACK=1`; do not
inherit the retry watchdog, fallback model or Eliot credentials. Sources: installed
CLI help and current [CLI](https://code.claude.com/docs/en/cli-reference),
[environment](https://code.claude.com/docs/en/env-vars) and
[headless](https://code.claude.com/docs/en/headless) docs, checked 2026-11-15.
No alternate adapter, account, model or automatic request retry exists.
The refusal switches are also confirmed against the installed 2.1.325 predicates.
Administrator-managed native policy still applies; qualification must establish no
managed hooks or explicit fallback override on the host.

Limits: 32 KiB UTF-8 input, 64 KiB result, 16 KiB byte range, one successful native
turn, a 1–120 second queue/native-attempt budget after input publication, 16 clients, 1,024 retained calls globally
and one native inference in flight. Each client retains at most 1,024
`inference.retention.advance` receipts. At most one cancel and one release binding may
be first-admitted per call; other IDs for that auxiliary operation return
`already_bound`. Replay the original ID. This bounds retained auxiliary writes
until released metadata can be pruned. IDs must use uppercase ULID spelling; no
normalization occurs. Profile, origin, disclosure, input digest/length,
response format, result bound and deadline participate in semantic binding. Prompt
and result text and raw provider diagnostics never enter SQLite, events or logs.
Known credentials in requests are rejected. Ranges return exact bytes; concatenate
them before UTF-8 decoding.

One Store transaction checks `commands`/`writes` conflicts, current credential,
capacity and signed-64 exhaustion; advances the monotonic client sequence; inserts
the immutable write receipt and physical call. Exact retry precedes new-admission,
config and drain gates and allocates no sequence/native work. Another binding is
`id_conflict`. Direct-RPC spelling is literal. Existing 24-hour new-ID expiration
and five-minute future skew apply; retained replay precedes age gates. Never allocate
another ID to recover uncertain delivery.

Exact private input publication/readback precedes durable `queued → sending` and
one native invocation. Possible send followed by timeout, cancellation, lost or
malformed output or persistence loss remains `unknown`, with no provider retry.
Success requires complete successful native output, private result publication,
exact digest/length readback and committed state. Restart changes unsent queued calls
to `failed`, sending to `unknown`, and never invokes the provider. A complete file
without a committed successful call is not readable. The immutable receipt contains
only call id and admission sequence; `inference.get` reads mutable state separately.

`inference.write.get` is read-only over `writes`: exact retained own method/digest/
original result, definite absent live ID, or `history_gap` outside the live window.
It never admits/adopts a mutation. Foreign principal/command IDs reject. A faulted
or unavailable Store is an error, never absence proof. Result reads verify the full
own file against exact digest/length before slicing.

Cancel admits a fresh mutation ID and cancels only queued work; otherwise its
immutable outcome is `too_late`. Release binds the exact own call and disposition:
`memory_copy_verified` requires its exact successful result digest;
`terminal_without_result` requires failed/cancelled state; `abandon_for_erasure`
fences any state. Commit the publication fence first, then clean private input/result
copies after the owned runner is quiescent. Exact release replay retries cleanup;
`inference.get.cleanup_complete` is separate readback. Late settlement cannot publish
through the fence. Local cleanup never proves provider deletion/non-execution.

Release carries Memory's exact-call `backup_dependency` classification:
`no_backup_dependencies`, `controller_closed` or `controller_dependent`. The last
returns `dependency_protected` without admitting a write or fencing publication.
Supply exactly one assertion. The original `no_backup_dependencies:true` spelling
remains accepted with the other field absent, preserving exact retained M1.1 release
digests/retries after upgrade; it cannot express backup-aware release.
Memory rechecks its current backup-set snapshot under mutation-owner/backup-set
serialization immediately before send; controller-closed means the backup contains
every terminal fact/body it needs with no later controller readback/copy/cleanup debt.
An unresolved fence is a dependency until Memory reconciles it. The controller trusts
this authenticated assertion; it never opens backup files or stores backup-pin tables.

`inference.high_water` captures the client's monotonic call and write sequences plus
their retained cuts in one committed read. `inference.list` pages fixed exclusive/
inclusive intervals for both streams, with a closed cursor bound to client, both
intervals, page limit and query version. Membership is fixed; call state can settle
between pages and is read separately with `inference.get`. A request below either
retained cut returns `history_gap`, including on continuation after concurrent pruning.
Pages have finite limits defined by the Core DTOs and never include private content.

Memory closes/drains its own mutation owner before a backup cut, dispositions every
created call/auxiliary ID with `inference.write.get`, captures high-waters, and holds
admission closed until its final fence publication/readback. It records admitted,
retired-not-sent and unknown IDs, and classifies admitted calls as controller-closed
or controller-dependent per Memory Contract 0.19 sections 8-10 and Implementation §8.
On restore, preserve installation/external-ID state outside rollback roots, capture
one current high-water and page the fixed post-backup intervals. Never use a mutation
to probe an old absent ID, adopt post-cut output as restored cognition, or send a
retired ID. Missing identity/history/copy or unresolved effects keep inference closed.

`inference.retention.advance` binds an idempotent mutation to observed high-waters,
Memory-asserted safe cuts and bounded work. Cuts are monotonic and cannot exceed the
observed bounds. Memory's safe call cut is no greater than the minimum high-water of
published non-retired backups and unresolved fences, or an observed controller
high-water if none remain; its safe write cut likewise protects needed auxiliary
readback. Advancing a cut permits gaps at/below it, never above it. Only released,
cleanup-complete calls prune. Open/unknown/unreleased calls remain, regardless of age.
Pending/previous credential readback also retains pre-rotation calls until retirement
completes; rotation requires at least one retained own call to prove the new credential.
Write receipts prune only after the normal new-ID retry window has expired and no
retained call references them. The pruning transaction persists a nondecreasing,
inclusive expiry timestamp watermark per client (current time minus 24 hours minus
one millisecond). Absent IDs at or below that watermark read `history_gap` and
first admission rejects with `id_expired`, even after clock rollback or restart.
Retained exact replay precedes the watermark and age gates for every mutation.
Schema v40 initializes the watermark from the greatest retained v39 retention
receipt's pruning time, or leaves it unset when no such receipt exists.
After bounded pruning, a first retention operation that would exceed the per-client
receipt cap returns `capacity` and rolls back the transaction. Exact replay, cancel
and release remain available at that cap. Ordinary retention continues to exclude
inference writes. The global retained-call cap counts all remaining calls and rejects admission
at capacity; pruning never resets either sequence. Embeddings and owner emergency
erase remain `unsupported`.

#### Private credential publication and recovery

Only Owner calls `inference.client.install`, `.rotate`, `.promote` or `.retire` with a retained
mutation ID, stable client id and expected current version (zero for install).
Generate 256 random bearer bits with the existing credential generator; no derivation
or routing-key hierarchy. Store version/hash and nonsecret publication metadata only.
Publish raw secret in a fixed daemon-owned private file
`inference-credential-<operation-id>.json`; accept no caller path. Its closed record
contains controller id, client id, token version, operation id and credential.
Atomic private temporary-file write, file sync, same-directory write-through rename
and exact readback precede the client/write transaction. No bearer enters RPC,
semantic data, receipts or logs. The returned private filename is handed to Memory
through its private local setup boundary.

An interrupted pre-commit publication is reused only for its exact operation/client/
version/controller binding. Retained install/rotate replay reconciles the same private
file against the installed hash; missing/corrupt committed publication is explicitly
unknown, never permission to mint another bearer/version. Keep the file for recovery.
Rotation stages one pending version while current remains installed. Pending auth
may read old own calls but cannot first-admit mutations. After it authenticates and
reads a pre-rotation call, Owner promotes it with a separate idempotent mutation.
Keep at most one previous read credential. Only current admits new mutations;
retained own reads/retries survive overlap. Previous retirement requires a daemon
restart after promotion, an exact pre-rotation call read authenticated with the new
current credential after that restart, and release of that same call with cleanup
complete. This proves reconnect and old-call/restart readback before revocation.
Owner's `.retire` binds the current expected version and one retained operation ID.
Exact old-file/hash readback precedes a committed revocation fence; only then delete
the fixed private file and confirm absence before committing cleanup completion and
returning the immutable response. Same-ID retry resumes incomplete cleanup, including
after restart, without minting a credential or revoking another version. Previous
auth fails after the fence. Further rotation waits for completed retirement. A retired
install/rotate publication cannot recreate its deleted secret on replay. Lost replies
never reset versions. The advertised capability is `0.19-M1.2`.

### MCP stdio server

`swarm mcp` is one local stdio server and never starts the daemon. Each stdio
bridge uses two async runtime workers, independent of the host's core count;
the on-demand blocking pool remains available for stdio and IPC. Daemon and
StreamableHTTP runtime sizing is unchanged. It exposes exactly one
tool, `swarm`, with required `method` (non-empty string) and `params` (object), and
forwards calls through the existing authenticated local client. Ordinary mode uses
[`eliot_daemon::client_call`](../../crates/eliot-daemon/src/daemon.rs); `--require-agent`
uses [`eliot_daemon::client_call_with_agent_identity`](../../crates/eliot-daemon/src/daemon.rs).
Ordinary stdio authenticates with `api.identity` before MCP initialization so the
daemon's local Owner refusal is reported when the bridge connects.
Both reuse the daemon methods and shared Core DTOs. Call known methods directly.
For discovery, `api.identity` gives the caller's identity and scope, `api.methods`
finds callable names, and `api.help` describes a canonical method. Only
`--require-agent` stdio includes standard `initialize.instructions` pointing to
the `swarm` tool and the bundled `eliot-swarm` skill.

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
usage error. StreamableHTTP uses its separate remote identity checks.

A mutation call may omit `params.id`: the MCP server mints one fresh ULID for that
call and forwards it to the daemon. Both success and structured daemon errors return
the minted id as top-level `id` in the tool result. To recover if the entire MCP response
is lost, supply a stable `params.id` before the first attempt. An explicit id is forwarded
untouched and still validated by the daemon. A retry must carry the same id, principal,
method and semantic payload;
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

Each authenticated Owner mutation records an `owner.submitted` event with
its method, mutation ID when available, and the PID and executable path
reported by the OS in `data.owner_process`. The image path is bounded to
4096 UTF-8 bytes and marked if shortened. Fields are null when unavailable.
For cloud MCP the process is the local bridge, and `remote_call` identifies
the cloud caller. Commands and queued launches retain the original submission
metadata on admission. A retry has its own submission event and preserves
earlier admission events.
If the Store cannot accept the event, the daemon records a bounded diagnostic
with the submission's process metadata.

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

Commands store the binding directly; other writes use the durable `writes`
receipt defined by the [Store migrations](../../crates/eliot-store/src/schema.rs).

The writer checks both command/write namespaces. Later linked inputs, selectors and
rule actions have bounded derived ids. No general receipt/floor framework.

`goal.round` reserves its id in this same write namespace with the durable round
marker before running its check. Its original response or sanitized error is
retained; exact retries resolve it before current Goal state, without repeating
external work. A live duplicate is `busy`; an abandoned holder reads `unknown`.
See [Goal loop](automation.md#goal-loop) and the canonical
[Goal request/response DTOs](../../crates/eliot-core/src/api/goals.rs). The round
response binds the original mutation id separately from the distinct continuation
command id frozen at claim. The settled event's persisted `kind` is exactly
`goal.round`. Stored Goal responses/round records use Core
[`MAX_FRAME_BYTES`](../../crates/eliot-core/src/limits.rs), not the event budget.

## Methods

| Method | Purpose |
| --- | --- |
| `api.methods/help/identity` | bounded authenticated method discovery, compact DTO fields/examples, and credential-free caller scope |
| `daemon.status/shutdown` | owner-only current state/admission and exact drain/force |
| `autonomy.get` | Owner-only windowed autonomy counts and source coverage |
| `maintenance.run` | owner-only immediate bounded retention pass reporting pruned counts |
| `maintenance.purge_legacy_attention_advisories` | owner-only dry-run count and exact-count purge of owner-sent legacy advisory-attention mail |
| `agent.list/get/result` | scoped, paginated Agent state and exact results; `agent.get` includes metadata-only owner-rules revision/digest/delivery evidence |
| `agent.processes/stats` | scoped process records and ephemeral OS samples |
| `agent.git/edits` | current Git snapshot with complete work line counts when available, and paginated native-attributed edits |
| `agent.patch_digest` | review digest of the Agent's patch against its worktree base (else HEAD): changed files and packages, new dependencies, public-interface lines, checker/lint configuration changes, suppressions and paths outside active claim globs, each list bounded; an attention filter, not a quality score |
| `workspace.list` | owner-only page of the managed worktrees Swarm created |
| `command.submit/get` | implemented immutable admission and command readback |
| `turn.get` | implemented running/completed/unknown turn read |
| `swarm.wait` | bounded wait over a frozen child command/turn set with all/any, timeout and honest per-entry outcomes |
| `swarm.digest` | durable digest over a frozen child set delivering one coalesced parent mailbox result with the standard mail wake |
| `command.resolve`, `turn.resolve` | constrained Owner resolution of unknown outcomes; failed handoff brief may be Owner-applied |
| `swarm.switch` | Owner launch-plus-handoff to a new Agent, using normal launch and the H2 transfer |
| `swarm.handoff`, `command.cancel` | atomic task transfer to an existing Agent; Owner or issuing-manager withdrawal of undispatched SEND |
| `command.children` | implemented scoped, paged child readback for a retained expansion |
| `project.list/get` | owner-only read of registered project authority |
| `launch.enqueue` | Owner or Agent submits an explicit durable launch intent |
| `launch.intent.get` | Read the exact intent and its current state |
| `launch.queue` | Read one page of scoped interactive intents and assignment waits |
| `project.budget.set` | Current manager adjusts free budget or priority within the Owner's grant |
| `project.overview` | Owner or Agent read of one project team or every team with workspaces, queue, capacity and whole-fleet totals; a manager sees its own team in full and aggregates for other projects |
| `rule.list/get/catch_up` | Owner-only paged rule state and definition list, one loaded rule state, and finite committed-history catch-up |
| `rule.read/save/delete` | Owner-only rule source read, revision-guarded create or replace, and revision-guarded delete through the [rule file editor](automation.md#rule-file-editing) |
| `profile.list` | owner-only sanitized configured-profile summaries with launch-time resolvability |
| `harness.list/models/quota` | owner-only installed harness inventory with versions, per-harness read-only native catalogues and provider-supported quota readback; no model turn or inference |
| `models.catalog` | Owner-only bounded, paginated search across installed harness catalogues; request, entries, failures and quota vocabulary: [canonical DTOs](../../crates/eliot-core/src/api/harness.rs). Membership never proves availability: affirmative fresh native quota and complete hold readback are required. Failed harness reads remain visible; a partial listing supports continuation but the next first page retries failed reads. An `after` cursor that is not in the current listing (the models changed between pages) is an `invalid` error with the message `catalogue cursor is no longer in the listing; restart from the first page`; it is never a restart from page one. Executable/version-bound caching and concurrent-read coalescing belong to the [catalogue owner](../../crates/eliot-daemon/src/router/catalog.rs). |
| `script.list/read/save` | Owner-only scripts under approved Run roots: cursor/encoded-byte-bounded list (one root scan per page; continue through empty pages until `next_cursor` is absent), verified-handle bounded read, and serialized revision-guarded replacement. Complete secret-bearing entries are withheld; raw and decoded TOML/JSON keys/values are checked. Saves never execute; external-writer conflict limits and editor environment are specified in [automation](automation.md#script-editing). DTOs/budgets: [`script_edit.rs`](../../crates/eliot-core/src/api/script_edit.rs). |
| `settings.read/save` | owner-only unified section read/save, including revisioned `owner_rules.text` and budget warnings, revision CAS, dry run, restart indication and bounded author history; see configuration |
| `project.transfer` | Owner-only transfer intent, optional replacement launch, atomic commit or cancellation with typed readback. See [Project transfer](coordination.md#project-transfer). |
| `project.assignment.get/claim/launch/release` | Current registered project manager, explicit adoption or manager launch, and revision-bound release through the ordinary command path. See [project manager assignments](#project-manager-assignments). |
| `chat.organization` | owner-only grouped work projection by project assignment, manager, repository/worktree and ancestry, with event-watermark and per-group readback; no client repeats Git discovery or grouping. See [Chat organization](#chat-organization). |
| `chat.attach` / `chat.detach` | owner-only chat placement moves between a retained project and the no-project bucket; optional per-Agent `expected_revision` refuses a stale write with `conflict`; committed per-Agent link readback; execution scope, workspace and history untouched |
| `chat.tag` | owner-only replacement tag groups for Agents or chats (an omitted tag list clears); optional per-Agent `expected_revision` refuses a stale write with `conflict`; committed per-Agent link readback |
| `project.discover` | owner-only opened Git checkout registration through the settings writer; non-Git paths return no project |
| `routing.preview` | owner-only advisory `RoutingDecision` for a role, computed like admission without admitting, freezing or reserving; see configuration |
| `timer.get/run` | owner-only timer read and idempotent manual firing |
| `goal.set/get/pause/clear/resume/round` | owner-only Eliot-mode Goal with explicit and automatic idle rounds plus explicit cadence slot rounds; automatic driving needs `max_turns` or `max_rounds`; native mode is `unsupported`; `max_cost` needs the Agent's native session cost in its unit |
| `provider.holds/clear_hold` | owner-only provider hold readback and constrained correction |
| `events.bounds/read/subscribe` | journal bounds, persisted replay and bounded live delivery |
| `agent.discover` | owner-only native metadata discovery, without creating Agents |
| `agent.history` | owner-only bounded history of an exact observed subject |
| `mail.send/inbox/read/ack`, `var.set/get` | addressed queued mail with separate acknowledgement, shared variables with CAS |
| `swarm.reparent` | Owner-only atomic subtree or all-children manager change; generation-bound, no native effect |
| `who`, `claim.set/list/release` | current-worker file areas plus advisory claims with deferred conflict details |

Canonical public wire types are re-exported by
[`eliot_core::api`](../../crates/eliot-core/src/api.rs). These DTOs and their validators
define field names, JSON shapes and grammar; this spec owns behavior and authority. Principal
authentication precedes method parameter validation.

### Project manager assignments

Owner may read registered assignments, name an eligible local manager, launch a
manager through ordinary launch, or release by expected epoch. A current Agent
may read within its registered project and release its own assignment by expected
epoch. An Owner grant permits eligible Agents to claim themselves. Launch uses
the Owner principal in 2a. A competing claim or
launch returns the occupied assignment with a typed reason. Mutations retain
their outcome under the existing operation ID and principal. A read returns the
latest record, including its current workspace and start command ID when present.
A confirmed preparation failure or cancellation leaves a vacant record. An
unknown preparation reads as blocked, with the outcome on the start command.
Owner release revokes that assignment at its epoch and retains the preparation
receipt and unknown outcome. Remote assignment is
unsupported. CLI callers use `swarm project assignment get|claim|launch|release`.
`ProjectAssignmentLaunchRequest` accepts optional `no_policy: String` and
`goal: Map<String, Value>` fields with the same reason and field-by-field Goal
semantics as ordinary launch. The CLI exposes them as `--no-policy REASON` and
`--goal-json JSON_OBJECT`. Only an Owner may override `check`, `check_cwd` or
`escalation`; explicit safe fields and opt-out remain available to permitted launch
principals. An explicit Owner claim supplies the Agent ID and generation. A scoped claim
omits the manager to select the caller. The project action
`project.assignment.claim` grants self-claim under the configured profile allowlist.
For transfer, use `swarm project transfer <project> --revision <revision> begin|launch|commit|cancel`.
The request and readback use [ProjectTransferRequest and ProjectTransferResult](../../crates/eliot-core/src/api/project_transfer.rs).

A managed manager launch always runs in a project worktree. Without an explicit
`adopt_worktree` the daemon prepares a fresh opaque worktree on a unique
branch from the frozen base OID and runs the existing preparation and setup chain
before native dispatch. Workers inherit that worktree through the parent link by default.
`adopt_worktree` names an existing checkout of the same repository explicitly;
adoption verifies Git identity and live workspace overlap, and freezes the profile
without running its setup. Cleanup and worktree removal keep bound or borrowed
worktrees. Releasing the assignment clears its binding.
CLI callers add `--adopt-worktree PATH` to the launch form when adopting.

For ordinary commands, a supplied `args.assignment_epoch` is a precondition when
the verb accepts it. Admission records the current binding even when it is
omitted. See [coordination](coordination.md#project-manager-assignment) for
generation, epoch and dispatch behavior.

`project.overview` returns exact fleet totals beside authorized project and
native-child detail. A `project` filter scopes the returned detail. Each
reply has an event watermark from its committed Store snapshot and a separate
native observation coverage label. When `next_cursor` is present, pass it as
`after` with the same filter. Each page applies current read authority and
reports its own snapshot watermark. CLI callers use
`swarm project overview [--project ID] [--after CURSOR]`. See
[project manager assignment](coordination.md#project-manager-assignment) for
detail visibility and capacity semantics.

### Chat organization

The owning semantics are [Coordination: Conversation association](coordination.md#conversation-association).
Owner only, with identical behavior from the CLI (`swarm chat ...`), MCP
(`chat.organization`, `chat.attach`, `chat.detach`, `chat.tag`) and the desktop
(its sidebar projects, and a task's Attach to project, Detach from project and
Tags).
Organization is placement, never authority: attach, detach and tag write the
organizational overlay beside the Agent id through Owner-only Store
transactions (`chat_organization` with placement, tags and a link revision
beside the Agent id, schema v64 and v65, plus the switch's display-only
`continued_from` pointer, v66, which these writes preserve); the execution project, scope,
workspace and history of every Agent keep their retained values.

`chat.organization` (`ChatOrganizationRequest`/`ChatOrganizationResponse` in
[`organization.rs`](../../crates/eliot-core/src/api/organization.rs)) groups
work by project assignment, manager, repository or worktree, and ancestry, and
returns the ready projection with event-watermark readback, so no client
repeats Git discovery or grouping. The optional `project` reads back one
registered project through an index-scoped member query with its exact total
from the same scoped predicates; `archived` includes
archived rows (default hides them). Groups carry at most 200 members each and
1,000 across the projection. The all-project read pages its group list: `after`
takes the previous page's `next_after`, and a truncated page sets
`groups_truncated` with the cursor for the next page. Member pages and totals
apply the default `agent.list` archived visibility and the same group key as
its `organization` field, so a listed row is counted in the group it reads
in. Optional `query` applies the `agent.list` search haystack to member
pages and every group's `member_total` alike, so header counts follow the
search; registered groups still read back their headers with zero totals
when nothing matches. A legacy Agent whose retained project label is a
checkout path groups under the registration of that exact path, without
rewriting the label; a path two registrations share stays unplaced. Only
trailing separators are normalized everywhere. On Windows the `\\?\`
verbatim prefix is dropped, `\` and `/` compare equal and ASCII case is
folded; on other platforms case and a literal backslash stay significant.
A registered group carries its header in the same read: the manager (with
its managed worktree `branch`), the assignment, `needs_assignment`, and its
exact team `totals` and project slot use (`capacity`), the numbers
`project.overview` reports for it, so the desktop sidebar reads every header
and total in one bounded read. Competing roots surface as an explicit
ambiguity only beside an assigned manager: every other retained root competes
with it (`conflicting_roots`, counted independently of the archived filter),
and none is auto-reassigned. Without a manager the roots wait for assignment
and the group reads `needs_assignment` alone. A registered group carries the
open `project.transfer` intent as `transfer` while its manager drains. Chats keep their retained
execution labels and group by their overlay link alone; a legacy chat that
carries an execution project reads in the no-project bucket until attached.

`chat.attach` places 1-16 chats under a retained project; `chat.detach` returns
1-16 chats to the no-project bucket. `chat.tag` replaces the tag groups of
1-16 Agents or chats with at most 16 trimmed nonempty tags of at most 64 bytes
each; an omitted tag list clears every tag. The batch bound keeps the worst
nominal receipt (every link with a full-length project and a full tag set)
inside the 64 KiB retained-result bound. Every mutation carries an id:
exact replay returns the committed result before current-state checks, and the
response carries the committed per-Agent links (`id`, `links:
Vec<OrgLinkView>` with placement, tags, the new link `revision` and any
`continued_from`). The
optional `expected_revision` carries, per selected Agent in order, the link
revision it was read with (`agent.list` `organization_revision`, 0 before its
first organization write); a write whose revision moved since, by any caller,
is refused whole with `conflict` and changes nothing. The desktop menu always
carries it; the CLI omits it. Each fresh write appends one
`organization.changed` event (`OrganizationChangedEvent`) in the same
transaction, naming each written Agent with the group its `agent.list` row
now reads in, its tags and its link revision, so a client refreshes the rows
it already loaded by id; the projection's event watermark advances, and the
router publishes it after commit. Rename
stays the desktop
`[desktop] display_names` presentation preference and archive stays
`agent.archive`. Errors include `invalid`, `not_found`, `not_allowed`,
`conflict`, `bad_state`, `unavailable`, `id_conflict`, `source_integrity` and
mutation binding/age errors.

### Authenticated method discovery

`api.methods` lists only currently registered methods callable by the authenticated
principal. `query` is a case-sensitive substring of the canonical name or summary;
`after` is the last returned canonical name for the same selection. Results are sorted
by canonical name and contain at most 50 entries. `full: true` adds one-line summaries.
`next` is null at the end. Owner-only methods are absent for Agents. Suggestions for
unavailable methods use the same callable set. Owner-only read denials tell Agents to use
`api.identity`, `who` or `agent.get` on themselves for scoped information; other Owner
data is not needed for Agent work. Run credentials retain their existing `command.get`-only
restriction and cannot discover methods. Discovery never changes authorization checks.

`api.help` is compact by default: the method summary, the sorted compact request field descriptors
(requiredness, types, enums, nested shapes and documented constraints, derived from the request DTO, never a copied catalog), one runnable example and the
limits. `full: true` adds aliases, scopes, capability notes and the full request and response JSON Schemas, generated by
`schemars` from the shared Rust DTOs using deserialize/serialize settings respectively.
Invalid `agent.get` requests include a runnable Agent ID example. Custom field
constraints and byte bounds are documented beside the
owning DTO fields
(byte limits use `x-maxBytes` / `x-maxEncodedBytes`; `x-nonBlank` means nonempty after
Rust Unicode whitespace trimming). Router-local descriptors supply only
the summary, example, mutation rules, scope/capability limits and CLI aliases. CLI aliases
are not RPC names. Every registered Router method has a descriptor. Help is capped at
256 KiB; it never includes credentials or another principal's identity. Examples naming
existing resources require actual retained IDs, profiles, sources or views. Direct
callers replace the illustrative mutation ID with a fresh ULID for a new write.
Compact MCP examples omit `id` when the bridge can mint it.

`api.identity` returns the authenticated principal kind, Agent id/generation when present,
and its allowed scope patterns. An Agent's variable scope defaults to its frozen
project authorization scope. Mailbox access is self-only. Owners explicitly select variable scopes and mailboxes.
Help's `scopes` describes only coordination authorization scopes relevant to the
requested method, rather than inheriting the caller-wide list from `api.identity`.
A mailbox scope denotes the `agent` mailbox selector, not a `scope` request field.
Other methods return an empty list; native, process and provider selector values
remain described by their owning request DTO fields.
Unsupported mail presentation modes, tree variable scopes and native Goal controls
are described as unsupported, not advertised as available native capabilities. Use
`agent.get` for target-generation capabilities before `command.submit`.

Malformed fields return Core `Error` code `invalid`, with a bounded `details.field`
path and a compact `details.expected` JSON Schema fragment. The safe `message` names
the field once (for example, `invalid parameter: params.id`) and does not repeat the
schema. DTO-owned validation retains its existing error code (including `unsupported`).
Nested action DTO failures retain `params.args.<field>` for command submission and
`rule[<index>].do[<index>].<field>` when loading authored rule files. Rejected values,
unknown user-supplied keys/variants and whole payloads are never echoed. Unknown keys
are reported as `<unknown-field>` with `additionalProperties: false` and bounded
`allowed_fields` and `required` lists for the owning DTO. `fields_truncated` marks
an incomplete list. Use `api.help` to inspect the full DTO. MCP error forwarding is a separate
transport concern.

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
`busy`, `unavailable`, `auth_required`, `capacity_unavailable`, `provider_held`, `history_gap`, `source_integrity`,
`secret_in_payload`, `frame_too_large`, `mailbox_full`, `version_conflict`, `claim_limit`, `internal`.

`auth_required` on a failed Claude start means the native CLI reported it
was signed out before any session or prompt was dispatched. The retained
message names the executable and the command the owner can run to sign in.

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

`owner.inbox` gives the Owner one decision queue over retained Store rows.
`OwnerInboxRequest` accepts an optional opaque
`after` cursor and `limit`. `OwnerInboxResponse` returns `items` and an optional
`next` cursor. Each item carries a snake_case kind, subject, subject revision,
age in milliseconds, one-line reason, optional recommended action and offered
choices. An action names the existing method and JSON arguments that implement
the choice. The projection includes harness requests, unknown command/turn
outcomes, owner-required holds, unarchived lost/failed Agents, blocked Goals,
launch items waiting for the owner, and #871 stuck objects. For a stuck Goal
(`active_goal_without_round_or_timer`, #871, #904), the item offers a ready
`goal.round` action (`round`) as the recommended choice alongside `goal.get`
(`inspect`) when the Agent is cleanly idle with an unconsumed key or due cadence
slot. Recommendations
are derived from retained facts. Send `owner.decide` with the item kind,
subject, revision, and an offered choice name, `recommended`, or `snooze`.
The daemon checks the revision under the Store writer when it admits the
selected mutation. For every kind, the revision binds the subject facts, reason,
offered choices, and recommendation, including the methods and arguments. An
item gets a new revision when a recommendation appears, changes, or disappears.
A stale item returns `stale_item`. A snooze lasts until
the next event on that subject. Supply required inputs in `args` to fill the
choice's displayed input fields. The same decision ID and payload reuse the
same inner mutation ID. [Owner inbox DTOs](../../crates/eliot-core/src/api/owner_inbox.rs)

Each stuck command has its own `command:<id>` subject, revision, snooze and
decision receipt. It offers `command.get` (`inspect`). The available corrections
are described in the [monitoring spec](monitoring.md#liveness).

`owner.decide.bulk` applies each item's own recommended action for an explicit
ordered set of 1–100 `OwnerInboxItemRef`s, with at most 16 KiB of encoded refs.
It is Owner-only. The batch ULID is recorded against the exact ordered refs
before any item is decided; reusing it with changed refs conflicts. Each item
uses a deterministic `owner.decide` mutation ID derived from the batch ULID and
its complete kind/subject/revision ref, so exact retries replay the same inner
decision. The item revision binds the projected reason, choices and exact
recommended action. Each ref then goes through the ordinary `owner.decide`
authority and Store revision check. Changed or missing items and items without
a recommendation are returned as skipped. Other items continue after a
failure; the compact response groups applied, skipped and failed refs, and
marks failures whose action outcome may be unknown. Retrying the exact batch
does not replay uncertain external effects.

`swarm inbox` prints Owner decisions in text. With `--json` it emits the
response DTO. An Agent's `swarm inbox` reads its own mailbox. Use
`swarm msg inbox` or the existing mailbox selectors to read mail explicitly.
Copy the printed item reference into `swarm inbox decide ITEM CHOICE`.
Use repeatable `--arg KEY=VALUE` flags for displayed inputs such as
`choice_id` and `feedback`. Pass JSON for `answers`.
`swarm inbox snooze ITEM` hides the displayed revision.
`swarm inbox apply --kind KIND` snapshots current refs of that kind across the
Owner inbox pages and submits them as one bounded batch (at most 100 refs); if
more than 100 match, it refuses before deciding any item. If the selection
includes a Stop recommendation, the kind-wide form requires `--yes`. Use
`--after` to narrow the selection or pass reviewed refs with repeated `--item
KIND/SUBJECT@REVISION` options. Explicit refs already identify the choices the
Owner reviewed and may be submitted without `--yes`. The explicit form also
submits stale refs so the daemon can report their skip reason. The root
`--id` is the batch ULID and must be reused with the same ordered refs for an
exact retry. `--json` emits the aggregate response.

The view calls use the Core view DTOs linked above. `daemon.status` is owner-only
and accepts an empty object. Its bounded blocker/command lists retain truncation flags;
unknown commands remain unknown. Agent list observations appear only when requested.
`owner_inbox` counts visible items by kind. `liveness` counts the current
Moving, Asked, Parked, Stuck, and Long classifications and groups Parked,
Stuck, and Long objects by reason. See the [monitoring spec](monitoring.md#liveness)
for the evidence used to classify a wait. `command_without_dispatcher` reports
the count and oldest command ID/age from the same current Store projection,
using the inbox's reportability confirmation and counting before snooze filtering.
Human `swarm status` renders that count and oldest command. The field is absent
when talking to a daemon version that predates this projection.
Absent source cursors and sequence/acknowledgement values remain null.
When a previous daemon panic marker was consumed once at startup or plugin panic markers exist, `daemon.status` exposes `previous_crash` and `plugin_crashes`, and human `swarm status` renders one summary line for each.
For a missing pinned executable, `harness.doctor` returns `profile_command_issues`
with the profile, harness, configured path and optional `replacement` path and
version. `swarm doctor` tells the Owner to omit `cmd` to restore automatic discovery.
`daemon.status` uses the same field for missing pins without running installation probes.

On Windows, `swarm doctor` lists non-baseline ACEs on the selected data root. It
reports SID, ACE type, rights mask, and flags and identifies the capability ACEs
accepted by the directory rule. When an ACL deviation prevents Owner startup or
RPC, doctor includes a local read-only ACL report with the error. The inspection
reads Windows ACL metadata before any endpoint or credential access.

`autonomy.get` is an Owner-only read for CLI, desktop and authenticated cloud
MCP callers. Its [Core request and response](../../crates/eliot-core/src/api/autonomy.rs)
use Unix milliseconds. Omit `since_ms` for the last 24 hours. An explicit
start must be between the Unix epoch and the daemon's current time, inclusive.
The return value gives the inclusive window used and one coverage label per row.
The [monitoring contract](monitoring.md#swarm-autonomy) defines the sources and
counting rules. `swarm autonomy` renders the same DTO in text or with `--json`.
`--since` accepts an RFC 3339 timestamp, Unix milliseconds, or a relative duration
such as `48h`. This read requires a running daemon.

`daemon.status` includes `build_identity`: the package version with a short Git
revision suffix (or `git-unknown` when built without an available repository),
the full compile-time `git_sha`, the parent directory of the running executable,
and its exact SHA-256. The version adds `-dirty` for tracked Git changes,
including staged changes, unstaged edits and deletions. A failed status query with
a valid `HEAD` adds `-tree-unknown`. Cargo watches the worktree index and the
tracked paths from `git ls-files -z`, so edits and dirty-to-clean transitions
refresh the marker even when `HEAD` stays the same.
The daemon computes the executable hash once at startup and reuses that snapshot
in status and the `daemon.build_identity` startup log record. `git_sha`,
`executable_directory`, and `executable_sha256` are null when their source cannot
be read; an archive build does not inherit an enclosing
repository's commit. `swarm --version` and `daemon.json`'s `diagnostic_version`
carry the same short revision suffix and literal `-dirty` marker. Human `swarm
status` prints the identity in one line; JSON status retains the typed
`build_identity` object.

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

`daemon.status` also reports the retention view: the `eliot.db` main and WAL
file sizes in bytes, the effective event and exited-child process windows,
the optional command and mail windows, and the retained-event watermark
`pruned_through`. The event and exited-child windows have finite defaults.
When `config.toml` cannot be read, status stays available with a redacted
`config_error` (`code: message`) and unknown window values. See
[configuration](configuration.md#daemon-and-automation-settings) for the defaults
and the older-Store conversion.

`maintenance.run` is owner-only and accepts an empty object. It reloads the
daemon retention settings and runs one bounded retention epoch (the periodic
tick's row and time budgets), so it returns inside the client call deadline; a
larger backlog continues through periodic maintenance. It returns the Core
[`MaintenanceRunResponse`](../../crates/eliot-core/src/api/maintenance.rs)
pruned counts and the retained-event watermark `pruned_through`. A rejected
settings reload fails the call instead of pruning under stale windows. The
manual pass adds no new retention semantics; the contract stays in
[core](core.md#retention).

`maintenance.purge_legacy_attention_advisories` is owner-only. An empty request
is a read-only dry-run that returns the exact number of owner-sent messages whose
thread matches `attention-*-advisory`, including both read and unread rows. To
apply the purge, the owner supplies that number as `expected_message_count`; the
Store rechecks the count and deletes the matching rows in one immediate
transaction. A changed count returns a conflict without deleting anything, so
the owner must run the dry-run again. The CLI command
`swarm maintenance purge-legacy-attention-advisories` previews by default;
`--apply --expected-count <COUNT>` explicitly applies the preview. The filter
preserves regular mail, non-advisory attention escalations, and messages not
sent by the owner. This operation does not change normal mail retention.

`project.list` and `project.get` are owner-only readbacks of the owner-registered
canonical checkout, repository common directory, explicit profile/action allowlists and
optional Git fetch approval. They do not create or modify registrations. Their project
values use Core [`ProjectRegistrationView`](../../crates/eliot-core/src/api/agent_views.rs),

whose optional `git` value uses [`ProjectGitView` and `GitCredentialHelper`](../../crates/eliot-core/src/api/agent_views.rs).
The read-only `origin` is the validated basename of the local `origin` remote's path,
with `.git` stripped. Malformed and pathless remotes produce no name. The daemon
reads each common directory in the Git probe's sanitized environment after releasing
the Store lock. The readback carries only the name. The
`ProjectRegistrationView.policy_usage` field carries
`applied`, `total`, `opted_out` and `opt_outs` for the last 24 hours. `total` counts distinct
retained launched Agent IDs, including stopped Agents; attaches and resumed
generations do not add another launch. `applied` counts those Agents that inherited
at least one project policy default. `opted_out` counts explicit opt-outs and `opt_outs`
returns the newest 16 with their `agent`, `reason` and `created_at`. Human `swarm project show` prints the line
`policy applied to N of M Agents in the last 24 h` and the opt-out reasons. JSON keeps
the DTO, and the desktop receives the same project data through its existing API client.
Owner-facing views render Windows paths without the verbatim prefix, through Core
`display_path` ([verbatim.rs](../../crates/eliot-core/src/verbatim.rs)): the registration
`checkout`, `repo_common_dir` and every `checkouts` path, and the Agent views'
`workspace`, `repo_common_dir` and the joined Git observation's `workspace`.
The Agent `project` and `project_scope` fields retain their exact stored spelling,
including path-valued identities, so clients can reuse them as list filters. The human
CLI formats a path-valued project for display. Store rows, Git lookups and path-keyed
tables keep their exact stored spelling.
The bounded `checkouts` inventory carries live verified paths, main-checkout identity,
and a branch when a bound `symbolic-ref HEAD` supplies one. A linked entry requires a
reciprocal Git registration in the pinned common directory. A list can carry the
inventory on one registration per repository even when multiple registrations share
it. Omitted entries give no live checkout proof. The desktop joins that inventory to
the exact retained Agent `workspace` for project and subproject bulk operations.

`workspace.list {after?, limit?}` is an owner-only page of the managed worktrees
that `launch --worktree` prepared and that are not yet removed, read from the
retained workspace preparations (never inferred from Agent project labels), ordered
by Agent ID. Rows use Core
[`ManagedWorkspaceView`](../../crates/eliot-core/src/api/workspaces.rs): Agent,
project, source repository, branch, worktree path, preparation phase and update
time. Like `agent.list`, the response carries the required `total` ignoring the
cursor, explicit `truncated`, and `next` at the last row of a nonempty page; `limit`
defaults to 100 and must be 1–1,000. Settings › Worktrees pages it to the end.

`rule.get` uses `RuleGetRequest`/`RuleShowView` and is Owner-only. It reads the selected
rule's cursor and retirement boundary, latest action outcome and latest refusal when
available. `suppressed_count` tracks live key suppressions and `rate_limited_firings`
tracks rate suppressions. An optional `project` selects a registered project principal. Omission selects
the global Owner scope. A global miss with a matching project rule includes a
`--project` hint. The read does not reload configuration or advance either cursor.
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
advancing its schedule. The `last_outcome` field reports the current assignment wait,
when present, or the last scheduled check. `timer.run` accepts
`TimerRunRequest { id, rule, project?, include? }`,
where `id` is the external ULID mutation ID and `rule` is the timer ID. An optional
`project` selects the registered project principal; omission selects global Owner scope.
An optional `include` selects one exact filename on a global Owner folder assignment.
It returns the existing Core `Command` parent bound to that timer and exact mutation ID.
An exact mutation-ID retry returns that parent without admitting or dispatching another
action child. Manual runs do not consume the scheduled slot. See the timer semantics in
[automation](automation.md#first-connected-timer-slice).

`agent.get` includes the current generation's source rows and the managed workspace
target generation's rows when it differs. If persisted state requires a live owner and
the corresponding source is absent, it returns `unknown` rather than asserting the
persisted state is current. It includes the same `provider_hold` and generation/turn-bound
`native_notice` facts as `agent.list`, with the native notice's bounded projection.
`AgentProviderHold.waiting_for_active_turn` is true only when a probing hold is the
current wait reason for an active turn; Store clears that projection when native
progress is newer than the hold's `updated_at`. Retry/reconnect notices are likewise
omitted when the active turn has newer native progress. When idle, a failed provider
notice is omitted after its matching hold is cleared; a new turn uses its own binding.
`AgentProviderHold.retry_after` reports a known provider reset; `next_probe_at` reports
the persisted automatic probe time when no reset is known; `held_since` preserves the
start of the uninterrupted hold across retries. The status line shows the hold age and
known reset or next probe without implying replay of an unknown native effect.
`agent.list` resolves all distinct holds for the selected page in one indexed scope
query and resolves native notices in one page-scoped query over the existing event
index. `agent.get` uses the same projection for its single Agent. Neither read adds a
polling source. `AgentView.policy_opt_out_reason` retains an explicit project-policy
opt-out reason; `swarm show` includes it in human output and JSON keeps the view field.
The Agent `effective` settings are the last
natively admitted values, never a prediction: the launch `session.ready`
readback sets them, and a `configure` revision's turn refreshes them when the
harness proves what its native turn request carried (Codex reports the
carried model/effort and Muse reports the carried effort through the
exact-turn association, which acknowledges native admission of those values;
`effort_verified`/`model_verified` mark that admission). Claude refreshes the verified serving model from a main-thread assistant
record on the bound turn. Values no turn
re-proves stay at their admission readback; no value is ever presented as
verified without that admission.

Request DTOs reject unknown fields. Required Agent IDs are nonempty after a trim check
and are kept as supplied. Optional `generation`, `after` and process `scope` members may
be omitted or null to mean absent; wrong types are `invalid`. `generation`, when present,
must be a nonnegative integer. `scope` accepts `agent` or `workspace`; absence/null means
`agent`. `limit` defaults to 100 only when omitted and must be 1–1,000; explicit null,
wrong types and out-of-range values are `invalid`. `observations` defaults to false only
when omitted and must be a boolean when present; explicit null is `invalid`. Absent
optional members are omitted when requests are serialized.

`agent.archive {id, agent:"aN", archived:bool}` is Owner-only and atomically
archives or unarchives the selected Agent and all current descendants. Every
member must be idle, ended (stopped, failed or lost), an external observer in
`unknown` state, or an ended native observation (a stopped observed
child keeping its `unknown` turn as retained ended-observation evidence), with no
active turn, no other uncertain turn, no pending request and no open command.
Archiving an ended member also needs its custody resolved: no unexited owned process,
`unknown` artifact or open native source (Eliot's `os`/`git`/`workspace_process`
observers may stay open). Archiving an external observer in `unknown` state
also requires resolved custody, so an open `observe` source blocks it.
A failed guard returns typed `busy` and names the blocking Agent and its state,
request, command, turn, process, source or artifact. Archive stops nothing and
preserves retained unknown evidence.
The desktop offers Archive and Unarchive in the task row and header menus.
Show archived in the sidebar includes archived tasks in the list.
Unarchive applies the same state, turn, request and command guards and skips
the custody check. The flag is durable. Admission of send, resume, steer or inject
atomically unarchives its target, including selector members, so new activity appears in
the default list. Reattaching an archived observer opens a new `observe` source and
clears the flag, so a live reader is never hidden.
`agent.list` accepts `archived:"exclude"` (default), `"only"` or `"include"`;
totals and cursors use the same filter. The default also lists an archived ended
(stopped, failed or lost) Agent while its custody is unresolved, as when an archived
idle Agent is lost with its process left running; `"only"` lists it too. The
default exclusion collects the live custody set once per page from the process,
artifact, and source tables (bounded by live custody, never by archived history):
the archived leg seeks those ids by primary key and the totals use the same set,
so no page scans archived history or probes custody per archived row. Summaries and full Agents expose `archived`.
CLI: `swarm archive aN`, `swarm unarchive aN`, `swarm ls --archived include`.

`agent.delete {id, agent:"aN"}` is Owner-only and removes a tree whose Agents are ended
(stopped or failed), a detached external observer, or an ended native observation with
its `unknown` turn retained as ended-observation evidence, using retention's leaf-first
command cleanup. A failed launch can be removed after its sources and custody drain; an
already-terminal `stop` does not need to rewrite its failure state. Delete removes a
retained tree-selector, rule, or timer admission parent with its full member command
set in the same transaction. Each member must pass the ordinary deletion checks. A set
with commands outside the requested Agent tree returns `busy`. A `busy` refusal names
the blocking Agent and its actual state, pending requests, command, turn (including
generation and retained state) or undrained process, source or artifact, rather than
treating stopped lifecycle as proof of drained history. An ordinary retained `unknown`
turn still blocks deletion after stop; the ended-observation evidence retires with its
tree instead. Delete removes the tree's turns and events without resolving them: no
resolution is recorded and no `turn.resolved` event is emitted. Live custody, open
commands, uncertain turns, active or uncertain cleanup attempts, or a pinned history
floor return `busy`; oversized operations return `unavailable` before cleanup. Before
removing the records, Delete uses the [cleanup ledger](coordination.md#registered-artifacts-and-cleanup-k69)
to archive eligible Codex chats, delete eligible OpenCode chats and remove the tree's
eligible Swarm-created worktrees. It uses the same worktree ownership and cleanliness
checks as `cleanup.apply`. Ordinary checkouts, Git branches, external sessions and
Claude Code, Muse and Antigravity chat history stay in place. The desktop confirms
the scope and offers Delete only for eligible stopped or failed task trees.
Preflight can defer a settled managed workspace's process source to worktree
removal. Open or uncertain processes still block preflight, and the source must
close before the task records are removed.
If cleanup is refused or uncertain, Delete keeps the task records. An unsuccessful
native session cleanup also keeps the tree's worktrees. Earlier confirmed cleanup
in the same request still stands. Uncertain effects are fenced, and an exact
retry reads the retained outcome. A successful Delete supersedes unapplied cleanup
reviews for the deleted tree's items. Process records for every generation are removed;
consumed cleanup plans and their exact apply replay remain retained. A deleted command
keeps any existing write receipt verbatim, including the receipt for a managed
`project.assignment.launch`. Delete records a retry tombstone when the command ID has
no receipt. Both preserve the ID binding until the retry window expires, preventing a
deleted native effect from being readmitted. A Store constraint refusal returns
`source_integrity` after rollback and leaves the writer available.
Archive/delete replies are `{agent, affected, archived:bool|null, deleted:bool}`;
mutation ID, result and changes commit together. Exact retries return the retained
reply even after deletion; changed payloads return `id_conflict`. MCP mints `id`
when omitted, as for other durable writes. CLI: `swarm delete aN`.

Removing a project already uses the settings writer: call
`settings.read {sections:["projects"]}`, remove its key from the returned complete
projects object, then `settings.save {expected_revision:<read revision>,
sections:{projects:<remaining object>},client:"desktop",dry_run:false}`. This
unregisters it without deleting its checkout or chats. No `project.remove` alias
is needed. The desktop labels the project menu action **Remove from Swarm** and
confirms that the checkout stays on disk. A later explicit open/launch in that
checkout can discover it again.

`agent.list` returns compact summaries ordered by Agent ID by default (`order: "id"`),
or newest activity first with `order: "recent"` (`updated_at` descending, then numeric
Agent ID descending; `updated_at` is bumped on every native event). The recent cursor is
`<id>@<updated_at_ms>` of the last row: `after` continues after that exact sort position,
and the unfiltered pages seek the `agents_recent` index, so the first and continuation
pages stay bounded on real volume; the `id` cursor stays the last returned Agent ID.
Recent-order traversal can skip an Agent when new activity moves it ahead of the
cursor between requests. Moving ahead of the cursor cannot repeat a returned Agent.
For observation-bearing pages, the Router joins and byte-bounds the rows before setting
`next`; it uses the last included row's cursor in the requested order (`id` for `id`,
`<id>@<updated_at_ms>` for `recent`). `truncated` is true when either the row cap or the
encoded response budget omits rows. The human `swarm top` footer calls out a byte-limited
page when it is truncated below the requested row limit. Text `swarm top` prints valid
rows and exits 0 when displaying a partial page. It warns on stderr about rejected
rows and withholds `next` when the page cursor or totals fail validation, including
when any wire row has no usable cursor key. JSON and other API consumers retain
strict whole-response validation. An unreadable response remains exit 4.
Existing callers keep the ID order unless they opt into `recent`. Each summary can carry
the retained execution directory in `workspace`; full records stay available through
`agent.get`. Optional `state`,
`project`, `project_scope`, `harness`, `parent` and `root` filters are exact
indexed matches: wrong types and unknown state names are `invalid`, and a
filter never matches silently. Optional `query` is a word search: every
whitespace-separated word must be an ASCII case-insensitive substring of the
`aN` id, the retained `name`, the first-input `preview`, the project label,
the `workspace` path or the harness (`agent_search_predicate` in
[`fleet.rs`](../../crates/eliot-store/src/fleet.rs)); rows, `total` and
`state_totals` apply it alike, combined with the other filters by
conjunction, in either order with the same cursors. It rides no index: a
searched page scans the fleet once under the Store lock.
`project_scope` matches the effective
authorization scope (`COALESCE(project_scope, project)`), the namespace
`swarm.reparent` and `swarm.handoff` compare, so one read reaches legacy rows
(matched through their project label) and registered rows with the same scope;
combined with `project` by conjunction when both are present. The selective
structural/scope index (`parent`/`root`/`project`/`project_scope`)
leads when several filters are present. `states` optionally selects 1 through
9 distinct lifecycle states and is mutually exclusive with `state`. It is one
ordinary page in the requested order (`id` or `recent`): `limit` remains a **total** row cap (at most 1,000),
not a per-state allowance; the existing cursor and byte bounds still apply.
When `states` is supplied, the response also requires `state_totals`, exactly
one `{state,total}` for each of the nine lifecycle states, including zeroes.
These counts apply the same caller scope, archived selection and non-state
filters, ignoring the state set and cursor; the selected states' counts sum
to the page's `total`. They count persisted lifecycle state, not observed-child
activity. No totals outside the caller's scope are exposed. Without `states`,
`state_totals` is absent. Rows and counts are captured under the same Store
lock; joined observations retain the generation checks below.

The desktop sidebar requests `order: "recent"` and refreshes its head on every poll:
the live and attention rows (`busy`, `starting`, `waiting`, plus the needs-attention
states `waiting`, `failed`, `lost`, `unknown`) arrive on their own state-filtered page,
and the first recent page is re-read and merged into the loaded history even after
`Show more`. Older pages append, deduplicated by id, so a launch while the desktop is
open appears without a manual refresh. The open Agent stays in place in its project
section, never folded under (nor counted in) `Show N older`; one opened from a search
beyond the loaded pages joins them, and the open row is kept when it ages out of the
newest page. Within each project the design order applies, ties on `updated_at`
break newest ID first as the server orders them, and children render in child-ID
order whatever the fetch order. The sidebar search sends `query` to `agent.list`
(newest first, the list's archived filter) and lists the server's matches flat in
their projects, with the loaded rows that match locally shown while the read is in
flight; `Show more` pages the matches, and an open search re-reads its first page on
every poll. The unfiltered history keeps its loaded pages underneath, so closing the
search never drops them. Project headers count working tasks from the listed rows,
so the desktop reads `chat.organization` without `query`. A linked worktree's header
shows the repository name trimmed first, then its subproject, on one row, so sibling
worktrees never trim to the same text.
`swarm ls`/`swarm top` default to `order: "recent"` for humans; `--json` and an
`id`-form `--after aN` cursor keep the ID order unless `--order` is explicit.

The desktop Monitor reads one such page for its eight live states, capped at
800 total rows, and shows at most 100 rows per state. It groups the rows
locally using the scoped totals; each group's shown/total count exposes a
shortfall caused by either row or byte truncation, including a state with no
rows in the returned prefix. Stopped rows remain a separate, rarer read, and
attention scanning is unchanged.

Each summary carries identity, state,
current turn, progress age, the last terminal outcome with its turn and
retained usage where available, child activity for native observed children,
and the last controller-observed write time. `provider_hold` reports the
applicable frozen account/profile hold without its shared scope or probe command;
`native_notice` reports a retry/reconnect/error fact with exact generation, turn and
observation time. Both are absent when no retained fact applies. The notice follows
the active turn or, after settlement, the latest terminal turn, so an older generation
or turn cannot read as current. The summary also carries what the Agent is
for, so a list needs no read per row (the pattern of Codex app-server
`thread/list`, whose threads carry `name` and a first-message `preview`): the
Agent `name` when one was set, its frozen `profile` name, and `preview`, the
first 256 characters ([`AGENT_PREVIEW_CHARS`](../../crates/eliot-core/src/api/agent_views.rs))
of its oldest retained `send` text, a launch's linked first input included,
read by one indexed seek per row. Each is omitted when absent; the scope that
lists a row may already read that command through `command.get`. `model` is the
model the harness last reported for the generation, the Agent row's retained
`effective.model` that `agent.get` and `agent.result` return, taken from the row
the page already reads; it is omitted until the harness reports one, and the
desktop labels an Agent by harness and this model. When the newest terminal turn
carries no native turn usage, the summary `usage` falls back to the
generation's latest retained `session.usage` snapshot (cumulative
input/output/cache counters only, never a derived total) with
`usage_scope: "session"`; turn-backed usage carries `"turn"`. The
session fallback never presents as turn usage: its inner `usage.scope` is
`"session"` as well, and per-turn accounting stays on `usage.summary`.
A missing outcome or progress
timestamp reads back absent, never zero or success; `updated_at` is freshness
metadata, not proof of liveness for a detached or restarted source. Each
summary also carries `project_scope`, the authorization namespace the server
compares for same-project moves and handoffs, present only when it differs
from `project`; pickers narrow by that scope while the server stays the judge.
`organization` names the [chat organization](#chat-organization) group the row
reads in, from the same committed predicate as the projection's group totals
(absent for the no-project bucket), under the same archived visibility rule
(a custody-held archived row the default list shows is counted there too),
`tags` carries its organization tags, and `organization_revision` the link
revision both were read with (absent before the Agent's first organization
write), which a guarded `chat.attach`/`chat.detach`/`chat.tag` carries back;
all are placement, never authority.
The response carries the required matching `total` ignoring the cursor, the
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
the human-readable STATE cell shows the read-only `activity` value with an
`observed` marker for native observed children (for example `idle (observed)`),
while `--json` and `swarm show` keep both the unchanged lifecycle `state`
(`unknown` for observed rows) and the `activity` object. Fleet `unknown`
counts bucket by activity, never by raw `state`: controlled Agents count by
lifecycle state, observed children with `running`/`idle`/`interrupted`
activity count under that activity, and only observed `unknown`/`lost`
activity counts as `unknown` (desktop monitor and `daemon.status` consumers
must use this bucket);
both tables carry a `USAGE` cell with the native total when supplied,
otherwise the input+output sum as a fleet signal, suffixed `(t)` for
turn-backed usage and `(s)` for the session fallback; `--json` keeps
printing the raw response.

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

`swarm launch --queue` calls `launch.enqueue` with a mutation ID and ordinary launch
arguments. `args.goal`, when present, is a partial Goal object. The Owner may
override individual defaults, while other principals may only tighten an applied
policy. `args.no_policy` is a nonblank reason of at most 256 bytes without control
or invisible format characters. It skips both the policy Goal and the fast check and
records `project.policy.opted_out` with the reason. Private chats have no registered
execution project or policy. A same-project switch retains the opt-out reason.
The same arguments reach direct and queued launch admission. Executable check
overrides and escalation require Owner authority. A queued intent has no Agent or command until
admitted. Exact retry
returns the current intent. When admission succeeds, the receipt gives the
ordinary launch command ID. Use `swarm launch-queue [INTENT]` for exact readback or
the pending page. The queue reports the wait code, scope and actor who can
change that scope. `project.overview` reports live harnesses, active turns, queued
work and retained unknown reservations as distinct counts.

A direct `launch` either admits immediately or returns `capacity_unavailable`
before creating a command, Agent or event. Fairness covers scheduled assignments
and explicit interactive intents admitted by the arbiter. A direct Owner launch may
use `--owner-override` to mark the command and its admission event as outside that
fair ordering. It still checks all admission limits.

A manager uses `swarm project budget PROJECT --revision REV` with `--max-live`,
`--priority`, `--pause` or `--resume` to apply an Owner-granted change to its current
assignment. `--pause` is a strict pause of new work for integration capacity. While it
holds, the assignment scope's effective limit is 0, so no new admission takes a slot,
including queued launch intents and a slot a finished child frees. Running children
continue. The pause lives in the same epoch-bound budget row, so a new manager or epoch
never inherits it. Lifting it restores the delegated limit without an increase check,
because a pause gives no capacity away.

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
([definition](../../crates/eliot-core/src/context.rs)); `point` (also accepted as `at`)
is `soonest`, `next_input`, `turn_safe_point`, or `active_turn`, with optional absolute
Unix-millisecond `expires_at_ms`. Store freezes the target generation and, for
`turn_safe_point` and `active_turn`, the exact active turn at admission. A supplied
`expected_turn_id` must match that turn; `next_input` has no turn binding and applies
to the next SEND in that generation. An `active_turn` command dispatches directly
as the plugin's `agent.command` inject to the native turn/steer effect on the
captured turn, expiry being revalidated at dispatch. An
adapter that lacks an explicitly requested point returns `unsupported`. With `soonest`,
Swarm uses the advertised in-turn point when the bound turn is active. It queues the
note for the next input when no in-turn point is available. The next input is an
ordinary SEND from any existing sender. Swarm adds context to it without starting a turn.

An unclaimed note whose captured turn ends moves to the next input when the current
generation supports it and the note is still valid. The original id remains usable.
`context.moved` and `command.get` report the move. Native confirmation records delivery,
and settlement reports cancellation or `unknown` when applicable. `inject.wait` waits
up to 30 seconds for delivery, a move, cancellation or uncertainty and returns the
retained command. A timeout returns the current command. `command.get` also reports
final settlement after a move.

The notes row of `autonomy.get` reports Swarm-generated fact notes by kind. It counts
delivery, moves and cancellation in the requested window and reports later evidence
that a delivered note's triggering fact cleared. The default window is 24 hours.

`project.broadcast` accepts a registered project id and note text from the Owner or its
assigned manager. The command captures the other working Agents and queues one `soonest`
insertion per recipient. The response includes the captured generation, turn and
command id so the sender can read each insertion outcome. Reusing the same mutation id
with the original input reads back the original recipient set.

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
switch runs live through `session/setModel` (durable for subsequent model
calls) with `session/read` proof, and a Claude `model` switch runs live
through the stream-json `set_model` control request between turns, each
refusing before any write while a turn is active. Claude `effort` has no
live switch and stays `unsupported` (`launch_only`). OpenCode switches the
retained session model through the native
`POST /api/session/{id}/model` setter at configure time and proves it with
a fresh `session.get` (`--effort` stays `unsupported`: the native API has
no effort knob); Antigravity switches `model` and `thought_level` through
ACP `session/set_config_option` at configure time and proves each write
with its echoed current value. On the natively-switching harnesses the
Router switches the live native session through the owned plugin first and
stores the overlay only after native proof, then settles `applied` with the
requested values, the monotonically increasing `config_revision` for the
generation and `application:"next_turn"`. Claude retains `accepted_model`
as the next-turn selector, with `effective.model` null and `model_verified`
false in the configure outcome. The next bound assistant record updates
the Agent's last observed serving model. Muse proves the session model at
configure and reports carried effort at the next turn association.
OpenCode and Antigravity report verified native values from their readback
or echo. A proven validation refusal settles `rejected`. An ambiguous
native effect or a Store commit failure after the effect settles
`unknown` and holds the settings lane. Neither path retries the setter.
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
revision does not match the stamped revision. Turn reads also carry the
effective native model when known and the message of a committed native
terminal error independently of the overlay revision. A `resume` explicitly inherits
the previous generation's latest overlay as its new generation's revision 1
with provenance `\"inherited_from\"` (the frozen snapshot is unchanged;
generations without an overlay start at 0). The lane is ordered by shared admission order: a
Store-only configure waits behind earlier queued or undispatched input,
and later input waits for an earlier configure. A native configure
submitted while input is pending is refused and must be requested again
between turns. When the native turn request carries a
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

Submit the targetless `git.integrate` verb through `command.submit`. Its
[`GitIntegrateDefinitionArgs`](../../crates/eliot-core/src/api/git_integrate.rs)
names a registered repository, a base branch, and the ordered local branches to combine.
The daemon merges the pinned commits in its own workspace and runs the project's
configured check there. Read the checked merge, conflict paths, or a bounded check failure
through `command.get`. If the combined check fails, the daemon checks each input against
the same base and reports branches that fail individually. An explicit request can
fast-forward a permitted local branch that no worktree has checked out. Integration leaves
the manager's working tree and index intact and has no remote effect. The same operation
is available to [authorized automation rules](automation.md#git-actions).

Generation-changing dispatch follows [core](core.md#generation-fence): advance and
revoke before native I/O, no rollback, no replay after unknown. Terminal settlement
revokes the token even if degraded Unix custody reports possible escaped descendants.

Admitted launches wait for transient internal metadata-reader concurrency before native
start. Launch preparation has its own bounded 32-reader lane; interactive discovery
keeps its four-reader lane. These bounds limit active metadata processes, not the
Agent fleet or a burst's admitted launches. Waiting consumes no native start attempt; closing admission
wakes waiters without starting a process. Configured capacity refusal remains
`capacity_unavailable` and names the scope and `max_live` bound. A retained `busy`
command outcome includes a redacted `message` naming the exhausted lane or bound;
the CLI prints its code and message, including through linked-start failure readback.

Launch/resume with explicit text creates a start plus linked `<id>/send`. Without
`--wait`, return session readiness; with it, follow the linked input to its exact terminal
turn. An input-created native session still reports these distinct outcomes.

An Owner `launch` may set `args.chat:true` and omit `workspace`. It must also
omit `project`, `parent`, `worktree` and `base`. The daemon freezes the configured
`[daemon].chats_root`, Store allocates the Agent ID, and the daemon exclusively
creates `<chats_root>/<agent-id>` as a private directory before native startup.
The directory identity is registered for artifact cleanup; collisions are refused.
The Agent and summaries carry `chat:true` and `project:""`; children inherit this
classification. Ordinary project discovery is skipped. The command retains the
root input, and same-ID retries use its frozen path even after settings change.
CLI: `swarm launch --chat --harness codex [PROMPT]` (or `--profile P`).

Worktree creation is opt-in on the same launch command:

```text
command.submit {id, verb:"launch",
  args:{profile?:NAME, harness?:HARNESS, model?:MODEL, effort?:EFFORT, cmd?:PATH,
        auth?:INTEGRATION,
        workspace, worktree?:BRANCH, base?:REF, parent?:AGENT, text?, sources?:[PATH]}}
```

For each of the five harnesses, omitted `cmd` uses the same harness-specific discovery
for stored profiles and ad-hoc launches. Launch preparation resolves and freezes the
selected native executable and available version in
that generation's snapshot. An explicit Owner `--cmd PATH` overrides discovery for an
ad-hoc launch; an explicit profile `cmd` remains pinned. A missing pin fails without
fallback, and doctor reports the missing profile path and discovered replacement
when available.

`sources` (launch and `send`, CLI `--source PATH`, at most 16 distinct paths) names
files the Agent must read. Before admission the daemon reads each one from the launch
workspace, or for `send` from the target Agent's workspace, and replaces `sources` with a
delivered block appended to `text`. Each section carries the path, original file
SHA-256 prefix and byte size, and the completeness of the delivered text.
The texts share what the 64 KiB persisted input text
leaves after the prompt, smallest first, so a short file is never cut to make room.
Known secrets are redacted. A redacted attachment is labeled as such, including when
its redacted text fits in full.
A file that is missing, outside the workspace, not regular, binary (including NUL bytes) or larger than 8 MiB
gets a `not delivered (reason)` line instead of being skipped. Delivery proves the text
reached the input. A same-ID retry of an admitted request reuses its retained text.

A parentless Owner launch names a stored `profile`, requests the configured default
by omitting profile/harness, or names an ad-hoc
`harness` (mutually exclusive); `model`/`effort`/`cmd`/`auth` are ad-hoc
only and require `harness` (`cmd` pins the native executable, e.g. a Muse
binary version the launcher no longer marks current; OpenCode-only `auth`
pins the native auth integration, defaulting to the model provider prefix
when the native auth list holds exactly that id). Ad-hoc launch is
Owner-only and freezes a profile-equivalent
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

Before admission, launch resolves its workspace against registered repositories by
verified Git common directory. The match becomes the recorded `project` on
`agent.get` and the compact `AgentSummary` from `agent.list`. The workspace remains
the execution directory. The existing workspace label and parent inheritance still
govern authorization scopes. When no registration matches, the Agent retains the
ordinary launch label. A managed Agent's summary can also carry its retained
`repo_common_dir` for display, including after worktree removal. Same-ID retries
reuse the admitted project decision.

With `parent`, the launch creates a child of that Agent. An Agent principal with
`control=tree` or `control=project` launches only its own children (the Router defaults
an absent parent to the caller). A caller with `control=self` is refused before a child
is created, including when the launch has no prompt. The child inherits the parent's
authorization namespace and tree root, and the configured fanout limits are checked
at admission. The parent must be controlled. An Agent-principal child
never supplies `profile_snapshot`: the daemon freezes it, so a supplied snapshot
rejects. With no configured default route, it may omit `profile` or specify the
parent's same profile name to reuse the parent's frozen profile snapshot verbatim.
It may instead name a profile from the parent snapshot's captured `child_profiles`
allow-list. The daemon resolves
that configured profile now and freezes its snapshot for the child. A `role`
instead selects ordered candidates intersected with the parent's frozen allow-list
(including its inherited profile). An unlisted name is rejected before any native effect. An omitted
`workspace` defaults to the parent's workspace, including for CLI child launches.
The child keeps the parent's recorded project ID and policy. An explicit `workspace`
must match the parent's. `worktree` optionally requests a managed checkout in the
same Git repository. The daemon freezes `base` at the parent's branch tip or at an
explicit ref that resolves to an ancestor of that tip. An omitted `base` uses the
parent's `HEAD`. A child request with `base` requires `worktree`. An explicit child
branch must meet the local name rules of `git.integrate` and be unused for a new
launch. Admission reserves the name within the repository before any Git write.
A bare `--worktree` generates a branch.
Same-harness children inherit native authority and captured credential bindings
verbatim. An approved
same-harness profile can set its model, effort, and daemon context ceiling.
An allow-listed different harness is an explicit Owner grant of that
profile's own native policy and bindings; parent credential/native-store bindings
are never copied across harnesses. Capacity scopes keep every non-profile scope
of the parent with the same bounds alongside the child's own scopes; conflicting
bounds reject. Control is narrowed or equal. Shared-workspace launches use the
prepared environment. New child worktrees use the ordinary managed preparation and
cleanup path. Owner child launches can specify a different workspace.
See [coordination](coordination.md#claims-and-hierarchy).

Removal uses the same command admission and readback path:

```text
command.submit {id, target:AGENT, verb:"worktree.remove",
  args:{expected_tip?:OID}, expected_generation?:N}
```

The Router/Store enforce owner authority, the captured generation and terminal-only,
clean removal under the admitted scratch policy. When supplied, `expected_tip`
must match the managed worktree's HEAD. Cleanup apply supplies the tip retained
in its plan.
The daemon selects the retained managed worktree,
preserves its branch and never removes a caller-supplied path. The
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
part of it. Retention and explicit Agent deletion remove selector parents and
their member commands atomically. Ordered rule actions reuse the
same representation with a distinct link kind, not selector execution semantics.

`command.children` is implemented for retained expansions through Core
[`CommandChildrenRequest`/`CommandChildrenResponse`](../../crates/eliot-core/src/api/reads.rs).
`id` names the parent and `after` names the last child of the previous page; both are
kept verbatim and use the bounded [Core command ID validator](../../crates/eliot-core/src/command.rs),
shared with `command.get`, `command.cancel` and `command.resolve`. CLI positional
`COMMAND_ID` references are independent of the mutation's optional `--id ULID`.
`limit` defaults to 100 only when omitted and must
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

### Diagnosis

`diagnose` (owner only) takes a command id, Agent id or `agent:turn` and returns
the causal chain: the command outcome with code, message, `where`, `origin` and
exit data; a start command's linked `<id>/send` input expansion when it exists,
because that expansion rather than the start settles the turn; the related turn
outcomes with `preceded_by`; process steps from the
`agent.lost`/`agent.failed` events with their recorded `origin`/`where`, exit code,
NTSTATUS and the redacted stderr tail (a loss to a daemon restart is `swarm` at
`daemon.restart`, a spontaneous plugin root exit is `harness` at
`plugin.exit`, see [core](core.md#failure-location-and-origin)). The retained
`agent.lost.stderr_tail` keeps the bounded, redacted capture verbatim, including
any raw panic marker lines. The process step can carry the startup panic marker
summary in `origin_evidence`. Diagnosis also includes the previous-run crash
marker when the terminal failure is unclassified.
For a refused `worktree.remove`, `swarm why COMMAND_ID` shows the retained
code, cause and failure location when available. `swarm why AGENT` includes
that refusal while it is the current managed worktree outcome for the terminal
Agent's generation. A later successful removal replaces it in Agent diagnosis.
When a failure exists, the one-line `summary` starts with its origin
(`provider origin: <target> <code> at <where>: <message>`). A healthy idle, busy,
waiting or stopped Agent with no failure in the bounded recent generation read
says that nothing has failed in that recent history; it carries no unrelated
previous-crash marker. Unknown origin remains for unexplained failures or
unknown outcomes. A terminal's typed `native_error.code` (for example
`quota_exhausted`) and native quota reset hint are included in its turn step.
For an Agent in any lifecycle state, diagnosis also lists retained unknown commands
and turns from any generation, appending `; retained ...` to the usual summary
within its length bound. The summary keeps its origin-first explanation. The
retained details name the exact command or turn and generation and the exact Owner
assertion through `swarm cmd resolve` or `swarm turn resolve` that can settle it.
Observe-only commands are not Owner-resolvable; their details say they remain
retained for the record instead of recommending `swarm cmd resolve`.
At most 16 steps;
no new store, only existing event and record readbacks.

Human CLI and desktop explanations lead with shared words for the typed code,
followed by the daemon's complete message when nonempty. Specific refusal reasons
(including reparent cycles, observe-only Agents and non-live managers) remain
visible; a generic code alone must not hide the reason or imply a temporary outage.
Codes without specific words display their readable code name without implying a
failure. Accepted, queued and dispatched commands describe progress; process steps
retain whether the Agent was lost or failed and explain their recorded reason.

The terminal event and `Turn.final_text` carry the final assistant message.
When the reply exceeds the encoded event budget, the retained suffix starts
with `[... truncated ...]`. Status, exact command/turn binding and any
structured provider restriction remain authoritative. `Turn.final_text = None`
means the public final text is unavailable. Full native text remains private history.
`turn.completed` can carry `data.worktree` when an enabled rule in the Owner or
Agent's project namespace consumes it. The data includes the SHA-256
`fingerprint`, the exact `base` commit, changed `files`, and changed `lines`
(insertions plus deletions) for [worktree review rules](automation.md#worktree-review-recipe).
Unavailable worktree observations do not supply a fingerprint or change totals.
When a project check applies, the rules consume the separately bound passing fact
for that worktree and author turn.
`Turn.usage = None` means native token usage is unavailable. A present usage
carries nullable token members with `scope`/`message_id` attribution when the harness
reports it; null attribution means the native scope is unknown. Message scope is never
a turn or session total, and a populated native total must not be assumed a per-turn or
per-call total when scope is unknown.

`usage.summary` reads bounded usage totals with explicit coverage over
exactly one of `agent` (with an optional `generation`; omitted covers all
retained generations and their registered native descendants), `project`, or `root` (the root Agent and its
descendants). The same read authorization as the other Agent reads applies:
an Agent principal sees only what its frozen scope allows. The response
echoes the scope, reports the included Agent, observed-native Agent and generation counts with explicit
`truncated`, states `history_complete` with its reason and the
`pruned_through` watermark, groups turn expenditure by native scope with
`turns_counted`/`turns_without_usage`/per-counter `turns_missing` (no
cross-scope totals) plus the excluded unknown/running turns the sums skip,
and carries session subtotals with their supply gaps
and per-unit costs.
The swarm_mcp member lists the exact Agent generation and turn with its Swarm
MCP call and refusal counts. Calls count when tool.started commits; refusals
count only when the matching tool.completed records an RPC error. Remote HTTP
calls have no local turn and are omitted. The list covers the most recent
1,000 turns; swarm_mcp_truncated marks older rows omitted. The CLI accepts
swarm usage <agent> and swarm usage --agent <agent> for the same Agent scope;
project and root scopes retain their flags. `swarm usage` renders this response as a compact table;
`--json` keeps printing the raw response.

`by_role` groups completed turns in the authorized usage scope using the
same role attribution as `swarm autonomy`. A turn counts as manager
usage when a retained child of that Agent existed by its completion.
Archived children and children outside the requested scope can establish
the role. Only authorized turn usage contributes to the totals.

The role rows report normalized input (uncached input plus cached input once),
output, and separate sample counts. `input_samples` and `output_samples` count
completed-turn usage records that supplied those counters. Normalized uncached
input includes cache writes once; cache writes are not added again as a
separate counter.
`average_context_tokens` averages observed `context_used_tokens` values from
retained `session.usage` events for the authorized Agent generations. Each
event is one context observation and uses the same role predicate at its
journal timestamp. `context_observations`,
`context_samples`, and `context_missing` report total observations,
valid occupancy samples, and observations without valid occupancy; missing
values are excluded from the mean, which is rounded down to a whole token.
Later retained observations are included.
Adapters that emit no `session.usage` event or omit context occupancy provide no
sample, so the mean describes observed snapshots rather than every possible
model call. Event pruning is reflected by `history_complete` and its reason;
that flag does not measure adapter sampling coverage.

The role aggregates use all retained terminal-turn usage records and
`session.usage` events in scope, independent of the bounded `first_calls`
display list.

`first_calls` reports known first-call input for exact retained Agent generations
and turns within that same usage scope, newest completion first.
`first_calls_truncated` marks older measured turns left out.
`swarm doctor` reports the average first-call input and sample count by profile,
harness and frozen MCP policy. It samples terminal turns from a bounded window of
the newest retained turn rows and uses profile facts for exact current generations.
Inherit and minimal are separate observed samples. Their values include the task's
input and the harness context.

Required IDs must be strings whose trimmed value is nonempty and are preserved without
trimming or normalization. Command references use the [Core command ID validator](../../crates/eliot-core/src/command.rs).
`turn.get` requires a nonnegative integer generation. An `agent.result` self-read
uses the generation bound to the Agent's credential, including when `generation`
is omitted. For other authorized callers, omitting `generation` selects all retained
generations. An exact `turn_id` lookup defaults to the current Agent generation
for those callers. An authorized explicit generation selects only that generation.
Omitted `limit` defaults to 100 and must be
1–1,000, including for an exact `turn_id` lookup. Any supplied generation must be a
nonnegative integer, and any supplied `turn_id` must be a nonempty string. Optional
request members that are absent are omitted during serialization; do not send `null`.
Explicit `null`, wrong types, invalid values and unknown request fields are
`invalid`. Unknown RPC methods and command verbs retain their existing
`unsupported` behavior.

An unfiltered `agent.result` page orders generations newest first. Within each
generation it sorts by the immutable turn insertion sequence, newest first. To
continue, pass the returned `next` as `before` with the same Agent and generation
filter. The byte budget includes turns, generation markers, Agent metadata, and
the continuation cursor. An exact `turn_id` lookup returns just that row and
cannot take `before`.
`next` is null when no older row remains.

The response includes `generation_boundaries` in descending generation order.
Each marker carries the generation number and its recorded origin, including
`restart_resume`, `owner_resume`, `switch` or `handoff`. A missing historical
origin is `unknown`. The first page can include the current generation before its
first turn. Markers can recur when two pages include turns from the same generation.
[Result DTOs](../../crates/eliot-core/src/api/reads.rs)

The existing principal checks and unknown-state masking apply. `current_generation`
identifies the current Agent generation. Request `metadata: true` or use
`swarm result --json --metadata` to include the full current `agent_metadata` object.
On a self-read, the response's `generation` is the
credential's generation. On other reads it echoes an explicit filter, or reports
the current Agent generation when the filter is omitted. Each turn carries its
own generation.

Clients validate the binding before displaying a read or using it to advance a
wait. `command.get` must return the requested command ID. `turn.get` must match
the requested Agent, generation, and turn ID. `agent.result` must match the
requested Agent and the applicable response generation described above. An
exact-turn lookup must return one row that matches the requested turn ID and the
response generation. Each result turn belongs to the requested Agent and any
applicable generation filter. On a cross-generation page a turn's generation
can be older than the response generation. The response respects the requested
limit, and the markers cover the generations in the page. A mismatch yields only the fixed
`unknown` error `read response does not match the requested binding`, with no response
data or diagnostics exposed. A `command.children` response must hold at most `limit`
well-formed commands with `next` equal to the last child id, or null when the
page is empty; a mismatch is the same fixed `unknown` error.

The desktop transcript uses the default `agent.result` read and continues older
pages with `before`. MCP `agent.result` uses the same contract. Each result row
keeps its own `final_text`, including the latest turn. MCP `swarm.wait` binds to
frozen command or turn references and returns bounded `final_text` for each exact turn.

`swarm cmd show`, `swarm cmd children` and `swarm result` exit 0 after a valid read
even if a returned command or turn is `unknown`; that state is not a successful task
outcome. A binding mismatch is unknown and must not print the mismatched response.
`swarm wait` prints its per-entry snapshot and exits 0 only when every waited entry
succeeded (`--all`) or at least one succeeded (`--any`); timeout or an uncompleted
snapshot exits 5, interrupted entries exit 7, `unknown`/`not_found`/`not_visible`
entries exit 4, and plain failures exit 1.

## Input, turn and unknown gates

Exact turn reads check uncertain commands by Agent, generation and turn ID. A full
fleet status page never makes an unrelated turn unavailable; durable and in-memory
unknown outcomes for the exact turn still mask completion. The one exception is a `steer`
claimed at a `turn_safe_point` hook (Claude): it keeps its own command state `unknown`
while the turn keeps its native outcome. An uncertain direct-RPC steer still masks its turn.

Transport/inbox admission is not send applied. Bind input to an exact native turn, then
track its result independently. Unassociated dispatched/unknown input blocks later
send/configure/Goal continuation; active/unknown turn blocks next turn; unknown transition
blocks further takeover. Exact stop/interrupt/request replies/readback remain available.

The one active-turn exception is a [self-started native turn](core.md#self-started-native-turns)
with no bound send. A `send` to it dispatches at once and binds to that turn, so
`send --wait` and `swarm result` report one turn with its final text. `Turn.native_trigger`
names what started such a turn and is absent for a turn started by a `send`.

Steer is bound to the captured generation and active turn and is checked against provider
holds before acceptance. On harnesses that deliver `steer` through a direct native RPC,
submitting to the plugin has a bounded response deadline; if that deadline expires, the
command settles as `unknown` with a timeout reason and releases the pending call slot
without repeating the steer or treating timeout as turn completion.

A session whose plugin declares `steer_delivery: "turn_safe_point"` (Claude) takes `steer`
at a hook. Its admission also requires `args.text` alone, a live runtime with that hook support, a healthy native stream and no in-flight
stop, and the `#888` Swarm-note frame must fit the 1 MiB hook response. Store rechecks the
dispatch gates, including rule-action order, and claims pending Claude steers in FIFO order
on the soonest `PreToolUse` or `PostToolUse` callback (`hook.event`) for that exact turn
using the `#888` Swarm note frame (preserving Owner and manager sender identity). A steer
that cannot be rendered is rejected alone, and the rest of the hook batch is delivered.
The steer stays bound to its exact turn: if the turn ends without another tool call, it
settles `rejected` (`captured_turn_ended`) and never moves to a later turn, so the sender
sees the rejection and can resend with `send`. It settles `unknown` only if a claimed hook
write outcome is unconfirmed, without replaying or moving to `next_input`.

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
theirs is refused with `not_allowed`. Such unknowns still stop blocking
drain once no live process, turn or reader backs them: the drain receipt
lists them as retained-unresolved instead (see the drain section).

`command.resolve {id, command_id, as: applied|failed, reason}` requires the target
command to be stored `unknown`. The one additional Owner assertion is
`failed -> applied` for an exact handoff brief; ordinary terminal commands,
dispatched, queued and accepted commands refuse with `bad_state`. `launch` and `resume` resolve as `failed`
only: `applied` is refused with `bad_state`, since an applied start without its
Agent transition is inconsistent. Resolving a managed `launch` that became unknown
during workspace preparation requires quiescent workspace custody and fresh Git
readback proving the reserved path, registration and branch are absent. The same
transaction marks the created Agent and preparation `failed`, cancels the linked
initial input and closes its drained workspace process source. For a cancelled
creation with a possible Git write, the Owner can inspect the exact start and stop
with `swarm cmd show`, the preparation with `workspace.list`, and target-generation
custody with `swarm processes A --scope workspace --generation N`. Once custody is
quiescent and any remaining Git artifacts are accounted for and absent, the Owner
can run `swarm cmd resolve LAUNCH_ID --status failed --reason TEXT`. Swarm rechecks
absence and requires the bound stop to be unknown or terminal. The Owner can
resolve the unknown stop separately. A workspace-stage
outcome reports Git dispatch from the workspace's
retained write marker. An unknown native configure resolves as `applied`
only when the owned session proves the requested configuration and the
Store commits its overlay in the resolution transaction. Muse supports
this with exact idle-session model readback. When the adapter cannot
prove the requested native setting, the command stays `unknown` unless
the Owner resolves it as `failed`. The command keeps any committed turn
association and gains no invented one — including a send whose native turn
association was lost, which resolves without creating a turn. The outcome
carries `resolution_source=owner`, as does the emitted `command.resolved`
event; no `command.applied`/`command.failed` receipt is emitted.
Future turn-result metadata retains the planned `command_ids` field; `usage` is
connected and stays null on owner-resolved turns without native usage.

`turn.resolve {id, agent, generation, turn_id, status: completed|failed|interrupted,
reason}` requires the turn to be stored `unknown`; running and terminal turns
are refused with `bad_state`. It further requires a stored terminal
generation (`Lost`, `Stopped`, or `Failed` — Eliot no longer controls it), a
superseded generation (one the Agent has moved past through resume, which
commits only from a stored terminal generation with no active turn), or a
native readback through the plugin (`agent.snapshot` for the exact turn)
proving the turn inactive. The readback proves the turn inactive only when it
binds this Agent generation, names the turn as its last turn with
`terminal: true` and a terminal status, and does not report it as the active
turn. The stored active turn alone is never proof: a live
generation without such a readback is refused with `bad_state`. A generation
ahead of the Agent's current generation is `stale_generation`. A superseded
generation resolves without a readback and touches only that exact turn. The
turn completes with the asserted status, and a `turn.resolved` event carrying
`resolution_source=owner` is emitted; no native `turn.completed` receipt is
invented and no command row is touched. The resolved `Turn` row carries
`resolution_source=owner`. `turn.get` and `agent.result` return the asserted
status and outcome with that marker. `swarm.wait` carries `resolution_source`
for both direct turn entries and command entries bound to that turn.
`agent.list` carries the newest terminal turn's marker as `last_resolution_source`.
Wait and list report an Owner-asserted success as `unknown`.

An adapter's own exact-turn terminal is evidence, so it also resolves a turn. When
`turn.completed` arrives for a turn that the daemon's
[attention monitor](plugin-protocol.md#translation-and-normalization) settled
`unknown` in the same live generation, bound to the same send, the Store commits it in
that `events.put` transaction instead of refusing it as inactive. The turn takes the
terminal's native status, outcome, final text and usage and reads back as a native
completion (no source marker). Beside the stored native event, a `turn.resolved` event
carries `resolution_source=native`, the turn, the send and the native event's
sequence. The Agent projection and command rows are untouched, and nothing is replayed
or re-run. This happens at most once and only for that settlement: a terminal for
another turn, send or generation, for a turn the Owner or an earlier terminal already
resolved, or for an unknown that came from loss, stop or the adapter itself (or whose
settlement event has expired) is refused as before. Consumers see the same
unknown-to-terminal transition as after an Owner resolution: the unknown-turn send gate
opens and wakes queued sends, and unsatisfied waits and digests read the native
outcome. A wait or digest already answered with `unknown` is not re-delivered.

When an upgraded terminal row lacks retained evidence of either native completion
or an Owner assertion, its source and reported outcome are both `unknown`. The
stored status is retained on the turn for inspection. Native completions omit the
source marker.

## Provider holds

`provider.holds` is owner-only and returns a bounded page ordered by local scope.
Optional `after` and `limit` select the page; the response provides `holds` and
`next`. The canonical Core `ProviderHoldsRequest`/`ProviderHoldsResponse` DTOs own
the wire shape.
`swarm status` shows the first page of held scopes, each hold's age,
and any scheduled retry or probe. JSON adds the age in milliseconds and a
continuation cursor for `swarm provider holds` when the status page is full.
`provider.clear_hold` is an owner-only idempotent write using
`ProviderClearHoldRequest`/`ProviderClearHoldResponse`. The corrective evidence is a
closed enum: `credentials_updated`, `configuration_updated`, or `owner_verified`;
free text is invalid. Its expected-probe binding is compare-and-swap, including explicit
null to mean no probe claim; a mismatch or unresolved probe cannot be cleared. See the canonical
[Core API DTOs](../../crates/eliot-core/src/api/provider.rs) rather than duplicating
their field list.

A provider scope comes from the frozen explicitly selected account, otherwise the
exact profile; a shared adopted proxy is only a capacity scope, never a provider
hold scope. Eliot never guesses a vendor account. A held scope gates launch/send
before native I/O and is rechecked when queued work dispatches. Bounded maintenance removes a
rate-limit/quota hold at or after its known `retry_after` and publishes the automatic
lift. Launch with linked input and
resume with new text are gated as sends. Plain resume, reads, request replies, interrupt
and stop remain available. At initial admission, `provider_held` rejects before binding
and creates no command row; that ID may be submitted again after the hold is resolved.
An already queued command that becomes held is rejected at dispatch under its existing
binding, before native I/O.

An explicit `send --probe` can claim an eligible rate-limit/quota hold through ordinary
SEND authorization and exact target/scope/generation checks. Once an unknown-reset
hold reaches `next_probe_at`, routing can select its profile for a fresh launch
with input. Store commits the launch and claims its linked SEND as the sole probe
in the same transaction. A launch without input still requires an unheld scope.
Routing preview reports eligibility for a launch with input.

Daemon maintenance also probes Codex profiles through the ordinary launch/SEND path for one fixed
minimal request in a private chat with no project, registered worktree, or user task.
Its frozen profile snapshot restricts permissions to read-only, disables approval and
native subagents, selects minimal MCP with an empty allow list, and marks the internal
probe mode. The Codex adapter skips the Swarm skill, shell/web/workspace instructions,
and Agent credentials for that mode. Store atomically binds that linked SEND as the sole
probe for its frozen provider scope before native spawn. The daemon-only outer launch
marker carries the exact scope and is rejected on public submissions. The probe flag
stays Store metadata. Native input is the launch's supplied text or the fixed
maintenance prompt.

A provider-supplied `retry_after` is a known reset: maintenance clears a held
rate-limit/quota row at or after that timestamp and records the lift event. A hold with
no reset persists `next_probe_at` and a completed-probe count, beginning at one minute
and backing off to five and then thirty minutes. Each admitted probe and known terminal
result is a provider event. A successful exact-turn native completion with no new
restriction clears the hold. A known failure releases only the exact probe claim,
retains the hold and schedules its next bounded attempt. Unknown command or turn
outcomes are never replayed and prevent further automatic probes. When background
preparation is deferred, a fresh launch remains eligible at the persisted deadline.
Deleting an Agent whose explicit probe command is settled releases that probe binding
back to the conservative held restriction instead of dropping it. Auth and
ModelRemoved holds require owner correction and are not probe-eligible. Provider
restriction facts are defined in [monitoring](monitoring.md#provider-holds).

## Handoff methods

The owning semantics are [Coordination: Handoff](coordination.md#handoff).
`swarm.reparent` is implemented; its sole wire DTOs are
[`ReparentRequest` / `ReparentResponse`](../../crates/eliot-core/src/api/hierarchy.rs).
Name exactly one of `agent` (whole subtree) or `children_of` (all direct children
and their subtrees), plus `expected_generation` for that source, `parent`,
`parent_generation`, and the mutation `id`. Both generations are required; no
active-Agent fallback or implicit generation refresh on retry. Owner only, same
project, controlled nonterminal/nonunknown destination. Native-child observation
bindings cannot move. Result `moved` lists every affected Agent/generation with
previous/current parent, root and depth, ordered by Agent number; an empty list
is a valid all-children result. No truncation or partial Store move is permitted.
Existing selector expansion and live fanout limits apply; encoded replies are
bounded by the existing retained-write budget. Errors include `invalid` (cycle
or bad source form), `stale_generation`, `not_found`, `not_allowed`, `bad_state`,
`fanout_limit`, `frame_too_large`, and mutation binding/age errors. Exact replay
returns the committed result before current-state checks. MCP discovery describes
this Owner method and mints a missing ID under the existing mutation rules.

CLI (explicit generations preserve an identical retry payload):

```text
swarm [--id ULID] reparent AGENT --parent MANAGER --generation N --parent-generation M
swarm [--id ULID] reparent --children-of MANAGER_A --parent MANAGER_B --generation N --parent-generation M
```

`swarm.handoff` implements the existing-Agent task transfer in
[Coordination](coordination.md#task-handoff-to-an-existing-agent-implemented).
[`HandoffRequest` / `HandoffResponse`](../../crates/eliot-core/src/api/handoff.rs)
are the sole wire shapes. The Owner names exact source/destination generations anywhere;
a manager may hand off only between two workers in its own controlled scope (neither
endpoint is the caller itself), and `reparent`/`switch` stay Owner-only. The caller
names one retained original-task SEND on A's generation, and at most 64 distinct plain
accepted/queued SEND IDs in successor order (also subject to the configured
selector-target bound). An Agent handoff transfers only SENDs the caller issued
itself (each successor keeps the original issuer's principal and rights) and
never moves children, claims or Goals; an Owner handoff transfers all
current live claims, the transferable Eliot Goal (paused on B), and all children
together. Active/unknown turns, pending requests, dispatched/unknown commands,
transitions or unresolved Goal rounds refuse; B must have no open work. When a Goal
transfers, B may have only a cleared Goal. Same-project and shared re-parent
invariants apply. No partial Store transfer commits.

The result names the applied Store-only parent, brief/ordered child ULIDs,
original/successor pairs, claims/conflicts, paused Goal revision, moved children,
truncation, and current `brief_state`/`successor_gate`. The parent being applied
means admission, not task success. `command.children` and exact command/turn reads
retain native outcomes. The gate is `queued_until_brief_applied`,
`queued_brief_failed` (including rejected/cancelled), `queued_brief_unknown`, or
`released`. A released gate still observes ordinary turn/FIFO/generation fences.
Retries return the same transfer identities before live/history/secret rebuild
checks, with current gate readback; no uncertain effect is repeated.

```text
swarm [--id ULID] handoff A --to B --generation N --to-generation M --task-command TASK_SEND [SEND_ID...]
swarm [--id ULID] cmd cancel HANDOFF_INPUT_ID
```

`command.cancel {id,command_id}` atomically records no-effect cancellation and a
retained mutation reply. The Owner may withdraw accepted/queued SEND children of a
handoff; ordinary, dispatched and unknown commands refuse with `bad_state` for the
Owner. A manager may cancel a `send` it issued itself while still accepted/queued
in its controlled scope (handoff child, ordinary SEND or own selector SEND child),
with no effect once dispatched; another manager's worker, a foreign issuer, or a
dispatched command refuses (`not_allowed`/`bad_state`). Launch/resume inputs,
including `<id>/send`, are outside this cancellation scope even while queued; the
refusal names this restriction. It never interrupts a native turn. `command.resolve` can release
an unknown brief, or narrowly assert a failed handoff brief applied, without a
resend. These recovery operations remain available during drain. MCP
advertises both mutations and mints omitted IDs using its normal rules.

`swarm.switch` implements Owner launch-plus-handoff to a new Agent, including a
cross-harness switch. [`SwitchRequest` / `SwitchResponse`](../../crates/eliot-core/src/api/switch.rs)
own the wire shape. It binds exact A generation, original-task SEND, selected SENDs
and profile or ad-hoc launch selectors. Workspace defaults to A's retained workspace;
workspace/worktree/base overrides still use normal launch validation. The route
preserves A's project authorization scope for handoff. The new Agent records the
registration resolved from its workspace. No launch prompt or parent override is accepted.

The immediate reply is the retained reservation, not proof of launch or transfer.
Use `swarm.wait` on the switch ID or retry that same ID for component readback.
The independent operation reserves launch/Handoff IDs before external preparation,
uses normal launch with no input, waits for exact committed session readiness,
then reuses H2's atomic transfer. The result exposes current parent/launch states,
created B and generation, Handoff result, error code and explicit `next_action`.
Applied parent means H2 admission, not native task completion. Failed/unknown brief
behavior remains H2's queued gate. Same-ID retry only reads retained bindings, even
during drain; it never launches another B or repeats work. See
[Coordination](coordination.md#agent-switch-implemented) for crash/boundary semantics.

```text
swarm [--id ULID] switch A --generation N --task-command TASK_SEND [SEND_ID...] --profile PROFILE
swarm [--id ULID] switch A --generation N --task-command TASK_SEND [SEND_ID...] --harness HARNESS [--model MODEL] [--effort EFFORT] [--auth INTEGRATION] [--cmd PATH]
```

Both forms accept optional `--workspace`, `--worktree` and `--base`. An initially
busy A refuses before B is allocated. If readiness/transfer fails after B is
created, B remains visible and A's transfer is rolled back. A definitely launched
but untransferred B is finished with ordinary `swarm.handoff` using B's exact
identity and a new handoff mutation ID; no finish mutation exists. Stopping either
Agent is explicit. MCP advertises `swarm.switch` as an Owner mutation and mints an
omitted ID through its normal rules.

## Waiting

`swarm.wait` is the implemented bounded completion wait over an explicitly captured
child set. The request carries `commands` (one command id or an array) and/or
`turns` (exact `agent`/`generation`/`turn_id` bindings). The default mode
`all` completes when every entry is
terminal. The `any` mode completes when any entry is terminal. The `wait_ms` bound is
30,000, matching `events.subscribe`. The set is frozen at request time: later turns or new
children never join it. A child being idle is not success; each entry reports the
terminal outcome of its exact captured command or turn. Per-entry `status` is one of
`succeeded`, `failed`, `interrupted` or `unknown` (from the canonical `TurnOutcome`),
`still_running` at timeout, or `not_found`/`not_visible` for ids outside the caller's
scope. A direct or command-bound turn can also carry its
[resolution provenance](#input-turn-and-unknown-gates). Every entry that
resolved to a turn also carries that turn's identity (`agent`, `generation`,
`turn_id`). A visible child launch, input, or turn can also carry its retained
managed worktree `branch`. A branch read failure returns `branch_error` on that
entry with its command or turn status. A completed turn carries its retained `final_text`,
cut to 16 KiB per entry within one 64 KiB response budget, with `truncated`
marking a cut or a text withheld by the budget (`agent.result` reads the whole
text). `mailbox: true` (Agent callers only; the owner has no mailbox) also ends
the wait with `mail_wake` when mail admitted after the wait started reaches the
caller's own mailbox; drain the inbox first when older mail must count. The
wait snapshots every entry under one Store lock and re-snapshots only on
command, turn, session, request or agent events of its own entries, or on a
delivery to that mailbox.
An Agent caller may wait only on what its frozen control scope permits, the same
rule as `agent.list` and direct reads; the set holds at most 256 entries. The daemon
reuses existing command/turn readback and its event bus for wakeups; there is no
scheduler or persistent workflow beyond the digest below. Run and project-rule principals are rejected;
Owner and Agent principals are allowed. Timeout never cancels admitted work. The CLI is
`swarm wait --all|--any [--mail] --timeout-seconds N ID...`, where an `ID` containing `/` is an
exact `agent/generation/turn_id` turn ref whose first segment is an `a<number>`
Agent id, and any other `ID` is a command id. A slashed positional whose first
segment is not an Agent id is a usage error, not a turn; Command ids
containing `/` (linked `<id>/send`, nested derived ids) and ambiguous triples must use the
repeatable `--command` option, which preserves them verbatim; exact turns may use the
repeatable `--turn` option. Exit codes
follow the read section: 0 only when every entry succeeded (`--all`) or at least one
succeeded (`--any`), 5 on timeout, 7 when an entry was interrupted, 4 for
`unknown`/`not_found`/`not_visible`, 1 for plain failure.

Each visible settled child Agent result (`succeeded`, `failed` or `interrupted`) also carries
the shared bounded `ChildRunFacts` DTO: generation-attributed edit paths and their
timestamps, workspace line counts, recent commit ids, the exact-generation last Goal
check exit code/time, and whether any workspace change is outside the Agent's active
claims. The CLI carries these fields through the same `WaitEntryResult`. Facts are
collected only for child waits and after the final wait snapshot, once per Agent
generation, so long polls do not repeat Git reads. Shared workspace Git paths, line
counts and commits remain unobserved; they are attached only when that workspace has
exactly one non-terminal Agent binding and it is the target generation. On shared
workspaces, only generation-attributed edit hints and the Goal check are reported;
an edit hint outside an active claim may set `outside_claims` to `true`, while no
mismatch does not prove `false`. No active claims leaves `outside_claims` unobserved.
Missing/stale targets and partial Git, history or claim evidence also remain
`unobserved`. Claims remain advisory. The combined facts in one wait response are
bounded to 3 MiB.

`swarm.digest` is the durable completion digest over the same frozen child set.
The request carries `id` (caller ULID, stable effect identity), `to` (one parent
Agent mailbox), `mode` (default `all`), and `commands`/`turns` (1..=256 entries,
frozen at admission like `swarm.wait`). An Agent caller may digest only to its own
mailbox and only entries within its frozen control scope; out-of-scope entries read
`not_visible` at registration and stay masked at delivery, which recomputes as
owner and never widens scope. A retry of the same registration keeps the first
admitted mask, so an entry hidden then reads `not_visible` (and takes no excerpt
budget) even after it moves into scope. At delivery, before writing excerpts, the
daemon also rechecks that the recipient (`to`) currently has scope over each entry
(intersected with the registration-time mask, including for Owner-registered
digests and workers reparented away before completion): entries where the
recipient lacks scope deliver status-only without `agent`, `turn_id`,
`final_text` or `truncated`. That recheck and the mail insert run under one Store
lock, so a reparent lands either before the check (status-only) or after the
committed mail. When the mode condition holds over terminal entries
(`still_running` is the only non-terminal status; `not_found`/`not_visible` count
as settled honest outcomes), the daemon delivers exactly one coalesced `mail.send`
(queue) result to the parent: mail id is the digest id, thread is
`completion-<id-prefix>`, and the body lists every entry outcome with its turn
identity once (a turn entry's id is `agent/generation/turn_id`; a command entry
adds `agent` and `turn_id`); an entry without a turn binding (outside the
recipient's scope, or `not_visible`) is named by its request position only
(`- turn #3: succeeded`), never printing the id, while the response keeps the
caller's original id. Answer-bearing entries carry `truncated` and a fairly
allocated bounded `final_text` excerpt for authorized completed bound turns within
`MAX_MAIL_TEXT_BYTES` (32 KiB), with NUL escaped (`\0`) before budgeting so one
answer never blocks the digest and earlier answers never starve later ones. A
quarter of that bound (or the whole answer demand when smaller) stays reserved
for excerpts across the batch; answer-bearing entries take the status-line
budget first and status-only entries fill what remains in request order, so a
wall of long status-only identities can never drop the only answer. The kept
entries render in request order and the rest are cut with an explicit
`... N more entries truncated` line, so a full batch of long identities never
spends the bound before any answer;
`SwarmDigestResponse.results`
uses `WaitEntryResult` with `not_visible` entries completely masked and
out-of-recipient-scope entries reduced to status-only. Visible settled entries
carry no observed work facts; fact collection is limited to `swarm.wait` and `child.run`.
The completion mail carries its bounded status and answer excerpt. Same id plus same
payload replays the retained registration and mail without a second delivery; same
id plus a different payload or principal rejects `id_conflict`. Reconnect or restart
replays the same mail id, so the digest is never duplicated. The driver sweeps
pending registrations with a progressing keyset cursor that seeks after the last
examined `(created_at, id)` and wraps once it passes the sweep high-water
observed at sweep start, so pending or undeliverable registrations at the front
never starve later eligible ones even while new registrations keep arriving;
new arrivals join the next sweep. A digest registered by its recipient in the
current generation is eligible for inline presentation under the ordinary mail
wake budget. Other digest deliveries remain available through the mailbox.
The digest emits no `x.user.agent.attention` event; a successful completion stays
distinct from an attention incident and no model call is needed to poll. The CLI is
`swarm digest --all|--any --to PARENT ID...` with the same `ID` shape as
`swarm wait`; it exits 0 when the digest is accepted (pending or delivered).
Neither the digest nor existing persistent HTTP notifications wake a
disconnected/stateless external client.

The following single-target server-side wait semantics are a future API contract; they
do not add `agent.wait` to the current method set. Current CLI `--wait` behavior is
described under Command admission.

Capture generation at call start. `turn --after-command ID` follows that input's exact
association/result; the retained turn row keeps its starting SEND command binding after
the `turn.started` event ages out. Without an id, capture the current turn or the first
after the call high-water. `idle` requires no open input/turn/transition or unresolved settings gate.
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
per-harness installation facts (`installed`, resolved `cmd`, and `--version`).
For Claude, `candidates` contains every discovered install, with parseable
numeric versions newest first and other entries following in discovery order.
Candidate `version` retains the bounded raw first line when available, or is
null when the probe is unavailable. Inventory presence does not make an
executable eligible for automatic Claude selection; that requires a successful
`--version` probe with a parseable numeric version.
Inventory and native discovery run on the blocking pool. The
`models.catalog` 90-second deadline covers the aggregate lock, inventory
worker and per-harness reads. Expiry during a per-harness read can return a
partial catalog. Cold version lookup and election are atomic for the resolved
binary. Followers share its result within their deadline. On Unix the
resolved payload must have an ELF or Mach-O header before a probe, including
for explicit paths. Inventory, model discovery, quota and launch resolve one
`agy` executable. They screen it for the vendor CLI module before running
bounded `acp --help` and version checks. An IDE executable is refused.
`harness.models` (`HarnessModelsRequest`/`HarnessModelsResponse`) returns the
harness's own read-only catalogue (Codex/Muse native `model/list`, OpenCode
native `opencode models` provider/model ids verbatim, Antigravity
`agy models`; Claude prefers the native `initialize` inventory — selectable
values with display names and per-model effort levels, no provenance marker —
and reports the documented aliases and full model ids plus the CLI default,
each entry carrying `source = "documented_alias"` with `unknown` availability,
only when the native inventory is unavailable). Each model keeps its native
`id` and carries an optional `display_name` from the vendor catalogue. The
plugin preserves the name verbatim when it is non-blank, contains no control
characters and is at most 512 UTF-8 bytes. Launch and configure catalogues
display the name when present and the native ID otherwise. Selection in both
catalogues uses the native ID only.
The `catalogue`
member is `native` for a live read (including the Claude native `initialize`
inventory), `aliases` for the Claude documented-alias fallback when the native
read is unavailable, `empty` when the
native read ran but listed no models (ad-hoc Muse on a launcher-marked binary
with an empty `model/list`: the note names that binary and the `--cmd` /
profile remedy), or `none` when the harness exposes no catalogue. Claude uses
the alias fallback when the frozen store or provider route cannot be verified.
The native cache requires a verified opaque route identity, and public scope
labels omit private route values. An `unknown` read or cleanup outcome,
`cancelled`, an integrity failure or a capacity refusal propagates as an error.
An optional `profile` selects the native route/account
(the named profile must exist and its frozen harness must match); omitted it
means ad-hoc daemon defaults. An optional `agent` (exclusive with `profile`)
selects the controlled Agent's frozen route. For OpenCode it reads the
owned generation's live session catalogue on every call and reports any read
failure. Without an Agent, OpenCode discovery is a launch preview and may take
an existing absolute `workspace` to apply that directory's configuration.
The Agent's harness must match and observed Agents are `not_allowed`. Every response
carries `configure`, the harness's mid-session `configure` support from the
same table `configure` admission uses: `model_refusal`/`effort_refusal` are
absent when the field has a verified mechanism and otherwise hold the exact
`unsupported` refusal admission returns. Native read caching requires a frozen,
opaque route identity verified against the exact private environment supplied
to the reader. The private key also binds the account, integration, full
configured endpoint and executable. Missing selectors, admitted unset
selectors and admitted empty values are distinct. Legacy and unverified
routes and previews with an explicit `workspace` bypass the cache.
Public `scope` names the profile and route without
private store paths. Every response carries `scope`, `fetched_at_ms`, `ttl_ms`
and a nonsecret `credential` reference. Native model IDs remain verbatim.
`harness.quota` (`HarnessQuotaRequest`/`HarnessQuotaResponse`) is an Owner-only,
read-only provider quota query. `harness` and optional `profile` keep their current
contract. The response retains `harness`, `scope`, `credential` (nonsecret names),
`fetched_at_ms`, `ttl_ms`, `status`, optional `note` and optional `details` (8192 bytes).
Codex keeps its `rateLimits` projection from `account/rateLimits/read`.
Claude uses its CLI OAuth `GET /api/oauth/usage`; Antigravity uses its native
Windows keyring credential and read-only Cloud Code Assist quota calls using
the installed CLI client fingerprint. Its native `retrieveUserQuotaSummary`
groups keep separate Gemini, Gemini 4 Argon and Claude/GPT model-family
5-hour/weekly buckets where supplied; missing groups are never filled from
legacy model-availability percentages (see [Antigravity](../harnesses/antigravity.md#read-only-provider-quota));
OpenCode Go uses the selected native credential export and `GET /zen/go/v1/usage`.
The named `opencode` integration reads Console workspace budgets and the Go
allowance through the installed standalone usage operation. The ad-hoc quota
read explicitly selects Go. Adopted profiles and profiles without a verified
integration report `unsupported`.
These provider readers return `details.windows`: at most 32
[`QuotaWindow`](../../crates/eliot-core/src/api/harness.rs) values
`{label, usedPercent, resetsAtMs, windowDurationMins}`. Percent is a finite number
in 0..100; reset time is Unix milliseconds or null when absent. Windows identify
5-hour, weekly and model-scoped limits when supplied; Eliot never invents limits
or reset times. Routing and the catalogue consume every reader through one
typed projection carrying provider, scope, observation time, verdict
(`exhausted`/`available`), mapping provenance and how the decisive bucket
maps to the model: an exact bucket id, a documented provider group, or
unmapped. Group matching is on whole model-id tokens against the documented
group table, never substrings: a depleted `Gemini 4 Argon weekly` bucket
skips Argon models but never a Hydrogen model, and an unrecognized label
such as `Model group` applies to no model (reported as unmapped, so callers
stay `unknown`). An omitted model has no group membership. Account limits
require a verified route, and a reader whose account is not the route's
reports none: a Codex profile on an adopted proxy (`route_binding`) is
refused before any reader starts, because `account/rateLimits/read`
describes the signed-in ChatGPT account whatever provider the route selects,
so its quota stays `unknown` (a failed read with that reason) and never skips
the proxied route. Console spend budgets constrain their covered
route or exact model, and Console's Go counters remain separate. For Codex
only the primary bucket's explicit limit state
settles exhaustion (`spendControlReached`, a non-null
`rateLimitReachedType`, a depleted `individualLimit`, or a full primary
window without credits); a full secondary window or a metered
`rateLimitsByLimitId` bucket never skips a route. Callers never reparse raw
quota JSON into booleans. Freshness is enforced through the same projection
entry point in both consumers. A response past its TTL leaves routing status
and availability `unknown`. Aggregate cache hits reproject availability
against the observation and reset times. `details.state` is `ready` or
`error`; `details.origin` is
`provider` or `environment`. A failed read remains `status:"supported"` with
`state:"error"`, a sanitized reason and no windows. Unsupported native integration
sources remain `unsupported` with a concrete reason. Native JSON-RPC method-not-found
(`-32601`) remains `unsupported`, naming the checked `account/rateLimits/read`
or `usage/read` method; it is not a supported environment failure. The transport disables
redirects, bounds each HTTP request to eight seconds and each response to 1 MiB;
credential reads are bounded to 256 KiB. Quota calls wait at most eight seconds
for one of the existing four metadata-reader permits, so a five-harness Usage
refresh can complete without an arbitrary fifth-read rejection. The wait starts
no process; shutdown/capacity failure remains an environment error. OpenCode's
native Console operation may refresh its active connection while resolving it.

Muse has no provider endpoint. The plugin projects MSP `usage/changed` as the
session-family `quota.observed` event; Store retains the typed observation, and
`harness.quota` selects the latest observation under the exact frozen profile.
Its `fetched_at_ms` remains the native `observedAtMs`, and `note` says "observed
during a turn". `usage/read` can also return that last native observation, exposed
under `details.usage`. Routing and `models.catalog` consume the retained
`windows[]`, live `usage` and legacy `window`/`weekly` shapes through one
normalized projection, alongside the other four harnesses' readers, so an
exhausted window skips the route exactly when the catalogue would refuse to
call it available. With no observation, or no installed MSP method, Muse
returns `unsupported` and its reason. No quota query starts a turn, performs
inference or polls Agents. CLI: `swarm quota --harness H [--profile P]`.

The quota response includes the effective [Owner reserve](configuration.md#owner-quota-reserve-872)
and `swarm quota` displays it with the recommended setting of 20%. For current
account-wide windows with known bounds, the command reports retained Swarm
usage for the selected profile route by Owner and automation, with coverage.
The provider reports account-wide percentages. Per-caller usage uses native
Swarm turn counters because the provider supplies no per-caller quota debit.
The reserve readback reports unknown when the observed windows map to no model
on the route, and exposes the profile's unknown-quota policy.

An automatic launch refused by the reserve or unknown-reading policy returns
`quota_reserve` or `quota_unknown`. Role-routed refusals include the typed
candidate skips in `Error.routing`. A queued launch or arbitrated assignment
can publish either code as its waiting reason before admission.

`profile.list` (`ProfileListRequest`/`ProfileListResponse`, no params) returns the
current `config.toml` revision, whether it parses, and per profile the name,
harness, requested model/effort (absent = native default), permissions,
approval policy (the resolved harness-native value: Codex `approval_policy`, Muse `approval`, Claude `permission_mode`; absent when the harness has no mapping or the profile does not resolve), control, proxy/account names, `max_live`, `child_profiles`
and whether launch-time resolution succeeds now (`launchable`, else `error`).
It reads no environment value, and a profile whose summary would carry a value
known to the daemon's secret policy is omitted from the list; parse diagnostics
are redacted with that policy before they enter the reply. The `[profiles.*]` source is
read and saved only through `settings.read`/`settings.save` ([configuration](configuration.md#unified-settings)).
`rule.list` (`RuleListRequest`/`RuleListResponse`) pages one rule namespace
(global, or `project`) by rule id with `after`/`limit`; each entry carries the
stored `RuleStateView`, its effective `TimerStateView` when the rule is a timer,
the canonical stored definition (`on`, `do`, `limit`, `catch_up`), and
`source_present`. A removed rule can retain a stored definition and reports
`source_present = false` regardless of its previous enabled value. Membership
tracks source presence while a prior revision retires. `source_file` names the editable
TOML file when the current definition has a file source. Entries whose
stored state, timer state or definition carry a value known to the daemon's
secret policy (keys included) are withheld, and `next` names the last returned
id. Like `agent.list`, every nonempty page sets `next` to its last id and the
caller continues until an empty page; a page shortened by the byte budget never
hides later rules. It changes nothing.

`swarm timer list` uses the same namespace read and returns timer entries only. Its
`--limit` counts timers and its `next` cursor is the last returned timer id.

`rule.read`, `rule.save` and `rule.delete` select the global namespace by default or a
registered namespace by `project`. `path` is a top-level TOML filename in that
namespace. `rule.read` returns the source and its byte revision. `rule.save` takes
`content` and an `expected_revision`; a null or omitted revision asserts absence and
creates the file. `rule.delete` requires the current `expected_revision`. A failed
precondition returns `conflict`. See [rule file editing](automation.md#rule-file-editing)
for source validation, reload and enablement.

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
the canonical scope/reference row. The requested `native_scope` is the discovery
page's store scope, never a subject `source` label: the proven store refuses any
other scope before native I/O, settling the attach `failed` (never `unknown`).
A controlled row or an active observer returns
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
The native subject projection read at attach is retained with the applied attach
outcome: when the live observer reader has lapsed, `source=native, view=subject` is
served from that retained metadata bound to the current generation. Turns/items views
still require the live reader, and a later detach still fences every read.
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
instead of replay from zero. A later retention gap still fails explicitly. A transient
transport timeout never ends the stream: the client re-issues `events.subscribe` from
its last cursor with bounded backoff. Only a typed rejection ends it; on `history_gap`
the client resolves the retained resume cursor from fresh bounds and reports it.

CLI `events` starts from `pruned_through` when `--since` is omitted. An explicit
`--since SEQ` remains exact. A cursor below the history floor returns `history_gap`
with the earliest resume cursor at the time of the rejection.

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
agent filter that excludes it returns `not_allowed`. Its filter also admits the
Agent-less, bodyless `var.changed` facts of the variable scopes it can read (global and
its own project).

Only `events.subscribe` accepts `wait_ms`. It defaults to 0, must be a nonnegative `u64`
when supplied, and values above 30,000 are clamped to 30,000. Bad types, negative values
and overflow are `invalid`; `events.read` rejects `wait_ms` as an unknown field. This wait
fills an event page and does not wait for command or turn settlement.

Subscribe before reading the persisted high-water mark, replay through that mark, then
release buffered persisted events above it in order. `next` is the exclusive replay
cursor; `high_water` reports the persisted high-water observed by the read. A buffer lag
sets `lagged` and triggers persisted replay from the current cursor. `truncated` reports
page count/encoded-size limits.

Both methods bound one call's work. Without an agent filter, a call scans at most 1,000
journal rows after the cursor and applies the kind/source filter to them, so a sparse
match can return an empty truncated page whose `next` still advances. With a nonempty
agent filter, the read goes through the `(agent, seq)` index with the kind/source filter
applied in the same query: `limit` and the encoded-size bound count matched rows only,
other Agents' traffic costs nothing, and `next` is the last returned row's sequence
(`truncated`) or `high_water` once the named Agents have no further row through it. Ephemeral delivery is best effort; pruned history returns
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
never create Agents. `to` accepts one canonical Agent ID string or an array of IDs;
both forms use the same strict item, uniqueness and count validation. The rule
`mail.send` action and `mail.ack.deliveries` use the same single-string list form.
Omitted `mode` defaults to `queue`. Only `queue` mode is connected; `mail.send`
presentation modes `send`, `steer` and `inject` return `unsupported`. This limitation
is specific to mail
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
The page may be narrowed by `thread` or by one exact `reply_to` delivery id
(rows whose `reply_to_delivery` names it), and `wait_ms` (clamped to 30,000)
long-polls for the first matching delivery instead of returning an empty page.
Every delivery appends one bodyless `mail.delivered` event
(`recipient`, `delivery_id`, `sender`, `thread`) bound to the recipient
generation, so the long-poll wakes without reading any body. The CLI is
`swarm inbox [--thread T] [--reply-to DELIVERY] [--wait SECS]`.
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
`var.get` takes an optional `after_version` baseline; `wait_ms` (clamped to
30,000) requires it. The read returns at once when the version already differs,
and otherwise long-polls for the matching `var.changed`; an absent row reads as
version 0, so a nonzero baseline on an absent row returns `not_found` at once.
The CLI is `swarm var get [--scope S] [--after-version N] [--wait SECS] KEY`.

The CLI supplies exactly one value source: inline `VALUE` or `--value-file
PATH`, mutually exclusive. The file is read as bounded UTF-8 (at most 16 KiB)
and its contents become the value; with `--as-json` the contents must parse
as JSON before admission. The file path is not part of command metadata;
only the resulting value is stored.

A routed recipient with `wake_on_mail` (profile snapshot, default true) may
receive one coalesced wake per minute with at most one open notice
command per recipient. An unread reply
whose `reply_to_delivery` names mail the recipient itself sent, admitted after
the last presentation, skips the one-minute cooldown once; the open-notice,
busy, pending-input and unknown gates still apply.
Eligible mail for the current generation is presented with sender, thread,
delivery ID and body within the encoded wake budget. Peer bodies are fenced
untrusted data. The exact wake's first committed model output or successful
terminal marks the inline rows read in the same transaction. A definite
no-output failure leaves them unread for the next eligible wake.
Command readback redacts wake text that contains mail. Uninlined deliveries
remain unread and the wake reports their count. Every notice uses the ordinary
owner `send` path bound to the
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
payload. `claim.set` emits `claim.changed`. `claim.release` removes one
live claim, emits `claim.changed`, and reports `released: 1`. It reports
`released: 0` for an absent, expired or prior-generation claim.

A claim is live only while its generation still matches the holder's
current generation, the holder is not terminal, and the TTL has not
passed. Generation change, terminal state and detach therefore release
by construction; expiry filters on read and is purged in a bounded batch
by the next claim write. A conflict — two globs that may overlap after
normalization (segments are compatible when equal or either side carries
a wildcard, with `**` realigning across directories), in the same project
— is reported in-band in the `claim.set` response. Each holder can receive
bounded details naming the Agents and globs through a generation-bound
`next_input` context item. The item waits for an independently scheduled
input. A replayed claim creates no new context item. Detection is
conservative. The in-band list holds up to 100 conflicts and `truncated`
signals the scan or report cap. Claims are advisory. `claim.list` pages
live claims by project and Agent. An Agent caller reads only its own project.

`agent.patch_digest.outside_claims` lists changed paths matching none of the target
Agent generation's active claim globs at the digest read. With no active claims the
list is empty and `outside_claims_complete` is false because there is no declared
scope to compare. The field is advisory and does not reject or limit work.
`outside_claims_complete` is also false when Git path coverage, the claim listing or
the bounded outside-path list is incomplete; known outside paths may still be present
when only Git path coverage is partial. For a shared workspace, the list uses only
the target generation's attributed edit paths and remains incomplete; it does not
claim all workspace changes belong to that Agent. Claims are live state, so the
digest does not reconstruct which claim was active when an edit occurred. A requested
stale generation still returns `stale_generation`.

`who` composes existing L3 projections per current worker: the roster
holds every non-terminal Agent in scope (at most 100, oldest first, with
a `truncated` flag instead of a cursor), including private chats, which
carry `chat: true` and no project. Each worker carries its
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
An admitted shutdown of either mode writes the data directory's `owner-stop` marker
(see [configuration](configuration.md)) before it replies, so a desktop does not
start the stopped daemon again on its own; the next daemon start clears it.

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

A stored `unknown` command with no live process, turn or reader behind its
generation is retained-unresolved: it is excluded from the blockers and listed
in the `drain` report under `retained_unresolved` instead, with its verb,
Agent, generation, stored outcome reason, resolvability and owner action.
An open source row counts as a reader only while its generation is the
agent's current live generation; rows on fenced or terminal generations are
residue. Retention is one SQL predicate over durable Store facts (no live
process, no open turn or reader for that generation), exhaustive over all
stored unknowns: there is no classification page cap an older unknown can
hide behind, and the exclusion binds no id lists, so no parameter limit can
fail the drain readback at any retained scale. Retained unknowns are excluded
from the receipt's unsettled section and its count as well, so the receipt
never double-lists them. The exclusion applies in SQL before the list bound
(not as a post-limit filter), so retained entries can neither occupy a
blocker slot nor hide a live command behind the bounded prefix; the excluding
total is the drain decision, and any live command the bound omits keeps
blocking as `CommandUnsettledMore` with the exact unlisted remainder. The
remainder counts only entries the list omits, never one it lists: a live Run
worker whose row the page omits is counted, while a listed one is not
counted again. A fleet of only retained unknowns still drains because the
predicate already excluded every one of them.
`resolvable` is false for observe-only commands, which `cmd resolve` refuses
by design; those entries need no action and stay retained as the honest
record (retention keeps sets containing `unknown` work). A `true` entry
names an owner-resolvable command the owner may still retire with
`cmd resolve` (or its turn with `turn resolve`). Either way the stored
outcome stays `unknown`: drain retires the wait, never the outcome.
Unresolved dispatched/queued/accepted commands, live turns, live owned
processes and live readers still block; `--force` stays their only escape.
Live script Run workers gate drain explicitly beside their command rows: a
worker whose row is still unsettled is already listed as `CommandUnsettled`,
and a worker that outlives its row settlement blocks as `ScriptRunActive`.
Every unfinished worker either appears in the blockers or is covered by the
overflow remainder, and the exit gate additionally refuses while any worker
is registered, so an omitted worker can never end the drain early.
Live owned processes gate on the Store custody snapshot (process records not
yet `exited`) as well as the workspace slots, never on routing eligibility
or workspace operation locks.

`daemon.status` carries the same bounded `drain` report; while draining it is the
current blockers snapshot, while open it reflects the current fleet.
`swarm daemon stop --drain` prints the retained `DaemonShutdownResponse` including
`blockers` and `drain`; `swarm status` prints the typed `DaemonStatusResponse`
including `blockers`, `unsettled_commands`, `unknown_commands`, `drain` and
`build_identity`, and the retention view with store sizes, configured windows,
SQLite page counts
and `pruned_through`. It also reports the automatic cleanup policy and latest
sweep outcome, and `profile_command_issues` for missing explicitly pinned executables.

Local IPC accept failures are logged and retried with bounded backoff.
During the delay, admitted work and existing connections continue, and the
daemon still honors shutdown. A successful accept with a usable listener
clears the failure count. After 20 consecutive failures, the daemon records
Owner attention and exits with `ipc_accept_exhausted`.

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

### Local operator commands

**Daemon update (#836).** The local Owner supplies a release directory or
Swarm executable. Both releases must support Store backup updates. A schema
downgrade is refused before the admission fence. The running daemon uses the planned-restart
`daemon.shutdown` modes. `prepare_restart` records a bounded admission
fence. The writer accepts `restart` under that fence only after admitted
turns and workers finish. If the wait expires, `cancel_restart` reopens the
same daemon. The fence also expires if its client disappears. Before
committing the restart, the writer checks free space, creates an online
SQLite backup next to `eliot.db`, and verifies SQLite integrity and the
source page count. The restart receipt commits only after the old Store
connection has been sealed. Confirmed cancellation discards its backup.

After commit, the old daemon retires its owned idle processes and leaves
the existing Store recovery facts for the next boot. Startup and rollback
both use #851 continuation and honor `on_restart`. The CLI submits no Agent
Stop or Resume commands during update. The output identifies the previous
and running release and includes observed Agent state. Boot recovery can
still be settling when that state is read.

The replacement publishes its endpoint before admitting RPC or background
work. A failure before publication restores the backup under the exclusive
daemon lock and restarts the previous executable. Restore retains the
original SQLite sidecars until database replacement succeeds. Once publication is
recorded, failure retains the backup and leaves the Store in place because
replacement writes may exist. Healthy open admission throughout a short
health check removes the temporary backup. On Windows the restart preserves the running daemon's
explicit absolute directory spelling and starts an included companion
detached with the same target data directory. For subsequent updates, the
operator closes a changed companion only when it can verify the process it
started for that directory.

**Private stand (#837).** `stand up` creates a fresh private daemon directory
and a scratch Git repository inside it. It accepts an optional private TOML
file containing the `[profiles]` table (written with private file permissions), an optional alternate Swarm binary, and
`--ttl DURATION` (default `DEFAULT_STAND_TTL_MS` = 24 hours, max `MAX_ARTIFACT_TTL_MS` = 30 days in [`api/cleanup.rs`](../../crates/eliot-core/src/api/cleanup.rs)). It prints the new
data, workspace and expiry after the daemon is ready. Operate on the printed
directory through explicit `--data-dir`. The stand starts with a fresh Store. A copy of
production data can retain production process custody names. Launch Agents only from a fresh Store.

When run inside a managed Agent session with inherited credentials, omitting
`--root` and `SWARM_STANDS_ROOT` places the stand under `ELIOT_ARTIFACT_ROOT`,
and `stand up` automatically registers the stand with the issuing parent daemon
via `artifact.register` (`kind: data_dir`, `purpose: SWARM_STAND_ARTIFACT_PURPOSE`,
`expires_at`). Otherwise `SWARM_STANDS_ROOT` selects the designated parent,
defaulting to the fixed stand home: `.eliot-stands` under the account's profile
Known Folder on Windows, or `.local/state/eliot-stands` under the account's
password-database home on Unix. The private registry always lives in
`.registry` under that fixed stand home; no environment variable selects it.
On Windows the home is outside AppData because Windows redirects AppData writes
from a process inside an MSIX package, such as a shell started by Claude
Desktop, to that package's private cache, where the out-of-package stand daemon
cannot see them. A packaged process refuses a stand root or designated parent
under AppData ([packaged-process rule](configuration.md#private-data-directory)).
A registration an earlier build left in `%LOCALAPPDATA%\EliotStands\.registry`
is still a stand. `stand list` shows it with that path, and `stand down` and
`stand prune` tear it down under the same marker, identity and owner-token
checks. Once its daemon exit is proven and the offline lock is held, the record
and its worktree records move once to the current registry, and the output
names the former path. Parent `cleanup` reads only the current registry.
It binds the random creation token to the canonical stand path outside the
stand itself and records the handle identity (volume serial and file index) of
the directory `stand up` created and the issuing daemon root when created
inside an Agent session; a `stand up` run without an Agent environment records
no issuing parent. While starting the daemon it created, `stand up`
records the SHA-256 of the owner token that daemon minted. From creation
through that bind it holds the new root and every ancestor by handle without
delete sharing, so none can be renamed or swapped for a junction; the bind
first refuses protected roots and reads the token relative to a root held by
handle (no reparse point) after matching its identity and marker. A token
still being written is retried, never bound. While a stand daemon prepares a
managed worktree, before any Git effect, it records the repository common
directory, branch, creation OID and worktree path in a sidecar of its
registration; a stand whose registration cannot be verified refuses the
creation. A missing registration refuses with `not_found`. One that exists but
is not a plain file, exceeds the record size limit, or cannot be inspected,
opened, read or parsed refuses with `not_allowed`. Both refusals precede any
effect and name the exact lookup path; the second names the reason without
quoting file content. An existing worktree record or stand marker that cannot
be used is refused the same way. `stand list` inspects registered stands with
expiry and liveness state, and `stand prune` tears down expired registered
stands. Both report every registry file they cannot use with its path and
reason. `stand prune` never tears down such an entry, because it cannot prove
its expiry or root, and exits 4.

`stand down` requires that registry binding, the stand marker and the owner
binding before any action. It refuses every daemon root the caller could resolve
as its own: the root resolved without `--data-dir`, `ELIOT_HOME`, the durable
selected root and the default, read from the environment and from the account's
fixed profile location, plus the issuing daemon persisted in the registration
(never inherited environment). Through the root held by handle, its identity
must equal the recorded creation identity, its marker must match and its
`owner.token` must hash to the value `stand up` recorded, so a copied, forged
or junction-swapped root cannot qualify without the target's own secret. A
root with a live daemon is refused unless it carries that full binding. It then stops owned processes, records exit proof
before removal, and deletes the marker last. Its local stand owner-credential
helper applies the same checks and permits only stand lifecycle and readback
RPCs (`daemon.status`, `agent.list`, `agent.get`, `swarm.wait`, `command.get`,
`daemon.shutdown`, and `command.submit` with `verb == "stop"`), leaving
server-side `#846`/`#886` owner boundaries unchanged. A retry uses the recorded removal phase. After a crash without
exit proof, start the registered stand again so boot can reconcile custody,
then retry teardown. [Workspace cancellation recovery](monitoring.md#workspace-preparation)
settles exact pre-effect cancellations before drain. Stand cleanup still requires
its own proof for any remaining Git artifacts.

After proving the stand daemon stopped, `stand down`, `stand prune`, and parent
`cleanup` remove stand files owned by the stand, including its scratch
repository, and remove stand-created clean worktrees in external repositories
under the `#885` write fence. Detached stand-created branches in external
repositories are retired to local `refs/eliot/archive/<branch>` refs. A worktree
or branch from the stand Store is touched
only when the external creation record matches its repository common directory,
branch, creation OID and worktree path; otherwise it is kept
(`unregistered_worktree`, or `not_allowed` with the record path and failed step
when the record exists but cannot be used). Any unusable file among the
creation records, even one no Store worktree refers to, is reported the same
way and keeps the stand until it is repaired or removed. Dirty or uncertain
external worktrees and branches whose retirement is blocked keep the stand directory and
registration (`retained`, CLI exit 4) so retried `stand down` after cleanup
completes removal (CLI exit 0).

## CLI

Daemon RPC commands accept `--json` and `--data-dir`, and RPC mutations accept
`--id`. The local update and stand commands use individual IDs for their
internal writes. Stand commands take the stand path directly. CLI Agent targets
are positional; `--to` remains the named-recipient selector for `msg send` and
`digest`. Pagination uses `--after` and `--limit`; `--data-dir`, `--json`,
`--generation`, and `--project` keep one spelling across command families.
For one compatibility release, the former pagination flags remain aliases:
`events --since`, `discover --cursor`, `history --cursor`,
`project overview --cursor`, and `cleanup plan --continuation`.
The legacy `--agent` options for `usage`, `heavy`, mail show/ack, and
claim set/list/release remain accepted. Command resolution accepts legacy
`--as applied|failed` alongside the canonical `--status` form.
The default presentation is concise human-readable output for status, Agent,
command, project, Goal and timer details, rule/timer lists, daemon update, Store
compaction and stop settlement. `--json` keeps each command's existing response
shape and prints it as compact JSON. `result` starts with the newest retained
final answer and includes its turn status. Scripts and agents parsing `show`
or `result` must request `--json`. Use `result --json` for the full page.
Generation/turn/request flags are optional for immediate calls, explicit for delayed ones.
The synopsis is target grammar; later-layer forms are not an inventory of current help.

```text
swarm daemon [--foreground]
swarm daemon install | uninstall
swarm daemon stop [--drain|--force]
swarm daemon update <RELEASE> [--wait-seconds N]
swarm stand up [--root PATH] [--profile FILE] [--binary PATH] [--ttl DURATION]
swarm stand down <PATH>
swarm stand list
swarm stand prune
swarm status
swarm autonomy [--since TIME] [--json]
swarm store compact
swarm cleanup plan [--after TOKEN]
swarm cleanup apply <PLAN_ID> [--artifact ID]... [--delete-remote]
swarm artifact register <PATH> --kind build|data_dir|temp|other --purpose TEXT [--ttl DURATION]
swarm launch [--profile P | --role R | --harness H] --workspace PATH [--worktree BRANCH [--base REF]] [--parent AGENT] [--source PATH]... [--no-policy REASON] [--goal-json JSON_OBJECT] [PROMPT] [--queue] [--wait] [--timeout-seconds N]
swarm launch [--workspace PATH | --chat] [--parent AGENT] [--no-policy REASON] [--goal-json JSON_OBJECT] [PROMPT] [--queue] [--wait] [--timeout-seconds N]
swarm settings read
swarm settings save FILE [--dry-run]
swarm settings claude-cmd PROFILE (--cmd PATH|--latest)
swarm worktree remove AGENT [--generation N]
swarm ls | show A | watch [A|SELECTOR] [--after SEQ] | top
swarm usage [A] [--generation N] [--project P] [--root A]
swarm heavy [A] [--generation N] [--window-secs N]
swarm models [A] [--harness H] [--profile P] [--workspace DIR] [--query Q] [--after CURSOR] [--limit N]
swarm processes A [--scope agent|workspace] [--generation N] [--after CURSOR] [--limit N]
swarm stats A [--generation N]
swarm git integrate --project P --base BRANCH [--fast-forward-to BRANCH] [--timeout DURATION] [--wait --timeout-seconds N] BRANCH...
swarm git-status A [--generation N]
swarm patch-digest A [--generation N]
swarm edits A [--generation N] [--after PATH] [--limit N]
swarm send A TEXT [--probe] [--wait] [--timeout-seconds N] [--generation N]
swarm steer A TEXT [--generation N] [--turn ID]
swarm inject A TEXT --point soonest|next_input|turn_safe_point|active_turn [--generation N] [--turn ID] [--expires-at-ms MS] [--wait]
swarm project broadcast PROJECT TEXT
swarm interrupt|abort A [--generation N] [--turn ID]
swarm stop A [--force] [--tree | --generation N]
swarm resume A [PROMPT] [--wait] [--timeout-seconds N]
swarm wait --all|--any --timeout-seconds N ID...
swarm wait --all|--any [--command ID]... [--turn AGENT/GENERATION/TURN_ID]...
swarm result A [--generation N] [--before CURSOR_JSON | --turn-id TURN] [--limit N] [--metadata]
swarm turn resolve A --generation N --turn ID --status completed|failed|interrupted --reason TEXT
swarm approve|deny A [--request R] [--choice ID] [--feedback TEXT] | answer ... [--request R]
swarm configure A [--model MODEL] [--effort EFFORT] [--generation N]
swarm compact|fork|background ...
swarm cmd show|children|cancel|resolve|settle-stopped ...
swarm cmd resolve ID --status completed|failed --reason TEXT
swarm why CMD|AGENT|AGENT:TURN
swarm events [--after SEQ] [--limit N]
swarm msg send --to RECIPIENT... TEXT | msg show DELIVERY [AGENT] | msg ack [AGENT] --delivery DELIVERY...
swarm inbox [AGENT] [--wait SECS] | inbox decide ITEM CHOICE | inbox snooze ITEM | inbox apply (--kind KIND [--yes] | --item REF...)
swarm var set KEY [VALUE | --value-file PATH] [--as-json] [...] | var get ... | who [--project P] [--history]
swarm claim set GLOB [AGENT] [--note TEXT] [--ttl-secs N] | claim list [AGENT] [--project P] | claim release GLOB [AGENT]
swarm project list | show ID
swarm rule list [--project PROJECT] [--after ID] [--limit N]
swarm rule show ID [--project PROJECT]
swarm rule read NAME.toml [--project PROJECT]
swarm rule save NAME.toml --file FILE [--expected-revision SHA256] [--project PROJECT]
swarm rule delete NAME.toml --expected-revision SHA256 [--project PROJECT]
swarm rule catch-up ID --after SEQ [--through SEQ] [--project PROJECT]
swarm timer show ID [--project PROJECT] | run ID [--project PROJECT | --include NAME]
swarm timer list [--project PROJECT] [--after ID] [--limit N]
swarm goal set A --text TEXT --check CMD [ARGS...] [--mode eliot|native] [--check-cwd PATH] [--check-timeout-ms MS] [--max-turns N] [--max-rounds N] [--max-without-progress N] [--cadence-ms MS] [--scope GLOB]... [--check-input GLOB]... [--max-cost AMOUNT --cost-unit UNIT] [--escalate-model M] [--escalate-effort E] [--escalate-reason TEXT]
swarm goal show|pause|clear|resume A [--mode eliot|native] [--reason R]
swarm goal round A (--idle-seq SEQ | --slot N) [--mode eliot|native]
swarm discover --profile P [--harness H] [--scope S] [--after C] [--archived] [--limit N]
swarm attach --profile P --harness H --scope S --ref R [--project LABEL]
swarm history A --view subject|turns|items [--source native|rollout] [--turn-id ID] [--after C] [--limit N]
swarm detach A [--generation N]
swarm chat organization [--project P] [--after P] [--archived]
swarm chat attach CHAT... --project P
swarm chat detach CHAT...
swarm chat tag AGENT... [--tag TAG]...
swarm provider holds [--after SCOPE] [--limit N]
swarm provider clear-hold SCOPE --corrective-evidence credentials_updated|configuration_updated|owner_verified [--expected-probe-command ID]
swarm project assignment claim PROJECT [AGENT] [--generation N]
swarm project assignment launch PROJECT --profile P [--adopt-worktree PATH] [--no-policy REASON] [--goal-json JSON_OBJECT]
swarm plugin NAME | plugin-test ... | hook ... | mcp
```

`cmd cancel` implements only undispatched handoff-input withdrawal. `cmd show`,
`cmd children`, `cmd resolve`, and `turn resolve` are available; `turn show`
remains unavailable. `swarm wait` implements only the frozen `--all`/`--any` set form
above; single-target `--for turn|idle|stopped|goal` forms remain future (`agent.wait`).
The set wait takes 1 through 30 seconds. Command and turn resolution use
`--status`. A completed command maps to the stored `applied` outcome.

`swarm daemon` (background) prints `starting` with the resolved data directory
and log path, then polls `daemon.status` for up to 30 s. It prints `ready` with the
resolved data directory and boot id on success. If Windows startup observed root
ACL repair, it reports that repair on stderr. ACL refusals identify the offending
SID and rights where available. Other startup failures include the logged
`daemon.failed` code newer than this start and bounded sanitized diagnostics with
the log path, when available. The tail is read boundedly (64 KiB, 64 records)
from the resolved private data directory only, and only codes, ids and existing
sanitized diagnostics are printed. `--foreground` uses the same startup and active
ACL checks. No ACL bypass or automatic Store migration or Store-data repair occurs.
Desktop startup reports the same repair outcome or exact refusal reason.

`swarm status` is a read-only probe: it neither starts the daemon nor changes ACLs.
When a shared startup wait expires and the root still has a repairable ACL deviation,
the client error preserves the current path/SID/rights diagnosis and names the
Owner-only retry command `swarm --data-dir '<path>' daemon`. The path is escaped for
PowerShell by doubling embedded single quotes. The local Owner startup path performs
that repair; Agent and Run callers keep their inherited identity and do not use their
credentials to probe Owner-only status or start the daemon.

`swarm status` and the start output include the resolved data directory, so
`--data-dir`, `ELIOT_HOME` and default-location differences are visible. Local
clients briefly wait for the active Windows root check before endpoint or credential
access. If a client cannot reach the daemon pipe, the error names whether daemon
metadata, lock and log files exist for that data directory and includes the last
`daemon.failed` code from the bounded log tail when present and recent. It never
prints secrets.

`resume --wait` accepts the same `--timeout-seconds` handling as
`launch`/`send`. When a launch or resume with new text fails and its linked
SEND is cancelled as a result, a prompted wait reports the parent start
failure rather than only the linked cancellation.

Only current-layer commands appear in help and Agent guidance. Exit codes: 0 success/
queued; 1 definite failure; 2 usage; 3 pre-effect rejection; 4 unknown; 5 wait timeout;
6 still dispatched/pending; 7 interrupted/cancelled. A queued/applied input is not
successful task completion. Every mutation prints its generated command id as `id=...`
on stderr so stdout stays pure response JSON; Windows PowerShell 5.1 turns that stderr
line into an error record when merged with `2>&1`, so callers must read the raw process
exit code (`$LASTEXITCODE` / `%ERRORLEVEL%`) rather than `$?`.

`steer` does not require `--generation`/`--turn`. When either flag
is omitted the CLI reads the Agent's current generation and active turn, prints
the exact target it captured (`<verb> target: agent=A generation=N turn=T` on
stderr), and submits exactly that binding; an explicit flag always wins over the
captured value, and when both flags are given the CLI submits them unchanged
with no extra read. A missing active turn fails clearly (exit 3) before any
submit. A captured submit that the Store rejects as `stale_generation` is
reported as a race (exit 3, `race:` message naming the submitted binding) and is
never retried against the new turn.

`interrupt` submits explicit generation and turn constraints unchanged.
When either is omitted the daemon captures the current binding during admission.
For the Owner, a controlled live native session is sufficient even when its turn
is uncertain. The result of a proven fallback stop carries
`outcome.reason=interrupt_unconfirmed_agent_stopped` and
`outcome.fallback_stop_id`. A confirmed Owner interruption is visible on the
bound turn as `outcome=interrupted` with `resolution_source=owner`.

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
