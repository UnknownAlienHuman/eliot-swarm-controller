# Muse Code

Implemented and selected L6 adapter (selected by the owner 2026-11-03, roadmap L6.4).
Use the official MSP SDK/transport in one per-Agent plugin and Muse host;
do not make Muse a dependency of the first Codex path.

Source: [meta-models/muse-code-sdk](https://github.com/meta-models/muse-code-sdk).
Inherited 2026-10-25 notes refer to MSP v1 / Muse 1.4. Verify installed host/SDK pairing,
methods, license and toolchain at integration; these are not current runtime findings.

## Native identity and transport

Subject: `(canonical Muse host/store/account scope, exact native session id)`.
MSP uses JSON-RPC NDJSON over stdio. Resume opens the retained session in a new Eliot
generation; callbacks carry that generation. Native children need exact provenance and
remain parent-attributed unless an isolated child environment can actually be installed.
Resume requests `excludeItems=true` so it receives only session metadata and live resume
state; the installed stable and experimental schemas define this history-exclusion option.

The [shared 8 MiB inbound frame cap](../spec/plugin-protocol.md#lifecycle-hooks-and-qualification)
covers Muse `view/page` readback (Eliot requests `limit: 1000`)
plus envelope overhead, along with the single bounded `item/readOutput` patch
page below (128 KiB of stored bytes). What still does not size this cap is the
full `item/readOutput` surface (6 MiB of stored bytes per page, exactly 8 MiB of
base64 before envelope overhead), which Eliot never fetches; and the installed
`muse-bin-1.4.0-R4161.1` schema (`muse schema generate-json-schema`) publishes no
per-surface text budget, so the true `view/page` maximum is unverified.
`session/resume` with `excludeItems=true` serves `history.mode: "none"` (no items),
so the resume response itself stays small when the host honors the option, which
Eliot verifies before proceeding. A frame over the cap ends live stream delivery with an
explicit `source.gap` carrying a bounded, secret-free diagnosis (byte length with
a `>=` lower-bound marker when the fault published at the threshold, limit,
envelope kind, allowlisted method); responses to Eliot's own calls are never
skipped. Remaining uncertainty: a single live item notification in a tool-heavy
turn could in theory still exceed 8 MiB; if that is ever observed, the gap
diagnosis will name its kind and, when the method is a known protocol method,
its method.
After an uncertain start/resume reply, Muse reports `native_endpoint_exited` only when
its owned process exit, closed reader and completed reader task are all observed. The
daemon keeps the command `unknown` and settles Agent loss only after owner cleanup.

Obtain the native command id before writing: use caller-selected `commandId` or an SDK
operation exposing its generated id before send. Text/time/list position is not identity.
Use supported typed APIs, not an inferred method list from a different host version.

## Additive native event decoding

Muse DTOs ignore unfamiliar fields. An unhandled notification for the owned session
commits `session.native_unclassified` with only a static site and the sanitized native
method tag; it does not persist the vendor payload, infer a lifecycle fact, or stop the
stream. Notifications explicitly naming another session are ignored.

On live events, unknown `health`, `noneReason`, `turn/completed.terminal`, and
workflow-child terminal values decode as `Other` and produce the same neutral diagnostic
with the native schema tag. An unknown health value does not change source health.
`health="unavailable"` still marks the exact session unavailable, including when its
reason is `Other`. A healthy live `turn/completed` bound to the exact active turn
establishes terminal lifecycle even when the terminal value is `Other`. Its outcome
is `unknown`. An unavailable view still requires command-plane proof. A workflow-child
status or readback value of `Other` carries no terminal or healthy-view evidence.

The adapter has no exact runtime-version equality gate. Its manifest declares
the `1.4.0` harness baseline. Owned sessions require a non-empty
`serverInfo.version` and the `rawLog` capability for model observation.

## Native mapping

The installed `turn/steer` result must echo the submitted `commandId`, the exact
`expectedTurnId` as `turnId`, and `status="accepted"` (1.4.0-R4161.1 exported
MSP schema). A malformed result or contradictory echo is a diagnostic
`harness_contract_mismatch` at this section; the effect remains unknown. A
native refusal alone does not prove a contract mismatch or an upstream failure.

| Eliot verb | Installed MSP mechanism to verify |
| --- | --- |
| launch/send | Explicit models use `serve --model <id>`, then `session/start` inherits that route. `turn/start` sends input after readiness. |
| steer/interrupt | steer is turn/steer with the exact expected turn. Interrupt is turn/cancel with the captured turn. An empty Owner binding allows idle confirmation only under the [shared control policy](../spec/plugin-protocol.md#host-requests). The native queue is retained |
| inject | exact supported steer or explicit next-send prepend, mechanism reported; active_turn uses the actual turn/steer mechanism with the exact expected turn (`muse.turn/steer`), other points unsupported here |
| stop/resume | interrupt the captured busy turn and await native terminal evidence within the core bound, then clear/pause native Goal as necessary and close owned host; missing evidence retains an unknown turn; resume exact session without old input |
| approve/deny/answer | exact session/child request id and offered choice/requirement; public details are the shared ApprovalRequest/QuestionRequest, answers are the universal answers map |
| configure | `session/setModel` carries the catalogue's provider and profile between turns. Its exact acknowledgment and `session/read` prove selector admission before the overlay commits. Effort travels in per-turn `reasoningEffort` in `turn/start`. An effort-only revision keeps the last selector. |
| goal | native set/edit/pause/resume/clear where installed schema supports them |
| compact/fork/background | native-only, advertised after implementation |

The Muse 1.4.25 MSP schema documents that an omitted `session/setModel`
`profileId` means no profile. In #949, the retained Contributor selector
lost its `tbh` route and the provider answered with the ordinary model.
Startup lets Muse resolve the profile for the exact `--model`. Resume uses
`session/resume.modelId` and rebinds an already-selected Contributor model
to its provider profile for sessions retained from earlier versions.
The Contributor model can be hidden from the public picker, so its absence
does not prevent attempting the exact selector on the native catalogue route.
Swarm disables auto-update in its owned host process and leaves Contributor
mode and disclosure consent under the Owner's native configuration.
On 2026-11-24, Muse 1.4.25-R9361.1 preserved the `tbh` profile and still
returned the base model, both with native settings and with the process
override `--contributor-mode on`. A qualified Contributor route on this
release remains unproven.

Model proof comes from the exact session's MSP `log/subscribe` live stream
(`rawLog`). The adapter binds each provider success record to the active
run and its foreground task. Only the model recorded in the provider's
response facet can verify the selector. `session/tokenUsage.modelId`
can still echo the configured selector. On a mismatch the adapter sends
`turn/cancel` for that exact turn, suppresses its unverified output, and
blocks further input on the affected session. The Owner inbox and
`swarm why` show both model IDs.

Before `turn/steer` (including `inject active_turn`), the adapter makes a bounded,
read-only `session/read` with `excludeItems: true`. It requires a readable
history envelope and the exact session and running turn, then rechecks the event
actor immediately before writing. A failed read, unavailable projection, or
unproven binding refuses the steer before dispatch and retains that refusal for
the same Eliot command; it never changes the turn outcome or retries the input.
This guards the observed long-log origin-read failure without inventing a
checkpoint parameter: the installed `turn/steer` schema accepts no origin or
checkpoint authority. A successful read is a point-in-time observation, not a
guarantee against a later vendor failure; an already admitted steer still keeps
its honest applied/unknown result and is never retroactively called rejected.

Busy stop first sends exact-session/turn `turn/cancel` once and waits up to the
core stop interrupt bound for native `turn/completed` (`cancelled` maps to
`interrupted`). Acceptance alone is not terminal evidence; missing evidence keeps
the turn unknown. Force skips this cooperative window. After the window,
owned-host stop drops MSP stdin before its graceful wait. One absolute stop RPC
deadline bounds waits for control preparation, native exit/output drain, event
settlement and the reply, with time reserved for ordered event acknowledgement and response.
When stop preempts a snapshot, its deadline is captured before replying to the
displaced snapshot and carried into stop dispatch; that reply shares the same budget.
Stop requires observed process exit, drained output and acknowledged `session.ended`;
deadline expiry or uncertain drain stays unknown. Stopping does not supply a missing
terminal turn result or answer a retained input submission.

## Usage attribution

Verified read-only 2026-11-13 against installed Muse 1.4.0-R4161.1
(`muse schema generate-json-schema`, stable surface), and schema-checked
read-only 2026-11-14 against 1.4.20-R8619.1.
`turn/completed.usage` (`TokenUsage`) is the turn's aggregate token usage,
summed across the turn's model completions, and maps to `TurnUsage` at
`turn` scope bound to the exact session/generation/turn. Only the explicit
counters project (`inputTokens`, `outputTokens`, `reasoningTokens`,
`cacheReadTokens`/`cacheWriteTokens`); `cachedTokens` is
provider-convention-dependent (inside or beside `inputTokens`) and never
mapped, and no total is derived. The per-completion `session/tokenUsage`
events (raw counters plus server-derived counted-once
`promptTokens`/`totalTokens` and the session `cumulative` block with
`cost`), and the `session/contextUsage`
(`usedTokens`/`windowTokens`/`pressure`) occupancy triple, are
session-scoped facts: not turn expenditure, so repeated
completions can never inflate the terminal count. The adapter folds them
into the retained `SessionUsage` snapshot and commits it as `session.usage`
when accepted (sources `muse:session/tokenUsage` and
`muse:session/contextUsage`), with `observed_at` captured at acceptance:
the `cumulative` counted-once prompt/output/cache counters replace the
retained cumulative counters. Store projects uncached input by removing
cache reads from the cumulative prompt total. The server-computed
estimated `cost.usd` maps with unit `usd_estimate`, and each context
notification replaces both context counters (absent members become null,
never keeping the previous window). Missing counters stay
null. Live 2026-11-14: real turns commit `turn.completed` with no `usage`
field at all, so these session notifications are the only usage signal for
this harness; session facts never derive per-turn usage.

## Observation and recovery

Controlled MSP stream owns native lifecycle; view/page and output reads are bounded
readback, not a second concurrent producer. Use an exclusive view/revision cursor only
where the installed API guarantees it. Otherwise record a gap and reconcile current
state, never replay full history as new turns.

`session/viewHealthChanged` with `health="unavailable"` applies only to the owned
native session in that Agent generation. It records an explicit source gap and
propagates degraded `source.health` (`"stale"`). A foreign session's notification and an
unrecognized health value do not change that source. The push is best-effort:
read/resume also retain `history.noneReason`. `projectionUnavailable` degrades
observation and requires explicit degraded health on the unavailable view even when
the push is lost; `excluded` alone is normal history omission. Missing or unknown
reasons remain uncertain.
The daemon attention monitor reads `agent.snapshot` for the exact active turn
on a recorded gap or stale health. The adapter reads the owned session's
command plane (`session/read`). A running reply must name the exact turn.
Only explicit idle with a present-null `activeTurnId` permits a bounded
backward `view/page` scan for the exact durable terminal and optional final
`agentMessage`. A projection reconstructed from an unfinished source prefix
can contain a synthetic failed terminal while the native run continues.
The stream adapter holds `turn/completed` when the view is unavailable or when
the frame reports failure without a structured error. It marks the view unavailable
for monitor readback and keeps the next send gated on fresh explicit idle proof.

The daemon commits the proven terminal and available final text, or commits
`turn.unknown` after its bounded inconclusive reads. It retains the recorded
gap and source health. In `events.read`, the unavailable-view episodes are
`source.gap` events carrying the unavailable prefix. Their unknown settlements
carry the shared unknown prefix, and terminal readbacks carry
`view_recovery: "readback"`.

Verified 2026-11-13 using the installed host's offline stable and experimental
JSON/TypeScript exports: Muse Code `1.4.0-R4161.1`, executable SHA-256
`B33B493069A2593E97CC63F9A4063FEB64269BF7F07A233F5DB2DB681AD5D950`, stable
bundle fingerprint `sha256:36466f634c8c78a812462ec941187fd4547b232ee06153e5feb2a1482f0d3d7f`.
Verified on 2026-11-23 with Muse Code `1.4.25-R9361.1`. The installed stable
and experimental schemas both allow `model/list` immediately after the
`initialize` / `initialized` handshake. Its only parameter, `sessionId`,
marks the active row for an existing session. The result is a snapshot and
MSP v1 publishes no catalogue subscription.
[Model listing](https://meta-models.github.io/muse-code-sdk/1.4/generated/msp/methods/model-list/)
and [host operations](https://meta-models.github.io/muse-code-sdk/1.4/guides/host-operations/)
describe the query and the host's startup provider and XDG roots.

With a fresh data root, `serve` returned an empty `bundledCatalog` even
after `session/start`. The installed `muse exec --help` documents
`exec --list-models`, which fetches models and populates Muse's cache under
`XDG_DATA_HOME/muse/model-catalog` without creating a session or running a turn.
The adapter first reads the host's `model/list` snapshot. An empty snapshot
triggers one refresh with the resolved executable and environment from the
private plugin directory. The
refresh and each host query have separate deadlines. A fresh `serve` reads the
resulting cache even if the refresh fails. An exact selector missing from the
public catalogue is passed unchanged and reported with
`catalogue_omits_model: true`. An exact row
supplies effort variants when known, so a known model/effort mismatch fails
before a native start or resume. The session `modelId` confirms admission
before Swarm dispatches input.

Muse makes a profile's standing reasoning effort durable when
`session/setReasoningEffort` acknowledges the exact command. The adapter
checks the view suffix after the start or resume reply for a conflicting
effort report. The acknowledgement is sufficient when the suffix omits the
change. A settings mismatch reports the field and the expected and reported
values with redaction.

A promptless resume can open another generation without creating a turn.
Resume preflight compares the newest native turn with the newest retained
turn on the same Agent, including earlier generations of that session.

Pre-session-effect refusals (profile, catalogue refresh, spawn, initialize,
`model/list`, resume preflight) return a failed-state start
result carrying a short non-secret reason, and the launch settles failed,
never unknown; any failure after a native session effect stays unknown.
`projectionUnavailable` is the projection-failed-closed health reason on the
command plane, independent of view subscription membership. It is distinct from
`view/gap`, whose opaque `after`/`next` cursors are exclusive bounds on dropped
delivery. The schema documents cursor replay through `view/subscribe` and
contiguous forward `view/page` reads (1–1000 events, short pages allowed), but no
projection rebuild method or guaranteed re-arm after projection failure. v1 emits
only `unavailable` health. No numeric projection size/time threshold is published;
`projectionReadLimit`, `viewTruncated`, and `pageEventTooLarge` describe coverage
limits without supplying such a threshold. `item/readOutput` fetches known item
output; there is no dedicated turn-read method.

A view-health notification or a read reporting `projectionUnavailable`
records the exact session's gap. A gap can arrive between send dispatch and
turn association. The daemon binds it to that turn once its start commits.
Transport loss also records degraded coverage. The existing process custody
and source-drain path handles confirmed native endpoint exit.

An exact failed native terminal without a structured error needs independent
command-plane confirmation. A contradictory running turn, an unavailable
readback, or an unproven outcome leaves degraded coverage for the daemon to
inspect. The terminal's free-text reason is display-only.

Installed Muse 1.4.0-R4161.1 stable and experimental schemas (exported
2026-11-16) expose only `sessionId` and `excludeItems` on `session/read`.
There is no caller-supplied origin/checkpoint/full-replay authority to repair
there; `excludeItems: true` omits returned items, not the underlying fold.
`view/page` remains the documented cursor-paged source, but a failed read cannot
be replaced with an unfenced projection terminal: an unfinished log prefix may
still synthesize a failure while the native turn runs. No prompt, steer, resume,
or other session mutation is replayed to repair a read.
After a proven terminal, a fresh send checks the event actor before any
`turn/start` write. If observation is still unavailable or its check fails,
that send is rejected as not dispatched; the previous turn's native verdict
and source gap remain intact. Retained uncertain sends remain unknown and
are never replayed. A failure after a native
write retains the numeric native error and bounded stderr diagnostics instead
of replacing them with an empty generic unknown error.
The installed 1.4.0 schema and the vendor's
[backpressure guide](https://meta-models.github.io/muse-code-sdk/1.4/guides/msp-wire/backpressure/)
distinguish slow-subscriber delivery (`view/gap`, durable log untouched) from
materialized-projection failure (`projectionUnavailable`). Neither documents
the internal projection-failure trigger or establishes that projection failure
causes a native `failed` terminal with reason `incomplete`. An unavailable-view
episode alone therefore does not justify changing consumer bounds or claiming
that batching repairs the native failure.
Projection unavailability leaves the owned host available for stop, snapshot,
and Owner resolution. A later snapshot can still prove the exact native
terminal for `turn.resolve`. Readback preserves the historical view gap.

Guard a pending-request snapshot with a local observation counter so a newer stream
resolution cannot be overwritten by stale snapshot data. This is an in-memory race guard,
not another persisted Agent epoch. `notLoaded` alone does not prove session closure.
Parent completion does not complete children; aggregate usage must be marked explicitly.

Native requests open durably bound to the exact session, turn and native request id
with the offered choices or questions (child approvals keep their subagent origin as
provenance); the transport receipt acknowledges presentation only and the
decision travels as `approval/decide`, `userInput/answer` or `userInput/cancel`
with the exact ids, an offered choice, and the current requirement. Replies decide
from the current offer, never a stale snapshot: a stale choice is rejected. A
genuinely newer multi-stage requirement (same approval, strictly greater source
index) replaces the in-memory offer, is persisted under the exact pending id with
normalized `offer_seq`, and resets a consumed (Written) reply so the next stage can be
decided; an Unknown reply is never reset nor rearmed. Each Applied approval outcome records `decided_offer_seq`, and the next
explicit choice is admitted only once the persisted current offer is fresh past
it. Duplicates and stale offers change nothing; unknown stays fenced. A decide
control result is qualified against the authoritative view stream before it is
returned, within the single absolute deadline captured before the write (never
renewed): Applied only with exact proof derived from the owning path's
successful validation and required persistence — a terminal resolution naming this
generation's native decide id, or, after a clean admission ack, an offer
advanced past the decided requirement that the owning path validated (for
`approval/request`, the full request validation, so an offer the owning path
rejects, such as one with no choices, never qualifies) and persisted. A
rejected frame, a gap, or a failed persistence never qualifies, and no
pre-parse of a frame counts as proof. Persistence is explicit: the offer
helper reports success only after its actual commit, so a deferred or
gapped offer (including an oversized envelope) never qualifies and the
old Store offer is never treated as fresh. A terminal resolution must
also name the exact retained turn on the wire: a resolution naming
another turn is gapped before any commit, removal, or proof, and never
mutates the retained entry. Changed offers without admission,
absent requests, final text, and artifacts alone never qualify; missing or
mismatched deciders, stream end, and deadline expiry stay unknown. An
admitted-but-unproven decide stays Unknown with both the Core command id and
the native decide id retained: only a late resolution naming that exact native
id binds it, otherwise it closes unbound, and it is never replayed. Outcomes
carry only whitelisted diagnostics: the numeric native error code and the
resolved-decider present/match booleans. Only
schema-documented pre-admission/validation errors prove rejection (anything else is
unknown, never resubmitted). Approve and deny take an optional `choice_id` and optional `feedback` where
the choice accepts it; answer takes the universal `{"answers": {question_id:
{"answers": [string]}}}` mapping every offered question id to its canonical
strings. The public `request.opened` carries kind `approval` with the shared
`ApprovalRequest` (`summary`, `choices` with explicit `approve`/`deny`
decisions, `feedback_allowed`) or kind `question` with the shared
`QuestionRequest`; native binding, offers, `offer_seq`, requirement and view
cursors stay private in the retained pending entry, and every initial, refresh
and reopen branch republishes the normalized view. Native option labels become
canonical option ids (upstream carries no ids); option-bearing single/multiple
modes map to `single`/`multiple`, option-less questions to `text` with free
entry. Natively stated selection bounds pass through, absent bounds stay
absent, and negative bounds reject at describe. Canonical strings map back to
the wire shape the offered question requires: a `single` answer matching an
offered label travels as `selected_label`, anything else as `free_text`; a
`multiple` answer whose every string matches an offered label travels as
`selected_labels` (even one selection, within the offered minimum and maximum),
a single non-matching string as `free_text`, and mixes or empty answers reject.
The desktop keeps the two `multiple` shapes apart at input: typing an offered
label checks that option instead of submitting label-identical free text, so a
checked option and typed text never share a payload; unknown future approval
decisions stay out
of the public offer and no verb decides them. Resume pulls `approval/listPending` once
for its pointer set and opens only what still binds to the live turn; anything
naming the dead generation's turn is receipted and dropped as an orphan, never
answered, and a failed pull never fails the resume.

## Item normalization (MSP 1.4 stream)

Item shapes are established from the installed host's exported stable schema,
not from another harness or a guessed payload: `muse schema
generate-json-schema` from `muse-bin-1.4.0-R4161.1` (channel `muse-stable`,
bundle fingerprint `sha256:36466f634c8c78a812462ec941187fd4547b232ee06153e5feb2a1482f0d3d7f`;
the experimental surface carries the same nine `ItemKind` values; re-verified
2026-11-14 against `muse-bin-1.4.20-R8619.1`, whose stable surface adds
fourteen `ItemKind` values and whose experimental surface adds more, all
declared additive evolution and all ignored by the adapter's explicit-kind
match). The adapter
normalizes only what that schema defines: `toolCall` (tool calls, command
execution and edit-family calls) and turn-bound `userShell` (command
execution) revisions into the shared `tool.started`/`tool.completed` events
with the Codex adapter's DTO fields (`command_id`, `turn_id`, `item_id`,
`tool`, plus the verbatim native `status` on completion), bound to the exact
session/turn while the turn runs ([`NativeItem`](../../plugins/muse/src/native/types.rs)).
`callId`, `args` and result payloads are never persisted. `item/updated`
folds no per-delta events; the authoritative terminal `item/completed` carries
every committed tool fact. Turn-less `userShell` (the one kind outside any
turn) stays unmapped, as do `agentMessage` (already `message.completed`),
`workflow` (the workflow-child observer above) and unknown future kinds.
These committed facts advance liveness model progress like Codex tool events;
CPU alone still never counts.

A completed edit-family `toolCall` (`apply_patch`, `delete_file`,
`edit_file`, `write_file` per the installed host's `DIRECT_MUTATORS` set)
normalizes as the shared completed-file-change payload: `tool` `fileChange`
with the verbatim native `status`, plus bounded `changed_paths` and
`changed_paths_truncated` exactly as the Codex adapter emits for its
`fileChange` items, bound to the exact session/turn while the turn runs. The
paths come first from the item's inline `args` target (`path`); `args`
itself, file contents, diffs and results are never persisted. Git
observation alone cannot establish authorship: only this native hint
attributes Muse edits. When the item carries no inline target, the adapter
makes exactly one bounded `item/readOutput` fetch of the stored `patchRef`
(offset 0, 128 KiB page, 10 s deadline, before any commit and holding no
Store lock) and attaches only the file paths from the patch headers
(`*** Add/Update/Delete File:` and `*** Move to:` markers, plus unified-diff
`---`/`+++` lines only as adjacent pairs with the `a/`/`b/` prefix stripped
and never inside a `*** ... File:` section) in the verbatim
relative-or-absolute form the Store hint normalizer accepts. The patch body,
contents and args are never persisted. Limit: when the reference is absent,
unservable, or the bounded read fails or times out, the completed edit
commits empty `changed_paths` with `changed_paths_truncated` set; when the
stored patch runs past the first page, the covered paths attach with
`changed_paths_truncated` set, recording the coverage gap instead of
presenting Git state as authorship.

## Host permissions

Sandbox posture is fixed at owned host birth (`muse serve` flags); MSP session/turn
wire carries no sandbox field and posture is not negotiable per session. Profile
`permissions = "danger-full-access"` passes `--disable-sandbox` at host launch; omission
or `workspace-write` preserves the default sandbox posture. Read-only permissions remain
unsupported. The installed MSP schema carries no sandbox or permissions readback field,
so effective permissions are reported as unknown (`permissions: null`,
`permissions_verified: false`) and never derived from the requested flag or any
unproven wire alias. The host flag remains applied at birth; a real native tool probe
is still required to confirm effective posture.

## Worker MCP policy

The installed Muse Code 1.4.0-R4161.1 stable and experimental exports
(`muse schema generate-json-schema`, verified 2026-11-14) define native per-session
MCP additions at `session/start.config.mcpServers` and
`session/resume.config.mcpServers`. Initialization must request `sessionMcp` through
`capabilities.requestedCapabilities` and check `grantedCapabilities`. A stdio entry
requires `transport: "stdio"` and `command`, and accepts ordered `args`, runtime
`env` additions, `framing` (`auto`, `contentLength`, `lineDelimitedJson`) and `mode`
(`required`, `optional`). Defaults are automatic framing and required startup.
The intended Eliot server is the current executable with arguments
`["mcp", "--require-agent", "--data-dir", <Agent data root>]`, matching Codex and
Claude's Agent-only server. No Codex `-c` override or owner-global config edit is
needed by this native contract.

The earlier `-32603 / session_mcp_base_unavailable` observation is not a
documented configuration prerequisite or proof that this host cannot construct
session MCP servers. Follow-up on 2026-11-14 tested the documented
[settings directory](https://meta-models.github.io/muse-code-sdk/1.4/guides/extend/mcp-servers/)
in fresh private directories, without changing the owner's config or creating a
workspace `.mcp.json`. The historical probe set `XDG_CONFIG_HOME` to the
private config root and ran `serve --disable-sandbox` on the then-installed
1.4.0-R4161.1 binary. It called `initialize` with
`capabilities.requestedCapabilities=["sessionMcp"]`, sent `initialized`,
then called `session/start` with a fresh UUIDv7 `commandId`,
`modelId="muse-spark-1.3-contributor"`, `workspaceRoot=<workspace>` and
`approvalMode="allowAll"`. `HOME`, `USERPROFILE` and `XDG_DATA_HOME` were unchanged;
no feature switch, login, credential copy or model turn was used.

- **Empty settings plus session additions:** write
  `<private config root>/muse/settings.json` as
  `{"schema_version":1,"mcpServers":{}}`. Add
  `config.mcpServers.eliot_swarm` to `session/start`, containing
  `transport="stdio"`, the current swarm executable and arguments shown above,
  `framing="lineDelimitedJson"`, and `mode="required"`.
  Result: `sessionMcp` granted and session returned `status="idle"`, with no
  native error. A fresh private config root with no settings file also accepted
  the same construction, so a missing file alone does not explain the old error.
- **Server declared in settings:** write the following instead, and omit
  `session/start.config`. Result: the session again returned `status="idle"`,
  with no native error. The same results held without a preceding `model/list`.

  ```json
  {
    "schema_version": 1,
    "mcpServers": {
      "eliot-swarm": {
        "type": "stdio",
        "command": "<current swarm.exe>",
        "args": ["mcp", "--require-agent", "--data-dir", "<private Eliot root>"],
        "env": {
          "ELIOT_AGENT_ID": "${ELIOT_AGENT_ID}",
          "ELIOT_AGENT_GENERATION": "${ELIOT_AGENT_GENERATION}",
          "ELIOT_AGENT_TOKEN": "${ELIOT_AGENT_TOKEN}",
          "ELIOT_AGENT_DATA_DIR": "${ELIOT_AGENT_DATA_DIR}"
        },
        "required": true,
        "startup_timeout_sec": 5
      }
    }
  }
  ```

The settings-only copy follow-up (2026-11-14) copied the owner's actual
`settings.json` byte-for-byte into a fresh private config root, retaining its
`schema_version`, `model`, `reasoning_effort`, `tui` and both original
`mcpServers` entries. Neither `auth.json` nor `trust.json` was copied. The first
probe used Eliot's Windows
[`BASE_ENVIRONMENT_NAMES`](../../crates/eliot-daemon/src/plugin_host/host.rs)
when present, plus the private `XDG_CONFIG_HOME`; it preserved `PATH` and
`USERPROFILE` and injected no provider credentials. All four probes used the
same binary, workspace and `serve --disable-sandbox` arguments, negotiated
`sessionMcp`, sent `initialized`, read `model/list`, then submitted the same
`session/start` shape above with a fresh command id. Results:

| Config root / environment | Session MCP additions | Native result |
| --- | --- | --- |
| Unchanged settings-only copy / plugin minimum | Present | Accepted, idle; `providerId="muse"` |
| Unchanged settings-only copy / additional noncredential ambient variables | Present | Accepted, idle; `providerId="muse"` |
| Normal config root / plugin minimum | Present | `-32603`, reason `session_mcp_base_unavailable` |
| Normal config root / plugin minimum | Omitted | Accepted, idle; `providerId="meta"` |

The complete copy was accepted, so the conditional entry-removal/transport
bisection was not triggered: neither original server was removed or normalized.
`codebase_memory.command` is an existing absolute executable, with no arguments;
its explicit `CBM_CACHE_DIR` setting has no variable references. The original
`context7` transport spelling and URL were retained. No missing plugin `PATH` or
`USERPROFILE` defect was demonstrated. Owner settings bytes were unchanged
before/after every probe; private roots contained only `settings.json`, and all
owned hosts and observed descendants exited. No model turns were run.

This localizes the refusal to the normal config/auth/runtime context; it does
**not** establish an exact offending server entry, field, or native internal
cause. In particular, copied settings can select the unauthenticated `muse`
runtime while the normal root selects `meta`, so acceptance in the former does
not prove those entries work in the latter. No owner-config normalization or
plugin environment change is justified by this comparison alone. Native MCP
startup/readiness still has no readback here, and constructor acceptance is not
tool connectivity proof.

Construction success does not qualify login or authenticated tools. On this
Windows installation, `XDG_CONFIG_HOME` relocates `settings.json`, `auth.json` and
`trust.json`; the normal login is in the owner's config root. It does not move
`museHome`, sessions or the plugin cache: those remain under the data root
(`XDG_DATA_HOME/muse`, default `~/.local/share/muse`; `MUSE_SESSIONS` can override
the session store). This split agrees with the installed binary's embedded docs
and [host operations](https://meta-models.github.io/muse-code-sdk/1.4/guides/host-operations/).
Both config roots returned the same seven-model `providerCatalog`; the shared
data root contains the cached catalogue, so that is not login proof.

A real Swarm Agent using the private declared-server settings, the normal `HOME`
and data root, and no copied credentials launched successfully but its first
turn failed with `not logged in: run /login to add an API key`, classified as an
`auth` provider restriction. The private Muse config still contained only
`settings.json`. A second Agent in the same private daemon, with no
`XDG_CONFIG_HOME` pass-through, completed its real model turn with
`LOGIN_BASELINE_OK`. Thus relocating config admits native construction but fails
the required reuse of the owner's normal login without copying credentials. No
supported auth-file reference was established. Launches remain unchanged;
Agent-scoped MCP mail/reply/ack, var CAS and MCP child identity on resume remain
unimplemented and unproven. The earlier unknown launches were never replayed.

The installed MSP surface has no `config/read`, `thread/start` or
`mcpServerStatus/list` method, nor an MCP inventory in the session response.
Effective MCP inventory therefore remains unknown; schema support, a capability
grant and accepted configuration would not establish `mcp_servers_verified`.
The current [vendor MCP guide](https://meta-models.github.io/muse-code-sdk/1.4/guides/extend/mcp-servers/#what-a-stdio-child-receives)
also documents a cleared child environment whose allowlist excludes Eliot's Agent
credentials. Any future implementation must explicitly supply the matching Agent
generation identity through the native runtime `env` additions, register secrets
for redaction, and keep them out of files, argv and public semantic metadata.
This guide's published schema fingerprint differs from the installed host's;
actual child identity and connectivity still need live qualification.

Trust/HOME follow-up, 2026-11-15, against the same installed executable and the
owner's real config (now including the owner-approved static `eliot-swarm`
entry): eight no-model host probes made sixteen `session/start` calls. All used
`serve --disable-sandbox`, the same worktree and model selection, the Windows
plugin environment, and `sessionMcp` negotiation. The settings' `${ELIOT_*}`
references received noncredential diagnostic values, never a controller Agent's
token; no authenticated Swarm operation was attempted. Settings and trust state
were checked unchanged after every probe.

| Config / flags / environment | Ordinary session | Session with native MCP additions |
| --- | --- | --- |
| Owner config, no trust flag, `HOME` absent and `USERPROFILE` present | Idle, `providerId="meta"` | `session_mcp_base_unavailable` |
| Owner config, `--trust-workspace`, same environment | Idle, `providerId="meta"` | Same refusal |
| Owner config, no trust flag, `HOME=USERPROFILE` | Idle, `providerId="meta"` | Same refusal |
| Owner config, `--trust-workspace`, `HOME=USERPROFILE` | Idle, `providerId="meta"` | Same refusal |
| Fresh settings-only copy, no auth/trust copy, no trust flag | Idle, `providerId="muse"` | Accepted, idle |
| Same private-copy setup, explicitly request `providerId="meta"` | Idle, `providerId="meta"` | Accepted, idle |

The final two probes repeated the first two owner-config cases and inspected
only their own startup logs: those carried `tool_surface_version` metadata, not
a documented tool inventory. Both native session responses and `session/read`
also returned metadata only. The installed stable **and experimental** MSP
indexes expose no tool-list/MCP-inventory method; a successful construction
does not establish which tools the model would receive. Consequently the exact
condition under which settings MCP tools appear was **unverified** under
the no-model constraint. The flag did not repair the observed native base
refusal, missing `HOME` did not cause that refusal with `USERPROFILE` present,
and the reported provider label alone does not explain it. This is not proof
about the authenticated Meta model loop's tool surface.

The stored-trust case was **NOT_RUN**: there was no `trust.json` next to the
owner's settings at the documented location. The public guide states its
location but does not document its serialized format; no trust file/decision
was invented or written. This worktree also had no root `.mcp.json`.

Security boundary: the [MCP trust guide](https://meta-models.github.io/muse-code-sdk/1.4/guides/extend/mcp-servers/#trust)
distinguishes user settings servers (no workspace trust prerequisite) from
project `.mcp.json` files (trusted workspaces only). It documents trust from the
nearest repository root through the session directory; a worktree uses its own
files. Top-level help says `--trust-workspace` does not save trust; `serve` help
says it loads each session workspace's skills/rules, so the option applies to
every session/workspace loaded by that host. Per the guide it also admits
project MCP configuration, which can start programs/open connections; it is a
broader grant than adding one Agent-scoped server. Saved trust would persist
across runs. `--yolo` additionally disables approval and sandboxing and is not
an MCP configuration mechanism.

Do not enable workspace trust automatically as an MCP fix. If a later
tool-visibility qualification establishes that it is required, the narrow
plugin design is an explicit Muse profile opt-in, default false, frozen with
the Agent's workspace and passed only to that Agent's dedicated host. Document
the project MCP/skills/rules grant, preserve normal approval/sandbox settings,
never persist an owner trust decision, and do not infer opt-in from full-access
permissions or `allowAll`. No launch or profile behavior changed here.

The authorized two-turn model comparison on 2026-11-15 then tested the owner's
real settings/login directly, using separate native hosts (not Eliot):
`muse-bin-1.4.0-R4161.1.exe serve` and
`muse-bin-1.4.0-R4161.1.exe serve --trust-workspace`. No sandbox-disabling or
approval override was added. Both selected `muse-spark-1.3-contributor` on
provider `meta`, submitted exactly one `turn/start` with
`reasoningEffort="low"`, and used this exact prompt:

> List the names of every tool available to you, grouped by MCP server. Reply only with the list.

| Host | Model-reported `codebase_memory` tools | Model-reported `context7` tools | Model-reported built-ins |
| --- | --- | --- | --- |
| `serve` | None | None | 25 tools, grouped under `muse` |
| `serve --trust-workspace` | None | None | 31 tools, grouped under `muse` |

Both replies omitted `eliot-swarm`; its availability was **not qualified**.
No real Agent identity was borrowed or synthesized. The referenced Eliot
environment variables were defined as empty strings to avoid unresolved
`${VAR}` references while leaving `--require-agent` without credentials. That
optional server was excluded from the comparison; the two existing servers
were the qualification subjects. `HOME` and `XDG_CONFIG_HOME` were absent,
`USERPROFILE` was present, and both hosts used this same worktree.

The trusted reply contained the same 25 built-in names plus exactly
`subagent_cancel`, `subagent_read_result`, `subagent_send_message`,
`subagent_spawn`, `subagent_status`, and `subagent_wait`. Thus the flag broadened
the model-reported native delegation surface but was **not sufficient** to
make the two existing settings MCP servers visible in this tested config.
Do not add it automatically as an MCP repair. These are model-reported names,
not an MCP inventory API or proof of successful tool invocation; the underlying
settings-loading/startup cause remains unresolved.

Both turns reached matching native `turn/completed` with `terminal="completed"`
and returned final `agentMessage` items. There were exactly two submitted
turns, no retries, no tool calls and no approval requests. Both owned hosts and
their observed descendants exited; owner settings and trust state were
unchanged. Existing Eliot launch behavior remains unchanged.

This harness keeps `inherit` only: a profile that omits `mcp` resolves to
`inherit` (the `minimal` default applies only where a harness maps it), and
an explicit non-`inherit` policy is rejected as `unsupported` at config load. Verified read-only
(`muse --help`, `muse mcp --help`): `muse mcp` covers OAuth login/logout
only, and no `serve` isolation flag for MCP sources was observed, so no
`minimal` mechanism is implemented.

## Workflow children (native, observe-only)

The Muse plugin observes actual `workflow` item revisions through the existing
[`NativeWorkflowItemNotification` and `NativeWorkflowChild` DTOs](../../plugins/muse/src/native/types.rs)
and tracks each child with [`WorkflowChildKey`](../../plugins/muse/src/workflow_child.rs)
under `K(parentSessionId, workflowRunId, childId, attempt)`. The key retains
`attempt` as the exact signed SDK wire integer. One native `host.agent` child
is one Eliot Agent per canonical key — the key builds the collision-free
canonical opaque `native_ref`, registration id, and turn key (stable across
revisions; the shared core carries only the harness-generic registration
RPC). The wire carries no session or turn id on the child itself, so the ONE
normalized Eliot turn key is synthesized and is explicitly NOT an MSP turn id.

Turn-only coverage (`turn` family): `scheduled` registers the observe-only
tokenless child (Agent `Unknown`, no principal, no control verbs) through the
shared `ChildRegistration` RPC; the first live actual-wire state (`started`,
`usage`, `completed` without terminal) commits exactly one `turn.started`;
`completed` WITHOUT an explicit `terminal` never completes; an explicit
terminal commits exactly one `turn.completed` with the existing Muse status
mapping (`completed`/`failed`/`cancelled` to
`completed`/`failed`/`interrupted`, unknown terminals gap instead); a repeated
terminal revision dedups. Never `session.ready`/`session.ended` or
`message.completed`: there is no child session to fake (`childSessionId` is
null on the wire), and `resultRef`/`message` history composites are
unsupported — no fact is ever synthesized from them, and public history
composites stay with the shared-boundary owner for dispatch.

Workflow child revisions route independently of active-parent-turn buffering,
and child tracking continues after the parent turn completes. Each key
preserves its ParentSession/run/child/attempt binding plus the latest actual
wire revision/status/sourceRange.
Terminal stream loss unknowns open children (with a child gap when a turn
started) and keeps already-completed ones; transient view gaps self-heal
because every revision re-emits the full child list, so tracking continues.
Detailed tracking is bounded (256 keys plus 256 unknown markers), but the
bound never hides a proven child: an over-limit newcomer is still persisted
through one idempotent registration plus an observation-unavailable child
gap (visible Agent row, Unknown state, no turn facts, no replay), then marked
unknown — never evicting a proven tracked child. Final text, usage
provenance/transitive attribution, and timing stay unknown: no totals, and no
timestamp is invented from `durationMs`. No new wire fields are introduced.

## Route and final checks

Do not assume a generic base-URL override. Use explicit native account/billing profiles
and report what the host actually exposes. L7 covers lost replies, exact request/turn
binding, cursor/gaps, stale approval snapshots, child lifetime and long-session behavior.
No adapter-specific journal, proof receipts or secondary workflow model is imported.

## Ad-hoc launch

`swarm launch --harness muse [--model M] [--effort E] [--cmd PATH]
--workspace W "prompt"` is Owner-only and freezes a profile-equivalent
snapshot from safe defaults (`workspace-write`, `never`, explicit MCP `inherit`) plus
flags. An omitted `--model` defers to the server default whether or not the
catalogue marks a row with `isDefault` (an empty catalogue refuses launch); the
effective id is read back from the session. An explicit model is passed
unchanged even when the catalogue omits it. `cmd` is discovered from the launcher dir version
the launcher marks current (`muse.cmd` wrappers are never frozen); omitted
profile `cmd` uses this same discovery. The selected native executable is
frozen only in the launch generation. `--cmd` is the Owner's one-launch
override. On Linux the native payload
installs directly (`muse`
on `PATH`, `/usr/local/bin/muse`) with no launcher selection; the
extensionless binary counts as native only after an ELF or Mach-O header
check, and a script is never a hot-start candidate.
`swarm models --harness muse` returns the live native catalogue; its note
names the `isDefault`-marked row when the server marks one, and an omitted
`--model` still launches with the server default. An empty native read
reports `catalogue: "empty"`.
## Agent skill delivery (`eliot-swarm`)

`muse serve` offers no per-session skill flag: it loads each session
workspace's skills and rules (`muse serve --help`: `--trust-workspace`,
"Load each session workspace's skills and rules"). Writing the skill into
the checkout is forbidden and workspace trust is a broader grant that is
never auto-enabled. The plugin prepends a three-line pointer with the
daemon-materialized path and the Swarm operations that need the skill.
The first accepted native turn consumes the pointer. A rejected or unknown
start leaves it available for the next admitted input. A missing or unreadable
canonical copy fails the session start. Implementation:
[`plugins/muse/src/host.rs`](../../plugins/muse/src/host.rs)
(`skill_pointer`) and
[`plugins/muse/src/host/start.rs`](../../plugins/muse/src/host/start.rs)
(`spawn_owned_session`).

## Minimal-context measurement (k61, 2026-11-16)

Private daemon, identical trivial turn, `muse-spark-1.3-contributor`, low
effort, installed 1.4.0 binary, `inherit` (explicit; no minimal mapping
exists). The session carries no MCP tools (the model confirmed no swarm tool
is available in-session); the worker coordinates through the `swarm` CLI.
Turn usage: session input 45,444 + output 209. A follow-up turn ran
`swarm show` for its own record through the CLI and reported its agent id and
state, proving Agent-scoped access. `muse serve --help` exposes no MCP
isolation or tool-deferral surface, so both stay unsupported.

## Native subagents (`native_subagents` allow; deny unsupported)

Checked against the installed Muse 1.4.20 binary (`muse --help`,
`muse serve --help`, and the exported MSP schema
`muse schema generate-json-schema`, which names native subagent provenance
`subagentOrigin` / `subagentId` but no disable switch): there is no verified
per-session switch to turn off native subagents. An omitted setting freezes
`native_subagents = "allow"` to preserve Muse's native behavior. An explicit
`deny` remains unsupported and the launch/resume preflight refuses it before
native effects. Previously frozen snapshots retain their original value.

## Access levels (`--access ask|auto|full`)

Ad-hoc mapping, frozen in the snapshot and reported in effective settings:
`ask` is `approval=onRequest` + `workspace-write` (pending
`approval/request` decided with `swarm approve`/`deny`); `auto` is
`approval=promptUnmatched` + `workspace-write`; `full` is
`approval=allowAll` + `danger-full-access` (`--disable-sandbox`), exactly the
current full-access posture. Implementation:
[`crates/eliot-daemon/src/adhoc.rs`](../../crates/eliot-daemon/src/adhoc.rs)
(`apply_adhoc_access`).


## Observed subscription quota

Meta supplies subscription usage during a turn, not through a provider quota
endpoint. When the native MSP forwards `usage/changed`, the plugin retains only
the typed 5-hour/weekly percentages, resets and original `observedAtMs` as
`quota.observed` under the exact generation's session source. `harness.quota`
reads the newest exact-profile observation and labels it "observed during a turn".
A native `usage/read` observation is also accepted, under `details.usage`; an
empty read or unsupported MSP method returns `unsupported` with that reason.
No synthetic usage, probing turn or account identity is persisted. The pattern
reference is OpenCodex 2.84.0 `muse-subscription-usage.ts`; no code was copied.
## Follow-up turns after a settled turn (k167 E44, E46, E47)

E44: before dispatching another turn on a degraded view, the host checks for
the exact native session idle with an explicitly null active turn and a
healthy history envelope. When the check cannot prove that state, the send is
rejected before dispatch with the restart action. The historical gap stays in
the journal.

E47: the second turn failure with invalid run configuration MCP startup audit failed with MCP disabled for this runtime comes from the Muse host, not from Swarm state. Turn start carries only command id, session id, input, and the optional per turn reasoning effort. The model switch runs through session setModel at configure time. The MCP posture is fixed at host birth through the muse serve args for the frozen permissions level, so follow-up turns on the same Agent run the identical MCP policy as the first turn. A host level MCP disabled posture persists across turns on that host: the terminal commit latches it as host scoped state on the event actor, and every later send is rejected with the restart action until the Agent restarts on a fresh native host. View related failure classification deliberately excludes the MCP audit message, so idle recovery can never clear the posture latch.

E46 vendor note: event log failed with Origin read requires valid checkpoint suffix or origin preserved full replay authority is Muse CLI internal session event log text from muse-bin 1.4.0. Swarm reports it honestly as a harness origin native error and never invents a checkpoint parameter, since the installed turn steer schema accepts no origin or checkpoint authority. Workaround is a fresh Agent on the same worktree. If the message repeats, collect the Muse local tracing log for the session and report it to the vendor.
