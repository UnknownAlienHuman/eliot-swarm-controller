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

Native frame budget: the inbound NDJSON frame cap for the Muse host is 8 MiB
(`MuseStdioConfig::MAX_FRAME_BYTES`; every other adapter keeps the shared 1 MiB
default). The cap covers `view/page` readback (Eliot requests `limit: 1000`)
plus envelope overhead, along with the single bounded `item/readOutput` patch
page below (128 KiB of stored bytes). What still does not size this cap is the
full `item/readOutput` surface (6 MiB of stored bytes per page, exactly 8 MiB of
base64 before envelope overhead), which Eliot never fetches; and the installed
`muse-bin-1.4.0-R4161.1` schema (`muse schema generate-json-schema`) publishes no
per-surface text budget, so the true `view/page` maximum is unverified.
`session/resume` with `excludeItems=true` serves `history.mode: "none"` (no items),
so the resume response itself stays small when the host honors the option, which
Eliot verifies before proceeding. A frame over the cap ends the session with an
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

## Native mapping

| Eliot verb | Installed MSP mechanism to verify |
| --- | --- |
| launch/send | session/start then turn/start; readiness, input association and result separate |
| steer/interrupt | steer is turn/steer with the exact expected turn; interrupt is turn/cancel on the exact turn (halt running, keep queue; turn/unqueue targets queued submits, never used here) |
| inject | exact supported steer or explicit next-send prepend, mechanism reported; active_turn uses the actual turn/steer mechanism with the exact expected turn (`muse.turn/steer`), other points unsupported here |
| stop/resume | clear/pause native Goal as necessary and close owned host; resume exact session without old input |
| approve/deny/answer | exact session/child request id and offered choice/requirement; public details are the shared ApprovalRequest/QuestionRequest, answers are the universal answers map |
| configure | effort rides per-turn `reasoningEffort` in `turn/start`, resolved from the retained revision at send dispatch; a model change needs a new native session and is refused as `unsupported` (the frozen snapshot never changes) |
| goal | native set/edit/pause/resume/clear where installed schema supports them |
| compact/fork/background | native-only, advertised after implementation |

Owned-host stop drops MSP stdin before its graceful wait. One absolute stop RPC
deadline bounds waits for control preparation, native exit/output drain, event
settlement and the reply, with time reserved for ordered event acknowledgement and response.
When stop preempts a snapshot, its deadline is captured before replying to the
displaced snapshot and carried into stop dispatch; that reply shares the same budget.
Stop requires observed process exit, drained output and acknowledged `session.ended`;
deadline expiry or uncertain drain stays unknown. Stopping does not supply a missing
terminal turn result or replay retained start/input outcomes.

## Usage attribution

Verified read-only 2026-11-13 against installed Muse 1.4.0-R4161.1
(`muse schema generate-json-schema`, stable surface), and schema-checked
read-only 2026-11-14 against 1.4.20-R8619.1 (whose export carries no bundle
fingerprint field). Schema checks are not runtime proof: a live 1.4.20
launch refuses before any session (see below), so the profile stays on
1.4.0:
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
the `cumulative` counted-once input/output/cache counters replace the
retained cumulative counters (no total is derived), the server-computed
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
Private readback cannot clear a live-source gap, replay history or acknowledge a
terminal effect. A later healthy read does not establish live-edge recovery.

Verified 2026-11-13 using the installed host's offline stable and experimental
JSON/TypeScript exports: Muse Code `1.4.0-R4161.1`, executable SHA-256
`B33B493069A2593E97CC63F9A4063FEB64269BF7F07A233F5DB2DB681AD5D950`, stable
bundle fingerprint `sha256:36466f634c8c78a812462ec941187fd4547b232ee06153e5feb2a1482f0d3d7f`.
Re-verified read-only 2026-11-14 against Muse Code `1.4.20-R8619.1`
(`muse schema generate-json-schema`, stable and experimental surfaces, no
model calls): every shape the adapter models or calls is unchanged or
additive-optional (new methods, notifications, `ItemKind` values, error
kinds, and optional fields only; no removals except the orphaned
`SessionDelete*` defs the adapter never referenced). Schema-level safety did
not hold at runtime: a live 1.4.20 stdio probe sending the same
`initialize` and `model/list` calls the plugin sends gets an empty
`bundledCatalog` (`models: []`) where 1.4.0 serves the provider catalogue,
so the exact-model lookup fails before any `session/start` is dispatched
and the launch is refused. Do not switch the profile to 1.4.20 while its
catalogue serves no models. Pre-session-effect refusals (profile, spawn,
initialize, `model/list`, resume preflight) return a failed-state start
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

During an associated turn, the adapter attempts one read-only `view/subscribe`
with `after` equal to its last successfully applied owned-session cursor. It
records that lower bound and the returned subscription head (when available)
on the source gap before processing replay. When the gap lands before turn
association (while `turn/start` awaits its reply), the acknowledged association
attempts the same subscribe and arms the same recovery deadline, so the turn
cannot stay busy with no settlement. Replay uses the same event actor and
exact session/turn/command binding as live delivery; a replayed cursor that was
already applied skips normalization instead of committing twice. Only a
successfully processed cursor after the observed subscription head, followed by
acknowledged `ok` source health, can restore live observation; the historical
gap stays visible. An actual replayed terminal can complete the exact turn while
source health remains stale. An interrupt refused during the gap names the
unavailable-view reason explicitly.
When the recovery window expires without live proof (or recovery cannot be
attempted), the adapter settles the active turn `unknown` with the recorded gap
through the shared turn-unknown path (`turn.unknown` with the exact
command/turn binding and the gap reason), never as success or failure. The turn
binding clears only after that terminal write is acknowledged; a failed write
keeps the binding and retries on the next opportunity instead of stranding the
Store turn. The plugin host stays alive and the Agent is not marked lost; the
session remains for stop/readback and owner resolution, while a later send
queues behind the unknown-turn gate until that turn is explicitly resolved
(core.md turn/input gates). Later facts for that turn gap instead of
resurrecting it, and interrupt/stop afterwards behave as for any unknown turn.
Subscription admission alone, a private page, or a timer never proves native turn
completion or process/session exit. Live recovery after projection failure remains
unqualified until exercised through Swarm by the controller.
PR #422's shared native-exit lifecycle helper still applies only after actual
endpoint exit; projection unavailability alone cannot invoke that exit path.
The projection path reuses the turn-unknown settlement without retiring the
plugin or reporting a stopped session to stand in for lost observation.

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
the wire shape the offered question requires (`selected_label`,
`selected_labels` or `free_text`); unknown future approval decisions stay out
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

This harness keeps `inherit` only: the frozen profile always records
`mcp = "inherit"`, and a non-`inherit` policy is rejected as `unsupported`
at config load. Verified read-only (`muse --help`, `muse mcp --help`): `muse
mcp` covers OAuth login/logout only, and no `serve` isolation flag for MCP
sources was observed, so no `minimal` mechanism is implemented.

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
