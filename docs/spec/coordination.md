# Coordination

## Registered artifacts and cleanup (K69)

The shared wire DTOs are in
[`api/cleanup.rs`](../../crates/eliot-core/src/api/cleanup.rs).
`artifact.register {id,path,kind,purpose,expires_at?}` admits an existing directory owned by
the authenticated Agent generation. Optional `expires_at` sets an explicit Unix-millisecond
expiry bounded by `MAX_ARTIFACT_TTL_MS` (30 days); CLI exposes `--ttl DURATION`. It captures the opened filesystem identity,
project and generation-bound ancestor chain. Before native startup Swarm creates
`<first-configured-artifact-root>/<agent>-<generation>/`, or
`<data-dir>/artifacts/<agent>-<generation>/` when none is configured. Creation is
exclusive: an existing unbound directory is refused, never adopted. Its opened
identity is committed as an internal `ownership_root` ledger row,
and the native process receives its path as `ELIOT_ARTIFACT_ROOT`. Agent-created
directories must use absolute local paths strictly below that durable root; location in a
workspace or a shared configured root alone proves no ownership. Legacy rows
outside this binding remain kept. Roots themselves, reparse points,
tracked source, overlapping claims, the daemon data directory and native/user
configuration are refused. Kinds accepted from Agents are `build`, `data_dir`,
`temp`, `other`; worktrees and branches come only from Swarm's retained workspace
preparation. Declared profile scratch directories are recorded only in singly
owned workspaces and when inside the Agent's durable root. The frozen
`CARGO_TARGET_DIR` captured from a profile that passes it through `pass_env`
is recorded when it lies outside the Agent
workspace and inside the Agent's durable root. Private daemon data
directories are recorded only by the Agent that created them through
`artifact.register`; a daemon lock or a live process holding one keeps it.
Existing worktree and native-subject facts are also reconciled
when planning, without scanning arbitrary filesystem roots.

`cleanup.plan {scope:self_tree|project|all,project?,older_than?,continuation?}` changes
no filesystem objects. It retains a bounded snapshot with per-item size, reason and
`safe`/`keep` verdict. The cutoff is an exclusive Unix-millisecond creation time.
Manual plans select `registered` rows. Settled rows (`removed`, `unknown`,
`skipped(...)`) are attempt history read back from retained plans, and filtering
them before the page cap keeps a settled prefix from starving later eligible
rows. The Store pages the final candidate order once (ordinary rows, then
worktrees, then branches and native sessions, each oldest insertion first), and a
truncated plan returns an opaque `continuation` token: the last shown artifact, the
consumed project-row offsets, the settings revision those offsets were read at, the
ordered project IDs and verdict bands those offsets were computed against, the
agent-backfill position, and the ledger snapshot the pages are pinned to. Rows
inserted after the snapshot surface on a fresh listing, never inside a page; a moved
settings revision, a reclassified project (a checkout appearing or disappearing moves
it between kept and safe without moving the revision), or an unresolvable
cursor rejects the continued page as stale. Automatic retirement filters the exact Agent,
generation and kind before the same cap.
Agents are restricted to their frozen and current subtree; Owner selects project
or all. Terminal means Stopped or Failed with no open native source, pending
command or retained non-exited owned process. Lost remains conservative.
Clean managed worktrees require exact reciprocal Git/common-directory identity.
Tracked changes and untracked or ignored paths outside frozen scratch paths keep the
worktree. Guarded cleanup can remove approved relative scratch files.
Native stores are never deleted as files. A Codex or OpenCode session of a
terminal Swarm-created Agent is `safe` with reason `native_archive_pending`.
Attached or observed sessions and every other harness stay
`keep(no_native_archive)`.
Owner-initiated worktree cleanup and direct `worktree.remove` accept clean
checkouts on unmerged branches. Age-based cleanup requires the merge proof below.
After the existing terminal-owner, borrower, assignment, age, reciprocal Git
identity, cleanliness and filesystem-guard checks pass, Swarm removes only the
checkout and its private worktree administration directory. The local branch
retains its exact tip. A dirty checkout stays kept. #922 owns the snapshot step.

Local managed-branch cleanup requires an explicit cleanup selection or an
applicable cleanup policy.
After confirming the branch's planned tip is still current and no worktree uses
it, one Git ref transaction creates `refs/eliot/archive/<branch>` at that exact
tip and deletes the local branch name. A conflicting archive ref or changed tip
keeps the branch. If Git exits unsuccessfully and both refs match the preflight
readback, cleanup retains the refusal code and bounded Git detail. A ref-name
prefix conflict reports `archive_collision` and names the blocking ref.
An uncertain transaction remains `unknown` until exact branch
and archive readback settles it. Archive refs stay local. An explicitly selected
remote-branch deletion uses its existing remote-state checks. Stand
plan/apply and `stand down` retain their independent root, registry, marker,
owner-binding and #885 two-pass cleanup checks.

Plans anchor the managed workspace identity and branch tip. Apply rechecks the
same identity and exact local tip before mutation. Missing or changed local identity,
checked-out branches, filesystem guards and Git readiness still keep/refuse the
candidate with their concrete reason. A present
`daemon.lock` with a live owner keeps a data directory. Only a provably dead
owner releases it. Artifacts carrying `expires_at` stay `keep(not_expired)` or
`keep(stand_not_expired)` until `expires_at`. Stand plan and apply refuse the
parent's own data root and apply the same protected-root, issuing-root,
handle-identity, marker and owner-binding checks as `stand down` before any
removal. Expiry only extends retention: an
expired artifact still keeps `retention_age` until the ordinary retention window
has passed. Expired registered private stands
(`purpose: SWARM_STAND_ARTIFACT_PURPOSE`) then verify external registry binding,
clean shutdown (`daemon.stopped`), and clean `#885` preflight on external-repo
worktrees/branches before surfacing `safe(expired_stand)`.

`cleanup.apply {id,plan_id,artifacts?}` consumes only the plan's safe candidates, rechecks
authority, generation, expiry, Git cleanliness, opened identities and busy/link refusal,
then records each outcome as `removed`, `skipped` with reason, or `unknown`. For an
expired stand candidate, apply performs `#885` two-pass external worktree and detached
branch cleanup under `WorktreeWriteGuard` + `OwnedChild`, keeps the stand if any
external item is kept/unknown, and deletes the offline stand root and external registry
entry only when external cleanup is complete. An
optional `artifacts` list (at most 64 unique ids, each a safe candidate of that
plan) attempts only those; the plan's other safe candidates settle
`skipped(not_selected)` without an attempt fence, so a later plan offers them again.
The selection is part of the request binding.
Deleting an Agent tree supersedes all of its items in unapplied plans at the
deletion commit. Apply keeps the original selection binding and records those
items as `skipped(superseded)`, without reserving an attempt or touching their
artifacts. Other items proceed under the same plan and checks.
The Store commits an unknown attempt before effects and settles results before
acknowledgement. Exact same-ID replay returns the retained response. Another ID
cannot consume the plan; another pre-existing plan cannot retry an attempted
artifact. Uncertain attempts fence resume at admission and dispatch. Windows
pins ancestors, opens the entire bounded tree through relative handles without
delete sharing for every directory and file until disposition/close, refuses
reparse points and deletes by handle disposition. Any file whose link count is
not exactly one keeps the entire tree with `hard_link`, including Cargo targets
with shared binaries. Guarded scans at both plan and apply refuse repository
roots (including bare repositories), every `.git` entry, and native/user config
descendants. Only a clean, retained Swarm worktree may include its exact
reciprocal `.git` pointer, verified by opened identity; nested repositories stay
protected even there.
Unix reports unsupported rather than using pathname unlink. Worktree directory
and its exact private Git administration directory are removed; its local branch
is retained when the worktree is removed through worktree cleanup. A selected
managed branch is archived and deleted in one local ref transaction after
current-tip verification and confirmation that no worktree uses it. A refused
`worktree.remove` command carries its stable code and the originating bounded,
sanitized message, with the failure location in `where`.
Remote branch deletion remains a separate explicit Owner-only flag
(`delete_remote`) on `cleanup.apply`. Its existing remote-state checks remain
in force. A remote failure after local deletion is `unknown`.
Native sessions are archived at apply through the harness documented archive
method (Codex `thread/archive`, OpenCode session delete on the retained
per-Agent store) using the Agent's frozen profile. Only a Swarm-created Agent's
session is eligible. Attached and observe-only sessions stay with their producer.
Claude Code's [background-session removal](https://code.claude.com/docs/en/agent-view#what-deleting-a-session-removes)
keeps the transcript. Muse keeps its durable host sessions. Antigravity 1.5.14
advertises ACP close, load, list and resume, and keeps its chat history.
Artifact
scope/reference must match that generation's native subject. The Codex reader
verifies its effective home/SQLite root against the frozen store binding before
dispatch; a legacy Codex home scope derived from an alias missing from the
frozen binding remains ineligible for Swarm archive, and only a new launch can
capture a verified binding for its own session. The OpenCode reader proves its
served store derives the exact retained native scope before the delete. The
reader records a proven-absent target as removed even if its former children
remain. A present target with native children stays in place because the
vendor delete would also remove the children. Each child needs its own
Swarm-created retained session artifact to qualify for cleanup. A delete
acknowledgement or 404 counts as removed only if the same server, store and
directory give a complete session list with the exact target absent. An authoritative
refusal settles as skipped and keeps the session. A timeout or malformed or
secret-bearing readback after dispatch is `unknown` and retains the durable
retry fence. The metadata reader owns the vendor helper through shutdown. Raw
native store files are never touched. Logical reclaimed bytes count only fully
confirmed removed trees; partial attempts report unknown and zero confirmed
bytes.

`agent.delete` runs the same guarded effects for the selected task tree's
eligible native sessions and managed worktrees before retiring the task records.
It records cleanup outcomes before retiring ownership and keeps the task records
if an attempted effect is refused or uncertain. Session cleanup runs first.
If a selected session is kept or uncertain, pending worktrees in that task
tree settle as `skipped(native_session_kept)`. This also applies to a
`cleanup.apply` selection that includes both effects.

Continue a cleanup review with the opaque `continuation` string from the previous
response and the same scope, project and age cutoff. The CLI passes it as
`--after <token>` and MCP passes the string as `params.continuation`.
The daemon also accepts legacy JSON continuations. A progress page can contain
zero candidates with a continuation while the daemon scans the retained inventory.
The backfill cursor advances only across completed probe batches. A failed probe
rejects the page. A time slice that ends mid-batch can return progress through
the last completed batch, with the unfinished batch retried on continuation.
The desktop advances through those pages and offers Next page for more candidates.

Owner plans with scope `all` or a registration ID in scope `project` include
project registrations from the current settings revision. A creation-time cutoff
excludes these entries because the settings do not record their creation time.
The plan marks a registration safe only when its configured checkout path is
missing, no live or unsettled Agent uses the registration or checkout, and no
unsettled project-principal command uses it. A present or unverifiable path,
or active use, keeps the entry with a reason.
Apply checks the plan's settings revision, path and Agent use again, then removes
the selected stale entries in one revision-checked write through the private
settings writer and records the change in settings history. Uncertain project
attempts retain their receipt fence for that settings revision. The Store revokes
their registration authority while retaining Agent project labels, workspaces
and archived history. No filesystem tree is deleted by a project-registration
candidate. CLI callers pass one `--artifact <id>` per safe registration selected with
`cleanup apply <plan-id>`. The desktop's Project
registrations group starts unchecked.

The current limits are 64 candidates, 4,096 retained artifacts and 1,024 retained
plans. On Windows, the [`CleanupTree` artifact-directory scan](../../crates/eliot-daemon/src/platform/script_file.rs#L391)
is bounded to 10,000 filesystem entries, depth 64 and five seconds per scan.
Managed-worktree removal uses a separate [write-fence inventory](../../crates/eliot-daemon/src/platform/worktree_guard.rs#L34),
bounded to 20,000 entries and depth 64 with a ten-second deadline for each
prepare and activation pass. Exceeding an applicable bound refuses its operation
before filesystem deletion. A full plan ledger self-heals
on retain first: it reclaims fully settled consumed plans (no `unknown` item
outcome), consumed plans whose receipt was pruned while their own settlement
is terminal and known (none of their item artifacts is still `unknown`), and
abandoned never-consumed previews older than the write-id retry window,
oldest first, so ordinary preview/apply cycles never wedge it. Plans with
an outstanding `unknown` attempt and their receipt fences are never reclaimed,
and a receipt-less consumed plan with an uncertain outcome is kept, never
replayed and never given an invented outcome; the full-ledger refusal then
names those stranded plans as the blocker. Only
when nothing reclaimable remains does exhaustion reject new admission. Exact
apply retries check the retained receipt before loading the plan row, so a
reclaimed settled plan still replays its retained response instead of `not_found`.
Write retention pins every consumed apply receipt while its plan row exists, so a
settled receipt can never prune before the ledger reclaims the plan and wedge it
permanently ineligible. Removed identities remain tombstones; unknown identities are never
automatically retried. The CLI exposes `artifact register`, `cleanup plan` and
`cleanup apply`. Desktop Settings › Worktrees & cleanup (Clean up finished work)
reviews an Owner `all` plan grouped by kind with each item's size and reason,
applies the safe items of the checked groups through the same Router after one
confirmation (local deletions only, one mutation id per plan and selection until a
definite reply), and shows removed, kept-with-reason and unknown outcomes. The plan
has no Owner per-Agent scope, so there is no per-task cleanup entry.

The daemon's automatic sweep pages through recorded owned directories of finished
Agent generations and uses the same guarded cleanup path.
Manual previews can backfill older ownership records. The [Owner settings]
(configuration.md#daemon-and-automation-settings) control the age and enablement.
Both the directory record and its exact finished generation must be older than
the configured window. Automatic apply starts after the first complete plan-only
sweep of each daemon startup. Each completed paged
sweep updates aggregate status and sends one Owner `notify` with would-remove and
kept totals and enablement; intermediate pages do not send notices. Per-item
`cleanup.automatic` reason events remain available.
Eligible paths include managed worktrees, recorded per-generation plugin directories,
and artifacts under exact Swarm-created generation roots, including archived terminal
Agents. Automatic worktree removal requires a clean checkout and a local Git ancestry
proof that the current base contains the branch. Dirty or unmerged worktrees are
kept with their sizes for the Owner. Disjoint owned artifacts can age out independently.
A generation root can be
removed when its creation identity is still bound, its registered descendants are
settled, and the guarded scan of its contents succeeds. Branches, project registrations
and native sessions require an explicit cleanup request. Status reports the policy
and the latest completed sweep's aggregate outcomes since daemon startup. A
`cleanup.automatic` event records removals, planned removals, uncertain outcomes
and new keep reasons. Repeated keep reasons are omitted from later events.

A folder assignment opts in with
`cleanup_on_retire`: after an applied stop retires its Agent, the existing
stop worker runs the same plan/apply for that Agent's build, temp and
worktree artifacts with Owner authority and retained receipts. It is off by
default and is not a second executor.

Add at L5, initially between two Codex Agents. Mail, shared variables, Git-backed `who`
and advisory claims reuse the existing API/router/Store; no second scheduler.

## Authority

Principals are owner, current `agent:<id>#<generation>`, scoped automation and
`memory_inference:<stable-client-id>`. Memory has only the
[M1 inference surface](api.md#memory-one-shot-inference-m1-199), no Agent/control/
coordination/config authority and no Owner/tokenless fallback. Only its current
credential admits new mutations; exact own reads/retries survive bounded rotation
overlap. Backup dependencies are authenticated Memory-owner assertions, serialized
with its backup-set transitions; the controller stores no pins or physical backups.
Current-credential old-call read/release after a post-promotion restart gates previous
credential retirement. Missing/
revoked identity rejects. An Agent's frozen profile `control` (self/tree/project,
default self) governs its commands and reads: self reaches only itself, tree reaches
its own subtree, and project reaches its frozen project authorization scope. The
scope is the original launch label or the scope inherited from the parent. Recording
a repository registration ID for display leaves that scope intact. The owner is unrestricted. A
current Agent may queue addressed mail to any existing Agent regardless of project
label or worktree; mail is communication only and does not grant command control.
Project rules access project coordination; vars, claims and control remain as defined
below. Only owner/global automation writes
global vars. Agent-launched children stay within parental Eliot scope; a frozen
Owner-written `child_profiles` entry explicitly grants a different harness's own
native policy, as defined under [Claims and hierarchy](#claims-and-hierarchy). Only the Owner
may grant a different manager control through [reparent](#change-the-manager-implemented);
[Handoff](#task-handoff-to-an-existing-agent-implemented) between two workers of the same
manager needs only that manager's own controlled scope. A native child without isolated
environment has no independent Eliot principal; calls from its parent environment are
attributed to that parent and this limitation is shown.

Pending request replies (`approve`, `deny`, `answer`) for a generation whose
frozen launch snapshot has `access=ask` require the Owner. The Agent itself,
its manager (including tree/project control), and non-Owner automation cannot
reply, through CLI or MCP. Admission and dispatch enforce `not_allowed` before
any native reply. For `auto`, `full`, and omitted access, existing frozen
control scope governs self and manager answers; this rule grants no new rights.

Queue mail is communication, not control. Full-text `send|steer|inject` presentation
requires ordinary command rights. No message content is executed as a router instruction.

## Project manager assignment

A registered project can have one current assignment. Its manager is a typed
local Agent ID and generation or a future remote manager instance and grant
revision. Store retains the current identity, monotonically increasing epoch,
state and revision. Workspaces have independent preparation receipts and current
[workspace bindings](monitoring.md#workspace-preparation). Remote grants require
their own verified, project-scoped ingress identity and are unsupported in 2a.

Claim and manager launch contend on the same Store transaction. The first wins
and the competing caller receives the current record with a typed reason.
Release advances the epoch and retains the vacant record. Resume advances the
manager generation and epoch atomically with dispatch. The Owner can replace a
manager through [project transfer](#project-transfer). Release revokes Swarm authority and
leaves existing execution to its native lifecycle. Dispatched or unresolved
native startup blocks release. Ordinary startup of a separate root is refused while the
assignment is occupied. Restart reads the existing record. A heartbeat timeout,
lost process or uncertain startup never frees an assignment. Owner retains explicit
administrative and recovery authority. A scoped self-claim requires a live eligible
root Agent in its own registered project and an Owner grant.

Managed control requires both ordinary scope authorization and the current
assignment. Admission freezes the project, generation and epoch used for
control. Dispatch checks the same binding before work starts, including queued
launches. Already dispatched and unknown effects keep their original bindings
and outcomes. Mail and authorized history reads retain their existing scope.
Project mutations, including vars, claims, workspace setup and cleanup, must
respect current authority. Automation uses the same command fence. Legacy rows
with no assignment history retain ordinary control. Adoption must account for
existing writers before claiming exclusive workspace custody. Assignment is
Swarm control authority. Filesystem isolation depends on the native sandbox.

A manager launch in managed multi-project mode reserves the assignment,
the workspace and capacity in one Store transaction before any external
effect. The daemon resolves the registered repository, verifies its identity,
freezes the base OID from the local HEAD without fetching, and prepares a
fresh opaque worktree on a unique branch through the existing [preparation
and setup chain](monitoring.md#workspace-preparation). Native dispatch starts
only once that worktree is ready. A retry under the same operation ID returns
the retained result; after a restart the daemon reads back the existing
preparation instead of preparing a second worktree or re-running an unknown
setup. A confirmed preparation failure or cancellation settles the launch and
clears the assignment, binding and unstarted capacity together. Uncertain
preparation retains a blocked assignment with the workspace and start command ID.
The Owner can explicitly release that epoch. Release preserves the unknown
command, workspace receipt and capacity, and fences subsequent startup.

Workers launched by that manager inherit its project assignment and workspace
automatically through the existing parent link. They run in the manager's ready
worktree under the current assignment fence and can reuse a frozen profile with
setup entries. Workspace setup runs only during preparation of a new worktree.

`project.overview` reads fleet totals and paged project detail from one
committed Store snapshot, identified by its event watermark. The Owner can
page through all detail. A live assigned manager can page through its current
team within its frozen Agent read scope. Other teams, including their native
children, appear as aggregates. A released manager or worker receives
aggregates. Authorized detail includes assignment state, revision and workspace
binding, including adopted workspaces.

Live harnesses, dispatched starts, active turns, waiting work and retained
unknown reservations use their respective committed generation, command, turn
and reservation evidence. The fleet separately counts distinct observed native
children, including any without a project. Observation coverage describes what
the daemon can observe. The page continuation describes which authorized rows
remain. Queue-depth reporting carries no scheduling promise; fairness and
queued interactive launch follow [command admission](api.md#command-admission).

The current workspace binding to the assignment is recorded separately from
the historical creator receipt, so a later transfer can move the binding without
rewriting history. Cleanup planning, cleanup apply and worktree removal keep
bound worktrees and worktrees with nonterminal borrowers, including after release.
Explicit adoption verifies the canonical checkout and common directory, rejects
overlap with retained bindings, managed worktrees and live Agent workspaces, and
freezes the approved profile for native startup. It keeps the existing checkout
environment and creates no setup effects or worktree cleanup artifact.

## Conversation association

A conversation association links an existing Agent or chat to a project or
display group. It leaves native session, execution workspace, permissions and
historical command attribution intact: association is placement, never
authority. The headless association API stores the overlay and its link
revision beside the existing Agent or chat identity (schema v64 and v65; v66
adds the switch's `continued_from` pointer).

- `chat.organization` returns the grouped projection: teams by project
  assignment, manager, repository or worktree, and ancestry, with
  event-watermark readback. Clients never repeat Git discovery or grouping.
  Chats group by their overlay link alone; a legacy chat that still carries
  an execution project reads in the no-project bucket until attached.
  A non-chat Agent groups by its execution project when that is a retained
  registration, else by the registration of its exact retained checkout
  path (normalized separators, verbatim prefix and case); the retained
  label is never rewritten, and a path two registrations share stays in
  the no-project bucket.
- `chat.attach` moves chats under a retained project; `chat.detach` returns
  them to the no-project bucket. `chat.tag` replaces tag groups. Every write
  advances each written link's revision; a write carrying the revision it
  read is refused whole when another write moved it since. Each mutation
  returns the committed per-Agent links; each fresh write appends one
  `organization.changed` event in the same transaction, naming each written
  Agent's resulting group, tags and revision, so the projection's event
  watermark advances and clients refresh loaded rows by id.
- Continuing work in another project stays a separate, authorized execution
  operation through the existing switch/handoff path, not an organization
  mutation: a running Agent cannot change execution scope through an edit.

Migration keeps every ID and all history. Links are derived only from
unambiguous retained facts (registration, common directory, parent); never
from names or the last message. Retained roots competing with a project's
assigned manager surface as an explicit ambiguity and are never
auto-reassigned; a project without a manager only needs assignment. Rename and
archive reuse their existing paths; presentation preferences stay in the
existing settings. The wire contract is [Chat organization](api.md#chat-organization).

## Mailbox

A stable Agent owns its mailbox. One row is one concrete recipient delivery:

The [`MailDelivery`](../../crates/eliot-core/src/api/coordination.rs) DTO owns the
mailbox delivery field catalog.

Standalone recipient deliveries use `<write-id>/<recipient-agent>` identifiers.
Admitted [`mail.send` rule children](automation.md#actions-are-commands) use
`rule:<ULID>/<recipient-agent>`: the ULID combines the child's persisted admission
timestamp with deterministic child-identity entropy, outside the public write ID
namespace. Both forms retain the timestamp needed to classify pruned deliveries
for reads and idempotent acknowledgements. Each creates atomic concrete recipient
rows from the frozen recipient list in one bounded transaction.
The frozen list is not reevaluated later. One admission names 1–100
concrete recipients. Same id with different identity, mode, body or wake policy rejects; unknown
recipient never creates an Agent. Text is at most 32 KiB, known secrets rejected.
Only `queue` mode is connected; `send`, `steer` and `inject` presentation
return `unsupported`.

`recipient_generation` records the addressee's incarnation at admission and binds any
native presentation command. It does **not** make queued correspondence disappear when
the same Agent resumes. A current authorized mailbox read may return older unread rows,
visibly labelled with their original generation. Old presentation commands expire stale
and are never replayed against the replacement. A new full-text presentation requires
explicit fresh command admission and its normal authority checks.

Replies reference the exact delivery and original participants, never an inferred latest
conversation. Native turn success is separate from message delivery/read state.

### Modes and wake

`queue` stores text only. `send`, `steer`, and `inject` use ordinary generation/turn-bound
commands and report their actual application point. Presented is not read or understood.

Senders can set `wake: false` on `mail.send` (`swarm msg send --no-wake`).
Deferred deliveries stay unread until a SEND to their generation
claims them for its native input or the recipient acknowledges them explicitly.
They do not trigger a wake or qualify for the reply cooldown bypass. The next
Swarm-supplied input, including a linked launch or resume SEND, an automation
SEND, or an existing mail wake, can carry the oldest eligible deferred
deliveries that fit. The remainder stays queued for later input or an inbox read.

With `wake_on_mail=true` (read from the recipient's frozen profile snapshot,
default true), a routed recipient may receive one coalesced notice per minute,
with at most one open notice command. The count-only fallback is
`N unread: mail.inbox, then mail.ack`. An unread reply whose
`reply_to_delivery` names mail the recipient itself sent, admitted after the
last presentation, skips the one-minute cooldown once, so an answer to an
Agent's own question wakes it at once; the open-notice, busy, pending-input
and unknown gates still apply.
The notice is an ordinary owner `send` bound to the recipient's
current generation. At claim, Store selects eligible unread mail addressed to
that generation, in admission order. Each inline entry carries its sender,
thread topic (or an explicit no-topic marker), exact delivery ID, and body.
Peer mail carries the sender's Agent identity and a [quoted data fence](#quoted-data).
Swarm-generated mail carries the service sender `swarm`. Completion messages
quote the child excerpts and leave status lines and counters outside the
fence. Other service notices quote their body. Owner mail uses plain text.
An actionable attention report can be inlined on its one permitted wake.
Older-generation mail and retained advisory copies remain in the inbox.
If other wake-eligible unread rows
remain, the text gives their count and points to `mail.inbox` and
`mail.ack`. A completion-digest delivery remains eligible only when its
service message matches the digest registration, the current recipient
requester (`agent:<recipient>:<generation>`), and that registration's digest
thread. Other registered digest rows are not treated as ordinary mail.

Store binds every inline delivery to the exact notice or ordinary send command
in the same claim transaction. Committing `turn.started` for that send stamps
`presented_at` and `read_at` on those rows in the same transaction.
For a send merged into a harness-started turn, `turn.input_bound` is the exact
receipt. Model output, a successful terminal, or an explicit mailbox
acknowledgement confirms delivery. A failed, interrupted, unknown or lost turn
with no such evidence returns its provisional acknowledgements to unread.
Wake mail is eligible for one automatic re-offer under the usual cooldown.
A rejected, failed or cancelled SEND without a start leaves its rows unread
and eligible for a later claim. Unknown SENDs without a start keep their binding. If no
current-generation delivery qualifies, or the serialized
`send` request including the complete rendered content and any unread
remainder line exceeds `MAX_EVENT_BYTES`, the notice falls back to the
count-only text and binds no rows; overflow never partially presents a batch.
A prior presentation binding can be claimed again when its command is missing
or settled as `rejected`, `failed`, or `cancelled`, or its accepted turn ended
and its acknowledgement was rolled back. The delivery must remain unread and
unpresented. The active and unknown gates still apply. Command readback
redacts inline notice text. Deferred mail text is attached only to the native
input and stays out of retained send arguments. The Router strips internal
notice metadata before plugin dispatch. Unknown
notice is not blindly retried;
provider hold, pending request, current unknown gate and drain suppress new wake.
A busy, waiting or input-pending mail recipient suppresses a new mail wake:
a current-generation `send` already accepted, queued, dispatched or unknown
but not yet `turn.started` still reads idle in that gap, so the wake is
deferred (skipped, not queued) rather than dispatched after the inbox was
already read and acked. Ordinary user sends queue FIFO.
The claimed `send` carries explicit `notice_kind: "mail"` metadata.
A send naming the open notice with a missing,
wrongly typed or other value is invalid, with no inferred default, while
normal sends never inspect it. Mail revalidates the busy, pending and
unknown gates at admission via its open-notice identity; the Store never
parses notice text. Internal `notice_kind` remains Store admission metadata
and is stripped at daemon dispatch, so vendor plugins receive only text arguments.
Deferred mail re-offers on idle through the same atomic claim. An idle event
caused by that recipient's own notice command cannot trigger a re-offer.
Deferral leaves the cooldown timestamp unchanged. A claim preserves the prior
`last_notice_at`, initially 0, and a failed admission clears only the matching
open claim. Settlement of a notice `send` as `rejected`, `failed`, or `cancelled`
stamps the shared cooldown and clears that exact open claim in the same
transaction. Restart recovery clears only commandless open claims.
Command retention clears the open reference of the exact completed notice it
deletes. Native acceptance starts the wake-attempt cooldown
(`turn.started` stamps `last_notice_at`).
A daemon driver re-pages due recipients every 2 seconds: current `idle`,
non-observer agents holding unread mail past the normal cooldown are
re-offered through the same atomic claim, so an agent that idles with no
new event still wakes once due. Normal queued mail and older-generation
unread rows can therefore surface as a fresh current-generation count notice.
Only current-generation ordinary mail bodies are inlined; older-generation
bodies remain in the inbox and are represented by the remainder count.
A recipient with no live plugin owner for a dispatched generation stays
queue-only.

Without independent recipient identity, route only a generic notice to a live routed
parent/manager, otherwise stay queue-only. Parent notifications follow the same policy.
No routine acknowledgements/status chatter or broadcasts are generated by default.

Automatic Agent-attention reports route a bounded mail pointer to the manager who can
act: the parent manager receives an ordinary queued mail through the existing wake
policy, with `children_unattended` routed to that manager itself. When no eligible
manager exists, the owner receives the ordinary local notify event and bus report. No
advisory self-copy is generated. Notices carry a short bounded diagnosis and point to
the `x.user.agent.attention` event for details; they contain no native transcript or
replay of the Agent's prompt or job.

The legacy advisory-attention messages retained by older versions can be previewed and
purged by the owner-only `maintenance.purge_legacy_attention_advisories` operation. The
default call reports an exact count across read and unread rows; applying requires that
count and rechecks it atomically before deletion. Scheduled retention also removes
expired unread advisories for terminal recipients using the same
[legacy advisory provenance](core.md#retention). See the [maintenance API](api.md) contract.

A completion digest (`swarm.digest`, see [api](api.md#waiting)) reuses the same
ordinary queued mail and wake policy for a different purpose: exactly one coalesced
parent result when a captured child set satisfies `all`/`any`. The digest mail id is
the digest registration id, so a reconnect or restart replays the same mail id and
the existing mail stable effect identity makes the retry a no-op. The thread is
`completion-<id-prefix>` (never `attention-*`, never `-advisory`), the body lists
every frozen entry outcome with its turn identity once, `truncated` and a fairly
allocated bounded `final_text` excerpt for authorized completed bound turns
(entries outside registration-time scope stay `not_visible`, also on a retry
after they move into scope; entries where the recipient lacks scope at
delivery—including Owner-registered digests and workers reparented away before
completion—deliver status-only, rendered by request position without the id or
any turn identity, the scope recheck and the mail insert sharing
one Store lock; NUL bytes are escaped as `\0` before budgeting) within
`MAX_MAIL_TEXT_BYTES` (32 KiB) with a batch-wide excerpt reserve (see
[api](api.md#waiting)), and no attention event is emitted: a successful
completion stays distinct from an attention incident. Registration ids share the
public write-id namespace: every public write path (`mail.send`, `mail.ack`,
`var.set` and groups, commands, generic writes, claims, cleanup, handoff,
resolution, and all other retained mutations) with an id bound to a completion
digest rejects `id_conflict`, symmetric to the command/write checks at
registration. Only the exact registered digest-delivery conversion reuses the
id: the Store verifies the pending registration and requires the mail to target
exactly the frozen parent recipient on the frozen `completion-<id-prefix>`
thread before binding anything, so a mismatched request through the digest
path (wrong recipient or wrong thread) rejects with `id_conflict` without
consuming the pending id. That conversion is exempt from the new-write age gate (its age
is the registration's age), so a digest stays deliverable however long its
child set takes. Mail and delivered mark commit in one Store transaction, so a
restart never recomputes a changed body: the registration reads delivered
immediately, an exact same-body replay returns the retained mail, and a changed
body under the consumed id conflicts instead of overwriting. A retained receipt
advances the delivered mark only with evidence it was that digest's conversion:
the same mail body to the frozen parent recipient on the frozen
`completion-<id-prefix>` thread; a legacy receipt on another thread leaves the
digest pending. Delivered digests are pruned under
the commands retention window (see [core](core.md#retention)), oldest delivery
first; pending digests are never pruned.

A `child.run` digest follows an interrupted child turn to its exact automatic
restart continuation. Startup waits for the initial restart pass to record
that binding before delivering pending digests. Explicit turn waits retain
their original turn identity.

A child's delivered completion can reopen its idle parent after restart. The
mail re-offer checks the parent's exact closed generation, proven custody exit,
and unread result from its own digest registration. It rechecks the generation
and Stop and archive fences before admitting Resume. Normal mail wake runs
when the resumed session is ready. This uses the existing digest and mailbox
records and leaves other mail queued for terminal Agents.

### Quoted data

Swarm uses the shared Core fence for text from Agents, processes and files when
it inserts that text into a model input. The delimiter uses at least three
backticks and exceeds the longest run in the body. Add sender, file digest,
completeness and counts outside the fence. The complete frame counts against
the delivery budget. Shortened data retains a complete fence and an explicit
completeness mark where the delivery reports truncation.

### Read and acknowledgement

`mail.inbox` returns at most 100 unread rows within the encoded frame limit for the
authenticated stable Agent, including older generations with their original metadata. The page may be
narrowed by `thread` or by one exact `reply_to` delivery id, and `wait_ms`
(clamped to 30,000) long-polls for the first matching delivery. Every delivery
appends one bodyless `mail.delivered` event bound to the recipient generation
(a rule child's delivery also carries that command's cause), so the long-poll
wakes without reading any body.
`mail.read` returns one exact row. Both
are read-only. After CLI output completes:

```text
mail.ack { id: <write id>, deliveries: [<exact delivery id>, ...] }
```

The current valid principal may acknowledge only its own recipient rows; owner access
is explicit. A stale token cannot read/ack after revocation. Ack is idempotent and cannot
affect messages that arrived after the fetch. Lost output/ack may cause redisplay, never
silent message loss.

Do not prune unread correspondence solely because generation changed. Bound admission
with `mail.max_unread_bytes_per_agent` (default 10 MiB of message text, from the
config-file `[mail]` table); overflow returns `mailbox_full` without a partial
fanout. `mail.max_unread_messages_per_agent` (default 10,000) also bounds
metadata for empty/tiny messages. Acknowledged/history retention is bounded by
`daemon.retention.mail_days` (unset disables pruning; unread mail is never
pruned); reads of removed deliveries return `history_gap`, while a retried
ack over them succeeds as already acked.

The [Store migrations](../../crates/eliot-store/src/schema.rs) own the
`messages` schema and inbox/thread indexes.

## Agent self-wake (#833 item 3)

`agent.wake {id,condition,timeout_secs}` is available only to the authenticated
Agent. Store binds it to that Agent's current generation and exact running turn,
retains the acknowledgement before returning, and reserves a separate stable
`delivery_command_id`. The response lets the Agent end its turn immediately.
There is one active wake per Agent generation and 1,024 active wakes daemon-wide.
The default timeout is one hour; the maximum is 24 hours. Terminal wake records
are retained for 25 hours.

Conditions are one of:

- `file_appears {path}`: relative to the caller's ready, retained managed
  worktree, at most 4,096 bytes and 64 path components. Only a regular file
  below that root can satisfy it; traversal, links, reparse points and
  worktree/generation changes cannot. If it already exists at admission, it is
  immediately satisfied.
- `process_exit {identity}`: exact `host_boot_id`, `pid` and `start_time` from
  the caller's current `agent.processes` or workspace-process scope. The Store
  verifies the owner is that exact Agent generation; an unknown observation
  does not count as exit.
- `argv_exit0 {run_id}`: wait for an existing `run` command. Admission rechecks
  the command's retained owner-approved rule origin and requires the frozen
  event's Agent, generation and workspace to match the caller's exact current
  binding. Timer runs and runs without that event binding are ineligible. Only
  an applied `ScriptRunApplied` receipt with known zero exit, quiescent custody
  and known custody reports success. A definite `script_exit_failed` receipt
  with a known nonzero direct exit also wakes the Agent with its exit code and
  bounded redacted stderr tail from the retained Run outcome. Failed receipts
  without that exact exit proof do not claim a nonzero code; malformed or
  unknown receipts remain unknown. This condition never starts an Agent-supplied
  argv.
- `child_completion {turn}`: one exact retained turn of a direct child in its
  current generation. Reconciliation uses the same exact-turn projection as
  `swarm.wait`; an unknown child turn does not count as finished.
- `seconds {seconds}`: elapsed from durable admission time.

A condition may become true while the caller's turn is still running. The
daemon records one bounded fact, then waits until that exact turn is completed
and the caller's generation is still current before submitting one ordinary
`send` with the reserved delivery command id. Definite pre-admission refusals
retry with bounded backoff for at most five total delivery submissions; the
terminal `failed` record retains a bounded, redacted reason in its existing
fact. The command id is checked before retry after a crash. A retained unknown
command is never replayed; an outcome that cannot prove delivery remains
`unknown`, and a definite rejected/failed delivery is retained as `failed`.
Generation replacement or timeout retires the wake without delivering into
another incarnation. The bounded worker scans keyset pages and reads the exact
process, run, child-turn, workspace and due-time facts; it does not create a
second rules engine.

An open or same-turn self-wake suppresses the
[automatic background-process wake](automation.md#automatic-background-process-wake),
and arming a self-wake cancels a waiting automatic wake.

Example Agent MCP call, then end the current turn:

```javascript
swarm({
  method: "agent.wake",
  params: {
    condition: { kind: "file_appears", path: "out/result.json" },
    timeout_secs: 1800
  }
})
```

The response includes `delivery_command_id`. After the wake message arrives,
read the retained delivery result with:

```javascript
swarm({
  method: "command.get",
  params: { id: "<delivery_command_id>" }
})
```

## Shared variables

Scopes encode as `global` or `project:<name>`; tree scopes need hierarchy
membership and return `unsupported`. JSON values are at most 16 KiB encoded,
known secrets rejected. Each write increments version; `expect_version` is CAS,
not last-writer guess: an omitted expectation writes unconditionally, while a
supplied one must equal the current version, where an absent row reads as 0.
`set_by` records the current principal. Changes emit `var.changed`: one
bodyless fact naming scope, key, version and `set_by`. It names no Agent, and an
Agent's event filter admits it for the scopes that Agent can read. `var.get`
takes an optional `after_version` baseline; `wait_ms` (clamped to 30,000)
requires it. The read returns at once when the version already differs, and
otherwise long-polls for the matching version bump; an absent row reads as
version 0.
Concurrent queued writes use bounded [Store commit grouping](core.md#storage);
each request still has its own CAS result and retained idempotency binding.

The [Store migrations](../../crates/eliot-store/src/schema.rs) own the
`vars` schema.

## Who works on this

`who` queries existing L3 projections: current-generation native edits whose paths remain
dirty, Git dirty files in a singly owned worktree, branch differences from frozen base,
advisory claims and optionally bounded recent history. Git history is provenance, not
current ownership. Unattributed dirty bytes in a shared checkout stay unattributed.
A private chat Agent is a current worker with `chat: true` and no project; it never
fails roster validation and never widens scope (an Agent caller still reads only its
own project, the empty chat scope included).

[Monitoring](monitoring.md#git-and-filesystem-monitor) owns `agent_edits`; coordination
does not declare a duplicate table. Git-clean paths remove edit rows. Old generations
are not presented as current workers. Inject Git identity in environment, not repo config.

## Claims and hierarchy

Claims are project-relative globs with note/TTL, owned by Agent generation. Release on
terminal state, generation change, detach, expiry or explicit request. Conflicts produce
one coalesced notice, not a lock or a hidden work reassignment.

`agent.patch_digest` compares changed paths against the target generation's active
claim globs when the digest is read. `outside_claims` lists paths matched by none of
those globs. A complete claim read with no active claims returns
`outside_claims_declared=false`, an empty list, and
`outside_claims_complete=false`. A missing or incomplete claim read omits the
declaration. This is
advisory; claims do not lock work, and live rows do not reconstruct which glob was
active when an edit occurred. The list is incomplete when Git path coverage, the
claim listing or the bounded path list is incomplete; known outside paths may remain
listed when only Git coverage is partial. For a shared workspace it considers only
generation-attributed edit paths and stays incomplete rather than attributing the
whole workspace diff to one Agent.

On Windows, advisory matching and overlap use case-folded spelling, and path
comparison accepts Windows separators. Goal scope keeps its existing matching
rules. Git reads patch paths with rename detection disabled, so both the removed
and the added path take part in the scope comparison.

```sql
CREATE TABLE claims (
  agent INTEGER NOT NULL, generation INTEGER NOT NULL, glob TEXT NOT NULL,
  note TEXT, expires_at INTEGER NOT NULL, created_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,glob)
);
```

An Agent principal or the owner launches a child through the ordinary launch path
with `args.parent`. An Agent with current `control=tree` or `control=project` can
launch its own children. A caller with `control=self` receives `not_allowed`
before profile or workspace preparation for both prompted and promptless launches.
Store admission rechecks the current generation's control before allocation.

The child takes the parent's project authorization scope and tree root at one
deeper depth. A prompted launch's linked first `SEND` uses the launch caller's
authority. The new child is in scope through its subtree link for `tree` and its
inherited project authorization scope for `project`. The parent link is durable.
A launch parent must be a controlled Agent. Under [Authority](#authority), a child
reuses the parent profile's frozen snapshot or names a
profile from the parent's captured `child_profiles` allow-list. An explicit profile
bypasses routing; a `role` selects candidates intersected with that frozen list
(including the inherited parent profile). A later config edit cannot widen the list.
An allow-listed cross-harness profile is an explicit Owner grant of that profile's
freshly frozen harness, model, permissions and MCP policy. The child gets its own
credential/selector bindings; parent bindings and native policy names never cross
harnesses. Approved same-harness profiles can set their model, effort, and daemon
context ceiling under the parent's native authority and captured credential bindings.
Every frozen account or proxy capacity scope of the parent
is kept with the same bounds, alongside the child's own scopes; conflicting bounds
reject. Control is narrowed or equal. The daemon freezes `profile_snapshot`. An Agent
child shares the parent's workspace by default. It can request its own Swarm-managed
worktree and branch in the same repository. A supplied workspace must match the
parent's. Creating a child worktree requires the parent to be on a local branch.
The frozen base is the parent's `HEAD` by default. An explicit base must resolve
to that branch tip or one of its ancestors. Swarm applies the integration branch
name rules and refuses an existing branch for a new child. The Store verifies the
parent's retained repository identity and reserves the branch at admission.
It creates the branch and checkout under the usual managed-worktree preparation
path, leaves the parent's checkout and index intact, and retains the child's branch
for integration and proof-based cleanup. Children inherit the project policy in
either workspace mode. Fanout limits (`[daemon.limits]`, checked at launch and re-parent admission)
prevent accidental new work but never hide an Agent row that already exists.
Above-limit children remain visible with no widened identity/control; over-limit
admission reads `fanout_limit` and persists nothing. Terminal Agents (stopped,
failed, lost) count toward neither fanout bound. Tree selectors (`tree:<agent>`,
the Agent plus its descendants in Agent-id order) expand once at admission for
`send` and `stop` only, using the Store-only parent plus ordered concrete children
representation with `<id>/<agent>` child ids; parent applied means expansion, not
children finished, and children settle independently with the same gates as targeted
commands. Any live-member failure rejects the whole expansion, except a tree
`stop` skips observe-only native children and reports them in the parent outcome
`skipped` list with reason `observe_only` instead of failing; their lifecycle
stays with their manager/native harness. `send` skips
terminal members instead of failing on them (an all-terminal send applies with no
children); `stop` still names every controlled member, including terminal ones.
A plain `stop` affects only its target:
parent completion or stop never marks children complete and sends nothing to them.
`stop --tree` is the only stop that names a subtree, as an explicit expansion, not
a cascade. Native-child observation is implemented for Codex (3 families) and Muse
workflow attempts through the shared
[`ChildParams`](../../crates/eliot-plugin/src/protocol.rs) and
[`ChildRegistration`](../../crates/eliot-core/src/native_child.rs) contract.
Muse workflow-attempt observation registers an observe-only tokenless
[`Agent`](../../crates/eliot-core/src/lib.rs) bound to
$K(\text{parentSession}, \text{workflowRunId}, \text{childId}, \text{attempt})$ with
generation 1, unknown state, normalized Eliot turn key rather than native MSP turn
identity, strictly turn-only coverage ([`MUSE_NATIVE_CHILD_FAMILIES`](../../crates/eliot-core/src/native_child.rs)),
no native MSP turn identity/child principal/control, and private history
([`agent.history`](../../crates/eliot-daemon/src/router.rs)) returning `unsupported`
at the owning daemon dispatch boundary. Child control stays a later increment,
as do tree forms for mail recipients (non-Agent ids stay `invalid`) and watch filters
(a tree value matches no events today).

### Integrating Agent branches

A manager with tree or project control can submit `git.integrate` for an Owner-registered
project that allows it and matches the manager's verified repository. The request names a
local base branch and the branches to combine. The daemon runs the project check in a
temporary checkout and retains the result under the command ID. The manager can read it
with `command.get` or wait through the [CLI](api.md#cli).

The manager can explicitly fast-forward its own bound branch after a passing check when
the branch is clear of all worktrees. The integration keeps the manager's working tree
and index intact. See [the automation contract](automation.md#local-branch-integration)
for the Git operation and failure readback.

### One-call child run

`child.run` uses one `ChildRunRequest` / `ChildRunResponse` DTO pair
([Core API](../../crates/eliot-core/src/api/child_run.rs)). An Agent starts
with `{text, sources?, profile?, worktree?, base?, wait_ms?}`. The MCP mutation wrapper supplies
the operation id and returns it with the response; direct callers provide a
ULID. The id is the ordinary launch command id, so the normal command binding
retains the principal, payload digest, launch and linked first input. A retry
with that same id cannot admit another child or first input.

The request uses the caller as `args.parent` and goes through ordinary child
launch admission. The current Agent generation and `control=tree|project` are
checked there; `control=self` refuses. An omitted profile reuses the parent's
frozen profile. An explicit profile must pass the captured child-profile
allow-list and existing same-harness/cross-harness checks. The child's first
input is `args.text`, committed as the launch's linked `SEND`; `sources` uses the
existing bounded source delivery and replay path.
`worktree` and `base` use the same rules as an ordinary Agent child launch.

`wait_ms` defaults to 20,000 and is limited to 25,000 ms per call, leaving
response time within the native 30-second MCP/plugin RPC deadline. `wait_ms:0`
takes an immediate durable snapshot. The same method continues with
`{id, wait_ms?}` and reads the same launch's exact linked first input; it never
launches or sends again. A pending or `unknown` result, and every zero-wait
request, registers the existing completion digest over that one linked input.
The registration id is a deterministic sibling ULID, preserving retry
identity while keeping the digest reservation separate from the launch
command id. Existing digest delivery provides one durable completion mail
wake, including after a daemon restart. A digest retry reuses its registration
and mail identity.

The compact response contains the exact input status, bound Agent/generation
when visible, the retained managed `branch` when available, a `branch_error` code
when that read fails, bounded retained
`final_text`, `truncated` and the shared bounded
`ChildRunFacts`. The same facts appear on visible settled child results in
`swarm.wait`. Visible waits for the child's launch, input or turn can also return
the managed `branch` for the matching generation. Use it in a `git.integrate`
request. The CLI serializes those `WaitEntryResult` fields. Fact collection runs
only after the final child-wait snapshot, once per exact Agent turn, so polling
does not repeat Git reads. `changed_files` comes from a complete owned workspace
view or generation-bound `agent.edits` rows (up to 8 paths of at most 128 bytes, with `truncated` for
omitted or overlong paths). The exact-generation Goal `eliot` check includes its
exit code and check time.

Workspace-wide Git line counts, observed commit ids and paths are reported only when the
target child generation is the sole non-terminal Agent bound to that workspace;
the cached Git view must be observed at or after that turn's completed time.
Paths and counts use the complete view and commit IDs use the generation's
retained Git observations. An earlier or undated view leaves workspace facts
unobserved. A shared workspace leaves
those facts unobserved. With active claims, a shared-workspace `outside_claims`
fact may be observed `true` when a generation-attributed edit path matches none
of the child's claim globs; lack of such a mismatch does not prove `false`. With
an exclusive workspace and complete Git/claim evidence, the flag compares all
dirty and committed paths and may be either value. No active claims, stale or
missing bindings, unknown/pending results, partial Git/history, or an unavailable
or truncated claim list leave the affected fact `unobserved`. Old-generation
claims do not count. Claims remain advisory. `unknown` remains an explicit result
and never triggers an effect retry. The same operation id continues reading the
retained first input.

## Project transfer

The Owner uses `project.transfer` to move a registered project's assignment to an
exact local Agent generation. Each action has its own mutation ID and expected
assignment revision. Same-ID retry reads the retained result, including a wait
or refusal. Use a fresh mutation ID to check a changed condition. Remote grants
remain unsupported.

- **Begin** retains an intent, keeps the assignment occupied and fences new A
  controls and automated work addressed to A. Owner readback, replies,
  interrupt and stop remain available. Other workers can finish work in their
  existing native sessions. Unsettled commands, approvals, unresolved Goal
  rounds and a blocked source Goal are reported. Source custody must close when
  A has a workspace. Idle alone supplies no writer-exit evidence.
- **Launch**, if needed, prefers a compatible idle root in A's actual workspace
  on the requested approved profile. A new root uses ordinary launch admission
  and reservations. The staged replacement holds a slot in the current
  assignment as well as in the project and fleet. A full capacity scope returns
  a capacity wait after continuation policy and source blockers have been checked.
  The Owner may authorize provisioning after stop by supplying saved
  continuation context at Begin, with the project launch action permitted.
  The daemon requires owned-process exit, reader drain and closure of A's
  workspace helpers before admitting that launch. Windows Job custody supplies
  the required guarantee. A platform whose custody result leaves escaped
  descendants possible returns a blocker. The daemon retains the launch command
  for readback and holds unknown custody.
- **Commit** requires the exact idle compatible B. Store advances assignment
  revision and epoch, moves the current workspace binding and worker topology,
  and transfers advisory claims and the paused Eliot Goal in one transaction.
  Only controlled workers move. Native observations keep their recorded
  parentage and roots. The managed workspace and its Git artifacts follow the
  successor's retained start so cleanup can find them after the old manager is
  deleted and the assignment is released. The original preparation identity is
  preserved. If the receipt remains with an earlier owner, deletion is blocked
  while the path is bound or borrowed. Worker generations, references and
  reservations stay in place. A successor team over its assignment limit and
  unsettled queues block the transaction. Queues keep their
  retained principals and assignment epochs are preserved. Project-owned rules
  stay project-owned and targeted automation follows current membership.
  Saved continuation context and any expressly selected source SENDs enter the
  ordinary Handoff delivery gate in the same transaction. The Owner may cite a
  retained original task and select ordered plain SENDs from the exact source
  generation that the transfer drain fence rejected during dispatch preflight
  or that source stop cancelled before dispatch. Their successors preserve
  the original principals and wait for the brief to become Applied. The
  retained handoff parent reports brief delivery and the original-to-successor
  command links. Failed or unknown brief delivery keeps successors queued.
- **Cancel** removes the intent and lifts its drain fence once any staged
  replacement has settled without open custody. Owner can stop a staged B
  through ordinary recovery.

The transfer receipt links to the [Handoff](#handoff) command for current
delivery state. Commit blockers roll back assignment and handoff writes
together. Exact replay reads the same admission and schedules any accepted or
queued handoff children while the daemon is open. Dispatch still checks their
retained state and generation.

## Handoff

Handoff uses durable commands, generation fences, parent links, advisory claims,
Goals and ordinary input/mail. It adds no task engine, queue, lease or ownership
database. The native harness keeps its loop, context and process custody. The
[L7 lifecycle donor](../reference/donors.md) informs commit-before-ack and exact
current-state checks: uncertainty preserves the existing binding, never authorizes
another external effect. Managed-agent thread/orchestration docs likewise keep
thread context, interruption and completion separate.

### Change the manager (implemented)

`swarm.reparent` is an Owner-only, generation-bound Store transaction. It moves
one Agent and its whole subtree, or snapshots all direct children of a source
manager and moves their subtrees together. The source manager itself stays put in
the latter form. Both forms include terminal historical descendants. An empty child
set succeeds with an empty result. A same-parent move is harmless but still validated.

A manager cannot transfer its children to another Agent it controls: that grants
the recipient authority over existing sessions and could bypass frozen launch
confinement. Owner-only admission makes that grant explicit, including cross-harness
or unlisted-profile children. The child's profile, control, credentials, native
identity, workspace, project, generation, turn, commands, Goal and claims do not
change. `child_profiles` constrains Agent launch, not this Owner grant; no snapshot
is widened or replaced, and the new manager cannot use the grant to launch an
otherwise forbidden profile. All moved rows must share the new parent's project
(the authorization scope, `COALESCE(project_scope, project)`).

The destination must be a controlled, nonterminal, non-unknown Agent. Observe-only
sources or descendants reject the whole move: their parent binding belongs to the
native harness. Terminal controlled children may move (including back after a stop).
Active or unknown turns in controlled children do not prevent a link-only edit;
no native input is delivered or replayed. Exact source and destination generations
are checked in the transaction. Reject cycles, including a destination within any
moving subtree. Recompute every affected `root` and `depth` from the destination.
Enforce maximum depth for every moved row, destination live direct-child/tree
fanout, and the selector-target bound on the total moved rows. Terminal members
occupy neither live fanout count; historical rows remain subject to depth and
expansion bounds. Oversize or invalid moves persist nothing.

The new topology immediately controls `agent.list`, direct reads and subsequent
`send`/`stop`/`tree:` admissions. Tree control loses or gains the moved subtree;
self/project scope retains its existing meaning. Already-expanded tree commands
keep their concrete targets; undispatched Agent commands recheck current scope
at dispatch and cannot exercise relinquished tree authority. Dispatched or unknown
effects remain bound to their original command and are never undone or replayed.
Admission also rechecks scope inside the Store transaction to close a Router-read race.
Existing `swarm.wait` captures keep their explicit IDs and recheck current visibility,
so relinquished entries become `not_visible`. Digests keep their frozen recipient,
entry set and registration visibility mask as documented; re-parenting never
redirects an already-registered completion. Mail remains in each stable Agent's
mailbox, with its original sender/recipient-generation binding. New attention
notifications consult the current parent; already-admitted mail is not rerouted.

Links, per-member `agent.reparented` events and the bounded retained result commit
together before publication/reply. Retry the same mutation ID and identical payload:
the original result is returned before live generation/topology/fanout gates, even
after another move or stop. A different binding rejects `id_conflict`. A lost reply
is an unknown observation until that readback; there is no native effect to replay.

### Task handoff to an existing Agent (implemented)

`swarm.handoff` is one Owner-or-manager operation bound to explicit A/B generations, an explicit
original-task SEND command ID and an ordered set of at most 64 distinct undispatched
SEND IDs, also bounded by the configured selector-target limit. The sole DTOs live in
[`api/handoff.rs`](../../crates/eliot-core/src/api/handoff.rs). Empty selection transfers
context (Owner: plus claims, Goal and children) without successor task inputs; an
Agent empty selection transfers context only.
Refuse rather than implicitly wait while A has an active/unknown turn, pending
request, dispatched/unknown command, generation transition or running/unknown Goal
round reservation. B must be controlled, nonterminal, same-scope (the
authorization scope, `COALESCE(project_scope, project)`) and cleanly idle
under the existing input/unknown gates. Explicitly retry after quiescence with a new
operation ID only if the previous operation was definitely rejected before admission.
Authority never widens: the Owner hands off anywhere, while a manager may hand off
only between two workers in its own controlled scope (both endpoints pass the
frozen `control` scope check, and neither endpoint is the caller itself). An
Agent handoff transfers only task inputs the caller issued itself (Owner-issued
or other-principal SENDs refuse; each successor keeps the original issuer's
principal and rights) and never moves children, claims or Goals;
moving children stays Owner-only. `reparent` and `switch` stay Owner-only.

Only plain `send` commands in `accepted|queued` state are transferable. Never transfer
in-flight, unknown, turn/request-bound controls or queued context injections; reject
the requested set atomically if any entry has moved state. Cancel the selected
originals with a durable no-effect outcome and admit fresh B-generation SENDs in the
same transaction. The original-task SEND may also be selected when undispatched: its
text appears as labelled context, and exactly one successor carries its work. Preserve
immutable original targets/payloads and retain explicit
old-command/new-command pairs in the handoff parent's result. Use the existing
Store-only parent/ordered-child command representation for the brief and successor
inputs, not a second work queue. Successor IDs and ordinals are frozen at admission;
their native dispatch uses the ordinary router after commit. Unselected work stays
on A; stopping A is a separate explicit command.

In that transaction, an Owner handoff releases A-generation claims and recreates all
live globs/notes on B's generation with their exact remaining expiry, reporting
advisory conflicts. All live A-generation claims transfer; expired and old-generation
claims do not. An exact live destination glob collision or destination claim-cap
overflow rejects the operation rather than overwriting B's claim. Known secrets in
notes are redacted; known-secret identifiers refuse the transfer. An Agent handoff
transfers no claims: live claims stay on A.
Claims never prove ownership or silently reassign work. An Owner handoff transfers
only a quiescent Eliot Goal when B has none: clear A's Goal and install its
definition/budgets/counters on B's generation with a new revision and no
inherited idle/round key. It arrives paused, except that a
[switch](#agent-switch-implemented) keeps an active Goal active and its first round
waits for the turn of the brief. A cleared destination Goal row may supply the next revision;
another destination Goal refuses. Completed/cleared source Goals stay historical; a
blocked source Goal refuses until explicitly reset or cleared, so handoff cannot
bypass its block. Without a transferable source Goal, B keeps its own Goal unchanged.
An Agent handoff transfers no Goal: both Goals stay put. A private additive used-turn
baseline preserves consumed turns and charges each subsequent B turn immediately,
including across another handoff. Cadence re-anchors at handoff; reserved continuations
and running/unknown Goal markers on either Agent refuse, except that an unknown round
of a lost or failed source stays burned and does not block its successor. A paused
Goal needs an explicit resume; reserved/unknown continuations are never copied. Native Goals
remain `unsupported` until the relevant adapters expose them. An Owner handoff
transfers controlled children through the same re-parent transaction and its
limits. Observed native children keep their harness-owned parentage. An Agent
handoff leaves children in place. Validation failure rolls back the transfer.

The first successor input is one bounded handoff brief, admitted as ordinary `send`
to B's exact generation. It contains the original task text, which a switch finds as
described under [Agent switch](#agent-switch-implemented); a quoted history of the later send/steer/inject texts A received after
it, oldest first, each with its command principal and verb. These capture limits
and clarifications, and omit A's automatic Goal continuations. The brief also
includes Store facts about A (workspace, branch and base, Git work, Goal status and
last check exit, open claims, open or unknown commands of the handed-off generation,
and any last turn whose effects are uncertain), labelled as recorded by Swarm. It
then quotes facts the Router read from
[Git and the file system](monitoring.md#work-recorded-in-git) for this transfer:
HEAD, base, status, commits since the base, the diff from the base, the uncommitted
patch of tracked files, the latest checkpoint and the task record. New files are named
in the status and kept in the checkpoint, because the brief itself is persisted. It carries
pointers to A/generation/command/turn retained history. A vendor transcript or a
model's own report is never part of the brief. Apply existing known-secret
filtering/redaction and SEND/frame limits; label any truncated text and history gaps.
Never copy credentials, raw native context or an inferred successful result. B keeps
its own native context. An ordinary handoff to an existing Agent labels the brief as
context only. A switch tells B to continue the task from the worktree. The brief
precedes successor inputs in FIFO order.
The builder reads retained source text in the transfer transaction. Each new input receives a fresh ULID,
frozen in the parent result before commit, and shares its parent/ordinal representation.

Local admission is atomic; native application is not. The Store-only parent is `applied`
when the transfer transaction commits, meaning admission rather than task completion.
The retained parent result names each cancellation, claim/Goal/child transfer and admitted successor ID. Ordinary child
command/turn readback distinguishes pending/applied/failed/unknown delivery. Only the
brief SEND becoming `applied` releases successor dispatch; B's ordinary
turn/FIFO gates still apply. Failed, rejected, cancelled or unknown brief commands
leave successors queued. Automatic native loss/restart preserves these withheld rows,
even when B becomes terminal. Owner may assert an unknown brief applied via
`command.resolve`; the same Owner assertion is narrowly available for a failed
handoff brief. It records `resolution_source=owner`, invents no native receipt and
never resends input. The Owner can withdraw each accepted/queued handoff input
with `command.cancel`; a manager may cancel a `send` it issued itself while still
accepted/queued in its controlled scope (handoff child, ordinary SEND or own
selector SEND child), with no effect once dispatched.
Dispatched/unknown input cannot be cancelled. Explicit
generation/stop lifecycle cancellation keeps its ordinary no-effect semantics.
The parent result updates only `brief_state` and `successor_gate` as delivery settles;
its original transfer identities and component facts remain fixed. `command.children`
provides each actual child outcome. Same-ID retry returns those same identities and
current brief-gate readback, and never repeats Store transfers or external effects.
The CLI waits for the brief's delivery outcome by default. `--no-wait` returns
the committed transfer and its current brief state.

### Agent switch (implemented)

`swarm.switch` is the one operation that moves work to a new session. It serves
every [`HandoffReason`](../../crates/eliot-core/src/api/switch.rs): an Owner or manager
request, a routing choice, a provider hold, and a lost or failed session. A session is
disposable. [Git and the file system](../architecture.md#work-truth-lives-in-git-and-the-file-system)
hold the task, so B in the same worktree continues from the files as they are and no
commit is required first. Moving dirty work to a different checkout is refused until
the source checkout is clean; the new checkout would otherwise omit its uncommitted
files.

Its sole wire DTOs are
[`SwitchRequest` / `SwitchResponse`](../../crates/eliot-core/src/api/switch.rs).
Choose an explicit profile, a routing `role`, or the existing ad-hoc
`harness/model/effort/auth/cmd` selectors, including another harness. A manager may
switch a worker in its controlled scope to that worker's profile or one of the manager's
frozen `child_profiles`, by profile or role, in the worker's worktree. B takes A's place
under A's live manager. Workspace defaults to A's retained workspace;
explicit workspace/worktree/base use normal launch canonicalization and Git freeze.
The route binds B to A's retained project identity, including worktree preparation;
it does not make B a child of A or rewrite either frozen native profile.
Normal launch profile/credential checks, provider holds, capacity and workspace
reservation/dispatch still apply. No native process or context is transplanted.

A daemon-initiated or manager switch must pass
[`handoff_authority_refusal`](../../crates/eliot-core/src/routing.rs) at routing, launch
admission and transfer. B's authority must be provably equal or narrower than A's:

- Native access maps to a range on one ladder (read-only, ask, automatic, full). B's
  highest possible level must not exceed A's lowest. A value that native settings can
  still change spans every level it could resolve to. OpenCode has no frozen permission
  knob or OS sandbox, so it counts as ask through full.
- Swarm control is equal or narrower. Native subagents and MCP inheritance are not
  enabled unless A had them. `mcp_allow`, `child_profiles`, `pass_env` and `scratch` are subsets.
  B's shell receives the Agent identity only if A's did.
- Sandbox, environment selection, collaboration mode and setup commands are identical.
- The worktree is A's exact worktree.

Credentials, model and executable belong to the profile the Owner named in policy.
A held account is never replaced by another account of the same harness.

A source generation has at most one open switch. The Store records the canonical source
checkout at reservation and rechecks its current owner at reservation and launch
admission. For generated workspaces, launch admission checks the Store-finalized checkout
path after allocation, before native dispatch. The durable
`switch_checkout_owners` row stores the canonical checkout key, current Agent and any
in-flight switch; a successful handoff moves ownership to B in the H2 transaction.
`agent_successors` retains the same-checkout transfer after event retention. A same-checkout
transfer records the successor even when the caller explicitly repeats the workspace path.
A second switch from a retired owner is refused and names the current Agent to switch instead. From then on the source
refuses `send`, `steer`, `inject` and `resume` with `handed_off`, naming the Agent that
now holds the worktree, and automatic resume skips it. `stop` still works. The
transfer closes any lost or failed `owner.inbox` item for the source. A busy source is
refused with the remedy: run `swarm interrupt <agent>`, then retry.

A Codex `workspace-write` Agent in a managed worktree gets its worktree admin directory
and the repository's common `objects` directory as writable roots. Generated branches
use `<project>/<aN>/work`; the daemon adds that branch's ref and reflog parent only after
Git's full ref inventory and the on-disk directories show no sibling refs or logs. Store
admission prevents another managed worktree from sharing the `<project>/<aN>` parent,
and a successor inherits the same roots from its managed branch despite having a different
Agent ID.

Explicit and legacy branches receive their actual ref and reflog parents at any depth.
A one-component branch receives `refs/heads` and `logs/refs/heads`. These recursive
directory grants allow writes to every loose branch ref and reflog beneath each granted
parent, including siblings. Before
granting them, the daemon verifies that the selected branch is the worktree's current
HEAD branch, exists in Git's ref inventory without a case-only alias or parent/child ref
conflict, and that each granted directory tree contains only regular files and plain
directories. A reparse point or special entry blocks the grant. These roots require a
managed worktree.

The writable object root is the repository-wide common `objects` tree. A sandboxed Agent
can create, replace, remove or corrupt object data visible through that tree, including
loose objects and pack data, and can affect objects used by other refs. Eliot does not
broker object writes or provide per-Agent object integrity. The branch grants exclude
`packed-refs`, tags, refs outside `refs/heads`, repository config and hooks.

A may be idle, or lost, failed or stopped with its process tree proven gone, in any
generation. The switch first admits and executes an exact-generation source stop. Git
facts and checkpointing wait until Store proves both the complete owned process-family
exit and native source drain, including for a Lost source. No successor launch is
admitted without that proof. The task comes from the named command of any of A's generations, else A's
first retained instruction, else the [Git task record](monitoring.md). None of them being
retained does not refuse the switch: the brief says so and points at Git. An uncertain
command or turn of an ended generation cannot still land, so it is named in the brief
and never replayed. On a live idle A, the switch itself applies the source stop before
reading Git facts.

The brief does not repeat a selected SEND that the transfer admits separately. It also
omits a Git task record bound to that same SEND. Unknown or still-dispatched inputs appear
only as quoted history with their state; the successor does not receive them as new SENDs
and must verify their effects before continuing.

Before external preparation the Store checks A's exact generation and readiness,
any named task and selected SENDs, transferable Goal, and controlled child
limits for the prospective root. The parent freezes the launch and handoff IDs
after these checks pass. The worker rechecks the source checkout identity and stop
proof when admitting the ordinary promptless launch and its parent link. H2 validates the complete
transfer against current A and B.

Only an Applied launch plus the exact generation/command-bound committed
`session.ready` stream event and Idle B permits transfer. For adapters omitting the
ready event's command ID, B's unique generation-1 start must be the reserved launch;
a supplied contradictory command/cause ID refuses. Start reply and ready
batch can race; the worker waits up to 30 seconds for committed readiness after
the ordinary launch worker returns. B must have no previous SEND/turn. In one
immediate transaction a savepoint calls the same H2 handoff implementation,
revalidates A/B, links the Handoff parent as the switch's second child and records
completion. A definite refusal admits a linked ordinary stop for the exact
unused successor, including when its readiness could not be established. The
Store admits disposal only after any attempted transfer has rolled back. If B has
any SEND or turn when the stop would be admitted or dispatched, B stays running
and the receipt records the reason. SEND admission checks that disposal has not
already dispatched or become uncertain. An uncertain transfer keeps B's state
for readback.

The switch parent is Accepted at reservation and Dispatched while orchestrating.
Applied means the H2 Store transfer committed, not that the brief/task succeeded.
A lost or failed source qualifies once
[custody proves its process tree exited](monitoring.md#work-recorded-in-git). Its unknown
commands and turns stay unknown, keep their Owner inbox items and are named in the
brief. Input its end cancelled, or a hold refused at dispatch, had no native effect and
transfers to B. When the last turn may have left partial effects, the brief tells B not
to repeat its steps before checking them, and the Owner inbox gets a `handoff` item.
Swarm never replays that turn.
[Automatic handoff](configuration.md#automatic-handoff-918) reserves this same operation.
An opt-in [provider switch](configuration.md#routing-policy-s3) reserves the same
operation in the exact failed turn's transaction. It rechecks the source again
at successor launch admission and H2 transfer. The first H2 SEND carries the
original task text after a confirmed pre-work refusal. Ordinary readiness,
workspace, authority, claims, Goal and successor delivery rules still apply.
Rejected means preparation/admission refused; Failed means a known launch or
handoff refusal; Unknown retains an uncertain launch or interrupted orchestration.

After restart, Swarm releases an interrupted checkout fence once Store can prove that
the source and any successor stopped writing. Recovery cancels linked operations
that never dispatched and leaves uncertain native effects available for readback.
A fence that still needs Owner review appears in `swarm inbox` as Stuck with
`release_checkout`. After verifying that the writers exited, the Owner can run
`swarm cmd resolve <switch-id> --status failed --reason "<exit evidence>"`. The
transaction releases only that switch's fence, rejects known live writers or active
workspace operations, and records the Owner's reason.

Switch reservations outside an explicit Owner switch have a durable budget of three per
continuation lineage. This includes automatic held/lost/provider, rule and session-growth
switches and manager-routed switches. Store maps each Agent generation to its lineage,
initially the root Agent ID, and carries that mapping through completed switches and resume.
The budget and last outcome live in Store independently of retained events. Store charges
in the same transaction that admits the parent switch command; replaying the same switch,
later routing, and settlement do not charge again. A reserved or unknown outcome is not
retried, while a definite refusal or failure follows its existing cooldown. Explicit Owner
switches and resume carry the lineage without spending the budget.
The result keeps the child IDs, created Agent/generation, launch state and
handoff or disposal outcome. Refusals carry a code and a bounded reason in the
result and daemon log. A terminal failure has an empty `next_action`.
`command.children` shows the source stop, launch, handoff and any successor stop. The CLI waits
for the switch and any admitted successor stop by default. `--no-wait` returns
the current receipt.

Same-ID identical-payload retry is readback before live/config/model/auth checks;
it never schedules another worker, creates another Agent or repeats launch/transfer.
A crash before the parent worker is claimed may recover that unattempted parent.
A claimed orchestration becomes Unknown on restart with its reserved IDs intact.
Workspace recovery can read back Git preparation for its launch. Native launch
dispatch requires an active Dispatched parent.
An exact Accepted, unattempted successor stop is recoverable through ordinary
command recovery. Dispatched or unknown children retain the normal no-replay
rules. A ready B from an interrupted orchestration can receive an explicit
ordinary Owner handoff after the Owner checks its generation and the retained
transfer outcome.

Continuing a chat in a project is an ordinary switch with `launch.project` set to
the registered target: the worker prepares the target's standard worktree (or
verifies an explicit workspace against the target repository identity) through the
same preparation, admission, revision, readiness and unknown handling above, with
no new tables or state machine. The frozen preparation is bound to the target
registration while B keeps A's authorization scope, so the handoff stays in one
namespace; the H2 brief carries the source reference, and the switch completion
records `continued_from` (A) on B's existing organization link as an ordinary
overlay write (a new link revision and its `organization.changed` row), which
attach/detach/tag preserve and `chat.organization` reads back. Source history and
usage attribution never change, and a deleted source leaves the plain-text pointer
dangling without faulting the Store.

## Prompt guidance

Only routed Agents receive commands actually implemented in the current layer. Shared
vars and `who` first, specific addressed mail second; no status broadcasting, politeness
loops or credentials in coordination data. Managers receive launch/send/wait/result/
steer/stop commands. Native children without independent identity are not instructed to
call an unavailable CLI principal.
