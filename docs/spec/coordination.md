# Coordination

## Registered artifacts and cleanup (K69)

The shared wire DTOs are in
[`api/cleanup.rs`](../../crates/eliot-core/src/api/cleanup.rs).
`artifact.register {id,path,kind,purpose}` admits an existing directory owned by
the authenticated Agent generation. It captures the opened filesystem identity,
project and generation-bound ancestor chain. Before native startup Swarm creates
`<first-configured-artifact-root>/<agent>-<generation>/`, or
`<data-dir>/artifacts/<agent>-<generation>/` when none is configured. Creation is
exclusive: an existing unbound directory is refused, never adopted. Its opened
identity is committed as an internal, non-deletable `ownership_root` ledger row,
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
Only `registered` rows are candidates: settled rows (`removed`, `unknown`,
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
Clean managed worktrees require exact reciprocal Git/common-directory identity;
tracked, untracked and ignored changes keep the worktree. Native stores are never
deleted as files. A Codex session of a terminal Agent is `safe` with reason
`native_archive_pending`; every other harness stays `keep(no_native_archive)`.
The plan snapshot anchors the freshly classified branch tip (the backfill also refreshes the stored row), and apply re-verifies that exact tip. A branch is `safe` only after a fresh upstream fetch proves the tip is
contained in the upstream default branch with no unpushed commits (a remote
branch already deleted upstream counts as merged only together with that
containment); otherwise it stays `keep` with `unmerged`, `ahead`, or a fetch
readiness reason. Default-branch discovery must freshly advertise the remote
HEAD symref; a cached remote HEAD never substitutes for a failed discovery
(`keep(no_default_branch)`). The plan fetch updates remote-tracking state but never
deletes branches. A present `daemon.lock` with a live owner keeps a data
directory; only a provably dead owner releases it.

`cleanup.apply {id,plan_id,artifacts?}` consumes only the plan's safe candidates, rechecks
authority, generation, Git cleanliness, opened identities and busy/link refusal,
then records each outcome as `removed`, `skipped` with reason, or `unknown`. An
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
is retained when the worktree is removed through worktree cleanup. A planned
branch is deleted locally with the safe `-d` form after a second fresh-fetch
verification at apply time, and only after its worktree is gone; refuse reasons
are `branch_checked_out`, `branch_delete_refused` and `identity_changed`, and
a branch that vanished concurrently is `unknown`. Remote branch deletion is a
separate explicit Owner-only flag (`delete_remote`) on `cleanup.apply` and
never automatic: the remote branch is verified absent before and after the
push, and a remote failure after local deletion is `unknown`, never `skipped`.
Native sessions are archived at apply through the harness documented archive
method (Codex `thread/archive`) using the Agent's frozen profile and store
binding. Artifact scope/reference must match that generation's native subject,
and the initialized reader verifies its effective home/SQLite root against the
frozen binding before dispatch. A legacy Codex home scope derived from an
alias missing from the frozen binding remains ineligible for Swarm archive.
Only a new launch can capture a verified binding for its own session. A
malformed or secret-bearing acknowledgement after dispatch is `unknown` and
retains the durable retry fence. Raw
native store files are never touched. Logical reclaimed bytes count only fully
confirmed removed trees; partial attempts report unknown and zero confirmed
bytes.

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

The current limits are 64 candidates, 4,096 retained artifacts, 1,024 retained
plans, 10,000 filesystem entries, depth 64 and five seconds per tree scan.
Exceeding a bound refuses the tree before deletion. A full plan ledger self-heals
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
has no Owner per-Agent scope, so there is no per-task cleanup entry. A folder assignment opts in with
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
concrete recipients. Same id with different identity/mode/body rejects; unknown
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

With `wake_on_mail=true` (read from the recipient's frozen profile snapshot,
default true), a routed recipient may receive one generic coalesced notice
per minute, with at most one open notice command. The wake text is one
neutral line for every harness: `N unread: mail.inbox, then mail.ack`
(`N overlapping work claims: who` for claim notices). An unread reply whose
`reply_to_delivery` names mail the recipient itself sent, admitted after the
last presentation, skips the one-minute cooldown once, so an answer to an
Agent's own question wakes it at once; the open-notice, busy, pending-input
and unknown gates still apply.
The notice is an ordinary owner `send` bound to the recipient's current
generation and carrying only the unread count at claim time; ordinary mail
notices never contain the body or mark it read. An unread completion digest
registered by the recipient Agent itself at its current generation
(`agent:<recipient>:<generation>`) is the explicit exception (the registrant
is already the recipient and holds command/read scope over the digest): when
it fits within the wake budget (`MAX_EVENT_BYTES`, 64 KiB), the wake `send`
shows the digest body directly to the recipient's own turn (appending
`N unread: mail.inbox, then mail.ack` when uninlined unread mail remains) and
binds `presentation_command` so `turn.started` of that wake stamps both
`presented_at` and `read_at` on the inlined digest delivery, while ordinary
command readback (`command.get`/`command.children`) redacts `args.text` so no
other principal can read excerpt text through the wake command. If it does not
fit, or the registrant differs (for
example Owner or an earlier generation), the wake falls back to the generic
count notice and leaves `read_at` unset until `mail.ack`. Unknown notice is not blindly retried;
provider hold, pending request, current unknown gate and drain suppress new wake.
A busy, waiting or input-pending mail recipient suppresses a new mail wake:
a current-generation `send` already accepted, queued, dispatched or unknown
but not yet `turn.started` still reads idle in that gap, so the wake is
deferred (skipped, not queued) rather than dispatched after the inbox was
already read and acked. Claim-conflict notices keep their prior busy policy
and still queue; only mail notices defer, ordinary user sends still queue
FIFO. The claimed `send` carries an explicit structured `notice_kind`
(`mail` or `claim` only); a send naming the open notice with a missing,
wrongly typed or other value is invalid, with no inferred default, while
normal sends never inspect it. Mail revalidates the busy, pending and
unknown gates at admission via its open-notice identity; the Store never
parses notice text. Internal `notice_kind` remains Store admission metadata
and is stripped at daemon dispatch, so vendor plugins receive only text arguments.
Deferred mail re-offers on idle through the same
atomic claim; no cooldown is spent while deferred. A claim preserves the
prior `last_notice_at` (0 until first presentation), a failed admission
clears only the open claim, restart recovery clears only commandless
open claims, command retention clears only the open reference of the
exact completed notice it deletes, and the shared cooldown
runs from actual presentation (`turn.started` stamps `last_notice_at`).
A daemon driver re-pages due recipients every 2 seconds: current `idle`,
non-observer agents holding unread mail past the normal cooldown are
re-offered through the same atomic claim, so an agent that idles with no
new event still wakes once due. Normal queued mail and older-generation
unread rows can therefore surface as a fresh current-generation generic
count notice; the notice never replays message or job bodies.
A recipient with no live plugin owner for a dispatched generation stays
queue-only.

Without independent recipient identity, route only a generic notice to a live routed
parent/manager, otherwise stay queue-only. Parent notifications follow the same policy.
No routine acknowledgements/status chatter or broadcasts are generated by default.

Automatic Agent-attention reports queue a mail pointer for the affected Agent
through ordinary `mail.send` with the existing wake policy. Its parent manager, when
present, receives a normal queued mail through the existing wake policy; without a
parent, the owner receives the ordinary local notify event and bus report. These notices
include a short bounded diagnosis from the structured report and point to the
`x.user.agent.attention` event for details. They contain no native transcript and never
replay the Agent's prompt or job.

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
harnesses. Same-harness children retain the model/effort-only comparison and inherit
parent credential bindings verbatim. Every frozen account or proxy capacity scope of the parent
is kept with the same bounds, alongside the child's own scopes; conflicting bounds
reject. Control is narrowed or equal (never widened). It omits `profile_snapshot` (the daemon
freezes it) and shares the parent's workspace (an omitted workspace defaults to
the parent's; a different workspace rejects, so a child never escapes the
parent's authority) and mints no worktree; owner launches are unrestricted. Fanout limits (`[daemon.limits]`, checked at launch and re-parent admission)
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
- **Cancel** removes the intent and lifts its drain fence once any staged
  replacement has settled without open custody. Owner can stop a staged B
  through ordinary recovery.

Saved context is available to the Owner in the retained result. Task brief
delivery and selected SEND continuation use their own [Handoff](#handoff)
contract.

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
definition/budgets/counters paused on B's generation with a new revision and no
inherited idle/round key. A cleared destination Goal row may supply the next revision;
another destination Goal refuses. Completed/cleared source Goals stay historical; a
blocked source Goal refuses until explicitly reset or cleared, so handoff cannot
bypass its block. Without a transferable source Goal, B keeps its own Goal unchanged.
An Agent handoff transfers no Goal: both Goals stay put. A private additive used-turn
baseline preserves consumed turns and charges each subsequent B turn immediately,
including across another handoff. Cadence re-anchors at handoff; reserved continuations
and running/unknown Goal markers on either Agent refuse. Explicit
resume is required; reserved/unknown continuations are never copied. Native Goals
remain `unsupported` until the relevant adapters expose them. An Owner handoff
transfers children through the same re-parent validation/transaction helper and its
limits, not new ownership records; an Agent handoff never moves children. Any
validation failure rolls back all these Store changes.

The first successor input is one bounded handoff brief, admitted as ordinary `send`
to B's exact generation. It contains the original task text from the named retained
command; the later send/steer/inject texts A received after it, oldest first, each with
its principal and verb, because those carry the limits and clarifications a summary
loses (A's Goal continuations are excluded: they are Swarm-generated, not instructions); Store facts about A (workspace and its Git work, Goal status and last check
exit, open or unknown commands of the handed-off generation), labelled as recorded by
Swarm; A's latest retained final output (exact turn binding, or an explicit absence),
labelled as model output Swarm did not verify; and pointers to A/generation/command/turn
retained history. Apply existing known-secret
filtering/redaction and SEND/frame limits; label any truncated text and history gaps.
Never copy credentials, raw native context or an inferred successful result. B keeps
its own native context. The brief is labelled as context only and precedes successor
inputs in FIFO order.
The builder reads retained source text in the transfer transaction and labels missing
or pruned final output without inferring success. Each new input receives a fresh ULID,
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
No compensating automatic stop/relaunch is implied.

### Agent switch (implemented)

`swarm.switch` is one Owner launch-plus-handoff operation. Its sole wire DTOs are
[`SwitchRequest` / `SwitchResponse`](../../crates/eliot-core/src/api/switch.rs).
Choose an explicit profile or the existing ad-hoc `harness/model/effort/auth/cmd`
selectors, including another harness. Workspace defaults to A's retained workspace;
explicit workspace/worktree/base use normal launch canonicalization and Git freeze.
The route binds B to A's retained project identity, including worktree preparation;
it does not make B a child of A or rewrite either frozen native profile.
Normal launch profile/credential checks, provider holds, capacity and workspace
reservation/dispatch still apply. No native process or context is transplanted.

Before any external preparation, an immediate Store transaction checks A's exact
generation and quiescence, retains the switch parent and freezes its launch and
handoff child IDs. A busy source refuses before any B allocation. The parent is
claimed once, then an independent worker prepares and admits the ordinary launch
and its parent/ordinal link in one transaction. No launch text is allowed: launch
only creates B's session. The original task and selected SENDs use H2's grammar and
bounds; their current eligibility is revalidated by H2, not frozen as ownership.

Only an Applied launch plus the exact generation/command-bound committed
`session.ready` stream event and Idle B permits transfer. For adapters omitting the
ready event's command ID, B's unique generation-1 start must be the reserved launch;
a supplied contradictory command/cause ID refuses. Start reply and ready
batch can race; the worker waits up to 30 seconds for committed readiness after
the ordinary launch worker returns. B must have no previous SEND/turn. In one
immediate transaction a savepoint calls the same H2 handoff implementation,
revalidates A/B, links the Handoff parent as the switch's second child and records
completion. A refusal rolls back every attempted H2 change before recording failure.
B remains explicit in the result; stopping A or B is a separate Owner command.

The switch parent is Accepted at reservation and Dispatched while orchestrating.
Applied means the H2 Store transfer committed, not that the brief/task succeeded.
Rejected means preparation/admission refused; Failed means a known launch or
handoff refusal; Unknown retains an uncertain launch or interrupted orchestration.
The result keeps both child IDs, created Agent/generation, current launch state,
Handoff result when admitted, refusal code and `next_action`. `command.children`
shows the ordinary launch and, when admitted, the Handoff parent; the latter owns
its brief/successor children and their H2 delivery gate.

Same-ID identical-payload retry is readback before live/config/model/auth checks;
it never schedules another worker, creates another Agent or repeats launch/transfer.
A crash before the parent worker is claimed may recover that unattempted parent.
A claimed orchestration becomes Unknown on restart with its reserved IDs intact;
it is not automatically resumed. Its exact Accepted, unattempted launch child is
recoverable through ordinary launch/workspace recovery; no new child ID is minted.
A subsequently ready B still requires ordinary Owner handoff. Dispatched/Unknown
launch children retain the normal no-replay rules; uncertainty never becomes a retry
signal. Owner resolution alone is not session readiness. Interrupted transport proves no
failure. No automatic stop, relaunch or compensation occurs.

A definitely launched B without a committed handoff is finished by ordinary
`swarm.handoff` to that exact existing Agent/generation after quiescence. The result
names this `next_action`; use a new handoff mutation ID. There is no special finish
mutation, and retrying the switch ID cannot perform this recovery.

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
