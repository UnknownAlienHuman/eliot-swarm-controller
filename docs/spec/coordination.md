# Coordination

Add at L5, initially between two Codex Agents. Mail, shared variables, Git-backed `who`
and advisory claims reuse the existing API/router/Store; no second scheduler.

## Authority

Principals are owner, current `agent:<id>#<generation>` and scoped automation. Missing/
revoked identity rejects. An Agent's frozen profile `control` (self/tree/project,
default self) governs its commands and reads: self reaches only itself, tree reaches
its own subtree, project reaches its project label; the owner is unrestricted. A
current Agent may queue addressed mail to any existing Agent regardless of project
label or worktree; mail is communication only and does not grant command control.
Project rules access project coordination; vars, claims and control remain as defined
below. Only owner/global automation writes
global vars. Children cannot widen parental authority. A native child without isolated
environment has no independent Eliot principal; calls from its parent environment are
attributed to that parent and this limitation is shown.

Queue mail is communication, not control. Full-text `send|steer|inject` presentation
requires ordinary command rights. No message content is executed as a router instruction.

## Mailbox

A stable Agent owns its mailbox. One row is one concrete recipient delivery:

```text
Message {
  id, delivery_id, sender, sender_generation?, recipient, recipient_generation,
  thread?, reply_to_delivery?, mode, text, content_digest,
  admitted_at, presented_at?, presentation_command?, read_at?
}
```

Direct delivery uses the write id; selector expansion captures concrete recipients and
creates `<write-id>/<recipient-agent>` rows in one bounded transaction. The selector is
not reevaluated later. One admission names 1–100 concrete recipients. Same id with
different identity/mode/body rejects; unknown recipient never creates an Agent. Text
is at most 32 KiB, known secrets rejected. Only `queue` mode is connected; `send`,
`steer` and `inject` presentation return `unsupported`.

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
per minute, with at most one open notice command: `N unread messages; run swarm inbox, then ack processed delivery IDs`.
The notice is an ordinary owner `send` bound to the recipient's current
generation and carrying only the unread count at claim time. It never contains
the body or marks it read. Unknown notice is not blindly retried;
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
A recipient with no live plugin owner for a dispatched generation stays
queue-only.

Without independent recipient identity, route only a generic notice to a live routed
parent/manager, otherwise stay queue-only. Parent notifications follow the same policy.
No routine acknowledgements/status chatter or broadcasts are generated by default.

### Read and acknowledgement

`mail.inbox` returns at most 100 unread rows within the encoded frame limit for the
authenticated stable Agent, including older generations with their original metadata. `mail.read` returns one exact row. Both
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

```sql
CREATE TABLE messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id TEXT NOT NULL UNIQUE,
  sender TEXT NOT NULL, sender_generation INTEGER,
  recipient INTEGER NOT NULL, recipient_generation INTEGER NOT NULL,
  thread TEXT, reply_to_delivery TEXT,
  mode TEXT NOT NULL, text TEXT NOT NULL, content_digest BLOB NOT NULL,
  admitted_at INTEGER NOT NULL, presented_at INTEGER,
  presentation_command TEXT, read_at INTEGER
);
CREATE INDEX messages_inbox ON messages(recipient,read_at,id);
CREATE INDEX messages_thread ON messages(thread,id);
```

## Shared variables

Scopes encode as `global` or `project:<name>`; tree scopes need hierarchy
membership and return `unsupported`. JSON values are at most 16 KiB encoded,
known secrets rejected. Each write increments version; `expect_version` is CAS,
not last-writer guess: an omitted expectation writes unconditionally, while a
supplied one must equal the current version, where an absent row reads as 0.
`set_by` records the current principal. Changes emit `var.changed`.

```sql
CREATE TABLE vars (
  scope TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
  version INTEGER NOT NULL, set_by TEXT NOT NULL, note TEXT, set_at INTEGER NOT NULL,
  PRIMARY KEY(scope,key)
);
```

## Who works on this

`who` queries existing L3 projections: current-generation native edits whose paths remain
dirty, Git dirty files in a singly owned worktree, branch differences from frozen base,
advisory claims and optionally bounded recent history. Git history is provenance, not
current ownership. Unattributed dirty bytes in a shared checkout stay unattributed.

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

Controller-launched children inherit project/root. An Agent principal or the owner
launches a child through the ordinary launch path with `args.parent`; an Agent
launches only its own children. The child takes the parent's project label and tree
root at one deeper depth; the parent link is durable. A launch parent must be a
controlled Agent: observe-only and unknown Agents cannot take children. Per
Authority above, an Agent's child cannot widen parental authority: it reuses the
parent's frozen profile snapshot with control narrowed or equal (never widened),
shares the parent's workspace, and mints no worktree; owner launches are
unrestricted. Fanout limits (`[daemon.limits]`, checked at launch admission)
prevent accidental new work but never hide an Agent row that already exists.
Above-limit children remain visible with no widened identity/control; over-limit
admission reads `fanout_limit` and persists nothing. Terminal Agents (stopped,
failed, lost) count toward neither fanout bound. Tree selectors (`tree:<agent>`,
the Agent plus its descendants in Agent-id order) expand once at admission for
`send` and `stop` only, using the Store-only parent plus ordered concrete children
representation with `<id>/<agent>` child ids; parent applied means expansion, not
children finished, and children settle independently with the same gates as targeted
commands. Any live-member failure rejects the whole expansion. `send` skips
terminal members instead of failing on them (an all-terminal send applies with no
children); `stop` still names every member. A plain `stop` affects only its target:
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

## Prompt guidance

Only routed Agents receive commands actually implemented in the current layer. Shared
vars and `who` first, specific addressed mail second; no status broadcasting, politeness
loops or credentials in coordination data. Managers receive launch/send/wait/result/
steer/stop commands. Native children without independent identity are not instructed to
call an unavailable CLI principal.
