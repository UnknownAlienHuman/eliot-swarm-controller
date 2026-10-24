# Current audit register

This is the active continuation register. AR-001–078 remain in the
[historical register](audit-register.md). AR-079–088 are frozen at
[`b7a5fe1`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b/docs/agent-operations/audit-current.md),
and AR-089–099 are frozen at
[`e42c464`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e42c464d58db770cc5490f46f391843f9fec8df2/docs/agent-operations/audit-current.md).
[Audit index](audit-index.md) maps every range to immutable evidence;
[implementation](implementation.md) owns source changes;
[agent communication](../agent-communication-program.md) owns the retained product
boundary; and [status](../implementation-status.md) owns landed behavior.

## Resume here

- Checkpoint: **2026-10-24 / mailbox, GM continuity and coordination-product deletion**.
- Source inspected at **`e42c464d58db770cc5490f46f391843f9fec8df2`**.
  The branch also contains the bounded runtime cleanup
  **`98b350e614fa660fe1d8c53b6063bdac94d80f68`**, which deletes disconnected GitHub
  PR/projection source modules identified by AR-098.
- Connected chain:
  `store/mailbox.rs` → `store/object_reads.rs::timeline(message.read)` →
  `store/gm.rs` handover/resync →
  `coordination/{thread,contract,code_scope,integration,watch,concilium}` and their
  Store owners → method policy, MCP catalog/profiles/subscriptions and CLI.
- Overall audit: **incomplete, 114 entries**. AR-100–114 are recorded below; prior
  dispositions remain in immutable checkpoints.
- Preserved in this pass: addressed sender/recipient attribution; immutable Operation
  admission; non-destructive reads; exact reply/cancel target checks; scoped
  Participant/reviewer access; GM epoch handover without rewriting historical owners or
  recipients; bounded continuation and explicit coverage gaps.
- Open in this pass: one logical message has three identities; mailbox reads scan the
  global Observation timeline and mix unrelated products; Thread, contract,
  code-scope, integration, Concilium and watch each own a separate advisory lifecycle;
  the duplicate participant registry is still required by launch and review access;
  public policy/catalog/profile/subscription/CLI surfaces encode every product.
- **Next exact chain:** Store bootstrap and migration authority:
  `store/mod.rs` embedded `migrations/001_core.sql` plus extension SQL → startup/open and
  schema verification → `doctor.rs` readback → writer/WAL reader ownership → partial
  migration, crash and corruption behavior. Prove which schema guards are useful and
  replace source-text/digest compatibility with `application_id`, numeric
  `user_version`, ordered transactional migrations and bounded postconditions.
- Runtime implementation order remains **A1b–A3, then B–V**. Source-audit order does
  not reorder implementation.
- Still unqualified: migration of concrete unresolved mailbox/coordination state,
  installed clients, live handover during native work, slow MCP subscribers, crash or
  power loss, scale/fairness and the autonomous end-to-end work cycle.
- The GitHub cleanup is production Rust deletion. It changes no external behavior and
  starts no replacement product. Minimal affected format/compile/Clippy evidence is
  tracked in PR #188; no broad or live-provider qualification is claimed here.

On resume, compare live `main` with the inspected head, reopen only changed owners and
consumers, then continue the named Store chain. A documentation change never closes a
runtime finding.

<a id="findings-ar-100114"></a>
## Findings AR-100–114

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-100"></a>AR-100 | One durable message identity | **Open.** `message.send` retains Operation ID, generated `cmsg-*` message ID and a distinct delivery ID. Legacy coordination methods add further identity forms. | Use the admitted Operation ID as message identity; reply/cancel reference it directly. Migrate concrete unresolved mail only. |
| <a id="ar-101"></a>AR-101 | Recipient mailbox storage/read | **Open ownership violation.** `message.read` is the global Observation timeline in mailbox mode. It filters JSON recipient fields and mixes direct/coordination messages with Task feedback and Check completion. | Recipient-indexed mailbox rows and a recipient cursor committed atomically with the Operation; feedback/check attention remain their own projections. |
| <a id="ar-102"></a>AR-102 | Reply/cancel correctness | **Preserve semantics, delete machinery.** Exact sender/recipient, scope, deadline, digest and prior-cancel checks are useful. Current lookup scans settled Operation JSON through typed/legacy identity unions and corruption sentinels. | Retain exact authorization and immutable reply/cancel relations on normalized rows; remove JSON expression indexes and legacy identity scans after finite migration. |
| <a id="ar-103"></a>AR-103 | GM continuity | **Preserve.** Handover changes designation/epoch only, does not rewrite Attempt owners or historical recipients, and labels watermarks as resync hints rather than acknowledgements. | Compute mailbox watermarks from recipient rows; successor reads current attention separately. Keep former recipients/authors immutable. |
| <a id="ar-104"></a>AR-104 | Thread scope | **Open product excess.** The current Thread is not a tiny grouping primitive: it owns mandatory reasonability forms, speech acts, proposal links, objections, ratification references, resolution states, supersession and multiple indexes. | Keep only bounded grouping if a current consumer still needs it; grouping grants no authority and owns no negotiation lifecycle. |
| <a id="ar-105"></a>AR-105 | Participant identity and scoped access | **Migration boundary.** The coordination participant registry duplicates base identity/directory state, but current launch registration, native streams and assigned-reviewer paths still depend on its exact Task/Attempt grants. | Move required assignment/reviewer grants to the retained application authority before deleting the duplicate directory. Never widen participants to Manager rights. |
| <a id="ar-106"></a>AR-106 | Work/contract cards and consultation | **Open duplication.** Cards, sparse relevance indexes and structured consult vocabulary implement a second discovery/directory product over facts already owned by Task/source/assignment/mailbox. | Consultation becomes one addressed message with evidence references; source and assignment projections remain authoritative. |
| <a id="ar-107"></a>AR-107 | Contract proposal/ratification | **Open duplicate authority.** Proposal revisions, counterproposals, objections, support, withdrawal, affected-scope snapshots and ratify/reject decisions form a parallel product-decision workflow. | Authoritative interface/product decisions are Task/source revisions plus connected producer/consumer verification; review and acceptance stay in their existing domains. |
| <a id="ar-108"></a>AR-108 | `code.scope.*` | **Open advisory duplicate.** Source explicitly states the records do not lock files, inspect Git, start work or release an Attempt. They nevertheless own revisions, active/task indexes, overlap classification and contract-decision dependencies. | Task path scope plus Attempt workspace lease is write admission. Remove advisory scope product and its contract coupling after current data disposition. |
| <a id="ar-109"></a>AR-109 | Integration cells and ACK | **Open duplicate agreement product.** Cells aggregate offers, requirements, carriers, comparisons, assumptions, coverage, member/card digests and ACKs while explicitly changing no Task, assignment, workspace or acceptance state. | Interface compatibility becomes source/Task revision plus exact connected checks. No generic integration ACK lifecycle remains. |
| <a id="ar-110"></a>AR-110 | Concilium | **Open separate workflow.** Concilium owns proposals, previews, blind rounds, slots, packets, positions, manager results and task/scope/viewer indexes. Current consumers are its direct Store/CLI/MCP/test surfaces, not ordinary Task review/acceptance. | Preserve independent review/research as ordinary assignments and mailbox communication; delete the council workflow, not peer collaboration. |
| <a id="ar-111"></a>AR-111 | Coordination watches | **Open duplicate scheduler.** Watches own active/list/subject/notice indexes, reconcile cursor, revisit queue/turn, quarantine and one-shot notice delivery. | Replace with current-state subscriptions or explicit automation conditions; one host loop owns due work and mailbox wake. |
| <a id="ar-112"></a>AR-112 | Public surface fan-out | **Open cut boundary.** Every coordination product is repeated in method policy, MCP catalog/profile/subscription categories, CLI parsing, Store routing, read authorization and fixtures. | Delete a product vertically: callers, schemas, policy, MCP/CLI, subscriptions, storage, backfills and fixtures in the same slice. |
| <a id="ar-113"></a>AR-113 | Finite transition | **Boundary.** Completed Operations are useful history but do not justify executable translators. Concrete unresolved messages, open threads and active grants must be enumerated before deletion. | Normalize only live required mail/thread state; finish/export/dispose other work explicitly, then remove old producers/readers/indexes together. |
| <a id="ar-114"></a>AR-114 | Disconnected GitHub PR/projection source | **Landed branch cleanup.** Search found no consumers of `github/pulls.rs`, `github/projection.rs` or the pull-origin helper. Commit `98b350e` removes two files, module declarations and the sole helper: net −250 Rust lines. | Minimal affected compile/Clippy must pass. This does not implement PR collection or remote observation; those remain explicit future work. |

<a id="trace-2026-10-24-mailbox-coordination"></a>
## Source trace at e42c464: mailbox and coordination

Inspected 2026-10-24. This is selected source evidence, not a migrated database or live
multi-agent qualification.

### AR-100–102 — the mailbox is an Operation/Observation projection, not a mailbox

`store/mailbox.rs` validates one `message.send` request, creates the immutable Operation,
then settles it with a generated `cmsg-*` `message_id` and a different `delivery_id`.
The Operation ID remains the actual admission receipt. Reply/cancel accepts several
identity shapes: typed message, typed cancellation target, Operation identity, legacy
delivery identity and legacy Operation identity. Lookup uses JSON extraction from
settled `operations.result_json`; migration 015 adds expression indexes over those JSON
fields. Additional conflict scans detect duplicate/corrupt identities.

The useful behavior is narrower than this machinery:

- the admitted request is immutable and idempotent by caller/request ID;
- sender and recipient are exact authenticated principals;
- task/attempt scope is revalidated;
- reply names one prior message and optional digest;
- cancel cannot silently cross sender/recipient/scope or cancel twice;
- reading does not delete evidence.

Retain those checks on one normalized row keyed by the message Operation ID. A
transport/display delivery identifier is not a second durable business identity.

`message.read` calls `object_reads::timeline` with `mailbox_only=true`. Candidate SQL
walks global `observations`, extracts `payload_json.recipient`, and includes
`message.send`, `coordination.message.send`, `task.feedback` and `check.completed`.
The same signed head continuation, global cut, authorization digest and bounded timeline
scan used by subscriptions is therefore also the mailbox. Feedback and Check attention
are useful, but they are not directed mail and should not determine mailbox identity or
cursor continuity.

Target row shape:

```text
message Operation ID
sender / recipient
optional Task / Attempt / tiny thread
kind
bounded inline body or artifact reference
reply target / cancellation target
recipient sequence
created/admitted time
```

`message.send` inserts the row in the Operation transaction. `message.read` keyset-pages
recipient rows. Reply/cancel records an exact relation. Store events only wake the
client; a lost notification is recovered from the mailbox table without another send.

### AR-103 — GM handover already has the right semantic boundary

`gm.rs` separates stable client role from current GM designation/epoch. Handover never
adopts another Manager's Attempt, restarts native work, cancels an admitted effect or
rewrites historical mailbox recipients. Its resync checkpoint includes a global report
watermark, previous/successor recipient watermarks, a bounded attention snapshot and
explicit `watermarks_are_acknowledgements=false` /
`historical_mail_reassigned=false` facts.

Preserve that law. Replace only the current Observation-derived mailbox watermark with
the normalized recipient sequence. The successor reads current Task/Attempt/native
attention because it is current GM; it does not become the recipient or author of old
mail.

### AR-104–106 — direct collaboration is buried inside a coordination platform

The retained Thread wire requires reasonability fields, participant reasons, topic
classification, related scopes, ten speech acts, reply deadlines, evidence references,
proposal links and manager ratification references. Store owns current/active/task/actor
and duplicate-fingerprint indexes plus resolve/withdraw/supersede state. That is a
negotiation product, not merely message grouping.

The broader coordination Store also registers a parallel Participant directory and
publishes work/contract cards. Consult locates peers through card indexes and asks one
structured question. These mechanisms grant no actual Task/workspace authority, but
they add their own freshness, index backfill, sparse search, corruption and pagination
lifecycles.

Do not delete the current registry first: launcher-created Participants, native stream
readers and assigned reviewers still use its scoped grants. First move the required
Task/Attempt/candidate access to the retained object/grant owner. Then delete duplicate
participant directory/card/index state while retaining bounded peer messages and exact
assigned-reviewer access.

### AR-107–110 — advisory state became four more products

Contract proposal/response/decision code owns immutable proposal revisions, objections,
support, withdrawals, complete affected-scope snapshots and ratify/reject decisions.
`code.scope.*` supplies those snapshots, although its own source states it neither locks
files nor controls Git/work. Integration cells independently collect contract-card
offers/requirements/carriers, comparisons, assumptions and ACKs while explicitly
changing no authoritative Task/workspace/acceptance state. Concilium adds a separate
proposal/preview/open/round/slot/position/close workflow.

These products contain useful facts, but their authority is elsewhere:

| Useful fact | Retained owner |
| --- | --- |
| Question/answer/consultation | Addressed mailbox message |
| Requested product/interface change | Task or selected source revision |
| Implementation write boundary | Task path scope + workspace lease |
| Candidate finding and verdict | Review subsystem |
| Connected producer/consumer compatibility | Check/evidence attached to exact candidate/revision |
| Final Task completion | Candidate-specific acceptance gate |
| Independent research/review | Ordinary assigned Task/review, not council rounds |

Keeping all four does not make those facts more authoritative; it multiplies copied
state, cursors, hashes, indexes and recovery paths.

### AR-111–112 — watch and frontend state multiply the same lifecycle again

Coordination watch is a full Store workflow: record/active/list/subject/notice indexes,
reconcile cursor, revisit records and turn, owner notices and quarantine. The host tick
reconciles it separately, and coordination inbox projection merges its notices with
messages. Retained subscriptions and the target single automation loop already own
wake/current-state behavior.

The deletion cannot stop in Store. Concilium and coordination categories also appear in
MCP profile requirements and subscription resync, while every method is repeated in the
public catalog, method policy, CLI parser, Store dispatch/authorization and fixtures.
A compatibility facade would leave the same product cost under different names.

### AR-113 — transition is finite, not universal compatibility

Before the cut, enumerate:

1. unresolved directed messages needed by current work;
2. open Threads whose grouping is still needed;
3. active participant/reviewer grants used by launch or current review;
4. pending watch/Concilium/contract/integration decisions requiring explicit
   finish/export/disposition.

Migrate required mail to normalized rows with original sender, recipient, scope, reply
relation and admitted time. Keep a tiny thread only for an actual grouping consumer.
Completed historical Operations remain readable through existing history. They do not
keep old mutation handlers, JSON identity indexes or an executable translation layer.

### AR-114 — first confirmed deletion from the GitHub-source audit

Repository search at `e42c464` found `github/pulls.rs::read_pull_request`,
`PullRequestObservation`, `github/projection.rs::{CheckProjection,SummaryProjection,
LabelProjection}` and `work_pool::task_origin_key_for_pull` only in their defining
files. GitHub PR effect code uses the lower-level client types directly and does not use
these helpers. Commit `98b350e` therefore removes the two disconnected modules and the
sole helper without replacing them or claiming PR-source support.

## Inspected source boundaries

| Source | Read boundary |
| --- | --- |
| `store/mailbox.rs`, migration 015 | Direct message admission, identity unions, reply/cancel lookup and JSON indexes. |
| `store/object_reads.rs` | Global timeline, mailbox candidate SQL, signed head continuation and object-grant projection. |
| `store/gm.rs` | Designation/epoch, handover, recipient watermarks and attention resync. |
| `coordination/thread.rs`, `store/coordination_threads.rs` | Thread wire, roster, message state, resolution, retained reads and contract decision seam. |
| `coordination/mod.rs`, `store/coordination.rs` | Participant registry, cards, consult, inbox and legacy index backfill. |
| `coordination/contract.rs`, `store/code_scopes.rs` | Proposal/response/ratification and advisory scope revisions/conflicts. |
| `store/integration.rs` | Integration cell contributions, comparison state and ACK lifecycle. |
| `store/concilium.rs` | Proposal, packet, round, slot, position and manager-result workflow. |
| `coordination/watch.rs`, `store/coordination_watch.rs` | Watch request shapes, indexes, notices, reconciliation, revisit and quarantine. |
| `swarm-contracts`, `swarm-mcp`, `swarm-cli`, Store router | Repeated policy/catalog/profile/subscription/CLI exposure. |
| `github/{pulls,projection,work_pool,mod}.rs` | Disconnected PR/projection code removed by `98b350e`. |

Selected paths, not whole-project certification. Concrete migration inventory, live
multi-client reconnection, damaged-database behavior and scale were not executed.

## Next work

Continue the Store bootstrap/migration chain named in [Resume here](#resume-here).
Preserve one durable authority, transactional domain receipts, WAL reads and honest
corruption handling while deleting source-text schema compatibility and unnecessary
reference-database ceremony. Do not create another migration framework or ORM.
