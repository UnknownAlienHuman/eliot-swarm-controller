# ELIOT Agent Communication — Peer Autonomy Implementation Amendment

**Revision:** 1 — 2026-10-03  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Normative basis:** [Peer Autonomy and Integration Handshake](agent-communication-peer-autonomy.md)  
**Amends:** [Implementation Issue Plan](agent-communication-implementation-issues.md), [Implementation Checklist](agent-communication-implementation-checklist.md)  
**Status:** source-exact implementation amendment. This file has precedence for I2–I4, participant credentials and peer-local agreements.

## 0. Purpose

The earlier implementation plan made communication safe but still left too many ordinary questions dependent on manager-created threads and manager-ratified contracts.

This amendment implements the intended operating model:

```text
manager assigns the work once
        │
        ▼
scoped participant credentials
        │
        ▼
agent reads current work/contract cards
        │
        ├── exact fact available ─────────────► continue
        ├── quick ask to exact owner ─────────► continue
        ├── integration handshake ────────────► peer-local agreement
        └── authority/global conflict ────────► manager digest
```

Routine coordination does not pass through Root, General Manager or auditor.

## 1. One intentional role addition

The previous checklist said not to add a generic worker role. That was correct for a role with normal mutation authority, but insufficient for direct peer communication: native workers cannot safely use manager credentials, observer is read-only and module credentials are correctly restricted to runtime reporting.

Add exactly one narrow role:

```rust
Role::Participant
```

It is an **assignment-bound coordination capability**, not a general writer role.

### 1.1 Participant can call only

```text
swarm.context.get
swarm.tools.search
coordination.send
coordination.inbox
coordination.consult
coordination.sync_integration
coordination.watch.create/list/cancel
swarm.overlap.check
review.get / review.submit only for an atomically bound exact review assignment
operation.get only for its own coordination Operations or that assignment's linked review Operation
```

The exact final allowlist is fixed in code and profile documentation. Unknown methods fail before generic writer routing.

### 1.2 Participant cannot call

```text
task.create/revise/claim/dispatch/submit/accept/request_changes/invalidate
attempt.bind_producer/release
agent.*
check.*
source.capture
artifact.assemble
host.mode
gm.handover
client.register/disable
module.*
contract ratification/rejection
scope acceptance/override/release of another owner
Concilium preview/open/advance/close
forge/module/service/admin methods
```

A participant's natural-language message cannot invoke any forbidden method.

### 1.3 Store routing

Current Store logic special-cases `Role::Module`, then routes reads or calls `require_writer()`. Do not let `Role::Participant` fall through that generic writer path.

Add a dedicated branch immediately after `current_principal`:

```text
if role == participant:
    validate current participant registration and work context
    if method in participant read allowlist: read
    else if method in participant mutation allowlist: mutate
    else: FORBIDDEN
```

`Principal::require_writer()` must not by itself grant participant authority.

## 2. Scoped participant registration

## 2.1 New manager operation

```text
coordination.participant.register
```

Authorized caller:

- exact current Attempt owner;
- local operator;
- current GM under existing authority.

It is not the general `client.register`, which currently requires GM authority and permits durable operator/manager/observer/module roles.

### Request

```json
{
  "client_request_id": "manager-known-id",
  "client_id": "opaque-uuid-or-local-alias",
  "token_hash": "sha256-hex",
  "task_id": "...",
  "task_revision": 4,
  "attempt_id": "...",
  "participation_basis": {
    "kind": "attempt_owner | producer_ref | sponsored_reviewer",
    "assignment_id": "optional-existing-producer-ref",
    "review_scope": null
  },
  "binding_id": "optional",
  "binding_generation": 3,
  "native_session_id": "optional",
  "display_alias": "producer-runtime",
  "inbound_policy": "pull_only | safe_boundary | hold | refuse"
}
```

The caller generates/retains the secret and supplies only its hash, matching the current client registration pattern. The secret is passed to the local MCP/IPC client configuration, not inserted into prompts, tool results, artifacts or diagnostics.

### Stored registration

Use existing `meta` client registration storage:

```json
{
  "role": "participant",
  "token_hash": "...",
  "disabled": false,
  "task_id": "...",
  "task_revision": 4,
  "attempt_id": "...",
  "participation_basis": {},
  "binding_id": null,
  "binding_generation": null,
  "native_session_id": null,
  "display_alias": "producer-runtime",
  "inbound_policy": "pull_only",
  "grant_revision": 1,
  "created_by": "manager-a",
  "created_operation_id": "..."
}
```

No schema migration is required for this current projection.

### Validation

- exact Task/Attempt exists and Task revision matches;
- Attempt is current and not released;
- caller owns Attempt or has operator/current-GM authority;
- `attempt_owner` identifies that owner;
- `producer_ref` names an already bound exact Assignment/ProducerRef;
- optional binding/generation/session agrees with the ProducerRef/current binding;
- reviewer scope is explicit and read/coordination-limited;
- client ID does not already exist;
- token hash is valid;
- no implicit credential rotation;
- no registration of a participant for a different manager's Attempt.

### Result

```json
{
  "operation_id": "...",
  "client_id": "...",
  "role": "participant",
  "task_id": "...",
  "task_revision": 4,
  "attempt_id": "...",
  "participation_basis": {},
  "grant_revision": 1,
  "inbound_policy": "pull_only"
}
```

## 2.2 Disable and lifecycle

New operation:

```text
coordination.participant.disable
```

The same manager/operator/current-GM authority may disable an exact participant registration. It does not erase mail, cards, agreements or audit history.

Every participant call also revalidates:

- registration enabled;
- exact Task revision;
- Attempt current/not released;
- participation basis still valid;
- optional binding generation/current native identity still matches.

After Attempt release/supersession, generic participant writes return `STALE_PARTICIPANT`; revocation also denies writes. The narrow assigned-review exception is exact and does not revive the Participant identity generally: an authenticated reviewer Participant may read `review.get`, read the linked `operation.get`, and submit `review.submit` only for its retained assignment, submission and candidate after Task revision or Attempt release. It gets no historical context/list, artifact or evidence reads. These exact historical reads/submission remain subject to credential validity and explicit revocation; generic stale writes remain denied.

Attempt release may mark ordinary participant registrations inactive in the same transaction or make them fail by revalidation. A registered assigned reviewer can retain only the exact review scope needed to finish/read its existing assignment. It must not silently map an old credential to a new Attempt.

If registration is prepared before a review assignment exists, `review_scope.review_assignment_id: null` is pending, not authority. The server must atomically bind the scope to the exact assignment before `review.get`, linked `operation.get`, or `review.submit` can succeed. Recheck the authenticated Participant, exact assignment/submission/candidate and revocation state on every call.

## 3. Native-agent access without manager relay

When a manager assigns a native subagent:

1. existing Task/Attempt/ProducerRef assignment is created through current authority;
2. manager registers one scoped Participant credential;
3. runtime launch/route injects a local ELIOT MCP profile with the opaque credential reference;
4. the agent receives coordination tools, not the credential value;
5. participant calls ELIOT directly;
6. every call is limited to its exact assignment context;
7. manager need not relay ordinary questions or answers.

The runtime adapter remains lifecycle owner. Participant registration does not give ELIOT a second session owner and does not permit the peer to control another native session.

For runtimes that cannot consume a local MCP/IPC tool surface yet, the manager may proxy coordination messages, but that is a compatibility fallback—not the target architecture.

## 4. Material current cards before messages

## 4.1 Bootstrap work card

`coordination.context.get` returns a controller-generated bootstrap card from Task/Attempt/ProducerRef facts even before the participant publishes anything.

The participant then publishes only material additions:

```text
provides
requires
planned exact scopes
assumptions
known contract gaps
integration points
```

It does not need to retype Task identity, owner, binding or canonical source references.

## 4.2 Card update coalescing

`work_card.publish` and `contract_card.publish` compute a `material_digest` excluding:

```text
liveness
percentage complete
tool activity
free-form status chatter
updated_at alone
```

Same material digest returns the retained/current revision and creates no new manager attention or peer message.

## 4.3 Field reads

A participant can request exact card fields:

```text
coordination.contract_card.get(contract_key, fields=[...])
```

Before sending a quick ask, `coordination.consult` checks whether the requested structured field is present in the current card. If yes, result is:

```json
{
  "status": "answered_from_card",
  "contract_key": "...",
  "card_revision": 3,
  "card_digest": "sha256:...",
  "answer": {},
  "delivery_created": false
}
```

This is the fastest/cheapest common path.

## 5. Peer graph and directory

Do not expose one global agent list as the normal UX.

Maintain current indexes from Task/Attempt/ProducerRef, cards and accepted scopes:

```text
Task/Attempt -> active participants
contract_key -> producer/consumer/carrier participants
path/symbol/interface -> participants
participant -> provides/requires
participant -> pending asks/agreement obligations
```

`coordination.peer.find` returns only matched current participants and the exact matching reason.

### Owner resolution states

```text
exact_owner
multiple_candidates
unowned
stale_owner
unknown_coverage
```

Only `exact_owner` permits the convenience method to send. Ambiguity never becomes broadcast.

## 6. Quick ask is not a thread by default

## 6.1 Convenience operation

```text
coordination.consult
```

Transactionally:

1. validate caller participant and Task/Attempt;
2. check current card field/query;
3. resolve exact owner from current index;
4. coalesce an identical unresolved ask fingerprint;
5. if still needed, create one addressed mailbox delivery;
6. return ask/delivery identity.

No separate manager action.

### Ask fingerprint

```text
task revision
attempt
sender participation basis
target relation (contract/path/symbol)
question kind
canonical question/fact query
expected answer kind
evidence set digest
```

Same unresolved fingerprint returns the existing ask. Changed evidence/question revision creates a new ask linked to the old one.

## 6.2 Answer operation

```text
coordination.answer
```

Statuses:

```text
answered
unknown
redirect
needs_negotiation
superseded
```

No mandatory acknowledgement/thanks response. Answering settles the ask and emits one small freshness fact.

### Redirect

Redirect identifies one exact current candidate and reason. It does not automatically forward through chains. The caller chooses whether to resend.

### Unknown

`unknown` is a valid answer and must be preferred over plausible invention.

## 6.3 Recipient availability

The ask is durable even when recipient native process is unavailable.

- `pull_only`: recipient reads later;
- `safe_boundary`: one header may be exposed at a real compatible boundary of an already active session;
- `hold`: not exposed to model until released;
- `refuse`: admission returns explicit refusal.

Neither `pull_only` nor `safe_boundary` starts an idle agent turn. A separate manager `followup_task`/runtime input is required to start more work.

## 7. Integration handshake before generic negotiation

## 7.1 Offer and requirement are current projections

Producer publishes `integration.offer`; consumer publishes `integration.requirement`. Revisions are material/digest-based and linked to their contract cards.

## 7.2 Pure compatibility comparator

Implement in `src/coordination.rs` with no LLM.

Compare exact dimensions:

```text
contract/version
producer/consumer/carrier/caller
input/output and serialization
ID equalities and distinctions
ownership/lifetime
inline bytes vs readable reference
availability/read boundary
clock/epoch/binding generation
result/disposition owner
rejected/failed/unknown semantics
retry/reconciliation
capability/feature gates
registration/production call site
limits/unsupported cases
```

Every dimension returns:

```text
match
mismatch
unknown
not_applicable
```

No aggregate `compatible` when a required dimension is unknown.

## 7.3 Autonomy classifier

Pure deterministic function:

```rust
fn classify_agreement_autonomy(
    task: &TaskSnapshot,
    participants: &[ParticipationBasis],
    offer: &IntegrationOffer,
    requirement: &IntegrationRequirement,
    comparison: &CompatibilityComparison,
    scopes: &[ScopeIntent],
) -> AutonomyDecision;
```

Result:

```json
{
  "class": "peer_local | manager_required",
  "reasons": [],
  "canonical_sources": [],
  "unrepresented_owners": [],
  "unknown_dimensions": []
}
```

The complete allow/deny conditions are in the Peer Autonomy normative document. Code uses explicit reason enums, not keyword matching over prose.

## 7.4 Peer-local acknowledgement

All directly affected current participants acknowledge the same:

```text
offer revision/digest
requirement revision/digest
comparison digest
autonomy decision digest
Task/Attempt revisions
```

Then one current projection becomes `peer_agreed`.

No manager inbox item is created for a compatible peer-local agreement.

## 7.5 Manager-required path

When `manager_required`:

- peer acknowledgements are stored as positions;
- agreement state is `pending_manager`;
- one manager digest item is created/coalesced by fingerprint;
- manager may ratify, reject, request change or open Concilium;
- no peer assumes approval from silence.

## 8. Scope coordination without manager micromanagement

A participant may publish planned scope in its work card without manager approval because it is descriptive.

`code.scope.propose` remains the operation for an advisory accepted scope intent.

Fast path:

- proposed paths/symbols/interfaces are inside the Task snapshot's allowed scope;
- no active exclusive overlap;
- no other owner is displaced;
- no broad/glob expansion;
- no privileged/shared manager-controlled scope.

The system may classify it `self_consistent`, making it visible immediately as a proposed/current intent without waiting for the manager.

Manager action is required for:

- exclusive overlap;
- expansion beyond Task scope;
- override/release of another owner's scope;
- broad central file/schema authority;
- missing/ambiguous owner;
- scope conflict unresolved by sequencing or contract agreement.

Peers may agree on sequencing/integration without changing ownership. Touching another assignment's exclusive file still requires manager scope revision.

## 9. Manager exception digest

`coordination.manager.digest` is a projection, not a stream of all activity.

Include one current item per fingerprint:

```text
unowned blocking contract/scope
manager_required agreement
unresolved mismatch
affected owner missing/stale
exclusive scope override
repeated no-progress loop
security/persistence/identity/lifecycle/external-effect decision
Concilium proposal
```

Do not include:

```text
answered asks
card reads
compatible peer-local handshakes
ordinary messages
status/liveness
safe-boundary delivery receipts
```

Manager can drill into exact supporting records on demand.

## 10. Auditor packet

Add one read projection:

```text
coordination.review_packet.get
```

Input: exact Task/Attempt/submission/candidate and optional agreement/contract key.

Output:

```text
Task/Attempt revisions and canonical sources
participants and scopes
work/contract card revisions used
ask/answer facts that affected the seam
peer agreement or manager ratification
compatibility comparison and unknown dimensions
remaining assumptions/dissent
Git overlap/coverage
evidence refs
implementation status
```

The auditor does not approve routine peer-local agreements. It verifies that the implementation and agreement comply with canonical requirements and can refute them during protected review.

## 11. Revised implementation Issues

This section replaces I2–I4 from the older Issue Plan. I1 and later scope/Git/Concilium/CLI/qualification slices remain, with the dependencies below.

```text
I1 mailbox extraction
  -> I2 participant role + scoped registration + directory/current cards
      -> I3 quick ask/answer + inbound policy
          -> I4 integration handshake + peer-local autonomy
              -> I5 generic bilateral thread + disputed/global contract path
                  -> I6 advisory scopes
                      -> I7 bounded Git inspection
                  -> I8 durable Concilium
                      -> I9 CLI/MCP/profile UX
                          -> I10 integrated verification/live qualification
```

### I2 — participant role, directory and current cards

Files:

```text
src/model.rs
src/coordination.rs
src/store/coordination.rs
src/store/mod.rs
src/store/tasks.rs or narrow participant validation helper
src/store/producers.rs read helpers only
src/store/projection.rs
src/doctor.rs
```

Deliver:

- `Role::Participant`;
- dedicated Store allowlist branch;
- register/disable Operations;
- Task/Attempt/ProducerRef revalidation;
- `context.get`, `peer.find`;
- work/contract cards;
- current indexes/meta projections;
- manager digest read projection;
- no mailbox sends yet.

Non-goals:

- no participant Task/runtime/check authority;
- no model wake;
- no general agent list;
- no new table.

### I3 — quick ask/answer

Files:

```text
src/coordination.rs
src/store/mailbox.rs
src/store/coordination.rs
src/store/mod.rs
src/store/projection.rs
```

Deliver:

- card-field fast answer;
- exact owner resolution;
- `coordination.consult` for card-backed answers or one exact-owner ask;
- ask coalescing;
- one-recipient mailbox delivery;
- inbound policy facts;
- one-shot availability freshness;
- no full thread requirement.

Non-goals:

- no auto forwarding/broadcast;
- no idle model start;
- no task assignment.

### I4 — integration handshake and autonomy

Deliver:

- offer/requirement revisions;
- deterministic comparison;
- explicit unknown dimensions;
- autonomy classifier;
- multi-party exact digest acknowledgement;
- peer-local agreement;
- pending-manager path and coalesced digest item;
- review packet projection.

Non-goals:

- no canonical doc mutation;
- no manager approval on peer-local fast path;
- no code/model generation;
- no vote/consensus authority.

### I5 — generic thread and disputed/global contract path

Only after quick asks and handshakes exist:

- full typed bilateral thread;
- immutable contract proposal/counterproposal/object/support;
- manager ratification for manager-required contracts;
- no ordinary-seam manager gate.

### I6/I7/I8/I9/I10

- I6: advisory scopes;
- I7: Git inspection;
- I8: Concilium;
- I9: CLI/MCP profiles and five convenience tools first;
- I10: integrated tests/load/live negotiation and token comparison.

## 12. Agent-facing MCP profile

A Participant profile exposes primarily:

```text
coordination_context
coordination_consult
coordination_answer
coordination_publish_work_card
coordination_publish_contract
coordination_check_integration
coordination_peer_agree
coordination_inbox
coordination_thread
code_scope_inspect
code_scope_conflicts
git_who_works_here
```

Low-level methods are hidden unless a diagnostic/manager profile needs them.

The model should not have to manually orchestrate ten RPC calls for the common path.

## 13. Notifications and context budget

Deliver only a compact header automatically at a safe boundary:

```json
{
  "kind": "coordination_ask | coordination_answer | agreement_changed | manager_decision",
  "from": "display-alias",
  "subject": "Operation ID equality",
  "record_id": "...",
  "requires_reply": true,
  "deadline_ms": null
}
```

No body, prior transcript, global status or other peer messages are injected automatically.

The agent explicitly calls a read tool when relevant.

## 14. Recovery

- participant credential remains scoped after host restart;
- exact current Attempt/ProducerRef is revalidated on each call;
- unresolved ask remains durable;
- answered ask does not redeliver as new work;
- same material card publish is idempotent;
- peer agreement survives restart with exact revisions/digests;
- stale Task/Attempt/binding makes it historical and blocks new writes;
- safe-boundary notification loss is recovered by inbox/current-state read, not repeated model wake;
- no automatic follow-up task after restart.

## 15. Scale contour

Qualification target, not current claim:

```text
10,000 registered/inactive participant records
1,000 active participants
5,000 current work/contract cards
10,000 quick asks/hour
2,000 concurrent integration handshakes
200 concurrent readers
100 manager-required exception fingerprints
```

Measure:

```text
context.get/peer.find/ask admission p50/p95/p99
DB transaction duration
card-update coalescing ratio
asks answered from cards
asks requiring peer model work
peer-local vs manager-required ratio
manager digest size and age
messages/model turns per resolved seam
RSS/handles/queue depth
native reply/control latency during load
```

## 16. Directed acceptance additions

1. Participant cannot call `task.revise`, `agent.send` or `check.run` even though those are normal writer mutations.
2. Participant registration by another Attempt owner is forbidden.
3. Released Attempt participant cannot publish or send.
4. Same participant name/old credential is not reused for a new Attempt.
5. Card field answers an ask without a delivery.
6. Ambiguous owner sends nothing.
7. Unknown owner creates one blocking digest item only when marked blocking.
8. Identical unresolved ask coalesces.
9. Answer `unknown` is retained and does not trigger another ask automatically.
10. Recipient `hold` stores but does not expose to model.
11. Recipient `refuse` returns explicit rejection.
12. Safe-boundary notification contains header only.
13. Quick ask changes no Task/Assignment and starts no turn.
14. Compatible handshake with all peer-local conditions commits without manager.
15. Public API/persistent schema/identity change is manager-required.
16. Missing required comparison dimension prevents compatible result.
17. Unrepresented affected owner prevents peer-local agreement.
18. Manager sees no ordinary compatible handshake in digest.
19. Ten duplicate mismatches create one current exception item.
20. Auditor reads one review packet instead of full thread history.
21. Participant scope proposal inside current Task and without overlap is immediately visible; it does not expand authority.
22. Exclusive cross-owner path still requires manager scope decision.
23. Host restart creates no duplicate ask, notification or model turn.
24. 10,000 inactive participants create no per-client Tokio task.

## 17. Final implementation constraint

The communication plane fails its purpose if a normal agent must ask the manager:

```text
who owns this contract?
what exact type are they returning?
has their version changed?
where do I connect my consumer?
does our identity/failure model match?
```

Those questions must be answerable by current cards, one exact peer ask or a deterministic handshake.

The manager is involved when the answer changes authority, canonical project behavior or another assignment—not because the agents need a switchboard.
