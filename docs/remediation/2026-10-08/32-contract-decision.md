# R32. Contract decision v1: canonical proposal digest + live ratify/reject producer

**Статус:** implementation handoff. Текущий diff содержит только это задание; production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленные участки не переписывать.

## 1. Результат

Immutable contract proposal проходит один законченный путь:

```text
coordination.contract.propose
  -> exact immutable revision + canonical proposal_digest
  -> proposal-linked message/read
  -> coordination.contract.ratify | coordination.contract.reject
  -> exact durable decision record
  -> coordination.contract.get/list decision projection
  -> thread.resolve may use exact ratified decision only
```

`ratify` и `reject` являются отдельными Manager decisions. Они:

- не изменяют proposal body;
- не запускают модель;
- не принимают Task;
- не доказывают реализацию или проверки;
- не закрывают Thread автоматически;
- не создают follow-up work;
- не дают участнику новых прав.

Один exact proposal revision получает не более одного terminal manager decision. Exact replay того же `client_request_id` возвращает прежнюю receipt; новый request ID после уже принятого решения получает typed conflict и не создаёт второй decision fact.

## 2. Подтверждённые дефекты

### 2.1 Writer и reader используют несовместимые поля и форматы digest

Current proposal parser:

```rust
let digest = model::digest(canonical_proposal.as_bytes());
```

Это bare lowercase SHA-256 hex длиной 64.

Current writer сохраняет:

```json
{
  "proposal_digest": "<64 hex>",
  "proposal": { ... }
}
```

Head/index сохраняют тот же value как `latest_digest`, mutation result — как `proposal_digest`.

Но `store/coordination_threads.rs::load_proposal_revision` читает:

```rust
model::text(&proposal, "digest")?
```

и требует:

```text
length = 71
prefix = sha256:
```

Production writer никогда не создаёт такое поле/значение.

Следствия:

- `coordination.message.send` с `proposal_revision_id` всегда падает;
- `thread.resolve` с `selected_proposal_revision_id` всегда падает;
- ratification validator, даже если Operation вставить вручную, не может получить proposal digest по текущему reader contract;
- response lane (`coordination.contract.respond`) использует правильный bare digest и поэтому живёт в другом digest-диалекте, чем Thread consumer.

Это не legacy compatibility question. Writer и reader одного current aggregate расходятся.

### 2.2 Digest не пересчитывается в Thread reader

Даже если заменить имя поля и формат, current `load_proposal_revision` доверяет сохранённому digest. Он не вычисляет SHA-256 canonical `revision.proposal` и не сравнивает body.

Authoritative proposal loader обязан проверять:

```text
pointer
head
revision identity
Task/Attempt scope
canonical proposal body digest
```

Stored digest — claim, не доказательство собственного body.

### 2.3 `thread.resolve` требует producer, которого нет

Resolved contract Thread требует:

- current exact proposal revision;
- `manager_ratification_operation_id`;
- settled Operation method `coordination.contract.ratify`;
- exact Task/Attempt/thread/revision/digest result.

Но production source не содержит полного producer path:

- нет method-policy entry для ratify/reject;
- coordination parser принимает только propose/respond;
- Store apply routes only propose/respond;
- MCP tool/schema/catalog entries отсутствуют;
- no exact decision record writer;
- no decision read projection.

`coordination.contract.ratify/reject` встречаются в normative docs, continuation sources и Thread validators, но не являются callable application mutations.

Итог: contract Thread может быть proposed/responded, но не может законно получить authority fact, необходимый для `resolved`.

### 2.4 Unfinished candidate exists, but is explicitly not READY

Repository retains:

```text
docs/continuation/2026-10-06/contract-decisions-v2/
  contract_decision.rs.txt
  contract_decisions.rs.txt
  README.md
```

README states:

```text
INCOMPLETE_PRIVATE_SNAPSHOT
root decided not to expose unfinished methods
no compiler/tests/manifest/READY
```

Known candidate defect: copied `has_unsupported_glob` treats supported terminal `/**` as unsupported.

This snapshot is a donor, not implementation authority. Do not copy the files wholesale. Reconcile every symbol with current main, R30 GM authority and current code-scope semantics.

## 3. Normative contract

Source documents agree on these points:

- proposals/revisions immutable;
- support/objection remain advisory;
- ratification is separate Manager authority;
- a contract Thread cannot resolve without exact ratification;
- ratification names exact proposal digest, Task/Attempt revision and affected scope revisions;
- exact Attempt owner Manager, current GM or local Operator may decide;
- ratified, implemented, verified and accepted remain separate facts;
- Task/Attempt/scope changes make a pending decision stale;
- no majority or model consensus acquires authority.

`reject` is the symmetric durable manager decision for one exact revision. It does not erase proposal/response history and does not prevent a new superseding proposal revision.

## 4. Canonical digest contract

### 4.1 One representation

Use the representation already emitted by current production writer and consumed by `coordination.contract.respond`:

```text
bare lowercase SHA-256 hex, exactly 64 bytes
```

Do not introduce `sha256:` into proposal records. Message payload digests may retain their existing prefixed contract; they are a different domain.

### 4.2 One field vocabulary

Retained records:

```text
proposal revision: proposal_digest
proposal head:     latest_digest
proposal index:    latest_digest
mutation result:   proposal_digest
response:          proposal_digest
manager decision:  proposal_digest
```

Delete use of proposal revision field `digest` from production readers.

No fallback:

```text
revision.proposal_digest missing/invalid -> PROPOSAL_DAMAGED
revision.digest alone                    -> not accepted as current contract
both fields with conflict                -> PROPOSAL_DAMAGED
```

Current production rows already contain `proposal_digest`; no DB migration is required.

### 4.3 One pure helper

In `coordination/contract.rs`, add a small data-only representation/helper, for example:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProposalDigest(String);

impl ProposalDigest {
    pub(crate) fn parse_wire(value: &str) -> Result<Self>;
    pub(crate) fn from_body(body: &Value) -> Result<Self>;
    pub(crate) fn as_str(&self) -> &str;
}
```

Exact names may differ. Required behavior:

- wire input may preserve current acceptance of uppercase and normalize once;
- retained/current values are lowercase only;
- `from_body` hashes `model::canonical(body)`;
- proposal parser, response parser, Store writer, Thread reader and decision parser use the same helper;
- no second `valid_sha256` copy.

## 5. One authoritative proposal loader

Create one Store helper owned by the contract aggregate, not separate versions in Thread and decision modules:

```rust
pub(crate) fn load_exact_proposal_revision(
    db: &Connection,
    thread: &ThreadContext,
    proposal_id: Option<&str>,
    proposal_revision_id: &str,
) -> Result<RetainedProposalRevision>;
```

It verifies:

1. revision pointer exists;
2. pointer names exact proposal and Thread;
3. revision record exists;
4. `record_type/schema_version` exact;
5. proposal/revision/thread IDs agree;
6. Task ID/revision/Attempt agree with Thread;
7. positive revision number agrees with pointer;
8. `proposal` is an object with the current canonical contract body;
9. `proposal_digest` parses as canonical lowercase digest;
10. recomputed body digest equals `proposal_digest`;
11. when current-head semantics are requested, head/index names the same latest revision/digest.

Return a typed/internal object containing exact IDs, revision number, digest and body. Do not make downstream code reparse arbitrary `Value` fields.

Use this loader from:

- proposal-linked `coordination.message.send`;
- `coordination.contract.respond`;
- `coordination.contract.get/list`;
- `thread.resolve`;
- ratify/reject producer;
- retained decision verification.

A historical non-head revision may still be read/responded to if existing policy permits; ratify/reject and resolved Thread require exact current head.

## 6. Public decision request

Add `coordination/contract_decision.rs` or a small sibling module. Reuse the useful shape from the unfinished snapshot only after fixing it against current source.

```rust
pub(crate) enum DecisionKind {
    Ratified,
    Rejected,
}

pub(crate) struct DecisionRequest {
    pub client_request_id: String,
    pub thread_id: String,
    pub expected_state_revision: i64,
    pub proposal_id: String,
    pub proposal_revision_id: String,
    pub proposal_digest: ProposalDigest,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub affected_scope_revisions: Vec<ScopeRevisionRef>,
    pub reason: String,
    pub conditions: Vec<String>,
    pub caveats: Vec<String>,
}
```

Closed request fields exactly:

```text
client_request_id
thread_id
expected_state_revision
proposal_id
proposal_revision_id
proposal_digest
task_id
task_revision
attempt_id
affected_scope_revisions
reason
conditions
caveats
```

Rules:

- canonical request <= current coordination request byte limit;
- IDs bounded/current prefixed forms;
- revisions positive;
- reason nonempty and bounded;
- conditions/caveats bounded arrays with bounded nonempty items;
- scope refs sorted by `scope_intent_id`, unique, positive revision, canonical digest;
- ratify/reject share one parser and differ only by method-derived `DecisionKind`;
- request cannot supply decision actor, authority basis, timestamp, observation or Operation ID.

Do not create a generic `coordination.contract.decide` alias. The documented methods remain the two explicit mutations.

## 7. Authority and currentness

At commit time, decision producer verifies:

1. Thread exists, topic `contract`, state `open`;
2. `expected_state_revision` exact;
3. request Task/revision/Attempt equal retained Thread;
4. Task current/open at exact revision;
5. Attempt belongs to Task/revision and is unreleased;
6. proposal revision is exact current head and body digest recomputes;
7. no decision already exists for this exact revision;
8. affected code-scope snapshot is complete and exact;
9. caller authority current.

Allowed caller:

```text
local Operator
OR exact Attempt owner with Role::Manager
OR current GM validated through R30/#56
```

A generic registered Manager is not sufficient. Do not use raw `meta["gm"].client_id`; consume R30's typed epoch/registration authority.

Decision record stores authority basis:

```text
local_operator
attempt_owner
current_gm + exact gm_epoch
```

Binding/session is not authority.

## 8. Affected scope revisions

### 8.1 One current snapshot

Use `code_scopes::current_scope_revisions` for the exact Task/revision/Attempt. Require:

```text
coverage == complete
```

The caller supplies every current accepted scope relevant to proposal `affected.paths/symbols/schemas`, exactly once, sorted. The Store derives the expected set and compares full values.

### 8.2 Do not copy the unfinished overlap implementation

The unfinished candidate duplicates path-overlap logic and contains a confirmed bug: terminal `/**` is classified as unsupported.

Reuse one reviewed `code_scopes` relation function. Preferred narrow change:

```rust
pub(crate) fn path_overlap(left: &str, right: &str) -> (Option<bool>, &'static str);
```

or a purpose-specific `affected_scope_revisions` helper owned by `code_scopes.rs`.

Required semantics:

- exact path;
- broad `**` / `**/*`;
- terminal `prefix/**` overlap/disjointness;
- unsupported glob -> unknown, never false-clean;
- symbols and schemas/interfaces exact;
- unknown relation with a current accepted scope -> `SCOPE_COVERAGE_INCOMPLETE`.

Do not introduce a second glob engine.

### 8.3 Expired-but-active is not absence

Current `current_scope_revisions` silently skips retained `state=active` records whose advisory expiry passed, while still capable of returning `coverage=complete`.

For a manager decision, expiry does not prove writer release. Narrowly correct the projection or return a gap:

```text
active record expired but not explicitly released
-> coverage=partial
-> gap=active_scope_expired_owner_unknown
```

This aligns the decision lane with the existing audit rule that TTL is not proof of work termination. Do not auto-release or block forever; require an explicit scope disposition/current owner decision.

This code overlaps R06/#32. Rebase after its context work; one manager owns `code_scopes.rs`.

## 9. Decision persistence

Store one immutable decision record per exact proposal revision in existing `meta`:

```text
coordination:contract-decision:<proposal-id>:<proposal-revision-id>
```

Record includes:

```text
schema/version + record_type
ratified | rejected
exact decision Operation ID
Thread + state revision
proposal/revision/digest
Task/revision/Attempt
Attempt owner
actor + role + authority basis + optional GM epoch
exact affected scope revisions
scope coverage=complete
reason/conditions/caveats
created_at_ms
model_work_started=false
native_execution=false
```

No separate decision-list index. Proposal get/list already provide bounded enumeration; attach the exact decision to those projections.

Observation contains identifiers only:

```text
source_stream_id = coordination:proposal:<proposal_id>
source_event_key = decision:<proposal_revision_id>
kind = coordination.contract_ratified | coordination.contract_rejected
payload = thread/proposal/revision/decision_operation IDs only
```

Full rationale/conditions/caveats remain behind Thread authorization.

### Existing decision

Under a new `client_request_id`, any existing ratified/rejected record for the revision returns a typed conflict, for example:

```text
CONTRACT_DECISION_ALREADY_RECORDED
```

Include existing decision kind/Operation ID only if current caller can read the exact Thread. Do not return the old Operation ID as the new Operation's success result.

Exact same `client_request_id` remains generic immutable replay and creates no second Operation.

## 10. Application/Store integration

Add both methods atomically to:

- `swarm-contracts::method_policy` as Manager-facing mutations, no Participant mutation grant;
- `model::validate_mutation` through the exact decision parser;
- Store `apply` routing;
- pre-savepoint `admitted_thread_operation_scope` stamping;
- Operation read authorization for contract operations;
- MCP tool schemas and catalog metadata;
- configured profile exposure/live authorization;
- generic CLI call remains available; add dedicated CLI subcommand only if the current coordination family already requires it consistently.

No method is advertised before its Store producer and readback are present in the same code slice.

### Pre-savepoint scope

The generic mutation wrapper must stamp decision Operations with exact Task/Attempt before handler execution, using authenticated retained Thread scope. Rejected decisions therefore remain scoped receipts without trusting request-supplied Task IDs.

Add ratify/reject to the exact thread-backed method set. The handler still revalidates all currentness inside the savepoint.

## 11. Reads

### `coordination.contract.get`

For the requested revision, return:

```json
{
  "decision": null | { full authorized decision record },
  "decision_coverage": "complete_for_revision"
}
```

### `coordination.contract.list`

For each returned proposal head, include compact latest-decision metadata only:

```text
decision kind
proposal_revision_id
decision_operation_id
created_at_ms
```

Respect the existing item/byte page bound. Do not run an unbounded secondary history scan or add a second decision cursor.

### Operation read

`authorize_contract_operation_read` must recognize propose/respond/ratify/reject.

- caller can read its own rejection receipt without acquiring Thread scope;
- settled decision contents for another reader require exact retained Thread authorization;
- full decision rationale is projected through contract read, not leaked through unrelated operation/report visibility.

## 12. Thread resolution

Replace current ratification validation with the shared decision loader.

For `outcome=resolved` and contract topic:

1. selected proposal revision required;
2. exact current proposal head required;
3. manager ratification Operation ID required;
4. decision record exists and `decision=ratified`;
5. decision Operation ID/Thread/Task/Attempt/proposal/digest exact;
6. affected scope snapshot retained in decision remains the one that was validated at decision time;
7. current Thread state revision/CAS still exact.

A rejected decision can never satisfy this guard.

Ratification does not itself close the Thread. `thread.resolve` remains the structural state transition and can record remaining objections/follow-ups separately.

If proposal is superseded after ratification but before resolve, old ratification is historical and resolution returns stale conflict.

## 13. Rejection semantics

`coordination.contract.reject`:

- records a terminal decision for that exact proposal revision;
- leaves Thread open;
- leaves proposal/response history immutable;
- cannot be converted to ratified under another request ID;
- permits a new proposal revision that explicitly supersedes the rejected revision;
- does not schedule a model turn or correction;
- may be followed by explicit `thread.resolve{outcome:"unresolved"}` under ordinary Thread authority.

Do not treat lack of ratification, silence, timeout or objections count as implicit rejection.

## 14. What may be reused from the unfinished snapshot

Useful source material:

- strict request field set/bounds;
- one decision per exact revision;
- identifier-only observation;
- full decision attached to proposal reads;
- exact Task/Attempt/proposal/affected-scope checks;
- no automatic model/native/Task effects.

Must be replaced/rebased:

- raw GM reader -> R30 typed authority;
- duplicate proposal loader -> shared exact loader;
- duplicate digest validation -> `ProposalDigest` helper;
- duplicate glob evaluator -> current `code_scopes` helper;
- unsupported `/**` bug;
- any stale line/file assumptions;
- incomplete integration glue.

Do not apply the `.txt` files as patches.

After production implementation is landed and qualified, remove the two source-like `.txt` snapshots or replace the continuation directory with one short historical pointer to the landed commit. Git history already preserves unfinished bytes; leaving parallel source copies invites future Frankenstein integration.

## 15. Tests

### Digest round trip

- [ ] Proposal writer stores one lowercase 64-byte `proposal_digest`.
- [ ] `contract.respond` accepts exact current digest.
- [ ] Proposal-linked `coordination.message.send` succeeds.
- [ ] `thread.resolve` loader reads the same field/format.
- [ ] Uppercase wire claim normalizes; retained output remains lowercase.
- [ ] Missing/invalid/conflicting legacy `digest` cannot act as fallback.
- [ ] Mutating proposal body while retaining digest yields `PROPOSAL_DAMAGED`.
- [ ] Mutating head/index/pointer identity yields explicit corruption/stale error.

### Producer availability

- [ ] Both methods appear in method policy, MCP schema/catalog and Store router only after handler exists.
- [ ] Participant cannot ratify/reject.
- [ ] Attempt owner Manager, current GM and local Operator positive cases pass.
- [ ] Unrelated Manager rejects.
- [ ] R30 malformed GM designation grants nothing.

### Decision currentness

- [ ] Exact current proposal can be ratified/rejected.
- [ ] Stale Thread revision rejects.
- [ ] Task revision or Attempt release rejects.
- [ ] Superseded proposal revision rejects.
- [ ] Changed relevant scope revision rejects.
- [ ] Missing/partial/unknown/expired-active scope coverage rejects.
- [ ] Unrelated scope change does not invalidate when relation is determinately disjoint.
- [ ] `prefix/**` disjoint/overlap cases are determinate and do not hit unsupported-glob fallback.

### Idempotency and immutability

- [ ] Lost response + same request ID returns exact original receipt.
- [ ] New request ID after decision returns typed conflict and creates no second decision/observation.
- [ ] Reject then ratify same revision is impossible.
- [ ] New superseding revision can receive a new decision.
- [ ] Rationale/conditions/caveats are immutable and bounded.

### Resolve/read

- [ ] Ratified exact current revision allows explicit resolved transition.
- [ ] Rejected decision cannot resolve contract Thread.
- [ ] Missing decision returns RATIFICATION_REQUIRED.
- [ ] Superseded ratification returns stale conflict.
- [ ] Contract get/list expose bounded authorized decision metadata.
- [ ] Unrelated participant/observer cannot read decision rationale.
- [ ] No Task/Attempt/Acceptance/native/model state changes on decision.

## 16. Files and symbols

Primary:

- `crates/swarm-kernel-host/src/coordination/contract.rs`
  - canonical proposal digest type/helper;
- `crates/swarm-kernel-host/src/coordination/contract_decision.rs`
  - strict decision request;
- `crates/swarm-kernel-host/src/store/coordination.rs`
  - exact proposal loader/writer/read integration;
- `crates/swarm-kernel-host/src/store/contract_decisions.rs`
  - decision persistence/verification;
- `crates/swarm-kernel-host/src/store/coordination_threads.rs`
  - proposal-linked send and resolve guard;
- `crates/swarm-kernel-host/src/store/code_scopes.rs`
  - shared affected-scope relation and expired-active coverage;
- `crates/swarm-kernel-host/src/store/mod.rs`
  - routing/pre-savepoint scope/read authorization;
- `crates/swarm-contracts/src/method_policy.rs`;
- `crates/swarm-kernel-host/src/model.rs`;
- `crates/swarm-mcp/src/mcp/{mod.rs,catalog.rs,profiles.rs}`.

Update exact communication docs/status after code, not before advertising methods as implemented.

## 17. What to delete

After migration:

- `coordination_threads::load_proposal_revision` wrong `digest`/`sha256:` implementation;
- duplicate proposal revision loaders;
- duplicate SHA validators for proposal decisions;
- duplicate path/glob evaluator from unfinished candidate;
- dead validator expecting a producer unavailable from application API;
- source-like continuation `.txt` files after landed history is linked;
- any MCP/catalog entry without matching live Store handler;
- comments/status claiming contract decision is complete before public path exists.

Do not delete immutable proposal/response history.

## 18. Dependencies and ownership

- R30/#56: typed current GM/epoch authority. Implement/rebase first.
- R06/#32: coordination context/code-scope changes. One manager owns `code_scopes.rs` and `coordination.rs` during rebase.
- R23/#49 and R24/#50: Operation/MCP fail-closed reads; decision Operations consume their shared relation/live authorization.
- R14/#40: future method/schema source-of-truth; R32 supplies one complete vertical method pair now and should expose typed request metadata for later extraction.
- R07/#33 Concilium remains advisory and does not ratify contracts.

One manager/worktree. Do not run parallel writers in `store/mod.rs`, `coordination.rs`, `coordination_threads.rs` or MCP registry files.

## 19. Implementation order

1. Add shared canonical proposal digest helper.
2. Replace current proposal writer/response/thread readers with one exact loader.
3. Add strict decision request type/parser.
4. Add typed GM/Attempt authority via R30.
5. Add affected-scope derivation using shared code-scope relation; fix expired-active coverage.
6. Add one decision record/observation writer and verifier.
7. Integrate Store routing and pre-savepoint scope.
8. Integrate contract get/list and Operation read.
9. Replace Thread resolve validator with exact decision record.
10. Add method policy + MCP/catalog only when producer/readback connected.
11. Remove duplicate/broken readers and unfinished source copies after qualification.
12. Scoped formatting and minimal Clippy.

## 20. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-contracts \
  -p swarm-kernel-host \
  -p swarm-mcp \
  --lib --bins -- -D warnings
```

Then exact public-path Store/MCP tests named in the implementation report. Broad/native qualification later.

## 21. Non-goals

- majority/LLM consensus as authority;
- automatic Thread resolution;
- Task acceptance/publication;
- model/native call;
- new decision list/index/table/event store;
- rewrite proposal history;
- compatibility alias `contract.decide`;
- global glob engine;
- hard code-scope lock;
- infer rejection from silence;
- preserve unfinished continuation source as a second implementation;
- expose methods before full producer/readback exists.
