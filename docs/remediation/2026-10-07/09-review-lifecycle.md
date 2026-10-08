# R09. Review lifecycle: exact slot CAS, reviewer replacement и bounded listing

**Статус:** implementation handoff. В текущей ветке production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Карточки исходного аудита: AUD-026/AUD-029. Перед реализацией сравнить текущий `main`; уже исправленное не переписывать.

## 1. Результат

Один exact review slot всегда имеет один current assignment pointer. Manager может заменить reviewer без создания фиктивного `review.result` в трёх доказанных состояниях:

```text
unanswered assignment
inconclusive result
changes_requested + exact ReturnForCorrection disposition
```

Старое assignment/result остаётся immutable history. Поздний результат superseded reviewer может быть сохранён только как `historical_candidate`; он не становится current review, feedback или acceptance evidence.

`review.list` читает bounded observation page и строит expensive `review_view` только для выбранных записей. Coalesced и new assignment возвращают одну response shape.

Не создаётся новый review queue, IAM framework, cancellation RPC или native-agent kill.

## 2. Нормативные источники

- [Owner decisions §1.2–1.4](../../owner-decisions.md): manager owns exact candidate, independent review, historical late feedback never mutates newer/current work.
- [GM session continuity](../../gm-session-continuity.md): current authority отделена от historical identity.
- [R22/#48](../2026-10-08/22-requirement-evidence.md): review coverage для V2 означает exact `review_required` subset; R09 не определяет criteria.
- [R21/#47](../2026-10-08/21-correction-package.md): multi-finding correction package; R09 не рендерит или отправляет findings.

Frozen owner policy не переписывать. Historical SHA — координата source review, не pin версии продукта.

## 3. Реальный current path

Пути относительно `crates/swarm-kernel-host/src/`.

```text
review.assign
  → review::ReviewAssignRequest::parse
  → store/reviews.rs::reserve_assign
  → load_submission_context / manager_may_assign
  → current slot pointer
  → resolve_request_reviewer
  → create_assignment
  → coordination::bind_review_assignment
  → review.assignment observation + slot meta + Operation receipt

review.submit
  → ReviewSubmitRequest::parse
  → reserve_submit
  → exact assignment + historical reviewer scope
  → retained Task/submission/candidate validation
  → result_observation
  → applicability
  → review.result observation
```

Read path:

```text
review.get / swarm.review.context
  → assignment_observation
  → authorize_assignment_read
  → review_view

review.list
  → list_assignments loads entire history
  → authorization + review_view for every row
  → only then pagination
```

## 4. Audit correction: reviewer role gate is already present

Старое HIGH-обвинение «review.assign принимает любой registered client, а submit позднее требует Participant» опровергнуто полным транзакционным path.

`create_assignment` действительно сначала читает generic `client:{id}`, но success невозможен без `coordination::bind_review_assignment`, который в той же transaction требует:

- `role == participant`;
- non-disabled registration;
- `participation_basis.kind == sponsored_reviewer`;
- exact sponsor;
- exact pending review scope;
- valid pending tuple.

Failure rolls back assignment observation, slot pointer и Operation mutation. Отдельный role validator/framework не нужен.

### Упрощение реализации

Сделать `bind_review_assignment` единственным eligibility gate и вызвать его до inserts assignment observation/slot pointer. Удалить более слабую generic client existence/disabled precheck из `create_assignment`. Transaction rollback сохраняет atomicity, а определения eligibility больше не дублируются.

Не ослаблять late-result `require_historical_review_result_scope`.

## 5. Подтверждённые lifecycle gaps

### 5.1 Unanswered assignment нельзя заменить

Current replacement path требует:

```text
old result exists
AND exact review.disposition ReturnForCorrection exists
```

Reviewer, который не прислал result, навсегда удерживает current slot. Manager не может корректно заменить его без фабрикации result/disposition.

### 5.2 Inconclusive result — тупиковый terminal

`ReviewVerdict::Inconclusive` является допустимым result. Но current disposition contract поддерживает только `ReturnForCorrection` для actionable changes-requested result. Replacement также требует disposition. Следовательно, inconclusive reviewer нельзя заменить и его slot нельзя довести до pass.

### 5.3 Future supersede ошибочно сделает late result current

`current_applicability(db, identity)` проверяет Task/Attempt/submission/candidate, но не current slot pointer. После будущего supersede старый reviewer сохранит узкую historical submit authority. Если candidate всё ещё current, его late result будет помечен `current_candidate`, хотя slot уже принадлежит другому assignment.

Это необходимо исправить **в той же поставке**, иначе manager supersede создаст новый correctness bug.

### 5.4 Listing выполняет работу до limit

`list_assignments` materializes все `review.assignment` observations. `list` затем выполняет authorization и `review_view`, где читаются result, disposition, Task/Attempt/submission/candidate, для всей истории до pagination. `limit=1` не ограничивает Store work.

## 6. Одна replacement state machine

Использовать существующий `review.assign` request с полями:

```text
replaces_review_assignment_id
replacement_reason
replacement_evidence_refs
```

Не добавлять `review.supersede`, alias или compatibility method.

### 6.1 Exact current-slot CAS

В serial Store transaction:

1. построить exact `ReviewSlotIdentity` и `slot_record_key`;
2. прочитать current pointer;
3. require `current.review_assignment_id == replaces_review_assignment_id`;
4. загрузить exact assignment/result/disposition;
5. определить один replacement class;
6. проверить distinct eligible new reviewer;
7. связать new reviewer;
8. insert new immutable assignment;
9. заменить slot pointer;
10. settle assigning Operation.

Если pointer изменился — `REVIEW_REPLACEMENT_STALE`; не повторять решение на новом slot автоматически.

### 6.2 Replacement classes

```rust
enum ReviewReplacementClass {
    Unanswered,
    Inconclusive,
    ReturnedForCorrection,
}
```

Это internal derived state, не новый public enum/endpoint.

#### Unanswered

- exact old assignment exists;
- `result_observation(old_id) == None`;
- slot pointer still old ID;
- reason/evidence refs обязательны;
- no fabricated result/disposition;
- new reviewer must differ.

#### Inconclusive

- exact old result exists;
- verdict=`inconclusive`;
- result identity matches old assignment/current candidate tuple;
- no acceptance/feedback side effect;
- slot pointer still old ID;
- explicit manager reason/evidence refs;
- no ReturnForCorrection disposition required.

#### ReturnedForCorrection

- exact old result verdict=`changes_requested`;
- exact current manager disposition validates `ReturnForCorrection` for that result;
- Task remains same current Attempt/submission/candidate in `needs_correction` or the precise existing allowed state;
- new reviewer distinct.

#### Rejected replacement states

- current applicable pass: no replacement; acceptance/invalidation policy owns next action;
- changes_requested without disposition: manager decision required;
- malformed/different result identity: damaged/conflict;
- historical candidate: cannot take over current slot;
- already superseded pointer: stale CAS.

## 7. Retained replacement evidence

New assignment keeps current existing fields and exact replacement context:

```json
{
  "supersedes_review_assignment_id":"OLD",
  "replacement_context":{
    "class":"unanswered|inconclusive|returned_for_correction",
    "reason":"...",
    "evidence_refs":["..."],
    "prior_review_result_operation_id":null,
    "prior_disposition_operation_id":null
  }
}
```

Populate operation IDs only when that class has those facts. No invented null-as-proof.

Old assignment/result is never updated or deleted. Slot pointer is the sole current assignment fact.

Do not add separate mutable `assignment_state` row as a second truth. Public projection derives:

```text
current_slot = slot pointer == assignment ID
superseded   = !current_slot && some later/current assignment names this ID, when bounded lookup available
```

At minimum expose `current_slot`; successor lookup is optional and must be bounded.

## 8. Late result semantics

Change applicability helper signature:

```rust
fn current_applicability(
    db: &Connection,
    identity: &ReviewSlotIdentity,
    review_assignment_id: &str,
) -> Result<bool>;
```

It returns true only when:

- Task revision/current Attempt/submission/candidate are current;
- Attempt state permits review;
- exact slot pointer still equals this assignment ID.

A superseded reviewer may still call `review.submit` through existing historical-scope guard, but result gets:

```text
applicability = historical_candidate
```

and cannot:

- become actionable current finding;
- feed automatic acceptance;
- displace current pointer;
- trigger current correction package;
- change Task/Attempt.

Coalesced retry of the exact retained late result remains byte-equivalent except request Operation/coalesced receipt fields.

## 9. One assignment response shape

Current coalesced branch clones the internal assignment observation, overwrites `operation_id`, and returns a different shape from `create_assignment`.

Add one pure projector:

```rust
fn assignment_receipt(
    assignment: &Value,
    request_operation_id: &str,
    coalesced: bool,
) -> Result<Value>;
```

Both branches return only the same public fields:

```text
operation_id (this request)
review_assignment_id
identity
sponsor_client_id
technical_requester_id
reviewer_client_id
review_profile
state=assigned
coalesced
supersedes_review_assignment_id
replacement_context
on_behalf
```

Do not leak nested internal `result`, schema bookkeeping or stale prior Operation ID through the coalesced response.

Stored observation remains closed/full and is validated independently.

## 10. Bounded review.list

### 10.1 Cursor domain

Use immutable `observations.observation_id` as scan position for `review.assignment`. Do not use offset over mutable filtered list and do not build a second sequence table.

Request/current response should use a versioned scan cursor or explicit `after_observation_id`. Preserve existing public field only if its semantics already are observation ID; otherwise perform one explicit schema change, not reinterpret an old offset silently.

### 10.2 Bounded algorithm

```text
scan_after = decoded cursor
items = []
last_scanned = scan_after
repeat bounded SQL pages:
  SELECT observation_id,payload_json
  FROM observations
  WHERE source_stream_id='controller:review'
    AND kind='review.assignment'
    AND observation_id > ?
  ORDER BY observation_id
  LIMIT scan_budget

  for each row:
    last_scanned = observation_id
    parse/validate assignment
    authorize current caller
      expected FORBIDDEN => filtered counter, continue
      Store/corruption error => return error/gap by existing policy
    build review_view only now
    if response item/byte limit reached before adding valid row:
      do not advance past that valid row
      return cursor before it
```

Separate:

- last scanned position;
- last emitted item;
- valid-but-not-emitted boundary.

A page containing only foreign assignments still advances scan cursor with explicit partial/filtered metadata. A damaged row must not be silently called authorization filtering.

Do not calculate full count by reading all history.

### 10.3 Byte bound

Apply existing response item/byte framing. `include_context` or detailed context must be explicit; list default should not load candidate document bytes or full context for every item.

## 11. Review profile assignment

`resolve_reviewer_profile` already selects one exact pending sponsored-reviewer registration and errors on ambiguity. Preserve it.

Direct `reviewer_client_id` and profile-selected reviewer converge at the same `bind_review_assignment` authority. No parallel direct-client shortcut.

Replacement through automation remains forbidden in first slice, as current request parser/actor gate specifies. Automatic ReviewDispatch may assign an empty current slot; manager replacement is an explicit direct action.

## 12. R22 requirement evidence integration

After #48/R22:

- assignment `required_coverage.requirement_ids` may continue listing all requirements for finding references;
- assigned pass `requirement_reviews` covers only frozen `review_required` subset;
- check-only requirement needs no fabricated review rationale;
- R09 does not derive check profiles or acceptance evidence;
- review policy generation naturally changes because requirements/acceptance are in its preimage.

Do not block R09 implementation on R22; rebase the coverage predicate after both branches stabilize.

## 13. R21 CorrectionPackage integration

R09 owns assignment/result currentness and late-result applicability. #47/R21 owns:

- selection of all findings;
- package digest;
- manager feedback;
- RepairDispatch/native send.

R09 must not join finding text, choose first finding or create per-finding sends.

## 14. Files and functions

Primary:

- `src/review.rs::ReviewAssignRequest` — same public fields, no new method;
- `store/reviews.rs::{reserve_assign, resolve_request_reviewer, create_assignment, reserve_submit, current_applicability, review_view, list_assignments, list}`;
- `store/coordination.rs::bind_review_assignment` — sole reviewer eligibility/binding gate;
- MCP catalog/schema and CLI paging fields only where response/cursor contract changes.

Narrow shared consumers:

- review automation assignment expects same flat receipt;
- automatic acceptance ignores historical results;
- R21 reads current exact result only.

Do not modify candidate provenance (R05/#31), general coordination context (R06/#32), mailbox (R08/#34) or requirement evidence semantics (R22/#48).

## 15. Removal list

After migration:

- remove generic reviewer existence/disabled precheck duplicated before `bind_review_assignment`;
- remove result+disposition as the only replacement branch;
- remove full internal-record coalesced response;
- remove unbounded `list_assignments() -> Vec<all rows>` from production read path;
- remove `current_applicability` that ignores current slot ID;
- remove any temporary compatibility cursor/response union after frontend migration.

No dead helper or `#[allow(dead_code)]` as completion.

## 16. Criteria

### Eligibility/assignment

- [ ] Exact reviewer ID with wrong role/basis/sponsor/scope is rejected and no assignment/slot fact commits.
- [ ] Profile resolution ambiguity remains explicit.
- [ ] New and coalesced assignment receipts have identical field sets.
- [ ] Changed target without `replaces` conflicts.

### Replacement

- [ ] Unanswered current assignment can be superseded by distinct eligible reviewer with exact CAS/reason/evidence.
- [ ] Inconclusive current result can be superseded without fabricated ReturnForCorrection.
- [ ] ChangesRequested replacement requires exact retained disposition/result.
- [ ] Pass cannot be replaced through this path.
- [ ] Stale current pointer/candidate/Attempt rejects without altering slot.
- [ ] Replacement transaction failure leaves old pointer/registration effective.

### Late results

- [ ] Superseded old reviewer can submit one exact late result under retained authority.
- [ ] Late result is `historical_candidate` even when Task/Attempt/candidate still current.
- [ ] It cannot drive acceptance, feedback or CorrectionPackage.
- [ ] Exact retry coalesces; changed second result conflicts.

### Listing

- [ ] `limit=1` builds at most bounded scan rows and one detailed view.
- [ ] Foreign-only page advances scan cursor with explicit partial/filter evidence.
- [ ] First valid row beyond item/byte bound is not skipped.
- [ ] Store/corruption error is not converted into authorization filtering.
- [ ] Concurrent append does not duplicate/drop rows relative to observation cursor.

## 17. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Add pure assignment receipt projector and slot-current helper.
2. Refactor `create_assignment` to use only authoritative bind gate.
3. Implement replacement class derivation and current pointer CAS.
4. Fix late-result applicability.
5. Implement bounded list/cursor.
6. Update frontend schemas/docs and direct manager request examples.
7. Rebase R22/R21 consumers; remove old paths.
8. Scoped formatting and minimal Clippy.

Do not merge a response DTO or cursor helper without all production callers.

## 18. Minimal gate

After complete code:

```sh
cargo clippy --locked \
  -p swarm-kernel \
  -p swarm-kernel-host \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad tests/native execution remain the final phase. Focused behavioral tests should enter through Store/MCP review methods, not only private helpers.

PR report names:

- base/head SHA;
- assignment/replacement state table;
- changed producer/reader symbols;
- deleted unbounded/duplicate paths;
- scoped Clippy result;
- historical/late-result scenarios not yet live-qualified.

## 19. Non-goals

- killing/restarting reviewer native agent;
- new reviewer IAM service;
- second review queue/table;
- multiple current assignments per slot;
- automatic replacement by age/silence;
- automatic Task acceptance;
- multi-finding repair implementation;
- changing CheckRunner or requirement criteria;
- rewriting historical assignment/result observations;
- offset pagination over full materialized history.
