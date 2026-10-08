# R21. Correction Package v1: все выбранные findings одним exact feedback и одним native send

**Статус:** implementation handoff. Текущая поставка содержит только это задание; production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленные участки не переписывать.

## 1. Результат

Один exact committed `review.result` с `changes_requested` превращается в один versioned `CorrectionPackageV1`, содержащий все явно выбранные manager findings в исходном порядке review result. Store сохраняет одну feedback Operation, один package digest, одну `review.disposition` для exact assignment/result и одну repair delivery.

```text
review.submit { findings[] }
  -> manager disposition selects finding_ids
  -> Store copies exact findings into CorrectionPackageV1
  -> task.request_changes v2 records one package
  -> RepairDispatch sends one package
  -> native admission/readback binds package_sha256
  -> new candidate is reviewed normally
```

Никакой конкатенации произвольных строк, одного send на каждый finding и выбора «первой» находки.

## 2. Что уже есть и где искусственно теряется массив

Provider-neutral review contract уже принимает несколько findings:

- `swarm-kernel/src/reviews.rs::validate_findings_shape` проверяет массив, unique `finding_id`, requirement IDs, reason, evidence и requested change;
- `ReviewVerdict::ChangesRequested` требует хотя бы одну finding;
- retained `review.result` сохраняет весь массив;
- `review.disposition.finding_ids` уже является nonempty array.

Ограничение `ровно одна` появляется позднее:

1. `store/automation_disposition.rs::consume_selected_disposition`:
   - `0` findings → damaged;
   - `1` → продолжить;
   - `>1` → `manual_finding_selection_required`.
2. `submission::ChangeRequest` хранит один `finding_id`, один reason, один requirement/evidence set.
3. `automation::ReviewDispositionContext::semantic_request_id` ключуется одним finding ID.
4. `store/submissions.rs::request_changes_core` создаёт один `finding:*` observation.
5. `store/automation_repair.rs` и `automation/repair.rs` сохраняют одну `ReviewFinding`, один semantic slot и требуют `disposition.finding_ids == [finding_id]`.
6. Repair text renderer получает одну finding.

Это не safety invariant review system. Это несовпадение между уже batch-shaped producer и single-shaped consumers.

## 3. Связь с Cogentic — только принцип проверки

Источник: [Cogentic: Multi-Agent Orchestration for Automated Proof Discovery, arXiv:2609.40324v1](https://arxiv.org/html/2609.40324v1).

Полезный принцип: verifier возвращает полный critique кандидата, а следующий шаг получает целевую информацию для исправления; generation и verification остаются раздельными. Для ELIOT это подтверждает передачу структурированного пакета findings одному владельцу candidate.

Не переносить:

- новый research-round orchestrator;
- второй ledger;
- LLM consensus как disposition;
- автоматический выбор subset моделью;
- самооценку «исправлено» вместо нового review/CheckRunner.

Manager/owner policy остаётся authority выбора findings. Acceptance остаётся отдельной.

## 4. Не добавлять новый публичный метод

Сохранить имя `task.request_changes`, потому что оно уже описывает множественное исправление. Для новых вызовов заменить request schema одним atomically switched v2 contract; не принимать union из старой и новой форм.

Новый request:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeRequestV2 {
    pub schema_version: u16, // exact 2
    pub client_request_id: String,
    pub attempt_id: String,
    pub expected_revision: i64,
    pub submission_ref: String,
    pub candidate_ref: String,
    pub review_assignment_id: String,
    pub review_result_operation_id: String,
    pub finding_ids: Vec<String>,
}
```

Caller не передаёт reason, requested change, requirement IDs или evidence. Store берёт их из exact retained `review.result`; это исключает расхождение manager request с auditor evidence.

Rules:

- `finding_ids` nonempty и unique;
- каждый ID обязан существовать в exact current review result;
- итоговый package сохраняет порядок findings из review result, а не порядок caller array;
- разные permutations одного set дают одинаковый package digest;
- unknown/duplicate/stale ID отвергается до feedback mutation;
- empty selection не означает «все»; automation явно передаёт все IDs.

Historical settled v1 Operations остаются readable. Новый mutation parser не поддерживает v1 fallback. Generic idempotent readback прежней Operation по тому же request ID должен происходить до v2 mutation parsing; если текущий dispatcher делает иначе, добавить один exact historical receipt path, а не держать два активных handlers.

## 5. Единственный новый domain type

Добавить provider-neutral type рядом с review contracts, предпочтительно в `swarm-kernel/src/reviews.rs` или отдельном небольшом `corrections.rs`:

```rust
pub const CORRECTION_PACKAGE_SCHEMA_ID: &str = "swarm.correction_package";
pub const CORRECTION_PACKAGE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrectionPackageV1 {
    pub schema_id: String,
    pub schema_version: u16,
    pub identity: ReviewSlotIdentity,
    pub review_assignment_id: String,
    pub review_result_operation_id: String,
    pub findings: Vec<CorrectionFindingV1>,
    pub package_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrectionFindingV1 {
    pub finding_id: String,
    pub requirement_ids: Vec<String>,
    pub reason: String,
    pub evidence_refs: Vec<String>,
    pub requested_change: String,
}
```

Не добавлять отдельные package-level unions `requirement_ids/evidence_refs`: это дублирующие определения фактов. Renderer при необходимости выводит данные из ordered findings.

`package_sha256` вычисляется над canonical package body без самого digest. Должна существовать одна функция:

```rust
pub fn correction_package_digest(body: &CorrectionPackageBodyV1) -> Result<String>;
```

Не допускать несколько preimage implementations в Store/automation/MCP.

## 6. Exact package construction

Добавить provider-neutral helper:

```rust
pub fn select_correction_package(
    review_record: &Value,
    selected_finding_ids: &[String],
) -> Result<CorrectionPackageV1, ReviewValidationError>;
```

Алгоритм:

1. `validate_result_record(review_record)`;
2. require verdict `changes_requested`;
3. require applicability `current_candidate`;
4. validate selected IDs unique/nonempty;
5. iterate original `result.findings` in retained order;
6. copy exact typed fields for selected IDs;
7. require every requested ID was found;
8. construct package body from exact slot/assignment/result identity;
9. canonicalize once and compute digest;
10. validate generated package before returning.

Не принимать caller-supplied finding body. Не сортировать findings alphabetically: reviewer order is the retained presentation order. Only the selection array is order-insensitive.

## 7. Store mutation path

### 7.1 Direct and automatic calls share one core

Replace `ChangeRequest`/`request_changes_core` single-finding path with:

```rust
fn request_changes_core(
    tx: &Transaction<'_>,
    actor: FeedbackActor<'_>,
    request: &ChangeRequestV2,
    package: &CorrectionPackageV1,
    operation_id: &str,
    now: i64,
) -> Result<Value>;
```

Both direct manager and automation must:

- resolve exact review assignment/result;
- verify current Task/Attempt/submission/candidate;
- build the same package;
- use the same semantic identity;
- retain the same disposition shape;
- update Attempt `submitted -> needs_correction` at most once.

Automation cannot synthesize another reason/evidence set.

### 7.2 Semantic request identity

Replace manager/submission/single-finding preimage with:

```text
manager_id
submission_ref
candidate_ref
package_sha256
```

```rust
semantic_request_id(manager, submission, candidate, package_sha256)
```

This makes:

- permutation of selected IDs idempotent;
- content change conflict rather than coalesce;
- same exact correction package from another retained review semantically recognizable;
- one candidate/package one feedback effect.

Exact disposition instance still records its `review_assignment_id` and `review_result_operation_id`.

### 7.3 Observations

Replace one `finding:{digest}` observation with one package fact:

```text
source_stream_id = controller:review
source_event_key = correction-package:{package_sha256}
kind             = task.correction_package
```

Payload includes:

- feedback Operation ID;
- sender/recipient;
- exact Task/Attempt/submission/candidate identity;
- complete `CorrectionPackageV1`;
- applied/current/historical status;
- native_input_sent=false;
- acceptance_changed=false;
- repair_started=false.

Do not emit one observation per finding. Individual finding IDs remain queryable inside the package.

## 8. Review disposition

`review.disposition` schema v1 already supports `finding_ids: []`; do not invent v2 solely for cardinality.

Change producer:

- persist all selected package finding IDs in package order;
- add `correction_package_sha256` only if the exact-object schema is deliberately revised everywhere; otherwise reference package through `task_feedback_operation_id` and verify digest from that Operation result.

Preferred minimal form: keep disposition fields unchanged and require its `finding_ids` to exactly equal package IDs. Feedback Operation result carries package digest.

For automatic disposition on `changes_requested`, select all findings from the exact result. Remove `manual_finding_selection_required` for the ordinary path. A future policy may choose a subset explicitly, but automation must not have an LLM choose.

## 9. RepairDispatch migration

Replace single `ReviewFinding` state with one package:

```rust
pub struct RepairDispatchContext {
    // existing exact manager/Task/Attempt/binding identity
    correction: CorrectionPackageV1,
}
```

### 9.1 Slot identity

Replace `semantic_slot_id(... finding_id ...)` with package identity:

```text
manager
Task/Attempt/submission/candidate
package_sha256
```

One package produces one repair slot and one native delivery.

### 9.2 Cause/link/receipt

Retained link and receipt must include:

- exact review assignment/result IDs;
- exact `finding_ids` array;
- package SHA-256;
- package schema/version;
- current Task/Attempt/submission/candidate tuple;
- manager/transfer authority;
- native send Operation/receipt identity.

Validation compares the full package digest and selected IDs, not only the first finding.

### 9.3 Renderer

One deterministic renderer, for example:

```text
ELIOT correction package v1
Task: ... / Attempt: ... / Candidate: ...
Package: sha256:...

1. [finding-id]
Requirements: R1, R2
Reason: ...
Requested change: ...
Evidence: ...

2. ...

Apply the complete package, preserve unrelated correct behavior, and report each finding ID as addressed or still blocked. Then continue the current task.
```

Renderer rules:

- exact package order;
- LF only;
- no summarizer/model call;
- no silent truncation or finding drop;
- native input boundary checked before effect;
- if package cannot fit, settle as explicit `CORRECTION_PACKAGE_INPUT_BOUND`/attention, not per-finding partial sends.

R20/#46 may later provide a shared prompt envelope, but R21 does not depend on it: correction is `agent.send`, not initial `task.dispatch`.

## 10. Duplicate and cross-review semantics

One exact package may be identified by more than one review assignment.

Required behavior:

1. each exact assignment/result may retain its own manager disposition;
2. semantic package feedback is deduplicated by candidate + package digest;
3. if an identical package already has a settled applied feedback Operation:
   - record/confirm current exact disposition;
   - return `semantic_duplicate` with prior feedback Operation ID;
   - do not send native input again;
4. if prior feedback effect is unresolved:
   - return pending/outcome_unknown readback;
   - no replay;
5. if IDs match but finding content differs, package digest differs and request conflicts only if caller reused the same `client_request_id`.

This replaces the current single-finding `semantic_duplicate_requires_current_disposition` branch with package-level semantics.

## 11. Frontend contract

Update atomically:

- Store mutation allowlist/parser;
- MCP schema and catalog;
- CLI mapping;
- automation call builder;
- operation/readback projections;
- exact profile authorization remains unchanged.

New MCP/CLI request contains only anchors and IDs; no caller-authored finding reason/evidence.

Do not add `task.request_corrections` alias. One method, one current schema.

## 12. Files and symbols

Primary code:

- `crates/swarm-kernel/src/reviews.rs` — package types, selection, digest, validation;
- `crates/swarm-kernel-host/src/submission.rs` — replace `ChangeRequest` with v2 anchors/IDs;
- `crates/swarm-kernel-host/src/store/submissions.rs` — package feedback core and disposition retention;
- `crates/swarm-kernel-host/src/store/automation_disposition.rs` — select all findings/build one package;
- `crates/swarm-kernel-host/src/automation/disposition.rs` — package semantic request identity;
- `crates/swarm-kernel-host/src/automation/repair.rs` — package-owned context/slot/rendering;
- `crates/swarm-kernel-host/src/store/automation_repair.rs` — package lineage/readback/delivery;
- `crates/swarm-mcp/src/mcp/{mod.rs,catalog.rs}` and `crates/swarm-cli` — one v2 request shape.

Do not modify review assignment replacement/listing logic owned by R09/#35 except for a narrow shared package helper after rebase.

## 13. What to delete

After all current callers migrate:

- `submission::ChangeRequest` single-finding fields;
- `review_contract::actionable_finding` from new mutation paths (historical reader may remain private until no caller);
- `manual_finding_selection_required` ordinary branch;
- manager/submission/finding semantic request preimage;
- `finding:{digest}` feedback identity for new Operations;
- single `ReviewFinding` field in RepairDispatchContext;
- single-finding slot/link/receipt comparisons;
- renderer accepting one finding;
- duplicate reason/evidence copies supplied by manager.

A compatibility union or `if findings.len()==1 { old } else { new }` is rejected.

## 14. Criteria

### Package

- [ ] Review with 3 findings creates one package with all 3 in retained order.
- [ ] Selection `[C,A]` over result `[A,B,C]` yields package `[A,C]`.
- [ ] Selection permutations have identical package digest.
- [ ] Unknown/duplicate IDs reject before Attempt/Operation mutation.
- [ ] Caller cannot change reason/evidence/requested_change.
- [ ] Package digest changes when any exact finding field changes.

### Feedback/disposition

- [ ] One feedback Operation and one Attempt transition per package.
- [ ] Disposition `finding_ids` equals package IDs exactly.
- [ ] Same package retry coalesces byte-for-byte.
- [ ] Same package from another exact review records current disposition but creates no second native send.
- [ ] Historical candidate package remains evidence only and cannot move current Attempt.
- [ ] Current manager/transfer authority is rechecked before mutation and before send.

### Repair delivery

- [ ] One package → one repair slot → one `agent.send`.
- [ ] Admission/readback binds package digest and exact native input.
- [ ] Lost response resolves by readback, never replay.
- [ ] Input too large produces explicit gap/attention; no dropped tail findings.
- [ ] New candidate still requires independent review/CheckRunner/acceptance.

### Simplification

- [ ] No new method alias.
- [ ] No LLM summarizer/comparator added.
- [ ] No per-finding native Operations.
- [ ] No second Store/ledger.
- [ ] Old single-finding production producer removed after migration.

## 15. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Rebase after R09/#35 stabilizes review assignment/result identity.
2. Add provider-neutral package type/selection tests in the same code slice, but do not merge without Store caller.
3. Switch direct `task.request_changes` request schema and Store core.
4. Switch automation disposition producer.
5. Switch RepairDispatch context/slot/link/renderer.
6. Switch MCP/CLI exactly.
7. Remove old producer/consumer code.
8. Scoped formatting and minimal Clippy.

No broad/native tests until code slice complete.

## 16. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel \
  -p swarm-kernel-host \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

В сдаче указать base/head SHA, package schema, producer/consumer symbols, deleted single-finding paths, Clippy result и remaining native qualification.

## 17. Dependencies and non-goals

Dependencies:

- R09/#35 — review replacement/late results/bounded listing;
- R05/#31 — exact candidate/result producer identity;
- current owner-policy-v2 scoped manager guard remains authoritative.

Non-goals:

- менять reviewer assignment policy;
- автоматически принимать Task;
- объединять findings разных candidates;
- LLM выбирать subset;
- auto-loop до «исправлено»;
- per-finding parallel writers;
- новый correction chat/thread;
- скрытая поддержка двух mutation schemas;
- изменение historical review/result/feedback records.
