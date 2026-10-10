# R22. Executable Requirement Evidence: Requirement → exact profile → CheckRun → acceptance

**Статус:** implementation handoff. В этой ветке пока изменена только документация; product-код, Task records и acceptance semantics ещё не изменены.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённый разрыв

Текущий `Requirement` содержит только:

```rust
pub struct Requirement {
    pub id: String,
    pub statement: String,
}
```

`AcceptancePolicy` отдельно перечисляет глобальные `required_check_profiles`. `task.accept` требует текстовый `RequirementReview { rationale, evidence }` для **каждого** requirement и независимо проверяет, что все глобальные check profiles прошли.

Поэтому Store может доказать:

```text
все requirements получили текстовое объяснение
И
все required profiles прошли
```

но не может доказать:

```text
какой CheckRun проверяет какой requirement
```

Пример `config/task-checked.example.json` описывает W1 как функциональное требование и A1 как прохождение Clippy, но `clippy-lib-bin` не связан с A1 в сохранённом контракте. Acceptance всё равно требует human rationale/evidence и для A1.

Это функциональный разрыв, а не дефект process runner. CheckRunner уже закрепляет exact candidate, Attempt, profile ID/revision, parser, argv/input identity, process receipt и parser coverage.

## 2. Результат

Новая Task specification явно задаёт для каждого requirement, какие независимые доказательства обязательны:

```text
review evidence
machine CheckRun profile(s)
или оба вида
```

Один CheckRun может удовлетворять несколько requirements, если каждый из них ссылается на тот же exact trusted profile revision. Acceptance сохраняет детерминированный per-requirement evidence manifest и больше не требует текстового review для check-only requirement.

```text
Task requirement
  → frozen verification policy
  → exact ReviewResult evidence (если требуется)
  → exact CheckRun profile revision (если требуется)
  → per-requirement evidence manifest
  → explicit GM/Operator acceptance decision
```

Machine pass не принимает Task автоматически. Review и acceptance остаются разными ролями; CheckRunner не вызывает модель и не решает, выполнено ли требование вне закреплённого критерия.

## 3. Что берём из Cogentic и чего не добавляем

Источник: [Cogentic: Multi-Agent Orchestration for Automated Proof Discovery, arXiv:2609.40324v1](https://arxiv.org/html/2609.40324v1).

Полезная граница:

- prover/generator не является verifier;
- verifier возвращает адресный critique;
- подтверждённые факты отделены от журнала попыток;
- итоговое утверждение проходит отдельную final audit;
- агент получает targeted briefing, а не внутренний журнал оркестратора.

В ELIOT этому уже соответствуют:

- candidate/result — предложение исполнителя;
- independent review — семантическая проверка;
- CheckRun — машинное доказательство exact candidate;
- acceptance — отдельное авторитетное решение.

R22 соединяет эти существующие факты с конкретными requirements. Не переносить из статьи:

- второй verified ledger;
- model-driven progress/completion judgement;
- автоматические proof rounds;
- consensus голосов;
- новый orchestrator или planner;
- LLM-generated criteria.

Критерий создаёт владелец Task в спецификации до исполнения.

## 4. Не расширять CheckRunner в первом срезе

Current CheckRunner уже даёт две пригодные формы доказательства:

1. `cargo_json`: exact trusted profile revision, build/check/clippy command, required Cargo target identities, `build_finished`, zero errors, no gaps.
2. `exit_code`: exact trusted profile revision and exact configured command, exit 0, complete process/source evidence.

Для одного конкретного теста владелец создаёт trusted `exit_code` profile с exact argv (`cargo test ... --exact`, другой native test executable и т.п.). Requirement ссылается на profile ID/revision. Writer не передаёт argv и не выбирает parser.

**Не добавлять в R22:**

- произвольную команду в Task;
- parser regex по stdout;
- новый test framework;
- experimental nextest run JSON как acceptance authority;
- dependency на nextest metadata;
- широкую систему assertion DSL.

Официальная nextest документация считает JUnit основным machine-readable run output, а JSON run output experimental. Если позднее потребуется доказательство отдельных test cases внутри одного широкого запуска, это отдельный parser slice с exact retained report, а не часть R22.

## 5. Новая текущая форма Task acceptance

### 5.1 Requirement verification

Расширить host DTO:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequirementVerificationV1 {
    pub review_required: bool,
    #[serde(default)]
    pub check_profiles: Vec<CheckProfileRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirement {
    pub id: String,
    pub statement: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<RequirementVerificationV1>,
}
```

`CheckProfileRef` остаётся один тип; не создавать отдельные Task/acceptance/MCP копии.

Rules для текущей формы:

- `review_required == true` **или** `check_profiles` nonempty;
- profile ID/revision nonempty и bounded по уже действующим правилам;
- profile identity unique внутри requirement;
- один profile ID не может фигурировать с разными revisions в одной Task;
- один exact profile/revision может обслуживать несколько requirements;
- порядок requirements и profile refs сохраняется в snapshot/brief; equality/digest не зависит от map iteration.

### 5.2 Versioned acceptance policy

Новая write-form:

```json
{
  "schema_id": "swarm.acceptance_policy",
  "schema_version": 2
}
```

V2 не содержит глобальный `required_check_profiles`: их exact union выводится из frozen requirement verification. Это удаляет второе определение обязательных checks.

Исторический v1 (`{"required_check_profiles":[...]}`) остаётся **только retained reader**. Реализовать явные типы:

```rust
enum RetainedAcceptancePolicy {
    LegacyV1(LegacyAcceptancePolicyV1),
    V2(AcceptancePolicyV2),
}
```

Не использовать untagged «угадай форму» во всех consumers. Один custom decoder:

- нет `schema_id/schema_version` → exact legacy object;
- exact schema/version 2 → exact V2 object;
- mixed fields, unknown version или partial marker → reject.

После активации:

- `task.create` и `task.revise` с acceptance принимают только V2;
- новая V2 Task требует verification у каждого requirement;
- V2 Task без acceptance запрещает verification fields (они не должны молча игнорироваться);
- существующий unreleased V1 Attempt может завершиться по frozen V1 semantics;
- новый claim для unclaimed V1 Task возвращает `TASK_ACCEPTANCE_POLICY_UPGRADE_REQUIRED`; владелец делает явный `task.revise`;
- historical Task/Attempt/acceptance readback остаётся byte-faithful.

Это versioned evolution, не fallback нового пути на старую семантику.

## 6. Пример V2

```json
{
  "objective": "Implement the assigned change and provide exact source evidence.",
  "phase": "implementation",
  "requirements": [
    {
      "id": "W1",
      "statement": "The code implements the assigned behavior.",
      "verification": {
        "review_required": true,
        "check_profiles": []
      }
    },
    {
      "id": "A1",
      "statement": "The exact candidate passes the controller clippy profile.",
      "verification": {
        "review_required": false,
        "check_profiles": [
          {"profile_id": "clippy-lib-bin", "profile_revision": "1"}
        ]
      }
    },
    {
      "id": "T1",
      "statement": "The regression scenario is both machine-checked and independently reviewed.",
      "verification": {
        "review_required": true,
        "check_profiles": [
          {"profile_id": "regression-exact-test", "profile_revision": "4"}
        ]
      }
    }
  ],
  "acceptance": {
    "schema_id": "swarm.acceptance_policy",
    "schema_version": 2
  },
  "dependencies": [],
  "owner_policy_id": "owner-policy-v2",
  "source_refs": []
}
```

Corresponding `task.accept` request:

```json
{
  "reviews": [
    {"requirement_id":"W1","rationale":"...","evidence":["..."]},
    {"requirement_id":"T1","rationale":"...","evidence":["..."]}
  ],
  "check_ids": ["CHECK_FOR_CLIPPY", "CHECK_FOR_REGRESSION"]
}
```

A1 does not need invented textual evidence. T1 needs both.

## 7. Provider-neutral validation

Keep closed DTOs in host boundary and deterministic Value predicates in `swarm-kernel`, following current repository layering. Do not add SQLite/process dependencies to `swarm-kernel`.

Add one provider-neutral derivation:

```rust
pub struct RequirementEvidencePlan {
    pub review_requirement_ids: Vec<String>,
    pub required_profiles: Vec<CheckProfileRef>,
    pub profile_to_requirements: BTreeMap<CheckProfileRef, Vec<String>>,
}

pub fn requirement_evidence_plan(spec: &Value) -> Result<RequirementEvidencePlan>;
```

If avoiding serde types in `swarm-kernel`, return bounded JSON plus typed host decode; do **not** implement the same traversal in Store and MCP separately.

The derivation verifies:

- exact Task requirement IDs;
- V2 policy marker;
- verification presence/shape;
- at least one evidence source per requirement;
- profile identity consistency;
- deterministic requirement/profile order.

This one function is used by:

- Task admission validation;
- Task brief projection;
- acceptance coverage validation;
- acceptance evidence manifest construction;
- frontend schema examples/tests.

## 8. Task create/revise/claim changes

### 8.1 Split retained shape from new admission

Current `store/tasks.rs::spec(v)` deserializes and calls one `TaskSpec::validate()` for create, revise and retained reads. Introduce explicit boundaries:

```rust
TaskSpec::validate_retained()
TaskSpec::validate_current_admission()
```

- retained accepts valid legacy V1 or current V2;
- current admission permits acceptance=None without verification, or exact V2 with complete verification;
- no new v1 writes.

Do not scatter `if legacy` across Store consumers.

### 8.2 Claim gate

Before freezing a new Attempt:

- current V2 Task → normal claim;
- Task with no acceptance → normal claim, but it still cannot be accepted;
- legacy V1 Task and no existing unreleased Attempt → explicit upgrade-required;
- already-frozen v1 Attempt → unaffected.

No automatic revision or migration under claim.

### 8.3 Frozen snapshot

Attempt snapshot stores exact V2 spec and a derived compact `requirement_evidence_plan` or its digest. Prefer storing the exact plan because it is small and removes repeated traversal. If stored, verify it against spec at every authority boundary; never trust an independently caller-supplied plan.

R20/#46 Task Prompt uses the same frozen plan to render human-readable criteria. It does not invent or summarize criteria.

## 9. Acceptance request semantics

### 9.1 Reviews

Replace current “reviews exactly equal all requirement IDs” with:

```text
review IDs exactly equal review_required requirement IDs
```

- no review entry for check-only requirement;
- missing review for review-required requirement → `REVIEW_INCOMPLETE`;
- extra review → reject;
- duplicate review ID remains invalid;
- rationale/evidence requirements remain unchanged for review-required entries.

### 9.2 Checks

Resolve every supplied CheckRun exactly as today: current Attempt/candidate, required exact profile revision, passed state, complete parser coverage, operation receipt, process/resource evidence, cache provenance and artifact bytes.

Then build:

```text
(profile_id, profile_revision) → check_id
```

Rules:

- every profile in derived requirement plan has exactly one accepted CheckRun;
- extra profile/check not required by any criterion is rejected;
- duplicate check IDs rejected;
- one CheckRun may appear in several requirements via the profile mapping;
- same profile ID with another revision is mismatch, not substitute;
- profile count equality alone is not sufficient.

Existing `validate_check` should return the full profile identity, not only `profile_id`. `validate_checks_complete` compares exact required profile identities as sets/maps, not two counts.

### 9.3 Evidence manifest

Construct and retain:

```json
{
  "requirement_evidence": [
    {
      "requirement_id": "W1",
      "review": {"rationale":"...","evidence":["..."]},
      "checks": []
    },
    {
      "requirement_id": "A1",
      "review": null,
      "checks": [
        {"profile_id":"clippy-lib-bin","profile_revision":"1","check_id":"..."}
      ]
    }
  ]
}
```

Ordering follows frozen Task requirements and profile refs. Store this in the acceptance Operation effective evidence and applied result/readback. Include an evidence digest if the current acceptance result already maintains a canonical evidence identity; do not add a second independent digest implementation.

The manager's top-level acceptance `reason` remains the decision rationale. It does not substitute missing requirement evidence.

## 10. CheckRunner remains the evidence producer

No changes to process execution are required for R22. Current facts reused:

- exact CheckProfile ID/revision;
- immutable resolved argv/environment/input fingerprint;
- exact candidate/Attempt;
- `state=passed`;
- parser-specific coverage with no gaps;
- matching `check.run` Operation receipt;
- output/result artifacts;
- source/resource/process proof;
- cache source validation when reused.

One required correction in acceptance validator: current `validate_check` returns only profile ID and `seen_profiles` keys only profile ID. V2 must compare `(profile_id, profile_revision)` everywhere and preserve exact revision in the evidence manifest.

Do not weaken CheckRunner to accept a writer-supplied command or exit code.

## 11. Frontend and documentation

Update atomically:

- `TaskSpec`/acceptance schemas in MCP catalog;
- `task.create` and `task.revise` examples;
- `task.accept` schema/help: reviews cover only review-required requirements;
- CLI JSON handling if any dedicated command reconstructs nested shapes;
- `docs/task-policy.md`;
- `docs/check-runner.md`;
- `config/task-checked.example.json`;
- Task Prompt v2 brief contract in #46 after rebase.

Do not add a second method, alias or `accept_v2` endpoint. Same methods, one current write schema, historical read only.

## 12. Donor boundaries

### Cogentic

Use separation of generator/verifier/final audit and targeted briefings. Do not import its orchestration loop or LLM ledger.

### cargo-nextest

Official useful mechanisms for a **future** parser:

- machine-readable test listing;
- stable JUnit report for run results;
- filter expressions for exact test selection.

Do not use experimental run JSON as durable acceptance proof. Do not add nextest as a required project dependency in R22. A repository may choose a trusted nextest profile later.

### Existing CheckRunner

It is the primary donor. Reuse `CheckProfile`, `validate_passed_coverage`, exact operation/artifact/process checks and frozen profile revisions. The correct fix is a missing relationship, not another check engine.

## 13. Removal list

After all current V2 callers are connected:

- remove global V2 `required_check_profiles` production field;
- remove acceptance set-equality against **all** requirement review IDs;
- remove count-only `validate_checks_complete`;
- remove examples implying a requirement is machine-proved solely because a global profile exists;
- remove duplicate profile-union calculations outside the provider-neutral helper;
- block new legacy V1 Task acceptance policy writes;
- retain only isolated legacy decoder/read/finish support for frozen historical Attempts, with a named deletion condition.

No `if v2 { new } else { old }` spread across application handlers.

## 14. Criteria for the implementation

### Task policy

- [ ] V2 acceptance requires verification on every requirement.
- [ ] Review-only, check-only and review+check forms are accepted.
- [ ] Empty evidence policy rejects.
- [ ] Same profile ID with conflicting revisions rejects.
- [ ] One exact profile reused by two requirements is valid and executes once.
- [ ] New v1 writes and new v1 claims reject with explicit upgrade error.
- [ ] Frozen v1 Attempt read/finish does not change semantics.

### Acceptance

- [ ] Check-only requirement accepts without fabricated textual review.
- [ ] Review-only requirement needs exact review entry but no check.
- [ ] Review+check requires both.
- [ ] Extra review or extra CheckRun rejects.
- [ ] Wrong profile revision rejects even when profile ID and exit status match.
- [ ] Passing check for another candidate/Attempt rejects.
- [ ] Parser gaps, incomplete target coverage, invalid cache origin or absent resource release reject exactly as today.
- [ ] Evidence manifest deterministically maps each requirement to exact review/check facts.
- [ ] Coalesced acceptance returns the same retained evidence identity; changed evidence conflicts or revalidates under current existing semantics.

### Frontend/prompt

- [ ] MCP schema exposes one current V2 form.
- [ ] Task brief presents each criterion compactly.
- [ ] Raw profile argv/environment are not copied into model prompt.
- [ ] R20 prompt renderer consumes the frozen evidence plan, not a second derivation.

### Non-regression

- [ ] GM/Operator decision authority unchanged.
- [ ] Writer cannot choose command/parser/verdict.
- [ ] Independent reviewer rule unchanged.
- [ ] Machine checks do not autoaccept.
- [ ] No LLM decides whether a criterion passed.
- [ ] No new table, scheduler, service or workflow engine.

## 15. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Add V2 acceptance/requirement DTO and one provider-neutral derivation.
2. Split retained/current Task validation; enforce new-write/claim rules.
3. Update Task brief and frozen snapshot derivation.
4. Change `task.accept` review/check matching and evidence manifest.
5. Update automation acceptance caller to produce only required reviews and exact checks; do not make automation invent rationale.
6. Update MCP/CLI/examples/docs.
7. Rebase R20/#46 and consume the same frozen plan.
8. Remove old current-write/global/count-only paths.
9. Scoped formatting and Clippy.

Do not merge a DTO-only slice without Task admission and acceptance consumers.

## 16. Minimal gate

After the whole vertical slice:

```sh
cargo clippy --locked \
  -p swarm-kernel \
  -p swarm-kernel-host \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad tests/native model execution remain the final phase. Focused tests added with implementation should exercise the public Store/MCP path, not only isolated helpers.

PR report must name:

- base/head SHA;
- current policy schema/version;
- exact provider-neutral derivation function;
- Task admission/claim and acceptance callers;
- deleted global/count-only code;
- scoped Clippy result;
- historical V1 support and its deletion condition;
- checks not yet run.

## 17. Dependencies and non-goals

Dependencies:

- R20/#46 consumes the frozen evidence plan for prompt rendering;
- R21/#47 keeps requirement IDs in correction findings, but does not define acceptance criteria;
- R09/#35 stabilizes review assignment/result lifecycle;
- existing CheckRunner remains evidence producer.

Non-goals:

- baseline-relative pass/fail semantics;
- automatic Task acceptance;
- new review roles;
- named test-case parser;
- LLM-generated criteria or rationale;
- per-requirement CheckRun execution when one profile can be shared;
- arbitrary shell command in Task JSON;
- compatibility alias or second acceptance endpoint;
- rewriting historical Task/Attempt snapshots.
