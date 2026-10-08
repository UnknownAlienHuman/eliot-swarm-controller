# R25. Task graph read scope: Task/Attempt/submission/acceptance/check/family

**Статус:** implementation handoff. В текущей ветке production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённый разрыв

Generic Store `read()` вызывает без `Principal`:

```text
tasks::get_task
tasks::list
tasks::get_attempt
submissions::describe
acceptance::describe
checks::describe
producers::family
assembly::parts
```

`agent.state/list` получает Principal, но для любого non-Operator возвращает public binding projection без Task/Attempt scope. `results::describe` принимает Principal, однако для большинства result kinds применяет authorization только к script artifacts.

Participant и AssignedReviewer частично перехватываются до generic read:

- exact current/historical assignment evidence;
- scoped candidate/submission/check/artifact;
- current participant context.

Manager и Observer идут в generic path. Поэтому session-fixed profile `assignment-read` фактически даёт глобальный Task/Attempt/evidence inventory, хотя документация прямо говорит:

```text
A role does not obtain a global Task inventory merely by loading this group.
```

Это отдельный object-authorization class от R23 Operation reads. Нельзя исправить его только удалением методов из MCP: direct IPC/other frontend остаётся.

## 2. Граница R25

R25 исправляет только объекты, которые выводят exact Task graph identity:

```text
Task
Attempt
Task submission
Task acceptance
CheckRun
Agent family / producer set
```

Не входят:

- generic Operation reads — R23/#49;
- artifact metadata/bytes/pages — отдельный artifact authority slice;
- fleet-wide binding/route monitoring — отдельный monitoring contract;
- mutation authorization;
- Task Prompt/Requirement semantics — R20/#46 и R22/#48.

## 3. Результат

Один resolver определяет current/historical read relation для exact Task graph:

```text
Principal + TaskGraphIdentity + object kind
→ TaskReadGrant { Summary | Detail | Evidence, basis }
→ closed projection
```

Unknown/malformed relation fail-closed. Local verified Operator retains global diagnostic access. Observer does not gain raw Task inventory by role alone.

No external IAM/ACL table, no duplicate Task store, no public grant tokens.

## 4. One exact identity

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskGraphIdentity {
    pub project_id: String,
    pub task_id: String,
    pub task_revision: Option<i64>,
    pub attempt_id: Option<String>,
    pub binding_id: Option<String>,
    pub binding_generation: Option<i64>,
}
```

Identity is derived from authoritative retained object, not request JSON.

Object-specific loaders:

```rust
fn identity_for_task(...)
fn identity_for_attempt(...)
fn identity_for_submission(...)
fn identity_for_acceptance(...)
fn identity_for_check(...)
fn identity_for_family(...)
```

Each validates internal coherence before returning identity:

- Attempt Task/revision exists and matches;
- submission document and Operation/candidate metadata agree;
- acceptance decision and Task accepted pointers agree;
- CheckRun spec/Operation/Attempt/candidate agree;
- family binding belongs to exact Attempt and producer assignments.

Do not accept caller-provided Task ID as a substitute for object identity.

## 5. Read grants

```rust
pub(super) enum TaskReadLevel {
    Summary,
    Detail,
    Evidence,
}

pub(super) enum TaskReadBasis {
    LocalOperator,
    CurrentTaskManager,
    CurrentGmProjectScope,
    CurrentParticipant,
    RetainedAssignedReviewer,
    ExactObjectCaller,
}
```

### 5.1 Local Operator

`require_local_operator` → Evidence/global.

### 5.2 Current Task manager/current GM

Use existing `current_manager_has_task_scope` and exact project identity.

- current Task/Attempt → Detail/Evidence as needed;
- historical accepted/released Task may remain readable to exact current GM/project only where existing owner policy says successor read is retained;
- Manager role alone is not sufficient.

### 5.3 Current Participant

Use existing `load_current_scope_for_client` / registration/basis:

- exact current Task/Attempt only;
- Task/Attempt Summary/Detail bounded;
- own candidate/submission/check/family only through exact assignment/producer relation;
- no full sibling/other Attempt inventory.

### 5.4 Assigned reviewer

Reuse review assignment scope:

- exact assignment Task/Attempt/submission/candidate;
- exact linked CheckRuns and family/evidence needed by canonical reviewer contract;
- retained historical reads remain only where existing assignment contract permits;
- no unrelated Task list.

### 5.5 Exact object caller

For readback objects created by an Operation (check, acceptance, submission), exact caller may retain its own bounded receipt/evidence after Task transition. Caller relation does not grant the rest of Task inventory.

## 6. Closed projections

### Task Summary

```text
task_id
project_id only when grant permits
revision/state/phase
current Attempt ID only when in scope
accepted status/candidate refs only when in scope
```

No full owner policy, source index, baseline receipts or dependency evidence for Observer/unscoped caller.

### Task Detail

Exact frozen spec/brief, current Attempt and acceptance pointers for authorized manager/participant/reviewer.

### Evidence

Exact submission/check/acceptance/family receipts needed by authorized role. Still bounded and redacted; not raw internal JSON.

Existing `tasks::get_task`/`get_attempt` become internal loaders. Public projection is separate.

## 7. Method-by-method wiring

### task.get

- derive identity from stored Task;
- resolve grant;
- return Summary/Detail.

### task.list

- no global `tasks::list` to Manager/Observer;
- bounded stable scan + exact resolver;
- filtered rows advance scan cursor with explicit coverage;
- first authorized-but-not-emitted row is not skipped;
- current OFFSET pagination over mutable list is replaced by versioned stable cursor.

Observer profile loses `task.get/list` until a named aggregate/public consumer exists. `swarm.dashboard` remains aggregate monitoring.

### attempt.get

- identity from Attempt + Task;
- exact current manager/participant/reviewer relation;
- historical own/accepted evidence by explicit policy only.

### task.submission

`submission_ref` document contains Task/Attempt/candidate identity. Resolve and authorize before pagination/projection. Existing Participant/AssignedReviewer specialized guard remains authoritative.

Manager path uses TaskReadGrant; Observer removed.

### task.acceptance

Acceptance Operation/decision must bind exact Task/Attempt/candidate and accepted Task pointers. Only exact Task manager/current GM/operator or retained canonical reviewer evidence path sees it. No global observer read.

### check.get

CheckRun identity derives from check row + check.run Operation + Attempt + candidate. Preserve exact assigned-reviewer evidence read. Manager requires Task scope. Exact check caller may read own receipt. Observer removed.

### agent.family

Binding/generation → exact Attempt/Task. Current manager/participant producer/reviewer gets bounded family projection according to role. Operator diagnostic remains global. Do not treat family observation as complete if coverage is partial.

R25 does not change `agent.state/list`; fleet monitoring needs its own explicit aggregate/scope design.

## 8. Application/frontend consistency

Update together:

- Store read handlers accept `Principal` or call shared resolver;
- MCP Observer profile removes Task/Attempt/submission/acceptance/check/family assignment reads;
- Manager/GM/Participant/AssignedReviewer profile exposure stays only where Store can authorize exact objects;
- `mcp.authorization.allowed_methods` reflects current role/scope but does not replace object resolver;
- R24 ensures hidden/revoked methods do not reach target IPC;
- catalog/docs state exact object scope.

Direct IPC gets same Store checks; MCP is not the security boundary.

## 9. Pagination and gaps

One invisible object must not fail a whole list or reveal its existence through counts.

For list-like methods:

```text
stable source position
→ load bounded candidates
→ resolve grants
→ project within item/byte budget
→ explicit filtered/damaged coverage
```

Separate filtered/unauthorized from damaged retained object. SQL/storage error remains error; missing current relation returns NOT_FOUND/filtered according to method contract.

Do not use OFFSET over live rows as stable cursor.

## 10. Artifact boundary

R25 intentionally does not “fix” artifact access by assuming every artifact has Task metadata. Artifact kinds include script bundles/results and other scopes.

However Task graph projectors must return only artifact references whose later `artifact.get/read/parts` authority can be proved. Do not leak an artifact ID that generic artifact endpoints expose globally.

A subsequent artifact-scope PR should reuse `TaskGraphIdentity` where applicable and script-specific authorization for script kinds; it must not duplicate R25.

## 11. Audit corrections / bounded claims

- Participant generic Task/evidence leak is not claimed where Store already intercepts with `reviews::authorize_evidence_read` or coordination scope.
- `results::describe` is not wholly unauthenticated: script artifact kinds use script authorization. The confirmed gap is that non-script artifacts lack equivalent Task/Attempt scope.
- `agent.state/list` are not included in R25; they expose public binding projections and require a separate monitoring decision.
- This PR does not infer confidentiality stronger than documented local credential isolation; it enforces the project’s own object-scope contract.

## 12. Donors

Primary donors are internal:

- review `authorize_evidence_read`;
- participant candidate/submission/check authorization;
- `current_manager_has_task_scope`;
- Task/Attempt frozen identity validation;
- CheckRunner exact candidate/Attempt checks.

Paseo/other session managers do not solve Task graph object authorization. External IAM/graph service is unnecessary.

## 13. Files/symbols

Primary:

- new small `store/task_read_scope.rs`;
- `store/mod.rs::read` Task/Attempt/evidence/family arms;
- `store/tasks.rs` internal loaders vs projections/list scan;
- `store/submissions.rs::describe`;
- `store/acceptance.rs::describe`;
- `store/checks.rs::describe`;
- `store/producers.rs::family`;
- `swarm-mcp/src/mcp/profiles.rs`, catalog/docs/tests.

R23 handles Operation; R24 handles live method membership; coordinate frontend edits to avoid parallel writers in profiles/tests.

## 14. Removal list

After migration remove:

- principal-free public Task/Attempt/evidence handlers;
- global Observer assignment-read methods;
- raw `get_task/get_attempt` as public projection;
- offset pagination for Task list;
- duplicated participant/reviewer identity derivations replaced by shared TaskGraphIdentity;
- tests that assert only object shape but not unrelated-principal denial.

No compatibility union or role-only fallback.

## 15. Criteria

- [ ] Unrelated Manager/Observer cannot get/list Task or Attempt by ID.
- [ ] Current manager/current GM exact project scope can read authorized Task graph.
- [ ] Participant sees exact current Task/Attempt and own evidence only.
- [ ] Assigned reviewer sees exact assignment evidence, including permitted retained history.
- [ ] Exact check/submission/acceptance caller reads own bounded receipt but not Task inventory.
- [ ] `task.list` filters before projection/pagination and uses stable cursor.
- [ ] `check.get`, `task.submission`, `task.acceptance`, `agent.family` all use same TaskGraphIdentity derivation.
- [ ] One damaged foreign object does not leak or wedge healthy authorized page.
- [ ] Observer profile exposes aggregate monitoring, not raw assignment graph.
- [ ] Store direct IPC and MCP produce the same authorization outcome.

## 16. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Add TaskGraphIdentity and resolver; wire `task.get/attempt.get`.
2. Add bounded `task.list` cursor.
3. Wire submission/acceptance/check/family.
4. Narrow MCP profile/catalog and add public-boundary denial fixtures.
5. Remove raw/principal-free paths.
6. Rebase R22/R09 consumers and R24 frontend edits.
7. Scoped formatting and minimal Clippy.

Do not merge resolver-only code without all listed production readers.

## 17. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-contracts \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad tests/native execution remain final phase. Focused tests enter Store/MCP public methods with unrelated/current/historical principals.

## 18. Non-goals

- Operation authorization;
- artifact byte authorization;
- fleet-wide binding monitoring;
- mutation authority changes;
- new IAM service/table;
- public Task summary feature;
- rewriting historical Task/Attempt/evidence records;
- changing requirement/check/acceptance semantics;
- automatic Task acceptance.
