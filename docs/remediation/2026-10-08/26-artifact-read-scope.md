# R26. Artifact scope: один grant для metadata, bytes, parts и assembly

**Статус:** implementation handoff. В текущей ветке production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённый разрыв

Artifact surface использует четыре разные authorization semantics:

| Путь | Текущий guard |
|---|---|
| `artifact.get` → `results::describe` | только Module deny; script kinds дополнительно `scripts::authorize_artifact_read`; остальные kinds доступны любому non-Module principal |
| `artifact.read` → `Store::read_artifact` | `reviews::authorize_artifact_read`; но helper сразу `Ok(())` для любого non-Participant; script kinds отдельно защищены |
| `artifact.parts` → `assembly::parts` | Principal не передаётся; assembled manifest доступен глобально |
| `artifact.assemble` | только role Manager/Operator; source pages проверяются на existence/integrity, но не на право caller читать их |

MCP Observer profile дополнительно выставляет `artifact.get/read/parts`, поэтому разрыв не ограничен direct IPC.

Следствие:

- unrelated Manager/Observer может получить metadata и bytes `source_snapshot`, `task_submission`, `check_result`, `check_output`, `native_result_page`, `native_result`;
- Manager может собрать arbitrary known page IDs в новый `native_result`, даже если не имеет права читать source pages;
- право assembler Operation фактически становится правом на чужое содержимое;
- integrity (digest/content-address) смешана с confidentiality/authority.

Content digest доказывает, какие bytes сохранены. Он **не** доказывает, кому их разрешено раскрывать.

## 2. Результат

Одна функция разрешает exact retained artifact и выдаёт уровень доступа:

```text
Principal + ArtifactRecord + retained provenance
→ ArtifactReadGrant { Metadata | Bytes | Assemble, basis, scope }
→ artifact.get / read / parts / assemble
```

Все четыре paths используют один resolver. `ArtifactFiles` остаётся чистым integrity/file-I/O слоем и не знает principals.

Assembled artifact наследует authorization источников. Caller, который создал assembly Operation, не становится владельцем чужих source bytes.

No external CAS service, signed URL framework, ACL table, IAM engine или duplicated artifact registry.

## 3. Scope and dependencies

R26 зависит от:

- R25/#51 `TaskGraphIdentity/TaskReadGrant` для Task-bound artifacts;
- R23/#49 Operation read scope для exact caller/on-behalf provenance;
- existing script artifact authorization;
- R05/#31 normalized candidate provenance;
- R22/#48 CheckRun/acceptance evidence identities.

R26 не меняет:

- artifact integrity/file format;
- result page production;
- Task submission/candidate semantics;
- CheckRunner process execution;
- object ownership rules themselves.

## 4. One internal artifact identity

```rust
pub(super) enum ArtifactDomainIdentity {
    Task(TaskGraphIdentity),
    Script(ScriptArtifactIdentity),
    NativeResult(NativeResultIdentity),
}

pub(super) struct NativeResultIdentity {
    pub binding_id: String,
    pub binding_generation: i64,
    pub operation_id: String,
    pub task: Option<TaskGraphIdentity>,
    pub native_session_id: Option<String>,
    pub native_input_id: Option<String>,
    pub source_kind: String,
}
```

Artifact kind selects one closed decoder. Unknown kind is already rejected by `results::get`/`ArtifactFiles::path`.

Do not trust artifact metadata in isolation. Every decoder cross-checks its retained producer:

- source Operation;
- Task/Attempt/candidate/check/submission row;
- normalized module receipt/provenance;
- script registry/run;
- assembly source pages.

## 5. Kind-specific provenance

### 5.1 `task_submission`

Use exact current `submissions::document`/Operation checks:

- metadata Task/Attempt/revision/candidate/operation;
- exact settled `task.submit` result;
- artifact digest/length match immutable document;
- derive `TaskGraphIdentity`.

Participant owner/producer read remains through current exact candidate/submission authorization. Manager/GM requires Task scope. Assigned reviewer requires exact assignment submission/candidate scope.

### 5.2 `source_snapshot`

Require metadata:

```text
task_id
attempt_id
task_revision
commit/tree/file_count/coverage
```

Cross-check exact source.capture Operation/result, Attempt and candidate relationship. Current Task manager/participant/reviewer rules derive from TaskGraphIdentity. Artifact caller may read its own bounded receipt only if current existing policy permits that source actor.

### 5.3 `check_result` and `check_output`

Derive exact CheckRun:

- check row names artifact as result/output;
- check.run Operation names exact Attempt/candidate/profile;
- artifact metadata identity agrees;
- TaskGraphIdentity from Attempt.

Grant uses R25:

- current manager/GM exact Task;
- assigned reviewer exact candidate/Attempt;
- exact check caller bounded evidence;
- current Participant only if current canonical work/review policy already permits it.

Do not expose inherited environment/raw secrets. Metadata projection stays current public/redacted shape.

### 5.4 `native_result_page`

Validate the same provenance used for result admission/assembly:

- artifact metadata operation ID;
- binding/generation;
- selector and source identity;
- normalized origin/producer or adapter-specific exact source;
- page offset/length/EOF/digest;
- source Operation and Task/Attempt where available.

Use existing validators from `results`, `normalized_result`, `command_results`, Claude/Antigravity status paths. Do not create a weaker generic decoder.

Grant is inherited from exact source Operation/Task/binding relation. Merely knowing artifact ID is insufficient.

### 5.5 `native_result`

Assembled metadata contains:

```text
assembly_operation_id
identity
parts[]
coverage
byte_length/sha256
```

Validate:

1. exact settled `artifact.assemble` Operation;
2. every part exists and matches ordered identity/digest/coverage;
3. all parts resolve to the **same semantic source identity**;
4. caller has a grant for each part;
5. assembled grant is intersection/narrowest grant of parts;
6. assembly caller does not widen the grant.

If one source part becomes damaged/unavailable, metadata/read/parts returns explicit artifact damage/gap; it does not silently authorize via assembly Operation owner.

### 5.6 Script kinds

Keep `scripts::authorize_artifact_read` as the authoritative domain-specific check. Wrap it in the common resolver, do not translate script bundle/run scope into TaskGraphIdentity unless the script contract actually contains one.

## 6. ArtifactReadGrant

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ArtifactReadLevel {
    Metadata,
    Bytes,
    Assemble,
}

pub(super) struct ArtifactReadGrant {
    pub level: ArtifactReadLevel,
    pub basis: ArtifactReadBasis,
    pub domain: ArtifactDomainIdentity,
}
```

Positive bases:

```text
LocalOperator
CurrentTaskManager
CurrentGmTaskScope
CurrentParticipantCandidate
AssignedReviewerCandidate
ExactCheckCaller
ExactResultOperationCaller
ValidatedOnBehalfTaskScope
ScriptScope
```

Observer role alone grants nothing.

Exact caller does not automatically get every byte forever. Caller basis must correspond to the producer Operation and current/historical policy of that artifact kind.

## 7. One resolver

Add small module:

```text
store/artifact_read_scope.rs
```

API:

```rust
pub(super) fn resolve_artifact_read(
    db: &Connection,
    principal: &Principal,
    artifact_id: &str,
    requested: ArtifactReadLevel,
) -> Result<Option<ArtifactReadGrant>>;
```

Sequence:

1. `current_principal` already applied by caller;
2. load exact ArtifactRecord once;
3. decode/cross-check provenance by kind;
4. resolve domain-specific principal relation;
5. require `grant.level >= requested`;
6. return grant + record to avoid second divergent load.

Prefer:

```rust
pub(super) struct AuthorizedArtifact {
    pub record: ArtifactRecord,
    pub grant: ArtifactReadGrant,
}
```

Do not load/validate the same artifact separately in get/read/parts.

## 8. `artifact.get`

Replace `results::describe` public behavior with:

```text
resolve Metadata
→ closed public artifact projection
```

Projection:

```text
artifact_id
kind
byte_length
content_digest
bounded public_metadata
scope summary permitted by grant
```

No internal relative_path, raw provenance receipts, tokens or full source frames.

`results::get` remains internal loader.

## 9. `artifact.read`

Current byte I/O flow is good after authorization:

```text
DB authorization + record
→ off-thread verified range read
```

Replace `reviews::authorize_artifact_read` + script special case with one `resolve Bytes` call.

Do not reauthorize after file read as if it could undo disclosure. Authorization and immutable record are captured first; file read verifies exact committed bytes. Revocation after authorization affects later calls, not already returned bytes.

No retry or model call.

## 10. `artifact.parts`

Pass Principal. Resolve assembled artifact at Metadata or Bytes level (choose Bytes because part IDs expose retrievable content identity). Validate manifest and return only authorized part references.

Each part need not repeat a full principal resolver if assembled provenance was verified in one function, but resolver must prove all parts share the same/narrower grant. Do not trust `metadata.parts` array alone.

Cursor/limit remains bounded; malformed part manifest is `ARTIFACT_DAMAGED`, not INVALID_PARAMS.

## 11. `artifact.assemble`

### Admission

Before creating Operation:

1. role/application mutation authority as today;
2. parse/validate AssemblyRequest;
3. resolve `Assemble` grant for every page_ref;
4. require same semantic source identity and compatible grant;
5. store exact source scope/grant digest in effective request/receipt;
6. only then durable Operation admission.

Manager role alone is insufficient.

### Begin/recovery

`assembly::begin` re-resolves exact retained pages and compares current immutable provenance to admitted scope digest. It does not require the manager still owns current Task if historical assembly recovery policy already permits exact caller readback; define this using retained caller/grant semantics, not broad current Manager role.

### Result authorization

The produced `native_result` stores source-derived domain identity/grant basis (or a derivable sealed scope), not only assembly Operation ID. Future read uses source scope.

Loss/recovery remains deterministic local publication; no native retry behavior changes.

## 12. Observer/frontend changes

Remove from Observer MCP profile:

```text
artifact.get
artifact.read
artifact.parts
```

until a named safe public artifact summary exists. Observer retains aggregate monitoring.

Manager/GM/Participant/AssignedReviewer exposure may stay where Store can resolve exact object. R24 ensures live membership before IPC; R26 still enforces object scope.

Update catalog docs: `assignment-read` does not grant arbitrary artifacts.

## 13. Direct IPC and other callers

Store resolver is authoritative for all frontends. CLI/MCP profile is not security boundary.

Inventory internal callers of `results::get`:

- internal validation/settlement may continue using raw loader within trusted transaction;
- any user-facing projection/read must use resolver;
- background automation with retained on-behalf context must use an explicit internal domain validator, not forge a Principal or bypass via `results::get`.

Do not refactor every internal integrity check through public authorization.

## 14. Audit corrections / bounded claims

- `artifact.read` is not generally protected for Manager/Observer: current review helper returns Ok for every non-Participant.
- Script artifacts are already separately scoped; do not claim they are globally exposed.
- Participant work/review candidate paths already have exact checks; preserve them.
- Content-addressed IDs prevent byte substitution, not unauthorized disclosure.
- No claim is made that artifact IDs are secret; authority is required even when ID is known.

## 15. Donors

Primary donors are internal exact validators:

- `submissions::authorize_participant_artifact_read`;
- `reviews::authorize_artifact_read` Participant branch;
- `scripts::authorize_artifact_read`;
- `normalized_result`/Claude/command result provenance validators;
- `ArtifactFiles::assemble` source identity/coverage plan;
- R25 TaskGraphIdentity and R23 OperationReadGrant.

External object stores often use signed URLs/ACL metadata. That would duplicate local Store authority and complicate offline local IPC. No external CAS/IAM dependency.

Goose verified-bytes pattern still applies: after one authorization/provenance validation, pass exact `AuthorizedArtifact` downstream; do not reopen identity from arbitrary ID in every layer.

## 16. Files/symbols

Primary:

- new `store/artifact_read_scope.rs`;
- `store/results.rs::{get,describe}` internal/public split;
- `Store::read_artifact`;
- `store/assembly.rs::{reserve,begin,parts}`;
- artifact assembly metadata/provenance only where needed;
- `store/reviews.rs` and `store/submissions.rs` existing participant helpers reused;
- `store/scripts.rs` existing script helper reused;
- MCP profiles/catalog/docs/tests.

Coordinate with R25/#51 and R23/#49 before shared frontend edits.

## 17. Removal list

After migration remove:

- non-Participant `Ok(())` as generic artifact authorization;
- public raw `results::describe` without domain scope;
- Principal-free `assembly::parts`;
- Manager-role-only assembly admission;
- duplicate script/review special cases in each endpoint;
- Observer artifact tools;
- any assembly-result rule that grants access from assembler caller alone.

No compatibility union or hidden unscoped fallback.

## 18. Criteria

### Authorization

- [ ] Unrelated Manager/Observer cannot get/read/parts Task/native/check artifacts.
- [ ] Current Task manager/GM can read exact Task artifacts.
- [ ] Participant reads exact current candidate/submission only.
- [ ] Assigned reviewer reads exact assigned candidate/check evidence only.
- [ ] Script artifacts preserve script-specific authorization.
- [ ] Local Operator retains diagnostic access.
- [ ] Direct IPC and MCP return same object decision.

### Assembly

- [ ] Manager cannot assemble pages it cannot read.
- [ ] All pages require one semantic source identity and compatible grant.
- [ ] Assembled result inherits source scope, not assembler ownership.
- [ ] Recovery revalidates admitted source scope and never broadens it.
- [ ] `artifact.parts` cannot reveal manifest/IDs without artifact grant.
- [ ] One unauthorized/damaged page prevents assembly before file publication.

### Integrity/non-regression

- [ ] Existing digest/range/EOF/source identity checks remain.
- [ ] File I/O stays off Store thread.
- [ ] Authorization does not invoke model/vendor service.
- [ ] No new artifact table/service/ACL engine.
- [ ] Public metadata remains bounded/redacted.

## 19. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Rebase after R25 TaskGraphIdentity/R23 Operation grant shapes stabilize.
2. Add kind-specific identity decoder and common resolver; wire `artifact.get/read`.
3. Wire `artifact.parts` and validate manifest through resolver.
4. Wire `artifact.assemble` admission/begin/result inheritance.
5. Narrow MCP profile/catalog and add public-boundary denial fixtures.
6. Remove old endpoint-specific guards/unscoped paths.
7. Scoped formatting and minimal Clippy.

Do not merge resolver-only code without all four public callers.

## 20. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-contracts \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad/native tests remain final phase. Focused tests enter public Store/MCP get/read/parts/assemble paths and assert no file publication for denied assembly.

## 21. Non-goals

- changing artifact binary formats;
- signed URLs/external object store;
- public artifact inventory;
- Task/check/review semantic changes;
- native result collection changes;
- rewriting historical artifact metadata;
- automatic artifact deletion/retention;
- encrypting local artifact files;
- making artifact IDs confidential.
