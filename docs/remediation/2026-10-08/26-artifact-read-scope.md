# R26. Artifact scope: один grant для metadata, bytes, parts и assembly

**Статус:** implementation handoff. Production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённый разрыв

Artifact surface использует четыре разные semantics:

| Путь | Текущий guard |
|---|---|
| `artifact.get` | Module deny; script kinds separately scoped; остальные kinds доступны любому non-Module principal |
| `artifact.read` | review helper ограничивает Participant, но сразу `Ok(())` для любого non-Participant; script kinds separately scoped |
| `artifact.parts` | Principal не передаётся |
| `artifact.assemble` | Manager/Operator role; source-page read authority не проверяется |

Следствие: unrelated Manager или Role::Observer может получить metadata/bytes Task/native/check artifacts; Manager может собрать известные page IDs, которыми не владеет.

Content digest доказывает bytes. Он не доказывает право раскрытия.

## 2. Observer correction

`observer` — frontend profile. Default `local-observer` может использовать verified local Operator credential и должен сохранить глобальную read-only диагностику.

Поэтому R26 **не удаляет artifact methods из observer profile** как object-security fix.

| Same observer profile | Object result |
|---|---|
| verified local Operator credential | global bounded Diagnostic artifact access |
| exact current Manager/Participant/Reviewer relation | scoped access |
| separate Role::Observer without relation | NOT_FOUND |

R24 checks live method membership before IPC. R26 checks exact artifact object in Store.

## 3. Result

One resolver controls all public artifact operations:

```text
Principal + ArtifactRecord + retained provenance
  -> ArtifactReadGrant { Metadata | Bytes | Assemble, basis, domain }
  -> artifact.get / read / parts / assemble
```

`ArtifactFiles` remains an integrity/file-I/O layer and never sees Principal.

Assembled artifact inherits source-page authority. Assembly caller does not become owner of foreign bytes.

No external CAS/IAM/signed URL service, ACL table or duplicated artifact registry.

## 4. Dependencies and scope

Reuse:

- R25/#51 `TaskGraphIdentity` and Task grants;
- R23/#49 exact Operation relation;
- existing script artifact authorization;
- R05/#31 normalized candidate provenance;
- existing result page/command/Claude validators.

R26 does not change binary formats, CheckRunner execution, native result collection, candidate semantics or object ownership policy.

## 5. One internal identity

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

Kind selects one closed decoder. Metadata alone is not authority; every decoder cross-checks retained producer facts.

## 6. Kind-specific provenance

### task_submission

Cross-check immutable submission document, settled `task.submit` Operation, Task/Attempt/revision/candidate/digest/length. Derive TaskGraphIdentity.

### source_snapshot

Cross-check exact `source.capture` Operation/result, metadata Task/Attempt/revision/commit/tree/coverage and current retained artifact identity.

### check_result / check_output

Check row, `check.run` Operation, exact Attempt/candidate/profile and artifact metadata agree. Derive TaskGraphIdentity.

### native_result_page

Use current result admission validators:

- operation ID;
- binding/generation;
- selector/source identity;
- normalized producer or adapter-specific exact provenance;
- offset/length/EOF/digest;
- Task/Attempt where present.

Do not create a weaker generic decoder.

### native_result

Validate exact settled assembly Operation and all parts:

1. parts exist and cover exact bytes;
2. ordered identity/digest/EOF remains valid;
3. all parts share one semantic source identity;
4. principal has compatible grant for each source part;
5. assembled grant is the narrowest/intersection of source grants;
6. assembly caller does not widen authority.

### script kinds

Reuse `scripts::authorize_artifact_read`. Do not translate script domain into TaskGraphIdentity unless its retained contract actually contains one.

## 7. Grant and authorized record

```rust
pub(super) enum ArtifactReadLevel {
    Metadata,
    Bytes,
    Assemble,
}

pub(super) enum ArtifactReadBasis {
    LocalOperator,
    CurrentTaskManager,
    CurrentGmTaskScope,
    CurrentParticipantCandidate,
    AssignedReviewerCandidate,
    ExactCheckCaller,
    ExactResultOperationCaller,
    ValidatedOnBehalfTaskScope,
    ScriptScope,
}

pub(super) struct AuthorizedArtifact {
    pub record: ArtifactRecord,
    pub grant: ArtifactReadGrant,
}
```

Resolver:

```rust
pub(super) fn resolve_artifact_read(
    db: &Connection,
    principal: &Principal,
    artifact_id: &str,
    requested: ArtifactReadLevel,
) -> Result<Option<AuthorizedArtifact>>;
```

Load/validate once. get/read/parts do not independently reopen provenance from artifact ID.

Observer role alone grants nothing. Exact caller basis still requires producer-specific historical/current policy; it is not unconditional permanent byte access.

## 8. artifact.get

Replace public raw `results::describe` path:

```text
resolve Metadata
→ closed projection
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

No relative path, raw module receipts, tokens or full source frame.

`results::get` becomes internal loader.

## 9. artifact.read

Current post-authorization file flow is sound:

```text
DB authorization + immutable record
→ off-thread verified range read
```

Replace review/script endpoint-specific checks with one `resolve Bytes`.

Revocation after authorization affects later calls; it cannot revoke bytes already returned. No retry/model/vendor call.

## 10. artifact.parts

Pass Principal and resolve assembled artifact at Bytes level. Validate manifest and source authority before returning part refs.

Do not trust `metadata.parts` alone. Malformed manifest is `ARTIFACT_DAMAGED`, not client INVALID_PARAMS.

## 11. artifact.assemble

### Admission

Before durable Operation:

1. application mutation authority;
2. parse closed AssemblyRequest;
3. resolve `Assemble` for every page;
4. require one compatible semantic source identity;
5. retain admitted source-scope/provenance digest;
6. only then queue assembly.

Manager role alone is insufficient.

### Begin/recovery

Re-resolve exact retained pages and compare provenance with admitted scope digest. Historical exact caller recovery follows retained policy; it does not broaden to generic Manager.

### Result

Produced `native_result` stores or can derive source-domain identity. Future reads use source scope, not assembly caller ownership.

One denied/damaged page stops before file publication.

## 12. Frontend consistency

- Keep artifact methods in observer profile for local Operator compatibility.
- Store resolver distinguishes Operator credential from Role::Observer.
- R24 handles current method membership before IPC.
- Catalog/help state that artifact ID/method visibility is not read authority.
- Tests run the same observer profile under local Operator and separate Observer credentials.

## 13. Internal callers

Trusted internal integrity/settlement code may continue using raw `results::get` inside its transaction. Every user-facing metadata/bytes/parts path uses resolver.

Automation with retained on-behalf context must validate that context; it must not forge a Principal or bypass through raw loader.

## 14. Audit corrections

- `artifact.read` is not globally protected: review helper permits every non-Participant.
- Script artifacts already have separate authorization; do not call them globally exposed.
- Participant candidate/submission and reviewer candidate paths already have exact checks; preserve them.
- Artifact IDs need not be secret; knowing ID still grants nothing.

## 15. Donors

Primary donors are internal validators:

- participant artifact authorization;
- assigned reviewer candidate authorization;
- script artifact authorization;
- normalized/command/Claude result provenance;
- assembly identity/coverage plan;
- R25 TaskGraphIdentity and R23 Operation relation.

External signed URL/ACL systems duplicate local Store authority and are not needed.

Goose verified-bytes pattern applies: pass one `AuthorizedArtifact` downstream instead of reopening identity in every layer.

## 16. Files

- new `store/artifact_read_scope.rs`;
- `store/results.rs` internal loader/public projection split;
- `Store::read_artifact`;
- `store/assembly.rs::{reserve,begin,parts}`;
- assembly metadata only where source-domain retention is required;
- existing review/submission/script/result validators reused;
- MCP docs/tests only where claims/fixtures change.

Coordinate shared types/frontend edits with R23/R24/R25 through one integration owner.

## 17. Removal list

After migration remove:

- non-Participant `Ok(())` as generic artifact authorization;
- public unscoped `results::describe`;
- Principal-free `assembly::parts`;
- Manager-role-only assembly admission;
- repeated endpoint-specific review/script branches;
- assembly caller ownership of result bytes.

Do **not** remove artifact methods from observer profile solely as object-security fix.

## 18. Criteria

- [ ] Same observer profile + local Operator reads global bounded artifact diagnostics.
- [ ] Same profile + unrelated Role::Observer cannot get/read/parts Task/native/check artifacts.
- [ ] Current Task manager/GM reads exact Task artifacts.
- [ ] Participant reads exact current candidate/submission only.
- [ ] Assigned reviewer reads exact candidate/check evidence only.
- [ ] Script authorization remains exact.
- [ ] Manager cannot assemble pages it cannot read.
- [ ] All assembled pages share semantic source identity and compatible grant.
- [ ] Assembled result inherits source scope, not assembler identity.
- [ ] Denied assembly publishes no file or Operation effect.
- [ ] Digest/range/EOF/source checks remain.
- [ ] Direct IPC and MCP decisions match.

## 19. Implementation order

1. Rebase after R23/R25 grant shapes stabilize.
2. Add kind identity decoder/resolver; wire get/read.
3. Wire parts.
4. Wire assembly admission/begin/result inheritance.
5. Add public-boundary fixtures for local Operator vs Observer role and denied file publication.
6. Remove old unscoped/duplicated paths.
7. Scoped formatting and Clippy.

One manager/worktree; writers do not run Cargo. Do not merge resolver-only code without all public callers.

## 20. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-contracts \
  -p swarm-mcp \
  -p swarm-cli \
  --lib --bins -- -D warnings
```

Broad/native tests remain final phase.

## 21. Non-goals

- artifact format changes;
- external object store/signed URLs;
- public artifact inventory;
- native result collection changes;
- historical metadata rewrite;
- artifact retention/deletion;
- encrypting local files;
- removing observer profile methods as a substitute for Store authorization.
