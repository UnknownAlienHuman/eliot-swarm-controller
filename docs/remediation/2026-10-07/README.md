# ELIOT — implementation readiness index

**Обновлено 8 октября 2026. База source review: `40591a295af94b1541ec2ba30afe8e3247701a71`.**

Это единственная рабочая навигация для исполнителей. Она не заменяет handoff выбранного PR. Агент читает:

```text
этот index
→ body своего PR
→ один-два связанных handoff-файла
→ текущий diff и названные source symbols
```

Не читать master audit как рабочий brief. Один manager — один worktree. Writers получают непересекающиеся файлы, не запускают Cargo и не пушат самостоятельно. Новый type/helper считается поставленным только вместе с production caller и удалением прежней ответственности.

## 1. Текущий статус

- Критический список повторно классифицирован: подтверждённые пункты имеют PR-владельца; неверные формулировки сняты или сужены.
- Полный HIGH/MED appendix ещё проверяется. **Аудит и подготовка не объявлены завершёнными.**
- Большинство веток — docs-only implementation handoff. Documentation CI не является Rust/runtime qualification.
- Код уже есть только в отдельных старых кандидатах: #26, #30, #36, #37. Их состояние проверять по exact diff/head, а не по старому отчёту.
- Независимые задачи Wave A/B можно реализовывать сейчас; ждать полного завершения аудита не требуется.

## 2. Обязательный порядок работы агента

```sh
git status --short
git rev-parse HEAD
git diff --stat origin/main...HEAD
```

Затем:

1. Сверить handoff с текущим `main`; уже исправленное не переписывать.
2. Построить один законченный `producer → retained fact/intent → effect → readback → bounded projection`.
3. Сначала использовать существующую ELIOT function/type. Donor применяется только в указанной узкой границе.
4. Не писать общий framework до двух реальных connected callers.
5. После переключения caller удалить прежний writer/renderer/state machine. Не оставлять постоянный `legacy/new/fallback` production fork.
6. Unknown external effect никогда не replay-ить вслепую.
7. Manager выполняет только scoped formatting/Clippy из handoff после полного code slice. Broad/native/load qualification — финальная фаза.

## 3. Wave 0 — восстановить проверяемую базу

| PR | Состояние | Действие |
|---|---|---|
| [#26](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/26) | code candidate | Перепроверить exact head, 18 compiler errors и текущий CI; получить один согласованный compiling baseline. Не копировать эти fixes во все ветки. |
| [#36 / R10](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/36) | partial code | Закончить scoped Rust gate и behavior qualification exact impact/disable path. |
| [#37 / R11](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/37) | partial code | Довести closed terminal event codec/aliases; не ослабить `authenticated_module_hello`. |
| [#30 / R04](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/30) | partial Muse code | Исправить artifact/example mismatch, затем проверить pending inventory races. Не активировать несовпадающий package. |

## 4. Wave A — малые shared primitives

| PR | Владеет | Первый production seam | Не смешивать |
|---|---|---|---|
| [#75 / R52](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/75) | SQLite writer bootstrap | `swarm-store::open_writer_inner`: options before I/O, postconditions before commit | Не добавлять pool/retry/migration framework |
| [#63 / R39](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/63) | crash-repairable state marker | shared `swarm-process::state_marker` → DataRoot | Module lifecycle остаётся #27 |
| [#65 / R41](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/65) | durable create/replace/remove + torn-tail verdict | shared file primitives → OpenCode/Claude journals | Retention policy остаётся #69 |
| [#61 / R35](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/61) | finite child/family/capture completion | CheckRun typed completion → Store cleanup-pending | Не добавлять PTY/process registry |
| [#56 / R30](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/56) | one GM designation/epoch | typed designation + Operation-derived epoch high-water | Не создавать IAM service |
| [#57 / R31](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/57) | closed RuntimeCommand registry | classify method/delivery/native-MCP phase; unknown fail closed | Application method registry #40 другой домен |

`swarm-process` изменения #63/#65/#61 интегрирует один manager последовательно. После них rebased consumers удаляют локальные копии.

## 5. Wave B — core identity, provenance и object reads

| PR | Владеет | Порядок |
|---|---|---|
| [#53 / R27](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/53) | coalesced create/claim/release Operation scope; CheckRun release wake | До #49/#51 |
| [#54 / R28](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/54) | `task.dispatch` start-slot reuse receipt | Один manager/rebase с #46 в `operations.rs` |
| [#31 / R05](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/31) | result provenance, exact expected Attempt | До artifact/submission readers |
| [#48 / R22](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/48) | Requirement → exact review/CheckRun evidence | После stable review/check identities |
| [#49 / R23](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/49) | fail-closed Operation get/list/delta | После #53/#54/#56 |
| [#51 / R25](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/51) | Task/Attempt/submission/acceptance/check/family reads | Reuse #49 relation resolver; fleet dashboard stays bounded/global by design |
| [#52 / R26](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/52) | artifact get/read/parts/assemble grant | Reuse #31/#49/#51 provenance |
| [#50 / R24](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/50) | live MCP allowed-method gate | One manager/rebase with #55 |
| [#55 / R29](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/55) | caller-owned logical request ID before effect | Same predicate later reused by #40 |

Authoritative object relation comes from retained columns/typed records, not handler `result_json`. Scope-specific readers do not invent a generic ACL engine.

## 6. Wave C — coordination, review и frontend

| PR | Владеет | Порядок/граница |
|---|---|---|
| [#32 / R06](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/32) | one work context, registration/fingerprint, code-scope | Existing `ScopeData`; no second context framework |
| [#58 / R32](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/58) | canonical proposal digest + ratify/reject | После #56 and shared context #32 |
| [#34 / R08](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/34) | delivery identity/order, mailbox cursor, watch, subscription cutoff | One digest producer; collision-safe lookup; monotonic sequence |
| [#35 / R09](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/35) | review assignment replacement, late result, exact link | Correction package отдельно #47 |
| [#47 / R21](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/47) | multi-finding `CorrectionPackageV1` | После #35; one package → one feedback/send |
| [#33 / R07](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/33) | contract/Concilium terminal/advisory path and exact ScriptRun source proof | После #56/#58; no model consensus authority |
| [#40 / R14](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/40) | data-only frontend/method/schema extraction | Последним после changing method forms and #50/#55 |

Shared-file serialization:

```text
store/coordination.rs: #32 → #58 → #34/#35 narrow seams
store/code_scopes.rs: #32 → #58/#39 consumers
MCP/CLI schemas: domain PR first → #40 extraction last
```

## 7. Wave D — automation, scheduler, launcher и resources

| PR | Владеет | Главное удаление/переиспользование |
|---|---|---|
| [#60 / R34](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/60) | poison-fact isolation, per-domain transactions | One private `Applied/Pending/Skipped/Quarantined`; no second DLQ |
| [#68 / R45](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/68) | truthful automatic-effect states + exact Forge ref CAS | One lifecycle parser; delete divergent local state allowlists |
| [#38 / R12](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/38) | due-source sequencing, pacing, issuance, DST | UTC floor before timezone; no cron engine |
| [#74 / R51](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/74) | complete launch-plan authority separate from bounded preview | Delete preview-as-effect-authority and duplicate WorkDispatch plan |
| [#64 / R40](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/64) | typed provider condition + route launch gate | Native fact → one Store row → one final admission helper |
| [#39 / R13](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/39) | capacity/resource/workspace/owned-service fences | Malformed ≠ empty; exact lease lineage; delete broad Task-wide holds |
| [#62 / R38](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/62) | resumable Git-hook install/revoke and reachable emit | One phased manifest; consume #61 finite process and #65 durable files |
| [#67 / R44](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/67) | ScriptRun start allow/deny race | One immutable start decision; delete competing go/deny writers |

Safe order in shared automation/launcher files:

```text
#60 poison isolation
→ #68 terminal state vocabulary
→ #38 scheduler pacing
→ #74 complete plan authority
→ #64 provider gate
→ #39 resource/release predicates
```

One completed vertical slice at a time; no parallel global refactor in the same Store file.

## 8. Wave E — host/module lifecycle

| PR | Владеет | Зависимости |
|---|---|---|
| [#27 / R01](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/27) | exact module owner handoff, worker/hello readiness, bounded replacement | Consume #63/#61 shared primitives |
| [#71 / R48](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/71) | optional bus/scheduler supervisors, scoped isolation, receipt closure | Rebase on #27/#61 |
| [#72 / R49](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/72) | Zed one-shot process ownership, durable controls, exhaustive command intake | Rebase on #61/#65; no persistent-session redesign |
| [#37 / R11](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/37) | closed terminal event codec/aliases/secondary codes | Preserve exact hello-gated Ready proof |

`ProcessRunning/Ready` remains gated by Store-confirmed exact `module.hello`. Spawn/process presence is never readiness proof.

## 9. Wave F — adapters and native services

### Codex

| PR | Scope | Order |
|---|---|---|
| [#29 / R03](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/29) | provisional thread candidate → exact Active root; steer without full-history preflight | Root classifier/adoption first |
| [#41 / R15](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/41) | continuous read-pump + Codex/Muse usage facts | Must not project provisional root as live |
| [#66 / R43](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/66) | one current Codex Goal controller | Reuse #29 Active-root predicate |

### OpenCode

| PR | Scope | Order |
|---|---|---|
| [#65 / R41](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/65) | durable journal/file seam | First |
| [#28 / R02](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/28) | recovery/outbox/IPC/result paging/owner lifetime | Rebase on #65/#27/#61 |
| [#43 / R17](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/43) | queue/loop-step, forms, permissions, background, durable log | After recovery seam |
| [#69 / R46](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/69) | acknowledged journal retention | Last; only exact releasable evidence |

### Claude

| PR | Scope | Order |
|---|---|---|
| [#65 / R41](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/65) | durable journal/file seam | First |
| [#42 / R16](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/42) | permission/reply/defer + terminal harness retirement + intent-once result Unknown | Consume #27/#61; no same-process root reopen |
| [#69 / R46](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/69) | acknowledged journal retention | After exact lifecycle semantics |

### Muse / Command / Antigravity / OpenCodex

| PR | Scope | Order |
|---|---|---|
| [#30 / R04](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/30) | Muse pending inventory freshness | Resolve artifact/example mismatch, then behavior qualification |
| [#44 / R18](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/44) | Command ACP session/permissions/recovery | One manager with #70; consume #57/#61 |
| [#70 / R47](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/70) | release Command captures only after result artifact persistence | After #44 and #65 |
| [#45 / R19](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/45) | Antigravity warm stream/model/one-turn/cumulative usage | Adapter semantics first |
| [#73 / R50](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/73) | OpenCodex positive effect verification | New artifact; no session/execution ownership |
| [#46 / R20](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/46) | Store-owned compact Task prompt | Migrate each adapter only after its behavior branch stabilizes |

## 10. Documentation/donor program

| PR | Scope |
|---|---|
| [#59 / R33/R36/R37](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/59) | 31 verified donors, field failures, reuse boundaries and implementation cards. Docs only; no dependency is automatically approved. |
| [#24](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/24) | Separate local-model/Kilo/vLLM design program on older base. Not part of the audit implementation order; rebase/re-evaluate separately before use. |

## 11. Shared-file locks and rebase rules

```text
swarm-process / process family:
  #63 + #65 + #61 by one serialized manager
  → #27
  → #71/#72/#28/#42/#44 consumers

store/mod.rs and authority:
  #56
  → #53/#54
  → #49/#51/#52
  #50 + #55 by one manager
  → #40 extraction

coordination:
  #32
  → #58
  → #33/#34/#35 narrow consumers
  → #47 correction package

launcher/work-dispatch/resources:
  #60 → #68 → #38 → #74 → #64 → #39

adapter prompt migration:
  #29/#28/#43/#42/#44/#45 behavior first
  → #46 TaskPrompt migration
```

A blocked consumer rebases on the shared owner. It does not create a local duplicate to avoid waiting.

## 12. Когда аудит и подготовка завершены

Я сообщу владельцу **«аудит и подготовка завершены»** только когда одновременно выполнено:

1. Каждый critical имеет source verdict и PR/explicit decision — выполнено на уровне аудита.
2. Каждый HIGH полного приложения повторно traced и owned/refuted — ещё не выполнено.
3. Каждый systemic class имеет одного владельца и test strategy — single-writer/load и часть test-vacuum ещё проверяются.
4. Ни один confirmed material finding не остаётся только в master audit.
5. Нет противоречивых handoff на общих symbols/files.
6. Каждый PR содержит exact symbols, reuse, минимальный type, deletion list, public-path tests и scoped gate.
7. Выполнен финальный drift pass по актуальному `main` и состоянию merged/closed PR.
8. Этот index и master audit соответствуют фактическим открытым PR.

До выполнения восьми условий аудит продолжается. Wave A/B можно реализовывать сейчас.

## 13. Сдача любого implementation PR

```text
base SHA
candidate SHA
producer → persisted fact → consumer
existing helpers/donors reused
old paths deleted
scoped command + actual result
native/full/load checks not executed
```

Docs CI подтверждает только Markdown/diff. Runtime implementation готова только после связанного production caller, scoped warnings-denied Clippy и названных public-path fixtures. Broad/native/account/load qualification — финальная фаза.