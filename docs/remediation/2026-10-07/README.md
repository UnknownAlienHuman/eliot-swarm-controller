# ELIOT — implementation readiness index

**Обновлено 8 октября 2026. База source review: `40591a295af94b1541ec2ba30afe8e3247701a71`.**

Это единая навигация для исполнителей. Она не заменяет body и Markdown выбранного PR: перед кодом открыть **только** свой handoff, текущий diff и названные source symbols. Не читать весь master audit как рабочую инструкцию.

Все ветки направлены в `main`. Один manager — один worktree. Writers получают непересекающиеся файлы, не запускают Cargo и не пушат самостоятельно. Новый DTO/helper считается поставленным только вместе с production caller в том же PR.

## 1. Текущий статус подготовки

- Критический список из 22 пунктов повторно классифицирован: подтверждённые пункты имеют владельца PR; неверные формулировки Forge force-push, script.revise, generic module events и module-ready-without-hello сняты или сужены.
- Production-код большинства блоков **ещё не изменён**. Docs CI не является Rust qualification.
- Code-bearing candidates: compiler baseline #26, Muse #30, R10 #36 и terminal events #37. Их код нельзя объявлять готовым по старому PR body: смотреть exact current diff и CI.
- Реестр остаточных HIGH/MED замечаний всё ещё проверяется. Аудит и подготовка **не объявлены завершёнными**; критерий завершения указан в §8.

## 2. С чего начать агенту

```sh
git status --short
git rev-parse HEAD
git diff --stat origin/main...HEAD
```

Затем:

1. Прочитать body своего PR и один связанный handoff-файл.
2. Сверить названные symbols с текущим `main`; уже исправленное не переписывать.
3. Построить один законченный producer → persisted fact → consumer путь.
4. Подключить существующий donor/helper там, где он указан; не писать общий framework заранее.
5. Удалить старый producer/renderer/state machine после переключения caller — не оставлять `legacy|compat|fallback` production fork без named historical reader.
6. После полного кода manager выполняет только scoped formatting/Clippy из handoff. Broad/native/load qualification — финальная фаза.

## 3. Волна A — база сборки и малые shared primitives

| PR | Владеет | Первый кодовый шаг | Не смешивать |
|---|---|---|---|
| [#26](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/26) | compiler baseline | Проверить exact текущий candidate и реальные diagnostics; один согласованный baseline | Не копировать fixes во все ветки |
| [#63 / R39](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/63) | crash-repairable state marker | `swarm-process::state_marker` → DataRoot caller | Module lifecycle остаётся #27 |
| [#61 / R35](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/61) | finite child, departure, bounded capture | CheckRun typed exit/departure/capture → Store cleanup-pending | Не добавлять PTY/process registry |
| [#65 / R41](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/65) | durable file/journal primitives | torn-final-record scan verdict + atomic durable update | Retention policy остаётся #69 |
| [#56 / R30](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/56) | one GM designation/epoch | typed current designation + Operation-derived epoch high-water | Не создавать новый IAM service |
| [#57 / R31](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/57) | closed RuntimeCommand registry | classify known command/delivery/native-MCP phase; unknown fail closed | Application method registry #40 другой домен |

Эти блоки можно вести параллельно **только** при непересекающихся files. `swarm-process` changes #61/#63/#65 интегрирует один manager последовательно.

## 4. Волна B — core identity, provenance и чтения

| PR | Владеет | Зависит / rebase |
|---|---|---|
| [#53 / R27](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/53) | coalesced task.create/claim/release receipts; release CheckRun wake | До #49/#51 |
| [#54 / R28](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/54) | `task.dispatch` start-slot reuse receipt | Один manager с #46 в `operations.rs` |
| [#31 / R05](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/31) | result provenance, exact expected Attempt | До artifact/task read integration |
| [#48 / R22](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/48) | requirement → exact review/check evidence | После stable CheckRun/result identities |
| [#49 / R23](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/49) | fail-closed Operation reads | Rebase after #53/#54/#56 |
| [#51 / R25](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/51) | Task/Attempt/submission/acceptance graph reads | Reuse #49 relation resolver, no parallel IAM |
| [#52 / R26](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/52) | artifact object grants | Reuse #31/#49/#51 provenance |
| [#50 / R24](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/50) | live MCP allowed-method gate | One manager/rebase with #55 |
| [#55 / R29](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/55) | caller-owned request ID before MCP effect | Same predicate later reused by #40 |

Authoritative relation comes from retained columns/typed records, not handler `result_json`. Scope-specific readers do not invent a generic ACL engine.

## 5. Волна C — coordination, review и frontend

| PR | Владеет | Реализация |
|---|---|---|
| [#32 / R06](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/32) | one work context, registration/fingerprint, code-scope | Existing `ScopeData`; no second context framework |
| [#58 / R32](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/58) | canonical contract proposal + ratify/reject | After #56 and shared context #32 |
| [#34 / R08](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/34) | delivery identity/order, mailbox cursor, watch, subscription cutoff | One digest producer; fail-closed delivery lookup; AUTOINCREMENT sequence |
| [#35 / R09](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/35) | review assignment/replacement/late result + exact link | Correction package is #47 |
| [#47 / R21](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/47) | multi-finding `CorrectionPackageV1` | Rebase after #35; one package → one feedback/send |
| [#33 / R07](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/33) | Concilium terminal/advisory contract | No model-driven consensus/acceptance |
| [#40 / R14](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/40) | data-only frontend/method/schema extraction | После method additions и #50/#55; cache schema bytes, not authority |

File ownership:

```text
store/coordination.rs: #32 context, #58 contract decisions, #34 delivery seams, #35 review seams
store/code_scopes.rs: #32 identity/accept, #39 resource collision consumers
MCP/CLI schemas: domain PR first; extraction #40 last
```

## 6. Волна D — automation, scheduler, resources и local effects

| PR | Владеет | Главное упрощение |
|---|---|---|
| [#60 / R34](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/60) | poison-fact isolation and per-domain transactions | One private `Applied/Pending/Skipped/Quarantined`; no second DLQ |
| [#68 / R45](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/68) | truthful automatic-effect terminal states | One row/state loader; domain evidence remains domain-specific |
| [#38 / R12](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/38) | due-source sequencing, pacing, issuance, DST instant boundary | UTC floor before timezone; no cron engine |
| [#64 / R40](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/64) | typed provider condition + route launch gate | Native evidence → one Store fact → launch/WorkDispatch gate |
| [#39 / R13](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/39) | capacity/resource/workspace fences | Malformed ≠ empty; exact lease/owner; no donor registry |
| [#62 / R38](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/62) | resumable Git-hook install/revoke | One phased manifest; consume #61 finite process |
| [#67 / R44](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/67) | ScriptRun start allow/deny race | One immutable start decision; remove competing files |

Order inside shared automation files:

```text
#60 poison isolation
→ #68 terminal classification
→ #38 scheduler pacing/source integration
→ #64 launch condition
```

Do not run parallel writers in the same large Store file. Rebase one completed vertical slice at a time.

## 7. Волна E — module lifecycle and adapters

### Host/module lifecycle

| PR | Владеет |
|---|---|
| [#27 / R01](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/27) | birth identity, prior owner, worker receipt fan-in, same-boot status, installed/source separation |
| [#37 / R11](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/37) | closed terminal event codec, aliases, secondary codes |
| [#63 / R39](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/63) | marker acquisition only |

`ProcessRunning/Ready` remains gated by exact Store-confirmed `module.hello`; spawn/process presence is not Ready. Do not redesign this proof.

### Codex

| PR | Scope |
|---|---|
| [#29 / R03](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/29) | exact steer without full history preflight; ACK ≠ persisted input |
| [#41 / R15](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/41) | continuous reader + Codex/Muse usage facts |
| [#66 / R43](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/66) | one current Codex goal controller, historical decoder only |

### OpenCode

| PR | Scope |
|---|---|
| [#28 / R02](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/28) | recovery/outbox/IPC/EOF/owner lifetime |
| [#43 / R17](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/43) | queue/loop-step, forms, permissions, background, durable log |
| [#65 / R41](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/65) | shared torn-tail/file seam |
| [#69 / R46](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/69) | acknowledged journal retention |

### Muse / Claude / Command / Antigravity

| PR | Scope |
|---|---|
| [#30 / R04](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/30) | Muse pending-request freshness; existing code candidate requires exact recheck |
| [#42 / R16](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/42) | Claude live callback/defer/reply, auth and permission semantics |
| [#44 / R18](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/44) | Command ACP owned transport/session/permissions/subagents |
| [#70 / R47](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/70) | Command capture release only after result artifact persistence |
| [#45 / R19](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/45) | Antigravity warm stream/model/one-turn/cumulative usage |
| [#46 / R20](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/46) | Store-owned compact Task prompt shared by new artifacts |

Conflict rules:

- #46 migrates adapters only after each adapter branch stabilizes; adapter-local Task renderer is not improved independently.
- #65 file primitives land before #28/#42 journal recovery; #69 retention follows both.
- #44/#70 share Command files and one manager.
- #61 owns finite child/capture; adapters and hook Git consume it, not copy it.

## 8. Когда аудит и подготовка считаются завершёнными

Я сообщу владельцу о завершении только когда одновременно выполнено:

1. **Critical coverage:** каждый пункт критического списка имеет source verdict `confirmed/refuted/conditional`, точного владельца или explicit owner decision. Это сейчас выполнено на уровне аудита.
2. **High coverage:** каждый HIGH из полного приложения повторно traced; он либо включён в существующий PR, либо опровергнут с producer→consumer evidence. Это ещё не закончено.
3. **Systemic classes:** poison, transient/terminal slots, unbounded waits, retention, single-writer/load и method/schema duplication имеют одного владельца и test strategy. Single-writer/load qualification и часть test-vacuum defects ещё требуют handoff.
4. **No orphan finding:** ни один confirmed material finding не остаётся только строкой master audit без PR/decision.
5. **No contradictory handoffs:** совпадающие files/symbols имеют один order/rebase owner; stale instructions удалены, а не дополнены второй policy.
6. **Implementation-ready shape:** каждый PR называет exact symbols, existing helpers/donors, минимальный новый type, deletion list, public-path tests и scoped gate.
7. **Closure pass against current main:** после последнего audit change повторно проверить изменившийся `main`, закрытые/merged PR и source drift.
8. **One current index:** эта карта и master audit отражают фактические открытые PR; obsolete private snapshots/duplicate source plans помечены к удалению после landed code.

До выполнения всех восьми условий аудит продолжается. Независимые Wave A/B задачи уже можно реализовывать — полное завершение аудита не является глобальной блокировкой.

## 9. Проверка и сдача любого PR

Сдача в body/comment:

```text
base SHA
candidate SHA
producer → persisted fact → consumer
удалённые старые paths
scoped command + exit/result
не выполненные native/full/load checks
```

Docs CI подтверждает только Markdown/diff. Rust implementation готова лишь после scoped warnings-denied Clippy указанного handoff. Broad tests/native/account calls — финальная фаза, не writer task.
