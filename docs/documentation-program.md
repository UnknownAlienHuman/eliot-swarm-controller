# ELIOT Swarm Controller — Documentation Program
## Нормативная программа замены ручного управления агентами и использования доноров

**Редакция:** 6 · 2 октября 2026.
**Baseline repository:** `UnknownAlienHuman/eliot-swarm-controller` at `c99a71fbba2de2f663e2709a31e3ccc205621df3`.
**Field evidence:** `eliot-root-scripts-20261002-1030.zip` (SHA-256 `f1279a409597912f8a9b4eeab0ebe1e74b68b760b9d73e26b31988112f3a457e`) and `MANAGER-BRIEF(1).md`, supplied separately and intentionally not committed.
**Status:** Phase A документации уже частично применена в `main`; этот PR теперь является актуальным Phase B implementation handoff. PR меняет только документацию. Предложенный метод, поле или state не считается реализованным до product commit и отдельной qualification evidence.

## 0. Как использовать этот документ

Это единый handoff-документ для следующего прохода по документации. It does not replace the architecture, module contract, implementation plan or module guides. It says exactly which existing documents must change, which current mechanisms must be preserved, which manual-control failure modes must be removed, and which donor units are approved for adoption, pilot or research.

Исполнитель документации должен соблюдать четыре правила:

1. **Read current source before editing documentation.** A capability already present on `main` is documented as implemented, not planned from scratch. A compiled or fixture-checked capability is not called live-qualified.
2. **Preserve authority boundaries.** ELIOT Task/Attempt/Operation, Store, immutable artifacts, CheckRunner and acceptance remain authoritative. No donor adds a second scheduler, task store, session owner or acceptance authority.
3. **Use donors as complete units or explicit patterns.** A whole SDK/backend/crate may be integrated at a named boundary. A few copied methods are a research influence, not an SDK integration.
4. **Every claim carries evidence class.** Use `CODE`, `DOC`, `RELEASE`, `ISSUE`, `USER`, `INFERENCE` or `UNVERIFIED`. Stable tag, current main, proposal and user workaround are not interchangeable.

### 0.1 Лестница доказательств

Документация должна различать следующие границы:

```text
intent persisted
transport attempted
native admission established
native execution started
exact execution terminal observed
result bytes captured and pinned
configured check passed against exact candidate
acceptance committed and still current
```

Ни один слой не может повышать следующий уровень по косвенным признакам: `idle`, process exit, silence, age, successful HTTP status, branch name, a model report or the absence of an item from a partial projection.

### 0.2 Карта текущей реализации на baseline

Проход начинается от уже существующей реализации:

- durable Tasks, Attempts, Operations, request receipts, immutable artifacts, submissions, review and acceptance;
- fixed-source CheckRunner with process-group ownership, active cancellation and conservative recovery;
- built-in OpenCode V2 HTTP adapter with exact configuration readback, controller-recorded goal, durable execution-log correlation, child reads and immutable result export;
- managed Muse SDK bridge with native steering, settings, goal/replies, retained result reads and recorded-session recovery;
- pinned Codex Python SDK plus a shared app-server read-only WebSocket adaptation;
- first-slice Claude, Antigravity, Command Code and Zed modules;
- OpenCodex observer and bridge.2 configuration Operations;
- stdio MCP façade over the existing application API;
- GM designation/handover and host-side reports.

Документация не должна описывать эти части как отсутствующие. Для каждой возможности указывается `implemented`, `fixture_checked`, `live_observed`, `qualified`, `unavailable` or `unknown` per capability.

### 0.3 Phase A уже применена на `main`

После открытия PR основная ветка продвинулась. На baseline этой редакции уже выполнены следующие пункты первоначальной программы:

- удалены committed merge-conflict marker и дублированный Atlas paragraph;
- module contract больше не утверждает, что реализации нет;
- README содержит per-capability implementation/qualification matrix;
- architecture и implementation plan включают R10–R17 / R0–R24;
- runtime notes разделяют research facts и implemented capability;
- CheckRunner документирует effective inputs, evidence publication и process recovery;
- MCP предупреждает, что caller обязан сохранить logical request ID до mutation;
- donor inventory получил adoption classes и stable/pin/qualification distinctions.

Их **не следует реализовывать повторно**. Оставшаяся работа определяется [Implementation Review](documentation-program-implementation-review.md): две P0-противоречия, vendor-neutral prerequisite boundary, составная подготовка, Muse donor conformance, OpenCode background, timeline/family/attention и нормализация donor evidence.

Текущие документы всё ещё должны сохранять distinction:

```text
implemented
fixture_checked
live_observed
role_qualified
unavailable
unknown
```

Source review и compilation не являются live qualification. Исторические доказательства остаются по ссылкам/архивам; действующие нормы — в текущих contracts, а не в растущем журнале.

### 0.4 Архитектурные решения

| Area | Decision |
|---|---|
| Task/work authority | Existing ELIOT Task/Attempt/Operation only |
| Native session owner | Exactly one adapter/backend per physical session binding |
| Submission identity | Task revision + Attempt + submission ref + candidate ref; never branch name alone |
| Verification | Existing fixed-source CheckRunner and independent acceptance |
| MCP | RMCP remains a façade over the application API; MCP Tasks may project Operations, never replace them |
| Muse | Keep the whole official SDK as the primary donor |
| Codex | Keep the whole pinned Python SDK models/router; local shared transport stays a reviewed seam |
| ACP | Pilot whole ACPX only as an optional ACP route |
| Timeline/family | Borrow Paseo projection semantics; retain ELIOT/OpenCode durable evidence rules |
| Messaging | Borrow CCCC identity/generation/reply binding, not its scheduler/store |
| Plugin/update consent | Borrow Agent of Empires/OpenCodex preview/fingerprint/reapproval patterns |
| Verifier semantics | Borrow Claw's `verified/refuted/unverified`, not its file workflow authority |
| Alternative facades | Helicon and Atlas ACP are research alternatives, not additional owners behind ELIOT |

### 0.5 Текущие P0 после сверки кода с документацией

1. **OpenCodex compatibility:** donor inventory объявляет operator-owned current service без mutation-blocking version pin, но `bridge.mjs` всё ещё реализует exact `2.73.0` gate и запрещает preview/configure при mismatch. Нельзя просто снять gate: reviewed upstream `2.75.0` содержит изменения Management API. Нужно разделить observed service version, adapter contract baseline и operation-kind compatibility.
2. **OpenCode goal evidence:** `goal.rs` выставляет `model_work_started=true` по наличию activation input ID. Это доказывает admission, но не `session.execution.started`. Goal result должен разделить record applied, activation admitted и execution started.

Подробный source-level разбор, donor comparison, файлы Phase B и negative acceptance cases находятся в [Implementation Review](documentation-program-implementation-review.md).

## 1. Цель и ограничения

Цель программы: заменить оперативный набор PowerShell/Python/Bash markers одной durable authority, сохранив доказанные полезные свойства и отказавшись от ложных эвристик.

Результат должен позволять одному менеджеру:

- читать одну принятую Task revision и её canonical docs;
- раздать несколько непересекающихся writer assignments;
- видеть точное состояние native executions и questions;
- получить writer outputs без ветки на каждого writer;
- интегрировать один candidate;
- запустить один trusted gate на candidate;
- отправить immutable submission;
- получить независимую review/acceptance;
- восстановиться после host/client loss без blind replay;
- не убивать и не заменять живую native family по возрасту.

Не цель:

- импортировать исторический `state.json` как новую core schema;
- реализовать общий workflow DAG;
- запускать ещё один model ради heartbeat, routing или status comment;
- создавать отдельную branch/worktree для каждого subagent;
- переписывать Muse SDK/Codex SDK/ACPX/Paseo state machines по сниппетам;
- считать prompt rules OS isolation;
- автоматически чистить пользовательские native stores, credentials или Downloads;
- добавлять криптографию, fences и receipts без конкретной границы отказа.

## 1A. Реестр использования доноров

| Donor | Version/source reviewed | Решение | Что берём | Что не берём |
|---|---|---|---|---|
| Muse Code SDK | `a7c10c5...`, package `1.3.0` | ADOPTED primary | Complete SDK/types/router, command identity/replay contract, pending questions, gap/host-death semantics | Model loop, global install, invented protocol |
| Codex Python SDK | `18194bfd...` | ADOPTED source closure | Generated models, router, goal/turn helpers; reviewed shared-transport seam | Default auto-approval, owned stdio lifecycle for shared route, private API claims without fixtures |
| RMCP | `3.5.0` exact release | ADOPTED | Official Rust MCP types/transports/tasks/subscriptions | Second Task manager, stale cache for authoritative reads |
| Atlas Redact | vendored exact snapshot | ADOPTED | Complete bounded redaction crate and notices | Atlas task/session stores |
| ACPX | stable `0.19.4` reviewed | PILOT optional ACP backend | Complete shared runtime/owner/queue/watch/cancel package | Mandatory bridge for Muse/OpenCode, flow engine, Task authority |
| Paseo | stable `0.10.3` | PATTERN | Authoritative timeline projection, source ranges, live/canonical split, child declaration identities | Whole daemon/UI, trusted plugins, item-count-only paging |
| CCCC | stable `0.4.41` | PATTERN + narrow crate candidate | Delivery identity/generation, reply/cancel binding; conditional `cccc-windows-process` | Scheduler, UI, whole ledger per client, observer-loss stop policy |
| Agent of Empires | stable `1.18.0` | PATTERN | Manifest/grants/update fingerprint, reapproval, active+reserved quotas | Plugin host as sandbox, tmux authority, in-memory reservation as durability |
| Claw | stable `7.6.1` | PATTERN | `blocked/superseded`, caller-owned verifier contract, protected tests, `unverified` | Second durable store/workflow engine, at-least-once external effects |
| OpenCodex | current release `2.75.0`; ELIOT bridge currently pinned earlier | EXTERNAL optional service | Preview/fingerprint/readback, partial outcomes, provider affinity | Provider translation in Rust core, hidden fallback, proxy health as Task health |
| Waku | stable `0.1.20` | PATTERN WITH EXCLUSIONS | Sequential session worker, bounded replay/subscriber pattern | GPL core, same-ID fresh fallback, ignored settings failures, duplicate persistence authority |
| Helicon | reviewed `80f3351e...` | RESEARCH alternative Muse facade | Official SDK usage, command/query separation, workspace/session UX | Additional owner behind ELIOT, title discovery by model, UI/store |
| Atlas ACP | reviewed `a34a6d44...` | RESEARCH alternative ACP product stack | Identity mapping, resume-pending gate, mode before first send | Broad manager/store/transcript/native-agent closure behind ELIOT |

### 1A.1 Наиболее эффективные эталонные реализации

1. **Muse SDK command semantics:** pending intent is recorded before send; safe replay retains the same command ID and identical params; transport failure does not become rejection; host durability and view gaps are explicit.
2. **Codex native steer:** `threadId + expectedTurnId`; stale target is rejected by native server. No local UUID comparison is called equivalent.
3. **Paseo timeline:** live stream for immediacy, canonical page fetch for correctness; source ranges and epoch/gap state retained. Add post-projection byte bounds before adopting.
4. **ACPX owner path:** controls go through the one shared owner; no owner means no second direct connection; queue admission and prompt start are separate facts.
5. **OpenCodex mutation management:** preview → fingerprint → explicit confirm → replan → one mutation → readback; partial 207 is parsed per element.
6. **ELIOT CheckRunner:** exact captured Git source, trusted profile, process group, immutable outputs and incomplete-on-uncertainty are already stronger than donor terminal self-reports.

## 2. Как обновить существующие документы

### `README.md`

Replace outdated “next code” prose with a compact truth table and link to this program. Do not copy R0–R24 into README.

Required matrix columns:

```text
surface
implementation unit
code status
fixture/CI evidence
live runtime evidence
known gaps
next implementation item
```

Separate rows for:

- host/task/artifact/check/acceptance;
- MCP façade;
- OpenCode HTTP;
- Muse bridge;
- Codex read-only shared attach;
- Claude first slice;
- OpenCodex provider observer/configuration;
- Command/Antigravity/Zed batch/profile units.

### `docs/agent_swarm.module-contract-v2.md`

- Replace historical non-implementation header.
- Preserve RuntimePort/vendor-neutral rule; label current OpenCode Store import as a transitional exception, not a precedent.
- Add typed delivery boundaries, steer classes, native attention/background, result/terminal evidence and module provenance.
- Keep `native.*` registered and typed; no arbitrary JSON escape.

### `docs/agent_swarm.implementation-v6.md`

- Convert from preimplementation C01–C11 checklist into postimplementation roadmap.
- Mark implemented slices by exact paths; do not mark them live-qualified.
- Add R10–R24 as remaining causal work.
- Each future implementation Issue should link to exact functions/files and negative acceptance scenarios from this document.

### `docs/runtime-notes.md`

- Keep time-stamped research facts immutable.
- Add a current implementation-status overlay, not edits pretending historical research was qualification.
- Document typed steer class per adapter, session ownership topology, recovery and source completeness.

### `docs/check-runner.md`

- Preserve current CheckRunner implementation.
- Add effective build input identity, resource lease, baseline/cache/reverse-dependency contract, protected acceptance inputs and evidence-before-pass rule.
- Clearly label unimplemented cache/scope behavior as pending.

### `docs/agent_swarm.donors-20260929.toml`

Add per donor:

```toml
stable_tag = ""
stable_commit = ""
reviewed_commit = ""
source_checked_at = ""
reuse_form = "whole_package | complete_crate | protocol_pattern | research_only"
installed = false
fixture_checked = false
live_smoke = false
role_qualified = false
license_compatibility = ""
authority_overlap = []
known_failure_modes = []
```

A release/tag should never silently rewrite historical evidence; add a new row or explicit current review fields.

### Module guides

Every module guide must answer:

1. Who owns the native process/service?
2. What exact source/package/runtime versions were reviewed, installed and observed?
3. Which Operation boundary returns `Applied`?
4. Which errors prove rejection and which leave unknown?
5. What can be replayed, under what exact ID?
6. What history/family range is complete?
7. What settings are requested/applied/inference-observed?
8. What shutdown/cleanup is permitted?

## 3. R0–R9: что сохраняется из v3

These items remain required but must be updated against the current baseline:

- **R0:** document truth/status cleanup, not product code.
- **R1:** caller-owned logical request ID for MCP mutations.
- **R2:** OpenCode goal record/admission/execution boundaries.
- **R3:** extract runtime-specific setup interpretation from Store.
- **R4:** Codex shared transport seam and negative server-request responses.
- **R5:** authoritative family/history projections.
- **R6:** bounded projected pages and detached large bodies.
- **R7:** process ownership and lifecycle postconditions.
- **R8:** optional ACPX backend, whole package.
- **R9:** module/source/artifact/grant provenance.

The implementation agent must read current source first: some portions landed after the earlier analysis, while the remaining gaps are narrower than the original headings.

## 4. R10. Единый рабочий контракт Issue и роли

### Проблема

Legacy control uses multiple mutable projections as if each were authority:

- Issue body/comments;
- generated TASK.md;
- BATCH/REMAINING;
- CHECKLIST current/previous;
- CLAIM;
- STATUS;
- PUSHED variants;
- branch and lane names.

The Manager Brief's §0 is current and explicitly outranks historical sections, but the historical log contains later rule changes that were operationally applied. ELIOT must not infer the winning policy by mtime, date text or model interpretation.

### Target contract

```text
Task revision
  canonical source references
  exact requirements
  accepted workflow_policy_revision
  owner/role policy
  required checks
  allowed scope
  source/comment provenance
```

A generated TASK/brief is a bounded read projection. It cannot add process rules or silently discard a new normative comment as “progress noise”.

### Roles

- **Manager:** reads canonical Task revision and docs, selects disjoint writer assignments, integrates output, owns candidate publication.
- **Writer:** receives one assignment, exact baseline and allowed paths; writes code or performs one requested investigation. No automatic cargo/process/service authority.
- **Reviewer:** gets immutable candidate and requirement map, read-only by default.
- **CheckRunner:** executes trusted profile against captured candidate.
- **GM/operator:** revises policy/Task and accepts/invalidate acceptance. Does not become a hidden product coder.

These are application permissions. They do not claim OS isolation while native shell remains unrestricted.

### One Issue / one publication unit

One Task revision maps to one final submission. Internal writers may deliver partial outputs to the manager. They do not produce independent product submissions merely because a script needs a marker.

An incomplete writer result may be retained; retention is not merge authorization. A partial Task submission requires an explicit accepted policy and exact outstanding requirements, not a free-text marker.

### Documentation changes

Add to architecture and module contract:

> The accepted Task revision and workflow policy are separate immutable identities. A projection may abbreviate them but cannot become authority. One Task has one current Attempt owner and one current submission chain. Internal writer assignments are subordinate outputs, not branches or submissions by default.

### Acceptance

- New normative comment after Attempt start creates a new revision/replan condition; it is not silently appended to a live writer prompt.
- Historical progress comments do not fill every future model context.
- A branch called `lane/W2` cannot identify a submission.
- Reviewer feedback names exact Attempt/submission/candidate.
- Same candidate and same logical request return the same receipt.

## 5. R11. Доставка результата и повторно используемая lane branch

### Confirmed legacy failure modes

- Late HOLD for old branch removes a newer PUSHED marker because branch name is reused.
- Mutable CHECKLIST beside another Issue can be read as evidence for the wrong submission.
- Multiple PUSHED-* queues were invisible to the acceptance daemon.
- Archived Git heads preserved bytes but did not prove delivery/integration.
- Untracked source could be omitted by `git add -u` and then force-deleted.

### Existing ELIOT foundation

Use current:

- immutable candidate artifacts;
- `task.submit` with expected submission ref;
- exact Attempt/revision ownership;
- addressed `task.request_changes`;
- CheckRunner tied to candidate;
- acceptance tied to submission/candidate/checks.

Do not add a new file protocol around them.

### Writer output contract

A writer result includes:

```json
{
  "assignment_id": "...",
  "attempt_id": "...",
  "task_revision": 1,
  "baseline_candidate": "...",
  "changed_files": ["..."],
  "output_ref": "artifact-or-patch",
  "summary": "...",
  "status": "complete | incomplete | blocked_by_task | rejected",
  "known_gaps": []
}
```

Manager records one disposition:

```text
integrated into candidate
rejected with reason
retained incomplete
superseded by exact output
```

Branch/worktree is an implementation detail of a chosen route, not writer identity.

### Submission snapshot

The immutable submission document already contains candidate digest, claims and previous submission ref. Extend only if needed with writer-output provenance references; do not copy every transcript.

### Acceptance

- Old feedback cannot mutate a later submission.
- Two submissions from same branch remain different.
- New untracked file is inside candidate bytes or explicit retained output before cleanup.
- An archive ref without integration stays visible as undelivered.
- Candidate verification runs on candidate bytes, not current branch head.

## 6. R12. Scoped observations, capacity и адресное внимание менеджера

### Confirmed legacy failure modes

- One SQLite cursor reused inside its own enumeration skipped managers.
- JSON errors were detected by exact whitespace-sensitive substring.
- One corrupt model-written timestamp could break every queue.
- Subagent counts for shared Codex server mixed several managers.
- “Always four writers” conflicted with frozen submission/review and native limits.
- Question forms were auto-answered by label/first choice, without authority.

### Observation schema

```json
{
  "source": "native | controller | derived",
  "scope": {
    "binding_id": "...",
    "generation": 1,
    "session_id": "...",
    "turn_id": "...",
    "assignment_id": "..."
  },
  "observed_at_ms": 0,
  "freshness": "fresh | stale | unknown",
  "coverage": "complete | partial | unknown",
  "gaps": [],
  "raw_state": {},
  "normalized_state": "...",
  "normalization_loss": []
}
```

No read-only report may mutate the observed system.

### Capacity

Capacity is not one hard-coded number. Account separately:

```text
configured maximum
active native work
pending admissions/reservations
unknown-disposition work
review/check capacity
quota/cooldown scope
```

A role profile may define desired parallelism, but controller does not create filler model work solely to reach it. Frozen candidate/review, quota, attention request and resource conflict are valid reasons for lower active writers.

Use Agent of Empires' active+reserved insight, but reservations must be durable/scoped; the donor's in-memory counter is not copied as crash authority.

### Attention

Typed attention categories:

```text
native approval
native form/user input
manager assignment empty
writer result ready
writer silent with observed progress/no progress
foreground tool blocks manager loop
configuration prerequisite pending
check result
stale/partial observation
quota/cooldown
```

An attention record identifies current native request/body fingerprint and authorized responder. Recommended option is display metadata, not automatic consent.

### Background foreground wait

OpenCode `/background` is a separate native control: it may release a manager blocked on a backgroundable tool without killing child work. Add only after exact installed-schema qualification. It is not steer, interrupt, reply or terminal.

### Acceptance

- One malformed event affects one observation, not the global queue.
- Shared-server event is attributed by native parent/turn identity.
- Missing source is unknown, not zero.
- A current form cannot be answered using an old request ID/fingerprint.
- Capacity does not ignore admitted-but-not-started sessions.
- No native session is killed because an observer declared it old.

## 7. R13. Native delivery, recovery и drain без новых дублей

**Проблемы:** десятикратные POST; идентификация по первой строке; spool retry меняет target; clear/interrupt всех same-cwd; завершающийся wrapper не означает конец native goal/family.

**Куда:** module contract open/send/shutdown/replay, README native modules, existing R1/R2/R4/R5. Будущий код: существующие runtime effects/reconcile и adapters.

**Предлагаемый нормативный текст:**

> Intent и target сохраняются до первого native write. Adapter различает доказанный pre-dispatch failure и неизвестный исход после отправки. Unknown create/prompt/control восстанавливается предусмотренным native readback либо остаётся адресным incident. Generic retry не повторяет мутацию только потому, что caller не получил ответ.
>
> Наличие input ID означает заявленную native boundary, не автоматически started execution. Expected turn защищает адрес, но не заменяет request idempotency. Старый spool intent не переносится в новую unrelated generation/Attempt. Отсутствие известной session не разрешает создать другую под прежним ID.
>
> Draining запрещает новый workload, но позволяет получить результаты детей, ответить на текущие вопросы и довести незавершённые операции. Parent idle и client exit не освобождают native goal/детей автоматически. Управление native continuation выполняется только явно поддержанным методом и с полномочиями на точную session; прекращение цели, interrupt, detach и release — разные действия.

**Shared service:** read-only helper не запускает и не перезапускает native server. Возможный service supervisor — одна отдельно выбранная owned-service роль; по умолчанию связь отсутствует → incident, не захват чужого process. Probe failure остаётся unknown.

**Доноры:** OpenCode native API вместо CLI; полный native SDK в существующих ELIOT bridges; ACPX same-session-only/owner-only для optional ACP-route. User-installed Codex proxy transport не считается реализованным TCP WebSocket attach по сходству названия.

**Критерии:** lost create response не создаёт второй root; lost prompt ACK не создаёт второй input; первой строкой может быть error, identity всё равно берётся из retained record; old reminder не попадает в новый turn без explicit policy; same-cwd foreign thread не прерывается; service restart не разрешает silent session replacement; неизвестная activation не становится model_work_started.

## 8. R14. Проверки, baseline, cache и build resources

**Проблемы:** actual HEAD ≠ cache label, formatter-title exception, grep вместо exit, красный unrelated package держит все закрытия, burst cargo и wipe/rebuild.

**Куда:** CheckRunner и implementation; будущая доработка existing checks profiles/resources, а не новый review daemon.

**Предлагаемый нормативный текст:**

> Проверочный результат относится к exact candidate и фактически исполненному trusted profile. Перед началом закрепляются source identity и объявленные effective входы: toolchain, target/features, profile/config и допустимое окружение. Изменение входов инвалидирует reuse по контракту, а не только по имени cache directory.
>
> Exit status, полнота наблюдения и ожидаемый scope проверяются независимо от извлечённых diagnostic строк. Неудачный spawn/read/parser/timeout не превращается в успешный пустой результат. Human review и текстовый отчёт писателя не заменяют machine CheckRun.
>
> Build target получает exclusive use на время работы. Активный или unknown-disposition worker удерживает этот ресурс. Писатели не запускают отдельные cargo workers в общей папке. Проверки одного итогового candidate coalesce; повтор неизменных входов без нового основания не выполняется по таймеру бесконечно.

**Scope:** учитывать reverse dependency closure там, где она определена; общий config/lock/build input требует расширения. Scope не выводится только из crate имени, file extension или substring. Доказательство «путь есть в source» не заменяет production reachability, а graph miss не доказывает отсутствие.

**Acceptance:** подходящий gate выбран policy проекта. В предоставленном позднем workflow manager выполняет Clippy, OR/root — тесты; это profile selection, не случайный текст в generated TASK. Полный run не навязывается каждой сдаче. Имеющийся baseline failure сохраняется с причиной и source; он не выдаётся за regression и не оправдывает новый дефект изменённой гарантии.

**Критерии:** candidate changed after fmt → новый проверочный результат; failed clippy с нераспознанным stderr → incomplete/failure, не clean; одинаковый SHA с изменённой effective profile не использует старый receipt; два задания не пишут в один target одновременно; timeout дочернего server не освобождает resource до известного disposition; acceptance смотрит current anchored decision, а не свежий mtime JSON.

## 9. R15. Writer result и безопасный lifecycle ресурсов

**Проблемы:** untracked исходники потеряны из preservation, shallow mtime, stale branch markers, deleted live native logs, archive с недоставленной работой, cold-target churn.

**Куда:** architecture ownership/resources, implementation cleanup, CheckRunner retention. Будущий код: расширение существующих resources/artifacts/Attempt, platform filesystem boundary; **никакого универсального очистителя пользовательского профиля**.

**Предлагаемый нормативный текст:**

> До удаления ресурса controller устанавливает принадлежность, отсутствие активного использования и disposition результата. Возраст и отсутствие записи в одном observer не являются доказательством освобождения. Отказ inventory/readback оставляет конкретный ресурс unresolved. Ошибка безопасного удаления не разрешает fallback с более широкими разрушительными полномочиями.
>
> Preservation включает все требуемые source bytes, в том числе новые untracked файлы и незавершённые tracked изменения. Сохранённый HEAD не равен сохранённому worktree. Неуспех commit/artifact publication запрещает cleanup. Не нужно без разбора коммитить build outputs/secrets ради резервной копии; такой конфликт оставляет worktree на месте.
>
> Writer output имеет исход в пределах Attempt: integrated, rejected with reason или retained incomplete. Archive retention не означает доставку, merge или acceptance. При одном manager worktree output может быть набором изменённых файлов/снимком, без writer branch/commit.

**Кэш:** разделить controller source/evidence, scratch, warm build targets и external native store. Освобождение idle cache допускается по принятому budget/retention с учётом следующей ожидаемой работы, не исключительно по mtime. Cleanup не меняет глобальный Cargo debug/wrapper config и не уничтожает warmed targets всех slots одновременно только ради большого отчётного числа GB.

**Native history:** `codex_db_trim.py`, age-based deletion чужих rollout и любые writes в OpenCode DB не переносятся в обычный adapter. Отдельное согласованное обслуживание native stores требует native-supported contract/версии и своей ответственности. Routine maintenance не расширяет scope на Downloads, credentials, весь `%TEMP%` или процессы приложений пользователя.

**Критерии:** untracked `new.rs` не теряется; failed preservation оставляет байты; deep edit/old log mtime не ведёт к удалению живого ресурса; stale marker старой lane generation не удаляет новую; новое pending submission автоматически удерживает source; archive сохранён, но unintegrated output виден менеджеру; unknown process enumeration не становится empty pool.

**Windows:** existing owner-first Job остаётся. CCCC suspended-child helper рассматривается только для иной нужной topology. WMI-created children не объявляются автоматически членами Job. Прикладное trusted scope не называется sandbox.

## 10. R16. Host scheduling, quota state и полезные отчёты

**Проблемы:** Report30 зависит от root, несколько guardians на slot, active=false смешан с quota, shared JSON перезаписывается, read-only report меняет markers.

**Куда:** architecture host responsibilities, report/Doctor и implementation. Будущий код: существующий host loop/Store; не внешний брокер и не новый agent на каждый status event.

**Предлагаемый нормативный текст:**

> Desired configuration и runtime availability хранятся раздельно. Локальный error не меняет owner-selected model, limits, safety, schedule или enabled policy. Ограничение quota/cooldown относится к scope native evidence: session, provider, account или shared service. Неизвестный scope не дублируется как независимый бюджет каждой lane.
>
> Периодическая работа имеет одного host owner, сохранённый due/outcome и явную политику пропуска/задержки. Смена GM и закрытие его клиента не теряют расписание и committed операции. Retry выполняется по виду ошибки и evidence; structural/identity error не запускает loop с неизменными входами.
>
> Чтение отчёта не запускает models, не переписывает очередь, не освобождает claim и не удаляет файл. Отсутствующая observation показывается unknown со source/freshness, а не нулём или DOWN. При неизменном incident отображение может обновиться, но новый модельный запрос не обязателен.

**Применить без смены пользовательских cadence:** maintenance остаётся на принятой пятичасовой сетке; reports/attention показывают next due и last failure. Пропущенные slots не должны неожиданно исполняться пачкой, если contract выбрал skip. Запуск отсутствующего external service и любое убийство семей не входят в read-only monitoring.

**Метрики:** finished valid requirements/Issues, pending addressed submissions, undelivered/retained writer outputs, actionable blockers, review/check reuse, waiting cause, cold rebuild cost, reliable per-native usage attribution. Не выдавать PR/hour и число созданных агентов за throughput продукта. Исправление истории не подменяет прошлый source snapshot.

**Критерии:** restart GM не останавливает host reports; два observers не поднимают два guardians; invalid model-written date не ломает все queues; shared quota событие не гасит чужой account; 429 с неизвестным subtype не объявляется автоматически hourly rate limit; status text с цитатой ошибки не считается native failure; read report не меняет persistent state.

## 11. R17. Миграция без двойной authority

**Этап 1 — read-only импорт и сверка.** Инвентаризировать архивные records с source provenance. Missing/malformed marker сохраняется как проблема импорта. Имя branch, log timestamp и STATUS text — hints, не доказательство current owner. Получить map Issue → current accepted Task revision → Attempt → native binding и отдельно archived outputs. Не импортировать credentials, private native DB internals или исторические команды остановки как права ELIOT.

**Этап 2 — одна lane на существующем native service.** Зафиксировать, какой старый runner больше не допускает новую работу; подтвердить disposition его текущей семьи. ELIOT может сначала только наблюдать её, не открывая второго control binding. Transfer ownership не заменяется созданием нового root с тем же названием. Нельзя simultaneously оставлять legacy reminder/launcher и ELIOT отправляющими команды одной семье.

**Этап 3 — одна полная Issue через existing API.** Сохранённая revision/requirements, новые writers по части задачи, итоговый candidate, один scoped manager gate, независимая требуемая проверка и адресный результат. Повтор client request безопасно получает retained receipt. Нельзя для демонстрации успеха пропустить вопрос, partial output или negative case.

**Этап 4 — обратимый переход наблюдения и ресурсов.** Legacy scripts перевести в read-only exporter либо отключить их mutating roles; состояние и forensic source не удалять до проверки переноса. Cleanup только controller-owned candidates после сохранения. Изменение living native settings не входит в этот переход автоматически.

**Этап 5 — следующие lanes по тем же invariants.** Только после доказанных boundary предыдущей. Rollback не запускает двух владельцев: сначала прекращение новых admissions, затем наблюдение/передача уже принятых операций. Ни force-kill живого native family, ни удаление неизвестного файла не являются fallback.

### Что должно исчезнуть из рабочего пути

| Legacy связка | Целевая замена |
|---|---|
| Mutable PUSHED/NOCHANGE/HOLD + state.json files | Existing task.submit / expected_submission_ref / addressed review + read-only export при необходимости |
| Plain text steer inbox | Existing message + stable Operation target; native-specific delivery semantics |
| `oc_run.py` POST retry и `oc_busy.py` age release | Existing native effects/readback + scoped family/continuation observations |
| `Remind-Subagents.py` самостоятельные SQL/роутинг/POST | Read-only report + attention policy поверх единой observation |
| Manual Codex/Muse transport lifecycle | Existing SDK modules, qualified shared transport и module owner |
| Review shell parser/checklist grep | Existing CheckRunner + immutable submission + review API |
| `archive-wip` как доставка | Retained writer output/artifact with manager disposition |
| Multiple acceptance daemons and mutable claims | Host scheduler/resource lease/Operation |

## 12. Причинный порядок реализации после принятия документации

| Порядок | Работа | Почему сейчас |
|---:|---|---|
| 1 | R0 document truth and owner-policy decision | Иначе агент будет реализовывать устаревший план и противоречивые process rules |
| 2 | R1 MCP logical request identity | Machine callers need safe first mutation before broader control |
| 3 | R2 OpenCode goal evidence split | Current field name can overstate execution |
| 4 | R3 typed adapter condition evidence | Required before adding more runtime prerequisites |
| 5 | R4 Codex SDK seam hardening | Existing bridge depends on private touch-points and default donor approval is unsafe |
| 6 | R5/R6 family + bounded projection | Needed for native managers and reports without script heuristics |
| 7 | R10/R11 Task policy and writer-output delivery | Replaces mutable branch/checklist authority |
| 8 | R12 attention/capacity/background | Depends on trustworthy identities/observations |
| 9 | R13 recovery/drain | Consolidates per-adapter rules after evidence contracts |
| 10 | R14 CheckRunner scope/cache/baseline | Existing runner works; extend without delaying basic migration |
| 11 | R15 resources/cleanup | Cleanup consumes authoritative ownership/output state |
| 12 | R16 host scheduling/reports | Replace root loops after core observations exist |
| 13 | R17 one-lane migration | Never run two control authorities over one family |
| 14 | R18/R19 donor conformance | Lock SDK-specific semantics before upgrades |
| 15 | R20 MCP Tasks/subscriptions | Projection over stable application state, not foundation |
| 16 | R21 ACPX pilot | Optional runtime after core boundaries are proven |
| 17 | R22/R23 timeline/messaging/supply chain | Generalization and module operations |

## 13. Приёмочные сценарии: короткие, по реальным границам

### Identity and submission

1. Old HOLD references same branch name but old submission; current submission remains untouched.
2. Candidate from another Attempt/revision is rejected.
3. Same logical request + same payload returns retained result; different payload conflicts.
4. A writer output retained in archive but never integrated remains visible and cannot satisfy submission.

### Delivery/recovery

5. Native accepts input and reply is lost; reconciliation never creates a fresh input ID.
6. Pre-dispatch connection failure may reject safely; post-write timeout remains unknown.
7. Expected-turn steer loses race; native rejects stale target; controller does not convert to next-turn input.
8. Host restarts while native family works; no replacement owner starts.
9. Native resume returns another session ID; recovery stops with identity incident.

### Observation/attention

10. One malformed manager row does not suppress the rest of the report.
11. Parent idle with active child stays family-active/partial.
12. Pending form changes before reply; stale fingerprint is rejected.
13. Manager waiting on a backgroundable foreground tool may receive a scoped background request; child remains live.
14. Missing event range returns gap and authoritative refetch requirement.

### Verification/resources

15. Candidate changes after check; previous pass does not apply.
16. Test script/profile changed from protected baseline; acceptance blocks or requires trusted policy change.
17. Worker dies before identity; known launch instance departed → incomplete, no replay.
18. Timed-out process keeps children; resource remains held.
19. Evidence artifact publication fails; verdict is not passed.
20. Unknown process/file ownership causes no deletion.

### Supply chain

21. Module update widens capability; old grant is not reused.
22. Source hash unchanged but built artifact changed; activation requires new artifact digest.
23. Stale preview fingerprint; no mutation, fresh plan returned.
24. Partial 207 response; each element recorded, no aggregate success.

## 14. Готовые короткие вставки в контракты

### Submission identity

> A branch, lane, session or mutable marker is not submission identity. Submission identity is Task revision + Attempt + submission reference + candidate digest. Review, hold, acceptance and invalidation address that identity exactly.

### Native delivery

> An adapter persists the native command identity and exact payload before its first write. A lost response never authorizes a fresh command identity. Admission, execution start and terminal outcome are separate evidence.

### Observation

> Reports are projections of recorded observations. Missing or stale observations are shown as unknown with source and coverage; a read-only report does not mutate, restart, reply, release or clean up.

### Native attention

> A pending approval or input request is identified by native scope, session, request ID and current body fingerprint. A recommendation is presentation metadata, not authority to answer.

### Cleanup

> Cleanup operates only on controller-owned resources after result preservation and proven non-use. Age, silence, mtime and an empty partial inventory are insufficient. Failed preservation or inspection retains the resource.

### Model work

> Coordination events do not implicitly invoke a model. Every model turn is created by a typed work intent and belongs to a Task/Attempt/binding. Notifications, comments and handoff records may request a turn but are not turns themselves.

## 15. R18. Семантика Muse command/replay/gap

### Use the donor, do not restate it

The complete official SDK is already the donor. Module documentation and bridge code should delegate rather than implement parallel semantics for:

- command ID minting and replay;
- pending command lifecycle;
- server-authored admission/rejection;
- cursor gap fill;
- durable vs ephemeral host death;
- pending approval/user input request shape.

Where controller durability requires data outside SDK memory, persist SDK/native identity and exact command payload, but drive replay through the SDK under the same command ID.

### Required contract

```text
submitted locally
acked queued/started
materialized by userMessage/activeTurn
rejected by durable commandRejected
reclaimed/abandoned
terminalUnknown for ephemeral host death
```

Transport/protocol error is not durable rejection.

### Gaps

Cursors are opaque. Gap fill pages to target while buffering live frames, discards overlap by exact cursor identity and then splices. Failure drains genuinely received live events and reports a gap; it does not silently proclaim a complete transcript.

ELIOT may retain a compact snapshot, not a second full transcript. `view/subscribe` is observation, not session load or writer lease.

### Host death

Respect handshake durability profile. Unknown future profile gives no durability guarantee. Durable abnormal death means resume is required; ephemeral abnormal death discharges local pending items as unknown and prohibits replay against a new host.

### Acceptance cases

- same command ID with identical params: safe replay path;
- same command ID/different params: rejected before write;
- only durable `commandRejected` restores input as rejected;
- transport EOF on durable session: waits for explicit resume, does not fabricate terminal;
- ephemeral host death: pending command becomes terminalUnknown, never replayed at replacement host;
- coalesced gaps and late duplicate live twin are folded once.

## 16. R19. Граница Codex SDK и shared app-server

### Existing good implementation

Keep pinned generated models/router and local WebSocket library. Shared client must never use donor default approval handler. Read-only bridge hard-refuses writes before transport and returns negative responses to command/file approvals.

### Private seam risk

Current subclass overrides donor private/semiprivate methods. For every pin update:

1. Compare those exact methods against previous pin.
2. Run an upstream-shape fixture for request, response, notification and server request.
3. Verify one reader only.
4. Verify transport closure fails all waiters.
5. Verify no process spawn/close of shared server.
6. Verify read-only allowlist before socket send.

Prefer an upstream public transport interface when available. Do not fork generated protocol types or copy the router into Rust.

### Exact steer class

Codex `turn/steer(threadId, expectedTurnId, input)` is `native_expected_target`. An app-server version and capability probe must establish that installed server rejects stale target. Local cached-turn guard alone is a weaker class.

### Retry

Donor `retry_on_overload` is safe only for explicitly classified overload requests whose operation itself is idempotent or native request identity is retained. Do not wrap turn/start, goal mutation or other external effects in generic retry solely because error is called busy.

### Goal

Goal may begin native execution immediately after mutation. Therefore all required model/cwd/sandbox/approval/task context must be bound before `thread/goal/set`. Clearing/stopping every active same-cwd thread is not an ownership rule.

### Acceptance

- shared app-server already exists; bridge attaches and never stops it;
- read-only `thread/read` does not resume;
- server approval request is declined;
- stale expected turn is rejected;
- unknown server request returns explicit method-not-supported, not `{}` success;
- server loss terminates client waiters and leaves admitted operations unknown/addressable;
- another thread with same cwd is not interrupted.

## 17. R20. MCP-фасад, Tasks и subscriptions

### 17.1 Проекция Operations как MCP Tasks

RMCP 3.5.0 supports MCP Tasks, but ELIOT already has durable Operations. If exposed:

```text
MCP task ID = projection/handle of ELIOT Operation
MCP tasks/get = operation.get/report projection
MCP tasks/cancel = addressed cancellation request when application method supports it
MCP tasks/update input request = existing addressed reply path
```

Do not instantiate an independent TaskManager holding product truth. MCP task state cannot accept a Task or release a native producer.

### 17.2 Идентичность запроса

Mutating tools require caller-provided stable `client_request_id` for retries. Generated convenience ID is correlation-only and must be returned even on tool-level errors when a response is delivered. If the transport response is lost, only the caller's pre-saved ID makes retry safe.

### 17.3 Подписки

Subscriptions are projections of reports/operations/mailbox, with:

- bounded per-client buffer;
- sequence/observation identity;
- explicit lagged termination;
- client resync through ordinary read API;
- drop does not cancel work.

No claim of reconnect resume if RMCP transport does not provide it. A new listen starts a new projection; authoritative state is read from Store.

### 17.4 Кэширование

RMCP stale-on-error default is unacceptable for authoritative Task/Operation/permission reads. Disable or partition/cache only static discovery/schema/catalog outputs where stale response is explicitly allowed and labelled.

### 17.5 Сопровождение схем

MCP long-running tasks, subscriptions, MRTR and caching are version-negotiated. Documentation lists exact negotiated protocol/version and fallback, not only crate version. `requestState` in MRTR is untrusted and must be HMAC-sealed or an opaque server handle if used.

### Acceptance

- dropped subscriber does not stop Operation;
- lag is reported, followed by read resync;
- cancelled MCP request does not automatically kill native family;
- stale cache never hides permission/form change;
- MCP task state cannot contradict Operation;
- request ID survives a delivered error response.

## 18. R21. Решение по ACP-route

### Decision

Pilot the complete ACPX shared runtime/owner package as one optional adapter backend. Do not copy queue methods into Rust and do not force ACPX between ELIOT and Muse MSP/OpenCode HTTP.

### Authority split

```text
ELIOT:
  Task/Attempt/Operation
  binding/generation
  logical idempotency
  artifacts/results
  family projections
  checks/acceptance

ACPX:
  exact ACP process/session owner
  queue ordering
  native connection retention
  targeted cancel/control
  watch stream
  native ACP protocol lifecycle
```

### Wrapper requirements

- exact scope `(agent command, cwd, name/account/profile)` retained in binding;
- one ACPX owner per scope;
- no owner → no direct control bypass;
- queue accepted vs prompt started vs completed remain separate;
- client disconnect does not cancel native work;
- uncertain prompt never retried with a new identity;
- old owner compatibility explicitly checked;
- response/body byte bounds and detached artifacts added outside ACPX response assembly;
- ELIOT Operation ID maps to a stable ACP request/turn identity.

### Qualification

- Windows exact commands/wrappers;
- provider session ID reuse;
- resume/load semantics for each agent;
- question/permission callbacks;
- cancellation queued vs active;
- owner crash before/after prompt start;
- large response;
- watch disconnect;
- model/mode controls and readback;
- child coverage.

## 19. R22. Контракты timeline, family и коммуникации

### Timeline

Borrow Paseo's semantic split:

```text
live notification stream = low latency, non-authoritative unless replay guaranteed
canonical paged native/store history = correctness
projected rows = presentation with source ranges
optimistic submissions = separate local/controller intent registry
```

Every page/projection reports:

```text
epoch/generation where native contract provides one
cursor start/end
source ranges
coverage complete/partial
has older/newer
gap reason
projection revision
serialized bytes
```

No generic epoch is fabricated for sources that do not expose it.

Enforce four independent bounds:

```text
source rows
projected item count
serialized bytes after projection
single item bytes
```

Oversized item becomes explicit detached body or gap; not silently truncated while claiming continuity.

### Family

Use declaration-first identities where runtime provides them. Preserve aliases/tool IDs separately. Retain `native_status_raw`; derive `normalized_status` for UI and record `normalization_loss`. Only raw/typed terminal evidence affects ProducerRef release.

### Коммуникация

Borrow CCCC concepts without its store:

```text
message_id
per-recipient delivery_id
sender/recipient generation
payload digest
stored/claimed/accepted/failed/ambiguous
read cursor
in_reply_to
reply deadline
```

Map them to existing mailbox/Operation. A mention/comment is data, not model execution trigger. Cancellation/reply names original delivery identity and current generation.

### Acceptance

- reconnect gap causes canonical refetch;
- one late duplicate event folds once;
- oversized item does not break whole session;
- child alias cannot become another child silently;
- parent terminal does not close child;
- reply to old generation rejected;
- routing-only notification does not spend model turn.

## 20. R23. Capacity, permissions и supply chain модулей

### Capacity

Use active + reserved admission. Reservation records owner/generation/acquisition/purpose. In-memory counters may be an optimization, not durable source after restart.

Rolling budgets may cover:

```text
session creates
model turns
active/pending writers
operator questions
external mutations
```

Unknown approval mode is high risk/fail closed, not interactive by optimism.

### Permissions

Separate:

```text
host API authorization
native approval mode
workspace trust
OS sandbox/isolation
credential access
network access
```

AoE manifest grants limit host API but its worker `NoSandbox` does not isolate arbitrary plugin code. ELIOT documentation must never call a capability manifest a sandbox.

### Install/update

Use preview/apply pattern:

```text
resolve source
fingerprint source + manifest + dependency lock + build artifact
show capability/build/runtime/trust delta
explicit approval
re-resolve
reject changed fingerprint
stage new generation
activate
verify postcondition
retire old generation
```

Track separately:

```text
source_tree_digest
manifest_digest
dependency_lock_digest
build_artifact_digest
runtime_binary_digest
granted_capabilities_digest
active_generation
```

A source hash that excludes build output is not an executable attestation.

### Acceptance

- wider update requires approval;
- changed artifact under same source cannot activate;
- unknown capability rejected;
- old generation late event fenced;
- failed activation keeps old generation or explicit unavailable, not half-applied state;
- worker API grant does not create filesystem/network privilege by itself.

## 21. R24. Handoff агенту и создание Issue

### Фаза A — правка документации

One documentation PR should:

1. fix immediate defects;
2. update truth/status overlays;
3. apply R10–R24 norms to existing documents without duplicating full text;
4. update donor inventory/pins/status;
5. add cross-links and exact pending labels;
6. run link/path/source-symbol checks appropriate for docs;
7. contain no product code.

### Фаза B — implementation Issue

After docs merge, create narrowly causal Issues. Each Issue must include:

```text
problem/failure
current source files and symbols
normative paragraphs
what already exists and must not be redone
complete change boundary
negative acceptance cases
required focused tests/fixtures
qualification boundary
explicit non-goals
```

Do not create Issue titles like “implement orchestration”, “add reliability” or “support donors”.

Suggested Issues:

1. MCP logical request ID/error correlation.
2. OpenCode goal record vs activation execution evidence.
3. Vendor-neutral typed prerequisite evidence.
4. Codex shared bridge public transport seam/server-request negatives.
5. Native family authoritative projection and byte bounds.
6. Writer-output delivery/disposition.
7. Scoped attention/background operation.
8. CheckRunner baseline/scope/cache contract.
9. Controller resource inventory/cleanup.
10. Host due work/quota/Doctor projection.
11. ACPX optional backend pilot.
12. MCP Operation task/subscription projection.
13. Module package/update preview and generation activation.

### Фаза C — порядок

Implement in the causal order from §12. Do not parallelize two tasks that edit the same shared contract unless one is explicitly the contract owner and the other waits for its merged revision.

## 22. План правок документации по файлам

| File | Exact work |
|---|---|
| `README.md` | Remove defects; current surface matrix; compact next-work list; link program |
| `docs/agent_swarm.md` | Add Task policy, writer output, observations/attention, host duties, migration authority |
| `docs/agent_swarm.module-contract-v2.md` | Current implementation status; operation evidence; steer classes; replay; background; package provenance |
| `docs/agent_swarm.implementation-v6.md` | Mark existing implementation; add causal R0–R24 roadmap and file ownership |
| `docs/runtime-notes.md` | Add current overlay, per-adapter control/evidence classes, keep historical research dated |
| `docs/check-runner.md` | Effective inputs, lease/recovery, baseline/cache/scope/protected input contract |
| `docs/agent_swarm.donors-20260929.toml` | Exact current tags/commits, reuse form, license/authority/qualification fields |
| `modules/muse/README.md` | SDK command/replay/gap/host-death delegation and upgrade qualification |
| `modules/codex/README.md` | Shared transport seam, approval negatives, exact steer class, runtime ownership |
| `modules/opencode/README.md` | Goal evidence split, `/background` status, family/history bounds |
| `modules/opencodex/README.md` | Preview/apply pattern, partial outcomes, affinity/static-header warning, post-core boundary |
| Future ACP module guide | ACPX authority split and exact qualification matrix |

## 23. Definition of done программы

Documentation program is complete when:

- current code is not described as absent or qualified beyond evidence;
- every runtime has one lifecycle owner/topology;
- every mutating boundary states replay/idempotency and completion condition;
- Task policy, Task revision, Attempt, assignment, submission and candidate identities are explicit;
- branch/session names are not authority;
- observation freshness/coverage/gaps are explicit;
- native status and normalized UI state are separate;
- writer outputs cannot vanish during cleanup;
- CheckRunner remains single verification authority;
- donor reuse units and licenses/provenance are exact;
- unimplemented features are labelled pending;
- the next agent can create narrow Issues without repeating repository discovery;
- no second scheduler/task DB/transcript/acceptance system is introduced.

## 24. Реестр зафиксированных источников

### Baseline ELIOT

- [README](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/README.md)
- [Module contract](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/docs/agent_swarm.module-contract-v2.md)
- [Donor inventory](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/docs/agent_swarm.donors-20260929.toml)
- [Submissions](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/store/submissions.rs)
- [OpenCode prerequisite gate](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/store/prerequisites.rs)
- [MCP façade](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/mcp.rs)
- [CheckRunner guide](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/docs/check-runner.md)
- [Muse guide](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/muse/README.md)
- [Codex guide](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/codex/README.md)
- [OpenCode execution log reader](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/runtime/opencode_v2/execution.rs)

### Основные пакеты и backend

- [Muse SDK pin](https://github.com/meta-models/muse-code-sdk/tree/a7c10c5dd3f66be412077d29f9d11111af70317b)
- [Muse SDK pending command set](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/pending/pending-command-set.ts)
- [Muse SDK turn submit](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/turn-submit.ts)
- [Muse SDK gap fill](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/gap-fill.ts)
- [Muse SDK host death](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/host-death.ts)
- [Pinned Codex SDK client in ELIOT](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/codex/vendor_bridge/src/openai_codex/client.py)
- [RMCP release source](https://github.com/modelcontextprotocol/rust-sdk/tree/0cde3c5cf3e6aff0cc852ce6045f107e95991f48)
- [ACPX shared sessions](https://github.com/openclaw/acpx/blob/v0.19.4/docs/shared-sessions.md)
- [OpenCodex Management API](https://github.com/lidge-jun/opencodex/blob/ef0297f86c4540c7d757c8595170d66f9c584aec/docs-site/src/content/docs/reference/management-api.md)

### Источники контрактных решений

- [Paseo timeline sync](https://github.com/getpaseo/paseo/blob/v0.10.3/docs/timeline-sync.md)
- [Paseo lifecycle](https://github.com/getpaseo/paseo/blob/v0.10.3/docs/agent-lifecycle.md)
- [CCCC delivery contract](https://github.com/ChesterRa/cccc/blob/v0.4.41/crates/cccc-core/src/connect_delivery.rs)
- [Agent of Empires plugin API](https://github.com/agent-of-empires/agent-of-empires/blob/v1.18.0/docs/plugin-api.md)
- [Agent of Empires automation policy](https://github.com/agent-of-empires/agent-of-empires/blob/v1.18.0/src/plugin/automation_policy.rs)
- [Claw verification](https://github.com/Enderfga/claw-orchestrator/blob/v7.6.1/skills/references/verification.md)
- [Waku server](https://github.com/egoist/waku/blob/v0.1.20/crates/waku-core/src/server.rs)
- [Helicon repository](https://github.com/HarjjotSinghh/helicon/tree/80f3351e49b12a5ca21de748e2b4c3551e886d11)
- [Atlas ACP host pin](https://github.com/pacifio/atlas/blob/a34a6d44bf37d26d9a6f8f6fe1fab5ce0a92d8d1/src-tauri/src/commands/agent_host.rs)

### Эксплуатационные материалы

- `MANAGER-BRIEF(1).md` — current §0 plus historical incident log; supplied file.
- `eliot-root-scripts-20261002-1030.zip` — source snapshot named at top, not committed.
- `manager-orchestrator-executors-deep-audit-revision-4-2026-10-02.md` — source/evidence separation and donor assessment; supplied file.
