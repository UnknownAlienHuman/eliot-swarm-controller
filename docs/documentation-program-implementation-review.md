# ELIOT Swarm Controller — Implementation Review for Documentation Program

**Редакция:** 1 · 2 октября 2026  
**Проверенный baseline:** `c99a71fbba2de2f663e2709a31e3ccc205621df3`  
**PR:** #13 `Documentation Program`  
**Назначение:** сверка фактической реализации с действующей документацией и донорскими контрактами.  
**Scope:** документация и implementation handoff. Этот файл не меняет runtime-код и не объявляет live-квалификацию.

## 0. Метод

Проверка выполнена по четырём независимым слоям:

1. **Фактический код ELIOT** на точном baseline: Store, OpenCode V2, Muse bridge, Codex bridge, MCP, CheckRunner, Doctor и OpenCodex module.
2. **Текущие документы ELIOT:** архитектура, module contract, implementation plan, runtime notes, module guides, donor inventory.
3. **Полные выбранные donor units и официальные contracts:** Muse SDK, Codex Python SDK, RMCP, OpenCodex Management API; для остальных — exact pattern sources, stable tags и лицензии.
4. **Field evidence:** присланный Manager Brief и архив управляющих скриптов. Они используются как evidence отказов и требований к migration, а не как новый canonical product contract.

Для каждого вывода используется одно из состояний:

- `ALIGNED` — код и документация описывают одну boundary;
- `IMPLEMENTED_UNQUALIFIED` — код есть, но live installed-runtime qualification не выполнена;
- `DOC_AHEAD_OF_CODE` — документ описывает будущую норму;
- `CODE_AHEAD_OF_DOC` — поведение реализовано, но документация неполна;
- `CONTRADICTION` — исполняемый код и действующий текст требуют несовместимое поведение;
- `RESEARCH_ONLY` — donor/source finding, не свойство ELIOT;
- `UNKNOWN` — исходные данные не дают достаточного доказательства.

Stable release, текущий `main`, открытый PR, issue/proposal и пользовательский workaround не считаются взаимозаменяемыми.

## 1. Краткий итог

На baseline уже выполнена значительная часть первой редакции Documentation Program: README очищен, текущая готовность разведена с research, architecture/module-contract/implementation/runtime notes обновлены, donor inventory расширен, MCP request-id contract уточнён, CheckRunner contract усилен. Повторять эти правки нельзя.

Остаток программы теперь состоит не из общей «доработки документации», а из восьми адресных задач:

| Приоритет | Работа | Статус |
|---|---|---|
| P0 | Разрешить конфликт OpenCodex no-pin policy и точного mutation gate | `CONTRADICTION` |
| P0 | Исправить OpenCode goal evidence: admission не равен model execution start | `CONTRADICTION` |
| P1 | Вынести OpenCode-specific prerequisite interpretation из Store | `DOC_AHEAD_OF_CODE` |
| P1 | Ввести bounded setup evidence для нескольких обязательных settings | `DOC_AHEAD_OF_CODE` |
| P1 | Точно описать, какие Muse state machines реально делегированы SDK, а какие написаны ELIOT | `DOC_AMBIGUOUS` |
| P1 | Добавить OpenCode `background` как адресную native capability, не watchdog | `RESEARCH_ONLY → IMPLEMENTATION` |
| P2 | Довести timeline/family/attention до одного typed projection contract | `PARTIAL` |
| P2 | Исправить donor inventory: лицензии, exact reviewed commits и compatibility state | `DOC_INCOMPLETE` |

## 2. Что на baseline уже сделано правильно

### 2.1. MCP: stable logical request ID отделён от transport reply

`src/mcp.rs` теперь прямо говорит machine client: для безопасного повтора mutation caller должен создать и сохранить `client_request_id` **до первого вызова**. Автоматически созданный ID возвращается только в полученном результате и не делает безопасным retry после потери самого ответа.

Это правильная boundary:

```text
caller-owned logical request ID
    != MCP/JSON-RPC transport request
    != Operation ID
```

MCP façade не делает silent retry после `DISCONNECTED`, `OUTCOME_UNKNOWN`, protocol/IO/write errors; соединение сбрасывается, а следующий вызов остаётся новым caller decision. Tool execution failures возвращаются как visible tool-level result, а невозможность маршрутизировать метод остаётся protocol error.

**Сохранить:**
- один application API для CLI и MCP;
- closed input schemas;
- read-only annotations;
- durable Operation handle;
- caller-owned logical ID для mutation.

**Не добавлять:** второй MCP Task store или автоматический retry mutating tool call.

Источники:
- [`src/mcp.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/mcp.rs)
- [RMCP 3.5.0 README](https://github.com/modelcontextprotocol/rust-sdk/blob/0cde3c5cf3e6aff0cc852ce6045f107e95991f48/README.md)

### 2.2. Codex read-only shared-server bridge имеет правильный fail-closed seam

Pinned Python SDK по умолчанию умеет запускать собственный stdio app-server и его default approval handler принимает command/file approvals. ELIOT не наследует эти defaults:

- shared bridge не запускает и не останавливает server;
- hard allowlist применяется **до записи в WebSocket**;
- разрешены только `initialize`, `initialized`, `thread/read`, `thread/list`;
- server-initiated command/file approvals получают `decline`;
- `thread/read` не называется resume;
- server executor version и SDK pin записываются отдельно.

Это одна из наиболее чистых реализаций в проекте: whole generated models/router сохранены, а policy находится в небольшом ELIOT-owned transport seam.

**Оставшаяся граница:** bridge subclass переопределяет private methods `_start_reader_thread`, `_write_message`, `_read_message`, `start`, `close`. Обновление SDK обязано иметь seam-diff check и protocol fixtures; наличие совместимого класса по имени недостаточно.

Источники:
- [`modules/codex/bridge.py`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/codex/bridge.py)
- [`modules/codex/vendor_bridge/src/openai_codex/client.py`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/codex/vendor_bridge/src/openai_codex/client.py)

### 2.3. CheckRunner уже сильнее большинства доноров

Реализация связывает CheckRun с:

- exact immutable source candidate;
- exact trusted profile and profile revision;
- resource key/lease;
- process-group identity;
- retained stdout/stderr and terminal receipt;
- conservative recovery without replay.

Cache key включает candidate bytes and selected profile; cross-attempt completed reuse выключен как `disabled_unversioned_environment`. Документ честно помечает reverse-dependency scope/cache reuse как `PENDING`.

Это лучше terminal self-report у cockpit/control-plane donors и лучше best-effort evidence publication у Claw. Здесь не нужен второй verifier store.

**Не решено автоматически:** writer может изменить тесты/config **до source capture**. Для задач, где acceptance meaning должен быть protected, baseline должен быть Task policy (`protected_paths`, profile revision, allowed test changes), а не эвристика по языку.

Источники:
- [`src/store/checks.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/store/checks.rs)
- [`docs/check-runner.md`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/docs/check-runner.md)

### 2.4. OpenCode durable execution reader имеет правильные границы

`ExecutionScan` не использует volatile event stream или projected idle как terminal evidence. Он:

- проверяет root origin, binding/generation/model;
- сопоставляет exact deterministic inbox ID и original payload;
- различает enqueue, delivery, execution-started и terminal;
- сохраняет anchor/hash/watermark;
- не требует плотной `seq + 1`;
- переводит history changes/restarts/unknown lifecycle в explicit uncertainty;
- считает shutdown/superseded recovery pending, а не completed.

Это сильная реализация. Её следует расширять адресно, а не заменять Paseo/Waku state machine.

Источник:
- [`src/runtime/opencode_v2/execution.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/runtime/opencode_v2/execution.rs)

## 3. P0: OpenCodex service policy противоречит исполняемому mutation contract

### 3.1. Три текущих утверждения

**Donor inventory на `main`:**

- operator сам запускает и обновляет OpenCodex;
- service не pin-ится ELIOT;
- observed version difference — observation, не defect и не mutation blocker;
- re-baseline к upstream current отложен в Phase B.

**Исполняемый bridge:**

- artifact и contract baseline — `2.73.0`;
- config требует `expectedVersion`;
- `gate(record)` вызывает attach и разрешает preview/configure только при `versionComparison == match`;
- mismatch возвращает `version_mismatch` **до любой mutation**.

**Module README/Doctor:**

- README называет exact version hard gate;
- Doctor создаёт `opencodex_version_mismatch` и советует выровнять service version/expectedVersion.

Это не спор терминов. Operator no-install-pin и adapter contract compatibility — разные оси, а текущие документы их смешали.

### 3.2. Почему нельзя просто удалить gate

Latest reviewed OpenCodex release — `v2.75.0`, а bridge реализован по `v2.73.0`. Между ними 43 commits. Изменены, среди прочего:

- Management API agent settings routes;
- config routes;
- model rows;
- shared management helpers;
- provider/catalog surfaces;
- configuration/schema documentation.

Следовательно, exact mutation shapes нельзя считать совместимыми только потому, что endpoint names сохранились.

### 3.3. Нормативное решение

Разделить три версии:

```json
{
  "service_version_observed": "2.75.0",
  "adapter_contract_baseline": "2.75.0",
  "compatibility_state": "verified_exact | verified_range | read_only_unknown | incompatible"
}
```

Правила:

1. **ELIOT не pin-ит и не обновляет operator-owned service.**
2. **Read-only observation** может работать при незнакомой версии, если response validation не ослабляется; sections degrade independently.
3. **Mutation** разрешается только по проверенному compatibility record конкретного operation kind.
4. До re-baseline safest state для newer service: `read_only_unknown`, а не «defect» и не «write anyway».
5. `expectedVersion` как operator install lock удалить; заменить `adapterContractBaseline` и generated/maintained compatibility table.
6. Doctor сообщает:
   - observed service version;
   - adapter baseline;
   - affected operation kinds;
   - whether observation or mutations are available.
7. Update procedure читает exact upstream Management docs/source diff, обновляет fixtures, затем повышает baseline.

### 3.4. Phase B work item

**OpenCodex contract re-baseline 2.73 → 2.75**

Acceptance:

- exact 2.75 commit recorded;
- endpoint/schema diff classified per implemented Operation;
- every writer fixture updated from reviewed contract shapes;
- 2.75 fake-server scenarios cover preview stale, partial 207, lost response, readback mismatch and unknown fields;
- read-only attach at newer unknown version remains possible and visibly unqualified;
- no mutation is sent when its operation-kind compatibility is unknown;
- no service start/stop/update is added.

Sources:
- [`docs/agent_swarm.donors-20260929.toml`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/docs/agent_swarm.donors-20260929.toml)
- [`modules/opencodex/bridge.mjs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/opencodex/bridge.mjs)
- [`modules/opencodex/README.md`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/opencodex/README.md)
- [`src/doctor.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/doctor.rs)
- [OpenCodex v2.75 Management API](https://github.com/lidge-jun/opencodex/blob/ef0297f86c4540c7d757c8595170d66f9c584aec/docs-site/src/content/docs/reference/management-api.md)

## 4. P0: OpenCode goal admission ошибочно называется началом model work

### 4.1. Фактическое поведение

Controller-recorded goal:

1. записывает `eliot.goal` instruction entry;
2. проверяет exact readback;
3. для active set/edit/resume отправляет deterministic prompt input;
4. получает/восстанавливает inbox/message evidence этого input;
5. возвращает `completion_condition = native_goal_recorded`.

Это честная boundary. Однако `goal_details` формирует:

```text
activation_input_id != null
→ model_work_started = true
```

Тесты закрепляют это поведение.

Input receipt доказывает admission/delivery projection, но не `session.execution.started`. В проекте уже существует durable execution-log reader, который умеет доказать execution start как отдельное событие.

### 4.2. Исправление контракта

Goal result должен разделить:

```json
{
  "record_applied": true,
  "activation_input_id": "msg_...",
  "activation_admitted": true,
  "activation_execution_started": false,
  "activation_execution_ref": null
}
```

`model_work_started` удалить либо оставить только как derived alias точного `activation_execution_started`.

Goal Operation остаётся завершённой на `native_goal_recorded`. Поздний execution evidence добавляется в observation/native refs; он не переписывает историческую completion boundary.

### 4.3. Реализация

Предпочтительный путь:

- обобщить exact input lifecycle reader на typed native input descriptor;
- descriptor goal activation содержит exact input ID, prompt digest and goal marker;
- не разрешать generic text/time matching;
- execution start correlates through `inbox.delivered` to active `session.execution.started`;
- terminal belongs to that serialized busy period;
- multiple inputs sharing a busy period remain allowed and explicitly documented.

Acceptance:

- admitted-but-not-delivered goal: `activation_admitted=true`, start false;
- delivered with missing start: uncertainty, not true;
- exact started event: start true + ref;
- lost activation response but message found: admitted true, start determined only by log;
- goal record applied but activation absent: goal applied, activation false;
- no Task acceptance or family completion synthesized.

Sources:
- [`src/runtime/opencode_v2/goal.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/runtime/opencode_v2/goal.rs)
- [`src/runtime/opencode_v2/execution.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/runtime/opencode_v2/execution.rs)
- [`modules/opencode/README.md`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/opencode/README.md)

## 5. P1: Store всё ещё интерпретирует OpenCode-specific evidence

`src/store/prerequisites.rs` импортирует `opencode_v2::ConfigurationExpectation` и знает:

- instruction-entry key/action/digest;
- native agent modes/hidden/model override;
- model provider/id/variant/status;
- OpenCode settings/catalog/definition revisions;
- exact OpenCode read methods and contract revisions.

Это прямое исключение из декларированной границы `Store`/`RuntimePort`. Документация уже признаёт исключение, но code boundary ещё не изменена.

### 5.1. Целевая boundary

Adapter формирует opaque-to-Store evidence:

```json
{
  "condition_kind": "effective_configuration",
  "condition_scope": "session:model",
  "desired_digest": "sha256:...",
  "effective_revision": "sha256:...",
  "native_scope_key": "...",
  "native_root_id": "...",
  "adapter_contract_revision": "...",
  "observed_at_ms": 0
}
```

Store проверяет только:

- operation/binding/generation ownership;
- earlier Operation;
- typed generic completion boundary;
- same scope;
- digest/revision equality;
- later conflicting condition evidence;
- observation freshness.

Native schema, catalog and readback interpretation остаются в adapter.

### 5.2. Составная подготовка

Одна `prerequisite_operation_id` корректно упорядочивает один step, но не доказывает весь setup.

Пример:

```text
A: instruction X
B: select agent, prerequisite A
D: replace X
C: send, prerequisite B
```

C проверяет B, но не обязательно X.

Не нужен общий DAG. Adapter должен выдавать bounded setup snapshot:

```json
{
  "setup_digest": "sha256:...",
  "conditions": [
    {"scope":"session:model","revision":"..."},
    {"scope":"session:agent","revision":"..."},
    {"scope":"instruction:eliot.policy","desired_digest":"..."}
  ]
}
```

Перед admission adapter повторно проверяет весь bounded set. Store сохраняет/сравнивает generic evidence.

Acceptance:

- изменение unrelated instruction не блокирует send;
- изменение required instruction блокирует;
- model catalog reload, изменивший выбранное definition, блокирует;
- later operation on same scope pending → prerequisite pending;
- no vendor types imported by Store;
- hard cap on condition count and bytes;
- no arbitrary workflow graph language.

Источник:
- [`src/store/prerequisites.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/src/store/prerequisites.rs)

## 6. P1: Muse donor adoption нужно описать точнее

Donor inventory говорит «use whole official SDK». Это правильно как supply-chain decision, но может быть прочитано как делегирование всех protocol state machines SDK.

Фактически ELIOT bridge использует low-level public Connection и собственные:

- durable `nativePending`;
- controller checkpoint;
- exact command-ID derivation;
- Operation reconciliation;
- family projection;
- result persistence;
- host IPC outcomes.

Official SDK на том же pin уже содержит высокоуровневые state machines:

- pending-before-send;
- same-commandId retry;
- only durable `commandRejected` settles a submission;
- host durability/ephemeral death handling;
- cursor gap splice-fill with opaque cursors;
- pending-command snapshot join.

### 6.1. Что в ELIOT сделано хорошо

- native command ID/payload persisted before write;
- explicit reconciliation reuses the same ID and byte-equivalent params;
- no reconnect/timeout blanket replay;
- exact expectedTurnId for steer;
- unsupported server requests fail instead of hanging;
- host ACK and native outcome remain separate.

### 6.2. Что нужно зафиксировать

Add module guide table:

| Concern | Owner |
|---|---|
| framing, generated types, request router, server-request plumbing | official SDK |
| command ID and Operation mapping | ELIOT bridge |
| durable checkpoint across bridge loss | ELIOT |
| same-ID replay semantics | native protocol + SDK contract, driven by ELIOT explicit reconcile |
| view gap fill | current ELIOT compact observation; SDK GapFiller is not automatically inherited |
| host-death durability classification | SDK handshake facts + ELIOT module owner/recovery |
| Task/Attempt/acceptance | ELIOT host |

On SDK upgrade, tests must compare ELIOT behavior with donor invariants, not only import/syntax.

Sources:
- [`modules/muse/bridge.mjs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/modules/muse/bridge.mjs)
- [Muse SDK `turn-submit.ts`](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/turn-submit.ts)
- [Muse SDK `pending-command-set.ts`](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/pending/pending-command-set.ts)
- [Muse SDK `host-death.ts`](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/host-death.ts)
- [Muse SDK `gap-fill.ts`](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/clients/sdk-ts/src/facade/gap-fill.ts)

## 7. P1: адресный OpenCode `background` нужен как capability, не как watchdog

Field scripts repeatedly needed to unblock a manager waiting on a foreground subagent call. Steer is queued until the agent loop reaches a boundary; the documented native `/background` operation converts supported foreground tools to background observation without killing the child.

Current runtime notes correctly mark this as research-only: adapter operation is absent.

Target operation:

```text
native.opencode.background_foreground_tools
```

Required contract:

- exact binding/session target;
- ownership/generation check;
- read current foreground tool inventory;
- only native-declared backgroundable tools;
- one native mutation;
- postcondition readback;
- no interrupt/kill/restart;
- no use based only on age;
- no claim that queued steer was consumed;
- safe `unsupported` when installed service lacks the endpoint/schema.

This capability is distinct from:

- sending steer;
- replying to form/permission;
- interrupting execution;
- declaring session idle;
- Task cancellation.

Source:
- [`docs/runtime-notes.md`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/docs/runtime-notes.md)

## 8. P2: timeline, family and attention contract

### 8.1. Timeline

Paseo supplies the best pattern for:

- low-latency live stream;
- authoritative paged history;
- epoch/range/gap;
- canonical vs optimistic user submissions;
- declaration-first child identities.

ELIOT should retain its stronger durable native evidence and add a uniform projection:

```text
source_kind
projection_revision
cursor/range identity
coverage_complete
has_older / has_newer
gap_reason
retained_stale_members
serialized_byte_length
```

Limits must apply after projection:

```text
max_source_rows
max_projected_items
max_serialized_bytes
max_single_item_bytes
```

An oversized irreducible item becomes detached artifact/gap, not silent truncation.

### 8.2. Family

Family membership and execution disposition are separate:

```text
declared child identity
native parent/alias facts
latest raw status
normalized display status
known execution periods
producer binding
coverage/gaps
```

Parent idle or root terminal never closes children. Normalized UI `running/paused/pending` cannot discharge ProducerRef.

### 8.3. Attention

Replace `Remind-Subagents.py` heuristics with a read-only projection:

```text
waiting_for_native_request
waiting_for_child_result
foreground_tool_blocking
input_queued_not_consumed
observation_stale
capacity_available
manager_actionable
```

Policy may suggest an action; only an addressed Operation performs it. No observer mutates the native service.

Donor pattern:
- Paseo timeline projection;
- CCCC delivery identity/generation;
- Atlas resume-pending gate;
- Agent of Empires active + reserved accounting.

## 9. Donor inventory corrections

Current inventory’s adoption classes are useful, but evidence metadata remains incomplete.

### 9.1. Licenses verified from exact tags

| Donor | Exact checked unit | License |
|---|---|---|
| Paseo `v0.10.3` | repository root | Apache-2.0, with retained third-party licenses |
| Waku `v0.1.20` | repository root | GPL-3.0 |
| Agent of Empires `v1.18.0` | repository root | MIT |
| Claw `v7.6.1` | `Enderfga/claw-orchestrator` | MIT |

Replace `UNVERIFIED` where these exact checks apply. Waku stays pattern-only; GPL is an additional reason not to vendor its core into this repository.

### 9.2. Exact reviewed commits

Every stable tag in inventory needs resolved commit SHA. In particular:

- ACPX `v0.19.4`: inventory still retains an older revision/manifest fact from `0.19.3`; resolve and record the exact `0.19.4` commit before pilot.
- Paseo/Waku/AoE/Claw: record tag commit, not tag string alone.
- OpenCodex: record both reviewed release commit and adapter contract baseline.

### 9.3. Qualification fields

Use separate fields:

```text
source_reviewed
installed
fixture_checked
live_smoke
fault_injected
soaked
role_qualified
```

A single prose `qualification` is useful for humans but insufficient for machine comparisons.

Sources:
- [`docs/agent_swarm.donors-20260929.toml`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c99a71fbba2de2f663e2709a31e3ccc205621df3/docs/agent_swarm.donors-20260929.toml)
- [Paseo LICENSE](https://github.com/getpaseo/paseo/blob/v0.10.3/LICENSE)
- [Waku LICENSE](https://github.com/egoist/waku/blob/v0.1.20/LICENSE)
- [Agent of Empires LICENSE](https://github.com/agent-of-empires/agent-of-empires/blob/v1.18.0/LICENSE)
- [Claw LICENSE](https://github.com/Enderfga/claw-orchestrator/blob/v7.6.1/LICENSE)

## 10. Donor decisions after code review

| Donor | Decision | Exact useful mechanism | Do not import |
|---|---|---|---|
| Muse SDK | ADOPT whole package | generated wire types, Connection/router, command replay and gap/host-death invariants | model loop or global install |
| Codex Python SDK | ADOPT pinned source closure | generated models, router, current-turn CAS methods | default auto-approval, owned stdio lifecycle in shared profile |
| RMCP | ADOPT official crate | typed tools, task/subscription protocol, tool-level errors | second Task authority, stale cache for authoritative reads |
| ACPX | PILOT whole optional backend | one owner, queue/watch, same-session-only, targeted cancel | replacing Operations/Store/acceptance |
| Paseo | PATTERN | live/canonical split, ranges/gaps, declaration-first children | app/server authority, unbounded projected pages |
| CCCC | PATTERN | delivery identity, actor generation, reply/cancel binding | scheduler, ledger, per-actor process topology |
| AoE | PATTERN | manifest fingerprint, reapproval, active+reserved | treating capability checks as OS sandbox |
| Claw | PATTERN | blocked/superseded, caller-owned verifier, unverified | second journal/workflow store, blind replay of external effects |
| OpenCodex | EXTERNAL SERVICE | preview/fingerprint/readback, partial envelopes, affinity | provider translation in Rust core |
| Waku | PATTERN WITH EXCLUSIONS | sequential service worker, reconnect vs process exit | same-ID fresh fallback, ignored settings failures |
| Helicon | RESEARCH ALTERNATIVE | official Muse SDK cockpit, command/query separation | simultaneous session owner behind ELIOT |
| Atlas ACP | RESEARCH ALTERNATIVE | resume-pending gate, identity mapping, mode-before-first-send | broad manager/store/transcript authority |

## 11. Current documentation status

### Phase A already landed on `main`

Do not repeat:

- README merge-marker and duplicate cleanup;
- implementation/qualification matrix;
- architecture R10–R17;
- updated module-contract vocabulary;
- implementation-plan R0–R24;
- runtime notes implementation/research split;
- CheckRunner effective-input/resource/recovery text;
- MCP stable-request-ID warning;
- donor adoption-class inventory.

### Remaining documentation edits

1. Update Documentation Program baseline and mark Phase A complete.
2. Add this implementation review as the Phase B source.
3. Resolve OpenCodex service/baseline compatibility language across:
   - donor inventory;
   - module README;
   - bridge contract;
   - Doctor.
4. Correct OpenCode goal evidence vocabulary.
5. Make vendor-neutral prerequisite extraction a concrete Phase B item.
6. Add Muse delegation/ownership table.
7. Add exact donor commits/licenses.
8. Keep owner workflow policy explicitly unresolved where supplied field instructions conflict; do not silently choose one historical line.

## 12. Phase B work order

### B1 — OpenCodex compatibility re-baseline

Files:
- `modules/opencodex/bridge.mjs`
- `modules/opencodex/README.md`
- `modules/opencodex/UPDATE.md`
- fixtures/selftest
- `src/doctor.rs`
- donor inventory

### B2 — OpenCode goal evidence

Files:
- `src/runtime/opencode_v2/goal.rs`
- `src/runtime/opencode_v2/execution.rs` or a shared input-evidence unit
- tests
- `modules/opencode/README.md`

### B3 — Typed configuration conditions

Files:
- RuntimePort/external outcome contract
- OpenCode adapter evidence
- `src/store/prerequisites.rs`
- module contract and implementation plan

### B4 — Bounded setup snapshot

Depends on B3. No generic DAG.

### B5 — Muse donor conformance

Docs first; then fixtures for:
- same-ID replay;
- commandRejected-only settlement;
- host durability profile;
- gap overlap;
- unknown server request;
- exact-turn stale rejection.

### B6 — OpenCode background operation

Depends on installed-schema qualification and exact readback.

### B7 — Timeline/family/attention projection

Build over current observations and durable log; no new transcript store.

### B8 — Donor inventory evidence normalization

Documentation-only, independent after exact tag resolution.

## 13. Negative acceptance scenarios

An implementation pass is incomplete unless these cases remain safe:

1. OpenCodex service is newer than adapter baseline: read-only evidence may be available; unknown mutation compatibility sends no write.
2. Goal activation prompt is admitted but execution has not started: no `model_work_started=true`.
3. One required setup condition changes after another prerequisite: send remains blocked.
4. Unrelated setting changes: send is not globally blocked.
5. Muse transport reply is lost after admission: same command ID only, no fresh input.
6. Muse non-settling error: command remains pending/unknown, not rejected.
7. Codex shared bridge receives approval request: declines; no work admitted.
8. MCP caller omits logical ID and loses response: documentation never claims retry safety.
9. Timeline projected item exceeds byte budget: explicit gap/artifact, no false continuity.
10. Child disappears from partial enumeration: retained stale/unknown, not terminal.
11. OpenCode foreground tool is backgrounded: child continues; no interrupt or Task cancellation.
12. Cleanup sees unknown process/file ownership: leaves resource unresolved.

## 14. Do not redo

The next implementation agent must not recreate:

- Store/Task/Attempt/Operation;
- immutable artifacts and assembly;
- fixed-source CheckRunner;
- OpenCode durable execution log;
- OpenCode exact result/message/diff/tool-file readers;
- Muse process-owner/checkpoint recovery;
- Codex generated models/router;
- OpenCodex preview/fingerprint/readback machinery;
- MCP tool catalogue/application routing;
- a second scheduler, message ledger, workflow journal or acceptance store.

## 15. Definition of done for this review

This review is complete when:

- Documentation Program is rebased onto the current baseline;
- this file is present in PR #13;
- PR body separates completed Phase A from remaining Phase B;
- the PR contains documentation only;
- exact source links resolve;
- no code or donor is called live-qualified without evidence;
- the two P0 contradictions are explicit and cannot be mistaken for already implemented fixes.
