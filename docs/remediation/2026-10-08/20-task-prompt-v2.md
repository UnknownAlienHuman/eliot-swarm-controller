# R20. Task Prompt v2: краткая детерминированная проекция вместо raw Attempt snapshot

**Статус:** implementation handoff. Текущий diff содержит только это задание; production-код, artifact descriptors и маршруты ещё не изменены.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Результат

Полный immutable `Attempt.task_snapshot` остаётся авторитетным доказательством в Store и readback. Нативный агент больше не получает этот внутренний снимок целиком. Для `task.dispatch` Store один раз строит и сохраняет компактный, детерминированный `TaskPromptEnvelopeV1` из уже замороженного `snapshot.brief`, исходного `text`, идентичности Task/Attempt и, когда он есть, точного `launch_dispatch_packet`.

Адаптер передаёт `envelope.prompt` нативному сервису **без собственного рендера Task**, подтверждает его digest/byte count в admission receipt и не имеет fallback к старому raw-snapshot prompt в новом artifact version.

```text
TaskSpec
  -> Attempt.task_snapshot { spec, policy, dependencies, baseline, brief }
  -> Store TaskPromptEnvelopeV1 { exact prompt bytes + identities + digests }
  -> versioned RuntimeCommand
  -> native input
  -> TaskDispatchAdmissionReceipt.native_payload_sha256/bytes
```

Это один producer и один формат prompt. Не новый planner, не новый ledger и не новый model loop.

## 2. Почему это отдельный контракт, а не косметический рефакторинг

Сейчас `task_snapshot` уже содержит детерминированный `brief`, но рабочие prompt builders сериализуют весь snapshot ещё раз:

- `store/tasks.rs::task_snapshot` сохраняет `spec`, `revision`, dependency receipts, baseline, owner policy и `brief`;
- `store/operations.rs::dispatch` кладёт полный `a["task_snapshot"]` в `effective_request_json`;
- `runtime/batch.rs::instruction` добавляет полный snapshot в prompt;
- `runtime/codex.rs`, `runtime/prepared.rs`, built-in OpenCode V2 и standalone adapters делают соседние варианты той же операции;
- Rust/JS/Python adapters повторяют собственные строки `Task specification:` или `ELIOT immutable task snapshot:`.

Следствия:

1. модель получает внутренние receipts/policy metadata и одновременно их краткую проекцию;
2. один и тот же факт определяется разными renderers;
3. изменение snapshot служебным полем меняет native payload, хотя задача для модели не изменилась;
4. Unicode/byte count/canonical ordering проверяются в нескольких местах;
5. prompt bytes являются частью admission evidence, поэтому тихая замена формата внутри существующего artifact нарушит immutable replay/readback.

Следовательно, старые artifact identities не переписываются. Новый prompt вводится как отдельный versioned command schema и новая artifact version.

## 3. Что полезно из Cogentic, а что не переносить

Источник: [Cogentic: Multi-Agent Orchestration for Automated Proof Discovery, arXiv:2609.40324v1](https://arxiv.org/html/2609.40324v1), 30.09.2026.

Полезные принципы:

- исполнителю выдаётся узкий role-specific briefing: **что** сделать и какой результат нужен, а не весь внутренний журнал оркестратора;
- полные документы могут оставаться по ссылкам/путям, а не копироваться в каждый prompt;
- попытки и critiques отделены от уже подтверждённых фактов;
- генерация и проверка разделены;
- итоговое утверждение проверяется отдельной процедурой, а не самооценкой автора.

Для ELIOT это означает:

- `Attempt.task_snapshot` остаётся authoritative record;
- `snapshot.brief` становится рабочей проекцией для модели;
- `Operation`/`Observation`/acceptance остаются verified ledger;
- review findings и CheckRunner остаются проверкой;
- prompt renderer не принимает решений о завершённости.

Не переносить:

- новый синхронный round orchestrator;
- второй record/ledger поверх SQLite Store;
- shared mutable workspace;
- LLM-оценки `is_request_satisfied`, `is_progress_being_made`, `is_in_loop` как authority;
- автоматический re-plan/re-prompt loop;
- consensus голосов как Task acceptance.

Cogentic — исследовательская схема для proof search, а не готовый runtime contract ELIOT.

## 4. Доноры: точные функции и границы заимствования

### 4.1 Goose: verified bytes, а не повторное чтение пути

Проверенный источник: `aaif-goose/goose@540df77`, `crates/goose/src/scheduler/common.rs`.

Полезный паттерн:

```rust
pub struct ValidatedScheduleRecipe {
    bytes: Vec<u8>,
    source: PathBuf,
}
```

`open_regular_schedule_recipe` проверяет regular file до и после open; downstream получает уже проверенные bytes и исходную base directory.

Применение в R20: Store передаёт adapters уже построенный `TaskPromptEnvelopeV1`; adapters не перечитывают Task, не рендерят `TaskSpec` и не интерпретируют source paths.

Не переносить Goose scheduler storage, JSON registry или model loop.

### 4.2 AutoGen Magentic-One: названия стадий, но не authority

Проверенный источник: `microsoft/autogen@027ecf0`, `_magentic_one_orchestrator.py`.

Можно использовать как терминологическую подсказку: task facts, plan, progress, stall. Нельзя переносить реализацию:

- `_task`, `_facts`, `_plan` живут в памяти manager;
- progress ledger генерирует LLM;
- модель сама отвечает, удовлетворён ли запрос и идёт ли прогресс;
- JSON разбирается с десятью повторами;
- outer loop сбрасывает участников и снова рассылает полный ledger.

ELIOT уже имеет durable Task/Attempt/Operation. Добавлять ещё один Magentic-style ledger — Frankenstein.

### 4.3 Ractor и Tokio: отдельный вывод для supervisor loops

Проверенный источник: `slawlor/ractor@1af0aaa`, `docs/runtime-semantics.md`.

Полезна семантика приоритетов:

```text
Kill / cancellation
Graceful stop
Child supervision event
Ordinary work
```

Импортировать второй actor runtime не требуется. В текущих Tokio coordinators тот же порядок выражается локально через `tokio::select! { biased; ... }`, где shutdown/stop стоят раньше постоянно готового work stream. Это относится к отдельным supervisor/scheduler fixes; R20 не добавляет actor abstraction.

### 4.4 Paseo: lifecycle owner только для subscriptions

Проверенный источник: `getpaseo/paseo@7fd469a`, `SessionDelivery`.

Полезны `hasDemand(family)`, host-assigned subscription ID, source ownership, idempotent `releaseOwner` и cleanup на detach. Эти функции уже относятся к R08/#34. Disconnect observation source не должен отменять принятую Task или native work.

## 5. Единственный новый DTO

Добавить в `swarm-contracts`, например `src/task_prompt.rs`:

```rust
pub const TASK_PROMPT_SCHEMA_ID: &str = "swarm.task_prompt";
pub const TASK_PROMPT_SCHEMA_VERSION: u16 = 1;
pub const TASK_PROMPT_CONTRACT_REVISION: &str = "task-prompt-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPromptEnvelopeV1 {
    pub schema_id: String,
    pub schema_version: u16,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub task_snapshot_sha256: String,
    pub prompt_sha256: String,
    pub prompt_bytes: u64,
    pub prompt: String,
}
```

### 5.1 DTO validation

`TaskPromptEnvelopeV1::validate_shape()` проверяет только data contract:

- exact schema ID/version;
- nonempty bounded Task/Attempt IDs без control characters;
- positive Task revision;
- lowercase 64-hex digests;
- `prompt` nonempty;
- `prompt_bytes == prompt.as_bytes().len()`;
- bytes помещаются в `i64`, но новый произвольный product limit здесь не вводится.

`swarm-contracts` не должен тянуть SQLite/process/runtime dependencies. Не добавлять `sha2` только ради DTO: producer и adapters уже имеют canonical/digest primitives. Формат digest проверяется в contract; содержание пересчитывается на effect boundary.

### 5.2 Один Store-owned builder

Добавить узкий helper в kernel-host, например:

```rust
pub(crate) fn build_task_prompt_v1(
    attempt: &Value,
    source_text: &str,
    launch_packet: Option<&Value>,
) -> Result<TaskPromptEnvelopeV1>;
```

Builder обязан:

1. проверить exact Attempt tuple;
2. взять `task_snapshot` из frozen Attempt;
3. потребовать object `task_snapshot.brief`;
4. canonicalize brief один раз через текущий `model::canonical`;
5. canonicalize и hash полный snapshot только для evidence;
6. сформировать prompt по одному фиксированному формату;
7. при наличии launch packet проверить его текущим validator и добавить exact canonical packet;
8. вычислить prompt digest/UTF-8 byte count;
9. вернуть DTO без чтения внешних файлов и без model call.

Рекомендуемый точный формат:

```text
{source_text}

ELIOT Task identity v1:
{"attempt_id":"...","task_id":"...","task_revision":N,"task_snapshot_sha256":"..."}

ELIOT Task brief v1:
{canonical snapshot.brief}

[если есть]
ELIOT Launch dispatch packet v1:
{canonical launch_dispatch_packet}
```

Порядок блоков и LF — часть contract. `prompt_bytes` — число UTF-8 bytes, не `String::chars().count()`.

`source_text` остаётся точным caller-owned Task input и продолжает участвовать в `TaskDispatchContext.source_text_sha256/bytes`.

## 6. Где строить и сохранять envelope

### 6.1 Admission, не adapter execution

Основной producer — `store/operations.rs::dispatch`.

Текущий код уже в одной transaction:

- проверяет Task/Attempt/binding;
- получает prerequisite;
- строит `launch_dispatch_packet`;
- пишет `effective_request_json`;
- связывает Attempt start Operation.

Именно здесь после `launcher_dispatch::prepare_admission` строится `TaskPromptEnvelopeV1` и сохраняется:

```json
{
  "route": {},
  "input": "original source text",
  "task_snapshot": {},
  "task_prompt": {},
  "launch_dispatch_packet": {}
}
```

Полный snapshot сохраняется в `effective_request_json` для readback/current-subject verification. Он **не должен** попадать в новый RuntimeCommand module payload.

### 6.2 Command projection

При построении `RuntimeCommand` для descriptor, который объявил `swarm.task_prompt` v1:

- скопировать trusted `task_prompt`;
- не копировать `task_snapshot`;
- не позволять original request подставить `task_prompt`;
- сохранить `TaskDispatchContext` и launch packet identities;
- записать `operation_contract.task_prompt.contract_revision = task-prompt-v1`.

Старый descriptor не получает новый envelope. Он остаётся только на своей исторической route/artifact semantics. Никакого runtime `if new fails -> render old snapshot`.

### 6.3 Удалить late enrichment

После включения нового producer убрать для migrated routes повторную сборку snapshot/prompt из:

- `store/runtime.rs`;
- `store/opencode.rs`;
- built-in runtime-specific `prompt(...)` helpers;
- adapter-local task renderers.

Store не должен сначала сохранить одно effective input, а при `module.next` придумывать другое.

## 7. Descriptor и artifact versioning

Добавить command schema descriptor:

```text
schema_id = swarm.task_prompt
version   = 1
```

Новый artifact opt-in требует одновременно:

- capability `task.dispatch`;
- `swarm.runtime_command`;
- существующий normalized dispatch schema pair, если artifact её заявляет;
- `swarm.task_prompt` v1.

Изменение command schema означает новую immutable artifact version/descriptor. Не менять body существующего artifact под прежним ID/version.

Минимальная проверка route selection:

```rust
fn selected_task_prompt_v1(binding: &Value) -> Result<bool>
```

Она читает только retained exact descriptor selector. Capability без schema не активирует v1; schema без `task.dispatch` также не активирует его.

Unknown prompt schema/version — fail-closed до native effect.

## 8. Adapter contract

Для migrated artifact adapter делает только следующее:

1. parse `TaskPromptEnvelopeV1`;
2. `validate_shape()`;
3. сверяет Task/Attempt/revision с authenticated RuntimeCommand и `TaskDispatchContext`;
4. пересчитывает `sha256(prompt.as_bytes())` и byte count;
5. сверяет `task_snapshot_sha256` с Store-supplied immutable identity/contract, где она доступна;
6. передаёт `prompt` нативному API **как есть**;
7. пишет тот же digest/bytes в `TaskDispatchAdmissionReceipt.native_payload_*`;
8. сохраняет `prompt_contract_revision` в outcome/readback details.

Adapter не:

- десериализует TaskSpec;
- выбирает source refs;
- сокращает текст;
- добавляет собственные process rules;
- переставляет блоки prompt;
- делает fallback к raw snapshot;
- вызывает LLM для summary.

## 9. Migration inventory

Перед кодом выполнить точный search всех task prompt consumers. На базовом SHA уже подтверждены:

### Store/built-ins

- `crates/swarm-kernel-host/src/store/operations.rs::dispatch`;
- `store/runtime.rs` command enrichment;
- `store/opencode.rs` command enrichment;
- `runtime/batch.rs::instruction`;
- `runtime/codex.rs`;
- `runtime/prepared.rs`;
- `runtime/opencode_v2/effects.rs`;
- `runtime/zed.rs`.

### Standalone Rust adapters

- `crates/swarm-adapter-opencode/src/native.rs`;
- `crates/swarm-adapter-command/src/native.rs`;
- `crates/swarm-adapter-codex/src/lib.rs`;
- `crates/swarm-adapter-claude/src/lib.rs`;
- `modules/antigravity-rust/src/wire.rs`.

### SDK/legacy bridges

- `modules/claude/bridge.mjs`;
- `modules/codex/controller.py`;
- `modules/antigravity/bridge.mjs`;
- `modules/command/glue.mjs`.

`modules/muse` проверить отдельно: он может получать уже подготовленный text и не должен получить второй renderer.

Новый artifact создаётся только там, где route остаётся production-supported. Historical bridge не мигрируется «для симметрии»: если у него нет будущего consumer, запретить новые binding и оставить readback его retained evidence.

## 10. Порядок реализации

Один manager/worktree владеет contract migration. Writers получают непересекающиеся файлы и не запускают Cargo.

### Slice A — shared contract + Store producer

- добавить DTO/schema helper в `swarm-contracts`;
- добавить Store builder;
- сохранять envelope в `operations.rs::dispatch`;
- exact same request возвращает retained envelope;
- changed source text остаётся conflict;
- пока нет consumer, этот slice **не сливать отдельно**.

### Slice B — один reference consumer

Выбрать artifact после стабилизации его текущего PR:

- OpenCode после R17/#43, либо
- Antigravity после R19/#45, либо
- Command ACP создаёт prompt-v1 сразу в R18/#44.

Consumer обязан пройти весь путь producer → RuntimeCommand → native request → admission receipt. После этого shared contract имеет живого caller.

### Slice C — остальные активные runtimes

Мигрировать по artifact version, не по глобальному feature flag. На каждый runtime:

1. новый exact descriptor;
2. adapter parser/use;
3. route qualification;
4. запрет новых binding на старый prompt artifact;
5. удаление соответствующего local renderer.

### Slice D — удалить дубли

После миграции последнего активного consumer:

- удалить `runtime/batch::instruction` старого формата;
- удалить `Task specification: {full snapshot}` builders;
- удалить JS/Python renderer у выведенных artifacts;
- убрать late snapshot enrichment;
- оставить historical readback, но не старый executor без consumer.

## 11. Concilium/review: вывод из статьи без нового workflow

Cogentic не является основанием расширять Concilium до model-driven consensus engine.

Текущий правильный раздел:

```text
review.submit findings
  -> manager disposition
  -> correction delivery
  -> new candidate
  -> independent review / CheckRunner
  -> acceptance
```

Concilium остаётся advisory recommendation/dissent record и не принимает Task. R07/#33 чинит digest, explicit manager ratify/reject и terminal lifecycle; не добавляет speaker-selection LLM, автоматические rounds или acceptance authority.

Отдельно обнаружено: current RepairDispatch принимает ровно один `finding_id`, а публичный `ChangeRequest`, semantic slot, link, receipt и validators также single-finding. Не лечить это конкатенацией текста. Multi-finding correction требует отдельного versioned `CorrectionPackage` contract и должна быть выполнена одним вертикальным срезом после R09/#35; R20 её не реализует.

## 12. Критерии готовности

### Contract/Store

- [ ] `TaskPromptEnvelopeV1` — единственная новая DTO; no unknown fields.
- [ ] Full snapshot остаётся в Attempt/Operation readback, но отсутствует в migrated RuntimeCommand payload.
- [ ] Envelope строится в admission transaction и не пересобирается при delivery/reconnect.
- [ ] Same Operation/retry возвращает byte-identical envelope.
- [ ] Changed text/Attempt/snapshot не coalesce с прежним native prompt.
- [ ] Client-supplied `task_prompt` отвергается.

### Prompt correctness

- [ ] Prompt содержит exact source text, Task identity, canonical brief и optional exact launch packet.
- [ ] UTF-8 example с non-ASCII/emoji даёт одинаковые digest/bytes в Store, adapter и receipt.
- [ ] Изменение `snapshot.brief` меняет prompt digest.
- [ ] Изменение только скрытой snapshot authority меняет snapshot digest, сохраняя читаемую brief projection.
- [ ] Malformed/mismatched envelope отвергается до native write.
- [ ] Oversize обрабатывается существующей native input boundary до эффекта; silent truncation запрещён.

### Versioning/deletion

- [ ] New artifact advertises schema v1 и не меняет прежнюю artifact identity.
- [ ] Old artifact не является fallback нового route.
- [ ] У каждого сохранённого старого executor указан реальный consumer и deletion condition.
- [ ] После миграции нет двух production renderers одного prompt contract.

### Product behaviour

- [ ] Модель получает compact Task brief, а не raw receipts/policy/baseline structures.
- [ ] Source index gaps остаются видимыми; renderer не выдумывает отсутствующие инструкции.
- [ ] CheckRunner/review/acceptance не заменены самооценкой модели.
- [ ] No second Store/ledger/orchestrator introduced.

## 13. Проверка

После связанного reference consumer — formatting только затронутых Rust-файлов и минимальный warnings-denied gate точных packages, например:

```sh
cargo clippy --locked \
  -p swarm-contracts \
  -p swarm-kernel-host \
  -p <migrated-adapter-package> \
  --lib --bins -- -D warnings
```

Не запускать broad workspace tests, native models или account-changing qualification до полной кодовой поставки. Transcript/integration qualification выполняется позднее на exact new artifact.

В PR report указать:

- base/head SHA;
- новый schema/artifact version;
- exact producer и consumer symbols;
- какие старые renderers удалены;
- результат scoped Clippy;
- какие native/live проверки ещё не выполнялись.

## 14. Зависимости и конфликты

- R17/#43 владеет OpenCode controls/history/background и меняет тот же standalone adapter.
- R18/#44 владеет новым Command ACP artifact; он должен начать сразу с prompt-v1, а не наследовать batch renderer.
- R19/#45 владеет Antigravity stream/model/usage.
- R05/#31 владеет normalized receipt binding.
- R09/#35 владеет review assignment lifecycle; multi-finding package — отдельный последующий contract.

Не вести параллельные writers в одних adapter files. Shared contract можно готовить, но окончательный rebase/consumer migration выполняет один manager после стабилизации соответствующей adapter branch.

## 15. Явные non-goals

- новый multi-agent research framework;
- Cogentic/Magentic-One port;
- automatic task decomposition;
- LLM summary of Task;
- progress/completion judgment by LLM;
- новый SQLite ledger/table только для prompt;
- изменение Task authority или acceptance;
- silent source truncation;
- hidden fallback/compatibility shim;
- переписывание historical Attempt snapshots;
- изменение пользовательских credentials, services или model routes.
