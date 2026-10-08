# R31. Module command gate: неизвестный RuntimeCommand не считается поддержанным

**Статус:** implementation handoff. Текущий diff содержит только это задание; production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленные участки не переписывать.

## 1. Результат

Каждый метод, который Store способен передать модулю как `RuntimeCommand`, принадлежит одному закрытому registry и имеет точное descriptor requirement.

```text
Store mutation / retained Operation
  -> closed RuntimeCommand classification
  -> Store authorization + object/binding guards
  -> exact selected-descriptor capability check
  -> module.next / native effect
```

Неизвестный или забытый в registry метод отклоняется **до** выдачи команды модулю:

```text
MODULE_COMMAND_UNMAPPED
```

Descriptor остаётся compatibility contract, а не выдаёт Store authority. Legacy binding без selector сохраняет старое поведение только для явно известных legacy `agent.*` methods. Native MCP и будущие descriptor-only commands без exact selector запрещены.

Никакого нового plugin framework, динамического DSL, общего workflow engine или auto-discovery методов из module claim.

## 2. Подтверждённая поломка

### 2.1 Unknown mapping сейчас означает success

`store/module_handshake.rs::selected_native_command_supported`:

```rust
let Some(required) = native_command_capability(method, input) else {
    return Ok(Some(true));
};
```

Затем `require_selected_native_command` принимает:

```rust
None | Some(true) => Ok(())
```

Поэтому command method, который дошёл до этого guard, но не внесён в `native_command_capability`, считается поддержанным:

- selected descriptor не проверяется;
- capability не требуется;
- command schema не требуется;
- legacy/descriptor distinction обходится.

Это fail-open seam. Сейчас application mutation/parser и runtime dispatcher используют закрытый набор методов, поэтому произвольная строка с провода не становится командой автоматически. Но добавление нового Store-dispatched method без синхронной правки mapping даст его **всем** selected modules как поддерживаемый. Дефект является реальной regression boundary, а не доказательством уже существующего внешнего метода.

### 2.2 Одно множество команд описано несколько раз

Текущий набор повторяется в разных формах:

1. `module_handshake.rs::native_command_capability` — 11 `agent.*`/Task methods + native MCP.
2. `module_credential.rs::MODULE_OPERATIONS` — 15 methods.
3. `module_demand.rs::MODULE_METHODS` — 11 methods.
4. `module_demand.rs::operation_candidates` — SQL literal того же набора.
5. `runtime.rs::user_command_with_actor` — method-specific validation/match.
6. Store operation routing/wake sets.
7. Adapter descriptor capability arrays.

Новый method легко добавить в dispatcher/Operation list и забыть в descriptor gate. Текущий fallback делает такую забывчивость незаметной.

### 2.3 `agent.send` содержит второй dimension

Capability зависит не только от method:

```text
agent.send + next_turn -> agent.send/next_turn
agent.send + steer     -> agent.send/steer
```

Сейчас mapping имеет generic fallback `agent.send` для отсутствующего/неизвестного delivery. Store позже валидирует `next_turn | steer`, но правильный contract не должен полагаться на порядок двух независимых функций. Classification получает уже разобранный delivery enum либо сам возвращает typed invalid/unmapped result.

### 2.4 Native MCP уже имеет отдельный closed set

`swarm-contracts::native_mcp::NATIVE_MCP_METHODS` содержит четыре метода и exact `NativeMcpPhase::method()`. Для них selected descriptor обязан иметь:

- exact method capability;
- `swarm.native_mcp_command` schema;
- validated enriched command bound to outer `RuntimeCommand`.

Legacy binding без descriptor для native MCP уже запрещён. R31 сохраняет это и убирает возможность обойти его через новый забытый method.

## 3. Нормативная граница

Trusted module descriptor сообщает, что установленный adapter **совместим** с конкретной командой. Он не:

- разрешает caller-у вызвать method;
- создаёт Task/Attempt/binding scope;
- заменяет Store method policy;
- разрешает неизвестные команды;
- расширяет host protocol по module claim;
- позволяет module самостоятельно рекламировать произвольный эффект.

Порядок остаётся:

```text
application authorization
-> exact Operation/object/binding admission
-> closed host command classification
-> selected descriptor compatibility
-> RuntimeCommand
```

Legacy route exception ограничена известным историческим `agent.*` contract. Отсутствие selector — не универсальный fallback для будущих команд.

## 4. Один маленький data-only registry

Добавить закрытый data-only command classifier в существующий общий contract owner, предпочтительно `swarm-contracts/src/runtime.rs` или новый небольшой `runtime_command.rs`, а не в новый crate.

Предлагаемая форма:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCommandKind {
    AgentOpen,
    TaskDispatch,
    AgentSendNextTurn,
    AgentSendSteer,
    AgentReply,
    AgentConfigure,
    AgentGoal,
    AgentBackground,
    AgentRefresh,
    AgentReconcile,
    AgentResult,
    AgentRecover,
    NativeMcp(NativeMcpPhase),
}

impl RuntimeCommandKind {
    pub const fn method(self) -> &'static str;
    pub const fn capability(self) -> &'static str;
    pub const fn requires_selected_descriptor(self) -> bool;
    pub const fn required_command_schema(self) -> Option<SchemaDescriptorRef>;
}
```

Exact type names may differ. Required semantics may not.

Classification input must not be arbitrary JSON after the first boundary. Use one small enum for send delivery:

```rust
pub enum AgentSendDelivery {
    NextTurn,
    Steer,
}
```

Store already validates `delivery`; parse it once and pass typed variant to classifier. Do not add `Unknown(String)` that later defaults to generic support.

If moving `NativeMcpPhase` into the enum introduces a dependency cycle, keep native MCP classification as an exact adjacent branch using its existing `NATIVE_MCP_METHODS`. Do not duplicate its four strings again.

## 5. Public/internal functions

Replace `native_command_capability(...) -> Option<&str>` with an exact result:

```rust
pub fn classify_runtime_command(
    method: &str,
    send_delivery: Option<AgentSendDelivery>,
) -> Result<RuntimeCommandKind, RuntimeCommandClassificationError>;
```

Errors:

```text
UnknownMethod
MissingSendDelivery
UnexpectedSendDelivery
```

Store maps them before effect:

- unknown method → `MODULE_COMMAND_UNMAPPED`;
- malformed method-specific input remains ordinary `INVALID_PARAMS` from request parsing;
- internal method/input mismatch → `STORE_INVARIANT` only when caller input was already validated and impossible state is retained.

Descriptor support should no longer use `Option<bool>`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DescriptorCommandSupport {
    LegacyKnownCommand,
    Supported,
    Unsupported,
}
```

or an equivalent explicit enum.

Meaning:

- `LegacyKnownCommand`: exact known legacy `agent.*`/Task command and no retained selector;
- `Supported`: selected descriptor has exact capability and any required schema;
- `Unsupported`: exact known command, but selected descriptor is incompatible or descriptor-only command lacks selector.

Unknown methods never become an enum member; they return error before support resolution.

## 6. Exact support matrix

| Command | No selector (legacy binding) | Selected descriptor |
|---|---|---|
| `agent.open` | legacy allowed | capability `agent.open` |
| `task.dispatch` | legacy allowed | capability `task.dispatch` |
| `agent.send/next_turn` | legacy allowed | exact capability or documented broad `agent.send` compatibility |
| `agent.send/steer` | legacy allowed only where existing legacy route contract supports it | exact capability or documented broad `agent.send` compatibility |
| `agent.reply` | legacy allowed | capability `agent.reply` |
| `agent.configure` | legacy allowed | capability `agent.configure` |
| `agent.goal` | legacy allowed | capability `agent.goal` |
| `agent.background` | legacy allowed | capability `agent.background` |
| `agent.refresh` | legacy allowed | capability `agent.refresh` |
| `agent.reconcile` | legacy allowed | capability `agent.reconcile`; target compatibility checked separately |
| `agent.result` | legacy allowed | capability `agent.result`; selector contract checked separately |
| `agent.recover` | legacy allowed | capability `agent.recover` |
| `native.mcp.*` | forbidden | exact phase capability + exact native MCP command schema |
| unknown/future method | forbidden | forbidden |

The table describes compatibility only. Existing route-specific validation may legitimately reject a known legacy command on a particular artifact; R31 does not broaden it.

### Broad `agent.send` capability

Current `capability_satisfies` allows advertised `agent.send` to satisfy `agent.send/<delivery>`. Preserve only if this is an intentional descriptor compatibility rule documented by current artifacts. Add tests demonstrating it. Do not extend the same prefix rule to other command families.

## 7. Store integration

### 7.1 Admission

In `store/runtime.rs::user_command_with_actor`:

1. parse/validate method-specific request exactly once;
2. derive `RuntimeCommandKind`;
3. run object/authority/binding/prerequisite checks;
4. call descriptor support with the typed kind;
5. persist effective command identity/digest;
6. only then expose to module demand/`module.next`.

Do not infer kind again from retained arbitrary JSON in downstream layers. Retain a bounded classifier projection if readback needs it:

```json
{
  "runtime_command": {
    "kind": "agent_send_next_turn",
    "capability": "agent.send/next_turn"
  }
}
```

This projection is not authority; on readback, recompute/compare from original request rather than trust it alone.

### 7.2 Initial operations

`agent.open` and `task.dispatch` have separate Store handlers but call the same command gate. They must use the same registry, not local literals.

### 7.3 Reconcile

`agent.reconcile` validates two kinds:

- current reconcile command itself;
- exact target Operation command.

Both must classify successfully. A historical Operation carrying an unknown method is not replayed or reconciled through a guessed adapter contract; return explicit unsupported/damaged readback without another native effect.

### 7.4 Native MCP

Use `NativeMcpPhase` as authority for exact method/phase relation. Do not map strings independently in Store and adapter.

## 8. Demand and credential provisioning

### 8.1 Canonical known method set

Expose an ordered constant/list from the registry for host-owned scans:

```rust
pub const MODULE_RUNTIME_METHODS: &[&str];
```

It contains base method names once; send delivery variants remain classifier-level.

`module_credential::MODULE_OPERATIONS` and `module_demand::MODULE_METHODS` must not define independent arrays.

### 8.2 SQL candidate query

SQLite SQL cannot import a Rust const directly. Choose one of these narrow implementations:

1. build a fixed placeholder list from `MODULE_RUNTIME_METHODS` and bind every value; or
2. pass the canonical method array as JSON and join `json_each(?methods)`; or
3. retain one SQL literal only if a test parses/compares it byte-for-byte with the canonical set.

Prefer bound values/`json_each` if query-plan inspection remains bounded and indexed. Do not remove the method predicate and scan every pending Operation in Rust.

### 8.3 Provisioning

Credential provisioning accepts only an already-admitted Operation whose method classifies through the same registry. A selected module credential is never issued for an unmapped method.

### 8.4 Demand block

If historical/current data contains an unmapped pending method, module demand should return an explicit bounded block:

```json
{
  "error_code": "MODULE_COMMAND_UNMAPPED",
  "operation_id": "...",
  "binding_id": "...",
  "generation": 1
}
```

It must not start an adapter and hope the module rejects it. One damaged Operation also must not abort demands for unrelated bindings; integrate with R12/#38 source isolation rather than invent another scheduler.

## 9. Adapter descriptor relation

Adapters continue to publish concrete capabilities. R31 does not change their behavior unless an existing descriptor is inconsistent with commands the adapter actually handles.

For each production artifact, compare:

```text
adapter command match arms
module descriptor capabilities
qualification manifest capabilities
Store registry
```

Exact discrepancies become artifact-version fixes, not host fallback aliases.

Do not infer capabilities by inspecting JavaScript/Rust match arms at runtime. This is a source/CI comparison.

## 10. Tests

### Classification

- [ ] Every known base method classifies.
- [ ] Unknown method returns `UnknownMethod`, never success.
- [ ] `agent.send` without delivery fails classification.
- [ ] `agent.send/next_turn` and `/steer` produce distinct kinds/capabilities.
- [ ] Invalid delivery never falls back to generic `agent.send`.
- [ ] Every `NativeMcpPhase::method()` classifies as exact native MCP kind.

### Descriptor support

- [ ] Unknown method + selected descriptor → `MODULE_COMMAND_UNMAPPED`.
- [ ] Unknown method + legacy binding → `MODULE_COMMAND_UNMAPPED`.
- [ ] Known legacy command + no selector preserves current legacy path.
- [ ] Native MCP + no selector is unsupported.
- [ ] Selected descriptor missing exact capability is unsupported.
- [ ] Exact capability is supported.
- [ ] Native MCP additionally requires exact command schema.
- [ ] Broad `agent.send` compatibility, if retained, is tested for only the two send variants.

### End-to-end public Store

- [ ] A synthetic new native mutation added to the application router but omitted from command registry is rejected before `module.next` and before module credential provisioning.
- [ ] No RuntimeCommand/adapter demand appears for the rejected Operation.
- [ ] A normal known command still produces exactly one demand/command.
- [ ] Reconcile of an unmapped target creates no native retry.
- [ ] One unmapped pending Operation does not hide a valid demand on another binding.

### Registry equivalence

- [ ] Module credential method set equals registry base methods plus exact native MCP set.
- [ ] Module demand candidate set equals the same canonical set.
- [ ] Store runtime dispatch arms have no command absent from registry.
- [ ] Production adapter descriptor capabilities are subsets/supersets according to their actual command handlers; exact expected relation documented per artifact.
- [ ] Adding a method to one list without registry update makes CI fail.

## 11. Files and symbols

Primary:

- `crates/swarm-contracts/src/runtime.rs` or one small adjacent command-registry module;
- `crates/swarm-contracts/src/native_mcp.rs` — reuse, do not duplicate phase/method list;
- `crates/swarm-kernel-host/src/store/module_handshake.rs`;
- `crates/swarm-kernel-host/src/store/runtime.rs`;
- `crates/swarm-kernel-host/src/store/operations.rs` — initial dispatch/open gates and recovery target classification;
- `crates/swarm-kernel-host/src/store/module_credential.rs`;
- `crates/swarm-kernel-host/src/store/module_demand.rs`.

Artifact consistency checks may touch:

- `crates/swarm-adapter-{opencode,codex,command,claude}/...module*`;
- `modules/antigravity-rust/src/contract.rs`;
- qualification manifest/tooling.

Do not change adapter runtime behavior unless the consistency pass proves an actual artifact mismatch.

## 12. What to delete

After migration:

- `native_command_capability(...) -> Option<_>`;
- `None | Some(true) => Ok(())` support branch;
- generic `agent.send` fallback for malformed/missing delivery;
- duplicate `MODULE_OPERATIONS` and `MODULE_METHODS` arrays;
- hand-maintained native MCP method strings outside `native_mcp`;
- local method lists whose only purpose is adapter demand/credential classification;
- comments claiming descriptor exactness while unknown commands still pass.

Do not delete route-specific command validators; they prove different facts.

## 13. Relationship to other PRs

- R01/#27 owns module lifecycle, process identity, hello/restart and installed artifact loading. R31 owns command compatibility/fail-closed mapping.
- R14/#40 owns frontend/schema source-of-truth. It may later generate application method metadata, but R31 must not wait for a global registry rewrite.
- R17/#43, R18/#44 and R19/#45 own concrete adapter capabilities. They consume the closed command contract after rebase.
- R12/#38 owns poison isolation/fair scheduling. R31 reports one command block; it does not build a second scheduler.
- R24/#50 owns live application `allowed_methods`; descriptor capability is a separate adapter-compatibility check.

One manager/worktree for shared `module_handshake.rs`/`runtime.rs`. Do not run parallel writers across these PRs.

## 14. Implementation order

1. Add closed data-only command kinds/classifier.
2. Replace module handshake Option-based mapping with explicit support enum/error.
3. Connect `agent.open`, `task.dispatch` and generic user commands.
4. Connect reconcile target classification.
5. Replace credential/demand method arrays and SQL predicate.
6. Add registry-equivalence and public Store tests.
7. Compare production descriptors/qualification manifests; correct only proven mismatches.
8. Delete old fallback/list copies.
9. Scoped formatting and minimal Clippy.

No broad/native qualification until the connected code slice is complete.

## 15. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-contracts \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then exact command-gate/Store tests named in the implementation report. Adapter packages enter the gate only if their descriptor source changed.

## 16. Non-goals

- grant application authority from descriptors;
- remove legacy bindings immediately;
- infer methods from module hello;
- dynamic capability/plugin DSL;
- global method-registry rewrite;
- new module protocol version solely for this fix;
- auto-enable disabled descriptors;
- add compatibility aliases or fallback methods;
- change native effect semantics;
- replay unknown Operations;
- treat a module's runtime rejection as an acceptable host-side gate.
