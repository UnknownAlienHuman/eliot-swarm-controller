# R24. MCP live authorization: каждое dispatch пересекается с current allowed_methods до IPC

**Статус:** implementation handoff. В текущей ветке production-код ещё не изменён.

**База проверки:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Перед реализацией сравнить актуальный `main`; уже исправленное не переписывать.

## 1. Подтверждённый разрыв

`ProfiledFacade` уже правильно разделяет две вещи:

```text
session-fixed frontend profile/surface
current application authorization from Store
```

`tools/list` запрашивает `mcp.authorization`, получает `authorization_revision + allowed_methods` и пересекает каталог с current Store scope. `swarm.tools.search` делает то же.

Но обычный `tools/call` проверяет только:

```rust
profiles::exposes_method(self.profile, spec.method)
```

и затем вызывает `inner.call_tool`, не проверяя current `allowed_methods`.

Та же проблема у:

- `tasks/get`: только `profiles::allows_task_get`;
- `tasks/cancel`: только `profiles::allows_task_cancel`;
- `eliot/subscribe`: только `profiles::allows_subscription_category`.

Следовательно, метод, скрытый live Store authorization после revoke/role/scope/GM change, всё равно отправляется в IPC. Store application guard может отказать, поэтому это **не автоматически mutation authorization bypass**. Но нарушена заявленная гарантия frontend:

```text
request outside selected + current surface is rejected before IPC
```

Это важно для restricted agents: hidden method не должен даже попадать в host queue, создавать rejected Operation, раскрывать различие object existence/error или нагружать single writer.

## 2. Результат

Every frontend dispatch uses one shared preflight:

```text
session-fixed profile exposure
AND current Store allowed_methods
AND method-specific facade prerequisites
→ only then IPC/application call
```

Store remains authoritative and rechecks current principal/object during the real call. Preflight is defense-in-depth, not an authorization lease or snapshot grant.

No cache, new token, extra approval, external policy engine or second method registry.

## 3. Existing authoritative source

Reuse current:

```rust
ProfiledFacade::catalog_authorization(task_id)
parse_catalog_authorization
CatalogAuthorization {
    revision,
    task_id,
    allowed_methods,
}
```

Do not create `live_allowed_methods_v2`, duplicate Store query or read client registration directly from MCP.

Add one helper:

```rust
impl ProfiledFacade {
    async fn require_live_methods<'a>(
        &self,
        methods: impl IntoIterator<Item = &'a str>,
        task_id: Option<&str>,
    ) -> Result<CatalogAuthorization, McpError>;
}
```

Rules:

1. call `catalog_authorization(task_id)` once;
2. every required method must be present;
3. otherwise return method-not-found for the user-visible requested method;
4. auth read/parse failure returns current existing internal authorization error;
5. returned revision is diagnostic correlation only, never attached as permission proof to the later Store call.

For ordinary `tools/call`, `task_id=None` is sufficient for method membership. Object authorization remains inside target handler. Do not infer Task from arbitrary JSON fields or dynamically change method authority based on untrusted request body.

`swarm.tools.search` keeps its current task-scoped authorization because search surface itself accepts exact task context.

## 4. tools/call order

Required sequence:

```text
find tool
→ profile exposes method
→ live Store allowed_methods contains method
→ validate restricted-profile client_request_id for mutation
→ validate/refine tool arguments as current path does
→ inner.call_tool / IPC
```

Do not perform IPC for target method before live preflight. The authorization query itself is IPC to the exact read-only `mcp.authorization`; the guarantee means no **target application method** reaches IPC.

Mutation `client_request_id` may be validated before or after live membership, but error precedence must be stable and must not leak method membership to a profile that does not expose it. Recommended:

1. profile method-not-found;
2. live method-not-found;
3. argument/client-request validation.

Full profile is not exempt. `full` means full frontend registry, not bypass of current Store role/GM/scope.

## 5. Tasks extension

### tasks/get

Before `inner.get_task` require current:

```text
operation.get
report.attention
```

These are the exact application reads the Tasks projection uses. If either is absent, return `tasks/get` method-not-found before its inner requests.

After preflight, inner calls still recheck `operation.get/report.attention`; no TOCTOU authority is assumed.

### tasks/cancel

Before reading target Operation require current:

```text
operation.get
operation.cancel
```

Then keep existing state logic and caller-owned request ID rule. If authority changes between preflight and cancellation, Store refusal is returned; facade does not retry.

Do not generate a cancel request ID for restricted profiles. Current full-profile compatibility may remain only under its documented contract; R24 does not broaden it.

## 6. Subscriptions

`eliot/subscribe` currently verifies only profile category. Add one current authorization read before the subscription pump starts.

Required methods:

| Category | Current required application methods |
|---|---|
| Reports | `report.delta` |
| Mailbox | `report.delta`, `message.read` |
| Operations | `report.delta`, `operation.get` |
| Concilium | `report.delta` plus at least one exact supported `concilium.get/list` according to existing category rule |
| Coordination | `report.delta` and all exact Thread/Contract reads used by resync |

Use one union of methods for all requested categories and one `catalog_authorization(None)` call. Do not query once per category.

If current authorization is absent:

- reject subscribe before spawning/registering pump state;
- do not allocate subscription ID;
- do not open a second host link.

`eliot/unsubscribe` only releases session-local subscription state and does not need application method authorization. It still requires valid session-owned subscription ID.

R08/#34 owns cursor/resync completeness; R24 only gates start of the pump.

## 7. list/search consistency

After implementation, three surfaces use the same current membership:

```text
tools/list
swarm.tools.search
tools/call
```

`authorization_revision` may change between list and call. That is expected: call checks current membership again. A method listed earlier can later return method-not-found before target IPC after revoke.

Do not cache allowed_methods for MCP session lifetime. The Store query is bounded and already required by design. Performance optimization, if measured, may cache only by exact authorization_revision with explicit invalidation; not in R24.

## 8. Application authority remains mandatory

Live allowed_methods is method-level membership, not object authorization.

Examples:

- Manager may have `operation.get` but R23 resolver can return NOT_FOUND for unrelated Operation;
- Participant may have `review.submit` but exact assignment/slot guard can reject;
- Manager may have `swarm.launch` but Task/Attempt/lease/route admission can reject;
- current GM membership does not waive epoch/object checks.

Do not move Task/Attempt/binding checks into MCP or infer success from `allowed_methods`.

## 9. Error semantics

| Condition | Frontend result | Target IPC? |
|---|---|---|
| tool absent from registry/profile | method-not-found | no |
| profile exposes, live Store membership absent | method-not-found | no |
| authorization read unavailable/malformed | internal authorization error | no target call |
| live membership present, object later unauthorized | Store application error/NOT_FOUND | yes, one target call |
| authority revoked after preflight before Store apply | Store application error | yes, no retry |

Do not translate application FORBIDDEN/CONFLICT into method-not-found after dispatch. Pre-dispatch and application denial remain distinguishable.

## 10. Tests must prove before-IPC, not bare `is_err()`

Current test class in the audit often proves only an error, not where it happened.

Focused harness must instrument fake/local host request count or exact method log.

### Required cases

1. Profile-hidden method:
   - error method-not-found;
   - zero `mcp.authorization` target? Profile check may avoid even auth;
   - zero target method IPC.
2. Profile-visible, live-disallowed method:
   - exactly one `mcp.authorization` read;
   - zero target method IPC.
3. Live-allowed method:
   - authorization read;
   - exactly one target method IPC.
4. Authority revoked after `tools/list`:
   - list contained method;
   - later call gets method-not-found;
   - zero target method IPC.
5. Full profile with role-disallowed method:
   - still live-denied before target IPC.
6. Tasks get/cancel missing one dependency:
   - no inner application request.
7. Subscription category live-denied:
   - no Entry/pump/subscription ID; no polling request.
8. Authorization service error:
   - fail closed; no target call.

Use public `ServerHandler`/MCP path, not only helper tests. Assertions include exact forwarded method sequence.

## 11. Files/symbols

Primary:

- `crates/swarm-mcp/src/mcp/mod.rs`:
  - `ProfiledFacade::catalog_authorization`;
  - new `require_live_methods`;
  - `ServerHandler for ProfiledFacade::{call_tool,get_task,cancel_task,on_custom_request}`.
- `crates/swarm-mcp/src/mcp/profiles.rs` remains session-fixed profile data; no live Store logic added there.
- `subscription_contract_tests.rs`, `frontend_contract_tests.rs`, MCP integration fixtures: target-method IPC counters/logs.
- docs `mcp-profiles.md`, catalog/loading only where current claims need exact wording.

Store `mcp_authorization` stays the single producer. R14/#40 later extracts schema/catalog data; R24 does not move crates or rebuild registry.

## 12. Removal / simplification

After migration:

- remove direct normal-call path from `ProfiledFacade` that checks only `exposes_method`;
- remove duplicate profile-only Tasks/category helpers from dispatch decisions where shared `required_methods` result replaces them; keep pure profile exposure helpers for session-fixed surface construction;
- remove tests that assert only `is_err()` while claiming before-IPC;
- no second allowed-method cache/list.

Do not remove Store application checks or profile allowlist.

## 13. Donors

Primary donor is current own `tools/list/search` implementation: it already has correct bounded producer and parser. Reuse it.

MCP protocol itself distinguishes discovery from invocation; discovery result is not a permanent grant. No external MCP proxy or gateway is needed.

AgentGateway/CEL/OpenFGA would duplicate method membership and still require current Store scope. Do not add them.

## 14. Criteria

- [ ] Every normal `tools/call` intersects profile + current allowed_methods before target IPC.
- [ ] Full profile does not bypass live role/scope membership.
- [ ] `tools/list`, search and call converge on the same method membership producer.
- [ ] Revocation between list and call is enforced.
- [ ] Tasks/get and cancel check their exact dependent methods.
- [ ] Subscription starts only after one successful union preflight.
- [ ] No target call, subscription/pump state or Operation on live denial.
- [ ] Store object authorization remains unchanged and authoritative.
- [ ] Tests prove forwarded method count/order, not only error result.
- [ ] No authorization cache, second registry or external policy engine.

## 15. Implementation order

One manager/worktree. Writers receive non-overlapping files and do not run Cargo.

1. Add `require_live_methods` using existing parser.
2. Wire ordinary `call_tool`.
3. Wire Tasks methods.
4. Wire subscription admission with one method union.
5. Add public-boundary forwarding tests.
6. Update docs and remove profile-only dispatch duplication.
7. Scoped formatting and minimal Clippy.

Do not merge helper-only code without all four production callers.

## 16. Minimal gate

```sh
cargo clippy --locked \
  -p swarm-mcp \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Broad/full integration tests remain final phase. PR report names exact forwarded-method fixtures, base/head SHA, removed direct paths, Clippy result and tests not run.

## 17. Non-goals

- object authorization in MCP;
- caching current authorization;
- new method registry;
- changing Store role/GM logic;
- changing Tasks semantics;
- subscription cursor/resync repair;
- remote gateway policy;
- automatic retry on authorization races;
- compatibility alias or fallback to profile-only dispatch.
