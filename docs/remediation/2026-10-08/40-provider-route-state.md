# R40. Provider conditions and route admission: typed facts before launch, no text-driven fallback

**Status:** implementation handoff. Production code, route configuration and active services are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). R15/#41 is the upstream native-usage producer task; this task is its Store/launcher consumer.

## 1. Result

A native provider/account/model condition is retained as one typed Store fact bound to its exact scope. Every new root admission asks one helper before any workspace/process/model effect. Queue and dashboard projections call the same helper in read-only mode.

```text
native authenticated usage/error event
→ ProviderCondition fact with exact scope + source sequence
→ one Store row per scope
→ route_admission(scope, route policy, current active roots)
→ Admit | Hold(until/reason) | Unavailable(reason)
→ launch/WorkDispatch/queue use the same decision
```

This slice does not buy credits, inject API keys, change accounts, switch provider/model without configured policy, restart services or introduce another scheduler.

## 2. Confirmed current mismatch

### 2.1 Store collapses rate limit and exhausted quota into one incident

`store/capacity.rs::quota_code` scans untyped JSON strings/status and maps every recognized `QUOTA*`, `RATE_LIMIT*`, `TOO_MANY_REQUESTS` and numeric HTTP 429 into one open `quota:<scope>` incident.

Consequences:

- a temporary request-rate limit and a weekly/subscription exhaustion become the same state;
- reset/retry metadata is copied as arbitrary JSON rather than interpreted by condition type;
- an unrelated later `Applied | Accepted` outcome on the same scope resolves the incident, even if it began before the negative condition or exercised another model/action;
- string whitespace, string status `"429"` and vendor-specific structured forms drift between producers and this Store parser.

Text classification inside Store is the wrong boundary. The adapter/native reader knows the actual vendor event contract and must emit the typed class.

### 2.2 `capacity_available` is a report fact, not an admission limit

`capacity_items` returns `capacity_available=true` only when:

```text
roster known
AND new_work enabled
AND no quota incident
AND pending_admissions > 0
```

It does not compare active/effective writers to any configured limit. Once a reservation becomes active and no pending admission remains, the value becomes false. Five active roots plus one pending reservation can still report true.

Launcher projections display this field, but launch and WorkDispatch do not use it as a final admission predicate. The name therefore invites a false interpretation that the route has a free slot.

### 2.3 Route configuration has no availability/limit policy

`config::Route` currently contains alias, runtime, module artifact, enabled flag, native options, optional workspace option and optional owned service. It cannot express:

- maximum concurrent root managers for a paid/shared route;
- hold-until behavior for temporary rate limits;
- disabled-until-readback behavior for exhausted quota, missing auth/data policy or removed model;
- explicit fallback route order.

R40 adds only the condition and admission core. Automatic fallback remains disabled unless a later owner-policy slice explicitly configures and records it.

## 3. One small provider condition contract

Add a data-only type under `swarm-contracts`; it has no Store, process or vendor dependency:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderCondition {
    Available,
    RateLimited { retry_at_ms: Option<i64> },
    QuotaExhausted { reset_at_ms: Option<i64> },
    ModelGone,
    DataPolicyRequired,
    AuthRequired,
    Overloaded { retry_at_ms: Option<i64> },
    Unknown { native_class: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConditionFact {
    pub schema_version: u16,
    pub binding_id: String,
    pub binding_generation: i64,
    pub route_alias: String,
    pub native_scope_key: String,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
    pub model_id: Option<String>,
    pub condition: ProviderCondition,
    pub observed_at_ms: i64,
    pub native_sequence: String,
}
```

Validation:

- positive generation;
- nonempty bounded route/scope/sequence;
- nonnegative timestamps;
- retry/reset not before observed time;
- bounded native class; no raw error text;
- optional identity values are nonempty when present.

Do not make this a general vendor-error framework. It is one condition fact consumed by route admission.

## 4. Producer boundary

R15/#41 supplies the first two real producers:

- Codex rate-limit/account stream;
- Muse usage read + changed notifications.

Adapters/native modules classify only from documented structured fields and exact fixture transcripts. Store never searches error messages for quota/rate words.

Other adapters may add producers later through the same fact after their native contracts are source-reviewed. Do not create empty per-vendor stubs in this PR.

Producer rules:

1. emit after authenticated binding/generation and route identity are known;
2. preserve native monotonic event/revision identity in `native_sequence`;
3. emit `Available` only from an authoritative current read or a newer explicit recovery signal, not from any successful tool/model outcome;
4. a missing field means unavailable evidence, not an invented default;
5. subscription/login remains native; no API-key or billing path is introduced.

## 5. Store representation

Use one ordinary table in the existing SQLite Store instead of another JSON index tree:

```sql
CREATE TABLE provider_conditions (
    scope_key TEXT PRIMARY KEY,
    route_alias TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    binding_generation INTEGER NOT NULL,
    native_scope_key TEXT NOT NULL,
    provider_id TEXT,
    account_id TEXT,
    model_id TEXT,
    condition_kind TEXT NOT NULL,
    retry_at_ms INTEGER,
    reset_at_ms INTEGER,
    observed_at_ms INTEGER NOT NULL,
    native_sequence TEXT NOT NULL,
    source_observation_id INTEGER NOT NULL,
    details_digest TEXT NOT NULL
);
```

`scope_key` uses the same structured scope identity corrected by R13/#39; do not rebuild it independently from loosely concatenated runtime/service strings.

One Store-private typed row parser/writer:

```text
apply newer exact fact
same sequence + same digest → no-op
same sequence + different digest → conflict/damaged
older sequence/time → stale, retain current
scope/binding mismatch → reject
```

The observation and current-row update commit in one transaction. `incidents` remains a diagnostic/history projection; it is not route authority.

Do not store raw vendor error bodies in this table.

## 6. Route policy: only what admission needs

Extend trusted local `config::Route` with one optional closed policy:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteAdmissionPolicy {
    pub max_concurrent_roots: u16,
}
```

Rules:

- absent policy preserves current unlimited behavior but projections say `limit_not_configured`, not `available`;
- zero is invalid; use `enabled=false` to disable a route;
- count only exact unreleased root bindings/owned service starts attributable to this route/scope;
- children/subagents inside one native root do not consume another root slot;
- unknown/damaged resource evidence makes admission `Hold`, never a guessed free slot.

Do not add automatic fallback, cost purchase, time-based route re-enable job or dynamic policy DSL in this slice.

## 7. One Store-private decision helper

Implement one helper used by final admission and read projections:

```rust
enum RouteAdmissionDecision {
    Admit,
    Hold { code: &'static str, until_ms: Option<i64> },
    Unavailable { code: &'static str },
}

fn route_admission(
    db: &Connection,
    route: &Route,
    exact_scope: &ResourceScope,
    now_ms: i64,
) -> Result<RouteAdmissionDecision>;
```

Decision table:

| Input | Decision |
|---|---|
| route disabled/missing | `Unavailable(ROUTE_DISABLED_OR_MISSING)` |
| resource roster/evidence damaged | `Hold(ROUTE_CAPACITY_UNKNOWN)` |
| configured root limit reached | `Hold(ROUTE_ROOT_LIMIT_REACHED)` |
| no condition fact yet | `Admit`, with projection `condition_unobserved`; preserves current behavior |
| `Available` | `Admit` if limit permits |
| `RateLimited`/`Overloaded`, future retry | `Hold` until exact time |
| same condition after retry time but no recovery read | `Hold` and request producer refresh; time alone does not fabricate Available |
| `QuotaExhausted` | `Hold`; include reset time if known, but require newer readback before Admit |
| `ModelGone` | `Unavailable(MODEL_GONE)` |
| `AuthRequired` | `Unavailable(AUTH_REQUIRED)` |
| `DataPolicyRequired` | `Unavailable(DATA_POLICY_REQUIRED)` |
| `Unknown` | `Hold(PROVIDER_CONDITION_UNKNOWN)` |

Clock passage wakes/readies a refresh; it does not mutate the condition itself.

## 8. Wire the real production path

Connect in this order:

1. R15 producers and Store ingestion of the typed fact.
2. `swarm.launch.preview`: report the exact decision and source observation.
3. `swarm.launch` final transaction: recompute the decision immediately before workspace/process reservation.
4. WorkDispatch admission: consume the same helper; retained subject becomes pending/held, not skipped or terminal.
5. direct task-dispatch/open routes that can allocate a new root: use the same final gate.
6. `swarm.queue.get`, dashboard, capacity and attention: project the helper result; do not infer from the old `capacity_available` boolean.

A preview is diagnostic only. Final admission always recomputes current condition/resource facts.

## 9. Replace misleading capacity semantics

After callers switch:

- remove Store-side `quota_code` text/status classifier;
- stop resolving provider condition from an arbitrary Applied/Accepted outcome;
- keep incident history as a projection generated from typed condition transitions;
- rename or remove `capacity_available`; if retained for compatibility during the same migration, project it as `pending_admission_recorded` and provide a named deletion condition;
- delete any launcher interpretation that treats report capacity as a permission.

Do not run two authorities indefinitely.

## 10. Donor mechanisms

- **Kubernetes controller/workqueue:** condition changes trigger level-based reconcile; retry/backoff is forgotten only after observed progress. Do not import Kubernetes.
- **systemd:** readiness/failed state is explicit; elapsed timeout does not equal readiness. Do not import a service manager.
- **Restate/DBOS:** stable correlation before an external effect. Do not import another workflow/database engine.
- **Current ELIOT Operation/resource rows:** remain the effect and root-count authority.

## 11. Exact implementation fixtures

- `provider_429_rate_limit_is_not_quota_exhaustion`
- `provider_quota_does_not_clear_on_unrelated_success`
- `provider_available_requires_newer_exact_readback`
- `provider_condition_stale_sequence_cannot_regress_current`
- `provider_same_sequence_changed_payload_conflicts`
- `route_root_limit_blocks_second_root_before_workspace_effect`
- `subagent_child_does_not_consume_root_slot`
- `route_unknown_resource_evidence_holds_not_admits`
- `work_dispatch_retains_held_subject_without_hot_loop`
- `launch_preview_and_final_gate_use_same_helper_but_final_rechecks`
- `model_gone_never_silently_switches_route`
- `missing_condition_preserves_current_behavior_and_reports_unobserved`

Public-path tests must assert zero workspace/process/native calls on denied/held admission.

## 12. Ownership and order

- R15/#41 owns Codex/Muse usage and condition producers.
- R13/#39 owns exact capacity/resource evidence and root counting primitives.
- R12/#38 owns worker pacing for held WorkDispatch subjects.
- R40 owns current provider condition, configured root limit and the final route gate.
- Adapter control PRs do not create local copies of route policy.

Recommended order:

```text
R13 resource identity
→ R15 native producers
→ R40 condition table + one manual launch caller
→ WorkDispatch/queue projections
→ remove old quota parser/capacity permission naming
```

One manager/worktree for shared `capacity.rs`, launcher admission and migration files. Other writers rebase; they do not create duplicate predicates.

## 13. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-contracts -p swarm-kernel-host -p swarm-automation --lib --bins -- -D warnings
```

Then exact producer syntax/fixture checks for the adapters changed by R15. Broad native/account qualification remains the final project phase.
