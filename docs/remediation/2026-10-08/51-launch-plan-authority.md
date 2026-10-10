# R51. Launch plan authority: lossy preview fallback must never become execution input

**Status:** implementation handoff. Production launch preview, manifests, work dispatch and native effects are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

The launcher has one complete internal plan authority and one bounded public projection:

```text
validated current Task/Attempt/route/MCP/workspace facts
→ complete private LaunchPlanAuthority
→ stable plan digest
→ bounded preview projection (full or detached)
→ caller confirms digest
→ recompute complete LaunchPlanAuthority
→ exact digest equality
→ manifest built only from complete authority
```

A public-size fallback may detach display data, but it can never remove fields later used for effects or validation.

No second launcher, plan database or generic projection framework is introduced.

## 2. Confirmed HIGH failure

### 2.1 Full preview is used as both public projection and effect authority

`launch_preview` first builds a large JSON object containing, among other fields:

```text
route
mcp
candidate_scope
workspace
baseline
capacity
```

It computes `plan_digest` from that full canonical plan. If the serialized response exceeds `projection::MAX_SERIALIZED_BYTES`, it returns a smaller fallback carrying the **full plan's digest** but omitting effect-bearing fields such as `route`, `mcp` and `candidate_scope`.

### 2.2 Revalidation accepts the fallback because the digest still matches

`launch_for_actor` recomputes the preview and checks only:

```rust
preview["plan_digest"] == request.plan_digest
```

For an oversized plan, both calls return the same fallback and the digest comparison succeeds because the fallback carries the digest of the hidden full plan.

### 2.3 Manifest is then constructed from missing fallback fields

The durable manifest uses:

```rust
"runtime": { "route": preview["route"], ... }
"mcp": preview["mcp"]
"task": { "candidate_scope": preview["candidate_scope"], ... }
```

Missing JSON keys become `null`. The launch is admitted as `pending_workspace`, but later readers require exact route aliases, MCP/participant facts and scope identity. For example `launcher_dispatch::launch_manifest_route_alias` rejects a missing `runtime.route.alias` as `LAUNCH_MANIFEST_CORRUPT`.

Thus a legitimate large preview can admit a manifest that its own downstream validators can never accept. The Operation can remain stuck even though the full internal plan was valid.

### 2.4 Terminal fallback bound is debug-only

If the first fallback is still too large, the terminal fallback is returned after:

```rust
debug_assert!(serialized_len <= MAX_SERIALIZED_BYTES)
```

Release builds do not enforce the response bound. Large bounded IDs/references can therefore still escape the advertised limit.

## 3. Root cause

One JSON value currently carries three incompatible responsibilities:

```text
internal authority for effects
public human/agent preview
bounded transport projection
```

When transport size forces information loss, authority is lost too. Recomputing a digest over the hidden full value does not restore the missing fields to the consumer.

The fix is not to add route/MCP fields ad hoc to each fallback. Any future effect-bearing field could be omitted again. Separate authority from projection once.

## 4. Minimal private plan type

Introduce one private, typed or strictly validated data-only value owned by launcher planning:

```rust
struct LaunchPlanAuthority {
    task: ExactTaskIdentity,
    attempt_action: AttemptAction,
    current_attempt: Option<ExactAttemptIdentity>,
    candidate_scope: Value,          // replace with existing typed form if available
    workspace: WorkspacePlan,
    baseline: Value,
    route: SelectedRoute,
    mcp: McpPlan,
    requested: RequestedLaunchFacts,
    blockers: Vec<LaunchBlocker>,
    gaps: Vec<LaunchGap>,
    readiness: LaunchReadiness,
}
```

Use existing domain types where they already exist. Do not create broad copies of route/workspace/MCP schemas merely to satisfy this sketch.

The minimum acceptable implementation may keep narrow `Value` leaves, but the top-level authority must be closed and validated once. It cannot be a caller-supplied/public projection.

Return privately:

```rust
struct PlannedLaunch {
    authority: LaunchPlanAuthority,
    projection: Value,
    plan_digest: String,
}
```

Public `swarm.launch.preview` returns only `projection`. WorkDispatch and direct launch retain/use `authority` inside the same Store transaction/call path.

## 5. Digest contract

Define one canonical digest input from complete effect-relevant authority:

```text
schema version
exact Task/revision/Attempt action
selected route identity and effective options digest
MCP profile/credential/schema identities
workspace policy and baseline identity
candidate scope
budget/stop/purpose
blocker/gap inputs that change admission
```

Exclude purely presentational fields:

- rendered task brief prose already committed by its digest;
- duplicated decision-card text;
- detached-reference display labels;
- serialized byte counts;
- pagination/UI hints.

Name the private value, for example:

```rust
LaunchPlanDigestInputV1
```

The digest remains caller-visible as `sha256:<hex>`. A changed authority changes the digest even when both old/new public projections are detached.

Do not digest the public fallback. Do not accept a client-supplied authority object.

## 6. Public projection

Build the public projection from `LaunchPlanAuthority` after the digest is fixed.

### Full projection

When within the byte bound, retain the current useful details.

### Detached projection

When oversized:

- keep exact plan digest, Task/revision, Attempt action/ID, readiness and bounded blockers;
- mark `authority_projection: detached` or equivalent explicit coverage;
- provide bounded, authorized read references for omitted sections;
- do not put `null` placeholders that look like effect values;
- never use the detached projection to construct the manifest.

### Terminal projection

If even the detached projection cannot fit:

- progressively reduce bounded optional presentation fields under an explicit deterministic policy;
- perform a real runtime serialized-length check;
- if the minimum closed projection still exceeds the limit, return `PAYLOAD_TOO_LARGE` before any launch Operation/slot/effect is admitted;
- no `debug_assert` as the only guard.

## 7. Manifest construction

`launch_for_actor` must receive/recompute `PlannedLaunch`, compare its `plan_digest`, and construct `launch_manifest` from `authority` only.

Required manifest invariants before the Operation update:

```text
runtime.route has exact nonempty alias and selected route identity
mcp has the selected profile/credential/schema facts required downstream
Task/project/revision and Attempt tuple are complete
candidate/workspace/baseline identities are internally coherent
manifest plan_digest == authority digest == request plan_digest
serialized effective request is bounded
```

Use one `validate_launch_manifest_precommit` call shared by direct launch and WorkDispatch admission. This validator operates on the complete candidate manifest and has no Store effects.

If validation fails, no semantic launch slot, workspace lease or native effect is created.

## 8. WorkDispatch integration

`automation_work_dispatch` currently reads preview fields to decide Pending/Admit. It must consume the same `PlannedLaunch` result inside Store code, not reconstruct authority from the public projection.

Rules:

- blockers/readiness come from `authority`;
- deterministic WorkDispatch request contains only caller-facing preview parameters plus the authoritative digest;
- final launch admission recomputes authority and rechecks digest/current rights;
- detached public projection does not change WorkDispatch behavior;
- no second plan/digest helper in automation.

R28/#54 later validates launch-owned task.dispatch reuse; it consumes the corrected manifest rather than adding fallbacks for null route/MCP.

## 9. Exact fixtures

### Oversize path

- `oversized_launch_preview_is_detached_but_launch_manifest_remains_complete`
- `oversized_preview_route_alias_survives_into_manifest`
- `oversized_preview_mcp_identity_survives_into_manifest`
- `oversized_preview_candidate_scope_survives_into_manifest`
- `oversized_preview_work_dispatch_reaches_same_complete_manifest`

Construct enough authorized context to cross the actual serialization bound; do not patch the projection after production planning.

### Digest

- `full_and_detached_projection_of_same_authority_share_digest`
- `route_change_changes_digest_when_both_projections_are_detached`
- `mcp_profile_change_changes_digest_when_both_are_detached`
- `presentation_only_detachment_does_not_change_digest`
- `client_cannot_supply_hidden_authority_fields`

### Bounds

- `terminal_launch_preview_never_exceeds_serialized_limit_in_release_logic`
- `minimum_projection_too_large_fails_before_operation_or_slot`
- `no_debug_assert_only_size_contract`

### Downstream public path

- direct preview→launch→workspace→binding→task.dispatch with an oversized preview;
- automatic WorkDispatch path with the same oversized source;
- inspect retained `effective_request_json.launch_manifest` and route/MCP validators;
- assert one launch Operation and one semantic slot, no replay/fallback.

## 10. Historical boundary

Existing malformed retained manifests are not repaired by inventing missing route/MCP data from current configuration. Current configuration may have changed.

For historical rows:

- read-only diagnostics classify `legacy_detached_manifest_missing_authority`;
- if an exact immutable linked plan/route receipt already exists and all identities/digests match, a dedicated recovery slice may use it;
- otherwise retain outcome unknown/attention and never replay the launch;
- new writes use the complete authority path only.

Do not add a compatibility writer that continues producing the old manifest.

## 11. Simplification and deletion

After migration, delete:

- use of public preview JSON as manifest authority;
- fallback-specific assumptions in downstream validators;
- hidden full-plan digest attached to a structurally incomplete effect input;
- debug-only terminal size guard;
- duplicate plan/digest construction in WorkDispatch;
- any rescue branch that reloads current route/MCP to fill a retained null manifest.

Keep one planning pass, one digest input, one bounded projection and one manifest validator.

## 12. Ownership and ordering

R51 owns:

- launch planning authority/projection split;
- stable launch plan digest;
- manifest precommit integrity;
- direct and WorkDispatch consumption of the same private plan.

R13/#39 owns resource/capacity/workspace release semantics. R20/#46 owns compact model prompt. R28/#54 owns task.dispatch semantic reuse. R31/#57 owns module command compatibility. R14/#40 owns frontend schemas, not launch authority.

Shared `launcher.rs`/WorkDispatch files require one manager/worktree. Recommended order:

```text
1. private PlannedLaunch/authority builder
2. digest input and tests
3. bounded projection builder
4. manifest precommit validator
5. switch direct launch
6. switch WorkDispatch
7. classify historical malformed manifests
8. delete projection-as-authority/fallback patches
```

## 13. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-automation \
  --lib --bins -- -D warnings
```

Then exact public preview→launch and WorkDispatch fixtures above. Broad/native qualification remains later.
