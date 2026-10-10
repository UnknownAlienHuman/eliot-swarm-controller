# R50. OpenCodex configuration: `applied` requires positive method-specific verification

**Status:** implementation handoff. Production OpenCodex bridge, artifact ID, routes and external service are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08), current artifact `opencodex-2.75.0-bridge.3`.

## 1. Result

Every saved OpenCodex configuration Operation ends in a truthful state:

```text
applied
    every requested effect has positive method-specific evidence

partial
    at least one requested effect is proved applied
    and at least one requested effect is proved not applied/mismatched

unknown
    mutation may have occurred but required verification is unavailable,
    ambiguous or only observational

rejected
    exact local/upstream evidence proves no admitted effect
```

HTTP 200, a non-null receipt, an `observed` readback or the absence of a mismatch is never enough for `applied`.

No mutation is replayed. Unknown results remain readback-only. OpenCodex stays an externally owned provider/protocol service, not a native Codex session owner.

## 2. Confirmed HIGH defect

### 2.1 Generic verifier converts “nothing checked” into `observed`

Current bridge helper:

```javascript
function verifyFields(fields) {
  const values = Object.values(fields);
  const state = values.includes('mismatch') ? 'mismatch'
    : values.length > 0 && values.every((value) => value === 'verified') ? 'verified'
    : 'observed';
  return { state, fields };
}
```

`observed` therefore includes materially different cases:

- every requested field is `not_checked` because readback failed;
- some fields are verified and others are `not_checked`;
- a field was merely observed under a default-resolving API and not compared;
- no positive effect evidence exists.

### 2.2 Success paths treat every non-mismatch as applied

Multiple configuration writers use:

```javascript
record.outcome = record.verification.state === 'mismatch'
  ? 'partial'
  : 'applied';
```

The same pattern exists for protocol settings, subagent V2, injection model, effort caps, subagent models and fallback. Model settings similarly returns applied unless there is an element failure, catalog refresh failure or mismatch.

Thus a 200 followed by failed/missing GET can produce:

```text
verification.state = observed
fields = { requestedField: not_checked }
outcome = applied
completion_condition = native_configuration_applied
```

This directly contradicts the module README's stated rule “Readback decides” and converts absence of verification into positive completion.

### 2.3 Integration success compares the service to its own receipt, not the request

For client/Aside integration configuration after HTTP 200:

```javascript
record.receipt = normalizeIntegrationOutcome(result.body);
const after = await integrationStateRead(...);
if (after && record.receipt?.state) {
  record.verification = verifyFields({
    state: after.state === record.receipt.state ? 'verified' : 'mismatch',
  });
}
record.outcome = elementFailures || verification.mismatch
  ? 'partial'
  : 'applied';
```

Problems:

1. `record.receipt.ok` is normalized but never required to be `true`.
2. `after.state` is compared with `receipt.state`; the requested expected state is not the comparison target.
3. If both service and receipt echo the same wrong/unchanged state, verification passes.
4. Missing `after` leaves the default `not_performed`/non-mismatch state and still permits applied.
5. An empty/missing element list does not prove that the requested element succeeded.

A 200 body with `ok:false`, or a self-consistent unchanged state, can therefore be retained as applied.

## 3. Closed verification vocabulary

Separate evidence quality from effect outcome. Replace the overloaded generic `observed` completion state with an exhaustive internal vocabulary:

```javascript
const FieldVerification = {
  Verified: 'verified',
  Mismatch: 'mismatch',
  Unverified: 'unverified',
};

const VerificationState = {
  Verified: 'verified',
  Mismatch: 'mismatch',
  Unverified: 'unverified',
};
```

A snapshot may still say a value was `observed`; a mutation completion verifier may not use that word as a success-shaped third state.

Recommended reducer:

```text
no requested fields                          → invalid local contract
all requested fields verified               → verified
one or more proven mismatch, none unknown    → mismatch
mixed verified+mismatch                      → mismatch with per-field facts
any required unverified and no mismatch      → unverified
mismatch plus unverified                     → unverified unless applied subset is independently proven and partial is truthful
```

Do not infer partial merely from a 200. `partial` needs positive evidence for at least one landed requested effect.

## 4. Method-specific evidence matrix

One generic reducer may combine fields, but each writer owns the evidence that can mark a field Verified.

### 4.1 Protocol settings

For every requested leaf:

- fresh `/api/protocols` readback must parse;
- exact effective leaf must equal the requested merged value;
- dependency-derived OAuth/native value uses the current documented merged rule;
- missing/malformed readback → unverified/unknown;
- HTTP receipt alone is not effect evidence.

### 4.2 Model settings

Requested axes split by actual upstream evidence:

- `contextWindow`, `inputModalities`: exact declared fields from fresh model row;
- `reasoningEfforts`, `defaultReasoningEffort`: exact normalized receipt echo only if the receipt has the required positive save shape (`saved:true` or the reviewed equivalent) and identifies the same provider/model/request; otherwise unverified;
- client-integration roster observation is diagnostic, not proof that model declarations landed;
- failed catalog refresh remains partial only when saved settings are positively verified; otherwise unknown.

Do not make a missing model row `not_checked` and then applied.

### 4.3 Subagent V2

- ordinary values require exact fresh `/api/v2` equality;
- text fields require the strongest evidence exposed by upstream; length/set-only evidence must be labelled limited and may not prove arbitrary text equality unless the contract explicitly defines it as sufficient;
- null/unset fields that GET resolves to defaults are not verified merely because a value was observed. Require a positive receipt field that proves removal plus compatible readback, or retain unknown;
- upstream warning is not a success proof.

### 4.4 Injection model, effort caps, subagent models and fallback

Every requested axis must be compared with a fresh normalized GET result. Missing GET, missing field or schema failure → unverified/unknown.

For fallback poll interval and null clears, compare the normalized semantic value exactly; do not treat a default-resolved value as proof of deletion without an authoritative receipt/removal marker.

### 4.5 Client integration / Aside profile

Success requires all of:

```text
HTTP response parsed under the documented success schema
receipt.ok == true
receipt/operation identity matches the requested operation
per-element result for the exact target exists and is ok == true
fresh state readback exists
fresh state equals paths.expectedState derived from the request/confirmed plan
no conflicting residual/drift marker
```

Compare the service with **the requested expected state**, not with `receipt.state`.

For restore, bind the receipt and readback to the exact `opId`/profile/client and the confirmed plan fingerprint. A self-consistent receipt from another target is not valid.

If upstream legitimately returns a success envelope without per-element rows for a single-target method, encode that method-specific shape explicitly and test it. Do not accept “missing elements means no failures”.

## 5. Outcome reducer

Use one small private function after method-specific verification:

```javascript
function outcomeFromVerification({ response, receipt, elements, verification }) {
  // returns applied | partial | unknown | rejected
}
```

It must be closed and exhaustive, not a generic “anything but mismatch is applied”.

Rules:

- `applied`: exact positive response/receipt requirements and `verification.state === 'verified'`;
- `partial`: exact evidence identifies at least one verified requested effect and at least one mismatched/refused requested effect;
- `unknown`: any required field/effect is unverified and no exact no-effect proof exists;
- `rejected`: local validation or structured upstream refusal proves the effect was not admitted;
- response lost: always start as unknown, then readback may upgrade to applied/partial only through the same method-specific verifier; never resend;
- HTTP 200 with malformed, `ok:false` or target-mismatched body is not a normal success.

Store both:

```text
verification.state
verification.fields
verification.basis per field
```

so `applied` can be audited without rereading prose.

## 6. Artifact migration

This changes the meaning of terminal result records. Publish a new current artifact, expected name:

```text
opencodex-2.75.0-bridge.4
```

Update in one connected slice:

- `bridge.mjs` artifact constant;
- `module.example.json`;
- README current artifact and truthful completion semantics;
- UPDATE.md change record and exact source review baseline;
- root README/runtime matrix references owned by this artifact;
- selftest expected artifact and fixtures.

Do not silently change bridge.3's claimed contract. Historical bridge.3 records remain readable as historical evidence but must not be reclassified as positively verified merely because they said `applied`.

No fallback from bridge.4 to bridge.3, no version pin/downgrade of the externally owned service and no account/provider mutation beyond the selected request.

## 7. Selftest expansion

Current selftest exercises request forms and some envelopes, but the acceptance bar must directly falsify the old logic.

### Generic verification

- `all_not_checked_is_unverified_not_observed`
- `mixed_verified_and_unverified_is_not_verified`
- `all_verified_is_verified`
- `mismatch_is_not_applied`
- `empty_requested_field_set_is_invalid`

### Every writer family

For protocol, model, V2, injection, effort caps, subagent models and fallback:

1. 200 + fresh exact readback → applied;
2. 200 + GET failure → unknown;
3. 200 + malformed GET → unknown;
4. 200 + exact mismatch → partial or unknown according to positive landed subset, never applied;
5. lost response + exact readback → applied without second mutation;
6. lost response + unavailable readback → unknown and one mutation attempt.

Count mutation calls in the fake server.

### Integration

- `integration_200_ok_false_is_not_applied`
- `integration_receipt_state_matches_after_but_not_request_is_not_applied`
- `integration_missing_target_element_is_unverified`
- `integration_wrong_target_element_is_unverified`
- `integration_expected_state_exact_match_applies`
- `integration_restore_binds_exact_op_and_target`
- `integration_lost_response_reads_back_without_replay`
- `integration_partial_requires_positive_success_and_failure_evidence`

### Historical boundary

- bridge.3 applied+unverified record remains historical and is never used as bridge.4 positive readback;
- bridge.4 qualification fixture contains `verification.state=verified` and per-field basis for every applied result.

## 8. Store/module consumer boundary

Audit the host consumer of OpenCodex configuration results in the same PR.

It must not treat a raw string `outcome:"applied"` from an older artifact or unsupported verification schema as sufficient for current `native_configuration_applied` authority.

Require:

```text
selected artifact supports verification schema v2
+ exact operation/request identity
+ outcome applied
+ verification state verified
+ method-specific required evidence present
```

Historical records retain their original facts; they may project `legacy_verification_insufficient` rather than being rewritten.

Keep module configuration authority in Store. The adapter's receipt is evidence, not permission to mutate unrelated configuration.

## 9. Simplification and deletion

After migration, delete:

- generic `observed` branch from mutation verification;
- every `state === 'mismatch' ? 'partial' : 'applied'` expression;
- integration self-comparison `after.state === receipt.state` as the final authority;
- success inference from missing `elements`;
- comments claiming readback decides where code accepts missing readback;
- any consumer that accepts current applied authority without the new verification schema.

Keep snapshot observation vocabulary separate and keep readback-only/no-replay behavior.

## 10. Ownership and non-import boundaries

R50 owns:

- OpenCodex configuration evidence classification;
- bridge.4 artifact migration;
- host validation of current OpenCodex applied evidence;
- fake-server/selftest qualification.

R40/#64 owns provider route availability, not configuration effect proof. R43/#66 owns the current Codex Goal controller, not the OpenCodex provider service. R15/#41 owns Codex/Muse usage facts.

Do not import a workflow engine, generic verification DSL or another HTTP client. Use the current bridge, normalizers and fake server; replace only the false success logic.

Recommended order:

```text
1. closed verification reducer
2. method-specific evidence functions
3. integration exact request/receipt/readback binding
4. bridge.4 artifact migration
5. host current-evidence validator
6. negative fake-server fixtures
7. delete old non-mismatch=applied paths
8. syntax/selftest and scoped Rust gate if host changes
```

## 11. Gates

After connected code:

```sh
node --check modules/opencodex/bridge.mjs
node --check modules/opencodex/control.mjs
node --check modules/opencodex/selftest.mjs
node modules/opencodex/selftest.mjs
```

If the host consumer changes:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-kernel-host -p swarm-contracts --lib --bins -- -D warnings
```

Live configuration qualification remains a later explicit phase against an operator-owned disposable configuration target. It must not modify a production OpenCodex service during this PR's default checks.
