# A2.2 — Antigravity successor delivery and aggregate coverage

**Packet status: BLOCKED**

Blocked until A2.1 lands and this packet records the merged commit plus exact blobs for its retained-delivery owner. Do not assign from the experimental A2 branch.

- **Owning contracts:** [implementation A2](../implementation.md#a--antigravity), [delivery §7](../delivery.md#7-complete-result-capture), [module API](../module-api.md).
- **Failure-prevention IDs:** AR-001–008, AR-012–024, AR-025–031; PF-02, PF-07, PF-10–12, PF-14–16.
- **Expected write owners:** the landed A2.1 adapter delivery owner, `crates/swarm-kernel-host/src/runtime/warm_stream.rs`, `crates/swarm-kernel-host/src/store/runtime.rs`, and only the exact result/coverage owner proven by source refresh.

## Observable result

After exact previous-owner departure is proven, an authorized successor uses the existing admitted reconciliation path to deliver retained outcome/page parameters for the **original** Operation. Store validates original Task/Attempt/binding/source identity and the reconcile relation. Store ACK reports authoritative aggregate coverage and next missing range. The adapter releases retained response bytes only after complete coverage or explicit authorized disposal.

### Completion condition

The connected crash/restart matrix closes all of these boundaries without native replay:

1. crash before durable file commit → visible unknown/no false completion;
2. crash after file commit before Store call → successor imports original delivery after custody fence;
3. Store commit followed by lost reply → exact replay deduplicates;
4. partial pages across several replacements → Store aggregate coverage selects next missing range;
5. superseded Task/Attempt → historical evidence remains attached to the original Operation only;
6. corrupt/missing file → bounded gap/native readback or unknown, never inferred success.

## Required path

```text
retained prior-boot delivery
-> exact previous-owner/process-family departure
-> admitted agent.reconcile for the original target
-> successor reads bounded immutable file
-> Store validates origin boot + binding generation + original Operation + reconcile relation
-> exact outcome/page ACK with authoritative coverage
-> next missing range or complete coverage
-> safe local body/file disposition
```

## Retained invariants

- Reconciliation performs readback/import only; it cannot Open, Dispatch, Send or write native stdin.
- Previous-owner absence, process-family departure and Store authorization are independent facts.
- Origin boot is preserved; imported evidence never becomes current activity or a new Attempt.
- A superseded assignment receives historical evidence, not completion of the new revision.
- Store owns page identity, aggregate coverage and completion. The adapter owns vendor/local byte lifetime.
- EOF or one page ACK is not complete coverage.
- Missing source remains `unknown`; timeout is not PASS.
- Unrelated work continues while one exact delivery remains unresolved.

## Source refresh required before approval

The manager must record:

1. merged A2.1 commit and blobs;
2. exact Store outcome/result ACK shapes and coverage owner;
3. current reconcile admission/target validator symbols;
4. process-custody owner and previous-family departure proof;
5. every reader of captured response bytes and each deletion/disposal path;
6. unresolved file/data inventory from the experimental branch or local state.

The current `main@797ea93` reference blobs are evidence only, not an approval lease:

| Path | Reference blob |
| --- | --- |
| `crates/swarm-kernel-host/src/runtime/warm_stream.rs` | `be4dab2a42b02e650bad51b98eaba42cb9dc208e` |
| `crates/swarm-kernel-host/src/store/runtime.rs` | `1f5c7ecde10af1226d69b8978dc5bb9c1a5f1b41` |
| `crates/swarm-kernel-host/src/store/results.rs` | `8081a62bdc02c4546e2010bb767b9661cbb63a65` |

## Prior attempt / anti-donor

The unmerged `impl/antigravity-a2-delivery-20261024` branch may contain useful file-format experiments, but its claims are not closure evidence. Reject any design that lets the adapter self-authorize prior-boot delivery, derives aggregate coverage locally, mutates an accepted terminal outcome, treats a reconcile ACK as native execution, or uses CI/finalizer machinery as runtime state.

## Deletions and data disposition

- Delete any temporary A2.1 prior-boot quarantine path once successor admission owns it.
- Delete adapter-local ACK range/coverage state after Store ACK exposes authoritative coverage.
- Dispose exact acknowledged files/body bytes only after complete coverage or explicit policy; retain unresolved/corrupt records for bounded diagnosis.
- Do not add a second recovery service, delivery database or universal adapter journal.

## Verification during construction

After source refresh, record exact minimal commands. Expected package boundary:

```text
cargo fmt --all -- --check
cargo clippy --locked -p swarm-antigravity-adapter -p swarm-kernel-host --all-targets -- -D warnings
```

Focused connected crash/restart and ACK-loss cases close A2.2. Installed Antigravity/provider, power-loss and scale qualification remain separately recorded product-stage evidence.

## Approval transition

Change this packet to `APPROVED`, add exact source/blob leases and update the [slice register](../implementation-slice-register.md) only after A2.1 is merged and the required source refresh is complete. A worker may not fill those fields while implementing.
