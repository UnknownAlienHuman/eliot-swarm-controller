# A2.1 — Antigravity durable same-owner Store delivery

**Packet status: APPROVED**

- **Source commit:** `797ea93f1cfcaad8c8dde2629d666e64efb131c2` (`main`, PR #194).
- **Manager worktree:** record at assignment.
- **Owning contracts:** [implementation A2](../implementation.md#a--antigravity), [delivery §7](../delivery.md#7-complete-result-capture), [module API](../module-api.md).
- **Failure-prevention IDs:** AR-001–008, AR-020–031; PF-02, PF-07, PF-10–12, PF-14–16.
- **Write scope:** `modules/antigravity-rust/**` only. Host/Store source is read-only in A2.1.

## Source lease

All listed pre-existing blobs must match before editing. A mismatch blocks the packet and requires a source refresh.

| Path | Required blob SHA |
| --- | --- |
| `modules/antigravity-rust/Cargo.toml` | `3a22fcca85299becd22c6c99524bd1577ed1b80d` |
| `modules/antigravity-rust/src/main.rs` | `c8f509dae283d3d052de1bf972b1031f3ac54656` |
| `modules/antigravity-rust/src/controller.rs` | `76bee357a26bfc03a38314ef5d570934587cc87a` |
| `modules/antigravity-rust/src/process.rs` | `7e0ff1e2d27c649954577a041ffdbad2b8b7ae22` |
| `modules/antigravity-rust/src/lib.rs` | `c29089b240feac16e35f53ca9aaf8dc90b646f36` |
| `modules/antigravity-rust/src/result_page.rs` | `0286b3275e57d3084ea9b9d2ce0ef9c4e819c6f7` |
| `modules/antigravity-rust/src/stream.rs` | `f8d01ab1620c3cb85b76ec72bb15ecfddd8141c4` |

A small new module under this existing package is allowed only when it deletes duplicated delivery handling from current callers. No new crate, service, database, workflow or generic journal.

## Observable result

After the adapter has formed exact `module.outcome` or `module.result` parameters, it durably retains those exact bytes and immutable delivery identity before attempting IPC. A connection loss may resend the identical retained parameters. Only an exact successful Store ACK removes the matching delivery. Files created by an earlier process boot remain visible but quarantined; A2.1 does not send or delete them.

### Completion condition

A focused connected case demonstrates:

```text
current terminal/body
-> exact Store params formed
-> private atomic retained file committed
-> first IPC response lost
-> reconnect by the same live adapter boot
-> byte-identical resend
-> Store duplicate/exact ACK
-> only the matching retained file removed
```

No second native prompt, terminal selection, outcome mutation or new Operation occurs.

## Required path

| Responsibility | Current path / symbol | Required change |
| --- | --- | --- |
| Terminal association and retained response | `src/controller.rs::Controller::{settle_terminal,record_outcome,pending_receipts,completed_turns,retained_response_for_operation}` | Keep the existing exact Operation/receipt/conversation/ordinal/boot association; form one immutable delivery payload. |
| IPC scheduling | `src/main.rs::HostSession::{request_saved,flush_reports}` and the outcome/result branches in `run` | Save before IPC, load pending current-boot deliveries, resend identical bytes, acknowledge exact match only. |
| Private state root / process identity | `src/process.rs` and manager-supplied module state | Use the existing bounded private durable-file primitives and the current boot identity; do not invent authority. |
| Result bytes | `src/result_page.rs::{build_normalized,build_normalized_page}` and `src/stream.rs::RetainedResponseBody` | Retain exact selected response bytes/digest needed to reproduce the already-formed result request. |
| Store boundary | existing `module.outcome` / `module.result` handlers | No source change. Treat the existing exact ACK/replay behavior as the only deletion authority. |

## Retained invariants

- Store remains Task/Attempt/Operation and aggregate result-coverage authority.
- The adapter stores only already-formed Store delivery and selected result bytes; it never stores native prompt intent for replay.
- `origin_boot_id`, binding ID/generation, Operation ID, method and exact params digest are immutable.
- Same-owner IPC reconnect is not process restart. A prior-boot file is quarantined for A2.2.
- ACK for outcome cannot delete a result page; ACK for one page cannot delete another page or the response body.
- Unknown or malformed files remain explicit and bounded; they are not silently discarded or treated as success.
- Storage pressure stops new native work before unacknowledged delivery is evicted.
- Current A1b Task/Attempt/route/binding/boot/root/model/workspace and command-time checks remain intact.

## Unsupported and uncertain cases

| Case | Required A2.1 behavior |
| --- | --- |
| Crash before atomic durable commit | No local-retention claim; existing source remains the only possible readback. |
| Crash after durable commit | File survives but a new boot quarantines it; no automatic send in A2.1. |
| IPC loss after possible Store commit | Resend identical params only; never form a new request or prompt. |
| Corrupt, oversized, linked or wrong-owner state file | Reject/quarantine with bounded diagnostic; do not follow or delete it. |
| Binding/generation mismatch | Reject without IPC or native I/O. |
| Duplicate current-boot file for one method/Operation | Require byte identity or surface conflict; do not pick newest. |
| Body retained but Store page coverage incomplete | Keep body; A2.1 cannot infer release from one ACK or EOF. |

## Prior attempt / anti-donor

`impl/antigravity-a2-delivery-20261024` at `37caeb337cdfa4a6242e401352b52fec4a09e105` is an unmerged experiment, not a patch source. Review it only to recover or reject mechanisms.

Preserve only mechanisms that satisfy this packet: private atomic bounded files, exact params digest, origin boot, exact ACK deletion and no prompt persistence. Do **not** inherit branch-local status claims, bot/finalizer machinery, cross-boot authority, local aggregate-coverage inference, universal journal structure or wholesale file layout. No cherry-pick.

## Deletions and data disposition

- Replace the in-memory-only ACK outbox for outcome/result Store delivery with one package-local durable outbox.
- Delete duplicated outcome/result resend branches after both call the same narrow owner.
- Do not delete existing captured response bytes until Store-authoritative complete coverage is available in A2.2.
- Existing prior-boot files have no automatic disposition in A2.1; enumerate and quarantine them.

## Verification during construction

```text
cargo fmt --all -- --check
cargo clippy --locked -p swarm-antigravity-adapter --all-targets -- -D warnings
```

Run one focused connected delivery case for first-response loss and identical replay when the path closes. Do not build an installed-provider, crash matrix or broad test campaign in A2.1; those are named A2.2/product-stage evidence.

## Acceptance evidence

| Claim | Required evidence |
| --- | --- |
| Save precedes IPC | Caller trace plus focused case showing file committed before request. |
| Replay is byte-identical | Exact serialized bytes/digest before and after reconnect. |
| Exact ACK deletion | Wrong method/Operation/digest cannot delete; correct ACK deletes one record. |
| Prior boot cannot self-authorize | Restarted process loads but does not send/delete the file. |
| Native work is not replayed | No path from spool load to native stdin/Open/Dispatch/Send. |
| Old volatile path absent | Exact symbol/caller scan after integration. |
| Documentation synchronized | Slice register, status, audit and AR/PF state updated in the same integration. |

## Output

Report only connected behavior, exact files/symbols changed, volatile paths deleted, checks actually run, net production Rust change, unresolved facts and the exact A2.2 handoff.
