# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

Source was inspected on **2026-10-24** at
`053f1e0fb15b3d40c00918f4a25ec8f9e4969e02`. The active audit contains
**141 entries**. AR-127–141 trace authenticated multiplexed IPC, host concurrent intake,
MCP connection slots, reconnect races, subscription polling/lag, MCP Tasks pending-input
reads, attention/timeline cost, Store writer occupancy and WAL-reader boundaries.

Merged work immediately preceding this checkpoint:

- PR #188 at `e99c06b`: AR-100–114 plus removal of disconnected GitHub
  PR/projection code, net **−250 Rust lines**; exact head `0a5a099`, run
  `40087522775` passed scoped Ubuntu/Windows compile and changed-line Clippy.
- PR #189 at `053f1e0`: AR-115–126 plus the numeric Store-evolution contract; exact
  head `3c05253`, run `40088525487` completed successfully; documentation only.

- [Current resume point and findings](agent-operations/audit-current.md#resume-here)
- [Evidence/source index](agent-operations/audit-index.md)
- [Architecture](agent-operations/architecture.md)
- [Single implementation program](agent-operations/implementation.md)
- [IPC/MCP target](agent-operations/ipc-mcp.md)
- [Store evolution target](agent-operations/store-evolution.md)
- [Agent communication target](agent-communication-program.md)
- [Observability target](agent-operations/observability.md)
- [Donor register](agent-operations/donor-map.md)

## Runtime changes

| Change | Evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains. |
| Active Antigravity coordinate aligned on v6 | `cba98b7` | No installed native-turn qualification. |
| Terminal receipt decoupled from Store observation | `eeea355`, recorded net −265 production Rust lines | Dispatch proof and crash-unsafe delivery remain. |
| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930` | Scoped regression/Clippy only; no crash/installed/load proof. |
| Disconnected GitHub PR/projection modules removed | `98b350e`, net −250 Rust lines; run `40087522775` passed scoped Ubuntu/Windows gates | No PR-source feature or remote observation was added. |

```text
A0   LANDED                   active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
B–V  TARGET WORK              largely unreduced current runtime
U/R  SMALL CLEANUP LANDED      disconnected GitHub PR/projection source removed
J0   SELECTED / NOT LANDED     release MCP slot lock before shared RPC
```

Runtime implementation still starts with **A1b–A3**. J0 is a bounded independent
frontend fix and does not reorder the main migration.

## Source-inspection result

| Area | Source-confirmed behavior | Open boundary |
| --- | --- | --- |
| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several useful delivery mechanisms. | Durable Antigravity delivery, first historical capture, continuation and live crash/power-loss. |
| Queue/provider/resources, AR-036–050 | Root dedup and common claim/Open transaction are useful. | Stable budget identity, cancellation and attached-binding retirement. |
| Goals/automation, AR-051–058 | Exact terminal/occurrence identity exists. | Actor/phase/transfer bugs, finite budgets and one ordinary loop. |
| Review/correction/acceptance, AR-059–064 | Replacement, all-findings feedback, same-Attempt resubmission and frozen evidence gate exist. | Automatic replacement, finite correction, subject fairness and common review/finding provenance. |
| CheckRun, AR-065–070 | Immutable source, Store-Go, process custody, parser coverage, cache lineage and acceptance revalidation exist. | Automatic interrupted source capture and standalone-only executor cut. |
| ScriptRun, AR-071–078 | One-shot interpreter, same-family restart and atomic parent/child admission exist. | Per-action identity/readback/retry, action vocabulary, executor precheck and mid-run cancellation. |
| External effects, AR-079–088 | Forge and GitHub effects retain exact intent, one-write boundary and readback. | Sticky process hold, duplicate PR PATCH owner, mutable-source recovery, manual unknown reconciliation and duplicate automation engines. |
| GitHub sources/import, AR-089–099/114 | Repository ID, safe GET retry, explicit gaps and local-divergence conflict are useful; disconnected PR/projection code is removed. | Stable source cut, aggregate coverage, explicit import, live-work conflict, work-pool deletion and remote view. |
| Mailbox/coordination, AR-100–113 | Exact sender/recipient/scope checks, scoped grants and GM epoch handover are useful. | Recipient rows; finite migration; grant relocation; deletion of coordination products/public/storage surfaces. |
| Store, AR-115–126 | Single-writer ownership, pre-query timeout, pre-WAL identity, immediate transaction, rollback and query-only WAL reader are useful. | Numeric v2 implementation, v1 bridge, semantic postconditions, reader/Doctor cutover and deletion migrations. |
| IPC/MCP, AR-127–141 | Client/server multiplexing, no-replay unknown outcome, live Store authorization, tool/pump isolation and bounded lag semantics are useful. | MCP outer-lock removal, exact pending-input read, bounded attention, one pump, WAL read routing and concurrent/load qualification. |
| Remaining | Structural and selected source reviews remain indexed. | Joined local observation and gateway completion. |

## IPC and MCP status

### Preserve

- One physical authentication before any application frame.
- Shared Client clones with one pending map and exact out-of-order correlation.
- Bounded in-flight requests and pre-submit capacity refusal.
- No replay after possible write; all unresolved calls become `OUTCOME_UNKNOWN` on
  transport damage.
- Concurrent host intake and draining of already accepted Store calls after disconnect.
- Separate tool and subscription-pump links.
- Current Store authorization rather than profile/session grant caching.
- Caller-known mutation request IDs for restricted profiles.
- Bounded subscriber item/byte queues and explicit lag range/count.
- Original mutation success when optional MCP Task projection fails.
- Queued-only ordinary `operation.cancel` for MCP Tasks cancellation.

### Repair

1. Clone the current Client under the connection-slot mutex, release the mutex before
   `request_shared`, and generation-safely clear only a disconnected slot after a
   transport-class failure.
2. Add focused concurrent/out-of-order/application-error/disconnect/late-old-failure
   cases for the facade.
3. Replace `tasks/get` global `report.attention` paging with one exact
   Operation/binding-generation pending-input read.
4. Make report page limits bound Store production work, not only response slicing.
5. Replace per-subscription pollers with one session delta/wake pump and independent
   bounded subscriber queues.
6. Remove Concilium/coordination subscription categories with those products.
7. Move selected pure bounded exact reads from the writer queue to a retained
   query-only WAL reader owner while keeping authorization singular/current.
8. Keep Store history and exact reads as replay authority; add no frontend database,
   retry queue or durable event log.

J0 is selected but **not yet landed**. This status must not be read as an executed
concurrent MCP fix.

## Store status

Preserve DataRoot lock, one writer, pre-query busy timeout, pre-WAL identity validation,
`BEGIN IMMEDIATE`, rollback, no fallible postcommit validation, existing-only recovery
and committed WAL reads.

Repair permanent version 1 plus SHA markers through the finite numeric v1→v2 program in
[Store evolution](agent-operations/store-evolution.md), including future-version
rejection, semantic postconditions, exact-current readers and product deletion
migrations.

## Mailbox and coordination status

Preserve exact sender/recipient/scope, immutable Operation admission, scoped worker/
reviewer access and GM handover without historical identity rewriting.

Repair through recipient rows keyed by Operation ID, grant migration and vertical
removal of Thread negotiation, cards/consult, contracts, `code.scope.*`, integration,
Concilium and watches with MCP/CLI/storage/migrations.

## Source audit versus runtime qualification

A completed source audit needs a named revision and evidence for every retained action:
caller → parse → exact authority → Store transition → effect/readback → consumer,
including cancellation, late result, restart, handover, migration, partial writes and
slow consumers. It also needs historical requirement mapping and concrete data/code
disposition for each deletion. This remains incomplete globally.

Executed qualification is separate: concurrent MCP/load, privilege/migration/corruption
cases, installed provider contracts, Windows custody, fairness and autonomous
end-to-end work. Source inspection and a successful compile cannot substitute for those
results.

## Next work

**Audit:** continue the local Governor observation chain named in
[audit-current](agent-operations/audit-current.md#resume-here): provider-local subjects,
process identity/liveness, live worktree/Git/remote facts, unmatched subjects and one
joined view with explicit source coverage.

**Runtime:** the main program still begins with Antigravity A1b–A3. The selected
independent IPC slice is J0 from [IPC/MCP](agent-operations/ipc-mcp.md): release the MCP
connection lock around the shared RPC without changing no-replay semantics.

During construction run rustfmt plus minimal affected warnings-denied compile/Clippy.
Focused connected cases close each slice; broad installed/live/migration/load
qualification remains product-stage work.
