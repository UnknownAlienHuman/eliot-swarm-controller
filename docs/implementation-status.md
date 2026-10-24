# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

Source was inspected on **2026-10-24** at
`e99c06bb490fe630eec70337ce5137a26e145d84`. The active audit contains
**126 entries**. AR-115–126 trace Store open ordering, numeric schema authority,
downgrade/future-version fencing, extension verification, reader/Doctor behavior,
product-storage retirement and the finite legacy-v1 transition.

PR #188 is merged at `e99c06b`:

- production cleanup `98b350e614fa660fe1d8c53b6063bdac94d80f68`
  removed disconnected GitHub PR/projection source: **net −250 Rust lines**;
- exact head `0a5a099f197b392401b7569879ad453fc2f102e4`;
- workflow run `40087522775` passed changed-path classification, documentation and
  rustfmt validation, all-target compile and changed-line Clippy on Ubuntu and Windows,
  plus selected IPC/native stream and Muse/Command boundaries;
- broad tests and installed/native qualification were not run.

- [Current resume point and findings](agent-operations/audit-current.md#resume-here)
- [Evidence/source index](agent-operations/audit-index.md)
- [Architecture](agent-operations/architecture.md)
- [Single implementation program](agent-operations/implementation.md)
- [Store evolution target](agent-operations/store-evolution.md)
- [Agent communication target](agent-communication-program.md)
- [GitHub-source contract](agent-operations/github-sources.md)
- [External-effects contract](agent-operations/external-effects.md)
- [Donor register](agent-operations/donor-map.md)

## Runtime changes

| Change | Evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains. |
| Active Antigravity coordinate aligned on v6 | `cba98b7` | No installed native-turn qualification. |
| Terminal receipt decoupled from Store observation | `eeea355`, recorded net −265 production Rust lines | Dispatch proof and crash-unsafe delivery remain. |
| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930` | Scoped regression/Clippy only; no crash/installed/load proof. |
| Disconnected GitHub PR/projection modules removed | `98b350e`, net −250 Rust lines; run `40087522775` passed Ubuntu/Windows scoped gates | No PR-source feature or remote observation was added. |

```text
A0   LANDED                   active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
B–V  TARGET WORK              largely unreduced current runtime
U/R  SMALL CLEANUP LANDED      disconnected GitHub PR/projection source removed
```

Runtime implementation still starts with **A1b–A3**. Audit order and bounded dead-code
removal do not change the implementation order.

## Source-inspection result

| Area | Source-confirmed behavior | Open boundary |
| --- | --- | --- |
| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several useful delivery mechanisms. | Durable Antigravity delivery, first historical capture, continuation and live crash/power-loss. |
| Queue/provider/resources, AR-036–050 | Root dedup and common claim/Open transaction are useful. | Stable budget identity, cancellation and attached-binding retirement. |
| Goals/automation, AR-051–058 | Exact terminal/occurrence identity exists. | Actor/phase/transfer bugs, finite budgets and one ordinary loop. |
| Review/correction/acceptance, AR-059–064 | Replacement, all-findings feedback, same-Attempt resubmission and frozen evidence gate exist. | Automatic replacement, finite correction, subject fairness and common review/finding provenance. |
| CheckRun, AR-065–070 | Immutable source, Store-Go, process custody, parser coverage, cache lineage and acceptance revalidation exist. | Automatic interrupted source capture and standalone-only executor cut. |
| ScriptRun, AR-071–078 | One-shot interpreter, same-family restart and atomic parent/child admission exist. | Per-action identity/readback/retry, action vocabulary, executor precheck and mid-run cancellation. |
| External effects, AR-079–088 | Forge and GitHub effects retain exact intent, one write boundary and readback. | Sticky process hold, duplicate PR PATCH owner, mutable-source recovery, manual unknown reconciliation and duplicate automation engines. |
| GitHub sources/import, AR-089–099/114 | Repository ID, safe GET retry, explicit gaps and local-divergence conflict are useful; disconnected PR/projection code is removed. | Stable source cut, aggregate coverage, explicit import, live-work conflict, work-pool deletion and remote view. |
| Mailbox/coordination, AR-100–113 | Exact sender/recipient/scope checks, non-destructive reads, scoped Participant/reviewer access and GM epoch handover are useful. | Recipient mailbox rows; finite migration; grant relocation; deletion of Thread/cards/contracts/code-scope/integration/Concilium/watch and all public/storage surfaces. |
| Store, AR-115–126 | Single-writer ownership, pre-query timeout, pre-WAL identity, immediate transaction, rollback and query-only WAL reader are useful. | Numeric v2 implementation, finite v1 bridge, downgrade fence, semantic postconditions, reader/Doctor cutover and deletion migrations. |
| Other areas | Structural and selected source reviews remain indexed. | IPC/MCP, observation and gateway completion. |

## Store status

### Preserve

- DataRoot lock and one durable writer.
- Caller busy timeout before the first SQLite query.
- Application identity inspection before persistent WAL configuration.
- `foreign_keys=ON`, WAL and FULL only after the database is accepted.
- One `BEGIN IMMEDIATE` transaction for bootstrap/migration/callback/postconditions.
- Rollback on initialization, migration or precommit verification error.
- No fallible validation after commit.
- Existing-only recovery open.
- Read-only/query-only status connection after writer readiness.
- Committed WAL snapshots visible while the writer holds an uncommitted transaction.

### Repair

1. Replace permanent `user_version=1` plus extension SHA markers with one complete
   numeric application version.
2. Consolidate current legacy physical state through a finite `1 → 2` bridge; create new
   databases directly at v2.
3. Reject future versions and foreign/nonempty untagged databases before WAL or any
   persistent mutation.
4. Remove exact base-SQL text digest as compatibility authority after the bridge.
5. Replace generic marker trust and special reference-DB/DDL-token implementations with
   migration-specific bounded semantic postconditions.
6. Keep domain migrations in kernel Store; keep connection/transaction mechanics in
   `swarm-store`.
7. Require exact current version for read-only and recovery connections; neither path
   migrates.
8. Make Doctor report application/current version, migration state and bounded gaps
   instead of the base SQL digest.
9. Attach a data-disposition and numeric removal migration to every product deletion.
10. Remove legacy digest/marker/reference machinery after all supported v1 databases
    have one connected bridge.

## Mailbox and coordination status

Preserve one authenticated sender/recipient, immutable Operation admission, exact
Task/Attempt/candidate/reviewer access, non-destructive reads, exact reply/cancel
relations and GM handover without historical identity rewriting.

Repair by normalizing recipient rows keyed by Operation ID; separating feedback/check
attention; moving assignment/reviewer grants before deleting the duplicate Participant
directory; reducing Thread to optional grouping; and deleting cards, consult,
contracts, `code.scope.*`, integration cells, Concilium and watches vertically with
MCP/CLI/storage/migrations.

## GitHub source/import status

Preserve immutable repository identity, exact safe GET retry, explicit coverage gaps,
immutable Issue origin and local Task divergence conflict.

Repair stable pagination/cut and aggregate coverage; separate collection from explicit
`source_import`; never revise beneath live work; delete duplicate work-pool selection;
add independent remote-ref observation; and keep disconnected PR/projection code absent
until a real end-to-end consumer exists.

## Source audit versus runtime qualification

A completed source audit needs a named revision and evidence for every retained action:
caller → parse → exact authority → Store transition → effect/readback → consumer,
including cancellation, late result, restart, handover, migration and partial writes.
It also needs historical requirement mapping and a concrete data/code disposition for
each deletion. This remains incomplete globally.

Executed qualification is separate: privilege/migration/corruption cases, installed
provider contracts, Windows custody, fairness/load and autonomous end-to-end work. Most
of that remains unexecuted. Source inspection and a successful compile cannot substitute
for migrated-database or provider qualification.

## Next work

**Audit:** continue the IPC/MCP chain named in
[audit-current](agent-operations/audit-current.md#resume-here): client multiplexing,
frontend connection locking, Store routing, subscription queues, exact hot reads,
disconnect and slow-consumer behavior.

**Runtime:** implementation still begins with Antigravity A1b–A3: remove the
refresh/proof prerequisite, persist exact delivery/body/page records and retain only
real actions. Store implementation I uses
[the numeric migration program](agent-operations/store-evolution.md), beginning with
numeric mechanics and the finite v1 bridge.

During construction run rustfmt plus minimal affected warnings-denied compile/Clippy.
Focused connected cases close each vertical path; broad installed/live/corruption/load
qualification remains product-stage work.
