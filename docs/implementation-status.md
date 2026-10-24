# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

Source was inspected on **2026-10-24** at
`e42c464d58db770cc5490f46f391843f9fec8df2`. The active audit contains
**114 entries**. AR-100–114 trace direct mailbox identity/storage, GM handover,
Participant access, Thread/cards/contracts/code scope/integration/Concilium/watch and
their public MCP/CLI surfaces.

The current branch also contains the first new production-code reduction after the
previous audit-only sequence:

- `98b350e614fa660fe1d8c53b6063bdac94d80f68` removes disconnected
  `github/pulls.rs`, `github/projection.rs`, their module declarations and the sole
  pull-origin helper: **net −250 Rust lines**;
- the deletion closes the disconnected-code portion of AR-098 only;
- it does not implement PR collection, GitHub remote observation or any replacement
  workflow;
- minimal affected format/compile/Clippy is tracked in PR #188. This document does not
  claim a pass until the workflow has completed.

- [Current resume point and findings](agent-operations/audit-current.md#resume-here)
- [Evidence/source index](agent-operations/audit-index.md)
- [Architecture](agent-operations/architecture.md)
- [Single implementation program](agent-operations/implementation.md)
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
| Disconnected GitHub PR/projection modules removed | branch commit `98b350e`, net −250 Rust lines | Minimal CI pending at this checkpoint; no PR-source feature added. |

```text
A0   LANDED                   active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
B–V  TARGET WORK              largely unreduced current runtime
U/R  SMALL CLEANUP LANDED      disconnected GitHub PR/projection source removed
```

Runtime implementation still starts with **A1b–A3**. Audit order and opportunistic dead
code deletion do not change the implementation order.

## Source-inspection result

| Area | Source-confirmed behavior | Open boundary |
| --- | --- | --- |
| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several useful delivery mechanisms. | Durable Antigravity delivery, first historical capture, continuation and live crash/power-loss. |
| Queue/provider/resources, AR-036–050 | Root dedup and common claim/Open transaction are useful. | Stable budget identity, cancellation and attached-binding retirement. |
| Goals/automation, AR-051–058 | Exact terminal/occurrence identity exists. | Actor/phase/transfer bugs, finite budgets and one ordinary loop. |
| Review/correction/acceptance, AR-059–064 | Replacement, all-findings feedback, same-Attempt resubmission and frozen evidence gate exist. | Automatic replacement, finite correction, subject fairness and common review/finding provenance. |
| CheckRun, AR-065–070 | Immutable source, Store-Go, process custody, parser coverage, cache lineage and acceptance revalidation exist. | Automatic interrupted source capture and standalone-only executor cut. |
| ScriptRun, AR-071–078 | One-shot interpreter, same-family restart and atomic parent/child admission exist. | Per-action identity/readback/retry, action vocabulary, executor precheck and mid-run cancellation. |
| External effects, AR-079–088 | Forge freezes exact candidate/ref intent, records `sending` before one write and settles by exact readback; label/PR slots are strict. | Sticky process hold, duplicate PR PATCH owner, mutable-source recovery dependency, manual unknown reconciliation and duplicate automation engines. |
| GitHub sources/import, AR-089–099 | Repository ID, safe GET retry, explicit partial coverage and local-divergence conflict are useful. | Stable pagination/cut, aggregate coverage, explicit import, live-work conflict, work-pool deletion and remote view. Disconnected PR/projection helpers are now removed. |
| Mailbox/coordination, AR-100–114 | Exact sender/recipient/scope checks, non-destructive reads, scoped Participant/reviewer access and GM epoch handover are useful. | Recipient mailbox rows; finite migration; participant-grant relocation; deletion of Thread/cards/contracts/code-scope/integration/Concilium/watch and every public/storage surface. |
| Other areas | Structural and selected source reviews remain indexed. | Store migration, IPC/MCP, observation and gateway completion. |

## Mailbox and coordination status

### Preserve

- One authenticated sender and one exact recipient.
- Immutable Operation admission and caller-owned request idempotency.
- Exact Task/Attempt/candidate/reviewer disclosure boundaries.
- Non-destructive reads and recovery after a lost notification.
- Reply/cancel relation to one exact prior message with deadline/digest checks where
  configured.
- Stable client identity across reconnect.
- GM handover changes designation/epoch only and never rewrites historical recipients,
  owners, authors or reviewers.
- Current Task/Attempt/native attention is a separate projection for the successor GM.

### Repair

1. Replace Operation ID + `cmsg-*` + delivery ID with one message identity: the admitted
   Operation ID.
2. Insert recipient-indexed mailbox rows atomically with `message.send`; page them by a
   recipient sequence rather than global Observation cursor.
3. Keep Task feedback, Check completion and other attention in their owning projections,
   not as mailbox message kinds.
4. Preserve exact scoped grants, but migrate launch/reviewer access away from the
   duplicate coordination participant directory before deleting it.
5. Reduce Thread to optional grouping only if a current consumer remains. Remove
   reasonability forms, speech acts, proposal/objection/ratification states and
   authority implications.
6. Delete work/contract cards, structured consult directory, contract negotiation,
   `code.scope.*`, integration cells/ACKs, Concilium and coordination watches after
   callers use mailbox, Task/source, workspace lease, review/check and ordinary
   subscription/automation owners.
7. Remove catalog, method policy, CLI, MCP profiles/subscriptions, Store routes,
   indexes/backfills, fixtures and compatibility aliases in the same vertical slice.
8. Migrate only concrete unresolved mail/thread/grant state. Completed Operations remain
   readable history and do not keep executable translators alive.

## GitHub source/import status

### Preserve

- Immutable repository-ID verification.
- Exact source request identity and safe retry of read-only GET after uncertainty.
- Explicit stale/partial/failure coverage rather than treating failure as an empty
  source.
- Immutable Issue origin keys and local Task divergence conflict.

### Repair

1. Separate source configuration/collection from Forge publication and local Operator
   calls.
2. Replace numeric pages over mutable `updated desc` with a stable cut or overlap plus
   periodic full reconciliation.
3. Make coverage converge across bounded passes, including selected comments/PR fields.
4. Commit source facts only during collection; direct/policy `source_import` owns Task
   create/revise.
5. Never revise beneath live work automatically.
6. Delete duplicate work-pool selection and use the ordinary queue after import.
7. Add independent remote-ref observation without granting push authority.
8. Keep the newly deleted PR/projection modules absent until an exact current consumer
   and end-to-end path exist.

## Source audit versus runtime qualification

A completed source audit needs a named revision and evidence for each retained action:
caller → parse → exact authority → Store transition → effect/readback → consumer,
including cancellation, late result, restart, handover and partial writes. It also
needs historical requirement mapping and a concrete replacement/disposition for each
deletion. This remains incomplete globally.

Executed qualification is separate: privilege/migration/corruption cases, installed
provider contracts, Windows custody, fairness/load and autonomous end-to-end work. Most
of that remains unexecuted. Source inspection cannot substitute for it, and a CI compile
does not qualify native providers or a migrated database.

## Next work

**Audit:** continue the Store bootstrap/migration chain named in
[audit-current](agent-operations/audit-current.md#resume-here): embedded base/extension
SQL, startup verification, Doctor readback, writer/WAL reader ownership and
partial-migration/corruption behavior. Record useful guards versus source-text schema
ceremony before implementation I.

**Runtime:** finish PR #188 verification for the bounded GitHub deletion. The main
implementation still begins with Antigravity A1b–A3: remove the refresh/proof
prerequisite, persist exact delivery/body/page records and retain only real actions.

During construction run rustfmt plus minimal affected warnings-denied compile/Clippy.
Broad and installed/live tests remain product-stage qualification.
