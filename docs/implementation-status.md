# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

Source was inspected on **2026-10-24** at
`c73c36c72d2c86cf710fa69e39ab07e2052c1f5e`. The active audit contains
**159 entries**. AR-142–159 trace binding-rooted discovery, provider source ownership,
process identity/liveness, workspace authority versus live Git, remote-ref observation,
the joined Governor view, disclosure boundaries and deletion of replaced surfaces.

Merged work immediately preceding this checkpoint:

- PR #188 at `e99c06b`: AR-100–114 plus removal of disconnected GitHub
  PR/projection code, net **−250 Rust lines**; exact head `0a5a099`, run
  `40087522775` passed scoped Ubuntu/Windows compile and changed-line Clippy.
- PR #189 at `053f1e0`: AR-115–126 plus the numeric Store-evolution contract; exact
  head `3c05253`, run `40088525487` completed successfully; documentation only.
- PR #190 at `c73c36c`: AR-127–141 plus the IPC/MCP contract; exact head
  `a93ab20`, run `40090027947` passed changed-path and Markdown validation;
  documentation only. J0 remains selected but not landed.

- [Current resume point and findings](agent-operations/audit-current.md#resume-here)
- [Evidence/source index](agent-operations/audit-index.md)
- [Architecture](agent-operations/architecture.md)
- [Single implementation program](agent-operations/implementation.md)
- [Observability target](agent-operations/observability.md)
- [IPC/MCP target](agent-operations/ipc-mcp.md)
- [Store evolution target](agent-operations/store-evolution.md)
- [Agent communication target](agent-communication-program.md)
- [Gateway target](gateway.md)
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
R0–R6 TARGET ONLY              source-owned joined Governor observation
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
| Observation, AR-142–159 | Binding/boot fences, typed provider evidence, process birth/membership checks, conservative workspace release and passive semantics are useful. | Independent source collectors, unmatched subjects, live Git/remote facts, joined WAL projection, split disclosure and old-surface deletion. |
| Remaining | Structural and selected source reviews remain indexed. | Gateway completion and untraced residual chains. |

## Governor observation status

### Preserve

- Binding/generation/boot/observation identity fences for private native evidence.
- Explicit unknown/stale/partial/unavailable gaps rather than fabricated zero/current
  values.
- Typed provider condition source sequence/evidence and invalidation semantics.
- PID birth/image and module-owner membership checks for custody/control.
- Workspace lease as assignment/write authority with conservative release.
- Exact remote-ref readback after an ambiguous push.
- Passive observation: no Task acceptance, reassignment, prompt retry, provider restart
  or process kill from a status read.

### Repair

1. Discover native/process/worktree/remote subjects from stable source owners, not only
   ready bindings linked to the current Manager's Task.
2. Retain assignment as an edge from a discovered subject to binding/Operation/Attempt/
   Task; absent edge means unassigned, not invisible.
3. Promote provider account/service/store identity to a source lifecycle independent of
   the binding that supplied one observation; preserve independent quota windows.
4. Replace caller-selected one-shot process metrics with one host collector that
   discovers/refreshes known processes and stores PID+birth/image/group/Job/liveness.
5. Display assigned workspace branch/baseline/scope beside independently collected
   repository identity, actual branch/HEAD/dirty/upstream/push target and observed time.
6. Add a read-only remote-ref collector independent of Forge publication/push authority.
7. Join SourceRecord/current subjects/assignment edges in one short WAL snapshot with
   source epoch, cursor, last attempt/success, freshness, coverage and gaps.
8. Split inventory existence/state permission from Task relation, transcript/result and
   host-private path disclosure.
9. Move all consumers, then remove binding-only runtime catalogs, duplicated provider
   lifecycle, process-metrics product CLI, static lease-as-live claims,
   publication-only remote status and parallel report reducers.
10. Keep collection independent of viewer count and optional diagnostic logging.

No R0–R6 runtime implementation or live collector qualification is claimed in this
checkpoint.

## IPC and MCP status

Preserve authenticated shared Client correlation, no-replay `OUTCOME_UNKNOWN`,
concurrent host intake, separate tool/pump links, live Store authorization and bounded
explicit lag. Repair the MCP outer lock, exact pending-input reads, bounded report
production, one pump and selective WAL read routing through
[IPC/MCP](agent-operations/ipc-mcp.md). J0 is **not landed**.

## Store status

Preserve DataRoot lock, one writer, pre-query busy timeout, pre-WAL identity validation,
`BEGIN IMMEDIATE`, rollback, no fallible postcommit validation, existing-only recovery
and committed WAL reads. Repair permanent version 1 plus SHA markers through
[Store evolution](agent-operations/store-evolution.md).

## Mailbox and coordination status

Preserve exact sender/recipient/scope, immutable Operation admission, scoped worker/
reviewer access and GM handover without historical identity rewriting. Repair through
recipient rows, grant migration and vertical deletion of duplicate coordination
products/public/storage surfaces.

## Source audit versus runtime qualification

A completed source audit needs a named revision and evidence for every retained action:
caller → parse → exact authority → Store transition → effect/readback → consumer,
including cancellation, late result, restart, handover, migration, partial writes,
source gaps and slow consumers. It also needs historical requirement mapping and
concrete data/code disposition for each deletion. This remains incomplete globally.

Executed qualification is separate: provider/process/Git collectors, host restart,
Windows/Linux custody, PID reuse, migration/corruption, concurrent MCP/load, fairness
and autonomous end-to-end work. Source inspection and a successful compile cannot
substitute for those results.

## Next work

**Audit:** continue the remote gateway chain named in
[audit-current](agent-operations/audit-current.md#resume-here): secret loading,
loopback/Host/Origin/auth/body/session bounds, fixed identity/profile, timeout-as-unknown,
compatibility launcher and disconnect behavior.

**Runtime:** the main program still begins with Antigravity A1b–A3. Selected independent
slices remain J0 from [IPC/MCP](agent-operations/ipc-mcp.md) and R0–R6 from
[Observability](agent-operations/observability.md); neither is landed here.

During construction run rustfmt plus minimal affected warnings-denied compile/Clippy.
Focused connected cases close each slice; broad installed/live/migration/load
qualification remains product-stage work.
