# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

**Documentation readiness: NOT READY for whole-project autonomous implementation.**
The current entry path, task-packet format and AR/PF prevention map are being repaired,
but package-level [source-audit coverage](agent-operations/source-audit-coverage.md)
still contains untraced surfaces. The generated
[readiness report](agent-operations/documentation-readiness-report.md) checks
classification and links; it does not prove source completeness. Agents may implement
only an owner-approved narrow packet at an exact revision. A branch or PR is not
landed behavior, and an implementation slice must not replace the outstanding audit.

Runtime slice was inspected and reduced on **2026-10-24** at
`3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb`. The active source audit remains
**incomplete at 170 entries**; AR-160–170 are frozen at the preceding gateway
checkpoint `0a228cd`. A1b is the first new runtime slice after that audit: it removes
the mandatory Antigravity dispatch refresh/session-proof chain while preserving
current Store binding/Open and adapter command fences.

Merged work immediately preceding this checkpoint:

- PR #188 at `e99c06b`: AR-100–114 plus removal of disconnected GitHub
  PR/projection code, net **−250 Rust lines**; exact head `0a5a099`, run
  `40087522775` passed scoped Ubuntu/Windows compile and changed-line Clippy.
- PR #189 at `053f1e0`: AR-115–126 plus the numeric Store-evolution contract; exact
  head `3c05253`, run `40088525487` completed successfully; documentation only.
- PR #190 at `c73c36c`: AR-127–141 plus the IPC/MCP contract; exact head
  `a93ab20`, run `40090027947` passed changed-path and Markdown validation;
  documentation only. J0 remains selected but not landed.
- PR #191 at `b61c0bd`: AR-142–159 plus the Governor-observation contract;
  documentation only. No R0–R6 collector/runtime implementation is claimed.
- PR #192 at `0a228cd`: AR-160–170 plus gateway target/dependency documentation;
  documentation only. No V0–V3 gateway runtime implementation is claimed.

- [Current resume point and findings](agent-operations/audit-current.md#resume-here)
- [Evidence/source index](agent-operations/audit-index.md)
- [Architecture](agent-operations/architecture.md)
- [Single implementation program](agent-operations/implementation.md)
- [Gateway target](gateway.md)
- [Gateway dependency register](agent-operations/gateway-dependency-register.md)
- [Observability target](agent-operations/observability.md)
- [IPC/MCP target](agent-operations/ipc-mcp.md)
- [Store evolution target](agent-operations/store-evolution.md)
- [Agent communication target](agent-communication-program.md)
- [General donor register](agent-operations/donor-map.md)

## Runtime changes

| Change | Evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains. |
| Active Antigravity coordinate aligned on v6 | `cba98b7` | No installed native-turn qualification. |
| Terminal receipt decoupled from Store observation | `eeea355`, recorded net −265 production Rust lines | Crash-unsafe delivery remains; A1b removed the dispatch proof gate. |
| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930` | Scoped regression/Clippy only; no crash/installed/load proof. |
| Disconnected GitHub PR/projection modules removed | `98b350e`, net −250 Rust lines; run `40087522775` passed scoped Ubuntu/Windows gates | No PR-source feature or remote observation was added. |
| Antigravity dispatch refresh/proof gate removed | `3c7a84f`, net −305 production Rust and −20 qualification-glue lines; run `40131522836`, job `120984044559` | A2 durable delivery, old-boot completion and installed/crash qualification remain. |

```text
A0   LANDED                   active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  LANDED / SCOPED PASS     refresh/proof gate removed; current fences retained
A2   NEXT / NOT IMPLEMENTED   durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
B–V  TARGET WORK              largely unreduced current runtime
U/R  SMALL CLEANUP LANDED      disconnected GitHub PR/projection source removed
J0   SELECTED / NOT LANDED     release MCP slot lock before shared RPC
R0–R6 TARGET ONLY              source-owned joined Governor observation
V0–V3 TARGET ONLY              secret/session/origin/standalone gateway cut
```

When the owner resumes implementation, the next selected runtime slice remains
**A2**. It is not the next source-audit action and must not be used as a substitute for
finishing the package/domain coverage recorded in
[source-audit coverage](agent-operations/source-audit-coverage.md). J0 is a bounded
independent frontend fix; R0–R6 and V0–V3 remain later connected programs.

## Source-inspection result

| Area | Source-confirmed behavior | Open boundary |
| --- | --- | --- |
| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several useful delivery mechanisms; A1b removed the duplicate Antigravity refresh/proof admission gate. | A2 durable Antigravity delivery, first historical capture, continuation and live crash/power-loss. |
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
| Gateway, AR-160–170 | Fixed credential/profile, loopback/path/auth/body checks, no-replay IPC, timeout-as-unknown and RMCP worker cleanup are useful. | Absolute private secrets, credential cap, configured edge Origins, separate connection/session limits, explicit idle policy, standalone cutover and connected qualification. |

## Antigravity A1b status

A1b removes `AntigravityDispatchSessionProof`, its runtime DTO/constants, adapter
producer, latest-refresh Store query, binding/snapshot proof validator, mandatory
qualification refresh and assignment/lease/session digest packet. Dispatch now reads
the existing successful Open evidence and revalidates current Task/Attempt, route,
binding generation, boot/root, model and workspace authority. The adapter still rejects
foreign binding/root commands before native I/O. `agent.refresh` remains available as
readback and grants no dispatch right.

Run `40131522836`, job `120984044559`, passed affected rustfmt and warnings-denied
Clippy. No integration tests or installed/native qualification were run. A2 remains
open: all outcome/body/page delivery is still process-memory-backed and cannot yet be
completed safely by a successor boot.

## Gateway status

### Preserve

- One process maps to one configured Eliot credential and restricted MCP profile.
- Profile `expected_client_id` is checked before listener bind; Full is rejected.
- Loopback-only listener, exact `/mcp`, no query and one Authorization header.
- Constant-work bearer comparison after bounded header parsing.
- RMCP Host validation, present-Origin validation, streamed body cap and cancellation.
- Existing bounded TCP connection admission.
- `swarm-mcp` drops a failed IPC link and never silently retries the request.
- Timeout/disconnect/shutdown leave admitted Store Operations authoritative.
- RMCP 3.5.0 removes sessions after worker termination and has finite default
  initialize/idle/cache lifetimes.

### Repair

1. Replace gateway-local `std::fs` secret readers with one shared absolute, no-follow,
   bounded regular private-file reader based on `swarm-process::permissions`.
2. Verify existing secret owner/ACL/mode; current shared exact-file helper does not
   complete that check by itself.
3. Cap credential file and token before allocation; retain closed JSON and bearer
   32–512 ASCII-graphic checks.
4. Configure exact allowed edge Origins while preserving rejection for every
   nonmatching present Origin, including an empty list.
5. Make connection count configurable and add an independent active MCP session limit.
   Acquire before constructing a facade and release on failed initialize, DELETE,
   worker terminal, idle timeout and shutdown.
6. Pin gateway initialization/inactivity policy explicitly instead of relying on RMCP
   defaults; keep dependency cleanup as an implementation mechanism, not product law.
7. Keep request timeout separate from SSE/session lifetime and exercise slow/streaming
   clients.
8. Prove timeout after Store commit through original request/Operation readback without
   replay.
9. Remove `swarm-kernel-host gateway`, sibling launcher diagnostics, build-policy text
   assertions and compatibility fixtures after standalone callers move.
10. Add minimal affected rustfmt/Clippy/build and focused connected security/capacity/
    readback cases before live tunnel/browser qualification.

No V0–V3 runtime implementation or listener qualification is claimed by this
checkpoint.

## Governor observation status

Preserve binding/generation/boot/observation fences, explicit gaps, typed provider
sequence/evidence, PID birth/membership checks, workspace authority, exact Forge remote
readback and passive semantics. Repair through source-owned provider/process/Git/remote
collectors, assignment edges, joined WAL projection, split disclosure and deletion of
old parallel views. No R0–R6 runtime implementation is claimed.

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

Executed qualification is separate: providers, gateway edge/tunnel, secret ACLs,
process/Git collectors, host restart, Windows/Linux custody, PID reuse, migration/
corruption, concurrent MCP/load, fairness and autonomous end-to-end work. Source
inspection and a successful compile cannot substitute for those results.

## Next work

**Runtime A2:** retain complete Antigravity outcome/body/page Store-delivery parameters
in private atomic files before treating them as locally retained. Same-boot reconnect
may resend identical bytes; after verified prior-owner departure, an authorized
successor may deliver the original Operation with immutable origin boot/source identity.
Never persist or replay native prompt intent. Missing/partial capture remains visible
unknown and must not complete a newer Attempt.

**Subsequent A3:** keep only real typed actions and one active Antigravity route; remove
placeholder controls and concrete retired Node work after finite disposition.

Selected independent future slices remain J0 from
[IPC/MCP](agent-operations/ipc-mcp.md), R0–R6 from
[Observability](agent-operations/observability.md) and V0–V3 from
[Gateway](gateway.md). None is landed here.

During construction run rustfmt plus minimal affected warnings-denied compile/Clippy.
Focused connected cases close each slice; broad installed/live/migration/load
qualification remains product-stage work.
