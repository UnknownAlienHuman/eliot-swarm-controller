# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** Current runtime source was audited through
`cefd74ee309e83660264d6ffb1a5b89d13af52f4` on 2026-10-20. Commits after the one
runtime deletion named below are documentation/audit changes unless explicitly stated.

- [Architecture](agent-operations/architecture.md) — compact product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Findings](agent-operations/refactoring.md) — confirmed excess/deletion boundaries.
- [Observability](agent-operations/observability.md) — current Governor source audit
  and R0–R6 implementation boundary.
- [Donor map](agent-operations/donor-map.md) — reusable mechanisms and limits.

The architecture and main overengineering mechanisms are deeply audited. The product
is not implemented according to the new architecture and the installed fleet is not
qualified.

## Audit coverage

| Track | Coverage |
| --- | --- |
| Product boundary, ownership and main overengineering mechanisms | deep |
| Adapter command/receipt/package protocol | deep cross-route review |
| Antigravity dispatch/result/delivery | deep connected review |
| Queue, WorkDispatch, provider state and capacity | deep |
| Workspace/code scope, process custody and scheduling | deep |
| TaskPacket and review/correction/acceptance | deep connected review |
| Public method routing/authorization ownership | deep structural review; full privilege matrix not executed |
| Store schema/migration ownership | deep structural review; corruption/partial-upgrade matrix not executed |
| IPC/MCP multiplex, subscriptions, task projection and native streams | deep source review; live load not qualified |
| Check, Script and Forge execution ownership | deep source review; one-executor deletions specified |
| Claude adapter state machine | deep connected source review; installed route not re-qualified |
| Codex adapter state machine | deep connected source review; installed route not re-qualified |
| Command Batch/ACP state machines | deep connected source review; installed agents not re-qualified |
| Muse Node/Rust state machines and upstream SDK | deep source review; installed route not re-qualified |
| OpenCode built-in/standalone state machines and current upstream contract | deep source review; selected installed contract not re-qualified |
| Passive Governor observation/process/worktree/provider join | deep source review; collectors and live fleet not qualified |
| GitHub effects, mailbox/coordination and gateway | partial |
| Windows/native restart, mixed providers and scale | unqualified except named receipts/issues |

“Every line and runtime behavior checked” remains false. Historical Issue #153 evidence
predates hundreds of source commits and is not current qualification.

## Implemented runtime reduction

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher:

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon/admin/internal-worker entrypoints remain.

No later documentation pass counts as runtime deletion.

## Current implementation sequence

```text
A0-A3  Antigravity v6, self-contained result, exact durable Store delivery
B0-B4  current-state queue, explicit routes, source provider facts, root count,
       dispatch_once
C0-C1  Task path scope and one workspace write lease
D0-D1  minimal process owner and conservative restart
E0-E3  one schedule model and one in-process admission loop
F0-F2  compact TaskPacket; Store identity outside model text
G0-G4  ordinary review/correction/acceptance transitions
H0-H3  separate public/internal/adapter protocols and domain authorizers
I0-I3  numeric migrations; delete SQL-text compatibility
J0-J3  concurrent MCP IPC, one wake pump, exact pending-input read,
       native content ring off writer
K0-K4  one Check executor, one Script wire, one Forge remote owner,
       no per-Operation executable hashing
L       shared adapter shell only after two current routes delete duplication
M       Claude: one session/effect/delivery/observation/result model
N       Codex: one app-server owner, native Goal, small lifetime-specific records
O       Command: one Batch profile and one stable ACP v1 route
P0-P6  Muse: retain/simplify Node route; finite custom-Rust deletion
Q0-Q6  OpenCode: one exact standalone contract; exact parent result/restart;
       delete built-in executor and giant journal machinery
R0-R6  independent source collectors and one joined Governor projection
S       delete optional product engines and compatibility/tooling bureaucracy
```

## Confirmed current gaps

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity | terminal result can be lost and outcome depends on proof/observation | self-contained terminal plus exact Store-delivery spool |
| Adapter bus | vendor strings, profile ladders, repeated hello/delivery and proof receipts | typed semantic protocol; shared shell only after two consumers |
| Execution | parallel generations for most harnesses | one current executor/format per retained runtime |
| Queue | WorkDispatch copies Task facts into workflow cursors/pending/slots | ranked current query plus atomic admission |
| Provider/capacity | provider proof matrix and second lifecycle ledger | source-scoped facts and transactional root count |
| Workspace authority | advisory `code.scope.*` beside real lease | Task path prefixes plus one lease |
| Scheduling | overlapping host/standalone/automation schedulers | one in-process due/current-state loop |
| Prompt | identity JSON, launch packet and hashes enter model/adapters | compact TaskPacket and Store-owned identity |
| Review/correction | repair/disposition/acceptance workflows repeat candidate state | ordinary candidate-specific Store transitions |
| Public API | parallel registries/routers include removal-target products | distinct protocols and domain authorizers |
| Store schema | base SQL SHA, extension digests and custom DDL verifiers | numeric transactional migrations |
| MCP | serialized frontend calls, many pollers, global attention scan | direct multiplex, one pump, exact addressed reads |
| Native streams | ephemeral ring work runs on Store writer | short authorization snapshot plus ring outside writer |
| Checks/Scripts/Forge | duplicate execution/wire/custody and per-effect hashes | one executor/wire/remote owner; shared OS primitives |
| Claude | generic journals, callback retention and memory-only terminal body | minimal native effect, exact Store delivery, persisted result |
| Codex | monolith/full checkpoint/event copies/tombstones | connection/session/effect/delivery/latest observation owners |
| Command | five profiles and two large persistence systems | one Batch, one stable ACP v1, minimal effect/delivery |
| Muse | active Node plus disabled custom Rust and separate journals | retain/simplify Node; finite Rust deletion |
| OpenCode | built-in host plus disabled standalone; old contract lacks parent edge | live-qualify one exact current contract; standalone only |
| Governor inventory | `agent.list/state/family` start from bindings | source/subject inventory first; assignment is an edge |
| Provider observation | route catalog requires ready current Task/Attempt binding; usage/conditions repeat binding/artifact identity | configured readers without Task/binding; source-scoped usage/availability |
| Process observation | one-shot receipt-driven Windows CLI, max eight selected processes; image rechecked each sample | one shared collector, PID+birth/image/group and bounded metrics |
| Workspace observation | lease exposes preparation baseline/clean state only | assigned facts plus independently observed branch/HEAD/dirty/upstream/push target |
| Remote observation | exact ref readback exists only in Forge publication/reconcile | independent bounded remote repository/ref cache |
| Observation surfaces | agent/report/runtime-catalog/diagnostic CLI each build a different partial view | one joined current projection and cursor invalidations |
| Product breadth | council, ratification/ACK and automatic projection remain | outside compact-core completion |
| Tooling | checkout/compiler/cache equality and artifact-specific validators remain | generic package boundary and route-specific qualification |

These are findings and planned deletions, not completed runtime repairs.

## Adapter decisions

### Claude

Retain one Rust adapter with a thin official SDK driver. Split live session,
NativeEffectRecord, exact StoreDelivery, latest Observation and persisted terminal
result. A lost JS callback resolver becomes unknown. Delete old Node generations after
finite transition.

### Codex

Retain the Rust app-server adapter and externally owned app-server. Replace the full
checkpoint/tombstones with SessionRecord, per-effect records, exact StoreDelivery and
latest observation. Map native Goal directly. Delete Goal proof packets, Python
executor and profile ladders.

### Command Batch/ACP

Keep one Batch and one stable ACP v1 route. Eliot `AcpV2` is an artifact generation,
not upstream protocol v2. Preserve `session/load` no-fresh-fallback recovery and
option readback. Module-owner Job/process group owns descendants.

### Muse

Keep `muse-sdk-1.3.0-bridge.13` as the only new-work executor during refactoring.
Retire disabled `muse-rust-controller.1@0.1.2` after concrete work disposition. The
official Rust SDK is a future replacement candidate only when it exposes the required
Operation-derived command-ID seam or a bounded low-level adapter deletes more than it
adds.

### OpenCode

Converge on `crates/swarm-adapter-opencode` as the only new-work executor after Q0–Q4.
Select and live-qualify one exact installed native contract that exposes durable inbox,
execution claim/restart terminal and assistant `parentID`. Bind results by exact
assistant ID + parent input ID + selected parts. Delete built-in
`runtime/opencode_v2`, legacy worker demand/MCP/owned-service paths and giant journal
machinery after finite disposition.

OpenCode has no reviewed native Harness Goal in the selected source. Eliot Goal sends
ordinary messages. The shared service remains externally owned.

## Passive Governor observation audit

The target architecture in
[observability](agent-operations/observability.md) is now source-backed.

Confirmed current structure:

1. `agent.list/state/family` is binding-rooted and cannot expose unmatched native
   sessions/processes.
2. `module_observations::live_reads` requires a ready live binding attached to one
   current unreleased Manager Attempt and chooses one generation per route.
3. module-supervisor public state omits PID, OS birth identity and process group/Job.
4. process metrics is a separate explicit CLI read with no Store join and repeats image
   verification rather than acting as a collector.
5. workspace leases retain launch-time baseline/clean evidence; lifecycle reconciliation
   performs no filesystem/Git read.
6. remote-ref readback is attached to Forge publication/reconciliation, so no publish
   means no current remote observation.
7. native usage/provider condition rows remain binding/artifact-oriented admission
   state; shared account overlap remains unknown.
8. optional diagnostics and report pages do not share one current-state reducer.

R0–R6 implements stable sources, provider-local readers, one OS collector, Git/remote
collectors, current subject/assignment tables, one joined WAL projection and deletion
of the replaced parallel paths. Unassigned subjects stay visible but grant no Task or
transcript rights.

## Runtime evidence retained

Historical receipts remain scoped to their named artifact/revision:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the planned protocol, queue, TaskPacket, correction flow, MCP,
adapter reductions, Governor collectors, executor deletion or installed fleet.

## Remaining audit tracks before a complete current-head claim

1. Finish GitHub/mailbox/coordination/gateway effect tracing and execute the public
   method → role → exact object → Store/effect privilege matrix.
2. Execute migration/corruption/partial-upgrade cases against concrete retained DB
   states after numeric migration is implemented.
3. Measure IPC/Store fairness and writer occupancy after J0–J3; source review is not
   load qualification.
4. Perform owner-machine qualification for provider restart, Windows process-family
   custody, mixed-provider execution and scale. Open Issues #1, #3, #4, #5, #11,
   #140 and #159 remain evidence boundaries.
5. Run minimal current-head format/warnings-denied Clippy/build for each landed runtime
   slice. Focused connected cases run when a vertical product path closes; broad tests
   follow the completed product.

## Verification status

This pass changes documentation only. No Clippy/build/test result is claimed.
Runtime implementation still begins with A0/A1 and proceeds by connected vertical
slice. Record each landed source slice and net owned Rust + JS + glue change here.
