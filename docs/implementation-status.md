# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** `main@e6b03b72ccec03b378c70b883f51ba2f34fde8d2`
contains one completed runtime reduction and a source-backed refactoring program.
Runtime source is unchanged since `25a8e3b`; later commits through this revision are
documentation changes.

- [Architecture](agent-operations/architecture.md) — compact product law.
- [Implementation](agent-operations/implementation.md) — the only work order.
- [Source findings](agent-operations/refactoring.md) — confirmed excess and deletion
  boundaries.
- [Donor map](agent-operations/donor-map.md) — reusable mechanisms and limits.

## Audit coverage

The repository has **not** received a complete current-head, line-by-line re-audit of
every runtime path.

The last broad audit tracked in Issue #153 used `ed1df447482d87ced09b60a12b301a7b0cfc4092`
on 2026-10-14. Current `main` is 644 commits ahead of that baseline. That interval
added or heavily changed the Rust Muse adapter, Claude, Codex, Command ACP, OpenCode,
Store automation, launcher, process custody, observation, checks, scripts, Forge, MCP
and CI. Historical findings remain useful, but the old audit cannot qualify the new
source wholesale.

The current 2026-10-19 study re-read the active architecture and the largest ownership
and state-machine surfaces at source revision `9c6192a70382bb12a842f5b960cab904f95908a7`;
`e6b03b7` changes documentation only. Coverage is:

| Track | Current coverage |
| --- | --- |
| Product boundary and ownership | deep source review; compact target defined |
| Adapter command/receipt/package protocol | deep cross-route review; replacement boundaries defined |
| Antigravity dispatch/result/delivery | deep connected source review; first executable slice selected |
| Queue, WorkDispatch, provider state and capacity | deep source review; duplicate state machines identified |
| Workspace/code scope and process ownership | deep source review; one-authority target defined |
| Scheduling/optional workers | deep source review; three overlapping control systems identified |
| TaskPrompt/model context | connected source review; duplicate identity/hash/JSON path confirmed |
| Review/correction/acceptance automation | connected source review; disposition/repair workflow duplication confirmed |
| Passive observation and executor duplication | architecture-level plus selected source paths; not every adapter branch |
| Store schema/migrations, authorization and object reads | partial; prior defects/fixes reviewed, not a fresh exhaustive matrix |
| Claude, Codex, Command, Muse and OpenCode internal state machines | partial targeted review; no full current-head line-by-line audit |
| Checks, scripts, Forge, GitHub effects, mailbox/coordination and gateway | partial ownership/error-boundary review |
| CLI/MCP/IPC, installer, packaging and CI | partial; one CLI deletion landed, broad current-head verification not rerun |
| Installed/native behavior, Windows custody and scale | unqualified except named historical receipts and open owner-machine cases |

Therefore “architecture audit mostly complete” is accurate; “all code and all runtime
behavior checked” is not.

## Completed runtime source change

Commit `25a8e3b` removed the host's duplicate general application CLI and MCP launcher.

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines;
- public commands remain in `swarm-cli`;
- daemon, local administration and internal-worker entrypoints remain.

This is the only runtime-code reduction in the current preparation series.

## Current implementation sequence

```text
A0  synchronize active Antigravity v6 coordinates
A1  remove Antigravity observation/proof coupling
A2  add minimal durable Store-delivery spool
A3  keep only real typed Antigravity actions

B0  one current-state queue projection
B1  explicit ordered route policy; lane stays a view
B2  source-scoped provider availability/usage
B3  transactional root count; remove capacity ledger
B4  dispatch_once instead of WorkDispatch workflow state

C0  Task path-prefix write scope
C1  workspace lease as sole write authority; remove code.scope.*

D0  minimal process owner record
D1  conservative restart reconciliation

E0  one automation-entry schedule model
E1  one in-process wake/timer admission loop
E2  delete standalone automation scheduler service/protocol
E3  retain workers only for real external effects/custody

F0  compact model-visible TaskPacket
F1  separate Store identity from prompt text
F2  one renderer for initial assignment, correction and resume

G0  review_once current-state query
G1  one task.request_changes transition and optional child agent.send
G2  candidate-specific correction rounds and no-progress stop
G3  accept_once through the common acceptance gate
G4  reduce automation steps to thin policy toggles
```

Shared adapter plumbing, executor retirement, passive observation and remaining Store
reduction follow only after these vertical slices prove the boundaries.

## Confirmed structural gaps

| Area | Current source | Target |
| --- | --- | --- |
| Antigravity result | terminal outcome depends on a recorded observation and proof packet | self-contained outcome plus exact Store-delivery spool |
| Common commands | semantic actions mixed with vendor/internal methods | closed typed semantic commands |
| Module contract | artifact/version/schema matrices drive behavior | one current protocol major plus capability subset |
| Adapter bootstrap | each adapter repeats env/owner/hello/delivery logic | existing `swarm-process`/`swarm-client` owners after two consumers prove the common part |
| Receipts | package/protocol/request/Task hashes repeated through outcomes and results | authenticated link + Operation/binding identity; hashes only on actual bytes |
| Prompt | source text + raw identity JSON + brief JSON + launch packet injected into model context | bounded TaskPacket; identity retained in Store |
| Work dispatch | Task observations copied into cursors, pending subjects, slots and links | bounded ranked query plus one atomic admission |
| Provider state | Codex-specific proof matrix and sticky per-binding conditions | source-scoped availability/usage with explicit freshness |
| Capacity | second `meta` ledger mirrors Operation lifecycle and counts read/control methods | transactional root count and read projection |
| Review/correction | disposition, repair and acceptance engines repeat one review/candidate identity | shared Store transitions and thin `*_once` automation |
| Write scope | advisory code-scope product beside real workspace lease | Task path prefixes plus one workspace lease |
| Process ownership | owner record/resolver repeat package/protocol/contract coordinates | minimal process identity and conservative reconciliation |
| Scheduling | in-process scheduler, standalone scheduler and automation loop overlap | one in-process due-state admission loop |
| Runtime duplication | parallel executors remain for most harnesses | one current executor/format per retained runtime |
| Goals | native and Eliot Goals are separate; current progression still vendor-branched | native Goal capability plus ordinary scheduled/event `Send` |
| Passive observation | joined unbound inventory/process/Git/quota view incomplete | local provider + OS + Git facts readable without provider calls |
| Acceptance | direct and automated paths do not share all candidate evidence cleanly | one candidate-specific review/check gate |
| Store/automation | writer-heavy reads, SQL text SHA, `meta` indexes, transfer chains and optional engines | bounded readers, explicit migrations, stable identities and deleted duplicate products |
| Product breadth | council, ratification/ACK and automatic publication remain | outside compact-core completion |
| Tooling | qualification retains compiler/checkout/cache equality and artifact-specific validators | generic package/install boundary and route-specific live qualification only |

These are source findings and planned deletions, not completed runtime repairs.

## Runtime evidence retained

Existing receipts remain scoped to their named artifacts and revisions:

- [Muse exact normalized result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json);
- [connected Store batch cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json);
- [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).

They do not qualify the planned compact protocol, TaskPacket, queue, correction flow,
passive observation, executor deletion or the installed fleet.

## Remaining audit tracks before a “complete current-head audit” claim

1. Re-audit all source changed since `ed1df44` by logical owner, not commit order.
2. Rebuild the public method → authorization → Store mutation/read matrix.
3. Recheck all migrations and retained-format transitions against actual current
   readers/writers, including corruption and partial-upgrade paths.
4. Trace every external effect owner: native prompts, process launch/kill, filesystem,
   Git, GitHub, checks and scripts; verify one retry/readback owner and no guessed replay.
5. Complete per-adapter state-machine audits for current Claude, Codex, Command ACP,
   Muse, OpenCode and Antigravity implementations, then select executor retirements.
6. Audit IPC/MCP backpressure, paging, fairness and writer occupancy under realistic
   concurrency.
7. Re-run only the minimal current-head format/Clippy/build gates needed for audited
   packages; connected tests and live qualification follow completed product slices.
8. Perform owner-machine qualification for native restart, Windows process-family
   custody, mixed-provider execution and scale. Open Issues #1, #3, #4, #5, #11,
   #140 and #159 remain evidence boundaries, not proof of completion.

## Verification status

The latest study changes documentation only. Runtime implementation begins with
formatting and minimal affected warnings-denied Clippy/build checks, followed by the
connected cases in
[implementation verification](agent-operations/implementation.md#verification).

Do not interpret older broad CI or fixtures as qualification of the planned protocol,
prompt, queue, review/correction flow, observation or executor deletion. Record each
completed source slice and net owned-code change here when it lands.
