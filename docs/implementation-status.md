# Implementation status

## Current state

**AUDIT_INCOMPLETE / RUNTIME_REFACTOR_IN_PROGRESS.**

Checked `main@c92120ed70ca7e625440a8a19517c481e73b13c9` on 2026-10-21.
This pass reconciles historical requirements, selected current code and active
architecture/delivery/implementation. It is not a full current-head source audit or
live fleet qualification. Previous audit-complete declarations remain withdrawn.

[Architecture](agent-operations/architecture.md) defines purpose;
[implementation](agent-operations/implementation.md) is the only work order;
[delivery](agent-operations/delivery.md) defines autonomous handoffs;
[findings](agent-operations/refactoring.md) and
[donor map](agent-operations/donor-map.md) retain detailed evidence.

## Landed code and exact verification scope

| Change | Landed evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removal | `25a8e3b`: recorded net −1,206 Rust lines | Not the full runtime reduction |
| Antigravity active v6 selection | `cba98b7`: Store selector, route, installer/package/qualification aligned | Not native turn qualification |
| Terminal outcome decoupled from observation | `eeea355`: recorded net −265 production Rust lines for that commit | Proof gate and in-memory delivery remain |
| One dispatch producer across terminal ERROR/CANCELED | `c92120e`: updates retained producer rather than appending another identity | Scoped connected regression, not installed-harness/crash/scale proof |

The failed repair at `912df90f` is historical, not the latest result. Run
[39579583956](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/39579583956),
job `119153571930`, passed the connected terminal regression and minimal Store Clippy,
then committed `c92120e` and removed its one-shot files. The run began on `bab493d9`
and checked a patched runner tree before publishing it; it is not a fresh full-product
run on every later commit. The commit diff was read in this pass. Recorded line
reductions are per-commit, not a new cumulative current-head measurement.

This documentation pass changes no runtime code and claims no new Clippy/test run.

## Active runtime slice

```text
A0   LANDED                 active Antigravity v6 coordinate
A1a  LANDED / SCOPED PASS    terminal decoupling and producer lifecycle repair
A1b  NOT IMPLEMENTED        delete dispatch proof and refresh prerequisite
A2   NOT IMPLEMENTED        durable Store delivery, including successor readback
A3   NOT IMPLEMENTED        only real typed actions and one active route
```

The original crash-loss defect is still open: `pending_receipts` can be lost when the
adapter exits before Store ACK. A2 must recover complete retained terminal evidence
through an authorized successor, not merely retry within the same process. A crash
before durable capture remains an explicit boundary. No native input is replayed.

## What this preservation pass corrected

| Documentation problem | Correction and evidence |
| --- | --- |
| Manual/explicit manager actions could be read as human approval at every step | Governor/Manager are agent roles; enabled policies connect the full work cycle through existing commands |
| `loop_step` equated to exact-turn steering | Native inbox timing and atomic expected-turn steering stay distinct; translate only semantics the selected harness actually supplies |
| Native-manager delegation disappeared from compact plan | Preserve existing `Attempt.start_owner` and `START_OWNED_BY_MANAGER` before native spawn/bind |
| Same-boot delivery spool described as solving crash loss | Add original-Operation/origin-boot reconciliation after previous delivery owner is fenced; no relabel/replay |
| Same candidate declared no-progress unconditionally | Relevant new evidence may progress an unchanged tree; new IDs or unrelated edits do not |
| Old missing-reviewer finding risked being reimplemented | Current `reviews.rs` already has Unanswered/Inconclusive/ReturnedForCorrection replacement; preserve it |
| Script wire consolidation did not preserve legacy controls | Current enum has three effects; use existing helpers or only required typed agent controls through normal Store authorization |
| Lease sounded like filesystem isolation; review source could move | One manager serializes Git operations; leaf edits are disjoint; review sees frozen content; native unrestricted shell is not sandboxed by Store |
| Removing coordination registry could remove participant rights | Keep authentication and scoped Task/reviewer grants; delete only the duplicate product machinery |
| Generic source/subject/edge vocabulary could grow into another framework | Keep concrete provider/process/worktree observations and joins; no generic entity graph or second database |

These are changes to target behavior and implementation instructions, not implemented
runtime features. Architecture was condensed; detailed behavior remains in delivery
and implementation rather than the lead developer's manifesto.

## Audit coverage and evidence limits

Prior work deeply inspected ownership and code-growth mechanisms across Core, adapters,
automation, Store, IPC/MCP, process/workspace control, review, observation, integrations
and gateway. Those labels are qualitative, not measured percentages or complete file/
branch coverage. Detailed source findings remain available in the existing documents.

This pass additionally read the original pre-refactor specification at `f1b854b6`, the
supplied October 6 audit and master audit v26, the retained Claude-script review,
current delivery/implementation, current start-owner guard, script effect enum,
review replacement enum, the latest Antigravity diff and successful repair job steps.
It reread Paseo's loading mechanism and current official Claude input/permission docs.
The private original script ZIP was not reopened or executed; its details were checked
through the retained review, not presented as a fresh archive audit.

Still unestablished:

- exhaustive retained-requirement coverage and all caller-to-consumer failure branches;
- full current public-method/object authorization coverage and execution matrix;
- all retained-format/migration/corruption/partial-upgrade cases;
- installed provider versions, native restarts and Windows process-family custody;
- IPC/Store fairness, mixed-provider behavior, scale and end-to-end autonomous operation.

## Next implementation and audit work

Implementation order stays in [implementation](agent-operations/implementation.md),
not repeated as another roadmap. Finish Antigravity A1b–A3 while preserving the repaired
producer path. Then proceed by connected slices with their deletions.

For each touched boundary, reconcile historical required behavior with actual callers,
authorizers, state transitions, effect/readback and result consumer. A source-backed
simplification is not demonstrated equivalence until that path is connected. Do not
turn this into a global audit gate before writing code.

Use minimal affected formatting/Clippy/build during construction. Focused connected
cases run when the slice closes; broad installed/native/load qualification follows the
product. Earlier live receipts and open Issues #1, #3, #4, #5, #11, #140 and #159 remain
scoped evidence, not proof that the new design works.

No claim that all errors have been found or that the refactor is complete is supported.
