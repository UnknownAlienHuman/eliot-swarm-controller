# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS_WORKING DRAFT /
RUNTIME_REFACTOR IN PROGRESS.**

Source was inspected on **2026-10-23** at
`38db378c559dd0d95cd60db26702ffa73a954258`. The latest runtime change remains
`c92120ed70ca7e625440a8a19517c481e73b13c9` from 2026-10-21. The **22 intervening
commits are documentation-only**; documentation progress is not runtime progress.

The active source register now contains **87 entries**. The latest pass traces Forge
publication, worker/process custody, exact remote-ref readback, managed labels, PR
metadata, automatic publication/projection, restart, transfer and ambiguous-write
settlement. It does not mark a runtime defect repaired and it does not claim live
Git/GitHub or installed-platform qualification.

- [Resume point and findings](agent-operations/audit-register.md#resume-here)
- [Evidence/source index](agent-operations/audit-index.md)
- [Architecture](agent-operations/architecture.md)
- [Single implementation program](agent-operations/implementation.md)
- [CheckRunner contract](check-runner.md)
- [ScriptRunner contract](script-runner.md)
- [Donor register](agent-operations/donor-map.md)

The 87 records include preserved behavior, scope corrections and partial boundaries,
not 87 bugs or a completion percentage.

## Landed runtime changes

| Change | Evidence | Remaining limit |
| --- | --- | --- |
| Duplicate host CLI/MCP launcher removed | `25a8e3b`, recorded net −1,206 Rust lines | Most runtime duplication remains. |
| Active Antigravity coordinate aligned on v6 | `cba98b7` | No installed native-turn qualification. |
| Terminal receipt decoupled from Store observation | `eeea355`, recorded net −265 production Rust lines | Dispatch proof and crash-unsafe delivery remain. |
| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930` | Scoped regression/Clippy only; no crash/installed/load proof. |

```text
A0   LANDED                   active v6 coordinate
A1a  LANDED / SCOPED PASS     terminal decoupling + producer lifecycle
A1b  NOT IMPLEMENTED          remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
B–V  TARGET WORK              largely unreduced current runtime
```

Runtime implementation still starts with **A1b–A3**. The audit sequence is allowed to
inspect later owners; it does not change implementation order.

## What source inspection has established

| Area | Source-confirmed result | Still open |
| --- | --- | --- |
| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several retained delivery mechanisms; cross-route loss/recovery gaps are named. | Durable Antigravity delivery, first historical capture, complete cross-route continuation and live crash/power-loss. |
| Queue/provider/resources, AR-036–050 | Existing root dedup and common claim/Open transaction are useful; history scans, stale precedence, cancellation and retirement gaps are concrete. | Corrected budget identity, attached-binding finalizer, installed concurrency/restart. |
| Goals/automation, AR-051–058 | Exact terminal/occurrence identity is useful; actor, phase, transfer and finite-budget gaps are concrete; native Goal support differs by route. | Ordinary bounded Goal messages, one loop, installed native mappings and restart. |
| Review/correction/acceptance, AR-059–064 | Direct replacement, all-findings feedback, same-Attempt resubmission and the frozen candidate/check/byte gate exist. | Automatic replacement, finite correction budget, fair subject isolation and common assigned-review/finding provenance. |
| Fixed source and checks, AR-065–070 | Exact immutable source, Store-Go-before-command, process custody, parser coverage, strict cache lineage and acceptance revalidation exist. | Automatic recovery of interrupted `source.capture`, standalone-only new CheckRuns and live OS/toolchain/load qualification. |
| Scripts, AR-071–078 | Script execution is one-shot, restart reconciles the retained family, parent completion and current child admissions are atomic, and automation lifecycle/transfer sources are exact. | Per-action identity/readback/retry, broader closed action vocabulary, pre-admission executor validation and exact mid-run cancellation. |
| Forge/GitHub effects, AR-079–087 | Forge remote-Git publication has strong write-before-effect, exact target/force-with-lease/readback and retained worker custody. PR reconciliation demonstrates immutable post-write completion authority. | GitHub REST writers are split; direct `GhCli` lacks retained custody; automatic label unknown recovery and post-write settlement are wrong; PR metadata lacks an automatic caller; publication/projection copy workflow state. |
| Other areas | Structural and selected source reviews remain indexed. | GitHub collection/import, Store migration, IPC/MCP, observation, mailbox/deletion and gateway completion. |

## CheckRunner status

The CheckRun producer is not an acceptance bypass:

- source capture binds exact commit/tree/blob/mode and revalidates manifest/bytes;
- CheckRun freezes trusted profile/tool/environment/scope inputs;
- command execution starts only after Store verifies exact worker identity and issues Go;
- restart reconciles one retained process family and never reruns the command;
- pass requires parser-specific complete coverage, source verification, output artifacts,
  exit zero and proven resource release;
- cache reuse follows one original direct process result and forbids cache chains;
- acceptance rechecks the original Operation/process/coverage/artifacts before and after
  off-DB byte verification.

Two implementation gaps remain:

1. `source.capture` has no automatic successor/current-state reconciler after a host
   interruption; exact caller retry can repeat the deterministic read.
2. `checks.executor=None` still routes new work through the in-host worker, while a pin
   routes to standalone `swarm-checks`. Both validate equivalent evidence, but the
   duplicate executor must be removed under implementation K.

## ScriptRunner status

The interpreter execution path already has useful effect discipline:

- immutable script revision, interpreter, invocation and represented Manager are
  rechecked before final reservation;
- standalone worker publishes exact process identity and cannot start before Store
  issues its token-bound `Allow`;
- restart reconciles the same process family and never launches a replacement script;
- result/stdout/stderr, parent terminal state and current typed child admissions commit
  together;
- identical terminal replay admits no second child;
- automation consumes exact bounded lifecycle projections and revalidates pending
  trigger intents after transfer/edit.

Current action handling is not implementation-ready:

1. child request identity hashes `(parent Operation, ScriptRun, effect kind)`, so two
   valid same-type actions collide instead of using `(ScriptRun, action_index)`;
2. rejected actions have no indexed lifecycle or exact retry path; rerunning the parent
   would risk duplicating actions that already applied;
3. `script.describe` advertises `operation.get`, but public `script.run` readback returns
   `result_status=not_projected`, hiding Store action outcomes and child Operation IDs;
4. the closed effect set is limited to Task creation and mailbox messages, not exact
   native Send/Reply/Configure/Goal where a real consumer requires them;
5. a missing executor pin creates a durable run which later fails before start instead
   of rejecting admission; the compatibility launcher remains;
6. queued cancellation is exact and starts no worker, but no public token-addressed
   mid-run ScriptRun cancellation path was found.

See [ScriptRunner](script-runner.md) for the retained boundary and finite cut.

## Forge and GitHub effects status

The remote-Git publication path has useful effect discipline:

- `forge.publish_ref` freezes the exact accepted candidate, commit/tree, repository,
  target ref, expected old target and create/update policy before external I/O;
- one target lane and retained native-worker custody prevent parallel/replayed pushes;
- the worker uses exact force-with-lease semantics and exact remote-ref readback;
- transport/CLI failure never authorizes another push;
- restart moves uncertain execution to readback-only and processes reconciliation before
  new publication dispatch.

The GitHub REST side is not implementation-ready:

1. labels execute through direct kernel-host `GhCli`, while PR title/body PATCH executes
   inside `swarm-forge-worker`; they do not have one API effect owner;
2. direct `GhCli` resolves bare `gh` and retains no executable/process-family custody;
3. host restart turns automatic label writes unknown, but the automatic drain handles
   only queued Operations and does not perform exact successor readback;
4. after a label write, disable/edit/transfer/source revision changes can prevent an
   exact observed desired state from settling the original Operation;
5. PR reconciliation has the correct immutable post-GET settlement pattern and should
   be retained when the API writer is consolidated;
6. publication and label projection retain copied cursor/pending/retry/drain lifecycle;
7. PR metadata remains a manual Manager tool with no enabled-policy caller.

## Required behavior to preserve

| Behavior | Target owner | Current gap |
| --- | --- | --- |
| Same durable Manager reconnect does not restart work | Task/Attempt/Operation + native readback | Adapter delivery/recovery gaps. |
| Native-manager and controller initial starts never both send | `Attempt.start_owner` | Preserve through TaskPacket/dispatch reduction. |
| Unknown native/external writes are never guessed-retried | Original Operation + exact readback | Several domains still lack automatic readback. |
| Complete attributable output survives control failure | StoreDelivery + source lifetime | A2 and cross-route result gaps. |
| One Manager owns one mutable worktree/Git index | Workspace lease + Manager Git ownership | `code.scope.*` and retirement integration remain. |
| Corrections return all findings and may resubmit same Attempt | Submission/review owners | Delivery classification and finite progress remain. |
| Acceptance proves frozen requirements for exact candidate | Common evidence gate | Direct review/finding provenance AR-064. |
| Required machine evidence is process/coverage/byte-backed | CheckRunner + acceptance | Standalone-only cut and live qualification. |
| Script execution and returned actions never replay together | ScriptRunner + indexed child Operations | Action index/readback/retry and cancellation gaps AR-071–078. |
| Harness Goal and Eliot messages are independent | Native adapter Goal + ordinary Send | Current progression machinery and route gaps. |
| Governor sees unmatched native/process/worktree facts | Independent collectors + joined view | Binding-rooted partial views remain. |
| Direct peer collaboration survives council deletion | One recipient-indexed mailbox | Coordination platform still exists. |
| External writes have one effect owner | Forge for remote Git; one GitHub API owner for REST writes | Forge is strong; REST owner split, direct `GhCli` custody and automatic unknown-readback gaps remain AR-081–087. |
| Handover preserves authors/owners/recipients/history | Stable identities + designation epoch | Mailbox normalization and cleanup gaps. |
| Restart never kills an unrelated PID | PID + birth/image/group/Job custody | Full platform race matrix unqualified. |

Deleting old machinery requires a connected positive path in the same slice. A manual
operator procedure, another workflow engine or a permanent compatibility facade is not
a replacement.

## Audit versus qualification

A source trace needs a named revision and a connected path:

```text
caller -> parse -> exact authority -> Store transition
-> external/native effect -> readback/recovery -> consumer
```

It must include cancellation, restart, late result, transfer and partial writes where
applicable. Source inspection does not prove installed behavior.

Executed qualification remains separate: real providers, Windows/Linux custody,
toolchain behavior, migrations/corruption, IPC fairness/load and the autonomous
end-to-end cycle. Most of it remains unexecuted.

## Next work

**Audit:** follow the GitHub source/import chain named in
[audit-register](agent-operations/audit-register.md#resume-here): source pagination and
revision identity, Issue/PR facts, Task mutation/work-pool coupling, automation import
and independent remote-ref observation. Prove collection, import and writes have
separate owners and that routine import does not fall back to a human.

**Runtime:** implement Antigravity A1b–A3 first. Use existing owners, remove replaced
paths in the same slice, and run format plus minimal affected warnings-denied Clippy/
build during construction. Broader connected and installed tests follow usable
vertical paths.

This checkpoint changes documentation only.
