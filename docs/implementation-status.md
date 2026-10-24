# Implementation status

## Current state

**SOURCE_AUDIT_INCOMPLETE / TARGET_DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

Source was inspected on **2026-10-24** at
`b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b`. The latest runtime change remains
`c92120ed70ca7e625440a8a19517c481e73b13c9` from 2026-10-21. The **23 intervening
commits are documentation-only**; documentation progress is not runtime progress.

The active audit contains **99 entries**. AR-089–099 trace GitHub source setup/poll,
mutable pagination, coverage, poll-time Task creation/revision, work-pool selection,
Issue semantics, disconnected PR/projection helpers and remote-ref observation.
No runtime defect is marked repaired and no installed GitHub behavior is qualified.

- [Current resume point and findings](agent-operations/audit-current.md#resume-here)
- [Evidence/source index](agent-operations/audit-index.md)
- [Architecture](agent-operations/architecture.md)
- [Single implementation program](agent-operations/implementation.md)
- [GitHub-source contract](agent-operations/github-sources.md)
- [External-effects contract](agent-operations/external-effects.md)
- [Donor register](agent-operations/donor-map.md)

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

Runtime implementation still starts with **A1b–A3**. Audit order does not change the
implementation order.

## Source-inspection result

| Area | Source-confirmed behavior | Open boundary |
| --- | --- | --- |
| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several useful delivery mechanisms. | Durable Antigravity delivery, first historical capture, continuation and live crash/power-loss. |
| Queue/provider/resources, AR-036–050 | Root dedup and common claim/Open transaction are useful. | Stable budget identity, cancellation and attached-binding retirement. |
| Goals/automation, AR-051–058 | Exact terminal/occurrence identity exists. | Actor/phase/transfer bugs, finite budgets and one ordinary loop. |
| Review/correction/acceptance, AR-059–064 | Replacement, all-findings feedback, same-Attempt resubmission and frozen evidence gate exist. | Automatic replacement, finite correction, subject fairness and common review/finding provenance. |
| CheckRun, AR-065–070 | Immutable source, Store-Go, process custody, parser coverage, cache lineage and acceptance revalidation exist. | Automatic interrupted source capture and standalone-only executor cut. |
| ScriptRun, AR-071–078 | One-shot interpreter, same-family restart and atomic parent/child admission exist. | Per-action identity/readback/retry, action vocabulary, executor precheck and mid-run cancellation. |
| External effects, AR-079–088 | Forge freezes exact candidate/ref intent, uses ancestry plus expected-ref lease, records `sending` before one write and settles by readback; label/PR slots and readbacks are strict. | Sticky manual process hold, duplicate PR PATCH owner, mutable-source recovery dependency, manual unknown reconciliation and duplicate automation engines. |
| GitHub sources/import, AR-089–099 | Repository ID, exact request identity, safe GET retry, explicit partial coverage and local-divergence conflict are useful. | Poll-time Task authority, unstable pagination, non-convergent coverage, ineffective work-pool selection, incomplete Issue semantics, dead PR/projection paths and missing remote view. |
| Other areas | Structural and selected source reviews remain indexed. | Store migration, IPC/MCP, observation, mailbox/deletion and gateway completion. |

## GitHub source/import status

### Preserve

- Source setup and every poll verify the immutable repository ID.
- A poll request/lease is exact; restarting an uncertain GET does not repeat a mutation.
- Failed reads remain explicit failures/partial coverage and are never treated as an
  empty source.
- A changed duplicate Issue inside one bounded page window rejects the unstable
  snapshot instead of silently choosing one version.
- Task origin uses immutable repository and Issue IDs rather than mutable names/numbers.
- If the local Task diverged from the last imported source spec, refresh reports a
  conflict instead of overwriting local intent.

### Repair

1. Decouple source setup/collection from Forge publication configuration and local
   Operator-only calls.
2. Replace persisted numeric pages over `updated desc` with a stable cursor/high-water
   or overlapping refresh plus periodic complete reconciliation.
3. Make coverage aggregate across bounded passes; a source with more than 400 rows or
   any comments must be able to converge after required data is captured.
4. Stop creating/revising Tasks inside the collector transaction. Add explicit direct/
   policy `source_import` against exact source revisions.
5. Never revise a Task beneath an unreleased Attempt. Surface one bounded import
   conflict/decision while preserving the frozen old Attempt.
6. Remove `github_work_pool_members` and `github.work_pool.*`: selection is currently
   not consulted by WorkDispatch, so unselected open Tasks can launch.
7. Define explicit close/reopen/comment/PR import policy. A closed Issue must not leave
   an unintended open dispatchable Task.
8. Retain only current facts and source revisions referenced by imports/effects; remove
   unused append-only fact chains.
9. Connect or delete the currently unconsumed PR/projection helpers.
10. Add independent remote repository/ref/head freshness/error observation.

See [GitHub sources](agent-operations/github-sources.md) for the connected target.

## External-effects status

### Preserve

- Forge admission binds the exact accepted candidate, commit/tree, canonical repository,
  target ref and expected old/missing ref.
- The worker proves expected predecessor ancestry and pushes once with
  `--force-with-lease=<target>:<expected>`.
- Store commits `sending` before worker authorization or GitHub mutation.
- Restart and ambiguous outcomes are readback-only; no fresh request ID authorizes a
  second write.
- Managed-label and PR readback validate immutable repository/resource identity.
- Exact resource slots prevent overlapping unknown effects.

### Repair

- Add an exact-evidence disposition for Forge's sticky process-tree hold.
- Move PR description PATCH out of `swarm-forge-worker` into the GitHub API owner.
- Automate bounded readback of unknown label/PR effects through ordinary Operations.
- Retain immutable effect targets independently of source/work-pool rows.
- Delete publication/projection cursor/pending/quarantine engines after ordinary owners
  and the host current-state loop are connected.

See [External effects](agent-operations/external-effects.md).

<a id="required-behavior-to-preserve"></a>
## Required behavior to preserve

| Behavior | Target owner | Current gap |
| --- | --- | --- |
| Durable Manager reconnect never restarts work | Task/Attempt/Operation + native/external readback | Adapter/effect automatic recovery gaps. |
| Native-manager and controller initial starts never both send | `Attempt.start_owner` | Preserve through TaskPacket/dispatch reduction. |
| Unknown native/external writes are never guessed-retried | Original Operation + exact readback | Several domains still require manual readback. |
| Complete attributable output survives control failure | StoreDelivery + source lifetime | A2 and cross-route result gaps. |
| One Manager owns one mutable worktree/Git index | Workspace lease + Manager Git ownership | `code.scope.*` and retirement integration remain. |
| Corrections return all findings and may resubmit same Attempt | Submission/review owners | Delivery classification and finite progress remain. |
| Acceptance proves frozen requirements for exact candidate | Common evidence gate | Assigned-review/finding provenance remains open. |
| Machine evidence is process/coverage/byte-backed | CheckRunner + acceptance | Standalone-only cut and live qualification. |
| Script and admitted children never replay together | ScriptRunner + indexed child Operations | Action index/readback/retry and cancellation. |
| Source collection cannot silently change product intent | Source records + explicit import | Current poll creates/revises Tasks and can revise beneath live work. |
| Unimported external facts cannot launch | Explicit import + ordinary queue | Work-pool selection is not a WorkDispatch gate. |
| Remote mutation has one owner and one write | Forge or GitHub API effect owner | PR PATCH split and automation/readback duplication. |
| Harness Goal and Eliot messages are independent | Native Goal + ordinary Send | Progression machinery and route gaps. |
| Governor sees unmatched native/process/worktree/remote facts | Independent collectors + joined view | Binding-rooted partial views and no independent remote ref. |
| Direct peer collaboration survives council deletion | Recipient-indexed mailbox | Coordination platform still exists. |
| Handover preserves authors/owners/recipients/history | Stable identities + designation epoch | Mailbox and effect-successor gaps. |
| Restart never kills an unrelated PID | PID + birth/image/group/Job custody | Full platform race matrix unqualified. |

Deleting old machinery requires a connected positive path in the same slice. A manual
operator procedure, new workflow engine or permanent compatibility facade is not a
replacement.

## Audit versus qualification

A source trace needs a named revision and the connected path:

```text
caller -> parse -> exact authority -> durable Store transition
-> external/native effect -> readback/recovery -> consumer
```

It includes cancellation, restart, transfer, late result and partial write where
applicable. Source inspection does not prove installed behavior.

Executed qualification remains separate: real providers, Git/GitHub service behavior,
Windows/Linux custody, migrations/corruption, IPC fairness/load and the autonomous
end-to-end cycle. Most remains unexecuted.

## Next work

**Audit:** follow the mailbox/coordination chain in
[current audit](agent-operations/audit-current.md#resume-here): durable send/read/reply/
cancel, global timeline scans, GM handover/resync and every coordination product/MCP
consumer. Preserve direct collaboration and historical recipients while proving the
finite deletion boundary.

**Runtime:** implement Antigravity A1b–A3 first. Use existing owners, remove replaced
paths in the same slice, and run format plus minimal affected warnings-denied Clippy/
build during construction. Broader connected and installed tests follow usable
vertical paths.

This checkpoint changes documentation only.
