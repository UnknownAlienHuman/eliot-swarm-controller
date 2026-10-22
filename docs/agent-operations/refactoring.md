# Refactoring — source-backed findings and deletion map

Current runtime findings were reconciled through
`c92120ed70ca7e625440a8a19517c481e73b13c9`; active target documents were reconciled
through `3e4bf9c24fd3024fcbf99107ab572e5bf56559d8` on 2026-10-22.
[Architecture](architecture.md) owns product law;
[implementation](implementation.md) owns the only work order;
[status](../implementation-status.md) records landed evidence.

This is not an exhaustive audit claim. It records confirmed ownership/code-growth
mechanisms, retained behavior and concrete deletion boundaries. A simplification is
valid only when its automatic positive path remains connected.

## 1. Retained behavior

| Required behavior | Retained owner/mechanism | Obsolete machinery may be removed |
| --- | --- | --- |
| Same Manager reconnect never restarts work | durable client, Task/Attempt/Operation and native readback | chat/link workflow state and marker files |
| GM handover preserves Attempt owners, authors, reviewers, candidates and historical recipients | GM designation/epoch + exact current grants | rewriting/adopting old objects |
| Native-manager delegation never receives a second controller initial prompt | `Attempt.start_owner`, `START_OWNED_BY_MANAGER`, exact producer binding | repeated dispatch receipts/proofs |
| Lost response never authorizes prompt/external-write replay | retained Operation/native identity + exact readback | transport retry guesses |
| Harness Goal and Eliot Goal coexist | adapter-native Goal + ordinary scheduled/event Send | vendor-branched Goal workflow/proof packets |
| Agents execute the full routine work cycle | one current-state policy loop using ordinary commands | WorkDispatch/review-repair/publication workflow engines |
| Native questions and permissions do not hang or default to deny/first choice | native policy + exact Reply + addressed Governor decision | queued Send masquerading as answer and manual relay |
| Corrections return all findings; missing reviewers can be replaced | current review result/replacement classes + one request-changes transition | ReviewDisposition/RepairDispatch slots/links/cursors |
| Acceptance proves frozen requirements for exact candidate | per-requirement evidence plan + exact CheckRuns/reviews | prose “done”, caller-authored proof copies and legacy policy branch |
| Governor observes native/process/worktree/provider facts locally | provider/process/Git/remote collectors + one joined Store view | optional diagnostics as product truth and newest-log/PID heuristics |
| One manager owns mutable Git/worktree; leaf writers are disjoint | Task path scope + workspace lease + manager Git ownership | `code.scope.*` proposal/override system |
| Results survive link/process failure without input replay | exact retained StoreDelivery + origin Operation/boot readback | memory-only pending receipts and universal journals |
| External writes happen at most once and settle by readback | one effect owner per domain | duplicate GitHub/Forge writers/projection workflow |
| Direct peer work remains available | normalized addressed mailbox + scoped grants + optional tiny Task thread | cards/contracts/integration/Concilium platform |
| Remote gateway exposes one fixed identity/profile | one loopback gateway + private credential loader | identity broker/proxy/database |

Historical implementation is not itself a requirement. Fixed-age rotation,
newest-session ownership, automatic “recommended” answers, silent model fallback and
independent lane daemons are rejected operating heuristics.

## 2. Landed source reduction and verification truth

### Duplicate host CLI

`25a8e3b` removed the duplicate host application CLI/MCP launcher: host `main.rs`
1,949 → 743 physical lines, recorded net −1,206 Rust lines.

### Antigravity

- `cba98b7`: active Rust coordinate aligned on v6.
- `eeea355`: terminal receipt no longer requires Store observation ID or duplicate
  `local_execution_results`; recorded net −265 production Rust lines for that commit.
- `c92120e`: one dispatch producer is updated across SUCCESS/ERROR/CANCELED instead of
  appending another identity. Run `39579583956`, job `119153571930`, passed the
  connected terminal regression and minimal Store Clippy before publishing it.

Still open: dispatch proof/refresh gate, crash-unsafe in-memory delivery and extra
command/profile surface. Installed harness, crash and scale behavior remain
unqualified.

## 3. Repeated causes of growth

### One fact becomes several products

The same decision is repeatedly represented as domain state, JSON receipt, proof
packet, workflow state, cursor family, compatibility profile and qualification
fixture. Refactoring must choose one owner and delete the others, not relocate them.

### Metadata becomes behavior and permission

Artifact version/build, schema inventories, document editions, source checkout,
compiler/cache path and payload hashes select runtime behavior or authorization.
Target behavior uses authenticated actor/object scope, current revision/generation/
boot and semantic capability. Verify package bytes at install/launch and artifact bytes
at storage boundaries.

### Receipts echo Store-owned identity

Task/Attempt/revision, request/source/prompt hashes and nested package/protocol receipts
are repeated across dispatch, outcome and result. Store already retained the Operation
and authenticated binding. Keep stale generation/boot guard, native identity,
disposition and real immutable-byte integrity only.

### Adapter persistence becomes universal journals

Claude, Codex, Command, Muse and OpenCode combine native intent, Eliot delivery,
results, observations, callbacks, startup inventory, tombstones, compaction and
retention. Use only actual lifetimes:

```text
SessionRecord
NativeEffectRecord
StoreDelivery
ObservationCheckpoint
persisted/readable result where required
```

Do not extract another shared event-sourcing framework.

### Parallel executors survive every migration

| Runtime | Current duplication | Selected target |
| --- | --- | --- |
| Antigravity | Rust + retained Node profiles | one Rust route after A1b–A3 |
| OpenCode | built-in host + standalone Rust | one exact live-qualified standalone route |
| Codex | Rust + retired Python/profile history | one Rust app-server route |
| Muse | active Node + disabled custom Rust | simplify Node `.13`; retire custom Rust |
| Claude | Rust/SDK + retained Node generations | one Rust adapter with selected official SDK driver |
| Command | Batch profile ladder + ACP generations | one Batch + one stable ACP v1 |

Completed history does not keep an executor or permanent translator alive.

<a id="code-reduction-candidates"></a>
## 4. Confirmed workflow/authority duplication

### WorkDispatch copies Task lifecycle

It retains global/per-entry cursors, pending subjects, activation/catch-up state,
rechecks, semantic slots, links and quarantine. Its 128-item pending behavior can hide
later eligible work. Replace with a ranked current query and one atomic
`dispatch_once`; reuse suitable sessions according to explicit continuation policy.

### Automation action registry hides missing ordinary actions

Current `AutomationStep` lists ten workflow steps and `has_consumer()` returns true for
all, making `capability_gap()` ineffective. `supported_action_for()` maps only some
steps; ReviewDisposition, RepairDispatch and Acceptance rely on separate consumers
rather than one ordinary Store action.

This is why “all steps have a consumer” does not prove a complete positive path.
Target actions are:

```text
dispatch | review | return_changes | accept | check | goal_message | script
optional source_import | publish
```

Each queries current state and calls the ordinary domain handler. Remove old enum
values, consumers, policy, config/schema, worker demand, cursors, slots, links and
fixtures in the same connected slice.

### Review/correction repeats one candidate relation

`automation_repair.rs` duplicates review/candidate/manager identity in semantic slots,
operation links, entry links, finding packages and digests after an explicit review
disposition and feedback chain. The domain already owns `review.submit`,
`task.request_changes`, review replacement and candidate evidence.

Use one `return_changes` action: select current actionable findings, apply one exact
Task transition and admit one correction Send when owner is live. Preserve current
`ReviewReplacementClass::{Unanswered, Inconclusive, ReturnedForCorrection}`. Do not
reimplement a fixed missing-reviewer defect.

### `code.scope.*` duplicates workspace authority

It models paths/symbols/interfaces, proposals, digests, modes, override and expiry but
does not own the worktree. Task path prefixes and Attempt workspace lease are the
actual controller write authority/conflict guard. Preserve role/object grants and
manager Git ownership; delete the advisory product.

### Capacity copies Operation lifecycle

Capacity phases/evidence/refusal rows and meta indexes mirror Operations and include
read/control methods. Count/reserve unresolved root opens and unreleased root bindings
inside launch admission; provider usage/availability is source observation;
`report.capacity` is a projection.

### Scheduling is triplicated

Host scheduler, standalone scheduler service and automation loops overlap. One
in-process Store-change/nearest-due/safety loop admits ordinary Operations. Separate
workers remain only for actual external I/O/custody.

## 5. Task, prompt, review and acceptance findings

### TaskPrompt spreads controller proof through every runtime

`TaskPromptEnvelopeV1` is consumed by Store, batch/zed paths and all major adapters.
Model input includes controller identity/JSON/hash structure, while adapters echo it
through dispatch context and admission receipts.

Target TaskPacket renders goal, requirements, current initial/continue/correction
state, selected sources, write boundary, workspace, route settings, deliverable and
verification. Store retains Task/Attempt/Operation/binding identity. Migrate consumers
route by route; do not introduce a flag-day prompt format.

### Verification criteria are not bureaucracy to delete

Current V2 acceptance has a frozen `requirement_evidence_plan`; every requirement must
name at least one evidence source. Reviews, checks and acceptance already validate it.
Preserve that semantic plan while removing duplicate hashes/envelopes. Expose meaningful
criteria in TaskPacket so an agent can finish the work; never reduce acceptance to
prose confidence or “no new errors” alone.

### Manual and automatic acceptance must converge

One common gate resolves current candidate-specific review/check evidence and frozen
requirements. Manager/Governor supplies a decision/reason, not another auditor result.
Candidate B needs B's evidence. Known unrelated baseline failures remain separate under
selected policy, but differential cleanliness alone proves no requirement.

## 6. API, Store and frontend findings

### Public API ownership is fragmented

Method registry, MCP catalog/profiles, request validation, Store routing, role checks
and visibility SQL each encode part of the surface. A retained domain owns typed parse,
exact role/object/revision authorization and handler. Public, internal-worker and
adapter protocols separate. Removing a product removes all its schemas/routes/policy/
storage together.

The old MCP profile document still claimed an already-fixed operator-only Task defect;
active docs now use current `require_task_planner` behavior. Stale defect text is itself
a code-growth hazard because it causes agents to add unnecessary grants.

### Store evolution protects historical SQL text

Base SQL SHA, extension digests, reference database and DDL-token checks protect source
text rather than current schema semantics. Use application ID, numeric `user_version`,
ordered transactional migrations and bounded postconditions. Migrate concrete retained
data only.

### MCP underuses multiplexing and duplicates wake paths

`swarm-client::Client` correlates concurrent requests while MCP retains a connection
mutex across calls. Clone the current client and release the mutex before RPC. Replace
per-subscription poller/forwarder/queue stacks with one coalesced wake pump; Store
history is replay authority.

### Hot reads perform broad work

`tasks/get` pages global attention to find exact pending input. Mailbox reads scan the
global observation timeline. Native in-memory ring operations run through Store writer.
Add exact addressed reads; move ephemeral ring work outside writer after a short
authorization snapshot.

## 7. Governor observation findings

Current inventory starts from ready bindings, so unmatched provider sessions/processes
and surviving shared services can disappear. Process metrics is a separate selected
Windows CLI path and repeats executable verification. Workspace lease shows launch-time
baseline/clean evidence rather than current branch/HEAD/dirty. Remote-ref readback is
attached to Forge publication. Provider usage/conditions remain binding/artifact
oriented.

Target uses concrete provider/process/worktree/remote sources with epoch/cursor/
freshness/coverage, current native/process/Git facts and assignment edges in one joined
WAL projection. Ordinary reads invoke no provider/model/filesystem/network call.
Do not build a generic entity graph or telemetry warehouse.

## 8. Mailbox and coordination findings

Current message bodies/identities live in settled Operation JSON; `message.read` scans
the common observation timeline. One message may have request ID, Operation ID,
`cmsg-*` ID and random delivery ID. Reply/cancel lookup uses JSON expression indexes
and corruption sentinels.

Normalize recipient-indexed mailbox rows keyed by Operation/message ID. Preserve
sender, recipient, Task/Attempt/thread, reply/cancel and cursor. Same client reconnects;
GM handover changes designation/epoch but never rewrites historical recipients.

Delete duplicate participant directory, cards, consult vocabulary, speech-act thread
workflow, watches, contracts/ratification, integration ACK, Concilium and code scope
after current callers move. Preserve base authentication, scoped worker/reviewer
access, direct peer mail, review and optional tiny grouping thread. Define automatic
wake/resume so simplification does not become a human relay.

## 9. External integration and gateway findings

### GitHub

Current poll wraps a GET in Operation/lease/recovery state, pages mutable `updated desc`
and in the same transaction stores facts, mutates Tasks and updates a separate work
pool. API writes have direct-label and Forge-PR owners; automatic projection adds
activation/catch-up/quarantine/drain workflow state.

Separate:

1. bounded source collector with stable coverage/high-water;
2. policy-selected Task import in a separate mutation;
3. one GitHub API effect owner with durable sending and exact readback;
4. Forge for remote Git only;
5. independent remote-ref observation.

Enabled policy may import/publish automatically through ordinary actions. Removing the
projection engine must not create a manual publication requirement.

### Gateway

Keep loopback-only, exact path, one credential/profile/bearer, Host/Origin/body/header/
request/connection bounds and timeout-as-unknown. Replace plain secret-file reads with
the shared private bounded loader, cap negotiated sessions/idle lifetime and remove the
compatibility launcher after callers switch. One process is one Eliot principal; no
identity broker/proxy/database.

## 10. Packaging, modularity and documentation findings

Active `modularity.md` still described the full descriptor/schema echo,
`ModuleReceiptIdentity` and hashed `swarm.task_prompt@1` as target after implementation
had selected a compact semantic protocol. `configuration.md` retained workflow actions
scheduled for deletion. `module-installation.md` left Muse executor selection open and
called product qualification manual. `mcp-profiles.md` repeated an already-fixed Task
planner defect.

These active contradictions were corrected in `3e4bf9c`: target protocol major +
semantic capability subset, one selected runtime route, agent policy actions, automated
public qualification and current profile boundary. Historical docs remain evidence,
not active requirements.

Package existence does not justify a process or permanent crate. A separate process is
for native SDK isolation, external I/O/custody or real fault containment. Conditional
donors such as CCCC remain conditional until a connected path needs them and deletes
current code.

## 11. Audit gaps

The audit remains incomplete. Before a final-complete claim:

1. trace every retained public action from current caller through typed parse,
   authorization, Store transition, effect/readback and final consumer;
2. include cancellation, restart, late result, transfer and partial-write branches;
3. map every retained historical requirement to current owner and connected behavior;
4. recheck findings after each runtime slice and remove stale/contradictory active docs;
5. execute privilege and retained-format/migration/corruption matrices on the reduced
   surface;
6. qualify installed providers, Windows custody, IPC/Store fairness, mixed routes,
   gateway, autonomous end-to-end cycle and scale in the required environment.

Do not turn these gaps into another framework or global gate before coding. Close the
matching audit path while implementing each existing vertical slice and keep status
honest.
