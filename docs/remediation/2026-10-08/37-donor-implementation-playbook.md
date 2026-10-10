# R37 — donor implementation playbook: minimum code, exact ownership, deletion in the same slice

**ELIOT evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71`  
**Donor registry:** [`36-donor-registry.md`](36-donor-registry.md)  
**Field failures:** [`33-donor-field-reviews.md`](33-donor-field-reviews.md)  
**Purpose:** turn donor research and draft PRs into agent-executable implementation work without another framework, parallel authority, compatibility maze or dead helper layer.

This document does not create another remediation block. It tells an implementation agent **which existing ELIOT seam to use, which donor mechanism to adapt, which code path to delete and in what order to connect the production path**.

## 1. Required shape of every implementation

A change is accepted only as one connected vertical slice:

```text
public/admitted input
→ typed internal value
→ exact authority and owner check
→ durable intent or retained fact
→ effect / state transition
→ readback or exact terminal evidence
→ bounded public projection
→ deletion of the replaced implementation
```

A DTO, helper, trait, migration or donor dependency without the production caller in the same PR is not progress. A new caller without removal of the old responsibility is a second implementation and is rejected.

### Agent working rules

1. Work in the existing PR branch named by the task. Do not open a helper PR for a DTO, registry or common crate.
2. One manager owns one worktree. Writers edit only delegated files and do not run Cargo.
3. Start from the exact symbols named in the task; do not reread all audits or invent a new architecture.
4. Write the connected code first. After the complete slice, the manager runs scoped formatting and the smallest warnings-denied Clippy command named in the PR.
5. Broad tests, native qualification and load tests remain the final product phase. Record the exact future fixture names now; do not mark them passed by source inspection.
6. No `git stash`, no reset/clean over another worktree, no global format pass and no opportunistic cleanup outside the owned slice.
7. A dependency is added only when calling its stable API removes more local responsibility than it introduces. Similar source code is not enough.

## 2. Reuse order: use the smallest existing owner

Use this order. Skipping a level requires an explicit reason in the PR.

| Order | Action | Rule |
|---:|---|---|
| 1 | Call an existing ELIOT function/type | Preferred. Fix or extend its contract rather than creating a sibling implementation. |
| 2 | Move one existing private helper to the narrow common owner | Only after two real callers exist. Keep the helper private or crate-private. |
| 3 | Use a small permissively licensed donor crate through its public API | Adapter/process boundary only; feature-gate optional platform functionality. |
| 4 | Reimplement one small invariant from a reference donor | Use when importing the runtime would create a second authority or the license forbids copying. |
| 5 | Reject the donor product/runtime | Default for workflow engines, dashboards, second Stores and product shells. |

### Direct dependency candidates

These are **candidates**, not automatic dependencies:

- ACP Rust SDK: generated wire types, roles, request/notification distinction and capability negotiation inside an ACP adapter only;
- `portable-pty`: interactive PTY transport only;
- `alacritty_terminal`: headless VT parsing only;
- `rust-landlock` and `seccompiler`: optional Linux confinement with explicit degraded/unavailable state;
- Kingfisher scanner/rules: isolated bounded secret-scanning evidence producer;
- OpenTelemetry Rust: optional bounded exporter after Store commit.

Everything else in the current registry is semantics, failure fixtures or architecture reference unless a later source review proves a smaller direct API.

### License boundary

- Kandev is AGPL: semantics and fixtures only by default.
- systemd is LGPL: lifecycle semantics and tests, not copied service-manager code.
- Superset is ELv2 and is not a promoted donor.
- MIT/Apache does not justify copying a subsystem when ELIOT already owns the same responsibility.

## 3. Existing ELIOT primitives to reuse before writing new code

| Concern | Existing owner / symbol | Use |
|---|---|---|
| Process birth and image evidence | `swarm_process::process_birth_identity`, `process_image_identity`, `spawned_departed`; process `Group`/Windows Job ownership | Build one exact retained/live identity and prove family departure. Do not reconstruct platform JSON in each supervisor. |
| Module lifecycle | `crates/swarm-supervisor/src/supervisor.rs::{capture_identity,process_identity_is_live}` and host module supervisor | Extend the existing owner/status path; do not introduce another module registry or actor runtime. |
| Durable external effect | existing Operation admission/settlement in Store | Intent is retained before the effect. Lost reply becomes readback/outcome-unknown, never blind replay. |
| Result provenance | `results::load_claude_result_origin`, `normalized_result::validate_candidate_origin`, `TaskDispatchAdmissionReceipt::validate` | Add the missing expected-object comparisons; do not write a second provenance framework. |
| Workspace recovery | `workspace::reconcile_lease_for_launch` and retained lease/ticket facts | Reconcile exact retained identity. Do not infer absence from a provider list or directory check. |
| Check cancellation/finish | existing `checks::next` → worker → `checks::finish` path | Keep one terminal producer. Add truthful cleanup-pending state rather than direct settlement from unrelated paths. |
| Automation subject isolation | `automation_publication::consume_event_isolated` | Reuse its savepoint/disposition shape, then unify divergent classifiers. Do not add Restate/Temporal. |
| MCP live authorization | `ProfiledFacade::catalog_authorization`, `parse_catalog_authorization`, current method registry | Reuse one authorization producer/parser before target IPC. Method presence never replaces object authorization. |
| Canonical digests | existing `model::digest` and domain validators | One preimage per fact. Do not add another `is_sha256`/case policy. |
| Private file publication | the Command journal temp/write/sync/directory-sync sequence | Reuse the sequence by moving a narrow helper only after a second caller exists. Do not import one adapter from another. |
| Task/Attempt/Operation authority | current Store rows and immutable Attempt snapshot | Do not add a donor session/task database. |

Targets such as `subscriptions::poll_loop`, `scan_to_head`, manual identity reconstruction and legacy script execution are **replacement targets**, not reusable authorities.

## 4. Five canonical implementation shapes

### 4.1 External effect

```text
validate request and object authority
→ allocate caller-owned semantic identity
→ persist Operation intent
→ perform effect once
→ persist native admission/result, or outcome_unknown
→ reconcile by exact readback
→ settle same Operation
```

Never:

- send first and create identity later;
- convert timeout into retry permission;
- classify external failure by substring in a Store reconciler;
- generate a request ID inside a facade for an effect whose reply can be lost.

### 4.2 Reconcile loop

```text
subject key
→ load current authoritative state
→ classify Applied | Pending | Skipped | Quarantined
→ subject-local savepoint
→ commit state/cursor together
→ clear backoff only after proven progress
```

Hard SQLite/read/write/commit uncertainty remains an error. Domain damage may be quarantined only by an exhaustive domain classifier. `let _ =`, `.ok()`, catch-all prefix matching and whole-domain rollback from one malformed subject are forbidden.

### 4.3 Session observation

```text
authorized snapshot + committed_event_seq
→ consumer applies snapshot
→ discard events <= committed_event_seq
→ apply later events in sequence
→ explicit gap when retention/queue budget is crossed
```

A UI detach removes the registration only. It does not cancel Task, session, process or result.

### 4.4 Process completion

```text
direct child exit
+ exact family departure
+ stdout/stderr capture completion
= terminal release evidence
```

The three facts may settle at different times. Timeout ends caller waiting, not proof of cleanup. Preserve the process owner until departure is proven or explicit kill escalation is read back.

### 4.5 Host resource deletion

```text
durable deletion intent/tombstone
→ idempotent host delete verb under exact generation
→ host readback observes resource absent
→ purge mirror/tombstone
```

RPC success is loop control, not resource truth. A missing item in an eventually consistent list is never absence proof.

## 5. Ready implementation cards

## Card A — R01 / #27: module identity, readiness and stop ownership

### Files and symbols

- `crates/swarm-supervisor/src/supervisor.rs`
  - `capture_identity`
  - `process_identity_is_live`
  - `identity_from_process_image`
  - owner/worker receipt fan-in and status publication
- `crates/swarm-process/src/process_group.rs`
  - birth/image/family-departure helpers
- `crates/swarm-kernel-host/src/host_module_supervisor.rs`
  - generation ownership, restart and reaper paths

### Take from donors

- systemd: readiness is explicit; watchdog and stop timeout are separate; descendants are one owned unit;
- Ractor: control priority and explicit stop-and-wait; kill is final escalation;
- ACP field issue: wrapper PID is not the agent family;
- Petri: stale writer fencing across replacement;
- Emdash: retired generation cannot publish into replacement state.

### Implement in this order

1. Replace manual platform-specific birth JSON reconstruction with one constructor/parser owned beside `process_birth_identity`/`process_image_identity`.
2. Make retained owner and live worker receipts converge only when boot, PID birth, image and owner generation all match.
3. Recheck generation after every awaited I/O and immediately before publishing Running/Ready.
4. Preserve the child/process owner through graceful stop, deadline, kill escalation and departure readback; no `take()` that destroys the only handle on timeout.
5. Separate IPC disconnect, direct child exit and family departure.
6. Delete `identity_from_process_image` manual reconstruction and duplicate stop paths after callers switch.

### Do not build

- actor framework;
- PID registry beside `swarm-process`;
- silence-based kill;
- restart that replays the original model input;
- platform fallback that calls a weaker predicate but reports exact identity.

### Later qualification fixtures

`process_identity_round_trip`, `stale_generation_cannot_publish`, `wrapper_grandchild_departure`, `failed_stop_retains_owner`, `external_service_is_not_killed`.

### Minimal code gate

```sh
cargo clippy --locked -p swarm-supervisor -p swarm-process --lib --bins -- -D warnings
```

## Card B — R08 / #34: mailbox order, watch delivery and live subscription cutoff

### Files and symbols

- `crates/swarm-kernel-host/src/coordination/mod.rs::mailbox_key`
- `crates/swarm-kernel-host/src/store/coordination.rs::{send,inbox}` and recipient index writers/readers
- `crates/swarm-kernel-host/src/store/coordination_watch.rs::{reconcile,notifications}`
- `crates/swarm-mcp/src/mcp/subscriptions.rs::{poll_loop,scan_to_head}`

### Take from donors

- CCCC: stored/delivered/read/replied are different facts; read cost follows unread tail, not total history;
- Paseo: registration identity belongs to the source and detach cleans only the subscription;
- Pebble: snapshot and event receiver share one committed cursor;
- OpenHands: control surface lifetime is not runtime/conversation lifetime.

### Implement in this order

1. Allocate the delivery sequence from the existing committed observation/event order inside the same transaction as delivery admission.
2. Define one versioned cursor with last-scanned and last-emitted positions; do not infer order from timestamp + random UUID.
3. Write the recipient index before ACK and make batch delivery call the same writer.
4. For cursor-less live subscribe, capture one authorized high-water mark and return it in subscribe ACK. Do not call `scan_to_head` from zero.
5. Register source ownership and delivery queue budgets before releasing the snapshot/cutoff boundary so no event falls into a gap.
6. On overflow/retention, emit one explicit gap covering an exact range and advance covered-through only after the gap is queued.
7. Detach/unsubscribe removes the registration and demand only.
8. Stop writing Manager/Operator notice indexes that have no reader; keep historical rows readable.
9. Delete timestamp/UUID ordering and bootstrap-from-zero after all readers use the new cursor.

### Do not build

- second message broker or event database;
- in-memory cursor as authority;
- queue-size increase as the overflow fix;
- universal notification endpoint that broadens object scope;
- disconnect-triggered Task/session cancellation.

### Later qualification fixtures

`same_millisecond_delivery_not_lost`, `live_subscribe_exact_cutoff`, `slow_subscriber_explicit_gap`, `second_gap_does_not_regress`, `detach_preserves_task`.

### Minimal code gate

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-mcp --lib --bins -- -D warnings
```

## Card C — R12 / #38 and R34 / #60: fair scheduling and poison-fact isolation

### Files and symbols

- `store/automation_scheduler.rs::call`
- `crates/swarm-automation/src/bin/worker.rs`
- `store/launcher_issuance.rs`
- `Store::reconcile_automations_once`
- domain loops in review/WorkDispatch/publication/Goal/GitHub projection
- `automation_publication::consume_event_isolated`
- `automation_work_dispatch::recheck_pending`

### Take from donors

- Kubernetes controller stack: level-based reconcile, deduplicated subject key, terminal error, error backoff, scheduled requeue and Forget after progress;
- Restate: retryability is typed, not inferred from text;
- Petri: explicit outstanding commands and deterministic replay boundary;
- Fabro/Goose failures: admitted watchdogs must be wired; partial/failure must not be reported as success.

### Implement in this order

1. Introduce one Store-private disposition enum only where at least two live domain consumers switch in the same PR.
2. Move fallible per-subject work under subject-local savepoints; perform identity/classification before cursor advance.
3. Fix `recheck_pending` so a fallible item cannot drop the whole vector after `mem::take`.
4. Split the five independent automation domains into separate transactions/outcomes.
5. Preserve hard Store uncertainty; quarantine only known domain damage with bounded evidence.
6. Make scheduler due families independent for semantic errors but atomic for their own writes.
7. Use one no-progress backoff for stale/read/admit/already-observed paths and reset only on committed progress.
8. Make issuance cursor/backoff instance-owned and advance through actually examined items.
9. Delete process-global pacing state, prefix/string retry classifiers and divergent per-domain disposition copies.

### Do not build

- Restate/Temporal/DBOS server;
- generic workflow DSL;
- swallow-all error wrapper;
- infinite retry;
- cursor advance before subject disposition is committed.

### Later qualification fixtures

`poison_subject_does_not_block_sibling`, `db_error_remains_hard`, `pending_subject_is_retained`, `backoff_resets_only_after_progress`, `two_stores_do_not_share_cursor`.

### Minimal code gate

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-automation --lib --bins -- -D warnings
```

## Card D — R18 / #44: Command ACP session backend

### Files and ownership

- new ACP artifact/route alongside the current honest one-shot Command route;
- `swarm-contracts`: only the required typed session/config/update/permission forms;
- `swarm-process`: process family owner and bounded child lifecycle;
- kernel-host adapter dispatch/attention/readback wiring;
- current Command batch implementation remains separate.

### Take from donors

- ACP Rust SDK: generated wire types, roles, framing semantics, capabilities and request/notification distinction;
- Pebble: unfinished-session continuation, snapshot+cursor observation, steering queue semantics;
- agentsessions: Session/Harness/Runtime separation and capability matching;
- Kandev/OpenHands: session identity is independent of UI and survives control-surface reconnect.

### Implement in this order

1. Spawn ACP stdio under ELIOT `swarm-process` ownership; use a bounded frame reader before UTF-8/JSON decode.
2. Initialize and retain exact advertised capabilities before any session input.
3. Create session without model work and persist exact native session ID.
4. Apply model/effort/mode only through advertised configuration with exact readback; unsupported requested values fail.
5. Prompt through one durable Operation; fold typed updates into bounded state; terminal output binds to that Operation/session/generation.
6. Restore only after prior family departure is proven. Install routing before load/resume so early replay notifications cannot be lost.
7. Permission requests use existing ELIOT attention + `agent.reply` with exact native option ID/fingerprint. Persistent “always” is a separate policy mutation.
8. Explicit cancel is distinct from host IPC disconnect. Close only when advertised.
9. Observe native subagents from typed tool events; do not invent child RPCs.
10. Keep batch and ACP as separate artifacts. Delete no batch code merely to force both through one conditional implementation.

### Extraction rule

Do not create `swarm-adapter-sdk` in the first ACP PR. After R02 and R18 both have live callers, extract only duplicated bounded framing/journal/snapshot-cursor pieces and delete both copies in the extraction PR.

### Later qualification fixtures

`acp_restore_routes_before_response`, `acp_unknown_notification_ignored`, `acp_oversized_frame_bounded`, `acp_disconnect_is_not_cancel`, `acp_wrapper_family_removed`.

### Minimal code gate

```sh
cargo clippy --locked -p swarm-contracts -p swarm-process -p swarm-kernel-host --lib --bins -- -D warnings
```

## Card E — R35 / #61: finite CheckRun, ScriptRun and probe completion

### Files and symbols

- `crates/swarm-checks/src/lib.rs::{wait_until_empty,finish_captures_after_group_empty}`
- check input/probe ownership in `checks/inputs.rs`
- `crates/swarm-script-worker`
- legacy `crates/swarm-kernel-host/src/scripts/runner.rs`
- Store CheckRun/ScriptRun settlement and resource hold paths

### Take from donors

- systemd: separate execution, watchdog and stop deadlines;
- Bazel REAPI: live logs and terminal result are separate facts;
- Ractor: stop-and-wait before kill;
- sandbox-driver: output retained/lost/incomplete are explicit; owner deletes managed workspace only after work stops.

### Implement in this order

1. Create one small private finite-completion component with three independent axes: direct exit, family departure and capture completeness.
2. Give execution, termination grace and capture drain separate trusted deadlines.
3. Make capture tasks interruptible/pollable; never drop a blocked join handle as cleanup.
4. At cleanup deadline return truthful cleanup-pending/outcome-unknown while retaining exact owner identity and partial bytes.
5. Keep the resource held until exact departure reconciliation settles the same Operation.
6. Preserve bytes/observed count/truncation/I/O error/completeness independently.
7. Migrate CheckRun first, then standalone ScriptRun, then probe.
8. Switch all new ScriptRun work to `swarm-script-worker`.
9. Delete the executable/capture loop from legacy `scripts/runner.rs` after the final caller switches. Keep retained historical receipt decoding only.

### Do not build

- PTY for finite noninteractive jobs;
- second worker registry;
- authoritative cleanup in `Drop`;
- `resource_released:true` on timeout;
- empty fabricated capture after a reader error.

### Later qualification fixtures

`descendant_holds_pipe_after_exit`, `cleanup_pending_then_reconcile_once`, `capture_error_preserves_bytes`, `probe_drop_never_blocks`, `legacy_runner_rejects_new_work`.

### Minimal code gate

```sh
cargo clippy --locked -p swarm-process -p swarm-checks -p swarm-script-worker -p swarm-kernel-host --lib --bins -- -D warnings
```

## Card F — R13 / #39: workspace/resource ownership and deletion

### Files and symbols

- `store/workspace.rs::{reconcile_lease_for_launch,mark_lease_stale,release_lease}`
- `store/workspace_lifecycle.rs`
- launcher retained workspace ticket/readback paths
- capacity evidence readers used only as evidence, not allocation authority

### Take from donors

- Kandev: Task/session/worktree are separate identities; logical retirement precedes destructive cleanup;
- Emdash: host registry is resource truth; desktop/Store row is intent/cache; durable tombstone + reconcile;
- sandbox-driver: workspace ownership is explicit and managed deletion happens only after stop;
- Petri: lease/writer fencing; an eventually consistent list is not absence proof.

### Implement in this order

1. Keep the existing ELIOT lease/ticket rows as authority. Do not add a donor workspace database.
2. Resolve the exact owner from retained lease/start/Attempt/binding/service/check evidence; remove Task-wide queued-operation inference.
3. Distinguish desired deletion, delete attempted, cleanup pending, observed absent and purged.
4. Run idempotent deletion under exact owner generation and serialize per physical workspace/repository.
5. Read back exact path/Git administrative state/process owner before purging the retained record.
6. A failed caller timeout leaves deletion intent active; later reconcile continues it.
7. Remove dead `mark_lease_stale`/`release_lease` alternatives after the live path owns every transition.
8. Use provider listing only for best-effort sweep/adoption, never as proof that a recorded resource is gone.

### Later qualification fixtures

`loser_cannot_delete_winner_workspace`, `stale_git_admin_state_reconciled`, `delete_timeout_keeps_tombstone`, `provider_list_miss_not_absence`, `retired_generation_cannot_publish`.

## Card G — adapter/session common code: extract only after two callers

### Extraction trigger

Create a common adapter crate only when two completed adapters contain the same responsibility and the extraction PR deletes both copies.

Eligible responsibilities:

- bounded frame/record decoder;
- pending → delivered/acknowledged journal state;
- snapshot + committed cursor observation;
- exact route/steering queue accounting;
- exported session record cursor advancement;
- transcript fixture harness.

Useful donor APIs/semantics:

- Pebble `continue_prompt`, `observe`, `SteeringBus`, `export_for_reuse`, `SessionRecord::resume_after`;
- Petri `apply`/`verify_replay` as reducer and replay-test patterns;
- agentsessions Session/Harness/Runtime separation and fencing.

The common crate does **not** own:

- SQLite/Store;
- Task/Attempt authority;
- process isolation;
- provider credentials;
- vendor session state machine;
- model/tool loop.

Reject a proposed adapter SDK if its first PR adds more generic code than it deletes from the two callers.

## Card H — R22/#48, R26/#52 and R35/#61: CheckRunner and artifact evidence

### Take from donors

- Bazel REAPI: Action/Command/InputRoot digest identity and exact ActionCache key;
- Petri: replay tests and outstanding-command reconstruction;
- sandbox-driver: provider conformance and explicit output-loss facts;
- mini-SWE-agent: minimal trajectory baseline for measuring scaffold value.

### Use current ELIOT owners

Keep CheckRun, trusted profiles, exact candidate capture, artifact store and Operation authority. Do not implement remote execution or a second CAS service.

### Implementation rule

A reusable check result binds exact:

```text
candidate/input root digest
+ command/profile revision
+ environment policy
+ platform/backend capability
+ completion/capture evidence
```

Live logs are not the terminal result. Artifact digest proves bytes, not read authority. R22 links each requirement to exact profile revision and CheckRun. R26 applies one ArtifactReadGrant across get/read/parts/assemble. R35 supplies truthful finite-process completion.

Delete mutable-name cache assumptions, public unscoped artifact reads and any self-referential receipt check replaced by the exact identity.

## Card I — R14/#40, R24/#50 and R31/#57: method registry, MCP authorization and tool admission

### Take from donors

- AgentGateway: phase-typed data-only policy and fail-closed policy construction;
- MCPProxy: authorized snapshot, schema digest, quarantine, retrieve-then-describe;
- Kingfisher: bounded scanner rules/fixtures as evidence;
- Snyk Agent Scan: tool poisoning/shadowing/rug-pull threat fixtures.

### Use current ELIOT owners

- current method registry/policy;
- `catalog_authorization` + `parse_catalog_authorization`;
- Store object authorization;
- current MCP catalog/search.

### Implement in this order

1. R31 closes the native command set; unknown/unmapped command fails before demand/credential/effect.
2. R24 intersects fixed profile exposure with current Store `allowed_methods` before target IPC.
3. Object authorization remains in Store and runs again after dispatch races.
4. R14 moves data-only schema/registry ownership out of kernel-host→MCP dependency without changing exact public bytes/digests.
5. Only after the closed registry is stable, add progressive search/describe. Filter by profile/current auth/object prerequisites **before** ranking.
6. Approved tool snapshot stores exact schema digest. Changed schema is quarantined until explicit approval.
7. Secret/tool scanners run as isolated bounded evidence producers; scanner verdict never grants execution.

### Do not build

- CEL engine before local phase-typed structs prove insufficient;
- dynamic arbitrary-method dispatcher;
- schema cache keyed by user rights;
- ranking as authorization;
- fail-open unknown method;
- scanner that executes untrusted config on the host.

## Card J — observability and accounting

### Take from donors

- OpenTelemetry Rust: bounded asynchronous export and trace context;
- Langfuse: optional trace/cost UI;
- Pebble/Fabro field evidence: parent-only accounting and dropped descendant usage are not total cost.

### Rules

1. Store commit is first. Export is post-commit and optional.
2. Export queue has item+byte bounds, deadline and drop counters.
3. Exporter never runs on the Store writer thread.
4. Shutdown is bounded on current-thread and multithread runtimes.
5. Usage records identify root, descendant, compaction/tool-owned and unknown-priced components separately.
6. Missing price yields unknown total, not a false subtotal.
7. Exporter failure cannot roll back or redefine Operation state.

No observability dependency is an authority or readiness proof.

## 6. Serialized ownership and safe parallel work

Parallelize by non-overlapping authority, not by PR number.

| Serial group | Order | Main collision surface |
|---|---|---|
| Process ownership | R01/#27 → R02/#28 and R18/#44 → R35/#61 extraction/reuse | `swarm-process`, supervisor owner/stop contracts |
| Coordination | R06/#32 → R32/#58 or remaining R07 → R08/#34 and R09/#35 integration | `store/coordination.rs`, thread/contract records |
| GM/Object authorization | R30/#56 → R23/#49 → R25/#51 → R26/#52 | Store principal/object readers and authorization SQL |
| Native command/frontend | R31/#57 → R24/#50 and R29/#55 → R14/#40 | method registry, MCP facade and schemas |
| Task evidence | R20/#46 → R22/#48 → R21/#47; R27/#53 and R28/#54 after shared row shapes | Task/Attempt/Operation DTO and `operations.rs` |
| Automation | R12/#38 → R34/#60 → domain consumers in R21/R22 | automation loops, pending state, transaction boundaries |
| Workspace/resource | R13/#39 after R01 identity contracts; coordinate with R35 process release | workspace lease/lifecycle and resource evidence |

Adapter-specific work R03, R04, R15, R16, R17 and R19 may proceed in parallel when they do not change the same shared contract files. Rebase onto the shared contract owner; do not create a local duplicate to avoid waiting.

Compiler baseline #26 is required before claiming Rust qualification. It does not block source implementation in an independent branch and must not be copied into every PR.

## 7. Agent task-card template

Every implementation task given to an agent should contain exactly this information:

```text
TASK
- objective: one externally observable behavior
- owning PR/branch:
- invariant established:
- current broken public path:

READ FIRST
- exact files and symbols:
- existing helper/types to reuse:
- donor IDs and exact mechanism only:

IMPLEMENT
1. producer/input change
2. typed internal value
3. authority/owner check
4. durable write/effect/readback
5. public reader/projection
6. caller switch
7. exact old code to delete

DO NOT
- out-of-scope files/behaviors
- forbidden frameworks/fallbacks/aliases

FAILURE MATRIX
- before intent
- after intent before effect
- effect accepted, reply lost
- result observed before commit
- cancel/replacement race
- restart/replay

LATER QUALIFICATION
- exact fixture names
- native/platform cases

FINAL MANAGER GATE
- scoped fmt
- smallest `cargo clippy ... -D warnings`
- diff contains live caller and deletion
```

Do not give agents the entire audit, full PR history or all donor reports. The task card plus its owning handoff is the working context.

## 8. Automatic rejection: Frankenstein checklist

Reject the change before review when any item is true:

- new Store, broker, scheduler, workflow engine or authority beside the current Store;
- new generic framework before two connected callers;
- compatibility alias/fallback with no named removal condition;
- `if legacy { ... } else { ... }` maintaining two writers indefinitely;
- helper/type with no production caller in the same PR;
- new external effect without durable intent and readback plan;
- cursor advanced before related state/effect identity is committed;
- provider list/directory/PID used as definitive absence or ownership proof;
- UI/IPC disconnect treated as Task/session cancel;
- timeout reported as cleanup success;
- unbounded `wait`, `join`, channel, history scan or capture;
- error classification by text in Store/core when the adapter can emit a type;
- `serde_json::Value` added to a new domain boundary instead of a closed struct;
- duplicated digest, method, permission, state or terminal-result vocabulary;
- scanner/ranker/telemetry used as authorization;
- donor product imported whole when only one invariant is needed;
- AGPL/ELv2 source copied into ELIOT;
- code added without naming the exact old responsibility removed.

## 9. Definition of implementation-complete

The manager may mark the code slice complete only when all are true:

- the public production caller reaches the new path;
- the exact authority/owner is checked at the final effect/read boundary;
- unknown outcome has a readback path and does not replay blindly;
- all waits, queues, scans and projections have item/byte/time bounds;
- the replaced writer/helper/executor is deleted or can no longer accept new work under a named migration condition;
- no unrelated subsystem was introduced;
- scoped format and the PR's minimal warnings-denied Clippy pass;
- future behavioral/native fixtures are named honestly and not marked executed;
- PR body states donor revision/license, adapted mechanism, non-import boundary and deleted ELIOT responsibility.

The objective is not “use more donor code”. The objective is **one owner, one fact shape, one effect path and less ELIOT code after the migration**.
