# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source and active tooling were rechecked at
`7f85a00c69f3b52cc250ad7dbded91692078eb97` on 2026-10-18.
[Findings](refactoring.md) preserve evidence; [status](../implementation-status.md)
records completed work.

Do not start another repository-wide framework migration. Complete one
caller-to-result slice, delete its replaced path, then use a second real route to
prove what is genuinely shared.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>

## 1. Work as vertical slices

Every coding slice states:

- observable behavior;
- exact owners and symbols;
- unsupported and uncertain cases;
- old callers, formats, fixtures and configuration to delete;
- one relevant native API or donor mechanism;
- minimal format and warnings-denied Clippy/build command;
- connected completion proof and net owned-code change.

The manager owns shared contracts, Store transitions and integration. Leaf workers
receive compact non-overlapping tasks. A type, schema, fixture or adapter stub is not
progress unless a current caller uses it and the superseded path disappears.

Migration is route-by-route. An unmigrated route may temporarily keep its current
protocol, but receives no new compatibility profile. A migrated route admits no new
work through its old producer.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, acceptance and durable projections | `crates/swarm-kernel-host/src/store` |
| Shared semantic commands and bounded normalized records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `crates/swarm-client::ModuleLink` |
| Adapter membership and private durable files | `crates/swarm-process::module_owner` and `permissions` |
| Native transport, session state, provider identities, effect readback and local readers | selected `swarm-adapter-*` or `modules/*-rust` implementation |
| Process/worktree/Git collection | existing host lifecycle/process owners |
| Enabled helper scheduling | one `swarm-automation` service with Store-owned due state |
| CLI and MCP | `swarm-cli` and `swarm-mcp`, both calling Store handlers |

No adapter accepts a Task, changes ownership or interprets review. No common shell
owns a vendor session, native retry policy, scheduler or transcript store.

## 3. First executable route: Antigravity Rust

Antigravity is first because it has one serialized native input window and a proven
lost-delivery defect. Do not define the common shell around Command, Codex or Muse:
their profile ladders and native state machines would move legacy complexity into the
abstraction.

### A0 — synchronize the active coordinate

Source and the shipped descriptor use artifact version `6`; active route/tooling
selectors still contain `5`.

Update together:

- `modules/antigravity-rust/route.example.toml`;
- `tools/ci/module-package-policy.json`;
- `tools/qualification/New-NativeQualification.ps1::Get-AdapterContract`;
- the narrow Antigravity validator in
  `tools/modules/Install-ModuleArtifact.ps1`;
- active catalog/qualification fixtures that select new work.

Do not rewrite historical v5 receipts under `docs/remediation`. New installation,
selection and qualification use v6. After concrete unsettled v5 bindings receive a
finite disposition, remove active `5 | 6` behavior branches.

Completion: source constant, descriptor, package policy, installer and qualification
select one active coordinate without fallback.

### A1 — remove the observation/proof dependency from terminal outcomes

Current terminal delivery is not directly durable. `Controller::record_outcome`
creates a receipt whose `local_execution_ref.observation_id` is empty. Before
`module.outcome`, `flush_reports` must publish an observation, receive a Store row ID,
rewrite the pending outcome and only then send it. A crash at any point loses the
in-memory receipt.

The Store warm-stream validator then compares the outcome with the same result copied
into the observation. The dispatch path separately requires
`AntigravityDispatchSessionProof`. These are two copies of adapter state acting as a
second authorization protocol.

Replace both with one self-contained terminal outcome. Store already knows the
authenticated link, Operation, binding generation and current adapter boot. The
outcome needs only:

- Operation ID;
- adapter boot and native conversation identity;
- sequential result ordinal for this serialized session;
- native terminal status;
- response-content hash and bounded result reference;
- disposition and bounded diagnostics.

Store validates this directly against the retained Operation and binding. It does not
require an observation row to authorize or identify the outcome. Monitoring
observations remain independent latest-value state.

Delete in the same connected change:

- `local_execution_ref.observation_id` and the pre-outcome observation round trip;
- the duplicate `local_execution_results` array used only to re-prove the receipt;
- current-observation matching from `runtime/warm_stream.rs`;
- `AntigravityDispatchSessionProof` and its constants;
- proof generation in `modules/antigravity-rust/src/controller.rs`;
- `launcher_dispatch/antigravity_capability.rs` proof lookup, validation and digest
  packet;
- refresh-before-dispatch proof qualification and fixtures;
- hashes whose only purpose was binding those proof packets.

Keep assignment, binding/generation, boot ownership, workspace/revision checks and
the actual response-content hash. `agent.refresh` remains safe readback; it no longer
mints permission to dispatch.

Completion: first dispatch needs a ready current binding, not a fresh refresh.
Terminal outcome is complete before any Store call. Diagnostics remain in
`agent.state`; no observation pointer or proof DTO is required.

### A2 — make Store delivery durable with the smallest possible spool

After A1, outcome and result envelopes are immutable before delivery. Implement a
route-local spool with existing `swarm-process::permissions`; do not add a database,
append journal, tombstone graph or retention index.

Scope:

```text
binding_id / generation / adapter_boot_id
```

Use two storage forms.

**Exact queue for outcomes and result pages**

One private file per delivery:

```text
version
sequence
method = module.outcome | module.result
lane = Operation ID
exact params
```

The adapter is the only writer. At startup it scans a small bounded directory,
orders by sequence and derives the next sequence. Gaps are harmless. Persist with
`write_private_new` before attempting IPC. Replay the same retained params after
Eliot-link reconnect. Delete with `remove_private_durable` only after an exact
recorded/replayed/superseded Store acknowledgment accepted by that method.

**Latest checkpoint for observations**

Observations are snapshots, not an event mirror. Retain one replaceable pending
checkpoint per source, containing its source epoch, sequence, coverage/gap and latest
state. A newer unsent snapshot may replace an older one. Store journals each accepted
snapshot; skipped intermediate native events are represented by source coverage, not
by thousands of outbox files.

Rules:

1. Never store native prompt, process launch or session mutation in this spool.
2. No payload SHA is needed: the private file is the retained exact value and Store
   validates its Operation/source identity. Result-content hashes remain.
3. Bound exact-item count, bytes and observation-source count. Stop pulling commands
   before capacity is exhausted and reserve one terminal slot for admitted work.
4. A permanent Store rejection blocks only its lane and remains visible. A corrupt
   file is retained as a diagnostic gap; it is never deleted or interpreted.
5. A replacement boot never silently adopts or deletes predecessor files. Recovery
   may inspect them and create a new current-boot disposition; common spool code does
   not decide that.
6. No global order exists between unrelated Operations. Preserve sequence within one
   Operation/result-page lane and one observation source.

Edit:

- `modules/antigravity-rust/src/controller.rs`;
- `modules/antigravity-rust/src/main.rs`;
- one small route-local spool module;
- focused Antigravity tests;
- only needed exports from `swarm-process`.

Delete `pending_receipts` as the delivery authority. Do not copy the OpenCode,
Claude or Command journals: they combine native intent, recovery, retention,
tombstones, references and Store delivery.

Completion cases:

- one native prompt write;
- disconnect after terminal observation leaves one durable outcome;
- same boot reconnect sends retained params and ACK removes the file;
- restart never converts the file into another native execution;
- predecessor-boot records remain visible but unclaimed;
- lost result-page ACK converges;
- observation churn stays bounded and reports a gap when coalesced.

### A3 — give the route only its real actions

The first Antigravity route supports:

```text
Open
Dispatch
Send(next_turn)
Refresh
Result
Reconcile
```

Process release remains the supervisor's lifecycle responsibility until a real public
`Shutdown` consumer exists. Do not advertise or implement placeholder `Reply`,
`Configure`, `Goal`, `Background`, `Recover` or live-steer branches. Unsupported
actions fail in Store routing before `module.next`.

Replace string dispatch with typed semantic commands for these actions. Collapse
`OperationIdentity` and per-outcome `ModuleReceiptIdentity` to facts Store cannot
derive:

- Operation ID;
- binding generation and adapter boot;
- native conversation/result identity when observed;
- disposition and bounded action details.

Remove artifact version, protocol, request hash and duplicated Task/source hashes
from each outcome. Keep hashes on immutable result/artifact bytes.

Completion: an optional action changes one capability value and adapter match, not an
artifact profile, schema family or Store vendor branch.

## 4. Extract shared code only after two real consumers

### Bootstrap threshold

Antigravity and Claude's contained
`crates/swarm-adapter-claude/src/module_runtime.rs::OwnedBootstrap` are the first
candidates for one reader in `swarm-process::module_owner`.

It may return only:

- verified owner/worker membership;
- binding ID and generation;
- module client/credential identity;
- private state directory;
- selected protocol major and required semantic actions;
- typed host connection configuration.

It does not parse vendor options, launch native processes or compare
artifact/schema matrices. Convert both adapters and delete both duplicate
env/path/protocol/claim parsers in the same change.

### Eliot-link threshold

`swarm-client::ModuleLink` already owns authenticated hello, `module.next`, outcome,
observation and result calls. Replace Antigravity's private `ManagerLink` after A2 and
the reduced hello contract.

A common loop may later own connection, one hello per physical link, reconnect,
typed command pull, delivery of an adapter-owned retained record and bounded link
shutdown. It may not own native session state, provider retries, result association,
Task state, timers or scheduling.

Move the spool into `swarm-client` only when a second migrated route uses the same
record and deletion semantics and both old delivery loops disappear. Reuse only the
small exact-envelope mechanism from existing journals; do not extract their native
intent, startup census, tombstones, reference graph or retention machinery.

### Why Command is not the proof route

Command currently combines Batch v3/v4/v5, ACP v1/v2, alternative capability/schema
sets, method aliases, a large journal and ACP queue/session state. Retire historical
profiles and choose the retained batch/ACP product first. Do not let that matrix
define common bootstrap or delivery.

<a id="p1--replace-the-module-compatibility-matrix-with-protocol-2"></a>

## 5. Semantic adapter protocol

The target internal vocabulary is closed and vendor-neutral:

```text
Open          open/resume/attach a selected native session
Dispatch      deliver the first immutable TaskPrompt
Send          next-turn input or exact active-turn steering
Reply         answer one observed pending request
Configure     apply supported model/mode/options
Goal          set/edit/pause/resume/clear Harness Goal
Background    move an eligible native run/tool to background
Cancel        cancel one exact turn or queued input
Recover       restore one known session after confirmed owner/control loss
Refresh       safe native readback
Result        retrieve output for one admitted input
Reconcile     read back one uncertain admitted effect
Shutdown      release/stop one explicitly owned resource
```

A route declares only supported actions and scopes. Store rights remain the
permission boundary. Vendor RPC names and payloads stay in the adapter.

Remove public/common internal methods as routes migrate:

- `native.opencode.loop_step` → `Send { steer }`;
- `native.command.cancel_turn` → `Cancel { turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → internal cleanup;
- `native.mcp.*` → OpenCode lifecycle/observation.

`Recover` restores a known session without replay. `Reconcile` reads an uncertain
effect. `Open` selects or attaches a session.

### Logical inputs and physical attempts

One Store Operation is one logical user/controller input. A harness may perform
several physical model attempts for that input. Attempt IDs, retries and current
attempt state are adapter observations. They do not create new Task Operations and do
not produce several accepted results.

Bind by the strongest native logical identity:

- OpenCode inbox/message ID;
- Codex `clientUserMessageId` plus thread/turn IDs;
- Muse command ID;
- ACP queue request plus prompt-start/terminal state;
- one serialized window when the harness exposes no parent ID.

A transport RPC ID is not durable input identity. A provider retry performed by the
harness is not permission for Eliot to resend the input.

### ACP stays adapter-local

The current Command dependency and the official ACP v2 draft have different package
and protocol generations. Do not dual-stack them in Core. When the Command route
actually migrates, select one maintained ACP generation and map it to Eliot actions.

Preserve the useful semantics:

- prompt response/admission is not terminal completion;
- updates are ID-addressed upserts;
- capabilities determine optional controls;
- unknown protocol values are tolerated at the ACP boundary;
- session cancel/close remain distinct.

Delete the old ACP profile after concrete current work is drained. Eliot's internal
protocol does not inherit ACP's v1/v2 compatibility matrix.

<a id="processes-and-frontends"></a>

## 6. One executor per retained runtime

| Runtime | Retained direction | Delete after finite disposition |
| --- | --- | --- |
| Antigravity | Rust serialized warm stream | Node bridge, proof packet and old delivery path |
| OpenCode | standalone Rust adapter using durable inbox IDs/log | built-in host executor, legacy demand and public native MCP phases |
| Codex | current Rust app-server adapter | Python executor/SDK, version templates and fixtures |
| Muse | choose Node or Rust by complete behavior and total Rust+JS+IPC | rejected executor and duplicate usage normalizer |
| Claude | one Rust adapter with thin current SDK driver | retained Node generations and duplicate bootstrap/transport |
| Command | one current batch profile plus one current ACP implementation only if both remain product requirements | historical Batch/ACP profile ladders and method aliases |

Historical completed records do not keep an executor alive. Enumerate concrete
unsettled Operations, finish/read back/export them or create a new Task
revision/Attempt, then delete producer, package, fixture, configuration and reader.

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

## 7. Native Goal and Eliot Goal

Harness Goal is vendor-native and default where supported. The adapter maps
set/edit/pause/resume/clear and associates native turns with the initiating Operation
and Task/Attempt.

Eliot Goal is a Store plugin that emits an ordinary `Send` Operation on an event or
cadence. Configuration needs message/template, trigger/cadence and delivery mode.
Replaying one occurrence returns the same Operation; a later occurrence may
intentionally repeat the text.

Remove provider branches, OpenCode pseudo-Goal, approval hashes and Goal-specific
continuation proof families. Plugin disable, native Goal pause, turn cancellation and
queued-input removal are independent.

<a id="passive-agent-observation"></a>

## 8. Passive observation, source cursors and quotas

One bounded host collection lifecycle runs independently of automation and optional
diagnostics. Adapters fold native events and provider-local JSON/JSONL/SQLite; host
adds OS process and Git facts; Store joins assignment, native session/turn/child,
process, worktree/Git/GitHub, source coverage and quota.

Configured sources run without a Task/binding. Unmatched sessions remain inventory.
`agent.list`, `agent.state`, `agent.family` and `agent.usage` read Store without
provider calls.

### Two cursor layers

Do not confuse:

1. **Provider/source cursor** — opaque and interpreted only by its adapter reader.
2. **Store observation cursor** — `observation_id` used by
   `monitor.snapshot`/`monitor.follow`.

For a source with replay and live subscription:

1. capture source epoch and subscribe before or together with replay;
2. read a bounded page after the retained source cursor;
3. apply ID-addressed facts/upserts;
4. record the source watermark/sync boundary;
5. commit normalized facts, next cursor and coverage in one Store transaction;
6. continue live updates.

For polling/file/SQLite sources, each pass is a bounded snapshot/read-after-cursor and
must recheck mutable open records. Cursor expired, foreign epoch, truncated JSONL,
WAL gap or retention loss becomes an explicit coverage gap and bounded rescan. Viewer
disconnect never changes work, owner TTL or native execution.

Store retains current normalized facts and its own journal. It does not mirror every
provider frame.

### Quota and provider availability

The current shared quota path is vendor-specific bureaucracy:
`NativeUsageSnapshot` hardcodes Codex/Muse native method names, and
`ProviderConditionFact` repeats binding, route, account, model, connection,
observation order, evidence method/revision, auth SHA and a details digest. Store
then stores/revalidates the same provenance in many columns. Artifact allowlists
reject a valid new Muse producer.

Replace it with one adapter-normalized capacity snapshot per actual source/account
scope:

```text
source_key and source_epoch
monotonic source sequence
observed_at and freshness/coverage
scope: account/service/connection, using only identities the provider actually gives
availability: available | rate_limited | quota_exhausted | auth_required |
              model_gone | data_policy_required | overloaded | unknown
optional retry/reset
bounded native-labelled buckets/windows/credits
```

The adapter owns native method names and patch folding. A partial rolling update does
not erase metadata it omitted. Store validates the authenticated source, bounds and
monotonic sequence, then retains one current snapshot. Bindings/routes reference that
source; they do not each copy an account snapshot.

Delete:

- `UsageService` as a Codex/Muse allowlist;
- native method allowlists from shared DTO validation;
- `auth_context_ref` SHA as proof;
- Codex-only `ordinary_usage_allowed` from the universal shape;
- artifact-version source allowlists;
- duplicate `ProviderConditionFact` provenance and `details_digest`;
- binding-local copies of an account-wide snapshot.

If the provider exposes no stable account identity, keep the snapshot connection-
scoped and never merge it across routes. Missing/invalid optional quota is
`unavailable`, not zero, and cannot reject valid activity/result facts. Manual work
defaults to visible `unknown`; automation may explicitly require current capacity
evidence.

Keep assignment, activity, pending wait, connection, process liveness, source
freshness, Git state, Task completion, run usage and account capacity separate.

<a id="launch-and-results"></a>

## 9. Launch, results, review and acceptance

Launch preview is optional. Store admits explicit settings and expected revisions,
resolves one launch plan under the Operation and references it from workspace and
dispatch. Remove caller-supplied plan hashes and MCP-readiness gates from first input.

Keep one result path:

```text
Result -> adapter page -> module.result -> immutable artifact
       -> artifact.read / artifact.assemble -> submission
```

Associate output with an explicit native logical-input identity or one serialized
admitted window. A changed generation, second independent input or unclassified
source gap requires readback; never choose the newest assistant message by guess.
Exact result pages remain in the delivery spool until Store ACK.
`artifact.assemble` is the only multi-page assembler.

Manual and automatic acceptance use the same candidate-specific retained review/check
gate. Remove caller-authored duplicate proof arrays. Candidate B cannot reuse A's
review; checks-only policies remain valid.

<a id="store-and-data-model"></a>
<a id="store-and-current-data"></a>
<a id="store-and-automation-implementation-details"></a>

## 10. Store, automation and product deletion

Keep one Store writer. Move pure bounded query families to the existing WAL reader
pool; authorization, page data and journal cut use one short snapshot. Never hold a
transaction across native/file/Git/network I/O or client pagination.

Replace base-SQL text SHA equality with application identity, current schema version
and explicit bounded migrations. Move indexed identity/state/due fields out of
`meta`; retain bounded payload JSON in its owning row. Remove old key codecs/readers
after consumers switch. No ORM, EAV store or second migration runner.

Give automation an immutable `entry_id` independent of owner/name. Update owner and
revision in one transaction; journals/occurrences retain the ID. Delete successor
copies, relocation paths, transfer chains/cycle logic and parallel schedulers. One
`swarm-automation` service evaluates due work; Store commits occurrence identity and
action disposition.

Remove from compact-core completion:

- Concilium/council state machine;
- generic integration negotiation/ratification/ACK engine;
- automatic publication/Issue-label projection;
- document-edition rights and `LegacyV1` acceptance branches;
- retired installers, checkout/compiler/cache provenance equality and duplicate
  package validators;
- host dependencies on worker implementations after required contracts move.

Retain explicit accepted-candidate publication, ordinary messages, independent
review, participant grants and manual integrations.

### Tooling reduction

Packaging retains one manifest, selected executable/resources, one byte check at
install/launch, path containment, private credentials and atomic publication.
Runtime hello verifies protocol major and required semantic actions.

Remove:

- equality of checkout, compiler, dependency graph and build-cache path;
- artifact-specific PowerShell descriptor validators;
- repeated hashing of the same local files across qualification stages;
- retired frontend dependency inventories;
- qualification gates for proof packets that no longer exist;
- incident-specific workflow modes after the permanent behavior is tested normally.

Qualification calls public APIs and checks observable route behavior. It does not
parse private Rust functions with regex or require host and adapter to share a build
directory.

<a id="current-format-and-existing-data"></a>

## 11. Finite transition

Backward compatibility is not a product feature.

For one migrated route:

1. stop admitting new work through the old producer;
2. enumerate concrete unsettled Operations and required retained results;
3. finish/read back/export them or create a new Task revision/Attempt;
4. remove the old producer, reader, descriptor, fixture, configuration and package;
5. admit new work only through the current route.

Do not build generic converters for hypothetical history. Completed records may
remain opaque history when Store already retains the facts needed by current reads.
They do not keep an executable, schema branch or adapter alive.

<a id="verification"></a>
<a id="5-verification"></a>

## 12. Verification

During construction run formatting and minimal affected warnings-denied
Clippy/build or syntax checks. Do not create a broad matrix around an incomplete
route. When the connected path is complete, verify:

| Boundary | Required proof |
| --- | --- |
| Antigravity receipt | Terminal outcome is self-contained; no refresh/observation pointer/proof packet authorizes it. |
| Durable delivery | Exact outcome/result survives disconnect; ACK removes it; observation checkpoint coalesces with explicit gap; predecessor boot is not adopted; native input is never replayed. |
| Typed port | Unsupported action never reaches adapter; optional capability does not create an artifact/profile branch. |
| Logical input | Several native physical attempts still settle one logical Operation/result. |
| ACP | Admission and terminal are separate; selected ACP generation is adapter-local; old profile is deleted after drain. |
| Observation | Source replay/live handoff has no silent gap; expired/foreign cursor is visible; Store monitor cursor remains separate. |
| Quota | New adapter source is accepted without artifact allowlist; omitted patch metadata is retained; account snapshot is not duplicated per session; unknown is not available/zero. |
| Goals | Native and Eliot Goal work alone or together; control of one does not mutate the other. |
| Results/review | Correct logical input owns result; stream gap stays unresolved; candidate B cannot reuse A's review. |
| Store/automation | WAL reads are bounded/authorized; restart retains admitted work; transfer keeps one entry; old schedulers/keys are gone. |
| Deletion | Removed path has no config, package, CI, docs, fixture or fallback wrapper. Report net owned Rust+JS+glue separately from tests. |

Every completed source slice updates [status](../implementation-status.md) with
behavior, evidence and net owned-code change.
