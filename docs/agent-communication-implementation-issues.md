# ELIOT Agent Communication — Implementation Issue Plan

**Revision:** 1 — 2026-10-02  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Normative sources:** [Implementation Checklist](agent-communication-implementation-checklist.md), [Agent Communication and Concilium](agent-communication-concilium.md), [Tool Contracts](agent-communication-tool-contracts.md)  
**Status:** implementation-ready Issue templates. This document does not authorize code changes outside the named Issue/scope.

## 0. Execution policy — applies to every Issue below

This section follows [Owner Decisions](owner-decisions.md) and has precedence over test lists embedded later in this program.

### Manager and workspace

- One Issue revision is one product delivery unit.
- One manager owns one worktree, branch and candidate.
- The manager reads the canonical Issue/comments/documents and gives writers exact non-overlapping assignments.
- Writers edit code or perform one assigned review in the manager-owned worktree context. They do not create competing branches/worktrees, choose process policy or publish/accept work.
- The candidate is frozen while it is being reviewed.
- A returned writer diff is not an independent submission.

### Build/check cadence

- **Code first.** Implement the complete production path named by the Issue before broad verification.
- Writers do **not** run Cargo.
- The manager reviews every returned diff and integrates it.
- On the final candidate for one Issue, the manager runs only:

```text
scoped rustfmt for touched Rust files/crate
cargo clippy --locked -p eliot-swarm-controller --lib --bins --no-deps -- -D warnings
```

- Do not run `cargo test`, workspace/all-target builds, load tests, live model calls or owner-machine qualification during I1–I8 merely because this document lists future acceptance cases.
- I9 is the explicit integrated test/qualification phase after I1–I8 code is complete and wired.
- An explicit later owner/acceptance instruction may advance a named test earlier; otherwise test commands remain planned evidence, not writer work.

### Documentation and evidence

- The Issue body plus named canonical documents are the specification.
- Do not infer implementation from field reports; use them only for negative cases.
- Every completed Issue records exact files/symbols changed, current limitations and the deferred I9 scenarios it enables.
- Do not close/reopen Issues or change labels as a side effect of implementation.

### Universal non-goals

No Issue below may add:

- another database, event store or message broker;
- a generic shell or JSON-RPC passthrough;
- a general chat room or broadcast;
- peer task assignment;
- automatic model wake;
- automatic consensus-to-acceptance;
- a Store-owned generic LLM client;
- a real domain, credential, local private path or external-service configuration;
- broad refactoring unrelated to the named slice.

## 1. Dependency graph

```text
I1 mailbox extraction
  -> I2 coordination schemas + reads
      -> I3 bilateral threads
          -> I4 contract proposals
          -> I5 advisory code scopes
              -> I6 bounded Git inspection
          -> I7 durable Concilium
              -> I8 CLI/MCP/profile/document wiring
                  -> I9 integrated verification/live qualification
```

I4 and I5 may proceed in parallel after I3 only when their file ownership does not overlap. I7 depends on I4. I6 depends on I5. I8 starts only after the public application methods it exposes exist.

---

# I1 — Extract the existing mailbox delivery primitive without behavior change

## Purpose

Create one reusable internal delivery boundary before adding typed coordination. This is a behavior-preserving refactor.

## Depends on

None.

## Owning files

```text
src/store/mailbox.rs                  new
src/store/mod.rs                      extraction/dispatch only
src/store/mailbox_tests.rs            existing behavior preserved
src/lib.rs                            only if module exposure requires it
```

No other files without a demonstrated compile requirement.

## Read first

- `src/model.rs`: `message_payload_digest`, `message_actor`, `message_scope`, `message_reply_reference`, deadline validation.
- `src/store/mod.rs`: `message.send`, `find_delivery`, `cancel_message`, generic mutation/Observation path.
- `src/store/mailbox_tests.rs`.
- Implementation Checklist §§2, 5.

## Work

1. Move legacy mailbox logic from `src/store/mod.rs` into `src/store/mailbox.rs`:
   - recipient registration lookup;
   - sender actor/scope projection;
   - legacy payload digest;
   - exact reply lookup and reverse-party validation;
   - delivery identity/result assembly;
   - delivery lookup;
   - cancellation.
2. Introduce one internal `DeliveryRequest`/`admit_delivery` helper that records delivery identity/scopes/deadlines but does not dictate typed payload content.
3. Keep `message.send` request, result shape, digest basis, legacy reply behavior and error codes unchanged.
4. Keep `message.cancel` behavior unchanged.
5. Keep the original immutable record unchanged by cancellation.
6. Do not add a coordination method yet.

## Code-complete acceptance

- Public API/result JSON for current mailbox calls is unchanged.
- Existing legacy digest basis is unchanged.
- Old delivery lookup by historical Operation ID still works where currently supported.
- Unknown recipient still fails closed; no placeholder client is created.
- No Task/Attempt/native state changes from a mailbox message.
- No new table, dependency, process or async worker.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only, per §0.

## Deferred I9 verification

- Existing mailbox test set.
- Same request replay and changed-payload conflict.
- Reply/cancel digest mismatch.
- Legacy records without a modern delivery digest.

## Non-goals

No typed thread, participant set, new Observation kind, MCP change or Git tool.

---

# I2 — Add coordination schemas and bounded read projections

## Purpose

Define strict pure types and read models before any new mutation is admitted.

## Depends on

I1.

## Owning files

```text
src/coordination.rs                    new pure schemas/limits/digests
src/store/coordination.rs              new read/current-projection helpers
src/store/coordination_tests.rs        fixture declarations only in code; execution deferred
src/store/mod.rs                       read classification/dispatch
src/model.rs                           minimal shared helpers only
src/lib.rs
src/store/projection.rs                reuse bounded projection frame
src/doctor.rs                          status/gap declaration
```

## Read first

- Implementation Checklist §§2–4, 7–10.
- `src/model.rs` actor/scope/roles/deadlines.
- `src/store/tasks.rs` Task/Attempt owner and revision semantics.
- `src/store/producers.rs` ProducerRef assignment identity.
- `src/store/projection.rs` byte/item limiting and gap frames.
- `migrations/001_core.sql` `meta`, `operations`, `observations`.

## Work

1. Add strict deny-unknown-field types for:
   - speech act;
   - topic kind;
   - reasonability declaration/result;
   - participant actor/scope;
   - participation basis;
   - thread header/state;
   - message header/body reference;
   - contract/scope/Concilium read summaries.
2. Preserve existing UUID generation; do not validate semantic prefixes.
3. Support `attempt_owner` participation basis first.
4. Parse/validate `producer_ref` only against an existing exact ProducerRef; do not create assignments.
5. Add bounded read methods:

```text
coordination.inbox
coordination.thread.get
coordination.thread.list
coordination.contract.get
coordination.contract.list
code.scope.inspect
code.scope.conflicts
concilium.get
concilium.list
concilium.preview
```

6. Reads use revisioned namespaced `meta` current projections and immutable Observation history.
7. Every list/page uses existing projection limits and explicit gaps.
8. Visibility requires current application authority and exact participant/work context. Observer role alone is insufficient.
9. Doctor reports `designed/implemented/fixture_checked/live_qualified` separately and must not imply mutations exist.

## Code-complete acceptance

- Unknown JSON fields/enums rejected.
- Ordinary manager actor has no invented generation.
- Actor and scope remain separate.
- No communication read creates an Operation, message or model work.
- Reads cannot expose another Task's unauthorized artifacts/thread.
- Partial/oversized data reports a gap instead of empty success.
- No startup full-log scan is required for exact current lookup.
- No table/schema migration.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only.

## Deferred I9 verification

- Strict serde/field/property tests.
- Pagination and byte-budget gaps.
- Unauthorized observer/participant access.
- Projection consistency after restart.

## Non-goals

No thread/message mutation, contract ratification, Git process or Concilium execution.

---

# I3 — Implement durable bilateral coordination threads

## Purpose

Ship the smallest useful feature: one addressed, typed, durable technical exchange that never starts work automatically.

## Depends on

I2.

## Owning files

```text
src/coordination.rs
src/store/mailbox.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/store/mod.rs
src/model.rs
src/store/projection.rs
src/doctor.rs
```

## Public mutations

```text
coordination.thread.open
coordination.message.send
coordination.thread.resolve
coordination.thread.withdraw
coordination.thread.supersede
```

Existing `message.cancel` must recognize an exact coordination delivery.

## Work

1. `thread.open` pins exact Task ID/revision, Attempt, sponsor owner, immutable participants/bases, subject/topic, reasonability and close condition.
2. Run deterministic reasonability classification without an LLM.
3. One recipient per `message.send`; reuse I1 delivery helper.
4. Allocate monotonic `message_seq` atomically without CAS on unrelated messages.
5. Use structural `expected_state_revision` only for close/withdraw/supersede.
6. Version the coordination payload digest separately from legacy mail.
7. Replies address exact prior delivery/message and reverse parties.
8. Incoming mail creates only durable mail/attention; no runtime command/model call.
9. Participant set is immutable. Changed membership creates a manager-sponsored successor thread.
10. Deadlines/age create attention only, never expiry/agreement/release.
11. Update namespaced `meta` projection and one automatic Observation in the same transaction.
12. Link Operation to exact Task/Attempt, not native binding.
13. After Attempt release/supersession, ordinary peer sends are rejected; manager/operator may close/supersede history.

## Code-complete acceptance

- Same request/payload returns same thread/message.
- Changed payload under same request ID conflicts.
- Peer prose shaped like `task.revise` is stored only as text.
- Unknown/stale actor rejected; no placeholder.
- No recipient array, broadcast or participant mutation.
- Concurrent valid sends receive distinct ordered sequence values.
- Reply to wrong thread/party/digest rejected.
- Existing cancellation does not erase the original.
- Five no-progress messages create at most one loop-attention fact and zero model calls.
- Closing thread creates no follow-up Task/Operation automatically.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only.

## Deferred I9 verification

- Lost response/retry and host restart.
- Concurrent sends.
- Slow reader/lag-resync.
- 1,000 inbound messages cause zero model calls.
- Old participant generation/binding cases.

## Non-goals

No contracts, scopes, Git subprocess, Concilium or native safe-boundary delivery.

---

# I4 — Implement immutable contract proposals and manager ratification

## Purpose

Make producer/consumer interface negotiation inspectable and separate peer support from authoritative selection.

## Depends on

I3.

## Owning files

```text
src/coordination.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/store/mod.rs
src/store/projection.rs
src/doctor.rs
```

## Public methods

```text
coordination.contract.propose
coordination.contract.respond
coordination.contract.get
coordination.contract.list
coordination.contract.ratify
coordination.contract.reject
```

## Work

1. Immutable proposal/revision with canonical digest.
2. Exact affected paths/symbols/schemas and producer/consumer/identity/payload/observation/failure/versioning fields.
3. `respond` enum: counterproposal, object, support, withdraw.
4. Objection names requirement, counterexample, evidence gap, identity/replay ambiguity or versioning incompatibility.
5. Support remains advisory.
6. Ratification is manager/operator/current-GM authority under current Task/Attempt.
7. Ratification CAS checks proposal digest, Task revision, Attempt ownership and relevant accepted scope revisions.
8. Record dissent/caveats; do not erase rejected/superseded history.
9. Keep `ratified`, `implemented`, `verified` and `accepted` separate.
10. No code generation/model execution on ratification.

## Code-complete acceptance

- Proposal revision immutable.
- Counterproposal does not overwrite original.
- Changed Task/proposal/scope makes ratification stale.
- All peers supporting does not ratify automatically.
- Former GM/old Attempt owner cannot ratify.
- An unratified implementation can only be recorded as an assumption.
- Ratification changes no Task acceptance or native work.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only.

## Deferred I9 verification

- Conflicting concurrent ratifications.
- Restart/replay.
- Minority objection persistence.
- Stale scope and Task revisions.

## Non-goals

No automatic API/schema generation, code editing, Task revision or acceptance.

---

# I5 — Implement advisory code-scope intents

## Purpose

Show who is expected to change a code/contract scope without creating hard locks, deadlocks or TTL-based ownership claims.

## Depends on

I3. May proceed in parallel with I4 only under non-overlapping file ownership.

## Owning files

```text
src/coordination.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/store/mod.rs
src/store/projection.rs
src/doctor.rs
```

## Public methods

```text
code.scope.propose
code.scope.accept
code.scope.inspect
code.scope.conflicts
code.scope.release
```

## Work

1. Participant proposes exact paths/symbols/interfaces/mode/reason/baseline.
2. Current Attempt owner/operator accepts or revises exact proposal digest.
3. Implement exact literal path/prefix, symbol and interface overlap first.
4. Modes:

```text
exclusive_edit
shared_edit
read_review
```

5. `exclusive_edit` overlap is conflict; `shared_edit` requires coordination; `read_review` informational.
6. Scope is advisory and never waits for another actor.
7. Override records manager identity, reason and revision.
8. TTL marks stale/needs-review only.
9. Release is an exact Operation and returns verified previous/current state.
10. Attempt release/supersession marks scope stale/needs-review; it does not delete history.
11. No Git subprocess yet.

## Code-complete acceptance

- Same accepted scope cannot be silently rebound to another Attempt/actor.
- Broad scope produces sponsorship warning.
- Shared/read overlap classification correct.
- Override visible to both owners.
- Expired/stale scope never proves work is gone or safe to delete.
- Failed/unchanged release is not reported as Applied.
- Incomplete evaluator reports unknown, not no conflict.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only.

## Deferred I9 verification

- Conflict matrices.
- Restart/current projection.
- Concurrent accept/release.
- Stale Attempt and manager authority.

## Non-goals

No filesystem lock, worktree deletion, Git hook, automatic cleanup or generic glob engine.

---

# I6 — Add bounded read-only Git inspection

## Purpose

Answer “who is working here?” by combining ELIOT ownership, current worktree changes and historical provenance without letting Git become assignment authority.

## Depends on

I5.

## Owning files

```text
src/git_inspect.rs                    new
src/git_inspect_tests.rs              planned verification
src/checks/source.rs                  extract/reuse command builder only
src/config.rs                         minimal trusted local Git settings if required
src/store/coordination.rs             compose exact projection
src/store/mod.rs
src/main.rs                           CLI later only if Issue scope includes it
src/doctor.rs
```

## Public reads

```text
git.worktree.inspect
git.changed_paths
git.overlap
git.history
git.blame.summary
git.who_works_here
```

## Work

1. Resolve repository root only from trusted local project/candidate/source context.
2. Accept only full recorded object IDs, never ref expressions/ranges/arbitrary strings.
3. Validate repository-relative literal paths; no Git pathspec magic from caller.
4. Reuse/extract the source-capture Git process boundary:
   - no optional locks/replacements/fsmonitor;
   - cleared Git redirection env;
   - stdin null;
   - hidden Windows process.
5. Add terminal prompt/pager/external-diff disabling.
6. Implement bounded output/stderr/deadline/process disposition. Do not reuse the unbounded `output()` helper for potentially large output.
7. Parse only documented machine formats (`-z`, porcelain v2, line porcelain).
8. Include untracked files.
9. Redact local absolute paths and author emails from remote projections by default.
10. Compose `git.who_works_here` from three labelled sources:
    - ELIOT active scope/Attempt ownership;
    - current worktree/status;
    - optional bounded history/blame.
11. Any incomplete Git source yields explicit coverage/gaps.

## Code-complete acceptance

- No Git write/network/credential/hook/config mutation subcommand.
- Option/ref/path injection rejected.
- Git failure cannot erase known ELIOT ownership.
- Timeout/output cap produces partial/unknown, never no-conflict.
- `git blame` never becomes active owner.
- Private absolute paths/emails absent from remote result.
- No Git call in SQLite transaction.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only.

## Deferred I9 verification

- Spaces/Unicode/rename/untracked paths.
- Invalid UTF-8 and oversized output.
- Windows hidden process/timeout descendants.
- Moved/deleted worktree.
- Concurrent repository changes.

## Non-goals

No stage/commit/checkout/reset/clean/fetch/push or global pre-commit hook.

---

# I7 — Implement durable manager-sponsored Concilium state

## Purpose

Provide bounded structured dissent and recommendation after a material bilateral negotiation fails, without automatically running models.

## Depends on

I4. I5/I6 are optional evidence providers but not required to store Concilium state.

## Owning files

```text
src/coordination.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/store/mod.rs
src/store/projection.rs
src/doctor.rs
```

## Public methods

```text
concilium.propose
concilium.preview
concilium.open
concilium.position.submit
concilium.round.advance
concilium.get
concilium.list
concilium.close
```

## Work

1. Participant proposal creates manager attention only.
2. `preview` is deterministic/read-only and resolves exact current participants, bases, correlation metadata, evidence/proposals, packet sizes, rounds, warnings and plan digest.
3. Manager/operator/current GM opens only with matching preview digest and reasonability confirmation.
4. `open` creates immutable participant slots and starts no model.
5. Round 1 accepts independent blind positions.
6. Position schema uses named claims, stance, fact, evidence refs, counterexample, falsifier, assumptions and confidence; no hidden chain-of-thought request/storage.
7. One malformed/timeout slot does not invalidate others.
8. Manager explicitly advances to one bounded cross-review round.
9. Optional merge round requires another manager operation and changed proposal digest.
10. Preserve correlation metadata and minority reports.
11. `close` records recommended/minority/insufficient/irreconcilable/cancelled/failed advisory outcome.
12. No result ratifies a contract, modifies Task or invokes follow-up work.

## Code-complete acceptance

- Ordinary participant cannot open/advance/close.
- Changed plan input makes open stale.
- Participant submits only its exact slot.
- Duplicate identical retry stable; changed response conflict.
- No next-speaker LLM.
- No nested Concilium.
- Max rounds requires manager action; no implicit continuation.
- Consensus grants no authority.
- Timeout preserves partial valid positions.
- Open/advance causes zero `agent.send` calls.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only.

## Deferred I9 verification

- Restart between rounds.
- Parallel slots and malformed response.
- Correlated participants/minority display.
- Budget/round limits.
- Real justified Concilium after optional native execution exists.

## Non-goals

No automatic participant selection, majority voting, generic model API, workflow store or acceptance.

---

# I8 — Wire CLI, MCP profiles, subscriptions and current documentation

## Purpose

Expose completed application methods without bypassing application authority or flooding model context.

## Depends on

I3–I7 methods must exist before they are exposed. I6 Git reads are included only when implemented.

## Owning files

```text
src/main.rs
src/mcp.rs
src/mcp/subscriptions.rs
src/mcp/coordination_tests.rs
src/store/mod.rs
src/doctor.rs
README.md
docs/agent_swarm.md
docs/agent_swarm.module-contract-v2.md
docs/agent_swarm.implementation-v6.md
docs/owner-decisions.md
```

## Work

1. Register every read in Store read classification and every mutation in strict validation/apply dispatch.
2. Add explicit CLI commands; keep `swarm call` compatibility.
3. Add one typed MCP tool per exposed method; no generic passthrough.
4. Update MCP tool-table completeness count/test.
5. Enforce named profile twice:
   - tool absent from discovery;
   - hidden tool rejected before IPC dispatch.
6. Observer/reviewer/manager/GM profiles cannot elevate the application role.
7. Add bounded MCP resources for inbox/thread/contracts/scopes/Concilium.
8. Subscriptions carry only IDs/cursors/freshness facts; full bodies require read.
9. Lagged subscriber resyncs authoritatively.
10. Update public method lists and status matrices to implemented/fixture/live states honestly.
11. Keep no remote listener/domain/config in this Issue.

## Code-complete acceptance

- Hidden write cannot be discovered or manually invoked under observer.
- Read-only annotation matches actual application method.
- Caller-owned request ID is required for safely retryable mutation.
- Subscription disconnect does not stop host/native work.
- Full unread thread is not injected automatically.
- No credential/private path/domain in tool schemas/results.
- Existing local full-profile behavior preserved when explicitly selected.

## Manager gate

Scoped rustfmt and minimal warnings-denied Clippy only.

## Deferred I9 verification

- MCP catalog/profile tests.
- Lag/resync/reconnect.
- Tasks extension remains separate authority.
- CLI parity.

## Non-goals

No Streamable HTTP/Cloudflare/OpenAI/Meta/Google deployment and no native model dispatch.

---

# I9 — Integrated verification and qualification phase

## Purpose

Run the tests and live scenarios deferred by I1–I8 after the complete production path is wired.

## Depends on

I1–I8 code complete and integrated on one exact candidate.

## Owning files

Tests/fixtures/qualification documents named by the verification plan. Do not alter production behavior merely to make a test green without reconciling the contract.

## Work

### Directed automated verification

Run exact tests for:

- mutation idempotency and request conflicts;
- mailbox backward compatibility;
- strict schemas/limits;
- thread concurrency and structural CAS;
- reply/cancel binding;
- stale/released Attempt and actor identity;
- contract ratification guards;
- scope overlap/release/readback;
- Git path/ref/process/output failures;
- Concilium slot/round/recovery/authority;
- MCP profile discovery/pre-dispatch and lag/resync;
- zero model calls from ordinary incoming coordination.

### Load contour

```text
10,000 registered clients
1,000 active threads
10,000 small messages/hour
200 concurrent inbox readers
1,000 active scope intents
bounded Git worker pool
10 simultaneous Concilium state machines
```

Record p50/p95/p99, DB time, RSS, handles, Observation growth, lag, Git process count and native-control latency. Contour numbers are not success claims until measured.

### Live product scenarios

1. One real producer and consumer negotiate a Rust contract.
2. Manager ratifies exact proposal.
3. Both sides integrate without conflicting rewrite.
4. One scope overlap is detected before integration.
5. One justified manager-sponsored Concilium preserves a minority objection.
6. Compare model context/tokens with a free-form shared-chat baseline.
7. Verify no Task/Acceptance change before manager operations.
8. Verify Windows Git subprocess containment.

### Optional native participant execution

Only as a separate, already implemented manager-controlled path:

- exact binding/session/slot;
- normal runtime Operation;
- unknown outcome reconciliation;
- retained result provenance;
- no automatic fallback/model substitution;
- no automatic follow-up round.

## Acceptance

- Exact commit/platform/tool versions and commands recorded.
- Failed/partial cases remain visible and do not become qualification.
- Test evidence is attached to the exact candidate.
- No paid/live model call in CI.
- Any product defect becomes a narrow Issue; no broad rewrite in this phase.

## Non-goals

No new feature scope, distributed broker, semantic search or remote gateway deployment.

---

# 10. Handoff checklist for each implementation manager

Before coding:

```text
[ ] Read Issue body/comments and the exact four communication documents.
[ ] Read Owner Decisions §1.
[ ] Verify dependency Issues are merged on current main.
[ ] Inspect exact current symbols/files; do not trust stale line numbers.
[ ] State one complete production chain and non-goals.
[ ] Divide only non-overlapping internal writer assignments.
```

Before submitting:

```text
[ ] Integrate and review every writer diff.
[ ] Confirm no peer message creates Task/native/model work.
[ ] Confirm no new authority/store/broker was introduced.
[ ] Confirm exact retry/identity/CAS semantics.
[ ] Confirm privacy: no domain, credential or private path.
[ ] Run scoped rustfmt + minimal warnings-denied Clippy once.
[ ] Freeze candidate during review.
[ ] Record deferred I9 verification cases; do not claim them passed.
```

The manager must not ask writers to run Cargo or convert the detailed test matrix into repeated per-writer verification. The program is intentionally code-first; integrated testing belongs to I9.