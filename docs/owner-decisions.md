# ELIOT Owner Decisions
## Accepted workflow policy and resolutions for the 2026-10-02 open contract questions

**Edition:** 1 — 2026-10-02  
**Repository baseline:** `0ff7129d11a15a71a94e1ed2c265f7a3eeb0e1db`  
**Status:** accepted owner policy and implementation decisions. A decision does not claim that its product code or live qualification already exists.

## 0. Authority

This document resolves the explicit owner-decision issues opened by the Documentation Program. It is subordinate to the product architecture and module contract on runtime invariants, but it is the single accepted edition for workflow-policy choices that those contracts intentionally left to the owner.

Older field briefs and incident logs remain evidence. They do not override this edition by date, file mtime, branch name or generated TASK text.

## 1. Owner policy v1 — resolves issue #14

### 1.1. Work unit and sources

- One GitHub Issue revision is one product delivery unit.
- The canonical specification is the Issue body, its source comments selected into the Task revision, and the canonical documents named there.
- Generated TASK/brief text is a projection. It cannot add process rules, hide an unknown source comment or become a second specification.
- A manager may divide an Issue into internal non-overlapping writer assignments. Those assignments are not independent product submissions.

### 1.2. Manager, writers and workspace

- One manager owns one worktree at a time. Extra writer worktrees are not part of the accepted policy.
- The manager reads the canonical documentation, prepares exact assignments, controls subagents, integrates their outputs, owns the branch/candidate and reviews every returned diff.
- Writers write code or perform one assigned review. They do not choose workflow policy, publish, accept work, mutate controller state outside the assignment, or run broad Cargo workflows.
- Writers do not run Cargo. The manager runs scoped formatting and the minimal warnings-denied Clippy gate once on the final candidate for the Issue. Broad tests and live qualification run after the product/code slice is complete or when an explicit acceptance phase requires them.
- While a candidate is frozen for verification/review, the manager does not change that worktree. Documentation and planning for the next Issue may be prepared outside the frozen source.

### 1.3. Submission and review identity

- Branch, lane, hostname, session and mutable marker are not submission identity.
- Review, HOLD, acceptance, invalidation, publication and cleanup address Task revision + Attempt + `submission_ref` + `candidate_ref`/digest.
- Late feedback about an older candidate remains historical and never mutates a newer submission on the same branch.
- Independent review and CheckRunner evidence are separate from writer self-report.
- Agents do not close/reopen Issues or change labels as a side effect of implementation. Project acceptance/publication policy performs those actions explicitly.

### 1.4. Live work

- Silence, age, parent idle, wrapper exit and tunnel disconnect are not terminal evidence.
- Automation does not kill/restart native services or manager families by heuristic. It may use only an exact supported addressed operation under the current owner policy, such as reply, steer, background, cancel or interrupt, and must preserve unknown outcomes.
- A read-only report never changes work, replies to a request, releases a resource or starts a model.

### 1.5. Publication and acceptance order

For this project the default order is:

```text
immutable candidate
  -> configured verification/review
  -> Task acceptance of the exact candidate
  -> forge publication of that exact accepted candidate
  -> publication readback/fact
  -> bookkeeping and cleanup
```

Consequences:

- acceptance may exist while publication is pending or failed;
- publication never fabricates acceptance;
- publication of bytes other than the accepted candidate is rejected;
- lost publication responses are reconciled by remote readback before any retry;
- a project may adopt another explicit policy later, but there is no implicit per-Issue override.

Issue #14 remains open only for the code slices that bind this policy edition to Attempts and generate source-indexed Task projections. The owner decisions themselves are complete.

## 2. Module update and retention — resolves issue #15

### 2.1. Module install/update

For the 0.1 line, module installation and update remain an explicit operator procedure through each module's `UPDATE.md`.

- No automatic updates.
- No controller-owned package catalog or installer.
- No agent may change global PATH, vendor credentials, native stores or services as a consequence of observation/Doctor.
- Activation uses exact artifact/provenance and rollback instructions already recorded by the module.
- AoE/OpenCodex preview → fingerprint → revalidation → mutation → readback remains the approved pattern for a future module manager, but that manager is not part of the current completion path.

### 2.2. Retention classes

The first policy deliberately avoids arbitrary numeric eviction budgets before the corresponding cache is implemented and measured.

**Authoritative — no automatic eviction in 0.1:**

- request receipts and Operations;
- Task/Attempt/submission/acceptance records;
- candidate/source and CheckRun evidence referenced by open or accepted work;
- unresolved native-effect evidence;
- active module artifact and its operator-selected rollback generation.

**Transient lifecycle data — cleanup by proof, not broad LRU:**

- own temporary artifact files after immutable publication succeeds;
- abandoned staging files only after the writer/owner is proven departed and no DB reference exists;
- controller-owned scratch after its Operation/CheckRun has terminal disposition and no retained result depends on it.

**Caches not enabled yet:**

- completed CheckRun reuse remains disabled until issue #6's versioned-input contract is implemented;
- module download/build caches remain operator-owned because installation is manual;
- native vendor stores are outside routine controller cleanup.

When a real cache is added, its own configuration must define byte budget, eviction order, pins and authorized cleaner. No generic cleaner gains authority over Downloads, all of `%TEMP%`, credentials or native stores.

Issue #15 can close as a decision issue. Future module-manager or cache implementations require their own narrow Issues.

## 3. OpenCode controller continuation — resolves issue #2

ELIOT will not implement an automatic controller re-prompt loop for OpenCode in the 0.1 line.

Reasons:

- OpenCode has no qualified native goal API;
- a hidden controller loop would make ELIOT a replacement model loop;
- goal-met evaluation and no-progress semantics are not established;
- at-least-once wake/retry can duplicate expensive model work;
- donor field evidence shows comment/status-driven ping-pong and token amplification.

The implemented contract remains:

```text
controller records goal
  -> one deterministic activation input may be admitted
  -> durable execution evidence is observed separately
  -> a human/current GM explicitly chooses any later continuation
```

Pause/clear remain exact record mutations. A terminal event may create an attention item; it does not automatically create another prompt. Issue #2 should close as `not planned` for 0.1.

## 4. Scheduled work first slice — resolves issue #7 decisions D1–D8

- **Registry:** controller configuration names schedules; agents cannot create arbitrary schedules in the first slice.
- **Period source:** fixed one-shot or interval schedules with an explicit wall-clock anchor and period. Cron expressions are outside the first slice.
- **Slot identity:** `schedule_id + floor((due-anchor)/period)`; one stable Operation/request identity per slot.
- **Principal:** a dedicated internal scheduler principal, not GM, observer or runtime module.
- **Missed ticks:** `coalesce_latest`/Skip. Restart never bursts all missed slots.
- **Relevance:** before admission, target still exists, schedule is enabled, expected Task/binding revision matches and new-work admission permits it.
- **Drain:** existing `new_work=disabled` is authoritative. Due state may be recorded, but no new scheduled Operation is admitted. On resume, at most the latest still-relevant slot is considered.
- **Reporting:** `host.status`/reports expose schedule ID, next due, last considered slot, last Operation/outcome and last failure.

No timer invokes a model directly; it admits a normal typed Operation. Issue #7 becomes an implementation issue with these decisions fixed.

## 5. GM rotation — resolves issue #8 contract questions

### 5.1. GM-only methods

The current operator/current-GM control surface includes acceptance/invalidation, client administration, host admission mode, `gm.handover` and the implemented `forge.publish_ref` operation. Task create/revise remain normal authorized application methods, not GM-only merely because the caller is remote. Module activation remains future work and requires its own contract.

### 5.2. Epoch and queued work

Forge publication is the current epoch-fenced queued operation. Its immutable intent records the GM epoch at admission, and the Store rechecks that epoch before queued work starts and again immediately before the push. A mismatch settles the never-sent Operation as `stale_gm_epoch`; a successor GM cannot adopt or resume that old publication.

`operation.cancel` may cancel a still-queued publication from its caller or the local operator while the admitted epoch is current. After an epoch change, only the local operator may cancel the settled `stale_gm_epoch` record. Once work reaches `sending` or `outcome_unknown`, local cancellation is refused and recovery is readback-only. There is no adopt operation; a desired later publication must be a new request under current authority and accepted-candidate checks.

### 5.3. Handover

- Initiated by the local operator or current GM.
- No self-claim by a prospective successor.
- If the old GM is unavailable, the local operator performs recovery/handover.
- Network or tunnel loss never changes GM automatically.

The project is independent of the GM chat. Reconnecting the same durable client
or changing its optional native binding preserves GM authority and epoch. A
designation change grants the successor explicit control of exact current
Attempts and readback of retained project work; historical owner and caller
identities remain recorded. The previous chat is not required for recovery.
See [GM session continuity](gm-session-continuity.md).

### 5.4. Mailbox and attention

Client-addressed historical mail is not reassigned. Task/Attempt/native attention is controller state and is reprojected to the successor through `report.attention`. Handover records the observation/mailbox cursors used for the transfer; the successor resyncs authoritative reads rather than inheriting an opaque live stream.

### 5.5. Wake

`checkpoint_poll` remains the safe mode until a real Dot/Muse entrypoint is live-qualified. MCP subscriptions are freshness hints, not wake/continuation authority.

Issue #8 remains open only for implementation/live qualification that follows these decisions.

## 6. Forge publication first slice — resolves issue #10 decisions

### 6.1. Transport and authority

- Use an explicitly resolved native `git` executable through the existing owned process boundary; no shell command string.
- `gh` is optional and only for a separately selected PR operation.
- Publication is allowed only for an accepted exact candidate under project policy.
- Operator or current GM may request publication; the forge rechecks current authority and acceptance at execution.

### 6.2. First operation

First slice: exact non-force ref publication with remote readback. PR creation/merge is a later operation, not part of the same transaction.

Publication intent contains at least:

```text
canonical repository identity
candidate artifact/digest and exact Git commit
remote name and target ref
expected old ref (or explicit create)
force = false
project policy revision
```

The first slice includes the origin resolver because exact repository identity is required for deduplication and readback. Prefer forge/global node identity when available; canonical host/owner/repository is the fallback, never display name alone.

After the Git runner proves its process tree empty, an unknown push response may be reconciled by reading the exact remote ref and commit. Reconciliation never repeats the push. If the runner cannot prove that its process tree ended, the Operation remains `outcome_unknown`, exact-ref readback cannot clear the hold, and later publications to that canonical repository are blocked pending manual operator intervention. Cleanup and bookkeeping, if added, are separate Operations.

The first-slice implementation is present in the current candidate. Combined gates and live native Git/remote qualification remain pending; this section records the owner policy and does not claim those gates have passed.

## 7. Zed sessionless batch wiring — resolves issue #12 questions

- Current `task.dispatch` uses the Store-produced `swarm.task_prompt` v1 envelope: exact prompt bytes and digest, immutable Task/Attempt/revision and snapshot identity, plus the matching retained dispatch context. Zed v2 passes those prompt bytes unchanged. The snapshot-based v1 codec remains only for historical readback and settlement.
- One dispatch Operation owns one Zed batch `run_id`. Producer evidence is batch-shaped, not a synthetic session/turn: exact operation, run ID, exit/result cross-check and terminal disposition.
- `agent.result` selector is `batch_output` with exact dispatch Operation ID and one allowlisted output name (`result.json`, `thread.md`, `thread.json`). It pages the already published immutable artifact; it cannot inject a filesystem path.
- `agent.open` is controller preflight/binding readiness only and creates no native session identity.
- `agent.reconcile` reads recorded process/result/artifact evidence and never reruns the batch.
- `agent.recover`, send, steer, goal, reply and family are unsupported unless a future native contract adds them; absence is not emulated.

Exit 0 remains native batch completion, not Task acceptance. Issue #12 is now a code-wiring issue; installed-runtime qualification remains separate.

## 8. CheckRunner cache and reverse scope — resolves issue #6 contract choices

### 8.1. Reproducibility and cache key

- Reuse is opt-in: trusted CheckProfile declares `reproducible=true`; default false.
- Essential identity includes candidate content digest, profile ID/revision, resolved argv, targets/features, platform/architecture, toolchain/executable fingerprint, declared inherited environment names and their non-secret value digests, and other versioned external inputs.
- Unversioned network/time/service state disables reuse.
- Cache points directly to the original process CheckRun, never cache-to-cache.
- Missing/corrupt evidence, changed profile/toolchain/env or invalidated source decision makes the entry unusable.

### 8.2. Reverse-dependency baseline

The baseline is the exact `baseline_candidate_ref` frozen when the Attempt starts (normally the accepted predecessor selected by Task policy), not moving `main` and not whichever checkout is current.

Metadata analysis runs as a trusted CheckRunner step over captured source and versioned toolchain inputs. The selected package set expands for shared config/codegen/lock inputs. Unknown graph means the configured wide profile.

A narrowed run reports both selected and excluded scope. An excluded required target is a coverage gap, not a pass. Issue #6 becomes implementable without weakening the current safe default; reuse remains disabled until the complete identity is present.

## 9. Issues that remain evidence/qualification work

The following are not owner-policy blocks and must not be “resolved” by prose:

- #3 — live OpenCode restart semantics;
- #4 — live family/child-discovery qualification after the already landed per-child reader;
- #5 — live Muse Max/Windows/resume qualification;
- #9 — Codex write route and installed-server qualification;
- #10 — live native Git/remote publication qualification for the implemented first slice;
- #12 — installed Zed binary/runtime qualification for the implemented sessionless batch route;
- #11 — quiet-machine load run;
- #19 — ACPX stays gated; the remote gateway is MCP/HTTP, not a real ACP consumer;
- #20 — project the already implemented mailbox/background methods through MCP.

Issue #1 is no longer a blank module task: observer/configuration bridge.3 against OpenCodex 2.75 is implemented. It should be rewritten to track only native Codex route composition and live mixed-provider qualification.

## 10. Implementation order recorded at the 2026-10-02 baseline

This was the planned sequence when the edition was accepted. Current implementation and gate status are tracked in the README and issue-specific evidence; do not treat this dated order as a current backlog.

1. #20 MCP projection gaps required by remote profiles.
2. #14 policy identity/projection code.
3. #12 Zed wiring and #10 forge in independent slices.
4. #7 scheduler only after the accepted registry contract is implemented.
5. #8 remote GM pilot after the Remote Agent Gateway read-only path is qualified.
6. #6 cache/reverse scope after essential environment identity exists.
7. Live qualification issues remain owner-machine exercises and do not block unrelated local code.
