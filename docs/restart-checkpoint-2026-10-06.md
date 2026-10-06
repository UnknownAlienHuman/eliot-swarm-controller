# Source queue checkpoint — 2026-10-06

**Product status: PARTIAL_PROGRESS. Current queue: incomplete; work stopped at owner request.**
This document records source delivery and the continuation point. It does not
qualify the current binaries, providers, models, or complete productive workflow.

## Published source entering this closeout

`a5fb45acf116c927a5be50f43efc8712de6e8a31` publishes shared mailbox admission,
Goal continuation and the first configured GitHub managed-label projection.
`4775410ef334c814b686c89ade00e24b97dc0a2c` corrects the Goal continuation
failure/unknown receipt path. Earlier commits remain part of this history;
`143befa` is the earlier automation-discovery repair, not a later publication.

## Current assigned queue

| Area | Delivered behavior | Integration state |
| --- | --- | --- |
| Mailbox | One immutable addressed message; shared legacy/typed admission and exact delivery cancellation/readback | Published |
| Thread | Immutable roster/context, typed messages, bounded history, current mutation authority, retained Manager history and documented `coord-<uuid>` IDs | Included in source closeout |
| Contracts | Immutable proposal revisions and explicit responses; bounded exact revision readback | Included in source closeout |
| Scope intents | Advisory propose/accept/inspect/conflicts/release; immutable decisions and real accepted-scope revision references | Included in source closeout |
| Contract decisions | Frozen V1 and incomplete V2 private candidates retained; no public ratify/reject | Not integrated; unfinished |
| Integration agreement | Exact-version acknowledgements and retained positions; conservative autonomy classification and historical readback | Included in source closeout |
| Goal progression | Exact terminal evidence and separate continuation admission; failed/unknown outcomes retain errors without success evidence | Published V5 |
| GitHub automation | Configured accepted-candidate managed-Issue label; provably unsent stale-operation rejection and fair durable drain | Included in source closeout |
| Public frontends | Typed Store policy, CLI, MCP catalog/profile/schema for the 18 delivered methods; small committed Thread/contract hints | Included as current source snapshot; unqualified |

The integrated Store source surface consists of 18 methods:

- `coordination.thread.open/get/list/resolve/withdraw/supersede` and
  `coordination.message.send`.
- `coordination.contract.propose/respond/get/list`.
- `code.scope.propose/accept/inspect/conflicts/release`.
- `coordination.integration.ack` and `coordination.agreement.get`.

These 18 methods have source handlers and corresponding CLI/MCP entries; no
ratify/reject entry is exposed. They use the existing authenticated Store, Operation/Observation stream and
transaction boundary. Read history does not depend on the old GM session or
Attempt remaining live. New effects still require actual current authority.
Proposal support, advisory agreement and a manager decision do not accept a Task.

Own rejected acknowledgement receipts are read before cell lookup. Stale
agreement reads return retained facts with `manager_required` and
`peer_agreed=false`. A peer-local agreement requires complete actual accepted
scope coverage and matching positions from every affected participant.

The GitHub correction is limited to an exact internal Operation that remains
queued, has no sent timestamp and retains its admission/cause/slot identity.
Sending or uncertain effects are never released or automatically replayed.
Subscriptions contain IDs/cursors and require the corresponding authorized
readback methods. Participant subscriptions remain unavailable because their
profile does not expose `report.delta`.

## Earlier source preserved

- Kernel in `crates/swarm-kernel-host`, public host/CLI wrappers and independent
  supervisor with authenticated IPC and separate process ownership.
- Retained startup, writer, panic, supervisor and child-process error paths,
  restart holds and Manager attention/readback.
- Five Rust adapter sources: OpenCode, Codex, Command Code, Claude and Antigravity;
  common immutable result contracts and installed sibling-chain validation.
- Eight Concilium methods, blind positions, bounded history and advisory closure.
- Manager-owned automation configuration, generic admitted-event selectors,
  dispatch/review/repair/acceptance/publication consumers, ScriptRun, hooks and
  scheduling. Any admitted event source/kind can be selected as a script trigger;
  source visibility and permission to perform an action remain separate checks.
- Source-gap/error retention, fair cursors, exact ScriptRun holds and transfer
  rules; generic adapter descriptors and bounded Manager attention projections.

See [implementation status](implementation-status.md) for the exact historical
source and verification scopes.

## Code not implemented or still partial

1. **Current queue remainder:** contract ratify/reject V2 and its module/parser/policy/
   dispatch/readback/frontend glue remain unfinished and unexposed. No private
   candidate is counted as delivered. **I6 bounded Git inspection:** canonical public inspection methods are absent.
   This closeout starts no new Git inspector or generic shell dispatcher.
2. **Thread lifecycle remainder:** bilateral exact resolution acknowledgement,
   actual soft-limit attention/model wake and cross-Attempt successor/closure
   behavior are not implemented by this substrate.
3. **Goal parity:** truthful terminal producers are limited to Codex/OpenCode.
   Complete continuation, native Goal capability and editor/client parity across
   the other adapters remain unfinished.
4. **Remote-effect breadth:** automatic GitHub projection implements only the
   configured managed label. Automatic Checks, PR summaries/closure and broader
   typed effects remain. Existing manual effects retain their own recorded scope.
5. **Complete local O7 workflow:** native correction, result/acceptance/publication,
   family reconstruction and recovery of unrecorded native effects are not fully
   qualified. Missing native identity evidence cannot be reconstructed by guess.
6. **Optional Eliot Memory OS:** the conditional compile-packet request is
   [committed](eliot-memory-os-integration.md); connected runtime integration is
   not implemented. ELIOT/codebase-memory tools were absent in this session;
   no empty database or substitute service was started.
7. **Dependency follow-up:** the rustls lock repair to `0.23.45` is source-only.
   Three optional OpenCode bundle advisories have no published patched version
   in the recorded audit; nine development-only Codex donor Python alerts require
   a deliberate donor refresh with matching provenance.

Pinned OpenCode evidence still records `NATIVE_ASSISTANT_PARENT_UNAVAILABLE`
when no immutable assistant/input parent join is available. Antigravity records
`RESULT_BODY_UNAVAILABLE` where the result body cannot be obtained. These are
explicit limitations, not completed productive results.

## Verification boundary

This source queue has **zero compiler/build runs, zero tests and zero
native/provider/model calls**. Source inspection, exact candidate/preimage hashes,
owned-file formatter parsing and whitespace checks are the only current checks.
Compilation and behavior remain unverified.

Historical checks apply only to their recorded revisions: strict Clippy for
24 production packages at `6074dcf`, 12 focused writer/Store/process cases, and
one consolidated release build for 17 production binaries at `9b5acc9`.
Those receipts do not qualify this source queue or all native adapters.

Development is stopped. After explicit resume, finish the remaining source slices
before tests. Then use one scoped build and
the external shared target
`C:\Users\kleym\AppData\Local\Eliot\build\rust-env-target`, with packaging output
outside the repository and target. No new checkout targets/worktrees were created
in this closeout. Native qualification order is OpenCode, Command Code, Codex,
Antigravity, then Claude with its limited quota. The test selector is
`inclusionai/ling-3.1-flash`; installed Command uses
`inclusionai/ling-3.1-flash:free`. Bunny is disabled. Codex uses OpenAI subscription
authentication. Actual provider/model execution remains unverified.

Current Codex/OpenCodex processes were preserved. Linux, WSL, all local models
and Zed remain deferred. The old `Eliot Swarm Controller` path was absent on
the final path check; this is not a claim that the separate `eliot-swarm`
directory or all historical caches have been removed or resized.

## Restart records

Root is the sole tracked/Git integrator. Luna source candidates remain in
`.local/pr-implementation/`; READY versions are immutable. Current publication,
pins, corrections and remaining work are saved in
`.local/pr-implementation/root-source-integration-20261005.json`, and the
append-only journal is
`.local/qualification/restart-checkpoint-20261003/HANDOFF.md`.
The unfinished decision source and exact continuation are also tracked under
[docs/continuation/2026-10-06/contract-decisions-v2](continuation/2026-10-06/contract-decisions-v2/README.md),
so this work is not limited to chat or ignored local packets.
After restart, read this checkpoint and the implementation status, verify the
local and remote `main` refs. Resume the remaining source queue only after owner
instruction. Do not
replace preserved state or resume uncertain native inputs automatically.
