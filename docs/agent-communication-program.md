# ELIOT Agent Communication, Launcher and MCP Program
## Start here

**Revision:** 15 — 2026-10-03
**Latest committed source:** C8 projection-fixture repair `7d518ef4edb84c5e8ce677fafa778de914abed30`; WorkDispatch implementation source is `a1577aee63094e6fcb3feea6fc6079d1a8454850`. Manager-owned operations requirements come from merged [PR #23](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/23).
**Status (2026-10-03):** the program remains partial and is not fully live-qualified. Canonical formatting and warnings-denied Clippy passed for C7 (13.06 s; `.local/qualification/r7-build-gate/clippy-c7-probe-repaired.log`). Full Rust CI run `37153513585` passed all Ubuntu and Windows job steps for exact commit `8570dae7f478b6dd2b604727b34c285a86ee9acc`, including Rust application/protocol tests, native-bridge fixtures/offline smoke, and the release build. The corrected targeted Windows `cargo test --test check_probe` also passed one test (1.04 s; build 43.34 s; `.local/qualification/r7-build-gate/windows-check-probe-c7-repaired.log`). The earlier failure came from a test fixture that omitted the outer `CREATE_NO_WINDOW` setting already used by production. A model-free `cmd-echo` diagnostic passed with that setting, but observed no live PID, so the exact OS/root cause is unconfirmed. C7 wires Participant credential issuance into launch admission with atomic private assignment context and a held database workspace lease; partial authenticated MCP readback covers configured/connect state only; and a durable bounded review-disposition consumer uses exact manager-on-behalf authority with semantic duplicate/gap handling. The lease/release lifecycle is database-only. Reviews of the unchanged C7 privacy, authority, disposition and lifecycle paths at `588a5ca21fbcbd5434540c011846535af7647f35` passed; `feedback_audit` also passed exact review of all three Windows-delta files in the final commit (`.local/qualification/r7-build-gate/windows-probe-lifecycle-audit.md`). C7 build/CI gates are green. C8 commit `a1577aee63094e6fcb3feea6fc6079d1a8454850` wires manager-authorized WorkDispatch auto-admission, the manual-shared immutable launch slot, and MCP install/proof handlers/readback. Owned-crate formatting and warnings-denied Clippy passed (12.46 s); the bounded StoreAPI regression passed 1/1 (0.06 s; build 35.09 s), covering Task-fact admission, exact slot reuse/conflict, idempotent receipt, one Operation, and Manager/outsider operation-scope filtering before pagination. C8 CI run `37157609062` failed in both OS jobs on three projection fixtures: the fixture database installed schema `001core` but omitted `002workspace`, so `workspace_leases` was absent. Fix commit `7d518ef4edb84c5e8ce677fafa778de914abed30` installs `002workspace`; the targeted projection filter passed 6/6 (0.05 s; build 37.36 s; `.local/qualification/r7-build-gate/projection-c8-ci-repaired.log`). Full Rust CI run `37158328828` passed all Ubuntu and Windows job steps for repair commit `7d518ef4edb84c5e8ce677fafa778de914abed30`, including formatting, Clippy, Rust application/protocol tests, native offline fixtures, and release build. Fresh-owned `mcp_plugin` startup remains unwired and unqualified. Productive dispatch, full manager-owned cycle, native-MCP harness loading and model execution remain unqualified. The runtime `0.160.0` native-harness schema and its three DTOs are repaired and Node `--check` passed; native-compose/trial and model execution remain unqualified. Credential/profile references are visible in Operation readback; the audit did not establish token/path exposure or a public bearer-token resolve route, so confidentiality is not qualified. All local model/inference work remains deferred by owner. C4's 221 Rust tests apply only to `2607c8858e573ae40459c27d76d8ae9e1ca9f8fc`. Other unsupported watch predicates, broader integration/scope-Git, Concilium, cron, Goal and native-Rust conversion remain planned or incomplete.

For the public list of remaining implementation blocks and their critical path, see [Implementation Status](implementation-status.md).

## 1. Product rule

ELIOT centralizes assignment, authority and acceptance, not every engineering conversation. Managers work manually by default and may enable individual automations that act on their behalf. Ordinary peer communication is neither assignment nor permission to start another model.

```text
manager selects work and scope
  -> launcher establishes the work context
  -> participants read current facts, contact exact owners and coordinate
  -> only real authority/global conflicts reach the manager
  -> exact-candidate review and protected acceptance/publication
```

Peers can ask, answer, publish current contracts, compare producer/consumer seams, record reversible assumptions and agree inside existing scope. Root and the auditor do not relay every message. Neither messages, silence, consensus, a watch nor a loaded tool schema grants authority.

## 2. Read the owner document for the change

Read the canonical product/module contract and [Owner Decisions](owner-decisions.md), then the relevant document below. This entrypoint is a routing index, not another copy of every schema/tool list. Do not load all research and historical examples into every writer's prompt.

| Responsibility | Current design owner |
|---|---|
| Fleet behavior, sparse relevance, integration cells, delivery states/backpressure | [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md) |
| Exact high-level tool names, role palettes and client topology | [Canonical MCP Surfaces](mcp-canonical-surfaces-and-topologies.md) |
| Registry metadata, groups, paged discovery and deferred loading | [MCP Tool Catalog](mcp-tool-catalog-and-loading.md) |
| Dashboard, queue, launch/assignment context, Git overlap | [Swarm Launcher](swarm-launcher-assignment-context.md) |
| Scoped Participant registration and authorization routing | [Peer Autonomy Implementation](agent-communication-peer-autonomy-implementation.md) |
| Self-service communication and agreement envelope | [Peer Autonomy](agent-communication-peer-autonomy.md) |
| Existing mailbox/Store/Git source constraints | [Implementation Checklist](agent-communication-implementation-checklist.md) |
| Manager-owned automations, review results, cron, hooks, Goal and scripts | [Agent Operations PR #23](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/23), `docs/agent-operations/` |

The fleet and canonical-surface documents replace old count-based sponsorship, eager aliases and full-directory examples. The canonical assigned-reviewer result is `review.submit`; `review.get` and `operation.get` may read only that reviewer's exact retained assignment/result, including after release, while authorization remains valid. They do not expose lists, historical context, artifacts or evidence. A pending `review_scope` with `review_assignment_id: null` is unusable until the server atomically binds it to the exact assignment. The reviewer cannot perform the manager's `task.request_changes` disposition. A legacy profile named `Reviewer` may retain old compatibility behavior only as an explicitly identified profile, never as the canonical assigned-reviewer contract.

Expanded [Concilium](agent-communication-concilium.md), [Tool Contracts](agent-communication-tool-contracts.md) and [Issue Plan](agent-communication-implementation-issues.md) remain useful detail/history, subject to these current owners. Their older example names, role restrictions and numbered implementation sequences are not parallel APIs or another work plan. Resolve an actual cross-document discrepancy in its owning documents; do not invent a new precedence appendix, authority or compatibility alias.

Read evidence only as needed: [MCP sources](mcp-tool-catalog-sources.md), [fleet sources](agent-communication-fleet-scale-sources.md), [peer sources](agent-communication-peer-autonomy-sources.md), [field evidence](agent-communication-field-evidence.md). Reports and research are not shipped ELIOT capabilities or mandatory software versions.

## 3. Core invariants

Keep these quantities separate:

```text
P registered participants
A active model turns/processes
E material coordination edges
X manager-required exceptions
```

Registration is not a running model; a project is not a shared room; thousands of participants do not require all-to-all communication or one polling task per identity.

The MCP contract has three independent layers: hard permission profile, small initial role surface, and authorized deferred catalogue. The application checks object/action rights again. Search or loading cannot widen role, Task ownership, repository scope or GM authority. Unsupported discovery does not silently fall back to the full profile.

The canonical surface document is the only exact convenience-tool list. Keep names and schemas there and in the shared code registry, not duplicated in this index. Configure all owned internal services/adapters in Rust; Python/PowerShell are optional external scripts, not required internal controllers. Do not prescribe old fixed crate/CLI/model releases.

One manager owns one mutable worktree/candidate and one in-flight product submission. Internal writers receive non-overlapping work inside that Issue; they do not independently assign work, publish fragments or run Cargo. Git history/blame corroborates provenance, not current assignment ownership.

## 4. Useful path before optional machinery

The launched agent gets a bounded assignment context: exact Task/Attempt, requirements/sources, workspace/scope, relevant peers/contracts, known overlap, runtime/tool capabilities and required result. Full source, history, queue, transcripts and tool schemas are read on demand.

Prefer the cheapest sufficient operation:

```text
current card/field
  -> exact consultation
  -> integration comparison/cell
  -> irreducible bilateral negotiation
  -> manager decision
  -> Concilium only when justified
```

A local agreement is not verification. Required unknowns, absent affected owners or changes to global/security/persistence/identity/lifecycle authority remain explicit. Participant count alone does not require manager approval. Only actually affected owners block the relevant decision.

Watch notices and inbox messages do not wake idle agents or replace their task. Preserve distinct stored/available/presented/consumed/held/refused/cancelled/stale delivery facts. Backend `tools/list` does not prove the model loaded the schema. Keep real evidence levels and gaps rather than generating extra model calls to claim readiness.

## 5. Current implementation sequence

```text
A1  mailbox primitive extraction/reuse
A2  scoped Participant, relevance indexes and cards
A3  consultation, watches, assumptions and delivery truth
A4  comparator, integration cells and peer-local autonomy
A5  irreducible negotiation and manager-required contracts
A6  advisory scopes and bounded Git inspection
A7  dashboard/queue/context/overlap read projections
A8  launch preview/orchestration and capability evidence
A9  grouped/paged/deferred MCP catalogue and role surfaces
A10 durable Concilium state
A11 CLI/MCP/UI parity, compact rendering and status
A12 correctness, catalogue/fleet scale, recovery, cost and live qualification
```

Align the shared authorization, watcher, launcher and review paths with the operations program; do not implement a second copy under its O1–O11 labels. Each production increment has a real producer, consumer, registration and result reader. Local manual work must not wait for optional remote GitHub, cron or external-script setup.

One implementation Issue has one manager/worktree/candidate. The manager reviews/integrates and runs scoped formatting/minimal warnings-denied Clippy once on the complete candidate. Broad test/load/live work follows the completed-product or explicit acceptance phase. Test targets in research are future qualification, not repeated writer Cargo work.

## 6. Concilium and acceptance

Concilium is manager-sponsored, bounded and advisory. Peers may propose; the authorized manager opens and advances rounds. Open commits slots, not model work. Preserve independent initial positions, evidence, minority objections and missing knowledge. No nested free-running chat or majority-vote acceptance.

Exact-candidate audit, Task acceptance and publication remain distinct. A reviewer submits findings; a manager or enabled manager-owned automation applies disposition. The same candidate/rights/epoch checks serve manual and automatic actions. Late results about A never authorize or modify B.

## 7. Qualification and privacy

Measure synthetic registered populations separately from active paid models. Existing contours for thousands of cards/participants, bounded readers and staged active turns are targets, not current capacity claims. Test lost notifications, stale identities, concurrent replies, actual tool availability, missing reporting capability, process cleanup, wrong-tool/token cost and useful completion per cost.

No second Store, Task graph, event authority, broker or per-agent polling service. Reuse durable Operations/Observations and bounded Rust machinery; in-memory signals are freshness hints.

Repository examples use placeholders only. Real deployment endpoints, credentials, infrastructure IDs and private paths stay local. This program itself activates nothing, changes no machine and makes no claim of runtime qualification.
