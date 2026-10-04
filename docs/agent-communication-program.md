# ELIOT Agent Communication, Launcher and MCP Program
## Start here

**Current C10 status:** Published source `cea63dde1d923f821c436c61f2561bcfb6a4bb0d` passed owned-source formatting, warnings-denied Clippy (12.16 s) and debug build (34.84 s); four bounded source audits passed. CI run [37170376636](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37170376636) completed successfully on Windows and Ubuntu. Native run `bb070791-ba7c-4c71-9cc7-660fbb531418` failed before an owned-service row, MCP proof or Bun/model call; the cause was traced to a manifest-route object/alias-string comparison. The three-file follow-up source repair passed independent audit, package formatting, production Clippy (14.67 s) and debug build (35.37 s; candidate SHA-256 `DEDBF403020780A35ED0141A31EF8A43654073B9B2A96CC4BF88AC77B462B528`), but has no final commit SHA or CI yet. Native run `46cef212-aa65-418a-a911-b54502fe9fd7` is active at `awaiting_binding` with a held lease and no owned-service row or MCP proof; opening-fence diagnosis is active. Do not classify this run as a final success or failure. See [Implementation Status](implementation-status.md).

The C7/C8 and C9 paragraphs below preserve prior qualification only; use the current C10 status above.

**Revision:** 18 — 2026-10-03
**Latest committed source:** `cea63dde1d923f821c436c61f2561bcfb6a4bb0d`. C9 source `2e609ecf7d826da7019fe5e5f2ed397a992bc45a` remains the prior green CI baseline (run 37168223030).
**Historical C7/C8 CI:** Exact runs and source commits are recorded in [Implementation Status](implementation-status.md).

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
