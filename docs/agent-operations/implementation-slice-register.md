# Implementation slice register

Status: current handoff register.
Source basis: `main` at `797ea93f1cfcaad8c8dde2629d666e64efb131c2`.
Current authority: [documentation map](documentation-map.md), [architecture](architecture.md), and the owner’s latest explicit task.

This register answers one question: **which bounded slice may be converted into an exact worker packet now?** It is not another roadmap and does not authorize implementation by itself. The strategic target remains [implementation.md](implementation.md); a worker receives only one approved packet from [`task-packets/`](task-packets/README.md).

## States

| State | Meaning |
| --- | --- |
| `LANDED` | Merged behavior with its recorded verification boundary. |
| `APPROVED_PACKET` | Exact source basis, source owners, positive path, deletions, uncertainty and focused acceptance are recorded in one packet. |
| `BLOCKED_DEPENDENCY` | The target is understood, but an earlier slice must land or dispose data first. |
| `AUDIT_REQUIRED` | The current contract exists, but exact callers/consumers or deletion/data disposition are not complete enough for a worker assignment. |
| `QUALIFICATION_ONLY` | Source work is complete for the stated boundary; only named executed evidence remains. |

Only `APPROVED_PACKET` may be assigned. Before assignment the manager verifies the packet’s source commit and listed blob SHAs. Any mismatch returns the slice to `AUDIT_REQUIRED`; the agent does not improvise around drift.

## Current handoff order

| Slice | State | Positive result | Required predecessor / reason | Owning contract and packet |
| --- | --- | --- | --- | --- |
| A0 | `LANDED` | One selected Antigravity v6 coordinate. | None. | [Implementation A](implementation.md#a--antigravity), [status](../implementation-status.md). |
| A1a–A1b | `LANDED` | One terminal producer and direct current Store admission without the duplicate refresh/proof gate. | Scoped Clippy only; runtime qualification remains separate. | [Status](../implementation-status.md). |
| A2.1 | `APPROVED_PACKET` | Exact `module.outcome` and `module.result` Store-delivery bytes survive IPC loss and are deleted only after an exact Store ACK; prior-boot files remain quarantined. | Source blobs must match `main@797ea93`; the unmerged A2 branch is evidence only. | [A2.1 packet](task-packets/a2-1-antigravity-durable-delivery.md). |
| A2.2 | `BLOCKED_DEPENDENCY` | Authorized successor imports prior-boot deliveries for the original Operation; Store aggregate coverage controls body release. | A2.1 must land and expose its exact retained-file contract. | [A2.2 packet](task-packets/a2-2-antigravity-restart-coverage.md). |
| A3 | `BLOCKED_DEPENDENCY` | Only real Antigravity actions and one active route remain; concrete old work is disposed. | A2.2 plus inventory of unsettled old-route work. | [Implementation A3](implementation.md#a--antigravity). |
| B | `AUDIT_REQUIRED` | Current-state queue and atomic route/root admission without copied workflow state. | Finish launcher/capacity consumers, cancellation and stable budget identity. | [Launcher](../swarm-launcher-assignment-context.md), AR-036–050. |
| C | `AUDIT_REQUIRED` | One manager-owned worktree and segment-safe write scopes. | Enumerate all current Git/workspace writers and review snapshot consumers. | [Implementation C](implementation.md#c--workspace-authority). |
| D | `AUDIT_REQUIRED` | Exact process custody and attached-binding retirement. | Trace every finalizer, late outcome and workspace-release consumer. | [Implementation D](implementation.md#d--process-custody), AR-017–019/045–050. |
| E | `AUDIT_REQUIRED` | One current-state automation loop using ordinary Operations. | Close actor/phase/transfer and occurrence-budget chains. | [Configuration](configuration.md), AR-051–058. |
| F | `BLOCKED_DEPENDENCY` | One compact renderer for initial, correction and resume input. | A connected initial/correction/result path must exist before prompt-format deletion. | [Implementation F](implementation.md#f--compact-taskpacket). |
| G | `AUDIT_REQUIRED` | Automatic review, all-findings correction and one common acceptance gate. | Close replacement, fairness and review-provenance consumers. | [Delivery](delivery.md), AR-059–064. |
| H | `AUDIT_REQUIRED` | Public, internal and adapter methods have one typed owner/authorizer. | Complete the CLI ↔ host ↔ MCP method/role/object matrix. | [Module API](module-api.md), [IPC/MCP](ipc-mcp.md). |
| I | `AUDIT_REQUIRED` | Numeric Store versioning, finite v1 bridge and query-only readers. | File-level Store/migration/read-model audit. | [Store evolution](store-evolution.md), AR-115–126. |
| J | `AUDIT_REQUIRED` | Concurrent frontend calls, one wake pump and exact hot reads. | Refresh current MCP/client symbols and consumer set before a packet. | [IPC/MCP](ipc-mcp.md), AR-127–141. |
| K | `AUDIT_REQUIRED` | One owner each for Check, Script, remote Git and GitHub API effects. | Complete effect/readback/cancellation and legacy-executor disposition. | [External effects](external-effects.md), AR-065–088. |
| L | `BLOCKED_DEPENDENCY` | Shared adapter shell exists only after two live adapters delete identical loops. | Two current consumers are not yet qualified. | [Implementation L](implementation.md#l--shared-adapter-shell). |
| M | `AUDIT_REQUIRED` | Claude uses one selected SDK driver and bounded delivery/recovery state. | Exact selected SDK/version/action matrix and old Node disposition. | [Implementation M](implementation.md#m--claude). |
| N | `AUDIT_REQUIRED` | Codex uses one app-server owner and verified native Goal capability. | Installed server capability and exact consumers must be refreshed. | [Implementation N](implementation.md#n--codex). |
| O | `AUDIT_REQUIRED` | One current Batch and one stable ACP v1 path. | Session/load, option readback and old profile/data inventory. | [Implementation O](implementation.md#o--command-batchacp). |
| P | `AUDIT_REQUIRED` | Muse keeps one qualified Node bridge, then retires custom Rust only after disposition. | Locked SDK/runtime capability and pending-result provenance. | [Implementation P](implementation.md#p--muse). |
| Q | `AUDIT_REQUIRED` | OpenCode converges on one standalone Rust owner. | Exact native contract, cutover consumers and old built-in work disposition. | [Implementation Q](implementation.md#q--opencode). |
| R | `AUDIT_REQUIRED` | Joined source-owned Governor observation with explicit gaps and unmatched subjects. | Provider/process/Git/remote collector consumers remain incomplete. | [Observability](observability.md), AR-142–159. |
| S | `BLOCKED_DEPENDENCY` | Optional products and duplicate tooling are removed vertically. | Each owning replacement/data disposition must be named first. | [Implementation S](implementation.md#s--non-coretooling-deletion). |
| T | `AUDIT_REQUIRED` | One recipient-indexed mailbox; coordination products are deleted. | Concrete unresolved-message/grant migration and public-surface inventory. | [Communication](../agent-communication-program.md), AR-100–113. |
| U | `AUDIT_REQUIRED` | Stable GitHub collection, explicit Task import and one write owner. | Collector cut, pagination coverage, import conflicts and work-pool consumers. | [GitHub sources](github-sources.md), AR-079–099/114. |
| V | `AUDIT_REQUIRED` | Bounded fixed-principal gateway and removal of the compatibility launcher. | Secret/session/origin/readback source refresh and caller cutover. | [Gateway](../gateway.md), AR-160–170. |

## Packet admission gate

A manager may change a slice to `APPROVED_PACKET` only when one document records:

1. exact source commit plus blob SHAs for every pre-existing write owner;
2. one observable caller-to-result path and one completion condition;
3. exact authority/state/effect/readback owners;
4. applicable AR/PF invariants and unsupported/unknown behavior;
5. old producer, caller, config, schema, fixture and data disposition;
6. comparison with prior experimental branches or rejected donor mechanisms;
7. minimal formatting and warnings-denied Clippy/build command;
8. focused connected acceptance evidence and explicitly deferred product-stage checks.

A design section, issue, source grep, experimental branch or passing compiler gate cannot change this register by itself.

## Documentation completion boundary

The documentation system is structurally complete for controlled implementation when:

- authority and navigation identify one current contract per responsibility;
- every known AR/PF failure maps to an invariant and closure evidence;
- this register prevents unaudited slices from being assigned;
- at least one selected slice has an exact approved packet;
- status, audit resume point, packet state and branch state agree;
- generated classification/link evidence is current.

This does **not** mean that the whole repository source audit or runtime qualification is complete. Those remain separately visible and cannot be converted into a false implementation-readiness claim.
