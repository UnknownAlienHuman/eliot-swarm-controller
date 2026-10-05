# MCP tool profiles

`swarm mcp` is a stdio client of the running host over authenticated local IPC. It does not open the database or a network listener. MCP profiles narrow the visible method set before the existing application authorization runs; they do not assign ELIOT roles, Task ownership, or GM authority.

## Selecting a profile

Profile names and ELIOT client bindings live in the local typed TOML configuration. `--profile NAME` selects one configured entry; when omitted, `mcp.default_profile` applies. The selected profile is fixed for the life of that stdio process. Start a new MCP session to select a different profile.

The default configuration selects `local-observer` for the `operator` client. Use `swarm mcp --profile local-full` to explicitly restore the complete local tool table for the same local operator. `full` is a compatibility surface, not a second authorization mode: the application still checks the credential role, object ownership, request identity, and GM epoch.

Each configured entry binds one local profile name to exactly one expected ELIOT client ID. Restricted profiles must use distinct client IDs. For example, use separate bindings for Dot and Muse; a credential whose client ID does not match the selected entry is rejected before the MCP server starts. The ID is not a secret. Keep credential files and local configuration outside shared repository content when they contain private installation details.

```toml
[mcp]
default_profile = "dot-observer"

[mcp.profiles.dot-observer]
tool_profile = "observer"
expected_client_id = "eliot-dot-observer"

[mcp.profiles.muse-observer]
tool_profile = "observer"
expected_client_id = "eliot-muse-observer"

[mcp.profiles.local-full]
tool_profile = "full"
expected_client_id = "operator"
```

Launch either named profile with `swarm mcp --profile dot-observer` or `swarm mcp --profile muse-observer`, and supply the corresponding local credential through the existing `--credential` option. No credential, token, path, tunnel, domain, or cloud account setting is exposed by MCP tools, resources, or diagnostics.

## Closed tool surfaces

At source `2aec51bb` (2026-10-05), the implementation has 120 exact tools: 54 reads and 66 mutations. These counts describe the closed `TOOLS` registry in `src/mcp.rs`, not the number injected into every model session; deferred loading is described in [Tool Catalog](mcp-tool-catalog-and-loading.md). The `full` profile exposes this entire table. Restricted profiles use explicit method allowlists; adding a tool to the table does not make it visible or callable through a restricted profile.

**Known authorization defect, reproduced at that source:** although the manager
surface includes `task.create` and `task.revise`, both Store handlers still call
`require_operator()`. A registered manager and the designated GM are rejected.
The [modular-runtime correction](agent-operations/modularity.md#4-less-authorization-ceremony-one-effective-policy)
removes this unintended gate without distributing the operator credential or
widening Participant/Module/Observer rights. This documentation update does not
fix the executable. Treat the table below as profile scope, not successful-call
qualification; exact current public names and assigned-reviewer scope are owned
by [Canonical MCP Surfaces](mcp-canonical-surfaces-and-topologies.md).

| Profile | Current method scope (summary, not an exhaustive registry) |
|---|---|
| `observer` | Search/dashboard/status and selected Task, Attempt, Operation, family, check, artifact, report and mailbox reads. |
| `reviewer` | Legacy observer-plus-`task.request_changes` profile. This is not the assignment-bound auditor role. |
| `participant` | Scoped context, peer/card/inbox/watch/consultation/integration, exact current Task submission and candidate/artifact reads, plus selected review/evidence methods. Application assignment checks remain authoritative. |
| `assigned_reviewer` | Selected review context/get/list/submit and linked submission/check/artifact/Operation reads. After release, only the exact retained reads permitted by the canonical assignment contract remain available. |
| `manager` | Observer plus Task/Attempt/agent control, messages, participant administration, review assignment, manager-owned automation configuration, manual scheduled invocation, hooks, Goal, scripts, queue/launch/overlap, scoped watches and selected GitHub effects. No generic shell passthrough or automatic acceptance rights. |
| `gm` | Most manager methods plus client/host/acceptance/Forge, GitHub source/work-pool and recovery methods, and GM handover. **Current code excludes `automation.config.preview` and `automation.config.apply` from this profile**; it is not an unconditional superset of manager. Application role/GM epoch checks still apply. |
| `full` | Entire closed tool registry; opt-in local compatibility. It does not bypass application checks or prove native capability. |

The exact allowlist is [`src/mcp/profiles.rs`](../src/mcp/profiles.rs); public names
and deferred groups are specified by the canonical surfaces/catalog documents.
The intended simplification is one effective method policy with explicit profile
narrowing, not multiple contradictory tables or extra approvals. Do not silently
broaden a remote profile while fixing a local manager's application rights.

Every `tools/list` result is filtered by the selected profile. The `tools/call` pre-dispatch check independently rejects hidden methods before opening or writing local IPC, including a tool name sent manually. Tool annotations mark only read methods as read-only.

The MCP Tasks projection follows the same boundary: `tasks/get` requires the profile's `operation.get` and `report.attention` reads; `tasks/cancel` requires both `operation.get` and `operation.cancel`. A profile without those methods receives method-not-found before IPC. Restricted profiles also require a caller-owned `client_request_id` in `tasks/cancel` request `_meta`; the full profile alone preserves optional generated-ID compatibility. Custom `eliot/subscribe` requests are also checked before the subscription pump starts: every category requires `report.delta`, mailbox requires `message.read`, and operations requires `operation.get`. `eliot/unsubscribe` only closes session-local subscription state.

## Projected Phase-B operations

`message.send` exposes `recipient`, `text`, optional `in_reply_to` and `in_reply_to_digest`, and the optional `admission_deadline_ms`, `delivery_deadline_ms`, and `reply_deadline_ms`. Its structured output schema includes the delivery ID and payload digest, sender/recipient scopes and actor, all deadlines, reply reference, and cancellation reference. `message.cancel` requires both the exact `delivery_id` and `payload_digest`; its result records the cancellation reference and leaves the original delivery unchanged. Application digest, sender, stale-request, and unsupported-runtime errors are forwarded with their original ELIOT error codes and messages.

Mailbox and receipt reads are addressed. `message.read` and the Mailbox subscription return only entries addressed to the authenticated recipient; cancellation receipts are not mailbox entries. `report.delta` and the Reports/Operations subscriptions expose each `message.send` or `message.cancel` receipt only to its authenticated sender/canceler and the original recipient; cancellation recipient visibility is resolved from the unique immutable settled send matching both delivery ID and payload digest. The same sender/recipient scope applies to `operation.get` and `operation.list`, with filtering before pagination. A verified bootstrap local operator retains the global Operation and report diagnostic view. These are application API scopes, not OS isolation between processes running as the same full-access user.

This receipt scope applies only to `message.send` and `message.cancel`. Shared `task.feedback` report events and Task review/acceptance history retain their existing visibility in `report.delta` and Operation reads. A directed notification in `message.read` is a delivery projection, not a confidentiality boundary.

`agent.background` requires `binding_id` and `generation`; optional `session_id` targets an owned family member, and omission targets the binding root. It forwards the existing addressed application operation. It is a mutation and is visible only in manager, GM, and full profiles.

Restricted-profile mutations require a non-empty caller-owned `client_request_id` in `tools/call` arguments, and reject a missing or blank ID before IPC. The full profile keeps optional generated-ID compatibility; its generated ID is echoed only with a received result and cannot make a lost response retryable. The facade does not retry requests. Profile selection does not change the operation's application-level owner or authority, and application errors such as stale revisions, digest mismatches, and unsupported runtimes keep their original codes.
