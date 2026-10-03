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

The implementation currently has 53 exact tools: 22 reads and 31 mutations. The `full` profile exposes this entire table. Restricted profiles use explicit method allowlists; adding a tool to the table does not make it visible or callable through a restricted profile.

| Profile | Additional methods | Surface |
|---|---|---|
| `observer` | None | `host.status`; Task, Attempt, and Operation reads; agent state/list/family; check reads; bounded artifact reads; `report.delta`, `report.attention`, `report.capacity`, and `message.read`. |
| `reviewer` | `task.request_changes` | Observer plus the explicitly addressed review/request-changes method. Acceptance and agent-control methods remain unavailable. |
| `manager` | `task.create`, `task.revise`, `task.claim`, `task.dispatch`, `task.submit`, `task.request_changes`, `attempt.release`, `attempt.bind_producer`, `operation.cancel`; typed `agent.open`, `agent.send`, `agent.reply`, `agent.configure`, `agent.goal`, `agent.background`, `agent.refresh`, `agent.reconcile`, `agent.recover`, `agent.result`; `message.send`, `message.cancel` | Observer plus selected Task, Attempt, agent, Operation, and mailbox operations. It excludes client administration, host admission mode, acceptance/invalidation, checks as mutations, source capture, artifact assembly, and shell/forge/module tools. |
| `gm` | `client.list`, `client.register`, `host.mode`, `task.accept`, `task.invalidate_acceptance`, `forge.publish_ref`, `gm.handover` | Manager plus the current GM-controlled surface. Forge publication additionally requires local allowlists and the exact accepted source candidate. Calls remain subject to the current application role and GM epoch. |
| `full` | Complete tool table | Explicit local compatibility for all currently exposed tools. Application checks remain authoritative. |

Every `tools/list` result is filtered by the selected profile. The `tools/call` pre-dispatch check independently rejects hidden methods before opening or writing local IPC, including a tool name sent manually. Tool annotations mark only read methods as read-only.

The MCP Tasks projection follows the same boundary: `tasks/get` requires the profile's `operation.get` and `report.attention` reads; `tasks/cancel` requires both `operation.get` and `operation.cancel`. A profile without those methods receives method-not-found before IPC. Restricted profiles also require a caller-owned `client_request_id` in `tasks/cancel` request `_meta`; the full profile alone preserves optional generated-ID compatibility. Custom `eliot/subscribe` requests are also checked before the subscription pump starts: every category requires `report.delta`, mailbox requires `message.read`, and operations requires `operation.get`. `eliot/unsubscribe` only closes session-local subscription state.

## Projected Phase-B operations

`message.send` exposes `recipient`, `text`, optional `in_reply_to` and `in_reply_to_digest`, and the optional `admission_deadline_ms`, `delivery_deadline_ms`, and `reply_deadline_ms`. Its structured output schema includes the delivery ID and payload digest, sender/recipient scopes and actor, all deadlines, reply reference, and cancellation reference. `message.cancel` requires both the exact `delivery_id` and `payload_digest`; its result records the cancellation reference and leaves the original delivery unchanged. Application digest, sender, stale-request, and unsupported-runtime errors are forwarded with their original ELIOT error codes and messages.

`agent.background` requires `binding_id` and `generation`; optional `session_id` targets an owned family member, and omission targets the binding root. It forwards the existing addressed application operation. It is a mutation and is visible only in manager, GM, and full profiles.

Restricted-profile mutations require a non-empty caller-owned `client_request_id` in `tools/call` arguments, and reject a missing or blank ID before IPC. The full profile keeps optional generated-ID compatibility; its generated ID is echoed only with a received result and cannot make a lost response retryable. The facade does not retry requests. Profile selection does not change the operation's application-level owner or authority, and application errors such as stale revisions, digest mismatches, and unsupported runtimes keep their original codes.
