# MCP tool profiles

`swarm mcp` is a local stdio frontend over authenticated Eliot IPC. A profile filters
which tools the frontend advertises and accepts. It does **not** assign an Eliot role,
Task ownership, GM authority or native capability. Store repeats current role/object/
revision authorization for every forwarded application request.

## 1. Selection and identity

A named local profile binds one presentation profile to one expected Eliot client ID:

```toml
[mcp]
default_profile = "dot-observer"

[mcp.profiles.dot-observer]
tool_profile = "observer"
expected_client_id = "eliot-dot-observer"

[mcp.profiles.manager]
tool_profile = "manager"
expected_client_id = "eliot-manager"

[mcp.profiles.local-full]
tool_profile = "full"
expected_client_id = "operator"
```

The selected name is fixed for one stdio process. A credential whose client ID does
not match the selected entry is rejected before serving tools. Restricted profiles
use distinct identities; `full` is an explicit local compatibility presentation and
still bypasses no Store check.

Credentials, paths, tunnels, domains and provider secrets remain outside model-visible
MCP tools/resources.

## 2. Current closed profile names

Current code defines:

```text
observer
reviewer
participant
assigned_reviewer
manager
gm
full
```

These are current presentation categories, not permanent product architecture.
`reviewer` is a legacy profile distinct from the assignment-bound auditor. The compact
target retains only profiles required by current clients and removes coordination/
Concilium/tool surfaces when those products are deleted.

| Profile | Intended scope |
| --- | --- |
| `observer` | Bounded status, Task/Attempt/Operation, result/check/report/mailbox and local Governor reads allowed by the credential. |
| `participant` | Exact assigned Task/Attempt context, mailbox, current candidate/result and selected agent actions within its scoped grant. No general Manager authority. |
| `assigned_reviewer` | Exact assigned review/candidate/source/check/artifact reads and `review.submit`; historical access remains assignment-scoped. |
| `manager` | Task planning, queue/route/workspace/agent control, messages, review/correction/acceptance policy, Goal, scripts/checks and configured integrations within current Manager scope. |
| `gm` | Current-GM actions such as handover and cross-manager current-work decisions in addition to its configured Manager surface. It is not automatically every local tool. |
| `full` | Entire current closed catalog for explicit trusted local compatibility; application authorization still applies. |

The old operator-only Task planning defect is already fixed: current Task handlers use
`require_task_planner`. Do not add another grant or approval layer to solve an obsolete
finding.

## 3. One catalog, two checks

`swarm-contracts::mcp_catalog` owns public names/schemas and profile exposure. The MCP
frontend checks the selected presentation profile; Store checks live application
policy and exact object authority.

```text
MCP profile exposure
-> current Store allowed-method read
-> typed application request
-> exact domain authorization
-> handler
```

A tool appearing in a profile does not prove that the current credential owns the
object or that the selected adapter implements the native action. Conversely,
`manual_tools`/catalog presentation means an explicitly selected schema, not that a
human must call it or that an authorized helper cannot automate it.

Do not maintain contradictory profile tables in several documents. Source registry
and current configuration are authoritative for current exposure; this document owns
the boundary and target.

## 4. Requests, retries and unknown outcomes

Restricted-profile mutations require a caller-owned nonempty `client_request_id`.
Compatibility-generated IDs in `full` do not make a lost response safely retryable.
The frontend never automatically replays a request after transport failure.

A timeout/disconnect means the Store outcome may be unknown. Reconnect with the same
identity and read the exact Operation/current object. Do not change request ID to
bypass an unresolved effect.

`tasks/get`, `tasks/cancel` and subscriptions require the exact underlying read/action
methods in the selected profile. Notifications are bounded wake hints; authoritative
history is Store readback. Slow or disconnected clients resume from their Store
cursor, not a second MCP event log.

## 5. Autonomous agents, not human-only tools

Governor and Manager are agent identities. Their profile must expose the ordinary
commands required by the configured autonomous work cycle:

```text
select/dispatch or resume
read attention and exact native request
Reply/Send/Configure/Goal where route supports it
capture result and submit candidate
assign/read review
return all findings
run checks
accept
selected import/publication integration
```

The positive path still passes Store authorization and adapter capability routing.
Do not hide every mutation from an agent and then call the resulting operator work a
safety feature. Use narrow identity/object scope, current revisions, typed actions and
readback—not per-step human confirmation.

Workers receive only the small surface needed for their assignment. A participant
should not see fleet administration or unrelated Task history. A Manager agent should
not need the operator credential for ordinary owned work.

## 6. Mailbox and subscriptions

The target mailbox is recipient-indexed and uses one durable message/Operation ID.
`message.read` returns only rows addressed to the current client; reading does not
delete. Reply/cancel uses exact message identity.

Current subscription categories still include Concilium/coordination. Remove those
categories and method requirements when the duplicate coordination platform is
removed. Retain:

- current Governor/report invalidation;
- mailbox wake;
- exact Operation/current-object reads;
- cursor gap/resync behavior.

A subscription never creates facts, starts a model or grants rights. Current-state
automation decides whether an idle assigned agent should resume to handle mail.

## 7. Target simplification

As implementation slices land:

- delete public `module.*`, `native.*` and standalone scheduler-service RPC from MCP;
- remove coordination/Concilium/contract/card/watch methods and profile entries with
  their products;
- expose closed semantic agent actions instead of vendor methods;
- keep high-level common-path tools small and searchable/deferred lower-level tools;
- preserve exact assigned-reviewer and participant scopes;
- derive profile exposure from the single catalog rather than copied allowlists;
- keep public profile changes synchronized with actual Store handlers in the same
  slice.

Do not replace the current catalog with a policy DSL or generic JSON passthrough.

## 8. Verification

- profile and expected client identity mismatch rejects before serving;
- profile exposure cannot grant a Store action or unrelated object read;
- Manager can perform ordinary owned Task planning/execution without operator bearer;
- participant and assigned reviewer cannot escape their Task/candidate scope;
- lost mutation response is recovered through Operation readback, never replayed;
- autonomous Manager cycle has the required tools without `full`/operator profile;
- removal of coordination products removes their tools/subscriptions without breaking
  mailbox, review or current Governor reads;
- no documentation claims an already-fixed operator-only Task defect is current.
