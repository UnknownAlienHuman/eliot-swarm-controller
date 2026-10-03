# ELIOT MCP Tool Catalog and Deferred Loading
## Small role-specific core, searchable domain groups, hard authorization and verified runtime capability

**Revision:** 5 — 2026-10-03
**Source baseline:** C7 implementation commit `2f00c2b3d7862788ca8a6bead4645d3ce64dc0ea`; C6/local TaskSubmission-intake baseline is `e035c0c3fe855490863be81902c5152b548c42cd`.
**Applies to:** [Communication Program](agent-communication-program.md), [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md), [MCP Profiles](mcp-profiles.md)
**Status:** normative catalog contract. Canonical formatting passed and warnings-denied Clippy passed for C7 (13.06 s; `.local/qualification/r7-build-gate/clippy-c7-probe-repaired.log`). The C6 CI run `37147184684` passed Linux and does not cover C7; full CI for this commit is pending. The corrected targeted Windows `cargo test --test check_probe` passed one test (1.04 s; build 43.34 s). The earlier failure came from a test fixture that omitted the outer `CREATE_NO_WINDOW` setting already used by production. A model-free `cmd-echo` diagnostic passed with that setting but observed no live PID, so the exact OS/root cause is unconfirmed. Reviews of the unchanged C7 privacy, authority, disposition and lifecycle paths at `588a5ca21fbcbd5434540c011846535af7647f35` passed; `feedback_audit` also passed exact review of all three Windows-delta files in the final commit (`.local/qualification/r7-build-gate/windows-probe-lifecycle-audit.md`). C7 wires Participant credential issuance into launch admission, partial authenticated configured/connect readback, and a durable bounded review-disposition consumer. This readback does not establish tool or model loading. Credential/profile references are visible in Operation readback; the audit did not establish token/path exposure or a public bearer-token resolve route, so confidentiality is not qualified. `launcher_mcp_tools.rs`, `mcp_plugin.rs` and work-dispatch modules are present but unwired and uncompiled. Lease/release lifecycle is database-only. No overall green gate, productive dispatch, full-cycle qualification or native-MCP harness loading is established. The runtime `0.160.0` schema and its three DTOs are repaired, and Node `--check` passed without model execution. Native-compose/trial execution is not qualified. No model execution ran. C4's 221 Rust tests remain tied to `2607c8858e573ae40459c27d76d8ae9e1ca9f8fc`.
**Precedence:** this file governs MCP grouping, eager/deferred loading, catalog metadata and launcher-facing tool UX. Existing application authorization and Task/Attempt authority remain unchanged.

## 0. Decision

The [Canonical MCP Surfaces](mcp-canonical-surfaces-and-topologies.md) document is authoritative for exact public names. The canonical assigned-reviewer core uses `review.submit`, with `review.get` and linked `operation.get` limited to that reviewer's exact retained assignment/result, even after release if the credential remains valid and unrevoked. No historical context/list, artifact or evidence reads are implied. A null `review_scope.review_assignment_id` is pending until an atomic server bind. The legacy profile named `Reviewer` may remain only as an explicitly identified compatibility profile; its behavior does not redefine the assigned-reviewer core.

ELIOT may eventually expose many application methods, but an ordinary model must not receive the complete schema catalog at session start.

The tool system has three independent axes:

```text
hard authorization profile   what this identity may ever call
session tool surface          what is immediately visible/callable
searchable catalog            what may be loaded on demand inside the hard profile
```

They must never be collapsed into one setting.

```text
profile denies a method
    -> hidden from discovery
    -> manual tools/call is rejected before IPC
    -> tool search cannot reveal or activate it

profile allows a method but surface defers it
    -> searchable by name/purpose/group
    -> loaded only when needed and supported by the client
    -> activation never widens application authority
```

The default model-facing experience is:

```text
6–9 high-value tools
+ one tool-search/catalog affordance
+ clear domain descriptions
+ exact on-demand loading
```

It is not:

```text
50–200 schemas injected into every turn
one enormous generic passthrough
all manager/admin tools visible to every worker
manual memorization of low-level application method names
```

## 1. Current source facts and the exact gap

At baseline `36cfb652`, the MCP facade had 53 typed tools, fixed profiles and profile filtering before local IPC. C4's 221 Rust tests apply only to `2607c8858e573ae40459c27d76d8ae9e1ca9f8fc`. The C6 catalog contained 91 `ToolSpec` entries. C6 adds five passive watch kinds, `coordination.sync_integration`, recomputed `swarm.overlap.check`, manager-admitted async workspace lease / exact Task claim / `agent.open`, and the bounded local TaskSubmission-intake consumer. C7 at `2f00c2b3d7862788ca8a6bead4645d3ce64dc0ea` adds Participant credential issuance at launch admission with atomic private assignment context and held database lease, partial authenticated configured/connect readback, and a durable bounded review-disposition consumer with exact manager-on-behalf authority and semantic duplicate/gap handling. The lease/release lifecycle is database-only. Frozen-source stale-fence and unchanged-path privacy, authority, disposition and lifecycle reviews passed on `588a5ca21fbcbd5434540c011846535af7647f35`; `feedback_audit` passed exact review of all three Windows-delta files in the final commit (`.local/qualification/r7-build-gate/windows-probe-lifecycle-audit.md`). Plugin, installer and work-dispatch methods are authored; the launcher MCP facade, plugin adapter and work-dispatch modules are present but unwired and uncompiled. Productive dispatch and native tool/model capability proof remain absent. C7 formatting and Clippy passed, and the corrected targeted Windows `check_probe` test passed one test; full CI is pending. The earlier Windows fixture omitted the production outer `CREATE_NO_WINDOW` setting; a matching model-free diagnostic passed but observed no live PID, so the exact OS/root cause is unconfirmed. No overall green gate or full-cycle qualification is established. Actual native-MCP harness loading remains unknown, so a registry entry, configured/connect readback or `tools/list` result does not prove that a model loaded or used a schema.

The [Canonical MCP Surfaces](mcp-canonical-surfaces-and-topologies.md) document owns exact public names. This document owns group metadata and deferred-loading design. Remaining end-to-end gaps include actual native harness consumption/qualification, productive dispatch beyond the lease/claim/`agent.open` admission, watch predicates without authoritative sources, broader integration/scope-Git, Concilium, cron, Goal and native-Rust program paths. Do not replace the application authorization layer with catalog metadata.

## 2. Evidence-based design constraints

### 2.1 OpenAI tool search

Current OpenAI guidance recommends:

- deferred tool definitions for large catalogs;
- namespaces or MCP servers rather than many unrelated top-level functions;
- clear high-level namespace/server descriptions;
- fewer than ten functions per namespace where practical;
- client-executed search when availability depends on project or tenant state;
- loaded tools appended later so the prompt cache remains useful.

ELIOT therefore groups tools by domain and keeps group descriptions precise enough for search.

### 2.2 GitHub Copilot CLI

Current Copilot CLI documentation reports two concrete failure pressures:

- a few dozen definitions may consume roughly 10–20K context tokens before work starts;
- after several dozen visible tools, wrong-tool selection becomes more likely.

Its production strategy is useful:

- a small always-loaded built-in set;
- external/MCP tools deferred automatically when the catalog is large;
- `deferTools: "never"` only for a server used constantly;
- custom agents may list tools while still opting into deferred loading;
- names/descriptions/parameter descriptions are the search index.

ELIOT should follow that shape without inheriting Copilot's authority model.

### 2.3 MCP protocol

The MCP tools contract already supplies the protocol mechanisms needed here:

- paged `tools/list` with opaque cursor;
- deterministic tool definitions with JSON Schema;
- optional output schema and tool annotations;
- `notifications/tools/list_changed` when the available list actually changes.

Annotations are descriptive hints, not enforcement. ELIOT profile/application checks remain the security boundary.

### 2.4 Claude Code Agent Teams and field reports

Agent Teams demonstrates that direct coordination tools and shared-task tools can be attached to a role-specific teammate. It also demonstrates why launch-time capability validation is mandatory:

- different teammate backends may receive different tool sets;
- plugin/custom-agent definitions have historically dropped tools or instructions on some paths;
- an agent can complete work but be unable to report because `SendMessage`/tool discovery is unavailable;
- large tool catalogs can defer core tools incorrectly if the registry is wrong.

ELIOT must fail loudly or mark a participant `relay_only`; it must not tell an agent to call a missing tool.

### 2.5 Donor registry patterns

Useful donor patterns:

- **Paseo:** named profiles package provider, model, mode, thinking/features and machine-readable `When to use` notes.
- **Poracode:** one registry is assembled from self-contained tool domains such as threads, projects, workspaces, files, agents, Git, GitHub, schedules, settings, MCP servers and skills.
- **GitHub Copilot custom agents:** tools can be selected by exact name, server namespace or tool alias.
- **MCP gateway profiles:** server-side allowlists are still required even when a client can hide tools locally.

ELIOT adopts the domain registry and role/profile ideas. It does not adopt one giant always-loaded instruction describing every tool.

## 3. Registry model

Replace the minimal `ToolSpec` with metadata sufficient for authorization, search, rendering and qualification.

Recommended internal shape:

```rust
struct ToolSpec {
    method: &'static str,
    title: &'static str,
    description: &'static str,
    fields: &'static [Field],
    required: &'static [&'static str],
    output_schema: Option<fn() -> Arc<JsonObject>>,

    group: ToolGroup,
    audience: ToolAudience,
    load_tier: LoadTier,
    risk: ToolRisk,

    when_to_use: &'static str,
    when_not_to_use: &'static str,
    search_terms: &'static [&'static str],
    required_context: &'static [&'static str],
}
```

Suggested enums:

```rust
enum LoadTier {
    Core,          // normally eager for the named surface
    Searchable,    // authorized but deferred
    ManualOnly,    // shown only after an explicit exact request/selection
}

enum ToolRisk {
    ReadLocal,
    ReadOpenWorld,
    ReversibleMutation,
    RuntimeEffect,
    IrreversibleEffect,
    AuthorityChange,
}
```

`ToolAudience` and `ToolGroup` are descriptive/search metadata. Hard permission still comes from `McpToolProfile` plus application authorization.

### 3.1 Required metadata rules

Every tool must have:

- one stable domain group;
- one concise purpose-first description;
- concrete `when_to_use` and `when_not_to_use` guidance;
- exact input schema with `additionalProperties: false`;
- structured output schema for stable results;
- read-only/destructive/idempotent/open-world hints where truthful;
- a named authority/risk class;
- a deterministic search-term set;
- a declared result size/bounding policy.

A new public method is not exposed through restricted profiles or deferred search until this metadata and its allowlists are reviewed.

## 4. Tool groups

Groups are search and presentation units, not new authorization roles. Keep each group at approximately 4–9 model-facing tools. Split low-level groups rather than creating a 25-tool namespace.

## 4.1 `core`

Common durable control-plane reads used by almost every role:

```text
swarm.context.get
swarm.tools.search
operation.get
```

`swarm.context.get` is role-sensitive and bounded. It returns the caller's current assignment/dashboard neighborhood rather than the whole fleet.

`swarm.tools.search` searches only methods already allowed by the hard profile. Native tool-search clients may not need to call it directly, but it remains the compatibility affordance for harnesses without native search.

## 4.2 `participant-coordination`

Ordinary agent collaboration:

```text
coordination.send
coordination.inbox
coordination.consult
coordination.sync_integration
coordination.watch.create/list/cancel
swarm.overlap.check
```

The high-level operations are preferred over raw mailbox calls. Raw `message.*` remains available in a deferred diagnostic group where authorized.

## 4.3 `assignment-read`

Task and immutable result inspection:

```text
task.get
task.list
attempt.get
task.submission
task.acceptance
operation.list
artifact.get
```

Participant results are automatically scoped to their work context. A role does not obtain a global Task inventory merely by loading this group.

## 4.4 `manager-core`

The manager's normal control loop:

```text
swarm.dashboard
swarm.queue.get
swarm.launch.preview
swarm.launch
swarm.agent.inspect
swarm.agent.steer
swarm.exceptions.get
operation.get
swarm.tools.search
```

These are high-level application methods. They do not bypass the existing Task, Attempt, Agent or Operation authority.

## 4.5 `runtime-control`

Lower-level exact runtime operations. `agent.open` is also admitted in the manager core; the asynchronous Task/workspace admission does not claim productive native dispatch is complete.

```text
agent.open
agent.send
agent.reply
agent.configure
agent.goal
agent.background
```

## 4.6 `runtime-recovery`

Rare recovery/readback operations:

```text
agent.refresh
agent.reconcile
agent.recover
agent.result
```

These should be loaded by an operator/manager only when a specific retained operation or binding requires them.

## 4.7 `monitoring`

Fleet and host observation:

```text
host.status
route.list
report.attention
report.capacity
report.delta
agent.state
agent.list
agent.family
```

Normal UI/manager prompts use `swarm.dashboard`; this group is for drill-down and diagnostics.

## 4.8 `git-read`

Bounded read-only repository awareness:

```text
git.worktree.inspect
git.changed_paths
git.overlap
git.history
git.blame.summary
git.who_works_here
```

Git history and blame are provenance only. Current ownership comes from ELIOT Task/Attempt/scope records.

## 4.9 `task-management`

Manager-owned work lifecycle:

```text
task.create
task.revise
task.claim
task.dispatch
task.submit
attempt.bind_producer
attempt.release
operation.cancel
```

## 4.10 `review`

Reviewer evidence and findings:

```text
swarm.review.context
task.submission
check.get
check.profiles
artifact.get
artifact.read
artifact.parts
review.submit
review.get
operation.get
```

## 4.11 `acceptance-effects`

Protected and normally manual-only:

```text
source.capture
check.run
check.cancel
artifact.assemble
task.accept
task.invalidate_acceptance
forge.publish_ref
```

These methods are never loaded for ordinary participants. Publication/acceptance remains separate from implementation.

## 4.12 `administration`

Rare authority/configuration methods:

```text
client.list
client.register
host.mode
gm.handover
```

GM authorization does not make this group eager. It is explicit/manual-only.

## 4.13 `schedules`

When schedule application methods are exposed through MCP, keep them in a separate manager/GM group:

```text
schedule.list
schedule.get
schedule.create
schedule.update
schedule.run
schedule.disable
schedule.delete
```

Schedules create future work and must not be confused with an agent reminder/freshness watch.

## 4.14 `mailbox-raw`

Low-level delivery diagnostics:

```text
message.read
message.send
message.cancel
```

Normal agents use typed coordination operations. Raw mailbox methods are retained for compatibility and exact delivery troubleshooting.

## 5. Role-specific eager surfaces

The hard profile defines the maximum set. The eager surface defines the small initial set.

## 5.1 Participant

Recommended eager tools:

```text
swarm.context.get
swarm.tools.search
coordination.send
coordination.inbox
coordination.consult
coordination.sync_integration
coordination.watch.create/list/cancel
swarm.overlap.check
operation.get
```

Searchable groups:

```text
participant-coordination detail
assignment-read
git-read
review reads explicitly sponsored for this participant
mailbox-raw
```

The participant never sees manager runtime, acceptance, admin or publication tools.

## 5.2 Manager

Recommended eager tools:

```text
swarm.dashboard
swarm.queue.get
swarm.launch.preview
swarm.launch
agent.open
swarm.agent.inspect
swarm.agent.steer
swarm.exceptions.get
operation.get
swarm.tools.search
```

Searchable groups:

```text
monitoring
task-management
runtime-control
runtime-recovery
git-read
participant-coordination
review
schedules
mailbox-raw
```

## 5.3 Assigned reviewer

Recommended eager tools:

```text
swarm.review.context
swarm.tools.search
task.submission
artifact.read
check.get
review.submit
review.get
operation.get
```

The canonical assigned reviewer can submit only its exact assigned result; after Task revision or Attempt release, only exact-assignment `review.get`, linked `operation.get` and `review.submit` remain available while authenticated and unrevoked. Historical context/list, artifact and evidence reads are not implied. `task.request_changes` is manager disposition. A legacy profile explicitly named `Reviewer` may retain old compatibility behavior separately.

## 5.4 GM/operator

Use the manager eager surface. Keep `acceptance-effects` and `administration` deferred/manual-only. High authority is a reason to reduce accidental visibility, not to preload more tools.

## 5.5 Observer

Recommended eager tools:

```text
swarm.dashboard
swarm.context.get
operation.get
swarm.tools.search
```

All results remain bounded and application-authorized.

## 6. Loading strategies by client capability

One catalog must support several harnesses without lying about their capabilities.

## 6.1 Native tool-search clients

Preferred path for OpenAI Responses/Agents and current Copilot-style clients:

```text
small ELIOT core server/surface       eager
role/domain surfaces                 deferred/searchable
```

Configuration principles:

- mark non-core MCP surfaces deferred;
- use short clear server/namespace descriptions;
- keep domain groups below roughly ten tools;
- use exact allowlists in addition to deferment;
- preserve loaded tools for the rest of the session where the client does so;
- measure search misses, wrong loads, added latency and context savings.

For OpenAI Responses, an MCP server can be marked `defer_loading: true`. A core surface used on most turns remains non-deferred. Never put a remote endpoint, credential or private installation value in repository configuration examples.

## 6.2 Clients with client-side `ToolSearch`

The launcher supplies:

```text
verified core tools
searchable authorized inventory
assignment-specific search hints
```

After launch, verify the actual visible/deferred inventory. Do not trust only an agent definition or plugin declaration.

If a required core tool is absent:

```text
fail launch before productive work
or register the participant as relay_only/pull_only with the exact gap
```

Do not let an agent work for an hour and discover at completion that it cannot report.

## 6.3 MCP clients without tool search

Fallback order:

1. launch with the small role core plus one assignment-relevant domain group;
2. let `swarm.tools.search` return matching authorized tools/groups and activation disposition;
3. if the harness supports a safe tool-list refresh, activate at the next safe boundary and emit `notifications/tools/list_changed`;
4. otherwise return `new_mcp_session_required` and let the launcher replace only the MCP child/profile view, not the host or native work;
5. never silently expose the full profile as fallback.

Suggested search result:

```json
{
  "catalog_revision": "sha256:...",
  "matches": [
    {
      "group": "git-read",
      "tools": ["git.who_works_here", "git.overlap"],
      "why": "current task mentions an overlapping symbol",
      "activation": "already_loaded | auto_activatable | new_mcp_session_required | forbidden"
    }
  ],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

Tool loading is session presentation state. It does not create Task work and does not need a second durable work ledger.

## 7. MCP protocol behavior

## 7.1 Deterministic paged `tools/list`

Current one-page behavior must be replaced before the catalog becomes large.

Requirements:

- deterministic order: group, load tier, method;
- opaque cursor;
- bounded page item and serialized-byte limits;
- stable page for one catalog revision;
- invalid/stale cursor returns an explicit protocol error;
- profile and selected surface are applied before pagination;
- no hidden method count or name leaks to unauthorized profiles.

## 7.2 `notifications/tools/list_changed`

Send only when this session's visible schema set actually changes:

```text
surface/group activation or deactivation
profile/capability revision requiring a new view
server upgrade changing an exposed schema
```

Do not send for:

```text
agent liveness
queue movement
message arrival
Task state change
runtime availability
```

Those are application facts and use ELIOT subscriptions/reads.

A client that ignores list-changed notifications remains safe: calls are still checked at dispatch, and the fallback is a new MCP child/session.

## 7.3 Stable catalog revision

Expose a digest over visible tool names, schemas and metadata:

```text
catalog_revision
profile
surface
core_tools
deferred_groups
```

The launcher records this in the assignment packet and capability receipt. A later mismatch is visible rather than silently changing the agent's tool contract.

## 8. Search quality and names

Tool search is only as good as the names and descriptions.

Rules:

- use verb/object names that match likely requests;
- describe the concrete result, not the internal implementation;
- include common search vocabulary in `search_terms` rather than stuffing prose into the system prompt;
- distinguish inspect/read from launch/mutate;
- distinguish message from steer and work assignment;
- avoid multiple aliases for the same semantic operation;
- describe when **not** to use a low-level method;
- place exact parameter descriptions in the deferred schema.

Examples:

```text
swarm.overlap.check
  good: "Find current ELIOT owners and uncommitted Git overlap for a path, symbol or contract before editing."

agent.send
  good: "Manager-only exact native input/steer. Do not use for peer coordination mail."

coordination.consult
  good: "Read the current contract card first; if the fact is absent, send one bounded question to the exact current owner."
```

## 9. Tool results

Every high-level read returns compact structured content first:

```text
status / next_action
exact IDs and revisions
freshness
coverage and gaps
bounded items
cursor or drill-down references
```

Do not return a verbose narrative plus raw dump by default.

Recommended common envelope:

```json
{
  "status": "ok | partial | stale | conflict | unavailable",
  "revision": "...",
  "summary": {},
  "items": [],
  "next_actions": [],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

Large logs, transcripts, diffs and evidence remain paged artifacts/resources.

## 10. Runtime capability verification

At participant/manager launch, record what the runtime actually exposes:

```json
{
  "profile": "participant",
  "surface": "participant-core",
  "catalog_revision": "sha256:...",
  "core": {
    "swarm.context.get": "available",
    "coordination.consult": "available",
    "coordination.send": "available",
    "operation.get": "available"
  },
  "tool_search": "native | launcher_assisted | unsupported",
  "list_changed": true,
  "presentation": "pull_only | safe_boundary",
  "gaps": []
}
```

States:

```text
ready
ready_with_gaps
relay_only
incompatible
```

Do not infer readiness from registration alone. A model/runtime that cannot see a required core tool is not fully ready even if its process is running.

## 11. Security boundary

### 11.1 Authorization before discovery

The order is:

```text
credential/profile binding
  -> hard profile allowlist
  -> object/work-context authorization
  -> selected surface/load tier
  -> tools/list/search result
  -> tools/call pre-dispatch check
  -> application authorization again
```

Tool search never sees denied tools.

### 11.2 Annotations are not enforcement

Set MCP hints accurately, but do not rely on them for:

- authorization;
- sandboxing;
- idempotency;
- external-effect safety;
- prompt-injection resistance.

The application method and external-effect policy remain authoritative.

### 11.3 No authority by tool activation

Loading a schema cannot:

- change the client's ELIOT role;
- add Task ownership;
- make a participant a manager;
- permit a new repository/project;
- authorize merge/publication;
- bypass GM epoch;
- expose a secret.

## 12. Source-level implementation plan

### M1 — catalog metadata refactor

Files:

```text
src/mcp.rs
src/mcp/catalog.rs             new
src/mcp/groups.rs              new
src/mcp/profiles.rs
src/mcp/profiles_tests.rs
src/config.rs
docs/mcp-profiles.md
```

Work:

- move the static registry out of the facade implementation;
- add group/audience/load/risk/search metadata;
- keep exact one-tool/one-application-method mapping;
- preserve current profile allowlists and request-ID rules;
- add planned `Participant` profile only with its exact application role implementation.

### M2 — deterministic paged discovery

- implement `PaginatedRequestParams.cursor`;
- deterministic ordering and catalog revision;
- page item/byte limits;
- profile filtering before pagination;
- optional list-changed support;
- no IPC connection merely to list static schemas.

### M3 — role surfaces

Configuration extension:

```toml
[mcp.profiles.agent]
tool_profile = "participant"
expected_client_id = "participant-example"
surface = "participant-core"
deferred_groups = ["assignment-read", "git-read", "mailbox-raw"]

[mcp.profiles.manager]
tool_profile = "manager"
expected_client_id = "manager-example"
surface = "manager-core"
deferred_groups = ["monitoring", "task-management", "runtime-control", "runtime-recovery", "git-read", "review"]
```

Repository examples use placeholders. Secrets remain in local credential files.

### M4 — high-level application methods

Implement high-level reads/mutations in the application layer first:

```text
swarm.context.get
swarm.tools.search
swarm.dashboard
swarm.queue.get
swarm.launch.preview
swarm.launch
swarm.agent.inspect
swarm.agent.steer
swarm.exceptions.get
swarm.overlap.check
swarm.review.context
```

The MCP facade then exposes them one-to-one. It must not assemble authoritative state from ad hoc facade-side reads.

### M5 — launcher capability receipt

The Swarm Launcher records and validates the actual core/deferred capability profile before productive work. See [Swarm Launcher Assignment Context](swarm-launcher-assignment-context.md).

## 13. Acceptance cases

1. Participant starts with no manager/admin/acceptance tool definitions.
2. Manual call of a hidden method returns method-not-found before IPC.
3. Tool search for “merge/publish” under Participant reveals no forbidden method names.
4. Manager core is at most nine eager ELIOT tools.
5. One assignment requiring Git overlap can discover/load `git-read` without loading acceptance/admin groups.
6. A client without native tool search gets an explicit activation disposition, never full-profile fallback.
7. `tools/list` pagination is deterministic across repeated calls at one catalog revision.
8. Catalog/profile change produces one list-changed notification; Task/liveness changes produce none.
9. Missing required core tool makes launch `ready_with_gaps`, `relay_only` or `incompatible`, never silently ready.
10. Tool annotations disagreeing with hard policy cannot widen authority.
11. Same tool catalog/profile produces the same digest after restart.
12. Large catalog qualification measures prompt tokens, search hit rate, wrong-tool rate and latency against eager loading.
13. A few dozen deferred tools do not enter the initial model context on supported clients.
14. No real domain, token, account, local private path or credential appears in any catalog/schema/result.

## 14. Final rule

```text
Profiles decide permission.
Surfaces decide what is immediately visible.
Tool search decides what is loaded later.
The application remains the authority.
```

The common tools should make correct work easy. The full catalog should be available when useful, but never dumped into every agent's context and never confused with permission.
