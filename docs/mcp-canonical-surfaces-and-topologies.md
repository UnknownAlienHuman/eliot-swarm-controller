# ELIOT MCP Canonical Surfaces and Client Topologies
## Exact public names, role cores and deployment patterns for OpenAI, Copilot, Claude and fallback clients

**Revision:** 1 — 2026-10-03  
**Source baseline:** `main` at `35e499ae73b622d873c44873f6993ee3fcbea87b`  
**Applies to:** [MCP Tool Catalog and Deferred Loading](mcp-tool-catalog-and-loading.md), [Swarm Launcher and Assignment Context](swarm-launcher-assignment-context.md), [Communication Program](agent-communication-program.md)  
**Status:** normative naming/topology amendment. It does not claim these methods are implemented.  
**Precedence:** this file governs canonical convenience-tool names and the mapping from one logical catalog to client-specific MCP deployments. Older `coordination.context.get`, eager `coordination.ask_owner`, `coordination.notify_when_available`, global-roster and one-monolithic-eager-server examples are superseded.

## 0. Decision

ELIOT has one logical application/catalog authority, but it may present that catalog through several client-specific MCP layouts.

```text
one application method registry
one hard profile decision
one catalog revision
        │
        ├── OpenAI Responses: eager core + deferred logical group servers/namespaces
        ├── OpenAI Agents: role-filtered MCP + automatic discovery where supported
        ├── Copilot CLI: core server deferTools=never + deferred catalog/group servers
        ├── Claude Code: verified core coordination server + deferred optional domains
        └── simple MCP client: generated fixed role surface; reconnect to change surface
```

No layout changes Task, role, project or method authority.

## 1. Canonical public convenience tools

The ordinary model-facing surface uses the names below. Lower-level methods remain deferred implementation/detail tools and must not compete with the convenience name in the eager catalog.

## 1.1 Common core

```text
swarm.context.get
swarm.tools.search
operation.get
```

## 1.2 Participant core

```text
swarm.context.get
swarm.tools.search
coordination.send
coordination.inbox
coordination.consult
coordination.sync_integration
coordination.watch.create
swarm.overlap.check
operation.get
```

## 1.3 Manager core

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

## 1.4 Reviewer core

```text
swarm.review.context
swarm.tools.search
task.submission
artifact.read
check.get
task.request_changes
operation.get
```

## 1.5 Observer core

```text
swarm.dashboard
swarm.context.get
swarm.tools.search
operation.get
```

Results remain profile/application filtered. Observer dashboard is read-only and bounded.

## 2. Canonical versus lower-level names

| Canonical model-facing tool | Lower-level/deferred implementation methods | Decision |
|---|---|---|
| `swarm.context.get` | old `coordination.context.get`, Task/Attempt/card list reads | only `swarm.context.get` is eager/public convenience name |
| `coordination.consult` | `coordination.ask_owner`, peer find, card field lookup, addressed ask | `ask_owner` is a lower-level exact step, not a competing eager alias |
| `coordination.watch.create/list/cancel` | old `coordination.notify_when_available` | watches are the canonical general mechanism; notify-when-available is a watch kind, not another public concept |
| `coordination.send` | `coordination.message.send`, `message.send` | typed coordination send is ordinary UX; raw mailbox remains diagnostic/compatibility |
| `swarm.overlap.check` | scope inspect/conflicts, `git.who_works_here`, Git changed paths/history | one convenience read returns labelled ownership/worktree/provenance sources |
| `swarm.agent.steer` | `agent.send` with exact binding/generation/turn | manager convenience wrapper; peer mail never uses it |
| `swarm.agent.inspect` | `agent.state/list/family`, operation/report reads | one bounded current work/runtime/capability view |
| `swarm.launch` | Task claim, workspace preparation, `agent.open`, Participant registration and runtime start Operations | high-level linked admission; lower-level authorities remain intact |
| `swarm.tools.search` | client-native tool search, catalog lookup, surface activation | compatibility method for harnesses without native search; denied tools remain invisible |

Do not create multiple public aliases merely to support prompt vocabulary. Put synonyms in `search_terms`.

## 3. Logical tool groups

Canonical groups:

```text
core
participant-coordination
assignment-read
manager-core
runtime-control
runtime-recovery
monitoring
git-read
task-management
review
acceptance-effects
administration
schedules
mailbox-raw
```

Each group has:

```text
group ID
title
short discriminative description
hard-profile eligibility
core/searchable/manual load tier
4–9 model-facing methods where practical
catalog revision
deterministic search terms
```

A logical group is not necessarily a separate process. It may be one filtered view of the same local MCP facade.

## 4. Why one monolithic server is insufficient

Different clients defer at different layers:

- OpenAI Responses can defer an MCP server definition; loading that server imports its visible tool list.
- OpenAI client-executed tool search can instead load selected functions/namespaces under application control.
- Copilot CLI can defer MCP tools and mark a frequently used server `deferTools: "never"`.
- some Claude/custom-agent paths dynamically search tools but have had backend-specific missing-tool behavior;
- simple MCP clients may only list tools at initialization and ignore list-changed notifications.

Therefore:

```text
one eager remote MCP containing every authorized method
```

is not the default. It defeats the context-saving goal on clients that load a whole MCP server at once.

## 5. Topology A — OpenAI Responses API

### 5.1 Preferred for a known role/session

```text
eliot-core          eager, 3–9 tools
eliot-participant   deferred when participant needs detail
eliot-manager       deferred manager detail
eliot-git           deferred
eliot-review        deferred
eliot-runtime       deferred manager-only
eliot-effects       deferred/manual GM-only
```

These are **logical MCP server views** of the same controller/application, not independent authorities or databases.

Repository examples use names and placeholder endpoints only. Local deployment binds each logical view to an exact credential/profile/surface.

### 5.2 Alternative: client-executed tool search

Where the application constructs Responses requests, expose:

```text
small eager ELIOT core functions/namespace
client-executed tool_search
trusted catalog lookup filtered by profile/project/work context
selected additional tools/namespaces returned by ELIOT
```

This gives the finest group/tool selection and is preferred when available tools depend on the current Task, project or participant grant.

### 5.3 Never

- never return a schema outside the hard profile;
- never use a generic JSON-RPC passthrough as one deferred tool;
- never place a private endpoint/token in repository config;
- never interpret loaded tool schemas as approval for effects.

## 6. Topology B — OpenAI Agents API

Current Agents API can automatically discover/defer supported MCP tools. Use:

```text
one role-filtered ELIOT MCP connection
small explicit eager high-level functions only when actually used on most turns
automatic MCP discovery for searchable groups
launcher-recorded actual capability receipt
```

The connection is still bound to one hard profile and expected client identity.

Qualification must verify the exact model/provider path; do not assume every configured model performs automatic MCP search identically.

## 7. Topology C — GitHub Copilot CLI/custom agents

Recommended layout:

```text
eliot-core MCP        deferTools: "never"
eliot-catalog MCP     deferTools: "auto"
optional domain MCPs  auto unless used constantly
custom agent tools    exact role subset or namespace patterns
deferred-tool-loading true for a large named subset
```

The custom agent definition narrows availability; ELIOT server-side profile/application checks remain authoritative.

Launch validation checks that core coordination/reporting tools are actually present in the custom agent's visible or searchable inventory. If not, fail or mark `relay_only`.

## 8. Topology D — Claude Code and Agent Teams

Recommended layout:

```text
project/local ELIOT core MCP with participant/manager credential
core coordination/reporting tools explicitly available to the role
optional heavy domains deferred/discoverable where the runtime supports it
one capability probe/receipt after teammate/session launch
```

Do not rely solely on plugin/custom-agent frontmatter. Field reports show differences between in-process and separate-process teammates and cases where definitions or coordination tools were silently absent.

Required launch outcome:

```text
ready
ready_with_gaps
relay_only
incompatible
```

An agent without a working result-delivery path is not `ready`.

Peer `SendMessage`-style communication remains information only. It cannot grant user authority, spawn peers or start an idle ELIOT participant model.

## 9. Topology E — client without dynamic tool search

Generate a fixed MCP surface at connection time:

```text
role core
+ at most one assignment-relevant optional group
```

`swarm.tools.search` may return matches and one of:

```text
already_loaded
auto_activatable
reconnect_surface_required
forbidden
unsupported
```

If the client supports safe tool-list refresh:

1. activate selected group in session presentation state;
2. emit `notifications/tools/list_changed`;
3. require client to relist;
4. record visible catalog revision.

If not, the launcher reconnects/replaces only the MCP client surface at a safe boundary. It does not restart the native work/session and never silently widens to the full profile.

## 10. Server/process layout

Logical group views should normally share one local ELIOT host/facade process:

```text
one Store/application authority
one local IPC endpoint
one or a small bounded set of MCP facade processes
many authenticated logical profile/surface sessions
```

Avoid one heavy MCP child process per registered participant. Registered inactive participants consume no process or dedicated Tokio task.

For remote access:

```text
Cloudflare/other tunnel = transport only
Agent Gateway/MCP facade = authentication/profile/surface filter
ELIOT application = object/work authorization
```

Local trusted coding agents use local transport; they do not need to traverse the remote tunnel.

## 11. Catalog search contract

Search input:

```json
{
  "query": "find who is changing this Rust symbol",
  "purpose": "implementation | review | diagnosis | recovery",
  "task_id": "optional exact current Task",
  "loaded_catalog_revision": "sha256:...",
  "max_results": 5
}
```

Search result:

```json
{
  "catalog_revision": "sha256:...",
  "matches": [
    {
      "group": "git-read",
      "tool": "swarm.overlap.check",
      "title": "Check current ownership and Git overlap",
      "why": ["symbol_overlap", "current_task_relation"],
      "activation": "already_loaded | auto_activatable | reconnect_surface_required"
    }
  ],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

Search is deterministic for the same catalog/profile/work-context revision. It never returns denied names or counts.

## 12. Surface activation

Activation changes session presentation only.

It may:

- make an authorized schema visible/callable in this MCP session;
- update loaded catalog revision;
- emit one list-changed notification;
- append a capability-receipt fact.

It may not:

- change role/profile;
- add Task/Attempt ownership;
- grant another project/repository;
- start a model turn;
- create work;
- authorize a network/merge/publication effect.

## 13. Configuration examples

### 13.1 ELIOT local profiles

```toml
[mcp.profiles.participant_local]
tool_profile = "participant"
expected_client_id = "participant-example"
surface = "participant-core"
deferred_groups = ["assignment-read", "git-read", "mailbox-raw"]

[mcp.profiles.manager_local]
tool_profile = "manager"
expected_client_id = "manager-example"
surface = "manager-core"
deferred_groups = [
  "monitoring",
  "task-management",
  "runtime-control",
  "runtime-recovery",
  "git-read",
  "review"
]
```

These fields are planned extensions to current profile config.

### 13.2 Remote placeholder

```json
{
  "type": "mcp",
  "server_label": "eliot_git",
  "server_url": "https://YOUR_DOMAIN/eliot/git/mcp",
  "defer_loading": true,
  "allowed_tools": ["swarm_overlap_check"]
}
```

Real domains/tokens remain local. A remote URL does not select a privileged profile by itself.

## 14. Qualification matrix

For every supported client/runtime verify:

```text
hard profile non-disclosure
initial eager tool count/schema tokens
catalog search hit/miss/wrong-tool rate
first-load latency and later-turn cache behavior
list pagination
list_changed or reconnect behavior
missing core tool detection
manual hidden method rejection before IPC
same catalog revision after restart
result/report delivery path
MCP child/process cleanup
no model wake from activation/watch/message
```

Client-specific scenarios:

```text
OpenAI Responses: group MCP loading versus client-executed search
OpenAI Agents: automatic MCP discovery and exact loaded inventory
Copilot: deferTools auto/never and custom-agent deferred-tool-loading
Claude: in-process/separate-process custom teammate capability parity
fallback: fixed surface and safe reconnect
```

## 15. Final invariant

```text
One logical catalog.
Several client-compatible presentations.
One hard authority boundary.
No eager mega-server by default.
No missing core tool hidden behind a successful process launch.
```

The agent gets a small reliable toolbox immediately and can reach the rest without asking Root, while denied or dangerous capabilities remain undiscoverable and uncallable.