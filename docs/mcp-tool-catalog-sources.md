# ELIOT MCP Catalog and Launcher — Source Map
## Official protocol guidance, donor source patterns and user-reported failures

**Revision:** 1 — 2026-10-03  
**Companions:** [MCP Tool Catalog and Deferred Loading](mcp-tool-catalog-and-loading.md), [Swarm Launcher and Assignment Context](swarm-launcher-assignment-context.md)  
**Purpose:** preserve evidence behind the role surfaces, tool groups, deferred loading, launcher packet and capability checks. This file is evidence, not product authority.

## 1. Evidence labels

```text
SOURCE      inspected source path and behavior
OFFICIAL    current official protocol/vendor documentation
ISSUE       concrete user report or reproduction
OWNER       operating evidence supplied by the ELIOT owner
INFERENCE   ELIOT conclusion from several sources
UNVERIFIED  plausible behavior not source/live qualified here
```

Rules:

- an available tool is not an authorized tool;
- a configured tool is not necessarily visible in the runtime that launched;
- a listed tool is not proof the model can invoke it successfully;
- tool deferral is context optimization, not a security boundary;
- a user issue proves a failure happened, not its population frequency;
- a feature on donor `main` is not silently attributed to a stable release.

## 2. Current ELIOT source baseline

### Sources

- SOURCE: [`src/mcp.rs`](../src/mcp.rs)
- SOURCE: [`src/mcp/profiles.rs`](../src/mcp/profiles.rs)
- SOURCE: [`src/mcp/profiles_tests.rs`](../src/mcp/profiles_tests.rs)
- SOURCE: [`src/config.rs`](../src/config.rs)
- SOURCE/DOC: [`docs/mcp-profiles.md`](mcp-profiles.md)
- SOURCE/DOC: [`docs/legacy-swarm-transition.md`](legacy-swarm-transition.md)

### Current properties on `main` `35e499ae73b622d873c44873f6993ee3fcbea87b`

```text
one typed MCP tool per public application method
no generic method passthrough
no shell tool
closed profiles: observer / reviewer / manager / gm / full
profile-bound expected client ID
profile filter on tools/list
independent profile filter on tools/call before IPC
application authorization still authoritative
caller-owned request ID required for restricted-profile mutation
Operations remain MCP Tasks authority
bounded subscriptions remain freshness hints
```

Current inventory is documented as 53 tools: 22 reads and 31 mutations.

### Current gap

The current profile mechanism is a good hard-permission layer but not yet a model-context strategy:

```text
all tools allowed by one profile are returned together
list_tools ignores pagination input
ToolSpec has no group / audience / load tier / risk / when-to-use metadata
manager and worker receive low-level method inventories rather than compact role affordances
no tool catalog search/activation contract
no launch-time receipt proving actual runtime-visible tools
```

ELIOT decision: retain the closed authorization layer and add role surfaces plus a searchable deferred catalog.

## 3. OpenAI tool search

### Sources

- OFFICIAL: [Tool search](https://developers.openai.com/api/docs/guides/tools-tool-search)
- OFFICIAL: [API deployment checklist](https://developers.openai.com/api/docs/guides/deployment-checklist)
- OFFICIAL: [Using tools](https://developers.openai.com/api/docs/guides/tools)
- OFFICIAL: [MCP servers](https://developers.openai.com/api/docs/guides/tools-connectors-mcp)

### Confirmed useful behavior

Tool search loads deferred definitions only when needed. Current guidance states:

- avoid loading a large full catalog in the initial context;
- group functions into namespaces or MCP servers where possible;
- use clear short namespace/server descriptions;
- aim for fewer than ten functions per namespace;
- use client-executed search when available tools depend on project, tenant, permission or another application-owned registry;
- discovered tools are appended later to preserve cache utility;
- eager loading remains appropriate for a small set used in most tasks;
- MCP servers in Responses may use `defer_loading: true`; Agents API can automatically defer supported MCP tools.

### ELIOT decision

Take:

```text
small eager role core
clear domain groups
fewer than ~10 model-facing methods per group
client-executed search over project/profile/current-work state
loaded-tool/catalog revision receipt
measure completion, input tokens and latency
```

Do not take:

```text
hosted search as permission authority
one remote tool inventory shared by all roles
silent full-catalog fallback on unsupported runtimes
repository-committed private endpoint or credential
```

ELIOT's search result is filtered by hard profile and application work context before any schema can be returned.

## 4. GitHub Copilot CLI tool search and custom agents

### Sources

- OFFICIAL: [Loading tools on demand with tool search](https://docs.github.com/en/copilot/concepts/agents/copilot-cli/tool-search)
- OFFICIAL: [Custom agents configuration](https://docs.github.com/en/copilot/reference/custom-agents-configuration)
- OFFICIAL: [Allowing and denying tool use](https://docs.github.com/en/copilot/how-tos/copilot-cli/use-copilot-cli/allowing-tools)
- OFFICIAL: [Custom agents and sub-agent orchestration](https://docs.github.com/en/copilot/how-tos/copilot-sdk/features/custom-agents)

### Confirmed useful behavior

Current GitHub documentation reports:

- a few dozen tool definitions may consume roughly 10–20K context tokens before useful work;
- after several dozen visible tools, wrong-tool selection becomes more likely;
- above an approximate inventory threshold, external tools can be held back and loaded by search;
- built-in/core tools remain immediately available;
- one MCP server can opt out with `deferTools: "never"` when its tools are constantly needed;
- custom agents may list exact tools, aliases or namespaced MCP subsets;
- a custom agent can opt its named tools back into deferred loading;
- names, descriptions and parameter descriptions form the search surface;
- available-tool filtering prevents wasting model interactions on tools that are guaranteed to be denied;
- specialist agents can own large-context tools so the parent/orchestrator context stays small.

### ELIOT decision

Take:

```text
role-specific exact allowlist
small core always loaded
explicit deferred groups
clear names/descriptions/search terms
specialist/deferred heavy-context tools
no model interaction for methods guaranteed forbidden
```

Improve:

```text
hard profile and application authorization remain server-side
unsupported search never falls back to full profile
catalog digest and capability receipt are durable facts
one high-level ELIOT tool replaces several low-level calls on common paths
```

## 5. MCP tool protocol

### Sources

- OFFICIAL: [MCP Tools specification](https://modelcontextprotocol.io/specification/2025-06-18/server/tools)
- OFFICIAL: [MCP schema reference](https://modelcontextprotocol.io/specification/2025-06-18/schema)

### Confirmed mechanisms

MCP already defines the primitives needed for a large catalog:

```text
tools/list with cursor pagination
tool name/title/description/inputSchema/outputSchema
read-only / destructive / idempotent / open-world annotations
notifications/tools/list_changed
```

Tool annotations are hints to clients/models and are not a security mechanism.

### ELIOT decision

- implement deterministic bounded paged `tools/list`;
- filter authorization and selected surface before pagination;
- send list-changed only when this session's visible schema set changes;
- keep Task/message/liveness updates in application subscriptions, not tool-list notifications;
- retain pre-dispatch and application authorization even when annotations say read-only;
- use structured result schemas and explicit coverage/gaps.

## 6. Claude Code Agent Teams and dynamic-tool field reports

### Official sources

- OFFICIAL: [Agent teams](https://code.claude.com/docs/en/agent-teams)
- OFFICIAL: [Cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging)

### Useful behavior

Agent Teams demonstrates:

- independent teammate contexts;
- direct peer messages;
- lead-owned task assignment;
- role/custom-agent-specific tools;
- compact teammate visibility and steering;
- explicit team/task tools attached to participating sessions.

Official guidance favors small teams for ordinary work and warns that coordination/token overhead rises with team size and shared mutable files.

### Relevant issues

- ISSUE: [#7328 — selective MCP tool filtering and tool groups](https://github.com/anthropics/claude-code/issues/7328)
- ISSUE: [#52004 — dynamic registry omitted core Glob/Grep tools](https://github.com/anthropics/claude-code/issues/52004)
- ISSUE: [#81185 — teammate completed work but lacked SendMessage/ToolSearch](https://github.com/anthropics/claude-code/issues/81185)
- ISSUE: [#83533 — custom teammate could not discover project MCP tools](https://github.com/anthropics/claude-code/issues/83533)
- ISSUE: [#98392 — agent definition/tool restrictions silently dropped on one backend](https://github.com/anthropics/claude-code/issues/98392)
- ISSUE: [#99111 — busy teammate saw correction only after turn completion](https://github.com/anthropics/claude-code/issues/99111)
- ISSUE: [#68110 — recursive subagent fan-out produced 48+ agents](https://github.com/anthropics/claude-code/issues/68110)
- ISSUE: [#66686 — roughly 70-agent review lost 26 agents without useful fleet visibility](https://github.com/anthropics/claude-code/issues/66686)
- ISSUE: [#1935 — orphaned MCP server processes accumulated](https://github.com/anthropics/claude-code/issues/1935)

### ELIOT decision

```text
verify actual runtime tool surface at launch
required reporting/coordination core missing -> fail or relay_only
stored delivery != presented to busy model
peer communication cannot recursively spawn agents
no per-session MCP subprocess when one controlled local facade suffices
no role/tool declaration silently widened or dropped
fleet UI shows capability gaps and process ownership
```

## 7. Paseo

### Sources

- SOURCE/DOC: [Paseo repository](https://github.com/getpaseo/paseo)
- SOURCE/DOC: repository `README.md`, daemon/client separation, CLI commands and managed worktree examples
- OWNER AUDIT: `Manager → Orchestrator → Executors`, revision 4, sections 5.1–5.5

### Useful patterns

Paseo packages agent execution as named profiles and a transport-neutral daemon/client protocol. Its profile design combines:

```text
provider
model
mode
thinking/features
human-readable notes / when-to-use guidance
```

Repository-level scripts/services and managed worktrees move setup out of model improvisation. Multiple clients use one daemon rather than each UI owning provider processes independently.

### ELIOT decision

Take:

- role/profile descriptions that explain when to use a runtime;
- daemon/application authority separate from UI;
- workspace creation before agent launch;
- one protocol across CLI/UI/MCP;
- scripts/services represented as typed configured capabilities.

Do not take:

- provider profile as Task/acceptance authority;
- unsandboxed plugin code as an implicit trust boundary;
- a follow-up input command as proof of exact current-turn steer for every provider;
- current `main` behavior as stable-release evidence.

## 8. Poracode

### Sources

- SOURCE: [`src/main/app-controls/mcp/toolRegistry.ts`](https://github.com/Porabuild/Poracode/blob/master/src/main/app-controls/mcp/toolRegistry.ts)
- SOURCE: [`src/main/app-controls/mcp/tools/types.ts`](https://github.com/Porabuild/Poracode/blob/master/src/main/app-controls/mcp/tools/types.ts)
- SOURCE/DOC: [Poracode repository](https://github.com/Porabuild/Poracode)

### Inspected pattern

Poracode assembles one MCP registry from self-contained domains:

```text
schedules
threads
projects
workspaces
settings
usage
search
app
files
agents
Git
GitHub
MCP servers
skills
```

A `ToolDomain` owns specifications and handlers. Its thread supervisor interface exposes typed operations for session input, queued steer, terminal reads, installed-agent inventory, project files, Git status/diff/worktree and more.

### ELIOT decision

Take:

- self-contained tool-domain modules;
- typed high-level app controls;
- one registry assembled from domains;
- compact thread/project/workspace/Git operations;
- separate human explanations for consequential actions.

Improve:

- do not expose every domain eagerly;
- avoid one enormous always-loaded MCP instruction paragraph;
- separate hard role authorization from UI/tool catalog;
- keep acceptance/publication/manual authority deferred;
- do not let two products own one physical provider session.

## 9. GitHub MCP/toolset UX

### Sources

- OFFICIAL: GitHub Copilot MCP configuration and custom-agent tool selection documentation listed above.
- ISSUE: Claude Code #7328 cites GitHub's toolsets as a working production UX for grouping and toggling related MCP tools.

### ELIOT decision

ELIOT groups are checked source metadata and role surfaces, not user-interface-only checkboxes. A UI may later enable/disable groups inside a hard profile, but:

```text
disabled group is hidden and rejected
adding a group never widens profile/application authority
repository settings cannot select credentials or GM authority
catalog/profile change has a revision/digest
```

## 10. Existing remote MCP gateway evidence

OWNER evidence records a current external MCP manifest using deferred loading and a server-side safe-tool allowlist. It also records the critical rule that real endpoint/domain, tunnel identity and credentials remain local and repository examples use placeholders.

ELIOT conclusion:

- remote clients get a distinct least-privilege profile;
- server-side filtering remains mandatory even if the client supports deferred tools;
- local stdio participants need not traverse the remote gateway;
- remote and local profiles share application semantics but not secrets or transport configuration;
- repository docs/config examples contain placeholders only.

## 11. Existing Swarm operating evidence

OWNER evidence from the current swarm reports:

- managers sending 260–360K input tokens per step;
- writers growing to 110–180K-token contexts;
- one Codex thread consuming about 1.28B input tokens;
- startup context containing many unrelated plugins/tools;
- long Issue comment histories and large queues repeatedly injected;
- duplicate work caused by stale queue snapshots;
- active-line status distorted by mixed parent/child sessions;
- one-worktree/one-manager and compact task projections improving control;
- direct exact steer and status requiring backend-specific semantics.

ELIOT requirements derived from this evidence:

```text
small role-specific eager tools
fresh bounded assignment neighborhood
no full queue/global roster in participant prompt
deferred low-level/rare groups
one manager-owned workspace/write lease
capability receipt per launched runtime
honest stored/presented/consumed delivery states
controller-owned queue and overlap projection
```

## 12. What is known to work best

Across official guidance, donor source and field reports, the most defensible configuration is:

```text
hard server-side role/profile allowlist
5–9 eager high-level tools per role
clear groups below roughly 10 model-facing tools
on-demand loading/search for lower-level and rare tools
one high-level common-path tool instead of many manual calls
exact launch packet and capability receipt
one manager/worktree/write lease
read-only peer/Git visibility
no peer model spawn or shared mutable-room workflow
protected review/acceptance outside writer authority
```

A completely eager full catalog remains useful only as an explicit local compatibility/debug surface, not the default manager or participant experience.

## 13. Qualification questions

The implementation must measure rather than assume:

```text
initial schema token cost by role
search/load success and wrong-tool rate
extra latency of first deferred lookup
catalog cache stability
core-tool absence detection
profile/surface bypass attempts
list pagination and list-changed compatibility by client
large catalog behavior on OpenAI, Claude, Copilot and fallback clients
MCP child/process cleanup
manager prompt size
assignment success with and without queue/overlap preview
```

No donor currently proves all of these for ELIOT's exact Rust/Windows/runtime mix.

## 14. Final evidence-based boundary

```text
authorization is server-side and fixed
presentation is small and role-specific
search is on demand and project-aware
launch verifies actual capability
workspace and assignment ownership are explicit
Git informs overlap but never assigns work
rare authority tools remain deferred even for powerful roles
```

This gives agents convenient access to a large capability catalog without dumping that catalog into every context or letting tool discovery become authority.