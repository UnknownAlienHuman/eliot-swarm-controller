# ELIOT MCP Canonical Surfaces and Client Topologies

**Current qualification status:** See [Implementation Status](implementation-status.md) for the current source and native qualification; this document remains authoritative for canonical MCP names and authorization contracts.

### Historical C7/C8 status snapshot
**Revision:** 12 — 2026-10-05
**Integration review:** published C5 snapshot `a0a931e` and [Agent Operations PR #23](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/23).
**Historical C7/C8 CI:** Exact runs and source commits are recorded in [Implementation Status](implementation-status.md).

All local model/inference execution remains deferred by owner. C10 provider authentication uses an explicit credential reference; `stored_unverified` is metadata only, not proof of key validity or consumption. Current native evidence is maintained in [Implementation Status](implementation-status.md).

This is the canonical convenience-tool list for the [Communication Program](agent-communication-program.md). [Catalog and Loading](mcp-tool-catalog-and-loading.md) owns registry metadata; [Launcher](swarm-launcher-assignment-context.md) owns work context. Older aliases and reviewer examples are corrected here rather than exposed as competing APIs.

## 1. One catalog, separate permission and presentation

```text
one application method registry
  -> hard profile and object authorization
  -> small role core and authorized deferred groups
  -> client-compatible presentation
```

Group loading never changes the caller's role, Task ownership, repository scope or GM authority. An unavailable/denied method stays rejected even if an old schema is cached. The manager may enable individual automations that exercise their existing rights; tool loading is not that enablement.

`Core`, `Searchable` and `ManualOnly` are catalog presentation tiers. `ManualOnly` means an explicitly selected schema, not a second rule forbidding the manager from automating an otherwise authorized action. Actions still use the shared application checks and the selected manager-owned definition.

## 2. Exact role cores

The list below applies as its application methods are implemented. Do not expose proposed names with missing handlers or silently broaden a current legacy profile.

### Participant

```text
swarm.context.get
swarm.tools.search
coordination.send
coordination.inbox
coordination.consult
coordination.sync_integration
coordination.watch.create
task.submit
artifact.read
swarm.overlap.check
operation.get
```

The ordinary assigned Participant may submit only the exact current Task revision and Attempt. This adds no Task creation, claim, acceptance, manager runtime, or GM recovery authority. Its `operation.get` is a narrow candidate-origin projection for applied `source.capture` and `agent.result` Operations in that current Attempt; sponsored review credentials remain review-only.

### Manager

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
agent.open
```

### Assigned reviewer

```text
swarm.review.context
swarm.tools.search
task.submission
artifact.read
check.get
review.submit
operation.get
```

`review.submit` records the assigned auditor's exact-slot verdict/evidence. `review.get` and linked `operation.get` read only that authenticated reviewer's exact retained assignment/result; they may complete after Task revision or Attempt release while the credential remains valid and unrevoked. For this historical exception, no context/list, artifact or evidence reads are implied; the current assignment's ordinary review context is separately scope-checked. A pending `review_scope.review_assignment_id: null` becomes usable only after the server atomically binds the exact assignment. The assigned reviewer cannot apply Task feedback, start repair or publish. `task.request_changes` is the guarded manager disposition and is not part of the normal assigned-reviewer core. A legacy profile explicitly named `Reviewer` may retain old compatibility behavior, clearly separated from this canonical surface.

Any previously implemented reviewer profile behavior is a separately identified legacy surface until deliberately migrated. The canonical coordination surface is `coordination.consult`; availability uses `coordination.watch.create/list/cancel`, not eager `ask_owner` or `notify_when_available` aliases. A tool's existence or read/write annotation never overrides application authorization. Missing new review support is an explicit capability gap, not a silent substitution of a manager tool.

### Observer

```text
swarm.dashboard
swarm.context.get
swarm.tools.search
operation.get
```

All reads are scoped and bounded. GM/operator uses the manager core, with rare authority-sensitive tools deferred. Having more rights is not a reason to preload the entire catalog.

## 3. Names and groups

| Convenience name | Lower-level implementation/detail |
|---|---|
| `swarm.context.get` | Task/Attempt/cards; supersedes old `coordination.context.get` examples. |
| `coordination.consult` | Card field lookup, exact owner discovery and addressed ask; `ask_owner` is not a competing eager alias. |
| `coordination.watch.create/list/cancel` | One passive shared watch service; current kinds are `operation_terminal`, `contract_revision_changed`, `task_revision_changed`, `attempt_disposition_changed`, `exact_deadline_reached` and `submission_reviewed`. Availability is not a separate `notify_when_available` API. |
| `coordination.sync_integration` | Store-derived scoped integration sync; advisory coordination does not accept work or wake a model. |
| `coordination.send` | Typed peer delivery over coordination/raw mailbox primitives. |
| `swarm.overlap.check` | Recomputed ELIOT ownership, scope and bounded Git evidence; history is not current ownership. |
| `swarm.agent.steer` | Manager-owned exact `agent.send` semantics, not peer mail. |
| `swarm.agent.inspect` | One current scoped work/runtime/capability projection. |
| `swarm.launch` | Linked existing Task/workspace/binding/Participant/runtime admissions, not an MCP-only macro. |
| `agent.open` | Manager-admitted asynchronous open tied to an exact Task claim and held workspace lease; C10 adds a parent-linked dispatch path. Current live-service qualification is tracked in [Implementation Status](implementation-status.md). |
| `task.dispatch` | Launch-owned Attempts require the exact `launch_operation_id`; Store binds the immutable prompt packet to retained Task/Attempt/binding/lease ancestry and rechecks the current MCP capability proof before native input. The legacy unlinked direct-dispatch contract remains separate; current qualification is tracked in [Implementation Status](implementation-status.md). |
| `swarm.tools.search` | Authorized catalog lookup and loading guidance, not a generic execute-method endpoint. |

Put prompt vocabulary synonyms in search metadata, not additional eager public aliases. Slash notation denotes separate typed methods.

Groups remain `core`, `participant-coordination`, `assignment-read`, `manager-core`, `runtime-control`, `runtime-recovery`, `monitoring`, `git-read`, `task-management`, `review`, `acceptance-effects`, `administration`, `schedules`, and `mailbox-raw`. The operations program adds its deferred configuration, runtime-profile, stream, hook, script and Goal groups to the same registry.

Each group has a stable ID, short discriminative purpose, profile eligibility, loading tier, catalog revision and search terms. Roughly 4–9 model-facing methods is a usability target, not a permission limit. A group is not a process or a new authority.

## 4. Discovery is not proof of model access

Record only what was observed:

```text
configured               the trusted launch configuration requested it
listed_by_transport      the authenticated MCP session listed its schema
acknowledged_by_harness   the native client explicitly reports discovery/loading
successfully_used        a real authorized call returned through this path
unknown / unsupported    the relevant evidence is absent
```

MCP `tools/list` and `notifications/tools/list_changed` describe server inventory. They do not prove that a particular harness loaded a schema into model context. Do not label server-side activation `model_ready` or create extra paid model turns merely to fill a readiness record.

Use actual native inventory/readback or a harmless call during already requested work where supported. A proven missing required reporting tool makes that launch incompatible or explicitly relay-only. Unknown client visibility is a precise gap, not proof of either success or failure, and does not disable unrelated tools or agents.

Overall launch states remain `ready`, `ready_with_gaps`, `relay_only` and `incompatible`, derived from the actual required capability contract. Reconnection and capability discovery must never start a second productive session.

## 5. Client-compatible loading

One logical catalog can have several presentations. Qualify the installed client/model path rather than require an old software release or infer support from a product name.

| Client path | Presentation |
|---|---|
| OpenAI Responses | Small eager core plus deferred logical MCP group views, or client-executed tool search over the authorized catalog. |
| OpenAI managed agent tooling where supported | Role-filtered MCP and the documented discovery mechanism; record actual support rather than assume automatic deferral. |
| GitHub Copilot CLI/custom agents | Core server `deferTools: "never"`; optional catalog/domains use supported automatic deferral and exact custom-agent subsets. |
| Claude Code/Agent Teams | Explicit local role core, optional searchable domains and native capability evidence for the actual teammate backend. |
| Simple MCP client | Generated fixed role core plus a relevant optional group; safe client-supported relist/reconnect when a different surface is needed. |

OpenAI client-executed search is useful when availability depends on current project/Task/permission state. Logical core, participant, manager, Git, review, runtime and effect views may expose the same application through narrow inventories. Never mark one huge server eager merely to make missing discovery appear to work.

For Copilot/Claude, configuration/frontmatter narrows requested tools but is not proof of runtime parity. Do not silently drop required tools or treat a different in-process/separate-process backend as equivalent. An agent without a usable result path must not start productive work under a false-ready claim.

Native tool search, server-side surface selection and ELIOT `swarm.tools.search` are different mechanisms. The search tool returns matches/loading disposition; printing a schema in text is not dynamically registering a callable tool. The harness must use its supported loader or refresh path. Unsupported clients get an explicit gap or generated surface, not a generic arbitrary RPC tool.

## 6. Relist and reconnect

Search/loading dispositions are `already_loaded`, `auto_activatable`, `reconnect_surface_required` and `unsupported`. Do not reveal denied names or hidden inventory counts through search; exact denied calls remain rejected by the existing protocol/application path.

If the session supports safe refresh, an explicit group selection changes its authorized presentation and emits one list-changed notification. The client relists; record the actual server-visible revision separately from client acknowledgement. Do not emit list-changed for message arrival, liveness, queue motion or Task state.

If refresh is unsupported, use a supported safe MCP reconnect/surface change without restarting native work. Do not promise a non-disruptive reconnect for a harness that cannot do it: report that limitation and prepare the next permitted connection instead. No silent full-profile fallback, dropped in-flight request, provider restart or replacement model session.

Static catalog pagination is deterministic at its catalog/profile/surface revision. Filter before paging; reject stale/invalid cursors explicitly. Reads and search do not execute work or change manager automation settings. Tool annotations remain descriptive hints, not enforcement.

## 7. Process and transport layout

The shared component is the ELIOT host/application/Store, not an imaginary shared stdin stream.

```text
stdio-only native client -> small Rust MCP facade child -> authenticated local IPC
another native client    -> its isolated facade/connection -> the same ELIOT host

HTTP-capable clients     -> authenticated Streamable HTTP sessions
                            -> the same ELIOT application
```

Standard stdio commonly means a server subprocess per connected client. Do not promise one stdio child multiplexes unrelated clients without a qualified transport. Keep those children thin: bounded protocol buffers, no SQLite ownership, no native session ownership and no full-ledger/transcript bootstrap. Reuse immutable catalog metadata where possible. Registered but disconnected/inactive participants require no facade process.

Streamable HTTP can support multiple authenticated sessions in one service when implemented. That does not permit cross-session credential, tool-surface or event leakage. Server/client session cleanup releases only its transport resources; closing a viewer does not stop the shared host or native work.

Do not create one heavy wrapper per logical group. Measure actual connected-facade count, memory and teardown separately from registered agents and active model turns. All owned facades/gateways are Rust. Native stdio clients need no external Cloudflare hop; remote tunneling is transport, never policy.

## 8. Catalog search example

Request and response are illustrative proposed schemas; placeholder IDs are not live invocation values.

```json
{
  "query": "compare changed paths with my code scope",
  "purpose": "implementation",
  "task_id": "CURRENT_TASK_ID",
  "loaded_catalog_revision": "CURRENT_CATALOG_REVISION",
  "max_results": 5
}
```

```json
{
  "catalog_revision": "CURRENT_CATALOG_REVISION",
  "matches": [
    {
      "group": "git-read",
      "tool": "git.overlap",
      "title": "Compare candidate paths with current scopes",
      "why": ["path_overlap", "current_task_relation"],
      "activation": "auto_activatable"
    }
  ],
  "coverage": "complete",
  "gaps": []
}
```

Same query plus catalog/profile/work-context revision produces deterministic ordering. Loading changes presentation only; it cannot grant another repository, create a Task, start a model or authorize a network/push/merge effect.

## 9. Local profile examples

These are planned additions to current configuration; private credentials remain local and are not included.

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
deferred_groups = ["monitoring", "task-management", "runtime-control", "runtime-recovery", "git-read", "review"]
```

For a remote client, use locally configured endpoint/credential handles and an exact role/tool subset. Documentation uses placeholder endpoints only. Endpoint path or server label cannot choose a privileged identity on its own. Actual flat MCP tool names must come from the registry's single reversible naming map, not manually invented dotted/underscore aliases.

## 10. Qualification and primary references

Verify hard-profile non-disclosure, manual hidden-call rejection, pagination, schema token cost, search misses/wrong-tool rate, cache/first-load latency, real client relist/reconnect behavior, missing-core diagnosis, per-connection credentials, thin-facade memory/cleanup and zero model wake from ordinary message/watch/surface changes.

Review cases must prove an assigned auditor can submit its own evidence but cannot invoke manager disposition or another review slot. Manager-owned automated effects remain visible to their manager even with a technical service requester.

Protocol references rechecked 2026-10-03: [MCP transports](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports), [MCP tools](https://modelcontextprotocol.io/specification/2025-06-18/server/tools), [OpenAI tool search](https://developers.openai.com/api/docs/guides/tools-tool-search), [Copilot tool search](https://docs.github.com/en/copilot/concepts/agents/copilot-cli/tool-search). A dated specification link identifies reviewed semantics; negotiated support is not frozen to that edition. Other donor evidence remains in [MCP Source Map](mcp-tool-catalog-sources.md).

**Invariant:** one logical catalog and one application authority, several truthful client presentations, a small immediately useful toolbox, and no unsupported promises about schema loading or process sharing.
