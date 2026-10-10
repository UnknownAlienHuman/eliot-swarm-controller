# Kilo Code — Rust Runtime Module and Scoped ELIOT MCP

Revision 1 · 2026-10-03 · design contract, not an installed or live-qualified plugin.
Base: `a0a931ef02aa4627a58f3076e17462272b13e755`. Read the [module contract](../agent_swarm.module-contract-v2.md), [MCP surface](../mcp-canonical-surfaces-and-topologies.md) and [local-provider contract](contracts.md) for shared rules. Kilo source references below identify reviewed code, not required installation releases.

## 1. Two directions, one owner per responsibility

```text
ELIOT manager -> Task/Attempt/Operation -> Rust Kilo RuntimePort
                                              |
                                      HTTP/SSE kilo serve
                                      /                \
                    local inference provider      ELIOT MCP client
                    llama/vLLM/LM/Unsloth          scoped role core
```

The requested plugin is an ELIOT-owned **Rust runtime adapter**, not an editor macro or a new TypeScript service. Native Kilo remains a whole external product. It owns its model/tool loop; ELIOT owns assignments, permissions, publication and acceptance. This module also works with an explicitly selected permitted cloud provider; selecting Kilo does not force Kilo Gateway or change billing.

Start with a Windows-native Kilo server and Windows-native ELIOT/MCP. vLLM alone may run in [WSL2](wsl-vllm.md). Neither Kilo's VS Code extension nor JetBrains nor deprecated Console is required for headless execution. An editor-owned server may be attached explicitly, but remains owned by the editor.

Do not rename the OpenCode V2 adapter to `kilo`. Kilo's OpenCode-derived code is useful evidence, not protocol equivalence. Runtime kind is the open string `kilo`; existing Store operations stay vendor-neutral.

## 2. Inspected native interfaces

Read source snapshot `76bcfd40be616a72f4697b3041565f322245b462`:

- [session API](https://github.com/Kilo-Org/kilocode/blob/76bcfd40be616a72f4697b3041565f322245b462/packages/opencode/src/server/routes/instance/httpapi/groups/session.ts): exact routes, request schemas, async admission and abort scope.
- [prompt pipeline](https://github.com/Kilo-Org/kilocode/blob/76bcfd40be616a72f4697b3041565f322245b462/packages/opencode/src/session/prompt.ts): session/tool/model ownership and Kilo-specific continuation seams.
- [prompt queue](https://github.com/Kilo-Org/kilocode/blob/76bcfd40be616a72f4697b3041565f322245b462/packages/opencode/src/kilocode/session/prompt-queue.ts): per-session waiting state, changed-only notifications, follow-up boundary and targeted cancellation.
- [steering notification](https://github.com/Kilo-Org/kilocode/blob/76bcfd40be616a72f4697b3041565f322245b462/packages/opencode/src/kilocode/session/steering.ts): human-marked subagent steer is distinct from the task tool's own prompt.

Use the installed server's advertised schema when available, otherwise a reviewed compatible packaged schema; never auto-upgrade the server to match this research SHA. The tracked [OpenAPI artifact](https://github.com/Kilo-Org/kilocode/blob/main/packages/sdk/openapi.json) and generated JS SDK are schema donors, not mandatory JS dependencies for ELIOT.

| ELIOT responsibility | Native source surface | Required interpretation |
|---|---|---|
| Service description | `/global/health`, configured connection, reported build/schema | Health is transport readiness, not agent progress. |
| Create / attach | `POST /session`; `GET /session/{sessionID}` | Creation is an effect; attach does not prompt, resume or replace missing identity. |
| Snapshot / family | `GET /session/status`, `GET /session/{sessionID}/children`, session/message reads | Filter by exact directory and owned lineage; root idle does not prove children done. |
| Send work | `POST /session/{sessionID}/prompt_async` | Returns prompt acceptance, may start an idle session; not peer mail or final outcome. |
| Read result | `GET /session/{sessionID}/message`, exact message reads | Correlate assistant result with retained native input; EOF/idle alone is insufficient. |
| Observe | `/event` or `/global/event` | Reconnectable live hints, not guaranteed durable replay. |
| Native request reply | Installed permission/question endpoint and exact pending request ID | Validate current target and choice; no guessed yes/first-answer policy. |
| Stop request | `POST /session/{sessionID}/abort` with explicit supported scope | Reviewed source defaults to tree. Never omit scope and accidentally stop descendants. |

Directory routing is explicit in every instance-scoped request (`directory` query or `x-kilo-directory` according to native schema). No fallback to server cwd for an ELIOT-managed assignment. Cold instance bootstrap may start native plugins/MCP/indexing: establish the authorized directory during setup, not by probing arbitrary directories during dashboard reads.

## 3. Session, input and recovery contract

Keep separate ELIOT Operation/binding/generation, native session, native message, assistant parent reference and native turn if actually provided. Do not manufacture an `expectedTurnId` from a message ID.

Persist input identity/effective request before native I/O. Caller-supplied native message IDs, where supported, are correlation aids; establish native duplicate semantics before treating them as idempotency. After uncertain POST, read retained native message/result. No match with incomplete coverage remains unknown; it does not authorize another prompt or replacement session.

Native prompt queue is not proof of crash-durable admission: inspected queue scheduling uses in-process state. A saved user message after server restart is not proof it is still queued. Reconcile each retained ELIOT operation against actual native status/result. Do not replay all old prompts on reconnect.

Initial send contract is `next_turn` / documented native queue behavior. The inspected source allows newer input to take over at an LLM-step boundary; that is not an atomic expected-active-turn contract. Advertise `native_expected_target` only after a real native target guard is available and verified. Never simulate exact steer with abort-and-resend.

`prompt_async` is never the transport for ordinary coordination notices. Agents pull the ELIOT inbox via MCP; a supported safe-boundary presentation may show a header without changing the task. An explicit manager command may start native work, including after a legitimate repair result.

Task completion requires retained result and ordinary submission/review. A Kilo todo, final sentence, idle event or successful HTTP response cannot accept the ELIOT Task.

## 4. Monitoring without a process per poll

Use a shared bounded Rust HTTP client and one appropriate event reader per connected service, with per-directory/session filtering. `/global/event` multiplexes directories; events from other users/workspaces must not enter the current binding's state or counts. Missing routing metadata is a gap, not a broadcast instruction.

Normalize native text/reasoning/tool/usage/status parts into existing observations/projections. Reasoning means only fields Kilo actually emits. Keep token deltas in bounded stream buffers and durable summary/artifact boundaries, not a full duplicated transcript per viewer. Heartbeats indicate connection life, not material work.

After a gap, reconcile owned sessions/messages using bounded readback; preserve partial/unknown coverage. Distinguish pending permission, queued input, running tool, waiting child, terminal failure and unknown. Never run `kilo daemon start/status` or `kilo run` on every dashboard refresh. Native daemon/client entrypoints can reuse, replace or start servers; use direct HTTP for observation.

## 5. Configuration and local inference

Use current Kilo config paths/schema, not legacy extension `mcp_settings.json` or OpenCode V2 settings. Read [CLI setup](https://kilo.ai/docs/code-with-ai/platforms/cli), [provider config](https://kilo.ai/docs/ai-providers/openai-compatible), [provider selection](https://kilo.ai/docs/ai-providers) and effective installed configuration before applying.

Manager-selected tuple:

```text
runtime = kilo
connection_ref = protected Kilo HTTP connection
workspace = current manager-owned Windows worktree
provider_id + model_id + supported native options
inference_backend_id = selected local-model descriptor
ELIOT role/profile + actual MCP surface evidence
```

Register a custom Chat Completions-compatible provider for llama.cpp/LM Studio/vLLM/Unsloth where that exact model supports the required tool cycle. Responses and Messages are separate dialects, not fallback aliases. Do not concatenate `/v1` or `/chat/completions` twice.

Apply provider/MCP changes to the explicitly selected configuration boundary, preferably an owned launch overlay supported by the installed runtime. Preserve user settings and read back the effective state. File saved is not runtime applied; if a restart is required, only the owner may plan it after active work is preserved. No restart of a shared editor server merely to qualify this plugin.

A local-only profile can use supported provider allowlists such as `enabled_providers`, but this is routing configuration, not an OS network sandbox. Also check children, title/summary/compaction, embeddings/indexing, update and sharing paths. Disable or explicitly authorize unwanted external calls. Never silently fall back to Kilo Gateway, an anonymous cloud catalog or a subscription credential when the local model fails. Keep server Basic Auth, provider credentials and ELIOT MCP credentials separate.

## 6. Scoped MCP and roles

Kilo connects to the existing ELIOT Rust MCP facade using the [native MCP config](https://kilo.ai/docs/automate/mcp/using-in-cli). A Windows Kilo process spawns the Windows `swarm mcp` executable as a small stdio child with the exact allowed profile. It does not execute that command inside the vLLM WSL environment. An explicitly configured remote MCP path is separate.

Start with the current role core, not all ELIOT tools. Defer detailed inference, runtime, audit and administration groups. Native dynamic discovery/relist behavior must be established for installed Kilo; otherwise use the fixed core and supported safe refresh path. `/mcp` connection state or `tools/list` does not prove model-context loading.

Kilo agent modes (`code`, `ask`, `orchestrator` or custom) do not appoint ELIOT managers. Bind the authenticated ELIOT role and work context independently. Native built-in shell/file/subagent tools also need the selected native permission policy: a restricted MCP credential does not sandbox those tools. Do not pass a parent's manager credential to every native child. Children have distinct scoped identities or a truthful relay-only path until supported injection is implemented.

No model invocation just to fill a readiness checkbox. The first explicitly authorized small workflow can supply actual successful-use evidence. Missing mandatory reporting/tool support is visible, not silently fixed by showing the full catalog.

## 7. Hooks: no fake Rust ABI

The official [Kilo plugin API](https://kilo.ai/docs/automate/extending/plugins) loads JS/TypeScript modules in-process. It describes `tool.execute.before/after`, chat and compaction hooks. It does not establish a native Rust plugin ABI or arbitrary executable hook transport.

Therefore the ELIOT module uses Rust HTTP/SSE observation plus existing ELIOT action hooks and native Kilo permissions. Map only events present on the actual wire; an internal plugin callback is not automatically exported by `/global/event`. Commit-to-auditor may use the existing verified Git/ELIOT event path without a Kilo-specific JS plugin.

Record native blocking interception as unsupported/unverified where no Rust-callable native boundary exists. Post-event observation cannot veto the action. Do not ship a required ELIOT-owned TS/Python shim to claim full hook coverage. A later native executable/HTTP hook transport may be integrated when actually offered; externally installed user plugins remain the user's separate trust boundary.

Native Kilo Goal, when present, and the ELIOT server Goal must not both own continuation for the same work. Enabling a Kilo route enables neither. The manager chooses continuation explicitly; disabling an ELIOT helper is not proof that a native Goal was stopped.

## 8. Ownership and rollout

Initial topology is `external_attach` to a deliberately configured `kilo serve`. Server Basic Auth, when configured, protects HTTP/SSE; use a protected local connection reference and strong credentials for owned launches, never encode credentials in a logged URL. Presence of `daemon.json` is a hint, not authorization to control the server.

Later `eliot_owned` may start one explicit foreground server via existing Rust process ownership. Do not rely on a detached daemon launcher as evidence that child cleanup is correct. One server can serve several authorized directories, but one physical session family has one lifecycle owner. Disconnect/unhealthy probe does not authorize service restart, new session, database write, worktree removal or setting changes.

Reuse, not fork: existing reqwest/SSE primitives, RuntimeCommand/RuntimeOutcome, prepared/prerequisite registry, bounded projections, owner process lifecycle and shared MCP catalog. Useful Kilo code patterns are exact directory routing, changed-only queue notifications and distinct prompt/result identities; do not import its board, Task scheduler, database or JS SDK into Store.

Implementation units, with names checked on current source before editing:

| Unit | Complete implementation work |
|---|---|
| New `src/runtime/kilo/` | Native DTO/HTTP mapping, prepared operations, snapshot/event/result readers, explicit unsupported operations. |
| Runtime registration / config / prerequisites | Register `kilo` through the existing adapter boundary; credentials referenced, no vendor branches in Store. |
| `modules/kilo/README.md` and module descriptor when code lands | Describe actual entrypoint, update procedure and per-capability evidence, no version-equality gate. |
| Shared launcher / runtime profile | Bind workspace, selected provider/model, owner and MCP context once; keep next-assignment preference semantics. |
| Shared MCP catalog / CLI / doctor | Existing generic methods target `kilo`; only necessary native-specific operations are deferred and schema-bound. |

Useful first increment: attach -> exact directory -> create session -> prompt admission -> observed result -> ordinary ELIOT submission; no mandatory UI, cron, cloud gateway or new tool loop. Keep manual control and manager-owned opt-in automation unchanged.

## 9. Qualification and user evidence

[Issue #8656](https://github.com/Kilo-Org/kilocode/issues/8656) reports MCP completion followed by a stalled agent loop while HTTP health remained responsive. This is a reported case, not a current universal defect. It motivates separate health/progress and tool-result-to-model checks, not an automatic kill policy.

[Issue #13082](https://github.com/Kilo-Org/kilocode/issues/13082) reports a `tool_choice` mismatch through an OpenAI-compatible proxy. It is not proof of a vLLM bug; verify the entire configured dialect chain rather than guessing flags or disabling tools globally.

Required scenarios: two directories on one server; child attribution; input accepted but reply lost; server restart with saved message but lost queue; MCP result returned then loop stalled; malformed tool delta; permission wait; model ID/template change; no cloud fallback; native API absence; explicit abort scope; external service survives client shutdown; Rust-only installed ELIOT pieces; Windows Kilo -> vLLM/WSL -> ELIOT MCP tool round-trip. None is declared passed in this documentation PR.
