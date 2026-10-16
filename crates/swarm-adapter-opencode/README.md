# OpenCode V2 module adapter

`eliot-opencode-v2.rust-http.1@0.7.0` is the standalone OpenCode adapter. Its descriptor uses `opencode-v2-native-options@4` (SHA-256 `112d277fe7774bef1c05d127a60f6fb8adfef0c6c3dab0e749eef0eb07267ed8`) and protocol 1.0. It declares `swarm.task_prompt@1`; every selected `task.dispatch` requires the Store-produced immutable envelope and submits its exact prompt text to the OpenCode 2.0.7 inbox as flat `text` plus `metadata.eliot`. Admission checks the actual `SessionInbox.User` acknowledgement; later reconciliation reads `session.inbox.enqueued` and `session.inbox.delivered`. The acknowledgement has no admission or promotion sequence, so the adapter never invents one. A legacy public `promoted_sequence` may represent inbox delivery only; it is not execution evidence and does not set `execution_complete`. Artifact selection is explicit, disabled by default, and requires a trusted catalog entry.

Artifact 0.7.0 is an uninstalled source candidate for the 2.0.7 SSE/forms/permission wire and native service registration. Native-options schema 4 requires only service, connection and directory; an explicit provider/model/variant trio is an optional caller-supplied invocation override with all-or-none validation, never defaulted by the controller. It exposes no provider credentials or isolated-owner launch configuration. Provider accounts remain entirely native. Current qualification and remaining failures are recorded in [Implementation Status](../../docs/implementation-status.md); no full CI or successful native model execution is claimed.

The descriptor declares agent.open, task.dispatch, agent.send/next_turn, agent.reply, agent.background, agent.refresh, agent.reconcile, and agent.result, along with the four native.mcp.* commands. Unknown OpenCode-native methods stay unsupported. This artifact does not declare native.opencode.loop_step because no supported internal producer exists.

The owner-approved human/current-GM continuation remains the host agent.goal action continue. It admits one deterministic queue input and is not a loop-step producer.
No supported internal Manager or automation producer for native.opencode.loop_step exists in this source baseline. The OpenCode adapter no longer advertises the capability or command schema, and new input submissions fail closed with UNSUPPORTED_CAPABILITY. No MCP or CLI command is added.
The adapter retains the loop-step delivery mapping only for exact readback of previously retained intents; uncertain prior POSTs remain unknown and are never replayed.

The descriptor opts into `swarm.native_mcp_command@2`. Native MCP observe commands carry an explicit `installed_server` purpose for C8 install readback or `assigned_session` for the C7 assignment observation. Assigned-session readback is pinned to OpenCode 2.0.7, uses location-scoped GETs for the MCP list and exact assigned session, and returns only bounded identity/status facts with server errors and integration IDs removed or hashed. Other command and event schemas remain at version 1.

## Service lifecycle

External attach uses the exact service ID, connection file and workspace from the selected route; the native harness supplies provider/model/variant defaults. An explicit caller-requested trio is passed through unchanged as an optional invocation override with all-or-none validation. The observed OpenCode version is diagnostic data, not an admission gate. The adapter never starts, stops, adopts, or restarts an externally attached service.

Fresh isolated SDK service starts are retired. Schema 4 rejects owner launch options, and the retained owner controller refuses a new `fresh_owned_service` start before any native effect. Historical owner/process receipts and custody checks remain readable; they do not authorize starting a replacement or replaying an uncertain launch. Old server source remains historical provenance. New frontend/kernel packages no longer carry that server or install its npm dependency closure; existing install receipts retain fenced read-only inventory support.

Registration examples use native defaults with no model field. When a caller explicitly requests a model, the trio is the exact selected route (for example inclusionai/ling-3.1-flash): `providerID` is `inclusionai`, `id` is `ling-3.1-flash`, and `variant` must be copied exactly with no alias, default or controller registry. The module's controller IPC credential is referenced through an opaque protected `credential_ref`; it is independent of native provider authorization. Examples contain no credential values or provider-auth file paths.

## Native history and controls

Queue sends use the durable OpenCode 2.0.7 inbox admission/readback path and persist their exact native delivery identity. Each admitted `SessionInbox.User` item must retain the exact flat text, `metadata.eliot` marker, session, input identity, and delivery. `session.inbox.enqueued` proves admission; `session.inbox.delivered` can extend an admitted input but cannot establish admission or provide a fabricated sequence. A legacy `promoted_sequence` may therefore mean inbox delivery only; it is not execution proof, and `execution_complete` remains false. The adapter retains loop-step delivery recognition only to reconcile previously retained intents; it does not admit new loop-step submissions. Filtered durable buses can leave sequence gaps; EOF without the scoped sync watermark and uncertain prior POSTs remain unknown.

Questions and permissions are read from the current pending endpoints and projected as bounded typed observations. Replies bind the exact current request fingerprint. Generic permission replies allow only `once` and `reject`; `always` is unsupported. Lost reply responses reconcile from complete contiguous durable history and exact reply payload digests. Permission feedback text is not exposed by that history, so a reject carrying feedback remains unknown after a lost response.

Background first reads `backgroundSubagents`; false is rejected before POST. The documented boolean response distinguishes changed from no-op, while a lost response remains unknown and is never replayed. It does not claim child IDs or execution completion.

`agent.refresh` uses one bounded history page and queues `module.observe`; the cursor advances only after Store acknowledgement. Observations omit transcript and tool output. Result reads bind the exact input operation/session/message. OpenCode's public message projection omits the assistant-to-input parent edge, so assistant result correlation fails closed with `NATIVE_ASSISTANT_PARENT_UNAVAILABLE`; the adapter never substitutes the latest assistant message or guesses from ordering.

Acknowledged non-source operation journals are eligible for bounded reclamation only after exact outcome/result acknowledgements, reference checks, and root-checkpoint readback. Dispatch, send, and loop-step journals remain as result sources only when they retain the exact native input and root identity: later `agent.result` reads reopen that intent and admission, while a per-page Store acknowledgement settles only that read Operation and does not say that no future reader exists.

## Registration examples

- `registration/route.example.json` shows external attach with native defaults (no model override).
- `registration/binding-launch-values.example.json` shows the route fields for external attach with native defaults. The native connection record is read in place; no provider authorization is projected.
- `registration/select-route.request.example.json` selects artifact version `0.7.0`.

The launch argument remains `--config <absolute-host-connection-config-path>`. That route-neutral file contains only host connection settings; descriptor-declared route values are supplied separately by the supervisor.
