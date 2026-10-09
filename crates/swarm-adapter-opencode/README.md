# OpenCode V2 module adapter

`eliot-opencode-v2.rust-http.1@0.5.0` is the standalone OpenCode adapter. Its descriptor uses `opencode-v2-native-options@3` (SHA-256 `070d37891aed021d6a5023cd885647b1403741927e87a0cb28050f30b7c4d97e`) and protocol 1.0. It declares `swarm.task_prompt@1`; every selected `task.dispatch` requires the Store-produced immutable envelope and submits its exact prompt bytes. Artifact selection is explicit, disabled by default, and requires a trusted catalog entry.

The descriptor declares `agent.open`, `task.dispatch`, `agent.send/next_turn`, `native.opencode.loop_step`, `agent.reply`, `agent.background`, `agent.refresh`, `agent.reconcile`, and `agent.result`, along with the four `native.mcp.*` commands. Loop-step is a vendor command with its own `swarm.opencode_loop_step_command@1` schema; ordinary `agent.send/next_turn` remains native queue delivery. Reply commands use `swarm.opencode_reply_command@1`. Unknown OpenCode-native methods stay unsupported.

## Service lifecycle

External attach uses the exact service ID, connection file, workspace, and provider/model/variant from the selected route. The observed OpenCode version is diagnostic data, not an admission gate. The adapter never starts, stops, adopts, or restarts an externally attached service.

A route can request a fresh owned service only with an explicit `fresh_owned_service` declaration. Store admission and the retained owner intent provide the exact task, binding, workspace, and nonce scope before private owner options reach the adapter. The adapter writes intent before launching the pinned child and does not replay or adopt an uncertain launch. A partial or invalid owner declaration fails closed rather than falling back to external attach.

The example model is the exact selected route inclusionai/ling-3.1-flash: `providerID` is `inclusionai`, `id` is `ling-3.1-flash`, and `variant` must be copied exactly from the selected Manager route. Owner credentials are referenced only through an opaque protected `credential_ref`; examples contain no credential values or auth-file paths.

## Native history and controls

Queue and loop-step inputs share the durable admission/readback path but persist distinct native deliveries. The adapter records intent before POST, never replays a possibly-sent request, and uses bounded session history to distinguish admission, promotion, and terminal step evidence. A history gap or missing exact event remains unknown.

Questions and permissions are read from the current pending endpoints and projected as bounded typed observations. Replies bind the exact current request fingerprint. Generic permission replies allow only `once` and `reject`; `always` is unsupported. Lost reply responses reconcile from complete contiguous durable history and exact reply payload digests. Permission feedback text is not exposed by that history, so a reject carrying feedback remains unknown after a lost response.

Background first reads `backgroundSubagents`; false is rejected before POST. The documented boolean response distinguishes changed from no-op, while a lost response remains unknown and is never replayed. It does not claim child IDs or execution completion.

`agent.refresh` uses one bounded history page and queues `module.observe`; the cursor advances only after Store acknowledgement. Observations omit transcript and tool output. Result reads bind the exact input operation/session/message. OpenCode's public message projection omits the assistant-to-input parent edge, so assistant result correlation fails closed with `NATIVE_ASSISTANT_PARENT_UNAVAILABLE`; the adapter never substitutes the latest assistant message or guesses from ordering.

Acknowledged non-source operation journals are eligible for bounded reclamation only after exact outcome/result acknowledgements, reference checks, and root-checkpoint readback. Dispatch, send, and loop-step journals remain as result sources only when they retain the exact native input and root identity: later `agent.result` reads reopen that intent and admission, while a per-page Store acknowledgement settles only that read Operation and does not say that no future reader exists.

## Registration examples

- `registration/route.example.json` shows external attach.
- `registration/route.fresh-owner.example.json` shows the explicit fresh-owner declaration and opaque protected provider reference. Its path and digest placeholders need locally reviewed values; the example remains disabled.
- `registration/binding-launch-values.example.json` shows the route fields for external attach. Owner fields are supplied only when that explicit owner declaration is admitted.
- `registration/select-route.request.example.json` selects artifact version `0.5.0`.

The launch argument remains `--config <absolute-host-connection-config-path>`. That route-neutral file contains only host connection settings; descriptor-declared route values are supplied separately by the supervisor.
