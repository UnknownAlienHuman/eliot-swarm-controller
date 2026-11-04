# Claude Code

L6 adapter, after automation/coordination already work on Codex. Owner decision
2026-11-03: a Rust plugin over `claude -p --input-format stream-json
--output-format stream-json --verbose` in one per-Agent plugin/native process
tree; no TypeScript Agent SDK, no Node.

## Sources and unresolved facts

The stream-json record shapes were verified against the installed CLI
(`claude -p --input-format stream-json --output-format stream-json --verbose`,
Claude Code 2.1.278): [headless](https://code.claude.com/docs/en/headless),
[hooks](https://code.claude.com/docs/en/hooks),
[CLI](https://code.claude.com/docs/en/cli-reference).
The TypeScript Agent SDK is not used.
These notes do not establish runtime support or a current license approval.

## Native identity and generation

Subject is `(canonical CLAUDE_CONFIG_DIR + project/store identity, exact session id)`.
Resume uses that session in a new Eliot generation. Late SDK callbacks belong to the old
generation and cannot affect replacement work. Native subagents normally share parent
environment; no independent Eliot token without actual isolated environment support.

## Transport

Run the installed native `claude` executable with `-p --input-format stream-json
--output-format stream-json --verbose` and an Eliot-minted `--session-id`; pass
`--model` for the frozen profile model and `--resume` only for explicit resume.
Speak NDJSON on stdin/stdout: `control_request` envelopes (`initialize`,
`set_model`, `set_permission_mode`, `interrupt`) with `request_id` correlation,
`user` input records whose Eliot-minted `uuid` is the exact turn association,
and the `command_lifecycle` / `assistant` / `system` / `result` stream records.
Arm the message reader before input. SessionStart hook lines carry the minted
session id before any input, so the adapter declares no `start_requires_input`.

Resolve Windows executables explicitly; never insert an unrequested shell wrapper.
Avoid modes that disable required hooks/instructions.

Stop drops the CLI's stdin and reaps that direct child by handle (kill after
a 2 s grace if EOF does not end it). Grandchildren — tool processes the CLI
itself spawns — are neither tracked nor reaped by the plugin: on stop they
are left to the OS (reparented on Unix, detached but running on Windows).
The fixture CLI spawns nothing, so no test covers their fate.

## Native mapping

| Eliot verb | Mechanism / limitation |
| --- | --- |
| launch/send | stdin `control_request initialize` with the pre-assigned session id, then one stdin `user` record per send; the Eliot-minted `uuid` echoed in `command_lifecycle` is the exact turn association, not local pipe acceptance |
| steer | unsupported: no exact-current-turn mechanism exists in PR1 |
| inject | unsupported: no inject verb; a turn never receives another turn's queued context |
| interrupt | stdin `control_request interrupt` for the bound active turn uuid, with the `still_queued` readback; the terminal follows and unknown is not repeated |
| stop | drop stdin (the CLI exits on EOF) and reap the owned CLI by handle; does not merely detach observation; grandchildren are left to the OS (see Transport) |
| resume | unsupported in PR1: `resume_from` is rejected; resume with the exact native session id lands in a later slice |
| approve/deny | unsupported in PR1: approvals are unconnected and the CLI runs its default permission posture |
| answer | unsupported in PR1: no question-answer verb |
| configure | unsupported in PR1: `set_model` / `set_permission_mode` are never sent; the CLI runs its launch flags |
| goal | no native goal mechanism; goal selection stays in Eliot |

If a turn ends before queued hook context is consumed, do not deliver it to a later
turn. A delayed hook is inject, never advertised as exact steer.

## Source ownership and recovery

SDK stream owns supplied session/turn/message/tool/usage facts. Callbacks own missing
permission/question decisions; hooks never mirror the same lifecycle. Final text is
persisted before exact terminal result. Distinguish content blocks/tool ids within a
message instead of deduplicating all blocks by message id.

Transcript JSONL is later observe/recovery data with file identity/offset where valid,
not a parallel live producer. No proven replay cursor means gap + exact current
readback, never full-history republishing. Child completion needs exact tool/task
provenance, not parent idle. Resumed cumulative cost is session scope, not a turn delta.

## Proxy and qualification

Use documented base URL/auth/header variables in the frozen profile; exclude unrelated
owner API keys. Report unavailable effective-route facts as unknown. Verify init ordering,
late callbacks, context expiry, request ids, resumed inputs, transcript overlap and actual
proxy/native billing in L7. Common generation/ack/custody rules remain in the plugin spec.
