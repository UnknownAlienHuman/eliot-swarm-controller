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
Verified live on Claude Code 2.1.278 (2026-11-04): with
`--permission-prompt-tool stdio` a tool use arrives as a `control_request`
`{"subtype":"can_use_tool","tool_name","display_name","input","description",
"permission_suggestions","tool_use_id"}`; a `control_response` with
`{"behavior":"allow","updatedInput":<input>}` runs the tool and
`{"behavior":"deny","message":…}` refuses it, each on the same `request_id`.
Unverified live (fake-only until a live run): the `set_model` /
`set_permission_mode` calls on a resumed session (their success shape was
observed once on a fresh CLI, but the resume-realign flow never ran live) and
the resumed CLI's session re-announcement.

## Native identity and generation

Subject is `(canonical CLAUDE_CONFIG_DIR + project/store identity, exact session id)`.
Resume uses that session in a new Eliot generation. Late stream-json records belong to
the old generation and cannot affect replacement work. Native subagents normally share parent
environment; no independent Eliot token without actual isolated environment support.

## Transport

Run the installed native `claude` executable with `-p --input-format stream-json
--output-format stream-json --verbose` and an Eliot-minted `--session-id`; pass
`--model` for the frozen profile model, `--permission-mode` when the frozen
profile sets one, and `--permission-prompt-tool stdio` always so every
permission ask reaches Eliot. Pass `--resume <session-id>` only for explicit
resume, instead of `--session-id`. Speak NDJSON on stdin/stdout: `control_request`
envelopes (`initialize`, `set_model`, `set_permission_mode`, `interrupt`) with
`request_id` correlation, `user` input records whose Eliot-minted `uuid` is the
exact turn association, the `command_lifecycle` / `assistant` / `system` /
`result` stream records, and inbound `can_use_tool` permission asks answered
with a `control_response` carrying the documented SDK `PermissionResult`.
Arm the message reader before input. SessionStart hook lines carry the minted
session id before any input, so the adapter declares no `start_requires_input`.
A resumed CLI re-announces the same retained session id (unverified live;
the fake re-announces it).

Resolve Windows executables explicitly; never insert an unrequested shell wrapper.
Avoid modes that disable required hooks/instructions.

Stop drops the CLI's stdin and kills only that direct child by handle (kill
after a 2 s grace if EOF does not end it). Grandchildren — tool processes
the CLI itself spawns — are reaped by the generation custody the daemon put
the plugin in, not by the plugin: on Windows the plugin starts inside the
generation's named kill-on-close Job (`OwnedChild::spawn_agent` in
`crates/eliot-daemon/src/platform/custody.rs`, via
`PluginHost::spawn_plugin`), so closing the Job kills the whole family; on
Unix the plugin leads its own process group and the daemon group-kills it
(degraded: escapable via setsid). The fixture CLI spawns nothing, so no test
covers their fate.

## Native mapping

| Eliot verb | Mechanism / limitation |
| --- | --- |
| launch/send | stdin `control_request initialize` with the pre-assigned session id, then one stdin `user` record per send; the Eliot-minted `uuid` echoed in `command_lifecycle` is the exact turn association, not local pipe acceptance |
| steer | unsupported: no exact-current-turn mechanism exists |
| inject | unsupported: no inject verb; a turn never receives another turn's queued context |
| interrupt | stdin `control_request interrupt` for the bound active turn uuid, with the `still_queued` readback; the terminal follows and unknown is not repeated |
| stop | drop stdin (the CLI exits on EOF) and kill only the direct CLI child by handle; does not merely detach observation; grandchildren are reaped by the generation Job on Windows / process group on Unix, not the plugin (see Transport) |
| resume | new plugin/CLI with `--resume` and the exact retained session id; never resend prior prompt; the frozen model is re-applied via `set_model` and a drifted permission mode via `set_permission_mode` on the idle preflight (both fake-only, unverified live) |
| approve/deny | exact `can_use_tool` native request id; allow echoes the asked `input` as `updatedInput`, deny sends the fixed message (verified live, 2.1.278) |
| answer | unsupported: the AskUserQuestion shape on the installed CLI is unverified |
| configure | applied at start, not a runtime verb: `--model` / `--permission-mode` argv on a fresh start, `set_model` / `set_permission_mode` realign on resume; requested vs effective settings are reported before readiness, with `effective.model` always null (the CLI never reads the active model back; `models[]` membership is the only proof) |
| goal | no native goal mechanism; goal selection stays in Eliot |

If a turn ends before queued hook context is consumed, do not deliver it to a later
turn. A delayed hook is inject, never advertised as exact steer.

## Source ownership and recovery

The stream-json stdout stream owns supplied session/turn/message/tool/usage facts.
Callbacks own missing permission/question decisions; hooks never mirror the same
lifecycle. Final text is
persisted before exact terminal result. Distinguish content blocks/tool ids within a
message instead of deduplicating all blocks by message id.

Transcript JSONL is later observe/recovery data with file identity/offset where valid,
not a parallel live producer. No proven replay cursor means gap + exact current
readback, never full-history republishing. Child completion needs exact tool/task
provenance, not parent idle. Resumed cumulative cost is session scope, not a turn delta.

## Login

The CLI keeps its own login in its config directory; the Claude desktop app's
session is host-managed and is not shared with other processes. Before every
launch or resume the plugin runs `claude auth status` with the frozen
environment. When the CLI is logged out it runs the CLI's own
`claude auth login --claudeai` (the owner completes it in the browser) and waits
up to five minutes; an unfinished login rejects the start with
`native_not_dispatched` before any session exists. The plugin never sees or
stores credentials.

## Proxy and qualification

Use documented base URL/auth/header variables in the frozen profile; exclude unrelated
owner API keys. Report unavailable effective-route facts as unknown. Verify init ordering,
late callbacks, context expiry, request ids, resumed inputs, transcript overlap and actual
proxy/native billing in L7. Common generation/ack/custody rules remain in the plugin spec.
