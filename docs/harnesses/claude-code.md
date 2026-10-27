# Claude Code

L6 adapter, after automation/coordination already work on Codex. Start with the
TypeScript Agent SDK in one per-Agent plugin/native process tree; a later Rust
stream-json replacement must remove the old executor rather than coexist with it.

## Sources and unresolved facts

Inherited protocol notes were checked on 2026-10-25. Implementation must verify the
selected installed SDK and its terms: [SDK documentation](https://code.claude.com/docs/en/agent-sdk/typescript),
[headless](https://code.claude.com/docs/en/headless),
[hooks](https://code.claude.com/docs/en/hooks),
[CLI](https://code.claude.com/docs/en/cli-reference).
These notes do not establish runtime support or a current license approval.

## Native identity and generation

Subject is `(canonical CLAUDE_CONFIG_DIR + project/store identity, exact session id)`.
Resume uses that session in a new Eliot generation. Late SDK callbacks belong to the old
generation and cannot affect replacement work. Native subagents normally share parent
environment; no independent Eliot token without actual isolated environment support.

## Transport

Use `query({prompt: AsyncIterable<SDKUserMessage>, options})` with exact cwd/model/
permissions, interrupt and permission callbacks. Arm the message reader before input.
Verify whether init/session id arrives before input; otherwise declare
`start_requires_input` and retain separate linked-session/input outcomes.

The alternative CLI is explicit print/stream-json input/output with resume/session flags,
not a hidden fallback. Avoid modes that disable required hooks/instructions. Resolve
Windows executables explicitly; never insert an unrequested shell wrapper.

## Native mapping

| Eliot verb | Mechanism / limitation |
| --- | --- |
| launch/send | SDK init and one queued SDK user message; exact turn association, not local pipe acceptance |
| steer | unsupported until an actual exact-current-turn mechanism is verified |
| inject | assigned prompt/tool-hook `additionalContext`; report safe point and bind captured turn |
| interrupt | `Query.interrupt()` for the bound active query/turn; unknown is not repeated |
| stop | close query/input and owned process tree; does not merely detach observation |
| resume | new plugin/query with exact native session id; never resend prior prompt |
| approve/deny | exact pending permission callback and offered native decision |
| answer | exact AskUserQuestion callback only if installed SDK exposes the required shape |
| configure | documented setter or next-turn/session setting; report actual application point |
| goal | native only if verified; otherwise explicitly selected Eliot Goal |

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
