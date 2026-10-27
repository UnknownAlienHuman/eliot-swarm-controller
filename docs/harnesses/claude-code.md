# Claude Code

Layer L4.2. Start with the TypeScript Agent SDK; later Rust may drive stream-json behind
the same plugin contract. Plugin mode is `per_agent`: one SDK/query process and native
Claude Code process tree per controlled Eliot Agent generation.

Facts checked 2026-10-25 against Claude Code/Agent SDK documentation and package versions
listed in the previous page revision. The SDK is under Anthropic commercial terms; check
current terms/version when adding it.

## Native identity and generation

```text
native_scope = claude:<canonical CLAUDE_CONFIG_DIR + project/store identity>
native_ref   = exact Claude session id from init/result metadata
```

The plugin arms its SDK message stream before the first input. First controlled start is
generation 1. Resume creates the next Eliot generation while reusing the native session
id. Exact reconnect to the same still-current SDK/query ownership may preserve
generation; replacement/takeover increments it. Every callback, hook decision, command
and event carries generation. Late callbacks from an old SDK object are historical only.

Subagents run inside the parent's Claude Code process and normally share its environment:
`child_environment=false` until installed evidence proves otherwise. They are canonical
child Agents controlled through parent plugin and receive no fake independent CLI token.

## Transport

- Agent SDK `query({prompt: AsyncIterable<SDKUserMessage>, options})` for streaming
  input, interrupt, permission callback and in-process hooks;
- later CLI route: explicit `-p --input-format stream-json --output-format stream-json
  --verbose`, optional partial events, exact resume/session flags;
- do not use `--bare` for a managed session because it disables hooks/instructions/MCP;
- Windows custom executable must be an absolute supported executable, not an implicit
  shell shim.

Whether SDK init/session id arrives before the first user message is unverified. If not,
manifest declares `start_requires_input=true`; linked first input enters `agent.start`
but still settles separately from session readiness and turn result.

## Source ownership and recovery

For a controlled generation, the SDK/message stream owns `session`, `turn`, `message`,
`tool`, `child` and `usage`. In-process callbacks own only exact permission/question
decisions and context injection facts not already represented by the stream. Transcript
JSONL is not a concurrent lifecycle publisher.

Observe-only/recovery may use project JSONL with an opaque file identity+byte-offset
cursor committed with events. It resumes strictly after that cursor. If a SDK stream
break has no replay cursor, replacement starts at live edge, records `source.gap` and
uses native session/result/current-request readback. It never republishes the whole
transcript as new lifecycle.

A hook/callback does not emit duplicate tool/turn completion already owned by the SDK
stream.

## Verb mapping

All calls are generation-bound; steer/interrupt bind the exact current Eliot turn and
request replies bind exact native request.

| Eliot verb | Claude mechanism | Settlement |
| --- | --- | --- |
| `launch` | SDK query/session init with cwd/model/permissions | `session.ready` from exact init/effective facts; prompt is linked input or start-required initial |
| `send` | push one SDK user message | applied at exact native turn association, not local stream write |
| `steer` | only a qualified exact current-turn mechanism | current known option is `additionalContext` at next `PostToolUse`; report as `inject applies=next_tool`, not exact immediate steer, unless installed recording proves otherwise |
| `inject` | assigned `UserPromptSubmit`/`PostToolUse` additional context | bound to captured turn/generation; if its turn ends before safe point, fail and never leak to next turn |
| `interrupt` | `Query.interrupt()` for captured active query/turn | unknown is read back, never repeated |
| `stop` | close input/query; force only this custody | applied after bound generation ended |
| `resume` | new plugin/query generation with exact native resume id | old user input never resent; system prompt limitations shown |
| `approve|deny` | exact `canUseTool`/permission callback decision | callback waits for exact Eliot request command; stale generation/request rejects |
| `answer` | exact AskUserQuestion callback shape if installed SDK exposes it | otherwise unsupported |
| `configure` | documented live setter or next-turn/session option | report application point/effective result |
| `goal.*` | native Goal only after installed qualification; otherwise Eliot Goal | native and Eliot modes remain separate |

## Event normalization

- init → one `session.ready` before first `turn.started`;
- assistant/user blocks → messages/tools with native block/tool ids, not message id alone;
- partial stream events → ephemeral deltas;
- result subtype → exact terminal turn status; pre-init definite failure may fail launch;
- permission/question callback → exact pending request id;
- `parent_tool_use_id`, subagent hooks and tool result → canonical child registration and
  exact child completion evidence;
- cumulative resumed-session cost is `scope=session`, never summed as a turn delta.

Final assistant text commits before terminal turn. Silence, Stop-hook absence or process
exit does not fabricate completion.

## Hooks

Use in-process hooks only for assigned decisions/injection/missing facts. A synchronous
callback has one native deadline; late result is discarded and never becomes a queued
approval. The later CLI port uses the common session-scoped hook shim.

The documented Stop-hook block cap is native behavior; after native override, report the
fact rather than pretending Eliot still blocks it.

## Transcript and proxy

Transcript path is under the configured Claude project/session store; format/retention
are internal. Parse defensively, use an exact cursor and expose gaps.

Proxy route uses documented base URL/auth/header variables in the per-Agent frozen
profile environment. Explicitly construct/clear environment so an owner's unrelated API
key cannot leak into a proxy route. Requested URL is not proof—report effective route
where available and verify during L7.

## Retained L7 cases

- init-before-input vs start-required-input on installed SDK;
- late callback/event from old generation ignored;
- delayed additionalContext cannot enter the next turn;
- content blocks sharing one message id do not dedupe each other;
- resume never repeats original prompt;
- SDK stream and transcript do not duplicate lifecycle;
- exact permission/question id and child completion;
- proxy/native login route and effective permission/model readback.
