# ACP agents

Layer L4.6. Plugin `plugins/acp` uses the official Rust Agent Client Protocol crate and
runs `mode=per_agent`: one plugin plus one configured ACP subprocess per controlled
Eliot Agent generation.

Facts checked 2026-10-25 against the ACP v1 transport/schema/registry and official crate
releases. Implement v1; v2 was still draft at the checked date. Recheck installed agent
capabilities rather than assuming every ACP agent implements the full schema.

## Native identity and generation

```text
native_scope = acp:<canonical agent command + its config/store identity>
native_ref   = exact ACP session id
```

First `session/new` is Eliot generation 1. `session/load`/resume uses the same native
subject under the next Eliot generation. Exact transport reconnect to the same
still-current ACP process may preserve generation; a replacement process/controller
increments it. Every ACP prompt/update/request/result is wrapped with Eliot generation.

ACP child support is capability-dependent. `child_environment=false` unless an installed
agent exposes a child before its tools run and permits isolated environment injection.

## Transport and source ownership

ACP v1 is JSON-RPC over stdio; Eliot is client, configured Agent is child process. The
plugin installs update/request/response routing and receives the committed source-owner
plan **before** `session/new`, `session/load` or first prompt.

For a controlled generation, the live ACP connection owns session/turn/message/tool/
request/plan lifecycle. It emits no concurrent transcript/log-derived lifecycle.

ACP v1 does not guarantee one generic durable history cursor across agents. When an
installed Agent advertises exact load/history cursor semantics, record/use that opaque
exclusive cursor. Otherwise reconnect/load:

- records a source gap for the uncovered interval;
- reads current session/request/config state where supported;
- treats replayed load updates as historical/readback, not new turns;
- never republishes old history as lifecycle.

Silence, process exit and `session/prompt` transport completion are not product success.

## Verb mapping

All calls are bound to Eliot generation. Cancel binds the exact active prompt/turn;
permission replies use the exact offered request/option ids.

| Eliot verb | ACP v1 | Settlement |
| --- | --- | --- |
| `launch` | initialize → `session/new` without prompt | commit `session.ready`; linked input is separate |
| `send` | `session/prompt` | applied on first exact update/turn association; terminal response settles turn status separately |
| `steer` | unsupported in generic v1 | reject before native I/O |
| `inject` | prepend to next prompt only | report `applies=next_turn`; never current-turn steer |
| `interrupt` | `session/cancel` for exact active prompt | queued-input cancel/session close are different semantics; unknown not retried |
| `stop` | exact `session/close` if advertised, then close process | force targets only this custody; applied after generation ended |
| `resume` | new Eliot generation + advertised `session/load`/resume | old prompt never resent |
| `approve|deny` | exact `session/request_permission` response using one offered option id | never invent generic allow/deny values |
| `answer` | only an advertised exact user-input request/reply shape | otherwise unsupported |
| `configure` | typed config/mode setters where advertised | read back and report application point/effective value |
| `goal.*` | Eliot Goal | no generic ACP native Goal |

`session/update` variants map from the installed schema crate to bounded message/tool/
plan events. The active `session/prompt` plus exact native update/response defines one
turn. A successful RPC return means the native turn ended; it does not prove task
acceptance/success beyond the returned native status.

## Profiles

The profile supplies the concrete Agent command, for example:

```toml
[profiles.gemini-acp]
harness = "acp"
cmd = ["gemini", "--acp"]
```

Known registry examples include Gemini CLI, Claude/Codex adapters and Copilot CLI. Prefer
native Eliot plugins where they expose stronger exact controls.

A non-ACP headless route is **not** an automatic fallback. If needed, implement it as a
separate harness/profile with its own identity, sources, capabilities and recordings.
Failure of ACP remains visible; Eliot never silently switches protocol.

## Retained L7 cases

- update/request route installed before new/load/prompt;
- stale Eliot generation/turn/request rejected before ACP I/O;
- load replay does not create duplicate lifecycle/rule firing;
- no cursor means explicit gap + current-state readback;
- unknown cancel/close/prompt is not repeated against replacement generation;
- permission decision uses offered option id;
- Agent-specific capability absence returns unsupported;
- ACPX/backend replacement never creates two concurrent owners of one native subject.
