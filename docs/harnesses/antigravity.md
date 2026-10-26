# Antigravity CLI (`agy`)

Layer L4.5. Plugin: `plugins/antigravity`, `session_mode = "per_agent"`. One warm
`agy` process belongs to one Eliot agent generation and its exact process group.

Facts checked 2026-10-25 against official headless/CLI/subagent/permission/hook pages
and `google-antigravity/antigravity-cli` changelog (top entry 1.4.7; pages undated).
Some documented defaults conflict and remain explicit below.

## Install and auth

`agy` comes from the vendor installer and is logged in interactively once; headless
runs use cached credentials. v1 observed Windows binary
`%LOCALAPPDATA%\agy\bin\agy.exe` at version 1.3.3 on 2026-10-14. Do not encode that
path/version as a gate; discover/report the installed binary.

## Transport and binding

```text
agy --input-format stream-json --output-format stream-json
    [--model M] [--mode accept-edits|auto|plan] [--effort E]
    [--conversation <id>]
```

- stdin prompt: one JSON line `{"event":"user","message":{"content":"..."}}`;
  text only. One `result` settles a turn. Closing stdin requests process exit.
- stdout events: `init` once, `step_update`, `result`.
- Stream mode does not accept slash commands or Claude-style control lines; observed
  versions exit 2 when they are sent.
- The plugin binds process/conversation to `(agent_id,generation)`. Inputs and results
  pair by order **inside that one generation/process** because native results do not
  name the input. At most one prompt is in flight.
- Resume starts a new per-agent process and a new Eliot generation with retained
  `--conversation`; late output from the old process is stale and cannot complete it.

## Verb mapping

| Eliot verb | Antigravity | Notes |
| --- | --- | --- |
| `launch` | start process, write optional first prompt | `native_ref` is `conversation_id` from init/updates. Emit `session.ready` after init/effective settings and before start success. A lost initial write/result is unknown; never start another conversation as retry. |
| `send` | next user line after prior terminal result | Native write acceptance is not turn start/completion; keep command/turn correlation locally. |
| `steer` | unsupported | Reject before native I/O. |
| `inject` | prepend to the next send only | Report `mechanism=queued:next_send`; never claim current-turn delivery. |
| `interrupt` | unsupported until an installed recording proves a native exact-turn mechanism | `stop --force` is not interrupt. |
| `stop` | close stdin and wait; force kills this process group | Preserve child handle/exit code. Unknown close/exit is not retried against a replacement generation. |
| `resume` | new process with exact conversation id | No old prompt replay. |
| `approve` / `deny` | no headless request channel; launch policy decides | A soft-denied tool remains a native tool outcome, not an Eliot pending approval. |
| `answer` | unsupported | |
| `configure` | launch flags only | `applies=next_session`; in-stream `/model` is forbidden. |
| `goal.*` | Eliot Goal loop | |

## Events and source ownership

The controlled stdout stream owns session/turn/message/tool lifecycle. Official
example fixtures are protocol examples, not a concurrent runtime source.

| Native | Eliot |
| --- | --- |
| `init` | `session.ready` with effective cwd/tools/permission/model |
| first progress for an admitted prompt | `turn.started` for the locally bound input |
| agent text delta | ephemeral `message.delta` |
| tool update | `tool.started/completed`; scan errors for soft denials |
| subagent info | exact child registration; omitted workspace list = unchanged, empty = cleared |
| pre-init terminal error | `session.error` only when it proves no session/prompt was admitted; otherwise unknown launch outcome |
| terminal `result` | exact bound `turn.completed`: SUCCESS completed; ERROR/INVALID failed; CANCELED/INTERRUPTED interrupted |
| WAITING/RUNNING result-like update | harness-specific nonterminal event, never completion |

If stdout closes while the process remains or a terminal line may have been lost, the
turn/command is unknown. Silence does not complete it. Usage in current result examples
is cumulative session usage: replace, do not sum.

## Config, permissions and children

Settings are under the vendor config directory. Permission rules use documented
allow/deny/ask precedence; `--dangerously-skip-permissions` is an explicit full-access
profile choice and effective init state must show it.

Native subagents are invoked by parent tools and may wake later. Register them by
conversation/provenance ids. Parent result or idle state does not prove child
completion.

Documented conflicts to resolve with installed recordings in L7: print-timeout default,
allowed effort values and headless error exit code.

## Proxy and retained failures

No documented base-URL override was found at the checked date; manifest proxy support
is `none`. Do not invent an API route or silently replace the subscription with an API
provider.

L7 retains these cases:

- `SUCCESS` with a soft-denied tool is not clean success; expose the denial.
- No qualified history API means crash recovery may remain unknown even if
  conversation resume is possible.
- Result/input correlation is valid only while one-at-a-time order and the same
  process generation are intact.
- v1 fixtures at `v1-final:modules/antigravity/fixtures/` are doc-derived, not live
  evidence.
