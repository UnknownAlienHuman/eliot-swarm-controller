# Antigravity CLI (`agy`)

Layer L4.5. Plugin: `plugins/antigravity` (one warm `agy` process per agent).

Facts checked 2026-10-25 against [headless](https://antigravity.google/docs/cli/headless/),
[CLI reference](https://antigravity.google/docs/cli/reference/),
[subagents](https://antigravity.google/docs/subagents/),
[permissions](https://antigravity.google/docs/permissions?tab=cli),
[hooks](https://antigravity.google/docs/hooks/) and the
[changelog](https://github.com/google-antigravity/antigravity-cli/blob/main/CHANGELOG.md)
(top entry 1.4.7; pages undated). The docs disagree on a few defaults (noted below).

## Install and auth

`agy` comes from the vendor installer and is logged in interactively once; headless
runs then use the cached credentials. v1 observed it on Windows at
`%LOCALAPPDATA%\agy\bin\agy.exe` (version 1.3.3, 2026-10-14).

## Transport

- `agy --input-format stream-json --output-format stream-json [--model M] [--mode
  accept-edits|auto|plan] [--effort E] [--conversation <id>]`.
- stdin: one JSON line per prompt, `{"event":"user","message":{"content":"..."}}`;
  only text blocks. The process stays up and reuses the conversation; one `result`
  per turn. Closing stdin exits 0. Do not combine `-p` with stream input.
- stdout: lines with an `event` field — `init` (once), `step_update`, `result`.
- Unsupported in stream mode: slash commands (`/model`, `/usage`) and Claude-style
  `control_request` lines — they end the process with exit code 2.

## Verb mapping

| Eliot verb | Antigravity | Notes |
| --- | --- | --- |
| `launch` | start `agy` with the flags above, write the first prompt | `native_ref` = `conversation_id` from `init`/`step_update`. |
| `send` | write the next `user` line | Arrives only after `result` (Eliot queues). `applied` means written to stdin, not started. |
| `steer`, `inject` | not available → `rejected` (or `inject` = prepend to next send) | |
| `interrupt` | not documented → `rejected` until verified; `stop --force` kills | |
| `stop` | close stdin (graceful), then kill after a timeout | Keep the child handle until its exit status is read; if it is gone without one, the status is `unknown`, not `0`. |
| `resume` | new process with `--conversation <id>` | |
| `approve` / `deny` | none in headless mode: policy decides, a tool needing approval is soft-denied and the turn still ends `SUCCESS` | Configure `permissions.allow/deny/ask` up front; report soft denials as `approval.resolved decision=deny`. |
| `configure` | launch flags only (`applies: next_session`) | `/model` in-stream kills the session. |
| `goal.*` | Eliot Goal loop | |

## Events

| Native | Eliot |
| --- | --- |
| `init` (`cwd`, `tools`, `permission_mode`, model) | `session.ready`; the model in `init` is the effective model |
| `step_update` `user_input` / `agent_response` (`text_delta`) / `tool` (`tool_info`, `error`) / `checkpoint` | `turn.started`, `message.delta`, `tool.*`, `approval.resolved` for soft denials |
| `step_update.subagent_info.subagents[]` (`conversation_id`, `role`, `type_name`, `workspace_uris`) | `child` registration. An omitted `workspace_uris` means unchanged; `[]` means cleared. |
| `result` before any `init` (e.g. unknown model) | `session.error` (launch failed) with the native error; no `conversation_id` is invented |
| `result` (`status`, `usage`, `num_turns`) | `turn.completed`: SUCCESS → `completed`; ERROR, INVALID → `failed`; CANCELED, INTERRUPTED → `interrupted`; WAITING, RUNNING are not turn ends (forward as `x.antigravity.result`). A `result` names no input: pair results with inputs by order within the process. If stdout closes while the process lives, in-flight sends are `unknown`. |

Usage counters in `result` are cumulative over the session: replace, never sum.
`/usage` is an interactive view, not a machine-readable quota source.

## Config, permissions, subagents

- Settings: `~/.gemini/antigravity-cli/settings.json`; `permissions.allow`/`deny`/`ask`
  rules like `command(git)`, `write_file(src/)`; precedence Deny > Ask > Allow.
  `--dangerously-skip-permissions` approves everything (init shows `always-proceed`).
- Subagents: Markdown files with frontmatter in `.agents/agents/<name>.md` or
  `~/.gemini/config/agents/`; invoked by the parent through `invoke_subagent`. Idle
  children can wake again on a message.
- Docs conflicts: `--print-timeout` default (unlimited vs 5 m), `--effort` values,
  exit code 3 on headless error (changelog yes, headless page no).

## Proxy

No documented base-URL override for `agy` (checked 2026-10-25). Proxy support:
`unsupported` in the manifest until one is documented.

## v1 pitfalls

- `SUCCESS` can hide soft-denied tools; always scan `tool_info.error`.
- No documented history API: after a crash, a new process cannot read what the old
  one was doing beyond `--conversation` resume.
- v1 fixtures transcribed from the official examples:
  `git show v1-final:modules/antigravity/fixtures/` (not live captures).
