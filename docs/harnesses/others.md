# Other harnesses (L4.6, only if still used)

Short notes carried over from v1 so nothing is lost. None of these is planned until
the owner confirms it is in use ([decisions](../decisions.md#open-questions-for-the-owner)).

## Muse (Muse Code SDK)

- [meta-models/muse-code-sdk](https://github.com/meta-models/muse-code-sdk), docs
  [1.4](https://meta-models.github.io/muse-code-sdk/1.4/); npm `@muse-code/sdk` and
  PyPI `muse-code-sdk` 1.4.13 (2026-10-24). MIT per v1 records.
- Muse Session Protocol (MSP) v1: JSON-RPC 2.0, one message per line on the agent
  process's stdin/stdout.
- Methods: `turn/start` (sessionId, commandId, input), `view/page`, `item/readOutput`,
  `goal/set|edit|pause|resume|clear` (commandId required) — native goal and
  caller-chosen command ids, so `unknown` outcomes can be checked by command id.
- v1: one inbound pump per connection; parent idle does not close children; answer
  approvals with the current choice ids; long sessions grew large (233 MB in 6.5 h).

## Command Code

- Docs: [mods](https://commandcode.ai/docs/mods), [headless](https://commandcode.ai/docs/headless),
  [CLI](https://commandcode.ai/docs/reference/cli), [sessions](https://commandcode.ai/docs/sessions),
  [goal](https://commandcode.ai/docs/goal) (v1 links, not re-checked).
- Batch `cmd -p` with NDJSON output, or ACP (`cmd acp`) → use the [ACP plugin](acp.md).
- v1: a mod exception could leave the CLI running; steer via `queueMessage` applies
  only after a tool batch; goal did not persist across resume.

## Kilo Code

- OpenCode-derived CLI (`kilo serve`, HTTP + SSE); endpoints mirror OpenCode
  (`/session`, `/session/{id}/prompt_async`, `/session/{id}/abort`,
  `/session/{id}/children`, permission replies). Likely served by the
  [OpenCode plugin](opencode.md) with a different command.
- v1: abort without an explicit scope could stop descendants; queued prompts are
  in-process and lost on restart.

## Zed

- Zed is an ACP **client** (editor), not an agent to drive. Its `eval-cli` is a
  one-shot batch evaluator, not a control server (v1 finding). No plugin planned.

## Candidates noted in v1, not integrated

CodeGraff/Harness, Sinew, Pi-Go, OpenChamber, Delta, Agent Teams AI, Vicoa,
JetBrains Air, SWE-agent — see `git show v1-final:docs/candidate-notes.md`.
