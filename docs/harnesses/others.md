# Other harnesses (L4.7, only when needed)

Short notes carried over from v1 so nothing is lost. None of these is built until
someone needs it ([defaults](../decisions.md#defaults)).

## Command Code

- Docs: [mods](https://commandcode.ai/docs/mods), [headless](https://commandcode.ai/docs/headless),
  [CLI](https://commandcode.ai/docs/reference/cli), [sessions](https://commandcode.ai/docs/sessions),
  [goal](https://commandcode.ai/docs/goal) (v1 links, not re-checked).
- Batch `cmd -p` with NDJSON output, or ACP (`cmd acp`) → use the [ACP plugin](acp.md).
- v1: a mod exception could leave the CLI running; steer via `queueMessage` applies
  only after a tool batch; goal did not persist across resume; `turn_end` can arrive
  before the agent's last commit (push on `git.commit`, not on `turn.completed`).

## Kilo Code

- OpenCode-derived CLI (`kilo serve`, HTTP + SSE); endpoints mirror OpenCode
  (`/session`, `/session/{id}/prompt_async`, `/session/{id}/abort`,
  `/session/{id}/children`, permission replies). Likely served by the
  [OpenCode plugin](opencode.md) with a different command.
- v1: abort without an explicit scope could stop descendants; queued prompts are
  in-process and lost on restart.
- Kilo sends titles, summaries, compaction and update checks to external services and
  may fall back to Kilo Gateway: disable or explicitly allow those before attaching.
  Observe over HTTP, never with `kilo run`.

## Zed

- Zed is an ACP **client** (editor), not an agent to drive. Its `eval-cli` is a
  one-shot batch evaluator, not a control server (v1 finding). No plugin planned.

## Candidates noted in v1, not integrated

CodeGraff/Harness, Sinew, Pi-Go, OpenChamber, Delta, Agent Teams AI, Vicoa,
JetBrains Air, SWE-agent — see `git show v1-final:docs/candidate-notes.md`.
