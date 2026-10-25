# Muse Code

Layer L4.3. Plugin: `plugins/muse`. Muse was v1's main swarm route (Muse Max
subscription, a GM with a pool of 7 subagents), so it comes right after Claude Code.

Facts checked 2026-10-25: [meta-models/muse-code-sdk](https://github.com/meta-models/muse-code-sdk)
("TypeScript SDK and developer documentation for building clients that drive Muse
Code agent sessions over the Muse Session Protocol"), docs for
[Muse Code 1.4](https://meta-models.github.io/muse-code-sdk/1.4/) and the
[method list](https://meta-models.github.io/muse-code-sdk/1.4/generated/msp/methods/);
npm `@muse-code/sdk` and PyPI `muse-code-sdk` 1.4.13 (2026-10-24). The repo has no
release tags; the registries are the version source. MIT per v1 records.

## Transport

- Muse Session Protocol (MSP) v1: JSON-RPC 2.0, one message per line over the Muse
  Code process's stdin/stdout. The SDK spawns or attaches to that process.
- Official Rust crate `muse-code-sdk` (1.4.11 when v1 reviewed it; MIT; Rust 1.97+):
  spawn/attach, typed MSP, sessions, turns, approvals, fold, gap recovery, bounded
  close. Use it for a Rust plugin, with one caveat: its high-level `send_user_turn`
  mints the command id itself, while Eliot wants to know the command id before
  sending (so an `unknown` outcome can be checked later). Use the lower-level
  connection with caller-chosen `commandId`, or record the minted id before the
  request leaves.
- SDK and Muse host versions are paired; upgrade them together.
- `initialize` needs `clientInfo`; its result carries `schema.version` (1) and
  `schema.fingerprint` (`sha256:` + 64 hex). A fingerprint mismatch is a warning to
  show, not an error.

## Verb mapping

| Eliot verb | MSP | Notes |
| --- | --- | --- |
| `launch` | `session/start`, then `turn/start` (`sessionId`, `commandId`, `input`) | |
| `send` | `turn/start` | Arrives only when idle (Eliot queues). |
| `steer` | `turn/steer` with the expected (current) turn id | v1 bridge supported exact steer. |
| `inject` | steer when busy, prepend when idle | |
| `interrupt` | `turn/interrupt` (`turn/cancel` and `turn/unqueue` also exist) | Method names from the 1.4 method list; check params in the generated types. |
| `resume` | `session/resume` for the recorded session | Never a fresh session, never the original prompt again. |
| `approve` / `deny` | `approval/decide` with the request's current choice ids (`approval/listPending` to read) | Key a pending request by (session, kind, native request id) — a bare id collides between root and child. Stale choice ids leave children waiting (v1). |
| `answer` | `userInput/answer` | |
| `goal.*` | `goal/set`, `goal/edit`, `goal/pause`, `goal/resume`, `goal/clear` (`commandId` required; objective non-empty) | Idle set/edit/resume may start a goal turn; pause/clear never do. |
| `configure` | `session/setModel`, `session/setReasoningEffort`, `session/setApprovalMode`, with readback | Effort per turn was fixed upstream in 1.2.1 (issue #6); host `settings.json` defaults are separate. |
| `compact` / `fork` / `background` | `session/compact`, `session/fork`, `task/background` | |

## Observation

- `view/subscribe` (observation, not a session load or lease), `view/page`
  (`sessionId`, `limit`; exact revision), `item/readOutput` (`itemId`, `outputRef`,
  `sessionId`; range reads, capped at 6 MiB per call in the rolling docs). v1 (SDK
  1.3) used `viewCursor/replay` after gaps; it is not in the 1.4 method list — recover
  with `view/page` from the last cursor.
- Use output-ref ids, not URIs; reuse cursors instead of re-walking history for every
  read (v1 had an unbounded rediscovery loop).
- `notLoaded` is one host's view, not proof the session is gone.
- Children: `subagent/*` methods (`interrupt`, `stop`, `resume`, `sendMessage`,
  `readResult`, ...) control native subagents — the plugin's `child_verbs`.
- Pending approvals: a refresh or `approval/listPending` snapshot taken before a
  resolution can resurrect a resolved request or drop a live one; keep a per-session
  generation, bump it on every server message, and apply a snapshot only if the
  generation did not change meanwhile. An `approval/updated` without its original
  request is a gap, not a new question.
- `hook/list` exists; whether Muse hooks can call an external command is unverified.

## Billing routes

Muse Max subscription, Muse Go and pay-as-you-go are different routes. A profile names
one; Eliot never substitutes another ([architecture](../architecture.md#core-rules)
rule 8).

## v1 pitfalls

- A host/IPC disconnect does not close Muse; a transport loss after a submit is
  `unknown` and is checked by its command id.
- Parent turn end does not mean the subagents are done; subagents sat silent for up to
  90 minutes while `inProgress`.
- A GM session with 7 subagents grew to 233 MB in 6.5 h and took a full CPU core;
  throughput dropped from 7.8 to 1–3 finished items per hour late in a session —
  rotate long sessions ([recipe](../spec/automation.md#workflow-recipes)).
- v1 code: `git show v1-final:modules/muse/bridge.mjs` (Node, SDK 1.3.0) and
  `v1-final:crates/swarm-adapter-muse` (retired Rust candidate).
