# Muse Code

Layer L4.3. Plugin: `plugins/muse`, `session_mode = "per_agent"`. One plugin instance
owns one Muse host/session process for one Eliot agent generation. The SDK may expose
multiple native subagents inside that session; those are Eliot children, not separate
unowned host processes.

Muse was v1's main swarm route (Muse Max subscription and native subagents), so its
known edge cases remain explicit without importing v1's journals or proof machinery.

Facts checked 2026-10-25 against
[meta-models/muse-code-sdk](https://github.com/meta-models/muse-code-sdk), Muse Code
1.4 documentation and generated MSP method list; npm `@muse-code/sdk` and PyPI
`muse-code-sdk` 1.4.13 (2026-10-24). The repository has no release tags; registries
are the version source. License recorded as MIT in v1 and must be rechecked when the
dependency is added.

## Transport and custody

- Muse Session Protocol v1: JSON-RPC 2.0 NDJSON over the Muse process stdin/stdout.
  The SDK spawns or attaches to that process.
- Official Rust crate `muse-code-sdk` supports spawn/attach, typed MSP, sessions,
  turns, approvals, fold, gap recovery and bounded close. Check current MSRV before
  adding it.
- The high-level SDK may mint a command id inside `send_user_turn`; Eliot must know the
  native id before bytes leave. Use the lower-level call with caller-selected
  `commandId`, or an SDK operation that exposes the id before send.
- SDK and host versions are paired and upgraded together.
- `initialize` returns schema version/fingerprint. Display mismatch as compatibility
  health; versions/fingerprints are not authority gates.

The plugin maps one native session to `(agent_id, generation)`. All MSP commands and
notifications are checked against that generation. Resume starts a new Eliot
generation even when the native session id is reused. `force` affects only this
per-agent custody group.

## Verb mapping

| Eliot verb | MSP | Notes |
| --- | --- | --- |
| `launch` | `session/start`, then optional `turn/start` | Emit one `session.ready` with session id/effective settings before returning success. Lost start reply is read back by native id, never retried as another session. |
| `send` | `turn/start` with caller-known `commandId` | Eliot dispatches only when idle and for the current generation. |
| `steer` | `turn/steer` with exact current turn id | Reject stale/wrong turn before native I/O. |
| `inject` | exact steer while busy; prepend to next send while idle | Report actual mechanism. |
| `interrupt` | `turn/interrupt` for exact active turn | `turn/cancel`/`turn/unqueue` are different semantics. Unknown is not retried. |
| `stop` | clear/pause native goal as required, close session/process; force only this custody group | Reply after the bound generation ended. |
| `resume` | new plugin/host generation + `session/resume` for recorded native session | Never start a fresh unrelated session and never resend original prompt. |
| `approve` / `deny` | `approval/decide` using current offered choice ids | Request key includes native session/child scope and request id. No latest-request guess. |
| `answer` | `userInput/answer` for exact request | |
| `goal.*` | `goal/set`, `goal/edit`, `goal/pause`, `goal/resume`, `goal/clear` | Idle set/edit/resume may start work. Apply configuration first. |
| `configure` | `session/setModel`, `session/setReasoningEffort`, `session/setApprovalMode`, then readback | Report `applies`; host defaults and session effective values stay separate. |
| `compact` / `fork` / `background` | `session/compact`, `session/fork`, `task/background` | Native-only. |

## Observation and source ownership

- The controlled MSP stream owns session/turn/message/tool/request/goal lifecycle.
  `view/page` and output reads are readback/recovery, not a second concurrent event
  producer.
- `view/subscribe` observes; it is not a session load or control lease.
- `view/page` uses exact revision/cursor. `item/readOutput` reads by output reference
  and range. Reuse cursors; never rediscover the full history on each read.
- If a stream gap occurs, page from the last committed cursor. If the installed 1.4
  host cannot prove a complete boundary, report a source gap; never re-emit an old
  `turn.completed`.
- `notLoaded` is one host's view, not proof that the native session ended.
- Children use `subagent/*` methods. Their native ids/provenance become Eliot child
  agents; parent turn completion does not imply child completion.
- Pending-approval snapshots race live updates. Maintain a private monotonically
  increasing **observation epoch** (not Eliot generation): capture it before the
  snapshot and apply only if unchanged. An `approval/updated` without the original
  request is a gap, not a new request.
- `hook/list` exists; use no hook until it proves a fact absent from the primary MSP
  stream, then assign that event family explicitly.

## Billing routes

Muse owns inference and documents no base-URL override; manifest `proxy = "none"`.
Muse Max, Muse Go and pay-as-you-go are different profiles/routes. Eliot reports the
selected/effective route and never silently substitutes another.

## v1 pitfalls retained for L7

- Host/IPC disconnect does not prove session closure. A transport loss after submit is
  unknown and checked by the exact native command id.
- Parent idle/turn end does not finish native subagents; some remained in progress for
  long silent periods.
- Long sessions degraded: one owner-observed GM with seven subagents reached about
  233 MB after 6.5 h and throughput fell materially. Rotation is an explicit rule:
  checkpoint, settle children/requests, then new generation.
- Stale approval choices can leave a child waiting; bind exact request and offered
  choice ids.
- v1 donor paths: `v1-final:modules/muse/bridge.mjs` and
  `v1-final:crates/swarm-adapter-muse`. Read protocol edge cases only; do not port
  journals, tombstones or receipts.
