# Muse Code

Layer L4.3, `plugins/muse`, default `per_agent`. One plugin/host custody unit owns one
Eliot generation; native subagents are child Agent rows, not extra host owners.

Facts checked 2026-10-25 against Muse Code SDK/MSP 1.4 docs/registries. Recheck package,
license and MSRV at implementation.

## Transport and start

MSP v1 is JSON-RPC 2.0 NDJSON. Use a lower-level/caller-known `commandId`; a helper that
mints the id after send is insufficient for unknown readback.

Muse starts a session before the user turn:

1. persist/open authoritative source epochs before native session I/O;
2. initialize + `session/start`;
3. validate native subject/effective model/approval mode;
4. commit-acknowledge `session.ready`;
5. return session outcome;
6. dispatch linked initial `turn/start` separately.

`start_requires_input=false`. Lost session-start and first-turn replies remain separate
unknowns. Resume opens new-generation sources first, then calls `session/resume` on the
retained subject; optional text is a linked send, never implicit replay.

The live MSP source declares the qualified native page/output cursor or stable per-event
ids. Lost source/event reply resends the identical Eliot source sequence. `view/page`
and output references are readback/recovery, not concurrent lifecycle producers. An
unbridgeable cursor transition uses atomic gapped source replacement.

Large system notes/config are ArtifactRefs. Discovery/page/output responses are paged
below plugin frame limits.

## Verb mapping

| Eliot | MSP |
| --- | --- |
| send | `turn/start` with caller-known command id; applied at exact turn association |
| steer | exact `turn/steer` current turn |
| inject | steer while busy or explicit prepend-next-send, mechanism reported |
| interrupt | exact `turn/interrupt`; cancel/unqueue remain distinct |
| stop | pause/clear Goal, close session/process, exact custody only |
| approve/deny | exact session/child/request and currently offered choice ids |
| answer | exact `userInput/answer` |
| configure | model/reasoning/approval setter + application point/readback |
| Goal | native set/edit/pause/resume/clear; idle set/resume may start work |
| compact/fork/background | native method with explicit new subject/result |

## Observation and readback

Controlled MSP stream owns lifecycle. Store source watermark plus native cursor/event
receipts suppress reconnect duplicates after event pruning. If page/output cursor cannot
bridge an interval, replace the source with a visible gap. `notLoaded` is one host view,
not terminal. Reconnect with session but no terminal proof produces unknown turn.

Pending-list snapshot races live updates: capture observation epoch and apply only if no
live update occurred. Update without original request is a gap, not a new request. Final
output commits before turn completion and obeys bounded projection rules.

## Native children and routing

Subagents register from exact native provenance with stable registration id, child
scope/ref and parent generation. Parent turn end never finishes a child. Child/page
queries are bounded and paginated.

Muse SDK/host may multiplex subagents inside one host session. Until an installed
recording proves child-specific environment injection before tools, declare
`child_environment=false`: child row and native controls remain first-class, but
`routing_enabled=false`, child CLI/Git activity is attributed to parent and no separate
token is minted. A vendor child beyond controller guards is still recorded with reduced
authority/wake.

A shared host/service PID is never assigned to child ownership. Per-Agent descendant
registration requires exact custody membership; OS monitor owns process exit/stats.

## Route and custody

Muse owns inference; no qualified proxy override. Max/Go/pay-as-you-go are explicit
owner-defined account/profile routes and are never substituted. Minimal environment
passes named Muse config/auth plus parent Eliot identity only when routing is enabled.

Exact force exists only on an exact platform backend. Transport disconnect is not
session close; exact command/session readback decides outcome.

## L7 cases

- source open committed before initialize/session start/resume;
- lost source-open/ready/initial-turn acknowledgements;
- caller-known command id and unknown readback;
- stale turn/request/choice ids and reconnect missing terminal;
- strict cursor/event-id replay, atomic gap handoff and paging/frame limits;
- approval snapshot race;
- child canonical subject/guard/completion and parent attribution without child env;
- native Goal wake/stop/generation pause and durable Eliot Goal eligibility;
- long-session rotation/resource behavior;
- process registration vs OS enumeration, account isolation and exact/degraded custody.

Read v1 protocol edge cases at the frozen commit only; do not port journals/tombstones/
receipts.
