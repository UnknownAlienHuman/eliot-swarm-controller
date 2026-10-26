# Muse Code

Layer L4.3, `plugins/muse`, default `per_agent`. One plugin/host custody unit owns one
Eliot generation; native subagents are child Agent rows, not extra host owners.

Facts checked 2026-10-25 against Muse Code SDK/MSP 1.4 docs/registries. Recheck package,
license and MSRV at implementation.

## Transport and start

MSP v1 is JSON-RPC 2.0 NDJSON. Use low-level/caller-known `commandId`; a helper that
mints id after send is insufficient for unknown readback.

Muse starts session before user turn:

1. initialize + `session/start`;
2. validate native subject/effective model/approval mode;
3. open authoritative source families and commit ready;
4. return session outcome;
5. dispatch linked initial `turn/start` separately.

`start_requires_input=false`. Lost session-start and first-turn replies remain separate
unknowns. Resume creates new Eliot generation and calls `session/resume` on retained
subject; optional text is linked send, never implicit replay.

MSP source uses stable open id/key, contiguous commit-ack sequence and native page/
output cursor. Lost source or event reply resends identical envelope. `view/page` and
output references are readback/recovery, not concurrent lifecycle sources.

## Verb mapping

| Eliot | MSP |
| --- | --- |
| send | `turn/start` with caller-known command id; applied at exact turn association |
| steer | exact `turn/steer` current turn |
| inject | steer while busy or explicit prepend-next-send, mechanism reported |
| interrupt | exact `turn/interrupt`; cancel/unqueue remain distinct |
| stop | pause/clear goal, close session/process, exact custody only |
| approve/deny | exact session/child/request and currently offered choice ids |
| answer | exact `userInput/answer` |
| configure | model/reasoning/approval setter + application point/readback |
| goal | native set/edit/pause/resume/clear; idle set/resume may start work |
| compact/fork/background | native method with explicit new subject/result |

## Observation and readback

Controlled MSP stream owns lifecycle. Store source watermark/native cursor suppresses
reconnect duplicates. If page/output cursor cannot bridge interval, source closes gapped.
`notLoaded` is one host view, not terminal. Reconnect with session but no terminal proof
produces unknown turn.

Pending-list snapshot races live updates: capture observation epoch and apply only if no
live update occurred. Update without original request is gap, not new request. Final
output commits before turn completion.

## Native children and environment

Subagents register from exact native provenance with stable registration id, child
scope/ref and parent generation. Parent turn end never finishes child.

Muse SDK/host may multiplex native subagents inside one host session. Until installed
recording proves a child-specific process/session environment that can accept distinct
Eliot token and Git identity, declare `child_environment=false`: child row and native
controls are first-class, but child shell/CLI activity is attributed to parent and no
separate token is minted. Any future true claim must prove retry-safe child registration
and token injection before spawn.

A shared host/service PID is never assigned to child ownership. Per-agent descendant
registration requires exact custody membership; OS monitor owns process exit/stats.

## Route and custody

Muse owns inference; no qualified proxy override. Max/Go/pay-as-you-go are explicit
account/profile routes and never substituted. Minimal environment passes named Muse
config/auth plus parent Eliot identity.

Exact force only on exact platform custody. Transport disconnect is not session close;
exact command/session readback decides outcome.

## L7 cases

- lost source-open/ready/initial-turn acknowledgements;
- caller-known command id and unknown readback;
- stale turn/request/choice ids and reconnect missing terminal;
- paging cursor gap, vendor duplicate and approval snapshot race;
- child canonical subject/completion and parent attribution without child env;
- native goal wake/stop/generation pause;
- long-session rotation/resource behavior;
- process registration vs OS enumeration, account isolation and exact/degraded custody.

Read v1 protocol edge cases at frozen commit only; do not port journals/tombstones/
receipts.
