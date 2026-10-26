# Muse Code

Layer L4.3, `plugins/muse`, default `per_agent`. One plugin/host process owns one Eliot
generation; native subagents are child Agent rows, not extra host owners.

Facts checked 2026-10-25 against Muse Code SDK/MSP 1.4 docs and registries. Recheck
package version, license and MSRV when adding the dependency.

## Transport/start

MSP v1 is JSON-RPC 2.0 NDJSON. Use a low-level/caller-known `commandId`; a high-level
helper that mints the id after send is insufficient for unknown readback.

Muse supports session start before a user turn:

1. initialize + `session/start`;
2. validate/read session id/effective model/approval mode;
3. emit ready/return session outcome;
4. dispatch optional linked initial `send` with `turn/start` separately.

`start_requires_input=false`. Lost session-start and lost first-turn replies remain
separate unknowns. Resume creates a new Eliot generation and calls `session/resume` on
the retained native subject; optional text is a linked send, never replayed implicitly.

## Verb mapping

| Eliot | MSP |
| --- | --- |
| send | `turn/start` with caller-known command id |
| steer | exact `turn/steer` current turn |
| inject | steer while busy or explicit prepend-next-send; report mechanism |
| interrupt | exact `turn/interrupt`; cancel/unqueue are separate verbs/semantics |
| stop | pause/clear native goal as needed, close session/process, exact custody only |
| approve/deny | `approval/decide` with exact session/child/request and current offered choice ids |
| answer | exact `userInput/answer` |
| configure | session model/reasoning/approval setters plus readback/application point |
| goal | native set/edit/pause/resume/clear; idle set/resume may immediately start work |
| compact/fork/background | native methods, with resulting new subject handled explicitly |

## Observation/source ownership

Controlled MSP stream owns lifecycle. `view/page`, output references and pending lists
are exact readback/recovery, not concurrent lifecycle producers. Store cursor/native
ids suppress reconnect duplicates. If paging cannot bridge a gap, report it.

`notLoaded` is one host view, not terminal evidence. Parent turn end does not finish
subagents. Child canonical scope/ref comes from exact native provenance.

Pending-list snapshot races live updates. Use an observation epoch: apply snapshot only
if no live update occurred since capture. An update without original request is a gap,
not a new request.

## Route/custody

Muse owns inference; no qualified proxy override. Muse Max/Go/pay-as-you-go are explicit
account/profile routes and are never substituted. Minimal environment passes only
named Muse auth/config values plus Eliot token/Git identity.

Exact force is available only on exact platform custody. Transport disconnect does not
close Muse; command-id readback decides outcome where possible.

## L7 cases

- separate session/initial-turn failures;
- caller-known native command id and unknown readback;
- stale turn/request/choice ids;
- paging cursor gaps and approval snapshot race;
- native child completion independent of parent;
- native goal wake/stop/generation pause;
- long-session rotation and resource behavior;
- minimal environment/account isolation and exact/degraded custody.

Read v1 protocol edge cases at frozen commit paths only; do not port journals,
tombstones or receipts.
