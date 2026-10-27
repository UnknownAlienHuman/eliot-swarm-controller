# Muse Code

Optional L6 adapter, implemented when selected. Use the official MSP SDK/transport in
one per-Agent plugin and Muse host; do not make Muse a dependency of the first Codex path.

Source: [meta-models/muse-code-sdk](https://github.com/meta-models/muse-code-sdk).
Inherited 2026-10-25 notes refer to MSP v1 / Muse 1.4. Verify installed host/SDK pairing,
methods, license and toolchain at integration; these are not current runtime findings.

## Native identity and transport

Subject: `(canonical Muse host/store/account scope, exact native session id)`.
MSP uses JSON-RPC NDJSON over stdio. Resume opens the retained session in a new Eliot
generation; callbacks carry that generation. Native children need exact provenance and
remain parent-attributed unless an isolated child environment can actually be installed.

Obtain the native command id before writing: use caller-selected `commandId` or an SDK
operation exposing its generated id before send. Text/time/list position is not identity.
Use supported typed APIs, not an inferred method list from a different host version.

## Native mapping

| Eliot verb | Installed MSP mechanism to verify |
| --- | --- |
| launch/send | session/start then turn/start; readiness, input association and result separate |
| steer/interrupt | exact current native turn controls; cancel/unqueue are different semantics |
| inject | exact supported steer or explicit next-send prepend, mechanism reported |
| stop/resume | clear/pause native Goal as necessary and close owned host; resume exact session without old input |
| approve/deny/answer | exact session/child request id and offered choice/requirement |
| configure | model/reasoning/approval setters followed by effective readback |
| goal | native set/edit/pause/resume/clear where installed schema supports them |
| compact/fork/background | native-only, advertised after implementation |

## Observation and recovery

Controlled MSP stream owns native lifecycle; view/page and output reads are bounded
readback, not a second concurrent producer. Use an exclusive view/revision cursor only
where the installed API guarantees it. Otherwise record a gap and reconcile current
state, never replay full history as new turns.

Guard a pending-request snapshot with a local observation counter so a newer stream
resolution cannot be overwritten by stale snapshot data. This is an in-memory race guard,
not another persisted Agent epoch. `notLoaded` alone does not prove session closure.
Parent completion does not complete children; aggregate usage must be marked explicitly.

## Route and final checks

Do not assume a generic base-URL override. Use explicit native account/billing profiles
and report what the host actually exposes. L7 covers lost replies, exact request/turn
binding, cursor/gaps, stale approval snapshots, child lifetime and long-session behavior.
No adapter-specific journal, proof receipts or secondary workflow model is imported.
