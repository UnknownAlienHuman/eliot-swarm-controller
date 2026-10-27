# Muse Code

Layer L4.3. Plugin `plugins/muse` uses Muse Session Protocol (MSP) and runs
`mode=per_agent`: one plugin and one Muse host per controlled Eliot Agent generation.
Native subagents live inside that host and normally share its environment.

Facts checked 2026-10-25 against `meta-models/muse-code-sdk`, Muse Code 1.4 docs/method
list and registry packages 1.4.13. Recheck current SDK/host pairing and license/MSRV when
adding the dependency.

## Native identity and generation

```text
native_scope = muse:<canonical host/store/account identity>
native_ref   = exact Muse session id
```

First start creates Eliot generation 1. Resume starts a new plugin/host controller and
uses `session/resume` for the same subject as the next generation. Exact current-host
transport recovery may preserve generation; uncertain/new takeover increments it.
Every MSP request/event, child, approval and Goal is wrapped with Eliot generation.

Subagents are canonical child Agents identified by exact native child/session provenance.
Unless the installed API proves a per-child environment, `child_environment=false` and
they receive no independent Eliot token.

## Transport

- MSP v1 JSON-RPC 2.0 NDJSON over Muse process stdio;
- official Rust SDK where it exposes exact ids, otherwise its lower-level connection;
- SDK and host versions are paired and upgraded together;
- initialization reports schema version/fingerprint as compatibility health, not an
  authority gate.

The high-level SDK may mint its own turn command id. Eliot must know the native
correlation id before bytes leave, so use a caller-selected `commandId` path or obtain
the generated id before send. Never treat text/time/list position as correlation.

## Source ownership and recovery

For controlled generations, MSP stream owns session/turn/message/tool/request/child/
Goal/usage lifecycle. `view/page`, output reads and pending-request lists are readback,
not concurrent lifecycle publishers.

Observe-only or recovery history uses Muse's exact view revision/page cursor where the
installed API guarantees exclusive continuation. That opaque cursor commits with each
native-record event batch and resumes strictly after it. If installed Muse cannot bridge
an interval losslessly, record `source.gap`, snapshot current session/turn/request state
and continue at live edge; never replay full history as new lifecycle.

Pending-request snapshot races the live stream. Capture a local observation version
before the snapshot and apply it only if no newer live request/update arrived meanwhile.
This is an in-memory read-coordination counter, **not** another persisted Agent
incarnation/generation. An update without the original request is a gap.

## Verb mapping

Every operation is additionally bound to Eliot Agent generation. Turn controls carry
exact native turn id; approval/question replies carry exact native request plus child
scope where required.

| Eliot verb | MSP mechanism | Settlement |
| --- | --- | --- |
| `launch` | `session/start` without user input | commit `session.ready`; linked prompt is a separate `turn/start` |
| `send` | `turn/start` with caller-known `commandId` | applied at exact turn association; unknown checked by native id/readback |
| `steer` | exact expected-turn steer | wrong/ended turn never receives it |
| `inject` | steer while busy or explicit prepend-to-next-turn | report actual mechanism |
| `interrupt` | exact `turn/interrupt` | cancel/unqueue are different verbs; unknown not retried |
| `stop` | clear/pause native Goal as required, close session/host | force only this Agent custody; applied after generation ended |
| `resume` | new Eliot generation + `session/resume` | old prompt never resent |
| `approve|deny` | `approval/decide` using exact current offered choice ids | key includes session/child scope + request id |
| `answer` | exact `userInput/answer` | stale request rejects |
| `goal.*` | native Goal methods with caller-known command id | idle set/edit/resume may start work; status maps exactly |
| `configure` | model/reasoning/approval setters + readback | report application point/effective values |
| `compact|fork|background` | native MSP methods | native-only declared semantics |

## Observation and children

- `view/subscribe` observes; it is not load/control ownership;
- `view/page` and `item/readOutput` use exact session/revision/output references and are
  bounded/paged;
- `notLoaded` is one host view, not proof of terminal session;
- native subagent methods implement only declared `child_verbs`;
- parent turn completion/idle never means children are done;
- child usage belongs to child unless native API explicitly reports aggregate.

## Billing routes

Muse owns its inference route and documents no generic base-URL override. Muse Max,
Muse Go and pay-as-you-go are explicit different profiles/accounts. Eliot reports the
chosen/effective route and never substitutes another.

## Retained L7 cases

- lost start/turn reply settled by exact native command/session id, never repeated;
- stale Eliot generation/turn/request rejected before MSP I/O;
- stream/page handoff cursor or explicit gap without duplicate lifecycle;
- pending approval snapshot cannot resurrect a resolved request;
- native child remains independently visible after parent turn end;
- long-session memory/throughput degradation and explicit checkpoint/rotation;
- effective model/effort/approval mode and native Goal status readback.
