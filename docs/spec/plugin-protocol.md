# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite and
never retries a native effect whose outcome is uncertain.

The protocol borrows three proven boundaries:

- sandbox-driver: each replacement plugin is a numbered generation and old handles are
  dead;
- ACPX/Paseo: one authoritative journal/history cursor, live delivery as an overlay and
  explicit gaps;
- Waku: subscribe before create, exact session demultiplexing, adopted services are not
  killed, reconnect triggers readback rather than synthetic completion.

## Process modes

- `per_agent`: one plugin process and custody unit per controlled Agent generation.
- `shared`: one plugin process talks to a native service that proves exact per-session
  identity, event attribution, readback and isolated stop. Its server process belongs to
  service custody, never an Agent.

Hosting several chats is not sufficient for `shared`. If session environment/identity or
isolated stop is missing, use one service per Agent or observe-only attachment.

## Framing and bounds

- JSON-RPC 2.0 over UTF-8 NDJSON; stdout is protocol, stderr is bounded log text.
- Both directions may issue requests. Request ids are unique per direction.
- Every Agent-scoped message carries `agent_id` and `generation`.
- Protocol version is `eliot-plugin/1`; fields may be added but existing meaning does
  not change.
- Maximum frame 1 MiB. Inline input is at most 512 KiB; persisted event data and final
  text at most 64 KiB; list/history pages at most 1,000 items and below 900 KiB.
- Readers are continuously drained and cancel-safe. A long-lived native reader owns its
  own task; do not cancel a partial line read.
- Credentials and Agent/owner/run tokens are excluded from logs, recordings and plugin
  state.

## Manifest

```json
{
  "name": "codex",
  "version": "0.1.0",
  "protocol": "eliot-plugin/1",
  "harness_version": "codex-cli 0.164.0",
  "available": true,
  "mode": "per_agent",
  "verbs": ["launch", "attach", "send", "steer", "inject", "interrupt", "stop",
            "resume", "approve", "deny", "answer", "configure",
            "goal.set", "goal.pause", "goal.resume", "goal.clear"],
  "child_verbs": ["send", "interrupt"],
  "features": {
    "start_requires_input": false,
    "native_goal": true,
    "native_children": true,
    "child_environment": false,
    "hooks": false,
    "transcripts": true,
    "discover": true,
    "cost": false,
    "isolated_force_stop": true
  },
  "sources": {
    "controlled": {
      "session": "stream", "turn": "stream", "message": "stream",
      "tool": "stream", "request": "stream", "child": "stream",
      "goal": "stream", "usage": "stream", "edit": "stream",
      "provider": "stream"
    },
    "observe": {
      "session": "transcript", "turn": "transcript", "message": "transcript",
      "tool": "transcript", "request": "transcript", "edit": "transcript"
    }
  },
  "proxy": "config"
}
```

The manifest declares available mechanisms. The daemon chooses and commits one owner for
each native family before dispatching start/attach. A hook may replace only a family not
provided by the primary stream, or provide a synchronous decision without also
publishing duplicate lifecycle. Versions are facts, not authority gates.

## Host → plugin requests

| Method | Essential fields | Result |
| --- | --- | --- |
| `initialize` | protocol, frozen plugin config, private instance directory | manifest |
| `agent.start` | agent, generation, frozen profile/plugin binding, workspace, complete target-generation environment, frozen system note, source-owner plan/state, saved cursors, optional `resume_from`, optional linked `initial` | exact native subject, Agent-owned PIDs, effective settings; session and initial outcome remain separate |
| `agent.attach` | agent, generation, exact subject, observe/control mode, frozen profile/plugin binding for control, complete target-generation environment when direct identity is qualified, source-owner plan/state, cursors, known active turn/requests | exact state/readback/PIDs/effective or `not_found` |
| `agent.command` | agent, generation, command id, verb/args, expected turn/request | `applied|rejected|failed|unknown`, mechanism and application point |
| `agent.snapshot` | agent, generation, command ids?, turn ids?, pagination cursor? | exact session/turn/request/command readback in bounded pages |
| `agent.stop` | agent, generation, command id, force | success only after that generation no longer controls the native session |
| `discover` | cursor?, limit≤1000 | bounded native-subject page + next cursor |
| `shutdown` | — | reply, drain owned output, exit |

```text
resume_from = { native_scope, native_ref }
initial = { command_id, text }
Pid = { pid, start_time, role: harness|tool|server|proxy }
```

For a generation-changing start/control attach, the daemon dispatch transaction has
already advanced the Agent fence, revoked the old identity and committed the new random
Agent token **before** this request when the target can receive its own environment. The
complete environment therefore contains only the target generation token. If the native
result is unknown, that token remains bound to the blocked target generation; it is not
rotated again and generation never rolls back. Exact readback may clear it only after
proving no process/session could have received that environment.

A plugin validates generation before native I/O. A stale generation is rejected and is
never redirected to the current session.

## Source plan before native I/O

The daemon commits `source_owners` plus one current `source_state` row per selected source
before `agent.start`/`agent.attach` dispatch. The plugin receives that plan and arms every
selected reader/subscription **before** the first native create/resume/input effect.
Waku's subscribe-before-create pattern is the model.

If the native subject is not yet known, facts are still scoped by Eliot Agent/generation;
`session.ready` later supplies the subject. The plugin buffers only within a bounded
budget. Overflow records an explicit gap and leaves uncertain commands unknown.

### Session-first harness

1. arm selected sources;
2. start/resume native session without user input;
3. establish exact subject/effective settings;
4. commit `session.ready` through `events.put`;
5. return start applied;
6. daemon dispatches the linked input normally.

### Input-created session

1. arm selected sources;
2. receive one linked `initial` command in `agent.start`;
3. perform one native start+input operation;
4. commit `session.ready` and input/turn facts separately;
5. return separate session/initial outcomes.

`session.ready` proves only the session/control path. It never proves input admission or
turn completion. A lost result never causes another start or input.

## Plugin → host durable requests

| Method | Fields | Result |
| --- | --- | --- |
| `events.put` | stable `batch_id`, agent, generation, source, one native-record batch of persisted event fields, optional exclusive record cursor | committed Store seq range and saved cursor |
| `source.gap` | stable `gap_id`, agent, generation, source, owned families, reason, optional from/to cursor metadata | committed daemon `source.gap` boundary; owner unchanged unless paired with handoff |
| `source.state` | agent, generation, optional families | family owners, mechanisms, committed cursor, source state, latest batch receipt and latest gap boundary |
| `source.handoff` | agent, generation, families, expected predecessor, successor/mechanism, predecessor final cursor or explicit gap | new owner map after one committed CAS transaction |
| `process.put` | agent, generation, exact PID/start time/role | committed process identity after custody verification |
| `child` | stable registration id, parent Agent/generation, exact child subject, name/labels?, request own identity? | canonical child Agent/current generation and optional memory-only token |

### Commit-acknowledged native-record batches

One native record that yields several normalized events is one atomic bounded batch. Each
event carries kind/family, stable normalized `native_id`, timestamp, exact turn/request/
input correlations and bounded data. The plugin does **not** choose integrity digests.
The daemon computes one versioned canonical event digest and one canonical batch digest
from semantic fields; transport ids/timing are excluded. The opaque record cursor is
compared separately and advances only with the complete batch.

`batch_id` is stable for that native record: use the vendor record id, or for a
cursor-backed record a domain-separated encoding of `(source,cursor)`. It is not a
monotonic source sequence. The plugin permits at most **one unacknowledged** `events.put`
per `(agent,generation,source)`. Later native facts wait in a bounded spool. Therefore a
lost reply requires only the last batch receipt in `source_state`, not an adapter journal
or an unbounded seen-events table.

The host:

1. verifies current generation and source ownership of every family;
2. verifies exact turn/request identity where applicable;
3. computes canonical event/batch digests;
4. compares `batch_id`, batch digest, cursor and event ids with the last committed batch;
5. accepts only a wholly new batch or the exact duplicate of the last committed batch;
6. commits events, `source_state` last-batch receipt/cursor, reducer/projections and
   derived state together;
7. replies only after commit.

An exact duplicate returns the original seq range. Same `batch_id` with another digest or
cursor, or a mixed new/duplicate event set, is `source_integrity` and advances nothing.
Once the plugin receives the reply it may send the next batch; that next commit replaces
the one-row last-batch receipt. If the plugin dies before receiving the reply, its
replacement reads `source.state` and retries only when the exact batch is still the last
committed receipt.

A source with neither a stable record id nor an exclusive cursor cannot safely retry a
lost `events.put`. It records a gap, reconciles current state and continues at the live
edge instead. A no-cursor source never republishes historical records after reconnect;
`native_id` handles only overlap within a currently committed batch/current live edge.

`events.put` accepts only plugin-owned native families declared in the source plan. A
plugin cannot publish `agent.*`, `session.lost`, `turn.resolved`, command/rule/mail/claim
or process/Git lifecycle. It supplies native `session.ready|ended|error`, exact attach/
snapshot facts and process registration; daemon subsystems derive the rest.

### Gap and handoff

`source.gap` is a daemon-recorded boundary, not plugin lifecycle. `gap_id` is stable for
one exact uncovered interval. The daemon verifies ownership, computes/stores its canonical
digest, updates source state and commits one bounded gap event before replying. Same id
and fields returns the stored result; same id with another boundary/reason or a
conflicting second boundary is `source_integrity`. Gap alone does not change ownership.

At most one unacknowledged source-control operation (`source.gap` or `source.handoff`) is
allowed per source. After a lost reply the plugin calls `source.state` and settles that
boundary before issuing another.

`source.handoff` is the only dynamic owner change. All named families must currently have
the same predecessor. In one transaction the daemon verifies predecessor, records its
final exclusive cursor or explicit gap and installs successor/source state. Successor
publishes only after commit. Lost response is settled through `source.state`: successor
ownership plus supplied boundary proves applied; predecessor ownership proves not
applied; mixed/other state remains unknown. Do not blindly issue another handoff.

This is a small compare-and-swap boundary, not a source epoch/open-id/sequence protocol.

### Processes and children

`process.put` is valid only for a PID inside the calling per-Agent custody. Shared
service PIDs are service-owned. The OS monitor remains lifecycle authority; plugin
registration and OS enumeration upsert one identity/event.

`child` is idempotent by `(parent generation, registration id, native subject)`. Its
commit creates/returns child row/generation and initial source plan before reply. The
plugin buffers child facts until reply, then publishes under the returned identity;
overflow is an explicit child gap. Repeating the tuple returns the same child and, only
while its current generation is unchanged and `child_environment=true`, the same current
memory-only token. Conflicting registration/subject/parent is `source_integrity`.
Plugin never persists raw token. Old-parent-generation retry rejects. Tree limits never
erase a child already created natively.

## Cursors, replay and handoff

A replayable source exposes an opaque **exclusive** cursor. The plugin includes it in the
atomic batch and resumes strictly after the committed value supplied on attach/resume.
The daemon never interprets cursor bytes.

A source without an exclusive cursor has no historical replay contract. After reconnect
it starts at live edge, records a gap, snapshots/readbacks current state and never
publishes old lifecycle history.

Initial ownership is host-committed before native I/O. Planned replacement uses
`source.handoff`; non-atomic close/reopen requires a gap. Live stream normally owns
controlled lifecycle; transcript/history owns observe/repair; hook owns only an assigned
missing fact or immediate decision. Two sources never independently publish the same
completion/request.

## Translation rules

1. Translate one Eliot verb to one documented native mechanism and report it.
2. Unsupported semantics reject before native I/O. Never silently map steer→send,
   resume→fresh unrelated session, answer→default choice or force-stop→shared service.
3. Steer/interrupt require supplied exact active turn; replies require exact request.
4. Transport/inbox acceptance is intermediate. Send applies only at exact command→turn
   association.
5. Unknown native effect is never retried.
6. Effective model/permissions/endpoint are read back; requested values are not proof.
7. Daemon owns queued-send FIFO and unknown input/turn/transition gates.

## Event normalization

- plugin native session facts are `session.ready|ended|error`; `session.lost` is daemon-
  derived after source drain/gap plus exact custody/session evidence;
- `session.ready` precedes first `turn.started` for the generation;
- one native turn produces one start and one terminal completion with exact turn id;
- final assistant message commits before terminal turn;
- tool/request facts retain exact native item/request/turn ids;
- cumulative usage replaces prior value and names its scope;
- 429 is rate limit unless native evidence proves quota;
- process exit/stream disconnect never fabricates completion;
- unknown native events may be `x.<harness>.*` but cannot reduce core state.

## Hooks

Hook config is session-scoped. Shim receives Agent id, generation and token and calls
`hook.event` with exact hook/request id/deadline. A non-default decision is usable only by
that immediate response. Timeout returns safe default (`ask` for decisions, `allow` for
observation); late result is discarded. No durable hook queue. A hook never republishes
lifecycle already owned by stream.

## Lifecycle and custody

- per-Agent plugin/descendants enter Agent Job/process group before native tools run;
- plugin exit closes that control generation; old calls are never rebound;
- replacement must report same provider/native namespace and exact subject;
- shared/adopted service is never killed for one Agent;
- process exit drains authoritative source to EOF/deadline; timeout records a gap before
  daemon reconciliation may emit `session.lost`;
- requests use one absolute deadline; possible effect on timeout is unknown;
- stdout is always drained; persisted events backpressure, ephemeral telemetry may drop
  with visible lag.

## Fake plugin

`plugins/fake` is deterministic and may be shared because sessions are isolated. It
implements session-first and input-created starts, generation/turn/request checks,
commit-ack batches, cursor/gap/state recovery, children, requests, crash/hang/exit and
snapshot readback. Script steps: `say`, `sleep_ms`, `tool`, `approval`, `question`,
`spawn`, `usage`, `crash`, `hang`, `exit`.

## Recordings and conformance

Real native traffic uses bounded sanitized NDJSON. Replay checks method/path/stable
payload fields and can inject hang/exit; CI never calls a model.

The eventual common black-box suite covers manifest/start/input/turn, generation fence,
request binding, batch lost-ack, cursor/gap/handoff, unknown non-retry, custody/attach and
declared capabilities. During L0–L6 add only targeted regressions required to review a
local contract; complete conformance/live runs belong to L7.
