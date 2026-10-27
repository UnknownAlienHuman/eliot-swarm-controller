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
publishing duplicate lifecycle.

Versions are facts, not authority gates.

## Host → plugin requests

| Method | Essential fields | Result |
| --- | --- | --- |
| `initialize` | protocol, plugin config, private instance directory | manifest |
| `agent.start` | agent, generation, frozen profile, workspace, complete environment, frozen system note, source-owner plan, saved cursors, optional `resume_from`, optional linked `initial` | exact native subject, Agent-owned PIDs, effective settings; session and initial outcome remain separate |
| `agent.attach` | agent, generation, exact subject, observe/control mode, frozen profile for control, source-owner plan, cursors, known active turn/requests | exact state/readback/PIDs/effective or `not_found` |
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

A plugin validates generation before native I/O. A stale generation is rejected and may
never be redirected to the current session.

## Source plan before native I/O

The daemon commits `source_owners` for the target generation before `agent.start` or
`agent.attach` is dispatched. The plugin receives the exact plan and must arm every
selected reader/subscription **before** the first native create/resume/input effect.
Waku's subscribe-before-create pattern is the model.

If the native subject is not yet known, events are still scoped by Eliot Agent and
generation; `session.ready` later supplies the subject. The plugin buffers only within a
bounded in-memory/spool budget until the daemon can commit events. Overflow produces an
explicit gap and leaves uncertain commands unknown.

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

`session.ready` proves only the session/control path. It never proves the input was
admitted or the turn completed. A lost result never causes another start or input.

## Plugin → host durable requests

| Method | Fields | Result |
| --- | --- | --- |
| `events.put` | agent, generation, source, one native-record batch of persisted events, optional exclusive cursor | committed Store `seq` range and saved cursor |
| `source.state` | agent, generation, optional families | current family owners, mechanisms, committed cursors and latest explicit gap boundary |
| `source.handoff` | agent, generation, families, expected predecessor source, successor source/mechanism, predecessor final cursor or explicit gap | new owner map after one committed handoff transaction |
| `process.put` | agent, generation, exact PID/start time/role | committed process identity after custody verification |
| `child` | stable registration id, parent Agent/generation, exact child subject, name/labels?, request own identity? | canonical child Agent/current generation and optional memory-only token |

An `events.put` batch is atomic and bounded. Each event includes `kind`, derived native
family, `native_id`, canonical payload digest, timestamp and data. One native record that
yields several normalized events is one batch; each normalized event gets a distinct
stable id (for example vendor item id + kind/ordinal), and its cursor advances only with
the entire batch.

`events.put` accepts only plugin-owned native families declared in the committed source
plan. A plugin cannot publish `agent.*`, `session.lost`, `turn.resolved`, command/rule/
mail/claim events, process lifecycle or Git facts. It supplies native
`session.ready|ended|error`, exact attach/snapshot facts and process registration;
daemon subsystems derive the rest.

A durable batch that may be resent after a lost acknowledgement **must** provide a stable
`native_id` for every event. Prefer a vendor event/item id. For a cursor-backed record,
the plugin may derive ids from the opaque record cursor plus record-local ordinal/kind.
A source with neither stable ids nor an exclusive cursor cannot safely repeat a lost
`events.put`: it records `source.gap`, reconciles current state and continues at the live
edge instead.

The host:

1. verifies current generation and that `source` owns every family;
2. verifies exact turn/request identity where applicable;
3. compares a repeated native id with the stored canonical payload digest;
4. commits events, cursor, reducer/projections and derived state together;
5. replies only after commit.

Same native id + same digest returns the original `seq`; another digest is
`source_integrity` and advances nothing. A lost reply may resend only an identical batch
whose events all have stable ids.

`source.handoff` is the only dynamic owner-change operation. All named families in one
request must currently have the same predecessor; otherwise split the handoff. The
daemon verifies that predecessor and, in one transaction, records its final exclusive
cursor or explicit `source.gap`, then installs the successor. The successor must not
publish until the reply commits. This is a small compare-and-swap boundary, not a source
epoch/open-id/sequence protocol.

A lost `source.handoff` reply is **not** answered by blindly issuing another handoff. The
plugin or its replacement calls `source.state`: successor ownership with the supplied
boundary proves applied; predecessor ownership proves not applied; any mixed/other state
remains unknown and requires daemon reconciliation. `source.state` is read-only and
paginated/bounded by the normal frame limits.

`process.put` is valid only for a process inside the calling per-Agent custody. Shared
service PIDs are service-owned. The OS monitor remains the lifecycle authority; plugin
registration and OS enumeration upsert one identity/event.

`child` is idempotent by `(parent generation, native subject, registration id)`. Tree
limits never erase a child the harness already created. A distinct token is returned
only when `child_environment=true` and the plugin can inject it before tools run; the
plugin keeps it only in memory.

## Cursors, replay and handoff

A replayable source exposes an opaque **exclusive** cursor. The plugin includes it in the
atomic batch and resumes strictly after the committed value supplied on attach/resume.
The daemon never interprets cursor bytes.

A source without an exclusive cursor has no historical replay contract. After reconnect
it:

1. starts at the native live edge;
2. emits `source.gap` for the uncovered interval;
3. reconciles current state through snapshot/readback;
4. never republishes old lifecycle history.

`native_id` handles bounded live/readback overlap, not indefinite replay after event
retention.

One source owns each family. Initial ownership is committed by the host before native
I/O. Planned dynamic replacement uses `source.handoff`; a non-atomic close/reopen is
allowed only with an explicit gap. No `source_epoch`, `open_id`, `source_seq` or
watermark protocol exists.

A live stream is normally authoritative for controlled lifecycle. Transcript/history is
a repair/observe source. Hook owns only explicitly assigned missing facts or the
synchronous decision. Two sources never independently publish the same completion or
request.

## Translation rules

1. Translate one Eliot verb to one documented native mechanism and report it.
2. Unsupported semantics reject before native I/O. Never silently map steer→send,
   resume→fresh unrelated session, answer→default choice or force-stop→shared service
   kill.
3. `steer`/`interrupt` require the supplied exact active turn; request replies require
   the exact pending request.
4. Native transport/inbox acceptance is intermediate. `send` applies only when the
   plugin associates the command with an exact turn/execution.
5. Unknown native effect is never retried.
6. Effective model/permissions/endpoint are read back; requested values are not proof.
7. Daemon owns queued-send FIFO and the unknown input gate.

## Event normalization

- plugin native session facts are `session.ready|ended|error`; `session.lost` is emitted
  only by daemon reconciliation after source drain/gap plus exact custody/session
  evidence;
- `session.ready` precedes the first `turn.started` for the generation;
- one native turn produces one `turn.started` and one terminal `turn.completed` with
  exact `turn_id`; intermediate statuses are not terminal;
- final `message.agent` is committed before terminal turn result;
- tool/request events retain exact native item/request/turn ids;
- cumulative usage values replace previous values and name their scope;
- 429 is rate limit unless native evidence proves quota exhaustion;
- process exit or stream disconnect is not a fabricated turn/session completion;
- unknown native events may be `x.<harness>.*` but cannot reduce core state.

## Hooks

Hook configuration is session-scoped. The shim receives Agent id, generation and token,
reads bounded native JSON and calls `hook.event` with exact hook/request id and deadline.
A non-default decision is usable only by that immediate hook response. Timeout returns
native safe default (`ask` for decisions, `allow` for observation); a late answer is
discarded and never becomes a queued approval. There is no durable hook queue.

A hook does not publish tool/request lifecycle already owned by the native stream.

## Lifecycle and custody

- per-Agent plugin and descendants enter the Agent Windows Job / Unix process group
  before native tools run;
- plugin exit closes that control generation; remaining exact custody is terminated and
  verified; no call from the dead generation is rebound to a replacement;
- replacement generation must report the same native/provider namespace or attach exact
  subject; otherwise recovery refuses rather than switching account;
- a shared/adopted service is never killed for one Agent;
- process exit races final stdout/native events: drain the authoritative source to EOF or
  a bounded terminal deadline; timeout records a gap, then daemon reconciliation may
  emit `session.lost` from combined evidence;
- requests have one absolute deadline; timeout after possible native I/O is unknown;
- plugin stdout is always drained; persisted events backpressure, ephemeral telemetry may
  be dropped with visible lag.

## Fake plugin

`plugins/fake` is deterministic and may be shared because it isolates sessions. It
implements both session-first and `start_requires_input` modes, exact generation/turn/
request checks, persisted-event commit acknowledgement, cursor recovery, children,
requests, crash/hang/exit and snapshot readback.

A script is `{"steps":[...]}`. Supported steps are `say`, `sleep_ms`, `tool`,
`approval`, `question`, `spawn`, `usage`, `crash`, `hang`, and `exit`. A wrong generation
or turn/request is rejected before any simulated effect.

## Recordings and conformance

Real native traffic uses bounded sanitized NDJSON recordings. Replay checks outgoing
method/path and stable payload fields, ignores generated ids/timestamps and can inject
hang/exit. CI never calls a model.

The eventual conformance suite is one black-box contract, as in sandbox-driver and Agent
Sessions: manifest/start/input/turn, generation fence, request binding, cursor/gap,
unknown non-retry, exact custody/attach and declared capabilities. During L0–L6 add only
targeted regressions required to review a local contract; the complete suite and live
runs belong to final qualification.
