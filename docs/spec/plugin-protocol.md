# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite or
retries an uncertain native effect.

## Modes

- `per_agent`: one plugin instance/custody unit per Eliot generation.
- `shared`: one plugin talks to a service with exact per-session identity,
  environment/tool attribution, attach/readback and isolated stop.

A service missing any shared-control prerequisite is per-agent or observe-only. Shared
mode is not selected merely because the vendor hosts multiple chats.

## Framing

JSON-RPC 2.0 over UTF-8 NDJSON, at most 1 MiB per line. stdout is protocol; stderr is
bounded logs. Both directions issue requests. Every scoped message carries agent and
generation. Readers are continuously drained and cancel-safe. Protocol
`eliot-plugin/1` permits additive fields only.

## Host to plugin

| Method | Key fields/result |
| --- | --- |
| `initialize` | config/private dir/instance → manifest |
| `agent.start` | target generation, frozen profile/workspace/env/note, resume subject?, linked initial? → session result + separate initial status |
| `agent.attach` | exact subject/generation/control mode + host-known turn/source watermarks → exact native state, turn/source reconciliation, requests, pids/effective or not-found |
| `agent.command` | command id, exact generation/turn/request, verb/args → applied/rejected/failed/unknown + mechanism/application point |
| `agent.snapshot` | exact generation + command/turn ids → current state/readback + source watermarks |
| `agent.stop` | exact generation/command/force → success only after generation ended |
| `discover` | canonical native subjects |
| `shutdown` | finish owned cleanup and exit |

```text
resume_from = { native_scope, native_ref }
initial = { command_id, text }
start result = {
  native_scope, native_ref, pids, effective,
  initial_status?: applied | rejected | failed | unknown,
  initial_turn_id?: string
}
turn readback =
  { turn_id, state: running | completed | not_active_unknown,
    input_commands?: [id], status?, final_text?, usage? }
source watermark =
  { open_id, source_key, high_water_seq, last_native_cursor?, state }
```

Most harnesses start without initial input. A plugin requiring input to obtain a native
subject declares `start_requires_input=true`; it receives the linked command in
`initial`. Session and initial command remain separate. Initial `applied` requires exact
turn/execution association, not a write or HTTP inbox acknowledgement.

### Start contract

1. Establish exact native subject ↔ generation.
2. Open authoritative sources with stable `open_id`s and obtain daemon source keys.
3. Emit one commit-acknowledged ready when control is possible.
4. If initial is required, admit it once; hold its reply until exact turn association,
   or return unknown on deadline after possible admission.
5. Return matching session result and separate initial status/turn id.

Ready never proves initial input. Before ready commit, the plugin backpressures native
input or uses a bounded spool. Spool loss/overflow reports a gap and preserves actual
session/input uncertainty; it never silently drops terminal data or falsely fails the
whole session.

Launch and resume both use `agent.start`. Resume supplies the same canonical subject and
a new Eliot generation. Old input is never inferred/replayed. Recovery attach to the
same live incarnation preserves generation.

### Send and asynchronous inboxes

Transport/inbox acceptance such as HTTP 204 is an intermediate fact. The plugin keeps
`agent.command` pending until:

- exact native turn/execution association → applied plus `turn.started|turn.inputs`;
- exact readback proves no effect → failed; or
- absolute deadline after possible admission → unknown.

The daemon does not dispatch another send/configuration/goal continuation while
association is unresolved. If one native execution drains several admitted inputs, one
turn carries the ordered `input_commands`; Store maps every wait to that result.

### Attach and turn reconciliation

The host supplies its known active turn. Empty active lists are not idle proof:

- exact turn running → running;
- exact terminal history/result → completed;
- session exists, turn not active, no terminal proof → `not_active_unknown`;
- subject absent → not-found.

Unknown turn blocks new turns until exact readback or constrained owner resolution.

## Plugin to host durable requests

Persisted facts are commit-acknowledged requests:

| Method | Result |
| --- | --- |
| `source.open {open_id,agent,generation,source_name,families,mechanism,native_cursor?}` | idempotent daemon source key + current watermark; exclusive family ownership validated |
| `event.put {agent,generation,source_key,source_seq,kind,native_id?,native_cursor?,ts?,data}` | Store event seq + duplicate/high-water after watermark/event/projection commit |
| `process.put {agent,generation,source_key,source_seq,pid,start_time,role}` | Store event seq + duplicate/high-water after verified process projection and `process.started` commit |
| `child {registration_id,parent agent/generation,child scope/ref,name?,labels?,request_token?}` | canonical child row/generation and, only when supported, the current re-derivable child token |
| `source.close {source_key,native_cursor?,state=closed|gap,reason?}` | idempotent committed terminal/handoff boundary |

Best-effort `log` and `event.ephemeral` remain notifications and make no durability
claim.

### Idempotent source opening

`open_id` is a bounded stable internal id durably chosen by the plugin **before** the
first request for one ownership epoch. The Store has a unique
`(agent,generation,open_id)` record. Same id + same canonical fields returns the same
`source_key` and watermark after a lost reply; any field mismatch is integrity failure.
A handoff/new epoch uses a new open id only after the old source closes or gaps.

The plugin never invents a source key. Normal reconnect reuses the key returned by
snapshot/attach. `source.close` retry is idempotent for the same final state/cursor;
conflicting close data is an integrity error.

### Source sequencing and lost acknowledgements

Every durable `event.put` **and** `process.put` belongs to exactly one source epoch with
a contiguous positive `source_seq`. A process source is normally a separate family/key
from model lifecycle. The plugin durably assigns the sequence and typed envelope before
send and permits at most one unacknowledged durable request per source key. Later facts
may accumulate in a bounded durable spool but are sent in order.

After reply, the plugin durably records the acknowledgement/removes the entry before
sending the next sequence. On restart it reads the host watermark first.

The daemon computes the envelope digest over one versioned/domain-separated canonical
encoding: length-prefixed scalar fields and recursively key-sorted normalized JSON.
Host-derived process `host_boot_id` is projection data, not part of the plugin resend
envelope.

- next sequence commits watermark, event/process projection and normalized event in one
  transaction;
- resend of current high-water with matching digest returns the saved Store seq;
- same sequence/different digest is integrity error;
- older sequence is rejected with current watermark/cursor;
- a hole is rejected until replay or explicit gapped close.

The plugin advances native cursor only after commit reply. `native_id`/cursor suppress
vendor replay; source sequence prevents Store duplicate admission after event-row or
process-history retention.

If vendor replay/backpressure is unavailable and spool is lost/overflows, close gapped
and stop claiming complete coverage. Source watermark rows stay for the current
generation and are reclaimed only after generation supersession and source/spool
closure.

### Native child registration and tokens

`registration_id` is stable across a lost child reply. Canonical child scope/ref is the
identity; retries upsert the same Agent row. Manifest feature `child_environment`
indicates whether the plugin can inject a distinct child environment/token.

When `request_token=true` and that feature is qualified, the daemon returns the token
for the row's current `token_version`. Routing tokens are re-derived from the private
daemon routing key and therefore the same registration retry can return the same raw
token without storing it in SQLite. The plugin durably stores the response before
injecting/spawning the child. If distinct injection is unavailable, token is absent and
child CLI activity is explicitly attributed to the parent.

A different token version is issued only by an explicit routing lifecycle transition
(control reattach/resume), never because a response was lost.

## Manifest

```json
{
  "name": "codex",
  "version": "0.1.0",
  "protocol": "eliot-plugin/1",
  "harness_version": "...",
  "available": true,
  "session_mode": "per_agent",
  "verbs": ["launch","send","steer","inject","interrupt","stop","resume"],
  "child_verbs": ["send","interrupt"],
  "features": {
    "start_requires_input": false,
    "native_goal": true,
    "native_children": true,
    "child_environment": false,
    "hooks": false,
    "transcripts": true,
    "discover": true,
    "cost": false,
    "session_environment": true,
    "isolated_force_stop": true
  },
  "proxy": "config"
}
```

Shared control requires per-session environment where tools need Eliot identity and an
exact isolated stop/interrupt. `instances=N` is shared-only. Versions are facts, not
authority gates.

## Translation rules

1. Validate generation before native I/O.
2. Steer/interrupt bind exact active turn; request replies bind exact request.
3. Map one semantic verb to one documented mechanism/application point.
4. Reject weaker substitutions and hidden route changes.
5. Unknown is never retried.
6. Preserve command/native input/turn/request/event correlation.
7. Daemon owns queued-send order and generation expiry.
8. Reject oversized/known-secret durable payloads before native I/O.

## Source ownership and normalization

One source owns each family per generation. Live stream normally owns controlled
lifecycle; transcript owns observe-only/explicit recovery; hooks own assigned missing
facts or synchronous decisions only.

There is one start and one terminal completion per native turn. `turn.started`
identifies known input commands; later coalesced inputs use `turn.inputs`. Intermediate
status, inbox acceptance, process exit or silence is not completion. Final output
commits before terminal event or publication waits for output drain.

Reconnect resends only the one unacknowledged source sequence, then facts newer than the
committed native cursor. An unbridgeable interval is a gap, never replayed completion.

## Hooks and transcripts

Hooks are session-scoped with current identity, stable native hook/request id and one
absolute deadline. `hook.event` returns either a committed non-dispatchable proposal or
native-safe `ask` (`allow` only for observation hooks). The proposal can act only
through that immediate response; late commit/response expires and never approves later.
There is no durable hook queue.

Transcript readers persist cursors, wait only for an incomplete final line, and report
complete unparsable input/gaps. They never infer end from silence/truncation.

## Custody

- Windows exact: assign-before-resume Job, no breakaway, kill-on-close for owned
  sessions.
- Linux exact: delegated cgroup v2/equivalent plus owner-death guardian; membership,
  group kill and readback prove termination.
- Linux/other Unix fallback: process group plus guardian for non-escaping members,
  degraded; exact force, complete descendant death and daemon-death containment are
  unsupported.

Guardian lives outside the owned group and disarms only after clean empty shutdown.
Attached services are never killed; managed services have separate custody.

When process exit races final output, drain authoritative source to EOF or terminal
deadline. Deadline creates a gap. Exact backend emptiness or exact shared-session
absence proves loss. On degraded backend, controlling process/channel loss may prove
session control lost while custody remains `orphan_possible`; it never proves every
descendant died.

## Fake, recordings and L7

Fake is shared/isolated, generation-bound and persists source open ids, keys, sequences,
spool state and exact session snapshots. Recordings are bounded sanitized native NDJSON.
CI never calls a model.

L7 conformance covers lost `source.open|close|event.put|process.put|child` replies,
source digest mismatch/hole/stale recovery, process-history pruning, child-token retry,
start versus initial input, async inbox association, coalesced inputs, source handoff/
gap, unknown turn, hook timeout, exact bindings, no unknown retry, custody, readback,
bounded buffers and unsupported-before-I/O.
