# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite or
retries an uncertain effect.

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
`eliot-plugin/1` allows additive fields only.

## Host to plugin

| Method | Key fields/result |
| --- | --- |
| `initialize` | config/private dir/instance → manifest |
| `agent.start` | target generation, resolved profile/workspace/env/note, resume subject?, linked initial? → session result + separate initial status |
| `agent.attach` | exact subject/generation/control mode + host-known active turn/source watermarks → exact native state, turn/source reconciliation, requests, pids/effective or not-found |
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
attach/snapshot turn readback =
  { turn_id, state: running | completed | not_active_unknown,
    input_commands?: [id], status?, final_text?, usage? }
source watermark =
  { source_key, high_water_seq, last_native_cursor?, state }
```

Most harnesses start without initial input. A plugin requiring input to obtain a native
subject declares `start_requires_input=true`; it receives the linked command in
`initial`. Session and initial command remain separate. Initial `applied` requires exact
native turn/execution association, not merely a write or HTTP inbox acknowledgement.

### Start contract

1. Establish exact native subject ↔ generation.
2. Register the authoritative source owner(s) and obtain daemon-issued source keys.
3. Emit one commit-acknowledged ready when control is possible.
4. If initial is required, admit it once; hold its command reply until exact turn/
   execution association, or return unknown on deadline after possible admission.
5. Return matching session result and separate initial status/turn id.

Ready never proves initial input. Before ready commit, the plugin backpressures the
native reader or keeps a bounded spool. If the spool cannot retain all persisted events,
report a gap, mark the initial effect unknown as needed and report session start from
actual evidence; never falsely fail the whole session or silently drop terminal data.

Launch and resume both use `agent.start`. Resume supplies the same canonical subject and
a new Eliot generation. Old input is never inferred or replayed. Exact recovery attach
to the same live incarnation preserves generation.

### Send and asynchronous inboxes

For `send`, transport/inbox acceptance such as HTTP 204 is an intermediate native fact,
not an Eliot applied reply. The plugin keeps `agent.command` pending until:

- an exact native turn/execution id is associated with that command, then replies
  applied and emits/acks `turn.started` or `turn.inputs`;
- exact readback proves no effect, then replies failed; or
- an absolute deadline expires after possible admission, then replies unknown.

The daemon does not dispatch another send/configuration/goal continuation while input
association is unresolved. A plugin must not return applied early merely to free the
lane. If a native service legitimately associates several Eliot inputs with one
execution, emit one turn plus exact ordered `input_commands`; the Store maps all waits
to that terminal turn.

### Attach and turn reconciliation

The host supplies its known active turn. The plugin must not answer idle merely because
an active list is empty:

- exact same turn running → running;
- exact terminal history/result → completed;
- session exists, turn no longer active, no terminal proof → `not_active_unknown`;
- subject absent → not-found.

Unknown turn keeps the live generation blocked from new turns until exact readback or
owner resolution. The plugin never invents completion or silently starts the next
input.

## Plugin to host requests and notifications

Persisted facts are commit-acknowledged requests:

| Method | Result |
| --- | --- |
| `source.open {agent,generation,source_name,families,mechanism,resume_source_key?,native_cursor?}` | daemon-issued source key + current watermark; validates exclusive family ownership |
| `event.put {agent,generation,source_key,source_seq,kind,native_id?,native_cursor?,ts?,data}` | `{store_seq,duplicate,high_water_seq}` after Store watermark + event/projection commit |
| `process.put {agent,generation,pid,start_time,role}` | committed identity/duplicate ack; host adds boot id |
| `child {parent agent/generation, child scope/ref,...}` | canonical child row/generation/token |
| `source.close {source_key,native_cursor?,state=closed|gap,reason?}` | committed source terminal state/handoff boundary |
| `log` notification | bounded diagnostic; no durability claim |
| `event.ephemeral` notification | best-effort delta/sample only |

The plugin never invents a source key. A normal reconnect reuses the key returned in
host snapshot/attach. A new key is allocated only for a committed ownership epoch or
source handoff, after the old owner is closed/gapped and the family boundary is stored.

### Source sequencing and lost acknowledgements

Each source epoch has a strictly contiguous positive `source_seq`. The plugin durably
assigns the sequence and exact event envelope before sending it and permits at most one
unacknowledged persisted request per source key. It may continue reading native data
into a bounded durable spool, but sends the spool in order.

After a successful reply the plugin durably records the acknowledged watermark/removes
that spool entry **before sending the next sequence**. On restart/attach it reads the
host watermark first and reconciles local state.

The daemon—not the plugin—computes `last_digest` from one shared canonical encoding:
domain-separated, length-prefixed scalar fields plus recursively key-sorted UTF-8 JSON
data with normalized JSON numbers. Retries must carry the same typed envelope; JSON
object insertion order is irrelevant.

The host maintains one compact watermark row per source key:

- next sequence commits and advances the watermark atomically with event/projections;
- resend of the current high-water sequence with the same envelope digest is the one
  possible lost-reply duplicate and returns the saved Store sequence;
- the same sequence with a different digest is an integrity error;
- an older sequence is rejected with the current watermark;
- a future sequence with a hole is rejected as a source gap.

The plugin advances its native cursor only after commit reply. `native_id` and
`native_cursor` still suppress vendor replay; source sequence prevents Store-side
duplicate admission after event-row retention without one receipt row per event.

If the vendor cannot replay/backpressure and the spool is lost or overflows, the plugin
closes the source as gapped and stops claiming complete coverage. It never assigns fresh
source sequences to old history as if current. Source watermark rows remain for the
current generation and are reclaimed only after that generation is superseded and all
old sources/spools are closed.

`child` upserts the canonical native subject; parent generation is provenance.

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

Shared control requires session environment where native tools need Eliot identity and
exact isolated stop/interrupt. `instances=N` is shared-only. Versions are facts, not
authority gates.

## Translation rules

1. Validate generation before native I/O.
2. Steer/interrupt bind exact active turn; request replies bind exact request.
3. Map one semantic verb to one documented mechanism/application point.
4. Reject weaker substitutions and hidden route changes.
5. Unknown is never retried.
6. Preserve command/native input/turn/request/event correlation.
7. The daemon owns queued-send order and generation expiry.
8. Reject oversized or known-secret durable payloads before native I/O.

## Source ownership and normalization

One source owns each family per generation. A live stream normally owns controlled
lifecycle; a transcript owns observe-only/explicit recovery; hooks own assigned missing
facts or synchronous decisions only.

There is one turn start and one terminal completion per native turn. `turn.started`
identifies all known input commands; later exact coalesced inputs use `turn.inputs`.
Intermediate status, inbox acceptance, process exit or silence is not completion. Final
output commits before terminal event, or the plugin delays terminal publication until
output drains.

Reconnect resends only the one unacknowledged source sequence, then events newer than
the committed native cursor. An unbridgeable interval is a gap, never replayed
completion.

## Hooks and transcripts

Hooks are session-scoped with current identity, stable native hook/request id and one
absolute deadline. The shim/SDK callback calls `hook.event`. Eliot returns either a
committed non-dispatchable proposal plus decision, or native-safe `ask` for decision
hooks (`allow` only for observation hooks).

The proposal can affect the native request only through that immediate response. It is
never queued in the ordinary action dispatcher. A response lost before expiry may be
retried with the same hook id and return the same proposal; a late commit/response is
expired and cannot approve later. No Store commit before the internal deadline means
`ask`. There is no durable hook queue.

Transcript readers persist cursors, wait only for an incomplete final line, and report
a complete unparsable line or gap. They never infer end from silence or truncation.

## Custody

- Windows exact: assign-before-resume Job, no breakaway, kill-on-close for owned
  sessions.
- Linux exact: delegated cgroup v2/equivalent plus owner-death guardian; membership,
  group kill and readback prove termination.
- Linux/other Unix fallback: process group plus guardian for non-escaping members,
  degraded; exact force, complete descendant death and daemon-death containment are
  unsupported.

The guardian lives outside the owned group and disarms only after clean empty-group
shutdown. Attached services are never killed; managed services have separate custody.

When process exit races final output, drain the authoritative source to EOF or a bounded
terminal-drain deadline. Deadline creates a gap. Exact backend emptiness or exact shared
session absence proves loss. On degraded backend, disappearance of the exact controlling
plugin/native controller plus authoritative EOF proves session control lost but marks
custody `orphan_possible`; it never claims every descendant died.

## Fake, recordings and L7

Fake is shared and isolated, generation-bound, persists source sequence/spool state and
the minimum exact snapshot/resume state. Recordings are bounded sanitized native NDJSON.
CI never calls a model.

L7 conformance covers start versus initial input, async inbox association/timeout,
multiple input commands per turn, lost `event.put` reply after event-row retention,
source-sequence digest mismatch/hole/stale recovery, source-key allocation/handoff/gap,
reconnect unknown turn, synchronous-hook timeout/late commit, exact bindings,
canonical children, no unknown retry, exact/degraded custody, attach/readback, bounded
buffers and unsupported-before-I/O.
