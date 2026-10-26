# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite or
retries an uncertain native effect.

## Modes

- `per_agent`: one plugin instance and custody unit per Eliot generation.
- `shared`: one plugin talks to a service with exact per-session identity,
  environment/tool attribution, attach/readback and isolated stop.

A service missing any shared-control prerequisite is per-agent or observe-only. Hosting
multiple chats is not sufficient for shared control.

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
| `agent.attach` | exact subject/generation/control + host-known turn/source watermarks → native state, turn/source reconciliation, requests, agent-owned PIDs/effective or not-found |
| `agent.command` | command id, exact generation/turn/request, verb/args → applied/rejected/failed/unknown + mechanism/application point |
| `agent.snapshot` | exact generation + command/turn ids → current state/readback + source watermarks |
| `agent.stop` | exact generation/command/force → success only after generation ended |
| `discover` | canonical native subjects |
| `shutdown` | finish owned cleanup and exit |

```text
resume_from = { native_scope, native_ref }
initial = { command_id, text }
start result = {
  native_scope?, native_ref?, agent_owned_pids, effective?,
  session_status: applied | failed | unknown,
  initial_status?: applied | rejected | failed | unknown,
  initial_turn_id?: string
}
turn readback = {
  turn_id, state: running | completed | not_active_unknown,
  input_commands?: [id], status?, final_text?, usage?
}
source watermark = {
  open_id, source_key, high_water_seq, last_native_cursor?, state
}
```

## Start and initial input

`source.open` is Store-only and keyed by Eliot agent/generation, so authoritative
sources are opened **before any native start/input effect**, even if the native subject
is not known yet. This gives the first native frame a durable sequence/spool path.

### Session-first harness (`start_requires_input=false`)

1. open source epochs with stable `open_id`s;
2. start/resume the native session without user input;
3. establish exact native subject and effective settings;
4. commit-acknowledge `session.ready` and return session applied;
5. daemon dispatches the linked initial `send` normally.

### Input-created session (`start_requires_input=true`)

Some harnesses reveal/create the native subject only when the first user input is sent.
For them:

1. open provisional source epochs before native I/O;
2. receive exactly one linked `initial` command from the daemon;
3. perform exactly one native start+input operation bound to that command;
4. spool every resulting native fact under the opened source epoch;
5. when the native subject/control channel becomes known, commit `session.ready` for the
   session outcome;
6. then commit the buffered `input.admitted`/turn association facts and report the
   separate initial-command outcome.

`session.ready` still proves only session control, never input association. Valid outcome
combinations include session applied + initial unknown, session applied + initial
applied, and both unknown after an ambiguous transport loss. A definite pre-I/O failure
fails both; a definite session creation with exact pre-turn input rejection applies the
session and fails only the initial command. The plugin never sends a replacement input
or replacement session to resolve uncertainty.

Before ready commits, the plugin backpressures native input or uses a bounded durable
spool. Loss/overflow reports a source gap and preserves actual uncertainty; it never
silently drops terminal data or falsely converts an ambiguous effect into failure.

Launch and resume both use `agent.start`. Resume supplies the same canonical subject and
a new Eliot generation. Old input is never inferred or replayed. Recovery attach to the
same live incarnation preserves generation.

For every later `send`, transport/inbox acceptance such as HTTP 204 is intermediate
only. The plugin keeps `agent.command` pending until exact turn association (applied),
exact no-effect readback (failed), or deadline after possible admission (unknown). One
native execution may drain several admitted inputs; emit one ordered
`input_commands` mapping. The daemon blocks later send/configure/Goal continuation while
association is unresolved.

## Attach and turn reconciliation

The host supplies its known active turn. Empty active lists are not idle proof:

- exact same turn running → running;
- exact terminal history/result → completed;
- session exists, turn not active, no terminal proof → `not_active_unknown`;
- native subject absent → not-found.

Unknown turn blocks new turns until exact readback or constrained owner resolution.

## Plugin to host durable requests

| Method | Result |
| --- | --- |
| `source.open {open_id,agent,generation,source_name,families,mechanism,native_cursor?}` | idempotent source key + watermark; family ownership validated |
| `event.put {agent,generation,source_key,source_seq,kind,native_id?,native_cursor?,ts?,data}` | Store event seq + duplicate/high-water after atomic commit |
| `process.put {agent,generation,source_key,source_seq,pid,start_time,role}` | verified per-agent descendant registration + duplicate/high-water |
| `child {registration_id,parent agent/generation,child scope/ref,name?,labels?,request_token?}` | canonical child row/generation and optional current re-derivable token |
| `source.close {source_key,native_cursor?,state=closed|gap,reason?}` | idempotent committed terminal/handoff boundary |

`log` and `event.ephemeral` are best-effort notifications and make no durability claim.

### Source opening and sequencing

The plugin durably chooses bounded `open_id` before first open request. Store uniquely
maps `(agent,generation,open_id)` to one daemon source key. Same id and canonical fields
return the same key/watermark after a lost reply; mismatch is integrity failure. A new
handoff epoch uses a new open id only after the old source closes or gaps.

Every durable `event.put` and valid per-agent `process.put` belongs to one source epoch
with contiguous positive `source_seq`. The plugin durably assigns the exact typed
envelope before sending and permits at most one unacknowledged durable request per key;
later facts may accumulate in a bounded ordered spool.

After the reply, the plugin durably records the acknowledgement/removes the entry before
sending the next sequence. On restart it first reads the host watermark. The daemon
uses one versioned canonical digest over length-prefixed scalar fields and recursively
key-sorted normalized JSON.

- next sequence commits watermark, event/process projection and reducer changes in one
  transaction;
- resend of current high-water with matching digest returns the saved Store seq;
- same sequence with another digest is integrity failure;
- older sequence is rejected with current watermark/cursor;
- a future sequence with a hole is rejected until replay or explicit gapped close.

A well-formed vendor duplicate or stale turn/request may advance the source sequence
while returning the original event seq or a bounded diagnostic. It cannot fire normal
lifecycle twice. A malformed envelope or undeclared family does not advance.

The plugin advances native cursor only after commit reply. Native event ids/cursors
suppress vendor replay; source sequence prevents duplicate Store admission after event
or process-history retention. An unbridgeable interval closes the source gapped.

### Process registration and ownership

`process.put` is valid only for a PID inside the calling per-agent plugin's Agent custody.
The host pins birth identity and verifies membership before accepting. The OS monitor is
the sole authority for process start/exit/stats; plugin registration and host enumeration
of one identity are idempotent and publish one start event.

A shared plugin never assigns its shared server or uncontained service child to an
Agent. Managed/attached service PIDs belong to `service:<name>`. Session-attributed tool
PID information may be diagnostic only; it cannot change custody or make Agent stop
kill the service.

### Native child registration and routing tokens

`registration_id` is stable across lost replies. Canonical child scope/ref is identity;
retries upsert the same row. Existing subject with conflicting parent/root/project/
harness is integrity failure. Manifest `child_environment` states whether the plugin can
inject a distinct child environment before child tools run.

When `request_token=true` and that capability is qualified, the daemon derives the raw
routing token for the row's current `token_version`. Retrying the same registration
returns the same token without plaintext in SQLite.

**The raw token is never written to plugin state, recordings, logs or source spools.**
The plugin persists only nonsecret registration data: registration id, child subject,
Agent id, generation and token version. Immediately before native environment injection
it repeats the same idempotent child request, holds the returned token only in memory,
injects it, and promptly drops the reference (zeroizing owned buffers where the runtime
permits). A crash or lost reply before injection repeats the request; it does not mint a
new version or persist the credential. If injection-before-tools cannot be reliable,
`child_environment=false`, no token is returned and child CLI/Git activity is attributed
to the parent.

A new token version is issued only by an explicit routing-lifecycle transition such as
control reattach or resume, never by response loss.

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

## Translation and normalization rules

1. Validate generation before native I/O.
2. Steer/interrupt bind the exact active turn; request replies bind the exact request.
3. Map one semantic verb to one documented native mechanism/application point.
4. Reject weaker substitutions and hidden route changes.
5. Unknown is never retried.
6. Preserve command/native input/turn/request/event correlation.
7. Daemon owns queued-send order and generation expiry.
8. Reject oversized or known-secret durable payloads before native I/O.

One source owns each normalized family per generation. Live stream normally owns
controlled lifecycle; transcript owns observe-only/explicit recovery; hooks own assigned
missing facts or synchronous decisions only. Final output commits before terminal turn
publication. Process exit, inbox acceptance, silence and intermediate states are never
completion. Reconnect resends only the one unacknowledged sequence, then facts newer
than the committed native cursor.

## Hooks and transcripts

Hooks are session-scoped with exact identity, stable native hook/request id and one
absolute deadline. `hook.event` returns a committed non-dispatchable proposal or
native-safe `ask` (`allow` only for observation). A proposal acts only through that
immediate response; late commit/response expires and can never approve later. There is
no durable hook queue.

Transcript readers persist cursors, wait only for an incomplete final line and report
complete unparsable input/gaps. They never infer end from silence or truncation.

## Custody

- Windows exact: assign-before-resume Job, no breakaway, kill-on-close for owned
  sessions.
- Linux exact: delegated cgroup v2/equivalent plus owner-death guardian; membership,
  group kill and readback prove termination.
- Linux/other Unix fallback: process group plus guardian for non-escaping members,
  degraded; exact force, complete descendant death and daemon-death containment are
  unsupported.

Attached services are never killed; managed services have separate custody. When
process exit races final output, drain the authoritative source to EOF or deadline.
Deadline creates a gap. Exact backend emptiness or exact shared-session absence proves
loss. On degraded backend, controller/channel loss may prove session control lost while
custody remains `orphan_possible`; it never proves every descendant died.

## Fake, recordings and L7

Fake is shared/isolated, generation-bound and persists source open ids, keys, sequences,
spool state and exact session snapshots. Recordings are bounded sanitized native NDJSON
and exclude every raw routing token/credential. CI never calls a model.

L7 conformance covers both start branches and every lost session/input reply,
`session.ready` without input proof, lost `source.open|close|event.put|process.put|child`
replies, source digest mismatch/hole/stale/vendor duplicate, process registration versus
OS enumeration/history pruning, rejection of shared-service PID ownership, child parent
conflict and memory-only retry-safe token injection, async inbox/coalesced inputs,
source handoff/gap, unknown turn, hook timeout, exact bindings, no unknown retry,
custody, readback, bounded buffers and unsupported-before-I/O.
