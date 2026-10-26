# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite or
retries an uncertain native effect.

## Modes

- `per_agent`: one plugin instance and custody unit per Eliot generation.
- `shared`: one plugin talks to a service with exact per-session identity,
  environment/tool attribution, attach/readback and isolated stop.

A service missing any shared-control prerequisite is per-agent or observe-only. Hosting
multiple chats is not sufficient for shared control.

## Framing and bounds

JSON-RPC 2.0 over UTF-8 NDJSON, at most 1 MiB per line. stdout is protocol; stderr is
bounded logs. Both directions issue requests. Every scoped message carries Agent and
generation. Readers are continuously drained and cancel-safe. Protocol
`eliot-plugin/1` permits additive fields only.

To keep every accepted semantic payload representable inside the frame:

- inline user input: at most 512 KiB UTF-8;
- resolved environment block: at most 128 KiB encoded;
- remaining start/profile metadata: at most 128 KiB encoded;
- persisted event `data` and projected final text: at most 64 KiB each;
- discovery/history/snapshot page: at most 1,000 items and at most 900 KiB encoded;
- immutable system notes/scripts/config are `ArtifactRef {digest,path,size}`, never
  large inline bytes.

A plugin rejects an oversized request before native I/O. Native output exceeding a
persisted bound is explicitly truncated with a safe native/artifact pointer; it is not
silently cut. Raw secrets and routing tokens are excluded from recordings, spools and
logs.

## Host to plugin

| Method | Key fields/result |
| --- | --- |
| `initialize` | config/private dir/instance credential → manifest |
| `agent.start` | target generation, frozen binding/ref, workspace, bounded resolved env, artifact refs, resume subject?, linked initial? → session result + separate initial status |
| `agent.attach` | exact subject/generation/control + frozen attach binding + host-known turn/source watermarks → native state, turn/source reconciliation, requests, agent-owned PIDs/effective or not-found |
| `agent.command` | command id, exact generation/turn/request, verb/args → applied/rejected/failed/unknown + mechanism/application point |
| `agent.snapshot` | exact generation + command/turn ids + page cursors → current state/readback + bounded pending/history pages/source watermarks |
| `discover` | `{cursor?,limit<=1000}` → bounded native-subject page + next cursor |
| `agent.stop` | exact generation/command/force → success only after generation ended |
| `shutdown` | finish owned cleanup and exit |

```text
ArtifactRef = { digest, path, size }
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
  inputs?: [{command_id,native_input_id?}], status?, final_text?, usage?
}
source watermark = {
  open_id, source_key, high_water_seq, last_native_cursor?, state, dedupe_mode
}
```

## Start and initial input

Authoritative source epochs open **before any native session/input effect**, even when
the native subject is not known yet. `source.open` is keyed by Eliot Agent/generation,
so a provisional epoch is valid before vendor identity exists.

### Session-first harness (`start_requires_input=false`)

1. plugin persists stable open ids and calls `source.open` for every initial family;
2. after open acknowledgement, start/resume the native session without user input;
3. establish exact native subject and effective settings;
4. commit-acknowledge `session.ready`;
5. return session applied;
6. daemon dispatches linked initial `send` normally.

### Input-created session (`start_requires_input=true`)

1. open provisional source epochs before native I/O;
2. receive exactly one linked `initial` command;
3. perform exactly one native start+input operation bound to it;
4. spool every native fact under the opened epoch;
5. once subject/control is known, commit `session.ready` for session outcome;
6. commit buffered input/turn facts and report the separate initial outcome.

Ready proves only session control. Valid combinations include session applied + initial
unknown, session applied + initial applied, and both unknown after ambiguous transport
loss. Definite pre-I/O failure fails both; definite session creation plus exact pre-turn
input rejection applies session and fails only initial. No replacement input/session is
sent to resolve uncertainty.

Before ready commits, plugin backpressures native input or uses the bounded durable
spool. Overflow closes the source with a visible gap and preserves actual uncertainty;
it never silently drops terminal data or converts ambiguity into failure.

Launch and resume both use `agent.start`. Resume supplies the same canonical subject and
a new Eliot generation/binding. Old input is never inferred or replayed. Recovery attach
to the same live incarnation preserves generation.

For later `send`, transport/inbox acceptance such as HTTP 204 is intermediate. The
plugin keeps `agent.command` pending until exact turn association (applied), exact
no-effect readback (failed), or deadline after possible admission (unknown). A native
execution may consume several inputs; publish one ordered `inputs` mapping. A native
input id is optional when the vendor exposes only exact command→turn association.

## Attach and turn reconciliation

The host supplies its known active turn and source watermarks. Empty active lists are
not idle proof:

- exact same turn running → running;
- exact terminal history/result → completed;
- session exists, turn not active, no terminal proof → `not_active_unknown`;
- native subject absent → not-found.

Control attach additionally validates the frozen profile/route/workspace/permission
binding and installs/rebinds the exact per-session environment where required. Failure
leaves the canonical row observe-only. Unknown turn blocks new turns until exact
readback or constrained owner resolution.

## Plugin to host durable requests

| Method | Result |
| --- | --- |
| `source.open {open_id,agent,generation,source_name,families,mechanism,dedupe_mode,native_cursor?,replaces?}` | idempotent source key/watermark; initial ownership or atomic predecessor handoff |
| `event.put {agent,generation,source_key,source_seq,kind,native_id?,native_cursor?,ts?,data}` | Store event seq + duplicate/high-water after atomic commit |
| `process.put {agent,generation,source_key,source_seq,pid,start_time,role}` | verified per-agent descendant registration + duplicate/high-water |
| `child {registration_id,parent agent/generation,child scope/ref,name?,labels?,request_token?}` | canonical child row/generation/routing capability and optional current memory-only token |
| `source.close {source_key,native_cursor?,state=closed|gap,reason?}` | idempotent terminal boundary when no replacement transaction is requested |

```text
replaces = {
  source_key,
  expected_high_water,
  final_native_cursor?,
  state: closed | gap,
  reason?
}
```

`log` and `event.ephemeral` are best-effort notifications and make no durability claim.

### Source opening, ownership and sequence

The plugin durably chooses bounded `open_id` before first open request. Store uniquely
maps `(agent,generation,open_id)` to one source key. Same id and canonical fields return
the same key/watermark after a lost reply; mismatch is integrity failure.

Initial open may claim only unowned families. Planned replacement supplies `replaces`.
One Store transaction verifies predecessor key/high-water/current family ownership,
closes or gaps it at the named cursor boundary, creates the new epoch and transfers all
named families. Therefore no moment has two owners or none. Separate `source.close` then
`source.open` is allowed only when the boundary is explicitly gapped.

Every durable `event.put` and valid per-agent `process.put` belongs to one epoch with
contiguous positive `source_seq`. Plugin durably assigns the exact typed envelope before
sending and permits at most one unacknowledged request per key; later facts accumulate in
a bounded ordered spool.

After reply, plugin durably records acknowledgement/removes entry before sending next.
On restart it first reads host watermark. Daemon uses one versioned canonical digest over
length-prefixed scalars and recursively key-sorted normalized JSON.

- next sequence commits watermark plus receipt/event/process/projection/reducer in one
  transaction;
- resend of current high-water with matching digest returns saved result;
- same sequence with another digest is integrity failure;
- older sequence rejects with watermark/cursor;
- future sequence with a hole rejects until replay or gapped replacement.

### Vendor replay modes

Each source declares:

- `strict_cursor`: vendor guarantees total-order resume strictly after committed cursor;
- `event_id`: every persisted lifecycle fact supplies stable per-event `native_id`;
- `none`: no lossless replay claim; reconnect/handoff must expose a gap.

In `event_id` mode Store keeps a compact native receipt independent of event retention.
A replay with matching `(agent,generation,kind,native_id,payload_digest)` returns the
original Store seq and may advance the new source sequence without publishing lifecycle
again. A digest mismatch is integrity failure. In `strict_cursor` mode every successful
put advances the committed native cursor. `none` never replays historical frames.

A well-formed stale turn/request may advance source sequence while returning one bounded
diagnostic. Malformed envelope, missing required native identity or undeclared family
does not advance.

### Process registration and ownership

`process.put` is valid only for a PID inside the calling per-agent plugin's Agent custody.
Host pins birth identity and verifies membership before accepting. OS monitor is sole
authority for process start/exit/stats; plugin registration and host enumeration of one
identity are idempotent and publish one start event.

A shared plugin never assigns its server or uncontained service child to an Agent.
Managed/attached service PIDs belong to `service:<name>`. Session-attributed tool PID
information is diagnostic only; it cannot change custody or make Agent stop kill service.

### Native child registration and routing

`registration_id` is stable across lost replies. Canonical child scope/ref is identity;
retries upsert the same row. Existing subject with conflicting parent/root/project/
harness is integrity failure.

Every already-created native child is recorded. If controller depth/tree/fanout guards
are exceeded, daemon returns the row with `guard_exceeded=true`, `control=self`,
`routing_enabled=false`, no automatic wake and no project authority. It does not reject
and create a monitoring blind spot.

Manifest `child_environment` says whether plugin can inject a distinct child environment
before child tools run. Only then may `routing_enabled=true` and `request_token=true`.
When qualified, daemon derives current token for the row/token version. Retry returns the
same token without plaintext in SQLite.

Raw token is never written to plugin state, recording, log or source spool. Plugin
persists only registration id, subject, Agent id, generation and token version.
Immediately before native environment injection it repeats the same idempotent child
request, holds token only in memory, injects it and drops the reference. If reliable
injection-before-tools is impossible, `child_environment=false`; no token is returned
and child CLI/Git activity is attributed to parent.

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
2. Steer/interrupt bind exact active turn; request replies bind exact request.
3. Map one semantic verb to one documented native mechanism/application point.
4. Reject weaker substitutions and hidden route changes.
5. Unknown is never retried.
6. Preserve command/native input/turn/request/event correlation.
7. Daemon owns queued-send order and generation expiry.
8. Reject oversized or known-secret durable payloads before native I/O.
9. Every persisted kind belongs to the source's declared family and satisfies its
   dedupe mode.

One source owns each normalized family per generation. Live stream normally owns
controlled lifecycle; transcript owns observe-only/explicit recovery; hooks own assigned
missing facts or synchronous decisions only. Final output commits before terminal turn
publication. Process exit, inbox acceptance, silence and intermediate state are never
completion.

## Hooks and transcripts

Hooks are session-scoped with exact identity, stable native hook/request id and one
absolute deadline. `hook.event` returns a committed non-dispatchable proposal or
native-safe `ask` (`allow` only for observation). Proposal acts only through that
immediate response; late commit/response expires and never approves later. There is no
durable hook queue.

Transcript readers persist cursors, wait only for an incomplete final line and report a
complete unparsable line/gap. They never infer end from silence or truncation.

## Custody

- Windows exact: assign-before-resume Job, no breakaway, kill-on-close for owned
  sessions.
- Linux exact: delegated cgroup v2/equivalent plus owner-death guardian; membership,
  group kill and readback prove termination.
- Linux/other Unix fallback: process group plus guardian for non-escaping members,
  degraded; exact force, complete descendant death and daemon-death containment are
  unsupported.

Attached services are never killed; managed services have separate custody. When
process exit races final output, drain authoritative source to EOF or deadline. Deadline
creates a gap. Exact backend emptiness or exact shared-session absence proves loss. On a
degraded backend, controller/channel loss may prove session control lost while custody
remains `orphan_possible`; it never proves every descendant died.

## Fake, recordings and L7

Fake is shared/isolated, generation-bound and persists source open ids, keys, sequences,
spool state, dedupe receipts and exact session snapshots. Recordings are bounded,
sanitized native NDJSON and exclude raw owner/routing tokens and credentials. CI never
calls a model.

L7 conformance covers both start branches, source opening before native I/O, every lost
session/input/source/handoff/event/process/child reply, strict-cursor/event-id/none replay,
event-row pruning, source digest mismatch/hole/stale/vendor duplicate, pagination/frame
limits, process ownership, child guard/routing/token retry, async/coalesced inputs,
unknown turn, hook timeout, exact bindings, no unknown retry, custody and bounded spools.
