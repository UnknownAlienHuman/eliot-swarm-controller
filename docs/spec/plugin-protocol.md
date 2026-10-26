# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite or
retries an uncertain native effect.

## Modes

- `per_agent`: one plugin instance and custody unit per Eliot generation.
- `shared`: one plugin talks to a service with exact per-session identity,
  environment/tool attribution, attach/readback and isolated stop.

A service missing any shared-control prerequisite is per-Agent or observe-only. Hosting
multiple chats is not sufficient for shared control.

## Framing and bounds

JSON-RPC 2.0 over UTF-8 NDJSON, at most 1 MiB per line. stdout is protocol; stderr is
bounded logs. Both directions issue requests. Every scoped message carries Agent and
generation. Readers are continuously drained and cancel-safe. Protocol
`eliot-plugin/1` permits additive fields only.

Inner limits keep every accepted payload representable:

- inline user input: at most 512 KiB UTF-8;
- resolved environment: at most 128 KiB encoded;
- remaining start/attach metadata: at most 128 KiB encoded;
- persisted event data/projected final text: at most 64 KiB each;
- discovery/history/snapshot page: at most 1,000 items and 900 KiB encoded;
- immutable system notes/scripts/config are `ArtifactRef`, never large inline bytes.

Oversized request rejects before native I/O. Oversized native output is explicitly
truncated with a safe pointer. Raw secrets/routing tokens are excluded from recordings,
spools and logs.

## Host to plugin

| Method | Key fields/result |
| --- | --- |
| `initialize` | config/private dir/instance credential → manifest |
| `agent.start` | target generation + exact binding revision/payload + workspace/env/artifacts + resume subject? + linked initial? → session result + separate initial status |
| `agent.attach` | exact subject/generation + candidate/current binding revision/payload + control mode + known turn/source watermarks → native state, compatibility, turn/source reconciliation, requests, Agent-owned PIDs/effective or not-found |
| `agent.command` | command id, exact generation/binding revision/turn/request, verb/args → applied/rejected/failed/unknown + mechanism/application point |
| `agent.snapshot` | exact generation/current binding + command/turn ids + page cursors → state/readback + bounded pages/source watermarks |
| `discover` | `{cursor?,limit<=1000}` → bounded native-subject page + next cursor |
| `agent.stop` | exact generation/command/force → success only after generation ended |
| `shutdown` | finish owned cleanup and exit |

```text
ArtifactRef = {digest,path,size}
resume_from = {native_scope,native_ref}
initial = {command_id,text}
binding = {generation,revision,mode,profile_revision,payload}
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

A plugin treats binding revision as immutable command scope. It never silently reloads
the current config/profile after dispatch. If daemon passes another revision for the
same generation, it is an explicit attach/detach transition and must be correlated to
that command.

## Start and initial input

Authoritative source epochs open **before any native session/input effect**, even when
native subject is unknown.

### Session-first harness (`start_requires_input=false`)

1. persist stable open ids and call `source.open` for every initial family;
2. after acknowledgement, start/resume native session without user input using the
   exact binding revision;
3. establish subject/effective settings;
4. commit-acknowledge `session.ready` including binding revision;
5. return session applied;
6. daemon dispatches linked initial `send`.

### Input-created session (`start_requires_input=true`)

1. open provisional source epochs before native I/O;
2. receive exactly one linked `initial` command and exact binding revision;
3. perform one native start+input operation;
4. spool every native fact under opened epoch;
5. once subject/control is known, commit `session.ready` for session outcome;
6. commit input/turn facts and report separate initial outcome.

Ready proves only session control. Valid outcomes include session applied + initial
unknown/applied and both unknown after ambiguous transport loss. Definite pre-I/O
failure fails both; definite session creation plus exact input rejection applies session
and fails input. No replacement input/session resolves uncertainty.

Before ready, plugin backpressures or uses bounded durable spool. Overflow closes source
with a visible gap and preserves uncertainty.

Launch and resume use `agent.start`. Resume supplies same canonical subject, new
generation and revision 1. Recovery attach to the same controlled incarnation supplies
the current revision and preserves it.

Later send transport/inbox acceptance is intermediate. Plugin keeps command pending
until exact turn association, exact no-effect readback or unknown deadline. One native
execution may consume several inputs. Native input id is optional when exact
command→turn mapping exists.

## Attach and binding transitions

Host supplies known active turn, source watermarks and either current or candidate
binding revision. Empty active list is not idle proof:

- exact same turn running → running;
- exact terminal history/result → completed;
- session exists, turn not active, no terminal proof → `not_active_unknown`;
- subject absent → not-found.

### Observe attach

Synthetic observe revision contains no control credentials/environment. Plugin performs
read-only attach/discovery and returns compatibility/effective facts. Daemon makes that
revision current only after exact attach/readback commits.

### Observe → control / reattach

Daemon creates a candidate revision and durable attach command but keeps the old current
pointer. Plugin receives both expected current revision and candidate revision. It must:

1. validate exact native subject/session incarnation;
2. prove candidate route/workspace/permissions are compatible rather than silently
   reconfigure a different session;
3. install/read back per-session environment where required;
4. report applied, failed or unknown for that candidate revision.

Applied response does not itself mutate Store. Daemon atomically applies command, moves
current pointer, updates Agent projection/token/capacity and commits effective facts.
Failed/unknown leaves old pointer. Same command/revision retry is readback only, not a
new attach. Exact recovery of an already-controlled current revision is not a new
revision.

### Detach

Detach carries a candidate `detach_observe` revision. Plugin releases only Eliot control
lease/native attachment; it never stops the independent session/service. Exact success
allows daemon to move pointer/revoke routing/release claims. Failed/unknown keeps the
control pointer. A daemon-owned per-Agent process cannot detach.

Unknown turn keeps generation blocked from new turns independent of binding transition.

## Plugin to host durable requests

| Method | Result |
| --- | --- |
| `source.open {open_id,agent,generation,source_name,families,mechanism,dedupe_mode,native_cursor?,replaces?}` | idempotent source key/watermark; initial ownership or atomic predecessor handoff |
| `event.put {agent,generation,source_key,source_seq,kind,native_id?,native_cursor?,ts?,data}` | Store event seq + duplicate/high-water after atomic commit |
| `process.put {agent,generation,source_key,source_seq,pid,start_time,role}` | verified per-Agent descendant registration + duplicate/high-water |
| `child {registration_id,parent agent/generation,child scope/ref,name?,labels?,request_token?}` | canonical child row/generation/routing capability and optional memory-only current token |
| `source.close {source_key,native_cursor?,state=closed|gap,reason?}` | idempotent boundary when no replacement transaction is requested |

```text
replaces = {
  source_key,
  expected_high_water,
  final_native_cursor?,
  state: closed | gap,
  reason?
}
```

`log` and `event.ephemeral` are best-effort and make no durability claim.

### Source opening, ownership and sequence

Plugin durably chooses bounded `open_id` before first open. Store maps
`(agent,generation,open_id)` to one source key. Same id/canonical fields return same
key/watermark after lost reply; mismatch is integrity failure.

Initial open claims only unowned families. Planned replacement supplies `replaces`; one
Store transaction verifies predecessor key/high-water/ownership, closes or gaps it,
creates new epoch and transfers families. Separate close/open is explicitly gapped.

Each durable `event.put`/valid `process.put` has contiguous positive `source_seq`.
Plugin assigns exact envelope before send and permits at most one unacknowledged request
per key; later facts collect in bounded ordered spool. After reply it durably records
ack/removes entry before sending next. On restart it reads host watermark. Daemon uses
one versioned canonical digest.

- next sequence commits watermark plus receipt/event/process/projection/reducer;
- resend current high-water with matching digest returns saved result;
- same sequence/different digest is integrity failure;
- older sequence rejects with watermark/cursor;
- future sequence with hole rejects until replay or gapped replacement.

### Vendor replay modes

- `strict_cursor`: vendor guarantees total-order resume strictly after committed cursor;
- `event_id`: every persisted lifecycle fact has stable per-event native id;
- `none`: no lossless replay claim; reconnect/handoff exposes a gap.

In `event_id`, Store native receipt survives event retention. Replay with matching
identity/digest returns original Store seq and may advance new source sequence without
publishing lifecycle again. Digest mismatch is integrity failure. In `strict_cursor`,
every successful put advances committed native cursor. `none` never replays history.

A well-formed stale turn/request may advance source sequence with one bounded diagnostic.
Malformed envelope, missing required native identity or undeclared family does not.

### Process registration and ownership

`process.put` is valid only for a PID inside the calling per-Agent plugin's Agent custody.
Host pins birth and verifies membership. OS monitor is sole authority for process
start/exit/stats; registration and enumeration of one identity publish one start.

Shared plugin never assigns its server/uncontained service child to Agent. Service PIDs
belong to `service:<name>`. Session tool PID data is diagnostic only.

### Native child registration and routing

`registration_id` is stable across lost replies. Canonical child subject is identity;
conflicting parent/root/project/harness is integrity failure.

Every already-created native child is recorded. If controller guards are exceeded,
daemon returns row with `guard_exceeded=true`, `control=self`,
`routing_enabled=false`, no automatic direct wake and no project authority.

Manifest `child_environment` says whether plugin injects a distinct child environment
before tools. Only then may routing/token be enabled. Retry returns same current token
without plaintext in SQLite.

Raw token is never written to plugin state/recording/log/spool. Plugin persists only
registration id, subject, Agent id, generation and token version. Immediately before
environment injection it repeats the idempotent child request, holds token only in
memory, injects and drops it. If reliable injection-before-tools is impossible,
`child_environment=false`; CLI/Git attribution remains parent.

## Manifest

```json
{
  "name": "codex",
  "version": "0.1.0",
  "protocol": "eliot-plugin/1",
  "harness_version": "...",
  "available": true,
  "session_mode": "per_agent",
  "verbs": ["launch","attach","send","steer","inject","interrupt","stop","resume","detach"],
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

Shared control requires per-session environment where tools need Eliot identity and
exact isolated stop. Versions are facts, not authority gates.

## Translation and normalization rules

1. Validate generation and binding revision before native I/O.
2. Steer/interrupt bind active turn; request replies bind exact request.
3. Map one semantic verb to one documented native mechanism/application point.
4. Reject weaker substitutions and hidden route changes.
5. Unknown is never retried.
6. Preserve command/native input/turn/request/event correlation.
7. Daemon owns queued-send order and generation expiry.
8. Reject oversized/known-secret durable payload before native I/O.
9. Persisted kind belongs to declared family and dedupe mode.

One source owns each family. Live stream normally owns controlled lifecycle; transcript
owns observe/recovery; hooks own assigned missing facts/synchronous decisions. Final
output commits before terminal turn. Process exit, inbox acceptance, silence and
intermediate state are not completion.

## Hooks and transcripts

Hooks are session-scoped with exact identity, stable native hook/request id and one
absolute deadline. `hook.event` returns a committed non-dispatchable proposal or safe
`ask` (`allow` only for observation). Proposal acts only through immediate response;
late response expires and never approves later. No durable hook queue.

Transcript readers persist cursors, wait only for incomplete final line and report a
complete unparsable line/gap. They never infer end from silence/truncation.

## Custody

- Windows exact: assign-before-resume Job, no breakaway, kill-on-close for owned
  sessions.
- Linux exact: delegated cgroup/equivalent + owner-death guardian.
- Linux/other Unix fallback: process group + guardian for non-escaping members,
  degraded; exact force/complete descendant death/daemon-death containment unsupported.

Attached services are never killed; managed services have separate custody. When
process exit races final output, drain authoritative source to EOF/deadline. Deadline is
a gap. Exact backend empty or shared-session absence proves loss. Degraded controller
loss may prove session control lost while custody remains `orphan_possible`.

## Fake, recordings and L7

Fake is shared/isolated, generation-bound and persists source open/sequence/replay
receipts/spool, binding revision readback and exact session snapshot. Recordings are
bounded sanitized native NDJSON and exclude owner/routing tokens/credentials. CI never
calls a model.

L7 conformance covers both start branches, observe→control→detach→control binding
revisions and pointer movement, failed/unknown attach pointer stability, source opening
before native I/O, every lost source/handoff/event/process/child reply, replay modes,
event pruning, source mismatch/hole/stale/vendor duplicate, pagination/frame limits,
process ownership, child guards/routing/token retry, async/coalesced inputs, unknown
turn, hook timeout, no unknown retry and custody.
