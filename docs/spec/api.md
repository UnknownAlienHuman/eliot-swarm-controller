# Client API and CLI

Implement only the current [roadmap](../roadmap.md) surface. The first client supports
core Agent commands/events; coordination, automation and service methods are added with
their modules. Do not advertise unimplemented commands.

## Transport and principal identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows uses a current-user-only named pipe; Unix a
mode-0600 socket under the private data root. One daemon owns the data-directory lock.
`daemon.json` records boot id, PID/birth identity, endpoint and diagnostic version.

Every call has an explicit principal. Owner CLI outside a managed Agent/rule environment
reads the private owner token automatically. Agents send id/generation/token; managed
scripts use scoped run tokens; plugins have instance credentials. Missing/incomplete/
revoked credentials reject, never become owner. No manual `--owner` step is required.
This prevents accidental attribution errors; it is not isolation from hostile processes
of the same OS user. Reads are scoped as well as writes.

Frames are at most 4 MiB, semantic fields smaller. Multiplex requests so one wait does
not block unrelated calls. Bound all lists and histories; never hold a read transaction
open while waiting for a client or native process.

## Mutation ids

External writes carry a ULID generated/printed before send. Bind it to principal,
method and canonical semantic payload. Exclude JSON-RPC id, output format, paging and
wait timeout. Same binding returns the retained result; different binding is
`id_conflict`; absent expired id is `id_expired`. Retry a lost client reply with the
same id, never a new native effect.

Commands store the binding directly; other writes use:

```sql
CREATE TABLE writes (
  id TEXT PRIMARY KEY, principal TEXT NOT NULL, method TEXT NOT NULL,
  payload_digest BLOB NOT NULL, result TEXT NOT NULL, created_at INTEGER NOT NULL
);
```

The writer checks both command/write namespaces. Later linked inputs, selectors and
rule actions have bounded derived ids. No general receipt/floor framework.

## Methods

| Method | Purpose |
| --- | --- |
| `daemon.status/shutdown` | current state, admission mode and exact drain/force |
| `agent.list/get/wait/result` | scoped, paginated Agent state and exact results |
| `command.submit/get/children/resolve` | immutable admission, linked input, fixed selector/action children |
| `turn.get/resolve` | running/completed/unknown turn; constrained owner assertion |
| `events.read/subscribe` | persisted replay plus bounded live delivery |
| `agent.discover/attach`, `detach` | later exact-subject observation/control |
| `hook.event` | later exact request/deadline decision |
| `mail.send/inbox/read/ack`, `var.*`, `who`, `claim.*` | later coordination |
| `rule.*`, `timer.*`, `goal.*` | later automation; effects remain commands |

Filter shape is `{kinds?,agents?,tree?,sources?,ephemeral?}`. Errors include
`not_found`, `unsupported`, `not_allowed`, `bad_state`, `stale_generation`, `invalid`,
`busy`, `unavailable`, `capacity_unavailable`, `history_gap`, `source_integrity`,
`secret_in_payload`, `frame_too_large`, `mailbox_full`, `internal`.

## Command admission

```text
command.submit {
  id, target?, selector?, verb, args,
  expected_generation?, expected_turn_id?, request_id?, probe? [false], wait_ms?
}
-> {command_id, agent?, generation?, state, outcome?, linked_command?, fanout?:{count}}
```

In one transaction resolve target/selector, validate principal/capability/state, compare
supplied expectations, capture exact generation/turn/request, then persist immutable
rows/events. Interactive calls may omit expectations; delayed automation supplies its
captured expectations. Mismatch rejects before native I/O.

Generation-changing dispatch follows [core](core.md#generation-fence): advance and
revoke before native I/O, no rollback, no replay after unknown. Terminal settlement
revokes the token even if degraded Unix custody reports possible escaped descendants.

Launch/resume with explicit text creates a start plus linked `<id>/send`. Without
`--wait`, return session readiness; with it, follow the linked input to its exact terminal
turn. An input-created native session still reports these distinct outcomes.

A selector parent snapshots/sorts concrete ids and generations in a bounded transaction;
children settle independently. Parent applied means expansion only. `command.children`
pages that fixed set. Retain/prune parent sets transactionally so a caller never sees a
partly pruned expansion; after removal return `history_gap`. Ordered rule actions reuse
the same representation with a distinct link kind, not selector execution semantics.

## Input, turn and unknown gates

Transport/inbox admission is not send applied. Bind input to an exact native turn, then
track its result independently. Unassociated dispatched/unknown input blocks later
send/configure/Goal continuation; active/unknown turn blocks next turn; unknown transition
blocks further takeover. Exact stop/interrupt/request replies/readback remain available.

A dispatched or unknown `configure` also holds the settings lane: do not dispatch a
later configure, new input or native/Eliot Goal start/resume until its exact outcome and
effective settings are read back or explicitly resolved. Timeout does not release this
hold, even when the observed Agent state is idle. This is a predicate over existing
command rows, not another queue/table. Stop/interrupt/request replies/readback still work.

```text
turn.get -> {agent,generation,turn_id,state:running|completed|unknown,
             command_ids,status?,final_text?,usage?,resolution_source?}
```

Owner turn resolution requires exact readback proving it inactive or the generation
terminal. It records `resolution_source=owner`, not a synthetic native completion.
Owner command resolution is `command.resolve {id, command_id, as: applied|failed,
reason}`; it records an owner assertion, never a native receipt, and releases only the
relevant hold. `turn.resolve` likewise takes a mutation id, Agent/generation/turn,
terminal status and reason; it cannot bypass the inactive/terminal precondition.
Known failure can include partial effects; it never implies rollback or automatic retry.

## Waiting

Capture generation at call start. `turn --after-command ID` follows that input's exact
association/result; without an id, capture the current turn or the first after the call
high-water. `idle` requires no open input/turn/transition or unresolved settings gate.
`stopped` means terminal; `goal` captures its revision. Replacement returns
`stale_generation`; timeout never cancels admitted work. Several native inputs may
legitimately share one turn result.

## Discover, attach, detach and resume

Discovery is read-only/paged and creates no Agent rows. Attachment uses one canonical
native subject row. First attach targets generation 1; a later ownership takeover gets
the next generation. Validate before dispatch, commit identity fence before native I/O,
and install exact readback on success. Failure never restores revoked control; unknown
blocks another takeover.

Detach applies only to independently living sessions/services: revoke control, become
observe-only on exact success, never kill the shared/adopted service. It does not settle
an unknown input. Resume reopens the same native subject as a new generation and never
resends previous input.

## Event replay

Register a bounded live buffer, capture persisted high-water, replay pages through it,
then release buffered persisted events above it in order. Ephemeral delivery is best
effort. Overflow reports lag; pruned history reports `history_gap`, never silent skipping.
Committed event sequence values are never reused after retention.

## Mail acknowledgement

Mail belongs to the stable recipient Agent, while its presentation command remains
bound to an exact generation. A valid current Agent may explicitly read older unread
mail for that same Agent; responses show the original recipient generation and any stale
presentation. Resume does not resend the old prompt or retarget its presentation command.

`mail.inbox` returns at most 100 deliveries within the encoded response-frame limit
and marks nothing. `mail.read` is also read-only. After writing complete output, CLI sends:

```text
mail.ack { id: <new write id>, deliveries: [<exact delivery id>, ...] }
```

Authorize the current caller for that stable mailbox and acknowledge only those ids.
A lost response may redisplay mail, not lose it or acknowledge newly arrived messages.
See [coordination](coordination.md#mailbox) for bounds and presentation policy.

## Drain and shutdown

Drain rejects new turn-producing work and new automation, while continuing pre-admitted
work and allowing exact stop/interrupt/request replies/cancel/detach/readback. Unknown
outcomes remain visible blockers. Force terminates only owned custody, never adopted
services. Degraded Unix reports possible orphans, not exact clean success.

## CLI

All commands accept `--json` and `--data-dir`; every mutation accepts `--id`.
Generation/turn/request flags are optional for immediate calls, explicit for delayed ones.

```text
eliot daemon [--foreground]
eliot daemon stop [--drain|--force]
eliot status
eliot launch --profile P --workspace PATH [PROMPT] [--wait]
eliot ls | show A | watch [A|SELECTOR] | top
eliot send A TEXT [--wait] [--generation N]
eliot steer|inject A TEXT [--generation N] [--turn ID]
eliot interrupt|abort A [--generation N] [--turn ID]
eliot stop A [--force] [--tree] [--generation N]
eliot resume A [PROMPT] [--wait]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--after-command ID]
eliot result A [--generation N] [--turns N]
eliot turn show|resolve ...
eliot approve|deny|answer ... [--request R]
eliot configure ... | compact|fork|background ... | goal ...
eliot cmd show|children|cancel|resolve ...
eliot events [--since SEQ] [--allow-gap]
eliot msg send ... | inbox [--wait SECS] [--all]
eliot var ... | who ... | claim ... | release ...
eliot rule ... | rules pause|resume | timer list|run
eliot discover [--harness H] [--cursor C] [--limit N]
eliot attach --harness H --scope S --ref R [--control --profile P]
eliot detach A
eliot service list|stop | hold list|clear | worktree remove | doctor
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

Only current-layer commands appear in help and Agent guidance. Exit codes: 0 success/
queued; 1 definite failure; 2 usage; 3 pre-effect rejection; 4 unknown; 5 wait timeout;
6 still dispatched/pending. A queued/applied input is not successful task completion.
