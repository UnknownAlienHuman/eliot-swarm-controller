# Client API and CLI

## Transport and principal identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows uses a current-user-only named pipe; Unix uses a
mode-0600 socket under the private data root. One daemon owns the data-directory lock and
`daemon.json` records host boot id, PID, process birth identity, endpoint and version.

Every connected mutation has one explicit principal:

- ordinary owner CLI, when no Eliot Agent/rule environment is present, automatically
  reads the private local owner token and sends it—no `--owner` flag;
- an Agent sends `(agent_id,generation,agent_token)` from its environment;
- a managed rule/script sends a scoped run token;
- a plugin connection uses its daemon-created instance credential.

Missing, incomplete, revoked or mismatched identity rejects. A process with
`ELIOT_AGENT_ID`/`ELIOT_RULE` but no matching token never falls back to owner. This is
correctness/attribution inside one trusted OS user, not protection from a deliberately
malicious same-user process reading private files.

API frame maximum is 4 MiB; semantic fields are smaller. Requests are multiplexed, so a
slow call does not block unrelated requests.

## Mutation ids

Every external mutation carries `id` (a canonical ULID) generated and printed before
send. Identity is `(principal, method, canonical semantic payload, id)`.

- same tuple returns the retained result;
- same id with another principal/method/payload is `invalid/id_conflict`;
- an absent id older than the retry window is `invalid/id_expired`;
- a client retry after a lost reply reuses the same id;
- linked input, selector children and automation commands use derived ids and no fake
  external receipt.

Canonicalization excludes JSON-RPC id, output format, pager and wait/client timeout.

## Methods

| Method | Semantics |
| --- | --- |
| `daemon.status/shutdown` | version, admission mode, caller, Store/WAL, plugins/services, source owners/gaps, custody and provider holds; exact drain/force |
| `agent.list/get` | paginated Agent projection including generation, native subject, active turn/input gate, source owners and pending requests |
| `agent.wait/result` | exact captured generation/turn/Goal result |
| `command.submit/get/children` | immutable admission, selector snapshot, linked first input and readback |
| `turn.get/resolve` | running/completed/unknown turn and constrained owner assertion |
| `events.read/subscribe` | paged persisted replay then bounded live stream with explicit lag/history gap |
| `hook.event` | internal generation/request/deadline-bound decision |
| `agent.discover/attach`, `detach` | paged native subjects; one canonical Agent row and generation-fenced takeover |
| `mail.send/inbox/ack/read` | explicit recipient deliveries and exact acknowledgement ids |
| `var.*`, `who`, `claim.*` | scoped coordination |
| `rule.*`, `timer.*`, `goal.*` | scoped automation state and commands |

Goals are commands. Filter is
`{kinds?, agents?, tree?, sources?, ephemeral?}`; every list/history is paginated.

Stable error codes: `not_found`, `unsupported`, `not_allowed`, `bad_state`,
`stale_generation`, `invalid`, `busy`, `unavailable`, `capacity_unavailable`,
`history_gap`, `source_integrity`, `secret_in_payload`, `frame_too_large`, `internal`.

## Command admission

```text
command.submit {
  id, target?, selector?, verb, args,
  expected_generation?, expected_turn_id?, request_id?,
  probe? [false], wait_ms?
}
-> {
  command_id, agent?, generation?, state, outcome?,
  linked_command?, fanout?: {count}
}
```

One Store transaction:

1. resolves a concrete target or selector snapshot;
2. validates principal/control, plugin capability and state;
3. compares caller-supplied expected binding when present;
4. captures current generation and exact active turn/request;
5. stores principal/method/payload identity and command rows/events.

Interactive calls may omit expected generation; admission captures it atomically.
Durable automation always supplies the generation/turn/request from the triggering
snapshot. A mismatch is rejected before native I/O.

Launch creates Agent generation 0 and a start command targeting generation 1. Resume,
control takeover and detach expect current generation and target the next generation.
Exact transport reconnect to the same current ownership may preserve generation; an
uncertain/new writer takes a new one.

### Linked first input

Launch/resume with text creates start command plus `<start-id>/send` in one transaction.
Without `--wait`, CLI waits for session readiness only. With `--wait`, it follows the
linked input to its exact terminal turn.

For `start_requires_input`, the plugin receives the linked input inside `agent.start`,
but session, input association and terminal turn still have separate states/results.

### Selector fanout

An external selector command is a Store-only parent. Admission resolves and sorts the
concrete Agent ids, checks `max_selector_targets`, captures each current generation,
creates derived children and applies the parent with `{count}`. Parent applied means
only that expansion committed. `command.children` pages the fixed set and each child
settles independently.

## Input, turn and unknown gates

Native HTTP/inbox acceptance is not `send applied`. A send remains dispatched until the
plugin associates its command id with an exact native turn/execution. The turn result is
separate.

One dispatched or unknown input without exact association blocks later `send`,
`configure` and Eliot Goal continuation in that generation. Exact interrupt, stop,
approval/answer and readback remain available. An active or unknown turn similarly
blocks the next turn.

`turn.get` returns:

```text
{ agent, generation, turn_id,
  state: running | completed | unknown,
  command_ids, status?, final_text?, usage?, resolution_source? }
```

If reconnect proves a session exists but cannot prove terminal outcome, the turn is
unknown. Owner may resolve only after exact native readback proves it not active or the
generation is terminal:

```text
turn.resolve { id, agent, generation, turn_id,
               status: completed|interrupted|failed,
               reason, final_text? }
```

This records `resolution_source=owner`, never plugin completion. An unsafe command/action
assertion is owner-only, explicit and releases only its narrow hold.

## Waiting

`agent.wait` captures the target generation at call start:

- `turn` with `after_command`: follow that input's exact turn association/result;
- `turn` without it: current turn or first turn after the call high-water;
- `idle`: current generation idle with no open input/unknown turn gate;
- `stopped`: current generation terminal;
- `goal`: captured Goal revision done/blocked.

Replacement generation returns `stale_generation`; timeout never cancels admitted work.
Several input commands may map to one native turn and their waits may resolve together.

## Discover, attach, detach and resume

`agent.discover` is read-only and paginated. It returns native subjects and bounded
metadata; it creates no Agent rows.

`agent.attach` reuses the canonical row when the subject already exists:

- observe attach: new/current generation gets observe source owners and no Agent token;
- control attach: owner-selected global profile, capacity and native compatibility are
  validated first; exact success advances generation, rotates token if independent
  environment is available and installs controlled source owners;
- unknown takeover leaves the old projection and blocks another takeover until readback;
- another live controller or unverified replacement is `not_allowed`.

`detach` is valid only for independently living service/external sessions. Exact success
advances generation, revokes Agent identity, releases claims/queued control work and
keeps observe sources. It never settles an unknown input and never kills the service.

`resume` starts the same native subject as a new generation. Old input is never resent.

## Event replay

Subscription boundary:

1. register bounded live buffer;
2. capture persisted high-water `seq`;
3. replay pages `(after_seq, high_water]`;
4. release buffered persisted events above high-water in order, then follow live.

No read transaction spans client wait. Ephemeral events are best effort. Overflow emits
`events.lagged`; a request before retained history returns `history_gap` rather than
silently jumping.

## Mail acknowledgement

`mail.inbox` returns at most 100 oldest unread deliveries plus their exact ids; it marks
nothing. CLI writes complete output, then calls `mail.ack {ids:[...]}` with a write id.
Only those ids are marked read. If the CLI dies or ack reply is lost, messages may be
shown again but are never lost; new messages arriving between fetch and ack are not
accidentally acknowledged.

## Drain and shutdown

Drain commits admission mode `draining`:

- reject new launch/resume/send/steer/inject/Goal continuation and new automation
  firings;
- allow exact interrupt/stop/request replies/cancel/detach, queries and readback;
- continue pre-drain admitted work;
- wait for running effects and owned sessions to settle.

Unknown effect remains a blocker. `force` terminates only exact owned custody; attached
services are never killed. Degraded Unix custody reports possible orphans instead of
claiming clean success.

## CLI

All commands accept `--json`, `--data-dir` and every mutation accepts `--id`. Owner CLI
requires no special flag: outside an Eliot Agent/rule environment it automatically uses
the owner token. Inside an Agent it must have the matching Agent id/generation/token.

```text
eliot daemon [--foreground]
eliot daemon stop [--drain|--force]
eliot status
eliot launch [profile/workspace/tree options] [PROMPT] [--wait]
eliot ls | show A | watch [A|SELECTOR] | top
eliot send|inject A TEXT [--generation N]
eliot steer A TEXT [--or-send] [--generation N] [--turn ID]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--after-command ID]
eliot result A [--generation N] [--turns N]
eliot turn show A TURN [--generation N]
eliot turn resolve A TURN --generation N --as completed|interrupted|failed --reason T
eliot interrupt|abort A [--generation N] [--turn ID]
eliot stop A [--force] [--tree] [--generation N]
eliot resume A [PROMPT] [--generation N] [--wait]
eliot approve|deny|answer ... [--generation N] [--request R]
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

Exit codes: 0 success/queued; 1 definite error/failure; 2 usage; 3 rejected before
effect; 4 unknown; 5 wait timeout; 6 still pending/dispatched.
