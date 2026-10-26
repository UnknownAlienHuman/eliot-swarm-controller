# Antigravity CLI (`agy`)

Layer L4.5, `plugins/antigravity`, `per_agent`. One warm `agy` custody unit belongs to
one Eliot generation. Exact force exists only with an exact platform backend.

Facts checked 2026-10-25 against official headless/CLI/subagent/permission/hook pages
and changelog; conflicting defaults require installed recording.

## Transport and start

`agy --input-format stream-json --output-format stream-json ...` accepts one JSON user
line at a time and emits init/step/result. Stream mode does not accept slash or Claude
control commands.

Before spawning `agy`, plugin persists and opens stdout-lifecycle and optional
descendant-registration source epochs. Installed behavior must then establish whether a
stable conversation/init exists before the first user line:

- init first → `start_requires_input=false`; spawn process, commit ready from init, then
  dispatch linked send;
- input required → `start_requires_input=true`; source is already open, linked send is
  `agent.start.initial`, and session/input/turn outcomes remain separate.

Never write optional prompt without linked command identity. Lost spawn/init/write/
result is the corresponding unknown; never create another conversation as retry. Resume
opens new-generation sources before spawning a process with the exact conversation id;
optional text is a linked send.

Controlled stdout has no assumed replay cursor. If installed events expose stable
per-event ids, use `event_id`; otherwise the live process uses `none`, and reconnect/
process replacement creates an explicit source gap instead of replaying old lines. The
plugin maintains a bounded durable unacknowledged spool; loss/overflow closes source
gapped. stdout/process exit alone is not completion.

## Correlation and verbs

A native result may not name the input. One process/generation therefore has at most one
unresolved prompt; plugin retains exact Eliot command ↔ local input ordinal ↔ native
turn/result. A separate native input id is optional. Ambiguity across reconnect/process
becomes unknown, never reconstructed by time/order from another generation.

| Eliot | Antigravity |
| --- | --- |
| send | next line only after previous association/turn terminal; applied at exact bound execution |
| steer | unsupported |
| inject | prepend-next-send only, reported explicitly |
| interrupt | unsupported until exact installed mechanism; force-stop is not interrupt |
| stop | close stdin/wait; exact force through custody only; unknown close not replayed |
| approve/deny/answer | no headless request channel; policy/soft denial is native tool outcome |
| configure | launch flags, `next_session` |
| Goal | Eliot Goal only |

## Events and readback

Controlled stdout owns lifecycle. Init provides conversation/effective settings where
available. First exact progress after admitted input starts the bound turn. Result
settles only that turn: success completed, error/invalid failed, cancelled/interrupted
interrupted; waiting/running is nonterminal.

Pre-init error is definite no-effect only with native evidence. stdout closure with a
possibly lost terminal frame creates unknown turn under source-drain rules. Cumulative
session usage replaces rather than sums. Soft-denied tools remain visible even if top
result says success. Persisted event/final text obeys bounds.

## Native children and process ownership

Subagents register by exact conversation/provenance subject and stable registration id.
Omitted workspace list means unchanged; empty means cleared. Parent result never proves
child completion. Child lists are paged if the installed format exposes more than one
bounded response.

Antigravity subagents normally execute inside the same `agy` process/environment. Until
installed evidence proves child-specific environment injection, declare
`child_environment=false`: child rows remain visible/natively controllable where
supported, but `routing_enabled=false`, shell/CLI/Git activity is attributed to parent
and no child token is minted. Guard-exceeding vendor children remain visible with reduced
authority/wake.

The warm `agy` PID is owned by the parent Agent custody. Additional descendant
registration must pass OS membership; OS monitor owns exit/stats. A future shared server
PID would be service-owned, never child-owned.

## Environment and route

No qualified proxy/base-URL override; manifest proxy is none. Minimal environment passes
named vendor auth/config plus parent Eliot identity only when routing is enabled.
Permission mode is explicit and reported from init; full access is never inferred.

A routing-disabled target cannot use direct Eliot Goal/mail notice; parent-mediated full
native delivery must be separately qualified or the operation rejects.

## L7 cases

- source open committed before process spawn/first input;
- init-before-input and every lost source/start/input/result acknowledgement;
- one-at-a-time association without native input id, reconnect ambiguity/unknown turn
  and stale generation;
- event-id/no-replay gap semantics and bounded spool/frame;
- soft denial under success and cumulative usage;
- child canonical subject/guard/workspace semantics, independent completion and parent
  attribution without child env;
- descendant registration vs OS enumeration;
- close/force exact/degraded custody;
- installed print-timeout/effort/exit-code conflicts.

Frozen v1 fixtures are official-example transcriptions, not live qualification.
