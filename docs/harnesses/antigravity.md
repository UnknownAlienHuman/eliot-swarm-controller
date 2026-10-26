# Antigravity CLI (`agy`)

Layer L4.5, `plugins/antigravity`, `per_agent`. One warm `agy` custody unit belongs to
one Eliot generation. Exact force only with exact platform backend.

Facts checked 2026-10-25 against official headless/CLI/subagent/permission/hook pages
and changelog; conflicting defaults require installed recording.

## Transport and start

`agy --input-format stream-json --output-format stream-json ...` accepts one JSON user
line at a time and emits init/step/result. Stream mode does not accept slash or Claude
control commands.

Installed behavior must establish whether stable conversation/init exists before first
user line:

- init first → `start_requires_input=false`; open source, commit ready, then linked send;
- input required → `start_requires_input=true`; linked send is `agent.start.initial`,
  but session and exact input/turn outcomes remain separate.

Never write optional prompt without linked command identity. Lost init/write/result is
the corresponding unknown; never create another conversation as retry. Resume starts
new generation with exact conversation id; optional text is linked send.

Controlled stdout source uses stable open id/key and contiguous commit acknowledgements.
If plugin cannot backpressure/replay lines, it maintains bounded durable spool; loss/
overflow closes source gapped. stdout/process exit alone is not completion.

## Correlation and verbs

Native result may not name input. One process/generation therefore has at most one
unresolved prompt; plugin retains exact Eliot command ↔ local input ordinal ↔ native
turn/result. Ambiguity across reconnect/process becomes unknown, never reconstructed by
time/order from another generation.

| Eliot | Antigravity |
| --- | --- |
| send | next line only after previous input association/turn terminal; applied at exact bound native execution |
| steer | unsupported |
| inject | prepend-next-send only, reported explicitly |
| interrupt | unsupported until exact installed mechanism; force-stop is not interrupt |
| stop | close stdin/wait; exact force via custody only; unknown close not replayed |
| approve/deny/answer | no headless request channel; policy/soft denial is native tool outcome |
| configure | launch flags, `next_session` |
| goal | Eliot goal only |

## Events and readback

Controlled stdout owns lifecycle. Init provides conversation/effective settings where
available. First exact progress after admitted input starts bound turn. Result settles
only that turn: success completed, error/invalid failed, cancelled/interrupted
interrupted; waiting/running nonterminal.

Pre-init error is definite no-effect only with native evidence. stdout closure with
possible lost terminal frame creates unknown turn after source drain rules. Cumulative
session usage replaces rather than sums. Soft-denied tools remain visible even if top
result says success.

## Native children and process ownership

Subagents register by exact conversation/provenance subject and stable registration id.
Omitted workspace list means unchanged; empty means cleared. Parent result never proves
child completion.

Antigravity subagents normally execute inside the same `agy` process/environment. Until
installed evidence proves child-specific environment injection, declare
`child_environment=false`: child rows are visible, but shell/CLI/Git activity is
attributed to parent and no child token is minted.

The warm `agy` PID is owned by parent agent custody. Additional descendant registration
must pass OS membership; OS monitor owns exit/stats. A future shared server PID would be
service-owned, never child-owned.

## Environment and route

No qualified proxy/base-URL override; manifest proxy none. Minimal environment passes
named vendor auth/config plus parent Eliot identity. Permission mode explicit and
reported from init; full access never inferred.

## L7 cases

- init-before-input and lost source/start/input/result acknowledgements;
- one-at-a-time association, reconnect ambiguity/unknown turn and stale generation;
- soft denial under success and cumulative usage;
- child canonical subject/workspace semantics, independent completion and parent
  attribution without child env;
- descendant registration vs OS enumeration;
- close/force exact/degraded custody;
- installed print-timeout/effort/exit-code conflicts.

Frozen v1 fixtures are official-example transcriptions, not live qualification.
