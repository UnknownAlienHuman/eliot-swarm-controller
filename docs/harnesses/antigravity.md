# Antigravity CLI (`agy`)

Layer L4.5, `plugins/antigravity`, `per_agent`. One warm `agy` process belongs to one
Eliot generation. Exact force is advertised only with an exact platform custody
backend.

Facts checked 2026-10-25 against official headless/CLI/subagent/permission/hook pages
and changelog; documented defaults still conflict and require installed recording.

## Transport/start

`agy --input-format stream-json --output-format stream-json ...` accepts one JSON user
line at a time and emits init/step/result lines. Stream mode does not accept slash or
Claude control commands.

It is **unverified whether installed `agy` emits a stable conversation/init before the
first user line**. L4.5 records this and selects one contract:

- init before input → `start_requires_input=false`; ready session, then linked send;
- init requires first input → `start_requires_input=true`; linked send is passed as
  `agent.start.initial`, and session start vs input outcome are reported separately.

Never write an optional prompt inside launch without the linked command identity. Lost
init/write/result remains the corresponding unknown command; never start another
conversation as retry. Resume starts a new Eliot generation with exact
`--conversation`; optional new text is a linked send, never old prompt replay.

## Correlation and verbs

Native results may not name input. Therefore one process/generation admits at most one
prompt, and the plugin retains exact Eliot command↔local turn↔ordered native result
binding. If the stream sequence becomes ambiguous, the turn is unknown; order is not
reconstructed across processes/generations.

| Eliot | Antigravity |
| --- | --- |
| send | next user line only after prior exact terminal result |
| steer | unsupported |
| inject | prepend-next-send only, reported as such |
| interrupt | unsupported until exact installed mechanism is recorded; force-stop is not interrupt |
| stop | close stdin/wait; exact force only through custody; unknown close is not replayed |
| approve/deny/answer | no headless request channel; permission policy/soft denial is native tool outcome |
| configure | launch flags, `next_session` |
| goal | Eliot goal only |

## Events/source ownership

Controlled stdout owns lifecycle. Init provides conversation/effective settings where
available. First exact progress after admitted input starts the bound turn. Result
settles only that bound turn: success/completed, error/invalid failed, cancelled/
interrupted interrupted; waiting/running are nonterminal.

A pre-init error is a definite start/initial failure only when native evidence proves
nothing was admitted. stdout closure with possible lost terminal frame is unknown until
process/source drain rules. Cumulative session usage replaces rather than sums.

Subagents register with canonical conversation/provenance scope. Omitted workspace list
means unchanged, empty means cleared. Parent result does not finish children.

Soft-denied tools remain visible even when top-level result says success.

## Environment/route

No qualified proxy/base-URL override; manifest proxy is none. Minimal environment
passes only named vendor auth/config plus Eliot token/Git identity. Permission mode is
explicit and effective init state is displayed; full-access flag is never inferred.

## L7 cases

- init-before-input behavior and every start/input lost-reply point;
- ordered correlation loss and stale generation output;
- soft-denial under success;
- cumulative usage, child workspace semantics and parent/child completion;
- close/force under exact and degraded custody;
- documented print-timeout/effort/exit-code conflicts.

Frozen v1 fixtures are official-example transcriptions, not live qualification.
