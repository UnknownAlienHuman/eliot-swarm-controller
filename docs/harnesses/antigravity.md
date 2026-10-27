# Antigravity CLI (`agy`)

Layer L4.5. Plugin `plugins/antigravity` runs `mode=per_agent`: one warm `agy` process
inside one Eliot Agent generation/custody.

Facts checked 2026-10-25 against official headless/CLI/subagent/permission/hook pages
and the `google-antigravity/antigravity-cli` changelog. Recheck documented default
conflicts on the installed version.

## Native identity and generation

```text
native_scope = antigravity:<canonical auth/config identity>
native_ref   = exact conversation_id from native init/update
```

First start creates generation 1. Resume starts a new plugin/process with exact
`--conversation` as the next Eliot generation. The old process has no authority over the
new one: late stdout/result/child facts carry the old generation and cannot mutate
current state.

Subagents run inside the same `agy` process and share its environment:
`child_environment=false`. They are visible child Agents but receive no independent
Eliot token.

## Transport

```text
agy --input-format stream-json --output-format stream-json
    [--model M] [--mode accept-edits|auto|plan] [--effort E]
    [--conversation ID]
```

- one JSON user line per prompt; one terminal `result` per turn;
- native stdout events: `init`, `step_update`, `result`;
- at most one prompt in flight because native result does not name input;
- stream mode does not accept slash/control messages; unsupported operations are
  rejected instead of killing/restarting the process;
- closing stdin is graceful stop; force targets only this Agent custody.

Whether `init` arrives before first input is unverified. If not, plugin declares
`start_requires_input=true`; linked input travels in `agent.start` but session/input/
turn result remain separate Eliot outcomes.

## Source ownership and recovery

The controlled stdout stream owns `session`, `turn`, `message`, `tool`, `child` and
`usage`. Hooks are not a second lifecycle publisher. The plugin arms stdout before the
first user line.

The stream has no qualified replay cursor/history API. After transport/process break:

- do not replay old conversation output;
- record `source.gap` for the uncovered interval;
- use only exact current conversation/process readback available from the installed CLI;
- unknown input/turn remains unknown;
- resume starts a new Eliot generation and never resends old input.

Input/result pairing by order is valid only inside one process/generation with one input
in flight.

## Verb mapping

| Eliot verb | Native mechanism | Settlement |
| --- | --- | --- |
| `launch` | start `agy`; optional linked first input | `session.ready` from exact init/effective facts before first `turn.started` |
| `send` | next native user line | applied when exact native user-input/turn start is observed; write acceptance alone is dispatched |
| `steer` | unsupported | reject before native I/O |
| `inject` | prepend to next send only | report `applies=next_turn`; never current-turn steer |
| `interrupt` | unsupported until an exact installed native mechanism is recorded | force-stop is not interrupt |
| `stop` | close stdin; bounded wait; force only this custody | applied after generation ended; unknown close not repeated |
| `resume` | new process + exact conversation id | no previous prompt replay |
| `approve|deny|answer` | no headless request channel | policy/soft denial is native fact, not a pending Eliot request |
| `configure` | launch flags only | `applies=next_session` |
| `goal.*` | Eliot Goal | no qualified native Goal |

## Event normalization

- `init` → `session.ready` with exact conversation id/effective cwd/tools/permissions/
  model;
- first generation-matched progress for an admitted input → `turn.started`;
- response deltas → ephemeral message delta;
- tool updates → tool facts; soft-denied tool is explicit failure/denial fact even if
  final status is SUCCESS;
- subagent info → exact child registration; omitted workspace list means unchanged,
  empty list means cleared;
- terminal result maps SUCCESS to completed, ERROR/INVALID to failed and
  CANCELED/INTERRUPTED to interrupted; WAITING/RUNNING are nonterminal diagnostics;
- cumulative session usage replaces previous value, never sums.

Final result after stdout break is not invented. Process exit is drained to EOF/deadline
before `session.lost` according to the monitoring contract.

## Permissions, configuration and proxy

Permissions come from documented allow/deny/ask settings; dangerous skip is an explicit
full-access profile and effective init state must show it. Subagent definitions remain
native files/features.

No documented provider base-URL override was found at the checked date; proxy support is
`none` until an installed official mechanism is proven. Eliot never replaces the route
with a cloud/API fallback.

## Retained L7 cases

- installed init-before-input behavior and documented default conflicts;
- stale old-generation stdout/result ignored after resume;
- no replay cursor → explicit gap and no duplicate history;
- `SUCCESS` with soft-denied tool is not clean success;
- one-in-flight ordering and unknown result after process/stream loss;
- exact child subject/provenance and shared parent environment;
- effective model/permission and no hidden proxy route.
