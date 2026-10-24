# ScriptRunner — one execution, indexed typed actions

Current contract and reduction target for trusted local scripts.
[Implementation K](agent-operations/implementation.md#k--selected-effect-owners) owns
source migration; [audit AR-071–078](agent-operations/audit-register.md#ar-071) records
the connected source trace. This document distinguishes working custody from open
action/readback gaps.

## 1. Product boundary

A ScriptRun executes one selected immutable script revision with bounded input and may
request a bounded list of typed controller actions.

```text
registered immutable script revision
-> exact invocation and represented Manager
-> one admitted ScriptRun Operation
-> one standalone process family
-> immutable result/stdout/stderr
-> indexed typed action requests
-> ordinary child Operations and readback
```

The script is not a Manager, workflow engine or Store passthrough. It cannot directly
mutate Task state, send native input, publish code or accept work. Store authorizes each
returned action against current actor/object state and admits the ordinary domain
Operation that owns its effect.

## 2. Registry and invocation

A script revision freezes:

- script ID, revision, owner and active/inactive state;
- immutable bundle bytes and digest;
- interpreter kind, executable identity and fixed arguments;
- allowed typed controller effects;
- bounded input/result schemas;
- inherited-environment allowlist.

Direct `script.run` requires the script owner plus exact current Task/Attempt scope when
the invocation is task-linked. An automation invocation retains the represented Manager,
entry/project/source occurrence and optional Task scope. The final reservation
transaction rechecks the active revision, authority, bundle/interpreter identity,
invocation size/schema and exact current source.

Disabling or transferring automation after admission does not erase the ScriptRun.
Returned actions recheck current rights and subject state at action admission.

## 3. One process, no replay

New work requires a verified standalone `swarm-script-worker` executor pin. Before the
script starts:

1. host materializes private invocation/work metadata;
2. standalone worker enters the owned Job/process group;
3. worker publishes exact identity;
4. Store verifies the worker and current ScriptRun scope;
5. Store writes the first-writer-wins token-bound `Allow` decision;
6. worker resolves the one-use plan and starts the interpreter once.

Missing, stale, denied or ambiguous start decision never starts the interpreter. The
worker executes direct argv with cleared environment, bounded stdin/stdout/stderr,
fixed maximum duration and the retained working directory.

Host restart changes active work to reconciliation. Recovery reads the same worker,
start decision, process family, terminal and completion files. It may publish one exact
retained completion or settle a proven-departed family as incomplete. It never launches
a replacement interpreter or repeats the script because a response was lost.

## 4. Terminal commit

After exact completion validation, Store commits in one Immediate transaction:

- result/stdout/stderr artifacts;
- ScriptRun terminal state;
- parent `script.run` result;
- typed child-action admissions or exact rejections;
- terminal lifecycle observation.

A Store failure rolls the transaction back. Replaying the identical retained completion
returns the terminal parent and does not admit the child actions again. Preserve this
atomicity.

No external or native I/O belongs in this transaction. A returned Send, Reply, Goal,
Configure, GitHub write or publication request must create its ordinary child Operation;
its owner performs the actual effect after commit and exposes normal unknown/readback
semantics.

## 5. Indexed action identity

Every returned action needs a stable position in the validated result:

```text
ActionRecord
  script_run_id
  action_index
  effect_kind
  request_digest
  state = pending | admitted | rejected | settled | unknown
  child_operation_id?
  error_code?
```

Identity is `(ScriptRun, action_index)`, not merely the effect kind. Same-type actions
are valid when each index has its own bounded payload. Replaying the same index and
payload returns the retained ActionRecord; the same index with different bytes is a
conflict.

The current implementation hashes `(parent Operation, ScriptRun, effect kind)`. Two
valid actions of the same kind therefore collide: the first may be admitted and the
second becomes `SCRIPT_EFFECT_REQUEST_CONFLICT`. Replace that identity without rerunning
existing scripts or losing already admitted child Operations.

## 6. Action lifecycle and retry

A rejected action is not the same as an unexecuted script. Parent completion must retain
all ActionRecords even when some current authority/subject checks fail.

- An admitted child Operation follows its domain lifecycle and is never recreated by
  rerunning the script.
- An unknown child is resolved through its own `operation.get`/domain readback.
- A deterministic current-state rejection remains attributable to its action index.
- Policy may retry only an unadmitted action after a relevant authority/subject change,
  using the same ScriptRun/action identity and original payload.
- Already admitted or settled actions are never repeated.

Do not create a script-action workflow engine. A small indexed child relation and the
ordinary Operation lifecycle are sufficient.

## 7. Public readback

`script.describe` currently points clients to `operation.get`, but the public
`script.run` projection reports `result_status=not_projected`. The Manager therefore
cannot read Store-applied/rejected action outcomes or follow child Operation IDs through
the advertised interface.

Target readback for the parent returns only bounded safe fields:

```text
run state and error code
result/stdout/stderr artifact references
ordered ActionRecords
child Operation IDs
resource/custody status
```

Then each child is read through its ordinary public operation/domain view. Raw script
content, private paths, environment and unbounded output remain hidden.

## 8. Closed action vocabulary

Current source supports only:

```text
task_create
manager_notification
Task-owner mailbox message
```

The Task-owner message is `message.send`; it is not native `agent.send`.

Retain common product actions outside scripts. Add a typed script action only when a
current consumer needs it and the ordinary handler already owns authorization and
lifecycle. Candidate retained actions may include exact Send, Reply, Configure or Goal.
Each action has a closed schema and current object scope.

Never add arbitrary method names, generic JSON Store passthrough, shell-generated
credentials or a second permission registry.

## 9. Transfer and automation events

Automation consumes ScriptRun lifecycle only after exact observation, Operation,
ScriptRun, terminal result and timestamp readback. It receives bounded status,
occurrence identity and ScriptRun ID; raw script result does not enter the trigger.

Pending trigger intents survive transfer/edit with their original occurrence and are
revalidated under the successor's current Manager, project, entry, script revision,
source and Task/Attempt scope. Held or invalid work remains visible; it is not silently
reattributed or replayed.

An already admitted ScriptRun retains its original represented Manager. Its returned
actions use the retained cause but must satisfy current rights/subject state before
child admission. Transfer never rewrites the original run or child authorship.

## 10. Cancellation

Queued `script.run` uses ordinary `operation.cancel`. The same transaction marks the
run terminal-before-start and emits one exact `script.cancelled` lifecycle event. No
worker starts.

After start, generic Operation cancellation refuses the in-flight mutation and there is
no current public exact ScriptRun cancellation command. The five-minute execution bound
is not a cancellation API.

Target mid-run cancellation, if retained, must be ScriptRun/token-addressed, persist
intent before signalling the exact owned process family, retain partial bounded output
and settle only after family departure. It cannot infer cancellation from timeout,
direct-child exit or a dropped connection.

## 11. Executor selection and finite cut

`Config.scripts.executor` is optional today. A queued run without a pin is durably
admitted and then settled as `SCRIPT_EXECUTOR_UNSELECTED` before execution. The legacy
`ScriptWorker` compatibility launcher remains but delegates to the standalone worker.

Target:

1. reject new admission before creating a ScriptRun when no verified executor is
   configured;
2. select standalone `swarm-script-worker` for all new work;
3. disposition concrete retained runs;
4. delete the compatibility launcher/config path and duplicate fixtures;
5. keep one Store control/recovery protocol and historical readback.

## 12. Verification

Minimum connected cases:

- exact request replay versus changed invocation conflict;
- no interpreter start before exact Store Allow;
- host restart with live, terminal, proven-departed and damaged process families;
- identical terminal replay admits no second action;
- two same-type actions receive distinct indexes and child Operations;
- changed payload under one action index conflicts;
- deterministic rejected action remains readable and may be retried without rerunning
  the script;
- unknown child is resolved through its own Operation, not parent replay;
- transfer/disable preserves admitted run identity and rechecks future action rights;
- queued cancel starts no worker; retained mid-run cancellation proves family departure;
- missing executor rejects before durable run admission;
- raw result/private environment never leaks through public readback.

During construction run format and minimal affected warnings-denied Clippy/build.
Installed Windows/Linux custody, power loss and load remain separate qualification.
