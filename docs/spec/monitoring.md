# Monitoring

Sources are independent and bounded. Failure reports health/gaps and never invents a
fact owned by another source.

## Source ownership

For each `(agent,generation,event family)` exactly one source is authoritative. Current
projection stores owner, stable source open id/key, Store source-sequence watermark and
last native cursor. Backup may track raw position privately but cannot publish duplicate
lifecycle.

Handoff begins strictly after last committed native cursor. Unprovable interval is a
visible gap. Wrong generation/turn/request, stale source sequence and vendor replay are
bounded diagnostics, never ordinary lifecycle or automation triggers.

A well-formed source-owned event that is a stale binding or vendor duplicate still
advances that source sequence transactionally: vendor duplicate returns the original
Store event seq; stale binding may create one bounded diagnostic. Malformed envelope,
undeclared family or digest mismatch is source integrity failure and does not advance.
This prevents one deterministic bad native fact from wedging all later source input
without letting an unauthorized family enter the bus.

## Process identity and custody projections

Process identity is `(host_boot_id,pid,start_time)`, never PID alone. Linux uses kernel
boot id plus proc start ticks. Windows uses stable machine identity plus native boot
session/time identity and `GetProcessTimes` creation time. Reboot cannot match old row.

A custody owner is one stable string:

```text
agent:<agent-id>#<generation>
service:<service-name>
action:<run-id>/<action-index>
workspace:<agent-id>/<step-index>
```

A process belongs to exactly one owner. Shared-service membership of agents is session
metadata, not duplicate process ownership.

```sql
CREATE TABLE custodies (
  owner TEXT PRIMARY KEY,
  backend TEXT NOT NULL,              -- windows_job|linux_cgroup|process_group|external
  capability TEXT NOT NULL,           -- exact|degraded|external
  native_id TEXT,
  guardian_boot TEXT,
  guardian_pid INTEGER,
  guardian_start INTEGER,
  state TEXT NOT NULL,                 -- live|stopping|empty|orphan_possible|unknown
  updated_at INTEGER NOT NULL
);

CREATE TABLE processes (
  host_boot_id TEXT NOT NULL,
  pid INTEGER NOT NULL,
  start_time INTEGER NOT NULL,
  owner TEXT NOT NULL REFERENCES custodies(owner),
  role TEXT NOT NULL,
  state TEXT NOT NULL,
  exit_code INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(host_boot_id,pid,start_time)
);
CREATE INDEX processes_owner ON processes(owner,state);
```

Plugin `process.put` uses the same commit-acknowledged contiguous source-sequence
contract as lifecycle events, normally through a separate process-family source key.
Host verifies boot/start identity, then commits source watermark, process projection and
normalized `process.started|exited` event in one transaction. A lost reply resends the
same envelope; history pruning never makes it a new process fact. Internal Git/script/
check/workspace runners register their action owner directly through daemon Store
transactions.

Restart never reconstructs custody from PID guesses.

### Peer caller classification

Persisted `processes` is an optimization/audit projection, **not proof that an unlisted
peer is uncontrolled**. Before no-token owner fallback, API server:

1. obtains peer PID and pins current boot/start identity;
2. looks for exact projection row;
3. on miss, checks live OS membership against every current custody:
   - Windows: pinned peer + `IsProcessInJob` on live Job handles;
   - Linux exact: pinned peer cgroup vs registered Eliot cgroups;
   - degraded: pinned peer PGID vs registered live groups, without upgrading guarantee;
4. rechecks birth/liveness;
5. grants no-token owner only when local OS user and definitively outside every custody.

Membership maps caller to custody principal and requires matching scoped token. Access
failure, birth race, incomplete inventory or ambiguity rejects
`caller_identity_unknown`. Containment applies before execution, so process-registration
lag creates no authority window.

Shared-service process still needs exact per-session token. Deliberate same-user escape
from a degraded group is outside the declared containment/security guarantee; status
reports that boundary rather than pretending hostile same-user isolation.

## Process custody backends

### Windows exact

Per-agent plugin/descendants enter a no-breakaway Job before execution resumes. Every
script/Git/check/workspace action has its own Job. Daemon-owned stdio Jobs use
kill-on-close; managed services use separate named non-killing Jobs. Force verifies
boot-scoped identity and zero active members.

### Linux exact and degraded

Exact custody requires delegated cgroup v2 (or qualified equivalent) plus internal
owner-death guardian. Guardian lives outside owned group, monitors daemon boot/process
identity and kills/verifies group on abnormal daemon loss. Clean shutdown disarms only
after group empty.

Guardian boot/PID/start and health are monitored. Missing/mismatched guardian blocks new
exact launches and visibly downgrades existing groups; daemon never silently claims
owner-death cleanup.

Process-group fallback may observe/gracefully stop non-escaping members. Same-user child
can `setsid`, so capability is degraded: exact force, kill-on-owner-loss and proof all
descendants are gone are unsupported. Other Unix remains degraded until qualified.

### Shared services

Shared plugin/service is watched once. Agent stop uses exact native session control,
never server kill. Attached owner service is external and never stopped. Managed service
custody/process projection is separate from agents/actions.

## Process observation and terminal settlement

Session lifecycle and descendant containment are distinct:

- `stopped` only from exact native terminal/close or exact normal-end readback;
- `lost` when exact session control is proven gone:
  - exact backend empty plus authoritative source drained/gapped;
  - shared-session readback says subject absent; or
  - degraded backend controlling plugin/native-controller identities gone and
    authoritative channel EOF/gap, although escaped descendants cannot be ruled out;
- degraded third case sets custody `orphan_possible`; `lost` describes session control,
  not proof every descendant died.

Additional rules:

- exact custody enumerates complete container; prior-boot rows are historical;
- stats are ephemeral and never model progress;
- unknown exit code stays unknown;
- process exit/final output may race; drain authoritative source to EOF or bounded
  terminal deadline;
- timeout alone is not terminal evidence; one exact/shared/degraded control-loss
  condition must also hold;
- live shared server says nothing about one session;
- late terminal data after source closure is diagnostic.

## Starting processes

- Resolve executable fully; argv/cwd/env separate and bounded.
- Apply containment before execution.
- Only intended child handles inheritable.
- Long input uses stdin/files; Eliot text UTF-8 without BOM and LF.
- Git prompts/editor disabled.
- Environment follows minimal/inherit policy; Windows keys merge case-insensitively.
- Running script/hook/plugin paths are immutable/versioned.
- Normalize paths; never follow untrusted symlink/reparse point for cleanup,
  fingerprint or setup.

## Workspace preparation

`launch --worktree` is durable pre-start work over a frozen
[workspace binding](configuration.md#frozen-workspace-binding):

1. create Agent generation 0, `workspace_state=preparing`, deterministic path;
2. journal repo, requested branch, diagnostic base ref, exact `base_oid`, path and setup
   revision;
3. run explicit `git worktree add` against the frozen OID under workspace custody;
4. read back `git worktree list --porcelain`, branch and exact HEAD OID;
5. run setup argv sequentially, each in its own custody;
6. commit ready; only then allow native start.

No implicit fetch/ref re-resolution occurs at dispatch. If requested branch already
exists, its exact current OID must equal frozen `base_oid` (or explicit policy must name
reuse); otherwise fail. A moving branch/remote ref after admission cannot alter the
workspace.

Lost Git response is read back, never blindly repeated. Lost setup has no generic
readback: workspace becomes unknown, launch stays blocked, owner resolves or removes/
recreates. Setup is not assumed idempotent.

Cancel/stop during preparation terminates current exact action. Git step is read back;
interrupted setup leaves workspace unknown. Native generation never starts from partial
workspace.

Collision/mismatch fails. Cleanup is explicit, stopped-agent only, exact path under data
root and Git-clean except allowlisted relative scratch. Scratch cleanup rejects symlink,
device and alternate-stream escape. Branch remains.

## Git and filesystem monitor

One monitor per distinct live worktree:

- read-only `git --no-optional-locks status --porcelain=v2 --branch -z`;
- bounded log on HEAD change;
- frozen base-OID-vs-HEAD diff for still-unmerged paths;
- watcher debounce 500 ms, minimum refresh 2 s;
- slow status backoff plus health;
- remote refs only after explicit fetch or optional `ls-remote`.

Monitor never writes index, refs or objects.

### Durable edit attribution

```sql
CREATE TABLE agent_edits (
  agent INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  workspace TEXT NOT NULL,
  path TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,generation,path)
);
CREATE INDEX agent_edits_workspace ON agent_edits(workspace,path);
```

Generation-matched edit events upsert with event. When Git proves path clean, remove
rows. Terminal/superseded generation remains until paths clean; restart never rebuilds
from event history. Unowned shared-checkout path is `unattributed`.

### Read-only progress fingerprint

```text
HEAD + sorted(dirty/untracked path, file type, size, content hash)
```

Hash only dirty/untracked excluding ignored/scratch; hash symlink target text without
following. Cache until watcher/metadata change and enforce caps. Incomplete is
unavailable, never no-progress. No temporary index, `git add`, object writes or
`write-tree`.

## Liveness

Only exact-generation model progress updates `last_progress_at`.

| Observation | Conclusion |
| --- | --- |
| no progress while busy/waiting | stalled flag only |
| parent idle, child busy | parent idle; tree incomplete |
| process alive, stream disconnected | source stale; state unchanged |
| exact custody empty + source drained/gapped | lost; no possible members in custody |
| degraded controller gone + source EOF/gap | lost session control; custody orphan possible |
| shared server live, session readback unknown | stale/unknown, not lost |
| handoff gap | explicit gap; no inferred event |
| old boot/generation/source sequence | historical diagnostic only |

## Restart reconciliation

1. compute current boot id; prior-boot live rows cannot match;
2. reopen exact named custody/service/guardian identities for current boot;
3. reattach sessions and snapshot commands, inputs, turns and source watermarks;
4. settle missing exact groups/degraded controller loss only under terminal rules;
5. reconcile preparing/unknown workspaces against frozen base OID/setup journal;
6. rebuild Git facts and load edit projection;
7. keep dispatched command/action/workspace effects unknown until readback;
8. report `routing_key_error` and block token issuance if key identity mismatches.

## Views

Expose boot-scoped process identity, custody owner, generation/turn/input gate/open stop,
workspace binding/state, exact/degraded custody/guardian/orphan possibility, source
open ids/keys/gaps/watermarks, model progress, usage, branch/edit, provider holds,
caller-classification failures, routing-key health and Store/WAL health.
