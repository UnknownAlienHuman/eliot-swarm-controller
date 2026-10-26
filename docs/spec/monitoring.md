# Monitoring

Sources are independent and bounded. Failure reports health/gaps and never invents a
fact owned by another source.

## Source ownership

For each `(agent,generation,event family)` exactly one source is authoritative. Current
projection stores owner, stable open id/key, Store sequence watermark, dedupe mode and
last native cursor. A backup may track raw position privately but cannot publish
lifecycle concurrently.

Initial epochs open before the first native start/input effect. Planned handoff is a
single predecessor-aware `source.open` transaction that verifies old watermark/cursor,
closes or gaps the predecessor and transfers family ownership. Separate close/open is an
explicitly gapped boundary.

Dedupe modes:

- `strict_cursor`: resume strictly after committed total-order vendor cursor;
- `event_id`: compact native-event receipts survive event-row retention;
- `none`: reconnect starts after a visible gap and never replays history.

A well-formed source-owned stale binding or vendor duplicate may advance source sequence
transactionally while returning the original Store event seq or one bounded diagnostic.
Malformed envelope, undeclared family, missing required native identity or digest
mismatch does not advance. Wrong generation/turn/request and replay are never ordinary
lifecycle or automation triggers.

## Process identity and custody projections

Process identity is `(host_boot_id,pid,start_time)`, never PID alone. Linux uses kernel
boot id plus proc start ticks. Windows uses stable machine identity plus boot-session
identity and `GetProcessTimes` creation time. Reboot cannot match an old row.

A custody owner is one stable string:

```text
agent:<agent-id>#<generation>
service:<service-name>
action:<run-id>/<action-index>
workspace:<agent-id>/<step-index>
```

A process belongs to exactly one owner. Shared-service membership of Agents is native
session metadata, not duplicate process ownership.

```sql
CREATE TABLE custodies (
  owner TEXT PRIMARY KEY,
  backend TEXT NOT NULL,
  capability TEXT NOT NULL,
  native_id TEXT,
  guardian_boot TEXT,
  guardian_pid INTEGER,
  guardian_start INTEGER,
  state TEXT NOT NULL,
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

Daemon/OS monitor is authoritative for `process.started`, `process.exited` and stats. It
registers plugin/harness processes before execution where possible and enumerates exact
custody. A per-agent plugin may additionally submit sequenced `process.put` for a
descendant it discovered; host pins exact birth and verifies custody membership before
upserting the same projection. Host enumeration and plugin registration publish one
start event. Shared plugin cannot assign its server or uncontained service child to an
Agent. Managed/attached service processes belong to `service:<name>`.

Internal Git/script/check/workspace runners register their action owner directly in the
spawn/custody transaction. Restart never reconstructs custody from PID guesses.

Process custody does **not** choose API authority. Connected API calls require explicit
owner/Agent/automation credentials; absence of a token is invalid. Process projection
exists for ownership, cleanup and diagnostics only.

## Process custody backends

### Windows exact

Per-agent plugin/descendants enter a no-breakaway Job before execution resumes. Every
script/Git/check/workspace action has its own Job. Daemon-owned stdio Jobs use
kill-on-close; managed services use separate named non-killing Jobs. Force verifies
boot-scoped identity and zero active members.

### Linux exact and degraded

Exact custody requires delegated cgroup v2 (or qualified equivalent) plus an internal
owner-death guardian. Guardian lives outside the owned group, monitors daemon boot/
process identity and kills/verifies the group on abnormal daemon loss. Clean shutdown
disarms only after group empty.

Guardian boot/PID/start and health are monitored. Missing/mismatched guardian blocks new
exact launches and visibly downgrades existing groups; daemon never silently claims
owner-death cleanup.

Process-group fallback may observe/gracefully stop non-escaping members. A same-user
child can `setsid`, so capability is degraded: exact force, kill-on-owner-loss and proof
all descendants are gone are unsupported. Other Unix remains degraded until qualified.

### Shared services

Shared plugin/service is watched once. Agent stop uses exact native session control,
never server kill. Attached owner service is external and never stopped. Managed
service custody/process projection is separate from Agents/actions.

## Process observation and terminal settlement

Session lifecycle and descendant containment are distinct:

- `stopped` only from exact native terminal/close or exact normal-end readback;
- `lost` when exact session control is proven gone:
  - exact backend empty plus authoritative source drained/gapped;
  - shared-session readback says subject absent; or
  - degraded backend controlling plugin/controller identities are gone and authoritative
    channel is EOF/gapped, while escaped descendants remain possible;
- degraded third case sets custody `orphan_possible`; `lost` describes session control,
  not proof every descendant died.

Additional rules:

- exact custody enumerates the complete container; prior-boot rows are historical;
- stats are ephemeral and never model progress;
- unknown exit code stays unknown;
- process exit/final native output may race; drain authoritative source to EOF or a
  bounded terminal deadline;
- timeout alone is not terminal evidence; one exact/shared/degraded control-loss
  condition must also hold;
- live shared server says nothing about one session;
- late terminal data after source closure is diagnostic.

## Starting processes

- Resolve executable fully; argv/cwd/env are separate and bounded.
- Apply containment before execution.
- Only intended child handles are inheritable.
- Long input uses stdin/files; Eliot text is UTF-8 without BOM and LF.
- Git prompts/editor are disabled.
- Environment follows minimal/inherit policy; Windows keys merge case-insensitively.
- Running script/hook/plugin paths are immutable/versioned.
- Normalize paths; never follow untrusted symlink/reparse point for cleanup,
  fingerprint or setup.

## Workspace preparation

`launch --worktree` is durable pre-start work over a frozen
[workspace binding](configuration.md#frozen-workspace-binding).

```sql
CREATE TABLE workspaces (
  agent INTEGER PRIMARY KEY REFERENCES agents(id),
  project TEXT NOT NULL,
  repo_path TEXT NOT NULL,
  requested_branch TEXT NOT NULL,
  base_ref TEXT,
  base_oid TEXT NOT NULL,
  worktree_path TEXT NOT NULL UNIQUE,
  setup_revision TEXT NOT NULL,
  state TEXT NOT NULL,
  outcome TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE workspace_steps (
  agent INTEGER NOT NULL REFERENCES workspaces(agent),
  idx INTEGER NOT NULL,
  kind TEXT NOT NULL,
  intent TEXT NOT NULL,
  state TEXT NOT NULL,
  outcome TEXT,
  custody_owner TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(agent,idx)
);
CREATE INDEX workspace_steps_open ON workspace_steps(state)
  WHERE state IN ('queued','dispatched','unknown');
```

Flow:

1. create Agent generation 0 plus `workspaces.state=preparing`, deterministic path and
   immutable binding;
2. journal repo/branch/diagnostic base ref/exact `base_oid`/path/setup revision;
3. run explicit `git worktree add` against frozen OID under workspace custody;
4. read back worktree list, branch and exact HEAD OID;
5. run immutable setup artifacts sequentially, each as its own journaled step/custody;
6. commit ready; only then allow native start.

No implicit fetch/ref re-resolution occurs at dispatch. Existing branch/path must match
frozen OID or an explicit reuse policy. Lost Git response is read back, never repeated
blindly. Lost setup has no generic readback: workspace becomes unknown and launch stays
blocked until owner resolution/removal/recreation. Setup is never assumed idempotent.

Cancel/stop during preparation terminates the current exact action. Git step is read
back; interrupted setup leaves workspace unknown. Native generation never starts from a
partial workspace.

Cleanup is explicit, stopped-Agent only, exact path under data root and Git-clean except
allowlisted relative scratch. Scratch cleanup rejects symlink/device/alternate-stream
escape. Branch remains.

## Git and filesystem monitor

One monitor per distinct live worktree:

- read-only `git --no-optional-locks status --porcelain=v2 --branch -z`;
- bounded log on HEAD change;
- frozen-base-OID-vs-HEAD diff for still-unmerged paths;
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

Generation-matched edit events upsert with their event. When Git proves a path clean,
remove rows. Terminal/superseded generation remains until paths clean for provenance but
is not a current worker. Restart loads the projection rather than replaying event history.
Unowned shared-checkout path is `unattributed`.

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
| old boot/generation/source sequence/native receipt | historical diagnostic only |

## Restart reconciliation

1. compute current boot id; prior-boot live rows cannot match;
2. reopen exact named custody/service/guardian identities for current boot;
3. reattach sessions and snapshot commands, inputs, turns and source watermarks;
4. reconcile source dedupe mode: strict cursor resumes after cursor, event-id checks
   receipts, none creates a gap;
5. settle missing exact groups/degraded controller loss only under terminal rules;
6. reconcile preparing/unknown workspaces against frozen base OID/setup journal;
7. rebuild Git facts and load edit projection;
8. keep dispatched command/action/workspace effects unknown until readback;
9. report `routing_key_error` and block new Agent-token issuance if key identity
   mismatches; owner credential remains independent.

## Views

Expose boot-scoped process identity, custody owner, generation/turn/input gate/open stop,
workspace binding/state, exact/degraded custody/guardian/orphan possibility, source
open ids/keys/dedupe modes/gaps/watermarks, model progress, usage, branch/edit, provider
holds, routing availability and Store/WAL health.
