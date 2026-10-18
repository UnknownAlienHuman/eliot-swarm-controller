# Claude-authored Swarm scripts — mechanisms to keep

Original review: 2026-10-10. Sources reread on 2026-10-18 against controller
`f7e1b63`: the owner's `eliot-swarm-scripts-2026-10-10.zip`, the original
`agent_swarm.md`, the October 6 Swarm audit and October 8 master audit v26.
The ZIP contains 157 files and 15,308 decoded source lines. This pass closely
reads the runners, monitors and message senders below; it does not claim a fresh
line-by-line audit of the whole archive.

Paths below are relative to `eliot-swarm-scripts/` inside the ZIP. These are
Claude-authored operations for several harnesses, not the Claude Agent SDK bridge.
The archive's operational databases were removed earlier; dated incident comments
are historical evidence, not measurements reproduced in this review. No archive
script was executed. Machine configuration and raw private scripts stay outside
this repository.

## Two Goal mechanisms

| Source | What it actually does | Keep in Eliot |
| --- | --- | --- |
| `v2/codex_as.py:194-228,330-353` | Sets a native `thread/goal/set` objective and follows turns started by the server. Comments describe native Goals continuing after a client departed. | Harness Goal is native state with its own controls. Observe it independently of runner/PID lifetime. The Governor uses it by default where supported. |
| `v2/Remind-Subagents.py:3-25,79-162,163-236` | Sends ordinary OpenCode prompts or Codex/Muse steer-inbox messages. A condition may cause the same reminder again after the configured cooldown; later comments change the historical policy. | A small Eliot-side Goal plugin, using ordinary `agent.send`, existing event/interval scheduling and the Governor's selected content/cadence. Intentional repetition does not require a new semantic reason. |
| `v2/codex_as.py:363-368` | Delivers steer files through `turn/steer` while the same runner follows native Goal turns. | Both mechanisms can coexist. The native Goal and Eliot messages need not have identical objectives. There is no exclusive driver switch. |
| `v2/long_session_nudge.py:35-75` | Sends one ordinary message per lane/session-start key after its age/no-result condition. | One-shot or repeated delivery is selected policy. Replaying one scheduled occurrence must not resend it; a later occurrence may intentionally carry the same text. |

The old specifications' single-continuation-owner wording was too broad. The
owner's 18 October clarification supersedes that restriction. Keep separate
Harness Goal control and Eliot plugin enablement. Pausing one does not pause the
other or cancel already admitted work. The current controller's shared progression
is terminal-event-only and still branches by vendor; it is not yet the general
message plugin. [Implementation](../../agent-operations/implementation.md#harness-goals-and-the-eliot-goal-plugin)
names the replacement owners and callers.

## Passive monitoring

The useful system was a join of local sources, not just a status request to a
harness. Preserve that capability while replacing its identity heuristics.

| Source | Observed mechanism and failure lesson |
| --- | --- |
| `v2/Agent-Watch.py:12-93` | Reads OpenCode SQLite sessions/messages in read-only mode. Parent IDs delimit the family; current tool state distinguishes foreground-child waiting from an unanswered stream. The script notes that session update time stayed unchanged while tools ran, so it also reads message activity. |
| `v2/Agent-Watch.py:146-209` | Joins shared Codex app-server JSONL with rollout `parent_thread_id`. The earlier monitor counted all lanes' children for every manager; exact parent matching corrected that overcount. A shared event stream is not a per-agent stream. |
| `v2/Agent-Watch.py:210-279` | Reads local Codex rollout files and separately reports app-server event freshness. A dated comment records clients hanging after server failure while the recent-activity window still showed workers as healthy. Last activity and connection health are different facts. |
| `v2/muse_owner_activity.py:3-42` | Reads Muse `session.jsonl` streams because the old subagent summary could remain `inProgress` for silent work. Preserve actual events; replace truncated stream IDs and path/title guesses with full observed native identities. |
| `v2/Lane-Status.ps1:6-59` | Combines PID files/OS checks, worker JSONL, native session IDs, errors and Muse session logs. Muse stdout buffering motivated the local native-log read. Its OpenCode HTTP query means this particular script was not entirely passive. |
| `v2/conveyor_status.py:25-88`, `v2/conveyor_task.py:281-288,794-826` | Joins task, owner, lane and assigned branch from local records. This establishes assignment, not the current checkout HEAD. Observe current worktree/Git state separately. |

Use existing binding/native IDs, Task/Attempt/Operation, workspace leases and
process ownership. At launch or attach retain the relevant native log/database
reference and session identity. Show OS birth identity with PID; one shared server
may serve many sessions, and a native child may have no separate OS process.
Keep local branch/HEAD/dirty state separate from the last observed GitHub head.
An unknown association remains visible instead of attaching it to the newest log.

Provider-local reads belong in the selected adapter and continue when its native
control connection is lost. OS and Git collection remain independent. Fold bounded
incremental observations into the existing local projection; the Governor can
read it at any time without querying the harness. Retain source times, cursor,
coverage and errors. Optional diagnostic logging and automation cannot disable
this observation. SDK-supported disk readers can replace private parsing where
available; current sources are in the [donor map](../../agent-operations/donor-map.md#local-observation-sources).

Do not copy the fixed age thresholds, newest-manager selection, title/path regex
ownership, PID-only identity, six-hop family truncation or timestamp-only death
claims. An old provider status, low CPU or unchanged branch is not enough to kill,
restart or reassign. `v2/codex_threads.py:17-58` uses live API calls and can clear
Goals/interrupt suspected orphans; that destructive policy is not a passive reader.
The old audit's concern about private DB formats calls for a small adapter-owned
reader and visible schema gaps, not removal of provider-local observation.

## Other useful mechanisms

| Source | Mechanism and existing Eliot owner |
| --- | --- |
| `v2/make_task.py:1-16,40-69,72-156` | Build one compact current task from selected source material, remaining findings and checklist IDs; search cited symbols once for the selected revision. Reuse `TaskSpec::brief`, frozen Attempt context and Store `task_prompt.rs`. Missing textual matches are a diagnostic, not proof of dead code or an admission gate. |
| `v2/issue_settled.py:29-99` | Omit already delivered, parked or review-waiting work until relevant input changes. Use existing Task/Attempt/submission/review and queue projections; no second Markdown/JSON task authority. |
| `v2/state.py:650-731` | Preserve a refuted claim, its source coordinates and why it failed, so another agent does not repeat the investigation. Use existing feedback records; an advisory cached finding is not a permanent ban or a new hash-based approval system. |
| `root-tools/tooltime.py:20-81`, `root-tools/ctxfill.py:22-78`, `v2/oc_timeline.py:53-87` | Separate tool time, waiting and context/output volume. Reuse recorded spans and byte counters. Count overlapping intervals correctly; characters are not billing tokens, and parent waits must not duplicate child time. |
| `v2/oc_http.py:1-25,83-121`, `v2/codex_as.py:58-166` | Reuse one connection reader/demultiplexer. The archive describes CLI health probes restarting a shared OpenCode service. Observation must not invoke a self-repairing launcher; current adapters already own HTTP/SDK connections. |
| `v2/answer_forms_http.py:16-60` | A queued steer does not answer a blocked foreground question. Use the existing exact pending-request/reply and supported background controls. The script's automatic first/recommended answer was historical policy, not a universal default. |
| `v2/plan_next.py:37-65`, `v2/state.py:1169-1196` | Rank actionable dependencies and locality; show blockers that hold several tasks. Use current Task dependencies and queue consumers, with bounded cycle-aware traversal and honest missing data. |
| `root-tools/write_rescue.py:1-40` | Compare surviving unpublished heads to current source and give the next worker a short head/path/reason reference. Reuse still-relevant changes without recreating old branch state or duplicating sibling snapshots. |
| `v2/pr_evidence.py:39-80`, `v2/conveyor_task.py:662-710` | Associate reported checks with the delivered candidate. Use submission/CheckRun and GitHub run IDs; planned commands and prose saying `exit 0` are not executed evidence. |
| `v2/revdeps.py:1-35`, `Launch-Manager.ps1:11-34` | Check changed packages plus affected dependents and coalesce shared-service startup. Reuse Eliot's existing package-scope/resource owners; do not copy a second classifier or fixed startup lock interval. |

## What is already delivered

The first review's TaskSpec duplicate-source-ref rejection and `TaskSpec::brief`
membership-set correction are already in main. They preserve first occurrence and
existing richer source-index priority; this pass does not implement them again.
The Goal, passive-observation and queue transfers above remain source work under
the single [implementation order](../../agent-operations/implementation.md).

The archive's lane counts, provider/model choices, automatic replies, forced Muse
rotation (`v2/muse_cycle.py`), cleanup and merge/closure routines are historical
operating policy. Retain useful capability through current commands and explicit
Governor choices; do not copy those defaults, compatibility copies, credentials,
private paths or independent control loops into the product.
