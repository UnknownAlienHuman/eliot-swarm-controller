# Decisions

Short records of non-obvious product choices. Change the owning entry in the PR that
changes behavior; do not append a contradictory addendum.

The mechanisms below are intentionally borrowed from the donor implementations in
[reference/donors](reference/donors.md). Eliot copies their small correctness
boundaries, not their applications or workflow systems.

| ID | Decision | Why |
| --- | --- | --- |
| D-01 | **v2 is a fresh layered build.** v1 is frozen at commit `dc6f70aa`; L0.1 creates tag `v1-final` there before deleting v1 from `main`. | v1 reached about 533k Rust lines and 61k lines of process/audit documents without qualifying its core path. |
| D-02 | One `eliot` binary contains the CLI, daemon entry, built-in plugin entry, hook shim and optional MCP entry. | One installation and one version; no sibling-artifact platform. |
| D-03 | A process-owning plugin runs once per controlled Agent generation. A shared plugin is allowed only for a native service that already isolates sessions, attributes every event to one exact session and can stop one session without killing the service. | This is the custody boundary used by `sandbox-driver` and Waku: replacement generations do not inherit old handles, and adopted services are never killed for one session. |
| D-04 | One SQLite database in WAL mode, one writer task and numbered `user_version` migrations. | Enough for one host and millions of logical Agents; no database server or schema-hash ceremony. |
| D-05 | Persist bounded normalized events with a global `seq`; deltas and process samples are ephemeral. Native transcripts remain the detailed record. | Replayable state without storing token streams. |
| D-06 | Core has no Task/Issue/review/acceptance/publication engine. Automation actions are ordinary commands. | That product was not requested and was v1's largest growth source. |
| D-07 | One local OS user is the trust boundary, but every API call still carries an explicit principal for correctness: owner CLI automatically reads one private local owner token; Agents and managed scripts use scoped tokens. Missing, incomplete or mismatched identity rejects and never becomes owner. | Same-user credentials are not hostile-user security, but they prevent an Agent whose environment was filtered from accidentally receiving owner authority. CCCC and MCP Agent Mail likewise bind work to explicit actor identities. |
| D-08 | Agent command authority is `self`, `tree` or `project`; owner/global automation may target all projects, project automation only its project. | Covers manager/child operation without policy editions or handover epochs. |
| D-09 | Native Goal and Eliot Goal are independent; at most one of each may exist per Agent. | Preserves native autonomy while providing a uniform fallback. |
| D-10 | Rules are strict TOML. Event rules have a durable cursor and at most two stored definitions during an edit: the old definition processes through captured high-water H, the new definition starts after H. A second semantic edit is deferred until the retiring definition drains. Actions are commands with deterministic ids. | This is the smallest gap-free edit boundary; no activation hierarchy, unbounded definition queue or generic run engine. |
| D-11 | `who` combines current Git facts, durable Agent edit attribution and advisory claims; history is provenance, not current ownership. | Git alone cannot attribute dirty bytes in a shared checkout. |
| D-12 | Claims are advisory and never lock files. | Eliot cannot enforce a lock against an unrestricted native shell. |
| D-13 | First real harness is Codex app-server; first provider proxy is OpenCodex. | Codex exposes the richest native control protocol and is a direct Rust first slice. |
| D-14 | **No native or external effect with an unknown outcome is retried automatically**, including start, input, interrupt, stop, setup, script and Git push. | ACPX, Agent Sessions and sandbox-driver all keep uncertain work distinct from safe retry. A late retry may duplicate work or hit a replacement turn/session. |
| D-15 | `docs/` contains current specification, roadmap and compact donor/research references only. | No audit/readiness/report hierarchy. |
| D-16 | Documentation is English; the owner's source statement in vision remains verbatim Russian. | Exact owner intent plus the language of vendor documentation and implementation. |
| D-17 | Harness/software versions are reported and recorded with fixtures, never used as authority gates. | Version equality does not prove semantics. |
| D-18 | Early stdio sessions are daemon/plugin owned. Long-lived continuity is added only through a native service with exact re-attach. | No detached ambiguous processes; Waku's adopted-service rule is the model. |
| D-19 | `on_idle` Eliot Goal waits while an active native Goal drives; explicit cadence may intentionally queue another turn. | Avoids accidental dual idle loops. |
| D-20 | Commands use readiness lanes. Ready native calls serialize per Agent; future queued sends stay FIFO but do not block exact control/request replies for the current turn. An unresolved input/turn/transition gate blocks later send, configure and Eliot Goal continuation. | Preserves configure→send ordering without allowing unknown input or takeover overlap. |
| D-21 | Capacity is capped by explicit profile, account and proxy scopes; provider holds are persisted per account/route. | Several profiles can spend one subscription, and restart must not launch a second probe over an unknown first probe. |
| D-22 | One stable Agent/native subject has one monotonically increasing `generation` fencing the current controlled/observed incarnation. A generation-changing command stores expected and target generation; dispatch advances the fence before native I/O and it never rolls back. Every plugin message, command, turn, request, claim and Goal carries the fence. There is no binding revision layer. | Agent Sessions uses a monotonic fence token; sandbox-driver numbers replacement plugin generations. One integer prevents late old-process output from mutating replacement work. Keeping an unknown target generation blocked is safer and simpler than rollback/candidate lease machinery. |
| D-23 | Session start, optional first input, input-to-turn association and terminal turn result are separate outcomes. | ACPX exposes queue admission, prompt start and final result separately; `session.ready` must never prove that a prompt ran. |
| D-24 | One `(native_scope,native_ref)` maps to one canonical Agent row. Attach, detach, resume and control takeover change that row/generation rather than creating another row for the same native subject. | Duplicate rows duplicate lifecycle, automation and capacity. |
| D-25 | Exactly one source owns each normalized lifecycle family for `(agent,generation)`. Initial ownership commits before native I/O; dynamic replacement is one predecessor-aware compare-and-swap handoff with an opaque cursor or explicit gap. A retriable durable event has a stable normalized native id; same id/different payload is integrity failure. There are no source epochs, open ids or source sequence numbers. | ACPX has one authoritative journal; Paseo separates immediate live delivery from authoritative history; Waku subscribes before create and reconciles on reconnect. This is the minimal implementable boundary. |
| D-26 | Harness environment is minimal by default; inherit-all is an explicit global-profile opt-in. | Prevents unrelated daemon credentials leaking into every Agent/tool. |
| D-27 | Project config may only narrow an owner-defined global profile. Routes, executable/plugin, account/proxy, environment and broader permissions/control are global-only. | Agents can write the repository; project files cannot widen authority or spending. |
| D-28 | Custody claims only what the OS proves: Windows Job is exact; Unix process group is degraded until a qualified cgroup/equivalent is implemented. | Process groups neither die with the daemon nor prevent `setsid` escape. |
| D-29 | Worktree admission freezes the local `base_oid` and one durable preparation phase (`creating`, `setup`, `ready`, `unknown`, `failed`). Unknown setup is never rerun. | Moving refs and non-idempotent setup must not change queued work after admission. |
| D-30 | External mutation ids are bound to `(principal, method, canonical payload)`. Commands store that binding directly; non-command writes use one small `writes` table. The single writer checks both id namespaces. Missing old ids are rejected after the supported retry window. | CCCC persists a delivery id plus actor/generation/content binding and rejects the same id with different content. No universal receipt/floor subsystem is needed. |
| D-31 | Bounded implementation inputs are frozen at admission without a global artifact store: small notes may be copied into the Store; files/scripts store canonical allowed path, size and digest and are verified before dispatch. | Mutable checkout files must not rewrite admitted work, but content-addressed artifact GC is unnecessary. |
| D-32 | Restart is level-reconciled from durable current state through deduplicated keys; callbacks only wake reconciliation. Reconciliation never blindly repeats a non-idempotent external effect. | controller-runtime's level-based pattern avoids a second edge-driven state machine. |
| D-33 | Mail has one stable delivery row per concrete recipient, with sender and recipient generation, content digest and exact acknowledgement id. Selector sends share a thread but derive one delivery id per recipient. Queue delivery never injects the full body without ordinary command authority. | CCCC binds delivery/reply/cancel to actor generation and digest; MCP Agent Mail keeps read/ack state per recipient. |
| D-34 | L0–L6 are implementation-first: formatting plus the smallest changed-package Clippy gate. Targeted regressions are added only where a local contract cannot be reviewed safely. The complete deterministic, crash/restart, conformance, live, platform and load campaign starts after the connected feature set exists. | Follows the owner's current development instruction and avoids rebuilding v1's ceremony while retaining a real final qualification gate. |
| D-35 | Linked-input and selector fanout use parent/child fields on ordinary command rows. Retention treats each parent set as a unit: an open/unknown child retains the parent and siblings required by the API; terminal children are pruned before the parent; after the complete set is pruned, `command.children` returns `history_gap`. | Avoids a separate link subsystem while keeping the parent foreign key implementable and preventing partial fanout history. |

## Defaults

| Topic | Default |
| --- | --- |
| L4.7 harnesses | Do not build until a concrete need. |
| Git identity | Per Agent; `git_identity = "inherit"` opts out. |
| Native child without isolated environment | `own_identity=false`; it is controlled through its parent plugin and receives no independent CLI principal. |
| Selector fanout | `limits.max_selector_targets = 1000`; no logical fleet-size limit. |
| Environment | minimal; `inherit_env=true` only in trusted global profiles. |
| Source reconnect without an opaque cursor | start at live edge and record a gap; never replay retained history heuristically. |
| Remote access / multi-host / GUI / Memory OS | Not built without a new owner decision. |
