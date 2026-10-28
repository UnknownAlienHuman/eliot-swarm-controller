# Decisions

Only non-obvious choices belong here. Exact fields/state transitions live in their
owning specs; do not reproduce them in every page. Existing IDs remain stable for links.

| ID | Decision | Reason / owning contract |
| --- | --- | --- |
| D-01 | Fresh v2; archive v1 at `dc6f70aa` before removing it. | v1 is a narrow code donor, not the refactor base. [Roadmap](roadmap.md) |
| D-02 | One `eliot` binary, separate plugin processes. | Simple installation and process isolation; no artifact-distribution platform. |
| D-03 | Start with per-Agent plugins. Shared services come only with demonstrated session/environment/stop isolation. | [Plugin protocol](spec/plugin-protocol.md) |
| D-04 | One SQLite WAL database, one writer, ordinary numbered migrations. | No database server, schema hashes or event-sourcing framework. |
| D-05 | Persist bounded facts; stream deltas/stats ephemerally. | Native transcripts retain full output. |
| D-06 | No Task/Issue/review/acceptance engine; actions are ordinary commands. | [Automation](spec/automation.md#actions-are-commands) |
| D-07 | Current-user IPC plus automatically supplied scoped identity. | Missing managed credentials must not accidentally become owner; not hostile-same-user isolation. [API](spec/api.md#transport-and-principal-identity) |
| D-08 | Agent control is self/tree/project; project rules remain project-scoped. | No policy editions or authority handover platform. |
| D-09 | At most one native and one Eliot Goal, independent. | Native autonomy plus explicit bounded continuation. |
| D-10 | Rule edits keep current and at most one retiring definition at high-water H. | Do not reinterpret backlog or add an activation hierarchy. [Automation](spec/automation.md#event-cursor-and-compact-definition-handoff) |
| D-11 | `who` combines Git, current Agent edits and advisory claims. | Git does not identify every writer of uncommitted shared bytes. |
| D-12 | Claims are advisory, never file locks. | Unrestricted native shells are not sandboxed by Eliot. |
| D-13 | Direct Codex/Luna is the first useful path; optional OpenCodex follows in L3. | Do not wait for proxies or more adapters before one real harness works. |
| D-14 | Never automatically replay an uncertain native/external effect. | Exact readback or explicit resolution; unsuccessful execution can also have partial effects. [Core](spec/core.md#commands-and-narrow-gates) |
| D-15 | One current spec per topic and one roadmap. | No audit/readiness/status hierarchies. |
| D-16 | Docs in English; original owner statement remains verbatim Russian. | Later explicit owner clarifications can change delivery order without rewriting the historical quote. |
| D-17 | Report versions; negotiate actual available fields/capabilities. | Version equality alone is not semantic compatibility. |
| D-18 | Early stdio sessions are owned; service continuity is a later concrete feature. | No detached ambiguous process lifetime. |
| D-19 | On-idle Eliot Goal waits for an active native Goal. | Avoid two accidental continuation loops. |
| D-20 | Ready controls bypass future queued sends, not unknown-input fences. | Exact interrupt/reply/readback remains available while uncertain turn-producing work is held. |
| D-21 | Named profile/account/proxy capacity and holds survive restart. | No second probe or overbooking behind an uncertain launch. |
| D-22 | One stable Agent and one monotonic generation fence. | Old messages/handles cannot change replacement work; no binding-revision hierarchy. [Core](spec/core.md#generation-fence) |
| D-23 | Session ready, input association and terminal result are distinct. | Acceptance of bytes is not execution or task success. |
| D-24 | One canonical Agent per native scope/ref. | Attach/resume do not duplicate lifecycle and capacity. |
| D-25 | One source owner per family; independent lifecycle, health, latest gap and last-batch receipt in `source_state`. | One unacknowledged batch per source permits bounded lost-ack recovery. New gaps get new IDs; same-ID conflicts reject. No source epochs/journals. [Plugin protocol](spec/plugin-protocol.md#gap-and-handoff) |
| D-26 | Minimal environment by default. | Inherit-all is an explicit owner opt-in, not automatic credential distribution. |
| D-27 | Project config narrows global profiles. | Repository changes cannot silently widen routing, billing or authority. |
| D-28 | Windows Job exact; Unix group explicitly degraded. | Claims match OS containment, not wishful orphan-free guarantees. |
| D-29 | One worktree preparation row records exact identity and setup phase. | No automatic repeat of unknown setup, no per-step workflow journal. |
| D-30 | Mutation IDs bind principal, method and semantic payload. | Repeated admission returns the same result; conflicts reject. Bounded `commands`/`writes`, not a universal receipt service. |
| D-31 | Freeze explicit admitted route/config and entry-file inputs. | Reload changes future admissions; a file digest is not a hermetic transitive runtime or artifact platform. |
| D-32 | Restart rereads durable/native state by deduplicated key. | A small reconciliation worker, not Kubernetes or another workflow engine. |
| D-33 | Mailbox belongs to stable Agent; automatic presentation is generation-bound. | Current authenticated Agent may explicitly read old unread mail. Do not replay old send/inject/steer automatically. Read then exact acknowledgement; bounded inbox. [Coordination](spec/coordination.md) |
| D-34 | Core/bus → useful Codex → monitoring → automation → coordination → more adapters. | Every completed connected increment has deterministic Cargo test coverage, using fake where suitable; changed-package tests and all-target Clippy run during implementation. Full crash/load/vendor/platform qualification is L7. No routine paid-model runs, line-count quotas or per-task report packets; record actual checks in PR/CI. |
| D-35 | Parent/child command rows represent linked input, selector expansion and ordered rule actions. | No link/run subsystem. Retain a referenced set; prune children then parent in one transaction, never expose partial fanout history. |
| D-36 | Optional automated Git wrappers use isolated transport configuration and frozen OIDs. | Mutable branch/rewrite settings must not retarget admitted pushes. No generic pull or interception of a harness's own Git. [Automation](spec/automation.md#git-actions) |

## Defaults

Direct Codex first; configured native model only. Optional vendors do not block the
selected release. Git identity is per Agent unless `git_identity="inherit"`. Native
children without isolated environment have no independent CLI principal. Remote,
multi-host, GUI and Memory OS are outside current scope.
