# Lessons from v1

Historical reference: `dc6f70aa`, preserved on branch `archive/v1-final`. These are
constraints learned from v1 records, not new runtime measurements. Do not make reading
the old audit history an implementation prerequisite.

| Failure | Small v2 response |
| --- | --- |
| Many commits, abstractions and reports without a connected usable path | First core/bus, then a real Codex task. Later capabilities cannot block it. No line-count gaming or readiness packets. |
| Contradictory addenda and the same policy copied across pages | One owning spec; fix it in place. Roadmap alone owns delivery order. |
| Several competing executors for one harness | One selected adapter path; replacement deletes the old path. |
| Proof/hash/binding/activation machinery became a product of its own | One Agent generation, exact native ids, ordinary transactions and bounded current projections. No generic proof platform. |
| Lost responses led to repeated prompts, setup or pushes | Unknown is not failure-to-apply. Exact readback/manual resolution, no automatic uncertain replay. |
| Parent idle, process exit or stream silence was called completion | Exact terminal result; drain buffered source; children have their own state; gaps remain visible. |
| Shared process ownership was treated as exact per-Agent termination | Per-Agent custody first; shared only with native isolation. Windows Job exact, Unix honestly degraded. |
| Hooks, transcripts and streams duplicated lifecycle | One family owner and explicit cursor/gap handoff when that feature is needed. |
| Queue/read notifications were confused with actual model delivery | Separate durable mail, generation-bound presentation and explicit exact acknowledgement. |
| Git monitoring wrote indexes and caused contention | Read-only monitoring, cached facts and bounded fingerprinting; no tool-by-tool snapshots. |
| Timers existed only in chat or disappeared on restart | Persist schedules and reconcile ordinary commands, not a workflow platform. |
| Requested route differed from effective route | Keep both visible; unavailable evidence is unknown, not permission to silently change provider. |
| Windows paths, inherited handles, process lifetime and file locks failed | Reuse narrow OS helpers and standard process APIs; verify actual platform behavior at L7. |
| Broad tests and audit ceremonies displaced product implementation | Code-first scoped compile gate plus brief development exercise; complete qualification after the selected feature set exists. |

Donor source and extraction boundaries are in [donors](donors.md). v1 contributes small
OS/pipe/protocol helpers, not its Store/workflow schemas, receipts, compatibility
executors, audit tooling or CI patchers. A required safety invariant is not permission to
recreate the old machinery around it.
