# Implementation status

## Current state

The PR22/PR23 program is at a partial foundation stage. **Ten grouped delivery
blocks remain**; they vary in size and are not ten equal tasks or a completion
percentage. The current catalog has 91 `ToolSpec` entries, but catalog size
does not establish complete handlers or native-client loading.

C6 plus the wired bounded local TaskSubmission-intake consumer is committed at
`e035c0c3fe855490863be81902c5152b548c42cd`. Owned-crate formatting and
`cargo clippy --locked --lib --bins --no-deps -- -D warnings` passed for this
commit (9.60 s; `.local/qualification/r7-build-gate/clippy-c6-publish-repaired.log`).
The bounded exact-commit review of C6 privacy, actor, workspace, no-replay and
overlap behavior passed. No Cargo tests, native process, or model execution ran
for this increment; new CI is pending after the main push. C5's last published
CI run failed on Linux `start_ticks` parsing and a Windows stdout fixture;
source repairs are included in this commit. C4's 221 Rust tests apply only to
`2607c8858e573ae40459c27d76d8ae9e1ca9f8fc`. Separate C7 Participant credential
issuance, broader readback, native capability proof, and disposition/lifecycle
modules remain unwired source WIP. Productive launch and the full manager-owned
cycle remain unqualified; actual native-MCP harness loading remains unknown.

C6 committed slices include five passive watch kinds, integration sync,
recomputed overlap, and manager-admitted asynchronous workspace lease / exact
Task claim / `agent.open`. Productive launch still stops at credentials and
native-MCP capability; productive dispatch is not implemented. Unsupported
watch predicates, broader program stages, and end-to-end recovery remain open.

## Additional qualification evidence

The Codex composition files `common.mjs`, `service.mjs` and `trial.mjs` passed
syntax checks; the preflight state is `prepared_not_executed`, with no native
parent/child execution or usage receipt established. The latest native CBM
index snapshot is a working-tree result of 11,023 nodes and 46,261 edges from
before the last repair; it includes unwired C7 work and does not qualify exact
commit `e035c0c3fe855490863be81902c5152b548c42cd`. Native process restart and
continuation remain unverified.

## Deferred scope

All local inference/model execution is deferred by owner, including WSL2/vLLM
and the PR24/Kilo local-model lane. It is excluded from the current delivery
path and completion qualification. OpenCode, Command, Codex and Antigravity
work continues; Claude-specific work is later.

## Critical path to a local reviewed delivery

```text
O1 manager ownership and admission + O2 durable intake and readback
    -> productive launch and owned workspace
    -> local applied submission -> assigned review -> return/correction
    -> fresh review -> exact acceptance (optional local publication)
```

GitHub, scripts, cron and external hooks are optional to the local audit path.
The first priority is a complete manager-owned local cycle; broader integrations
and qualification follow it.

## Remaining delivery blocks

1. **Peer autonomy beyond consultation — Partial.** C6 authors passive watches
   for `operation_terminal`, `contract_revision_changed`,
   `task_revision_changed`, `attempt_disposition_changed` and
   `exact_deadline_reached`, plus `coordination.sync_integration` and recomputed
   `swarm.overlap.check`. Broader integration-cell negotiation, assumptions,
   negotiated contracts, unsupported watch predicates and durable Concilium
   rounds remain.

2. **O1 manager-owned automation actions — Partial.** Owner-scoped
   configuration get/preview/apply/explain and durable `review_dispatch`
   admission exist. Work dispatch, disposition, repair, acceptance,
   publication and GitHub projection still need consumers that preserve current
   manager rights, semantic slots, on-behalf linkage and result readback.

3. **O2 durable intake and shared monitoring — Partial.** Commit
   `e035c0c3fe855490863be81902c5152b548c42cd` wires the dispatcher to consume
   bounded shared intake and journal readback for committed local
   `controller/task.submission` observations. Separate C7 credential issuance,
   broader readback, native capability proof, and disposition/lifecycle modules
   remain unwired; other source adapters are not admitted.

4. **Productive launcher, workspace ownership and complete local O7 cycle —
   Partial.** Queue/context/overlap projections, launch preview, and authored
   async lease / exact Task claim / `agent.open` slices exist. Credentials and
   native-MCP capability still gate productive launch, and dispatch is not
   implemented. The complete frozen-candidate, review, return, safe correction,
   fresh-review and acceptance path remains.

5. **O3 Rust adapters and provider lifecycle — Partial.** Rust OpenCode V2 and
   Zed paths exist alongside JavaScript/Python module bridges. Rust-owned
   adapter/control/stream coverage and observed provider capabilities remain
   incomplete.

6. **O4 Git/GitHub intake, work pools and distribution — Partial.** Local
   non-force Git ref publication exists as a first slice, with live Git/remote
   qualification pending. GitHub reconciliation, source-to-Task mapping, pool
   distribution, PR/check effects and shared bounded Git-scope inspection
   remain.

7. **O5 Rust hook observation — Unimplemented.** Authenticated bounded hook
   intake and safe install/readback need a specific runtime/plugin contract.

8. **O6 optional script bundles and runner — Unimplemented.** A scoped bundle
   registry, immutable environment capture, runner ownership, bounded output
   and durable result readback are not present.

9. **O8 cron/typed rules and O9 shared Goal progression — Partial.** A legacy
   interval scheduler exists, but the cron program remains separate. OpenCode
   has a controller-recorded Goal with one activation; native Goal APIs and
   shared manager-enabled progression remain incomplete.

10. **O10 cross-contract parity and O11 integrated qualification — Partial /
    qualification pending.** The 91-entry catalog and role profiles do not
    prove all handlers have consumers, native harnesses load schemas, or the
    integrated workflow survives recovery. C6 formatting, warnings-denied
    Clippy, and the bounded exact-commit privacy/actor/workspace/no-replay/
    overlap review passed for `e035c0c3fe855490863be81902c5152b548c42cd`.
    No Cargo tests or native/model execution ran in this increment; new CI,
    productive launch, full-cycle recovery and native-client evidence remain
    pending.

The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
