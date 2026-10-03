# Implementation status

## Current state

The PR22/PR23 program remains partial. **Ten grouped delivery blocks remain**;
they vary in size and are not ten equal tasks or a completion percentage. The
C6 committed catalog had 91 `ToolSpec` entries; catalog size does not establish
complete handlers or native-client loading.

C7 implementation is committed at
`2f00c2b3d7862788ca8a6bead4645d3ce64dc0ea`. Canonical formatting passed, and
warnings-denied Clippy passed (13.06 s;
`.local/qualification/r7-build-gate/clippy-c7-probe-repaired.log`). Full Rust
CI run `37153513585` passed all Ubuntu and Windows job steps for exact CI
commit `8570dae7f478b6dd2b604727b34c285a86ee9acc`, including Rust
application/protocol tests, native-bridge fixtures/offline smoke, and the
release build. The corrected targeted Windows `cargo test --test
check_probe` passed one test (1.04 s; build 43.34 s;
`.local/qualification/r7-build-gate/windows-check-probe-c7-repaired.log`).
The earlier failure was a fixture mismatch: it omitted the outer
`CREATE_NO_WINDOW` setting already used by production. A model-free `cmd-echo`
diagnostic passed with that setting, but no live PID was observed, so the exact
OS/root cause is unconfirmed. Review of unchanged C7 privacy, authority,
disposition and lifecycle paths at `588a5ca21fbcbd5434540c011846535af7647f35`
passed; `feedback_audit` also passed exact review of all three Windows-delta
files in the final commit (`.local/qualification/r7-build-gate/windows-probe-lifecycle-audit.md`).
The C7 slices wire
Participant credential issuance into launch admission with atomic private
assignment context and a held database workspace lease; partial authenticated
MCP readback for configured/connect state; and a durable bounded
review-disposition consumer with exact manager-on-behalf authority and semantic
duplicate/gap handling. The readback does not prove tool loading or
model-visible capability. Lease/release state is database-only, not an OS or
filesystem lock. The C7 build/CI gates are green. C8 source commit
`a1577aee63094e6fcb3feea6fc6079d1a8454850` wires WorkDispatch auto-admission
under typed manager authority, a manager-scoped immutable launch slot shared
with manual launches, and MCP install/proof handlers and readback. Owned-crate
formatting and warnings-denied Clippy passed (12.46 s;
`.local/qualification/r7-build-gate/clippy-c8-qualified.log`). The StoreAPI
WorkDispatch regression passed 1/1 (0.06 s; build 35.09 s;
`.local/qualification/r7-build-gate/work-dispatch-c8-qualified.log`); it covers
Task-fact admission, exact manual-slot reuse/conflict and idempotent receipt,
one Operation, and Manager/outsider `operation.get`/`operation.list` scope
filtering before pagination. C8 CI run `37157609062` failed in both OS jobs on
three projection fixtures because fixture setup installed `001core` but omitted
`002workspace`; this left `workspace_leases` absent in test databases. The
production path was unaffected. Repair commit
`7d518ef4edb84c5e8ce677fafa778de914abed30` installs `002workspace`; the
targeted projection filter passed 6/6 (0.05 s; build 37.36 s;
`.local/qualification/r7-build-gate/projection-c8-ci-repaired.log`). Full CI
run `37158328828` for the repair is in progress. C9 dirtyguard remains active,
unqualified WIP; its separate unused-warning is not the C8 CI failure. The
installed R6 is unchanged. Fresh-owned `mcp_plugin` startup remains
unwired and unqualified. Productive/native dispatch,
full manager-owned cycle, native-MCP harness loading and model execution remain
unqualified. C4's 221 Rust tests apply only to
`2607c8858e573ae40459c27d76d8ae9e1ca9f8fc`.

Credential/profile references are visible in Operation readback. The audit did
not establish token/path exposure or a public bearer-token resolve route; this
does not qualify those references as confidential.

C6 implemented five passive watch kinds, integration sync, recomputed overlap,
and manager-admitted asynchronous workspace lease / exact Task claim /
`agent.open`. C7 adds the launch-admission, authenticated readback and
review-disposition slices above; unsupported watch predicates, broader program
stages and end-to-end recovery remain open.

## Additional qualification evidence

Earlier `common.mjs`, `service.mjs` and `trial.mjs` syntax checks apply to a
prior composition only. The runtime `0.160.0` native-harness schema and its
three DTOs are repaired, and Node `--check` passed. No native-compose/trial or
model execution is qualified. No native parent/child execution
or usage receipt is established; process restart, continuation and native-MCP
harness loading remain unverified.

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
   admission exist. C7 adds a bounded review-disposition consumer that preserves
   exact manager-on-behalf authority and semantic duplicate/gap handling.
    C8 source commit `a1577aee63094e6fcb3feea6fc6079d1a8454850` wires
    manager-authorized WorkDispatch auto-admission, shared manual and automatic
    launch slots, and MCP install/proof handlers/readback. Formatting, Clippy
    and the bounded StoreAPI regression passed. CI run `37157609062` failed on
    the projection test fixture's omitted `002workspace`; fix commit
    `7d518ef4edb84c5e8ce677fafa778de914abed30` adds it, the focused projection
    filter passed 6/6, and CI `37158328828` is in progress.
    Productive/native dispatch, repair, acceptance, publication and GitHub
    projection still need qualified consumers preserving manager rights,
    semantic slots, linkage and result readback. Fresh-owned `mcp_plugin`
    startup remains unwired.

3. **O2 durable intake and shared monitoring — Partial.** Commit
   `e035c0c3fe855490863be81902c5152b548c42cd` wires the dispatcher to consume
   bounded shared intake and journal readback for committed local
   `controller/task.submission` observations. C7 adds launch-bound Participant
   credentials and partial authenticated configured/connect readback; it does
   not prove tool or model loading. Other source adapters are not admitted.

4. **Productive launcher, workspace ownership and complete local O7 cycle —
   Partial.** Queue/context/overlap projections, launch preview, async lease /
   exact Task claim / `agent.open`, and C7 atomic private context plus held
    database lease admission are wired in source. C8 adds manager-scoped
    WorkDispatch admission and the shared immutable manual/automatic launch
    slot, with targeted regression evidence recorded above. Lease enforcement is database-only;
   filesystem/OS ownership is not established, and stale-fence retention while
   old/unknown effects survive a revision is not yet gate-qualified. Productive dispatch
   and native tool/model capability proof remain absent. The complete
   frozen-candidate, review, return, safe correction, fresh-review and
   acceptance path remains.

5. **O3 Rust adapters and provider lifecycle — Partial.** Rust OpenCode V2 and
   Zed paths exist alongside JavaScript/Python module bridges. Rust-owned
   adapter/control/stream coverage and observed provider capabilities remain
   incomplete. C7's authenticated configured/connect readback does not prove
   native tool loading or model-visible capability.

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
    qualification pending.** The C6 91-entry catalog and role profiles do not
    prove all handlers have consumers, native harnesses load schemas, or the
    integrated workflow survives recovery. C6 formatting, warnings-denied
    Clippy, and the bounded exact-commit privacy/actor/workspace/no-replay/
    overlap review passed for `e035c0c3fe855490863be81902c5152b548c42cd`.
    C7 at `2f00c2b3d7862788ca8a6bead4645d3ce64dc0ea` passes formatting and
    warnings-denied Clippy. Full Rust CI run `37153513585` passed all Ubuntu
    and Windows job steps for exact commit
    `8570dae7f478b6dd2b604727b34c285a86ee9acc`, including Rust app/protocol
    tests, native-bridge fixtures/offline smoke and release build. The corrected
    Windows `check_probe` also passed one targeted test. Reviews of unchanged C7 privacy,
    authority, disposition and lifecycle paths at `588a5ca21fbcbd5434540c011846535af7647f35`
    passed; exact `feedback_audit` review of the final three-file Windows delta
    passed (`.local/qualification/r7-build-gate/windows-probe-lifecycle-audit.md`). The
    C8 WorkDispatch auto-admission, typed manager authority, the manual-shared
    immutable launch slot and MCP install/proof handlers/readback are wired in
    source at `a1577aee63094e6fcb3feea6fc6079d1a8454850`. Formatting, Clippy
    and the bounded StoreAPI regression passed; C8 CI run `37157609062` failed in both OS jobs on three projection fixtures: the fixture database installed schema `001core` but omitted `002workspace`, so `workspace_leases` was absent. Fix commit `7d518ef4edb84c5e8ce677fafa778de914abed30` installs `002workspace`; the targeted projection filter passed 6/6 (0.05 s; build 37.36 s; `.local/qualification/r7-build-gate/projection-c8-ci-repaired.log`). Full CI run `37158328828` for the repair is in progress.
    Fresh-owned `mcp_plugin` startup remains
    unwired. C7 build/CI gates are green; productive launch, full-cycle
    recovery, native-client or model evidence is still unqualified.

The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
