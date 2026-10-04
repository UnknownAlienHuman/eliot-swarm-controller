# Implementation status

## Current state

**Source checkpoint:** GM continuity and host-crash submission readback are saved in main. Their production Clippy, debug build and 13 focused GM/Store checks passed. The subsequent MCP table corrections and OpenCode Windows path fix passed full Windows and Ubuntu CI [37186030488](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37186030488) for source 8f3998ffb5da24deaac8445f883fe19780b02f3b, verified on 2026-10-04. Explicit automation transfer now passes production Clippy, its real-Store preservation regression, all 29 MCP checks, the Windows input-probe integration check and debug build. Its qualification boundary and candidate are recorded below. Successful live OpenCode startup, effective hosted-model identity and the full O7 cycle remain unqualified.

C10 adds the optional exact `launch_operation_id` to the actual `task.dispatch` schema and Store contract for launch-owned Attempts. The Store checks the retained parent, exact Task/Attempt/binding/lease lineage, immutable prompt packet, and current C8 MCP capability proof before native input. The owned-provider path accepts one explicitly configured provider credential source and reports `stored_unverified` only for credential metadata; that does not prove key validity or provider/model consumption.

The prior native run `bb070791-ba7c-4c71-9cc7-660fbb531418` ended `OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`, after repeated `OWNED_SERVICE_SCOPE_STALE` and before an owned-service row, MCP proof, Bun call, or model call. Its failure was traced to comparing manifest `runtime.route` metadata as an object against an alias string before start reservation; the route repair is published in `e637d45`. Historical CI 37172541315 failed the Windows `check_probe` lifetime step; the focused regression now passes with `/D` disabling ambient CMD AutoRun startup hooks in the fixture, but the precise historical cause is unproven.

Native run 46cef212-aa65-418a-a911-b54502fe9fd7 timed out at service_start after 240 BINDING_NOT_READY observations; normal stdin EOF closed the host, and the run remains retained without replay. Its opening-fence audit found that database module IDs matched the child while get_binding omitted instance and artifact IDs.

Earlier native run e16291b7-0913-43b0-bf72-35fa409ab4da ended with OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT at service_start. The start outcome remains unknown; final private-audit SHA-256 is 1CA31BA6A1BD6A093BC32FB5CCA5A06C9D1AC4647A4C4717FB35EC45274ADD42. No MCP proof, owner-ready receipt, connection receipt, or family-stop receipt exists. It remains retained without replay, and process=NULL does not establish whether Bun exists.

C11 wires RepairDispatch and acceptance consumers to typed ledgers/cursors, exact same-slot reuse, GM epoch and byte-verification checks, structured requirement reviews, and manager history/visibility. Reviewer independence, GM-ownership precondition, and reviewer-equals-writer checks are fixed; C11 full CI is recorded above.

C12 publishes the owner-sponsored acceptance route to an independent GM; the real-Store regression covered owner-positive, GM-self, and Operator-negative cases. Source and CI gates are listed above.

OpenCode owned-startup diagnostic e2814fe9-ac32-42be-90b5-7ea4173ee2c2 admitted one launch and reached the helper. Its journal recorded process_identity_failed / OWNED_SERVICE_PROCESS_IDENTITY_UNAVAILABLE with a spawnedPID field. The outcome remains unknown, with no ready or native-proof receipt, and the launch is retained without replay. Host EOF completed normally; departure and helper-family stop proof are not yet known. Do not infer a Bun-exit cause.

An earlier Command preflight stopped before host or model startup with RUN_ROOT_ACL_FAILED while autoloading Get-Acl; an ACL-only private probe passed. Later Command run f413bd5d-149d-4344-972b-125b6b3bf23a passed one bounded Bunny task.dispatch: marker 50 bytes, exit 0, no timeout. Requested route was stealth/space-bunny-alpha; effective model was null/unknown, and the native result exposed no upstream provider/model identity. Proof summary SHA-256 is 1C71DAB9592AE72752032556251A602BD5D268F9BEE42ED06BFCB944196453D2. Module owner family was empty and owner exit was 0; host 50688 exited on EOF with code 0, current owned PIDs were absent, and four Codex processes retained their same birth identities.

This single Command result does not establish Task completion, acceptance, OpenCode service/MCP readiness, model identity, or the full O7 cycle. Installed R6 is unchanged. All local model/inference work, including PR24/Kilo, remains deferred.

### Fresh OpenCode diagnostic — 2026-10-04 07:25 UTC

Run aa62853f-8722-48e8-b6ef-eec20c42cf6b admitted exactly one fresh launch after
the private ACL readback was corrected. The actual child exited with code 1;
the retained receipt reports process_gone/process_exit_race. Its complete,
untruncated 62-byte diagnostic says `Refusing a redirected directory path`.
The retained workspace uses a Windows extended-length DOS path; OpenCode's
owner comparison treated that spelling as different from the ordinary realpath.
The module source correction is saved in main 8f3998ffb5da24deaac8445f883fe19780b02f3b; syntax and four extracted path comparisons passed. A successful native restart is not yet qualified. This run has no ready
or native MCP proof and no model turn. Its Operation remains unknown and is
retained without replay. This new cause does not establish the cause of older
unknown runs.

### C13/C14 saved implementation checkpoint — 2026-10-04 05:46 UTC

C13 implements an explicit accepted-candidate publication consumer, typed current-GM authority, shared manual/automatic effect slots, no-effect slot release after handover, and scoped operation/history/explanation readback. C14 adds bounded private startup diagnostics: a closed process-identity failure class, the actual immediate child exit observation, and redacted stderr metadata. These are implemented source paths; live automatic publication and successful owned OpenCode startup are not qualified.

Production formatting, warnings-denied Clippy (15.19 s) and debug build (37.78 s) passed. The candidate SHA-256 is 984B38A3567E572D14EE1D01BADBD343C1FE1575BB6ECEB75AD2A55598B6371B. The existing owner-sponsored acceptance/history check passed. The dedicated publication regression exposed an incomplete synthetic submission result, an outdated explain method name, and an incorrect successor-manager history-read expectation. All three are corrected; the final corrected regression and pending Forge endpoint check have not yet run. No production permission check was weakened.

The later full CI for 509715b passed both publication and Forge endpoint regressions. Later GM continuity source replaces the historical successor-manager denial with the explicit continuity requirement below. The C14 fresh-start diagnostic has now run once; its failure and source correction are recorded above. Preserve all unknown runs without replay. The full native O7 cycle remains incomplete.

### GM continuity correction — 2026-10-04

The accepted requirement is that losing a GM chat must not strand project work.
Same-credential reconnect already preserves the durable client identity. The
source now implements different-successor control of current Attempts,
native admission and operational history, former-owner automation readback, and
same-client binding changes that previously rotated the GM epoch. Original owner,
caller, workspace and producer history remain retained. It also preserves verified
submission artifacts when GM authority changes during local publication. The
source increment is saved in f3eda2883474b28b8d3e8c6e587e696fad5477f6.
Production Clippy (16.10 s), debug build (35.66 s), and 11 focused GM/Store
regressions passed on 2026-10-04 at 06:23 UTC. The source remained unchanged during
these gates; candidate SHA-256 is 6AC53A11511AF1C3A34A7571D5B3A505E66CA50AC471E70E7A3EC9D748FECDBA.
Cross-platform CI [37182521189](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37182521189)
completed successfully on Windows and Ubuntu, verified at 06:37 UTC. The requirement is saved in ac6691c and
described in [GM session continuity](gm-session-continuity.md).

The saved source increment implements `task.submit.recover` and
`swarm task recover-submission <operation_id>` for an exact prior submission
left unknown by a host crash. It verifies the existing deterministic artifact
without publishing or native replay, keeps original caller/submitted-by, checks
the actual current GM again at finalization, and retains stale submission history
without changing the Attempt. A missing file keeps the original target unknown.
An already-settled target returns its stored result without duplicate artifacts.
Source ab8f7aed314b59bd64a05266c1124e3a9412b32c passed all 13 focused GM/Store
regressions (0.17 s execution, 40.33 s including compilation) and the debug build
(39.08 s), verified on 2026-10-04 at 07:04 UTC. Production Clippy passed in 15.14 s
on d6af378; source hashes prove that only the recovery test changed afterward.
The new candidate SHA-256 is
128DC8791F31E29ED18FB2FFC4DF411F896658C1591461116F9C3FC8FEBC022F.
Cross-platform CI [37184545179](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37184545179)
failed solely on the outdated MCP table contract, repaired in 2f3e22a. The focused table guard passes; subsequent full Windows and Ubuntu CI37186030488 passed for source 8f3998f. The installed R6 binary and running native environments have
not been replaced or restarted by these source gates.

**Resume here:** continue successor automatic review/correction from the saved
transfer increment, then owned native startup qualification. Use the transfer
candidate below when a fresh harness requires this code. Preserve earlier unknown
runs without replay. Transferring every former manager's entry automatically
remains separate from explicit per-entry transfer.

### Automation transfer — qualified source increment, 2026-10-04

The source adds explicit `automation.config.transfer` to the current GM,
with revision checks, retired source identity, preserved typed journals,
historical operation links and exact queued-operation continuation. The GM MCP
profile exposes former-owner configuration reads and transfer. The real-Store
regression passed, including all four cursors and nonempty pending journals,
unchanged historical configuration Operation, denied unrelated/stale transfers,
and blocked former-owner reenablement. All 29 MCP checks, the Windows input-probe
integration check, warnings-denied production Clippy and debug build passed.
The production source was unchanged after Clippy; only the test's protected-record
reads were corrected before the successful test. The final source stayed unchanged
during the remaining gates. Luna's bounded authority/effect audit found no further
blocker in this slice. Candidate SHA-256 is
`000B92BE2CDF4A0FE789A25CB021603607138F0CD35B9845D14462D1A3FD77C0`.
This is a built candidate, not a newly installed or live-native-qualified binary.

Transferred WorkDispatch workspace reconciliation has a distinct readback-only
path for `workspace_effect_unknown`. It records an observed held lease and keeps
the parent unknown; it cannot claim, open, start or replay. Other uncertain native
start phases retain their existing recovery contracts.

New autonomous ReviewDispatch and non-pass correction of an old-owner Attempt
are still owner-scoped after entry transfer. Existing old-owner-sponsored passes
can use the independent-GM acceptance path, and manual successor control remains
implemented. A new automatic successor review/correction route is the next
implementation gap; transfer does not establish full autonomous O7.

### Prior C7/C8 evidence

C7 full Rust CI run 37153513585 passed all Ubuntu and Windows steps for exact CI commit 8570dae7f478b6dd2b604727b34c285a86ee9acc. C8 run 37157609062 exposed projection fixtures missing 002workspace; repair 7d518ef4edb84c5e8ce677fafa778de914abed30 passed the focused projection filter 6/6 and full Ubuntu/Windows CI run 37158328828. C4's 221 Rust tests apply only to 2607c8858e573ae40459c27d76d8ae9e1ca9f8fc.
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

2. **O1 manager-owned automation actions — Partial.** Owner-scoped configuration get/preview/apply/explain, WorkDispatch, ReviewDispatch, bounded ReviewDisposition, typed manager authority, and shared manual/automatic semantic slots are wired. C10 publishes the launch-parent dispatch schema and Store gate. C11 adds RepairDispatch and acceptance consumers on typed ledgers/cursors, with same-slot reuse, GM epoch and byte-verification checks, structured reviews, and manager history/visibility. C12 publishes owner-sponsored acceptance to an independent GM. C13 implements automated accepted-candidate publication; live publication qualification and GitHub projection remain gaps.

3. **O2 durable intake and shared monitoring — Partial.** The dispatcher consumes bounded shared intake and journal readback for committed local controller/task.submission observations. Participant credential issuance, authenticated configured/connect readback, and C9 fresh-owned service lifecycle/readback are in the source path. Other source adapters are not admitted. C10 actual MCP schema and provider-auth gate are source-verified. Native OpenCode and end-to-end qualification remain partial; see Current State above for the latest evidence.

4. **Productive launcher, workspace ownership and complete local O7 cycle — Partial.** Queue/context/overlap projections, launch preview, async lease, exact Task claim, agent.open, Participant context, WorkDispatch admission and C10 launch-linked task.dispatch gate are implemented. Native service capability remains unqualified. Workspace enforcement remains database-backed, with a separate retained-proof service-departure fence. Productive dispatch and the full candidate/review/return/correction/fresh-review/acceptance path remain unqualified.
5. **O3 Rust adapters and provider lifecycle — Partial.** Rust OpenCode V2 and
   Zed paths exist alongside JavaScript/Python module bridges. C10 publishes the
   exact provider credential gate; `stored_unverified` denotes credential
   metadata only, not key validity or model consumption. Earlier failed runs produced no new credential observation. Native plugin loading, callable tools and provider/model capability remain unqualified; current native evidence is in Current State above.
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
    qualification pending.** Current source gates and retained native evidence are
    recorded in Current State above. No listed CI run proves live MCP/model
    capability or the complete manager-owned workflow.
The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
