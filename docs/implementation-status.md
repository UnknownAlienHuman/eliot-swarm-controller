# Implementation status

## Current state

The previous delivered and installed source is
`7061e455f04a76bfaa19699edba7f58c14a27748`.
It implements successor-GM continuation of the retained work, including exact
repair dispatch across explicit automation transfers. Full Windows/Ubuntu CI
[37195035427](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37195035427)
passed. The matching controller was installed on 2026-10-04 at 10:25 UTC;
its SHA-256 is `C4A28DA65495FAC229EE1202DA7F4E75EA642F4B909F6F375DE0CED07102EF9E`.
The prior installed controller is preserved in the private installation receipt.

The current source increment `8ae72e3c1330a603d413b345336dbeddee979c26`
adds manager-owned calendar CheckRun automation,
Command native event envelopes (`command-mod-0.1.0-glue.4`), and bounded parent-side
OpenCode startup failure observations. The Command fixtures passed 17 glue checks
and the bridge checks; that artifact has no new native qualification yet. The
Rust increment passed warnings-denied production Clippy, the real Store
admission/coalescing/foreign-owner/transfer/restart regression, three calendar
boundary/DST checks, exact Command artifact admission, and debug build on
2026-10-04 at 11:37 UTC. The successful Clippy was retained after verifying
that the subsequent Windows fixture path correction changed test code only.
All source stayed unchanged during the final gate. Candidate SHA-256 is
`BC74D592B6C422E82B0E1E9277C0D1C33DA59E037D1A92A88C95F0E6F5C55910`.
Luna's bounded authority/consumer and native-diagnostic reviews passed after
the transferred-CheckRun allowlist and coalesced-receipt corrections. The
current source passed its local gates. Full CI
[37199464664](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37199464664)
completed on 2026-10-04: Windows passed; Ubuntu had 247 passing Rust tests and
one failure in `current_gm_transfer_preserves_all_entry_journals_and_retires_old_owner`.
The assertion expected four relocated ledgers and observed five after the new
calendar ledger; its source contract and fixture correction are being reviewed.
The fresh one-attempt C16 run `b93b7821-e6ce-409d-88f4-d1f6f51a3ab9`
ended at `owned_service_start_bootstrap` with `NATIVE_REJECTED` before a model
call. The retained parent diagnostic identifies the bootstrap stage; it does
not establish credential validity or the exact rejected API contract. The
OpenCode bootstrap implementation is being investigated against the pinned
native source. Preserve the consumed run claim and unknown operation without
replay. The installed controller is still the prior qualified `7061e45` candidate.
The project remains **PARTIAL_PROGRESS**, with the full O7 workflow, the remaining
automation programs and cross-environment qualification outstanding. All local
model and inference work remains deferred.

**Source checkpoint:** GM continuity and host-crash submission readback are saved in main. Their production Clippy, debug build and 13 focused GM/Store checks passed. The subsequent MCP table corrections and OpenCode Windows path fix passed full Windows and Ubuntu CI [37186030488](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37186030488) for source 8f3998ffb5da24deaac8445f883fe19780b02f3b, verified on 2026-10-04. Explicit automation transfer now passes production Clippy, its real-Store preservation regression, all 29 MCP checks, the Windows input-probe integration check and debug build. Its qualification boundary and candidate are recorded below. Successful live OpenCode startup, effective hosted-model identity and the full O7 cycle remain unqualified.

C10 adds the optional exact `launch_operation_id` to the actual `task.dispatch` schema and Store contract for launch-owned Attempts. The Store checks the retained parent, exact Task/Attempt/binding/lease lineage, immutable prompt packet, and current C8 MCP capability proof before native input. The owned-provider path accepts one explicitly configured provider credential source and reports `stored_unverified` only for credential metadata; that does not prove key validity or provider/model consumption.

The prior native run `bb070791-ba7c-4c71-9cc7-660fbb531418` ended `OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`, after repeated `OWNED_SERVICE_SCOPE_STALE` and before an owned-service row, MCP proof, Bun call, or model call. Its failure was traced to comparing manifest `runtime.route` metadata as an object against an alias string before start reservation; the route repair is published in `e637d45`. Historical CI 37172541315 failed the Windows `check_probe` lifetime step; the focused regression now passes with `/D` disabling ambient CMD AutoRun startup hooks in the fixture, but the precise historical cause is unproven.

Native run 46cef212-aa65-418a-a911-b54502fe9fd7 timed out at service_start after 240 BINDING_NOT_READY observations; normal stdin EOF closed the host, and the run remains retained without replay. Its opening-fence audit found that database module IDs matched the child while get_binding omitted instance and artifact IDs.

Earlier native run e16291b7-0913-43b0-bf72-35fa409ab4da ended with OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT at service_start. The start outcome remains unknown; final private-audit SHA-256 is 1CA31BA6A1BD6A093BC32FB5CCA5A06C9D1AC4647A4C4717FB35EC45274ADD42. No MCP proof, owner-ready receipt, connection receipt, or family-stop receipt exists. It remains retained without replay, and process=NULL does not establish whether Bun exists.

C11 wires RepairDispatch and acceptance consumers to typed ledgers/cursors, exact same-slot reuse, GM epoch and byte-verification checks, structured requirement reviews, and manager history/visibility. Reviewer independence, GM-ownership precondition, and reviewer-equals-writer checks are fixed; C11 full CI is recorded above.

C12 publishes the owner-sponsored acceptance route to an independent GM; the real-Store regression covered owner-positive, GM-self, and Operator-negative cases. Source and CI gates are listed above.

OpenCode owned-startup diagnostic e2814fe9-ac32-42be-90b5-7ea4173ee2c2 admitted one launch and reached the helper. Its journal recorded process_identity_failed / OWNED_SERVICE_PROCESS_IDENTITY_UNAVAILABLE with a spawnedPID field. The outcome remains unknown, with no ready or native-proof receipt, and the launch is retained without replay. Host EOF completed normally; departure and helper-family stop proof are not yet known. Do not infer a Bun-exit cause.

An earlier Command preflight stopped before host or model startup with RUN_ROOT_ACL_FAILED while autoloading Get-Acl; an ACL-only private probe passed. Later Command run f413bd5d-149d-4344-972b-125b6b3bf23a passed one bounded Bunny task.dispatch: marker 50 bytes, exit 0, no timeout. Requested route was stealth/space-bunny-alpha; effective model was null/unknown, and the native result exposed no upstream provider/model identity. Proof summary SHA-256 is 1C71DAB9592AE72752032556251A602BD5D268F9BEE42ED06BFCB944196453D2. Module owner family was empty and owner exit was 0; host 50688 exited on EOF with code 0, current owned PIDs were absent, and four Codex processes retained their same birth identities.

This single Command result does not establish the full O7 cycle, OpenCode service/MCP readiness or served-model identity. R6 was unchanged at that run's historical checkpoint; the current installation is recorded above. All local model/inference work, including PR24/Kilo, remains deferred.

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

**Resume here:** qualify owned native startup and the complete local O7 cycle.
Successor RepairDispatch on the retained Attempt's binding now passes its Store gate.
Use the latest built candidate recorded
below when a fresh harness requires this code. Preserve earlier unknown
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

Full Windows and Ubuntu CI for source `21343e7afca61d66b0330e4deb7e6a0c3548d4f7`
passed in [37189940977](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37189940977),
verified on 2026-10-04 at 09:16 UTC.

### Successor review and correction — source increment, 2026-10-04

After explicit entry transfer, the current GM can assign a new review of the
former owner's exact current Attempt. A later successor can consume that review
through the sealed transfer lineage. The actual review sponsor, original Attempt
owner and submission author remain recorded; the current GM is the acceptance
or correction decision actor, and feedback still addresses the Attempt owner.
Historical direct and automatic assignments use their retained assigning
Operation and actual sponsorship, including after a second GM handover.

Warnings-denied production Clippy, the A→B→C Store regression, the successor-GM
continuation regression, legacy owner-sponsored acceptance compatibility, and
debug build passed on 2026-10-04. Production stayed unchanged after Clippy; only
the new fixture's assertions were aligned with the retained queued receipt.
The final gate source stayed unchanged. Luna ratified the retained sponsorship
and authority predicates. Built candidate SHA-256 is
`6B5978C85B5715E06B8A9C88FB97409D2763A0534CFEB1DDADFF9DED08623764`.
The gate covers A-owned Attempts, B-sponsored independent reviews, then C's exact
correction and queued acceptance after a second transfer. Acceptance reservation
does not establish the final artifact check or accepted Task transition. Fresh
RepairDispatch on the original owner's binding is qualified in the next source
increment below; the complete native O7 cycle remains unqualified.

Full Windows and Ubuntu CI for source
`4ce438b10d5facb82188feea34daeba36bd8216f` passed in
[37192501762](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37192501762),
verified on 2026-10-04 at 09:49 UTC.

### Successor repair continuation — qualified source increment, 2026-10-04

After explicit A→B→C entry transfer, C can queue a fresh correction on A's
retained ready binding. Attempt owner and feedback recipient A, retained review
sponsor, disposition decision manager B, and current automation manager C stay
distinct. The validator preserves the decision manager's committed correction
reason while checking the original reviewer provenance separately. Historical
manager slots are checked across the complete transfer lineage before a new
slot is admitted. Existing direct slots are retained for readback; an exact
queued unsent automation slot uses its explicit transfer continuation.

The queued effect retains its captured GM epoch. A designation change away from
C and back to C invalidates that old effect without overwriting its Operation or
duplicating its slot. Canonical current Attempts are derived from unreleased
Attempt records rather than an absent Task column.

Warnings-denied production Clippy passed (18.96 s). The real-Store A→B→C repair,
duplicate-consumption and C→B→C epoch regression passed (0.11 s execution;
44.10 s including compilation), followed by debug build (39.77 s), verified on
2026-10-04 at 10:19 UTC. Production source stayed unchanged after Clippy; only
the fixture's canonical request read was corrected. All source stayed unchanged
during the final gate. Luna's bounded authority audit found no remaining blocker.
Built candidate SHA-256 is
`C4A28DA65495FAC229EE1202DA7F4E75EA642F4B909F6F375DE0CED07102EF9E`.
The gate qualifies retained Store admission and pre-effect checks. It does not
establish native delivery or the full O7 cycle. The previous GM chat is
unnecessary for handover and continuation.

Full Windows and Ubuntu CI for source
`7061e455f04a76bfaa19699edba7f58c14a27748` passed in
[37195035427](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37195035427),
verified on 2026-10-04 at 10:40 UTC.

### Installed controller and fresh OpenCode startup — 2026-10-04

The qualified `7061e45` candidate above was installed at
`C:\Users\kleym\.cargo\bin\swarm.exe` at 10:25 UTC. Installed byte hash and
`swarm --version` passed readback. The prior R6 binary was preserved for rollback;
no active controller process existed, and no service, PATH, hook, or provider
credential was changed by this update.

Fresh native run `f5ad880d-c691-4134-82e3-4784c9384158`, launch
`8f988bea-bec1-4882-aa67-60dcd37914e2`, progressed beyond the previous Windows
path failure. Retained native receipts report Bun PID 55076 with the pinned
executable identity, owner `ready`, then a completed `stdin-eof` stop. The
controller never retained a ready/native MCP proof and the harness timed out at
`service_start`. The Operation remains unknown and is retained without replay.
The source review found that parent-side startup errors lost their stage before
normal helper EOF cleanup. The next increment retains a bounded stage/code
observation before that cleanup; C16 will check its actual native result. The
historical C15 cause remains unknown. No model request was made. All six protected
Codex processes retained their birth identities.

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

9. **O8 cron/typed rules and O9 shared Goal progression — Partial.**
   Manager-owned calendar CheckRuns now share the legacy scheduler, entry
   enablement, durable occurrence identities, normal CheckRunner and explicit
   transfer/restart paths. The current source gates are recorded above.
   Typed event rules, the manual run-now editor and shared Goal progression
   remain. OpenCode has a controller-recorded Goal with one activation;
   native Goal APIs and shared manager-enabled progression remain incomplete.

10. **O10 cross-contract parity and O11 integrated qualification — Partial /
    qualification pending.** Current source gates and retained native evidence are
    recorded in Current State above. No listed CI run proves live MCP/model
    capability or the complete manager-owned workflow.
The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
