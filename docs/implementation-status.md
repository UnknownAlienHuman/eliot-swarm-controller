# Implementation status

## Current state

**Current source status:** C12 source fa38ea3287dfba0bf9c7e3c7658c4f3aad9580d8 is published. Formatting, JavaScript syntax, production Clippy (14.39 s), debug build (39.04 s), the corrected real-Store regression (1/1, 0.10 s; owner-positive, GM-self and Operator-negative cases), and exact-source audit SHA-256 5A28B4C381492C70D86F7ACE68DEF82EA9DA2202261B26F03FA7C6C4B5CA389D passed. CI run [37176386371](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37176386371) completed successfully on Windows and Ubuntu, verified on 2026-10-04 at 04:28 UTC. The earlier C11 run [37174102156](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37174102156) remains green for exact source 4ecc030e072be1b3fdf39e2b3ead122953e4de82.

C10 adds the optional exact `launch_operation_id` to the actual `task.dispatch` schema and Store contract for launch-owned Attempts. The Store checks the retained parent, exact Task/Attempt/binding/lease lineage, immutable prompt packet, and current C8 MCP capability proof before native input. The owned-provider path accepts one explicitly configured provider credential source and reports `stored_unverified` only for credential metadata; that does not prove key validity or provider/model consumption.

The prior native run `bb070791-ba7c-4c71-9cc7-660fbb531418` ended `OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`, after repeated `OWNED_SERVICE_SCOPE_STALE` and before an owned-service row, MCP proof, Bun call, or model call. Its failure was traced to comparing manifest `runtime.route` metadata as an object against an alias string before start reservation; the route repair is published in `e637d45`. Historical CI 37172541315 failed the Windows `check_probe` lifetime step; the focused regression now passes with `/D` disabling ambient CMD AutoRun startup hooks in the fixture, but the precise historical cause is unproven.

Native run 46cef212-aa65-418a-a911-b54502fe9fd7 timed out at service_start after 240 BINDING_NOT_READY observations; normal stdin EOF closed the host, and the run remains retained without replay. Its opening-fence audit found that database module IDs matched the child while get_binding omitted instance and artifact IDs.

Earlier native run e16291b7-0913-43b0-bf72-35fa409ab4da ended with OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT at service_start. The start outcome remains unknown; final private-audit SHA-256 is 1CA31BA6A1BD6A093BC32FB5CCA5A06C9D1AC4647A4C4717FB35EC45274ADD42. No MCP proof, owner-ready receipt, connection receipt, or family-stop receipt exists. It remains retained without replay, and process=NULL does not establish whether Bun exists.

C11 wires RepairDispatch and acceptance consumers to typed ledgers/cursors, exact same-slot reuse, GM epoch and byte-verification checks, structured requirement reviews, and manager history/visibility. Reviewer independence, GM-ownership precondition, and reviewer-equals-writer checks are fixed; C11 full CI is recorded above.

C12 publishes the owner-sponsored acceptance route to an independent GM; the real-Store regression covered owner-positive, GM-self, and Operator-negative cases. Source and CI gates are listed above.

OpenCode owned-startup diagnostic e2814fe9-ac32-42be-90b5-7ea4173ee2c2 admitted one launch and reached the helper. Its journal recorded process_identity_failed / OWNED_SERVICE_PROCESS_IDENTITY_UNAVAILABLE with a spawnedPID field. The outcome remains unknown, with no ready or native-proof receipt, and the launch is retained without replay. Host EOF completed normally; departure and helper-family stop proof are not yet known. Do not infer a Bun-exit cause.

An earlier Command preflight stopped before host or model startup with RUN_ROOT_ACL_FAILED while autoloading Get-Acl; an ACL-only private probe passed. Later Command run f413bd5d-149d-4344-972b-125b6b3bf23a passed one bounded Bunny task.dispatch: marker 50 bytes, exit 0, no timeout. Requested route was stealth/space-bunny-alpha; effective model was null/unknown, and the native result exposed no upstream provider/model identity. Proof summary SHA-256 is 1C71DAB9592AE72752032556251A602BD5D268F9BEE42ED06BFCB944196453D2. Module owner family was empty and owner exit was 0; host 50688 exited on EOF with code 0, current owned PIDs were absent, and four Codex processes retained their same birth identities.

This single Command result does not establish Task completion, acceptance, OpenCode service/MCP readiness, model identity, or the full O7 cycle. C13 publication work is active across four writers and remains unbuilt. Its integration exposed a mismatch between the acceptance producer envelope and the historical linkage reader; a producer correction is prepared and unbuilt. Installed R6 is unchanged. All local model/inference work, including PR24/Kilo, remains deferred.

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

2. **O1 manager-owned automation actions — Partial.** Owner-scoped configuration get/preview/apply/explain, WorkDispatch, ReviewDispatch, bounded ReviewDisposition, typed manager authority, and shared manual/automatic semantic slots are wired. C10 publishes the launch-parent dispatch schema and Store gate. C11 adds RepairDispatch and acceptance consumers on typed ledgers/cursors, with same-slot reuse, GM epoch and byte-verification checks, structured reviews, and manager history/visibility. C12 publishes the owner-sponsored acceptance route to an independent GM. Automated publication and GitHub projection remain gaps.

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
