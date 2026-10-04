# Implementation status

## Current state

**C10 source status:** Published main commit `cea63dde1d923f821c436c61f2561bcfb6a4bb0d`. Owned-source formatting passed; warnings-denied production Clippy passed in 12.16 s; the debug build passed in 34.84 s (candidate SHA-256 `CBED6FD418BBE389E136E5716FB6E8394253481A81A3707E7B2F4730DC641ACE`). Four bounded independent source audits for the provider path, typed actor, getter, and consumer passed against their exact current pins. Full CI run [37170376636](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37170376636) completed successfully on Windows and Ubuntu. C9 run 37168223030 remains green only for its exact source `2e609ecf7d826da7019fe5e5f2ed397a992bc45a`.

C10 adds the optional exact `launch_operation_id` to the actual `task.dispatch` schema and Store contract for launch-owned Attempts. The Store checks the retained parent, exact Task/Attempt/binding/lease lineage, immutable prompt packet, and current C8 MCP capability proof before native input. The owned-provider path accepts one explicitly configured provider credential source and reports `stored_unverified` only for credential metadata; that does not prove key validity or provider/model consumption.

The prior native run `bb070791-ba7c-4c71-9cc7-660fbb531418` ended `OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT` at `service_start`, after repeated `OWNED_SERVICE_SCOPE_STALE` and before an owned-service row, MCP proof, Bun call, or model call. Its failure was traced to comparing manifest `runtime.route` metadata as an object against an alias string before start reservation. A three-file follow-up source repair has since passed independent exact-source audit (`.local/pr-implementation/c10-route-shape-audit.md`, report SHA-256 `A4089D435AC3DE11D1DFE1A12DED3FEC147FAE21520068697B02ABE98E734BB1`), package formatting, production Clippy (14.67 s), and debug build (35.37 s; candidate SHA-256 `DEDBF403020780A35ED0141A31EF8A43654073B9B2A96CC4BF88AC77B462B528`). The repair does not yet have a final commit SHA or CI run.

New native run `46cef212-aa65-418a-a911-b54502fe9fd7` is active. Its current observation is `awaiting_binding` with a held lease and no owned-service row or MCP proof yet; diagnosis of the remaining opening fences is active. Do not classify this run as a final failure or success. The prior `bb070...` run exited by normal EOF; no replay occurred. Earlier runs `f368339d-1247-4118-bdac-a5441d29b8be` and `114d4ca7` remain retained as unknown. No listed run produced a new provider credential observation or model call. Installed R6 is unchanged. C11 repair/acceptance work is being authored and wired across shared tracked files plus four new modules; it remains uncompiled and unpublished. All local model/inference work, including PR24/Kilo, remains deferred.

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

2. **O1 manager-owned automation actions — Partial.** Owner-scoped configuration
   get/preview/apply/explain, WorkDispatch, ReviewDispatch, bounded
   ReviewDisposition, typed manager authority, and shared manual/automatic
   semantic slots are wired. C10 publishes the launch-parent dispatch schema
   and Store gate. The prior native failure led to the audited three-file route
   repair; its new native run is active before owned-service startup. In the
   published C10 source, RepairDispatch, automated acceptance/publication, and
   GitHub projection still have no registered consumers. C11 work to add/wire
   repair and acceptance is in progress but uncompiled and unpublished.
3. **O2 durable intake and shared monitoring — Partial.** The dispatcher
   consumes bounded shared intake and journal readback for committed local
   controller/task.submission observations. Participant credential issuance,
   authenticated configured/connect readback, and C9 fresh-owned service
   lifecycle/readback are in the source path. Other source adapters are not
   admitted. C10's actual MCP schema and provider-auth gate are source-verified.
   A route-shape follow-up repair passed source audit, formatting, Clippy and
   build, but has no final commit SHA or CI yet. The new native run is active at
   `awaiting_binding`, before any owned-service row or MCP proof; native
   capability and provider/model use remain unqualified.
4. **Productive launcher, workspace ownership and complete local O7 cycle —
   Partial.** Queue/context/overlap projections, launch preview, async lease,
   exact Task claim, `agent.open`, Participant context and WorkDispatch
   admission exist. C10 adds a launch-linked `task.dispatch` parent/packet and
   current MCP-proof gate; the prior native attempt stopped before an
   owned-service row, and the route-shape repair is awaiting final commit/CI.
   The new native attempt is active at `awaiting_binding` with held lease and no
   owned-service row or MCP proof yet. Workspace enforcement remains
   database-backed, with a separate retained-proof service-departure fence.
   Productive dispatch and the full candidate/review/return/correction/fresh-
   review/acceptance path remain unqualified.
5. **O3 Rust adapters and provider lifecycle — Partial.** Rust OpenCode V2 and
   Zed paths exist alongside JavaScript/Python module bridges. C10 publishes the
   exact provider credential gate; `stored_unverified` denotes credential
   metadata only, not key validity or model consumption. Neither the prior
   failed run nor the active run has produced a new credential observation.
   Native plugin loading, callable tools and provider/model capability remain
   unqualified.
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
    qualification pending.** C10 formatting, warnings-denied Clippy, debug
    build and four bounded source audits passed for the published source
    `cea63dde1d923f821c436c61f2561bcfb6a4bb0d`; CI 37170376636 passed Windows
    and Ubuntu. The prior native failure was traced to a manifest-route
    object/alias-string comparison. Its three-file follow-up repair passed
    source audit, formatter, Clippy and debug build but lacks a final commit SHA
    and CI. A new native run is active at `awaiting_binding`, with held lease
    and no owned-service row/MCP proof yet. C9 CI 37168223030 is green for exact
    source 2e609ec only. Neither CI run proves live MCP/model capability or the
    complete manager-owned workflow.
The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
