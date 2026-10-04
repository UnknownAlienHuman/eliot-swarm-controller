# Implementation status

## Current state

**C9 source status:** Main/remote commit 2e609ecf7d826da7019fe5e5f2ed397a992bc45a contains the held-workspace overlay fix reviewed against source SHA prefix 0A905. Full Windows/Linux CI passed all steps on this exact source: formatting, Clippy, tests, build and native offline fixtures ([run 37168223030](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37168223030)). The native-derived path has not been locally rebuilt or retested. Prior Git-argv fix dd4a965571c8f846be8465309acebcb97bfb3f0c also passed full Windows/Linux CI run 37166867596; its local formatting/Clippy passed in 15.42 s and candidate build in 37.37 s. Staged run f368339d-1247-4118-bdac-a5441d29b8be ended OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT/service_start: workspace hold and binding link succeeded, but the host repeatedly reported OWNED_SERVICE_SCOPE_STALE before an owned_service row, native MCP or model. The issue was comparing the whole route against an intentional native_options.directory lease overlay. Commit 2e609ec permits only that exact held directory and requires equality for every other route field; native retest remains pending. Runs f368339d and 114d4ca7 remain retained as unknown; no replay occurred and the host exited through normal EOF. Installed R6 is unchanged. C10 API/immutable-prompt/parent-dispatch, typed bound-ready authority, current C8 proof gate and a single opencode-go credential resolver are active WIP, not published, built or native-qualified. All local models/inference, including PR24, remain deferred.

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
   get/preview/apply/explain, review-disposition handling, WorkDispatch
   auto-admission, typed manager authority, the shared manual/automatic launch
   slot, and MCP install/proof handlers/readback are wired through C8. C9 adds
   fresh-owned service startup/readback, plugin-directory index and pinned
   server syntax preparation, plus bounded departure reconciliation. Native
   plugin/tool loading, productive dispatch and GitHub projection remain
   unqualified.
3. **O2 durable intake and shared monitoring — Partial.** The dispatcher
   consumes bounded shared intake and journal readback for committed local
   controller/task.submission observations. Participant credential issuance,
   authenticated configured/connect readback, and C9 fresh-owned service
   startup/readback are wired. Other source adapters are not admitted; native
   MCP tool loading and model-visible capability remain unqualified.
4. **Productive launcher, workspace ownership and complete local O7 cycle —
   Partial.** Queue/context/overlap projections, launch preview, async lease,
   exact Task claim, agent.open, Participant context and WorkDispatch admission
   exist. Workspace lease enforcement remains database-backed; C9 adds a
   separate retained-proof fence for managed-service departure, not an OS-level
   workspace lock. Productive dispatch, native capability and the full
   frozen-candidate/review/return/correction/acceptance path remain unqualified.
5. **O3 Rust adapters and provider lifecycle — Partial.** Rust OpenCode V2 and
   Zed paths exist alongside JavaScript/Python module bridges. C9 configures a
   fresh-owned pinned OpenCode service, but native plugin loading, callable
   tools, and observed provider/model capability remain unqualified.
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
    qualification pending.** CI 37168223030 passed all Windows/Linux steps for
    source 2e609ec, including formatting, Clippy, tests, build and offline
    native fixtures. The live native run still timed out before an
    owned_service row; the overlay fix needs a local rebuild and native retest.
    Hosted green does not qualify native MCP/model capability or the complete
    manager-owned workflow.
The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
