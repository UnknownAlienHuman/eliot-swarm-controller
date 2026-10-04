# Implementation status

## Current state

**C9 source status:** C9 source 4218e7a4c67f5de244081d6e9dd37e2ee2e30544 is committed. The previous source 56bb19b9fbb5e3665fd567251d48a5c6e70e089b passed owned-crate formatting and production warnings-denied Clippy (11.95 s); the SQL-only 4218e7a delta passed formatting, and the debug candidate build passed (35.85 s). The SQL repair fixes escaped continuation whitespace in launcher::pending_launches; the exact extracted query also passed read-only preparation against the retained database. The earlier native host passed workspace configuration but failed at the first Store tick before Manager, Task or service. Independent bounded exact-source audits of 4218e7a passed: Store authority artifact .local/pr-implementation/c9-store-authority-audit.md (SHA-256 A7297B523C0964458778E0F25D3F39462E83CC497BA3ADF8A63EC060D9FC4EC0); owned runtime artifact .local/pr-implementation/c9-owned-runtime-audit.md (SHA-256 B67736D6AF04822E36C5DC8E657F9C05D31858B0973452678D169FF6964166B6). Corrected staged native acceptance remains pending. Hosted CI for 4218e7a4c67f5de244081d6e9dd37e2ee2e30544 is pending. Native MCP loading, callable tools, productive dispatch, model execution and the complete manager-owned cycle remain unqualified. Installed R6 is unchanged; all local model/inference work, including PR24/Kilo, remains deferred.

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
    qualification pending.** Catalog entries, handler wiring and individual CI
    runs do not prove native harness loading or a recovered end-to-end
    workflow. Historical C7/C8 CI evidence is recorded above. Source
    56bb19b9fbb5e3665fd567251d48a5c6e70e089b passed formatting and
    warnings-denied Clippy; hosted CI and corrected native acceptance remain
    pending, while productive dispatch and the full manager-owned cycle remain
    unqualified.
The status separates implemented slices from authored work and from runtime
qualification. A registry entry, configuration, or successful unrelated gate
does not establish productive launch or completion of the local delivery path.
