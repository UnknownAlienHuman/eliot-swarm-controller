# Donor Map: Agent Operations

Research date: 2026-10-03. Source baseline: `35e499ae73b622d873c44873f6993ee3fcbea87b`.

`CODE` means inspected implementation, `DOC` official documentation, `OWNER_AUDIT` supplied operating evidence, `DESIGN` our proposed adaptation. No source review here establishes live ELIOT qualification. A moving documentation page is not a pin of an installed binary. Record exact package/source versions and lockfile checks when implementation starts.

## 1. Reuse ELIOT before importing another controller

All links in this section are pinned to the inspected main commit.

| Unit | Inspected property | Reuse decision |
|---|---|---|
| [scheduler.rs](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/src/scheduler.rs) | `latest_due_slot`, `next_due_at_ms`, `run`; one-shot/interval, monotonic waiting, closed CheckRun action | Extend calendar/action types and registry indexing; do not install a second scheduler |
| [schedules.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/docs/schedules.md) | Due-slot admission, receipts and schedule cursor share one transaction; unknown previous run prevents overlap | Preserve old occurrence semantics through migration |
| [MCP subscriptions](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/src/mcp/subscriptions.rs) | Bounded queues and explicit lag; per-subscription 250-ms polling over committed facts | Preserve API/recovery; share polling/projector and add a separate live-content path |
| [Command native mod](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/modules/command/mod/eliot-command.ts) | Native `cmd.on` event hooks; queue admission is not application; partial text/thinking events excluded | Extend this adapter, not another Command launcher. Bound synchronous journal writes and repeated whole-inbox reads; report telemetry loss |
| [Muse observation helpers](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/modules/muse/observe.mjs) | Pure command-ID/death/durability facts; GapFiller deliberately not used on compact path | Keep native SDK and explicit unfilled gaps. Do not claim missing recovery is inherited automatically |
| [Claude bridge contract](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/modules/claude/README.md) | Pinned SDK 0.3.287, compact child/tool/result mapping; token partials counted, not retained as transcript | Wire supported SDK hooks and separate live streams within this bridge. SDK is commercially licensed, not a permissive code donor |
| [Forge publication](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/docs/forge-publication.md) | Exact accepted candidate, process-tree ownership, readback-only unknown recovery, documented non-atomic preflight | Reuse for push. Merge needs a new exact policy/operation, not a PowerShell shortcut |
| [Cargo manifest](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/Cargo.toml) | Rust MSRV 1.89, Tokio, rusqlite, RMCP 3.5, reqwest, existing atlas-redact | Prefer current ecosystem and preserve immutable donor packages. No dependency change in this PR |

A useful implementation distinction: the current universal module contract says native interpretations belong in adapters. Telemetry/automation core must not parse private Muse/Codex/OpenCode wire schemas in Store.

## 2. Ready Rust building blocks

### 2.1 Croner: adopt a complete expression evaluator, not its own scheduler

**DOC:** [Croner 3.0.1 API](https://docs.rs/croner/3.0.1/croner/) and [versioned crate documentation](https://docs.rs/crate/croner/3.0.1).

The reviewed API supplies parse plus next/previous occurrence calculations and timezone-aware scheduling. Its fixed-time versus wildcard DST rules differ; that matters for a supposedly simple daily job. Use the whole library with its documented grammar and preview actual occurrences. ELIOT owns persistence, dedupe, authority and overlap.

The discovery result also advertised [Croner 4.0.0](https://docs.rs/crate/croner/latest), including a Jiff backend. Version-specific 4.0.0 API/source pages were not retrievable in this review. Therefore 4.0.0 is not an approved implementation pin. Initial candidate is 3.0.1 with one compatible timezone backend; a later upgrade needs an explicit comparison, not silent use of `latest`.

**DESIGN:** select one calendar ecosystem; do not bring both Chrono and Jiff merely for flexibility. Freeze exact dependencies/MSRV/license and DST behavior in the implementation PR. Do not fork or rewrite Croner's parser.

### 2.2 sysinfo: adopt a compatible sensor, not lifecycle authority

**DOC:** [sysinfo 0.39.6/current](https://docs.rs/sysinfo/latest/sysinfo/) requires Rust 1.95; [0.37.2](https://docs.rs/sysinfo/0.37.2/sysinfo/) states MSRV 1.88.

That is a real incompatibility with ELIOT's declared 1.89 MSRV. Use 0.37.2 as the initial compatibility candidate unless the project explicitly changes toolchain policy. Keep a single long-lived `System`, refresh only needed fields and account for CPU sampling intervals. Unsupported platforms must not report healthy empty data.

**DESIGN:** metrics augment recorded OS ownership. They must not decide that an arbitrary PID/name is safe to kill. Verify transitive MSRV, platform support and dependency advisories during integration; a versioned doc is not a compiled or security qualification.

### 2.3 notify: adopt whole file-event library, treat events as hints

**DOC:** [notify 8.2.0](https://docs.rs/notify/latest/notify/).

The library documents platform/backend gaps, editor-dependent event shapes, network-filesystem issues and event loss on large watched trees. It offers polling fallback.

**DESIGN:** use a shared watcher to invalidate Git/workspace projections, followed by bounded exact reads. Do not derive a definitive commit or absent file from one event. Do not watch every file separately per agent. Exclude build/artifact directories where possible, preserve overflow evidence and qualify linked worktrees/Windows paths.

### 2.4 Why not tokio-cron-scheduler as the primary runtime?

**DOC:** [tokio-cron-scheduler 0.15.1](https://docs.rs/crate/tokio-cron-scheduler/0.15.1).

It is a credible async scheduler, with optional external persistence. ELIOT already has slot admission and Operation receipts in SQLite. Importing another scheduler/queue authority would require proving equivalence across two persistence paths. Choose a parser/evaluator dependency and retain ELIOT's owner instead.

## 3. Native hook and stream sources

### 3.1 Claude Code / Agent SDK

**DOC:** [official hooks reference](https://code.claude.com/docs/en/hooks).

Native before/after/lifecycle events provide suitable integration points. Async hooks cannot veto completed work; the current docs also state that background hook firings are not deduplicated and have lifecycle/timeout caveats. Stop hooks can cause continuation, so they are not neutral observation callbacks.

**DESIGN:** adapt hooks through the already pinned SDK/bridge; verify actual installed version and callback options. Use a small fast ingress, with ELIOT owning long jobs. Never run a full audit after every edit, put heavy work in an inline pre-tool hook or implement Goal by an unbounded Stop hook. Command examples from documentation are examples, not hardened script sandboxes.

### 3.2 OpenAI Codex

**DOC:** [configuration reference](https://developers.openai.com/codex/config-reference/) and [app-server protocol](https://developers.openai.com/codex/app-server/).

The current configuration reference documents local lifecycle hooks and separately notes cloud-orchestration restrictions. Command/MCP handlers and async semantics must be checked on the installed client; a local hook declaration is not evidence it runs in cloud Work orchestration.

The app-server documents `item/agentMessage/delta`, readable `item/reasoning/summaryTextDelta` and, only when supported by the model, `item/reasoning/textDelta`. Use the documented native item/turn IDs, not transcript-string heuristics. Not every model supplies every reasoning class.

**DESIGN:** keep the existing official SDK/protocol bridge. Add observability without taking ownership of a shared app-server or exposing opaque reasoning state. Protect exact parent/child attribution and gap recovery. Hook success is not a replacement for final durable execution evidence.

### 3.3 Gemini CLI

**DOC:** [hooks reference](https://geminicli.com/docs/hooks/reference/) and [hook overview](https://geminicli.com/docs/hooks/).

Hook stdin/stdout have a structured JSON contract; event-specific exit/output behavior matters. A post-tool decision cannot undo an already performed side effect. After-agent feedback may cause another turn and therefore needs continuation-loop control.

**DESIGN:** implement a Gemini CLI adapter only for an installed CLI route that is actually in scope. Do not attribute these APIs to Gemini Spark or Antigravity by name similarity. Keep logs off protocol stdout and preserve native failure semantics.

### 3.4 OpenCode and Command

**DOC:** [OpenCode plugins](https://opencode.ai/docs/plugins/) exposes event callbacks and before/after tool extension points.

**CODE:** ELIOT's [Command mod](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/35e499ae73b622d873c44873f6993ee3fcbea87b/modules/command/mod/eliot-command.ts) explicitly uses observational ModApi callbacks.

**DESIGN:** use official plugin APIs, not terminal scraping. Reconcile the public OpenCode plugin docs with the installed V2 service/source bundle before selecting types. These are separate product/version contracts. Command's `queueMessage` returning void is only local admission; it must not become `model_consumed=true`.

### 3.5 Every other installed module

Muse, Zed, Antigravity and future modules must publish a hook/stream capability matrix. Supported callbacks are wired through their actual SDK/plugin surface; batch-only adapters report process-level coverage. Do not promise all tool hooks on every runtime, and do not make optional hook absence stop unrelated work.

## 4. Automation product donors

### 4.1 Temporal: borrow scheduling semantics

**DOC:** [Schedule concepts and policies](https://docs.temporal.io/schedule).

The useful distinctions are schedule identity versus execution identity; pause of future starts versus pause/cancel of running work; overlap policy; catch-up window; explicit backfill; action limits. These prevent a timer from becoming an unbounded retry storm.

**DESIGN:** retain ELIOT's latest-only default, add bounded explicit alternatives and separate manual runs. Do not add a Temporal cluster/worker runtime to this local host merely to obtain cron. Temporal's `AllowAll`/termination options are not ELIOT defaults. Durable scheduling does not confer exactly-once arbitrary effects.

### 4.2 Windmill: borrow script/trigger/permission UX, not the whole control plane

**DOC:** [Schedules](https://www.windmill.dev/docs/core_concepts/scheduling), [roles and permissions](https://www.windmill.dev/docs/core_concepts/roles_and_permissions), [script settings](https://www.windmill.dev/docs/script_editor/settings), [draft/deploy](https://www.windmill.dev/docs/core_concepts/draft_and_deploy), [MCP](https://www.windmill.dev/docs/core_concepts/mcp), [concurrency](https://www.windmill.dev/docs/core_concepts/concurrency_limits).

Useful pieces: one runnable used by manual/scheduled/event triggers; input schema and run history; draft separated from deployed definition; scoped resources and execution identity; exact jobs available through MCP. Documentation warns that UI visibility is not a permission boundary and secret-read access is real access.

**DESIGN:** scripts are named/versioned actions with the same ELIOT invocation path. Pin the script version in each schedule/run rather than silently following an edited deployment. Preserve author/activator/runner identities and validate effective authority at execution. Some concurrency controls are paid-edition features, so do not describe all Windmill functionality as freely vendorable. Full adoption would bring another product/queue/permission model and is not proposed.

### 4.3 Goose: concrete Rust recipe-capture unit

**CODE:** inspected commit `591edd47cf2cfea4957d720c607cf2a4def8673d`:

- [scheduler/common.rs](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose/src/scheduler/common.rs)
- [scheduler_trait.rs](https://github.com/aaif-goose/goose/blob/591edd47cf2cfea4957d720c607cf2a4def8673d/crates/goose/src/scheduler_trait.rs)

`ValidatedScheduleRecipe`, bounded regular-file reads and retained `recipe_base_dir` are good implementation references. Relative dependency resolution must not unexpectedly move when content is copied to scheduler storage.

**DESIGN:** capture one immutable script bundle plus manifest/environment identity. Do not import Goose's session scheduler or its separate `schedule.json`. A copied entrypoint alone does not pin mutable imported files; ELIOT must capture declared dependencies too. The inspected write helper is not a substitute for ELIOT's durable artifact publication contract.

### 4.4 Paseo, AoE, CCCC and Claw

**OWNER_AUDIT:** supplied `Manager → Orchestrator → Executors`, Revision 4, 2026-10-02. Its ratings and release inventory are historical, not independently renewed by this PR.

Use its specific patterns as research inputs:

- Paseo: daemon/client separation and configured workspace scripts/services.
- Agent of Empires: manifest/grant fingerprints and reapproval when privileges expand.
- CCCC: exact addressed delivery and separation of stored/read/replied facts.
- Claw: independent acceptance contract and evidence, not self-reported completion.

Do not copy these whole controllers, inherit their authorization defaults or claim their workflow replay makes push/merge idempotent. ELIOT already has the corresponding durable ownership layers. Any later direct code reuse requires current source/license/version review of the exact unit.

## 5. GitHub and Git are event sources, not command text

**DOC:** [webhook best practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks), [signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), [Git hooks](https://git-scm.com/docs/githooks).

GitHub requires timely responses and recommends asynchronous processing. `X-GitHub-Delivery` survives requested redelivery. Validate HMAC-SHA256 against raw bytes before adopting a delivery; authenticate event/action and repository as well. Use durable intake and bounded reconciliation rather than assuming every event arrives once and in order.

**DESIGN:** local commit hooks nominate an OID; ELIOT verifies the exact repository/worktree/ref and deduplicates the resulting audit action. Branch names, human comments and commit author strings do not confer Task ownership. Git hooks are optional observations and must not replace Git readback or override the protected forge runner's existing hook suppression.

## 6. Owner operating evidence translated into safeguards

The supplied MANAGER-BRIEF is operating history, not a new policy source. The current repository's Owner Decisions govern implementation cadence and authority.

| Recorded incident | Required design response |
|---|---|
| Harness cron never fired during a long manager session | Timers and due receipts belong to the long-lived ELIOT host |
| Native CLI health checks restarted a busy shared service | Persistent native client/read-only reconciliation; never launch that CLI for monitoring |
| Shared Codex events counted the same children under every manager | Exact native ancestry and binding attribution |
| Raw command deltas duplicated final output and rapidly grew logs | Separate live presentation from retained terminal/evidence records |
| Parent idle/finished while children or native Goal continued | Keep family/continuation ownership separate from wrapper exit |
| Reminder text replaced the manager's actual work | Notice is not a Task; context injection is typed and bounded |
| Busy hook spool blocked compaction | Optional observation is not an inline liveness gate |
| Cleanup deleted live Windows transcripts based on mtime | Retain by actual active ownership/reference, not mtime alone |
| Large process fleets included orphan shell/MCP/fsmonitor children | Reuse owned process trees and shared sensors; no poller/process per observer |
| Old submission feedback modified a newer branch iteration | Every audit/action names exact Task revision, Attempt, candidate and source OID |

These are negative scenarios to qualify, not measured defects in the proposed new code.

## 7. Reuse decision summary

| Component | Decision |
|---|---|
| Existing ELIOT RuntimePort/SDKs/Store/CheckRunner/forge/atlas-redact | Reuse and extend |
| Croner | Whole parser/evaluator dependency; reviewed version and one timezone backend |
| sysinfo | Whole compatible metrics library; start with MSRV-compatible candidate, not unreviewed latest |
| notify | Whole sensor library; reconciliation remains ELIOT-owned |
| Tokio/rusqlite/RMCP | Existing stack; no parallel framework |
| Temporal/Windmill/Goose scheduling engines | Patterns/source references only, no replacement authority |
| Native provider hooks | Official callback adapters with installed capability evidence |
| Arbitrary hook shell snippets/global monkey patches | Do not adopt |
| Ready distributed brokers/full workflow engines | Not needed for the local first implementation |

No dependency or license decision is finalized by this docs-only PR. Implementation records the exact resolved artifact, license, transitive dependencies, supported MSRV and platform checks before adoption.
